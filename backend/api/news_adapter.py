# -*- coding: utf-8 -*-
"""
XauTrendLab — News Adapter v2.1
================================
Symbol-aware pre-trade news block check.
+ Discord alerts for active blocks, upcoming events, rate decision days.

Architecture:
    cron (every 30-60 min)
        ↓
    ForexFactory (primary) / Investing.com (backup)
        ↓
    Redis  xtl:news:calendar:daily  (TTL = 8 hrs)
        ↓
    check_news_block(symbol, now_ms, R)
        ↓
    block=True  →  Return WAIT
        ↓
    Redis  xtl:news:block:latest:{symbol}  (TTL = 36 hrs)
        ↓
    DB  news_block_events  (permanent audit + outcome learning)

Two entry points:
    1. fetch_and_store_calendar(R)         — background cron job
    2. check_news_block(symbol, now_ms, R) — gate call, Redis only

Manual Discord check (no cron needed):
    python -m api.news_adapter --check
    python -m api.news_adapter --morning
    python -m api.news_adapter --check --symbol XAUUSD
    python -m api.news_adapter --status
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

log = logging.getLogger("xtl.news_adapter")

# ---------------------------------------------------------------------------
# Redis Keys
# ---------------------------------------------------------------------------
REDIS_CALENDAR_KEY   = "xtl:news:calendar:daily"
REDIS_CALENDAR_TTL   = 8 * 3600          # 8 hours
REDIS_BLOCK_KEY      = "xtl:news:block:latest:{symbol}"
REDIS_BLOCK_TTL      = 36 * 3600         # 36 hours
REDIS_FETCH_LOCK_KEY = "xtl:news:fetch_lock"
REDIS_FETCH_LOCK_TTL = 300               # 5 min lock
REDIS_RATE_DAY_KEY   = "xtl:news:rate_day:{symbol}:{date}"   # NEW v2.1
REDIS_DISCORD_DEDUP  = "xtl:discord:news:sent:{key}"         # NEW v2.1

# ---------------------------------------------------------------------------
# Symbol-Aware Event Whitelist
# ---------------------------------------------------------------------------
# USD base events — inherited by ALL symbols
_USD_BASE_EVENTS = [
    "FOMC",
    "Fed Minutes",
    "Fed Speech",
    "Interest Rate Decision",
    "CPI",
    "Core CPI",
    "PCE",
    "Core PCE",
    "Non-Farm Payrolls",
    "NFP",
    "GDP",
    "ISM Manufacturing",
    "ISM Services",
]

# Symbol-specific additional events
_SYMBOL_EXTRA_EVENTS: Dict[str, List[str]] = {
    "XAUUSD": [],
    "EURUSD": [
        "ECB Rate Decision", "ECB Speech", "ECB Minutes",
        "EU CPI", "EU GDP", "European Central Bank",
    ],
    "GBPUSD": [
        "BOE Rate Decision", "BOE Speech", "BOE Minutes",
        "UK CPI", "UK GDP", "Bank of England", "MPC",
    ],
    "USDJPY": [
        "BOJ Rate Decision", "BOJ Speech", "BOJ Minutes",
        "Japan CPI", "Bank of Japan",
    ],
    "USDCHF": [
        "SNB Rate Decision", "SNB Speech",
        "SNB Quarterly Bulletin", "Swiss National Bank",
    ],
    "USDCAD": [
        "BOC Rate Decision", "BOC Speech", "BOC Minutes",
        "Canada Employment", "Canada CPI", "Bank of Canada","Unemployment Rate",
    ],
}

def _get_whitelist(symbol: str) -> List[str]:
    sym = str(symbol or "").upper().strip()
    return _USD_BASE_EVENTS + _SYMBOL_EXTRA_EVENTS.get(sym, [])

# ---------------------------------------------------------------------------
# Currency → directly-affected symbols (used as a fallback when an event name
# is generic and doesn't match the name whitelist, e.g. "Monetary Policy
# Statement"). XAUUSD is included for EVERY currency because gold reacts to all
# major rate decisions (USD real yields + broad risk sentiment).
#   USD high-impact  -> all pairs + XAUUSD
#   EUR rate decision -> EURUSD + XAUUSD
#   GBP rate decision -> GBPUSD + XAUUSD
#   JPY rate decision -> USDJPY + XAUUSD
#   AUD rate decision -> (no AUD pair traded) + XAUUSD
#   CAD rate decision -> USDCAD + XAUUSD
#   CHF rate decision -> USDCHF + XAUUSD
# ---------------------------------------------------------------------------
_CURRENCY_SYMBOLS: Dict[str, List[str]] = {
    "USD": ["XAUUSD", "EURUSD", "GBPUSD", "USDJPY", "USDCHF", "USDCAD"],
    "EUR": ["EURUSD", "XAUUSD"],
    "GBP": ["GBPUSD", "XAUUSD"],
    "JPY": ["USDJPY", "XAUUSD"],
    "CHF": ["USDCHF", "XAUUSD"],
    "CAD": ["USDCAD", "XAUUSD"],
    "AUD": ["XAUUSD"],   # no AUD pair in the traded set, but gold still reacts
    "NZD": ["XAUUSD"],
}

# ---------------------------------------------------------------------------
# Block Windows
# First match wins — specific names listed before generic ones
# ---------------------------------------------------------------------------
_BLOCK_WINDOWS: List[Tuple[str, Dict[str, int]]] = [
    # Major rate decisions
    ("FOMC",                   {"pre": 60, "post": 60, "stabilization": 30}),
    ("Interest Rate Decision",  {"pre": 60, "post": 60, "stabilization": 30}),
    ("ECB Rate Decision",       {"pre": 60, "post": 60, "stabilization": 30}),
    ("BOE Rate Decision",       {"pre": 60, "post": 60, "stabilization": 30}),
    ("BOJ Rate Decision",       {"pre": 60, "post": 60, "stabilization": 30}),
    ("SNB Rate Decision",       {"pre": 60, "post": 60, "stabilization": 30}),
    ("BOC Rate Decision",       {"pre": 60, "post": 60, "stabilization": 30}),
    # Employment
    ("Non-Farm Payrolls",       {"pre": 30, "post": 30, "stabilization": 15}),
    ("NFP",                     {"pre": 30, "post": 30, "stabilization": 15}),
    ("Canada Employment",       {"pre": 30, "post": 30, "stabilization": 15}),
    ("Unemployment Rate",       {"pre": 15, "post": 15, "stabilization":  0}),
    # Inflation — specific before generic
    ("Core CPI",                {"pre": 30, "post": 30, "stabilization": 15}),
    ("CPI",                     {"pre": 30, "post": 30, "stabilization": 15}),
    ("Core PCE",                {"pre": 30, "post": 30, "stabilization": 15}),
    ("PCE",                     {"pre": 30, "post": 30, "stabilization": 15}),
    ("EU CPI",                  {"pre": 30, "post": 30, "stabilization": 15}),
    ("UK CPI",                  {"pre": 30, "post": 30, "stabilization": 15}),
    ("Japan CPI",               {"pre": 30, "post": 30, "stabilization": 15}),
    ("Canada CPI",              {"pre": 30, "post": 30, "stabilization": 15}),
    # Growth
    ("GDP",                     {"pre": 30, "post": 30, "stabilization": 15}),
    ("EU GDP",                  {"pre": 30, "post": 30, "stabilization": 15}),
    ("UK GDP",                  {"pre": 30, "post": 30, "stabilization": 15}),
    # Central bank speeches / minutes
    ("Fed Minutes",             {"pre": 15, "post": 30, "stabilization":  0}),
    ("Fed Speech",              {"pre": 15, "post": 30, "stabilization":  0}),
    ("ECB Speech",              {"pre": 15, "post": 30, "stabilization":  0}),
    ("ECB Minutes",             {"pre": 15, "post": 30, "stabilization":  0}),
    ("BOE Speech",              {"pre": 15, "post": 30, "stabilization":  0}),
    ("BOE Minutes",             {"pre": 15, "post": 30, "stabilization":  0}),
    ("BOJ Speech",              {"pre": 15, "post": 30, "stabilization":  0}),
    ("BOJ Minutes",             {"pre": 15, "post": 30, "stabilization":  0}),
    ("SNB Speech",              {"pre": 15, "post": 30, "stabilization":  0}),
    ("SNB Quarterly Bulletin",  {"pre": 15, "post": 30, "stabilization":  0}),
    ("BOC Speech",              {"pre": 15, "post": 30, "stabilization":  0}),
    ("BOC Minutes",             {"pre": 15, "post": 30, "stabilization":  0}),
    # Activity
    ("ISM Manufacturing",       {"pre": 15, "post": 15, "stabilization":  0}),
    ("ISM Services",            {"pre": 15, "post": 15, "stabilization":  0}),
    # Generic central bank names (fallback)
    ("European Central Bank",   {"pre": 15, "post": 30, "stabilization":  0}),
    ("Bank of England",         {"pre": 15, "post": 30, "stabilization":  0}),
    ("Bank of Japan",           {"pre": 15, "post": 30, "stabilization":  0}),
    ("Swiss National Bank",     {"pre": 15, "post": 30, "stabilization":  0}),
    ("Bank of Canada",          {"pre": 15, "post": 30, "stabilization":  0}),
    ("MPC",                     {"pre": 15, "post": 30, "stabilization":  0}),
    ("BOC Rate Statement",     {"pre": 60, "post": 60, "stabilization": 30}),
    ("Monetary Policy Statement", {"pre": 60, "post": 60, "stabilization": 30}),
    ("ECB Press Conference",   {"pre": 15, "post": 30, "stabilization":  0}),
    ("BOE Gov",                {"pre": 15, "post": 30, "stabilization":  0}),
    ("BOJ Gov",                {"pre": 15, "post": 30, "stabilization":  0}),
    # Default
    ("__default__",             {"pre": 15, "post": 15, "stabilization":  0}),
]

def _get_block_window(event_name: str) -> Dict[str, int]:
    name = str(event_name or "").strip()
    for keyword, window in _BLOCK_WINDOWS:
        if keyword == "__default__":
            continue
        if name.lower().startswith(keyword.lower()):
            return dict(window)
    return dict(_BLOCK_WINDOWS[-1][1])

def _is_relevant(event_name: str, currency: str, symbol: str) -> bool:
    name = str(event_name or "").strip()
    cur  = str(currency or "").upper().strip()
    sym  = str(symbol or "").upper().strip()

    # 1) Name-based whitelist (existing behavior)
    for entry in _get_whitelist(symbol):
        if name.lower().startswith(entry.lower()):
            # If event is in USD base list, only match if currency is USD
            if entry in _USD_BASE_EVENTS and cur and cur != "USD":
                continue
            return True

    # 2) Currency-based fallback.
    #    Handles generic event names (e.g. "Monetary Policy Statement", "RBA Rate
    #    Statement") that don't match the name whitelist but clearly belong to a
    #    currency. Each currency maps to its direct pair; XAUUSD is included for
    #    EVERY currency because gold is sensitive to all major rate decisions.
    if cur and sym in _CURRENCY_SYMBOLS.get(cur, []):
        return True

    return False

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_ms() -> int:
    return int(time.time() * 1000)

def _json_load(raw) -> Optional[dict]:
    try:
        if raw is None:
            return None
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", "ignore")
        return json.loads(str(raw).strip())
    except Exception:
        return None

def _build_event(name: str, currency: str, impact: str, event_time_ms: int) -> dict:
    window = _get_block_window(name)
    return {
        "event":             str(name),
        "currency":          str(currency or "").upper(),
        "impact":            str(impact or "").upper(),
        "time_ms":           int(event_time_ms),
        "pre_block_min":     int(window["pre"]),
        "post_block_min":    int(window["post"]),
        "stabilization_min": int(window["stabilization"]),
    }

def _wait_response(reason: str, window: str, shadow: bool) -> dict:
    return {
        "block":            True,
        "verdict":          "WAIT",
        "shadow":           bool(shadow),
        "reason":           reason,
        "event_name":       None,
        "event_time_ms":    None,
        "minutes_to_event": None,
        "impact":           None,
        "window":           window,
    }

# ---------------------------------------------------------------------------
# ForexFactory Scraper (Primary)
# ---------------------------------------------------------------------------

def _scrape_forexfactory(lookahead_hours: int = 48) -> List[dict]:
    events: List[dict] = []
    try:
        import requests
        from bs4 import BeautifulSoup

        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9",
        }

        resp = requests.get(
            "https://www.forexfactory.com/calendar",
            headers=headers, timeout=15
        )
        if resp.status_code != 200:
            log.warning("[NEWS] ForexFactory status %s", resp.status_code)
            return []

        soup = BeautifulSoup(resp.text, "html.parser")
        rows = soup.select("tr.calendar__row")

        now_utc      = datetime.now(timezone.utc)
        cutoff       = now_utc + timedelta(hours=lookahead_hours)
        current_date = now_utc.date()

        for row in rows:
            try:
                # Date cell
                date_cell = row.select_one("td.calendar__date span")
                if date_cell and date_cell.text.strip():
                    try:
                        current_date = datetime.strptime(
                            f"{date_cell.text.strip()} {now_utc.year}", "%a %b %d %Y"
                        ).date()
                    except Exception:
                        pass

                # Impact — HIGH only
                impact_cell = row.select_one("td.calendar__impact span")
                if not impact_cell:
                    continue
                if "high" not in " ".join(impact_cell.get("class", [])).lower():
                    continue

                # Currency
                currency_cell = row.select_one("td.calendar__currency")
                currency = currency_cell.text.strip() if currency_cell else ""

                # Event name
                event_cell = (
                    row.select_one("td.calendar__event span.calendar__event-title")
                    or row.select_one("td.calendar__event")
                )
                event_name = event_cell.text.strip() if event_cell else ""
                if not event_name:
                    continue

                # Time — ForexFactory uses Eastern Time (UTC-5 conservative)
                time_cell = row.select_one("td.calendar__time")
                time_text = time_cell.text.strip() if time_cell else ""
                event_dt  = None
                if time_text and ":" in time_text:
                    try:
                        t = datetime.strptime(time_text.lower(), "%I:%M%p")
                        event_dt = (
                            datetime.combine(current_date, t.time())
                            .replace(tzinfo=timezone.utc)
                            + timedelta(hours=5)
                        )
                    except Exception:
                        pass

                if event_dt is None:
                    event_dt = datetime.combine(
                        current_date, datetime.min.time(), tzinfo=timezone.utc
                    ).replace(hour=13, minute=30)

                if event_dt < now_utc or event_dt > cutoff:
                    continue

                # Relevant to at least one XTL symbol
                if not any(
                    _is_relevant(event_name, currency, s)
                    for s in _SYMBOL_EXTRA_EVENTS
                ):
                    continue

                events.append(_build_event(
                    event_name, currency, "HIGH",
                    int(event_dt.timestamp() * 1000)
                ))

            except Exception as e:
                log.debug("[NEWS] FF row error: %s", e)

        log.info("[NEWS] ForexFactory: %d relevant HIGH events", len(events))

    except ImportError:
        log.error("[NEWS] pip install requests beautifulsoup4")
    except Exception as e:
        log.error("[NEWS] ForexFactory error: %s", e)

    return events

# ---------------------------------------------------------------------------
# Investing.com Scraper (Backup)
# ---------------------------------------------------------------------------

def _scrape_investing(lookahead_hours: int = 48) -> List[dict]:
    events: List[dict] = []
    try:
        import requests
        from bs4 import BeautifulSoup

        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "X-Requested-With": "XMLHttpRequest",
            "Referer": "https://www.investing.com/economic-calendar/",
        }

        now_utc = datetime.now(timezone.utc)
        cutoff  = now_utc + timedelta(hours=lookahead_hours)

        resp = requests.post(
            "https://www.investing.com/economic-calendar/Service/getCalendarFilteredData",
            data={
                "country[]":    ["5", "4", "35", "14", "12", "72"],
                "importance[]": "3",
                "dateFrom":     now_utc.strftime("%Y-%m-%d"),
                "dateTo":       cutoff.strftime("%Y-%m-%d"),
                "timeZone":     "0",
                "timeFilter":   "timeRemain",
                "currentTab":   "custom",
                "submitFilters":"1",
            },
            headers=headers, timeout=15
        )
        if resp.status_code != 200:
            log.warning("[NEWS] Investing.com status %s", resp.status_code)
            return []

        soup = BeautifulSoup(resp.json().get("data", ""), "html.parser")
        for row in soup.select("tr.js-event-item"):
            try:
                event_cell = row.select_one("td.event a")
                event_name = event_cell.text.strip() if event_cell else ""
                if not event_name:
                    continue

                currency_cell = row.select_one("td.flagCur")
                currency = currency_cell.text.strip() if currency_cell else "USD"

                impact_cell = row.select_one("td.sentiment")
                if not impact_cell:
                    continue
                if len(impact_cell.select(
                    "i.grayFullBullishIcon, i.redFullBullishIcon"
                )) < 3:
                    continue

                time_cell = row.select_one("td.time")
                time_text = time_cell.text.strip() if time_cell else ""
                event_dt  = None
                if time_text and ":" in time_text:
                    try:
                        t = datetime.strptime(time_text, "%H:%M")
                        event_dt = now_utc.replace(
                            hour=t.hour, minute=t.minute,
                            second=0, microsecond=0
                        )
                    except Exception:
                        pass

                if event_dt is None:
                    event_dt = now_utc.replace(
                        hour=13, minute=30, second=0, microsecond=0
                    )

                if event_dt < now_utc or event_dt > cutoff:
                    continue

                if not any(
                    _is_relevant(event_name, currency, s)
                    for s in _SYMBOL_EXTRA_EVENTS
                ):
                    continue

                events.append(_build_event(
                    event_name, currency, "HIGH",
                    int(event_dt.timestamp() * 1000)
                ))

            except Exception as e:
                log.debug("[NEWS] Investing row error: %s", e)

        log.info("[NEWS] Investing.com: %d relevant HIGH events", len(events))

    except ImportError:
        log.error("[NEWS] pip install requests beautifulsoup4")
    except Exception as e:
        log.error("[NEWS] Investing.com error: %s", e)

    return events

# ---------------------------------------------------------------------------
# Step 1 — Background Fetcher (cron every 30-60 min)
# ---------------------------------------------------------------------------

def fetch_and_store_calendar(R, lookahead_hours: int = 48) -> dict:
    """
    Fetch and store economic calendar in Redis.
    Primary: ForexFactory | Backup: Investing.com
    """
    if R is None:
        return {"ok": False, "reason": "redis_unavailable"}

    # Distributed lock
    try:
        if not R.set(REDIS_FETCH_LOCK_KEY, "1", nx=True, ex=REDIS_FETCH_LOCK_TTL):
            log.info("[NEWS] Fetch already in progress — skipping")
            return {"ok": False, "reason": "fetch_locked"}
    except Exception as e:
        log.warning("[NEWS] Lock check failed: %s", e)

    events: List[dict] = []
    source = "none"

    try:
        events = _scrape_forexfactory(lookahead_hours)
        if events:
            source = "forexfactory"
    except Exception as e:
        log.warning("[NEWS] ForexFactory failed: %s", e)

    if not events:
        try:
            log.info("[NEWS] Falling back to Investing.com")
            events = _scrape_investing(lookahead_hours)
            if events:
                source = "investing"
        except Exception as e:
            log.warning("[NEWS] Investing.com failed: %s", e)

    now_ms = _now_ms()
    try:
        R.set(
            REDIS_CALENDAR_KEY,
            json.dumps({
                "events":          events,
                "fetched_at_ms":   now_ms,
                "source":          source,
                "lookahead_hours": lookahead_hours,
                "count":           len(events),
            }, separators=(",", ":")),
            ex=REDIS_CALENDAR_TTL,
        )
        log.info("[NEWS] Stored %d events from %s", len(events), source)
    except Exception as e:
        log.error("[NEWS] Redis store failed: %s", e)
        return {"ok": False, "reason": f"redis_store_failed:{e}"}
    finally:
        try:
            R.delete(REDIS_FETCH_LOCK_KEY)
        except Exception:
            pass

    return {"ok": True, "source": source, "events_count": len(events), "fetched_at_ms": now_ms}

# ---------------------------------------------------------------------------
# Step 2 — Gate Check (Redis only, zero API cost)
# ---------------------------------------------------------------------------

def check_news_block(
    symbol: str,
    now_ms: int,
    R,
    *,
    shadow_mode: bool = False,
    db=None,
    gate_context: Optional[dict] = None,
) -> dict:
    """
    Check if now_ms is inside a news block window for this symbol.

    Returns:
        {
            "block":            bool,
            "verdict":          "WAIT" | "ALLOW",
            "shadow":           bool,
            "reason":           str | None,
            "event_name":       str | None,
            "event_time_ms":    int | None,
            "minutes_to_event": float | None,
            "impact":           str | None,
            "window":           "PRE_EVENT"|"POST_EVENT"|"STABILIZATION"|None,
        }
    """
    sym_u  = str(symbol or "").upper().strip()
    now_ms = int(now_ms or 0)

    _allow = {
        "block": False, "verdict": "ALLOW", "shadow": bool(shadow_mode),
        "reason": None, "event_name": None, "event_time_ms": None,
        "minutes_to_event": None, "impact": None, "window": None,
    }

    # Read Redis
    try:
        raw = R.get(REDIS_CALENDAR_KEY) if R is not None else None
        calendar_data = _json_load(raw) if raw else None
    except Exception as e:
        log.warning("[NEWS] Redis read failed: %s", e)
        if shadow_mode:
            return {**_allow, "reason": "NEWS_ADAPTER_REDIS_UNAVAILABLE"}
        return _wait_response("NEWS_ADAPTER_REDIS_UNAVAILABLE", "REDIS_UNAVAILABLE", shadow_mode)

    if not isinstance(calendar_data, dict) or not calendar_data.get("events"):
        msg = "NEWS_CALENDAR_MISSING_OR_STALE"
        log.warning("[NEWS] %s | symbol=%s", msg, sym_u)
        if shadow_mode:
            return {**_allow, "reason": msg}
        return _wait_response(msg, "CALENDAR_STALE", shadow_mode)

    # Scan events
    for ev in calendar_data.get("events", []):
        try:
            event_name    = str(ev.get("event") or "")
            currency      = str(ev.get("currency") or "")
            event_time_ms = int(ev.get("time_ms") or 0)
            pre_min       = int(ev.get("pre_block_min") or 15)
            post_min      = int(ev.get("post_block_min") or 15)
            stab_min      = int(ev.get("stabilization_min") or 0)

            if event_time_ms <= 0:
                continue
            if not _is_relevant(event_name, currency, sym_u):
                continue

            pre_ms  = pre_min  * 60_000
            post_ms = post_min * 60_000
            stab_ms = stab_min * 60_000

            window_start = event_time_ms - pre_ms
            window_end   = event_time_ms + post_ms + stab_ms

            if not (window_start <= now_ms <= window_end):
                continue

            delta_ms = event_time_ms - now_ms

            if delta_ms >= 0:
                window_type = "PRE_EVENT"
                reason = f"HIGH_IMPACT_NEWS | {event_name} in {int(delta_ms/60000)} min"
            elif now_ms <= event_time_ms + post_ms:
                window_type = "POST_EVENT"
                mins_after = int(abs(delta_ms) / 60000)
                reason = f"HIGH_IMPACT_NEWS | {event_name} released {mins_after} min ago"
            else:
                window_type = "STABILIZATION"
                mins_into = int((now_ms - (event_time_ms + post_ms)) / 60000)
                reason = (
                    f"HIGH_IMPACT_NEWS | {event_name} "
                    f"stabilization {mins_into}/{stab_min} min"
                )

            log.info("[NEWS] BLOCK: %s | symbol=%s", reason, sym_u)

            result = {
                "block":            not shadow_mode,
                "verdict":          "ALLOW" if shadow_mode else "WAIT",
                "shadow":           bool(shadow_mode),
                "reason":           f"SHADOW_WARN | {reason}" if shadow_mode else reason,
                "event_name":       event_name,
                "event_time_ms":    event_time_ms,
                "minutes_to_event": round(delta_ms / 60000, 1),
                "impact":           str(ev.get("impact") or "HIGH"),
                "window":           window_type,
            }

            # Redis snapshot
            _store_block_snapshot(R, sym_u, result)

            # DB audit row (blocking mode only)
            if not shadow_mode:
                _insert_audit_row(db, sym_u, result, gate_context)

            return result

        except Exception as e:
            log.debug("[NEWS] Event check error: %s", e)

    return _allow

# ---------------------------------------------------------------------------
# Redis Snapshot
# ---------------------------------------------------------------------------

def _store_block_snapshot(R, symbol: str, result: dict) -> None:
    if R is None:
        return
    try:
        R.set(
            REDIS_BLOCK_KEY.format(symbol=symbol),
            json.dumps({**result, "stored_at_ms": _now_ms()}, separators=(",", ":")),
            ex=REDIS_BLOCK_TTL,
        )
    except Exception as e:
        log.debug("[NEWS] Snapshot write failed: %s", e)

# ---------------------------------------------------------------------------
# DB Audit Row
# ---------------------------------------------------------------------------

def _insert_audit_row(db, symbol: str, result: dict, gate_context: Optional[dict]) -> None:
    """
    Insert audit row into news_block_events.

    CREATE TABLE IF NOT EXISTS news_block_events (
        id                SERIAL PRIMARY KEY,
        symbol            VARCHAR(20),
        direction         VARCHAR(10),
        event_name        VARCHAR(200),
        event_time_ms     BIGINT,
        blocked_at_ms     BIGINT,
        window_type       VARCHAR(30),
        zone_low          FLOAT,
        zone_high         FLOAT,
        entry_trigger     FLOAT,
        verdict           VARCHAR(20),
        outcome_simulated VARCHAR(20),
        outcome_price     FLOAT,
        created_at        TIMESTAMP DEFAULT NOW()
    );
    """
    if db is None:
        return
    try:
        gc        = gate_context or {}
        zone      = gc.get("zone_used") or {}
        trigger   = gc.get("rev_trigger") or {}
        direction = str(gc.get("resolved_dir") or gc.get("direction") or "")

        entry_trigger = None
        try:
            entry_trigger = float(
                trigger.get("entry_above") if direction == "BUY"
                else trigger.get("entry_below") or 0
            ) or None
        except Exception:
            pass

        row = {
            "symbol":            symbol,
            "direction":         direction,
            "event_name":        str(result.get("event_name") or ""),
            "event_time_ms":     int(result.get("event_time_ms") or 0),
            "blocked_at_ms":     _now_ms(),
            "window_type":       str(result.get("window") or ""),
            "zone_low":          float(zone.get("low") or 0) or None,
            "zone_high":         float(zone.get("high") or 0) or None,
            "entry_trigger":     entry_trigger,
            "verdict":           str(result.get("verdict") or "WAIT"),
            "outcome_simulated": None,
            "outcome_price":     None,
        }

        db.execute(
            "INSERT INTO news_block_events ({cols}) VALUES ({vals})".format(
                cols=", ".join(row.keys()),
                vals=", ".join(f":{k}" for k in row.keys()),
            ),
            row,
        )
        db.commit()
    except Exception as e:
        log.warning("[NEWS] DB audit insert failed: %s", e)
        try:
            db.rollback()
        except Exception:
            pass

# ---------------------------------------------------------------------------
# NEW v2.1 — Rate decision helper
# ---------------------------------------------------------------------------

ALL_SYMBOLS = list(_SYMBOL_EXTRA_EVENTS.keys())

_RATE_DECISION_PREFIXES = [
    "FOMC", "Interest Rate Decision",
    "ECB Rate Decision", "BOE Rate Decision",
    "BOJ Rate Decision", "SNB Rate Decision",
    "BOC Rate Decision", "BOC Rate Statement",
    "Monetary Policy Statement",
]

def _is_rate_decision(event_name: str) -> bool:
    name = str(event_name or "").strip().lower()
    return any(name.startswith(p.lower()) for p in _RATE_DECISION_PREFIXES)

def _fmt_time_utc(time_ms: int) -> str:
    try:
        return datetime.utcfromtimestamp(time_ms / 1000).strftime("%H:%M UTC")
    except Exception:
        return "??"

def _cb_name(event_name: str) -> str:
    e = str(event_name or "").upper()
    if "ECB" in e:                           return "ECB"
    if "BOE" in e or "BANK OF ENGLAND" in e: return "BOE"
    if "BOJ" in e or "BANK OF JAPAN" in e:   return "BOJ"
    if "SNB" in e or "SWISS" in e:           return "SNB"
    if "BOC" in e or "BANK OF CANADA" in e:  return "BOC"
    if "FOMC" in e or "INTEREST RATE" in e:  return "Fed"
    return str(event_name or "")[:20]

# ---------------------------------------------------------------------------
# NEW v2.1 — Discord
# ---------------------------------------------------------------------------

def _discord_webhook_url() -> str:
    return (
        os.getenv("DISCORD_WEBHOOK_URL")
        or os.getenv("XTL_DISCORD_WEBHOOK_URL")
        or ""
    ).strip()

def _discord_post(content: str) -> bool:
    """Fire-and-forget Discord webhook. Returns True if sent."""
    url = _discord_webhook_url()
    if not url:
        log.warning("[DISCORD] DISCORD_WEBHOOK_URL not set — skipping alert")
        return False
    try:
        import urllib.request
        # Normalize domain — discordapp.com redirects to discord.com
        # but urllib does not follow redirects on POST so we fix it here
        url = url.replace("discordapp.com", "discord.com")
        data = json.dumps({"content": content[:1900]}).encode("utf-8")
        req = urllib.request.Request(
            url, data=data,
            headers={"Content-Type": "application/json", "User-Agent": "XTLBot/1.0"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=6) as resp:
            _ = resp.read()
        log.info("[DISCORD] Sent OK")
        return True
    except Exception as e:
        log.warning("[DISCORD] Post failed: %s", e)
        return False

def _discord_dedupe(R, key: str, ttl_sec: int = 4 * 3600) -> bool:
    """Return True only if this key has NOT been sent recently (Redis NX)."""
    if R is None:
        return True
    dk = REDIS_DISCORD_DEDUP.format(key=key)
    try:
        return bool(R.set(dk, "1", nx=True, ex=int(ttl_sec)))
    except Exception:
        return True

def _alert_block_active(symbol: str, result: dict) -> str:
    ev   = result.get("event_name", "?")
    dt   = result.get("datetime_utc") or _fmt_time_utc(result.get("event_time_ms") or 0)
    win  = result.get("window", "")
    mins = result.get("minutes_to_event") or 0
    rate_flag = "🔴 **RATE DECISION BLOCK**" if result.get("is_rate_decision") else "⚠️ **NEWS BLOCK**"
    if win == "PRE_EVENT":
        timing = f"Event in **{abs(mins):.0f} min** — pre-block active"
    elif win == "POST_EVENT":
        timing = "Event passed — post-block active"
    else:
        timing = "Stabilization window active"
    return (
        f"{rate_flag} — **{symbol}**\n"
        f"Event: `{ev}` | `{dt}`\n"
        f"{timing}\n"
        f"Status: `BLOCKED — no entries until window clears`"
    )

def _alert_upcoming(events_by_symbol: dict) -> str:
    lines = ["📅 **UPCOMING HIGH IMPACT NEWS**", ""]
    seen: set = set()
    for sym, ev_list in events_by_symbol.items():
        for ev in ev_list:
            key = f"{ev['event']}|{ev.get('datetime_utc','')}"
            if key in seen:
                continue
            seen.add(key)
            rate_flag = "🔴 " if ev.get("is_rate_decision") else "⚠️ "
            lines.append(
                f"{rate_flag}`{ev['event']}` ({ev['currency']}) — "
                f"in **{ev['mins_to_event']:.0f} min** | `{ev.get('datetime_utc','')}`"
            )
    lines += ["", "⏳ Block windows will activate before each event."]
    return "\n".join(lines)

def _alert_rate_day_start(cb: str, affected_symbols: str, ev: dict) -> str:
    ev_time  = _fmt_time_utc(ev["time_ms"])
    pre_time = _fmt_time_utc(ev["time_ms"] - ev["pre_block_min"] * 60 * 1000)
    clr_time = _fmt_time_utc(
        ev["time_ms"] + (ev["post_block_min"] + ev["stabilization_min"]) * 60 * 1000
    )
    return (
        f"🔴 **RATE DECISION DAY — {cb}**\n\n"
        f"Event:     `{ev['event']}`\n"
        f"Time:      `{ev_time}`\n"
        f"Pre-block: `{pre_time}` (60 min before)\n"
        f"Est. clear:`{clr_time}`\n"
        f"Symbols:   `{affected_symbols}`\n\n"
        f"Status: `Entries BLOCKED from {pre_time} until after stabilization`\n"
        f"⚠️ Watch Discord for outcome alert after decision fires."
    )

# ---------------------------------------------------------------------------
# NEW v2.1 — Morning Rate Check
# ---------------------------------------------------------------------------

def morning_rate_check(R, events: Optional[List[dict]] = None) -> None:
    """
    Scan today's calendar for rate decisions.
    Sets Redis day flag per symbol + sends Discord day-start alert.
    Run once per day — manually or via cron at 06:00 UTC.
    """
    if events is None:
        events = _load_calendar_events(R)

    today      = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    now_ms     = _now_ms()
    day_end_ms = now_ms + 24 * 60 * 60 * 1000

    rate_events = [
        e for e in events
        if e.get("is_rate_decision", _is_rate_decision(e.get("event", "")))
        and now_ms <= int(e.get("time_ms") or 0) <= day_end_ms
    ]

    if not rate_events:
        log.info("[RATE_CHECK] No rate decisions in next 24h")
        _discord_post(
            f"✅ **XTL News Check** — No rate decisions today ({today}). "
            "Normal trading windows apply."
        )
        return

    log.info("[RATE_CHECK] %d rate decision event(s) found", len(rate_events))
    alerted_cbs: set = set()

    for ev in rate_events:
        affected = [
            sym for sym in ALL_SYMBOLS
            if _is_relevant(ev.get("event", ""), ev.get("currency", ""), sym)
        ]
        if not affected:
            affected = ALL_SYMBOLS

        for sym in affected:
            key = REDIS_RATE_DAY_KEY.format(symbol=sym, date=today)
            payload = {
                "event":             ev.get("event", ""),
                "time_ms":           ev.get("time_ms", 0),
                "currency":          ev.get("currency", ""),
                "cb_name":           _cb_name(ev.get("event", "")),
                "pre_block_min":     ev.get("pre_block_min", 60),
                "post_block_min":    ev.get("post_block_min", 60),
                "stabilization_min": ev.get("stabilization_min", 30),
            }
            try:
                R.setex(key, 24 * 3600, json.dumps(payload))
                log.info("[RATE_CHECK] Set flag %s", key)
            except Exception as exc:
                log.warning("[RATE_CHECK] Redis set failed %s: %s", key, exc)

        cb = _cb_name(ev.get("event", ""))
        if cb not in alerted_cbs:
            alerted_cbs.add(cb)
            msg = _alert_rate_day_start(cb, ", ".join(affected), ev)
            if _discord_dedupe(R, f"rate_day:{cb}:{today}", ttl_sec=20 * 3600):
                _discord_post(msg)

# ---------------------------------------------------------------------------
# NEW v2.1 — Discord Block Check
# ---------------------------------------------------------------------------

def _load_calendar_events(R) -> List[dict]:
    try:
        raw  = R.get(REDIS_CALENDAR_KEY) if R is not None else None
        data = _json_load(raw) if raw else None
        return data.get("events") or [] if isinstance(data, dict) else []
    except Exception:
        return []

def _check_block_internal(symbol: str, now_ms: int, events: List[dict]) -> dict:
    """Internal check — returns block status + upcoming list. No Redis writes."""
    sym_u    = str(symbol or "").upper().strip()
    relevant = [e for e in events if _is_relevant(e.get("event",""), e.get("currency",""), sym_u)]
    upcoming = []

    for ev in relevant:
        t_ms    = int(ev.get("time_ms") or 0)
        pre_ms  = int(ev.get("pre_block_min",  15)) * 60_000
        post_ms = int(ev.get("post_block_min", 15)) * 60_000
        stab_ms = int(ev.get("stabilization_min", 0)) * 60_000
        delta_ms = t_ms - now_ms
        mins_to  = delta_ms / 60_000

        if 0 < mins_to <= 120:
            upcoming.append({
                "event":            ev.get("event", ""),
                "currency":         ev.get("currency", ""),
                "datetime_utc":     _fmt_time_utc(t_ms),
                "mins_to_event":    round(mins_to, 1),
                "is_rate_decision": ev.get("is_rate_decision", _is_rate_decision(ev.get("event",""))),
            })

        if (t_ms - pre_ms) <= now_ms <= (t_ms + post_ms + stab_ms):
            if delta_ms >= 0:
                wtype = "PRE_EVENT"
            elif now_ms <= t_ms + post_ms:
                wtype = "POST_EVENT"
            else:
                wtype = "STABILIZATION"
            return {
                "block":            True,
                "verdict":          "WAIT",
                "event_name":       ev.get("event", ""),
                "currency":         ev.get("currency", ""),
                "event_time_ms":    t_ms,
                "datetime_utc":     _fmt_time_utc(t_ms),
                "minutes_to_event": round(mins_to, 1),
                "window":           wtype,
                "is_rate_decision": ev.get("is_rate_decision", _is_rate_decision(ev.get("event",""))),
                "upcoming":         upcoming,
            }

    upcoming.sort(key=lambda x: x["mins_to_event"])
    return {"block": False, "verdict": "ALLOW", "upcoming": upcoming}

def discord_check(R, symbol: Optional[str] = None) -> None:
    """
    Check current block status for all (or one) symbol(s).
    Sends Discord alerts for active blocks and upcoming events within 60 min.
    """
    now_ms  = _now_ms()
    now_utc = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    events  = _load_calendar_events(R)
    symbols = [symbol.upper()] if symbol else ALL_SYMBOLS

    if not events:
        _discord_post(
            f"⚠️ **XTL News Adapter** — No calendar data at {now_utc}.\n"
            "Run `scraper_local.py` + `upload_calendar.bat` to populate."
        )
        return

    blocks_active: List[Tuple[str, dict]] = []
    upcoming_by_sym: Dict[str, List[dict]] = {}

    for sym in symbols:
        result = _check_block_internal(sym, now_ms, events)
        if result["block"]:
            blocks_active.append((sym, result))
            log.warning("[BLOCK] %s BLOCKED | %s | %s", sym, result.get("event_name"), result.get("window"))
        else:
            near = [u for u in result.get("upcoming", []) if u["mins_to_event"] <= 60]
            if near:
                upcoming_by_sym[sym] = near
            log.info("[CHECK] %s ALLOW | upcoming_60min=%d", sym, len(near))

    for sym, result in blocks_active:
        msg = _alert_block_active(sym, result)
        dk  = f"block:{sym}:{result.get('event_name','')}:{result.get('window','')}"
        if _discord_dedupe(R, dk, ttl_sec=2 * 3600):
            _discord_post(msg)

    if upcoming_by_sym and not blocks_active:
        msg = _alert_upcoming(upcoming_by_sym)
        first_evs = next(iter(upcoming_by_sym.values()), [{}])
        first_ev  = first_evs[0] if first_evs else {}
        first_key = f"upcoming:{first_ev.get('event','')}:{first_ev.get('datetime_utc','')}"
        if _discord_dedupe(R, first_key, ttl_sec=90 * 60):
            _discord_post(msg)

    if not blocks_active and not upcoming_by_sym:
        log.info("[CHECK] All clear — no blocks or events within 60 min")
        if symbol:
            _discord_post(
                f"✅ **{symbol}** — No news blocks active. "
                f"No high-impact events within 60 min. (`{now_utc}`)"
            )

# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def get_upcoming_events(R, symbol: Optional[str] = None, hours_ahead: int = 24) -> List[dict]:
    """Return upcoming events from Redis — optionally filtered by symbol."""
    try:
        raw  = R.get(REDIS_CALENDAR_KEY) if R is not None else None
        data = _json_load(raw) if raw else None
        if not isinstance(data, dict):
            return []
        now_ms = _now_ms()
        cut_ms = now_ms + hours_ahead * 3_600_000
        result = [
            e for e in data.get("events", [])
            if isinstance(e, dict)
            and now_ms <= int(e.get("time_ms") or 0) <= cut_ms
            and (not symbol or _is_relevant(e.get("event",""), e.get("currency",""), symbol))
        ]
        result.sort(key=lambda x: int(x.get("time_ms") or 0))
        return result
    except Exception:
        return []


def get_block_snapshot(R, symbol: str) -> Optional[dict]:
    """Return latest block snapshot for a symbol."""
    try:
        raw = R.get(REDIS_BLOCK_KEY.format(symbol=str(symbol).upper().strip())) if R else None
        return _json_load(raw)
    except Exception:
        return None

# ---------------------------------------------------------------------------
# NEW v2.1 — Today's Calendar Summary
# ---------------------------------------------------------------------------

def discord_today(R) -> None:
    """
    Post today's high-impact events to Discord with affected symbols.
    One clean message — sent every time the bat runs.
    """
    now_utc    = datetime.now(timezone.utc)
    today_str  = now_utc.strftime("%Y-%m-%d")
    today_start_ms = int(datetime(now_utc.year, now_utc.month, now_utc.day, tzinfo=timezone.utc).timestamp() * 1000)
    today_end_ms   = today_start_ms + 24 * 60 * 60 * 1000

    events = _load_calendar_events(R)

    today_events = [
        e for e in events
        if today_start_ms <= int(e.get("time_ms") or 0) < today_end_ms
    ]
    today_events.sort(key=lambda x: int(x.get("time_ms") or 0))

    if not today_events:
        msg = (
            f"📅 **XTL News — Today ({today_str})**\n"
            f"─────────────────────────────────\n"
            f"✅ No high-impact events today. Clear trading day."
        )
        _discord_post(msg)
        return

    lines = [
        f"📅 **XTL News — Today ({today_str})**",
        "─────────────────────────────────",
    ]

    for ev in today_events:
        t_utc    = _fmt_time_utc(ev["time_ms"])
        currency = ev.get("currency", "")
        name     = ev.get("event", "")
        is_rate  = ev.get("is_rate_decision", _is_rate_decision(name))
        flag     = "🔴" if is_rate else "⚠️"

        # Which symbols does this event affect?
        affected = [
            sym for sym in ALL_SYMBOLS
            if _is_relevant(name, currency, sym)
        ]
        sym_str = ", ".join(affected) if affected else "ALL"

        lines.append(
            f"{flag} `{t_utc}` | **{currency}** | {name}\n"
            f"   └ Affects: `{sym_str}`"
        )

    msg = "\n".join(lines)
    _discord_post(msg)
    log.info("[TODAY] Sent daily calendar summary (%d events)", len(today_events))

# ---------------------------------------------------------------------------
# Standalone runner
# crontab: */45 * * * * cd /opt/xauapi && python -m api.news_adapter
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import argparse
    import redis as _redis

    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    parser = argparse.ArgumentParser(description="XTL News Adapter v2.1")
    parser.add_argument("--check",   action="store_true", help="Check blocks + send Discord alerts for all symbols")
    parser.add_argument("--morning", action="store_true", help="Morning rate decision check + Discord alert")
    parser.add_argument("--today",   action="store_true", help="Post today's event calendar to Discord with symbols")
    parser.add_argument("--symbol",  default=None,        help="Limit to one symbol e.g. XAUUSD")
    parser.add_argument("--shadow",  action="store_true", help="Gate check in shadow mode (no blocks)")
    parser.add_argument("--status",  action="store_true", help="Show Redis calendar status")
    args = parser.parse_args()

    REDIS_URL = os.getenv("REDIS_URL", "redis://default:xau12345@10.0.0.132:6379/0")
    _R = _redis.from_url(REDIS_URL, decode_responses=True)

    if args.status:
        print(json.dumps(get_calendar_status(_R), indent=2))

    elif args.morning:
        print("\n=== Morning Rate Check ===")
        morning_rate_check(_R)

    elif args.today:
        print("\n=== Today's Calendar (Discord) ===")
        discord_today(_R)

    elif args.check:
        print(f"\n=== Discord Block Check {'(' + args.symbol + ')' if args.symbol else '(all symbols)'} ===")
        discord_check(_R, symbol=args.symbol)

    else:
        print("\n=== Upcoming events ===")
        for ev in get_upcoming_events(_R, symbol=args.symbol):
            ts = datetime.fromtimestamp(ev["time_ms"] / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
            rate = " [RATE]" if ev.get("is_rate_decision") else ""
            print(
                f"  {ts} | {ev['currency']:4} | {ev['event']:<40} | "
                f"pre={ev['pre_block_min']}m post={ev['post_block_min']}m "
                f"stab={ev['stabilization_min']}m{rate}"
            )

        sym = args.symbol or "XAUUSD"
        print(f"\n=== Gate check {sym} (shadow={args.shadow}) ===")
        print(json.dumps(check_news_block(sym, _now_ms(), _R, shadow_mode=args.shadow), indent=2))


def get_calendar_status(R) -> dict:
    """Return calendar cache status for health check / UI."""
    try:
        raw  = R.get(REDIS_CALENDAR_KEY) if R is not None else None
        data = _json_load(raw) if raw else None
        if not isinstance(data, dict):
            return {"ok": False, "reason": "calendar_missing"}
        now_ms = _now_ms()
        age_s  = (now_ms - int(data.get("fetched_at_ms") or 0)) / 1000
        return {
            "ok":            True,
            "source":        data.get("source"),
            "events_count":  data.get("count", 0),
            "fetched_at_ms": data.get("fetched_at_ms"),
            "age_minutes":   round(age_s / 60, 1),
            "ttl_hours":     REDIS_CALENDAR_TTL // 3600,
        }
    except Exception as e:
        return {"ok": False, "reason": str(e)}
