# -*- coding: utf-8 -*-
"""
XauTrendLab — News Adapter v3.0 (MT5 canonical)
================================================

Purpose
-------
New-entry event safety gate backed only by the broker-native MT5 calendar
published by the XTL Agent.

Events are deliberately kept separate from bias/regime.  This module answers
only one production question:

    "Is it safe to open a NEW trade in this symbol right now?"

Canonical source
----------------
    XTL_Calendar_Auto / Agent
        -> POST /devices/{device_id}/mt5/calendar
        -> Redis xtl:news:calendar:mt5:*
        -> check_news_block(...)
        -> WAIT / ALLOW

ForexFactory / Investing.com scraping has been removed.  Discord/reporting
helpers below consume the same canonical MT5 calendar used by the gate.
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple
import hashlib

log = logging.getLogger("xtl.news_adapter")

# ---------------------------------------------------------------------------
# Redis keys
# ---------------------------------------------------------------------------

REDIS_MT5_CALENDAR_PATTERN = "xtl:news:calendar:mt5:*"
REDIS_BLOCK_KEY = "xtl:news:block:latest:{symbol}"
REDIS_BLOCK_TTL = 36 * 3600
REDIS_RATE_DAY_KEY = "xtl:news:rate_day:{symbol}:{date}"
REDIS_DISCORD_DEDUP = "xtl:discord:news:sent:{key}"

# Agent publishes the broker calendar about every 5 minutes.  A production
# gate must not silently use a calendar whose server receive time is stale.
MT5_CALENDAR_MAX_RECEIVE_AGE_SEC = 20 * 60
MT5_CALENDAR_WEEKEND_MAX_RECEIVE_AGE_SEC = 72 * 3600

# ---------------------------------------------------------------------------
# Traded symbols and canonical event relevance
# ---------------------------------------------------------------------------

ALL_SYMBOLS = [
    "XAUUSD",
    "EURUSD",
    "GBPUSD",
    "USDJPY",
    "USDCAD",
    "USDCHF",
]

# Event relevance is execution-risk relevance, NOT directional bias.
# Gold intentionally receives USD events only.  A CAD/JPY/EUR/GBP/CHF event
# must not automatically block XAUUSD merely because MT5 labels it HIGH.
_MT5_SYMBOL_CURRENCIES: Dict[str, set[str]] = {
    "XAUUSD": {"USD"},
    "EURUSD": {"EUR", "USD"},
    "GBPUSD": {"GBP", "USD"},
    "USDJPY": {"USD", "JPY"},
    "USDCAD": {"USD", "CAD"},
    "USDCHF": {"USD", "CHF"},
}


def _mt5_event_relevant_to_symbol(currency: str, symbol: str) -> bool:
    cur = str(currency or "").upper().strip()
    sym = str(symbol or "").upper().strip()
    return bool(
        cur
        and sym in _MT5_SYMBOL_CURRENCIES
        and cur in _MT5_SYMBOL_CURRENCIES[sym]
    )


# Keep this name for existing internal/diagnostic callers.
def _is_relevant(event_name: str, currency: str, symbol: str) -> bool:
    del event_name  # relevance is canonical currency/symbol mapping in v3
    return _mt5_event_relevant_to_symbol(currency, symbol)


# ---------------------------------------------------------------------------
# XTL event policy
# ---------------------------------------------------------------------------
#
# MT5 importance=HIGH is an input, not the final XTL gate policy.
# Only explicitly classified HIGH events can block a new entry.
#
# Tier 1 major shock/policy/inflation/labour: 30m PRE + 30m POST
# Tier 2 meaningful high macro:                15m PRE + 15m POST
# Unclassified MT5 HIGH: analytics/Discord only; no automatic gate block.
#
# There is intentionally no separate stabilization period in v3.  POST is the
# stabilization/settling protection period.  Once POST expires, existing
# market/RC/Point-A gates must re-evaluate the setup from fresh state.
# ---------------------------------------------------------------------------

_TIER1_TOKENS = (
    # Central-bank policy / press conference
    "fomc",
    "federal funds rate",
    "interest rate decision",
    "rate decision",
    "rate statement",
    "official bank rate",
    "bank rate",
    "policy rate",
    "overnight rate",
    "main refinancing rate",
    "deposit facility rate",
    "monetary policy statement",
    "press conference",
    # Inflation
    "core cpi",
    "consumer price index",
    "cpi",
    "core pce",
    "core-pce",
    "pce price index",
    # Major labour
    "non-farm payroll",
    "nonfarm payroll",
    "non-farm employment",
    "nonfarm employment",
    "nfp",
)

_TIER2_TOKENS = (
    "gdp",
    "gross domestic product",
    "retail sales",
    "durable goods",
    "consumer confidence",
    "initial jobless claims",
    "jobless claims",
    "unemployment rate",
    "employment change",
    "employment",
    "ism manufacturing",
    "ism services",
    "manufacturing pmi",
    "services pmi",
    "composite pmi",
    "pmi",
    "new home sales",
)


def _classify_mt5_gate_event(
    event_name: str,
    event_code: str = "",
) -> Optional[dict]:
    """Return XTL entry-safety policy for one MT5 HIGH event."""
    name = str(event_name or "").lower().strip()
    code = str(event_code or "").lower().strip()
    text = f"{name} {code}"

    if any(token in text for token in _TIER1_TOKENS):
        return {
            "tier": "TIER_1_MAJOR",
            "pre": 30,
            "post": 30,
            "stabilization": 0,
        }

    if any(token in text for token in _TIER2_TOKENS):
        return {
            "tier": "TIER_2_HIGH",
            "pre": 15,
            "post": 15,
            "stabilization": 0,
        }

    return None


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


def _wait_response(reason: str, window: str, shadow: bool) -> dict:
    return {
        "block": True,
        "verdict": "WAIT",
        "shadow": bool(shadow),
        "reason": reason,
        "event_name": None,
        "event_time_ms": None,
        "minutes_to_event": None,
        "impact": None,
        "window": window,
        "currency": None,
        "event_code": None,
        "event_id": None,
        "value_id": None,
        "event_tier": None,
        "calendar_source": "MT5_CALENDAR",
        "pre_block_min": None,
        "post_block_min": None,
        "stabilization_min": 0,
    }


def _calendar_owner_id(
    uid: str,
    account_server: str,
    account_login: str,
) -> str:
    material = (
        f"{str(uid or '').strip()}|"
        f"{str(account_server or '').strip().casefold()}|"
        f"{str(account_login or '').strip()}"
    )

    return hashlib.sha256(
        material.encode("utf-8")
    ).hexdigest()[:32]


def _load_mt5_gate_calendar(
    R,
    now_ms: int,
    gate_context: Optional[dict] = None,
) -> Optional[dict]:
    """
    Load the exact broker/account-owned canonical MT5 calendar
    for this execution context.

    STRICT BROKER SCOPE:
      - no Redis SCAN
      - no freshest-global calendar
      - no cross-broker fallback
      - no alternate-device fallback

    Ownership identity must match ingestion exactly:

        SHA256(
            uid
            + "|"
            + account_server.casefold()
            + "|"
            + account_login
        )[:32]

    Calendar freshness:
      - MT5 calendar is a slow scheduled-event snapshot,
        not a live market feed.
      - collected_at_ms is the content-freshness authority.
      - server_received_ms is transport/provenance only.
      - requested evaluation time must remain inside
        coverage_from_utc_ms -> coverage_to_utc_ms.

    Failure returns None. check_news_block() will then preserve
    the existing EVENT_DATA_UNAVAILABLE_BYPASS behavior.
    """

    if R is None:
        return None

    evaluation_ms = int(
        now_ms or _now_ms()
    )

    server_now_ms = _now_ms()

    ctx = (
        gate_context
        if isinstance(gate_context, dict)
        else {}
    )

    uid = str(
        ctx.get("uid") or ""
    ).strip()

    device_id = str(
        ctx.get("device_id") or ""
    ).strip()

    profile_id = str(
        ctx.get("profile_id") or ""
    ).strip()

    # ---------------------------------------------------------
    # Exact execution identity is mandatory.
    #
    # Never fall back to another broker's calendar when the
    # current execution context is incomplete.
    # ---------------------------------------------------------
    if not uid or not device_id:
        log.warning(
            "[NEWS_MT5] CALENDAR_CONTEXT_MISSING "
            "uid=%s dev=%s profile=%s",
            uid or None,
            device_id or None,
            profile_id or None,
        )
        return None

    # ---------------------------------------------------------
    # Resolve the MT5 account attached to this exact device.
    #
    # This gives us login + server, which together with uid form
    # the same stable calendar-owner identity used by ingestion.
    # ---------------------------------------------------------
    try:
        account_raw = R.get(
            f"xtl:mt5:account:{device_id}:demo"
        )

        account = _json_load(
            account_raw
        )

    except Exception as exc:
        log.warning(
            "[NEWS_MT5] ACCOUNT_LOAD_FAILED "
            "uid=%s dev=%s profile=%s err=%r",
            uid,
            device_id,
            profile_id or None,
            exc,
        )
        return None

    if not isinstance(account, dict):
        log.warning(
            "[NEWS_MT5] ACCOUNT_UNAVAILABLE "
            "uid=%s dev=%s profile=%s",
            uid,
            device_id,
            profile_id or None,
        )
        return None

    account_login = str(
        account.get("login")
        or account.get("account_login")
        or ""
    ).strip()

    account_server = str(
        account.get("server")
        or account.get("account_server")
        or ""
    ).strip()

    if not account_login or not account_server:
        log.warning(
            "[NEWS_MT5] ACCOUNT_IDENTITY_MISSING "
            "uid=%s dev=%s profile=%s "
            "login=%s server=%s",
            uid,
            device_id,
            profile_id or None,
            account_login or None,
            account_server or None,
        )
        return None

    # ---------------------------------------------------------
    # Derive EXACT same owner ID as routes_devices.py ingestion.
    # ---------------------------------------------------------
    owner_material = (
        f"{uid}|"
        f"{account_server.casefold()}|"
        f"{account_login}"
    )

    calendar_owner_id = hashlib.sha256(
        owner_material.encode("utf-8")
    ).hexdigest()[:32]

    redis_key = (
        "xtl:news:calendar:mt5:"
        f"{calendar_owner_id}"
    )

    # ---------------------------------------------------------
    # Exact Redis GET.
    #
    # NO SCAN.
    # NO selection among brokers.
    # ---------------------------------------------------------
    try:
        raw = R.get(redis_key)
        data = _json_load(raw)

    except Exception as exc:
        log.warning(
            "[NEWS_MT5] CALENDAR_GET_FAILED "
            "uid=%s dev=%s profile=%s "
            "owner=%s key=%s err=%r",
            uid,
            device_id,
            profile_id or None,
            calendar_owner_id,
            redis_key,
            exc,
        )
        return None

    if not isinstance(data, dict):
        log.warning(
            "[NEWS_MT5] CALENDAR_NOT_FOUND "
            "uid=%s dev=%s profile=%s "
            "login=%s server=%s owner=%s",
            uid,
            device_id,
            profile_id or None,
            account_login,
            account_server,
            calendar_owner_id,
        )
        return None

    # ---------------------------------------------------------
    # Canonical source validation.
    # ---------------------------------------------------------
    if (
        str(
            data.get("source") or ""
        ).upper().strip()
        != "MT5_CALENDAR"
    ):
        log.warning(
            "[NEWS_MT5] CALENDAR_SOURCE_INVALID "
            "uid=%s owner=%s source=%s",
            uid,
            calendar_owner_id,
            data.get("source"),
        )
        return None

    stored_owner_id = str(
        data.get("calendar_owner_id") or ""
    ).strip()

    if (
        stored_owner_id
        and stored_owner_id != calendar_owner_id
    ):
        log.error(
            "[NEWS_MT5] CALENDAR_OWNER_ID_MISMATCH "
            "expected=%s stored=%s "
            "uid=%s dev=%s",
            calendar_owner_id,
            stored_owner_id,
            uid,
            device_id,
        )
        return None

    # ---------------------------------------------------------
    # Defensive owner verification.
    #
    # Redis key is already deterministic, but verify the
    # embedded ownership too. This protects against accidental
    # bad writes / stale migrations.
    # ---------------------------------------------------------
    owner = (
        data.get("owner")
        if isinstance(
            data.get("owner"),
            dict,
        )
        else {}
    )

    stored_uid = str(
        owner.get("uid") or ""
    ).strip()

    stored_login = str(
        owner.get("account_login") or ""
    ).strip()

    stored_server = str(
        owner.get("account_server") or ""
    ).strip()

    if stored_uid and stored_uid != uid:
        log.error(
            "[NEWS_MT5] CALENDAR_UID_MISMATCH "
            "expected=%s stored=%s owner=%s",
            uid,
            stored_uid,
            calendar_owner_id,
        )
        return None

    if (
        stored_login
        and stored_login != account_login
    ):
        log.error(
            "[NEWS_MT5] CALENDAR_LOGIN_MISMATCH "
            "expected=%s stored=%s owner=%s",
            account_login,
            stored_login,
            calendar_owner_id,
        )
        return None

    if (
        stored_server
        and (
            stored_server.casefold()
            != account_server.casefold()
        )
    ):
        log.error(
            "[NEWS_MT5] CALENDAR_SERVER_MISMATCH "
            "expected=%s stored=%s owner=%s",
            account_server,
            stored_server,
            calendar_owner_id,
        )
        return None

    # ---------------------------------------------------------
    # Transport/provenance sanity.
    #
    # server_received_ms is NOT the weekday expiry authority.
    # ---------------------------------------------------------
    received_ms = int(
        data.get("server_received_ms") or 0
    )

    if received_ms <= 0:
        return None

    received_age_ms = (
        server_now_ms - received_ms
    )

    # Reject materially future-dated API receive timestamps.
    if received_age_ms < -180_000:
        log.warning(
            "[NEWS_MT5] CALENDAR_RECEIVED_IN_FUTURE "
            "owner=%s age_ms=%s",
            calendar_owner_id,
            received_age_ms,
        )
        return None

    # ---------------------------------------------------------
    # Calendar content freshness.
    #
    # Calendar is slow-moving scheduled data. Agent deliberately
    # does not re-POST every five minutes when file is unchanged.
    #
    # Allow up to 36 hours since the broker snapshot itself was
    # collected. This safely spans broker-midnight/reset timing
    # without accepting old multi-day snapshots.
    # ---------------------------------------------------------
    collected_ms = int(
        data.get("collected_at_ms") or 0
    )

    if collected_ms <= 0:
        return None

    collected_age_ms = (
        server_now_ms - collected_ms
    )

    if collected_age_ms < -180_000:
        log.warning(
            "[NEWS_MT5] CALENDAR_COLLECTED_IN_FUTURE "
            "owner=%s age_ms=%s",
            calendar_owner_id,
            collected_age_ms,
        )
        return None

    max_collected_age_ms = (
        36 * 60 * 60 * 1000
    )

    if (
        collected_age_ms
        > max_collected_age_ms
    ):
        log.warning(
            "[NEWS_MT5] CALENDAR_CONTENT_STALE "
            "uid=%s dev=%s profile=%s "
            "owner=%s age_hours=%.2f",
            uid,
            device_id,
            profile_id or None,
            calendar_owner_id,
            (
                collected_age_ms
                / 3_600_000.0
            ),
        )
        return None

    # ---------------------------------------------------------
    # Coverage validation.
    #
    # evaluation_ms is intentional here because replay /
    # historical analytics may evaluate another timestamp.
    # ---------------------------------------------------------
    coverage_from = int(
        data.get(
            "coverage_from_utc_ms"
        )
        or 0
    )

    coverage_to = int(
        data.get(
            "coverage_to_utc_ms"
        )
        or 0
    )

    if (
        coverage_from > 0
        and evaluation_ms < coverage_from
    ):
        log.warning(
            "[NEWS_MT5] CALENDAR_BEFORE_COVERAGE "
            "owner=%s eval=%s from=%s",
            calendar_owner_id,
            evaluation_ms,
            coverage_from,
        )
        return None

    if (
        coverage_to > 0
        and evaluation_ms > coverage_to
    ):
        log.warning(
            "[NEWS_MT5] CALENDAR_AFTER_COVERAGE "
            "owner=%s eval=%s to=%s",
            calendar_owner_id,
            evaluation_ms,
            coverage_to,
        )
        return None

    # ---------------------------------------------------------
    # Events container validation.
    #
    # An empty event list can legitimately mean there are no
    # broker calendar events in the covered period. That is a
    # valid calendar, not infrastructure failure.
    # ---------------------------------------------------------
    events = data.get("events")

    if not isinstance(events, list):
        log.warning(
            "[NEWS_MT5] CALENDAR_EVENTS_INVALID "
            "owner=%s type=%s",
            calendar_owner_id,
            type(events).__name__,
        )
        return None

    log.debug(
        "[NEWS_MT5] CALENDAR_SELECTED "
        "uid=%s dev=%s profile=%s "
        "login=%s server=%s owner=%s "
        "events=%s collected_age_h=%.2f",
        uid,
        device_id,
        profile_id or None,
        account_login,
        account_server,
        calendar_owner_id,
        len(events),
        (
            collected_age_ms
            / 3_600_000.0
        ),
    )

    return data

def _normalize_mt5_gate_events(
    calendar_data: dict,
    symbol: Optional[str] = None,
    *,
    include_unclassified: bool = False,
) -> List[dict]:
    """
    Convert canonical MT5 events into the stable news-adapter event shape.

    Production gate callers pass a symbol and include_unclassified=False.
    Discord/reporting may set include_unclassified=True to see all MT5 HIGH
    events without granting them blocking authority.
    """
    out: List[dict] = []
    sym_u = str(symbol or "").upper().strip()

    if not isinstance(calendar_data, dict):
        return out

    for src in calendar_data.get("events") or []:
        try:
            if not isinstance(src, dict):
                continue

            importance = str(src.get("importance") or "").upper().strip()
            if importance != "HIGH":
                continue

            time_mode = str(src.get("time_mode") or "").upper().strip()
            if time_mode != "DATETIME":
                continue

            currency = str(src.get("currency") or "").upper().strip()
            if sym_u and not _mt5_event_relevant_to_symbol(currency, sym_u):
                continue

            event_name = str(src.get("event_name") or "").strip()
            event_code = str(src.get("event_code") or "").strip()
            event_time_ms = int(src.get("event_time_utc_ms") or 0)

            if not event_name or event_time_ms <= 0:
                continue

            policy = _classify_mt5_gate_event(event_name, event_code)
            if not policy and not include_unclassified:
                continue

            out.append({
                "event": event_name,
                "event_code": event_code or None,
                "event_id": src.get("event_id"),
                "value_id": src.get("value_id"),
                "currency": currency,
                "impact": "HIGH",
                "time_ms": event_time_ms,
                "time_known": True,
                "pre_block_min": int(policy["pre"]) if policy else 0,
                "post_block_min": int(policy["post"]) if policy else 0,
                "stabilization_min": 0,
                "xtl_event_tier": policy["tier"] if policy else "UNCLASSIFIED_HIGH",
                "gate_classified": bool(policy),
                "calendar_source": "MT5_CALENDAR",
                "actual": src.get("actual"),
                "forecast": src.get("forecast"),
                "previous": src.get("previous"),
            })
        except Exception as exc:
            log.debug("[NEWS_MT5] normalize event failed: %s", exc)

    out.sort(key=lambda e: int(e.get("time_ms") or 0))
    return out


# ---------------------------------------------------------------------------
# Production gate
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
    Return whether a NEW entry must WAIT for a relevant MT5 calendar event.

    Existing positions are not managed here.  This function must not create,
    flip, or modify DXY/pair bias.  It is execution-safety only.
    """
    sym_u = str(symbol or "").upper().strip()
    now_ms = int(now_ms or 0)

    _allow = {
        "block": False,
        "verdict": "ALLOW",
        "shadow": bool(shadow_mode),
        "reason": None,
        "event_name": None,
        "event_time_ms": None,
        "minutes_to_event": None,
        "impact": None,
        "window": None,
        "currency": None,
        "event_code": None,
        "event_id": None,
        "value_id": None,
        "event_tier": None,
        "calendar_source": "MT5_CALENDAR",
        "pre_block_min": None,
        "post_block_min": None,
        "stabilization_min": 0,
    }

    if sym_u not in _MT5_SYMBOL_CURRENCIES:
        return {**_allow, "reason": "NEWS_SYMBOL_NOT_MANAGED"}

    calendar_data = _load_mt5_gate_calendar(R, now_ms,gate_context=gate_context,)
    if not isinstance(calendar_data, dict):
        # ---------------------------------------------------------
        # EVENT DATA IS OPTIONAL.
        #
        # Missing / stale / unsupported MT5 calendar must NEVER
        # stop the original XTL execution path.
        #
        # The event layer is an additional safety filter only.
        # When unavailable, bypass it and continue to Point-A /
        # existing execution logic exactly as XTL did before the
        # event integration.
        # ---------------------------------------------------------
        msg = "EVENT_DATA_UNAVAILABLE_BYPASS"

        log.warning(
            "[NEWS_MT5] BYPASS "
            "reason=%s symbol=%s",
            msg,
            sym_u,
        )

        return {
            **_allow,
            "verdict": "ALLOW",
            "block": False,
            "reason": msg,
            "calendar_source": None,
            "event_mode": "UNAVAILABLE_BYPASS",
            "event_name": None,
            "event_code": None,
            "event_tier": None,
            "window": None,
            "minutes_to_event": None,
        }

    gate_events = _normalize_mt5_gate_events(calendar_data, sym_u)

    for ev in gate_events:
        try:
            event_name = str(ev.get("event") or "")
            currency = str(ev.get("currency") or "")
            event_time_ms = int(ev.get("time_ms") or 0)
            pre_min = int(ev.get("pre_block_min") or 0)
            post_min = int(ev.get("post_block_min") or 0)

            if event_time_ms <= 0:
                continue

            pre_ms = pre_min * 60_000
            post_ms = post_min * 60_000
            window_start = event_time_ms - pre_ms
            window_end = event_time_ms + post_ms

            if not (window_start <= now_ms <= window_end):
                continue

            delta_ms = event_time_ms - now_ms

            if delta_ms >= 0:
                window_type = "PRE_EVENT"
                reason = (
                    f"HIGH_IMPACT_NEWS | {event_name} "
                    f"in {int(delta_ms / 60000)} min"
                )
            else:
                window_type = "POST_EVENT"
                mins_after = int(abs(delta_ms) / 60000)
                reason = (
                    f"HIGH_IMPACT_NEWS | {event_name} "
                    f"released {mins_after} min ago"
                )

            log.info(
                "[NEWS_MT5] BLOCK: %s | symbol=%s tier=%s currency=%s",
                reason,
                sym_u,
                ev.get("xtl_event_tier"),
                currency,
            )

            result = {
                "block": not shadow_mode,
                "verdict": "ALLOW" if shadow_mode else "WAIT",
                "shadow": bool(shadow_mode),
                "reason": f"SHADOW_WARN | {reason}" if shadow_mode else reason,
                "event_name": event_name,
                "event_time_ms": event_time_ms,
                "minutes_to_event": round(delta_ms / 60000, 1),
                "impact": "HIGH",
                "window": window_type,
                "currency": currency,
                "event_code": ev.get("event_code"),
                "event_id": ev.get("event_id"),
                "value_id": ev.get("value_id"),
                "event_tier": ev.get("xtl_event_tier"),
                "calendar_source": "MT5_CALENDAR",
                "pre_block_min": pre_min,
                "post_block_min": post_min,
                "stabilization_min": 0,
            }

            _store_block_snapshot(R, sym_u, result)

            if not shadow_mode:
                _insert_audit_row(db, sym_u, result, gate_context)

            return result

        except Exception as exc:
            log.debug("[NEWS_MT5] Event check error: %s", exc)

    return _allow


# ---------------------------------------------------------------------------
# Redis block snapshot + DB audit
# ---------------------------------------------------------------------------


def _store_block_snapshot(R, symbol: str, result: dict) -> None:
    if R is None:
        return
    try:
        R.set(
            REDIS_BLOCK_KEY.format(symbol=symbol),
            json.dumps(
                {**result, "stored_at_ms": _now_ms()},
                separators=(",", ":"),
                default=str,
            ),
            ex=REDIS_BLOCK_TTL,
        )
    except Exception as exc:
        log.debug("[NEWS] Snapshot write failed: %s", exc)


def _insert_audit_row(
    db,
    symbol: str,
    result: dict,
    gate_context: Optional[dict],
) -> None:
    """Insert one blocking decision into news_block_events when DB is supplied."""
    if db is None:
        return

    try:
        gc = gate_context or {}
        zone = gc.get("zone_used") or {}
        trigger = gc.get("rev_trigger") or {}
        direction = str(gc.get("resolved_dir") or gc.get("direction") or "")

        entry_trigger = None
        try:
            entry_trigger = float(
                trigger.get("entry_above")
                if direction == "BUY"
                else trigger.get("entry_below") or 0
            ) or None
        except Exception:
            pass

        row = {
            "symbol": symbol,
            "direction": direction,
            "event_name": str(result.get("event_name") or ""),
            "event_time_ms": int(result.get("event_time_ms") or 0),
            "blocked_at_ms": _now_ms(),
            "window_type": str(result.get("window") or ""),
            "zone_low": float(zone.get("low") or 0) or None,
            "zone_high": float(zone.get("high") or 0) or None,
            "entry_trigger": entry_trigger,
            "verdict": str(result.get("verdict") or "WAIT"),
            "outcome_simulated": None,
            "outcome_price": None,
        }

        db.execute(
            "INSERT INTO news_block_events ({cols}) VALUES ({vals})".format(
                cols=", ".join(row.keys()),
                vals=", ".join(f":{k}" for k in row.keys()),
            ),
            row,
        )
        db.commit()
    except Exception as exc:
        log.warning("[NEWS] DB audit insert failed: %s", exc)
        try:
            db.rollback()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Compatibility: old fetcher is intentionally disabled
# ---------------------------------------------------------------------------


def fetch_and_store_calendar(R, lookahead_hours: int = 48) -> dict:
    """
    Compatibility stub for any forgotten cron/import.

    External scraping was removed in v3.  The XTL Agent owns calendar ingest.
    """
    del R, lookahead_hours
    log.info("[NEWS_MT5] legacy external calendar fetch disabled; Agent MT5 calendar is authoritative")
    return {
        "ok": False,
        "reason": "EXTERNAL_CALENDAR_DISABLED_USE_MT5",
        "source": "MT5_CALENDAR",
    }


# ---------------------------------------------------------------------------
# Rate-decision + formatting helpers
# ---------------------------------------------------------------------------

_RATE_DECISION_TOKENS = (
    "fomc",
    "federal funds rate",
    "interest rate decision",
    "rate decision",
    "rate statement",
    "official bank rate",
    "policy rate",
    "overnight rate",
    "main refinancing rate",
    "deposit facility rate",
    "monetary policy statement",
)


def _is_rate_decision(event_name: str) -> bool:
    name = str(event_name or "").strip().lower()
    return any(token in name for token in _RATE_DECISION_TOKENS)


def _fmt_time_utc(time_ms: int) -> str:
    try:
        return datetime.fromtimestamp(
            int(time_ms) / 1000,
            tz=timezone.utc,
        ).strftime("%H:%M UTC")
    except Exception:
        return "??"


def _cb_name(event_name: str, currency: str = "") -> str:
    e = str(event_name or "").upper()
    cur = str(currency or "").upper().strip()
    if "ECB" in e or cur == "EUR":
        return "ECB"
    if "BOE" in e or "BANK OF ENGLAND" in e or cur == "GBP":
        return "BOE"
    if "BOJ" in e or "BANK OF JAPAN" in e or cur == "JPY":
        return "BOJ"
    if "SNB" in e or "SWISS" in e or cur == "CHF":
        return "SNB"
    if "BOC" in e or "BANK OF CANADA" in e or cur == "CAD":
        return "BOC"
    if "FOMC" in e or cur == "USD":
        return "Fed"
    return str(event_name or "")[:20]


# ---------------------------------------------------------------------------
# Canonical event loaders for Discord/UI/analytics
# ---------------------------------------------------------------------------


def _load_calendar_events(
    R,
    gate_context: Optional[dict] = None,
) -> List[dict]:
    """
    Load canonical MT5 HIGH events for one exact broker/account.

    No global calendar fallback.
    No cross-broker aggregation.
    """
    data = _load_mt5_gate_calendar(
        R,
        _now_ms(),
        gate_context=gate_context,
    )

    if not isinstance(data, dict):
        return []

    return _normalize_mt5_gate_events(
        data,
        symbol=None,
        include_unclassified=True,
    )


def get_upcoming_events(
    R,
    symbol: Optional[str] = None,
    hours_ahead: int = 24,
    gate_context: Optional[dict] = None,
) -> List[dict]:
    """
    Return upcoming canonical MT5 HIGH events for the exact
    broker/account represented by gate_context.

    This is the backend/UI read path.
    It uses the same canonical calendar as check_news_block().
    """
    try:
        now_ms = _now_ms()

        data = _load_mt5_gate_calendar(
            R,
            now_ms,
            gate_context=gate_context,
        )

        if not isinstance(data, dict):
            return []

        events = _normalize_mt5_gate_events(
            data,
            symbol=(
                str(symbol).upper().strip()
                if symbol
                else None
            ),
            include_unclassified=True,
        )

        cut_ms = (
            now_ms
            + int(hours_ahead) * 3_600_000
        )

        result = [
            e
            for e in events
            if (
                now_ms
                <= int(e.get("time_ms") or 0)
                <= cut_ms
            )
        ]

        result.sort(
            key=lambda x: int(
                x.get("time_ms") or 0
            )
        )

        return result

    except Exception as exc:
        log.warning(
            "[NEWS_MT5] UPCOMING_LOAD_FAILED "
            "err=%r",
            exc,
        )
        return []


def get_calendar_status(
    R,
    gate_context: Optional[dict] = None,
) -> dict:
    """
    Health/status for the exact broker-owned MT5 calendar
    used by the production entry gate.

    This is also the canonical backend status returned to UI.
    """
    try:
        now_ms = _now_ms()

        data = _load_mt5_gate_calendar(
            R,
            now_ms,
            gate_context=gate_context,
        )

        ctx = (
            gate_context
            if isinstance(gate_context, dict)
            else {}
        )

        if not isinstance(data, dict):
            return {
                "ok": False,
                "available": False,
                "reason": (
                    "mt5_calendar_missing_or_stale"
                ),
                "source": "MT5_CALENDAR",
                "profile_id": (
                    ctx.get("profile_id")
                ),
                "device_id": (
                    ctx.get("device_id")
                ),
            }

        received_ms = int(
            data.get("server_received_ms")
            or 0
        )

        collected_ms = int(
            data.get("collected_at_ms")
            or 0
        )

        all_high = _normalize_mt5_gate_events(
            data,
            symbol=None,
            include_unclassified=True,
        )

        classified = [
            e
            for e in all_high
            if e.get("gate_classified")
        ]

        owner = (
            data.get("owner")
            if isinstance(
                data.get("owner"),
                dict,
            )
            else {}
        )

        return {
            "ok": True,
            "available": True,

            "source": "MT5_CALENDAR",
            "source_of_truth": (
                "BROKER_NATIVE_MT5_CALENDAR"
            ),

            "calendar_owner_id": (
                data.get(
                    "calendar_owner_id"
                )
            ),

            "profile_id": (
                ctx.get("profile_id")
            ),

            "device_id": (
                ctx.get("device_id")
            ),

            "account_server": (
                owner.get("account_server")
            ),

            "broker_company": (
                owner.get("broker_company")
            ),

            "events_count": int(
                data.get("event_count")
                or len(
                    data.get("events")
                    or []
                )
            ),

            "high_events_count": len(
                all_high
            ),

            "gate_classified_high_count": len(
                classified
            ),

            "collected_at_ms": collected_ms,

            "collected_age_minutes": (
                round(
                    (
                        now_ms
                        - collected_ms
                    ) / 60_000,
                    1,
                )
                if collected_ms > 0
                else None
            ),

            "server_received_ms": (
                received_ms
            ),

            # Transport age for diagnostics only.
            "server_receive_age_minutes": (
                round(
                    (
                        now_ms
                        - received_ms
                    ) / 60_000,
                    1,
                )
                if received_ms > 0
                else None
            ),

            "coverage_from_utc_ms": (
                data.get(
                    "coverage_from_utc_ms"
                )
            ),

            "coverage_to_utc_ms": (
                data.get(
                    "coverage_to_utc_ms"
                )
            ),

            "timezone": (
                data.get("timezone")
            ),
        }

    except Exception as exc:
        return {
            "ok": False,
            "available": False,
            "reason": (
                f"{type(exc).__name__}:{exc}"
            ),
            "source": "MT5_CALENDAR",
        }



def get_block_snapshot(R, symbol: str) -> Optional[dict]:
    try:
        raw = (
            R.get(REDIS_BLOCK_KEY.format(symbol=str(symbol).upper().strip()))
            if R
            else None
        )
        return _json_load(raw)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Discord helpers — preserved, now backed by MT5 calendar
# ---------------------------------------------------------------------------


def _discord_webhook_url() -> str:
    return (
        os.getenv("DISCORD_WEBHOOK_URL")
        or os.getenv("XTL_DISCORD_WEBHOOK_URL")
        or ""
    ).strip()


def _discord_post(content: str) -> bool:
    url = _discord_webhook_url()
    if not url:
        log.warning("[DISCORD] DISCORD_WEBHOOK_URL not set — skipping alert")
        return False
    try:
        import urllib.request

        url = url.replace("discordapp.com", "discord.com")
        data = json.dumps({"content": content[:1900]}).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=data,
            headers={"Content-Type": "application/json", "User-Agent": "XTLBot/1.0"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=6) as resp:
            _ = resp.read()
        log.info("[DISCORD] Sent OK")
        return True
    except Exception as exc:
        log.warning("[DISCORD] Post failed: %s", exc)
        return False


def _discord_dedupe(R, key: str, ttl_sec: int = 4 * 3600) -> bool:
    if R is None:
        return True
    dk = REDIS_DISCORD_DEDUP.format(key=key)
    try:
        return bool(R.set(dk, "1", nx=True, ex=int(ttl_sec)))
    except Exception:
        return True


def _check_block_internal(symbol: str, now_ms: int, events: List[dict]) -> dict:
    """Discord-only block calculation over already-normalized events."""
    sym_u = str(symbol or "").upper().strip()
    relevant = [
        e
        for e in events
        if _mt5_event_relevant_to_symbol(e.get("currency", ""), sym_u)
    ]
    upcoming = []

    for ev in relevant:
        t_ms = int(ev.get("time_ms") or 0)
        pre_ms = int(ev.get("pre_block_min") or 0) * 60_000
        post_ms = int(ev.get("post_block_min") or 0) * 60_000
        delta_ms = t_ms - now_ms
        mins_to = delta_ms / 60_000

        if 0 < mins_to <= 120:
            upcoming.append({
                "event": ev.get("event", ""),
                "currency": ev.get("currency", ""),
                "datetime_utc": _fmt_time_utc(t_ms),
                "mins_to_event": round(mins_to, 1),
                "event_tier": ev.get("xtl_event_tier"),
                "gate_classified": bool(ev.get("gate_classified")),
                "is_rate_decision": _is_rate_decision(ev.get("event", "")),
            })

        if not ev.get("gate_classified"):
            continue

        if (t_ms - pre_ms) <= now_ms <= (t_ms + post_ms):
            return {
                "block": True,
                "verdict": "WAIT",
                "event_name": ev.get("event", ""),
                "currency": ev.get("currency", ""),
                "event_time_ms": t_ms,
                "datetime_utc": _fmt_time_utc(t_ms),
                "minutes_to_event": round(mins_to, 1),
                "window": "PRE_EVENT" if delta_ms >= 0 else "POST_EVENT",
                "event_tier": ev.get("xtl_event_tier"),
                "is_rate_decision": _is_rate_decision(ev.get("event", "")),
                "upcoming": upcoming,
            }

    upcoming.sort(key=lambda x: x["mins_to_event"])
    return {"block": False, "verdict": "ALLOW", "upcoming": upcoming}


def _alert_block_active(symbol: str, result: dict) -> str:
    ev = result.get("event_name", "?")
    dt = result.get("datetime_utc") or _fmt_time_utc(result.get("event_time_ms") or 0)
    win = result.get("window", "")
    mins = result.get("minutes_to_event") or 0
    tier = result.get("event_tier") or "HIGH"
    flag = "🔴 **RATE DECISION BLOCK**" if result.get("is_rate_decision") else "⚠️ **NEWS BLOCK**"
    timing = (
        f"Event in **{abs(mins):.0f} min** — pre-block active"
        if win == "PRE_EVENT"
        else "Event passed — post-block active"
    )
    return (
        f"{flag} — **{symbol}**\n"
        f"Event: `{ev}` | `{dt}` | `{tier}`\n"
        f"{timing}\n"
        f"Status: `BLOCKED — no new entries until window clears`"
    )


def _alert_upcoming(events_by_symbol: dict) -> str:
    lines = ["📅 **UPCOMING MT5 HIGH IMPACT NEWS**", ""]
    seen: set = set()
    for _, ev_list in events_by_symbol.items():
        for ev in ev_list:
            key = f"{ev['event']}|{ev.get('datetime_utc', '')}"
            if key in seen:
                continue
            seen.add(key)
            marker = "🔴 " if ev.get("is_rate_decision") else "⚠️ "
            gate_note = "" if ev.get("gate_classified") else " [INFO ONLY]"
            lines.append(
                f"{marker}`{ev['event']}` ({ev['currency']}) — "
                f"in **{ev['mins_to_event']:.0f} min** | "
                f"`{ev.get('datetime_utc', '')}`{gate_note}"
            )
    return "\n".join(lines)


def discord_check(R, symbol: Optional[str] = None) -> None:
    now_ms = _now_ms()
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    events = _load_calendar_events(R)
    symbols = [str(symbol).upper().strip()] if symbol else ALL_SYMBOLS

    if not events:
        _discord_post(
            f"⚠️ **XTL News Adapter** — canonical MT5 calendar unavailable at {now_utc}."
        )
        return

    blocks_active: List[Tuple[str, dict]] = []
    upcoming_by_sym: Dict[str, List[dict]] = {}

    for sym in symbols:
        result = _check_block_internal(sym, now_ms, events)
        if result["block"]:
            blocks_active.append((sym, result))
            log.warning(
                "[BLOCK] %s BLOCKED | %s | %s",
                sym,
                result.get("event_name"),
                result.get("window"),
            )
        else:
            near = [u for u in result.get("upcoming", []) if u["mins_to_event"] <= 60]
            if near:
                upcoming_by_sym[sym] = near

    for sym, result in blocks_active:
        dk = f"block:{sym}:{result.get('event_name', '')}:{result.get('window', '')}"
        if _discord_dedupe(R, dk, ttl_sec=2 * 3600):
            _discord_post(_alert_block_active(sym, result))

    if upcoming_by_sym and not blocks_active:
        first_evs = next(iter(upcoming_by_sym.values()), [{}])
        first_ev = first_evs[0] if first_evs else {}
        dk = f"upcoming:{first_ev.get('event', '')}:{first_ev.get('datetime_utc', '')}"
        if _discord_dedupe(R, dk, ttl_sec=90 * 60):
            _discord_post(_alert_upcoming(upcoming_by_sym))


def discord_today(R) -> None:
    now_utc = datetime.now(timezone.utc)
    today_str = now_utc.strftime("%Y-%m-%d")
    day_start = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end = day_start + timedelta(days=1)
    start_ms = int(day_start.timestamp() * 1000)
    end_ms = int(day_end.timestamp() * 1000)

    events = [
        e
        for e in _load_calendar_events(R)
        if start_ms <= int(e.get("time_ms") or 0) < end_ms
    ]
    events.sort(key=lambda x: int(x.get("time_ms") or 0))

    if not events:
        _discord_post(
            f"📅 **XTL News — Today ({today_str})**\n"
            "✅ No MT5 HIGH-impact events in the canonical calendar."
        )
        return

    lines = [
        f"📅 **XTL News — Today ({today_str})**",
        "─────────────────────────────────",
    ]

    for ev in events:
        name = ev.get("event", "")
        currency = ev.get("currency", "")
        affected = [
            sym
            for sym in ALL_SYMBOLS
            if _mt5_event_relevant_to_symbol(currency, sym)
        ]
        tier = ev.get("xtl_event_tier") or "UNCLASSIFIED_HIGH"
        gate_note = "" if ev.get("gate_classified") else " — info only"
        lines.append(
            f"⚠️ `{_fmt_time_utc(ev['time_ms'])}` | **{currency}** | {name}\n"
            f"   └ `{tier}{gate_note}` | Affects: `{', '.join(affected) or 'none'}`"
        )

    _discord_post("\n".join(lines))


def morning_rate_check(R, events: Optional[List[dict]] = None) -> None:
    """Preserved Discord/day-flag helper, now using canonical MT5 events."""
    if events is None:
        events = _load_calendar_events(R)

    now_utc = datetime.now(timezone.utc)
    today = now_utc.strftime("%Y-%m-%d")
    day_start = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end = day_start + timedelta(days=1)
    start_ms = int(day_start.timestamp() * 1000)
    end_ms = int(day_end.timestamp() * 1000)

    rate_events = [
        e
        for e in events
        if _is_rate_decision(e.get("event", ""))
        and start_ms <= int(e.get("time_ms") or 0) < end_ms
    ]

    if not rate_events:
        log.info("[RATE_CHECK] No rate decisions on UTC day=%s", today)
        return

    for ev in rate_events:
        affected = [
            sym
            for sym in ALL_SYMBOLS
            if _mt5_event_relevant_to_symbol(ev.get("currency", ""), sym)
        ]
        for sym in affected:
            key = REDIS_RATE_DAY_KEY.format(symbol=sym, date=today)
            payload = {
                "event": ev.get("event", ""),
                "time_ms": ev.get("time_ms", 0),
                "currency": ev.get("currency", ""),
                "cb_name": _cb_name(ev.get("event", ""), ev.get("currency", "")),
                "pre_block_min": ev.get("pre_block_min", 30),
                "post_block_min": ev.get("post_block_min", 30),
                "stabilization_min": 0,
                "source": "MT5_CALENDAR",
            }
            try:
                R.setex(key, 24 * 3600, json.dumps(payload, separators=(",", ":")))
            except Exception as exc:
                log.warning("[RATE_CHECK] Redis set failed %s: %s", key, exc)


# ---------------------------------------------------------------------------
# Optional standalone diagnostics / Discord runner
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    import redis as _redis

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="XTL News Adapter v3.0 — MT5 canonical")
    parser.add_argument("--check", action="store_true", help="Discord block/upcoming check")
    parser.add_argument("--morning", action="store_true", help="Rate-decision day check")
    parser.add_argument("--today", action="store_true", help="Discord today's MT5 HIGH calendar")
    parser.add_argument("--symbol", default=None, help="Limit to one symbol")
    parser.add_argument("--shadow", action="store_true", help="Gate check in shadow mode")
    parser.add_argument("--status", action="store_true", help="Show MT5 calendar status")
    args = parser.parse_args()

    redis_url = os.getenv(
        "REDIS_URL",
        "redis://default:xau12345@10.0.0.132:6379/0",
    )
    R = _redis.from_url(redis_url, decode_responses=True)

    if args.status:
        print(json.dumps(get_calendar_status(R), indent=2))
    elif args.morning:
        morning_rate_check(R)
    elif args.today:
        discord_today(R)
    elif args.check:
        discord_check(R, symbol=args.symbol)
    else:
        sym = args.symbol or "XAUUSD"
        print(json.dumps(check_news_block(sym, _now_ms(), R, shadow_mode=args.shadow), indent=2))
