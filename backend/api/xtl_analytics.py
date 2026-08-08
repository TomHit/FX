#!/usr/bin/env python3
"""
XTL Trade Analytics Engine - Phase 1 (Option B: H1-bar exit approximation)
==========================================================================
Captures every trade's FROZEN entry context + an approximated exit into a
permanent JSONL learning database. NO agent change, NO Windows risk.

Captures BOTH entry paths:
  - clean        : normal pipeline trade, snapshot fires at ACK (ticket lands)
  - broker_repair: reconstructed trade, snapshot fires at creation (ticket present)
Each tagged via `entry_provenance` so repaired trades (partial entry context) can
be filtered out of entry-quality studies while still counting for win/loss.

Contract (locked):
  - Snapshot written when mt5_ticket is known (irreplaceable entry context).
  - Close detected by polling: ticket absent from live open-positions set.
  - Orphan sweep mandatory.
  - Redis = in-flight lifecycle ; JSONL = permanent history.
  - Analytics NEVER blocks trading: every public fn swallows its own errors.

Exit precision is approximate now (h1_bar_approx / medium), exact later
(mt5_deal_history / high). Both `exit_source` and `exit_confidence` are stored so
pandas can separate or compare the two qualities of data.
"""

import json
import os
import time
import logging
import fcntl
import stat
import tempfile

from contextlib import contextmanager

log = logging.getLogger("xtl.analytics")

SNAP_PREFIX    = "xtl:analytics:trade:"
DEAL_WAIT_MS   = 180_000   # wait up to 3 min for the broker deal before approximating (fixes finalize-vs-deal race)
SNAP_TTL_SEC   = 14 * 24 * 3600
JSONL_PATH     = "/opt/xauapi/api/trend/out/trades.jsonl"
JSONL_LOCK_PATH = JSONL_PATH + ".lock"
PENDING_TRUTH_KEY = "xtl:analytics:pending_truth"
ORPHAN_AGE_MS  = 10 * 60 * 1000
SCHEMA_VERSION = "2.0"

# First close-sweep after process start performs pending-set reseed and broker
# truth reconciliation before any absence-based close detection. This makes
# startup recovery self-contained even when the caller has no dedicated startup
# hook. The tri-state broker snapshot guard still prevents cold-start/outage
# snapshots from being treated as verified empty.
_ANALYTICS_STARTUP_RECON_DONE = False

# Live entry timestamps (now_ms) are TRUE UTC, so offset 0. (The historical parquet
# needed +3 because it was broker-encoded; the live clock is not.) VERIFY once against
# a known entry before trusting session analysis, then adjust here if needed.
LIVE_TZ_OFFSET_H = 0.0

# DXY M15 shadow analytics wiring. These keys are produced by the unified
# REAL_DXY / SYNTHETIC_DXY detector. Analytics reads them only; it never
# changes the detector or blocks/modifies a trade.
DXY_M15_SOURCES = ("REAL_DXY", "SYNTHETIC_DXY")

DXY_M15_STATE_PREFIX = "xtl:dxy:turn:state:M15"
DXY_M15_HISTORY_PREFIX = "xtl:dxy:turn:history:M15"
DXY_M15_MAX_TRACKED_EVENTS = 200
DXY_M15_STATE_FRESH_MS = 90 * 60 * 1000

# Phase-1 REAL DXY M15 extreme-impulse classification.  Shadow analytics only:
# it records that a new entry would ideally WAIT during an abnormal directional
# USD candle, but this module never blocks/modifies trading.  Thresholds were
# calibrated against the current canonical FTMO REAL DXY sample (286 completed
# M15 bars): range >=1.75 ATR, body >=1.50 ATR, directional body >=70% of range.
DXY_M15_EXTREME_RANGE_ATR = float(os.getenv(
    "XTL_DXY_M15_EXTREME_RANGE_ATR",
    "1.75",
))
DXY_M15_EXTREME_BODY_ATR = float(os.getenv(
    "XTL_DXY_M15_EXTREME_BODY_ATR",
    "1.50",
))
DXY_M15_EXTREME_BODY_RATIO = float(os.getenv(
    "XTL_DXY_M15_EXTREME_BODY_RATIO",
    "0.70",
))
DXY_M15_EXTREME_SNAPSHOT_FRESH_MS = int(os.getenv(
    "XTL_DXY_M15_EXTREME_SNAPSHOT_FRESH_MS",
    str(30 * 60 * 1000),
))
# DXY H1 directional-feature analytics.
#
# Produced by api/dxy_h1_features.py. Read-only here:
# this never changes entry direction, gates, scoring, risk, or execution.
DXY_H1_SOURCES = (
    "REAL_DXY",
    "SYNTHETIC_DXY",
)

DXY_H1_FEATURE_PREFIX = "xtl:dxy:features:H1"
DXY_H1_LATEST_PREFIX = "xtl:dxy:features:latest:H1"

# Entry capture may use the most recently completed H1 bar only when it
# was reasonably fresh at the trade-entry timestamp.
DXY_H1_FEATURE_FRESH_MS = int(
    os.getenv(
        "XTL_DXY_H1_FEATURE_FRESH_MS",
        str(2 * 60 * 60 * 1000),
    )
)

DXY_CANONICAL_H1_FRESH_MS = int(os.getenv(
    "XTL_DXY_CANONICAL_H1_FRESH_MS",
    str(2 * 60 * 60 * 1000),
))
DXY_CANONICAL_SR_FRESH_MS = int(os.getenv(
    "XTL_DXY_CANONICAL_SR_FRESH_MS",
    str(2 * 60 * 60 * 1000),
))

# Canonical REAL DXY source shared across all prop-firm trade devices.
# The publisher remains device-specific; readers resolve this one authoritative
# FTMO device and use same-trade-device SYNTHETIC_DXY only as a fallback.
DXY_CANONICAL_CONFIG_KEY = os.getenv(
    "XTL_DXY_CANONICAL_CONFIG_KEY",
    "xtl:dxy:canonical",
)
# Optional operator pin. When empty, the resolver automatically selects the
# freshest healthy REAL_DXY publisher and pins it in Redis.
DXY_CANONICAL_REAL_DEVICE_CONFIGURED = os.getenv(
    "XTL_CANONICAL_REAL_DXY_DEVICE_ID",
    "",
).strip()
DXY_CANONICAL_PIN_TTL_SEC = int(os.getenv(
    "XTL_DXY_CANONICAL_PIN_TTL_SEC",
    str(7 * 24 * 60 * 60),
))

# Static drift table from the 18-month profiler. Only |reliab|>=2 combos are reliable;
# everything else is noise and never flags against_drift. Regenerate monthly.
DRIFT_TABLE = {
    "XAUUSD": {"Asia":   {"signed": 4.3, "reliab": 2.56}},
    "USDCAD": {"London": {"signed": 2.1, "reliab": 2.24}},
}


# Live per-trade milestone analytics (shadow-only; never changes orders).
MILESTONE_LEVELS = (
    ("r_025", 0.25),
    ("r_050", 0.50),
    ("r_075", 0.75),
    ("r_100", 1.00),
    ("r_150", 1.50),
    ("r_200", 2.00),
)
MILESTONE_UPDATE_MIN_INTERVAL_MS = int(os.getenv(
    "XTL_ANALYTICS_MILESTONE_UPDATE_MIN_MS",
    "5000",
))
MILESTONE_DXY_SAMPLE_INTERVAL_MS = int(os.getenv(
    "XTL_ANALYTICS_MILESTONE_DXY_SAMPLE_MS",
    "60000",
))
MILESTONE_MAX_DXY_EVENTS = 100
MILESTONE_MAX_EXIT_CANDIDATES = 50


# -- tiny helpers -------------------------------------------------------------

@contextmanager
def _trades_jsonl_lock():
    """Exclusive advisory lock shared by _append_jsonl and the reconciler."""
    lf = open(JSONL_LOCK_PATH, "a+", encoding="utf-8")   
    try:
        fcntl.flock(lf.fileno(), fcntl.LOCK_EX)
        yield
    finally:
        try:
            fcntl.flock(lf.fileno(), fcntl.LOCK_UN)
        finally:
            lf.close()



def _now_ms() -> int:
    return int(time.time() * 1000)


def _pip(symbol: str) -> float:
    s = (symbol or "").upper()
    if s == "XAUUSD" or s.endswith("JPY"):
        return 0.01
    return 0.0001


def _safe_float(x, d=None):
    try:
        if x is None:
            return d
        return float(x)
    except Exception:
        return d


def _safe_int(x, default=None):
    try:
        if x is None or x == "":
            return default
        return int(float(x))
    except Exception:
        return default
def _json_load(value, default=None):
    if default is None:
        default = {}

    try:
        if value is None:
            return default

        if isinstance(value, (bytes, bytearray)):
            value = value.decode("utf-8", "ignore")

        obj = json.loads(value) if isinstance(value, str) else value

        if isinstance(obj, str):
            obj = json.loads(obj)

        return obj

    except Exception:
        return default



def _new_trade_milestone_state() -> dict:
    return {
        "schema_version": 1,
        "analytics_only": True,
        "initialized_at_ms": _now_ms(),
        "milestones": {
            name: {
                "target_r": target,
                "reached": False,
                "first_reached_ms": None,
                "first_reached_price": None,
                "dxy_snapshot": None,
            }
            for name, target in MILESTONE_LEVELS
        },
        "state": {
            "current_r": None,
            "current_price": None,
            "last_update_ms": None,
            "max_r_seen": None,
            "max_r_seen_ms": None,
            "max_r_seen_price": None,
            "min_r_seen": None,
            "min_r_seen_ms": None,
            "min_r_seen_price": None,
            "highest_milestone_reached": None,
            "giveback_from_max_r": None,
            "lowest_r_after_050": None,
            "lowest_r_after_075": None,
            "lowest_r_after_100": None,
            "returned_to_entry_after_050": False,
            "returned_to_entry_after_075": False,
            "returned_to_entry_after_100": False,
            "returned_below_050_after_100": False,
        },
        "dxy_events": [],
        "dxy_last_signature": None,
        "dxy_last_sample_ms": None,
        "exit_candidates": [],
        "exit_candidate_keys": [],
    }


def _ensure_trade_milestone_state(snap: dict) -> dict:
    current = snap.get("trade_milestone_history")
    if not isinstance(current, dict):
        current = _new_trade_milestone_state()
        snap["trade_milestone_history"] = current

    current.setdefault("schema_version", 1)
    current.setdefault("analytics_only", True)
    current.setdefault("milestones", {})
    current.setdefault("state", {})
    current.setdefault("dxy_events", [])
    current.setdefault("exit_candidates", [])
    current.setdefault("exit_candidate_keys", [])

    for name, target in MILESTONE_LEVELS:
        current["milestones"].setdefault(name, {
            "target_r": target,
            "reached": False,
            "first_reached_ms": None,
            "first_reached_price": None,
            "dxy_snapshot": None,
        })

    return current


def analyze_liquidity_target_during_trade(
    snap: dict,
    bars_h1: list,
) -> dict:
    """
    Analyze whether the trade reached the opposing entry-frozen
    liquidity pool before exiting.

    BUY  -> target = entry-time BSL
    SELL -> target = entry-time SSL

    Only bars inside the actual trade lifetime are considered.
    Pure analytics only.
    """

    out = {
        "target_liquidity_type": None,
        "target_liquidity_level": None,
        "target_liquidity_touched": False,
        "target_liquidity_touch_bar_ms": None,
        "tp_beyond_target_liquidity": False,
        "target_liquidity_distance_r": None,
    }

    try:
        side = str(
            snap.get("side") or ""
        ).upper().strip()

        entry = _safe_float(
            snap.get("entry_price")
        )
        sl = _safe_float(
            snap.get("sl_price")
        )
        tp = _safe_float(
            snap.get("tp_price")
        )

        if side == "BUY":
            target = _safe_float(
                snap.get("bsl_level")
            )
            out["target_liquidity_type"] = "BSL"

        elif side == "SELL":
            target = _safe_float(
                snap.get("ssl_level")
            )
            out["target_liquidity_type"] = "SSL"

        else:
            return out

        if (
            entry is None
            or entry <= 0
            or sl is None
            or sl <= 0
            or target is None
            or target <= 0
        ):
            return out

        # Reject a liquidity level that is on the wrong side
        # of the entry price.
        if side == "BUY" and target <= entry:
            return out

        if side == "SELL" and target >= entry:
            return out

        out["target_liquidity_level"] = target

        risk_distance = abs(
            entry - sl
        )

        if risk_distance > 0:
            out["target_liquidity_distance_r"] = round(
                abs(target - entry) / risk_distance,
                3,
            )

        if tp is not None and tp > 0:
            if side == "BUY":
                out["tp_beyond_target_liquidity"] = bool(
                    tp > target
                )
            else:
                out["tp_beyond_target_liquidity"] = bool(
                    tp < target
                )

        entry_ms = _norm_ms(
            snap.get("broker_open_time_utc_ms")
            or snap.get("enqueue_timestamp")
            or 0
        )

        close_ms = _norm_ms(
            snap.get("broker_close_time_utc_ms")
            or snap.get("close_timestamp")
            or 0
        )

        for bar in bars_h1 or []:
            if not isinstance(bar, dict):
                continue

            # Prefer close time for lifetime filtering because an H1
            # bar can begin before entry but still contain post-entry price.
            bar_open_ms = _norm_ms(
                bar.get("t_open_ms")
                or bar.get("t")
                or 0
            )

            bar_close_ms = _norm_ms(
                bar.get("t_close_ms")
                or 0
            )

            if not bar_close_ms and bar_open_ms:
                bar_close_ms = (
                    bar_open_ms + 3_600_000
                )

            # Skip bars that finished before the trade opened.
            if (
                entry_ms
                and bar_close_ms
                and bar_close_ms <= entry_ms
            ):
                continue

            # Skip bars that opened after the trade closed.
            if (
                close_ms
                and bar_open_ms
                and bar_open_ms >= close_ms
            ):
                continue

            high = _safe_float(
                bar.get("h")
            )
            low = _safe_float(
                bar.get("l")
            )

            if high is None or low is None:
                continue

            touched = False

            if side == "BUY":
                touched = high >= target
            else:
                touched = low <= target

            if touched:
                out["target_liquidity_touched"] = True
                out["target_liquidity_touch_bar_ms"] = (
                    bar_open_ms
                    or bar_close_ms
                    or None
                )
                break

    except Exception as exc:
        log.warning(
            "analytics: liquidity target analysis failed "
            "ticket=%s err=%r",
            snap.get("mt5_ticket"),
            exc,
        )

    return out

def read_news_day_context(symbol: str, ts_ms: int) -> dict:
    out = {
        "news_day_has_high_impact": False,
        "news_day_events": [],
        "nearest_news_event": None,
        "nearest_news_time_ms": None,
        "nearest_news_distance_minutes": None,
        "nearest_news_relation": None,
    }

    try:
        R = from_app_R()
        raw = R.get("xtl:news:calendar:daily")
        if not raw:
            return out

        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", "ignore")

        data = json.loads(raw)
        events = data.get("events") if isinstance(data, dict) else []

        if not isinstance(events, list):
            return out

        currencies = _currencies_for_symbol(symbol)

        from datetime import datetime, timezone

        entry_day = datetime.fromtimestamp(
            int(ts_ms) / 1000,
            timezone.utc,
        ).date()

        matches = []

        for event in events:
            event_ms = int(event.get("time_ms") or 0)
            if event_ms <= 0:
                continue

            currency = str(event.get("currency") or "").upper()
            impact = str(event.get("impact") or "").upper()

            if currency not in currencies or impact != "HIGH":
                continue

            event_day = datetime.fromtimestamp(
                event_ms / 1000,
                timezone.utc,
            ).date()

            if event_day != entry_day:
                continue

            minutes_delta = round((event_ms - int(ts_ms)) / 60000, 1)

            if minutes_delta > 0:
                relation = "NEWS_IN_FUTURE"      # trade entered before the news
            elif minutes_delta < 0:
                relation = "NEWS_ALREADY_OCCURRED"       # trade entered after the news
            else:
                relation = "NEWS_NOW"

            matches.append({
                "event": event.get("event"),
                "currency": currency,
                "impact": impact,
                "time_ms": event_ms,

                # Always positive
                "distance_minutes": abs(minutes_delta),

                # Human readable
                "relation": relation,

                "pre_block_min": event.get("pre_block_min"),
                "post_block_min": event.get("post_block_min"),
                "stabilization_min": event.get("stabilization_min"),
            })

        matches.sort(
            key=lambda event: float(
                event.get("distance_minutes") or 0
            )
        )

        out["news_day_events"] = matches
        out["news_day_has_high_impact"] = bool(matches)

        if matches:
            nearest = matches[0]

            out["nearest_news_event"] = nearest.get("event")
            out["nearest_news_time_ms"] = nearest.get("time_ms")
            out["nearest_news_distance_minutes"] = nearest.get(
                "distance_minutes"
            )
            out["nearest_news_relation"] = nearest.get("relation")

    except Exception as exc:
        log.warning(
            "analytics: news day context failed symbol=%s err=%s",
            symbol,
            exc,
        )

    return out

def _session_for_ts_ms(ts_ms, tz_offset_h=0.0) -> str:
    try:
        corrected = int(ts_ms) - int(tz_offset_h * 3_600_000)
        h = (corrected // 3_600_000) % 24
        if 7 <= h < 12:  return "London"
        if 12 <= h < 16: return "Overlap"
        if 16 <= h < 21: return "NY_late"
        return "Asia"
    except Exception:
        return "Asia"


def _extract_ticket(p: dict):
    t = p.get("mt5_ticket")
    if t in (None, "", 0):
        ack = p.get("mt5_ack") if isinstance(p.get("mt5_ack"), dict) else {}
        res = ack.get("result") if isinstance(ack.get("result"), dict) else {}
        t = res.get("ticket")
    return t if t not in (None, "", 0) else None


def _bar_ms_any(b: dict) -> int:
    """Bar time in ms. This feed writes epoch SECONDS in 't'."""
    for k in ("t_close_ms", "t_open_ms", "t", "time"):
        v = b.get(k)
        if v:
            try:
                n = int(v)
            except (TypeError, ValueError):
                continue
            return n * 1000 if n < 100_000_000_000 else n
    return 0

def _drift_lookup(sym: str, session: str, side: str) -> dict:
    rec = (DRIFT_TABLE.get((sym or "").upper()) or {}).get(session)
    if not rec:
        return {"signed": None, "reliab": None, "direction": None, "against": False}
    signed = rec.get("signed"); reliab = rec.get("reliab")
    direction = "BUY" if (signed or 0) > 0 else "SELL"
    against = (abs(reliab or 0) >= 2.0) and (str(side or "").upper() != direction)
    return {"signed": signed, "reliab": reliab, "direction": direction, "against": against}


# -- host wiring (lazy imports so this module loads cleanly / no circular import) -
def from_app_R():
    from api.trend_endpoints import R
    return R


def _redis_key_text(value) -> str:
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", "ignore")
    return str(value or "")


def _dxy_real_device_health(R, device_id: str, now_ms: int | None = None) -> dict:
    """Return health/freshness for one REAL_DXY publisher.

    Canonical eligibility requires a fresh M15 REAL_DXY detector state
    and a fresh raw broker DXY H1 snapshot. The derived H1 trend state
    is retained for diagnostics only and does not determine eligibility.
    """
    dev = str(device_id or "").strip()
    now = int(now_ms or _now_ms())
    out = {
        "device_id": dev or None,
        "healthy": False,
        "m15_available": False,
        "h1_available": False,
        "m15_fresh": False,
        "h1_fresh": False,
        "m15_freshness_ms": 0,
        "h1_freshness_ms": 0,
        "m15_age_ms": None,
        "h1_age_ms": None,
        "freshness_ms": 0,
    }
    if not dev or R is None:
        return out

    def _state_freshness(state: dict) -> int:
        if not isinstance(state, dict):
            return 0
        # Prefer when the detector actually processed/published the state.
        for field in (
            "detected_at_ms",
            "updated_at_ms",
            "published_at_ms",
            "captured_at_ms",
            "server_received_ms",
        ):
            value = _safe_int(state.get(field), 0) or 0
            if value > 0:
                return value * 1000 if value < 10_000_000_000 else value
        # Fallback to normalized UTC bar identity.
        value = _safe_int(
            state.get("last_evaluated_bar_close_ms")
            or state.get("bar_close_ms"),
            0,
        ) or 0
        return value * 1000 if 0 < value < 10_000_000_000 else value

    try:
        m15 = _json_load(
            R.get(
                f"{DXY_M15_STATE_PREFIX}:"
                f"REAL_DXY:{dev}"
            ),
            {},
        )

        h1_state = _json_load(
            R.get(
                f"xtl:dxy:state:H1:"
                f"REAL_DXY:{dev}"
            ),
            {},
        )

        h1_raw = _json_load(
            R.get(
                f"xtl:ohlc:snap:"
                f"{dev}:DXY:H1"
            ),
            {},
        )

        if not isinstance(m15, dict):
            m15 = {}

        if not isinstance(h1_state, dict):
            h1_state = {}

        if not isinstance(h1_raw, dict):
            h1_raw = {}

        # -----------------------------------------
        # M15 health comes from the derived detector
        # state because the M15 tracker is already
        # responsible for confirming REAL_DXY.
        # -----------------------------------------
        m15_ms = _state_freshness(m15)

        # -----------------------------------------
        # H1 canonical health must come from the raw
        # broker snapshot, not the derived H1 state.
        #
        # Requiring the H1 state here creates a
        # bootstrap deadlock:
        #   canonical -> H1 state -> canonical
        # -----------------------------------------
        h1_raw_ms = _safe_int(
            h1_raw.get("server_received_ms")
            or h1_raw.get("received_at_ms")
            or h1_raw.get("published_at_ms"),
            0,
        ) or 0

        if 0 < h1_raw_ms < 10_000_000_000:
            h1_raw_ms *= 1000

        # Keep derived H1-state freshness for
        # diagnostics only. It must not determine
        # canonical source eligibility.
        h1_state_ms = _state_freshness(
            h1_state
        )

        m15_age = (
            max(0, now - m15_ms)
            if m15_ms > 0
            else None
        )

        h1_raw_age = (
            max(0, now - h1_raw_ms)
            if h1_raw_ms > 0
            else None
        )

        h1_state_age = (
            max(0, now - h1_state_ms)
            if h1_state_ms > 0
            else None
        )

        m15_fresh = bool(
            m15_ms
            and m15_age is not None
            and m15_age
            <= DXY_M15_STATE_FRESH_MS
        )

        h1_raw_fresh = bool(
            h1_raw_ms
            and h1_raw_age is not None
            and h1_raw_age
            <= DXY_CANONICAL_H1_FRESH_MS
        )

        h1_state_fresh = bool(
            h1_state_ms
            and h1_state_age is not None
            and h1_state_age
            <= DXY_CANONICAL_H1_FRESH_MS
        )

        out.update({
            "m15_available": bool(m15),

            # Canonical H1 availability is based on
            # the broker snapshot.
            "h1_available": bool(h1_raw),

            "m15_freshness_ms": m15_ms,
            "h1_freshness_ms": h1_raw_ms,

            "m15_age_ms": m15_age,
            "h1_age_ms": h1_raw_age,

            "m15_fresh": m15_fresh,
            "h1_fresh": h1_raw_fresh,

            # Additional diagnostics for the derived
            # H1 tracker state.
            "h1_state_available": bool(
                h1_state
            ),
            "h1_state_freshness_ms": (
                h1_state_ms
            ),
            "h1_state_age_ms": h1_state_age,
            "h1_state_fresh": h1_state_fresh,

            "h1_health_source": (
                "RAW_BROKER_SNAPSHOT"
            ),

            "freshness_ms": (
                min(m15_ms, h1_raw_ms)
                if m15_ms and h1_raw_ms
                else 0
            ),
        })

        out["healthy"] = bool(
            m15_fresh
            and h1_raw_fresh
        )

    except Exception as exc:
        out["health_error"] = (
            f"{type(exc).__name__}:{exc}"
        )

    return out



def _discover_freshest_real_dxy_device(
    R,
    now_ms: int | None = None,
) -> dict:
    now = int(now_ms or _now_ms())
    devices: set[str] = set()

    # Existing derived M15 state discovery.
    try:
        prefix = f"{DXY_M15_STATE_PREFIX}:REAL_DXY:"
        for key in R.scan_iter(f"{prefix}*", count=200):
            key_s = _redis_key_text(key)
            if key_s.startswith(prefix):
                dev = key_s[len(prefix):].strip()
                if dev:
                    devices.add(dev)
    except Exception:
        pass

    # NEW: discover directly from raw REAL DXY snapshots.
    try:
        raw_prefix = "xtl:ohlc:snap:"
        raw_suffix = ":DXY:M15"

        for key in R.scan_iter(
            "xtl:ohlc:snap:*:DXY:M15",
            count=200,
        ):
            key_s = _redis_key_text(key)

            if (
                key_s.startswith(raw_prefix)
                and key_s.endswith(raw_suffix)
            ):
                dev = key_s[
                    len(raw_prefix):
                    -len(raw_suffix)
                ].strip()

                if dev:
                    devices.add(dev)
    except Exception:
        pass

    candidates = [
        _dxy_real_device_health(R, dev, now)
        for dev in sorted(devices)
    ]

    healthy = [
        item
        for item in candidates
        if item.get("healthy")
    ]

    healthy.sort(
        key=lambda item: (
            int(item.get("freshness_ms") or 0),
            str(item.get("device_id") or ""),
        ),
        reverse=True,
    )

    return {
        "selected": healthy[0] if healthy else None,
        "candidates_checked": len(candidates),
        "healthy_candidates": len(healthy),
    }

def resolve_canonical_dxy_source(
    R,
    trade_device_id: str | None = None,
    *,
    trade_firm: str | None = None,
    trade_profile_id: str | None = None,
) -> dict:
    """Resolve one stable canonical REAL_DXY source for every prop firm.

    Priority:
      1. Keep the Redis-pinned canonical device while it remains healthy.
      2. Use the optional environment-configured device when healthy.
      3. Auto-select the freshest healthy REAL_DXY publisher and pin it.
      4. If none is healthy, callers may use same-trade-device SYNTHETIC_DXY.

    This prevents source flapping: a different device is selected only when the
    current canonical device is stale or unavailable.
    """
    trade_dev = str(trade_device_id or "").strip()
    trade_firm_u = str(trade_firm or "").strip().lower()
    trade_profile = str(trade_profile_id or "").strip().lower()
    now = _now_ms()
    config_error = None
    pinned_payload = {}

    # 1. Keep the existing Redis pin while healthy.
    try:
        raw = R.get(DXY_CANONICAL_CONFIG_KEY) if R is not None else None
        pinned_payload = _json_load(raw, {}) if raw else {}
        if not isinstance(pinned_payload, dict):
            pinned_payload = {}
        pinned_source = str(pinned_payload.get("source") or "REAL_DXY").upper().strip()
        pinned_dev = str(
            pinned_payload.get("device_id")
            or pinned_payload.get("real_device_id")
            or ""
        ).strip()
        if pinned_source == "REAL_DXY" and pinned_dev:
            health = _dxy_real_device_health(R, pinned_dev, now)
            if health.get("healthy"):
                return {
                    "source": "REAL_DXY",

                    "trade_device_id": trade_dev or None,
                    "trade_firm": trade_firm_u or None,
                    "trade_profile_id": trade_profile or None,

                    "real_device_id": pinned_dev,
                    "real_firm": str(
                        pinned_payload.get("firm")
                        or pinned_payload.get("real_firm")
                        or ""
                    ).strip().lower() or None,
                    "real_profile_id": str(
                        pinned_payload.get("profile_id")
                        or pinned_payload.get("real_profile_id")
                        or ""
                    ).strip().lower() or None,

                    "synthetic_device_id": trade_dev or None,
                    "synthetic_firm": trade_firm_u or None,
                    "synthetic_profile_id": trade_profile or None,

                    "configured_by": "REDIS_PIN_HEALTHY",
                    "selection_reason": "KEEP_HEALTHY_CANONICAL",
                    "config_key": DXY_CANONICAL_CONFIG_KEY,
                    "config_error": None,
                    "fallback_policy": "SAME_TRADE_DEVICE_SYNTHETIC_ONLY",
                    "real_health": health,
                }
    except Exception as exc:
        config_error = f"CANONICAL_CONFIG_READ_ERROR:{type(exc).__name__}"

    # 2. Optional explicit operator pin, but only when healthy.
    configured_dev = str(DXY_CANONICAL_REAL_DEVICE_CONFIGURED or "").strip()
    selected = None
    configured_by = None
    selection_reason = None
    candidates_checked = 0
    healthy_candidates = 0

    if configured_dev:
        health = _dxy_real_device_health(R, configured_dev, now)
        if health.get("healthy"):
            selected = health
            configured_by = "ENV_HEALTHY"
            selection_reason = "CONFIGURED_REAL_DXY_HEALTHY"
        else:
            config_error = "CONFIGURED_REAL_DXY_STALE_OR_UNAVAILABLE"

    # 3. Automatic discovery when no healthy configured source exists.
    if selected is None:
        discovery = _discover_freshest_real_dxy_device(R, now)
        selected = discovery.get("selected")
        candidates_checked = int(discovery.get("candidates_checked") or 0)
        healthy_candidates = int(discovery.get("healthy_candidates") or 0)
        if selected:
            configured_by = "AUTO_DISCOVERY"
            selection_reason = "FRESHEST_HEALTHY_REAL_DXY"

    real_dev = str((selected or {}).get("device_id") or "").strip()

    # Pin the selected source so all workers and prop firms use the same device.
    if real_dev:
        try:
            payload = {
                "source": "REAL_DXY",
                "device_id": real_dev,
                # Current canonical REAL DXY is supplied by the FTMO terminal.
                # This is descriptive analytics metadata only.
                "firm": "ftmo",
                "profile_id": None,
                "selected_at_ms": int(now),
                "last_verified_ms": int(now),
                "selection_reason": selection_reason,
                "configured_by": configured_by,
            }
            R.set(
                DXY_CANONICAL_CONFIG_KEY,
                json.dumps(payload, separators=(",", ":")),
                ex=max(60, int(DXY_CANONICAL_PIN_TTL_SEC)),
            )
        except Exception as exc:
            config_error = (
                config_error
                or f"CANONICAL_CONFIG_WRITE_ERROR:{type(exc).__name__}"
            )

    return {
        "source": "REAL_DXY" if real_dev else None,

        "trade_device_id": trade_dev or None,
        "trade_firm": trade_firm_u or None,
        "trade_profile_id": trade_profile or None,

        "real_device_id": real_dev or None,
        "real_firm": (
            "ftmo"
            if real_dev
            else None
        ),
        "real_profile_id": None,

        "synthetic_device_id": trade_dev or None,
        "synthetic_firm": trade_firm_u or None,
        "synthetic_profile_id": trade_profile or None,

        "configured_by": configured_by or "NONE",
        "selection_reason": selection_reason or "NO_FRESH_REAL_DXY",
        "config_key": DXY_CANONICAL_CONFIG_KEY,
        "config_error": config_error,
        "fallback_policy": "SAME_TRADE_DEVICE_SYNTHETIC_ONLY",
        "fallback_required": not bool(real_dev),
        "real_health": selected,
        "candidates_checked": candidates_checked,
        "healthy_candidates": healthy_candidates,
    }


def _open_tickets(
    uid: str,
    profile_id: str,
    account_type: str = "demo",
) -> set | None:
    """Return broker-open tickets with explicit unknown state.

    None means the broker snapshot could not be verified. An empty set means
    the broker snapshot was successfully read and contains no open positions.
    The distinction prevents an agent outage/cold start from finalizing every
    analytics snapshot as closed.
    """
    try:
        from api.trend_endpoints import (
            _live_broker_tickets_for_prop,
        )

        uid_u = str(uid or "").strip()
        pid = str(profile_id or "").strip().lower()

        if not uid_u or not pid:
            log.error(
                "analytics: open-ticket snapshot unverified "
                "uid=%r profile=%r reason=MISSING_OWNER",
                uid,
                profile_id,
            )
            return None

        result = _live_broker_tickets_for_prop(
            uid_u,
            pid,
            account_type,
        )

        # The provider must return an actual collection. None means no verified
        # broker snapshot was available; never coerce it to an empty set.
        if result is None:
            log.warning(
                "analytics: open-ticket snapshot unverified "
                "uid=%s profile=%s reason=PROVIDER_RETURNED_NONE",
                uid_u,
                pid,
            )
            return None

        return {str(ticket) for ticket in result if str(ticket or "").strip()}

    except Exception as e:
        log.error(
            "analytics: cannot verify open tickets "
            "uid=%s profile=%s err=%s",
            uid,
            profile_id,
            e,
        )
        return None

def _resolve_bar_device(
    symbol: str,
    prefer: str = None,
    *,
    allow_scan_fallback: bool = True,
) -> str:
    """
    Resolve the OHLC device for a symbol.

    Priority:
      1. Explicit preferred trade/execution device, when it has H1 bars.
      2. Optional fallback: choose the freshest matching H1 snapshot.

    Never select the first Redis SCAN result because stale/retired devices
    may still retain OHLC keys.
    """
    try:
        R = from_app_R()
        sym = str(symbol or "").upper().strip()
        preferred = str(prefer or "").strip()

        if not sym:
            return ""

        # -------------------------------------------------
        # 1. Strict preferred-device path.
        # This is the normal broker-confirmed entry path.
        # -------------------------------------------------
        if preferred:
            preferred_key = f"xtl:ohlc:snap:{preferred}:{sym}:H1"

            if R.exists(preferred_key):
                return preferred

            if not allow_scan_fallback:
                log.warning(
                    "analytics: preferred OHLC device missing "
                    "symbol=%s device=%s key=%s",
                    sym,
                    preferred,
                    preferred_key,
                )
                return ""

        if not allow_scan_fallback:
            return ""

        # -------------------------------------------------
        # 2. Diagnostic/legacy fallback.
        # Select the freshest snapshot, not the first SCAN key.
        # -------------------------------------------------
        best_device = ""
        best_freshness_ms = -1

        for key in R.scan_iter(
            f"xtl:ohlc:snap:*:{sym}:H1",
            count=200,
        ):
            key_s = (
                key.decode("utf-8", "ignore")
                if isinstance(key, (bytes, bytearray))
                else str(key)
            )

            parts = key_s.split(":")
            if len(parts) < 6:
                continue

            device = parts[3]
            raw = R.get(key_s)
            if not raw:
                continue

            try:
                if isinstance(raw, (bytes, bytearray)):
                    raw = raw.decode("utf-8", "ignore")

                snap = json.loads(raw)

                if isinstance(snap, str):
                    snap = json.loads(snap)

                if not isinstance(snap, dict):
                    continue

                freshness_ms = 0

                for field in (
                    "server_received_ms",
                    "received_at_ms",
                    "published_at_ms",
                    "updated_ts_ms",
                    "lastClosedTs",
                    "last_closed_ts",
                ):
                    value = _safe_int(
                        snap.get(field),
                        0,
                    ) or 0

                    if 0 < value < 10_000_000_000:
                        value *= 1000

                    freshness_ms = max(
                        freshness_ms,
                        int(value),
                    )

                # Fall back to newest bar timestamp when the snapshot
                # does not carry an ingest/publish timestamp.
                bars = snap.get("bars")
                if isinstance(bars, list) and bars:
                    bar_ms = _bar_ms_any(
                        bars[-1]
                        if isinstance(bars[-1], dict)
                        else {}
                    )
                    freshness_ms = max(
                        freshness_ms,
                        int(bar_ms or 0),
                    )

                if freshness_ms > best_freshness_ms:
                    best_freshness_ms = freshness_ms
                    best_device = device

            except Exception:
                continue

        if best_device:
            log.warning(
                "analytics: OHLC device fallback "
                "symbol=%s preferred=%s selected=%s freshness_ms=%s",
                sym,
                preferred or "-",
                best_device,
                best_freshness_ms,
            )

        return best_device

    except Exception as exc:
        log.warning(
            "analytics: _resolve_bar_device failed "
            "symbol=%s preferred=%s err=%s",
            symbol,
            prefer,
            exc,
        )
        return str(prefer or "").strip()
def read_regime_at_ack(symbol: str, device_id: str):
    """H1+H4+D1 regime recomputed live at the entry moment. None on failure.
    Resolves the bar-storing device (which may differ from the trade's device)."""
    try:
        if not symbol:
            return None
        from api.trend_endpoints import _get_closed_h1_bars, _get_closed_h4_bars
        from api.liq_structure import detect_regime
        dev = _resolve_bar_device(
            symbol,
            device_id,
            allow_scan_fallback=False,
        )
        if not dev:
            return None
        h1 = _get_closed_h1_bars(symbol, dev) or []
        h4 = _get_closed_h4_bars(symbol, dev) or []
        if not h1 and not h4:
            return None
        return detect_regime(h1, h4)
    except Exception as e:
        log.warning("analytics: regime read failed for %s: %s", symbol, e)
        return None


def read_sr_at_ack(symbol: str) -> dict:
    """SR levels from the cached bundle (cheap GET, no engine recompute).
    Pulls nearest + best-scored support/resistance, plus bundle atr/price."""
    out = {"best_resistance": None, "best_support": None,
           "nearest_resistance": None, "nearest_support": None,
           "sr_atr": None, "sr_price": None}
    try:
        if not symbol:
            return out
        R = from_app_R()
        sym = symbol.upper()
        raw = R.get(f"xtl:sr:bundle:last_good:{sym}") or R.get(f"xtl:sr:bundle:last:{sym}")
        if not raw:
            return out
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", "ignore")
        d = json.loads(raw)
        out["nearest_resistance"] = _level_of(d.get("nearest_resistance"))
        out["nearest_support"]    = _level_of(d.get("nearest_support"))
        out["best_resistance"]    = _best_scored(d.get("active_resistances"))
        out["best_support"]       = _best_scored(d.get("active_supports"))
        out["sr_atr"]   = _safe_float(d.get("atr"))
        out["sr_price"] = _safe_float(d.get("price"))
        return out
    except Exception as e:
        log.warning("analytics: SR read failed for %s: %s", symbol, e)
        return out


def _level_of(x):
    if isinstance(x, dict):
        return _safe_float(x.get("level") or x.get("price") or x.get("value"))
    return _safe_float(x)


def _best_scored(levels):
    """Pick the highest-scored level from an active_supports/resistances list."""
    try:
        if not isinstance(levels, list) or not levels:
            return None
        def _score(z):
            return _safe_float((z or {}).get("sr_score") or (z or {}).get("score"), 0.0) or 0.0
        best = max(levels, key=_score)
        return _level_of(best)
    except Exception:
        return None


def _atr_from_bars(bars: list, n: int = 14) -> float:
    """ATR(14) from o/h/l/c bars (entry-frozen normalizer). 0.0 if too few bars."""
    try:
        if not bars or len(bars) < n + 1:
            return 0.0
        trs = []
        for i in range(1, len(bars)):
            h = _safe_float(bars[i].get("h")); l = _safe_float(bars[i].get("l"))
            pc = _safe_float(bars[i - 1].get("c"))
            if h is None or l is None or pc is None:
                continue
            trs.append(max(h - l, abs(h - pc), abs(l - pc)))
        if len(trs) < n:
            return 0.0
        atr = sum(trs[:n]) / n
        for tr in trs[n:]:
            atr = (atr * (n - 1) + tr) / n
        return round(float(atr), 6)
    except Exception:
        return 0.0


def read_liquidity_at_ack(symbol, side, zone, device_id, entry_price):
    """Liquidity model + score + sweep flag, recomputed live at entry (no cache dep).
    Mirrors read_regime_at_ack: resolve device, fetch bars, call detect_liq_signals,
    derive the three flat fields the analysis needs. Returns dict; safe."""
    out = {"liquidity_model": None, "liquidity_score": None, "sweep_detected": None, "atr": None}
    try:
        if not symbol:
            return out
        from api.trend_endpoints import _get_closed_h1_bars, _get_closed_h4_bars
        from api.liq_structure import detect_liq_signals
        dev = _resolve_bar_device(
            symbol,
            device_id,
            allow_scan_fallback=False,
        )
        if not dev:
            return out
        h1 = _get_closed_h1_bars(symbol, dev) or []
        h4 = _get_closed_h4_bars(symbol, dev) or []
        atr = _atr_from_bars(h1)
        out["atr"] = atr or None
        if not h1 and not h4:
            return out
        zdict = zone if isinstance(zone, dict) else None
        res = detect_liq_signals(symbol, (side or ""), zdict, h1, h4,
                                 float(entry_price or 0.0), float(atr or 0.0)) or {}
        signals = res.get("signals") or []
        # liquidity_model: most salient signal label present (sweep/OB/FVG/BSL/SSL)
        out["liquidity_model"] = (signals[0] if signals else None)
        # liquidity_score: map the confidence string -> number, else count of signals
        conf = str(res.get("liq_confidence") or "").upper()
        conf_map = {"HIGH": 3, "MED": 2, "MEDIUM": 2, "LOW": 1}
        out["liquidity_score"] = conf_map.get(conf, len(signals) if signals else 0)
        # sweep_detected: any sweep evidence in bsl_ssl / signals
        bsl_ssl = res.get("bsl_ssl") or {}
        _ = bsl_ssl  # exposed below
        swept = False
        try:
            for k in ("bsl", "ssl"):
                v = bsl_ssl.get(k)
                if isinstance(v, dict) and (v.get("candles_since_sweep") is not None or v.get("swept")):
                    swept = True
        except Exception:
            pass
        if not swept:
            swept = any("SWEEP" in str(s).upper() or "SWEPT" in str(s).upper() for s in signals)
        out["sweep_detected"] = bool(swept)
        out["_detail"]  = res.get("liq_detail") or {}
        out["_bsl_ssl"] = bsl_ssl
        return out
    except Exception as e:
        log.warning("analytics: liquidity read failed for %s: %s", symbol, e)
        return out


def read_shadow_bias_at_ack(
    symbol: str,
    side: str,
    device_id: str,
    entry_price: float,
    entry_zone: dict | None = None,
    computed_ms: int | None = None,
) -> dict:
    """
    Compute and freeze the XTL Evidence Bias payload at broker-confirmed entry.

    Shadow analytics only:
      - no Redis writes
      - no gate/watch changes
      - no order/risk effects
      - any failure returns an UNKNOWN payload and never raises

    H1/H4 bars, SR bundle, liquidity and regime are all read at the same
    capture moment so the stored evidence is internally consistent.
    """
    sym = str(symbol or "").upper().strip()
    executed_side = str(side or "").upper().strip()

    fallback = {
        "bias_engine_version": None,
        "symbol": sym,
        "shadow_bias": "UNKNOWN",
        "shadow_bias_score": 0.0,
        "shadow_bias_confidence": "NONE",
        "shadow_bias_data_ok": False,
        "shadow_bias_actionable": False,
        "shadow_bias_actionability_reason": "CAPTURE_NOT_AVAILABLE",
        "shadow_bias_relation": "UNKNOWN",
        "executed_side": executed_side or None,
        "computed_ms": int(computed_ms or _now_ms()),
        "data_errors": ["BIAS_CAPTURE_NOT_AVAILABLE"],
    }

    try:
        if not sym:
            fallback["data_errors"] = ["BIAS_CAPTURE_MISSING_SYMBOL"]
            return fallback

        px = _safe_float(entry_price)
        if px is None or px <= 0:
            fallback["data_errors"] = ["BIAS_CAPTURE_INVALID_ENTRY_PRICE"]
            return fallback

        from api.trend_endpoints import (
            _get_closed_h1_bars,
            _get_closed_h4_bars,
        )
        from api.liq_structure import (
            detect_liq_signals,
            detect_regime,
        )
        from api.shadow_bias import (
            BIAS_ENGINE_VERSION,
            compute_shadow_bias,
        )

        dev = _resolve_bar_device(
            sym,
            device_id,
            allow_scan_fallback=False,
        )
        if not dev:
            fallback["bias_engine_version"] = BIAS_ENGINE_VERSION
            fallback["data_errors"] = ["BIAS_CAPTURE_NO_BAR_DEVICE"]
            return fallback

        bars_h1 = _get_closed_h1_bars(sym, dev) or []
        bars_h4 = _get_closed_h4_bars(sym, dev) or []

        # -------------------------------------------------
        # Load the complete SR evidence bundle.
        # Do not use read_sr_at_ack() here because it intentionally
        # flattens the bundle to four scalar levels.
        # -------------------------------------------------
        sr_bundle = {}
        try:
            R = from_app_R()
            raw_sr = (
                R.get(f"xtl:sr:bundle:last_good:{sym}")
                or R.get(f"xtl:sr:bundle:last:{sym}")
            )

            if raw_sr:
                if isinstance(raw_sr, (bytes, bytearray)):
                    raw_sr = raw_sr.decode("utf-8", "ignore")

                sr_bundle = json.loads(raw_sr)

                # Handle an accidentally double-encoded JSON value safely.
                if isinstance(sr_bundle, str):
                    sr_bundle = json.loads(sr_bundle)

                if not isinstance(sr_bundle, dict):
                    sr_bundle = {}

        except Exception as exc:
            log.warning(
                "analytics: shadow bias SR read failed for %s: %s",
                sym,
                exc,
            )
            sr_bundle = {}

        atr_h1 = _atr_from_bars(bars_h1)

        zone_for_liq = (
            dict(entry_zone)
            if isinstance(entry_zone, dict)
            else None
        )

        liquidity_context = {}
        try:
            liquidity_context = detect_liq_signals(
                sym,
                executed_side,
                zone_for_liq,
                bars_h1,
                bars_h4,
                float(px),
                float(atr_h1 or 0.0),
            ) or {}

            if not isinstance(liquidity_context, dict):
                liquidity_context = {}

        except Exception as exc:
            log.warning(
                "analytics: shadow bias liquidity read failed for %s: %s",
                sym,
                exc,
            )
            liquidity_context = {}

        regime_context = {}
        try:
            regime_context = detect_regime(
                bars_h1,
                bars_h4,
            ) or {}

            if not isinstance(regime_context, dict):
                regime_context = {}

        except Exception as exc:
            log.warning(
                "analytics: shadow bias regime read failed for %s: %s",
                sym,
                exc,
            )
            regime_context = {}

        result = compute_shadow_bias(
            symbol=sym,
            bars_h1=bars_h1,
            bars_h4=bars_h4,
            price=float(px),
            sr_bundle=sr_bundle,
            liquidity_context=liquidity_context,
            regime_context=regime_context,
            executed_side=executed_side,
            computed_ms=int(computed_ms or _now_ms()),
        )

        if not isinstance(result, dict):
            fallback["bias_engine_version"] = BIAS_ENGINE_VERSION
            fallback["data_errors"] = ["BIAS_ENGINE_NON_DICT_RESULT"]
            return fallback

        # Capture provenance for future forensic/replay work.
        result["capture_source"] = "xtl_analytics_entry_ack"
        result["bar_device_id"] = dev
        result["input_bar_counts"] = {
            "h1": len(bars_h1),
            "h4": len(bars_h4),
        }
        result["input_entry_price"] = float(px)
        result["input_executed_side"] = executed_side or None
        result["input_zone"] = (
            dict(entry_zone)
            if isinstance(entry_zone, dict)
            else None
        )

        return result

    except Exception as exc:
        log.warning(
            "analytics: shadow bias capture failed for %s: %s",
            sym,
            exc,
        )

        fallback["shadow_bias_actionability_reason"] = "CAPTURE_EXCEPTION"
        fallback["data_errors"] = [
            f"BIAS_CAPTURE_EXCEPTION:{type(exc).__name__}:{exc}"
        ]
        return fallback

def default_fetch_h1_bars(symbol: str, start_ms: int, end_ms: int, device_id: str = None):
    """Sweep adapter: recent closed H1 bars for the symbol's device."""
    try:
        if not symbol or not device_id:
            return []
        from api.trend_endpoints import _get_closed_h1_bars
        return _get_closed_h1_bars(symbol, device_id) or []
    except Exception as e:
        log.warning("analytics: default_fetch_h1_bars failed for %s: %s", symbol, e)
        return []


def default_fetch_m1_bars(
    symbol: str,
    start_ms: int,
    end_ms: int,
    device_id: str = None,
) -> list:
    """
    Load completed broker M1 bars from the exact trade device.

    Redis timestamps are broker-wall epoch seconds in `t`.
    Filtering to the trade lifetime happens inside the excursion calculator.
    """
    try:
        if not symbol or not device_id:
            return []

        R = from_app_R()

        key = (
            f"xtl:ohlc:snap:"
            f"{str(device_id).strip()}:"
            f"{str(symbol).upper().strip()}:M1"
        )

        raw = R.get(key)

        if not raw:
            return []

        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode(
                "utf-8",
                "ignore",
            )

        payload = json.loads(raw)

        if isinstance(payload, str):
            payload = json.loads(payload)

        bars = (
            payload.get("bars")
            if isinstance(payload, dict)
            else []
        )

        if not isinstance(bars, list):
            return []

        result = []

        for bar in bars:
            if (
                not isinstance(bar, dict)
                or bar.get("complete") is False
            ):
                continue

            open_ms = _norm_ms(
                bar.get("t_open_ms")
                or bar.get("t")
                or bar.get("time")
                or 0
            )

            if open_ms <= 0:
                continue

            row = dict(bar)
            row["t_open_ms"] = int(open_ms)
            row["t_close_ms"] = int(
                open_ms + 60_000
            )
            row["complete"] = True

            result.append(row)

        result.sort(
            key=lambda row: int(
                row.get("t_open_ms") or 0
            )
        )

        return result

    except Exception as exc:
        log.warning(
            "analytics: default_fetch_m1_bars "
            "failed symbol=%s device=%s err=%r",
            symbol,
            device_id,
            exc,
        )
        return []

# -- Phase-F  13: reversal-candle OHLC reconstructed from H1 bar at rc_open_ms -
def read_reversal_candle(symbol: str, device_id: str, pos: dict) -> dict:
    """Multi-source RC capture with provenance. Resolves rc_open_ms from the first
    available source (pos -> entry_gate.rev_state -> live watch -> touch fallback),
    records WHERE it came from (rc_source), then reconstructs OHLC from the H1 bar
    at that timestamp if the source didn't already carry high/low/open/close.
    Never assumes rc_open_ms exists; on total miss returns rc_source='missing'."""
    rc = {"rc_found": False, "rc_source": "missing", "rc_open_ms": None,
          "rc_open": None, "rc_high": None, "rc_low": None, "rc_close": None,
          "rc_body_pct": None, "rc_size_pips": None, "rc_direction": None,
          "rc_capture_note": "missing"}
    try:
        p = pos if isinstance(pos, dict) else {}

        # The broker-entry payload is the only authoritative description of the
        # RC that actually triggered this trade.  Use it only when complete;
        # older/incomplete payloads continue through the pre-existing fallback
        # chain below.  This is analytics-only and never changes gate state.
        ec = p.get("entry_confirmation")
        if isinstance(ec, dict):
            ec_rc = {
                "rc_open_ms": ec.get("rc_open_ms") or ec.get("open_ms"),
                "rc_open": _safe_float(ec.get("rc_open") if ec.get("rc_open") is not None else ec.get("open")),
                "rc_high": _safe_float(ec.get("rc_high") if ec.get("rc_high") is not None else ec.get("high")),
                "rc_low": _safe_float(ec.get("rc_low") if ec.get("rc_low") is not None else ec.get("low")),
                "rc_close": _safe_float(ec.get("rc_close") if ec.get("rc_close") is not None else ec.get("close")),
            }
            if ec_rc["rc_open_ms"] and None not in (
                ec_rc["rc_open"], ec_rc["rc_high"],
                ec_rc["rc_low"], ec_rc["rc_close"],
            ):
                rc.update(ec_rc)
                rc["rc_open_ms"] = int(ec_rc["rc_open_ms"])
                rc["rc_found"] = True
                rc["rc_source"] = "entry_confirmation"
                rc["rc_capture_note"] = "authoritative_entry_time_rc"
                o, h, l, c = (rc["rc_open"], rc["rc_high"], rc["rc_low"], rc["rc_close"])
                rng = (h - l) or 0.0
                rc["rc_size_pips"] = round(rng / _pip(symbol), 1) if rng else 0.0
                rc["rc_body_pct"] = round(abs(c - o) / rng * 100.0, 1) if rng else 0.0
                rc["rc_direction"] = "BULL" if c > o else ("BEAR" if c < o else "DOJI")
                for key in ("rc_shifted_before_entry", "rc_shifted"):
                    if key in ec:
                        rc["rc_shifted_before_entry"] = bool(ec.get(key))
                        break
                return rc

        sources = [("pos", p)]
        eg = p.get("entry_gate")
        if isinstance(eg, dict):
            rev = eg.get("rev_state")
            if isinstance(rev, dict):
                sources.append(("entry_gate.rev_state", rev))
        # live watch, if it still exists
        wk = str(p.get("watch_key") or "")
        if not wk:
            sym_u = (symbol or "").upper(); side = str(p.get("side") or "").upper()
            tf = p.get("entry_zone_tf") or "H1"
            if sym_u and side:
                wk = f"xtl:zone:watch:{sym_u}:{side}:{tf}"
        if wk:
            try:
                R = from_app_R()
                raw = R.get(wk)
                if raw:
                    if isinstance(raw, (bytes, bytearray)):
                        raw = raw.decode("utf-8", "ignore")
                    w = json.loads(raw)
                    if isinstance(w, dict):
                        sources.append(("watch", w))
            except Exception:
                pass

        chosen = None; chosen_src = None
        for name, s in sources:
            ts = (s.get("rc_open_ms") or s.get("rev_ok_bar_open_ms")
                  or s.get("touch_candle_open_ms") or s.get("touch_open_ms"))
            if ts:
                chosen = s; chosen_src = name
                rc["rc_open_ms"] = int(ts)
                # direct OHLC if the source carries it
                rc["rc_high"]  = _safe_float(s.get("rc_high") or s.get("rev_ok_bar_hi"))
                rc["rc_low"]   = _safe_float(s.get("rc_low")  or s.get("rev_ok_bar_lo"))
                rc["rc_open"]  = _safe_float(s.get("rc_open"))
                rc["rc_close"] = _safe_float(s.get("rc_close"))
                rc["rc_source"] = name
                note = "touch_fallback" if name == "pos" and not (s.get("rc_open_ms") or s.get("rev_ok_bar_open_ms")) else name
                rc["rc_capture_note"] = note
                rc["rc_found"] = True
                break

        if not rc["rc_found"]:
            return rc

        # reconstruct OHLC from the H1 bar at rc_open_ms if not supplied by source
        if None in (rc["rc_open"], rc["rc_high"], rc["rc_low"], rc["rc_close"]):
            dev = _resolve_bar_device(
                symbol,
                device_id,
                allow_scan_fallback=False,
            )
            if dev:
                from api.trend_endpoints import _get_closed_h1_bars
                bars = _get_closed_h1_bars(symbol, dev) or []
                bar = None
                H1_MS = 3_600_000
                _target = int(rc["rc_open_ms"] or 0)

                def _open_ms(b):
                    o = int(b.get("t_open_ms") or 0)
                    if o:
                        return o
                    c = int(b.get("t_close_ms") or 0)
                    return (c - H1_MS) if c else 0

                bar = None
                if _target:
                    for b in bars:
                        if _open_ms(b) == _target:
                            bar = b
                            break
                    if bar is None:
                        cands = [b for b in bars if _open_ms(b) and _open_ms(b) <= _target]
                        bar = max(cands, key=_open_ms) if cands else None
                if bar:
                    rc["rc_open"]  = _safe_float(bar.get("o"))
                    rc["rc_high"]  = _safe_float(bar.get("h"))
                    rc["rc_low"]   = _safe_float(bar.get("l"))
                    rc["rc_close"] = _safe_float(bar.get("c"))
                    rc["rc_capture_note"] = f"{rc.get('rc_capture_note')}:h1_reconstructed"

        # derive body/size/direction if we have OHLC
        o, h, l, c = rc["rc_open"], rc["rc_high"], rc["rc_low"], rc["rc_close"]
        if None not in (o, h, l, c):
            rng = (h - l) or 0.0
            rc["rc_size_pips"] = round(rng / _pip(symbol), 1) if rng else 0.0
            rc["rc_body_pct"]  = round(abs(c - o) / rng * 100.0, 1) if rng else 0.0
            rc["rc_direction"] = "BULL" if c > o else ("BEAR" if c < o else "DOJI")
        return rc
    except Exception as e:
        log.warning("analytics: reversal_candle read failed for %s: %s", symbol, e)
        return rc


# -- Phase-F  14: full liquidity breakdown from liq_detail/bsl_ssl ------------
def read_liquidity_detail(liq_res: dict, side: str) -> dict:
    """Extract EQH/EQL, session liquidity, swept flags, sweep direction from the
    detect_liq_signals result (already computed in read_liquidity_at_ack)."""
    out = {"equal_highs": None, "equal_lows": None, "liquidity_pool_count": None,
           "session_liquidity": None, "sweep_direction": None,
           "bsl_level": None, "ssl_level": None}
    try:
        det = (liq_res or {}).get("_detail") or {}
        eqs = det.get("eq_levels") or []
        eqh = [e for e in eqs if "EQH" in str(e.get("type","")).upper() or "BSL" in str(e.get("type","")).upper()]
        eql = [e for e in eqs if "EQL" in str(e.get("type","")).upper() or "SSL" in str(e.get("type","")).upper()]
        out["equal_highs"] = len(eqh) or None
        out["equal_lows"]  = len(eql) or None
        sess = det.get("session") or []
        out["liquidity_pool_count"] = (len(eqs) + len(sess)) or None
        out["session_liquidity"] = [
            {"level": s.get("level"), "session": s.get("session"),
             "swept": s.get("swept"), "swept_by": s.get("swept_by")}
            for s in sess[:6]
        ] or None
        bs = (liq_res or {}).get("_bsl_ssl") or {}
        bsl = bs.get("bsl") if isinstance(bs.get("bsl"), dict) else {}
        ssl = bs.get("ssl") if isinstance(bs.get("ssl"), dict) else {}
        out["bsl_level"] = _safe_float(bsl.get("level"))
        out["ssl_level"] = _safe_float(ssl.get("level"))
        # sweep direction: which side was swept
        bsl_swept = bool(bsl.get("swept")); ssl_swept = bool(ssl.get("swept"))
        if bsl_swept and not ssl_swept: out["sweep_direction"] = "BSL"   # buy-side taken
        elif ssl_swept and not bsl_swept: out["sweep_direction"] = "SSL" # sell-side taken
        elif bsl_swept and ssl_swept: out["sweep_direction"] = "BOTH"
    except Exception as e:
        log.warning("analytics: liquidity_detail failed: %s", e)
    return out


# -- setup quality flags   testable risk hypotheses frozen at entry -----------
# NOTE: these are HYPOTHESES to validate against outcomes, not proven rules.
# 1H matters as REGIME (reversals need RANGE; TREND runs zones over).
# 4H matters as DIRECTION (fading against a strong higher-TF trend is risky).
QFLAG_WEAK_ZONE_SCORE   = 10.0   # zone_score below this = weak
QFLAG_LOW_LIQ_SCORE     = 2      # liquidity_score at/below this = thin
QFLAG_4H_ADX_TREND      = 25.0   # 4H ADX above this = meaningful trend


def _regime_dir_from(label_er_adx: dict, price_side_hint=None):
    """Rough 4H trend DIRECTION proxy: we only have label/er/adx, not slope, so we
    can't know up/down from regime alone. Return None (direction unknown) unless a
    caller supplies it. Kept simple: we flag 'strong 4H trend present', and the
    direction test is applied by the caller using zone_kind as a proxy."""
    return None


def compute_setup_quality_flags(snap_like: dict) -> list:
    """Compute risk flags from already-captured fields. Frozen at entry so a closed
    trade self-documents which quality concerns were present. Never raises."""
    flags = []
    try:
        d = snap_like or {}
        side = str(d.get("side") or "").upper()

        # -- 1H regime check: reversals want RANGE; TREND runs zones over --
        r1 = d.get("regime_1h") or {}
        if isinstance(r1, dict) and str(r1.get("label") or "").upper() == "TREND":
            flags.append("1h_trending")            # zone likely run over (bad, any dir)

        # -- 4H direction check: fading against a strong 4H trend is the real veto --
        r4 = d.get("regime_4h") or {}
        if isinstance(r4, dict) and str(r4.get("label") or "").upper() == "TREND":
            adx4 = _safe_float(r4.get("adx"), 0.0) or 0.0
            if adx4 >= QFLAG_4H_ADX_TREND:
                # direction proxy: a reversal SELL fades at resistance (betting price
                # falls)   risky if 4H trend is UP; a BUY fades at support   risky if
                # 4H trend is DOWN. We lack 4H slope here, so flag the STRONG-4H-trend
                # condition and let analysis confirm direction correlation.
                flags.append("strong_4h_trend")
                # zone_kind gives a weak direction hint: fading resistance in a strong
                # 4H uptrend, or support in a strong 4H downtrend, is the classic trap.
                zk = str(d.get("zone_kind") or "").lower()
                if (side == "SELL" and "resist" in zk) or (side == "BUY" and "support" in zk):
                    flags.append("reversal_vs_strong_4h")

        # -- zone quality --
        zs = _safe_float(d.get("zone_score"))
        if zs is not None and zs < QFLAG_WEAK_ZONE_SCORE:
            flags.append("weak_zone")

        # -- liquidity / sweep --
        ls = _safe_float(d.get("liquidity_score"))
        if ls is not None and ls <= QFLAG_LOW_LIQ_SCORE:
            flags.append("low_liquidity")
        if d.get("sweep_detected") is False:
            flags.append("no_sweep")

        # -- against reliable drift (already computed) --
        if d.get("against_drift") is True:
            flags.append("against_drift")

        # -- entered far from zone (context; XAUUSD reads in 0.01 units) --
        dz = _safe_float(d.get("dist_to_zone_pips"))
        if dz is not None and dz > 20 and str(d.get("symbol") or "").upper() != "XAUUSD":
            flags.append("far_from_zone")

    except Exception as e:
        log.warning("analytics: quality flags failed: %s", e)
    return flags


# -- Phase-F  11: news_block context (shadow read   observes, never blocks) ---
def read_news_at_ack(symbol: str, ts_ms: int) -> dict:
    """Record whether this entry sat near a scheduled high-impact event, using the
    canonical check_news_block() in SHADOW mode (reports, does not block). Captures
    the news context the trade was taken under - answers later 'do near-news entries
    underperform?' Never raises"""
    out = {"news_block": None, "news_verdict": None, "news_event": None,
           "news_minutes_to_event": None, "news_window": None, "news_impact": None}
    try:
        if not symbol:
            return out
        from api.news_adapter import check_news_block
        R = from_app_R()
        res = check_news_block(symbol, int(ts_ms or _now_ms()), R, shadow_mode=True) or {}
        out["news_block"]            = bool(res.get("block"))
        out["news_verdict"]          = res.get("verdict")
        out["news_event"]            = res.get("event_name")
        out["news_minutes_to_event"] = res.get("minutes_to_event")
        out["news_window"]           = res.get("window")
        out["news_impact"]           = res.get("impact")
        # Freeze upcoming same-currency high-impact events (next 48h) so the
        # during-trade check at close is TTL-proof (calendar may expire by then).
        out["upcoming_events"] = _freeze_currency_events(symbol, int(ts_ms or _now_ms()))
    except Exception as e:
        log.warning("analytics: news read failed for %s: %s", symbol, e)
    return out


def _currencies_for_symbol(symbol: str) -> set:
    s = (symbol or "").upper()
    out = set()
    if len(s) >= 6:
        out.add(s[:3]); out.add(s[3:6])
    return out


def _freeze_currency_events(symbol: str, now_ms: int, horizon_h: int = 48) -> list:
    """Snapshot upcoming high-impact events for THIS symbol's currencies, so the
    during-trade news check at finalize doesn't depend on the calendar TTL."""
    frozen = []
    try:
        R = from_app_R()
        raw = R.get("xtl:news:calendar:daily")
        if not raw:
            return frozen
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", "ignore")
        data = json.loads(raw)
        evs = data.get("events") if isinstance(data, dict) else None
        if not isinstance(evs, list):
            return frozen
        ccys = _currencies_for_symbol(symbol)
        horizon = now_ms + horizon_h * 3_600_000
        for e in evs:
            cur = str(e.get("currency") or "").upper()
            t = int(e.get("time_ms") or 0)
            if cur in ccys and now_ms <= t <= horizon:
                frozen.append({
                    "event": e.get("event"), "currency": cur, "time_ms": t,
                    "impact": e.get("impact"),
                    "pre_block_min": e.get("pre_block_min"),
                    "post_block_min": e.get("post_block_min"),
                    "stabilization_min": e.get("stabilization_min"),
                })
    except Exception as e:
        log.warning("analytics: freeze events failed for %s: %s", symbol, e)
    return frozen


# -- Phase-F: prop-risk snapshot + account snapshot --------------------------
def read_ftmo_state_at_ack(
    uid: str,
    profile_id: str,
) -> dict:
    """
    Freeze the latest authoritative prop-risk snapshot.

    Analytics is a read-only consumer:
      - no risk recomputation;
      - no broker reconciliation;
      - no stale-reservation cleanup;
      - no prop-state mutation.

    Returns a flat dict and never raises.
    """
    out = {}

    uid_u = str(uid or "").strip()
    profile_u = str(profile_id or "").strip().lower()

    if not uid_u or not profile_u:
        log.warning(
            "analytics: risk snapshot skipped "
            "uid_present=%s profile=%s",
            bool(uid_u),
            profile_u or None,
        )
        return out

    try:
        from api.trend_endpoints import _read_prop_risk_snapshot

        rs = _read_prop_risk_snapshot(
            uid_u,
            profile_u,
            max_age_ms=None,
            fallback_compute=False,
        ) or {}
        #  7 FTMO state
        out["drawdown_pct"]            = rs.get("drawdown_pct")
        out["drawdown_band"]           = rs.get("drawdown_band")
        out["effective_risk_pct"]      = rs.get("effective_risk_pct")
        out["daily_r"]                 = rs.get("daily_r")
        out["daily_loss_used"]         = rs.get("daily_loss_used")
        out["daily_loss_remaining"]    = rs.get("ftmo_daily_loss_remaining")
        out["daily_loss_limit"]        = rs.get("ftmo_daily_loss_limit")
        out["daily_risk_reserved"]     = rs.get("daily_risk_reserved")
        out["projected_daily_loss_if_all_sl"] = rs.get("projected_daily_loss_if_all_sl")
        out["consecutive_losing_days"] = rs.get("consecutive_losing_days")
        out["trading_halted"]          = rs.get("trading_halted")
        out["halt_reason"]             = rs.get("halt_reason")
        out["wins_today"]              = rs.get("wins_today")
        out["losses_today"]            = rs.get("losses_today")
        #  8 account-core (engine's view) /  10 portfolio
        out["broker_balance"]          = rs.get("broker_balance")
        out["broker_equity"]           = rs.get("broker_equity")
        out["floating_pnl"]            = rs.get("floating_pnl")
        out["start_balance"]           = rs.get("start_balance")
        out["open_positions_count"]    = rs.get("open_positions_count")
        out["open_risk_usd"]           = rs.get("open_risk_usd")
    except Exception as e:
        log.warning("analytics: ftmo_state read failed: %s", e)
    return out


def read_account_at_ack(device_id: str, account_type: str = "demo") -> dict:
    """Full account snapshot (margin/leverage/login/server) from the device's
    account key. The :last: variant is a POINTER to the real key - resolve it """
    out = {}
    try:
        if not device_id:
            return out
        R = from_app_R()
        acct = (account_type or "demo")
        # canonical key; fall back to resolving the :last: pointer
        raw = R.get(f"xtl:mt5:account:{device_id}:{acct}")
        if not raw:
            ptr = R.get(f"xtl:mt5:account:last:{device_id}:{acct}")
            if ptr:
                if isinstance(ptr, (bytes, bytearray)):
                    ptr = ptr.decode("utf-8", "ignore")
                raw = R.get(ptr.strip())
        if not raw:
            return out
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", "ignore")
        d = json.loads(raw)
        if isinstance(d, str):
            d = json.loads(d)
        bal = _safe_float(d.get("balance")); eq = _safe_float(d.get("equity"))
        mar = _safe_float(d.get("margin")); fm = _safe_float(d.get("free_margin"))
        out["account_login"]   = d.get("login")
        out["account_server"]  = d.get("server")
        out["account_currency"] = d.get("currency")
        out["leverage"]        = d.get("leverage")
        out["balance"]         = bal
        out["equity"]          = eq
        out["used_margin"]     = mar
        out["free_margin"]     = fm
        out["margin_level"]    = _safe_float(d.get("margin_level"))
        # margin utilization % = used / equity
        if eq and eq > 0 and mar is not None:
            out["margin_utilization_pct"] = round(mar / eq * 100.0, 2)
    except Exception as e:
        log.warning("analytics: account read failed for %s: %s", device_id, e)
    return out

# -----------------------------------------------------------------------------
# H1 20-bar entry-direction helpers
#
# Paste both functions into /opt/xauapi/api/xtl_analytics.py directly ABOVE
#   def build_entry_snapshot(pos: dict, capture_source: str = "normal") -> dict:
#
# They depend only on _safe_float, which already exists in that module.
#
# Emitted fields (added to the entry snapshot):
#   h1_20_direction      gated label  : UP/DOWN only if |net|>=1 ATR AND r2>=gate, else SIDEWAYS
#   h1_20_direction_raw  ungated      : UP/DOWN/SIDEWAYS by net_atr sign alone
#   h1_20_net_atr        net close-to-close move over the window, in ATR units
#   h1_20_slope_atr      least-squares slope per bar, in ATR units
#   h1_20_r2             0..1, how linear/clean the move was (trend vs chop)
#   h1_20_bars_used      how many closes actually went into the calc
#   h1_20_vs_trade       WITH/AGAINST/NEUTRAL   trade side vs GATED direction
# -----------------------------------------------------------------------------


def _h1_window_direction(bars_h1, atr, n=20, r2_gate=0.20):
    """Overall direction of the last n H1 candles at entry.

    Measures net displacement + regression slope (both ATR-normalized) rather
    than counting candle colors, so a clean trend and a choppy round-trip that
    ends flat are correctly distinguished. A weak move that only *happens* to
    close higher (low r2) is labeled SIDEWAYS by the gate, while the raw label
    preserves the pure net-sign read for analysis.

    bars_h1 : chronological, oldest -> newest; bars_h1[-1] is the entry bar.
              (These bars carry OHLC under 'o'/'h'/'l'/'c'; only 'c' is used.)
    atr     : entry-frozen H1 ATR (from liq.get('atr')).
    Returns {} if data is missing or too thin to be meaningful.
    """
    try:
        if not bars_h1 or atr is None or atr <= 0:
            return {}
        window = bars_h1[-n:]
        closes = [_safe_float(b.get("c")) for b in window]
        closes = [c for c in closes if c is not None]
        m = len(closes)
        if m < max(5, n // 2):          # need enough bars to mean anything
            return {}

        # 1) net close-to-close move over the window, in ATR units
        net_atr = (closes[-1] - closes[0]) / atr

        # 2) least-squares slope over the window (per bar), in ATR units
        xs = list(range(m))
        mx = sum(xs) / m
        my = sum(closes) / m
        sxx = sum((x - mx) ** 2 for x in xs)
        sxy = sum((xs[i] - mx) * (closes[i] - my) for i in range(m))
        slope = (sxy / sxx) if sxx else 0.0

        # 3) r^2   how linear the move was (0 choppy .. 1 clean trend)
        ss_tot = sum((c - my) ** 2 for c in closes)
        if ss_tot > 0:
            ss_res = sum(
                (closes[i] - (my + slope * (xs[i] - mx))) ** 2
                for i in range(m)
            )
            r2 = max(0.0, 1.0 - ss_res / ss_tot)
        else:
            r2 = 0.0

        # raw label: pure net-sign; gated label: also requires a real trend (r2)
        raw = "UP" if net_atr >= 1.0 else ("DOWN" if net_atr <= -1.0 else "SIDEWAYS")

        # A strong net move (>=2 ATR over the window) is directional even if the
        # path was choppy (low r2). The r2 gate only arbitrates the marginal
        # 1-2 ATR zone, where "closed higher by a little" might just be noise.
        strong = abs(net_atr) >= 2.0

        gated = (
            raw
            if (
                raw in ("UP", "DOWN")
                and (strong or r2 >= r2_gate)
            )
            else "SIDEWAYS"
        )
        # -------------------------------------------------
        # Trend tilt (analytics only)
        #
        # Unlike h1_20_direction, tilt does NOT require a
        # 1 ATR displacement or high R .
        #
        # It answers:
        # "If there is any directional bias at all,
        # which way is it leaning?"
        #
        # Examples:
        #   Direction = SIDEWAYS
        #   Tilt      = UP
        # -------------------------------------------------

        slope_atr = slope / atr

        if slope_atr >= 0.01:
            tilt = "UP"

        elif slope_atr <= -0.01:
            tilt = "DOWN"

        else:
            tilt = "FLAT"

        return {
            "h1_20_direction":     gated,
            "h1_20_direction_raw": raw,

            # Analytics-only directional lean.
            "h1_20_tilt":          tilt,

            "h1_20_net_atr":       round(net_atr, 2),
            "h1_20_slope_atr":     round(slope_atr, 4),
            "h1_20_r2":            round(r2, 2),
            "h1_20_bars_used":     m,
        }
    except Exception:
        return {}


def _h1_dir_vs_trade(direction, side):
    """Did the trade side align with the (gated) 20-bar H1 direction?

    UP + BUY  -> WITH      UP + SELL  -> AGAINST
    DOWN+ SELL-> WITH      DOWN+ BUY  -> AGAINST
    SIDEWAYS / unknown     -> NEUTRAL

    This is the field to group by for "am I entering with or against the
    H1 trend, and does AGAINST correlate with losses / streaks."
    """
    side = (side or "").upper()
    if direction not in ("UP", "DOWN"):
        return "NEUTRAL"
    if side == "BUY":
        return "WITH" if direction == "UP" else "AGAINST"
    if side == "SELL":
        return "WITH" if direction == "DOWN" else "AGAINST"
    return "NEUTRAL"

def capture_dxy_structure_snapshot(
    device_id: str,
    direction: str | None = None,
) -> dict:
    """
    Freeze the current canonical DXY support/resistance context.

    Lookup priority:
      1. Canonical FTMO REAL_DXY last_good
      2. Canonical FTMO REAL_DXY last
      3. Same-trade-device SYNTHETIC_DXY last_good
      4. Same-trade-device SYNTHETIC_DXY last

    Analytics only. Never blocks trading. Synthetic is a strict fallback,
    never a parallel decision source.
    """

    trade_dev = str(device_id or "").strip()
    direction_u = str(direction or "").upper().strip()

    out = {
        "schema_version": 1,
        "snapshot_status": "MISSING",
        "source": None,
        "device_id": None,
        "trade_device_id": trade_dev or None,
        "canonical_real_device_id": None,
        "fallback_used": False,
        "fallback_reason": None,
        "redis_key": None,

        "price": None,
        "atr": None,
        "nearest_support": None,
        "nearest_resistance": None,
        "distance_support_atr": None,
        "distance_resistance_atr": None,

        "room_up_atr": None,
        "room_down_atr": None,
        "directional_room_atr": None,
        "direction_used": (
            direction_u
            if direction_u in ("UP", "DOWN", "BULLISH", "BEARISH")
            else None
        ),

        "support_tf": None,
        "support_low": None,
        "support_high": None,
        "support_strength": None,
        "support_touches": None,
        "support_source_type": None,

        "resistance_tf": None,
        "resistance_low": None,
        "resistance_high": None,
        "resistance_strength": None,
        "resistance_touches": None,
        "resistance_source_type": None,

        "sr_safety": None,
        "published_at_ms": None,
        "captured_at_ms": int(time.time() * 1000),
        "bundle_age_ms": None,
        "shadow_only": True,
    }

    if not trade_dev:
        out["snapshot_status"] = "MISSING_DEVICE_ID"
        return out

    def _safe_float(value):
        try:
            if value is None:
                return None
            return float(value)
        except Exception:
            return None

    def _safe_int(value):
        try:
            if value is None:
                return None
            return int(value)
        except Exception:
            return None

   

    def _level_value(level):
        if not isinstance(level, dict):
            return None

        return _safe_float(
            level.get("level")
            if level.get("level") is not None
            else level.get("price")
        )

    def _candidate_levels(bundle: dict, side: str) -> list:
        candidates = []

        inventory = (
            bundle.get("sr_inventory")
            if isinstance(bundle.get("sr_inventory"), dict)
            else {}
        )

        inventory_key = (
            "supports"
            if side == "support"
            else "resistances"
        )

        candidates.extend(
            inventory.get(inventory_key) or []
        )

        active_key = (
            "active_supports"
            if side == "support"
            else "active_resistances"
        )

        candidates.extend(
            bundle.get(active_key) or []
        )

        for tf_key in ("h1", "h4", "H1", "H4"):
            tf_block = bundle.get(tf_key)

            if not isinstance(tf_block, dict):
                continue

            candidates.extend(
                tf_block.get(inventory_key) or []
            )

            near_key = (
                "supports_near"
                if side == "support"
                else "resistances_near"
            )

            candidates.extend(
                tf_block.get(near_key) or []
            )

        return [
            level
            for level in candidates
            if isinstance(level, dict)
        ]

    def _find_matching_level(
        bundle: dict,
        side: str,
        target: float | None,
    ) -> dict:
        if target is None:
            return {}

        candidates = _candidate_levels(
            bundle,
            side,
        )

        best = None
        best_distance = None

        for level in candidates:
            value = _level_value(level)

            if value is None:
                continue

            distance = abs(value - target)

            if (
                best_distance is None
                or distance < best_distance
            ):
                best = level
                best_distance = distance

        return dict(best) if isinstance(best, dict) else {}

    try:
        R = from_app_R()
        canonical = resolve_canonical_dxy_source(R, trade_dev)
        real_dev = str(canonical.get("real_device_id") or "").strip()
        synthetic_dev = str(canonical.get("synthetic_device_id") or "").strip()
        out["canonical_real_device_id"] = real_dev or None

        lookups = []
        if real_dev:
            lookups.extend((
                (
                    "REAL_DXY",
                    real_dev,
                    f"xtl:dxy:sr:bundle:last_good:M15:REAL_DXY:{real_dev}",
                ),
                (
                    "REAL_DXY",
                    real_dev,
                    f"xtl:dxy:sr:bundle:last:M15:REAL_DXY:{real_dev}",
                ),
            ))
        if synthetic_dev:
            lookups.extend((
                (
                    "SYNTHETIC_DXY",
                    synthetic_dev,
                    f"xtl:dxy:sr:bundle:last_good:M15:SYNTHETIC_DXY:{synthetic_dev}",
                ),
                (
                    "SYNTHETIC_DXY",
                    synthetic_dev,
                    f"xtl:dxy:sr:bundle:last:M15:SYNTHETIC_DXY:{synthetic_dev}",
                ),
            ))

        selected_source = None
        selected_device = None
        selected_key = None
        bundle = None
        canonical_real_stale = False
        now_ms = int(time.time() * 1000)

        for source, source_device, key in lookups:
            raw = R.get(key)

            if not raw:
                continue

            if isinstance(raw, (bytes, bytearray)):
                raw = raw.decode(
                    "utf-8",
                    "replace",
                )

            try:
                candidate = json.loads(raw)
            except Exception:
                continue

            if not isinstance(candidate, dict):
                continue

            published_ms = _safe_int(
                candidate.get("_published_at_ms")
                or candidate.get("published_at_ms")
            )
            if (
                source == "REAL_DXY"
                and published_ms
                and now_ms - published_ms > DXY_CANONICAL_SR_FRESH_MS
            ):
                canonical_real_stale = True
                continue

            selected_source = source
            selected_device = source_device
            selected_key = key
            bundle = candidate
            break

        if not isinstance(bundle, dict):
            out["snapshot_status"] = "SR_BUNDLE_MISSING"
            return out

        price = _safe_float(bundle.get("price"))
        atr = _safe_float(bundle.get("atr"))

        nearest_support = _safe_float(
            bundle.get("nearest_support")
        )

        nearest_resistance = _safe_float(
            bundle.get("nearest_resistance")
        )

        distance_atr = (
            bundle.get("distance_atr")
            if isinstance(
                bundle.get("distance_atr"),
                dict,
            )
            else {}
        )

        support_distance_atr = _safe_float(
            distance_atr.get("support")
        )

        resistance_distance_atr = _safe_float(
            distance_atr.get("resistance")
        )

        support_level = _find_matching_level(
            bundle,
            "support",
            nearest_support,
        )

        resistance_level = _find_matching_level(
            bundle,
            "resistance",
            nearest_resistance,
        )

        published_at_ms = _safe_int(
            bundle.get("_published_at_ms")
        )

        captured_at_ms = int(time.time() * 1000)

        out.update({
            "snapshot_status": "OK",
            "source": selected_source,
            "device_id": selected_device,
            "trade_device_id": trade_dev or None,
            "canonical_real_device_id": real_dev or None,
            "fallback_used": selected_source == "SYNTHETIC_DXY",
            "fallback_reason": (
                "CANONICAL_REAL_SR_STALE"
                if selected_source == "SYNTHETIC_DXY" and canonical_real_stale
                else "CANONICAL_REAL_SR_UNAVAILABLE"
                if selected_source == "SYNTHETIC_DXY"
                else None
            ),
            "redis_key": selected_key,

            "price": price,
            "atr": atr,
            "nearest_support": nearest_support,
            "nearest_resistance": nearest_resistance,
            "distance_support_atr": support_distance_atr,
            "distance_resistance_atr": resistance_distance_atr,

            "room_up_atr": resistance_distance_atr,
            "room_down_atr": support_distance_atr,

            "support_tf": support_level.get("tf"),
            "support_low": _safe_float(
                support_level.get("low")
            ),
            "support_high": _safe_float(
                support_level.get("high")
            ),
            "support_strength": _safe_float(
                support_level.get("sr_score")
                if support_level.get("sr_score") is not None
                else support_level.get("strength")
            ),
            "support_touches": _safe_int(
                support_level.get("touches")
            ),
            "support_source_type": (
                support_level.get("source_type")
                or support_level.get("band_type")
            ),

            "resistance_tf": resistance_level.get("tf"),
            "resistance_low": _safe_float(
                resistance_level.get("low")
            ),
            "resistance_high": _safe_float(
                resistance_level.get("high")
            ),
            "resistance_strength": _safe_float(
                resistance_level.get("sr_score")
                if resistance_level.get("sr_score") is not None
                else resistance_level.get("strength")
            ),
            "resistance_touches": _safe_int(
                resistance_level.get("touches")
            ),
            "resistance_source_type": (
                resistance_level.get("source_type")
                or resistance_level.get("band_type")
            ),

            "sr_safety": bundle.get("sr_safety"),
            "published_at_ms": published_at_ms,
            "captured_at_ms": captured_at_ms,
            "bundle_age_ms": (
                max(
                    0,
                    captured_at_ms - published_at_ms,
                )
                if published_at_ms
                else None
            ),
        })

        if direction_u in ("UP", "BULLISH"):
            out["directional_room_atr"] = (
                resistance_distance_atr
            )
        elif direction_u in ("DOWN", "BEARISH"):
            out["directional_room_atr"] = (
                support_distance_atr
            )

        return out

    except Exception as exc:
        log.warning(
            "analytics: DXY structure capture failed "
            "device=%s err=%r",
            trade_dev,
            exc,
        )

        out["snapshot_status"] = "CAPTURE_ERROR"
        out["capture_error"] = (
            f"{type(exc).__name__}:{exc}"
        )

        return out

def capture_dxy_market_snapshot(
    device_id: str,
) -> dict:
    """
    Freeze real broker DXY H1 market context.

    Market-only function:
      - receives the trade device for provenance
      - reads DXY H1 bars from the canonical FTMO REAL_DXY device
      - calculates the last-20-H1 direction
      - does not know the traded symbol or trade side
      - never scans for an arbitrary REAL_DXY device

    Analytics only. Never blocks trading.
    """

    trade_dev = str(device_id or "").strip()

    out = {
        "dxy_available": False,
        "dxy_source": "broker_mt5",
        "dxy_symbol": "DXY",
        "dxy_device_id": None,
        "dxy_trade_device_id": trade_dev or None,
        "dxy_canonical_device": True,
        "dxy_canonical_config_source": None,

        "dxy_last_closed_h1_close": None,
        "dxy_last_closed_h1_close_ms": None,

        "dxy_h1_20_direction": None,
        "dxy_h1_20_direction_raw": None,
        "dxy_h1_20_net_atr": None,
        "dxy_h1_20_slope_atr": None,
        "dxy_h1_20_r2": None,
        "dxy_h1_20_bars_used": None,
        "dxy_h1_20_tilt": None,
        "dxy_unavailable_reason": None,
    }

    try:
        if not trade_dev:
            out["dxy_unavailable_reason"] = "MISSING_DEVICE_ID"
            return out

        R = from_app_R()
        canonical = resolve_canonical_dxy_source(R, trade_dev)
        dev = str(canonical.get("real_device_id") or "").strip()
        out["dxy_device_id"] = dev or None
        out["dxy_canonical_config_source"] = canonical.get("configured_by")

        if not dev:
            out["dxy_unavailable_reason"] = "CANONICAL_REAL_DXY_DEVICE_MISSING"
            return out

        key = (
            f"xtl:ohlc:snap:{dev}:DXY:H1"
        )

        raw = R.get(key)

        if not raw:
            out["dxy_unavailable_reason"] = (
                "DXY_H1_SNAPSHOT_NOT_FOUND"
            )
            return out

        if isinstance(
            raw,
            (bytes, bytearray),
        ):
            raw = raw.decode(
                "utf-8",
                "ignore",
            )

        payload = json.loads(raw)

        # Safely handle accidentally double-encoded JSON.
        if isinstance(payload, str):
            payload = json.loads(payload)

        if not isinstance(payload, dict):
            out["dxy_unavailable_reason"] = (
                "INVALID_DXY_SNAPSHOT"
            )
            return out

        received_ms = _safe_int(
            payload.get("server_received_ms")
            or payload.get("received_at_ms")
            or payload.get("published_at_ms"),
            0,
        ) or 0
        if (
            received_ms > 0
            and _now_ms() - received_ms > DXY_CANONICAL_H1_FRESH_MS
        ):
            out["dxy_unavailable_reason"] = (
                "CANONICAL_REAL_DXY_H1_STALE"
            )
            out["dxy_snapshot_age_ms"] = max(0, _now_ms() - received_ms)
            return out

        bars = payload.get("bars") or []

        if not isinstance(bars, list):
            out["dxy_unavailable_reason"] = (
                "INVALID_DXY_BARS"
            )
            return out

        closed_bars = [
            bar
            for bar in bars
            if (
                isinstance(bar, dict)
                and bool(
                    bar.get("complete", True)
                )
            )
        ]

        if len(closed_bars) < 20:
            out["dxy_unavailable_reason"] = (
                "INSUFFICIENT_DXY_H1_BARS"
            )
            return out

        atr = _atr_from_bars(
            closed_bars
        )

        if not atr or atr <= 0:
            out["dxy_unavailable_reason"] = (
                "INVALID_DXY_ATR"
            )
            return out

        direction = _h1_window_direction(
            closed_bars,
            atr,
            n=20,
        )

        if not direction:
            out["dxy_unavailable_reason"] = (
                "DXY_DIRECTION_CALC_FAILED"
            )
            return out

        last_bar = closed_bars[-1]

        last_close = _safe_float(
            last_bar.get("c")
        )

        # This is the latest completed DXY H1 candle time.
        # It remains in the feed's broker-encoded time convention.
        last_close_ms = _norm_ms(
            last_bar.get("t_close_ms")
            or last_bar.get("t_open_ms")
            or last_bar.get("t")
            or 0
        )

        out.update({
            "dxy_available": True,

            "dxy_last_closed_h1_close": (
                last_close
            ),
            "dxy_last_closed_h1_close_ms": (
                last_close_ms or None
            ),

            "dxy_h1_20_direction": (
                direction.get(
                    "h1_20_direction"
                )
            ),
            "dxy_h1_20_direction_raw": (
                direction.get(
                    "h1_20_direction_raw"
                )
            ),
            "dxy_h1_20_net_atr": (
                direction.get(
                    "h1_20_net_atr"
                )
            ),
            "dxy_h1_20_slope_atr": (
                direction.get(
                    "h1_20_slope_atr"
                )
            ),
            "dxy_h1_20_r2": (
                direction.get(
                    "h1_20_r2"
                )
            ),
            "dxy_h1_20_tilt": (
                direction.get(
                    "h1_20_tilt"
                )
            ),
            "dxy_h1_20_bars_used": (
                direction.get(
                    "h1_20_bars_used"
                )
            ),

            "dxy_unavailable_reason": None,
        })

        # Immutable raw H1 candle behavior for cross-market entry research.
        # It is descriptive only and is never consumed by execution.
        out["dxy_h1_last10_behavior"] = _h1_last10_behavior(
            closed_bars,
            atr,
        )

        return out

    except Exception as exc:
        out["dxy_unavailable_reason"] = (
            "DXY_MARKET_CAPTURE_EXCEPTION:"
            f"{type(exc).__name__}:{exc}"
        )

        log.warning(
            "analytics: DXY market capture failed "
            "device=%s err=%r",
            device_id,
            exc,
        )

        return out



def build_synthetic_dxy_h1_bars(
    device_id: str,
    max_bars: int = 300,
) -> list:
    """
    Build a DXY-like H1 OHLC series from the same device's USD pairs.

    USD quote pairs are inverted:
      EURUSD, GBPUSD

    USD base pairs are kept:
      USDJPY, USDCHF, USDCAD

    Each component is normalized to 100 at its first usable close.
    At least 3 pair contributions are required for each synthetic bar.

    Analytics only. Never scans or borrows another device.
    """

    dev = str(device_id or "").strip()

    if not dev:
        return []

    pair_signs = {
        "EURUSD": -1,
        "GBPUSD": -1,
        "USDJPY": +1,
        "USDCHF": +1,
        "USDCAD": +1,
    }

    try:
        R = from_app_R()

        pair_maps = {}
        pair_bases = {}

        for symbol, sign in pair_signs.items():
            key = (
                f"xtl:ohlc:snap:{dev}:"
                f"{symbol}:H1"
            )

            raw = R.get(key)
            if not raw:
                continue

            if isinstance(raw, (bytes, bytearray)):
                raw = raw.decode(
                    "utf-8",
                    "ignore",
                )

            payload = json.loads(raw)

            if isinstance(payload, str):
                payload = json.loads(payload)

            if not isinstance(payload, dict):
                continue

            bars = payload.get("bars") or []
            if not isinstance(bars, list):
                continue

            bar_map = {}

            for bar in bars[-int(max_bars or 300):]:
                if not isinstance(bar, dict):
                    continue

                if not bool(
                    bar.get("complete", True)
                ):
                    continue

                ts_ms = _norm_ms(
                    bar.get("t_open_ms")
                    or bar.get("t")
                    or 0
                )

                open_px = _safe_float(
                    bar.get("o")
                )
                high_px = _safe_float(
                    bar.get("h")
                )
                low_px = _safe_float(
                    bar.get("l")
                )
                close_px = _safe_float(
                    bar.get("c")
                )

                if (
                    not ts_ms
                    or open_px is None
                    or high_px is None
                    or low_px is None
                    or close_px is None
                    or open_px <= 0
                    or high_px <= 0
                    or low_px <= 0
                    or close_px <= 0
                ):
                    continue

                bar_map[int(ts_ms)] = {
                    "o": open_px,
                    "h": high_px,
                    "l": low_px,
                    "c": close_px,
                }

            if not bar_map:
                continue

            first_ts = min(bar_map)
            first_close = _safe_float(
                bar_map[first_ts].get("c")
            )

            if (
                first_close is None
                or first_close <= 0
            ):
                continue

            pair_maps[symbol] = {
                "sign": sign,
                "bars": bar_map,
            }

            pair_bases[symbol] = first_close

        if len(pair_maps) < 3:
            return []

        all_timestamps = sorted({
            ts
            for rec in pair_maps.values()
            for ts in rec["bars"].keys()
        })

        synthetic = []

        for ts_ms in all_timestamps:
            open_components = []
            high_components = []
            low_components = []
            close_components = []
            contributors = []

            for symbol, rec in pair_maps.items():
                bar = rec["bars"].get(ts_ms)
                if not bar:
                    continue

                base = pair_bases.get(symbol)
                sign = int(rec.get("sign") or 0)

                if not base or sign not in (-1, +1):
                    continue

                o = _safe_float(bar.get("o"))
                h = _safe_float(bar.get("h"))
                l = _safe_float(bar.get("l"))
                c = _safe_float(bar.get("c"))

                if (
                    o is None
                    or h is None
                    or l is None
                    or c is None
                    or min(o, h, l, c) <= 0
                ):
                    continue

                if sign == +1:
                    # USD is base:
                    # rising pair means stronger USD.
                    component_o = 100.0 * (o / base)
                    component_h = 100.0 * (h / base)
                    component_l = 100.0 * (l / base)
                    component_c = 100.0 * (c / base)

                else:
                    # USD is quote:
                    # falling pair means stronger USD.
                    #
                    # Inversion reverses high and low:
                    # inverted high comes from original low.
                    component_o = 100.0 * (base / o)
                    component_h = 100.0 * (base / l)
                    component_l = 100.0 * (base / h)
                    component_c = 100.0 * (base / c)

                open_components.append(
                    component_o
                )
                high_components.append(
                    component_h
                )
                low_components.append(
                    component_l
                )
                close_components.append(
                    component_c
                )
                contributors.append(
                    symbol
                )

            if len(close_components) < 3:
                continue

            synthetic_open = (
                sum(open_components)
                / len(open_components)
            )
            synthetic_high = (
                sum(high_components)
                / len(high_components)
            )
            synthetic_low = (
                sum(low_components)
                / len(low_components)
            )
            synthetic_close = (
                sum(close_components)
                / len(close_components)
            )

            # Preserve valid OHLC ordering after averaging.
            synthetic_high = max(
                synthetic_high,
                synthetic_open,
                synthetic_close,
            )

            synthetic_low = min(
                synthetic_low,
                synthetic_open,
                synthetic_close,
            )

            synthetic.append({
                "t": int(ts_ms // 1000),
                "t_open_ms": int(ts_ms),
                "t_close_ms": int(
                    ts_ms + 3_600_000
                ),

                "o": round(
                    synthetic_open,
                    6,
                ),
                "h": round(
                    synthetic_high,
                    6,
                ),
                "l": round(
                    synthetic_low,
                    6,
                ),
                "c": round(
                    synthetic_close,
                    6,
                ),

                "complete": True,
                "synthetic": True,
                "synthetic_pair_count": len(
                    contributors
                ),
                "synthetic_pairs": contributors,
            })

        synthetic.sort(
            key=lambda bar: int(
                bar.get("t_open_ms") or 0
            )
        )

        return synthetic

    except Exception as exc:
        log.warning(
            "analytics: synthetic DXY build failed "
            "device=%s err=%r",
            device_id,
            exc,
        )
        return []

def compute_dxy_trade_alignment(
    trade_symbol: str,
    trade_side: str,
    dxy_direction: str,
) -> str:
    """
    Compare a trade direction with the frozen DXY direction.

    Contains no Redis or OHLC reads.

    DXY UP:
      EURUSD/GBPUSD BUY  -> ADVERSE
      EURUSD/GBPUSD SELL -> FAVORABLE

      USDJPY/USDCHF/USDCAD BUY  -> FAVORABLE
      USDJPY/USDCHF/USDCAD SELL -> ADVERSE

    DXY DOWN reverses those relationships.

    XAUUSD remains analytics-only for Phase 1.
    """

    try:
        symbol = str(
            trade_symbol or ""
        ).upper().strip()

        side = str(
            trade_side or ""
        ).upper().strip()

        direction = str(
            dxy_direction or ""
        ).upper().strip()

        if symbol == "XAUUSD":
            return "ANALYTICS_ONLY"

        if side not in (
            "BUY",
            "SELL",
        ):
            return "UNKNOWN"

        if direction not in (
            "UP",
            "DOWN",
        ):
            return "NEUTRAL"

        usd_base_pairs = {
            "USDJPY",
            "USDCHF",
            "USDCAD",
        }

        usd_quote_pairs = {
            "EURUSD",
            "GBPUSD",
        }

        if symbol in usd_base_pairs:
            favorable = (
                (
                    side == "BUY"
                    and direction == "UP"
                )
                or
                (
                    side == "SELL"
                    and direction == "DOWN"
                )
            )

        elif symbol in usd_quote_pairs:
            favorable = (
                (
                    side == "BUY"
                    and direction == "DOWN"
                )
                or
                (
                    side == "SELL"
                    and direction == "UP"
                )
            )

        else:
            return "UNKNOWN"

        return (
            "FAVORABLE"
            if favorable
            else "ADVERSE"
        )

    except Exception as exc:
        log.warning(
            "analytics: DXY alignment failed "
            "symbol=%s side=%s direction=%s err=%r",
            trade_symbol,
            trade_side,
            dxy_direction,
            exc,
        )

        return "UNKNOWN"

def capture_usd_reference_snapshot(
    device_id: str,
    trade_symbol: str,
    trade_side: str,
) -> dict:
    """
    Produce one unified USD-reference snapshot for a trade.

    Priority:
      1. Canonical FTMO REAL DXY shared across all trade devices.
      2. Synthetic DXY built from the exact trade device's five USD pairs.

    Both sources use _h1_window_direction() with:
      20 H1 bars
      ATR normalization
      net displacement
      regression slope
      R2 gate
      trend tilt

    Analytics only Never changes zones gates or execution.
    """

    dev = str(device_id or "").strip()

    out = {
        "usd_reference_available": False,
        "usd_reference_source": None,
        "usd_reference_device_id": dev or None,

        "usd_reference_last_closed_h1_close": None,
        "usd_reference_last_closed_h1_close_ms": None,

        "usd_reference_direction": None,
        "usd_reference_direction_raw": None,
        "usd_reference_tilt": None,
        "usd_reference_net_atr": None,
        "usd_reference_slope_atr": None,
        "usd_reference_r2": None,
        "usd_reference_bars_used": None,

        "usd_reference_alignment_at_entry": "UNAVAILABLE",

        "usd_reference_synthetic_pair_count": None,
        "usd_reference_synthetic_pairs": None,

        "usd_reference_unavailable_reason": None,
    }

    try:
        if not dev:
            out["usd_reference_unavailable_reason"] = (
                "MISSING_DEVICE_ID"
            )
            return out

        # -------------------------------------------------
        # 1. Prefer real DXY from this exact device.
        # -------------------------------------------------
        real_dxy = capture_dxy_market_snapshot(
            device_id=dev,
        )

        if real_dxy.get("dxy_available"):
            direction = str(
                real_dxy.get(
                    "dxy_h1_20_direction"
                )
                or ""
            ).upper().strip()

            out.update({
                "usd_reference_available": True,
                "usd_reference_source": (
                    "REAL_BROKER_DXY"
                ),
                "usd_reference_device_id": (
                    real_dxy.get("dxy_device_id")
                    or dev
                ),

                "usd_reference_last_closed_h1_close": (
                    real_dxy.get(
                        "dxy_last_closed_h1_close"
                    )
                ),
                "usd_reference_last_closed_h1_close_ms": (
                    real_dxy.get(
                        "dxy_last_closed_h1_close_ms"
                    )
                ),

                "usd_reference_direction": (
                    real_dxy.get(
                        "dxy_h1_20_direction"
                    )
                ),
                "usd_reference_direction_raw": (
                    real_dxy.get(
                        "dxy_h1_20_direction_raw"
                    )
                ),
                "usd_reference_tilt": (
                    real_dxy.get(
                        "dxy_h1_20_tilt"
                    )
                ),
                "usd_reference_net_atr": (
                    real_dxy.get(
                        "dxy_h1_20_net_atr"
                    )
                ),
                "usd_reference_slope_atr": (
                    real_dxy.get(
                        "dxy_h1_20_slope_atr"
                    )
                ),
                "usd_reference_r2": (
                    real_dxy.get(
                        "dxy_h1_20_r2"
                    )
                ),
                "usd_reference_bars_used": (
                    real_dxy.get(
                        "dxy_h1_20_bars_used"
                    )
                ),

                "usd_reference_alignment_at_entry": (
                    compute_dxy_trade_alignment(
                        trade_symbol=trade_symbol,
                        trade_side=trade_side,
                        dxy_direction=direction,
                    )
                ),

                "usd_reference_unavailable_reason": None,
            })

            return out

        # -------------------------------------------------
        # 2. Same-device synthetic DXY fallback.
        # -------------------------------------------------
        synthetic_bars = (
            build_synthetic_dxy_h1_bars(
                device_id=dev,
                max_bars=300,
            )
        )

        if len(synthetic_bars) < 20:
            out["usd_reference_source"] = (
                "SYNTHETIC_USD_BASKET"
            )
            out["usd_reference_unavailable_reason"] = (
                "INSUFFICIENT_SYNTHETIC_DXY_H1_BARS"
            )
            return out

        synthetic_atr = _atr_from_bars(
            synthetic_bars
        )

        if not synthetic_atr or synthetic_atr <= 0:
            out["usd_reference_source"] = (
                "SYNTHETIC_USD_BASKET"
            )
            out["usd_reference_unavailable_reason"] = (
                "INVALID_SYNTHETIC_DXY_ATR"
            )
            return out

        synthetic_direction = (
            _h1_window_direction(
                synthetic_bars,
                synthetic_atr,
                n=20,
            )
            or {}
        )

        if not synthetic_direction:
            out["usd_reference_source"] = (
                "SYNTHETIC_USD_BASKET"
            )
            out["usd_reference_unavailable_reason"] = (
                "SYNTHETIC_DXY_DIRECTION_CALC_FAILED"
            )
            return out

        last_bar = synthetic_bars[-1]

        direction = str(
            synthetic_direction.get(
                "h1_20_direction"
            )
            or ""
        ).upper().strip()

        out.update({
            "usd_reference_available": True,
            "usd_reference_source": (
                "SYNTHETIC_USD_BASKET"
            ),
            "usd_reference_device_id": dev,

            "usd_reference_last_closed_h1_close": (
                _safe_float(
                    last_bar.get("c")
                )
            ),
            "usd_reference_last_closed_h1_close_ms": (
                _norm_ms(
                    last_bar.get("t_close_ms")
                    or last_bar.get("t_open_ms")
                    or last_bar.get("t")
                    or 0
                )
            ),

            "usd_reference_direction": (
                synthetic_direction.get(
                    "h1_20_direction"
                )
            ),
            "usd_reference_direction_raw": (
                synthetic_direction.get(
                    "h1_20_direction_raw"
                )
            ),
            "usd_reference_tilt": (
                synthetic_direction.get(
                    "h1_20_tilt"
                )
            ),
            "usd_reference_net_atr": (
                synthetic_direction.get(
                    "h1_20_net_atr"
                )
            ),
            "usd_reference_slope_atr": (
                synthetic_direction.get(
                    "h1_20_slope_atr"
                )
            ),
            "usd_reference_r2": (
                synthetic_direction.get(
                    "h1_20_r2"
                )
            ),
            "usd_reference_bars_used": (
                synthetic_direction.get(
                    "h1_20_bars_used"
                )
            ),

            "usd_reference_alignment_at_entry": (
                compute_dxy_trade_alignment(
                    trade_symbol=trade_symbol,
                    trade_side=trade_side,
                    dxy_direction=direction,
                )
            ),

            "usd_reference_synthetic_pair_count": (
                last_bar.get(
                    "synthetic_pair_count"
                )
            ),
            "usd_reference_synthetic_pairs": (
                last_bar.get(
                    "synthetic_pairs"
                )
            ),

            "usd_reference_unavailable_reason": None,
        })

        return out

    except Exception as exc:
        out["usd_reference_unavailable_reason"] = (
            "USD_REFERENCE_CAPTURE_EXCEPTION:"
            f"{type(exc).__name__}:{exc}"
        )

        log.warning(
            "analytics: USD reference capture failed "
            "device=%s symbol=%s side=%s err=%r",
            device_id,
            trade_symbol,
            trade_side,
            exc,
        )

        return out


# -- DXY M15 SHADOW ANALYTICS -------------------------------------------------
def _dxy_m15_state_key(source: str, device_id: str) -> str:
    return f"{DXY_M15_STATE_PREFIX}:{str(source).upper()}:{str(device_id).strip()}"


def _dxy_m15_history_key(source: str, device_id: str) -> str:
    return f"{DXY_M15_HISTORY_PREFIX}:{str(source).upper()}:{str(device_id).strip()}"


def _dxy_m15_direction(value) -> str:
    direction = str(value or "").upper().strip()
    if direction in ("BULLISH", "UP"):
        return "BULLISH"
    if direction in ("BEARISH", "DOWN"):
        return "BEARISH"
    return "NEUTRAL"


def _dxy_m15_trade_alignment(symbol: str, side: str, dxy_direction: str) -> str:
    """Return ALIGNED / AGAINST / NEUTRAL for USD direction vs trade side."""
    try:
        sym = str(symbol or "").upper().strip()
        trade_side = str(side or "").upper().strip()
        direction = _dxy_m15_direction(dxy_direction)
        if trade_side not in ("BUY", "SELL") or direction == "NEUTRAL":
            return "NEUTRAL"

        usd_base = sym in {"USDJPY", "USDCHF", "USDCAD"}
        usd_quote_or_gold = sym in {"EURUSD", "GBPUSD", "XAUUSD"}
        if not (usd_base or usd_quote_or_gold):
            return "UNKNOWN"

        usd_strength_trade = (
            (usd_base and trade_side == "BUY")
            or (usd_quote_or_gold and trade_side == "SELL")
        )
        aligned = usd_strength_trade if direction == "BULLISH" else not usd_strength_trade
        return "ALIGNED" if aligned else "AGAINST"
    except Exception:
        return "UNKNOWN"


def _dxy_m15_decode(raw, default=None):
    if default is None:
        default = {}
    try:
        if raw is None:
            return default
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", "ignore")
        value = json.loads(raw) if isinstance(raw, str) else raw
        if isinstance(value, str):
            value = json.loads(value)
        return value
    except Exception:
        return default


def _dxy_m15_compact_features(features: dict) -> dict:
    """Keep explainable evidence while avoiding duplicating the full 300-bar series."""
    f = features if isinstance(features, dict) else {}
    sr = f.get("sr_audit") if isinstance(f.get("sr_audit"), dict) else {}
    context = sr.get("structure_context") if isinstance(sr.get("structure_context"), dict) else {}
    pin_current = f.get("pin_current") if isinstance(f.get("pin_current"), dict) else {}
    pin_previous = f.get("pin_previous") if isinstance(f.get("pin_previous"), dict) else {}
    return {
        "model": f.get("model"),
        "bar_close_ms": _safe_int(f.get("bar_close_ms"), 0),
        "detected_at_ms": _safe_int(f.get("detected_at_ms"), 0),
        "recent_net_atr": _safe_float(f.get("recent_net_atr")),
        "prior_net_atr": _safe_float(f.get("prior_net_atr")),
        "slope_5_atr": _safe_float(f.get("slope_5_atr")),
        "slope_12_atr": _safe_float(f.get("slope_12_atr")),
        "prior_slope_5_atr": _safe_float(f.get("prior_slope_5_atr")),
        "acceleration_atr": _safe_float(f.get("acceleration_atr")),
        "bullish_structure_break": bool(f.get("bullish_structure_break")),
        "bearish_structure_break": bool(f.get("bearish_structure_break")),
        "compression_ratio": _safe_float(f.get("compression_ratio")),
        "expansion_ratio": _safe_float(f.get("expansion_ratio")),
        "body_ratio": _safe_float(f.get("body_ratio")),
        "bullish_pin_score": _safe_int(f.get("bullish_pin_score"), 0),
        "bearish_pin_score": _safe_int(f.get("bearish_pin_score"), 0),
        "pin_current": pin_current,
        "pin_previous": pin_previous,
        "bull_evidence_score": _safe_int(f.get("bull_evidence_score"), 0),
        "bear_evidence_score": _safe_int(f.get("bear_evidence_score"), 0),
        "evidence_direction": f.get("evidence_direction"),
        "evidence_margin": _safe_float(f.get("evidence_margin")),
        "evidence_detail": f.get("evidence_detail") if isinstance(f.get("evidence_detail"), dict) else {},
        "candidate_qualification_reason": f.get("candidate_qualification_reason"),
        "candidate_qualified": bool(f.get("candidate_qualified")),
        "synthetic_pair_count": _safe_int(f.get("synthetic_pair_count"), 0),
        "synthetic_pairs": f.get("synthetic_pairs") if isinstance(f.get("synthetic_pairs"), list) else [],
        "sr_score_enabled": bool(f.get("sr_score_enabled")),
        "sr_score_applied": _safe_float(f.get("sr_score_applied"), 0.0),
        "sr": {
            "structure_context": context,
            "near_support_tfs": sr.get("near_support_tfs") if isinstance(sr.get("near_support_tfs"), list) else [],
            "near_resistance_tfs": sr.get("near_resistance_tfs") if isinstance(sr.get("near_resistance_tfs"), list) else [],
            "bullish_h1_sweep_reclaim": bool(sr.get("bullish_h1_sweep_reclaim")),
            "bearish_h1_sweep_reject": bool(sr.get("bearish_h1_sweep_reject")),
            "sweep_conflict": bool(sr.get("sweep_conflict")),
            "bullish_confluence_count": _safe_int(sr.get("bullish_confluence_count"), 0),
            "bearish_confluence_count": _safe_int(sr.get("bearish_confluence_count"), 0),
            "h1": sr.get("h1") if isinstance(sr.get("h1"), dict) else {},
            "h4": sr.get("h4") if isinstance(sr.get("h4"), dict) else {},
            "m15": sr.get("m15") if isinstance(sr.get("m15"), dict) else {},
        },
    }



def _dxy_reason_part_code(name: str) -> str:
    return str(name or "").strip().upper().replace(" ", "_")


def _dxy_m15_reasoning(state_snapshot: dict) -> dict:
    """Build an explainable, analytics-only interpretation of one DXY state.

    Important:
      - Never changes DXY lifecycle status or direction.
      - Never changes candidate qualification or confidence.
      - Never blocks, permits, delays, sizes, or modifies a trade.
      - SR remains analytics/audit context only.
    """
    state = (
        state_snapshot
        if isinstance(state_snapshot, dict)
        else {}
    )

    features = (
        state.get("features")
        if isinstance(state.get("features"), dict)
        else {}
    )

    detail = (
        features.get("evidence_detail")
        if isinstance(features.get("evidence_detail"), dict)
        else {}
    )

    bull_parts = (
        detail.get("bull_parts")
        if isinstance(detail.get("bull_parts"), dict)
        else {}
    )

    bear_parts = (
        detail.get("bear_parts")
        if isinstance(detail.get("bear_parts"), dict)
        else {}
    )

    evidence_direction = _dxy_m15_direction(
        features.get("evidence_direction")
    )

    status = str(
        state.get("status") or "UNAVAILABLE"
    ).upper().strip()

    direction = _dxy_m15_direction(
        state.get("direction")
    )

    alignment = str(
        state.get("trade_alignment") or "NEUTRAL"
    ).upper().strip()

    qualified = bool(
        features.get("candidate_qualified")
    )

    qualification_reason = str(
        features.get("candidate_qualification_reason")
        or ""
    ).upper().strip() or None

    selected_parts = (
        bull_parts
        if evidence_direction == "BULLISH"
        else bear_parts
        if evidence_direction == "BEARISH"
        else {}
    )

    opposing_parts = (
        bear_parts
        if evidence_direction == "BULLISH"
        else bull_parts
        if evidence_direction == "BEARISH"
        else {}
    )

    positive_factors = []
    penalties = []

    for name, raw_value in selected_parts.items():
        value = _safe_float(raw_value)

        if value is None or value == 0:
            continue

        item = {
            "code": _dxy_reason_part_code(name),
            "contribution": round(value, 4),
        }

        if value > 0:
            positive_factors.append(item)
        else:
            penalties.append(item)

    opposing_factors = []

    for name, raw_value in opposing_parts.items():
        value = _safe_float(raw_value)

        if value is None or value <= 0:
            continue

        opposing_factors.append({
            "code": _dxy_reason_part_code(name),
            "contribution": round(value, 4),
        })

    positive_factors.sort(
        key=lambda item: abs(
            float(item.get("contribution") or 0)
        ),
        reverse=True,
    )

    penalties.sort(
        key=lambda item: abs(
            float(item.get("contribution") or 0)
        ),
        reverse=True,
    )

    opposing_factors.sort(
        key=lambda item: abs(
            float(item.get("contribution") or 0)
        ),
        reverse=True,
    )

    blockers = []

    if not qualified and qualification_reason:
        blockers.append(qualification_reason)

    if (
        status == "IDLE"
        and state.get("last_event_status") == "REJECTED"
    ):
        blockers.append(
            str(
                state.get("last_event_reason")
                or "LAST_CANDIDATE_REJECTED"
            ).upper()
        )

    if not state.get("available"):
        blockers.append(
            str(
                state.get("unavailable_reason")
                or "DXY_STATE_UNAVAILABLE"
            ).upper()
        )

    sr = (
        features.get("sr")
        if isinstance(features.get("sr"), dict)
        else {}
    )

    context = (
        sr.get("structure_context")
        if isinstance(sr.get("structure_context"), dict)
        else {}
    )

    sr_enabled = bool(
        features.get("sr_score_enabled")
    )

    sr_applied = (
        _safe_float(
            features.get("sr_score_applied"),
            0.0,
        )
        or 0.0
    )

    downside_room_atr = _safe_float(
        context.get("available_downside_atr")
    )

    upside_room_atr = _safe_float(
        context.get("available_upside_atr")
    )

    near_h1_support = bool(
        context.get("near_h1_support")
    )

    near_h1_resistance = bool(
        context.get("near_h1_resistance")
    )

    inside_h4_support = bool(
        context.get("inside_h4_support")
    )

    inside_h4_resistance = bool(
        context.get("inside_h4_resistance")
    )

    inside_m15_support = bool(
        context.get("inside_m15_support")
    )

    inside_m15_resistance = bool(
        context.get("inside_m15_resistance")
    )

    near_support_tfs = (
        sr.get("near_support_tfs")
        if isinstance(sr.get("near_support_tfs"), list)
        else []
    )

    near_resistance_tfs = (
        sr.get("near_resistance_tfs")
        if isinstance(sr.get("near_resistance_tfs"), list)
        else []
    )

    sr_context = {
        "context": context.get("context"),
        "near_h1_support": near_h1_support,
        "near_h1_resistance": near_h1_resistance,
        "inside_h4_support": inside_h4_support,
        "inside_h4_resistance": inside_h4_resistance,
        "inside_m15_support": inside_m15_support,
        "inside_m15_resistance": inside_m15_resistance,
        "bullish_h1_sweep_reclaim": bool(
            context.get("bullish_h1_sweep_reclaim")
        ),
        "bearish_h1_sweep_reject": bool(
            context.get("bearish_h1_sweep_reject")
        ),
        "sweep_conflict": bool(
            context.get("sweep_conflict")
        ),
        "available_downside_atr": downside_room_atr,
        "available_upside_atr": upside_room_atr,
        "bullish_room_ratio": _safe_float(
            context.get("bullish_room_ratio")
        ),
        "bearish_room_ratio": _safe_float(
            context.get("bearish_room_ratio")
        ),
        "near_support_tfs": near_support_tfs,
        "near_resistance_tfs": near_resistance_tfs,
        "bullish_confluence_count": _safe_int(
            sr.get("bullish_confluence_count"),
            0,
        ),
        "bearish_confluence_count": _safe_int(
            sr.get("bearish_confluence_count"),
            0,
        ),
        "influence_mode": (
            "ACTIVE_BOUNDED"
            if sr_enabled
            else "AUDIT_ONLY"
        ),
        "score_applied": round(
            sr_applied,
            4,
        ),
    }

    # ---------------------------------------------------------
    # Existing lifecycle-level analytics interpretation.
    # ---------------------------------------------------------
    if status == "UNAVAILABLE":
        decision = "DXY_UNAVAILABLE"
        decision_reason = (
            state.get("unavailable_reason")
            or "STATE_UNAVAILABLE"
        )

    elif status == "CONFIRMED":
        decision = "CONFIRMED_DXY_OPINION"
        decision_reason = (
            f"CONFIRMED_{direction}"
        )

    elif status == "PENDING":
        decision = "DEVELOPING_DXY_OPINION"
        decision_reason = (
            f"PENDING_{direction}"
        )

    elif status == "IDLE":
        decision = "NO_DXY_OPINION"

        if (
            evidence_direction
            in ("BULLISH", "BEARISH")
            and not qualified
        ):
            decision_reason = (
                f"{evidence_direction}_EVIDENCE_PRESENT_"
                "BUT_CANDIDATE_NOT_QUALIFIED"
            )
        else:
            decision_reason = (
                "NO_ACTIVE_QUALIFIED_TURN"
            )

    else:
        decision = "LIFECYCLE_STATE_ONLY"
        decision_reason = (
            f"STATUS_{status}"
        )

    # ---------------------------------------------------------
    # NEW: analytics-only DXY timing/location interpretation.
    #
    # This layer explains whether the directional evidence has
    # structural room or is approaching a possible reversal area.
    # It does not alter detector state or execution.
    # ---------------------------------------------------------
    analysis_direction = (
        direction
        if direction in ("BULLISH", "BEARISH")
        else evidence_direction
    )

    directional_room_atr = None
    near_directional_structure = False
    inside_directional_structure = False
    structure_conflict = False

    if analysis_direction == "BEARISH":
        directional_room_atr = downside_room_atr

        near_directional_structure = bool(
            near_h1_support
            or "H1" in near_support_tfs
            or "H4" in near_support_tfs
        )

        inside_directional_structure = bool(
            inside_h4_support
            or inside_m15_support
        )

    elif analysis_direction == "BULLISH":
        directional_room_atr = upside_room_atr

        near_directional_structure = bool(
            near_h1_resistance
            or "H1" in near_resistance_tfs
            or "H4" in near_resistance_tfs
        )

        inside_directional_structure = bool(
            inside_h4_resistance
            or inside_m15_resistance
        )

    structure_conflict = bool(
        analysis_direction in ("BULLISH", "BEARISH")
        and (
            near_directional_structure
            or inside_directional_structure
        )
    )

    # Conservative structural room labels.
    if directional_room_atr is None:
        room_class = "UNKNOWN"

    elif directional_room_atr < 0.25:
        room_class = "VERY_LOW"

    elif directional_room_atr < 0.50:
        room_class = "LOW"

    elif directional_room_atr < 1.00:
        room_class = "MODERATE"

    else:
        room_class = "OPEN"

    # Estimate reversal/location risk from structure only.
    if inside_directional_structure:
        reversal_risk = "VERY_HIGH"

    elif (
        near_directional_structure
        and directional_room_atr is not None
        and directional_room_atr < 0.50
    ):
        reversal_risk = "HIGH"

    elif near_directional_structure:
        reversal_risk = "ELEVATED"

    elif (
        directional_room_atr is not None
        and directional_room_atr < 0.50
    ):
        reversal_risk = "ELEVATED"

    else:
        reversal_risk = "LOW"

    # Timing phase describes lifecycle + location.
    if status == "UNAVAILABLE":
        timing_phase = "UNAVAILABLE"
        timing_opinion = "IGNORE"
        timing_reason = "DXY_STATE_UNAVAILABLE"

    elif analysis_direction not in (
        "BULLISH",
        "BEARISH",
    ):
        timing_phase = "NO_DIRECTION"
        timing_opinion = "IGNORE"
        timing_reason = "NO_DIRECTIONAL_DXY_EVIDENCE"

    elif (
        status == "CONFIRMED"
        and qualified
        and structure_conflict
    ):
        timing_phase = "CONFIRMED_NEAR_STRUCTURE"
        timing_opinion = "CAUTION"
        timing_reason = (
            f"CONFIRMED_{analysis_direction}_"
            "BUT_NEAR_OPPOSING_STRUCTURE"
        )

    elif (
        status == "CONFIRMED"
        and qualified
        and not structure_conflict
        and room_class in ("MODERATE", "OPEN")
    ):
        timing_phase = "CONFIRMED_WITH_ROOM"
        timing_opinion = "CONFIRM"
        timing_reason = (
            f"CONFIRMED_{analysis_direction}_"
            "WITH_DIRECTIONAL_ROOM"
        )

    elif (
        status == "CONFIRMED"
        and qualified
    ):
        timing_phase = "CONFIRMED_LIMITED_ROOM"
        timing_opinion = "CAUTION"
        timing_reason = (
            f"CONFIRMED_{analysis_direction}_"
            f"WITH_{room_class}_ROOM"
        )

    elif (
        status == "PENDING"
        and structure_conflict
    ):
        timing_phase = "DEVELOPING_NEAR_STRUCTURE"
        timing_opinion = "WAIT"
        timing_reason = (
            f"PENDING_{analysis_direction}_"
            "NEAR_OPPOSING_STRUCTURE"
        )

    elif status == "PENDING":
        timing_phase = "DEVELOPING_WITH_ROOM"
        timing_opinion = "WAIT"
        timing_reason = (
            f"PENDING_{analysis_direction}_"
            "NOT_YET_CONFIRMED"
        )

    elif (
        status == "IDLE"
        and evidence_direction
        in ("BULLISH", "BEARISH")
        and structure_conflict
    ):
        timing_phase = "EVIDENCE_NEAR_STRUCTURE"
        timing_opinion = "WAIT"
        timing_reason = (
            f"{evidence_direction}_EVIDENCE_"
            "AT_POSSIBLE_REVERSAL_STRUCTURE"
        )

    elif (
        status == "IDLE"
        and evidence_direction
        in ("BULLISH", "BEARISH")
    ):
        timing_phase = "UNQUALIFIED_EVIDENCE"
        timing_opinion = "IGNORE"
        timing_reason = (
            f"{evidence_direction}_EVIDENCE_"
            "NOT_QUALIFIED"
        )

    else:
        timing_phase = "NO_ACTIVE_TURN"
        timing_opinion = "IGNORE"
        timing_reason = "NO_ACTIVE_DXY_TURN"

    return {
        "schema_version": 2,
        "analytics_only": True,

        "decision": decision,
        "decision_reason": decision_reason,

        "status": status,
        "direction": direction,
        "trade_alignment": alignment,

        "evidence_direction": evidence_direction,
        "bull_score": _safe_int(
            features.get("bull_evidence_score"),
            _safe_int(
                state.get("bull_score"),
                0,
            ),
        ),
        "bear_score": _safe_int(
            features.get("bear_evidence_score"),
            _safe_int(
                state.get("bear_score"),
                0,
            ),
        ),
        "evidence_margin": _safe_float(
            features.get("evidence_margin")
        ),

        "candidate_qualified": qualified,
        "candidate_qualification_reason": (
            qualification_reason
        ),

        "positive_factors": positive_factors,
        "penalties": penalties,
        "opposing_factors": opposing_factors,
        "blockers": list(
            dict.fromkeys(blockers)
        ),

        "top_positive_factor": (
            positive_factors[0]
            if positive_factors
            else None
        ),
        "top_penalty": (
            penalties[0]
            if penalties
            else None
        ),
        "top_opposing_factor": (
            opposing_factors[0]
            if opposing_factors
            else None
        ),

        "sr_context": sr_context,

        # New analytics-only timing fields.
        "analysis_direction": analysis_direction,
        "timing_phase": timing_phase,
        "timing_opinion": timing_opinion,
        "timing_reason": timing_reason,
        "directional_room_atr": (
            round(directional_room_atr, 4)
            if directional_room_atr is not None
            else None
        ),
        "room_class": room_class,
        "reversal_risk": reversal_risk,
        "structure_conflict": structure_conflict,
        "near_directional_structure": (
            near_directional_structure
        ),
        "inside_directional_structure": (
            inside_directional_structure
        ),
    }

def _capture_real_dxy_m15_extreme_impulse(
    R,
    device_id: str,
    entry_ms: int,
) -> dict:
    """Freeze the latest completed canonical REAL DXY M15 candle at entry.

    This is deliberately independent of the normal DXY turn classifier.  It
    answers only one Phase-1 question: was the latest completed REAL DXY M15
    candle an unusually large *directional* impulse?

    Analytics only.  No trading/gate/risk/execution side effects.
    """
    out = {
        "schema_version": 1,
        "analytics_only": True,
        "source": "REAL_DXY",
        "device_id": str(device_id or "").strip() or None,
        "available": False,
        "fresh": False,
        "classification": "NOT_EVALUATED",
        "extreme_impulse": False,
        "direction": "NEUTRAL",
        "shadow_entry_action": None,
        "shadow_entry_reason": None,
        "bar_open_ms": None,
        "bar_close_ms": None,
        "open": None,
        "high": None,
        "low": None,
        "close": None,
        "range": None,
        "body": None,
        "atr14": None,
        "range_atr": None,
        "body_atr": None,
        "body_ratio": None,
        "range_pct": None,
        "body_pct": None,
        "signed_change_pct": None,
        "snapshot_received_ms": None,
        "snapshot_age_ms": None,
        "thresholds": {
            "range_atr_gte": DXY_M15_EXTREME_RANGE_ATR,
            "body_atr_gte": DXY_M15_EXTREME_BODY_ATR,
            "body_ratio_gte": DXY_M15_EXTREME_BODY_RATIO,
        },
        "unavailable_reason": None,
    }

    try:
        dev = str(device_id or "").strip()
        if not dev:
            out["classification"] = "UNAVAILABLE"
            out["unavailable_reason"] = "REAL_DXY_DEVICE_MISSING"
            return out

        raw = R.get(f"xtl:ohlc:snap:{dev}:DXY:M15") if R is not None else None
        snap = _dxy_m15_decode(raw, {})
        if not isinstance(snap, dict) or not snap:
            out["classification"] = "UNAVAILABLE"
            out["unavailable_reason"] = "REAL_DXY_M15_SNAPSHOT_MISSING"
            return out

        bars = snap.get("bars") if isinstance(snap.get("bars"), list) else []
        completed = [
            b for b in bars
            if isinstance(b, dict) and b.get("complete") is not False
        ]
        if len(completed) < 15:
            out["classification"] = "UNAVAILABLE"
            out["unavailable_reason"] = "INSUFFICIENT_COMPLETED_M15_BARS"
            return out

        received_ms = _safe_int(
            snap.get("server_received_ms")
            or snap.get("received_at_ms")
            or snap.get("published_at_ms"),
            0,
        ) or 0
        if 0 < received_ms < 10_000_000_000:
            received_ms *= 1000
        now_ms = int(entry_ms or _now_ms())
        age_ms = max(0, now_ms - received_ms) if received_ms else None
        fresh = bool(
            age_ms is not None
            and age_ms <= DXY_M15_EXTREME_SNAPSHOT_FRESH_MS
        )

        b = completed[-1]
        o = _safe_float(b.get("o")); h = _safe_float(b.get("h"))
        l = _safe_float(b.get("l")); c = _safe_float(b.get("c"))
        if None in (o, h, l, c) or h < l:
            out["classification"] = "UNAVAILABLE"
            out["unavailable_reason"] = "INVALID_LATEST_COMPLETED_M15_BAR"
            return out

        atr = _atr_from_bars(completed, 14)
        if not atr or atr <= 0:
            out["classification"] = "UNAVAILABLE"
            out["unavailable_reason"] = "M15_ATR_UNAVAILABLE"
            return out

        rng = max(0.0, h - l)
        body = abs(c - o)
        range_atr = rng / atr if atr > 0 else None
        body_atr = body / atr if atr > 0 else None
        body_ratio = body / rng if rng > 0 else 0.0
        range_pct = (rng / o) * 100.0 if o else None
        body_pct = (body / o) * 100.0 if o else None
        signed_change_pct = ((c - o) / o) * 100.0 if o else None
        direction = "BULLISH" if c > o else "BEARISH" if c < o else "NEUTRAL"

        extreme = bool(
            fresh
            and direction in ("BULLISH", "BEARISH")
            and range_atr is not None
            and range_atr >= DXY_M15_EXTREME_RANGE_ATR
            and body_atr is not None
            and body_atr >= DXY_M15_EXTREME_BODY_ATR
            and body_ratio >= DXY_M15_EXTREME_BODY_RATIO
        )

        bar_open_ms = _bar_ms_any(b) or None
        bar_close_ms = (bar_open_ms + 15 * 60 * 1000) if bar_open_ms else None

        out.update({
            "available": True,
            "fresh": fresh,
            "classification": (
                "REAL_DXY_M15_EXTREME_IMPULSE"
                if extreme
                else "REAL_DXY_M15_NORMAL"
            ),
            "extreme_impulse": extreme,
            "direction": direction,
            "shadow_entry_action": "WAIT_NEW_ENTRY" if extreme else None,
            "shadow_entry_reason": (
                "REAL_DXY_M15_EXTREME_IMPULSE" if extreme else None
            ),
            "bar_open_ms": bar_open_ms,
            "bar_close_ms": bar_close_ms,
            "open": o,
            "high": h,
            "low": l,
            "close": c,
            "range": round(rng, 8),
            "body": round(body, 8),
            "atr14": round(float(atr), 8),
            "range_atr": round(float(range_atr), 4),
            "body_atr": round(float(body_atr), 4),
            "body_ratio": round(float(body_ratio), 4),
            "range_pct": round(float(range_pct), 5) if range_pct is not None else None,
            "body_pct": round(float(body_pct), 5) if body_pct is not None else None,
            "signed_change_pct": (
                round(float(signed_change_pct), 5)
                if signed_change_pct is not None
                else None
            ),
            "snapshot_received_ms": received_ms or None,
            "snapshot_age_ms": age_ms,
            "unavailable_reason": None if fresh else "REAL_DXY_M15_SNAPSHOT_STALE",
        })
        return out

    except Exception as exc:
        out["classification"] = "UNAVAILABLE"
        out["unavailable_reason"] = (
            f"EXTREME_IMPULSE_CAPTURE_EXCEPTION:{type(exc).__name__}"
        )
        return out


def _dxy_m15_state_snapshot(R, source: str, device_id: str, symbol: str, side: str, now_ms: int) -> dict:
    out = {
        "source": str(source).upper(),
        "device_id": str(device_id or ""),
        "available": False,
        "fresh": False,
        "status": "UNAVAILABLE",
        "direction": "NEUTRAL",
        "trade_alignment": "NEUTRAL",
        "unavailable_reason": None,
    }
    try:
        if not device_id:
            out["unavailable_reason"] = "DEVICE_ID_MISSING"
            return out
        raw = R.get(_dxy_m15_state_key(source, device_id))
        state = _dxy_m15_decode(raw, {})
        if not isinstance(state, dict) or not state:
            out["unavailable_reason"] = "STATE_NOT_FOUND"
            return out

        direction = _dxy_m15_direction(state.get("direction") or state.get("candidate_direction"))
        status = str(state.get("status") or "IDLE").upper().strip()
        detected_ms = _safe_int(state.get("detected_at_ms"), 0) or 0
        bar_close_ms = _safe_int(state.get("last_evaluated_bar_close_ms"), 0) or 0
        freshness_anchor = max(detected_ms, bar_close_ms)
        age_ms = max(0, int(now_ms) - freshness_anchor) if freshness_anchor else None
        fresh = bool(age_ms is not None and age_ms <= DXY_M15_STATE_FRESH_MS)

        out.update({
            "available": True,
            "fresh": fresh,
            "stale": not fresh,
            "state_age_ms": age_ms,
            "schema_version": state.get("schema_version"),
            "model": state.get("model"),
            "status": status,
            "direction": direction,
            "trade_alignment": _dxy_m15_trade_alignment(symbol, side, direction),
            "candidate_started_ms": state.get("candidate_started_ms"),
            "candidate_age_bars": state.get("candidate_age_bars"),
            "candidate_start_price": state.get("candidate_start_price"),
            "candidate_start_atr": state.get("candidate_start_atr"),
            "confirmed_ms": state.get("confirmed_ms"),
            "support_bars": state.get("support_bars"),
            "confidence": state.get("confidence"),
            "bull_score": state.get("bull_score"),
            "bear_score": state.get("bear_score"),
            "accumulated_score": state.get("accumulated_score"),
            "directional_move_atr": state.get("directional_move_atr"),
            "max_favorable_atr": state.get("max_favorable_atr"),
            "max_adverse_atr": state.get("max_adverse_atr"),
            "revoke_score": state.get("revoke_score"),
            "revoke_reasons": state.get("revoke_reasons") or [],
            "last_event_status": state.get("last_event_status"),
            "last_event_reason": state.get("last_event_reason"),
            "last_event_ms": state.get("last_event_ms"),
            "last_evaluated_bar_close_ms": bar_close_ms or None,
            "broker_bar_close_ms": state.get("broker_bar_close_ms"),
            "broker_offset_minutes": state.get("broker_offset_minutes"),
            "detected_at_ms": detected_ms or None,
            "features": _dxy_m15_compact_features(state.get("features") or {}),
            "shadow_only": True,
            "unavailable_reason": None,
        })
        out["reasoning"] = _dxy_m15_reasoning(out)
        return out
    except Exception as exc:
        out["unavailable_reason"] = f"STATE_READ_EXCEPTION:{type(exc).__name__}"
        return out


def read_dxy_m15_at_entry(device_id: str, symbol: str, side: str, entry_ms: int,*,
    trade_firm: str | None = None,
    trade_profile_id: str | None = None,) -> dict:
    """Freeze canonical REAL plus same-trade-device SYNTHETIC fallback states."""
    trade_dev = str(device_id or "").strip()
    trade_firm_u = str(trade_firm or "").strip().lower()
    trade_profile = str(trade_profile_id or "").strip().lower()
    result = {
        "schema_version": 3,
        "captured_at_ms": int(entry_ms or _now_ms()),
        "device_id": trade_dev,
        "trade_device_id": trade_dev or None,
        "trade_firm": trade_firm_u or None,
        "trade_profile_id": trade_profile or None,
        "canonical_real_device_id": None,
        "canonical_real_firm": None,
        "canonical_real_profile_id": None,

        
        "sources": {},
        "source_devices": {},
        "source_firms": {},
        "source_profiles": {},
        "selected_source": None,
        "selected_device_id": None,
        "selected_firm": None,
        "selected_profile_id": None,
        "fallback_used": False,
        "fallback_reason": None,
        "selected": None,
        "real_dxy_extreme_impulse": None,
        "shadow_only": True,
    }
    try:
        R = from_app_R()
        canonical = resolve_canonical_dxy_source(R, trade_dev,trade_firm=trade_firm_u,trade_profile_id=trade_profile,)
        real_dev = str(canonical.get("real_device_id") or "").strip()
        synthetic_dev = str(canonical.get("synthetic_device_id") or "").strip()
        result["canonical_real_device_id"] = real_dev or None
        result["canonical_real_firm"] = canonical.get("real_firm")
        result["canonical_real_profile_id"] = canonical.get(
            "real_profile_id"
        )

        result["source_firms"] = {
            "REAL_DXY": canonical.get("real_firm"),
            "SYNTHETIC_DXY": canonical.get("synthetic_firm"),
        }

        result["source_profiles"] = {
            "REAL_DXY": canonical.get("real_profile_id"),
            "SYNTHETIC_DXY": canonical.get("synthetic_profile_id"),
        }
        result["canonical_config_source"] = canonical.get("configured_by")
        result["canonical_config_error"] = canonical.get("config_error")
        result["source_devices"] = {
            "REAL_DXY": real_dev or None,
            "SYNTHETIC_DXY": synthetic_dev or None,
        }

        result["sources"]["REAL_DXY"] = _dxy_m15_state_snapshot(
            R, "REAL_DXY", real_dev, symbol, side, int(entry_ms or _now_ms())
        )
        result["sources"]["SYNTHETIC_DXY"] = _dxy_m15_state_snapshot(
            R, "SYNTHETIC_DXY", synthetic_dev, symbol, side, int(entry_ms or _now_ms())
        )

        # Phase-1 only: freeze the latest completed canonical REAL DXY M15
        # candle and classify unusually large directional displacement.  This
        # remains independent of the normal DXY turn state and never blocks.
        result["real_dxy_extreme_impulse"] = (
            _capture_real_dxy_m15_extreme_impulse(
                R,
                real_dev,
                int(entry_ms or _now_ms()),
            )
        )

        real = result["sources"].get("REAL_DXY") or {}
        synthetic = result["sources"].get("SYNTHETIC_DXY") or {}
        if real.get("available") and real.get("fresh"):
            selected_source = "REAL_DXY"
        elif synthetic.get("available") and synthetic.get("fresh"):
            selected_source = "SYNTHETIC_DXY"
        elif real.get("available"):
            selected_source = "REAL_DXY"
        elif synthetic.get("available"):
            selected_source = "SYNTHETIC_DXY"
        else:
            selected_source = None

        result["selected_source"] = selected_source
        result["selected"] = result["sources"].get(selected_source) if selected_source else None
        result["selected_device_id"] = (
            (result.get("selected") or {}).get("device_id")
            if selected_source
            else None
        )
        if selected_source == "REAL_DXY":
            result["selected_firm"] = canonical.get("real_firm")
            result["selected_profile_id"] = canonical.get(
                "real_profile_id"
            )

        elif selected_source == "SYNTHETIC_DXY":
            result["selected_firm"] = trade_firm_u or None
            result["selected_profile_id"] = trade_profile or None
        result["fallback_used"] = selected_source == "SYNTHETIC_DXY"
        if selected_source == "SYNTHETIC_DXY":
            result["fallback_reason"] = (
                "CANONICAL_REAL_DXY_STALE"
                if real.get("available") and not real.get("fresh")
                else "CANONICAL_REAL_DXY_UNAVAILABLE"
            )

        elif selected_source is None:
            real_available = bool(real.get("available"))
            real_fresh = bool(real.get("fresh"))
            synthetic_available = bool(
                synthetic.get("available")
            )
            synthetic_fresh = bool(
                synthetic.get("fresh")
            )

            if not real_available and not synthetic_available:
                result["fallback_reason"] = (
                    "REAL_AND_SYNTHETIC_DXY_UNAVAILABLE"
                )

            elif not real_fresh and not synthetic_fresh:
                result["fallback_reason"] = (
                    "REAL_AND_SYNTHETIC_DXY_STALE"
                )

            elif not synthetic_available:
                result["fallback_reason"] = (
                    "SYNTHETIC_DXY_UNAVAILABLE"
                )

            elif not synthetic_fresh:
                result["fallback_reason"] = (
                    "SYNTHETIC_DXY_STALE"
                )

            else:
                result["fallback_reason"] = (
                    "NO_DXY_SOURCE_SELECTED"
                )
        log.warning(
            "[COMMON_DXY] "
            "trade_firm=%s trade_profile=%s trade_device=%s "
            "selected_source=%s selected_firm=%s "
            "selected_profile=%s selected_device=%s "
            "fallback_used=%s",
            trade_firm_u or None,
            trade_profile or None,
            trade_dev,
            selected_source,
            result.get("selected_firm"),
            result.get("selected_profile_id"),
            result.get("selected_device_id"),
            result.get("fallback_used"),
        )
        if selected_source == "SYNTHETIC_DXY":
            result["fallback_reason"] = (
                "CANONICAL_REAL_DXY_STALE"
                if real.get("available") and not real.get("fresh")
                else "CANONICAL_REAL_DXY_UNAVAILABLE"
            )
        result["dxy_reasoning"] = (
            _dxy_m15_reasoning(result["selected"])
            if isinstance(result.get("selected"), dict)
            else {
                "schema_version": 1,
                "analytics_only": True,
                "decision": "DXY_UNAVAILABLE",
                "decision_reason": "NO_SOURCE_SELECTED",
            }
        )
        return result
    except Exception as exc:
        result["capture_error"] = f"{type(exc).__name__}:{exc}"
        return result



def _dxy_h1_decode(
    raw,
    default=None,
):
    try:
        if raw is None:
            return default

        if isinstance(
            raw,
            (bytes, bytearray),
        ):
            raw = raw.decode(
                "utf-8",
                "replace",
            )

        value = json.loads(raw)

        return (
            value
            if isinstance(value, dict)
            else default
        )

    except Exception:
        return default


def _dxy_h1_feature_key(
    source: str,
    device_id: str,
    broker_close_ms: int,
) -> str:
    return (
        f"{DXY_H1_FEATURE_PREFIX}:"
        f"{str(source).upper().strip()}:"
        f"{str(device_id or '').strip()}:"
        f"{int(broker_close_ms or 0)}"
    )


def _dxy_h1_latest_key(
    source: str,
    device_id: str,
) -> str:
    return (
        f"{DXY_H1_LATEST_PREFIX}:"
        f"{str(source).upper().strip()}:"
        f"{str(device_id or '').strip()}"
    )


def _dxy_h1_feature_at_or_before(
    R,
    *,
    source: str,
    device_id: str,
    symbol: str,
    side: str,
    entry_ms: int,
) -> dict:
    """
    Read the latest completed H1 feature whose broker bar close is not
    later than the trade-entry timestamp.

    This prevents future-candle leakage when analytics capture is delayed,
    repaired, replayed or performed after a service restart.
    """
    source_u = str(
        source or ""
    ).upper().strip()

    device_u = str(
        device_id or ""
    ).strip()

    entry_ms_i = _safe_int(
        entry_ms,
        0,
    ) or 0

    out = {
        "source": source_u,
        "device_id": device_u or None,
        "available": False,
        "fresh": False,
        "stale": False,
        "feature_age_ms": None,
        "unavailable_reason": None,
        "feature_key": None,
        "feature": None,
    }

    if not device_u:
        out["unavailable_reason"] = (
            "DEVICE_ID_MISSING"
        )
        return out

    if entry_ms_i <= 0:
        out["unavailable_reason"] = (
            "ENTRY_TIMESTAMP_MISSING"
        )
        return out

    try:
        selected = None
        selected_key = None
        selected_close_ms = 0

        # ---------------------------------------------------------
        # Fast path.
        #
        # In normal live capture, the latest immutable H1 payload is
        # normally the correct completed bar at entry.
        # ---------------------------------------------------------
        latest_raw = R.get(
            _dxy_h1_latest_key(
                source_u,
                device_u,
            )
        )

        latest = _dxy_h1_decode(
            latest_raw,
            {},
        )

        latest_close_ms = _safe_int(
            latest.get(
                "broker_bar_close_ms"
            )
            if isinstance(latest, dict)
            else 0,
            0,
        ) or 0

        if (
            isinstance(latest, dict)
            and latest
            and latest_close_ms > 0
            and latest_close_ms <= entry_ms_i
        ):
            selected = latest
            selected_close_ms = (
                latest_close_ms
            )
            selected_key = (
                _dxy_h1_feature_key(
                    source_u,
                    device_u,
                    selected_close_ms,
                )
            )

        # ---------------------------------------------------------
        # Historical fallback.
        #
        # Required for delayed ACK, broker repair, restart recovery
        # and analytics replay. Select the greatest close timestamp
        # that is <= entry time.
        # ---------------------------------------------------------
        if selected is None:
            pattern = (
                f"{DXY_H1_FEATURE_PREFIX}:"
                f"{source_u}:"
                f"{device_u}:*"
            )

            for raw_key in R.scan_iter(
                match=pattern,
                count=200,
            ):
                key_s = (
                    raw_key.decode(
                        "utf-8",
                        "ignore",
                    )
                    if isinstance(
                        raw_key,
                        (bytes, bytearray),
                    )
                    else str(raw_key)
                )

                try:
                    close_ms = int(
                        key_s.rsplit(
                            ":",
                            1,
                        )[-1]
                    )
                except Exception:
                    continue

                if (
                    close_ms <= 0
                    or close_ms > entry_ms_i
                    or close_ms <= selected_close_ms
                ):
                    continue

                raw_payload = R.get(
                    raw_key
                )

                payload = _dxy_h1_decode(
                    raw_payload,
                    {},
                )

                if not payload:
                    continue

                payload_close_ms = (
                    _safe_int(
                        payload.get(
                            "broker_bar_close_ms"
                        ),
                        close_ms,
                    )
                    or close_ms
                )

                if payload_close_ms > entry_ms_i:
                    continue

                selected = payload
                selected_key = key_s
                selected_close_ms = (
                    payload_close_ms
                )

        if not isinstance(
            selected,
            dict,
        ) or not selected:
            out["unavailable_reason"] = (
                "FEATURE_NOT_FOUND_AT_OR_BEFORE_ENTRY"
            )
            return out

        feature_age_ms = max(
            0,
            entry_ms_i
            - int(selected_close_ms),
        )

        fresh = (
            feature_age_ms
            <= DXY_H1_FEATURE_FRESH_MS
        )

        direction = str(
            selected.get(
                "candidate_direction"
            )
            or "NEUTRAL"
        ).upper().strip()

        if direction not in (
            "BULLISH",
            "BEARISH",
            "NEUTRAL",
        ):
            direction = "NEUTRAL"

        evidence_direction = str(
            selected.get(
                "evidence_direction"
            )
            or "NEUTRAL"
        ).upper().strip()

        if evidence_direction not in (
            "BULLISH",
            "BEARISH",
            "NEUTRAL",
        ):
            evidence_direction = "NEUTRAL"

        compact_feature = {
            "schema_version": selected.get(
                "schema_version"
            ),
            "model": selected.get(
                "model"
            ),
            "timeframe": "H1",
            "source": source_u,
            "device_id": device_u,

            "broker_bar_open_ms": (
                selected.get(
                    "broker_bar_open_ms"
                )
            ),
            "broker_bar_close_ms": (
                selected_close_ms
            ),
            "bar_close_ms": selected.get(
                "bar_close_ms"
            ),
            "detected_at_ms": selected.get(
                "detected_at_ms"
            ),

            "candidate_direction": (
                direction
            ),
            "candidate_confidence": (
                _safe_int(
                    selected.get(
                        "candidate_confidence"
                    ),
                    0,
                )
            ),

            "evidence_direction": (
                evidence_direction
            ),
            "bull_evidence_score": (
                _safe_int(
                    selected.get(
                        "bull_evidence_score"
                    ),
                    0,
                )
            ),
            "bear_evidence_score": (
                _safe_int(
                    selected.get(
                        "bear_evidence_score"
                    ),
                    0,
                )
            ),
            "evidence_margin": (
                _safe_float(
                    selected.get(
                        "evidence_margin"
                    )
                )
            ),

            "bullish_structure_break": bool(
                selected.get(
                    "bullish_structure_break"
                )
            ),
            "bearish_structure_break": bool(
                selected.get(
                    "bearish_structure_break"
                )
            ),

            "recent_net_atr": _safe_float(
                selected.get(
                    "recent_net_atr"
                )
            ),
            "prior_net_atr": _safe_float(
                selected.get(
                    "prior_net_atr"
                )
            ),
            "slope_5_atr": _safe_float(
                selected.get(
                    "slope_5_atr"
                )
            ),
            "prior_slope_5_atr": (
                _safe_float(
                    selected.get(
                        "prior_slope_5_atr"
                    )
                )
            ),
            "slope_12_atr": _safe_float(
                selected.get(
                    "slope_12_atr"
                )
            ),
            "acceleration_atr": (
                _safe_float(
                    selected.get(
                        "acceleration_atr"
                    )
                )
            ),

            "up_steps": _safe_int(
                selected.get(
                    "up_steps"
                ),
                0,
            ),
            "down_steps": _safe_int(
                selected.get(
                    "down_steps"
                ),
                0,
            ),
            "bullish_bodies": _safe_int(
                selected.get(
                    "bullish_bodies"
                ),
                0,
            ),
            "bearish_bodies": _safe_int(
                selected.get(
                    "bearish_bodies"
                ),
                0,
            ),

            "pin_bar_direction": (
                selected.get(
                    "pin_bar_direction"
                )
            ),
            "compression_ratio": (
                _safe_float(
                    selected.get(
                        "compression_ratio"
                    )
                )
            ),
            "expansion_ratio": (
                _safe_float(
                    selected.get(
                        "expansion_ratio"
                    )
                )
            ),
            "body_ratio": _safe_float(
                selected.get(
                    "body_ratio"
                )
            ),

            "bars_used": _safe_int(
                selected.get(
                    "bars_used"
                ),
                0,
            ),

            "sr_status": selected.get(
                "sr_status"
            ),

            "shadow_only": True,
            "execution_wired": False,
            "entry_gate_wired": False,
            "risk_wired": False,
        }

        out.update({
            "available": True,
            "fresh": bool(fresh),
            "stale": not bool(fresh),
            "feature_age_ms": int(
                feature_age_ms
            ),
            "feature_key": selected_key,
            "broker_bar_close_ms": int(
                selected_close_ms
            ),
            "direction": direction,
            "confidence": (
                compact_feature.get(
                    "candidate_confidence"
                )
            ),
            "evidence_direction": (
                evidence_direction
            ),
            "trade_alignment": (
                _dxy_m15_trade_alignment(
                    symbol,
                    side,
                    direction,
                )
            ),
            "feature": compact_feature,
            "unavailable_reason": (
                None
                if fresh
                else "FEATURE_STALE_AT_ENTRY"
            ),
        })

        return out

    except Exception as exc:
        out["unavailable_reason"] = (
            "FEATURE_READ_EXCEPTION:"
            f"{type(exc).__name__}"
        )
        return out


def _dxy_h1_source_agreement(
    real: dict,
    synthetic: dict,
) -> str:
    real_available = bool(
        isinstance(real, dict)
        and real.get("available")
        and real.get("fresh")
    )

    synthetic_available = bool(
        isinstance(synthetic, dict)
        and synthetic.get("available")
        and synthetic.get("fresh")
    )

    if (
        not real_available
        and not synthetic_available
    ):
        return "UNAVAILABLE"

    if not real_available:
        return "SYNTHETIC_ONLY"

    if not synthetic_available:
        return "REAL_ONLY"

    real_dir = str(
        real.get("direction")
        or "NEUTRAL"
    ).upper()

    synthetic_dir = str(
        synthetic.get("direction")
        or "NEUTRAL"
    ).upper()

    if real_dir == synthetic_dir:
        return (
            "AGREE_NEUTRAL"
            if real_dir == "NEUTRAL"
            else "AGREE"
        )

    if (
        real_dir == "NEUTRAL"
        or synthetic_dir == "NEUTRAL"
    ):
        return "PARTIAL"

    return "CONFLICT"


def _dxy_m15_h1_alignment(
    dxy_m15_entry: dict,
    dxy_h1_entry: dict,
) -> str:
    try:
        m15_selected = (
            dxy_m15_entry.get("selected")
            if isinstance(
                dxy_m15_entry,
                dict,
            )
            else {}
        ) or {}

        h1_selected = (
            dxy_h1_entry.get("selected")
            if isinstance(
                dxy_h1_entry,
                dict,
            )
            else {}
        ) or {}

        if (
            not m15_selected
            or not h1_selected
            or not m15_selected.get(
                "available"
            )
            or not h1_selected.get(
                "available"
            )
            or not m15_selected.get(
                "fresh",
                True,
            )
            or not h1_selected.get(
                "fresh"
            )
        ):
            return "UNAVAILABLE"

        m15_direction = str(
            m15_selected.get(
                "direction"
            )
            or "NEUTRAL"
        ).upper()

        h1_direction = str(
            h1_selected.get(
                "direction"
            )
            or "NEUTRAL"
        ).upper()

        if m15_direction == h1_direction:
            return (
                "AGREE_NEUTRAL"
                if m15_direction == "NEUTRAL"
                else "AGREE"
            )

        if (
            m15_direction == "NEUTRAL"
            or h1_direction == "NEUTRAL"
        ):
            return "PARTIAL"

        return "CONFLICT"

    except Exception:
        return "UNAVAILABLE"


def read_dxy_h1_at_entry(
    device_id: str,
    symbol: str,
    side: str,
    entry_ms: int,
    *,
    trade_firm: str | None = None,
    trade_profile_id: str | None = None,
    dxy_m15_entry: dict | None = None,
) -> dict:
    """
    Freeze REAL and SYNTHETIC native-H1 directional features at entry.

    Shadow analytics only. This function does not affect production
    direction, entry permission, risk, sizing or order placement.
    """
    trade_dev = str(
        device_id or ""
    ).strip()

    trade_firm_u = str(
        trade_firm or ""
    ).strip().lower()

    trade_profile = str(
        trade_profile_id or ""
    ).strip().lower()

    entry_ms_i = int(
        entry_ms or _now_ms()
    )

    result = {
        "schema_version": 1,
        "model": (
            "DXY_H1_ENTRY_SNAPSHOT_V1_"
            "SHADOW_ANALYTICS"
        ),
        "timeframe": "H1",
        "captured_at_ms": entry_ms_i,

        "trade_device_id": (
            trade_dev or None
        ),
        "trade_firm": (
            trade_firm_u or None
        ),
        "trade_profile_id": (
            trade_profile or None
        ),

        "canonical_real_device_id": None,
        "sources": {},
        "source_devices": {},

        "selected_source": None,
        "selected_device_id": None,
        "selected": None,

        "real_synthetic_agreement": (
            "UNAVAILABLE"
        ),
        "m15_h1_alignment": (
            "UNAVAILABLE"
        ),

        "fallback_used": False,
        "fallback_reason": None,

        "shadow_only": True,
        "analytics_only": True,
        "execution_wired": False,
        "entry_gate_wired": False,
        "risk_wired": False,
    }

    try:
        R = from_app_R()

        canonical = resolve_canonical_dxy_source(
            R,
            trade_dev,
            trade_firm=trade_firm_u,
            trade_profile_id=trade_profile,
        )

        real_dev = str(
            canonical.get(
                "real_device_id"
            )
            or ""
        ).strip()

        synthetic_dev = str(
            canonical.get(
                "synthetic_device_id"
            )
            or trade_dev
            or ""
        ).strip()

        result[
            "canonical_real_device_id"
        ] = real_dev or None

        result["source_devices"] = {
            "REAL_DXY": real_dev or None,
            "SYNTHETIC_DXY": (
                synthetic_dev or None
            ),
        }

        real = (
            _dxy_h1_feature_at_or_before(
                R,
                source="REAL_DXY",
                device_id=real_dev,
                symbol=symbol,
                side=side,
                entry_ms=entry_ms_i,
            )
        )

        synthetic = (
            _dxy_h1_feature_at_or_before(
                R,
                source="SYNTHETIC_DXY",
                device_id=synthetic_dev,
                symbol=symbol,
                side=side,
                entry_ms=entry_ms_i,
            )
        )

        result["sources"] = {
            "REAL_DXY": real,
            "SYNTHETIC_DXY": synthetic,
        }

        # Same source-selection policy as existing M15 entry capture:
        # fresh canonical REAL first, then fresh SYNTHETIC fallback.
        if (
            real.get("available")
            and real.get("fresh")
        ):
            selected_source = "REAL_DXY"

        elif (
            synthetic.get("available")
            and synthetic.get("fresh")
        ):
            selected_source = (
                "SYNTHETIC_DXY"
            )

        elif real.get("available"):
            selected_source = "REAL_DXY"

        elif synthetic.get("available"):
            selected_source = (
                "SYNTHETIC_DXY"
            )

        else:
            selected_source = None

        result["selected_source"] = (
            selected_source
        )

        result["selected"] = (
            result["sources"].get(
                selected_source
            )
            if selected_source
            else None
        )

        result["selected_device_id"] = (
            (result.get("selected") or {}).get(
                "device_id"
            )
            if selected_source
            else None
        )

        result["fallback_used"] = (
            selected_source
            == "SYNTHETIC_DXY"
        )

        if selected_source == "SYNTHETIC_DXY":
            result["fallback_reason"] = (
                "CANONICAL_REAL_H1_STALE"
                if real.get("available")
                and not real.get("fresh")
                else
                "CANONICAL_REAL_H1_UNAVAILABLE"
            )

        elif selected_source is None:
            result["fallback_reason"] = (
                "REAL_AND_SYNTHETIC_H1_"
                "UNAVAILABLE"
            )

        result[
            "real_synthetic_agreement"
        ] = _dxy_h1_source_agreement(
            real,
            synthetic,
        )

        result["m15_h1_alignment"] = (
            _dxy_m15_h1_alignment(
                dxy_m15_entry or {},
                result,
            )
        )

        return result

    except Exception as exc:
        result["capture_error"] = (
            f"{type(exc).__name__}:{exc}"
        )
        return result

def _dxy_m15_event_id(event: dict) -> str:
    return "|".join((
        str(event.get("source") or ""),
        str(event.get("change_ms") or event.get("bar_close_ms") or 0),
        str(event.get("status") or ""),
        str(event.get("direction") or ""),
    ))


def _dxy_m15_compact_event(event: dict, symbol: str, side: str) -> dict:
    direction = _dxy_m15_direction(event.get("direction"))
    out = {
        "event_id": _dxy_m15_event_id(event),
        "source": event.get("source"),
        "status": str(event.get("status") or "").upper(),
        "direction": direction,
        "trade_alignment": _dxy_m15_trade_alignment(symbol, side, direction),
        "reason": event.get("reason"),
        "outcome": event.get("outcome"),
        "change_ms": event.get("change_ms") or event.get("bar_close_ms"),
        "detected_at_ms": event.get("detected_at_ms"),
        "candidate_started_ms": event.get("candidate_started_ms"),
        "confirmed_ms": event.get("confirmed_ms"),
        "candidate_age_bars": event.get("candidate_age_bars"),
        "bars_alive": event.get("bars_alive"),
        "confidence": event.get("confidence"),
        "bull_score": event.get("bull_score"),
        "bear_score": event.get("bear_score"),
        "accumulated_score": event.get("accumulated_score"),
        "directional_move_atr": event.get("directional_move_atr"),
        "max_favorable_atr": event.get("max_favorable_atr"),
        "max_adverse_atr": event.get("max_adverse_atr"),
        "peak_favorable_ms": event.get("peak_favorable_ms"),
        "bars_to_peak": event.get("bars_to_peak"),
        "end_move_atr": event.get("end_move_atr"),
        "revoke_score": event.get("revoke_score"),
        "revoke_reasons": event.get("revoke_reasons") or [],
        "historical_backfill": bool(event.get("historical_backfill")),
        "features": _dxy_m15_compact_features(event.get("features") or {}),
    }
    out["reasoning"] = _dxy_m15_reasoning({
        "available": True,
        "status": out.get("status"),
        "direction": out.get("direction"),
        "trade_alignment": out.get("trade_alignment"),
        "confidence": out.get("confidence"),
        "bull_score": out.get("bull_score"),
        "bear_score": out.get("bear_score"),
        "last_event_status": out.get("status"),
        "last_event_reason": out.get("reason"),
        "features": out.get("features") or {},
    })
    return out


def update_dxy_m15_trade_tracking(snap: dict, until_ms: int | None = None) -> bool:
    """Append post-entry DXY lifecycle events to one in-flight analytics snapshot."""
    try:
        if not isinstance(snap, dict):
            return False
        R = from_app_R()
        device_id = str(snap.get("device_id") or _resolve_device(snap) or "").strip()
        symbol = str(snap.get("symbol") or "").upper().strip()
        side = str(snap.get("side") or "").upper().strip()
        entry_ms = _safe_int(
            snap.get("broker_open_time_utc_ms")
            or snap.get("enqueue_timestamp")
            or snap.get("ts_ms"),
            0,
        ) or 0
        end_ms = int(until_ms or _now_ms())
        if not device_id or entry_ms <= 0:
            return False

        tracking = snap.get("dxy_m15_tracking") if isinstance(snap.get("dxy_m15_tracking"), dict) else {}
        tracking.setdefault("schema_version", 1)
        tracking.setdefault("entry_ms", entry_ms)
        tracking.setdefault("device_id", device_id)
        tracking.setdefault("sources", {})
        canonical = resolve_canonical_dxy_source(R, device_id)
        source_devices = {
            "REAL_DXY": str(canonical.get("real_device_id") or "").strip(),
            "SYNTHETIC_DXY": str(canonical.get("synthetic_device_id") or "").strip(),
        }
        tracking["canonical_real_device_id"] = source_devices["REAL_DXY"] or None
        tracking["source_devices"] = {
            key: value or None
            for key, value in source_devices.items()
        }
        changed = False

        for source in DXY_M15_SOURCES:
            source_device_id = source_devices.get(source) or ""
            source_rec = tracking["sources"].get(source) if isinstance(tracking["sources"].get(source), dict) else {}
            source_rec.setdefault("source", source)
            source_rec["device_id"] = source_device_id or None
            source_rec.setdefault("events", [])
            existing_ids = {
                str(e.get("event_id") or "")
                for e in source_rec["events"]
                if isinstance(e, dict)
            }

            raw_events = (
                R.lrange(_dxy_m15_history_key(source, source_device_id), 0, -1)
                if source_device_id
                else []
            ) or []
            for raw_event in raw_events:
                event = _dxy_m15_decode(raw_event, {})
                if not isinstance(event, dict):
                    continue
                event_ms = _safe_int(event.get("change_ms") or event.get("bar_close_ms"), 0) or 0
                if event_ms <= entry_ms or event_ms > end_ms:
                    continue
                # Bootstrap history represents genuine historical events, but events before
                # entry are filtered above. Keeping the flag allows later segmentation.
                compact = _dxy_m15_compact_event(event, symbol, side)
                event_id = compact["event_id"]
                if event_id and event_id not in existing_ids:
                    source_rec["events"].append(compact)
                    existing_ids.add(event_id)
                    changed = True

            source_rec["events"] = sorted(
                source_rec["events"],
                key=lambda e: int(e.get("change_ms") or 0),
            )[-DXY_M15_MAX_TRACKED_EVENTS:]
            current = _dxy_m15_state_snapshot(
                R, source, source_device_id, symbol, side, end_ms
            )
            if source_rec.get("current_state") != current:
                source_rec["current_state"] = current
                changed = True
            source_rec["last_updated_ms"] = end_ms
            tracking["sources"][source] = source_rec

        tracking["last_updated_ms"] = end_ms
        snap["dxy_m15_tracking"] = tracking
        return changed
    except Exception as exc:
        log.warning(
            "analytics: DXY M15 tracking update failed ticket=%s err=%r",
            snap.get("mt5_ticket") if isinstance(snap, dict) else None,
            exc,
        )
        return False


def _dxy_m15_source_summary(source: str, source_entry: dict, source_tracking: dict, symbol: str, side: str) -> dict:
    events = source_tracking.get("events") if isinstance(source_tracking, dict) else []
    events = [e for e in (events or []) if isinstance(e, dict)]
    confirmed = [e for e in events if e.get("status") == "CONFIRMED"]
    completed = [e for e in events if e.get("status") == "COMPLETED"]
    weak = [e for e in events if e.get("status") == "WEAK_COMPLETION"]
    failed = [e for e in events if e.get("status") == "INVALIDATED"]
    entry_direction = _dxy_m15_direction((source_entry or {}).get("direction"))
    entry_alignment = (source_entry or {}).get("trade_alignment") or _dxy_m15_trade_alignment(symbol, side, entry_direction)
    against_events = [e for e in confirmed if e.get("trade_alignment") == "AGAINST"]
    aligned_events = [e for e in confirmed if e.get("trade_alignment") == "ALIGNED"]
    strongest = max(
        events,
        key=lambda e: abs(float(e.get("max_favorable_atr") or e.get("directional_move_atr") or 0.0)),
        default=None,
    )
    return {
        "source": source,
        "entry_available": bool((source_entry or {}).get("available")),
        "entry_fresh": bool((source_entry or {}).get("fresh")),
        "entry_status": (source_entry or {}).get("status"),
        "entry_direction": entry_direction,
        "entry_alignment": entry_alignment,
        "entry_confidence": (source_entry or {}).get("confidence"),
        "entry_reasoning": (source_entry or {}).get("reasoning") or _dxy_m15_reasoning(source_entry or {}),
        "events_during_trade": len(events),
        "confirmed_turns_during_trade": len(confirmed),
        "completed_turns_during_trade": len(completed),
        "weak_turns_during_trade": len(weak),
        "failed_turns_during_trade": len(failed),
        "aligned_confirmations_during_trade": len(aligned_events),
        "against_confirmations_during_trade": len(against_events),
        "first_confirmed_ms": confirmed[0].get("change_ms") if confirmed else None,
        "first_against_confirmed_ms": against_events[0].get("change_ms") if against_events else None,
        "first_aligned_confirmed_ms": aligned_events[0].get("change_ms") if aligned_events else None,
        "direction_changed_during_trade": len({_dxy_m15_direction(e.get("direction")) for e in confirmed}) > 1,
        "strongest_event": strongest,
        "strongest_event_reasoning": strongest.get("reasoning") if isinstance(strongest, dict) else None,
        "current_state": source_tracking.get("current_state") if isinstance(source_tracking, dict) else None,
        "current_reasoning": (
            (source_tracking.get("current_state") or {}).get("reasoning")
            if isinstance(source_tracking, dict) and isinstance(source_tracking.get("current_state"), dict)
            else None
        ),
    }


def finalize_dxy_m15_trade_summary(snap: dict) -> None:
    """Freeze selected-source and per-source DXY summaries into the closed row."""
    try:
        if not isinstance(snap, dict):
            return
        close_ms = _safe_int(
            snap.get("broker_close_time_utc_ms")
            or snap.get("close_timestamp")
            or _now_ms(),
            _now_ms(),
        ) or _now_ms()
        update_dxy_m15_trade_tracking(snap, until_ms=close_ms)
        entry = snap.get("dxy_m15_entry") if isinstance(snap.get("dxy_m15_entry"), dict) else {}
        tracking = snap.get("dxy_m15_tracking") if isinstance(snap.get("dxy_m15_tracking"), dict) else {}
        symbol = str(snap.get("symbol") or "").upper().strip()
        side = str(snap.get("side") or "").upper().strip()
        summaries = {}
        for source in DXY_M15_SOURCES:
            source_entry = (entry.get("sources") or {}).get(source) if isinstance(entry.get("sources"), dict) else {}
            source_tracking = (tracking.get("sources") or {}).get(source) if isinstance(tracking.get("sources"), dict) else {}
            summaries[source] = _dxy_m15_source_summary(source, source_entry or {}, source_tracking or {}, symbol, side)

        selected_source = entry.get("selected_source")
        selected = summaries.get(selected_source) if selected_source else None
        warning = bool(
            selected
            and (
                (selected.get("entry_status") == "CONFIRMED" and selected.get("entry_alignment") == "AGAINST")
                or int(selected.get("against_confirmations_during_trade") or 0) > 0
            )
        )
        snap["dxy_m15_trade_summary"] = {
            "schema_version": 1,
            "selected_source": selected_source,
            "selected": selected,
            "dxy_reasoning": (selected or {}).get("entry_reasoning") if isinstance(selected, dict) else None,
            "sources": summaries,
            "possible_dxy_warning": warning,
            "warning_reason": (
                "ENTRY_OR_DURING_TRADE_CONFIRMED_USD_MOVE_AGAINST_TRADE"
                if warning else None
            ),
            "finalized_at_ms": _now_ms(),
            "shadow_only": True,
        }
    except Exception as exc:
        log.warning(
            "analytics: DXY M15 summary failed ticket=%s err=%r",
            snap.get("mt5_ticket") if isinstance(snap, dict) else None,
            exc,
        )

def _build_entry_usd_context(snap: dict) -> dict:
    """
    Consolidate the already-frozen DXY H1, M15 and SR data into one
    immutable entry-time USD context object.

    Analytics only. Never changes gates, entries, exits or position sizing.
    """

    def _as_dict(value):
        return value if isinstance(value, dict) else {}

    def _as_list(value):
        return value if isinstance(value, list) else []

    def _normalise_direction(value) -> str:
        value_u = str(value or "").upper().strip()

        if value_u in ("UP", "BULLISH"):
            return "BULLISH"

        if value_u in ("DOWN", "BEARISH"):
            return "BEARISH"

        if value_u in (
            "SIDEWAYS",
            "FLAT",
            "NEUTRAL",
            "IDLE",
            "",
        ):
            return "NEUTRAL"

        return value_u

    def _normalise_alignment(value) -> str:
        value_u = str(value or "").upper().strip()

        if value_u in (
            "FAVORABLE",
            "FAVOURABLE",
            "WITH",
            "ALIGNED",
        ):
            return "ALIGNED"

        if value_u in (
            "ADVERSE",
            "AGAINST",
        ):
            return "AGAINST"

        if value_u in (
            "",
            "NONE",
            "NEUTRAL",
            "UNAVAILABLE",
            "UNKNOWN",
            "ANALYTICS_ONLY",
        ):
            return "NEUTRAL"

        return value_u

    def _compact_level(level):
        level = _as_dict(level)

        if not level:
            return None

        return {
            "low": _safe_float(level.get("low")),
            "high": _safe_float(level.get("high")),
            "level": _safe_float(
                level.get("level")
                if level.get("level") is not None
                else level.get("price")
            ),
            "distance_atr": _safe_float(
                level.get("distance_atr")
            ),
            "touches": _safe_int(
                level.get("touches"),
                0,
            ),
            "strength": _safe_float(
                level.get("strength")
                if level.get("strength") is not None
                else level.get("sr_score")
            ),
            "source_tf": (
                level.get("source_tf")
                or level.get("tf")
            ),
            "maturity": level.get("maturity"),
            "age_bars": _safe_int(
                level.get("age_bars"),
                0,
            ),
            "near": bool(level.get("near")),
            "role": level.get("role"),
            "causal": bool(
                level.get("causal", True)
            ),
        }

    try:
        dxy_m15_entry = _as_dict(
            snap.get("dxy_m15_entry")
        )

        selected_m15 = _as_dict(
            dxy_m15_entry.get("selected")
        )

        selected_features = _as_dict(
            selected_m15.get("features")
        )

        selected_sr = _as_dict(
            selected_features.get("sr")
        )

        structure_context = _as_dict(
            selected_sr.get("structure_context")
        )

        h1_sr = _as_dict(
            selected_sr.get("h1")
        )

        h4_sr = _as_dict(
            selected_sr.get("h4")
        )

        m15_sr = _as_dict(
            selected_sr.get("m15")
        )

        # -------------------------------------------------------------
        # H1 direction
        #
        # usd_reference_* is canonical:
        # REAL DXY first, same-device synthetic DXY fallback.
        # -------------------------------------------------------------
        h1_direction = _normalise_direction(
            snap.get("usd_reference_direction")
            or snap.get("dxy_h1_20_direction")
        )

        h1_direction_raw = _normalise_direction(
            snap.get("usd_reference_direction_raw")
            or snap.get("dxy_h1_20_direction_raw")
        )

        h1_alignment = _normalise_alignment(
            snap.get(
                "usd_reference_alignment_at_entry"
            )
            or snap.get("dxy_alignment_at_entry")
        )

        # -------------------------------------------------------------
        # M15 turn state
        # -------------------------------------------------------------
        m15_direction = _normalise_direction(
            selected_m15.get("direction")
        )

        m15_alignment = _normalise_alignment(
            selected_m15.get("trade_alignment")
        )

        m15_status = str(
            selected_m15.get("status")
            or "UNAVAILABLE"
        ).upper().strip()

        # -------------------------------------------------------------
        # Overall alignment
        # Only directional states count.
        # -------------------------------------------------------------
        directional_alignments = [
            value
            for value in (
                h1_alignment,
                m15_alignment,
            )
            if value in ("ALIGNED", "AGAINST")
        ]

        if not directional_alignments:
            overall_alignment = "NEUTRAL_OR_UNAVAILABLE"

        elif all(
            value == "ALIGNED"
            for value in directional_alignments
        ):
            overall_alignment = "ALIGNED"

        elif all(
            value == "AGAINST"
            for value in directional_alignments
        ):
            overall_alignment = "AGAINST"

        else:
            overall_alignment = "MIXED"

        # -------------------------------------------------------------
        # Explicit H1 room
        # These fields answer:
        # - How far is DXY from H1 support?
        # - How far is DXY from H1 resistance?
        # -------------------------------------------------------------
        h1_nearest_support = _compact_level(
            h1_sr.get("nearest_support")
        )

        h1_nearest_resistance = _compact_level(
            h1_sr.get("nearest_resistance")
        )

        h4_nearest_support = _compact_level(
            h4_sr.get("nearest_support")
        )

        h4_nearest_resistance = _compact_level(
            h4_sr.get("nearest_resistance")
        )

        m15_nearest_support = _compact_level(
            m15_sr.get("nearest_support")
        )

        m15_nearest_resistance = _compact_level(
            m15_sr.get("nearest_resistance")
        )

        context = {
            "schema_version": 1,
            "captured_at_ms": _safe_int(
                snap.get("enqueue_timestamp")
                or dxy_m15_entry.get("captured_at_ms")
                or _now_ms(),
                0,
            ),

            "immutable_entry_snapshot": True,
            "analytics_only": True,

            "trade": {
                "symbol": snap.get("symbol"),
                "side": snap.get("side"),
                "ticket": snap.get("mt5_ticket"),
                "trade_id": snap.get("trade_id"),
            },

            "source": {
                "h1_source": (
                    snap.get("usd_reference_source")
                ),
                "h1_device_id": (
                    snap.get(
                        "usd_reference_device_id"
                    )
                ),
                "m15_source": (
                    dxy_m15_entry.get(
                        "selected_source"
                    )
                ),
                "m15_device_id": (
                    dxy_m15_entry.get(
                        "selected_device_id"
                    )
                ),
                "fallback_used": bool(
                    dxy_m15_entry.get(
                        "fallback_used"
                    )
                ),
                "fallback_reason": (
                    dxy_m15_entry.get(
                        "fallback_reason"
                    )
                ),
            },

            "h1": {
                "available": bool(
                    snap.get(
                        "usd_reference_available"
                    )
                    or snap.get("dxy_available")
                ),

                "direction": h1_direction,
                "direction_raw": h1_direction_raw,

                "trend_tilt": (
                    snap.get(
                        "usd_reference_tilt"
                    )
                    or snap.get(
                        "dxy_h1_20_tilt"
                    )
                ),

                "net_atr": _safe_float(
                    snap.get(
                        "usd_reference_net_atr"
                    )
                    if snap.get(
                        "usd_reference_net_atr"
                    ) is not None
                    else snap.get(
                        "dxy_h1_20_net_atr"
                    )
                ),

                "slope_atr": _safe_float(
                    snap.get(
                        "usd_reference_slope_atr"
                    )
                    if snap.get(
                        "usd_reference_slope_atr"
                    ) is not None
                    else snap.get(
                        "dxy_h1_20_slope_atr"
                    )
                ),

                "r2": _safe_float(
                    snap.get(
                        "usd_reference_r2"
                    )
                    if snap.get(
                        "usd_reference_r2"
                    ) is not None
                    else snap.get(
                        "dxy_h1_20_r2"
                    )
                ),

                "bars_used": _safe_int(
                    snap.get(
                        "usd_reference_bars_used"
                    )
                    or snap.get(
                        "dxy_h1_20_bars_used"
                    ),
                    0,
                ),

                "last_closed_price": _safe_float(
                    snap.get(
                        "usd_reference_last_closed_h1_close"
                    )
                    if snap.get(
                        "usd_reference_last_closed_h1_close"
                    ) is not None
                    else snap.get(
                        "dxy_last_closed_h1_close"
                    )
                ),

                "last_closed_ms": _safe_int(
                    snap.get(
                        "usd_reference_last_closed_h1_close_ms"
                    )
                    or snap.get(
                        "dxy_last_closed_h1_close_ms"
                    ),
                    0,
                ),

                "trade_alignment": h1_alignment,

                "nearest_support": (
                    h1_nearest_support
                ),

                "nearest_resistance": (
                    h1_nearest_resistance
                ),

                "distance_to_support_atr": (
                    _safe_float(
                        (
                            h1_nearest_support
                            or {}
                        ).get("distance_atr")
                    )
                ),

                "distance_to_resistance_atr": (
                    _safe_float(
                        (
                            h1_nearest_resistance
                            or {}
                        ).get("distance_atr")
                    )
                ),

                "near_support": bool(
                    structure_context.get(
                        "near_h1_support"
                    )
                ),

                "near_resistance": bool(
                    structure_context.get(
                        "near_h1_resistance"
                    )
                ),

                "sweep_reclaim_bullish": bool(
                    structure_context.get(
                        "bullish_h1_sweep_reclaim"
                    )
                ),

                "sweep_reject_bearish": bool(
                    structure_context.get(
                        "bearish_h1_sweep_reject"
                    )
                ),
            },

            "m15": {
                "available": bool(
                    selected_m15.get("available")
                ),

                "fresh": bool(
                    selected_m15.get("fresh")
                ),

                "status": m15_status,
                "direction": m15_direction,

                "trade_alignment": m15_alignment,

                "confidence": _safe_float(
                    selected_m15.get(
                        "confidence"
                    )
                ),

                "bull_score": _safe_float(
                    selected_m15.get(
                        "bull_score"
                    )
                ),

                "bear_score": _safe_float(
                    selected_m15.get(
                        "bear_score"
                    )
                ),

                "reasoning": (
                    dxy_m15_entry.get(
                        "dxy_reasoning"
                    )
                ),

                "nearest_support": (
                    m15_nearest_support
                ),

                "nearest_resistance": (
                    m15_nearest_resistance
                ),

                "distance_to_support_atr": (
                    _safe_float(
                        (
                            m15_nearest_support
                            or {}
                        ).get("distance_atr")
                    )
                ),

                "distance_to_resistance_atr": (
                    _safe_float(
                        (
                            m15_nearest_resistance
                            or {}
                        ).get("distance_atr")
                    )
                ),

                "inside_support": bool(
                    structure_context.get(
                        "inside_m15_support"
                    )
                ),

                "inside_resistance": bool(
                    structure_context.get(
                        "inside_m15_resistance"
                    )
                ),
            },

            # H4 direction is intentionally not invented.
            # This stores H4 SR context already calculated by the M15 engine.
            "h4_sr": {
                "nearest_support": (
                    h4_nearest_support
                ),

                "nearest_resistance": (
                    h4_nearest_resistance
                ),

                "distance_to_support_atr": (
                    _safe_float(
                        (
                            h4_nearest_support
                            or {}
                        ).get("distance_atr")
                    )
                ),

                "distance_to_resistance_atr": (
                    _safe_float(
                        (
                            h4_nearest_resistance
                            or {}
                        ).get("distance_atr")
                    )
                ),

                "inside_support": bool(
                    structure_context.get(
                        "inside_h4_support"
                    )
                ),

                "inside_resistance": bool(
                    structure_context.get(
                        "inside_h4_resistance"
                    )
                ),
            },

            "sr_context": {
                "context": (
                    structure_context.get(
                        "context"
                    )
                ),

                "current_price": _safe_float(
                    structure_context.get(
                        "current_price"
                    )
                ),

                "available_upside_atr": (
                    _safe_float(
                        structure_context.get(
                            "available_upside_atr"
                        )
                    )
                ),

                "available_downside_atr": (
                    _safe_float(
                        structure_context.get(
                            "available_downside_atr"
                        )
                    )
                ),

                "bullish_room_ratio": (
                    _safe_float(
                        structure_context.get(
                            "bullish_room_ratio"
                        )
                    )
                ),

                "bearish_room_ratio": (
                    _safe_float(
                        structure_context.get(
                            "bearish_room_ratio"
                        )
                    )
                ),

                "near_support_tfs": _as_list(
                    selected_sr.get(
                        "near_support_tfs"
                    )
                ),

                "near_resistance_tfs": _as_list(
                    selected_sr.get(
                        "near_resistance_tfs"
                    )
                ),

                "sweep_conflict": bool(
                    structure_context.get(
                        "sweep_conflict"
                    )
                ),
            },

            "alignment": {
                "h1": h1_alignment,
                "m15": m15_alignment,
                "overall": overall_alignment,
            },

            "capture_quality": {
                "h1_direction_present": (
                    h1_direction
                    in (
                        "BULLISH",
                        "BEARISH",
                        "NEUTRAL",
                    )
                ),

                "h1_sr_present": bool(
                    h1_nearest_support
                    or h1_nearest_resistance
                ),

                "m15_state_present": bool(
                    selected_m15
                ),

                "m15_sr_present": bool(
                    m15_nearest_support
                    or m15_nearest_resistance
                ),
            },
        }

        return context

    except Exception as exc:
        log.warning(
            "analytics: entry USD context build failed "
            "ticket=%s err=%r",
            snap.get("mt5_ticket")
            if isinstance(snap, dict)
            else None,
            exc,
        )

        return {
            "schema_version": 1,
            "captured_at_ms": _now_ms(),
            "immutable_entry_snapshot": True,
            "analytics_only": True,
            "capture_error": (
                f"{type(exc).__name__}:{exc}"
            ),
        }

def _classify_actual_market_behavior(snap: dict, bars_h1: list) -> dict:
    """Observe the realized zone behavior after entry, independent of P/L."""
    out = {
        "observation_classifier_version": "market_behavior_observation_v2",
        "analytics_only": True,
        "observed_market_behavior": "UNCLASSIFIED",
        "observed_direction": None,
        "reason_codes": ["INSUFFICIENT_POST_ENTRY_EVIDENCE"],
        "continuation_sequence": {
            "momentum_present": False,
            "zone_break_confirmed": False,
            "retest_present": False,
            "retest_held": False,
        },
        "evidence_after_entry": {},
    }
    out["observation_data_resolution"] = "H1_FULL_BARS_AFTER_ENTRY"
    out["entry_overlap_bar_excluded"] = True
    try:
        side = str(snap.get("side") or "").upper().strip()
        entry = _safe_float(snap.get("entry_price"))
        sl = _safe_float(snap.get("sl_price"))
        setup = snap.get("setup_analysis") if isinstance(snap.get("setup_analysis"), dict) else {}
        pred_ev = setup.get("evidence_at_prediction") if isinstance(setup.get("evidence_at_prediction"), dict) else {}
        zl = _safe_float(snap.get("zone_low"), _safe_float(pred_ev.get("zone_low")))
        zh = _safe_float(snap.get("zone_high"), _safe_float(pred_ev.get("zone_high")))
        if side not in ("BUY", "SELL") or entry is None or sl is None or zl is None or zh is None:
            return out
        risk = abs(entry - sl)
        if risk <= 0:
            return out

        entry_ms = _norm_ms(snap.get("broker_open_time_utc_ms") or snap.get("enqueue_timestamp") or 0)
        close_ms = _norm_ms(snap.get("broker_close_time_utc_ms") or snap.get("close_timestamp") or 0)
        rows = []
        for b in bars_h1 or []:
            if not isinstance(b, dict):
                continue
            bo = _norm_ms(b.get("t_open_ms") or b.get("t") or b.get("time") or 0)
            bc = _norm_ms(b.get("t_close_ms") or 0) or (bo + 3_600_000 if bo else 0)
            # Actual behavior must use price action strictly after entry.
            # H1 OHLC cannot tell which part of an overlapping candle occurred
            # before versus after entry, so skip the entry candle entirely.
            if entry_ms and bo and bo < entry_ms < bc:
                continue
            if entry_ms and bc and bc <= entry_ms:
                continue
            if close_ms and bo and bo >= close_ms:
                continue
            o=_safe_float(b.get("o")); h=_safe_float(b.get("h")); l=_safe_float(b.get("l")); c=_safe_float(b.get("c"))
            if None in (o,h,l,c):
                continue
            rows.append({"bo":bo,"bc":bc,"o":o,"h":h,"l":l,"c":c})

        if not rows:
            return out

        max_fav = max_adv = 0.0
        first_away_050 = None
        first_break = None
        break_idx = None
        directional_after_break = 0
        retest_present = False
        retest_held = False

        for i,b in enumerate(rows):
            if side == "BUY":
                fav=max(0.0,b["h"]-entry)/risk; adv=max(0.0,entry-b["l"])/risk
                broke=b["c"] < zl
            else:
                fav=max(0.0,entry-b["l"])/risk; adv=max(0.0,b["h"]-entry)/risk
                broke=b["c"] > zh
            max_fav=max(max_fav,fav); max_adv=max(max_adv,adv)
            if first_away_050 is None and fav >= 0.50:
                first_away_050=b["bo"] or b["bc"] or None
            if first_break is None and broke:
                first_break=b["bo"] or b["bc"] or None; break_idx=i

        if break_idx is not None:
            # After a support break, bearish candles indicate continuation;
            # after a resistance break, bullish candles indicate continuation.
            zone_side = str(pred_ev.get("zone_side") or "").upper()
            for b in rows[break_idx:min(len(rows), break_idx+3)]:
                if zone_side == "SUPPORT" and b["c"] < b["o"]:
                    directional_after_break += 1
                elif zone_side == "RESISTANCE" and b["c"] > b["o"]:
                    directional_after_break += 1
            for b in rows[break_idx+1:]:
                touched = b["h"] >= zl if zone_side == "SUPPORT" else b["l"] <= zh
                if touched:
                    retest_present=True
                    retest_held = b["c"] < zl if zone_side == "SUPPORT" else b["c"] > zh
                    break

        break_first = bool(first_break and (not first_away_050 or first_break < first_away_050))
        momentum_after_break = bool(break_idx is not None and directional_after_break >= 2 and max_adv >= 0.50)

        ev={
            "bars_used":len(rows),
            "max_favorable_r":round(max_fav,3),
            "max_adverse_r":round(max_adv,3),
            "first_reversal_followthrough_0_5r_ms":first_away_050,
            "first_zone_break_ms":first_break,
            "zone_break_before_reversal_followthrough":break_first,
            "directional_bars_after_break":directional_after_break,
        }
        out["evidence_after_entry"]=ev
        out["continuation_sequence"]={
            "momentum_present":momentum_after_break,
            "zone_break_confirmed":bool(first_break),
            "retest_present":retest_present,
            "retest_held":retest_held,
        }

        if break_first and max_adv >= 0.50:
            out.update({
                "observed_market_behavior":"CONTINUATION",
                "observed_direction":"SELL" if side=="BUY" else "BUY",
                "reason_codes":["ZONE_BREAK_AGAINST_REVERSAL", "CONTINUATION_FOLLOWTHROUGH_GE_0_5R"],
            })
        elif max_fav >= 0.50 and not break_first:
            out.update({
                "observed_market_behavior":"REVERSAL",
                "observed_direction":side,
                "reason_codes":["ZONE_REJECTION_FOLLOWTHROUGH_GE_0_5R", "NO_EARLY_ZONE_ACCEPTANCE"],
            })
        elif max_fav < 0.50 and max_adv < 0.50:
            out.update({"observed_market_behavior":"CHOP","observed_direction":None,"reason_codes":["NO_DIRECTION_REACHED_0_5R"]})
        else:
            out.update({"observed_market_behavior":"MIXED","observed_direction":None,"reason_codes":["CONFLICTING_POST_ENTRY_PATH"]})
        return out
    except Exception as exc:
        out["reason_codes"]=[f"CLASSIFIER_ERROR:{type(exc).__name__}"]
        return out


def _merge_setup_actual(snap: dict, bars_h1: list) -> None:
    frozen = snap.get("setup_analysis") if isinstance(snap.get("setup_analysis"), dict) else {}
    predicted = str(frozen.get("predicted_market_behavior") or "UNCLASSIFIED").upper()
    observed = _classify_actual_market_behavior(snap, bars_h1)
    actual = str(observed.get("observed_market_behavior") or "UNCLASSIFIED").upper()
    comparable = predicted in ("REVERSAL","CONTINUATION") and actual in ("REVERSAL","CONTINUATION")
    comparison={
        "comparison_version":"market_behavior_comparison_v2",
        "prediction":predicted,
        "observation":actual,
        "comparable":comparable,
        "match":bool(comparable and predicted==actual),
        "transition":f"{predicted}_TO_{actual}",
    }
    if not frozen:
        frozen={
            "schema_version":2,
            "analytics_only":True,
            "immutable_prediction":True,
            "prediction_classifier_version":"legacy_missing",
            "predicted_market_behavior":"UNCLASSIFIED",
            "prediction_stage":"UNKNOWN",
            "reason_codes":["PREDICTION_NOT_CAPTURED"],
            "continuation_sequence":{},
            "evidence_at_prediction":{},
            "selected_production_strategy":"ZONE_REVERSAL",
        }
    snap["market_behavior"]={
        "schema_version":2,
        "analytics_only":True,
        "prediction":frozen,
        "observation":observed,
        "comparison":comparison,
    }
    # Compatibility: keep the entry object intact under setup_analysis too.
    snap["setup_analysis"]=frozen
    snap["predicted_market_behavior"]=predicted
    snap["observed_market_behavior"]=actual
    snap["market_behavior_match"]=comparison["match"] if comparable else None
    snap["market_behavior_transition"]=comparison["transition"]


# -- ENTRY: build snapshot from a pos/repaired record -------------------------
def _phase1_label(label: str, evidence: dict) -> dict:
    """Small, stable, auditable classification record."""
    return {"classification": label, "evidence": evidence}


def _h1_last10_behavior(bars: list, atr: float | None = None) -> dict:
    """Summarize the last ten completed H1 candles without using future bars.

    This deliberately describes candle pressure only.  S/R location remains a
    separate frozen fact so later research can change the interpretation without
    rewriting the underlying evidence.
    """
    rows = []
    for bar in list(bars or []):
        if not isinstance(bar, dict) or bar.get("complete") is False:
            continue
        o = _safe_float(bar.get("o")); h = _safe_float(bar.get("h"))
        l = _safe_float(bar.get("l")); c = _safe_float(bar.get("c"))
        if None in (o, h, l, c) or h < l:
            continue
        rows.append({"o": o, "h": h, "l": l, "c": c,
                     "open_ms": _norm_ms(bar.get("t_open_ms") or bar.get("t") or 0) or None})
    rows = rows[-10:]
    if not rows:
        return {"schema_version": 1, "analytics_only": True,
                "completed_bars_used": 0, "classification": "INSUFFICIENT_DATA"}
    bullish = sum(1 for r in rows if r["c"] > r["o"])
    bearish = sum(1 for r in rows if r["c"] < r["o"])
    higher = sum(1 for a, b in zip(rows, rows[1:]) if b["c"] > a["c"])
    lower = sum(1 for a, b in zip(rows, rows[1:]) if b["c"] < a["c"])
    upper_rejections = 0; lower_rejections = 0
    for r in rows:
        rng = r["h"] - r["l"]
        if rng <= 0:
            continue
        body_hi = max(r["o"], r["c"]); body_lo = min(r["o"], r["c"])
        if (r["h"] - body_hi) / rng >= 0.40:
            upper_rejections += 1
        if (body_lo - r["l"]) / rng >= 0.40:
            lower_rejections += 1
    net = rows[-1]["c"] - rows[0]["o"]
    net_atr = net / atr if atr and atr > 0 else None
    if bearish >= 6 and lower >= 5:
        classification = "BEARISH_PRESSURE"
    elif bullish >= 6 and higher >= 5:
        classification = "BULLISH_PRESSURE"
    elif upper_rejections >= 3 and bearish >= bullish:
        classification = "UPPER_REJECTION_PRESSURE"
    elif lower_rejections >= 3 and bullish >= bearish:
        classification = "LOWER_REJECTION_PRESSURE"
    else:
        classification = "MIXED"
    return {
        "schema_version": 1, "analytics_only": True, "bars_requested": 10,
        "completed_bars_used": len(rows), "first_open_ms": rows[0]["open_ms"],
        "last_open_ms": rows[-1]["open_ms"], "bullish_bodies": bullish,
        "bearish_bodies": bearish, "doji_bodies": len(rows) - bullish - bearish,
        "higher_closes": higher, "lower_closes": lower,
        "upper_wick_rejections": upper_rejections,
        "lower_wick_rejections": lower_rejections,
        "net_move": round(net, 8),
        "net_move_atr": round(net_atr, 4) if net_atr is not None else None,
        "classification": classification,
    }


def _dxy_supportive_direction(symbol: str, side: str) -> tuple[str | None, str]:
    """Return the DXY direction supportive of the trade, plus relationship."""
    sym = str(symbol or "").upper().replace(".", "")
    side = str(side or "").upper()
    if side not in ("BUY", "SELL"):
        return None, "UNKNOWN"
    if sym.startswith("XAUUSD") or sym.startswith("XAGUSD"):
        return ("BEARISH" if side == "BUY" else "BULLISH"), "INVERSE"
    if len(sym) >= 6:
        base, quote = sym[:3], sym[3:6]
        if base == "USD":
            return ("BULLISH" if side == "BUY" else "BEARISH"), "DIRECT"
        if quote == "USD":
            return ("BEARISH" if side == "BUY" else "BULLISH"), "INVERSE"
    return None, "CONTEXT_ONLY"


def _apply_phase1_entry_analytics(snap: dict) -> None:
    """Freeze additive Phase-1 entry analytics from data already in *snap*.

    No live reads are performed here.  Unknown facts remain None/UNKNOWN and no
    result is consumed by trading, risk, sizing, gates or execution.
    """
    if not isinstance(snap, dict):
        return
    try:
        side = str(snap.get("side") or "").upper()
        entry = _safe_float(snap.get("entry_price"))
        atr = _safe_float(snap.get("atr"))
        zone_low = _safe_float(snap.get("zone_low"))
        zone_high = _safe_float(snap.get("zone_high"))

        zone_distance = None
        if entry is not None and zone_low is not None and zone_high is not None:
            lo, hi = sorted((zone_low, zone_high))
            zone_distance = max(lo - entry, entry - hi, 0.0)
        elif entry is not None:
            zl = _safe_float(snap.get("zone_level"))
            if zl is not None:
                zone_distance = abs(entry - zl)
        zone_distance_atr = (
            zone_distance / atr if zone_distance is not None and atr and atr > 0 else None
        )

        barrier_type = "RESISTANCE" if side == "BUY" else ("SUPPORT" if side == "SELL" else None)
        candidates = []
        for key in (("nearest_resistance", "best_resistance") if side == "BUY" else
                    (("nearest_support", "best_support") if side == "SELL" else ())):
            value = _safe_float(snap.get(key))
            if value is not None and entry is not None:
                if (side == "BUY" and value > entry) or (side == "SELL" and value < entry):
                    candidates.append(value)
        barrier = (min(candidates) if side == "BUY" else max(candidates)) if candidates else None
        room = abs(barrier - entry) if barrier is not None and entry is not None else None
        room_atr = room / atr if room is not None and atr and atr > 0 else None

        # Current SR evidence is nested under the frozen bias snapshot.  Read
        # the opposing-side candidates directly so the flat Phase-1 warning is
        # guaranteed to describe the same immutable zones retained for audit.
        bias = snap.get("shadow_bias_snapshot") if isinstance(snap.get("shadow_bias_snapshot"), dict) else {}
        context_key = "sell_zone_context" if side == "BUY" else "buy_zone_context"
        opposing = bias.get(context_key) if isinstance(bias.get(context_key), dict) else {}
        nested_zones = []
        best_zone = opposing.get("best_zone")
        if isinstance(best_zone, dict):
            nested_zones.append(best_zone)
        for row in opposing.get("top_candidates") or []:
            if isinstance(row, dict):
                nested_zones.append(row)
        nested_candidates = []
        for row in nested_zones:
            level = _safe_float(row.get("level"))
            low = _safe_float(row.get("low")); high = _safe_float(row.get("high"))
            if entry is None or level is None:
                continue
            if side == "BUY" and (high is None or high <= entry):
                continue
            if side == "SELL" and (low is None or low >= entry):
                continue
            distance = _safe_float(row.get("distance"))
            if distance is None:
                distance = max((low or level) - entry, 0.0) if side == "BUY" else max(entry - (high or level), 0.0)
            distance_atr = _safe_float(row.get("distance_atr"))
            if distance_atr is None and atr and atr > 0:
                distance_atr = distance / atr
            nested_candidates.append((distance, distance_atr, level, row))
        if nested_candidates:
            distance, distance_atr, level, barrier_evidence = min(
                nested_candidates,
                key=lambda item: (item[0], item[1] if item[1] is not None else float("inf")),
            )
            barrier = level
            room = distance
            room_atr = distance_atr
        else:
            barrier_evidence = None

        extension = _safe_float(snap.get("h1_20_net_atr"))
        late_extension = abs(extension) >= 3.0 if extension is not None else None
        far_from_zone = zone_distance_atr >= 0.75 if zone_distance_atr is not None else None
        low_room = room_atr < 0.50 if room_atr is not None else None
        weak_zone = None
        zone_score = _safe_float(snap.get("zone_score"))
        if zone_score is not None:
            weak_zone = zone_score < QFLAG_WEAK_ZONE_SCORE

        setup = snap.get("setup_analysis") if isinstance(snap.get("setup_analysis"), dict) else {}
        predicted = setup.get("predicted_direction") or snap.get("predicted_direction")
        if not predicted:
            behavior = setup.get("predicted_market_behavior") or snap.get("predicted_market_behavior")
            if isinstance(behavior, dict):
                predicted = behavior.get("direction") or behavior.get("predicted_direction")
        predicted = str(predicted or "").upper() or None
        prediction_alignment = (
            "ALIGNED" if predicted == side else "CONFLICT" if predicted in ("BUY", "SELL") else "UNCONFIRMED"
        )

        real_available = bool(snap.get("dxy_available"))
        real_alignment = str(snap.get("dxy_alignment_at_entry") or "UNAVAILABLE").upper()
        real_direction = snap.get("dxy_h1_20_direction")
        real_timing = "UNAVAILABLE"
        m15 = snap.get("dxy_m15_entry") if isinstance(snap.get("dxy_m15_entry"), dict) else {}
        real_m15 = (m15.get("sources") or {}).get("REAL_DXY") if isinstance(m15.get("sources"), dict) else {}
        if isinstance(real_m15, dict) and real_m15:
            real_timing = str(real_m15.get("status") or real_m15.get("timing_opinion") or "UNKNOWN").upper()

        real_extreme = (
            m15.get("real_dxy_extreme_impulse")
            if isinstance(m15.get("real_dxy_extreme_impulse"), dict)
            else {}
        )
        real_extreme_active = bool(real_extreme.get("extreme_impulse"))

        supportive_dxy, dxy_relationship = _dxy_supportive_direction(
            snap.get("symbol"), side,
        )
        symbol_h1_behavior = (
            snap.get("symbol_h1_last10_behavior")
            if isinstance(snap.get("symbol_h1_last10_behavior"), dict) else {}
        )
        dxy_h1_behavior = (
            snap.get("dxy_h1_last10_behavior")
            if isinstance(snap.get("dxy_h1_last10_behavior"), dict) else {}
        )
        dxy_selected_direction = str(
            real_m15.get("direction")
            or snap.get("dxy_h1_direction_at_entry")
            or real_direction
            or ""
        ).upper() or None
        dxy_momentum_alignment = (
            "ALIGNED" if supportive_dxy and dxy_selected_direction == supportive_dxy
            else "AGAINST" if supportive_dxy and dxy_selected_direction in ("BULLISH", "BEARISH")
            else "CONTEXT_ONLY" if not supportive_dxy else "UNAVAILABLE"
        )
        real_reasoning = real_m15.get("entry_reasoning") if isinstance(real_m15, dict) else {}
        if not isinstance(real_reasoning, dict):
            real_reasoning = real_m15.get("reasoning") if isinstance(real_m15, dict) else {}
        if not isinstance(real_reasoning, dict):
            real_reasoning = {}
        sr_context = real_reasoning.get("sr_context") if isinstance(real_reasoning.get("sr_context"), dict) else {}
        dxy_at_resistance = bool(
            sr_context.get("near_h1_resistance")
            or sr_context.get("inside_h1_resistance")
            or "RESISTANCE" in str(sr_context.get("context") or "").upper()
        )
        dxy_at_support = bool(
            sr_context.get("near_h1_support")
            or sr_context.get("inside_h1_support")
            or "SUPPORT" in str(sr_context.get("context") or "").upper()
        )
        location_supportive = bool(
            (supportive_dxy == "BEARISH" and dxy_at_resistance)
            or (supportive_dxy == "BULLISH" and dxy_at_support)
        )
        if not supportive_dxy:
            comparison_class = "NOT_DXY_SENSITIVE"
        elif dxy_momentum_alignment == "ALIGNED":
            comparison_class = "FULLY_ALIGNED"
        elif location_supportive:
            comparison_class = "CONDITIONAL_REVERSAL_SETUP"
        elif dxy_momentum_alignment == "AGAINST":
            comparison_class = "DXY_ADVERSE"
        else:
            comparison_class = "INSUFFICIENT_DATA"
        snap["entry_market_comparison"] = {
            "schema_version": 1,
            "captured_at_ms": snap.get("enqueue_timestamp"),
            "immutable_entry_snapshot": True,
            "analytics_only": True,
            "symbol": snap.get("symbol"),
            "trade_side": side,
            "symbol_h1_behavior": symbol_h1_behavior,
            "dxy_h1_behavior": dxy_h1_behavior,
            "dxy_relationship": dxy_relationship,
            "supportive_dxy_direction": supportive_dxy,
            "dxy_direction": dxy_selected_direction,
            "dxy_m15_status": real_timing,
            "dxy_at_h1_resistance": dxy_at_resistance,
            "dxy_at_h1_support": dxy_at_support,
            "momentum_alignment": dxy_momentum_alignment,
            "location_alignment": "SUPPORTIVE" if location_supportive else "NOT_SUPPORTIVE",
            "overall_classification": comparison_class,
            "decision_note": (
                "DXY momentum is aligned with the trade."
                if comparison_class == "FULLY_ALIGNED" else
                "DXY momentum is adverse, but its H1 location supports a reversal thesis."
                if comparison_class == "CONDITIONAL_REVERSAL_SETUP" and dxy_momentum_alignment == "AGAINST" else
                "DXY H1 location supports a reversal thesis; momentum is not confirmed."
                if comparison_class == "CONDITIONAL_REVERSAL_SETUP" else
                "DXY momentum and location are adverse to the trade."
                if comparison_class == "DXY_ADVERSE" else
                "DXY is contextual only for this symbol."
                if comparison_class == "NOT_DXY_SENSITIVE" else
                "Insufficient frozen evidence for a DXY comparison."
            ),
        }

        flags = []
        evidence = []
        def add(label, facts):
            flags.append(label); evidence.append(_phase1_label(label, facts))
        if side == "SELL" and barrier_type == "SUPPORT" and low_room:
            add("SELL_INTO_STRONG_SUPPORT", {"entry_price": entry, "support_price": barrier, "directional_room_atr": room_atr})
        if side == "BUY" and barrier_type == "RESISTANCE" and low_room:
            add("BUY_INTO_STRONG_RESISTANCE", {"entry_price": entry, "resistance_price": barrier, "directional_room_atr": room_atr})
        if late_extension:
            add("LATE_EXTENSION", {"h1_20_net_atr": extension, "threshold_abs_atr": 3.0})
        if far_from_zone:
            add("FAR_FROM_SELECTED_ZONE", {"distance_from_zone_atr": zone_distance_atr, "threshold_atr": 0.75})
        if low_room:
            add("LOW_DIRECTIONAL_ROOM", {"barrier_type": barrier_type, "barrier_price": barrier, "directional_room_atr": room_atr, "threshold_atr": 0.50})
        if weak_zone:
            add("WEAK_ENTRY_ZONE", {"zone_score": zone_score, "threshold": QFLAG_WEAK_ZONE_SCORE})
        if prediction_alignment != "ALIGNED":
            add("PREDICTION_" + prediction_alignment, {"trade_side": side, "predicted_direction": predicted})
        if not real_available:
            add("REAL_DXY_UNAVAILABLE", {"reason": snap.get("dxy_unavailable_reason")})
        elif real_timing in ("WAIT", "PENDING", "IDLE"):
            add("REAL_DXY_WAIT", {"status": real_timing, "direction": real_direction, "alignment": real_alignment})
        elif real_alignment in ("AGAINST", "CONFLICT"):
            add("REAL_DXY_CONFLICT", {"direction": real_direction, "alignment": real_alignment})
        if real_extreme_active:
            add(
                "REAL_DXY_M15_EXTREME_IMPULSE",
                {
                    "source": "REAL_DXY",
                    "direction": real_extreme.get("direction"),
                    "bar_open_ms": real_extreme.get("bar_open_ms"),
                    "bar_close_ms": real_extreme.get("bar_close_ms"),
                    "open": real_extreme.get("open"),
                    "high": real_extreme.get("high"),
                    "low": real_extreme.get("low"),
                    "close": real_extreme.get("close"),
                    "atr14": real_extreme.get("atr14"),
                    "range_atr": real_extreme.get("range_atr"),
                    "body_atr": real_extreme.get("body_atr"),
                    "body_ratio": real_extreme.get("body_ratio"),
                    "range_pct": real_extreme.get("range_pct"),
                    "body_pct": real_extreme.get("body_pct"),
                    "signed_change_pct": real_extreme.get("signed_change_pct"),
                    "thresholds": real_extreme.get("thresholds"),
                    "shadow_entry_action": "WAIT_NEW_ENTRY",
                },
            )
        if not snap.get("rc_found"):
            add("RC_CAPTURE_MISSING", {"rc_source": snap.get("rc_source"), "note": snap.get("rc_capture_note")})
        elif "reconstructed" in str(snap.get("rc_capture_note") or "").lower():
            add("RC_RECONSTRUCTED", {"rc_source": snap.get("rc_source"), "note": snap.get("rc_capture_note")})

        shadow_decision = "BLOCK" if late_extension and far_from_zone and low_room else ("REVIEW" if flags else "ALLOW")
        shadow_reason = "LATE_EXTENSION_FAR_FROM_ZONE_LOW_ROOM" if shadow_decision == "BLOCK" else (flags[0] if flags else "NO_PHASE1_WARNING")
        shadow_entry_action = "WAIT_NEW_ENTRY" if real_extreme_active else "NONE"
        shadow_entry_reason = (
            "REAL_DXY_M15_EXTREME_IMPULSE" if real_extreme_active else None
        )
        summary = {
            "schema_version": 1, "captured_at_ms": snap.get("enqueue_timestamp"),
            "immutable_entry_snapshot": True, "analytics_only": True,
            "prediction_alignment": prediction_alignment,
            "trend_h1_direction": snap.get("h1_20_direction"),
            "trend_h1_vs_trade": snap.get("h1_20_vs_trade"),
            "real_dxy_available": real_available, "real_dxy_direction": real_direction,
            "real_dxy_alignment": real_alignment, "real_dxy_timing": real_timing,
            "real_dxy_m15_extreme_impulse": real_extreme_active,
            "real_dxy_m15_extreme_impulse_evidence": (dict(real_extreme) if real_extreme else None),
            "shadow_entry_action": shadow_entry_action,
            "shadow_entry_reason": shadow_entry_reason,
            "distance_from_zone_price": zone_distance, "distance_from_zone_atr": zone_distance_atr,
            "far_from_zone": far_from_zone, "nearest_directional_barrier_type": barrier_type,
            "nearest_directional_barrier_price": barrier, "directional_room_price": room,
            "directional_room_atr": room_atr, "low_directional_room": low_room,
            "h1_20_net_atr": extension, "h1_20_slope_atr": snap.get("h1_20_slope_atr"),
            "h1_20_r2": snap.get("h1_20_r2"), "late_extension": late_extension,
            "weak_zone": weak_zone, "rc_capture_complete": bool(snap.get("rc_found")),
            "rc_capture_source": snap.get("rc_source"), "entry_flags": list(flags),
            "shadow_decision": shadow_decision, "shadow_reason": shadow_reason,
        }
        if isinstance(barrier_evidence, dict):
            summary["nearest_directional_barrier_evidence"] = dict(barrier_evidence)
        if "rc_shifted_before_entry" in snap:
            summary["rc_shifted_before_entry"] = bool(snap.get("rc_shifted_before_entry"))
        snap["entry_quality_summary"] = summary
        snap["entry_flags"] = list(flags)
        snap["entry_situation_classifications"] = evidence
        snap["shadow_decision"] = shadow_decision
        snap["shadow_reason"] = shadow_reason
        snap["shadow_entry_action"] = shadow_entry_action
        snap["shadow_entry_reason"] = shadow_entry_reason
        snap["real_dxy_m15_extreme_impulse"] = dict(real_extreme) if real_extreme else None
    except Exception as exc:
        log.warning("analytics: Phase-1 entry analytics failed: %s", exc)


def build_entry_snapshot(pos: dict, capture_source: str = "normal") -> dict:
    """Map a pos (clean) or repaired record -> frozen entry snapshot.
    Provenance-aware; reads prop from the prop_check dict so it works for both the
    OK (clean) and ALLOW (repair) verdicts. Never raises."""
    try:
        p = pos or {}
        trade_device_id = str(
            p.get("device_id")
            or ""
        ).strip()
        sym  = str(p.get("symbol") or "").upper()
        side = str(p.get("side") or "").upper()
        ticket = _extract_ticket(p)
        uid = str(
            p.get("uid")
            or p.get("user_id")
            or p.get("owner_uid")
            or ""
        ).strip()
        entry = _safe_float(p.get("entry_price")) or _safe_float(p.get("mt5_fill_price"))
        sl = _safe_float(p.get("sl_price"))
        tp = _safe_float(p.get("tp_price"))

        # zone: prefer nested entry_zone dict (carries score/touches), else flat fields
        z = p.get("entry_zone") if isinstance(p.get("entry_zone"), dict) else {}
        zone_level   = _safe_float(z.get("level") if z else p.get("entry_zone_level"))
        zone_low     = _safe_float(z.get("low")   if z else p.get("entry_zone_low"))
        zone_high    = _safe_float(z.get("high")  if z else p.get("entry_zone_high"))
        zone_score   = _safe_float(z.get("sr_score") or z.get("score")) if z else None
        zone_touches = z.get("touches") if z else None

        # prop: read from the prop_check dict (path-agnostic)
        pc = p.get("prop_check") if isinstance(p.get("prop_check"), dict) else {}
        risk_usd   = _safe_float(pc.get("risk_usd")  or p.get("risk_usd")  or p.get("prop_risk_usd"))
        risk_pct   = _safe_float(pc.get("risk_pct")  or p.get("risk_pct")  or p.get("prop_risk_pct"))
        target_rr  = _safe_float(pc.get("target_rr") or p.get("target_rr"))
        planned_rr = _safe_float(pc.get("planned_rr") or p.get("planned_rr"))

        # provenance: explicit from caller (capture_source), with field-based fallback
        cs = str(capture_source or "").lower()
        if cs in ("broker_repair", "repair"):
            provenance = "broker_repair"
        elif cs in ("normal", "clean"):
            provenance = "clean"
        else:
            provenance = ("broker_repair" if (str(p.get("source") or "") == "broker_repair"
                          or bool(p.get("repair_source"))
                          or str(p.get("trade_id") or "").startswith("BROKER_REPAIR:")) else "clean")
        is_repair = (provenance == "broker_repair")

        ets = int(
            p.get("broker_open_time_ms")
            or p.get("opened_at_ms")
            or p.get("enqueue_timestamp")
            or _now_ms()
        )
        session = _session_for_ts_ms(ets, LIVE_TZ_OFFSET_H)

        dist_pips = None
        if entry is not None and zone_level:
            dist_pips = round(abs(entry - zone_level) / _pip(sym), 1)

        regime = read_regime_at_ack(
            sym,
            p.get("device_id"),
        )

        liq = read_liquidity_at_ack(
            sym,
            side,
            (z or None),
            p.get("device_id"),
            entry,
        )

        sr = read_sr_at_ack(sym)

        # -------------------------------------------------
        # XTL Evidence Bias   frozen at broker-confirmed entry.
        #
        # Shadow analytics only. This result never changes live
        # direction, zones, watches, gate, risk or execution.
        # -------------------------------------------------
        shadow_bias_snapshot = read_shadow_bias_at_ack(
            symbol=sym,
            side=side,
            device_id=p.get("device_id"),
            entry_price=entry,
            entry_zone=(z or None),
            computed_ms=ets,
        )

        drift = _drift_lookup(
            sym,
            session,
            side,
        )
        ftmo   = read_ftmo_state_at_ack(
            uid,
            p.get("profile_id"),
        )
        acct   = read_account_at_ack(p.get("device_id"),
                                     str(p.get("account_type") or "demo"))
        rc     = read_reversal_candle(sym, p.get("device_id"), p)
        liqdet = read_liquidity_detail(liq, side)
        news   = read_news_at_ack(sym, ets)
        news_day = read_news_day_context(sym, ets)

        # -- derivations (no new source) --
        import datetime as _dt
        _d = _dt.datetime.fromtimestamp(ets / 1000.0, _dt.timezone.utc)
        weekday = _d.strftime("%A"); month = _d.month
        quarter = (month - 1) // 3 + 1; year = _d.year
        pipf = _pip(sym)
        stop_distance = round(abs(entry - sl) / pipf, 1) if (entry is not None and sl is not None) else None
        tp_distance   = round(abs(tp - entry) / pipf, 1) if (entry is not None and tp is not None) else None
        atr_pct = None
        _atrv = None
        try:
            _atrv = liq.get("atr")
            if _atrv and entry: atr_pct = round(_atrv / entry * 100.0, 4)
        except Exception:
            pass
        dist_res = dist_sup = None
        try:
            if entry is not None:
                _br = sr.get("nearest_resistance"); _bs = sr.get("nearest_support")
                if _br: dist_res = round(abs(_br - entry) / pipf, 1)
                if _bs: dist_sup = round(abs(entry - _bs) / pipf, 1)
        except Exception:
            pass
        # richer zone fields from the nested entry_zone item
        zone_width = _safe_float(z.get("band_width")) if z else None
        zone_strength = _safe_float(z.get("strength")) if z else None
        zone_merged_tfs = z.get("merged_tfs") if z else None
        zone_reaction = _safe_float((z.get("major_reason") or {}).get("strength")) if isinstance(z, dict) and isinstance(z.get("major_reason"), dict) else None
        prop_obj = p.get("prop_check") if isinstance(p.get("prop_check"), dict) else {}

        snap = {
            "schema_version":   SCHEMA_VERSION,
            # Ownership (multi-user / multi-profile safe)
            "uid": uid,
            "user_id": uid,
            "owner_uid": uid,
            "trade_id":         p.get("trade_id"),
            "mt5_ticket":       str(ticket) if ticket else None,
            "profile_id": str(
                p.get("profile_id")
                or ""
            ).strip().lower(),
            "device_id": trade_device_id or None,
            "symbol":           sym,
            "side":             side,
            "entry_provenance": provenance,
            "capture_source":   str(capture_source or "normal"),
            "setup_analysis": (dict(p.get("setup_analysis")) if isinstance(p.get("setup_analysis"), dict) else None),
            "entry_confirmation": (dict(p.get("entry_confirmation")) if isinstance(p.get("entry_confirmation"), dict) else None),
            "selected_strategy": ((p.get("setup_analysis") or {}).get("selected_production_strategy") if isinstance(p.get("setup_analysis"), dict) else None),
            "predicted_market_behavior": ((p.get("setup_analysis") or {}).get("predicted_market_behavior") if isinstance(p.get("setup_analysis"), dict) else None),
            "enqueue_timestamp": ets,
            "session":          session,

            # -- location --
            "entry_price":      entry,
            "zone_level":       zone_level,
            "zone_low":         zone_low,
            "zone_high":        zone_high,
            "zone_score":       zone_score,
            "zone_touches":     zone_touches,
            "zone_tf":          (z.get("tf")   if z else p.get("entry_zone_tf")),
            "zone_kind":        (z.get("kind") if z else p.get("entry_zone_kind")),
            "dist_to_zone_pips": dist_pips,

            # -- regime (H1 entry TF, H4 confirmation TF, D1 context) --
            "regime_1h":        (regime or {}).get("h1"),
            "regime_4h":        (regime or {}).get("h4"),
            "regime_1d":        (regime or {}).get("d1"),
            # flat, sortable regime scalars (so analysis buckets by raw ADX/ER,
            # not just the TREND/MIXED label   lets data find the real threshold)
            "regime_1h_label":  ((regime or {}).get("h1") or {}).get("label"),
            "regime_1h_adx":    _safe_float(((regime or {}).get("h1") or {}).get("adx")),
            "regime_1h_er":     _safe_float(((regime or {}).get("h1") or {}).get("er")),
            "regime_4h_label":  ((regime or {}).get("h4") or {}).get("label"),
            "regime_4h_adx":    _safe_float(((regime or {}).get("h4") or {}).get("adx")),
            "regime_4h_er":     _safe_float(((regime or {}).get("h4") or {}).get("er")),
            "regime_1d_label":  ((regime or {}).get("d1") or {}).get("label"),
            "regime_1d_adx":    _safe_float(((regime or {}).get("d1") or {}).get("adx")),
            "regime_1d_er":     _safe_float(((regime or {}).get("d1") or {}).get("er")),

            # -- liquidity + entry-frozen ATR (recomputed at entry) --
            "liquidity_model":  liq.get("liquidity_model"),
            "liquidity_score":  liq.get("liquidity_score"),
            "sweep_detected":   liq.get("sweep_detected"),
            "atr":              liq.get("atr"),

            # -- XTL Evidence Bias at entry (shadow analytics only) --
            #
            # Keep the complete payload for forensic analysis and replay.
            # Also expose the most important fields flat for easy pandas/
            # dashboard filtering.
            "shadow_bias_snapshot": shadow_bias_snapshot,

            "shadow_bias": (
                shadow_bias_snapshot.get("shadow_bias")
                if isinstance(shadow_bias_snapshot, dict)
                else "UNKNOWN"
            ),
            "shadow_bias_score": (
                shadow_bias_snapshot.get("shadow_bias_score")
                if isinstance(shadow_bias_snapshot, dict)
                else 0.0
            ),
            "shadow_bias_confidence": (
                shadow_bias_snapshot.get("shadow_bias_confidence")
                if isinstance(shadow_bias_snapshot, dict)
                else "NONE"
            ),
            "shadow_bias_relation": (
                shadow_bias_snapshot.get("shadow_bias_relation")
                if isinstance(shadow_bias_snapshot, dict)
                else "UNKNOWN"
            ),
            "shadow_bias_actionable": (
                bool(
                    shadow_bias_snapshot.get(
                        "shadow_bias_actionable"
                    )
                )
                if isinstance(shadow_bias_snapshot, dict)
                else False
            ),
            "shadow_bias_actionability_reason": (
                shadow_bias_snapshot.get(
                    "shadow_bias_actionability_reason"
                )
                if isinstance(shadow_bias_snapshot, dict)
                else "CAPTURE_NOT_AVAILABLE"
            ),
            "shadow_bias_engine_version": (
                shadow_bias_snapshot.get(
                    "bias_engine_version"
                )
                if isinstance(shadow_bias_snapshot, dict)
                else None
            ),

            # -- support / resistance (cheap read from cached SR bundle) --
            "best_resistance":   sr.get("best_resistance"),
            "best_support":      sr.get("best_support"),
            "nearest_resistance": sr.get("nearest_resistance"),
            "nearest_support":   sr.get("nearest_support"),
            "live_price":        sr.get("sr_price"),
            "distance_to_resistance": dist_res,
            "distance_to_support":    dist_sup,

            # -- timing derivations ( 6) --
            "weekday":   weekday,
            "month":     month,
            "quarter":   quarter,
            "year":      year,

            # -- position geometry ( 9) --
            "stop_distance": stop_distance,
            "tp_distance":   tp_distance,

            # -- richer zone ( 12) --
            "zone_width":     zone_width,
            "zone_strength":  zone_strength,
            "zone_merged_tfs": zone_merged_tfs,
            "zone_reaction":  zone_reaction,

            # -- market-context derivation ( 11) --
            "atr_pct":   atr_pct,

            # -- news context @ entry ( 11, shadow   observed not blocking) --
            "news_block":            news.get("news_block"),
            "news_verdict":          news.get("news_verdict"),
            "news_event":            news.get("news_event"),
            "news_minutes_to_event": news.get("news_minutes_to_event"),
            "news_window":           news.get("news_window"),
            "news_impact":           news.get("news_impact"),
            "upcoming_events":       news.get("upcoming_events"),
            "news_day_has_high_impact": (
                news_day.get("news_day_has_high_impact")
            ),
            "news_day_events": news_day.get("news_day_events"),
            "nearest_news_event": news_day.get("nearest_news_event"),
            "nearest_news_time_ms": news_day.get("nearest_news_time_ms"),
            "nearest_news_distance_minutes": (
                news_day.get("nearest_news_distance_minutes")
            ),
            "nearest_news_relation": news_day.get(
                "nearest_news_relation"
            ),

            # -- FTMO risk state @ entry ( 7/ 8-core/ 10)   canonical source --
            "ftmo":      ftmo,

            # -- account snapshot @ entry ( 8) --
            "account":   acct,

            # -- prop object, UNFLATTENED ( 16) --
            "prop_check": prop_obj,

            # -- reversal candle ( 13) --
            "rc_open":      rc.get("rc_open"),
            "rc_high":      rc.get("rc_high"),
            "rc_low":       rc.get("rc_low"),
            "rc_close":     rc.get("rc_close"),
            "rc_body_pct":  rc.get("rc_body_pct"),
            "rc_size_pips": rc.get("rc_size_pips"),
            "rc_direction": rc.get("rc_direction"),
            "rc_open_ms":   rc.get("rc_open_ms"),
            "rc_found":     rc.get("rc_found"),
            "rc_source":    rc.get("rc_source"),
            "rc_capture_note": rc.get("rc_capture_note"),

            # -- full liquidity breakdown ( 14) --
            "equal_highs":        liqdet.get("equal_highs"),
            "equal_lows":         liqdet.get("equal_lows"),
            "liquidity_pool_count": liqdet.get("liquidity_pool_count"),
            "session_liquidity":  liqdet.get("session_liquidity"),
            "sweep_direction":    liqdet.get("sweep_direction"),
            "bsl_level":          liqdet.get("bsl_level"),
            "ssl_level":          liqdet.get("ssl_level"),

            # -- gate detail ( 17) --
            "watch_key":       p.get("watch_key") or (f"xtl:zone:watch:{sym}:{side}:{p.get('entry_zone_tf') or 'H1'}"),
            "gate_reason":     p.get("entry_gate_reason"),
            "selection_model": (z.get("selection_model") if z else None) or p.get("selection_model"),
            "execution_tf":    p.get("entry_zone_tf") or p.get("execution_tf"),

            # -- completeness marker (Phase-F final rec) --
            "broker_verified": False,
            "broker_truth_upgraded": False,
            "broker_holding_minutes": None,

            "capture_status": {
                "entry_snapshot_complete": True,
                "exit_snapshot_complete": False,
                "broker_verified": False,
                "dxy_h1_entry_captured": False,
                "dxy_h1_entry_data_ok": False,
                "analytics_schema_version": SCHEMA_VERSION,
                "shadow_bias_captured": bool(
                    isinstance(shadow_bias_snapshot, dict)
                    and shadow_bias_snapshot.get(
                        "bias_engine_version"
                    )
                ),
                "shadow_bias_data_ok": bool(
                    isinstance(shadow_bias_snapshot, dict)
                    and shadow_bias_snapshot.get(
                        "shadow_bias_data_ok"
                    )
                ),
            },

            # -- direction --
            "trigger_type":     p.get("trigger_type"),     # None on repair
            "trigger_level":    p.get("trigger_level"),
            "drift_signed":     drift.get("signed"),
            "drift_reliab":     drift.get("reliab"),
            "drift_direction":  drift.get("direction"),
            "against_drift":    drift.get("against"),

            # -- entry style --
            "entry_style":      p.get("trigger_type") or ("broker_repair" if is_repair else None),

            # -- risk / plan --
            "sl_price":         sl,
            "tp_price":         tp,
            "planned_rr":       planned_rr,
            "target_rr":        target_rr,
            "lots":             _safe_float(p.get("qty")),
            "risk_usd":         risk_usd,
            "risk_pct":         risk_pct,
            "prop_verdict":     pc.get("verdict"),
            "prop_firm":        pc.get("firm")  or p.get("prop_firm"),
            "prop_phase":       pc.get("phase") or p.get("prop_phase"),

            # -- provenance detail --
            "mt5_account": str(
                p.get("mt5_account")
                or p.get("account_type")
                or "demo"
            ).lower().strip(),
            "source":           p.get("source"),
        }

        # Optional only: do not manufacture a false value for older payloads.
        if "rc_shifted_before_entry" in rc:
            snap["rc_shifted_before_entry"] = bool(rc.get("rc_shifted_before_entry"))
        
        # compute quality flags from the assembled snapshot (self-documenting risk)
        try:
            snap["setup_quality_flags"] = compute_setup_quality_flags(snap)
            snap["setup_quality_flag_count"] = len(snap["setup_quality_flags"])
        except Exception:
            snap["setup_quality_flags"] = []
            snap["setup_quality_flag_count"] = 0

        # -- USD-strength / bias capture at ENTRY (broker-independent, analytics-only) --
        # Records what the synthetic dollar + pair trend were AT the moment of entry.
        # Non-fatal: never let this break entry capture.
        try:
            from api.usd_strength import macro_bias_for_trade
            snap.update(
                macro_bias_for_trade(
                    from_app_R(),
                    sym,
                    side,
                    device_id=p.get("device_id"),
                )
            )
        except Exception as _me:
            log.warning("analytics: usd_strength capture failed: %s", _me)
        # -- Weekend/session GAP context at ENTRY (broker-independent) --
        # Records the last market-closure gap and whether this trade is a
        # continuation of it or a fade. Lets you segment SL clusters by gap
        # later instead of guessing. Non-fatal.
        try:
            from api.gap_detect import gap_context_for_trade
            snap.update(gap_context_for_trade(from_app_R(), sym, side))
        except Exception as _ge:
            log.warning("analytics: gap capture failed: %s", _ge)
        # -- H1 20-bar overall direction at entry (shadow analytics; no new source) --
        try:
            from api.trend_endpoints import _get_closed_h1_bars
            _dir_bars = _get_closed_h1_bars(sym, p.get("device_id")) or []
            _h1dir = _h1_window_direction(_dir_bars, liq.get("atr"), n=20)
            if _h1dir:
                _h1dir["h1_20_vs_trade"] = _h1_dir_vs_trade(
                    _h1dir.get("h1_20_direction"), side)
                snap.update(_h1dir)
            if _dir_bars:
                snap["symbol_h1_last10_behavior"] = _h1_last10_behavior(
                    _dir_bars,
                    _safe_float(liq.get("atr")),
                )
        except Exception as _he:
            log.warning("analytics: h1_20 direction capture failed: %s", _he)
        
        
        # -- Real DXY or same-device synthetic DXY reference --
        try:
            # Preserve raw real-DXY fields where available.
            dxy_market = capture_dxy_market_snapshot(
                device_id=p.get("device_id"),
            )

            snap.update(
                dxy_market
            )
            snap["dxy_structure_entry"] = (
                capture_dxy_structure_snapshot(
                    device_id=(
                        dxy_market.get("dxy_device_id")
                        or p.get("device_id")
                    ),
                    direction=dxy_market.get(
                        "dxy_h1_20_direction"
                    ),
                )
            )

            snap["dxy_alignment_at_entry"] = (
                compute_dxy_trade_alignment(
                    trade_symbol=sym,
                    trade_side=side,
                    dxy_direction=dxy_market.get(
                        "dxy_h1_20_direction"
                    ),
                )
                if dxy_market.get("dxy_available")
                else "UNAVAILABLE"
            )

            # Unified source selected for cross-firm analysis.
            usd_reference = (
                capture_usd_reference_snapshot(
                    device_id=p.get("device_id"),
                    trade_symbol=sym,
                    trade_side=side,
                )
            )

            snap.update(
                usd_reference
            )

        except Exception as _de:
            log.warning(
                "analytics: USD reference capture failed: %s",
                _de,
            )

            snap.setdefault(
                "dxy_alignment_at_entry",
                "UNAVAILABLE",
            )

            snap.setdefault(
                "usd_reference_available",
                False,
            )

            snap.setdefault(
                "usd_reference_alignment_at_entry",
                "UNAVAILABLE",
            )

        # -- Unified M15 DXY evidence snapshot (REAL + SYNTHETIC) --
        # Frozen at entry and strictly shadow-only. No execution effect.
        try:
            snap["dxy_m15_entry"] = read_dxy_m15_at_entry(
                device_id=p.get("device_id"),
                symbol=sym,
                side=side,
                entry_ms=ets,
                trade_firm=(
                    pc.get("firm")
                    or p.get("prop_firm")
                ),
                trade_profile_id=p.get("profile_id"),
            )
        except Exception as _dxy_m15_exc:
            log.warning(
                "analytics: DXY M15 entry capture failed ticket=%s err=%r",
                ticket,
                _dxy_m15_exc,
            )

            snap["dxy_m15_entry"] = {
                "schema_version": 1,
                "captured_at_ms": ets,
                "sources": {},
                "selected_source": None,
                "selected_device_id": None,
                "selected": None,
                "capture_error": (
                    f"{type(_dxy_m15_exc).__name__}:"
                    f"{_dxy_m15_exc}"
                ),
                "shadow_only": True,
            }

        # -------------------------------------------------------------
        # Final immutable USD context
        #
        # Must run AFTER:
        #   1. DXY/USB reference H1 capture
        #   2. DXY structure capture
        #   3. DXY M15 entry capture
        # -------------------------------------------------------------
        try:
            snap["entry_usd_context"] = (
                _build_entry_usd_context(
                    snap
                )
            )

            capture_status = (
                snap.get("capture_status")
                if isinstance(
                    snap.get("capture_status"),
                    dict,
                )
                else {}
            )

            capture_quality = (
                snap["entry_usd_context"].get(
                    "capture_quality"
                )
                if isinstance(
                    snap.get(
                        "entry_usd_context"
                    ),
                    dict,
                )
                else {}
            )

            capture_status[
                "entry_usd_context_captured"
            ] = True

            capture_status[
                "dxy_h1_direction_captured"
            ] = bool(
                capture_quality.get(
                    "h1_direction_present"
                )
            )

            capture_status[
                "dxy_h1_sr_captured"
            ] = bool(
                capture_quality.get(
                    "h1_sr_present"
                )
            )

            capture_status[
                "dxy_m15_state_captured"
            ] = bool(
                capture_quality.get(
                    "m15_state_present"
                )
            )

            capture_status[
                "dxy_m15_sr_captured"
            ] = bool(
                capture_quality.get(
                    "m15_sr_present"
                )
            )

            snap["capture_status"] = (
                capture_status
            )

        except Exception as _usd_ctx_exc:
            log.warning(
                "analytics: final entry USD context "
                "capture failed ticket=%s err=%r",
                ticket,
                _usd_ctx_exc,
            )

            snap["entry_usd_context"] = {
                "schema_version": 1,
                "captured_at_ms": ets,
                "immutable_entry_snapshot": True,
                "analytics_only": True,
                "capture_error": (
                    f"{type(_usd_ctx_exc).__name__}:"
                    f"{_usd_ctx_exc}"
                ),
            }
        
        # -- Native H1 DXY directional features at ENTRY --
        #
        # Frozen entry-time analytics only:
        # - REAL_DXY plus SYNTHETIC_DXY
        # - no entry blocking
        # - no score or classifier production change
        # - no risk or order-placement change
        try:
            snap["dxy_h1_entry"] = (
                read_dxy_h1_at_entry(
                    device_id=p.get(
                        "device_id"
                    ),
                    symbol=sym,
                    side=side,
                    entry_ms=ets,
                    trade_firm=(
                        pc.get("firm")
                        or p.get(
                            "prop_firm"
                        )
                    ),
                    trade_profile_id=(
                        p.get(
                            "profile_id"
                        )
                    ),
                    dxy_m15_entry=(
                        snap.get(
                            "dxy_m15_entry"
                        )
                        if isinstance(
                            snap.get(
                                "dxy_m15_entry"
                            ),
                            dict,
                        )
                        else {}
                    ),
                )
            )

            # Flat fields for quick jq/pandas grouping.
            _dxy_h1_selected = (
                snap["dxy_h1_entry"].get(
                    "selected"
                )
                if isinstance(
                    snap.get(
                        "dxy_h1_entry"
                    ),
                    dict,
                )
                else {}
            ) or {}

            _dxy_h1_feature = (
                _dxy_h1_selected.get(
                    "feature"
                )
                if isinstance(
                    _dxy_h1_selected,
                    dict,
                )
                else {}
            ) or {}

            snap[
                "dxy_h1_selected_source"
            ] = snap[
                "dxy_h1_entry"
            ].get(
                "selected_source"
            )

            snap[
                "dxy_h1_direction_at_entry"
            ] = _dxy_h1_selected.get(
                "direction"
            )

            snap[
                "dxy_h1_confidence_at_entry"
            ] = _dxy_h1_selected.get(
                "confidence"
            )

            snap[
                "dxy_h1_trade_alignment_at_entry"
            ] = _dxy_h1_selected.get(
                "trade_alignment"
            )

            snap[
                "dxy_h1_evidence_direction_at_entry"
            ] = _dxy_h1_selected.get(
                "evidence_direction"
            )

            snap[
                "dxy_h1_bull_score_at_entry"
            ] = _dxy_h1_feature.get(
                "bull_evidence_score"
            )

            snap[
                "dxy_h1_bear_score_at_entry"
            ] = _dxy_h1_feature.get(
                "bear_evidence_score"
            )

            snap[
                "dxy_h1_bullish_break_at_entry"
            ] = bool(
                _dxy_h1_feature.get(
                    "bullish_structure_break"
                )
            )

            snap[
                "dxy_h1_bearish_break_at_entry"
            ] = bool(
                _dxy_h1_feature.get(
                    "bearish_structure_break"
                )
            )

            snap[
                "dxy_h1_real_synthetic_agreement"
            ] = snap[
                "dxy_h1_entry"
            ].get(
                "real_synthetic_agreement"
            )

            snap[
                "dxy_m15_h1_alignment_at_entry"
            ] = snap[
                "dxy_h1_entry"
            ].get(
                "m15_h1_alignment"
            )

            snap[
                "dxy_h1_shadow_only"
            ] = True

        except Exception as _dxy_h1_exc:
            log.warning(
                "analytics: DXY H1 entry capture "
                "failed ticket=%s err=%r",
                ticket,
                _dxy_h1_exc,
            )

            snap["dxy_h1_entry"] = {
                "schema_version": 1,
                "timeframe": "H1",
                "captured_at_ms": ets,
                "sources": {},
                "selected_source": None,
                "selected": None,
                "capture_error": (
                    f"{type(_dxy_h1_exc).__name__}:"
                    f"{_dxy_h1_exc}"
                ),
                "shadow_only": True,
                "analytics_only": True,
                "execution_wired": False,
                "entry_gate_wired": False,
                "risk_wired": False,
            }

            snap[
                "dxy_h1_selected_source"
            ] = None

            snap[
                "dxy_h1_direction_at_entry"
            ] = None

            snap[
                "dxy_h1_trade_alignment_at_entry"
            ] = "UNAVAILABLE"

            snap[
                "dxy_h1_real_synthetic_agreement"
            ] = "UNAVAILABLE"

            snap[
                "dxy_m15_h1_alignment_at_entry"
            ] = "UNAVAILABLE"

            snap[
                "dxy_h1_shadow_only"
            ] = True

        # -------------------------------------------------------------
        # Final H1 DXY capture-status update
        # -------------------------------------------------------------
        try:
            _capture_status = (
                snap.get("capture_status")
                if isinstance(
                    snap.get("capture_status"),
                    dict,
                )
                else {}
            )

            _h1_entry = (
                snap.get("dxy_h1_entry")
                if isinstance(
                    snap.get("dxy_h1_entry"),
                    dict,
                )
                else {}
            )

            _h1_selected = (
                _h1_entry.get("selected")
                if isinstance(_h1_entry, dict)
                else {}
            ) or {}

            _capture_status[
                "dxy_h1_entry_captured"
            ] = bool(
                isinstance(_h1_entry, dict)
                and _h1_entry
                and not _h1_entry.get("capture_error")
            )

            _capture_status[
                "dxy_h1_entry_data_ok"
            ] = bool(
                _h1_selected.get("available")
                and _h1_selected.get("fresh")
                and isinstance(
                    _h1_selected.get("feature"),
                    dict,
                )
            )

            snap["capture_status"] = _capture_status

        except Exception as _h1_status_exc:
            log.warning(
                "analytics: DXY H1 capture-status update "
                "failed ticket=%s err=%r",
                ticket,
                _h1_status_exc,
            )

        _apply_phase1_entry_analytics(snap)
        return snap
        
    except Exception as e:
        log.error("analytics: build_entry_snapshot failed: %s", e)
        return {
            "schema_version": SCHEMA_VERSION,
            "trade_id": (pos or {}).get("trade_id"),
            "mt5_ticket": (str(_extract_ticket(pos or {}) or "") or None),
            "symbol": (pos or {}).get("symbol"),
            "side": (pos or {}).get("side"),
            "entry_provenance": "unknown",
            "capture_source": str(capture_source or "normal"),
            "enqueue_timestamp": _now_ms(),
            "_build_error": str(e),
        }


def write_entry_snapshot(snap: dict) -> bool:
    """Persist the frozen entry snapshot.

    Repairs missing ownership from UID-scoped strategy records before writing.
    Analytics failure never blocks trading.
    """
    try:
        if not isinstance(snap, dict):
            log.error(
                "analytics: invalid entry snapshot type=%s",
                type(snap).__name__,
            )
            return False

        ticket = str(
            snap.get("mt5_ticket")
            or ""
        ).strip()

        if not ticket:
            log.warning(
                "analytics: snapshot missing mt5_ticket; skipped"
            )
            return False

        R = from_app_R()

        uid = str(
            snap.get("uid")
            or snap.get("user_id")
            or snap.get("owner_uid")
            or ""
        ).strip()

        # ---------------------------------------------------------
        # P0: prevent creation of another ownerless snapshot.
        # Try authoritative Redis ownership recovery at write time.
        # ---------------------------------------------------------
        if not uid:
            recovered_uid, recovery_source = _recover_analytics_uid(
                R,
                ticket,
                snap,
            )

            if recovered_uid:
                uid = recovered_uid

                snap["ownership_recovered"] = True
                snap["ownership_recovery_source"] = (
                    f"ENTRY_WRITE:{recovery_source}"
                )
                snap["ownership_recovered_at_ms"] = _now_ms()

                log.warning(
                    "analytics: entry ownership recovered "
                    "ticket=%s trade_id=%s uid=%s source=%s",
                    ticket,
                    snap.get("trade_id"),
                    uid,
                    recovery_source,
                )

        if uid:
            snap["uid"] = uid
            snap["user_id"] = uid
            snap["owner_uid"] = uid
        else:
            # Do not silently create corrupt analytics.
            log.error(
                "analytics: REFUSE_OWNERLESS_ENTRY "
                "ticket=%s trade_id=%s profile=%s symbol=%s",
                ticket,
                snap.get("trade_id"),
                snap.get("profile_id"),
                snap.get("symbol"),
            )
            return False

        profile_id = str(
            snap.get("profile_id")
            or snap.get("prop_profile_id")
            or ""
        ).strip().lower()

        if not profile_id:
            log.error(
                "analytics: REFUSE_PROFILELESS_ENTRY "
                "ticket=%s trade_id=%s uid=%s symbol=%s",
                ticket,
                snap.get("trade_id"),
                uid,
                snap.get("symbol"),
            )
            return False

        snap["profile_id"] = profile_id
        snap.setdefault("schema_version", SCHEMA_VERSION)
        snap.setdefault("enqueue_timestamp", _now_ms())
        snap["_status"] = "open"

        # Initialize per-ticket journey tracking at broker-confirmed entry.
        # This is shadow analytics only and never changes SL/TP/orders.
        _ensure_trade_milestone_state(snap)

        R.set(
            SNAP_PREFIX + ticket,
            json.dumps(
                snap,
                default=str,
                separators=(",", ":"),
            ),
            ex=SNAP_TTL_SEC,
        )

        return True

    except Exception as exc:
        log.error(
            "analytics: write_entry_snapshot failed: %s",
            exc,
        )
        return False

def capture_entry(pos: dict, capture_source: str = "normal") -> bool:
    """One-call entry capture for the hooks: build + write, idempotent.
    `capture_source` is passed explicitly by the caller ("normal" | "broker_repair")
    rather than inferred. Safe to call every cycle - writes once per ticket.
    Never blocks trading. """
    try:
        ticket = _extract_ticket(pos or {})
        if not ticket:
            return False
        R = from_app_R()
        if R.exists(SNAP_PREFIX + str(ticket)):
            return False
        return write_entry_snapshot(build_entry_snapshot(pos, capture_source=capture_source))
    except Exception as e:
        log.warning("analytics: capture_entry skipped: %s", e)
        return False


# -- EXIT (Option B: classify from H1 bars) -----------------------------------
def approximate_exit(snap: dict, bars_h1: list) -> dict:
    out = {
        "exit_source": "h1_bar_approx",
        "exit_confidence": "medium",
        "exit_price": None,
        "exit_reason": "manual",
        "realized_r": None,
        "close_timestamp": _now_ms(),
    }
    try:
        side  = (snap.get("side") or "").upper()
        entry = _safe_float(snap.get("entry_price"))
        sl    = _safe_float(snap.get("sl_price"))
        tp    = _safe_float(snap.get("tp_price"))
        rr    = _safe_float(snap.get("target_rr") or snap.get("planned_rr"), 0.0)
        if entry is None or sl is None or not bars_h1:
            return out

        entry_ts = int(snap.get("enqueue_timestamp") or 0)
        hit = None; hit_ts = None
        for b in bars_h1:
            t = int(b.get("t_close_ms") or b.get("t_open_ms") or b.get("t") or 0)
            if entry_ts and t < entry_ts:
                continue
            hi = _safe_float(b.get("h")); lo = _safe_float(b.get("l"))
            if hi is None or lo is None:
                continue
            tp_hit = tp is not None and (hi >= tp if side == "BUY" else lo <= tp)
            sl_hit = (lo <= sl if side == "BUY" else hi >= sl)
            if tp_hit and sl_hit:       # same bar   can't order   conservative SL
                hit, hit_ts = "sl", t; break
            if sl_hit:
                hit, hit_ts = "sl", t; break
            if tp_hit:
                hit, hit_ts = "tp", t; break

        if hit == "tp":
            out.update(exit_reason="tp", exit_price=tp,
                       realized_r=round(rr, 2) if rr else None,
                       close_timestamp=hit_ts or out["close_timestamp"])
        elif hit == "sl":
            out.update(exit_reason="sl", exit_price=sl, realized_r=-1.0,
        close_timestamp=hit_ts or out["close_timestamp"])
        # neither -> manual/unknown, realized_r stays None (honest; backfill later)

        # Bound excursion to the trade's own lifetime. Bars are chronological, so
        # slice out anything at/after the exit before measuring MFE/MAE. Without
        # this, _excursion_r reads bars AFTER the trade closed and reports
        # favorable/adverse moves the trade never actually experienced  
        # inflating mfe_r AND mae_r (this is why SL trades showed mae_r < -1).
        _end_ts = _norm_ms(hit_ts or int(out.get("close_timestamp") or 0))
        if _end_ts:
            _win_bars = [
                b for b in bars_h1
                if _norm_ms(b.get("t_close_ms") or b.get("t_open_ms") or b.get("t") or 0) <= _end_ts
            ]
        else:
            _win_bars = bars_h1  # no exit timestamp resolvable -> fall back (rare)
        out.update(_excursion_r(snap, _win_bars, entry, sl))
        return out
    except Exception as e:
        log.error("analytics: approximate_exit failed: %s", e)
        return out



def _broker_offset_minutes_for_trade(
    snap: dict,
    deal: dict,
    R,
) -> tuple[int, str]:
    """
    Resolve the broker UTC offset used by MT5 wall-clock timestamps.

    Returns:
        (offset_minutes, source)

    Example:
        FTMO UTC+03:00 -> (180, "redis_account_snapshot")
    """
    candidates = []

    # Direct analytics/deal fields.
    for obj_name, obj in (
        ("snap", snap),
        ("deal", deal),
    ):
        if not isinstance(obj, dict):
            continue

        for key in (
            "broker_tz_offset_minutes",
            "broker_timezone_offset_minutes",
            "broker_offset_minutes",
            "tz_offset_minutes",
        ):
            if obj.get(key) not in (None, ""):
                candidates.append(
                    (
                        obj.get(key),
                        f"{obj_name}.{key}",
                    )
                )

    # Nested account snapshots captured by analytics.
    for obj_name in (
        "account_before",
        "account_after",
        "ftmo_before",
        "ftmo_after",
    ):
        obj = snap.get(obj_name)

        if not isinstance(obj, dict):
            continue

        for key in (
            "broker_tz_offset_minutes",
            "broker_timezone_offset_minutes",
            "broker_offset_minutes",
            "tz_offset_minutes",
        ):
            if obj.get(key) not in (None, ""):
                candidates.append(
                    (
                        obj.get(key),
                        f"{obj_name}.{key}",
                    )
                )

    # Current Redis MT5 account snapshot for the deal's device.
    try:
        device_id = str(
            deal.get("device_id")
            or snap.get("device_id")
            or ""
        ).strip()

        account_type = str(
            deal.get("mt5_account")
            or snap.get("account_type")
            or "demo"
        ).strip().lower()

        if device_id:
            account_key = (
                f"xtl:mt5:account:{device_id}:{account_type}"
            )

            raw = R.get(account_key)

            if isinstance(raw, (bytes, bytearray)):
                raw = raw.decode("utf-8", "ignore")

            account = (
                json.loads(raw)
                if isinstance(raw, str) and raw
                else {}
            )

            if isinstance(account, dict):
                for key in (
                    "broker_tz_offset_minutes",
                    "broker_timezone_offset_minutes",
                    "broker_offset_minutes",
                    "tz_offset_minutes",
                ):
                    if account.get(key) not in (None, ""):
                        candidates.append(
                            (
                                account.get(key),
                                f"redis_account_snapshot.{key}",
                            )
                        )
    except Exception as exc:
        log.warning(
            "analytics: broker offset account lookup failed "
            "ticket=%s err=%r",
            snap.get("mt5_ticket"),
            exc,
        )

    for value, source in candidates:
        try:
            offset = int(float(value))

            # Reject obviously corrupt offsets.
            if -14 * 60 <= offset <= 14 * 60:
                return offset, source
        except Exception:
            continue

    return 0, "missing"


def _normalize_broker_wall_ms(
    raw_ms: int,
    offset_minutes: int,
) -> int:
    """
    Convert MT5 broker-wall epoch milliseconds into UTC epoch
    milliseconds.

    FTMO example:
        raw broker time encoded as UTC = 06:59 UTC
        broker offset                = +180 min
        normalized actual UTC        = 03:59 UTC
    """
    raw_ms = _safe_int(raw_ms)

    if raw_ms <= 0:
        return 0

    offset_minutes = _safe_int(offset_minutes)

    if offset_minutes == 0:
        return raw_ms

    return raw_ms - (offset_minutes * 60_000)

def _exit_from_broker_deal(ticket: str, snap: dict, R) -> dict | None:
    """Resolve exit from the real MT5 deal (broker truth) instead of H1 replay.
    Returns None if no deal exists -> caller falls back to approximate_exit."""
    try:
        raw = R.get(f"xtl:mt5:deal:{ticket}")
        if not raw:
            return None
        deal = json.loads(raw)
        close_price = _safe_float(
            deal.get("close_price")
            or deal.get("price")
        )

        if close_price is None or close_price <= 0:
            log.warning(
                "analytics: broker deal missing valid close price ticket=%s",
                ticket,
            )
            return None

        raw_close_ms = _safe_int(
            deal.get("close_time_ms")
            or deal.get("close_timestamp")
            or 0
        )

        raw_open_ms = _safe_int(
            deal.get("open_time_ms")
            or 0
        )

        if raw_close_ms <= 0:
            log.warning(
                "analytics: broker deal missing valid close time "
                "ticket=%s",
                ticket,
            )
            return None

        broker_offset_min, broker_offset_source = (
            _broker_offset_minutes_for_trade(
                snap,
                deal,
                R,
            )
        )

        open_ms = _normalize_broker_wall_ms(
            raw_open_ms,
            broker_offset_min,
        )

        close_ms = _normalize_broker_wall_ms(
            raw_close_ms,
            broker_offset_min,
        )
        log.warning(
            "analytics: BROKER_TIME_NORMALIZED "
            "ticket=%s offset_min=%s source=%s "
            "raw_open=%s utc_open=%s "
            "raw_close=%s utc_close=%s",
            ticket,
            broker_offset_min,
            broker_offset_source,
            raw_open_ms,
            open_ms,
            raw_close_ms,
            close_ms,
        )


        if close_ms <= 0:
            log.warning(
                "analytics: broker deal missing valid close time ticket=%s",
                ticket,
            )
            return None
        net_profit = deal.get("net_profit")
        net_profit = (
            float(net_profit)
            if net_profit is not None
            else None
        )

        entry = float(snap.get("entry_price") or 0) or None
        sl    = float(snap.get("sl_price") or 0) or None
        tp    = float(snap.get("tp_price") or 0) or None
        side  = (snap.get("side") or "").upper()

        # realized_r from the REAL close price (never hardcoded)
        realized_r = None
        if entry and sl:
            risk = abs(entry - sl)
            if risk > 0:
                realized_r = ((close_price - entry) if side == "BUY"
                              else (entry - close_price)) / risk

        

   

        # -- capture manual TP/SL modifications (original snapshot vs broker-final) --
        _prev = deal.get("prev_position") or {}
        _final_tp = _prev.get("tp")
        _final_sl = _prev.get("sl")
        _final_tp = float(_final_tp) if _final_tp not in (None, 0) else None
        _final_sl = float(_final_sl) if _final_sl not in (None, 0) else None

        # Broker-native exit reason is authoritative.
      
        # Price proximity is only a fallback for old/incomplete
        # deal payloads. Never relabel an explicit broker SL as
        # MANUAL merely because the final fill differs from the
        # frozen SL price.
        # ---------------------------------------------------------
        broker_reason_raw = str(
            deal.get("broker_reason")
            or deal.get("exit_reason")
            or deal.get("reason")
            or ""
        ).upper().strip()

        broker_reason_map = {
            "TP": "tp",
            "TAKE_PROFIT": "tp",
            "TAKEPROFIT": "tp",

            "SL": "sl",
            "STOP_LOSS": "sl",
            "STOPLOSS": "sl",

            "SO": "stopout",
            "STOP_OUT": "stopout",
            "STOPOUT": "stopout",

            "CLIENT": "manual",
            "EXPERT": "manual",
            "MOBILE": "manual",
            "WEB": "manual",
            "MANUAL": "manual",
        }

        exit_reason = broker_reason_map.get(
            broker_reason_raw
        )

        exit_reason_source = (
            "BROKER_REASON"
            if exit_reason
            else None
        )

        # Fallback only when broker reason is missing or unknown.
        if not exit_reason:
            tol = (
                abs(entry - sl) * 0.10
                if entry and sl
                else 0.0
            )

            near_tp = bool(
                tp is not None
                and abs(close_price - tp) <= tol
            )

            near_sl = bool(
                sl is not None
                and abs(close_price - sl) <= tol
            )

            if near_tp:
                exit_reason = "tp"
                exit_reason_source = "PRICE_NEAR_TP"

            elif near_sl:
                exit_reason = "sl"
                exit_reason_source = "PRICE_NEAR_SL"

            else:
                exit_reason = "manual"
                exit_reason_source = (
                    "BROKER_REASON_UNAVAILABLE"
                )
        tp_modified = bool(_final_tp and tp and abs(_final_tp - tp) > 1e-9)
        sl_modified = bool(_final_sl and sl and abs(_final_sl - sl) > 1e-9)
        # R-impact of a TP move: what the original TP would have been in R vs actual
        tp_original_r = None
        if entry and sl and tp:
            _risk = abs(entry - sl)
            if _risk > 0:
                tp_original_r = ((tp - entry) if side == "BUY" else (entry - tp)) / _risk

        return {
            "exit_source": "broker_deal",
            "exit_confidence": "high",
            "exit_price": round(close_price, 5),
            "exit_reason": exit_reason,
            "exit_reason_source": exit_reason_source,
            "broker_reason": (
                broker_reason_raw or None
            ),
            "broker_close_reason_code": (
                deal.get("close_reason_code")
                or deal.get("exit_deal_reason")
            ),
            "broker_exit_comment": (
                deal.get("exit_comment")
                or deal.get("comment")
            ),
            "realized_r": round(realized_r, 3) if realized_r is not None else None,
            "net_profit": net_profit,
            
            "tp_modified": tp_modified,
            "sl_modified": sl_modified,
            "tp_original": round(tp, 5) if tp else None,
            "tp_final": round(_final_tp, 5) if _final_tp else None,
            "sl_original": round(sl, 5) if sl else None,
            "sl_final": round(_final_sl, 5) if _final_sl else None,
            "tp_original_r": round(tp_original_r, 3) if tp_original_r is not None else None,
            # Existing broker-domain timestamps retained for compatibility
            # with OHLC bars, excursion analysis, and historical rows.
            "broker_open_time_ms": raw_open_ms,
            "broker_close_time_ms": raw_close_ms,
            "close_timestamp": raw_close_ms,

            # Explicit normalized UTC timestamps for news/session analysis.
            "broker_open_time_utc_ms": open_ms,
            "broker_close_time_utc_ms": close_ms,

            # Raw aliases retained for forensic clarity.
            "broker_open_time_raw_ms": raw_open_ms,
            "broker_close_time_raw_ms": raw_close_ms,

            "broker_tz_offset_minutes": broker_offset_min,
            "broker_timestamp_normalized": bool(
                broker_offset_min != 0
            ),
            "broker_timestamp_normalization_source": (
                broker_offset_source
            ),
        }
    except Exception as e:
        log.error("analytics: _exit_from_broker_deal failed for %s: %s",
                  snap.get("mt5_ticket"), e)
        return None

def _norm_ms(v) -> int:
    """Bars store 't' in seconds (10-digit); trade timestamps are ms (13-digit).
    Normalize any bar/trade time to milliseconds before comparing."""
    try:
        v = int(v or 0)
    except (TypeError, ValueError):
        return 0
    return v * 1000 if 0 < v < 10_000_000_000 else v

def _resolve_device(snap) -> str:
    """device_id can be dropped from a finalized snapshot; bar_device_id (inside
    the shadow_bias_snapshot) is the stable fallback. Resolve from the chain."""
    if not isinstance(snap, dict):
        return None
    return (snap.get("device_id")
            or snap.get("bar_device_id")
            or (snap.get("shadow_bias_snapshot") or {}).get("bar_device_id")
            or snap.get("usd_strength_device_id")
            or None)




def _apply_realized_r_net_and_outcome(record: dict) -> dict:
    """Apply deterministic broker-truth R and outcome fields in place.

    - realized_r_net uses actual net P/L divided by the frozen entry risk_usd.
    - outcome prefers realized_r, then realized_r_net, then net_profit/exit_reason.
    - Never guesses realized_r_net when risk_usd is absent or non-positive.
    """
    if not isinstance(record, dict):
        return record

    net_profit = _safe_float(record.get("net_profit"))
    risk_usd = _safe_float(record.get("risk_usd"))

    if net_profit is not None and risk_usd is not None and risk_usd > 0:
        record["realized_r_net"] = round(net_profit / risk_usd, 4)
        record["realized_r_net_source"] = "NET_PROFIT_DIV_FROZEN_RISK_USD"
    else:
        record["realized_r_net"] = None
        record["realized_r_net_source"] = (
            "RISK_USD_MISSING_OR_NONPOSITIVE"
            if risk_usd is None or risk_usd <= 0
            else "NET_PROFIT_MISSING"
        )

    realized_r = _safe_float(record.get("realized_r"))
    realized_r_net = _safe_float(record.get("realized_r_net"))
    exit_reason = str(record.get("exit_reason") or "").lower().strip()

    outcome_basis = None
    outcome_value = None

    if realized_r is not None:
        outcome_basis = "realized_r"
        outcome_value = realized_r
    elif realized_r_net is not None:
        outcome_basis = "realized_r_net"
        outcome_value = realized_r_net
    elif net_profit is not None:
        outcome_basis = "net_profit"
        outcome_value = net_profit

    if outcome_value is not None:
        if outcome_value > 0.05:
            outcome = "WIN"
        elif outcome_value < -0.05:
            outcome = "LOSS"
        else:
            outcome = "BREAK_EVEN"
    elif exit_reason == "sl":
        outcome = "LOSS"
        outcome_basis = "exit_reason"
    elif exit_reason == "tp":
        outcome = "WIN"
        outcome_basis = "exit_reason"
    else:
        outcome = "UNKNOWN"
        outcome_basis = "insufficient_data"

    record["outcome"] = outcome
    record["outcome_source"] = outcome_basis
    record["exit_type"] = {
        "tp": "TP",
        "sl": "SL",
        "stopout": "STOP_OUT",
        "manual": "MANUAL",
    }.get(exit_reason, "OTHER")
    return record

def _excursion_r_m1(
    snap: dict,
    bars_m1: list,
    entry: float,
    sl: float,
) -> dict:
    """
    Compute MFE/MAE from completed broker M1 bars overlapping the actual
    trade lifetime.

    Entry and exit M1 candles are included, but confidence reflects that
    their OHLC may contain movement immediately before entry or after exit.
    """
    try:
        side = str(
            snap.get("side") or ""
        ).upper().strip()

        risk = abs(
            float(entry) - float(sl)
        )

        if (
            side not in ("BUY", "SELL")
            or risk <= 0
        ):
            return {}

        offset_min = int(
            _safe_int(
                snap.get(
                    "broker_tz_offset_minutes"
                ),
                0,
            )
            or 0
        )

        offset_ms = offset_min * 60_000

        entry_utc_ms = _norm_ms(
            snap.get(
                "broker_open_time_utc_ms"
            )
            or snap.get(
                "enqueue_timestamp"
            )
            or snap.get(
                "opened_at_ms"
            )
            or 0
        )

        close_utc_ms = _norm_ms(
            snap.get(
                "broker_close_time_utc_ms"
            )
            or 0
        )

        if close_utc_ms <= 0:
            raw_close_ms = _norm_ms(
                snap.get(
                    "broker_close_time_ms"
                )
                or snap.get(
                    "close_timestamp"
                )
                or 0
            )

            if raw_close_ms > 0:
                close_utc_ms = (
                    raw_close_ms - offset_ms
                )

        if (
            entry_utc_ms <= 0
            or close_utc_ms <= entry_utc_ms
        ):
            return {}

        first_expected_open_ms = (
            entry_utc_ms // 60_000
        ) * 60_000

        last_expected_open_ms = (
            close_utc_ms // 60_000
        ) * 60_000

        expected_bars = int(
            (
                last_expected_open_ms
                - first_expected_open_ms
            )
            // 60_000
        ) + 1

        selected = []
        seen_open_ms = set()

        for bar in bars_m1 or []:
            if not isinstance(bar, dict):
                continue

            wall_open_ms = _norm_ms(
                bar.get("t_open_ms")
                or bar.get("t")
                or bar.get("time")
                or 0
            )

            if wall_open_ms <= 0:
                continue

            bar_open_utc_ms = (
                wall_open_ms - offset_ms
            )

            bar_close_utc_ms = (
                bar_open_utc_ms + 60_000
            )

            # Include every M1 candle that overlaps the true trade lifetime.
            if bar_close_utc_ms <= entry_utc_ms:
                continue

            if bar_open_utc_ms > close_utc_ms:
                continue

            hi = _safe_float(
                bar.get("h")
            )
            lo = _safe_float(
                bar.get("l")
            )

            if hi is None or lo is None:
                continue

            if bar_open_utc_ms in seen_open_ms:
                continue

            selected.append({
                "open_utc_ms": (
                    bar_open_utc_ms
                ),
                "close_utc_ms": (
                    bar_close_utc_ms
                ),
                "high": hi,
                "low": lo,
            })

            seen_open_ms.add(
                bar_open_utc_ms
            )

        selected.sort(
            key=lambda row: row[
                "open_utc_ms"
            ]
        )

        observed_bars = len(selected)

        coverage_pct = (
            round(
                min(
                    100.0,
                    observed_bars
                    / expected_bars
                    * 100.0,
                ),
                2,
            )
            if expected_bars > 0
            else 0.0
        )

        gap_count = 0

        for index in range(
            1,
            len(selected),
        ):
            gap_minutes = int(
                (
                    selected[index][
                        "open_utc_ms"
                    ]
                    - selected[index - 1][
                        "open_utc_ms"
                    ]
                )
                // 60_000
            )

            if gap_minutes > 1:
                gap_count += (
                    gap_minutes - 1
                )

        best = None
        worst = None
        best_price = None
        worst_price = None
        best_ts = None
        worst_ts = None

        for row in selected:
            hi = row["high"]
            lo = row["low"]

            if side == "BUY":
                favorable = (
                    hi - entry
                ) / risk

                adverse = (
                    lo - entry
                ) / risk

                favorable_price = hi
                adverse_price = lo

            else:
                favorable = (
                    entry - lo
                ) / risk

                adverse = (
                    entry - hi
                ) / risk

                favorable_price = lo
                adverse_price = hi

            if (
                best is None
                or favorable > best
            ):
                best = favorable
                best_price = (
                    favorable_price
                )
                best_ts = row[
                    "open_utc_ms"
                ]

            if (
                worst is None
                or adverse < worst
            ):
                worst = adverse
                worst_price = adverse_price
                worst_ts = row[
                    "open_utc_ms"
                ]

        # Entry is an exact, observed 0R point in every trade lifetime.
        # Including it prevents positive MAE or negative MFE when price moves
        # immediately in only one direction after entry.
        if best is not None and best < 0.0:
            best = 0.0
            best_price = float(entry)
            best_ts = entry_utc_ms

        if worst is not None and worst > 0.0:
            worst = 0.0
            worst_price = float(entry)
            worst_ts = entry_utc_ms

        first_bar_partial = bool(
            selected
            and entry_utc_ms
            > selected[0][
                "open_utc_ms"
            ]
        )

        last_bar_partial = bool(
            selected
            and close_utc_ms
            < selected[-1][
                "close_utc_ms"
            ]
        )

        # M1 OHLC cannot distinguish movement before entry or after exit inside
        # the boundary candles. Keep this visible in confidence classification.
        if (
            coverage_pct >= 95.0
            and gap_count == 0
        ):
            confidence = "HIGH"
        elif coverage_pct >= 70.0:
            confidence = "MEDIUM"
        elif observed_bars > 0:
            confidence = "LOW"
        else:
            confidence = "UNAVAILABLE"

        eligible = bool(
            confidence == "HIGH"
            and observed_bars > 0
        )

        pipf = _pip(
            snap.get("symbol")
        )

        out = {
            "mfe_r": (
                round(best, 3)
                if best is not None
                else None
            ),
            "mae_r": (
                round(worst, 3)
                if worst is not None
                else None
            ),

            "excursion_source": (
                "BROKER_M1"
                if observed_bars
                else "BROKER_M1_UNAVAILABLE"
            ),
            "excursion_timeframe": "M1",
            "excursion_precision": (
                confidence.lower()
            ),
            "excursion_confidence": (
                confidence
            ),

            "excursion_expected_bars": (
                expected_bars
            ),
            "excursion_observed_bars": (
                observed_bars
            ),
            "excursion_bars_used": (
                observed_bars
            ),
            "excursion_coverage_pct": (
                coverage_pct
            ),
            "excursion_gap_count": (
                gap_count
            ),

            "excursion_first_bar_partial": (
                first_bar_partial
            ),
            "excursion_last_bar_partial": (
                last_bar_partial
            ),

            "excursion_window_start_utc_ms": (
                entry_utc_ms
            ),
            "excursion_window_end_utc_ms": (
                close_utc_ms
            ),
            "excursion_broker_offset_minutes": (
                offset_min
            ),

            "excursion_initial_sl_source": (
                "ENTRY_SNAPSHOT"
            ),
            "excursion_eligible_for_optimization": (
                eligible
            ),
        }

        if best_price is not None:
            out["mfe_price"] = round(
                best_price,
                5,
            )
            out["mfe_pips"] = (
                round(
                    abs(
                        best_price - entry
                    )
                    / pipf,
                    1,
                )
                if pipf
                else None
            )
            out["mfe_bar_ts_ms"] = (
                best_ts
            )

        if worst_price is not None:
            out["mae_price"] = round(
                worst_price,
                5,
            )
            out["mae_pips"] = (
                round(
                    abs(
                        worst_price - entry
                    )
                    / pipf,
                    1,
                )
                if pipf
                else None
            )
            out["mae_bar_ts_ms"] = (
                worst_ts
            )

        return out

    except Exception as exc:
        log.warning(
            "analytics: M1 excursion "
            "failed ticket=%s err=%r",
            snap.get("mt5_ticket")
            if isinstance(snap, dict)
            else None,
            exc,
        )
        return {}

def _excursion_r(snap, bars_h1, entry, sl) -> dict:
    """Compute conservative H1 MFE/MAE inside the verified trade lifetime.

    Timestamp contract:
      * broker_open_time_utc_ms / broker_close_time_utc_ms are already UTC.
      * enqueue_timestamp / opened_at_ms are server UTC fallbacks and MUST NOT
        be shifted by the broker offset.
      * OHLC bar timestamps are broker-wall values and are normalized here.

    Only fully enclosed H1 bars are measured. Partial entry/exit candles are
    deliberately excluded because H1 OHLC cannot distinguish movement before
    entry or after exit. Broker-verified realized R is retained as a lower
    bound when excluded partial candles contain the actual close:
      * verified winner -> MFE cannot be below realized positive R
      * verified loser  -> MAE cannot be above realized negative R

    When no complete H1 bar is available, unavailable sides are returned as
    None rather than 0.0. This keeps "measurement unavailable" distinct from
    a genuine zero excursion.
    """
    try:
        side = str(snap.get("side") or "").upper().strip()
        risk = abs(float(entry) - float(sl))
        if side not in ("BUY", "SELL") or risk <= 0:
            return {}

        offset_min = int(
            _safe_int(snap.get("broker_tz_offset_minutes"), 0)
            or 0
        )
        offset_ms = offset_min * 60_000

        # Explicit UTC broker truth wins. Server-side entry fallbacks are
        # already UTC and are never offset-adjusted.
        entry_utc_ms = _norm_ms(
            snap.get("broker_open_time_utc_ms")
            or snap.get("enqueue_timestamp")
            or snap.get("opened_at_ms")
            or 0
        )
        close_utc_ms = _norm_ms(
            snap.get("broker_close_time_utc_ms")
            or 0
        )

        # Compatibility fallback: close_timestamp/broker_close_time_ms are
        # broker-wall fields in the broker-deal path. Convert only those raw
        # broker-wall values to UTC.
        if close_utc_ms <= 0:
            raw_close_ms = _norm_ms(
                snap.get("broker_close_time_ms")
                or snap.get("close_timestamp")
                or 0
            )
            if raw_close_ms > 0:
                close_utc_ms = raw_close_ms - offset_ms

        pipf = _pip(snap.get("symbol"))
        best = None
        worst = None
        best_price = worst_price = None
        best_ts = worst_ts = None
        best_idx = worst_idx = None
        used = 0

        for bar in bars_h1 or []:
            if not isinstance(bar, dict):
                continue

            # Raw ``t`` is always bar-open. Older trend adapters also copied it
            # into t_close_ms while leaving t_open_ms empty, so treat a lone
            # t_close_ms as bar-open rather than trusting the label.
            raw_t = _norm_ms(bar.get("t") or 0)
            explicit_open = _norm_ms(bar.get("t_open_ms") or 0)
            labeled_close = _norm_ms(bar.get("t_close_ms") or 0)

            if explicit_open > 0:
                bar_open_wall_ms = explicit_open
            elif raw_t > 0:
                bar_open_wall_ms = raw_t
            elif labeled_close > 0:
                bar_open_wall_ms = labeled_close
            else:
                continue

            bar_close_wall_ms = bar_open_wall_ms + 3_600_000
            bar_open_utc_ms = bar_open_wall_ms - offset_ms
            bar_close_utc_ms = bar_close_wall_ms - offset_ms

            # Only complete bars wholly inside the actual trade lifetime.
            if entry_utc_ms and bar_open_utc_ms < entry_utc_ms:
                continue
            if close_utc_ms and bar_close_utc_ms > close_utc_ms:
                continue

            hi = _safe_float(bar.get("h"))
            lo = _safe_float(bar.get("l"))
            if hi is None or lo is None:
                continue

            idx = used
            used += 1

            if side == "BUY":
                fav = (hi - entry) / risk
                adv = (lo - entry) / risk
                fav_price, adv_price = hi, lo
            else:
                fav = (entry - lo) / risk
                adv = (entry - hi) / risk
                fav_price, adv_price = lo, hi

            if best is None or fav > best:
                best, best_price, best_ts, best_idx = (
                    fav, fav_price, bar_open_utc_ms, idx
                )
            if worst is None or adv < worst:
                worst, worst_price, worst_ts, worst_idx = (
                    adv, adv_price, bar_open_utc_ms, idx
                )

        # Entry is the exact 0R baseline. Keep unavailable H1 measurements as
        # None, but constrain measured excursions to their valid sign domains.
        if best is not None and best < 0.0:
            best = 0.0
            best_price = float(entry)
            best_ts = entry_utc_ms or None
            best_idx = 0

        if worst is not None and worst > 0.0:
            worst = 0.0
            worst_price = float(entry)
            worst_ts = entry_utc_ms or None
            worst_idx = 0

        # Excursion is measured in price-R using abs(entry - sl), therefore
        # its invariant floor must use price-based realized_r. realized_r_net
        # divides money P/L by frozen planned risk_usd and is a different unit.
        realized_price = _safe_float(snap.get("realized_r"))
        realized_exit_r = realized_price

        floor_applied = None
        if bool(snap.get("broker_verified")) and realized_exit_r is not None:
            if realized_exit_r > 0 and (best is None or realized_exit_r > best):
                best = float(realized_exit_r)
                best_price = float(
                    entry + (best * risk)
                    if side == "BUY"
                    else entry - (best * risk)
                )
                best_ts = close_utc_ms or None
                best_idx = used
                floor_applied = "MFE_REALIZED_WIN_FLOOR"
            elif realized_exit_r < 0 and (worst is None or realized_exit_r < worst):
                worst = float(realized_exit_r)
                worst_price = float(
                    entry + (worst * risk)
                    if side == "BUY"
                    else entry - (worst * risk)
                )
                worst_ts = close_utc_ms or None
                worst_idx = used
                floor_applied = "MAE_REALIZED_LOSS_FLOOR"

        out = {
            "mfe_r": round(best, 2) if best is not None else None,
            "mae_r": round(worst, 2) if worst is not None else None,
            "excursion_source": "h1_fully_enclosed_bars",
            "excursion_precision": (
                "low" if used
                else "broker_exit_floor_only" if floor_applied
                else "unavailable"
            ),
            "excursion_bars_used": used,
            "excursion_window_start_utc_ms": entry_utc_ms or None,
            "excursion_window_end_utc_ms": close_utc_ms or None,
            "excursion_broker_offset_minutes": offset_min,
            "excursion_partial_bars_excluded": True,
            "excursion_realized_floor_applied": floor_applied,
        }
        if best_price is not None:
            out["mfe_price"] = round(best_price, 5)
            out["mfe_pips"] = round(abs(best_price - entry) / pipf, 1) if pipf else None
            out["mfe_bars_after_entry"] = best_idx
            out["mfe_bar_ts_ms"] = best_ts
        if worst_price is not None:
            out["mae_price"] = round(worst_price, 5)
            out["mae_pips"] = round(abs(worst_price - entry) / pipf, 1) if pipf else None
            out["mae_bars_after_entry"] = worst_idx
            out["mae_bar_ts_ms"] = worst_ts
        return out
    except Exception as exc:
        log.warning(
            "analytics: excursion computation failed ticket=%s err=%r",
            snap.get("mt5_ticket") if isinstance(snap, dict) else None,
            exc,
        )
        return {}


_EXCURSION_FIELDS = (
    "mfe_r",
    "mae_r",
    "mfe_price",
    "mae_price",
    "mfe_price_source",
    "mae_price_source",
    "mfe_pips",
    "mae_pips",
    "mfe_bars_after_entry",
    "mae_bars_after_entry",
    "mfe_bar_ts_ms",
    "mae_bar_ts_ms",

    "excursion_source",
    "excursion_timeframe",
    "excursion_precision",
    "excursion_confidence",

    "excursion_bars_used",
    "excursion_expected_bars",
    "excursion_observed_bars",
    "excursion_coverage_pct",
    "excursion_gap_count",

    "excursion_first_bar_partial",
    "excursion_last_bar_partial",
    "excursion_boundary_precision",
    "excursion_boundary_extrema_exact",

    "excursion_window_start_utc_ms",
    "excursion_window_end_utc_ms",
    "excursion_broker_offset_minutes",

    "excursion_partial_bars_excluded",
    "excursion_realized_floor_applied",
    "excursion_initial_sl_source",
    "excursion_eligible_for_optimization",

    "excursion_live_price_samples",
    "excursion_finalized_at_ms",
    "excursion_fallback_reason",
    "excursion_history_truncated_bars",
    "excursion_internal_gap_count",
    "excursion_retained_expected_bars",
    "excursion_retained_coverage_pct",
    "excursion_leading_unobserved_bars",
    "excursion_trailing_unobserved_bars",
    "excursion_coverage_status",
    "excursion_confidence_reason",
)

def _clear_excursion_fields(record: dict) -> None:
    """Remove stale excursion values before any lifecycle-path recomputation."""
    if not isinstance(record, dict):
        return
    for key in _EXCURSION_FIELDS:
        record.pop(key, None)


def _finalize_live_excursion(
    record: dict,
) -> dict:
    """
    Convert the persisted live M1 accumulator into final MFE/MAE fields.

    Source priority inside the accumulator:
      1. completed M1 high/low
      2. broker live-price samples
      3. exact broker exit fill

    The original frozen entry SL remains the R normalizer.

    This is analytics-only and never changes execution.
    """
    try:
        if not isinstance(record, dict):
            return {}

        live = record.get("excursion_live")

        if not isinstance(live, dict):
            return {}

        side = str(
            record.get("side")
            or ""
        ).upper().strip()

        entry = _safe_float(
            record.get("entry_price")
        )

        sl = _safe_float(
            record.get("sl_price")
        )

        if (
            side not in ("BUY", "SELL")
            or entry is None
            or sl is None
        ):
            return {}

        risk = abs(
            float(entry)
            - float(sl)
        )

        if risk <= 0:
            return {}

        entry_ms = _norm_ms(
            record.get("broker_open_time_utc_ms")
            or record.get("enqueue_timestamp")
            or record.get("opened_at_ms")
            or live.get("entry_utc_ms")
            or 0
        )

        close_ms = _norm_ms(
            record.get("broker_close_time_utc_ms")
            or record.get("close_timestamp")
            or record.get("broker_close_time_ms")
            or 0
        )

        if (
            entry_ms <= 0
            or close_ms <= entry_ms
        ):
            return {}

        highest_high = _safe_float(
            live.get("highest_high")
        )

        lowest_low = _safe_float(
            live.get("lowest_low")
        )

        highest_ts_ms = _norm_ms(
            live.get("highest_high_ts_ms")
            or 0
        ) or None

        lowest_ts_ms = _norm_ms(
            live.get("lowest_low_ts_ms")
            or 0
        ) or None

        highest_source = str(
            live.get("highest_high_source")
            or ""
        ).strip() or None

        lowest_source = str(
            live.get("lowest_low_source")
            or ""
        ).strip() or None

        # Entry itself is exact excursion evidence at 0R. Older accumulators
        # may not contain this baseline, so restore it during finalization.
        if highest_high is None or highest_high < float(entry):
            highest_high = float(entry)
            highest_ts_ms = int(entry_ms)
            highest_source = "ENTRY_BASELINE"

        if lowest_low is None or lowest_low > float(entry):
            lowest_low = float(entry)
            lowest_ts_ms = int(entry_ms)
            lowest_source = "ENTRY_BASELINE"

        # -------------------------------------------------
        # The broker exit fill is an exact price observed
        # inside the trade lifetime. Always include it.
        # -------------------------------------------------
        exit_price = _safe_float(
            record.get("exit_price")
            or record.get("close_price")
        )

        if exit_price is not None:
            if (
                highest_high is None
                or float(exit_price)
                > highest_high
            ):
                highest_high = float(
                    exit_price
                )
                highest_ts_ms = int(
                    close_ms
                )
                highest_source = (
                    "BROKER_EXIT_FILL"
                )

            if (
                lowest_low is None
                or float(exit_price)
                < lowest_low
            ):
                lowest_low = float(
                    exit_price
                )
                lowest_ts_ms = int(
                    close_ms
                )
                lowest_source = (
                    "BROKER_EXIT_FILL"
                )

        if (
            highest_high is None
            or lowest_low is None
        ):
            return {}

        if side == "BUY":
            mfe_price = highest_high
            mae_price = lowest_low

            mfe_r = (
                highest_high
                - float(entry)
            ) / risk

            mae_r = (
                lowest_low
                - float(entry)
            ) / risk

            mfe_ts_ms = highest_ts_ms
            mae_ts_ms = lowest_ts_ms

            mfe_source = highest_source
            mae_source = lowest_source

        else:
            mfe_price = lowest_low
            mae_price = highest_high

            mfe_r = (
                float(entry)
                - lowest_low
            ) / risk

            mae_r = (
                float(entry)
                - highest_high
            ) / risk

            mfe_ts_ms = lowest_ts_ms
            mae_ts_ms = highest_ts_ms

            mfe_source = lowest_source
            mae_source = highest_source

        # -------------------------------------------------
        # Broker-truth realized-R safety floor.
        #
        # A profitable close cannot have MFE below its
        # realized price R, and a losing close cannot have
        # MAE less adverse than its realized price R.
        # -------------------------------------------------
        realized_r = _safe_float(
            record.get("realized_r")
        )

        floor_applied = False

        if realized_r is not None:
            if (
                realized_r > 0
                and mfe_r < realized_r
            ):
                mfe_r = realized_r

                if exit_price is not None:
                    mfe_price = float(
                        exit_price
                    )
                    mfe_ts_ms = int(
                        close_ms
                    )
                    mfe_source = (
                        "BROKER_EXIT_REALIZED_FLOOR"
                    )

                floor_applied = True

            elif (
                realized_r < 0
                and mae_r > realized_r
            ):
                mae_r = realized_r

                if exit_price is not None:
                    mae_price = float(
                        exit_price
                    )
                    mae_ts_ms = int(
                        close_ms
                    )
                    mae_source = (
                        "BROKER_EXIT_REALIZED_FLOOR"
                    )

                floor_applied = True

        entry_minute_ms = (
            entry_ms // 60_000
        ) * 60_000

        close_minute_ms = (
            close_ms // 60_000
        ) * 60_000

        expected_bars = int(
            (
                close_minute_ms
                - entry_minute_ms
            )
            // 60_000
        ) + 1

        observed_bars = int(
            _safe_int(
                live.get(
                    "observed_m1_bars"
                ),
                0,
            )
            or 0
        )

        internal_gap_count = int(
            _safe_int(
                live.get(
                    "internal_gap_count"
                ),
                live.get("gap_count")
                or 0,
            )
            or 0
        )

        history_truncated_bars = int(
            _safe_int(
                live.get(
                    "history_truncated_bars"
                ),
                0,
            )
            or 0
        )

        coverage_pct = (
            round(
                min(
                    100.0,
                    observed_bars
                    / expected_bars
                    * 100.0,
                ),
                2,
            )
            if expected_bars > 0
            else 0.0
        )
        retained_expected_bars = max(
            0,
            expected_bars
            - history_truncated_bars,
        )

        retained_coverage_pct = (
            round(
                min(
                    100.0,
                    observed_bars
                    / retained_expected_bars
                    * 100.0,
                ),
                2,
            )
            if retained_expected_bars > 0
            else 0.0
        )

        first_partial = bool(
            entry_ms % 60_000
        )

        last_partial = bool(
            close_ms % 60_000
        )

        live_samples = int(
            _safe_int(
                live.get(
                    "live_price_samples"
                ),
                0,
            )
            or 0
        )

        first_observed_ms = _norm_ms(live.get("first_m1_open_ms") or 0)
        last_observed_ms = _norm_ms(live.get("last_m1_open_ms") or 0)
        leading_unobserved_bars = (
            max(0, int((first_observed_ms - entry_minute_ms) // 60_000))
            if first_observed_ms > entry_minute_ms else 0
        )
        trailing_unobserved_bars = (
            max(0, int((close_minute_ms - last_observed_ms) // 60_000))
            if last_observed_ms > 0 and close_minute_ms > last_observed_ms
            else expected_bars if last_observed_ms <= 0 else 0
        )

        # -------------------------------------------------
        # Confidence policy
        # -------------------------------------------------
        if (
            coverage_pct >= 95.0
            and internal_gap_count == 0
            and history_truncated_bars == 0
            and observed_bars > 0
        ):
            confidence = "HIGH"

        elif (
            coverage_pct >= 70.0
            and observed_bars > 0
        ):
            confidence = "MEDIUM"

        elif (
            observed_bars > 0
            or live_samples > 0
        ):
            confidence = "LOW"

        else:
            confidence = "UNAVAILABLE"

        # Only high-coverage records may later be used to
        # tune entry gates or exits automatically.
        eligible = bool(
            confidence == "HIGH"
            and coverage_pct >= 95.0
            and internal_gap_count == 0
            and history_truncated_bars == 0

        )

        if confidence == "HIGH":
            coverage_status = "COMPLETE"
            confidence_reason = "M1_LIFETIME_COVERAGE_GE_95_PCT"
        elif leading_unobserved_bars and trailing_unobserved_bars:
            coverage_status = "PARTIAL_BOTH_BOUNDARIES"
            confidence_reason = "M1_LEADING_AND_TRAILING_COVERAGE_MISSING"
        elif leading_unobserved_bars:
            coverage_status = "PARTIAL_LEADING"
            confidence_reason = "M1_HISTORY_BEGAN_AFTER_ENTRY"
        elif trailing_unobserved_bars:
            coverage_status = "PARTIAL_TRAILING"
            confidence_reason = "M1_FEED_DID_NOT_REACH_BROKER_CLOSE"
        else:
            coverage_status = "PARTIAL_INTERNAL_OR_SPARSE"
            confidence_reason = "M1_LIFETIME_COVERAGE_BELOW_95_PCT"

        pipf = _pip(
            record.get("symbol")
        )

        return {
            "mfe_r": round(
                float(mfe_r),
                4,
            ),
            "mae_r": round(
                float(mae_r),
                4,
            ),

            "mfe_price": round(
                float(mfe_price),
                8,
            ),
            "mae_price": round(
                float(mae_price),
                8,
            ),

            "mfe_pips": (
                round(
                    abs(
                        float(mfe_price)
                        - float(entry)
                    )
                    / pipf,
                    1,
                )
                if pipf
                else None
            ),

            "mae_pips": (
                round(
                    abs(
                        float(mae_price)
                        - float(entry)
                    )
                    / pipf,
                    1,
                )
                if pipf
                else None
            ),

            "mfe_bar_ts_ms": (
                int(mfe_ts_ms)
                if mfe_ts_ms
                else None
            ),

            "mae_bar_ts_ms": (
                int(mae_ts_ms)
                if mae_ts_ms
                else None
            ),

            "mfe_price_source": (
                mfe_source
            ),

            "mae_price_source": (
                mae_source
            ),

            "excursion_source": (
                "LIVE_M1_ACCUMULATOR"
            ),

            "excursion_timeframe": "M1",

            "excursion_precision": (
                "high"
                if confidence == "HIGH"
                else (
                    "medium"
                    if confidence == "MEDIUM"
                    else "low"
                )
            ),

            "excursion_confidence": (
                confidence
            ),

            "excursion_bars_used": (
                observed_bars
            ),

            "excursion_expected_bars": (
                expected_bars
            ),

            "excursion_observed_bars": (
                observed_bars
            ),

            "excursion_coverage_pct": (
                coverage_pct
            ),

            "excursion_gap_count": (
                internal_gap_count
            ),

            "excursion_internal_gap_count": (
                internal_gap_count
            ),

            "excursion_history_truncated_bars": (
                history_truncated_bars
            ),

            "excursion_retained_expected_bars": (
                retained_expected_bars
            ),

            "excursion_retained_coverage_pct": (
                retained_coverage_pct
            ),

            "excursion_leading_unobserved_bars": leading_unobserved_bars,
            "excursion_trailing_unobserved_bars": trailing_unobserved_bars,
            "excursion_coverage_status": coverage_status,
            "excursion_confidence_reason": confidence_reason,

            "excursion_first_bar_partial": (
                first_partial
            ),

            "excursion_last_bar_partial": (
                last_partial
            ),

            "excursion_boundary_precision": (
                live.get(
                    "boundary_precision"
                )
                or "M1_OHLC_PARTIAL_BOUNDARIES"
            ),

            "excursion_boundary_extrema_exact": (
                False
            ),

            "excursion_window_start_utc_ms": (
                int(entry_ms)
            ),

            "excursion_window_end_utc_ms": (
                int(close_ms)
            ),

            "excursion_broker_offset_minutes": 0,

            "excursion_partial_bars_excluded": (
                False
            ),

            "excursion_realized_floor_applied": (
                floor_applied
            ),

            "excursion_initial_sl_source": (
                "ENTRY_SNAPSHOT"
            ),

            "excursion_eligible_for_optimization": (
                eligible
            ),

            "excursion_live_price_samples": (
                live_samples
            ),

            "excursion_finalized_at_ms": (
                _now_ms()
            ),
        }

    except Exception as exc:
        log.warning(
            "analytics: live excursion finalize "
            "failed ticket=%s err=%r",
            (
                record.get("mt5_ticket")
                if isinstance(record, dict)
                else None
            ),
            exc,
        )
        return {}


def _recompute_excursion(
    record: dict,
    bars_h1: list | None = None,
    fetch_h1_bars=None,
    fetch_m1_bars=None,
) -> dict:
    """
    Rebuild excursion from the record's current final truth.

    Priority:
      1. Persisted live M1 accumulator
      2. Current rolling M1 Redis snapshot
      3. Conservative H1 fallback

    Every path writes source, confidence, coverage and optimization eligibility.
    """
    _clear_excursion_fields(record)

    entry = _safe_float(
        record.get("entry_price")
    )

    sl = _safe_float(
        record.get("sl_price")
    )

    if entry is None or sl is None:
        return {}

    # -------------------------------------------------
    # 1. Persisted live M1 accumulator
    # -------------------------------------------------
    live_result = (
        _finalize_live_excursion(
            record
        )
    )

    if live_result:
        record.update(
            live_result
        )
        return live_result

    start_ms = _norm_ms(
        record.get(
            "broker_open_time_utc_ms"
        )
        or record.get(
            "enqueue_timestamp"
        )
        or record.get(
            "opened_at_ms"
        )
        or 0
    )

    end_ms = _norm_ms(
        record.get(
            "broker_close_time_utc_ms"
        )
        or record.get(
            "close_timestamp"
        )
        or record.get(
            "broker_close_time_ms"
        )
        or _now_ms()
    )

    device_id = _resolve_device(
        record
    )

    # -------------------------------------------------
    # 2. Current rolling M1 snapshot
    # -------------------------------------------------
    m1_fetcher = (
        fetch_m1_bars
        or default_fetch_m1_bars
    )

    bars_m1 = []

    try:
        bars_m1 = m1_fetcher(
            record.get("symbol"),
            start_ms,
            end_ms,
            device_id,
        ) or []

    except TypeError:
        try:
            bars_m1 = m1_fetcher(
                record.get("symbol"),
                start_ms,
                end_ms,
            ) or []
        except Exception:
            bars_m1 = []

    except Exception:
        bars_m1 = []

    if bars_m1:
        m1_result = _excursion_r_m1(
            record,
            bars_m1,
            entry,
            sl,
        )

        if m1_result:
            # Rolling M1 may cover only the final five hours.
            # Its own coverage calculation determines whether
            # it is eligible for optimization.
            record.update(
                m1_result
            )
            return m1_result

    # -------------------------------------------------
    # 3. Conservative H1 fallback
    # -------------------------------------------------
    bars = list(
        bars_h1 or []
    )

    if (
        not bars
        and fetch_h1_bars is not None
    ):
        try:
            bars = fetch_h1_bars(
                record.get("symbol"),
                start_ms,
                end_ms,
                device_id,
            ) or []

        except TypeError:
            bars = fetch_h1_bars(
                record.get("symbol"),
                start_ms,
                end_ms,
            ) or []

        except Exception:
            bars = []

    h1_result = _excursion_r(
        record,
        bars,
        entry,
        sl,
    )

    if h1_result:
        h1_result.update({
            "excursion_timeframe": "H1",
            "excursion_confidence": "LOW",
            "excursion_eligible_for_optimization": False,
            "excursion_initial_sl_source": "ENTRY_SNAPSHOT",
            "excursion_fallback_reason": (
                "LIVE_M1_AND_ROLLING_M1_UNAVAILABLE"
            ),
            "excursion_finalized_at_ms": _now_ms(),
        })

        record.update(
            h1_result
        )

    return h1_result

# -- FINALIZE + APPEND --------------------------------------------------------
def _jsonl_has_ticket_unlocked(ticket: str) -> bool:
    """Return True when the permanent JSONL already contains this MT5 ticket.

    Caller must hold _trades_jsonl_lock().
    """
    ticket_s = str(ticket or "").strip()
    if not ticket_s or not os.path.exists(JSONL_PATH):
        return False

    try:
        with open(JSONL_PATH, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                try:
                    row = json.loads(line)
                except Exception:
                    continue

                if not isinstance(row, dict):
                    continue

                if str(row.get("mt5_ticket") or "").strip() == ticket_s:
                    return True

    except FileNotFoundError:
        return False

    return False


def _append_jsonl(record: dict) -> bool:
    """Durably append one analytics row, idempotent by MT5 ticket."""
    try:
        if not isinstance(record, dict):
            log.error(
                "analytics: JSONL append rejected non-dict type=%s",
                type(record).__name__,
            )
            return False

        ticket = str(record.get("mt5_ticket") or "").strip()

        with _trades_jsonl_lock():
            # P0: a retry after append-but-before-Redis-delete must not create
            # a duplicate permanent row.
            if ticket and _jsonl_has_ticket_unlocked(ticket):
                log.warning(
                    "analytics: JSONL ticket already present; treating append "
                    "as success ticket=%s",
                    ticket,
                )
                return True

            os.makedirs(
                os.path.dirname(JSONL_PATH),
                exist_ok=True,
            )

            with open(JSONL_PATH, "a", encoding="utf-8") as f:
                f.write(
                    json.dumps(
                        record,
                        default=str,
                        separators=(",", ":"),
                    )
                    + "\n"
                )
                f.flush()
                os.fsync(f.fileno())

        return True

    except Exception as exc:
        log.error(
            "analytics: JSONL append failed ticket=%s err=%s",
            (record or {}).get("mt5_ticket")
            if isinstance(record, dict)
            else None,
            exc,
        )
        return False


def _holding_minutes(snap) -> int:
    try:
        open_ms = int(
            snap.get("broker_open_time_ms")
            or snap.get("opened_at_ms")
            or 0
        )

        close_ms = int(
            snap.get("broker_close_time_ms")
            or snap.get("close_timestamp")
            or 0
        )

        if open_ms > 0 and close_ms > open_ms:
            return int(round((close_ms - open_ms) / 60000.0))

        return None

    except Exception:
        return None


def _apply_news_during_trade(snap: dict) -> None:
    
    """
    Determine whether a relevant high-impact event occurred while
    the position was open.

    Calendar time_ms is UTC. Prefer explicitly normalized broker UTC
    timestamps because this MT5 feed encodes broker-local wall time
    into its raw deal timestamp fields.
    """
    
    snap["news_during_trade"] = False
    snap["news_event_during"] = None
    snap["news_event_during_ms"] = None
    snap["news_currency_during"] = None
    snap["news_impact_during"] = None
    snap["news_event_mins_after_entry"] = None
    

    try:
        entry_ms = int(
            snap.get("broker_open_time_utc_ms")
            or snap.get("enqueue_timestamp")
            or snap.get("ts_ms")
            or 0
        )

        close_ms = int(
            snap.get("broker_close_time_utc_ms")
            or snap.get("close_timestamp")
            or 0
        )

        if entry_ms <= 0 or close_ms <= entry_ms:
            return

        frozen = snap.get("upcoming_events") or []

        # Entry-time event freezing may have failed or the calendar may
        # have been temporarily unavailable. Rebuild from the current
        # calendar using the actual broker trade duration.
        if not frozen:
            duration_h = max(
                48,
                int((close_ms - entry_ms) / 3_600_000) + 2,
            )

            frozen = _freeze_currency_events(
                str(snap.get("symbol") or ""),
                entry_ms,
                horizon_h=duration_h,
            )

            snap["upcoming_events"] = frozen

        hit = None

        for event in frozen:
            if not isinstance(event, dict):
                continue

            event_ms = int(
                event.get("time_ms") or 0
            )

            if (
                event_ms > 0
                and entry_ms <= event_ms <= close_ms
            ):
                hit = event
                break

        if not hit:
            return

        event_ms = int(
            hit.get("time_ms") or 0
        )

        snap["news_during_trade"] = True
        snap["news_event_during"] = hit.get("event")
        snap["news_event_during_ms"] = event_ms
        snap["news_currency_during"] = hit.get("currency")
        snap["news_impact_during"] = hit.get("impact")
        snap["news_event_mins_after_entry"] = round(
            (event_ms - entry_ms) / 60000.0,
            1,
        )

    except Exception as exc:
        log.warning(
            "analytics: during-trade news check failed "
            "ticket=%s err=%r",
            snap.get("mt5_ticket"),
            exc,
        )
def _phase1_level_change(record: dict, kind: str) -> dict:
    original = _safe_float(record.get(f"{kind}_original"))
    final = _safe_float(record.get(f"{kind}_final"))
    entry = _safe_float(record.get("entry_price"))
    symbol = str(record.get("symbol") or "")
    side = str(record.get("side") or "").upper().strip()
    delta = final - original if original is not None and final is not None else None
    # The module has no broker symbol-spec/tick-size snapshot.  Use its existing
    # pip unit as the conservative normalization unit; <= half a unit is broker
    # precision noise, while larger changes remain UNKNOWN without provenance.
    tick = _pip(symbol) if symbol else None
    ticks = delta / tick if delta is not None and tick else None
    changed = delta is not None and abs(delta) > 1e-9
    change_type = "NONE"
    evidence = {"original": original, "final": final, "delta_price": delta,
                "normalization_unit": tick, "delta_ticks": ticks,
                "normalization_unit_source": "EXISTING_SYMBOL_PIP"}
    if changed:
        if tick and abs(delta) <= tick * 0.500001:
            change_type = "BROKER_ROUNDING"
        elif kind == "sl" and entry is not None and final is not None and tick and abs(final - entry) <= tick:
            change_type = "BREAK_EVEN"
        elif kind == "sl" and entry is not None and side in ("BUY", "SELL"):
            if (side == "BUY" and final > entry) or (side == "SELL" and final < entry):
                change_type = "PROFIT_LOCK"
            else:
                original_risk = abs(original - entry) if original is not None else None
                final_risk = abs(final - entry)
                change_type = (
                    "RISK_REDUCED" if original_risk is not None and final_risk < original_risk
                    else "RISK_INCREASED" if original_risk is not None and final_risk > original_risk
                    else "UNKNOWN"
                )
        elif kind == "tp" and side in ("BUY", "SELL"):
            favorable_delta = delta if side == "BUY" else -delta
            change_type = "TP_EXTENDED" if favorable_delta > 0 else "TP_REDUCED"
        else:
            # The closed-trade payload does not prove who/what changed a level.
            # Preserve uncertainty instead of inventing trailing/manual intent.
            change_type = "UNKNOWN"
    return {"change_type": change_type, "evidence": evidence}


def _apply_phase1_close_analytics(snap: dict) -> None:
    """Add deterministic close-time summaries immediately before JSONL write."""
    if not isinstance(snap, dict):
        return
    try:
        sl_change = _phase1_level_change(snap, "sl")
        tp_change = _phase1_level_change(snap, "tp")
        snap["sl_change_type"] = sl_change["change_type"]
        snap["tp_change_type"] = tp_change["change_type"]
        snap["sl_delta"] = sl_change["evidence"]["delta_price"]
        snap["tp_delta"] = tp_change["evidence"]["delta_price"]
        snap["sl_change_ticks"] = sl_change["evidence"]["delta_ticks"]
        snap["tp_change_ticks"] = tp_change["evidence"]["delta_ticks"]
        snap["tp_sl_change_classification"] = {
            "schema_version": 1, "sl": sl_change, "tp": tp_change,
            "broker_normalized": (
                sl_change["change_type"] in ("NONE", "BROKER_ROUNDING") and
                tp_change["change_type"] in ("NONE", "BROKER_ROUNDING")
            ), "analytics_only": True,
        }

        reason = str(snap.get("exit_reason") or "").lower()
        net = _safe_float(snap.get("net_profit"))
        rr = _safe_float(snap.get("realized_r"))
        outcome = ("TP_HIT" if reason == "tp" else "SL_HIT" if reason == "sl" else
                   "BREAK_EVEN" if (rr is not None and abs(rr) <= 0.05) or (rr is None and net is not None and abs(net) <= 0.01) else
                   "MANUAL_CLOSE" if reason == "manual" else "BROKER_CLOSE")
        snap["outcome_classification"] = outcome
        snap["outcome_classification_evidence"] = {
            "exit_reason": reason or None, "exit_reason_source": snap.get("exit_reason_source"),
            "realized_r": rr, "realized_r_net": snap.get("realized_r_net"), "net_profit": net,
            "broker_verified": bool(snap.get("broker_verified")),
        }

        mfe = _safe_float(snap.get("mfe_r")); mae = _safe_float(snap.get("mae_r"))
        behavior = []
        def add(label, facts): behavior.append(_phase1_label(label, facts))
        if mae is not None and mae <= -0.50 and (mfe is None or mfe < 0.10):
            add("IMMEDIATE_ADVERSE_MOVE", {"mae_r": mae, "mfe_r": mfe, "threshold_mae_r": -0.50})
        if mfe is not None and mfe < 0.10:
            add("NO_MEANINGFUL_FAVORABLE_MOVE", {"mfe_r": mfe, "threshold_r": 0.10})
        if mfe is not None and mfe >= 0.50 and outcome in ("SL_HIT", "BREAK_EVEN"):
            add("FAVORABLE_THEN_REVERSED", {"mfe_r": mfe, "outcome": outcome})
        if outcome == "TP_HIT" and mae is not None and mae > -0.25:
            add("CLEAN_TREND_FOLLOW_THROUGH", {"mae_r": mae, "outcome": outcome, "threshold_mae_r": -0.25})
        dxy_summary = snap.get("dxy_m15_trade_summary") if isinstance(snap.get("dxy_m15_trade_summary"), dict) else {}
        if str(dxy_summary.get("trade_alignment") or dxy_summary.get("final_alignment") or "").upper() == "AGAINST":
            add("REAL_DXY_TURNED_AGAINST", {"source": "REAL_DXY", "trade_alignment": "AGAINST"})
        snap["trade_behavior_flags"] = [x["classification"] for x in behavior]
        snap["trade_behavior_classifications"] = behavior

        missing = []
        def require(ok, name):
            if not ok: missing.append(name)
            return bool(ok)
        entry_ok = require(bool(snap.get("entry_price") is not None and snap.get("enqueue_timestamp")), "ENTRY_SNAPSHOT")
        broker_entry_ok = require(bool(snap.get("broker_open_time_ms") and snap.get("mt5_ticket")), "BROKER_ENTRY_TRUTH")
        broker_exit_ok = require(bool(snap.get("broker_verified") and snap.get("exit_price") is not None), "BROKER_EXIT_TRUTH")
        zone_ok = require(bool(snap.get("zone_low") is not None or snap.get("zone_level") is not None), "ZONE_SNAPSHOT")
        rc_ok = require(bool(snap.get("rc_found") and None not in (snap.get("rc_open_ms"), snap.get("rc_open"), snap.get("rc_high"), snap.get("rc_low"), snap.get("rc_close"))), "ENTRY_RC")
        prediction_ok = require(bool(isinstance(snap.get("setup_analysis"), dict)), "PREDICTION_SNAPSHOT")
        trend_ok = require(bool(snap.get("h1_20_direction") is not None), "TREND_CONTEXT")
        dxy_ok = require(bool(snap.get("dxy_available")), "REAL_DXY_ENTRY_SNAPSHOT")
        risk_ok = require(bool(snap.get("sl_price") is not None and snap.get("tp_price") is not None and snap.get("risk_usd") is not None), "RISK_CONTEXT")
        outcome_ok = require(bool(snap.get("exit_reason") and (rr is not None or net is not None)), "OUTCOME")
        class_ok = require(bool(snap.get("entry_situation_classifications") is not None and snap.get("trade_behavior_classifications") is not None), "CLASSIFICATIONS")
        runtime_present = bool(snap.get("excursion_source") and mfe is not None and mae is not None)
        mfe_sign_ok = bool(mfe is None or mfe >= 0.0)
        mae_sign_ok = bool(mae is None or mae <= 0.0)
        excursion_quality_ok = bool(
            snap.get("excursion_eligible_for_optimization")
        )
        runtime_ok = bool(
            runtime_present
            and mfe_sign_ok
            and mae_sign_ok
            and excursion_quality_ok
        )
        warnings = []
        if snap.get("rc_source") != "entry_confirmation": warnings.append("RC_NOT_FROM_ENTRY_CONFIRMATION")
        if not runtime_present: warnings.append("RUNTIME_EXCURSION_INCOMPLETE")
        elif not excursion_quality_ok: warnings.append("RUNTIME_EXCURSION_LOW_COVERAGE")
        if not mfe_sign_ok: warnings.append("MFE_SIGN_INVALID")
        if not mae_sign_ok: warnings.append("MAE_SIGN_INVALID")
        eligibility = {
            "entry_quality": entry_ok and zone_ok and rc_ok and prediction_ok and trend_ok and risk_ok,
            "outcome_analysis": broker_entry_ok and broker_exit_ok and outcome_ok and risk_ok,
            "real_dxy_analysis": dxy_ok and entry_ok,
            "runtime_management": broker_entry_ok and broker_exit_ok and runtime_ok,
        }
        snap["learning_eligibility"] = eligibility
        snap["pipeline_validation"] = {
            "schema_version": 1, "entry_snapshot_complete": entry_ok,
            "broker_entry_verified": broker_entry_ok, "broker_exit_verified": broker_exit_ok,
            "zone_complete": zone_ok, "rc_complete": rc_ok,
            "prediction_complete": prediction_ok, "trend_context_complete": trend_ok,
            "real_dxy_entry_complete": dxy_ok,
            "real_dxy_tracking_complete": bool(dxy_summary), "risk_context_complete": risk_ok,
            "sl_tp_truth_complete": bool(sl_change and tp_change), "outcome_complete": outcome_ok,
            "classification_complete": class_ok,
            "excursion_sign_valid": bool(mfe_sign_ok and mae_sign_ok),
            "excursion_quality_complete": excursion_quality_ok,
            "missing_fields": missing,
            "warnings": warnings, "ready_for_learning": all(eligibility.values()),
            "analytics_only": True,
        }
    except Exception as exc:
        log.warning("analytics: Phase-1 close analytics failed: %s", exc)


def finalize_ticket(ticket: str, bars_h1: list) -> bool:
    """Read snapshot, approximate exit, append JSONL, delete snapshot.
    Snapshot is deleted ONLY after the durable JSONL write succeeds."""
    try:
        ticket = str(ticket)
        R = from_app_R()
        raw = R.get(SNAP_PREFIX + ticket)
        if not raw:
            return False
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", "ignore")

        snap = json.loads(raw)

        if not isinstance(snap, dict):
            log.error(
                "analytics: finalize invalid snapshot "
                "ticket=%s type=%s",
                ticket,
                type(snap).__name__,
            )
            return False
        # device_id sometimes drops between entry snapshot and finalize; restore
        # from the stable fallback so the record keeps a usable device and
        # excursion/bar re-fetch works.
        if not snap.get("device_id"):
            _dev = _resolve_device(snap)
            if _dev:
                snap["device_id"] = _dev

        # P0 idempotent cleanup:
        # A process can crash after the durable JSONL append but before Redis
        # deletion. On the next sweep, _status is already closed. Re-run the
        # idempotent append check and then remove the stale in-flight snapshot.
        if snap.get("_status") == "closed":
            if _append_jsonl(snap):
                R.delete(SNAP_PREFIX + ticket)
                try:
                    if snap.get("broker_verified"):
                        R.srem(PENDING_TRUTH_KEY, ticket)
                    else:
                        R.sadd(PENDING_TRUTH_KEY, ticket)
                except Exception:
                    pass

                log.warning(
                    "analytics: stale closed snapshot cleaned "
                    "ticket=%s",
                    ticket,
                )
                return True

            return False

        broker_exit = _exit_from_broker_deal(
            ticket,
            snap,
            R,
        )

        if broker_exit:
            snap.update(broker_exit)

            # Broker truth was available on the first finalization pass.
            snap["broker_verified"] = True
            snap["broker_truth_upgraded"] = False
            snap["pending_broker_truth"] = False
            snap["exit_deal_timeout"] = False

        else:
            # -- Deal not present yet   it may still be arriving (the agent's deal
            #    push races this sweep). Defer finalize and retry on later sweeps;
            #    only approximate after a real timeout so nothing hangs forever. --
            _now = _now_ms()
            _first = snap.get("_first_closed_seen_ms")
            if not _first:
                snap["_first_closed_seen_ms"] = _now
                R.set(
                    SNAP_PREFIX + ticket,
                    json.dumps(
                        snap,
                        default=str,
                        separators=(",", ":"),
                    ),
                    ex=SNAP_TTL_SEC,
                )
                return False   # keep snapshot OPEN, retry next sweep
            elif (_now - int(_first)) < DEAL_WAIT_MS:
                R.set(
                    SNAP_PREFIX + ticket,
                    json.dumps(
                        snap,
                        default=str,
                        separators=(",", ":"),
                    ),
                    ex=SNAP_TTL_SEC,
                )
                return False   # still within grace window, retry next sweep
            else:
                snap.update(
                    approximate_exit(
                        snap,
                        bars_h1,
                    )
                )

                snap["exit_deal_timeout"] = True
                snap["pending_broker_truth"] = True
                snap["broker_verified"] = False
                snap["broker_truth_upgraded"] = False

                try:
                    R.sadd(
                        PENDING_TRUTH_KEY,
                        ticket,
                    )
                except Exception:
                    pass

        # Apply net-R and deterministic outcome before excursion so verified
        # losses can floor MAE using actual account impact.
        _apply_realized_r_net_and_outcome(snap)

        # -- ALWAYS recompute excursion from the snapshot's CURRENT truth --
        # This canonical helper clears stale approximate values first and is also
        # used by broker-truth reconciliation and historical repair.
        try:
            if not bars_h1:
                _dev = _resolve_device(snap)
                if _dev:
                    try:
                        bars_h1 = _get_closed_h1_bars(
                            snap.get("symbol"),
                            _dev,
                        ) or []
                    except Exception:
                        bars_h1 = []
            _recompute_excursion(snap, bars_h1=bars_h1)
        except Exception as _e:
            log.warning("analytics: excursion compute failed: %s", _e)

        # Entry expectation remains immutable; actual behavior is derived only
        # after the full trade path is available. Analytics-only.
        try:
            _merge_setup_actual(snap, bars_h1)
        except Exception as _e:
            log.warning("analytics: setup behavior classification failed: %s", _e)
        # Analyze whether price reached the entry-frozen opposing
        # liquidity target during this trade.
        try:
            liq_analysis = analyze_liquidity_target_during_trade(
                snap,
                bars_h1,
            )
            snap.update(liq_analysis)

        except Exception as exc:
            log.warning(
                "analytics: liquidity target finalize failed "
                "ticket=%s err=%r",
                ticket,
                exc,
            )

        # Freeze DXY lifecycle through the actual close time before durable append.
        finalize_dxy_m15_trade_summary(snap)

        snap["_status"] = "closed"
       

        # --  18: account-after / FTMO-after (no agent change   re-read at close) --
        try:
            close_uid = str(
                snap.get("uid")
                or snap.get("user_id")
                or snap.get("owner_uid")
                or ""
            ).strip()

            snap["ftmo_after"] = read_ftmo_state_at_ack(
                close_uid,
                snap.get("profile_id"),
            )

            snap["account_after"] = read_account_at_ack(
                snap.get("device_id"),
                str(snap.get("account_type") or "demo"),
            )
        except Exception as _e:
            log.warning("analytics: close-side enrichment failed: %s", _e)

        
        # Recalculate news overlap using server/UTC-domain timestamps.
        _apply_news_during_trade(snap)
        snap["holding_minutes"] = _holding_minutes(snap)

        try:
            broker_open_ms = int(
                snap.get("broker_open_time_ms")
                or 0
            )

            broker_close_ms = int(
                snap.get("broker_close_time_ms")
                or 0
            )

            if (
                broker_open_ms > 0
                and broker_close_ms > broker_open_ms
            ):
                snap["broker_holding_minutes"] = int(
                    round(
                        (
                            broker_close_ms
                            - broker_open_ms
                        )
                        / 60000.0
                    )
                )
            else:
                snap["broker_holding_minutes"] = None

        except Exception:
            snap["broker_holding_minutes"] = None
        # --  18: trade classification from data on hand --
        try:
            _apply_realized_r_net_and_outcome(snap)
            rr = _safe_float(snap.get("realized_r"))
            if rr is None:
                rr = _safe_float(snap.get("realized_r_net"))
            # bars_held (H1 bars between entry and close)
            try:
                hm = snap.get("holding_minutes")
                snap["bars_held"] = int(hm // 60) if isinstance(hm, (int, float)) else None
            except Exception:
                snap["bars_held"] = None
            # efficiency: realized_r / mfe_r  (how much of the favourable move was captured)
            try:
                mfe = snap.get("mfe_r")
                # Only meaningful for winners with a non-trivial favourable excursion.
                # -1.01R / 0.05R = -20.22 is arithmetically true and analytically poison.
                snap["efficiency"] = (round(rr / mfe, 2)
                                      if (rr is not None and rr > 0 and mfe and mfe >= 0.1)
                                      else None)
            except Exception:
                snap["efficiency"] = None
            # heat: how close to the stop it ran (|mae_r|, 1.0 = touched stop)
            try:
                mae = snap.get("mae_r")
                snap["heat"] = round(abs(mae), 2) if mae is not None else None
            except Exception:
                snap["heat"] = None
        except Exception as _e:
            log.warning("analytics: classification failed: %s", _e)

        try:
            cs = (
                snap.get("capture_status")
                if isinstance(
                    snap.get("capture_status"),
                    dict,
                )
                else {}
            )

            cs["exit_snapshot_complete"] = True
            cs["broker_verified"] = bool(
                snap.get("broker_verified")
                and snap.get("exit_source")
                == "broker_deal"
                and snap.get("broker_open_time_ms")
                and snap.get("broker_close_time_ms")
                and snap.get("exit_price") is not None
                and snap.get("net_profit") is not None
            )

            snap["broker_verified"] = bool(
                cs["broker_verified"]
            )

            cs["analytics_schema_version"] = (
                SCHEMA_VERSION
            )

            snap["capture_status"] = cs

        except Exception as exc:
            log.warning(
                "analytics: capture_status finalize "
                "failed ticket=%s err=%s",
                ticket,
                exc,
            )

        # Final additive Phase-1 record.  Entry classifications read only the
        # immutable snapshot; close classifications read verified outcome/path.
        _apply_phase1_close_analytics(snap)
        
        if _append_jsonl(snap):
            R.delete(SNAP_PREFIX + ticket)

            try:
                if snap.get("broker_verified"):
                    R.srem(
                        PENDING_TRUTH_KEY,
                        ticket,
                    )
                else:
                    # Approximate rows remain repairable until broker truth
                    # successfully upgrades the permanent JSONL record.
                    R.sadd(
                        PENDING_TRUTH_KEY,
                        ticket,
                    )
            except Exception:
                pass

            return True
        return False
    except Exception as e:
        log.error("analytics: finalize_ticket %s failed: %s", ticket, e)
        return False


def reseed_pending_broker_truth() -> dict:
    """Rebuild the pending-truth set from unresolved permanent JSONL rows.

    This repairs the historical failure where approximate rows were removed
    from the Redis pending set before broker truth arrived. Safe and idempotent.
    """
    out = {"scanned": 0, "reseeded": 0, "errors": 0}
    try:
        if not os.path.exists(JSONL_PATH):
            return out
        R = from_app_R()
        tickets = set()
        with _trades_jsonl_lock():
            with open(JSONL_PATH, "r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        row = json.loads(line)
                    except Exception:
                        continue
                    if not isinstance(row, dict):
                        continue
                    out["scanned"] += 1
                    ticket = str(row.get("mt5_ticket") or "").strip()
                    if not ticket:
                        continue
                    unresolved = bool(
                        row.get("pending_broker_truth")
                        or row.get("exit_deal_timeout")
                        or not row.get("broker_verified")
                        or str(row.get("exit_source") or "").lower() != "broker_deal"
                    )
                    if unresolved:
                        tickets.add(ticket)
        if tickets:
            R.sadd(PENDING_TRUTH_KEY, *sorted(tickets))
            out["reseeded"] = len(tickets)
        return out
    except Exception as exc:
        out["errors"] += 1
        log.exception("analytics: reseed pending broker truth failed: %s", exc)
        return out


def reconcile_pending_broker_truth(fetch_h1_bars=None) -> dict:
    """Upgrade any approximated row whose broker deal has since arrived.

    Invariant: an h1_bar_approx / exit_deal_timeout row must NOT stay final once
    its broker deal exists. Runs every sweep; touches only tickets in the pending
    set (no full-file scan). Prefers repeating a repair over losing one.

    Correctness properties (from review):
      - shared lock with _append_jsonl (no lost concurrent appends)
      - atomic os.replace (no half-written file)
      - SREM only AFTER a successful replace (repair never silently lost)
      - file mode/owner preserved; temp file always cleaned up
      - resolver OWNS exit_source; we verify, never force it
      - upgrade built on a COPY; original row untouched on any reject path
      - missing rows kept pending and logged (surfaces upstream bugs)
      - H1 excursion labeled very_low/low, and only when actually computed
      - structured self-contained failure (never propagates to the sweep)
    """
    out = {"checked": 0, "upgraded": 0, "orphans": 0, "errors": 0}
    try:
        R = from_app_R()                                          
        try:
            pend = R.smembers(PENDING_TRUTH_KEY) or set()
        except Exception:
            return out
        pend = {t.decode() if isinstance(t, (bytes, bytearray)) else str(t) for t in pend}
        if not pend:
            return out

        # which pending tickets now actually have a broker deal?
        try:
            ready = {t for t in pend if R.get(f"xtl:mt5:deal:{t}")}
        except Exception:
            return out
        if not ready:
            return out

        fetch_h1_bars = fetch_h1_bars or default_fetch_h1_bars  
        upgraded_tickets = set()
        matched_tickets  = set()

        with _trades_jsonl_lock():
            original_stat = os.stat(JSONL_PATH)                   
            tmp = None
            try:
                rows = []
                with open(JSONL_PATH, encoding="utf-8") as f:     
                    for line in f:
                        line = line.rstrip("\n")
                        if not line.strip():
                            continue
                        try:
                            r = json.loads(line)                  
                        except Exception:
                            rows.append(line)     # preserve unparseable line verbatim
                            continue

                        tk = str(r.get("mt5_ticket") or "")
                        if tk in ready:
                            matched_tickets.add(tk)
                            out["checked"] += 1

                            be = _exit_from_broker_deal(
                                tk,
                                r,
                                R,
                            )

                            if isinstance(be, dict):
                                # Build on a COPY   never mutate the real row
                                # until the broker result is validated.
                                upgraded_row = dict(r)
                                upgraded_row.update(be)

                                if (
                                    str(
                                        upgraded_row.get("exit_source")
                                        or ""
                                    ).lower()
                                    != "broker_deal"
                                ):
                                    log.error(
                                        "reconcile: resolver returned unexpected "
                                        "source ticket=%s source=%r",
                                        tk,
                                        upgraded_row.get("exit_source"),
                                    )

                                    # Preserve the original approximate row.
                                    # Ticket remains pending for a later retry.
                                    rows.append(r)
                                    continue

                                # ---------------------------------------------
                                # Broker truth upgrade is now validated.
                                # ---------------------------------------------
                                upgraded_row["broker_verified"] = True
                                upgraded_row["broker_truth_upgraded"] = True
                                upgraded_row["pending_broker_truth"] = False
                                upgraded_row["exit_deal_timeout"] = False

                                upgraded_row["holding_minutes"] = (
                                    _holding_minutes(upgraded_row)
                                )

                                try:
                                    broker_open_ms = int(
                                        upgraded_row.get(
                                            "broker_open_time_ms"
                                        )
                                        or 0
                                    )

                                    broker_close_ms = int(
                                        upgraded_row.get(
                                            "broker_close_time_ms"
                                        )
                                        or 0
                                    )

                                    if (
                                        broker_open_ms > 0
                                        and broker_close_ms
                                        > broker_open_ms
                                    ):
                                        upgraded_row[
                                            "broker_holding_minutes"
                                        ] = int(
                                            round(
                                                (
                                                    broker_close_ms
                                                    - broker_open_ms
                                                )
                                                / 60000.0
                                            )
                                        )
                                    else:
                                        upgraded_row[
                                            "broker_holding_minutes"
                                        ] = None

                                except Exception:
                                    upgraded_row[
                                        "broker_holding_minutes"
                                    ] = None

                                capture_status = (
                                    upgraded_row.get("capture_status")
                                    if isinstance(
                                        upgraded_row.get(
                                            "capture_status"
                                        ),
                                        dict,
                                    )
                                    else {}
                                )

                                capture_status[
                                    "exit_snapshot_complete"
                                ] = True

                                capture_status[
                                    "broker_verified"
                                ] = bool(
                                    upgraded_row.get("exit_source")
                                    == "broker_deal"
                                    and upgraded_row.get(
                                        "broker_open_time_ms"
                                    )
                                    and upgraded_row.get(
                                        "broker_close_time_ms"
                                    )
                                    and upgraded_row.get(
                                        "exit_price"
                                    ) is not None
                                    and upgraded_row.get(
                                        "net_profit"
                                    ) is not None
                                )

                                capture_status[
                                    "analytics_schema_version"
                                ] = SCHEMA_VERSION

                                upgraded_row[
                                    "broker_verified"
                                ] = bool(
                                    capture_status[
                                        "broker_verified"
                                    ]
                                )

                                upgraded_row[
                                    "capture_status"
                                ] = capture_status

                                # Reconciliation must rebuild derived truth, not
                                # only copy broker fields.
                                _apply_realized_r_net_and_outcome(upgraded_row)

                                _apply_news_during_trade(
                                    upgraded_row
                                )

                                # P0 permanent fix: broker truth changes the
                                # lifetime/offset domain, so every pre-upgrade
                                # excursion field is stale by definition. Clear
                                # and rebuild it from the upgraded row before the
                                # permanent JSONL replacement.
                                try:
                                    _recompute_excursion(
                                        upgraded_row,
                                        fetch_h1_bars=fetch_h1_bars,
                                    )
                                except Exception as _exc_err:
                                    log.warning(
                                        "reconcile: excursion rebuild failed "
                                        "ticket=%s err=%s",
                                        tk,
                                        _exc_err,
                                    )

                                # Broker truth may extend or shift the close window.
                                # Rebuild DXY M15 lifecycle through the actual broker
                                # close so an approximate-close summary cannot survive.
                                try:
                                    finalize_dxy_m15_trade_summary(upgraded_row)
                                except Exception as _dxy_err:
                                    log.warning(
                                        "reconcile: DXY M15 summary rebuild failed "
                                        "ticket=%s err=%r",
                                        tk,
                                        _dxy_err,
                                    )

                                # Broker truth changes every close-derived
                                # classification.  Rebuild these only after the
                                # authoritative deal, excursion and DXY lifetime
                                # have all been reconciled.
                                _apply_phase1_close_analytics(upgraded_row)

                                rows.append(upgraded_row)
                                upgraded_tickets.add(tk)
                                out["upgraded"] += 1

                                log.warning(
                                    "analytics: broker truth upgraded "
                                    "ticket=%s exit_reason=%s "
                                    "net_profit=%s holding_minutes=%s",
                                    tk,
                                    upgraded_row.get("exit_reason"),
                                    upgraded_row.get("net_profit"),
                                    upgraded_row.get(
                                        "broker_holding_minutes"
                                    ),
                                )

                                # Exactly one row per ticket: keep the upgraded
                                # copy and do not append the old approximate row.
                                continue

                            else:
                                # Deal key exists, but resolver could not yet
                                # produce valid broker truth. Preserve original
                                # row once and keep the ticket pending.
                                log.warning(
                                    "reconcile: broker deal unresolved "
                                    "ticket=%s",
                                    tk,
                                )
                                

                                # Keep the unresolved approximate row honest.
                                # The canonical helper clears stale values and
                                # returns None rather than false zeroes when no
                                # complete H1 candle is measurable.
                                try:
                                    _recompute_excursion(
                                        r,
                                        fetch_h1_bars=fetch_h1_bars,
                                    )
                                except Exception as _ee:
                                    log.warning(
                                        "reconcile: excursion redo failed "
                                        "ticket=%s err=%s",
                                        tk,
                                        _ee,
                                    )

                        rows.append(r)

                # write to temp, preserve mode/owner, atomic replace
                fd, tmp = tempfile.mkstemp(
                    dir=os.path.dirname(JSONL_PATH),               
                    prefix=".trades_", suffix=".tmp")
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    for r in rows:
                        f.write((r if isinstance(r, str)
                                 else json.dumps(r, default=str, separators=(",", ":"))) + "\n")
                    f.flush()
                    os.fsync(f.fileno())

                os.chmod(tmp, stat.S_IMODE(original_stat.st_mode))
                try:
                    os.chown(tmp, original_stat.st_uid, original_stat.st_gid)
                except PermissionError:
                    pass

                os.replace(tmp, JSONL_PATH)                        
                tmp = None
            finally:
                if tmp and os.path.exists(tmp):
                    try:
                        os.unlink(tmp)
                    except Exception:
                        pass

        # -- file is durably written; NOW clear pending (only the upgraded ones) --
        if upgraded_tickets:
            try:
                R.srem(PENDING_TRUTH_KEY, *sorted(upgraded_tickets))
            except Exception as exc:
                log.warning(                                       
                    "reconcile: JSONL upgraded but pending cleanup failed "
                    "tickets=%s err=%s", sorted(upgraded_tickets), exc)

        # deal exists but no JSONL row   a real upstream bug. Surface, keep pending.
        orphans = ready - matched_tickets
        for tk in sorted(orphans):
            log.error(                                             
                "reconcile: pending broker truth has no JSONL row ticket=%s", tk)

        out["orphans"]  = len(orphans)
        out["upgraded"] = len(upgraded_tickets)
        return out

    except Exception as exc:
        out["errors"] += 1
        log.exception(                                             
            "analytics: reconcile_pending_broker_truth failed: %s", exc)
        return out

def _decode_json_value(raw):
    """Safely decode Redis JSON bytes/strings."""
    if raw is None:
        return None

    try:
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", "ignore")
        if isinstance(raw, str):
            return json.loads(raw)
        if isinstance(raw, dict):
            return raw
    except Exception:
        return None

    return None


def _ticket_matches(record: dict, ticket: str) -> bool:
    if not isinstance(record, dict):
        return False

    wanted = str(ticket or "").strip()
    if not wanted:
        return False

    candidates = (
        record.get("mt5_ticket"),
        record.get("ticket"),
        record.get("position_ticket"),
        record.get("position_id"),
        record.get("broker_ticket"),
    )

    return any(
        str(value or "").strip() == wanted
        for value in candidates
    )


def _uid_from_ledger_key(key) -> str:
    """
    Extract UID from:
      xtl:strategy:oppt:open:{uid}
      xtl:strategy:oppt:closed:{uid}
    """
    try:
        if isinstance(key, (bytes, bytearray)):
            key = key.decode("utf-8", "ignore")

        key_s = str(key or "")
        return key_s.rsplit(":", 1)[-1].strip()
    except Exception:
        return ""


def _recover_analytics_uid(
    R,
    ticket: str,
    snap: dict | None = None,
) -> tuple[str, str]:
    """
    Recover analytics ownership only from authoritative UID-scoped records.

    Returns:
        (uid, recovery_source)
    """
    snap = snap if isinstance(snap, dict) else {}
    ticket_s = str(ticket or "").strip()

    if not ticket_s:
        return "", "NO_TICKET"

    # ---------------------------------------------------------
    # 1. Open strategy ledger   strongest ownership source
    # ---------------------------------------------------------
    try:
        for ledger_key in R.scan_iter("xtl:strategy:oppt:open:*"):
            owner_uid = _uid_from_ledger_key(ledger_key)
            if not owner_uid:
                continue

            try:
                rows = R.hvals(ledger_key) or []
            except Exception:
                rows = []

            for raw_row in rows:
                row = _decode_json_value(raw_row)
                if _ticket_matches(row, ticket_s):
                    return owner_uid, "OPEN_LEDGER"
    except Exception as exc:
        log.warning(
            "analytics: UID recovery open-ledger failed "
            "ticket=%s err=%s",
            ticket_s,
            exc,
        )

    # ---------------------------------------------------------
    # 2. Closed strategy ledger
    # ---------------------------------------------------------
    try:
        for ledger_key in R.scan_iter("xtl:strategy:oppt:closed:*"):
            owner_uid = _uid_from_ledger_key(ledger_key)
            if not owner_uid:
                continue

            try:
                rows = R.lrange(ledger_key, 0, -1) or []
            except Exception:
                rows = []

            for raw_row in rows:
                row = _decode_json_value(raw_row)
                if _ticket_matches(row, ticket_s):
                    return owner_uid, "CLOSED_LEDGER"
    except Exception as exc:
        log.warning(
            "analytics: UID recovery closed-ledger failed "
            "ticket=%s err=%s",
            ticket_s,
            exc,
        )

    # ---------------------------------------------------------
    # 3. UID-scoped watch records
    #
    # New key shape:
    # xtl:zone:watch:{uid}:{symbol}:{side}:H1
    # ---------------------------------------------------------
    try:
        trade_id = str(snap.get("trade_id") or "").strip()

        for watch_key in R.scan_iter("xtl:zone:watch:*"):
            try:
                if isinstance(watch_key, (bytes, bytearray)):
                    watch_key_s = watch_key.decode("utf-8", "ignore")
                else:
                    watch_key_s = str(watch_key)

                parts = watch_key_s.split(":")

                # Require UID-scoped format:
                # xtl zone watch UID SYMBOL SIDE TF
                if len(parts) < 7:
                    continue

                owner_uid = str(parts[3] or "").strip()
                if not owner_uid:
                    continue

                watch = _decode_json_value(R.get(watch_key))
                if not isinstance(watch, dict):
                    continue

                if _ticket_matches(watch, ticket_s):
                    return owner_uid, "WATCH_TICKET"

                watch_trade_id = str(
                    watch.get("trade_id")
                    or watch.get("open_trade_id")
                    or watch.get("entry_trade_id")
                    or ""
                ).strip()

                if trade_id and watch_trade_id == trade_id:
                    return owner_uid, "WATCH_TRADE_ID"

            except Exception:
                continue
    except Exception as exc:
        log.warning(
            "analytics: UID recovery watch scan failed "
            "ticket=%s err=%s",
            ticket_s,
            exc,
        )

    return "", "NOT_FOUND"




def _load_broker_position_for_snapshot(
    R,
    snap: dict,
) -> dict | None:
    """
    Find the exact live broker position for an analytics snapshot.

    Priority:
      1. Frozen entry device/account.
      2. Fallback across live MT5 position snapshots when the Agent device ID
         changed after reinstall/re-registration.

    A fallback is accepted only when exactly one live ticket match exists.
    """
    try:
        ticket = str(
            snap.get("mt5_ticket")
            or ""
        ).strip()

        device_id = str(
            snap.get("device_id")
            or ""
        ).strip()

        account_type = str(
            snap.get("mt5_account")
            or snap.get("account_type")
            or "demo"
        ).strip().lower()

        if not ticket:
            return None

        def _positions_from_key(
            key: str,
        ) -> list:
            raw = R.get(key)
            positions = _json_load(
                raw,
                [],
            )

            if isinstance(positions, dict):
                positions = (
                    positions.get("positions")
                    or positions.get("rows")
                    or positions.get("items")
                    or []
                )

            return (
                positions
                if isinstance(positions, list)
                else []
            )

        def _find_ticket(
            positions: list,
        ) -> dict | None:
            for position in positions:
                if not isinstance(
                    position,
                    dict,
                ):
                    continue

                position_ticket = str(
                    position.get("ticket")
                    or position.get(
                        "position_ticket"
                    )
                    or position.get(
                        "mt5_ticket"
                    )
                    or ""
                ).strip()

                if position_ticket == ticket:
                    return dict(position)

            return None

        # -------------------------------------------------
        # Primary: exact frozen device/account.
        # -------------------------------------------------
        if device_id:
            exact_key = (
                f"xtl:mt5:pos:"
                f"{device_id}:{account_type}"
            )

            exact_position = _find_ticket(
                _positions_from_key(
                    exact_key
                )
            )

            if exact_position:
                exact_position[
                    "_analytics_position_key"
                ] = exact_key

                exact_position[
                    "_analytics_device_resolution"
                ] = "ENTRY_DEVICE"

                return exact_position

        # -------------------------------------------------
        # Fallback:
        # Agent reinstall/re-registration may change the
        # device ID while the same broker position remains
        # open. Search current broker snapshots by ticket.
        #
        # This runs only after the exact lookup fails.
        # -------------------------------------------------
        matches = []

        for raw_key in R.scan_iter(
            "xtl:mt5:pos:*"
        ):
            key = (
                raw_key.decode(
                    "utf-8",
                    "ignore",
                )
                if isinstance(
                    raw_key,
                    (bytes, bytearray),
                )
                else str(raw_key)
            )

            # Prefer the same account type.
            if not key.endswith(
                f":{account_type}"
            ):
                continue

            position = _find_ticket(
                _positions_from_key(key)
            )

            if not position:
                continue

            key_parts = key.split(":")

            resolved_device_id = (
                key_parts[-2]
                if len(key_parts) >= 2
                else ""
            )

            matches.append({
                "position": position,
                "key": key,
                "device_id": (
                    resolved_device_id
                ),
            })

        # Ticket must resolve unambiguously.
        if len(matches) != 1:
            if len(matches) > 1:
                log.error(
                    "analytics: broker position "
                    "ambiguous ticket=%s "
                    "entry_device=%s matches=%s",
                    ticket,
                    device_id,
                    [
                        item.get("key")
                        for item in matches
                    ],
                )

            return None

        selected = matches[0]

        position = dict(
            selected["position"]
        )

        resolved_device_id = str(
            selected.get("device_id")
            or ""
        ).strip()

        position[
            "_analytics_position_key"
        ] = selected["key"]

        position[
            "_analytics_device_resolution"
        ] = "TICKET_FALLBACK"

        position[
            "_analytics_original_device_id"
        ] = device_id or None

        position[
            "_analytics_resolved_device_id"
        ] = resolved_device_id or None

        # Repair the in-memory snapshot. The caller persists
        # this snapshot when update_open_trade_snapshot()
        # returns True.
        if (
            resolved_device_id
            and resolved_device_id
            != device_id
        ):
            snap[
                "entry_device_id"
            ] = (
                snap.get("entry_device_id")
                or device_id
                or None
            )

            snap[
                "device_id"
            ] = resolved_device_id

            snap[
                "analytics_device_rebound"
            ] = True

            snap[
                "analytics_device_rebound_at_ms"
            ] = _now_ms()

            snap[
                "analytics_device_rebound_reason"
            ] = (
                "LIVE_TICKET_FOUND_ON_NEW_DEVICE"
            )

            log.warning(
                "analytics: live position device "
                "rebound ticket=%s "
                "old_device=%s new_device=%s "
                "key=%s",
                ticket,
                device_id,
                resolved_device_id,
                selected["key"],
            )

        return position

    except Exception as exc:
        log.warning(
            "analytics: broker position lookup "
            "failed ticket=%s err=%r",
            (
                snap.get("mt5_ticket")
                if isinstance(snap, dict)
                else None
            ),
            exc,
        )
        return None


def _compact_live_dxy_snapshot(snap: dict, captured_ms: int) -> dict:
    """Capture a compact current DXY view using the existing canonical/fallback reader."""
    try:
        full = read_dxy_m15_at_entry(
            device_id=snap.get("device_id"),
            symbol=str(snap.get("symbol") or "").upper().strip(),
            side=str(snap.get("side") or "").upper().strip(),
            entry_ms=int(captured_ms),
            trade_firm=snap.get("prop_firm"),
            trade_profile_id=snap.get("profile_id"),
        )
        selected = full.get("selected") if isinstance(full, dict) else {}
        if not isinstance(selected, dict):
            selected = {}

        return {
            "captured_at_ms": int(captured_ms),
            "selected_source": full.get("selected_source") if isinstance(full, dict) else None,
            "selected_device_id": full.get("selected_device_id") if isinstance(full, dict) else None,
            "fallback_used": bool(full.get("fallback_used")) if isinstance(full, dict) else False,
            "available": bool(selected.get("available")),
            "fresh": bool(selected.get("fresh")),
            "status": selected.get("status"),
            "direction": selected.get("direction"),
            "trade_alignment": selected.get("trade_alignment"),
            "confidence": selected.get("confidence"),
            "directional_move_atr": selected.get("directional_move_atr"),
            "max_favorable_atr": selected.get("max_favorable_atr"),
            "max_adverse_atr": selected.get("max_adverse_atr"),
            "last_event_status": selected.get("last_event_status"),
            "last_event_reason": selected.get("last_event_reason"),
            "last_event_ms": selected.get("last_event_ms"),
            "state_age_ms": selected.get("state_age_ms"),
            "reasoning": selected.get("reasoning"),
            "unavailable_reason": selected.get("unavailable_reason"),
            "shadow_only": True,
        }
    except Exception as exc:
        return {
            "captured_at_ms": int(captured_ms),
            "available": False,
            "fresh": False,
            "capture_error": f"{type(exc).__name__}:{exc}",
            "shadow_only": True,
        }


def _dxy_milestone_signature(dxy: dict) -> str:
    if not isinstance(dxy, dict):
        return ""
    fields = (
        "selected_source",
        "available",
        "fresh",
        "status",
        "direction",
        "trade_alignment",
        "confidence",
        "last_event_status",
        "last_event_reason",
    )
    return "|".join(str(dxy.get(field) or "") for field in fields)


def _append_exit_candidate(
    history: dict,
    *,
    key: str,
    ts_ms: int,
    price: float,
    current_r: float,
    reasons: list[str],
    dxy: dict | None,
) -> None:
    keys = history.setdefault("exit_candidate_keys", [])
    if key in keys:
        return

    rows = history.setdefault("exit_candidates", [])
    rows.append({
        "ts_ms": int(ts_ms),
        "price": round(float(price), 8),
        "trade_r": round(float(current_r), 4),
        "candidate_action": "CONSIDER_EXIT",
        "reasons": list(reasons),
        "dxy_snapshot": dxy if isinstance(dxy, dict) else None,
        "analytics_only": True,
        "order_modified": False,
        "sl_modified": False,
        "tp_modified": False,
        "trade_closed": False,
    })
    keys.append(key)

    if len(rows) > MILESTONE_MAX_EXIT_CANDIDATES:
        del rows[:-MILESTONE_MAX_EXIT_CANDIDATES]
    if len(keys) > MILESTONE_MAX_EXIT_CANDIDATES:
        del keys[:-MILESTONE_MAX_EXIT_CANDIDATES]


def _update_live_m1_excursion_accumulator(
    snap: dict,
    broker_position: dict,
    *,
    now_ms: int,
) -> bool:
    """
    Persist compact M1/live-price excursion evidence for one active trade.

    Analytics-only:
      - does not modify orders
      - does not modify SL/TP
      - does not affect gates or execution

    M1 candles provide authoritative intraminute high/low.
    Broker price_current supplements the currently forming minute.

    MT5 rates.time stored in Redis is Unix epoch UTC seconds, so no broker
    timezone offset is applied here.
    """
    try:
        if (
            not isinstance(snap, dict)
            or not isinstance(broker_position, dict)
        ):
            return False

        symbol = str(
            snap.get("symbol")
            or broker_position.get("symbol")
            or ""
        ).upper().strip()

        device_id = str(
            _resolve_device(snap)
            or snap.get("device_id")
            or ""
        ).strip()

        if not symbol or not device_id:
            return False

        entry_price = _safe_float(
            snap.get("entry_price")
        )

        initial_sl = _safe_float(
            snap.get("sl_price")
        )

        live_price = _safe_float(
            broker_position.get("price_current")
            or broker_position.get("current_price")
            or broker_position.get("price")
        )

        entry_utc_ms = _norm_ms(
            snap.get("broker_open_time_utc_ms")
            or snap.get("enqueue_timestamp")
            or snap.get("opened_at_ms")
            or 0
        )

        if (
            entry_price is None
            or initial_sl is None
            or entry_utc_ms <= 0
        ):
            return False

        initial_risk = abs(
            float(entry_price)
            - float(initial_sl)
        )

        if initial_risk <= 0:
            return False

        R = from_app_R()

        m1_key = (
            f"xtl:ohlc:snap:"
            f"{device_id}:{symbol}:M1"
        )

        raw = R.get(m1_key)

        payload = {}

        if raw:
            if isinstance(
                raw,
                (bytes, bytearray),
            ):
                raw = raw.decode(
                    "utf-8",
                    "ignore",
                )

            payload = json.loads(raw)

            if isinstance(payload, str):
                payload = json.loads(payload)

            if not isinstance(payload, dict):
                payload = {}

        bars = payload.get("bars") or []

        if not isinstance(bars, list):
            bars = []

        accumulator = snap.get(
            "excursion_live"
        )

        if not isinstance(
            accumulator,
            dict,
        ):
            accumulator = {
                "version": 1,
                "coverage_schema_version": 2,
                "source": (
                    "LIVE_M1_PLUS_BROKER_PRICE"
                ),
                "analytics_only": True,

                "symbol": symbol,
                "device_id": device_id,
                "m1_key": m1_key,

                "entry_price": round(
                    float(entry_price),
                    8,
                ),
                "initial_sl": round(
                    float(initial_sl),
                    8,
                ),
                "initial_risk_price": round(
                    float(initial_risk),
                    8,
                ),
                "initial_sl_source": (
                    "ENTRY_SNAPSHOT"
                ),

                "entry_utc_ms": int(
                    entry_utc_ms
                ),
                "entry_minute_open_ms": int(
                    (
                        entry_utc_ms
                        // 60_000
                    )
                    * 60_000
                ),
                "boundary_precision": (
                    "M1_OHLC_PARTIAL_BOUNDARIES"
                ),
                "boundary_extrema_exact": False,


                "highest_high": round(float(entry_price), 8),
                "highest_high_ts_ms": int(entry_utc_ms),
                "highest_high_source": "ENTRY_BASELINE",

                "lowest_low": round(float(entry_price), 8),
                "lowest_low_ts_ms": int(entry_utc_ms),
                "lowest_low_source": "ENTRY_BASELINE",

                "first_m1_open_ms": None,
                "last_m1_open_ms": None,
                "observed_m1_bars": 0,
                "expected_m1_bars_so_far": 0,
                "coverage_pct_so_far": 0.0,

                "coverage_quality_note": (
                    "Lifetime coverage includes rolling-history truncation. "
                    "internal_gap_count counts missing candles only after "
                    "accumulation begins. Entry/exit boundary candles may "
                    "contain pre-entry or post-exit movement."
                ),

                # Candles older than the available rolling M1 snapshot.
                "history_truncated_bars": 0,

                # Missing candles inside the period actively observed by XTL.
                "internal_gap_count": 0,

                # Compatibility alias. Keep this equal to internal_gap_count.
                "gap_count": 0,

                "first_bar_partial": bool(
                    entry_utc_ms % 60_000
                ),
                "last_bar_partial": True,

                "live_price_samples": 0,
                "last_live_price": None,
                "last_live_price_ms": None,

                "m1_snapshot_server_received_ms": None,
                "m1_snapshot_last_closed_ms": None,

                "started_at_ms": int(now_ms),
                "last_updated_ms": None,
            }

        changed = False

        previous_last_m1_ms = _safe_int(
            accumulator.get(
                "last_m1_open_ms"
            ),
            0,
        ) or 0

        highest_high = _safe_float(
            accumulator.get(
                "highest_high"
            )
        )

        lowest_low = _safe_float(
            accumulator.get(
                "lowest_low"
            )
        )

        # Migrate older active accumulators to the exact entry baseline.
        if highest_high is None or highest_high < float(entry_price):
            highest_high = float(entry_price)
            accumulator["highest_high"] = round(highest_high, 8)
            accumulator["highest_high_ts_ms"] = int(entry_utc_ms)
            accumulator["highest_high_source"] = "ENTRY_BASELINE"
            changed = True

        if lowest_low is None or lowest_low > float(entry_price):
            lowest_low = float(entry_price)
            accumulator["lowest_low"] = round(lowest_low, 8)
            accumulator["lowest_low_ts_ms"] = int(entry_utc_ms)
            accumulator["lowest_low_source"] = "ENTRY_BASELINE"
            changed = True

        observed = int(
            _safe_int(
                accumulator.get(
                    "observed_m1_bars"
                ),
                0,
            )
            or 0
        )

        # -------------------------------------------------
        # Load the new split coverage fields.
        #
        # Older live accumulators stored both rolling-history
        # truncation and genuine internal gaps in one field:
        #
        #     gap_count
        #
        # Migrate that legacy value deterministically using
        # the distance from the entry minute to the first M1
        # candle that XTL actually observed.
        # -------------------------------------------------
        coverage_schema_version = int(
            _safe_int(
                accumulator.get(
                    "coverage_schema_version"
                ),
                0,
            )
            or 0
        )

        if coverage_schema_version >= 2:
            internal_gap_count = int(
                _safe_int(
                    accumulator.get(
                        "internal_gap_count"
                    ),
                    0,
                )
                or 0
            )

            history_truncated_bars = int(
                _safe_int(
                    accumulator.get(
                        "history_truncated_bars"
                    ),
                    0,
                )
                or 0
            )

        else:
            # Older accumulators may already contain the new field
            # names because an intermediate deployment copied the
            # legacy combined gap_count into internal_gap_count.
            legacy_gap_count = int(
                _safe_int(
                    accumulator.get(
                        "legacy_gap_count_before_migration"
                    ),
                    accumulator.get(
                        "internal_gap_count"
                    )
                    or accumulator.get(
                        "gap_count"
                    )
                    or 0,
                )
                or 0
            )

            entry_minute_for_migration = int(
                accumulator.get(
                    "entry_minute_open_ms"
                )
                or (
                    entry_utc_ms // 60_000
                ) * 60_000
            )

            first_m1_for_migration = int(
                _safe_int(
                    accumulator.get(
                        "first_m1_open_ms"
                    ),
                    0,
                )
                or 0
            )

            initial_unobserved_bars = 0

            if (
                first_m1_for_migration > 0
                and first_m1_for_migration
                > entry_minute_for_migration
            ):
                initial_unobserved_bars = int(
                    (
                        first_m1_for_migration
                        - entry_minute_for_migration
                    )
                    // 60_000
                )

            history_truncated_bars = min(
                legacy_gap_count,
                max(
                    0,
                    initial_unobserved_bars,
                ),
            )

            internal_gap_count = max(
                0,
                legacy_gap_count
                - history_truncated_bars,
            )

            accumulator[
                "coverage_schema_version"
            ] = 2

            accumulator[
                "coverage_schema_migrated"
            ] = True

            accumulator[
                "coverage_schema_migrated_at_ms"
            ] = int(now_ms)

            accumulator[
                "legacy_gap_count_before_migration"
            ] = legacy_gap_count

            accumulator[
                "history_truncated_bars"
            ] = history_truncated_bars

            accumulator[
                "internal_gap_count"
            ] = internal_gap_count

            accumulator[
                "gap_count"
            ] = internal_gap_count

            changed = True

        normalized_new_bars = []

        for bar in bars:
            if (
                not isinstance(bar, dict)
                or bar.get("complete") is False
            ):
                continue

            bar_open_ms = _norm_ms(
                bar.get("t_open_ms")
                or bar.get("t")
                or bar.get("time")
                or 0
            )

            if bar_open_ms <= 0:
                continue

            bar_close_ms = (
                bar_open_ms + 60_000
            )

            # Ignore candles ending before or exactly at entry.
            if bar_close_ms <= entry_utc_ms:
                continue

            # Process only bars not already persisted.
            if (
                previous_last_m1_ms > 0
                and bar_open_ms
                <= previous_last_m1_ms
            ):
                continue

            high = _safe_float(
                bar.get("h")
            )

            low = _safe_float(
                bar.get("l")
            )

            if (
                high is None
                or low is None
                or high <= 0
                or low <= 0
                or low > high
            ):
                continue

            normalized_new_bars.append({
                "open_ms": int(
                    bar_open_ms
                ),
                "high": float(high),
                "low": float(low),
            })

        normalized_new_bars.sort(
            key=lambda row: row["open_ms"]
        )

        for row in normalized_new_bars:
            bar_open_ms = int(
                row["open_ms"]
            )

            high = float(
                row["high"]
            )

            low = float(
                row["low"]
            )

            last_known_open_ms = _safe_int(
                accumulator.get(
                    "last_m1_open_ms"
                ),
                0,
            ) or 0

            if last_known_open_ms > 0:
                minute_distance = int(
                    (
                        bar_open_ms
                        - last_known_open_ms
                    )
                    // 60_000
                )

                if minute_distance > 1:
                    internal_gap_count += (
                        minute_distance - 1
                    )

            elif accumulator.get(
                "first_m1_open_ms"
            ) is None:
                entry_minute_ms = int(
                    accumulator[
                        "entry_minute_open_ms"
                    ]
                )

                initial_distance = int(
                    (
                        bar_open_ms
                        - entry_minute_ms
                    )
                    // 60_000
                )

                # These candles are not internal data gaps. They are older
                # than the currently retained rolling M1 history.
                if initial_distance > 0:
                    history_truncated_bars = max(
                        history_truncated_bars,
                        initial_distance,
                    )

                accumulator[
                    "first_m1_open_ms"
                ] = bar_open_ms
            observed += 1

            accumulator[
                "last_m1_open_ms"
            ] = bar_open_ms

            if (
                highest_high is None
                or high > highest_high
            ):
                highest_high = high

                accumulator[
                    "highest_high"
                ] = round(
                    high,
                    8,
                )

                accumulator[
                    "highest_high_ts_ms"
                ] = bar_open_ms

                accumulator[
                    "highest_high_source"
                ] = "COMPLETED_M1_HIGH"

            if (
                lowest_low is None
                or low < lowest_low
            ):
                lowest_low = low

                accumulator[
                    "lowest_low"
                ] = round(
                    low,
                    8,
                )

                accumulator[
                    "lowest_low_ts_ms"
                ] = bar_open_ms

                accumulator[
                    "lowest_low_source"
                ] = "COMPLETED_M1_LOW"

            changed = True

        accumulator[
            "observed_m1_bars"
        ] = observed

        accumulator[
            "history_truncated_bars"
        ] = history_truncated_bars

        accumulator[
            "internal_gap_count"
        ] = internal_gap_count

        # Compatibility alias for existing readers.
        accumulator[
            "gap_count"
        ] = internal_gap_count

        # -------------------------------------------------
        # Supplement the forming M1 candle using broker
        # position price. This reduces history dependency,
        # but does not replace completed M1 high/low.
        # -------------------------------------------------
        if live_price is not None:
            live_price_f = float(
                live_price
            )

            accumulator[
                "live_price_samples"
            ] = int(
                _safe_int(
                    accumulator.get(
                        "live_price_samples"
                    ),
                    0,
                )
                or 0
            ) + 1

            accumulator[
                "last_live_price"
            ] = round(
                live_price_f,
                8,
            )

            accumulator[
                "last_live_price_ms"
            ] = int(now_ms)

            if (
                highest_high is None
                or live_price_f
                > highest_high
            ):
                highest_high = live_price_f

                accumulator[
                    "highest_high"
                ] = round(
                    live_price_f,
                    8,
                )

                accumulator[
                    "highest_high_ts_ms"
                ] = int(now_ms)

                accumulator[
                    "highest_high_source"
                ] = (
                    "BROKER_LIVE_PRICE_SAMPLE"
                )

            if (
                lowest_low is None
                or live_price_f
                < lowest_low
            ):
                lowest_low = live_price_f

                accumulator[
                    "lowest_low"
                ] = round(
                    live_price_f,
                    8,
                )

                accumulator[
                    "lowest_low_ts_ms"
                ] = int(now_ms)

                accumulator[
                    "lowest_low_source"
                ] = (
                    "BROKER_LIVE_PRICE_SAMPLE"
                )

            changed = True

        latest_closed_open_ms = _safe_int(
            accumulator.get(
                "last_m1_open_ms"
            ),
            0,
        ) or 0

        entry_minute_ms = int(
            accumulator.get(
                "entry_minute_open_ms"
            )
            or (
                entry_utc_ms // 60_000
            )
            * 60_000
        )

        expected = 0

        if latest_closed_open_ms >= entry_minute_ms:
            expected = int(
                (
                    latest_closed_open_ms
                    - entry_minute_ms
                )
                // 60_000
            ) + 1

        coverage_pct = (
            round(
                min(
                    100.0,
                    observed
                    / expected
                    * 100.0,
                ),
                2,
            )
            if expected > 0
            else 0.0
        )
        retained_expected_bars = max(
            0,
            expected
            - history_truncated_bars,
        )

        retained_coverage_pct = (
            round(
                min(
                    100.0,
                    observed
                    / retained_expected_bars
                    * 100.0,
                ),
                2,
            )
            if retained_expected_bars > 0
            else 0.0
        )

        accumulator[
            "expected_m1_bars_so_far"
        ] = expected

        accumulator[
            "coverage_pct_so_far"
        ] = coverage_pct
        
        accumulator[
            "retained_expected_m1_bars"
        ] = retained_expected_bars

        accumulator[
            "retained_coverage_pct"
        ] = retained_coverage_pct

        accumulator[
            "m1_snapshot_server_received_ms"
        ] = _norm_ms(
            payload.get(
                "server_received_ms"
            )
            or payload.get(
                "written_at"
            )
            or 0
        ) or None

        accumulator[
            "m1_snapshot_last_closed_ms"
        ] = _norm_ms(
            payload.get(
                "lastClosedTs"
            )
            or 0
        ) or None

        accumulator[
            "last_updated_ms"
        ] = int(now_ms)

        accumulator[
            "last_bar_partial"
        ] = True

        # Preliminary live R values. Final close-time MFE/MAE will be
        side_u = str(
            snap.get("side")
            or ""
        ).upper().strip()

        # Preliminary live R values. Final close-time MFE/MAE will be
        # recomputed and quality-gated separately.
        if side_u == "BUY":

            if highest_high is not None:
                accumulator["live_mfe_r"] = round(
                    (
                       highest_high
                       - float(entry_price)
                    )
                    / initial_risk,
                    4,
                )

            if lowest_low is not None:
                accumulator["live_mae_r"] = round(
                    (
                       lowest_low
                       - float(entry_price)
                    )
                    / initial_risk,
                    4,
                )

        elif side_u == "SELL":

            if lowest_low is not None:
                accumulator["live_mfe_r"] = round(
                    (
                       float(entry_price)
                       - lowest_low
                    )
                    / initial_risk,
                    4,
                )

            if highest_high is not None:
                accumulator["live_mae_r"] = round(
                    (
                       float(entry_price)
                       - highest_high
                    )
                    / initial_risk,
                    4,
                )

        snap["excursion_live"] = (
            accumulator
        )

        return changed

    except Exception as exc:
        log.warning(
            "analytics: live M1 excursion "
            "update failed ticket=%s err=%r",
            (
                snap.get("mt5_ticket")
                if isinstance(snap, dict)
                else None
            ),
            exc,
        )
        return False

def update_open_trade_snapshot(
    snap: dict,
    broker_position: dict,
    *,
    now_ms: int | None = None,
) -> bool:
    """Update one open ticket's R journey and DXY milestones. Shadow-only."""
    try:
        if not isinstance(snap, dict) or not isinstance(broker_position, dict):
            return False

        now = int(now_ms or _now_ms())
        history = _ensure_trade_milestone_state(snap)
        state = history.setdefault("state", {})

        last_update = _safe_int(state.get("last_update_ms"), 0) or 0
        if last_update and now - last_update < MILESTONE_UPDATE_MIN_INTERVAL_MS:
            return False

        side = str(snap.get("side") or "").upper().strip()
        entry = _safe_float(snap.get("entry_price"))
        sl = _safe_float(snap.get("sl_price"))
        price = _safe_float(
            broker_position.get("price_current")
            or broker_position.get("current_price")
            or broker_position.get("price")
        )

        if side not in ("BUY", "SELL") or entry is None or sl is None or price is None:
            return False

        risk = abs(entry - sl)
        if risk <= 0:
            return False

        current_r = (
            (price - entry) / risk
            if side == "BUY"
            else (entry - price) / risk
        )

        state["current_r"] = round(current_r, 4)
        state["current_price"] = round(price, 8)
        state["last_update_ms"] = now
        # -------------------------------------------------
        # Analytics-only live M1 excursion accumulator.
        #
        # Completed M1 high/low is primary. The broker live
        # price supplements the currently forming minute.
        # -------------------------------------------------
        try:
            _update_live_m1_excursion_accumulator(
                snap,
                broker_position,
                now_ms=now,
            )
        except Exception as _excursion_exc:
            log.warning(
                "analytics: live excursion accumulator "
                "failed ticket=%s err=%r",
                snap.get("mt5_ticket"),
                _excursion_exc,
            )

        previous_max = _safe_float(state.get("max_r_seen"))
        previous_min = _safe_float(state.get("min_r_seen"))

        if previous_max is None or current_r > previous_max:
            state["max_r_seen"] = round(current_r, 4)
            state["max_r_seen_ms"] = now
            state["max_r_seen_price"] = round(price, 8)

        if previous_min is None or current_r < previous_min:
            state["min_r_seen"] = round(current_r, 4)
            state["min_r_seen_ms"] = now
            state["min_r_seen_price"] = round(price, 8)

        max_r = _safe_float(state.get("max_r_seen"), current_r)
        state["giveback_from_max_r"] = round(max(0.0, max_r - current_r), 4)

        milestone_rows = history.setdefault("milestones", {})
        newly_reached: list[str] = []
        for name, target in MILESTONE_LEVELS:
            row = milestone_rows.setdefault(name, {
                "target_r": target,
                "reached": False,
                "first_reached_ms": None,
                "first_reached_price": None,
                "dxy_snapshot": None,
            })
            if not bool(row.get("reached")) and current_r >= target:
                row["reached"] = True
                row["first_reached_ms"] = now
                row["first_reached_price"] = round(price, 8)
                newly_reached.append(name)
                state["highest_milestone_reached"] = name

        # Track post-milestone giveback continuously.
        for milestone_name, field_name in (
            ("r_050", "lowest_r_after_050"),
            ("r_075", "lowest_r_after_075"),
            ("r_100", "lowest_r_after_100"),
        ):
            if bool((milestone_rows.get(milestone_name) or {}).get("reached")):
                old = _safe_float(state.get(field_name))
                if old is None or current_r < old:
                    state[field_name] = round(current_r, 4)

        if bool((milestone_rows.get("r_050") or {}).get("reached")) and current_r <= 0:
            state["returned_to_entry_after_050"] = True
        if bool((milestone_rows.get("r_075") or {}).get("reached")) and current_r <= 0:
            state["returned_to_entry_after_075"] = True
        if bool((milestone_rows.get("r_100") or {}).get("reached")):
            if current_r <= 0:
                state["returned_to_entry_after_100"] = True
            if current_r < 0.50:
                state["returned_below_050_after_100"] = True

        # Capture DXY once for all milestones reached in this update, and sample
        # periodically for meaningful state changes.
        last_dxy_sample = _safe_int(history.get("dxy_last_sample_ms"), 0) or 0
        need_dxy = bool(newly_reached) or (
            not last_dxy_sample
            or now - last_dxy_sample >= MILESTONE_DXY_SAMPLE_INTERVAL_MS
        )
        dxy = None
        if need_dxy:
            dxy = _compact_live_dxy_snapshot(snap, now)
            history["dxy_last_sample_ms"] = now

            for name in newly_reached:
                milestone_rows[name]["dxy_snapshot"] = dxy

            signature = _dxy_milestone_signature(dxy)
            old_signature = str(history.get("dxy_last_signature") or "")
            if signature and signature != old_signature:
                events = history.setdefault("dxy_events", [])
                events.append({
                    "ts_ms": now,
                    "event": "DXY_STATE_CHANGED" if old_signature else "DXY_LIVE_BASELINE",
                    "trade_r": round(current_r, 4),
                    "price": round(price, 8),
                    "snapshot": dxy,
                    "shadow_only": True,
                })
                if len(events) > MILESTONE_MAX_DXY_EVENTS:
                    del events[:-MILESTONE_MAX_DXY_EVENTS]
                history["dxy_last_signature"] = signature

        # Analytics-only candidate observations. No order action is taken.
        alignment = str((dxy or {}).get("trade_alignment") or "").upper()
        status = str((dxy or {}).get("status") or "").upper()
        giveback = _safe_float(state.get("giveback_from_max_r"), 0.0) or 0.0

        if current_r >= 1.0 and alignment == "AGAINST":
            _append_exit_candidate(
                history, key="R100_DXY_AGAINST", ts_ms=now, price=price,
                current_r=current_r,
                reasons=["R_100_REACHED", "DXY_ALIGNMENT_AGAINST"],
                dxy=dxy,
            )

        if max_r >= 1.0 and giveback >= 0.50:
            _append_exit_candidate(
                history, key="AFTER_R100_GIVEBACK_050", ts_ms=now, price=price,
                current_r=current_r,
                reasons=["R_100_PREVIOUSLY_REACHED", "GIVEBACK_FROM_PEAK_GE_0_50R"],
                dxy=dxy,
            )

        if current_r >= 1.5 and status in ("REVOKED", "IDLE"):
            _append_exit_candidate(
                history, key="R150_DXY_NOT_CONFIRMED", ts_ms=now, price=price,
                current_r=current_r,
                reasons=["R_150_REACHED", "DXY_STATUS_NOT_CONFIRMED"],
                dxy=dxy,
            )

        snap["trade_milestone_history"] = history
        snap["milestone_capture_complete"] = True
        snap["milestone_last_updated_ms"] = now
        return True

    except Exception as exc:
        log.warning(
            "analytics: open milestone update failed ticket=%s err=%r",
            snap.get("mt5_ticket") if isinstance(snap, dict) else None,
            exc,
        )
        return False


def update_open_trade_snapshots() -> dict:
    """Update all in-flight analytics snapshots from exact broker positions."""
    stats = {
        "checked": 0,
        "updated": 0,
        "position_missing": 0,
        "errors": 0,
    }
    try:
        R = from_app_R()
        now = _now_ms()

        for key in R.scan_iter(SNAP_PREFIX + "*"):
            stats["checked"] += 1
            try:
                raw = R.get(key)
                snap = _json_load(raw, {})
                if not isinstance(snap, dict) or snap.get("_status") == "closed":
                    continue

                broker_position = _load_broker_position_for_snapshot(R, snap)
                if not broker_position:
                    stats["position_missing"] += 1
                    continue

                if update_open_trade_snapshot(
                    snap,
                    broker_position,
                    now_ms=now,
                ):
                    ttl = _safe_int(R.ttl(key), -1)
                    payload = json.dumps(
                        snap,
                        default=str,
                        separators=(",", ":"),
                    )
                    if ttl and ttl > 0:
                        R.set(key, payload, ex=ttl)
                    else:
                        R.set(key, payload, ex=SNAP_TTL_SEC)
                    stats["updated"] += 1

            except Exception as exc:
                stats["errors"] += 1
                log.warning(
                    "analytics: milestone sweep item failed key=%s err=%r",
                    key,
                    exc,
                )

        return stats

    except Exception as exc:
        stats["errors"] += 1
        log.warning("analytics: milestone sweep failed err=%r", exc)
        return stats


# -- CLOSE DETECTION + ORPHAN SWEEP -------------------------------------------
def sweep_closed_trades(fetch_h1_bars=None) -> dict:
    """Diff in-flight snapshots against the live open-position set; finalize any
    whose ticket is gone (and old enough). Call once per BROKER_RECON cycle."""
    fetch_h1_bars = fetch_h1_bars or default_fetch_h1_bars
    stats = {"checked": 0, "finalized": 0, "skipped_unverified": 0, "errors": 0}
    try:
        global _ANALYTICS_STARTUP_RECON_DONE
        R = from_app_R()
        now = _now_ms()

        # P0 startup recovery: rebuild any historically-evicted pending tickets
        # and consume broker deals before absence-based close detection begins.
        # Mark complete only after both passes return normally; a transient Redis
        # or file error will retry on the next reconciliation cycle.
        if not _ANALYTICS_STARTUP_RECON_DONE:
            reseed = reseed_pending_broker_truth()
            startup_rec = reconcile_pending_broker_truth(fetch_h1_bars)
            _ANALYTICS_STARTUP_RECON_DONE = True
            stats["startup_pending_reseeded"] = int(
                reseed.get("reseeded", 0) or 0
            )
            stats["startup_truth_upgraded"] = int(
                startup_rec.get("upgraded", 0) or 0
            )
            stats["startup_truth_orphans"] = int(
                startup_rec.get("orphans", 0) or 0
            )
            log.warning(
                "analytics: STARTUP_RECON_COMPLETE reseeded=%s upgraded=%s "
                "orphans=%s errors=%s",
                stats["startup_pending_reseeded"],
                stats["startup_truth_upgraded"],
                stats["startup_truth_orphans"],
                int(reseed.get("errors", 0) or 0)
                + int(startup_rec.get("errors", 0) or 0),
            )

        # Cache broker-open tickets separately for each
        # UID-owned prop profile during this sweep.
        open_tickets_cache: dict[
            tuple[str, str],
            set,
        ] = {}

        for key in R.scan_iter(SNAP_PREFIX + "*"):
            stats["checked"] += 1
            try:
                raw = R.get(key)
                if not raw:
                    continue
                snap = json.loads(raw)
                uid = str(
                    snap.get("uid")
                    or snap.get("user_id")
                    or snap.get("owner_uid")
                    or ""
                ).strip()

                profile_id = str(
                    snap.get("profile_id")
                    or snap.get("prop_profile_id")
                    or ""
                ).strip().lower()

                ticket = str(
                    snap.get("mt5_ticket")
                    or str(key).split(":")[-1]
                    or ""
                ).strip()

                # ---------------------------------------------------------
                # P0: self-heal ownerless analytics snapshots.
                # Recovery is attempted only when UID is absent.
                # ---------------------------------------------------------
                if not uid:
                    recovered_uid, recovery_source = _recover_analytics_uid(
                        R,
                        ticket,
                        snap,
                    )

                    if recovered_uid:
                        uid = recovered_uid

                        snap["uid"] = uid
                        snap["user_id"] = uid
                        snap["owner_uid"] = uid
                        snap["ownership_recovered"] = True
                        snap["ownership_recovery_source"] = recovery_source
                        snap["ownership_recovered_at_ms"] = now

                        # Preserve the existing TTL when possible.
                        try:
                            ttl = int(R.ttl(key))
                        except Exception:
                            ttl = -1

                        try:
                            payload = json.dumps(
                                snap,
                                default=str,
                                separators=(",", ":"),
                            )

                            if ttl > 0:
                                R.set(key, payload, ex=ttl)
                            else:
                                R.set(key, payload)

                            log.warning(
                                "analytics: close sweep ownership recovered "
                                "key=%s ticket=%s uid=%s profile=%s source=%s",
                                key,
                                ticket,
                                uid,
                                profile_id,
                                recovery_source,
                            )
                        except Exception as exc:
                            log.error(
                                "analytics: ownership recovered but persistence failed "
                                "key=%s ticket=%s uid=%s err=%s",
                                key,
                                ticket,
                                uid,
                                exc,
                            )
                    else:
                        log.error(
                            "analytics: close sweep ownership missing "
                            "key=%s ticket=%s uid=%r profile=%r recovery=%s",
                            key,
                            ticket,
                            uid,
                            profile_id,
                            recovery_source,
                        )
                        stats["errors"] += 1
                        continue

                # Profile ownership is still mandatory.
                if not profile_id:
                    log.error(
                        "analytics: close sweep profile missing "
                        "key=%s ticket=%s uid=%r profile=%r",
                        key,
                        ticket,
                        uid,
                        profile_id,
                    )
                    stats["errors"] += 1
                    continue

                owner_key = (
                    uid,
                    profile_id,
                )

                if owner_key not in open_tickets_cache:
                    open_tickets_cache[owner_key] = (
                        _open_tickets(
                            uid,
                            profile_id,
                        )
                    )

                open_tickets = open_tickets_cache[
                    owner_key
                ]

                if open_tickets is None:
                    stats["skipped_unverified"] += 1
                    log.warning(
                        "analytics: close sweep skipped unverified broker snapshot "
                        "ticket=%s uid=%s profile=%s",
                        ticket,
                        uid,
                        profile_id,
                    )
                    continue

                if ticket in open_tickets:
                    try:
                        if update_dxy_m15_trade_tracking(snap, until_ms=now):
                            ttl = int(R.ttl(key))
                            payload = json.dumps(
                                snap,
                                default=str,
                                separators=(",", ":"),
                            )
                            if ttl > 0:
                                R.set(key, payload, ex=ttl)
                            else:
                                R.set(key, payload, ex=SNAP_TTL_SEC)
                    except Exception as _dxy_track_exc:
                        log.warning(
                            "analytics: DXY M15 open tracking failed ticket=%s err=%r",
                            ticket,
                            _dxy_track_exc,
                        )
                    continue
                age = now - int(snap.get("enqueue_timestamp") or now)
                if age < ORPHAN_AGE_MS:
                    continue                       # too fresh; avoid just-placed race
                bars = []
                try:
                    bars = fetch_h1_bars(snap.get("symbol"),
                                         int(snap.get("enqueue_timestamp") or 0),
                                         now, _resolve_device(snap)) or []
                except TypeError:
                    bars = fetch_h1_bars(snap.get("symbol"),
                                         int(snap.get("enqueue_timestamp") or 0), now) or []
                except Exception as e:
                    log.warning("analytics: bar fetch failed for %s: %s", snap.get("symbol"), e)
                if finalize_ticket(ticket, bars):
                    stats["finalized"] += 1
            except Exception as e:
                stats["errors"] += 1
                log.error("analytics: sweep item failed: %s", e)
    except Exception as e:
        log.error("analytics: sweep_closed_trades failed: %s", e)

    try:
        rec = reconcile_pending_broker_truth(
            fetch_h1_bars
        )

        if rec.get("upgraded"):
            log.info(
                "analytics: broker-truth upgraded %d row(s)",
                rec["upgraded"],
            )

        stats["truth_upgraded"] = rec.get(
            "upgraded",
            0,
        )

        stats["truth_orphans"] = rec.get(
            "orphans",
            0,
        )

    except Exception as e:
        log.error(
            "analytics: reconcile pass failed: %s",
            e,
        )
    return stats


REJECTED_JSONL = "/opt/xauapi/api/trend/out/trades_rejected.jsonl"


def capture_rejection(candidate: dict, reason: str, gate: str = None, enrich: bool = False) -> bool:
    """Log a rejected trade candidate. Light by default (cheap context only);
    enrich=True recomputes regime/SR for meaningful gate-blocks. Never raises."""
    try:
        c = candidate or {}
        sym  = str(c.get("symbol") or "").upper()
        side = str(c.get("side") or "").upper()
        ts   = int(c.get("opened_at_ms") or c.get("ts_ms") or _now_ms())
        z = c.get("entry_zone") if isinstance(c.get("entry_zone"), dict) else {}
        rec = {
            "schema_version":  SCHEMA_VERSION,
            "timestamp":       ts,
            "session":         _session_for_ts_ms(ts, LIVE_TZ_OFFSET_H),
            "symbol":          sym,
            "side":            side,
            "rejection_reason": reason,
            "gate":            gate,
            "trade_id":        c.get("trade_id"),
            "profile_id":      c.get("profile_id"),
            "trigger_type":    c.get("trigger_type"),
            "zone_level":      _safe_float(z.get("level") if z else c.get("entry_zone_level")),
            "entry_price":     _safe_float(c.get("entry_price")),
        }
        if enrich:
            try:
                rec["regime_1h"] = (read_regime_at_ack(sym, c.get("device_id")) or {}).get("h1")
                sr = read_sr_at_ack(sym)
                rec["best_resistance"] = sr.get("best_resistance")
                rec["best_support"]    = sr.get("best_support")
            except Exception:
                pass
        os.makedirs(os.path.dirname(REJECTED_JSONL), exist_ok=True)
        with open(REJECTED_JSONL, "a") as f:
            f.write(json.dumps(rec, separators=(",", ":")) + "\n")
            f.flush()
        return True
    except Exception as e:
        log.warning("analytics: capture_rejection failed: %s", e)
        return False


def load_real_dxy_bars(
    device_id: str,
    tf: str = "H1",
    max_bars: int = 300,
) -> list:
    """
    Load completed real broker DXY bars from one exact device/timeframe.
    Never scans another device and never falls back to synthetic DXY.
    """
    dev = str(device_id or "").strip()
    tf_u = str(tf or "H1").upper().strip()

    if not dev or tf_u not in ("M15", "H1"):
        return []

    try:
        R = from_app_R()

        raw = R.get(
            f"xtl:ohlc:snap:{dev}:DXY:{tf_u}"
        )

        if not raw:
            return []

        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode(
                "utf-8",
                "ignore",
            )

        payload = json.loads(raw)

        if isinstance(payload, str):
            payload = json.loads(payload)

        bars = (
            payload.get("bars")
            if isinstance(payload, dict)
            else []
        )

        if not isinstance(bars, list):
            return []

        limit = max(
            1,
            int(max_bars or 300),
        )

        out = []
        seen = set()

        for bar in bars[-limit:]:
            if (
                not isinstance(bar, dict)
                or not bool(
                    bar.get("complete", True)
                )
            ):
                continue

            ts_ms = _norm_ms(
                bar.get("t_open_ms")
                or bar.get("t")
                or bar.get("time")
                or 0
            )

            if not ts_ms or ts_ms in seen:
                continue

            o = _safe_float(bar.get("o"))
            h = _safe_float(bar.get("h"))
            l = _safe_float(bar.get("l"))
            c = _safe_float(bar.get("c"))

            if (
                None in (o, h, l, c)
                or min(o, h, l, c) <= 0
            ):
                continue

            row = dict(bar)
            row["t"] = int(ts_ms // 1000)
            row["t_open_ms"] = int(ts_ms)
            row["complete"] = True

            out.append(row)
            seen.add(ts_ms)

        out.sort(
            key=lambda b: int(
                b.get("t_open_ms") or 0
            )
        )

        return out

    except Exception as exc:
        log.warning(
            "analytics: real DXY bars load failed "
            "device=%s tf=%s err=%r",
            device_id,
            tf_u,
            exc,
        )
        return []


def build_synthetic_dxy_bars(
    device_id: str,
    tf: str = "H1",
    max_bars: int = 300,
    min_pairs: int = 3,
) -> list:
    """
    Build broker-aligned synthetic DXY OHLC bars for H1 or M15.

    Components:
      EURUSD, GBPUSD             inverted
      USDJPY, USDCHF, USDCAD     direct

    Candles are combined only when the component bars have the exact
    same broker candle-open timestamp.
    """
    dev = str(device_id or "").strip()
    tf_u = str(tf or "H1").upper().strip()

    tf_ms_map = {
        "M15": 15 * 60 * 1000,
        "H1": 60 * 60 * 1000,
    }

    tf_ms = tf_ms_map.get(tf_u)

    if not dev or not tf_ms:
        return []

    pair_signs = {
        "EURUSD": -1,
        "GBPUSD": -1,
        "USDJPY": 1,
        "USDCHF": 1,
        "USDCAD": 1,
    }

    required = max(
        3,
        min(
            5,
            int(min_pairs or 3),
        ),
    )

    try:
        R = from_app_R()

        pair_maps = {}
        pair_bases = {}

        limit = max(
            1,
            int(max_bars or 300),
        )

        for symbol, sign in pair_signs.items():
            raw = R.get(
                f"xtl:ohlc:snap:{dev}:{symbol}:{tf_u}"
            )

            if not raw:
                continue

            if isinstance(raw, (bytes, bytearray)):
                raw = raw.decode(
                    "utf-8",
                    "ignore",
                )

            payload = json.loads(raw)

            if isinstance(payload, str):
                payload = json.loads(payload)

            bars = (
                payload.get("bars")
                if isinstance(payload, dict)
                else []
            )

            if not isinstance(bars, list):
                continue

            bar_map = {}

            for bar in bars[-limit:]:
                if (
                    not isinstance(bar, dict)
                    or not bool(
                        bar.get("complete", True)
                    )
                ):
                    continue

                ts_ms = _norm_ms(
                    bar.get("t_open_ms")
                    or bar.get("t")
                    or bar.get("time")
                    or 0
                )

                o = _safe_float(bar.get("o"))
                h = _safe_float(bar.get("h"))
                l = _safe_float(bar.get("l"))
                c = _safe_float(bar.get("c"))

                if (
                    not ts_ms
                    or None in (o, h, l, c)
                    or min(o, h, l, c) <= 0
                ):
                    continue

                bar_map[int(ts_ms)] = {
                    "o": o,
                    "h": h,
                    "l": l,
                    "c": c,
                }

            if not bar_map:
                continue

            first_ts = min(bar_map)

            first_close = _safe_float(
                bar_map[first_ts].get("c")
            )

            if (
                first_close is None
                or first_close <= 0
            ):
                continue

            pair_maps[symbol] = {
                "sign": sign,
                "bars": bar_map,
            }

            pair_bases[symbol] = first_close

        if len(pair_maps) < required:
            return []

        all_timestamps = sorted(
            {
                ts
                for rec in pair_maps.values()
                for ts in rec["bars"].keys()
            }
        )

        synthetic = []

        for ts_ms in all_timestamps:
            opens = []
            highs = []
            lows = []
            closes = []
            contributors = []

            for symbol, rec in pair_maps.items():
                bar = rec["bars"].get(ts_ms)

                if not bar:
                    continue

                base = pair_bases.get(symbol)
                sign = int(
                    rec.get("sign") or 0
                )

                if (
                    not base
                    or sign not in (-1, 1)
                ):
                    continue

                o = _safe_float(bar.get("o"))
                h = _safe_float(bar.get("h"))
                l = _safe_float(bar.get("l"))
                c = _safe_float(bar.get("c"))

                if (
                    None in (o, h, l, c)
                    or min(o, h, l, c) <= 0
                ):
                    continue

                if sign == 1:
                    so = 100.0 * o / base
                    sh = 100.0 * h / base
                    sl = 100.0 * l / base
                    sc = 100.0 * c / base

                else:
                    so = 100.0 * base / o
                    sh = 100.0 * base / l
                    sl = 100.0 * base / h
                    sc = 100.0 * base / c

                opens.append(so)
                highs.append(sh)
                lows.append(sl)
                closes.append(sc)
                contributors.append(symbol)

            if len(closes) < required:
                continue

            synth_open = (
                sum(opens) / len(opens)
            )
            synth_high = (
                sum(highs) / len(highs)
            )
            synth_low = (
                sum(lows) / len(lows)
            )
            synth_close = (
                sum(closes) / len(closes)
            )

            synth_high = max(
                synth_high,
                synth_open,
                synth_close,
            )

            synth_low = min(
                synth_low,
                synth_open,
                synth_close,
            )

            synthetic.append(
                {
                    "t": int(ts_ms // 1000),
                    "t_open_ms": int(ts_ms),
                    "t_close_ms": int(
                        ts_ms + tf_ms
                    ),
                    "o": round(
                        synth_open,
                        6,
                    ),
                    "h": round(
                        synth_high,
                        6,
                    ),
                    "l": round(
                        synth_low,
                        6,
                    ),
                    "c": round(
                        synth_close,
                        6,
                    ),
                    "complete": True,
                    "synthetic": True,
                    "synthetic_tf": tf_u,
                    "synthetic_pair_count": len(
                        contributors
                    ),
                    "synthetic_pairs": contributors,
                }
            )

        synthetic.sort(
            key=lambda bar: int(
                bar.get("t_open_ms") or 0
            )
        )

        return synthetic[-limit:]

    except Exception as exc:
        log.warning(
            "analytics: synthetic DXY build failed "
            "device=%s tf=%s err=%r",
            device_id,
            tf_u,
            exc,
        )
        return []


def build_synthetic_dxy_h1_bars_generic(
    device_id: str,
    max_bars: int = 300,
) -> list:
    """
    Generic H1 builder retained only for regression comparison.

    Production dxy_tracker.py must continue importing the original
    build_synthetic_dxy_h1_bars implementation defined earlier.
    """
    return build_synthetic_dxy_bars(
        device_id=device_id,
        tf="H1",
        max_bars=max_bars,
    )

def build_synthetic_dxy_m15_bars(
    device_id: str,
    max_bars: int = 300,
) -> list:
    return build_synthetic_dxy_bars(
        device_id=device_id,
        tf="M15",
        max_bars=max_bars,
    )

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # build_entry_snapshot   regime degrades to None (no host), drift/session computed
    clean_pos = {
        "trade_id": "WATCH:XAUUSD:SELL:H1:1782417600000", "symbol": "XAUUSD", "side": "SELL",
        "entry_price": 4008.32, "qty": 0.06, "sl_price": 4044.15, "tp_price": 3936.96,
        "opened_at_ms": 1782447439976,  # ~04:17 UTC -> Asia
        "source": "oppt", "device_id": "dev_x", "trigger_type": "WATCHLIST_REV_OK_BAR_BREAK",
        "trigger_level": 4023.75, "profile_id": "ftmo-main",
        "entry_zone": {"level": 4041.33, "low": 4034.60, "high": 4043.65, "touches": 2, "sr_score": 5.5, "tf": "H1", "kind": "resistance"},
        "prop_check": {"verdict": "OK", "firm": "ftmo", "phase": "challenge", "risk_usd": 214.4, "risk_pct": 0.858, "target_rr": 2.0, "planned_rr": 2.0},
        "mt5_ticket": 2104259779,
    }
    s = build_entry_snapshot(clean_pos)
    print("CLEAN provenance:", s["entry_provenance"], "| session:", s["session"],
          "| against_drift:", s["against_drift"], "(SELL vs", s["drift_direction"], "drift)",
          "| dist_to_zone:", s["dist_to_zone_pips"], "| regime_1h:", s["regime_1h"])

    repair_pos = {
        "trade_id": "BROKER_REPAIR:GBPUSD:SELL:2107942429", "symbol": "GBPUSD", "side": "SELL",
        "entry_price": 1.32255, "qty": 0.06, "sl_price": 1.32540, "tp_price": 1.31600,
        "opened_at_ms": 1782447439976, "source": "broker_repair", "repair_source": "broker_snapshot",
        "device_id": "dev_y", "profile_id": "ftmo-main", "mt5_ticket": 2107942429,
        "entry_zone": {"level": 1.32503, "low": 1.32370, "high": 1.32523, "touches": 14},
        "prop_check": {"verdict": "ALLOW", "source": "broker_repair", "firm": "ftmo", "phase": "challenge", "risk_usd": 251.4, "risk_pct": 0.86, "target_rr": 2.0, "planned_rr": 2.2},
    }
    r = build_entry_snapshot(repair_pos)
    print("REPAIR provenance:", r["entry_provenance"], "| trigger_type:", r["trigger_type"],
          "| prop_verdict:", r["prop_verdict"], "| entry_style:", r["entry_style"])

    print("\napproximate_exit (SELL hits SL):")
    bars = [{"t_close_ms": 1782447500000, "h": 4010, "l": 4005},
            {"t_close_ms": 1782451100000, "h": 4046, "l": 4030}]
    print(" ", approximate_exit(s, bars))
