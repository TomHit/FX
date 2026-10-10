"""
XauTrendLab Strategy B - Macro Thesis Persistence Service
=========================================================

Phase 1: observation-only.

Responsibilities
----------------
- Consume canonical MT5 event releases from the explicit Strategy-B release index.
- Exact-GET immutable release records; never Redis keyspace SCAN.
- Build Strategy-B MacroEvidence using the approved macro intelligence module.
- Read the already-selected canonical REAL_DXY device using exact Redis GET.
- Build canonical D1/H4/H1 DXY bias from existing REAL_DXY bars + bias_engine.
- Read existing REAL_DXY M15 turn/flow/structure state.
- Validate mandatory input freshness.
- Build and persist observation-only DXY macro thesis snapshots.
- Re-evaluate tracked evidence as M15 / structure / HTF bias evolves.
- Persist transition history and restart-safe indexes/cursors.

Non-responsibilities
--------------------
- NO broker/order calls.
- NO Strategy-A Redis writes.
- NO entry/exit/risk authority.
- NO SR calculation.
- NO event/news fetching.
- NO Redis keyspace SCAN.

Expected companion module
-------------------------
    api/macro_event_intelligence_strategy_b_thesis_v1.py

The companion module remains pure interpretation. This service owns Redis IO,
persistence, freshness checks, and restart-safe observation scheduling.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import time
import uuid
from typing import Any, Mapping, Optional

log = logging.getLogger("uvicorn.error")

# Process-local Strategy-B calibration store.
#
# The calibration JSONL can be very large. Rebuilding its dedupe indexes for
# every macro reevaluation batch causes the entire historical JSONL to be
# parsed repeatedly. XAUAPI currently runs one Uvicorn process and the macro
# observer is serialized by its Redis tick lock, so reuse one store for macro
# batches within this process.
#
# The store itself updates its in-memory indexes for captures written through
# this macro path. Keep initialization lazy so application startup is not
# blocked by the historical JSONL load.
_MACRO_CALIBRATION_STORE = None

SCHEMA_VERSION = 1
SERVICE_NAME = "strategy_b_macro_service"
MODE = "OBSERVATION_ONLY"

# ---------------------------------------------------------------------------
# Redis namespace
# ---------------------------------------------------------------------------

RELEASE_OWNERS_KEY = "xtl:strategy_b:macro:release_owners"
RELEASE_INDEX_PREFIX = "xtl:strategy_b:macro:release_index"
RELEASE_CURSOR_PREFIX = "xtl:strategy_b:macro:release_cursor"

# Phase-7 scheduled-event research discovery.
# Separate from the immutable MT5 result/release path above.
SCHEDULE_OWNERS_KEY = "xtl:strategy_b:macro:schedule_owners"
SCHEDULE_EVENT_PREFIX = "xtl:strategy_b:phase7:schedule_event"
SCHEDULE_EVENT_INDEX_KEY = "xtl:strategy_b:phase7:schedule_event_index"


EVIDENCE_PREFIX = "xtl:strategy_b:macro:evidence"
CONTEXT_PREFIX = "xtl:strategy_b:macro:context"
THESIS_LATEST_PREFIX = "xtl:strategy_b:macro:thesis:latest"
THESIS_HISTORY_PREFIX = "xtl:strategy_b:macro:thesis:history"
OWNER_LATEST_PREFIX = "xtl:strategy_b:macro:latest"

ACTIVE_KEY = "xtl:strategy_b:macro:active"
ACTIVE_RR_CURSOR_KEY = "xtl:strategy_b:macro:active_rr_cursor"

DEADLETTER_PREFIX = "xtl:strategy_b:macro:deadletter"
HEARTBEAT_KEY = "xtl:strategy_b:macro:heartbeat"

TICK_LOCK_KEY = "xtl:strategy_b:macro:tick_lock"

DXY_CANONICAL_CONFIG_KEY = os.getenv(
    "XTL_DXY_CANONICAL_CONFIG_KEY",
    "xtl:dxy:canonical",
)
DXY_M15_STATE_PREFIX = "xtl:dxy:turn:state:M15"
DXY_H1_STATE_PREFIX = "xtl:dxy:turn:state:H1"

# ---------------------------------------------------------------------------
# Bounds / TTLs
# ---------------------------------------------------------------------------

RELEASE_RETENTION_MS = 14 * 86_400_000
PERSIST_TTL_SEC = int(
    os.getenv("XTL_STRATEGY_B_MACRO_PERSIST_TTL_SEC", str(21 * 86_400))
)

TICK_LOCK_SEC = int(
    os.getenv("XTL_STRATEGY_B_MACRO_TICK_LOCK_SEC", "15")
)

OWNER_BATCH = int(
    os.getenv("XTL_STRATEGY_B_MACRO_OWNER_BATCH", "20")
)

RELEASE_BATCH_PER_OWNER = int(
    os.getenv("XTL_STRATEGY_B_MACRO_RELEASE_BATCH", "20")
)

SCHEDULE_EVENTS_PER_OWNER = int(
    os.getenv(
        "XTL_STRATEGY_B_SCHEDULE_EVENTS_PER_OWNER",
        "500",
    )
)

SCHEDULE_CHECKPOINT_PREFIX = (
    "xtl:strategy_b:phase7:checkpoint"
)

SCHEDULE_CHECKPOINT_ORDINALS = (1, 2, 4)

SCHEDULE_CHECKPOINT_TTL_SEC = int(
    os.getenv(
        "XTL_STRATEGY_B_SCHEDULE_CHECKPOINT_TTL_SEC",
        str(21 * 86_400),
    )
)

SCHEDULE_CHECKPOINT_LOOKBACK_MS = 90 * 60 * 1000
SCHEDULE_CHECKPOINT_LOOKAHEAD_MS = 15 * 60 * 1000

SCHEDULE_CHECKPOINT_EVENT_BATCH = int(
    os.getenv(
        "XTL_STRATEGY_B_SCHEDULE_CHECKPOINT_EVENT_BATCH",
        "50",
    )
)

SCHEDULE_LOOKAHEAD_MS = int(
    os.getenv(
        "XTL_STRATEGY_B_SCHEDULE_LOOKAHEAD_MS",
        str(8 * 86_400_000),
    )
)

SCHEDULE_RESEARCH_RETENTION_SEC = int(
    os.getenv(
        "XTL_STRATEGY_B_SCHEDULE_RESEARCH_RETENTION_SEC",
        str(21 * 86_400),
    )
)

ACTIVE_BATCH = int(
    os.getenv("XTL_STRATEGY_B_MACRO_ACTIVE_BATCH", "40")
)

MAX_HISTORY = int(
    os.getenv("XTL_STRATEGY_B_MACRO_MAX_HISTORY", "100")
)

# Existing analytics contract currently considers M15 state fresh for 90 min.
DXY_M15_FRESH_MS = int(
    os.getenv(
        "XTL_STRATEGY_B_DXY_M15_FRESH_MS",
        str(90 * 60 * 1000),
    )
)

# Allow small producer/server ordering skew only.
FUTURE_TOLERANCE_MS = int(
    os.getenv(
        "XTL_STRATEGY_B_FUTURE_TOLERANCE_MS",
        str(2 * 60 * 1000),
    )
)


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def _now_ms() -> int:
    return int(time.time() * 1000)


def _txt(value: Any) -> str:
    return str(value or "").strip()


def _upper(value: Any) -> str:
    return _txt(value).upper()


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except Exception:
        return default


def _json_load(value: Any, default: Any = None) -> Any:
    if default is None:
        default = {}

    try:
        if value is None:
            return default
        if isinstance(value, (bytes, bytearray)):
            value = value.decode("utf-8", "ignore")
        if isinstance(value, str):
            return json.loads(value)
        return value
    except Exception:
        return default


def _json_dump(value: Any) -> str:
    return json.dumps(
        value,
        separators=(",", ":"),
        sort_keys=True,
        default=str,
    )


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(
        _json_dump(value).encode("utf-8")
    ).hexdigest()


def _release_index_key(calendar_owner_id: str) -> str:
    return f"{RELEASE_INDEX_PREFIX}:{calendar_owner_id}"


def _release_cursor_key(calendar_owner_id: str) -> str:
    return f"{RELEASE_CURSOR_PREFIX}:{calendar_owner_id}"


def _release_key(calendar_owner_id: str, value_id: str) -> str:
    return (
        "xtl:news:calendar:mt5:release:"
        f"{calendar_owner_id}:{value_id}"
    )

def _schedule_key(calendar_owner_id: str) -> str:
    return (
        "xtl:news:calendar:mt5:"
        f"{calendar_owner_id}"
    )

def _schedule_checkpoint_key(
    calendar_owner_id: str,
    value_id: str,
    ordinal: int,
) -> str:
    return (
        f"{SCHEDULE_CHECKPOINT_PREFIX}:"
        f"{calendar_owner_id}:{value_id}:"
        f"POST_M15_{int(ordinal)}"
    )

def _schedule_checkpoint_baseline_key(
    calendar_owner_id: str,
    value_id: str,
) -> str:
    return (
        f"{SCHEDULE_CHECKPOINT_PREFIX}:"
        f"{calendar_owner_id}:{value_id}:"
        "EVENT_BASELINE"
    )


def _schedule_event_key(
    calendar_owner_id: str,
    value_id: str,
) -> str:
    return (
        f"{SCHEDULE_EVENT_PREFIX}:"
        f"{calendar_owner_id}:{value_id}"
    )


def _evidence_key(evidence_id: str) -> str:
    return f"{EVIDENCE_PREFIX}:{evidence_id}"


def _context_key(evidence_id: str) -> str:
    return f"{CONTEXT_PREFIX}:{evidence_id}"


def _thesis_latest_key(evidence_id: str) -> str:
    return f"{THESIS_LATEST_PREFIX}:{evidence_id}"


def _thesis_history_key(evidence_id: str) -> str:
    return f"{THESIS_HISTORY_PREFIX}:{evidence_id}"


def _owner_latest_key(calendar_owner_id: str) -> str:
    owner_id = _txt(calendar_owner_id)

    if not owner_id:
        raise ValueError(
            "strategy_b_calendar_owner_id_required"
        )

    return f"{OWNER_LATEST_PREFIX}:{owner_id}"


def _deadletter_key(calendar_owner_id: str, value_id: str) -> str:
    return f"{DEADLETTER_PREFIX}:{calendar_owner_id}:{value_id}"


def _canonical_m15_key(device_id: str) -> str:
    return (
        f"{DXY_M15_STATE_PREFIX}:REAL_DXY:"
        f"{device_id}"
    )


def _canonical_h1_key(device_id: str) -> str:
    return (
        f"{DXY_H1_STATE_PREFIX}:REAL_DXY:"
        f"{device_id}"
    )


# ---------------------------------------------------------------------------
# Lock
# ---------------------------------------------------------------------------

_UNLOCK_LUA = """
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
end
return 0
"""


def _acquire_tick_lock(R) -> Optional[str]:
    token = uuid.uuid4().hex
    try:
        ok = R.set(
            TICK_LOCK_KEY,
            token,
            nx=True,
            ex=max(5, TICK_LOCK_SEC),
        )
        return token if ok else None
    except Exception:
        return None


def _release_tick_lock(R, token: str) -> None:
    if not token:
        return
    try:
        R.eval(_UNLOCK_LUA, 1, TICK_LOCK_KEY, token)
    except Exception:
        # TTL is the final lock-release authority.
        pass


# ---------------------------------------------------------------------------
# Cursor-safe release-index consumption
# ---------------------------------------------------------------------------

def _load_release_cursor(R, calendar_owner_id: str) -> dict:
    raw = R.get(_release_cursor_key(calendar_owner_id))
    row = _json_load(raw, {})
    return row if isinstance(row, dict) else {}


def _save_release_cursor(
    R,
    calendar_owner_id: str,
    *,
    score: int,
    member: str,
) -> None:
    payload = {
        "score": int(score),
        "member": _txt(member),
        "updated_at_ms": _now_ms(),
    }
    R.setex(
        _release_cursor_key(calendar_owner_id),
        PERSIST_TTL_SEC,
        _json_dump(payload),
    )


def _fetch_releases_after_cursor(
    R,
    calendar_owner_id: str,
    *,
    limit: int,
) -> list[tuple[str, int]]:
    """
    Return index members strictly after a persisted (score, member) cursor.

    Redis sorted sets order equal scores lexicographically by member. We preserve
    both score + member, so two results sharing the same millisecond are not lost.
    """
    index_key = _release_index_key(calendar_owner_id)
    cursor = _load_release_cursor(R, calendar_owner_id)

    limit = max(1, int(limit or 1))
    cursor_score = _safe_int(cursor.get("score"), 0)
    cursor_member = _txt(cursor.get("member"))

    out: list[tuple[str, int]] = []

    if cursor_score <= 0 or not cursor_member:
        rows = R.zrange(
            index_key,
            0,
            limit - 1,
            withscores=True,
        ) or []

        for member, score in rows:
            out.append((_txt(member), int(float(score))))
        return out

    # First consume any equal-score members lexically after the cursor member.
    same_score = R.zrangebyscore(
        index_key,
        cursor_score,
        cursor_score,
        withscores=True,
    ) or []

    for member, score in same_score:
        member_s = _txt(member)
        if member_s > cursor_member:
            out.append((member_s, int(float(score))))
            if len(out) >= limit:
                return out

    remaining = limit - len(out)
    if remaining <= 0:
        return out

    # Then consume strictly higher scores.
    higher = R.zrangebyscore(
        index_key,
        f"({cursor_score}",
        "+inf",
        start=0,
        num=remaining,
        withscores=True,
    ) or []

    for member, score in higher:
        out.append((_txt(member), int(float(score))))

    return out


# ---------------------------------------------------------------------------
# Canonical release validation
# ---------------------------------------------------------------------------

def _load_canonical_release(
    R,
    *,
    calendar_owner_id: str,
    value_id: str,
) -> tuple[Optional[dict], str]:
    raw = R.get(
        _release_key(calendar_owner_id, value_id)
    )

    row = _json_load(raw, None)

    if not isinstance(row, dict):
        return None, "RELEASE_MISSING_OR_INVALID"

    if (
        _upper(row.get("source"))
        != "MT5_CALENDAR_RESULT_OBSERVER"
    ):
        return None, "RELEASE_SOURCE_INVALID"

    if (
        _txt(row.get("calendar_owner_id"))
        != calendar_owner_id
    ):
        return None, "RELEASE_OWNER_MISMATCH"

    if _txt(row.get("value_id")) != value_id:
        return None, "RELEASE_VALUE_ID_MISMATCH"

    event = row.get("event")
    if not isinstance(event, dict):
        return None, "RELEASE_EVENT_INVALID"

    event_id = _txt(
        row.get("event_id")
        or event.get("event_id")
    )
    event_value_id = _txt(
        event.get("value_id")
    )

    if not event_id:
        return None, "RELEASE_EVENT_ID_MISSING"

    if event_value_id and event_value_id != value_id:
        return None, "RELEASE_EVENT_VALUE_ID_MISMATCH"

    first_seen_ms = _safe_int(
        row.get("first_result_seen_ms"),
        0,
    )
    if first_seen_ms <= 0:
        return None, "RELEASE_FIRST_SEEN_MISSING"

    return row, ""


def _deadletter_release(
    R,
    *,
    calendar_owner_id: str,
    value_id: str,
    reason: str,
    index_score: int,
) -> None:
    payload = {
        "schema_version": SCHEMA_VERSION,
        "service": SERVICE_NAME,
        "calendar_owner_id": calendar_owner_id,
        "value_id": value_id,
        "reason": _txt(reason) or "UNKNOWN",
        "index_score": int(index_score or 0),
        "recorded_at_ms": _now_ms(),
        "observation_only": True,
    }

    R.setex(
        _deadletter_key(calendar_owner_id, value_id),
        PERSIST_TTL_SEC,
        _json_dump(payload),
    )


# ---------------------------------------------------------------------------
# Existing canonical DXY inputs
# ---------------------------------------------------------------------------

def _load_canonical_real_dxy_device(R) -> tuple[Optional[str], dict, str]:
    """
    Exact GET only.

    Deliberately does NOT call resolve_canonical_dxy_source(), because that
    resolver can perform automatic discovery with Redis keyspace SCAN when no
    healthy pin exists. Strategy B hot-path policy is exact-key only.
    """
    raw = R.get(DXY_CANONICAL_CONFIG_KEY)
    row = _json_load(raw, {})

    if not isinstance(row, dict):
        return None, {}, "DXY_CANONICAL_CONFIG_INVALID"

    if _upper(row.get("source") or "REAL_DXY") != "REAL_DXY":
        return None, row, "DXY_CANONICAL_SOURCE_NOT_REAL"

    device_id = _txt(
        row.get("device_id")
        or row.get("real_device_id")
    )

    if not device_id:
        return None, row, "DXY_CANONICAL_DEVICE_MISSING"

    return device_id, row, ""


def _load_real_dxy_bias(
    *,
    device_id: str,
    now_ms: int,
) -> tuple[
    Optional[dict],
    list[dict],
    list[dict],
    list[dict],
    str,
]:
    """
    Reuse canonical underlying production components:
      - load_real_dxy_bars()
      - bias_engine.compute_dxy_bias()

    No separate D1/H4/H1 bias implementation is created here.
    """
    try:
        from api.xtl_analytics import load_real_dxy_bars
        from api.bias_engine import (
            BIAS_ENGINE_VERSION,
            compute_dxy_bias,
        )
    except Exception as exc:
        return None, [], [], [], (
            "DXY_BIAS_IMPORT_FAILED:"
            f"{type(exc).__name__}"
        )

    try:
        bars_d1 = (
            load_real_dxy_bars(
                device_id=device_id,
                tf="D1",
                max_bars=60,
            )
            or []
        )
        bars_h4 = (
            load_real_dxy_bars(
                device_id=device_id,
                tf="H4",
                max_bars=100,
            )
            or []
        )
        bars_h1 = (
            load_real_dxy_bars(
                device_id=device_id,
                tf="H1",
                max_bars=150,
            )
            or []
        )

        bias = compute_dxy_bias(
            bars_d1=bars_d1,
            bars_h4=bars_h4,
            bars_h1=bars_h1,
            as_of_ms=now_ms,
            source="REAL_DXY",
        )

        if not isinstance(bias, dict):
            return None, [], [], [], "DXY_BIAS_RESULT_INVALID"

        bias = dict(bias)

        # Strategy-B structural D1 macro-leg evidence.
        #
        # Reuse the D1 bars already loaded above.  This is maturity
        # evidence only; it does not grant execution authority and does
        # not replace the existing D1/H4 macro-leg, location, M15, or H1
        # qualification gates.
        from api.strategy_b_d1_macro_leg import (
            calculate_strategy_b_d1_macro_leg,
        )

        strategy_b_d1_macro_leg = (
            calculate_strategy_b_d1_macro_leg(
                bars_d1,
                bias,
            )
        )

        bias["strategy_b_d1_macro_leg"] = (
            strategy_b_d1_macro_leg
        )

        bias.update({
            "engine_version": BIAS_ENGINE_VERSION,
            "source": "REAL_DXY",
            "canonical_device_id": device_id,
            "captured_at_ms": int(now_ms),
            "bars_available": {
                "D1": len(bars_d1),
                "H4": len(bars_h4),
                "H1": len(bars_h1),
            },
            "strategy_b_observation_only": True,
            "execution_wired": False,
            "risk_wired": False,
        })

        return (
            bias,
            list(bars_d1),
            list(bars_h4),
            list(bars_h1),
            "",
        ) 

    except Exception as exc:
        return None, [], [], [], (
            "DXY_BIAS_LOAD_FAILED:"
            f"{type(exc).__name__}"
        )


def _load_real_dxy_m15_state(
    R,
    *,
    device_id: str,
) -> tuple[Optional[dict], str]:
    key = _canonical_m15_key(device_id)

    try:
        raw = R.get(key)
    except Exception as exc:
        return None, (
            "DXY_M15_GET_FAILED:"
            f"{type(exc).__name__}"
        )

    row = _json_load(raw, None)

    if not isinstance(row, dict):
        return None, "DXY_M15_STATE_MISSING"

    if _upper(row.get("source")) != "REAL_DXY":
        return None, "DXY_M15_SOURCE_INVALID"

    state_device = _txt(row.get("device_id"))
    if state_device and state_device != device_id:
        return None, "DXY_M15_DEVICE_MISMATCH"

    return row, ""



def _load_real_dxy_h1_state(
    R,
    *,
    device_id: str,
) -> tuple[Optional[dict], str]:
    """
    Exact-GET the existing REAL_DXY H1 early-turn state.

    This reuses dxy_tracker output. Strategy B does not create another H1
    turn detector.
    """
    key = _canonical_h1_key(device_id)

    try:
        raw = R.get(key)
    except Exception as exc:
        return None, (
            "DXY_H1_GET_FAILED:"
            f"{type(exc).__name__}"
        )

    row = _json_load(raw, None)

    if not isinstance(row, dict):
        return None, "DXY_H1_STATE_MISSING"

    if _upper(row.get("source")) != "REAL_DXY":
        return None, "DXY_H1_SOURCE_INVALID"

    state_device = _txt(row.get("device_id"))
    if state_device and state_device != device_id:
        return None, "DXY_H1_DEVICE_MISMATCH"

    return row, ""


def _validate_m15_precheck(
    *,
    dxy_m15: Mapping[str, Any],
    canonical_device_id: str,
    now_ms: int,
) -> dict:
    """
    Cheap M15-only guard used before the expensive D1/H4/H1 bias load.

    This intentionally duplicates only the M15 side of
    validate_input_freshness(). Full freshness validation remains authoritative
    after HTF bias is loaded.
    """
    reasons: list[str] = []

    if not isinstance(dxy_m15, Mapping):
        reasons.append("DXY_M15_MISSING")
        m15_close_ms = 0
    else:
        if _upper(dxy_m15.get("source")) != "REAL_DXY":
            reasons.append("DXY_M15_SOURCE_NOT_REAL")

        state_device = _txt(dxy_m15.get("device_id"))
        if state_device and state_device != canonical_device_id:
            reasons.append("DXY_M15_DEVICE_MISMATCH")

        m15_close_ms = _safe_int(
            dxy_m15.get("last_evaluated_bar_close_ms")
            or dxy_m15.get("broker_bar_close_ms")
            or dxy_m15.get("bar_close_ms"),
            0,
        )

        if m15_close_ms <= 0:
            reasons.append("DXY_M15_CLOSE_TIME_MISSING")
        elif m15_close_ms > now_ms + FUTURE_TOLERANCE_MS:
            reasons.append("DXY_M15_CLOSE_TIME_FUTURE")
        elif now_ms - m15_close_ms > DXY_M15_FRESH_MS:
            reasons.append("DXY_M15_STALE")

        structure = dxy_m15.get("structure")
        if not isinstance(structure, Mapping):
            reasons.append("DXY_STRUCTURE_MISSING")
        elif (
            "room_up_atr" not in structure
            and "room_down_atr" not in structure
        ):
            reasons.append("DXY_STRUCTURE_ROOM_MISSING")

    return {
        "ok": not reasons,
        "reasons": reasons,
        "canonical_device_id": canonical_device_id,
        "checked_at_ms": int(now_ms),
        "m15_close_ms": int(m15_close_ms or 0),
        "m15_age_ms": (
            max(0, int(now_ms - m15_close_ms))
            if m15_close_ms > 0
            else None
        ),
        "m15_fresh_limit_ms": DXY_M15_FRESH_MS,
        "stage": "M15_PRECHECK",
    }


def validate_input_freshness(
    *,
    dxy_bias: Mapping[str, Any],
    dxy_m15: Mapping[str, Any],
    canonical_device_id: str,
    now_ms: int,
) -> dict:
    """
    Mandatory-input freshness check.

    This does NOT decide PLACE/WAIT. It only says whether Strategy-B thesis
    interpretation has trustworthy canonical inputs at this observation time.
    """
    reasons: list[str] = []

    if not isinstance(dxy_bias, Mapping):
        reasons.append("DXY_BIAS_MISSING")
    else:
        if dxy_bias.get("data_ok") is not True:
            reasons.append("DXY_BIAS_DATA_NOT_OK")

        if _upper(dxy_bias.get("source")) != "REAL_DXY":
            reasons.append("DXY_BIAS_SOURCE_NOT_REAL")

        bias_device = _txt(
            dxy_bias.get("canonical_device_id")
            or dxy_bias.get("device_id")
        )
        if bias_device != canonical_device_id:
            reasons.append("DXY_BIAS_DEVICE_MISMATCH")

        timeframes = dxy_bias.get("timeframes")
        if not isinstance(timeframes, Mapping):
            reasons.append("DXY_BIAS_TIMEFRAMES_MISSING")
        else:
            for tf in ("D1", "H4", "H1"):
                row = timeframes.get(tf)
                if not isinstance(row, Mapping):
                    reasons.append(f"DXY_{tf}_BIAS_MISSING")
                    continue

                if row.get("data_ok") is not True:
                    reasons.append(f"DXY_{tf}_BIAS_NOT_OK")

                # bias_engine already owns timeframe-specific freshness rules.
                if row.get("fresh") is False:
                    reasons.append(f"DXY_{tf}_BIAS_STALE")

    if not isinstance(dxy_m15, Mapping):
        reasons.append("DXY_M15_MISSING")
        m15_close_ms = 0
    else:
        if _upper(dxy_m15.get("source")) != "REAL_DXY":
            reasons.append("DXY_M15_SOURCE_NOT_REAL")

        state_device = _txt(dxy_m15.get("device_id"))
        if state_device and state_device != canonical_device_id:
            reasons.append("DXY_M15_DEVICE_MISMATCH")

        m15_close_ms = _safe_int(
            dxy_m15.get("last_evaluated_bar_close_ms")
            or dxy_m15.get("broker_bar_close_ms")
            or dxy_m15.get("bar_close_ms"),
            0,
        )

        if m15_close_ms <= 0:
            reasons.append("DXY_M15_CLOSE_TIME_MISSING")
        elif m15_close_ms > now_ms + FUTURE_TOLERANCE_MS:
            reasons.append("DXY_M15_CLOSE_TIME_FUTURE")
        elif now_ms - m15_close_ms > DXY_M15_FRESH_MS:
            reasons.append("DXY_M15_STALE")

        structure = dxy_m15.get("structure")
        if not isinstance(structure, Mapping):
            reasons.append("DXY_STRUCTURE_MISSING")
        else:
            # trend_sr structure must have at least directional room fields.
            if (
                "room_up_atr" not in structure
                and "room_down_atr" not in structure
            ):
                reasons.append("DXY_STRUCTURE_ROOM_MISSING")

    return {
        "ok": not reasons,
        "reasons": reasons,
        "canonical_device_id": canonical_device_id,
        "checked_at_ms": int(now_ms),
        "m15_close_ms": int(m15_close_ms or 0),
        "m15_age_ms": (
            max(0, int(now_ms - m15_close_ms))
            if m15_close_ms > 0
            else None
        ),
        "m15_fresh_limit_ms": DXY_M15_FRESH_MS,
    }


def _load_strategy_b_dxy_inputs(
    R,
    *,
    now_ms: int,
) -> tuple[Optional[dict], str]:
    device_id, canonical_cfg, err = (
        _load_canonical_real_dxy_device(R)
    )
    if not device_id:
        return None, err or "DXY_CANONICAL_UNAVAILABLE"

    # Cheap exact-key M15 check first. Do not load D1/H4/H1 bars when the
    # canonical M15 state is already stale/unusable (e.g. weekend).
    m15, err = _load_real_dxy_m15_state(
        R,
        device_id=device_id,
    )
    if not isinstance(m15, dict):
        return None, err or "DXY_M15_UNAVAILABLE"

    precheck = _validate_m15_precheck(
        dxy_m15=m15,
        canonical_device_id=device_id,
        now_ms=now_ms,
    )
    if not precheck.get("ok"):
        return {
            "device_id": device_id,
            "canonical_config": canonical_cfg,
            "dxy_m15": m15,
            "dxy_h1_turn": {},
            "freshness": precheck,
            "m15_precheck": precheck,
        }, "MANDATORY_DXY_INPUT_NOT_FRESH"

    bias, d1_bars, h4_bars, h1_bars, err = _load_real_dxy_bias(
        device_id=device_id,
        now_ms=now_ms,
    )
    if not isinstance(bias, dict):
        return None, err or "DXY_BIAS_UNAVAILABLE"

    # H1 turn evidence is an additive Strategy-B input. Missing H1 state does
    # not invalidate canonical DXY freshness; macro qualification will fail
    # closed at WAIT_H1_REVERSAL.
    h1_turn, h1_err = _load_real_dxy_h1_state(
        R,
        device_id=device_id,
    )
    if not isinstance(h1_turn, dict):
        h1_turn = {}

    freshness = validate_input_freshness(
        dxy_bias=bias,
        dxy_m15=m15,
        canonical_device_id=device_id,
        now_ms=now_ms,
    )

    payload = {
        "device_id": device_id,
        "canonical_config": canonical_cfg,
        "dxy_bias": bias,
        "dxy_d1_bars": list(d1_bars or []),
        "dxy_h4_bars": list(h4_bars or []),
        "dxy_h1_bars": list(h1_bars or []),
        "dxy_m15": m15,
        "dxy_h1_turn": h1_turn,
        "dxy_h1_error": h1_err or "",
        "freshness": freshness,
        "m15_precheck": precheck,
    }

    if not freshness.get("ok"):
        return payload, "MANDATORY_DXY_INPUT_NOT_FRESH"

    return payload, ""


# ---------------------------------------------------------------------------
# Evidence + thesis persistence
# ---------------------------------------------------------------------------

def _import_intelligence():
    from api.macro_event_intelligence_strategy_b_thesis_v1 import (
        build_dxy_macro_thesis,
        build_mt5_event_evidence,
    )
    return build_mt5_event_evidence, build_dxy_macro_thesis


def _persist_evidence_and_context(
    R,
    *,
    evidence,
    calendar_owner_id: str,
    value_id: str,
    release_record: Mapping[str, Any],
    index_score: int,
) -> dict:
    evidence_id = _txt(evidence.evidence_id)

    evidence_payload = evidence.to_dict()
    evidence_payload.update({
        "observation_only": True,
        "execution_authority": False,
        "persisted_at_ms": _now_ms(),
    })

    # Preserve an already-frozen Strategy-B D1 macro-thesis contract.
    #
    # The release cursor normally prevents duplicate release consumption,
    # but permanent execution-dead state must not depend on cursor behavior.
    # If this evidence_id is persisted again, retain its existing frozen
    # structural identity and terminal latch.
    existing_context = _json_load(
        R.get(_context_key(evidence_id)),
        {},
    )
    existing_macro_thesis_contract = (
        existing_context.get("macro_thesis_contract")
        if isinstance(existing_context, dict)
        and isinstance(
            existing_context.get("macro_thesis_contract"),
            dict,
        )
        else None
    )

    context = {
        "schema_version": SCHEMA_VERSION,
        "service": SERVICE_NAME,
        "evidence_id": evidence_id,
        "calendar_owner_id": calendar_owner_id,
        "value_id": value_id,
        "event_id": _txt(
            release_record.get("event_id")
            or (release_record.get("event") or {}).get("event_id")
        ),
        "first_result_seen_ms": _safe_int(
            release_record.get("first_result_seen_ms"),
            index_score,
        ),
        "release_index_score": int(index_score),
        "release_key": _release_key(
            calendar_owner_id,
            value_id,
        ),
        "created_at_ms": _now_ms(),
        "last_evaluated_at_ms": 0,
        "last_evaluation_status": "PENDING",
        "last_evaluation_reason": "NOT_YET_EVALUATED",
        "observation_only": True,
        "execution_authority": False,
    }

    if isinstance(existing_macro_thesis_contract, dict):
        context["macro_thesis_contract"] = dict(
            existing_macro_thesis_contract
        )

    pipe = R.pipeline(transaction=True)
    pipe.setex(
        _evidence_key(evidence_id),
        PERSIST_TTL_SEC,
        _json_dump(evidence_payload),
    )
    pipe.setex(
        _context_key(evidence_id),
        PERSIST_TTL_SEC,
        _json_dump(context),
    )
    pipe.zadd(
        ACTIVE_KEY,
        {
            evidence_id: float(
                _safe_int(
                    release_record.get(
                        "first_result_seen_ms"
                    ),
                    index_score,
                )
                or index_score
            )
        },
    )
    pipe.expire(ACTIVE_KEY, PERSIST_TTL_SEC)
    pipe.execute()

    return context


def _material_thesis_projection(
    thesis_row: Mapping[str, Any],
) -> dict:
    """
    Ignore ephemeral IDs/timestamps so we persist history only when material
    Strategy-B interpretation changes.
    """
    return {
        "state": thesis_row.get("state"),
        "fundamental_direction": thesis_row.get(
            "fundamental_direction"
        ),
        "expected_dxy_direction": thesis_row.get(
            "expected_dxy_direction"
        ),
        "market_reaction": thesis_row.get(
            "market_reaction"
        ),
        "d1_direction": thesis_row.get("d1_direction"),
        "h4_direction": thesis_row.get("h4_direction"),
        "h1_direction": thesis_row.get("h1_direction"),
        "m15_flow_direction": thesis_row.get(
            "m15_flow_direction"
        ),
        "m15_turn_status": thesis_row.get(
            "m15_turn_status"
        ),
        "m15_turn_direction": thesis_row.get(
            "m15_turn_direction"
        ),
        "structure_capacity": thesis_row.get(
            "structure_capacity"
        ),
        "structure_pressure": thesis_row.get(
            "structure_pressure"
        ),
        "structure_pressure_reason": thesis_row.get(
            "structure_pressure_reason"
        ),
        "directional_room_atr": thesis_row.get(
            "directional_room_atr"
        ),
        "inside_blocking_zone": thesis_row.get(
            "inside_blocking_zone"
        ),
        "h1_reversal_confirmed": thesis_row.get(
            "h1_reversal_confirmed"
        ),
        "confidence": thesis_row.get("confidence"),
        "reasons": thesis_row.get("reasons") or [],
    }


def persist_macro_thesis(
    R,
    *,
    evidence_id: str,
    calendar_owner_id: str,
    value_id: str,
    thesis,
    input_freshness: Mapping[str, Any],
    now_ms: int,
) -> dict:
    thesis_row = thesis.to_dict()

    material = _material_thesis_projection(thesis_row)
    fingerprint = _sha256_json(material)

    latest_key = _thesis_latest_key(evidence_id)
    previous = _json_load(R.get(latest_key), {})

    previous_fp = (
        _txt(previous.get("material_fingerprint"))
        if isinstance(previous, dict)
        else ""
    )

    changed = previous_fp != fingerprint

    previous_state = (
        _txt(
            (previous.get("thesis") or {}).get("state")
        )
        if isinstance(previous, dict)
        and isinstance(previous.get("thesis"), dict)
        else ""
    )

    wrapper = {
        "schema_version": SCHEMA_VERSION,
        "service": SERVICE_NAME,
        "mode": MODE,
        "evidence_id": evidence_id,
        "calendar_owner_id": calendar_owner_id,
        "value_id": value_id,
        "material_fingerprint": fingerprint,
        "previous_state": previous_state or None,
        "state_changed": (
            bool(previous_state)
            and previous_state != _txt(thesis_row.get("state"))
        ),
        "material_changed": bool(changed),
        "persisted_at_ms": int(now_ms),
        "input_freshness": dict(input_freshness),
        "thesis": thesis_row,
        "observation_only": True,
        "execution_authority": False,
        "strategy_a_mutation": False,
    }

    if changed:
        history_key = _thesis_history_key(evidence_id)

        pipe = R.pipeline(transaction=True)
        pipe.setex(
            latest_key,
            PERSIST_TTL_SEC,
            _json_dump(wrapper),
        )
        pipe.rpush(
            history_key,
            _json_dump(wrapper),
        )
        pipe.ltrim(
            history_key,
            -MAX_HISTORY,
            -1,
        )
        pipe.expire(
            history_key,
            PERSIST_TTL_SEC,
        )
        pipe.setex(
            _owner_latest_key(calendar_owner_id),
            PERSIST_TTL_SEC,
            _json_dump({
                "evidence_id": evidence_id,
                "value_id": value_id,
                "state": thesis_row.get("state"),
                "persisted_at_ms": int(now_ms),
                "thesis_latest_key": latest_key,
                "observation_only": True,
            }),
        )
        pipe.execute()
    else:
        # Do not append duplicate history every manager tick.
        # Refresh latest TTL and observation timestamp only.
        refreshed = dict(previous)
        refreshed["last_observed_at_ms"] = int(now_ms)
        refreshed["input_freshness"] = dict(input_freshness)

        R.setex(
            latest_key,
            PERSIST_TTL_SEC,
            _json_dump(refreshed),
        )

    return {
        "changed": changed,
        "fingerprint": fingerprint,
        "state": thesis_row.get("state"),
        "previous_state": previous_state or None,
    }


def load_macro_thesis(
    R,
    evidence_id: str,
) -> Optional[dict]:
    row = _json_load(
        R.get(_thesis_latest_key(evidence_id)),
        None,
    )
    return row if isinstance(row, dict) else None


def _update_context_status(
    R,
    *,
    evidence_id: str,
    status: str,
    reason: str,
    now_ms: int,
    extra: Optional[Mapping[str, Any]] = None,
) -> None:
    key = _context_key(evidence_id)
    context = _json_load(R.get(key), {})

    if not isinstance(context, dict):
        context = {
            "schema_version": SCHEMA_VERSION,
            "service": SERVICE_NAME,
            "evidence_id": evidence_id,
        }

    context["last_evaluated_at_ms"] = int(now_ms)
    context["last_evaluation_status"] = _txt(status)
    context["last_evaluation_reason"] = _txt(reason)

    if extra:
        context["last_evaluation_meta"] = dict(extra)

    context["observation_only"] = True
    context["execution_authority"] = False

    R.setex(
        key,
        PERSIST_TTL_SEC,
        _json_dump(context),
    )


def _persist_macro_thesis_contract(
    R,
    *,
    evidence_id: str,
    contract: Mapping[str, Any],
) -> dict:
    """
    Persist one frozen Strategy-B D1 macro-thesis contract in the existing
    evidence context.

    execution_dead=True is terminal and monotonic.  A later reevaluation
    must never replace a dead contract with an ACTIVE/WAIT contract.
    """
    key = _context_key(evidence_id)
    context = _json_load(R.get(key), {})

    if not isinstance(context, dict):
        return {}

    existing = (
        context.get("macro_thesis_contract")
        if isinstance(
            context.get("macro_thesis_contract"),
            dict,
        )
        else {}
    )

    if existing.get("execution_dead") is True:
        return dict(existing)

    incoming = (
        dict(contract)
        if isinstance(contract, Mapping)
        else {}
    )

    if not incoming:
        return dict(existing)

    context["macro_thesis_contract"] = incoming
    context["observation_only"] = True
    context["execution_authority"] = False

    R.setex(
        key,
        PERSIST_TTL_SEC,
        _json_dump(context),
    )

    return dict(incoming)


# ---------------------------------------------------------------------------
# One evidence evaluation
# ---------------------------------------------------------------------------

def _rebuild_evidence_from_persisted(
    row: Mapping[str, Any],
):
    """
    Reconstruct immutable MacroEvidence without depending on private helpers.
    """
    from api.macro_event_intelligence_strategy_b_thesis_v1 import (
        MacroEvidence,
    )

    return MacroEvidence(
        evidence_id=_txt(row.get("evidence_id")),
        source=_txt(row.get("source")),
        source_id=_txt(row.get("source_id")),
        currency=_txt(row.get("currency")),
        title=_txt(row.get("title")),
        event_time_ms=_safe_int(row.get("event_time_ms"), 0),
        published_at_ms=_safe_int(row.get("published_at_ms"), 0),
        first_seen_at_ms=_safe_int(row.get("first_seen_at_ms"), 0),
        observed_at_ms=_safe_int(row.get("observed_at_ms"), 0),
        importance=_txt(row.get("importance")),
        topics=tuple(row.get("topics") or ()),
        actual=row.get("actual"),
        forecast=row.get("forecast"),
        previous=row.get("previous"),
        revised_previous=row.get("revised_previous"),
        fundamental_direction=_txt(
            row.get("fundamental_direction")
        )
        or "UNKNOWN",
        market_reaction=_txt(
            row.get("market_reaction")
        )
        or "UNKNOWN",
        thesis_relationship=_txt(
            row.get("thesis_relationship")
        )
        or "UNKNOWN",
        effect=_txt(row.get("effect")) or "NO_CHANGE",
        confidence=float(row.get("confidence") or 0.0),
        reasons=tuple(row.get("reasons") or ()),
        metadata=(
            dict(row.get("metadata"))
            if isinstance(row.get("metadata"), dict)
            else {}
        ),
    )




def _strategy_b_apply_selected_h1_proof(
    *,
    macro_qualification,
    selected_symbol,
    selected_side,
    h1_proof_by_candidate,
):
    """
    Return a selected-expression Strategy-B macro qualification view with
    only the selected candidate's approved H1 proof overlaid.

    The canonical macro/DXY qualification remains unchanged.  This helper
    has no execution, broker, risk, Redis, or Strategy-A authority.
    """
    from dataclasses import replace

    if macro_qualification is None:
        return None

    symbol = str(selected_symbol or "").strip().upper()
    side = str(selected_side or "").strip().upper()

    if not symbol or side not in {"BUY", "SELL"}:
        return None

    proofs = (
        h1_proof_by_candidate
        if isinstance(h1_proof_by_candidate, dict)
        else {}
    )

    proof = proofs.get((symbol, side))
    if not isinstance(proof, dict):
        return None

    proof_symbol = str(proof.get("symbol") or "").strip().upper()
    proof_side = str(proof.get("side") or "").strip().upper()

    if proof_symbol != symbol or proof_side != side:
        return None

    proven = proof.get("h1_reversal_proven") is True
    reason = str(
        proof.get("reason")
        or (
            "APPROVED_H1_REVERSAL_PROOF"
            if proven
            else "NO_APPROVED_H1_REVERSAL_PROOF"
        )
    )

    primary = str(proof.get("primary_proof_path") or "").strip()
    status = str(proof.get("status") or "").strip()
    source = primary or status or "H1_REVERSAL_NOT_PROVEN"

    old_diagnostics = getattr(
        macro_qualification,
        "diagnostics",
        None,
    )
    diagnostics = (
        dict(old_diagnostics)
        if isinstance(old_diagnostics, dict)
        else {}
    )

    diagnostics["h1_confirmation_source"] = source
    diagnostics["_strategy_b_symbol_h1_proof"] = dict(proof)

    try:
        return replace(
            macro_qualification,
            h1_reversal_confirmed=proven,
            h1_reversal_reason=reason,
            diagnostics=diagnostics,
        )
    except (TypeError, ValueError):
        return None


def _evaluate_strategy_b_canonical_macro_qualification(
    *,
    evidence,
    dxy_inputs: Mapping[str, Any],
):
    """
    Evaluate the existing canonical Strategy-B macro qualification without
    granting any additional execution authority.

    This helper intentionally reuses the same live-input adapter and
    qualification evaluator used by the shadow composition.  It exists so
    frozen D1 thesis ownership can be established before H1 pattern,
    expression, Point-3, risk, or order work.
    """
    from api.strategy_b_live_input_adapter import (
        build_strategy_b_live_inputs,
    )
    from api.strategy_b_macro_qualification import (
        evaluate_strategy_b_macro_qualification,
    )

    dxy_m15 = (
        dxy_inputs.get("dxy_m15")
        if isinstance(dxy_inputs.get("dxy_m15"), Mapping)
        else {}
    )
    dxy_structure = (
        dxy_m15.get("structure")
        if isinstance(dxy_m15.get("structure"), Mapping)
        else {}
    )

    live = build_strategy_b_live_inputs(
        dxy_bias=(
            dxy_inputs.get("dxy_bias")
            if isinstance(dxy_inputs.get("dxy_bias"), Mapping)
            else {}
        ),
        dxy_m15=dxy_m15,
        dxy_h1_turn=(
            dxy_inputs.get("dxy_h1_turn")
            if isinstance(dxy_inputs.get("dxy_h1_turn"), Mapping)
            else {}
        ),
        dxy_structure=dxy_structure,
        pattern_observations={},
        expression_candidates=(),
    )

    macro = evaluate_strategy_b_macro_qualification(
        evidence,
        dxy_bias=live.dxy_bias,
        dxy_m15=live.dxy_m15,
        dxy_h1_turn=live.dxy_h1_turn,
        dxy_structure=live.dxy_structure,
    )

    return macro, live


def _compose_and_persist_strategy_b_macro_shadow(
    R,
    *,
    evidence,
    evidence_id: str,
    context: Mapping[str, Any],
    dxy_inputs: Mapping[str, Any],
    now_ms: int,
    calibration_store: Any = None,
) -> dict:
    """
    Compose the Strategy-B pre-trade shadow view with the V6.4 live expression
    basket and durable JSONL calibration capture.

    Still observation-only:
    - no risk reservation;
    - no broker order/modify/close calls;
    - no Strategy-A mutation;
    - no production structural buffer is invented.
    """
    from api.strategy_b_shadow_orchestrator import (
        compose_strategy_b_shadow_decision,
    )

    macro, live = (
        _evaluate_strategy_b_canonical_macro_qualification(
            evidence=evidence,
            dxy_inputs=dxy_inputs,
        )
    )

    dxy_m15 = live.dxy_m15
    dxy_structure = live.dxy_structure

    macro_row = macro.to_dict()
    if (
        _safe_int(macro_row.get("schema_version"), 0) < 2
        or "market_reaction_confirmed" not in macro_row
    ):
        raise RuntimeError("STRATEGY_B_MACRO_QUALIFICATION_V2_REQUIRED")

    from api.strategy_b_h1_pattern_observer import (
        observe_strategy_b_h1_patterns,
    )
    from api.strategy_b_entry_patterns import (
        evaluate_strategy_b_entry_patterns,
    )

    h1_bars = (
        dxy_inputs.get("dxy_h1_bars")
        if isinstance(dxy_inputs.get("dxy_h1_bars"), list)
        else []
    )

    catalyst_first_seen_ms = _safe_int(
        getattr(macro, "catalyst_first_seen_ms", 0),
        0,
    )

    pattern_observer = observe_strategy_b_h1_patterns(
        h1_bars=h1_bars,
        expected_direction=macro.expected_dxy_direction,
        catalyst_first_seen_ms=catalyst_first_seen_ms,
        dxy_bias=live.dxy_bias,
        dxy_structure=live.dxy_structure,
        as_of_ms=now_ms,
    )

    # Evidence-specific selected-symbol H1 observations.
    # Reuse the ranker's canonical public expression maps; do not duplicate map
    # policy in this service.
    from api.strategy_b_expression_ranker import (
        USD_BEARISH_EXPRESSIONS,
        USD_BULLISH_EXPRESSIONS,
    )
    from api.strategy_b_symbol_h1_live_observer import (
        observe_strategy_b_symbol_h1_live,
    )

    expected_dxy_direction = _upper(
        getattr(macro, "expected_dxy_direction", "")
    )
    if expected_dxy_direction == "BEARISH":
        approved_expressions = dict(USD_BEARISH_EXPRESSIONS)
    elif expected_dxy_direction == "BULLISH":
        approved_expressions = dict(USD_BULLISH_EXPRESSIONS)
    else:
        approved_expressions = {}

    # Request/composition-scoped native OHLC cache.
    #
    # Lifetime is exactly this Strategy-B macro composition. It is deliberately
    # not module-global, not persisted, has no TTL, and creates no Redis keys.
    # D1/H4/H1 reads shared by H1 observation and expression structural
    # invalidation therefore reuse the same immutable acquisition result.
    native_tf_cache: dict[tuple[str, str], dict[str, Any]] = {}

    symbol_h1_rows: dict[tuple[str, str], dict[str, Any]] = {}
    h1_proof_by_candidate: dict[
        tuple[str, str], Mapping[str, Any]
    ] = {}
    h1_break_ms_by_candidate: dict[
        tuple[str, str], int
    ] = {}

    # Immutable causal references for Strategy-B forward-path calibration.
    # These come from the SAME evidence-specific H1 pattern observation as
    # h1_break_ms and must never be replaced with later live price / ATR.
    h1_break_close_by_candidate: dict[
        tuple[str, str], float
    ] = {}
    h1_reference_atr_by_candidate: dict[
        tuple[str, str], float
    ] = {}

    for _symbol, _side in approved_expressions.items():
        _key = (_upper(_symbol), _upper(_side))
        try:
            _h1 = observe_strategy_b_symbol_h1_live(
                R,
                symbol=_key[0],
                side=_key[1],
                evidence_id=evidence_id,
                as_of_ms=now_ms,
                native_tf_cache=native_tf_cache,
            )
            _h1_row = (
                _h1.to_dict()
                if hasattr(_h1, "to_dict")
                else dict(_h1)
                if isinstance(_h1, Mapping)
                else {}
            )
        except Exception as exc:
            _h1_row = {
                "schema_version": 2,
                "model": "STRATEGY_B_LIVE_SYMBOL_H1_OBSERVER_V2",
                "symbol": _key[0],
                "side": _key[1],
                "as_of_ms": int(now_ms),
                "catalyst_first_seen_ms": int(
                    catalyst_first_seen_ms or 0
                ),
                "evidence_id": evidence_id,
                "ok": False,
                "reason": (
                    "H1_OBSERVER_EXCEPTION:"
                    + type(exc).__name__
                ),
                "pattern": None,
                "proof": None,
                "observation_only": True,
                "execution_authority": False,
                "strategy_a_mutation": False,
                "redis_scan_used": False,
            }

        symbol_h1_rows[_key] = dict(_h1_row)

        # Never fabricate False proof when H1 observation failed.
        if _h1_row.get("ok") is not True:
            continue

        _proof = (
            _h1_row.get("proof")
            if isinstance(_h1_row.get("proof"), Mapping)
            else None
        )
        if isinstance(_proof, Mapping):
            h1_proof_by_candidate[_key] = dict(_proof)

        _pattern = (
            _h1_row.get("pattern")
            if isinstance(_h1_row.get("pattern"), Mapping)
            else {}
        )
        _break_ms = _safe_int(_pattern.get("break_ms"), 0)
        if _break_ms > 0:
            h1_break_ms_by_candidate[_key] = _break_ms

        try:
            _break_close = float(_pattern.get("break_close"))
        except Exception:
            _break_close = 0.0
        if _break_close > 0:
            h1_break_close_by_candidate[_key] = _break_close

        try:
            _reference_atr = float(_pattern.get("atr"))
        except Exception:
            _reference_atr = 0.0
        if _reference_atr > 0:
            h1_reference_atr_by_candidate[_key] = _reference_atr

    from api.strategy_b_expression_live_observer import (
        observe_strategy_b_expression_basket_live,
    )

    expression_basket = observe_strategy_b_expression_basket_live(
        R,
        expected_dxy_direction=expected_dxy_direction,
        as_of_ms=now_ms,
        catalyst_first_seen_ms=catalyst_first_seen_ms,
        h1_proof_by_candidate=h1_proof_by_candidate,
        h1_break_ms_by_candidate=h1_break_ms_by_candidate,
        h1_break_close_by_candidate=h1_break_close_by_candidate,
        h1_reference_atr_by_candidate=h1_reference_atr_by_candidate,
        preferred_htf_owner=None,
        structural_buffer_price=None,
        native_tf_cache=native_tf_cache,
    )
    expression_row = expression_basket.to_dict()

    _ranking = (
        expression_row.get("ranking")
        if isinstance(expression_row.get("ranking"), Mapping)
        else {}
    )
    downstream_expression = (
        dict(_ranking)
        if _ranking
        else {
            "decision": "WAIT",
            "reason": (
                expression_row.get("reason")
                or "EXPRESSION_RANKING_NOT_AVAILABLE"
            ),
            "best_symbol": None,
            "best_side": None,
            "observation_only": True,
            "execution_authority": False,
        }
    )

    # Strategy-B Point-3 risk boundary.
    #
    # Consume only the finalized selected expression.  Do not recompute
    # structural invalidation here.  Production structural buffer policy is
    # deliberately not calibrated yet, so an invalid/unavailable finalized
    # stop must short-circuit before any account/risk/conversion IO.
    _selected_expression: dict[str, Any] = {}

    _best_symbol = _upper(_ranking.get("best_symbol"))
    _best_side = _upper(_ranking.get("best_side"))
    _ranked = _ranking.get("ranked")

    # Selected-expression H1 qualification boundary.
    #
    # The canonical macro/DXY qualification is intentionally not mutated.
    # Only the expression ranker's selected (symbol, side) may contribute
    # its symbol-specific H1 reversal proof to downstream entry/lifecycle.
    _selected_macro = _strategy_b_apply_selected_h1_proof(
        macro_qualification=macro,
        selected_symbol=_best_symbol,
        selected_side=_best_side,
        h1_proof_by_candidate=h1_proof_by_candidate,
    )

    if _selected_macro is None:
        # Fail closed when there is no canonical selected expression/proof.
        # Never fall back to another candidate's H1 evidence.
        _selected_macro = macro

    downstream_entry = evaluate_strategy_b_entry_patterns(
        _selected_macro,
        pattern_observations=(
            pattern_observer.to_entry_pattern_observations()
        ),
        m15_state=live.dxy_m15,
    )

    if (
        _best_symbol
        and _best_side in {"BUY", "SELL"}
        and isinstance(_ranked, list)
    ):
        for _ranked_item in _ranked:
            if not isinstance(_ranked_item, Mapping):
                continue

            if (
                _upper(_ranked_item.get("symbol")) != _best_symbol
                or _upper(_ranked_item.get("side")) != _best_side
            ):
                continue

            _raw = _ranked_item.get("raw")
            if isinstance(_raw, Mapping):
                _selected_expression = dict(_raw)
            break

    _structural_invalidation_price = (
        _selected_expression.get("structural_invalidation_price")
        if _selected_expression
        else None
    )
    _structural_buffer_price = (
        _selected_expression.get("structural_buffer_price")
        if _selected_expression
        else None
    )
    _proposed_structural_stop_price = (
        _selected_expression.get("proposed_structural_stop_price")
        if _selected_expression
        else None
    )

    _structural_stop_ready = bool(
        _selected_expression
        and _structural_invalidation_price is not None
        and _structural_buffer_price is not None
        and _proposed_structural_stop_price is not None
    )

    # Global Strategy-B readiness boundary.
    #
    # Account/profile-specific prop risk does NOT belong in this global
    # macro service.  The per-UID executor owns execution assignment,
    # authoritative prop capacity, loss-per-lot conversion, production
    # sizing, broker normalization, risk reservation and order authority.
    #
    # At this boundary we prove only that the selected expression has
    # complete structural trade geometry.  This allows the canonical
    # ENTRY_READY shadow/lifecycle to publish a global Strategy-B intent;
    # the executor must still pass every production risk/execution gate.
    if not _selected_expression:
        _risk_wait_reason = "SELECTED_EXPRESSION_NOT_AVAILABLE"
        _risk_ready = False
    elif not _structural_stop_ready:
        _risk_wait_reason = "STRUCTURAL_STOP_NOT_READY"
        _risk_ready = False
    else:
        _risk_wait_reason = "STRUCTURAL_RISK_READY_FOR_EXECUTION"
        _risk_ready = True

    downstream_risk = {
        "decision": (
            "RISK_PROPOSAL_READY_SHADOW"
            if _risk_ready
            else "WAIT"
        ),
        "reason": _risk_wait_reason,
        "valid": bool(_risk_ready),
        "symbol": _best_symbol or None,
        "side": _best_side or None,
        "structural_invalidation_price": _structural_invalidation_price,
        "structural_buffer_price": _structural_buffer_price,
        "proposed_structural_stop_price": _proposed_structural_stop_price,

        # Deliberately unset here. Production monetary risk and lots are
        # resolved later by the per-UID executor from the active prop
        # profile and authoritative account-risk state.
        "proposed_risk_usd": None,
        "proposed_lots_raw": None,

        # This global composition layer itself never has broker/order
        # authority. Execution authority remains downstream.
        "observation_only": True,
        "execution_authority": False,
    }

    # Durable JSONL capture. Reuse the H1 rows already observed above so this
    # does not perform another six-symbol H1 acquisition.
    calibration_capture: dict[str, Any]

    if expression_row.get("complete") is True:
        from api.strategy_b_live_calibration_runtime import (
            capture_strategy_b_runtime_calibration,
        )

        def _cached_symbol_h1_observer(
            _R,
            *,
            symbol: str,
            side: str,
            evidence_id: str,
            as_of_ms: Optional[int] = None,
        ):
            _cache_key = (_upper(symbol), _upper(side))
            _cached = symbol_h1_rows.get(_cache_key)
            if isinstance(_cached, Mapping):
                return dict(_cached)

            return {
                "schema_version": 2,
                "model": "STRATEGY_B_LIVE_SYMBOL_H1_OBSERVER_V2",
                "symbol": _cache_key[0],
                "side": _cache_key[1],
                "as_of_ms": int(as_of_ms or now_ms),
                "catalyst_first_seen_ms": int(
                    catalyst_first_seen_ms or 0
                ),
                "evidence_id": evidence_id,
                "ok": False,
                "reason": "H1_OBSERVATION_CACHE_MISSING",
                "pattern": None,
                "proof": None,
                "observation_only": True,
                "execution_authority": False,
                "strategy_a_mutation": False,
                "redis_scan_used": False,
            }

        try:
            calibration_capture = (
                capture_strategy_b_runtime_calibration(
                    R=R,
                    basket=expression_basket,
                    evidence_id=evidence_id,
                    as_of_ms=now_ms,
                    release_id=_txt(context.get("value_id")) or None,
                    device_id=_txt(
                        dxy_inputs.get("device_id")
                    ) or None,
                    environment="DEMO",
                    source="LIVE",
                    h1_observer=_cached_symbol_h1_observer,
                    thesis_id=evidence_id,
                    store=calibration_store,
                )
            )
        except Exception as exc:
            calibration_capture = {
                "ok": False,
                "written": 0,
                "duplicate": 0,
                "reason": (
                    "CALIBRATION_CAPTURE_EXCEPTION:"
                    + type(exc).__name__
                ),
                "evidence_id": evidence_id,
                "error": str(exc),
            }

        if calibration_capture.get("ok") is not True:
            log.warning(
                "[STRATEGY_B_MACRO] CALIBRATION_CAPTURE_SKIP "
                "evidence_id=%s reason=%s path=%s",
                evidence_id,
                calibration_capture.get("reason"),
                calibration_capture.get("path"),
            )
    else:
        calibration_capture = {
            "ok": False,
            "written": 0,
            "duplicate": 0,
            "reason": "EXPRESSION_BASKET_INCOMPLETE",
            "evidence_id": evidence_id,
            "path": None,
        }

    prior = reconstruct_strategy_b_shadow(
        R,
        evidence_id=evidence_id,
    )
    current_state = (
        _upper(prior.get("derived_state"))
        if isinstance(prior, Mapping) and prior.get("found") is True
        else "OBSERVING"
    )
    if not current_state:
        current_state = "OBSERVING"

    shadow = compose_strategy_b_shadow_decision(
        current_state=current_state,
        macro_qualification=_selected_macro,
        entry_pattern=downstream_entry,
        expression_ranking=downstream_expression,
        risk_proposal=downstream_risk,
    )

    try:
        selected_capture_link = _resolve_strategy_b_selected_capture_link(
            shadow_decision=shadow,
            calibration_capture=calibration_capture,
            prior_shadow=prior,
        )
    except Exception as exc:
        log.exception(
            "[STRATEGY_B_MACRO] SELECTED_CAPTURE_CORRELATION_ERROR "
            "evidence_id=%s err=%r",
            evidence_id,
            exc,
        )
        selected_capture_link = {
            "selected_capture_id": None,
            "status": "SELECTED_CAPTURE_CORRELATION_ERROR",
            "reason": "CORRELATION_EXCEPTION",
        }

    persisted = persist_strategy_b_shadow(
        R,
        evidence_id=evidence_id,
        calendar_owner_id=_txt(context.get("calendar_owner_id")),
        value_id=_txt(context.get("value_id")),
        shadow_decision=shadow,
        now_ms=now_ms,
        thesis_id=evidence_id,
        selected_capture_id=selected_capture_link["selected_capture_id"],
        selected_capture_link_status=selected_capture_link["status"],
        selected_capture_link_reason=selected_capture_link["reason"],
        expected_dxy_direction=expected_dxy_direction,
    )

    # Strategy-B lifecycle bootstrap is deliberately downstream of
    # successful canonical shadow persistence. It is observation-only:
    # lifecycle failure must never rewrite the macro shadow decision or
    # escalate this service into broker/risk/order authority.
    try:
        from api.strategy_b_lifecycle_runtime_hook import (
            process_strategy_b_lifecycle_bootstrap,
        )

        _b_lifecycle_bootstrap = (
            process_strategy_b_lifecycle_bootstrap(
                R,
                evidence_id,
            )
        )

        # Publish execution intent only when the canonical ENTRY_READY
        # lifecycle bootstrap was committed or confirmed identical.
        #
        # ALREADY_ADVANCED must not recreate an old entry opportunity.
        if (
            getattr(_b_lifecycle_bootstrap, "ok", False) is True
            and _upper(
                getattr(
                    _b_lifecycle_bootstrap,
                    "action",
                    "",
                )
            )
            in {"BOOTSTRAPPED", "IDEMPOTENT"}
        ):
            _selected_key = (
                _upper(_best_symbol),
                _upper(_best_side),
            )
            _setup_entry_price = (
                h1_break_close_by_candidate.get(
                    _selected_key
                )
            )

            if (
                _setup_entry_price is not None
                and _structural_invalidation_price is not None
                and _structural_buffer_price is not None
                and expected_dxy_direction
                in {"BULLISH", "BEARISH"}
                and _best_symbol
                and _best_side in {"BUY", "SELL"}
            ):
                from api.strategy_b_entry_intent_transport import (
                    build_strategy_b_entry_intent,
                    publish_strategy_b_entry_intent,
                )

                _b_entry_intent = (
                    build_strategy_b_entry_intent(
                        thesis_id=evidence_id,
                        selected_capture_id=(
                            selected_capture_link[
                                "selected_capture_id"
                            ]
                        ),
                        symbol=_best_symbol,
                        side=_best_side,
                        setup_entry_price=(
                            _setup_entry_price
                        ),
                        structural_invalidation_price=(
                            _structural_invalidation_price
                        ),
                        structural_buffer_price=(
                            _structural_buffer_price
                        ),
                        expected_dxy_direction=(
                            expected_dxy_direction
                        ),
                        created_at_ms=now_ms,
                    )
                )

                _b_entry_publish = (
                    publish_strategy_b_entry_intent(
                        R,
                        intent=_b_entry_intent,
                    )
                )

                log.info(
                    "[STRATEGY_B_MACRO] ENTRY_INTENT "
                    "evidence_id=%s thesis=%s "
                    "capture=%s symbol=%s side=%s "
                    "setup_entry=%s dxy=%s "
                    "created=%s reason=%s",
                    evidence_id,
                    _b_entry_intent["thesis_id"],
                    _b_entry_intent[
                        "selected_capture_id"
                    ],
                    _b_entry_intent["symbol"],
                    _b_entry_intent["side"],
                    _b_entry_intent[
                        "setup_entry_price"
                    ],
                    _b_entry_intent[
                        "expected_dxy_direction"
                    ],
                    _b_entry_publish.get("created"),
                    _b_entry_publish.get("reason"),
                )
            else:
                log.warning(
                    "[STRATEGY_B_MACRO] "
                    "ENTRY_INTENT_NOT_READY "
                    "evidence_id=%s symbol=%s side=%s "
                    "setup_entry=%s invalidation=%s "
                    "buffer=%s dxy=%s",
                    evidence_id,
                    _best_symbol,
                    _best_side,
                    _setup_entry_price,
                    _structural_invalidation_price,
                    _structural_buffer_price,
                    expected_dxy_direction,
                )

    except Exception as exc:
        log.exception(
            "[STRATEGY_B_MACRO] LIFECYCLE_BOOTSTRAP_ERROR "
            "evidence_id=%s err=%r",
            evidence_id,
            exc,
        )

    return {
        "changed": bool(persisted.get("changed")),
        "derived_state": _txt(
            getattr(shadow, "derived_state", "")
        ),
        "decision": _txt(getattr(shadow, "decision", "")),
        "reason": _txt(getattr(shadow, "reason", "")),
        "blocking_component": _txt(
            getattr(shadow, "blocking_component", "")
        ) or None,
        "transition_plan_valid": bool(
            getattr(shadow, "transition_plan_valid", False)
        ),
        "macro": macro_row,
        "live_inputs": {
            "data_ok": bool(live.data_ok),
            "reason": live.reason,
            "reasons": list(live.reasons),
            "diagnostics": dict(live.diagnostics),
        },
        "pattern_observer": pattern_observer.to_dict(),
        "entry_pattern": downstream_entry.to_dict(),
        "symbol_h1_observations": list(
            symbol_h1_rows.values()
        ),
        "expression_basket": expression_row,
        "expression_ranking": downstream_expression,
        "calibration_capture": dict(calibration_capture),
        "downstream_entry_wired": True,
        "downstream_expression_wired": True,
        "downstream_risk_wired": False,
        "observation_only": True,
        "execution_authority": False,
    }


def _evaluate_evidence(
    R,
    *,
    evidence_id: str,
    dxy_inputs: Optional[Mapping[str, Any]],
    dxy_input_error: str,
    now_ms: int,
    calibration_store: Any = None,
) -> dict:
    evidence_row = _json_load(
        R.get(_evidence_key(evidence_id)),
        None,
    )
    context = _json_load(
        R.get(_context_key(evidence_id)),
        {},
    )

    if not isinstance(evidence_row, dict):
        return {
            "ok": False,
            "reason": "EVIDENCE_MISSING",
            "remove_active": True,
        }

    if not isinstance(context, dict):
        return {
            "ok": False,
            "reason": "CONTEXT_MISSING",
            "remove_active": True,
        }

    existing_macro_thesis_contract = (
        context.get("macro_thesis_contract")
        if isinstance(
            context.get("macro_thesis_contract"),
            dict,
        )
        else {}
    )

    if existing_macro_thesis_contract.get(
        "execution_dead"
    ) is True:
        dead_reason = (
            _txt(
                existing_macro_thesis_contract.get(
                    "reason"
                )
            )
            or "D1_MACRO_THESIS_EXECUTION_DEAD"
        )

        _update_context_status(
            R,
            evidence_id=evidence_id,
            status="EXECUTION_DEAD",
            reason=dead_reason,
            now_ms=now_ms,
            extra={
                "macro_thesis_contract": dict(
                    existing_macro_thesis_contract
                ),
            },
        )

        return {
            "ok": False,
            "terminal": True,
            "execution_dead": True,
            "reason": dead_reason,
            "remove_active": True,
        }

    if not isinstance(dxy_inputs, Mapping):
        _update_context_status(
            R,
            evidence_id=evidence_id,
            status="WAIT_INPUTS",
            reason=dxy_input_error or "DXY_INPUTS_UNAVAILABLE",
            now_ms=now_ms,
        )
        return {
            "ok": False,
            "reason": dxy_input_error or "DXY_INPUTS_UNAVAILABLE",
            "wait": True,
        }

    freshness = dxy_inputs.get("freshness")
    if (
        not isinstance(freshness, Mapping)
        or freshness.get("ok") is not True
    ):
        _update_context_status(
            R,
            evidence_id=evidence_id,
            status="WAIT_INPUTS",
            reason=dxy_input_error or "MANDATORY_DXY_INPUT_NOT_FRESH",
            now_ms=now_ms,
            extra={
                "freshness": (
                    dict(freshness)
                    if isinstance(freshness, Mapping)
                    else {}
                )
            },
        )
        return {
            "ok": False,
            "reason": dxy_input_error or "MANDATORY_DXY_INPUT_NOT_FRESH",
            "wait": True,
        }

    try:
        evidence = _rebuild_evidence_from_persisted(
            evidence_row
        )

        from api.strategy_b_macro_thesis_validity import (
            STATE_ACTIVE,
            STATE_INVALID,
            build_frozen_d1_contract,
            evaluate_frozen_d1_contract,
        )

        macro_for_contract, live_for_contract = (
            _evaluate_strategy_b_canonical_macro_qualification(
                evidence=evidence,
                dxy_inputs=dxy_inputs,
            )
        )

        contract = (
            dict(existing_macro_thesis_contract)
            if isinstance(
                existing_macro_thesis_contract,
                dict,
            )
            else {}
        )

        if not contract:
            d1_macro_leg = (
                live_for_contract.dxy_bias.get(
                    "strategy_b_d1_macro_leg"
                )
                if isinstance(
                    live_for_contract.dxy_bias,
                    Mapping,
                )
                and isinstance(
                    live_for_contract.dxy_bias.get(
                        "strategy_b_d1_macro_leg"
                    ),
                    Mapping,
                )
                else {}
            )

            built_contract = build_frozen_d1_contract(
                d1_macro_leg=d1_macro_leg,
                old_direction=_txt(
                    getattr(
                        macro_for_contract,
                        "old_macro_direction",
                        "",
                    )
                ),
                frozen_at_ms=_safe_int(
                    getattr(
                        macro_for_contract,
                        "catalyst_first_seen_ms",
                        0,
                    ),
                    0,
                ),
            )

            built_contract_row = built_contract.to_dict()

            if (
                built_contract.state != STATE_ACTIVE
                or built_contract.valid is not True
            ):
                _update_context_status(
                    R,
                    evidence_id=evidence_id,
                    status="WAIT_MACRO_THESIS_VALIDITY",
                    reason=built_contract.reason,
                    now_ms=now_ms,
                    extra={
                        "macro_thesis_contract_candidate":
                            built_contract_row,
                    },
                )

                return {
                    "ok": False,
                    "wait": True,
                    "reason": built_contract.reason,
                    "macro_thesis_state":
                        built_contract.state,
                }

            contract = _persist_macro_thesis_contract(
                R,
                evidence_id=evidence_id,
                contract=built_contract_row,
            )

            if not contract:
                _update_context_status(
                    R,
                    evidence_id=evidence_id,
                    status="WAIT_MACRO_THESIS_VALIDITY",
                    reason="D1_THESIS_CONTRACT_PERSIST_FAILED",
                    now_ms=now_ms,
                )

                return {
                    "ok": False,
                    "wait": True,
                    "reason":
                        "D1_THESIS_CONTRACT_PERSIST_FAILED",
                }

        d1_bars = (
            dxy_inputs.get("dxy_d1_bars")
            if isinstance(
                dxy_inputs.get("dxy_d1_bars"),
                list,
            )
            else []
        )

        validity = evaluate_frozen_d1_contract(
            contract=contract,
            d1_bars=d1_bars,
            as_of_ms=now_ms,
        )
        validity_row = validity.to_dict()

        if (
            validity.state == STATE_INVALID
            or validity.execution_dead is True
        ):
            terminal_contract = (
                _persist_macro_thesis_contract(
                    R,
                    evidence_id=evidence_id,
                    contract=validity_row,
                )
            )

            _update_context_status(
                R,
                evidence_id=evidence_id,
                status="EXECUTION_DEAD",
                reason=validity.reason,
                now_ms=now_ms,
                extra={
                    "macro_thesis_contract": (
                        terminal_contract
                        or validity_row
                    ),
                },
            )

            return {
                "ok": False,
                "terminal": True,
                "execution_dead": True,
                "reason": validity.reason,
                "remove_active": True,
            }

        if (
            validity.state != STATE_ACTIVE
            or validity.valid is not True
        ):
            _update_context_status(
                R,
                evidence_id=evidence_id,
                status="WAIT_MACRO_THESIS_VALIDITY",
                reason=validity.reason,
                now_ms=now_ms,
                extra={
                    "macro_thesis_contract":
                        validity_row,
                },
            )

            return {
                "ok": False,
                "wait": True,
                "reason": validity.reason,
                "macro_thesis_state": validity.state,
            }

        # Keep the persisted frozen identity immutable.  ACTIVE validity
        # results contain current checked_at_ms diagnostics; they do not
        # replace the original frozen contract.
        #
        # H4/H1/M15 qualification now continues through the existing
        # Strategy-B pipeline.  None of those layers can set
        # execution_dead here.

        _, build_dxy_macro_thesis = _import_intelligence()

        dxy_m15 = dxy_inputs["dxy_m15"]
        structure = (
            dxy_m15.get("structure")
            if isinstance(
                dxy_m15.get("structure"),
                Mapping,
            )
            else None
        )

        # H1 reversal proof is deliberately NOT fabricated here.
        # A later approved Strategy-B H1 transition/pattern module supplies it.
        thesis = build_dxy_macro_thesis(
            evidence,
            dxy_bias=dxy_inputs["dxy_bias"],
            dxy_m15=dxy_m15,
            dxy_structure=structure,
            h1_reversal_confirmed=False,
            h1_reversal_reason="NOT_YET_WIRED",
            as_of_ms=now_ms,
            metadata={
                "service": SERVICE_NAME,
                "calendar_owner_id": _txt(
                    context.get("calendar_owner_id")
                ),
                "value_id": _txt(
                    context.get("value_id")
                ),
                "canonical_dxy_device_id": _txt(
                    dxy_inputs.get("device_id")
                ),
                "observation_only": True,
                "execution_authority": False,
                "h1_confirmation_source": "NOT_YET_WIRED",
            },
        )

        persisted = persist_macro_thesis(
            R,
            evidence_id=evidence_id,
            calendar_owner_id=_txt(
                context.get("calendar_owner_id")
            ),
            value_id=_txt(context.get("value_id")),
            thesis=thesis,
            input_freshness=freshness,
            now_ms=now_ms,
        )

        shadow_result = _compose_and_persist_strategy_b_macro_shadow(
            R,
            evidence=evidence,
            evidence_id=evidence_id,
            context=context,
            dxy_inputs=dxy_inputs,
            now_ms=now_ms,
            calibration_store=calibration_store,
        )

        _update_context_status(
            R,
            evidence_id=evidence_id,
            status="EVALUATED",
            reason="THESIS_BUILT",
            now_ms=now_ms,
            extra={
                "state": persisted.get("state"),
                "material_changed": persisted.get("changed"),
                "canonical_dxy_device_id": _txt(
                    dxy_inputs.get("device_id")
                ),
                "shadow_state": shadow_result.get("derived_state"),
                "shadow_decision": shadow_result.get("decision"),
                "shadow_reason": shadow_result.get("reason"),
                "shadow_changed": shadow_result.get("changed"),
                "h1_shadow_reason": (
                    (shadow_result.get("macro") or {}).get(
                        "h1_reversal_reason"
                    )
                    if isinstance(shadow_result.get("macro"), Mapping)
                    else None
                ),
            },
        )

        return {
            "ok": True,
            "reason": "THESIS_BUILT",
            "state": persisted.get("state"),
            "changed": bool(
                persisted.get("changed")
                or shadow_result.get("changed")
            ),
            "shadow_state": shadow_result.get("derived_state"),
            "shadow_decision": shadow_result.get("decision"),
            "shadow_reason": shadow_result.get("reason"),
            "shadow_changed": bool(shadow_result.get("changed")),
        }

    except Exception as exc:
        reason = (
            "THESIS_BUILD_FAILED:"
            f"{type(exc).__name__}"
        )

        _update_context_status(
            R,
            evidence_id=evidence_id,
            status="ERROR",
            reason=reason,
            now_ms=now_ms,
        )

        log.exception(
            "[STRATEGY_B_MACRO] THESIS_BUILD_FAILED "
            "evidence_id=%s",
            evidence_id,
        )

        return {
            "ok": False,
            "reason": reason,
            "wait": True,
        }


# ---------------------------------------------------------------------------
# New release consumption
# ---------------------------------------------------------------------------

def _consume_one_release(
    R,
    *,
    calendar_owner_id: str,
    value_id: str,
    index_score: int,
    dxy_inputs: Optional[Mapping[str, Any]],
    dxy_input_error: str,
    now_ms: int,
) -> dict:
    release, err = _load_canonical_release(
        R,
        calendar_owner_id=calendar_owner_id,
        value_id=value_id,
    )

    if not isinstance(release, dict):
        _deadletter_release(
            R,
            calendar_owner_id=calendar_owner_id,
            value_id=value_id,
            reason=err or "RELEASE_INVALID",
            index_score=index_score,
        )
        return {
            "ok": False,
            "terminal": True,
            "reason": err or "RELEASE_INVALID",
        }

    event = release.get("event")
    if not isinstance(event, dict):
        _deadletter_release(
            R,
            calendar_owner_id=calendar_owner_id,
            value_id=value_id,
            reason="RELEASE_EVENT_INVALID",
            index_score=index_score,
        )
        return {
            "ok": False,
            "terminal": True,
            "reason": "RELEASE_EVENT_INVALID",
        }

    try:
        build_mt5_event_evidence, _ = _import_intelligence()

        evidence = build_mt5_event_evidence(
            event,
            calendar_owner_id=calendar_owner_id,
            first_result_seen_ms=_safe_int(
                release.get("first_result_seen_ms"),
                index_score,
            ),
            metadata={
                "service": SERVICE_NAME,
                "release_key": _release_key(
                    calendar_owner_id,
                    value_id,
                ),
                "schedule_match": release.get(
                    "schedule_match"
                ),
                "schedule_check_reason": release.get(
                    "schedule_check_reason"
                ),
                "server_received_ms": release.get(
                    "server_received_ms"
                ),
                "observation_only": True,
                "execution_authority": False,
            },
        )

        _persist_evidence_and_context(
            R,
            evidence=evidence,
            calendar_owner_id=calendar_owner_id,
            value_id=value_id,
            release_record=release,
            index_score=index_score,
        )

        evaluation = _evaluate_evidence(
            R,
            evidence_id=evidence.evidence_id,
            dxy_inputs=dxy_inputs,
            dxy_input_error=dxy_input_error,
            now_ms=now_ms,
        )

        return {
            "ok": True,
            "reason": "EVIDENCE_PERSISTED",
            "evidence_id": evidence.evidence_id,
            "evaluation": evaluation,
        }

    except Exception as exc:
        reason = (
            "EVIDENCE_BUILD_OR_PERSIST_FAILED:"
            f"{type(exc).__name__}"
        )

        log.exception(
            "[STRATEGY_B_MACRO] RELEASE_CONSUME_FAILED "
            "owner=%s value_id=%s",
            calendar_owner_id,
            value_id,
        )

        # Do NOT mark terminal. Do NOT advance cursor.
        return {
            "ok": False,
            "terminal": False,
            "reason": reason,
        }


def _consume_new_releases_for_owner(
    R,
    *,
    calendar_owner_id: str,
    dxy_inputs: Optional[Mapping[str, Any]],
    dxy_input_error: str,
    now_ms: int,
) -> dict:
    stats = {
        "owner": calendar_owner_id,
        "seen": 0,
        "consumed": 0,
        "deadletter": 0,
        "errors": 0,
    }

    rows = _fetch_releases_after_cursor(
        R,
        calendar_owner_id,
        limit=RELEASE_BATCH_PER_OWNER,
    )

    for value_id, score in rows:
        if not value_id:
            continue

        stats["seen"] += 1

        result = _consume_one_release(
            R,
            calendar_owner_id=calendar_owner_id,
            value_id=value_id,
            index_score=score,
            dxy_inputs=dxy_inputs,
            dxy_input_error=dxy_input_error,
            now_ms=now_ms,
        )

        if result.get("ok"):
            stats["consumed"] += 1

            # Evidence is now safely persisted and tracked, so cursor can move
            # even when current DXY inputs are temporarily unavailable.
            _save_release_cursor(
                R,
                calendar_owner_id,
                score=score,
                member=value_id,
            )
            continue

        if result.get("terminal"):
            stats["deadletter"] += 1

            # Canonical release itself is malformed/missing. Deadletter avoids
            # permanently blocking later releases for the same owner.
            _save_release_cursor(
                R,
                calendar_owner_id,
                score=score,
                member=value_id,
            )
            continue

        # Persistence/internal failure: stop here and retry same item next tick.
        stats["errors"] += 1
        break

    return stats


# ---------------------------------------------------------------------------
# Re-evaluation of tracked evidence
# ---------------------------------------------------------------------------

def _trim_active(R, now_ms: int) -> int:
    """
    ACTIVE Strategy-B macro-thesis membership is structure-owned.

    Catalyst/release age is not an execution invalidation condition.
    Terminal removal is performed by _evaluate_evidence() when the
    frozen macro-thesis contract becomes execution-dead.

    Keep this compatibility hook so existing tick statistics and call
    sites remain unchanged.  It is intentionally non-destructive.
    """
    return 0


def _active_round_robin_batch(R) -> list[str]:
    total = _safe_int(R.zcard(ACTIVE_KEY), 0)
    if total <= 0:
        try:
            R.set(ACTIVE_RR_CURSOR_KEY, "0")
        except Exception:
            pass
        return []

    start = _safe_int(
        R.get(ACTIVE_RR_CURSOR_KEY),
        0,
    )
    if start < 0 or start >= total:
        start = 0

    count = max(1, min(ACTIVE_BATCH, total))

    rows = R.zrange(
        ACTIVE_KEY,
        start,
        min(total - 1, start + count - 1),
    ) or []

    # Wrap once if needed.
    if len(rows) < count and total > len(rows):
        remaining = count - len(rows)
        wrap = R.zrange(
            ACTIVE_KEY,
            0,
            remaining - 1,
        ) or []
        rows.extend(wrap)

    next_start = (start + len(rows)) % max(1, total)
    R.set(
        ACTIVE_RR_CURSOR_KEY,
        str(next_start),
        ex=PERSIST_TTL_SEC,
    )

    # Preserve order while removing accidental duplicates from wrap.
    out: list[str] = []
    seen: set[str] = set()

    for item in rows:
        item_s = _txt(item)
        if item_s and item_s not in seen:
            seen.add(item_s)
            out.append(item_s)

    return out


def _reevaluate_tracked(
    R,
    *,
    dxy_inputs: Optional[Mapping[str, Any]],
    dxy_input_error: str,
    now_ms: int,
) -> dict:
    stats = {
        "checked": 0,
        "evaluated": 0,
        "changed": 0,
        "wait": 0,
        "removed": 0,
        "errors": 0,
    }

    # Reuse one process-local calibration index across macro reevaluation
    # batches. The first use performs the historical JSONL load; subsequent
    # batches reuse the already-built dedupe indexes instead of reparsing the
    # complete calibration file.
    global _MACRO_CALIBRATION_STORE

    if _MACRO_CALIBRATION_STORE is None:
        from api.strategy_b_calibration_jsonl import (
            StrategyBCalibrationJsonlStore,
        )
        from api.strategy_b_live_calibration_runtime import (
            strategy_b_calibration_jsonl_path,
        )

        _calibration_path = strategy_b_calibration_jsonl_path()
        _load_started_ms = int(time.time() * 1000)

        _MACRO_CALIBRATION_STORE = StrategyBCalibrationJsonlStore(
            _calibration_path
        )

        log.info(
            "[STRATEGY_B_MACRO] CALIBRATION_STORE_READY "
            "path=%s load_ms=%s",
            _calibration_path,
            int(time.time() * 1000) - _load_started_ms,
        )

    calibration_store = _MACRO_CALIBRATION_STORE

    for evidence_id in _active_round_robin_batch(R):
        stats["checked"] += 1

        result = _evaluate_evidence(
            R,
            evidence_id=evidence_id,
            dxy_inputs=dxy_inputs,
            dxy_input_error=dxy_input_error,
            now_ms=now_ms,
            calibration_store=calibration_store,
        )

        if result.get("remove_active"):
            try:
                R.zrem(ACTIVE_KEY, evidence_id)
                stats["removed"] += 1
            except Exception:
                stats["errors"] += 1
            continue

        # C4: execution lifetime is owned by structural validity, not age.
        #
        # A nonterminal evidence item remains part of the active structural
        # reevaluation set.  Refresh only operational persistence TTLs;
        # never mutate the ACTIVE ZSET score because it preserves the
        # original first_result_seen_ms / catalyst ordering.
        try:
            pipe = R.pipeline(transaction=False)
            pipe.expire(
                _evidence_key(evidence_id),
                PERSIST_TTL_SEC,
            )
            pipe.expire(
                _context_key(evidence_id),
                PERSIST_TTL_SEC,
            )
            pipe.expire(
                ACTIVE_KEY,
                PERSIST_TTL_SEC,
            )
            pipe.execute()
        except Exception:
            stats["errors"] += 1

        if result.get("ok"):
            stats["evaluated"] += 1
            if result.get("changed"):
                stats["changed"] += 1
        elif result.get("wait"):
            stats["wait"] += 1
        else:
            stats["errors"] += 1

    return stats



# ---------------------------------------------------------------------------
# Strategy-B selected calibration-capture correlation
# ---------------------------------------------------------------------------

def _resolve_strategy_b_selected_capture_link(
    *,
    shadow_decision,
    calibration_capture,
    prior_shadow,
) -> dict:
    """
    Pure correlation of an ENTRY_READY shadow expression to its causal
    calibration snapshot.

    This function has no Redis, broker, execution, risk-reservation, or
    Strategy-A authority. It never changes the trading decision.

    A selected capture may be latched only for a genuinely ENTRY_READY /
    ENTRY_READY_SHADOW decision with a valid transition plan. An already
    persisted selected_capture_id is immutable.
    """

    def _field(obj, name, default=None):
        if isinstance(obj, Mapping):
            return obj.get(name, default)
        return getattr(obj, name, default)

    prior = prior_shadow if isinstance(prior_shadow, Mapping) else {}

    prior_capture_id = _txt(prior.get("selected_capture_id"))
    if prior_capture_id:
        return {
            "selected_capture_id": prior_capture_id,
            "status": (
                _txt(prior.get("selected_capture_link_status"))
                or "LINKED"
            ),
            "reason": (
                _txt(prior.get("selected_capture_link_reason"))
                or "SELECTED_CAPTURE_ALREADY_LINKED"
            ),
        }

    derived_state = _upper(
        _field(shadow_decision, "derived_state", "")
    )
    decision = _upper(
        _field(shadow_decision, "decision", "")
    )
    transition_plan_valid = (
        _field(shadow_decision, "transition_plan_valid", False) is True
    )
    selected_symbol = _upper(
        _field(shadow_decision, "selected_symbol", "")
    )
    selected_side = _upper(
        _field(shadow_decision, "selected_side", "")
    )

    entry_ready = (
        derived_state == "ENTRY_READY"
        and decision == "ENTRY_READY_SHADOW"
        and transition_plan_valid
        and bool(selected_symbol)
        and bool(selected_side)
    )

    if not entry_ready:
        return {
            "selected_capture_id": None,
            "status": "NOT_APPLICABLE",
            "reason": "SHADOW_NOT_ENTRY_READY",
        }

    capture = (
        calibration_capture
        if isinstance(calibration_capture, Mapping)
        else {}
    )

    if capture.get("ok") is not True:
        return {
            "selected_capture_id": None,
            "status": "CALIBRATION_CAPTURE_UNAVAILABLE",
            "reason": (
                _txt(capture.get("reason"))
                or "CALIBRATION_CAPTURE_UNAVAILABLE"
            ),
        }

    rows = capture.get("rows")
    if not isinstance(rows, (list, tuple)):
        rows = []

    matches = []

    for row in rows:
        if not isinstance(row, Mapping):
            continue

        row_symbol = _upper(row.get("symbol"))
        row_side = _upper(row.get("side"))
        capture_id = _txt(row.get("capture_id"))

        if (
            row_symbol == selected_symbol
            and row_side == selected_side
            and capture_id
        ):
            matches.append(capture_id)

    if len(matches) == 1:
        return {
            "selected_capture_id": matches[0],
            "status": "LINKED",
            "reason": "SELECTED_CAPTURE_MATCHED",
        }

    if not matches:
        return {
            "selected_capture_id": None,
            "status": "SELECTED_CAPTURE_NOT_FOUND",
            "reason": "NO_CAPTURE_ROW_MATCHED_SELECTED_EXPRESSION",
        }

    return {
        "selected_capture_id": None,
        "status": "SELECTED_CAPTURE_AMBIGUOUS",
        "reason": "MULTIPLE_CAPTURE_ROWS_MATCHED_SELECTED_EXPRESSION",
    }


# ---------------------------------------------------------------------------
# Strategy-B composed shadow persistence hooks
# ---------------------------------------------------------------------------

def persist_strategy_b_shadow(
    R,
    *,
    evidence_id: str,
    calendar_owner_id: str,
    value_id: str,
    shadow_decision,
    now_ms: int,
    thesis_id: Optional[str] = None,
    selected_capture_id: Optional[str] = None,
    selected_capture_link_status: Optional[str] = None,
    selected_capture_link_reason: Optional[str] = None,
    expected_dxy_direction: Optional[str] = None,
) -> dict:
    """
    Persist one already-composed Strategy-B shadow decision.

    Additive integration only:
    - no broker/order authority;
    - no risk reservation;
    - no Strategy-A mutation;
    - no change to current macro-thesis evaluation semantics.
    """
    from api.strategy_b_shadow_persistence import (
        persist_strategy_b_shadow_decision,
    )

    return persist_strategy_b_shadow_decision(
        R,
        evidence_id=evidence_id,
        calendar_owner_id=calendar_owner_id,
        value_id=value_id,
        shadow_decision=shadow_decision,
        now_ms=now_ms,
        thesis_id=thesis_id,
        selected_capture_id=selected_capture_id,
        selected_capture_link_status=selected_capture_link_status,
        selected_capture_link_reason=selected_capture_link_reason,
        expected_dxy_direction=expected_dxy_direction,
        ttl_sec=PERSIST_TTL_SEC,
        max_history=MAX_HISTORY,
    )


def reconstruct_strategy_b_shadow(
    R,
    *,
    evidence_id: str,
    history_limit: int = MAX_HISTORY,
) -> dict:
    """
    Exact-key reconstruction of one composed Strategy-B shadow state.

    Deliberately separate from reconstruct_strategy_b_macro_state(), which is
    a global service summary. This function never SCANs Redis.
    """
    from api.strategy_b_shadow_persistence import (
        reconstruct_strategy_b_shadow_state,
    )

    return reconstruct_strategy_b_shadow_state(
        R,
        evidence_id=evidence_id,
        history_limit=history_limit,
    )


# ---------------------------------------------------------------------------
# Restart/reconstruction view
# ---------------------------------------------------------------------------

def reconstruct_strategy_b_macro_state(R) -> dict:
    """
    Read-only reconstruction summary from explicit indexes.

    No SCAN. This does not need to recreate ephemeral Python objects because
    evidence/context/latest thesis are already Redis-persisted and exact-keyed.
    """
    now_ms = _now_ms()

    owners = sorted(
        _txt(x)
        for x in (R.smembers(RELEASE_OWNERS_KEY) or set())
        if _txt(x)
    )

    active_count = _safe_int(
        R.zcard(ACTIVE_KEY),
        0,
    )

    heartbeat = _json_load(
        R.get(HEARTBEAT_KEY),
        {},
    )

    return {
        "schema_version": SCHEMA_VERSION,
        "service": SERVICE_NAME,
        "mode": MODE,
        "reconstructed_at_ms": now_ms,
        "release_owner_count": len(owners),
        "release_owners": owners,
        "active_evidence_count": active_count,
        "heartbeat": (
            heartbeat
            if isinstance(heartbeat, dict)
            else {}
        ),
        "observation_only": True,
        "execution_authority": False,
        "uses_redis_scan": False,
    }

def _load_phase7_real_dxy_m15_bars(
    *,
    device_id: str,
) -> tuple[list[dict], str]:
    """
    One bounded canonical REAL_DXY M15 load per Strategy-B tick.

    Research-only. No synthetic fallback, Redis SCAN, execution authority,
    or Strategy-A mutation.
    """
    try:
        from api.xtl_analytics import load_real_dxy_bars
    except Exception as exc:
        return [], (
            "PHASE7_DXY_M15_IMPORT_FAILED:"
            f"{type(exc).__name__}"
        )

    try:
        bars = (
            load_real_dxy_bars(
                device_id=device_id,
                tf="M15",
                max_bars=300,
            )
            or []
        )
    except Exception as exc:
        return [], (
            "PHASE7_DXY_M15_LOAD_FAILED:"
            f"{type(exc).__name__}"
        )

    clean = []

    for bar in bars:
        if not isinstance(bar, Mapping):
            continue

        if bar.get("complete") is not True:
            continue

        open_ms = _safe_int(bar.get("t_open_ms"), 0)
        close_ms = _safe_int(bar.get("t_close_ms"), 0)

        if open_ms <= 0 or close_ms <= open_ms:
            continue

        clean.append(dict(bar))

    clean.sort(
        key=lambda row: _safe_int(
            row.get("t_close_ms"),
            0,
        )
    )

    if not clean:
        return [], "PHASE7_DXY_M15_BARS_EMPTY"

    return clean, ""

def _capture_phase7_schedule_checkpoints(
    R,
    *,
    calendar_owner_id: str,
    value_id: str,
    frozen_event: Mapping,
    dxy_device_id: str,
    m15_bars: list[dict],
    now_ms: int,
) -> dict:
    """
    Freeze raw causal REAL_DXY observations for the 1st, 2nd and 4th
    full completed M15 candles after scheduled event T0.

    No directional classification is performed here.
    """
    out = {
        "due": 0,
        "frozen": 0,
        "existing": 0,
        "errors": 0,
    }

    owner = _txt(calendar_owner_id)
    value = _txt(value_id)
    event_time_ms = _safe_int(
        frozen_event.get("event_time_utc_ms"),
        0,
    )

    if (
        not owner
        or not value
        or not dxy_device_id
        or event_time_ms <= 0
    ):
        out["errors"] += 1
        return out

    # -------------------------------------------------------------
    # Immutable pre-event REAL_DXY M15 baseline.
    #
    # Select the latest fully completed candle whose close was
    # knowable at or before scheduled event T0.
    #
    # This preserves the causal reference price after the rolling
    # canonical M15 history has expired.
    # -------------------------------------------------------------
    baseline_candidates = [
        dict(bar)
        for bar in (m15_bars or [])
        if (
            isinstance(bar, Mapping)
            and bar.get("complete") is True
            and _safe_int(bar.get("t_open_ms"), 0) > 0
            and _safe_int(bar.get("t_close_ms"), 0)
                > _safe_int(bar.get("t_open_ms"), 0)
            and _safe_int(bar.get("t_close_ms"), 0)
                <= event_time_ms
        )
    ]

    baseline_candidates.sort(
        key=lambda row: _safe_int(
            row.get("t_close_ms"),
            0,
        )
    )

    if baseline_candidates:
        baseline_bar = baseline_candidates[-1]

        baseline_open_ms = _safe_int(
            baseline_bar.get("t_open_ms"),
            0,
        )
        baseline_close_ms = _safe_int(
            baseline_bar.get("t_close_ms"),
            0,
        )

        # Additional causal guard. Normally guaranteed because
        # baseline_close_ms <= event_time_ms <= now_ms for a due event.
        if (
            baseline_close_ms > 0
            and baseline_close_ms <= event_time_ms
            and baseline_close_ms <= int(now_ms)
        ):
            baseline_key = (
                _schedule_checkpoint_baseline_key(
                    owner,
                    value,
                )
            )

            baseline_payload = {
                "schema_version": SCHEMA_VERSION,
                "source": "REAL_DXY",
                "research_source":
                    "STRATEGY_B_PHASE7_EVENT_EXPRESSION",
                "calendar_owner_id": owner,
                "event_id": _txt(
                    frozen_event.get("event_id")
                ),
                "value_id": value,
                "event_name": _txt(
                    frozen_event.get("event_name")
                ),
                "currency": _upper(
                    frozen_event.get("currency")
                ),
                "importance": _upper(
                    frozen_event.get("importance")
                ),
                "event_time_utc_ms": event_time_ms,
                "checkpoint": "EVENT_BASELINE",
                "checkpoint_ordinal": 0,
                "dxy_device_id": dxy_device_id,
                "bar_open_ms": baseline_open_ms,
                "bar_close_ms": baseline_close_ms,
                "captured_at_ms": int(now_ms),
                "capture_latency_ms": max(
                    0,
                    int(now_ms) - baseline_close_ms,
                ),
                "dxy_bar": {
                    "o": baseline_bar.get("o"),
                    "h": baseline_bar.get("h"),
                    "l": baseline_bar.get("l"),
                    "c": baseline_bar.get("c"),
                    "v": baseline_bar.get("v"),
                },
                "observation_only": True,
                "execution_authority": False,
                "strategy_a_mutation": False,
            }

            try:
                baseline_created = bool(
                    R.set(
                        baseline_key,
                        _json_dump(
                            baseline_payload
                        ),
                        nx=True,
                        ex=SCHEDULE_CHECKPOINT_TTL_SEC,
                    )
                )

                if baseline_created:
                    log.info(
                        "[STRATEGY_B_PHASE7] "
                        "CHECKPOINT_BASELINE_FROZEN "
                        "owner=%s value_id=%s "
                        "bar_close_ms=%s",
                        owner,
                        value,
                        baseline_close_ms,
                    )

            except Exception as exc:
                out["errors"] += 1

                log.warning(
                    "[STRATEGY_B_PHASE7] "
                    "CHECKPOINT_BASELINE_FREEZE_FAILED "
                    "owner=%s value_id=%s err=%r",
                    owner,
                    value,
                    exc,
                )

    # Only candles whose entire interval starts at/after T0.
    post_bars = [
        dict(bar)
        for bar in (m15_bars or [])
        if (
            isinstance(bar, Mapping)
            and bar.get("complete") is True
            and _safe_int(bar.get("t_open_ms"), 0)
                >= event_time_ms
            and _safe_int(bar.get("t_close_ms"), 0)
                > _safe_int(bar.get("t_open_ms"), 0)
        )
    ]

    post_bars.sort(
        key=lambda row: _safe_int(
            row.get("t_close_ms"),
            0,
        )
    )

    for ordinal in SCHEDULE_CHECKPOINT_ORDINALS:
        index = int(ordinal) - 1

        if len(post_bars) <= index:
            continue

        bar = post_bars[index]
        bar_close_ms = _safe_int(
            bar.get("t_close_ms"),
            0,
        )

        # Causal boundary: never freeze before that candle was knowable.
        if bar_close_ms <= 0 or bar_close_ms > int(now_ms):
            continue

        out["due"] += 1

        key = _schedule_checkpoint_key(
            owner,
            value,
            ordinal,
        )

        payload = {
            "schema_version": SCHEMA_VERSION,
            "source": "REAL_DXY",
            "research_source":
                "STRATEGY_B_PHASE7_EVENT_EXPRESSION",
            "calendar_owner_id": owner,
            "event_id": _txt(
                frozen_event.get("event_id")
            ),
            "value_id": value,
            "event_name": _txt(
                frozen_event.get("event_name")
            ),
            "currency": _upper(
                frozen_event.get("currency")
            ),
            "importance": _upper(
                frozen_event.get("importance")
            ),
            "event_time_utc_ms": event_time_ms,
            "checkpoint":
                f"POST_M15_{int(ordinal)}",
            "checkpoint_ordinal": int(ordinal),
            "dxy_device_id": dxy_device_id,
            "bar_open_ms": _safe_int(
                bar.get("t_open_ms"),
                0,
            ),
            "bar_close_ms": bar_close_ms,
            "captured_at_ms": int(now_ms),
            "capture_latency_ms": max(
                0,
                int(now_ms) - bar_close_ms,
            ),
            "dxy_bar": {
                "o": bar.get("o"),
                "h": bar.get("h"),
                "l": bar.get("l"),
                "c": bar.get("c"),
                "v": bar.get("v"),
            },
            "observation_only": True,
            "execution_authority": False,
            "strategy_a_mutation": False,
        }

        try:
            created = bool(
                R.set(
                    key,
                    _json_dump(payload),
                    nx=True,
                    ex=SCHEDULE_CHECKPOINT_TTL_SEC,
                )
            )

            if created:
                out["frozen"] += 1

                log.info(
                    "[STRATEGY_B_PHASE7] "
                    "CHECKPOINT_FROZEN "
                    "owner=%s value_id=%s "
                    "checkpoint=POST_M15_%s "
                    "bar_close_ms=%s",
                    owner,
                    value,
                    ordinal,
                    bar_close_ms,
                )
            else:
                out["existing"] += 1

        except Exception as exc:
            out["errors"] += 1

            log.warning(
                "[STRATEGY_B_PHASE7] "
                "CHECKPOINT_FREEZE_FAILED "
                "owner=%s value_id=%s "
                "checkpoint=POST_M15_%s err=%r",
                owner,
                value,
                ordinal,
                exc,
            )

    return out

def _process_phase7_due_schedule_checkpoints(
    R,
    *,
    dxy_device_id: str,
    now_ms: int,
) -> dict:
    """
    Process only Strategy-B scheduled events near T0.

    Uses the explicit Phase-7 schedule-event ZSET.
    Loads canonical REAL_DXY M15 bars at most once for the whole batch.

    Observation/research only:
      - no Redis SCAN
      - no synthetic DXY fallback
      - no direction classification
      - no execution/risk authority
      - no Strategy-A mutation
    """
    out = {
        "events_due": 0,
        "events_loaded": 0,
        "events_missing": 0,
        "m15_loaded": 0,
        "checkpoint_due": 0,
        "checkpoint_frozen": 0,
        "checkpoint_existing": 0,
        "errors": 0,
    }

    device_id = _txt(dxy_device_id)

    if not device_id:
        out["errors"] += 1
        return out

    min_score = (
        int(now_ms)
        - SCHEDULE_CHECKPOINT_LOOKBACK_MS
    )
    max_score = (
        int(now_ms)
        + SCHEDULE_CHECKPOINT_LOOKAHEAD_MS
    )

    try:
        members = R.zrangebyscore(
            SCHEDULE_EVENT_INDEX_KEY,
            min_score,
            max_score,
            start=0,
            num=max(
                1,
                SCHEDULE_CHECKPOINT_EVENT_BATCH,
            ),
        ) or []
    except Exception as exc:
        out["errors"] += 1

        log.warning(
            "[STRATEGY_B_PHASE7] "
            "CHECKPOINT_DUE_INDEX_READ_FAILED err=%r",
            exc,
        )

        return out

    decoded_members = [
        _txt(member)
        for member in members
        if _txt(member)
    ]

    out["events_due"] = len(decoded_members)

    if not decoded_members:
        return out

    # Exactly one bounded canonical M15 load for the entire due-event batch.
    m15_bars, m15_err = (
        _load_phase7_real_dxy_m15_bars(
            device_id=device_id,
        )
    )

    if not m15_bars:
        out["errors"] += 1

        log.warning(
            "[STRATEGY_B_PHASE7] "
            "CHECKPOINT_M15_UNAVAILABLE "
            "device_id=%s reason=%s",
            device_id,
            m15_err or "UNKNOWN",
        )

        return out

    out["m15_loaded"] = len(m15_bars)

    for member in decoded_members:
        if ":" not in member:
            out["errors"] += 1

            log.warning(
                "[STRATEGY_B_PHASE7] "
                "CHECKPOINT_INDEX_MEMBER_INVALID "
                "member=%s",
                member,
            )

            continue

        owner, value_id = member.split(":", 1)

        owner = _txt(owner)
        value_id = _txt(value_id)

        if not owner or not value_id:
            out["errors"] += 1
            continue

        try:
            frozen_event = _json_load(
                R.get(
                    _schedule_event_key(
                        owner,
                        value_id,
                    )
                ),
                {},
            )
        except Exception as exc:
            out["errors"] += 1

            log.warning(
                "[STRATEGY_B_PHASE7] "
                "CHECKPOINT_EVENT_READ_FAILED "
                "owner=%s value_id=%s err=%r",
                owner,
                value_id,
                exc,
            )

            continue

        if not isinstance(frozen_event, Mapping):
            out["events_missing"] += 1
            continue

        # Fail closed on identity mismatch.
        if (
            _txt(
                frozen_event.get(
                    "calendar_owner_id"
                )
            )
            != owner
            or _txt(
                frozen_event.get("value_id")
            )
            != value_id
        ):
            out["errors"] += 1

            log.warning(
                "[STRATEGY_B_PHASE7] "
                "CHECKPOINT_EVENT_IDENTITY_REJECT "
                "owner=%s value_id=%s",
                owner,
                value_id,
            )

            continue

        out["events_loaded"] += 1

        result = (
            _capture_phase7_schedule_checkpoints(
                R,
                calendar_owner_id=owner,
                value_id=value_id,
                frozen_event=frozen_event,
                dxy_device_id=device_id,
                m15_bars=m15_bars,
                now_ms=now_ms,
            )
        )

        out["checkpoint_due"] += int(
            result.get("due") or 0
        )
        out["checkpoint_frozen"] += int(
            result.get("frozen") or 0
        )
        out["checkpoint_existing"] += int(
            result.get("existing") or 0
        )
        out["errors"] += int(
            result.get("errors") or 0
        )

    return out

def _phase7_full_post_event_m15_bars(
    bars: list,
    *,
    event_time_ms: int,
) -> list:
    """
    Return completed REAL_DXY M15 bars whose entire interval is
    causally after the scheduled event.

    No pre-event portion of a candle is allowed.
    """
    event_ms = int(event_time_ms or 0)

    out = []

    for bar in bars or []:
        if not isinstance(bar, Mapping):
            continue

        if bar.get("complete") is not True:
            continue

        open_ms = _safe_int(
            bar.get("t_open_ms"),
            0,
        )
        close_ms = _safe_int(
            bar.get("t_close_ms"),
            0,
        )

        if (
            open_ms <= 0
            or close_ms <= open_ms
        ):
            continue

        # Entire candle must start at/after T0.
        if open_ms < event_ms:
            continue

        out.append(dict(bar))

    out.sort(
        key=lambda x: _safe_int(
            x.get("t_close_ms"),
            0,
        )
    )

    return out


def _freeze_strategy_b_schedule_events_for_owner(
    R,
    *,
    calendar_owner_id: str,
    now_ms: int,
) -> dict:
    """
    Phase-7 observation-only scheduled-event discovery.

    Reads one exact canonical MT5 calendar snapshot and freezes genuine
    upcoming HIGH-impact event identity into a Strategy-B research namespace.

    This function:
      - does not build MacroEvidence
      - does not infer direction
      - does not consume MT5 result releases
      - does not mutate Strategy-A
      - has no broker/order/risk authority
      - does not SCAN Redis
    """
    out = {
        "seen": 0,
        "eligible": 0,
        "frozen": 0,
        "existing": 0,
        "errors": 0,
    }

    owner = _txt(calendar_owner_id)
    if not owner:
        out["errors"] += 1
        return out

    try:
        snapshot = _json_load(
            R.get(_schedule_key(owner)),
            {},
        )
    except Exception as exc:
        out["errors"] += 1
        log.warning(
            "[STRATEGY_B_PHASE7] SCHEDULE_READ_FAILED "
            "owner=%s err=%r",
            owner,
            exc,
        )
        return out

    if not isinstance(snapshot, Mapping):
        out["errors"] += 1
        return out

    if _upper(snapshot.get("source")) != "MT5_CALENDAR":
        out["errors"] += 1
        log.warning(
            "[STRATEGY_B_PHASE7] SCHEDULE_SOURCE_REJECT "
            "owner=%s source=%s",
            owner,
            _txt(snapshot.get("source")),
        )
        return out

    snapshot_owner = _txt(
        snapshot.get("calendar_owner_id")
    )
    if snapshot_owner != owner:
        out["errors"] += 1
        log.warning(
            "[STRATEGY_B_PHASE7] SCHEDULE_OWNER_REJECT "
            "expected=%s actual=%s",
            owner,
            snapshot_owner,
        )
        return out

    events = snapshot.get("events")
    if not isinstance(events, list):
        out["errors"] += 1
        return out

    # Hard runtime bound even if a malformed/oversized snapshot reaches Redis.
    events = events[:max(1, SCHEDULE_EVENTS_PER_OWNER)]

    max_event_ms = int(now_ms) + max(
        0,
        SCHEDULE_LOOKAHEAD_MS,
    )

    for event in events:
        out["seen"] += 1

        if not isinstance(event, Mapping):
            continue

        event_id = _txt(event.get("event_id"))
        value_id = _txt(event.get("value_id"))
        event_name = _txt(event.get("event_name"))
        currency = _upper(event.get("currency"))
        importance = _upper(event.get("importance"))
        event_time_ms = _safe_int(
            event.get("event_time_utc_ms"),
            0,
        )

        # Phase-7 clean sample starts with major scheduled events only.
        if importance != "HIGH":
            continue

        if (
            not event_id
            or not value_id
            or not event_name
            or not currency
            or event_time_ms <= 0
        ):
            continue

        # Do not create new research observations for already-past events.
        # Once frozen, however, the immutable record survives schedule rollover.
        if event_time_ms < int(now_ms):
            continue

        if event_time_ms > max_event_ms:
            continue

        out["eligible"] += 1

        frozen = {
            "schema_version": SCHEMA_VERSION,
            "source": "MT5_CALENDAR",
            "research_source": "STRATEGY_B_PHASE7_SCHEDULE",
            "calendar_owner_id": owner,
            "event_id": event_id,
            "value_id": value_id,
            "event_name": event_name,
            "currency": currency,
            "importance": importance,
            "event_time_utc_ms": event_time_ms,
            "first_schedule_seen_ms": int(now_ms),
            "calendar_collected_at_ms": _safe_int(
                snapshot.get("collected_at_ms"),
                0,
            ),
            "event": dict(event),
            "observation_only": True,
            "execution_authority": False,
            "strategy_a_mutation": False,
        }

        key = _schedule_event_key(
            owner,
            value_id,
        )

        try:
            created = bool(
                R.set(
                    key,
                    _json_dump(frozen),
                    nx=True,
                    ex=SCHEDULE_RESEARCH_RETENTION_SEC,
                )
            )

            if created:
                # Explicit due-time index for later checkpoint processing.
                # No runtime SCAN will be required.
                R.zadd(
                    SCHEDULE_EVENT_INDEX_KEY,
                    {
                        f"{owner}:{value_id}": event_time_ms
                    },
                )
                out["frozen"] += 1

                log.info(
                    "[STRATEGY_B_PHASE7] SCHEDULE_EVENT_FROZEN "
                    "owner=%s value_id=%s event_id=%s "
                    "currency=%s importance=%s event_time_ms=%s",
                    owner,
                    value_id,
                    event_id,
                    currency,
                    importance,
                    event_time_ms,
                )
            else:
                out["existing"] += 1

        except Exception as exc:
            out["errors"] += 1
            log.warning(
                "[STRATEGY_B_PHASE7] SCHEDULE_EVENT_FREEZE_FAILED "
                "owner=%s value_id=%s err=%r",
                owner,
                value_id,
                exc,
            )

    # Keep the explicit research index bounded to retained observations.
    try:
        cutoff_ms = int(now_ms) - (
            SCHEDULE_RESEARCH_RETENTION_SEC * 1000
        )
        R.zremrangebyscore(
            SCHEDULE_EVENT_INDEX_KEY,
            "-inf",
            cutoff_ms,
        )
        R.expire(
            SCHEDULE_EVENT_INDEX_KEY,
            SCHEDULE_RESEARCH_RETENTION_SEC,
        )
    except Exception as exc:
        out["errors"] += 1
        log.warning(
            "[STRATEGY_B_PHASE7] SCHEDULE_INDEX_TRIM_FAILED "
            "owner=%s err=%r",
            owner,
            exc,
        )

    return out

# ---------------------------------------------------------------------------
# Public tick
# ---------------------------------------------------------------------------

def tick_strategy_b_macro_observer(
    R,
    now_ms: Optional[int] = None,
) -> dict:
    """
    One bounded observation tick.

    Safe for every uvicorn worker to call: a short Redis NX lock makes the
    global Strategy-B observer single-active across workers.
    """
    started_ms = int(now_ms or _now_ms())

    stats = {
        "schema_version": SCHEMA_VERSION,
        "service": SERVICE_NAME,
        "mode": MODE,
        "started_at_ms": started_ms,
        "lock": False,
        "owners": 0,
        "release_seen": 0,
        "release_consumed": 0,
        "release_deadletter": 0,
        "release_errors": 0,
        "active_trimmed": 0,
        "active_checked": 0,
        "active_evaluated": 0,
        "active_changed": 0,
        "active_wait": 0,
        "active_removed": 0,
        "active_errors": 0,
        "dxy_input_ok": False,
        "dxy_input_reason": "",
        "canonical_dxy_device_id": None,
        "observation_only": True,
        "execution_authority": False,
        "strategy_a_mutation": False,
        "schedule_owners": 0,
        "schedule_seen": 0,
        "schedule_eligible": 0,
        "schedule_frozen": 0,
        "schedule_existing": 0,
        "schedule_errors": 0,
        "checkpoint_events_due": 0,
        "checkpoint_events_loaded": 0,
        "checkpoint_events_missing": 0,
        "checkpoint_m15_loaded": 0,
        "checkpoint_due": 0,
        "checkpoint_frozen": 0,
        "checkpoint_existing": 0,
        "checkpoint_errors": 0,
    }

    token = _acquire_tick_lock(R)
    if not token:
        stats["dxy_input_reason"] = "LOCK_NOT_ACQUIRED"
        return stats

    stats["lock"] = True

    try:
        dxy_inputs, dxy_input_error = (
            _load_strategy_b_dxy_inputs(
                R,
                now_ms=started_ms,
            )
        )

        if isinstance(dxy_inputs, Mapping):
            stats["canonical_dxy_device_id"] = _txt(
                dxy_inputs.get("device_id")
            ) or None

            freshness = dxy_inputs.get("freshness")
            stats["dxy_input_ok"] = bool(
                isinstance(freshness, Mapping)
                and freshness.get("ok") is True
            )

        stats["dxy_input_reason"] = (
            dxy_input_error
            or (
                "OK"
                if stats["dxy_input_ok"]
                else "MANDATORY_DXY_INPUT_NOT_FRESH"
            )
        )
        # -------------------------------------------------------------
        # Phase-7 genuine scheduled-event research discovery.
        # Independent from the existing result/release consumer below.
        # -------------------------------------------------------------
        try:
            schedule_owners = sorted(
                _txt(x)
                for x in (
                    R.smembers(SCHEDULE_OWNERS_KEY)
                    or set()
                )
                if _txt(x)
            )
        except Exception as exc:
            schedule_owners = []
            stats["schedule_errors"] += 1
            log.warning(
                "[STRATEGY_B_PHASE7] SCHEDULE_OWNER_READ_FAILED "
                "err=%r",
                exc,
            )

        schedule_owners = schedule_owners[
            :max(1, OWNER_BATCH)
        ]
        stats["schedule_owners"] = len(
            schedule_owners
        )

        for schedule_owner in schedule_owners:
            schedule_stats = (
                _freeze_strategy_b_schedule_events_for_owner(
                    R,
                    calendar_owner_id=schedule_owner,
                    now_ms=started_ms,
                )
            )

            stats["schedule_seen"] += int(
                schedule_stats.get("seen") or 0
            )
            stats["schedule_eligible"] += int(
                schedule_stats.get("eligible") or 0
            )
            stats["schedule_frozen"] += int(
                schedule_stats.get("frozen") or 0
            )
            stats["schedule_existing"] += int(
                schedule_stats.get("existing") or 0
            )
            stats["schedule_errors"] += int(
                schedule_stats.get("errors") or 0
            )

        # -------------------------------------------------------------
        # Phase-7 causal REAL_DXY M15 checkpoint capture.
        #
        # Runs once per Strategy-B tick, outside the schedule-owner loop.
        # Uses only bounded due-event ZSET discovery and one shared M15 load.
        # -------------------------------------------------------------
        checkpoint_device_id = _txt(
            stats.get("canonical_dxy_device_id")
        )

        if checkpoint_device_id:
            checkpoint_stats = (
                _process_phase7_due_schedule_checkpoints(
                    R,
                    dxy_device_id=checkpoint_device_id,
                    now_ms=started_ms,
                )
            )

            stats["checkpoint_events_due"] = int(
                checkpoint_stats.get("events_due") or 0
            )
            stats["checkpoint_events_loaded"] = int(
                checkpoint_stats.get("events_loaded") or 0
            )
            stats["checkpoint_events_missing"] = int(
                checkpoint_stats.get("events_missing") or 0
            )
            stats["checkpoint_m15_loaded"] = int(
                checkpoint_stats.get("m15_loaded") or 0
            )
            stats["checkpoint_due"] = int(
                checkpoint_stats.get("checkpoint_due") or 0
            )
            stats["checkpoint_frozen"] = int(
                checkpoint_stats.get("checkpoint_frozen") or 0
            )
            stats["checkpoint_existing"] = int(
                checkpoint_stats.get("checkpoint_existing") or 0
            )
            stats["checkpoint_errors"] = int(
                checkpoint_stats.get("errors") or 0
            )
        try:
            owners = sorted(
                _txt(x)
                for x in (
                    R.smembers(RELEASE_OWNERS_KEY)
                    or set()
                )
                if _txt(x)
            )
        except Exception as exc:
            owners = []
            stats["release_errors"] += 1
            log.warning(
                "[STRATEGY_B_MACRO] OWNER_INDEX_READ_FAILED "
                "err=%r",
                exc,
            )

        owners = owners[:max(1, OWNER_BATCH)]
        stats["owners"] = len(owners)

        for owner in owners:
            owner_stats = _consume_new_releases_for_owner(
                R,
                calendar_owner_id=owner,
                dxy_inputs=dxy_inputs,
                dxy_input_error=dxy_input_error,
                now_ms=started_ms,
            )

            stats["release_seen"] += int(
                owner_stats.get("seen") or 0
            )
            stats["release_consumed"] += int(
                owner_stats.get("consumed") or 0
            )
            stats["release_deadletter"] += int(
                owner_stats.get("deadletter") or 0
            )
            stats["release_errors"] += int(
                owner_stats.get("errors") or 0
            )

        stats["active_trimmed"] = _trim_active(
            R,
            started_ms,
        )

        active_stats = _reevaluate_tracked(
            R,
            dxy_inputs=dxy_inputs,
            dxy_input_error=dxy_input_error,
            now_ms=started_ms,
        )

        for field in (
            "checked",
            "evaluated",
            "changed",
            "wait",
            "removed",
            "errors",
        ):
            stats[f"active_{field}"] = int(
                active_stats.get(field) or 0
            )

        stats["finished_at_ms"] = _now_ms()
        stats["duration_ms"] = max(
            0,
            stats["finished_at_ms"] - started_ms,
        )

        try:
            R.setex(
                HEARTBEAT_KEY,
                300,
                _json_dump(stats),
            )
        except Exception:
            pass

        if (
            stats["release_consumed"]
            or stats["active_changed"]
            or stats["release_errors"]
            or stats["active_errors"]
        ):
            log.info(
                "[STRATEGY_B_MACRO] TICK "
                "owners=%s consumed=%s deadletter=%s "
                "active_checked=%s active_changed=%s "
                "dxy_ok=%s dxy_reason=%s device=%s "
                "duration_ms=%s",
                stats["owners"],
                stats["release_consumed"],
                stats["release_deadletter"],
                stats["active_checked"],
                stats["active_changed"],
                stats["dxy_input_ok"],
                stats["dxy_input_reason"],
                stats["canonical_dxy_device_id"],
                stats["duration_ms"],
            )

        return stats

    except Exception as exc:
        stats["fatal_error"] = (
            f"{type(exc).__name__}:{exc}"
        )
        stats["finished_at_ms"] = _now_ms()
        stats["duration_ms"] = max(
            0,
            stats["finished_at_ms"] - started_ms,
        )

        log.exception(
            "[STRATEGY_B_MACRO] TICK_FAILED"
        )

        try:
            R.setex(
                HEARTBEAT_KEY,
                300,
                _json_dump(stats),
            )
        except Exception:
            pass

        return stats

    finally:
        _release_tick_lock(R, token)


__all__ = [
    "tick_strategy_b_macro_observer",
    "reconstruct_strategy_b_macro_state",
    "persist_macro_thesis",
    "load_macro_thesis",
    "validate_input_freshness",
    "_validate_m15_precheck",
    "persist_strategy_b_shadow",
    "reconstruct_strategy_b_shadow",
]
