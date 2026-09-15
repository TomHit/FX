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

SCHEMA_VERSION = 1
SERVICE_NAME = "strategy_b_macro_service"
MODE = "OBSERVATION_ONLY"

# ---------------------------------------------------------------------------
# Redis namespace
# ---------------------------------------------------------------------------

RELEASE_OWNERS_KEY = "xtl:strategy_b:macro:release_owners"
RELEASE_INDEX_PREFIX = "xtl:strategy_b:macro:release_index"
RELEASE_CURSOR_PREFIX = "xtl:strategy_b:macro:release_cursor"

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
) -> tuple[Optional[dict], list[dict], str]:
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
        return None, [], (
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
            return None, [], "DXY_BIAS_RESULT_INVALID"

        bias = dict(bias)
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

        return bias, list(bars_h1), ""

    except Exception as exc:
        return None, [], (
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

    bias, h1_bars, err = _load_real_dxy_bias(
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



def _compose_and_persist_strategy_b_macro_shadow(
    R,
    *,
    evidence,
    evidence_id: str,
    context: Mapping[str, Any],
    dxy_inputs: Mapping[str, Any],
    now_ms: int,
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
    from api.strategy_b_live_input_adapter import (
        build_strategy_b_live_inputs,
    )
    from api.strategy_b_macro_qualification import (
        evaluate_strategy_b_macro_qualification,
    )
    from api.strategy_b_shadow_orchestrator import (
        compose_strategy_b_shadow_decision,
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

    downstream_entry = evaluate_strategy_b_entry_patterns(
        macro,
        pattern_observations=(
            pattern_observer.to_entry_pattern_observations()
        ),
        m15_state=live.dxy_m15,
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

    # Shared account/prop risk is deliberately still unwired.
    downstream_risk = {
        "decision": "WAIT",
        "reason": "RISK_INPUT_NOT_YET_WIRED",
        "valid": False,
        "proposed_risk_usd": None,
        "proposed_lots_raw": None,
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
        macro_qualification=macro,
        entry_pattern=downstream_entry,
        expression_ranking=downstream_expression,
        risk_proposal=downstream_risk,
    )

    persisted = persist_strategy_b_shadow(
        R,
        evidence_id=evidence_id,
        calendar_owner_id=_txt(context.get("calendar_owner_id")),
        value_id=_txt(context.get("value_id")),
        shadow_decision=shadow,
        now_ms=now_ms,
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
    cutoff = int(now_ms - RELEASE_RETENTION_MS)

    try:
        removed = int(
            R.zremrangebyscore(
                ACTIVE_KEY,
                "-inf",
                cutoff,
            )
            or 0
        )
        return removed
    except Exception:
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

    for evidence_id in _active_round_robin_batch(R):
        stats["checked"] += 1

        result = _evaluate_evidence(
            R,
            evidence_id=evidence_id,
            dxy_inputs=dxy_inputs,
            dxy_input_error=dxy_input_error,
            now_ms=now_ms,
        )

        if result.get("remove_active"):
            try:
                R.zrem(ACTIVE_KEY, evidence_id)
                stats["removed"] += 1
            except Exception:
                stats["errors"] += 1
            continue

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
