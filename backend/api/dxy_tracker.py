# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import logging
import time
from typing import Any

log = logging.getLogger("uvicorn.error")


STATE_PREFIX = "xtl:dxy:state:H1"
HISTORY_PREFIX = "xtl:dxy:history:H1"
TURN_STATE_PREFIX = "xtl:dxy:turn:state:H1"
TURN_HISTORY_PREFIX = "xtl:dxy:turn:history:H1"
TURN_BOOTSTRAP_PREFIX = "xtl:dxy:turn:bootstrap:H1"
BOOTSTRAP_PREFIX = "xtl:dxy:bootstrap:H1"
EVENT_CLAIM_PREFIX = "xtl:dxy:event_claim:H1"

BINDINGS_CACHE_KEY = "xtl:dxy:tracker:bindings"
TICK_LOCK_KEY = "xtl:dxy:tracker:tick_lock"

BINDINGS_CACHE_SEC = 300
TICK_LOCK_SEC = 20

HISTORY_MAX_EVENTS = 2000
HISTORY_TTL_SEC = 180 * 24 * 3600
BOOTSTRAP_MAX_BARS = 300

H1_MS = 60 * 60 * 1000

VALID_DIRECTIONS = {
    "BULLISH",
    "BEARISH",
    "NEUTRAL",
}


def _decode(value: Any) -> str:
    if isinstance(value, (bytes, bytearray)):
        return value.decode(
            "utf-8",
            "ignore",
        )

    return str(value or "")


def _json_load(value: Any, default=None):
    if default is None:
        default = {}

    try:
        if value is None:
            return default

        if isinstance(value, (bytes, bytearray)):
            value = value.decode(
                "utf-8",
                "ignore",
            )

        obj = (
            json.loads(value)
            if isinstance(value, str)
            else value
        )

        # Defensive support for double-encoded JSON.
        if isinstance(obj, str):
            obj = json.loads(obj)

        return obj

    except Exception:
        return default


def _to_ms(value: Any) -> int:
    try:
        v = int(float(value or 0))
    except Exception:
        return 0

    if v <= 0:
        return 0

    # Seconds.
    if v < 10_000_000_000:
        return v * 1000

    # Nanoseconds.
    if v >= 1_000_000_000_000_000_000:
        return v // 1_000_000

    # Microseconds.
    if v >= 1_000_000_000_000_000:
        return v // 1000

    return v


def _broker_offset_minutes(
    R,
    device_id: str,
) -> int:
    """
    Read the broker wall-clock offset published by the MT5 device.

    Example:
        broker_tz_offset_min = 180
        UTC+03:00 broker candle timestamps
        canonical UTC = broker timestamp - 180 minutes
    """
    dev = str(device_id or "").strip()

    if not dev:
        return 0

    try:
        raw = R.hget(
            f"device:{dev}",
            "broker_tz_offset_min",
        )

        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode(
                "utf-8",
                "ignore",
            )

        return int(float(raw or 0))

    except Exception:
        return 0

def _broker_ms_to_utc_ms(
    broker_ms: int,
    offset_minutes: int,
) -> int:
    try:
        value = int(broker_ms or 0)
        offset = int(offset_minutes or 0)

        if value <= 0:
            return 0

        return int(
            value
            - offset * 60 * 1000
        )

    except Exception:
        return 0

def _bar_open_ms(bar: dict) -> int:
    if not isinstance(bar, dict):
        return 0

    return _to_ms(
        bar.get("t_open_ms")
        or bar.get("tOpenMs")
        or bar.get("open_time_ms")
        or bar.get("t")
        or bar.get("time")
        or bar.get("ts")
        or 0
    )


def _bar_close_ms(bar: dict) -> int:
    if not isinstance(bar, dict):
        return 0

    close_ms = _to_ms(
        bar.get("t_close_ms")
        or bar.get("tCloseMs")
        or bar.get("close_time_ms")
        or bar.get("t_close")
        or 0
    )

    if close_ms > 0:
        return close_ms

    open_ms = _bar_open_ms(bar)

    if open_ms > 0:
        return open_ms + H1_MS

    return 0


def _canonical_direction(value: Any) -> str:
    direction = str(
        value or ""
    ).upper().strip()

    if direction in (
        "UP",
        "BULL",
        "BULLISH",
        "LONG",
        "BUY",
    ):
        return "BULLISH"

    if direction in (
        "DOWN",
        "BEAR",
        "BEARISH",
        "SHORT",
        "SELL",
    ):
        return "BEARISH"

    return "NEUTRAL"


def _state_key(
    source: str,
    device_id: str,
) -> str:
    return (
        f"{STATE_PREFIX}:"
        f"{source}:{device_id}"
    )


def _history_key(
    source: str,
    device_id: str,
) -> str:
    return (
        f"{HISTORY_PREFIX}:"
        f"{source}:{device_id}"
    )


def _evaluation_claim_key(
    source: str,
    device_id: str,
    bar_close_ms: int,
) -> str:
    return (
        "xtl:dxy:evaluated:H1:"
        f"{source}:{device_id}:"
        f"{int(bar_close_ms)}"
    )



def _bootstrap_key(source: str, device_id: str) -> str:
    return f"{BOOTSTRAP_PREFIX}:{source}:{device_id}"


def _event_claim_key(source: str, device_id: str, change_ms: int, from_dir: str, to_dir: str) -> str:
    return (
        f"{EVENT_CLAIM_PREFIX}:{source}:{device_id}:"
        f"{int(change_ms)}:{from_dir}:{to_dir}"
    )


def _closed_real_dxy_bars(R, device_id: str, max_bars: int = BOOTSTRAP_MAX_BARS) -> list[dict]:
    try:
        raw = R.get(f"xtl:ohlc:snap:{device_id}:DXY:H1")
        payload = _json_load(raw, {})
        bars = payload.get("bars") if isinstance(payload, dict) else []
        if not isinstance(bars, list):
            return []
        out = [b for b in bars[-int(max_bars or BOOTSTRAP_MAX_BARS):]
               if isinstance(b, dict) and bool(b.get("complete", True))]
        out.sort(key=lambda b: _bar_open_ms(b))
        return out
    except Exception:
        return []


def _direction_event(
    *, source: str, device_id: str, binding: dict, from_dir: str, to_dir: str,
    change_ms: int, broker_bar_close_ms: int, broker_offset_minutes: int,
    detected_at_ms: int, metrics: dict, historical_backfill: bool,
) -> dict:
    return {
        "schema_version": 1,
        "event_type": "DXY_DIRECTION_CHANGE",
        "model": "DXY_H1_20_ATR_R2_STRONG_OVERRIDE_V1",
        "source": source,
        "device_id": device_id,
        "uids": binding.get("uids") or [],
        "profile_ids": binding.get("profile_ids") or [],
        "firms": binding.get("firms") or [],
        "from": from_dir,
        "to": to_dir,
        "change_ms": int(change_ms),
        "bar_close_ms": int(change_ms),
        "broker_bar_close_ms": int(broker_bar_close_ms or 0) or None,
        "broker_offset_minutes": int(broker_offset_minutes or 0),
        "detected_at_ms": int(detected_at_ms),
        "direction_raw": metrics.get("h1_20_direction_raw"),
        "tilt": metrics.get("h1_20_tilt"),
        "net_atr": metrics.get("h1_20_net_atr"),
        "slope_atr": metrics.get("h1_20_slope_atr"),
        "r2": metrics.get("h1_20_r2"),
        "bars_used": metrics.get("h1_20_bars_used"),
        "historical_backfill": bool(historical_backfill),
    }


def _append_event_once(R, event: dict) -> bool:
    source = str(event.get("source") or "")
    device_id = str(event.get("device_id") or "")
    change_ms = int(event.get("change_ms") or 0)
    from_dir = str(event.get("from") or "")
    to_dir = str(event.get("to") or "")
    if not source or not device_id or change_ms <= 0 or not from_dir or not to_dir:
        return False
    claim = _event_claim_key(source, device_id, change_ms, from_dir, to_dir)
    try:
        if not R.set(claim, "1", nx=True, ex=HISTORY_TTL_SEC):
            return False
        hk = _history_key(source, device_id)
        pipe = R.pipeline()
        pipe.rpush(hk, json.dumps(event, default=str, separators=(",", ":")))
        pipe.ltrim(hk, -HISTORY_MAX_EVENTS, -1)
        pipe.expire(hk, HISTORY_TTL_SEC)
        pipe.execute()
        return True
    except Exception:
        try:
            R.delete(claim)
        except Exception:
            pass
        return False


def _bootstrap_direction_history(
    R, *, source: str, device_id: str, binding: dict, bars: list[dict],
    broker_offset_minutes: int, detected_at_ms: int,
) -> dict:
    marker_key = _bootstrap_key(source, device_id)
    marker = _json_load(R.get(marker_key), {})
    if isinstance(marker, dict) and marker.get("completed"):
        return marker
    if len(bars) < 20:
        return {"completed": False, "reason": "INSUFFICIENT_BARS", "transition_count": 0}

    from api.xtl_analytics import _atr_from_bars, _h1_window_direction

    evaluations = []
    for idx in range(19, len(bars)):
        prefix = bars[:idx + 1]
        atr = _atr_from_bars(prefix)
        if not atr or atr <= 0:
            continue
        metrics = _h1_window_direction(prefix, atr, n=20) or {}
        if not metrics:
            continue
        direction = _canonical_direction(metrics.get("h1_20_direction"))
        broker_close_ms = _bar_close_ms(bars[idx])
        close_ms = _broker_ms_to_utc_ms(broker_close_ms, broker_offset_minutes)
        if close_ms <= 0 or close_ms > int(detected_at_ms) + 120000:
            continue
        evaluations.append((close_ms, broker_close_ms, direction, metrics))

    events = []
    for prev, cur in zip(evaluations, evaluations[1:]):
        if prev[2] == cur[2]:
            continue
        event = _direction_event(
            source=source, device_id=device_id, binding=binding,
            from_dir=prev[2], to_dir=cur[2], change_ms=cur[0],
            broker_bar_close_ms=cur[1], broker_offset_minutes=broker_offset_minutes,
            detected_at_ms=detected_at_ms, metrics=cur[3], historical_backfill=True,
        )
        if _append_event_once(R, event):
            events.append(event)

    last_eval = evaluations[-1] if evaluations else None
    last_event = events[-1] if events else None
    marker = {
        "completed": True,
        "completed_at_ms": int(detected_at_ms),
        "source": source,
        "device_id": device_id,
        "evaluations": len(evaluations),
        "transition_count": len(events),
        "first_evaluated_ms": evaluations[0][0] if evaluations else None,
        "last_evaluated_ms": last_eval[0] if last_eval else None,
        "last_direction": last_eval[2] if last_eval else None,
        "last_change_ms": last_event.get("change_ms") if last_event else None,
        "last_change_from": last_event.get("from") if last_event else None,
        "last_change_to": last_event.get("to") if last_event else None,
    }
    R.set(marker_key, json.dumps(marker, separators=(",", ":")), ex=HISTORY_TTL_SEC)
    return marker


def read_dxy_state(R, source: str, device_id: str) -> dict:
    state = _json_load(R.get(_state_key(source, device_id)), {})
    return state if isinstance(state, dict) else {}


def read_dxy_events_between(R, source: str, device_id: str, start_ms: int, end_ms: int) -> list[dict]:
    if int(start_ms or 0) <= 0 or int(end_ms or 0) < int(start_ms or 0):
        return []
    out = []
    try:
        for raw in R.lrange(_history_key(source, device_id), 0, -1) or []:
            event = _json_load(raw, {})
            if not isinstance(event, dict):
                continue
            change_ms = int(event.get("change_ms") or 0)
            if int(start_ms) <= change_ms <= int(end_ms):
                out.append(event)
    except Exception:
        return []
    out.sort(key=lambda e: int(e.get("change_ms") or 0))
    return out



def _turn_state_key(source: str, device_id: str) -> str:
    return f"{TURN_STATE_PREFIX}:{source}:{device_id}"


def _turn_history_key(source: str, device_id: str) -> str:
    return f"{TURN_HISTORY_PREFIX}:{source}:{device_id}"


def _turn_bootstrap_key(source: str, device_id: str) -> str:
    return f"{TURN_BOOTSTRAP_PREFIX}:{source}:{device_id}"


def _turn_eval_claim_key(source: str, device_id: str, bar_close_ms: int) -> str:
    return f"xtl:dxy:turn:evaluated:H1:{source}:{device_id}:{int(bar_close_ms)}"


def _turn_event_claim_key(
    source: str,
    device_id: str,
    change_ms: int,
    direction: str,
) -> str:
    return (
        f"xtl:dxy:turn:event_claim:H1:{source}:{device_id}:"
        f"{int(change_ms)}:{direction}"
    )


def _safe_num(value: Any) -> float | None:
    try:
        number = float(value)
        return number if number == number else None
    except Exception:
        return None


def _dxy_turn_metrics(bars: list[dict], atr: float) -> dict:
    """
    Early H1 turn detector, independent of the rolling-20 bias model.

    It looks for a credible short-window displacement plus agreement across
    closes and candle bodies. This is intentionally analytics/shadow only.
    """
    if not bars or len(bars) < 7 or not atr or atr <= 0:
        return {}

    window = bars[-7:]
    closes = [_safe_num(b.get("c")) for b in window]
    opens = [_safe_num(b.get("o")) for b in window]
    highs = [_safe_num(b.get("h")) for b in window]
    lows = [_safe_num(b.get("l")) for b in window]
    if any(v is None for v in closes + opens + highs + lows):
        return {}

    # Latest three completed H1 moves (four close points).
    recent_net_atr = (closes[-1] - closes[-4]) / atr
    prior_net_atr = (closes[-4] - closes[-7]) / atr

    close_steps = [closes[i] - closes[i - 1] for i in range(4, 7)]
    up_steps = sum(1 for x in close_steps if x > 0)
    down_steps = sum(1 for x in close_steps if x < 0)

    recent_bodies = [closes[i] - opens[i] for i in range(4, 7)]
    bullish_bodies = sum(1 for x in recent_bodies if x > 0)
    bearish_bodies = sum(1 for x in recent_bodies if x < 0)

    # Close must take out the preceding three closes. This is earlier and less
    # noisy than waiting for the full rolling-20 bias to reverse.
    bullish_break = closes[-1] > max(closes[-4:-1])
    bearish_break = closes[-1] < min(closes[-4:-1])

    direction = "NEUTRAL"
    if (
        recent_net_atr >= 0.75
        and up_steps >= 2
        and bullish_bodies >= 2
        and bullish_break
    ):
        direction = "BULLISH"
    elif (
        recent_net_atr <= -0.75
        and down_steps >= 2
        and bearish_bodies >= 2
        and bearish_break
    ):
        direction = "BEARISH"

    strength = min(40.0, abs(recent_net_atr) * 20.0)
    step_score = (up_steps if direction == "BULLISH" else down_steps) * 10.0
    body_score = (
        bullish_bodies if direction == "BULLISH" else bearish_bodies
    ) * 7.5
    reversal_bonus = 0.0
    if direction == "BULLISH" and prior_net_atr <= 0.0:
        reversal_bonus = 10.0
    elif direction == "BEARISH" and prior_net_atr >= 0.0:
        reversal_bonus = 10.0

    confidence = 0
    if direction in ("BULLISH", "BEARISH"):
        confidence = int(round(min(100.0, strength + step_score + body_score + reversal_bonus)))

    return {
        "direction": direction,
        "recent_net_atr": round(recent_net_atr, 3),
        "prior_net_atr": round(prior_net_atr, 3),
        "up_steps": up_steps,
        "down_steps": down_steps,
        "bullish_bodies": bullish_bodies,
        "bearish_bodies": bearish_bodies,
        "bullish_break": bool(bullish_break),
        "bearish_break": bool(bearish_break),
        "confidence": confidence,
        "bars_used": 7,
        "model": "DXY_H1_EARLY_TURN_3BAR_ATR_STRUCTURE_V1",
    }


def _append_turn_event_once(R, event: dict) -> bool:
    source = str(event.get("source") or "")
    device_id = str(event.get("device_id") or "")
    change_ms = int(event.get("change_ms") or 0)
    direction = str(event.get("direction") or "")
    if not source or not device_id or change_ms <= 0 or direction not in ("BULLISH", "BEARISH"):
        return False
    claim = _turn_event_claim_key(source, device_id, change_ms, direction)
    try:
        if not R.set(claim, "1", nx=True, ex=HISTORY_TTL_SEC):
            return False
        hk = _turn_history_key(source, device_id)
        pipe = R.pipeline()
        pipe.rpush(hk, json.dumps(event, default=str, separators=(",", ":")))
        pipe.ltrim(hk, -HISTORY_MAX_EVENTS, -1)
        pipe.expire(hk, HISTORY_TTL_SEC)
        pipe.execute()
        return True
    except Exception:
        try:
            R.delete(claim)
        except Exception:
            pass
        return False


def _bootstrap_turn_history(
    R,
    *,
    source: str,
    device_id: str,
    binding: dict,
    bars: list[dict],
    broker_offset_minutes: int,
    detected_at_ms: int,
) -> dict:
    marker_key = _turn_bootstrap_key(source, device_id)
    marker = _json_load(R.get(marker_key), {})
    if isinstance(marker, dict) and marker.get("completed"):
        return marker
    if len(bars) < 7:
        return {"completed": False, "reason": "INSUFFICIENT_BARS", "event_count": 0}

    from api.xtl_analytics import _atr_from_bars

    evaluations = []
    events = []
    previous_direction = "NEUTRAL"
    for idx in range(6, len(bars)):
        prefix = bars[:idx + 1]
        atr = _atr_from_bars(prefix)
        metrics = _dxy_turn_metrics(prefix, atr) if atr else {}
        if not metrics:
            continue
        direction = str(metrics.get("direction") or "NEUTRAL")
        broker_close_ms = _bar_close_ms(bars[idx])
        close_ms = _broker_ms_to_utc_ms(broker_close_ms, broker_offset_minutes)
        if close_ms <= 0 or close_ms > int(detected_at_ms) + 120000:
            continue
        evaluations.append((close_ms, broker_close_ms, direction, metrics))

        # Persist directional starts only. NEUTRAL remains an arming/reset state.
        if direction in ("BULLISH", "BEARISH") and direction != previous_direction:
            event = {
                "schema_version": 1,
                "event_type": "DXY_EARLY_TURN",
                "model": metrics.get("model"),
                "source": source,
                "device_id": device_id,
                "uids": binding.get("uids") or [],
                "profile_ids": binding.get("profile_ids") or [],
                "firms": binding.get("firms") or [],
                "direction": direction,
                "from_short_state": previous_direction,
                "change_ms": close_ms,
                "bar_close_ms": close_ms,
                "broker_bar_close_ms": broker_close_ms,
                "broker_offset_minutes": broker_offset_minutes,
                "detected_at_ms": int(detected_at_ms),
                "confidence": metrics.get("confidence"),
                "recent_net_atr": metrics.get("recent_net_atr"),
                "prior_net_atr": metrics.get("prior_net_atr"),
                "up_steps": metrics.get("up_steps"),
                "down_steps": metrics.get("down_steps"),
                "bullish_bodies": metrics.get("bullish_bodies"),
                "bearish_bodies": metrics.get("bearish_bodies"),
                "historical_backfill": True,
            }
            if _append_turn_event_once(R, event):
                events.append(event)

        previous_direction = direction

    last_eval = evaluations[-1] if evaluations else None
    last_event = events[-1] if events else None
    marker = {
        "completed": True,
        "completed_at_ms": int(detected_at_ms),
        "source": source,
        "device_id": device_id,
        "evaluations": len(evaluations),
        "event_count": len(events),
        "last_short_state": last_eval[2] if last_eval else "NEUTRAL",
        "last_turn_ms": last_event.get("change_ms") if last_event else None,
        "last_turn_direction": last_event.get("direction") if last_event else None,
    }
    R.set(marker_key, json.dumps(marker, separators=(",", ":")), ex=HISTORY_TTL_SEC)
    return marker


def _persist_turn_snapshot(
    R,
    snapshot: dict,
    binding: dict,
    detected_at_ms: int,
) -> dict:
    source = str(snapshot.get("source") or "").upper().strip()
    device_id = str(snapshot.get("device_id") or "").strip()
    bars = snapshot.get("_bars") or []
    bar_close_ms = int(snapshot.get("bar_close_ms") or 0)
    broker_close_ms = int(snapshot.get("broker_bar_close_ms") or 0)
    offset_min = int(snapshot.get("broker_offset_minutes") or 0)
    if source not in ("REAL_DXY", "SYNTHETIC_DXY") or not device_id or len(bars) < 7:
        return {"ok": False, "reason": "TURN_INPUT_MISSING"}
    if bar_close_ms <= 0 or bar_close_ms > int(detected_at_ms) + 120000:
        return {"ok": False, "reason": "TURN_FUTURE_BAR"}

    claim_key = _turn_eval_claim_key(source, device_id, bar_close_ms)
    try:
        if not R.set(claim_key, str(detected_at_ms), nx=True, ex=2 * 60 * 60):
            return {"ok": True, "skipped": "TURN_BAR_ALREADY_EVALUATED"}
    except Exception:
        return {"ok": False, "reason": "TURN_CLAIM_FAILED"}

    from api.xtl_analytics import _atr_from_bars
    atr = _atr_from_bars(bars)
    metrics = _dxy_turn_metrics(bars, atr) if atr else {}
    if not metrics:
        return {"ok": False, "reason": "TURN_METRICS_EMPTY"}

    state_key = _turn_state_key(source, device_id)
    old = _json_load(R.get(state_key), {})
    if not isinstance(old, dict):
        old = {}
    initialized = not bool(old.get("initialized"))
    bootstrap = {}
    if initialized:
        bootstrap = _bootstrap_turn_history(
            R,
            source=source,
            device_id=device_id,
            binding=binding,
            bars=bars,
            broker_offset_minutes=offset_min,
            detected_at_ms=detected_at_ms,
        )

    old_short = str(old.get("short_state") or bootstrap.get("last_short_state") or "NEUTRAL")
    direction = str(metrics.get("direction") or "NEUTRAL")
    emitted = False
    event = None
    if direction in ("BULLISH", "BEARISH") and direction != old_short:
        event = {
            "schema_version": 1,
            "event_type": "DXY_EARLY_TURN",
            "model": metrics.get("model"),
            "source": source,
            "source_detail": snapshot.get("source_detail"),
            "device_id": device_id,
            "uids": binding.get("uids") or [],
            "profile_ids": binding.get("profile_ids") or [],
            "firms": binding.get("firms") or [],
            "direction": direction,
            "from_short_state": old_short,
            "change_ms": bar_close_ms,
            "bar_close_ms": bar_close_ms,
            "broker_bar_close_ms": broker_close_ms,
            "broker_offset_minutes": offset_min,
            "detected_at_ms": int(detected_at_ms),
            "confidence": metrics.get("confidence"),
            "recent_net_atr": metrics.get("recent_net_atr"),
            "prior_net_atr": metrics.get("prior_net_atr"),
            "up_steps": metrics.get("up_steps"),
            "down_steps": metrics.get("down_steps"),
            "bullish_bodies": metrics.get("bullish_bodies"),
            "bearish_bodies": metrics.get("bearish_bodies"),
            "historical_backfill": False,
        }
        emitted = _append_turn_event_once(R, event)

    state = {
        "schema_version": 1,
        "initialized": True,
        "model": metrics.get("model"),
        "source": source,
        "source_detail": snapshot.get("source_detail"),
        "device_id": device_id,
        "uids": binding.get("uids") or [],
        "profile_ids": binding.get("profile_ids") or [],
        "firms": binding.get("firms") or [],
        "short_state": direction,
        "last_turn_direction": (
            direction if emitted else old.get("last_turn_direction") or bootstrap.get("last_turn_direction")
        ),
        "last_turn_ms": (
            bar_close_ms if emitted else old.get("last_turn_ms") or bootstrap.get("last_turn_ms")
        ),
        "last_evaluated_bar_close_ms": bar_close_ms,
        "broker_bar_close_ms": broker_close_ms,
        "broker_offset_minutes": offset_min,
        "detected_at_ms": int(detected_at_ms),
        "confidence": metrics.get("confidence"),
        "recent_net_atr": metrics.get("recent_net_atr"),
        "prior_net_atr": metrics.get("prior_net_atr"),
        "up_steps": metrics.get("up_steps"),
        "down_steps": metrics.get("down_steps"),
        "bullish_bodies": metrics.get("bullish_bodies"),
        "bearish_bodies": metrics.get("bearish_bodies"),
        "bootstrap_completed": bool(bootstrap.get("completed")) if initialized else bool(old.get("bootstrap_completed")),
        "bootstrap_event_count": int(bootstrap.get("event_count") or 0) if initialized else int(old.get("bootstrap_event_count") or 0),
    }
    R.set(state_key, json.dumps(state, default=str, separators=(",", ":")))

    if emitted:
        log.warning(
            "[DXY_TURN] EARLY_TURN source=%s device=%s direction=%s "
            "change_ms=%s confidence=%s recent_net_atr=%s",
            source,
            device_id,
            direction,
            bar_close_ms,
            metrics.get("confidence"),
            metrics.get("recent_net_atr"),
        )

    return {"ok": True, "initialized": initialized, "emitted": emitted, "event": event}


def read_dxy_turn_state(R, source: str, device_id: str) -> dict:
    state = _json_load(R.get(_turn_state_key(source, device_id)), {})
    return state if isinstance(state, dict) else {}


def read_dxy_turn_events_between(
    R,
    source: str,
    device_id: str,
    start_ms: int,
    end_ms: int,
) -> list[dict]:
    if int(start_ms or 0) <= 0 or int(end_ms or 0) < int(start_ms or 0):
        return []
    out = []
    try:
        for raw in R.lrange(_turn_history_key(source, device_id), 0, -1) or []:
            event = _json_load(raw, {})
            if not isinstance(event, dict):
                continue
            change_ms = int(event.get("change_ms") or 0)
            if int(start_ms) <= change_ms <= int(end_ms):
                out.append(event)
    except Exception:
        return []
    out.sort(key=lambda e: int(e.get("change_ms") or 0))
    return out


def _profile_ids(R) -> list[str]:
    result = []

    try:
        values = (
            R.smembers(
                "xtl:prop:profiles"
            )
            or set()
        )
    except Exception:
        values = set()

    for value in values:
        profile_id = _decode(value).strip().lower()

        if profile_id:
            result.append(profile_id)

    return sorted(set(result))


def _enabled_profile_config(
    R,
    profile_id: str,
) -> dict | None:
    try:
        raw = R.get(
            f"xtl:prop:profile:{profile_id}"
        )

        cfg = _json_load(
            raw,
            {},
        )

        if not isinstance(cfg, dict):
            return None

        if not bool(
            cfg.get("enabled")
        ):
            return None

        return cfg

    except Exception:
        return None


def _candidate_uids(R) -> list[str]:
    """
    UID is used only to resolve strict profile-to-device ownership.

    DXY state calculation remains device-scoped.
    """
    result = set()

    try:
        for raw_key in R.scan_iter(
            match="xtl:user:*:devices",
            count=100,
        ):
            key = _decode(
                raw_key
            ).strip()

            prefix = "xtl:user:"
            suffix = ":devices"

            if (
                key.startswith(prefix)
                and key.endswith(suffix)
            ):
                uid = key[
                    len(prefix):
                    -len(suffix)
                ].strip()

                if uid:
                    result.add(uid)

    except Exception:
        log.exception(
            "[DXY_TRACKER] UID_DISCOVERY_FAILED"
        )

    return sorted(result)


def _discover_bindings(
    R,
) -> list[dict]:
    """
    Resolve each enabled prop profile to its strictly matched broker device.

    A disconnected profile is skipped. Multiple profiles resolving to the
    same device are merged so market calculations run only once per device.
    """
    from api.trend_endpoints import (
        _resolve_prop_profile_device,
    )

    profile_ids = _profile_ids(R)
    uids = _candidate_uids(R)

    devices: dict[str, dict] = {}

    for uid in uids:
        for profile_id in profile_ids:
            cfg = _enabled_profile_config(
                R,
                profile_id,
            )

            if not cfg:
                continue

            try:
                resolved = (
                    _resolve_prop_profile_device(
                        profile_id,
                        uid,
                    )
                    or {}
                )
            except Exception as exc:
                detail = str(
                    getattr(exc, "detail", "")
                    or ""
                ).strip()

                # Expected when another UID has devices but does not own
                # this configured profile. Skip quietly.
                if detail == "PROP_PROFILE_NOT_FOUND":
                    continue

                log.warning(
                    "[DXY_TRACKER] PROFILE_RESOLVE_FAILED "
                    "uid=%s profile=%s err=%r",
                    uid,
                    profile_id,
                    exc,
                )
                continue

            if not bool(
                resolved.get("ok")
            ):
                # Expected for configured but disconnected firms.
                continue

            device_id = str(
                resolved.get("device_id")
                or ""
            ).strip()

            if not device_id:
                continue

            firm = str(
                cfg.get("firm")
                or ""
            ).strip().lower()

            item = devices.setdefault(
                device_id,
                {
                    "device_id": device_id,
                    "uids": [],
                    "profile_ids": [],
                    "firms": [],
                },
            )

            if uid not in item["uids"]:
                item["uids"].append(uid)

            if (
                profile_id
                not in item["profile_ids"]
            ):
                item["profile_ids"].append(
                    profile_id
                )

            if (
                firm
                and firm not in item["firms"]
            ):
                item["firms"].append(firm)

    out = list(
        devices.values()
    )

    for item in out:
        item["uids"] = sorted(
            item.get("uids") or []
        )
        item["profile_ids"] = sorted(
            item.get("profile_ids") or []
        )
        item["firms"] = sorted(
            item.get("firms") or []
        )

    out.sort(
        key=lambda x: str(
            x.get("device_id") or ""
        )
    )

    return out


def _load_bindings(
    R,
) -> list[dict]:
    """
    Share profile/device discovery across API workers using a Redis cache.
    """
    try:
        cached = _json_load(
            R.get(BINDINGS_CACHE_KEY),
            [],
        )

        if isinstance(cached, list):
            valid = [
                row
                for row in cached
                if (
                    isinstance(row, dict)
                    and str(
                        row.get("device_id")
                        or ""
                    ).strip()
                )
            ]

            if valid:
                return valid

    except Exception:
        pass

    bindings = _discover_bindings(R)

    try:
        R.set(
            BINDINGS_CACHE_KEY,
            json.dumps(
                bindings,
                default=str,
                separators=(",", ":"),
            ),
            ex=BINDINGS_CACHE_SEC,
        )
    except Exception:
        pass

    return bindings


def _real_snapshot(
    R,
    device_id: str,
) -> dict | None:
    from api.xtl_analytics import (
        capture_dxy_market_snapshot,
    )

    result = (
        capture_dxy_market_snapshot(
            device_id=device_id,
        )
        or {}
    )

    if not bool(
        result.get("dxy_available")
    ):
        return None

    broker_bar_close_ms = _to_ms(
        result.get(
            "dxy_last_closed_h1_close_ms"
        )
    )

    broker_offset_min = (
        _broker_offset_minutes(
            R,
            device_id,
        )
    )

    bar_close_ms = _broker_ms_to_utc_ms(
        broker_bar_close_ms,
        broker_offset_min,
    )

    if bar_close_ms <= 0:
        return None

    return {
        "_bars": _closed_real_dxy_bars(R, device_id),
        "source": "REAL_DXY",
        "source_detail": (
            result.get("dxy_source")
            or "broker_mt5"
        ),
        "device_id": device_id,
        "bar_close_ms": bar_close_ms,
        "last_close": result.get(
            "dxy_last_closed_h1_close"
        ),
        "direction": _canonical_direction(
            result.get(
                "dxy_h1_20_direction"
            )
        ),
        "direction_raw": result.get(
            "dxy_h1_20_direction_raw"
        ),
        "tilt": result.get(
            "dxy_h1_20_tilt"
        ),
        "net_atr": result.get(
            "dxy_h1_20_net_atr"
        ),
        "slope_atr": result.get(
            "dxy_h1_20_slope_atr"
        ),
        "r2": result.get(
            "dxy_h1_20_r2"
        ),
        "bars_used": result.get(
            "dxy_h1_20_bars_used"
        ),
        "synthetic_pair_count": None,
        "synthetic_pairs": None,
        "broker_bar_close_ms": broker_bar_close_ms,
        "broker_offset_minutes": broker_offset_min,
    }


def _synthetic_snapshot(
    R,
    device_id: str,
) -> dict | None:
    from api.xtl_analytics import (
        _atr_from_bars,
        _h1_window_direction,
        build_synthetic_dxy_h1_bars,
    )

    bars = (
        build_synthetic_dxy_h1_bars(
            device_id=device_id,
            max_bars=300,
        )
        or []
    )

    if len(bars) < 20:
        return None

    atr = _atr_from_bars(
        bars
    )

    if not atr or atr <= 0:
        return None

    result = (
        _h1_window_direction(
            bars,
            atr,
            n=20,
        )
        or {}
    )

    if not result:
        return None

    last_bar = bars[-1]

    broker_bar_close_ms = _bar_close_ms(
        last_bar
    )

    broker_offset_min = (
        _broker_offset_minutes(
            R,
            device_id,
        )
    )

    bar_close_ms = _broker_ms_to_utc_ms(
        broker_bar_close_ms,
        broker_offset_min,
    )

    if bar_close_ms <= 0:
        return None

    return {
        "_bars": bars,
        "source": "SYNTHETIC_DXY",
        "broker_bar_close_ms": broker_bar_close_ms,
        "broker_offset_minutes": broker_offset_min,
        "source_detail": (
            "SYNTHETIC_USD_BASKET"
        ),
        "device_id": device_id,
        "bar_close_ms": bar_close_ms,
        "last_close": last_bar.get("c"),
        "direction": _canonical_direction(
            result.get(
                "h1_20_direction"
            )
        ),
        "direction_raw": result.get(
            "h1_20_direction_raw"
        ),
        "tilt": result.get(
            "h1_20_tilt"
        ),
        "net_atr": result.get(
            "h1_20_net_atr"
        ),
        "slope_atr": result.get(
            "h1_20_slope_atr"
        ),
        "r2": result.get(
            "h1_20_r2"
        ),
        "bars_used": result.get(
            "h1_20_bars_used"
        ),
        "synthetic_pair_count": (
            last_bar.get(
                "synthetic_pair_count"
            )
        ),
        "synthetic_pairs": (
            last_bar.get(
                "synthetic_pairs"
            )
        ),
    }


def _persist_snapshot(
    R,
    snapshot: dict,
    binding: dict,
    detected_at_ms: int,
) -> dict:
    source = str(
        snapshot.get("source")
        or ""
    ).upper().strip()

    device_id = str(
        snapshot.get("device_id")
        or ""
    ).strip()

    bar_close_ms = int(
        snapshot.get("bar_close_ms")
        or 0
    )

    # ---------------------------------------------------------
    # P0: canonical completed H1 close must not be in the future.
    #
    # Keep this guard even after broker-offset conversion. It
    # protects against wrong/stale offsets and malformed future
    # candles incorrectly marked complete.
    # ---------------------------------------------------------
    max_future_ms = 2 * 60 * 1000

    if (
        bar_close_ms > 0
        and bar_close_ms
        > int(detected_at_ms) + max_future_ms
    ):
        log.error(
            "[DXY_TRACKER] FUTURE_BAR_REJECTED "
            "source=%s device=%s "
            "bar_close_ms=%s detected_at_ms=%s "
            "broker_bar_close_ms=%s "
            "broker_offset_minutes=%s ahead_ms=%s",
            source,
            device_id,
            bar_close_ms,
            int(detected_at_ms),
            snapshot.get("broker_bar_close_ms"),
            snapshot.get("broker_offset_minutes"),
            (
                bar_close_ms
                - int(detected_at_ms)
            ),
        )

        return {
            "ok": False,
            "changed": False,
            "reason": "FUTURE_BAR_CLOSE_MS",
            "bar_close_ms": bar_close_ms,
            "detected_at_ms": int(
                detected_at_ms
            ),
        }

    direction = _canonical_direction(
        snapshot.get("direction")
    )

    if (
        source not in (
            "REAL_DXY",
            "SYNTHETIC_DXY",
        )
        or not device_id
        or bar_close_ms <= 0
        or direction
        not in VALID_DIRECTIONS
    ):
        return {
            "ok": False,
            "reason": "INVALID_SNAPSHOT",
        }

    state_key = _state_key(
        source,
        device_id,
    )

    old_state = _json_load(
        R.get(state_key),
        {},
    )

    if not isinstance(old_state, dict):
        old_state = {}

    old_bar_ms = int(
        old_state.get(
            "last_evaluated_bar_close_ms"
        )
        or 0
    )

    # Same or older completed H1 bar: no work.
    if old_bar_ms >= bar_close_ms:
        return {
            "ok": True,
            "changed": False,
            "skipped": "BAR_ALREADY_EVALUATED",
        }

    claim_key = _evaluation_claim_key(
        source,
        device_id,
        bar_close_ms,
    )

    try:
        claimed = bool(
            R.set(
                claim_key,
                str(detected_at_ms),
                nx=True,
                ex=2 * 60 * 60,
            )
        )
    except Exception:
        claimed = False

    if not claimed:
        return {
            "ok": True,
            "changed": False,
            "skipped": "EVALUATION_CLAIM_BUSY",
        }

    old_direction = _canonical_direction(
        old_state.get("direction")
    )

    initialized = not bool(
        old_state.get("initialized")
    )

    bootstrap = {}
    if initialized:
        bootstrap = _bootstrap_direction_history(
            R,
            source=source,
            device_id=device_id,
            binding=binding,
            bars=(snapshot.get("_bars") or []),
            broker_offset_minutes=int(snapshot.get("broker_offset_minutes") or 0),
            detected_at_ms=int(detected_at_ms),
        )

    changed = (
        not initialized
        and old_direction != direction
    )

    direction_since_ms = int(
        old_state.get("direction_since_ms")
        or 0
    )

    if initialized and bootstrap.get("last_change_ms"):
        direction_since_ms = int(bootstrap.get("last_change_ms"))
    elif initialized or changed or direction_since_ms <= 0:
        direction_since_ms = bar_close_ms

    state = {
        "schema_version": 1,
        "initialized": True,
        "broker_bar_close_ms": snapshot.get(
            "broker_bar_close_ms"
        ),
        "broker_offset_minutes": snapshot.get(
            "broker_offset_minutes"
        ),
        "model": (
            "DXY_H1_20_ATR_R2_STRONG_OVERRIDE_V1"
        ),

        "source": source,
        "source_detail": snapshot.get(
            "source_detail"
        ),

        "device_id": device_id,
        "uids": (
            binding.get("uids")
            or []
        ),
        "profile_ids": (
            binding.get("profile_ids")
            or []
        ),
        "firms": (
            binding.get("firms")
            or []
        ),

        "direction": direction,
        "previous_direction": (
            bootstrap.get("last_change_from")
            if initialized and bootstrap.get("last_change_from")
            else old_direction if changed
            else old_state.get("previous_direction")
        ),

        "direction_since_ms": (
            direction_since_ms
        ),

        "last_change_ms": (
            bootstrap.get("last_change_ms")
            if initialized and bootstrap.get("last_change_ms")
            else bar_close_ms if changed
            else old_state.get("last_change_ms")
        ),
        "last_change_from": (
            bootstrap.get("last_change_from")
            if initialized and bootstrap.get("last_change_from")
            else old_direction if changed
            else old_state.get("last_change_from")
        ),
        "last_change_to": (
            bootstrap.get("last_change_to")
            if initialized and bootstrap.get("last_change_to")
            else direction if changed
            else old_state.get("last_change_to")
        ),

        "last_evaluated_bar_close_ms": (
            bar_close_ms
        ),
        "detected_at_ms": int(
            detected_at_ms
        ),

        "last_close": snapshot.get(
            "last_close"
        ),
        "direction_raw": snapshot.get(
            "direction_raw"
        ),
        "tilt": snapshot.get("tilt"),
        "net_atr": snapshot.get(
            "net_atr"
        ),
        "slope_atr": snapshot.get(
            "slope_atr"
        ),
        "r2": snapshot.get("r2"),
        "bars_used": snapshot.get(
            "bars_used"
        ),
        "bootstrap_completed": bool(bootstrap.get("completed")) if initialized else bool(old_state.get("bootstrap_completed")),
        "bootstrap_transition_count": int(bootstrap.get("transition_count") or 0) if initialized else int(old_state.get("bootstrap_transition_count") or 0),

        "synthetic_pair_count": (
            snapshot.get(
                "synthetic_pair_count"
            )
        ),
        "synthetic_pairs": (
            snapshot.get(
                "synthetic_pairs"
            )
        ),
    }

    pipe = R.pipeline()

    pipe.set(
        state_key,
        json.dumps(
            state,
            default=str,
            separators=(",", ":"),
        ),
    )

    event = None

    if changed:
        event = {
            "schema_version": 1,
            "event_type": "DXY_DIRECTION_CHANGE",
            "historical_backfill": False,
            "model": state["model"],
            "broker_bar_close_ms": snapshot.get(
                "broker_bar_close_ms"
            ),
            "broker_offset_minutes": snapshot.get(
                "broker_offset_minutes"
            ),
            "source": source,
            "source_detail": state.get(
                "source_detail"
            ),

            "device_id": device_id,
            "uids": state.get("uids"),
            "profile_ids": state.get(
                "profile_ids"
            ),
            "firms": state.get("firms"),

            "from": old_direction,
            "to": direction,

            # Canonical change time:
            # close of the completed H1 candle that changed the state.
            "change_ms": bar_close_ms,
            "bar_close_ms": bar_close_ms,
            "detected_at_ms": int(
                detected_at_ms
            ),

            "last_close": snapshot.get(
                "last_close"
            ),
            "direction_raw": snapshot.get(
                "direction_raw"
            ),
            "tilt": snapshot.get("tilt"),
            "net_atr": snapshot.get(
                "net_atr"
            ),
            "slope_atr": snapshot.get(
                "slope_atr"
            ),
            "r2": snapshot.get("r2"),
            "bars_used": snapshot.get(
                "bars_used"
            ),

            "synthetic_pair_count": (
                snapshot.get(
                    "synthetic_pair_count"
                )
            ),
            "synthetic_pairs": (
                snapshot.get(
                    "synthetic_pairs"
                )
            ),
        }


    pipe.execute()

    if changed and event:
        _append_event_once(R, event)

    if initialized:
        log.warning(
            "[DXY_TRACKER] INITIALIZED "
            "source=%s device=%s direction=%s "
            "bar_close_ms=%s profiles=%s",
            source,
            device_id,
            direction,
            bar_close_ms,
            state.get("profile_ids"),
        )

    elif changed:
        log.warning(
            "[DXY_TRACKER] DIRECTION_CHANGE "
            "source=%s device=%s from=%s to=%s "
            "change_ms=%s net_atr=%s r2=%s profiles=%s",
            source,
            device_id,
            old_direction,
            direction,
            bar_close_ms,
            snapshot.get("net_atr"),
            snapshot.get("r2"),
            state.get("profile_ids"),
        )

    return {
        "ok": True,
        "changed": bool(changed),
        "initialized": bool(initialized),
        "state_key": state_key,
        "event": event,
    }


def update_global_dxy_state(
    *,
    R,
    now_ms: int | None = None,
) -> dict:
    """
    Continuously update firm/device-aligned real and synthetic DXY timelines.

    This function is independent of open trades and strategy-enabled state.
    It is safe under multiple API workers because Redis claims deduplicate work.
    """
    detected_at_ms = int(
        now_ms
        or time.time() * 1000
    )

    stats = {
        "bindings": 0,
        "devices": 0,
        "real_available": 0,
        "synthetic_available": 0,
        "initialized": 0,
        "changed": 0,
        "turn_initialized": 0,
        "turn_events": 0,
        "errors": 0,
        "lock": False,
    }

    try:
        got_lock = bool(
            R.set(
                TICK_LOCK_KEY,
                str(detected_at_ms),
                nx=True,
                ex=TICK_LOCK_SEC,
            )
        )
    except Exception:
        got_lock = False

    if not got_lock:
        return stats

    stats["lock"] = True

    try:
        bindings = _load_bindings(R)

        stats["bindings"] = len(
            bindings
        )
        stats["devices"] = len(
            bindings
        )

        for binding in bindings:
            device_id = str(
                binding.get("device_id")
                or ""
            ).strip()

            if not device_id:
                continue

            # ---------------------------------------------
            # Track real broker DXY independently.
            # ---------------------------------------------
            try:
                real = _real_snapshot(
                    R,
                    device_id
                )

                if real:
                    stats[
                        "real_available"
                    ] += 1

                    result = _persist_snapshot(
                        R,
                        real,
                        binding,
                        detected_at_ms,
                    )

                    if result.get(
                        "initialized"
                    ):
                        stats[
                            "initialized"
                        ] += 1

                    if result.get("changed"):
                        stats["changed"] += 1

                    turn_result = _persist_turn_snapshot(
                        R, real, binding, detected_at_ms
                    )
                    if turn_result.get("initialized"):
                        stats["turn_initialized"] += 1
                    if turn_result.get("emitted"):
                        stats["turn_events"] += 1

            except Exception:
                stats["errors"] += 1

                log.exception(
                    "[DXY_TRACKER] REAL_UPDATE_FAILED "
                    "device=%s",
                    device_id,
                )

            # ---------------------------------------------
            # Track same-device synthetic DXY independently,
            # even when real DXY is available.
            # ---------------------------------------------
            try:
                synthetic = (
                    _synthetic_snapshot(
                        R,
                        device_id
                    )
                )

                if synthetic:
                    stats[
                        "synthetic_available"
                    ] += 1

                    result = _persist_snapshot(
                        R,
                        synthetic,
                        binding,
                        detected_at_ms,
                    )

                    if result.get(
                        "initialized"
                    ):
                        stats[
                            "initialized"
                        ] += 1

                    if result.get("changed"):
                        stats["changed"] += 1

                    turn_result = _persist_turn_snapshot(
                        R, synthetic, binding, detected_at_ms
                    )
                    if turn_result.get("initialized"):
                        stats["turn_initialized"] += 1
                    if turn_result.get("emitted"):
                        stats["turn_events"] += 1

            except Exception:
                stats["errors"] += 1

                log.exception(
                    "[DXY_TRACKER] SYNTHETIC_UPDATE_FAILED "
                    "device=%s",
                    device_id,
                )

        return stats

    except Exception:
        stats["errors"] += 1

        log.exception(
            "[DXY_TRACKER] GLOBAL_UPDATE_FAILED"
        )

        return stats
