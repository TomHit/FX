# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import logging
import math
import os
import time
from typing import Any
from api.trend_sr import (
    summarize_sr_multi_tf,
    build_market_structure_state,
)

log = logging.getLogger("uvicorn.error")

TF = "M15"
TF_MS = 15 * 60 * 1000
SOURCE_VALUES = ("REAL_DXY", "SYNTHETIC_DXY")

STATE_PREFIX = "xtl:dxy:turn:state:M15"
HISTORY_PREFIX = "xtl:dxy:turn:history:M15"
FEATURES_PREFIX = "xtl:dxy:features:M15"
SERIES_PREFIX = "xtl:dxy:series:M15"
BOOTSTRAP_PREFIX = "xtl:dxy:turn:bootstrap:M15"
EVAL_PREFIX = "xtl:dxy:turn:evaluated:M15"
EVENT_CLAIM_PREFIX = "xtl:dxy:turn:event_claim:M15"
TICK_LOCK_KEY = "xtl:dxy:m15:tracker:tick_lock"

TICK_LOCK_SEC = 20
HISTORY_TTL_SEC = 180 * 24 * 3600
HISTORY_MAX_EVENTS = 5000
MAX_BARS = 300
MIN_FEATURE_BARS = 16
CANDIDATE_EXPIRY_BARS = 6

# Synthetic basket finalization. Five symbol snapshots arrive independently.
# Never permanently evaluate a synthetic candle until all five contributors exist.
SYNTHETIC_EXPECTED_PAIRS = ("EURUSD", "GBPUSD", "USDJPY", "USDCHF", "USDCAD")
SYNTHETIC_EXPECTED_PAIR_COUNT = len(SYNTHETIC_EXPECTED_PAIRS)
SYNTHETIC_CLOSE_GRACE_MS = int(os.getenv("XTL_DXY_M15_CLOSE_GRACE_MS", "30000"))

# Evidence-accumulation lifecycle (shadow only).
# Bullish thresholds preserve V4 responsiveness.
BULL_CANDIDATE_START_SCORE = 46
BULL_CANDIDATE_START_MARGIN = 14
BULL_CONFIRM_SCORE = 62
BULL_CONFIRM_MARGIN = 18

# Bearish evidence was less reliable in the first replay, so require a
# modestly stronger score and separation before starting/confirming.
BEAR_CANDIDATE_START_SCORE = 50
BEAR_CANDIDATE_START_MARGIN = 18
BEAR_CONFIRM_SCORE = 66
BEAR_CONFIRM_MARGIN = 22

CONFIRM_SUPPORT_BARS = 2
REVOKE_SCORE = 60
REVOKE_ADVERSE_ATR = 0.50
CANDIDATE_HARD_REJECT_ATR = 0.65
EVIDENCE_DECAY = 0.55

# Confirmed-turn outcome classification. A later opposite turn is not a
# failure when the active direction already delivered a useful move.
TURN_COMPLETED_ATR = 0.80
TURN_WEAK_ATR = 0.30

# Pin-bar evidence is deliberately capped. A pin bar is one vote, never a
# standalone direction decision or mandatory confirmation gate.
PIN_EVIDENCE_MAX = 8.0
PIN_CLUSTER_BONUS_MAX = 4.0

# V8 SR/context remains audit-only. It cannot alter evidence, candidates, confirmation,
# revocation, or outcomes until its zones pass visual validation.
SR_SCORE_ENABLED = False
SR_PIVOT_LEFT = 2
SR_PIVOT_RIGHT = 2
SR_CLUSTER_ATR = {"M15": 0.22, "H1": 0.28, "H4": 0.35}
SR_ZONE_PAD_ATR = {"M15": 0.08, "H1": 0.10, "H4": 0.12}
SR_NEAR_ATR = {"M15": 0.45, "H1": 0.60, "H4": 0.85}

# V9 current-flow layer (same unified engine, shadow only).
# It describes what DXY is doing now, independently from the slower turn lifecycle.
FLOW_START_SCORE = 50
FLOW_START_MARGIN = 20
FLOW_STRONG_SINGLE_SCORE = 80
FLOW_STRONG_SINGLE_MARGIN = 45
FLOW_MAINTAIN_SCORE = 42
FLOW_MAINTAIN_MARGIN = 8
FLOW_FLIP_SCORE = 70
FLOW_FLIP_MARGIN = 30
FLOW_HARD_FLIP_NET_ATR = 0.80
FLOW_NEUTRAL_AFTER_BARS = 2

# Shared XTL SR engine settings for DXY H1/H4 structure.
DXY_SR_PIP_FACTOR = float(os.getenv("XTL_DXY_SR_PIP_FACTOR", "0.001"))
DXY_SR_LAST_TTL_SEC = int(os.getenv("XTL_DXY_SR_LAST_TTL_SEC", "300"))
DXY_SR_GOOD_TTL_SEC = int(os.getenv("XTL_DXY_SR_GOOD_TTL_SEC", "900"))
DXY_SR_LAST_PREFIX = "xtl:dxy:sr:bundle:last:M15"
DXY_SR_GOOD_PREFIX = "xtl:dxy:sr:bundle:last_good:M15"


def _json_load(value: Any, default=None):
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


def _safe_float(value: Any) -> float | None:
    try:
        number = float(value)
        return number if math.isfinite(number) else None
    except Exception:
        return None


def _to_ms(value: Any) -> int:
    try:
        v = int(float(value or 0))
    except Exception:
        return 0
    if v <= 0:
        return 0
    return v * 1000 if v < 10_000_000_000 else v


def _bar_open_ms(bar: dict) -> int:
    return _to_ms(
        bar.get("t_open_ms")
        or bar.get("t")
        or bar.get("time")
        or 0
    ) if isinstance(bar, dict) else 0


def _bar_close_ms(bar: dict) -> int:
    if not isinstance(bar, dict):
        return 0
    close_ms = _to_ms(bar.get("t_close_ms") or bar.get("t_close") or 0)
    return close_ms or (_bar_open_ms(bar) + TF_MS if _bar_open_ms(bar) else 0)


def _state_key(source: str, device_id: str) -> str:
    return f"{STATE_PREFIX}:{source}:{device_id}"


def _history_key(source: str, device_id: str) -> str:
    return f"{HISTORY_PREFIX}:{source}:{device_id}"


def _features_key(source: str, device_id: str, close_ms: int) -> str:
    return f"{FEATURES_PREFIX}:{source}:{device_id}:{int(close_ms)}"


def _series_key(source: str, device_id: str) -> str:
    return f"{SERIES_PREFIX}:{source}:{device_id}"


def _bootstrap_key(source: str, device_id: str) -> str:
    return f"{BOOTSTRAP_PREFIX}:{source}:{device_id}"


def _eval_key(source: str, device_id: str, close_ms: int) -> str:
    return f"{EVAL_PREFIX}:{source}:{device_id}:{int(close_ms)}"


def _event_claim_key(source: str, device_id: str, close_ms: int, status: str, direction: str) -> str:
    return f"{EVENT_CLAIM_PREFIX}:{source}:{device_id}:{int(close_ms)}:{status}:{direction}"


def _broker_offset_minutes(R, device_id: str) -> int:
    try:
        raw = R.hget(f"device:{device_id}", "broker_tz_offset_min")
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", "ignore")
        return int(float(raw or 0))
    except Exception:
        return 0


def _broker_to_utc_ms(broker_ms: int, offset_min: int) -> int:
    return int(broker_ms or 0) - int(offset_min or 0) * 60 * 1000


def _load_source_bars(source: str, device_id: str) -> list[dict]:
    from api.xtl_analytics import (
        build_synthetic_dxy_m15_bars,
        load_real_dxy_bars,
    )
    if source == "REAL_DXY":
        return load_real_dxy_bars(device_id=device_id, tf="M15", max_bars=MAX_BARS) or []
    return build_synthetic_dxy_m15_bars(device_id=device_id, max_bars=MAX_BARS) or []


def _atr(bars: list[dict], n: int = 14) -> float:
    if len(bars) < n + 1:
        return 0.0
    trs = []
    for i in range(1, len(bars)):
        h = _safe_float(bars[i].get("h")); l = _safe_float(bars[i].get("l"))
        pc = _safe_float(bars[i - 1].get("c"))
        if None in (h, l, pc):
            continue
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    if len(trs) < n:
        return 0.0
    value = sum(trs[:n]) / n
    for tr in trs[n:]:
        value = (value * (n - 1) + tr) / n
    return float(value)


def _slope(values: list[float]) -> float:
    n = len(values)
    if n < 2:
        return 0.0
    mx = (n - 1) / 2.0
    my = sum(values) / n
    sxx = sum((i - mx) ** 2 for i in range(n))
    return sum((i - mx) * (v - my) for i, v in enumerate(values)) / sxx if sxx else 0.0


def _pin_bar_metrics(bar: dict, atr: float) -> dict:
    """Return symmetric bullish/bearish pin-bar quality metrics.

    A valid pin needs a meaningful total range, dominant rejection wick, and
    a close displaced toward the opposite end of the candle. Scores are
    evidence only; they never create or confirm a turn by themselves.
    """
    if not isinstance(bar, dict) or atr <= 0:
        return {
            "direction": "NEUTRAL",
            "bullish_score": 0,
            "bearish_score": 0,
            "range_atr": 0.0,
            "body_ratio": 0.0,
            "upper_wick_ratio": 0.0,
            "lower_wick_ratio": 0.0,
            "close_position": 0.5,
        }

    o = _safe_float(bar.get("o"))
    h = _safe_float(bar.get("h"))
    l = _safe_float(bar.get("l"))
    c = _safe_float(bar.get("c"))
    if None in (o, h, l, c):
        return {"direction": "NEUTRAL", "bullish_score": 0, "bearish_score": 0}

    candle_range = max(0.0, h - l)
    if candle_range <= 0:
        return {"direction": "NEUTRAL", "bullish_score": 0, "bearish_score": 0}

    body = abs(c - o)
    upper_wick = max(0.0, h - max(o, c))
    lower_wick = max(0.0, min(o, c) - l)
    body_ratio = body / candle_range
    upper_ratio = upper_wick / candle_range
    lower_ratio = lower_wick / candle_range
    close_position = (c - l) / candle_range
    range_atr = candle_range / atr

    bullish_score = 0.0
    bearish_score = 0.0

    if range_atr >= 0.45:
        bullish_score += min(40.0, lower_ratio * 55.0)
        bullish_score += min(20.0, max(0.0, lower_wick - upper_wick) / candle_range * 35.0)
        bullish_score += min(20.0, max(0.0, close_position - 0.50) * 40.0)
        bullish_score += min(10.0, max(0.0, range_atr - 0.45) * 12.0)
        if lower_wick >= max(body * 1.8, upper_wick * 1.4) and lower_ratio >= 0.50:
            bullish_score += 10.0

        bearish_score += min(40.0, upper_ratio * 55.0)
        bearish_score += min(20.0, max(0.0, upper_wick - lower_wick) / candle_range * 35.0)
        bearish_score += min(20.0, max(0.0, 0.50 - close_position) * 40.0)
        bearish_score += min(10.0, max(0.0, range_atr - 0.45) * 12.0)
        if upper_wick >= max(body * 1.8, lower_wick * 1.4) and upper_ratio >= 0.50:
            bearish_score += 10.0

    bullish_score = int(round(max(0.0, min(100.0, bullish_score))))
    bearish_score = int(round(max(0.0, min(100.0, bearish_score))))
    direction = (
        "BULLISH" if bullish_score >= 55 and bullish_score >= bearish_score + 12 else
        "BEARISH" if bearish_score >= 55 and bearish_score >= bullish_score + 12 else
        "NEUTRAL"
    )

    return {
        "direction": direction,
        "bullish_score": bullish_score,
        "bearish_score": bearish_score,
        "range_atr": round(range_atr, 4),
        "body_ratio": round(body_ratio, 4),
        "upper_wick_ratio": round(upper_ratio, 4),
        "lower_wick_ratio": round(lower_ratio, 4),
        "close_position": round(close_position, 4),
    }



def _aggregate_bars_causal(bars: list[dict], tf_ms: int) -> list[dict]:
    """Aggregate completed M15 bars into completed higher-timeframe bars.

    Buckets are based on broker timestamps. The current incomplete H1/H4
    bucket is excluded, so replay sees exactly what live processing knew.
    """
    if not bars or tf_ms <= 0:
        return []
    latest_close = _bar_close_ms(bars[-1])
    buckets: dict[int, list[dict]] = {}
    for bar in bars:
        op = _bar_open_ms(bar)
        cl = _bar_close_ms(bar)
        if op <= 0 or cl <= 0 or cl > latest_close:
            continue
        bucket_open = (op // tf_ms) * tf_ms
        bucket_close = bucket_open + tf_ms
        if bucket_close > latest_close:
            continue
        buckets.setdefault(bucket_open, []).append(bar)

    out: list[dict] = []
    for bucket_open in sorted(buckets):
        group = sorted(buckets[bucket_open], key=_bar_open_ms)
        expected = max(1, tf_ms // TF_MS)
        if len(group) < expected:
            continue
        vals = []
        valid = True
        for b in group:
            o = _safe_float(b.get("o")); h = _safe_float(b.get("h"))
            l = _safe_float(b.get("l")); c = _safe_float(b.get("c"))
            if None in (o, h, l, c):
                valid = False
                break
            vals.append((o, h, l, c))
        if not valid:
            continue
        out.append({
            "t_open_ms": bucket_open,
            "t_close_ms": bucket_open + tf_ms,
            "o": vals[0][0],
            "h": max(v[1] for v in vals),
            "l": min(v[2] for v in vals),
            "c": vals[-1][3],
            "complete": True,
            "aggregated_from": "M15",
            "component_bars": len(group),
        })
    return out


def _confirmed_pivots_causal(
    bars: list[dict], *, left: int = SR_PIVOT_LEFT, right: int = SR_PIVOT_RIGHT
) -> tuple[list[dict], list[dict]]:
    """Return pivots known by the final bar; no future candles are used."""
    highs: list[dict] = []
    lows: list[dict] = []
    if len(bars) < left + right + 1:
        return highs, lows
    for i in range(left, len(bars) - right):
        h = _safe_float(bars[i].get("h")); l = _safe_float(bars[i].get("l"))
        if h is None or l is None:
            continue
        left_bars = bars[i-left:i]
        right_bars = bars[i+1:i+right+1]
        other_highs = [_safe_float(b.get("h")) for b in left_bars + right_bars]
        other_lows = [_safe_float(b.get("l")) for b in left_bars + right_bars]
        if all(v is not None and h > v for v in other_highs):
            highs.append({
                "level": h,
                "pivot_open_ms": _bar_open_ms(bars[i]),
                "known_ms": _bar_close_ms(bars[i + right]),
                "index": i,
            })
        if all(v is not None and l < v for v in other_lows):
            lows.append({
                "level": l,
                "pivot_open_ms": _bar_open_ms(bars[i]),
                "known_ms": _bar_close_ms(bars[i + right]),
                "index": i,
            })
    return highs, lows


def _cluster_pivot_zones(
    pivots: list[dict], *, atr: float, tf: str, current_close_ms: int
) -> list[dict]:
    if not pivots or atr <= 0:
        return []
    radius = max(atr * SR_CLUSTER_ATR.get(tf, 0.25), 1e-9)
    pad = max(atr * SR_ZONE_PAD_ATR.get(tf, 0.10), 1e-9)
    clusters: list[list[dict]] = []
    for pivot in sorted(pivots, key=lambda p: float(p.get("level") or 0.0)):
        level = float(pivot.get("level") or 0.0)
        if level <= 0:
            continue
        best = None
        best_dist = None
        for cluster in clusters:
            center = sum(float(x["level"]) for x in cluster) / len(cluster)
            dist = abs(level - center)
            if dist <= radius and (best_dist is None or dist < best_dist):
                best, best_dist = cluster, dist
        if best is None:
            clusters.append([pivot])
        else:
            best.append(pivot)

    zones = []
    for cluster in clusters:
        levels = sorted(float(x["level"]) for x in cluster)
        center = sum(levels) / len(levels)
        last_known = max(int(x.get("known_ms") or 0) for x in cluster)
        age_bars = max(0, int((current_close_ms - last_known) // ({"M15": TF_MS, "H1": 4*TF_MS, "H4": 16*TF_MS}[tf])))
        touches = len(cluster)
        recency = max(0.0, 30.0 - min(30.0, age_bars * 1.5))
        strength = int(round(min(100.0, 25.0 + min(45.0, touches * 15.0) + recency)))
        zones.append({
            "low": round(min(levels) - pad, 6),
            "high": round(max(levels) + pad, 6),
            "level": round(center, 6),
            "touches": touches,
            "strength": strength,
            "source_tf": tf,
            "first_pivot_ms": min(int(x.get("pivot_open_ms") or 0) for x in cluster),
            "last_pivot_ms": max(int(x.get("pivot_open_ms") or 0) for x in cluster),
            "last_known_ms": last_known,
            "age_bars": age_bars,
            "cluster_radius_atr": SR_CLUSTER_ATR.get(tf),
            "zone_pad_atr": SR_ZONE_PAD_ATR.get(tf),
            "causal": True,
        })
    return zones


def _nearest_sr(zones: list[dict], price: float, atr: float, kind: str) -> dict | None:
    if price <= 0 or atr <= 0:
        return None
    if kind == "support":
        eligible = [z for z in zones if float(z.get("level") or 0.0) <= price]
        if not eligible:
            return None
        zone = max(eligible, key=lambda z: float(z.get("level") or 0.0))
        distance = max(0.0, price - float(zone.get("high") or zone.get("level") or 0.0))
    else:
        eligible = [z for z in zones if float(z.get("level") or 0.0) >= price]
        if not eligible:
            return None
        zone = min(eligible, key=lambda z: float(z.get("level") or 0.0))
        distance = max(0.0, float(zone.get("low") or zone.get("level") or 0.0) - price)
    out = dict(zone)
    out["role"] = kind.upper()
    out["distance_atr"] = round(distance / atr, 4)
    out["near"] = bool(out["distance_atr"] <= SR_NEAR_ATR.get(str(zone.get("source_tf")), 0.6))
    return out


def _sr_tf_snapshot(bars: list[dict], *, tf: str, current_price: float) -> dict:
    atr = _atr(bars) if len(bars) >= 15 else 0.0
    highs, lows = _confirmed_pivots_causal(bars)
    current_close_ms = _bar_close_ms(bars[-1]) if bars else 0
    resistance_zones = _cluster_pivot_zones(highs, atr=atr, tf=tf, current_close_ms=current_close_ms)
    support_zones = _cluster_pivot_zones(lows, atr=atr, tf=tf, current_close_ms=current_close_ms)
    return {
        "tf": tf,
        "bars_used": len(bars),
        "atr": round(atr, 6),
        "confirmed_pivot_highs": len(highs),
        "confirmed_pivot_lows": len(lows),
        "support_zone_count": len(support_zones),
        "resistance_zone_count": len(resistance_zones),
        "nearest_support": _nearest_sr(support_zones, current_price, atr, "support"),
        "nearest_resistance": _nearest_sr(resistance_zones, current_price, atr, "resistance"),
        "causal_right_bars": SR_PIVOT_RIGHT,
    }


def _zone_maturity(zone: dict | None) -> str:
    """Audit label only; based on causal pivot-cluster evidence."""
    if not isinstance(zone, dict) or not zone:
        return "NONE"
    touches = int(zone.get("touches") or 0)
    age_bars = int(zone.get("age_bars") or 0)
    if touches >= 3:
        return "STRONG"
    if touches == 2:
        return "BUILDING"
    if touches == 1 and age_bars <= 12:
        return "FRESH"
    return "SINGLE_PIVOT"


def _structure_context_snapshot(
    *,
    current_price: float,
    h1_snap: dict,
    h4_snap: dict,
    m15_snap: dict,
    bullish_sweep: bool,
    bearish_sweep: bool,
    sweep_conflict: bool,
) -> dict:
    """Describe structure without changing any directional score."""
    h1_sup = h1_snap.get("nearest_support") or {}
    h1_res = h1_snap.get("nearest_resistance") or {}
    h4_sup = h4_snap.get("nearest_support") or {}
    h4_res = h4_snap.get("nearest_resistance") or {}
    m15_sup = m15_snap.get("nearest_support") or {}
    m15_res = m15_snap.get("nearest_resistance") or {}

    near_h1_support = bool(h1_sup.get("near"))
    near_h1_resistance = bool(h1_res.get("near"))
    if near_h1_support and near_h1_resistance:
        context = "H1_COMPRESSION_BETWEEN_ZONES"
    elif near_h1_support:
        context = "AT_H1_SUPPORT"
    elif near_h1_resistance:
        context = "AT_H1_RESISTANCE"
    elif h1_sup and h1_res:
        context = "BETWEEN_H1_ZONES"
    elif h1_sup:
        context = "ABOVE_H1_SUPPORT_NO_RESISTANCE"
    elif h1_res:
        context = "BELOW_H1_RESISTANCE_NO_SUPPORT"
    else:
        context = "NO_H1_STRUCTURE"

    down_room = h1_sup.get("distance_atr")
    up_room = h1_res.get("distance_atr")
    down_room = float(down_room) if down_room is not None else None
    up_room = float(up_room) if up_room is not None else None

    def _ratio(numerator, denominator):
        if numerator is None or denominator is None:
            return None
        return round(min(20.0, max(0.0, numerator) / max(0.05, max(0.0, denominator))), 4)

    return {
        "schema_version": 1,
        "context": context,
        "current_price": round(float(current_price), 6),
        "h1_support_maturity": _zone_maturity(h1_sup),
        "h1_resistance_maturity": _zone_maturity(h1_res),
        "h4_support_maturity": _zone_maturity(h4_sup),
        "h4_resistance_maturity": _zone_maturity(h4_res),
        "m15_support_maturity": _zone_maturity(m15_sup),
        "m15_resistance_maturity": _zone_maturity(m15_res),
        "available_downside_atr": down_room,
        "available_upside_atr": up_room,
        "bullish_room_ratio": _ratio(up_room, down_room),
        "bearish_room_ratio": _ratio(down_room, up_room),
        "near_h1_support": near_h1_support,
        "near_h1_resistance": near_h1_resistance,
        "inside_h4_support": bool(h4_sup.get("distance_atr") == 0),
        "inside_h4_resistance": bool(h4_res.get("distance_atr") == 0),
        "inside_m15_support": bool(m15_sup.get("distance_atr") == 0),
        "inside_m15_resistance": bool(m15_res.get("distance_atr") == 0),
        "bullish_h1_sweep_reclaim": bool(bullish_sweep),
        "bearish_h1_sweep_reject": bool(bearish_sweep),
        "sweep_conflict": bool(sweep_conflict),
        "audit_only": True,
    }


def _sr_audit_snapshot(prefix: list[dict], *, current_price: float) -> dict:
    """Build causal M15/H1/H4 structure using only bars known at evaluation time."""
    h1 = _aggregate_bars_causal(prefix, 60 * 60 * 1000)
    h4 = _aggregate_bars_causal(prefix, 4 * 60 * 60 * 1000)
    m15_snap = _sr_tf_snapshot(prefix, tf="M15", current_price=current_price)
    h1_snap = _sr_tf_snapshot(h1, tf="H1", current_price=current_price)
    h4_snap = _sr_tf_snapshot(h4, tf="H4", current_price=current_price)

    latest = prefix[-1] if prefix else {}
    low = float(latest.get("l") or 0.0)
    high = float(latest.get("h") or 0.0)
    close = float(latest.get("c") or 0.0)
    h1_atr = float(h1_snap.get("atr") or 0.0)

    h1_sup = h1_snap.get("nearest_support") or {}
    h1_res = h1_snap.get("nearest_resistance") or {}
    bullish_raw = bool(
        h1_sup and low < float(h1_sup.get("low") or 0.0)
        and close > float(h1_sup.get("level") or 0.0)
    )
    bearish_raw = bool(
        h1_res and high > float(h1_res.get("high") or 0.0)
        and close < float(h1_res.get("level") or 0.0)
    )

    bullish_sweep = bullish_raw
    bearish_sweep = bearish_raw
    sweep_conflict = bool(bullish_raw and bearish_raw)
    bullish_sweep_strength = 0.0
    bearish_sweep_strength = 0.0
    if h1_atr > 0:
        if bullish_raw:
            excursion = max(0.0, float(h1_sup.get("low") or 0.0) - low) / h1_atr
            reclaim = max(0.0, close - float(h1_sup.get("level") or 0.0)) / h1_atr
            bullish_sweep_strength = excursion + reclaim
        if bearish_raw:
            excursion = max(0.0, high - float(h1_res.get("high") or 0.0)) / h1_atr
            reject = max(0.0, float(h1_res.get("level") or 0.0) - close) / h1_atr
            bearish_sweep_strength = excursion + reject

    # A single wide candle can cross overlapping H1 zones. Do not emit both
    # directional sweep flags. Keep raw flags and mark unresolved conflicts.
    if sweep_conflict:
        delta = bullish_sweep_strength - bearish_sweep_strength
        if delta >= 0.10:
            bearish_sweep = False
        elif delta <= -0.10:
            bullish_sweep = False
        else:
            bullish_sweep = False
            bearish_sweep = False

    near_support_tfs = [
        x["tf"] for x in (m15_snap, h1_snap, h4_snap)
        if (x.get("nearest_support") or {}).get("near")
    ]
    near_resistance_tfs = [
        x["tf"] for x in (m15_snap, h1_snap, h4_snap)
        if (x.get("nearest_resistance") or {}).get("near")
    ]
    structure_context = _structure_context_snapshot(
        current_price=current_price,
        h1_snap=h1_snap,
        h4_snap=h4_snap,
        m15_snap=m15_snap,
        bullish_sweep=bullish_sweep,
        bearish_sweep=bearish_sweep,
        sweep_conflict=sweep_conflict,
    )
    return {
        "schema_version": 2,
        "model": "DXY_CAUSAL_MTF_SR_CONTEXT_AUDIT_V2",
        "score_enabled": SR_SCORE_ENABLED,
        "score_applied": 0,
        "no_lookahead": True,
        "pivot_left": SR_PIVOT_LEFT,
        "pivot_right": SR_PIVOT_RIGHT,
        "m15": m15_snap,
        "h1": h1_snap,
        "h4": h4_snap,
        "near_support_tfs": near_support_tfs,
        "near_resistance_tfs": near_resistance_tfs,
        "bullish_h1_sweep_reclaim_raw": bullish_raw,
        "bearish_h1_sweep_reject_raw": bearish_raw,
        "bullish_h1_sweep_reclaim": bullish_sweep,
        "bearish_h1_sweep_reject": bearish_sweep,
        "bullish_sweep_strength_atr": round(bullish_sweep_strength, 4),
        "bearish_sweep_strength_atr": round(bearish_sweep_strength, 4),
        "sweep_conflict": sweep_conflict,
        "bullish_confluence_count": len(near_support_tfs),
        "bearish_confluence_count": len(near_resistance_tfs),
        "structure_context": structure_context,
    }

def _feature_snapshot(bars: list[dict]) -> dict:
    if len(bars) < MIN_FEATURE_BARS:
        return {}
    window = bars[-MIN_FEATURE_BARS:]
    o = [_safe_float(b.get("o")) for b in window]
    h = [_safe_float(b.get("h")) for b in window]
    l = [_safe_float(b.get("l")) for b in window]
    c = [_safe_float(b.get("c")) for b in window]
    if any(v is None for v in o + h + l + c):
        return {}
    atr = _atr(bars)
    if atr <= 0:
        return {}

    recent_net = (c[-1] - c[-4]) / atr
    prior_net = (c[-4] - c[-7]) / atr
    slope_5 = _slope(c[-5:]) / atr
    slope_12 = _slope(c[-12:]) / atr
    prior_slope_5 = _slope(c[-8:-3]) / atr
    acceleration = slope_5 - prior_slope_5
    up_steps = sum(1 for i in range(len(c) - 3, len(c)) if c[i] > c[i - 1])
    down_steps = sum(1 for i in range(len(c) - 3, len(c)) if c[i] < c[i - 1])
    bullish_bodies = sum(1 for i in range(len(c) - 3, len(c)) if c[i] > o[i])
    bearish_bodies = sum(1 for i in range(len(c) - 3, len(c)) if c[i] < o[i])
    bullish_break = c[-1] > max(h[-5:-1])
    bearish_break = c[-1] < min(l[-5:-1])
    ranges = [h[i] - l[i] for i in range(len(c))]
    compression_ratio = (sum(ranges[-4:]) / 4.0) / atr
    expansion_ratio = ranges[-1] / atr
    body_ratio = abs(c[-1] - o[-1]) / ranges[-1] if ranges[-1] > 0 else 0.0
    pin_current = _pin_bar_metrics(window[-1], atr)
    pin_previous = _pin_bar_metrics(window[-2], atr)

    # Previous-candle pin remains relevant only when the latest close follows
    # through in the pin direction. This captures a pin + confirmation candle
    # cluster without letting an old isolated wick dominate the score.
    previous_bull_follow = c[-1] > c[-2]
    previous_bear_follow = c[-1] < c[-2]
    bullish_pin_cluster = max(
        int(pin_current.get("bullish_score") or 0),
        int(round((pin_previous.get("bullish_score") or 0) * 0.70))
        if previous_bull_follow else 0,
    )
    bearish_pin_cluster = max(
        int(pin_current.get("bearish_score") or 0),
        int(round((pin_previous.get("bearish_score") or 0) * 0.70))
        if previous_bear_follow else 0,
    )

    direction = "NEUTRAL"
    # Candidate requires displacement, slope reversal/acceleration and a close
    # through recent structure. It deliberately avoids candle-pattern names.
    if (
        recent_net >= 0.40
        and slope_5 >= 0.06
        and acceleration >= 0.06
        and up_steps >= 2
        and bullish_bodies >= 2
        and bullish_break
        and (prior_net <= 0.20 or prior_slope_5 <= 0.0)
    ):
        direction = "BULLISH"
    elif (
        recent_net <= -0.40
        and slope_5 <= -0.06
        and acceleration <= -0.06
        and down_steps >= 2
        and bearish_bodies >= 2
        and bearish_break
        and (prior_net >= -0.20 or prior_slope_5 >= 0.0)
    ):
        direction = "BEARISH"

    confidence = 0
    if direction != "NEUTRAL":
        confidence = int(round(min(100.0,
            25.0
            + min(30.0, abs(recent_net) * 25.0)
            + min(20.0, abs(acceleration) * 60.0)
            + (10.0 if abs(slope_5) > abs(slope_12) else 0.0)
            + (10.0 if body_ratio >= 0.55 else 0.0)
            + (5.0 if expansion_ratio >= 0.8 else 0.0)
        )))

    last = bars[-1]
    return {
        "schema_version": 1,
        "model": "DXY_M15_ATR_SLOPE_STRUCTURE_CANDIDATE_V1",
        "tf": TF,
        "broker_bar_open_ms": _bar_open_ms(last),
        "broker_bar_close_ms": _bar_close_ms(last),
        "atr": round(atr, 6),
        "recent_net_atr": round(recent_net, 4),
        "prior_net_atr": round(prior_net, 4),
        "slope_5_atr": round(slope_5, 5),
        "slope_12_atr": round(slope_12, 5),
        "prior_slope_5_atr": round(prior_slope_5, 5),
        "acceleration_atr": round(acceleration, 5),
        "up_steps": up_steps,
        "down_steps": down_steps,
        "bullish_bodies": bullish_bodies,
        "bearish_bodies": bearish_bodies,
        "bullish_structure_break": bool(bullish_break),
        "bearish_structure_break": bool(bearish_break),
        "compression_ratio": round(compression_ratio, 4),
        "expansion_ratio": round(expansion_ratio, 4),
        "body_ratio": round(body_ratio, 4),
        "pin_bar_direction": pin_current.get("direction"),
        "bullish_pin_score": int(pin_current.get("bullish_score") or 0),
        "bearish_pin_score": int(pin_current.get("bearish_score") or 0),
        "bullish_pin_cluster_score": int(bullish_pin_cluster),
        "bearish_pin_cluster_score": int(bearish_pin_cluster),
        "pin_bar_metrics": pin_current,
        "previous_pin_bar_metrics": pin_previous,
        "candidate_direction": direction,
        "candidate_confidence": confidence,
        "bars_used": MIN_FEATURE_BARS,
    }


def _append_event_once(R, event: dict) -> bool:
    source = str(event.get("source") or "")
    device = str(event.get("device_id") or "")
    close_ms = int(event.get("bar_close_ms") or 0)
    status = str(event.get("status") or "")
    direction = str(event.get("direction") or "")
    if not source or not device or close_ms <= 0 or not status or not direction:
        return False
    claim = _event_claim_key(source, device, close_ms, status, direction)
    try:
        if not R.set(claim, "1", nx=True, ex=HISTORY_TTL_SEC):
            return False
        pipe = R.pipeline()
        pipe.rpush(_history_key(source, device), json.dumps(event, separators=(",", ":"), default=str))
        pipe.ltrim(_history_key(source, device), -HISTORY_MAX_EVENTS, -1)
        pipe.expire(_history_key(source, device), HISTORY_TTL_SEC)
        pipe.execute()
        return True
    except Exception:
        try:
            R.delete(claim)
        except Exception:
            pass
        return False


def _qualify_candidate(
    features: dict,
    *,
    source: str,
) -> tuple[str, int, str]:
    """
    Qualify an early M15 reversal candidate.

    Returns:
        direction: BULLISH | BEARISH | NEUTRAL
        confidence: 0..100
        reason: audit reason

    The feature engine remains unchanged. This function decides whether
    the directional feature represents a credible reversal rather than
    continuation or late exhaustion.
    """
    raw_direction = str(
        features.get("candidate_direction")
        or "NEUTRAL"
    ).upper().strip()

    if raw_direction not in (
        "BULLISH",
        "BEARISH",
    ):
        return (
            "NEUTRAL",
            0,
            "FEATURE_DIRECTION_NEUTRAL",
        )

    # Synthetic validation initially requires the complete five-pair basket.
    # Real DXY does not use synthetic pair metadata.
    if source == "SYNTHETIC_DXY":
        pair_count = int(
            features.get("synthetic_pair_count")
            or 0
        )

        if pair_count != 5:
            return (
                "NEUTRAL",
                0,
                f"SYNTHETIC_PAIR_COUNT_{pair_count}",
            )

    try:
        recent_net_atr = float(
            features.get("recent_net_atr")
            or 0.0
        )
        prior_net_atr = float(
            features.get("prior_net_atr")
            or 0.0
        )
        slope_5_atr = float(
            features.get("slope_5_atr")
            or 0.0
        )
        prior_slope_5_atr = float(
            features.get("prior_slope_5_atr")
            or 0.0
        )
        acceleration_atr = float(
            features.get("acceleration_atr")
            or 0.0
        )
        expansion_ratio = float(
            features.get("expansion_ratio")
            or 0.0
        )
        body_ratio = float(
            features.get("body_ratio")
            or 0.0
        )
    except Exception:
        return (
            "NEUTRAL",
            0,
            "INVALID_NUMERIC_FEATURES",
        )

    bullish_break = bool(
        features.get("bullish_structure_break")
    )
    bearish_break = bool(
        features.get("bearish_structure_break")
    )
    try:
        up_steps = int(
            features.get("up_steps")
            or 0
        )
        down_steps = int(
            features.get("down_steps")
            or 0
        )
        bullish_bodies = int(
            features.get("bullish_bodies")
            or 0
        )
        bearish_bodies = int(
            features.get("bearish_bodies")
            or 0
        )
    except Exception:
        up_steps = 0
        down_steps = 0
        bullish_bodies = 0
        bearish_bodies = 0

    # Prevent late entries after an already overextended displacement.
    if abs(recent_net_atr) > 1.60:
        return (
            "NEUTRAL",
            0,
            "MOVE_ALREADY_EXHAUSTED",
        )

    # Require meaningful new movement, but not extreme movement.
    if abs(recent_net_atr) < 0.55:
        return (
            "NEUTRAL",
            0,
            "RECENT_MOVE_TOO_SMALL",
        )
    # Strong established-reversal override.
    #
    # The normal qualification path requires the exact slope-cross bar.
    # A powerful reversal can be detected one or two bars later, after the
    # slope has already crossed. In that case, do not report NEUTRAL when
    # displacement, structure, slope, acceleration and candle participation
    # all strongly agree.
    strong_bullish_reversal = (
        raw_direction == "BULLISH"
        and bullish_break
        and recent_net_atr >= 1.00
        and recent_net_atr <= 1.60
        and slope_5_atr >= 0.20
        and acceleration_atr >= 0.15
        and up_steps >= 3
        and bullish_bodies >= 3
        and body_ratio >= 0.35
    )

    strong_bearish_reversal = (
        raw_direction == "BEARISH"
        and bearish_break
        and recent_net_atr <= -1.00
        and recent_net_atr >= -1.60
        and slope_5_atr <= -0.20
        and acceleration_atr <= -0.15
        and down_steps >= 3
        and bearish_bodies >= 3
        and body_ratio >= 0.35
    )

    if strong_bullish_reversal or strong_bearish_reversal:
        confidence = 80

        confidence += min(
            10,
            int(
                max(
                    0.0,
                    abs(recent_net_atr) - 1.00,
                )
                * 20
            ),
        )

        confidence += min(
            5,
            int(
                abs(acceleration_atr)
                * 10
            ),
        )

        if body_ratio >= 0.50:
            confidence += 5

        return (
            raw_direction,
            min(
                100,
                int(confidence),
            ),
            "QUALIFIED_STRONG_ESTABLISHED_REVERSAL",
        )

    if raw_direction == "BULLISH":
        if not bullish_break:
            return (
                "NEUTRAL",
                0,
                "NO_BULLISH_STRUCTURE_BREAK",
            )

        # The short slope must cross from non-positive to positive.
        if not (
            prior_slope_5_atr <= 0.0
            and slope_5_atr > 0.0
        ):
            return (
                "NEUTRAL",
                0,
                "NO_BULLISH_SLOPE_CROSS",
            )

        # A reversal must follow meaningful bearish pressure.
        if prior_net_atr > -0.30:
            return (
                "NEUTRAL",
                0,
                "NO_PRIOR_BEARISH_PRESSURE",
            )

        if acceleration_atr <= 0.0:
            return (
                "NEUTRAL",
                0,
                "BULLISH_ACCELERATION_MISSING",
            )

    else:
        if not bearish_break:
            return (
                "NEUTRAL",
                0,
                "NO_BEARISH_STRUCTURE_BREAK",
            )

        # The short slope must cross from non-negative to negative.
        if not (
            prior_slope_5_atr >= 0.0
            and slope_5_atr < 0.0
        ):
            return (
                "NEUTRAL",
                0,
                "NO_BEARISH_SLOPE_CROSS",
            )

        # A reversal must follow meaningful bullish pressure.
        if prior_net_atr < 0.30:
            return (
                "NEUTRAL",
                0,
                "NO_PRIOR_BULLISH_PRESSURE",
            )

        if acceleration_atr >= 0.0:
            return (
                "NEUTRAL",
                0,
                "BEARISH_ACCELERATION_MISSING",
            )

    # Confidence is computed only after all mandatory conditions pass.
    confidence = 50

    # Opposite prior pressure.
    confidence += min(
        15,
        int(
            abs(prior_net_atr)
            * 12
        ),
    )

    # Fresh directional displacement.
    confidence += min(
        15,
        int(
            abs(recent_net_atr)
            * 10
        ),
    )

    # Acceleration strength.
    confidence += min(
        10,
        int(
            abs(acceleration_atr)
            * 20
        ),
    )

    # Healthy candle participation.
    if body_ratio >= 0.35:
        confidence += 5

    # Expansion without excessive exhaustion.
    if 1.0 <= expansion_ratio <= 1.8:
        confidence += 5

    return (
        raw_direction,
        min(
            100,
            int(confidence),
        ),
        "QUALIFIED_REVERSAL",
    )

def _directional_move_atr(
    direction: str,
    start_px: float,
    current_px: float,
    atr: float,
) -> float:
    if atr <= 0 or start_px <= 0 or current_px <= 0:
        return 0.0
    move = (current_px - start_px) / atr
    return move if str(direction).upper() == "BULLISH" else -move


def _evidence_scores(features: dict) -> tuple[int, int, dict]:
    """Return independent bullish/bearish evidence scores for one closed M15 bar."""
    recent = float(features.get("recent_net_atr") or 0.0)
    prior = float(features.get("prior_net_atr") or 0.0)
    slope5 = float(features.get("slope_5_atr") or 0.0)
    slope12 = float(features.get("slope_12_atr") or 0.0)
    prior_slope5 = float(features.get("prior_slope_5_atr") or 0.0)
    accel = float(features.get("acceleration_atr") or 0.0)
    body = float(features.get("body_ratio") or 0.0)
    expansion = float(features.get("expansion_ratio") or 0.0)
    up_steps = int(features.get("up_steps") or 0)
    down_steps = int(features.get("down_steps") or 0)
    bull_bodies = int(features.get("bullish_bodies") or 0)
    bear_bodies = int(features.get("bearish_bodies") or 0)
    bull_break = bool(features.get("bullish_structure_break"))
    bear_break = bool(features.get("bearish_structure_break"))
    bullish_pin = int(features.get("bullish_pin_score") or 0)
    bearish_pin = int(features.get("bearish_pin_score") or 0)
    bullish_pin_cluster = int(features.get("bullish_pin_cluster_score") or 0)
    bearish_pin_cluster = int(features.get("bearish_pin_cluster_score") or 0)

    bull = 0.0
    bear = 0.0
    bull_parts = {}
    bear_parts = {}

    def add(parts: dict, name: str, value: float) -> float:
        if value > 0:
            parts[name] = round(value, 2)
        return max(0.0, value)

    bull += add(bull_parts, "recent_displacement", min(22.0, max(0.0, recent) * 18.0))
    bear += add(bear_parts, "recent_displacement", min(22.0, max(0.0, -recent) * 18.0))

    bull += add(bull_parts, "short_slope", min(16.0, max(0.0, slope5) * 48.0))
    bear += add(bear_parts, "short_slope", min(16.0, max(0.0, -slope5) * 48.0))

    bull += add(bull_parts, "acceleration", min(14.0, max(0.0, accel) * 36.0))
    bear += add(bear_parts, "acceleration", min(14.0, max(0.0, -accel) * 36.0))

    if prior_slope5 <= 0 < slope5:
        bull += add(bull_parts, "slope_cross", 12.0)
    if prior_slope5 >= 0 > slope5:
        bear += add(bear_parts, "slope_cross", 12.0)

    if prior < 0:
        bull += add(bull_parts, "opposite_prior_pressure", min(10.0, abs(prior) * 8.0))
    if prior > 0:
        bear += add(bear_parts, "opposite_prior_pressure", min(10.0, abs(prior) * 8.0))

    if bull_break:
        bull += add(bull_parts, "structure_break", 18.0)
    if bear_break:
        bear += add(bear_parts, "structure_break", 18.0)

    # Pin bars are bounded supporting evidence. The cluster score recognizes
    # a pin on the prior candle followed by directional confirmation now.
    bull += add(
        bull_parts,
        "bullish_pin_bar",
        min(PIN_EVIDENCE_MAX, bullish_pin * PIN_EVIDENCE_MAX / 100.0),
    )
    bear += add(
        bear_parts,
        "bearish_pin_bar",
        min(PIN_EVIDENCE_MAX, bearish_pin * PIN_EVIDENCE_MAX / 100.0),
    )
    if bullish_pin_cluster > bullish_pin:
        bull += add(
            bull_parts,
            "bullish_pin_follow_through",
            min(PIN_CLUSTER_BONUS_MAX, bullish_pin_cluster * PIN_CLUSTER_BONUS_MAX / 100.0),
        )
    if bearish_pin_cluster > bearish_pin:
        bear += add(
            bear_parts,
            "bearish_pin_follow_through",
            min(PIN_CLUSTER_BONUS_MAX, bearish_pin_cluster * PIN_CLUSTER_BONUS_MAX / 100.0),
        )

    bull += add(bull_parts, "close_steps", min(9.0, up_steps * 3.0))
    bear += add(bear_parts, "close_steps", min(9.0, down_steps * 3.0))
    bull += add(bull_parts, "body_agreement", min(9.0, bull_bodies * 3.0))
    bear += add(bear_parts, "body_agreement", min(9.0, bear_bodies * 3.0))

    if body >= 0.45:
        if recent > 0:
            bull += add(bull_parts, "body_quality", 5.0)
        elif recent < 0:
            bear += add(bear_parts, "body_quality", 5.0)

    if 0.75 <= expansion <= 1.80:
        if recent > 0:
            bull += add(bull_parts, "healthy_expansion", 5.0)
        elif recent < 0:
            bear += add(bear_parts, "healthy_expansion", 5.0)

    # Penalize an already exhausted burst. It may be the end, not the start, of a turn.
    if abs(recent) > 1.80:
        penalty = min(20.0, (abs(recent) - 1.80) * 12.0 + 8.0)
        if recent > 0:
            bull -= penalty
            bull_parts["exhaustion_penalty"] = round(-penalty, 2)
        else:
            bear -= penalty
            bear_parts["exhaustion_penalty"] = round(-penalty, 2)

    # Medium slope is supporting evidence, not a mandatory gate.
    if slope12 > 0:
        bull += add(bull_parts, "medium_slope", min(5.0, slope12 * 12.0))
    elif slope12 < 0:
        bear += add(bear_parts, "medium_slope", min(5.0, -slope12 * 12.0))

    return (
        int(round(max(0.0, min(100.0, bull)))),
        int(round(max(0.0, min(100.0, bear)))),
        {"bull_parts": bull_parts, "bear_parts": bear_parts},
    )


def _revoke_evidence(
    *,
    direction: str,
    features: dict,
    directional_move_atr: float,
    bull_score: int,
    bear_score: int,
) -> tuple[int, list[str]]:
    """Combined failure evidence; one ordinary pullback must not revoke a turn."""
    direction = str(direction or "").upper()
    reasons: list[str] = []
    score = 0

    slope5 = float(features.get("slope_5_atr") or 0.0)
    accel = float(features.get("acceleration_atr") or 0.0)
    bull_break = bool(features.get("bullish_structure_break"))
    bear_break = bool(features.get("bearish_structure_break"))
    bull_bodies = int(features.get("bullish_bodies") or 0)
    bear_bodies = int(features.get("bearish_bodies") or 0)

    opposite_score = bear_score if direction == "BULLISH" else bull_score
    active_score = bull_score if direction == "BULLISH" else bear_score

    if directional_move_atr <= -REVOKE_ADVERSE_ATR:
        score += 30
        reasons.append("ADVERSE_MOVE_0_5_ATR")
    elif directional_move_atr <= -0.30:
        score += 18
        reasons.append("ADVERSE_MOVE_0_3_ATR")

    if direction == "BULLISH":
        if slope5 < 0:
            score += 18
            reasons.append("SLOPE_FLIPPED_BEARISH")
        if accel < 0:
            score += 10
            reasons.append("BEARISH_ACCELERATION")
        if bear_break:
            score += 32
            reasons.append("BEARISH_STRUCTURE_BREAK")
        if bear_bodies >= 2:
            score += 12
            reasons.append("REPEATED_BEARISH_BODIES")
    else:
        if slope5 > 0:
            score += 18
            reasons.append("SLOPE_FLIPPED_BULLISH")
        if accel > 0:
            score += 10
            reasons.append("BULLISH_ACCELERATION")
        if bull_break:
            score += 32
            reasons.append("BULLISH_STRUCTURE_BREAK")
        if bull_bodies >= 2:
            score += 12
            reasons.append("REPEATED_BULLISH_BODIES")

    if opposite_score >= 60 and opposite_score - active_score >= 18:
        score += 25
        reasons.append("OPPOSITE_EVIDENCE_DOMINANT")

    return min(100, int(score)), reasons





def _public_zone(zone: dict | None, *, tf: str, role: str) -> dict | None:
    """Return compact, chart-friendly causal SR metadata."""
    if not isinstance(zone, dict) or not zone:
        return None
    level = float(zone.get("level") or 0.0)
    low = float(zone.get("low") or level or 0.0)
    high = float(zone.get("high") or level or 0.0)
    if level <= 0:
        return None
    return {
        "role": role,
        "tf": tf,
        "price": round(level, 6),
        "low": round(low, 6),
        "high": round(high, 6),
        "distance_atr": zone.get("distance_atr"),
        "near": bool(zone.get("near")),
        "touches": int(zone.get("touches") or 0),
        "strength": int(zone.get("strength") or 0),
        "maturity": _zone_maturity(zone),
        "age_bars": int(zone.get("age_bars") or 0),
        "source_tf": str(zone.get("source_tf") or tf),
        "causal": bool(zone.get("causal", True)),
    }


def _structure_aware_snapshot(sr_audit: dict, *, flow_direction: str) -> dict:
    """Expose causal M15/H1/H4 SR and directional room in one compact object."""
    if not isinstance(sr_audit, dict):
        sr_audit = {}
    context = sr_audit.get("structure_context") or {}
    zones = []
    for tf_key in ("m15", "h1", "h4"):
        snap = sr_audit.get(tf_key) or {}
        tf = str(snap.get("tf") or tf_key.upper())
        sup = _public_zone(snap.get("nearest_support"), tf=tf, role="SUPPORT")
        res = _public_zone(snap.get("nearest_resistance"), tf=tf, role="RESISTANCE")
        if sup:
            zones.append(sup)
        if res:
            zones.append(res)

    supports = sorted(
        [z for z in zones if z.get("role") == "SUPPORT"],
        key=lambda z: float(z.get("price") or 0.0),
        reverse=True,
    )
    resistances = sorted(
        [z for z in zones if z.get("role") == "RESISTANCE"],
        key=lambda z: float(z.get("price") or 0.0),
    )
    nearest_support = supports[0] if supports else None
    nearest_resistance = resistances[0] if resistances else None
    next_support = supports[1] if len(supports) > 1 else None
    next_resistance = resistances[1] if len(resistances) > 1 else None

    room_up = context.get("available_upside_atr")
    room_down = context.get("available_downside_atr")
    room_up = float(room_up) if room_up is not None else None
    room_down = float(room_down) if room_down is not None else None
    flow_direction = str(flow_direction or "NEUTRAL").upper()
    directional_room = room_up if flow_direction == "BULLISH" else room_down if flow_direction == "BEARISH" else None
    near_tfs = (
        sr_audit.get("near_resistance_tfs") if flow_direction == "BULLISH"
        else sr_audit.get("near_support_tfs") if flow_direction == "BEARISH"
        else []
    ) or []
    confluence = len(near_tfs)

    if directional_room is None:
        pressure = "UNKNOWN"
    elif directional_room <= 0.10 or confluence >= 3:
        pressure = "EXTREME"
    elif directional_room <= 0.35 or confluence >= 2:
        pressure = "HIGH"
    elif directional_room <= 0.80 or confluence >= 1:
        pressure = "MEDIUM"
    else:
        pressure = "LOW"

    return {
        "schema_version": 1,
        "context": context.get("context") or "NO_STRUCTURE",
        "current_price": context.get("current_price"),
        "nearest_support": nearest_support,
        "next_support": next_support,
        "nearest_resistance": nearest_resistance,
        "next_resistance": next_resistance,
        "room_up_atr": round(room_up, 4) if room_up is not None else None,
        "room_down_atr": round(room_down, 4) if room_down is not None else None,
        "directional_room_atr": round(directional_room, 4) if directional_room is not None else None,
        "pressure": pressure,
        "pressure_direction": flow_direction,
        "near_support_tfs": list(sr_audit.get("near_support_tfs") or []),
        "near_resistance_tfs": list(sr_audit.get("near_resistance_tfs") or []),
        "bullish_confluence_count": int(sr_audit.get("bullish_confluence_count") or 0),
        "bearish_confluence_count": int(sr_audit.get("bearish_confluence_count") or 0),
        "bullish_h1_sweep_reclaim": bool(context.get("bullish_h1_sweep_reclaim")),
        "bearish_h1_sweep_reject": bool(context.get("bearish_h1_sweep_reject")),
        "sweep_conflict": bool(context.get("sweep_conflict")),
        "audit_only": True,
    }



def _bars_to_sr_frame(bars: list[dict]):
    """Convert completed aggregated DXY bars to the DataFrame expected by trend_sr.py."""
    import pandas as pd

    rows = []
    for bar in bars or []:
        if not isinstance(bar, dict) or not bool(bar.get("complete", True)):
            continue
        o = _safe_float(bar.get("o"))
        h = _safe_float(bar.get("h"))
        l = _safe_float(bar.get("l"))
        c = _safe_float(bar.get("c"))
        if None in (o, h, l, c):
            continue
        rows.append({
            "t_open_ms": _bar_open_ms(bar),
            "t_close_ms": _bar_close_ms(bar),
            "o": float(o),
            "h": float(h),
            "l": float(l),
            "c": float(c),
        })

    if not rows:
        return pd.DataFrame(columns=["t_open_ms", "t_close_ms", "o", "h", "l", "c"])

    frame = pd.DataFrame(rows)
    frame = frame.sort_values("t_open_ms").drop_duplicates("t_open_ms", keep="last")
    return frame.reset_index(drop=True)


def _persist_dxy_sr_bundle(R, *, source: str, device_id: str, bundle: dict) -> None:
    """Publish source/device-specific DXY SR bundles without touching normal-symbol SR keys."""
    if not isinstance(bundle, dict):
        return
    try:
        payload = dict(bundle)
        payload["_dxy_source"] = source
        payload["_device_id"] = device_id
        payload["_structure_engine"] = "trend_sr.py"
        payload["_published_at_ms"] = int(time.time() * 1000)
        raw = json.dumps(payload, separators=(",", ":"), default=str)
        R.set(
            f"{DXY_SR_LAST_PREFIX}:{source}:{device_id}",
            raw,
            
        )
        if (
            payload.get("active_supports")
            or payload.get("active_resistances")
            or payload.get("nearest_support")
            or payload.get("nearest_resistance")
        ):
            R.set(
                f"{DXY_SR_GOOD_PREFIX}:{source}:{device_id}",
                raw,
                
            )
    except Exception:
        log.exception(
            "[DXY_M15] SHARED_SR_PERSIST_FAILED source=%s device=%s",
            source,
            device_id,
        )


def _market_flow_state(
    *,
    old: dict,
    features: dict,
    bull_score: int,
    bear_score: int,
    close_ms: int,
    structure: dict | None = None,
) -> dict:
    """Fast current-flow state with two-bar promotion and hysteresis.

    This is part of the same DXY engine but does not replace the conservative
    turn lifecycle. It answers what DXY is doing now, while turn status answers
    whether a reversal has been formally validated.
    """
    current_direction = (
        "BULLISH" if bull_score > bear_score else
        "BEARISH" if bear_score > bull_score else
        "NEUTRAL"
    )
    current_score = max(int(bull_score), int(bear_score))
    current_margin = abs(int(bull_score) - int(bear_score))
    recent_net = float(features.get("recent_net_atr") or 0.0)
    slope5 = float(features.get("slope_5_atr") or 0.0)
    structure_break = bool(
        features.get("bullish_structure_break")
        if current_direction == "BULLISH"
        else features.get("bearish_structure_break")
    )

    current_sign_ok = (
        current_direction == "BULLISH" and recent_net > 0 and slope5 > 0
    ) or (
        current_direction == "BEARISH" and recent_net < 0 and slope5 < 0
    )
    current_signal = (
        current_direction in ("BULLISH", "BEARISH")
        and current_score >= FLOW_START_SCORE
        and current_margin >= FLOW_START_MARGIN
        and current_sign_ok
    )
    strong_single = (
        current_signal
        and current_score >= FLOW_STRONG_SINGLE_SCORE
        and current_margin >= FLOW_STRONG_SINGLE_MARGIN
        and abs(recent_net) >= 0.55
    )

    old_flow = old.get("market_flow") if isinstance(old, dict) else {}
    if not isinstance(old_flow, dict):
        old_flow = {}
    old_direction = str(old_flow.get("direction") or "NEUTRAL").upper()
    old_stage = str(old_flow.get("stage") or "NEUTRAL").upper()
    old_support_bars = int(old_flow.get("support_bars") or 0)
    old_opposite_bars = int(old_flow.get("opposite_bars") or 0)
    old_weak_bars = int(old_flow.get("weak_bars") or 0)
    old_started_ms = int(old_flow.get("started_ms") or 0)

    prev_features = old.get("features") if isinstance(old, dict) else {}
    if not isinstance(prev_features, dict):
        prev_features = {}
    prev_direction = str(prev_features.get("evidence_direction") or "NEUTRAL").upper()
    prev_bull = int(prev_features.get("bull_evidence_score") or 0)
    prev_bear = int(prev_features.get("bear_evidence_score") or 0)
    prev_score = max(prev_bull, prev_bear)
    prev_margin = abs(prev_bull - prev_bear)
    previous_support = (
        prev_direction == current_direction
        and prev_score >= FLOW_START_SCORE
        and prev_margin >= FLOW_START_MARGIN
    )
    two_bar_signal = bool(current_signal and previous_support)

    direction = old_direction
    stage = old_stage
    support_bars = old_support_bars
    opposite_bars = old_opposite_bars
    weak_bars = old_weak_bars
    started_ms = old_started_ms
    reason = "NO_DIRECTIONAL_FLOW"

    if old_direction not in ("BULLISH", "BEARISH"):
        direction = "NEUTRAL"
        stage = "NEUTRAL"
        support_bars = 0
        opposite_bars = 0
        weak_bars = 0
        started_ms = 0
        if strong_single or two_bar_signal:
            direction = current_direction
            stage = "EARLY" if strong_single and not two_bar_signal else "BUILDING"
            support_bars = 2 if two_bar_signal else 1
            started_ms = close_ms
            reason = "STRONG_SINGLE_BAR_FLOW" if strong_single and not two_bar_signal else "TWO_BAR_DIRECTIONAL_FLOW"
    elif current_signal and current_direction == old_direction:
        direction = old_direction
        support_bars += 1
        opposite_bars = 0
        weak_bars = 0
        stage = "STRONG" if support_bars >= 3 else "BUILDING"
        reason = "FLOW_CONTINUES"
    elif current_signal and current_direction != old_direction:
        opposite_bars += 1
        support_bars = max(1, support_bars - 1)
        weak_bars += 1
        hard_flip = (
            current_score >= FLOW_FLIP_SCORE
            and current_margin >= FLOW_FLIP_MARGIN
            and abs(recent_net) >= FLOW_HARD_FLIP_NET_ATR
            and (structure_break or opposite_bars >= 2)
        )
        if hard_flip or opposite_bars >= 2:
            direction = current_direction
            stage = "EARLY" if hard_flip else "BUILDING"
            support_bars = 1 if hard_flip else 2
            opposite_bars = 0
            weak_bars = 0
            started_ms = close_ms
            reason = "HARD_FLOW_REVERSAL" if hard_flip else "TWO_BAR_FLOW_REVERSAL"
        else:
            direction = old_direction
            stage = "WEAKENING"
            reason = "ONE_BAR_OPPOSITE_EVIDENCE_HELD_BY_HYSTERESIS"
    else:
        opposite_bars = 0
        weak_bars += 1
        support_bars = max(0, support_bars - 1)
        if current_direction == old_direction and current_score >= FLOW_MAINTAIN_SCORE and current_margin >= FLOW_MAINTAIN_MARGIN:
            direction = old_direction
            stage = "WEAKENING"
            reason = "FLOW_WEAK_BUT_DIRECTION_STILL_DOMINANT"
        elif weak_bars >= FLOW_NEUTRAL_AFTER_BARS:
            direction = "NEUTRAL"
            stage = "NEUTRAL"
            support_bars = 0
            weak_bars = 0
            started_ms = 0
            reason = "FLOW_DECAYED_TO_NEUTRAL"
        else:
            direction = old_direction
            stage = "WEAKENING"
            reason = "FLOW_MOMENTUM_PAUSED"

    directional_score = bull_score if direction == "BULLISH" else bear_score if direction == "BEARISH" else 0
    opposite_score = bear_score if direction == "BULLISH" else bull_score if direction == "BEARISH" else 0
    evidence_margin = max(0, directional_score - opposite_score)
    expansion = float(features.get("expansion_ratio") or 0.0)
    acceleration = abs(float(features.get("acceleration_atr") or 0.0))

    # Strength describes force of movement. Confidence describes reliability.
    strength = int(max(0, min(100, round(
        directional_score * 0.55
        + min(25.0, abs(recent_net) * 18.0)
        + min(10.0, abs(slope5) * 20.0)
        + min(10.0, expansion * 8.0)
    )))) if direction != "NEUTRAL" else 0
    confidence = int(max(0, min(100, round(
        directional_score * 0.50
        + evidence_margin * 0.30
        + min(15.0, support_bars * 5.0)
        + (5.0 if structure_break else 0.0)
        - min(10.0, acceleration * 8.0 if stage == "WEAKENING" else 0.0)
    )))) if direction != "NEUTRAL" else 0

    structure = structure if isinstance(structure, dict) else {}
    pressure = str(
        structure.get("structure_pressure")
        or structure.get("pressure")
        or "UNKNOWN"
    ).upper()
    directional_room = structure.get("directional_room_atr")
    if direction in ("BULLISH", "BEARISH"):
        if pressure in ("HIGH", "EXTREME") and stage in ("BUILDING", "STRONG"):
            stage = "EXHAUSTING"
            reason = "FLOW_AT_DIRECTIONAL_STRUCTURE_PRESSURE"
            confidence = max(0, confidence - (15 if pressure == "EXTREME" else 8))
        elif stage == "WEAKENING" and opposite_bars > 0:
            stage = "REVERSING"
        elif support_bars >= 4 and stage == "BUILDING":
            stage = "STRONG"

    return {
        "schema_version": 1,
        "direction": direction,
        "strength": strength,
        "confidence": confidence,
        "stage": stage,
        "reason": reason,
        "support_bars": support_bars,
        "opposite_bars": opposite_bars,
        "weak_bars": weak_bars,
        "started_ms": started_ms or None,
        "updated_ms": close_ms,
        "current_feature_direction": current_direction,
        "current_feature_score": current_score,
        "current_feature_margin": current_margin,
        "two_bar_signal": two_bar_signal,
        "strong_single_signal": strong_single,
        "recent_net_atr": round(recent_net, 4),
        "slope_5_atr": round(slope5, 5),
        "structure_break": structure_break,
        "structure_pressure": pressure,
        "directional_room_atr": directional_room,
        "structure": structure,
        "shadow_only": True,
    }

def _synthetic_health(bar: dict, *, detected_at_ms: int, close_ms: int) -> dict:
    pairs = [str(x).upper().strip() for x in ((bar or {}).get("synthetic_pairs") or []) if str(x).strip()]
    pairs = list(dict.fromkeys(pairs))
    missing = [x for x in SYNTHETIC_EXPECTED_PAIRS if x not in pairs]
    count = int((bar or {}).get("synthetic_pair_count") or len(pairs) or 0)
    age_ms = max(0, int(detected_at_ms or 0) - int(close_ms or 0))
    complete = count == SYNTHETIC_EXPECTED_PAIR_COUNT and not missing
    return {
        "synthetic_health": "COMPLETE" if complete else "DEGRADED",
        "synthetic_complete": bool(complete),
        "synthetic_expected_pair_count": SYNTHETIC_EXPECTED_PAIR_COUNT,
        "synthetic_pair_count": count,
        "synthetic_pairs": pairs,
        "synthetic_missing_pairs": missing,
        "synthetic_close_age_ms": age_ms,
        "synthetic_grace_ms": SYNTHETIC_CLOSE_GRACE_MS,
        "synthetic_grace_complete": age_ms >= SYNTHETIC_CLOSE_GRACE_MS,
    }


def _persist_degraded_health(R, *, source: str, device_id: str, close_ms: int,
                             broker_close_ms: int, offset_min: int, detected_at_ms: int,
                             health: dict) -> None:
    reason = (
        "SYNTHETIC_GRACE_WAIT" if not health.get("synthetic_grace_complete")
        else f"SYNTHETIC_PAIR_COUNT_{int(health.get('synthetic_pair_count') or 0)}"
    )
    diagnostic = {
        "schema_version": 12,
        "model": "DXY_M15_UNIFIED_MARKET_STATE_V12_COMPLETE_BASKET",
        "source": source,
        "device_id": device_id,
        "bar_close_ms": int(close_ms),
        "broker_bar_close_ms": int(broker_close_ms),
        "broker_offset_minutes": int(offset_min),
        "detected_at_ms": int(detected_at_ms),
        "evaluation_final": False,
        "candidate_qualified": False,
        "candidate_qualification_reason": reason,
        **health,
    }
    R.set(_features_key(source, device_id, close_ms), json.dumps(diagnostic, separators=(",", ":"), default=str), ex=HISTORY_TTL_SEC)
    old = _json_load(R.get(_state_key(source, device_id)), {})
    if not isinstance(old, dict):
        old = {}
    state = dict(old)
    state.update({
        "schema_version": max(12, int(old.get("schema_version") or 0)),
        "model": "DXY_M15_UNIFIED_MARKET_STATE_V12_COMPLETE_BASKET",
        "source": source,
        "device_id": device_id,
        "basket_health": health,
        "synthetic_health": health.get("synthetic_health"),
        "synthetic_complete": health.get("synthetic_complete"),
        "synthetic_expected_pair_count": health.get("synthetic_expected_pair_count"),
        "synthetic_pair_count": health.get("synthetic_pair_count"),
        "synthetic_pairs": health.get("synthetic_pairs"),
        "synthetic_missing_pairs": health.get("synthetic_missing_pairs"),
        "latest_degraded_bar_close_ms": int(close_ms),
        "latest_degraded_detected_at_ms": int(detected_at_ms),
        "evaluation_pending_retry": True,
        "shadow_only": True,
    })
    R.set(_state_key(source, device_id), json.dumps(state, separators=(",", ":"), default=str))


def _history_last_confirmed_direction(R, source: str, device_id: str) -> str | None:
    try:
        for raw in reversed(R.lrange(_history_key(source, device_id), -100, -1) or []):
            event = _json_load(raw, {})
            if not isinstance(event, dict):
                continue
            status = str(event.get("status") or "").upper().strip()
            direction = str(event.get("direction") or "").upper().strip()
            if status in ("CONFIRMED", "COMPLETED", "WEAK_COMPLETION", "INVALIDATED") and direction in ("BULLISH", "BEARISH"):
                return direction
    except Exception:
        pass
    return None


def _evaluate_one(R, *, source: str, device_id: str, binding: dict, bars: list[dict], index: int,
                  detected_at_ms: int, offset_min: int, historical: bool) -> dict:
    prefix = bars[:index + 1]
    features = _feature_snapshot(prefix)
    if not features:
        return {"ok": False, "reason": "FEATURES_EMPTY"}

    broker_close_ms = int(features.get("broker_bar_close_ms") or 0)
    close_ms = _broker_to_utc_ms(broker_close_ms, offset_min)
    if close_ms <= 0 or close_ms > detected_at_ms + 120000:
        return {"ok": False, "reason": "FUTURE_OR_INVALID_BAR"}

    current_close = float(bars[index].get("c") or 0.0)
    current_atr = float(features.get("atr") or 0.0)
    sr_audit = _sr_audit_snapshot(prefix, current_price=current_close)
    features.update({
        "source": source,
        "device_id": device_id,
        "bar_close_ms": close_ms,
        "broker_offset_minutes": offset_min,
        "detected_at_ms": detected_at_ms,
        "synthetic_pair_count": bars[index].get("synthetic_pair_count"),
        "synthetic_pairs": bars[index].get("synthetic_pairs"),
        "historical_backfill": bool(historical),
        "sr_audit": sr_audit,
        "sr_score_enabled": False,
        "sr_score_applied": 0,
    })

    raw_direction = str(features.get("candidate_direction") or "NEUTRAL").upper().strip()
    raw_confidence = int(features.get("candidate_confidence") or 0)
    qualified_direction, qualified_confidence, qualification_reason = _qualify_candidate(
        features,
        source=source,
    )
    bull_score, bear_score, evidence_detail = _evidence_scores(features)
    score_direction = (
        "BULLISH" if bull_score > bear_score else
        "BEARISH" if bear_score > bull_score else
        "NEUTRAL"
    )
    score_margin = abs(bull_score - bear_score)

    if source == "SYNTHETIC_DXY":
        features.update(_synthetic_health(bars[index], detected_at_ms=detected_at_ms, close_ms=close_ms))
    else:
        features.update({
            "synthetic_health": None,
            "synthetic_complete": None,
            "synthetic_expected_pair_count": None,
            "synthetic_missing_pairs": [],
        })

    features.update({
        "schema_version": 12,
        "model": "DXY_M15_UNIFIED_MARKET_STATE_V12_COMPLETE_BASKET",
        "raw_candidate_direction": raw_direction,
        "raw_candidate_confidence": raw_confidence,
        "strict_qualified_direction": qualified_direction,
        "strict_qualified_confidence": int(qualified_confidence),
        "candidate_qualification_reason": qualification_reason,
        "candidate_qualified": qualified_direction in ("BULLISH", "BEARISH"),
        "bull_evidence_score": bull_score,
        "bear_evidence_score": bear_score,
        "evidence_direction": score_direction,
        "evidence_margin": score_margin,
        "evidence_detail": evidence_detail,
    })

    old = _json_load(R.get(_state_key(source, device_id)), {})
    if not isinstance(old, dict):
        old = {}
    last_confirmed_direction = str(
        old.get("last_confirmed_direction")
        or old.get("previous_direction")
        or _history_last_confirmed_direction(R, source, device_id)
        or ""
    ).upper().strip()
    if last_confirmed_direction not in ("BULLISH", "BEARISH"):
        last_confirmed_direction = None
    previous_direction = str(old.get("previous_direction") or last_confirmed_direction or "").upper().strip()
    if previous_direction not in ("BULLISH", "BEARISH"):
        previous_direction = None

    provisional_flow_direction = (
        "BULLISH" if bull_score > bear_score else
        "BEARISH" if bear_score > bull_score else
        "NEUTRAL"
    )

    # Build causal completed H1/H4 bars from this evaluation prefix, then ask
    # the canonical XTL SR engine for structure.
    h1_bars = _aggregate_bars_causal(prefix, 4 * TF_MS)
    h4_bars = _aggregate_bars_causal(prefix, 16 * TF_MS)
    h1_df = _bars_to_sr_frame(h1_bars)
    h4_df = _bars_to_sr_frame(h4_bars)

    bundle = summarize_sr_multi_tf(
        symbol=f"{source}:{device_id}",
        price=current_close,
        h4_df=h4_df,
        h1_df=h1_df,
        pip_factor=DXY_SR_PIP_FACTOR,
        cache=None,
    )
    if not historical:
        _persist_dxy_sr_bundle(
            R,
            source=source,
            device_id=device_id,
            bundle=bundle,
        )

    structure = build_market_structure_state(
        bundle,
        price=current_close,
        flow_direction=provisional_flow_direction,
    )

    market_flow = _market_flow_state(
        old=old,
        features=features,
        bull_score=bull_score,
        bear_score=bear_score,
        close_ms=close_ms,
        structure=structure,
    )
    # Recalculate directional pressure using the persisted/hysteretic flow direction.
    structure = build_market_structure_state(
        bundle,
        price=current_close,
        flow_direction=market_flow.get("direction"),
    )
    log.warning(
        "[DXY] STRUCTURE_PATH_DEBUG "
        "dir=%s "
        "support_path=%d "
        "resistance_path=%d",
        market_flow.get("direction"),
        len(structure.get("support_path") or []),
        len(structure.get("resistance_path") or []),
    )

    market_flow["structure"] = structure
    market_flow["structure_pressure"] = structure.get(
        "structure_pressure"
    )
    market_flow["directional_room_atr"] = structure.get(
        "directional_room_atr"
    )

    features["market_flow"] = market_flow
    features["structure"] = structure
    features["structure_source"] = "trend_sr"

    # ------------------------------------------------------------
    # Shadow analytics: track both directional SR candidates.
    #
    # NEAREST:
    #   Closest canonical active SR in the flow direction.
    #
    # BEST:
    #   Highest-scored valid SR in the flow direction.
    #
    # This does not change flow, pressure, target selection, or execution.
    # Future hit-order/timestamps belong to analytics tracking.
    # ------------------------------------------------------------
    flow_dir = str(
        market_flow.get("direction")
        or "NEUTRAL"
    ).upper().strip()

    if flow_dir == "BULLISH":
        structure_targets = {
            "schema_version": 1,
            "analytics_only": True,
            "direction": "BULLISH",
            "role": "RESISTANCE",
            "created_at_bar_close_ms": int(close_ms),
            "origin_price": float(current_close),

            "nearest": structure.get("nearest_resistance"),
            "best": structure.get("best_resistance"),
            "path": structure.get("resistance_path") or [],
            "nearest_reached": False,
            "nearest_reached_at_ms": None,
            "best_reached": False,
            "best_reached_at_ms": None,
            "first_reached": None,
        }

    elif flow_dir == "BEARISH":
        structure_targets = {
            "schema_version": 1,
            "analytics_only": True,
            "direction": "BEARISH",
            "role": "SUPPORT",
            "created_at_bar_close_ms": int(close_ms),
            "origin_price": float(current_close),

            "nearest": structure.get("nearest_support"),
            "best": structure.get("best_support"),
            "path": structure.get("support_path") or [],
            "nearest_reached": False,
            "nearest_reached_at_ms": None,
            "best_reached": False,
            "best_reached_at_ms": None,
            "first_reached": None,
        }

    else:
        structure_targets = {
            "schema_version": 1,
            "analytics_only": True,
            "direction": "NEUTRAL",
            "role": None,
            "created_at_bar_close_ms": int(close_ms),
            "origin_price": float(current_close),

            "nearest": None,
            "best": None,

            "nearest_reached": False,
            "nearest_reached_at_ms": None,
            "best_reached": False,
            "best_reached_at_ms": None,
            "first_reached": None,
        }
    # ---------------------------------------------------------
    # Keep only the first 5 structure levels.
    # ---------------------------------------------------------
    if structure_targets.get("path"):
        structure_targets["path"] = structure_targets["path"][:5]

    # Save in per-bar features as well as the live state.
    features["structure_targets"] = structure_targets

    old_status = str(old.get("status") or "IDLE").upper().strip()
    old_direction = str(old.get("candidate_direction") or old.get("direction") or "NEUTRAL").upper().strip()
    started_ms = int(old.get("candidate_started_ms") or 0)
    start_price = float(old.get("candidate_start_price") or 0.0)
    start_atr = float(old.get("candidate_start_atr") or 0.0)
    support_bars = int(old.get("support_bars") or 0)
    age_bars = int((close_ms - started_ms) // TF_MS) if started_ms > 0 else 0
    previous_confidence = int(old.get("confidence") or 0)
    old_accumulated = float(old.get("accumulated_score") or 0.0)
    old_max_fav = float(old.get("max_favorable_atr") or 0.0)
    old_max_adv = float(old.get("max_adverse_atr") or 0.0)
    peak_favorable_ms = int(old.get("peak_favorable_ms") or 0)
    bars_to_peak = int(old.get("bars_to_peak") or 0)
    confirmed_ms = int(old.get("confirmed_ms") or 0)

    status = old_status
    direction = old_direction
    event = None
    directional_move_atr = 0.0
    max_favorable_atr = old_max_fav
    max_adverse_atr = old_max_adv
    revoke_score = 0
    revoke_reasons: list[str] = []

    if old_status in ("PENDING", "CONFIRMED") and start_price > 0 and start_atr > 0:
        directional_move_atr = _directional_move_atr(direction, start_price, current_close, start_atr)
        if directional_move_atr > old_max_fav:
            max_favorable_atr = directional_move_atr
            peak_favorable_ms = close_ms
            bars_to_peak = age_bars
        else:
            max_favorable_atr = old_max_fav
        max_adverse_atr = min(old_max_adv, directional_move_atr)

    current_direction_score = bull_score if direction == "BULLISH" else bear_score
    current_opposite_score = bear_score if direction == "BULLISH" else bull_score
    accumulated_score = (
        old_accumulated * EVIDENCE_DECAY
        + current_direction_score * (1.0 - EVIDENCE_DECAY)
        if old_status in ("PENDING", "CONFIRMED")
        else 0.0
    )

    # Full basket is required only for synthetic candidates. Real DXY is naturally exempt.
    basket_ok = source != "SYNTHETIC_DXY" or int(features.get("synthetic_pair_count") or 0) == 5
    start_direction = score_direction if basket_ok else "NEUTRAL"
    start_score = max(bull_score, bear_score)

    if direction == "BEARISH":
        confirm_score_required = BEAR_CONFIRM_SCORE
        confirm_margin_required = BEAR_CONFIRM_MARGIN
    else:
        confirm_score_required = BULL_CONFIRM_SCORE
        confirm_margin_required = BULL_CONFIRM_MARGIN

    if start_direction == "BEARISH":
        start_score_required = BEAR_CANDIDATE_START_SCORE
        start_margin_required = BEAR_CANDIDATE_START_MARGIN
    else:
        start_score_required = BULL_CANDIDATE_START_SCORE
        start_margin_required = BULL_CANDIDATE_START_MARGIN

    if old_status == "PENDING":
        supporting = direction == score_direction and current_direction_score >= 45 and score_margin >= 10
        if supporting:
            support_bars += 1
        elif current_opposite_score >= current_direction_score + 18:
            support_bars = max(0, support_bars - 1)

        revoke_score, revoke_reasons = _revoke_evidence(
            direction=direction,
            features=features,
            directional_move_atr=directional_move_atr,
            bull_score=bull_score,
            bear_score=bear_score,
        )

        confidence = int(round(max(current_direction_score, accumulated_score)))
        confirm_ready = (
            age_bars >= 1
            and support_bars >= CONFIRM_SUPPORT_BARS
            and confidence >= confirm_score_required
            and current_direction_score - current_opposite_score >= confirm_margin_required
        )

        if directional_move_atr <= -CANDIDATE_HARD_REJECT_ATR:
            status = "REJECTED"
            event = {"status": "REJECTED", "direction": direction, "reason": "HARD_ADVERSE_MOVE"}
        elif revoke_score >= REVOKE_SCORE:
            status = "REJECTED"
            event = {"status": "REJECTED", "direction": direction, "reason": "COMBINED_FAILURE_EVIDENCE"}
        elif confirm_ready:
            status = "CONFIRMED"
            confirmed_ms = close_ms
            event = {"status": "CONFIRMED", "direction": direction, "reason": "TWO_CANDLE_EVIDENCE_CONFIRMED"}
        elif age_bars >= CANDIDATE_EXPIRY_BARS:
            status = "EXPIRED"
            event = {"status": "EXPIRED", "direction": direction, "reason": "CANDIDATE_EVIDENCE_EXPIRED"}

    elif old_status == "CONFIRMED":
        support_bars += 1 if direction == score_direction and current_direction_score >= 45 else 0
        revoke_score, revoke_reasons = _revoke_evidence(
            direction=direction,
            features=features,
            directional_move_atr=directional_move_atr,
            bull_score=bull_score,
            bear_score=bear_score,
        )
        confidence = int(round(max(0.0, min(100.0, accumulated_score))))
        if revoke_score >= REVOKE_SCORE:
            if max_favorable_atr >= TURN_COMPLETED_ATR:
                status = "COMPLETED"
                reason = "SUCCESSFUL_TURN_LATER_REVERSED"
                outcome = "COMPLETED"
            elif max_favorable_atr >= TURN_WEAK_ATR:
                status = "WEAK_COMPLETION"
                reason = "WEAK_TURN_LATER_REVERSED"
                outcome = "WEAK"
            else:
                status = "INVALIDATED"
                reason = "CONFIRMED_TURN_FAILED"
                outcome = "FAILED"
            event = {
                "status": status,
                "direction": direction,
                "reason": reason,
                "outcome": outcome,
            }

    elif old_status in ("REJECTED", "EXPIRED", "INVALIDATED", "COMPLETED", "WEAK_COMPLETION"):
        status = "IDLE"
        direction = "NEUTRAL"
        started_ms = 0
        start_price = 0.0
        start_atr = 0.0
        support_bars = 0
        age_bars = 0
        accumulated_score = 0.0
        previous_confidence = 0
        max_favorable_atr = 0.0
        max_adverse_atr = 0.0
        peak_favorable_ms = 0
        bars_to_peak = 0
        confirmed_ms = 0

    else:  # IDLE / unknown
        status = "IDLE"
        direction = "NEUTRAL"
        if (
            basket_ok
            and start_direction in ("BULLISH", "BEARISH")
            and start_score >= start_score_required
            and score_margin >= start_margin_required
        ):
            status = "PENDING"
            direction = start_direction
            started_ms = close_ms
            start_price = current_close
            start_atr = current_atr
            support_bars = 1
            age_bars = 0
            accumulated_score = float(start_score)
            max_favorable_atr = 0.0
            max_adverse_atr = 0.0
            peak_favorable_ms = close_ms
            bars_to_peak = 0
            confirmed_ms = 0
            event = {"status": "PENDING", "direction": direction, "reason": "EARLY_EVIDENCE_CANDIDATE"}

    active = status in ("PENDING", "CONFIRMED")
    active_score = bull_score if direction == "BULLISH" else bear_score if direction == "BEARISH" else 0
    confidence = int(round(max(active_score, accumulated_score))) if active else 0

    event_record = None
    if event:
        payload = {
            "schema_version": 11,
            "event_type": "DXY_M15_MARKET_STATE_TURN",
            "model": "DXY_M15_UNIFIED_MARKET_STATE_V12_COMPLETE_BASKET",
            "source": source,
            "device_id": device_id,
            "uids": binding.get("uids") or [],
            "profile_ids": binding.get("profile_ids") or [],
            "firms": binding.get("firms") or [],
            "status": event["status"],
            "direction": event["direction"],
            "reason": event.get("reason"),
            "outcome": event.get("outcome"),
            "change_ms": close_ms,
            "bar_close_ms": close_ms,
            "broker_bar_close_ms": broker_close_ms,
            "broker_offset_minutes": offset_min,
            "detected_at_ms": detected_at_ms,
            "confidence": confidence if active else previous_confidence,
            "candidate_started_ms": started_ms or None,
            "candidate_age_bars": age_bars,
            "support_bars": support_bars,
            "candidate_start_price": start_price or None,
            "candidate_start_atr": start_atr or None,
            "directional_move_atr": round(directional_move_atr, 4),
            "max_favorable_atr": round(max_favorable_atr, 4),
            "max_adverse_atr": round(max_adverse_atr, 4),
            "confirmed_ms": confirmed_ms or None,
            "bars_alive": int((close_ms - confirmed_ms) // TF_MS) if confirmed_ms > 0 else 0,
            "peak_favorable_ms": peak_favorable_ms or None,
            "bars_to_peak": bars_to_peak,
            "end_move_atr": round(directional_move_atr, 4),
            "bull_score": bull_score,
            "bear_score": bear_score,
            "accumulated_score": round(accumulated_score, 2),
            "revoke_score": revoke_score,
            "revoke_reasons": revoke_reasons,
            "features": features,
            "historical_backfill": bool(historical),
        }
        event_record = payload
        _append_event_once(R, payload)

    if event and str(event.get("status") or "").upper() == "CONFIRMED":
        confirmed_direction_now = str(event.get("direction") or "").upper().strip()
        if confirmed_direction_now in ("BULLISH", "BEARISH"):
            if last_confirmed_direction and last_confirmed_direction != confirmed_direction_now:
                previous_direction = last_confirmed_direction
            last_confirmed_direction = confirmed_direction_now

    state = {
        "schema_version": 12,
        "initialized": True,
        "model": "DXY_M15_UNIFIED_MARKET_STATE_V12_COMPLETE_BASKET",
        "source": source,
        "device_id": device_id,
        "uids": binding.get("uids") or [],
        "profile_ids": binding.get("profile_ids") or [],
        "firms": binding.get("firms") or [],
        "status": status,
        "candidate_direction": direction if active else "NEUTRAL",
        "direction": direction if active else "NEUTRAL",
        "previous_direction": previous_direction or last_confirmed_direction,
        "last_confirmed_direction": last_confirmed_direction,
        "candidate_started_ms": started_ms if active else None,
        "candidate_age_bars": age_bars if active else None,
        "candidate_start_price": start_price if active else None,
        "candidate_start_atr": start_atr if active else None,
        "support_bars": support_bars if active else 0,
        "confidence": confidence,
        "bull_score": bull_score,
        "bear_score": bear_score,
        "accumulated_score": round(accumulated_score, 2) if active else 0.0,
        "directional_move_atr": round(directional_move_atr, 4) if active else 0.0,
        "max_favorable_atr": round(max_favorable_atr, 4) if active else 0.0,
        "max_adverse_atr": round(max_adverse_atr, 4) if active else 0.0,
        "confirmed_ms": confirmed_ms if active else None,
        "peak_favorable_ms": peak_favorable_ms if active else None,
        "bars_to_peak": bars_to_peak if active else 0,
        "revoke_score": revoke_score,
        "revoke_reasons": revoke_reasons,
        "market_flow": market_flow,
        "market_flow_direction": market_flow.get("direction"),
        "market_flow_strength": market_flow.get("strength"),
        "market_flow_confidence": market_flow.get("confidence"),
        "market_flow_stage": market_flow.get("stage"),
        "market_flow_reason": market_flow.get("reason"),

        "structure": structure,
        "structure_targets": structure_targets,
        "structure_source": "trend_sr",
        "structure_pressure": structure.get("structure_pressure"),
        "structure_pressure_reason": structure.get("structure_pressure_reason"),
        "nearest_support": structure.get("nearest_support"),
        "nearest_resistance": structure.get("nearest_resistance"),
        "next_support": structure.get("next_support"),
        "next_resistance": structure.get("next_resistance"),
        "best_support": structure.get("best_support"),
        "best_resistance": structure.get("best_resistance"),
        "active_target": structure.get("active_target"),
        "active_target_source": structure.get("active_target_source"),
        "room_up_atr": structure.get("room_up_atr"),
        "room_down_atr": structure.get("room_down_atr"),
        "summary": {
            "flow": market_flow.get("direction"),
            "stage": market_flow.get("stage"),
            "strength": market_flow.get("strength"),
            "confidence": market_flow.get("confidence"),
            "pressure": structure.get("structure_pressure"),
            "directional_room_atr": structure.get("directional_room_atr"),
            "nearest_resistance": (structure.get("nearest_resistance") or {}).get("price"),
            "nearest_support": (structure.get("nearest_support") or {}).get("price"),
            "active_target": (structure.get("active_target") or {}).get("price"),
            "active_target_source": structure.get("active_target_source"),
            "structure_source": "trend_sr",
            "expectation": (
                "REVERSAL_RISK_HIGH" if structure.get("structure_pressure") in ("HIGH", "EXTREME")
                else "CONTINUATION_FAVOURED" if market_flow.get("direction") in ("BULLISH", "BEARISH")
                else "NO_DIRECTIONAL_EDGE"
            ),
        },
        "turn_status": status,
        "turn_direction": direction if active else "NEUTRAL",
        "latest_feature_direction": score_direction,
        "latest_feature_qualified": bool(features.get("candidate_qualified")),
        "latest_qualification_reason": qualification_reason,
        "last_event_status": event.get("status") if event else old.get("last_event_status"),
        "last_event_reason": event.get("reason") if event else old.get("last_event_reason"),
        "last_event_ms": close_ms if event else old.get("last_event_ms"),
        "last_evaluated_bar_close_ms": close_ms,
        "broker_bar_close_ms": broker_close_ms,
        "broker_offset_minutes": offset_min,
        "detected_at_ms": detected_at_ms,
        "features": features,
        "basket_health": {
            "synthetic_health": features.get("synthetic_health"),
            "synthetic_complete": features.get("synthetic_complete"),
            "synthetic_expected_pair_count": features.get("synthetic_expected_pair_count"),
            "synthetic_pair_count": features.get("synthetic_pair_count"),
            "synthetic_pairs": features.get("synthetic_pairs"),
            "synthetic_missing_pairs": features.get("synthetic_missing_pairs"),
        } if source == "SYNTHETIC_DXY" else None,
        "evaluation_pending_retry": False,
        "shadow_only": True,
    }

    R.set(
        _features_key(source, device_id, close_ms),
        json.dumps(features, separators=(",", ":"), default=str),
        ex=HISTORY_TTL_SEC,
    )
    R.set(
        _state_key(source, device_id),
        json.dumps(state, separators=(",", ":"), default=str),
    )
    return {"ok": True, "event": event, "event_record": event_record, "state": state}

def _persist_series(R, source: str, device_id: str, bars: list[dict], offset_min: int, detected_at_ms: int) -> dict:
    normalized = []
    seen = set()
    future = 0
    for bar in bars:
        broker_open = _bar_open_ms(bar); broker_close = _bar_close_ms(bar)
        close_ms = _broker_to_utc_ms(broker_close, offset_min)
        if not broker_open or not broker_close or close_ms > detected_at_ms + 120000:
            future += 1
            continue
        if broker_open in seen:
            continue
        seen.add(broker_open)
        row = dict(bar)
        row["broker_bar_open_ms"] = broker_open
        row["broker_bar_close_ms"] = broker_close
        row["bar_open_ms"] = _broker_to_utc_ms(broker_open, offset_min)
        row["bar_close_ms"] = close_ms
        normalized.append(row)
    normalized.sort(key=lambda b: int(b.get("bar_open_ms") or 0))
    payload = {
        "schema_version": 1,
        "model": "DXY_M15_BROKER_ALIGNED_SERIES_V1",
        "source": source,
        "tf": TF,
        "device_id": device_id,
        "broker_offset_minutes": offset_min,
        "built_at_ms": detected_at_ms,
        "bars_built": len(normalized),
        "duplicate_timestamps": max(0, len(bars) - len(seen)),
        "future_bars": future,
        "timestamps_strictly_increasing": all(
            int(normalized[i].get("bar_open_ms") or 0) < int(normalized[i + 1].get("bar_open_ms") or 0)
            for i in range(len(normalized) - 1)
        ),
        "bars": normalized,
    }
    R.set(_series_key(source, device_id), json.dumps(payload, separators=(",", ":"), default=str), ex=HISTORY_TTL_SEC)
    return payload


def _bootstrap(R, *, source: str, device_id: str, binding: dict, bars: list[dict],
               offset_min: int, detected_at_ms: int) -> dict:
    key = _bootstrap_key(source, device_id)
    old = _json_load(R.get(key), {})
    if isinstance(old, dict) and old.get("completed"):
        return old
    evaluations = 0
    events = 0
    status_counts = {
        "PENDING": 0,
        "CONFIRMED": 0,
        "REJECTED": 0,
        "EXPIRED": 0,
        "INVALIDATED": 0,
        "COMPLETED": 0,
        "WEAK_COMPLETION": 0,
    }
    direction_counts = {"BULLISH": 0, "BEARISH": 0}
    pin_stats = {
        "bullish_pin_bars": 0,
        "bearish_pin_bars": 0,
        "qualified_with_pin_support": 0,
        "confirmed_with_pin_support": 0,
    }
    sr_stats = {
        "bars_with_h1_support": 0,
        "bars_with_h1_resistance": 0,
        "bars_near_h1_support": 0,
        "bars_near_h1_resistance": 0,
        "bullish_h1_sweep_reclaims": 0,
        "bearish_h1_sweep_rejects": 0,
        "candidates_near_h1_support": 0,
        "candidates_near_h1_resistance": 0,
        "confirmed_near_h1_support": 0,
        "confirmed_near_h1_resistance": 0,
        "three_tf_support_confluence": 0,
        "three_tf_resistance_confluence": 0,
        "sweep_conflicts": 0,
        "resolved_bullish_sweeps": 0,
        "resolved_bearish_sweeps": 0,
        "h1_compression_context_bars": 0,
        "between_h1_zones_bars": 0,
        "candidate_structure_contexts": {},
        "confirmed_structure_contexts": {},
    }
    # Replay chronologically. State is intentionally built by the exact live function.
    R.delete(_state_key(source, device_id))
    for idx in range(MIN_FEATURE_BARS - 1, len(bars)):
        result = _evaluate_one(
            R, source=source, device_id=device_id, binding=binding, bars=bars,
            index=idx, detected_at_ms=detected_at_ms, offset_min=offset_min,
            historical=True,
        )
        if result.get("ok"):
            evaluations += 1
            state_features = (result.get("state") or {}).get("features") or {}
            if int(state_features.get("bullish_pin_score") or 0) >= 55:
                pin_stats["bullish_pin_bars"] += 1
            if int(state_features.get("bearish_pin_score") or 0) >= 55:
                pin_stats["bearish_pin_bars"] += 1
            sr = state_features.get("sr_audit") or {}
            h1_sr = sr.get("h1") or {}
            h1_sup = h1_sr.get("nearest_support") or {}
            h1_res = h1_sr.get("nearest_resistance") or {}
            if h1_sup:
                sr_stats["bars_with_h1_support"] += 1
            if h1_res:
                sr_stats["bars_with_h1_resistance"] += 1
            if h1_sup.get("near"):
                sr_stats["bars_near_h1_support"] += 1
            if h1_res.get("near"):
                sr_stats["bars_near_h1_resistance"] += 1
            if sr.get("bullish_h1_sweep_reclaim"):
                sr_stats["bullish_h1_sweep_reclaims"] += 1
            if sr.get("bearish_h1_sweep_reject"):
                sr_stats["bearish_h1_sweep_rejects"] += 1
            if sr.get("sweep_conflict"):
                sr_stats["sweep_conflicts"] += 1
            if sr.get("bullish_h1_sweep_reclaim"):
                sr_stats["resolved_bullish_sweeps"] += 1
            if sr.get("bearish_h1_sweep_reject"):
                sr_stats["resolved_bearish_sweeps"] += 1
            context_name = str(((sr.get("structure_context") or {}).get("context") or "UNKNOWN"))
            if context_name == "H1_COMPRESSION_BETWEEN_ZONES":
                sr_stats["h1_compression_context_bars"] += 1
            if context_name == "BETWEEN_H1_ZONES":
                sr_stats["between_h1_zones_bars"] += 1
            if int(sr.get("bullish_confluence_count") or 0) >= 3:
                sr_stats["three_tf_support_confluence"] += 1
            if int(sr.get("bearish_confluence_count") or 0) >= 3:
                sr_stats["three_tf_resistance_confluence"] += 1
            if bool(state_features.get("candidate_qualified")) and max(
                int(state_features.get("bullish_pin_cluster_score") or 0),
                int(state_features.get("bearish_pin_cluster_score") or 0),
            ) >= 55:
                pin_stats["qualified_with_pin_support"] += 1
            if result.get("event"):
                events += 1
                ev = result.get("event") or {}
                st = str(ev.get("status") or "").upper()
                dr = str(ev.get("direction") or "").upper()
                if st in status_counts:
                    status_counts[st] += 1
                if st == "PENDING" and dr in direction_counts:
                    direction_counts[dr] += 1
                event_record = result.get("event_record") or {}
                ev_features = event_record.get("features") or state_features
                ev_sr = ev_features.get("sr_audit") or {}
                ev_h1 = ev_sr.get("h1") or {}
                ev_context = str(((ev_sr.get("structure_context") or {}).get("context") or "UNKNOWN"))
                if st == "PENDING":
                    bucket = sr_stats["candidate_structure_contexts"]
                    bucket[ev_context] = int(bucket.get(ev_context) or 0) + 1
                if st == "CONFIRMED":
                    bucket = sr_stats["confirmed_structure_contexts"]
                    bucket[ev_context] = int(bucket.get(ev_context) or 0) + 1
                if st == "PENDING" and dr == "BULLISH" and (ev_h1.get("nearest_support") or {}).get("near"):
                    sr_stats["candidates_near_h1_support"] += 1
                if st == "PENDING" and dr == "BEARISH" and (ev_h1.get("nearest_resistance") or {}).get("near"):
                    sr_stats["candidates_near_h1_resistance"] += 1
                if st == "CONFIRMED" and dr == "BULLISH" and (ev_h1.get("nearest_support") or {}).get("near"):
                    sr_stats["confirmed_near_h1_support"] += 1
                if st == "CONFIRMED" and dr == "BEARISH" and (ev_h1.get("nearest_resistance") or {}).get("near"):
                    sr_stats["confirmed_near_h1_resistance"] += 1
                if st == "CONFIRMED":
                    pin_support = (
                        int(ev_features.get("bullish_pin_cluster_score") or 0)
                        if dr == "BULLISH"
                        else int(ev_features.get("bearish_pin_cluster_score") or 0)
                    )
                    if pin_support >= 55:
                        pin_stats["confirmed_with_pin_support"] += 1
    marker = {
        "completed": True,
        "completed_at_ms": detected_at_ms,
        "source": source,
        "device_id": device_id,
        "evaluations": evaluations,
        "event_count": events,
        "status_counts": status_counts,
        "pending_direction_counts": direction_counts,
        "pin_bar_stats": pin_stats,
        "sr_audit_stats": sr_stats,
        "sr_score_enabled": False,
        "confirmation_rate": round(
            status_counts["CONFIRMED"] / max(1, status_counts["PENDING"]),
            4,
        ),
        "completed_rate": round(
            status_counts["COMPLETED"] / max(1, status_counts["CONFIRMED"]),
            4,
        ),
        "weak_completion_rate": round(
            status_counts["WEAK_COMPLETION"] / max(1, status_counts["CONFIRMED"]),
            4,
        ),
        "failure_rate": round(
            status_counts["INVALIDATED"] / max(1, status_counts["CONFIRMED"]),
            4,
        ),
        "resolved_confirmed_turns": (
            status_counts["COMPLETED"]
            + status_counts["WEAK_COMPLETION"]
            + status_counts["INVALIDATED"]
        ),
        "first_bar_close_ms": _broker_to_utc_ms(_bar_close_ms(bars[MIN_FEATURE_BARS - 1]), offset_min) if len(bars) >= MIN_FEATURE_BARS else None,
        "last_bar_close_ms": _broker_to_utc_ms(_bar_close_ms(bars[-1]), offset_min) if bars else None,
    }
    R.set(key, json.dumps(marker, separators=(",", ":")), ex=HISTORY_TTL_SEC)
    return marker


def update_global_dxy_m15_state(*, R, now_ms: int | None = None) -> dict:
    """Update real and synthetic M15 feature/candidate timelines in shadow mode."""
    detected_at_ms = int(now_ms or time.time() * 1000)
    stats = {
        "bindings": 0, "devices": 0, "real_available": 0,
        "synthetic_available": 0, "series_built": 0,
        "bootstrapped": 0, "evaluated": 0, "candidate_events": 0,
        "grace_wait": 0, "degraded_retry": 0, "eval_failed_retry": 0,
        "errors": 0, "lock": False,
    }
    try:
        if not R.set(TICK_LOCK_KEY, str(detected_at_ms), nx=True, ex=TICK_LOCK_SEC):
            return stats
    except Exception:
        return stats
    stats["lock"] = True

    try:
        from api.dxy_tracker import _load_bindings
        bindings = _load_bindings(R) or []
        stats["bindings"] = len(bindings); stats["devices"] = len(bindings)
        for binding in bindings:
            device_id = str(binding.get("device_id") or "").strip()
            if not device_id:
                continue
            offset_min = _broker_offset_minutes(R, device_id)
            for source in SOURCE_VALUES:
                try:
                    bars = _load_source_bars(source, device_id)
                    if len(bars) < MIN_FEATURE_BARS:
                        continue
                    if source == "REAL_DXY":
                        stats["real_available"] += 1
                    else:
                        stats["synthetic_available"] += 1
                    _persist_series(R, source, device_id, bars, offset_min, detected_at_ms)
                    stats["series_built"] += 1
                    marker = _bootstrap(
                        R, source=source, device_id=device_id, binding=binding,
                        bars=bars, offset_min=offset_min, detected_at_ms=detected_at_ms,
                    )
                    if marker.get("completed"):
                        stats["bootstrapped"] += 1
                    broker_close_ms = _bar_close_ms(bars[-1])
                    close_ms = _broker_to_utc_ms(broker_close_ms, offset_min)
                    if source == "SYNTHETIC_DXY":
                        health = _synthetic_health(bars[-1], detected_at_ms=detected_at_ms, close_ms=close_ms)
                        if not health.get("synthetic_grace_complete"):
                            stats["grace_wait"] += 1
                            _persist_degraded_health(
                                R, source=source, device_id=device_id, close_ms=close_ms,
                                broker_close_ms=broker_close_ms, offset_min=offset_min,
                                detected_at_ms=detected_at_ms, health=health,
                            )
                            continue
                        if not health.get("synthetic_complete"):
                            stats["degraded_retry"] += 1
                            _persist_degraded_health(
                                R, source=source, device_id=device_id, close_ms=close_ms,
                                broker_close_ms=broker_close_ms, offset_min=offset_min,
                                detected_at_ms=detected_at_ms, health=health,
                            )
                            continue
                    eval_key = _eval_key(source, device_id, close_ms)
                    if not R.set(eval_key, str(detected_at_ms), nx=True, ex=2 * 60 * 60):
                        continue
                    result = _evaluate_one(
                        R, source=source, device_id=device_id, binding=binding,
                        bars=bars, index=len(bars) - 1, detected_at_ms=detected_at_ms,
                        offset_min=offset_min, historical=False,
                    )
                    if result.get("ok"):
                        stats["evaluated"] += 1
                    else:
                        try:
                            R.delete(eval_key)
                        except Exception:
                            pass
                        stats["eval_failed_retry"] += 1
                    if result.get("event"):
                        stats["candidate_events"] += 1
                        log.warning(
                            "[DXY_M15] CANDIDATE_EVENT source=%s device=%s status=%s direction=%s close_ms=%s confidence=%s",
                            source, device_id, result["event"].get("status"),
                            result["event"].get("direction"), close_ms,
                            (result.get("state") or {}).get("confidence"),
                        )
                except Exception:
                    stats["errors"] += 1
                    log.exception("[DXY_M15] UPDATE_FAILED source=%s device=%s", source, device_id)
        return stats
    except Exception:
        stats["errors"] += 1
        log.exception("[DXY_M15] GLOBAL_UPDATE_FAILED")
        return stats


def read_dxy_m15_state(R, source: str, device_id: str) -> dict:
    state = _json_load(R.get(_state_key(str(source).upper(), device_id)), {})
    return state if isinstance(state, dict) else {}


def read_dxy_m15_events_between(R, source: str, device_id: str, start_ms: int, end_ms: int) -> list[dict]:
    out = []
    try:
        for raw in R.lrange(_history_key(str(source).upper(), device_id), 0, -1) or []:
            event = _json_load(raw, {})
            ms = int(event.get("change_ms") or 0) if isinstance(event, dict) else 0
            if int(start_ms) <= ms <= int(end_ms):
                out.append(event)
    except Exception:
        return []
    out.sort(key=lambda e: int(e.get("change_ms") or 0))
    return out
