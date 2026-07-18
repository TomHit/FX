# -*- coding: utf-8 -*-
"""XTL Evidence-Driven Shadow Bias Engine.

Production contract
-------------------
* Pure and side-effect free: no Redis, broker, gate, executor, or API imports.
* Shadow analytics only. The caller decides what to do with the result.
* Consumes already-computed SR/liquidity/regime evidence instead of duplicating it.
* Uses closed H1/H4 OHLC only for structure, CHOCH and ATR.
* Bias and actionability are separate outputs.
* UNKNOWN means inputs are untrusted; NEUTRAL means valid inputs but no edge.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple
import math

BIAS_ENGINE_VERSION = "XTL_EVIDENCE_BIAS_PROD_1"
H1_MIN_BARS = 60
H4_MIN_BARS = 40
CHOCH_ATR_BUFFER = 0.10
ZONE_ACTIONABLE_ATR = 2.0
BIAS_MIN_SCORE = 35.0
BIAS_MIN_EDGE = 15.0
HIGH_CONFIDENCE_SCORE = 70.0
MEDIUM_CONFIDENCE_SCORE = 52.0


@dataclass(frozen=True)
class Swing:
    kind: str
    index: int
    level: float
    open_ms: int
    prominence_atr: float = 0.0


def _f(value: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        out = float(value)
        return out if math.isfinite(out) else default
    except Exception:
        return default


def _ms(value: Any) -> int:
    try:
        out = int(float(value or 0))
        if 0 < out < 10_000_000_000:
            out *= 1000
        return out
    except Exception:
        return 0


def _bar_time_ms(bar: Dict[str, Any]) -> int:
    for key in ("t_open_ms", "tOpenMs", "open_time_ms", "t", "time", "ts"):
        out = _ms(bar.get(key))
        if out > 0:
            return out
    return 0


def _normalize_bars(bars: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Chronological closed OHLC bars. Invalid/forming rows are ignored."""
    out: List[Dict[str, Any]] = []
    for raw in bars or []:
        if not isinstance(raw, dict) or raw.get("complete") is False:
            continue
        o = _f(raw.get("o", raw.get("open")))
        h = _f(raw.get("h", raw.get("high")))
        l = _f(raw.get("l", raw.get("low")))
        c = _f(raw.get("c", raw.get("close")))
        if None in (o, h, l, c) or h < l or h < max(o, c) or l > min(o, c):
            continue
        row = dict(raw)
        row.update({"o": o, "h": h, "l": l, "c": c, "_open_ms": _bar_time_ms(raw)})
        out.append(row)
    if any(int(x.get("_open_ms") or 0) > 0 for x in out):
        out.sort(key=lambda x: int(x.get("_open_ms") or 0))
        dedup: Dict[int, Dict[str, Any]] = {}
        no_ts: List[Dict[str, Any]] = []
        for row in out:
            ts = int(row.get("_open_ms") or 0)
            if ts > 0:
                dedup[ts] = row
            else:
                no_ts.append(row)
        out = [dedup[k] for k in sorted(dedup)] + no_ts
    return out


def _atr14_normalized(bs: Sequence[Dict[str, Any]]) -> float:
    if len(bs) < 15:
        return 0.0
    trs: List[float] = []
    prev_c: Optional[float] = None
    for b in bs:
        h, l, c = float(b["h"]), float(b["l"]), float(b["c"])
        tr = h - l if prev_c is None else max(h - l, abs(h - prev_c), abs(l - prev_c))
        trs.append(tr)
        prev_c = c
    return float(sum(trs[-14:]) / 14.0)


def _efficiency_ratio(bs: Sequence[Dict[str, Any]], steps: int) -> float:
    closes = [float(b["c"]) for b in bs]
    if len(closes) < 3:
        return 0.0
    window = closes[-(steps + 1):] if len(closes) > steps else closes
    net = abs(window[-1] - window[0])
    noise = sum(abs(window[i] - window[i - 1]) for i in range(1, len(window)))
    return float(net / noise) if noise > 0 else 0.0


# ---------------------------------------------------------------------------
# Structure evidence
# ---------------------------------------------------------------------------
def extract_confirmed_swings(
    bars: Sequence[Dict[str, Any]],
    lookback: int = 2,
    atr: Optional[float] = None,
) -> Dict[str, Any]:
    bs = _normalize_bars(bars)
    atr_use = float(atr or _atr14_normalized(bs) or 0.0)
    highs: List[Swing] = []
    lows: List[Swing] = []
    n = len(bs)
    if lookback < 1 or n < lookback * 2 + 1:
        return {"highs": [], "lows": [], "bar_count": n, "lookback": lookback}

    for i in range(lookback, n - lookback):
        hi, lo = float(bs[i]["h"]), float(bs[i]["l"])
        left = bs[i - lookback:i]
        right = bs[i + 1:i + 1 + lookback]
        is_high = all(hi > float(x["h"]) for x in left) and all(hi >= float(x["h"]) for x in right)
        is_low = all(lo < float(x["l"]) for x in left) and all(lo <= float(x["l"]) for x in right)
        ts = int(bs[i].get("_open_ms") or 0)
        if is_high:
            neighbourhood_low = min(float(x["l"]) for x in left + right + [bs[i]])
            prom = (hi - neighbourhood_low) / atr_use if atr_use > 0 else 0.0
            highs.append(Swing("HIGH", i, hi, ts, round(prom, 4)))
        if is_low:
            neighbourhood_high = max(float(x["h"]) for x in left + right + [bs[i]])
            prom = (neighbourhood_high - lo) / atr_use if atr_use > 0 else 0.0
            lows.append(Swing("LOW", i, lo, ts, round(prom, 4)))

    return {
        "highs": [asdict(x) for x in highs],
        "lows": [asdict(x) for x in lows],
        "bar_count": n,
        "lookback": lookback,
    }


def classify_structure(swings: Dict[str, Any]) -> Dict[str, Any]:
    highs = swings.get("highs") or []
    lows = swings.get("lows") or []
    if len(highs) < 2 or len(lows) < 2:
        return {
            "state": "UNKNOWN",
            "strength": 0.0,
            "reason": "INSUFFICIENT_CONFIRMED_SWINGS",
            "last_high": highs[-1] if highs else None,
            "last_low": lows[-1] if lows else None,
        }

    prev_h, last_h = highs[-2], highs[-1]
    prev_l, last_l = lows[-2], lows[-1]
    high_delta = float(last_h["level"]) - float(prev_h["level"])
    low_delta = float(last_l["level"]) - float(prev_l["level"])
    hh, lh = high_delta > 0, high_delta < 0
    hl, ll = low_delta > 0, low_delta < 0

    if hh and hl:
        state = "BULLISH"
    elif lh and ll:
        state = "BEARISH"
    elif hh and ll:
        state = "EXPANDING_MIXED"
    elif lh and hl:
        state = "CONTRACTING_RANGE"
    else:
        state = "MIXED"

    prominence = [
        _f(last_h.get("prominence_atr"), 0.0) or 0.0,
        _f(last_l.get("prominence_atr"), 0.0) or 0.0,
    ]
    strength = min(1.0, sum(prominence) / 4.0)
    return {
        "state": state,
        "strength": round(strength, 4),
        "hh": hh, "hl": hl, "lh": lh, "ll": ll,
        "high_delta": high_delta,
        "low_delta": low_delta,
        "previous_high": prev_h,
        "last_high": last_h,
        "previous_low": prev_l,
        "last_low": last_l,
    }


def build_structure_context(bars: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    bs = _normalize_bars(bars)
    atr = _atr14_normalized(bs)
    major_swings = extract_confirmed_swings(bs, lookback=2, atr=atr)
    minor_swings = extract_confirmed_swings(bs, lookback=1, atr=atr)
    major = classify_structure(major_swings)
    minor = classify_structure(minor_swings)

    if major.get("state") in ("BULLISH", "BEARISH") and major.get("state") == minor.get("state"):
        quality = "STRONG"
    elif major.get("state") in ("BULLISH", "BEARISH"):
        quality = "HEALTHY" if minor.get("state") not in ("BULLISH", "BEARISH") else "WEAK"
    elif minor.get("state") in ("BULLISH", "BEARISH"):
        quality = "TRANSITION"
    else:
        quality = "NON_DIRECTIONAL"

    return {
        "atr": atr,
        "major": major,
        "minor": minor,
        "quality": quality,
        "er": round(_efficiency_ratio(bs, 24), 4),
        "last_closed_ms": int(bs[-1].get("_open_ms") or 0) if bs else 0,
    }


def detect_choch(
    bars: Sequence[Dict[str, Any]],
    structure_context: Dict[str, Any],
    buffer_atr: float = CHOCH_ATR_BUFFER,
) -> Dict[str, Any]:
    bs = _normalize_bars(bars)
    atr = _f(structure_context.get("atr"), 0.0) or 0.0
    major = structure_context.get("major") if isinstance(structure_context.get("major"), dict) else {}
    state = str(major.get("state") or "UNKNOWN")
    if not bs or atr <= 0:
        return {"state": "UNKNOWN", "reason": "NO_VALID_BARS_OR_ATR"}

    close = float(bs[-1]["c"])
    high = float(bs[-1]["h"])
    low = float(bs[-1]["l"])
    buffer_value = max(0.0, float(buffer_atr) * atr)
    pivot: Optional[Dict[str, Any]] = None
    trigger = "NONE"

    if state == "BEARISH":
        pivot = major.get("last_high")
        level = _f((pivot or {}).get("level"))
        close_confirmed = level is not None and close > level + buffer_value
        wick_only = level is not None and high > level + buffer_value and not close_confirmed
        trigger = "BULLISH_CHOCH" if close_confirmed else "NONE"
    elif state == "BULLISH":
        pivot = major.get("last_low")
        level = _f((pivot or {}).get("level"))
        close_confirmed = level is not None and close < level - buffer_value
        wick_only = level is not None and low < level - buffer_value and not close_confirmed
        trigger = "BEARISH_CHOCH" if close_confirmed else "NONE"
    else:
        return {
            "state": "NONE",
            "prior_structure": state,
            "reason": "CHOCH_REQUIRES_DIRECTIONAL_MAJOR_STRUCTURE",
            "close": close,
            "buffer": buffer_value,
        }

    return {
        "state": trigger,
        "prior_structure": state,
        "pivot_level": level,
        "close": close,
        "buffer": buffer_value,
        "confirmed_by_close": bool(close_confirmed),
        "wick_only_rejected": bool(wick_only),
        "close_ms": int(bs[-1].get("_open_ms") or 0),
    }


# ---------------------------------------------------------------------------
# Existing XTL evidence consumers
# ---------------------------------------------------------------------------
def _regime_for_tf(regime_context: Optional[Dict[str, Any]], tf: str) -> Dict[str, Any]:
    ctx = regime_context if isinstance(regime_context, dict) else {}
    row = ctx.get(tf.lower()) if isinstance(ctx.get(tf.lower()), dict) else {}
    return {
        "label": str(row.get("label") or "UNKNOWN").upper(),
        "er": _f(row.get("er")),
        "adx": _f(row.get("adx")),
    }


def evaluate_regime_context(
    regime_context: Optional[Dict[str, Any]],
    h1_structure: Dict[str, Any],
    h4_structure: Dict[str, Any],
) -> Dict[str, Any]:
    h1 = _regime_for_tf(regime_context, "h1")
    h4 = _regime_for_tf(regime_context, "h4")
    if h1["label"] == "UNKNOWN":
        h1 = {"label": "TREND" if (h1_structure.get("er") or 0) >= 0.45 else "RANGE" if (h1_structure.get("er") or 0) < 0.30 else "MIXED", "er": h1_structure.get("er"), "adx": None}
    if h4["label"] == "UNKNOWN":
        h4 = {"label": "TREND" if (h4_structure.get("er") or 0) >= 0.45 else "RANGE" if (h4_structure.get("er") or 0) < 0.30 else "MIXED", "er": h4_structure.get("er"), "adx": None}

    if h1["label"] == "TREND" and h4["label"] == "TREND":
        phase = "EXPANSION"
    elif h1["label"] == "RANGE" and h4["label"] == "RANGE":
        phase = "COMPRESSION_RANGE"
    elif h1["label"] == "TREND" and h4["label"] != "TREND":
        phase = "EARLY_EXPANSION"
    elif h1["label"] == "RANGE" and h4["label"] == "TREND":
        phase = "PULLBACK_OR_DISTRIBUTION"
    else:
        phase = "TRANSITION_MIXED"
    return {"h1": h1, "h4": h4, "phase": phase}


def evaluate_liquidity_evidence(liquidity_context: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    liq = liquidity_context if isinstance(liquidity_context, dict) else {}
    detail = liq.get("liq_detail") if isinstance(liq.get("liq_detail"), dict) else {}
    bsl_ssl = liq.get("bsl_ssl") if isinstance(liq.get("bsl_ssl"), dict) else detail.get("bsl_ssl") if isinstance(detail.get("bsl_ssl"), dict) else {}
    bsl = bsl_ssl.get("bsl") if isinstance(bsl_ssl.get("bsl"), dict) else {}
    ssl = bsl_ssl.get("ssl") if isinstance(bsl_ssl.get("ssl"), dict) else {}
    signals = [str(x).upper() for x in (liq.get("signals") or [])]

    def _swept(obj: Dict[str, Any], token: str) -> bool:
        return bool(obj.get("swept")) or obj.get("candles_since_sweep") is not None or any(token in s and ("SWEEP" in s or "SWEPT" in s) for s in signals)

    bsl_swept = _swept(bsl, "BSL")
    ssl_swept = _swept(ssl, "SSL")
    bsl_reaction = _f(bsl.get("reaction_after_sweep"), 0.0) or 0.0
    ssl_reaction = _f(ssl.get("reaction_after_sweep"), 0.0) or 0.0
    bsl_wick = _f(bsl.get("sweep_wick"), 0.0) or 0.0
    ssl_wick = _f(ssl.get("sweep_wick"), 0.0) or 0.0

    buy = 0.0
    sell = 0.0
    reasons_buy: List[str] = []
    reasons_sell: List[str] = []

    if ssl_swept:
        buy += 10.0
        reasons_buy.append("SSL_SWEPT")
        if ssl_reaction > 0:
            buy += 8.0
            reasons_buy.append("SSL_BULLISH_REACTION")
    if bsl_swept:
        sell += 10.0
        reasons_sell.append("BSL_SWEPT")
        if bsl_reaction > 0:
            sell += 8.0
            reasons_sell.append("BSL_BEARISH_REACTION")

    if any("BULL" in s or "RECLAIM" in s for s in signals):
        buy += 4.0
        reasons_buy.append("BULLISH_LIQ_SIGNAL")
    if any("BEAR" in s or "REJECT" in s for s in signals):
        sell += 4.0
        reasons_sell.append("BEARISH_LIQ_SIGNAL")

    inventory = liq.get("liq_inventory") if isinstance(liq.get("liq_inventory"), dict) else {}
    buy_targets = inventory.get("ssl") or inventory.get("below") or []
    sell_targets = inventory.get("bsl") or inventory.get("above") or []
    if buy_targets:
        buy += min(4.0, float(len(buy_targets)))
        reasons_buy.append("SSL_INVENTORY")
    if sell_targets:
        sell += min(4.0, float(len(sell_targets)))
        reasons_sell.append("BSL_INVENTORY")

    if buy >= 14 and sell >= 14:
        state = "CONFLICT"
    elif buy > sell + 4:
        state = "CONFIRM_BUY"
    elif sell > buy + 4:
        state = "CONFIRM_SELL"
    else:
        state = "NEUTRAL"

    return {
        "state": state,
        "buy_score": round(buy, 2),
        "sell_score": round(sell, 2),
        "buy_reasons": reasons_buy,
        "sell_reasons": reasons_sell,
        "bsl_swept": bsl_swept,
        "ssl_swept": ssl_swept,
        "bsl_reaction": bsl_reaction,
        "ssl_reaction": ssl_reaction,
        "bsl_wick": bsl_wick,
        "ssl_wick": ssl_wick,
        "signals": signals[:30],
    }


def _zone_distance(zone: Dict[str, Any], price: float) -> Optional[float]:
    lo = _f(zone.get("low"), _f(zone.get("level")))
    hi = _f(zone.get("high"), _f(zone.get("level")))
    if lo is None or hi is None:
        return None
    lo, hi = min(lo, hi), max(lo, hi)
    if lo <= price <= hi:
        return 0.0
    return lo - price if price < lo else price - hi


def _zone_candidates(sr: Dict[str, Any], side: str) -> List[Dict[str, Any]]:
    if side == "BUY":
        keys = ("scored_supports", "active_supports", "supports_near")
        best_key = "best_support"
    else:
        keys = ("scored_resistances", "active_resistances", "resistances_near")
        best_key = "best_resistance"
    rows: List[Dict[str, Any]] = []
    best = sr.get(best_key)
    if isinstance(best, dict):
        rows.append(dict(best))
    for key in keys:
        vals = sr.get(key) if isinstance(sr.get(key), list) else []
        rows.extend(dict(v) for v in vals if isinstance(v, dict))
    seen: set[Tuple[Any, Any, Any]] = set()
    unique: List[Dict[str, Any]] = []
    for row in rows:
        sig = (row.get("level"), row.get("low"), row.get("high"))
        if sig in seen:
            continue
        seen.add(sig)
        unique.append(row)
    return unique


def evaluate_zone_evidence(
    sr_bundle: Optional[Dict[str, Any]],
    side: str,
    price: float,
    atr: float,
    actionable_atr: float = ZONE_ACTIONABLE_ATR,
) -> Dict[str, Any]:
    sr = sr_bundle if isinstance(sr_bundle, dict) else {}
    candidates = _zone_candidates(sr, side)
    ranked: List[Dict[str, Any]] = []
    for z in candidates:
        level = _f(z.get("level"))
        dist = _zone_distance(z, price)
        dist_atr = dist / atr if dist is not None and atr > 0 else None
        correct_side = level is not None and ((side == "BUY" and level <= price) or (side == "SELL" and level >= price))
        valid = bool(
            correct_side
            and z.get("side_ok") is not False
            and z.get("stale") is not True
            and z.get("broken") is not True
            and z.get("invalidated") is not True
            and dist is not None
        )
        quality = _f(z.get("quality_score"), _f(z.get("sr_score"), 0.0)) or 0.0
        score = min(28.0, max(0.0, quality) * 0.45)
        reasons: List[str] = []
        penalties: List[str] = []
        if z.get("htf_confluence"):
            score += 5.0; reasons.append("HTF_CONFLUENCE")
        evidence = z.get("evidence") if isinstance(z.get("evidence"), dict) else {}
        if evidence.get("ob_overlap"):
            score += 3.0; reasons.append("OB_OVERLAP")
        if evidence.get("fvg_overlap"):
            score += 2.0; reasons.append("FVG_OVERLAP")
        if evidence.get("liq_pool"):
            score += 3.0; reasons.append("LIQ_POOL")
        if evidence.get("fresh_swing"):
            score += 2.0; reasons.append("FRESH_SWING")
        if z.get("swept") and z.get("reclaimed"):
            score += 5.0; reasons.append("SWEPT_RECLAIMED")
        reaction = _f(evidence.get("reaction_atr"), _f(z.get("reaction_atr"), 0.0)) or 0.0
        if reaction > 0:
            score += min(5.0, reaction * 1.5); reasons.append("REACTION_STRENGTH")
        if dist_atr is not None:
            if dist_atr <= 0.75:
                score += 5.0; reasons.append("NEAR_ZONE")
            elif dist_atr <= actionable_atr:
                score += 2.0; reasons.append("ACTIONABLE_DISTANCE")
            else:
                score -= min(10.0, (dist_atr - actionable_atr) * 3.0); penalties.append("ZONE_TOO_FAR")
        if not valid:
            score = min(score, 0.0)
            if not correct_side or z.get("side_ok") is False: penalties.append("WRONG_SIDE")
            if z.get("stale") is True: penalties.append("STALE")
            if z.get("broken") is True or z.get("invalidated") is True: penalties.append("BROKEN")
        row = dict(z)
        row.update({
            "bias_side": side,
            "valid": valid,
            "distance": dist,
            "distance_atr": dist_atr,
            "evidence_score": round(score, 2),
            "evidence_reasons": reasons,
            "evidence_penalties": penalties,
        })
        ranked.append(row)

    ranked.sort(key=lambda z: (
        not bool(z.get("valid")),
        -float(z.get("evidence_score") or 0.0),
        float(z.get("distance_atr") if z.get("distance_atr") is not None else 9999.0),
    ))
    best = ranked[0] if ranked else None
    actionable = bool(best and best.get("valid") and best.get("distance_atr") is not None and float(best["distance_atr"]) <= actionable_atr)
    return {
        "side": side,
        "best_zone": best,
        "candidate_count": len(ranked),
        "top_candidates": ranked[:5],
        "score": round(float(best.get("evidence_score") or 0.0), 2) if best else 0.0,
        "valid": bool(best and best.get("valid")),
        "actionable": actionable,
        "reason": "VALID_ACTIONABLE_ZONE" if actionable else "NO_VALID_ZONE" if not best or not best.get("valid") else "VALID_ZONE_TOO_FAR",
    }


# ---------------------------------------------------------------------------
# Evidence fusion
# ---------------------------------------------------------------------------
def _add_score(bucket: Dict[str, Any], points: float, code: str, source: str, detail: Optional[Dict[str, Any]] = None) -> None:
    bucket["score"] += float(points)
    bucket["evidence"].append({"code": code, "points": round(float(points), 2), "source": source, "detail": detail or {}})


def _structure_scores(h1: Dict[str, Any], h4: Dict[str, Any], choch: Dict[str, Any], buy: Dict[str, Any], sell: Dict[str, Any]) -> None:
    for tf, ctx, weight in (("H1", h1, 18.0), ("H4", h4, 22.0)):
        major = ctx.get("major") if isinstance(ctx.get("major"), dict) else {}
        minor = ctx.get("minor") if isinstance(ctx.get("minor"), dict) else {}
        state = str(major.get("state") or "UNKNOWN")
        strength = max(0.5, _f(major.get("strength"), 0.5) or 0.5)
        if state == "BULLISH":
            _add_score(buy, weight * strength, f"{tf}_MAJOR_BULLISH", "structure")
        elif state == "BEARISH":
            _add_score(sell, weight * strength, f"{tf}_MAJOR_BEARISH", "structure")
        elif state == "EXPANDING_MIXED":
            _add_score(buy, 2.0, f"{tf}_EXPANDING_MIXED", "structure")
            _add_score(sell, 2.0, f"{tf}_EXPANDING_MIXED", "structure")

        mstate = str(minor.get("state") or "UNKNOWN")
        if mstate == "BULLISH":
            _add_score(buy, 6.0 if tf == "H1" else 4.0, f"{tf}_MINOR_BULLISH", "structure")
        elif mstate == "BEARISH":
            _add_score(sell, 6.0 if tf == "H1" else 4.0, f"{tf}_MINOR_BEARISH", "structure")

    cstate = str(choch.get("state") or "NONE")
    if cstate == "BULLISH_CHOCH":
        _add_score(buy, 12.0, "BULLISH_CHOCH_CONFIRMED", "choch", choch)
    elif cstate == "BEARISH_CHOCH":
        _add_score(sell, 12.0, "BEARISH_CHOCH_CONFIRMED", "choch", choch)


def _relation(executed_side: str, bias: str) -> str:
    side = str(executed_side or "").upper()
    if bias == "UNKNOWN": return "UNKNOWN"
    if bias == "NEUTRAL": return "NEUTRAL"
    if side not in ("BUY", "SELL"): return "UNKNOWN"
    return "ALIGNED" if side == bias else "CONFLICT"


def compute_shadow_bias(
    *,
    symbol: str,
    bars_h1: Sequence[Dict[str, Any]],
    bars_h4: Sequence[Dict[str, Any]],
    price: float,
    sr_bundle: Optional[Dict[str, Any]] = None,
    liquidity_context: Optional[Dict[str, Any]] = None,
    regime_context: Optional[Dict[str, Any]] = None,
    executed_side: str = "",
    computed_ms: Optional[int] = None,
) -> Dict[str, Any]:
    """Return a complete, frozen, explainable shadow-bias payload. Never raises."""
    errors: List[str] = []
    try:
        h1 = _normalize_bars(bars_h1)
        h4 = _normalize_bars(bars_h4)
        px = _f(price)
        if px is None or px <= 0: errors.append("INVALID_PRICE")
        if len(h1) < H1_MIN_BARS: errors.append("INSUFFICIENT_H1_BARS")
        if len(h4) < H4_MIN_BARS: errors.append("INSUFFICIENT_H4_BARS")

        h1_ctx = build_structure_context(h1)
        h4_ctx = build_structure_context(h4)
        if (h1_ctx.get("atr") or 0) <= 0: errors.append("INVALID_H1_ATR")
        if (h4_ctx.get("atr") or 0) <= 0: errors.append("INVALID_H4_ATR")
        if h1_ctx.get("major", {}).get("state") == "UNKNOWN": errors.append("INSUFFICIENT_H1_SWINGS")
        if h4_ctx.get("major", {}).get("state") == "UNKNOWN": errors.append("INSUFFICIENT_H4_SWINGS")

        now = int(computed_ms or h1_ctx.get("last_closed_ms") or 0)
        if errors:
            return {
                "bias_engine_version": BIAS_ENGINE_VERSION,
                "symbol": str(symbol or "").upper(),
                "shadow_bias": "UNKNOWN",
                "shadow_bias_score": 0.0,
                "shadow_bias_confidence": "NONE",
                "shadow_bias_data_ok": False,
                "shadow_bias_actionable": False,
                "shadow_bias_actionability_reason": "DATA_NOT_TRUSTED",
                "shadow_bias_relation": "UNKNOWN",
                "executed_side": str(executed_side or "").upper() or None,
                "computed_ms": now,
                "data_errors": errors,
                "buy_score": 0.0,
                "sell_score": 0.0,
                "score_edge": 0.0,
                "evidence": {"buy": [], "sell": []},
            }

        choch = detect_choch(h1, h1_ctx)
        regime = evaluate_regime_context(regime_context or (liquidity_context or {}).get("regime"), h1_ctx, h4_ctx)
        liquidity = evaluate_liquidity_evidence(liquidity_context)
        buy_zone = evaluate_zone_evidence(sr_bundle, "BUY", float(px), float(h1_ctx["atr"]))
        sell_zone = evaluate_zone_evidence(sr_bundle, "SELL", float(px), float(h1_ctx["atr"]))

        buy = {"score": 0.0, "evidence": []}
        sell = {"score": 0.0, "evidence": []}
        conflicts: List[str] = []
        vetoes: List[str] = []

        _structure_scores(h1_ctx, h4_ctx, choch, buy, sell)

        if liquidity["buy_score"] > 0:
            _add_score(buy, liquidity["buy_score"], "BUY_LIQUIDITY_EVIDENCE", "liquidity", liquidity)
        if liquidity["sell_score"] > 0:
            _add_score(sell, liquidity["sell_score"], "SELL_LIQUIDITY_EVIDENCE", "liquidity", liquidity)
        if liquidity["state"] == "CONFLICT":
            conflicts.append("LIQUIDITY_TWO_SIDED_CONFLICT")

        if buy_zone["score"] > 0:
            _add_score(buy, buy_zone["score"], "BUY_ZONE_EVIDENCE", "sr", {"reason": buy_zone["reason"]})
        if sell_zone["score"] > 0:
            _add_score(sell, sell_zone["score"], "SELL_ZONE_EVIDENCE", "sr", {"reason": sell_zone["reason"]})

        phase = str(regime.get("phase") or "")
        h1_major = str(h1_ctx.get("major", {}).get("state") or "")
        h4_major = str(h4_ctx.get("major", {}).get("state") or "")
        if phase == "EXPANSION":
            if h1_major == h4_major == "BULLISH": _add_score(buy, 8.0, "ALIGNED_TREND_EXPANSION", "regime")
            elif h1_major == h4_major == "BEARISH": _add_score(sell, 8.0, "ALIGNED_TREND_EXPANSION", "regime")
        elif phase == "COMPRESSION_RANGE":
            if buy_zone["actionable"]: _add_score(buy, 4.0, "RANGE_SUPPORT_FAVOURABLE", "regime")
            if sell_zone["actionable"]: _add_score(sell, 4.0, "RANGE_RESISTANCE_FAVOURABLE", "regime")
        elif phase == "PULLBACK_OR_DISTRIBUTION":
            conflicts.append("H1_RANGE_INSIDE_H4_TREND")

        # Explicit contradictions reduce confidence; no single module forces direction.
        if h1_major == "BULLISH" and h4_major == "BEARISH" or h1_major == "BEARISH" and h4_major == "BULLISH":
            conflicts.append("H1_H4_MAJOR_CONFLICT")
            buy["score"] -= 8.0
            sell["score"] -= 8.0

        buy_score = max(0.0, round(float(buy["score"]), 2))
        sell_score = max(0.0, round(float(sell["score"]), 2))
        edge = round(abs(buy_score - sell_score), 2)
        winner = "BUY" if buy_score > sell_score else "SELL" if sell_score > buy_score else "NEUTRAL"
        winner_score = max(buy_score, sell_score)

        if winner_score < BIAS_MIN_SCORE or edge < BIAS_MIN_EDGE:
            bias = "NEUTRAL"
        else:
            bias = winner

        actionable = buy_zone["actionable"] if bias == "BUY" else sell_zone["actionable"] if bias == "SELL" else False
        actionability_reason = buy_zone["reason"] if bias == "BUY" else sell_zone["reason"] if bias == "SELL" else "NO_DIRECTIONAL_EDGE"

        if bias in ("BUY", "SELL"):
            if winner_score >= HIGH_CONFIDENCE_SCORE and edge >= 25 and actionable and not conflicts:
                confidence = "HIGH"
            elif winner_score >= MEDIUM_CONFIDENCE_SCORE and edge >= BIAS_MIN_EDGE and not conflicts:
                confidence = "MEDIUM"
            else:
                confidence = "LOW"
        else:
            confidence = "NONE"

        selected_zone = buy_zone if bias == "BUY" else sell_zone if bias == "SELL" else None
        return {
            "bias_engine_version": BIAS_ENGINE_VERSION,
            "symbol": str(symbol or "").upper(),
            "shadow_bias": bias,
            "shadow_bias_score": round(winner_score, 2) if bias in ("BUY", "SELL") else 0.0,
            "shadow_bias_confidence": confidence,
            "shadow_bias_data_ok": True,
            "shadow_bias_actionable": bool(actionable),
            "shadow_bias_actionability_reason": actionability_reason,
            "shadow_bias_relation": _relation(executed_side, bias),
            "executed_side": str(executed_side or "").upper() or None,
            "computed_ms": now,
            "h1_last_closed_ms": int(h1_ctx.get("last_closed_ms") or 0),
            "h4_last_closed_ms": int(h4_ctx.get("last_closed_ms") or 0),
            "buy_score": buy_score,
            "sell_score": sell_score,
            "score_edge": edge,
            "thresholds": {
                "min_score": BIAS_MIN_SCORE,
                "min_edge": BIAS_MIN_EDGE,
                "high_confidence": HIGH_CONFIDENCE_SCORE,
                "medium_confidence": MEDIUM_CONFIDENCE_SCORE,
            },
            "h1_structure": h1_ctx,
            "h4_structure": h4_ctx,
            "choch": choch,
            "regime_context": regime,
            "liquidity_evidence": liquidity,
            "buy_zone_context": buy_zone,
            "sell_zone_context": sell_zone,
            "selected_zone_context": selected_zone,
            "evidence": {"buy": buy["evidence"], "sell": sell["evidence"]},
            "conflicts": conflicts,
            "vetoes": vetoes,
            "data_errors": [],
        }
    except Exception as exc:
        return {
            "bias_engine_version": BIAS_ENGINE_VERSION,
            "symbol": str(symbol or "").upper(),
            "shadow_bias": "UNKNOWN",
            "shadow_bias_score": 0.0,
            "shadow_bias_confidence": "NONE",
            "shadow_bias_data_ok": False,
            "shadow_bias_actionable": False,
            "shadow_bias_actionability_reason": "ENGINE_EXCEPTION",
            "shadow_bias_relation": "UNKNOWN",
            "executed_side": str(executed_side or "").upper() or None,
            "computed_ms": int(computed_ms or 0),
            "buy_score": 0.0,
            "sell_score": 0.0,
            "score_edge": 0.0,
            "evidence": {"buy": [], "sell": []},
            "conflicts": [],
            "vetoes": [],
            "data_errors": [f"ENGINE_EXCEPTION:{type(exc).__name__}:{exc}"],
        }


__all__ = [
    "BIAS_ENGINE_VERSION",
    "extract_confirmed_swings",
    "classify_structure",
    "build_structure_context",
    "detect_choch",
    "evaluate_regime_context",
    "evaluate_liquidity_evidence",
    "evaluate_zone_evidence",
    "compute_shadow_bias",
]
