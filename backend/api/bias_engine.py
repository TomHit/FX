"""
XauTrendLab canonical bias engine — SHADOW v1.

Purpose
-------
Compute explainable intraday directional context from completed D1/H4/H1 OHLC
for the traded instrument and for DXY, then classify a proposed trade as
STRONG / PAIR_ONLY / DXY_ONLY / MIXED / CONFLICT / NEUTRAL / UNAVAILABLE.

This file is intentionally independent from legacy shadow_bias.py,
shadow_bias_v2.py, usd_strength.py, Point-A, news, Redis and order execution.
It has no side effects and MUST NOT block trades in v1.

H4 is the primary intraday directional layer, D1 is the slower anchor, and H1
is current alignment. Only completed bars are eligible.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from math import isfinite
from statistics import mean
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

BIAS_ENGINE_VERSION = "XTL_BIAS_ENGINE_V1_SHADOW_20260823"
VALID_TFS = ("D1", "H4", "H1")
VALID_SIDES = ("BUY", "SELL")


@dataclass(frozen=True)
class BiasConfig:
    # Intraday hierarchy: H4 primary, D1 anchor, H1 current alignment.
    tf_weight_d1: float = 0.25
    tf_weight_h4: float = 0.50
    tf_weight_h1: float = 0.25

    # Minimum completed bars. D1 remains short enough for intraday use.
    min_bars_d1: int = 12
    min_bars_h4: int = 24
    min_bars_h1: int = 24

    # Maximum history retained inside each calculation.
    max_bars_d1: int = 30
    max_bars_h4: int = 48
    max_bars_h1: int = 72

    # Effective directional focus.
    direction_window_d1: int = 10
    direction_window_h4: int = 18
    direction_window_h1: int = 24

    atr_period: int = 14
    ema_fast: int = 8
    ema_slow: int = 21
    pivot_left: int = 2
    pivot_right: int = 2

    # Direction thresholds.
    tf_bullish_threshold: float = 18.0
    tf_bearish_threshold: float = -18.0
    final_buy_threshold: float = 18.0
    final_sell_threshold: float = -18.0

    # Trend-vs-range classification. Regime is NOT direction.
    trend_er_min: float = 0.38
    trend_r2_min: float = 0.35
    range_er_max: float = 0.22
    range_r2_max: float = 0.22

    # Optional freshness checks, only used when as_of_ms is provided.
    # D1 is weekend-safe by design.
    max_age_h1_hours: float = 4.5
    max_age_h4_hours: float = 14.0
    max_age_d1_hours: float = 96.0

    # Aggregate confidence.
    confidence_high_score: float = 55.0
    confidence_medium_score: float = 32.0

    # Minimum sum of usable timeframe weights needed for a directional result.
    min_available_weight: float = 0.50


DEFAULT_CONFIG = BiasConfig()


# ---------------------------------------------------------------------------
# Safe primitives
# ---------------------------------------------------------------------------

def _f(v: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        x = float(v)
        return x if isfinite(x) else default
    except Exception:
        return default


def _first(m: Mapping[str, Any], names: Sequence[str], default: Any = None) -> Any:
    for k in names:
        if k in m and m.get(k) is not None:
            return m.get(k)
    return default

def _ts_close_ms(bar: Mapping[str, Any]) -> int:
    raw = _first(
        bar,
        (
            "t_close_ms",
            "tCloseMs",
            "close_time_ms",
            "close_ms",
        ),
        0,
    )

    try:
        x = int(float(raw))
    except Exception:
        return 0

    if 0 < x < 10_000_000_000:
        x *= 1000

    return x


_TF_DURATION_MS = {
    "H1": 60 * 60 * 1000,
    "H4": 4 * 60 * 60 * 1000,
    "D1": 24 * 60 * 60 * 1000,
}


def _bar_close_ms(
    bar: Mapping[str, Any],
    tf: str,
) -> int:
    """
    Prefer broker-published explicit close timestamp.

    Fallback:
        open timestamp + canonical timeframe duration.
    """
    close_ms = int(bar.get("t_close_ms") or 0)

    if close_ms > 0:
        return close_ms

    open_ms = int(bar.get("t") or 0)

    tf_ms = int(
        _TF_DURATION_MS.get(
            str(tf or "").upper().strip(),
            0,
        )
    )

    if open_ms > 0 and tf_ms > 0:
        return open_ms + tf_ms

    return open_ms


def _fx_market_closed_utc(dt: datetime) -> bool:
    """
    Approximate normal spot-FX weekend closure.

    Friday 22:00 UTC -> Sunday 22:00 UTC.

    This function is used ONLY for freshness aging.
    It does not control trading/session execution.
    """
    wd = dt.weekday()  # Mon=0 ... Sun=6

    if wd == 4 and dt.hour >= 22:
        return True

    if wd == 5:
        return True

    if wd == 6 and dt.hour < 22:
        return True

    return False


def _market_elapsed_hours(
    start_ms: int,
    end_ms: int,
) -> float:
    """
    Elapsed OPEN-market hours between two UTC timestamps.

    Weekend-closed time is excluded so Friday's valid H1/H4
    snapshot does not become stale merely because Saturday/Sunday pass.

    Conservative minute-level integration is more than accurate enough
    for H1/H4 freshness and avoids fragile calendar arithmetic.
    """
    if start_ms <= 0 or end_ms <= start_ms:
        return 0.0

    start = datetime.fromtimestamp(
        start_ms / 1000.0,
        tz=timezone.utc,
    )

    end = datetime.fromtimestamp(
        end_ms / 1000.0,
        tz=timezone.utc,
    )

    cursor = start
    open_seconds = 0.0

    step = timedelta(minutes=15)

    while cursor < end:
        nxt = min(cursor + step, end)

        if not _fx_market_closed_utc(cursor):
            open_seconds += (
                nxt - cursor
            ).total_seconds()

        cursor = nxt

    return open_seconds / 3600.0


def _ts_ms(bar: Mapping[str, Any]) -> int:
    raw = _first(
        bar,
        ("t_open_ms", "tOpenMs", "open_time_ms", "time_msc", "timestamp_ms",
         "ts_ms", "t", "time", "timestamp"),
        0,
    )
    try:
        x = int(float(raw))
    except Exception:
        return 0
    if 0 < x < 10_000_000_000:  # seconds -> milliseconds
        x *= 1000
    return x


def _completed(bar: Mapping[str, Any]) -> bool:
    for key in ("complete", "closed", "is_closed", "is_complete"):
        if key in bar:
            v = bar.get(key)
            if isinstance(v, str):
                return v.strip().lower() not in ("0", "false", "no", "open", "forming")
            return bool(v)
    return True


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, float(x)))


def normalize_bars(
    bars: Sequence[Mapping[str, Any]] | None,
    *,
    as_of_ms: Optional[int] = None,
    max_bars: Optional[int] = None,
) -> List[Dict[str, float | int]]:
    """Normalize, validate, sort and deduplicate completed OHLC bars."""
    by_ts: Dict[int, Dict[str, float | int]] = {}

    for raw in bars or ():
        if not isinstance(raw, Mapping) or not _completed(raw):
            continue
        ts = _ts_ms(raw)
        if ts <= 0 or (as_of_ms is not None and ts > int(as_of_ms)):
            continue

        o = _f(_first(raw, ("o", "open", "Open")))
        h = _f(_first(raw, ("h", "high", "High")))
        l = _f(_first(raw, ("l", "low", "Low")))
        c = _f(_first(raw, ("c", "close", "Close")))
        if None in (o, h, l, c):
            continue
        assert o is not None and h is not None and l is not None and c is not None
        if min(o, h, l, c) <= 0 or h < l or h < max(o, c) or l > min(o, c):
            continue

        close_ts = _ts_close_ms(raw)

        by_ts[ts] = {
            "t": ts,
            "t_close_ms": close_ts,
            "o": o,
            "h": h,
            "l": l,
            "c": c,
        }

    out = [by_ts[k] for k in sorted(by_ts)]
    if max_bars and max_bars > 0:
        out = out[-int(max_bars):]
    return out


def _atr(bars: Sequence[Mapping[str, float | int]], period: int) -> float:
    if not bars:
        return 0.0
    tr: List[float] = []
    prev: Optional[float] = None
    for b in bars:
        h, l, c = float(b["h"]), float(b["l"]), float(b["c"])
        tr.append(h - l if prev is None else max(h - l, abs(h - prev), abs(l - prev)))
        prev = c
    n = min(max(1, int(period)), len(tr))
    return float(mean(tr[-n:])) if n else 0.0


def _ema(values: Sequence[float], period: int) -> float:
    if not values:
        return 0.0
    a = 2.0 / (max(1, int(period)) + 1.0)
    out = float(values[0])
    for v in values[1:]:
        out = a * float(v) + (1.0 - a) * out
    return out


def _linreg(values: Sequence[float]) -> Tuple[float, float]:
    """Return slope/bar and R²."""
    n = len(values)
    if n < 3:
        return 0.0, 0.0
    xs = [float(i) for i in range(n)]
    ys = [float(v) for v in values]
    mx, my = mean(xs), mean(ys)
    sxx = sum((x - mx) ** 2 for x in xs)
    if sxx <= 0:
        return 0.0, 0.0
    slope = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / sxx
    intercept = my - slope * mx
    sst = sum((y - my) ** 2 for y in ys)
    if sst <= 0:
        return slope, 0.0
    ssr = sum((y - (intercept + slope * x)) ** 2 for x, y in zip(xs, ys))
    return float(slope), _clamp(1.0 - ssr / sst, 0.0, 1.0)


def _er(values: Sequence[float]) -> float:
    if len(values) < 2:
        return 0.0
    net = abs(float(values[-1]) - float(values[0]))
    path = sum(abs(float(values[i]) - float(values[i - 1])) for i in range(1, len(values)))
    return _clamp(net / path, 0.0, 1.0) if path > 0 else 0.0


def _signed_points(v: float, full_scale: float, max_points: float) -> float:
    return _clamp(v / full_scale, -1.0, 1.0) * max_points if full_scale > 0 else 0.0


# ---------------------------------------------------------------------------
# Swing structure / regime
# ---------------------------------------------------------------------------

def _pivot_highs(bars: Sequence[Mapping[str, float | int]], left: int, right: int) -> List[int]:
    out: List[int] = []
    for i in range(left, len(bars) - right):
        v = float(bars[i]["h"])
        if all(v > float(bars[j]["h"]) for j in range(i - left, i)) and all(
            v >= float(bars[j]["h"]) for j in range(i + 1, i + right + 1)
        ):
            out.append(i)
    return out


def _pivot_lows(bars: Sequence[Mapping[str, float | int]], left: int, right: int) -> List[int]:
    out: List[int] = []
    for i in range(left, len(bars) - right):
        v = float(bars[i]["l"])
        if all(v < float(bars[j]["l"]) for j in range(i - left, i)) and all(
            v <= float(bars[j]["l"]) for j in range(i + 1, i + right + 1)
        ):
            out.append(i)
    return out


def detect_swing_structure(
    bars: Sequence[Mapping[str, float | int]],
    *,
    left: int = 2,
    right: int = 2,
) -> Dict[str, Any]:
    hi = _pivot_highs(bars, left, right)
    lo = _pivot_lows(bars, left, right)
    out: Dict[str, Any] = {
        "state": "UNKNOWN",
        "high_pattern": None,
        "low_pattern": None,
        "pivot_high_count": len(hi),
        "pivot_low_count": len(lo),
    }
    if len(hi) < 2 or len(lo) < 2:
        return out

    ph0, ph1 = float(bars[hi[-2]]["h"]), float(bars[hi[-1]]["h"])
    pl0, pl1 = float(bars[lo[-2]]["l"]), float(bars[lo[-1]]["l"])
    hp = "HH" if ph1 > ph0 else "LH" if ph1 < ph0 else "EH"
    lp = "HL" if pl1 > pl0 else "LL" if pl1 < pl0 else "EL"
    state = "BULLISH" if hp == "HH" and lp == "HL" else "BEARISH" if hp == "LH" and lp == "LL" else "MIXED"
    out.update({
        "state": state,
        "high_pattern": hp,
        "low_pattern": lp,
        "prev_high": ph0,
        "last_high": ph1,
        "prev_low": pl0,
        "last_low": pl1,
    })
    return out


def _regime(er: float, r2: float, cfg: BiasConfig) -> str:
    if er >= cfg.trend_er_min and r2 >= cfg.trend_r2_min:
        return "TREND"
    if er <= cfg.range_er_max and r2 <= cfg.range_r2_max:
        return "RANGE"
    return "MIXED"


# ---------------------------------------------------------------------------
# One-timeframe analysis
# ---------------------------------------------------------------------------

def _tf_params(tf: str, cfg: BiasConfig) -> Tuple[int, int, int, float, float]:
    if tf == "D1":
        return cfg.min_bars_d1, cfg.max_bars_d1, cfg.direction_window_d1, cfg.max_age_d1_hours, cfg.tf_weight_d1
    if tf == "H4":
        return cfg.min_bars_h4, cfg.max_bars_h4, cfg.direction_window_h4, cfg.max_age_h4_hours, cfg.tf_weight_h4
    if tf == "H1":
        return cfg.min_bars_h1, cfg.max_bars_h1, cfg.direction_window_h1, cfg.max_age_h1_hours, cfg.tf_weight_h1
    raise ValueError(f"unsupported timeframe {tf}")


def analyze_timeframe(
    bars: Sequence[Mapping[str, Any]] | None,
    tf: str,
    *,
    as_of_ms: Optional[int] = None,
    cfg: BiasConfig = DEFAULT_CONFIG,
) -> Dict[str, Any]:
    tf = str(tf).upper().strip()
    min_bars, max_bars, window, max_age_h, weight = _tf_params(tf, cfg)
    norm = normalize_bars(bars, as_of_ms=as_of_ms, max_bars=max_bars)

    out: Dict[str, Any] = {
        "tf": tf,
        "weight": weight,
        "data_ok": False,
        "bars_used": len(norm),
        "min_bars": min_bars,
        "last_closed_ms": 0,
        "age_hours": None,
        "fresh": None,
        "direction": "UNKNOWN",
        "score": 0.0,
        "regime": "UNKNOWN",
        "structure": {"state": "UNKNOWN"},
        "features": {},
        "components": {},
        "reasons": [],
        "warnings": [],
    }

    if len(norm) < min_bars:
        out["warnings"].append(f"INSUFFICIENT_{tf}_BARS:{len(norm)}<{min_bars}")
        return out

    if as_of_ms is not None:
        last_close_ms = _bar_close_ms(
            norm[-1],
            tf,
        )

        wall_age_h = max(
            0.0,
            (
               int(as_of_ms)
               - int(last_close_ms)
            )
            / 3_600_000.0,
        )

        market_age_h = _market_elapsed_hours(
            int(last_close_ms),
            int(as_of_ms),
        )

        # Keep age_hours as the canonical freshness age.
        out["age_hours"] = round(
           market_age_h,
           3,
        )

        # Expose raw elapsed time for diagnostics.
        out["wall_age_hours"] = round(
            wall_age_h,
            3,
        )

        out["last_closed_ms"] = int(
            last_close_ms
        )

        out["fresh"] = (
            market_age_h <= max_age_h
        )

        if not out["fresh"]:
            out["warnings"].append(
                f"{tf}_STALE:"
                f"market_age={market_age_h:.2f}h>"
                f"{max_age_h:.2f}h "
                f"wall_age={wall_age_h:.2f}h"
            )

    else:
        out["fresh"] = True

    tail_n = max(min_bars, window, cfg.ema_slow + 2, cfg.atr_period + 2)
    tail = norm[-tail_n:]
    closes = [float(b["c"]) for b in tail]
    atr = _atr(tail, cfg.atr_period)
    if atr <= 0:
        out["warnings"].append(f"{tf}_ATR_INVALID")
        return out

    w = min(window, len(closes))
    dc = closes[-w:]
    slope, r2 = _linreg(dc)
    er = _er(dc)
    net_atr = (dc[-1] - dc[0]) / atr
    slope_atr = slope / atr
    ef, es = _ema(closes, cfg.ema_fast), _ema(closes, cfg.ema_slow)
    ema_spread_atr = (ef - es) / atr

    lo = min(float(b["l"]) for b in tail[-w:])
    hi = max(float(b["h"]) for b in tail[-w:])
    range_pos = 0.5 if hi <= lo else (closes[-1] - lo) / (hi - lo)
    range_signed = (range_pos - 0.5) * 2.0

    structure = detect_swing_structure(tail, left=cfg.pivot_left, right=cfg.pivot_right)
    st = structure.get("state")

    # Transparent score: no single feature can dominate alone.
    components = {
        "structure": 30.0 if st == "BULLISH" else -30.0 if st == "BEARISH" else 0.0,
        "slope": _signed_points(slope_atr, 0.10, 22.0),
        "net_displacement": _signed_points(net_atr, 2.0, 18.0),
        "ema_spread": _signed_points(ema_spread_atr, 0.60, 14.0),
        "efficiency": _signed_points(er if dc[-1] > dc[0] else -er if dc[-1] < dc[0] else 0.0, 0.55, 10.0),
        "range_position": _signed_points(range_signed, 1.0, 6.0),
    }
    score = _clamp(sum(components.values()), -100.0, 100.0)
    direction = "BULLISH" if score >= cfg.tf_bullish_threshold else "BEARISH" if score <= cfg.tf_bearish_threshold else "NEUTRAL"
    regime = _regime(er, r2, cfg)

    reasons: List[str] = [f"{tf}_REGIME_{regime}"]
    if st in ("BULLISH", "BEARISH"):
        reasons.append(f"{tf}_SWING_{st}")
    if slope_atr >= 0.04:
        reasons.append(f"{tf}_POSITIVE_SLOPE")
    elif slope_atr <= -0.04:
        reasons.append(f"{tf}_NEGATIVE_SLOPE")
    if net_atr >= 0.75:
        reasons.append(f"{tf}_POSITIVE_DISPLACEMENT")
    elif net_atr <= -0.75:
        reasons.append(f"{tf}_NEGATIVE_DISPLACEMENT")
    if ema_spread_atr >= 0.20:
        reasons.append(f"{tf}_FAST_ABOVE_SLOW")
    elif ema_spread_atr <= -0.20:
        reasons.append(f"{tf}_FAST_BELOW_SLOW")

    out.update({
        "data_ok": bool(out["fresh"]),
        "direction": direction if out["fresh"] else "UNKNOWN",
        "score": round(score, 3) if out["fresh"] else 0.0,
        "regime": regime,
        "structure": structure,
        "features": {
            "atr": atr,
            "window": w,
            "net_atr": round(net_atr, 5),
            "slope_atr_per_bar": round(slope_atr, 6),
            "r2": round(r2, 5),
            "efficiency_ratio": round(er, 5),
            "ema_fast": ef,
            "ema_slow": es,
            "ema_spread_atr": round(ema_spread_atr, 5),
            "range_position": round(range_pos, 5),
            "last_close": closes[-1],
        },
        "components": {k: round(v, 3) for k, v in components.items()},
        "reasons": reasons,
    })
    return out


# ---------------------------------------------------------------------------
# Multi-timeframe aggregation
# ---------------------------------------------------------------------------

def _aggregate(market: str, tfs: Mapping[str, Dict[str, Any]], pair_style: bool, cfg: BiasConfig) -> Dict[str, Any]:
    usable: List[Tuple[str, float, float]] = []
    unavailable: List[str] = []
    for tf in VALID_TFS:
        ctx = tfs.get(tf) or {}
        if ctx.get("data_ok"):
            usable.append((tf, float(ctx.get("weight") or 0.0), float(ctx.get("score") or 0.0)))
        else:
            unavailable.append(tf)

    avail_weight = sum(w for _, w, _ in usable)
    out: Dict[str, Any] = {
        "engine_version": BIAS_ENGINE_VERSION,
        "market": market,
        "data_ok": False,
        "data_quality": "NONE",
        "direction": "UNKNOWN",
        "raw_direction": "UNKNOWN",
        "score": 0.0,
        "confidence": "NONE",
        "available_timeframes": [tf for tf, _, _ in usable],
        "unavailable_timeframes": unavailable,
        "available_weight": round(avail_weight, 4),
        "timeframes": dict(tfs),
        "agreements": [],
        "conflicts": [],
        "reasons": [],
    }
    if not usable or avail_weight < cfg.min_available_weight:
        out["reasons"].append("INSUFFICIENT_MULTI_TF_WEIGHT")
        return out

    score = _clamp(sum((w / avail_weight) * s for _, w, s in usable), -100.0, 100.0)
    dirs = {tf: str((tfs.get(tf) or {}).get("direction") or "UNKNOWN") for tf in VALID_TFS}

    def compare(a: str, b: str, tag: str) -> None:
        da, db = dirs[a], dirs[b]
        if da in ("BULLISH", "BEARISH") and db in ("BULLISH", "BEARISH"):
            if da == db:
                out["agreements"].append(f"{tag}_AGREE")
            else:
                out["conflicts"].append(f"{tag}_CONFLICT")

    compare("D1", "H4", "D1_H4_DIRECTION")
    compare("H4", "H1", "H4_H1_DIRECTION")

    directional = [d for d in dirs.values() if d in ("BULLISH", "BEARISH")]
    if len(directional) >= 2 and len(set(directional)) == 1:
        out["agreements"].append("MULTI_TF_DIRECTION_AGREE")

    raw = "BULLISH" if score >= cfg.final_buy_threshold else "BEARISH" if score <= cfg.final_sell_threshold else "NEUTRAL"
    direction = ("BUY" if raw == "BULLISH" else "SELL" if raw == "BEARISH" else "NEUTRAL") if pair_style else raw

    a = abs(score)
    if raw == "NEUTRAL":
        confidence = "NEUTRAL"
    elif a >= cfg.confidence_high_score and not out["conflicts"] and avail_weight >= 0.99:
        confidence = "HIGH"
    elif a >= cfg.confidence_medium_score and len(out["conflicts"]) <= 1:
        confidence = "MEDIUM"
    else:
        confidence = "LOW"

    quality = "FULL" if avail_weight >= 0.99 else "GOOD" if avail_weight >= 0.75 else "PARTIAL"
    out.update({
        "data_ok": True,
        "data_quality": quality,
        "direction": direction,
        "raw_direction": raw,
        "score": round(score, 3),
        "confidence": confidence,
        "reasons": [f"AGG_SCORE_{score:+.1f}", f"D1_{dirs['D1']}", f"H4_{dirs['H4']}", f"H1_{dirs['H1']}"] + out["agreements"] + out["conflicts"],
    })
    return out


def compute_pair_bias(
    *,
    symbol: str,
    bars_d1: Sequence[Mapping[str, Any]] | None,
    bars_h4: Sequence[Mapping[str, Any]] | None,
    bars_h1: Sequence[Mapping[str, Any]] | None,
    as_of_ms: Optional[int] = None,
    cfg: BiasConfig = DEFAULT_CONFIG,
) -> Dict[str, Any]:
    sym = str(symbol or "").upper().strip()
    try:
        tfs = {
            "D1": analyze_timeframe(bars_d1, "D1", as_of_ms=as_of_ms, cfg=cfg),
            "H4": analyze_timeframe(bars_h4, "H4", as_of_ms=as_of_ms, cfg=cfg),
            "H1": analyze_timeframe(bars_h1, "H1", as_of_ms=as_of_ms, cfg=cfg),
        }
        out = _aggregate(sym or "PAIR", tfs, True, cfg)
        out.update({"role": "PAIR_BIAS", "symbol": sym})
        return out
    except Exception as exc:
        return _engine_error("PAIR_BIAS", sym, exc)


def compute_dxy_bias(
    *,
    bars_d1: Sequence[Mapping[str, Any]] | None,
    bars_h4: Sequence[Mapping[str, Any]] | None,
    bars_h1: Sequence[Mapping[str, Any]] | None,
    as_of_ms: Optional[int] = None,
    source: str = "UNKNOWN",
    cfg: BiasConfig = DEFAULT_CONFIG,
) -> Dict[str, Any]:
    try:
        tfs = {
            "D1": analyze_timeframe(bars_d1, "D1", as_of_ms=as_of_ms, cfg=cfg),
            "H4": analyze_timeframe(bars_h4, "H4", as_of_ms=as_of_ms, cfg=cfg),
            "H1": analyze_timeframe(bars_h1, "H1", as_of_ms=as_of_ms, cfg=cfg),
        }
        out = _aggregate("DXY", tfs, False, cfg)
        out.update({"role": "DXY_BIAS", "source": str(source or "UNKNOWN").upper().strip()})
        return out
    except Exception as exc:
        e = _engine_error("DXY_BIAS", "DXY", exc)
        e["source"] = str(source or "UNKNOWN").upper().strip()
        return e


def _engine_error(role: str, market: str, exc: Exception) -> Dict[str, Any]:
    return {
        "engine_version": BIAS_ENGINE_VERSION,
        "role": role,
        "market": market,
        "data_ok": False,
        "data_quality": "NONE",
        "direction": "UNKNOWN",
        "score": 0.0,
        "confidence": "NONE",
        "timeframes": {},
        "agreements": [],
        "conflicts": [],
        "reasons": [f"{role}_ENGINE_EXCEPTION:{type(exc).__name__}"],
    }


# ---------------------------------------------------------------------------
# Pair + DXY trade alignment
# ---------------------------------------------------------------------------

# +1: proposed trade benefits from DXY bullish / USD strength.
# -1: proposed trade benefits from DXY bearish / USD weakness.
_DXY_POLARITY: Dict[Tuple[str, str], int] = {
    ("EURUSD", "BUY"): -1, ("EURUSD", "SELL"): +1,
    ("GBPUSD", "BUY"): -1, ("GBPUSD", "SELL"): +1,
    ("USDJPY", "BUY"): +1, ("USDJPY", "SELL"): -1,
    ("USDCAD", "BUY"): +1, ("USDCAD", "SELL"): -1,
    ("USDCHF", "BUY"): +1, ("USDCHF", "SELL"): -1,
    # Gold/USD inverse USD relation is contextual, not guaranteed causality.
    ("XAUUSD", "BUY"): -1, ("XAUUSD", "SELL"): +1,
}


def _pair_relation(side: str, direction: str) -> str:
    d = str(direction or "UNKNOWN").upper().strip()
    if d == "UNKNOWN":
        return "UNAVAILABLE"
    if d == "NEUTRAL":
        return "NEUTRAL"
    return "ALIGNED" if d == side else "CONFLICT" if d in VALID_SIDES else "UNAVAILABLE"


def _dxy_relation(symbol: str, side: str, direction: str) -> str:
    d = str(direction or "UNKNOWN").upper().strip()
    if d == "UNKNOWN":
        return "UNAVAILABLE"
    if d == "NEUTRAL":
        return "NEUTRAL"
    polarity = _DXY_POLARITY.get((symbol, side))
    if polarity is None:
        return "UNAVAILABLE"
    supportive = (polarity > 0 and d == "BULLISH") or (polarity < 0 and d == "BEARISH")
    return "ALIGNED" if supportive else "CONFLICT"


def classify_trade_alignment(
    *,
    symbol: str,
    trade_side: str,
    pair_bias: Mapping[str, Any] | None,
    dxy_bias: Mapping[str, Any] | None,
) -> Dict[str, Any]:
    sym, side = str(symbol or "").upper().strip(), str(trade_side or "").upper().strip()
    if side not in VALID_SIDES:
        return {"symbol": sym, "trade_side": side, "classification": "UNAVAILABLE", "pair_relation": "UNAVAILABLE", "dxy_relation": "UNAVAILABLE", "reasons": ["INVALID_TRADE_SIDE"]}

    pb = pair_bias if isinstance(pair_bias, Mapping) else {}
    db = dxy_bias if isinstance(dxy_bias, Mapping) else {}
    pr = _pair_relation(side, str(pb.get("direction") or "UNKNOWN"))
    dr = _dxy_relation(sym, side, str(db.get("direction") or "UNKNOWN"))

    if pr == "ALIGNED" and dr == "ALIGNED":
        cls = "STRONG"
    elif pr == "ALIGNED" and dr in ("NEUTRAL", "UNAVAILABLE"):
        cls = "PAIR_ONLY"
    elif dr == "ALIGNED" and pr in ("NEUTRAL", "UNAVAILABLE"):
        cls = "DXY_ONLY"
    elif "ALIGNED" in (pr, dr) and "CONFLICT" in (pr, dr):
        cls = "MIXED"
    elif "CONFLICT" in (pr, dr):
        cls = "CONFLICT"
    elif pr == "NEUTRAL" and dr == "NEUTRAL":
        cls = "NEUTRAL"
    elif pr == "UNAVAILABLE" and dr == "UNAVAILABLE":
        cls = "UNAVAILABLE"
    else:
        cls = "NEUTRAL"

    return {
        "symbol": sym,
        "trade_side": side,
        "classification": cls,
        "pair_relation": pr,
        "dxy_relation": dr,
        "pair_direction": pb.get("direction", "UNKNOWN"),
        "pair_score": pb.get("score"),
        "pair_confidence": pb.get("confidence"),
        "dxy_direction": db.get("direction", "UNKNOWN"),
        "dxy_score": db.get("score"),
        "dxy_confidence": db.get("confidence"),
        "dxy_source": db.get("source"),
        "reasons": [f"PAIR_{pr}", f"DXY_{dr}"],
    }


# ---------------------------------------------------------------------------
# Complete frozen snapshot
# ---------------------------------------------------------------------------

def build_bias_snapshot(
    *,
    symbol: str,
    trade_side: str,
    pair_d1: Sequence[Mapping[str, Any]] | None,
    pair_h4: Sequence[Mapping[str, Any]] | None,
    pair_h1: Sequence[Mapping[str, Any]] | None,
    dxy_d1: Sequence[Mapping[str, Any]] | None = None,
    dxy_h4: Sequence[Mapping[str, Any]] | None = None,
    dxy_h1: Sequence[Mapping[str, Any]] | None = None,
    dxy_source: str = "UNKNOWN",
    as_of_ms: Optional[int] = None,
    cfg: BiasConfig = DEFAULT_CONFIG,
) -> Dict[str, Any]:
    """Build one side-effect-free shadow-bias snapshot."""
    sym, side = str(symbol or "").upper().strip(), str(trade_side or "").upper().strip()

    pair = compute_pair_bias(
        symbol=sym,
        bars_d1=pair_d1,
        bars_h4=pair_h4,
        bars_h1=pair_h1,
        as_of_ms=as_of_ms,
        cfg=cfg,
    )
    dxy = compute_dxy_bias(
        bars_d1=dxy_d1,
        bars_h4=dxy_h4,
        bars_h1=dxy_h1,
        as_of_ms=as_of_ms,
        source=dxy_source,
        cfg=cfg,
    )
    alignment = classify_trade_alignment(symbol=sym, trade_side=side, pair_bias=pair, dxy_bias=dxy)

    return {
        "schema_version": 1,
        "engine_version": BIAS_ENGINE_VERSION,
        "mode": "SHADOW",
        "computed_ms": int(as_of_ms or 0),
        "symbol": sym,
        "trade_side": side,
        "pair_bias": pair,
        "dxy_bias": dxy,
        "trade_alignment": alignment,
        "execution_contract": {
            "blocks_trade": False,
            "changes_zone": False,
            "changes_point_a": False,
            "changes_event_gate": False,
            "changes_risk": False,
        },
    }


def config_dict(cfg: BiasConfig = DEFAULT_CONFIG) -> Dict[str, Any]:
    return asdict(cfg)


__all__ = [
    "BIAS_ENGINE_VERSION",
    "BiasConfig",
    "DEFAULT_CONFIG",
    "normalize_bars",
    "detect_swing_structure",
    "analyze_timeframe",
    "compute_pair_bias",
    "compute_dxy_bias",
    "classify_trade_alignment",
    "build_bias_snapshot",
    "config_dict",
]
