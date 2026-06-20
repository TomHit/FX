# trend_sr.py (or top of trend_endpoints.py)
from __future__ import annotations
from typing import Any, Dict, List, Literal, Optional, Tuple
import math
import numpy as np
import pandas as pd
import os


import json
import time

def _norm_sym(sym: str) -> str:
    s = (sym or "").upper().strip()
    if ":" in s:
        s = s.split(":")[-1]
    return s

def _sr_has_any_levels(sr_bundle: dict) -> bool:
    """Return True if SR bundle has at least one support/resistance level in any TF."""
    if not isinstance(sr_bundle, dict):
        return False
    for tfk in ("h1", "h4", "m15", "m30", "d1", "d", "H1", "H4"):
        v = sr_bundle.get(tfk)
        if isinstance(v, dict):
            sups = v.get("supports") or []
            ress = v.get("resistances") or []
            if isinstance(sups, list) and len(sups) > 0:
                return True
            if isinstance(ress, list) and len(ress) > 0:
                return True
    # also check top-level merged lists if you store them
    for k in ("supports", "resistances", "supports_near", "resistances_near", "supports_major", "resistances_major"):
        v = sr_bundle.get(k)
        if isinstance(v, list) and len(v) > 0:
            return True
    return False

# -----------------------------
# Redis-backed SR "last good" cache (optional)
# -----------------------------
def _cache_get_json(cache, key: str):
    """Return parsed JSON dict from Redis-like cache or None."""
    if not cache or not key:
        return None
    try:
        raw = cache.get(key)
    except Exception:
        return None
    if not raw:
        return None
    try:
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", "ignore")
        return json.loads(raw)
    except Exception:
        return None


def _cache_set_json(cache, key: str, ttl_sec: int, obj):
    """Persist obj as JSON in Redis-like cache."""
    if not cache or not key or obj is None:
        return
    try:
        val = json.dumps(obj, separators=(",", ":"), ensure_ascii=False)
    except Exception:
        return
    try:
        if ttl_sec and hasattr(cache, "setex"):
            cache.setex(key, int(ttl_sec), val)
        else:
            cache.set(key, val)
    except Exception:
        return

def _atr14(df: pd.DataFrame) -> float:
    """
    Simple ATR(14) on H1/H4 frame for SR spacing.
    Expects columns: 'h', 'l', 'c'.
    """
    if df is None or df.empty:
        return 0.0

    h = df["h"].to_numpy(dtype="float64")
    l = df["l"].to_numpy(dtype="float64")
    c = df["c"].to_numpy(dtype="float64")

    prev_c = np.roll(c, 1)
    prev_c[0] = c[0]

    tr = np.maximum.reduce([
        h - l,
        np.abs(h - prev_c),
        np.abs(l - prev_c),
    ])

    n = min(14, len(tr))
    if n <= 0:
        return 0.0

    return float(pd.Series(tr[-n:]).mean())


def _find_swings(
    df: pd.DataFrame,
    lookback: int = 2,
) -> Tuple[List[float], List[float]]:
    """
    Detect simple swing highs/lows.
    Swing high: high[i] > high[i-k..i-1] and high[i] >= high[i+1..i+k]
    Swing low : low[i]  < low[i-k..i-1] and low[i] <= low[i+1..i+k]
    """
    highs = df["h"].to_numpy(dtype="float64")
    lows = df["l"].to_numpy(dtype="float64")

    swing_highs: List[float] = []
    swing_lows: List[float] = []

    n = len(df)
    if n < 2 * lookback + 1:
        return swing_highs, swing_lows

    for i in range(lookback, n - lookback):
        h = highs[i]
        l = lows[i]

        left_h = highs[i - lookback : i]
        right_h = highs[i + 1 : i + 1 + lookback]
        left_l = lows[i - lookback : i]
        right_l = lows[i + 1 : i + 1 + lookback]

        if h > left_h.max() and h >= right_h.max():
            swing_highs.append(float(h))
        if l < left_l.min() and l <= right_l.min():
            swing_lows.append(float(l))

    return swing_highs, swing_lows


def _count_distinct_touches(
    df: pd.DataFrame,
    zone_low: float,
    zone_high: float,
    side: str,
    atr: float,
) -> int:
    """
    Count how many DISTINCT times price approached a band, not how many bars
    sat inside it. A consolidation of 40 bars inside the band = 1 touch, not 40.
    A new touch is counted when price has LEFT the band and then RE-ENTERS it.
    """
    try:
        if df is None or df.empty or zone_high < zone_low:
            return 0
        leave_buffer = max(0.25 * float(atr), 1e-9)
        highs = df["h"].to_numpy(dtype="float64")
        lows = df["l"].to_numpy(dtype="float64")
        closes = df["c"].to_numpy(dtype="float64")

        touches = 0
        outside = True
        for i in range(len(df)):
            bar_lo = lows[i]
            bar_hi = highs[i]
            in_band = (bar_hi >= zone_low) and (bar_lo <= zone_high)
            if in_band:
                if outside:
                    touches += 1
                    outside = False
            else:
                if side == "support":
                    if closes[i] > zone_high + leave_buffer or bar_lo > zone_high:
                        outside = True
                    elif closes[i] < zone_low - leave_buffer:
                        outside = True
                else:
                    if closes[i] < zone_low - leave_buffer or bar_hi < zone_low:
                        outside = True
                    elif closes[i] > zone_high + leave_buffer:
                        outside = True
        return int(touches)
    except Exception:
        return 0

def _cluster_levels(
    levels: List[float],
    tol: float,
) -> List[Dict[str, Any]]:
    """
    Cluster raw levels within 'tol'.

    Robust SR bands:
      - level = median (robust center)
      - low/high from IQR (Q1..Q3) with adaptive widening
      - optional Tukey outlier removal (touches>=5)
      - enforce minimum width based on tol (prevents collapsed bands)
    """
    if not levels:
        return []

    levels_sorted = sorted(levels)
    clusters: List[List[float]] = []
    current: List[float] = [levels_sorted[0]]

    max_cluster_span = max(2.0 * float(tol), 12.0 * 0.0001)

    for lvl in levels_sorted[1:]:
        if abs(lvl - current[-1]) <= tol and abs(lvl - current[0]) <= max_cluster_span:
            current.append(lvl)
        else:
            clusters.append(current)
            current = [lvl]
    if current:
        clusters.append(current)

    out: List[Dict[str, Any]] = []

    # Minimum band width safety (smaller than your tol/2.0)
    # tol is ~0.25*ATR in your caller; this yields a modest minimum band.
    min_half_width = max(0.10 * float(tol), 1e-6)

    for cluster in clusters:
        xs = np.array(cluster, dtype=float)
        touches = int(xs.size)

        xs_used = xs
        outliers_removed = 0

        # Tukey outlier filter for stability (only when enough points)
        if touches >= 5:
            q1_raw = float(np.percentile(xs, 25))
            q3_raw = float(np.percentile(xs, 75))
            iqr_raw = float(q3_raw - q1_raw)
            if iqr_raw > 0:
                lo_fence = q1_raw - 1.5 * iqr_raw
                hi_fence = q3_raw + 1.5 * iqr_raw
                mask = (xs >= lo_fence) & (xs <= hi_fence)
                xs_f = xs[mask]
                # keep at least 3 samples; otherwise keep original
                if xs_f.size >= 3:
                    outliers_removed = int(xs.size - xs_f.size)
                    xs_used = xs_f

        lvl = float(np.median(xs_used))

        q1 = float(np.percentile(xs_used, 25))
        q3 = float(np.percentile(xs_used, 75))
        iqr = float(q3 - q1)

        # Adaptive band based on strength (touches)
        if touches >= 5:
            zone_low = q1
            zone_high = q3
            band_type = "tight_iqr"
        elif touches >= 3:
            buffer = 0.25 * iqr
            zone_low = q1 - buffer
            zone_high = q3 + buffer
            band_type = "medium_iqr"
        else:
            buffer = 0.50 * iqr
            zone_low = q1 - buffer
            zone_high = q3 + buffer
            band_type = "wide_iqr"

        # Enforce minimum width
        cur_half = float(zone_high - zone_low) / 2.0
        if (not np.isfinite(zone_low)) or (not np.isfinite(zone_high)) or cur_half < float(min_half_width):
            zone_low = float(lvl) - float(min_half_width)
            zone_high = float(lvl) + float(min_half_width)
            band_type = f"{band_type}_expanded"

        # Debug cluster stats (use original cluster, not filtered)
        cmin = float(np.min(xs)) if xs.size else lvl
        cmax = float(np.max(xs)) if xs.size else lvl

        out.append(
            {
                # Core (what gate/picker/watch expects)
                "level": float(lvl),
                "low": float(zone_low),
                "high": float(zone_high),

                # Strength
                "touches": int(touches),
                "strength": int(touches),

                # Helpful metadata
                "band_width": float(zone_high - zone_low),
                "band_type": str(band_type),
                "q1": float(q1),
                "q3": float(q3),
                "iqr": float(iqr),
                "median": float(lvl),
                "outliers_removed": int(outliers_removed),

                # Cluster debug
                "cluster_min": float(cmin),
                "cluster_max": float(cmax),
                "cluster_range": float(cmax - cmin),
                "cluster_std": float(np.std(xs)) if xs.size else 0.0,
            }
        )

    out.sort(key=lambda x: (-int(x.get("strength") or 0), float(x.get("level") or 0.0)))
    return out


def _find_structure_origin_levels(
    df: pd.DataFrame,
    atr: float,
    tf_label: str,
    pip_factor: float,
    base_bars: int = 2,
    impulse_atr_mult: float = 1.0,
    max_out: int = 12,
) -> tuple[list[dict], list[dict]]:
    """
    Detect STRUCTURE_ORIGIN SR:
      support = small base before strong bullish impulse
      resistance = small base before strong bearish impulse

    This complements swing-cluster SR.
    """
    supports: list[dict] = []
    resistances: list[dict] = []

    try:
        if df is None or df.empty or len(df) < base_bars + 3 or atr <= 0:
            return supports, resistances

        h = df["h"].to_numpy(dtype="float64")
        l = df["l"].to_numpy(dtype="float64")
        c = df["c"].to_numpy(dtype="float64")

        min_width = max(0.10 * atr, 5.0 * pip_factor)

        for i in range(base_bars, len(df) - 1):
            b0 = i - base_bars
            b1 = i

            base_high = float(np.max(h[b0:b1]))
            base_low = float(np.min(l[b0:b1]))
            base_mid = float((base_high + base_low) / 2.0)
            base_width = float(base_high - base_low)

            # base should not be huge
            if base_width > 2.00 * atr:
                continue

            impulse = float(c[i + 1] - c[i])
            impulse_abs = abs(impulse)

            if impulse_abs < impulse_atr_mult * atr:
                continue

            zone_low = float(base_low)
            zone_high = float(base_high)

            if (zone_high - zone_low) < min_width:
                zone_low = base_mid - min_width / 2.0
                zone_high = base_mid + min_width / 2.0

            strength = max(1, int(round(impulse_abs / atr * 2)))

            row = {
                "level": float(base_mid),
                "low": float(zone_low),
                "high": float(zone_high),
                "touches": 1,
                "strength": strength,
                "band_width": float(zone_high - zone_low),
                "band_type": "structure_origin",
                "source_type": "STRUCTURE_ORIGIN",
                "tf": tf_label,
                "impulse_atr": round(float(impulse_abs / atr), 3),
                "base_bars": int(base_bars),
            }

            if impulse > 0:
                row["kind"] = "support"
                supports.append(row)
            else:
                row["kind"] = "resistance"
                resistances.append(row)

        supports.sort(key=lambda x: (-float(x.get("strength") or 0), -float(x.get("level") or 0)))
        resistances.sort(key=lambda x: (-float(x.get("strength") or 0), float(x.get("level") or 0)))

        return supports[:max_out], resistances[:max_out]

    except Exception:
        return supports, resistances


def _find_reaction_zones(
    df: pd.DataFrame,
    atr: float,
    tf_label: str,
    pip_factor: float,
    lookahead: int = 5,
    impulse_atr_mult: float = 1.5,
    max_out: int = 12,
) -> tuple[list[dict], list[dict]]:
    """
    Detect major demand/supply reaction zones.

    Major support:
      price makes a low, then next few candles create strong bullish reaction.

    Major resistance:
      price makes a high, then next few candles create strong bearish reaction.
    """
    supports: list[dict] = []
    resistances: list[dict] = []

    try:
        if df is None or df.empty or len(df) < lookahead + 5 or atr <= 0:
            return supports, resistances

        h = df["h"].to_numpy(dtype="float64")
        l = df["l"].to_numpy(dtype="float64")
        o = df["o"].to_numpy(dtype="float64") if "o" in df.columns else df["c"].to_numpy(dtype="float64")
        c = df["c"].to_numpy(dtype="float64")

        min_width = max(0.15 * atr, 8.0 * pip_factor)

        for i in range(2, len(df) - lookahead):
            cur_low = float(l[i])
            cur_high = float(h[i])
            cur_open = float(o[i])
            cur_close = float(c[i])

            # local extreme filter
            prev_low = float(np.min(l[max(0, i - 2):i]))
            prev_high = float(np.max(h[max(0, i - 2):i]))

            future_high = float(np.max(h[i + 1:i + 1 + lookahead]))
            future_low = float(np.min(l[i + 1:i + 1 + lookahead]))

            bullish_reaction = future_high - cur_low
            bearish_reaction = cur_high - future_low

            body_low = min(cur_open, cur_close)
            body_high = max(cur_open, cur_close)

            # -------------------------
            # Major demand / support
            # -------------------------
            is_local_low = cur_low <= prev_low or cur_low <= float(np.min(l[max(0, i - 5):i]))

            if is_local_low and bullish_reaction >= impulse_atr_mult * atr:
                zone_low = cur_low
                zone_high = max(body_low, cur_low + min_width)

                if zone_high - zone_low < min_width:
                    zone_high = zone_low + min_width

                impulse_atr = bullish_reaction / atr
                _imp_capped = min(float(impulse_atr), 3.0)
                strength = max(3, int(round(_imp_capped * 4)))

                supports.append({
                    "level": float((zone_low + zone_high) / 2.0),
                    "low": float(zone_low),
                    "high": float(zone_high),
                    "touches": 1,
                    "strength": int(strength),
                    "band_width": float(zone_high - zone_low),
                    "band_type": "demand_reaction",
                    "source_type": "REACTION_ZONE",
                    "tf": tf_label,
                    "kind": "support",
                    "impulse_atr": round(float(impulse_atr), 3),
                    "reaction_bars": int(lookahead),
                    "origin_index": int(i),
                })

            # -------------------------
            # Major supply / resistance
            # -------------------------
            is_local_high = cur_high >= prev_high or cur_high >= float(np.max(h[max(0, i - 5):i]))

            if is_local_high and bearish_reaction >= impulse_atr_mult * atr:
                zone_high = cur_high
                zone_low = min(body_high, cur_high - min_width)

                if zone_high - zone_low < min_width:
                    zone_low = zone_high - min_width

                impulse_atr = bearish_reaction / atr
                _imp_capped = min(float(impulse_atr), 3.0)
                strength = max(3, int(round(_imp_capped * 4)))

                resistances.append({
                    "level": float((zone_low + zone_high) / 2.0),
                    "low": float(zone_low),
                    "high": float(zone_high),
                    "touches": 1,
                    "strength": int(strength),
                    "band_width": float(zone_high - zone_low),
                    "band_type": "supply_reaction",
                    "source_type": "REACTION_ZONE",
                    "tf": tf_label,
                    "kind": "resistance",
                    "impulse_atr": round(float(impulse_atr), 3),
                    "reaction_bars": int(lookahead),
                    "origin_index": int(i),
                })

        supports.sort(key=lambda x: (-float(x.get("strength") or 0), -float(x.get("level") or 0)))
        resistances.sort(key=lambda x: (-float(x.get("strength") or 0), float(x.get("level") or 0)))

        return supports[:max_out], resistances[:max_out]

    except Exception:
        return supports, resistances

def _find_base_impulse_zones(
    df: pd.DataFrame,
    atr: float,
    tf_label: str,
    pip_factor: float,
    base_bars: int = 4,
    lookahead: int = 8,
    impulse_atr_mult: float = 1.0,
    max_out: int = 12,
) -> tuple[list[dict], list[dict]]:
    supports: list[dict] = []
    resistances: list[dict] = []

    try:
        if df is None or df.empty or len(df) < base_bars + lookahead + 5 or atr <= 0:
            return supports, resistances

        h = df["h"].to_numpy(dtype="float64")
        l = df["l"].to_numpy(dtype="float64")
        c = df["c"].to_numpy(dtype="float64")

        min_width = max(0.20 * atr, 10.0 * pip_factor)

        for i in range(base_bars, len(df) - lookahead):
            b0 = i - base_bars
            b1 = i

            base_low = float(np.min(l[b0:b1]))
            base_high = float(np.max(h[b0:b1]))
            base_mid = float((base_low + base_high) / 2.0)
            base_width = float(base_high - base_low)

            if base_width <= 0 or base_width > 1.50 * atr:
                continue

            future_high = float(np.max(h[i:i + lookahead]))
            future_low = float(np.min(l[i:i + lookahead]))

            rally = future_high - base_high
            drop = base_low - future_low

            if rally >= impulse_atr_mult * atr:
                zlow = base_low
                zhigh = base_high
                if zhigh - zlow < min_width:
                    zlow = base_mid - min_width / 2.0
                    zhigh = base_mid + min_width / 2.0

                impulse_atr = rally / atr
                supports.append({
                    "level": float((zlow + zhigh) / 2.0),
                    "low": float(zlow),
                    "high": float(zhigh),
                    "touches": int(base_bars),
                    "strength": max(4, int(round(impulse_atr * 5)) + base_bars),
                    "band_width": float(zhigh - zlow),
                    "band_type": "base_impulse_demand",
                    "source_type": "BASE_IMPULSE",
                    "tf": tf_label,
                    "kind": "support",
                    "impulse_atr": round(float(impulse_atr), 3),
                    "base_bars": int(base_bars),
                    "lookahead": int(lookahead),
                })

            if drop >= impulse_atr_mult * atr:
                zlow = base_low
                zhigh = base_high
                if zhigh - zlow < min_width:
                    zlow = base_mid - min_width / 2.0
                    zhigh = base_mid + min_width / 2.0

                impulse_atr = drop / atr
                resistances.append({
                    "level": float((zlow + zhigh) / 2.0),
                    "low": float(zlow),
                    "high": float(zhigh),
                    "touches": int(base_bars),
                    "strength": max(4, int(round(impulse_atr * 5)) + base_bars),
                    "band_width": float(zhigh - zlow),
                    "band_type": "base_impulse_supply",
                    "source_type": "BASE_IMPULSE",
                    "tf": tf_label,
                    "kind": "resistance",
                    "impulse_atr": round(float(impulse_atr), 3),
                    "base_bars": int(base_bars),
                    "lookahead": int(lookahead),
                })

        supports.sort(key=lambda x: (-float(x.get("strength") or 0), -float(x.get("level") or 0)))
        resistances.sort(key=lambda x: (-float(x.get("strength") or 0), float(x.get("level") or 0)))
        return supports[:max_out], resistances[:max_out]

    except Exception:
        return supports, resistances
def compute_sr_for_frame(
    df: pd.DataFrame,
    tf_label: Literal["H1", "H4"],
    pip_factor: float,
    max_levels_per_side: int = 25,
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Compute SR levels for a single timeframe (H1 or H4).
    Returns dict with 'supports' and 'resistances'.
    """
    if df is None or df.empty:
        return {"supports": [], "resistances": []}

    atr = _atr14(df)
    if atr <= 0:
        atr = float(df["h"].max() - df["l"].min()) / 50.0 or 0.0001

    # tolerance ~ 0.25 * ATR
    # Wider clustering for FX zones.
    # H1 bases often spread 8–15 pips, so 0.25 ATR is too tight.
    if tf_label == "H1":
        tol = max(0.35 * atr, 5.0 * pip_factor)
    else:
        tol = max(0.50 * atr, 10.0 * pip_factor)

    swing_highs_2, swing_lows_2 = _find_swings(df, lookback=2)
    swing_highs_1, swing_lows_1 = _find_swings(df, lookback=1)

    swing_highs = sorted(swing_highs_2 + swing_highs_1)
    swing_lows  = sorted(swing_lows_2 + swing_lows_1)
    # Recent raw H/L clusters catch active SR that may not be confirmed swing pivots yet.
    recent_n = 120 if tf_label == "H1" else 120
    recent_df = df.tail(recent_n) if len(df) > recent_n else df

    raw_recent_lows = []
    raw_recent_highs = []

    try:
        raw_recent_lows = [float(x) for x in recent_df["l"].dropna().tolist()]
        raw_recent_highs = [float(x) for x in recent_df["h"].dropna().tolist()]
    except Exception:
        raw_recent_lows = []
        raw_recent_highs = []

    recent_low_clusters = _cluster_levels(raw_recent_lows, tol=max(0.75 * atr, 10.0 * pip_factor))
    recent_high_clusters = _cluster_levels(raw_recent_highs, tol=max(0.75 * atr, 10.0 * pip_factor))

    # FIX: recent clusters were built from EVERY bar's high/low, so 'touches'
    # equalled bars-in-band. Recompute as DISTINCT approaches.
    for _row in recent_low_clusters:
        try:
            _t = _count_distinct_touches(recent_df, float(_row.get("low")), float(_row.get("high")), "support", atr)
            _row["touches"] = int(_t)
            _row["strength"] = int(_t)
        except Exception:
            pass
    for _row in recent_high_clusters:
        try:
            _t = _count_distinct_touches(recent_df, float(_row.get("low")), float(_row.get("high")), "resistance", atr)
            _row["touches"] = int(_t)
            _row["strength"] = int(_t)
        except Exception:
            pass
    # ------------------------------------------------------------
    # Current structure extremes
    # Purpose:
    #   Catch obvious current market support/resistance even when
    #   it is not a confirmed swing pivot or strength-sorted cluster.
    # ------------------------------------------------------------
    current_structure_supports = []
    current_structure_resistances = []

    try:
        struct_n = 120 if tf_label == "H1" else 80
        sdf = df.tail(struct_n) if len(df) > struct_n else df

        lows_arr = sdf["l"].to_numpy(dtype="float64")
        highs_arr = sdf["h"].to_numpy(dtype="float64")

        if len(lows_arr) >= 10:
            min_idx = int(np.argmin(lows_arr))
            max_idx = int(np.argmax(highs_arr))

            min_low = float(lows_arr[min_idx])
            max_high = float(highs_arr[max_idx])

            # bounce/drop after the extreme inside same recent window
            bounce_after_low = float(np.max(highs_arr[min_idx:]) - min_low)
            drop_after_high = float(max_high - np.min(lows_arr[max_idx:]))

            half = max(0.20 * float(atr), 10.0 * float(pip_factor))

            if bounce_after_low >= max(0.75 * float(atr), 20.0 * float(pip_factor)):
                current_structure_supports.append({
                    "level": float(min_low + half),
                    "low": float(min_low),
                    "high": float(min_low + 2.0 * half),
                    "touches": 1,
                    "strength": max(5, int(round((bounce_after_low / float(atr)) * 3))) if atr else 5,
                    "band_width": float(2.0 * half),
                    "band_type": "current_structure_low",
                    "source_type": "CURRENT_STRUCTURE_LOW",
                    "tf": tf_label,
                    "kind": "support",
                    "bounce_atr": round(float(bounce_after_low / float(atr)), 3) if atr else None,
                })

            if drop_after_high >= max(0.75 * float(atr), 20.0 * float(pip_factor)):
                current_structure_resistances.append({
                    "level": float(max_high - half),
                    "low": float(max_high - 2.0 * half),
                    "high": float(max_high),
                    "touches": 1,
                    "strength": max(5, int(round((drop_after_high / float(atr)) * 3))) if atr else 5,
                    "band_width": float(2.0 * half),
                    "band_type": "current_structure_high",
                    "source_type": "CURRENT_STRUCTURE_HIGH",
                    "tf": tf_label,
                    "kind": "resistance",
                    "drop_atr": round(float(drop_after_high / float(atr)), 3) if atr else None,
            })
    except Exception:
        current_structure_supports = []
        current_structure_resistances = []

    for row in recent_low_clusters:
        row["source_type"] = "RECENT_LOW_CLUSTER"
        row["band_type"] = str(row.get("band_type") or "") + "_recent_low"
        row["kind"] = "support"
        row["tf"] = tf_label

    for row in recent_high_clusters:
        row["source_type"] = "RECENT_HIGH_CLUSTER"
        row["band_type"] = str(row.get("band_type") or "") + "_recent_high"
        row["kind"] = "resistance"
        row["tf"] = tf_label
    raw_swing_resistances = []
    raw_swing_supports = []

    for lv in swing_highs:
        raw_swing_resistances.append({
            "level": float(lv),
            "low": float(lv) - max(0.10 * atr, 5.0 * pip_factor),
            "high": float(lv) + max(0.10 * atr, 5.0 * pip_factor),
            "touches": 1,
            "strength": 1,
            "band_width": 2.0 * max(0.10 * atr, 5.0 * pip_factor),
            "band_type": "raw_swing_high",
            "source_type": "RAW_SWING",
            "tf": tf_label,
            "kind": "resistance",
        })

    for lv in swing_lows:
        raw_swing_supports.append({
            "level": float(lv),
            "low": float(lv) - max(0.10 * atr, 5.0 * pip_factor),
            "high": float(lv) + max(0.10 * atr, 5.0 * pip_factor),
            "touches": 1,
            "strength": 1,
            "band_width": 2.0 * max(0.10 * atr, 5.0 * pip_factor),
            "band_type": "raw_swing_low",
            "source_type": "RAW_SWING",
            "tf": tf_label,
            "kind": "support",
        })

    res_clusters = _cluster_levels(swing_highs, tol=tol)
    sup_clusters = _cluster_levels(swing_lows, tol=tol)

    
    origin_supports, origin_resistances = _find_structure_origin_levels(
        df=df,
        atr=atr,
        tf_label=tf_label,
        pip_factor=pip_factor,
    )

    reaction_supports, reaction_resistances = _find_reaction_zones(
        df=df,
        atr=atr,
        tf_label=tf_label,
        pip_factor=pip_factor,
        lookahead=5,
        impulse_atr_mult=1.0,
    )
    base_supports, base_resistances = _find_base_impulse_zones(
        df=df,
        atr=atr,
        tf_label=tf_label,
        pip_factor=pip_factor,
        base_bars=4,
        lookahead=8,
        impulse_atr_mult=1.0,
    )

    sup_clusters = (
        sup_clusters
        + origin_supports
        + reaction_supports
        + base_supports
        + current_structure_supports
        + raw_swing_supports
        + recent_low_clusters
    )

    res_clusters = (
        res_clusters
        + origin_resistances
        + reaction_resistances
        + base_resistances
        + current_structure_resistances
        + raw_swing_resistances
        + recent_high_clusters
    )
    

    # weight strength by timeframe
    tf_weight = 2.0 if tf_label == "H4" else 1.0
    for row in res_clusters:
        row["strength"] = row["strength"] * tf_weight
        row["kind"] = "resistance"
        row["tf"] = tf_label
    for row in sup_clusters:
        row["strength"] = row["strength"] * tf_weight
        row["kind"] = "support"
        row["tf"] = tf_label

    return {
        # keep full detected SR bundle
        "supports": sup_clusters,
        "resistances": res_clusters,

        # optional limited views for UI/debug
        "supports_top": sup_clusters[:max_levels_per_side],
        "resistances_top": res_clusters[:max_levels_per_side],
    }

def _consolidate_zones(levels, atr, merge_atr=0.5, min_sep_atr=1.0):
    """Merge same-side levels within merge_atr*ATR into clean zones, then
    enforce min_sep_atr*ATR spacing. Produces a hand-drawn-chart-style map."""
    if not levels or atr <= 0:
        return levels or []
    merge_tol = float(merge_atr) * float(atr)
    min_sep = float(min_sep_atr) * float(atr)
    xs = sorted([x for x in levels if isinstance(x, dict) and x.get("level") is not None],
                key=lambda r: float(r.get("level")))
    if not xs:
        return []
    groups = [[xs[0]]]
    for x in xs[1:]:
        if abs(float(x["level"]) - float(groups[-1][-1]["level"])) <= merge_tol:
            groups[-1].append(x)
        else:
            groups.append([x])
    merged = []
    for g in groups:
        best = max(g, key=lambda r: float(r.get("sr_score") or r.get("strength") or 0.0))
        lows = [float(r.get("low", r.get("level"))) for r in g]
        highs = [float(r.get("high", r.get("level"))) for r in g]
        tfs = {str(r.get("tf") or "") for r in g if r.get("tf")}
        srcs = {str(r.get("source_type") or r.get("band_type") or "") for r in g}
        z = dict(best)
        _raw_low, _raw_high = min(lows), max(highs)
        _center = round((_raw_low + _raw_high) / 2.0, 5)
        _max_half = 0.30 * float(atr)   # cap total zone width at ~0.6 ATR
        z["low"] = round(max(_raw_low, _center - _max_half), 5)
        z["high"] = round(min(_raw_high, _center + _max_half), 5)
        z["level"] = _center
        z["touches"] = max(int(r.get("touches") or 0) for r in g)
        z["merged_count"] = len(g)
        z["merged_tfs"] = sorted(tfs)
        z["merged_sources"] = sorted(s for s in srcs if s)
        z["htf_confluence"] = len(tfs) >= 2
        bump = (2.0 if z["htf_confluence"] else 0.0) + (1.0 if len(srcs) >= 2 else 0.0)
        z["sr_score"] = round(float(best.get("sr_score") or 0.0) + bump, 3)
        merged.append(z)
    merged.sort(key=lambda r: -float(r.get("sr_score") or 0.0))
    kept = []
    for z in merged:
        if not any(abs(float(z["level"]) - float(k["level"])) < min_sep for k in kept):
            kept.append(z)
    kept.sort(key=lambda r: float(r.get("level")))
    return kept


def _mark_broken_levels(levels, df, side, atr, confirm_closes=2):
    """Sweep-safe break detection. broken = confirm_closes consecutive closes
    beyond the zone w/o reclaim. close-beyond-then-reclaim = swept (survives)."""
    if not levels or df is None or df.empty or atr <= 0:
        return levels or []
    closes = df["c"].to_numpy(dtype="float64")
    if len(closes) < confirm_closes + 1:
        return levels
    recent = closes[-(confirm_closes + 3):]
    for z in levels:
        try:
            z_low = float(z.get("low", z.get("level")))
            z_high = float(z.get("high", z.get("level")))
        except Exception:
            continue
        z["broken"] = False; z["swept"] = False; z["reclaimed"] = False
        tail = closes[-confirm_closes:]
        if side == "support":
            if bool(np.all(tail < z_low)):
                z["broken"] = True
            elif bool(np.any(recent < z_low) and closes[-1] >= z_low):
                z["swept"] = True; z["reclaimed"] = True
        else:
            if bool(np.all(tail > z_high)):
                z["broken"] = True
            elif bool(np.any(recent > z_high) and closes[-1] <= z_high):
                z["swept"] = True; z["reclaimed"] = True
    return levels


def _select_active_levels(levels, price, side, atr, top_n=3, buffer_atr=0.10):
    """Pointer over the inventory: nearest top_n VALID (not broken) levels on the
    correct side of price. Broken levels go dormant (excluded), not flipped here."""
    if not levels or not price or price <= 0:
        return []
    buf = float(buffer_atr) * float(atr or 0.0)
    out = []
    for z in levels:
        if z.get("broken"):
            continue
        try:
            lv = float(z.get("level"))
        except Exception:
            continue
        if side == "support" and lv <= price - buf:
            out.append(z)
        elif side == "resistance" and lv >= price + buf:
            out.append(z)
    out.sort(key=lambda z: abs(price - float(z.get("level"))))
    return out[:top_n]


def summarize_sr_multi_tf(
    symbol: str,
    price: float,
    h4_df: Optional[pd.DataFrame],
    h1_df: Optional[pd.DataFrame],
    pip_factor: float,
    cache=None,
    cache_ttl_sec: int = int(os.getenv("XTL_SR_LAST_TTL_SEC", "300")),
    good_ttl_sec: int = int(os.getenv("XTL_SR_BUNDLE_TTL_SEC", "900")),
) -> Dict[str, Any]:
    """
    Combine H4 + H1 SR into a single summary for a symbol.

    Adds per-TF views:
      - supports_near / resistances_near (actionable: correct-side, within NEAR_ATR)
      - supports_major / resistances_major (strongest, can be far)
      - each level includes: distance, distance_atr, side_ok, stale

    Still returns existing keys:
      nearest_support / nearest_resistance / distance_pips / distance_atr / sr_safety
    """
    out: Dict[str, Any] = {
        "symbol": symbol,
        "h4": {},
        "h1": {},
        "nearest_support": None,
        "nearest_resistance": None,
        "distance_pips": {"support": None, "resistance": None},
        "distance_atr": {"support": None, "resistance": None},
        "sr_safety": "unknown",
    }

    
    sym_u = _norm_sym(symbol)

    

    # canonical keys
    k_last = f"xtl:sr:bundle:last:{sym_u}"
    k_good = f"xtl:sr:bundle:last_good:{sym_u}"

    # If OHLC frames are missing, fall back to last-good SR bundle.
    if (h4_df is None or len(h4_df) < 50) and (h1_df is None or len(h1_df) < 50):
        out["_early_return"] = "missing_frames"
        try:
            _cache_set_json(cache, k_last, cache_ttl_sec, out)  # <-- always create LAST
        except Exception:
            pass
        cached = _cache_get_json(cache, k_good)
        if isinstance(cached, dict):
            cached["_cache"] = "last_good_missing_frames"
            return cached
        return out

    if price is None or price <= 0:
        out["_early_return"] = "no_price"
        out["_price_in"] = price
        try:
            _cache_set_json(cache, k_last, cache_ttl_sec, out)  # <-- always create LAST
        except Exception:
            pass
        cached = _cache_get_json(cache, k_good)
        if isinstance(cached, dict):
            cached["_cache"] = "last_good_no_price"
            return cached
        return out

    # per-TF SR (RAW)
    h4_sr = compute_sr_for_frame(h4_df, "H4", pip_factor) if h4_df is not None else {"supports": [], "resistances": []}
    h1_sr = compute_sr_for_frame(h1_df, "H1", pip_factor) if h1_df is not None else {"supports": [], "resistances": []}

    # ATR for distance normalization: prefer H1 if available
    atr_frame = h1_df if h1_df is not None and not getattr(h1_df, "empty", True) else h4_df
    atr = _atr14(atr_frame) if atr_frame is not None else 0.0
    if atr <= 0:
        atr = 1.0  # fallback just to avoid division by zero

    # ---- NEW: enrich + compute "near" and "major" SR lists (per TF) ----
    NEAR_ATR = 3.0      # actionable window (tune 2..4)
    NEAR_TOPK = 5       # show up to N near levels per side
    MAJOR_TOPK = 10      # show top K strongest levels per side

    # hysteresis buffer: avoid flip-flop around SR
    cross_buf = max(0.05 * float(atr), 3.0 * float(pip_factor))

    # expose for consumers (UI/entry)
    out["atr"] = float(atr)
    out["cross_buf"] = float(cross_buf)
    out["pip_factor"] = float(pip_factor)
    out["price"] = float(price)	

    def _annotate(levels: List[Dict[str, Any]], kind: str, tf: str) -> List[Dict[str, Any]]:
        xs: List[Dict[str, Any]] = []
        for r in (levels or []):
            try:
                lvl = float(r.get("level"))
            except Exception:
                continue

            strength = r.get("strength")
            touches = r.get("touches")

            try:
                strength_f = float(strength) if strength is not None else 0.0
            except Exception:
                strength_f = 0.0
            try:
                touches_i = int(touches) if touches is not None else 0
            except Exception:
                touches_i = 0

            dist_abs = abs(price - lvl)
            dist_atr = dist_abs / float(atr) if atr else None

            if kind == "support":
                side_ok = (lvl <= price)
                stale = (price < (lvl - cross_buf))
            else:
                side_ok = (lvl >= price)
                stale = (price > (lvl + cross_buf))

            rr = dict(r)
            rr["tf"] = tf
            rr["kind"] = kind
            rr["distance"] = float(dist_abs)
            rr["distance_atr"] = float(dist_atr) if dist_atr is not None else None
            rr["side_ok"] = bool(side_ok)
            rr["stale"] = bool(stale)
            rr["strength"] = strength_f
            rr["touches"] = touches_i
            rr["recency_score"] = 0

            base = (
                float(strength_f)
                + (1.0 * float(touches_i))
            )
            side_bonus = 1.5 if side_ok else -2.0
            stale_penalty = -2.0 if stale else 0.0

            dist_penalty = 0.0
            if dist_atr is not None:
                try:
                    da = float(dist_atr)
                    if da > 3.0:
                        dist_penalty = -1.5
                    elif da < 0.2:
                        dist_penalty = -0.5
                except Exception:
                    pass

            rr["sr_score"] = round(base + side_bonus + stale_penalty + dist_penalty, 3)
            xs.append(rr)
        return xs

    def _dedupe_levels(xs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Merge entries at the same price so a level detected by multiple
        sources appears once. Keeps highest score; merges touches/sources."""
        tol = max(0.05 * float(atr), 3.0 * float(pip_factor))
        kept: List[Dict[str, Any]] = []
        for x in sorted(xs, key=lambda r: -float(r.get("sr_score") or 0.0)):
            try:
                lv = float(x.get("level"))
            except Exception:
                continue
            merged = False
            for k in kept:
                try:
                    if abs(float(k.get("level")) - lv) <= tol:
                        k["touches"] = max(int(k.get("touches") or 0), int(x.get("touches") or 0))
                        srcs = k.get("merged_source_types") or [k.get("source_type") or k.get("band_type")]
                        srcs.append(x.get("source_type") or x.get("band_type"))
                        k["merged_source_types"] = [s for s in srcs if s]
                        merged = True
                        break
                except Exception:
                    pass
            if not merged:
                kept.append(x)
        return kept

    def _source_side_ok(x: Dict[str, Any], want_kind: str) -> bool:
        """Reject levels whose ORIGIN contradicts the side they serve.
        A low-cluster/demand origin can't be resistance unless explicitly flipped."""
        st = str(x.get("source_type") or "").upper()
        if bool(x.get("flip_source")):
            return True
        low_origin = ("LOW_CLUSTER" in st) or ("DEMAND" in st) or st.endswith("_LOW") or ("CURRENT_STRUCTURE_LOW" in st)
        high_origin = ("HIGH_CLUSTER" in st) or ("SUPPLY" in st) or st.endswith("_HIGH") or ("CURRENT_STRUCTURE_HIGH" in st)
        if want_kind == "resistance" and low_origin:
            return False
        if want_kind == "support" and high_origin:
            return False
        return True

    def _near(xs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        xs = _dedupe_levels(xs)
        ys = [
            x for x in xs
            if x.get("side_ok")
            and not x.get("stale")
            and _source_side_ok(x, str(x.get("kind") or ""))
            and isinstance(x.get("distance_atr"), (int, float))
            and float(x["distance_atr"]) <= float(NEAR_ATR)
        ]
        ys.sort(key=lambda x: (float(x.get("distance_atr") or 1e9), -float(x.get("sr_score") or 0.0),
                              -float(x.get("strength") or 0.0), -int(x.get("touches") or 0)))
        return ys[:NEAR_TOPK]
    
    def _major(xs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        xs = _dedupe_levels(xs)
        ys = [
            x for x in xs
            if isinstance(x, dict)
            and x.get("side_ok")
            and not x.get("stale")
            and _source_side_ok(x, str(x.get("kind") or ""))
            and isinstance(x.get("distance_atr"), (int, float))
            and float(x.get("distance_atr") or 999) <= 5.0
            and float(x.get("sr_score") or 0) >= 3.0
            and int(x.get("touches") or 0) >= 2
        ]

        ys.sort(
            key=lambda x: (
                -float(x.get("sr_score") or 0.0),
                -float(x.get("strength") or 0.0),
                -int(x.get("touches") or 0),
                float(x.get("distance_atr") or 1e9),
            )
        )
        for x in ys:
            x["major_reason"] = {
                "score": x.get("sr_score"),
                "strength": x.get("strength"),
                "touches": x.get("touches"),
                "distance_atr": x.get("distance_atr"),
            }

        return ys[:MAJOR_TOPK]


    # annotate each TF list
    h4_supp_a = _annotate(h4_sr.get("supports") or [], "support", "H4")
    h4_res_a  = _annotate(h4_sr.get("resistances") or [], "resistance", "H4")
    h1_supp_a = _annotate(h1_sr.get("supports") or [], "support", "H1")
    h1_res_a  = _annotate(h1_sr.get("resistances") or [], "resistance", "H1")
    
    # ------------------------------------------------------------
    # Preserve raw detected SR, then expose price-aware SR lists.
    # Raw supports above price are broken supports, not live supports.
    # ------------------------------------------------------------
    h1_sr["supports_raw_detected"] = h1_sr.get("supports") or []
    h1_sr["resistances_raw_detected"] = h1_sr.get("resistances") or []
    h4_sr["supports_raw_detected"] = h4_sr.get("supports") or []
    h4_sr["resistances_raw_detected"] = h4_sr.get("resistances") or []

    h1_true_supports = [x for x in h1_supp_a if x.get("side_ok") and not x.get("stale")]
    h4_true_supports = [x for x in h4_supp_a if x.get("side_ok") and not x.get("stale")]

    h1_true_resistances = [x for x in h1_res_a if x.get("side_ok") and not x.get("stale")]
    h4_true_resistances = [x for x in h4_res_a if x.get("side_ok") and not x.get("stale")]

    # Replace public supports/resistances with live price-aware roles
    h1_sr["supports"] = h1_true_supports
    h4_sr["supports"] = h4_true_supports
    h1_sr["resistances"] = h1_true_resistances
    h4_sr["resistances"] = h4_true_resistances

    # ------------------------------------------------------------
    # Add nearest current-structure support below price.
    # Purpose:
    #   When all historical supports are above price (broken),
    #   still expose obvious market-structure support visible
    #   on chart from recent lows that produced a strong bounce.
    # ------------------------------------------------------------
    def _inject_current_structure_support(tf_obj, tf_name):
        try:
            supports = tf_obj.get("supports") or []
            if supports:
                return

            raw = tf_obj.get("supports_raw_detected") or []
            if not raw:
                return

            below_price = []

            for r in raw:
                try:
                    hi = float(r.get("high", r.get("level", 0)))
                    lvl = float(r.get("level", 0))
                except Exception:
                    continue

                if hi >= float(price):
                    continue

                below_price.append(r)

            if not below_price:
                return

            below_price.sort(
                key=lambda x: abs(float(price) - float(x.get("level", 0)))
            )

            chosen = below_price[0]

            chosen["source_type"] = (
                str(chosen.get("source_type") or "CURRENT_STRUCTURE_LOW")
            )

            tf_obj["supports"] = [chosen]
            tf_obj["current_structure_supports"] = [chosen]

        except Exception:
            pass

    _inject_current_structure_support(h1_sr, "H1")
    _inject_current_structure_support(h4_sr, "H4")

    def _flip_broken_supports_to_resistance(xs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        out = []
        for x in xs or []:
            if not isinstance(x, dict):
                continue
            try:
                lvl = float(x.get("level"))
            except Exception:
                continue

            # support above current price = broken support, now possible resistance
            if lvl <= float(price):
                continue

            y = dict(x)
            y["kind"] = "resistance"
            y["flip_source"] = "BROKEN_SUPPORT"
            y["was_kind"] = "support"
            y["side_ok"] = True
            y["stale"] = False
            y["distance"] = abs(float(price) - lvl)
            y["distance_atr"] = y["distance"] / float(atr) if atr else None

            # penalty because flipped zones are weaker than clean resistance
            y["sr_score"] = round(float(y.get("sr_score") or 0.0) - 3.0, 3)
            out.append(y)

        out.sort(key=lambda x: (
            float(x.get("distance_atr") or 1e9),
            -float(x.get("sr_score") or 0.0),
            -float(x.get("strength") or 0.0),
        ))
        return out

    # attach near/major views (keep original supports/resistances unchanged)
    # ------------------------------------------------------------
    # Broken support -> flipped resistance
    # ------------------------------------------------------------
    h4_flip_res = _flip_broken_supports_to_resistance(h4_supp_a)
    h1_flip_res = _flip_broken_supports_to_resistance(h1_supp_a)

    # ------------------------------------------------------------
    # attach near/major views (keep original supports/resistances unchanged)
    # ------------------------------------------------------------

    h4_sr["supports_near"] = _near(h4_supp_a)
    h4_sr["supports_major"] = _major(h4_supp_a)

    h4_sr["flipped_resistances"] = h4_flip_res
    h4_sr["resistances_near"] = _near(h4_res_a + h4_flip_res)
    h4_sr["resistances_major"] = _major(h4_res_a + h4_flip_res)

    h1_sr["supports_near"] = _near(h1_supp_a)
    h1_sr["supports_major"] = _major(h1_supp_a)

    h1_sr["flipped_resistances"] = h1_flip_res
    h1_sr["resistances_near"] = _near(h1_res_a + h1_flip_res)
    h1_sr["resistances_major"] = _major(h1_res_a + h1_flip_res)

    # ===== CLEAN SR PIPELINE: inventory -> broken-status -> active pointer =====
    try:
        raw_sup = (h1_sr.get("supports_major") or []) + (h4_sr.get("supports_major") or [])
        raw_res = (h1_sr.get("resistances_major") or []) + (h4_sr.get("resistances_major") or [])
        sup_inv = _consolidate_zones(raw_sup, atr, merge_atr=0.3, min_sep_atr=0.6)
        res_inv = _consolidate_zones(raw_res, atr, merge_atr=0.3, min_sep_atr=0.6)
        _bframe = h1_df if (h1_df is not None and not getattr(h1_df, "empty", True)) else h4_df
        sup_inv = _mark_broken_levels(sup_inv, _bframe, "support", atr, confirm_closes=2)
        res_inv = _mark_broken_levels(res_inv, _bframe, "resistance", atr, confirm_closes=2)
        out["sr_inventory"] = {"supports": sup_inv, "resistances": res_inv}
        out["active_supports"] = _select_active_levels(sup_inv, price, "support", atr, top_n=3)
        out["active_resistances"] = _select_active_levels(res_inv, price, "resistance", atr, top_n=3)
    except Exception:
        out["sr_inventory"] = {"supports": [], "resistances": []}
        out["active_supports"] = []
        out["active_resistances"] = []
    
    # ------------------------------------------------------------
    # SR INVENTORY
    # Purpose:
    #   Find all potential supports/resistances relative to live price.
    #   Zone engine will later choose only when price comes near/touches.
    # ------------------------------------------------------------
    def _sr_inventory_row(x: Dict[str, Any], role: str, source_role: str | None = None) -> Dict[str, Any] | None:
        if not isinstance(x, dict):
            return None
        try:
            lv = float(x.get("level"))
            lo = float(x.get("low", lv))
            hi = float(x.get("high", lv))
        except Exception:
            return None

        dist = abs(float(price) - lv)
        dist_atr = dist / float(atr) if atr else None

        y = dict(x)
        y["role"] = role
        y["source_role"] = source_role or role
        y["distance"] = float(dist)
        y["distance_atr"] = float(dist_atr) if dist_atr is not None else None
        y["is_below_price"] = bool(hi < float(price))
        y["is_above_price"] = bool(lo > float(price))
        return y

    support_inventory = []
    resistance_inventory = []

    # True supports below price
    for x in (h1_supp_a + h4_supp_a):
        try:
            if float(x.get("high", x.get("level"))) < float(price):
                r = _sr_inventory_row(x, "support", "support")
                if r:
                    support_inventory.append(r)
        except Exception:
            pass

    # True resistances above price
    for x in (h1_res_a + h4_res_a):
        try:
            if float(x.get("low", x.get("level"))) > float(price):
                r = _sr_inventory_row(x, "resistance", "resistance")
                if r:
                    resistance_inventory.append(r)
        except Exception:
            pass

    # Broken supports above price become resistance candidates
    for x in (h1_supp_a + h4_supp_a):
        try:
            if float(x.get("low", x.get("level"))) > float(price):
                r = _sr_inventory_row(x, "resistance", "broken_support")
                if r:
                    r["flip_source"] = "BROKEN_SUPPORT"
                    resistance_inventory.append(r)
        except Exception:
            pass

    # Broken resistances below price become support candidates
    for x in (h1_res_a + h4_res_a):
        try:
            if float(x.get("high", x.get("level"))) < float(price):
                r = _sr_inventory_row(x, "support", "broken_resistance")
                if r:
                    r["flip_source"] = "BROKEN_RESISTANCE"
                    support_inventory.append(r)
        except Exception:
            pass

    def _dedupe_inventory(xs: list[dict]) -> list[dict]:
        tol = max(0.05 * float(atr), 3.0 * float(pip_factor))
        kept: list[dict] = []

        for x in sorted(xs, key=lambda r: float(r.get("distance_atr") or 1e9)):
            try:
                lv = float(x.get("level"))
            except Exception:
                continue

            found = None
            for k in kept:
                try:
                    if abs(float(k.get("level")) - lv) <= tol:
                        found = k
                        break
                except Exception:
                    pass

            if found is None:
                kept.append(x)
            else:
                # Keep the stronger / cleaner one, but preserve source info.
                if float(x.get("sr_score") or 0) > float(found.get("sr_score") or 0):
                    old_sources = found.get("merged_sources") or [found.get("source_role")]
                    x["merged_sources"] = old_sources + [x.get("source_role")]
                    kept[kept.index(found)] = x
                else:
                    found.setdefault("merged_sources", []).append(x.get("source_role"))

        return kept

    support_inventory = _dedupe_inventory(support_inventory)
    resistance_inventory = _dedupe_inventory(resistance_inventory)

    support_inventory.sort(key=lambda x: (
        float(x.get("distance_atr") or 1e9),
        -float(x.get("sr_score") or 0.0),
        -float(x.get("strength") or 0.0),
    ))

    resistance_inventory.sort(key=lambda x: (
        float(x.get("distance_atr") or 1e9),
        -float(x.get("sr_score") or 0.0),
        -float(x.get("strength") or 0.0),
    ))

    out["supports_below_price"] = support_inventory[:20]
    out["resistances_above_price"] = resistance_inventory[:20]

    out["h4"] = h4_sr
    out["h1"] = h1_sr

    
    # ---- Nearest computations ----
    # Major-first: prefer strongest (MAJOR) levels (H4 then H1) for nearest_* metrics.
    # If majors are absent, fall back to NEAR, then to annotated ALL.

    # Nearest_* depends on CURRENT price -> never trust cached values
    try:
        out["nearest_support"] = None
        out["nearest_resistance"] = None
        if isinstance(out.get("distance_pips"), dict):
            out["distance_pips"]["support"] = None
            out["distance_pips"]["resistance"] = None
        if isinstance(out.get("distance_atr"), dict):
            out["distance_atr"]["support"] = None
            out["distance_atr"]["resistance"] = None
    except Exception:
        pass

    major_supp = (h4_sr.get("supports_major") or []) + (h1_sr.get("supports_major") or [])
    major_res  = (h4_sr.get("resistances_major") or []) + (h1_sr.get("resistances_major") or [])
    near_supp  = (h4_sr.get("supports_near") or []) + (h1_sr.get("supports_near") or [])
    near_res   = (h4_sr.get("resistances_near") or []) + (h1_sr.get("resistances_near") or [])

    def _valid(xs):
        return [x for x in (xs or []) if isinstance(x, dict) and isinstance(x.get("distance_atr"), (int, float))]
    def _lvl(x):
        try:
            return float(x.get("level"))
        except Exception:
            return None

    # Side-aware buffer for nearest_* (liquidity sweep tolerance)
    try:
        buf = max(float(cross_buf or 0.0), 0.10 * float(atr or 0.0))
    except Exception:
        buf = float(cross_buf or 0.0) if cross_buf is not None else 0.0

    px_for_support = float(price) + float(buf)   # allow support slightly ABOVE price
    px_for_resist  = float(price) - float(buf)   # allow resistance slightly BELOW price

    # For nearest metrics: enforce correct side using level vs price (+ buffer) and not stale.
    supp_candidates = [
        x for x in _valid(major_supp)
        if (not x.get("stale")) and (_lvl(x) is not None) and (_lvl(x) <= px_for_support)
    ]
    res_candidates = [
        x for x in _valid(major_res)
        if (not x.get("stale")) and (_lvl(x) is not None) and (_lvl(x) >= px_for_resist)
    ]

    if not supp_candidates:
        supp_candidates = [
            x for x in _valid(near_supp)
            if (not x.get("stale")) and (_lvl(x) is not None) and (_lvl(x) <= px_for_support)
        ]
    if not res_candidates:
        res_candidates = [
            x for x in _valid(near_res)
            if (not x.get("stale")) and (_lvl(x) is not None) and (_lvl(x) >= px_for_resist)
        ]

    if not supp_candidates:
        supp_candidates = [
            x for x in _valid(h4_supp_a + h1_supp_a)
            if (not x.get("stale")) and (_lvl(x) is not None) and (_lvl(x) <= px_for_support)
        ]
    if not res_candidates:
        res_candidates = [
            x for x in _valid(h4_res_a + h1_res_a)
            if (not x.get("stale")) and (_lvl(x) is not None) and (_lvl(x) >= px_for_resist)
        ]
    # ------------------------------------------------------------
    # Set nearest_* from candidates (major -> near -> annotated ALL)
    # ------------------------------------------------------------
    if supp_candidates:
        try:
            lvl = max([_lvl(x) for x in supp_candidates if _lvl(x) is not None])
            dist = float(price) - float(lvl)
            out["nearest_support"] = float(lvl)
            out["distance_pips"]["support"] = float(dist / pip_factor) if pip_factor else None
            out["distance_atr"]["support"] = float(dist / atr) if atr else None
        except Exception:
            pass

    if res_candidates:
        try:
            lvl = min([_lvl(x) for x in res_candidates if _lvl(x) is not None])
            dist = float(lvl) - float(price)
            out["nearest_resistance"] = float(lvl)
            out["distance_pips"]["resistance"] = float(dist / pip_factor) if pip_factor else None
            out["distance_atr"]["resistance"] = float(dist / atr) if atr else None
        except Exception:
            pass




    
    # Fallback: legacy RAW supports/resistances
    # Use soft-cross buffer so "slightly below support" still counts as interacting.
    if out["nearest_support"] is None or out["nearest_resistance"] is None:
        all_supp = (h4_sr.get("supports") or []) + (h1_sr.get("supports") or [])
        all_res  = (h4_sr.get("resistances") or []) + (h1_sr.get("resistances") or [])

        # soft buffer (in price units) – prefer SR's cross_buf if available
        try:
            buf = max(float(cross_buf or 0.0), 0.10 * float(atr or 0.0))
        except Exception:
            buf = float(cross_buf or 0.0) if cross_buf is not None else 0.0

        px_for_support = float(price) + buf
        px_for_resist  = float(price) - buf

        if all_supp and out["nearest_support"] is None:
            supp_below = []
            for row in all_supp:
                try:
                    lvl = float(row.get("level"))
                    # side-aware with buffer: allow support slightly ABOVE price
                    if lvl <= px_for_support:
                        supp_below.append(lvl)
                except Exception:
                    continue

            if supp_below:
                lvl = max(supp_below)
                dist = float(price) - float(lvl)
                out["nearest_support"] = float(lvl)
                out["distance_pips"]["support"] = float(dist / pip_factor) if pip_factor else None
                out["distance_atr"]["support"] = float(dist / atr) if atr else None

        if all_res and out["nearest_resistance"] is None:
            res_above = []
            for row in all_res:
                try:
                    lvl = float(row.get("level"))
                    # side-aware with buffer: allow resistance slightly BELOW price
                    if lvl >= px_for_resist:
                        res_above.append(lvl)
                except Exception:
                    continue

            if res_above:
                lvl = min(res_above)
                dist = float(lvl) - float(price)
                out["nearest_resistance"] = float(lvl)
                out["distance_pips"]["resistance"] = float(dist / pip_factor) if pip_factor else None
                out["distance_atr"]["resistance"] = float(dist / atr) if atr else None


    # safety classification (use closest side)
    distances_atr: List[float] = []
    for side in ("support", "resistance"):
        da = out["distance_atr"].get(side)
        if isinstance(da, (int, float)):
            distances_atr.append(float(da))

    if not distances_atr:
        out["sr_safety"] = "unknown"
    else:
        dmin = min(distances_atr)
        if dmin < 0.25:
            out["sr_safety"] = "danger"
        elif dmin < 0.5:
            out["sr_safety"] = "tight"
        else:
            out["sr_safety"] = "safe"

    # ------------------------------------------------------------
    # DEBUG: supports below current price
    # ------------------------------------------------------------
    try:
        out["debug_support_below_price"] = [
            {
                "tf": x.get("tf"),
                "level": x.get("level"),
                "kind": x.get("kind"),
                "strength": x.get("strength"),
                "touches": x.get("touches"),
                "score": x.get("sr_score"),
                "side_ok": x.get("side_ok"),
                "stale": x.get("stale"),
            }
            for x in (h1_supp_a + h4_supp_a)
            if float(x.get("level") or 0) < float(price)
        ][:50]

        out["debug_support_above_price"] = [
            {
                "tf": x.get("tf"),
                "level": x.get("level"),
                "kind": x.get("kind"),
                "strength": x.get("strength"),
                "touches": x.get("touches"),
                "score": x.get("sr_score"),
                "side_ok": x.get("side_ok"),
                "stale": x.get("stale"),
            }
            for x in (h1_supp_a + h4_supp_a)
            if float(x.get("level") or 0) >= float(price)
        ][:50]

    except Exception:
        out["debug_support_below_price"] = []
        out["debug_support_above_price"] = []

    # -------------------------------
    # Cache policy (ONLY at end)
    # -------------------------------
    try:
        # MEMORY FIX (final, pre-cache): slim the bundle right before it's written
        # to Redis. Heavy raw/intermediate lists (500-600+ items, mostly RAW_SWING)
        # bloated it to ~1.1MB, parsed per-request -> memory thrash. Strip the
        # unused ones; cap the gate-fallback lists to top-30 by score.
        def _cap_by_score(xs, n=30):
            if not isinstance(xs, list) or len(xs) <= n:
                return xs
            try:
                return sorted(xs, key=lambda z: -float((z or {}).get("sr_score") or (z or {}).get("strength") or 0))[:n]
            except Exception:
                return xs[:n]
        _DROP = ("supports_raw_detected", "resistances_raw_detected",
                 "flipped_resistances", "flipped_supports",
                 "supports_top", "resistances_top")
        for _tfk in ("h1", "h4"):
            _tfo = out.get(_tfk)
            if isinstance(_tfo, dict):
                for _dk in _DROP:
                    _tfo.pop(_dk, None)
                if isinstance(_tfo.get("resistances"), list):
                    _tfo["resistances"] = _cap_by_score(_tfo["resistances"], 30)
                if isinstance(_tfo.get("supports"), list):
                    _tfo["supports"] = _cap_by_score(_tfo["supports"], 30)

        
        # Always store LAST (short TTL)
        out["_computed_at_ms"] = int(time.time() * 1000)

        # Always store LAST (short TTL)
        try:
            out["_dbg_persist_key"] = k_last
            out["_dbg_cache_type"] = type(cache).__name__
        except Exception:
            pass

        _cache_set_json(cache, k_last, cache_ttl_sec, out)

        # Store LAST_GOOD only if SR has any levels
        if _sr_has_any_levels(out):
            _cache_set_json(cache, k_good, good_ttl_sec, out)
        else:
            cached = _cache_get_json(cache, k_good)
            if isinstance(cached, dict):
                cached["_cache"] = "last_good_empty_compute"
                return cached
    except Exception:
        pass

    return out
