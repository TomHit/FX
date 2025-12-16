# trend_sr.py (or top of trend_endpoints.py)
from __future__ import annotations
from typing import Any, Dict, List, Literal, Optional, Tuple
import math
import numpy as np
import pandas as pd


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


def _cluster_levels(
    levels: List[float],
    tol: float,
) -> List[Dict[str, Any]]:
    """
    Cluster raw levels that are within 'tol' of each other.
    Returns clusters with mean level, touch count, and strength.
    """
    if not levels:
        return []

    levels_sorted = sorted(levels)
    clusters: List[List[float]] = []
    current: List[float] = [levels_sorted[0]]

    for lvl in levels_sorted[1:]:
        if abs(lvl - current[-1]) <= tol:
            current.append(lvl)
        else:
            clusters.append(current)
            current = [lvl]
    if current:
        clusters.append(current)

    out: List[Dict[str, Any]] = []
    for cluster in clusters:
        lvl = float(np.mean(cluster))
        touches = len(cluster)
        out.append(
            {
                "level": lvl,
                "touches": touches,
                "strength": touches,  # can weight by timeframe later
            }
        )
    # strongest first
    out.sort(key=lambda x: (-x["strength"], x["level"]))
    return out


def compute_sr_for_frame(
    df: pd.DataFrame,
    tf_label: Literal["H1", "H4"],
    pip_factor: float,
    max_levels_per_side: int = 5,
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
    tol = 0.25 * atr

    swing_highs, swing_lows = _find_swings(df, lookback=2)

    res_clusters = _cluster_levels(swing_highs, tol=tol)
    sup_clusters = _cluster_levels(swing_lows, tol=tol)

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
        "supports": sup_clusters[:max_levels_per_side],
        "resistances": res_clusters[:max_levels_per_side],
    }


def summarize_sr_multi_tf(
    symbol: str,
    price: float,
    h4_df: Optional[pd.DataFrame],
    h1_df: Optional[pd.DataFrame],
    pip_factor: float,
) -> Dict[str, Any]:
    """
    Combine H4 + H1 SR into a single summary for a symbol.
    Returns:
      {
        "symbol": "...",
        "h4": [...],
        "h1": [...],
        "nearest_support": float | None,
        "nearest_resistance": float | None,
        "distance_pips": {"support": float | None, "resistance": float | None},
        "distance_atr": {"support": float | None, "resistance": float | None},
        "sr_safety": "safe" | "tight" | "danger" | "unknown"
      }
    """
    out: Dict[str, Any] = {
        "symbol": symbol,
        "h4": [],
        "h1": [],
        "nearest_support": None,
        "nearest_resistance": None,
        "distance_pips": {"support": None, "resistance": None},
        "distance_atr": {"support": None, "resistance": None},
        "sr_safety": "unknown",
    }

    if price is None or price <= 0:
        return out

    # per-TF SR
    h4_sr = compute_sr_for_frame(h4_df, "H4", pip_factor) if h4_df is not None else {"supports": [], "resistances": []}
    h1_sr = compute_sr_for_frame(h1_df, "H1", pip_factor) if h1_df is not None else {"supports": [], "resistances": []}

    out["h4"] = h4_sr
    out["h1"] = h1_sr

    # flatten all levels
    all_supp = (h4_sr["supports"] or []) + (h1_sr["supports"] or [])
    all_res = (h4_sr["resistances"] or []) + (h1_sr["resistances"] or [])

    if not all_supp and not all_res:
        return out

    # ATR for distance normalization: prefer H1 if available
    atr_frame = h1_df if h1_df is not None and not h1_df.empty else h4_df
    atr = _atr14(atr_frame) if atr_frame is not None else 0.0
    if atr <= 0:
        atr = 1.0  # fallback just to avoid division by zero

    # nearest support: max level < price
    supp_below = [row for row in all_supp if row["level"] <= price]
    if supp_below:
        ns = max(supp_below, key=lambda r: r["level"])
        dist = price - ns["level"]
        dist_pips = dist / pip_factor
        out["nearest_support"] = ns["level"]
        out["distance_pips"]["support"] = float(dist_pips)
        out["distance_atr"]["support"] = float(dist / atr)

    # nearest resistance: min level > price
    res_above = [row for row in all_res if row["level"] >= price]
    if res_above:
        nr = min(res_above, key=lambda r: r["level"])
        dist = nr["level"] - price
        dist_pips = dist / pip_factor
        out["nearest_resistance"] = nr["level"]
        out["distance_pips"]["resistance"] = float(dist_pips)
        out["distance_atr"]["resistance"] = float(dist / atr)

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
        if dmin < 0.25:        # <0.25 ATR ? dangerous
            out["sr_safety"] = "danger"
        elif dmin < 0.5:       # <0.5 ATR ? tight
            out["sr_safety"] = "tight"
        else:                  # >=0.5 ATR ? safe
            out["sr_safety"] = "safe"

    return out
