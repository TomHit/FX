# trend_sr.py (or top of trend_endpoints.py)
from __future__ import annotations
from typing import Any, Dict, List, Literal, Optional, Tuple
import math
import numpy as np
import pandas as pd


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
    cache=None,
    cache_ttl_sec: int = 300,
    good_ttl_sec: int = 7*24*3600,
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
        "h4": [],
        "h1": [],
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
    MAJOR_TOPK = 3      # show top K strongest levels per side

    # hysteresis buffer: avoid flip-flop around SR
    cross_buf = max(0.05 * float(atr), 3.0 * float(pip_factor))

    # expose for consumers (UI/entry)
    out["atr"] = float(atr)
    out["cross_buf"] = float(cross_buf)
    out["pip_factor"] = float(pip_factor)

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

            base = float(strength_f) + (0.5 * float(touches_i))
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

    def _near(xs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        ys = [
            x for x in xs
            if x.get("side_ok")
            and not x.get("stale")
            and isinstance(x.get("distance_atr"), (int, float))
            and float(x["distance_atr"]) <= float(NEAR_ATR)
        ]
        ys.sort(key=lambda x: (float(x.get("distance_atr") or 1e9), -float(x.get("sr_score") or 0.0),
                              -float(x.get("strength") or 0.0), -int(x.get("touches") or 0)))
        return ys[:NEAR_TOPK]

    def _major(xs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        ys = [x for x in xs if x.get("side_ok")]
        ys.sort(key=lambda x: (-float(x.get("sr_score") or 0.0), -float(x.get("strength") or 0.0),
                               -int(x.get("touches") or 0), float(x.get("distance_atr") or 1e9)))
        return ys[:MAJOR_TOPK]

    # annotate each TF list
    h4_supp_a = _annotate(h4_sr.get("supports") or [], "support", "H4")
    h4_res_a  = _annotate(h4_sr.get("resistances") or [], "resistance", "H4")
    h1_supp_a = _annotate(h1_sr.get("supports") or [], "support", "H1")
    h1_res_a  = _annotate(h1_sr.get("resistances") or [], "resistance", "H1")

    # attach near/major views (keep original supports/resistances unchanged)
    h4_sr["supports_near"] = _near(h4_supp_a)
    h4_sr["resistances_near"] = _near(h4_res_a)
    h4_sr["supports_major"] = _major(h4_supp_a)
    h4_sr["resistances_major"] = _major(h4_res_a)

    h1_sr["supports_near"] = _near(h1_supp_a)
    h1_sr["resistances_near"] = _near(h1_res_a)
    h1_sr["supports_major"] = _major(h1_supp_a)
    h1_sr["resistances_major"] = _major(h1_res_a)

    out["h4"] = h4_sr
    out["h1"] = h1_sr

    # ---- Nearest computations ----
    supp_candidates = [x for x in (h4_supp_a + h1_supp_a) if x.get("side_ok") and not x.get("stale") and isinstance(x.get("distance_atr"), (int, float))]
    res_candidates  = [x for x in (h4_res_a  + h1_res_a)  if x.get("side_ok") and not x.get("stale") and isinstance(x.get("distance_atr"), (int, float))]

    if supp_candidates:
        ns = min(supp_candidates, key=lambda r: float(r.get("distance_atr") or 1e9))
        lvl = float(ns.get("level"))
        dist = price - lvl
        out["nearest_support"] = lvl
        out["distance_pips"]["support"] = float(dist / pip_factor)
        out["distance_atr"]["support"] = float(dist / atr)

    if res_candidates:
        nr = min(res_candidates, key=lambda r: float(r.get("distance_atr") or 1e9))
        lvl = float(nr.get("level"))
        dist = lvl - price
        out["nearest_resistance"] = lvl
        out["distance_pips"]["resistance"] = float(dist / pip_factor)
        out["distance_atr"]["resistance"] = float(dist / atr)

    # Fallback: legacy RAW supports/resistances
    if out["nearest_support"] is None or out["nearest_resistance"] is None:
        all_supp = (h4_sr.get("supports") or []) + (h1_sr.get("supports") or [])
        all_res  = (h4_sr.get("resistances") or []) + (h1_sr.get("resistances") or [])

        if all_supp and out["nearest_support"] is None:
            supp_below = []
            for row in all_supp:
                try:
                    if float(row.get("level")) <= price:
                        supp_below.append(row)
                except Exception:
                    continue
            if supp_below:
                ns = max(supp_below, key=lambda r: float(r.get("level")))
                lvl = float(ns.get("level"))
                dist = price - lvl
                out["nearest_support"] = lvl
                out["distance_pips"]["support"] = float(dist / pip_factor)
                out["distance_atr"]["support"] = float(dist / atr)

        if all_res and out["nearest_resistance"] is None:
            res_above = []
            for row in all_res:
                try:
                    if float(row.get("level")) >= price:
                        res_above.append(row)
                except Exception:
                    continue
            if res_above:
                nr = min(res_above, key=lambda r: float(r.get("level")))
                lvl = float(nr.get("level"))
                dist = lvl - price
                out["nearest_resistance"] = lvl
                out["distance_pips"]["resistance"] = float(dist / pip_factor)
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
        if dmin < 0.25:
            out["sr_safety"] = "danger"
        elif dmin < 0.5:
            out["sr_safety"] = "tight"
        else:
            out["sr_safety"] = "safe"

    # -------------------------------
    # Cache policy (ONLY at end)
    # -------------------------------
    try:
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
