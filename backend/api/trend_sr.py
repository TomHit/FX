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

    for lvl in levels_sorted[1:]:
        if abs(lvl - current[-1]) <= tol:
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


def compute_sr_for_frame(
    df: pd.DataFrame,
    tf_label: Literal["H1", "H4"],
    pip_factor: float,
    max_levels_per_side: int = 10,
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
        # Active MAJOR levels only:
        # support must be below/near current price
        # resistance must be above/near current price
        # stale/broken levels are excluded from active dashboard/gate display.
        ys = [
            x for x in xs
            if isinstance(x, dict)
            and x.get("side_ok")
            and not x.get("stale")
        ]

        ys.sort(
            key=lambda x: (
               -float(x.get("sr_score") or 0.0),
               -float(x.get("strength") or 0.0),
               -int(x.get("touches") or 0),
               float(x.get("distance_atr") or 1e9),
            )
        )
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
