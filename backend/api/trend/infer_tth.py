import os
import json
import time
from typing import Any, Dict, List, Tuple, Optional

import numpy as np

try:
    from xgboost import XGBClassifier
except Exception:
    XGBClassifier = None  # type: ignore

try:
    # We are inside api/trend/, so relative import works
    from .trend_endpoints_helpers import _read_freshest_snap_for_user_or_any  # optional
except Exception:
    _read_freshest_snap_for_user_or_any = None  # type: ignore


# ---------------------------------------------------------------------
# Paths (matches your screenshot)
# ---------------------------------------------------------------------
MODEL_DIR = "/opt/xauapi/api/trend/out/tth_models"
HIT_MODEL_JSON = os.path.join(MODEL_DIR, "hit_cls.json")
TBUCKET_MODEL_JSON = os.path.join(MODEL_DIR, "t_bucket_cls.json")
META_JSON = os.path.join(MODEL_DIR, "meta.json")

# ---------------------------------------------------------------------
# Globals (lazy loaded)
# ---------------------------------------------------------------------
_MODELS_LOADED = False
_HIT_CLS: Any = None
_T_BUCKET_CLS: Any = None

_META: Dict[str, Any] = {}
_FEATURES: List[str] = []
_IDX_TO_BUCKET: Dict[int, int] = {}


def _load_meta() -> Dict[str, Any]:
    if not os.path.exists(META_JSON):
        return {}
    try:
        with open(META_JSON, "r", encoding="utf-8") as f:
            return json.load(f) or {}
    except Exception:
        return {}


def _ensure_models_loaded() -> None:
    global _MODELS_LOADED, _HIT_CLS, _T_BUCKET_CLS, _META, _FEATURES, _IDX_TO_BUCKET

    if _MODELS_LOADED:
        return

    if XGBClassifier is None:
        raise RuntimeError("xgboost is not available")

    if not os.path.exists(HIT_MODEL_JSON):
        raise FileNotFoundError(f"Missing hit model: {HIT_MODEL_JSON}")
    if not os.path.exists(TBUCKET_MODEL_JSON):
        raise FileNotFoundError(f"Missing bucket model: {TBUCKET_MODEL_JSON}")

    _META = _load_meta()

    feats = _META.get("feat_cols") or _META.get("features") or _META.get("FEATURES")
    if not isinstance(feats, list) or not all(isinstance(x, str) for x in feats):
        raise RuntimeError(f"meta.json missing feat_cols/features. meta keys={list(_META.keys())}")

    _FEATURES = [x.strip() for x in feats if x and x.strip()]
    if len(_FEATURES) != 5:
        raise RuntimeError(f"Expected 5 features, got {len(_FEATURES)}: {_FEATURES}")

    idx_to_bucket = _META.get("idx_to_bucket") or {}
    out_map: Dict[int, int] = {}
    if isinstance(idx_to_bucket, dict):
        for k, v in idx_to_bucket.items():
            try:
                out_map[int(k)] = int(v)
            except Exception:
                pass
    if not out_map:
        # fallback: common mapping
        out_map = {0: 0, 1: 15, 2: 30, 3: 45, 4: 60, 5: 90, 6: 120, 7: 180, 8: 240, 9: 360, 10: 480}
    _IDX_TO_BUCKET = out_map

    hit = XGBClassifier()
    hit.load_model(HIT_MODEL_JSON)

    tb = XGBClassifier()
    tb.load_model(TBUCKET_MODEL_JSON)

    _HIT_CLS = hit
    _T_BUCKET_CLS = tb
    _MODELS_LOADED = True


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def _ms_from_bar(b: Dict[str, Any]) -> Optional[int]:
    t = b.get("t_open_ms") or b.get("t") or b.get("time") or b.get("ts")
    if t is None:
        return None
    try:
        ti = int(t)
        # seconds ? ms
        if ti < 10_000_000_000:
            ti *= 1000
        return ti
    except Exception:
        return None


def _get_last_closed_m15_bars(symbol: str, user_id: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Pulls latest M15 bars from the backend snapshot store.
    This relies on your existing snapshot mechanism.
    """
    if _read_freshest_snap_for_user_or_any is None:
        return []

    try:
        snap, _broker = _read_freshest_snap_for_user_or_any(user_id, symbol, "M15")
    except Exception:
        snap = None

    bars = (snap or {}).get("bars") or []
    if not isinstance(bars, list):
        return []
    return [b for b in bars if isinstance(b, dict)]


def _compute_atr14_pct(bars: List[Dict[str, Any]]) -> Optional[float]:
    """
    ATR14% = (ATR14 / last_close) * 100
    ATR computed using True Range over last 14 periods.
    """
    if len(bars) < 20:
        return None

    # build arrays of h,l,c in correct order
    hs, ls, cs = [], [], []
    for b in bars:
        h = b.get("h")
        l = b.get("l")
        c = b.get("c")
        if isinstance(h, (int, float)) and isinstance(l, (int, float)) and isinstance(c, (int, float)):
            hs.append(float(h))
            ls.append(float(l))
            cs.append(float(c))

    if len(cs) < 20:
        return None

    # use last ~50 bars max for safety
    hs = hs[-60:]
    ls = ls[-60:]
    cs = cs[-60:]

    trs = []
    prev_close = cs[0]
    for i in range(1, len(cs)):
        tr = max(
            hs[i] - ls[i],
            abs(hs[i] - prev_close),
            abs(ls[i] - prev_close),
        )
        trs.append(tr)
        prev_close = cs[i]

    if len(trs) < 14:
        return None

    atr14 = float(np.mean(trs[-14:]))
    last_close = cs[-1]
    if last_close <= 0:
        return None

    return (atr14 / last_close) * 100.0


def _tod_min_and_dow_from_last_bar(bars: List[Dict[str, Any]]) -> Tuple[Optional[int], Optional[int]]:
    """
    tod_min = minute-of-day (0..1439)
    dow     = day-of-week (0=Mon..6=Sun)
    Derived from last bar timestamp (assumes ms epoch).
    """
    if not bars:
        return None, None

    # pick last bar that has timestamp
    tms = None
    for b in reversed(bars):
        tms = _ms_from_bar(b)
        if tms:
            break
    if not tms:
        return None, None

    # convert using server timezone UTC (ok as relative encoding); if you want broker tz,
    # you can adjust here later using your broker offset.
    import datetime as _dt
    dt = _dt.datetime.utcfromtimestamp(tms / 1000.0)

    tod_min = dt.hour * 60 + dt.minute
    dow = dt.weekday()
    return tod_min, dow


def _vector_from_features(atr14_pct: float, k: float, barrier_pct: float, tod_min: int, dow: int) -> np.ndarray:
    """
    X must be (1, 5) in exact training feature order.
    """
    vals_by_name = {
        "atr14_pct": atr14_pct,
        "k": k,
        "barrier_pct": barrier_pct,
        "tod_min": float(tod_min),
        "dow": float(dow),
    }
    X = np.array([[float(vals_by_name[n]) for n in _FEATURES]], dtype=float)
    return X


def predict_tth(symbol: str, user_id: Optional[str] = None, k: float = 1.0) -> Dict[str, Any]:
    """
    Predict dynamic time-to-hit (TTH).

    Returns:
      {
        ok, symbol,
        p_hit, p_no_hit,
        p_up, p_down,
        bucket_idx, horizon_min,
        features: {...}
      }
    """
    sym = (symbol or "").upper()

    try:
        _ensure_models_loaded()
    except Exception as e:
        return {"ok": False, "symbol": sym, "reason": "tth_load_failed", "detail": str(e)}

    try:
        bars = _get_last_closed_m15_bars(sym, user_id=user_id)
        atr14_pct = _compute_atr14_pct(bars)
        tod_min, dow = _tod_min_and_dow_from_last_bar(bars)

        # Safe fallbacks if data missing
        if atr14_pct is None:
            atr14_pct = 0.10  # conservative default (0.10% ATR on FX)
        if tod_min is None:
            tod_min = 0
        if dow is None:
            dow = 0

        k_list = _META.get("k_list") or [0.5, 1.0, 1.5, 2.0]
        if k not in k_list:
            k = 1.0

        # barrier_pct is the target barrier distance in percent terms
        barrier_pct = float(k) * float(atr14_pct)

        X = _vector_from_features(float(atr14_pct), float(k), float(barrier_pct), int(tod_min), int(dow))

        expected = getattr(_HIT_CLS, "n_features_in_", None)
        if isinstance(expected, int) and X.shape[1] != expected:
            return {
                "ok": False,
                "symbol": sym,
                "reason": "feature_shape_mismatch",
                "expected": expected,
                "got": int(X.shape[1]),
                "features_order": list(_FEATURES),
            }

        # hit prob
        p = _HIT_CLS.predict_proba(X)[0]
        p_no = float(p[0]) if len(p) > 0 else 0.0
        p_hit = float(p[1]) if len(p) > 1 else 0.0

        # bucket
        pb = _T_BUCKET_CLS.predict_proba(X)[0]
        idx = int(np.argmax(pb)) if len(pb) else 0
        horizon_min = int(_IDX_TO_BUCKET.get(idx, 60))

        # Direction in TTH is not UP/DOWN specific; we expose:
        p_up = p_hit
        p_down = max(0.0, 1.0 - p_hit)

        return {
            "ok": True,
            "symbol": sym,
            "p_hit": round(p_hit, 4),
            "p_no_hit": round(p_no, 4),
            "p_up": round(p_up, 4),
            "p_down": round(p_down, 4),
            "bucket_idx": idx,
            "horizon_min": horizon_min,
            "features": {
                "atr14_pct": round(float(atr14_pct), 6),
                "k": float(k),
                "barrier_pct": round(float(barrier_pct), 6),
                "tod_min": int(tod_min),
                "dow": int(dow),
            },
            "server_ms": int(time.time() * 1000),
        }

    except Exception as e:
        return {"ok": False, "symbol": sym, "reason": "tth_infer_exception", "detail": str(e)}
