
# -*- coding: utf-8 -*-
from __future__ import annotations
import json, pathlib, numpy as np, pandas as pd
from typing import Optional, Dict, Any, List
import redis

from api.utils.redis_client import get_client

BASE = pathlib.Path("/opt/xauapi/api/trend")
MODEL_DIR = BASE / "models"
CLS_PATH = MODEL_DIR / "xgb_cls.json"
REG_PATH = MODEL_DIR / "xgb_reg.json"

CALIB_PATH = MODEL_DIR / "calib.json"
_CALIB = None


TF_MS = {"M1":60_000, "M5":300_000, "M15":900_000, "H1":3_600_000, "H4":14_400_000}
FEATURE_COLS = ["atr14_m15_pct","rvol15","ret_15m","usd_basket_d1h_pct","tod_min","dow"]

R = get_client()
_XGB = None   # (cls, reg) cache

def _load_models():
    """Load xgboost lazily so app boot doesn't require it."""
    global _XGB
    if _XGB is not None:
        return _XGB
    try:
        import xgboost as xgb
    except Exception as e:
        raise RuntimeError("xgboost not installed in the server Python") from e
    cls = xgb.XGBClassifier()
    reg = xgb.XGBRegressor()
    cls.load_model(str(CLS_PATH))
    reg.load_model(str(REG_PATH))
    _XGB = (xgb, cls, reg)
    return _XGB


def _load_calib():
    global _CALIB
    if _CALIB is not None:
        return _CALIB
    try:
        _CALIB = json.loads(CALIB_PATH.read_text())
    except Exception:
        _CALIB = {"global_scale": 1.0, "per_symbol": {}, "clip_pct": {"majors":1.5,"XAUUSD":2.5}, "abstain":{"p_up_margin":0.10,"min_pct":0.03}}
    return _CALIB

def _norm_tf(s: str) -> str:
    s = (s or "").upper().replace("MIN","M").replace("15M","M15").replace("60M","H1")
    if s in TF_MS: return s
    if s in ("15","15MIN","15M","M15"): return "M15"
    if s in ("1","1M","M1"): return "M1"
    if s in ("5","5M","M5"): return "M5"
    if s in ("H","1H","H1"): return "H1"
    if s in ("4H","H4"): return "H4"
    return "M15"

def _scan_keys(symbol: str) -> List[str]:
    pats = [f"xtl:ohlc:snap:*:{symbol}:*", f"xtl:trend:snap:*:{symbol}:*"]
    keys: List[str] = []
    cur = 0
    for pat in pats:
        cur = 0
        while True:
            cur, batch = R.scan(cursor=cur, match=pat, count=200)
            for k in batch:
                ks = k.decode() if isinstance(k, (bytes,bytearray)) else k
                keys.append(ks)
            if cur == 0: break
    keys = sorted(set(keys), key=lambda x: (0 if ":ohlc:" in x else 1, x))
    return keys

def _parse(val: str):
    try:
        d = json.loads(val)
    except Exception:
        return None, None
    bars = d.get("bars") or []
    if not bars: return None, None
    import pandas as pd
    df = pd.DataFrame(bars)
    if "ts_ms" not in df.columns and "t" in df.columns:
        df = df.rename(columns={"t":"ts_ms"})
    if "open" not in df.columns and "o" in df.columns:
        df = df.rename(columns={"o":"open","h":"high","l":"low","c":"close","v":"volume"})
    if "ts_ms" not in df.columns: return None, None
    if (df["ts_ms"] < 2_000_000_000_000).any():
        df["ts_ms"] = df["ts_ms"].astype("int64") * 1000
    keep = [c for c in ["ts_ms","open","high","low","close","volume"] if c in df.columns]
    df = df[keep].dropna().drop_duplicates().sort_values("ts_ms")
    tf = d.get("tf") or d.get("timeframe") or d.get("TF")
    tf = _norm_tf(str(tf)) if tf else None
    return df, tf

def _resample_m15(df_in, tf):
    if tf == "M15": return df_in.copy()
    import pandas as pd, numpy as np
    df = df_in.copy()
    dt = pd.to_datetime(df["ts_ms"], unit="ms", utc=True)
    df = df.assign(dt=dt).set_index("dt")
    rule = "15min"
    o = df["open"].resample(rule).first()
    h = df["high"].resample(rule).max()
    l = df["low"].resample(rule).min()
    c = df["close"].resample(rule).last()
    v = df["volume"].resample(rule).sum()
    out = pd.DataFrame({"open":o,"high":h,"low":l,"close":c,"volume":v}).dropna(how="any")
    out = out.reset_index()
    out["ts_ms"] = (out["dt"].astype("int64") // 1_000_000).astype("int64")
    return out[["ts_ms","open","high","low","close","volume"]]

def pull_latest_m15(symbol: str, need_rows: int = 60):
    import pandas as pd
    keys = _scan_keys(symbol)
    frames = []
    for k in keys:
        raw = R.get(k)
        if not raw: continue
        raw = raw.decode("utf-8","ignore") if isinstance(raw, (bytes,bytearray)) else raw
        df, tf = _parse(raw)
        if df is None or df.empty: continue
        tf = tf or _norm_tf(k.split(":")[-1])
        frames.append(_resample_m15(df, tf))
    if not frames: 
        return pd.DataFrame(columns=["ts_ms","open","high","low","close","volume"])
    out = pd.concat(frames, ignore_index=True)
    out = out.sort_values("ts_ms").drop_duplicates(subset=["ts_ms"]).reset_index(drop=True)
    return out.tail(max(need_rows, 8))

def _atr14(close, high, low):
    import numpy as np, pandas as pd
    c = close.to_numpy(); h = high.to_numpy(); l = low.to_numpy()
    if len(c) == 0: return np.array([])
    prev_c = np.concatenate([[c[0]], c[:-1]])
    tr = np.maximum(h - l, np.maximum(np.abs(h - prev_c), np.abs(l - prev_c)))
    return pd.Series(tr).ewm(alpha=1/14, adjust=False).mean().to_numpy()

def _rvol15(df):
    import pandas as pd
    dt = pd.to_datetime(df["ts_ms"], unit="ms", utc=True)
    tmp = df.copy()
    tmp["dt"] = dt
    tmp["tod_min"] = tmp["dt"].dt.hour * 60 + tmp["dt"].dt.minute
    tmp["date"] = tmp["dt"].dt.date
    last = tmp.iloc[-1]
    base = tmp[tmp["date"] < last["date"]].groupby("tod_min")["volume"].mean()
    base_mean = float(base.mean()) if len(base) else 0.0
    baseline = float(base.reindex([int(last["tod_min"])]).fillna(base_mean).iloc[0] if len(base) else 0.0)
    curr = float(last["volume"] or 0.0)
    denom = baseline if baseline > 0 else 1.0
    return curr / denom

def build_features_m15(df_m15, usd_basket):
    import numpy as np, pandas as pd
    out = df_m15.copy()
    out["ret_15m"] = out["close"].pct_change().fillna(0.0) * 100.0
    atr = _atr14(out["close"], out["high"], out["low"])
    out["atr14_m15_pct"] = (pd.Series(atr) / out["close"]).fillna(0.0) * 100.0
    out["rvol15"] = 0.0
    if len(out) >= 5:
        out.loc[out.index[-1], "rvol15"] = _rvol15(out)
    dt = pd.to_datetime(out["ts_ms"], unit="ms", utc=True)
    out["tod_min"] = dt.dt.hour * 60 + dt.dt.minute
    out["dow"] = dt.dt.dayofweek + 1
    out["usd_basket_d1h_pct"] = np.nan
    if usd_basket is not None and not usd_basket.empty:
        out = out.merge(usd_basket, on="ts_ms", how="left")
    return out.tail(1)[FEATURE_COLS].replace([np.inf, -np.inf], 0).fillna(0.0)

def compute_usd_basket(now_frames: Dict[str, 'pd.DataFrame']):
    import pandas as pd
    signs = {"EURUSD":"-", "GBPUSD":"-", "AUDUSD":"-", "USDJPY":"+", "USDCHF":"+", "USDCAD":"+"}
    pieces = []
    for sym, sign in signs.items():
        f = now_frames.get(sym)
        if f is None or f.empty: continue
        g = f[["ts_ms","close"]].copy()
        g["ret_15m"] = g["close"].pct_change().fillna(0.0) * 100.0
        g = g[["ts_ms","ret_15m"]].rename(columns={"ret_15m": f"r_{sym}"})
        pieces.append(g)
    if not pieces: return None
    m = pieces[0]
    for p in pieces[1:]:
        m = m.merge(p, on="ts_ms", how="outer")
    m = m.sort_values("ts_ms").ffill()
    cols = [c for c in m.columns if c.startswith("r_")]
    for c in cols:
        if c.endswith(("EURUSD","GBPUSD","AUDUSD")):
            m[c] = -m[c]
    m["usd_basket_d1h_pct"] = m[cols].mean(axis=1).rolling(4, min_periods=1).sum()
    return m[["ts_ms","usd_basket_d1h_pct"]]

def predict_next_hour(symbol: str) -> Dict[str, Any]:
    try:
        xgb, cls, reg = _load_models()
    except Exception as e:
        return {"ok": False, "reason": "ml_import_error", "detail": str(e)}

    need_syms = ["XAUUSD","EURUSD","GBPUSD","AUDUSD","USDJPY","USDCHF","USDCAD"]
    now_frames = {s: pull_latest_m15(s) for s in need_syms}
    xau = now_frames.get(symbol) or __import__("pandas").DataFrame()
    if xau.empty or len(xau) < 8:
        return {"ok": False, "reason": "insufficient_data", "rows": int(len(xau))}

    basket = compute_usd_basket(now_frames)
    X = build_features_m15(xau, basket).astype("float32")

    prob_up = float(cls.predict_proba(X)[:,1][0])
    move_pct = float(reg.predict(X)[0])
    last_close = float(xau["close"].iloc[-1])
    # --- apply calibration & clipping ---
    cal = _load_calib()
    scale = float(cal.get("per_symbol", {}).get(symbol, cal.get("global_scale", 1.0)))
    move_pct *= scale

    # clip to sane 1h bounds (majors vs XAU)
    cap = float(cal.get("clip_pct", {}).get("XAUUSD" if symbol=="XAUUSD" else "majors", 1.5))
    move_pct = float(np.clip(move_pct, -cap, +cap))

    # (optional abstain gating for consumers; keeping raw fields here)

    target_price = last_close * (1.0 + move_pct/100.0)

    return {
        "ok": True,
        "symbol": symbol,
        "lastTs": int(xau["ts_ms"].iloc[-1]),
        "lastClose": last_close,
        "probUp": prob_up,
        "predMovePct": move_pct,
        "targetPrice": target_price,
        "features_used": FEATURE_COLS,
    }

