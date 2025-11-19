
# -*- coding: utf-8 -*-
import os, json, pathlib, numpy as np, pandas as pd

from api.utils.redis_client import get_client
R = get_client()

BASE = pathlib.Path("/opt/xauapi/api/trend")
OUT  = BASE / "out"
OUT.mkdir(parents=True, exist_ok=True)

from api.utils.config_loader import FEATURES
SYMS = FEATURES["symbols"]

TF_MS = {
    "M1":  60_000,
    "M5":  300_000,
    "M15": 900_000,
    "H1":  3_600_000,
    "H4":  14_400_000,
}

def _norm_tf(s: str) -> str:
    s = (s or "").upper().replace("MIN","M").replace("15M","M15").replace("60M","H1")
    if s in TF_MS: return s
    if s in ("15","15MIN","15M","M15"): return "M15"
    if s in ("1","1M","M1"): return "M1"
    if s in ("5","5M","M5"): return "M5"
    if s in ("H","1H","H1"): return "H1"
    if s in ("4H","H4"): return "H4"
    return "M15"

def scan_snap_keys(symbol):
    pats = [
        f"xtl:ohlc:snap:*:{symbol}:*",
        f"xtl:trend:snap:*:{symbol}:*",
    ]
    keys = []
    for pat in pats:
        cursor = 0
        while True:
            cursor, batch = R.scan(cursor=cursor, match=pat, count=200)
            if not batch and cursor == 0: break
            for k in batch:
                ks = k.decode() if isinstance(k,(bytes,bytearray)) else k
                keys.append(ks)
            if cursor == 0: break
    keys = sorted(set(keys), key=lambda x: (0 if ":ohlc:" in x else 1, x))
    return keys

def parse_snap_value(val):
    try:
        d = json.loads(val)
    except Exception:
        return None, None
    bars = d.get("bars") or []
    if not bars: return None, None
    df = pd.DataFrame(bars)
    if "ts_ms" not in df.columns and "t" in df.columns:
        df = df.rename(columns={"t":"ts_ms"})
    if "open" not in df.columns and "o" in df.columns:
        df = df.rename(columns={"o":"open","h":"high","l":"low","c":"close","v":"volume"})
    if "ts_ms" not in df.columns: return None, None
    if (df["ts_ms"] < 2_000_000_000_000).any():
        df["ts_ms"] = df["ts_ms"].astype(np.int64) * 1000
    keep_cols = [c for c in ["ts_ms","open","high","low","close","volume"] if c in df.columns]
    df = df[keep_cols].dropna().drop_duplicates().sort_values("ts_ms")
    tf = d.get("tf") or d.get("timeframe") or d.get("TF") or None
    tf = _norm_tf(str(tf)) if tf else None
    return df, tf

def resample_to_m15(df_in, tf_label):
    if tf_label == "M15":
        return df_in.copy()
    df = df_in.copy()
    dt = pd.to_datetime(df["ts_ms"], unit="ms", utc=True)
    df = df.assign(dt=dt).set_index("dt")
    rule = "15min"  # Pandas >=2.2
    o = df["open"].resample(rule).first()
    h = df["high"].resample(rule).max()
    l = df["low"].resample(rule).min()
    c = df["close"].resample(rule).last()
    v = df["volume"].resample(rule).sum()
    out = pd.DataFrame({"open":o,"high":h,"low":l,"close":c,"volume":v}).dropna(how="any")
    out = out.reset_index()
    out["ts_ms"] = (out["dt"].astype("int64") // 1_000_000).astype(np.int64)
    return out[["ts_ms","open","high","low","close","volume"]]

def pull_symbol_any_tf(symbol):
    keys = scan_snap_keys(symbol)
    if not keys:
        return pd.DataFrame(columns=["ts_ms","open","high","low","close","volume"])
    frames = []
    for k in keys:
        raw = R.get(k)
        if not raw: continue
        if isinstance(raw,(bytes,bytearray)): raw = raw.decode("utf-8","ignore")
        df, tf = parse_snap_value(raw)
        if df is None or df.empty: continue
        tf = tf or _norm_tf(k.split(":")[-1])
        df_m15 = resample_to_m15(df, tf)
        if not df_m15.empty: frames.append(df_m15)
    if not frames:
        return pd.DataFrame(columns=["ts_ms","open","high","low","close","volume"])
    out = pd.concat(frames, ignore_index=True)
    out = out.sort_values("ts_ms").drop_duplicates(subset=["ts_ms"]).reset_index(drop=True)
    out["symbol"] = symbol
    out["timeframe"] = "M15"
    return out

def atr14(ohlc):
    h = ohlc["high"].to_numpy()
    l = ohlc["low"].to_numpy()
    c = ohlc["close"].to_numpy()
    if len(c) == 0: return np.array([])
    prev_c = np.concatenate([[c[0]], c[:-1]])
    tr = np.maximum(h - l, np.maximum(np.abs(h - prev_c), np.abs(l - prev_c)))
    return pd.Series(tr).ewm(alpha=1/14, adjust=False).mean().to_numpy()

def rvol_m15_rowwise(df_upto):
    df = df_upto.copy()
    dt = pd.to_datetime(df["ts_ms"], unit="ms", utc=True)
    df["dt"] = dt
    df["tod_min"] = df["dt"].dt.hour * 60 + df["dt"].dt.minute
    df["date"] = df["dt"].dt.date
    last = df.iloc[-1]
    base = df[df["date"] < last["date"]].groupby("tod_min")["volume"].mean()
    base_mean = float(base.mean()) if len(base) else 0.0
    baseline = float(base.reindex([int(last["tod_min"])]).fillna(base_mean).iloc[0] if len(base) else 0.0)
    curr = float(last["volume"] or 0.0)
    denom = baseline if baseline > 0 else 1.0
    return curr / denom

def build_features(df):
    out = df.copy()
    out["ret_15m"] = out["close"].pct_change().fillna(0.0) * 100.0
    atr = atr14(out)
    out["atr14_m15_pct"] = (pd.Series(atr) / out["close"]).fillna(0.0) * 100.0
    rvol_vals = []
    for i in range(len(out)):
        rvol_vals.append(rvol_m15_rowwise(out.iloc[:i+1]))
    out["rvol15"] = rvol_vals
    dt = pd.to_datetime(out["ts_ms"], unit="ms", utc=True)
    out["tod_min"] = dt.dt.hour * 60 + dt.dt.minute
    out["dow"] = dt.dt.dayofweek + 1
    out["usd_basket_d1h_pct"] = np.nan  # stitched later if majors present
    return out

def label_next_1h(df):
    df = df.copy()
    next_close = df["close"].shift(-4)  # 4 x M15
    move_pct = (next_close / df["close"] - 1.0) * 100.0
    df["move_1h_pct"] = move_pct
    df["up_1h"] = (move_pct > 0).astype(int)
    return df

def stitch_usd_basket(symbol_frames):
    needed = {"EURUSD":"-", "GBPUSD":"-", "AUDUSD":"-", "USDJPY":"+", "USDCHF":"+", "USDCAD":"+"}
    avail = {k:v for k,v in needed.items() if k in symbol_frames and not symbol_frames[k].empty}
    if not avail: return None
    aligned = None
    for sym in avail.keys():
        f = symbol_frames[sym][["ts_ms","ret_15m"]].rename(columns={"ret_15m": f"r_{sym}"})
        aligned = f if aligned is None else aligned.merge(f, on="ts_ms", how="outer")
    aligned = aligned.sort_values("ts_ms").ffill()
    cols = [c for c in aligned.columns if c.startswith("r_")]
    arr = aligned[cols].copy()
    for c in cols:
        if c.endswith(("EURUSD","GBPUSD","AUDUSD")): arr[c] = -arr[c]
        else: arr[c] = +arr[c]
    aligned["usd_basket_d1h_pct"] = arr.mean(axis=1).rolling(4, min_periods=1).sum()
    return aligned[["ts_ms","usd_basket_d1h_pct"]]

def main():
    frames = {}
    for sym in SYMS:
        df = pull_symbol_any_tf(sym)
        if df.empty:
            print(f"[warn] no redis bars for {sym}")
            continue
        df = build_features(df)
        df = label_next_1h(df)
        frames[sym] = df

    if not frames:
        raise SystemExit("no frames found in Redis; ensure your agent pushed OHLC snapshots")

    basket = stitch_usd_basket(frames)
    if basket is not None:
        for sym in frames:
            frames[sym] = frames[sym].merge(basket, on="ts_ms", how="left")

    all_df = pd.concat(frames.values(), ignore_index=True)
    all_df = all_df.dropna(subset=["move_1h_pct"])

    # expected columns (ensure they exist)
    keep = [
        "symbol","ts_ms","close",
        "atr14_m15_pct","rvol15","ret_15m",
        "usd_basket_d1h_pct","tod_min","dow",
        "move_1h_pct","up_1h"
    ]
    for c in keep:
        if c not in all_df.columns:
            all_df[c] = np.nan

    all_df = all_df[keep].sort_values(["symbol","ts_ms"]).reset_index(drop=True)
    outp = OUT / "train.parquet"
    all_df.to_parquet(outp, index=False)
    print(f"[ok] wrote {outp} rows={len(all_df)} symbols={sorted(frames.keys())}")

if __name__ == "__main__":
    main()

