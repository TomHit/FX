# -*- coding: utf-8 -*-
import os, json, pathlib, numpy as np, pandas as pd
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

# ---- configurable ----
BASE_TF = os.getenv("TTH_BASE_TF", "M15").upper()         # prediction grid
FWD_TF  = os.getenv("TTH_FWD_TF",  "M1").upper()          # hit detection resolution
MAX_HOURS = float(os.getenv("TTH_MAX_HOURS", "8"))        # scalable to 24
MAX_FWD_BARS = int((MAX_HOURS * 60) / (TF_MS[FWD_TF] / 60_000))
# Time buckets (minutes) — model can output any bucket (not fixed horizons)
TIME_BUCKETS_MIN = [15, 30, 45, 60, 90, 120, 180, 240, 360, 480]  # 8h max

# ATR multiples (volatility-normalized target sizes)
K_LIST = [0.5, 1.0, 1.5, 2.0]

def _norm_tf(tf: str) -> str:
    tf = (tf or "").upper().replace("MIN", "M")
    if tf in TF_MS: return tf
    if tf.endswith("M") and tf[:-1].isdigit():
        x = int(tf[:-1])
        if x == 1: return "M1"
        if x == 5: return "M5"
        if x == 15: return "M15"
    if tf in ("60", "H1"): return "H1"
    return tf

def scan_snap_keys(symbol: str) -> list[str]:
    pats = [
        f"xtl:ohlc:snap:*:{symbol}:*",
        f"xtl:trend:snap:*:{symbol}:*",
    ]
    keys: list[str] = []
    for pat in pats:
        cursor = 0
        while True:
            cursor, batch = R.scan(cursor=cursor, match=pat, count=200)
            if not batch and cursor == 0:
                break
            for k in batch:
                ks = k.decode() if isinstance(k, (bytes, bytearray)) else k
                keys.append(ks)
            if cursor == 0:
                break
    return sorted(set(keys))

def parse_snap_value(val: str):
    try:
        d = json.loads(val)
    except Exception:
        return None, None
    bars = d.get("bars") or []
    if not bars:
        return None, None
    df = pd.DataFrame(bars)

    # normalize column names
    if "ts_ms" not in df.columns and "t" in df.columns:
        df = df.rename(columns={"t": "ts_ms"})
    if "open" not in df.columns and "o" in df.columns:
        df = df.rename(columns={"o": "open", "h": "high", "l": "low", "c": "close", "v": "volume"})

    if "ts_ms" not in df.columns:
        return None, None

    # seconds ? ms
    if (df["ts_ms"] < 2_000_000_000_000).any():
        df["ts_ms"] = df["ts_ms"].astype(np.int64) * 1000

    keep = [c for c in ["ts_ms", "open", "high", "low", "close", "volume"] if c in df.columns]
    df = df[keep].dropna().drop_duplicates().sort_values("ts_ms")
    if df.empty:
        return None, None

    tf = d.get("tf") or d.get("timeframe") or None
    tf = _norm_tf(tf) if tf else None
    return df, tf

def pull_symbol_tf(symbol: str, want_tf: str) -> pd.DataFrame:
    want_tf = _norm_tf(want_tf)
    keys = scan_snap_keys(symbol)
    if not keys:
        return pd.DataFrame(columns=["ts_ms","open","high","low","close","volume"])

    frames = []
    for k in keys:
        raw = R.get(k)
        if not raw:
            continue
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", "ignore")
        df, tf = parse_snap_value(raw)
        if df is None or df.empty:
            continue
        tf = tf or _norm_tf(k.split(":")[-1])
        if tf == want_tf:
            frames.append(df)

    if not frames:
        return pd.DataFrame(columns=["ts_ms","open","high","low","close","volume"])

    out = pd.concat(frames, ignore_index=True).drop_duplicates().sort_values("ts_ms")
    return out.reset_index(drop=True)

def resample(df: pd.DataFrame, tf: str) -> pd.DataFrame:
    tf = _norm_tf(tf)
    if df is None or df.empty:
        return pd.DataFrame(columns=["ts_ms","open","high","low","close","volume"])
    # already same tf (best-effort)
    # we assume input is true OHLC bars for that tf (from agent)
    return df.copy()

def atr14_pct(df: pd.DataFrame) -> pd.Series:
    # basic ATR(14) in pct (use OHLC)
    h, l, c = df["high"].astype(float), df["low"].astype(float), df["close"].astype(float)
    prev_c = c.shift(1)
    tr = pd.concat([(h-l).abs(), (h-prev_c).abs(), (l-prev_c).abs()], axis=1).max(axis=1)
    atr = tr.rolling(14, min_periods=14).mean()
    return (atr / c) * 100.0

def bucket_minutes(x_min: float) -> int:
    # map minutes to nearest upper bucket
    for b in TIME_BUCKETS_MIN:
        if x_min <= b:
            return b
    return TIME_BUCKETS_MIN[-1]

def label_tth(base_m15: pd.DataFrame, fwd_m1: pd.DataFrame, k: float) -> pd.DataFrame:
    """
    For each base bar close, look forward up to MAX_HOURS in fwd series and label:
      hit_side ? {UP, DOWN, NONE}
      t_hit_min_bucket ? TIME_BUCKETS_MIN or 0 for NONE
    Barrier = k * ATR14 (computed on base)
    """
    df = base_m15.copy()
    if df.empty or len(df) < 60:
        return pd.DataFrame()

    df["atr14_pct"] = atr14_pct(df)
    df = df.dropna(subset=["atr14_pct"]).reset_index(drop=True)
    if df.empty:
        return pd.DataFrame()

    # index forward bars by time for fast slicing
    fwd = fwd_m1.copy()
    if fwd.empty:
        return pd.DataFrame()
    fwd = fwd.drop_duplicates(subset=["ts_ms"]).sort_values("ts_ms").reset_index(drop=True)

    # Use close as entry reference (you can switch to next open later if you want)
    closes = df["close"].astype(float).values
    atrp   = df["atr14_pct"].astype(float).values
    ts     = df["ts_ms"].astype(np.int64).values

    # forward arrays
    f_ts = fwd["ts_ms"].astype(np.int64).values
    f_hi = fwd["high"].astype(float).values
    f_lo = fwd["low"].astype(float).values

    out_rows = []
    for i in range(len(df)):
        t0 = ts[i]
        entry = closes[i]
        barrier_pct = k * atrp[i]
        if not np.isfinite(barrier_pct) or barrier_pct <= 0:
            continue

        up_px = entry * (1.0 + barrier_pct / 100.0)
        dn_px = entry * (1.0 - barrier_pct / 100.0)

        # find forward window [t0, t0 + MAX_HOURS]
        t1 = t0 + int(MAX_HOURS * 60 * 60 * 1000)
        # locate start/end indices in fwd arrays
        j0 = np.searchsorted(f_ts, t0, side="left")
        j1 = np.searchsorted(f_ts, t1, side="right")
        if j1 <= j0:
            continue

        hit_side = "NONE"
        hit_min_bucket = 0
        hit_min = None

        # scan forward until first hit
        for j in range(j0, min(j1, j0 + MAX_FWD_BARS)):
            if f_hi[j] >= up_px:
                hit_side = "UP"
                hit_min = (f_ts[j] - t0) / 60000.0
                hit_min_bucket = bucket_minutes(hit_min)
                break
            if f_lo[j] <= dn_px:
                hit_side = "DOWN"
                hit_min = (f_ts[j] - t0) / 60000.0
                hit_min_bucket = bucket_minutes(hit_min)
                break

        out_rows.append({
            "symbol": None,  # filled later
            "ts_ms": int(t0),
            "close": float(entry),
            "atr14_pct": float(atrp[i]),
            "k": float(k),
            "barrier_pct": float(barrier_pct),
            "hit_side": hit_side,
            "t_hit_min_bucket": int(hit_min_bucket),
        })

    if not out_rows:
        return pd.DataFrame()
    return pd.DataFrame(out_rows)

def build_dataset_for_symbol(sym: str) -> pd.DataFrame:
    base = pull_symbol_tf(sym, BASE_TF)
    fwd  = pull_symbol_tf(sym, FWD_TF)

    # best effort: require base and fwd
    if base.empty or fwd.empty:
        return pd.DataFrame()

    base = resample(base, BASE_TF)
    fwd  = resample(fwd, FWD_TF)

    all_k = []
    for k in K_LIST:
        lab = label_tth(base, fwd, k)
        if lab.empty:
            continue
        lab["symbol"] = sym
        all_k.append(lab)

    if not all_k:
        return pd.DataFrame()

    out = pd.concat(all_k, ignore_index=True)
    # Add TOD/DOW
    dt = pd.to_datetime(out["ts_ms"], unit="ms", utc=True)
    out["tod_min"] = dt.dt.hour * 60 + dt.dt.minute
    out["dow"] = dt.dt.dayofweek + 1
    return out

def main():
    frames = []
    for sym in SYMS:
        sym_u = str(sym).upper()
        df = build_dataset_for_symbol(sym_u)
        if df is None or df.empty:
            continue
        frames.append(df)

    if not frames:
        raise RuntimeError("No data produced. Check Redis snaps for M15/M1.")

    all_df = pd.concat(frames, ignore_index=True).sort_values(["symbol","ts_ms","k"]).reset_index(drop=True)
    outp = OUT / f"train_tth_{BASE_TF.lower()}_{int(MAX_HOURS)}h.parquet"
    all_df.to_parquet(outp, index=False)
    print(f"[ok] wrote {outp} rows={len(all_df)} syms={all_df['symbol'].nunique()}")

if __name__ == "__main__":
    main()
