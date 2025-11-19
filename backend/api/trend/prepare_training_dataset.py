
# -*- coding: utf-8 -*-
# v1: builds train.parquet from SQLite (scanner_data.db or backtest_data.db)
# Tables it tries (override via env):
#   SCANNER_DB=/opt/xauapi/data/scanner_data.db   table=historical_data
#   BACKTEST_DB=/opt/xauapi/data/backtest_data.db table=backtest_data
#
# Assumed columns: ts (seconds UTC) or ts_ms (ms UTC), symbol, timeframe, open, high, low, close, volume
# Output: /opt/xauapi/api/trend/out/train.parquet

import os
import sqlite3
import pathlib
import pandas as pd
import numpy as np

BASE = pathlib.Path("/opt/xauapi/api/trend")
OUT  = BASE / "out"
OUT.mkdir(parents=True, exist_ok=True)

SCANNER_DB  = os.getenv("SCANNER_DB",  "/opt/xauapi/data/scanner_data.db")
BACKTEST_DB = os.getenv("BACKTEST_DB", "/opt/xauapi/data/backtest_data.db")
USE_DB = SCANNER_DB if os.path.exists(SCANNER_DB) else (BACKTEST_DB if os.path.exists(BACKTEST_DB) else None)
TABLE = os.getenv("OHLC_TABLE", "historical_data" if USE_DB == SCANNER_DB else "backtest_data")

from api.utils.config_loader import FEATURES

SYMS = FEATURES["symbols"]
HORIZON_MIN = 60  # next 1h
TF = "M15"        # v1 features on M15

def _read_ohlc(db_path, table, symbol, tf_label):
    con = sqlite3.connect(db_path)
    q = f"""
    SELECT
      COALESCE(ts_ms, ts*1000) AS ts_ms, symbol, timeframe, open, high, low, close, volume
    FROM {table}
    WHERE symbol = ? AND (
          timeframe = ?
       OR timeframe = 'M15'
       OR timeframe = '15'
       OR timeframe = '15m'
    )
    ORDER BY ts_ms
    """
    df = pd.read_sql_query(q, con, params=[symbol, tf_label])
    con.close()
    if df.empty:
        return df
    # normalize TF
    df["timeframe"] = "M15"
    df = df.drop_duplicates(subset=["ts_ms"]).reset_index(drop=True)
    return df

def atr14(ohlc):
    # Wilder ATR
    h = ohlc["high"].to_numpy()
    l = ohlc["low"].to_numpy()
    c = ohlc["close"].to_numpy()
    prev_c = np.concatenate([[c[0]], c[:-1]])
    tr = np.maximum(h - l, np.maximum(np.abs(h - prev_c), np.abs(l - prev_c)))
    atr = pd.Series(tr).ewm(alpha=1/14, adjust=False).mean().to_numpy()
    return atr

def rvol_m15_rowwise(df_upto):
    """
    RVOL for the last M15 bar vs 5-day baseline at the same time-of-day.
    We use the history up to current row (df_upto) to avoid look-ahead.
    """
    df = df_upto.copy()
    dt = pd.to_datetime(df["ts_ms"], unit="ms", utc=True)
    df["tod_min"] = dt.dt.hour * 60 + dt.dt.minute
    df["date"] = dt.dt.date
    last_row = df.iloc[-1]
    last_tod = int(last_row["tod_min"])
    last_date = last_row["date"]
    base = df[df["date"] < last_date].groupby("tod_min")["volume"].mean()
    base_mean = float(base.mean()) if len(base) else 0.0
    baseline = float(base.reindex([last_tod]).fillna(base_mean).iloc[0] if len(base) else 0.0)
    curr_vol = float(last_row["volume"] or 0.0)
    denom = baseline if baseline > 0 else 1.0
    return curr_vol / denom

def build_features(df):
    out = df.copy()
    out["ret_15m"] = out["close"].pct_change().fillna(0.0) * 100.0
    out["atr14_m15_pct"] = (pd.Series(atr14(out)) / out["close"]).fillna(0.0) * 100.0

    # RVOL per-row using only data up to that row (no look-ahead)
    rvol_vals = []
    for i in range(len(out)):
        rvol_vals.append(rvol_m15_rowwise(out.iloc[: i + 1]))
    out["rvol15"] = rvol_vals

    # Session features
    dt = pd.to_datetime(out["ts_ms"], unit="ms", utc=True)
    out["tod_min"] = dt.dt.hour * 60 + dt.dt.minute
    out["dow"] = dt.dt.dayofweek + 1

    # Placeholder for USD basket (stitched later)
    out["usd_basket_d1h_pct"] = np.nan
    return out

def label_next_1h(df):
    df = df.copy()
    # next 1h close = shift by +4 M15 bars
    next_close = df["close"].shift(-4)
    move_pct = (next_close / df["close"] - 1.0) * 100.0
    df["move_1h_pct"] = move_pct
    df["up_1h"] = (move_pct > 0).astype(int)
    return df

def stitch_usd_basket(symbol_frames):
    """
    Build a simple USD basket across majors at each timestamp (same TF).
    Pairs with USD as quote: EURUSD, GBPUSD, AUDUSD -> usd change approx = -ret
    Pairs with USD as base:  USDJPY, USDCHF, USDCAD -> usd change approx = +ret
    """
    need = {"EURUSD": "-", "GBPUSD": "-", "AUDUSD": "-", "USDJPY": "+", "USDCHF": "+", "USDCAD": "+"}
    present = {k: v for k, v in need.items() if k in symbol_frames}
    if not present:
        return None

    aligned = None
    for sym in present.keys():
        f = symbol_frames[sym][["ts_ms", "ret_15m"]].rename(columns={"ret_15m": f"r_{sym}"})
        aligned = f if aligned is None else aligned.merge(f, on="ts_ms", how="outer")
    aligned = aligned.sort_values("ts_ms").ffill()
    cols = [c for c in aligned.columns if c.startswith("r_")]
    arr = aligned[cols].copy()
    for c in cols:
        if c.endswith(("EURUSD", "GBPUSD", "AUDUSD")):
            arr[c] = -arr[c]
        else:
            arr[c] = +arr[c]
    # approx 1h change by summing 4 consecutive M15 returns
    aligned["usd_basket_d1h_pct"] = arr.mean(axis=1).rolling(4, min_periods=1).sum()
    return aligned[["ts_ms", "usd_basket_d1h_pct"]]

def main():
    if not USE_DB:
        raise SystemExit("No DB found. Set SCANNER_DB or BACKTEST_DB to your sqlite path.")

    frames = {}
    for sym in SYMS:
        try:
            df = _read_ohlc(USE_DB, TABLE, sym, TF)
            if df.empty:
                print(f"[warn] no data for {sym}")
                continue
            df = build_features(df)
            df = label_next_1h(df)
            frames[sym] = df
        except Exception as e:
            print(f"[warn] {sym}: {e}")

    if not frames:
        raise SystemExit("No frames built; check DB, table and symbol list in features.yaml")

    # Stitch USD basket into each frame (if majors present)
    basket = stitch_usd_basket(frames)
    if basket is not None:
        for sym, df in frames.items():
            frames[sym] = df.merge(basket, on="ts_ms", how="left")

    # Concatenate and clean
    all_df = pd.concat(frames.values(), ignore_index=True)
    # Remove tail rows where 1h label is unknown
    all_df = all_df.dropna(subset=["move_1h_pct"])

    # Keep a minimal set of columns consistent with v1 features/labels
    keep = [
        "symbol", "ts_ms", "close",
        "atr14_m15_pct", "rvol15", "ret_15m",
        "usd_basket_d1h_pct", "tod_min", "dow",
        "move_1h_pct", "up_1h"
    ]
    all_df = all_df[keep].sort_values(["symbol", "ts_ms"]).reset_index(drop=True)

    outp = OUT / "train.parquet"
    all_df.to_parquet(outp, index=False)
    print(f"[ok] wrote {outp} rows={len(all_df)} symbols={sorted(frames.keys())}")

if __name__ == "__main__":
    main()
PY
