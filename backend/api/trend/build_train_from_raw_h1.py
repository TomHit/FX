# build_train_from_raw_h1.py
# Convert 1-year raw H1 OHLC -> train.parquet used by train_xgb.py

import pathlib
from typing import List

import numpy as np
import pandas as pd

BASE = pathlib.Path("/opt/xauapi/api/trend")
RAW = BASE / "out" / "train_raw_h1_1y.parquet"
OUT = BASE / "out" / "train.parquet"

FEATURE_COLS: List[str] = [
    "atr14_h1_pct",
    "rvol_h1",
    "ret_1h",
    "usd_basket_h1_pct",
    "tod_min",
    "dow",
]

TARGET_BIN = "up_1h"
TARGET_REG = "move_1h_pct"


def _compute_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if not {"symbol", "ts_ms", "open", "high", "low", "close", "volume"}.issubset(df.columns):
        raise RuntimeError("train_raw_h1_1y.parquet must have symbol, ts_ms, open, high, low, close, volume")

    df = df.sort_values(["symbol", "ts_ms"])

    def _per_symbol(g: pd.DataFrame) -> pd.DataFrame:
        g = g.sort_values("ts_ms")

        # previous close for TR / ret
        g["close_prev"] = g["close"].shift(1)

        # True range for ATR
        tr1 = g["high"] - g["low"]
        tr2 = (g["high"] - g["close_prev"]).abs()
        tr3 = (g["low"] - g["close_prev"]).abs()
        g["tr"] = np.nanmax(np.vstack([tr1.to_numpy(), tr2.to_numpy(), tr3.to_numpy()]), axis=0)

        # ATR(14) simple rolling mean
        g["atr14"] = g["tr"].rolling(14, min_periods=14).mean()
        g["atr14_h1_pct"] = (g["atr14"] / g["close"]) * 100.0

        # Past 1h return (feature)
        g["ret_1h"] = (g["close"] / g["close_prev"] - 1.0) * 100.0

        # Label: next 1h move (future)
        g["close_fwd"] = g["close"].shift(-1)
        g["move_1h_pct"] = (g["close_fwd"] - g["close"]) / g["close"] * 100.0
        g["up_1h"] = (g["move_1h_pct"] > 0).astype("int8")

        # Relative volume: vol vs 20-bar mean
        g["vol_ma20"] = g["volume"].rolling(20, min_periods=10).mean()
        g["rvol_h1"] = g["volume"] / g["vol_ma20"]
        g["rvol_h1"] = g["rvol_h1"].replace([np.inf, -np.inf], np.nan)

        # Time-of-day + day-of-week
        ts = pd.to_datetime(g["ts_ms"], unit="ms", utc=True)
        g["tod_min"] = ts.dt.hour * 60 + ts.dt.minute
        g["dow"] = ts.dt.weekday.astype("int16")

        # Macro placeholder: if you don't have usd basket history here,
        # keep neutral 0.0 so training still works.
        g["usd_basket_h1_pct"] = 0.0

        return g

    df = df.groupby("symbol", group_keys=False).apply(_per_symbol)

    # Drop rows where labels are NaN (edges of series)
    df = df.dropna(subset=["move_1h_pct", "up_1h"])

    # Drop rows where required feature cols are NaN/inf
    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.dropna(subset=FEATURE_COLS + [TARGET_BIN, TARGET_REG])

    return df


def main() -> None:
    if not RAW.exists():
        raise SystemExit(f"Missing raw file: {RAW}")

    print(f"[LOAD] {RAW}")
    df_raw = pd.read_parquet(RAW)
    print(f"[INFO] raw rows: {len(df_raw)}")

    df = _compute_features(df_raw)
    print(f"[INFO] after feature build + label filtering: {len(df)} rows")

    # Keep only the columns train_xgb.py needs (plus symbol/ts/close for H4 training)
    keep_cols = ["symbol", "ts_ms", "close"] + FEATURE_COLS + [TARGET_BIN, TARGET_REG]
    df_out = df[keep_cols].copy()

    df_out.to_parquet(OUT)
    print(f"[DONE] wrote {len(df_out)} rows to {OUT}")


if __name__ == "__main__":
    main()
