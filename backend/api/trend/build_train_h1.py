# -*- coding: utf-8 -*-
"""
build_train_h1_parquet.py

Rebuild *out/train.parquet* as a pure H1-feature training set:
 - Features:
     atr14_h1_pct
     rvol_h1
     ret_1h
     usd_basket_h1_pct
     tod_min
     dow
 - Targets:
     move_1h_pct
     up_1h

After running this, train_xgb.py can safely use H1 FEATURE_COLS
without KeyError, and still read from out/train.parquet.
"""

import pathlib
import numpy as np
import pandas as pd

BASE = pathlib.Path("/opt/xauapi/api/trend")
OUT_DIR = BASE / "out"
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_PATH = OUT_DIR / "train.parquet"      # <--- overwrite existing file here


# ------------------ small helpers ------------------ #

def _atr14(close: pd.Series, high: pd.Series, low: pd.Series) -> pd.Series:
    close = close.astype(float)
    high = high.astype(float)
    low = low.astype(float)

    prev_close = close.shift(1)
    tr1 = (high - low).abs()
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return tr.rolling(14, min_periods=1).mean()


def _rvol_h1(vol: pd.Series, lookback: int = 20) -> pd.Series:
    vol = vol.astype(float)
    ma = vol.rolling(lookback, min_periods=1).mean()
    std = vol.rolling(lookback, min_periods=1).std().replace(0.0, np.nan)
    out = (vol - ma) / std
    return out.fillna(0.0)


def _attach_time_features(df: pd.DataFrame) -> pd.DataFrame:
    dt = pd.to_datetime(df["ts_ms"], unit="ms", utc=True)
    df["tod_min"] = dt.dt.hour * 60 + dt.dt.minute
    df["dow"] = dt.dt.dayofweek + 1
    return df


def _compute_usd_basket_h1(df_all: pd.DataFrame) -> pd.DataFrame:
    """
    Very simple USD basket example:

    For each ts_ms, compute mean of 1h returns across all symbols.
    Adjust if you already have a more precise usd_basket recipe.
    """
    df = df_all[["symbol", "ts_ms", "close"]].copy()
    df["ret_1h_raw"] = df.groupby("symbol")["close"].pct_change()
    basket = (
        df.groupby("ts_ms")["ret_1h_raw"]
        .mean()
        .reset_index()
        .rename(columns={"ret_1h_raw": "usd_basket_h1"})
    )
    return basket


def _build_features_h1(df_sym: pd.DataFrame,
                       basket: pd.DataFrame | None) -> pd.DataFrame:
    """
    Build H1 features for ONE symbol.

    Required columns in df_sym:
      symbol, ts_ms, open, high, low, close, volume
    """
    df = df_sym.sort_values("ts_ms").reset_index(drop=True).copy()

    # 1h return in percent
    df["ret_1h"] = df["close"].pct_change().fillna(0.0) * 100.0

    # ATR% on 1h
    atr = _atr14(df["close"], df["high"], df["low"])
    df["atr14_h1_pct"] = (
        (atr / df["close"])
        .replace([np.inf, -np.inf], np.nan)
        .fillna(0.0)
        * 100.0
    )

    # RVOL on 1h
    df["rvol_h1"] = _rvol_h1(df["volume"])

    # time features
    df = _attach_time_features(df)

    # USD basket in percent
    if basket is not None and not basket.empty:
        df = df.merge(basket, on="ts_ms", how="left")
        df["usd_basket_h1_pct"] = df["usd_basket_h1"].fillna(0.0) * 100.0
    else:
        df["usd_basket_h1_pct"] = 0.0

    return df


def _attach_targets(df: pd.DataFrame) -> pd.DataFrame:
    """
    Label: NEXT 1h move from current close.
      move_1h_pct = (close_next - close_this) / close_this * 100
      up_1h       = 1 if move_1h_pct > 0 else 0
    """
    df = df.sort_values("ts_ms").reset_index(drop=True)
    close = df["close"].astype(float)
    close_next = close.shift(-1)
    move_pct = (close_next - close) / close * 100.0
    df["move_1h_pct"] = move_pct.fillna(0.0)
    df["up_1h"] = (df["move_1h_pct"] > 0.0).astype(int)
    return df


# ------------------ your data source ------------------ #

def load_h1_history() -> pd.DataFrame:
    """
    TODO: adjust this to where your historical H1 OHLC actually lives.

    For example, if you already dumped OHLC to a parquet:
        /opt/xauapi/api/trend/out/ohlc_h1_all.parquet

    with columns:
        symbol, ts_ms, open, high, low, close, volume

    you can just read that. If you store it in Postgres, replace this
    with a read_sql query.
    """
    src = OUT_DIR / "ohlc_h1_all.parquet"
    df = pd.read_parquet(src)

    needed = {"symbol", "ts_ms", "open", "high", "low", "close", "volume"}
    missing = needed - set(df.columns)
    if missing:
        raise RuntimeError(f"Missing required columns in H1 source: {missing}")
    return df


# ------------------ main pipeline ------------------ #

def main():
    df_all = load_h1_history()

    # build USD basket once from all symbols
    basket = _compute_usd_basket_h1(df_all)

    out_chunks: list[pd.DataFrame] = []

    for sym, df_sym in df_all.groupby("symbol"):
        df_feat = _build_features_h1(df_sym, basket)
        df_feat = _attach_targets(df_feat)
        out_chunks.append(df_feat)

    out = pd.concat(out_chunks, ignore_index=True)

    # drop rows with NaN targets (last bar per symbol)
    out = out[out["move_1h_pct"].notna()].copy()

    out.to_parquet(OUT_PATH, index=False)
    print(f"Wrote H1 training set into {OUT_PATH} rows={len(out)}")


if __name__ == "__main__":
    main()
