#!/usr/bin/env python3
"""
XTL Session Volatility Profiler
================================
Answers: "how much does each symbol move per session?" — the basis for picking
WHICH symbol to trade when several confirm the same direction in a given session.

Reads the existing 1-year H1 parquet (all 6 symbols), buckets each bar by UTC
session, and reports per symbol x session:
  - range_pip      : avg (high-low) in instrument pips/points   <- room-to-target
  - absmove_pip    : avg |close-open| in pips                   <- net directional travel
  - atr_pip        : avg ATR(14) of bars in that session        <- normalized volatility
  - tick_vol       : avg tick volume (NOT real volume; spot CFD) <- activity proxy
  - n              : sample size (bars)

Outputs: printed tables + xtl_session_volatility.csv + a ranked
"best session per symbol" summary.

RUN ON SERVER:
  /opt/xauapi/venv_ml/bin/python xtl_session_volatility.py
  (venv_ml has pandas/pyarrow)

Options:
  --parquet PATH   (default: the train_raw_h1_1y.parquet)
  --tz-offset H    if timestamps are NOT UTC, set broker offset hours (e.g. 2 or 3) to subtract
  --months N       only use the last N months (default: all ~14)
"""

import argparse, sys
import pandas as pd
import numpy as np

# ---- pip/point size per symbol (CFD conventions; VERIFY vs your broker spec) ----
# 1 "pip" = how we scale the move into readable units.
#   FX majors: 0.0001 ; JPY pairs: 0.01 ; XAUUSD: 0.1 ($0.10) -> we report in $ for gold
PIP = {
    "EURUSD": 0.0001, "GBPUSD": 0.0001, "USDCAD": 0.0001, "USDCHF": 0.0001,
    "USDJPY": 0.01,
    "XAUUSD": 1.0,   # report gold move in DOLLARS (so 9.1 = $9.1), not pips
}
GOLD_NOTE = "(XAUUSD reported in $ move, others in pips)"

# ---- UTC session buckets (hour of day, UTC) ----
# Asia/Tokyo ~ 23:00-07:00 ; London ~ 07:00-12:00 ; Overlap(LDN+NY) ~ 12:00-16:00 ; NY-late ~ 16:00-21:00
# (21:00-23:00 = thin pre-Asia, folded into Asia)
def session_of(hour_utc: int) -> str:
    h = hour_utc
    if 7 <= h < 12:   return "London"
    if 12 <= h < 16:  return "Overlap"   # London+NY, usually highest movement on majors
    if 16 <= h < 21:  return "NY_late"
    return "Asia"                          # 21:00-07:00

SESS_ORDER = ["Asia", "London", "Overlap", "NY_late"]

def atr14(g: pd.DataFrame) -> pd.Series:
    """True Range then 14-period rolling mean, computed per symbol in time order."""
    h, l, c = g["high"], g["low"], g["close"]
    pc = c.shift(1)
    tr = pd.concat([(h - l), (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
    return tr.rolling(14, min_periods=14).mean()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--parquet", default="/opt/xauapi/api/trend/out/train_raw_h1_1y.parquet")
    ap.add_argument("--tz-offset", type=float, default=0.0,
                    help="hours to SUBTRACT if ts_ms is broker time, not UTC (e.g. 2 or 3)")
    ap.add_argument("--months", type=int, default=0, help="limit to last N months (0=all)")
    ap.add_argument("--csv", default="xtl_session_volatility.csv")
    args = ap.parse_args()

    try:
        df = pd.read_parquet(args.parquet)
    except Exception as e:
        print(f"ERROR reading {args.parquet}: {e}", file=sys.stderr); sys.exit(1)

    need = {"symbol", "ts_ms", "open", "high", "low", "close"}
    if not need.issubset(df.columns):
        print(f"ERROR: parquet missing columns. have={list(df.columns)}", file=sys.stderr); sys.exit(1)

    df = df.copy()
    df["dt"] = pd.to_datetime(df["ts_ms"], unit="ms", utc=True)
    if args.tz_offset:
        df["dt"] = df["dt"] - pd.to_timedelta(args.tz_offset, unit="h")

    if args.months and args.months > 0:
        cutoff = df["dt"].max() - pd.DateOffset(months=args.months)
        df = df[df["dt"] >= cutoff]

    df["hour"] = df["dt"].dt.hour
    df["dow"]  = df["dt"].dt.dayofweek          # 0=Mon..6=Sun
    df = df[df["dow"] < 5]                        # drop weekend stragglers
    df["session"] = df["hour"].map(session_of)

    # per-symbol ATR in time order
    df = df.sort_values(["symbol", "ts_ms"])
    df["atr"] = df.groupby("symbol", group_keys=False).apply(lambda g: atr14(g))

    # raw moves in price units
    df["range_px"]   = (df["high"] - df["low"]).abs()
    df["absmove_px"] = (df["close"] - df["open"]).abs()

    print(f"\nrows used: {len(df):,}   span: {df['dt'].min()}  ->  {df['dt'].max()}")
    print(f"tz-offset applied: {args.tz_offset}h   {GOLD_NOTE}\n")

    rows_out = []
    for sym, g in df.groupby("symbol"):
        pip = PIP.get(sym, 0.0001)
        agg = g.groupby("session").agg(
            range_px=("range_px", "mean"),
            absmove_px=("absmove_px", "mean"),
            atr_px=("atr", "mean"),
            tick_vol=("volume", "mean") if "volume" in g.columns else ("range_px", "size"),
            n=("range_px", "size"),
        )
        agg = agg.reindex(SESS_ORDER)
        for sess, r in agg.iterrows():
            rows_out.append({
                "symbol": sym, "session": sess,
                "range_pip":   round(r["range_px"]   / pip, 1) if pd.notna(r["range_px"]) else None,
                "absmove_pip": round(r["absmove_px"] / pip, 1) if pd.notna(r["absmove_px"]) else None,
                "atr_pip":     round(r["atr_px"]     / pip, 1) if pd.notna(r["atr_px"]) else None,
                "tick_vol":    int(r["tick_vol"]) if pd.notna(r.get("tick_vol")) else None,
                "n":           int(r["n"]) if pd.notna(r["n"]) else 0,
            })

    out = pd.DataFrame(rows_out)
    out.to_csv(args.csv, index=False)

    # ---- printed table: range_pip per symbol x session (the headline metric) ----
    piv = out.pivot(index="symbol", columns="session", values="range_pip").reindex(columns=SESS_ORDER)
    print("=== AVG H1 RANGE per session (pips; XAUUSD=$) — room-to-target ===")
    print(piv.to_string(), "\n")

    piv2 = out.pivot(index="symbol", columns="session", values="atr_pip").reindex(columns=SESS_ORDER)
    print("=== AVG ATR(14) per session (pips; XAUUSD=$) — normalized volatility ===")
    print(piv2.to_string(), "\n")

    # ---- best session per symbol (ranked by range) ----
    print("=== BEST SESSION per symbol (by avg range) — which session to favor ===")
    for sym in sorted(out["symbol"].unique()):
        s = out[out["symbol"] == sym].dropna(subset=["range_pip"]).sort_values("range_pip", ascending=False)
        if s.empty:
            continue
        ranked = "  >  ".join(f"{r.session}({r.range_pip})" for r in s.itertuples())
        print(f"  {sym:8} {ranked}")
    print()

    print(f"wrote {len(out)} rows -> {args.csv}")
    print("\nHOW TO USE for symbol-picking:")
    print("  When N symbols confirm the SAME direction in the current session,")
    print("  favor the one whose CURRENT session has the highest range/ATR here")
    print("  (most room to reach target). Cross-check vs your stop/target distance:")
    print("  if target needs more pips than the symbol typically moves this session, skip it.")
    print("\nCAVEAT: this is VOLATILITY (does it move?), NOT expectancy (do XTL setups WIN?).")
    print("Layer setup win-rate per session on top once you have logged trades.")

if __name__ == "__main__":
    main()
