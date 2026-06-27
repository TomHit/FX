#!/usr/bin/env python3
"""
XTL Directional Session/Day Profiler
====================================
Extends the volatility profiler to answer the DIRECTIONAL question:
  "Which symbol moves best on the BUY side vs the SELL side, in which
   session, on which weekday?"  -> your symbol/session/day picking list.

KEY IDEA (read this):
  Volatility (does it move?) != direction (which way, with room?).
  This tool measures DIRECTION from price history:
    - signed move  = close - open   (net drift; + = buy bias, - = sell bias)
    - up-room       = high - open    (room a long had to a target)
    - down-room     = open - low     (room a short had to a target)
    - %green days   = directional consistency
  PLUS the original range/ATR volatility for room-to-target.

HONEST LIMIT:
  Drift is REGIME-DESCRIPTIVE, not predictive. A symbol showing a strong
  "buy bias" over the window usually just trended up in that window. If the
  regime flips, the bias flips. Re-run monthly. Cross-check against your
  setup direction. The 'reliability' column flags buckets whose drift is
  too small vs its noise to trust (|t| < 2 = WEAK). Do not trade WEAK days.

AGGREGATION:
  One row per (symbol, date, session) -- NOT per H1 bar. Averaging
  consecutive H1 bars inflates the sample with autocorrelated data and makes
  noise look like signal. Session-days are the honest unit; n = number of days.

RUN ON SERVER:
  /opt/xauapi/venv_ml/bin/python xtl_directional_profiler.py
  # if parquet timestamps are BROKER time (often UTC+2/+3), set the offset or
  # every session label is shifted and the conclusions are wrong:
  /opt/xauapi/venv_ml/bin/python xtl_directional_profiler.py --tz-offset 3

Options:
  --parquet PATH    default: the train_raw_h1_1y.parquet
  --tz-offset H     hours to SUBTRACT if ts_ms is broker time, not UTC
  --months N        only last N months (0 = all)
  --min-days N      hide weekday buckets with fewer than N session-days (default 25)
"""

import argparse, sys
import numpy as np
import pandas as pd

# ---- pip/point size per symbol (CFD conventions; VERIFY vs your broker) ----
PIP = {
    "EURUSD": 0.0001, "GBPUSD": 0.0001, "USDCAD": 0.0001, "USDCHF": 0.0001,
    "USDJPY": 0.01,
    "XAUUSD": 1.0,   # gold reported in DOLLARS of move
}
GOLD_NOTE = "(XAUUSD reported in $ move, others in pips)"

def session_of(hour_utc: int) -> str:
    h = hour_utc
    if 7 <= h < 12:   return "London"
    if 12 <= h < 16:  return "Overlap"
    if 16 <= h < 21:  return "NY_late"
    return "Asia"                      # 21:00-07:00

SESS_ORDER = ["Asia", "London", "Overlap", "NY_late"]
DOW_NAME   = {0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri"}
DOW_ORDER  = ["Mon", "Tue", "Wed", "Thu", "Fri"]


def load(args):
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

    df["hour"]    = df["dt"].dt.hour
    df["dow"]     = df["dt"].dt.dayofweek
    df = df[df["dow"] < 5]
    df["weekday"] = df["dow"].map(DOW_NAME)
    df["session"] = df["hour"].map(session_of)
    df["date"]    = df["dt"].dt.date
    df = df.sort_values(["symbol", "ts_ms"])
    return df


def session_days(df: pd.DataFrame) -> pd.DataFrame:
    """Collapse H1 bars -> one row per (symbol, date, session). This is the
    honest unit of observation for directional stats."""
    has_vol = "volume" in df.columns
    agg = {
        "open":  "first",
        "close": "last",
        "high":  "max",
        "low":   "min",
        "weekday": "first",
    }
    if has_vol:
        agg["volume"] = "sum"
    sd = (df.groupby(["symbol", "date", "session"], as_index=False)
            .agg(agg))

    sd["signed_px"] = sd["close"] - sd["open"]        # + buy bias / - sell bias
    sd["range_px"]  = sd["high"]  - sd["low"]
    sd["up_px"]     = sd["high"]  - sd["open"]        # long room
    sd["dn_px"]     = sd["open"]  - sd["low"]         # short room
    sd["green"]     = (sd["signed_px"] > 0).astype(int)
    # efficiency ratio: how much of the day's range became NET direction.
    # ~1 = clean trend day (breakout follows through); ~0 = round-trip / chop.
    sd["eff"]       = sd["signed_px"].abs() / sd["range_px"].replace(0, np.nan)
    return sd


def _reliability(mean, std, n):
    """Rough t-like score: is the drift meaningfully different from zero?"""
    if n is None or n < 2 or std is None or std == 0 or pd.isna(std):
        return np.nan
    return mean / (std / np.sqrt(n))


def summarize(sd: pd.DataFrame, keys, pip_map):
    """Aggregate session-day rows by `keys` into pip-scaled directional stats."""
    out = []
    for kv, g in sd.groupby(keys):
        if not isinstance(kv, tuple):
            kv = (kv,)
        rec = dict(zip(keys, kv))
        sym = rec["symbol"]
        pip = pip_map.get(sym, 0.0001)
        n   = len(g)
        signed_mean = g["signed_px"].mean()
        signed_std  = g["signed_px"].std()
        rec.update({
            "n_days":      n,
            "signed_pip":  round(signed_mean / pip, 1),
            "reliab":      round(_reliability(signed_mean, signed_std, n), 2),
            "up_room_pip": round(g["up_px"].mean()    / pip, 1),
            "dn_room_pip": round(g["dn_px"].mean()    / pip, 1),
            "range_pip":   round(g["range_px"].mean() / pip, 1),
            "pct_green":   round(100 * g["green"].mean(), 1),
            "eff_day":     round(g["eff"].mean(), 2),
        })
        out.append(rec)
    return pd.DataFrame(out)


def reliab_tag(t):
    if pd.isna(t):       return "n/a"
    if abs(t) >= 3:      return "STRONG"
    if abs(t) >= 2:      return "ok"
    return "WEAK"


def trend_tag(eff):
    # ~0.40 = random-walk baseline. Above => trends; below => mean-reverts.
    if pd.isna(eff):     return "n/a"
    if eff >= 0.50:      return "TREND"
    if eff >= 0.38:      return "mixed"
    return "CHOP"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--parquet", default="/opt/xauapi/api/trend/out/train_raw_h1_1y.parquet")
    ap.add_argument("--tz-offset", type=float, default=0.0)
    ap.add_argument("--months", type=int, default=0)
    ap.add_argument("--min-days", type=int, default=25)
    ap.add_argument("--csv-prefix", default="xtl_directional")
    args = ap.parse_args()

    df = load(args)
    sd = session_days(df)

    print(f"\nsession-days: {len(sd):,}   span: {df['dt'].min()} -> {df['dt'].max()}")
    print(f"tz-offset applied: {args.tz_offset}h   {GOLD_NOTE}")
    print("signed_pip>0 = BUY bias, <0 = SELL bias | reliab: |>=2| ok, |>=3| STRONG, else WEAK(noise)\n")

    # ---------- symbol x session ----------
    ss = summarize(sd, ["symbol", "session"], PIP)
    ss["sess_ord"] = ss["session"].map({s: i for i, s in enumerate(SESS_ORDER)})
    ss = ss.sort_values(["symbol", "sess_ord"]).drop(columns="sess_ord")
    ss.to_csv(f"{args.csv_prefix}_symbol_session.csv", index=False)

    print("=== DIRECTIONAL BIAS  (symbol x session) ===")
    print(ss.to_string(index=False), "\n")

    # ---------- TREND vs CHOP (efficiency ratio) ----------
    print("=== TREND vs CHOP  (eff_day = net direction / range per session-day) ===")
    print("    ~0.40 = random-walk baseline | >=0.50 TREND (momentum/breakout-friendly) | <0.38 CHOP (whipsaw)")
    for sym in sorted(ss["symbol"].unique()):
        s = ss[ss["symbol"] == sym].sort_values("eff_day", ascending=False)
        parts = "   ".join(f"{r.session}={r.eff_day}[{trend_tag(r.eff_day)}]" for r in s.itertuples())
        print(f"  {sym:8} {parts}")
    print()

    # ---------- symbol x session x weekday ----------
    ssd = summarize(sd, ["symbol", "session", "weekday"], PIP)
    ssd = ssd[ssd["n_days"] >= args.min_days].copy()
    ssd["sess_ord"] = ssd["session"].map({s: i for i, s in enumerate(SESS_ORDER)})
    ssd["dow_ord"]  = ssd["weekday"].map({d: i for i, d in enumerate(DOW_ORDER)})
    ssd = ssd.sort_values(["symbol", "sess_ord", "dow_ord"]).drop(columns=["sess_ord", "dow_ord"])
    ssd.to_csv(f"{args.csv_prefix}_symbol_session_weekday.csv", index=False)

    # ---------- BEST BUY ----------
    print("=== BEST BUY  (signed_pip > 0, ranked; WEAK = likely noise, ignore) ===")
    buys = ss[ss["signed_pip"] > 0].copy()
    buys["tag"] = buys["reliab"].map(reliab_tag)
    buys = buys.sort_values("signed_pip", ascending=False)
    for r in buys.itertuples():
        print(f"  {r.symbol:8} {r.session:8} +{r.signed_pip:7} pip  "
              f"up_room={r.up_room_pip:6}  green={r.pct_green:5}%  n={r.n_days:4}  [{r.tag}]")
    print()

    # ---------- BEST SELL ----------
    print("=== BEST SELL (signed_pip < 0, ranked; WEAK = likely noise, ignore) ===")
    sells = ss[ss["signed_pip"] < 0].copy()
    sells["tag"] = sells["reliab"].map(reliab_tag)
    sells = sells.sort_values("signed_pip", ascending=True)
    for r in sells.itertuples():
        print(f"  {r.symbol:8} {r.session:8} {r.signed_pip:8} pip  "
              f"dn_room={r.dn_room_pip:6}  green={r.pct_green:5}%  n={r.n_days:4}  [{r.tag}]")
    print()

    # ---------- best weekday per symbol/session (only reliable ones) ----------
    print("=== STRONGEST WEEKDAY per symbol x session (|reliab|>=2 only; rest hidden as noise) ===")
    rel = ssd[ssd["reliab"].abs() >= 2].copy()
    if rel.empty:
        print("  (none survived the reliability filter -- weekday effects are noise in this window)\n")
    else:
        rel["dir"] = np.where(rel["signed_pip"] > 0, "BUY ", "SELL")
        rel = rel.reindex(rel["signed_pip"].abs().sort_values(ascending=False).index)
        for r in rel.itertuples():
            print(f"  {r.symbol:8} {r.session:8} {r.weekday}  {r.dir} "
                  f"{r.signed_pip:+8} pip  green={r.pct_green:5}%  n={r.n_days:4}  reliab={r.reliab}")
        print()

    print(f"wrote: {args.csv_prefix}_symbol_session.csv, {args.csv_prefix}_symbol_session_weekday.csv")
    print("\nHOW TO USE:")
    print("  1. Pick symbol+session from BEST BUY / BEST SELL where tag is ok/STRONG.")
    print("  2. Confirm up_room (buys) / dn_room (sells) exceeds your typical target distance.")
    print("  3. Use weekday only where it survived the reliability filter; ignore WEAK.")
    print("  4. RE-RUN MONTHLY. Drift is regime-bound -- gold's buy bias is this year's uptrend,")
    print("     not a permanent rule. When trend flips, these tables flip.")


if __name__ == "__main__":
    main()
