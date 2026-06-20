#!/usr/bin/env python3
"""
verify_sr.py  —  READ-ONLY SR verification.

Pulls live XAUUSD H1/H4 bars from Redis (dev_2cb...), runs the REAL
trend_sr.py detection, and prints major/near levels so you can eyeball
them against your TradingView chart.

Writes nothing. Touches no cache. Safe to run anytime.

Usage:
    python3 verify_sr.py
    python3 verify_sr.py --symbol XAUUSD --dev dev_2cb873afce2a43b6823e629c602b1120

Requires: redis, pandas, numpy, and trend_sr.py importable
(run from the api dir, e.g.  cd /opt/xauapi/api && python3 /path/verify_sr.py)
"""
from __future__ import annotations
import os, sys, json, argparse

# ---- config ----
REDIS_PASS = os.getenv("XTL_REDIS_PASS", "xau12345")
REDIS_HOST = os.getenv("XTL_REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.getenv("XTL_REDIS_PORT", "6379"))
DEFAULT_DEV = "dev_2cb873afce2a43b6823e629c602b1120"

def _load_bars(R, dev, sym, tf):
    """Read the OHLC snapshot exactly as the app stores it."""
    key = f"xtl:ohlc:snap:{dev}:{sym}:{tf}"
    raw = R.get(key)
    if not raw:
        return [], key
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8", "ignore")
    obj = json.loads(raw)
    bars = obj.get("bars") or obj.get("ohlc") or (obj if isinstance(obj, list) else [])
    return bars, key

def _bars_to_df(bars):
    """Convert snapshot bars -> the DataFrame shape trend_sr expects (o/h/l/c)."""
    import pandas as pd
    rows = []
    for b in bars:
        if not isinstance(b, dict):
            continue
        try:
            rows.append({
                "o": float(b["o"]), "h": float(b["h"]),
                "l": float(b["l"]), "c": float(b["c"]),
                "t": int(b.get("t") or b.get("t_close_ms") or 0),
            })
        except Exception:
            continue
    df = pd.DataFrame(rows)
    return df

def _fmt(x, n=2):
    try:
        return f"{float(x):.{n}f}"
    except Exception:
        return str(x)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", default="XAUUSD")
    ap.add_argument("--dev", default=DEFAULT_DEV)
    ap.add_argument("--price", type=float, default=None)
    args = ap.parse_args()
    sym, dev = args.symbol, args.dev

    try:
        import redis
    except ImportError:
        print("pip install redis  (or run inside the app venv)"); sys.exit(1)

    # import the REAL detection code
    try:
        from trend_sr import compute_sr_for_frame, summarize_sr_multi_tf, _atr14
    except ImportError as e:
        print(f"Cannot import trend_sr: {e}")
        print("Run from the api directory, e.g.  cd /opt/xauapi/api && python3 verify_sr.py")
        sys.exit(1)

    R = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASS)

    # live price (for side/role context)
    # live price: --price override first, else try redis keys
    if args.price:
        price = args.price
    else:
        price = None
        for pk in (f"xtl:price:{dev}:{sym}", f"xtl:price:{sym}"):
            try:
                v = R.get(pk)
                if v:
                    price = float(v.decode() if isinstance(v, (bytes, bytearray)) else v)
                    break
            except Exception:
                continue

    h1_bars, h1_key = _load_bars(R, dev, sym, "H1")
    h4_bars, h4_key = _load_bars(R, dev, sym, "H4")
    h1_df = _bars_to_df(h1_bars)
    h4_df = _bars_to_df(h4_bars)

    pip = 0.01 if sym == "XAUUSD" else (0.01 if sym.endswith("JPY") else 0.0001)

    print("=" * 64)
    print(f"SR VERIFY  sym={sym}  dev={dev[:16]}…")
    print(f"  price (live) : {_fmt(price)}")
    print(f"  H1 bars={len(h1_df)}  min_low={_fmt(h1_df['l'].min()) if len(h1_df) else 'NA'}  "
          f"max_high={_fmt(h1_df['h'].max()) if len(h1_df) else 'NA'}")
    print(f"  H4 bars={len(h4_df)}  min_low={_fmt(h4_df['l'].min()) if len(h4_df) else 'NA'}  "
          f"max_high={_fmt(h4_df['h'].max()) if len(h4_df) else 'NA'}")
    if len(h1_df):
        print(f"  H1 ATR14={_fmt(_atr14(h1_df))}")
    if len(h4_df):
        print(f"  H4 ATR14={_fmt(_atr14(h4_df))}")
    print("=" * 64)

    # full multi-tf summary (no cache passed -> writes nothing)
    summary = summarize_sr_multi_tf(
        symbol=sym, price=price or 0.0,
        h4_df=h4_df, h1_df=h1_df, pip_factor=pip, cache=None,
    )

    def _print_levels(title, levels, side):
        print(f"\n--- {title} ({len(levels)}) ---")
        if not levels:
            print("  (none)")
            return
        print(f"  {'level':>10} {'tf':>3} {'touch':>5} {'score':>6} "
              f"{'dist':>8} {'dATR':>6}  source/type")
        for x in levels:
            lvl = x.get("level")
            d = (price - lvl) if (price and side == "support") else \
                ((lvl - price) if price else None)
            print(f"  {_fmt(lvl):>10} {str(x.get('tf','')):>3} "
                  f"{str(x.get('touches','')):>5} {_fmt(x.get('sr_score'),1):>6} "
                  f"{_fmt(d):>8} {_fmt(x.get('distance_atr'),2):>6}  "
                  f"{x.get('source_type') or x.get('band_type') or ''}")

    h1 = summary.get("h1", {})
    h4 = summary.get("h4", {})

    print("\n########## SUPPORTS (should be BELOW price) ##########")
    _print_levels("H1 MAJOR supports", h1.get("supports_major") or [], "support")
    _print_levels("H4 MAJOR supports", h4.get("supports_major") or [], "support")
    _print_levels("H1 NEAR supports",  h1.get("supports_near")  or [], "support")

    print("\n########## RESISTANCES (should be ABOVE price) ##########")
    _print_levels("H1 MAJOR resistances", h1.get("resistances_major") or [], "resistance")
    _print_levels("H4 MAJOR resistances", h4.get("resistances_major") or [], "resistance")
    _print_levels("H1 NEAR resistances",  h1.get("resistances_near")  or [], "resistance")

    print("\n########## SUMMARY ##########")
    print(f"  nearest_support    = {_fmt(summary.get('nearest_support'))}")
    print(f"  nearest_resistance = {_fmt(summary.get('nearest_resistance'))}")
    print(f"  sr_safety          = {summary.get('sr_safety')}")

    # sanity flags
    print("\n########## SANITY CHECKS ##########")
    issues = []
    for x in (h1.get("supports_major") or []) + (h4.get("supports_major") or []):
        if price and x.get("level", 0) > price + 1e-9:
            issues.append(f"  ⚠ major SUPPORT above price: {_fmt(x.get('level'))}")
    for x in (h1.get("resistances_major") or []) + (h4.get("resistances_major") or []):
        if price and x.get("level", 0) < price - 1e-9:
            issues.append(f"  ⚠ major RESISTANCE below price: {_fmt(x.get('level'))}")
    # touch-count plausibility: flag anything >20 (doc says possible weakness)
    for x in (h1.get("supports_major") or []) + (h1.get("resistances_major") or []):
        if (x.get("touches") or 0) > 20:
            issues.append(f"  ⚠ very high touch count ({x.get('touches')}) at {_fmt(x.get('level'))} — check clustering")
    if issues:
        print("\n".join(issues))
    else:
        print("  ✓ no side violations, no extreme touch counts")

    print("\nDone. Compare the MAJOR levels above against your TradingView chart.")

if __name__ == "__main__":
    main()
