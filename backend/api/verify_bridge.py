#!/usr/bin/env python3
"""
verify_bridge.py  —  READ-ONLY SR×LIQUIDITY bridge verification.

Pulls live XAUUSD H1/H4 bars from Redis (dev_2cb...), builds the REAL SR
bundle (trend_sr), runs the REAL liquidity bridge (liq_structure.
score_sr_with_liquidity), and prints each SR active level WITH its liquidity
evidence + quality_score + selection_reason — so you can verify the scoring
against your chart before wiring it into the live flow.

Writes nothing. Touches no cache. Safe to run anytime.

Usage:
    cd /opt/xauapi/api && python3 verify_bridge.py --price 4222
    python3 verify_bridge.py --price 4222 --dev dev_2cb873afce2a43b6823e629c602b1120
"""
from __future__ import annotations
import os, sys, json, argparse

REDIS_PASS = os.getenv("XTL_REDIS_PASS", "xau12345")
REDIS_HOST = os.getenv("XTL_REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.getenv("XTL_REDIS_PORT", "6379"))
DEFAULT_DEV = "dev_2cb873afce2a43b6823e629c602b1120"


def _load_bars(R, dev, sym, tf):
    key = f"xtl:ohlc:snap:{dev}:{sym}:{tf}"
    raw = R.get(key)
    if not raw:
        return [], key
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8", "ignore")
    obj = json.loads(raw)
    bars = obj.get("bars") or obj.get("ohlc") or (obj if isinstance(obj, list) else [])
    return bars, key


def _norm_bars(bars):
    out = []
    for b in bars:
        if not isinstance(b, dict):
            continue
        try:
            out.append({"o": float(b["o"]), "h": float(b["h"]),
                        "l": float(b["l"]), "c": float(b["c"]),
                        "t": int(b.get("t") or b.get("t_close_ms") or 0)})
        except Exception:
            continue
    return out


def _bars_to_df(bars):
    import pandas as pd
    return pd.DataFrame(bars)


def _f(x, n=2):
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
        print("pip install redis (or run in the app venv)"); sys.exit(1)
    try:
        from trend_sr import summarize_sr_multi_tf, _atr14
        from liq_structure import score_sr_with_liquidity
    except ImportError as e:
        print(f"Import failed: {e}\nRun from /opt/xauapi/api"); sys.exit(1)

    R = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASS)

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
    if not price:
        print("No price; pass --price <value>"); sys.exit(1)

    h1_raw, _ = _load_bars(R, dev, sym, "H1")
    h4_raw, _ = _load_bars(R, dev, sym, "H4")
    h1 = _norm_bars(h1_raw)
    h4 = _norm_bars(h4_raw)
    h1_df = _bars_to_df(h1)
    h4_df = _bars_to_df(h4)
    atr = _atr14(h1_df) if len(h1_df) else 0.0
    pip = 0.01 if sym == "XAUUSD" else (0.01 if sym.endswith("JPY") else 0.0001)

    print("=" * 72)
    print(f"BRIDGE VERIFY  {sym}  price={_f(price)}  H1={len(h1)} H4={len(h4)}  ATR={_f(atr)}")
    print("=" * 72)

    # 1) build the real SR bundle (no cache -> writes nothing)
    sr = summarize_sr_multi_tf(symbol=sym, price=price, h4_df=h4_df, h1_df=h1_df,
                               pip_factor=pip, cache=None)

    act_sup = sr.get("active_supports") or []
    act_res = sr.get("active_resistances") or []
    print(f"\nSR active supports: {len(act_sup)}   active resistances: {len(act_res)}")
    if not act_sup and not act_res:
        print("  (SR produced no active levels — check price/bars)")
        return

    # 2) run the bridge (liquidity bars are the raw dict lists)
    scored = score_sr_with_liquidity(sym, sr, h1, h4, price, atr)

    def _show(title, rows, side):
        print(f"\n########## {title} ##########")
        if not rows:
            print("  (none)")
            return
        for r in rows:
            lvl = r.get("level"); lo = r.get("low"); hi = r.get("high")
            q = r.get("quality_score"); ev = r.get("evidence", {})
            dist = (price - lvl) if side == "support" else (lvl - price)
            print(f"  {_f(lvl):>9}  band {_f(lo)}-{_f(hi)}  qScore={_f(q,1):>6}  dist={_f(dist)}")
            print(f"            {r.get('selection_reason','')}")
            # compact evidence line
            flags = []
            for k in ("htf_confluence","ob_overlap","fvg_overlap","liq_pool",
                      "fresh_swing","round_number","swept_reclaimed","broken","too_wide"):
                if ev.get(k):
                    extra = ""
                    if k == "ob_overlap": extra = f"(Q{ev.get('ob_quality')})"
                    if k == "liq_pool": extra = f"({ev.get('pool_touches')}x)"
                    flags.append(k + extra)
            print(f"            evidence: react={ev.get('reaction_atr')}ATR  " +
                  (", ".join(flags) if flags else "none"))

    _show("SCORED SUPPORTS (best first)", scored.get("scored_supports"), "support")
    _show("SCORED RESISTANCES (best first)", scored.get("scored_resistances"), "resistance")

    bs = scored.get("best_support"); br = scored.get("best_resistance")
    print("\n########## BEST PICKS ##########")
    print(f"  BEST SUPPORT    : {_f(bs.get('level')) if bs else 'None'}"
          f"  (q={_f(bs.get('quality_score'),1) if bs else '-'})")
    print(f"  BEST RESISTANCE : {_f(br.get('level')) if br else 'None'}"
          f"  (q={_f(br.get('quality_score'),1) if br else '-'})")

    print("\nVerify against your chart:")
    print("  - do the highest-scored levels sit on real reaction zones?")
    print("  - does a level with 'liq_pool' actually have equal highs/lows there?")
    print("  - does 'fvg_overlap' match a real unfilled gap on the chart?")
    print("  - is 'best_support'/'best_resistance' what YOU would pick by eye?")


if __name__ == "__main__":
    main()
