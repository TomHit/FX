#!/usr/bin/env python3
"""
verify_liq.py  —  READ-ONLY liquidity verification.

Pulls live XAUUSD H1/H4 bars from Redis (dev_2cb...), runs the REAL
liq_structure.py detectors, and prints each one WITH its lifecycle status
(fresh vs filled/mitigated/swept) so you can eyeball against your chart's
OB / FVG / sweep annotations.

Writes nothing. Touches no cache. Safe to run anytime.

Usage:
    cd /opt/xauapi/api && python3 verify_liq.py --price 4227
    python3 verify_liq.py --price 4227 --dev dev_2cb873afce2a43b6823e629c602b1120
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
    """Ensure each bar has o/h/l/c as floats (detectors expect these keys)."""
    out = []
    for b in bars:
        if not isinstance(b, dict):
            continue
        try:
            out.append({
                "o": float(b["o"]), "h": float(b["h"]),
                "l": float(b["l"]), "c": float(b["c"]),
                "t": int(b.get("t") or b.get("t_close_ms") or 0),
            })
        except Exception:
            continue
    return out


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
        import liq_structure as L
    except ImportError as e:
        print(f"Cannot import liq_structure: {e}")
        print("Run from /opt/xauapi/api"); sys.exit(1)

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

    h1, _ = _load_bars(R, dev, sym, "H1")
    h4, _ = _load_bars(R, dev, sym, "H4")
    h1 = _norm_bars(h1)
    h4 = _norm_bars(h4)

    atr = L._atr14(h1) if h1 else 0.0

    print("=" * 70)
    print(f"LIQUIDITY VERIFY  sym={sym}  price={_f(price)}  H1bars={len(h1)} H4bars={len(h4)} ATR={_f(atr)}")
    print("=" * 70)

    # ── ORDER BLOCKS (both directions) ──────────────────────────────
    print("\n########## ORDER BLOCKS ##########")
    for d, lbl in (("BUY", "bullish/support OB (below price)"),
                   ("SELL", "bearish/resistance OB (above price)")):
        obs = L.find_order_blocks(h1, d, atr=atr)
        print(f"\n-- {lbl} : {len(obs)} found (H1) --")
        for ob in obs[:8]:
            mid = (ob.get("low", 0) + ob.get("high", 0)) / 2
            side = "below" if mid < price else "above"
            print(f"   {_f(ob.get('low'))}-{_f(ob.get('high'))}  mid={_f(mid)} ({side} px)  "
                  f"Q={ob.get('quality')}  impAtr={ob.get('impulse_body_atr')}  {ob.get('label','')[:40]}")

    # ── FAIR VALUE GAPS (lifecycle: filled vs fresh) ────────────────
    print("\n\n########## FAIR VALUE GAPS (lifecycle: filled vs fresh) ##########")
    for d, lbl in (("BUY", "bullish FVG (support)"),
                   ("SELL", "bearish FVG (resistance)")):
        fvgs = L.find_fair_value_gaps(h1, d, atr=atr)
        fresh = [f for f in fvgs if not f.get("filled")]
        filled = [f for f in fvgs if f.get("filled")]
        print(f"\n-- {lbl} : {len(fresh)} FRESH, {len(filled)} filled (H1) --")
        for f in fresh[:6]:
            mid = f.get("mid")
            side = "below" if (mid and mid < price) else "above"
            print(f"   FRESH  {_f(f.get('low'))}-{_f(f.get('high'))} mid={_f(mid)} ({side} px)  "
                  f"gapAtr={f.get('gap_size_atr')}  status={f.get('fill_status')}")
        for f in filled[:3]:
            print(f"   filled {_f(f.get('low'))}-{_f(f.get('high'))}  (consumed -> should NOT be active)")

    # ── EQUAL HIGHS / LOWS (BSL / SSL pools) ────────────────────────
    print("\n\n########## EQUAL LEVELS (liquidity pools) ##########")
    for d, lbl in (("BUY", "SSL — equal lows below price"),
                   ("SELL", "BSL — equal highs above price")):
        eq = L.find_equal_levels(h1, atr, d)
        print(f"\n-- {lbl} : {len(eq)} found --")
        for e in eq[:6]:
            print(f"   {_f(e.get('level'))}  type={e.get('type')}  {e.get('label','')[:40]}")

    # ── UNTOUCHED / FRESH SWINGS ────────────────────────────────────
    print("\n\n########## FRESH (untouched) SWINGS ##########")
    for d, lbl in (("BUY", "fresh swing lows (SSL)"),
                   ("SELL", "fresh swing highs (BSL)")):
        sw = L.find_untouched_swings(h1, d)
        fresh = [s for s in sw if s.get("fresh")]
        print(f"\n-- {lbl} : {len(fresh)} fresh / {len(sw)} total --")
        for s in sw[:6]:
            tag = "FRESH" if s.get("fresh") else "used "
            print(f"   {tag}  {_f(s.get('level'))}  type={s.get('type')}")

    # ── SWEEP STATUS on nearest pools ───────────────────────────────
    print("\n\n########## SWEEP STATUS (nearest BSL/SSL) ##########")
    try:
        bs = L.find_nearest_bsl_ssl(h1, price, atr)
        for side in ("bsl", "ssl"):
            o = bs.get(side)
            if isinstance(o, dict) and o.get("level"):
                print(f"   {side.upper()} {_f(o.get('level'))}  swept={o.get('swept')}  "
                      f"candles_since={o.get('candles_since_sweep')}  reaction={o.get('reaction_after_sweep')}")
        print(f"   range_text: {bs.get('range_text','—')}")
    except Exception as e:
        print(f"   (bsl/ssl error: {e})")

    # ── ROUND NUMBERS ───────────────────────────────────────────────
    print("\n\n########## ROUND NUMBERS ##########")
    for d in ("BUY", "SELL"):
        rn = L.find_round_numbers(price, atr, sym, d)
        print(f"   {d}: " + ", ".join(_f(r.get('level')) for r in rn[:6]))

    print("\nDone. Compare OB/FVG against your chart's OB/FVG annotations.")
    print("Key lifecycle checks: FRESH FVGs should be unfilled gaps still visible;")
    print("filled FVGs should be ones price already traded back through.")


if __name__ == "__main__":
    main()
