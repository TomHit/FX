#!/usr/bin/env python3
"""
XTL SR / Zone level extractor — for plotting on chart and validating best-vs-nearest.

Pulls per-symbol from the opportunities endpoint:
  best_support / best_resistance (+ band low/high, quality_score)
  nearest_support / nearest_resistance  (for the best-vs-nearest comparison)
  resolved_dir, zone_source, actionable_cap, watch flags (to spot masking)

Outputs:
  1) a readable table to stdout
  2) xtl_levels.csv  — flat rows for importing into a charting tool
     (one row per plottable line/zone: symbol, side, kind, level, low, high, qs, is_best)

Usage:
  python3 xtl_extract_levels.py
  python3 xtl_extract_levels.py --tf H1 --url http://127.0.0.1:8000
  python3 xtl_extract_levels.py --symbols XAUUSD,EURUSD,USDJPY
"""

import argparse, csv, json, sys, urllib.request

def get(url):
    with urllib.request.urlopen(url, timeout=20) as r:
        return json.load(r)

def f(v):
    try:
        x = float(v)
        return x if x == x else None  # drop NaN
    except Exception:
        return None

def band(d):
    """Return (level, low, high, qs) from a level dict, tolerating missing band."""
    if not isinstance(d, dict):
        return (None, None, None, None)
    return (f(d.get("level")), f(d.get("low")), f(d.get("high")), f(d.get("quality_score")))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://127.0.0.1:8000")
    ap.add_argument("--tf", default="H1")
    ap.add_argument("--symbols", default="", help="comma-separated filter, e.g. XAUUSD,EURUSD")
    ap.add_argument("--debug_gate", default="1")
    ap.add_argument("--csv", default="xtl_levels.csv")
    args = ap.parse_args()

    want = {s.strip().upper() for s in args.symbols.split(",") if s.strip()}
    endpoint = f"{args.url.rstrip('/')}/trend/opportunities?tf={args.tf}&debug_gate={args.debug_gate}"

    try:
        data = get(endpoint)
    except Exception as e:
        print(f"ERROR fetching {endpoint}: {type(e).__name__}: {e}", file=sys.stderr)
        sys.exit(1)

    rows = data.get("rows") or []
    csv_rows = []
    print(f"\nendpoint: {endpoint}")
    print(f"symbols : {len(rows)} rows returned\n")
    hdr = ("SYM", "dir", "src", "best_sup", "qs", "near_sup", "best_res", "qs", "near_res", "cap", "masked?")
    print("{:<8}{:<6}{:<16}{:>10}{:>6}{:>10}{:>10}{:>6}{:>10}{:>9}  {}".format(*hdr))
    print("-" * 110)

    for r in rows:
        sym = str(r.get("symbol") or "").upper()
        if want and sym not in want:
            continue
        sr = r.get("sr") or {}
        g  = r.get("entry_gate") or {}
        pz = g.get("zone") or {}

        bs_lvl, bs_lo, bs_hi, bs_qs = band(sr.get("best_support"))
        br_lvl, br_lo, br_hi, br_qs = band(sr.get("best_resistance"))
        near_sup = f((sr.get("nearest_support") if not isinstance(sr.get("nearest_support"), dict)
                      else (sr.get("nearest_support") or {}).get("level")))
        near_res = f((sr.get("nearest_resistance") if not isinstance(sr.get("nearest_resistance"), dict)
                      else (sr.get("nearest_resistance") or {}).get("level")))
        # nearest_levels endpoint shape sometimes nests differently; also try flat keys
        if near_sup is None: near_sup = f(sr.get("nearest_support"))
        if near_res is None: near_res = f(sr.get("nearest_resistance"))

        rdir = g.get("resolved_dir")
        src  = pz.get("zone_source")
        cap  = f(g.get("actionable_cap"))
        masked = bool(g.get("watch_reused") or g.get("frozen_zone"))
        masked_s = "WATCH/" + str(g.get("reason") or "")[:18] if masked else ""

        def s(x, p=5):
            return f"{x:.{p}f}" if isinstance(x, (int, float)) else "-"

        print("{:<8}{:<6}{:<16}{:>10}{:>6}{:>10}{:>10}{:>6}{:>10}{:>9}  {}".format(
            sym, str(rdir or "-"), str(src or "-"),
            s(bs_lvl), s(bs_qs,1), s(near_sup),
            s(br_lvl), s(br_qs,1), s(near_res),
            s(cap), masked_s))

        # CSV: one plottable row per line/zone
        if bs_lvl is not None:
            csv_rows.append([sym, "BUY", "support", "best", bs_lvl, bs_lo, bs_hi, bs_qs])
        if near_sup is not None:
            csv_rows.append([sym, "BUY", "support", "nearest", near_sup, "", "", ""])
        if br_lvl is not None:
            csv_rows.append([sym, "SELL", "resistance", "best", br_lvl, br_lo, br_hi, br_qs])
        if near_res is not None:
            csv_rows.append([sym, "SELL", "resistance", "nearest", near_res, "", "", ""])

    with open(args.csv, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["symbol", "side", "kind", "is_best", "level", "low", "high", "quality_score"])
        w.writerows(csv_rows)

    print("-" * 110)
    print(f"\nwrote {len(csv_rows)} plottable rows -> {args.csv}")
    print("Plot tip: draw 'best' rows as bold lines + shaded box (low..high), 'nearest' as a thin dashed line.")
    print("VALIDATE: does each 'best' level sit on stronger structure than the 'nearest' it beat?\n")

if __name__ == "__main__":
    main()
