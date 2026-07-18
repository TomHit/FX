#!/usr/bin/env python3
"""
backfill_gap.py — stamp gap context onto trades that closed before gap capture
was wired into build_entry_snapshot.

WHAT IT DOES
------------
For each row in trades.jsonl that has NO gap fields, finds the last market-closure
gap for that symbol and — only if the trade entered AFTER that gap — stamps:
    gap_pct, gap_pips, gap_direction, gap_is_significant,
    hours_since_gap, gap_recent, gap_trade_type

WHY THE "ENTERED AFTER" GUARD MATTERS
-------------------------------------
last_gap() returns the MOST RECENT gap. For a trade from last Thursday that would
be the *following* weekend's gap — a gap that had not happened yet when the trade
was placed. Stamping it would be worse than leaving the field empty. So any row
whose entry predates gap_open_ms is skipped, honestly, and left as-is.

hours_since_gap is computed from the trade's OWN entry timestamp, not from now.

SAFETY
------
Writes trades_gapfilled.jsonl. Never touches the original. Inspect, then swap.
"""

import json
import os
import sys

sys.path.insert(0, "/opt/xauapi")

JSONL_IN = "/opt/xauapi/api/trend/out/trades.jsonl"
JSONL_OUT = "/opt/xauapi/api/trend/out/trades_gapfilled.jsonl"


def main():
    from api.trend_endpoints import R
    from api.gap_detect import last_gap, GAP_RECENT_H

    if not os.path.exists(JSONL_IN):
        print(f"Input not found: {JSONL_IN}")
        return

    gap_cache = {}

    def gap_for(symbol):
        if symbol not in gap_cache:
            gap_cache[symbol] = last_gap(R, symbol)
        return gap_cache[symbol]

    total = 0
    stamped = 0
    already = 0
    skipped_before_gap = 0
    skipped_no_gap = 0
    changes = []

    with open(JSONL_IN) as f_in, open(JSONL_OUT, "w") as f_out:
        for line in f_in:
            line = line.strip()
            if not line:
                continue
            total += 1
            try:
                row = json.loads(line)
            except Exception:
                f_out.write(line + "\n")
                continue

            if row.get("gap_pct") is not None:
                already += 1
                f_out.write(json.dumps(row) + "\n")
                continue

            symbol = str(row.get("symbol") or "").upper()
            side = str(row.get("side") or "").upper()
            entry_ts = int(row.get("enqueue_timestamp") or row.get("ts_ms") or 0)

            g = gap_for(symbol) if symbol else None
            if not g or not entry_ts:
                skipped_no_gap += 1
                f_out.write(json.dumps(row) + "\n")
                continue

            # the gap must have happened BEFORE this trade was placed
            if entry_ts <= int(g["gap_open_ms"]):
                skipped_before_gap += 1
                f_out.write(json.dumps(row) + "\n")
                continue

            hours_since = (entry_ts - int(g["gap_open_ms"])) / 3_600_000.0
            recent = hours_since <= GAP_RECENT_H

            trade_dir = 1 if side == "BUY" else (-1 if side == "SELL" else 0)
            gap_dir = 1 if g["gap_direction"] == "up" else (-1 if g["gap_direction"] == "down" else 0)
            gap_trade_type = "n/a"
            if recent and g["gap_is_significant"] and trade_dir and gap_dir:
                gap_trade_type = "continuation" if trade_dir == gap_dir else "fade"

            fix = {
                "gap_pct": g["gap_pct"],
                "gap_pips": g["gap_pips"],
                "gap_direction": g["gap_direction"],
                "gap_is_significant": g["gap_is_significant"],
                "hours_since_gap": round(hours_since, 1),
                "gap_recent": recent,
                "gap_trade_type": gap_trade_type,
                "gap_backfilled": True,
            }
            row.update(fix)
            stamped += 1
            changes.append({
                "ticket": row.get("mt5_ticket"),
                "symbol": symbol,
                "side": side,
                "reason": row.get("exit_reason"),
                "gap": f"{g['gap_direction']} {g['gap_pct']*100:+.3f}%",
                "sig": "SIG" if g["gap_is_significant"] else "noise",
                "hrs": round(hours_since, 1),
                "type": gap_trade_type,
            })
            f_out.write(json.dumps(row) + "\n")

    print("=" * 72)
    print(f"GAP BACKFILL  ->  {JSONL_OUT}")
    print(f"  total rows              : {total}")
    print(f"  stamped                 : {stamped}")
    print(f"  already had gap fields  : {already}")
    print(f"  skipped (entry pre-gap) : {skipped_before_gap}")
    print(f"  skipped (no gap found)  : {skipped_no_gap}")
    print("=" * 72)
    if changes:
        print("STAMPED:")
        for c in changes:
            print(f"  {c['ticket']} {c['symbol']:7} {c['side']:4} {str(c['reason']):7} "
                  f"| gap {c['gap']:16} {c['sig']:5} | {c['hrs']:5}h after | {c['type']}")
    print()
    print("Original trades.jsonl UNCHANGED. If correct, swap with:")
    print(f"  cp {JSONL_IN} {JSONL_IN}.bak")
    print(f"  mv {JSONL_OUT} {JSONL_IN}")


if __name__ == "__main__":
    main()
