#!/usr/bin/env python3
"""
backfill_exits.py — one-time backfill of trade exits from stored MT5 broker deals.

WHAT IT DOES
------------
For each row in trades.jsonl that has a matching broker deal in Redis
(xtl:mt5:deal:{ticket}), re-resolve the exit from the REAL broker close
(close_price / net_profit / broker_reason) and rewrite the row with correct:
    exit_source = broker_deal
    exit_reason = tp / sl / manual  (from broker_reason, else close-vs-levels)
    exit_price  = real close_price
    net_profit  = real net_profit
    realized_r  = real R from actual close

WHAT IT DOES NOT DO (honest limitations)
----------------------------------------
- Does NOT touch rows without a matching deal (RoboForex 21xx, expired deals) —
  left exactly as-is.
- Does NOT fabricate macro/bias fields. Those must be captured AT ENTRY; the
  entry-time OHLC for old trades is gone, so stamping today's macro would be
  wrong. Old rows stay without macro — honestly.
- Does NOT recompute MFE/MAE (needs entry->exit bars that may have scrolled out).
  Existing excursion values are left untouched.

SAFETY
------
- Reads trades.jsonl, writes to trades_backfilled.jsonl (NEW file).
- NEVER overwrites the original. You inspect the output, then swap manually.
- Prints a summary of what changed so you can verify before replacing.
"""

import json
import os
import sys
import time

sys.path.insert(0, "/opt/xauapi")

JSONL_IN  = "/opt/xauapi/api/trend/out/trades.jsonl"
JSONL_OUT = "/opt/xauapi/api/trend/out/trades_backfilled.jsonl"


def _safe_float(v, default=None):
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def resolve_from_deal(R, ticket: str, row: dict):
    """Return exit fields from the broker deal, or None if no usable deal."""
    try:
        raw = R.get(f"xtl:mt5:deal:{ticket}")
        if not raw:
            return None
        deal = json.loads(raw)

        close_price = _safe_float(deal.get("close_price"))
        if close_price is None:
            return None
        net_profit = _safe_float(deal.get("net_profit"))

        entry = _safe_float(row.get("entry_price"))
        sl    = _safe_float(row.get("sl_price"))
        tp    = _safe_float(row.get("tp_price"))
        side  = (row.get("side") or "").upper()

        # realized R from the REAL close
        realized_r = None
        if entry and sl:
            risk = abs(entry - sl)
            if risk > 0:
                realized_r = ((close_price - entry) if side == "BUY"
                              else (entry - close_price)) / risk

        # classification: prefer the broker's own reason if present
        broker_reason = str(deal.get("broker_reason") or "").upper()
        if broker_reason == "TP":
            exit_reason = "tp"
        elif broker_reason == "SL":
            exit_reason = "sl"
        else:
            # fall back to close-vs-levels, then net_profit sign
            tol = (abs(entry - sl) * 0.10) if (entry and sl) else 0.0
            if tp is not None and abs(close_price - tp) <= tol:
                exit_reason = "tp"
            elif sl is not None and abs(close_price - sl) <= tol:
                exit_reason = "sl"
            elif net_profit is not None and net_profit > 0:
                exit_reason = "tp"
            else:
                exit_reason = "manual"

        return {
            "exit_source": "broker_deal_backfill",
            "exit_confidence": "high",
            "exit_price": round(close_price, 5),
            "exit_reason": exit_reason,
            "realized_r": round(realized_r, 3) if realized_r is not None else None,
            "net_profit": net_profit,
            "close_timestamp": int(deal.get("close_time_ms") or row.get("close_timestamp") or 0),
            "backfilled_at_ms": int(time.time() * 1000),
        }
    except Exception as e:
        print(f"  ! error resolving {ticket}: {e}")
        return None


def main():
    from api.trend_endpoints import R

    if not os.path.exists(JSONL_IN):
        print(f"Input not found: {JSONL_IN}")
        return

    total = 0
    backfilled = 0
    skipped_no_deal = 0
    already_broker = 0
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
                f_out.write(line + "\n")   # preserve unparseable lines verbatim
                continue

            ticket = str(row.get("mt5_ticket") or "").strip()
            src = row.get("exit_source") or ""

            # Skip rows already resolved from a live broker deal (forward trades)
            if src == "broker_deal":
                already_broker += 1
                f_out.write(json.dumps(row) + "\n")
                continue

            if not ticket:
                skipped_no_deal += 1
                f_out.write(json.dumps(row) + "\n")
                continue

            fix = resolve_from_deal(R, ticket, row)
            if fix is None:
                skipped_no_deal += 1
                f_out.write(json.dumps(row) + "\n")
                continue

            # record the change for the summary
            changes.append({
                "ticket": ticket,
                "symbol": row.get("symbol"),
                "old_reason": row.get("exit_reason"),
                "old_net": row.get("net_profit"),
                "new_reason": fix["exit_reason"],
                "new_net": fix["net_profit"],
                "new_r": fix["realized_r"],
            })
            row.update(fix)
            backfilled += 1
            f_out.write(json.dumps(row) + "\n")

    # ---- summary ----
    print("=" * 60)
    print(f"BACKFILL COMPLETE  ->  {JSONL_OUT}")
    print(f"  total rows           : {total}")
    print(f"  backfilled (fixed)   : {backfilled}")
    print(f"  already broker_deal  : {already_broker}")
    print(f"  skipped (no deal)    : {skipped_no_deal}")
    print("=" * 60)
    if changes:
        print("CHANGES (old -> new):")
        for c in changes:
            print(f"  {c['ticket']} {c['symbol']:7} "
                  f"{str(c['old_reason']):7}/{str(c['old_net']):8}  ->  "
                  f"{c['new_reason']:7}/{c['new_net']:8}  r={c['new_r']}")
    print()
    print("Original trades.jsonl is UNCHANGED.")
    print("Inspect trades_backfilled.jsonl above. If correct, swap with:")
    print(f"  cp {JSONL_IN} {JSONL_IN}.bak   # backup first")
    print(f"  mv {JSONL_OUT} {JSONL_IN}")


if __name__ == "__main__":
    main()
