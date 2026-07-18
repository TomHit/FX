#!/usr/bin/env python3
"""
backfill_excursion.py

One-time backfill of mfe_r / mae_r on historical CLOSED trades in trades.jsonl.

Why this exists:
  Older rows were written before the seconds-vs-milliseconds timestamp fix in
  _excursion_r, so their excursion loop skipped every fetched H1 bar and left
  mfe_r = 0.0 (and mae_r truncated). This recomputes those rows from the H1
  snapshot bars in Redis, using the SAME _excursion_r the live path now uses.

Correctness guards (learned the hard way):
  1. COVERAGE  - only touch a row if the H1 snapshot actually spans
                 [entry_enqueue -> broker_close]. Otherwise skip + flag; never
                 overwrite with a value computed from bars that don't cover the
                 trade.
  2. WINDOW    - clip bars by INTERVAL OVERLAP ([open, open+1H] vs [entry,close]),
                 not by open-time falling inside the window. This keeps the entry
                 bar and the close bar, which a naive `start <= t <= end` drops.
  3. INTRABAR  - H1 bars are blind to intrabar exits (manual close / moved TP/SL
                 that fills mid-bar at a level no H1 high/low reached). When the
                 broker-realized R exceeds what H1 extremes captured, FLOOR the
                 excursion to the realized value (broker truth) and flag it
                 `intrabar_unresolvable` rather than writing a misleading H1 number.
  4. SINGLE-BAR- sub-hour trades span one H1 bar; excursion is not meaningful.
                 Flag `single_bar`.

Every changed row gets:
  excursion_precision   : 'h1_bar' | 'intrabar_unresolvable' | 'single_bar'
  excursion_source      : 'h1_bar_approx_backfill'
  excursion_backfilled  : True
  excursion_backfill_note (optional)

Usage:
  python3 backfill_excursion.py            # DRY RUN, writes nothing
  python3 backfill_excursion.py --apply    # backs up original, writes .backfilled

After --apply: review the .backfilled file, then `mv` it over the original.
"""

import sys
import json
import shutil
import datetime

sys.path.insert(0, '/opt/xauapi/api')
import xtl_analytics as X          # noqa: E402  (uses the live, fixed _excursion_r)
import redis                       # noqa: E402

# ---- config -----------------------------------------------------------------
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_PW   = 'xau12345'
SRC        = '/opt/xauapi/api/trend/out/trades.jsonl'
HOUR       = 3600_000              # one H1 bar, in ms
MFE_VERIFY_THRESHOLD = 4.0         # flag (don't block) MFE above this for eyeball

DRY = '--apply' not in sys.argv
R = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PW,
                decode_responses=True)


# ---- helpers ----------------------------------------------------------------
def norm_ms(v):
    """Bars store 't' in seconds (10-digit); trade timestamps are ms (13-digit)."""
    v = int(v or 0)
    return v * 1000 if 0 < v < 10_000_000_000 else v


def bar_t(b):
    return norm_ms(b.get('t_close_ms') or b.get('t_open_ms') or b.get('t') or 0)


def overlaps(b, start, end):
    """Bar interval [open, open+1H] overlaps trade interval [entry, close]."""
    o = bar_t(b)
    c = o + HOUR
    return o < end and c > start


def load_bars(dev, sym):
    if not dev or not sym:
        return []
    raw = R.get(f"xtl:ohlc:snap:{dev}:{sym.upper()}:H1")
    if not raw:
        return []
    d = json.loads(raw)
    return d.get('bars') or (d if isinstance(d, list) else [])


def span_ok(bars, s, e):
    if not bars:
        return False
    ts = [bar_t(b) for b in bars]
    return min(ts) <= s and max(ts) >= e


# ---- main -------------------------------------------------------------------
def main():
    rows = []
    changed = skipped_cov = skipped_ok = errors = 0
    n_h1 = n_intrabar = n_single = 0

    for line in open(SRC):
        line = line.strip()
        if not line:
            continue
        try:
            r = json.loads(line)
        except Exception:
            errors += 1
            continue

        if r.get('_status') != 'closed':
            rows.append(r)
            continue

        mfe = r.get('mfe_r')
        rr = r.get('realized_r')
        needs = (mfe in (None, 0.0)) or (r.get('mae_r') is None)
        winner_zeroed = (mfe in (None, 0.0)) and (rr is not None and rr > 0.05)
        if not (needs or winner_zeroed):
            skipped_ok += 1
            rows.append(r)
            continue

        entry = r.get('entry_price')
        sl = r.get('sl_price')
        start = norm_ms(r.get('enqueue_timestamp'))
        end = norm_ms(r.get('broker_close_time_utc_ms') or r.get('close_timestamp'))
        if not (entry and sl and start and end and end > start):
            skipped_cov += 1
            rows.append(r)
            continue

        bars = load_bars(r.get('device_id'), r.get('symbol'))
        if not span_ok(bars, start, end):
            r['excursion_backfill_note'] = 'SKIPPED_NO_COVERAGE'
            skipped_cov += 1
            rows.append(r)
            continue

        win = [b for b in bars if overlaps(b, start, end)]
        if not win:
            r['excursion_backfill_note'] = 'SKIPPED_EMPTY_WINDOW'
            skipped_cov += 1
            rows.append(r)
            continue

        try:
            exc = X._excursion_r(r, win, float(entry), float(sl))
        except Exception:
            errors += 1
            rows.append(r)
            continue
        if not (exc and exc.get('mfe_r') is not None):
            rows.append(r)
            continue

        new_mfe = exc['mfe_r']
        new_mae = exc['mae_r']
        precision = 'h1_bar'
        note = None

        if len(win) <= 1:
            precision = 'single_bar'
            note = 'sub_hour_one_bar'
            n_single += 1
        else:
            # intrabar exit: broker realized exceeds what H1 extremes captured
            if rr is not None and rr > 0 and new_mfe < rr - 0.02:
                precision = 'intrabar_unresolvable'
                note = 'mfe_floored_to_realized'
                new_mfe = round(rr, 2)
                n_intrabar += 1
            elif rr is not None and rr < 0 and new_mae > rr + 0.02:
                precision = 'intrabar_unresolvable'
                note = 'mae_floored_to_realized'
                new_mae = round(rr, 2)
                n_intrabar += 1
            else:
                n_h1 += 1
            if new_mfe > MFE_VERIFY_THRESHOLD:
                note = (note + '|' if note else '') + 'mfe_high_verified'

        span_h = round((end - start) / HOUR, 1)
        print(f"{r.get('mt5_ticket'):>12} {r.get('side'):4} rr={rr}  "
              f"mfe {mfe}->{new_mfe}  mae {r.get('mae_r')}->{new_mae}  "
              f"bars={len(win)} span={span_h}h  [{precision}]"
              f"{'  ' + note if note else ''}")

        r['mfe_r'] = new_mfe
        r['mae_r'] = new_mae
        r['excursion_precision'] = precision
        if note:
            r['excursion_backfill_note'] = note
        r['excursion_backfilled'] = True
        r['excursion_source'] = 'h1_bar_approx_backfill'
        changed += 1
        rows.append(r)

    print("\n--- summary ---")
    print(f"changed              : {changed}")
    print(f"  trustworthy H1     : {n_h1}")
    print(f"  intrabar (floored) : {n_intrabar}")
    print(f"  single-bar (flag)  : {n_single}")
    print(f"skipped (already ok) : {skipped_ok}")
    print(f"skipped (no coverage): {skipped_cov}")
    print(f"errors               : {errors}")
    print(f"total rows           : {len(rows)}")

    if DRY:
        print("\nDRY RUN - nothing written. Re-run with --apply to commit.")
        return

    stamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    backup = f"{SRC}.bak_{stamp}"
    out = SRC + '.backfilled'
    shutil.copy(SRC, backup)
    with open(out, 'w') as f:
        for r in rows:
            f.write(json.dumps(r) + '\n')
    print(f"\nBackup:  {backup}")
    print(f"Written: {out}")
    print(f"Review, then commit with: mv {out} {SRC}")


if __name__ == '__main__':
    main()
