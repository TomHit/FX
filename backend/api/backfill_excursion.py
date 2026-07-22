#!/usr/bin/env python3
"""
XTL — excursion + derived-field backfill.

Fixes, for every row in trades.jsonl:

  1. MFE/MAE window. `_excursion_r` compares `enqueue_timestamp` (server UTC)
     against H1 bar timestamps (broker wall, UTC+offset). The window therefore
     opened `offset` minutes early — 3h of pre-trade bars counted as excursion.
     And at finalize the exit bar had not closed yet, so it was absent from the
     OHLC snapshot entirely and the stop wick was never seen.

  2. Derived fields that were never populated: realized_r_net, planned_rr,
     efficiency.

  3. capture_status.exit_snapshot_complete, which was set to a bare True.

Everything is computed in TRUE UTC. Bars are normalized on read.

Idempotent: rows already at EXCURSION_VERSION are skipped unless --force.
Dry run by default. Writes atomically, backs up first.

    python3 backfill_excursion.py                 # report only
    python3 backfill_excursion.py --apply         # write
    python3 backfill_excursion.py --apply --ticket 5620700
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import tempfile
import time
from datetime import datetime, timezone

JSONL_PATH = "/opt/xauapi/api/trend/out/trades.jsonl"
REDIS_URL = os.getenv("REDIS_URL", "redis://default:xau12345@10.0.0.132:6379/0")
OHLC_KEY = "xtl:ohlc:snap:{dev}:{sym}:H1"

EXCURSION_VERSION = 2
DEFAULT_OFFSET_MIN = 180          # EEST. Valid for the whole 2026-06-29..07-22 range.
BAR_MS = 3_600_000


def _f(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%m-%d %H:%M")


def _safe_float(x, d=None):
    try:
        return float(x)
    except Exception:
        return d


def _pip(symbol):
    s = (symbol or "").upper()
    return 0.01 if (s == "XAUUSD" or s.endswith("JPY")) else 0.0001


# ---------------------------------------------------------------------------
# Bars
# ---------------------------------------------------------------------------

def load_bars_utc(R, symbol, device, offset_min, cache):
    """Closed H1 bars with timestamps normalized to true UTC.

    Raw `t` is seconds in broker wall time. Returns bar OPEN in UTC; a bar
    covers [t_open_utc, t_open_utc + 1h).
    """
    ck = (device, symbol)
    if ck in cache:
        return cache[ck]

    out = []
    try:
        raw = R.get(OHLC_KEY.format(dev=device, sym=(symbol or "").upper()))
        js = json.loads(raw) if raw else None
        if isinstance(js, str):
            js = json.loads(js)
        for b in (js or {}).get("bars") or (js or {}).get("ohlc") or []:
            if not isinstance(b, dict) or b.get("complete") is False:
                continue
            t = b.get("t")
            h, l = _safe_float(b.get("h")), _safe_float(b.get("l"))
            if t is None or h is None or l is None:
                continue
            t = float(t)
            t_ms = int(t * 1000) if t < 10_000_000_000 else int(t)
            out.append({"t": t_ms - offset_min * 60_000, "h": h, "l": l})
        out.sort(key=lambda x: x["t"])
    except Exception as e:
        print(f"    ! bar load failed {symbol}/{device}: {e}")

    cache[ck] = out
    return out


# ---------------------------------------------------------------------------
# Window resolution — close_timestamp is written in three different domains
# ---------------------------------------------------------------------------

def resolve_window_utc(row, offset_min):
    """(start_utc_ms, end_utc_ms, provenance) — or (None, None, reason)."""
    start = int(row.get("enqueue_timestamp") or 0)   # always server UTC
    if start <= 0:
        return None, None, "no_enqueue_timestamp"

    # 1. Explicitly normalized — trust it.
    utc = row.get("broker_close_time_utc_ms")
    if utc:
        return start, int(utc), "broker_close_time_utc_ms"

    ct = int(row.get("close_timestamp") or 0)
    if ct <= 0:
        return None, None, "no_close_timestamp"

    src = str(row.get("exit_source") or "")
    reason = str(row.get("exit_reason") or "")

    # 2. approximate_exit with a level hit stamps a BAR timestamp (broker domain);
    #    with no hit it stamps _now_ms() (already UTC).
    if src == "h1_bar_approx":
        if reason in ("sl", "tp"):
            return start, ct - offset_min * 60_000, "approx_hit_ts_shifted"
        return start, ct, "approx_now_ms_utc"

    # 3. Every broker path writes raw broker wall time.
    return start, ct - offset_min * 60_000, "close_timestamp_shifted"


# ---------------------------------------------------------------------------
# Excursion
# ---------------------------------------------------------------------------

def excursion(row, bars, start_utc, end_utc):
    entry = _safe_float(row.get("entry_price"))
    sl = _safe_float(row.get("sl_price"))
    if entry is None or sl is None:
        return {}, "no_entry_or_sl"
    risk = abs(entry - sl)
    if risk <= 0:
        return {}, "zero_risk"

    side = (row.get("side") or "").upper()

    # A bar is in-window if its span intersects the trade's lifetime. The bar
    # holding the exit is included — its wick carries the stop fill. Sub-hour
    # resolution cannot separate pre- from post-exit movement inside that bar,
    # which is why precision is capped at h1 granularity.
    win = [b for b in bars if b["t"] + BAR_MS > start_utc and b["t"] <= end_utc]
    if not win:
        return {}, "no_bars_in_window"

    best = worst = 0.0
    bp = wp = bt = wt = bi = wi = None
    for i, b in enumerate(win):
        if side == "BUY":
            fav, adv = (b["h"] - entry) / risk, (b["l"] - entry) / risk
            fp, ap = b["h"], b["l"]
        else:
            fav, adv = (entry - b["l"]) / risk, (entry - b["h"]) / risk
            fp, ap = b["l"], b["h"]
        if fav > best:
            best, bp, bt, bi = fav, fp, b["t"], i
        if adv < worst:
            worst, wp, wt, wi = adv, ap, b["t"], i

    pipf = _pip(row.get("symbol"))
    out = {
        "mfe_r": round(best, 2),
        "mae_r": round(worst, 2),
        "excursion_source": "h1_bar",
        "excursion_precision": "low" if len(win) <= 3 else "medium",
        "excursion_bars_used": len(win),
        "excursion_version": EXCURSION_VERSION,
        "excursion_window_start_utc_ms": start_utc,
        "excursion_window_end_utc_ms": end_utc,
    }
    if bp is not None:
        out |= {"mfe_price": round(bp, 5),
                "mfe_pips": round(abs(bp - entry) / pipf, 1),
                "mfe_bar_ts_ms": bt, "mfe_bars_after_entry": bi}
    if wp is not None:
        out |= {"mae_price": round(wp, 5),
                "mae_pips": round(abs(wp - entry) / pipf, 1),
                "mae_bar_ts_ms": wt, "mae_bars_after_entry": wi}
    return out, "ok"


def derived(row, exc):
    """Pure-arithmetic fields. No bars, no broker."""
    out = {}
    np_, ru = _safe_float(row.get("net_profit")), _safe_float(row.get("risk_usd"))
    if np_ is not None and ru:
        out["realized_r_net"] = round(np_ / ru, 3)

    e, sl, tp = (_safe_float(row.get("entry_price")),
                 _safe_float(row.get("sl_price")),
                 _safe_float(row.get("tp_price")))
    if e and sl and tp and abs(e - sl) > 0:
        side = (row.get("side") or "").upper()
        out["planned_rr"] = round(((tp - e) if side == "BUY" else (e - tp)) / abs(e - sl), 3)

    rr = _safe_float(row.get("realized_r"))
    mfe = exc.get("mfe_r", row.get("mfe_r"))
    if rr is not None and mfe and mfe > 0:
        out["efficiency"] = round(rr / mfe, 3)

    cs = dict(row.get("capture_status") or {})
    cs["exit_snapshot_complete"] = bool(
        row.get("exit_price") is not None
        and row.get("outcome") not in (None, "UNKNOWN")
        and row.get("realized_r") is not None
    )
    out["capture_status"] = cs
    return out


# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="write (default: dry run)")
    ap.add_argument("--force", action="store_true", help="redo rows already at v%d" % EXCURSION_VERSION)
    ap.add_argument("--ticket", help="single ticket only")
    ap.add_argument("--offset", type=int, default=DEFAULT_OFFSET_MIN)
    ap.add_argument("--file", default=JSONL_PATH)
    a = ap.parse_args()

    import redis
    R = redis.from_url(REDIS_URL, decode_responses=True)

    rows = [json.loads(l) for l in open(a.file) if l.strip()]
    print(f"{len(rows)} rows | offset {a.offset}m | {'APPLY' if a.apply else 'DRY RUN'}\n")

    cache, stats, changed = {}, {}, 0
    print(f"{'ticket':<12} {'sym':<8} {'window (UTC)':<26} {'mae_r':>14} {'mfe_r':>14}")
    print("-" * 82)

    for row in rows:
        tk = str(row.get("mt5_ticket") or "")
        if a.ticket and tk != a.ticket:
            continue
        if row.get("excursion_version") == EXCURSION_VERSION and not a.force:
            stats["skip_current"] = stats.get("skip_current", 0) + 1
            continue

        off = row.get("broker_tz_offset_minutes")
        off = int(off) if off is not None else a.offset

        start, end, prov = resolve_window_utc(row, off)
        if start is None or not end or end <= start:
            stats[f"skip_{prov}"] = stats.get(f"skip_{prov}", 0) + 1
            print(f"{tk:<12} {str(row.get('symbol')):<8} SKIP: {prov}")
            continue

        bars = load_bars_utc(R, row.get("symbol"), row.get("device_id"), off, cache)
        exc, why = excursion(row, bars, start, end)
        if why != "ok":
            stats[f"skip_{why}"] = stats.get(f"skip_{why}", 0) + 1
            print(f"{tk:<12} {str(row.get('symbol')):<8} SKIP: {why}")
            continue

        exc["excursion_window_source"] = prov
        upd = exc | derived(row, exc)

        print(f"{tk:<12} {str(row.get('symbol')):<8} "
              f"{_f(start)}->{_f(end)} ({(end-start)/60000:>5.0f}m) "
              f"{str(row.get('mae_r')):>6}->{exc['mae_r']:>6} "
              f"{str(row.get('mfe_r')):>6}->{exc['mfe_r']:>6}")

        row.update(upd)
        changed += 1
        stats["updated"] = stats.get("updated", 0) + 1

    print("-" * 82)
    for k, v in sorted(stats.items()):
        print(f"  {k}: {v}")

    if not a.apply:
        print(f"\nDRY RUN — {changed} rows would change. Re-run with --apply.")
        return 0
    if not changed:
        print("\nnothing to write")
        return 0

    bak = f"{a.file}.bak-{time.strftime('%Y%m%d-%H%M%S')}"
    shutil.copy2(a.file, bak)
    st = os.stat(a.file)
    d = os.path.dirname(a.file)
    fd, tmp = tempfile.mkstemp(dir=d, prefix=".backfill-", suffix=".jsonl")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            for r in rows:
                f.write(json.dumps(r, separators=(",", ":")) + "\n")
            f.flush()
            os.fsync(f.fileno())
        os.chmod(tmp, st.st_mode)
        try:
            os.chown(tmp, st.st_uid, st.st_gid)
        except PermissionError:
            pass
        os.replace(tmp, a.file)
    except Exception:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise

    print(f"\nwrote {changed} rows | backup: {bak}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
