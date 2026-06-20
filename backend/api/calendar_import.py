# -*- coding: utf-8 -*-
"""
XauTrendLab — Calendar Import
==============================
Runs on Oracle server.
Reads events.csv uploaded from local machine → stores in Redis.

Usage:
    python -m api.calendar_import
    python -m api.calendar_import --file /opt/xauapi/events.csv

After upload from local machine:
    ssh ubuntu@SERVER 'cd /opt/xauapi && /opt/xauapi/venv/bin/python -m api.calendar_import'
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import List, Optional

log = logging.getLogger("xtl.calendar_import")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DEFAULT_CSV_PATH  = "/opt/xauapi/events.csv"
REDIS_CALENDAR_KEY = "xtl:news:calendar:daily"
REDIS_CALENDAR_TTL = 8 * 3600   # 8 hours

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_ms() -> int:
    return int(time.time() * 1000)


def _parse_dt_utc(dt_str: str) -> Optional[int]:
    """Parse datetime string to UTC milliseconds."""
    try:
        dt_str = str(dt_str or "").strip()
        if not dt_str:
            return None
        # Format: "2026-06-06 12:30:00"
        dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S").replace(
            tzinfo=timezone.utc
        )
        return int(dt.timestamp() * 1000)
    except Exception:
        return None


def _validate_event(row: dict) -> Optional[dict]:
    """Validate and normalize a CSV row into an event dict."""
    try:
        event_name = str(row.get("event") or "").strip()
        currency   = str(row.get("currency") or "").strip().upper()
        impact     = str(row.get("impact") or "HIGH").strip().upper()

        if not event_name:
            return None

        time_ms = _parse_dt_utc(row.get("datetime_utc", ""))
        if time_ms is None:
            log.warning("[IMPORT] Skipping — bad datetime: %s", row)
            return None

        # Skip events in the past (more than 2 hours ago)
        now_ms = _now_ms()
        # To this (keep today's events all day):
        today_start_ms = int(datetime.now(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0
        ).timestamp() * 1000)
        if time_ms < today_start_ms:
            return None

        pre_min  = int(row.get("pre_block_min") or 15)
        post_min = int(row.get("post_block_min") or 15)
        stab_min = int(row.get("stabilization_min") or 0)

        return {
            "event":             event_name,
            "currency":          currency,
            "impact":            impact,
            "time_ms":           time_ms,
            "pre_block_min":     pre_min,
            "post_block_min":    post_min,
            "stabilization_min": stab_min,
        }

    except Exception as e:
        log.warning("[IMPORT] Row validation error: %s | row=%s", e, row)
        return None


# ---------------------------------------------------------------------------
# Main import function
# ---------------------------------------------------------------------------

def import_calendar(
    csv_path: str = DEFAULT_CSV_PATH,
    R=None,
) -> dict:
    """
    Read events.csv → validate → store in Redis.

    Returns:
        {"ok": bool, "imported": int, "skipped": int, "path": str}
    """
    # Read CSV
    if not os.path.exists(csv_path):
        msg = f"CSV not found: {csv_path}"
        log.error("[IMPORT] %s", msg)
        return {"ok": False, "reason": msg}

    rows = []
    try:
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        log.info("[IMPORT] Read %d rows from %s", len(rows), csv_path)
    except Exception as e:
        msg = f"CSV read error: {e}"
        log.error("[IMPORT] %s", msg)
        return {"ok": False, "reason": msg}

    # Validate and normalize
    events = []
    skipped = 0
    for row in rows:
        ev = _validate_event(row)
        if ev:
            events.append(ev)
        else:
            skipped += 1

    log.info("[IMPORT] Valid: %d | Skipped: %d", len(events), skipped)

    # Sort by time
    events.sort(key=lambda x: int(x.get("time_ms") or 0))

    # Store in Redis
    if R is None:
        log.error("[IMPORT] Redis not connected")
        return {"ok": False, "reason": "redis_unavailable"}

    now_ms = _now_ms()
    payload = {
        "events":          events,
        "fetched_at_ms":   now_ms,
        "source":          "local_csv",
        "csv_path":        csv_path,
        "count":           len(events),
    }

    try:
        R.set(
            REDIS_CALENDAR_KEY,
            json.dumps(payload, separators=(",", ":")),
            ex=REDIS_CALENDAR_TTL,
        )
        log.info(
            "[IMPORT] Stored %d events in Redis | key=%s | TTL=%dh",
            len(events), REDIS_CALENDAR_KEY, REDIS_CALENDAR_TTL // 3600
        )
    except Exception as e:
        msg = f"Redis store failed: {e}"
        log.error("[IMPORT] %s", msg)
        return {"ok": False, "reason": msg}

    return {
        "ok":       True,
        "imported": len(events),
        "skipped":  skipped,
        "path":     csv_path,
    }


# ---------------------------------------------------------------------------
# Standalone runner
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )

    parser = argparse.ArgumentParser(description="XTL Calendar Import")
    parser.add_argument(
        "--file", default=DEFAULT_CSV_PATH,
        help=f"Path to events.csv (default: {DEFAULT_CSV_PATH})"
    )
    args = parser.parse_args()

    import redis as _redis
    REDIS_URL = os.getenv("REDIS_URL", "redis://default:xau12345@10.0.0.132:6379/0")
    _R = _redis.from_url(REDIS_URL, decode_responses=True)

    print(f"\n=== XauTrendLab Calendar Import ===")
    print(f"CSV : {args.file}")
    print(f"Redis: {REDIS_URL.split('@')[-1]}")
    print()

    result = import_calendar(csv_path=args.file, R=_R)
    print(json.dumps(result, indent=2))

    if result.get("ok"):
        print(f"\n✓ {result['imported']} events imported into Redis")
        print(f"  Key: {REDIS_CALENDAR_KEY}")
        print(f"  TTL: {REDIS_CALENDAR_TTL // 3600} hours")

        # Show what was stored
        try:
            raw  = _R.get(REDIS_CALENDAR_KEY)
            data = json.loads(raw) if raw else {}
            print(f"\nUpcoming events stored:")
            now_ms = _now_ms()
            for ev in data.get("events", []):
                ts = datetime.fromtimestamp(
                    ev["time_ms"] / 1000, tz=timezone.utc
                ).strftime("%Y-%m-%d %H:%M UTC")
                print(
                    f"  {ts} | {ev['currency']:4} | "
                    f"{ev['event']:<40} | "
                    f"pre={ev['pre_block_min']}m "
                    f"post={ev['post_block_min']}m "
                    f"stab={ev['stabilization_min']}m"
                )
        except Exception:
            pass
    else:
        print(f"\n✗ Import failed: {result.get('reason')}")
        sys.exit(1)
