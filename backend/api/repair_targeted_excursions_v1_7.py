#!/usr/bin/env python3
"""
One-time targeted excursion repair for selected XTL analytics trades.

Uses the deployed api.xtl_analytics v1.7 canonical broker normalization and
excursion helpers. It does not duplicate MFE/MAE math.

Default is DRY RUN. Pass --apply to atomically replace trades.jsonl.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import stat
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

# Run from /opt/xauapi, or ensure it is importable.
ROOT = Path("/opt/xauapi")
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from api import xtl_analytics as xa  # noqa: E402


TARGETS = {
    "6063330",  # USDCHF — dev_790d476e, offset was 0
}


def _summary(row: dict) -> dict:
    return {
        "ticket": str(row.get("mt5_ticket") or ""),
        "symbol": row.get("symbol"),
        "broker_verified": row.get("broker_verified"),
        "broker_open_time_utc_ms": row.get("broker_open_time_utc_ms"),
        "broker_close_time_utc_ms": row.get("broker_close_time_utc_ms"),
        "broker_tz_offset_minutes": row.get("broker_tz_offset_minutes"),
        "holding_minutes": row.get("holding_minutes"),
        "realized_r": row.get("realized_r"),
        "realized_r_net": row.get("realized_r_net"),
        "mfe_r": row.get("mfe_r"),
        "mae_r": row.get("mae_r"),
        "excursion_bars_used": row.get("excursion_bars_used"),
        "excursion_precision": row.get("excursion_precision"),
        "excursion_window_start_utc_ms": row.get(
            "excursion_window_start_utc_ms"
        ),
        "excursion_window_end_utc_ms": row.get(
            "excursion_window_end_utc_ms"
        ),
        "excursion_broker_offset_minutes": row.get(
            "excursion_broker_offset_minutes"
        ),
        "excursion_realized_floor_applied": row.get(
            "excursion_realized_floor_applied"
        ),
    }


def _repair_row(row: dict, redis_client) -> tuple[dict, str]:
    ticket = str(row.get("mt5_ticket") or "").strip()
    repaired = dict(row)

    # Refresh broker truth when the deal still exists. This guarantees current
    # normalized open/close timestamps and broker offset before excursion.
    broker_exit = xa._exit_from_broker_deal(ticket, repaired, redis_client)
    if isinstance(broker_exit, dict):
        repaired.update(broker_exit)
        repaired["broker_verified"] = True
        repaired["pending_broker_truth"] = False
        repaired["exit_deal_timeout"] = False
        source = "broker_deal_refreshed"
    elif (
        repaired.get("broker_open_time_utc_ms")
        and repaired.get("broker_close_time_utc_ms")
    ):
        # Deal TTL may have expired, but the permanent row already contains the
        # required normalized broker truth.
        source = "stored_broker_truth"
    else:
        return row, "SKIP_MISSING_BROKER_TIME"

    repaired["holding_minutes"] = xa._holding_minutes(repaired)

    try:
        raw_open = int(repaired.get("broker_open_time_ms") or 0)
        raw_close = int(repaired.get("broker_close_time_ms") or 0)
        repaired["broker_holding_minutes"] = (
            int(round((raw_close - raw_open) / 60000.0))
            if raw_open > 0 and raw_close > raw_open
            else None
        )
    except Exception:
        repaired["broker_holding_minutes"] = None

    xa._apply_realized_r_net_and_outcome(repaired)

    result = xa._recompute_excursion(
        repaired,
        bars_h1=None,
        fetch_h1_bars=xa.default_fetch_h1_bars,
    )

    if not result:
        return row, "SKIP_EXCURSION_RECOMPUTE_FAILED"

    # Hard integrity checks. Never write another mixed timestamp-domain row.
    if (
        repaired.get("excursion_window_start_utc_ms")
        != repaired.get("broker_open_time_utc_ms")
    ):
        return row, "SKIP_START_WINDOW_MISMATCH"

    if (
        repaired.get("excursion_window_end_utc_ms")
        != repaired.get("broker_close_time_utc_ms")
    ):
        return row, "SKIP_END_WINDOW_MISMATCH"

    if (
        repaired.get("excursion_broker_offset_minutes")
        != repaired.get("broker_tz_offset_minutes")
    ):
        return row, "SKIP_OFFSET_MISMATCH"

    # Prevent the old false-zero signature.
    if (
        int(repaired.get("excursion_bars_used") or 0) == 0
        and repaired.get("mfe_r") == 0
        and repaired.get("mae_r") == 0
    ):
        return row, "SKIP_FALSE_ZERO_SIGNATURE"

    repaired["schema_version"] = xa.SCHEMA_VERSION
    repaired["excursion_repaired_at_ms"] = xa._now_ms()
    repaired["excursion_repair_source"] = source
    repaired["excursion_repair_version"] = "TARGETED_V1_7_CANONICAL"

    return repaired, "REPAIRED"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Atomically replace trades.jsonl. Without this flag, dry-run only.",
    )
    args = parser.parse_args()

    path = Path(xa.JSONL_PATH)
    if not path.exists():
        print(f"ERROR: JSONL not found: {path}", file=sys.stderr)
        return 2

    redis_client = xa.from_app_R()
    found: set[str] = set()
    changed: dict[str, tuple[dict, dict, str]] = {}
    output_rows: list[dict | str] = []

    with xa._trades_jsonl_lock():
        original_stat = path.stat()

        with path.open("r", encoding="utf-8") as handle:
            for raw_line in handle:
                raw_line = raw_line.rstrip("\n")
                if not raw_line.strip():
                    continue

                try:
                    row = json.loads(raw_line)
                except Exception:
                    output_rows.append(raw_line)
                    continue

                if not isinstance(row, dict):
                    output_rows.append(raw_line)
                    continue

                ticket = str(row.get("mt5_ticket") or "").strip()
                if ticket not in TARGETS:
                    output_rows.append(row)
                    continue

                if ticket in found:
                    print(f"ERROR duplicate ticket in JSONL: {ticket}", file=sys.stderr)
                    return 3

                found.add(ticket)
                before = dict(row)
                after, status = _repair_row(row, redis_client)
                changed[ticket] = (before, after, status)
                output_rows.append(after)

        missing = sorted(TARGETS - found)
        if missing:
            print("ERROR: target tickets missing from JSONL:", ",".join(missing))
            return 4

        failed = {
            ticket: status
            for ticket, (_, _, status) in changed.items()
            if status != "REPAIRED"
        }
        if failed:
            print("ERROR: no file was changed because some repairs failed:")
            for ticket, status in sorted(failed.items()):
                print(f"  {ticket}: {status}")
            return 5

        for ticket in sorted(changed):
            before, after, status = changed[ticket]
            print(f"\n=== {ticket} {status} ===")
            print("BEFORE", json.dumps(_summary(before), sort_keys=True))
            print("AFTER ", json.dumps(_summary(after), sort_keys=True))

        if not args.apply:
            print(
                "\nDRY_RUN_OK: all 7 rows passed. "
                "Run again with --apply to write atomically."
            )
            return 0

        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        backup = path.with_name(f"{path.name}.bak_excursion_repair_{stamp}")
        shutil.copy2(path, backup)

        fd, tmp_name = tempfile.mkstemp(
            dir=str(path.parent),
            prefix=".trades_excursion_repair_",
            suffix=".tmp",
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as out:
                for row in output_rows:
                    if isinstance(row, str):
                        out.write(row + "\n")
                    else:
                        out.write(
                            json.dumps(
                                row,
                                default=str,
                                separators=(",", ":"),
                            )
                            + "\n"
                        )
                out.flush()
                os.fsync(out.fileno())

            os.chmod(tmp_name, stat.S_IMODE(original_stat.st_mode))
            try:
                os.chown(tmp_name, original_stat.st_uid, original_stat.st_gid)
            except PermissionError:
                pass

            os.replace(tmp_name, path)
            tmp_name = ""
        finally:
            if tmp_name and os.path.exists(tmp_name):
                os.unlink(tmp_name)

        print(f"\nAPPLY_OK repaired={len(changed)} backup={backup}")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
