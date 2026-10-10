"""
XauTrendLab Strategy B - Structural Buffer Calibration Aggregator
=================================================================

Offline / analytics-only JSONL aggregator.

Reads Strategy-B calibration JSONL records and summarizes the observed
structural-pivot wick breach distribution by:

    symbol x owner_timeframe
and optionally:
    symbol x owner_timeframe x side

This module NEVER proposes or authorizes a production stop buffer. It only
reports the empirical distribution needed for later policy approval.

Expected flattened snapshot fields (V1.1 additive schema):
- symbol
- side
- buffer_calibration_owner_timeframe
- buffer_max_pivot_breach_price
- buffer_max_pivot_breach_atr
- buffer_breach_count
- buffer_bars_after_pivot_count
- buffer_entry_to_pivot_distance_price
- buffer_entry_to_pivot_distance_atr
- buffer_calibration_ready
- buffer_proposed_structural_buffer_price

Historical rows without these fields are ignored rather than treated as zero.
"""

from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict
from pathlib import Path
from statistics import mean
from typing import Any, Iterable, Mapping, Optional


SNAPSHOT_RECORD_TYPES = {
    "SNAPSHOT",
    "STRATEGY_B_CALIBRATION_SNAPSHOT",
}


def _upper(v: Any) -> str:
    return str(v or "").strip().upper()


def _num(v: Any) -> Optional[float]:
    try:
        x = float(v)
        return x if math.isfinite(x) else None
    except Exception:
        return None


def _int(v: Any, default: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return default


def _quantile(values: list[float], q: float) -> Optional[float]:
    if not values:
        return None
    xs = sorted(float(x) for x in values)
    if len(xs) == 1:
        return xs[0]
    pos = (len(xs) - 1) * float(q)
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return xs[lo]
    frac = pos - lo
    return xs[lo] + (xs[hi] - xs[lo]) * frac


def iter_jsonl(path: str | Path) -> Iterable[dict[str, Any]]:
    p = Path(path)
    with p.open("r", encoding="utf-8") as fh:
        for line_no, raw in enumerate(fh, 1):
            if not raw.strip():
                continue
            try:
                row = json.loads(raw)
            except Exception as exc:
                raise ValueError(
                    f"INVALID_JSONL line={line_no} err={exc!r}"
                ) from exc
            if isinstance(row, dict):
                yield row


def _snapshot_payload(row: Mapping[str, Any]) -> Mapping[str, Any]:
    """
    Support both:
      A) flat snapshot records where calibration fields are top-level;
      B) wrapper records containing snapshot/payload/evidence.
    """
    if any(
        k in row
        for k in (
            "buffer_calibration_owner_timeframe",
            "buffer_max_pivot_breach_atr",
            "buffer_calibration_reason",
        )
    ):
        return row

    for key in ("snapshot", "payload", "evidence"):
        v = row.get(key)
        if isinstance(v, Mapping):
            if any(
                k in v
                for k in (
                    "buffer_calibration_owner_timeframe",
                    "buffer_max_pivot_breach_atr",
                    "buffer_calibration_reason",
                )
            ):
                return v

    return {}


def _is_snapshot_record(row: Mapping[str, Any]) -> bool:
    rt = _upper(row.get("record_type"))
    if not rt:
        # Historical/plain JSONL snapshot.
        return True
    return rt in SNAPSHOT_RECORD_TYPES or "SNAPSHOT" in rt


def _eligible_buffer_sample(payload: Mapping[str, Any]) -> tuple[bool, str]:
    if not payload:
        return False, "BUFFER_FIELDS_NOT_PRESENT"

    # Calibration integrity boundary:
    # schedule-anomaly / explicitly non-calibration H1 observations remain
    # preserved in JSONL for diagnostics, but must never contribute to
    # empirical structural-buffer calibration.
    h1_sample_class = _upper(payload.get("h1_sample_class"))
    h1_schedule_consistent = payload.get("h1_schedule_consistent")

    if (
        h1_sample_class == "SCHEDULE_ANOMALY_NONCALIBRATION"
        or "NONCALIBRATION" in h1_sample_class
    ):
        return False, "H1_NONCALIBRATION_SAMPLE"

    if h1_schedule_consistent is False:
        return False, "H1_SCHEDULE_INCONSISTENT"

    symbol = _upper(payload.get("symbol"))
    side = _upper(payload.get("side"))
    tf = _upper(payload.get("buffer_calibration_owner_timeframe"))

    if not symbol:
        return False, "SYMBOL_MISSING"
    if side not in ("BUY", "SELL"):
        return False, "SIDE_MISSING"
    if tf not in ("D1", "H4", "H1"):
        return False, "OWNER_TIMEFRAME_MISSING"

    breach_atr = _num(payload.get("buffer_max_pivot_breach_atr"))
    breach_price = _num(payload.get("buffer_max_pivot_breach_price"))
    if breach_atr is None or breach_price is None:
        return False, "BREACH_MEASUREMENT_MISSING"

    bars = _int(payload.get("buffer_bars_after_pivot_count"), 0)
    if bars <= 0:
        return False, "NO_POST_PIVOT_BARS"

    # A calibrated/proposed production buffer must NOT be inferred from these
    # rows; this check is informational only.
    return True, "ELIGIBLE"


def aggregate_strategy_b_buffer_calibration(
    rows: Iterable[Mapping[str, Any]],
    *,
    include_side: bool = False,
) -> dict[str, Any]:
    groups: dict[tuple[str, ...], list[Mapping[str, Any]]] = defaultdict(list)
    skipped: dict[str, int] = defaultdict(int)

    # Raw JSONL contains longitudinal observations. Calibration statistics must
    # count independent structural setups, not every runtime snapshot.
    #
    # Value = (outer_record, payload)
    setup_samples: dict[
        tuple[Any, ...],
        tuple[Mapping[str, Any], Mapping[str, Any]],
    ] = {}

    eligible_observations_before_dedupe = 0

    total_records = 0
    snapshot_records = 0
    eligible_records = 0

    for row in rows:
        if not isinstance(row, Mapping):
            continue
        total_records += 1

        if not _is_snapshot_record(row):
            continue
        snapshot_records += 1

        payload = _snapshot_payload(row)
        ok, reason = _eligible_buffer_sample(payload)
        if not ok:
            skipped[reason] += 1
            continue

        symbol = _upper(payload.get("symbol"))
        side = _upper(payload.get("side"))
        tf = _upper(payload.get("buffer_calibration_owner_timeframe"))

        eligible_observations_before_dedupe += 1

        # Outer-record identity.
        evidence_id = str(
            row.get("evidence_id")
            or payload.get("evidence_id")
            or ""
        ).strip()

        release_id = str(
            row.get("release_id")
            or payload.get("release_id")
            or ""
        ).strip()

        # Structural identity inside the calibration payload.
        invalidation_price = _num(
            payload.get("selected_invalidation_price")
        )
        buffer_pivot_ms = _int(payload.get("buffer_pivot_ms"), 0)

        # Fail closed for setup identity. We must not silently merge unrelated
        # calibration rows when their causal/structural identity is incomplete.
        if not evidence_id:
            skipped["SETUP_EVIDENCE_ID_MISSING"] += 1
            continue
        if invalidation_price is None:
            skipped["SETUP_INVALIDATION_PRICE_MISSING"] += 1
            continue
        if buffer_pivot_ms <= 0:
            skipped["SETUP_BUFFER_PIVOT_MS_MISSING"] += 1
            continue

        # Statistical identity is the physical causal market setup.
        #
        # evidence_id / release_id remain capture-lineage metadata and
        # evidence_id is still required above for traceability integrity,
        # but neither may split one physical market structure into multiple
        # calibration samples.
        setup_key = (
            symbol,
            side,
            tf,
            int(buffer_pivot_ms),
            float(invalidation_price),
        )

        previous_pair = setup_samples.get(setup_key)

        if previous_pair is None:
            setup_samples[setup_key] = (row, payload)
        else:
            previous_row, previous_payload = previous_pair

            previous_bars = _int(
                previous_payload.get("buffer_bars_after_pivot_count"), 0
            )
            candidate_bars = _int(
                payload.get("buffer_bars_after_pivot_count"), 0
            )

            previous_capture_ms = _int(
                previous_row.get("captured_at_ms")
                or previous_row.get("written_at_ms")
                or previous_payload.get("captured_at_ms")
                or previous_payload.get("written_at_ms"),
                0,
            )
            candidate_capture_ms = _int(
                row.get("captured_at_ms")
                or row.get("written_at_ms")
                or payload.get("captured_at_ms")
                or payload.get("written_at_ms"),
                0,
            )

            # Prefer the longest observation horizon. If horizons are equal,
            # retain the latest observation so later breach development wins.
            if (
                candidate_bars > previous_bars
                or (
                    candidate_bars == previous_bars
                    and candidate_capture_ms > previous_capture_ms
                )
            ):
                setup_samples[setup_key] = (row, payload)

    # Only independent structural setups enter empirical distributions.
    eligible_records = len(setup_samples)

    for _, payload in setup_samples.values():
        symbol = _upper(payload.get("symbol"))
        side = _upper(payload.get("side"))
        tf = _upper(payload.get("buffer_calibration_owner_timeframe"))

        key = (symbol, tf, side) if include_side else (symbol, tf)
        groups[key].append(payload)

    out_groups: list[dict[str, Any]] = []

    for key in sorted(groups):
        items = groups[key]

        breach_atr = [
            _num(x.get("buffer_max_pivot_breach_atr"))
            for x in items
        ]
        breach_atr = [x for x in breach_atr if x is not None]

        breach_price = [
            _num(x.get("buffer_max_pivot_breach_price"))
            for x in items
        ]
        breach_price = [x for x in breach_price if x is not None]

        entry_dist_atr = [
            _num(x.get("buffer_entry_to_pivot_distance_atr"))
            for x in items
        ]
        entry_dist_atr = [x for x in entry_dist_atr if x is not None]

        breach_counts = [
            _int(x.get("buffer_breach_count"), 0)
            for x in items
        ]
        post_bars = [
            _int(x.get("buffer_bars_after_pivot_count"), 0)
            for x in items
        ]

        zero_breach_n = sum(1 for x in breach_atr if x == 0.0)
        positive_breach_n = sum(1 for x in breach_atr if x > 0.0)

        row_out: dict[str, Any] = {
            "symbol": key[0],
            "owner_timeframe": key[1],
            "sample_count": len(items),
            "zero_breach_count": zero_breach_n,
            "positive_breach_count": positive_breach_n,
            "zero_breach_rate": (
                zero_breach_n / len(breach_atr)
                if breach_atr
                else None
            ),
            "breach_atr": {
                "mean": mean(breach_atr) if breach_atr else None,
                "p50": _quantile(breach_atr, 0.50),
                "p75": _quantile(breach_atr, 0.75),
                "p90": _quantile(breach_atr, 0.90),
                "p95": _quantile(breach_atr, 0.95),
                "max": max(breach_atr) if breach_atr else None,
            },
            "breach_price": {
                "mean": mean(breach_price) if breach_price else None,
                "p50": _quantile(breach_price, 0.50),
                "p90": _quantile(breach_price, 0.90),
                "max": max(breach_price) if breach_price else None,
            },
            "entry_to_pivot_atr": {
                "mean": mean(entry_dist_atr) if entry_dist_atr else None,
                "p50": _quantile(entry_dist_atr, 0.50),
                "p90": _quantile(entry_dist_atr, 0.90),
                "max": max(entry_dist_atr) if entry_dist_atr else None,
            },
            "breach_event_count_total": sum(breach_counts),
            "post_pivot_bar_count_total": sum(post_bars),

            # Explicitly no policy output.
            "production_buffer_policy_ready": False,
            "proposed_buffer_atr": None,
            "proposed_buffer_price": None,
            "policy_reason": "CALIBRATION_ONLY_NO_PRODUCTION_BUFFER_AUTHORITY",
        }

        if include_side:
            row_out["side"] = key[2]

        out_groups.append(row_out)

    return {
        "schema_version": 1,
        "model": "STRATEGY_B_BUFFER_CALIBRATION_AGGREGATOR_V1",
        "grouping": (
            "SYMBOL_OWNER_TIMEFRAME_SIDE"
            if include_side
            else "SYMBOL_OWNER_TIMEFRAME"
        ),
        "total_records": total_records,
        "snapshot_records": snapshot_records,
        "eligible_buffer_samples": eligible_records,
        "eligible_observations_before_dedupe": (
            eligible_observations_before_dedupe
        ),
        "duplicate_observations_collapsed": max(
            0,
            eligible_observations_before_dedupe
            - eligible_records
            - skipped.get("SETUP_EVIDENCE_ID_MISSING", 0)
            - skipped.get("SETUP_INVALIDATION_PRICE_MISSING", 0)
            - skipped.get("SETUP_BUFFER_PIVOT_MS_MISSING", 0)
        ),
        "skipped": dict(sorted(skipped.items())),
        "group_count": len(out_groups),
        "groups": out_groups,
        "observation_only": True,
        "execution_authority": False,
        "risk_authority": False,
        "production_buffer_policy_ready": False,
    }


def aggregate_jsonl_file(
    path: str | Path,
    *,
    include_side: bool = False,
) -> dict[str, Any]:
    return aggregate_strategy_b_buffer_calibration(
        iter_jsonl(path),
        include_side=include_side,
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("jsonl_path")
    ap.add_argument(
        "--include-side",
        action="store_true",
        help="group by symbol x owner timeframe x BUY/SELL",
    )
    ap.add_argument(
        "--pretty",
        action="store_true",
        help="pretty-print JSON output",
    )
    ns = ap.parse_args()

    out = aggregate_jsonl_file(
        ns.jsonl_path,
        include_side=ns.include_side,
    )
    print(
        json.dumps(
            out,
            indent=2 if ns.pretty else None,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
