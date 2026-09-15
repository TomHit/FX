"""
XauTrendLab Strategy B - Forward Path Calibration Aggregator
============================================================

Offline / descriptive-only calibration aggregator.

Consumes Strategy-B append-only calibration JSONL snapshots and produces:

PATH calibration:
- clean causal forward-path samples;
- MFE / MAE distributions;
- favorable/adverse ratio distributions.

PIVOT calibration:
- subset of PATH samples with a resolved raw structural pivot;
- wick-breach / close-beyond / recovery rates;
- raw pivot penetration distributions.

This module has:
- NO Redis IO;
- NO broker/order calls;
- NO risk authority;
- NO execution authority;
- NO Strategy-A mutation;
- NO production ranking policy;
- NO production structural-buffer policy.

Repeated longitudinal snapshots for the same causal setup are deduplicated
offline. The most mature observation is retained using:
    1. greatest eligible_bar_count;
    2. latest captured_at_ms;
    3. latest written_at_ms.

Raw JSONL is never modified.
"""

from __future__ import annotations

import argparse
import json
import math
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional


MODEL = "STRATEGY_B_FORWARD_PATH_CALIBRATION_AGGREGATOR_V1"
SCHEMA_VERSION = 1


def _upper(v: Any) -> str:
    return str(v or "").strip().upper()


def _num(v: Any) -> Optional[float]:
    try:
        x = float(v)
        if not math.isfinite(x):
            return None
        return x
    except Exception:
        return None


def _int(v: Any, default: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return default


def _bool(v: Any) -> bool:
    return v is True


def _percentile(values: Iterable[float], q: float) -> Optional[float]:
    """
    Linear percentile, equivalent to NumPy-style interpolation between
    adjacent ordered observations.

    q is in [0, 1].
    """
    xs = sorted(float(x) for x in values if _num(x) is not None)
    if not xs:
        return None

    if len(xs) == 1:
        return xs[0]

    q = min(1.0, max(0.0, float(q)))
    pos = (len(xs) - 1) * q

    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))

    if lo == hi:
        return xs[lo]

    frac = pos - lo
    return xs[lo] + (xs[hi] - xs[lo]) * frac


def _distribution(values: Iterable[Any]) -> dict[str, Any]:
    xs: list[float] = []

    for v in values:
        n = _num(v)
        if n is not None:
            xs.append(n)

    if not xs:
        return {
            "count": 0,
            "min": None,
            "median": None,
            "p75": None,
            "p90": None,
            "max": None,
            "mean": None,
        }

    return {
        "count": len(xs),
        "min": min(xs),
        "median": _percentile(xs, 0.50),
        "p75": _percentile(xs, 0.75),
        "p90": _percentile(xs, 0.90),
        "max": max(xs),
        "mean": sum(xs) / len(xs),
    }


def _ratio(favorable: Any, adverse: Any) -> Optional[float]:
    f = _num(favorable)
    a = _num(adverse)

    if f is None or a is None:
        return None

    if a < 0 or f < 0:
        return None

    # Zero MAE with positive MFE is valid but mathematically unbounded.
    # Do not invent a finite cap; exclude from ratio distribution and expose
    # separately through favorable_without_adverse_count.
    if a == 0:
        return None

    return f / a


def _setup_identity(
    row: Mapping[str, Any],
    payload: Mapping[str, Any],
    fp: Mapping[str, Any],
) -> tuple[Any, ...]:
    """
    Causal setup identity.

    Do NOT include longitudinal fields such as eligible_bar_count,
    MFE/MAE, capture_id, captured_at_ms, or written_at_ms.
    """
    return (
        str(row.get("evidence_id") or payload.get("h1_evidence_id") or ""),
        str(row.get("release_id") or ""),
        _upper(payload.get("symbol") or row.get("symbol")),
        _upper(payload.get("side") or row.get("side")),
        _int(payload.get("h1_break_ms") or fp.get("h1_break_ms")),
        _num(payload.get("h1_break_close") or fp.get("reference_price")),
        _num(payload.get("h1_reference_atr") or fp.get("reference_atr")),
    )


def _path_eligibility(
    row: Mapping[str, Any],
) -> tuple[bool, str]:
    payload = row.get("payload")
    if not isinstance(payload, Mapping):
        return False, "PAYLOAD_MISSING"

    sample_class = _upper(payload.get("h1_sample_class"))
    if not sample_class:
        return False, "H1_SAMPLE_CLASS_MISSING"

    if "NONCALIBRATION" in sample_class:
        return False, "H1_NONCALIBRATION_SAMPLE"

    if payload.get("h1_schedule_consistent") is False:
        return False, "H1_SCHEDULE_INCONSISTENT"

    if payload.get("h1_calibration_eligible") is not True:
        return False, "H1_CALIBRATION_NOT_ELIGIBLE"

    fp = payload.get("forward_path_observation")
    if not isinstance(fp, Mapping):
        return False, "FORWARD_PATH_MISSING"

    if _upper(fp.get("observation_state")) != "OBSERVING":
        return False, "FORWARD_PATH_NOT_OBSERVING"

    if fp.get("future_data_used") is True:
        return False, "FORWARD_PATH_FUTURE_DATA_USED"

    if fp.get("execution_authority") is True:
        return False, "FORWARD_PATH_EXECUTION_AUTHORITY_TRUE"

    if fp.get("risk_authority") is True:
        return False, "FORWARD_PATH_RISK_AUTHORITY_TRUE"

    if fp.get("strategy_a_mutation") is True:
        return False, "FORWARD_PATH_STRATEGY_A_MUTATION_TRUE"

    break_ms = _int(payload.get("h1_break_ms") or fp.get("h1_break_ms"))
    if break_ms <= 0:
        return False, "H1_BREAK_TIME_INVALID"

    break_close = _num(
        payload.get("h1_break_close")
        if payload.get("h1_break_close") is not None
        else fp.get("reference_price")
    )
    if break_close is None or break_close <= 0:
        return False, "H1_REFERENCE_PRICE_INVALID"

    ref_atr = _num(
        payload.get("h1_reference_atr")
        if payload.get("h1_reference_atr") is not None
        else fp.get("reference_atr")
    )
    if ref_atr is None or ref_atr <= 0:
        return False, "H1_REFERENCE_ATR_INVALID"

    if _int(fp.get("eligible_bar_count")) < 1:
        return False, "NO_COMPLETED_FORWARD_H1_BAR"

    mfe = _num(fp.get("max_favorable_excursion_atr"))
    mae = _num(fp.get("max_adverse_excursion_atr"))

    if mfe is None:
        return False, "MFE_ATR_MISSING"

    if mae is None:
        return False, "MAE_ATR_MISSING"

    if mfe < 0:
        return False, "MFE_ATR_NEGATIVE"

    if mae < 0:
        return False, "MAE_ATR_NEGATIVE"

    return True, "PATH_ELIGIBLE"


def _pivot_eligibility(
    row: Mapping[str, Any],
) -> tuple[bool, str]:
    ok, reason = _path_eligibility(row)
    if not ok:
        return False, reason

    payload = row.get("payload") or {}
    fp = payload.get("forward_path_observation") or {}

    if fp.get("raw_pivot_available") is not True:
        return False, "RAW_PIVOT_UNAVAILABLE"

    pivot = _num(fp.get("selected_invalidation_price"))
    if pivot is None or pivot <= 0:
        return False, "RAW_PIVOT_PRICE_INVALID"

    tf = _upper(fp.get("selected_invalidation_timeframe"))
    if not tf:
        return False, "RAW_PIVOT_TIMEFRAME_MISSING"

    penetration = _num(fp.get("max_raw_pivot_penetration_atr"))
    if penetration is None:
        return False, "RAW_PIVOT_PENETRATION_ATR_MISSING"

    if penetration < 0:
        return False, "RAW_PIVOT_PENETRATION_ATR_NEGATIVE"

    return True, "PIVOT_ELIGIBLE"


def _maturity_key(row: Mapping[str, Any]) -> tuple[int, int, int]:
    payload = row.get("payload") or {}
    fp = payload.get("forward_path_observation") or {}

    return (
        _int(fp.get("eligible_bar_count")),
        _int(row.get("captured_at_ms") or payload.get("captured_at_ms")),
        _int(row.get("written_at_ms")),
    )


def _dedupe_most_mature(
    rows: Iterable[Mapping[str, Any]],
) -> tuple[list[Mapping[str, Any]], int]:
    best: dict[tuple[Any, ...], Mapping[str, Any]] = {}
    raw_count = 0

    for row in rows:
        raw_count += 1

        payload = row.get("payload")
        if not isinstance(payload, Mapping):
            continue

        fp = payload.get("forward_path_observation")
        if not isinstance(fp, Mapping):
            continue

        key = _setup_identity(row, payload, fp)

        prior = best.get(key)
        if prior is None or _maturity_key(row) > _maturity_key(prior):
            best[key] = row

    return list(best.values()), max(0, raw_count - len(best))


def _sample_summary(rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    mfe: list[float] = []
    mae: list[float] = []
    ratios: list[float] = []

    favorable_without_adverse = 0
    bars: list[int] = []

    for row in rows:
        payload = row.get("payload") or {}
        fp = payload.get("forward_path_observation") or {}

        f = _num(fp.get("max_favorable_excursion_atr"))
        a = _num(fp.get("max_adverse_excursion_atr"))

        if f is not None:
            mfe.append(f)

        if a is not None:
            mae.append(a)

        r = _ratio(f, a)
        if r is not None:
            ratios.append(r)
        elif f is not None and f > 0 and a == 0:
            favorable_without_adverse += 1

        bars.append(_int(fp.get("eligible_bar_count")))

    return {
        "sample_count": len(rows),
        "completed_forward_h1_bars": _distribution(bars),
        "mfe_atr": _distribution(mfe),
        "mae_atr": _distribution(mae),
        "favorable_to_adverse_ratio": _distribution(ratios),
        "favorable_without_adverse_count": favorable_without_adverse,
    }


def _pivot_summary(rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    n = len(rows)

    wick = 0
    close_beyond = 0
    recovered_wick = 0
    recovered_close = 0
    penetrations: list[float] = []

    for row in rows:
        payload = row.get("payload") or {}
        fp = payload.get("forward_path_observation") or {}

        wick += int(fp.get("raw_pivot_wick_breached") is True)
        close_beyond += int(fp.get("raw_pivot_close_beyond") is True)

        recovered_wick += int(
            fp.get("recovered_inside_after_wick_breach") is True
        )
        recovered_close += int(
            fp.get("recovered_inside_after_close_beyond") is True
        )

        p = _num(fp.get("max_raw_pivot_penetration_atr"))
        if p is not None:
            penetrations.append(p)

    def rate(x: int) -> Optional[float]:
        return (x / n) if n > 0 else None

    # Conditional recovery rates are more useful than only unconditional rates.
    return {
        "sample_count": n,

        "wick_breach_count": wick,
        "wick_breach_rate": rate(wick),

        "close_beyond_count": close_beyond,
        "close_beyond_rate": rate(close_beyond),

        "recovered_after_wick_count": recovered_wick,
        "recovered_after_wick_rate_all_samples": rate(recovered_wick),
        "recovered_after_wick_rate_given_breach": (
            recovered_wick / wick if wick > 0 else None
        ),

        "recovered_after_close_count": recovered_close,
        "recovered_after_close_rate_all_samples": rate(recovered_close),
        "recovered_after_close_rate_given_close_beyond": (
            recovered_close / close_beyond
            if close_beyond > 0
            else None
        ),

        "max_raw_pivot_penetration_atr": _distribution(penetrations),
    }


def _group_rows(
    rows: Iterable[Mapping[str, Any]],
) -> dict[tuple[str, str], list[Mapping[str, Any]]]:
    grouped: dict[tuple[str, str], list[Mapping[str, Any]]] = defaultdict(list)

    for row in rows:
        payload = row.get("payload") or {}

        symbol = _upper(payload.get("symbol") or row.get("symbol"))
        side = _upper(payload.get("side") or row.get("side"))

        grouped[(symbol, side)].append(row)

    return grouped


def aggregate_strategy_b_forward_path_calibration(
    records: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    total_records = 0
    snapshot_records = 0

    raw_path_candidates: list[Mapping[str, Any]] = []

    path_skipped: Counter[str] = Counter()

    for row in records:
        total_records += 1

        if not isinstance(row, Mapping):
            path_skipped["ROW_NOT_MAPPING"] += 1
            continue

        record_type = _upper(row.get("record_type"))
        if record_type and record_type != "SNAPSHOT":
            path_skipped["NOT_SNAPSHOT"] += 1
            continue

        snapshot_records += 1

        ok, reason = _path_eligibility(row)
        if not ok:
            path_skipped[reason] += 1
            continue

        raw_path_candidates.append(row)

    deduped_path_rows, path_duplicates_collapsed = _dedupe_most_mature(
        raw_path_candidates
    )

    # Evaluate pivot eligibility AFTER path dedupe so pivot statistics operate
    # on one mature observation per causal setup.
    pivot_rows: list[Mapping[str, Any]] = []
    pivot_skipped: Counter[str] = Counter()

    for row in deduped_path_rows:
        ok, reason = _pivot_eligibility(row)
        if ok:
            pivot_rows.append(row)
        else:
            pivot_skipped[reason] += 1

    path_groups = _group_rows(deduped_path_rows)
    pivot_groups = _group_rows(pivot_rows)

    all_group_keys = sorted(set(path_groups) | set(pivot_groups))

    groups = []

    for symbol, side in all_group_keys:
        p_rows = path_groups.get((symbol, side), [])
        v_rows = pivot_groups.get((symbol, side), [])

        groups.append({
            "symbol": symbol,
            "side": side,
            "path": _sample_summary(p_rows),
            "pivot": _pivot_summary(v_rows),
        })

    return {
        "schema_version": SCHEMA_VERSION,
        "model": MODEL,

        "total_records": total_records,
        "snapshot_records": snapshot_records,

        "path_candidate_records_before_dedupe": len(raw_path_candidates),
        "path_duplicates_collapsed": path_duplicates_collapsed,
        "path_eligible_samples": len(deduped_path_rows),

        "pivot_eligible_samples": len(pivot_rows),

        "path_skipped": dict(sorted(path_skipped.items())),
        "pivot_skipped": dict(sorted(pivot_skipped.items())),

        "overall": {
            "path": _sample_summary(deduped_path_rows),
            "pivot": _pivot_summary(pivot_rows),
        },

        "group_count": len(groups),
        "groups": groups,

        "observation_only": True,
        "execution_authority": False,
        "risk_authority": False,
        "strategy_a_mutation": False,

        "ranking_policy_ready": False,
        "production_buffer_policy_ready": False,
        "outcome_labels_produced": False,
        "policy_reason": (
            "DESCRIPTIVE_CALIBRATION_ONLY_NO_PRODUCTION_AUTHORITY"
        ),
    }


def iter_jsonl(path: str | Path):
    p = Path(path)

    with p.open("r", encoding="utf-8") as fh:
        for line_no, raw in enumerate(fh, 1):
            raw = raw.strip()

            if not raw:
                continue

            try:
                row = json.loads(raw)
            except Exception as exc:
                raise ValueError(
                    f"INVALID_JSONL line={line_no} err={exc!r}"
                ) from exc

            yield row


def aggregate_jsonl_file(
    path: str | Path,
) -> dict[str, Any]:
    return aggregate_strategy_b_forward_path_calibration(
        iter_jsonl(path)
    )


def main() -> int:
    ap = argparse.ArgumentParser()

    ap.add_argument(
        "jsonl_path",
        help="Strategy-B calibration JSONL path",
    )

    ap.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty-print JSON output",
    )

    args = ap.parse_args()

    result = aggregate_jsonl_file(args.jsonl_path)

    print(
        json.dumps(
            result,
            indent=2 if args.pretty else None,
            sort_keys=True,
        )
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "aggregate_strategy_b_forward_path_calibration",
    "aggregate_jsonl_file",
    "iter_jsonl",
]
