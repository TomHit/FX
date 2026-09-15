"""
XauTrendLab Strategy B - Phase 7 Dataset Auditor
=================================================

Offline / read-only dataset quality and coverage audit.

Purpose:
- audit Strategy-B Phase-7 calibration JSONL;
- count independent releases/evidence rather than treating raw rows as samples;
- expose causal-integrity and contamination problems;
- expose basket/symbol/H1/forward-path coverage;
- reuse the existing forward-path calibration aggregator for PATH/PIVOT
  eligibility and causal-setup deduplication.

This module has:
- NO Redis IO;
- NO broker/order calls;
- NO risk authority;
- NO execution authority;
- NO Strategy-A mutation;
- NO JSONL writes;
- NO production ranking policy;
- NO production structural-buffer policy;
- NO invented Phase-7 readiness sample threshold.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping


MODEL = "STRATEGY_B_PHASE7_DATASET_AUDIT_V1"
SCHEMA_VERSION = 1


def _upper(v: Any) -> str:
    return str(v or "").strip().upper()


def _text(v: Any) -> str:
    return str(v or "").strip()


def _int(v: Any, default: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return default


def _bucket_room(v: Any) -> str:
    try:
        x = float(v)
    except Exception:
        return "MISSING"

    if x < 0:
        return "NEGATIVE"
    if x <= 0.20:
        return "LE_0_20_ATR"
    if x <= 0.50:
        return "GT_0_20_LE_0_50_ATR"
    if x <= 1.00:
        return "GT_0_50_LE_1_00_ATR"
    return "GT_1_00_ATR"


def iter_jsonl(path: str | Path):
    p = Path(path)

    if not p.exists():
        raise FileNotFoundError(str(p))

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

            if not isinstance(row, Mapping):
                raise ValueError(
                    f"INVALID_JSONL_RECORD line={line_no} "
                    f"type={type(row).__name__}"
                )

            yield dict(row)


def audit_strategy_b_phase7_dataset(
    records: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    # Materialize once because the same immutable records are also passed to
    # the existing descriptive forward-path aggregator.
    rows = list(records)

    total_records = len(rows)

    record_types: Counter[str] = Counter()
    sources: Counter[str] = Counter()
    environments: Counter[str] = Counter()

    sample_classes: Counter[str] = Counter()
    schedule_consistency: Counter[str] = Counter()
    calibration_eligibility: Counter[str] = Counter()
    h1_proof_statuses: Counter[str] = Counter()
    h1_observation_reasons: Counter[str] = Counter()

    symbols: Counter[str] = Counter()
    symbol_sides: Counter[str] = Counter()

    location_statuses: Counter[str] = Counter()
    location_block_reasons: Counter[str] = Counter()
    invalidation_ownership_statuses: Counter[str] = Counter()

    structural_pressure: Counter[str] = Counter()
    structural_room_buckets: Counter[str] = Counter()

    forward_states: Counter[str] = Counter()
    forward_bar_buckets: Counter[str] = Counter()

    releases: set[str] = set()
    evidences: set[str] = set()
    captures: set[str] = set()

    release_to_evidence: dict[str, set[str]] = defaultdict(set)
    evidence_to_release: dict[str, set[str]] = defaultdict(set)
    evidence_to_symbols: dict[str, set[str]] = defaultdict(set)
    evidence_to_symbol_sides: dict[str, set[str]] = defaultdict(set)

    duplicate_capture_ids = 0
    snapshot_records = 0
    outcome_records = 0

    envelope_payload_evidence_mismatch = 0
    future_data_contamination = 0
    replay_test_contamination = 0
    execution_authority_violations = 0
    strategy_a_mutation_violations = 0
    observation_only_violations = 0

    canonical_causal = 0
    schedule_anomalies = 0
    causal_source_incomplete = 0

    forward_path_present = 0
    forward_path_with_completed_bar = 0

    outcome_capture_ids: set[str] = set()
    unknown_outcome_capture_ids: set[str] = set()

    snapshot_capture_ids: set[str] = set()

    for row in rows:
        record_type = _upper(row.get("record_type")) or "MISSING"
        record_types[record_type] += 1

        source = _upper(row.get("source")) or "MISSING"
        environment = _upper(row.get("environment")) or "MISSING"

        sources[source] += 1
        environments[environment] += 1

        # Record-level contamination flags.
        #
        # A SNAPSHOT may expose the same condition in both its envelope and
        # payload/sample classification. Count contaminated RECORDS once,
        # rather than counting individual detection locations.
        row_future_data_contamination = (
            row.get("future_data_used") is True
        )

        row_replay_test_contamination = (
            source in {"REPLAY", "TEST", "SYNTHETIC", "SYNTHETIC_TEST"}
            or "REPLAY" in source
            or "TEST" in source
        )

        if row.get("execution_authority") is True:
            execution_authority_violations += 1

        if row.get("strategy_a_mutation") is True:
            strategy_a_mutation_violations += 1

        if row.get("observation_only") is not True:
            observation_only_violations += 1

        if record_type == "OUTCOME":
            outcome_records += 1
            cid = _text(row.get("capture_id"))
            if cid:
                outcome_capture_ids.add(cid)
            continue

        if record_type != "SNAPSHOT":
            continue

        snapshot_records += 1

        cid = _text(row.get("capture_id"))
        if cid:
            if cid in captures:
                duplicate_capture_ids += 1
            captures.add(cid)
            snapshot_capture_ids.add(cid)

        release_id = _text(row.get("release_id"))
        evidence_id = _text(row.get("evidence_id"))

        if release_id:
            releases.add(release_id)

        if evidence_id:
            evidences.add(evidence_id)

        if release_id and evidence_id:
            release_to_evidence[release_id].add(evidence_id)
            evidence_to_release[evidence_id].add(release_id)

        payload = row.get("payload")
        if not isinstance(payload, Mapping):
            sample_classes["PAYLOAD_MISSING"] += 1
            continue

        symbol = _upper(payload.get("symbol") or row.get("symbol")) or "MISSING"
        side = _upper(payload.get("side") or row.get("side")) or "MISSING"

        symbols[symbol] += 1
        symbol_sides[f"{symbol}:{side}"] += 1

        if evidence_id:
            evidence_to_symbols[evidence_id].add(symbol)
            evidence_to_symbol_sides[evidence_id].add(f"{symbol}:{side}")

        payload_evidence_id = _text(payload.get("h1_evidence_id"))
        if (
            evidence_id
            and payload_evidence_id
            and evidence_id != payload_evidence_id
        ):
            envelope_payload_evidence_mismatch += 1

        sample_class = (
            _upper(payload.get("h1_sample_class"))
            or "MISSING"
        )
        sample_classes[sample_class] += 1

        schedule_value = payload.get("h1_schedule_consistent")
        if schedule_value is True:
            schedule_consistency["TRUE"] += 1
        elif schedule_value is False:
            schedule_consistency["FALSE"] += 1
        else:
            schedule_consistency["MISSING"] += 1

        eligible = payload.get("h1_calibration_eligible")
        if eligible is True:
            calibration_eligibility["TRUE"] += 1
        elif eligible is False:
            calibration_eligibility["FALSE"] += 1
        else:
            calibration_eligibility["MISSING"] += 1

        if sample_class == "CANONICAL_CAUSAL":
            canonical_causal += 1
        elif sample_class == "SCHEDULE_ANOMALY_NONCALIBRATION":
            schedule_anomalies += 1
        elif sample_class == "CAUSAL_SOURCE_INCOMPLETE_NONCALIBRATION":
            causal_source_incomplete += 1

        if "REPLAY" in sample_class or "TEST" in sample_class:
            row_replay_test_contamination = True

        if payload.get("future_data_used") is True:
            row_future_data_contamination = True

        if row_replay_test_contamination:
            replay_test_contamination += 1

        if row_future_data_contamination:
            future_data_contamination += 1

        proof_status = (
            _upper(
                payload.get("h1_proof_status")
                or payload.get("proof_status")
            )
            or "MISSING"
        )
        h1_proof_statuses[proof_status] += 1

        obs_reason = (
            _upper(payload.get("h1_observation_reason"))
            or "MISSING"
        )
        h1_observation_reasons[obs_reason] += 1

        location_status = (
            _upper(payload.get("location_status"))
            or "MISSING"
        )
        location_statuses[location_status] += 1

        location_block_reason = (
            _upper(payload.get("location_block_reason"))
            or "NONE"
        )
        location_block_reasons[location_block_reason] += 1

        invalidation_owner = (
            _upper(payload.get("invalidation_ownership_status"))
            or "MISSING"
        )
        invalidation_ownership_statuses[invalidation_owner] += 1

        # These fields can evolve between calibration schema versions.
        # Audit whichever canonical field is present without inventing values.
        pressure = (
            _upper(payload.get("structure_pressure"))
            or _upper(payload.get("structural_pressure"))
            or _upper(payload.get("location_structure_pressure"))
            or "MISSING"
        )
        structural_pressure[pressure] += 1

        room = payload.get("h1_structural_room_atr")
        if room is None:
            room = payload.get("directional_room_atr")
        structural_room_buckets[_bucket_room(room)] += 1

        fp = payload.get("forward_path_observation")
        if isinstance(fp, Mapping):
            forward_path_present += 1

            state = _upper(fp.get("observation_state")) or "MISSING"
            forward_states[state] += 1

            bars = _int(fp.get("eligible_bar_count"))

            if bars >= 1:
                forward_path_with_completed_bar += 1

            if bars <= 0:
                forward_bar_buckets["0"] += 1
            elif bars == 1:
                forward_bar_buckets["1"] += 1
            elif bars <= 3:
                forward_bar_buckets["2_3"] += 1
            elif bars <= 6:
                forward_bar_buckets["4_6"] += 1
            else:
                forward_bar_buckets["7_PLUS"] += 1
        else:
            forward_states["MISSING"] += 1
            forward_bar_buckets["MISSING"] += 1

    unknown_outcome_capture_ids = (
        outcome_capture_ids - snapshot_capture_ids
    )

    release_evidence_conflicts = {
        rid: sorted(eids)
        for rid, eids in release_to_evidence.items()
        if len(eids) > 1
    }

    evidence_release_conflicts = {
        eid: sorted(rids)
        for eid, rids in evidence_to_release.items()
        if len(rids) > 1
    }

    evidence_symbol_counts = Counter(
        len(v) for v in evidence_to_symbols.values()
    )

    evidence_symbol_side_counts = Counter(
        len(v) for v in evidence_to_symbol_sides.values()
    )

    six_symbol_evidence = sum(
        1 for v in evidence_to_symbols.values()
        if len(v) == 6
    )

    non_six_symbol_evidence = sum(
        1 for v in evidence_to_symbols.values()
        if len(v) != 6
    )

    # Reuse the production-reviewed descriptive aggregator. It already owns:
    # - path eligibility;
    # - pivot eligibility;
    # - causal setup identity;
    # - longitudinal most-mature dedupe.
    from api.strategy_b_forward_path_calibration_aggregator import (
        aggregate_strategy_b_forward_path_calibration,
    )

    forward = aggregate_strategy_b_forward_path_calibration(rows)

    hard_integrity_issues: list[str] = []

    if future_data_contamination:
        hard_integrity_issues.append("FUTURE_DATA_CONTAMINATION")

    if replay_test_contamination:
        hard_integrity_issues.append("REPLAY_TEST_CONTAMINATION")

    if envelope_payload_evidence_mismatch:
        hard_integrity_issues.append(
            "ENVELOPE_PAYLOAD_EVIDENCE_ID_MISMATCH"
        )

    if release_evidence_conflicts:
        hard_integrity_issues.append(
            "RELEASE_TO_MULTIPLE_EVIDENCE_IDS"
        )

    if evidence_release_conflicts:
        hard_integrity_issues.append(
            "EVIDENCE_TO_MULTIPLE_RELEASE_IDS"
        )

    if unknown_outcome_capture_ids:
        hard_integrity_issues.append(
            "OUTCOME_REFERENCES_UNKNOWN_CAPTURE"
        )

    if execution_authority_violations:
        hard_integrity_issues.append(
            "EXECUTION_AUTHORITY_VIOLATION"
        )

    if strategy_a_mutation_violations:
        hard_integrity_issues.append(
            "STRATEGY_A_MUTATION_VIOLATION"
        )

    if observation_only_violations:
        hard_integrity_issues.append(
            "OBSERVATION_ONLY_VIOLATION"
        )

    if total_records == 0:
        dataset_status = "COLLECTING_EMPTY"
    elif hard_integrity_issues:
        dataset_status = "INTEGRITY_REVIEW_REQUIRED"
    else:
        dataset_status = "COLLECTING"

    return {
        "schema_version": SCHEMA_VERSION,
        "model": MODEL,
        "dataset_status": dataset_status,

        "policy": {
            "phase7_ready": False,
            "phase7_ready_reason": (
                "EXIT_CRITERIA_NOT_FROZEN_REPORTING_ONLY"
            ),
            "ranking_policy_ready": False,
            "production_buffer_policy_ready": False,
        },

        "records": {
            "total": total_records,
            "snapshot_records": snapshot_records,
            "outcome_records": outcome_records,
            "record_types": dict(sorted(record_types.items())),
            "unique_capture_ids": len(captures),
            "duplicate_capture_ids": duplicate_capture_ids,
        },

        "independent_identity": {
            "unique_release_ids": len(releases),
            "unique_evidence_ids": len(evidences),
            "release_to_evidence_conflict_count": len(
                release_evidence_conflicts
            ),
            "evidence_to_release_conflict_count": len(
                evidence_release_conflicts
            ),
            "release_to_evidence_conflicts": release_evidence_conflicts,
            "evidence_to_release_conflicts": evidence_release_conflicts,
        },

        "basket_coverage": {
            "evidence_with_exactly_6_symbols": six_symbol_evidence,
            "evidence_without_exactly_6_symbols": non_six_symbol_evidence,
            "evidence_symbol_count_distribution": {
                str(k): v
                for k, v in sorted(evidence_symbol_counts.items())
            },
            "evidence_symbol_side_count_distribution": {
                str(k): v
                for k, v in sorted(
                    evidence_symbol_side_counts.items()
                )
            },
            "symbols": dict(sorted(symbols.items())),
            "symbol_sides": dict(sorted(symbol_sides.items())),
        },

        "causal_integrity": {
            "canonical_causal_records": canonical_causal,
            "schedule_anomaly_records": schedule_anomalies,
            "causal_source_incomplete_records": causal_source_incomplete,
            "replay_test_contamination_records": (
                replay_test_contamination
            ),
            "future_data_contamination_records": (
                future_data_contamination
            ),
            "envelope_payload_evidence_mismatch_records": (
                envelope_payload_evidence_mismatch
            ),
            "sample_classes": dict(sorted(sample_classes.items())),
            "schedule_consistency": dict(
                sorted(schedule_consistency.items())
            ),
            "calibration_eligibility": dict(
                sorted(calibration_eligibility.items())
            ),
        },

        "isolation": {
            "sources": dict(sorted(sources.items())),
            "environments": dict(sorted(environments.items())),
            "observation_only_violations": observation_only_violations,
            "execution_authority_violations": (
                execution_authority_violations
            ),
            "strategy_a_mutation_violations": (
                strategy_a_mutation_violations
            ),
        },

        "h1_coverage": {
            "proof_statuses": dict(sorted(h1_proof_statuses.items())),
            "observation_reasons": dict(
                sorted(h1_observation_reasons.items())
            ),
        },

        "location_invalidation_coverage": {
            "location_statuses": dict(
                sorted(location_statuses.items())
            ),
            "location_block_reasons": dict(
                sorted(location_block_reasons.items())
            ),
            "invalidation_ownership_statuses": dict(
                sorted(invalidation_ownership_statuses.items())
            ),
        },

        "structural_coverage": {
            "structure_pressure": dict(
                sorted(structural_pressure.items())
            ),
            "h1_structural_room_atr_buckets": dict(
                sorted(structural_room_buckets.items())
            ),
        },

        "forward_path_coverage": {
            "records_with_forward_path": forward_path_present,
            "records_with_completed_forward_h1_bar": (
                forward_path_with_completed_bar
            ),
            "observation_states": dict(sorted(forward_states.items())),
            "eligible_bar_count_buckets": dict(
                sorted(forward_bar_buckets.items())
            ),
            "aggregator": {
                "path_candidate_records_before_dedupe": (
                    forward.get(
                        "path_candidate_records_before_dedupe", 0
                    )
                ),
                "path_duplicates_collapsed": forward.get(
                    "path_duplicates_collapsed", 0
                ),
                "path_eligible_samples": forward.get(
                    "path_eligible_samples", 0
                ),
                "pivot_eligible_samples": forward.get(
                    "pivot_eligible_samples", 0
                ),
                "path_skipped": forward.get("path_skipped", {}),
                "pivot_skipped": forward.get("pivot_skipped", {}),
            },
        },

        "outcome_linkage": {
            "outcome_records": outcome_records,
            "outcome_capture_count": len(outcome_capture_ids),
            "unknown_outcome_capture_count": len(
                unknown_outcome_capture_ids
            ),
            "unknown_outcome_capture_ids": sorted(
                unknown_outcome_capture_ids
            ),
        },

        "integrity": {
            "hard_issue_count": len(hard_integrity_issues),
            "hard_issues": hard_integrity_issues,
        },

        "observation_only": True,
        "execution_authority": False,
        "risk_authority": False,
        "strategy_a_mutation": False,
    }


def audit_jsonl_file(path: str | Path) -> dict[str, Any]:
    return audit_strategy_b_phase7_dataset(iter_jsonl(path))


def main() -> int:
    ap = argparse.ArgumentParser()

    ap.add_argument(
        "jsonl_path",
        help="Strategy-B Phase-7 calibration JSONL path",
    )

    ap.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty-print JSON output",
    )

    args = ap.parse_args()

    result = audit_jsonl_file(args.jsonl_path)

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
    "audit_strategy_b_phase7_dataset",
    "audit_jsonl_file",
    "iter_jsonl",
]
