"""
XauTrendLab Strategy B - H1 Calibration Enrichment
==================================================

Pure observation/replay enrichment for Strategy-B expression calibration rows.

It combines an existing expression calibration snapshot with an evidence-specific
Strategy-B symbol H1 live observation. It does not fetch data, write Redis, place
orders, reserve risk, mutate Strategy A, or invent thresholds.

Important:
- canonical catalyst timing must already be resolved by the H1 live observer;
- a scheduled-release anomaly (schedule_consistent == False) is preserved and
  explicitly marked calibration_eligible=False;
- direct replay/test timestamp overrides are preserved and are not treated as
  canonical live calibration samples.
"""
from __future__ import annotations

from dataclasses import asdict, is_dataclass
from typing import Any, Mapping, Optional


def _to_dict(v: Any) -> dict[str, Any]:
    if hasattr(v, "to_dict"):
        v = v.to_dict()
    elif is_dataclass(v):
        v = asdict(v)
    if not isinstance(v, Mapping):
        raise TypeError("EXPECTED_MAPPING_DATACLASS_OR_TO_DICT")
    return dict(v)


def _num(v: Any) -> Optional[float]:
    try:
        x = float(v)
        return x if x == x else None
    except Exception:
        return None


def _int(v: Any) -> int:
    try:
        return int(v or 0)
    except Exception:
        return 0


def _upper(v: Any) -> str:
    return str(v or "").strip().upper()


def enrich_strategy_b_calibration_snapshot_with_h1(
    snapshot: Any,
    h1_observation: Any,
) -> dict[str, Any]:
    """
    Return a NEW mapping containing the original entry-time snapshot plus
    evidence-specific H1 raw/proof fields.
    """
    s = _to_dict(snapshot)
    h = _to_dict(h1_observation)

    if s.get("future_data_used") is True:
        raise ValueError("H1_ENRICHMENT_REJECTED_FUTURE_DATA_USED")
    if s.get("outcome_attached") is True:
        raise ValueError("H1_ENRICHMENT_REJECTED_OUTCOME_ALREADY_ATTACHED")

    s_symbol = _upper(s.get("symbol"))
    s_side = _upper(s.get("side"))
    h_symbol = _upper(h.get("symbol"))
    h_side = _upper(h.get("side"))

    if not s_symbol or not s_side:
        raise ValueError("H1_ENRICHMENT_SNAPSHOT_SYMBOL_SIDE_MISSING")
    if h_symbol != s_symbol or h_side != s_side:
        raise ValueError("H1_ENRICHMENT_SYMBOL_SIDE_MISMATCH")

    pattern = h.get("pattern") if isinstance(h.get("pattern"), Mapping) else {}
    proof = h.get("proof") if isinstance(h.get("proof"), Mapping) else {}

    evidence_id = str(h.get("evidence_id") or "").strip() or None
    catalyst_source = str(h.get("catalyst_source_field") or "").strip() or None
    schedule_consistent = h.get("schedule_consistent")
    if not isinstance(schedule_consistent, bool):
        schedule_consistent = None

    canonical_source = catalyst_source in {
        "context.first_result_seen_ms",
        "evidence.first_seen_at_ms",
    }
    direct_override = catalyst_source == "DIRECT_REPLAY_OR_TEST_OVERRIDE"

    if schedule_consistent is False:
        sample_class = "SCHEDULE_ANOMALY_NONCALIBRATION"
    elif direct_override:
        sample_class = "REPLAY_OR_TEST_OVERRIDE_NONCALIBRATION"
    elif canonical_source and evidence_id:
        sample_class = "CANONICAL_CAUSAL"
    else:
        sample_class = "CAUSAL_SOURCE_INCOMPLETE_NONCALIBRATION"

    calibration_eligible = bool(
        h.get("ok") is True
        and canonical_source
        and evidence_id
        and schedule_consistent is not False
    )

    out = dict(s)
    out.update({
        "h1_enrichment_schema_version": 1,
        "h1_enrichment_model": "STRATEGY_B_H1_CALIBRATION_ENRICHMENT_V1",
        "h1_observation_attached": True,
        "h1_observation_model": h.get("model"),
        "h1_observation_ok": h.get("ok") is True,
        "h1_observation_reason": str(h.get("reason") or "UNKNOWN"),

        "h1_evidence_id": evidence_id,
        "h1_catalyst_first_seen_ms": _int(h.get("catalyst_first_seen_ms")),
        "h1_catalyst_source_field": catalyst_source,
        "h1_schedule_consistent": schedule_consistent,
        "h1_sample_class": sample_class,
        "h1_calibration_eligible": calibration_eligible,

        "h1_pattern_reason": str(pattern.get("reason") or "UNKNOWN"),
        "h1_choch_bos": pattern.get("h1_choch_bos") is True,
        "h1_break_ms": _int(pattern.get("break_ms")),
        "h1_break_direction": (
            _upper(pattern.get("break_direction")) or None
        ),
        "h1_break_level": _num(pattern.get("break_level")),
        "h1_break_close": _num(pattern.get("break_close")),

        # Immutable causal ATR from the same H1 pattern observation that
        # produced h1_break_ms / h1_break_close. Forward-path calibration must
        # never normalize against a later live ATR.
        "h1_reference_atr": _num(pattern.get("atr")),

        "h1_displacement_atr_raw": _num(pattern.get("displacement_atr")),
        "h1_displacement_qualified": pattern.get("h1_displacement") is True,
        "h1_retest_seen": pattern.get("retest_seen") is True,
        "h1_retest_distance_atr": _num(pattern.get("retest_distance_atr")),
        "h1_retest_qualified": pattern.get("h1_retest") is True,
        "h1_retest_failure": pattern.get("retest_failure") is True,
        "h1_move_consumed_atr": _num(pattern.get("move_consumed_atr")),
        "h1_distance_to_invalidation_atr": _num(
            pattern.get("distance_to_invalidation_atr")
        ),
        "h1_structural_room_atr": _num(pattern.get("structural_room_atr")),

        "h1_proof_status": str(proof.get("status") or "UNKNOWN"),
        "h1_proof_reason": str(proof.get("reason") or "UNKNOWN"),
        "h1_reversal_proven": proof.get("h1_reversal_proven") is True,
        "h1_primary_proof_path": proof.get("primary_proof_path"),
        "h1_proven_paths": list(proof.get("proven_paths") or []),
        "h1_developing_evidence": list(proof.get("developing_evidence") or []),
        "h1_supporting_evidence": list(proof.get("supporting_evidence") or []),

        # Preserve original anti-leakage flags.
        "future_data_used": False,
    })
    return out


__all__ = [
    "enrich_strategy_b_calibration_snapshot_with_h1",
]
