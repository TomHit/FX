"""
XauTrendLab Strategy B - Live Calibration Capture With Causal H1 Evidence
=======================================================================

Production-safe calibration capture orchestrator.

This module composes:
- an already-built Strategy-B expression basket;
- canonical evidence_id;
- evidence-specific causal H1 live observations for every basket candidate;
- pure H1 calibration enrichment;
- the existing validated append-only JSONL writer.

It has NO broker/order/risk authority, performs NO Redis SCAN, and does not
mutate Strategy A. If any H1 observation cannot be built from the canonical
evidence boundary, the whole capture fails before any JSONL write.
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


def _key(symbol: Any, side: Any) -> tuple[str, str]:
    return (
        str(symbol or "").strip().upper(),
        str(side or "").strip().upper(),
    )


def capture_strategy_b_live_calibration_with_h1(
    *,
    R: Any,
    store: Any,
    basket: Any,
    evidence_id: str,
    snapshot_builder: Any = None,
    h1_observer: Any = None,
    h1_enricher: Any = None,
    capture_fn: Any = None,
    as_of_ms: Optional[int] = None,
    **capture_kwargs: Any,
) -> dict[str, Any]:
    """
    Capture one complete expression basket with causal H1 evidence.

    All H1 rows must resolve from the same canonical evidence_id.
    Schedule anomalies are preserved as NONCALIBRATION rows by the enricher;
    they are not silently discarded because they remain useful plumbing data.
    """
    eid = str(evidence_id or "").strip()
    if not eid:
        return {
            "ok": False,
            "written": 0,
            "duplicate": 0,
            "reason": "EVIDENCE_ID_MISSING",
            "rows": [],
        }

    b = _to_dict(basket)
    candidates = b.get("candidates")
    if not isinstance(candidates, (list, tuple)) or not candidates:
        return {
            "ok": False,
            "written": 0,
            "duplicate": 0,
            "reason": "EXPRESSION_BASKET_CANDIDATES_MISSING",
            "rows": [],
        }

    if snapshot_builder is None:
        from api.strategy_b_expression_calibration_snapshot import (
            build_strategy_b_basket_calibration_snapshots,
        )
        snapshot_builder = build_strategy_b_basket_calibration_snapshots

    if h1_observer is None:
        from api.strategy_b_symbol_h1_live_observer import (
            observe_strategy_b_symbol_h1_live,
        )
        h1_observer = observe_strategy_b_symbol_h1_live

    if h1_enricher is None:
        from api.strategy_b_h1_calibration_enrichment import (
            enrich_strategy_b_calibration_snapshot_with_h1,
        )
        h1_enricher = enrich_strategy_b_calibration_snapshot_with_h1

    if capture_fn is None:
        from api.strategy_b_calibration_jsonl import (
            capture_validated_basket_jsonl,
        )
        capture_fn = capture_validated_basket_jsonl

    snapshots = tuple(snapshot_builder(basket))
    if len(snapshots) != len(candidates):
        return {
            "ok": False,
            "written": 0,
            "duplicate": 0,
            "reason": "SNAPSHOT_COUNT_MISMATCH",
            "candidate_count": len(candidates),
            "snapshot_count": len(snapshots),
            "rows": [],
        }

    snap_by_key: dict[tuple[str, str], Any] = {}
    for snap in snapshots:
        s = _to_dict(snap)
        k = _key(s.get("symbol"), s.get("side"))
        if not all(k) or k in snap_by_key:
            return {
                "ok": False,
                "written": 0,
                "duplicate": 0,
                "reason": "SNAPSHOT_SYMBOL_SIDE_INVALID_OR_DUPLICATE",
                "symbol": k[0],
                "side": k[1],
                "rows": [],
            }
        snap_by_key[k] = snap

    enriched: list[dict[str, Any]] = []
    h1_rows: list[dict[str, Any]] = []

    for candidate in candidates:
        c = _to_dict(candidate)
        symbol, side = _key(c.get("symbol"), c.get("side"))
        snap = snap_by_key.get((symbol, side))
        if snap is None:
            return {
                "ok": False,
                "written": 0,
                "duplicate": 0,
                "reason": "SNAPSHOT_NOT_FOUND_FOR_CANDIDATE",
                "symbol": symbol,
                "side": side,
                "rows": [],
            }

        try:
            obs = h1_observer(
                R,
                symbol=symbol,
                side=side,
                evidence_id=eid,
                as_of_ms=as_of_ms,
            )
            od = _to_dict(obs)
        except Exception as exc:
            return {
                "ok": False,
                "written": 0,
                "duplicate": 0,
                "reason": f"H1_OBSERVER_EXCEPTION:{type(exc).__name__}",
                "symbol": symbol,
                "side": side,
                "rows": [],
            }

        if od.get("ok") is not True:
            return {
                "ok": False,
                "written": 0,
                "duplicate": 0,
                "reason": "H1_OBSERVATION_FAILED",
                "symbol": symbol,
                "side": side,
                "h1_reason": od.get("reason"),
                "rows": [],
            }

        if str(od.get("evidence_id") or "").strip() != eid:
            return {
                "ok": False,
                "written": 0,
                "duplicate": 0,
                "reason": "H1_EVIDENCE_ID_MISMATCH",
                "symbol": symbol,
                "side": side,
                "rows": [],
            }

        try:
            enriched_row = h1_enricher(snap, od)
        except Exception as exc:
            return {
                "ok": False,
                "written": 0,
                "duplicate": 0,
                "reason": f"H1_ENRICHMENT_FAILED:{type(exc).__name__}",
                "symbol": symbol,
                "side": side,
                "rows": [],
            }

        enriched.append(enriched_row)
        h1_rows.append({
            "symbol": symbol,
            "side": side,
            "proof_status": enriched_row.get("h1_proof_status"),
            "sample_class": enriched_row.get("h1_sample_class"),
            "calibration_eligible": enriched_row.get("h1_calibration_eligible"),
            "break_ms": enriched_row.get("h1_break_ms"),
        })

    def enriched_builder(_basket: Any):
        return tuple(enriched)

    result = capture_fn(
        store=store,
        basket=basket,
        snapshot_builder=enriched_builder,
        evidence_id=eid,
        **capture_kwargs,
    )

    out = dict(result)
    out["evidence_id"] = eid
    out["h1_rows"] = h1_rows
    out["h1_all_observed"] = True
    out["h1_calibration_eligible_count"] = sum(
        1 for row in enriched
        if row.get("h1_calibration_eligible") is True
    )
    out["h1_noncalibration_count"] = sum(
        1 for row in enriched
        if row.get("h1_calibration_eligible") is not True
    )
    return out


__all__ = [
    "capture_strategy_b_live_calibration_with_h1",
]
