"""
XauTrendLab Strategy B - Expression Calibration Snapshot
========================================================

Pure observation/replay serialization for Strategy-B expression calibration.

Purpose
-------
Freeze one deterministic candidate row from the already-computed Strategy-B
observation stack so live/demo/replay samples can later be compared without
inventing thresholds or ranking weights.

This module does NOT:
- fetch Redis/broker data;
- calculate SR, pivots, DXY, news, events, ranking, or risk;
- write Redis;
- mutate Strategy A;
- select an expression;
- qualify HTF location;
- approve a structural stop;
- attach future outcome data automatically.

It only normalizes evidence that already exists in:
- StrategyBLiveExpressionBasket / candidate rows;
- HTF location observation;
- structural invalidation observation;
- invalidation ownership observation;
- expression ranker diagnostics.

Outcome labeling is intentionally a separate explicit step so replay/live
capture cannot accidentally leak future information into entry-time evidence.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from typing import Any, Mapping, Optional
import json


def _mapping(v: Any) -> Mapping[str, Any]:
    return v if isinstance(v, Mapping) else {}


def _upper(v: Any) -> str:
    return str(v or "").strip().upper()


def _num(v: Any) -> Optional[float]:
    try:
        x = float(v)
        return x if x == x else None
    except Exception:
        return None


def _bool_or_none(v: Any) -> Optional[bool]:
    return v if isinstance(v, bool) else None


def _candidate_by_tf(
    invalidation: Mapping[str, Any],
    tf: str,
) -> Mapping[str, Any]:
    for row in invalidation.get("candidates") or ():
        if isinstance(row, Mapping) and _upper(row.get("timeframe")) == tf:
            return row
    return {}


@dataclass(frozen=True)
class StrategyBExpressionCalibrationSnapshot:
    schema_version: int
    model: str

    captured_at_ms: int
    expected_dxy_direction: str
    usd_thesis: str

    symbol: str
    side: str
    acquisition_ok: bool
    acquisition_reason: str

    # Location / structure.
    location_status: str
    location_hard_blocked: bool
    location_block_reason: Optional[str]
    location_observed: bool
    location_qualified: bool

    d1_structure_state: str
    d1_high_pattern: Optional[str]
    d1_low_pattern: Optional[str]
    d1_regime: str
    d1_direction: str
    d1_range_position: Optional[float]
    d1_net_atr: Optional[float]
    d1_anchor_distance_atr: Optional[float]
    d1_opposing_room_atr: Optional[float]

    canonical_directional_room_atr: Optional[float]
    structure_pressure: str
    structure_pressure_reason: str
    inside_support: bool
    inside_resistance: bool

    # Structural invalidation observation.
    invalidation_ownership_status: str
    htf_invalidation_available: bool
    d1_invalidation_available: bool
    h4_invalidation_available: bool
    h1_invalidation_available: bool
    h1_only_invalidation_requires_proof: bool

    d1_invalidation_price: Optional[float]
    d1_invalidation_distance_price: Optional[float]
    d1_invalidation_distance_atr: Optional[float]
    h4_invalidation_price: Optional[float]
    h4_invalidation_distance_price: Optional[float]
    h4_invalidation_distance_atr: Optional[float]
    h1_invalidation_price: Optional[float]
    h1_invalidation_distance_price: Optional[float]
    h1_invalidation_distance_atr: Optional[float]

    structural_invalidation_valid: bool
    selected_invalidation_timeframe: Optional[str]
    selected_invalidation_price: Optional[float]

    # Ranker diagnostics at capture time.
    ranker_calibrated: bool
    ranker_calibration_reason: str
    ranker_eligible: Optional[bool]
    ranker_score: Optional[float]
    ranker_veto_reasons: tuple[str, ...]
    basket_decision: str
    basket_reason: str
    basket_best_symbol: Optional[str]
    basket_best_side: Optional[str]

    # V6.4 structural-owner + buffer calibration evidence.
    # Defaults preserve compatibility with historical V1 snapshots.
    htf_owner_resolution_decision: str = "UNKNOWN"
    htf_owner_resolution_reason: str = "UNKNOWN"
    resolved_preferred_htf_owner: Optional[str] = None
    buffer_calibration_reason: str = "UNKNOWN"
    buffer_calibration_owner_timeframe: Optional[str] = None
    buffer_entry_to_pivot_distance_price: Optional[float] = None
    buffer_entry_to_pivot_distance_atr: Optional[float] = None
    buffer_bars_after_pivot_count: int = 0
    buffer_post_pivot_extreme_price: Optional[float] = None
    buffer_max_pivot_breach_price: Optional[float] = None
    buffer_max_pivot_breach_atr: Optional[float] = None
    buffer_breach_count: int = 0
    buffer_first_breach_ms: int = 0
    buffer_last_breach_ms: int = 0
    buffer_calibration_ready: bool = False
    buffer_proposed_structural_buffer_price: Optional[float] = None

    # Longitudinal market-path analytics measured only from completed H1 bars
    # after the frozen causal H1 break. This remains observation-only and is
    # NOT an outcome label / WIN / LOSS / production exit decision.
    forward_path_observation: Optional[Mapping[str, Any]] = None

    # Future-result fields remain empty at evidence-capture time.
    outcome_attached: bool = False
    outcome_label: Optional[str] = None
    outcome_close_reason: Optional[str] = None
    outcome_mfe_r: Optional[float] = None
    outcome_mae_r: Optional[float] = None
    outcome_net_r: Optional[float] = None
    outcome_metadata: Optional[Mapping[str, Any]] = None

    observation_only: bool = True
    execution_authority: bool = False
    strategy_a_mutation: bool = False
    future_data_used: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def build_strategy_b_expression_calibration_snapshot(
    *,
    basket: Any,
    candidate: Any,
) -> StrategyBExpressionCalibrationSnapshot:
    """
    Freeze one candidate using only evidence already present at decision time.

    `basket` and `candidate` may be dataclasses/objects from the live observer
    or plain mappings reconstructed during replay.
    """
    basket_map = (
        basket.to_dict()
        if hasattr(basket, "to_dict")
        else dict(basket)
        if isinstance(basket, Mapping)
        else {}
    )
    candidate_map = (
        candidate.to_dict()
        if hasattr(candidate, "to_dict")
        else dict(candidate)
        if isinstance(candidate, Mapping)
        else {}
    )

    htf = _mapping(candidate_map.get("htf_location"))
    inval = _mapping(candidate_map.get("structural_invalidation"))
    owner = _mapping(candidate_map.get("invalidation_ownership"))
    owner_resolution = _mapping(
        candidate_map.get("htf_invalidation_owner_resolution")
    )
    buffer_cal = _mapping(
        candidate_map.get("structural_buffer_calibration")
    )
    finalization = _mapping(
        candidate_map.get("structural_invalidation_finalization")
    )
    forward_path = _mapping(
        candidate_map.get("forward_path_observation")
    )
    ranker_candidate = _mapping(candidate_map.get("ranker_candidate"))
    ranking = _mapping(basket_map.get("ranking"))

    ranked_row: Mapping[str, Any] = {}
    symbol = _upper(candidate_map.get("symbol"))
    side = _upper(candidate_map.get("side"))
    for row in ranking.get("ranked") or ():
        if not isinstance(row, Mapping):
            continue
        if (
            _upper(row.get("symbol")) == symbol
            and _upper(row.get("side")) == side
        ):
            ranked_row = row
            break

    d1_inv = _candidate_by_tf(inval, "D1")
    h4_inv = _candidate_by_tf(inval, "H4")
    h1_inv = _candidate_by_tf(inval, "H1")

    expected = _upper(basket_map.get("expected_dxy_direction"))
    usd_thesis = (
        "USD_BEARISH"
        if expected == "BEARISH"
        else "USD_BULLISH"
        if expected == "BULLISH"
        else "UNKNOWN"
    )

    return StrategyBExpressionCalibrationSnapshot(
        schema_version=1,
        model="STRATEGY_B_EXPRESSION_CALIBRATION_SNAPSHOT_V1",
        captured_at_ms=int(basket_map.get("as_of_ms") or 0),
        expected_dxy_direction=expected or "UNKNOWN",
        usd_thesis=usd_thesis,
        symbol=symbol,
        side=side,
        acquisition_ok=candidate_map.get("acquisition_ok") is True,
        acquisition_reason=str(
            candidate_map.get("acquisition_reason") or "UNKNOWN"
        ),
        location_status=str(
            htf.get("htf_location_status") or "UNKNOWN"
        ),
        location_hard_blocked=(
            htf.get("htf_location_hard_blocked") is True
        ),
        location_block_reason=(
            str(htf.get("htf_location_block_reason"))
            if htf.get("htf_location_block_reason") is not None
            else None
        ),
        location_observed=htf.get("htf_location_observed") is True,
        location_qualified=htf.get("htf_location_qualified") is True,
        d1_structure_state=_upper(
            htf.get("d1_structure_state")
        ) or "UNKNOWN",
        d1_high_pattern=_upper(htf.get("d1_high_pattern")) or None,
        d1_low_pattern=_upper(htf.get("d1_low_pattern")) or None,
        d1_regime=_upper(htf.get("d1_regime")) or "UNKNOWN",
        d1_direction=_upper(htf.get("d1_direction")) or "UNKNOWN",
        d1_range_position=_num(htf.get("d1_range_position")),
        d1_net_atr=_num(htf.get("d1_net_atr")),
        d1_anchor_distance_atr=_num(
            htf.get("d1_anchor_distance_atr")
        ),
        d1_opposing_room_atr=_num(
            htf.get("d1_opposing_room_atr")
        ),
        canonical_directional_room_atr=_num(
            htf.get("h4_directional_room_atr")
        ),
        structure_pressure=_upper(
            htf.get("h4_structure_pressure")
        ) or "UNKNOWN",
        structure_pressure_reason=str(
            htf.get("h4_structure_pressure_reason") or "UNKNOWN"
        ),
        inside_support=htf.get("inside_support") is True,
        inside_resistance=htf.get("inside_resistance") is True,
        invalidation_ownership_status=str(
            owner.get("status") or "UNKNOWN"
        ),
        htf_invalidation_available=(
            owner.get("htf_candidate_available") is True
        ),
        d1_invalidation_available=(
            owner.get("d1_candidate_available") is True
        ),
        h4_invalidation_available=(
            owner.get("h4_candidate_available") is True
        ),
        h1_invalidation_available=(
            owner.get("h1_candidate_available") is True
        ),
        h1_only_invalidation_requires_proof=(
            owner.get("h1_only_requires_proof") is True
        ),
        d1_invalidation_price=_num(d1_inv.get("price")),
        d1_invalidation_distance_price=_num(
            d1_inv.get("distance_price")
        ),
        d1_invalidation_distance_atr=_num(
            d1_inv.get("distance_atr")
        ),
        h4_invalidation_price=_num(h4_inv.get("price")),
        h4_invalidation_distance_price=_num(
            h4_inv.get("distance_price")
        ),
        h4_invalidation_distance_atr=_num(
            h4_inv.get("distance_atr")
        ),
        h1_invalidation_price=_num(h1_inv.get("price")),
        h1_invalidation_distance_price=_num(
            h1_inv.get("distance_price")
        ),
        h1_invalidation_distance_atr=_num(
            h1_inv.get("distance_atr")
        ),
        structural_invalidation_valid=(
            ranker_candidate.get("structural_invalidation_valid") is True
        ),
        selected_invalidation_timeframe=(
            str(finalization.get("selected_timeframe"))
            if finalization.get("selected_timeframe") is not None
            else str(owner.get("selected_timeframe"))
            if owner.get("selected_timeframe") is not None
            else None
        ),
        selected_invalidation_price=_num(
            finalization.get("structural_invalidation_price")
            if finalization.get("structural_invalidation_price") is not None
            else owner.get("structural_invalidation_price")
        ),
        ranker_calibrated=ranking.get("calibrated") is True,
        ranker_calibration_reason=str(
            ranking.get("calibration_reason") or "UNKNOWN"
        ),
        ranker_eligible=(
            bool(ranked_row.get("eligible"))
            if isinstance(ranked_row.get("eligible"), bool)
            else None
        ),
        ranker_score=_num(ranked_row.get("score")),
        ranker_veto_reasons=tuple(
            str(x) for x in (ranked_row.get("veto_reasons") or ())
        ),
        basket_decision=str(
            ranking.get("decision")
            or basket_map.get("decision")
            or "WAIT"
        ),
        basket_reason=str(
            ranking.get("reason")
            or basket_map.get("reason")
            or "UNKNOWN"
        ),
        basket_best_symbol=(
            str(ranking.get("best_symbol"))
            if ranking.get("best_symbol") is not None
            else None
        ),
        basket_best_side=(
            str(ranking.get("best_side"))
            if ranking.get("best_side") is not None
            else None
        ),
        htf_owner_resolution_decision=str(
            owner_resolution.get("decision") or "UNKNOWN"
        ),
        htf_owner_resolution_reason=str(
            owner_resolution.get("reason") or "UNKNOWN"
        ),
        resolved_preferred_htf_owner=(
            str(owner_resolution.get("preferred_htf_owner"))
            if owner_resolution.get("preferred_htf_owner") is not None
            else None
        ),
        buffer_calibration_reason=str(
            buffer_cal.get("reason") or "UNKNOWN"
        ),
        buffer_calibration_owner_timeframe=(
            str(buffer_cal.get("owner_timeframe"))
            if buffer_cal.get("owner_timeframe") is not None
            else None
        ),
        buffer_entry_to_pivot_distance_price=_num(
            buffer_cal.get("entry_to_pivot_distance_price")
        ),
        buffer_entry_to_pivot_distance_atr=_num(
            buffer_cal.get("entry_to_pivot_distance_atr")
        ),
        buffer_bars_after_pivot_count=int(
            buffer_cal.get("bars_after_pivot_count") or 0
        ),
        buffer_post_pivot_extreme_price=_num(
            buffer_cal.get("post_pivot_extreme_price")
        ),
        buffer_max_pivot_breach_price=_num(
            buffer_cal.get("max_pivot_breach_price")
        ),
        buffer_max_pivot_breach_atr=_num(
            buffer_cal.get("max_pivot_breach_atr")
        ),
        buffer_breach_count=int(
            buffer_cal.get("breach_count") or 0
        ),
        buffer_first_breach_ms=int(
            buffer_cal.get("first_breach_ms") or 0
        ),
        buffer_last_breach_ms=int(
            buffer_cal.get("last_breach_ms") or 0
        ),
        buffer_calibration_ready=(
            buffer_cal.get("buffer_calibration_ready") is True
        ),
        buffer_proposed_structural_buffer_price=_num(
            buffer_cal.get("proposed_structural_buffer_price")
        ),
        forward_path_observation=(
            dict(forward_path)
            if forward_path
            else None
        ),
    )


def build_strategy_b_basket_calibration_snapshots(
    basket: Any,
) -> tuple[StrategyBExpressionCalibrationSnapshot, ...]:
    """
    Freeze all candidates from one basket observation.
    """
    basket_map = (
        basket.to_dict()
        if hasattr(basket, "to_dict")
        else dict(basket)
        if isinstance(basket, Mapping)
        else {}
    )
    candidates = getattr(basket, "candidates", None)
    if candidates is None:
        candidates = basket_map.get("candidates") or ()

    return tuple(
        build_strategy_b_expression_calibration_snapshot(
            basket=basket,
            candidate=row,
        )
        for row in candidates
    )


def attach_strategy_b_calibration_outcome(
    snapshot: StrategyBExpressionCalibrationSnapshot,
    *,
    outcome_label: str,
    close_reason: Optional[str] = None,
    mfe_r: Any = None,
    mae_r: Any = None,
    net_r: Any = None,
    metadata: Optional[Mapping[str, Any]] = None,
) -> StrategyBExpressionCalibrationSnapshot:
    """
    Explicit post-hoc outcome labeling.

    This returns a NEW snapshot and leaves the original entry-time evidence
    unchanged. The separate function boundary is intentional to make future
    leakage visible during replay/calibration review.
    """
    if snapshot.outcome_attached:
        raise ValueError("OUTCOME_ALREADY_ATTACHED")

    return replace(
        snapshot,
        outcome_attached=True,
        outcome_label=str(outcome_label or "").strip() or "UNKNOWN",
        outcome_close_reason=(
            str(close_reason).strip()
            if close_reason is not None
            else None
        ),
        outcome_mfe_r=_num(mfe_r),
        outcome_mae_r=_num(mae_r),
        outcome_net_r=_num(net_r),
        outcome_metadata=dict(metadata or {}),
        # Entry-time computation still did not use future data.
        future_data_used=False,
    )


def strategy_b_calibration_snapshot_jsonl(
    snapshot: StrategyBExpressionCalibrationSnapshot,
) -> str:
    return json.dumps(
        snapshot.to_dict(),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


__all__ = [
    "StrategyBExpressionCalibrationSnapshot",
    "build_strategy_b_expression_calibration_snapshot",
    "build_strategy_b_basket_calibration_snapshots",
    "attach_strategy_b_calibration_outcome",
    "strategy_b_calibration_snapshot_jsonl",
]
