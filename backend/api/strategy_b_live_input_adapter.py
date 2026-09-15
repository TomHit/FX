"""
XauTrendLab Strategy B - Live Input Adapter
============================================

Pure normalization boundary between existing XTL analytics outputs and the
Strategy-B decision modules.

This module:
- performs no Redis I/O;
- performs no broker/order I/O;
- does not calculate SR, turns, CHOCH/BOS, sizing, or ranking scores;
- does not mutate Strategy-A;
- does not grant execution authority.

It preserves upstream evidence and fails closed when required fields are absent.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping, Optional, Sequence


DIRECTIONS = ("BULLISH", "BEARISH")


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


def _int(v: Any) -> int:
    try:
        return int(v or 0)
    except Exception:
        return 0


def _bool_true(v: Any) -> bool:
    return v is True


def _first(mapping: Mapping[str, Any], *names: str) -> Any:
    for name in names:
        if name in mapping and mapping.get(name) is not None:
            return mapping.get(name)
    return None


@dataclass(frozen=True)
class StrategyBLiveInputs:
    schema_version: int
    model: str
    dxy_bias: Mapping[str, Any]
    dxy_m15: Mapping[str, Any]
    dxy_h1_turn: Mapping[str, Any]
    dxy_structure: Mapping[str, Any]
    pattern_observations: Mapping[str, Any]
    expression_candidates: tuple[Mapping[str, Any], ...]
    data_ok: bool
    reason: str
    reasons: tuple[str, ...]
    diagnostics: Mapping[str, Any]
    observation_only: bool = True
    execution_authority: bool = False
    strategy_a_mutation: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def normalize_dxy_bias(raw: Mapping[str, Any]) -> tuple[dict[str, Any], tuple[str, ...]]:
    """Preserve the Strategy-B macro qualification contract: timeframes.D1/H4/H1."""
    src = _mapping(raw)
    tfs = _mapping(src.get("timeframes"))
    out_tfs: dict[str, Any] = {}
    reasons: list[str] = []

    for tf in ("D1", "H4", "H1"):
        row = _mapping(tfs.get(tf))
        if not row:
            reasons.append(f"DXY_BIAS_{tf}_MISSING")
            out_tfs[tf] = {}
            continue
        out_tfs[tf] = dict(row)

    out = dict(src)
    out["timeframes"] = out_tfs
    return out, tuple(reasons)


def normalize_dxy_m15(raw: Mapping[str, Any]) -> tuple[dict[str, Any], tuple[str, ...]]:
    """
    Normalize existing M15 turn/flow state without re-interpreting it.
    Strategy-B requires a formally CONFIRMED aligned turn for timing/reaction.
    """
    src = _mapping(raw)
    turn = _mapping(src.get("turn"))
    features = _mapping(src.get("features"))
    market_flow = _mapping(features.get("market_flow"))

    status = _upper(_first(src, "status", "turn_status") or turn.get("status")) or "UNKNOWN"
    direction = _upper(_first(src, "direction", "turn_direction") or turn.get("direction")) or "UNKNOWN"
    flow_direction = _upper(market_flow.get("direction") or src.get("flow_direction")) or "UNKNOWN"

    out = dict(src)
    out["status"] = status
    out["direction"] = direction
    out["features"] = dict(features)
    out["features"]["market_flow"] = dict(market_flow)
    if flow_direction != "UNKNOWN":
        out["features"]["market_flow"]["direction"] = flow_direction

    reasons: list[str] = []
    if status == "UNKNOWN":
        reasons.append("DXY_M15_STATUS_MISSING")
    if direction == "UNKNOWN":
        reasons.append("DXY_M15_DIRECTION_MISSING")
    return out, tuple(reasons)


def normalize_dxy_h1_turn(raw: Mapping[str, Any]) -> tuple[dict[str, Any], tuple[str, ...]]:
    """
    Adapt the existing dxy_tracker H1 turn state.

    No confidence threshold is introduced here. Qualification remains owned by
    the existing H1 detector; Strategy-B later combines its qualified turn with
    bias_engine H1 swing structure and catalyst causality.
    """
    src = _mapping(raw)
    out = {
        "short_state": _upper(src.get("short_state")) or "UNKNOWN",
        "last_turn_direction": _upper(src.get("last_turn_direction")) or "UNKNOWN",
        "last_turn_ms": _int(src.get("last_turn_ms")),
        "last_evaluated_bar_close_ms": _int(src.get("last_evaluated_bar_close_ms")),
        "detected_at_ms": _int(src.get("detected_at_ms")),
        "confidence": _int(src.get("confidence")),
        "recent_net_atr": _num(src.get("recent_net_atr")),
        "prior_net_atr": _num(src.get("prior_net_atr")),
        "up_steps": _int(src.get("up_steps")),
        "down_steps": _int(src.get("down_steps")),
        "bullish_bodies": _int(src.get("bullish_bodies")),
        "bearish_bodies": _int(src.get("bearish_bodies")),
    }
    # Preserve extra upstream audit fields without overriding canonical names.
    for k, v in src.items():
        out.setdefault(k, v)

    reasons: list[str] = []
    if out["short_state"] not in DIRECTIONS:
        reasons.append("DXY_H1_SHORT_STATE_NOT_DIRECTIONAL")
    if out["last_turn_direction"] not in DIRECTIONS:
        reasons.append("DXY_H1_LAST_TURN_DIRECTION_MISSING")
    if out["last_turn_ms"] <= 0:
        reasons.append("DXY_H1_LAST_TURN_TIME_MISSING")
    return out, tuple(reasons)


def normalize_dxy_structure(raw: Mapping[str, Any]) -> tuple[dict[str, Any], tuple[str, ...]]:
    """
    Preserve existing market-structure/SR output. No SR is recomputed.

    Macro qualification deliberately accepts only D1/H4-tagged location levels.
    If the live producer supplies H1/H4 only, H4 remains usable and no D1 level
    is fabricated.
    """
    src = _mapping(raw)
    out = dict(src)
    reasons: list[str] = []

    macro_levels = []
    for name in (
        "inside_support", "nearest_support", "best_support",
        "inside_resistance", "nearest_resistance", "best_resistance",
    ):
        row = _mapping(src.get(name))
        if row:
            tf = _upper(row.get("tf"))
            if tf in ("D1", "H4"):
                macro_levels.append((name, tf))

    if not macro_levels:
        reasons.append("DXY_D1_H4_LOCATION_LEVEL_NOT_PRESENT")
    return out, tuple(reasons)


def normalize_pattern_observations(raw: Mapping[str, Any]) -> dict[str, Any]:
    """
    Normalize already-computed pattern observations. This adapter never detects
    a pattern from OHLC; absent evidence remains False/None and therefore WAIT.
    """
    src = _mapping(raw)
    bool_fields = (
        "retest_failure",
        "double_top_bottom",
        "h1_choch_bos",
        "h1_displacement",
        "h1_retest",
        "h1_order_block_validated",
        "h1_order_block_retest",
        "liquidity_sweep_rejection",
        "failed_breakout_reacceptance",
        "lower_high_higher_low",
        "ma_structure_reclaim",
        "too_far_from_invalidation",
        "move_already_consumed",
        "price_ran_without_retest",
    )
    out = {k: _bool_true(src.get(k)) for k in bool_fields}
    for k in ("distance_to_invalidation_atr", "move_consumed_atr", "structural_room_atr"):
        out[k] = _num(src.get(k))
    for k, v in src.items():
        out.setdefault(k, v)
    return out


def normalize_expression_candidate(raw: Mapping[str, Any]) -> dict[str, Any]:
    """
    Normalize one already-analysed symbol candidate for the expression ranker.
    Quality scores are not invented: absent scores remain None and ranker fails
    closed with RANKING_INPUT_INCOMPLETE once calibrated.
    """
    src = _mapping(raw)
    out = dict(src)
    out["symbol"] = _upper(src.get("symbol"))
    out["side"] = _upper(src.get("side"))
    for k in (
        "htf_location_quality", "major_structure_quality",
        "h1_transition_quality", "volatility_quality", "liquidity_quality",
        "non_usd_driver_alignment", "directional_room_atr",
        "move_consumed_atr", "spread_cost_atr",
    ):
        out[k] = _num(src.get(k))
    for k in (
        "htf_location_qualified",
        "structural_invalidation_valid",
        "symbol_data_fresh",
    ):
        if k in src:
            out[k] = _bool_true(src.get(k))
    return out


def build_strategy_b_live_inputs(
    *,
    dxy_bias: Mapping[str, Any],
    dxy_m15: Mapping[str, Any],
    dxy_h1_turn: Mapping[str, Any],
    dxy_structure: Mapping[str, Any],
    pattern_observations: Optional[Mapping[str, Any]] = None,
    expression_candidates: Optional[Sequence[Mapping[str, Any]]] = None,
) -> StrategyBLiveInputs:
    bias, bias_reasons = normalize_dxy_bias(dxy_bias)
    m15, m15_reasons = normalize_dxy_m15(dxy_m15)
    h1, h1_reasons = normalize_dxy_h1_turn(dxy_h1_turn)
    structure, structure_reasons = normalize_dxy_structure(dxy_structure)

    patterns = normalize_pattern_observations(pattern_observations or {})
    candidates = tuple(
        normalize_expression_candidate(row)
        for row in (expression_candidates or ())
        if isinstance(row, Mapping)
    )

    reasons = tuple(dict.fromkeys(
        (*bias_reasons, *m15_reasons, *h1_reasons, *structure_reasons)
    ))

    # Core adapter readiness means mandatory canonical DXY inputs are present.
    # It does NOT mean macro qualification, entry, ranking, or risk is ready.
    core_missing = {
        "DXY_BIAS_D1_MISSING",
        "DXY_BIAS_H4_MISSING",
        "DXY_BIAS_H1_MISSING",
        "DXY_M15_STATUS_MISSING",
        "DXY_M15_DIRECTION_MISSING",
    }
    data_ok = not any(r in core_missing for r in reasons)

    return StrategyBLiveInputs(
        schema_version=1,
        model="STRATEGY_B_LIVE_INPUT_ADAPTER_V1",
        dxy_bias=bias,
        dxy_m15=m15,
        dxy_h1_turn=h1,
        dxy_structure=structure,
        pattern_observations=patterns,
        expression_candidates=candidates,
        data_ok=data_ok,
        reason="LIVE_INPUTS_NORMALIZED" if data_ok else "MANDATORY_LIVE_INPUTS_MISSING",
        reasons=reasons,
        diagnostics={
            "candidate_count": len(candidates),
            "pattern_observation_count": len(patterns),
            "dxy_h1_turn_directional": h1.get("short_state") in DIRECTIONS,
            "dxy_macro_location_present": "DXY_D1_H4_LOCATION_LEVEL_NOT_PRESENT" not in structure_reasons,
        },
    )


__all__ = [
    "StrategyBLiveInputs",
    "normalize_dxy_bias",
    "normalize_dxy_m15",
    "normalize_dxy_h1_turn",
    "normalize_dxy_structure",
    "normalize_pattern_observations",
    "normalize_expression_candidate",
    "build_strategy_b_live_inputs",
]
