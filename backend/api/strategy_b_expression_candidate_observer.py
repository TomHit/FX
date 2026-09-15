"""
XauTrendLab Strategy B - Expression Candidate Observer
======================================================

Pure observation-only adapter that converts already-computed symbol analytics
into the candidate contract consumed by strategy_b_expression_ranker.py.

This module does NOT:
- fetch symbol OHLC;
- calculate support/resistance;
- invent ranking weights or 0..1 quality scores;
- reserve risk;
- call broker/order APIs;
- mutate Strategy A.

It preserves deterministic symbol gates and raw measurements. Quality fields
remain None until a separately calibrated scorer is approved.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping, Optional, Sequence

from api.strategy_b_expression_ranker import (
    USD_BEARISH_EXPRESSIONS,
    USD_BULLISH_EXPRESSIONS,
)


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


def _bool(v: Any) -> Optional[bool]:
    return v if isinstance(v, bool) else None


def _first(m: Mapping[str, Any], *names: str) -> Any:
    for name in names:
        if name in m and m.get(name) is not None:
            return m.get(name)
    return None


def _approved_map(expected_dxy_direction: str) -> Mapping[str, str]:
    d = _upper(expected_dxy_direction)
    if d == "BEARISH":
        return USD_BEARISH_EXPRESSIONS
    if d == "BULLISH":
        return USD_BULLISH_EXPRESSIONS
    return {}


def _extract_structure_state(row: Mapping[str, Any], tf: str) -> str:
    tfrow = _mapping(_mapping(row.get("timeframes")).get(tf))
    structure = _mapping(tfrow.get("structure"))
    return _upper(structure.get("state")) or "UNKNOWN"


def _extract_tf_fresh(row: Mapping[str, Any], tf: str) -> Optional[bool]:
    tfrow = _mapping(_mapping(row.get("timeframes")).get(tf))
    if not tfrow:
        return None
    fresh = tfrow.get("fresh")
    if isinstance(fresh, bool):
        return fresh
    data_ok = tfrow.get("data_ok")
    return data_ok if isinstance(data_ok, bool) else None


def _extract_room(row: Mapping[str, Any], side: str) -> Optional[float]:
    structure = _mapping(
        row.get("sr")
        or row.get("structure")
        or row.get("market_structure")
    )
    direct = _num(structure.get("directional_room_atr"))
    if direct is not None:
        return direct
    if side == "BUY":
        return _num(structure.get("room_up_atr"))
    if side == "SELL":
        return _num(structure.get("room_down_atr"))
    return None


def _extract_location_qualified(row: Mapping[str, Any], side: str) -> Optional[bool]:
    explicit = _bool(
        _first(
            row,
            "htf_location_qualified",
            "location_qualified",
        )
    )
    if explicit is not None:
        return explicit

    structure = _mapping(
        row.get("sr")
        or row.get("structure")
        or row.get("market_structure")
    )
    if side == "BUY":
        for key in ("inside_support", "nearest_support", "best_support"):
            level = _mapping(structure.get(key))
            if _upper(level.get("tf")) in ("D1", "H4"):
                return True
    elif side == "SELL":
        for key in ("inside_resistance", "nearest_resistance", "best_resistance"):
            level = _mapping(structure.get(key))
            if _upper(level.get("tf")) in ("D1", "H4"):
                return True
    return None


def _extract_structural_invalidation_valid(
    row: Mapping[str, Any],
    side: str,
) -> Optional[bool]:
    explicit = _bool(
        _first(
            row,
            "structural_invalidation_valid",
            "invalidation_valid",
        )
    )
    if explicit is not None:
        return explicit

    invalidation = _num(
        _first(
            row,
            "structural_invalidation_price",
            "invalidation_price",
            "structural_sl_reference",
        )
    )
    price = _num(_first(row, "price", "live_price", "last_price"))

    if invalidation is None or price is None:
        return None
    if side == "BUY":
        return invalidation < price
    if side == "SELL":
        return invalidation > price
    return None


def _symbol_data_fresh(row: Mapping[str, Any]) -> Optional[bool]:
    explicit = _bool(_first(row, "symbol_data_fresh", "data_fresh"))
    if explicit is not None:
        return explicit

    states = [_extract_tf_fresh(row, tf) for tf in ("D1", "H4", "H1")]
    known = [x for x in states if x is not None]
    if not known:
        return None
    return all(known)


@dataclass(frozen=True)
class StrategyBExpressionCandidateObservation:
    schema_version: int
    model: str
    expected_dxy_direction: str
    symbol: str
    side: str
    approved_expression: bool

    htf_location_qualified: Optional[bool]
    structural_invalidation_valid: Optional[bool]
    symbol_data_fresh: Optional[bool]

    d1_structure_state: str
    h4_structure_state: str
    h1_structure_state: str

    directional_room_atr: Optional[float]
    move_consumed_atr: Optional[float]
    spread_cost_atr: Optional[float]

    # Ranking qualities deliberately not inferred.
    htf_location_quality: Optional[float]
    major_structure_quality: Optional[float]
    h1_transition_quality: Optional[float]
    volatility_quality: Optional[float]
    liquidity_quality: Optional[float]
    non_usd_driver_alignment: Optional[float]

    reason: str
    reasons: tuple[str, ...]
    raw: Mapping[str, Any]

    observation_only: bool = True
    execution_authority: bool = False
    strategy_a_mutation: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_ranker_candidate(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "side": self.side,
            "htf_location_qualified": self.htf_location_qualified,
            "structural_invalidation_valid":
                self.structural_invalidation_valid,
            "symbol_data_fresh": self.symbol_data_fresh,
            "htf_location_quality": self.htf_location_quality,
            "major_structure_quality": self.major_structure_quality,
            "h1_transition_quality": self.h1_transition_quality,
            "volatility_quality": self.volatility_quality,
            "directional_room_atr": self.directional_room_atr,
            "move_consumed_atr": self.move_consumed_atr,
            "liquidity_quality": self.liquidity_quality,
            "spread_cost_atr": self.spread_cost_atr,
            "non_usd_driver_alignment":
                self.non_usd_driver_alignment,
            "_strategy_b_expression_observer": {
                "approved_expression": self.approved_expression,
                "d1_structure_state": self.d1_structure_state,
                "h4_structure_state": self.h4_structure_state,
                "h1_structure_state": self.h1_structure_state,
                "reason": self.reason,
                "reasons": list(self.reasons),
                "qualities_calibrated": False,
                "observation_only": True,
                "execution_authority": False,
            },
        }


def observe_strategy_b_expression_candidate(
    *,
    expected_dxy_direction: str,
    symbol_snapshot: Mapping[str, Any],
) -> StrategyBExpressionCandidateObservation:
    row = _mapping(symbol_snapshot)
    expected = _upper(expected_dxy_direction)
    symbol = _upper(row.get("symbol"))

    approved = _approved_map(expected)
    expected_side = _upper(approved.get(symbol))
    side = _upper(row.get("side") or expected_side)

    reasons: list[str] = []

    approved_expression = bool(
        symbol in approved
        and side == expected_side
    )
    if not approved:
        reasons.append("EXPECTED_DXY_DIRECTION_NOT_ACTIONABLE")
    elif symbol not in approved:
        reasons.append("SYMBOL_NOT_APPROVED_FOR_V1_EXPRESSION_SET")
    elif side != expected_side:
        reasons.append(f"SIDE_MISMATCH_EXPECTED_{expected_side}")

    location_ok = _extract_location_qualified(row, side)
    if location_ok is not True:
        reasons.append(
            "HTF_LOCATION_UNQUALIFIED"
            if location_ok is False
            else "HTF_LOCATION_NOT_AVAILABLE"
        )

    invalidation_ok = _extract_structural_invalidation_valid(row, side)
    if invalidation_ok is not True:
        reasons.append(
            "STRUCTURAL_INVALIDATION_INVALID"
            if invalidation_ok is False
            else "STRUCTURAL_INVALIDATION_NOT_AVAILABLE"
        )

    fresh = _symbol_data_fresh(row)
    if fresh is not True:
        reasons.append(
            "SYMBOL_DATA_STALE"
            if fresh is False
            else "SYMBOL_FRESHNESS_NOT_AVAILABLE"
        )

    room = _extract_room(row, side)
    if room is None:
        reasons.append("DIRECTIONAL_ROOM_NOT_AVAILABLE")

    # Raw observations that ranking wants, but we do not normalize them into
    # qualities here because the source-of-truth marks ranking weights/vetoes
    # as calibration work.
    move_consumed = _num(row.get("move_consumed_atr"))
    spread_cost = _num(row.get("spread_cost_atr"))

    if move_consumed is None:
        reasons.append("MOVE_CONSUMED_ATR_NOT_AVAILABLE")
    if spread_cost is None:
        reasons.append("SPREAD_COST_ATR_NOT_AVAILABLE")

    # Never copy arbitrary "quality" fields from upstream into authoritative
    # ranker inputs. Those need an approved calibrated scoring layer.
    qualities = {
        "htf_location_quality": None,
        "major_structure_quality": None,
        "h1_transition_quality": None,
        "volatility_quality": None,
        "liquidity_quality": None,
        "non_usd_driver_alignment": None,
    }
    reasons.append("RANKING_QUALITY_CALIBRATION_NOT_WIRED")

    mandatory_gate_ok = bool(
        approved_expression
        and location_ok is True
        and invalidation_ok is True
        and fresh is True
        and room is not None
    )

    reason = (
        "MANDATORY_SYMBOL_GATES_OBSERVED_AWAIT_RANKING_CALIBRATION"
        if mandatory_gate_ok
        else reasons[0]
    )

    return StrategyBExpressionCandidateObservation(
        schema_version=1,
        model="STRATEGY_B_EXPRESSION_CANDIDATE_OBSERVER_V1",
        expected_dxy_direction=expected or "UNKNOWN",
        symbol=symbol,
        side=side,
        approved_expression=approved_expression,
        htf_location_qualified=location_ok,
        structural_invalidation_valid=invalidation_ok,
        symbol_data_fresh=fresh,
        d1_structure_state=_extract_structure_state(row, "D1"),
        h4_structure_state=_extract_structure_state(row, "H4"),
        h1_structure_state=_extract_structure_state(row, "H1"),
        directional_room_atr=room,
        move_consumed_atr=move_consumed,
        spread_cost_atr=spread_cost,
        htf_location_quality=qualities["htf_location_quality"],
        major_structure_quality=qualities["major_structure_quality"],
        h1_transition_quality=qualities["h1_transition_quality"],
        volatility_quality=qualities["volatility_quality"],
        liquidity_quality=qualities["liquidity_quality"],
        non_usd_driver_alignment=qualities[
            "non_usd_driver_alignment"
        ],
        reason=reason,
        reasons=tuple(dict.fromkeys(reasons)),
        raw=dict(row),
    )


def observe_strategy_b_expression_candidates(
    *,
    expected_dxy_direction: str,
    symbol_snapshots: Sequence[Mapping[str, Any]],
) -> tuple[StrategyBExpressionCandidateObservation, ...]:
    return tuple(
        observe_strategy_b_expression_candidate(
            expected_dxy_direction=expected_dxy_direction,
            symbol_snapshot=row,
        )
        for row in symbol_snapshots
        if isinstance(row, Mapping)
    )


__all__ = [
    "StrategyBExpressionCandidateObservation",
    "observe_strategy_b_expression_candidate",
    "observe_strategy_b_expression_candidates",
]
