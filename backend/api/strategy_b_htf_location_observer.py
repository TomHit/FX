"""
XauTrendLab Strategy B - HTF Location Observer V2
==============================================

Pure observation-only HTF location evidence for Strategy-B.

Purpose
-------
Reuse already-computed canonical symbol analytics to expose D1 structural
location evidence without inventing a second D1 SR engine or ranking rule.

Inputs
------
- symbol / side / price
- existing pair_bias from api.bias_engine.compute_pair_bias()
- existing canonical H4/H1 market-structure state from trend_sr

Reuses
------
pair_bias["timeframes"]["D1"]["structure"]:
    state
    high_pattern / low_pattern
    prev_high / last_high
    prev_low / last_low
    pivot_high_count / pivot_low_count

pair_bias["timeframes"]["D1"]["features"]:
    atr
    range_position
    net_atr
    last_close

canonical H4/H1 structure:
    directional_room_atr
    structure_pressure
    structure_pressure_reason
    inside_support / inside_resistance

Does NOT
--------
- fetch Redis/broker data
- compute SR
- synthesize D1
- modify Strategy A
- place/modify/close orders
- reserve risk
- invent HTF-location qualification thresholds
- declare a structural stop valid

The output distinguishes:
    htf_location_observed
    htf_location_status
    htf_location_hard_blocked
from:
    htf_location_qualified

V2 adds an asymmetric, fail-closed location state:
    HTF_LOCATION_INPUT_INCOMPLETE
    HTF_LOCATION_BLOCKED
    HTF_LOCATION_OBSERVED_AWAIT_CALIBRATION

The BLOCKED state reuses only the existing canonical trend_sr semantics:
    structure_pressure == "EXTREME"

No new room threshold is invented here. Qualification still remains False until
explicit Strategy-B calibration / semantics are approved.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping, Optional


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


def _positive(v: Any) -> Optional[float]:
    x = _num(v)
    if x is None or x <= 0:
        return None
    return x


def _distance_atr(distance: Optional[float], atr: Optional[float]) -> Optional[float]:
    if distance is None or atr is None or atr <= 0:
        return None
    return float(distance) / float(atr)


@dataclass(frozen=True)
class StrategyBHTFLocationObservation:
    schema_version: int
    model: str

    symbol: str
    side: str
    price: Optional[float]

    data_ok: bool
    reason: str
    reasons: tuple[str, ...]

    d1_data_ok: bool
    d1_fresh: Optional[bool]
    d1_regime: str
    d1_direction: str
    d1_structure_state: str
    d1_high_pattern: Optional[str]
    d1_low_pattern: Optional[str]

    d1_prev_high: Optional[float]
    d1_last_high: Optional[float]
    d1_prev_low: Optional[float]
    d1_last_low: Optional[float]
    d1_pivot_high_count: Optional[int]
    d1_pivot_low_count: Optional[int]

    d1_atr: Optional[float]
    d1_range_position: Optional[float]
    d1_net_atr: Optional[float]
    d1_last_close: Optional[float]

    d1_anchor_kind: Optional[str]
    d1_anchor_price: Optional[float]
    d1_anchor_distance_atr: Optional[float]

    d1_opposing_pivot_kind: Optional[str]
    d1_opposing_pivot_price: Optional[float]
    d1_opposing_room_atr: Optional[float]

    h4_directional_room_atr: Optional[float]
    h4_structure_pressure: str
    h4_structure_pressure_reason: str
    inside_support: bool
    inside_resistance: bool

    candidate_macro_invalidation_kind: Optional[str]
    candidate_macro_invalidation_price: Optional[float]

    htf_location_observed: bool
    htf_location_status: str
    htf_location_hard_blocked: bool
    htf_location_block_reason: Optional[str]
    htf_location_qualified: bool
    structural_invalidation_valid: bool

    observation_only: bool = True
    execution_authority: bool = False
    strategy_a_mutation: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def observe_strategy_b_htf_location(
    *,
    symbol: str,
    side: str,
    price: Any,
    pair_bias: Mapping[str, Any] | None,
    canonical_structure: Mapping[str, Any] | None,
) -> StrategyBHTFLocationObservation:
    """
    Build one fail-closed HTF location observation.

    BUY:
      D1 anchor          = last confirmed D1 swing low
      opposing pivot     = last confirmed D1 swing high
      macro invalidation = last confirmed D1 swing low (candidate only)

    SELL:
      D1 anchor          = last confirmed D1 swing high
      opposing pivot     = last confirmed D1 swing low
      macro invalidation = last confirmed D1 swing high (candidate only)

    No threshold is applied to anchor distance / opposing room, therefore
    htf_location_qualified always remains False in this observation module.
    """
    sym = _upper(symbol)
    side_u = _upper(side)
    px = _positive(price)

    pb = _mapping(pair_bias)
    tfs = _mapping(pb.get("timeframes"))
    d1 = _mapping(tfs.get("D1"))
    structure = _mapping(d1.get("structure"))
    features = _mapping(d1.get("features"))
    canon = _mapping(canonical_structure)

    reasons: list[str] = []

    if side_u not in ("BUY", "SELL"):
        reasons.append("SIDE_NOT_ACTIONABLE")
    if px is None:
        reasons.append("PRICE_INVALID")

    d1_data_ok = d1.get("data_ok") is True
    if not d1_data_ok:
        reasons.append("D1_DATA_NOT_OK")

    d1_fresh_raw = d1.get("fresh")
    d1_fresh = (
        bool(d1_fresh_raw)
        if isinstance(d1_fresh_raw, bool)
        else None
    )
    if d1_fresh is False:
        reasons.append("D1_DATA_STALE")

    d1_atr = _positive(features.get("atr"))
    if d1_atr is None:
        reasons.append("D1_ATR_NOT_AVAILABLE")

    d1_state = _upper(structure.get("state")) or "UNKNOWN"
    high_pattern = _upper(structure.get("high_pattern")) or None
    low_pattern = _upper(structure.get("low_pattern")) or None

    prev_high = _num(structure.get("prev_high"))
    last_high = _num(structure.get("last_high"))
    prev_low = _num(structure.get("prev_low"))
    last_low = _num(structure.get("last_low"))

    phc = structure.get("pivot_high_count")
    plc = structure.get("pivot_low_count")
    try:
        phc_i = int(phc) if phc is not None else None
    except Exception:
        phc_i = None
    try:
        plc_i = int(plc) if plc is not None else None
    except Exception:
        plc_i = None

    anchor_kind: Optional[str] = None
    anchor_price: Optional[float] = None
    opposing_kind: Optional[str] = None
    opposing_price: Optional[float] = None
    candidate_invalid_kind: Optional[str] = None
    candidate_invalid_price: Optional[float] = None

    if side_u == "BUY":
        anchor_kind = "D1_LAST_SWING_LOW"
        anchor_price = last_low
        opposing_kind = "D1_LAST_SWING_HIGH"
        opposing_price = last_high
        candidate_invalid_kind = "D1_LAST_SWING_LOW"
        candidate_invalid_price = last_low
    elif side_u == "SELL":
        anchor_kind = "D1_LAST_SWING_HIGH"
        anchor_price = last_high
        opposing_kind = "D1_LAST_SWING_LOW"
        opposing_price = last_low
        candidate_invalid_kind = "D1_LAST_SWING_HIGH"
        candidate_invalid_price = last_high

    if anchor_price is None:
        reasons.append("D1_STRUCTURAL_ANCHOR_NOT_AVAILABLE")
    if opposing_price is None:
        reasons.append("D1_OPPOSING_PIVOT_NOT_AVAILABLE")

    anchor_distance_atr: Optional[float] = None
    opposing_room_atr: Optional[float] = None

    if px is not None and d1_atr is not None and anchor_price is not None:
        if side_u == "BUY":
            anchor_distance_atr = _distance_atr(
                float(px) - float(anchor_price),
                d1_atr,
            )
        elif side_u == "SELL":
            anchor_distance_atr = _distance_atr(
                float(anchor_price) - float(px),
                d1_atr,
            )

    if px is not None and d1_atr is not None and opposing_price is not None:
        if side_u == "BUY":
            opposing_room_atr = _distance_atr(
                float(opposing_price) - float(px),
                d1_atr,
            )
        elif side_u == "SELL":
            opposing_room_atr = _distance_atr(
                float(px) - float(opposing_price),
                d1_atr,
            )

    # Keep signed geometry. Negative values are informative:
    # price has already moved beyond the selected historical pivot.
    if (
        anchor_distance_atr is not None
        and anchor_distance_atr < 0
    ):
        reasons.append("PRICE_BEYOND_D1_ANCHOR")
    if (
        opposing_room_atr is not None
        and opposing_room_atr < 0
    ):
        reasons.append("PRICE_BEYOND_D1_OPPOSING_PIVOT")

    h4_room = _num(canon.get("directional_room_atr"))
    pressure = _upper(canon.get("structure_pressure")) or "UNKNOWN"
    pressure_reason = str(
        canon.get("structure_pressure_reason") or "UNKNOWN"
    )

    inside_support = isinstance(
        canon.get("inside_support"),
        Mapping,
    )
    inside_resistance = isinstance(
        canon.get("inside_resistance"),
        Mapping,
    )

    if h4_room is None:
        reasons.append("CANONICAL_DIRECTIONAL_ROOM_NOT_AVAILABLE")

    # "Observed" means the evidence set is available, not that it passes any
    # Strategy-B qualification threshold.
    htf_location_observed = bool(
        side_u in ("BUY", "SELL")
        and px is not None
        and d1_data_ok
        and d1_atr is not None
        and anchor_price is not None
        and opposing_price is not None
        and h4_room is not None
    )

    # ------------------------------------------------------------------
    # V2 asymmetric location state.
    #
    # We are allowed to identify an already-known canonical BAD condition
    # without inventing a new Strategy-B calibration threshold.  The existing
    # trend_sr structure model already owns the EXTREME classification and its
    # reason (e.g. INSIDE_RESISTANCE, INSIDE_SUPPORT, ROOM_LE_0_20_ATR).
    #
    # We still do NOT emit HTF_LOCATION_QUALIFIED.
    # ------------------------------------------------------------------
    htf_location_hard_blocked = pressure == "EXTREME"
    htf_location_block_reason = (
        pressure_reason if htf_location_hard_blocked else None
    )

    if htf_location_hard_blocked:
        reasons.append(
            "CANONICAL_STRUCTURE_EXTREME:"
            + str(pressure_reason or "UNKNOWN")
        )

    # Deliberately fail-closed. Thresholds / qualification semantics are not
    # invented here.
    htf_location_qualified = False
    structural_invalidation_valid = False

    if not htf_location_observed:
        htf_location_status = "HTF_LOCATION_INPUT_INCOMPLETE"
    elif htf_location_hard_blocked:
        htf_location_status = "HTF_LOCATION_BLOCKED"
    else:
        htf_location_status = "HTF_LOCATION_OBSERVED_AWAIT_CALIBRATION"
        reasons.append("HTF_LOCATION_CALIBRATION_REQUIRED")

    if htf_location_observed:
        reasons.append(
            "MACRO_INVALIDATION_CANDIDATE_OBSERVED_NOT_APPROVED"
        )

    # data_ok means the required location evidence was actually observed.
    # A location can therefore be data_ok=True and still be hard-blocked.
    data_ok = htf_location_observed

    reason = htf_location_status

    return StrategyBHTFLocationObservation(
        schema_version=1,
        model="STRATEGY_B_HTF_LOCATION_OBSERVER_V2",
        symbol=sym,
        side=side_u,
        price=px,
        data_ok=data_ok,
        reason=reason,
        reasons=tuple(dict.fromkeys(reasons)),
        d1_data_ok=d1_data_ok,
        d1_fresh=d1_fresh,
        d1_regime=_upper(d1.get("regime")) or "UNKNOWN",
        d1_direction=_upper(d1.get("direction")) or "UNKNOWN",
        d1_structure_state=d1_state,
        d1_high_pattern=high_pattern,
        d1_low_pattern=low_pattern,
        d1_prev_high=prev_high,
        d1_last_high=last_high,
        d1_prev_low=prev_low,
        d1_last_low=last_low,
        d1_pivot_high_count=phc_i,
        d1_pivot_low_count=plc_i,
        d1_atr=d1_atr,
        d1_range_position=_num(features.get("range_position")),
        d1_net_atr=_num(features.get("net_atr")),
        d1_last_close=_num(features.get("last_close")),
        d1_anchor_kind=anchor_kind,
        d1_anchor_price=anchor_price,
        d1_anchor_distance_atr=anchor_distance_atr,
        d1_opposing_pivot_kind=opposing_kind,
        d1_opposing_pivot_price=opposing_price,
        d1_opposing_room_atr=opposing_room_atr,
        h4_directional_room_atr=h4_room,
        h4_structure_pressure=pressure,
        h4_structure_pressure_reason=pressure_reason,
        inside_support=inside_support,
        inside_resistance=inside_resistance,
        candidate_macro_invalidation_kind=candidate_invalid_kind,
        candidate_macro_invalidation_price=candidate_invalid_price,
        htf_location_observed=htf_location_observed,
        htf_location_status=htf_location_status,
        htf_location_hard_blocked=htf_location_hard_blocked,
        htf_location_block_reason=htf_location_block_reason,
        htf_location_qualified=htf_location_qualified,
        structural_invalidation_valid=structural_invalidation_valid,
    )


__all__ = [
    "StrategyBHTFLocationObservation",
    "observe_strategy_b_htf_location",
]
