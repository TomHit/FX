from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping, Optional

APPROVED_STRONG_PATTERNS = (
    "RETEST_FAILURE",
    "DOUBLE_TOP_BOTTOM",
    "H1_CHOCH_BOS_DISPLACEMENT_RETEST",
    "H1_ORDER_BLOCK_RETEST",
    "LIQUIDITY_SWEEP_REJECTION",
    "FAILED_BREAKOUT_REACCEPTANCE",
)
APPROVED_SUPPORTING_PATTERNS = (
    "LOWER_HIGH_HIGHER_LOW",
    "MA_STRUCTURE_RECLAIM",
)


def _txt(v: Any) -> str:
    return str(v or "").strip()


def _upper(v: Any) -> str:
    return _txt(v).upper()


def _bool(v: Any) -> bool:
    return bool(v is True)


def _num(v: Any) -> Optional[float]:
    try:
        x = float(v)
        return x if x == x else None
    except Exception:
        return None


def _mapping(v: Any) -> Mapping[str, Any]:
    return v if isinstance(v, Mapping) else {}


def _field(obj: Any, name: str, default: Any = None) -> Any:
    if isinstance(obj, Mapping):
        return obj.get(name, default)
    return getattr(obj, name, default)


@dataclass(frozen=True)
class StrategyBEntryPatternConfig:
    max_entry_distance_to_invalidation_atr: Optional[float] = None
    max_move_consumed_atr: Optional[float] = None
    require_macro_qualified: bool = True
    require_h1_reversal_confirmed: bool = True
    require_m15_timing_confirmation: bool = True
    supporting_pattern_can_standalone: bool = False


@dataclass(frozen=True)
class StrategyBEntryPatternDecision:
    schema_version: int
    model: str
    direction: str
    macro_qualified: bool
    h1_reversal_confirmed: bool
    m15_timing_confirmed: bool
    strong_patterns: tuple[str, ...]
    supporting_patterns: tuple[str, ...]
    selected_pattern: Optional[str]
    entry_ready_shadow: bool
    decision: str
    reason: str
    reasons: tuple[str, ...]
    no_chase_veto: bool
    no_chase_reason: Optional[str]
    distance_to_invalidation_atr: Optional[float]
    move_consumed_atr: Optional[float]
    structural_room_atr: Optional[float]
    observations: Mapping[str, Any]
    observation_only: bool = True
    execution_authority: bool = False
    strategy_a_mutation: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _macro_flags(macro_qualification: Any) -> tuple[bool, bool, str]:
    macro_ok = bool(_field(macro_qualification, "macro_qualified", False))
    h1_ok = bool(_field(macro_qualification, "h1_reversal_confirmed", False))
    direction = _upper(_field(macro_qualification, "expected_dxy_direction", "UNKNOWN")) or "UNKNOWN"
    return macro_ok, h1_ok, direction


def _m15_timing_confirmed(m15_state: Mapping[str, Any], expected_direction: str) -> tuple[bool, str]:
    m15 = _mapping(m15_state)
    status = _upper(m15.get("status") or m15.get("turn_status") or _mapping(m15.get("turn")).get("status"))
    direction = _upper(m15.get("direction") or m15.get("turn_direction") or _mapping(m15.get("turn")).get("direction"))
    if expected_direction not in ("BULLISH", "BEARISH"):
        return False, "EXPECTED_DIRECTION_UNKNOWN"
    if status == "CONFIRMED" and direction == expected_direction:
        return True, "M15_CONFIRMED_TURN_ALIGNED"
    if status == "CONFIRMED" and direction in ("BULLISH", "BEARISH"):
        return False, "M15_CONFIRMED_TURN_OPPOSITE"
    return False, "M15_TIMING_NOT_FORMALLY_CONFIRMED"


def _pattern_flags(obs: Mapping[str, Any]) -> tuple[tuple[str, ...], tuple[str, ...]]:
    o = _mapping(obs)
    strong: list[str] = []
    support: list[str] = []
    if _bool(o.get("retest_failure")):
        strong.append("RETEST_FAILURE")
    if _bool(o.get("double_top_bottom")):
        strong.append("DOUBLE_TOP_BOTTOM")
    if _bool(o.get("h1_choch_bos")) and _bool(o.get("h1_displacement")) and _bool(o.get("h1_retest")):
        strong.append("H1_CHOCH_BOS_DISPLACEMENT_RETEST")
    if _bool(o.get("h1_order_block_validated")) and _bool(o.get("h1_order_block_retest")) and _bool(o.get("h1_displacement")) and _bool(o.get("h1_choch_bos")):
        strong.append("H1_ORDER_BLOCK_RETEST")
    if _bool(o.get("liquidity_sweep_rejection")):
        strong.append("LIQUIDITY_SWEEP_REJECTION")
    if _bool(o.get("failed_breakout_reacceptance")):
        strong.append("FAILED_BREAKOUT_REACCEPTANCE")
    if _bool(o.get("lower_high_higher_low")):
        support.append("LOWER_HIGH_HIGHER_LOW")
    if _bool(o.get("ma_structure_reclaim")):
        support.append("MA_STRUCTURE_RECLAIM")
    return tuple(strong), tuple(support)


def _select_pattern(strong: tuple[str, ...], support: tuple[str, ...]) -> Optional[str]:
    for name in APPROVED_STRONG_PATTERNS:
        if name in strong:
            return name
    return support[0] if support else None


def _no_chase(obs: Mapping[str, Any], cfg: StrategyBEntryPatternConfig):
    o = _mapping(obs)
    distance = _num(o.get("distance_to_invalidation_atr"))
    consumed = _num(o.get("move_consumed_atr"))
    room = _num(o.get("structural_room_atr"))
    if _bool(o.get("too_far_from_invalidation")):
        return True, "TOO_FAR_FROM_STRUCTURAL_INVALIDATION", distance, consumed, room
    if _bool(o.get("move_already_consumed")):
        return True, "MOVE_ALREADY_CONSUMED_NO_CHASE", distance, consumed, room
    if cfg.max_entry_distance_to_invalidation_atr is not None and distance is not None and distance > float(cfg.max_entry_distance_to_invalidation_atr):
        return True, "DISTANCE_TO_INVALIDATION_EXCEEDS_LIMIT", distance, consumed, room
    if cfg.max_move_consumed_atr is not None and consumed is not None and consumed > float(cfg.max_move_consumed_atr):
        return True, "MOVE_CONSUMED_EXCEEDS_LIMIT", distance, consumed, room
    if _bool(o.get("price_ran_without_retest")):
        return True, "PRICE_RAN_WITHOUT_RETEST_LET_IT_GO", distance, consumed, room
    return False, None, distance, consumed, room


def evaluate_strategy_b_entry_patterns(
    macro_qualification: Any,
    *,
    pattern_observations: Mapping[str, Any],
    m15_state: Mapping[str, Any],
    config: Optional[StrategyBEntryPatternConfig] = None,
) -> StrategyBEntryPatternDecision:
    cfg = config or StrategyBEntryPatternConfig()
    reasons: list[str] = []
    macro_ok, h1_ok, direction = _macro_flags(macro_qualification)
    strong, supporting = _pattern_flags(pattern_observations)
    selected = _select_pattern(strong, supporting)
    m15_ok, m15_reason = _m15_timing_confirmed(m15_state, direction)
    reasons.append(m15_reason)
    veto, veto_reason, distance, consumed, room = _no_chase(pattern_observations, cfg)
    if veto_reason:
        reasons.append(veto_reason)

    if cfg.require_macro_qualified and not macro_ok:
        decision, reason = "WAIT", "MACRO_NOT_QUALIFIED"
    elif cfg.require_h1_reversal_confirmed and not h1_ok:
        decision, reason = "WAIT", "H1_REVERSAL_NOT_CONFIRMED"
    elif not strong and not (cfg.supporting_pattern_can_standalone and supporting):
        decision, reason = "WAIT", "NO_APPROVED_STRONG_ENTRY_PATTERN"
    elif veto:
        decision, reason = "WAIT", veto_reason or "NO_CHASE_VETO"
    elif cfg.require_m15_timing_confirmation and not m15_ok:
        decision, reason = "WAIT", "M15_TIMING_NOT_CONFIRMED"
    else:
        decision, reason = "ENTRY_READY_SHADOW", "APPROVED_PATTERN_AND_TIMING_CONFIRMED"

    if selected:
        reasons.append(f"SELECTED_PATTERN:{selected}")

    return StrategyBEntryPatternDecision(
        schema_version=1,
        model="STRATEGY_B_ENTRY_PATTERN_ENGINE_V1",
        direction=direction,
        macro_qualified=macro_ok,
        h1_reversal_confirmed=h1_ok,
        m15_timing_confirmed=m15_ok,
        strong_patterns=strong,
        supporting_patterns=supporting,
        selected_pattern=selected,
        entry_ready_shadow=(decision == "ENTRY_READY_SHADOW"),
        decision=decision,
        reason=reason,
        reasons=tuple(dict.fromkeys(reasons)),
        no_chase_veto=veto,
        no_chase_reason=veto_reason,
        distance_to_invalidation_atr=distance,
        move_consumed_atr=consumed,
        structural_room_atr=room,
        observations=dict(_mapping(pattern_observations)),
    )


__all__ = [
    "APPROVED_STRONG_PATTERNS",
    "APPROVED_SUPPORTING_PATTERNS",
    "StrategyBEntryPatternConfig",
    "StrategyBEntryPatternDecision",
    "evaluate_strategy_b_entry_patterns",
]
