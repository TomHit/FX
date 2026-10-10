from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping, Optional


DIRECTIONS = ("BULLISH", "BEARISH")


def _mapping(v: Any) -> Mapping[str, Any]:
    return v if isinstance(v, Mapping) else {}


def _upper(v: Any) -> str:
    return str(v or "").strip().upper()


def _opposite(direction: str) -> str:
    if direction == "BULLISH":
        return "BEARISH"
    if direction == "BEARISH":
        return "BULLISH"
    return "UNKNOWN"


@dataclass(frozen=True)
class StrategyBPostEntryValidity:
    schema_version: int
    model: str

    ready: bool
    reason: str

    expected_dxy_direction: str

    macro_thesis_valid: Optional[bool]
    dxy_structure_valid: Optional[bool]

    h1_short_state: str
    h1_last_turn_direction: str
    h1_swing_state: str

    observation_only: bool = True
    execution_authority: bool = False
    broker_modify_authority: bool = False
    broker_close_authority: bool = False
    strategy_a_mutation: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def evaluate_strategy_b_post_entry_validity(
    *,
    expected_dxy_direction: str,
    dxy_h1_turn: Mapping[str, Any],
    dxy_bias: Mapping[str, Any],
) -> StrategyBPostEntryValidity:
    """
    Pure post-entry validity policy.

    This deliberately does NOT use:
      - macro_qualified aggregate
      - M15-only opposition
      - SR pressure / EXTREME
      - directional room
      - reaction-capacity WAIT state
      - any new confidence threshold

    VALID:
      H1 short state + last turn + H1 swing all align with the
      frozen expected DXY direction.

    INVALID:
      those same three independent H1 structural facts all align
      with the opposite direction.

    Mixed, neutral, or missing evidence is UNKNOWN / NOT_READY.
    """
    expected = _upper(expected_dxy_direction)
    turn = _mapping(dxy_h1_turn)
    bias = _mapping(dxy_bias)

    short_state = _upper(turn.get("short_state")) or "UNKNOWN"
    last_turn = _upper(turn.get("last_turn_direction")) or "UNKNOWN"

    h1 = _mapping(bias.get("H1"))
    swing = _upper(
        h1.get("state")
        or h1.get("direction")
        or h1.get("swing_state")
    ) or "UNKNOWN"

    if expected not in DIRECTIONS:
        return StrategyBPostEntryValidity(
            schema_version=1,
            model="STRATEGY_B_POST_ENTRY_VALIDITY_V1",
            ready=False,
            reason="EXPECTED_DXY_DIRECTION_UNKNOWN",
            expected_dxy_direction=expected,
            macro_thesis_valid=None,
            dxy_structure_valid=None,
            h1_short_state=short_state,
            h1_last_turn_direction=last_turn,
            h1_swing_state=swing,
        )

    if (
        short_state not in DIRECTIONS
        or last_turn not in DIRECTIONS
        or swing not in DIRECTIONS
    ):
        return StrategyBPostEntryValidity(
            schema_version=1,
            model="STRATEGY_B_POST_ENTRY_VALIDITY_V1",
            ready=False,
            reason="DXY_H1_STRUCTURE_INCOMPLETE",
            expected_dxy_direction=expected,
            macro_thesis_valid=None,
            dxy_structure_valid=None,
            h1_short_state=short_state,
            h1_last_turn_direction=last_turn,
            h1_swing_state=swing,
        )

    if (
        short_state == expected
        and last_turn == expected
        and swing == expected
    ):
        return StrategyBPostEntryValidity(
            schema_version=1,
            model="STRATEGY_B_POST_ENTRY_VALIDITY_V1",
            ready=True,
            reason="DXY_H1_THESIS_CONFIRMED",
            expected_dxy_direction=expected,
            macro_thesis_valid=True,
            dxy_structure_valid=True,
            h1_short_state=short_state,
            h1_last_turn_direction=last_turn,
            h1_swing_state=swing,
        )

    opposite = _opposite(expected)

    if (
        short_state == opposite
        and last_turn == opposite
        and swing == opposite
    ):
        return StrategyBPostEntryValidity(
            schema_version=1,
            model="STRATEGY_B_POST_ENTRY_VALIDITY_V1",
            ready=True,
            reason="DXY_H1_CONFIRMED_OPPOSITE_STRUCTURE",
            expected_dxy_direction=expected,
            macro_thesis_valid=False,
            dxy_structure_valid=False,
            h1_short_state=short_state,
            h1_last_turn_direction=last_turn,
            h1_swing_state=swing,
        )

    return StrategyBPostEntryValidity(
        schema_version=1,
        model="STRATEGY_B_POST_ENTRY_VALIDITY_V1",
        ready=False,
        reason="DXY_H1_STRUCTURE_MIXED",
        expected_dxy_direction=expected,
        macro_thesis_valid=None,
        dxy_structure_valid=None,
        h1_short_state=short_state,
        h1_last_turn_direction=last_turn,
        h1_swing_state=swing,
    )


__all__ = [
    "StrategyBPostEntryValidity",
    "evaluate_strategy_b_post_entry_validity",
]
