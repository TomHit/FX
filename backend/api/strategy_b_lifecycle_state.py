"""
XauTrendLab Strategy B - Lifecycle State
=========================================

Pure immutable state record for one Strategy-B thesis.

This module owns no persistence and performs no external actions.
It only binds immutable thesis identity to the existing canonical
Strategy-B lifecycle transition primitive.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Optional

from api.strategy_b_lifecycle import (
    CANONICAL_STATES,
    StrategyBTransition,
    transition_strategy_b_lifecycle,
)


def _required_identity(name: str, value: Any) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be str")

    resolved = value.strip()

    if not resolved:
        raise ValueError(f"{name} required")

    return resolved


def _positive_ms(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError("at_ms must be positive int")

    if value <= 0:
        raise ValueError("at_ms must be positive int")

    return value


def _canonical_state(value: Any) -> str:
    resolved = str(value or "").strip().upper()

    if resolved not in CANONICAL_STATES:
        raise ValueError("UNKNOWN_LIFECYCLE_STATE")

    return resolved


@dataclass(frozen=True)
class StrategyBLifecycleState:
    thesis_id: str
    selected_capture_id: str
    current_state: str
    updated_at_ms: int
    last_transition: Optional[StrategyBTransition]

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "thesis_id",
            _required_identity("thesis_id", self.thesis_id),
        )
        object.__setattr__(
            self,
            "selected_capture_id",
            _required_identity(
                "selected_capture_id",
                self.selected_capture_id,
            ),
        )
        object.__setattr__(
            self,
            "current_state",
            _canonical_state(self.current_state),
        )
        object.__setattr__(
            self,
            "updated_at_ms",
            _positive_ms(self.updated_at_ms),
        )

        if (
            self.last_transition is not None
            and not isinstance(
                self.last_transition,
                StrategyBTransition,
            )
        ):
            raise TypeError(
                "last_transition must be StrategyBTransition or None"
            )


def create_strategy_b_lifecycle_state(
    *,
    thesis_id: str,
    selected_capture_id: str,
    current_state: str,
    at_ms: int,
) -> StrategyBLifecycleState:
    return StrategyBLifecycleState(
        thesis_id=thesis_id,
        selected_capture_id=selected_capture_id,
        current_state=current_state,
        updated_at_ms=at_ms,
        last_transition=None,
    )


def apply_strategy_b_lifecycle_transition(
    state: StrategyBLifecycleState,
    *,
    target_state: str,
    reason: str,
    at_ms: int,
    evidence_snapshot: Optional[Mapping[str, Any]] = None,
) -> StrategyBLifecycleState:
    if not isinstance(state, StrategyBLifecycleState):
        raise TypeError(
            "state must be StrategyBLifecycleState"
        )

    transition_at_ms = _positive_ms(at_ms)

    if transition_at_ms < state.updated_at_ms:
        raise ValueError("LIFECYCLE_TIME_REGRESSION")

    tx = transition_strategy_b_lifecycle(
        state.current_state,
        target_state,
        reason=reason,
        evidence_snapshot=(
            dict(evidence_snapshot)
            if isinstance(evidence_snapshot, Mapping)
            else {}
        ),
        at_ms=transition_at_ms,
    )

    if not tx.valid:
        raise ValueError(tx.validation_reason)

    return StrategyBLifecycleState(
        thesis_id=state.thesis_id,
        selected_capture_id=state.selected_capture_id,
        current_state=tx.to_state,
        updated_at_ms=transition_at_ms,
        last_transition=tx,
    )


__all__ = [
    "StrategyBLifecycleState",
    "create_strategy_b_lifecycle_state",
    "apply_strategy_b_lifecycle_transition",
]
