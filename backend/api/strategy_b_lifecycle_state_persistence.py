"""
XauTrendLab Strategy B - Lifecycle State Replacement Contract
==============================================================

Pure validation contract for replacing one immutable Strategy-B lifecycle
state with another.

This module owns no storage and performs no external actions. A replacement
is accepted only when it is identical, an initial state, or the proposed
object itself proves the canonical one-step transition from the stored state.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from api.strategy_b_lifecycle import (
    transition_strategy_b_lifecycle,
    validate_strategy_b_transition,
)
from api.strategy_b_lifecycle_state import (
    StrategyBLifecycleState,
)


@dataclass(frozen=True)
class StrategyBLifecycleStateReplacement:
    allowed: bool
    reason: str
    identical: bool


def _result(
    allowed: bool,
    reason: str,
    *,
    identical: bool = False,
) -> StrategyBLifecycleStateReplacement:
    return StrategyBLifecycleStateReplacement(
        allowed=bool(allowed),
        reason=str(reason),
        identical=bool(identical),
    )


def _canonical_edge(
    from_state: str,
    to_state: str,
) -> tuple[bool, str]:
    valid, reason = validate_strategy_b_transition(
        from_state,
        to_state,
    )

    return bool(valid), str(reason)


def validate_strategy_b_lifecycle_state_replacement(
    stored: Optional[StrategyBLifecycleState],
    proposed: StrategyBLifecycleState,
) -> StrategyBLifecycleStateReplacement:
    if stored is not None and not isinstance(
        stored,
        StrategyBLifecycleState,
    ):
        raise TypeError(
            "stored must be StrategyBLifecycleState or None"
        )

    if not isinstance(
        proposed,
        StrategyBLifecycleState,
    ):
        raise TypeError(
            "proposed must be StrategyBLifecycleState"
        )

    if stored is None:
        return _result(
            True,
            "INITIAL_STATE_ALLOWED",
        )

    if stored == proposed:
        return _result(
            True,
            "IDENTICAL_STATE",
            identical=True,
        )

    if stored.thesis_id != proposed.thesis_id:
        return _result(
            False,
            "THESIS_ID_MISMATCH",
        )

    if (
        stored.selected_capture_id
        != proposed.selected_capture_id
    ):
        return _result(
            False,
            "SELECTED_CAPTURE_ID_MISMATCH",
        )

    tx = proposed.last_transition

    # ---------------------------------------------------------
    # No transition proof.
    #
    # First determine whether the proposed state is a canonical
    # predecessor of the stored state. That is a stale lifecycle
    # regression, even though its timestamp will normally also be older.
    # ---------------------------------------------------------
    if tx is None:
        reverse_valid, reverse_reason = _canonical_edge(
            proposed.current_state,
            stored.current_state,
        )

        if (
            reverse_valid
            and reverse_reason == "ALLOWED_TRANSITION"
        ):
            return _result(
                False,
                "LIFECYCLE_STATE_REGRESSION",
            )

        if proposed.updated_at_ms < stored.updated_at_ms:
            return _result(
                False,
                "LIFECYCLE_TIME_REGRESSION",
            )

        return _result(
            False,
            "CANONICAL_TRANSITION_PROOF_REQUIRED",
        )

    # ---------------------------------------------------------
    # A changed state carrying transition proof must prove that
    # it starts from the exact currently stored lifecycle state.
    #
    # Check this before generic clock regression so malformed or
    # unrelated proof is classified structurally.
    # ---------------------------------------------------------
    if tx.from_state != stored.current_state:
        reverse_valid, reverse_reason = _canonical_edge(
            proposed.current_state,
            stored.current_state,
        )

        if (
            reverse_valid
            and reverse_reason == "ALLOWED_TRANSITION"
        ):
            return _result(
                False,
                "LIFECYCLE_STATE_REGRESSION",
            )

        return _result(
            False,
            "TRANSITION_FROM_STATE_MISMATCH",
        )

    if tx.to_state != proposed.current_state:
        return _result(
            False,
            "TRANSITION_TO_STATE_MISMATCH",
        )

    if tx.at_ms != proposed.updated_at_ms:
        return _result(
            False,
            "TRANSITION_TIME_MISMATCH",
        )

    if proposed.updated_at_ms < stored.updated_at_ms:
        return _result(
            False,
            "LIFECYCLE_TIME_REGRESSION",
        )

    canonical_valid, canonical_reason = _canonical_edge(
        stored.current_state,
        proposed.current_state,
    )

    if (
        not canonical_valid
        or canonical_reason != "ALLOWED_TRANSITION"
    ):
        reverse_valid, reverse_reason = _canonical_edge(
            proposed.current_state,
            stored.current_state,
        )

        if (
            reverse_valid
            and reverse_reason == "ALLOWED_TRANSITION"
        ):
            return _result(
                False,
                "LIFECYCLE_STATE_REGRESSION",
            )

        return _result(
            False,
            "CANONICAL_TRANSITION_PROOF_REQUIRED",
        )

    # Proposed state must carry the exact transition produced by
    # the frozen canonical lifecycle primitive.  Do not trust
    # caller-supplied valid/material/validation_reason flags.
    try:
        canonical_tx = transition_strategy_b_lifecycle(
            tx.from_state,
            tx.to_state,
            reason=tx.reason,
            evidence_snapshot=dict(tx.evidence_snapshot),
            at_ms=tx.at_ms,
        )
    except Exception:
        return _result(
            False,
            "CANONICAL_TRANSITION_PROOF_REQUIRED",
        )

    if canonical_tx != tx:
        return _result(
            False,
            "CANONICAL_TRANSITION_PROOF_REQUIRED",
        )

    return _result(
        True,
        "CANONICAL_NEXT_STATE",
    )


__all__ = [
    "StrategyBLifecycleStateReplacement",
    "validate_strategy_b_lifecycle_state_replacement",
]
