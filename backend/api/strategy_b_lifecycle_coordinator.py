"""
Narrow Strategy-B lifecycle persistence coordinator.

Boundary:

    load lifecycle state
        +
    load lifecycle evidence
        |
        v
    frozen pure evidence consumer
        |
        v
    persist proposed lifecycle state

This module does not:
- create or infer broker evidence;
- discover Redis keys;
- use Redis SCAN/KEYS;
- submit/modify/cancel orders;
- open/close positions;
- reserve risk;
- manage THESIS_* states;
- write calibration outcomes;
- mutate Strategy-A;
- provide execution authority.

The Redis-compatible client and exact persistence functions are supplied by
the caller. At most one lifecycle boundary may be advanced per invocation.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from api.strategy_b_lifecycle_evidence_consumer import (
    consume_strategy_b_lifecycle_evidence,
)


@dataclass(frozen=True)
class StrategyBLifecycleCoordination:
    advanced: bool
    persisted: bool
    reason: str
    from_state: str
    to_state: str
    authorization_reason: str
    persistence_reason: str
    observation_only: bool = True
    execution_authority: bool = False
    strategy_a_mutation: bool = False


def _result(
    *,
    advanced: bool,
    persisted: bool,
    reason: str,
    from_state: str,
    to_state: str,
    authorization_reason: str = "NO_AUTHORIZATION",
    persistence_reason: str = "NOT_ATTEMPTED",
) -> StrategyBLifecycleCoordination:
    return StrategyBLifecycleCoordination(
        advanced=bool(advanced),
        persisted=bool(persisted),
        reason=str(reason),
        from_state=str(from_state),
        to_state=str(to_state),
        authorization_reason=str(authorization_reason),
        persistence_reason=str(persistence_reason),
        observation_only=True,
        execution_authority=False,
        strategy_a_mutation=False,
    )


def coordinate_strategy_b_lifecycle(
    *,
    redis_client: Any,
    thesis_id: str,
    selected_symbol: str,
    selected_side: str,
    load_state: Callable[..., Any],
    load_evidence: Callable[..., Any],
    persist_state: Callable[..., Any],
) -> StrategyBLifecycleCoordination:
    """
    Coordinate at most one externally-evidenced lifecycle boundary.

    The supplied load/persist functions remain the persistence owners.
    This coordinator performs no evidence creation and no broker action.
    """

    thesis_id = str(thesis_id or "").strip()

    if not thesis_id:
        return _result(
            advanced=False,
            persisted=False,
            reason="THESIS_ID_REQUIRED",
            from_state="UNKNOWN",
            to_state="UNKNOWN",
        )

    try:
        state = load_state(
            redis_client,
            thesis_id,
        )
    except Exception:
        return _result(
            advanced=False,
            persisted=False,
            reason="LIFECYCLE_STATE_LOAD_ERROR",
            from_state="UNKNOWN",
            to_state="UNKNOWN",
        )

    if state is None:
        return _result(
            advanced=False,
            persisted=False,
            reason="LIFECYCLE_STATE_NOT_FOUND",
            from_state="UNKNOWN",
            to_state="UNKNOWN",
        )

    current_state = str(
        getattr(state, "current_state", "UNKNOWN")
    )

    # State identity must agree with the exact thesis requested.
    if str(getattr(state, "thesis_id", "")) != thesis_id:
        return _result(
            advanced=False,
            persisted=False,
            reason="LIFECYCLE_STATE_THESIS_ID_MISMATCH",
            from_state=current_state,
            to_state=current_state,
        )

    try:
        evidence = load_evidence(
            redis_client,
            thesis_id,
        )
    except Exception:
        return _result(
            advanced=False,
            persisted=False,
            reason="LIFECYCLE_EVIDENCE_LOAD_ERROR",
            from_state=current_state,
            to_state=current_state,
        )

    if evidence is None:
        return _result(
            advanced=False,
            persisted=False,
            reason="LIFECYCLE_EVIDENCE_NOT_FOUND",
            from_state=current_state,
            to_state=current_state,
        )

    consumption = consume_strategy_b_lifecycle_evidence(
        state=state,
        evidence_envelope=evidence,
        selected_symbol=selected_symbol,
        selected_side=selected_side,
    )

    if not consumption.advanced:
        return _result(
            advanced=False,
            persisted=False,
            reason=consumption.reason,
            from_state=consumption.from_state,
            to_state=consumption.to_state,
            authorization_reason=(
                consumption.authorization_reason
            ),
        )

    proposed = consumption.proposed_state

    if proposed is None:
        # Defensive fail-closed guard. A consumer may never claim an
        # advancement without supplying the canonical proposed state.
        return _result(
            advanced=False,
            persisted=False,
            reason="PROPOSED_LIFECYCLE_STATE_REQUIRED",
            from_state=consumption.from_state,
            to_state=consumption.from_state,
            authorization_reason=(
                consumption.authorization_reason
            ),
        )

    try:
        persistence = persist_state(
            redis_client,
            proposed,
        )
    except Exception:
        return _result(
            advanced=True,
            persisted=False,
            reason="LIFECYCLE_STATE_PERSIST_ERROR",
            from_state=consumption.from_state,
            to_state=consumption.to_state,
            authorization_reason=(
                consumption.authorization_reason
            ),
            persistence_reason=(
                "PERSISTENCE_EXCEPTION"
            ),
        )

    persistence_ok = bool(
        getattr(persistence, "ok", False)
    )
    persistence_reason = str(
        getattr(
            persistence,
            "reason",
            "PERSISTENCE_RESULT_INVALID",
        )
    )

    if not persistence_ok:
        return _result(
            advanced=True,
            persisted=False,
            reason="LIFECYCLE_STATE_PERSIST_FAILED",
            from_state=consumption.from_state,
            to_state=consumption.to_state,
            authorization_reason=(
                consumption.authorization_reason
            ),
            persistence_reason=persistence_reason,
        )

    return _result(
        advanced=True,
        persisted=True,
        reason="LIFECYCLE_STATE_PERSISTED",
        from_state=consumption.from_state,
        to_state=consumption.to_state,
        authorization_reason=(
            consumption.authorization_reason
        ),
        persistence_reason=persistence_reason,
    )


__all__ = [
    "StrategyBLifecycleCoordination",
    "coordinate_strategy_b_lifecycle",
]
