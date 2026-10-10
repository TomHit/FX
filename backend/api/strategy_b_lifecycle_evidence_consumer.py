"""
Pure Strategy-B lifecycle evidence consumer.

This module bridges already-reconstructed immutable lifecycle evidence to a
proposed canonical lifecycle state transition.

It does NOT:
- read or write Redis;
- call a broker;
- submit, modify, or cancel orders;
- open or close positions;
- reserve risk;
- write calibration outcomes;
- manage post-entry thesis state;
- mutate Strategy-A;
- provide execution or broker authority.

Persistence remains owned by the frozen lifecycle-state Redis adapter.
External facts remain owned by the frozen lifecycle-evidence store.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from api.strategy_b_lifecycle_evidence_authorizer import (
    authorize_closed,
    authorize_order_pending,
    authorize_trade_active,
)
from api.strategy_b_lifecycle_evidence_envelope import (
    StrategyBLifecycleEvidenceEnvelope,
)
from api.strategy_b_lifecycle_state import (
    StrategyBLifecycleState,
    apply_strategy_b_lifecycle_transition,
)


@dataclass(frozen=True)
class StrategyBLifecycleEvidenceConsumption:
    advanced: bool
    reason: str
    authorization_reason: str
    from_state: str
    to_state: str
    proposed_state: Optional[StrategyBLifecycleState]
    observation_only: bool = True
    execution_authority: bool = False
    strategy_a_mutation: bool = False


def _result(
    *,
    advanced: bool,
    reason: str,
    authorization_reason: str,
    from_state: str,
    to_state: str,
    proposed_state: Optional[StrategyBLifecycleState] = None,
) -> StrategyBLifecycleEvidenceConsumption:
    return StrategyBLifecycleEvidenceConsumption(
        advanced=bool(advanced),
        reason=str(reason),
        authorization_reason=str(authorization_reason),
        from_state=str(from_state),
        to_state=str(to_state),
        proposed_state=proposed_state,
        observation_only=True,
        execution_authority=False,
        strategy_a_mutation=False,
    )


def _no_advance(
    state: StrategyBLifecycleState,
    *,
    reason: str,
    authorization_reason: str = "NO_AUTHORIZATION",
) -> StrategyBLifecycleEvidenceConsumption:
    return _result(
        advanced=False,
        reason=reason,
        authorization_reason=authorization_reason,
        from_state=state.current_state,
        to_state=state.current_state,
        proposed_state=None,
    )


def _advance(
    state: StrategyBLifecycleState,
    *,
    target_state: str,
    reason: str,
    at_ms: int,
    evidence_snapshot: dict,
) -> StrategyBLifecycleEvidenceConsumption:
    if at_ms < state.updated_at_ms:
        return _no_advance(
            state,
            reason="EVIDENCE_TIME_REGRESSION",
            authorization_reason=reason,
        )

    proposed = apply_strategy_b_lifecycle_transition(
        state,
        target_state=target_state,
        reason=reason,
        at_ms=at_ms,
        evidence_snapshot=evidence_snapshot,
    )

    return _result(
        advanced=True,
        reason="LIFECYCLE_ADVANCE_AUTHORIZED",
        authorization_reason=reason,
        from_state=state.current_state,
        to_state=target_state,
        proposed_state=proposed,
    )


def consume_strategy_b_lifecycle_evidence(
    *,
    state: StrategyBLifecycleState,
    evidence_envelope: StrategyBLifecycleEvidenceEnvelope,
    selected_symbol: str,
    selected_side: str,
) -> StrategyBLifecycleEvidenceConsumption:
    """
    Consume one immutable evidence envelope against one lifecycle state.

    At most one lifecycle boundary is proposed per call.

    The returned state is only a proposal. This function never persists it.
    """

    if not isinstance(state, StrategyBLifecycleState):
        raise TypeError("StrategyBLifecycleState required")

    if not isinstance(
        evidence_envelope,
        StrategyBLifecycleEvidenceEnvelope,
    ):
        raise TypeError(
            "StrategyBLifecycleEvidenceEnvelope required"
        )

    if state.thesis_id != evidence_envelope.thesis_id:
        return _no_advance(
            state,
            reason="THESIS_ID_MISMATCH",
        )

    if (
        state.selected_capture_id
        != evidence_envelope.selected_capture_id
    ):
        return _no_advance(
            state,
            reason="SELECTED_CAPTURE_ID_MISMATCH",
        )

    current = state.current_state

    # ---------------------------------------------------------
    # ENTRY_READY -> ORDER_PENDING
    # ---------------------------------------------------------
    if current == "ENTRY_READY":
        accepted = evidence_envelope.accepted_order

        if accepted is None:
            return _no_advance(
                state,
                reason="ORDER_ACCEPTED_EVIDENCE_NOT_AVAILABLE",
            )

        authorization = authorize_order_pending(
            current_state=current,
            thesis_id=state.thesis_id,
            selected_capture_id=state.selected_capture_id,
            selected_symbol=selected_symbol,
            selected_side=selected_side,
            evidence=accepted,
        )

        if not authorization.authorized:
            return _no_advance(
                state,
                reason="LIFECYCLE_ADVANCE_NOT_AUTHORIZED",
                authorization_reason=authorization.reason,
            )

        return _advance(
            state,
            target_state=authorization.to_state,
            reason=authorization.reason,
            at_ms=accepted.accepted_at_ms,
            evidence_snapshot={
                "order_intent_id": accepted.order_intent_id,
                "broker_order_id": accepted.broker_order_id,
                "symbol": accepted.symbol,
                "side": accepted.side,
            },
        )

    # ---------------------------------------------------------
    # ORDER_PENDING -> TRADE_ACTIVE
    # ---------------------------------------------------------
    if current == "ORDER_PENDING":
        accepted = evidence_envelope.accepted_order
        opened = evidence_envelope.opened_position

        if accepted is None:
            return _no_advance(
                state,
                reason="ORDER_ACCEPTED_EVIDENCE_NOT_AVAILABLE",
            )

        if opened is None:
            return _no_advance(
                state,
                reason="POSITION_OPENED_EVIDENCE_NOT_AVAILABLE",
            )

        authorization = authorize_trade_active(
            current_state=current,
            accepted_evidence=accepted,
            opened_evidence=opened,
        )

        if not authorization.authorized:
            return _no_advance(
                state,
                reason="LIFECYCLE_ADVANCE_NOT_AUTHORIZED",
                authorization_reason=authorization.reason,
            )

        return _advance(
            state,
            target_state=authorization.to_state,
            reason=authorization.reason,
            at_ms=opened.opened_at_ms,
            evidence_snapshot={
                "broker_order_id": opened.broker_order_id,
                "broker_position_id": opened.broker_position_id,
                "symbol": opened.symbol,
                "side": opened.side,
                "entry_price": opened.entry_price,
            },
        )

    # ---------------------------------------------------------
    # Position-owning state -> CLOSED
    #
    # EXIT_PENDING covers Strategy-B requested exits.
    # Other post-open states cover externally observed broker
    # terminal events such as broker SL or manual close.
    # ---------------------------------------------------------
    close_source_states = {
        "TRADE_ACTIVE",
        "THESIS_STRONG",
        "THESIS_STABLE",
        "THESIS_WEAKENING",
        "THESIS_INVALID",
        "EXIT_PENDING",
    }

    if current in close_source_states and evidence_envelope.closed_position is not None:
        opened = evidence_envelope.opened_position
        closed = evidence_envelope.closed_position

        if opened is None:
            return _no_advance(
                state,
                reason="POSITION_OPENED_EVIDENCE_NOT_AVAILABLE",
            )

        if closed is None:
            return _no_advance(
                state,
                reason="POSITION_CLOSED_EVIDENCE_NOT_AVAILABLE",
            )

        authorization = authorize_closed(
            current_state=current,
            opened_evidence=opened,
            closed_evidence=closed,
        )

        if not authorization.authorized:
            return _no_advance(
                state,
                reason="LIFECYCLE_ADVANCE_NOT_AUTHORIZED",
                authorization_reason=authorization.reason,
            )

        return _advance(
            state,
            target_state=authorization.to_state,
            reason=authorization.reason,
            at_ms=closed.closed_at_ms,
            evidence_snapshot={
                "broker_position_id": closed.broker_position_id,
                "symbol": closed.symbol,
                "side": closed.side,
                "entry_price": closed.entry_price,
                "exit_price": closed.exit_price,
                "close_reason": closed.close_reason,
                "net_profit": closed.net_profit,
                "net_r": closed.net_r,
                "mfe_r": closed.mfe_r,
                "mae_r": closed.mae_r,
            },
        )

    # Post-entry thesis states deliberately belong to a different owner.
    return _no_advance(
        state,
        reason="STATE_NOT_OWNED_BY_EVIDENCE_CONSUMER",
    )


__all__ = [
    "StrategyBLifecycleEvidenceConsumption",
    "consume_strategy_b_lifecycle_evidence",
]
