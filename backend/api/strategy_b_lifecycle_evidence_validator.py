"""
Pure Strategy-B external lifecycle evidence identity validation.

This module only verifies correlation between immutable evidence objects.

It has no lifecycle-transition, persistence, Redis, broker, risk,
order-placement, position-management, outcome-writing, or Strategy-A
authority.
"""

from dataclasses import dataclass

from api.strategy_b_lifecycle_evidence import (
    StrategyBOrderAcceptedEvidence,
    StrategyBPositionClosedEvidence,
    StrategyBPositionOpenedEvidence,
)


@dataclass(frozen=True)
class StrategyBEvidenceIdentityValidation:
    ok: bool
    reason: str


_MATCH = StrategyBEvidenceIdentityValidation(
    ok=True,
    reason="IDENTITY_MATCH",
)


def _reject(reason: str) -> StrategyBEvidenceIdentityValidation:
    return StrategyBEvidenceIdentityValidation(
        ok=False,
        reason=reason,
    )


def _validate_common_identity(left, right):
    if left.thesis_id != right.thesis_id:
        return _reject("THESIS_ID_MISMATCH")

    if left.selected_capture_id != right.selected_capture_id:
        return _reject("SELECTED_CAPTURE_ID_MISMATCH")

    if left.symbol != right.symbol:
        return _reject("SYMBOL_MISMATCH")

    if left.side != right.side:
        return _reject("SIDE_MISMATCH")

    return None


def validate_order_to_position_identity(
    accepted: StrategyBOrderAcceptedEvidence,
    opened: StrategyBPositionOpenedEvidence,
) -> StrategyBEvidenceIdentityValidation:
    """
    Verify that position-open evidence belongs to the accepted Strategy-B
    order evidence.

    Validation only. No lifecycle transition is performed.
    """

    common = _validate_common_identity(accepted, opened)
    if common is not None:
        return common

    if accepted.broker_order_id != opened.broker_order_id:
        return _reject("BROKER_ORDER_ID_MISMATCH")

    return _MATCH


def validate_position_to_close_identity(
    opened: StrategyBPositionOpenedEvidence,
    closed: StrategyBPositionClosedEvidence,
) -> StrategyBEvidenceIdentityValidation:
    """
    Verify that position-close evidence belongs to the opened Strategy-B
    position evidence.

    Validation only. No lifecycle transition or OUTCOME write is performed.
    """

    common = _validate_common_identity(opened, closed)
    if common is not None:
        return common

    if opened.broker_position_id != closed.broker_position_id:
        return _reject("BROKER_POSITION_ID_MISMATCH")

    return _MATCH


__all__ = [
    "StrategyBEvidenceIdentityValidation",
    "validate_order_to_position_identity",
    "validate_position_to_close_identity",
]
