"""
Pure Strategy-B external lifecycle evidence authorization.

This module answers only whether immutable external evidence is sufficient
to authorize one of the broker-facing lifecycle boundaries:

    ENTRY_READY  -> ORDER_PENDING
    ORDER_PENDING -> TRADE_ACTIVE
    EXIT_PENDING -> CLOSED

It does NOT:
- perform a lifecycle transition;
- persist lifecycle state;
- read/write Redis;
- place, modify, or cancel broker orders;
- open or close positions;
- reserve risk;
- write calibration outcomes;
- mutate Strategy-A;
- provide execution or broker authority.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from api.strategy_b_lifecycle import validate_strategy_b_transition
from api.strategy_b_lifecycle_evidence import (
    StrategyBOrderAcceptedEvidence,
    StrategyBPositionClosedEvidence,
    StrategyBPositionOpenedEvidence,
)
from api.strategy_b_lifecycle_evidence_validator import (
    validate_order_to_position_identity,
    validate_position_to_close_identity,
)


@dataclass(frozen=True)
class StrategyBLifecycleEvidenceAuthorization:
    authorized: bool
    reason: str
    from_state: str
    to_state: str


def _upper(value: Any) -> str:
    return str(value or "").strip().upper()


def _exact_identity(value: Any) -> str:
    # Thesis/capture IDs are opaque causal identities.
    # Do not normalize them beyond requiring an actual non-empty string.
    if not isinstance(value, str) or not value:
        return ""
    return value


def _result(
    authorized: bool,
    reason: str,
    from_state: str,
    to_state: str,
) -> StrategyBLifecycleEvidenceAuthorization:
    return StrategyBLifecycleEvidenceAuthorization(
        authorized=bool(authorized),
        reason=str(reason),
        from_state=_upper(from_state),
        to_state=_upper(to_state),
    )


def _source_state_required(
    current_state: Any,
    required_state: str,
    target_state: str,
) -> StrategyBLifecycleEvidenceAuthorization | None:
    src = _upper(current_state)

    if src != required_state:
        return _result(
            False,
            f"SOURCE_STATE_NOT_{required_state}",
            src,
            target_state,
        )

    # Reuse the canonical lifecycle contract rather than manufacturing
    # a parallel state machine.
    legal, reason = validate_strategy_b_transition(src, target_state)
    if not legal:
        return _result(
            False,
            reason,
            src,
            target_state,
        )

    return None


def authorize_order_pending(
    *,
    current_state: str,
    thesis_id: str,
    selected_capture_id: str,
    selected_symbol: str,
    selected_side: str,
    evidence: Any,
) -> StrategyBLifecycleEvidenceAuthorization:
    target = "ORDER_PENDING"

    state_error = _source_state_required(
        current_state,
        "ENTRY_READY",
        target,
    )
    if state_error is not None:
        return state_error

    if not isinstance(evidence, StrategyBOrderAcceptedEvidence):
        return _result(
            False,
            "ORDER_ACCEPTED_EVIDENCE_REQUIRED",
            current_state,
            target,
        )

    expected_thesis = _exact_identity(thesis_id)
    expected_capture = _exact_identity(selected_capture_id)
    expected_symbol = _upper(selected_symbol)
    expected_side = _upper(selected_side)

    if not (
        expected_thesis
        and expected_capture
        and expected_symbol
        and expected_side in {"BUY", "SELL"}
    ):
        return _result(
            False,
            "LATCHED_IDENTITY_INCOMPLETE",
            current_state,
            target,
        )

    if evidence.thesis_id != expected_thesis:
        return _result(
            False,
            "THESIS_ID_MISMATCH",
            current_state,
            target,
        )

    if evidence.selected_capture_id != expected_capture:
        return _result(
            False,
            "SELECTED_CAPTURE_ID_MISMATCH",
            current_state,
            target,
        )

    if evidence.symbol != expected_symbol:
        return _result(
            False,
            "SYMBOL_MISMATCH",
            current_state,
            target,
        )

    if evidence.side != expected_side:
        return _result(
            False,
            "SIDE_MISMATCH",
            current_state,
            target,
        )

    return _result(
        True,
        "ORDER_ACCEPTED_EVIDENCE_AUTHORIZED",
        current_state,
        target,
    )


def authorize_trade_active(
    *,
    current_state: str,
    accepted_evidence: Any,
    opened_evidence: Any,
) -> StrategyBLifecycleEvidenceAuthorization:
    target = "TRADE_ACTIVE"

    state_error = _source_state_required(
        current_state,
        "ORDER_PENDING",
        target,
    )
    if state_error is not None:
        return state_error

    if not isinstance(
        accepted_evidence,
        StrategyBOrderAcceptedEvidence,
    ):
        return _result(
            False,
            "ORDER_ACCEPTED_EVIDENCE_REQUIRED",
            current_state,
            target,
        )

    if not isinstance(
        opened_evidence,
        StrategyBPositionOpenedEvidence,
    ):
        return _result(
            False,
            "POSITION_OPENED_EVIDENCE_REQUIRED",
            current_state,
            target,
        )

    identity = validate_order_to_position_identity(
        accepted_evidence,
        opened_evidence,
    )

    if not identity.ok:
        return _result(
            False,
            identity.reason,
            current_state,
            target,
        )

    return _result(
        True,
        "POSITION_OPENED_EVIDENCE_AUTHORIZED",
        current_state,
        target,
    )


def authorize_closed(
    *,
    current_state: str,
    opened_evidence: Any,
    closed_evidence: Any,
) -> StrategyBLifecycleEvidenceAuthorization:
    target = "CLOSED"

    allowed_source_states = {
        "TRADE_ACTIVE",
        "THESIS_STRONG",
        "THESIS_STABLE",
        "THESIS_WEAKENING",
        "THESIS_INVALID",
        "EXIT_PENDING",
    }

    source_state = str(current_state or "").strip().upper()
    if source_state not in allowed_source_states:
        return _result(
            False,
            "SOURCE_STATE_NOT_POSITION_OWNING",
            current_state,
            target,
        )

    if not isinstance(
        opened_evidence,
        StrategyBPositionOpenedEvidence,
    ):
        return _result(
            False,
            "POSITION_OPENED_EVIDENCE_REQUIRED",
            current_state,
            target,
        )

    if not isinstance(
        closed_evidence,
        StrategyBPositionClosedEvidence,
    ):
        return _result(
            False,
            "POSITION_CLOSED_EVIDENCE_REQUIRED",
            current_state,
            target,
        )

    identity = validate_position_to_close_identity(
        opened_evidence,
        closed_evidence,
    )

    if not identity.ok:
        return _result(
            False,
            identity.reason,
            current_state,
            target,
        )

    return _result(
        True,
        "POSITION_CLOSED_EVIDENCE_AUTHORIZED",
        current_state,
        target,
    )


__all__ = [
    "StrategyBLifecycleEvidenceAuthorization",
    "authorize_order_pending",
    "authorize_trade_active",
    "authorize_closed",
]
