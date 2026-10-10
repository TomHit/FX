from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from api.strategy_b_execution_assignment import StrategyBExecutionAssignment


@dataclass(frozen=True)
class StrategyBExecutionOwnership:
    thesis_id: str
    selected_capture_id: str
    symbol: str
    side: str

    uid: str
    profile_id: str
    device_id: str
    account_login: str
    account_server: str
    broker_company: str

    observation_only: bool = True
    risk_reservation_authority: bool = False
    order_authority: bool = False
    broker_authority: bool = False
    execution_authority: bool = False
    strategy_a_mutation: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class StrategyBExecutionOwnershipDecision:
    allowed: bool
    reason: str
    ownership: StrategyBExecutionOwnership | None


def _text(value: Any) -> str:
    return str(value or "").strip()


def _upper(value: Any) -> str:
    return _text(value).upper()


def _identity_tuple(
    value: StrategyBExecutionOwnership,
) -> tuple[str, ...]:
    return (
        value.thesis_id,
        value.selected_capture_id,
        value.symbol,
        value.side,
        value.uid,
        value.profile_id,
        value.device_id,
        value.account_login,
        value.account_server.lower(),
        value.broker_company.lower(),
    )


def ownership_from_assignment(
    assignment: StrategyBExecutionAssignment,
) -> StrategyBExecutionOwnershipDecision:
    """
    Project one already-proven execution assignment into immutable ownership.

    This function does not choose a user/account. The caller must already have
    resolved the production execution identity and obtained a valid assignment.
    """

    if not isinstance(
        assignment,
        StrategyBExecutionAssignment,
    ):
        return StrategyBExecutionOwnershipDecision(
            False,
            "EXECUTION_ASSIGNMENT_REQUIRED",
            None,
        )

    if assignment.valid is not True:
        return StrategyBExecutionOwnershipDecision(
            False,
            "EXECUTION_ASSIGNMENT_INVALID",
            None,
        )

    required = {
        "thesis_id": _text(assignment.thesis_id),
        "selected_capture_id": _text(
            assignment.selected_capture_id
        ),
        "symbol": _upper(assignment.symbol),
        "side": _upper(assignment.side),
        "uid": _text(assignment.uid),
        "profile_id": _text(assignment.profile_id),
        "device_id": _text(assignment.device_id),
        "account_login": _text(assignment.account_login),
        "account_server": _text(assignment.account_server),
        "broker_company": _text(assignment.broker_company),
    }

    if not all(required.values()):
        return StrategyBExecutionOwnershipDecision(
            False,
            "EXECUTION_ASSIGNMENT_IDENTITY_INCOMPLETE",
            None,
        )

    if required["side"] not in ("BUY", "SELL"):
        return StrategyBExecutionOwnershipDecision(
            False,
            "SIDE_INVALID",
            None,
        )

    ownership = StrategyBExecutionOwnership(
        **required,
    )

    return StrategyBExecutionOwnershipDecision(
        True,
        "OWNERSHIP_CANDIDATE_READY",
        ownership,
    )


def validate_strategy_b_execution_ownership_replacement(
    current: StrategyBExecutionOwnership | None,
    proposed: StrategyBExecutionOwnership,
) -> StrategyBExecutionOwnershipDecision:
    """
    Pure immutable ownership replacement contract.

    Missing -> proposed:
        allowed.

    Existing identical -> proposed:
        allowed as idempotent.

    Existing different -> proposed:
        rejected. Ownership cannot move between accounts.
    """

    if not isinstance(
        proposed,
        StrategyBExecutionOwnership,
    ):
        return StrategyBExecutionOwnershipDecision(
            False,
            "PROPOSED_OWNERSHIP_REQUIRED",
            None,
        )

    if current is None:
        return StrategyBExecutionOwnershipDecision(
            True,
            "OWNERSHIP_AVAILABLE",
            proposed,
        )

    if not isinstance(
        current,
        StrategyBExecutionOwnership,
    ):
        return StrategyBExecutionOwnershipDecision(
            False,
            "CURRENT_OWNERSHIP_INVALID",
            None,
        )

    if _identity_tuple(current) == _identity_tuple(proposed):
        return StrategyBExecutionOwnershipDecision(
            True,
            "IDENTICAL_OWNERSHIP",
            current,
        )

    return StrategyBExecutionOwnershipDecision(
        False,
        "THESIS_ALREADY_OWNED",
        current,
    )
