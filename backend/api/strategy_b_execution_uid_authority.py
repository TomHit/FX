from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Collection


@dataclass(frozen=True)
class StrategyBExecutionUidDecision:
    valid: bool
    decision: str
    reason: str
    uid: str | None

    observation_only: bool = True
    profile_selection_authority: bool = False
    account_selection_authority: bool = False
    risk_reservation_authority: bool = False
    order_authority: bool = False
    broker_authority: bool = False
    execution_authority: bool = False
    strategy_a_mutation: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _uid(value: Any) -> str:
    if value is None:
        return ""

    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8")
        except Exception:
            return ""

    if not isinstance(value, str):
        return ""

    return value.strip()


def validate_strategy_b_execution_uid(
    configured_uid: Any,
    enabled_uids: Collection[Any],
) -> StrategyBExecutionUidDecision:
    """
    Pure Strategy-B execution-UID authority.

    This function does not choose a UID.

    It validates one explicitly configured UID against the caller-supplied
    current enabled-user membership.

    It owns no profile/account/device resolution, Redis access, risk,
    reservation, lifecycle transition, order, broker, or MT5 authority.
    """

    uid = _uid(configured_uid)

    if not uid:
        return StrategyBExecutionUidDecision(
            valid=False,
            decision="WAIT",
            reason="STRATEGY_B_EXECUTION_UID_NOT_CONFIGURED",
            uid=None,
        )

    try:
        normalized_enabled = {
            candidate
            for value in enabled_uids
            if (candidate := _uid(value))
        }
    except Exception:
        return StrategyBExecutionUidDecision(
            valid=False,
            decision="WAIT",
            reason="ENABLED_UIDS_INVALID",
            uid=None,
        )

    if uid not in normalized_enabled:
        return StrategyBExecutionUidDecision(
            valid=False,
            decision="WAIT",
            reason="STRATEGY_B_EXECUTION_UID_NOT_ENABLED",
            uid=uid,
        )

    return StrategyBExecutionUidDecision(
        valid=True,
        decision="UID_READY",
        reason="STRATEGY_B_EXECUTION_UID_VALID",
        uid=uid,
    )


__all__ = [
    "StrategyBExecutionUidDecision",
    "validate_strategy_b_execution_uid",
]
