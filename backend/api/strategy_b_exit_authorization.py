from __future__ import annotations

from dataclasses import dataclass
from typing import Any


def _txt(value: Any) -> str:
    return str(value or "").strip()


def _upper(value: Any) -> str:
    return _txt(value).upper()


@dataclass(frozen=True)
class StrategyBExitAuthorization:
    authorized: bool
    reason: str
    thesis_id: str
    selected_capture_id: str
    symbol: str
    side: str
    lifecycle_state: str
    decision: str
    transition_plan_valid: bool
    execution_authority: bool = False
    broker_close_authority: bool = False
    strategy_a_mutation: bool = False


def authorize_strategy_b_exit_intent(
    *,
    thesis_id: str,
    selected_capture_id: str,
    symbol: str,
    side: str,
    shadow_decision: Any,
) -> StrategyBExitAuthorization:
    """
    Validate that the existing Strategy-B shadow/orchestrator result
    has produced a valid EXIT_PENDING lifecycle intent.

    This function does not:
    - persist lifecycle state;
    - submit or modify broker orders;
    - close a position;
    - reserve or release risk;
    - mutate Strategy-A.

    Broker execution authority remains outside this pure contract.
    """

    thesis_id_u = _txt(thesis_id)
    capture_id_u = _txt(selected_capture_id)
    symbol_u = _upper(symbol)
    side_u = _upper(side)

    def out(
        authorized: bool,
        reason: str,
        lifecycle_state: str = "",
        decision: str = "",
        transition_plan_valid: bool = False,
    ) -> StrategyBExitAuthorization:
        return StrategyBExitAuthorization(
            authorized=bool(authorized),
            reason=str(reason),
            thesis_id=thesis_id_u,
            selected_capture_id=capture_id_u,
            symbol=symbol_u,
            side=side_u,
            lifecycle_state=_upper(lifecycle_state),
            decision=_upper(decision),
            transition_plan_valid=bool(transition_plan_valid),
            execution_authority=False,
            broker_close_authority=False,
            strategy_a_mutation=False,
        )

    if not thesis_id_u:
        return out(False, "THESIS_ID_REQUIRED")

    if not capture_id_u:
        return out(False, "SELECTED_CAPTURE_ID_REQUIRED")

    if not symbol_u:
        return out(False, "SYMBOL_REQUIRED")

    if side_u not in ("BUY", "SELL"):
        return out(False, "SIDE_INVALID")

    if shadow_decision is None:
        return out(False, "SHADOW_DECISION_REQUIRED")

    decision = _upper(
        getattr(shadow_decision, "decision", "")
    )
    derived_state = _upper(
        getattr(shadow_decision, "derived_state", "")
    )
    plan_valid_raw = getattr(
        shadow_decision,
        "transition_plan_valid",
        None,
    )

    if not isinstance(plan_valid_raw, bool):
        return out(
            False,
            "TRANSITION_PLAN_VALID_REQUIRED",
            derived_state,
            decision,
            False,
        )

    if decision != "EXIT_REVIEW":
        return out(
            False,
            "EXIT_REVIEW_REQUIRED",
            derived_state,
            decision,
            plan_valid_raw,
        )

    if plan_valid_raw is not True:
        return out(
            False,
            "TRANSITION_PLAN_NOT_VALID",
            derived_state,
            decision,
            False,
        )

    if derived_state != "EXIT_PENDING":
        return out(
            False,
            "EXIT_PENDING_REQUIRED",
            derived_state,
            decision,
            True,
        )

    return out(
        True,
        "EXIT_PENDING_INTENT_AUTHORIZED",
        derived_state,
        decision,
        True,
    )


__all__ = [
    "StrategyBExitAuthorization",
    "authorize_strategy_b_exit_intent",
]
