"""
Pure Strategy-B pending-risk reservation lifecycle coordinator.

This module has no Redis, broker, MT5, executor, queue, or order authority.

It translates an already-established reservation disposition:

    KEEP
    RELEASE
    TRANSFER_READY

into either:

    no mutation request

or an immutable atomic risk-disposition commit request.

The actual Redis commit remains owned by the separate atomic disposition
adapter.

Restart/timeout without authoritative terminal or exact-position risk
evidence must arrive as KEEP and therefore cannot manufacture release.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from api.strategy_b_atomic_risk_disposition_contract import (
    StrategyBRiskDispositionCommitRequest,
)


_ALLOWED_DISPOSITIONS = frozenset(
    {
        "KEEP",
        "RELEASE",
        "TRANSFER_READY",
    }
)


def _clean(value: object, name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be str")

    cleaned = value.strip()

    if not cleaned:
        raise ValueError(f"{name} must be non-empty")

    if ":" in cleaned:
        raise ValueError(f"{name} contains forbidden ':'")

    return cleaned


def _strict_nonnegative_int(
    value: object,
    name: str,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be int")

    if value < 0:
        raise ValueError(f"{name} must be >= 0")

    return value


@dataclass(frozen=True)
class StrategyBReservationCoordinationInput:
    uid: str
    profile_id: str
    device_id: str
    account_login: str

    reservation_id: str
    thesis_id: str
    selected_capture_id: str

    decision_now_ms: int

    disposition: str
    disposition_reason: str


@dataclass(frozen=True)
class StrategyBReservationCoordinationResult:
    action: str
    reason: str

    commit_request: Optional[
        StrategyBRiskDispositionCommitRequest
    ]

    observation_only: bool = True
    execution_authority: bool = False
    order_authority: bool = False
    broker_authority: bool = False
    redis_authority: bool = False
    strategy_a_mutation: bool = False


def coordinate_strategy_b_risk_reservation(
    request: StrategyBReservationCoordinationInput,
) -> StrategyBReservationCoordinationResult:
    if not isinstance(
        request,
        StrategyBReservationCoordinationInput,
    ):
        raise TypeError(
            "request must be "
            "StrategyBReservationCoordinationInput"
        )

    uid = _clean(request.uid, "uid")
    profile_id = _clean(
        request.profile_id,
        "profile_id",
    )
    device_id = _clean(
        request.device_id,
        "device_id",
    )
    account_login = _clean(
        request.account_login,
        "account_login",
    )

    reservation_id = _clean(
        request.reservation_id,
        "reservation_id",
    )
    _clean(
        request.thesis_id,
        "thesis_id",
    )
    _clean(
        request.selected_capture_id,
        "selected_capture_id",
    )

    decision_now_ms = _strict_nonnegative_int(
        request.decision_now_ms,
        "decision_now_ms",
    )

    disposition = _clean(
        request.disposition,
        "disposition",
    ).upper()

    reason = _clean(
        request.disposition_reason,
        "disposition_reason",
    )

    if disposition not in _ALLOWED_DISPOSITIONS:
        raise ValueError(
            f"unsupported disposition: {disposition}"
        )

    if disposition == "KEEP":
        return StrategyBReservationCoordinationResult(
            action="KEEP",
            reason=reason,
            commit_request=None,
        )

    if disposition == "RELEASE":
        atomic_disposition = "RELEASED_BEFORE_FILL"

    elif disposition == "TRANSFER_READY":
        atomic_disposition = (
            "TRANSFERRED_TO_BROKER_RISK"
        )

    else:
        # Defensive fail closed. The allowed set above makes this
        # unreachable unless this function is modified incorrectly.
        raise ValueError(
            f"unsupported disposition: {disposition}"
        )

    commit = StrategyBRiskDispositionCommitRequest(
        uid=uid,
        profile_id=profile_id,
        device_id=device_id,
        account_login=account_login,
        reservation_id=reservation_id,
        disposition=atomic_disposition,
        decided_at_ms=decision_now_ms,
    )

    return StrategyBReservationCoordinationResult(
        action="COMMIT",
        reason=reason,
        commit_request=commit,
    )
