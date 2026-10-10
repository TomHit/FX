from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Optional

from ._validation import ContractError, require_aware, require_text
from .enums import AckStatus, LifecycleState as S, OrderType, Side
from .risk import RiskReservation


@dataclass(frozen=True)
class ExecutionCommand:
    command_id: str
    reservation: RiskReservation
    order_type: OrderType
    created_at: datetime
    expires_at: datetime

    def __post_init__(self) -> None:
        require_text("command_id", self.command_id)
        require_aware("created_at", self.created_at)
        require_aware("expires_at", self.expires_at)
        if self.expires_at > self.reservation.expires_at:
            raise ContractError("command cannot outlive its risk reservation")

    @property
    def trade_id(self) -> str:
        return self.reservation.trade_id


@dataclass(frozen=True)
class ExecutionAck:
    command_id: str
    trade_id: str
    status: AckStatus
    broker_order_id: Optional[str]
    reason: str
    received_at: datetime

    @property
    def confirmed_placement(self) -> bool:
        # PRD 14: absence of an ACK is never success.
        return self.status is AckStatus.ACCEPTED and bool(self.broker_order_id)


@dataclass(frozen=True)
class BrokerPosition:
    profile_id: str
    venue_symbol: str
    side: Side
    quantity: Decimal
    broker_position_id: str
    trade_id: Optional[str]


class InvalidTransition(ContractError):
    pass


ALLOWED_TRANSITIONS: dict[S, frozenset[S]] = {
    S.THESIS_READY: frozenset({S.POLICY_ELIGIBLE, S.BLOCKED}),
    S.POLICY_ELIGIBLE: frozenset({S.RISK_EVALUATED, S.BLOCKED}),
    S.RISK_EVALUATED: frozenset({S.AUTHORIZED_RESERVED, S.BLOCKED}),
    # RELEASED here = reservation expired/cancelled before anything was sent
    S.AUTHORIZED_RESERVED: frozenset({S.ORDER_PENDING, S.RELEASED}),
    # RELEASED here only on broker-confirmed reject/cancel, never on missing ACK
    S.ORDER_PENDING: frozenset({S.TRADE_ACTIVE, S.RELEASED}),
    S.TRADE_ACTIVE: frozenset({S.PROTECT, S.MODIFY, S.EXIT, S.CLOSED}),
    S.PROTECT: frozenset({S.TRADE_ACTIVE, S.MODIFY, S.EXIT, S.CLOSED}),
    S.MODIFY: frozenset({S.TRADE_ACTIVE, S.PROTECT, S.EXIT, S.CLOSED}),
    S.EXIT: frozenset({S.CLOSED}),
    S.CLOSED: frozenset({S.RELEASED}),
    S.RELEASED: frozenset({S.RECONCILED}),
    S.RECONCILED: frozenset(),
    S.BLOCKED: frozenset(),
}


def can_transition(current: S, target: S) -> bool:
    return target in ALLOWED_TRANSITIONS[current]


def assert_transition(current: S, target: S) -> None:
    if not can_transition(current, target):
        raise InvalidTransition(f"{current} -> {target} is not allowed")
