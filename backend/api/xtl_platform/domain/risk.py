from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Optional

from ._validation import ContractError, require_aware, require_decimal, require_text
from .account import AccountIdentity
from .enums import RiskDecisionCode, Side, StrategyId, Venue
from .thesis import ChildExecutionIntent


@dataclass(frozen=True)
class AccountRiskLimits:
    profile_id: str
    daily_loss_limit_usd: Decimal
    max_loss_limit_usd: Decimal
    max_open_risk_usd: Decimal
    max_positions: int

    def __post_init__(self) -> None:
        require_text("profile_id", self.profile_id)
        for name in ("daily_loss_limit_usd", "max_loss_limit_usd", "max_open_risk_usd"):
            require_decimal(name, getattr(self, name), positive=True)
        if self.max_positions < 1:
            raise ContractError("max_positions must be >= 1")


@dataclass(frozen=True)
class AccountRiskSnapshot:
    """Broker-derived account state. Open risk is NOT taken from here: the
    reservation ledger is the single open-risk truth."""

    profile_id: str
    balance: Decimal
    equity: Decimal
    daily_loss_used_usd: Decimal
    max_loss_used_usd: Decimal
    halted: bool
    as_of: datetime

    def __post_init__(self) -> None:
        require_text("profile_id", self.profile_id)
        require_decimal("balance", self.balance, positive=True)
        require_decimal("equity", self.equity, non_negative=True)
        require_decimal("daily_loss_used_usd", self.daily_loss_used_usd, non_negative=True)
        require_decimal("max_loss_used_usd", self.max_loss_used_usd, non_negative=True)
        require_aware("as_of", self.as_of)


@dataclass(frozen=True)
class RiskRequest:
    intent: ChildExecutionIntent
    requested_risk_usd: Decimal
    quantity: Decimal
    policy_version: str

    def __post_init__(self) -> None:
        require_decimal("requested_risk_usd", self.requested_risk_usd, positive=True)
        require_decimal("quantity", self.quantity, positive=True)
        require_text("policy_version", self.policy_version)


@dataclass(frozen=True)
class RiskReservation:
    authorization_id: str
    trade_id: str
    thesis_id: str
    strategy_id: StrategyId
    profile_id: str
    account: AccountIdentity
    venue: Venue
    venue_symbol: str
    side: Side
    risk_usd: Decimal
    risk_pct: Decimal
    quantity: Decimal
    entry: Decimal
    stop: Decimal
    target: Optional[Decimal]
    authorized_at: datetime
    expires_at: datetime
    policy_version: str


@dataclass(frozen=True)
class RiskAuthorizationResult:
    trade_id: str
    code: RiskDecisionCode
    reason: str
    reservation: Optional[RiskReservation] = None

    @property
    def authorized(self) -> bool:
        return self.code is RiskDecisionCode.AUTHORIZED_RESERVED
