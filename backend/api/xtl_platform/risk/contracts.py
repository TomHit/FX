"""Day-3 risk EVALUATION contracts.

RISK_EVALUATION_PASS IS NOT AUTHORIZATION.

These types describe inputs to, and results of, a PURE capacity check. They are
deliberately separate from the Day-1 authority types in `domain/risk.py`:

* Day-1 `AccountRiskSnapshot` excludes open risk by design (the reference
  ledger owns it) and has a single `halted` flag. Day-3 evaluation must consume
  open risk, position count and two distinct halts as supplied INPUT.
* No type here can represent a reservation or an authorization.

Construction performs no value validation. `evaluator.evaluate_risk` validates
every input first and returns BLOCK_INVALID_RISK_INPUT instead of raising, so
evaluation is total over well-typed objects. Callers who want an exception can
use `validation.validate_risk_inputs`.

Amount fields keep the Day-1 `_usd` suffix convention; values are in the
account currency (`AccountIdentity.currency`).
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import ClassVar, Optional

from ..domain._validation import ContractError
from ..domain.account import AccountIdentity
from ..domain.enums import LossModel, Side, StrategyId, StrEnum
from ..domain.thesis import ChildExecutionIntent


class RiskEvaluationCode(StrEnum):
    """Dedicated Day-3 vocabulary. Contains no authorization or reservation code."""

    RISK_EVALUATION_PASS = "RISK_EVALUATION_PASS"
    BLOCK_INVALID_RISK_INPUT = "BLOCK_INVALID_RISK_INPUT"
    BLOCK_ACCOUNT_MISMATCH = "BLOCK_ACCOUNT_MISMATCH"
    STALE_ACCOUNT_STATE = "STALE_ACCOUNT_STATE"
    BLOCK_ACCOUNT_HALT = "BLOCK_ACCOUNT_HALT"
    BLOCK_DAILY_HALT = "BLOCK_DAILY_HALT"
    BLOCK_TRADE_RISK = "BLOCK_TRADE_RISK"
    BLOCK_MAX_POSITIONS = "BLOCK_MAX_POSITIONS"
    BLOCK_DAILY_LIMIT = "BLOCK_DAILY_LIMIT"
    BLOCK_MAX_LOSS = "BLOCK_MAX_LOSS"
    BLOCK_OPEN_RISK = "BLOCK_OPEN_RISK"


class FloatingPnlTreatment(StrEnum):
    """How floating (unrealized) PnL enters a loss calculation. Always explicit.

    EXCLUDED     floating PnL is ignored
    LOSSES_ONLY  floating losses count; floating profit adds no capacity
    FULL         floating profit and loss both count (profit may offset loss)
    """

    EXCLUDED = "EXCLUDED"
    LOSSES_ONLY = "LOSSES_ONLY"
    FULL = "FULL"


class CapacityCheckName(StrEnum):
    TRADE_RISK = "TRADE_RISK"
    MAX_POSITIONS = "MAX_POSITIONS"
    DAILY_LOSS = "DAILY_LOSS"
    MAX_LOSS = "MAX_LOSS"
    OPEN_RISK = "OPEN_RISK"


class CapacityUnit(StrEnum):
    ACCOUNT_CURRENCY = "ACCOUNT_CURRENCY"
    POSITIONS = "POSITIONS"


#: Loss models the generic Day-3 formulas support. Anything else fails closed.
SUPPORTED_DAILY_LOSS_MODELS = frozenset({LossModel.NONE, LossModel.FIXED, LossModel.EQUITY_BASED})
SUPPORTED_MAX_LOSS_MODELS = frozenset(
    {LossModel.NONE, LossModel.STATIC, LossModel.TRAILING_EOD, LossModel.TRAILING_INTRADAY})

CAPACITY_BLOCK_CODES = {
    CapacityCheckName.TRADE_RISK: RiskEvaluationCode.BLOCK_TRADE_RISK,
    CapacityCheckName.MAX_POSITIONS: RiskEvaluationCode.BLOCK_MAX_POSITIONS,
    CapacityCheckName.DAILY_LOSS: RiskEvaluationCode.BLOCK_DAILY_LIMIT,
    CapacityCheckName.MAX_LOSS: RiskEvaluationCode.BLOCK_MAX_LOSS,
    CapacityCheckName.OPEN_RISK: RiskEvaluationCode.BLOCK_OPEN_RISK,
}


@dataclass(frozen=True)
class RiskEvaluationConfig:
    """Evaluation configuration. Freshness is configuration, never a hidden constant."""

    max_snapshot_age: timedelta


@dataclass(frozen=True)
class RiskEvaluationSnapshot:
    """Immutable account state supplied by the caller. Authoritative INPUT for Day 3.

    floating PnL is derived (equity - balance) rather than stored, so it cannot
    disagree with balance and equity.

    Position slots (Day-4 CSA correction, additive):
      open_positions           committed/broker open positions ONLY
      reserved_position_slots  slots held by authorized-but-unfilled reservations;
                               NOT positions. Defaults to 0 (no reservations).
      MAX_POSITIONS projected slots = open_positions + reserved_position_slots + 1
    """

    profile_id: str
    account: AccountIdentity
    as_of: datetime
    balance: Decimal
    equity: Decimal
    realized_daily_pnl: Decimal
    open_risk_usd: Decimal
    open_positions: int
    account_halted: bool
    daily_halted: bool
    # Reference for max-loss: initial balance (STATIC) or supplied high-water
    # mark (TRAILING_*). Required unless max_loss_model is NONE.
    max_loss_reference_balance: Optional[Decimal]
    # Day-4 CSA correction (additive). Must be the last field so existing
    # constructors are unaffected.
    reserved_position_slots: int = 0

    @property
    def floating_pnl(self) -> Decimal:
        return self.equity - self.balance


@dataclass(frozen=True)
class RiskEvaluationLimits:
    """Validated limits for ONE account, supplied by the caller. No defaults: every
    model choice, including floating-PnL treatment, must be explicit."""

    profile_id: str
    source_policy_version: str
    max_risk_per_trade_usd: Decimal
    max_positions: int
    max_open_risk_usd: Decimal
    daily_loss_model: LossModel
    daily_loss_limit_usd: Optional[Decimal]
    daily_floating_pnl: Optional[FloatingPnlTreatment]
    max_loss_model: LossModel
    max_loss_limit_usd: Optional[Decimal]
    max_loss_floating_pnl: Optional[FloatingPnlTreatment]


@dataclass(frozen=True)
class RiskEvaluationRequest:
    """Proposed, already-normalised monetary risk for one child trade.

    `proposed_risk_usd` is authoritative. `proposed_risk_pct` is optional,
    validated, and recorded for audit only.
    """

    intent: ChildExecutionIntent
    proposed_risk_usd: Decimal
    proposed_risk_pct: Optional[Decimal]
    evaluated_at: datetime

    @property
    def thesis_id(self) -> str:
        return self.intent.thesis_id

    @property
    def child_trade_id(self) -> str:
        return self.intent.child_trade_id

    @property
    def profile_id(self) -> str:
        return self.intent.profile_id

    @property
    def account(self) -> AccountIdentity:
        return self.intent.account


@dataclass(frozen=True)
class CapacityCheck:
    """One capacity calculation. Canonical boundary rule: fits iff projected <= limit.

    `used` is None for checks with no existing usage (TRADE_RISK).
    `components` lists the named terms that produced `used`, for audit.
    """

    name: CapacityCheckName
    unit: CapacityUnit
    limit: Decimal
    used: Optional[Decimal]
    proposed: Decimal
    projected: Decimal
    remaining_before: Decimal
    remaining_after: Decimal
    fits: bool
    components: tuple[tuple[str, Decimal], ...] = ()


@dataclass(frozen=True)
class RiskEvaluation:
    """Structured, non-authoritative evaluation result.

    RISK_EVALUATION_PASS means only: the supplied snapshot appears to have
    sufficient capacity. It is NOT authorization and reserves nothing.
    """

    IS_AUTHORIZATION: ClassVar[bool] = False

    thesis_id: str
    child_trade_id: str
    strategy_id: StrategyId
    profile_id: str
    account: AccountIdentity
    canonical_symbol: str
    side: Side
    code: RiskEvaluationCode
    reason: str
    evaluated_at: Optional[datetime]
    snapshot_at: Optional[datetime]
    proposed_risk_usd: Optional[Decimal]
    source_policy_version: Optional[str]
    checks: tuple[CapacityCheck, ...]
    skipped_checks: tuple[CapacityCheckName, ...]
    decisive_check: Optional[CapacityCheck]
    input_errors: tuple[str, ...]

    def __post_init__(self) -> None:
        C = RiskEvaluationCode
        if not isinstance(self.code, RiskEvaluationCode):
            raise ContractError("code must be a RiskEvaluationCode")
        if not isinstance(self.reason, str) or not self.reason.strip():
            raise ContractError("reason must be a non-empty string")
        if self.code is C.BLOCK_INVALID_RISK_INPUT:
            if not self.input_errors:
                raise ContractError("BLOCK_INVALID_RISK_INPUT requires input_errors")
            if self.checks or self.decisive_check is not None:
                raise ContractError("invalid input must not carry capacity checks")
            return
        if self.input_errors:
            raise ContractError("input_errors only allowed with BLOCK_INVALID_RISK_INPUT")
        for name in ("evaluated_at", "snapshot_at"):
            v = getattr(self, name)
            if not isinstance(v, datetime) or v.tzinfo is None or v.utcoffset() is None:
                raise ContractError(f"{name} must be timezone-aware for {self.code}")
        if self.proposed_risk_usd is None or self.source_policy_version is None:
            raise ContractError(f"proposed_risk_usd and source_policy_version required for {self.code}")
        capacity_code_names = {v: k for k, v in CAPACITY_BLOCK_CODES.items()}
        if self.code is C.RISK_EVALUATION_PASS:
            if self.decisive_check is not None or not self.checks or not all(c.fits for c in self.checks):
                raise ContractError("PASS requires evaluated checks that all fit and no decisive check")
        elif self.code in capacity_code_names:
            d = self.decisive_check
            if (d is None or d.fits or d.name is not capacity_code_names[self.code]
                    or not self.checks or self.checks[-1] is not d):
                raise ContractError(f"{self.code} requires a failing decisive {capacity_code_names[self.code]} check")
        elif self.decisive_check is not None or self.checks:
            raise ContractError(f"{self.code} must not carry capacity checks")

    @property
    def passed(self) -> bool:
        return self.code is RiskEvaluationCode.RISK_EVALUATION_PASS

    @property
    def relevant_limit(self) -> Optional[Decimal]:
        return self.decisive_check.limit if self.decisive_check else None

    @property
    def used_capacity(self) -> Optional[Decimal]:
        return self.decisive_check.used if self.decisive_check else None

    @property
    def remaining_capacity_before(self) -> Optional[Decimal]:
        return self.decisive_check.remaining_before if self.decisive_check else None

    @property
    def projected_capacity_after(self) -> Optional[Decimal]:
        return self.decisive_check.remaining_after if self.decisive_check else None
