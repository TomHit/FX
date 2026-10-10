"""Day-4 atomic risk authorization CONTRACTS.

REFERENCE ONLY - NON-PRODUCTION. These types model the semantics of a future
authorize_and_reserve() operation. They grant no production authority, create
no production reservation, and describe no order.

Reused frozen types: ChildExecutionIntent, AccountIdentity, PolicyDecision
(Day 1/2) and RiskEvaluation, RiskEvaluationLimits (Day 3).

New types (and why the frozen Day-1 equivalents were not reused):
* AtomicAuthorizationCode - Day-1 RiskDecisionCode lacks BLOCK_POLICY_PRECONDITION,
  BLOCK_ACCOUNT_MISMATCH, BLOCK_ACCOUNT_HALT, BLOCK_TRADE_RISK and
  BLOCK_INVALID_RISK_INPUT; extending it would modify a frozen file.
* ReferenceReservation - Day-1 RiskReservation requires quantity, entry, stop,
  venue and expiry: execution fields Day 4 explicitly excludes.
* ReferenceReservationStatus - Day-1 ReservationStatus (RESERVED/SENT/...) are
  broker-lifecycle states; Day 4 needs ACTIVE only.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import ClassVar, Optional

from ..domain._validation import ContractError
from ..domain.account import AccountIdentity
from ..domain.enums import Side, StrategyId, StrEnum
from ..domain.policy import PolicyDecision
from ..domain.thesis import ChildExecutionIntent
from .contracts import RiskEvaluation, RiskEvaluationCode, RiskEvaluationLimits


class AtomicAuthorizationCode(StrEnum):
    AUTHORIZED_RESERVED = "AUTHORIZED_RESERVED"
    DUPLICATE_TRADE_ID = "DUPLICATE_TRADE_ID"
    BLOCK_POLICY_PRECONDITION = "BLOCK_POLICY_PRECONDITION"
    BLOCK_ACCOUNT_MISMATCH = "BLOCK_ACCOUNT_MISMATCH"
    STALE_ACCOUNT_STATE = "STALE_ACCOUNT_STATE"
    BLOCK_ACCOUNT_HALT = "BLOCK_ACCOUNT_HALT"
    BLOCK_DAILY_HALT = "BLOCK_DAILY_HALT"
    BLOCK_TRADE_RISK = "BLOCK_TRADE_RISK"
    BLOCK_MAX_POSITIONS = "BLOCK_MAX_POSITIONS"
    BLOCK_DAILY_LIMIT = "BLOCK_DAILY_LIMIT"
    BLOCK_MAX_LOSS = "BLOCK_MAX_LOSS"
    BLOCK_OPEN_RISK = "BLOCK_OPEN_RISK"
    BLOCK_INVALID_RISK_INPUT = "BLOCK_INVALID_RISK_INPUT"
    # Reference-only: clock failure, evaluator exception, or reservation build failure.
    BLOCK_REFERENCE_EVALUATION_FAILURE = "BLOCK_REFERENCE_EVALUATION_FAILURE"


class ReferenceReservationStatus(StrEnum):
    ACTIVE = "ACTIVE"


A = AtomicAuthorizationCode
R = RiskEvaluationCode

#: Deterministic Day-3 -> Day-4 block mapping. PASS is intentionally absent.
RISK_BLOCK_TO_AUTHORIZATION_CODE = {
    R.BLOCK_INVALID_RISK_INPUT: A.BLOCK_INVALID_RISK_INPUT,
    R.BLOCK_ACCOUNT_MISMATCH: A.BLOCK_ACCOUNT_MISMATCH,
    R.STALE_ACCOUNT_STATE: A.STALE_ACCOUNT_STATE,
    R.BLOCK_ACCOUNT_HALT: A.BLOCK_ACCOUNT_HALT,
    R.BLOCK_DAILY_HALT: A.BLOCK_DAILY_HALT,
    R.BLOCK_TRADE_RISK: A.BLOCK_TRADE_RISK,
    R.BLOCK_MAX_POSITIONS: A.BLOCK_MAX_POSITIONS,
    R.BLOCK_DAILY_LIMIT: A.BLOCK_DAILY_LIMIT,
    R.BLOCK_MAX_LOSS: A.BLOCK_MAX_LOSS,
    R.BLOCK_OPEN_RISK: A.BLOCK_OPEN_RISK,
}


def _aware(v: object) -> bool:
    return isinstance(v, datetime) and v.tzinfo is not None and v.utcoffset() is not None


@dataclass(frozen=True)
class AtomicAuthorizationRequest:
    """Immutable request to atomically authorize and reserve one child trade.

    `policy_decision` is evidence of Day-2 eligibility. It is NEVER proof of risk
    capacity: risk is always re-evaluated inside the atomic section.
    `requested_at` is audit only. The authority uses its own clock for freshness,
    so a caller cannot back-date a request to bypass staleness.
    """

    intent: ChildExecutionIntent
    proposed_risk_usd: Decimal
    requested_at: datetime
    source_policy_version: str
    policy_decision: Optional[PolicyDecision]

    @property
    def child_trade_id(self) -> str:
        return self.intent.child_trade_id

    @property
    def thesis_id(self) -> str:
        return self.intent.thesis_id

    @property
    def strategy_id(self) -> StrategyId:
        return self.intent.strategy_id

    @property
    def profile_id(self) -> str:
        return self.intent.profile_id

    @property
    def account(self) -> AccountIdentity:
        return self.intent.account

    @property
    def idempotency_fingerprint(self) -> tuple:
        """Material identity of the request. Excludes requested_at and the policy
        evidence object; the policy version itself is included."""
        return (self.intent, self.proposed_risk_usd, self.source_policy_version)


@dataclass(frozen=True)
class ReferenceAccountState:
    """Authoritative NON-RESERVED state for one account ledger (REFERENCE ONLY).

    `committed_open_risk_usd` and `committed_open_positions` are broker/committed
    exposure only and exclude reservations. When deriving the Day-3 snapshot the
    authority adds reservation RISK to open risk, and supplies reservation COUNT
    separately as reserved position slots. A reservation is never an open position.
    """

    profile_id: str
    account: AccountIdentity
    as_of: datetime
    balance: Decimal
    equity: Decimal
    realized_daily_pnl: Decimal
    committed_open_risk_usd: Decimal
    committed_open_positions: int
    account_halted: bool
    daily_halted: bool
    max_loss_reference_balance: Optional[Decimal]
    limits: RiskEvaluationLimits

    def __post_init__(self) -> None:
        if not isinstance(self.profile_id, str) or not self.profile_id.strip():
            raise ContractError("profile_id must be a non-empty string")
        if not isinstance(self.account, AccountIdentity):
            raise ContractError("account must be an AccountIdentity")
        if not isinstance(self.limits, RiskEvaluationLimits):
            raise ContractError("limits must be RiskEvaluationLimits")
        if self.limits.profile_id != self.profile_id:
            raise ContractError("limits.profile_id must equal profile_id")


@dataclass(frozen=True)
class ReferenceReservation:
    """Immutable ACTIVE reservation (REFERENCE ONLY). Not an order."""

    reservation_id: str
    child_trade_id: str
    thesis_id: str
    strategy_id: StrategyId
    profile_id: str
    account: AccountIdentity
    canonical_symbol: str
    side: Side
    risk_usd: Decimal
    source_policy_version: str
    created_at: datetime
    status: ReferenceReservationStatus
    authorizing_evaluation: RiskEvaluation
    request_fingerprint: tuple

    def __post_init__(self) -> None:
        ev = self.authorizing_evaluation
        if self.status is not ReferenceReservationStatus.ACTIVE:
            raise ContractError("reservation status must be ACTIVE")
        if not isinstance(ev, RiskEvaluation) or not ev.passed:
            raise ContractError("reservation requires a passing Day-3 evaluation")
        if (ev.child_trade_id, ev.profile_id, ev.account, ev.proposed_risk_usd) != \
                (self.child_trade_id, self.profile_id, self.account, self.risk_usd):
            raise ContractError("reservation does not match its authorizing evaluation")
        if not isinstance(self.risk_usd, Decimal) or not self.risk_usd.is_finite() or self.risk_usd <= 0:
            raise ContractError("risk_usd must be a finite positive Decimal")
        if not _aware(self.created_at) or ev.evaluated_at != self.created_at:
            raise ContractError("created_at must be aware and equal the evaluation time")


@dataclass(frozen=True)
class ReferenceLedger:
    account_state: ReferenceAccountState
    reservations: tuple[ReferenceReservation, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.reservations, tuple):
            raise ContractError("reservations must be a tuple")
        for r in self.reservations:
            if r.profile_id != self.account_state.profile_id or r.account != self.account_state.account:
                raise ContractError(f"reservation {r.reservation_id} belongs to another account")

    @property
    def profile_id(self) -> str:
        return self.account_state.profile_id

    @property
    def active_position_reservations(self) -> int:
        """Position slots held by ACTIVE reservations. Not open positions."""
        return len(self.reservations)


@dataclass(frozen=True)
class ReferenceAuthorityState:
    """Complete immutable authority state. Replaced as a whole on commit."""

    ledgers: tuple[ReferenceLedger, ...]

    def __post_init__(self) -> None:
        ids = [l.profile_id for l in self.ledgers]
        if ids != sorted(ids) or len(set(ids)) != len(ids):
            raise ContractError("ledgers must be unique and sorted by profile_id")
        accounts = [l.account_state.account for l in self.ledgers]
        if len(set(accounts)) != len(accounts):
            raise ContractError("an account identity may back only one ledger")
        child_ids = [r.child_trade_id for l in self.ledgers for r in l.reservations]
        if len(set(child_ids)) != len(child_ids):
            raise ContractError("child_trade_id must be unique across the authority")

    def ledger(self, profile_id: str) -> Optional[ReferenceLedger]:
        return next((l for l in self.ledgers if l.profile_id == profile_id), None)

    def reservation_for(self, child_trade_id: str) -> Optional[ReferenceReservation]:
        return next((r for l in self.ledgers for r in l.reservations if r.child_trade_id == child_trade_id), None)

    @property
    def reservations(self) -> tuple[ReferenceReservation, ...]:
        return tuple(r for l in self.ledgers for r in l.reservations)

    def with_ledger(self, ledger: ReferenceLedger) -> "ReferenceAuthorityState":
        others = [l for l in self.ledgers if l.profile_id != ledger.profile_id]
        return ReferenceAuthorityState(tuple(sorted(others + [ledger], key=lambda l: l.profile_id)))


@dataclass(frozen=True)
class AtomicAuthorizationResult:
    """Result of one atomic authorization attempt (REFERENCE ONLY).

    AUTHORIZED_RESERVED is still NOT an order. Invariants enforced at construction:
    AUTHORIZED_RESERVED <=> reservation present <=> authorized_at present;
    idempotent_replay only with AUTHORIZED_RESERVED; blocks never carry a reservation.
    """

    IS_ORDER: ClassVar[bool] = False
    GRANTS_PRODUCTION_AUTHORITY: ClassVar[bool] = False

    child_trade_id: str
    thesis_id: str
    strategy_id: StrategyId
    profile_id: str
    account: AccountIdentity
    canonical_symbol: str
    side: Side
    code: AtomicAuthorizationCode
    reason: str
    decided_at: Optional[datetime]
    authorized_at: Optional[datetime]
    risk_evaluation: Optional[RiskEvaluation]
    reservation: Optional[ReferenceReservation]
    idempotent_replay: bool
    source_policy_version: Optional[str]

    def __post_init__(self) -> None:
        if not isinstance(self.code, AtomicAuthorizationCode):
            raise ContractError("code must be an AtomicAuthorizationCode")
        if not isinstance(self.reason, str) or not self.reason.strip():
            raise ContractError("reason must be a non-empty string")
        if type(self.idempotent_replay) is not bool:
            raise ContractError("idempotent_replay must be a bool")
        authorized = self.code is A.AUTHORIZED_RESERVED
        if authorized != (self.reservation is not None) or authorized != (self.authorized_at is not None):
            raise ContractError("AUTHORIZED_RESERVED if and only if a reservation and authorized_at are present")
        if self.idempotent_replay and not authorized:
            raise ContractError("idempotent_replay requires AUTHORIZED_RESERVED")
        if self.decided_at is not None and not _aware(self.decided_at):
            raise ContractError("decided_at must be timezone-aware")
        if authorized:
            res, ev = self.reservation, self.risk_evaluation
            if res.child_trade_id != self.child_trade_id or res.profile_id != self.profile_id:
                raise ContractError("reservation does not belong to this result")
            if ev is not res.authorizing_evaluation or self.authorized_at != res.created_at:
                raise ContractError("authorized result must carry the reservation's authorizing evaluation")
        elif self.code in RISK_BLOCK_TO_AUTHORIZATION_CODE.values() and self.risk_evaluation is not None:
            if RISK_BLOCK_TO_AUTHORIZATION_CODE.get(self.risk_evaluation.code) is not self.code:
                raise ContractError("block code does not match the Day-3 evaluation evidence")

    @property
    def authorized(self) -> bool:
        return self.code is A.AUTHORIZED_RESERVED
