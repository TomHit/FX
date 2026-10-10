"""Day-5 reservation ownership lifecycle CONTRACTS.

REFERENCE ONLY - NON-PRODUCTION.
AUTHORIZED_RESERVED is NOT an order. COMMITTED_POSITION is a REFERENCE
ACCOUNTING STATE ONLY: it does NOT mean a broker position exists.

Core invariant: ONE TRADE -> ONE EXPOSURE OWNER.

    ACTIVE_RESERVATION --transfer--> COMMITTED_POSITION   (terminal in Day 5)
    ACTIVE_RESERVATION --release---> RELEASED             (terminal in Day 5)

For a child_trade_id, exactly one of these holds at every observable instant:
  * an ACTIVE ReferenceReservation is in its account ledger, or
  * a ReservationLifecycleRecord exists for it,
and never both. `ReferenceLifecycleState` enforces this at construction, so a
double-owned state cannot be built, let alone committed.

Reused frozen Day-4 types: ReferenceAuthorityState, ReferenceReservation,
ReferenceAccountState. No frozen type is modified.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import ClassVar, Optional

from ..domain._validation import ContractError
from ..domain.account import AccountIdentity
from ..domain.enums import Side, StrategyId, StrEnum
from .atomic_contracts import ReferenceAuthorityState, ReferenceReservation


class ExposureOwner(StrEnum):
    ACTIVE_RESERVATION = "ACTIVE_RESERVATION"
    COMMITTED_POSITION = "COMMITTED_POSITION"   # reference accounting state; NOT a broker position
    RELEASED = "RELEASED"


TERMINAL_OWNERS = frozenset({ExposureOwner.COMMITTED_POSITION, ExposureOwner.RELEASED})


class CommittedExposureReplacementBlocked(ContractError):
    """REFERENCE ONLY - Day-5 Correction-1.

    Raised when an account-state replacement is attempted for an account whose
    committed exposure is (partly) owned by COMMITTED_POSITION lifecycle records.
    The reference model cannot prove that a replacement preserves that exposure,
    so it fails closed and nothing is changed. Ownership reconciliation belongs
    to a later explicit phase.
    """

    def __init__(self, profile_id: str, transition_ids: tuple[str, ...]) -> None:
        self.profile_id = profile_id
        self.transition_ids = transition_ids
        super().__init__(
            f"REFERENCE ONLY: account-state replacement blocked for {profile_id}: committed exposure is owned by "
            f"COMMITTED_POSITION lifecycle records {list(transition_ids)}; the replacement cannot prove it "
            "preserves that exposure (no reconciliation in Day 5); state unchanged")


class LifecycleTransitionCode(StrEnum):
    TRANSITION_APPLIED = "TRANSITION_APPLIED"
    BLOCK_INVALID_TRANSITION_REQUEST = "BLOCK_INVALID_TRANSITION_REQUEST"
    BLOCK_UNKNOWN_RESERVATION = "BLOCK_UNKNOWN_RESERVATION"
    BLOCK_ACCOUNT_MISMATCH = "BLOCK_ACCOUNT_MISMATCH"
    BLOCK_INVALID_TRANSITION = "BLOCK_INVALID_TRANSITION"
    BLOCK_CONFLICTING_TRANSITION = "BLOCK_CONFLICTING_TRANSITION"
    BLOCK_REFERENCE_TRANSITION_FAILURE = "BLOCK_REFERENCE_TRANSITION_FAILURE"


def _aware(v: object) -> bool:
    return isinstance(v, datetime) and v.tzinfo is not None and v.utcoffset() is not None


@dataclass(frozen=True)
class LifecycleTransitionRequest:
    """Immutable request to move an ACTIVE reservation to a terminal owner.

    `transition_id` is the idempotency key of the transition itself. `requested_at`
    is audit only: the authority clock stamps the transition.
    """

    transition_id: str
    child_trade_id: str
    reservation_id: str
    profile_id: str
    account: AccountIdentity
    target_owner: ExposureOwner
    reason: str
    requested_at: datetime

    @property
    def fingerprint(self) -> tuple:
        """Material identity. Excludes requested_at so an exact retry is a replay."""
        return (self.transition_id, self.child_trade_id, self.reservation_id, self.profile_id,
                self.account, self.target_owner, self.reason)


@dataclass(frozen=True)
class ReservationLifecycleRecord:
    """Immutable audit record of one applied transition.

    `reservation` is the frozen historical authorization (created_at, thesis,
    strategy, profile, account, symbol, side, authorized risk, policy version,
    authorizing Day-3 evaluation). Its own `status` field records the state at
    authorization; current ownership is `to_owner`.
    """

    transition_id: str
    reservation: ReferenceReservation
    from_owner: ExposureOwner
    to_owner: ExposureOwner
    transitioned_at: datetime
    reason: str
    request_fingerprint: tuple
    transferred_risk_usd: Decimal
    transferred_position_slots: int

    def __post_init__(self) -> None:
        if not isinstance(self.reservation, ReferenceReservation):
            raise ContractError("reservation must be a ReferenceReservation")
        if self.from_owner is not ExposureOwner.ACTIVE_RESERVATION:
            raise ContractError("Day 5 transitions start from ACTIVE_RESERVATION only")
        if self.to_owner not in TERMINAL_OWNERS:
            raise ContractError("to_owner must be COMMITTED_POSITION or RELEASED")
        if not isinstance(self.transition_id, str) or not self.transition_id.strip():
            raise ContractError("transition_id must be a non-empty string")
        if not isinstance(self.reason, str) or not self.reason.strip():
            raise ContractError("reason must be a non-empty string")
        if not _aware(self.transitioned_at) or self.transitioned_at < self.reservation.created_at:
            raise ContractError("transitioned_at must be aware and not before the reservation was created")
        if self.to_owner is ExposureOwner.COMMITTED_POSITION:
            expected = (self.reservation.risk_usd, 1)
        else:
            expected = (Decimal("0"), 0)
        if (self.transferred_risk_usd, self.transferred_position_slots) != expected:
            raise ContractError("transferred exposure must equal the reservation (COMMITTED) or zero (RELEASED)")
        if (not isinstance(self.request_fingerprint, tuple) or len(self.request_fingerprint) != 7
                or self.request_fingerprint[0] != self.transition_id
                or self.request_fingerprint[1] != self.reservation.child_trade_id
                or self.request_fingerprint[5] is not self.to_owner):
            raise ContractError("request_fingerprint does not describe this transition")

    @property
    def child_trade_id(self) -> str:
        return self.reservation.child_trade_id

    @property
    def reservation_id(self) -> str:
        return self.reservation.reservation_id

    @property
    def profile_id(self) -> str:
        return self.reservation.profile_id

    @property
    def account(self) -> AccountIdentity:
        return self.reservation.account

    @property
    def thesis_id(self) -> str:
        return self.reservation.thesis_id

    @property
    def strategy_id(self) -> StrategyId:
        return self.reservation.strategy_id

    @property
    def canonical_symbol(self) -> str:
        return self.reservation.canonical_symbol

    @property
    def side(self) -> Side:
        return self.reservation.side

    @property
    def authorized_risk_usd(self) -> Decimal:
        return self.reservation.risk_usd

    @property
    def source_policy_version(self) -> str:
        return self.reservation.source_policy_version

    @property
    def reservation_created_at(self) -> datetime:
        return self.reservation.created_at


@dataclass(frozen=True)
class ReferenceLifecycleState:
    """Complete immutable Day-5 authority state, replaced as a whole on every commit.

    Enforced at construction (single-owner invariant):
      * each child_trade_id has at most one lifecycle record;
      * each transition_id is used by at most one record;
      * no child_trade_id is both an ACTIVE reservation and a record;
      * every record belongs to a registered account ledger.
    """

    authority: ReferenceAuthorityState
    records: tuple[ReservationLifecycleRecord, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.authority, ReferenceAuthorityState):
            raise ContractError("authority must be a ReferenceAuthorityState")
        if not isinstance(self.records, tuple):
            raise ContractError("records must be a tuple")
        child_ids = [r.child_trade_id for r in self.records]
        if len(set(child_ids)) != len(child_ids):
            raise ContractError("a child_trade_id may have only one lifecycle record")
        transition_ids = [r.transition_id for r in self.records]
        if len(set(transition_ids)) != len(transition_ids):
            raise ContractError("a transition_id may be used only once")
        active = {r.child_trade_id for r in self.authority.reservations}
        double = active & set(child_ids)
        if double:
            raise ContractError(f"single-owner invariant violated for {sorted(double)}")
        for r in self.records:
            ledger = self.authority.ledger(r.profile_id)
            if ledger is None or ledger.account_state.account != r.account:
                raise ContractError(f"record {r.transition_id} does not belong to a registered account")

    def record_for(self, child_trade_id: str) -> Optional[ReservationLifecycleRecord]:
        return next((r for r in self.records if r.child_trade_id == child_trade_id), None)

    def record_for_transition(self, transition_id: str) -> Optional[ReservationLifecycleRecord]:
        return next((r for r in self.records if r.transition_id == transition_id), None)

    def committed_records_for(self, profile_id: str) -> tuple[ReservationLifecycleRecord, ...]:
        """COMMITTED_POSITION records owning exposure on this account. RELEASED owns none."""
        return tuple(r for r in self.records
                     if r.profile_id == profile_id and r.to_owner is ExposureOwner.COMMITTED_POSITION)

    def owner_of(self, child_trade_id: str) -> Optional[ExposureOwner]:
        if self.authority.reservation_for(child_trade_id) is not None:
            return ExposureOwner.ACTIVE_RESERVATION
        record = self.record_for(child_trade_id)
        return record.to_owner if record is not None else None


@dataclass(frozen=True)
class LifecycleTransitionResult:
    """Result of one transition attempt (REFERENCE ONLY).

    TRANSITION_APPLIED <=> record present. idempotent_replay only with APPLIED.
    For blocks, resulting_owner == previous_owner (nothing changed).
    """

    IS_ORDER: ClassVar[bool] = False
    GRANTS_PRODUCTION_AUTHORITY: ClassVar[bool] = False

    transition_id: str
    child_trade_id: str
    target_owner: ExposureOwner
    code: LifecycleTransitionCode
    reason: str
    decided_at: Optional[datetime]
    previous_owner: Optional[ExposureOwner]
    resulting_owner: Optional[ExposureOwner]
    record: Optional[ReservationLifecycleRecord]
    idempotent_replay: bool

    def __post_init__(self) -> None:
        if not isinstance(self.code, LifecycleTransitionCode):
            raise ContractError("code must be a LifecycleTransitionCode")
        if not isinstance(self.reason, str) or not self.reason.strip():
            raise ContractError("reason must be a non-empty string")
        if type(self.idempotent_replay) is not bool:
            raise ContractError("idempotent_replay must be a bool")
        if self.decided_at is not None and not _aware(self.decided_at):
            raise ContractError("decided_at must be timezone-aware")
        applied = self.code is LifecycleTransitionCode.TRANSITION_APPLIED
        if applied != (self.record is not None):
            raise ContractError("TRANSITION_APPLIED if and only if a lifecycle record is present")
        if self.idempotent_replay and not applied:
            raise ContractError("idempotent_replay requires TRANSITION_APPLIED")
        if applied:
            r = self.record
            if (r.transition_id, r.child_trade_id, r.to_owner) != (self.transition_id, self.child_trade_id,
                                                                   self.target_owner):
                raise ContractError("record does not match this result")
            if self.previous_owner is not r.from_owner or self.resulting_owner is not r.to_owner:
                raise ContractError("applied result owners must come from the record")
        elif self.resulting_owner is not self.previous_owner:
            raise ContractError("a blocked transition must leave ownership unchanged")

    @property
    def applied(self) -> bool:
        return self.code is LifecycleTransitionCode.TRANSITION_APPLIED
