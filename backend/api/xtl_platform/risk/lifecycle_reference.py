"""ReferenceLifecycleRiskAuthority - Day 5.

    REFERENCE ONLY - NON-PRODUCTION.
    PRODUCTION AUTHORITY = NONE. BROKER AUTHORITY = NONE. REDIS = NONE.
    AUTHORIZED_RESERVED IS NOT AN ORDER.
    COMMITTED_POSITION IS A REFERENCE ACCOUNTING STATE, NOT A BROKER POSITION.

Extends the frozen Day-4 ReferenceAtomicRiskAuthority WITHOUT modifying it.

State model: ONE immutable ReferenceLifecycleState (Day-4 authority state +
lifecycle records) held in a single attribute. Frozen Day-4 code reads and
writes its authority part through `_state`; every Day-4 commit and every Day-5
transition therefore replaces the one combined object in a single assignment
under the same lock.

Transition atomic section:

    LOCK
      0. authority clock                         -> BLOCK_REFERENCE_TRANSITION_FAILURE
      1. validate request                        -> BLOCK_INVALID_TRANSITION_REQUEST
                                                    (target ACTIVE_RESERVATION -> BLOCK_INVALID_TRANSITION)
      2. transition_id reused for another child  -> BLOCK_CONFLICTING_TRANSITION
      3. existing record for child_trade_id:
           identity differs                      -> BLOCK_ACCOUNT_MISMATCH
           exact same transition                 -> TRANSITION_APPLIED (idempotent_replay)
           different terminal target             -> BLOCK_INVALID_TRANSITION
           same target, different transition     -> BLOCK_CONFLICTING_TRANSITION
      4. ACTIVE reservation lookup               -> BLOCK_UNKNOWN_RESERVATION / BLOCK_ACCOUNT_MISMATCH
      5. build complete new state (ownership move, record)   exception -> BLOCK_REFERENCE_TRANSITION_FAILURE
      6. single commit
    UNLOCK

Ownership move (no risk formula; Day 3 still owns capacity semantics):
    COMMITTED_POSITION: remove reservation; committed_open_risk += risk;
                        committed_open_positions += 1
    RELEASED:           remove reservation; committed state unchanged

Transitions do not re-evaluate risk: a transfer does not change exposure, and a
release only removes it.

Correction-1 (state feed guard): replace_account_state fails closed with
CommittedExposureReplacementBlocked for any account that has one or more
COMMITTED_POSITION lifecycle records, whatever the incoming committed values
(lower, equal or higher). Ownership is never inferred from equality. Accounts
with no COMMITTED_POSITION records (none, ACTIVE only, or RELEASED only) keep
the exact Day-4 behaviour. The guard and the replacement share ONE locked
section, so a concurrent transfer cannot slip between check and commit.
"""
from __future__ import annotations

import dataclasses
from datetime import datetime
from decimal import Decimal, localcontext
from typing import Callable, ClassVar, Optional

from ..domain._validation import ContractError
from ..domain.account import AccountIdentity
from ..domain.thesis import ChildExecutionIntent
from .atomic_contracts import (
    AtomicAuthorizationCode,
    AtomicAuthorizationRequest,
    AtomicAuthorizationResult,
    ReferenceAccountState,
    ReferenceAuthorityState,
    ReferenceLedger,
)
from .atomic_reference import ReferenceAtomicRiskAuthority
from .calculator import RISK_DECIMAL_CONTEXT
from .contracts import RiskEvaluationConfig
from .lifecycle_contracts import (
    TERMINAL_OWNERS,
    CommittedExposureReplacementBlocked,
    ExposureOwner,
    LifecycleTransitionCode as T,
    LifecycleTransitionRequest,
    LifecycleTransitionResult,
    ReferenceLifecycleState,
    ReservationLifecycleRecord,
)
from .validation import is_aware, is_finite_decimal


def transition_request_errors(request: LifecycleTransitionRequest) -> list[str]:
    errors: list[str] = []
    for name in ("transition_id", "child_trade_id", "reservation_id", "profile_id", "reason"):
        v = getattr(request, name)
        if not isinstance(v, str) or not v.strip():
            errors.append(f"request.{name} must be a non-empty string")
    if not isinstance(request.account, AccountIdentity):
        errors.append("request.account must be an AccountIdentity")
    if not isinstance(request.target_owner, ExposureOwner):
        errors.append("request.target_owner must be an ExposureOwner")
    if not is_aware(request.requested_at):
        errors.append("request.requested_at must be a timezone-aware datetime")
    return errors


class ReferenceLifecycleRiskAuthority(ReferenceAtomicRiskAuthority):
    """REFERENCE ONLY - NON-PRODUCTION reservation ownership lifecycle authority.

    NOT production authority. Adds ACTIVE_RESERVATION -> COMMITTED_POSITION and
    ACTIVE_RESERVATION -> RELEASED reference transitions to the frozen Day-4
    authority. No close, no PnL settlement, no broker truth, no reconciliation.
    """

    GRANTS_PRODUCTION_AUTHORITY: ClassVar[bool] = False
    AUTHORITY_CLASSIFICATION: ClassVar[str] = "REFERENCE_ONLY_NON_PRODUCTION"

    def __init__(self, config: RiskEvaluationConfig, clock: Callable[[], datetime], *,
                 acknowledge_reference_only_non_production: bool = False) -> None:
        super().__init__(config, clock,
                         acknowledge_reference_only_non_production=acknowledge_reference_only_non_production)

    # ---- single combined state --------------------------------------------------

    @property
    def _state(self) -> ReferenceAuthorityState:
        return self._lifecycle.authority

    @_state.setter
    def _state(self, authority: ReferenceAuthorityState) -> None:
        current = self.__dict__.get("_lifecycle")
        records = current.records if current is not None else ()
        self._lifecycle = ReferenceLifecycleState(authority, records)

    def lifecycle_state(self) -> ReferenceLifecycleState:
        with self._lock:
            return self._lifecycle

    def _commit_lifecycle(self, new_state: ReferenceLifecycleState) -> None:
        if not self._lock.locked():
            raise RuntimeError("lifecycle commit attempted outside the atomic section")
        self._lifecycle = new_state

    # ---- state feed guard (Correction-1) ---------------------------------------------

    def replace_account_state(self, account_state: ReferenceAccountState) -> None:
        """Day-4 reference state feed, guarded for lifecycle-owned committed exposure.

        Same checks, errors and effect as the frozen Day-4 method, in the same order,
        plus: if the account has any COMMITTED_POSITION lifecycle record, raise
        CommittedExposureReplacementBlocked and change nothing. The inherited method
        cannot be called here: its lock is not re-entrant, and releasing the lock
        between guard and replacement would allow a concurrent transfer in between.
        """
        if not isinstance(account_state, ReferenceAccountState):
            raise TypeError("account_state must be a ReferenceAccountState")
        with self._lock:
            state = self._lifecycle
            current = state.authority.ledger(account_state.profile_id)
            if current is None:
                raise ContractError(f"ledger {account_state.profile_id} is not registered")
            if current.account_state.account != account_state.account:
                raise ContractError("account identity of a ledger is frozen")
            committed = state.committed_records_for(account_state.profile_id)
            if committed:
                raise CommittedExposureReplacementBlocked(account_state.profile_id,
                                                          tuple(r.transition_id for r in committed))
            self._commit(state.authority.with_ledger(ReferenceLedger(account_state, current.reservations)))

    # ---- authorization: history is checked before the frozen Day-4 path ----------

    def _authorize_locked(self, request: AtomicAuthorizationRequest) -> AtomicAuthorizationResult:
        intent: ChildExecutionIntent = request.intent
        record = self._lifecycle.record_for(intent.child_trade_id)
        if record is None:
            return super()._authorize_locked(request)
        try:
            now: Optional[datetime] = self._clock()
        except Exception:  # noqa: BLE001 - decision time unknown; still fail closed
            now = None
        if not is_aware(now):
            now = None
        version = request.source_policy_version
        return AtomicAuthorizationResult(
            child_trade_id=intent.child_trade_id, thesis_id=intent.thesis_id, strategy_id=intent.strategy_id,
            profile_id=intent.profile_id, account=intent.account, canonical_symbol=intent.canonical_symbol,
            side=intent.side, code=AtomicAuthorizationCode.DUPLICATE_TRADE_ID,
            reason=(f"child_trade_id {intent.child_trade_id} is no longer an ACTIVE reservation; lifecycle owner is "
                    f"{record.to_owner} (transition {record.transition_id}); no new authority granted"),
            decided_at=now, authorized_at=None, risk_evaluation=None, reservation=None, idempotent_replay=False,
            source_policy_version=version if isinstance(version, str) and version.strip() else None,
        )

    # ---- lifecycle transition -------------------------------------------------------

    def reference_transition(self, request: LifecycleTransitionRequest) -> LifecycleTransitionResult:
        if not isinstance(request, LifecycleTransitionRequest):
            raise TypeError("request must be a LifecycleTransitionRequest")
        with self._lock:
            return self._transition_locked(request)

    def _transition_locked(self, request: LifecycleTransitionRequest) -> LifecycleTransitionResult:
        state = self._lifecycle
        child_id = request.child_trade_id if isinstance(request.child_trade_id, str) else ""
        current_owner = state.owner_of(child_id) if child_id else None

        def result(code: T, reason: str, *, now: Optional[datetime] = None,
                   record: Optional[ReservationLifecycleRecord] = None, replay: bool = False) -> LifecycleTransitionResult:
            return LifecycleTransitionResult(
                transition_id=request.transition_id if isinstance(request.transition_id, str) else "",
                child_trade_id=child_id,
                target_owner=request.target_owner,
                code=code,
                reason=reason,
                decided_at=now,
                previous_owner=record.from_owner if record is not None else current_owner,
                resulting_owner=record.to_owner if record is not None else current_owner,
                record=record,
                idempotent_replay=replay,
            )

        # 0. clock
        try:
            now = self._clock()
        except Exception as exc:  # noqa: BLE001
            return result(T.BLOCK_REFERENCE_TRANSITION_FAILURE, f"authority clock raised {type(exc).__name__}")
        if not is_aware(now):
            return result(T.BLOCK_REFERENCE_TRANSITION_FAILURE, "authority clock returned a non-aware value")

        # 1. request validity
        errors = transition_request_errors(request)
        if errors:
            return result(T.BLOCK_INVALID_TRANSITION_REQUEST, "invalid transition request: " + "; ".join(errors),
                          now=now)
        if request.target_owner not in TERMINAL_OWNERS:
            return result(T.BLOCK_INVALID_TRANSITION,
                          f"target {request.target_owner} is not a Day-5 terminal owner", now=now)

        # 2. transition_id may identify only one child transition
        by_tid = state.record_for_transition(request.transition_id)
        if by_tid is not None and by_tid.child_trade_id != request.child_trade_id:
            return result(T.BLOCK_CONFLICTING_TRANSITION,
                          f"transition_id {request.transition_id} already used for {by_tid.child_trade_id}", now=now)

        # 3. child already transitioned
        existing = state.record_for(request.child_trade_id)
        if existing is not None:
            if (existing.profile_id, existing.account, existing.reservation_id) != \
                    (request.profile_id, request.account, request.reservation_id):
                return result(T.BLOCK_ACCOUNT_MISMATCH,
                              "request identity does not match the transitioned reservation", now=now)
            if existing.request_fingerprint == request.fingerprint:
                return result(T.TRANSITION_APPLIED,
                              f"idempotent replay of {existing.transition_id}; ownership unchanged",
                              now=now, record=existing, replay=True)
            if existing.to_owner is not request.target_owner:
                return result(T.BLOCK_INVALID_TRANSITION,
                              f"{existing.to_owner} -> {request.target_owner} is not permitted in Day 5", now=now)
            return result(T.BLOCK_CONFLICTING_TRANSITION,
                          f"{request.child_trade_id} already moved to {existing.to_owner} by a different "
                          f"transition ({existing.transition_id})", now=now)

        # 4. ACTIVE reservation
        reservation = state.authority.reservation_for(request.child_trade_id)
        if reservation is None or reservation.reservation_id != request.reservation_id:
            return result(T.BLOCK_UNKNOWN_RESERVATION,
                          f"no ACTIVE reservation {request.reservation_id} for {request.child_trade_id}", now=now)
        if (reservation.profile_id, reservation.account) != (request.profile_id, request.account):
            return result(T.BLOCK_ACCOUNT_MISMATCH, "request profile/account does not match the reservation", now=now)

        # 5. build the complete new state; nothing is visible until the commit
        try:
            ledger = state.authority.ledger(reservation.profile_id)
            remaining = tuple(r for r in ledger.reservations if r.child_trade_id != reservation.child_trade_id)
            account_state = ledger.account_state
            if request.target_owner is ExposureOwner.COMMITTED_POSITION:
                if not is_finite_decimal(account_state.committed_open_risk_usd) or \
                        type(account_state.committed_open_positions) is not int:
                    raise ValueError("ledger committed exposure is not valid")
                with localcontext(RISK_DECIMAL_CONTEXT):
                    committed_risk = account_state.committed_open_risk_usd + reservation.risk_usd
                account_state = dataclasses.replace(
                    account_state,
                    committed_open_risk_usd=committed_risk,
                    committed_open_positions=account_state.committed_open_positions + 1,
                )
                transferred = (reservation.risk_usd, 1)
            else:
                transferred = (Decimal("0"), 0)
            record = ReservationLifecycleRecord(
                transition_id=request.transition_id,
                reservation=reservation,
                from_owner=ExposureOwner.ACTIVE_RESERVATION,
                to_owner=request.target_owner,
                transitioned_at=now,
                reason=request.reason,
                request_fingerprint=request.fingerprint,
                transferred_risk_usd=transferred[0],
                transferred_position_slots=transferred[1],
            )
            new_state = ReferenceLifecycleState(
                authority=state.authority.with_ledger(ReferenceLedger(account_state, remaining)),
                records=state.records + (record,),
            )
        except Exception as exc:  # noqa: BLE001 - never partially transition
            return result(T.BLOCK_REFERENCE_TRANSITION_FAILURE,
                          f"transition build raised {type(exc).__name__}; state unchanged", now=now)

        # 6. single commit
        self._commit_lifecycle(new_state)
        return result(T.TRANSITION_APPLIED,
                      f"{reservation.reservation_id}: ACTIVE_RESERVATION -> {request.target_owner} (REFERENCE ONLY)",
                      now=now, record=record)
