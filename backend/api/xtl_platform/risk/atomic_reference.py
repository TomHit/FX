"""ReferenceAtomicRiskAuthority - Day 4.

    REFERENCE ONLY - NON-PRODUCTION.
    PRODUCTION AUTHORITY = NONE. PRODUCTION RESERVATION = NONE.
    BROKER AUTHORITY = NONE. REDIS = NONE. AUTHORIZED_RESERVED IS NOT AN ORDER.

Proves the LOGICAL semantics of authorize_and_reserve() with an in-process
lock standing in for a future atomic transaction. It is not the distributed or
persistent design.

Atomic section (one lock covers all of it; there is no gap between the capacity
check and the reservation commit):

    LOCK
      0. read authority clock                   -> BLOCK_REFERENCE_EVALUATION_FAILURE
      1. validate request                       -> BLOCK_INVALID_RISK_INPUT
      2. idempotency on child_trade_id          -> exact replay: existing reservation
                                                   conflicting: DUPLICATE_TRADE_ID
      3. load current account ledger            -> BLOCK_ACCOUNT_MISMATCH
      4. policy precondition evidence           -> BLOCK_POLICY_PRECONDITION
      5. derive Day-3 snapshot (committed + reservations)
      6. re-run frozen Day-3 evaluate_risk      -> mapped Day-3 block code
      7. PASS: build reservation + new state; single commit
    UNLOCK

Day 3 owns risk calculation semantics. This module owns atomic authorization
semantics only and contains no loss or capacity formula.
"""
from __future__ import annotations

import threading
from datetime import datetime
from decimal import Decimal, localcontext
from typing import Callable, ClassVar, Optional

from ..domain.enums import PolicyDecisionCode
from ..domain.policy import PolicyDecision
from ..domain.thesis import ChildExecutionIntent
from .atomic_contracts import (
    RISK_BLOCK_TO_AUTHORIZATION_CODE,
    AtomicAuthorizationCode as A,
    AtomicAuthorizationRequest,
    AtomicAuthorizationResult,
    ReferenceAccountState,
    ReferenceAuthorityState,
    ReferenceLedger,
    ReferenceReservation,
    ReferenceReservationStatus,
)
from .calculator import RISK_DECIMAL_CONTEXT
from .contracts import (
    RiskEvaluation,
    RiskEvaluationCode,
    RiskEvaluationConfig,
    RiskEvaluationRequest,
    RiskEvaluationSnapshot,
)
from .evaluator import evaluate_risk
from .validation import RiskInputValidationError, config_errors, is_aware, is_finite_decimal

from ..domain._validation import ContractError

ACK_FLAG = "acknowledge_reference_only_non_production"


def authorization_request_errors(request: AtomicAuthorizationRequest) -> list[str]:
    errors: list[str] = []
    if not is_finite_decimal(request.proposed_risk_usd) or request.proposed_risk_usd <= 0:
        errors.append("request.proposed_risk_usd must be a finite Decimal > 0")
    if not is_aware(request.requested_at):
        errors.append("request.requested_at must be a timezone-aware datetime")
    if not isinstance(request.source_policy_version, str) or not request.source_policy_version.strip():
        errors.append("request.source_policy_version must be a non-empty string")
    if request.policy_decision is not None and not isinstance(request.policy_decision, PolicyDecision):
        errors.append("request.policy_decision must be None or a PolicyDecision")
    return errors


def policy_precondition_failure(request: AtomicAuthorizationRequest, state: ReferenceAccountState,
                                now: datetime) -> Optional[str]:
    """Evidence check only. Passing it never implies risk capacity."""
    d = request.policy_decision
    if d is None:
        return "no Day-2 policy decision evidence supplied"
    if d.code is not PolicyDecisionCode.ELIGIBLE:
        return f"policy decision is {d.code}, not ELIGIBLE"
    if d.child_trade_id != request.child_trade_id:
        return "policy decision belongs to a different child_trade_id"
    if d.policy_version != request.source_policy_version:
        return f"policy decision version {d.policy_version} != request version {request.source_policy_version}"
    if state.limits.source_policy_version != request.source_policy_version:
        return (f"authoritative limits derive from policy {state.limits.source_policy_version}, "
                f"request carries {request.source_policy_version}")
    if d.evaluated_at > now:
        return "policy decision is dated after the authorization time"
    return None


def account_state_errors(state: ReferenceAccountState) -> list[str]:
    """Only what snapshot derivation needs; Day 3 validates everything else."""
    errors: list[str] = []
    if not is_finite_decimal(state.committed_open_risk_usd):
        errors.append("ledger.committed_open_risk_usd must be a finite Decimal")
    if type(state.committed_open_positions) is not int:
        errors.append("ledger.committed_open_positions must be an int")
    return errors


def derive_evaluation_snapshot(ledger: ReferenceLedger) -> RiskEvaluationSnapshot:
    """Authoritative Day-3 snapshot for one account.

    open_risk_usd           = committed open risk + risk of ALL active reservations
    open_positions          = committed/broker open positions ONLY
    reserved_position_slots = number of ACTIVE reservations (slot commitments)

    Reservations of every strategy on this account are included; no other account's are.
    """
    s = ledger.account_state
    with localcontext(RISK_DECIMAL_CONTEXT):
        reserved = sum((r.risk_usd for r in ledger.reservations), Decimal("0"))
        open_risk = s.committed_open_risk_usd + reserved
    return RiskEvaluationSnapshot(
        profile_id=s.profile_id,
        account=s.account,
        as_of=s.as_of,
        balance=s.balance,
        equity=s.equity,
        realized_daily_pnl=s.realized_daily_pnl,
        open_risk_usd=open_risk,
        open_positions=s.committed_open_positions,
        reserved_position_slots=ledger.active_position_reservations,
        account_halted=s.account_halted,
        daily_halted=s.daily_halted,
        max_loss_reference_balance=s.max_loss_reference_balance,
    )


class ReferenceAtomicRiskAuthority:
    """REFERENCE ONLY - NON-PRODUCTION atomic risk authority.

    NOT production authority. Independent in-memory ledger per account; one
    shared capacity per account across all strategies. No release, no broker
    lifecycle, no persistence.
    """

    GRANTS_PRODUCTION_AUTHORITY: ClassVar[bool] = False
    AUTHORITY_CLASSIFICATION: ClassVar[str] = "REFERENCE_ONLY_NON_PRODUCTION"

    def __init__(self, config: RiskEvaluationConfig, clock: Callable[[], datetime], *,
                 acknowledge_reference_only_non_production: bool = False) -> None:
        if acknowledge_reference_only_non_production is not True:
            raise RuntimeError("ReferenceAtomicRiskAuthority is REFERENCE ONLY - NON-PRODUCTION; "
                               f"pass {ACK_FLAG}=True")
        if not isinstance(config, RiskEvaluationConfig):
            raise TypeError("config must be a RiskEvaluationConfig")
        errors = config_errors(config)
        if errors:
            raise RiskInputValidationError(errors)
        if not callable(clock):
            raise TypeError("clock must be callable")
        self._config = config
        self._clock = clock
        self._lock = threading.Lock()
        self._state = ReferenceAuthorityState(ledgers=())

    # ---- state ----------------------------------------------------------------

    def state(self) -> ReferenceAuthorityState:
        with self._lock:
            return self._state

    def _commit(self, new_state: ReferenceAuthorityState) -> None:
        if not self._lock.locked():
            raise RuntimeError("commit attempted outside the atomic section")
        self._state = new_state

    def register_account(self, account_state: ReferenceAccountState) -> None:
        if not isinstance(account_state, ReferenceAccountState):
            raise TypeError("account_state must be a ReferenceAccountState")
        with self._lock:
            if self._state.ledger(account_state.profile_id) is not None:
                raise ContractError(f"ledger {account_state.profile_id} already registered")
            self._commit(self._state.with_ledger(ReferenceLedger(account_state, ())))

    def replace_account_state(self, account_state: ReferenceAccountState) -> None:
        """Reference state feed: replace NON-RESERVED authoritative state.
        Reservations are carried over untouched; nothing is released."""
        if not isinstance(account_state, ReferenceAccountState):
            raise TypeError("account_state must be a ReferenceAccountState")
        with self._lock:
            current = self._state.ledger(account_state.profile_id)
            if current is None:
                raise ContractError(f"ledger {account_state.profile_id} is not registered")
            if current.account_state.account != account_state.account:
                raise ContractError("account identity of a ledger is frozen")
            self._commit(self._state.with_ledger(ReferenceLedger(account_state, current.reservations)))

    # ---- authorization ----------------------------------------------------------

    def authorize_and_reserve(self, request: AtomicAuthorizationRequest) -> AtomicAuthorizationResult:
        if not isinstance(request, AtomicAuthorizationRequest):
            raise TypeError("request must be an AtomicAuthorizationRequest")
        if not isinstance(request.intent, ChildExecutionIntent):
            raise TypeError("request.intent must be a ChildExecutionIntent")
        with self._lock:
            return self._authorize_locked(request)

    def _authorize_locked(self, request: AtomicAuthorizationRequest) -> AtomicAuthorizationResult:
        intent = request.intent
        state = self._state
        version = request.source_policy_version

        def result(code: A, reason: str, *, now: Optional[datetime] = None,
                   evaluation: Optional[RiskEvaluation] = None,
                   reservation: Optional[ReferenceReservation] = None,
                   replay: bool = False) -> AtomicAuthorizationResult:
            return AtomicAuthorizationResult(
                child_trade_id=intent.child_trade_id,
                thesis_id=intent.thesis_id,
                strategy_id=intent.strategy_id,
                profile_id=intent.profile_id,
                account=intent.account,
                canonical_symbol=intent.canonical_symbol,
                side=intent.side,
                code=code,
                reason=reason,
                decided_at=now,
                authorized_at=reservation.created_at if reservation is not None else None,
                risk_evaluation=evaluation,
                reservation=reservation,
                idempotent_replay=replay,
                source_policy_version=version if isinstance(version, str) and version.strip() else None,
            )

        # 0. authority clock (inside the lock)
        try:
            now = self._clock()
        except Exception as exc:  # noqa: BLE001 - fail closed
            return result(A.BLOCK_REFERENCE_EVALUATION_FAILURE, f"authority clock raised {type(exc).__name__}")
        if not is_aware(now):
            return result(A.BLOCK_REFERENCE_EVALUATION_FAILURE, "authority clock returned a non-aware value")

        # 1. request validity
        errors = authorization_request_errors(request)
        if errors:
            return result(A.BLOCK_INVALID_RISK_INPUT, "invalid authorization request: " + "; ".join(errors), now=now)

        # 2. idempotency - BEFORE any evaluation, so a replay never re-counts itself
        existing = state.reservation_for(intent.child_trade_id)
        if existing is not None:
            if existing.request_fingerprint == request.idempotency_fingerprint:
                return result(A.AUTHORIZED_RESERVED,
                              f"idempotent replay of reservation {existing.reservation_id}; nothing new reserved",
                              now=now, evaluation=existing.authorizing_evaluation, reservation=existing, replay=True)
            return result(A.DUPLICATE_TRADE_ID,
                          f"child_trade_id {intent.child_trade_id} already reserved by a materially different "
                          f"request ({existing.reservation_id}); existing reservation unchanged", now=now)

        # 3. current authoritative ledger
        ledger = state.ledger(intent.profile_id)
        if ledger is None:
            return result(A.BLOCK_ACCOUNT_MISMATCH, f"no reference ledger registered for profile {intent.profile_id}",
                          now=now)

        # 4. policy precondition (evidence only; never capacity proof)
        failure = policy_precondition_failure(request, ledger.account_state, now)
        if failure:
            return result(A.BLOCK_POLICY_PRECONDITION, failure, now=now)

        # 5-6. derive current snapshot and re-run the frozen Day-3 evaluator
        state_errors = account_state_errors(ledger.account_state)
        if state_errors:
            return result(A.BLOCK_INVALID_RISK_INPUT, "invalid ledger state: " + "; ".join(state_errors), now=now)
        try:
            snapshot = derive_evaluation_snapshot(ledger)
            evaluation = evaluate_risk(
                RiskEvaluationRequest(intent=intent, proposed_risk_usd=request.proposed_risk_usd,
                                      proposed_risk_pct=None, evaluated_at=now),
                snapshot, ledger.account_state.limits, self._config)
        except Exception as exc:  # noqa: BLE001 - never authorize on an exception
            return result(A.BLOCK_REFERENCE_EVALUATION_FAILURE,
                          f"Day-3 re-evaluation raised {type(exc).__name__}; failed closed, nothing reserved", now=now)
        if not isinstance(evaluation, RiskEvaluation):
            return result(A.BLOCK_REFERENCE_EVALUATION_FAILURE, "Day-3 re-evaluation returned no RiskEvaluation",
                          now=now)
        if evaluation.code is not RiskEvaluationCode.RISK_EVALUATION_PASS:
            return result(RISK_BLOCK_TO_AUTHORIZATION_CODE[evaluation.code],
                          f"atomic Day-3 re-evaluation: {evaluation.reason}", now=now, evaluation=evaluation)

        # 7. reserve and commit in the same atomic section
        try:
            reservation = ReferenceReservation(
                reservation_id=f"REF-RESV:{intent.child_trade_id}",
                child_trade_id=intent.child_trade_id,
                thesis_id=intent.thesis_id,
                strategy_id=intent.strategy_id,
                profile_id=intent.profile_id,
                account=intent.account,
                canonical_symbol=intent.canonical_symbol,
                side=intent.side,
                risk_usd=request.proposed_risk_usd,
                source_policy_version=request.source_policy_version,
                created_at=now,
                status=ReferenceReservationStatus.ACTIVE,
                authorizing_evaluation=evaluation,
                request_fingerprint=request.idempotency_fingerprint,
            )
            new_state = state.with_ledger(ReferenceLedger(ledger.account_state, ledger.reservations + (reservation,)))
        except Exception as exc:  # noqa: BLE001
            return result(A.BLOCK_REFERENCE_EVALUATION_FAILURE,
                          f"reservation build raised {type(exc).__name__}; nothing reserved", now=now)
        self._commit(new_state)
        return result(A.AUTHORIZED_RESERVED,
                      f"authorized and reserved {reservation.reservation_id} (REFERENCE ONLY; not an order)",
                      now=now, evaluation=evaluation, reservation=reservation)
