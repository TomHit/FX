"""Day-9 Durable Account-Level Risk Authority - REFERENCE ONLY - NON-PRODUCTION.

    EXECUTION AUTHORITY = FALSE. BROKER AUTHORITY = FALSE.
    No orders, no positions, no market data, no broker symbols, no lot/contract
    sizing, no prop-firm rules, no daily reset, no cross-account allocation.
    Normalized risk only: reusable by CFD and futures paths alike.

Canonical operation, entirely inside ONE durable transaction per account:

    risk request -> durable account state (revision N) -> frozen policy/capacity
    decision -> atomic AUTHORIZE + RESERVE -> durable revision N+1 (compare-and-set)

This removes the check-then-reserve race: the decision reads the state the write is
conditioned on, and competing writers are serialized by the transaction, so the
loser re-decides against the winner's committed capacity instead of stale capacity.

No duplicated rules. The decision is made by the FROZEN authority stack replayed on
exactly the durable state just read:
  * Day-6 codec decodes the document (fail closed on corrupt/unsupported state)
  * a shadow ReferencePersistentLifecycleAuthority is restored from that state into
    an ephemeral in-memory repository and asked to decide
  * its resulting ledger/records are re-encoded at revision N+1 and written under CAS
Nothing is written unless the frozen authority actually authorized a NEW reservation.

Determinism: the decision time is supplied by the caller (`at`); no wall clock, no RNG
and no generated identity. Reservation identity comes from the frozen contracts
(`child_trade_id`, `REF-RESV:<child_trade_id>`).
"""
from __future__ import annotations

from datetime import datetime
from typing import Callable, ClassVar, Optional

from ..domain._validation import ContractError
from .atomic_contracts import AtomicAuthorizationRequest, ReferenceAccountState, ReferenceLedger
from .contracts import RiskEvaluationConfig
from .durable_authority_contracts import (
    DurableAuthorizationOutcome,
    DurableOutcomeCode as C,
    DurableTransitionOutcome,
    TransactionalLifecycleStateRepository,
)
from .lifecycle_contracts import LifecycleTransitionRequest, ReferenceLifecycleState
from .persistence_codec import DecodedAccount, decode_account_document, encode_account_document
from .persistence_contracts import PersistenceDecodeError, RepositoryConflictError, StoredDocument
from .persistence_repository import InMemoryReferenceRepository
from .persistent_reference import ReferencePersistentLifecycleAuthority
from .validation import is_aware

ACK_FLAG = "acknowledge_reference_only_non_production"


class DurableAccountRiskAuthority:
    """REFERENCE ONLY - NON-PRODUCTION durable per-account risk authority."""

    EXECUTION_AUTHORITY: ClassVar[bool] = False
    BROKER_AUTHORITY: ClassVar[bool] = False
    GRANTS_PRODUCTION_AUTHORITY: ClassVar[bool] = False
    AUTHORITY_CLASSIFICATION: ClassVar[str] = "REFERENCE_ONLY_NON_PRODUCTION"

    def __init__(self, config: RiskEvaluationConfig, repository: TransactionalLifecycleStateRepository, *,
                 acknowledge_reference_only_non_production: bool = False) -> None:
        if acknowledge_reference_only_non_production is not True:
            raise RuntimeError(f"DurableAccountRiskAuthority is REFERENCE ONLY - NON-PRODUCTION; pass {ACK_FLAG}=True")
        if not isinstance(config, RiskEvaluationConfig):
            raise TypeError("config must be a RiskEvaluationConfig")
        if not isinstance(repository, TransactionalLifecycleStateRepository):
            raise TypeError("repository must be a TransactionalLifecycleStateRepository")
        self._config = config
        self._repository = repository

    # ---- read-only durable view ----------------------------------------------------------

    def account_state(self, profile_id: str) -> Optional[ReferenceLifecycleState]:
        document = self._repository.load(profile_id)
        if document is None:
            return None
        decoded = decode_account_document(document)
        return self._state_of(decoded)

    def durable_revision(self, profile_id: str) -> Optional[int]:
        document = self._repository.load(profile_id)
        return None if document is None else document.revision

    # ---- registration ------------------------------------------------------------------------

    def register_account(self, account_state: ReferenceAccountState) -> int:
        """Durably create an account at revision 1. Fails closed if it already exists."""
        if not isinstance(account_state, ReferenceAccountState):
            raise TypeError("account_state must be a ReferenceAccountState")
        with self._repository.authorization_transaction(account_state.profile_id) as transaction:
            if transaction.load() is not None or transaction.problem() is not None:
                raise ContractError(f"{account_state.profile_id} already has durable state")
            document = encode_account_document(ReferenceLedger(account_state, ()), (), 1)
            transaction.write(document, None)
            return document.revision

    # ---- atomic authorize + reserve -------------------------------------------------------------

    def authorize_and_reserve(self, request: AtomicAuthorizationRequest, *,
                              at: datetime) -> DurableAuthorizationOutcome:
        if not isinstance(request, AtomicAuthorizationRequest):
            raise TypeError("request must be an AtomicAuthorizationRequest")
        if not is_aware(at):
            raise ContractError("at must be a timezone-aware decision time supplied by the caller")
        profile_id, child_trade_id = request.profile_id, request.child_trade_id

        def outcome(code: C, reason: str, **kw) -> DurableAuthorizationOutcome:
            return DurableAuthorizationOutcome(code=code, profile_id=profile_id, child_trade_id=child_trade_id,
                                               reason=reason, authorization=kw.get("authorization"),
                                               reservation=kw.get("reservation"),
                                               durable_revision=kw.get("durable_revision"),
                                               rejection_errors=kw.get("errors", ()))

        with self._repository.authorization_transaction(profile_id) as transaction:
            loaded = self._load_state(transaction, profile_id)
            if isinstance(loaded, DurableAuthorizationOutcome):
                return loaded
            document, decoded = loaded
            shadow = self._shadow(decoded, at)
            result = shadow.authorize_and_reserve(request)

            if result.authorized and not result.idempotent_replay:
                new_document = self._encode_next(shadow, decoded, profile_id, document.revision + 1)
                try:
                    transaction.write(new_document, document.revision)
                except RepositoryConflictError as exc:
                    return outcome(C.REVISION_CONFLICT, f"compare-and-set lost; nothing reserved ({exc})")
                return outcome(C.PERSISTED, f"authorized and durably reserved at revision {new_document.revision}",
                               authorization=result, reservation=result.reservation,
                               durable_revision=new_document.revision)
            if result.idempotent_replay:
                return outcome(C.IDEMPOTENT_REPLAY, "already reserved durably; nothing written",
                               authorization=result, reservation=result.reservation,
                               durable_revision=document.revision)
            return outcome(C.BLOCKED, f"frozen authority refused: {result.reason}", authorization=result,
                           durable_revision=document.revision)

    # ---- durable lifecycle transition (transfer / release) ---------------------------------------

    def apply_transition(self, request: LifecycleTransitionRequest, *, at: datetime) -> DurableTransitionOutcome:
        if not isinstance(request, LifecycleTransitionRequest):
            raise TypeError("request must be a LifecycleTransitionRequest")
        if not is_aware(at):
            raise ContractError("at must be a timezone-aware decision time supplied by the caller")
        profile_id, child_trade_id = request.profile_id, request.child_trade_id

        def outcome(code: C, reason: str, **kw) -> DurableTransitionOutcome:
            return DurableTransitionOutcome(code=code, profile_id=profile_id, child_trade_id=child_trade_id,
                                            reason=reason, transition=kw.get("transition"), record=kw.get("record"),
                                            durable_revision=kw.get("durable_revision"),
                                            rejection_errors=kw.get("errors", ()))

        with self._repository.authorization_transaction(profile_id) as transaction:
            loaded = self._load_state(transaction, profile_id, transition=True)
            if isinstance(loaded, DurableTransitionOutcome):
                return loaded
            document, decoded = loaded
            shadow = self._shadow(decoded, at)
            result = shadow.reference_transition(request)

            if result.applied and not result.idempotent_replay:
                new_document = self._encode_next(shadow, decoded, profile_id, document.revision + 1)
                try:
                    transaction.write(new_document, document.revision)
                except RepositoryConflictError as exc:
                    return outcome(C.REVISION_CONFLICT, f"compare-and-set lost; ownership unchanged ({exc})")
                return outcome(C.PERSISTED, f"{result.reason}; durable revision {new_document.revision}",
                               transition=result, record=result.record, durable_revision=new_document.revision)
            if result.idempotent_replay:
                return outcome(C.IDEMPOTENT_REPLAY, "transition already durable; nothing written",
                               transition=result, record=result.record, durable_revision=document.revision)
            return outcome(C.BLOCKED, f"frozen lifecycle refused: {result.reason}", transition=result,
                           durable_revision=document.revision)

    # ---- internals ---------------------------------------------------------------------------------

    def _load_state(self, transaction, profile_id: str, transition: bool = False):
        build = (lambda code, reason, errors=(): DurableTransitionOutcome(
            code=code, profile_id=profile_id, child_trade_id="", reason=reason, transition=None, record=None,
            durable_revision=None, rejection_errors=errors)) if transition else (
            lambda code, reason, errors=(): DurableAuthorizationOutcome(
                code=code, profile_id=profile_id, child_trade_id="", reason=reason, authorization=None,
                reservation=None, durable_revision=None, rejection_errors=errors))
        document = transaction.load()
        if document is None:
            problem = transaction.problem()
            if problem is not None:
                return build(C.DURABLE_STATE_REJECTED, f"durable row unreadable: {problem}", (problem,))
            return build(C.ACCOUNT_NOT_REGISTERED, f"no durable state for account {profile_id}")
        try:
            decoded = decode_account_document(document)
        except PersistenceDecodeError as exc:
            return build(C.DURABLE_STATE_REJECTED, f"durable document rejected: {exc.code}", exc.errors)
        return document, decoded

    def _state_of(self, decoded: DecodedAccount) -> ReferenceLifecycleState:
        from .atomic_contracts import ReferenceAuthorityState
        return ReferenceLifecycleState(ReferenceAuthorityState((decoded.ledger,)),
                                       tuple(record for _, record in decoded.records))

    def _shadow(self, decoded: DecodedAccount, at: datetime) -> ReferencePersistentLifecycleAuthority:
        """The frozen authority, restored from exactly the durable state just read."""
        ephemeral = InMemoryReferenceRepository()
        ephemeral.save(encode_account_document(decoded.ledger, decoded.records, 1), None)
        shadow, report = ReferencePersistentLifecycleAuthority.restore(
            self._config, self._fixed_clock(at), ephemeral, acknowledge_reference_only_non_production=True)
        if report.rejections:
            raise ContractError(f"durable state could not be replayed: {report.rejections}")
        return shadow

    @staticmethod
    def _fixed_clock(at: datetime) -> Callable[[], datetime]:
        return lambda: at

    @staticmethod
    def _encode_next(shadow: ReferencePersistentLifecycleAuthority, decoded: DecodedAccount, profile_id: str,
                     revision: int) -> StoredDocument:
        """Re-encode the frozen authority's resulting state at the next durable revision.
        Record sequences are preserved; new records continue the sequence deterministically."""
        state = shadow.lifecycle_state()
        sequences = {record.child_trade_id: sequence for sequence, record in decoded.records}
        next_sequence = max(sequences.values(), default=-1) + 1
        ordered = []
        for record in state.records:
            if record.profile_id != profile_id:
                continue
            sequence = sequences.get(record.child_trade_id)
            if sequence is None:
                sequence = next_sequence
                next_sequence += 1
            ordered.append((sequence, record))
        return encode_account_document(state.authority.ledger(profile_id), tuple(ordered), revision)
