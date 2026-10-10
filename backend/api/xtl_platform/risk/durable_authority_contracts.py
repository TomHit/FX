"""Day-9 durable account-risk authority CONTRACTS - REFERENCE ONLY - NON-PRODUCTION.

EXECUTION AUTHORITY = FALSE. BROKER AUTHORITY = FALSE.
No orders, positions, market data, broker symbols, lot sizing or prop-firm rules.
Day 9 decides on NORMALIZED RISK only and is reusable by CFD and futures paths.

It adds ONE thing to the frozen stack: a durable transaction boundary in which the
capacity decision and the reservation write happen together, so a stale revision can
never authorize against obsolete capacity.

    authorization_transaction(profile_id):     # serialized, durable
        load durable document (revision N)
        decide with the FROZEN Day-4/5/6 authority on exactly that state
        write revision N+1 under compare-and-set, or write nothing
    commit

Reused frozen types (no parallel concepts): AtomicAuthorizationRequest,
AtomicAuthorizationResult, ReferenceReservation, ReferenceAccountState,
ReferenceLifecycleState, LifecycleTransitionRequest/Result, StoredDocument.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass
from typing import ClassVar, Iterator, Optional

from ..domain._validation import ContractError
from ..domain.enums import StrEnum
from .atomic_contracts import AtomicAuthorizationResult, ReferenceReservation
from .lifecycle_contracts import LifecycleTransitionResult, ReservationLifecycleRecord
from .persistence_contracts import RepositoryConflictError, StoredDocument
from .persistence_repository import InMemoryReferenceRepository, LifecycleStateRepository


class DurableOutcomeCode(StrEnum):
    PERSISTED = "PERSISTED"                            # decided and durably reserved exactly once
    IDEMPOTENT_REPLAY = "IDEMPOTENT_REPLAY"            # already reserved; nothing written
    BLOCKED = "BLOCKED"                                # frozen authority refused; nothing written
    REVISION_CONFLICT = "REVISION_CONFLICT"            # compare-and-set lost; nothing written
    ACCOUNT_NOT_REGISTERED = "ACCOUNT_NOT_REGISTERED"  # no durable document for this account
    DURABLE_STATE_REJECTED = "DURABLE_STATE_REJECTED"  # corrupt/unsupported durable state; fail closed


class AccountAuthorizationTransaction(ABC):
    """One serialized durable transaction over ONE account document."""

    @abstractmethod
    def load(self) -> Optional[StoredDocument]:
        """The account's current document, or None if absent or structurally unreadable."""

    @abstractmethod
    def problem(self) -> Optional[str]:
        """Why load() returned None for an existing row, if that is the case."""

    @abstractmethod
    def write(self, document: StoredDocument, expected_revision: Optional[int]) -> None:
        """Compare-and-set inside this transaction. Raises RepositoryConflictError on loss."""


class TransactionalLifecycleStateRepository(LifecycleStateRepository):
    """Day-6 port plus a durable transaction for decide-and-write."""

    @abstractmethod
    def authorization_transaction(self, profile_id: str) -> Iterator[AccountAuthorizationTransaction]:
        """Context manager. Commits on clean exit, rolls back on any exception."""


class _InMemoryTransaction(AccountAuthorizationTransaction):
    def __init__(self, repository: "InMemoryTransactionalRepository", profile_id: str) -> None:
        self._repository, self._profile_id = repository, profile_id
        self.staged: Optional[StoredDocument] = None

    def load(self) -> Optional[StoredDocument]:
        return self._repository.documents.get(self._profile_id)

    def problem(self) -> Optional[str]:
        return None

    def write(self, document: StoredDocument, expected_revision: Optional[int]) -> None:
        if not isinstance(document, StoredDocument) or document.profile_id != self._profile_id:
            raise ContractError("document does not belong to this transaction")
        current = self._repository.documents.get(self._profile_id)
        current_revision = current.revision if current is not None else None
        if current_revision != expected_revision or document.revision != (expected_revision or 0) + 1:
            raise RepositoryConflictError(
                f"{self._profile_id}: stored revision {current_revision} != expected {expected_revision}")
        self.staged = document


class InMemoryTransactionalRepository(InMemoryReferenceRepository, TransactionalLifecycleStateRepository):
    """REFERENCE ONLY - NON-PRODUCTION in-memory transactional repository (tests/parity)."""

    @property
    def documents(self) -> dict:
        return self._documents

    @contextmanager
    def authorization_transaction(self, profile_id: str):
        with self._lock:                                   # serializes the whole decide-and-write
            transaction = _InMemoryTransaction(self, profile_id)
            yield transaction                              # an exception here discards the staged write
            if transaction.staged is not None:
                self._documents[profile_id] = transaction.staged


@dataclass(frozen=True)
class DurableAuthorizationOutcome:
    """Result of one durable authorize+reserve. Carries the frozen authority result."""

    EXECUTION_AUTHORITY: ClassVar[bool] = False
    BROKER_AUTHORITY: ClassVar[bool] = False
    GRANTS_PRODUCTION_AUTHORITY: ClassVar[bool] = False

    code: DurableOutcomeCode
    profile_id: str
    child_trade_id: str
    reason: str
    authorization: Optional[AtomicAuthorizationResult]
    reservation: Optional[ReferenceReservation]
    durable_revision: Optional[int]
    rejection_errors: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.code, DurableOutcomeCode):
            raise ContractError("code must be a DurableOutcomeCode")
        if not isinstance(self.reason, str) or not self.reason.strip():
            raise ContractError("reason must be a non-empty string")
        C = DurableOutcomeCode
        if self.code is C.PERSISTED:
            if not (self.authorization is not None and self.authorization.authorized
                    and not self.authorization.idempotent_replay and self.reservation is not None
                    and type(self.durable_revision) is int):
                raise ContractError("PERSISTED requires a first-time authorization, reservation and revision")
        elif self.code is C.IDEMPOTENT_REPLAY:
            if not (self.authorization is not None and self.authorization.idempotent_replay
                    and self.reservation is not None):
                raise ContractError("IDEMPOTENT_REPLAY requires the existing reservation")
        elif self.code is C.BLOCKED:
            if self.authorization is None or self.authorization.authorized or self.reservation is not None:
                raise ContractError("BLOCKED requires a refusing authorization and no reservation")
        elif self.reservation is not None or self.authorization is not None:
            raise ContractError(f"{self.code} must carry neither authorization nor reservation")

    @property
    def persisted(self) -> bool:
        return self.code is DurableOutcomeCode.PERSISTED


@dataclass(frozen=True)
class DurableTransitionOutcome:
    """Result of one durable lifecycle transition (transfer/release)."""

    EXECUTION_AUTHORITY: ClassVar[bool] = False
    BROKER_AUTHORITY: ClassVar[bool] = False

    code: DurableOutcomeCode
    profile_id: str
    child_trade_id: str
    reason: str
    transition: Optional[LifecycleTransitionResult]
    record: Optional[ReservationLifecycleRecord]
    durable_revision: Optional[int]
    rejection_errors: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.code, DurableOutcomeCode):
            raise ContractError("code must be a DurableOutcomeCode")
        C = DurableOutcomeCode
        if self.code is C.PERSISTED and not (self.transition is not None and self.transition.applied
                                             and not self.transition.idempotent_replay and self.record is not None
                                             and type(self.durable_revision) is int):
            raise ContractError("PERSISTED requires a first-time applied transition")
        if self.code is C.IDEMPOTENT_REPLAY and not (self.transition is not None
                                                     and self.transition.idempotent_replay):
            raise ContractError("IDEMPOTENT_REPLAY requires a replayed transition")
        if self.code is C.BLOCKED and (self.transition is None or self.transition.applied):
            raise ContractError("BLOCKED requires a refused transition")

    @property
    def persisted(self) -> bool:
        return self.code is DurableOutcomeCode.PERSISTED
