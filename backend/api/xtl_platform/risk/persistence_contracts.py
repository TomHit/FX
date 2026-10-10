"""Day-6 persistence CONTRACTS - REFERENCE ONLY - NON-PRODUCTION.

Versioned, per-account persisted representation of the authoritative Day-4/Day-5
state: account committed state, ACTIVE reservations and lifecycle records.

PRODUCTION AUTHORITY = NONE. No Redis, database, file or network backend.
"""
from __future__ import annotations

from dataclasses import dataclass

from ..domain._validation import ContractError
from ..domain.enums import StrEnum

SCHEMA_NAME = "xtl.reference.account_lifecycle_state"
SCHEMA_VERSION = 1
SUPPORTED_SCHEMA_VERSIONS = frozenset({SCHEMA_VERSION})


class RestoreRejectionCode(StrEnum):
    MALFORMED_DOCUMENT = "MALFORMED_DOCUMENT"
    UNSUPPORTED_SCHEMA_VERSION = "UNSUPPORTED_SCHEMA_VERSION"
    INTEGRITY_CHECK_FAILED = "INTEGRITY_CHECK_FAILED"
    REVISION_MISMATCH = "REVISION_MISMATCH"
    PROFILE_MISMATCH = "PROFILE_MISMATCH"
    INVALID_RECORD = "INVALID_RECORD"
    OWNERSHIP_CONFLICT = "OWNERSHIP_CONFLICT"


class PersistenceDecodeError(ContractError):
    def __init__(self, code: RestoreRejectionCode, errors: list[str]) -> None:
        self.code = code
        self.errors = tuple(errors)
        super().__init__(f"{code}: " + "; ".join(self.errors))


class PersistenceEncodeError(ContractError):
    """In-memory state cannot be represented faithfully; nothing is written."""


class RepositoryConflictError(ContractError):
    """Compare-and-set failed: the stored revision is not the expected revision."""


class PersistenceCommitError(ContractError):
    """A commit could not be persisted. In-memory state is unchanged."""


class QuarantinedAccountError(ContractError):
    """The account's persisted state was rejected at restore; it may not be re-created."""


@dataclass(frozen=True)
class StoredDocument:
    """Opaque stored payload for one account. The repository never interprets it."""

    profile_id: str
    revision: int
    payload: str

    def __post_init__(self) -> None:
        if not isinstance(self.profile_id, str) or not self.profile_id.strip():
            raise ContractError("profile_id must be a non-empty string")
        if type(self.revision) is not int or self.revision < 1:
            raise ContractError("revision must be an int >= 1")
        if not isinstance(self.payload, str):
            raise ContractError("payload must be a string")


@dataclass(frozen=True)
class AccountRestoreRejection:
    profile_id: str
    code: RestoreRejectionCode
    errors: tuple[str, ...]


@dataclass(frozen=True)
class RestoreReport:
    """Outcome of restoring an authority. Rejected accounts are quarantined, never re-created."""

    restored_profiles: tuple[str, ...]
    rejections: tuple[AccountRestoreRejection, ...]

    @property
    def quarantined_profiles(self) -> tuple[str, ...]:
        return tuple(r.profile_id for r in self.rejections)

    @property
    def clean(self) -> bool:
        return not self.rejections
