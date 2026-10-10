"""Day-6 repository PORT and in-memory REFERENCE repository.

REFERENCE ONLY - NON-PRODUCTION. The port sits underneath the domain layer:
the authority depends on this interface, never on a storage technology.
InMemoryReferenceRepository is deterministic, process-local and for tests only.
No Redis, database, file or network access exists here.
"""
from __future__ import annotations

import threading
from abc import ABC, abstractmethod
from typing import Optional

from .persistence_contracts import RepositoryConflictError, StoredDocument


class LifecycleStateRepository(ABC):
    """Per-account document store with compare-and-set writes."""

    @abstractmethod
    def list_profiles(self) -> tuple[str, ...]:
        """All stored profile ids, sorted."""

    @abstractmethod
    def load(self, profile_id: str) -> Optional[StoredDocument]: ...

    @abstractmethod
    def save(self, document: StoredDocument, expected_revision: Optional[int]) -> None:
        """Store `document` only if the current revision equals `expected_revision`
        (None = must not exist) and document.revision == expected_revision + 1.
        Otherwise raise RepositoryConflictError and store nothing."""


class InMemoryReferenceRepository(LifecycleStateRepository):
    """REFERENCE ONLY - NON-PRODUCTION deterministic in-memory repository."""

    GRANTS_PRODUCTION_AUTHORITY = False

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._documents: dict[str, StoredDocument] = {}

    def list_profiles(self) -> tuple[str, ...]:
        with self._lock:
            return tuple(sorted(self._documents))

    def load(self, profile_id: str) -> Optional[StoredDocument]:
        with self._lock:
            return self._documents.get(profile_id)

    def save(self, document: StoredDocument, expected_revision: Optional[int]) -> None:
        if not isinstance(document, StoredDocument):
            raise TypeError("document must be a StoredDocument")
        with self._lock:
            current = self._documents.get(document.profile_id)
            current_revision = current.revision if current is not None else None
            if current_revision != expected_revision:
                raise RepositoryConflictError(
                    f"{document.profile_id}: stored revision {current_revision} != expected {expected_revision}")
            if document.revision != (expected_revision or 0) + 1:
                raise RepositoryConflictError(
                    f"{document.profile_id}: new revision {document.revision} must be {(expected_revision or 0) + 1}")
            self._documents[document.profile_id] = document

    def snapshot(self) -> tuple[StoredDocument, ...]:
        with self._lock:
            return tuple(self._documents[k] for k in sorted(self._documents))
