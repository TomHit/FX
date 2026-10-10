"""Day-9 SQLite transactional repository - REFERENCE ONLY - NON-PRODUCTION.

EXECUTION AUTHORITY = FALSE. BROKER AUTHORITY = FALSE.

Extends the frozen Day-8 SQLiteReferenceRepository (no modification) with one
durable transaction that spans the capacity decision and the reservation write:

    BEGIN IMMEDIATE          -- serializes competing writers on this database
      SELECT the account row -- the decision reads exactly this state
      UPDATE/INSERT ... WHERE revision = expected   -- compare-and-set, rowcount must be 1
    COMMIT                   -- any exception rolls the whole transaction back

Nothing else is added: row parsing, integrity digests, schema checks, PRAGMAs and
the failure-injection hooks are inherited from the frozen Day-8 repository.
This file and the frozen Day-8 repository are the only approved sqlite3 surfaces.
"""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from typing import Optional

from ..domain._validation import ContractError
from .durable_authority_contracts import AccountAuthorizationTransaction, TransactionalLifecycleStateRepository
from .lifecycle_repository_sqlite import SQLiteReferenceRepository, payload_sha256
from .persistence_contracts import RepositoryConflictError, StoredDocument


class _SQLiteTransaction(AccountAuthorizationTransaction):
    def __init__(self, connection: sqlite3.Connection, repository: SQLiteReferenceRepository, profile_id: str) -> None:
        self._connection, self._repository, self._profile_id = connection, repository, profile_id
        self._problem: Optional[str] = None
        self.wrote = False

    def load(self) -> Optional[StoredDocument]:
        row = self._connection.execute(
            "SELECT profile_id, revision, payload, payload_sha256 FROM xtl_account_documents WHERE profile_id = ?",
            (self._profile_id,)).fetchone()
        if row is None:
            self._problem = None
            return None
        document, problem = self._repository._row_to_document(row)
        self._problem = problem
        return document

    def problem(self) -> Optional[str]:
        return self._problem

    def write(self, document: StoredDocument, expected_revision: Optional[int]) -> None:
        if not isinstance(document, StoredDocument) or document.profile_id != self._profile_id:
            raise ContractError("document does not belong to this transaction")
        if expected_revision is not None and (type(expected_revision) is not int or expected_revision < 1):
            raise RepositoryConflictError(f"{self._profile_id}: invalid expected revision {expected_revision!r}")
        if document.revision != (expected_revision or 0) + 1:
            raise RepositoryConflictError(
                f"{self._profile_id}: new revision {document.revision} must be {(expected_revision or 0) + 1}")
        digest = payload_sha256(document.payload)
        if expected_revision is None:
            cursor = self._connection.execute(
                "INSERT INTO xtl_account_documents(profile_id, revision, payload, payload_sha256) "
                "VALUES (?, ?, ?, ?) ON CONFLICT(profile_id) DO NOTHING",
                (document.profile_id, document.revision, document.payload, digest))
        else:
            cursor = self._connection.execute(
                "UPDATE xtl_account_documents SET revision = ?, payload = ?, payload_sha256 = ? "
                "WHERE profile_id = ? AND revision = ?",
                (document.revision, document.payload, digest, document.profile_id, expected_revision))
        if cursor.rowcount != 1:
            raise RepositoryConflictError(
                f"{self._profile_id}: compare-and-set failed (expected revision {expected_revision})")
        self.wrote = True


class SQLiteTransactionalRepository(SQLiteReferenceRepository, TransactionalLifecycleStateRepository):
    """REFERENCE ONLY - NON-PRODUCTION durable transactional repository."""

    @contextmanager
    def authorization_transaction(self, profile_id: str):
        if not isinstance(profile_id, str) or not profile_id.strip():
            raise ContractError("profile_id must be a non-empty string")
        with self._lock:
            connection = self._connection()
            self._before_transaction()
            connection.execute("BEGIN IMMEDIATE")
            transaction = _SQLiteTransaction(connection, self, profile_id)
            try:
                yield transaction
                if transaction.wrote:
                    self._before_commit()
                connection.execute("COMMIT")
            except BaseException:
                if connection.in_transaction:
                    connection.execute("ROLLBACK")
                raise
        if transaction.wrote:
            self._after_commit()
