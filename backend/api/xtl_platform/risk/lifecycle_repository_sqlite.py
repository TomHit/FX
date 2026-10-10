"""Day-8 durable SQLite REFERENCE repository - REFERENCE ONLY - NON-PRODUCTION.

Implements the frozen Day-6 LifecycleStateRepository port on a local SQLite file
to prove transactional compare-and-set and restart semantics. SQLite here is a
reference backend, not a production database decision.

PERSISTENCE STORES AUTHORITY; IT DOES NOT CREATE AUTHORITY.
This module only loads, transactionally persists, compares revisions and returns
persistence results. It never decides orders, releases, closures, PnL, repairs,
reconciliation outcomes or broker truth, and it never interprets the payload:
the Day-6 codec and restore remain the only readers of authority.

This is the ONLY production module permitted to import sqlite3 (see
tests/test_isolation.py APPROVED_PERSISTENCE_BOUNDARY).

Schema (repository_schema_version 1):

    xtl_repository_meta(key TEXT PRIMARY KEY, value TEXT NOT NULL)
        ('repository_schema_version', '1')

    xtl_account_documents(
        profile_id     TEXT    PRIMARY KEY  CHECK non-empty text
        revision       INTEGER NOT NULL     CHECK integer >= 1
        payload        TEXT    NOT NULL     CHECK text            -- Day-6 envelope, stored verbatim
        payload_sha256 TEXT    NOT NULL     CHECK 64 chars        -- storage-level integrity of payload
    )

Compare-and-set (inside BEGIN IMMEDIATE ... COMMIT):
    expected None : INSERT ... ON CONFLICT(profile_id) DO NOTHING   (rowcount must be 1; revision must be 1)
    expected N    : UPDATE ... WHERE profile_id = ? AND revision = N (rowcount must be 1; revision must be N+1)
A rowcount other than 1 raises RepositoryConflictError and the transaction is rolled back.

Failure-injection hooks (no-ops in this class; tests subclass them):
    _before_transaction()  _before_commit()  _after_commit()
"""
from __future__ import annotations

import hashlib
import sqlite3
import threading
from dataclasses import dataclass
from typing import ClassVar, Optional

from .persistence_contracts import RepositoryConflictError, StoredDocument
from .persistence_repository import LifecycleStateRepository

REPOSITORY_SCHEMA_VERSION = "1"
SUPPORTED_REPOSITORY_SCHEMA_VERSIONS = frozenset({REPOSITORY_SCHEMA_VERSION})

_SCHEMA = (
    "CREATE TABLE IF NOT EXISTS xtl_repository_meta ("
    " key TEXT PRIMARY KEY NOT NULL,"
    " value TEXT NOT NULL)",
    "CREATE TABLE IF NOT EXISTS xtl_account_documents ("
    " profile_id TEXT PRIMARY KEY NOT NULL CHECK (typeof(profile_id) = 'text' AND length(profile_id) > 0),"
    " revision INTEGER NOT NULL CHECK (typeof(revision) = 'integer' AND revision >= 1),"
    " payload TEXT NOT NULL CHECK (typeof(payload) = 'text'),"
    " payload_sha256 TEXT NOT NULL CHECK (typeof(payload_sha256) = 'text' AND length(payload_sha256) = 64))",
)


class RepositorySchemaError(RuntimeError):
    """The database is not a supported XTL reference repository; nothing is read or written."""


class RepositoryClosedError(RuntimeError):
    pass


def payload_sha256(payload: str) -> str:
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class StoredRowIssue:
    """A row that cannot be returned as a StoredDocument. Reported, never repaired."""

    profile_id: str
    problem: str


class SQLiteReferenceRepository(LifecycleStateRepository):
    """REFERENCE ONLY - NON-PRODUCTION durable repository on one SQLite file."""

    GRANTS_PRODUCTION_AUTHORITY: ClassVar[bool] = False

    def __init__(self, database_path: str) -> None:
        if not isinstance(database_path, str) or not database_path.strip() or database_path.strip() == ":memory:":
            raise ValueError("database_path must be a file path string (':memory:' cannot survive a restart)")
        self._lock = threading.Lock()
        self._conn: Optional[sqlite3.Connection] = sqlite3.connect(
            database_path, isolation_level=None, timeout=10.0, check_same_thread=False)
        try:
            self._conn.execute("PRAGMA synchronous = FULL")
            self._conn.execute("PRAGMA foreign_keys = ON")
            self._initialize_schema()
        except BaseException:
            self._conn.close()
            self._conn = None
            raise

    # ---- lifecycle of the connection ---------------------------------------------------------

    def _initialize_schema(self) -> None:
        conn = self._conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            for statement in _SCHEMA:
                conn.execute(statement)
            row = conn.execute("SELECT value FROM xtl_repository_meta WHERE key = 'repository_schema_version'").fetchone()
            if row is None:
                existing = conn.execute("SELECT COUNT(*) FROM xtl_account_documents").fetchone()[0]
                if existing:
                    raise RepositorySchemaError("documents exist without a repository_schema_version")
                conn.execute("INSERT INTO xtl_repository_meta(key, value) VALUES ('repository_schema_version', ?)",
                             (REPOSITORY_SCHEMA_VERSION,))
            elif row[0] not in SUPPORTED_REPOSITORY_SCHEMA_VERSIONS:
                raise RepositorySchemaError(f"unsupported repository_schema_version {row[0]!r}")
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            raise

    def close(self) -> None:
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

    def _connection(self) -> sqlite3.Connection:
        if self._conn is None:
            raise RepositoryClosedError("repository is closed")
        return self._conn

    # ---- read ---------------------------------------------------------------------------------

    def list_profiles(self) -> tuple[str, ...]:
        with self._lock:
            rows = self._connection().execute("SELECT profile_id FROM xtl_account_documents").fetchall()
        return tuple(sorted(str(r[0]) for r in rows))

    def _row_to_document(self, row) -> tuple[Optional[StoredDocument], Optional[str]]:
        profile_id, revision, payload, digest = row
        if not isinstance(profile_id, str) or not profile_id:
            return None, "profile_id is not non-empty text"
        if type(revision) is not int or revision < 1:
            return None, f"revision {revision!r} is not an integer >= 1"
        if not isinstance(payload, str):
            return None, "payload is not text"
        if not isinstance(digest, str) or payload_sha256(payload) != digest:
            return None, "payload_sha256 does not match stored payload"
        return StoredDocument(profile_id=profile_id, revision=revision, payload=payload), None

    def load(self, profile_id: str) -> Optional[StoredDocument]:
        """Return the stored document, or None if absent OR if the row is structurally
        corrupt. Day-6 restore quarantines a listed profile whose load returns None, so a
        corrupt row fails closed without affecting other accounts. See inspect()."""
        with self._lock:
            row = self._connection().execute(
                "SELECT profile_id, revision, payload, payload_sha256 FROM xtl_account_documents WHERE profile_id = ?",
                (profile_id,)).fetchone()
        if row is None:
            return None
        document, _ = self._row_to_document(row)
        return document

    def inspect(self) -> tuple[StoredRowIssue, ...]:
        """Report structurally corrupt rows (read-only; never repairs)."""
        with self._lock:
            rows = self._connection().execute(
                "SELECT profile_id, revision, payload, payload_sha256 FROM xtl_account_documents").fetchall()
        issues = []
        for row in rows:
            _, problem = self._row_to_document(row)
            if problem is not None:
                issues.append(StoredRowIssue(str(row[0]), problem))
        return tuple(sorted(issues, key=lambda i: i.profile_id))

    def snapshot(self) -> tuple[tuple, ...]:
        """Raw rows, sorted, for test comparison (read-only)."""
        with self._lock:
            rows = self._connection().execute(
                "SELECT profile_id, revision, payload, payload_sha256 FROM xtl_account_documents "
                "ORDER BY profile_id").fetchall()
        return tuple(tuple(r) for r in rows)

    # ---- transactional compare-and-set ---------------------------------------------------------

    def save(self, document: StoredDocument, expected_revision: Optional[int]) -> None:
        if not isinstance(document, StoredDocument):
            raise TypeError("document must be a StoredDocument")
        if expected_revision is not None and (type(expected_revision) is not int or expected_revision < 1):
            raise RepositoryConflictError(f"{document.profile_id}: invalid expected revision {expected_revision!r}")
        if document.revision != (expected_revision or 0) + 1:
            raise RepositoryConflictError(
                f"{document.profile_id}: new revision {document.revision} must be {(expected_revision or 0) + 1}")
        with self._lock:
            conn = self._connection()
            self._before_transaction()
            conn.execute("BEGIN IMMEDIATE")
            try:
                digest = payload_sha256(document.payload)
                if expected_revision is None:
                    cursor = conn.execute(
                        "INSERT INTO xtl_account_documents(profile_id, revision, payload, payload_sha256) "
                        "VALUES (?, ?, ?, ?) ON CONFLICT(profile_id) DO NOTHING",
                        (document.profile_id, document.revision, document.payload, digest))
                else:
                    cursor = conn.execute(
                        "UPDATE xtl_account_documents SET revision = ?, payload = ?, payload_sha256 = ? "
                        "WHERE profile_id = ? AND revision = ?",
                        (document.revision, document.payload, digest, document.profile_id, expected_revision))
                if cursor.rowcount != 1:
                    raise RepositoryConflictError(
                        f"{document.profile_id}: compare-and-set failed (expected revision {expected_revision})")
                self._before_commit()
                conn.execute("COMMIT")
            except BaseException:
                if conn.in_transaction:
                    conn.execute("ROLLBACK")
                raise
        self._after_commit()

    # ---- failure-injection hooks (no-ops) ----------------------------------------------------

    def _before_transaction(self) -> None:
        return None

    def _before_commit(self) -> None:
        return None

    def _after_commit(self) -> None:
        return None
