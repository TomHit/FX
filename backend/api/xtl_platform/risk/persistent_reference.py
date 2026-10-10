"""ReferencePersistentLifecycleAuthority - Day 6.

    REFERENCE ONLY - NON-PRODUCTION.
    PRODUCTION AUTHORITY = NONE. BROKER AUTHORITY = NONE. REDIS = NONE.
    AUTHORIZED_RESERVED IS NOT AN ORDER.

Extends the frozen Day-5 ReferenceLifecycleRiskAuthority (and therefore the
frozen Day-4 authorization) WITHOUT modifying it. No parallel state machine.

Write-ahead commit. Every Day-4/Day-5 commit funnels through `_commit` or
`_commit_lifecycle`. Inside the lock, this class:
  1. diffs old vs new state; a commit may change at most ONE account
  2. encodes that account's complete document (revision = stored + 1)
  3. saves it with compare-and-set on the expected revision
  4. only then swaps the in-memory state
If 1-3 fail, PersistenceCommitError propagates and in-memory state is unchanged.
If the process dies after 3, restore() yields the committed state.

Restore. `restore()` builds a new authority from the repository only; it never
writes. Each account document is decoded independently. A rejected account is
QUARANTINED: it is not loaded, it cannot be re-registered, and it cannot affect
any other account. Cross-account ownership collisions quarantine every account
involved. Nothing is ever silently released.
"""
from __future__ import annotations

from datetime import datetime
from typing import Callable, ClassVar, Optional

from .atomic_contracts import ReferenceAccountState, ReferenceAuthorityState
from .contracts import RiskEvaluationConfig
from .lifecycle_contracts import ReferenceLifecycleState
from .lifecycle_reference import ReferenceLifecycleRiskAuthority
from .persistence_codec import DecodedAccount, decode_account_document, encode_account_document
from .persistence_contracts import (
    AccountRestoreRejection,
    PersistenceCommitError,
    PersistenceDecodeError,
    PersistenceEncodeError,
    QuarantinedAccountError,
    RestoreRejectionCode as RC,
    RestoreReport,
)
from .persistence_repository import LifecycleStateRepository
from .reconciliation_contracts import ExternalAccountSnapshot, ReconciliationReport
from .reconciliation_reference import quarantined_report, reconcile_account

_RESTORE_TOKEN = object()


class ReferencePersistentLifecycleAuthority(ReferenceLifecycleRiskAuthority):
    """REFERENCE ONLY - NON-PRODUCTION restart-safe lifecycle authority.

    NOT production authority. Persists through a LifecycleStateRepository port;
    Day 6 provides only the in-memory reference repository.
    """

    GRANTS_PRODUCTION_AUTHORITY: ClassVar[bool] = False
    AUTHORITY_CLASSIFICATION: ClassVar[str] = "REFERENCE_ONLY_NON_PRODUCTION"

    def __init__(self, config: RiskEvaluationConfig, clock: Callable[[], datetime],
                 repository: LifecycleStateRepository, *, acknowledge_reference_only_non_production: bool = False,
                 _restore_token: object = None) -> None:
        if not isinstance(repository, LifecycleStateRepository):
            raise TypeError("repository must be a LifecycleStateRepository")
        if _restore_token is not _RESTORE_TOKEN and repository.list_profiles():
            raise PersistenceCommitError("repository already holds state; use restore() instead of a fresh authority")
        self._repository = repository
        self._revisions: dict[str, int] = {}
        self._sequences: dict[str, int] = {}
        self._quarantined: frozenset[str] = frozenset()
        super().__init__(config, clock,
                         acknowledge_reference_only_non_production=acknowledge_reference_only_non_production)

    # ---- restore ----------------------------------------------------------------------------

    @classmethod
    def restore(cls, config: RiskEvaluationConfig, clock: Callable[[], datetime],
                repository: LifecycleStateRepository, *,
                acknowledge_reference_only_non_production: bool = False
                ) -> tuple["ReferencePersistentLifecycleAuthority", RestoreReport]:
        auth = cls(config, clock, repository,
                   acknowledge_reference_only_non_production=acknowledge_reference_only_non_production,
                   _restore_token=_RESTORE_TOKEN)
        decoded: dict[str, DecodedAccount] = {}
        rejections: list[AccountRestoreRejection] = []
        for pid in repository.list_profiles():
            stored = repository.load(pid)
            if stored is None:
                rejections.append(AccountRestoreRejection(pid, RC.MALFORMED_DOCUMENT, ("document vanished during restore",)))
                continue
            try:
                decoded[pid] = decode_account_document(stored)
            except PersistenceDecodeError as exc:
                rejections.append(AccountRestoreRejection(pid, exc.code, exc.errors))

        # Cross-account ownership collisions quarantine every account involved.
        claims: dict[tuple[str, object], set[str]] = {}
        for pid, d in decoded.items():
            keys = [("child_trade_id", r.child_trade_id) for r in d.ledger.reservations]
            keys += [("child_trade_id", r.child_trade_id) for _, r in d.records]
            keys += [("transition_id", r.transition_id) for _, r in d.records]
            keys += [("account", d.ledger.account_state.account)]
            for key in keys:
                claims.setdefault(key, set()).add(pid)
        conflicted: dict[str, list[str]] = {}
        for (kind, value), pids in claims.items():
            if len(pids) > 1:
                for pid in pids:
                    conflicted.setdefault(pid, []).append(f"{kind} {value} also claimed by {sorted(pids - {pid})}")
        for pid in sorted(conflicted):
            rejections.append(AccountRestoreRejection(pid, RC.OWNERSHIP_CONFLICT, tuple(sorted(conflicted[pid]))))

        good = {pid: d for pid, d in decoded.items() if pid not in conflicted}
        ordered = sorted(((seq, pid, r) for pid, d in good.items() for seq, r in d.records),
                         key=lambda x: (x[0], x[1]))
        try:
            state = ReferenceLifecycleState(
                ReferenceAuthorityState(tuple(good[pid].ledger for pid in sorted(good))),
                tuple(r for _, _, r in ordered))
        except Exception as exc:  # noqa: BLE001 - cannot prove consistency: quarantine everything
            for pid in sorted(good):
                rejections.append(AccountRestoreRejection(pid, RC.OWNERSHIP_CONFLICT,
                                                          (f"combined state rejected: {type(exc).__name__}",)))
            good = {}
            ordered = []
            state = ReferenceLifecycleState(ReferenceAuthorityState(()), ())

        auth._lifecycle = state
        auth._revisions = {pid: d.revision for pid, d in good.items()}
        auth._sequences = {r.child_trade_id: seq for seq, _, r in ordered}
        auth._quarantined = frozenset(r.profile_id for r in rejections)
        report = RestoreReport(restored_profiles=tuple(sorted(good)),
                               rejections=tuple(sorted(rejections, key=lambda r: (r.profile_id, r.code.value))))
        return auth, report

    def quarantined_profiles(self) -> tuple[str, ...]:
        with self._lock:
            return tuple(sorted(self._quarantined))

    # ---- write-ahead commit -----------------------------------------------------------------

    def _commit(self, new_state: ReferenceAuthorityState) -> None:
        if not self._lock.locked():
            raise RuntimeError("commit attempted outside the atomic section")
        self._persist_then_swap(ReferenceLifecycleState(new_state, self._lifecycle.records))

    def _commit_lifecycle(self, new_state: ReferenceLifecycleState) -> None:
        if not self._lock.locked():
            raise RuntimeError("lifecycle commit attempted outside the atomic section")
        self._persist_then_swap(new_state)

    def _persist_then_swap(self, new: ReferenceLifecycleState) -> None:
        old = self._lifecycle
        profiles = {l.profile_id for l in old.authority.ledgers} | {l.profile_id for l in new.authority.ledgers}

        def records_of(state: ReferenceLifecycleState, pid: str):
            return tuple(r for r in state.records if r.profile_id == pid)

        changed = sorted(pid for pid in profiles
                         if old.authority.ledger(pid) != new.authority.ledger(pid)
                         or records_of(old, pid) != records_of(new, pid))
        if not changed:
            self._lifecycle = new
            return
        if len(changed) != 1:
            raise PersistenceCommitError(f"a commit may change exactly one account; got {changed}")
        pid = changed[0]
        if pid in self._quarantined:
            raise QuarantinedAccountError(f"{pid} is quarantined after a rejected restore")
        ledger = new.authority.ledger(pid)
        if ledger is None:
            raise PersistenceCommitError(f"removing account {pid} is not supported")

        sequences = dict(self._sequences)
        next_seq = max(sequences.values(), default=-1) + 1
        for r in new.records:
            if r.child_trade_id not in sequences:
                sequences[r.child_trade_id] = next_seq
                next_seq += 1
        expected = self._revisions.get(pid)
        try:
            document = encode_account_document(
                ledger, tuple((sequences[r.child_trade_id], r) for r in records_of(new, pid)), (expected or 0) + 1)
        except PersistenceEncodeError as exc:
            raise PersistenceCommitError(f"{pid}: state not persistable ({exc}); nothing changed") from exc
        try:
            self._repository.save(document, expected)
        except Exception as exc:  # noqa: BLE001 - any storage failure: do not swap
            raise PersistenceCommitError(f"{pid}: persistence failed ({type(exc).__name__}); nothing changed") from exc
        # Persisted. Swap in-memory state (no failure possible below).
        self._revisions = {**self._revisions, pid: document.revision}
        self._sequences = sequences
        self._lifecycle = new

    # ---- account registration ----------------------------------------------------------------

    def register_account(self, account_state: ReferenceAccountState) -> None:
        if isinstance(account_state, ReferenceAccountState):
            with self._lock:
                if account_state.profile_id in self._quarantined:
                    raise QuarantinedAccountError(
                        f"{account_state.profile_id} is quarantined; re-registration could silently drop persisted risk")
        super().register_account(account_state)

    # ---- reconciliation (read-only) --------------------------------------------------------------

    def reference_reconcile(self, snapshot: ExternalAccountSnapshot) -> ReconciliationReport:
        """Pure, read-only reconciliation report. Never mutates, persists, releases or authorizes."""
        with self._lock:
            state, quarantined = self._lifecycle, self._quarantined
        if isinstance(snapshot, ExternalAccountSnapshot) and snapshot.profile_id in quarantined:
            return quarantined_report(snapshot)
        return reconcile_account(state, snapshot)
