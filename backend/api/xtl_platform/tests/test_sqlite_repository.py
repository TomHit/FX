"""Day 8 - durable SQLite reference repository and restart recovery.

REFERENCE ONLY - NON-PRODUCTION. Deterministic and offline: a temporary SQLite
file, no network, no broker, no wall-clock decisions.
Persistence stores authority; it does not create authority.
"""
from __future__ import annotations

import ast
import hashlib
import json
import os
import sqlite3
import subprocess
import sys
import threading
from datetime import timedelta
from decimal import Decimal

import pytest

from api.xtl_platform.domain.enums import StrategyId
from api.xtl_platform.risk.atomic_contracts import AtomicAuthorizationCode as A
from api.xtl_platform.risk.atomic_reference import derive_evaluation_snapshot
from api.xtl_platform.risk.lifecycle_contracts import ExposureOwner as EO
from api.xtl_platform.risk.lifecycle_repository_sqlite import (
    REPOSITORY_SCHEMA_VERSION, RepositoryClosedError, RepositorySchemaError, SQLiteReferenceRepository,
    StoredRowIssue, payload_sha256,
)
from api.xtl_platform.risk.persistence_codec import canonical_json, content_digest
from api.xtl_platform.risk.persistence_contracts import (
    PersistenceCommitError, QuarantinedAccountError, RepositoryConflictError, RestoreRejectionCode as RC,
    StoredDocument,
)
from api.xtl_platform.risk.persistence_repository import LifecycleStateRepository
from api.xtl_platform.risk.persistent_reference import ReferencePersistentLifecycleAuthority
from api.xtl_platform.risk.resolution_contracts import (
    ResolutionDecision as RD, ResolutionPolicy, ResolutionReasonCode as RRC, ResolutionRequest,
)
from api.xtl_platform.risk.resolution_reference import build_reconciliation_evidence, resolve_reconciliation
from api.xtl_platform.risk.reconciliation_contracts import ExternalObjectKind as K
from api.xtl_platform.tests._builders import NOW, D
from api.xtl_platform.tests.test_atomic_reference import CONFIG, P1, P2, FakeClock, request
from api.xtl_platform.tests.test_lifecycle_reference import transition
from api.xtl_platform.tests.test_persistence_reconciliation import P3, obj, snap, state

AT = NOW + timedelta(seconds=30)
POLICY = ResolutionPolicy(max_evidence_age=timedelta(minutes=5))


# ---- helpers -----------------------------------------------------------------------------------

@pytest.fixture
def db(tmp_path):
    return str(tmp_path / "xtl_reference.sqlite3")


def repo(db):
    return SQLiteReferenceRepository(db)


def authority(repository, clock=None, *profiles):
    auth = ReferencePersistentLifecycleAuthority(CONFIG, clock or FakeClock(), repository,
                                                 acknowledge_reference_only_non_production=True)
    for pid, profile in (profiles or (("p1", P1),)):
        auth.register_account(state(pid, profile))
    return auth


def restore(repository, clock=None):
    return ReferencePersistentLifecycleAuthority.restore(CONFIG, clock or FakeClock(), repository,
                                                         acknowledge_reference_only_non_production=True)


def reserve(auth, risk="500", thesis_id="A-1", strategy=StrategyId.STRATEGY_A, profile=P1):
    r = auth.authorize_and_reserve(request(risk=risk, thesis_id=thesis_id, strategy=strategy, profile=profile))
    assert r.authorized, r.reason
    return r.reservation


def exposure(auth, pid="p1"):
    ledger = auth.state().ledger(pid)
    s = derive_evaluation_snapshot(ledger)
    reserved = sum((r.risk_usd for r in ledger.reservations), Decimal("0"))
    return (s.open_risk_usd, reserved, ledger.account_state.committed_open_risk_usd, s.reserved_position_slots)


def raw(db, sql, params=(), *, ignore_checks=False):
    conn = sqlite3.connect(db, isolation_level=None)
    try:
        if ignore_checks:
            conn.execute("PRAGMA ignore_check_constraints = ON")
        cur = conn.execute(sql, params)
        return cur.fetchall()
    finally:
        conn.close()


def rewrite_payload(db, pid, mutate, *, fix_payload_digest=True, fix_content_digest=True):
    """Corrupt a stored document exactly as an attacker or bit-rot would."""
    payload = raw(db, "SELECT payload FROM xtl_account_documents WHERE profile_id = ?", (pid,))[0][0]
    envelope = json.loads(payload)
    mutate(envelope)
    if fix_content_digest and isinstance(envelope.get("content"), dict):
        envelope["content_sha256"] = content_digest(envelope["content"])
    new_payload = canonical_json(envelope)
    digest = payload_sha256(new_payload) if fix_payload_digest else "0" * 64
    raw(db, "UPDATE xtl_account_documents SET payload = ?, payload_sha256 = ? WHERE profile_id = ?",
        (new_payload, digest, pid))


# ---- 1. create / load / revision progression / round trip ------------------------------------------

def test_create_load_and_revision_progression(db):
    r = repo(db)
    assert r.list_profiles() == () and r.load("p1") is None
    auth = authority(r)
    assert r.load("p1").revision == 1
    reserve(auth, "100", "A")
    assert r.load("p1").revision == 2
    auth.reference_transition(transition(auth.state().ledger("p1").reservations[0], EO.RELEASED))
    doc = r.load("p1")
    assert (doc.revision, doc.profile_id) == (3, "p1") and r.list_profiles() == ("p1",)
    assert payload_sha256(doc.payload) == raw(db, "SELECT payload_sha256 FROM xtl_account_documents")[0][0]
    assert raw(db, "SELECT value FROM xtl_repository_meta WHERE key='repository_schema_version'")[0][0] == REPOSITORY_SCHEMA_VERSION


def test_exact_document_round_trip_through_disk(db):
    r = repo(db)
    auth = authority(r)
    reserve(auth, "500", "RT")
    stored = r.load("p1")
    r.close()
    reopened = repo(db)
    assert reopened.load("p1") == stored                       # byte-identical payload and revision
    restored, report = restore(reopened)
    assert report.clean and restored.lifecycle_state() == auth.lifecycle_state()


# ---- 4. restart proof (new repository AND a real new process) ---------------------------------------

def test_active_reservation_survives_restart_with_new_repository_instance(db):
    instance_a = repo(db)
    auth_a = authority(instance_a)
    res = reserve(auth_a, "500", "HELD")
    before = exposure(auth_a)
    revision = instance_a.load("p1").revision
    instance_a.close()
    with pytest.raises(RepositoryClosedError):
        instance_a.load("p1")

    instance_b = repo(db)                                       # genuinely new repository object
    auth_b, report = restore(instance_b)                        # genuinely new authority object
    assert report.clean
    assert auth_b.lifecycle_state().owner_of(res.child_trade_id) is EO.ACTIVE_RESERVATION
    assert exposure(auth_b) == before == (D("900"), D("500"), D("400"), 1)
    assert auth_b.state().reservation_for(res.child_trade_id) == res      # same ownership identity
    assert instance_b.load("p1").revision == revision                     # same persisted revision
    replay = auth_b.authorize_and_reserve(request(risk="500", thesis_id="HELD"))
    assert replay.idempotent_replay and len(auth_b.state().reservations) == 1


CHILD_WRITER = """
import sys
sys.path.insert(0, sys.argv[2])
from datetime import timedelta
from decimal import Decimal
from api.xtl_platform.risk.lifecycle_repository_sqlite import SQLiteReferenceRepository
from api.xtl_platform.risk.persistent_reference import ReferencePersistentLifecycleAuthority
from api.xtl_platform.tests.test_sqlite_repository import authority, reserve
repository = SQLiteReferenceRepository(sys.argv[1])
auth = authority(repository)
res = reserve(auth, "500", "HELD")
print(res.child_trade_id)
"""


def test_real_process_restart_child_writes_parent_restores(db, tmp_path):
    root = str(__import__("pathlib").Path(__file__).resolve().parents[3])
    script = tmp_path / "writer.py"
    script.write_text(CHILD_WRITER)
    out = subprocess.run([sys.executable, str(script), db, root], capture_output=True, text=True, timeout=120)
    assert out.returncode == 0, out.stderr
    child_trade_id = out.stdout.strip()

    auth, report = restore(repo(db))                             # this process never wrote anything
    assert report.clean
    assert auth.lifecycle_state().owner_of(child_trade_id) is EO.ACTIVE_RESERVATION
    assert exposure(auth) == (D("900"), D("500"), D("400"), 1)


# ---- 5/6. committed and released survive restart ------------------------------------------------------

@pytest.mark.parametrize("target,expected", [(EO.COMMITTED_POSITION, (D("900"), D("0"), D("900"), 0)),
                                             (EO.RELEASED, (D("400"), D("0"), D("400"), 0))])
def test_terminal_owner_survives_restart_and_is_never_resurrected(db, target, expected):
    r = repo(db)
    auth = authority(r)
    res = reserve(auth, "500", "T")
    auth.reference_transition(transition(res, target))
    r.close()

    auth2, report = restore(repo(db))
    assert report.clean and auth2.lifecycle_state().owner_of(res.child_trade_id) is target
    assert exposure(auth2) == expected
    assert auth2.state().reservation_for(res.child_trade_id) is None      # never back to ACTIVE
    assert auth2.authorize_and_reserve(request(risk="500", thesis_id="T")).code is A.DUPLICATE_TRADE_ID
    other = EO.RELEASED if target is EO.COMMITTED_POSITION else EO.COMMITTED_POSITION
    assert auth2.reference_transition(transition(res, other)).code.value == "BLOCK_INVALID_TRANSITION"
    replay = auth2.reference_transition(transition(res, target))
    assert replay.applied and replay.idempotent_replay


# ---- 7. split brain -----------------------------------------------------------------------------------

def test_two_connections_cannot_both_commit_from_the_same_revision(db):
    writer_a, writer_b = repo(db), repo(db)
    authority(writer_a)
    doc_a, doc_b = writer_a.load("p1"), writer_b.load("p1")
    assert doc_a.revision == doc_b.revision == 1

    writer_a.save(StoredDocument("p1", 2, doc_a.payload.replace('"revision":1', '"revision":2')), 1)
    with pytest.raises(RepositoryConflictError):
        writer_b.save(StoredDocument("p1", 2, doc_b.payload.replace('"revision":1', '"revision":2') + " "), 1)
    winner = repo(db).load("p1")
    assert winner.revision == 2 and winner.payload == doc_a.payload.replace('"revision":1', '"revision":2')


def test_concurrent_writers_exactly_one_wins(db):
    base = repo(db)
    authority(base)
    doc = base.load("p1")
    results, lock, barrier = [], threading.Lock(), threading.Barrier(8)
    repos = [repo(db) for _ in range(8)]

    def writer(i):
        barrier.wait()
        try:
            repos[i].save(StoredDocument("p1", 2, doc.payload + " " * i), 1)
            outcome = ("ok", i)
        except RepositoryConflictError:
            outcome = ("conflict", i)
        with lock:
            results.append(outcome)

    threads = [threading.Thread(target=writer, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    winners = [i for kind, i in results if kind == "ok"]
    assert len(winners) == 1
    final = repo(db).load("p1")
    assert final.revision == 2 and final.payload == doc.payload + " " * winners[0]
    assert len(raw(db, "SELECT * FROM xtl_account_documents")) == 1


def test_two_authorities_on_the_same_database_cannot_both_write(db):
    r = repo(db)
    authority(r)
    auth_a, _ = restore(repo(db))
    auth_b, _ = restore(repo(db))
    assert auth_a.authorize_and_reserve(request(risk="400", thesis_id="A1")).authorized
    before = auth_b.lifecycle_state()
    with pytest.raises(PersistenceCommitError):
        auth_b.authorize_and_reserve(request(risk="400", thesis_id="B1"))
    assert auth_b.lifecycle_state() is before
    final, _ = restore(repo(db))
    assert [r.child_trade_id for r in final.state().reservations] == ["A1:p1"]


@pytest.mark.parametrize("expected,new_revision", [
    (None, 1),        # row already exists
    (3, 4),           # revision never reached
    (2, 3),           # ahead of the stored revision
    (0, 1),           # invalid expected revision
    ("1", 2),         # wrong type
    (1, 3),           # correct expected revision but a non-consecutive new revision
])
def test_stale_or_invalid_expected_revision_is_refused(db, expected, new_revision):
    r = repo(db)
    authority(r)                                   # stores p1 at revision 1
    doc = r.load("p1")
    before = r.snapshot()
    with pytest.raises(RepositoryConflictError):
        r.save(StoredDocument("p1", new_revision, doc.payload), expected)
    assert r.snapshot() == before                  # durable state untouched
    assert r.load("p1") == doc


# ---- 3/8. atomicity and failure injection ----------------------------------------------------------------

class InjectingRepository(SQLiteReferenceRepository):
    def __init__(self, path, stage=None):
        super().__init__(path)
        self.stage = stage

    def _fail(self, stage):
        if self.stage == stage:
            raise OSError(f"injected failure {stage}")

    def _before_transaction(self):
        self._fail("before_transaction")

    def _before_commit(self):
        self._fail("during_transaction")

    def _after_commit(self):
        self._fail("after_commit")


@pytest.mark.parametrize("stage", ["before_transaction", "during_transaction"])
def test_failure_before_durable_commit_rolls_back_completely(db, stage):
    r = InjectingRepository(db)
    auth = authority(r)
    reserve(auth, "100", "OK")
    durable_before, memory_before = r.snapshot(), auth.lifecycle_state()

    r.stage = stage
    with pytest.raises(PersistenceCommitError):
        auth.authorize_and_reserve(request(risk="100", thesis_id="LOST"))
    assert r.snapshot() == durable_before                       # no partial row, no new revision
    assert auth.lifecycle_state() is memory_before              # no in-memory swap
    r.stage = None
    restored, _ = restore(repo(db))
    assert [x.child_trade_id for x in restored.state().reservations] == ["OK:p1"]
    assert restored.authorize_and_reserve(request(risk="100", thesis_id="LOST")).authorized


def test_crash_after_durable_commit_before_memory_swap_recovers_forward(db):
    r = InjectingRepository(db)
    auth = authority(r)
    reserve(auth, "100", "OK")
    r.stage = "after_commit"
    with pytest.raises(PersistenceCommitError):
        auth.authorize_and_reserve(request(risk="500", thesis_id="FORWARD"))
    assert len(auth.state().reservations) == 1                   # stale memory at revision N
    assert r.load("p1").revision == 3                            # durable is N+1

    restored, report = restore(repo(db))                         # restart recovers FORWARD, not stale memory
    assert report.clean and {x.child_trade_id for x in restored.state().reservations} == {"OK:p1", "FORWARD:p1"}
    assert exposure(restored)[1] == D("600")
    replay = restored.authorize_and_reserve(request(risk="500", thesis_id="FORWARD"))
    assert replay.idempotent_replay


KILL_WRITER = """
import os, sys
sys.path.insert(0, sys.argv[2])
from api.xtl_platform.risk.lifecycle_repository_sqlite import SQLiteReferenceRepository
from api.xtl_platform.risk.persistent_reference import ReferencePersistentLifecycleAuthority
from api.xtl_platform.tests.test_sqlite_repository import authority, reserve

class Killer(SQLiteReferenceRepository):
    def __init__(self, path):
        super().__init__(path)
        self.armed = False
    def _before_commit(self):
        if self.armed:
            os._exit(9)          # hard kill INSIDE the transaction, before COMMIT

repository = Killer(sys.argv[1])
auth = authority(repository)
reserve(auth, "100", "SAFE")
repository.armed = True
reserve(auth, "500", "KILLED")
"""


def test_process_killed_inside_transaction_leaves_previous_revision_intact(db, tmp_path):
    root = str(__import__("pathlib").Path(__file__).resolve().parents[3])
    script = tmp_path / "killer.py"
    script.write_text(KILL_WRITER)
    out = subprocess.run([sys.executable, str(script), db, root], capture_output=True, text=True, timeout=120)
    assert out.returncode == -9 or out.returncode == 9            # died mid-transaction

    r = repo(db)                                                  # SQLite recovers the journal on open
    assert r.load("p1").revision == 2
    restored, report = restore(r)
    assert report.clean
    assert [x.child_trade_id for x in restored.state().reservations] == ["SAFE:p1"]
    assert exposure(restored)[1] == D("100")
    assert restored.authorize_and_reserve(request(risk="500", thesis_id="KILLED")).authorized


# ---- 9. corruption / malformed persisted state ---------------------------------------------------------

CORRUPTIONS = {
    "unsupported_schema_version": (lambda db: rewrite_payload(db, "p1", lambda e: e.update(schema_version=2)),
                                   RC.UNSUPPORTED_SCHEMA_VERSION),
    "malformed_json": (lambda db: raw(db, "UPDATE xtl_account_documents SET payload=?, payload_sha256=? "
                                          "WHERE profile_id='p1'", ("{not json", payload_sha256("{not json"))),
                       RC.MALFORMED_DOCUMENT),
    "missing_account_identity": (lambda db: rewrite_payload(db, "p1", lambda e: e["content"]["account_state"].pop("account")),
                                 RC.INVALID_RECORD),
    "account_identity_mismatch": (lambda db: rewrite_payload(db, "p1", lambda e: e["content"]["account_state"]["account"].update(login="999")),
                                  RC.INVALID_RECORD),
    "envelope_profile_mismatch": (lambda db: rewrite_payload(db, "p1", lambda e: e.update(profile_id="p9")),
                                  RC.PROFILE_MISMATCH),
    "envelope_revision_mismatch": (lambda db: rewrite_payload(db, "p1", lambda e: e.update(revision=99)),
                                   RC.REVISION_MISMATCH),
    "content_digest_mismatch": (lambda db: rewrite_payload(db, "p1", lambda e: e["content"]["account_state"].update(committed_open_risk_usd="0"), fix_content_digest=False),
                                RC.INTEGRITY_CHECK_FAILED),
    "invalid_risk_value": (lambda db: rewrite_payload(db, "p1", lambda e: e["content"]["reservations"][0].update(risk_usd="-5")),
                           RC.INVALID_RECORD),
    "invalid_lifecycle_state": (lambda db: rewrite_payload(db, "p1", lambda e: e["content"]["reservations"][0].update(status="RELEASED")),
                                RC.INVALID_RECORD),
    "duplicate_ownership": (lambda db: rewrite_payload(db, "p1", lambda e: e["content"]["reservations"].append(json.loads(json.dumps(e["content"]["reservations"][0])))),
                            RC.OWNERSHIP_CONFLICT),
}


@pytest.mark.parametrize("name", sorted(CORRUPTIONS))
def test_corrupt_persisted_document_fails_closed(db, name):
    corrupt, expected_code = CORRUPTIONS[name]
    r = repo(db)
    auth = authority(r, None, ("p1", P1), ("p2", P2))
    reserve(auth, "500", "A", profile=P1)
    reserve(auth, "300", "B", profile=P2)
    r.close()
    corrupt(db)

    r2 = repo(db)
    restored, report = restore(r2)
    assert [(x.profile_id, x.code) for x in report.rejections] == [("p1", expected_code)]
    assert report.restored_profiles == ("p2",)                       # corrupt A never touches B
    assert restored.state().ledger("p1") is None
    assert exposure(restored, "p2")[1] == D("300")
    with pytest.raises(QuarantinedAccountError):
        restored.register_account(state("p1", P1))
    assert r2.snapshot() == repo(db).snapshot()                      # restore never repaired or wrote


@pytest.mark.parametrize("problem,setup", [
    ("payload_sha256 does not match stored payload",
     lambda db: raw(db, "UPDATE xtl_account_documents SET payload_sha256=? WHERE profile_id='p1'", ("0" * 64,))),
    ("revision 0 is not an integer >= 1",
     lambda db: raw(db, "UPDATE xtl_account_documents SET revision=0 WHERE profile_id='p1'", ignore_checks=True)),
    ("revision 'two' is not an integer >= 1",
     lambda db: raw(db, "UPDATE xtl_account_documents SET revision='two' WHERE profile_id='p1'", ignore_checks=True)),
])
def test_structurally_corrupt_row_is_reported_not_repaired(db, problem, setup):
    r = repo(db)
    auth = authority(r, None, ("p1", P1), ("p2", P2))
    reserve(auth, "500", "A", profile=P1)
    r.close()
    setup(db)

    r2 = repo(db)
    assert r2.load("p1") is None and r2.load("p2") is not None
    assert r2.inspect() == (StoredRowIssue("p1", problem),)
    restored, report = restore(r2)
    assert report.quarantined_profiles == ("p1",) and report.restored_profiles == ("p2",)
    assert restored.state().ledger("p1") is None


def test_database_check_constraints_reject_invalid_rows(db):
    repo(db)
    for sql, params in [("INSERT INTO xtl_account_documents VALUES ('', 1, 'x', ?)", (payload_sha256("x"),)),
                        ("INSERT INTO xtl_account_documents VALUES ('p9', 0, 'x', ?)", (payload_sha256("x"),)),
                        ("INSERT INTO xtl_account_documents VALUES ('p9', 1, 'x', 'short')", ())]:
        with pytest.raises(sqlite3.IntegrityError):
            raw(db, sql, params)


def test_unsupported_repository_schema_version_refuses_to_open(db):
    repo(db).close()
    raw(db, "UPDATE xtl_repository_meta SET value='99' WHERE key='repository_schema_version'")
    with pytest.raises(RepositorySchemaError, match="unsupported repository_schema_version"):
        repo(db)


def test_documents_without_schema_meta_refuse_to_open(db):
    r = repo(db)
    authority(r)
    r.close()
    raw(db, "DELETE FROM xtl_repository_meta")
    with pytest.raises(RepositorySchemaError, match="without a repository_schema_version"):
        repo(db)


# ---- 10. account isolation -----------------------------------------------------------------------------

def test_account_isolation_across_updates_conflicts_and_restart(db):
    r = repo(db)
    auth = authority(r, None, ("p1", P1), ("p2", P2), ("p3", P3))
    reserve(auth, "500", "A", profile=P1)
    reserve(auth, "300", "B", profile=P2)
    reserve(auth, "200", "C", profile=P3)
    rows = {pid: row for row, *_ in [(x,) for x in r.snapshot()] for pid in [row[0]]}

    reserve(auth, "100", "A2", profile=P1)                                   # update A
    after = {row[0]: row for row in r.snapshot()}
    assert after["p2"] == rows["p2"] and after["p3"] == rows["p3"]            # B and C untouched

    stale = repo(db)
    doc_p1 = rows["p1"]
    with pytest.raises(RepositoryConflictError):                             # CAS conflict on A
        stale.save(StoredDocument("p1", doc_p1[1] + 1, doc_p1[2]), doc_p1[1])
    assert reserve(auth, "50", "B2", profile=P2)                             # B still writable
    restored, report = restore(repo(db))
    assert report.clean and [l.profile_id for l in restored.state().ledgers] == ["p1", "p2", "p3"]
    assert (exposure(restored, "p1")[1], exposure(restored, "p2")[1], exposure(restored, "p3")[1]) == \
           (D("600"), D("350"), D("200"))


def test_repeated_restore_is_idempotent_and_never_writes(db):
    r = repo(db)
    auth = authority(r, None, ("p1", P1), ("p2", P2))
    reserve(auth, "500", "A", profile=P1)
    before_rows = r.snapshot()
    before_file = hashlib.sha256(open(db, "rb").read()).hexdigest()
    states = [restore(repo(db))[0].lifecycle_state() for _ in range(3)]
    assert all(s == states[0] for s in states) and states[0] == auth.lifecycle_state()
    assert repo(db).snapshot() == before_rows
    assert hashlib.sha256(open(db, "rb").read()).hexdigest() == before_file


# ---- 11. Day-6 reconciliation and Day-7 resolution after restart --------------------------------------------

def _resolve_after_restart(db, *objects, pid="p1", profile=P1):
    repository = repo(db)
    restored, report = restore(repository)
    assert report.clean
    evidence = build_reconciliation_evidence(restored.lifecycle_state(), repository.load(pid),
                                             snap(pid, profile, *objects))
    result = resolve_reconciliation(ResolutionRequest(1, "RES-1", evidence, AT), restored.lifecycle_state(),
                                    repository.load(pid), POLICY)
    return restored, evidence, result


def test_CRITICAL_active_500_absent_externally_after_restart_holds_and_releases_nothing(db):
    r = repo(db)
    auth = authority(r)
    res = reserve(auth, "500", "HELD")
    r.close()

    restored, evidence, result = _resolve_after_restart(db)
    assert evidence.report.items[0].outcome.value == "ACTIVE_RESERVATION_NOT_OBSERVED"
    assert (result.decision, result.reason_code) == (RD.HOLD, RRC.UNCERTAINTY_HELD)
    open_risk_before, reserved, committed, slots = exposure(restored)
    assert (reserved, open_risk_before, committed, slots) == (D("500"), D("900"), D("400"), 1)
    assert restored.lifecycle_state().owner_of(res.child_trade_id) is EO.ACTIVE_RESERVATION
    after = exposure(restored)
    assert after == (open_risk_before, reserved, committed, slots)          # available-risk increase = $0
    assert repo(db).load("p1").revision == 2                                # resolution wrote nothing


def test_committed_position_absent_externally_after_restart_is_repair_required(db):
    r = repo(db)
    auth = authority(r)
    res = reserve(auth, "500", "C")
    auth.reference_transition(transition(res, EO.COMMITTED_POSITION))
    r.close()

    restored, evidence, result = _resolve_after_restart(db)
    assert evidence.report.items[0].outcome.value == "COMMITTED_POSITION_EXTERNALLY_MISSING"
    assert (result.decision, result.reason_code) == (RD.REPAIR_REQUIRED, RRC.REPAIR_PLANNED)
    plan = result.repair_plans[0]
    assert plan.required_action.value == "INVESTIGATE_COMMITTED_POSITION_MISSING" and plan.required_transition is None
    assert restored.lifecycle_state().owner_of(res.child_trade_id) is EO.COMMITTED_POSITION
    assert exposure(restored) == (D("900"), D("0"), D("900"), 0)            # nothing released


def test_matched_external_objects_after_restart_resolve_clean(db):
    r = repo(db)
    auth = authority(r)
    res = reserve(auth, "500", "M")
    r.close()
    restored, evidence, result = _resolve_after_restart(db, obj("ORD-M", res.child_trade_id, K.PENDING_ORDER))
    assert (result.decision, result.reason_code) == (RD.ACCEPT, RRC.CLEAN_RECONCILIATION)
    assert exposure(restored)[1] == D("500")


def test_resolution_evidence_binds_the_durable_revision(db):
    r = repo(db)
    auth = authority(r)
    reserve(auth, "500", "HELD")
    r.close()
    restored, evidence, _ = _resolve_after_restart(db)
    assert evidence.state_revision == repo(db).load("p1").revision
    reserve(restored, "100", "MORE")                                        # durable revision moves on
    stale = resolve_reconciliation(ResolutionRequest(1, "RES-2", evidence, AT), restored.lifecycle_state(),
                                   repo(db).load("p1"), POLICY)
    assert (stale.decision, stale.reason_code) == (RD.REJECT, RRC.STALE_EVIDENCE)


# ---- 12/13. no new authority, approved persistence boundary only ---------------------------------------------

def test_repository_creates_no_authority_and_touches_no_forbidden_surface():
    from api.xtl_platform.tests.test_isolation import (
        APPROVED_PERSISTENCE_BOUNDARY, PACKAGE, PKG_ROOT, PRODUCTION_MANIFEST, scan_source,
    )
    rel = "risk/lifecycle_repository_sqlite.py"
    source = (PKG_ROOT / rel).read_text()
    assert rel in PRODUCTION_MANIFEST and APPROVED_PERSISTENCE_BOUNDARY[rel] == frozenset({"sqlite3"})
    violations = [v for v in scan_source(source, PACKAGE + ("risk",)) if "sqlite3" not in v]
    assert violations == []
    tree = ast.parse(source)
    absolute = {n.module for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.level == 0} | \
               {a.name for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names}
    assert {m.split(".")[0] for m in absolute} == {"__future__", "hashlib", "sqlite3", "threading", "dataclasses", "typing"}
    relative = {n.module for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.level}
    assert relative == {"persistence_contracts", "persistence_repository"}   # no lifecycle/resolution imports
    names = {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)} | \
            {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)}
    forbidden = {"reference_transition", "authorize_and_reserve", "register_account", "replace_account_state",
                 "reference_reconcile", "resolve_reconciliation", "reconcile_account", "decode_account_document",
                 "encode_account_document", "Thread", "Timer", "now", "utcnow", "uuid4", "random", "environ",
                 "getenv", "sleep", "connect_broker", "urlopen"}
    assert names.isdisjoint(forbidden), names & forbidden
    assert "REFERENCE ONLY - NON-PRODUCTION" in source
    assert SQLiteReferenceRepository.GRANTS_PRODUCTION_AUTHORITY is False
    assert issubclass(SQLiteReferenceRepository, LifecycleStateRepository)
    public = {n for n in dir(SQLiteReferenceRepository) if not n.startswith("_")}
    assert public == {"load", "save", "list_profiles", "close", "inspect", "snapshot", "GRANTS_PRODUCTION_AUTHORITY"}


def test_sqlite_repository_satisfies_the_frozen_day6_port(db):
    """Same contract as the frozen in-memory reference repository."""
    from api.xtl_platform.risk.persistence_repository import InMemoryReferenceRepository
    for repository in (InMemoryReferenceRepository(), repo(db)):
        doc = StoredDocument("pX", 1, '{"a":1}')
        repository.save(doc, None)
        assert repository.load("pX") == doc and repository.list_profiles() == ("pX",)
        for bad, expected in [(StoredDocument("pX", 1, "y"), None), (StoredDocument("pX", 3, "y"), 1),
                              (StoredDocument("pX", 2, "y"), 2)]:
            with pytest.raises(RepositoryConflictError):
                repository.save(bad, expected)
        assert repository.load("pX") == doc
        repository.save(StoredDocument("pX", 2, '{"a":2}'), 1)
        assert repository.load("pX").revision == 2


FROZEN_DAY7_SHA256 = {
    "__init__.py": "02d6d351df2fe8a60273633192ada8dc1ed482affa734cea8280b7e2878fe8a7",
    "accounts/__init__.py": "6c76451e207cd2b955c7ca4de5faea9c3eab967a8ef9f09599eb83a30b610985",
    "accounts/router.py": "4d5d5d502618fee36af8d83292aaa9a35f163ba0ced9c6f1af704589245b5c7e",
    "domain/__init__.py": "0a43ecd7049cce4401ea30a8063347981017507ab5b57b979810d40a63a9a5cf",
    "domain/_validation.py": "336093dfe4ead10ab4cb6dfa190beaee721d24f3663289dbaaf015aba4a5e954",
    "domain/account.py": "508f52ddf029f01acced6c8a71cec78f7790f46c5c6b8914ec5049c44632115d",
    "domain/enums.py": "ea3cda9e4c04721e031d98830ed6438887ed83feeda403789c191f0bd5a36ef3",
    "domain/execution.py": "fbade6a80d8a9f1262e1c0b9afea221e698b9f168b5b75eadfd0134f444a7e7f",
    "domain/policy.py": "cdf38157a98c8d8e75f6d8eff058327b46d8b4c6447d7cc01a227a1e41396ca4",
    "domain/risk.py": "2766041402a59092db27edf6c5b9426f17ae8c61020dbce14821a8634c19b456",
    "domain/thesis.py": "48bcb6726ef4e2d9e3645c5725fe35a11bf2afae4326d16e8768b333d0700613",
    "execution/__init__.py": "6c827aa331a1adfdddc52edb498e62e79f8a89052010d8e601396717575f0f12",
    "execution/adapter.py": "6e8f540d4dd6c238a0b47a7e4044fd647a6d25a14c0ec47c3613d6f4a7dd6da7",
    "policy/__init__.py": "9d52062614420e0cfc35bea80efc691cdf5e08228ad4b520373dfd66983a8340",
    "policy/base.py": "c9abb9ecefa39dd98f6fcade713f7bae1bea692f7c594dbcbdafbfe95154864e",
    "policy/evaluator.py": "ba63054b8507c59436db83983f653aae5e4204be68e9c0e8752885a5115cf084",
    "policy/registry.py": "f51fcb5c035f1209e6a803e93fb0c6f6008e592bf412200c13524ef746e66a4f",
    "policy/validation.py": "64486e7676e83cb3462eeab486a490a9005b16c3da6478fa22d49a70db5417f4",
    "pytest.ini": "eed4f531dfeafadd83b86e0284bc9aff7d5dd921d7bec7fa3bf5f657d5558468",
    "risk/__init__.py": "2827bca897035f668bc73204e3e66af46550867ad13f5a83e1ee7aff41191305",
    "risk/atomic_contracts.py": "615a8046af917d620120c95b53205c02a0e4d48b5f7a4817ce043d14d88d3158",
    "risk/atomic_reference.py": "6c7c90934233971318383b20d29d40b16360a6b3c109f67234c8b2f44d22f86b",
    "risk/authority.py": "b0df075a1617389936823f713a7fbbe978831307531b4706e7ee21419af1f0a7",
    "risk/calculator.py": "ac12bc2ee316f4b0519c09740d4b27744f55fcc2969ca9438382733ebff6b603",
    "risk/contracts.py": "ece8a4061eb89fdca370184785540604d16b61599639e9a6e696218a269e0da1",
    "risk/evaluator.py": "c2cfcf8cab8877f7528d50338c12b45443c340eb2cd33a91ba56c109e88f8201",
    "risk/lifecycle_contracts.py": "003f4a77f55603bc34f7f6435f38b70490517aa4c6e9fa57298390f7c5146e68",
    "risk/lifecycle_reference.py": "6bce5333310eb26f029283ad15b59659ff6881c49d9aca2bd1002b207863916a",
    "risk/persistence_codec.py": "2a41b9bcd44d0fafd8ba1a596898a04c64b080e3c9e303bc454505a200fc3c8a",
    "risk/persistence_contracts.py": "5b9db23241ae491fa635fcdb79f38ba032f554ddcd91cbbbe970533c4b384120",
    "risk/persistence_repository.py": "f982c1d613fc205144f1624e9221e464d00bdf71eb49f140b71ee8b7e70077e5",
    "risk/persistent_reference.py": "f9a2ee0090a2b2c073492c0072693dc35b250bbf364152738c970da0b0ad8c7b",
    "risk/reconciliation_contracts.py": "7c9b6dbc6831da43e210c712924c248e87897c2d093f39d7e5a243a6af872a67",
    "risk/reconciliation_reference.py": "1b35e7fccd1754bc76921330a6c9cd6558303e61e752a5346b8e19376537516d",
    "risk/reference_test_only.py": "3edd3c151fe49ad264fae52986ae91180a71ba46e6adfe9c640c50e293eca9e7",
    "risk/resolution_contracts.py": "e9955ad8f9aa44638b989bfa5836fd70cf395c1fca81e5a8e05bd3036ec55936",
    "risk/resolution_reference.py": "6d0ecf2953b250bb5114f77db81860f05a1d04acdeabd31f7d7454da3a435a01",
    "risk/validation.py": "ddea557444c5e8513fa93631cafb7a1591fabd2959e4f880825eee7c90425bad",
    "tests/__init__.py": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    "tests/_builders.py": "4bf536e11dac33ef9f5dff0c176e55abcf6f85d96e8ce12a59e7207d6255a51a",
    "tests/conftest.py": "0893751f3d47a26d26e4294c8a348c78393cee79e133f6d5bc1ef2c2e6e6b79f",
    "tests/test_account_identity.py": "2165ec6dcca34a73b96c8555bddfffe4cde7f4be42e7167ed7db2239bb4b61e0",
    "tests/test_atomic_reference.py": "cb07e69fc4aca82137bacf3d95d25ae8a11672186163ee24dbd8e3662e25e57f",
    "tests/test_domain_contracts.py": "a64712d0114904d8d3aa308bfbbd834cf8286144ac3744381dfee47a961c0ee3",
    "tests/test_lifecycle_reference.py": "629424f41bccc957908b0014370e1e87c021f1351e37960c0ee21e0c4da3f366",
    "tests/test_lifecycle_state_feed_guard.py": "b72ee921da038006e76dedbf0034e255a2229c0e3bb398ff42c04fc7b37f3ffe",
    "tests/test_persistence_reconciliation.py": "0a6a8928f7d2d197ddc80641d5713df047b03a56eecee47cca6fb6011848a85d",
    "tests/test_policy_engine.py": "791cb3b9c0c314e4773fd3ea69c33bba1c0fcab9f318728a61aaefd2cca34187",
    "tests/test_position_slots.py": "19e3c93b7812d57e9e71e4eb63b87145f98fbffae297a330b71804d29d1057fd",
    "tests/test_resolution_authority.py": "cf66d14cf694fe02051d701673a615f9c0ccc70f7ee10e889ae0d73dcace505b",
    "tests/test_risk_evaluator.py": "e64c800d1344cad6d0469eebf323b61d7963e318ade8cf924921d066e2929d82",
    "tests/test_thesis_fanout.py": "1fb20203b692d95c6f45054b804f051b6e840ab0079ca4cf4c6fe9d607ba4ea3",
}


def test_frozen_day1_to_day7_files_unchanged():
    from api.xtl_platform.tests.test_isolation import PKG_ROOT
    changed = [rel for rel, digest in FROZEN_DAY7_SHA256.items()
               if hashlib.sha256((PKG_ROOT / rel).read_bytes()).hexdigest() != digest]
    assert changed == [] and len(FROZEN_DAY7_SHA256) == 52
