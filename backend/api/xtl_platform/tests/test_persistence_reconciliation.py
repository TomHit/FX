"""Day 6 - restart-safe lifecycle persistence and reconciliation.

REFERENCE ONLY - NON-PRODUCTION. No Redis, database, file, network, MT5 or broker.
Tests prefixed R## map to the Day-6 required test list; C## are crash boundaries.
All limit values are TEST FIXTURE - NOT VERIFIED PRODUCTION FIRM RULE.
"""
from __future__ import annotations

import ast
import dataclasses
import hashlib
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from api.xtl_platform.domain.account import AccountIdentity
from api.xtl_platform.domain.enums import Side, StrategyId
from api.xtl_platform.risk.atomic_contracts import AtomicAuthorizationCode as A
from api.xtl_platform.risk.atomic_reference import derive_evaluation_snapshot
from api.xtl_platform.risk.lifecycle_contracts import CommittedExposureReplacementBlocked, ExposureOwner as O
from api.xtl_platform.risk.persistence_codec import canonical_json, content_digest, decode_account_document
from api.xtl_platform.risk.persistence_contracts import (
    SCHEMA_NAME, PersistenceCommitError, PersistenceDecodeError, QuarantinedAccountError,
    RepositoryConflictError, RestoreRejectionCode as RC, StoredDocument,
)
from api.xtl_platform.risk.persistence_repository import InMemoryReferenceRepository, LifecycleStateRepository
from api.xtl_platform.risk.persistent_reference import ReferencePersistentLifecycleAuthority
from api.xtl_platform.risk.reconciliation_contracts import (
    ExternalAccountSnapshot, ExternalObject, ExternalObjectKind as K, ReconciliationClass,
    ReconciliationOutcome as RO, ReconciliationReport, ReconciliationStatus as RS,
)
from api.xtl_platform.tests._builders import NOW, D, make_profile
from api.xtl_platform.tests.test_atomic_reference import CONFIG, P1, P2, FakeClock, account_state, request
from api.xtl_platform.tests.test_lifecycle_reference import transition

P3 = make_profile("p3")
LIMITS = dict(max_open_risk_usd=D("1000"), max_positions=10)


# ---- builders ---------------------------------------------------------------------------------

def state(pid="p1", profile=None, **kw):
    kw.setdefault("committed_open_risk_usd", D("400"))
    kw.setdefault("committed_open_positions", 2)
    return account_state(pid, profile or make_profile(pid), limit_kw=LIMITS, **kw)


def fresh(repo=None, clock=None, *profiles):
    repo = repo if repo is not None else InMemoryReferenceRepository()
    auth = ReferencePersistentLifecycleAuthority(CONFIG, clock or FakeClock(), repo,
                                                 acknowledge_reference_only_non_production=True)
    for pid in (profiles or ("p1",)):
        auth.register_account(state(pid))
    return auth, repo


def restore(repo, clock=None):
    return ReferencePersistentLifecycleAuthority.restore(CONFIG, clock or FakeClock(), repo,
                                                         acknowledge_reference_only_non_production=True)


def reserve(auth, risk="100", thesis_id="A-1", strategy=StrategyId.STRATEGY_A, profile=P1):
    r = auth.authorize_and_reserve(request(risk=risk, thesis_id=thesis_id, strategy=strategy, profile=profile))
    assert r.authorized, r.reason
    return r.reservation


def capacity(auth, pid="p1"):
    ledger = auth.state().ledger(pid)
    s = derive_evaluation_snapshot(ledger)
    return (s.open_risk_usd, s.open_positions, s.reserved_position_slots,
            ledger.account_state.committed_open_risk_usd, len(ledger.reservations))


def populated():
    """p1: ACTIVE(A), COMMITTED(B), RELEASED(C); p2: ACTIVE(D); p3: COMMITTED(E). Interleaved across accounts."""
    auth, repo = fresh(None, None, "p1", "p2", "p3")
    a = reserve(auth, "100", "A", StrategyId.STRATEGY_A, P1)
    b = reserve(auth, "150", "B", StrategyId.STRATEGY_B, P1)
    e = reserve(auth, "120", "E", StrategyId.STRATEGY_A, P3)
    auth.reference_transition(transition(e, O.COMMITTED_POSITION))
    auth.reference_transition(transition(b, O.COMMITTED_POSITION))
    c = reserve(auth, "50", "C", StrategyId.STRATEGY_B, P1)
    d = reserve(auth, "200", "D", StrategyId.STRATEGY_B, P2)
    auth.reference_transition(transition(c, O.RELEASED))
    return auth, repo, dict(a=a, b=b, c=c, d=d, e=e)


def tamper(repo, pid, mutate, fix_digest=True):
    doc = repo.load(pid)
    env = json.loads(doc.payload)
    mutate(env)
    env["revision"] = doc.revision + 1 if env.get("revision") == doc.revision else env.get("revision")
    if fix_digest and isinstance(env.get("content"), dict):
        env["content_sha256"] = content_digest(env["content"])
    repo.save(StoredDocument(pid, doc.revision + 1, canonical_json(env) if isinstance(env, dict) else str(env)),
              doc.revision)


def raw_save(repo, pid, payload):
    doc = repo.load(pid)
    repo.save(StoredDocument(pid, (doc.revision if doc else 0) + 1, payload), doc.revision if doc else None)


def snap(pid="p1", profile=P1, *objects, observed_at=NOW):
    return ExternalAccountSnapshot(pid, profile.identity, observed_at, tuple(objects))


def obj(external_id, child, kind=K.POSITION, symbol="XAUUSD", side=Side.BUY):
    return ExternalObject(external_id, kind, symbol, side, child)


# ---- R1 / R15 persist -> restart -> restore ---------------------------------------------------------

def test_R01_normal_persist_restart_restore():
    auth, repo, ids = populated()
    restored, report = restore(repo)
    assert report.clean and report.restored_profiles == ("p1", "p2", "p3")
    assert restored.lifecycle_state() == auth.lifecycle_state()          # identical ledgers, reservations, records, order
    assert repr(restored.lifecycle_state()) == repr(auth.lifecycle_state())
    assert restored.lifecycle_state().owner_of(ids["a"].child_trade_id) is O.ACTIVE_RESERVATION
    assert restored.lifecycle_state().owner_of(ids["b"].child_trade_id) is O.COMMITTED_POSITION


def test_R15_risk_and_capacity_totals_identical_before_and_after_clean_restart():
    auth, repo, _ = populated()
    restored, _ = restore(repo)
    for pid in ("p1", "p2", "p3"):
        assert capacity(restored, pid) == capacity(auth, pid)
    assert capacity(restored, "p1") == (D("650"), 3, 1, D("550"), 1)   # 400+150 committed, +100 active; C released


def test_R01b_restored_authority_continues_with_restored_capacity():
    auth, repo = fresh()
    reserve(auth, "600", "BIG")                                          # 400 committed + 600 = 1000 limit
    restored, _ = restore(repo)
    assert restored.authorize_and_reserve(request(risk="1", thesis_id="MORE")).code is A.BLOCK_OPEN_RISK
    replay = restored.authorize_and_reserve(request(risk="600", thesis_id="BIG"))
    assert replay.idempotent_replay and len(restored.state().reservations) == 1


# ---- R2 / R3 / R17 accounts and strategies -----------------------------------------------------------

def test_R02_multiple_independent_accounts():
    auth, repo, _ = populated()
    restored, _ = restore(repo)
    assert [l.profile_id for l in restored.state().ledgers] == ["p1", "p2", "p3"]
    assert capacity(restored, "p2") == (D("600"), 2, 1, D("400"), 1)
    assert capacity(restored, "p3") == (D("520"), 3, 0, D("520"), 0)
    assert len(repo.list_profiles()) == 3


def test_R03_multiple_strategies_and_accounts_share_per_account_capacity_after_restore():
    auth, repo, ids = populated()
    restored, _ = restore(repo)
    ledger = restored.state().ledger("p1")
    strategies = {r.strategy_id for r in ledger.reservations} |                  {r.strategy_id for r in restored.lifecycle_state().records if r.profile_id == "p1"}
    assert strategies == {StrategyId.STRATEGY_A, StrategyId.STRATEGY_B}
    # One shared pool per account: 650 used on p1 -> 351 blocks, 350 fits
    assert restored.authorize_and_reserve(request(risk="351", thesis_id="X", strategy=StrategyId.STRATEGY_B)).code is A.BLOCK_OPEN_RISK
    assert restored.authorize_and_reserve(request(risk="350", thesis_id="Y", strategy=StrategyId.STRATEGY_A)).authorized


def test_R17_one_accounts_recovery_cannot_alter_another():
    auth, repo, _ = populated()
    p1_before, p3_before = repo.load("p1"), repo.load("p3")
    tamper(repo, "p2", lambda env: env["content"]["account_state"].update(balance=1.5))   # corrupt p2 only
    restored, report = restore(repo)
    assert report.restored_profiles == ("p1", "p3") and report.quarantined_profiles == ("p2",)
    assert restored.state().ledger("p1") == auth.state().ledger("p1")
    assert restored.state().ledger("p3") == auth.state().ledger("p3")
    assert restored.state().ledger("p2") is None
    assert repo.load("p1") == p1_before and repo.load("p3") == p3_before       # restore never writes
    # p1 keeps working and persisting; p2 stays quarantined and cannot be re-created
    reserve(restored, "10", "NEW", profile=P1)
    assert repo.load("p1").revision == p1_before.revision + 1
    with pytest.raises(QuarantinedAccountError):
        restored.register_account(state("p2"))
    assert restored.authorize_and_reserve(request(risk="1", thesis_id="Q", profile=P2)).code is A.BLOCK_ACCOUNT_MISMATCH
    assert restored.reference_reconcile(snap("p2", P2)).status is RS.REJECTED


# ---- R4 / R5 / R16 idempotency ------------------------------------------------------------------------

def test_R04_duplicate_restore_is_idempotent():
    auth, repo, _ = populated()
    before = repo.snapshot()
    (r1, rep1), (r2, rep2) = restore(repo), restore(repo)
    assert r1.lifecycle_state() == r2.lifecycle_state() == auth.lifecycle_state()
    assert rep1 == rep2 and repo.snapshot() == before
    assert len(r2.state().reservations) == len(auth.state().reservations) == 2


def test_R05_R16_duplicate_and_repeated_reconciliation_identical_state_and_result():
    auth, repo, ids = populated()
    restored, _ = restore(repo)
    s = snap("p1", P1, obj("POS-B", ids["b"].child_trade_id), obj("ORD-A", ids["a"].child_trade_id, K.PENDING_ORDER))
    state_before, repo_before = restored.lifecycle_state(), repo.snapshot()
    reports = [restored.reference_reconcile(s) for _ in range(5)]
    assert all(r == reports[0] for r in reports)
    assert restored.lifecycle_state() is state_before and repo.snapshot() == repo_before
    assert restore(repo)[0].reference_reconcile(s) == reports[0]            # identical after another restart
    assert reports[0].status is RS.CLEAN


# ---- R6 / R7 / R8 / R9 reconciliation ------------------------------------------------------------------

def test_R06_persisted_active_reservation_with_matching_external_object():
    auth, repo, ids = populated()
    restored, _ = restore(repo)
    child = ids["a"].child_trade_id
    r = restored.reference_reconcile(snap("p1", P1, obj("ORD-A", child, K.PENDING_ORDER),
                                          obj("POS-B", ids["b"].child_trade_id)))
    item = next(i for i in r.items if i.child_trade_id == child)
    assert item.outcome is RO.MATCHED_ACTIVE_RESERVATION_PENDING_ORDER and item.external_ids == ("ORD-A",)
    assert capacity(restored) == capacity(auth)


def test_R07_persisted_ownership_missing_externally_holds_capacity():
    auth, repo, ids = populated()
    restored, _ = restore(repo)
    before = capacity(restored)
    r = restored.reference_reconcile(snap("p1", P1))
    outcomes = {i.child_trade_id: i.outcome for i in r.items}
    assert outcomes[ids["a"].child_trade_id] is RO.ACTIVE_RESERVATION_NOT_OBSERVED
    assert outcomes[ids["b"].child_trade_id] is RO.COMMITTED_POSITION_EXTERNALLY_MISSING
    assert outcomes[ids["c"].child_trade_id] is RO.MATCHED_RELEASED_ABSENT
    assert r.status is RS.UNRESOLVED and capacity(restored) == before
    assert ReconciliationReport.RELEASES_CAPACITY is False and ReconciliationReport.AUTHORIZES_RISK is False


def test_R08_external_object_with_no_internal_owner():
    auth, repo, ids = populated()
    restored, _ = restore(repo)
    r = restored.reference_reconcile(snap("p1", P1, obj("POS-B", ids["b"].child_trade_id), obj("ORD-A", ids["a"].child_trade_id, K.PENDING_ORDER),
                                          obj("MANUAL-1", None), obj("GHOST-2", "UNKNOWN:p1")))
    unowned = [i for i in r.items if i.outcome is RO.EXTERNAL_OBJECT_UNOWNED]
    assert [i.external_ids for i in unowned] == [(("MANUAL-1",)), ("GHOST-2",)] or            sorted(i.external_ids for i in unowned) == [("GHOST-2",), ("MANUAL-1",)]
    assert r.status is RS.CONFLICT and all(i.internal_owner is None for i in unowned)
    assert capacity(restored) == capacity(auth)


@pytest.mark.parametrize("case,outcome", [
    ("duplicate_external", RO.DUPLICATE_EXTERNAL_OWNERSHIP),
    ("other_account", RO.EXTERNAL_OBJECT_OWNED_BY_OTHER_ACCOUNT),
    ("released_present", RO.RELEASED_BUT_EXTERNALLY_PRESENT),
    ("committed_as_order", RO.COMMITTED_POSITION_OBSERVED_AS_PENDING_ORDER),
    ("side_mismatch", RO.OWNERSHIP_ATTRIBUTE_MISMATCH),
    ("symbol_mismatch", RO.OWNERSHIP_ATTRIBUTE_MISMATCH),
    ("active_as_position", RO.ACTIVE_RESERVATION_OBSERVED_AS_POSITION),
])
def test_R09_conflicting_ownership_reconciliation_outcomes(case, outcome):
    auth, repo, ids = populated()
    restored, _ = restore(repo)
    a, b, c, d = (ids[k].child_trade_id for k in "abcd")
    objects = {
        "duplicate_external": [obj("P1", b), obj("P2", b)],
        "other_account": [obj("X", d)],
        "released_present": [obj("X", c)],
        "committed_as_order": [obj("X", b, K.PENDING_ORDER)],
        "side_mismatch": [obj("X", b, side=Side.SELL)],
        "symbol_mismatch": [obj("X", b, symbol="EURUSD")],
        "active_as_position": [obj("X", a)],
    }[case]
    before = restored.lifecycle_state()
    r = restored.reference_reconcile(snap("p1", P1, *objects))
    assert outcome in {i.outcome for i in r.items}
    expected = RS.UNRESOLVED if case == "active_as_position" else RS.CONFLICT
    assert r.status in {expected, RS.CONFLICT}
    assert restored.lifecycle_state() is before


def test_R09b_forged_foreign_ownership_rejects_only_the_forger():
    """p2's document forges p1's ACTIVE reservation. p2 is rejected; p1's authoritative
    document is intact, so p1 restores with exactly one reservation (no duplicate)."""
    auth, repo, ids = populated()
    p1_reservation = json.loads(repo.load("p1").payload)["content"]["reservations"][0]
    tamper(repo, "p2", lambda env: env["content"]["reservations"].append(json.loads(json.dumps(p1_reservation))))
    restored, report = restore(repo)
    assert [(r.profile_id, r.code) for r in report.rejections] == [("p2", RC.INVALID_RECORD)]
    assert restored.state().ledger("p1") == auth.state().ledger("p1")
    replay = restored.authorize_and_reserve(request(risk="100", thesis_id="A"))
    assert replay.idempotent_replay and len(restored.state().ledger("p1").reservations) == 1


def test_R09c_duplicate_active_ownership_within_one_document_rejected():
    auth, repo, ids = populated()

    def dup(env):
        content = env["content"]
        content["reservations"].append(json.loads(json.dumps(content["lifecycle_records"][0]["reservation"])))

    tamper(repo, "p1", dup)                                   # child is both ACTIVE and COMMITTED in p1
    _, report = restore(repo)
    assert [(r.profile_id, r.code) for r in report.rejections] == [("p1", RC.OWNERSHIP_CONFLICT)]


def test_R09d_same_account_identity_in_two_documents_quarantines_both():
    auth, repo = fresh(None, None, "p1", "p2")
    p1_account = json.loads(repo.load("p1").payload)["content"]["account_state"]["account"]
    tamper(repo, "p2", lambda env: env["content"]["account_state"].update(account=p1_account))
    restored, report = restore(repo)
    assert {(r.profile_id, r.code) for r in report.rejections} == {("p1", RC.OWNERSHIP_CONFLICT),
                                                                   ("p2", RC.OWNERSHIP_CONFLICT)}
    assert restored.state().ledgers == () and report.restored_profiles == ()
    for pid in ("p1", "p2"):
        with pytest.raises(QuarantinedAccountError):
            restored.register_account(state(pid))


def test_R09e_same_transition_id_in_two_documents_quarantines_both_and_spares_others():
    auth, repo = fresh(None, None, "p1", "p2", "p3")
    a = reserve(auth, "100", "A", profile=P1)
    b = reserve(auth, "100", "B", profile=P2)
    auth.reference_transition(transition(a, O.COMMITTED_POSITION, transition_id="SHARED"))
    auth.reference_transition(transition(b, O.RELEASED, transition_id="OTHER"))
    reserve(auth, "100", "C", profile=P3)
    tamper(repo, "p2", lambda env: env["content"]["lifecycle_records"][0].update(transition_id="SHARED"))
    restored, report = restore(repo)
    assert {(r.profile_id, r.code) for r in report.rejections} == {("p1", RC.OWNERSHIP_CONFLICT),
                                                                   ("p2", RC.OWNERSHIP_CONFLICT)}
    assert report.restored_profiles == ("p3",)
    assert restored.state().ledger("p3") == auth.state().ledger("p3")


# ---- R10 / R11 / R12 malformed, unsupported, corrupt ---------------------------------------------------

MALFORMED = {
    "decimal_as_float": lambda env: env["content"]["account_state"].update(balance=100000.0),
    "nan_decimal": lambda env: env["content"]["account_state"].update(equity="NaN"),
    "negative_positions": lambda env: env["content"]["account_state"].update(committed_open_positions=-1),
    "negative_committed_risk": lambda env: env["content"]["account_state"].update(committed_open_risk_usd="-1"),
    "zero_balance": lambda env: env["content"]["account_state"].update(balance="0"),
    "unsupported_limit_model": lambda env: env["content"]["account_state"]["limits"].update(daily_loss_model="TRAILING_EOD"),
    "bool_as_int": lambda env: env["content"]["account_state"].update(account_halted=0),
    "naive_datetime": lambda env: env["content"]["account_state"].update(as_of="2026-09-16T11:59:55"),
    "unknown_enum": lambda env: env["content"]["reservations"][0]["intent"].update(side="LONG"),
    "missing_key": lambda env: env["content"]["reservations"][0].pop("risk_usd"),
    "unexpected_key": lambda env: env["content"]["reservations"][0].update(open_risk_usd="1"),
    "zero_risk": lambda env: env["content"]["reservations"][0].update(risk_usd="0"),
    "bad_status": lambda env: env["content"]["reservations"][0].update(status="RELEASED"),
    "record_bad_owner": lambda env: env["content"]["lifecycle_records"][0].update(to_owner="ACTIVE_RESERVATION"),
    "record_before_creation": lambda env: env["content"]["lifecycle_records"][0].update(transitioned_at="2020-01-01T00:00:00+00:00"),
    "evaluation_not_pass": lambda env: env["content"]["reservations"][0]["authorizing_evaluation"].update(code="BLOCK_OPEN_RISK"),
    "negative_sequence": lambda env: env["content"]["lifecycle_records"][0].update(sequence=-5),
}


@pytest.mark.parametrize("name", sorted(MALFORMED))
def test_R10_malformed_record_rejected_and_quarantined(name):
    auth, repo, _ = populated()
    tamper(repo, "p1", MALFORMED[name])
    restored, report = restore(repo)
    assert report.quarantined_profiles == ("p1",)
    assert report.rejections[0].code in {RC.INVALID_RECORD, RC.OWNERSHIP_CONFLICT}
    assert restored.state().ledger("p1") is None and report.restored_profiles == ("p2", "p3")


@pytest.mark.parametrize("version", [0, 2, "1", None, 1.0])
def test_R11_unsupported_schema_version_rejected(version):
    auth, repo, _ = populated()
    tamper(repo, "p1", lambda env: env.update(schema_version=version))
    _, report = restore(repo)
    assert [(r.profile_id, r.code) for r in report.rejections] == [("p1", RC.UNSUPPORTED_SCHEMA_VERSION)]


@pytest.mark.parametrize("corruption,code", [
    ("truncated_json", RC.MALFORMED_DOCUMENT),
    ("not_json", RC.MALFORMED_DOCUMENT),
    ("json_array", RC.MALFORMED_DOCUMENT),
    ("wrong_schema_name", RC.MALFORMED_DOCUMENT),
    ("digest_mismatch", RC.INTEGRITY_CHECK_FAILED),
    ("content_edit_without_digest", RC.INTEGRITY_CHECK_FAILED),
    ("envelope_revision_mismatch", RC.REVISION_MISMATCH),
    ("envelope_profile_mismatch", RC.PROFILE_MISMATCH),
    ("partial_content", RC.INVALID_RECORD),
])
def test_R12_partial_or_corrupt_state_rejected(corruption, code):
    auth, repo, _ = populated()
    good = repo.load("p1").payload
    if corruption == "truncated_json":
        raw_save(repo, "p1", good[: len(good) // 2])
    elif corruption == "not_json":
        raw_save(repo, "p1", "\x00garbage")
    elif corruption == "json_array":
        raw_save(repo, "p1", "[]")
    elif corruption == "wrong_schema_name":
        tamper(repo, "p1", lambda env: env.update(schema="something.else"))
    elif corruption == "digest_mismatch":
        tamper(repo, "p1", lambda env: env.update(content_sha256="0" * 64), fix_digest=False)
    elif corruption == "content_edit_without_digest":
        tamper(repo, "p1", lambda env: env["content"]["account_state"].update(committed_open_risk_usd="0"), fix_digest=False)
    elif corruption == "envelope_revision_mismatch":
        doc = repo.load("p1")
        env = json.loads(doc.payload)
        env["revision"] = 99
        raw_save(repo, "p1", canonical_json(env))
    elif corruption == "envelope_profile_mismatch":
        tamper(repo, "p1", lambda env: env.update(profile_id="p9"))
    elif corruption == "partial_content":
        tamper(repo, "p1", lambda env: env["content"].pop("lifecycle_records"))
    restored, report = restore(repo)
    assert [(r.profile_id, r.code) for r in report.rejections] == [("p1", code)]
    assert restored.state().ledger("p1") is None
    with pytest.raises(QuarantinedAccountError):
        restored.register_account(state("p1"))


# ---- R13 / R14 terminal and released ----------------------------------------------------------------------

def test_R13_terminal_committed_state_restoration():
    auth, repo, ids = populated()
    restored, _ = restore(repo)
    b = ids["b"].child_trade_id
    record = restored.lifecycle_state().record_for(b)
    assert record == auth.lifecycle_state().record_for(b) and record.to_owner is O.COMMITTED_POSITION
    replay = restored.reference_transition(transition(ids["b"], O.COMMITTED_POSITION))
    assert replay.applied and replay.idempotent_replay
    assert restored.reference_transition(transition(ids["b"], O.RELEASED)).code.value == "BLOCK_INVALID_TRANSITION"
    with pytest.raises(CommittedExposureReplacementBlocked):                 # Day-5 Correction-1 guard survives restart
        restored.replace_account_state(restored.state().ledger("p1").account_state)


def test_R14_released_reservation_not_resurrected():
    auth, repo, ids = populated()
    restored, _ = restore(repo)
    c = ids["c"]
    assert restored.lifecycle_state().owner_of(c.child_trade_id) is O.RELEASED
    assert restored.state().reservation_for(c.child_trade_id) is None
    again = restored.authorize_and_reserve(request(risk="50", thesis_id="C", strategy=StrategyId.STRATEGY_B))
    assert again.code is A.DUPLICATE_TRADE_ID and again.reservation is None
    assert restored.reference_transition(transition(c, O.COMMITTED_POSITION)).code.value == "BLOCK_INVALID_TRANSITION"
    assert capacity(restored) == capacity(auth)


# ---- crash boundaries ---------------------------------------------------------------------------------------

class CrashingRepository(InMemoryReferenceRepository):
    """mode 'before': raise before storing. mode 'after': store, then raise (process died before swap)."""

    def __init__(self):
        super().__init__()
        self.mode = None

    def save(self, document, expected_revision):
        if self.mode == "before":
            raise OSError("crash before write")
        super().save(document, expected_revision)
        if self.mode == "after":
            raise OSError("crash after write")


def test_C01_crash_after_reservation_persisted_restart_replay_is_idempotent():
    repo = CrashingRepository()
    auth, _ = fresh(repo)
    req = request(risk="100", thesis_id="CR")
    first = auth.authorize_and_reserve(req)
    restored, _ = restore(repo)
    replay = restored.authorize_and_reserve(req)
    assert replay.idempotent_replay and replay.reservation == first.reservation
    assert len(restored.state().reservations) == 1


def test_C02_crash_before_write_leaves_memory_and_store_unchanged_and_retry_reserves_once():
    repo = CrashingRepository()
    auth, _ = fresh(repo)
    mem_before, store_before = auth.lifecycle_state(), repo.snapshot()
    repo.mode = "before"
    with pytest.raises(PersistenceCommitError):
        auth.authorize_and_reserve(request(risk="100", thesis_id="CR"))
    assert auth.lifecycle_state() is mem_before and repo.snapshot() == store_before
    repo.mode = None
    restored, _ = restore(repo)
    assert restored.lifecycle_state() == mem_before
    assert restored.authorize_and_reserve(request(risk="100", thesis_id="CR")).authorized
    assert len(restored.state().reservations) == 1


def test_C03_crash_after_write_before_swap_restores_committed_state_without_duplicate():
    repo = CrashingRepository()
    auth, _ = fresh(repo)
    repo.mode = "after"
    with pytest.raises(PersistenceCommitError):
        auth.authorize_and_reserve(request(risk="100", thesis_id="CR"))
    assert auth.state().reservations == ()                          # this process never swapped
    repo.mode = None
    restored, _ = restore(repo)                                     # the write survived the "crash"
    assert len(restored.state().reservations) == 1
    replay = restored.authorize_and_reserve(request(risk="100", thesis_id="CR"))
    assert replay.idempotent_replay and len(restored.state().reservations) == 1
    with pytest.raises(PersistenceCommitError):                     # the stale process can no longer write (CAS)
        auth.authorize_and_reserve(request(risk="100", thesis_id="OTHER"))


@pytest.mark.parametrize("target", [O.COMMITTED_POSITION, O.RELEASED])
@pytest.mark.parametrize("crash", ["none", "before", "after"])
def test_C04_C05_crash_around_lifecycle_transition(target, crash):
    repo = CrashingRepository()
    auth, _ = fresh(repo)
    res = reserve(auth, "100", "T1")
    repo.mode = None if crash == "none" else crash
    if crash == "none":
        assert auth.reference_transition(transition(res, target)).applied
    else:
        with pytest.raises(PersistenceCommitError):
            auth.reference_transition(transition(res, target))
        assert auth.lifecycle_state().owner_of(res.child_trade_id) is O.ACTIVE_RESERVATION
    repo.mode = None
    restored, _ = restore(repo)
    owner = restored.lifecycle_state().owner_of(res.child_trade_id)
    expected_owner = O.ACTIVE_RESERVATION if crash == "before" else target
    assert owner is expected_owner
    outcome = restored.reference_transition(transition(res, target))
    assert outcome.applied and outcome.idempotent_replay is (crash != "before")
    assert len(restored.lifecycle_state().records) == 1
    exposure = capacity(restored)
    assert exposure[:3] == ((D("500"), 3, 0) if target is O.COMMITTED_POSITION else (D("400"), 2, 0))


def test_C06_two_restored_processes_cannot_both_write_split_brain():
    auth, repo = fresh()
    a, _ = restore(repo)
    b, _ = restore(repo)
    assert a.authorize_and_reserve(request(risk="400", thesis_id="A1")).authorized
    b_before = b.lifecycle_state()
    with pytest.raises(PersistenceCommitError):
        b.authorize_and_reserve(request(risk="400", thesis_id="B1"))
    assert b.lifecycle_state() is b_before
    final, _ = restore(repo)
    assert [r.child_trade_id for r in final.state().reservations] == ["A1:p1"]


# ---- persistence contract ---------------------------------------------------------------------------------------

def test_every_commit_persists_exactly_one_account_revision():
    auth, repo = fresh(None, None, "p1", "p2")
    revs = lambda: {pid: repo.load(pid).revision for pid in repo.list_profiles()}
    assert revs() == {"p1": 1, "p2": 1}
    res = reserve(auth, "100", "A", profile=P1)
    assert revs() == {"p1": 2, "p2": 1}
    auth.reference_transition(transition(res, O.RELEASED))
    assert revs() == {"p1": 3, "p2": 1}
    auth.replace_account_state(state("p2", realized_daily_pnl=D("-5")))
    assert revs() == {"p1": 3, "p2": 2}
    auth.authorize_and_reserve(request(risk="1", thesis_id="A", profile=P1))   # DUPLICATE: no commit
    auth.reference_transition(transition(res, O.RELEASED))                     # replay: no commit
    assert revs() == {"p1": 3, "p2": 2}


def test_no_derived_state_is_persisted():
    auth, repo, _ = populated()
    payload = repo.load("p1").payload
    content = json.loads(payload)["content"]
    assert set(content) == {"account_state", "reservations", "lifecycle_records"}
    keys = set()

    def walk(v):
        if isinstance(v, dict):
            keys.update(v)
            for x in v.values():
                walk(x)
        elif isinstance(v, list):
            for x in v:
                walk(x)

    walk(content)
    derived = {"open_risk_usd", "reserved_position_slots", "request_fingerprint", "transferred_risk_usd",
               "transferred_position_slots", "owner", "active_position_reservations", "open_positions"}
    assert keys.isdisjoint(derived), keys & derived
    assert "committed_open_risk_usd" in keys and "committed_open_positions" in keys


def test_document_envelope_is_versioned_canonical_and_digest_protected():
    auth, repo, _ = populated()
    doc = repo.load("p1")
    env = json.loads(doc.payload)
    assert set(env) == {"schema", "schema_version", "profile_id", "revision", "content_sha256", "content"}
    assert (env["schema"], env["schema_version"], env["profile_id"], env["revision"]) == (SCHEMA_NAME, 1, "p1", doc.revision)
    assert env["content_sha256"] == hashlib.sha256(canonical_json(env["content"]).encode()).hexdigest()
    assert doc.payload == canonical_json(env)
    assert decode_account_document(doc).ledger == auth.state().ledger("p1")


def test_fresh_authority_refuses_non_empty_repository():
    auth, repo = fresh()
    with pytest.raises(PersistenceCommitError, match="use restore"):
        ReferencePersistentLifecycleAuthority(CONFIG, FakeClock(), repo, acknowledge_reference_only_non_production=True)


@pytest.mark.parametrize("change", [dict(committed_open_positions="2"), dict(committed_open_positions=-1),
                                    dict(committed_open_risk_usd=D("-1")), dict(balance=D("0"))])
def test_unpersistable_state_fails_closed_before_any_write(change):
    auth, repo = fresh()
    before, store = auth.lifecycle_state(), repo.snapshot()
    with pytest.raises(PersistenceCommitError):
        auth.replace_account_state(dataclasses.replace(state("p1"), **change))
    assert auth.lifecycle_state() is before and repo.snapshot() == store


def test_repository_compare_and_set_contract():
    repo = InMemoryReferenceRepository()
    repo.save(StoredDocument("p1", 1, "x"), None)
    for doc, expected in [(StoredDocument("p1", 1, "y"), None), (StoredDocument("p1", 3, "y"), 1),
                          (StoredDocument("p1", 2, "y"), 2)]:
        with pytest.raises(RepositoryConflictError):
            repo.save(doc, expected)
    assert repo.load("p1").payload == "x"
    assert isinstance(repo, LifecycleStateRepository) and repo.GRANTS_PRODUCTION_AUTHORITY is False


def test_reconciliation_input_validation_rejects_without_side_effects():
    auth, repo, ids = populated()
    restored, _ = restore(repo)
    before = restored.lifecycle_state()
    bad = [
        snap("p1", P1, obj("DUP", None), obj("DUP", None)),
        ExternalAccountSnapshot("p1", P1.identity, datetime(2026, 9, 16), ()),
        snap("p1", P2),
        snap("p9", make_profile("p9")),
        ExternalAccountSnapshot("p1", P1.identity, NOW, [obj("X", None)]),
    ]
    for s in bad:
        r = restored.reference_reconcile(s)
        assert r.status is RS.REJECTED and r.items == () and r.rejection_reasons
    assert restored.lifecycle_state() is before


def test_reconciliation_outcome_classification_table_complete():
    from api.xtl_platform.risk.reconciliation_contracts import OUTCOME_CLASS
    assert set(OUTCOME_CLASS) == set(RO)
    assert {k for k, v in OUTCOME_CLASS.items() if v is ReconciliationClass.MATCHED} == {
        RO.MATCHED_ACTIVE_RESERVATION_PENDING_ORDER, RO.MATCHED_COMMITTED_POSITION, RO.MATCHED_RELEASED_ABSENT}


# ---- isolation and frozen baseline ---------------------------------------------------------------------------

DAY6_FILES = ("persistence_contracts.py", "persistence_repository.py", "persistence_codec.py",
              "persistent_reference.py", "reconciliation_contracts.py", "reconciliation_reference.py")


def test_no_production_dependencies_io_or_authority():
    from api.xtl_platform.tests.test_isolation import PACKAGE, PKG_ROOT, PRODUCTION_MANIFEST, scan_source
    for name in DAY6_FILES:
        source = (PKG_ROOT / "risk" / name).read_text()
        assert f"risk/{name}" in PRODUCTION_MANIFEST
        assert scan_source(source, PACKAGE + ("risk",)) == [], name
        tree = ast.parse(source)
        absolute = {n.module for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.level == 0}
        absolute |= {a.name for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names}
        modules = absolute | {n.module or "" for n in ast.walk(tree) if isinstance(n, ast.ImportFrom)}
        top = {m.split(".")[0] for m in absolute}
        assert top <= {"__future__", "abc", "dataclasses", "datetime", "decimal", "typing", "threading", "json",
                       "hashlib", "re"}, (name, top)
        attrs = {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)}
        assert attrs.isdisjoint({"now", "utcnow", "environ", "getenv", "connect", "urlopen", "read_text",
                                 "write_text", "fsync"}), name
        file_json = {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)
                     and isinstance(n.value, ast.Name) and n.value.id == "json" and n.attr in {"load", "dump"}}
        assert file_json == set(), (name, file_json)          # only in-memory json.loads / json.dumps
        assert "REFERENCE ONLY - NON-PRODUCTION" in source, name
        assert not any("strategy" in m.lower() or "execution" in m for m in modules), name
    assert ReferencePersistentLifecycleAuthority.GRANTS_PRODUCTION_AUTHORITY is False
    assert ReferencePersistentLifecycleAuthority.AUTHORITY_CLASSIFICATION == "REFERENCE_ONLY_NON_PRODUCTION"
    import api.xtl_platform.risk as risk_pkg
    assert risk_pkg.__all__ == ["RiskAuthority"]


FROZEN_DAY5_SHA256 = {
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
    "risk/reference_test_only.py": "3edd3c151fe49ad264fae52986ae91180a71ba46e6adfe9c640c50e293eca9e7",
    "risk/validation.py": "ddea557444c5e8513fa93631cafb7a1591fabd2959e4f880825eee7c90425bad",
    "tests/__init__.py": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    "tests/_builders.py": "4bf536e11dac33ef9f5dff0c176e55abcf6f85d96e8ce12a59e7207d6255a51a",
    "tests/conftest.py": "0893751f3d47a26d26e4294c8a348c78393cee79e133f6d5bc1ef2c2e6e6b79f",
    "tests/test_account_identity.py": "2165ec6dcca34a73b96c8555bddfffe4cde7f4be42e7167ed7db2239bb4b61e0",
    "tests/test_atomic_reference.py": "cb07e69fc4aca82137bacf3d95d25ae8a11672186163ee24dbd8e3662e25e57f",
    "tests/test_domain_contracts.py": "a64712d0114904d8d3aa308bfbbd834cf8286144ac3744381dfee47a961c0ee3",
    "tests/test_lifecycle_reference.py": "629424f41bccc957908b0014370e1e87c021f1351e37960c0ee21e0c4da3f366",
    "tests/test_lifecycle_state_feed_guard.py": "b72ee921da038006e76dedbf0034e255a2229c0e3bb398ff42c04fc7b37f3ffe",
    "tests/test_policy_engine.py": "791cb3b9c0c314e4773fd3ea69c33bba1c0fcab9f318728a61aaefd2cca34187",
    "tests/test_position_slots.py": "19e3c93b7812d57e9e71e4eb63b87145f98fbffae297a330b71804d29d1057fd",
    "tests/test_risk_evaluator.py": "e64c800d1344cad6d0469eebf323b61d7963e318ade8cf924921d066e2929d82",
    "tests/test_thesis_fanout.py": "1fb20203b692d95c6f45054b804f051b6e840ab0079ca4cf4c6fe9d607ba4ea3",
}


def test_frozen_day1_to_day5_files_unchanged():
    from api.xtl_platform.tests.test_isolation import PKG_ROOT
    changed = [rel for rel, digest in FROZEN_DAY5_SHA256.items()
               if hashlib.sha256((PKG_ROOT / rel).read_bytes()).hexdigest() != digest]
    assert changed == [] and len(FROZEN_DAY5_SHA256) == 42


# ---- gaps found by mutation testing (P6, P17) --------------------------------------------------------------

def _quarantined_p2():
    auth, repo, _ = populated()
    tamper(repo, "p2", lambda env: env["content"]["account_state"].update(balance="0"))
    restored, report = restore(repo)
    assert report.quarantined_profiles == ("p2",)
    return restored, repo


def test_quarantine_guard_at_registration():
    restored, repo = _quarantined_p2()
    store = repo.snapshot()
    with pytest.raises(QuarantinedAccountError, match="re-registration could silently drop persisted risk"):
        restored.register_account(state("p2"))
    assert repo.snapshot() == store


def test_quarantine_guard_at_commit_independently_of_registration_guard():
    """Bypass the registration guard (call the frozen Day-4 registration directly): the
    commit-level guard must still refuse, before any write."""
    from api.xtl_platform.risk.atomic_reference import ReferenceAtomicRiskAuthority
    restored, repo = _quarantined_p2()
    store, before = repo.snapshot(), restored.lifecycle_state()
    with pytest.raises(QuarantinedAccountError, match="quarantined after a rejected restore"):
        ReferenceAtomicRiskAuthority.register_account(restored, state("p2"))
    assert repo.snapshot() == store and restored.lifecycle_state() is before


def test_commit_changing_more_than_one_account_is_refused_without_writes():
    from api.xtl_platform.risk.atomic_contracts import ReferenceLedger
    from api.xtl_platform.risk.lifecycle_contracts import ReferenceLifecycleState
    auth, repo = fresh(None, None, "p1", "p2")
    current = auth.lifecycle_state()
    two_changed = current.authority
    for pid in ("p1", "p2"):
        ledger = two_changed.ledger(pid)
        two_changed = two_changed.with_ledger(
            ReferenceLedger(dataclasses.replace(ledger.account_state, realized_daily_pnl=D("-1")), ledger.reservations))
    store = repo.snapshot()
    with auth._lock:
        with pytest.raises(PersistenceCommitError, match="exactly one account"):
            auth._commit_lifecycle(ReferenceLifecycleState(two_changed, current.records))
    assert auth.lifecycle_state() is current and repo.snapshot() == store
