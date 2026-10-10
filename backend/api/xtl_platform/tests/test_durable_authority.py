"""Day 9 - durable account-level risk authority and atomic reservation.

REFERENCE ONLY - NON-PRODUCTION. EXECUTION AUTHORITY = FALSE. BROKER AUTHORITY = FALSE.
Deterministic and offline: temporary SQLite files, barriers and injected failures only.
All limit values are TEST FIXTURE - NOT VERIFIED PRODUCTION FIRM RULE.
"""
from __future__ import annotations

import ast
import hashlib
import json
import threading
from contextlib import contextmanager
from datetime import timedelta
from decimal import Decimal

import pytest

from api.xtl_platform.domain.enums import StrategyId
from api.xtl_platform.risk.atomic_contracts import AtomicAuthorizationCode as A
from api.xtl_platform.risk.durable_authority import DurableAccountRiskAuthority
from api.xtl_platform.risk.durable_authority_contracts import (
    DurableAuthorizationOutcome, DurableOutcomeCode as C, DurableTransitionOutcome,
    InMemoryTransactionalRepository, TransactionalLifecycleStateRepository,
)
from api.xtl_platform.risk.durable_transaction_sqlite import SQLiteTransactionalRepository
from api.xtl_platform.risk.lifecycle_contracts import ExposureOwner as EO
from api.xtl_platform.risk.persistence_codec import canonical_json, content_digest
from api.xtl_platform.risk.persistence_contracts import RepositoryConflictError, StoredDocument
from api.xtl_platform.risk.persistent_reference import ReferencePersistentLifecycleAuthority
from api.xtl_platform.tests._builders import NOW, D
from api.xtl_platform.tests.test_atomic_reference import CONFIG, P1, P2, FakeClock, account_state, request
from api.xtl_platform.tests.test_lifecycle_reference import transition
from api.xtl_platform.tests.test_persistence_reconciliation import state
from api.xtl_platform.tests.test_sqlite_repository import payload_sha256, raw, rewrite_payload

AT = NOW


# ---- helpers ---------------------------------------------------------------------------------------

@pytest.fixture
def db(tmp_path):
    return str(tmp_path / "xtl_day9.sqlite3")


def account(pid="p1", profile=None, capacity="100", committed="0", positions=0):
    return account_state(pid, profile or P1, limit_kw=dict(max_open_risk_usd=D(capacity), max_positions=10,
                                                           max_risk_per_trade_usd=D("1000")),
                         committed_open_risk_usd=D(committed), committed_open_positions=positions)


def authority(repository, *accounts):
    auth = DurableAccountRiskAuthority(CONFIG, repository, acknowledge_reference_only_non_production=True)
    for a in (accounts or (account(),)):
        auth.register_account(a)
    return auth


def reserve(auth, risk="70", thesis_id="A-1", profile=P1, strategy=StrategyId.STRATEGY_A, at=AT):
    return auth.authorize_and_reserve(request(risk=risk, thesis_id=thesis_id, profile=profile, strategy=strategy),
                                      at=at)


def reserved_risk(auth, pid="p1"):
    state_ = auth.account_state(pid)
    ledger = state_.authority.ledger(pid)
    return sum((r.risk_usd for r in ledger.reservations), Decimal("0"))


# ---- authorize + reserve, capacity, idempotency -------------------------------------------------------

def test_successful_authorize_and_reserve_is_durable(db):
    repository = SQLiteTransactionalRepository(db)
    auth = authority(repository)
    assert auth.durable_revision("p1") == 1

    outcome = reserve(auth, "70", "A-1")
    assert outcome.code is C.PERSISTED and outcome.persisted
    assert outcome.durable_revision == 2 and outcome.reservation.risk_usd == D("70")
    assert outcome.reservation.reservation_id == "REF-RESV:A-1:p1"          # identity from frozen contracts
    assert outcome.authorization.authorized and not outcome.authorization.idempotent_replay
    assert reserved_risk(auth) == D("70")
    assert DurableAuthorizationOutcome.EXECUTION_AUTHORITY is False and DurableAuthorizationOutcome.BROKER_AUTHORITY is False


def test_insufficient_capacity_is_rejected_without_any_reservation(db):
    repository = SQLiteTransactionalRepository(db)
    auth = authority(repository)
    assert reserve(auth, "70", "A-1").persisted
    before_rows = repository.snapshot()

    blocked = reserve(auth, "70", "A-2")
    assert blocked.code is C.BLOCKED and blocked.reservation is None
    assert blocked.authorization.code is A.BLOCK_OPEN_RISK
    assert repository.snapshot() == before_rows and auth.durable_revision("p1") == 2
    assert reserved_risk(auth) == D("70")


def test_duplicate_request_cannot_double_reserve(db):
    repository = SQLiteTransactionalRepository(db)
    auth = authority(repository)
    first = reserve(auth, "70", "A-1")
    rows = repository.snapshot()
    for _ in range(3):
        replay = reserve(auth, "70", "A-1")
        assert replay.code is C.IDEMPOTENT_REPLAY and replay.reservation == first.reservation
        assert replay.durable_revision == 2
    assert repository.snapshot() == rows and reserved_risk(auth) == D("70")


def test_conflicting_duplicate_child_id_is_blocked(db):
    auth = authority(SQLiteTransactionalRepository(db))
    reserve(auth, "70", "A-1")
    conflicting = reserve(auth, "30", "A-1")                                  # same child id, different risk
    assert conflicting.code is C.BLOCKED and conflicting.authorization.code is A.DUPLICATE_TRADE_ID
    assert reserved_risk(auth) == D("70")


# ---- CRITICAL concurrency invariant ---------------------------------------------------------------------

def test_CRITICAL_two_workers_70_against_capacity_100_cannot_both_reserve(db):
    """Two independent authorities on two connections to the same durable database."""
    for round_ in range(20):
        path = f"{db}.{round_}"
        authority(SQLiteTransactionalRepository(path))                        # capacity 100
        workers = [DurableAccountRiskAuthority(CONFIG, SQLiteTransactionalRepository(path),
                                               acknowledge_reference_only_non_production=True) for _ in range(2)]
        barrier, results, lock = threading.Barrier(2), [], threading.Lock()

        def run(i):
            barrier.wait()
            outcome = workers[i].authorize_and_reserve(
                request(risk="70", thesis_id=f"W{i}", strategy=StrategyId.STRATEGY_A if i == 0 else StrategyId.STRATEGY_B),
                at=AT)
            with lock:
                results.append(outcome)

        threads = [threading.Thread(target=run, args=(i,)) for i in (0, 1)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        persisted = [o for o in results if o.persisted]
        refused = [o for o in results if not o.persisted]
        assert len(persisted) == 1 and len(refused) == 1, [o.code for o in results]
        assert refused[0].code in {C.BLOCKED, C.REVISION_CONFLICT}
        if refused[0].code is C.BLOCKED:
            assert refused[0].authorization.code is A.BLOCK_OPEN_RISK       # re-decided against committed capacity
        final = DurableAccountRiskAuthority(CONFIG, SQLiteTransactionalRepository(path),
                                            acknowledge_reference_only_non_production=True)
        assert reserved_risk(final) == D("70")                               # never 140
        assert final.durable_revision("p1") == 2


def test_many_concurrent_workers_never_exceed_capacity(db):
    authority(SQLiteTransactionalRepository(db), account(capacity="300"))
    workers = [DurableAccountRiskAuthority(CONFIG, SQLiteTransactionalRepository(db),
                                           acknowledge_reference_only_non_production=True) for _ in range(10)]
    barrier, results, lock = threading.Barrier(10), [], threading.Lock()

    def run(i):
        barrier.wait()
        outcome = workers[i].authorize_and_reserve(request(risk="100", thesis_id=f"T{i}"), at=AT)
        with lock:
            results.append(outcome)

    threads = [threading.Thread(target=run, args=(i,)) for i in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sum(o.persisted for o in results) == 3                            # 300 / 100
    final = DurableAccountRiskAuthority(CONFIG, SQLiteTransactionalRepository(db),
                                        acknowledge_reference_only_non_production=True)
    assert reserved_risk(final) == D("300")
    assert final.durable_revision("p1") == 4


def test_two_authorities_respect_the_same_durable_capacity_sequentially(db):
    authority(SQLiteTransactionalRepository(db))
    first = DurableAccountRiskAuthority(CONFIG, SQLiteTransactionalRepository(db),
                                        acknowledge_reference_only_non_production=True)
    second = DurableAccountRiskAuthority(CONFIG, SQLiteTransactionalRepository(db),
                                         acknowledge_reference_only_non_production=True)
    assert first.authorize_and_reserve(request(risk="70", thesis_id="A"), at=AT).persisted
    blocked = second.authorize_and_reserve(request(risk="70", thesis_id="B"), at=AT)
    assert blocked.code is C.BLOCKED and blocked.authorization.code is A.BLOCK_OPEN_RISK
    assert reserved_risk(second) == D("70")


# ---- stale revision / CAS conflict ------------------------------------------------------------------------

class StaleWriteRepository(InMemoryTransactionalRepository):
    """Deterministic CAS-conflict injection: a competing writer commits between decide and write."""

    def __init__(self):
        super().__init__()
        self.inject = False

    @contextmanager
    def authorization_transaction(self, profile_id):
        with super().authorization_transaction(profile_id) as transaction:
            outer = self

            class Wrapper:
                def load(self):
                    return transaction.load()

                def problem(self):
                    return transaction.problem()

                def write(self, document, expected_revision):
                    if outer.inject:
                        # A competing writer commits a VALID next revision first.
                        current = outer.documents[profile_id]
                        envelope = json.loads(current.payload)
                        envelope["revision"] = current.revision + 1
                        outer.documents[profile_id] = StoredDocument(
                            profile_id, current.revision + 1, canonical_json(envelope))
                    return transaction.write(document, expected_revision)

            yield Wrapper()


def test_stale_revision_loses_compare_and_set_and_reserves_nothing():
    repository = StaleWriteRepository()
    auth = authority(repository)
    repository.inject = True
    outcome = reserve(auth, "70", "A-1")
    assert outcome.code is C.REVISION_CONFLICT and outcome.reservation is None
    repository.inject = False
    assert reserved_risk(auth) == D("0")
    assert auth.durable_revision("p1") == 2                                   # only the competing write landed
    assert reserve(auth, "70", "A-2").persisted                               # retry works against fresh state


def test_transaction_write_enforces_consecutive_revisions(db):
    repository = SQLiteTransactionalRepository(db)
    authority(repository)
    with repository.authorization_transaction("p1") as transaction:
        document = transaction.load()
        for revision, expected in [(3, 1), (2, 2), (1, None)]:
            with pytest.raises(RepositoryConflictError):
                transaction.write(StoredDocument("p1", revision, document.payload), expected)
    assert repository.load("p1").revision == 1


# ---- failure injection --------------------------------------------------------------------------------------

class FailingRepository(SQLiteTransactionalRepository):
    def __init__(self, path, stage=None):
        super().__init__(path)
        self.stage = stage

    def _before_transaction(self):
        if self.stage == "before_transaction":
            raise OSError("injected before transaction")

    def _before_commit(self):
        if self.stage == "during_transaction":
            raise OSError("injected before durable commit")

    def _after_commit(self):
        if self.stage == "after_commit":
            raise OSError("injected after durable commit")


@pytest.mark.parametrize("stage", ["before_transaction", "during_transaction"])
def test_failure_before_durable_commit_leaves_no_reservation(db, stage):
    repository = FailingRepository(db)
    auth = authority(repository)
    assert reserve(auth, "70", "OK").persisted
    rows = repository.snapshot()

    repository.stage = stage
    with pytest.raises(OSError):
        reserve(auth, "20", "LOST")
    assert repository.snapshot() == rows
    repository.stage = None
    assert reserved_risk(auth) == D("70")
    fresh = DurableAccountRiskAuthority(CONFIG, SQLiteTransactionalRepository(db),
                                        acknowledge_reference_only_non_production=True)
    assert reserved_risk(fresh) == D("70") and fresh.durable_revision("p1") == 2


def test_failure_after_durable_commit_recovers_forward_without_duplicate(db):
    repository = FailingRepository(db)
    auth = authority(repository)
    repository.stage = "after_commit"
    with pytest.raises(OSError):
        reserve(auth, "70", "FORWARD")
    repository.stage = None

    fresh = DurableAccountRiskAuthority(CONFIG, SQLiteTransactionalRepository(db),
                                        acknowledge_reference_only_non_production=True)
    assert reserved_risk(fresh) == D("70") and fresh.durable_revision("p1") == 2
    replay = fresh.authorize_and_reserve(request(risk="70", thesis_id="FORWARD"), at=AT)
    assert replay.code is C.IDEMPOTENT_REPLAY and fresh.durable_revision("p1") == 2


# ---- restart, terminal states, idempotent restore ----------------------------------------------------------

def test_committed_reservation_survives_restart(db):
    repository = SQLiteTransactionalRepository(db)
    auth = authority(repository)
    outcome = reserve(auth, "70", "HELD")
    repository.close()

    restarted = DurableAccountRiskAuthority(CONFIG, SQLiteTransactionalRepository(db),
                                            acknowledge_reference_only_non_production=True)
    state_ = restarted.account_state("p1")
    assert state_.owner_of("HELD:p1") is EO.ACTIVE_RESERVATION
    assert reserved_risk(restarted) == D("70") and restarted.durable_revision("p1") == 2
    assert state_.authority.ledger("p1").reservations[0] == outcome.reservation
    assert restarted.authorize_and_reserve(request(risk="70", thesis_id="HELD"), at=AT).code is C.IDEMPOTENT_REPLAY


@pytest.mark.parametrize("target,expected_reserved", [(EO.COMMITTED_POSITION, D("0")), (EO.RELEASED, D("0"))])
def test_terminal_transition_is_durable_and_never_resurrects(db, target, expected_reserved):
    repository = SQLiteTransactionalRepository(db)
    auth = authority(repository)
    outcome = reserve(auth, "70", "T")
    applied = auth.apply_transition(transition(outcome.reservation, target), at=AT + timedelta(seconds=1))
    assert applied.code is C.PERSISTED and applied.durable_revision == 3
    repository.close()

    restarted = DurableAccountRiskAuthority(CONFIG, SQLiteTransactionalRepository(db),
                                            acknowledge_reference_only_non_production=True)
    assert restarted.account_state("p1").owner_of("T:p1") is target
    assert reserved_risk(restarted) == expected_reserved
    assert restarted.authorize_and_reserve(request(risk="70", thesis_id="T"), at=AT).authorization.code is A.DUPLICATE_TRADE_ID
    replay = restarted.apply_transition(transition(outcome.reservation, target), at=AT + timedelta(seconds=1))
    assert replay.code is C.IDEMPOTENT_REPLAY and restarted.durable_revision("p1") == 3
    other = EO.RELEASED if target is EO.COMMITTED_POSITION else EO.COMMITTED_POSITION
    refused = restarted.apply_transition(transition(outcome.reservation, other), at=AT + timedelta(seconds=2))
    assert refused.code is C.BLOCKED and restarted.durable_revision("p1") == 3


def test_repeated_restore_is_idempotent_and_never_writes(db):
    repository = SQLiteTransactionalRepository(db)
    auth = authority(repository)
    reserve(auth, "70", "A-1")
    rows, file_digest = repository.snapshot(), hashlib.sha256(open(db, "rb").read()).hexdigest()
    states = [DurableAccountRiskAuthority(CONFIG, SQLiteTransactionalRepository(db),
                                          acknowledge_reference_only_non_production=True).account_state("p1")
              for _ in range(3)]
    assert all(s == states[0] for s in states)
    assert repository.snapshot() == rows
    assert hashlib.sha256(open(db, "rb").read()).hexdigest() == file_digest


def test_day8_persistence_and_day6_restore_semantics_remain_valid(db):
    repository = SQLiteTransactionalRepository(db)
    auth = authority(repository)
    reserve(auth, "70", "A-1")
    frozen_authority, report = ReferencePersistentLifecycleAuthority.restore(
        CONFIG, FakeClock(), SQLiteTransactionalRepository(db), acknowledge_reference_only_non_production=True)
    assert report.clean
    assert frozen_authority.lifecycle_state() == auth.account_state("p1")
    assert frozen_authority.authorize_and_reserve(request(risk="70", thesis_id="A-1")).idempotent_replay


# ---- account isolation --------------------------------------------------------------------------------------

def test_account_isolation(db):
    repository = SQLiteTransactionalRepository(db)
    auth = authority(repository, account("p1", P1, capacity="100"), account("p2", P2, capacity="100"))
    rows = {row[0]: row for row in repository.snapshot()}

    assert reserve(auth, "70", "A", profile=P1).persisted
    after = {row[0]: row for row in repository.snapshot()}
    assert after["p2"] == rows["p2"]                                        # B untouched
    assert reserve(auth, "70", "B", profile=P2).persisted                   # B has its own capacity
    assert reserved_risk(auth, "p1") == reserved_risk(auth, "p2") == D("70")
    assert reserve(auth, "70", "A2", profile=P1).code is C.BLOCKED          # A still capped
    assert reserved_risk(auth, "p2") == D("70")
    assert auth.account_state("p1").authority.ledger("p2") is None


def test_unregistered_account_fails_closed(db):
    repository = SQLiteTransactionalRepository(db)
    auth = authority(repository)
    outcome = auth.authorize_and_reserve(request(risk="70", thesis_id="X", profile=P2), at=AT)
    assert outcome.code is C.ACCOUNT_NOT_REGISTERED and outcome.reservation is None
    assert repository.load("p2") is None
    with pytest.raises(Exception):
        auth.register_account(account("p1", P1))                             # already registered


# ---- corruption / schema fail closed -------------------------------------------------------------------------

@pytest.mark.parametrize("name,corrupt,marker", [
    ("unsupported_schema", lambda db: rewrite_payload(db, "p1", lambda e: e.update(schema_version=2)),
     "schema_version 2 is not supported"),
    ("invalid_risk_value", lambda db: rewrite_payload(db, "p1", lambda e: e["content"]["reservations"][0].update(risk_usd="-5")),
     "reservations[0]"),
    ("content_digest", lambda db: rewrite_payload(db, "p1", lambda e: e["content"]["account_state"].update(balance="12345"),
                                                  fix_content_digest=False), "content_sha256"),
    ("row_digest", lambda db: raw(db, "UPDATE xtl_account_documents SET payload_sha256=? WHERE profile_id='p1'", ("0" * 64,)),
     "payload_sha256"),
])
def test_corrupt_durable_state_fails_closed(db, name, corrupt, marker):
    repository = SQLiteTransactionalRepository(db)
    auth = authority(repository, account("p1", P1), account("p2", P2))
    reserve(auth, "70", "A-1", profile=P1)
    repository.close()
    corrupt(db)

    fresh = DurableAccountRiskAuthority(CONFIG, SQLiteTransactionalRepository(db),
                                        acknowledge_reference_only_non_production=True)
    rows_before = SQLiteTransactionalRepository(db).snapshot()
    outcome = fresh.authorize_and_reserve(request(risk="10", thesis_id="NEW", profile=P1), at=AT)
    assert outcome.code is C.DURABLE_STATE_REJECTED and outcome.reservation is None
    assert any(marker in e for e in outcome.rejection_errors), outcome.rejection_errors
    assert SQLiteTransactionalRepository(db).snapshot() == rows_before       # nothing written or repaired
    assert fresh.authorize_and_reserve(request(risk="10", thesis_id="OK", profile=P2), at=AT).persisted   # B unaffected


def test_unsupported_repository_schema_fails_closed(db):
    SQLiteTransactionalRepository(db).close()
    raw(db, "UPDATE xtl_repository_meta SET value='99' WHERE key='repository_schema_version'")
    with pytest.raises(Exception, match="unsupported repository_schema_version"):
        SQLiteTransactionalRepository(db)


# ---- determinism, boundaries, isolation ------------------------------------------------------------------------

def test_same_inputs_produce_byte_identical_durable_state(db, tmp_path):
    digests = []
    for i in range(2):
        path = str(tmp_path / f"run{i}.sqlite3")
        auth = authority(SQLiteTransactionalRepository(path))
        reserve(auth, "70", "A-1", at=AT)
        digests.append(SQLiteTransactionalRepository(path).load("p1").payload)
    assert digests[0] == digests[1]


def test_caller_supplied_time_is_used_and_no_wall_clock_is_read(db):
    auth = authority(SQLiteTransactionalRepository(db))
    at = NOW + timedelta(seconds=5)
    outcome = reserve(auth, "70", "A-1", at=at)
    assert outcome.reservation.created_at == at
    assert outcome.authorization.authorized and outcome.authorization.risk_evaluation.evaluated_at == at
    with pytest.raises(Exception):
        auth.authorize_and_reserve(request(risk="70", thesis_id="B"), at=NOW.replace(tzinfo=None))


DAY9_FILES = ("durable_authority_contracts.py", "durable_transaction_sqlite.py", "durable_authority.py")


def test_no_execution_authority_strategy_imports_or_forbidden_surface():
    from api.xtl_platform.tests.test_isolation import (
        APPROVED_PERSISTENCE_BOUNDARY, PACKAGE, PKG_ROOT, PRODUCTION_MANIFEST, scan_source,
    )
    for name in DAY9_FILES:
        rel = f"risk/{name}"
        source = (PKG_ROOT / "risk" / name).read_text()
        assert rel in PRODUCTION_MANIFEST
        allowed = APPROVED_PERSISTENCE_BOUNDARY.get(rel, frozenset())
        violations = [v for v in scan_source(source, PACKAGE + ("risk",))
                      if not any(f": {m}" in v for m in allowed)]
        assert violations == [], (name, violations)
        tree = ast.parse(source)
        modules = {n.module for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.level == 0} | \
                  {a.name for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names}
        assert not any(m and ("strategy" in m.lower() or "oppt" in m.lower() or "zone_entry" in m.lower()
                              or "metatrader" in m.lower() or "redis" in m.lower() or "psycopg" in m.lower()
                              or "socket" in m or "urllib" in m or "http" in m) for m in modules), name
        names = {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)} | \
                {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)}
        assert names.isdisjoint({"now", "utcnow", "today", "uuid4", "uuid1", "random", "environ", "getenv",
                                 "Thread", "Timer", "sleep", "order_send", "place", "lot", "contract_size",
                                 "tick_value", "margin"}), (name, names)
        assert "REFERENCE ONLY - NON-PRODUCTION" in source
    assert (DurableAccountRiskAuthority.EXECUTION_AUTHORITY, DurableAccountRiskAuthority.BROKER_AUTHORITY,
            DurableAccountRiskAuthority.GRANTS_PRODUCTION_AUTHORITY) == (False, False, False)
    assert (DurableTransitionOutcome.EXECUTION_AUTHORITY, DurableTransitionOutcome.BROKER_AUTHORITY) == (False, False)
    assert issubclass(SQLiteTransactionalRepository, TransactionalLifecycleStateRepository)
    with pytest.raises(RuntimeError, match="NON-PRODUCTION"):
        DurableAccountRiskAuthority(CONFIG, InMemoryTransactionalRepository())
    import api.xtl_platform.risk as risk_pkg
    assert risk_pkg.__all__ == ["RiskAuthority"]


def test_in_memory_and_sqlite_transactional_repositories_behave_identically(db):
    outcomes = []
    for repository in (InMemoryTransactionalRepository(), SQLiteTransactionalRepository(db)):
        auth = authority(repository)
        outcomes.append([reserve(auth, "70", "A-1").code, reserve(auth, "70", "A-2").code,
                         reserve(auth, "70", "A-1").code, reserved_risk(auth), auth.durable_revision("p1")])
    assert outcomes[0] == outcomes[1] == [C.PERSISTED, C.BLOCKED, C.IDEMPOTENT_REPLAY, D("70"), 2]


FROZEN_DAY8_SHA256 = {
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
    "risk/lifecycle_repository_sqlite.py": "33af12679426a92fbcc5594e32270710555b1840cbc583d341322231cf8baf6a",
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
    "tests/test_sqlite_repository.py": "156161f0c703fb01aedb4c18794aa04829d4d2e92cd73b9a312c74f0903ea214",
    "tests/test_thesis_fanout.py": "1fb20203b692d95c6f45054b804f051b6e840ab0079ca4cf4c6fe9d607ba4ea3",
}


def test_frozen_day1_to_day8_files_unchanged():
    from api.xtl_platform.tests.test_isolation import PKG_ROOT
    changed = [rel for rel, digest in FROZEN_DAY8_SHA256.items()
               if hashlib.sha256((PKG_ROOT / rel).read_bytes()).hexdigest() != digest]
    assert changed == [] and len(FROZEN_DAY8_SHA256) == 54


# ---- gaps found by mutation testing (N2, N10, N11) ------------------------------------------------------

def test_transactional_write_uses_a_real_compare_and_set_clause(db):
    """Row is at revision 2; a writer holding revision 1 must lose even inside its own transaction."""
    repository = SQLiteTransactionalRepository(db)
    auth = authority(repository)
    reserve(auth, "70", "A-1")                                    # revision 2
    stored = repository.load("p1")
    assert stored.revision == 2

    stale_payload = json.loads(stored.payload)
    stale_payload["revision"] = 2
    with repository.authorization_transaction("p1") as transaction:
        with pytest.raises(RepositoryConflictError, match="compare-and-set failed"):
            transaction.write(StoredDocument("p1", 2, canonical_json(stale_payload)), 1)
    assert repository.load("p1") == stored                        # untouched


def test_record_sequences_are_preserved_across_later_writes(db):
    repository = SQLiteTransactionalRepository(db)
    auth = authority(repository, account("p1", P1, capacity="500"))
    first = reserve(auth, "70", "R-1")
    auth.apply_transition(transition(first.reservation, EO.RELEASED), at=AT + timedelta(seconds=1))
    sequences_after_release = [r["sequence"] for r in json.loads(repository.load("p1").payload)["content"]["lifecycle_records"]]

    second = reserve(auth, "70", "R-2")                            # unrelated later write
    auth.apply_transition(transition(second.reservation, EO.COMMITTED_POSITION), at=AT + timedelta(seconds=2))
    records = json.loads(repository.load("p1").payload)["content"]["lifecycle_records"]
    by_child = {r["reservation"]["intent"]["thesis_id"]: r["sequence"] for r in records}

    assert sequences_after_release == [0]
    assert by_child == {"R-1": 0, "R-2": 1}                        # original sequence never regenerated
    restored, report = ReferencePersistentLifecycleAuthority.restore(
        CONFIG, FakeClock(), SQLiteTransactionalRepository(db), acknowledge_reference_only_non_production=True)
    assert report.clean
    assert [r.child_trade_id for r in restored.lifecycle_state().records] == ["R-1:p1", "R-2:p1"]


def test_exception_inside_a_transaction_discards_the_staged_write():
    for repository in (InMemoryTransactionalRepository(), ):
        auth = authority(repository)
        before = repository.load("p1")
        payload = json.loads(before.payload)
        payload["revision"] = 2
        with pytest.raises(RuntimeError, match="injected"):
            with repository.authorization_transaction("p1") as transaction:
                transaction.write(StoredDocument("p1", 2, canonical_json(payload)), 1)
                raise RuntimeError("injected after write, before commit")
        assert repository.load("p1") == before                     # staged write discarded
        assert auth.durable_revision("p1") == 1
