"""Day 7 - Reconciliation Resolution Authority (REFERENCE ONLY - NON-PRODUCTION).

Pure/read-only: resolution never releases capacity, never mutates lifecycle
ownership, never authorizes risk, never executes a repair.
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

from api.xtl_platform.domain.enums import Side, StrategyId
from api.xtl_platform.risk import resolution_reference as resolution_module
from api.xtl_platform.risk.atomic_reference import derive_evaluation_snapshot
from api.xtl_platform.risk.lifecycle_contracts import ExposureOwner as EO, ReferenceLifecycleState
from api.xtl_platform.risk.persistence_codec import canonical_json
from api.xtl_platform.risk.reconciliation_contracts import (
    ExternalAccountSnapshot, ExternalObjectKind as K, ReconciliationItem, ReconciliationOutcome as O,
    ReconciliationReport, ReconciliationStatus as RS,
)
from api.xtl_platform.risk.resolution_contracts import (
    ItemDecision as D, RepairAction as RA, RepairPlan, ResolutionDecision as RD, ResolutionPolicy,
    ResolutionReasonCode as RC, ResolutionRequest, ResolutionResult,
)
from api.xtl_platform.risk.resolution_reference import (
    DECISION_TABLE, EvidenceBuildError, build_reconciliation_evidence, encode_result, evidence_digest,
    resolve_reconciliation,
)
from api.xtl_platform.tests._builders import NOW, D as Dec, make_profile
from api.xtl_platform.tests.test_atomic_reference import P1, P2, request
from api.xtl_platform.tests.test_lifecycle_reference import transition
from api.xtl_platform.tests.test_persistence_reconciliation import (
    capacity, fresh, obj, populated, reserve, restore, snap,
)

POLICY = ResolutionPolicy(max_evidence_age=timedelta(minutes=5))
AT = NOW + timedelta(seconds=30)


# ---- helpers ------------------------------------------------------------------------------------

def evidence(auth, repo, pid="p1", profile=P1, *objects, observed_at=NOW):
    return build_reconciliation_evidence(auth.lifecycle_state(), repo.load(pid),
                                         snap(pid, profile, *objects, observed_at=observed_at))


def resolve(auth, repo, ev, at=AT, request_id="RES-1", policy=POLICY, document_pid=None, state=None):
    return resolve_reconciliation(ResolutionRequest(1, request_id, ev, at),
                                  state if state is not None else auth.lifecycle_state(),
                                  repo.load(document_pid or ev.profile_id), policy)


def forge(ev, **changes):
    """Tamper with evidence AND recompute its evidence_id (a sophisticated forgery)."""
    draft = dataclasses.replace(ev, **changes)
    return dataclasses.replace(draft, evidence_id=evidence_digest(draft))


def assert_nothing_changed(auth, repo, before_state, before_store):
    assert auth.lifecycle_state() is before_state
    assert repo.snapshot() == before_store


def items_by_outcome(result):
    return {i.outcome: i for i in result.items}


# ---- the critical negative test ---------------------------------------------------------------------

def test_CRITICAL_active_reservation_not_observed_never_releases_capacity():
    """ACTIVE reservation $500, external snapshot has no corresponding order."""
    auth, repo = fresh()
    res = reserve(auth, "500", "HELD-500")
    before_state, before_store = auth.lifecycle_state(), repo.snapshot()
    reserved_before = sum((r.risk_usd for r in auth.state().ledger("p1").reservations), Decimal("0"))
    open_risk_before = derive_evaluation_snapshot(auth.state().ledger("p1")).open_risk_usd

    ev = evidence(auth, repo)
    assert ev.report.items[0].outcome is O.ACTIVE_RESERVATION_NOT_OBSERVED
    result = resolve(auth, repo, ev)

    assert result.decision in {RD.HOLD, RD.REPAIR_REQUIRED}
    assert result.decision is RD.HOLD and result.items[0].decision is D.HOLD
    reserved_after = sum((r.risk_usd for r in auth.state().ledger("p1").reservations), Decimal("0"))
    assert reserved_before == reserved_after == Dec("500")                      # reserved risk after = $500
    assert derive_evaluation_snapshot(auth.state().ledger("p1")).open_risk_usd - open_risk_before == 0  # +$0 capacity
    assert auth.lifecycle_state().owner_of(res.child_trade_id) is EO.ACTIVE_RESERVATION
    assert_nothing_changed(auth, repo, before_state, before_store)
    assert ResolutionResult.RELEASES_CAPACITY is False and RepairPlan.EXECUTES is False
    # still held on every replay and after a restart
    for _ in range(3):
        assert resolve(auth, repo, ev) == result
    restored, _ = restore(repo)
    assert resolve(restored, repo, ev) == result
    assert capacity(restored) == capacity(auth)


# ---- every Day-6 outcome ----------------------------------------------------------------------------

def _scenario(outcome):
    auth, repo, ids = populated()
    a, b, c, d = (ids[k].child_trade_id for k in "abcd")
    base = [obj("ORD-A", a, K.PENDING_ORDER), obj("POS-B", b)]            # clean for A (active) and B (committed)
    objects = {
        O.MATCHED_ACTIVE_RESERVATION_PENDING_ORDER: base,
        O.MATCHED_COMMITTED_POSITION: base,
        O.MATCHED_RELEASED_ABSENT: base,
        O.ACTIVE_RESERVATION_NOT_OBSERVED: [obj("POS-B", b)],
        O.ACTIVE_RESERVATION_OBSERVED_AS_POSITION: [obj("POS-A", a), obj("POS-B", b)],
        O.COMMITTED_POSITION_EXTERNALLY_MISSING: [obj("ORD-A", a, K.PENDING_ORDER)],
        O.COMMITTED_POSITION_OBSERVED_AS_PENDING_ORDER: [obj("ORD-A", a, K.PENDING_ORDER), obj("ORD-B", b, K.PENDING_ORDER)],
        O.RELEASED_BUT_EXTERNALLY_PRESENT: base + [obj("POS-C", c)],
        O.EXTERNAL_OBJECT_UNOWNED: base + [obj("MANUAL", None)],
        O.EXTERNAL_OBJECT_OWNED_BY_OTHER_ACCOUNT: base + [obj("FOREIGN", d)],
        O.DUPLICATE_EXTERNAL_OWNERSHIP: [obj("ORD-A", a, K.PENDING_ORDER), obj("POS-B1", b), obj("POS-B2", b)],
        O.OWNERSHIP_ATTRIBUTE_MISMATCH: [obj("ORD-A", a, K.PENDING_ORDER), obj("POS-B", b, side=Side.SELL)],
    }[outcome]
    return auth, repo, ids, evidence(auth, repo, "p1", P1, *objects)


EXPECTED_ITEM = {o: row[0] for o, row in DECISION_TABLE.items()}


@pytest.mark.parametrize("outcome", list(O), ids=[o.value for o in O])
def test_every_day6_outcome_resolves_per_decision_table_without_side_effects(outcome):
    auth, repo, ids, ev = _scenario(outcome)
    before_state, before_store, cap = auth.lifecycle_state(), repo.snapshot(), capacity(auth)
    result = resolve(auth, repo, ev)
    item = items_by_outcome(result)[outcome]
    assert item.decision is EXPECTED_ITEM[outcome]
    if item.decision is D.CONFLICT_BLOCK:
        assert result.decision is RD.REJECT and result.reason_code is RC.HARD_CONFLICT
    assert_nothing_changed(auth, repo, before_state, before_store)
    assert capacity(auth) == cap
    for owner_child, owner in ((ids["a"].child_trade_id, EO.ACTIVE_RESERVATION),
                               (ids["b"].child_trade_id, EO.COMMITTED_POSITION),
                               (ids["c"].child_trade_id, EO.RELEASED)):
        assert auth.lifecycle_state().owner_of(owner_child) is owner


def test_decision_table_is_complete_and_never_releases():
    assert set(DECISION_TABLE) == set(O)
    transitions = {row[2] for row in DECISION_TABLE.values() if row[2] is not None}
    assert transitions == {(EO.ACTIVE_RESERVATION, EO.COMMITTED_POSITION)}
    assert all(EO.RELEASED not in t for t in transitions)
    assert DECISION_TABLE[O.ACTIVE_RESERVATION_NOT_OBSERVED] == (D.HOLD, None, None)


# ---- named scenarios ----------------------------------------------------------------------------------

def test_clean_reconciliation_is_acknowledged_without_changes():
    auth, repo = fresh()
    res = reserve(auth, "100", "A")
    ev = evidence(auth, repo, "p1", P1, obj("ORD-A", res.child_trade_id, K.PENDING_ORDER))
    before_state, before_store = auth.lifecycle_state(), repo.snapshot()
    result = resolve(auth, repo, ev)
    assert (result.decision, result.reason_code, result.repair_plans) == (RD.ACCEPT, RC.CLEAN_RECONCILIATION, ())
    assert_nothing_changed(auth, repo, before_state, before_store)


def test_reservation_seen_as_position_produces_declarative_transfer_plan_only():
    auth, repo = fresh()
    res = reserve(auth, "100", "A")
    ev = evidence(auth, repo, "p1", P1, obj("POS-A", res.child_trade_id))
    before_state, before_store = auth.lifecycle_state(), repo.snapshot()
    result = resolve(auth, repo, ev)
    assert result.decision is RD.REPAIR_REQUIRED and len(result.repair_plans) == 1
    plan = result.repair_plans[0]
    assert (plan.child_trade_id, plan.expected_current_owner, plan.observed_external_state, plan.required_action,
            plan.required_transition, plan.evidence_id) == \
           (res.child_trade_id, EO.ACTIVE_RESERVATION, (("POS-A", K.POSITION),), RA.TRANSFER_TO_COMMITTED_POSITION,
            (EO.ACTIVE_RESERVATION, EO.COMMITTED_POSITION), ev.evidence_id)
    assert "not executed" in plan.reason and plan.EXECUTES is False
    assert auth.lifecycle_state().owner_of(res.child_trade_id) is EO.ACTIVE_RESERVATION    # NOT transferred
    assert_nothing_changed(auth, repo, before_state, before_store)


def test_committed_position_missing_never_infers_closure_or_release():
    auth, repo = fresh()
    res = reserve(auth, "100", "A")
    auth.reference_transition(transition(res, EO.COMMITTED_POSITION))
    ev = evidence(auth, repo)
    cap = capacity(auth)
    result = resolve(auth, repo, ev)
    assert result.decision is RD.REPAIR_REQUIRED
    plan = result.repair_plans[0]
    assert plan.required_action is RA.INVESTIGATE_COMMITTED_POSITION_MISSING and plan.required_transition is None
    assert auth.lifecycle_state().owner_of(res.child_trade_id) is EO.COMMITTED_POSITION
    assert capacity(auth) == cap and capacity(auth)[0] == Dec("500")


def test_unowned_external_exposure_is_hard_conflict_and_never_adopted():
    auth, repo = fresh()
    ev = evidence(auth, repo, "p1", P1, obj("MANUAL-1", None), obj("GHOST", "UNKNOWN:p1"))
    before_state, before_store = auth.lifecycle_state(), repo.snapshot()
    result = resolve(auth, repo, ev)
    assert (result.decision, result.reason_code) == (RD.REJECT, RC.HARD_CONFLICT)
    assert {p.required_action for p in result.repair_plans} == {RA.ADOPTION_POLICY_REQUIRED}
    assert all(p.expected_current_owner is None and p.required_transition is None for p in result.repair_plans)
    assert auth.state().ledger("p1").reservations == () and auth.lifecycle_state().records == ()
    assert_nothing_changed(auth, repo, before_state, before_store)


def test_released_but_present_is_hard_conflict_and_never_resurrected():
    auth, repo = fresh()
    res = reserve(auth, "100", "A")
    auth.reference_transition(transition(res, EO.RELEASED))
    ev = evidence(auth, repo, "p1", P1, obj("POS-A", res.child_trade_id))
    result = resolve(auth, repo, ev)
    assert (result.decision, result.reason_code) == (RD.REJECT, RC.HARD_CONFLICT)
    assert auth.lifecycle_state().owner_of(res.child_trade_id) is EO.RELEASED
    assert auth.state().reservation_for(res.child_trade_id) is None


def test_duplicate_ownership_is_hard_conflict():
    auth, repo = fresh()
    res = reserve(auth, "100", "A")
    ev = evidence(auth, repo, "p1", P1, obj("P1", res.child_trade_id), obj("P2", res.child_trade_id))
    result = resolve(auth, repo, ev)
    assert result.decision is RD.REJECT and items_by_outcome(result)[O.DUPLICATE_EXTERNAL_OWNERSHIP].decision is D.CONFLICT_BLOCK


def test_aggregation_precedence_and_plans_preserved_under_reject():
    auth, repo = fresh()
    held = reserve(auth, "50", "HELD")
    seen = reserve(auth, "50", "SEEN")
    hold_and_repair = evidence(auth, repo, "p1", P1, obj("POS-SEEN", seen.child_trade_id))
    assert resolve(auth, repo, hold_and_repair).decision is RD.REPAIR_REQUIRED
    with_conflict = evidence(auth, repo, "p1", P1, obj("POS-SEEN", seen.child_trade_id), obj("MANUAL", None))
    result = resolve(auth, repo, with_conflict)
    assert result.decision is RD.REJECT
    assert {p.required_action for p in result.repair_plans} == {RA.TRANSFER_TO_COMMITTED_POSITION, RA.ADOPTION_POLICY_REQUIRED}
    assert {i.decision for i in result.items} == {D.HOLD, D.REPAIR_REQUIRED, D.CONFLICT_BLOCK}


def test_rejected_reconciliation_resolves_to_reject():
    auth, repo = fresh()
    ev = evidence(auth, repo, "p1", P1, obj("DUP", None), obj("DUP", None))
    assert ev.report.status is RS.REJECTED
    result = resolve(auth, repo, ev)
    assert (result.decision, result.reason_code, result.items) == (RD.REJECT, RC.RECONCILIATION_REJECTED, ())


# ---- stale evidence ------------------------------------------------------------------------------------

def test_stale_evidence_after_state_revision_changes():
    auth, repo = fresh()
    reserve(auth, "100", "A")
    ev = evidence(auth, repo)
    reserve(auth, "100", "B")                                     # persisted revision moves on
    result = resolve(auth, repo, ev)
    assert (result.decision, result.reason_code) == (RD.REJECT, RC.STALE_EVIDENCE)
    assert result.items == () and result.repair_plans == ()


def test_stale_evidence_by_age_and_future_snapshot():
    auth, repo = fresh()
    reserve(auth, "100", "A")
    ev = evidence(auth, repo, observed_at=NOW)
    assert resolve(auth, repo, ev, at=NOW + timedelta(minutes=5)).decision is RD.HOLD           # == max age: fresh
    old = resolve(auth, repo, ev, at=NOW + timedelta(minutes=5, microseconds=1))
    assert (old.decision, old.reason_code) == (RD.REJECT, RC.STALE_EVIDENCE)
    future = resolve(auth, repo, ev, at=NOW - timedelta(microseconds=1))
    assert (future.decision, future.reason_code) == (RD.REJECT, RC.STALE_EVIDENCE)


def test_state_not_durable_when_in_memory_differs_from_persisted_document():
    auth, repo = fresh()
    reserve(auth, "100", "A")
    ev = evidence(auth, repo)
    current = auth.lifecycle_state()
    ledger = current.authority.ledger("p1")
    diverged = ReferenceLifecycleState(current.authority.with_ledger(dataclasses.replace(
        ledger, account_state=dataclasses.replace(ledger.account_state, realized_daily_pnl=Dec("-1")))), current.records)
    result = resolve(auth, repo, ev, state=diverged)
    assert (result.decision, result.reason_code) == (RD.REJECT, RC.STATE_NOT_DURABLE)
    with pytest.raises(EvidenceBuildError):
        build_reconciliation_evidence(diverged, repo.load("p1"), snap())


# ---- forgery / identity ---------------------------------------------------------------------------------

def test_tampered_evidence_without_recomputed_id_fails_integrity():
    auth, repo, ids = populated()
    ev = evidence(auth, repo, "p1", P1, obj("MANUAL", None))
    hidden = dataclasses.replace(ev, report=dataclasses.replace(ev.report, items=(), status=RS.CLEAN))
    result = resolve(auth, repo, hidden)
    assert (result.decision, result.reason_code) == (RD.REJECT, RC.EVIDENCE_INTEGRITY_FAILED)


def test_forged_clean_report_with_recomputed_id_is_rejected():
    """Attacker hides an unowned external position by forging a CLEAN report and a valid evidence_id."""
    auth, repo, ids = populated()
    ev = evidence(auth, repo, "p1", P1, obj("ORD-A", ids["a"].child_trade_id, K.PENDING_ORDER),
                  obj("POS-B", ids["b"].child_trade_id), obj("MANUAL", None))
    clean_items = tuple(i for i in ev.report.items if i.outcome is not O.EXTERNAL_OBJECT_UNOWNED)
    forged = forge(ev, report=dataclasses.replace(ev.report, items=clean_items, status=RS.CLEAN))
    result = resolve(auth, repo, forged)
    assert (result.decision, result.reason_code) == (RD.REJECT, RC.EVIDENCE_MISMATCH)


@pytest.mark.parametrize("field", ["profile", "account", "revision", "digest", "document_of_other_account"])
def test_forged_account_profile_or_revision_identity_is_rejected(field):
    auth, repo, ids = populated()
    ev = evidence(auth, repo)
    p2_account = auth.state().ledger("p2").account_state.account
    if field == "profile":
        result = resolve(auth, repo, forge(ev, profile_id="p2"))
        assert result.reason_code in {RC.ACCOUNT_MISMATCH}
    elif field == "account":
        result = resolve(auth, repo, forge(ev, account=p2_account))
        assert result.reason_code is RC.ACCOUNT_MISMATCH
    elif field == "revision":
        result = resolve(auth, repo, forge(ev, state_revision=ev.state_revision + 1))
        assert result.reason_code is RC.STALE_EVIDENCE
    elif field == "digest":
        result = resolve(auth, repo, forge(ev, state_content_sha256="0" * 64))
        assert result.reason_code is RC.STALE_EVIDENCE
    else:
        result = resolve(auth, repo, ev, document_pid="p2")
        assert result.reason_code is RC.ACCOUNT_MISMATCH
    assert result.decision is RD.REJECT and result.repair_plans == ()


def test_foreign_child_ids_are_rejected():
    auth, repo, ids = populated()
    ev = evidence(auth, repo, "p1", P1, obj("ORD-A", ids["a"].child_trade_id, K.PENDING_ORDER))
    foreign = ids["d"].child_trade_id                                  # owned by p2
    fake = ReconciliationItem(foreign, EO.ACTIVE_RESERVATION, (), O.ACTIVE_RESERVATION_NOT_OBSERVED,
                              ev.report.items[0].classification.__class__("UNRESOLVED"), "forged")
    forged_report = dataclasses.replace(ev.report, items=ev.report.items + (fake,), status=RS.UNRESOLVED)
    forged = forge(ev, report=forged_report)
    assert resolve(auth, repo, forged).reason_code is RC.EVIDENCE_MISMATCH       # caught by recomputation


def test_foreign_child_defense_in_depth_when_reconciliation_is_subverted(monkeypatch):
    auth, repo, ids = populated()
    ev = evidence(auth, repo)
    fake = ReconciliationItem(ids["d"].child_trade_id, EO.ACTIVE_RESERVATION, (), O.ACTIVE_RESERVATION_NOT_OBSERVED,
                              ev.report.items[0].classification.__class__("UNRESOLVED"), "forged")
    forged_report = dataclasses.replace(ev.report, items=(fake,), status=RS.UNRESOLVED)
    forged = forge(ev, report=forged_report)
    monkeypatch.setattr(resolution_module, "reconcile_account", lambda state, snapshot: forged_report)
    assert resolve(auth, repo, forged).reason_code is RC.FOREIGN_CHILD


def test_unknown_outcome_fails_closed(monkeypatch):
    auth, repo, ids, ev = _scenario(O.MATCHED_COMMITTED_POSITION)
    table = dict(DECISION_TABLE)
    del table[O.MATCHED_COMMITTED_POSITION]
    monkeypatch.setattr(resolution_module, "DECISION_TABLE", table)
    result = resolve(auth, repo, ev)
    assert (result.decision, result.reason_code) == (RD.REJECT, RC.UNKNOWN_OUTCOME)


# ---- malformed / unsupported ------------------------------------------------------------------------------

@pytest.mark.parametrize("case,code", [
    ("request_version_2", RC.UNSUPPORTED_VERSION), ("request_version_str", RC.UNSUPPORTED_VERSION),
    ("evidence_version_0", RC.UNSUPPORTED_VERSION), ("evidence_not_evidence", RC.UNSUPPORTED_VERSION),
    ("blank_request_id", RC.MALFORMED_REQUEST), ("naive_time", RC.MALFORMED_REQUEST),
    ("bad_policy", RC.MALFORMED_REQUEST), ("bad_state", RC.MALFORMED_REQUEST),
    ("evidence_bad_snapshot_type", RC.MALFORMED_REQUEST),
])
def test_malformed_and_unsupported_contracts_reject(case, code):
    auth, repo = fresh()
    reserve(auth, "100", "A")
    ev = evidence(auth, repo)
    state, policy, req = auth.lifecycle_state(), POLICY, ResolutionRequest(1, "RES", ev, AT)
    if case == "request_version_2":
        req = dataclasses.replace(req, schema_version=2)
    elif case == "request_version_str":
        req = dataclasses.replace(req, schema_version="1")
    elif case == "evidence_version_0":
        req = dataclasses.replace(req, evidence=forge(ev, schema_version=0))
    elif case == "evidence_not_evidence":
        req = dataclasses.replace(req, evidence="evidence")
    elif case == "blank_request_id":
        req = dataclasses.replace(req, request_id=" ")
    elif case == "naive_time":
        req = dataclasses.replace(req, resolution_time=datetime(2026, 9, 16, 12, 0))
    elif case == "bad_policy":
        policy = ResolutionPolicy(max_evidence_age=timedelta(0))
    elif case == "bad_state":
        state = "state"
    elif case == "evidence_bad_snapshot_type":
        req = dataclasses.replace(req, evidence=dataclasses.replace(ev, snapshot="snap"))
    before_state, before_store = auth.lifecycle_state(), repo.snapshot()
    result = resolve_reconciliation(req, state, repo.load("p1"), policy)
    assert (result.decision, result.reason_code) == (RD.REJECT, code)
    assert result.items == () and result.repair_plans == ()
    assert_nothing_changed(auth, repo, before_state, before_store)


def test_evidence_builder_rejects_mismatched_inputs():
    auth, repo, ids = populated()
    with pytest.raises(EvidenceBuildError):
        build_reconciliation_evidence(auth.lifecycle_state(), repo.load("p1"), snap("p2", P2))
    with pytest.raises(EvidenceBuildError):
        build_reconciliation_evidence("state", repo.load("p1"), snap())


# ---- idempotency / determinism -------------------------------------------------------------------------------

def test_duplicate_resolution_requests_and_deterministic_replay_are_byte_identical():
    auth, repo, ids = populated()
    ev = evidence(auth, repo, "p1", P1, obj("POS-A", ids["a"].child_trade_id), obj("POS-B", ids["b"].child_trade_id))
    results = [resolve(auth, repo, ev) for _ in range(5)]
    blobs = {canonical_json(encode_result(r)) for r in results}
    assert len(blobs) == 1 and all(r == results[0] for r in results)
    restored, _ = restore(repo)                                    # a different process instance
    ev2 = evidence(restored, repo, "p1", P1, obj("POS-A", ids["a"].child_trade_id), obj("POS-B", ids["b"].child_trade_id))
    assert ev2 == ev and ev2.evidence_id == ev.evidence_id
    assert canonical_json(encode_result(resolve(restored, repo, ev2))) in blobs
    other_id = resolve(auth, repo, ev, request_id="RES-2")
    assert other_id.decision is results[0].decision and other_id.result_id != results[0].result_id
    assert results[0].result_id == hashlib.sha256(
        canonical_json(encode_result(results[0], include_id=False)).encode()).hexdigest()


# ---- multi-account isolation -------------------------------------------------------------------------------------

def test_multi_account_isolation():
    auth, repo, ids = populated()
    p2_ledger, p3_ledger = auth.state().ledger("p2"), auth.state().ledger("p3")
    store = repo.snapshot()
    ev1 = evidence(auth, repo, "p1", P1, obj("POS-A", ids["a"].child_trade_id))
    r1 = resolve(auth, repo, ev1)
    assert r1.profile_id == "p1" and all(p.profile_id == "p1" for p in r1.repair_plans)
    assert all(i.child_trade_id is None or i.child_trade_id.endswith(":p1") for i in r1.items)
    cross = resolve(auth, repo, ev1, document_pid="p2")
    assert (cross.decision, cross.reason_code) == (RD.REJECT, RC.ACCOUNT_MISMATCH)
    assert auth.state().ledger("p2") is p2_ledger and auth.state().ledger("p3") is p3_ledger
    assert repo.snapshot() == store
    # p2's evidence cannot be resolved against p1's document
    ev2 = evidence(auth, repo, "p2", P2)
    assert resolve(auth, repo, ev2, document_pid="p1").reason_code is RC.ACCOUNT_MISMATCH
    assert resolve(auth, repo, ev2).profile_id == "p2"


# ---- contract / purity ----------------------------------------------------------------------------------------------

def test_result_contract_rejects_unsafe_combinations():
    auth, repo, ids, ev = _scenario(O.EXTERNAL_OBJECT_UNOWNED)
    rejected = resolve(auth, repo, ev)
    with pytest.raises(Exception):
        dataclasses.replace(rejected, decision=RD.ACCEPT)                    # conflict cannot be accepted
    with pytest.raises(Exception):
        dataclasses.replace(rejected, decision=RD.HOLD)
    accepted = resolve(*_scenario(O.MATCHED_COMMITTED_POSITION)[:2], _scenario(O.MATCHED_COMMITTED_POSITION)[3])
    with pytest.raises(Exception):
        dataclasses.replace(accepted, repair_plans=rejected.repair_plans)    # ACCEPT with plans
    for obj_, field in [(rejected, "decision"), (rejected.repair_plans[0], "required_action"), (ev, "report")]:
        with pytest.raises(dataclasses.FrozenInstanceError):
            setattr(obj_, field, None)


DAY7_FILES = ("resolution_contracts.py", "resolution_reference.py")
MUTATORS = {"reference_transition", "authorize_and_reserve", "register_account", "replace_account_state",
            "_commit", "_commit_lifecycle", "_persist_then_swap", "save", "restore", "reference_reconcile",
            "_transition_locked", "_authorize_locked"}


def test_no_mutation_authority_execution_or_production_dependency():
    from api.xtl_platform.tests.test_isolation import PACKAGE, PKG_ROOT, PRODUCTION_MANIFEST, scan_source
    for name in DAY7_FILES:
        source = (PKG_ROOT / "risk" / name).read_text()
        assert f"risk/{name}" in PRODUCTION_MANIFEST
        assert scan_source(source, PACKAGE + ("risk",)) == [], name
        tree = ast.parse(source)
        called = {getattr(n.func, "attr", getattr(n.func, "id", None)) for n in ast.walk(tree) if isinstance(n, ast.Call)}
        names = {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)} | \
                {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)}
        assert called.isdisjoint(MUTATORS) and names.isdisjoint(MUTATORS), (name, (called | names) & MUTATORS)
        absolute = {n.module for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.level == 0} | \
                   {a.name for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names}
        assert {m.split(".")[0] for m in absolute} <= {"__future__", "dataclasses", "datetime", "typing", "json"}, name
        assert names.isdisjoint({"now", "utcnow", "today", "uuid4", "uuid1", "random", "environ", "getenv",
                                 "time", "monotonic", "open"}), name
        assert "REFERENCE ONLY - NON-PRODUCTION" in source
    assert (ResolutionResult.RELEASES_CAPACITY, ResolutionResult.MUTATES_LIFECYCLE, ResolutionResult.AUTHORIZES_RISK,
            ResolutionResult.GRANTS_PRODUCTION_AUTHORITY, RepairPlan.EXECUTES) == (False, False, False, False, False)
    import api.xtl_platform.risk as risk_pkg
    assert risk_pkg.__all__ == ["RiskAuthority"]


FROZEN_DAY6_SHA256 = {
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
    "tests/test_risk_evaluator.py": "e64c800d1344cad6d0469eebf323b61d7963e318ade8cf924921d066e2929d82",
    "tests/test_thesis_fanout.py": "1fb20203b692d95c6f45054b804f051b6e840ab0079ca4cf4c6fe9d607ba4ea3",
}


def test_frozen_day1_to_day6_files_unchanged():
    from api.xtl_platform.tests.test_isolation import PKG_ROOT
    changed = [rel for rel, digest in FROZEN_DAY6_SHA256.items()
               if hashlib.sha256((PKG_ROOT / rel).read_bytes()).hexdigest() != digest]
    assert changed == [] and len(FROZEN_DAY6_SHA256) == 49


# ---- gap found by mutation testing (Q19) -------------------------------------------------------------------

def test_evidence_id_binds_snapshot_fields_that_do_not_appear_in_the_report():
    """Changing an external object's symbol does not change the Day-6 report of an UNOWNED
    object, so only the evidence_id binding can detect it."""
    auth, repo = fresh()
    ev = evidence(auth, repo, "p1", P1, obj("MANUAL", None, symbol="XAUUSD"))
    swapped = dataclasses.replace(ev.snapshot, objects=(obj("MANUAL", None, symbol="EURUSD"),))
    from api.xtl_platform.risk.reconciliation_reference import reconcile_account
    assert reconcile_account(auth.lifecycle_state(), swapped) == ev.report          # report cannot see the change
    tampered = dataclasses.replace(ev, snapshot=swapped)                            # evidence_id NOT recomputed
    result = resolve(auth, repo, tampered)
    assert (result.decision, result.reason_code) == (RD.REJECT, RC.EVIDENCE_INTEGRITY_FAILED)
