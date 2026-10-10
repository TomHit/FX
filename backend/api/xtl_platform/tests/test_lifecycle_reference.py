"""Day 5 - Reservation ownership lifecycle (REFERENCE ONLY - NON-PRODUCTION).

ONE TRADE -> ONE EXPOSURE OWNER.
COMMITTED_POSITION is a reference accounting state, NOT a broker position.
Numbered tests map 1:1 to the Day-5 brief, TEST REQUIREMENTS 1-26.
All limit values are TEST FIXTURE - NOT VERIFIED PRODUCTION FIRM RULE.
"""
from __future__ import annotations

import ast
import dataclasses
import threading
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from api.xtl_platform.domain.account import AccountIdentity
from api.xtl_platform.domain.enums import StrategyId
from api.xtl_platform.risk import lifecycle_reference as lifecycle_module
from api.xtl_platform.risk.atomic_contracts import AtomicAuthorizationCode as A
from api.xtl_platform.risk.atomic_reference import ReferenceAtomicRiskAuthority, derive_evaluation_snapshot
from api.xtl_platform.risk.contracts import CapacityCheckName as N, RiskEvaluationRequest
from api.xtl_platform.risk.evaluator import evaluate_risk
from api.xtl_platform.risk.lifecycle_contracts import (
    ExposureOwner as O,
    LifecycleTransitionCode as T,
    LifecycleTransitionRequest,
    LifecycleTransitionResult,
    ReferenceLifecycleState,
    ReservationLifecycleRecord,
)
from api.xtl_platform.risk.lifecycle_reference import ReferenceLifecycleRiskAuthority
from api.xtl_platform.tests._builders import NOW, D, make_profile
from api.xtl_platform.tests.test_atomic_reference import CONFIG, P1, P2, FakeClock, account_state, request, run_concurrently

UTC = timezone.utc


# ---- builders ---------------------------------------------------------------------------

def lifecycle_authority(clock=None, *states) -> ReferenceLifecycleRiskAuthority:
    auth = ReferenceLifecycleRiskAuthority(CONFIG, clock or FakeClock(), acknowledge_reference_only_non_production=True)
    for s in (states or (account_state(committed_open_risk_usd=D("400"), committed_open_positions=2,
                                       limit_kw=dict(max_open_risk_usd=D("1000"), max_positions=5)),)):
        auth.register_account(s)
    return auth


def reserve(auth, risk="100", thesis_id="A-1", strategy=StrategyId.STRATEGY_A, profile=P1):
    r = auth.authorize_and_reserve(request(risk=risk, thesis_id=thesis_id, strategy=strategy, profile=profile))
    assert r.authorized, r.reason
    return r.reservation


def transition(reservation, target, transition_id=None, reason=None, profile_id=None, account=None,
               reservation_id=None, requested_at=NOW):
    return LifecycleTransitionRequest(
        transition_id=transition_id or f"{target.value}:{reservation.child_trade_id}",
        child_trade_id=reservation.child_trade_id,
        reservation_id=reservation_id or reservation.reservation_id,
        profile_id=profile_id or reservation.profile_id,
        account=account or reservation.account,
        target_owner=target,
        reason=reason or ("reference fill accounting" if target is O.COMMITTED_POSITION else "reference cancel"),
        requested_at=requested_at,
    )


def used(auth, pid="p1"):
    """(evaluated open risk, used position slots, committed risk, reservation risk, committed positions, slots)."""
    ledger = auth.state().ledger(pid)
    snap = derive_evaluation_snapshot(ledger)
    reservation_risk = sum((r.risk_usd for r in ledger.reservations), Decimal("0"))
    return (snap.open_risk_usd, snap.open_positions + snap.reserved_position_slots,
            ledger.account_state.committed_open_risk_usd, reservation_risk,
            ledger.account_state.committed_open_positions, snap.reserved_position_slots)


@pytest.fixture
def slow_build(monkeypatch):
    """Widen the window inside the transition's state construction."""
    original = lifecycle_module.ReservationLifecycleRecord

    def slow(*a, **k):
        time.sleep(0.002)
        return original(*a, **k)

    monkeypatch.setattr(lifecycle_module, "ReservationLifecycleRecord", slow)


# ---- 1-5 transitions and capacity -----------------------------------------------------------

def test_01_active_reservation_to_committed_transfer():
    auth = lifecycle_authority()
    res = reserve(auth)
    r = auth.reference_transition(transition(res, O.COMMITTED_POSITION))
    assert r.code is T.TRANSITION_APPLIED and not r.idempotent_replay
    assert (r.previous_owner, r.resulting_owner) == (O.ACTIVE_RESERVATION, O.COMMITTED_POSITION)
    state = auth.lifecycle_state()
    assert state.owner_of(res.child_trade_id) is O.COMMITTED_POSITION
    assert state.authority.reservation_for(res.child_trade_id) is None
    assert r.record.transferred_risk_usd == D("100") and r.record.transferred_position_slots == 1


def test_02_active_reservation_to_released():
    auth = lifecycle_authority()
    res = reserve(auth)
    r = auth.reference_transition(transition(res, O.RELEASED))
    assert r.applied and (r.previous_owner, r.resulting_owner) == (O.ACTIVE_RESERVATION, O.RELEASED)
    assert auth.lifecycle_state().owner_of(res.child_trade_id) is O.RELEASED
    assert r.record.transferred_risk_usd == D("0") and r.record.transferred_position_slots == 0


def test_03_risk_capacity_unchanged_across_transfer():
    auth = lifecycle_authority()
    res = reserve(auth, risk="100")
    before = used(auth)
    assert (before[0], before[2], before[3]) == (D("500"), D("400"), D("100"))   # evaluated / committed / reserved
    auth.reference_transition(transition(res, O.COMMITTED_POSITION))
    after = used(auth)
    assert (after[0], after[2], after[3]) == (D("500"), D("500"), D("0"))


def test_04_position_capacity_unchanged_across_transfer():
    auth = lifecycle_authority()
    res = reserve(auth)
    before = used(auth)
    assert (before[1], before[4], before[5]) == (3, 2, 1)       # used / committed / reserved slots
    auth.reference_transition(transition(res, O.COMMITTED_POSITION))
    after = used(auth)
    assert (after[1], after[4], after[5]) == (3, 3, 0)


def test_05_released_reservation_stops_consuming_capacity():
    auth = lifecycle_authority()
    res = reserve(auth, risk="100")
    auth.reference_transition(transition(res, O.RELEASED))
    evaluated_risk, used_slots, committed_risk, reserved_risk, committed_positions, slots = used(auth)
    assert (evaluated_risk, committed_risk, reserved_risk) == (D("400"), D("400"), D("0"))
    assert (used_slots, committed_positions, slots) == (2, 2, 0)


# ---- 6-9 idempotency ----------------------------------------------------------------------------

def test_06_exact_transfer_replay_is_idempotent():
    auth = lifecycle_authority()
    res = reserve(auth)
    first = auth.reference_transition(transition(res, O.COMMITTED_POSITION))
    state_after_first = auth.lifecycle_state()
    replay = auth.reference_transition(transition(res, O.COMMITTED_POSITION, requested_at=NOW + timedelta(seconds=9)))
    assert replay.applied and replay.idempotent_replay and replay.record is first.record
    assert auth.lifecycle_state() is state_after_first


def test_07_exact_release_replay_is_idempotent():
    auth = lifecycle_authority()
    res = reserve(auth)
    first = auth.reference_transition(transition(res, O.RELEASED))
    state_after_first = auth.lifecycle_state()
    for _ in range(3):
        replay = auth.reference_transition(transition(res, O.RELEASED))
        assert replay.applied and replay.idempotent_replay and replay.record is first.record
    assert auth.lifecycle_state() is state_after_first


def test_08_transfer_replay_cannot_double_add_risk():
    auth = lifecycle_authority()
    res = reserve(auth, risk="100")
    for _ in range(5):
        auth.reference_transition(transition(res, O.COMMITTED_POSITION))
    assert used(auth)[:5] == (D("500"), 3, D("500"), D("0"), 3)


def test_09_release_replay_cannot_double_remove_risk():
    auth = lifecycle_authority()
    keep = reserve(auth, risk="100", thesis_id="KEEP")
    gone = reserve(auth, risk="200", thesis_id="GONE")
    for _ in range(5):
        auth.reference_transition(transition(gone, O.RELEASED))
    evaluated_risk, used_slots, committed_risk, reserved_risk, _, slots = used(auth)
    assert (evaluated_risk, reserved_risk, committed_risk, used_slots, slots) == (D("500"), D("100"), D("400"), 3, 1)
    assert auth.lifecycle_state().owner_of(keep.child_trade_id) is O.ACTIVE_RESERVATION


# ---- 10-14 fail closed ------------------------------------------------------------------------------

def _assert_unchanged_block(auth, req, code):
    before = auth.lifecycle_state()
    r = auth.reference_transition(req)
    assert r.code is code, r.reason
    assert r.record is None and not r.idempotent_replay and r.resulting_owner is r.previous_owner
    assert auth.lifecycle_state() is before
    return r


def test_10_released_to_committed_fails_closed():
    auth = lifecycle_authority()
    res = reserve(auth)
    auth.reference_transition(transition(res, O.RELEASED))
    r = _assert_unchanged_block(auth, transition(res, O.COMMITTED_POSITION), T.BLOCK_INVALID_TRANSITION)
    assert r.previous_owner is O.RELEASED


def test_11_committed_to_released_fails_closed_for_day5():
    auth = lifecycle_authority()
    res = reserve(auth)
    auth.reference_transition(transition(res, O.COMMITTED_POSITION))
    r = _assert_unchanged_block(auth, transition(res, O.RELEASED), T.BLOCK_INVALID_TRANSITION)
    assert r.previous_owner is O.COMMITTED_POSITION


@pytest.mark.parametrize("target", [O.COMMITTED_POSITION, O.RELEASED])
@pytest.mark.parametrize("variant", ["other_transition_id", "other_reason", "transition_id_of_other_child"])
def test_12_conflicting_duplicate_transition_fails_closed(target, variant):
    auth = lifecycle_authority()
    res = reserve(auth, thesis_id="A-1")
    other = reserve(auth, thesis_id="A-2")
    auth.reference_transition(transition(res, target))
    auth.reference_transition(transition(other, O.RELEASED, transition_id="TID-OTHER"))
    conflicting = {
        "other_transition_id": transition(res, target, transition_id="TID-NEW"),
        "other_reason": transition(res, target, reason="a different reason"),
        "transition_id_of_other_child": transition(res, target, transition_id="TID-OTHER"),
    }[variant]
    _assert_unchanged_block(auth, conflicting, T.BLOCK_CONFLICTING_TRANSITION)


def test_12b_target_active_reservation_is_invalid():
    auth = lifecycle_authority()
    res = reserve(auth)
    _assert_unchanged_block(auth, transition(res, O.ACTIVE_RESERVATION), T.BLOCK_INVALID_TRANSITION)


def test_13_unknown_reservation_fails_closed():
    auth = lifecycle_authority()
    res = reserve(auth)
    ghost = dataclasses.replace(transition(res, O.RELEASED), child_trade_id="NOPE:p1", transition_id="t-ghost")
    _assert_unchanged_block(auth, ghost, T.BLOCK_UNKNOWN_RESERVATION)
    _assert_unchanged_block(auth, transition(res, O.RELEASED, reservation_id="REF-RESV:other"),
                            T.BLOCK_UNKNOWN_RESERVATION)


@pytest.mark.parametrize("field", ["profile", "account"])
def test_14_wrong_account_or_profile_fails_closed(field):
    auth = lifecycle_authority()
    res = reserve(auth)
    kw = {"profile": dict(profile_id="p2"), "account": dict(account=AccountIdentity("999", "S", "C", "USD"))}[field]
    _assert_unchanged_block(auth, transition(res, O.COMMITTED_POSITION, **kw), T.BLOCK_ACCOUNT_MISMATCH)
    auth.reference_transition(transition(res, O.COMMITTED_POSITION))
    _assert_unchanged_block(auth, transition(res, O.COMMITTED_POSITION, **kw), T.BLOCK_ACCOUNT_MISMATCH)


@pytest.mark.parametrize("change", [dict(transition_id=" "), dict(reason=""), dict(target_owner="RELEASED"),
                                    dict(account="acct"), dict(requested_at=datetime(2026, 9, 16, 12, 0))])
def test_14b_invalid_transition_request_fails_closed(change):
    auth = lifecycle_authority()
    res = reserve(auth)
    _assert_unchanged_block(auth, dataclasses.replace(transition(res, O.RELEASED), **change),
                            T.BLOCK_INVALID_TRANSITION_REQUEST)


# ---- 15-16 shared capacity / isolation ------------------------------------------------------------

def test_15_strategy_ab_shared_account_capacity_preserved():
    auth = lifecycle_authority(None, account_state(limit_kw=dict(max_open_risk_usd=D("500"), max_positions=5)))
    a = reserve(auth, risk="300", thesis_id="A-1", strategy=StrategyId.STRATEGY_A)
    b = reserve(auth, risk="200", thesis_id="B-1", strategy=StrategyId.STRATEGY_B)
    auth.reference_transition(transition(a, O.COMMITTED_POSITION))
    ledger = auth.state().ledger("p1")
    assert ledger.reservations == (b,)                                   # B untouched, same object
    assert used(auth)[:4] == (D("500"), 2, D("300"), D("200"))
    blocked = auth.authorize_and_reserve(request(risk="1", thesis_id="A-2"))
    assert blocked.code is A.BLOCK_OPEN_RISK                              # still one shared pool
    auth.reference_transition(transition(b, O.RELEASED))
    assert auth.authorize_and_reserve(request(risk="200", thesis_id="A-3", strategy=StrategyId.STRATEGY_A)).authorized


def test_16_different_accounts_isolated():
    auth = lifecycle_authority(None, account_state("p1", P1), account_state("p2", P2))
    r1 = reserve(auth, risk="300", profile=P1)
    r2 = reserve(auth, risk="300", profile=P2)
    p2_before = auth.state().ledger("p2")
    auth.reference_transition(transition(r1, O.COMMITTED_POSITION))
    assert auth.state().ledger("p2") is p2_before
    assert auth.lifecycle_state().owner_of(r2.child_trade_id) is O.ACTIVE_RESERVATION
    assert used(auth, "p1")[:4] == (D("300"), 1, D("300"), D("0"))
    assert used(auth, "p2")[:4] == (D("300"), 1, D("0"), D("300"))


# ---- 17 failure injection --------------------------------------------------------------------------

@pytest.mark.parametrize("failure", ["record_raises", "state_raises", "with_ledger_raises", "replace_raises",
                                     "clock_raises", "clock_naive", "clock_before_creation", "invalid_committed"])
@pytest.mark.parametrize("target", [O.COMMITTED_POSITION, O.RELEASED])
def test_17_failure_injection_leaves_state_identical(monkeypatch, failure, target):
    clock = FakeClock()
    auth = lifecycle_authority(clock)
    keep = reserve(auth, thesis_id="KEEP")
    res = reserve(auth, thesis_id="A-1")
    auth.reference_transition(transition(keep, O.RELEASED))
    if failure == "invalid_committed":
        auth.replace_account_state(dataclasses.replace(auth.state().ledger("p1").account_state,
                                                       committed_open_positions="2"))
    before = auth.lifecycle_state()
    before_repr = repr(before)
    boom = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("injected"))
    if failure == "record_raises":
        monkeypatch.setattr(lifecycle_module, "ReservationLifecycleRecord", boom)
    elif failure == "state_raises":
        monkeypatch.setattr(lifecycle_module, "ReferenceLifecycleState", boom)
    elif failure == "with_ledger_raises":
        monkeypatch.setattr(type(before.authority), "with_ledger", boom)
    elif failure == "replace_raises":
        monkeypatch.setattr(lifecycle_module.dataclasses, "replace", boom)
        if target is O.RELEASED:
            monkeypatch.setattr(lifecycle_module, "ReferenceLedger", boom)
    elif failure == "clock_raises":
        auth._clock = boom
    elif failure == "clock_naive":
        auth._clock = lambda: datetime(2026, 9, 16, 12, 0)
    elif failure == "clock_before_creation":
        clock.now = NOW - timedelta(seconds=1)
    r = auth.reference_transition(transition(res, target))
    monkeypatch.undo()
    if failure == "invalid_committed" and target is O.RELEASED:
        # Release never touches committed exposure, so it is not blocked by it.
        assert r.applied and auth.state().ledger("p1").account_state.committed_open_positions == "2"
        return
    assert r.code is T.BLOCK_REFERENCE_TRANSITION_FAILURE and r.record is None
    after = auth.lifecycle_state()
    assert after is before and after == before and repr(after) == before_repr
    assert after.owner_of(res.child_trade_id) is O.ACTIVE_RESERVATION


def test_17b_double_owned_state_cannot_be_constructed():
    auth = lifecycle_authority()
    res = reserve(auth)
    applied = auth.reference_transition(transition(res, O.COMMITTED_POSITION)).record
    with_reservation = auth.state().with_ledger(dataclasses.replace(
        auth.state().ledger("p1"), reservations=(res,)))
    with pytest.raises(Exception, match="single-owner"):
        ReferenceLifecycleState(with_reservation, (applied,))


# ---- 18-20 concurrency ---------------------------------------------------------------------------------

ROUNDS = 25


def test_18_concurrent_transfer_attempts_one_ownership_transfer(slow_build):
    for i in range(ROUNDS):
        auth = lifecycle_authority()
        res = reserve(auth, thesis_id=f"C-{i}")
        same = run_concurrently([lambda: auth.reference_transition(transition(res, O.COMMITTED_POSITION))] * 8)
        assert sum(r.applied and not r.idempotent_replay for r in same) == 1
        assert all(r.applied for r in same) and len({id(r.record) for r in same}) == 1
        assert used(auth)[:5] == (D("500"), 3, D("500"), D("0"), 3)

        auth2 = lifecycle_authority()
        res2 = reserve(auth2, thesis_id=f"D-{i}")
        different = run_concurrently([
            (lambda k=k: auth2.reference_transition(transition(res2, O.COMMITTED_POSITION, transition_id=f"T{k}")))
            for k in range(8)])
        assert sum(r.applied for r in different) == 1
        assert all(r.code is T.BLOCK_CONFLICTING_TRANSITION for r in different if not r.applied)
        assert used(auth2)[:5] == (D("500"), 3, D("500"), D("0"), 3)


def test_19_concurrent_release_attempts_release_once(slow_build):
    for i in range(ROUNDS):
        auth = lifecycle_authority()
        keep = reserve(auth, risk="150", thesis_id=f"K-{i}")
        res = reserve(auth, risk="100", thesis_id=f"R-{i}")
        results = run_concurrently([lambda: auth.reference_transition(transition(res, O.RELEASED))] * 8)
        assert sum(r.applied and not r.idempotent_replay for r in results) == 1
        assert all(r.applied for r in results)
        assert used(auth)[:4] == (D("550"), 3, D("400"), D("150"))
        assert auth.state().ledger("p1").reservations == (keep,)
        assert len(auth.lifecycle_state().records) == 1


def test_20_concurrent_transfer_vs_release_exactly_one_terminal_owner(slow_build):
    outcomes = set()
    for i in range(ROUNDS * 2):
        auth = lifecycle_authority()
        res = reserve(auth, thesis_id=f"X-{i}")
        t, r = run_concurrently([lambda: auth.reference_transition(transition(res, O.COMMITTED_POSITION)),
                                 lambda: auth.reference_transition(transition(res, O.RELEASED))])
        assert (t.applied + r.applied) == 1
        loser = r if t.applied else t
        assert loser.code is T.BLOCK_INVALID_TRANSITION
        owner = auth.lifecycle_state().owner_of(res.child_trade_id)
        outcomes.add(owner)
        expected = (D("500"), 3, D("500"), D("0"), 3) if owner is O.COMMITTED_POSITION else (D("400"), 2, D("400"), D("0"), 2)
        assert used(auth)[:5] == expected
        assert len(auth.lifecycle_state().records) == 1
    assert outcomes <= {O.COMMITTED_POSITION, O.RELEASED}


def test_20b_observer_never_sees_intermediate_capacity(slow_build):
    """A concurrent reader must only ever see (500, 3) during a transfer - never 400/600 or 2/4."""
    for i in range(ROUNDS):
        auth = lifecycle_authority()
        res = reserve(auth, thesis_id=f"O-{i}")
        seen, stop = set(), threading.Event()

        def observe():
            while not stop.is_set():
                ledger = auth.state().ledger("p1")
                snap = derive_evaluation_snapshot(ledger)
                seen.add((snap.open_risk_usd, snap.open_positions + snap.reserved_position_slots))

        observer = threading.Thread(target=observe)
        observer.start()
        auth.reference_transition(transition(res, O.COMMITTED_POSITION))
        time.sleep(0.001)
        stop.set()
        observer.join()
        assert seen == {(D("500"), 3)}, seen


def test_20c_concurrent_release_and_new_authorization_never_over_reserve(slow_build):
    for i in range(ROUNDS):
        auth = lifecycle_authority(None, account_state(limit_kw=dict(max_open_risk_usd=D("500"), max_positions=5)))
        big = reserve(auth, risk="500", thesis_id=f"BIG-{i}")
        rel, new = run_concurrently([lambda: auth.reference_transition(transition(big, O.RELEASED)),
                                     lambda: auth.authorize_and_reserve(request(risk="300", thesis_id=f"NEW-{i}"))])
        assert rel.applied
        total = derive_evaluation_snapshot(auth.state().ledger("p1")).open_risk_usd
        assert total == (D("300") if new.authorized else D("0")) and total <= D("500")


# ---- 21-22 Day-3 integration ------------------------------------------------------------------------------

def _evaluate(auth, risk="1", thesis_id="PROBE"):
    ledger = auth.state().ledger("p1")
    req = request(risk=risk, thesis_id=thesis_id)
    return evaluate_risk(RiskEvaluationRequest(req.intent, req.proposed_risk_usd, None, NOW),
                         derive_evaluation_snapshot(ledger), ledger.account_state.limits, CONFIG)


def _check(evaluation, name):
    return next(c for c in evaluation.checks if c.name is name)


def test_21_snapshot_after_transfer_counts_exposure_exactly_once():
    auth = lifecycle_authority()
    res = reserve(auth, risk="100")
    before = _evaluate(auth)
    auth.reference_transition(transition(res, O.COMMITTED_POSITION))
    after = _evaluate(auth)
    for name in (N.OPEN_RISK, N.MAX_POSITIONS, N.DAILY_LOSS, N.MAX_LOSS):
        assert _check(before, name).used == _check(after, name).used, name
    assert _check(after, N.OPEN_RISK).used == D("500")
    assert dict(_check(after, N.MAX_POSITIONS).components) == {"open_positions": D("3"), "reserved_position_slots": D("0")}
    assert dict(_check(before, N.MAX_POSITIONS).components) == {"open_positions": D("2"), "reserved_position_slots": D("1")}


def test_22_snapshot_after_release_removes_reservation_capacity():
    auth = lifecycle_authority()
    res = reserve(auth, risk="100")
    auth.reference_transition(transition(res, O.RELEASED))
    after = _evaluate(auth)
    assert _check(after, N.OPEN_RISK).used == D("400")
    assert dict(_check(after, N.MAX_POSITIONS).components) == {"open_positions": D("2"), "reserved_position_slots": D("0")}


def test_22b_transitioned_child_id_cannot_be_reauthorized():
    for target in (O.COMMITTED_POSITION, O.RELEASED):
        auth = lifecycle_authority()
        req = request(risk="100", thesis_id="ONCE")
        res = auth.authorize_and_reserve(req).reservation
        auth.reference_transition(transition(res, target))
        before = auth.lifecycle_state()
        again = auth.authorize_and_reserve(req)
        assert again.code is A.DUPLICATE_TRADE_ID and again.reservation is None
        assert str(target) in again.reason and auth.lifecycle_state() is before


# ---- 23-24 identity and audit -----------------------------------------------------------------------------------

def test_23_frozen_identity_fields_survive_transition():
    for target in (O.COMMITTED_POSITION, O.RELEASED):
        auth = lifecycle_authority()
        res = reserve(auth, risk="100", thesis_id="ID-1", strategy=StrategyId.STRATEGY_B)
        record = auth.reference_transition(transition(res, target)).record
        assert record.reservation is res
        assert (record.thesis_id, record.strategy_id, record.profile_id, record.account, record.canonical_symbol,
                record.side, record.authorized_risk_usd, record.source_policy_version, record.child_trade_id,
                record.reservation_id) == \
               (res.thesis_id, res.strategy_id, res.profile_id, res.account, res.canonical_symbol, res.side,
                res.risk_usd, res.source_policy_version, res.child_trade_id, res.reservation_id)
        assert auth.state().ledger("p1").account_state.account == res.account


def test_24_audit_evidence_deterministic_and_immutable():
    def run():
        clock = FakeClock()
        auth = lifecycle_authority(clock)
        a = reserve(auth, thesis_id="A-1")
        b = reserve(auth, thesis_id="B-1", strategy=StrategyId.STRATEGY_B)
        clock.now = NOW + timedelta(seconds=7)
        ra = auth.reference_transition(transition(a, O.COMMITTED_POSITION))
        clock.now = NOW + timedelta(seconds=9)
        rb = auth.reference_transition(transition(b, O.RELEASED))
        return auth.lifecycle_state(), (ra, rb)

    (s1, r1), (s2, r2) = run(), run()
    assert s1 == s2 and r1 == r2
    rec = s1.record_for("A-1:p1")
    assert (rec.reservation_created_at, rec.transitioned_at, rec.from_owner, rec.to_owner, rec.transition_id,
            rec.reason) == (NOW, NOW + timedelta(seconds=7), O.ACTIVE_RESERVATION, O.COMMITTED_POSITION,
                            "COMMITTED_POSITION:A-1:p1", "reference fill accounting")
    assert rec.request_fingerprint == transition(rec.reservation, O.COMMITTED_POSITION).fingerprint
    assert isinstance(s1.records, tuple)
    for obj, field in [(rec, "to_owner"), (s1, "records"), (r1[0], "code"), (transition(rec.reservation, O.RELEASED), "reason")]:
        with pytest.raises(dataclasses.FrozenInstanceError):
            setattr(obj, field, None)


def test_24b_result_and_record_contracts_reject_inconsistency():
    auth = lifecycle_authority()
    res = reserve(auth)
    ok = auth.reference_transition(transition(res, O.COMMITTED_POSITION))
    for change in [dict(record=None), dict(code=T.BLOCK_UNKNOWN_RESERVATION), dict(resulting_owner=O.RELEASED),
                   dict(target_owner=O.RELEASED)]:
        with pytest.raises(Exception):
            dataclasses.replace(ok, **change)
    for change in [dict(transferred_risk_usd=D("0")), dict(to_owner=O.ACTIVE_RESERVATION),
                   dict(from_owner=O.RELEASED), dict(transferred_position_slots=0),
                   dict(transitioned_at=NOW - timedelta(seconds=1))]:
        with pytest.raises(Exception):
            dataclasses.replace(ok.record, **change)


# ---- 25-26 formula ownership and dependencies -------------------------------------------------------------------

DAY5_FILES = ("lifecycle_contracts.py", "lifecycle_reference.py")


def _sources():
    from api.xtl_platform.tests.test_isolation import PKG_ROOT
    return {n: (PKG_ROOT / "risk" / n).read_text() for n in DAY5_FILES}


def test_25_no_day3_risk_formulas_duplicated():
    limit_fields = {"max_risk_per_trade_usd", "max_positions", "max_open_risk_usd", "daily_loss_limit_usd",
                    "max_loss_limit_usd", "daily_loss_model", "max_loss_model", "daily_floating_pnl",
                    "max_loss_floating_pnl", "limits", "realized_daily_pnl", "balance", "equity"}
    calc = {"daily_loss_check", "max_loss_check", "open_risk_check", "max_positions_check", "trade_risk_check",
            "build_check", "floating_component", "evaluate_risk", "derive_evaluation_snapshot"}
    for name, source in _sources().items():
        tree = ast.parse(source)
        names = {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)} | \
                {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)} | \
                {a.name for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) for a in n.names}
        assert names.isdisjoint(limit_fields | calc), (name, names & (limit_fields | calc))
    ref = ast.parse(_sources()["lifecycle_reference.py"])
    binops = sorted(ast.unparse(n) for n in ast.walk(ref) if isinstance(n, ast.BinOp))
    assert binops == sorted([
        "account_state.committed_open_positions + 1",                   # ownership move: one position slot
        "account_state.committed_open_risk_usd + reservation.risk_usd",  # ownership move: exact authorized risk
        "state.records + (record,)",                                     # append audit record (tuple)
        "'invalid transition request: ' + '; '.join(errors)",            # message text
    ])
    assert [a.name for n in ast.walk(ref) if isinstance(n, ast.ImportFrom) and n.module == "calculator"
            for a in n.names] == ["RISK_DECIMAL_CONTEXT"]


def test_26_no_production_dependencies_or_authority_introduced():
    from api.xtl_platform.tests.test_isolation import PACKAGE, PKG_ROOT, PRODUCTION_MANIFEST, scan_source
    forbidden_names = {"ExecutionCommand", "ExecutionAdapter", "BrokerPosition", "order_send", "TRADE_ACTIVE",
                       "redis", "Redis", "lua", "evalsha", "setnx"}
    for name, source in _sources().items():
        assert f"risk/{name}" in PRODUCTION_MANIFEST
        assert scan_source(source, PACKAGE + ("risk",)) == [], name
        tree = ast.parse(source)
        names = {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)} | \
                {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)}
        modules = {n.module or "" for n in ast.walk(tree) if isinstance(n, ast.ImportFrom)} | \
                  {a.name for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names}
        assert names.isdisjoint(forbidden_names), (name, names & forbidden_names)
        assert not any(m.split(".")[0] in {"os", "io", "pathlib", "socket", "time", "random"} for m in modules)
        assert not any("strategy" in m.lower() or "execution" in m for m in modules), name
        assert "REFERENCE ONLY - NON-PRODUCTION" in source and "TRADE_ACTIVE" not in \
            source.replace("Do not call the state TRADE_ACTIVE", "")
    assert ReferenceLifecycleRiskAuthority.GRANTS_PRODUCTION_AUTHORITY is False
    assert ReferenceLifecycleRiskAuthority.AUTHORITY_CLASSIFICATION == "REFERENCE_ONLY_NON_PRODUCTION"
    assert LifecycleTransitionResult.IS_ORDER is False and LifecycleTransitionResult.GRANTS_PRODUCTION_AUTHORITY is False
    with pytest.raises(RuntimeError, match="NON-PRODUCTION"):
        ReferenceLifecycleRiskAuthority(CONFIG, FakeClock())
    public = {n for n in dir(ReferenceLifecycleRiskAuthority) if not n.startswith("_")}
    assert public == {"authorize_and_reserve", "register_account", "replace_account_state", "state",
                      "lifecycle_state", "reference_transition", "GRANTS_PRODUCTION_AUTHORITY",
                      "AUTHORITY_CLASSIFICATION"}
    import api.xtl_platform.risk as risk_pkg
    assert risk_pkg.__all__ == ["RiskAuthority"]
    for bad in ("redis_authority.py", "production_authority.py", "shared_risk_service.py"):
        assert not (PKG_ROOT / "risk" / bad).exists()


# ---- frozen Day-4 semantics preserved by the subclass ------------------------------------------------------------

def test_day4_semantics_preserved_400_plus_400_and_replay():
    for _ in range(ROUNDS):
        auth = lifecycle_authority(None, account_state(limit_kw=dict(max_open_risk_usd=D("500"), max_positions=5)))
        a, b = run_concurrently([lambda: auth.authorize_and_reserve(request(risk="400", thesis_id="A")),
                                 lambda: auth.authorize_and_reserve(request(risk="400", thesis_id="B",
                                                                            strategy=StrategyId.STRATEGY_B))])
        assert sorted([a.code.value, b.code.value]) == ["AUTHORIZED_RESERVED", "BLOCK_OPEN_RISK"]
    auth = lifecycle_authority()
    first = auth.authorize_and_reserve(request())
    replay = auth.authorize_and_reserve(request())
    assert replay.idempotent_replay and replay.reservation is first.reservation
    assert auth.authorize_and_reserve(request(risk="1")).code is A.DUPLICATE_TRADE_ID


def test_day4_classes_unchanged_and_subclassed_not_modified():
    assert issubclass(ReferenceLifecycleRiskAuthority, ReferenceAtomicRiskAuthority)
    assert "_state" not in vars(ReferenceAtomicRiskAuthority)
    assert "reference_transition" not in vars(ReferenceAtomicRiskAuthority)


def test_transition_commit_outside_lock_is_refused():
    auth = lifecycle_authority()
    with pytest.raises(RuntimeError, match="outside the atomic section"):
        auth._commit_lifecycle(auth.lifecycle_state())


def test_full_transition_is_under_the_atomic_lock(monkeypatch):
    from api.xtl_platform.tests.test_atomic_reference import RecordingLock
    auth = lifecycle_authority()
    res = reserve(auth)
    events = []
    lock = RecordingLock(events)
    auth._lock = lock
    clock = auth._clock
    auth._clock = lambda: (events.append(("clock", lock.held)), clock())[1]
    original_record = lifecycle_module.ReservationLifecycleRecord
    monkeypatch.setattr(lifecycle_module, "ReservationLifecycleRecord",
                        lambda *a, **k: (events.append(("build", lock.held)), original_record(*a, **k))[1])
    original_commit = auth._commit_lifecycle
    auth._commit_lifecycle = lambda s: (events.append(("commit", lock.held)), original_commit(s))[1]
    assert auth.reference_transition(transition(res, O.COMMITTED_POSITION)).applied
    assert events == ["acquire", ("clock", True), ("build", True), ("commit", True), "release"]


# ---- gaps found by mutation testing (L10, L13) --------------------------------------------------------------

def test_day4_commits_preserve_lifecycle_history():
    """Authorization, account registration and the state feed are Day-4 commits; none may drop history."""
    auth = lifecycle_authority(None, account_state("p1", P1, limit_kw=dict(max_open_risk_usd=D("1000"))))
    once = request(risk="100", thesis_id="ONCE")
    res = auth.authorize_and_reserve(once).reservation
    record = auth.reference_transition(transition(res, O.RELEASED)).record

    assert auth.authorize_and_reserve(request(risk="100", thesis_id="LATER")).authorized     # Day-4 commit
    auth.register_account(account_state("p2", P2))                                          # Day-4 commit
    auth.replace_account_state(account_state("p1", P1, realized_daily_pnl=D("-1"),
                                             limit_kw=dict(max_open_risk_usd=D("1000"))))    # Day-4 commit

    state = auth.lifecycle_state()
    assert state.records == (record,) and state.owner_of(res.child_trade_id) is O.RELEASED
    again = auth.authorize_and_reserve(once)
    assert again.code is A.DUPLICATE_TRADE_ID and again.reservation is None
    assert auth.lifecycle_state().authority.reservation_for(res.child_trade_id) is None


def test_12c_transition_id_reused_for_an_active_child_is_conflicting():
    auth = lifecycle_authority()
    first = reserve(auth, thesis_id="A-1")
    second = reserve(auth, thesis_id="A-2")
    auth.reference_transition(transition(first, O.COMMITTED_POSITION, transition_id="TID-SHARED"))
    r = _assert_unchanged_block(auth, transition(second, O.COMMITTED_POSITION, transition_id="TID-SHARED"),
                                T.BLOCK_CONFLICTING_TRANSITION)
    assert r.previous_owner is O.ACTIVE_RESERVATION
    assert "already used" in r.reason
