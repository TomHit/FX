"""Day-5 Correction-1 - state feed guard (REFERENCE ONLY - NON-PRODUCTION).

replace_account_state must fail closed for any account whose committed exposure is
owned by COMMITTED_POSITION lifecycle records, regardless of the incoming values.
Ownership is never inferred from equality. No reconciliation is implemented.
Lettered tests map 1:1 to the CSA correction (A-J).
All limit values are TEST FIXTURE - NOT VERIFIED PRODUCTION FIRM RULE.
"""
from __future__ import annotations

import dataclasses
import time

import pytest

from api.xtl_platform.domain._validation import ContractError
from api.xtl_platform.domain.account import AccountIdentity
from api.xtl_platform.domain.enums import StrategyId
from api.xtl_platform.risk import lifecycle_reference as lifecycle_module
from api.xtl_platform.risk.atomic_reference import ReferenceAtomicRiskAuthority, derive_evaluation_snapshot
from api.xtl_platform.risk.lifecycle_contracts import CommittedExposureReplacementBlocked, ExposureOwner as O
from api.xtl_platform.tests._builders import D
from api.xtl_platform.tests.test_atomic_reference import CONFIG, P1, P2, FakeClock, account_state, run_concurrently
from api.xtl_platform.tests.test_lifecycle_reference import lifecycle_authority, reserve, transition

LIMITS = dict(max_open_risk_usd=D("1000"), max_positions=10)


def base_state(pid="p1", profile=P1, **kw):
    kw.setdefault("committed_open_risk_usd", D("400"))
    kw.setdefault("committed_open_positions", 2)
    return account_state(pid, profile, limit_kw=LIMITS, **kw)


def committed_authority():
    auth = lifecycle_authority(None, base_state())
    res = reserve(auth, risk="100", thesis_id="C-1")
    assert auth.reference_transition(transition(res, O.COMMITTED_POSITION)).applied
    return auth, res


def _snapshot(auth, pid="p1"):
    ledger = auth.state().ledger(pid)
    s = derive_evaluation_snapshot(ledger)
    return (ledger.account_state.committed_open_risk_usd, ledger.account_state.committed_open_positions,
            s.open_risk_usd, s.open_positions + s.reserved_position_slots)


def _assert_blocked_and_identical(auth, replacement, pid="p1"):
    before = auth.lifecycle_state()
    before_repr = repr(before)
    with pytest.raises(CommittedExposureReplacementBlocked) as exc:
        auth.replace_account_state(replacement)
    after = auth.lifecycle_state()
    assert after is before and after == before and repr(after) == before_repr
    assert exc.value.profile_id == pid
    assert "REFERENCE ONLY" in str(exc.value) and "state unchanged" in str(exc.value)
    return exc.value


# ---- A-C lower / equal / higher committed exposure all fail closed --------------------------------

def test_A_lower_committed_exposure_replacement_fails_closed():
    auth, _ = committed_authority()
    assert _snapshot(auth) == (D("500"), 3, D("500"), 3)
    _assert_blocked_and_identical(auth, base_state(committed_open_risk_usd=D("400"), committed_open_positions=2))
    assert _snapshot(auth) == (D("500"), 3, D("500"), 3)


def test_B_equal_committed_exposure_replacement_still_fails_closed():
    """Equality is NOT proof that the feed contains the lifecycle-owned exposure."""
    auth, _ = committed_authority()
    _assert_blocked_and_identical(auth, base_state(committed_open_risk_usd=D("500"), committed_open_positions=3))
    _assert_blocked_and_identical(auth, auth.state().ledger("p1").account_state)       # the identical object


def test_C_higher_committed_exposure_replacement_fails_closed():
    auth, _ = committed_authority()
    _assert_blocked_and_identical(auth, base_state(committed_open_risk_usd=D("600"), committed_open_positions=4))


@pytest.mark.parametrize("change", [dict(realized_daily_pnl=D("-10")), dict(account_halted=True),
                                    dict(as_of=base_state().as_of.replace(microsecond=1))])
def test_C2_any_ordinary_replacement_blocked_even_if_committed_fields_untouched(change):
    auth, _ = committed_authority()
    current = auth.state().ledger("p1").account_state
    _assert_blocked_and_identical(auth, dataclasses.replace(current, **change))


# ---- D-E accounts without COMMITTED_POSITION records keep Day-4 behaviour -----------------------------

def test_D_released_records_do_not_block_replacement():
    auth = lifecycle_authority(None, base_state())
    res = reserve(auth, risk="100", thesis_id="R-1")
    assert auth.reference_transition(transition(res, O.RELEASED)).applied
    replacement = base_state(committed_open_risk_usd=D("250"), committed_open_positions=1)
    auth.replace_account_state(replacement)
    assert auth.state().ledger("p1").account_state is replacement
    assert auth.lifecycle_state().owner_of(res.child_trade_id) is O.RELEASED
    assert _snapshot(auth) == (D("250"), 1, D("250"), 1)


def test_E_active_reservations_preserve_day4_semantics():
    auth = lifecycle_authority(None, base_state())
    active = reserve(auth, risk="100", thesis_id="A-1")
    replacement = base_state(committed_open_risk_usd=D("300"), committed_open_positions=1)
    auth.replace_account_state(replacement)
    ledger = auth.state().ledger("p1")
    assert ledger.account_state is replacement and ledger.reservations == (active,)
    assert _snapshot(auth) == (D("300"), 1, D("400"), 2)


def _day4_and_day5(*states):
    clock = FakeClock()
    day4 = ReferenceAtomicRiskAuthority(CONFIG, clock, acknowledge_reference_only_non_production=True)
    day5 = lifecycle_authority(clock, *states)
    for s in states:
        day4.register_account(s)
    return day4, day5


def _outcome(fn):
    try:
        fn()
        return ("ok", None)
    except Exception as exc:  # noqa: BLE001
        return (type(exc), str(exc))


@pytest.mark.parametrize("case", ["valid", "unregistered", "identity_change", "wrong_type"])
def test_E2_parity_with_frozen_day4_when_no_committed_records(case):
    day4, day5 = _day4_and_day5(base_state())
    for auth in (day4, day5):
        reserve(auth, risk="100", thesis_id="A-1")
    incoming = {
        "valid": base_state(committed_open_risk_usd=D("123"), committed_open_positions=1),
        "unregistered": base_state("p2", P2),
        "identity_change": base_state(account=AccountIdentity("999", "S", "C", "USD")),
        "wrong_type": "not a state",
    }[case]
    o4 = _outcome(lambda: day4.replace_account_state(incoming))
    o5 = _outcome(lambda: day5.replace_account_state(incoming))
    assert o4 == o5
    assert day4.state() == day5.state()
    if case != "valid":
        assert o5[0] in (ContractError, TypeError)


# ---- F-G multiple records / account isolation -----------------------------------------------------------

def test_F_multiple_committed_records_same_account_block_replacement():
    auth = lifecycle_authority(None, base_state())
    a = reserve(auth, risk="100", thesis_id="A-1", strategy=StrategyId.STRATEGY_A)
    b = reserve(auth, risk="150", thesis_id="B-1", strategy=StrategyId.STRATEGY_B)
    auth.reference_transition(transition(a, O.COMMITTED_POSITION))
    auth.reference_transition(transition(b, O.COMMITTED_POSITION))
    err = _assert_blocked_and_identical(auth, base_state(committed_open_risk_usd=D("650"), committed_open_positions=4))
    assert set(err.transition_ids) == {f"COMMITTED_POSITION:{a.child_trade_id}", f"COMMITTED_POSITION:{b.child_trade_id}"}
    assert _snapshot(auth) == (D("650"), 4, D("650"), 4)


def test_F2_mixed_released_and_committed_still_blocks():
    auth = lifecycle_authority(None, base_state())
    rel = reserve(auth, risk="100", thesis_id="R-1")
    com = reserve(auth, risk="100", thesis_id="C-1")
    auth.reference_transition(transition(rel, O.RELEASED))
    auth.reference_transition(transition(com, O.COMMITTED_POSITION))
    err = _assert_blocked_and_identical(auth, base_state())
    assert err.transition_ids == (f"COMMITTED_POSITION:{com.child_trade_id}",)


def test_G_committed_record_on_account_a_does_not_block_account_b():
    auth = lifecycle_authority(None, base_state("p1", P1), base_state("p2", P2))
    res = reserve(auth, risk="100", thesis_id="C-1", profile=P1)
    auth.reference_transition(transition(res, O.COMMITTED_POSITION))
    replacement_b = base_state("p2", P2, committed_open_risk_usd=D("50"), committed_open_positions=0)
    auth.replace_account_state(replacement_b)
    assert auth.state().ledger("p2").account_state is replacement_b
    _assert_blocked_and_identical(auth, base_state("p1", P1))
    assert _snapshot(auth, "p1") == (D("500"), 3, D("500"), 3)


# ---- H failed replacement preserves everything ---------------------------------------------------------

def test_H_failed_replacement_preserves_records_exposure_reservations_identity():
    auth = lifecycle_authority(None, base_state())
    committed = reserve(auth, risk="100", thesis_id="C-1", strategy=StrategyId.STRATEGY_A)
    active = reserve(auth, risk="75", thesis_id="A-2", strategy=StrategyId.STRATEGY_B)
    record = auth.reference_transition(transition(committed, O.COMMITTED_POSITION)).record
    before = auth.lifecycle_state()
    ledger_before = before.authority.ledger("p1")

    _assert_blocked_and_identical(auth, base_state(committed_open_risk_usd=D("0"), committed_open_positions=0))

    after = auth.lifecycle_state()
    ledger = after.authority.ledger("p1")
    assert after.records == (record,)                                                    # lifecycle records
    assert ledger.account_state.committed_open_risk_usd == D("500")                      # committed risk
    assert ledger.account_state.committed_open_positions == 3                            # committed positions
    assert ledger.reservations == (active,)                                              # active reservations
    assert ledger.account_state.account == ledger_before.account_state.account == P1.identity   # identity
    assert after.owner_of(committed.child_trade_id) is O.COMMITTED_POSITION
    assert after.owner_of(active.child_trade_id) is O.ACTIVE_RESERVATION


# ---- I concurrency: transfer vs replacement --------------------------------------------------------------

@pytest.fixture
def transfer_holding_lock(monkeypatch):
    """Signal when a transfer is INSIDE its locked section, then keep holding the lock briefly."""
    import threading
    entered = threading.Event()
    original = lifecycle_module.ReservationLifecycleRecord

    def slow(*a, **k):
        entered.set()
        time.sleep(0.01)
        return original(*a, **k)

    monkeypatch.setattr(lifecycle_module, "ReservationLifecycleRecord", slow)
    return entered


@pytest.mark.parametrize("order", ["transfer_holds_lock_first", "replacement_first"])
def test_I_concurrent_transfer_vs_replacement_only_valid_outcomes(transfer_holding_lock, order):
    """Real threads and the real lock, with each interleaving forced:
      transfer_holds_lock_first: replacement arrives while the transfer holds the lock
                                 -> waits -> sees the COMMITTED record -> BLOCKED
      replacement_first:         replacement commits before the transfer starts
                                 -> transfer adds exposure on the NEW base
    Never: a COMMITTED record whose transferred exposure has been overwritten."""
    import threading
    for i in range(15):
        transfer_holding_lock.clear()
        auth = lifecycle_authority(None, base_state())
        res = reserve(auth, risk="100", thesis_id=f"I-{i}")
        replacement = base_state(committed_open_risk_usd=D("250"), committed_open_positions=1)
        out = {}

        def do_transfer():
            out["transfer"] = auth.reference_transition(transition(res, O.COMMITTED_POSITION))

        def do_replace():
            try:
                auth.replace_account_state(replacement)
                out["replace"] = "replaced"
            except CommittedExposureReplacementBlocked:
                out["replace"] = "blocked"

        t = threading.Thread(target=do_transfer)
        r = threading.Thread(target=do_replace)
        if order == "transfer_holds_lock_first":
            t.start()
            assert transfer_holding_lock.wait(2)          # transfer is inside the atomic section now
            r.start()                                     # replacement must wait for the lock
        else:
            r.start()
            r.join()                                      # replacement fully committed first
            t.start()
        t.join()
        r.join()

        assert out["transfer"].applied
        assert auth.lifecycle_state().owner_of(res.child_trade_id) is O.COMMITTED_POSITION
        committed = _snapshot(auth)
        if order == "transfer_holds_lock_first":
            assert out["replace"] == "blocked"
            assert committed == (D("500"), 3, D("500"), 3)   # 400 + 100, 2 + 1 preserved
        else:
            assert out["replace"] == "replaced"
            assert committed == (D("350"), 2, D("350"), 2)   # 250 + 100, 1 + 1 on the new base
        assert committed[:2] not in {(D("250"), 1), (D("400"), 2)}   # never overwritten, never lost


def test_I2_guard_and_replacement_are_one_locked_section(monkeypatch):
    from api.xtl_platform.tests.test_atomic_reference import RecordingLock
    auth = lifecycle_authority(None, base_state())
    state_type = type(auth.lifecycle_state())            # read BEFORE installing the recording lock
    events = []
    lock = RecordingLock(events)
    auth._lock = lock
    original_commit = auth._commit
    auth._commit = lambda s: (events.append(("commit", lock.held)), original_commit(s))[1]
    original_committed = state_type.committed_records_for
    monkeypatch.setattr(state_type, "committed_records_for",
                        lambda self, pid: (events.append(("guard", lock.held)), original_committed(self, pid))[1])
    auth.replace_account_state(base_state(committed_open_risk_usd=D("1")))
    assert events == ["acquire", ("guard", True), ("commit", True), "release"]


# ---- J-adjacent: the guard does not weaken other invariants -----------------------------------------------

def test_committed_account_still_authorizes_and_transitions_normally():
    auth, _ = committed_authority()
    new = reserve(auth, risk="100", thesis_id="N-1")
    assert auth.reference_transition(transition(new, O.RELEASED)).applied
    assert _snapshot(auth) == (D("500"), 3, D("500"), 3)


def test_blocked_error_is_reference_only_contract_error():
    assert issubclass(CommittedExposureReplacementBlocked, ContractError)
    auth, _ = committed_authority()
    with pytest.raises(ContractError):
        auth.replace_account_state(base_state())
