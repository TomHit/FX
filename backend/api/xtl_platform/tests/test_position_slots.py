"""Day-4 CSA corrections and rulings.

CHANGE (ruling 2): a reservation consumes risk capacity and a position SLOT, but
is never an open position.

    open_risk_usd            = committed open risk + active reservation risk
    open_positions           = committed/broker positions ONLY
    reserved_position_slots  = active reservations
    projected position slots = open_positions + reserved_position_slots + 1

KEEP (ruling 1): an exact replay returns the committed authorization even if the
account is later halted or stale.

Also the regression suite for the ADDITIVE Day-3 change (reserved_position_slots).
All limit values are TEST FIXTURE - NOT VERIFIED PRODUCTION FIRM RULE.
"""
from __future__ import annotations

import dataclasses
from datetime import timedelta
from decimal import Decimal

import pytest

from api.xtl_platform.domain.enums import StrategyId
from api.xtl_platform.risk import atomic_reference as atomic_module
from api.xtl_platform.risk.atomic_contracts import AtomicAuthorizationCode as A
from api.xtl_platform.risk.contracts import CapacityCheckName as N, RiskEvaluationCode as C, RiskEvaluationSnapshot
from api.xtl_platform.tests._builders import NOW, D
from api.xtl_platform.tests.test_atomic_reference import FakeClock, account_state, new_authority, request
from api.xtl_platform.tests.test_risk_evaluator import ev, lim, req, snap


def _positions_check(result):
    return next(c for c in result.checks if c.name is N.MAX_POSITIONS)


# ---- Day-3 additive change: reserved_position_slots --------------------------------

def test_day3_default_reserved_slots_is_zero_and_equivalent_to_explicit_zero():
    assert snap().reserved_position_slots == 0
    for kw in [{}, dict(open_positions=2), dict(open_positions=3), dict(realized_daily_pnl=D("-4600"))]:
        assert ev(snapshot=snap(**kw)) == ev(snapshot=snap(reserved_position_slots=0, **kw))


def test_day3_reserved_slots_count_toward_max_positions():
    fits = ev(snapshot=snap(open_positions=1, reserved_position_slots=1))        # 1 + 1 + 1 == 3
    assert fits.passed
    check = _positions_check(fits)
    assert (check.used, check.projected, check.remaining_after) == (D("2"), D("3"), D("0"))
    assert dict(check.components) == {"open_positions": D("1"), "reserved_position_slots": D("1")}

    blocked = ev(snapshot=snap(open_positions=2, reserved_position_slots=1))     # 2 + 1 + 1 > 3
    assert blocked.code is C.BLOCK_MAX_POSITIONS and blocked.used_capacity == D("3")
    assert ev(snapshot=snap(open_positions=0, reserved_position_slots=3)).code is C.BLOCK_MAX_POSITIONS


@pytest.mark.parametrize("value", [-1, True, "1", 1.0, None])
def test_day3_invalid_reserved_slots_fail_closed(value):
    r = ev(snapshot=snap(reserved_position_slots=value))
    assert r.code is C.BLOCK_INVALID_RISK_INPUT
    assert any("reserved_position_slots" in e for e in r.input_errors)


def test_day3_reserved_slots_affect_only_the_positions_check():
    base, slotted = ev(), ev(snapshot=snap(reserved_position_slots=2))
    for name in (N.TRADE_RISK, N.DAILY_LOSS, N.MAX_LOSS, N.OPEN_RISK):
        assert next(c for c in base.checks if c.name is name) == next(c for c in slotted.checks if c.name is name)


def test_day3_snapshot_field_order_is_additive():
    names = [f.name for f in dataclasses.fields(RiskEvaluationSnapshot)]
    assert names[-1] == "reserved_position_slots"
    assert names[:-1] == ["profile_id", "account", "as_of", "balance", "equity", "realized_daily_pnl",
                          "open_risk_usd", "open_positions", "account_halted", "daily_halted",
                          "max_loss_reference_balance"]


# ---- Day-4 correction: reservation != position ---------------------------------------

def test_reservation_is_a_slot_not_an_open_position():
    auth = new_authority()
    auth.authorize_and_reserve(request(risk="100", thesis_id="A-1"))
    auth.authorize_and_reserve(request(risk="100", thesis_id="A-2", strategy=StrategyId.STRATEGY_B))
    ledger = auth.state().ledger("p1")
    snapshot = atomic_module.derive_evaluation_snapshot(ledger)
    assert ledger.account_state.committed_open_positions == 0
    assert ledger.active_position_reservations == 2
    assert snapshot.open_positions == 0                          # never claims unfilled orders are positions
    assert snapshot.reserved_position_slots == 2
    assert snapshot.open_risk_usd == D("200")                    # risk consumption preserved


def test_csa_example_broker_2_plus_reservation_1_against_max_3():
    """Firm max 3; broker positions 2; one reservation awaiting execution -> a new trade blocks."""
    auth = new_authority(None, account_state(committed_open_positions=2, limit_kw=dict(max_positions=3)))
    first = auth.authorize_and_reserve(request(risk="100", thesis_id="A-1"))     # 2 + 0 + 1 == 3 fits
    assert first.authorized
    second = auth.authorize_and_reserve(request(risk="100", thesis_id="A-2"))
    assert second.code is A.BLOCK_MAX_POSITIONS                                  # 2 + 1 + 1 > 3 (conservative)
    check = _positions_check(second.risk_evaluation)
    assert dict(check.components) == {"open_positions": D("2"), "reserved_position_slots": D("1")}
    assert (check.used, check.projected, check.limit) == (D("3"), D("4"), D("3"))


def test_limit_not_loosened_by_the_correction():
    for committed, reservations, expected in [(0, 3, A.BLOCK_MAX_POSITIONS), (1, 2, A.BLOCK_MAX_POSITIONS),
                                              (2, 0, A.AUTHORIZED_RESERVED), (0, 2, A.AUTHORIZED_RESERVED)]:
        auth = new_authority(None, account_state(limit_kw=dict(max_positions=3, max_open_risk_usd=D("5000"))))
        for i in range(reservations):
            assert auth.authorize_and_reserve(request(risk="100", thesis_id=f"R-{i}")).authorized
        auth.replace_account_state(account_state(committed_open_positions=committed,
                                                 limit_kw=dict(max_positions=3, max_open_risk_usd=D("5000"))))
        r = auth.authorize_and_reserve(request(risk="100", thesis_id="NEW"))
        assert r.code is expected, (committed, reservations, r.code)


def test_state_feed_updates_committed_positions_without_touching_slots():
    auth = new_authority()
    auth.authorize_and_reserve(request(risk="100"))
    auth.replace_account_state(account_state(committed_open_positions=4))
    ledger = auth.state().ledger("p1")
    assert (ledger.account_state.committed_open_positions, ledger.active_position_reservations) == (4, 1)
    snapshot = atomic_module.derive_evaluation_snapshot(ledger)
    assert (snapshot.open_positions, snapshot.reserved_position_slots) == (4, 1)


def test_invalid_committed_positions_fail_closed():
    auth = new_authority()
    auth.replace_account_state(account_state(committed_open_positions="2"))
    before = auth.state()
    r = auth.authorize_and_reserve(request())
    assert r.code is A.BLOCK_INVALID_RISK_INPUT and auth.state() is before


# ---- CSA ruling 1: exact replay is history, not new authority --------------------------

def test_exact_replay_after_halt_and_staleness_returns_committed_authorization():
    clock = FakeClock()
    auth = new_authority(clock)
    first = auth.authorize_and_reserve(request(thesis_id="B-123", strategy=StrategyId.STRATEGY_B))
    assert first.authorized and not first.idempotent_replay

    auth.replace_account_state(account_state(account_halted=True, daily_halted=True))
    clock.now = NOW + timedelta(hours=2)                       # snapshot now far beyond max age

    replay = auth.authorize_and_reserve(request(thesis_id="B-123", strategy=StrategyId.STRATEGY_B))
    assert replay.code is A.AUTHORIZED_RESERVED and replay.idempotent_replay is True
    assert replay.reservation is first.reservation
    assert replay.risk_evaluation is first.risk_evaluation and replay.authorized_at == first.authorized_at == NOW
    assert replay.decided_at == NOW + timedelta(hours=2)       # when the replay was answered

    new_trade = auth.authorize_and_reserve(request(thesis_id="B-124", strategy=StrategyId.STRATEGY_B))
    assert new_trade.code is A.STALE_ACCOUNT_STATE and new_trade.reservation is None
    assert len(auth.state().reservations) == 1
