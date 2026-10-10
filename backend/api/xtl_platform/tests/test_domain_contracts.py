from dataclasses import FrozenInstanceError
from datetime import datetime, timedelta
from decimal import Decimal

import pytest

from api.xtl_platform.tests._builders import NOW, D, make_limits, make_policy, make_profile, make_snapshot, make_thesis
from api.xtl_platform.domain._validation import ContractError
from api.xtl_platform.domain.enums import AckStatus, LifecycleState as S, LossModel, OrderType, Side, Venue
from api.xtl_platform.domain.execution import (
    ExecutionAck, ExecutionCommand, InvalidTransition, assert_transition, can_transition,
)
from api.xtl_platform.domain.risk import RiskRequest
from api.xtl_platform.domain.thesis import ChildExecutionIntent
from api.xtl_platform.execution.adapter import AdapterContractError, ExecutionAdapter


# ---- thesis -------------------------------------------------------------

def test_thesis_is_immutable():
    t = make_thesis()
    with pytest.raises(FrozenInstanceError):
        t.entry = D("1")


@pytest.mark.parametrize("kw", [
    dict(side=Side.BUY, stop=D("2660")),
    dict(side=Side.BUY, target=D("2600")),
    dict(side=Side.SELL, stop=D("2640"), target=D("2600")),
    dict(entry=D("-1")),
    dict(entry=2650.0),                                   # float, not Decimal
    dict(created_at=datetime(2026, 9, 16, 12, 0)),        # naive datetime
    dict(expires_at=NOW - timedelta(hours=1)),
    dict(thesis_id="  "),
])
def test_thesis_rejects_invalid_data(kw):
    with pytest.raises(ContractError):
        make_thesis(**kw)


def test_child_trade_id_format():
    child = ChildExecutionIntent.from_thesis(make_thesis(), make_profile("ftmo-100k-01"), "XAUUSD")
    assert child.child_trade_id == "A-20260916-XAU-BUY:ftmo-100k-01"


def test_profile_symbol_map_is_read_only():
    p = make_profile("p1")
    with pytest.raises(TypeError):
        p.symbol_map["EURUSD"] = "EURUSD"


# ---- policy -------------------------------------------------------------

def test_policy_rejects_unknown_timezone():
    with pytest.raises(ContractError):
        make_policy(daily_reset_tz="Mars/Olympus")


def test_policy_requires_limit_when_model_set():
    with pytest.raises(ContractError):
        make_policy(daily_loss_model=LossModel.TRAILING_INTRADAY, daily_loss_limit_pct=None)


# ---- lifecycle ----------------------------------------------------------

def test_happy_path_lifecycle_is_allowed():
    path = [S.THESIS_READY, S.POLICY_ELIGIBLE, S.RISK_EVALUATED, S.AUTHORIZED_RESERVED,
            S.ORDER_PENDING, S.TRADE_ACTIVE, S.PROTECT, S.EXIT, S.CLOSED, S.RELEASED, S.RECONCILED]
    for a, b in zip(path, path[1:]):
        assert_transition(a, b)


@pytest.mark.parametrize("a,b", [
    (S.THESIS_READY, S.ORDER_PENDING),     # skipping policy + risk
    (S.POLICY_ELIGIBLE, S.ORDER_PENDING),  # skipping risk
    (S.TRADE_ACTIVE, S.RELEASED),          # release without broker close
    (S.BLOCKED, S.POLICY_ELIGIBLE),
    (S.RECONCILED, S.TRADE_ACTIVE),
])
def test_illegal_transitions_rejected(a, b):
    assert not can_transition(a, b)
    with pytest.raises(InvalidTransition):
        assert_transition(a, b)


# ---- execution adapter contract -----------------------------------------

def _reservation(authority, profile_id="ftmo-100k-01"):
    child = ChildExecutionIntent.from_thesis(make_thesis(), make_profile(profile_id), "XAUUSD")
    res = authority.authorize_and_reserve(
        RiskRequest(child, D("500"), D("0.5"), "v1"), make_snapshot(profile_id), make_limits(profile_id), NOW)
    assert res.authorized
    return res.reservation


class FakeMT5(ExecutionAdapter):
    venue = Venue.MT5_CFD

    def __init__(self, authority, behaviour="accept"):
        super().__init__(authority)
        self.behaviour, self.sent = behaviour, []

    def _submit(self, command):
        self.sent.append(command.command_id)
        if self.behaviour == "raise":
            raise TimeoutError("socket timeout")
        return ExecutionAck(command.command_id, command.trade_id, AckStatus.ACCEPTED, "ord-1", "ok", NOW)

    def cancel(self, command, now):
        raise NotImplementedError

    def fetch_positions(self, profile_id):
        return []


class FakeFutures(FakeMT5):
    venue = Venue.FUTURES


def _command(reservation, expires_at=None):
    return ExecutionCommand("cmd-1", reservation, OrderType.MARKET, NOW,
                            expires_at or reservation.expires_at)


def test_command_cannot_outlive_reservation(authority):
    res = _reservation(authority)
    with pytest.raises(ContractError):
        _command(res, expires_at=res.expires_at + timedelta(seconds=1))


def test_adapter_places_and_pins_reservation(authority):
    res = _reservation(authority)
    adapter = FakeMT5(authority)
    ack = adapter.place(_command(res), NOW)
    assert ack.confirmed_placement
    assert authority.status(res.authorization_id).value == "SENT"
    # a SENT reservation never expires by timeout; only broker truth releases it
    assert authority.expire_unsent(NOW + timedelta(hours=1)) == []


def test_adapter_refuses_expired_command(authority):
    res = _reservation(authority)
    adapter = FakeMT5(authority)
    ack = adapter.place(_command(res), res.expires_at)
    assert ack.status is AckStatus.REJECTED and adapter.sent == []


def test_adapter_refuses_released_reservation(authority):
    res = _reservation(authority)
    authority.release(res.authorization_id, "operator cancel", NOW)
    adapter = FakeMT5(authority)
    assert adapter.place(_command(res), NOW).status is AckStatus.REJECTED
    assert adapter.sent == []


def test_transport_error_is_no_ack_not_success(authority):
    res = _reservation(authority)
    ack = FakeMT5(authority, behaviour="raise").place(_command(res), NOW)
    assert ack.status is AckStatus.NO_ACK
    assert not ack.confirmed_placement
    assert authority.status(res.authorization_id).value == "SENT"  # still held


def test_adapter_rejects_wrong_venue(authority):
    res = _reservation(authority)
    with pytest.raises(AdapterContractError):
        FakeFutures(authority).place(_command(res), NOW)
