"""Strict profile/account/device identity; fails closed (PRD sections 9, 18)."""
from dataclasses import FrozenInstanceError
from datetime import timedelta

import pytest

from api.xtl_platform.tests._builders import NOW, USER, D, make_device, make_profile, make_thesis
from api.xtl_platform.accounts.router import AccountRouter
from api.xtl_platform.domain._validation import ContractError
from api.xtl_platform.domain.account import AccountIdentity, ExecutionSet
from api.xtl_platform.domain.enums import FanOutCode as F, Venue


def _route(profile, device=None, **router_kw):
    devices = {} if device is False else {"dev-1": device or make_device()}
    router = AccountRouter(devices, max_heartbeat_age=router_kw.get("max_age", timedelta(seconds=30)))
    (result,) = router.fan_out(make_thesis(), ExecutionSet(USER, (profile,)), NOW)
    return result


def test_valid_binding_creates_child():
    r = _route(make_profile("p1"))
    assert r.created and r.intent.account.login == "login-p1"


@pytest.mark.parametrize("device,code", [
    (False, F.BLOCK_DEVICE_NOT_FOUND),
    (make_device(owner_user_id="someone-else"), F.BLOCK_DEVICE_OWNER_MISMATCH),
    (make_device(healthy=False), F.BLOCK_DEVICE_UNHEALTHY),
    (make_device(last_heartbeat=NOW - timedelta(minutes=5)), F.BLOCK_DEVICE_STALE),
    (make_device(venues=frozenset({Venue.FUTURES})), F.BLOCK_DEVICE_VENUE_UNSUPPORTED),
])
def test_bad_device_fails_closed(device, code):
    r = _route(make_profile("p1"), device)
    assert r.code is code and r.intent is None


def test_disabled_profile_is_skipped():
    assert _route(make_profile("p1", enabled=False)).code is F.SKIP_PROFILE_DISABLED


def test_unmapped_symbol_blocks():
    assert _route(make_profile("p1", symbol_map={})).code is F.BLOCK_SYMBOL_UNMAPPED


def test_profile_owned_by_other_user_rejected_at_construction():
    with pytest.raises(ContractError):
        ExecutionSet(USER, (make_profile("p1", owner_user_id="intruder"),))


def test_same_login_bound_to_two_profiles_rejected():
    ident = AccountIdentity("123", "FTMO-Demo", "FTMO", "USD")
    with pytest.raises(ContractError):
        ExecutionSet(USER, (make_profile("p1", identity=ident), make_profile("p2", identity=ident)))


def test_duplicate_profile_id_rejected():
    with pytest.raises(ContractError):
        ExecutionSet(USER, (make_profile("p1"), make_profile("p1", identity=AccountIdentity("9", "s", "c", "USD"))))


def test_child_identity_is_frozen():
    child = _route(make_profile("p1")).intent
    with pytest.raises(FrozenInstanceError):
        child.profile_id = "p2"
    with pytest.raises(FrozenInstanceError):
        child.account.login = "999"
