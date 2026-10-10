"""One thesis -> independent child decisions per account (PRD sections 5, 7, 21)."""
import threading
from datetime import timedelta
from decimal import Decimal

from api.xtl_platform.tests._builders import NOW, USER, D, make_device, make_limits, make_policy, make_profile, make_snapshot, make_thesis
from api.xtl_platform.accounts.router import AccountRouter
from api.xtl_platform.domain.account import ExecutionSet
from api.xtl_platform.domain.enums import FanOutCode as F, Permission, PolicyDecisionCode as P, Side, StrategyId, Venue
from api.xtl_platform.domain.policy import OpenExposure, PolicyContext
from api.xtl_platform.domain.risk import RiskRequest
from api.xtl_platform.domain.thesis import ChildExecutionIntent
from api.xtl_platform.policy.base import BasePolicyEvaluator, PolicyRegistry


def test_one_thesis_fans_out_to_every_enabled_account(router, execution_set):
    results = router.fan_out(make_thesis(), execution_set, NOW)
    children = [r.intent for r in results if r.created]
    assert len(children) == 3
    assert len({c.child_trade_id for c in children}) == 3
    assert {c.thesis_id for c in children} == {"A-20260916-XAU-BUY"}


def test_fan_out_order_is_deterministic(router, execution_set):
    ids = [r.profile_id for r in router.fan_out(make_thesis(), execution_set, NOW)]
    assert ids == sorted(ids)


def test_one_blocked_account_does_not_stop_siblings():
    devices = {"dev-1": make_device(), "dev-2": make_device("dev-2", healthy=False)}
    router = AccountRouter(devices, timedelta(seconds=30))
    es = ExecutionSet(USER, (make_profile("a"), make_profile("b", device_id="dev-2"), make_profile("c")))
    codes = {r.profile_id: r.code for r in router.fan_out(make_thesis(), es, NOW)}
    assert codes == {"a": F.CHILD_CREATED, "b": F.BLOCK_DEVICE_UNHEALTHY, "c": F.CHILD_CREATED}


def test_cfd_thesis_is_never_expressed_on_futures_account(router):
    fut = make_profile("fut-1", venue=Venue.FUTURES, symbol_map={"XAUUSD": "MGCZ6"})
    (r,) = router.fan_out(make_thesis(), ExecutionSet(USER, (fut,)), NOW)
    assert r.code is F.BLOCK_VENUE_MISMATCH


def test_expired_thesis_creates_no_children(router, execution_set):
    results = router.fan_out(make_thesis(), execution_set, NOW + timedelta(hours=1))
    assert all(r.code is F.BLOCK_THESIS_EXPIRED for r in results)


def test_accounts_of_different_size_get_independent_decisions(router, execution_set, authority):
    """Same thesis, 1% risk each: 100k fits, 10k with a used-up daily limit blocks."""
    results = {r.profile_id: r.intent for r in router.fan_out(make_thesis(), execution_set, NOW)}
    decisions = {}
    for pid, child in results.items():
        size = D("10000") if pid == "ftmo-10k-01" else D("100000")
        risk = size * Decimal("0.01")
        snap = make_snapshot(pid, balance=size, equity=size,
                             daily_loss_used_usd=D("450") if pid == "ftmo-10k-01" else D("0"))
        limits = make_limits(pid, daily_loss_limit_usd=size * Decimal("0.05"),
                             max_loss_limit_usd=size * Decimal("0.10"), max_open_risk_usd=size * Decimal("0.03"))
        decisions[pid] = authority.authorize_and_reserve(RiskRequest(child, risk, D("1"), "v1"), snap, limits, NOW)

    assert decisions["ftmo-100k-01"].authorized and decisions["ftmo-100k-01"].reservation.risk_usd == D("1000")
    assert decisions["ftmo-100k-02"].authorized
    assert decisions["ftmo-10k-01"].code.value == "BLOCK_DAILY_LIMIT"


def test_atomic_reservation_prevents_concurrent_over_reservation(authority):
    """50 threads race for capacity that fits exactly 10 reservations."""
    pid = "ftmo-100k-01"
    profile = make_profile(pid)
    snap = make_snapshot(pid)
    limits = make_limits(pid, max_open_risk_usd=D("1000"), max_positions=100)
    barrier = threading.Barrier(50)
    results = []
    lock = threading.Lock()

    def worker(i):
        child = ChildExecutionIntent.from_thesis(make_thesis(thesis_id=f"A-{i}"), profile, "XAUUSD")
        barrier.wait()
        r = authority.authorize_and_reserve(RiskRequest(child, D("100"), D("1"), "v1"), snap, limits, NOW)
        with lock:
            results.append(r)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(50)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert sum(r.authorized for r in results) == 10
    total = sum(r.risk_usd for r in authority.active_reservations(pid))
    assert total == D("1000")


def test_stale_snapshot_blocks(authority):
    pid = "ftmo-100k-01"
    child = ChildExecutionIntent.from_thesis(make_thesis(), make_profile(pid), "XAUUSD")
    r = authority.authorize_and_reserve(RiskRequest(child, D("100"), D("1"), "v1"),
                                        make_snapshot(pid, as_of=NOW - timedelta(minutes=1)),
                                        make_limits(pid), NOW)
    assert r.code.value == "STALE_ACCOUNT_STATE"


def test_unsent_reservation_expires_and_frees_capacity(authority):
    pid = "ftmo-100k-01"
    child = ChildExecutionIntent.from_thesis(make_thesis(), make_profile(pid), "XAUUSD")
    res = authority.authorize_and_reserve(RiskRequest(child, D("100"), D("1"), "v1"),
                                          make_snapshot(pid), make_limits(pid), NOW).reservation
    assert authority.expire_unsent(res.expires_at) == [res.authorization_id]
    assert authority.active_reservations(pid) == []


# ---- policy on fanned-out children ----------------------------------------

def _evaluate(child, profile, policy, **ctx):
    registry = PolicyRegistry([policy])
    context = PolicyContext(now=NOW, **ctx)
    return BasePolicyEvaluator().evaluate(child, profile, registry.resolve(profile, context), context)


def _child_for(profile, **thesis_kw):
    return ChildExecutionIntent.from_thesis(make_thesis(**thesis_kw), profile, "XAUUSD")


def test_eligible_child_records_policy_version():
    p = make_profile("p1")
    d = _evaluate(_child_for(p), p, make_policy())
    assert d.eligible and d.policy_version == "ftmo-cfd-2step-challenge-2026.09"


def test_no_policy_for_phase_blocks():
    from api.xtl_platform.domain.enums import Phase
    p = make_profile("p1", phase=Phase.FUNDED)
    assert _evaluate(_child_for(p), p, make_policy()).code is P.BLOCK_NO_POLICY


def test_unverified_automation_blocks():
    p = make_profile("p1")
    assert _evaluate(_child_for(p), p, make_policy(automation_verified=False)).code is P.BLOCK_AUTOMATION_UNVERIFIED


def test_opposite_exposure_at_same_firm_blocks():
    """Strategy A long XAU on one FTMO account, Strategy B short on another."""
    p2 = make_profile("p2")
    child_b = _child_for(p2, thesis_id="B-1", strategy_id=StrategyId.STRATEGY_B, side=Side.SELL,
                         stop=D("2660"), target=D("2620"))
    exposure = OpenExposure("p1", "FTMO", "XAUUSD", Side.BUY)
    d = _evaluate(child_b, p2, make_policy(), open_exposures=(exposure,))
    assert d.code is P.BLOCK_OPPOSITE_EXPOSURE


def test_opposite_exposure_at_other_firm_is_not_this_policys_concern():
    p2 = make_profile("p2")
    child = _child_for(p2, side=Side.SELL, stop=D("2660"), target=D("2620"))
    exposure = OpenExposure("x1", "FUNDEDNEXT", "XAUUSD", Side.BUY)
    assert _evaluate(child, p2, make_policy(), open_exposures=(exposure,)).eligible


def test_own_account_copy_prohibited_blocks_second_child():
    p2 = make_profile("p2")
    d = _evaluate(_child_for(p2), p2, make_policy(own_account_copy=Permission.PROHIBITED),
                  approved_sibling_firm_ids=("FTMO",))
    assert d.code is P.BLOCK_OWN_ACCOUNT_COPY
