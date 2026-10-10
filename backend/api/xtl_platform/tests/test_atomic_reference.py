"""Day 4 - ReferenceAtomicRiskAuthority (REFERENCE ONLY - NON-PRODUCTION).

AUTHORIZED_RESERVED IS NOT AN ORDER. PRODUCTION AUTHORITY = NONE.
Numbered tests map 1:1 to the Day-4 brief, section 28.
All limit values are TEST FIXTURE - NOT VERIFIED PRODUCTION FIRM RULE.
"""
from __future__ import annotations

import ast
import dataclasses
import hashlib
import threading
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from api.xtl_platform.domain.account import AccountIdentity
from api.xtl_platform.domain.enums import LossModel, PolicyDecisionCode, Side, StrategyId
from api.xtl_platform.domain.policy import PolicyDecision
from api.xtl_platform.domain.thesis import ChildExecutionIntent
from api.xtl_platform.risk import atomic_reference as atomic_module
from api.xtl_platform.risk.atomic_contracts import (
    RISK_BLOCK_TO_AUTHORIZATION_CODE,
    AtomicAuthorizationCode as A,
    AtomicAuthorizationRequest,
    AtomicAuthorizationResult,
    ReferenceAccountState,
    ReferenceAuthorityState,
    ReferenceReservationStatus,
)
from api.xtl_platform.risk.atomic_reference import ReferenceAtomicRiskAuthority
from api.xtl_platform.risk.contracts import (
    FloatingPnlTreatment as F,
    RiskEvaluationCode as C,
    RiskEvaluationConfig,
    RiskEvaluationLimits,
    RiskEvaluationRequest,
)
from api.xtl_platform.risk.evaluator import evaluate_risk
from api.xtl_platform.tests._builders import NOW, D, make_profile, make_thesis

UTC = timezone.utc
CONFIG = RiskEvaluationConfig(max_snapshot_age=timedelta(seconds=30))
VERSION = "TEST-2026.09"
P1, P2 = make_profile("p1"), make_profile("p2")


# ---- builders (TEST FIXTURE - NOT VERIFIED PRODUCTION FIRM RULE) ---------------

class FakeClock:
    def __init__(self, now=NOW):
        self.now = now

    def __call__(self):
        return self.now


def limits(pid="p1", **kw) -> RiskEvaluationLimits:
    base = dict(profile_id=pid, source_policy_version=VERSION, max_risk_per_trade_usd=D("1000"), max_positions=5,
                max_open_risk_usd=D("500"), daily_loss_model=LossModel.FIXED, daily_loss_limit_usd=D("5000"),
                daily_floating_pnl=F.LOSSES_ONLY, max_loss_model=LossModel.STATIC, max_loss_limit_usd=D("10000"),
                max_loss_floating_pnl=F.LOSSES_ONLY)
    base.update(kw)
    return RiskEvaluationLimits(**base)


def account_state(pid="p1", profile=None, limit_kw=None, **kw) -> ReferenceAccountState:
    profile = profile or make_profile(pid)
    base = dict(profile_id=pid, account=profile.identity, as_of=NOW - timedelta(seconds=5), balance=D("100000"),
                equity=D("100000"), realized_daily_pnl=D("0"), committed_open_risk_usd=D("0"), committed_open_positions=0,
                account_halted=False, daily_halted=False, max_loss_reference_balance=D("100000"),
                limits=limits(pid, **(limit_kw or {})))
    base.update(kw)
    return ReferenceAccountState(**base)


def request(risk="400", thesis_id="A-1", strategy=StrategyId.STRATEGY_A, profile=P1, side=Side.BUY,
            version=VERSION, decision="eligible", requested_at=NOW, **thesis_kw) -> AtomicAuthorizationRequest:
    if side is Side.SELL:
        thesis_kw.setdefault("stop", D("2660")); thesis_kw.setdefault("target", D("2620"))
    thesis = make_thesis(thesis_id=thesis_id, strategy_id=strategy, side=side, **thesis_kw)
    intent = ChildExecutionIntent.from_thesis(thesis, profile, "XAUUSD")
    if decision == "eligible":
        decision = PolicyDecision(intent.child_trade_id, PolicyDecisionCode.ELIGIBLE, "eligible", version,
                                  NOW - timedelta(seconds=1))
    return AtomicAuthorizationRequest(intent, D(risk), requested_at, version, decision)


def new_authority(clock=None, *states) -> ReferenceAtomicRiskAuthority:
    auth = ReferenceAtomicRiskAuthority(CONFIG, clock or FakeClock(),
                                        acknowledge_reference_only_non_production=True)
    for s in (states or (account_state(),)):
        auth.register_account(s)
    return auth


def reserved_total(auth, pid="p1") -> Decimal:
    return sum((r.risk_usd for r in auth.state().ledger(pid).reservations), Decimal("0"))


def run_concurrently(fns):
    barrier = threading.Barrier(len(fns))
    results = [None] * len(fns)

    def runner(i, fn):
        barrier.wait()
        results[i] = fn()

    threads = [threading.Thread(target=runner, args=(i, fn)) for i, fn in enumerate(fns)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return results


@pytest.fixture
def slow_evaluation(monkeypatch):
    """Widen the race window between capacity check and commit."""
    def slow(*args, **kwargs):
        time.sleep(0.002)
        return evaluate_risk(*args, **kwargs)
    monkeypatch.setattr(atomic_module, "evaluate_risk", slow)


# ---- 01-05 authorization and idempotency ----------------------------------------

def test_01_valid_authorization_reserves_once():
    auth = new_authority()
    r = auth.authorize_and_reserve(request())
    assert r.code is A.AUTHORIZED_RESERVED and r.authorized and not r.idempotent_replay
    assert len(auth.state().reservations) == 1


def test_02_result_contains_reservation():
    auth = new_authority()
    r = auth.authorize_and_reserve(request())
    res = r.reservation
    assert res is not None and res.status is ReferenceReservationStatus.ACTIVE
    assert (res.child_trade_id, res.thesis_id, res.strategy_id, res.profile_id, res.account) ==         ("A-1:p1", "A-1", StrategyId.STRATEGY_A, "p1", P1.identity)
    assert (res.canonical_symbol, res.side, res.risk_usd, res.source_policy_version, res.created_at) ==         ("XAUUSD", Side.BUY, D("400"), VERSION, NOW)
    assert res.reservation_id == "REF-RESV:A-1:p1" and r.authorized_at == NOW


def test_03_reservation_immediately_consumes_capacity():
    auth = new_authority()
    auth.authorize_and_reserve(request(risk="400", thesis_id="A-1"))
    second = auth.authorize_and_reserve(request(risk="200", thesis_id="A-2"))
    assert second.code is A.BLOCK_OPEN_RISK
    assert second.risk_evaluation.used_capacity == D("400")          # reservation counted with no broker step
    positions = auth.authorize_and_reserve(request(risk="100", thesis_id="A-3"))
    assert positions.authorized
    derived = atomic_module.derive_evaluation_snapshot(auth.state().ledger("p1"))
    assert derived.reserved_position_slots == 2 and derived.open_positions == 0   # slots, not positions


def test_04_exact_retry_is_idempotent():
    auth = new_authority(None, account_state(limit_kw=dict(max_open_risk_usd=D("400"))))  # fully consumed by first
    first = auth.authorize_and_reserve(request())
    retry = auth.authorize_and_reserve(request(requested_at=NOW + timedelta(seconds=3)))
    assert first.authorized and not first.idempotent_replay
    assert retry.code is A.AUTHORIZED_RESERVED and retry.idempotent_replay is True
    assert len(auth.state().reservations) == 1 and reserved_total(auth) == D("400")


def test_05_exact_retry_returns_same_reservation():
    auth = new_authority()
    first = auth.authorize_and_reserve(request())
    retries = [auth.authorize_and_reserve(request()) for _ in range(5)]
    assert all(r.reservation is first.reservation for r in retries)
    assert all(r.risk_evaluation is first.risk_evaluation and r.authorized_at == first.authorized_at for r in retries)


# ---- 06-07 conflicting duplicates ------------------------------------------------

CONFLICTS = {
    "risk": dict(risk="300"),
    "side": dict(side=Side.SELL),
    "symbol": dict(symbol="EURUSD"),
    "policy_version": dict(version="TEST-OTHER"),
    "entry_level": dict(entry=D("2651")),
    "strategy": dict(strategy=StrategyId.STRATEGY_B),
    "account": dict(profile=dataclasses.replace(P1, identity=AccountIdentity("999", "S", "C", "USD"))),
}


@pytest.mark.parametrize("name", sorted(CONFLICTS))
def test_06_conflicting_duplicate_trade_id_blocks(name):
    auth = new_authority()
    assert auth.authorize_and_reserve(request()).authorized
    r = auth.authorize_and_reserve(request(**CONFLICTS[name]))
    assert r.code is A.DUPLICATE_TRADE_ID and r.reservation is None


def test_06b_different_thesis_or_profile_is_a_different_child_trade_id():
    """thesis_id and profile_id are part of child_trade_id itself."""
    assert request(thesis_id="A-9").child_trade_id != request().child_trade_id
    assert request(profile=P2).child_trade_id != request().child_trade_id


@pytest.mark.parametrize("name", sorted(CONFLICTS))
def test_07_conflicting_duplicate_does_not_alter_original(name):
    auth = new_authority()
    original = auth.authorize_and_reserve(request()).reservation
    before = auth.state()
    auth.authorize_and_reserve(request(**CONFLICTS[name]))
    assert auth.state() is before
    assert auth.state().reservation_for("A-1:p1") is original and original.risk_usd == D("400")


# ---- 08-15 old PASS is never trusted -------------------------------------------------

def _earlier_day3_pass(auth, req, pid="p1"):
    ledger = auth.state().ledger(pid)
    ev = evaluate_risk(RiskEvaluationRequest(req.intent, req.proposed_risk_usd, None, NOW),
                       atomic_module.derive_evaluation_snapshot(ledger), ledger.account_state.limits, CONFIG)
    assert ev.passed
    return ev


def test_08_stale_state_after_earlier_pass_blocks():
    clock = FakeClock()
    auth = new_authority(clock)
    req = request()
    _earlier_day3_pass(auth, req)
    clock.now = NOW + timedelta(minutes=1)
    before = auth.state()
    r = auth.authorize_and_reserve(req)
    assert r.code is A.STALE_ACCOUNT_STATE and r.reservation is None and auth.state() is before


def test_08b_caller_requested_at_cannot_bypass_freshness():
    clock = FakeClock(NOW + timedelta(minutes=1))
    auth = new_authority(clock)
    assert auth.authorize_and_reserve(request(requested_at=NOW)).code is A.STALE_ACCOUNT_STATE


@pytest.mark.parametrize("test_id,change,expected", [
    ("09", dict(account_halted=True), A.BLOCK_ACCOUNT_HALT),
    ("10", dict(daily_halted=True), A.BLOCK_DAILY_HALT),
    ("11", dict(committed_open_risk_usd=D("200")), A.BLOCK_OPEN_RISK),
    ("12", dict(committed_open_positions=5), A.BLOCK_MAX_POSITIONS),
    ("13", dict(realized_daily_pnl=D("-4700")), A.BLOCK_DAILY_LIMIT),
    ("14", dict(balance=D("90000"), equity=D("90000")), A.BLOCK_MAX_LOSS),
    ("15", dict(limit_kw=dict(max_risk_per_trade_usd=D("300"))), A.BLOCK_TRADE_RISK),
])
def test_09_to_15_changed_state_after_earlier_pass_is_rechecked(test_id, change, expected):
    auth = new_authority()
    req = request()
    _earlier_day3_pass(auth, req)
    auth.replace_account_state(account_state(**change))
    before = auth.state()
    r = auth.authorize_and_reserve(req)
    assert r.code is expected, (test_id, r.reason)
    assert r.reservation is None and auth.state() is before
    assert RISK_BLOCK_TO_AUTHORIZATION_CODE[r.risk_evaluation.code] is expected


# ---- 16-19 concurrency ----------------------------------------------------------------

ROUNDS = 60


def test_16_400_plus_400_against_500_exactly_one_reservation(slow_evaluation):
    for i in range(ROUNDS):
        auth = new_authority()
        a, b = run_concurrently([
            lambda: auth.authorize_and_reserve(request(risk="400", thesis_id=f"A-{i}")),
            lambda: auth.authorize_and_reserve(request(risk="400", thesis_id=f"B-{i}", strategy=StrategyId.STRATEGY_B)),
        ])
        codes = sorted([a.code.value, b.code.value])
        assert codes == ["AUTHORIZED_RESERVED", "BLOCK_OPEN_RISK"], (i, codes)
        assert len(auth.state().reservations) == 1


def test_17_concurrent_final_reserved_total_correct(slow_evaluation):
    for i in range(ROUNDS // 3):
        auth = new_authority(None, account_state(limit_kw=dict(max_positions=50)))   # isolate open-risk capacity
        results = run_concurrently([
            (lambda k=k: auth.authorize_and_reserve(request(risk="100", thesis_id=f"T{i}-{k}")))
            for k in range(12)
        ])
        assert sum(r.authorized for r in results) == 5
        assert reserved_total(auth) == D("500")
        assert all(r.code is A.BLOCK_OPEN_RISK for r in results if not r.authorized)


def test_18_same_id_concurrency_creates_one_reservation(slow_evaluation):
    for i in range(ROUNDS // 3):
        auth = new_authority()
        results = run_concurrently([lambda: auth.authorize_and_reserve(request(thesis_id=f"S-{i}"))] * 16)
        assert all(r.code is A.AUTHORIZED_RESERVED for r in results)
        assert len({id(r.reservation) for r in results}) == 1
        assert sum(not r.idempotent_replay for r in results) == 1
        assert len(auth.state().reservations) == 1


def test_19_same_id_retries_counted_once(slow_evaluation):
    auth = new_authority()
    run_concurrently([lambda: auth.authorize_and_reserve(request(risk="400"))] * 10)
    for _ in range(10):
        auth.authorize_and_reserve(request(risk="400"))
    assert reserved_total(auth) == D("400")
    assert atomic_module.derive_evaluation_snapshot(auth.state().ledger("p1")).open_risk_usd == D("400")
    assert auth.authorize_and_reserve(request(risk="100", thesis_id="other")).authorized   # 400 + 100 == 500


# ---- 20-21 shared capacity / isolation ------------------------------------------------

@pytest.mark.parametrize("first", [StrategyId.STRATEGY_A, StrategyId.STRATEGY_B])
def test_20_strategy_a_and_b_share_capacity(first):
    second = StrategyId.STRATEGY_B if first is StrategyId.STRATEGY_A else StrategyId.STRATEGY_A
    auth = new_authority()
    r1 = auth.authorize_and_reserve(request(risk="300", thesis_id="X-1", strategy=first))
    r2 = auth.authorize_and_reserve(request(risk="300", thesis_id="X-2", strategy=second))
    assert r1.authorized and r2.code is A.BLOCK_OPEN_RISK
    assert r2.risk_evaluation.used_capacity == D("300")


def test_21_separate_accounts_remain_isolated():
    auth = new_authority(None, account_state("p1", P1), account_state("p2", P2))
    assert auth.authorize_and_reserve(request(risk="500", thesis_id="A-1", profile=P1)).authorized
    assert auth.authorize_and_reserve(request(risk="500", thesis_id="A-1", profile=P2)).authorized
    assert auth.authorize_and_reserve(request(risk="1", thesis_id="A-2", profile=P1)).code is A.BLOCK_OPEN_RISK
    assert reserved_total(auth, "p1") == reserved_total(auth, "p2") == D("500")
    assert atomic_module.derive_evaluation_snapshot(auth.state().ledger("p2")).open_risk_usd == D("500")


def test_21b_unregistered_account_blocks():
    auth = new_authority()
    r = auth.authorize_and_reserve(request(profile=P2))
    assert r.code is A.BLOCK_ACCOUNT_MISMATCH and not auth.state().reservations


# ---- 22-26 policy precondition and failure atomicity --------------------------------

def _decision(req, **kw):
    base = dict(child_trade_id=req.child_trade_id, code=PolicyDecisionCode.ELIGIBLE, reason="x",
                policy_version=VERSION, evaluated_at=NOW - timedelta(seconds=1))
    base.update(kw)
    return PolicyDecision(**base)


@pytest.mark.parametrize("case", ["missing", "not_eligible", "other_child", "other_version", "future",
                                  "limits_version"])
def test_22_policy_precondition_missing_blocks(case):
    auth = new_authority()
    base = request()
    decision = {
        "missing": None,
        "not_eligible": _decision(base, code=PolicyDecisionCode.BLOCK_INSTRUMENT, policy_version=VERSION),
        "other_child": _decision(base, child_trade_id="Z-9:p1"),
        "other_version": _decision(base, policy_version="TEST-OTHER"),
        "future": _decision(base, evaluated_at=NOW + timedelta(seconds=1)),
        "limits_version": _decision(base),
    }[case]
    if case == "limits_version":
        auth.replace_account_state(account_state(limit_kw=dict(source_policy_version="TEST-NEWER")))
    before = auth.state()
    r = auth.authorize_and_reserve(dataclasses.replace(base, policy_decision=decision))
    assert r.code is A.BLOCK_POLICY_PRECONDITION and r.risk_evaluation is None and auth.state() is before


def test_22b_policy_evidence_is_not_capacity_proof():
    auth = new_authority(None, account_state(committed_open_risk_usd=D("450")))
    r = auth.authorize_and_reserve(request(risk="100"))
    assert r.code is A.BLOCK_OPEN_RISK


@pytest.mark.parametrize("change", [
    dict(proposed_risk_usd=D("0")), dict(proposed_risk_usd=Decimal("NaN")), dict(proposed_risk_usd=400.0),
    dict(requested_at=datetime(2026, 9, 16, 12, 0)), dict(source_policy_version=" "), dict(policy_decision="yes"),
])
def test_23_invalid_request_leaves_state_unchanged(change):
    auth = new_authority()
    auth.authorize_and_reserve(request(thesis_id="seed", risk="100"))
    before = auth.state()
    r = auth.authorize_and_reserve(dataclasses.replace(request(), **change))
    assert r.code is A.BLOCK_INVALID_RISK_INPUT and r.reservation is None and auth.state() is before


def test_23b_invalid_ledger_state_fails_closed():
    auth = new_authority()
    auth.replace_account_state(account_state(committed_open_risk_usd=Decimal("sNaN")))
    before = auth.state()
    assert auth.authorize_and_reserve(request()).code is A.BLOCK_INVALID_RISK_INPUT and auth.state() is before
    auth.replace_account_state(account_state(max_loss_reference_balance=None))
    before = auth.state()
    r = auth.authorize_and_reserve(request())
    assert r.code is A.BLOCK_INVALID_RISK_INPUT and r.risk_evaluation is not None and auth.state() is before


def test_24_capacity_block_leaves_state_unchanged():
    auth = new_authority()
    auth.authorize_and_reserve(request(risk="400"))
    before = auth.state()
    r = auth.authorize_and_reserve(request(risk="101", thesis_id="B-1"))
    assert r.code is A.BLOCK_OPEN_RISK and auth.state() is before and auth.state() == before


def test_25_duplicate_conflict_leaves_state_unchanged():
    auth = new_authority()
    auth.authorize_and_reserve(request())
    before = auth.state()
    assert auth.authorize_and_reserve(request(risk="1")).code is A.DUPLICATE_TRADE_ID
    assert auth.state() is before


@pytest.mark.parametrize("failure", ["raises", "returns_none", "clock_raises", "clock_naive"])
def test_26_evaluation_exception_leaves_state_unchanged(monkeypatch, failure):
    clock = FakeClock()
    auth = new_authority(clock)
    auth.authorize_and_reserve(request(thesis_id="seed", risk="100"))
    before = auth.state()
    if failure == "raises":
        monkeypatch.setattr(atomic_module, "evaluate_risk", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")))
    elif failure == "returns_none":
        monkeypatch.setattr(atomic_module, "evaluate_risk", lambda *a, **k: None)
    elif failure == "clock_raises":
        auth._clock = lambda: (_ for _ in ()).throw(OSError("no time"))
    else:
        auth._clock = lambda: datetime(2026, 9, 16, 12, 0)
    r = auth.authorize_and_reserve(request())
    assert r.code is A.BLOCK_REFERENCE_EVALUATION_FAILURE and r.reservation is None
    assert auth.state() is before


# ---- 27-30 dependencies ------------------------------------------------------------------

DAY4_FILES = ("atomic_contracts.py", "atomic_reference.py")


def _sources():
    from api.xtl_platform.tests.test_isolation import PKG_ROOT
    return {n: (PKG_ROOT / "risk" / n).read_text() for n in DAY4_FILES}


def _names(source):
    tree = ast.parse(source)
    names = {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)}
    names |= {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)}
    names |= {a.name for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) for a in n.names}
    modules = {(n.module or "", n.level) for n in ast.walk(tree) if isinstance(n, ast.ImportFrom)}
    modules |= {(a.name, 0) for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names}
    return names, modules


def test_27_no_broker_side_effects():
    forbidden = {"ExecutionCommand", "ExecutionAdapter", "ExecutionAck", "BrokerPosition", "place", "_submit",
                 "order_send", "send", "queue", "Queue"}
    for name, source in _sources().items():
        names, modules = _names(source)
        assert names.isdisjoint(forbidden), (name, names & forbidden)
        assert not any("execution" in m or "accounts" in m for m, _ in modules), name
    assert AtomicAuthorizationResult.IS_ORDER is False


def test_28_no_redis_dependency():
    from api.xtl_platform.tests.test_isolation import PACKAGE, scan_source
    redis_words = ("redis", "lua", "evalsha", "setnx", "hset", "rpush", "lpush", "setex", "watch", "multi")
    for name, source in _sources().items():
        assert scan_source(source, PACKAGE + ("risk",)) == [], name
        names, modules = _names(source)
        assert not {n.lower() for n in names} & set(redis_words), name
        components = {c.lower() for m, _ in modules for c in m.split(".") if c}
        assert not components & set(redis_words), (name, components & set(redis_words))


def test_29_no_filesystem_network_environment_dependency():
    for name, source in _sources().items():
        names, modules = _names(source)
        top = {m.split(".")[0] for m, level in modules if level == 0}
        assert top <= {"__future__", "threading", "datetime", "decimal", "typing", "dataclasses"}, (name, top)
        tree = ast.parse(source)
        attributes = {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)}
        called = {n.func.id for n in ast.walk(tree) if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)}
        assert attributes.isdisjoint({"now", "utcnow", "today", "environ", "getenv", "sleep", "time"}), name
        assert called.isdisjoint({"open", "__import__", "exec", "eval", "compile", "input"}), name


def test_30_no_strategy_ab_imports():
    from api.xtl_platform.tests.test_isolation import PACKAGE, scan_source
    for name, source in _sources().items():
        _, modules = _names(source)
        assert not any("strategy" in m.lower() for m, _ in modules), name
        assert scan_source(source, PACKAGE + ("risk",)) == []


# ---- 31-33 frozen baseline -------------------------------------------------------------------

# CSA-frozen Day-3 host baseline (xautrendlab-api). test_isolation.py excluded: Day 4 extends it.
FROZEN_DAY3_SHA256 = {
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
    "risk/authority.py": "b0df075a1617389936823f713a7fbbe978831307531b4706e7ee21419af1f0a7",
    "risk/evaluator.py": "c2cfcf8cab8877f7528d50338c12b45443c340eb2cd33a91ba56c109e88f8201",
    "risk/reference_test_only.py": "3edd3c151fe49ad264fae52986ae91180a71ba46e6adfe9c640c50e293eca9e7",
    "tests/__init__.py": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    "tests/_builders.py": "4bf536e11dac33ef9f5dff0c176e55abcf6f85d96e8ce12a59e7207d6255a51a",
    "tests/conftest.py": "0893751f3d47a26d26e4294c8a348c78393cee79e133f6d5bc1ef2c2e6e6b79f",
    "tests/test_account_identity.py": "2165ec6dcca34a73b96c8555bddfffe4cde7f4be42e7167ed7db2239bb4b61e0",
    "tests/test_domain_contracts.py": "a64712d0114904d8d3aa308bfbbd834cf8286144ac3744381dfee47a961c0ee3",
    "tests/test_policy_engine.py": "791cb3b9c0c314e4773fd3ea69c33bba1c0fcab9f318728a61aaefd2cca34187",
    "tests/test_risk_evaluator.py": "e64c800d1344cad6d0469eebf323b61d7963e318ade8cf924921d066e2929d82",
    "tests/test_thesis_fanout.py": "1fb20203b692d95c6f45054b804f051b6e840ab0079ca4cf4c6fe9d607ba4ea3",
}

# Day-3 files changed ADDITIVELY by the Day-4 CSA position-slot correction
# (reserved_position_slots). Pinned to the corrected content; CSA acknowledgement required.
DAY3_FILES_WITH_CSA_ACKNOWLEDGED_ADDITIVE_CHANGE = {
    "risk/contracts.py": "ece8a4061eb89fdca370184785540604d16b61599639e9a6e696218a269e0da1",
    "risk/validation.py": "ddea557444c5e8513fa93631cafb7a1591fabd2959e4f880825eee7c90425bad",
    "risk/calculator.py": "ac12bc2ee316f4b0519c09740d4b27744f55fcc2969ca9438382733ebff6b603",
}
DAY1_TESTS = ("tests/test_domain_contracts.py", "tests/test_account_identity.py", "tests/test_thesis_fanout.py")
DAY2_TESTS = ("tests/test_policy_engine.py",)
DAY3_TESTS = ("tests/test_risk_evaluator.py",)


def _sha(rel):
    from api.xtl_platform.tests.test_isolation import PKG_ROOT
    return hashlib.sha256((PKG_ROOT / rel).read_bytes()).hexdigest()


@pytest.mark.parametrize("group", [DAY1_TESTS, DAY2_TESTS, DAY3_TESTS], ids=["31_day1", "32_day2", "33_day3"])
def test_31_to_33_previous_day_tests_unchanged(group):
    for rel in group:
        assert _sha(rel) == FROZEN_DAY3_SHA256[rel], rel


def test_33b_no_frozen_file_modified():
    """31 frozen files unchanged; exactly 3 Day-3 files carry the acknowledged additive change."""
    assert [rel for rel, digest in FROZEN_DAY3_SHA256.items() if _sha(rel) != digest] == []
    assert len(FROZEN_DAY3_SHA256) == 31
    assert [rel for rel, digest in DAY3_FILES_WITH_CSA_ACKNOWLEDGED_ADDITIVE_CHANGE.items() if _sha(rel) != digest] == []
    assert set(DAY3_FILES_WITH_CSA_ACKNOWLEDGED_ADDITIVE_CHANGE) == {"risk/contracts.py", "risk/validation.py",
                                                                     "risk/calculator.py"}


# ---- 34-40 contracts, evidence, formula ownership, lock, determinism ---------------------

def test_34_reservation_request_result_state_immutable():
    auth = new_authority()
    req = request()
    r = auth.authorize_and_reserve(req)
    for obj, field, value in [(req, "proposed_risk_usd", D("1")), (r.reservation, "risk_usd", D("1")),
                              (r, "code", A.BLOCK_OPEN_RISK), (auth.state(), "ledgers", ()),
                              (auth.state().ledger("p1"), "reservations", ())]:
        with pytest.raises(dataclasses.FrozenInstanceError):
            setattr(obj, field, value)
    assert isinstance(auth.state().ledgers, tuple) and isinstance(auth.state().ledger("p1").reservations, tuple)


def test_35_result_preserves_day3_evaluation_evidence():
    auth = new_authority()
    ok = auth.authorize_and_reserve(request(risk="400"))
    assert ok.risk_evaluation.passed and ok.risk_evaluation is ok.reservation.authorizing_evaluation
    assert [c.name.value for c in ok.risk_evaluation.checks][-1] == "OPEN_RISK"
    blocked = auth.authorize_and_reserve(request(risk="200", thesis_id="B-1"))
    ev = blocked.risk_evaluation
    assert ev.code is C.BLOCK_OPEN_RISK and ev.used_capacity == D("400") and ev.relevant_limit == D("500")
    assert ev.evaluated_at == NOW and ev.child_trade_id == blocked.child_trade_id


def _any_result(**kw):
    auth = new_authority()
    base = auth.authorize_and_reserve(request())
    return dataclasses.replace(base, **kw)


def test_36_authorized_reserved_only_occurs_with_reservation():
    with pytest.raises(Exception):
        _any_result(reservation=None)
    with pytest.raises(Exception):
        _any_result(authorized_at=None)
    assert all(RISK_BLOCK_TO_AUTHORIZATION_CODE[c] is not A.AUTHORIZED_RESERVED for c in RISK_BLOCK_TO_AUTHORIZATION_CODE)
    assert set(RISK_BLOCK_TO_AUTHORIZATION_CODE) == set(C) - {C.RISK_EVALUATION_PASS}


@pytest.mark.parametrize("code", [c for c in A if c is not A.AUTHORIZED_RESERVED])
def test_37_block_result_can_never_carry_reservation(code):
    with pytest.raises(Exception):
        _any_result(code=code)
    with pytest.raises(Exception):
        _any_result(code=code, authorized_at=None)
    with pytest.raises(Exception):
        _any_result(code=code, reservation=None, authorized_at=None, idempotent_replay=True)


def test_38_no_second_risk_formula_in_day4_authority():
    limit_fields = {"max_risk_per_trade_usd", "max_positions", "max_open_risk_usd", "daily_loss_limit_usd",
                    "max_loss_limit_usd", "daily_loss_model", "max_loss_model", "daily_floating_pnl",
                    "max_loss_floating_pnl"}
    calc_functions = {"daily_loss_check", "max_loss_check", "open_risk_check", "max_positions_check",
                      "trade_risk_check", "build_check", "floating_component"}
    for name, source in _sources().items():
        names, modules = _names(source)
        assert names.isdisjoint(limit_fields | calc_functions), (name, names & (limit_fields | calc_functions))
    ref = ast.parse(_sources()["atomic_reference.py"])
    calc_imports = [a.name for n in ast.walk(ref) if isinstance(n, ast.ImportFrom) and n.module == "calculator"
                    for a in n.names]
    assert calc_imports == ["RISK_DECIMAL_CONTEXT"]
    calls = [n for n in ast.walk(ref) if isinstance(n, ast.Call) and getattr(n.func, "id", None) == "evaluate_risk"]
    assert len(calls) == 1
    compares = [n for n in ast.walk(ref) if isinstance(n, ast.Compare)
                and any(isinstance(op, (ast.Lt, ast.LtE, ast.Gt, ast.GtE)) for op in n.ops)]
    assert len(compares) == 2   # proposed_risk <= 0 validation and policy evidence dated after now


class RecordingLock:
    def __init__(self, events):
        self._inner, self.events, self.held = threading.Lock(), events, False

    def __enter__(self):
        self._inner.acquire()
        self.held = True
        self.events.append("acquire")
        return self

    def __exit__(self, *exc):
        self.events.append("release")
        self.held = False
        self._inner.release()

    def locked(self):
        return self.held


def test_39_full_operation_is_under_atomic_reference_lock(monkeypatch):
    events = []
    auth = new_authority()
    lock = RecordingLock(events)
    auth._lock = lock
    clock = auth._clock
    auth._clock = lambda: (events.append(("clock", lock.held)), clock())[1]
    monkeypatch.setattr(atomic_module, "evaluate_risk",
                        lambda *a, **k: (events.append(("evaluate", lock.held)), evaluate_risk(*a, **k))[1])
    original_commit = auth._commit
    auth._commit = lambda s: (events.append(("commit", lock.held)), original_commit(s))[1]
    r = auth.authorize_and_reserve(request())
    assert r.authorized
    assert events == ["acquire", ("clock", True), ("evaluate", True), ("commit", True), "release"]


def test_39b_commit_outside_lock_is_refused():
    auth = new_authority()
    with pytest.raises(RuntimeError, match="outside the atomic section"):
        auth._commit(auth.state())


def test_40_deterministic_behavior_for_sequential_inputs():
    def run():
        clock = FakeClock()
        auth = new_authority(clock, account_state("p1", P1), account_state("p2", P2))
        out = []
        for i, (risk, profile, strategy) in enumerate([("300", P1, StrategyId.STRATEGY_A), ("300", P1, StrategyId.STRATEGY_B),
                                                       ("200", P1, StrategyId.STRATEGY_B), ("600", P2, StrategyId.STRATEGY_A),
                                                       ("200", P1, StrategyId.STRATEGY_A)]):
            clock.now = NOW + timedelta(seconds=i)
            out.append(auth.authorize_and_reserve(request(risk=risk, thesis_id=f"T-{i}", profile=profile, strategy=strategy)))
        out.append(auth.authorize_and_reserve(request(risk="300", thesis_id="T-0", profile=P1)))
        return out, auth.state()

    (r1, s1), (r2, s2) = run(), run()
    assert r1 == r2 and s1 == s2
    assert [r.code.value for r in r1] == ["AUTHORIZED_RESERVED", "BLOCK_OPEN_RISK", "AUTHORIZED_RESERVED",
                                          "BLOCK_OPEN_RISK", "BLOCK_OPEN_RISK", "AUTHORIZED_RESERVED"]
    assert r1[-1].idempotent_replay


# ---- reference-only marking -----------------------------------------------------------------

def test_reference_authority_marked_non_production():
    assert ReferenceAtomicRiskAuthority.GRANTS_PRODUCTION_AUTHORITY is False
    assert ReferenceAtomicRiskAuthority.AUTHORITY_CLASSIFICATION == "REFERENCE_ONLY_NON_PRODUCTION"
    assert "NON-PRODUCTION" in ReferenceAtomicRiskAuthority.__doc__
    for source in _sources().values():
        assert "REFERENCE ONLY - NON-PRODUCTION" in source
    with pytest.raises(RuntimeError, match="NON-PRODUCTION"):
        ReferenceAtomicRiskAuthority(CONFIG, FakeClock())
    import api.xtl_platform.risk as risk_pkg
    assert risk_pkg.__all__ == ["RiskAuthority"]


def test_no_release_or_production_names_exist():
    from api.xtl_platform.tests.test_isolation import PKG_ROOT
    public = {n for n in dir(ReferenceAtomicRiskAuthority) if not n.startswith("_")}
    assert public == {"authorize_and_reserve", "register_account", "replace_account_state", "state",
                      "GRANTS_PRODUCTION_AUTHORITY", "AUTHORITY_CLASSIFICATION"}
    for bad in ("redis_authority.py", "production_authority.py", "shared_risk_service.py"):
        assert not (PKG_ROOT / "risk" / bad).exists()


def test_replace_account_state_cannot_change_identity_or_drop_reservations():
    auth = new_authority()
    res = auth.authorize_and_reserve(request()).reservation
    with pytest.raises(Exception):
        auth.replace_account_state(account_state(account=AccountIdentity("999", "S", "C", "USD")))
    auth.replace_account_state(account_state(realized_daily_pnl=D("-10")))
    assert auth.state().ledger("p1").reservations == (res,)
    with pytest.raises(Exception):
        auth.register_account(account_state())
