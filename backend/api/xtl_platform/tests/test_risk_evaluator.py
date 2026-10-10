"""Day 3 - pure Shared Risk Evaluation Engine.

RISK_EVALUATION_PASS IS NOT AUTHORIZATION.
Numbered tests map 1:1 to the Day-3 brief, section 23.
All limit values are TEST FIXTURE - NOT VERIFIED PRODUCTION FIRM RULE.
"""
from __future__ import annotations

import ast
import copy
import dataclasses
import decimal
import hashlib
import threading
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from api.xtl_platform.domain.account import AccountIdentity
from api.xtl_platform.domain.enums import LossModel, RiskDecisionCode, Side
from api.xtl_platform.domain.thesis import ChildExecutionIntent
from api.xtl_platform.risk.contracts import (
    CapacityCheckName as N,
    FloatingPnlTreatment as F,
    RiskEvaluation,
    RiskEvaluationCode as C,
    RiskEvaluationConfig,
    RiskEvaluationLimits,
    RiskEvaluationRequest,
    RiskEvaluationSnapshot,
)
from api.xtl_platform.risk.evaluator import PASS_REASON, RiskEvaluationEngine, evaluate_risk
from api.xtl_platform.risk.validation import RiskInputValidationError, validate_risk_inputs
from api.xtl_platform.tests._builders import NOW, D, make_limits, make_profile, make_snapshot, make_thesis

UTC = timezone.utc
CONFIG = RiskEvaluationConfig(max_snapshot_age=timedelta(seconds=30))
ENGINE = RiskEvaluationEngine(CONFIG)
PROFILE = make_profile("p1")


# ---- builders (TEST FIXTURE - NOT VERIFIED PRODUCTION FIRM RULE) -----------

def snap(**kw) -> RiskEvaluationSnapshot:
    base = dict(profile_id="p1", account=PROFILE.identity, as_of=NOW - timedelta(seconds=5),
                balance=D("100000"), equity=D("100000"), realized_daily_pnl=D("0"),
                open_risk_usd=D("0"), open_positions=0, account_halted=False, daily_halted=False,
                max_loss_reference_balance=D("100000"))
    base.update(kw)
    return RiskEvaluationSnapshot(**base)


def lim(**kw) -> RiskEvaluationLimits:
    base = dict(profile_id="p1", source_policy_version="TEST-2026.09",
                max_risk_per_trade_usd=D("1000"), max_positions=3, max_open_risk_usd=D("3000"),
                daily_loss_model=LossModel.FIXED, daily_loss_limit_usd=D("5000"), daily_floating_pnl=F.LOSSES_ONLY,
                max_loss_model=LossModel.STATIC, max_loss_limit_usd=D("10000"), max_loss_floating_pnl=F.LOSSES_ONLY)
    base.update(kw)
    return RiskEvaluationLimits(**base)


def req(profile=PROFILE, thesis_id="A-1", **kw) -> RiskEvaluationRequest:
    base = dict(intent=ChildExecutionIntent.from_thesis(make_thesis(thesis_id=thesis_id), profile, "XAUUSD"),
                proposed_risk_usd=D("500"), proposed_risk_pct=None, evaluated_at=NOW)
    base.update(kw)
    return RiskEvaluationRequest(**base)


def ev(request=None, snapshot=None, limits=None) -> RiskEvaluation:
    return ENGINE.evaluate(request or req(), snapshot or snap(), limits or lim())


def replace_unchecked(obj, **changes):
    clone = object.__new__(type(obj))
    for f in dataclasses.fields(obj):
        object.__setattr__(clone, f.name, changes.get(f.name, getattr(obj, f.name)))
    return clone


# ---- 01-02 -------------------------------------------------------------------

def test_01_valid_request_passes():
    r = ev()
    assert r.code is C.RISK_EVALUATION_PASS and r.passed
    assert [c.name for c in r.checks] == [N.TRADE_RISK, N.MAX_POSITIONS, N.DAILY_LOSS, N.MAX_LOSS, N.OPEN_RISK]
    assert "NOT AUTHORIZATION" in r.reason and r.reason == PASS_REASON


def test_02_repeated_identical_evaluation_is_identical():
    request, snapshot, limits = req(), snap(realized_daily_pnl=D("-1234.56")), lim()
    results = [ENGINE.evaluate(request, snapshot, limits) for _ in range(10)]
    results.append(evaluate_risk(request, snapshot, limits, CONFIG))
    results.append(RiskEvaluationEngine(RiskEvaluationConfig(timedelta(seconds=30))).evaluate(request, snapshot, limits))
    assert all(r == results[0] for r in results)


# ---- 03-06 identity and freshness -------------------------------------------

def test_03_profile_mismatch_blocks():
    assert ev(snapshot=snap(profile_id="p2")).code is C.BLOCK_ACCOUNT_MISMATCH
    assert ev(limits=lim(profile_id="p2")).code is C.BLOCK_ACCOUNT_MISMATCH


def test_04_account_mismatch_blocks():
    other = AccountIdentity(login="999999", server=PROFILE.identity.server,
                            company=PROFILE.identity.company, currency=PROFILE.identity.currency)
    r = ev(snapshot=snap(account=other))
    assert r.code is C.BLOCK_ACCOUNT_MISMATCH and r.checks == ()


def test_05_stale_snapshot_blocks():
    assert ev(snapshot=snap(as_of=NOW - timedelta(seconds=30))).passed                 # age == max: fresh
    r = ev(snapshot=snap(as_of=NOW - timedelta(seconds=30, microseconds=1)))
    assert r.code is C.STALE_ACCOUNT_STATE


def test_05b_freshness_threshold_comes_from_config():
    old = snap(as_of=NOW - timedelta(minutes=5))
    assert ENGINE.evaluate(req(), old, lim()).code is C.STALE_ACCOUNT_STATE
    assert RiskEvaluationEngine(RiskEvaluationConfig(timedelta(minutes=10))).evaluate(req(), old, lim()).passed


def test_06_future_snapshot_blocks():
    r = ev(snapshot=snap(as_of=NOW + timedelta(microseconds=1)))
    assert r.code is C.STALE_ACCOUNT_STATE and "after evaluation" in r.reason
    assert ev(snapshot=snap(as_of=NOW)).passed                                         # same instant: fresh


def test_06b_freshness_compares_instants_across_timezones():
    plus2 = timezone(timedelta(hours=2))
    r = ev(snapshot=snap(as_of=(NOW - timedelta(seconds=5)).astimezone(plus2)))
    assert r.passed


# ---- 07-08 halts ---------------------------------------------------------------

def test_07_account_halt_blocks():
    assert ev(snapshot=snap(account_halted=True)).code is C.BLOCK_ACCOUNT_HALT


def test_08_daily_halt_blocks():
    assert ev(snapshot=snap(daily_halted=True)).code is C.BLOCK_DAILY_HALT


# ---- 09-10 per-trade risk ------------------------------------------------------

def test_09_proposed_risk_above_per_trade_cap_blocks():
    r = ev(request=req(proposed_risk_usd=D("1000.01")))
    assert r.code is C.BLOCK_TRADE_RISK
    assert r.relevant_limit == D("1000") and r.used_capacity is None


def test_10_proposed_risk_equal_to_cap_passes():
    """Canonical rule: projected <= limit fits. Equal to cap is permitted."""
    r = ev(request=req(proposed_risk_usd=D("1000")))
    assert r.passed
    assert r.checks[0].remaining_after == D("0") and r.checks[0].fits


# ---- 11-12 max positions ---------------------------------------------------------

def test_11_max_positions_capacity_available_passes():
    r = ev(snapshot=snap(open_positions=2))          # max 3: 2 open + 1 new = 3 fits
    assert r.passed
    positions = next(c for c in r.checks if c.name is N.MAX_POSITIONS)
    assert positions.projected == D("3") and positions.remaining_after == D("0")


@pytest.mark.parametrize("open_positions", [3, 4])
def test_12_max_positions_exhausted_blocks(open_positions):
    r = ev(snapshot=snap(open_positions=open_positions))
    assert r.code is C.BLOCK_MAX_POSITIONS and r.projected_capacity_after < 0


# ---- 13-15 daily loss --------------------------------------------------------------

def test_13_daily_loss_capacity_available_passes():
    r = ev(snapshot=snap(realized_daily_pnl=D("-4000")))       # 4000 + 500 = 4500 <= 5000
    daily = next(c for c in r.checks if c.name is N.DAILY_LOSS)
    assert r.passed and daily.used == D("4000") and daily.projected == D("4500")


def test_14_daily_loss_projected_breach_blocks():
    r = ev(snapshot=snap(realized_daily_pnl=D("-4600")))       # 4600 + 500 = 5100 > 5000
    assert r.code is C.BLOCK_DAILY_LIMIT
    assert (r.relevant_limit, r.used_capacity, r.remaining_capacity_before, r.projected_capacity_after) == \
        (D("5000"), D("4600"), D("400"), D("-100"))


def test_15_daily_loss_exact_boundary_semantics_pinned():
    exact = ev(snapshot=snap(realized_daily_pnl=D("-4500")))                 # 4500 + 500 == 5000
    assert exact.passed
    assert next(c for c in exact.checks if c.name is N.DAILY_LOSS).remaining_after == D("0")
    over = ev(snapshot=snap(realized_daily_pnl=D("-4500.00000001")))
    assert over.code is C.BLOCK_DAILY_LIMIT


def test_15b_existing_open_risk_counts_toward_daily_loss():
    r = ev(snapshot=snap(realized_daily_pnl=D("-4000"), open_risk_usd=D("600")))   # 4000+600+500
    assert r.code is C.BLOCK_DAILY_LIMIT and r.used_capacity == D("4600")


@pytest.mark.parametrize("treatment,expected", [
    (F.EXCLUDED, C.BLOCK_DAILY_LIMIT),      # floating +2000 ignored: 4600 + 500 > 5000
    (F.LOSSES_ONLY, C.BLOCK_DAILY_LIMIT),   # floating profit adds no capacity
    (F.FULL, C.RISK_EVALUATION_PASS),       # explicitly permitted: 2600 + 500
])
def test_15c_floating_profit_only_adds_capacity_when_explicit(treatment, expected):
    s = snap(realized_daily_pnl=D("-4600"), equity=D("102000"))
    assert ev(snapshot=s, limits=lim(daily_floating_pnl=treatment)).code is expected


@pytest.mark.parametrize("treatment,expected", [
    (F.EXCLUDED, C.RISK_EVALUATION_PASS),   # floating loss ignored: 0 + 500
    (F.LOSSES_ONLY, C.BLOCK_DAILY_LIMIT),   # 4800 + 500 > 5000
    (F.FULL, C.BLOCK_DAILY_LIMIT),
])
def test_15d_floating_loss_treatment(treatment, expected):
    s = snap(equity=D("95200"))
    assert ev(snapshot=s, limits=lim(daily_floating_pnl=treatment, max_loss_floating_pnl=F.EXCLUDED)).code is expected


def test_15e_daily_model_none_skips_daily_check():
    r = ev(snapshot=snap(realized_daily_pnl=D("-9000")),
           limits=lim(daily_loss_model=LossModel.NONE, daily_loss_limit_usd=None, daily_floating_pnl=None))
    assert r.passed and r.skipped_checks == (N.DAILY_LOSS,)


# ---- 16-17 max loss ------------------------------------------------------------------

def test_16_max_loss_capacity_available_passes():
    r = ev(snapshot=snap(balance=D("91000"), equity=D("91000")))      # 9000 + 500 <= 10000
    assert r.passed
    assert next(c for c in r.checks if c.name is N.MAX_LOSS).used == D("9000")


def test_17_max_loss_projected_breach_blocks():
    r = ev(snapshot=snap(balance=D("90400"), equity=D("90400")))      # 9600 + 500 > 10000
    assert r.code is C.BLOCK_MAX_LOSS and r.used_capacity == D("9600")


def test_17b_max_loss_exact_boundary_and_trailing_reference():
    assert ev(snapshot=snap(balance=D("90500"), equity=D("90500"))).passed          # == limit
    trailing = lim(max_loss_model=LossModel.TRAILING_EOD)
    r = ev(snapshot=snap(max_loss_reference_balance=D("110000")), limits=trailing)  # 10000 + 500
    assert r.code is C.BLOCK_MAX_LOSS


def test_17c_balance_above_reference_gives_no_negative_loss():
    r = ev(snapshot=snap(balance=D("120000"), equity=D("120000")))
    check = next(c for c in r.checks if c.name is N.MAX_LOSS)
    assert check.used == D("0") and r.passed


# ---- 18-19 open risk ---------------------------------------------------------------

def test_18_open_risk_capacity_available_passes():
    r = ev(snapshot=snap(open_risk_usd=D("2500")))      # 2500 + 500 == 3000 fits
    assert r.passed
    assert next(c for c in r.checks if c.name is N.OPEN_RISK).remaining_after == D("0")


def test_19_open_risk_projected_breach_blocks():
    r = ev(snapshot=snap(open_risk_usd=D("2600")))
    assert r.code is C.BLOCK_OPEN_RISK and r.projected_capacity_after == D("-100")


# ---- 20-25 invalid input -------------------------------------------------------------

def _invalid(request=None, snapshot=None, limits=None, fragment=""):
    r = ev(request, snapshot, limits)
    assert r.code is C.BLOCK_INVALID_RISK_INPUT, r
    assert any(fragment in e for e in r.input_errors), r.input_errors
    assert r.checks == () and not r.passed
    with pytest.raises(RiskInputValidationError):
        validate_risk_inputs(request or req(), snapshot or snap(), limits or lim(), CONFIG)
    return r


def test_20_zero_proposed_risk_rejected():
    _invalid(request=req(proposed_risk_usd=D("0")), fragment="proposed_risk_usd")


def test_21_negative_proposed_risk_rejected():
    _invalid(request=req(proposed_risk_usd=D("-500")), fragment="proposed_risk_usd")


@pytest.mark.parametrize("value", [Decimal("NaN"), Decimal("sNaN"), Decimal("-NaN")])
def test_22_decimal_nan_rejected(value):
    _invalid(request=req(proposed_risk_usd=value), fragment="proposed_risk_usd")
    _invalid(snapshot=snap(balance=value), fragment="balance")


@pytest.mark.parametrize("value", [Decimal("Infinity"), Decimal("-Infinity")])
def test_23_decimal_infinity_rejected(value):
    _invalid(request=req(proposed_risk_usd=value), fragment="proposed_risk_usd")
    _invalid(limits=lim(max_open_risk_usd=value), fragment="max_open_risk_usd")


@pytest.mark.parametrize("value", [500.0, "500", 500])
def test_23b_no_silent_coercion(value):
    _invalid(request=req(proposed_risk_usd=value), fragment="proposed_risk_usd")


@pytest.mark.parametrize("pct", [D("0"), D("-1"), D("100.01"), Decimal("NaN"), 1.0])
def test_23c_invalid_percentage_rejected(pct):
    _invalid(request=req(proposed_risk_pct=pct), fragment="proposed_risk_pct")


@pytest.mark.parametrize("kwargs,snap_kwargs,fragment", [
    (dict(daily_loss_limit_usd=None), {}, "daily_loss_limit_usd is required"),
    (dict(daily_floating_pnl=None), {}, "floating PnL treatment is required"),
    (dict(max_loss_limit_usd=None), {}, "max_loss_limit_usd is required"),
    (dict(max_loss_floating_pnl=None), {}, "floating PnL treatment is required"),
    ({}, dict(max_loss_reference_balance=None), "max_loss_reference_balance is required"),
    (dict(daily_loss_model=LossModel.EQUITY_BASED, daily_floating_pnl=F.EXCLUDED), {}, "cannot exclude"),
    (dict(max_loss_model=LossModel.TRAILING_INTRADAY, max_loss_floating_pnl=F.EXCLUDED), {}, "cannot exclude"),
    (dict(daily_loss_model=LossModel.TRAILING_EOD), {}, "not supported"),
    (dict(max_loss_model=LossModel.FIXED), {}, "not supported"),
    (dict(daily_loss_model=LossModel.NONE), {}, "must be None when model is NONE"),
    (dict(max_positions=0), {}, "max_positions"),
    (dict(max_positions=True), {}, "max_positions"),
    (dict(source_policy_version=" "), {}, "source_policy_version"),
    ({}, dict(open_positions=-1), "open_positions"),
    ({}, dict(daily_halted=0), "daily_halted"),
    ({}, dict(open_risk_usd=D("-1")), "open_risk_usd"),
])
def test_24_missing_or_invalid_model_field_fails_closed(kwargs, snap_kwargs, fragment):
    _invalid(snapshot=snap(**snap_kwargs), limits=lim(**kwargs), fragment=fragment)


def test_25_naive_timestamps_rejected():
    r = _invalid(snapshot=snap(as_of=datetime(2026, 9, 16, 11, 59, 55)), fragment="snapshot.as_of")
    assert r.snapshot_at is None
    r = _invalid(request=req(evaluated_at=datetime(2026, 9, 16, 12, 0)), fragment="evaluated_at")
    assert r.evaluated_at is None


def test_25b_invalid_config_rejected():
    with pytest.raises(RiskInputValidationError):
        RiskEvaluationEngine(RiskEvaluationConfig(max_snapshot_age=timedelta(0)))
    r = evaluate_risk(req(), snap(), lim(), RiskEvaluationConfig(max_snapshot_age=timedelta(seconds=-1)))
    assert r.code is C.BLOCK_INVALID_RISK_INPUT


# ---- 26-29 non-authority ---------------------------------------------------------------

def _corpus():
    yield ev()
    yield ev(snapshot=snap(profile_id="p2"))
    yield ev(snapshot=snap(as_of=NOW + timedelta(seconds=1)))
    yield ev(snapshot=snap(account_halted=True))
    yield ev(snapshot=snap(daily_halted=True))
    yield ev(request=req(proposed_risk_usd=D("5000")))
    yield ev(snapshot=snap(open_positions=3))
    yield ev(snapshot=snap(realized_daily_pnl=D("-5000")))
    yield ev(snapshot=snap(balance=D("90000"), equity=D("90000")))
    yield ev(snapshot=snap(open_risk_usd=D("3000")))
    yield ev(request=req(proposed_risk_usd=D("0")))


def test_26_evaluation_never_emits_authorized_reserved():
    assert "AUTHORIZED_RESERVED" not in C.__members__
    assert all("AUTHORIZ" not in m and "RESERV" not in m for m in C.__members__)
    for r in _corpus():
        assert isinstance(r.code, C) and r.code.value != RiskDecisionCode.AUTHORIZED_RESERVED.value
        assert r.IS_AUTHORIZATION is False
    assert RiskEvaluationEngine.GRANTS_RISK_AUTHORITY is False
    base = ev()
    with pytest.raises(Exception):
        dataclasses.replace(base, code=RiskDecisionCode.AUTHORIZED_RESERVED)


DAY3_RISK_FILES = ("contracts.py", "validation.py", "calculator.py", "evaluator.py")


def _day3_sources():
    from api.xtl_platform.tests.test_isolation import PKG_ROOT
    return {name: (PKG_ROOT / "risk" / name).read_text() for name in DAY3_RISK_FILES}


def test_27_no_reservation_object_is_created(monkeypatch):
    from api.xtl_platform.domain import risk as domain_risk

    def boom(*a, **k):
        raise AssertionError("RiskReservation constructed during evaluation")

    monkeypatch.setattr(domain_risk.RiskReservation, "__init__", boom)
    monkeypatch.setattr(domain_risk.RiskAuthorizationResult, "__init__", boom)
    for r in _corpus():
        assert not hasattr(r, "reservation") and not hasattr(r, "authorization_id")
    forbidden_names = {"RiskReservation", "RiskAuthorizationResult", "RiskRequest", "authorize_and_reserve",
                       "mark_sent", "release", "expire_unsent", "RiskAuthority", "ReferenceTestOnlyRiskAuthority"}
    for name, source in _day3_sources().items():
        used = {n.id for n in ast.walk(ast.parse(source)) if isinstance(n, ast.Name)} | \
               {n.attr for n in ast.walk(ast.parse(source)) if isinstance(n, ast.Attribute)} | \
               {a.name for n in ast.walk(ast.parse(source)) if isinstance(n, ast.ImportFrom) for a in n.names}
        assert used.isdisjoint(forbidden_names), (name, used & forbidden_names)


def test_28_no_reference_authority_state_changes(authority):
    pid = "ftmo-100k-01"
    prof = make_profile(pid)
    from api.xtl_platform.domain.risk import RiskRequest
    child = ChildExecutionIntent.from_thesis(make_thesis(), prof, "XAUUSD")
    held = authority.authorize_and_reserve(RiskRequest(child, D("400"), D("1"), "v1"),
                                           make_snapshot(pid), make_limits(pid), NOW).reservation
    before = (authority.active_reservations(pid), authority.status(held.authorization_id))

    s = snap(profile_id=pid, account=prof.identity, open_risk_usd=D("400"))
    request = RiskEvaluationRequest(ChildExecutionIntent.from_thesis(make_thesis(thesis_id="A-2"), prof, "XAUUSD"),
                                    D("500"), None, NOW)
    for _ in range(5):
        assert ENGINE.evaluate(request, s, lim(profile_id=pid)).passed

    assert (authority.active_reservations(pid), authority.status(held.authorization_id)) == before
    for name, source in _day3_sources().items():
        assert "reference_test_only" not in source and "authority" not in source.split('"""', 2)[-1], name


def test_29_no_redis_broker_filesystem_or_clock_dependency():
    from api.xtl_platform.tests.test_isolation import PACKAGE, PRODUCTION_MANIFEST, scan_source
    for name, source in _day3_sources().items():
        assert f"risk/{name}" in PRODUCTION_MANIFEST
        assert scan_source(source, PACKAGE + ("risk",)) == [], name
        tree = ast.parse(source)
        clock_or_env = [n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)
                        and n.attr in {"now", "utcnow", "today", "time", "monotonic", "perf_counter",
                                       "environ", "getenv", "getcontext", "setcontext"}]
        imports = [a.name for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names]
        assert clock_or_env == [] and not {"time", "os", "random", "secrets"} & set(imports), name


# ---- 30-31 frozen Day-1 / Day-2 baseline ------------------------------------------------

# CSA-frozen host baseline (xautrendlab-api, Day-2 approval). test_isolation.py is
# excluded: Day 3 legitimately extends its manifest.
FROZEN_DAY1_DAY2_SHA256 = {
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
    "risk/reference_test_only.py": "3edd3c151fe49ad264fae52986ae91180a71ba46e6adfe9c640c50e293eca9e7",
    "tests/__init__.py": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    "tests/_builders.py": "4bf536e11dac33ef9f5dff0c176e55abcf6f85d96e8ce12a59e7207d6255a51a",
    "tests/conftest.py": "0893751f3d47a26d26e4294c8a348c78393cee79e133f6d5bc1ef2c2e6e6b79f",
    "tests/test_account_identity.py": "2165ec6dcca34a73b96c8555bddfffe4cde7f4be42e7167ed7db2239bb4b61e0",
    "tests/test_domain_contracts.py": "a64712d0114904d8d3aa308bfbbd834cf8286144ac3744381dfee47a961c0ee3",
    "tests/test_policy_engine.py": "791cb3b9c0c314e4773fd3ea69c33bba1c0fcab9f318728a61aaefd2cca34187",
    "tests/test_thesis_fanout.py": "1fb20203b692d95c6f45054b804f051b6e840ab0079ca4cf4c6fe9d607ba4ea3",
}
DAY1_TEST_FILES = ("tests/test_domain_contracts.py", "tests/test_account_identity.py", "tests/test_thesis_fanout.py")
DAY2_TEST_FILES = ("tests/test_policy_engine.py",)


def _sha(rel: str) -> str:
    from api.xtl_platform.tests.test_isolation import PKG_ROOT
    return hashlib.sha256((PKG_ROOT / rel).read_bytes()).hexdigest()


def test_30_day1_tests_and_contracts_unchanged():
    """Day-1 tests are byte-identical to the frozen baseline (and run in this suite)."""
    for rel in DAY1_TEST_FILES:
        assert _sha(rel) == FROZEN_DAY1_DAY2_SHA256[rel], rel


def test_31_day2_tests_unchanged():
    for rel in DAY2_TEST_FILES:
        assert _sha(rel) == FROZEN_DAY1_DAY2_SHA256[rel], rel


def test_31b_no_frozen_production_file_modified():
    changed = [rel for rel, digest in FROZEN_DAY1_DAY2_SHA256.items() if _sha(rel) != digest]
    assert changed == []


# ---- 32 ordering --------------------------------------------------------------------------

def test_32_first_failure_ordering_pinned():
    """Start with every check failing; fix one at a time; the code must follow the documented order."""
    other_account = AccountIdentity("999999", PROFILE.identity.server, PROFILE.identity.company, "USD")
    state = dict(
        request=dict(proposed_risk_pct=D("150")),                            # 1 invalid input
        snapshot=dict(account=other_account,                                  # 2 mismatch
                      as_of=NOW - timedelta(hours=1),                         # 3 stale
                      account_halted=True, daily_halted=True,                 # 4, 5
                      open_positions=3,                                       # 7
                      realized_daily_pnl=D("-4950"),                          # 8
                      balance=D("91000"), equity=D("91000")),                 # 9 (9000 + 1100 > 10000)
        limits=dict(max_risk_per_trade_usd=D("50"),                           # 6
                    max_open_risk_usd=D("100")),                              # 10
    )
    request_kw = dict(proposed_risk_usd=D("1100"))
    fixes = [
        (C.BLOCK_INVALID_RISK_INPUT, lambda: state["request"].pop("proposed_risk_pct")),
        (C.BLOCK_ACCOUNT_MISMATCH, lambda: state["snapshot"].pop("account")),
        (C.STALE_ACCOUNT_STATE, lambda: state["snapshot"].pop("as_of")),
        (C.BLOCK_ACCOUNT_HALT, lambda: state["snapshot"].pop("account_halted")),
        (C.BLOCK_DAILY_HALT, lambda: state["snapshot"].pop("daily_halted")),
        (C.BLOCK_TRADE_RISK, lambda: state["limits"].update(max_risk_per_trade_usd=D("5000"))),
        (C.BLOCK_MAX_POSITIONS, lambda: state["snapshot"].pop("open_positions")),
        (C.BLOCK_DAILY_LIMIT, lambda: state["snapshot"].pop("realized_daily_pnl")),
        (C.BLOCK_MAX_LOSS, lambda: (state["snapshot"].pop("balance"), state["snapshot"].pop("equity"))),
        (C.BLOCK_OPEN_RISK, lambda: state["limits"].update(max_open_risk_usd=D("3000"))),
    ]
    observed = []
    for expected, fix in fixes:
        r = ev(req(**request_kw, **state["request"]), snap(**state["snapshot"]), lim(**state["limits"]))
        observed.append(r.code)
        fix()
    final = ev(req(**request_kw, **state["request"]), snap(**state["snapshot"]), lim(**state["limits"]))
    observed.append(final.code)
    assert observed == [e for e, _ in fixes] + [C.RISK_EVALUATION_PASS]


# ---- 33 concurrency model ------------------------------------------------------------------

def test_33_concurrent_looking_evaluations_do_not_mutate_state():
    """Documented non-authority: both $400 trades PASS against $500 of capacity."""
    s = snap()
    limits = lim(max_open_risk_usd=D("500"))
    a = ENGINE.evaluate(req(thesis_id="A-400", proposed_risk_usd=D("400")), s, limits)
    b = ENGINE.evaluate(req(thesis_id="B-400", proposed_risk_usd=D("400")), s, limits)
    assert a.passed and b.passed            # expected; atomic reservation is future work
    snapshot_copy = copy.deepcopy(s)

    results, lock, barrier = [], threading.Lock(), threading.Barrier(20)

    def worker(i):
        request = req(thesis_id=f"T-{i % 2}", proposed_risk_usd=D("400"))
        barrier.wait()
        r = ENGINE.evaluate(request, s, limits)
        with lock:
            results.append(r)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert all(r.passed for r in results)
    assert len({r for r in results}) == 2    # exactly one distinct result per distinct request
    assert s == snapshot_copy


# ---- 34-35 audit fields and input immutability -------------------------------------------

def test_34_result_contains_audit_and_capacity_fields():
    r = ev(snapshot=snap(realized_daily_pnl=D("-4600")))
    assert r.thesis_id == "A-1" and r.child_trade_id == "A-1:p1" and r.profile_id == "p1"
    assert r.account == PROFILE.identity and r.canonical_symbol == "XAUUSD" and r.side is Side.BUY
    assert r.code is C.BLOCK_DAILY_LIMIT and "DAILY_LOSS" in r.reason
    assert r.evaluated_at == NOW and r.snapshot_at == NOW - timedelta(seconds=5)
    assert r.proposed_risk_usd == D("500") and r.source_policy_version == "TEST-2026.09"
    assert r.relevant_limit == D("5000") and r.used_capacity == D("4600")
    assert r.remaining_capacity_before == D("400") and r.projected_capacity_after == D("-100")
    assert dict(r.decisive_check.components)["current_daily_loss"] == D("4600")

    p = ev()
    assert p.decisive_check is None and p.relevant_limit is None      # no single decisive limit on PASS
    assert all(c.fits for c in p.checks) and p.input_errors == ()

    halted = ev(snapshot=snap(account_halted=True))
    assert halted.checks == () and halted.relevant_limit is None and halted.proposed_risk_usd == D("500")


def test_35_inputs_remain_unchanged_after_evaluation():
    request, s, limits = req(), snap(realized_daily_pnl=D("-100")), lim()
    before = copy.deepcopy((request, s, limits))
    ids = (id(request), id(s), id(limits), id(s.balance))
    for _ in range(3):
        ENGINE.evaluate(request, s, limits)
    assert (request, s, limits) == before
    assert (id(request), id(s), id(limits), id(s.balance)) == ids
    with pytest.raises(dataclasses.FrozenInstanceError):
        s.open_risk_usd = D("0")


# ---- additional -------------------------------------------------------------------------------

def test_caller_decimal_context_cannot_change_results():
    request, s, limits = req(proposed_risk_usd=D("500.123456789")), snap(realized_daily_pnl=D("-1234.987654321")), lim()
    expected = ENGINE.evaluate(request, s, limits)
    with decimal.localcontext() as ctx:
        ctx.prec = 3
        ctx.rounding = decimal.ROUND_UP
        assert ENGINE.evaluate(request, s, limits) == expected


def test_precision_loss_fails_closed():
    huge = "1" + "0" * 40
    s = snap(balance=D(huge), equity=D(huge + ".000000000000000000000000000001"),   # 71 significant digits
             max_loss_reference_balance=D(huge))
    r = ev(snapshot=s, limits=lim(daily_floating_pnl=F.FULL, max_loss_floating_pnl=F.FULL))
    assert r.code is C.BLOCK_INVALID_RISK_INPUT and "decimal arithmetic failed closed" in r.reason


def test_evaluation_contract_rejects_inconsistent_results():
    p = ev()
    blocked = ev(snapshot=snap(open_risk_usd=D("2600")))
    with pytest.raises(Exception):
        dataclasses.replace(p, decisive_check=blocked.decisive_check)                 # PASS with failing check
    with pytest.raises(Exception):
        dataclasses.replace(blocked, code=C.BLOCK_DAILY_LIMIT)                        # wrong capacity code
    with pytest.raises(Exception):
        dataclasses.replace(p, code=C.BLOCK_ACCOUNT_HALT)                             # halt with checks
    with pytest.raises(Exception):
        dataclasses.replace(p, evaluated_at=datetime(2026, 9, 16, 12, 0))             # naive time


def test_wrong_top_level_types_raise():
    with pytest.raises(TypeError):
        ENGINE.evaluate(req(), make_snapshot("p1"), lim())          # Day-1 authority snapshot is not accepted


def test_risk_package_exports_unchanged():
    import api.xtl_platform.risk as risk_pkg
    assert risk_pkg.__all__ == ["RiskAuthority"]
