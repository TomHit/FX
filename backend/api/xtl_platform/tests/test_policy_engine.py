"""Day 2 - versioned Prop Policy Engine.

All FirmPolicy data in this file is TEST FIXTURE - NOT VERIFIED PRODUCTION FIRM RULE.
Numbered tests map 1:1 to the Day-2 brief, section 14.
"""
from __future__ import annotations

import dataclasses
from dataclasses import FrozenInstanceError
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal

import pytest

from api.xtl_platform.domain._validation import ContractError
from api.xtl_platform.domain.enums import (
    LossModel, Permission, Phase, PolicyDecisionCode as P, RiskDecisionCode, Side, StrategyId,
)
from api.xtl_platform.domain.policy import OpenExposure, PolicyContext, PolicyDecision
from api.xtl_platform.domain.thesis import ChildExecutionIntent
from api.xtl_platform.policy import (
    BasePolicyEvaluator, PolicyAmbiguityError, PolicyEvaluator, PolicyIdentityError,
    PolicyRegistrationError, PolicyRegistry, PolicyScope, PolicyValidationError, ResolutionStatus,
    policy_validation_errors, resolve_from,
)
from api.xtl_platform.tests._builders import NOW, TEST_FIXTURE_LABEL, D, make_policy, make_profile, make_thesis

UTC = timezone.utc
JAN1, JUN30, JUL1 = date(2026, 1, 1), date(2026, 6, 30), date(2026, 7, 1)
MAY15 = datetime(2026, 5, 15, 12, 0, tzinfo=UTC)
SEP15 = datetime(2026, 9, 15, 12, 0, tzinfo=UTC)


# ---- helpers --------------------------------------------------------------

def policy(**kw):
    """TEST FIXTURE - NOT VERIFIED PRODUCTION FIRM RULE."""
    base = dict(version="TEST-2026.09", effective_from=date(2026, 9, 1), effective_to=None)
    base.update(kw)
    return make_policy(**base)


def v1_v2():
    return (policy(version="TEST-V1", effective_from=JAN1, effective_to=JUN30),
            policy(version="TEST-V2", effective_from=JUL1, effective_to=None))


def profile(pid="p1", **kw):
    return make_profile(pid, **kw)


def child(prof, symbol="XAUUSD", side=Side.BUY, thesis_id="A-1"):
    if side is Side.BUY:
        t = make_thesis(thesis_id=thesis_id, symbol=symbol, side=side)
    else:
        t = make_thesis(thesis_id=thesis_id, symbol=symbol, side=side, stop=D("2660"), target=D("2620"))
    return ChildExecutionIntent.from_thesis(t, prof, symbol)


def ctx(now=NOW, **kw):
    return PolicyContext(now=now, **kw)


def evaluate(policies, prof=None, now=NOW, symbol="XAUUSD", side=Side.BUY, **ctx_kw):
    prof = prof or profile()
    return PolicyEvaluator(PolicyRegistry(policies)).evaluate(child(prof, symbol, side), prof, ctx(now, **ctx_kw))


def scope_of(prof=None):
    return PolicyScope.of_profile(prof or profile())


# ---- 01-07 version resolution -------------------------------------------

def test_01_exact_scope_resolves_correct_policy():
    target = policy(version="TEST-CFD-CHALLENGE")
    others = [
        policy(version="TEST-OTHER-FIRM", firm_id="OTHERFIRM"),
        policy(version="TEST-FUTURES", product="FUTURES"),
        policy(version="TEST-1STEP", program="1-Step"),
        policy(version="TEST-FUNDED", phase=Phase.FUNDED),
    ]
    r = PolicyRegistry([*others, target]).resolve_scope(scope_of(), NOW)
    assert r.status is ResolutionStatus.RESOLVED and r.policy is target


def test_02_historical_date_resolves_old_version():
    reg = PolicyRegistry(v1_v2())
    assert reg.resolve_scope(scope_of(), MAY15).policy.version == "TEST-V1"
    assert reg.resolve_scope(scope_of(), SEP15).policy.version == "TEST-V2"
    assert evaluate(v1_v2(), now=MAY15).policy_version == "TEST-V1"
    assert evaluate(v1_v2(), now=SEP15).policy_version == "TEST-V2"


def test_03_future_version_not_selected_early():
    reg = PolicyRegistry(v1_v2())
    last_v1_moment = datetime(2026, 6, 30, 23, 59, 59, tzinfo=UTC)
    assert reg.resolve_scope(scope_of(), last_v1_moment).policy.version == "TEST-V1"
    assert reg.resolve_scope(scope_of(), last_v1_moment + timedelta(seconds=1)).policy.version == "TEST-V2"

    only_future = policy(version="TEST-OCT", effective_from=date(2026, 10, 1))
    d = evaluate([only_future], now=SEP15)
    assert d.code is P.BLOCK_POLICY_NOT_EFFECTIVE and d.policy_version is None
    assert "TEST-OCT" in d.reason and "2026-10-01" in d.reason


def test_04_expired_policy_not_eligible():
    expired = policy(version="TEST-OLD", effective_from=JAN1, effective_to=JUN30)
    d = evaluate([expired], now=SEP15)
    assert d.code is P.BLOCK_POLICY_NOT_EFFECTIVE and not d.eligible
    assert "expired" in d.reason

    # Explicit-policy path must also refuse an expired version.
    prof = profile()
    d2 = BasePolicyEvaluator().evaluate(child(prof), prof, expired, ctx(SEP15))
    assert d2.code is P.BLOCK_POLICY_NOT_EFFECTIVE and d2.policy_version == "TEST-OLD"


def test_05_no_policy_blocks():
    d = evaluate([])
    assert d.code is P.BLOCK_NO_POLICY and d.policy_version is None
    prof = profile()
    assert BasePolicyEvaluator().evaluate(child(prof), prof, None, ctx()).code is P.BLOCK_NO_POLICY


def test_06_duplicate_version_registration_fails():
    reg = PolicyRegistry([policy(version="TEST-DUP", effective_from=JAN1, effective_to=JUN30)])
    with pytest.raises(PolicyRegistrationError, match="duplicate"):
        reg.register(policy(version="TEST-DUP", effective_from=JUL1))  # same version, even non-overlapping
    assert len(reg) == 1


def test_07_overlapping_effective_periods_fail_closed():
    a = policy(version="TEST-A", effective_from=JAN1, effective_to=JUN30)
    overlapping = [
        policy(version="TEST-B", effective_from=JUN30),                         # shares boundary day
        policy(version="TEST-C", effective_from=date(2025, 12, 1), effective_to=JAN1),
        policy(version="TEST-D", effective_from=date(2026, 3, 1), effective_to=date(2026, 3, 31)),
    ]
    for b in overlapping:
        reg = PolicyRegistry([a])
        with pytest.raises(PolicyRegistrationError, match="overlaps"):
            reg.register(b)
        assert reg.versions(scope_of()) == (a,)

    # Open-ended then later version also overlaps.
    with pytest.raises(PolicyRegistrationError):
        PolicyRegistry([policy(version="TEST-OPEN", effective_from=JAN1), policy(version="TEST-LATER", effective_from=JUL1)])


def test_07b_overlap_in_batch_leaves_registry_unchanged():
    reg = PolicyRegistry()
    with pytest.raises(PolicyRegistrationError):
        reg.register_all([policy(version="TEST-X", effective_from=JAN1),
                          policy(version="TEST-Y", effective_from=JUL1)])
    assert len(reg) == 0


def test_07c_ambiguous_resolution_never_selects_a_version():
    a = policy(version="TEST-A", effective_from=JAN1)
    b = policy(version="TEST-B", effective_from=JUL1)
    r = resolve_from([a, b], scope_of(), SEP15)
    assert r.status is ResolutionStatus.AMBIGUOUS and r.policy is None

    class LeakyRegistry(PolicyRegistry):
        """Simulates a corrupted store that bypassed registration checks."""
        def resolve_scope(self, scope, at):
            return resolve_from([a, b], scope, at)

    prof = profile()
    d = PolicyEvaluator(LeakyRegistry()).evaluate(child(prof), prof, ctx(SEP15))
    assert d.code is P.BLOCK_POLICY_AMBIGUOUS and d.policy_version is None
    with pytest.raises(PolicyAmbiguityError):
        LeakyRegistry().resolve(prof, ctx(SEP15))


def test_07d_resolution_uses_utc_calendar_date():
    reg = PolicyRegistry(v1_v2())
    # 2026-07-01 01:00 in UTC+3 is still 2026-06-30 in UTC -> v1
    plus3 = timezone(timedelta(hours=3))
    assert reg.resolve_scope(scope_of(), datetime(2026, 7, 1, 1, 0, tzinfo=plus3)).policy.version == "TEST-V1"


# ---- 08-10 scope mismatch -----------------------------------------------

@pytest.mark.parametrize("field,value", [
    ("product", "FUTURES"),          # 08
    ("program", "1-Step"),           # 09
    ("phase", Phase.FUNDED),         # 10
])
def test_08_10_wrong_product_program_phase_blocks(field, value):
    prof = profile(**{field: value})
    # registry path: nothing registered for that exact scope
    assert evaluate([policy()], prof=prof).code is P.BLOCK_NO_POLICY
    # explicit-policy path: supplied policy is for a different scope
    d = BasePolicyEvaluator().evaluate(child(prof), prof, policy(), ctx())
    assert d.code is P.BLOCK_PROGRAM_MISMATCH and d.policy_version == "TEST-2026.09"


# ---- 11-13 automation ----------------------------------------------------

def test_11_automation_prohibited_blocks():
    d = evaluate([policy(automation=Permission.PROHIBITED, automation_verified=False)])
    assert d.code is P.BLOCK_AUTOMATION_PROHIBITED


@pytest.mark.parametrize("automation,verified", [
    (Permission.ALLOWED, False),
    (Permission.CONDITIONAL, False),
    (Permission.CONDITIONAL, True),   # no verified-condition mechanism exists yet
])
def test_12_automation_unverified_blocks(automation, verified):
    d = evaluate([policy(automation=automation, automation_verified=verified)])
    assert d.code is P.BLOCK_AUTOMATION_UNVERIFIED


def test_13_allowed_and_verified_automation_passes():
    d = evaluate([policy(automation=Permission.ALLOWED, automation_verified=True)])
    assert d.code is P.ELIGIBLE


# ---- 14-15 instruments ---------------------------------------------------

def test_14_unsupported_symbol_blocks():
    d = evaluate([policy(allowed_symbols=frozenset({"XAUUSD", "EURUSD"}))], symbol="GBPUSD")
    assert d.code is P.BLOCK_INSTRUMENT and "GBPUSD" in d.reason


def test_15_allowed_symbol_passes():
    d = evaluate([policy(allowed_symbols=frozenset({"XAUUSD", "EURUSD"}))], symbol="EURUSD")
    assert d.eligible


# ---- 16-19 copy policy ---------------------------------------------------

def test_16_same_firm_prohibited_copying_blocks():
    d = evaluate([policy(own_account_copy=Permission.PROHIBITED)], approved_sibling_firm_ids=("FTMO",))
    assert d.code is P.BLOCK_OWN_ACCOUNT_COPY


def test_16b_conditional_own_account_copy_fails_closed():
    d = evaluate([policy(own_account_copy=Permission.CONDITIONAL)], approved_sibling_firm_ids=("FTMO",))
    assert d.code is P.BLOCK_OWN_ACCOUNT_COPY


def test_17_allowed_same_firm_copying_passes():
    d = evaluate([policy(own_account_copy=Permission.ALLOWED)], approved_sibling_firm_ids=("FTMO",))
    assert d.eligible


def test_18_prohibited_cross_firm_copying_blocks():
    d = evaluate([policy(cross_firm_copy=Permission.PROHIBITED)], approved_sibling_firm_ids=("OTHERFIRM",))
    assert d.code is P.BLOCK_CROSS_FIRM_COPY and "OTHERFIRM" in d.reason


def test_19_allowed_cross_firm_copying_passes():
    d = evaluate([policy(cross_firm_copy=Permission.ALLOWED)], approved_sibling_firm_ids=("OTHERFIRM",))
    assert d.eligible


def test_19b_first_child_of_thesis_is_not_a_copy():
    d = evaluate([policy(own_account_copy=Permission.PROHIBITED, cross_firm_copy=Permission.PROHIBITED)])
    assert d.eligible


# ---- 20-21 opposite exposure ---------------------------------------------

def test_20_same_firm_opposite_exposure_blocks_when_prohibited():
    exposure = OpenExposure("p0", "FTMO", "XAUUSD", Side.BUY)
    d = evaluate([policy(allows_opposite_exposure_across_accounts=False)],
                 side=Side.SELL, open_exposures=(exposure,))
    assert d.code is P.BLOCK_OPPOSITE_EXPOSURE


def test_20b_opposite_exposure_allowed_by_policy_passes():
    exposure = OpenExposure("p0", "FTMO", "XAUUSD", Side.BUY)
    d = evaluate([policy(allows_opposite_exposure_across_accounts=True)],
                 side=Side.SELL, open_exposures=(exposure,))
    assert d.eligible


def test_21_other_firm_opposite_exposure_does_not_block():
    exposures = (
        OpenExposure("x1", "OTHERFIRM", "XAUUSD", Side.BUY),   # other firm
        OpenExposure("p0", "FTMO", "EURUSD", Side.BUY),        # same firm, other symbol
        OpenExposure("p2", "FTMO", "XAUUSD", Side.SELL),       # same firm, same side
    )
    d = evaluate([policy(allows_opposite_exposure_across_accounts=False)],
                 side=Side.SELL, open_exposures=exposures)
    assert d.eligible


# ---- 22-24 validation and immutability ------------------------------------

def test_22_invalid_timezone_rejected():
    with pytest.raises(ContractError):
        policy(daily_reset_tz="Mars/Olympus")


def test_23_invalid_effective_period_rejected():
    with pytest.raises(ContractError):
        policy(effective_from=JUL1, effective_to=JUN30)


def _replace_unchecked(p, **changes):
    """Bypass FirmPolicy.__post_init__ to prove registry-level validation."""
    clone = object.__new__(type(p))
    for f in dataclasses.fields(p):
        object.__setattr__(clone, f.name, changes.get(f.name, getattr(p, f.name)))
    return clone


@pytest.mark.parametrize("changes,fragment", [
    (dict(firm_id=" FTMO"), "firm_id"),
    (dict(version=""), "version"),
    (dict(phase="CHALLENGE"), "phase"),
    (dict(automation="ALLOWED"), "automation"),
    (dict(own_account_copy=True), "own_account_copy"),
    (dict(automation_verified=1), "automation_verified"),
    (dict(automation=Permission.PROHIBITED, automation_verified=True), "PROHIBITED"),
    (dict(max_accounts=0), "max_accounts"),
    (dict(max_accounts=True), "max_accounts"),
    (dict(max_accounts=2.0), "max_accounts"),
    (dict(effective_from=datetime(2026, 9, 1, tzinfo=UTC)), "effective_from"),
    (dict(effective_to=date(2026, 8, 1)), "precedes"),
    (dict(daily_reset_tz="Nowhere/Land"), "daily_reset_tz"),
    (dict(daily_reset_time="00:00"), "daily_reset_time"),
    (dict(daily_loss_limit_pct=Decimal("0")), "daily_loss_limit_pct"),
    (dict(max_loss_limit_pct=Decimal("-5")), "max_loss_limit_pct"),
    (dict(max_loss_limit_pct=Decimal("150")), "max_loss_limit_pct"),
    (dict(daily_loss_limit_pct=5.0), "daily_loss_limit_pct"),
    (dict(daily_loss_limit_pct=None), "required"),
    (dict(daily_loss_model=LossModel.NONE), "must be None"),
    (dict(allowed_symbols=frozenset()), "allowed_symbols"),
    (dict(allowed_symbols=frozenset({"xauusd"})), "allowed_symbols"),
    (dict(allowed_symbols=frozenset({"X", "A", "U"})), "allowed_symbols"),
    (dict(allowed_symbols={"XAUUSD"}), "allowed_symbols"),
])
def test_invalid_policy_rejected_at_registration_and_evaluation(changes, fragment):
    bad = _replace_unchecked(policy(), **changes)
    errors = policy_validation_errors(bad)
    assert any(fragment in e for e in errors), errors
    with pytest.raises(PolicyValidationError):
        PolicyRegistry([bad])
    prof = profile()
    d = BasePolicyEvaluator().evaluate(child(prof), prof, bad, ctx())
    assert d.code is P.BLOCK_NO_POLICY and "validation" in d.reason


def test_valid_fixture_policy_has_no_errors():
    assert policy_validation_errors(policy()) == []


def test_24_policy_objects_immutable():
    p = policy()
    with pytest.raises(FrozenInstanceError):
        p.version = "TEST-HACKED"
    with pytest.raises(AttributeError):
        p.allowed_symbols.add("GBPUSD")
    d = evaluate([p])
    with pytest.raises(FrozenInstanceError):
        d.code = P.ELIGIBLE
    r = PolicyRegistry([p]).resolve_scope(scope_of(), NOW)
    with pytest.raises(FrozenInstanceError):
        r.policy = None


# ---- 25-27 decision contract -----------------------------------------------

def test_25_decision_contains_exact_policy_version():
    d = evaluate(v1_v2(), now=SEP15)
    assert d.eligible and d.policy_version == "TEST-V2"
    blocked = evaluate([policy(version="TEST-EXACT")], symbol="GBPUSD")
    assert blocked.code is P.BLOCK_INSTRUMENT and blocked.policy_version == "TEST-EXACT"


def test_26_decision_contains_evaluated_at():
    at = datetime(2026, 9, 20, 8, 30, tzinfo=UTC)
    d = evaluate([policy()], now=at)
    assert d.evaluated_at == at
    assert evaluate([], now=at).evaluated_at == at


def test_26b_decision_contract_rejects_ambiguous_results():
    with pytest.raises(ContractError):
        PolicyDecision("t:p", P.ELIGIBLE, "ok", None, NOW)           # eligible without version
    with pytest.raises(ContractError):
        PolicyDecision("t:p", P.ELIGIBLE, "ok", "v1", datetime(2026, 9, 1))  # naive time
    with pytest.raises(ContractError):
        PolicyDecision("t:p", RiskDecisionCode.BLOCK_POLICY, "x", "v1", NOW)  # risk code reused
    with pytest.raises(ContractError):
        PolicyDecision("t:p", P.BLOCK_INSTRUMENT, "  ", "v1", NOW)       # blank reason


def test_27_repeated_same_input_produces_same_decision():
    policies = [*v1_v2(), policy(version="TEST-OTHER", firm_id="OTHERFIRM")]
    prof = profile()
    c, context = child(prof), ctx(SEP15, approved_sibling_firm_ids=("FTMO",))
    forward = PolicyEvaluator(PolicyRegistry(policies))
    backward = PolicyEvaluator(PolicyRegistry(list(reversed(policies))))
    results = {forward.evaluate(c, prof, context) for _ in range(5)} | {backward.evaluate(c, prof, context)}
    assert len(results) == 1


# ---- 28 isolation -----------------------------------------------------------

def test_28_no_production_dependency_imports():
    from api.xtl_platform.tests.test_isolation import (
        PACKAGE, PKG_ROOT, PRODUCTION_MANIFEST, _production_files, scan_source,
    )
    policy_files = [p for p in _production_files() if p.relative_to(PKG_ROOT).parts[0] == "policy"]
    names = {p.relative_to(PKG_ROOT).as_posix() for p in policy_files}
    assert names == {"policy/__init__.py", "policy/base.py", "policy/registry.py",
                     "policy/evaluator.py", "policy/validation.py"}
    assert names <= PRODUCTION_MANIFEST
    for py in policy_files:
        assert scan_source(py.read_text(), PACKAGE + ("policy",)) == [], py.name


# ---- extension point and identity -------------------------------------------

class _BlocksEURUSD(BasePolicyEvaluator):
    def firm_specific_checks(self, intent, profile, policy, context):
        if intent.canonical_symbol == "EURUSD":
            return self.firm_rule_block(intent, policy, context, "example firm rule")
        return None


class _Raises(BasePolicyEvaluator):
    def firm_specific_checks(self, intent, profile, policy, context):
        raise KeyError("bug")


class _ReturnsEligible(BasePolicyEvaluator):
    def firm_specific_checks(self, intent, profile, policy, context):
        return PolicyDecision(intent.child_trade_id, P.ELIGIBLE, "sneaky", policy.version, context.now)


def _eval_with(rules, symbol="XAUUSD"):
    prof = profile()
    return PolicyEvaluator(PolicyRegistry([policy()]), rules).evaluate(child(prof, symbol), prof, ctx())


def test_firm_specific_hook_can_block():
    assert _eval_with(_BlocksEURUSD(), "EURUSD").code is P.BLOCK_FIRM_RULE
    assert _eval_with(_BlocksEURUSD(), "XAUUSD").eligible


@pytest.mark.parametrize("rules", [_Raises(), _ReturnsEligible()])
def test_broken_firm_hook_fails_closed(rules):
    d = _eval_with(rules)
    assert d.code is P.BLOCK_FIRM_RULE and "failing closed" in d.reason


def test_no_firm_adapters_exist():
    import api.xtl_platform.policy as pkg
    subclasses = [c.__name__ for c in BasePolicyEvaluator.__subclasses__() if c.__module__.startswith(pkg.__name__)]
    assert subclasses == []


def test_identity_mismatch_raises():
    prof = profile("p1")
    other = profile("p2")
    with pytest.raises(PolicyIdentityError):
        PolicyEvaluator(PolicyRegistry([policy()])).evaluate(child(other), prof, ctx())


def test_fixture_label_is_explicit():
    assert TEST_FIXTURE_LABEL == "TEST FIXTURE - NOT VERIFIED PRODUCTION FIRM RULE"
    assert policy().source_url.startswith("test-fixture://")


def test_day1_compat_imports_still_work():
    from api.xtl_platform.policy.base import BasePolicyEvaluator as B, PolicyRegistry as R
    assert B is BasePolicyEvaluator and R is PolicyRegistry
