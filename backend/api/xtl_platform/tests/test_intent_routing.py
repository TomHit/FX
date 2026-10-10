"""Day 10 - canonical strategy intent and deterministic account routing.

REFERENCE ONLY - NON-PRODUCTION. EXECUTION AUTHORITY = FALSE. BROKER AUTHORITY = FALSE.
Pure and offline: no broker, no persistence, no clock, no RNG, no threads.
"""
from __future__ import annotations

import ast
import dataclasses
import hashlib
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from api.xtl_platform.accounts.intent_contracts import (
    INFRA_TEST_ENVIRONMENTS, PLATFORM_NONE, CanonicalStrategyIntent, ExecutionEnvironment as Env,
    IntentIdentityConflict, IntentIdentityIndex, IntentType, RequestedAction, RoutableAccount,
    frozen_strategy_id, intent_errors,
)
from api.xtl_platform.accounts.intent_routing import (
    CandidateVerdict as V, RoutingDecision as RD, RoutingReason as RR, RoutingResult, route_intent,
)
from api.xtl_platform.domain._validation import ContractError
from api.xtl_platform.domain.account import AccountIdentity
from api.xtl_platform.domain.enums import Side, StrategyId, Venue
from api.xtl_platform.tests._builders import NOW, D

STRATEGY_A = "STRATEGY_A"
STRATEGY_C = "STRATEGY_C_TBD"


def intent(**kw) -> CanonicalStrategyIntent:
    base = dict(schema_version=1, intent_id="INT-1", intent_type=IntentType.STRATEGY, strategy_ref=STRATEGY_A,
                environment=Env.DEMO, venue=Venue.MT5_CFD, canonical_symbol="XAUUSD", side=Side.BUY,
                action=RequestedAction.OPEN, effective_at=NOW, thesis_id="A-20260918-XAU",
                entry=D("2650"), invalidation=D("2640"), objective=D("2680"), requested_risk_usd=D("500"),
                evidence_ref="EV-1")
    base.update(kw)
    return CanonicalStrategyIntent(**base)


def infra_intent(**kw) -> CanonicalStrategyIntent:
    base = dict(intent_type=IntentType.INFRA_TEST, strategy_ref=PLATFORM_NONE, test_case_id="TC-7",
                thesis_id=None, intent_id="INFRA-1", environment=Env.SIMULATION)
    base.update(kw)
    return intent(**base)


def acct(profile_id="p1", venue=Venue.MT5_CFD, environment=Env.DEMO, enabled=True,
         permitted=(STRATEGY_A,), infra=False, login=None) -> RoutableAccount:
    return RoutableAccount(profile_id=profile_id,
                           account=AccountIdentity(login or f"login-{profile_id}", "SRV", "FIRM", "USD"),
                           venue=venue, environment=environment, enabled=enabled,
                           permitted_strategy_refs=frozenset(permitted), accepts_infrastructure_tests=infra)


# ---- canonical intent -------------------------------------------------------------------------------

def test_valid_canonical_strategy_intent():
    i = intent()
    assert intent_errors(i) == [] and not i.is_infrastructure_test
    assert i.frozen_strategy_id is StrategyId.STRATEGY_A          # maps onto the frozen enum when it matches
    assert frozen_strategy_id(STRATEGY_C) is None                 # a future strategy needs no frozen change
    assert i.venue is Venue.MT5_CFD and i.side is Side.BUY
    assert (CanonicalStrategyIntent.EXECUTION_AUTHORITY, CanonicalStrategyIntent.BROKER_AUTHORITY) == (False, False)


def test_future_strategy_c_and_neutral_identity_are_representable():
    assert intent(strategy_ref=STRATEGY_C).frozen_strategy_id is None
    assert infra_intent().strategy_ref == PLATFORM_NONE


MALFORMED = {
    "unsupported_schema": dict(schema_version=2),
    "blank_intent_id": dict(intent_id=" "),
    "bad_strategy_ref": dict(strategy_ref="strategy a"),
    "bad_symbol": dict(canonical_symbol="xauusd"),
    "naive_time": dict(effective_at=datetime(2026, 9, 18, 12, 0)),
    "wrong_type_enum": dict(venue="MT5_CFD"),
    "float_risk": dict(requested_risk_usd=500.0),
    "zero_risk": dict(requested_risk_usd=D("0")),
    "both_risk_forms": dict(risk_reference="RISK-1"),
    "no_risk_form": dict(requested_risk_usd=None),
    "missing_levels": dict(entry=None),
    "buy_invalidation_above_entry": dict(invalidation=D("2660")),
    "buy_objective_below_entry": dict(objective=D("2600")),
    "sell_invalidation_below_entry": dict(side=Side.SELL, invalidation=D("2640"), objective=D("2620")),
    "metadata_unsorted": dict(metadata=(("b", "1"), ("a", "2"))),
    "metadata_wrong_shape": dict(metadata=(("a",),)),
    "strategy_with_test_case": dict(test_case_id="TC-1"),
    "strategy_with_neutral_identity": dict(strategy_ref=PLATFORM_NONE),
}


@pytest.mark.parametrize("name", sorted(MALFORMED))
def test_malformed_intent_fails_closed(name):
    with pytest.raises(ContractError):
        intent(**MALFORMED[name])


INFRA_MALFORMED = {
    "infra_without_test_case": dict(test_case_id=None),
    "infra_with_strategy_identity": dict(strategy_ref=STRATEGY_A),
    "infra_with_thesis": dict(thesis_id="A-1"),
    "infra_in_live": dict(environment=Env.LIVE),
}


@pytest.mark.parametrize("name", sorted(INFRA_MALFORMED))
def test_malformed_infrastructure_test_intent_fails_closed(name):
    with pytest.raises(ContractError):
        infra_intent(**INFRA_MALFORMED[name])


def test_intent_identity_is_deterministic_and_conflict_fails_closed():
    first, same = intent(), intent()
    assert first == same and first.fingerprint == same.fingerprint
    index = IntentIdentityIndex().with_intent(first)
    assert index.with_intent(same) is index                          # idempotent re-record
    with pytest.raises(IntentIdentityConflict, match="materially different"):
        index.with_intent(intent(requested_risk_usd=D("600")))
    assert len(index.entries) == 1
    with pytest.raises(dataclasses.FrozenInstanceError):
        first.entry = D("1")


def test_repeated_routing_evaluation_is_identical():
    candidates = (acct("p1"), acct("p2", enabled=False))
    results = [route_intent(intent(), candidates) for _ in range(5)]
    assert all(r == results[0] for r in results)
    assert route_intent(intent(), tuple(reversed(candidates))) == results[0]   # candidate order irrelevant


# ---- domain (CFD / FUTURES) isolation -------------------------------------------------------------------

def test_cfd_intent_routes_only_to_cfd_account():
    result = route_intent(intent(venue=Venue.MT5_CFD), (acct("cfd", venue=Venue.MT5_CFD),
                                                        acct("fut", venue=Venue.FUTURES)))
    assert result.routed and result.selected_profile_id == "cfd"
    assert {a.profile_id: a.verdict for a in result.assessments}["fut"] is V.DOMAIN_MISMATCH


def test_futures_intent_routes_only_to_futures_account():
    result = route_intent(intent(venue=Venue.FUTURES), (acct("cfd", venue=Venue.MT5_CFD),
                                                        acct("fut", venue=Venue.FUTURES)))
    assert result.routed and result.selected_profile_id == "fut"


def test_futures_intent_cannot_reach_a_cfd_only_account():
    result = route_intent(intent(venue=Venue.FUTURES), (acct("cfd", venue=Venue.MT5_CFD),))
    assert (result.decision, result.reason_code) == (RD.REJECT, RR.NO_DOMAIN_MATCH)
    assert result.selected_profile_id is None


# ---- environment isolation ---------------------------------------------------------------------------------

@pytest.mark.parametrize("intent_env,account_env", [
    (Env.DEMO, Env.LIVE), (Env.SIMULATION, Env.LIVE), (Env.LIVE, Env.DEMO), (Env.DEMO, Env.SIMULATION)])
def test_environment_mismatch_fails_closed(intent_env, account_env):
    result = route_intent(intent(environment=intent_env), (acct("p1", environment=account_env),))
    assert (result.decision, result.reason_code) == (RD.REJECT, RR.NO_ENVIRONMENT_MATCH)


def test_infrastructure_test_can_never_reach_a_live_account():
    with pytest.raises(ContractError):                                   # cannot even be expressed
        infra_intent(environment=Env.LIVE)
    live_account = acct("live", environment=Env.LIVE, infra=True)
    result = route_intent(infra_intent(environment=Env.SIMULATION), (live_account,))
    assert result.decision is RD.REJECT and result.selected_profile_id is None
    assert Env.LIVE not in INFRA_TEST_ENVIRONMENTS


# ---- eligibility, explicit selection, ambiguity ----------------------------------------------------------------

def test_disabled_account_cannot_be_selected():
    result = route_intent(intent(), (acct("p1", enabled=False),))
    assert (result.decision, result.reason_code) == (RD.WAIT, RR.ALL_ELIGIBLE_DISABLED)
    assert result.selected_profile_id is None


def test_strategy_permission_is_required():
    result = route_intent(intent(strategy_ref=STRATEGY_C), (acct("p1", permitted=(STRATEGY_A,)),))
    assert (result.decision, result.reason_code) == (RD.REJECT, RR.STRATEGY_NOT_PERMITTED)


def test_explicit_target_selects_deterministically():
    candidates = (acct("p1"), acct("p2"), acct("p3"))
    result = route_intent(intent(target_profile_id="p2"), candidates)
    assert result.routed and result.selected_profile_id == "p2"
    assert result.reason_code is RR.ROUTED_EXPLICIT_TARGET
    assert result.selected_account == acct("p2").account


@pytest.mark.parametrize("target,reason", [("p2", RR.EXPLICIT_TARGET_NOT_ELIGIBLE),
                                           ("nope", RR.EXPLICIT_TARGET_NOT_ELIGIBLE)])
def test_explicit_target_that_is_not_eligible_fails_closed(target, reason):
    candidates = (acct("p1"), acct("p2", enabled=False))
    result = route_intent(intent(target_profile_id=target), candidates)
    assert (result.decision, result.reason_code) == (RD.REJECT, reason)
    assert result.selected_profile_id is None


def test_zero_candidates_fails_closed():
    result = route_intent(intent(), ())
    assert (result.decision, result.reason_code) == (RD.REJECT, RR.NO_CANDIDATES)


def test_ambiguous_multiple_eligible_accounts_fail_closed():
    result = route_intent(intent(), (acct("p1"), acct("p2")))
    assert (result.decision, result.reason_code) == (RD.AMBIGUOUS, RR.AMBIGUOUS_CANDIDATES)
    assert result.selected_profile_id is None and "p1, p2" in result.reason
    assert [a.verdict for a in result.assessments] == [V.ELIGIBLE, V.ELIGIBLE]
    # an explicit discriminator resolves it; nothing is chosen arbitrarily
    assert route_intent(intent(target_profile_id="p2"), (acct("p1"), acct("p2"))).selected_profile_id == "p2"


def test_routing_never_fans_out_to_more_than_one_account():
    result = route_intent(intent(target_profile_id="p1"), (acct("p1"), acct("p2"), acct("p3")))
    assert result.routed and isinstance(result.selected_profile_id, str)
    assert not hasattr(result, "selected_profile_ids") and not hasattr(result, "allocations")
    assert sum(1 for a in result.assessments if a.verdict is V.ELIGIBLE) == 1


def test_account_isolation_between_candidates():
    a, b = acct("p1", login="111"), acct("p2", login="222", permitted=(STRATEGY_C,))
    result = route_intent(intent(), (a, b))
    assert result.selected_profile_id == "p1" and result.selected_account == a.account
    assert result.selected_account != b.account
    other = route_intent(intent(strategy_ref=STRATEGY_C), (a, b))
    assert other.selected_profile_id == "p2" and other.selected_account == b.account


@pytest.mark.parametrize("candidates,reason", [
    ((acct("p1"), acct("p1")), RR.INVALID_CANDIDATES),
    ((acct("p1", login="same"), acct("p2", login="same")), RR.INVALID_CANDIDATES),
    ("not a tuple", RR.INVALID_CANDIDATES),
    ((object(),), RR.INVALID_CANDIDATES),
])
def test_adversarial_candidate_sets_fail_closed(candidates, reason):
    result = route_intent(intent(), candidates)
    assert (result.decision, result.reason_code) == (RD.REJECT, reason)


def test_malformed_routable_account_fails_closed():
    for change in (dict(profile_id=""), dict(venue="MT5_CFD"), dict(environment="DEMO"), dict(enabled="yes"),
                   dict(permitted_strategy_refs=frozenset()), dict(permitted_strategy_refs=frozenset({"bad ref"})),
                   dict(account="acct")):
        with pytest.raises(ContractError):
            dataclasses.replace(acct(), **change)


# ---- infrastructure-test isolation ---------------------------------------------------------------------------

def test_infrastructure_test_intent_is_distinguishable_and_grants_no_authority():
    strategy, infra = intent(), infra_intent()
    assert infra.is_infrastructure_test and not strategy.is_infrastructure_test
    assert infra.intent_type is IntentType.INFRA_TEST and infra.frozen_strategy_id is None
    assert infra.test_case_id == "TC-7" and infra.thesis_id is None

    accounts = (acct("strategy_only", environment=Env.SIMULATION, infra=False),
                acct("infra_ok", environment=Env.SIMULATION, infra=True, permitted=(PLATFORM_NONE,)))
    routed = route_intent(infra, accounts)
    assert routed.routed and routed.selected_profile_id == "infra_ok"
    assert (RoutingResult.EXECUTION_AUTHORITY, RoutingResult.BROKER_AUTHORITY,
            RoutingResult.GRANTS_RISK_AUTHORIZATION) == (False, False, False)


def test_infrastructure_test_cannot_use_a_strategy_account_or_bypass_controls():
    infra = infra_intent()
    result = route_intent(infra, (acct("p1", environment=Env.SIMULATION, infra=False),))
    assert (result.decision, result.reason_code) == (RD.REJECT, RR.INFRA_TEST_NOT_ACCEPTED)
    disabled = route_intent(infra, (acct("p1", environment=Env.SIMULATION, infra=True, enabled=False),))
    assert disabled.decision is RD.WAIT                                   # no special authority to proceed
    assert not any(hasattr(r, "bypass") or hasattr(r, "force") for r in (result, disabled))


def test_strategy_intent_cannot_masquerade_as_infrastructure_test_and_vice_versa():
    with pytest.raises(ContractError):
        intent(intent_type=IntentType.INFRA_TEST)                         # strategy payload, test type
    with pytest.raises(ContractError):
        infra_intent(intent_type=IntentType.STRATEGY)                     # test payload, strategy type


# ---- Day-9 boundary, purity, isolation -------------------------------------------------------------------------

DAY10_FILES = ("intent_contracts.py", "intent_routing.py")
FORBIDDEN_NAMES = {"authorize_and_reserve", "reference_transition", "register_account", "route", "place",
                   "order_send", "lot_size", "contract_size", "tick_value", "margin", "now", "utcnow",
                   "uuid4", "uuid1", "random", "environ", "getenv", "Thread", "Timer", "sleep", "connect",
                   "execute", "save", "load"}


def test_no_strategy_broker_persistence_or_execution_dependency():
    from api.xtl_platform.tests.test_isolation import PACKAGE, PKG_ROOT, PRODUCTION_MANIFEST, scan_source
    for name in DAY10_FILES:
        rel = f"accounts/{name}"
        source = (PKG_ROOT / "accounts" / name).read_text()
        assert rel in PRODUCTION_MANIFEST
        assert scan_source(source, PACKAGE + ("accounts",)) == [], name
        tree = ast.parse(source)
        absolute = {n.module for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.level == 0} | \
                   {a.name for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names}
        assert {m.split(".")[0] for m in absolute} <= {"__future__", "dataclasses", "datetime", "decimal",
                                                       "typing", "re"}, (name, absolute)
        relative = {n.module for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.level}
        assert relative <= {"domain._validation", "domain.account", "domain.enums", "intent_contracts"}, name
        names = {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)} | \
                {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)}
        assert names.isdisjoint(FORBIDDEN_NAMES), (name, names & FORBIDDEN_NAMES)
        assert "REFERENCE ONLY - NON-PRODUCTION" in source
        # Imported modules and referenced symbols only: prose in docstrings is not a dependency.
        symbols = {m.lower() for m in absolute | relative} | {n.lower() for n in names}
        for forbidden in ("oppt_executor", "zone_entry_gate", "trend_sr", "strategy_b_macro", "strategy_b",
                          "strategy_a", "metatrader", "metatrader5", "mt5", "redis", "psycopg", "postgres",
                          "sqlite3", "socket", "urllib", "requests", "httpx"):
            assert not any(forbidden in symbol for symbol in symbols), (name, forbidden)


def test_day10_does_not_touch_the_day9_risk_authority():
    """Routing selects a destination; it never authorizes, reserves or persists."""
    from api.xtl_platform.tests.test_isolation import PKG_ROOT
    for name in DAY10_FILES:
        tree = ast.parse((PKG_ROOT / "accounts" / name).read_text())
        modules = {n.module or "" for n in ast.walk(tree) if isinstance(n, ast.ImportFrom)} | \
                  {a.name for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names}
        symbols = {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)} | \
                  {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)} | \
                  {a.name for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) for a in n.names}
        for forbidden in ("durable_authority", "DurableAccountRiskAuthority", "AtomicAuthorizationRequest",
                          "persistence", "repository", "lifecycle_repository_sqlite", "ReferenceReservation"):
            assert not any(forbidden in m for m in modules), (name, forbidden)
            assert forbidden not in symbols, (name, forbidden)
    result = route_intent(intent(), (acct("p1"),))
    assert result.routed and not hasattr(result, "reservation") and not hasattr(result, "authorization")


FROZEN_DAY9_SHA256 = {
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
    "risk/durable_authority.py": "6084bfb08c821d7e984245493f1a70142aca3bcc67a5fef6dca8570e86e7a874",
    "risk/durable_authority_contracts.py": "5796e0e1cff2b37e08575247a6292d7793e0a76d7c9eb18824222f27a64e5be9",
    "risk/durable_transaction_sqlite.py": "71e5e6b29e41d7dbaf7a2c53d43c7ae09221880a1b14e1e2385914939e025224",
    "risk/evaluator.py": "c2cfcf8cab8877f7528d50338c12b45443c340eb2cd33a91ba56c109e88f8201",
    "risk/lifecycle_contracts.py": "003f4a77f55603bc34f7f6435f38b70490517aa4c6e9fa57298390f7c5146e68",
    "risk/lifecycle_reference.py": "6bce5333310eb26f029283ad15b59659ff6881c49d9aca2bd1002b207863916a",
    "risk/lifecycle_repository_sqlite.py": "33af12679426a92fbcc5594e32270710555b1840cbc583d341322231cf8baf6a",
    "risk/persistence_codec.py": "2a41b9bcd44d0fafd8ba1a596898a04c64b080e3c9e303bc454505a200fc3c8a",
    "risk/persistence_contracts.py": "5b9db23241ae491fa635fcdb79f38ba032f554ddcd91cbbbe970533c4b384120",
    "risk/persistence_repository.py": "f982c1d613fc205144f1624e9221e464d00bdf71eb49f140b71ee8b7e70077e5",
    "risk/persistent_reference.py": "f9a2ee0090a2b2c073492c0072693dc35b250bbf364152738c970da0b0ad8c7b",
    "risk/reconciliation_contracts.py": "7c9b6dbc6831da43e210c712924c248e87897c2d093f39d7e5a243a6af872a67",
    "risk/reconciliation_reference.py": "1b35e7fccd1754bc76921330a6c9cd6558303e61e752a5346b8e19376537516d",
    "risk/reference_test_only.py": "3edd3c151fe49ad264fae52986ae91180a71ba46e6adfe9c640c50e293eca9e7",
    "risk/resolution_contracts.py": "e9955ad8f9aa44638b989bfa5836fd70cf395c1fca81e5a8e05bd3036ec55936",
    "risk/resolution_reference.py": "6d0ecf2953b250bb5114f77db81860f05a1d04acdeabd31f7d7454da3a435a01",
    "risk/validation.py": "ddea557444c5e8513fa93631cafb7a1591fabd2959e4f880825eee7c90425bad",
    "tests/__init__.py": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    "tests/_builders.py": "4bf536e11dac33ef9f5dff0c176e55abcf6f85d96e8ce12a59e7207d6255a51a",
    "tests/conftest.py": "0893751f3d47a26d26e4294c8a348c78393cee79e133f6d5bc1ef2c2e6e6b79f",
    "tests/test_account_identity.py": "2165ec6dcca34a73b96c8555bddfffe4cde7f4be42e7167ed7db2239bb4b61e0",
    "tests/test_atomic_reference.py": "cb07e69fc4aca82137bacf3d95d25ae8a11672186163ee24dbd8e3662e25e57f",
    "tests/test_domain_contracts.py": "a64712d0114904d8d3aa308bfbbd834cf8286144ac3744381dfee47a961c0ee3",
    "tests/test_durable_authority.py": "291af27f4b0862fe59c64ee7eae8fe754da8dae3d3de8ec27f5535ed4cd6ccc2",
    "tests/test_lifecycle_reference.py": "629424f41bccc957908b0014370e1e87c021f1351e37960c0ee21e0c4da3f366",
    "tests/test_lifecycle_state_feed_guard.py": "b72ee921da038006e76dedbf0034e255a2229c0e3bb398ff42c04fc7b37f3ffe",
    "tests/test_persistence_reconciliation.py": "0a6a8928f7d2d197ddc80641d5713df047b03a56eecee47cca6fb6011848a85d",
    "tests/test_policy_engine.py": "791cb3b9c0c314e4773fd3ea69c33bba1c0fcab9f318728a61aaefd2cca34187",
    "tests/test_position_slots.py": "19e3c93b7812d57e9e71e4eb63b87145f98fbffae297a330b71804d29d1057fd",
    "tests/test_resolution_authority.py": "cf66d14cf694fe02051d701673a615f9c0ccc70f7ee10e889ae0d73dcace505b",
    "tests/test_risk_evaluator.py": "e64c800d1344cad6d0469eebf323b61d7963e318ade8cf924921d066e2929d82",
    "tests/test_sqlite_repository.py": "156161f0c703fb01aedb4c18794aa04829d4d2e92cd73b9a312c74f0903ea214",
    "tests/test_thesis_fanout.py": "1fb20203b692d95c6f45054b804f051b6e840ab0079ca4cf4c6fe9d607ba4ea3",
}


def test_frozen_day1_to_day9_files_unchanged():
    from api.xtl_platform.tests.test_isolation import PKG_ROOT
    changed = [rel for rel, digest in FROZEN_DAY9_SHA256.items()
               if hashlib.sha256((PKG_ROOT / rel).read_bytes()).hexdigest() != digest]
    assert changed == [] and len(FROZEN_DAY9_SHA256) == 58


# ---- gap found by mutation testing (O16) ----------------------------------------------------------------

def test_routing_result_contract_rejects_inconsistent_outcomes():
    routed = route_intent(intent(), (acct("p1"),))
    rejected = route_intent(intent(), ())
    for change in (dict(selected_profile_id=None), dict(selected_account=None),
                   dict(decision=RD.AMBIGUOUS), dict(decision=RD.WAIT), dict(decision=RD.REJECT),
                   dict(reason_code=RR.NO_CANDIDATES)):
        with pytest.raises(ContractError):
            dataclasses.replace(routed, **change)                     # ROUTED must keep exactly one selection
    for change in (dict(selected_profile_id="p1"), dict(selected_account=acct("p1").account),
                   dict(decision=RD.ROUTED)):
        with pytest.raises(ContractError):
            dataclasses.replace(rejected, **change)                   # a non-ROUTED result cannot select
    assert routed.routed and not rejected.routed


# ---- CSA corrective patch: decision/reason semantic invariant ----------------------------------------------

from api.xtl_platform.accounts.intent_routing import DECISION_REASONS  # noqa: E402

VALID_COMBINATIONS = [(decision, reason) for decision, reasons in DECISION_REASONS.items() for reason in reasons]
INVALID_COMBINATIONS = [(decision, reason) for decision in RD for reason in RR
                        if reason not in DECISION_REASONS[decision]]


def _result(decision, reason_code, *, selected=True):
    account = acct("p1")
    routed = decision is RD.ROUTED
    return RoutingResult(intent_id="INT-1", decision=decision, reason_code=reason_code,
                         reason="constructed for contract testing",
                         selected_profile_id=account.profile_id if (routed and selected) else None,
                         selected_account=account.account if (routed and selected) else None,
                         assessments=(), errors=())


def test_decision_reason_table_is_complete_and_disjoint():
    assert set(DECISION_REASONS) == set(RD)
    assert set().union(*DECISION_REASONS.values()) == set(RR)
    seen = [r for reasons in DECISION_REASONS.values() for r in reasons]
    assert len(seen) == len(set(seen))                                    # no reason serves two decisions
    assert DECISION_REASONS[RD.WAIT] == frozenset({RR.ALL_ELIGIBLE_DISABLED})
    assert DECISION_REASONS[RD.AMBIGUOUS] == frozenset({RR.AMBIGUOUS_CANDIDATES})
    assert DECISION_REASONS[RD.ROUTED] == frozenset({RR.ROUTED_SINGLE_ELIGIBLE, RR.ROUTED_EXPLICIT_TARGET})


@pytest.mark.parametrize("decision,reason", VALID_COMBINATIONS,
                         ids=[f"{d.value}-{r.value}" for d, r in VALID_COMBINATIONS])
def test_every_valid_decision_reason_combination_is_accepted(decision, reason):
    result = _result(decision, reason)
    assert (result.decision, result.reason_code) == (decision, reason)
    assert result.routed is (decision is RD.ROUTED)


@pytest.mark.parametrize("decision,reason", INVALID_COMBINATIONS,
                         ids=[f"{d.value}-{r.value}" for d, r in INVALID_COMBINATIONS])
def test_every_invalid_decision_reason_combination_raises(decision, reason):
    with pytest.raises(ContractError, match="may only carry"):
        _result(decision, reason)


@pytest.mark.parametrize("decision,reason", [
    (RD.REJECT, RR.ROUTED_SINGLE_ELIGIBLE),          # a rejection dressed as a routing success
    (RD.WAIT, RR.ROUTED_EXPLICIT_TARGET),
    (RD.AMBIGUOUS, RR.ALL_ELIGIBLE_DISABLED),        # ambiguity disguised as a transient wait
    (RD.WAIT, RR.AMBIGUOUS_CANDIDATES),
    (RD.ROUTED, RR.NO_CANDIDATES),
    (RD.ROUTED, RR.EXPLICIT_TARGET_NOT_ELIGIBLE),
])
def test_adversarial_decision_reason_mismatches_fail_closed(decision, reason):
    with pytest.raises(ContractError):
        _result(decision, reason)


def test_routed_still_requires_exactly_one_selection_after_hardening():
    with pytest.raises(ContractError, match="exactly one account"):
        _result(RD.ROUTED, RR.ROUTED_SINGLE_ELIGIBLE, selected=False)
    routed = _result(RD.ROUTED, RR.ROUTED_SINGLE_ELIGIBLE)
    for change in (dict(selected_profile_id=None), dict(selected_account=None)):
        with pytest.raises(ContractError):
            dataclasses.replace(routed, **change)


def test_every_route_intent_outcome_satisfies_the_strengthened_contract():
    """route_intent behaviour is unchanged: every emitted combination is already valid."""
    infra_account = acct("infra", environment=Env.SIMULATION, infra=True, permitted=(PLATFORM_NONE,))
    corpus = [
        (intent(), (acct("p1"),)),                                                  # ROUTED single
        (intent(target_profile_id="p2"), (acct("p1"), acct("p2"))),                 # ROUTED explicit
        (intent(), (acct("p1"), acct("p2"))),                                       # AMBIGUOUS
        (intent(), (acct("p1", enabled=False),)),                                   # WAIT
        (intent(), ()),                                                             # REJECT no candidates
        (intent(venue=Venue.FUTURES), (acct("p1", venue=Venue.MT5_CFD),)),          # REJECT domain
        (intent(environment=Env.SIMULATION), (acct("p1", environment=Env.DEMO),)),  # REJECT environment
        (intent(strategy_ref=STRATEGY_C), (acct("p1"),)),                           # REJECT permission
        (intent(target_profile_id="nope"), (acct("p1"),)),                          # REJECT explicit target
        (infra_intent(), (acct("p1", environment=Env.SIMULATION),)),                # REJECT infra not accepted
        (infra_intent(), (infra_account,)),                                         # ROUTED infra
        (intent(), (acct("p1"), acct("p1"))),                                       # REJECT invalid candidates
    ]
    emitted = set()
    for i, candidates in corpus:
        result = route_intent(i, candidates)
        assert result.reason_code in DECISION_REASONS[result.decision], (result.decision, result.reason_code)
        emitted.add((result.decision, result.reason_code))
    assert {d for d, _ in emitted} == set(RD)                                        # all four decisions exercised
