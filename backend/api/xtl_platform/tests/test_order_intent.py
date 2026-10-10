"""Day 11 - canonical order intent and pre-execution contract.

REFERENCE ONLY - NON-PRODUCTION.
EXECUTION AUTHORITY = FALSE. BROKER AUTHORITY = FALSE.
RISK AUTHORIZATION AUTHORITY = FALSE. PERSISTENCE AUTHORITY = FALSE.
Building an order intent is normalization: nothing is sent, queued, stored or transitioned.
"""
from __future__ import annotations

import ast
import dataclasses
import hashlib
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from api.xtl_platform.accounts.intent_contracts import (
    PLATFORM_NONE, CanonicalStrategyIntent, ExecutionEnvironment as Env, IntentType, RequestedAction,
)
from api.xtl_platform.accounts.intent_routing import RoutingDecision as RD, RoutingReason as RR, route_intent
from api.xtl_platform.domain._validation import ContractError
from api.xtl_platform.domain.account import AccountIdentity
from api.xtl_platform.domain.enums import Side, Venue
from api.xtl_platform.execution.order_intent import (
    AUTHORIZED_OUTCOMES, CanonicalOrderIntent, ExpiryPolicy as EP, OrderIntentContractError, OrderType as OT,
    build_order_intent, canonical_order_json, decode_order_intent, derive_idempotency_key, encode_order_intent,
    order_intent_digest, order_intent_errors,
)
from api.xtl_platform.risk.durable_authority import DurableAccountRiskAuthority
from api.xtl_platform.risk.durable_authority_contracts import (
    DurableAuthorizationOutcome, DurableOutcomeCode as DC, InMemoryTransactionalRepository,
)
from api.xtl_platform.tests._builders import NOW, D
from api.xtl_platform.tests.test_atomic_reference import CONFIG, P1, request as day9_request
from api.xtl_platform.tests.test_durable_authority import account as day9_account
from api.xtl_platform.accounts.intent_contracts import RoutableAccount
from api.xtl_platform.tests.test_atomic_reference import P2
from api.xtl_platform.tests.test_intent_routing import STRATEGY_A, infra_intent, intent

AT = NOW + timedelta(seconds=10)


# ---- upstream evidence (genuine Day-10 routing + genuine Day-9 authorization) ----------------------

#: Day-10 routable accounts must carry the SAME AccountIdentity the Day-9 reservation holds,
#: otherwise the Day-11 contract correctly refuses to normalize (see the mismatch tests).
PROFILES = {"p1": P1, "p2": P2}


def acct(profile_id="p1", venue=Venue.MT5_CFD, environment=Env.DEMO, enabled=True,
         permitted=(STRATEGY_A, PLATFORM_NONE), infra=False) -> RoutableAccount:
    return RoutableAccount(profile_id=profile_id, account=PROFILES[profile_id].identity, venue=venue,
                           environment=environment, enabled=enabled,
                           permitted_strategy_refs=frozenset(permitted), accepts_infrastructure_tests=infra)


def routed(strategy_intent=None, candidates=None):
    strategy_intent = strategy_intent or intent()
    result = route_intent(strategy_intent, candidates or (acct("p1"),))
    assert result.routed, result.reason
    return strategy_intent, result


def authorized(profile_id="p1", risk="500", thesis_id="A-1", side=Side.BUY, symbol=None):
    profile = PROFILES[profile_id]
    repository = InMemoryTransactionalRepository()
    authority = DurableAccountRiskAuthority(CONFIG, repository, acknowledge_reference_only_non_production=True)
    authority.register_account(day9_account(profile_id, profile, capacity="5000"))
    outcome = authority.authorize_and_reserve(
        day9_request(risk=risk, thesis_id=thesis_id, profile=profile, side=side), at=NOW)
    assert outcome.code is DC.PERSISTED, outcome.reason
    return authority, outcome


def build(order_type=OT.MARKET, expiry=EP.IMMEDIATE, **kw):
    strategy_intent, routing = routed(kw.pop("strategy_intent", None), kw.pop("candidates", None))
    authorization = kw.pop("authorization", None)
    if authorization is None:                                    # evidence matched to this intent
        authorization = authorized(profile_id=routing.selected_profile_id, side=strategy_intent.side)[1]
    params = dict(order_type=order_type, quantity=D("0.25"), created_at=AT, expiry_policy=expiry)
    params.update(kw)
    return build_order_intent(strategy_intent, routing, authorization, **params)


# ---- valid constructions -------------------------------------------------------------------------------

@pytest.mark.parametrize("side", [Side.BUY, Side.SELL])
def test_valid_market_order_intent(side):
    strategy_intent = intent(side=side, entry=D("2650"),
                             invalidation=D("2640") if side is Side.BUY else D("2660"),
                             objective=D("2680") if side is Side.BUY else D("2620"))
    order = build(strategy_intent=strategy_intent)
    assert order.order_type is OT.MARKET and order.side is side and order.requested_price is None
    assert order.expiry_policy is EP.IMMEDIATE and order.expires_at is None
    assert order.quantity == D("0.25") and order.stop_loss == strategy_intent.invalidation
    assert order.routed_profile_id == "p1" and order.routed_account == acct("p1").account
    assert order.risk_reservation_id == "REF-RESV:A-1:p1" and order.risk_authorization_revision == 2
    assert order.reserved_risk_usd == D("500") and order.created_at == AT
    assert order_intent_errors(order) == []
    assert (order.EXECUTION_AUTHORITY, order.BROKER_AUTHORITY, order.RISK_AUTHORIZATION_AUTHORITY,
            order.PERSISTENCE_AUTHORITY, order.IS_BROKER_ORDER) == (False, False, False, False, False)


@pytest.mark.parametrize("order_type,price", [(OT.LIMIT, D("2645")), (OT.STOP, D("2655"))])
@pytest.mark.parametrize("expiry,expires_at", [(EP.GOOD_TILL_CANCELLED, None),
                                               (EP.GOOD_TILL_TIME, AT + timedelta(hours=4))])
def test_valid_limit_and_stop_order_intents(order_type, price, expiry, expires_at):
    order = build(order_type=order_type, expiry=expiry, requested_price=price, expires_at=expires_at)
    assert (order.order_type, order.requested_price, order.expiry_policy, order.expires_at) == \
           (order_type, price, expiry, expires_at)
    assert order.stop_loss < order.requested_price                       # BUY structural consistency


def test_take_profit_defaults_to_the_strategy_objective_and_can_be_overridden():
    assert build().take_profit == intent().objective
    assert build(take_profit=D("2700")).take_profit == D("2700")
    assert build(strategy_intent=intent(objective=None)).take_profit is None


def test_infrastructure_test_intent_can_be_normalized_in_a_test_environment():
    infra = infra_intent(environment=Env.SIMULATION)
    candidates = (acct("p1", environment=Env.SIMULATION, infra=True, permitted=(PLATFORM_NONE,)),)
    order = build(strategy_intent=infra, candidates=candidates)
    assert order.strategy_ref == PLATFORM_NONE and order.environment is Env.SIMULATION
    assert order.thesis_id is None and order.IS_BROKER_ORDER is False


# ---- fail closed: quantity, prices, evidence, identity -----------------------------------------------------

@pytest.mark.parametrize("quantity", [D("0"), D("-1"), Decimal("NaN"), Decimal("Infinity"), Decimal("-Infinity"),
                                      0.25, "0.25", None])
def test_invalid_quantity_fails_closed(quantity):
    with pytest.raises(OrderIntentContractError):
        build(quantity=quantity)


@pytest.mark.parametrize("field,value", [
    ("requested_price", Decimal("NaN")), ("requested_price", Decimal("Infinity")), ("requested_price", D("0")),
    ("requested_price", D("-5")), ("take_profit", Decimal("NaN")), ("take_profit", D("0")),
])
def test_invalid_prices_fail_closed(field, value):
    with pytest.raises(OrderIntentContractError):
        build(order_type=OT.LIMIT, expiry=EP.GOOD_TILL_CANCELLED,
              **{"requested_price": D("2645"), field: value})


@pytest.mark.parametrize("stop", [Decimal("NaN"), Decimal("Infinity"), D("0"), D("-1")])
def test_invalid_stop_loss_from_intent_fails_closed(stop):
    with pytest.raises(ContractError):
        build(strategy_intent=intent(invalidation=stop))


def _genuine_refusals():
    """Real Day-9 outcomes that are NOT authorization evidence (never fabricated)."""
    repository = InMemoryTransactionalRepository()
    authority = DurableAccountRiskAuthority(CONFIG, repository, acknowledge_reference_only_non_production=True)
    authority.register_account(day9_account("p1", P1, capacity="100"))
    blocked = authority.authorize_and_reserve(day9_request(risk="500", thesis_id="TOO-BIG", profile=P1), at=NOW)
    unknown = authority.authorize_and_reserve(day9_request(risk="10", thesis_id="X", profile=P2), at=NOW)
    assert blocked.code is DC.BLOCKED and unknown.code is DC.ACCOUNT_NOT_REGISTERED
    return blocked, unknown


@pytest.mark.parametrize("index", [0, 1])
def test_missing_authorization_reference_fails_closed(index):
    strategy_intent, routing = routed()
    outcome = _genuine_refusals()[index]
    assert outcome.reservation is None
    with pytest.raises(OrderIntentContractError, match="no upstream risk authorization"):
        build_order_intent(strategy_intent, routing, outcome, order_type=OT.MARKET, quantity=D("0.25"),
                           created_at=AT, expiry_policy=EP.IMMEDIATE)


def test_idempotent_replay_authorization_is_accepted_evidence():
    authority, first = authorized()
    replay = authority.authorize_and_reserve(day9_request(risk="500", thesis_id="A-1", profile=P1), at=NOW)
    assert replay.code is DC.IDEMPOTENT_REPLAY and DC.IDEMPOTENT_REPLAY in AUTHORIZED_OUTCOMES
    strategy_intent, routing = routed()
    order = build_order_intent(strategy_intent, routing, replay, order_type=OT.MARKET, quantity=D("0.25"),
                               created_at=AT, expiry_policy=EP.IMMEDIATE)
    assert order.risk_reservation_id == first.reservation.reservation_id


@pytest.mark.parametrize("decision_case", ["ambiguous", "wait", "reject"])
def test_missing_routing_or_account_identity_fails_closed(decision_case):
    strategy_intent = intent()
    candidates = {"ambiguous": (acct("p1"), acct("p2")),
                  "wait": (acct("p1", enabled=False),),
                  "reject": ()}[decision_case]
    routing = route_intent(strategy_intent, candidates)
    assert not routing.routed
    _, authorization = authorized()
    with pytest.raises(OrderIntentContractError, match="routing did not select an account"):
        build_order_intent(strategy_intent, routing, authorization, order_type=OT.MARKET, quantity=D("0.25"),
                           created_at=AT, expiry_policy=EP.IMMEDIATE)


def test_mismatched_routed_account_fails_closed():
    strategy_intent, routing = routed(candidates=(acct("p2"),))                            # routed to p2
    _, authorization = authorized(profile_id="p1")                                         # reserved on p1
    with pytest.raises(OrderIntentContractError, match="reservation profile"):
        build_order_intent(strategy_intent, routing, authorization, order_type=OT.MARKET, quantity=D("0.25"),
                           created_at=AT, expiry_policy=EP.IMMEDIATE)


def test_routing_result_for_another_intent_fails_closed():
    other_intent, routing = routed(intent(intent_id="OTHER"))
    _, authorization = authorized()
    with pytest.raises(OrderIntentContractError, match="different canonical intent"):
        build_order_intent(intent(), routing, authorization, order_type=OT.MARKET, quantity=D("0.25"),
                           created_at=AT, expiry_policy=EP.IMMEDIATE)


def test_symbol_or_side_mismatch_between_intent_and_reservation_fails_closed():
    _, authorization = authorized()                                       # XAUUSD BUY reservation
    for strategy_intent in (intent(canonical_symbol="EURUSD"),
                            intent(side=Side.SELL, invalidation=D("2660"), objective=D("2620"))):
        # authorization above is deliberately XAUUSD/BUY
        routing = route_intent(strategy_intent, (acct("p1"),))
        with pytest.raises(OrderIntentContractError, match="reservation (symbol|side)"):
            build_order_intent(strategy_intent, routing, authorization, order_type=OT.MARKET, quantity=D("0.25"),
                               created_at=AT, expiry_policy=EP.IMMEDIATE)


# ---- order type / price / expiry combinations ------------------------------------------------------------------

@pytest.mark.parametrize("order_type,price,expiry,expires_at,marker", [
    (OT.LIMIT, None, EP.GOOD_TILL_CANCELLED, None, "requires requested_price"),
    (OT.STOP, None, EP.GOOD_TILL_CANCELLED, None, "requires requested_price"),
    (OT.MARKET, D("2650"), EP.IMMEDIATE, None, "must not carry requested_price"),
    (OT.MARKET, None, EP.GOOD_TILL_CANCELLED, None, "MARKET requires IMMEDIATE"),
    (OT.MARKET, None, EP.GOOD_TILL_TIME, AT + timedelta(hours=1), "MARKET requires IMMEDIATE"),
    (OT.LIMIT, D("2645"), EP.IMMEDIATE, None, "cannot use IMMEDIATE"),
    (OT.LIMIT, D("2645"), EP.GOOD_TILL_TIME, None, "requires a timezone-aware expires_at"),
    (OT.LIMIT, D("2645"), EP.GOOD_TILL_TIME, AT, "expires_at must be after created_at"),
    (OT.LIMIT, D("2645"), EP.GOOD_TILL_TIME, AT - timedelta(minutes=1), "expires_at must be after created_at"),
    (OT.LIMIT, D("2645"), EP.GOOD_TILL_CANCELLED, AT + timedelta(hours=1), "must not carry expires_at"),
    (OT.LIMIT, D("2645"), EP.GOOD_TILL_TIME, datetime(2026, 9, 19, 12, 0), "timezone-aware"),
])
def test_invalid_order_type_price_and_expiry_combinations_fail_closed(order_type, price, expiry, expires_at, marker):
    with pytest.raises(OrderIntentContractError, match=marker):
        build(order_type=order_type, expiry=expiry, requested_price=price, expires_at=expires_at)


@pytest.mark.parametrize("side,invalidation,objective,price,take,marker", [
    # The strategy intent itself is valid; the ORDER's requested_price puts the levels on the wrong side.
    (Side.BUY, D("2640"), None, D("2635"), None, "BUY stop_loss must be below requested_price"),
    (Side.SELL, D("2660"), None, D("2665"), None, "SELL stop_loss must be above requested_price"),
    (Side.BUY, D("2640"), None, D("2645"), D("2642"), "BUY take_profit must be above requested_price"),
    (Side.SELL, D("2660"), None, D("2655"), D("2658"), "SELL take_profit must be below requested_price"),
])
def test_direction_inconsistent_protective_levels_fail_closed(side, invalidation, objective, price, take, marker):
    strategy_intent = intent(side=side, entry=D("2650"), invalidation=invalidation, objective=objective)
    _, authorization = authorized(side=side)
    routing = route_intent(strategy_intent, (acct("p1"),))
    with pytest.raises(OrderIntentContractError, match=marker):
        build_order_intent(strategy_intent, routing, authorization, order_type=OT.LIMIT, quantity=D("0.25"),
                           created_at=AT, expiry_policy=EP.GOOD_TILL_CANCELLED, requested_price=price,
                           take_profit=take)


@pytest.mark.parametrize("created_at", [datetime(2026, 9, 19, 12, 0), None, "now"])
def test_invalid_created_at_fails_closed(created_at):
    with pytest.raises(OrderIntentContractError):
        build(created_at=created_at)


# ---- deterministic idempotency -----------------------------------------------------------------------------------

def test_identical_authorized_intent_rebuilds_identically():
    first, second = build(), build()
    assert first == second and first.idempotency_key == second.idempotency_key
    assert canonical_order_json(first) == canonical_order_json(second)


def test_replay_at_a_later_time_is_the_same_logical_order():
    early, late = build(created_at=AT), build(created_at=AT + timedelta(hours=3))
    assert early.idempotency_key == late.idempotency_key           # created_at is excluded by design
    assert early != late and order_intent_digest(early) != order_intent_digest(late)


MATERIAL_DIFFERENCES = {
    "quantity": dict(quantity=D("0.5")),
    "order_type": dict(order_type=OT.LIMIT, expiry=EP.GOOD_TILL_CANCELLED, requested_price=D("2645")),
    "take_profit": dict(take_profit=D("2690")),
    "expiry": dict(order_type=OT.LIMIT, expiry=EP.GOOD_TILL_TIME, requested_price=D("2645"),
                   expires_at=AT + timedelta(hours=2)),
}


@pytest.mark.parametrize("name", sorted(MATERIAL_DIFFERENCES))
def test_materially_different_intent_produces_a_different_identity(name):
    assert build().idempotency_key != build(**MATERIAL_DIFFERENCES[name]).idempotency_key


def test_different_intent_strategy_account_or_reservation_changes_identity():
    base = build()
    other_intent = build(strategy_intent=intent(intent_id="INT-2"))
    other_account = build(strategy_intent=intent(target_profile_id="p2"), candidates=(acct("p1"), acct("p2")))
    authority, _ = authorized()
    second_reservation = authority.authorize_and_reserve(day9_request(risk="300", thesis_id="A-2", profile=P1), at=NOW)
    strategy_intent, routing = routed()
    other_reservation = build_order_intent(strategy_intent, routing, second_reservation, order_type=OT.MARKET,
                                           quantity=D("0.25"), created_at=AT, expiry_policy=EP.IMMEDIATE)
    keys = {base.idempotency_key, other_intent.idempotency_key, other_account.idempotency_key,
            other_reservation.idempotency_key}
    assert len(keys) == 4


def test_idempotency_key_is_derived_not_generated():
    order = build()
    assert order.idempotency_key == derive_idempotency_key(order.identity_payload)
    assert "created_at" not in order.identity_payload
    with pytest.raises(OrderIntentContractError, match="idempotency_key does not match"):
        dataclasses.replace(order, idempotency_key="0" * 64)
    assert len(order.idempotency_key) == 64 and int(order.idempotency_key, 16) >= 0


# ---- immutability and serialization -----------------------------------------------------------------------------

def test_order_intent_is_immutable_and_mutation_protected():
    order = build()
    for field in ("quantity", "stop_loss", "routed_profile_id", "risk_reservation_id", "idempotency_key"):
        with pytest.raises(dataclasses.FrozenInstanceError):
            setattr(order, field, None)
    for change in (dict(quantity=D("0.5")), dict(routed_profile_id="p2"), dict(risk_reservation_id="OTHER")):
        with pytest.raises(OrderIntentContractError):        # identity no longer matches the key
            dataclasses.replace(order, **change)


@pytest.mark.parametrize("order_type,expiry,price,expires_at", [
    (OT.MARKET, EP.IMMEDIATE, None, None),
    (OT.LIMIT, EP.GOOD_TILL_CANCELLED, D("2645"), None),
    (OT.STOP, EP.GOOD_TILL_TIME, D("2655"), AT + timedelta(hours=6)),
])
def test_serialization_round_trip(order_type, expiry, price, expires_at):
    order = build(order_type=order_type, expiry=expiry, requested_price=price, expires_at=expires_at)
    payload = encode_order_intent(order)
    restored = decode_order_intent(payload)
    assert restored == order and restored.idempotency_key == order.idempotency_key
    assert canonical_order_json(restored) == canonical_order_json(order)


@pytest.mark.parametrize("corrupt", [
    lambda p: p.update(quantity="0"),
    lambda p: p.update(stop_loss="not-a-number"),
    lambda p: p.pop("risk_reservation_id"),
    lambda p: p.update(side="LONG"),
    lambda p: p.update(order_type="ICEBERG"),
    lambda p: p.update(idempotency_key="0" * 64),
    lambda p: p.update(created_at="2026-09-19T12:00:00"),
    lambda p: p.update(routed_account={"login": "x"}),
])
def test_malformed_serialized_payload_fails_closed(corrupt):
    payload = encode_order_intent(build())
    corrupt(payload)
    with pytest.raises(OrderIntentContractError):
        decode_order_intent(payload)


def test_decode_rejects_non_objects():
    for payload in (None, [], "payload", 7):
        with pytest.raises(OrderIntentContractError):
            decode_order_intent(payload)


# ---- no execution / broker / risk / persistence authority ---------------------------------------------------------

def test_building_an_order_intent_changes_no_upstream_state():
    authority, authorization = authorized()
    before_state = authority.account_state("p1")
    before_revision = authority.durable_revision("p1")
    strategy_intent, routing = routed()
    order = build_order_intent(strategy_intent, routing, authorization, order_type=OT.MARKET, quantity=D("0.25"),
                               created_at=AT, expiry_policy=EP.IMMEDIATE)
    assert authority.account_state("p1") == before_state                 # risk state untouched
    assert authority.durable_revision("p1") == before_revision           # nothing persisted
    assert order.risk_reservation_id == authorization.reservation.reservation_id
    assert not hasattr(order, "ticket") and not hasattr(order, "broker_order_id")
    assert not hasattr(order, "status") and not hasattr(order, "lifecycle_state")


def test_order_intent_is_not_a_broker_order_or_lifecycle_transition():
    order = build()
    forbidden_fields = {"ticket", "magic", "magic_number", "deviation", "filling_mode", "broker_symbol",
                        "lot", "lots", "contract_size", "tick_value", "margin", "broker_order_id",
                        "order_pending", "position_id", "fill", "acknowledgement"}
    assert {f.name for f in dataclasses.fields(order)}.isdisjoint(forbidden_fields)
    from api.xtl_platform.domain.enums import LifecycleState
    assert not any(isinstance(getattr(order, f.name, None), LifecycleState) for f in dataclasses.fields(order))


DAY11_FILES = ("order_intent.py",)
FORBIDDEN_SYMBOLS = {"authorize_and_reserve", "apply_transition", "register_account", "reference_transition",
                     "save", "load", "commit", "connect", "send", "submit", "order_send", "place", "cancel",
                     "now", "utcnow", "uuid4", "uuid1", "random", "environ", "getenv", "Thread", "Timer",
                     "sleep", "Popen", "run"}


def test_day11_has_no_broker_strategy_persistence_or_execution_dependency():
    from api.xtl_platform.tests.test_isolation import PACKAGE, PKG_ROOT, PRODUCTION_MANIFEST, scan_source
    for name in DAY11_FILES:
        rel = f"execution/{name}"
        source = (PKG_ROOT / "execution" / name).read_text()
        assert rel in PRODUCTION_MANIFEST
        assert scan_source(source, PACKAGE + ("execution",)) == [], name
        tree = ast.parse(source)
        absolute = {n.module for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.level == 0} | \
                   {a.name for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names}
        assert {m.split(".")[0] for m in absolute} <= {"__future__", "dataclasses", "datetime", "decimal",
                                                       "typing"}, (name, absolute)
        relative = {n.module for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.level}
        assert relative <= {"accounts.intent_contracts", "accounts.intent_routing", "domain._validation",
                            "domain.account", "domain.enums", "risk.durable_authority_contracts",
                            "risk.persistence_codec"}, (name, relative)
        symbols = {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)} | \
                  {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)}
        assert symbols.isdisjoint(FORBIDDEN_SYMBOLS), (name, symbols & FORBIDDEN_SYMBOLS)
        for forbidden in ("oppt_executor", "zone_entry_gate", "trend_sr", "strategy_b_macro", "strategy_a",
                          "strategy_b", "metatrader", "mt5", "redis", "sqlite3", "psycopg", "socket", "urllib",
                          "requests", "subprocess"):
            assert not any(forbidden in s.lower() for s in symbols | absolute | relative), (name, forbidden)
    assert (CanonicalOrderIntent.EXECUTION_AUTHORITY, CanonicalOrderIntent.BROKER_AUTHORITY,
            CanonicalOrderIntent.RISK_AUTHORIZATION_AUTHORITY, CanonicalOrderIntent.PERSISTENCE_AUTHORITY,
            CanonicalOrderIntent.IS_BROKER_ORDER) == (False, False, False, False, False)


FROZEN_DAY10_SHA256 = {
    "__init__.py": "02d6d351df2fe8a60273633192ada8dc1ed482affa734cea8280b7e2878fe8a7",
    "accounts/__init__.py": "6c76451e207cd2b955c7ca4de5faea9c3eab967a8ef9f09599eb83a30b610985",
    "accounts/intent_contracts.py": "03f4179ef64d2222485d00fdb32bfbc85f23e7f3c61c6baf566d37f73d30f63d",
    "accounts/intent_routing.py": "96cb66d6ba59a448309abc40b0cd87cf3dfef630ca58f65633d6433f0b9e2e58",
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
    "tests/test_intent_routing.py": "5b22cdd9384803e4ed9246183361717989161bacf0736dc437d1a2ee590aca84",
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


def test_frozen_day1_to_day10_files_unchanged():
    from api.xtl_platform.tests.test_isolation import PKG_ROOT
    changed = [rel for rel, digest in FROZEN_DAY10_SHA256.items()
               if hashlib.sha256((PKG_ROOT / rel).read_bytes()).hexdigest() != digest]
    assert changed == [] and len(FROZEN_DAY10_SHA256) == 61


# ---- gaps found by mutation testing (Q4, Q14) ---------------------------------------------------------------

def test_same_profile_id_but_different_account_identity_fails_closed():
    """Profile ids agree, the account identities do not: the order must not be normalized."""
    impostor = RoutableAccount(profile_id="p1",
                               account=AccountIdentity("999999", P1.identity.server, P1.identity.company, "USD"),
                               venue=Venue.MT5_CFD, environment=Env.DEMO, enabled=True,
                               permitted_strategy_refs=frozenset((STRATEGY_A,)))
    strategy_intent, routing = routed(candidates=(impostor,))
    _, authorization = authorized(profile_id="p1")
    assert routing.selected_profile_id == authorization.reservation.profile_id == "p1"
    assert routing.selected_account != authorization.reservation.account
    with pytest.raises(OrderIntentContractError, match="account identity"):
        build_order_intent(strategy_intent, routing, authorization, order_type=OT.MARKET, quantity=D("0.25"),
                           created_at=AT, expiry_policy=EP.IMMEDIATE)


def _forged_live_infra_intent() -> CanonicalStrategyIntent:
    """Bypass the Day-10 constructor guard to reach the Day-11 defence-in-depth check."""
    template = infra_intent(environment=Env.SIMULATION)
    forged = object.__new__(CanonicalStrategyIntent)
    for f in dataclasses.fields(template):
        object.__setattr__(forged, f.name, Env.LIVE if f.name == "environment" else getattr(template, f.name))
    return forged


def test_infrastructure_test_targeting_live_is_refused_even_if_upstream_guards_are_bypassed():
    forged = _forged_live_infra_intent()
    assert forged.intent_type is IntentType.INFRA_TEST and forged.environment is Env.LIVE
    live_account = RoutableAccount(profile_id="p1", account=P1.identity, venue=Venue.MT5_CFD,
                                   environment=Env.LIVE, enabled=True,
                                   permitted_strategy_refs=frozenset((PLATFORM_NONE,)),
                                   accepts_infrastructure_tests=True)
    from api.xtl_platform.accounts.intent_routing import RoutingResult
    routing = RoutingResult(intent_id=forged.intent_id, decision=RD.ROUTED,
                            reason_code=RR.ROUTED_SINGLE_ELIGIBLE, reason="forged for defence-in-depth testing",
                            selected_profile_id="p1", selected_account=live_account.account, assessments=())
    _, authorization = authorized(thesis_id=forged.thesis_id or "INFRA-1")
    with pytest.raises(OrderIntentContractError, match="never target LIVE"):
        build_order_intent(forged, routing, authorization, order_type=OT.MARKET, quantity=D("0.25"),
                           created_at=AT, expiry_policy=EP.IMMEDIATE)
