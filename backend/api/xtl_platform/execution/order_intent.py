"""Day-11 canonical order intent - REFERENCE ONLY - NON-PRODUCTION.

    EXECUTION AUTHORITY = FALSE. BROKER AUTHORITY = FALSE.
    RISK AUTHORIZATION AUTHORITY = FALSE. PERSISTENCE AUTHORITY = FALSE.

The contract that sits AFTER Day-10 routing and Day-9 risk authorization and BEFORE any
future execution adapter. It answers one question: given an intent already routed and
already authorized, what exact immutable order request would an adapter be allowed to
receive? Building one is normalization, not execution: no broker order exists, nothing
is pending, no lifecycle transition occurs, nothing is sent, queued or persisted.

Broker neutral: it carries no broker-specific identifiers, no venue routing parameters,
no instrument conversion and no symbol mapping. Every such field belongs to a later adapter,
and a test asserts none of them appear on this contract.

No trading policy: Day 11 invents no stop distance, R:R, maximum quantity, symbol buffer,
news rule or entry rule. Those belong to strategy, risk and policy layers.

Upstream authority is consumed, never recreated:
  * Day-10 `RoutingResult` supplies the account. Day 11 never selects or switches accounts.
  * Day-9 `DurableAuthorizationOutcome` supplies the reservation. Day 11 never calculates,
    approves, reserves, releases or alters risk, and an intent without a persisted (or
    replayed) reservation fails closed.
  * `quantity` is the already-authorized quantity supplied by the caller. Day 11 validates
    it but NEVER derives it: converting reserved risk into quantity needs instrument data
    (contract size, tick value, symbol mapping) that belongs to the adapter layer.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, ClassVar, Optional

from ..accounts.intent_contracts import CanonicalStrategyIntent, ExecutionEnvironment, IntentType
from ..accounts.intent_routing import RoutingResult
from ..domain._validation import ContractError
from ..domain.account import AccountIdentity
from ..domain.enums import Side, StrEnum
from ..risk.durable_authority_contracts import DurableAuthorizationOutcome, DurableOutcomeCode
from ..risk.persistence_codec import canonical_json, content_digest

ORDER_INTENT_SCHEMA_VERSION = 1
SUPPORTED_ORDER_INTENT_VERSIONS = frozenset({ORDER_INTENT_SCHEMA_VERSION})

#: Day-9 outcomes that count as valid upstream authorization evidence.
AUTHORIZED_OUTCOMES = frozenset({DurableOutcomeCode.PERSISTED, DurableOutcomeCode.IDEMPOTENT_REPLAY})


class OrderType(StrEnum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    STOP = "STOP"


class ExpiryPolicy(StrEnum):
    IMMEDIATE = "IMMEDIATE"              # MARKET only
    GOOD_TILL_CANCELLED = "GOOD_TILL_CANCELLED"
    GOOD_TILL_TIME = "GOOD_TILL_TIME"    # requires expires_at


#: Order types that require a requested price.
PRICED_ORDER_TYPES = frozenset({OrderType.LIMIT, OrderType.STOP})


class OrderIntentContractError(ContractError):
    """The requested order intent is not permitted. Nothing is built and nothing is sent."""

    def __init__(self, errors: list[str]) -> None:
        self.errors = tuple(errors)
        super().__init__("invalid canonical order intent: " + "; ".join(self.errors))


def _finite(value: object) -> bool:
    return isinstance(value, Decimal) and value.is_finite()


def _aware(value: object) -> bool:
    return isinstance(value, datetime) and value.tzinfo is not None and value.utcoffset() is not None


@dataclass(frozen=True)
class CanonicalOrderIntent:
    """Immutable broker-neutral order request. Constructing one is NOT an order.

    It does not mean the broker accepted anything, that an order is pending, that a trade
    is active or that a position exists. Those transitions belong to later work.
    """

    EXECUTION_AUTHORITY: ClassVar[bool] = False
    BROKER_AUTHORITY: ClassVar[bool] = False
    RISK_AUTHORIZATION_AUTHORITY: ClassVar[bool] = False
    PERSISTENCE_AUTHORITY: ClassVar[bool] = False
    IS_BROKER_ORDER: ClassVar[bool] = False

    schema_version: int
    canonical_intent_id: str            # Day-10 intent identity
    strategy_ref: str
    thesis_id: Optional[str]            # correlation identity where applicable
    routed_profile_id: str              # exactly the Day-10 selection
    routed_account: AccountIdentity
    environment: ExecutionEnvironment
    canonical_symbol: str
    side: Side
    order_type: OrderType
    requested_price: Optional[Decimal]
    stop_loss: Decimal
    take_profit: Optional[Decimal]
    quantity: Decimal                   # already-authorized; never derived here
    risk_reservation_id: str            # Day-9 reservation reference
    risk_authorization_revision: int    # Day-9 durable revision
    reserved_risk_usd: Decimal
    created_at: datetime                # caller-supplied
    expiry_policy: ExpiryPolicy
    expires_at: Optional[datetime]
    idempotency_key: str

    def __post_init__(self) -> None:
        errors = order_intent_errors(self)
        if errors:
            raise OrderIntentContractError(errors)

    @property
    def identity_payload(self) -> dict:
        """The immutable identifiers that define this logical order. `created_at` is
        deliberately excluded so a replay at a later time is the SAME logical order."""
        return {
            "schema_version": self.schema_version,
            "canonical_intent_id": self.canonical_intent_id,
            "strategy_ref": self.strategy_ref,
            "thesis_id": self.thesis_id,
            "routed_profile_id": self.routed_profile_id,
            "routed_account": {"login": self.routed_account.login, "server": self.routed_account.server,
                               "company": self.routed_account.company, "currency": self.routed_account.currency},
            "environment": self.environment.value,
            "canonical_symbol": self.canonical_symbol,
            "side": self.side.value,
            "order_type": self.order_type.value,
            "requested_price": None if self.requested_price is None else str(self.requested_price),
            "stop_loss": str(self.stop_loss),
            "take_profit": None if self.take_profit is None else str(self.take_profit),
            "quantity": str(self.quantity),
            "risk_reservation_id": self.risk_reservation_id,
            "risk_authorization_revision": self.risk_authorization_revision,
            "reserved_risk_usd": str(self.reserved_risk_usd),
            "expiry_policy": self.expiry_policy.value,
            "expires_at": None if self.expires_at is None else self.expires_at.isoformat(),
        }


def derive_idempotency_key(payload: dict) -> str:
    """Deterministic key over the immutable identity payload. No clock, no RNG, no UUID."""
    return content_digest(payload)


def order_intent_errors(order: CanonicalOrderIntent) -> list[str]:
    """Every contract violation (empty means valid). Never raises, never sends anything."""
    errors: list[str] = []
    if type(order.schema_version) is not int or order.schema_version not in SUPPORTED_ORDER_INTENT_VERSIONS:
        errors.append(f"schema_version {order.schema_version!r} is not supported")
    for name in ("canonical_intent_id", "strategy_ref", "routed_profile_id", "canonical_symbol",
                 "risk_reservation_id"):
        value = getattr(order, name)
        if not isinstance(value, str) or not value.strip():
            errors.append(f"{name} must be a non-empty string")
    if order.thesis_id is not None and (not isinstance(order.thesis_id, str) or not order.thesis_id.strip()):
        errors.append("thesis_id must be None or a non-empty string")
    if not isinstance(order.routed_account, AccountIdentity):
        errors.append("routed_account must be an AccountIdentity")
    for name, enum_type in (("environment", ExecutionEnvironment), ("side", Side),
                            ("order_type", OrderType), ("expiry_policy", ExpiryPolicy)):
        if not isinstance(getattr(order, name), enum_type):
            errors.append(f"{name} must be a {enum_type.__name__}")

    if not _finite(order.quantity) or order.quantity <= 0:
        errors.append("quantity must be a finite Decimal > 0")
    if not _finite(order.reserved_risk_usd) or order.reserved_risk_usd <= 0:
        errors.append("reserved_risk_usd must be a finite Decimal > 0")
    if type(order.risk_authorization_revision) is not int or order.risk_authorization_revision < 1:
        errors.append("risk_authorization_revision must be an int >= 1")
    if not _finite(order.stop_loss) or order.stop_loss <= 0:
        errors.append("stop_loss must be a finite Decimal > 0")
    for name in ("requested_price", "take_profit"):
        value = getattr(order, name)
        if value is not None and (not _finite(value) or value <= 0):
            errors.append(f"{name} must be None or a finite Decimal > 0")
    if not _aware(order.created_at):
        errors.append("created_at must be a caller-supplied timezone-aware datetime")

    if isinstance(order.order_type, OrderType):
        if order.order_type in PRICED_ORDER_TYPES and order.requested_price is None:
            errors.append(f"{order.order_type} requires requested_price")
        if order.order_type is OrderType.MARKET and order.requested_price is not None:
            errors.append("MARKET must not carry requested_price")
    if isinstance(order.expiry_policy, ExpiryPolicy):
        if order.expiry_policy is ExpiryPolicy.GOOD_TILL_TIME:
            if not _aware(order.expires_at):
                errors.append("GOOD_TILL_TIME requires a timezone-aware expires_at")
            elif _aware(order.created_at) and order.expires_at <= order.created_at:
                errors.append("expires_at must be after created_at")
        elif order.expires_at is not None:
            errors.append(f"{order.expiry_policy} must not carry expires_at")
        if isinstance(order.order_type, OrderType):
            if order.order_type is OrderType.MARKET and order.expiry_policy is not ExpiryPolicy.IMMEDIATE:
                errors.append("MARKET requires IMMEDIATE expiry")
            if order.order_type in PRICED_ORDER_TYPES and order.expiry_policy is ExpiryPolicy.IMMEDIATE:
                errors.append(f"{order.order_type} cannot use IMMEDIATE expiry")

    # Direction-consistent protective levels (structural only; no policy such as distance or R:R).
    if isinstance(order.side, Side) and _finite(order.stop_loss):
        reference = order.requested_price if _finite(order.requested_price) else None
        if reference is not None:
            if order.side is Side.BUY and not order.stop_loss < reference:
                errors.append("BUY stop_loss must be below requested_price")
            if order.side is Side.SELL and not order.stop_loss > reference:
                errors.append("SELL stop_loss must be above requested_price")
            if order.take_profit is not None and _finite(order.take_profit):
                if order.side is Side.BUY and not order.take_profit > reference:
                    errors.append("BUY take_profit must be above requested_price")
                if order.side is Side.SELL and not order.take_profit < reference:
                    errors.append("SELL take_profit must be below requested_price")

    if not isinstance(order.idempotency_key, str) or not order.idempotency_key.strip():
        errors.append("idempotency_key must be a non-empty string")
    elif not errors:
        expected = derive_idempotency_key(order.identity_payload)
        if order.idempotency_key != expected:
            errors.append("idempotency_key does not match the immutable identity payload")
    return errors


def build_order_intent(strategy_intent: CanonicalStrategyIntent, routing: RoutingResult,
                       authorization: DurableAuthorizationOutcome, *, order_type: OrderType,
                       quantity: Decimal, created_at: datetime, expiry_policy: ExpiryPolicy,
                       requested_price: Optional[Decimal] = None, take_profit: Optional[Decimal] = None,
                       expires_at: Optional[datetime] = None) -> CanonicalOrderIntent:
    """Pure, deterministic, side-effect free normalization. Sends nothing, stores nothing.

    Fails closed unless the upstream evidence is genuine and mutually consistent:
    routed (Day 10), authorized with a reservation (Day 9), and the intent, routing and
    reservation all describe the SAME account, symbol and side.
    """
    errors: list[str] = []
    if not isinstance(strategy_intent, CanonicalStrategyIntent):
        raise TypeError("strategy_intent must be a CanonicalStrategyIntent")
    if not isinstance(routing, RoutingResult):
        raise TypeError("routing must be a Day-10 RoutingResult")
    if not isinstance(authorization, DurableAuthorizationOutcome):
        raise TypeError("authorization must be a Day-9 DurableAuthorizationOutcome")

    if not routing.routed or routing.selected_profile_id is None or routing.selected_account is None:
        errors.append(f"routing did not select an account ({routing.decision}); Day 11 never selects one")
    if authorization.code not in AUTHORIZED_OUTCOMES or authorization.reservation is None:
        errors.append(f"no upstream risk authorization ({authorization.code}); Day 11 never authorizes risk")
    if errors:
        raise OrderIntentContractError(errors)

    reservation = authorization.reservation
    if reservation.profile_id != routing.selected_profile_id:
        errors.append(f"reservation profile {reservation.profile_id} != routed profile {routing.selected_profile_id}")
    if reservation.account != routing.selected_account:
        errors.append("reservation account identity != routed account identity")
    if authorization.profile_id != routing.selected_profile_id:
        errors.append("authorization profile != routed profile")
    if reservation.canonical_symbol != strategy_intent.canonical_symbol:
        errors.append("reservation symbol != strategy intent symbol")
    if reservation.side is not strategy_intent.side:
        errors.append("reservation side != strategy intent side")
    if routing.intent_id != strategy_intent.intent_id:
        errors.append("routing result belongs to a different canonical intent")
    if strategy_intent.intent_type is IntentType.INFRA_TEST and strategy_intent.environment is ExecutionEnvironment.LIVE:
        errors.append("an infrastructure-test intent can never target LIVE")
    if type(authorization.durable_revision) is not int:
        errors.append("authorization is missing its durable revision")
    if errors:
        raise OrderIntentContractError(errors)

    payload_source = dict(
        schema_version=ORDER_INTENT_SCHEMA_VERSION,
        canonical_intent_id=strategy_intent.intent_id,
        strategy_ref=strategy_intent.strategy_ref,
        thesis_id=strategy_intent.thesis_id,
        routed_profile_id=routing.selected_profile_id,
        routed_account=routing.selected_account,
        environment=strategy_intent.environment,
        canonical_symbol=strategy_intent.canonical_symbol,
        side=strategy_intent.side,
        order_type=order_type,
        requested_price=requested_price,
        stop_loss=strategy_intent.invalidation,
        take_profit=take_profit if take_profit is not None else strategy_intent.objective,
        quantity=quantity,
        risk_reservation_id=reservation.reservation_id,
        risk_authorization_revision=authorization.durable_revision,
        reserved_risk_usd=reservation.risk_usd,
        created_at=created_at,
        expiry_policy=expiry_policy,
        expires_at=expires_at,
    )
    # The key is derived from the identity payload, so it is computed on an unvalidated
    # provisional view first and then validated by the real contract below.
    provisional = object.__new__(CanonicalOrderIntent)
    for name, value in dict(payload_source, idempotency_key="").items():
        object.__setattr__(provisional, name, value)
    try:
        key = derive_idempotency_key(provisional.identity_payload)
    except Exception as exc:  # noqa: BLE001 - unencodable values fail closed before any contract is built
        raise OrderIntentContractError([f"order intent is not representable: {type(exc).__name__}: {exc}"]) from exc
    return CanonicalOrderIntent(idempotency_key=key, **payload_source)


def encode_order_intent(order: CanonicalOrderIntent) -> dict:
    """Canonical, JSON-compatible representation. Pure; writes nothing anywhere."""
    payload = dict(order.identity_payload)
    payload["created_at"] = order.created_at.isoformat()
    payload["idempotency_key"] = order.idempotency_key
    return payload


def decode_order_intent(payload: Any) -> CanonicalOrderIntent:
    """Strictly rebuild an order intent from `encode_order_intent` output."""
    if not isinstance(payload, dict):
        raise OrderIntentContractError(["payload must be an object"])
    try:
        account = payload["routed_account"]
        order = CanonicalOrderIntent(
            schema_version=payload["schema_version"],
            canonical_intent_id=payload["canonical_intent_id"],
            strategy_ref=payload["strategy_ref"],
            thesis_id=payload["thesis_id"],
            routed_profile_id=payload["routed_profile_id"],
            routed_account=AccountIdentity(account["login"], account["server"], account["company"],
                                           account["currency"]),
            environment=ExecutionEnvironment(payload["environment"]),
            canonical_symbol=payload["canonical_symbol"],
            side=Side(payload["side"]),
            order_type=OrderType(payload["order_type"]),
            requested_price=None if payload["requested_price"] is None else Decimal(payload["requested_price"]),
            stop_loss=Decimal(payload["stop_loss"]),
            take_profit=None if payload["take_profit"] is None else Decimal(payload["take_profit"]),
            quantity=Decimal(payload["quantity"]),
            risk_reservation_id=payload["risk_reservation_id"],
            risk_authorization_revision=payload["risk_authorization_revision"],
            reserved_risk_usd=Decimal(payload["reserved_risk_usd"]),
            created_at=datetime.fromisoformat(payload["created_at"]),
            expiry_policy=ExpiryPolicy(payload["expiry_policy"]),
            expires_at=None if payload["expires_at"] is None else datetime.fromisoformat(payload["expires_at"]),
            idempotency_key=payload["idempotency_key"],
        )
    except OrderIntentContractError:
        raise
    except Exception as exc:  # noqa: BLE001 - malformed payloads fail closed
        raise OrderIntentContractError([f"malformed payload: {type(exc).__name__}: {exc}"]) from exc
    return order


def order_intent_digest(order: CanonicalOrderIntent) -> str:
    """Digest of the full encoded intent (including created_at), for audit only."""
    return content_digest(encode_order_intent(order))


def canonical_order_json(order: CanonicalOrderIntent) -> str:
    return canonical_json(encode_order_intent(order))
