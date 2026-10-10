"""Day-10 canonical strategy intent CONTRACTS - REFERENCE ONLY - NON-PRODUCTION.

EXECUTION AUTHORITY = FALSE. BROKER AUTHORITY = FALSE.
No orders, no broker APIs, no market data, no symbols/lots/contracts/ticks/margin,
no persistence, no clock, no generated identity, no strategy logic.

This is the platform-neutral socket an external strategy speaks into. Day 10 never
understands what a strategy's signal MEANS; it validates identity and semantics only.

Reused frozen concepts (no duplication):
  Venue           market/execution domain (MT5_CFD | FUTURES)
  Side            direction
  AccountIdentity account identity
  Decimal         normalized risk and price levels

New Day-10 concepts, with the reason each is unavoidable:
  StrategyRef            frozen StrategyId is closed to STRATEGY_A/STRATEGY_B, so it cannot
                         express a neutral identity, INFRA_TEST or a future strategy. An opaque
                         validated string is used; `frozen_strategy_id()` maps back when it matches.
  IntentType             separates STRATEGY from INFRA_TEST so a test can never masquerade as a trade.
  ExecutionEnvironment   no frozen contract distinguishes SIMULATION/DEMO/LIVE (Phase is a prop
                         program phase, not an environment).
  RoutableAccount        an explicitly supplied routing candidate; no discovery of any kind.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import ClassVar, Mapping, Optional

from ..domain._validation import ContractError
from ..domain.account import AccountIdentity
from ..domain.enums import Side, StrategyId, StrEnum, Venue

INTENT_SCHEMA_VERSION = 1
SUPPORTED_INTENT_SCHEMA_VERSIONS = frozenset({INTENT_SCHEMA_VERSION})

#: Platform-defined neutral strategy identity (INFRA_TEST and platform-originated intents).
PLATFORM_NONE = "NONE"
_REF_PATTERN = re.compile(r"^[A-Z][A-Z0-9_]{1,63}$")
_SYMBOL_PATTERN = re.compile(r"^[A-Z][A-Z0-9._]{1,31}$")


class IntentType(StrEnum):
    STRATEGY = "STRATEGY"        # an external strategy's trading intent
    INFRA_TEST = "INFRA_TEST"    # infrastructure test; never a Strategy A/B/C trade


class RequestedAction(StrEnum):
    OPEN = "OPEN"
    PROTECT = "PROTECT"
    CLOSE = "CLOSE"


class ExecutionEnvironment(StrEnum):
    SIMULATION = "SIMULATION"
    DEMO = "DEMO"
    LIVE = "LIVE"


#: Environments in which an INFRA_TEST intent may ever be routed. LIVE is excluded.
INFRA_TEST_ENVIRONMENTS = frozenset({ExecutionEnvironment.SIMULATION, ExecutionEnvironment.DEMO})


def frozen_strategy_id(strategy_ref: str) -> Optional[StrategyId]:
    """The frozen StrategyId when the opaque reference names one, else None."""
    try:
        return StrategyId(strategy_ref)
    except ValueError:
        return None


def _text(errors: list[str], name: str, value: object, pattern: Optional[re.Pattern] = None) -> None:
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        errors.append(f"{name} must be a non-empty string without surrounding whitespace")
    elif pattern is not None and not pattern.match(value):
        errors.append(f"{name} {value!r} does not match {pattern.pattern}")


@dataclass(frozen=True)
class CanonicalStrategyIntent:
    """What an external strategy wants, expressed platform-neutrally.

    Construction validates shape and semantics and fails closed; it grants nothing.
    `intent_id` is caller-supplied: Day 10 generates no identity and reads no clock.
    """

    EXECUTION_AUTHORITY: ClassVar[bool] = False
    BROKER_AUTHORITY: ClassVar[bool] = False

    schema_version: int
    intent_id: str
    intent_type: IntentType
    strategy_ref: str
    environment: ExecutionEnvironment
    venue: Venue
    canonical_symbol: str
    side: Side
    action: RequestedAction
    effective_at: datetime
    thesis_id: Optional[str] = None
    test_case_id: Optional[str] = None
    target_profile_id: Optional[str] = None          # explicit, deterministic destination
    entry: Optional[Decimal] = None
    invalidation: Optional[Decimal] = None
    objective: Optional[Decimal] = None
    requested_risk_usd: Optional[Decimal] = None     # normalized risk (account currency)
    risk_reference: Optional[str] = None             # or a reference resolved by a later layer
    evidence_ref: Optional[str] = None
    metadata: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        errors = intent_errors(self)
        if errors:
            raise ContractError("invalid canonical intent: " + "; ".join(errors))

    @property
    def is_infrastructure_test(self) -> bool:
        return self.intent_type is IntentType.INFRA_TEST

    @property
    def frozen_strategy_id(self) -> Optional[StrategyId]:
        return frozen_strategy_id(self.strategy_ref)

    @property
    def fingerprint(self) -> tuple:
        """Material identity. Reuse of an intent_id with a different fingerprint is a conflict."""
        return (self.schema_version, self.intent_id, self.intent_type, self.strategy_ref, self.environment,
                self.venue, self.canonical_symbol, self.side, self.action, self.effective_at, self.thesis_id,
                self.test_case_id, self.target_profile_id, self.entry, self.invalidation, self.objective,
                self.requested_risk_usd, self.risk_reference, self.evidence_ref, self.metadata)


def intent_errors(intent: CanonicalStrategyIntent) -> list[str]:
    """Every validation error for an intent (empty means valid). Never raises."""
    errors: list[str] = []
    if type(intent.schema_version) is not int or intent.schema_version not in SUPPORTED_INTENT_SCHEMA_VERSIONS:
        errors.append(f"schema_version {intent.schema_version!r} is not supported")
    _text(errors, "intent_id", intent.intent_id)
    for name, enum_type in (("intent_type", IntentType), ("environment", ExecutionEnvironment),
                            ("venue", Venue), ("side", Side), ("action", RequestedAction)):
        if not isinstance(getattr(intent, name), enum_type):
            errors.append(f"{name} must be a {enum_type.__name__}")
    _text(errors, "strategy_ref", intent.strategy_ref, _REF_PATTERN)
    _text(errors, "canonical_symbol", intent.canonical_symbol, _SYMBOL_PATTERN)
    if not isinstance(intent.effective_at, datetime) or intent.effective_at.tzinfo is None \
            or intent.effective_at.utcoffset() is None:
        errors.append("effective_at must be a caller-supplied timezone-aware datetime")
    for name in ("thesis_id", "test_case_id", "target_profile_id", "risk_reference", "evidence_ref"):
        value = getattr(intent, name)
        if value is not None:
            _text(errors, name, value)
    for name in ("entry", "invalidation", "objective", "requested_risk_usd"):
        value = getattr(intent, name)
        if value is not None and (not isinstance(value, Decimal) or not value.is_finite()):
            errors.append(f"{name} must be a finite Decimal")
    if intent.requested_risk_usd is not None and isinstance(intent.requested_risk_usd, Decimal) \
            and intent.requested_risk_usd.is_finite() and intent.requested_risk_usd <= 0:
        errors.append("requested_risk_usd must be > 0")
    if not isinstance(intent.metadata, tuple) or any(
            not (isinstance(pair, tuple) and len(pair) == 2 and all(isinstance(x, str) for x in pair))
            for pair in intent.metadata):
        errors.append("metadata must be a tuple of (str, str) pairs")
    elif [k for k, _ in intent.metadata] != sorted({k for k, _ in intent.metadata}):
        errors.append("metadata keys must be unique and sorted")

    if isinstance(intent.intent_type, IntentType):
        if intent.is_infrastructure_test:
            if intent.strategy_ref != PLATFORM_NONE:
                errors.append(f"INFRA_TEST must use the neutral strategy identity {PLATFORM_NONE}")
            if intent.test_case_id is None:
                errors.append("INFRA_TEST requires test_case_id")
            if intent.thesis_id is not None:
                errors.append("INFRA_TEST must not carry a strategy thesis_id")
            if intent.environment not in INFRA_TEST_ENVIRONMENTS:
                errors.append(f"INFRA_TEST must not target {intent.environment}")
        else:
            if intent.strategy_ref == PLATFORM_NONE:
                errors.append("a STRATEGY intent requires a real strategy identity")
            if intent.test_case_id is not None:
                errors.append("test_case_id belongs to INFRA_TEST intents only")

    if isinstance(intent.action, RequestedAction) and intent.action is RequestedAction.OPEN:
        if (intent.requested_risk_usd is None) == (intent.risk_reference is None):
            errors.append("an OPEN intent needs exactly one of requested_risk_usd or risk_reference")
        if intent.entry is None or intent.invalidation is None:
            errors.append("an OPEN intent requires entry and invalidation levels")
        elif isinstance(intent.side, Side) and isinstance(intent.entry, Decimal) \
                and isinstance(intent.invalidation, Decimal) and intent.entry.is_finite() \
                and intent.invalidation.is_finite():
            if intent.side is Side.BUY and not intent.invalidation < intent.entry:
                errors.append("BUY invalidation must be below entry")
            if intent.side is Side.SELL and not intent.invalidation > intent.entry:
                errors.append("SELL invalidation must be above entry")
            if intent.objective is not None and isinstance(intent.objective, Decimal) and intent.objective.is_finite():
                if intent.side is Side.BUY and not intent.objective > intent.entry:
                    errors.append("BUY objective must be above entry")
                if intent.side is Side.SELL and not intent.objective < intent.entry:
                    errors.append("SELL objective must be below entry")
    return errors


@dataclass(frozen=True)
class RoutableAccount:
    """An explicitly supplied routing candidate. Day 10 discovers nothing."""

    profile_id: str
    account: AccountIdentity
    venue: Venue
    environment: ExecutionEnvironment
    enabled: bool
    permitted_strategy_refs: frozenset[str]
    accepts_infrastructure_tests: bool = False

    def __post_init__(self) -> None:
        errors: list[str] = []
        _text(errors, "profile_id", self.profile_id)
        if not isinstance(self.account, AccountIdentity):
            errors.append("account must be an AccountIdentity")
        if not isinstance(self.venue, Venue):
            errors.append("venue must be a Venue")
        if not isinstance(self.environment, ExecutionEnvironment):
            errors.append("environment must be an ExecutionEnvironment")
        for name in ("enabled", "accepts_infrastructure_tests"):
            if type(getattr(self, name)) is not bool:
                errors.append(f"{name} must be a bool")
        if not isinstance(self.permitted_strategy_refs, frozenset) or not self.permitted_strategy_refs:
            errors.append("permitted_strategy_refs must be a non-empty frozenset")
        else:
            for ref in self.permitted_strategy_refs:
                _text(errors, "permitted_strategy_refs entry", ref, _REF_PATTERN)
        if errors:
            raise ContractError("invalid routable account: " + "; ".join(errors))


class IntentIdentityConflict(ContractError):
    """intent_id reused with a materially different immutable payload."""


@dataclass(frozen=True)
class IntentIdentityIndex:
    """Pure, immutable intent_id -> fingerprint index. No storage, no clock."""

    entries: tuple[tuple[str, tuple], ...] = ()

    def fingerprint_of(self, intent_id: str) -> Optional[tuple]:
        return next((fingerprint for known, fingerprint in self.entries if known == intent_id), None)

    def with_intent(self, intent: CanonicalStrategyIntent) -> "IntentIdentityIndex":
        """Return an index containing `intent`. Identical re-record is a no-op; a conflicting
        reuse of the same intent_id fails closed."""
        known = self.fingerprint_of(intent.intent_id)
        if known is None:
            return IntentIdentityIndex(self.entries + ((intent.intent_id, intent.fingerprint),))
        if known != intent.fingerprint:
            raise IntentIdentityConflict(
                f"intent_id {intent.intent_id} already used with a materially different payload")
        return self
