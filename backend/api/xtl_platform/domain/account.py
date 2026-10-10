from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from types import MappingProxyType
from typing import Mapping

from ._validation import ContractError, require_aware, require_decimal, require_text
from .enums import Phase, Venue


@dataclass(frozen=True)
class AccountIdentity:
    login: str
    server: str
    company: str
    currency: str

    def __post_init__(self) -> None:
        for name in ("login", "server", "company", "currency"):
            require_text(name, getattr(self, name))

    @property
    def key(self) -> tuple[str, str, str]:
        return (self.company, self.server, self.login)


@dataclass(frozen=True)
class Device:
    device_id: str
    owner_user_id: str
    healthy: bool
    venues: frozenset[Venue]
    last_heartbeat: datetime

    def __post_init__(self) -> None:
        require_text("device_id", self.device_id)
        require_text("owner_user_id", self.owner_user_id)
        require_aware("last_heartbeat", self.last_heartbeat)
        object.__setattr__(self, "venues", frozenset(self.venues))


@dataclass(frozen=True)
class Profile:
    profile_id: str
    owner_user_id: str
    firm_id: str
    product: str
    program: str
    phase: Phase
    account_size: Decimal
    venue: Venue
    identity: AccountIdentity
    device_id: str
    enabled: bool
    # canonical XTL symbol -> broker/venue symbol, e.g. {"XAUUSD": "XAUUSD.r"}
    symbol_map: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for name in ("profile_id", "owner_user_id", "firm_id", "product", "program", "device_id"):
            require_text(name, getattr(self, name))
        require_decimal("account_size", self.account_size, positive=True)
        object.__setattr__(self, "symbol_map", MappingProxyType(dict(self.symbol_map)))


@dataclass(frozen=True)
class ExecutionSet:
    """The profiles a user has registered for fan-out."""

    user_id: str
    profiles: tuple[Profile, ...]

    def __post_init__(self) -> None:
        require_text("user_id", self.user_id)
        object.__setattr__(self, "profiles", tuple(self.profiles))
        seen_ids: set[str] = set()
        seen_identities: set[tuple[str, str, str]] = set()
        for p in self.profiles:
            if p.owner_user_id != self.user_id:
                raise ContractError(f"profile {p.profile_id} is not owned by {self.user_id}")
            if p.profile_id in seen_ids:
                raise ContractError(f"duplicate profile_id {p.profile_id}")
            if p.identity.key in seen_identities:
                raise ContractError(f"account identity {p.identity.key} bound to more than one profile")
            seen_ids.add(p.profile_id)
            seen_identities.add(p.identity.key)
