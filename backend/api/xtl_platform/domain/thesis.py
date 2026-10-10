from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Optional

from ._validation import ContractError, require_aware, require_decimal, require_text
from .account import AccountIdentity, Profile
from .enums import Side, StrategyId, Venue


def _validate_levels(side: Side, entry: Decimal, stop: Decimal, target: Optional[Decimal]) -> None:
    require_decimal("entry", entry, positive=True)
    require_decimal("stop", stop, positive=True)
    if target is not None:
        require_decimal("target", target, positive=True)
    if side is Side.BUY:
        if not stop < entry:
            raise ContractError("BUY stop must be below entry")
        if target is not None and not target > entry:
            raise ContractError("BUY target must be above entry")
    else:
        if not stop > entry:
            raise ContractError("SELL stop must be above entry")
        if target is not None and not target < entry:
            raise ContractError("SELL target must be below entry")


@dataclass(frozen=True)
class Thesis:
    """One canonical strategy idea. Emitted once, fanned out to many accounts.

    `venue` is the market family the levels were derived from. A thesis built
    on XAUUSD CFD prices is never expressed on MGC futures (PRD section 20).
    """

    thesis_id: str
    strategy_id: StrategyId
    venue: Venue
    symbol: str
    side: Side
    entry: Decimal
    stop: Decimal
    target: Optional[Decimal]
    created_at: datetime
    expires_at: datetime

    def __post_init__(self) -> None:
        require_text("thesis_id", self.thesis_id)
        require_text("symbol", self.symbol)
        if not isinstance(self.strategy_id, StrategyId):
            raise ContractError("strategy_id must be a StrategyId")
        _validate_levels(self.side, self.entry, self.stop, self.target)
        require_aware("created_at", self.created_at)
        require_aware("expires_at", self.expires_at)
        if self.expires_at <= self.created_at:
            raise ContractError("expires_at must be after created_at")

    @property
    def stop_distance(self) -> Decimal:
        return abs(self.entry - self.stop)


@dataclass(frozen=True)
class ChildExecutionIntent:
    """Account-specific expression of a thesis. Identity is frozen for life."""

    thesis_id: str
    strategy_id: StrategyId
    profile_id: str
    firm_id: str
    account: AccountIdentity
    device_id: str
    venue: Venue
    canonical_symbol: str
    venue_symbol: str
    side: Side
    entry: Decimal
    stop: Decimal
    target: Optional[Decimal]
    created_at: datetime
    expires_at: datetime

    def __post_init__(self) -> None:
        for name in ("thesis_id", "profile_id", "firm_id", "device_id", "canonical_symbol", "venue_symbol"):
            require_text(name, getattr(self, name))
        _validate_levels(self.side, self.entry, self.stop, self.target)
        require_aware("created_at", self.created_at)
        require_aware("expires_at", self.expires_at)

    @property
    def child_trade_id(self) -> str:
        return f"{self.thesis_id}:{self.profile_id}"

    @classmethod
    def from_thesis(cls, thesis: Thesis, profile: Profile, venue_symbol: str) -> "ChildExecutionIntent":
        return cls(
            thesis_id=thesis.thesis_id,
            strategy_id=thesis.strategy_id,
            profile_id=profile.profile_id,
            firm_id=profile.firm_id,
            account=profile.identity,
            device_id=profile.device_id,
            venue=profile.venue,
            canonical_symbol=thesis.symbol,
            venue_symbol=venue_symbol,
            side=thesis.side,
            entry=thesis.entry,
            stop=thesis.stop,
            target=thesis.target,
            created_at=thesis.created_at,
            expires_at=thesis.expires_at,
        )
