from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, time
from decimal import Decimal
from typing import Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from ._validation import ContractError, require_aware, require_decimal, require_text
from .enums import LossModel, Permission, Phase, PolicyDecisionCode, Side


@dataclass(frozen=True)
class FirmPolicy:
    """Versioned rules for one firm + product + program + phase.

    Values come from the firm's official published rules at `effective_from`.
    They are configuration, never strategy code.
    """

    firm_id: str
    product: str
    program: str
    phase: Phase
    version: str
    effective_from: date
    effective_to: Optional[date]
    source_url: str

    automation: Permission
    automation_verified: bool
    own_account_copy: Permission
    cross_firm_copy: Permission
    allows_opposite_exposure_across_accounts: bool
    max_accounts: Optional[int]

    daily_loss_model: LossModel
    daily_loss_limit_pct: Optional[Decimal]
    max_loss_model: LossModel
    max_loss_limit_pct: Optional[Decimal]
    daily_reset_tz: str
    daily_reset_time: time

    allowed_symbols: frozenset[str] = field(default_factory=frozenset)

    def __post_init__(self) -> None:
        for name in ("firm_id", "product", "program", "version", "source_url", "daily_reset_tz"):
            require_text(name, getattr(self, name))
        try:
            ZoneInfo(self.daily_reset_tz)
        except (ZoneInfoNotFoundError, ValueError) as exc:
            raise ContractError(f"unknown daily_reset_tz {self.daily_reset_tz!r}") from exc
        if self.effective_to is not None and self.effective_to < self.effective_from:
            raise ContractError("effective_to precedes effective_from")
        for name in ("daily_loss_limit_pct", "max_loss_limit_pct"):
            v = getattr(self, name)
            if v is not None:
                require_decimal(name, v, positive=True)
        if self.daily_loss_model is not LossModel.NONE and self.daily_loss_limit_pct is None:
            raise ContractError("daily_loss_limit_pct required unless daily_loss_model is NONE")
        if self.max_loss_model is not LossModel.NONE and self.max_loss_limit_pct is None:
            raise ContractError("max_loss_limit_pct required unless max_loss_model is NONE")
        object.__setattr__(self, "allowed_symbols", frozenset(self.allowed_symbols))

    def is_effective(self, on: date) -> bool:
        return self.effective_from <= on and (self.effective_to is None or on <= self.effective_to)

    @property
    def key(self) -> tuple[str, str, str, Phase]:
        return (self.firm_id, self.product, self.program, self.phase)


@dataclass(frozen=True)
class OpenExposure:
    profile_id: str
    firm_id: str
    canonical_symbol: str
    side: Side


@dataclass(frozen=True)
class PolicyContext:
    now: datetime
    open_exposures: tuple[OpenExposure, ...] = ()
    # firm_ids of children of the SAME thesis already approved, in fan-out order
    approved_sibling_firm_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        require_aware("now", self.now)


@dataclass(frozen=True)
class PolicyDecision:
    """Structured policy eligibility result. Never a bare boolean.

    Day 2: `evaluated_at` is mandatory and equals the PolicyContext time, so
    the same input always yields an identical decision.
    """

    child_trade_id: str
    code: PolicyDecisionCode
    reason: str
    policy_version: Optional[str]
    evaluated_at: datetime

    def __post_init__(self) -> None:
        require_text("child_trade_id", self.child_trade_id)
        if not isinstance(self.code, PolicyDecisionCode):
            raise ContractError("code must be a PolicyDecisionCode")
        require_text("reason", self.reason)
        require_aware("evaluated_at", self.evaluated_at)
        if self.policy_version is not None:
            require_text("policy_version", self.policy_version)
        if self.code is PolicyDecisionCode.ELIGIBLE and self.policy_version is None:
            raise ContractError("an ELIGIBLE decision must carry the exact policy_version")

    @property
    def eligible(self) -> bool:
        return self.code is PolicyDecisionCode.ELIGIBLE
