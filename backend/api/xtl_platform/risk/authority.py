"""Shared Risk Authority interface (PRD section 7).

This module defines the CONTRACT only. It contains no implementation that
grants production risk authority. The production mechanism (Redis Lua,
transaction, or service boundary) is an open decision (PRD section 22) and is
deliberately not implemented at P1.

The only implementation today is `risk.reference_test_only`, which is
REFERENCE / TEST ONLY.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import ClassVar, Optional

from ..domain.enums import ReservationStatus as RS
from ..domain.risk import (
    AccountRiskLimits,
    AccountRiskSnapshot,
    RiskAuthorizationResult,
    RiskRequest,
    RiskReservation,
)


class RiskAuthority(ABC):
    #: Every implementation must declare this explicitly. P1 has no
    #: implementation for which it is True.
    GRANTS_PRODUCTION_AUTHORITY: ClassVar[bool]
    AUTHORITY_CLASSIFICATION: ClassVar[str]

    @abstractmethod
    def authorize_and_reserve(
        self,
        request: RiskRequest,
        snapshot: AccountRiskSnapshot,
        limits: AccountRiskLimits,
        now: datetime,
    ) -> RiskAuthorizationResult:
        """Check capacity and create the reservation as ONE atomic operation."""

    @abstractmethod
    def mark_sent(self, authorization_id: str, now: datetime) -> bool:
        """Pin a reservation before it is handed to a broker adapter.
        Returns False if it is expired, released or unknown: caller must not send."""

    @abstractmethod
    def release(self, authorization_id: str, reason: str, now: datetime) -> bool:
        """Release capacity. True exactly once per reservation."""

    @abstractmethod
    def expire_unsent(self, now: datetime) -> list[str]:
        """Expire RESERVED (never SENT) reservations past expiry."""

    @abstractmethod
    def status(self, authorization_id: str) -> Optional[RS]: ...

    @abstractmethod
    def active_reservations(self, profile_id: str) -> list[RiskReservation]: ...
