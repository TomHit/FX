"""REFERENCE / TEST ONLY risk authority.

GRANTS NO PRODUCTION RISK AUTHORITY. Not exported from `xtl_platform.risk`.
Import it explicitly, and only from tests.
"""
from __future__ import annotations

import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import ClassVar, Optional

from ..domain.enums import ReservationStatus as RS, RiskDecisionCode as R
from ..domain.risk import (
    AccountRiskLimits,
    AccountRiskSnapshot,
    RiskAuthorizationResult,
    RiskRequest,
    RiskReservation,
)
from .authority import RiskAuthority

_ACTIVE = frozenset({RS.RESERVED, RS.SENT})


@dataclass
class _Entry:
    reservation: RiskReservation
    status: RS
    released_reason: Optional[str] = None
    released_at: Optional[datetime] = None


class ReferenceTestOnlyRiskAuthority(RiskAuthority):
    """REFERENCE / TEST ONLY. GRANTS NO PRODUCTION RISK AUTHORITY.

    Single-process, in-memory, lost on restart. It exists to pin down the
    behaviour any real authority must reproduce (atomic authorize+reserve,
    idempotent trade_id, two-phase RESERVED -> SENT, exactly-once release).
    It must never be wired into a live or demo execution path.
    """

    GRANTS_PRODUCTION_AUTHORITY: ClassVar[bool] = False
    AUTHORITY_CLASSIFICATION: ClassVar[str] = "REFERENCE_TEST_ONLY"

    def __init__(
        self,
        reservation_ttl: timedelta,
        max_snapshot_age: timedelta,
        *,
        acknowledge_reference_test_only: bool = False,
    ) -> None:
        if acknowledge_reference_test_only is not True:
            raise RuntimeError(
                "ReferenceTestOnlyRiskAuthority is REFERENCE / TEST ONLY and grants no "
                "production risk authority; pass acknowledge_reference_test_only=True"
            )
        self._ttl = reservation_ttl
        self._max_age = max_snapshot_age
        self._lock = threading.Lock()
        self._by_auth: dict[str, _Entry] = {}
        self._auth_by_trade: dict[str, str] = {}

    def authorize_and_reserve(self, request, snapshot, limits, now) -> RiskAuthorizationResult:
        intent = request.intent
        trade_id = intent.child_trade_id
        if not (snapshot.profile_id == limits.profile_id == intent.profile_id):
            raise ValueError("snapshot/limits/intent profile mismatch")

        def block(code: R, reason: str) -> RiskAuthorizationResult:
            return RiskAuthorizationResult(trade_id, code, reason)

        with self._lock:
            existing = self._auth_by_trade.get(trade_id)
            if existing is not None:
                return RiskAuthorizationResult(
                    trade_id, R.DUPLICATE_TRADE_ID, "trade_id already authorized",
                    self._by_auth[existing].reservation,
                )
            if now - snapshot.as_of > self._max_age:
                return block(R.STALE_ACCOUNT_STATE, f"snapshot age {now - snapshot.as_of} exceeds {self._max_age}")
            if snapshot.halted:
                return block(R.BLOCK_DAILY_HALT, "account halted")

            active = [e.reservation for e in self._by_auth.values()
                      if e.status in _ACTIVE and e.reservation.profile_id == intent.profile_id]
            open_risk = sum((r.risk_usd for r in active), Decimal("0"))
            req = request.requested_risk_usd

            if len(active) + 1 > limits.max_positions:
                return block(R.BLOCK_MAX_POSITIONS, f"{len(active)} active of max {limits.max_positions}")
            if open_risk + req > limits.max_open_risk_usd:
                return block(R.BLOCK_OPEN_RISK, f"open {open_risk} + {req} > {limits.max_open_risk_usd}")
            # Worst case: every open reservation hits its stop today.
            if snapshot.daily_loss_used_usd + open_risk + req > limits.daily_loss_limit_usd:
                return block(R.BLOCK_DAILY_LIMIT, "daily-loss capacity insufficient")
            if snapshot.max_loss_used_usd + open_risk + req > limits.max_loss_limit_usd:
                return block(R.BLOCK_MAX_LOSS, "maximum-loss capacity insufficient")

            reservation = RiskReservation(
                authorization_id=f"auth_{uuid.uuid4()}",
                trade_id=trade_id,
                thesis_id=intent.thesis_id,
                strategy_id=intent.strategy_id,
                profile_id=intent.profile_id,
                account=intent.account,
                venue=intent.venue,
                venue_symbol=intent.venue_symbol,
                side=intent.side,
                risk_usd=req,
                risk_pct=(req / snapshot.balance * 100).quantize(Decimal("0.0001")),
                quantity=request.quantity,
                entry=intent.entry,
                stop=intent.stop,
                target=intent.target,
                authorized_at=now,
                expires_at=min(now + self._ttl, intent.expires_at),
                policy_version=request.policy_version,
            )
            self._by_auth[reservation.authorization_id] = _Entry(reservation, RS.RESERVED)
            self._auth_by_trade[trade_id] = reservation.authorization_id
            return RiskAuthorizationResult(trade_id, R.AUTHORIZED_RESERVED, "capacity reserved", reservation)

    def mark_sent(self, authorization_id, now) -> bool:
        with self._lock:
            e = self._by_auth.get(authorization_id)
            if e is None or e.status is not RS.RESERVED:
                return False
            if now >= e.reservation.expires_at:
                e.status = RS.EXPIRED
                return False
            e.status = RS.SENT
            return True

    def release(self, authorization_id, reason, now) -> bool:
        with self._lock:
            e = self._by_auth.get(authorization_id)
            if e is None or e.status not in _ACTIVE:
                return False
            e.status, e.released_reason, e.released_at = RS.RELEASED, reason, now
            return True

    def expire_unsent(self, now) -> list[str]:
        expired: list[str] = []
        with self._lock:
            for auth_id, e in self._by_auth.items():
                if e.status is RS.RESERVED and now >= e.reservation.expires_at:
                    e.status = RS.EXPIRED
                    expired.append(auth_id)
        return expired

    def status(self, authorization_id) -> Optional[RS]:
        with self._lock:
            e = self._by_auth.get(authorization_id)
            return e.status if e else None

    def active_reservations(self, profile_id) -> list[RiskReservation]:
        with self._lock:
            return [e.reservation for e in self._by_auth.values()
                    if e.status in _ACTIVE and e.reservation.profile_id == profile_id]
