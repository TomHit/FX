from __future__ import annotations

"""
Strategy-B pure risk authorization contract.

This module has no Redis, broker, MT5, order, queue, or Strategy-A authority.

It evaluates an immutable account/profile capacity snapshot against one
Strategy-B risk request and, when valid, produces an immutable reservation
proposal.

Persistence and true atomic commit are owned by a later layer.
"""

from dataclasses import dataclass
import hashlib
import math
from typing import Optional


@dataclass(frozen=True)
class StrategyBRiskCapacity:
    uid: str
    profile_id: str
    profile_enabled: bool

    device_id: str
    account_login: str
    account_server: str
    broker_company: str

    account_size: float

    daily_loss_used_usd: float
    daily_loss_limit_usd: float

    max_loss_used_usd: float
    max_loss_limit_usd: float

    open_risk_usd: float
    max_open_risk_usd: float

    open_positions_count: int
    max_open_positions: int

    snapshot_at_ms: int
    now_ms: int
    snapshot_max_age_ms: int


@dataclass(frozen=True)
class StrategyBRiskAuthorizationRequest:
    uid: str
    profile_id: str

    thesis_id: str
    selected_capture_id: str

    symbol: str
    side: str

    entry: float
    sl: float
    tp: float

    lots: float
    risk_usd: float
    risk_pct: float

    device_id: str
    account_login: str
    account_server: str
    broker_company: str

    requested_at_ms: int


@dataclass(frozen=True)
class StrategyBRiskReservation:
    reservation_id: str
    strategy: str

    uid: str
    profile_id: str

    thesis_id: str
    selected_capture_id: str

    symbol: str
    side: str

    entry: float
    sl: float
    tp: float

    lots: float
    risk_usd: float
    risk_pct: float

    device_id: str
    account_login: str
    account_server: str
    broker_company: str

    status: str
    reserved_at_ms: int


@dataclass(frozen=True)
class StrategyBRiskAuthorizationResult:
    authorized: bool
    reason: str
    reservation: Optional[StrategyBRiskReservation]

    observation_only: bool = True
    execution_authority: bool = False
    order_authority: bool = False
    broker_authority: bool = False
    strategy_a_mutation: bool = False


def _text(value: object) -> str:
    return str(value or "").strip()


def _finite_positive(value: object) -> bool:
    if isinstance(value, bool):
        return False

    try:
        number = float(value)
    except (TypeError, ValueError):
        return False

    return math.isfinite(number) and number > 0.0


def _finite_nonnegative(value: object) -> bool:
    if isinstance(value, bool):
        return False

    try:
        number = float(value)
    except (TypeError, ValueError):
        return False

    return math.isfinite(number) and number >= 0.0


def _strict_nonnegative_int(value: object) -> bool:
    return (
        isinstance(value, int)
        and not isinstance(value, bool)
        and value >= 0
    )


def _strict_positive_int(value: object) -> bool:
    return (
        isinstance(value, int)
        and not isinstance(value, bool)
        and value > 0
    )


def _reservation_id(
    request: StrategyBRiskAuthorizationRequest,
) -> str:
    # Identity deliberately excludes mutable market/risk numbers.
    # One causal selected expression owns one reservation identity.
    canonical = "\x1f".join(
        (
            "STRATEGY_B",
            _text(request.uid),
            _text(request.profile_id),
            _text(request.thesis_id),
            _text(request.selected_capture_id),
            _text(request.symbol).upper(),
            _text(request.side).upper(),
        )
    )

    digest = hashlib.sha256(
        canonical.encode("utf-8")
    ).hexdigest()

    return f"strategy_b:{digest}"


def _same_reservation_identity(
    existing: StrategyBRiskReservation,
    request: StrategyBRiskAuthorizationRequest,
) -> bool:
    return (
        existing.reservation_id == _reservation_id(request)
        and existing.strategy == "B"
        and existing.uid == _text(request.uid)
        and existing.profile_id == _text(request.profile_id)
        and existing.thesis_id == _text(request.thesis_id)
        and existing.selected_capture_id
        == _text(request.selected_capture_id)
        and existing.symbol == _text(request.symbol).upper()
        and existing.side == _text(request.side).upper()
        and existing.device_id == _text(request.device_id)
        and existing.account_login == _text(request.account_login)
        and existing.account_server == _text(request.account_server)
        and existing.broker_company == _text(request.broker_company)
        and existing.status == "RESERVED"
    )


def _blocked(
    reason: str,
) -> StrategyBRiskAuthorizationResult:
    return StrategyBRiskAuthorizationResult(
        authorized=False,
        reason=reason,
        reservation=None,
    )


def authorize_and_reserve_strategy_b_risk(
    *,
    request: StrategyBRiskAuthorizationRequest,
    capacity: StrategyBRiskCapacity,
    existing_reservation: Optional[StrategyBRiskReservation],
) -> StrategyBRiskAuthorizationResult:
    """
    Pure authorization decision.

    Despite the function name, this function does not persist anything.
    A later atomic persistence layer must revalidate authoritative capacity
    and commit the returned reservation before any order becomes eligible.
    """

    # ------------------------------------------------------------
    # Profile/account boundary
    # ------------------------------------------------------------
    if not bool(capacity.profile_enabled):
        return _blocked("PROFILE_DISABLED")

    if not _text(capacity.profile_id):
        return _blocked("PROFILE_ID_INVALID")

    if not _text(capacity.device_id):
        return _blocked("DEVICE_ID_INVALID")

    if (
        not _text(capacity.account_login)
        or not _text(capacity.account_server)
        or not _text(capacity.broker_company)
    ):
        return _blocked("ACCOUNT_IDENTITY_INVALID")

    # ------------------------------------------------------------
    # Canonical identity
    # ------------------------------------------------------------
    if (
        _text(request.uid) != _text(capacity.uid)
        or _text(request.profile_id) != _text(capacity.profile_id)
        or _text(request.device_id) != _text(capacity.device_id)
        or _text(request.account_login)
        != _text(capacity.account_login)
        or _text(request.account_server)
        != _text(capacity.account_server)
        or _text(request.broker_company)
        != _text(capacity.broker_company)
    ):
        return _blocked("CAPACITY_IDENTITY_MISMATCH")

    if (
        not _text(request.thesis_id)
        or not _text(request.selected_capture_id)
        or not _text(request.symbol)
        or _text(request.side).upper() not in {"BUY", "SELL"}
    ):
        return _blocked("REQUEST_IDENTITY_INVALID")

    # ------------------------------------------------------------
    # Capacity time validity
    # ------------------------------------------------------------
    if (
        not _strict_positive_int(capacity.snapshot_at_ms)
        or not _strict_positive_int(capacity.now_ms)
        or not _strict_nonnegative_int(capacity.snapshot_max_age_ms)
    ):
        return _blocked("RISK_CAPACITY_TIME_INVALID")

    if capacity.snapshot_at_ms > capacity.now_ms:
        return _blocked("RISK_CAPACITY_TIME_INVALID")

    if (
        capacity.now_ms - capacity.snapshot_at_ms
        > capacity.snapshot_max_age_ms
    ):
        return _blocked("RISK_CAPACITY_STALE")

    # ------------------------------------------------------------
    # Requested risk validity
    # ------------------------------------------------------------
    if not _finite_positive(request.risk_usd):
        return _blocked("RISK_USD_INVALID")

    if not _finite_positive(request.lots):
        return _blocked("LOTS_INVALID")

    if not _finite_positive(request.risk_pct):
        return _blocked("RISK_PCT_INVALID")

    if (
        not _finite_positive(request.entry)
        or not _finite_positive(request.sl)
        or not _finite_positive(request.tp)
    ):
        return _blocked("PRICE_INPUT_INVALID")

    # ------------------------------------------------------------
    # Existing reservation: immutable/idempotent ownership
    # ------------------------------------------------------------
    if existing_reservation is not None:
        if not _same_reservation_identity(
            existing_reservation,
            request,
        ):
            return _blocked("RESERVATION_IDENTITY_CONFLICT")

        return StrategyBRiskAuthorizationResult(
            authorized=True,
            reason="IDENTICAL_RESERVATION",
            reservation=existing_reservation,
        )

    # ------------------------------------------------------------
    # Capacity numeric validity
    # ------------------------------------------------------------
    nonnegative_values = (
        capacity.daily_loss_used_usd,
        capacity.daily_loss_limit_usd,
        capacity.max_loss_used_usd,
        capacity.max_loss_limit_usd,
        capacity.open_risk_usd,
        capacity.max_open_risk_usd,
    )

    if not all(
        _finite_nonnegative(v)
        for v in nonnegative_values
    ):
        return _blocked("RISK_CAPACITY_INVALID")

    if not _finite_positive(capacity.account_size):
        return _blocked("RISK_CAPACITY_INVALID")

    if (
        not _strict_nonnegative_int(capacity.open_positions_count)
        or not _strict_positive_int(capacity.max_open_positions)
    ):
        return _blocked("RISK_CAPACITY_INVALID")

    risk_usd = float(request.risk_usd)

    # ------------------------------------------------------------
    # Projected account/profile limits
    # ------------------------------------------------------------
    if (
        float(capacity.daily_loss_used_usd) + risk_usd
        > float(capacity.daily_loss_limit_usd)
    ):
        return _blocked("DAILY_LOSS_LIMIT_EXCEEDED")

    if (
        float(capacity.max_loss_used_usd) + risk_usd
        > float(capacity.max_loss_limit_usd)
    ):
        return _blocked("MAX_LOSS_LIMIT_EXCEEDED")

    if capacity.open_positions_count >= capacity.max_open_positions:
        return _blocked("MAX_OPEN_POSITIONS_REACHED")

    if (
        float(capacity.open_risk_usd) + risk_usd
        > float(capacity.max_open_risk_usd)
    ):
        return _blocked("MAX_OPEN_RISK_EXCEEDED")

    reservation = StrategyBRiskReservation(
        reservation_id=_reservation_id(request),
        strategy="B",

        uid=_text(request.uid),
        profile_id=_text(request.profile_id),

        thesis_id=_text(request.thesis_id),
        selected_capture_id=_text(request.selected_capture_id),

        symbol=_text(request.symbol).upper(),
        side=_text(request.side).upper(),

        entry=float(request.entry),
        sl=float(request.sl),
        tp=float(request.tp),

        lots=float(request.lots),
        risk_usd=risk_usd,
        risk_pct=float(request.risk_pct),

        device_id=_text(request.device_id),
        account_login=_text(request.account_login),
        account_server=_text(request.account_server),
        broker_company=_text(request.broker_company),

        status="RESERVED",
        reserved_at_ms=capacity.now_ms,
    )

    return StrategyBRiskAuthorizationResult(
        authorized=True,
        reason="RESERVED",
        reservation=reservation,
    )
