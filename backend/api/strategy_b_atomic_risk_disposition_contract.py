from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Literal


Disposition = Literal[
    "RELEASED_BEFORE_FILL",
    "TRANSFERRED_TO_BROKER_RISK",
]

_MICRO_USD = Decimal("1000000")


@dataclass(frozen=True)
class StrategyBRiskDispositionCommitRequest:
    uid: str
    profile_id: str
    device_id: str
    account_login: str
    reservation_id: str
    disposition: Disposition
    decided_at_ms: int


@dataclass(frozen=True)
class StrategyBRiskDispositionReceipt:
    reservation_id: str
    uid: str
    profile_id: str
    device_id: str
    account_login: str
    disposition: Disposition
    stored_risk_usd: float
    stored_risk_micro_usd: int
    decided_at_ms: int
    status: str = "COMMITTED"


@dataclass(frozen=True)
class StrategyBRiskDispositionArithmetic:
    ok: bool
    reason: str
    stored_risk_micro_usd: int | None
    pending_total_before_micro_usd: int | None
    pending_total_after_micro_usd: int | None


def _clean(value: object, name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{name}_EMPTY")
    if ":" in text:
        raise ValueError(f"{name}_INVALID_SEPARATOR")
    return text


def _clean_reservation_id(value: object) -> str:
    text = str(value or "").strip()

    if not text:
        raise ValueError("RESERVATION_ID_EMPTY")

    prefix = "strategy_b:"
    digest = text[len(prefix):] if text.startswith(prefix) else ""

    if (
        not text.startswith(prefix)
        or len(digest) != 64
        or any(c not in "0123456789abcdef" for c in digest)
    ):
        raise ValueError("RESERVATION_ID_INVALID")

    return text


def validate_disposition_request(
    request: StrategyBRiskDispositionCommitRequest,
) -> StrategyBRiskDispositionCommitRequest:
    _clean(request.uid, "UID")
    _clean(request.profile_id, "PROFILE_ID")
    _clean(request.device_id, "DEVICE_ID")
    _clean(request.account_login, "ACCOUNT_LOGIN")
    _clean_reservation_id(request.reservation_id)

    if request.disposition not in {
        "RELEASED_BEFORE_FILL",
        "TRANSFERRED_TO_BROKER_RISK",
    }:
        raise ValueError("DISPOSITION_INVALID")

    if (
        isinstance(request.decided_at_ms, bool)
        or not isinstance(request.decided_at_ms, int)
        or request.decided_at_ms <= 0
    ):
        raise ValueError("DECIDED_AT_MS_INVALID")

    return request


def usd_to_micro_usd(value: object) -> int:
    try:
        d = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        raise ValueError("USD_INVALID")

    if not d.is_finite() or d < 0:
        raise ValueError("USD_INVALID")

    scaled = d * _MICRO_USD

    # Risk accounting must be representable exactly to micro-dollar
    # precision. Never silently truncate higher precision.
    integral = scaled.to_integral_value(rounding=ROUND_HALF_UP)

    if scaled != integral:
        raise ValueError("USD_PRECISION_EXCEEDED")

    return int(integral)


def compute_disposition_arithmetic(
    *,
    stored_risk_usd: object,
    pending_total_micro_usd: object,
) -> StrategyBRiskDispositionArithmetic:

    try:
        risk_micro = usd_to_micro_usd(stored_risk_usd)
    except ValueError as exc:
        return StrategyBRiskDispositionArithmetic(
            ok=False,
            reason=str(exc),
            stored_risk_micro_usd=None,
            pending_total_before_micro_usd=None,
            pending_total_after_micro_usd=None,
        )

    if (
        isinstance(pending_total_micro_usd, bool)
        or not isinstance(pending_total_micro_usd, int)
        or pending_total_micro_usd < 0
    ):
        return StrategyBRiskDispositionArithmetic(
            ok=False,
            reason="PENDING_TOTAL_INVALID",
            stored_risk_micro_usd=risk_micro,
            pending_total_before_micro_usd=None,
            pending_total_after_micro_usd=None,
        )

    if risk_micro > pending_total_micro_usd:
        return StrategyBRiskDispositionArithmetic(
            ok=False,
            reason="PENDING_TOTAL_UNDERFLOW",
            stored_risk_micro_usd=risk_micro,
            pending_total_before_micro_usd=pending_total_micro_usd,
            pending_total_after_micro_usd=None,
        )

    after = pending_total_micro_usd - risk_micro

    return StrategyBRiskDispositionArithmetic(
        ok=True,
        reason="OK",
        stored_risk_micro_usd=risk_micro,
        pending_total_before_micro_usd=pending_total_micro_usd,
        pending_total_after_micro_usd=after,
    )


def build_disposition_receipt(
    *,
    request: StrategyBRiskDispositionCommitRequest,
    stored_risk_usd: object,
) -> StrategyBRiskDispositionReceipt:

    validate_disposition_request(request)
    risk_micro = usd_to_micro_usd(stored_risk_usd)

    return StrategyBRiskDispositionReceipt(
        reservation_id=request.reservation_id,
        uid=request.uid,
        profile_id=request.profile_id,
        device_id=request.device_id,
        account_login=request.account_login,
        disposition=request.disposition,
        stored_risk_usd=float(Decimal(str(stored_risk_usd))),
        stored_risk_micro_usd=risk_micro,
        decided_at_ms=request.decided_at_ms,
    )
