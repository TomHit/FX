from __future__ import annotations

from datetime import datetime
from decimal import Decimal


class ContractError(ValueError):
    """Raised when a platform contract object is constructed with invalid data."""


def require_text(name: str, value: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ContractError(f"{name} must be a non-empty string")


def require_aware(name: str, value: datetime) -> None:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ContractError(f"{name} must be a timezone-aware datetime")


def require_decimal(name: str, value: Decimal, *, positive: bool = False, non_negative: bool = False) -> None:
    if not isinstance(value, Decimal) or not value.is_finite():
        raise ContractError(f"{name} must be a finite Decimal")
    if positive and value <= 0:
        raise ContractError(f"{name} must be > 0")
    if non_negative and value < 0:
        raise ContractError(f"{name} must be >= 0")
