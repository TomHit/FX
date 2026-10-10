"""Fail-closed FirmPolicy validation (Day 2, brief section 11).

`FirmPolicy.__post_init__` (Day 1) already rejects blank identity fields,
unknown timezones, reversed effective periods and missing loss limits at
construction. This module adds the strict type and range checks that every
policy must pass before it can be registered or evaluated.

Pure functions only: no I/O, no environment, no clock.
"""
from __future__ import annotations

import re
from datetime import date, datetime, time
from decimal import Decimal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from ..domain._validation import ContractError
from ..domain.enums import LossModel, Permission, Phase
from ..domain.policy import FirmPolicy

#: Canonical XTL instrument symbol, e.g. XAUUSD, EURUSD. Not a broker symbol.
CANONICAL_SYMBOL_PATTERN = re.compile(r"^[A-Z][A-Z0-9._]{1,31}$")
MAX_LIMIT_PCT = Decimal("100")

_TEXT_FIELDS = ("firm_id", "product", "program", "version", "source_url", "daily_reset_tz")
_PERMISSION_FIELDS = ("automation", "own_account_copy", "cross_firm_copy")
_BOOL_FIELDS = ("automation_verified", "allows_opposite_exposure_across_accounts")
_LOSS_RULES = (("daily_loss_model", "daily_loss_limit_pct"), ("max_loss_model", "max_loss_limit_pct"))


class PolicyValidationError(ContractError):
    def __init__(self, errors: list[str]) -> None:
        self.errors = tuple(errors)
        super().__init__("invalid FirmPolicy: " + "; ".join(self.errors))


def _is_plain_date(value: object) -> bool:
    # datetime is a subclass of date; effective periods are calendar dates only.
    return isinstance(value, date) and not isinstance(value, datetime)


def policy_validation_errors(policy: object) -> list[str]:
    """Return every validation error (empty list means valid). Never raises."""
    if not isinstance(policy, FirmPolicy):
        return [f"expected FirmPolicy, got {type(policy).__name__}"]
    errors: list[str] = []

    for name in _TEXT_FIELDS:
        v = getattr(policy, name)
        if not isinstance(v, str) or not v.strip():
            errors.append(f"{name} must be a non-empty string")
        elif v != v.strip():
            errors.append(f"{name} must not have leading or trailing whitespace")

    if not isinstance(policy.phase, Phase):
        errors.append("phase must be a Phase")
    for name in _PERMISSION_FIELDS:
        if not isinstance(getattr(policy, name), Permission):
            errors.append(f"{name} must be a Permission")
    for name in _BOOL_FIELDS:
        if type(getattr(policy, name)) is not bool:
            errors.append(f"{name} must be a bool")
    if policy.automation_verified is True and policy.automation is Permission.PROHIBITED:
        errors.append("automation_verified cannot be true when automation is PROHIBITED")

    if policy.max_accounts is not None and (type(policy.max_accounts) is not int or policy.max_accounts < 1):
        errors.append("max_accounts must be None or an int >= 1")

    if not _is_plain_date(policy.effective_from):
        errors.append("effective_from must be a date (not datetime)")
    if policy.effective_to is not None and not _is_plain_date(policy.effective_to):
        errors.append("effective_to must be None or a date (not datetime)")
    if (_is_plain_date(policy.effective_from) and _is_plain_date(policy.effective_to)
            and policy.effective_to < policy.effective_from):
        errors.append("effective_to precedes effective_from")

    if not isinstance(policy.daily_reset_time, time):
        errors.append("daily_reset_time must be a time")
    if isinstance(policy.daily_reset_tz, str) and policy.daily_reset_tz.strip():
        try:
            ZoneInfo(policy.daily_reset_tz)
        except (ZoneInfoNotFoundError, ValueError):
            errors.append(f"unknown daily_reset_tz {policy.daily_reset_tz!r}")

    for model_name, pct_name in _LOSS_RULES:
        model, pct = getattr(policy, model_name), getattr(policy, pct_name)
        if not isinstance(model, LossModel):
            errors.append(f"{model_name} must be a LossModel")
            continue
        if pct is None:
            if model is not LossModel.NONE:
                errors.append(f"{pct_name} is required when {model_name} is {model}")
            continue
        if model is LossModel.NONE:
            errors.append(f"{pct_name} must be None when {model_name} is NONE")
        if not isinstance(pct, Decimal) or not pct.is_finite():
            errors.append(f"{pct_name} must be a finite Decimal")
        elif not (Decimal("0") < pct <= MAX_LIMIT_PCT):
            errors.append(f"{pct_name} must be > 0 and <= {MAX_LIMIT_PCT}")

    symbols = policy.allowed_symbols
    if not isinstance(symbols, frozenset) or not symbols:
        errors.append("allowed_symbols must be a non-empty frozenset")
    else:
        bad = sorted(repr(s) for s in symbols
                     if not isinstance(s, str) or not CANONICAL_SYMBOL_PATTERN.match(s))
        if bad:
            errors.append(f"allowed_symbols contains invalid canonical symbols: {', '.join(bad)}")

    return errors


def validate_firm_policy(policy: object) -> None:
    errors = policy_validation_errors(policy)
    if errors:
        raise PolicyValidationError(errors)
