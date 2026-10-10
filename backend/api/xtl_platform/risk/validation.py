"""Fail-closed validation of Day-3 risk evaluation inputs.

Pure functions: no I/O, no environment, no clock. Every function returns a list
of errors (empty means valid) and never raises for bad values.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from typing import Optional

from ..domain._validation import ContractError
from ..domain.account import AccountIdentity
from ..domain.enums import LossModel
from ..domain.thesis import ChildExecutionIntent
from .contracts import (
    SUPPORTED_DAILY_LOSS_MODELS,
    SUPPORTED_MAX_LOSS_MODELS,
    FloatingPnlTreatment,
    RiskEvaluationConfig,
    RiskEvaluationLimits,
    RiskEvaluationRequest,
    RiskEvaluationSnapshot,
)

_ZERO = Decimal("0")
_HUNDRED = Decimal("100")


class RiskInputValidationError(ContractError):
    def __init__(self, errors: list[str]) -> None:
        self.errors = tuple(errors)
        super().__init__("invalid risk input: " + "; ".join(self.errors))


def is_finite_decimal(v: object) -> bool:
    return isinstance(v, Decimal) and v.is_finite()


def is_aware(v: object) -> bool:
    return isinstance(v, datetime) and v.tzinfo is not None and v.utcoffset() is not None


def _text(errors: list[str], name: str, v: object) -> None:
    if not isinstance(v, str) or not v.strip():
        errors.append(f"{name} must be a non-empty string")


def _decimal(errors: list[str], name: str, v: object, *, positive=False, non_negative=False) -> None:
    if not is_finite_decimal(v):
        errors.append(f"{name} must be a finite Decimal (got {type(v).__name__})")
    elif positive and v <= _ZERO:
        errors.append(f"{name} must be > 0")
    elif non_negative and v < _ZERO:
        errors.append(f"{name} must be >= 0")


def _int(errors: list[str], name: str, v: object, minimum: int) -> None:
    if type(v) is not int or v < minimum:
        errors.append(f"{name} must be an int >= {minimum}")


def _bool(errors: list[str], name: str, v: object) -> None:
    if type(v) is not bool:
        errors.append(f"{name} must be a bool")


def config_errors(config: RiskEvaluationConfig) -> list[str]:
    errors: list[str] = []
    if not isinstance(config.max_snapshot_age, timedelta) or config.max_snapshot_age <= timedelta(0):
        errors.append("config.max_snapshot_age must be a positive timedelta")
    return errors


def request_errors(request: RiskEvaluationRequest) -> list[str]:
    errors: list[str] = []
    if not isinstance(request.intent, ChildExecutionIntent):
        errors.append("request.intent must be a ChildExecutionIntent")
    _decimal(errors, "request.proposed_risk_usd", request.proposed_risk_usd, positive=True)
    pct = request.proposed_risk_pct
    if pct is not None:
        if not is_finite_decimal(pct):
            errors.append("request.proposed_risk_pct must be None or a finite Decimal")
        elif not (_ZERO < pct <= _HUNDRED):
            errors.append("request.proposed_risk_pct must be > 0 and <= 100")
    if not is_aware(request.evaluated_at):
        errors.append("request.evaluated_at must be a timezone-aware datetime")
    return errors


def snapshot_errors(s: RiskEvaluationSnapshot) -> list[str]:
    errors: list[str] = []
    _text(errors, "snapshot.profile_id", s.profile_id)
    if not isinstance(s.account, AccountIdentity):
        errors.append("snapshot.account must be an AccountIdentity")
    if not is_aware(s.as_of):
        errors.append("snapshot.as_of must be a timezone-aware datetime")
    _decimal(errors, "snapshot.balance", s.balance, positive=True)
    _decimal(errors, "snapshot.equity", s.equity, non_negative=True)
    _decimal(errors, "snapshot.realized_daily_pnl", s.realized_daily_pnl)
    _decimal(errors, "snapshot.open_risk_usd", s.open_risk_usd, non_negative=True)
    _int(errors, "snapshot.open_positions", s.open_positions, 0)
    _int(errors, "snapshot.reserved_position_slots", s.reserved_position_slots, 0)
    _bool(errors, "snapshot.account_halted", s.account_halted)
    _bool(errors, "snapshot.daily_halted", s.daily_halted)
    if s.max_loss_reference_balance is not None:
        _decimal(errors, "snapshot.max_loss_reference_balance", s.max_loss_reference_balance, positive=True)
    return errors


def _loss_model_errors(errors: list[str], prefix: str, model: object, supported: frozenset,
                       limit: object, treatment: object) -> None:
    if not isinstance(model, LossModel):
        errors.append(f"limits.{prefix}_model must be a LossModel")
        return
    if model not in supported:
        errors.append(f"limits.{prefix}_model {model} is not supported by the generic evaluator")
        return
    if model is LossModel.NONE:
        if limit is not None or treatment is not None:
            errors.append(f"limits.{prefix} limit and floating treatment must be None when model is NONE")
        return
    if limit is None:
        errors.append(f"limits.{prefix}_limit_usd is required for {model}")
    else:
        _decimal(errors, f"limits.{prefix}_limit_usd", limit, positive=True)
    if not isinstance(treatment, FloatingPnlTreatment):
        errors.append(f"limits.{prefix} floating PnL treatment is required for {model}")
    elif model in (LossModel.EQUITY_BASED, LossModel.TRAILING_INTRADAY) and treatment is FloatingPnlTreatment.EXCLUDED:
        errors.append(f"limits.{prefix} model {model} cannot exclude floating PnL")


def limits_errors(limits: RiskEvaluationLimits) -> list[str]:
    errors: list[str] = []
    _text(errors, "limits.profile_id", limits.profile_id)
    _text(errors, "limits.source_policy_version", limits.source_policy_version)
    _decimal(errors, "limits.max_risk_per_trade_usd", limits.max_risk_per_trade_usd, positive=True)
    _int(errors, "limits.max_positions", limits.max_positions, 1)
    _decimal(errors, "limits.max_open_risk_usd", limits.max_open_risk_usd, positive=True)
    _loss_model_errors(errors, "daily_loss", limits.daily_loss_model, SUPPORTED_DAILY_LOSS_MODELS,
                       limits.daily_loss_limit_usd, limits.daily_floating_pnl)
    _loss_model_errors(errors, "max_loss", limits.max_loss_model, SUPPORTED_MAX_LOSS_MODELS,
                       limits.max_loss_limit_usd, limits.max_loss_floating_pnl)
    return errors


def cross_errors(snapshot: RiskEvaluationSnapshot, limits: RiskEvaluationLimits) -> list[str]:
    errors: list[str] = []
    if (isinstance(limits.max_loss_model, LossModel) and limits.max_loss_model is not LossModel.NONE
            and snapshot.max_loss_reference_balance is None):
        errors.append(f"snapshot.max_loss_reference_balance is required for max_loss_model {limits.max_loss_model}")
    return errors


def risk_input_errors(request: RiskEvaluationRequest, snapshot: RiskEvaluationSnapshot,
                      limits: RiskEvaluationLimits, config: RiskEvaluationConfig) -> list[str]:
    return (config_errors(config) + request_errors(request) + snapshot_errors(snapshot)
            + limits_errors(limits) + cross_errors(snapshot, limits))


def validate_risk_inputs(request, snapshot, limits, config) -> None:
    errors = risk_input_errors(request, snapshot, limits, config)
    if errors:
        raise RiskInputValidationError(errors)
