"""Day-3 Shared Risk Evaluation Engine.

    PURE . READ-ONLY . REPEATABLE . NON-AUTHORITATIVE

    RISK_EVALUATION_PASS IS NOT AUTHORIZATION.

evaluate() != authorize_and_reserve(). This module cannot authorize, cannot
reserve, and does not import the risk authority.

Concurrency: evaluation is NOT concurrency-safe authorization. Two trades of
$400 each can both PASS against the same snapshot with $500 of capacity. That
is expected: evaluation reads a snapshot, it does not claim capacity. Preventing
both from executing is the job of a future atomic authorize-and-reserve step.

First-failure evaluation order (pinned by tests):
   1. input validity         BLOCK_INVALID_RISK_INPUT
   2. account/profile match  BLOCK_ACCOUNT_MISMATCH
   3. snapshot freshness     STALE_ACCOUNT_STATE
   4. account halt           BLOCK_ACCOUNT_HALT
   5. daily halt             BLOCK_DAILY_HALT
   6. per-trade risk         BLOCK_TRADE_RISK
   7. max positions          BLOCK_MAX_POSITIONS
   8. daily loss             BLOCK_DAILY_LIMIT   (skipped if daily_loss_model NONE)
   9. maximum loss           BLOCK_MAX_LOSS      (skipped if max_loss_model NONE)
  10. open risk              BLOCK_OPEN_RISK
  11.                        RISK_EVALUATION_PASS

Freshness: fresh iff snapshot.as_of <= evaluated_at and
evaluated_at - snapshot.as_of <= config.max_snapshot_age (age equal to the
maximum is fresh; any future snapshot is stale).
"""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal, localcontext
from typing import ClassVar, Optional

from ..domain.enums import LossModel
from ..domain.thesis import ChildExecutionIntent
from .calculator import (
    RISK_DECIMAL_CONTEXT,
    daily_loss_check,
    max_loss_check,
    max_positions_check,
    open_risk_check,
    trade_risk_check,
)
from .contracts import (
    CAPACITY_BLOCK_CODES,
    CapacityCheck,
    CapacityCheckName as N,
    RiskEvaluation,
    RiskEvaluationCode as C,
    RiskEvaluationConfig,
    RiskEvaluationLimits,
    RiskEvaluationRequest,
    RiskEvaluationSnapshot,
)
from .validation import RiskInputValidationError, config_errors, is_aware, is_finite_decimal, risk_input_errors

PASS_REASON = ("supplied snapshot appears to have sufficient capacity; "
               "RISK_EVALUATION_PASS IS NOT AUTHORIZATION and reserves nothing")


def _require_types(request, snapshot, limits, config) -> None:
    for obj, typ, name in ((request, RiskEvaluationRequest, "request"), (snapshot, RiskEvaluationSnapshot, "snapshot"),
                           (limits, RiskEvaluationLimits, "limits"), (config, RiskEvaluationConfig, "config")):
        if not isinstance(obj, typ):
            raise TypeError(f"{name} must be a {typ.__name__}, got {type(obj).__name__}")
    if not isinstance(request.intent, ChildExecutionIntent):
        raise TypeError("request.intent must be a ChildExecutionIntent")


def evaluate_risk(request: RiskEvaluationRequest, snapshot: RiskEvaluationSnapshot,
                  limits: RiskEvaluationLimits, config: RiskEvaluationConfig) -> RiskEvaluation:
    """Pure function. Same inputs always return an equal RiskEvaluation."""
    _require_types(request, snapshot, limits, config)
    intent = request.intent

    def result(code: C, reason: str, *, checks: tuple[CapacityCheck, ...] = (),
               skipped: tuple[N, ...] = (), decisive: Optional[CapacityCheck] = None,
               errors: tuple[str, ...] = ()) -> RiskEvaluation:
        version = limits.source_policy_version
        return RiskEvaluation(
            thesis_id=intent.thesis_id,
            child_trade_id=intent.child_trade_id,
            strategy_id=intent.strategy_id,
            profile_id=intent.profile_id,
            account=intent.account,
            canonical_symbol=intent.canonical_symbol,
            side=intent.side,
            code=code,
            reason=reason,
            evaluated_at=request.evaluated_at if is_aware(request.evaluated_at) else None,
            snapshot_at=snapshot.as_of if is_aware(snapshot.as_of) else None,
            proposed_risk_usd=request.proposed_risk_usd if is_finite_decimal(request.proposed_risk_usd) else None,
            source_policy_version=version if isinstance(version, str) and version.strip() else None,
            checks=checks,
            skipped_checks=skipped,
            decisive_check=decisive,
            input_errors=errors,
        )

    # 1. input validity
    errors = risk_input_errors(request, snapshot, limits, config)
    if errors:
        return result(C.BLOCK_INVALID_RISK_INPUT, "invalid risk input: " + "; ".join(errors), errors=tuple(errors))

    # 2. identity
    if snapshot.profile_id != intent.profile_id:
        return result(C.BLOCK_ACCOUNT_MISMATCH,
                      f"snapshot profile {snapshot.profile_id} != request profile {intent.profile_id}")
    if limits.profile_id != intent.profile_id:
        return result(C.BLOCK_ACCOUNT_MISMATCH,
                      f"limits profile {limits.profile_id} != request profile {intent.profile_id}")
    if snapshot.account != intent.account:
        return result(C.BLOCK_ACCOUNT_MISMATCH, "snapshot account identity != request account identity")

    # 3. freshness
    at, as_of = request.evaluated_at, snapshot.as_of
    if as_of > at:
        return result(C.STALE_ACCOUNT_STATE, f"snapshot at {as_of.isoformat()} is after evaluation at {at.isoformat()}")
    age = at - as_of
    if age > config.max_snapshot_age:
        return result(C.STALE_ACCOUNT_STATE, f"snapshot age {age} exceeds max_snapshot_age {config.max_snapshot_age}")

    # 4-5. halts (never cleared here)
    if snapshot.account_halted:
        return result(C.BLOCK_ACCOUNT_HALT, "account is halted")
    if snapshot.daily_halted:
        return result(C.BLOCK_DAILY_HALT, "daily trading is halted")

    # 6-10. capacity
    proposed = request.proposed_risk_usd
    steps = (
        (N.TRADE_RISK, True, lambda: trade_risk_check(limits, proposed)),
        (N.MAX_POSITIONS, True, lambda: max_positions_check(snapshot, limits)),
        (N.DAILY_LOSS, limits.daily_loss_model is not LossModel.NONE, lambda: daily_loss_check(snapshot, limits, proposed)),
        (N.MAX_LOSS, limits.max_loss_model is not LossModel.NONE, lambda: max_loss_check(snapshot, limits, proposed)),
        (N.OPEN_RISK, True, lambda: open_risk_check(snapshot, limits, proposed)),
    )
    checks: list[CapacityCheck] = []
    skipped: list[N] = []
    try:
        with localcontext(RISK_DECIMAL_CONTEXT):
            for name, applicable, compute in steps:
                if not applicable:
                    skipped.append(name)
                    continue
                check = compute()
                checks.append(check)
                if not check.fits:
                    return result(
                        CAPACITY_BLOCK_CODES[name],
                        f"{name}: projected {check.projected} exceeds limit {check.limit} "
                        f"(used {check.used}, proposed {check.proposed})",
                        checks=tuple(checks), skipped=tuple(skipped), decisive=check)
    except ArithmeticError as exc:
        reason = f"decimal arithmetic failed closed ({type(exc).__name__})"
        return result(C.BLOCK_INVALID_RISK_INPUT, reason, errors=(reason,))

    return result(C.RISK_EVALUATION_PASS, PASS_REASON, checks=tuple(checks), skipped=tuple(skipped))


class RiskEvaluationEngine:
    """Holds immutable configuration only. RISK_EVALUATION_PASS IS NOT AUTHORIZATION."""

    GRANTS_RISK_AUTHORITY: ClassVar[bool] = False

    def __init__(self, config: RiskEvaluationConfig) -> None:
        if not isinstance(config, RiskEvaluationConfig):
            raise TypeError("config must be a RiskEvaluationConfig")
        errors = config_errors(config)
        if errors:
            raise RiskInputValidationError(errors)
        self._config = config

    @property
    def config(self) -> RiskEvaluationConfig:
        return self._config

    def evaluate(self, request: RiskEvaluationRequest, snapshot: RiskEvaluationSnapshot,
                 limits: RiskEvaluationLimits) -> RiskEvaluation:
        return evaluate_risk(request, snapshot, limits, self._config)
