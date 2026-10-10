"""Pure Day-3 capacity calculations.

Decimal only. All arithmetic runs inside RISK_DECIMAL_CONTEXT (see evaluator),
which is independent of the caller's thread-local decimal context and traps
any inexact result, so precision loss fails closed instead of rounding.

Canonical boundary rule for EVERY check:  fits  <=>  projected <= limit
(consuming capacity exactly to the limit fits; exceeding it by any amount blocks).

Formulas (fc = floating component, see floating_component):
  TRADE_RISK     projected = proposed_risk                         limit = max_risk_per_trade
  MAX_POSITIONS  used      = open_positions + reserved_position_slots
                 projected = used + 1                              limit = max_positions
                 (reserved slots are slot commitments, not positions)
  DAILY_LOSS     current   = max(0, -(realized_daily_pnl + fc_daily))
                 used      = current + open_risk
                 projected = used + proposed_risk                  limit = daily_loss_limit
  MAX_LOSS       current   = max(0, reference - (balance + fc_max))
                 used      = current + open_risk
                 projected = used + proposed_risk                  limit = max_loss_limit
  OPEN_RISK      used      = open_risk
                 projected = used + proposed_risk                  limit = max_open_risk
"""
from __future__ import annotations

from decimal import ROUND_HALF_EVEN, Context, Decimal, DivisionByZero, Inexact, InvalidOperation, Overflow
from typing import Optional

from .contracts import (
    CapacityCheck,
    CapacityCheckName as N,
    CapacityUnit,
    FloatingPnlTreatment as F,
    RiskEvaluationLimits,
    RiskEvaluationSnapshot,
)

ZERO = Decimal("0")
ONE = Decimal("1")

RISK_DECIMAL_CONTEXT = Context(
    prec=34, rounding=ROUND_HALF_EVEN, Emin=-999999, Emax=999999,
    capitals=1, clamp=0, flags=[], traps=[InvalidOperation, DivisionByZero, Overflow, Inexact],
)


def floating_component(floating: Decimal, treatment: F) -> Decimal:
    if treatment is F.EXCLUDED:
        return ZERO
    if treatment is F.LOSSES_ONLY:
        return min(floating, ZERO)
    if treatment is F.FULL:
        return floating
    raise ValueError(f"unsupported floating treatment {treatment!r}")


def build_check(name: N, unit: CapacityUnit, limit: Decimal, used: Optional[Decimal], proposed: Decimal,
                components: tuple[tuple[str, Decimal], ...] = ()) -> CapacityCheck:
    projected = proposed if used is None else used + proposed
    remaining_before = limit if used is None else limit - used
    return CapacityCheck(
        name=name, unit=unit, limit=limit, used=used, proposed=proposed, projected=projected,
        remaining_before=remaining_before, remaining_after=limit - projected,
        fits=projected <= limit, components=components,
    )


def trade_risk_check(limits: RiskEvaluationLimits, proposed: Decimal) -> CapacityCheck:
    return build_check(N.TRADE_RISK, CapacityUnit.ACCOUNT_CURRENCY, limits.max_risk_per_trade_usd, None, proposed)


def max_positions_check(snapshot: RiskEvaluationSnapshot, limits: RiskEvaluationLimits) -> CapacityCheck:
    open_positions = Decimal(snapshot.open_positions)
    reserved_slots = Decimal(snapshot.reserved_position_slots)
    return build_check(N.MAX_POSITIONS, CapacityUnit.POSITIONS, Decimal(limits.max_positions),
                       open_positions + reserved_slots, ONE,
                       (("open_positions", open_positions), ("reserved_position_slots", reserved_slots)))


def daily_loss_check(snapshot: RiskEvaluationSnapshot, limits: RiskEvaluationLimits,
                     proposed: Decimal) -> CapacityCheck:
    floating = snapshot.equity - snapshot.balance
    fc = floating_component(floating, limits.daily_floating_pnl)
    current_loss = max(ZERO, -(snapshot.realized_daily_pnl + fc))
    used = current_loss + snapshot.open_risk_usd
    return build_check(N.DAILY_LOSS, CapacityUnit.ACCOUNT_CURRENCY, limits.daily_loss_limit_usd, used, proposed, (
        ("realized_daily_pnl", snapshot.realized_daily_pnl),
        ("floating_pnl", floating),
        ("floating_pnl_counted", fc),
        ("current_daily_loss", current_loss),
        ("existing_open_risk", snapshot.open_risk_usd),
    ))


def max_loss_check(snapshot: RiskEvaluationSnapshot, limits: RiskEvaluationLimits,
                   proposed: Decimal) -> CapacityCheck:
    reference = snapshot.max_loss_reference_balance
    floating = snapshot.equity - snapshot.balance
    fc = floating_component(floating, limits.max_loss_floating_pnl)
    current_value = snapshot.balance + fc
    current_loss = max(ZERO, reference - current_value)
    used = current_loss + snapshot.open_risk_usd
    return build_check(N.MAX_LOSS, CapacityUnit.ACCOUNT_CURRENCY, limits.max_loss_limit_usd, used, proposed, (
        ("reference_balance", reference),
        ("balance", snapshot.balance),
        ("floating_pnl", floating),
        ("floating_pnl_counted", fc),
        ("current_max_loss", current_loss),
        ("existing_open_risk", snapshot.open_risk_usd),
    ))


def open_risk_check(snapshot: RiskEvaluationSnapshot, limits: RiskEvaluationLimits,
                    proposed: Decimal) -> CapacityCheck:
    return build_check(N.OPEN_RISK, CapacityUnit.ACCOUNT_CURRENCY, limits.max_open_risk_usd,
                       snapshot.open_risk_usd, proposed, (("existing_open_risk", snapshot.open_risk_usd),))
