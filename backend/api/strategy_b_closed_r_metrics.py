from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Optional


@dataclass(frozen=True)
class StrategyBClosedRMetrics:
    """
    Pure Strategy-B closed-position R metrics.

    R is defined from broker fill to the immutable initial stop:
        abs(broker_fill_price - original_sl_price)

    No Redis, broker, lifecycle, execution, or Strategy-A authority.
    """

    data_ok: bool
    reason: str
    initial_risk_distance: Optional[float] = None
    net_r: Optional[float] = None
    mfe_r: Optional[float] = None
    mae_r: Optional[float] = None


def _finite_positive(value: object) -> Optional[float]:
    try:
        x = float(value)
    except (TypeError, ValueError):
        return None

    if not math.isfinite(x) or x <= 0.0:
        return None

    return x


def build_strategy_b_closed_r_metrics(
    *,
    side,
    broker_fill_price,
    original_sl_price,
    exit_price,
    observed_price_min,
    observed_price_max,
) -> StrategyBClosedRMetrics:
    """
    Calculate final Strategy-B R metrics from broker/position evidence only.

    BUY:
        net_r = (exit - fill) / R
        mfe_r = max(0, observed_max - fill) / R
        mae_r = max(0, fill - observed_min) / R

    SELL:
        net_r = (fill - exit) / R
        mfe_r = max(0, fill - observed_min) / R
        mae_r = max(0, observed_max - fill) / R
    """

    side_u = str(side or "").strip().upper()
    if side_u not in {"BUY", "SELL"}:
        return StrategyBClosedRMetrics(
            data_ok=False,
            reason="INVALID_SIDE",
        )

    fill = _finite_positive(broker_fill_price)
    if fill is None:
        return StrategyBClosedRMetrics(
            data_ok=False,
            reason="INVALID_BROKER_FILL_PRICE",
        )

    initial_sl = _finite_positive(original_sl_price)
    if initial_sl is None:
        return StrategyBClosedRMetrics(
            data_ok=False,
            reason="INVALID_ORIGINAL_SL_PRICE",
        )

    exit_px = _finite_positive(exit_price)
    if exit_px is None:
        return StrategyBClosedRMetrics(
            data_ok=False,
            reason="INVALID_EXIT_PRICE",
        )

    observed_min = _finite_positive(observed_price_min)
    if observed_min is None:
        return StrategyBClosedRMetrics(
            data_ok=False,
            reason="INVALID_OBSERVED_PRICE_MIN",
        )

    observed_max = _finite_positive(observed_price_max)
    if observed_max is None:
        return StrategyBClosedRMetrics(
            data_ok=False,
            reason="INVALID_OBSERVED_PRICE_MAX",
        )

    if observed_min > observed_max:
        return StrategyBClosedRMetrics(
            data_ok=False,
            reason="INVALID_OBSERVED_PRICE_RANGE",
        )

    initial_risk = abs(fill - initial_sl)
    if not math.isfinite(initial_risk) or initial_risk <= 0.0:
        return StrategyBClosedRMetrics(
            data_ok=False,
            reason="ZERO_INITIAL_RISK_DISTANCE",
        )

    if side_u == "BUY":
        net_r = (exit_px - fill) / initial_risk
        mfe_r = max(0.0, observed_max - fill) / initial_risk
        mae_r = max(0.0, fill - observed_min) / initial_risk
    else:
        net_r = (fill - exit_px) / initial_risk
        mfe_r = max(0.0, fill - observed_min) / initial_risk
        mae_r = max(0.0, observed_max - fill) / initial_risk

    if not all(
        math.isfinite(x)
        for x in (net_r, mfe_r, mae_r)
    ):
        return StrategyBClosedRMetrics(
            data_ok=False,
            reason="NONFINITE_R_METRICS",
        )

    return StrategyBClosedRMetrics(
        data_ok=True,
        reason="OK",
        initial_risk_distance=float(initial_risk),
        net_r=float(net_r),
        mfe_r=float(mfe_r),
        mae_r=float(mae_r),
    )


__all__ = [
    "StrategyBClosedRMetrics",
    "build_strategy_b_closed_r_metrics",
]
