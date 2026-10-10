"""
Strategy-B read-only prop-risk capacity adapter.

Purpose
-------
Translate an already-produced canonical prop configuration and authoritative
prop-risk snapshot into the narrow account/shared-risk contract consumed by
strategy_b_risk_proposal.py.

This module does NOT:
- read or write Redis
- compute/refresh the canonical prop-risk snapshot
- reserve/release risk
- normalize broker lots
- place/modify/cancel orders
- call MT5
- mutate Strategy A
- grant execution authority
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Mapping


@dataclass(frozen=True)
class StrategyBPropRiskInput:
    account: dict[str, Any]
    shared_risk: dict[str, Any]
    valid: bool
    decision: str
    reason: str
    observation_only: bool = True
    execution_authority: bool = False
    strategy_a_mutation: bool = False


def _finite(value: Any) -> float | None:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {
        "1", "true", "yes", "on"
    }


def _wait(reason: str) -> StrategyBPropRiskInput:
    return StrategyBPropRiskInput(
        account={"equity": None},
        shared_risk={
            "data_ok": False,
            "new_risk_allowed": False,
            "available_risk_usd": 0.0,
        },
        valid=False,
        decision="WAIT",
        reason=reason,
    )


def build_strategy_b_prop_risk_input(
    *,
    prop_config: Mapping[str, Any] | None,
    risk_snapshot: Mapping[str, Any] | None,
) -> StrategyBPropRiskInput:
    """
    Adapt existing production authority into Strategy-B's narrow risk input.

    No production risk rules are invented here.  Capacity is the minimum
    remaining monetary room across the canonical limits represented by the
    supplied config/snapshot.  Non-monetary gates fail closed.
    """

    if not isinstance(prop_config, Mapping):
        return _wait("PROP_CONFIG_UNAVAILABLE")

    if not _truthy(prop_config.get("enabled")):
        return _wait("PROP_PROFILE_DISABLED")

    if not isinstance(risk_snapshot, Mapping):
        return _wait("PROP_RISK_SNAPSHOT_UNAVAILABLE")

    if not _truthy(risk_snapshot.get("snapshot_valid")):
        return _wait("PROP_RISK_SNAPSHOT_INVALID")

    if _truthy(risk_snapshot.get("trading_halted")):
        return _wait("PROP_TRADING_HALTED")

    if _truthy(risk_snapshot.get("daily_r_blocked")):
        return _wait("PROP_DAILY_R_BLOCKED")

    equity = _finite(risk_snapshot.get("broker_equity"))
    if equity is None or equity <= 0.0:
        return _wait("BROKER_EQUITY_INVALID")

    daily_limit = _finite(risk_snapshot.get("daily_loss_limit"))
    daily_used = _finite(risk_snapshot.get("daily_loss_used"))

    max_loss_limit = _finite(risk_snapshot.get("max_loss_limit"))
    max_loss_used = _finite(risk_snapshot.get("max_loss_used"))

    live_open_risk = _finite(
        risk_snapshot.get(
            "live_open_risk_usd",
            risk_snapshot.get("open_risk_usd"),
        )
    )

    max_open_risk_pct = _finite(prop_config.get("max_open_risk_pct"))

    open_positions = _finite(
        risk_snapshot.get(
            "live_open_positions_count",
            risk_snapshot.get("open_positions_count"),
        )
    )
    max_positions = _finite(prop_config.get("max_open_positions"))

    required = {
        "daily_loss_limit": daily_limit,
        "daily_loss_used": daily_used,
        "max_loss_limit": max_loss_limit,
        "max_loss_used": max_loss_used,
        "open_risk_usd": live_open_risk,
        "max_open_risk_pct": max_open_risk_pct,
        "open_positions": open_positions,
        "max_open_positions": max_positions,
    }

    if any(v is None for v in required.values()):
        return _wait("PROP_RISK_CAPACITY_FIELDS_MISSING")

    if (
        daily_limit <= 0.0
        or max_loss_limit <= 0.0
        or max_open_risk_pct <= 0.0
        or max_positions <= 0.0
        or daily_used < 0.0
        or max_loss_used < 0.0
        or live_open_risk < 0.0
        or open_positions < 0.0
    ):
        return _wait("PROP_RISK_CAPACITY_FIELDS_INVALID")

    if open_positions >= max_positions:
        return _wait("PROP_MAX_POSITIONS_REACHED")

    daily_room = max(0.0, daily_limit - daily_used)
    max_loss_room = max(0.0, max_loss_limit - max_loss_used)

    max_open_risk_usd = (
        equity * (max_open_risk_pct / 100.0)
    )
    open_risk_room = max(
        0.0,
        max_open_risk_usd - live_open_risk,
    )

    available = min(
        daily_room,
        max_loss_room,
        open_risk_room,
    )

    if available <= 0.0:
        return _wait("PROP_NO_SHARED_RISK_CAPACITY")

    return StrategyBPropRiskInput(
        account={
            "equity": float(equity),
        },
        shared_risk={
            "data_ok": True,
            "new_risk_allowed": True,
            "available_risk_usd": float(available),
        },
        valid=True,
        decision="READY",
        reason="PROP_SHARED_RISK_INPUT_READY",
    )
