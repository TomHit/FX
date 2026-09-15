
"""
XauTrendLab Strategy B - Structural Risk Proposal
=================================================

Observation-only pure risk proposal.

This module does NOT reserve risk, round broker lots, enforce prop-firm limits,
or place orders. Shared account/prop risk authority and the existing broker
metadata/sizing path remain authoritative.

Contract:
- structural invalidation comes first;
- Strategy-B structural buffer must be supplied explicitly (calibrated upstream);
- default demo thesis request is 0.10% of account equity;
- final proposal risk is capped by shared risk authority;
- raw lots are derived only from an authoritative loss-per-lot-at-stop conversion;
- no fixed lots and no stretching the stop to fit size.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping, Optional


def _upper(v: Any) -> str:
    return str(v or "").strip().upper()


def _num(v: Any) -> Optional[float]:
    try:
        x = float(v)
        return x if x == x else None
    except Exception:
        return None


def _mapping(v: Any) -> Mapping[str, Any]:
    return v if isinstance(v, Mapping) else {}


@dataclass(frozen=True)
class StrategyBRiskConfig:
    # Source-of-truth demo baseline: 0.10% per macro thesis.
    demo_thesis_risk_pct: float = 0.001

    # Buffer is intentionally NOT defaulted. It must be supplied by calibrated
    # symbol/regime logic because the source-of-truth explicitly marks this as
    # a research/calibration item.
    structural_buffer_price: Optional[float] = None

    require_shared_risk_authority: bool = True
    require_authoritative_conversion: bool = True


@dataclass(frozen=True)
class StrategyBRiskProposal:
    schema_version: int
    model: str

    symbol: str
    side: str
    entry_price: Optional[float]
    structural_invalidation_price: Optional[float]
    structural_buffer_price: Optional[float]
    proposed_stop_price: Optional[float]
    stop_distance_price: Optional[float]

    account_equity: Optional[float]
    requested_thesis_risk_pct: float
    requested_thesis_risk_usd: Optional[float]

    shared_risk_available_usd: Optional[float]
    shared_risk_authority_ok: bool
    proposed_risk_usd: Optional[float]

    loss_per_lot_at_stop: Optional[float]
    conversion_source: Optional[str]
    proposed_lots_raw: Optional[float]

    valid: bool
    decision: str
    reason: str
    reasons: tuple[str, ...]

    risk_reservation_authority: bool = False
    broker_lot_normalization_authority: bool = False
    execution_authority: bool = False
    observation_only: bool = True
    strategy_a_mutation: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _structural_stop(
    *,
    side: str,
    entry_price: Optional[float],
    invalidation_price: Optional[float],
    buffer_price: Optional[float],
) -> tuple[Optional[float], Optional[float], str]:
    if side not in ("BUY", "SELL"):
        return None, None, "INVALID_SIDE"
    if entry_price is None or entry_price <= 0:
        return None, None, "ENTRY_PRICE_INVALID"
    if invalidation_price is None or invalidation_price <= 0:
        return None, None, "STRUCTURAL_INVALIDATION_MISSING"
    if buffer_price is None:
        return None, None, "STRUCTURAL_BUFFER_NOT_CALIBRATED"
    if buffer_price < 0:
        return None, None, "STRUCTURAL_BUFFER_INVALID"

    if side == "BUY":
        if invalidation_price >= entry_price:
            return None, None, "BUY_INVALIDATION_NOT_BELOW_ENTRY"
        stop = invalidation_price - buffer_price
        if stop <= 0 or stop >= entry_price:
            return None, None, "BUY_STOP_INVALID"
        distance = entry_price - stop
    else:
        if invalidation_price <= entry_price:
            return None, None, "SELL_INVALIDATION_NOT_ABOVE_ENTRY"
        stop = invalidation_price + buffer_price
        if stop <= entry_price:
            return None, None, "SELL_STOP_INVALID"
        distance = stop - entry_price

    return float(stop), float(distance), "STRUCTURAL_STOP_OK"


def build_strategy_b_risk_proposal(
    *,
    symbol: str,
    side: str,
    entry_price: Any,
    structural_invalidation_price: Any,
    account: Mapping[str, Any],
    shared_risk: Mapping[str, Any],
    instrument_conversion: Mapping[str, Any],
    config: Optional[StrategyBRiskConfig] = None,
) -> StrategyBRiskProposal:
    """
    Build an observation-only Strategy-B risk proposal.

    Expected shared_risk inputs:
      data_ok: bool
      new_risk_allowed: bool
      available_risk_usd: numeric

    Expected instrument_conversion inputs:
      data_ok: bool
      loss_per_lot_at_stop: numeric
      source: string

    `loss_per_lot_at_stop` should come from the existing authoritative
    broker/prop sizing path after applying symbol metadata/conversion to the
    proposed structural stop distance. Strategy B does not duplicate that math.
    """
    cfg = config or StrategyBRiskConfig()
    reasons: list[str] = []

    symbol_u = _upper(symbol)
    side_u = _upper(side)
    entry = _num(entry_price)
    invalidation = _num(structural_invalidation_price)

    stop, stop_distance, stop_reason = _structural_stop(
        side=side_u,
        entry_price=entry,
        invalidation_price=invalidation,
        buffer_price=cfg.structural_buffer_price,
    )
    reasons.append(stop_reason)

    acct = _mapping(account)
    equity = _num(acct.get("equity"))
    if equity is None or equity <= 0:
        requested_usd = None
        reasons.append("ACCOUNT_EQUITY_INVALID")
    else:
        requested_usd = float(equity) * float(cfg.demo_thesis_risk_pct)
        reasons.append("THESIS_RISK_REQUESTED_FROM_EQUITY")

    sr = _mapping(shared_risk)
    shared_data_ok = sr.get("data_ok") is True
    new_risk_allowed = sr.get("new_risk_allowed") is True
    shared_available = _num(sr.get("available_risk_usd"))

    shared_ok = bool(
        shared_data_ok
        and new_risk_allowed
        and shared_available is not None
        and shared_available > 0
    )

    if not shared_data_ok:
        reasons.append("SHARED_RISK_DATA_UNAVAILABLE")
    elif not new_risk_allowed:
        reasons.append("SHARED_RISK_BLOCKS_NEW_ENTRY")
    elif shared_available is None or shared_available <= 0:
        reasons.append("NO_SHARED_RISK_AVAILABLE")
    else:
        reasons.append("SHARED_RISK_AUTHORITY_OK")

    proposed_risk = None
    if requested_usd is not None:
        if cfg.require_shared_risk_authority:
            if shared_ok:
                proposed_risk = min(float(requested_usd), float(shared_available))
        else:
            proposed_risk = float(requested_usd)

    conv = _mapping(instrument_conversion)
    conv_ok = conv.get("data_ok") is True
    loss_per_lot = _num(conv.get("loss_per_lot_at_stop"))
    conversion_source = str(conv.get("source") or "").strip() or None

    if not conv_ok:
        reasons.append("INSTRUMENT_CONVERSION_UNAVAILABLE")
    elif loss_per_lot is None or loss_per_lot <= 0:
        reasons.append("LOSS_PER_LOT_AT_STOP_INVALID")
    else:
        reasons.append("AUTHORITATIVE_CONVERSION_OK")

    raw_lots = None
    if (
        proposed_risk is not None
        and proposed_risk > 0
        and loss_per_lot is not None
        and loss_per_lot > 0
        and (conv_ok or not cfg.require_authoritative_conversion)
    ):
        raw_lots = float(proposed_risk) / float(loss_per_lot)

    if stop is None or stop_distance is None:
        decision = "WAIT"
        reason = stop_reason
    elif requested_usd is None:
        decision = "WAIT"
        reason = "ACCOUNT_EQUITY_INVALID"
    elif cfg.require_shared_risk_authority and not shared_ok:
        decision = "WAIT"
        reason = (
            "SHARED_RISK_BLOCKS_NEW_ENTRY"
            if shared_data_ok and not new_risk_allowed
            else "SHARED_RISK_AUTHORITY_UNAVAILABLE"
        )
    elif proposed_risk is None or proposed_risk <= 0:
        decision = "WAIT"
        reason = "PROPOSED_RISK_NOT_POSITIVE"
    elif cfg.require_authoritative_conversion and (
        not conv_ok or loss_per_lot is None or loss_per_lot <= 0
    ):
        decision = "WAIT"
        reason = "AUTHORITATIVE_INSTRUMENT_CONVERSION_REQUIRED"
    elif raw_lots is None or raw_lots <= 0:
        decision = "WAIT"
        reason = "RAW_LOT_PROPOSAL_INVALID"
    else:
        decision = "RISK_PROPOSAL_READY_SHADOW"
        reason = "STRUCTURAL_STOP_AND_SHARED_RISK_VALID"

    valid = decision == "RISK_PROPOSAL_READY_SHADOW"

    return StrategyBRiskProposal(
        schema_version=1,
        model="STRATEGY_B_STRUCTURAL_RISK_PROPOSAL_V1",
        symbol=symbol_u,
        side=side_u,
        entry_price=entry,
        structural_invalidation_price=invalidation,
        structural_buffer_price=cfg.structural_buffer_price,
        proposed_stop_price=stop,
        stop_distance_price=stop_distance,
        account_equity=equity,
        requested_thesis_risk_pct=float(cfg.demo_thesis_risk_pct),
        requested_thesis_risk_usd=requested_usd,
        shared_risk_available_usd=shared_available,
        shared_risk_authority_ok=shared_ok,
        proposed_risk_usd=proposed_risk,
        loss_per_lot_at_stop=loss_per_lot,
        conversion_source=conversion_source,
        proposed_lots_raw=raw_lots,
        valid=valid,
        decision=decision,
        reason=reason,
        reasons=tuple(dict.fromkeys(reasons)),
    )


__all__ = [
    "StrategyBRiskConfig",
    "StrategyBRiskProposal",
    "build_strategy_b_risk_proposal",
]
