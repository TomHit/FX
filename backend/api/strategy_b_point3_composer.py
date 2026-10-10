from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Callable, Mapping

from api.strategy_b_execution_assignment import StrategyBExecutionAssignment
from api.strategy_b_prop_risk_adapter import build_strategy_b_prop_risk_input
from api.strategy_b_risk_proposal import (
    StrategyBRiskConfig,
    StrategyBRiskProposal,
    build_strategy_b_risk_proposal,
)


@dataclass(frozen=True)
class StrategyBPoint3Result:
    valid: bool
    decision: str
    reason: str

    assignment_valid: bool
    prop_risk_valid: bool
    structural_stop_ready: bool
    conversion_attempted: bool
    conversion_valid: bool

    proposed_stop_price: float | None
    proposed_risk_usd: float | None
    proposed_lots_raw: float | None

    risk_proposal: dict[str, Any] | None

    observation_only: bool = True
    risk_reservation_authority: bool = False
    broker_lot_normalization_authority: bool = False
    order_authority: bool = False
    execution_authority: bool = False
    strategy_a_mutation: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _result(
    *,
    valid: bool,
    decision: str,
    reason: str,
    assignment_valid: bool,
    prop_risk_valid: bool,
    structural_stop_ready: bool,
    conversion_attempted: bool,
    conversion_valid: bool,
    proposal: StrategyBRiskProposal | None,
) -> StrategyBPoint3Result:
    return StrategyBPoint3Result(
        valid=valid,
        decision=decision,
        reason=reason,
        assignment_valid=assignment_valid,
        prop_risk_valid=prop_risk_valid,
        structural_stop_ready=structural_stop_ready,
        conversion_attempted=conversion_attempted,
        conversion_valid=conversion_valid,
        proposed_stop_price=(
            proposal.proposed_stop_price if proposal else None
        ),
        proposed_risk_usd=(
            proposal.proposed_risk_usd if proposal else None
        ),
        proposed_lots_raw=(
            proposal.proposed_lots_raw if proposal else None
        ),
        risk_proposal=(
            proposal.to_dict() if proposal else None
        ),
    )


def compose_strategy_b_point3(
    *,
    assignment: StrategyBExecutionAssignment,
    prop_config: Mapping[str, Any] | None,
    risk_snapshot: Mapping[str, Any] | None,
    entry_price: Any,
    structural_invalidation_price: Any,
    structural_buffer_price: Any,
    loss_per_lot_fn: Callable[..., Mapping[str, Any]],
) -> StrategyBPoint3Result:
    """
    Pure Strategy-B Point-3 composition boundary.

    Caller owns:
      - UID/profile/device/account resolution,
      - read-only authoritative risk-snapshot retrieval,
      - supplying the authoritative loss-per-lot conversion primitive.

    This function does not:
      - read/write Redis,
      - reserve/release risk,
      - normalize broker lots,
      - create/submit orders,
      - contact MT5,
      - mutate Strategy A.

    Structural stop remains owned by strategy_b_risk_proposal.
    Conversion is not attempted unless that owner proves a usable stop.
    """

    if not isinstance(assignment, StrategyBExecutionAssignment):
        return _result(
            valid=False,
            decision="WAIT",
            reason="EXECUTION_ASSIGNMENT_MISSING",
            assignment_valid=False,
            prop_risk_valid=False,
            structural_stop_ready=False,
            conversion_attempted=False,
            conversion_valid=False,
            proposal=None,
        )

    if not assignment.valid:
        return _result(
            valid=False,
            decision="WAIT",
            reason="EXECUTION_ASSIGNMENT_INVALID",
            assignment_valid=False,
            prop_risk_valid=False,
            structural_stop_ready=False,
            conversion_attempted=False,
            conversion_valid=False,
            proposal=None,
        )

    prop_risk = build_strategy_b_prop_risk_input(
        prop_config=prop_config,
        risk_snapshot=risk_snapshot,
    )

    if not prop_risk.valid:
        return _result(
            valid=False,
            decision="WAIT",
            reason=prop_risk.reason,
            assignment_valid=True,
            prop_risk_valid=False,
            structural_stop_ready=False,
            conversion_attempted=False,
            conversion_valid=False,
            proposal=None,
        )

    cfg = StrategyBRiskConfig(
        structural_buffer_price=structural_buffer_price,
    )

    # First pass:
    # Let the existing risk proposal remain the sole owner of structural-stop
    # geometry. Conversion is intentionally unavailable in this pass.
    structural_probe = build_strategy_b_risk_proposal(
        symbol=assignment.symbol,
        side=assignment.side,
        entry_price=entry_price,
        structural_invalidation_price=structural_invalidation_price,
        account=prop_risk.account,
        shared_risk=prop_risk.shared_risk,
        instrument_conversion={
            "data_ok": False,
            "source": None,
            "loss_per_lot_at_stop": None,
        },
        config=cfg,
    )

    stop = structural_probe.proposed_stop_price

    if stop is None:
        return _result(
            valid=False,
            decision="WAIT",
            reason=structural_probe.reason,
            assignment_valid=True,
            prop_risk_valid=True,
            structural_stop_ready=False,
            conversion_attempted=False,
            conversion_valid=False,
            proposal=structural_probe,
        )

    try:
        conversion = loss_per_lot_fn(
            symbol=assignment.symbol,
            entry=float(entry_price),
            sl=float(stop),
        )
    except Exception:
        return _result(
            valid=False,
            decision="WAIT",
            reason="LOSS_PER_LOT_CONVERSION_EXCEPTION",
            assignment_valid=True,
            prop_risk_valid=True,
            structural_stop_ready=True,
            conversion_attempted=True,
            conversion_valid=False,
            proposal=structural_probe,
        )

    if not isinstance(conversion, Mapping):
        conversion = {
            "data_ok": False,
            "source": None,
            "loss_per_lot_at_stop": None,
        }

    final_proposal = build_strategy_b_risk_proposal(
        symbol=assignment.symbol,
        side=assignment.side,
        entry_price=entry_price,
        structural_invalidation_price=structural_invalidation_price,
        account=prop_risk.account,
        shared_risk=prop_risk.shared_risk,
        instrument_conversion=conversion,
        config=cfg,
    )

    conversion_valid = bool(
        conversion.get("data_ok") is True
        and final_proposal.loss_per_lot_at_stop is not None
        and final_proposal.loss_per_lot_at_stop > 0.0
    )

    return _result(
        valid=bool(final_proposal.valid),
        decision=final_proposal.decision,
        reason=final_proposal.reason,
        assignment_valid=True,
        prop_risk_valid=True,
        structural_stop_ready=True,
        conversion_attempted=True,
        conversion_valid=conversion_valid,
        proposal=final_proposal,
    )
