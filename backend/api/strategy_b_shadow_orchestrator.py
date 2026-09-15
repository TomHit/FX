
"""
XauTrendLab Strategy B - Shadow Orchestrator
============================================

Pure composition layer joining the validated Strategy-B decision modules.

No Redis, broker, order, modify, close, risk reservation, or Strategy-A mutation.

Responsibilities:
- compose pre-entry shadow decision from existing module outputs;
- compose post-entry thesis decision into canonical lifecycle intent;
- produce a deterministic legal transition plan rather than jumping states;
- keep ENTRY_READY / HOLD / PROTECT / PRE_EVENT_REVIEW / EXIT_REVIEW shadow-only;
- provide one replayable decision envelope for later persistence/manager wiring.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping, Optional

from api.strategy_b_lifecycle import (
    CANONICAL_STATES,
    compose_strategy_b_pretrade_shadow,
    transition_strategy_b_lifecycle,
)


_PRETRADE_SEQUENCE = (
    "OBSERVING",
    "MACRO_IMPULSE",
    "EXPANDING",
    "EXTENDED",
    "MATURE_AT_HTF_SR",
    "CATALYST_DETECTED",
    "REACTION_PENDING",
    "REVERSAL_CONFIRMED",
    "EXPRESSION_RANKING",
    "ENTRY_READY",
)

_POSTTRADE_STATES = {
    "TRADE_ACTIVE",
    "THESIS_STRONG",
    "THESIS_STABLE",
    "THESIS_WEAKENING",
    "THESIS_INVALID",
    "EXIT_PENDING",
    "CLOSED",
}


def _upper(v: Any) -> str:
    return str(v or "").strip().upper()


def _field(obj: Any, name: str, default: Any = None) -> Any:
    if isinstance(obj, Mapping):
        return obj.get(name, default)
    return getattr(obj, name, default)


@dataclass(frozen=True)
class StrategyBShadowTransitionPlan:
    from_state: str
    to_state: str
    reason: str
    valid: bool

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class StrategyBShadowDecision:
    schema_version: int
    model: str
    mode: str

    current_state: str
    derived_state: str
    decision: str
    reason: str
    blocking_component: Optional[str]

    transition_plan: tuple[StrategyBShadowTransitionPlan, ...]
    transition_plan_valid: bool

    selected_symbol: Optional[str]
    selected_side: Optional[str]
    proposed_risk_usd: Optional[float]
    proposed_lots_raw: Optional[float]

    evidence_snapshot: Mapping[str, Any]

    observation_only: bool = True
    execution_authority: bool = False
    risk_reservation_authority: bool = False
    broker_order_authority: bool = False
    broker_modify_authority: bool = False
    broker_close_authority: bool = False
    strategy_a_mutation: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _pretrade_path(current_state: str, target_state: str) -> tuple[StrategyBShadowTransitionPlan, ...]:
    src = _upper(current_state)
    dst = _upper(target_state)

    if src == dst:
        return ()

    if src not in _PRETRADE_SEQUENCE or dst not in _PRETRADE_SEQUENCE:
        return (
            StrategyBShadowTransitionPlan(
                src, src, f"UNSUPPORTED_PRETRADE_PATH_{src}_TO_{dst}", False
            ),
        )

    i = _PRETRADE_SEQUENCE.index(src)
    j = _PRETRADE_SEQUENCE.index(dst)

    # Observation/replay must never silently rewind a lifecycle.
    if j < i:
        return (
            StrategyBShadowTransitionPlan(
                src, src, f"PRETRADE_REWIND_REQUIRES_EXPLICIT_RESET_{src}_TO_{dst}", False
            ),
        )

    out = []
    state = src
    for target in _PRETRADE_SEQUENCE[i + 1 : j + 1]:
        tx = transition_strategy_b_lifecycle(
            state,
            target,
            reason=f"SHADOW_ADVANCE_TO_{target}",
            evidence_snapshot={},
            at_ms=0,
        )
        out.append(
            StrategyBShadowTransitionPlan(
                from_state=state,
                to_state=tx.to_state,
                reason=tx.validation_reason,
                valid=tx.valid,
            )
        )
        if not tx.valid:
            break
        state = tx.to_state

    return tuple(out)


def _posttrade_path(
    current_state: str,
    thesis_state: str,
    decision: str,
) -> tuple[StrategyBShadowTransitionPlan, ...]:
    src = _upper(current_state)
    target = _upper(thesis_state)
    action = _upper(decision)

    if src not in _POSTTRADE_STATES:
        return (
            StrategyBShadowTransitionPlan(
                src, src, "POSTTRADE_CURRENT_STATE_INVALID", False
            ),
        )

    if src in ("EXIT_PENDING", "CLOSED"):
        if target == src:
            return ()
        return (
            StrategyBShadowTransitionPlan(
                src, src, f"POSTTRADE_STATE_TERMINAL_OR_EXITING_{src}", False
            ),
        )

    steps: list[str] = []

    if src == "TRADE_ACTIVE":
        if target not in ("THESIS_STRONG", "THESIS_STABLE", "THESIS_WEAKENING", "THESIS_INVALID"):
            return (
                StrategyBShadowTransitionPlan(
                    src, src, f"INVALID_POSTTRADE_TARGET_{target}", False
                ),
            )
        steps.append(target)
    elif src != target:
        steps.append(target)

    # EXIT_REVIEW is still shadow-only, but lifecycle intent may advance to
    # EXIT_PENDING after a valid THESIS_INVALID or THESIS_WEAKENING state.
    if action == "EXIT_REVIEW":
        effective = steps[-1] if steps else src
        if effective in ("THESIS_INVALID", "THESIS_WEAKENING"):
            steps.append("EXIT_PENDING")

    out = []
    state = src
    for target_state in steps:
        tx = transition_strategy_b_lifecycle(
            state,
            target_state,
            reason=f"SHADOW_{action}_TO_{target_state}",
            evidence_snapshot={},
            at_ms=0,
        )
        out.append(
            StrategyBShadowTransitionPlan(
                from_state=state,
                to_state=tx.to_state,
                reason=tx.validation_reason,
                valid=tx.valid,
            )
        )
        if not tx.valid:
            break
        state = tx.to_state

    return tuple(out)


def compose_strategy_b_shadow_decision(
    *,
    current_state: str,
    macro_qualification: Any = None,
    entry_pattern: Any = None,
    expression_ranking: Any = None,
    risk_proposal: Any = None,
    position_thesis: Any = None,
) -> StrategyBShadowDecision:
    current = _upper(current_state)
    if current not in CANONICAL_STATES:
        return StrategyBShadowDecision(
            1, "STRATEGY_B_SHADOW_ORCHESTRATOR_V1", "INVALID",
            current, current, "WAIT", "UNKNOWN_CURRENT_STATE",
            "LIFECYCLE", (), False, None, None, None, None, {},
        )

    posttrade_mode = current in _POSTTRADE_STATES

    if posttrade_mode:
        if position_thesis is None:
            return StrategyBShadowDecision(
                1, "STRATEGY_B_SHADOW_ORCHESTRATOR_V1", "POSTTRADE",
                current, current, "WAIT", "POSITION_THESIS_REQUIRED",
                "POSITION_THESIS", (), False, None, None, None, None, {},
            )

        thesis_state = _upper(_field(position_thesis, "thesis_state", ""))
        decision = _upper(_field(position_thesis, "decision", "WAIT"))
        reason = str(_field(position_thesis, "reason", "POSITION_THESIS_DECISION"))

        plan = _posttrade_path(current, thesis_state, decision)
        valid = all(x.valid for x in plan)

        derived = current
        if plan and valid:
            derived = plan[-1].to_state
        elif not plan and thesis_state:
            derived = thesis_state if thesis_state in CANONICAL_STATES else current

        return StrategyBShadowDecision(
            schema_version=1,
            model="STRATEGY_B_SHADOW_ORCHESTRATOR_V1",
            mode="POSTTRADE",
            current_state=current,
            derived_state=derived,
            decision=decision,
            reason=reason,
            blocking_component=None if valid else "LIFECYCLE",
            transition_plan=plan,
            transition_plan_valid=valid,
            selected_symbol=None,
            selected_side=None,
            proposed_risk_usd=None,
            proposed_lots_raw=None,
            evidence_snapshot={
                "position_thesis": (
                    position_thesis.to_dict()
                    if hasattr(position_thesis, "to_dict")
                    else dict(position_thesis)
                    if isinstance(position_thesis, Mapping)
                    else {}
                )
            },
        )

    pre = compose_strategy_b_pretrade_shadow(
        macro_qualification=macro_qualification,
        entry_pattern=entry_pattern,
        expression_ranking=expression_ranking,
        risk_proposal=risk_proposal,
    )

    plan = _pretrade_path(current, pre.derived_state)
    valid = all(x.valid for x in plan)

    selected_symbol = _field(expression_ranking, "best_symbol")
    selected_side = _field(expression_ranking, "best_side")
    proposed_risk_usd = _field(risk_proposal, "proposed_risk_usd")
    proposed_lots_raw = _field(risk_proposal, "proposed_lots_raw")

    return StrategyBShadowDecision(
        schema_version=1,
        model="STRATEGY_B_SHADOW_ORCHESTRATOR_V1",
        mode="PRETRADE",
        current_state=current,
        derived_state=pre.derived_state if valid else current,
        decision=pre.decision if valid else "WAIT",
        reason=pre.reason if valid else "LIFECYCLE_TRANSITION_PLAN_INVALID",
        blocking_component=pre.blocking_component if valid else "LIFECYCLE",
        transition_plan=plan,
        transition_plan_valid=valid,
        selected_symbol=selected_symbol,
        selected_side=selected_side,
        proposed_risk_usd=proposed_risk_usd,
        proposed_lots_raw=proposed_lots_raw,
        evidence_snapshot=pre.evidence_snapshot,
    )


__all__ = [
    "StrategyBShadowTransitionPlan",
    "StrategyBShadowDecision",
    "compose_strategy_b_shadow_decision",
]
