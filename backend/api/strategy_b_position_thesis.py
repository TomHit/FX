
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping, Optional

POST_ENTRY_STATES = (
    "THESIS_STRONG",
    "THESIS_STABLE",
    "THESIS_WEAKENING",
    "THESIS_INVALID",
)

POST_ENTRY_DECISIONS = (
    "HOLD",
    "PROTECT",
    "PRE_EVENT_REVIEW",
    "EXIT_REVIEW",
)


def _mapping(v: Any) -> Mapping[str, Any]:
    return v if isinstance(v, Mapping) else {}


def _bool(v: Any) -> bool:
    return bool(v is True)


@dataclass(frozen=True)
class StrategyBPositionThesisDecision:
    schema_version: int
    model: str
    thesis_state: str
    decision: str
    reason: str
    reasons: tuple[str, ...]
    macro_thesis_valid: bool
    symbol_structure_valid: bool
    dxy_structure_valid: bool
    favorable_structure_confirmed: bool
    pre_event_review: bool
    fast_event_path_qualified: bool
    market_rejected_adverse_information: bool
    meaningfully_profitable: bool
    adverse_event_high_quality: bool
    adverse_event_materially_contradicts: bool
    dxy_adverse_reaction_confirmed: bool
    symbol_or_cross_market_adverse_confirmed: bool
    m15_h1_adverse_transition_confirmed: bool
    evidence_snapshot: Mapping[str, Any]
    observation_only: bool = True
    execution_authority: bool = False
    broker_modify_authority: bool = False
    broker_close_authority: bool = False
    strategy_a_mutation: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def evaluate_strategy_b_position_thesis(
    *,
    thesis_evidence: Mapping[str, Any],
    event_evidence: Optional[Mapping[str, Any]] = None,
    account_safety: Optional[Mapping[str, Any]] = None,
) -> StrategyBPositionThesisDecision:
    t = _mapping(thesis_evidence)
    e = _mapping(event_evidence)
    a = _mapping(account_safety)
    reasons: list[str] = []

    macro_valid = _bool(t.get("macro_thesis_valid"))
    symbol_valid = _bool(t.get("symbol_structure_valid"))
    dxy_valid = _bool(t.get("dxy_structure_valid"))
    favorable_structure = _bool(t.get("favorable_h1_h4_structure_confirmed"))
    materially_weakened = _bool(t.get("thesis_materially_weakened"))

    high_impact_approaching = _bool(e.get("high_impact_event_approaching"))
    meaningfully_profitable = _bool(e.get("meaningfully_profitable"))
    adverse_high_quality = _bool(e.get("adverse_event_high_quality"))
    adverse_material = _bool(e.get("adverse_event_materially_contradicts"))
    dxy_adverse = _bool(e.get("dxy_adverse_reaction_confirmed"))
    symbol_cross_adverse = _bool(e.get("symbol_or_cross_market_adverse_confirmed"))
    m15_h1_adverse = _bool(e.get("m15_h1_adverse_transition_confirmed"))
    adverse_rejected = _bool(e.get("market_rejected_or_ignored_adverse_information"))

    account_data_ok = a.get("data_ok") is True if a else True
    force_exit = _bool(a.get("force_exit_required"))

    fast_event_path = bool(
        meaningfully_profitable
        and adverse_high_quality
        and adverse_material
        and dxy_adverse
        and symbol_cross_adverse
        and m15_h1_adverse
    )

    snapshot = {
        "thesis": dict(t),
        "event": dict(e),
        "account_safety": dict(a),
        "derived": {
            "fast_event_path_qualified": fast_event_path,
            "normal_structure_intact": bool(macro_valid and symbol_valid and dxy_valid),
        },
    }

    def out(state: str, decision: str, reason: str, *, pre_event=False, fast=False):
        reasons.append(reason)
        return StrategyBPositionThesisDecision(
            schema_version=1,
            model="STRATEGY_B_POSITION_THESIS_V1",
            thesis_state=state,
            decision=decision,
            reason=reason,
            reasons=tuple(dict.fromkeys(reasons)),
            macro_thesis_valid=macro_valid,
            symbol_structure_valid=symbol_valid,
            dxy_structure_valid=dxy_valid,
            favorable_structure_confirmed=favorable_structure,
            pre_event_review=pre_event,
            fast_event_path_qualified=fast,
            market_rejected_adverse_information=adverse_rejected,
            meaningfully_profitable=meaningfully_profitable,
            adverse_event_high_quality=adverse_high_quality,
            adverse_event_materially_contradicts=adverse_material,
            dxy_adverse_reaction_confirmed=dxy_adverse,
            symbol_or_cross_market_adverse_confirmed=symbol_cross_adverse,
            m15_h1_adverse_transition_confirmed=m15_h1_adverse,
            evidence_snapshot=snapshot,
        )

    if not account_data_ok:
        return out("THESIS_WEAKENING", "EXIT_REVIEW", "ACCOUNT_SAFETY_DATA_UNAVAILABLE")

    if force_exit:
        return out("THESIS_INVALID", "EXIT_REVIEW", "ACCOUNT_OR_PROP_SAFETY_REQUIRES_EXIT")

    if not macro_valid:
        return out("THESIS_INVALID", "EXIT_REVIEW", "MACRO_THESIS_INVALIDATED")

    if not symbol_valid:
        return out("THESIS_INVALID", "EXIT_REVIEW", "SELECTED_SYMBOL_STRUCTURE_INVALIDATED")

    if not dxy_valid:
        return out("THESIS_INVALID", "EXIT_REVIEW", "DXY_STRUCTURE_INVALIDATED")

    if fast_event_path:
        reasons.extend((
            "MEANINGFULLY_PROFITABLE",
            "ADVERSE_HIGH_QUALITY_EVENT_OR_NEWS",
            "ADVERSE_INFORMATION_MATERIALLY_CONTRADICTS_THESIS",
            "DXY_ADVERSE_REACTION_CONFIRMED",
            "SYMBOL_OR_CROSS_MARKET_ADVERSE_REACTION_CONFIRMED",
            "M15_H1_ADVERSE_TRANSITION_CONFIRMED",
        ))
        return out(
            "THESIS_WEAKENING",
            "EXIT_REVIEW",
            "FAST_EVENT_PROFIT_PROTECTION_REVIEW",
            fast=True,
        )

    if high_impact_approaching:
        return out(
            "THESIS_STABLE",
            "PRE_EVENT_REVIEW",
            "HIGH_IMPACT_EVENT_APPROACHING_PRE_EVENT_REVIEW",
            pre_event=True,
        )

    if adverse_rejected:
        return out(
            "THESIS_STRONG",
            "HOLD",
            "MARKET_REJECTED_ADVERSE_INFORMATION_HOLD",
        )

    if materially_weakened:
        return out(
            "THESIS_WEAKENING",
            "PROTECT",
            "THESIS_WEAKENING_STRUCTURAL_PROTECTION_REVIEW",
        )

    if favorable_structure:
        return out(
            "THESIS_STRONG",
            "PROTECT",
            "FAVORABLE_STRUCTURE_ALLOWS_STRUCTURAL_PROTECTION",
        )

    return out(
        "THESIS_STABLE",
        "HOLD",
        "THESIS_AND_STRUCTURE_INTACT",
    )


__all__ = [
    "POST_ENTRY_STATES",
    "POST_ENTRY_DECISIONS",
    "StrategyBPositionThesisDecision",
    "evaluate_strategy_b_position_thesis",
]
