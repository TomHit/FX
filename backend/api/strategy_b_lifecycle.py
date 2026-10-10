
"""
XauTrendLab Strategy B - Canonical Lifecycle
============================================

Pure, observation-only state machine for Strategy-B.

This module:
- does not read/write Redis;
- does not place/modify/close broker orders;
- does not reserve risk;
- does not mutate Strategy-A;
- provides deterministic transition validation, evidence snapshots, replay,
  and a pre-entry shadow-state composer.

Persistence and manager wiring belong outside this module.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Iterable, Mapping, Optional


CANONICAL_STATES = (
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
    "ORDER_PENDING",
    "TRADE_ACTIVE",
    "THESIS_STRONG",
    "THESIS_STABLE",
    "THESIS_WEAKENING",
    "THESIS_INVALID",
    "EXIT_PENDING",
    "CLOSED",
)

TERMINAL_STATE = "CLOSED"


# Forward-only production lifecycle. Recovery/reconstruction should replay
# persisted transitions rather than silently jumping backward.
_ALLOWED_NEXT = {
    "OBSERVING": {"MACRO_IMPULSE"},
    "MACRO_IMPULSE": {"EXPANDING"},
    "EXPANDING": {"EXTENDED"},
    "EXTENDED": {"MATURE_AT_HTF_SR"},
    "MATURE_AT_HTF_SR": {"CATALYST_DETECTED"},
    "CATALYST_DETECTED": {"REACTION_PENDING"},
    "REACTION_PENDING": {"REVERSAL_CONFIRMED"},
    "REVERSAL_CONFIRMED": {"EXPRESSION_RANKING"},
    "EXPRESSION_RANKING": {"ENTRY_READY"},
    "ENTRY_READY": {"ORDER_PENDING"},
    "ORDER_PENDING": {"TRADE_ACTIVE"},
    "TRADE_ACTIVE": {"THESIS_STRONG", "THESIS_STABLE", "THESIS_WEAKENING", "THESIS_INVALID", "CLOSED"},
    "THESIS_STRONG": {"THESIS_STABLE", "THESIS_WEAKENING", "THESIS_INVALID", "CLOSED"},
    "THESIS_STABLE": {"THESIS_STRONG", "THESIS_WEAKENING", "THESIS_INVALID", "CLOSED"},
    "THESIS_WEAKENING": {"THESIS_STABLE", "THESIS_INVALID", "EXIT_PENDING", "CLOSED"},
    "THESIS_INVALID": {"EXIT_PENDING", "CLOSED"},
    "EXIT_PENDING": {"CLOSED"},
    "CLOSED": set(),
}


def _upper(v: Any) -> str:
    return str(v or "").strip().upper()


def _mapping(v: Any) -> Mapping[str, Any]:
    return v if isinstance(v, Mapping) else {}


def _field(obj: Any, name: str, default: Any = None) -> Any:
    if isinstance(obj, Mapping):
        return obj.get(name, default)
    return getattr(obj, name, default)


@dataclass(frozen=True)
class StrategyBTransition:
    schema_version: int
    model: str
    from_state: str
    to_state: str
    at_ms: int
    reason: str
    evidence_snapshot: Mapping[str, Any]
    material: bool
    valid: bool
    validation_reason: str
    observation_only: bool = True
    execution_authority: bool = False
    strategy_a_mutation: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class StrategyBLifecycleReplay:
    schema_version: int
    model: str
    initial_state: str
    final_state: str
    valid: bool
    reason: str
    applied: tuple[StrategyBTransition, ...]
    observation_only: bool = True

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class StrategyBPretradeShadow:
    schema_version: int
    model: str
    derived_state: str
    decision: str
    reason: str
    blocking_component: Optional[str]
    evidence_snapshot: Mapping[str, Any]
    observation_only: bool = True
    execution_authority: bool = False
    strategy_a_mutation: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def validate_strategy_b_transition(from_state: str, to_state: str) -> tuple[bool, str]:
    src = _upper(from_state)
    dst = _upper(to_state)

    if src not in CANONICAL_STATES:
        return False, "UNKNOWN_FROM_STATE"
    if dst not in CANONICAL_STATES:
        return False, "UNKNOWN_TO_STATE"

    if src == dst:
        return True, "NO_STATE_CHANGE"

    if dst in _ALLOWED_NEXT.get(src, set()):
        return True, "ALLOWED_TRANSITION"

    return False, f"ILLEGAL_TRANSITION_{src}_TO_{dst}"


def transition_strategy_b_lifecycle(
    current_state: str,
    target_state: str,
    *,
    reason: str,
    evidence_snapshot: Optional[Mapping[str, Any]] = None,
    at_ms: int,
) -> StrategyBTransition:
    src = _upper(current_state)
    dst = _upper(target_state)
    valid, validation_reason = validate_strategy_b_transition(src, dst)

    return StrategyBTransition(
        schema_version=1,
        model="STRATEGY_B_LIFECYCLE_V1",
        from_state=src,
        to_state=dst if valid else src,
        at_ms=int(at_ms),
        reason=str(reason or "").strip() or "UNSPECIFIED",
        evidence_snapshot=dict(_mapping(evidence_snapshot)),
        material=bool(valid and src != dst),
        valid=valid,
        validation_reason=validation_reason,
    )


def replay_strategy_b_lifecycle(
    transitions: Iterable[Mapping[str, Any] | StrategyBTransition],
    *,
    initial_state: str = "OBSERVING",
) -> StrategyBLifecycleReplay:
    state = _upper(initial_state)
    if state not in CANONICAL_STATES:
        return StrategyBLifecycleReplay(
            schema_version=1,
            model="STRATEGY_B_LIFECYCLE_REPLAY_V1",
            initial_state=state,
            final_state=state,
            valid=False,
            reason="UNKNOWN_INITIAL_STATE",
            applied=(),
        )

    applied: list[StrategyBTransition] = []

    for raw in transitions:
        if isinstance(raw, StrategyBTransition):
            target = raw.to_state
            reason = raw.reason
            snapshot = raw.evidence_snapshot
            at_ms = raw.at_ms
        else:
            row = _mapping(raw)
            target = _upper(row.get("to_state"))
            reason = str(row.get("reason") or "REPLAY")
            snapshot = _mapping(row.get("evidence_snapshot"))
            try:
                at_ms = int(row.get("at_ms") or 0)
            except Exception:
                at_ms = 0

        tx = transition_strategy_b_lifecycle(
            state,
            target,
            reason=reason,
            evidence_snapshot=snapshot,
            at_ms=at_ms,
        )
        applied.append(tx)

        if not tx.valid:
            return StrategyBLifecycleReplay(
                schema_version=1,
                model="STRATEGY_B_LIFECYCLE_REPLAY_V1",
                initial_state=_upper(initial_state),
                final_state=state,
                valid=False,
                reason=tx.validation_reason,
                applied=tuple(applied),
            )

        state = tx.to_state

    return StrategyBLifecycleReplay(
        schema_version=1,
        model="STRATEGY_B_LIFECYCLE_REPLAY_V1",
        initial_state=_upper(initial_state),
        final_state=state,
        valid=True,
        reason="REPLAY_OK",
        applied=tuple(applied),
    )


def _snapshot(
    macro_qualification: Any,
    entry_pattern: Any,
    expression_ranking: Any,
    risk_proposal: Any,
) -> dict[str, Any]:
    return {
        "macro": {
            "macro_leg_established": bool(_field(macro_qualification, "macro_leg_established", False)),
            "maturity_state": _upper(_field(macro_qualification, "maturity_state", "UNKNOWN")),
            "maturity_qualified": bool(_field(macro_qualification, "maturity_qualified", False)),
            "location_qualified": bool(_field(macro_qualification, "location_qualified", False)),
            "market_reaction_confirmed": bool(_field(macro_qualification, "market_reaction_confirmed", False)),
            "h1_reversal_confirmed": bool(_field(macro_qualification, "h1_reversal_confirmed", False)),
            "macro_qualified": bool(_field(macro_qualification, "macro_qualified", False)),
            "qualification_state": _upper(_field(macro_qualification, "qualification_state", "UNKNOWN")),
        },
        "entry_pattern": {
            "decision": _upper(_field(entry_pattern, "decision", "UNKNOWN")),
            "reason": _upper(_field(entry_pattern, "reason", "UNKNOWN")),
            "entry_ready_shadow": bool(_field(entry_pattern, "entry_ready_shadow", False)),
            "selected_pattern": _field(entry_pattern, "selected_pattern"),
        },
        "expression_ranking": {
            "decision": _upper(_field(expression_ranking, "decision", "UNKNOWN")),
            "reason": _upper(_field(expression_ranking, "reason", "UNKNOWN")),
            "best_symbol": _field(expression_ranking, "best_symbol"),
            "best_side": _field(expression_ranking, "best_side"),
        },
        "risk_proposal": {
            "decision": _upper(_field(risk_proposal, "decision", "UNKNOWN")),
            "reason": _upper(_field(risk_proposal, "reason", "UNKNOWN")),
            "valid": bool(_field(risk_proposal, "valid", False)),
            "proposed_risk_usd": _field(risk_proposal, "proposed_risk_usd"),
            "proposed_lots_raw": _field(risk_proposal, "proposed_lots_raw"),
        },
    }


def compose_strategy_b_pretrade_shadow(
    *,
    macro_qualification: Any,
    entry_pattern: Any,
    expression_ranking: Any,
    risk_proposal: Any,
) -> StrategyBPretradeShadow:
    """
    Compose the current pre-trade Strategy-B shadow state.

    This is intentionally conservative:
    - it never returns ORDER_PENDING;
    - it never returns TRADE_ACTIVE;
    - ENTRY_READY requires macro qualification, an approved timed pattern,
      one best expression, and a valid shadow risk proposal.
    """
    snap = _snapshot(
        macro_qualification,
        entry_pattern,
        expression_ranking,
        risk_proposal,
    )

    m = snap["macro"]
    p = snap["entry_pattern"]
    r = snap["expression_ranking"]
    risk = snap["risk_proposal"]

    if not m["macro_leg_established"]:
        return StrategyBPretradeShadow(
            1, "STRATEGY_B_PRETRADE_SHADOW_V1",
            "OBSERVING", "WAIT", "MACRO_LEG_NOT_ESTABLISHED",
            "MACRO_QUALIFICATION", snap
        )

    maturity = m["maturity_state"]
    if maturity == "EXPANDING":
        return StrategyBPretradeShadow(
            1, "STRATEGY_B_PRETRADE_SHADOW_V1",
            "EXPANDING", "WAIT", "MACRO_MOVE_EXPANDING",
            "MACRO_QUALIFICATION", snap
        )

    if not m["maturity_qualified"] or not m["location_qualified"]:
        return StrategyBPretradeShadow(
            1, "STRATEGY_B_PRETRADE_SHADOW_V1",
            "EXTENDED", "WAIT", "MATURE_HTF_LOCATION_NOT_FULLY_QUALIFIED",
            "MACRO_QUALIFICATION", snap
        )

    if not m["market_reaction_confirmed"]:
        qstate = m["qualification_state"]
        if qstate in ("WAIT_CATALYST_DIRECTION",):
            state = "MATURE_AT_HTF_SR"
            reason = "CATALYST_NOT_YET_ACTIONABLE"
        else:
            state = "REACTION_PENDING"
            reason = "MARKET_REACTION_PENDING"
        return StrategyBPretradeShadow(
            1, "STRATEGY_B_PRETRADE_SHADOW_V1",
            state, "WAIT", reason,
            "MACRO_QUALIFICATION", snap
        )

    if not m["h1_reversal_confirmed"]:
        return StrategyBPretradeShadow(
            1, "STRATEGY_B_PRETRADE_SHADOW_V1",
            "REACTION_PENDING", "WAIT", "H1_REVERSAL_PROOF_PENDING",
            "MACRO_QUALIFICATION", snap
        )

    if not m["macro_qualified"]:
        return StrategyBPretradeShadow(
            1, "STRATEGY_B_PRETRADE_SHADOW_V1",
            "REVERSAL_CONFIRMED", "WAIT", "MACRO_QUALIFICATION_INCOMPLETE",
            "MACRO_QUALIFICATION", snap
        )

    if not p["entry_ready_shadow"]:
        return StrategyBPretradeShadow(
            1, "STRATEGY_B_PRETRADE_SHADOW_V1",
            "REVERSAL_CONFIRMED", "WAIT",
            p["reason"] or "ENTRY_PATTERN_PENDING",
            "ENTRY_PATTERN", snap
        )

    if r["decision"] != "BEST_EXPRESSION_SELECTED_SHADOW" or not r["best_symbol"]:
        return StrategyBPretradeShadow(
            1, "STRATEGY_B_PRETRADE_SHADOW_V1",
            "EXPRESSION_RANKING", "WAIT",
            r["reason"] or "EXPRESSION_RANKING_PENDING",
            "EXPRESSION_RANKER", snap
        )

    if not risk["valid"] or risk["decision"] != "RISK_PROPOSAL_READY_SHADOW":
        return StrategyBPretradeShadow(
            1, "STRATEGY_B_PRETRADE_SHADOW_V1",
            "EXPRESSION_RANKING", "WAIT",
            risk["reason"] or "RISK_PROPOSAL_PENDING",
            "RISK_PROPOSAL", snap
        )

    return StrategyBPretradeShadow(
        1, "STRATEGY_B_PRETRADE_SHADOW_V1",
        "ENTRY_READY", "ENTRY_READY_SHADOW",
        "ALL_PRETRADE_SHADOW_GATES_PASS",
        None, snap
    )


__all__ = [
    "CANONICAL_STATES",
    "StrategyBTransition",
    "StrategyBLifecycleReplay",
    "StrategyBPretradeShadow",
    "validate_strategy_b_transition",
    "transition_strategy_b_lifecycle",
    "replay_strategy_b_lifecycle",
    "compose_strategy_b_pretrade_shadow",
]
