"""
Pure Strategy-B lifecycle ENTRY_READY bootstrap projection.

Input:
    one already-persisted/reconstructed Strategy-B shadow state.

Output:
    either:
      - a validated initial StrategyBLifecycleState at ENTRY_READY; or
      - a deterministic fail-closed result.

This module does NOT:
- evaluate macro qualification;
- evaluate H1/M15;
- rank expressions;
- capture calibration data;
- evaluate or reserve risk;
- read/write Redis;
- contact a broker;
- submit/modify/cancel orders;
- open/close positions;
- mutate Strategy-A;
- grant execution authority.

The persisted shadow remains the canonical owner of selected expression
(symbol/side). The lifecycle state receives only immutable thesis/capture
identity plus ENTRY_READY lifecycle position.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Optional

from api.strategy_b_lifecycle_state import (
    StrategyBLifecycleState,
    create_strategy_b_lifecycle_state,
)


def _txt(value: Any) -> str:
    return str(value or "").strip()


def _upper(value: Any) -> str:
    return _txt(value).upper()


@dataclass(frozen=True)
class StrategyBLifecycleBootstrap:
    ok: bool
    reason: str
    thesis_id: Optional[str]
    selected_capture_id: Optional[str]
    selected_symbol: Optional[str]
    selected_side: Optional[str]
    state: Optional[StrategyBLifecycleState]
    observation_only: bool = True
    execution_authority: bool = False
    strategy_a_mutation: bool = False


def _reject(
    reason: str,
    *,
    thesis_id: Optional[str] = None,
    selected_capture_id: Optional[str] = None,
    selected_symbol: Optional[str] = None,
    selected_side: Optional[str] = None,
) -> StrategyBLifecycleBootstrap:
    return StrategyBLifecycleBootstrap(
        ok=False,
        reason=str(reason),
        thesis_id=thesis_id,
        selected_capture_id=selected_capture_id,
        selected_symbol=selected_symbol,
        selected_side=selected_side,
        state=None,
        observation_only=True,
        execution_authority=False,
        strategy_a_mutation=False,
    )


def bootstrap_strategy_b_lifecycle_from_shadow(
    *,
    shadow_state: Any,
    at_ms: int,
) -> StrategyBLifecycleBootstrap:
    """
    Validate one persisted/reconstructed shadow and project the initial
    authoritative lifecycle state at ENTRY_READY.

    This function performs no persistence. The caller remains responsible
    for committing the returned state through the frozen lifecycle-state
    persistence adapter.
    """

    if not isinstance(shadow_state, Mapping):
        return _reject("SHADOW_STATE_REQUIRED")

    if shadow_state.get("found") is not True:
        return _reject("SHADOW_STATE_NOT_FOUND")

    evidence_id = _txt(
        shadow_state.get("evidence_id")
    )
    if not evidence_id:
        return _reject("EVIDENCE_ID_REQUIRED")

    thesis_id = _txt(
        shadow_state.get("thesis_id")
    )
    if not thesis_id:
        return _reject("THESIS_ID_REQUIRED")

    if thesis_id != evidence_id:
        return _reject(
            "THESIS_EVIDENCE_ID_MISMATCH",
            thesis_id=thesis_id,
        )

    selected_capture_id = _txt(
        shadow_state.get("selected_capture_id")
    )
    if not selected_capture_id:
        return _reject(
            "SELECTED_CAPTURE_ID_REQUIRED",
            thesis_id=thesis_id,
        )

    link_status = _upper(
        shadow_state.get(
            "selected_capture_link_status"
        )
    )
    if link_status != "LINKED":
        return _reject(
            "SELECTED_CAPTURE_NOT_LINKED",
            thesis_id=thesis_id,
            selected_capture_id=selected_capture_id,
        )

    derived_state = _upper(
        shadow_state.get("derived_state")
    )
    if derived_state != "ENTRY_READY":
        return _reject(
            "SHADOW_NOT_ENTRY_READY",
            thesis_id=thesis_id,
            selected_capture_id=selected_capture_id,
        )

    decision = _upper(
        shadow_state.get("decision")
    )
    if decision != "ENTRY_READY_SHADOW":
        return _reject(
            "SHADOW_DECISION_NOT_ENTRY_READY",
            thesis_id=thesis_id,
            selected_capture_id=selected_capture_id,
        )

    selected_symbol = _upper(
        shadow_state.get("selected_symbol")
    )
    if not selected_symbol:
        return _reject(
            "SELECTED_SYMBOL_REQUIRED",
            thesis_id=thesis_id,
            selected_capture_id=selected_capture_id,
        )

    selected_side = _upper(
        shadow_state.get("selected_side")
    )
    if selected_side not in {
        "BUY",
        "SELL",
    }:
        return _reject(
            "SELECTED_SIDE_INVALID",
            thesis_id=thesis_id,
            selected_capture_id=selected_capture_id,
            selected_symbol=selected_symbol,
        )

    latest = shadow_state.get("latest")
    if not isinstance(latest, Mapping):
        return _reject(
            "LATEST_SHADOW_WRAPPER_REQUIRED",
            thesis_id=thesis_id,
            selected_capture_id=selected_capture_id,
            selected_symbol=selected_symbol,
            selected_side=selected_side,
        )

    latest_evidence_id = _txt(
        latest.get("evidence_id")
    )
    if latest_evidence_id != evidence_id:
        return _reject(
            "LATEST_EVIDENCE_ID_MISMATCH",
            thesis_id=thesis_id,
            selected_capture_id=selected_capture_id,
            selected_symbol=selected_symbol,
            selected_side=selected_side,
        )

    latest_thesis_id = _txt(
        latest.get("thesis_id")
    )
    if latest_thesis_id != thesis_id:
        return _reject(
            "LATEST_THESIS_ID_MISMATCH",
            thesis_id=thesis_id,
            selected_capture_id=selected_capture_id,
            selected_symbol=selected_symbol,
            selected_side=selected_side,
        )

    latest_capture_id = _txt(
        latest.get("selected_capture_id")
    )
    if latest_capture_id != selected_capture_id:
        return _reject(
            "LATEST_SELECTED_CAPTURE_ID_MISMATCH",
            thesis_id=thesis_id,
            selected_capture_id=selected_capture_id,
            selected_symbol=selected_symbol,
            selected_side=selected_side,
        )

    latest_link_status = _upper(
        latest.get(
            "selected_capture_link_status"
        )
    )
    if latest_link_status != "LINKED":
        return _reject(
            "LATEST_SELECTED_CAPTURE_NOT_LINKED",
            thesis_id=thesis_id,
            selected_capture_id=selected_capture_id,
            selected_symbol=selected_symbol,
            selected_side=selected_side,
        )

    persisted_shadow = latest.get("shadow")
    if not isinstance(persisted_shadow, Mapping):
        return _reject(
            "PERSISTED_SHADOW_REQUIRED",
            thesis_id=thesis_id,
            selected_capture_id=selected_capture_id,
            selected_symbol=selected_symbol,
            selected_side=selected_side,
        )

    persisted_derived_state = _upper(
        persisted_shadow.get("derived_state")
    )
    if persisted_derived_state != derived_state:
        return _reject(
            "PERSISTED_SHADOW_STATE_MISMATCH",
            thesis_id=thesis_id,
            selected_capture_id=selected_capture_id,
            selected_symbol=selected_symbol,
            selected_side=selected_side,
        )

    persisted_decision = _upper(
        persisted_shadow.get("decision")
    )
    if persisted_decision != decision:
        return _reject(
            "PERSISTED_SHADOW_DECISION_MISMATCH",
            thesis_id=thesis_id,
            selected_capture_id=selected_capture_id,
            selected_symbol=selected_symbol,
            selected_side=selected_side,
        )

    if (
        persisted_shadow.get(
            "transition_plan_valid"
        )
        is not True
    ):
        return _reject(
            "TRANSITION_PLAN_NOT_VALID",
            thesis_id=thesis_id,
            selected_capture_id=selected_capture_id,
            selected_symbol=selected_symbol,
            selected_side=selected_side,
        )

    persisted_symbol = _upper(
        persisted_shadow.get("selected_symbol")
    )
    if persisted_symbol != selected_symbol:
        return _reject(
            "SELECTED_SYMBOL_MISMATCH",
            thesis_id=thesis_id,
            selected_capture_id=selected_capture_id,
            selected_symbol=selected_symbol,
            selected_side=selected_side,
        )

    persisted_side = _upper(
        persisted_shadow.get("selected_side")
    )
    if persisted_side != selected_side:
        return _reject(
            "SELECTED_SIDE_MISMATCH",
            thesis_id=thesis_id,
            selected_capture_id=selected_capture_id,
            selected_symbol=selected_symbol,
            selected_side=selected_side,
        )

    try:
        bootstrap_at_ms = int(at_ms)
    except (TypeError, ValueError):
        bootstrap_at_ms = 0

    if bootstrap_at_ms <= 0:
        return _reject(
            "BOOTSTRAP_TIME_INVALID",
            thesis_id=thesis_id,
            selected_capture_id=selected_capture_id,
            selected_symbol=selected_symbol,
            selected_side=selected_side,
        )

    state = create_strategy_b_lifecycle_state(
        thesis_id=thesis_id,
        selected_capture_id=selected_capture_id,
        current_state="ENTRY_READY",
        at_ms=bootstrap_at_ms,
    )

    return StrategyBLifecycleBootstrap(
        ok=True,
        reason="ENTRY_READY_BOOTSTRAP_READY",
        thesis_id=thesis_id,
        selected_capture_id=selected_capture_id,
        selected_symbol=selected_symbol,
        selected_side=selected_side,
        state=state,
        observation_only=True,
        execution_authority=False,
        strategy_a_mutation=False,
    )


__all__ = [
    "StrategyBLifecycleBootstrap",
    "bootstrap_strategy_b_lifecycle_from_shadow",
]
