from __future__ import annotations

"""
XauTrendLab Strategy B - Lifecycle Runtime Bootstrap Hook

Narrow runtime boundary between one already-persisted Strategy-B
shadow and the frozen ENTRY_READY lifecycle bootstrap bridge.

This module:

- reconstructs exactly one shadow by evidence_id;
- ignores shadows that are absent or not ENTRY_READY;
- delegates genuine persisted ENTRY_READY bootstrap to the frozen
  lifecycle bootstrap-persistence bridge;
- treats an already-advanced lifecycle as an idempotent/no-op outcome;
- contains reconstruction/bootstrap exceptions;
- owns no macro evaluation, risk reservation, broker evidence,
  order placement, position handling, lifecycle advancement,
  Redis enumeration, Strategy-A mutation, or execution authority.
"""

from dataclasses import dataclass
from typing import Any, Callable, Mapping, Optional


@dataclass(frozen=True)
class StrategyBLifecycleRuntimeHookResult:
    ok: bool
    action: str
    reason: str

    thesis_id: Optional[str] = None
    selected_capture_id: Optional[str] = None
    selected_symbol: Optional[str] = None
    selected_side: Optional[str] = None

    observation_only: bool = True
    execution_authority: bool = False
    strategy_a_mutation: bool = False


def _txt(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _upper(value: Any) -> str:
    return _txt(value).upper()


def _result(
    *,
    ok: bool,
    action: str,
    reason: str,
    thesis_id: Any = None,
    selected_capture_id: Any = None,
    selected_symbol: Any = None,
    selected_side: Any = None,
) -> StrategyBLifecycleRuntimeHookResult:
    return StrategyBLifecycleRuntimeHookResult(
        ok=bool(ok),
        action=_upper(action),
        reason=_upper(reason),
        thesis_id=_txt(thesis_id) or None,
        selected_capture_id=(
            _txt(selected_capture_id) or None
        ),
        selected_symbol=(
            _upper(selected_symbol) or None
        ),
        selected_side=(
            _upper(selected_side) or None
        ),
        observation_only=True,
        execution_authority=False,
        strategy_a_mutation=False,
    )


def _shadow_identity(
    shadow: Mapping[str, Any],
) -> dict[str, Optional[str]]:
    return {
        "thesis_id": (
            _txt(shadow.get("thesis_id"))
            or _txt(shadow.get("evidence_id"))
            or None
        ),
        "selected_capture_id": (
            _txt(shadow.get("selected_capture_id"))
            or None
        ),
        "selected_symbol": (
            _upper(shadow.get("selected_symbol"))
            or None
        ),
        "selected_side": (
            _upper(shadow.get("selected_side"))
            or None
        ),
    }


def _persist_result_identity(
    persisted: Any,
    *,
    fallback: Mapping[str, Optional[str]],
) -> dict[str, Optional[str]]:
    return {
        "thesis_id": (
            _txt(getattr(persisted, "thesis_id", None))
            or fallback.get("thesis_id")
        ),
        "selected_capture_id": (
            _txt(
                getattr(
                    persisted,
                    "selected_capture_id",
                    None,
                )
            )
            or fallback.get("selected_capture_id")
        ),
        "selected_symbol": (
            _upper(
                getattr(
                    persisted,
                    "selected_symbol",
                    None,
                )
            )
            or fallback.get("selected_symbol")
        ),
        "selected_side": (
            _upper(
                getattr(
                    persisted,
                    "selected_side",
                    None,
                )
            )
            or fallback.get("selected_side")
        ),
    }


def _valid_persisted_entry_ready_shadow(
    shadow: Mapping[str, Any],
    *,
    evidence_id: str,
) -> bool:
    """
    Strict external-boundary validation for one persisted ENTRY_READY
    shadow before lifecycle bootstrap is allowed.

    The frozen bootstrap remains the authoritative lifecycle validator.
    This hook additionally proves identity continuity across the
    reconstructed persistence envelope.
    """

    if shadow.get("found") is not True:
        return False

    top_evidence = _txt(shadow.get("evidence_id"))
    top_thesis = _txt(shadow.get("thesis_id"))
    top_capture = _txt(
        shadow.get("selected_capture_id")
    )
    top_link = _upper(
        shadow.get("selected_capture_link_status")
    )
    top_symbol = _upper(
        shadow.get("selected_symbol")
    )
    top_side = _upper(
        shadow.get("selected_side")
    )

    if (
        not top_evidence
        or top_evidence != evidence_id
    ):
        return False

    # Canonical Strategy-B thesis identity = evidence_id.
    if (
        not top_thesis
        or top_thesis != top_evidence
    ):
        return False

    if not top_capture:
        return False

    if top_link != "LINKED":
        return False

    if not top_symbol:
        return False

    if top_side not in ("BUY", "SELL"):
        return False

    if (
        _upper(shadow.get("derived_state"))
        != "ENTRY_READY"
    ):
        return False

    if (
        _upper(shadow.get("decision"))
        != "ENTRY_READY_SHADOW"
    ):
        return False

    latest = shadow.get("latest")

    if not isinstance(latest, Mapping):
        return False

    if (
        _txt(latest.get("evidence_id"))
        != top_evidence
    ):
        return False

    if (
        _txt(latest.get("thesis_id"))
        != top_thesis
    ):
        return False

    if (
        _txt(latest.get("selected_capture_id"))
        != top_capture
    ):
        return False

    if (
        _upper(
            latest.get(
                "selected_capture_link_status"
            )
        )
        != "LINKED"
    ):
        return False

    # Strict persisted timestamp:
    # no bool/string/float coercion.
    persisted_at_ms = latest.get(
        "persisted_at_ms"
    )

    if (
        isinstance(persisted_at_ms, bool)
        or not isinstance(persisted_at_ms, int)
        or persisted_at_ms <= 0
    ):
        return False

    persisted_shadow = latest.get("shadow")

    if not isinstance(
        persisted_shadow,
        Mapping,
    ):
        return False

    if (
        _upper(
            persisted_shadow.get(
                "derived_state"
            )
        )
        != "ENTRY_READY"
    ):
        return False

    if (
        _upper(
            persisted_shadow.get("decision")
        )
        != "ENTRY_READY_SHADOW"
    ):
        return False

    if (
        persisted_shadow.get(
            "transition_plan_valid"
        )
        is not True
    ):
        return False

    if (
        _upper(
            persisted_shadow.get(
                "selected_symbol"
            )
        )
        != top_symbol
    ):
        return False

    if (
        _upper(
            persisted_shadow.get(
                "selected_side"
            )
        )
        != top_side
    ):
        return False

    return True


def _bridge_identity_matches_shadow(
    persisted: Any,
    *,
    expected: Mapping[str, Optional[str]],
) -> bool:
    """
    Bridge results must explicitly preserve the immutable identity
    supplied by the persisted shadow.

    Missing or different bridge identity fails closed.
    """

    actual_thesis = _txt(
        getattr(
            persisted,
            "thesis_id",
            None,
        )
    )

    actual_capture = _txt(
        getattr(
            persisted,
            "selected_capture_id",
            None,
        )
    )

    actual_symbol = _upper(
        getattr(
            persisted,
            "selected_symbol",
            None,
        )
    )

    actual_side = _upper(
        getattr(
            persisted,
            "selected_side",
            None,
        )
    )

    expected_thesis = _txt(
        expected.get("thesis_id")
    )

    expected_capture = _txt(
        expected.get(
            "selected_capture_id"
        )
    )

    expected_symbol = _upper(
        expected.get("selected_symbol")
    )

    expected_side = _upper(
        expected.get("selected_side")
    )

    if not all(
        (
            actual_thesis,
            actual_capture,
            actual_symbol,
            actual_side,
            expected_thesis,
            expected_capture,
            expected_symbol,
            expected_side,
        )
    ):
        return False

    return (
        actual_thesis == expected_thesis
        and actual_capture == expected_capture
        and actual_symbol == expected_symbol
        and actual_side == expected_side
    )

def process_strategy_b_lifecycle_bootstrap(
    R,
    evidence_id: str,
    *,
    reconstruct_shadow: Optional[Callable[..., Any]] = None,
    persist_entry_ready: Optional[Callable[..., Any]] = None,
) -> StrategyBLifecycleRuntimeHookResult:
    """
    Process lifecycle bootstrap for exactly one evidence_id.

    No discovery/enumeration occurs here. The caller owns bounded evidence
    scheduling. This function only performs exact reconstruction and, when
    appropriate, delegates to the frozen bootstrap persistence bridge.
    """

    evidence_id = _txt(evidence_id)

    if not evidence_id:
        return _result(
            ok=False,
            action="FAIL_CLOSED",
            reason="EVIDENCE_ID_REQUIRED",
        )

    if reconstruct_shadow is None:
        from api.strategy_b_shadow_persistence import (
            reconstruct_strategy_b_shadow_state,
        )

        reconstruct_shadow = (
            reconstruct_strategy_b_shadow_state
        )

    if persist_entry_ready is None:
        from api.strategy_b_lifecycle_bootstrap_persistence import (
            persist_strategy_b_entry_ready_lifecycle,
        )

        persist_entry_ready = (
            persist_strategy_b_entry_ready_lifecycle
        )

    try:
        shadow = reconstruct_shadow(
            R,
            evidence_id,
        )
    except Exception:
        return _result(
            ok=False,
            action="FAIL_CLOSED",
            reason="SHADOW_RECONSTRUCTION_ERROR",
            thesis_id=evidence_id,
        )

    if not isinstance(shadow, Mapping):
        return _result(
            ok=False,
            action="FAIL_CLOSED",
            reason="PERSISTED_SHADOW_INVALID",
            thesis_id=evidence_id,
        )

    if shadow.get("found") is not True:
        return _result(
            ok=True,
            action="NOOP",
            reason="SHADOW_NOT_FOUND",
            thesis_id=evidence_id,
        )

    state = _upper(
        shadow.get("derived_state")
    )
    decision = _upper(
        shadow.get("decision")
    )

    identity = _shadow_identity(shadow)

    # If either persisted field claims the ENTRY_READY boundary,
    # require the complete persisted ENTRY_READY contract before
    # classifying the observation. This prevents a malformed
    # ENTRY_READY object from being downgraded to a harmless NOOP.
    claims_entry_ready = (
        state == "ENTRY_READY"
        or decision == "ENTRY_READY_SHADOW"
    )

    if claims_entry_ready:
        if not _valid_persisted_entry_ready_shadow(
            shadow,
            evidence_id=evidence_id,
        ):
            return _result(
                ok=False,
                action="FAIL_CLOSED",
                reason="PERSISTED_SHADOW_INVALID",
                **identity,
            )

    # A genuinely non-entry-ready persisted shadow is a NOOP.
    if (
        state != "ENTRY_READY"
        or decision != "ENTRY_READY_SHADOW"
    ):
        return _result(
            ok=True,
            action="NOOP",
            reason="SHADOW_NOT_ENTRY_READY",
            **identity,
        )

    try:
        persisted = persist_entry_ready(
            redis_client=R,
            shadow_state=shadow,
        )
    except Exception:
        return _result(
            ok=False,
            action="FAIL_CLOSED",
            reason="BOOTSTRAP_PERSISTENCE_ERROR",
            **identity,
        )

    # Persistence results are contract objects. A malformed return must
    # never be interpreted as successful lifecycle progress.
    if persisted is None:
        return _result(
            ok=False,
            action="FAIL_CLOSED",
            reason="BOOTSTRAP_RESULT_INVALID",
            **identity,
        )

    persisted_ok = getattr(
        persisted,
        "ok",
        None,
    )
    persisted_reason = _upper(
        getattr(
            persisted,
            "reason",
            None,
        )
    )

    if not isinstance(persisted_ok, bool):
        return _result(
            ok=False,
            action="FAIL_CLOSED",
            reason="BOOTSTRAP_RESULT_INVALID",
            **identity,
        )

    if not persisted_reason:
        return _result(
            ok=False,
            action="FAIL_CLOSED",
            reason="BOOTSTRAP_RESULT_INVALID",
            **identity,
        )

    # Never allow the bridge result to substitute, omit, or
    # manufacture lifecycle identity. The persisted shadow owns
    # canonical identity at this runtime boundary.
    if not _bridge_identity_matches_shadow(
        persisted,
        expected=identity,
    ):
        return _result(
            ok=False,
            action="FAIL_CLOSED",
            reason="BOOTSTRAP_IDENTITY_MISMATCH",
            **identity,
        )

    result_identity = _persist_result_identity(
        persisted,
        fallback=identity,
    )

    if persisted_ok is True and persisted_reason == "COMMITTED":
        return _result(
            ok=True,
            action="BOOTSTRAPPED",
            reason="COMMITTED",
            **result_identity,
        )

    if (
        persisted_ok is True
        and persisted_reason == "IDENTICAL_STATE"
    ):
        return _result(
            ok=True,
            action="IDEMPOTENT",
            reason="IDENTICAL_STATE",
            **result_identity,
        )

    if (
        persisted_ok is False
        and persisted_reason
        == "LIFECYCLE_STATE_REGRESSION"
    ):
        return _result(
            ok=True,
            action="ALREADY_ADVANCED",
            reason="LIFECYCLE_STATE_REGRESSION",
            **result_identity,
        )

    # Every other bridge rejection remains fail closed.
    return _result(
        ok=False,
        action="FAIL_CLOSED",
        reason=persisted_reason,
        **result_identity,
    )


__all__ = [
    "StrategyBLifecycleRuntimeHookResult",
    "process_strategy_b_lifecycle_bootstrap",
]
