"""
Strategy-B ENTRY_READY lifecycle bootstrap persistence bridge.

Purpose
-------
Bridge one already-persisted/reconstructed Strategy-B shadow state into the
authoritative Strategy-B lifecycle-state ledger.

The bridge is intentionally narrow:

    persisted shadow
        -> causal persisted_at_ms
        -> frozen pure bootstrap validator
        -> initial ENTRY_READY lifecycle state
        -> frozen atomic lifecycle-state persistence adapter

Authority boundary
------------------
This module does NOT:
- evaluate macro qualification;
- evaluate DXY/H1/M15;
- rank expressions;
- capture calibration data;
- reserve risk;
- contact a broker;
- submit, modify, or cancel orders;
- open or close positions;
- write outcomes;
- mutate Strategy-A;
- grant execution authority.

The persisted shadow remains the canonical owner of selected symbol/side.
The lifecycle ledger receives only thesis/capture identity and lifecycle
progression.

Bootstrap time is taken ONLY from latest.persisted_at_ms.  There is no
wall-clock fallback.  This makes restart/retry reconstruction deterministic
and allows the frozen persistence adapter to identify an identical state.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Mapping, Optional

from api.strategy_b_lifecycle_bootstrap import (
    bootstrap_strategy_b_lifecycle_from_shadow,
)
from api.strategy_b_lifecycle_state_redis_adapter import (
    persist_strategy_b_lifecycle_state_atomic,
)


@dataclass(frozen=True)
class StrategyBLifecycleBootstrapPersistence:
    ok: bool
    reason: str
    bootstrap_reason: Optional[str]
    persistence_reason: Optional[str]
    thesis_id: Optional[str]
    selected_capture_id: Optional[str]
    selected_symbol: Optional[str]
    selected_side: Optional[str]
    observation_only: bool = True
    execution_authority: bool = False
    strategy_a_mutation: bool = False


def _result(
    ok: bool,
    reason: str,
    *,
    bootstrap_reason: Optional[str] = None,
    persistence_reason: Optional[str] = None,
    thesis_id: Optional[str] = None,
    selected_capture_id: Optional[str] = None,
    selected_symbol: Optional[str] = None,
    selected_side: Optional[str] = None,
) -> StrategyBLifecycleBootstrapPersistence:
    return StrategyBLifecycleBootstrapPersistence(
        ok=bool(ok),
        reason=str(reason),
        bootstrap_reason=bootstrap_reason,
        persistence_reason=persistence_reason,
        thesis_id=thesis_id,
        selected_capture_id=selected_capture_id,
        selected_symbol=selected_symbol,
        selected_side=selected_side,
        observation_only=True,
        execution_authority=False,
        strategy_a_mutation=False,
    )


def _persisted_at_ms_from_shadow(
    shadow_state: Any,
) -> tuple[Optional[int], Optional[str]]:
    """
    Return the causal material-shadow persistence timestamp.

    last_observed_at_ms is deliberately ignored.  A later observer/restart
    must not manufacture a new ENTRY_READY lifecycle timestamp.
    """

    if not isinstance(shadow_state, Mapping):
        # Let the frozen bootstrap validator own the structural rejection.
        return None, None

    latest = shadow_state.get("latest")

    if not isinstance(latest, Mapping):
        # Again, structural validation belongs to the bootstrap validator.
        return None, None

    if "persisted_at_ms" not in latest:
        return None, "PERSISTED_AT_MS_REQUIRED"

    value = latest.get("persisted_at_ms")

    # Persisted causal time is part of lifecycle identity.
    # Require the same strict integer representation owned by the
    # canonical lifecycle-state contract.  Do not coerce strings,
    # floats, booleans, or other external representations.
    if isinstance(value, bool) or not isinstance(value, int):
        return None, "PERSISTED_AT_MS_INVALID"

    if value <= 0:
        return None, "PERSISTED_AT_MS_INVALID"

    return value, None


def persist_strategy_b_entry_ready_lifecycle(
    *,
    redis_client: Any,
    shadow_state: Any,
    persist_state: Callable[
        [Any, Any],
        Any,
    ] = persist_strategy_b_lifecycle_state_atomic,
) -> StrategyBLifecycleBootstrapPersistence:
    """
    Validate and persist the initial Strategy-B ENTRY_READY lifecycle state.

    Persistence is attempted only after the frozen bootstrap contract has
    accepted the persisted shadow.

    The injected persist_state boundary exists for deterministic tests.
    Production defaults to the already-frozen atomic Redis adapter.
    """

    # ------------------------------------------------------------
    # First preserve ownership of structural shadow validation.
    #
    # We need a timestamp to call the pure bootstrap, but malformed
    # shadow/latest structures should still return the frozen bootstrap's
    # canonical rejection reasons rather than inventing bridge reasons.
    # ------------------------------------------------------------
    persisted_at_ms, timestamp_error = (
        _persisted_at_ms_from_shadow(
            shadow_state
        )
    )

    if timestamp_error is not None:
        return _result(
            False,
            timestamp_error,
        )

    # If structural shape prevented timestamp extraction, use an invalid
    # timestamp solely to invoke the frozen validator.  Its earlier
    # structural checks must reject before BOOTSTRAP_TIME_INVALID.
    bootstrap_at_ms = (
        persisted_at_ms
        if persisted_at_ms is not None
        else 0
    )

    try:
        bootstrap = (
            bootstrap_strategy_b_lifecycle_from_shadow(
                shadow_state=shadow_state,
                at_ms=bootstrap_at_ms,
            )
        )
    except Exception:
        return _result(
            False,
            "BOOTSTRAP_VALIDATION_ERROR",
            bootstrap_reason="BOOTSTRAP_EXCEPTION",
        )

    if not bootstrap.ok:
        return _result(
            False,
            bootstrap.reason,
            bootstrap_reason=bootstrap.reason,
            thesis_id=bootstrap.thesis_id,
            selected_capture_id=(
                bootstrap.selected_capture_id
            ),
            selected_symbol=bootstrap.selected_symbol,
            selected_side=bootstrap.selected_side,
        )

    # A successful bootstrap necessarily requires a real causal timestamp.
    # Keep this guard explicit so no future validator change can introduce
    # a wall-clock or synthetic-time fallback here.
    if persisted_at_ms is None:
        return _result(
            False,
            "PERSISTED_AT_MS_REQUIRED",
            bootstrap_reason=bootstrap.reason,
            thesis_id=bootstrap.thesis_id,
            selected_capture_id=(
                bootstrap.selected_capture_id
            ),
            selected_symbol=bootstrap.selected_symbol,
            selected_side=bootstrap.selected_side,
        )

    if bootstrap.state is None:
        return _result(
            False,
            "BOOTSTRAP_STATE_REQUIRED",
            bootstrap_reason=bootstrap.reason,
            thesis_id=bootstrap.thesis_id,
            selected_capture_id=(
                bootstrap.selected_capture_id
            ),
            selected_symbol=bootstrap.selected_symbol,
            selected_side=bootstrap.selected_side,
        )

    # ------------------------------------------------------------
    # Persistence boundary.
    #
    # The frozen adapter owns:
    # - exact-key lifecycle namespace;
    # - missing -> ENTRY_READY only;
    # - identity continuity;
    # - regression prevention;
    # - canonical transition proof;
    # - identical-state idempotence;
    # - atomic Redis CAS.
    # ------------------------------------------------------------
    try:
        persisted = persist_state(
            redis_client,
            bootstrap.state,
        )
    except Exception:
        return _result(
            False,
            "LIFECYCLE_STATE_PERSIST_ERROR",
            bootstrap_reason=bootstrap.reason,
            persistence_reason="PERSISTENCE_EXCEPTION",
            thesis_id=bootstrap.thesis_id,
            selected_capture_id=(
                bootstrap.selected_capture_id
            ),
            selected_symbol=bootstrap.selected_symbol,
            selected_side=bootstrap.selected_side,
        )

    persistence_reason = str(
        getattr(
            persisted,
            "reason",
            "PERSISTENCE_RESULT_INVALID",
        )
    )

    persistence_ok = (
        getattr(
            persisted,
            "ok",
            False,
        )
        is True
    )

    if not persistence_ok:
        return _result(
            False,
            persistence_reason,
            bootstrap_reason=bootstrap.reason,
            persistence_reason=persistence_reason,
            thesis_id=bootstrap.thesis_id,
            selected_capture_id=(
                bootstrap.selected_capture_id
            ),
            selected_symbol=bootstrap.selected_symbol,
            selected_side=bootstrap.selected_side,
        )

    # Do not broaden the success vocabulary here.  The frozen adapter
    # currently owns these successful outcomes.
    if persistence_reason not in {
        "COMMITTED",
        "IDENTICAL_STATE",
    }:
        return _result(
            False,
            "PERSISTENCE_RESULT_INVALID",
            bootstrap_reason=bootstrap.reason,
            persistence_reason=persistence_reason,
            thesis_id=bootstrap.thesis_id,
            selected_capture_id=(
                bootstrap.selected_capture_id
            ),
            selected_symbol=bootstrap.selected_symbol,
            selected_side=bootstrap.selected_side,
        )

    return _result(
        True,
        persistence_reason,
        bootstrap_reason=bootstrap.reason,
        persistence_reason=persistence_reason,
        thesis_id=bootstrap.thesis_id,
        selected_capture_id=bootstrap.selected_capture_id,
        selected_symbol=bootstrap.selected_symbol,
        selected_side=bootstrap.selected_side,
    )


__all__ = [
    "StrategyBLifecycleBootstrapPersistence",
    "persist_strategy_b_entry_ready_lifecycle",
]
