"""
XauTrendLab Strategy B - Lifecycle State Codec
===============================================

Deterministic, versioned encoding for the immutable Strategy-B lifecycle
state contract.

Reconstruction does not trust a serialized transition.  It replays the
serialized transition through the canonical lifecycle primitive and requires
the canonical result to match the stored transition exactly.
"""

from __future__ import annotations

import json
from typing import Any, Mapping, Optional

from api.strategy_b_lifecycle import (
    StrategyBTransition,
    transition_strategy_b_lifecycle,
)
from api.strategy_b_lifecycle_state import (
    StrategyBLifecycleState,
    create_strategy_b_lifecycle_state,
)


STRATEGY_B_LIFECYCLE_STATE_SCHEMA = (
    "xtl.strategy_b.lifecycle_state"
)
STRATEGY_B_LIFECYCLE_STATE_SCHEMA_VERSION = 1


_STATE_FIELDS = {
    "schema",
    "schema_version",
    "thesis_id",
    "selected_capture_id",
    "current_state",
    "updated_at_ms",
    "last_transition",
}

_TRANSITION_FIELDS = {
    "schema_version",
    "model",
    "from_state",
    "to_state",
    "at_ms",
    "reason",
    "evidence_snapshot",
    "material",
    "valid",
    "validation_reason",
    "observation_only",
    "execution_authority",
    "strategy_a_mutation",
}


def _mapping(
    value: Any,
    *,
    reason: str,
) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(reason)

    return value


def _exact_fields(
    value: Mapping[str, Any],
    expected: set[str],
    *,
    reason: str,
) -> None:
    if set(value.keys()) != expected:
        raise ValueError(reason)


def _transition_payload(
    tx: StrategyBTransition,
) -> dict[str, Any]:
    return {
        "schema_version": tx.schema_version,
        "model": tx.model,
        "from_state": tx.from_state,
        "to_state": tx.to_state,
        "at_ms": tx.at_ms,
        "reason": tx.reason,
        "evidence_snapshot": dict(tx.evidence_snapshot),
        "material": tx.material,
        "valid": tx.valid,
        "validation_reason": tx.validation_reason,
        "observation_only": tx.observation_only,
        "execution_authority": tx.execution_authority,
        "strategy_a_mutation": tx.strategy_a_mutation,
    }


def _state_payload(
    state: StrategyBLifecycleState,
) -> dict[str, Any]:
    if not isinstance(state, StrategyBLifecycleState):
        raise TypeError(
            "state must be StrategyBLifecycleState"
        )

    return {
        "schema": STRATEGY_B_LIFECYCLE_STATE_SCHEMA,
        "schema_version": (
            STRATEGY_B_LIFECYCLE_STATE_SCHEMA_VERSION
        ),
        "thesis_id": state.thesis_id,
        "selected_capture_id": state.selected_capture_id,
        "current_state": state.current_state,
        "updated_at_ms": state.updated_at_ms,
        "last_transition": (
            None
            if state.last_transition is None
            else _transition_payload(
                state.last_transition
            )
        ),
    }


def serialize_strategy_b_lifecycle_state(
    state: StrategyBLifecycleState,
) -> str:
    return json.dumps(
        _state_payload(state),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def _decode_json(raw: Any) -> Mapping[str, Any]:
    if not isinstance(raw, str):
        raise TypeError(
            "lifecycle state payload must be str"
        )

    if not raw:
        raise ValueError(
            "MALFORMED_LIFECYCLE_STATE_PAYLOAD"
        )

    try:
        payload = json.loads(raw)
    except Exception as exc:
        raise ValueError(
            "MALFORMED_LIFECYCLE_STATE_PAYLOAD"
        ) from exc

    return _mapping(
        payload,
        reason="MALFORMED_LIFECYCLE_STATE_PAYLOAD",
    )


def _reconstruct_transition(
    payload: Any,
) -> StrategyBTransition:
    tx_payload = _mapping(
        payload,
        reason="MALFORMED_LIFECYCLE_TRANSITION",
    )

    _exact_fields(
        tx_payload,
        _TRANSITION_FIELDS,
        reason="MALFORMED_LIFECYCLE_TRANSITION",
    )

    evidence_snapshot = _mapping(
        tx_payload["evidence_snapshot"],
        reason="MALFORMED_LIFECYCLE_TRANSITION",
    )

    try:
        canonical = transition_strategy_b_lifecycle(
            tx_payload["from_state"],
            tx_payload["to_state"],
            reason=tx_payload["reason"],
            evidence_snapshot=dict(evidence_snapshot),
            at_ms=tx_payload["at_ms"],
        )
    except Exception as exc:
        raise ValueError(
            "LIFECYCLE_TRANSITION_CORRUPT"
        ) from exc

    canonical_payload = _transition_payload(
        canonical
    )

    if canonical_payload != dict(tx_payload):
        raise ValueError(
            "LIFECYCLE_TRANSITION_CORRUPT"
        )

    return canonical


def deserialize_strategy_b_lifecycle_state(
    raw: str,
) -> StrategyBLifecycleState:
    payload = _decode_json(raw)

    _exact_fields(
        payload,
        _STATE_FIELDS,
        reason="MALFORMED_LIFECYCLE_STATE_PAYLOAD",
    )

    if (
        payload["schema"]
        != STRATEGY_B_LIFECYCLE_STATE_SCHEMA
    ):
        raise ValueError(
            "UNKNOWN_LIFECYCLE_STATE_SCHEMA"
        )

    if (
        payload["schema_version"]
        != STRATEGY_B_LIFECYCLE_STATE_SCHEMA_VERSION
    ):
        raise ValueError(
            "UNKNOWN_LIFECYCLE_STATE_SCHEMA_VERSION"
        )

    last_payload: Optional[Any] = payload[
        "last_transition"
    ]

    if last_payload is None:
        try:
            return create_strategy_b_lifecycle_state(
                thesis_id=payload["thesis_id"],
                selected_capture_id=payload[
                    "selected_capture_id"
                ],
                current_state=payload["current_state"],
                at_ms=payload["updated_at_ms"],
            )
        except Exception as exc:
            raise ValueError(
                "MALFORMED_LIFECYCLE_STATE_PAYLOAD"
            ) from exc

    tx = _reconstruct_transition(last_payload)

    if tx.to_state != payload["current_state"]:
        raise ValueError(
            "LIFECYCLE_STATE_TRANSITION_MISMATCH"
        )

    if tx.at_ms != payload["updated_at_ms"]:
        raise ValueError(
            "LIFECYCLE_STATE_TRANSITION_MISMATCH"
        )

    try:
        return StrategyBLifecycleState(
            thesis_id=payload["thesis_id"],
            selected_capture_id=payload[
                "selected_capture_id"
            ],
            current_state=payload["current_state"],
            updated_at_ms=payload["updated_at_ms"],
            last_transition=tx,
        )
    except Exception as exc:
        raise ValueError(
            "MALFORMED_LIFECYCLE_STATE_PAYLOAD"
        ) from exc


__all__ = [
    "STRATEGY_B_LIFECYCLE_STATE_SCHEMA",
    "STRATEGY_B_LIFECYCLE_STATE_SCHEMA_VERSION",
    "serialize_strategy_b_lifecycle_state",
    "deserialize_strategy_b_lifecycle_state",
]
