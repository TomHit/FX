"""
Strategy-B lifecycle evidence persistence contract.

This module owns only:
- Strategy-B Redis key construction; and
- pure monotonic-envelope persistence validation.

It deliberately performs no Redis I/O.  Atomic storage is a later adapter
layer and must consume this contract rather than duplicating its rules.

It has no broker, execution, lifecycle-transition, risk-reservation,
outcome-writing, or Strategy-A authority.
"""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Optional

from api.strategy_b_lifecycle_evidence_envelope import (
    StrategyBLifecycleEvidenceEnvelope,
)


STRATEGY_B_REDIS_ROOT = "xtl:strategy_b"
STRATEGY_B_LIFECYCLE_EVIDENCE_PREFIX = (
    f"{STRATEGY_B_REDIS_ROOT}:lifecycle:evidence"
)

_SAFE_THESIS_ID = re.compile(r"^[A-Za-z0-9._-]+$")


@dataclass(frozen=True)
class StrategyBLifecycleEvidencePersistenceDecision:
    allowed: bool
    reason: str


def lifecycle_evidence_key(thesis_id: str) -> str:
    if not isinstance(thesis_id, str):
        raise TypeError("thesis_id must be str")

    value = thesis_id.strip()
    if not value:
        raise ValueError("thesis_id required")

    if not _SAFE_THESIS_ID.fullmatch(value):
        raise ValueError("UNSAFE_THESIS_ID")

    key = f"{STRATEGY_B_LIFECYCLE_EVIDENCE_PREFIX}:{value}"

    # Defensive invariant: this module can construct Strategy-B keys only.
    if not key.startswith(f"{STRATEGY_B_REDIS_ROOT}:"):
        raise ValueError("STRATEGY_B_NAMESPACE_VIOLATION")

    return key


def _decision(
    allowed: bool,
    reason: str,
) -> StrategyBLifecycleEvidencePersistenceDecision:
    return StrategyBLifecycleEvidencePersistenceDecision(
        allowed=bool(allowed),
        reason=str(reason),
    )


def _require_envelope(
    value: object,
    *,
    allow_none: bool,
) -> Optional[StrategyBLifecycleEvidenceEnvelope]:
    if value is None and allow_none:
        return None

    if not isinstance(
        value,
        StrategyBLifecycleEvidenceEnvelope,
    ):
        raise TypeError(
            "StrategyBLifecycleEvidenceEnvelope required"
        )

    return value


def _stage(
    envelope: StrategyBLifecycleEvidenceEnvelope,
) -> int:
    """
    EMPTY=0, ACCEPTED=1, OPENED=2, CLOSED=3.

    The envelope reducer itself guarantees causal shape.  This helper
    deliberately does not manufacture or repair missing evidence.
    """
    if envelope.closed_position is not None:
        return 3
    if envelope.opened_position is not None:
        return 2
    if envelope.accepted_order is not None:
        return 1
    return 0


def validate_strategy_b_lifecycle_evidence_persistence(
    current: Optional[StrategyBLifecycleEvidenceEnvelope],
    incoming: StrategyBLifecycleEvidenceEnvelope,
) -> StrategyBLifecycleEvidencePersistenceDecision:
    current = _require_envelope(
        current,
        allow_none=True,
    )
    incoming = _require_envelope(
        incoming,
        allow_none=False,
    )

    if current is None:
        return _decision(
            True,
            "INITIAL_ENVELOPE_ALLOWED",
        )

    if current.thesis_id != incoming.thesis_id:
        return _decision(
            False,
            "THESIS_ID_MISMATCH",
        )

    if (
        current.selected_capture_id
        != incoming.selected_capture_id
    ):
        return _decision(
            False,
            "SELECTED_CAPTURE_ID_MISMATCH",
        )

    if current == incoming:
        return _decision(
            True,
            "IDENTICAL_ENVELOPE",
        )

    current_stage = _stage(current)
    incoming_stage = _stage(incoming)

    if incoming_stage < current_stage:
        return _decision(
            False,
            "ENVELOPE_REGRESSION",
        )

    # Once a fact exists it is immutable.  Compare oldest causal fact
    # first so conflict reasons remain deterministic.
    if (
        current.accepted_order is not None
        and current.accepted_order
        != incoming.accepted_order
    ):
        return _decision(
            False,
            "ACCEPTED_ORDER_CONFLICT",
        )

    if (
        current.opened_position is not None
        and current.opened_position
        != incoming.opened_position
    ):
        return _decision(
            False,
            "OPENED_POSITION_CONFLICT",
        )

    if (
        current.closed_position is not None
        and current.closed_position
        != incoming.closed_position
    ):
        return _decision(
            False,
            "CLOSED_POSITION_CONFLICT",
        )

    if incoming_stage == current_stage:
        # Same stage but non-identical and not explained by an immutable
        # existing-fact conflict is not a legal persistence mutation.
        return _decision(
            False,
            "NON_MONOTONIC_MUTATION",
        )

    return _decision(
        True,
        "MONOTONIC_EXTENSION",
    )


__all__ = [
    "STRATEGY_B_REDIS_ROOT",
    "STRATEGY_B_LIFECYCLE_EVIDENCE_PREFIX",
    "StrategyBLifecycleEvidencePersistenceDecision",
    "lifecycle_evidence_key",
    "validate_strategy_b_lifecycle_evidence_persistence",
]
