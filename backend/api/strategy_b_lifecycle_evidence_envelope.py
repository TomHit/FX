"""
Pure Strategy-B lifecycle evidence envelope.

The envelope retains the immutable causal external facts belonging to one
Strategy-B thesis / selected-entry capture:

    accepted order -> opened position -> closed position

It is an immutable reducer only.

It does NOT:
- read or write Redis;
- persist anything;
- call a broker;
- place, modify, cancel, open, or close anything;
- perform lifecycle transitions;
- reserve risk;
- write outcomes;
- mutate Strategy-A;
- provide execution or broker authority.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from typing import Any, Mapping, Optional

from api.strategy_b_lifecycle_evidence import (
    StrategyBOrderAcceptedEvidence,
    StrategyBPositionClosedEvidence,
    StrategyBPositionOpenedEvidence,
)
from api.strategy_b_lifecycle_evidence_validator import (
    validate_order_to_position_identity,
    validate_position_to_close_identity,
)


def _required_identity(name: str, value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} required")
    return value.strip()


@dataclass(frozen=True)
class StrategyBLifecycleEvidenceEnvelope:
    thesis_id: str
    selected_capture_id: str
    accepted_order: Optional[StrategyBOrderAcceptedEvidence] = None
    opened_position: Optional[StrategyBPositionOpenedEvidence] = None
    closed_position: Optional[StrategyBPositionClosedEvidence] = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "thesis_id",
            _required_identity("thesis_id", self.thesis_id),
        )
        object.__setattr__(
            self,
            "selected_capture_id",
            _required_identity(
                "selected_capture_id",
                self.selected_capture_id,
            ),
        )


def empty_strategy_b_lifecycle_evidence_envelope(
    *,
    thesis_id: str,
    selected_capture_id: str,
) -> StrategyBLifecycleEvidenceEnvelope:
    return StrategyBLifecycleEvidenceEnvelope(
        thesis_id=thesis_id,
        selected_capture_id=selected_capture_id,
    )


def _validate_envelope_identity(
    envelope: StrategyBLifecycleEvidenceEnvelope,
    evidence: Any,
) -> None:
    if (
        evidence.thesis_id != envelope.thesis_id
        or evidence.selected_capture_id != envelope.selected_capture_id
    ):
        raise ValueError("ENVELOPE_IDENTITY_MISMATCH")


def append_strategy_b_lifecycle_evidence(
    envelope: StrategyBLifecycleEvidenceEnvelope,
    evidence: Any,
) -> StrategyBLifecycleEvidenceEnvelope:
    if not isinstance(envelope, StrategyBLifecycleEvidenceEnvelope):
        raise TypeError("StrategyBLifecycleEvidenceEnvelope required")

    if isinstance(evidence, StrategyBOrderAcceptedEvidence):
        _validate_envelope_identity(envelope, evidence)

        existing = envelope.accepted_order
        if existing is not None:
            if existing == evidence:
                return envelope
            raise ValueError("ACCEPTED_ORDER_CONFLICT")

        # A late accepted-order fact must never be inserted underneath
        # already-established later causal facts.
        if (
            envelope.opened_position is not None
            or envelope.closed_position is not None
        ):
            raise ValueError("ACCEPTED_ORDER_CAUSAL_ORDER_VIOLATION")

        return replace(
            envelope,
            accepted_order=evidence,
        )

    if isinstance(evidence, StrategyBPositionOpenedEvidence):
        _validate_envelope_identity(envelope, evidence)

        existing = envelope.opened_position
        if existing is not None:
            if existing == evidence:
                return envelope
            raise ValueError("OPENED_POSITION_CONFLICT")

        if envelope.accepted_order is None:
            raise ValueError("ACCEPTED_ORDER_REQUIRED")

        if envelope.closed_position is not None:
            raise ValueError("OPENED_POSITION_CAUSAL_ORDER_VIOLATION")

        continuity = validate_order_to_position_identity(
            envelope.accepted_order,
            evidence,
        )
        if not continuity.ok:
            raise ValueError(continuity.reason)

        return replace(
            envelope,
            opened_position=evidence,
        )

    if isinstance(evidence, StrategyBPositionClosedEvidence):
        _validate_envelope_identity(envelope, evidence)

        existing = envelope.closed_position
        if existing is not None:
            if existing == evidence:
                return envelope
            raise ValueError("CLOSED_POSITION_CONFLICT")

        if envelope.opened_position is None:
            raise ValueError("OPENED_POSITION_REQUIRED")

        continuity = validate_position_to_close_identity(
            envelope.opened_position,
            evidence,
        )
        if not continuity.ok:
            raise ValueError(continuity.reason)

        return replace(
            envelope,
            closed_position=evidence,
        )

    raise TypeError("UNSUPPORTED_LIFECYCLE_EVIDENCE")


_ENVELOPE_SCHEMA_VERSION = 1
_ENVELOPE_MODEL = "STRATEGY_B_LIFECYCLE_EVIDENCE_ENVELOPE_V1"

_ENVELOPE_FIELDS = {
    "schema_version",
    "model",
    "thesis_id",
    "selected_capture_id",
    "accepted_order",
    "opened_position",
    "closed_position",
}


def _evidence_payload(value: Any) -> Optional[dict]:
    if value is None:
        return None
    return asdict(value)


def serialize_strategy_b_lifecycle_evidence_envelope(
    envelope: StrategyBLifecycleEvidenceEnvelope,
) -> dict:
    if not isinstance(envelope, StrategyBLifecycleEvidenceEnvelope):
        raise TypeError("StrategyBLifecycleEvidenceEnvelope required")

    return {
        "schema_version": _ENVELOPE_SCHEMA_VERSION,
        "model": _ENVELOPE_MODEL,
        "thesis_id": envelope.thesis_id,
        "selected_capture_id": envelope.selected_capture_id,
        "accepted_order": _evidence_payload(envelope.accepted_order),
        "opened_position": _evidence_payload(envelope.opened_position),
        "closed_position": _evidence_payload(envelope.closed_position),
    }


def _nested_payload(
    payload: Mapping[str, Any],
    field: str,
) -> Optional[dict]:
    value = payload.get(field)

    if value is None:
        return None

    if not isinstance(value, Mapping):
        raise TypeError(f"{field} must be mapping or None")

    return dict(value)


def reconstruct_strategy_b_lifecycle_evidence_envelope(
    payload: Mapping[str, Any],
) -> StrategyBLifecycleEvidenceEnvelope:
    if not isinstance(payload, Mapping):
        raise TypeError("lifecycle evidence envelope payload must be mapping")

    unknown = set(payload.keys()) - _ENVELOPE_FIELDS
    if unknown:
        raise ValueError(
            "UNKNOWN_ENVELOPE_FIELDS:"
            + ",".join(sorted(str(x) for x in unknown))
        )

    if payload.get("schema_version") != _ENVELOPE_SCHEMA_VERSION:
        raise ValueError("UNSUPPORTED_ENVELOPE_SCHEMA_VERSION")

    if payload.get("model") != _ENVELOPE_MODEL:
        raise ValueError("UNSUPPORTED_ENVELOPE_MODEL")

    env = empty_strategy_b_lifecycle_evidence_envelope(
        thesis_id=payload.get("thesis_id"),
        selected_capture_id=payload.get("selected_capture_id"),
    )

    accepted_payload = _nested_payload(
        payload,
        "accepted_order",
    )
    opened_payload = _nested_payload(
        payload,
        "opened_position",
    )
    closed_payload = _nested_payload(
        payload,
        "closed_position",
    )

    # Rebuild typed facts in canonical causal order.  Constructors validate
    # each fact, while append_strategy_b_lifecycle_evidence revalidates
    # envelope identity and cross-fact broker continuity.
    if accepted_payload is not None:
        accepted = StrategyBOrderAcceptedEvidence(
            **accepted_payload
        )
        env = append_strategy_b_lifecycle_evidence(
            env,
            accepted,
        )

    if opened_payload is not None:
        opened = StrategyBPositionOpenedEvidence(
            **opened_payload
        )
        env = append_strategy_b_lifecycle_evidence(
            env,
            opened,
        )

    if closed_payload is not None:
        closed = StrategyBPositionClosedEvidence(
            **closed_payload
        )
        env = append_strategy_b_lifecycle_evidence(
            env,
            closed,
        )

    return env


__all__ = [
    "StrategyBLifecycleEvidenceEnvelope",
    "empty_strategy_b_lifecycle_evidence_envelope",
    "append_strategy_b_lifecycle_evidence",
    "serialize_strategy_b_lifecycle_evidence_envelope",
    "reconstruct_strategy_b_lifecycle_evidence_envelope",
]
