"""
Atomic Redis persistence adapter for Strategy-B lifecycle evidence.

Authority boundary:
- exact Strategy-B lifecycle-evidence key only;
- no SCAN / KEYS / wildcard operations;
- no broker, execution, lifecycle-transition, risk, outcome, or Strategy-A
  authority;
- corrupt persisted state fails closed and is never auto-repaired.

The caller supplies a Redis-compatible connection exposing get() and eval().
"""

from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any, Optional

from api.strategy_b_lifecycle_evidence_envelope import (
    StrategyBLifecycleEvidenceEnvelope,
    reconstruct_strategy_b_lifecycle_evidence_envelope,
    serialize_strategy_b_lifecycle_evidence_envelope,
)
from api.strategy_b_lifecycle_evidence_persistence import (
    lifecycle_evidence_key,
    validate_strategy_b_lifecycle_evidence_persistence,
)


STRATEGY_B_LIFECYCLE_EVIDENCE_TTL_SECONDS = 21 * 24 * 60 * 60


_ATOMIC_CAS_LUA = r"""
local current = redis.call('GET', KEYS[1])
local expected = ARGV[1]
local replacement = ARGV[2]
local ttl = tonumber(ARGV[3])

if expected == '__XTL_STRATEGY_B_EXPECT_MISSING__' then
    if current ~= false then
        return {0, 'CAS_MISMATCH'}
    end
else
    if current == false or current ~= expected then
        return {0, 'CAS_MISMATCH'}
    end
end

redis.call('SET', KEYS[1], replacement, 'EX', ttl)
return {1, 'COMMITTED'}
"""


@dataclass(frozen=True)
class StrategyBLifecycleEvidenceRedisResult:
    ok: bool
    reason: str
    key: str


def _result(
    ok: bool,
    reason: str,
    key: str,
) -> StrategyBLifecycleEvidenceRedisResult:
    return StrategyBLifecycleEvidenceRedisResult(
        ok=bool(ok),
        reason=str(reason),
        key=str(key),
    )


def _text(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.decode("utf-8")
    if isinstance(value, str):
        return value
    raise ValueError("CURRENT_ENVELOPE_CORRUPT")


def _encode_envelope(
    envelope: StrategyBLifecycleEvidenceEnvelope,
) -> str:
    payload = serialize_strategy_b_lifecycle_evidence_envelope(
        envelope
    )
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _decode_current(
    raw: Any,
) -> Optional[StrategyBLifecycleEvidenceEnvelope]:
    text = _text(raw)
    if text is None:
        return None

    try:
        payload = json.loads(text)
        return reconstruct_strategy_b_lifecycle_evidence_envelope(
            payload
        )
    except Exception as exc:
        raise ValueError("CURRENT_ENVELOPE_CORRUPT") from exc


def load_strategy_b_lifecycle_evidence(
    redis_client: Any,
    thesis_id: str,
) -> Optional[StrategyBLifecycleEvidenceEnvelope]:
    key = lifecycle_evidence_key(thesis_id)
    raw = redis_client.get(key)
    return _decode_current(raw)


def _parse_eval_result(value: Any) -> tuple[bool, str]:
    if not isinstance(value, (list, tuple)) or len(value) < 2:
        return False, "REDIS_CAS_INVALID_RESPONSE"

    status = value[0]
    reason = value[1]

    if isinstance(reason, bytes):
        reason = reason.decode("utf-8")

    try:
        ok = int(status) == 1
    except Exception:
        return False, "REDIS_CAS_INVALID_RESPONSE"

    return ok, str(reason)


def persist_strategy_b_lifecycle_evidence_atomic(
    redis_client: Any,
    envelope: StrategyBLifecycleEvidenceEnvelope,
) -> StrategyBLifecycleEvidenceRedisResult:
    if not isinstance(
        envelope,
        StrategyBLifecycleEvidenceEnvelope,
    ):
        raise TypeError(
            "StrategyBLifecycleEvidenceEnvelope required"
        )

    key = lifecycle_evidence_key(envelope.thesis_id)

    # Exact-key read only.  The raw serialized value becomes the CAS token.
    raw_current = redis_client.get(key)

    try:
        current_text = _text(raw_current)
        current = _decode_current(raw_current)
    except Exception:
        return _result(
            False,
            "CURRENT_ENVELOPE_CORRUPT",
            key,
        )

    decision = validate_strategy_b_lifecycle_evidence_persistence(
        current,
        envelope,
    )

    if not decision.allowed:
        return _result(
            False,
            decision.reason,
            key,
        )

    if decision.reason == "IDENTICAL_ENVELOPE":
        return _result(
            True,
            "IDENTICAL_ENVELOPE",
            key,
        )

    replacement = _encode_envelope(envelope)

    expected_token = (
        "__XTL_STRATEGY_B_EXPECT_MISSING__"
        if current_text is None
        else current_text
    )

    response = redis_client.eval(
        _ATOMIC_CAS_LUA,
        1,
        key,
        expected_token,
        replacement,
        STRATEGY_B_LIFECYCLE_EVIDENCE_TTL_SECONDS,
    )

    ok, reason = _parse_eval_result(response)

    if not ok:
        return _result(
            False,
            reason,
            key,
        )

    return _result(
        True,
        "COMMITTED",
        key,
    )


__all__ = [
    "STRATEGY_B_LIFECYCLE_EVIDENCE_TTL_SECONDS",
    "StrategyBLifecycleEvidenceRedisResult",
    "load_strategy_b_lifecycle_evidence",
    "persist_strategy_b_lifecycle_evidence_atomic",
]
