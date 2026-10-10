"""
Atomic Redis persistence adapter for Strategy-B lifecycle state.

Authority boundary:
- exact Strategy-B lifecycle-state key only;
- no SCAN / KEYS / wildcard operations;
- no broker, execution, risk, outcome, or Strategy-A authority;
- corrupt persisted state fails closed and is never auto-repaired;
- lifecycle replacement legality remains owned by the frozen pure
  lifecycle-state replacement contract.

The caller supplies a Redis-compatible connection exposing get() and eval().
"""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Optional

from api.strategy_b_lifecycle_state import (
    StrategyBLifecycleState,
)
from api.strategy_b_lifecycle_state_codec import (
    deserialize_strategy_b_lifecycle_state,
    serialize_strategy_b_lifecycle_state,
)
from api.strategy_b_lifecycle_state_persistence import (
    validate_strategy_b_lifecycle_state_replacement,
)


STRATEGY_B_REDIS_ROOT = "xtl:strategy_b"

STRATEGY_B_LIFECYCLE_STATE_PREFIX = (
    f"{STRATEGY_B_REDIS_ROOT}:lifecycle:state"
)

STRATEGY_B_LIFECYCLE_STATE_TTL_SECONDS = (
    21 * 24 * 60 * 60
)

_SAFE_THESIS_ID = re.compile(
    r"^[A-Za-z0-9._-]+$"
)

_EXPECT_MISSING = (
    "__XTL_STRATEGY_B_EXPECT_MISSING__"
)


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
class StrategyBLifecycleStateRedisResult:
    ok: bool
    reason: str
    key: str


def _result(
    ok: bool,
    reason: str,
    key: str,
) -> StrategyBLifecycleStateRedisResult:
    return StrategyBLifecycleStateRedisResult(
        ok=bool(ok),
        reason=str(reason),
        key=str(key),
    )


def lifecycle_state_key(
    thesis_id: str,
) -> str:
    if not isinstance(thesis_id, str):
        raise TypeError(
            "thesis_id must be str"
        )

    value = thesis_id.strip()

    if not value:
        raise ValueError(
            "thesis_id required"
        )

    if not _SAFE_THESIS_ID.fullmatch(value):
        raise ValueError(
            "UNSAFE_THESIS_ID"
        )

    key = (
        f"{STRATEGY_B_LIFECYCLE_STATE_PREFIX}:"
        f"{value}"
    )

    if not key.startswith(
        f"{STRATEGY_B_REDIS_ROOT}:"
    ):
        raise ValueError(
            "STRATEGY_B_NAMESPACE_VIOLATION"
        )

    return key


def _text(
    value: Any,
) -> Optional[str]:
    if value is None:
        return None

    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except Exception as exc:
            raise ValueError(
                "CURRENT_STATE_CORRUPT"
            ) from exc

    if isinstance(value, str):
        return value

    raise ValueError(
        "CURRENT_STATE_CORRUPT"
    )


def _decode_current(
    raw: Any,
) -> Optional[StrategyBLifecycleState]:
    try:
        text = _text(raw)

        if text is None:
            return None

        return deserialize_strategy_b_lifecycle_state(
            text
        )

    except Exception as exc:
        raise ValueError(
            "CURRENT_STATE_CORRUPT"
        ) from exc


def load_strategy_b_lifecycle_state(
    redis_client: Any,
    thesis_id: str,
) -> Optional[StrategyBLifecycleState]:
    key = lifecycle_state_key(
        thesis_id
    )

    raw = redis_client.get(key)

    return _decode_current(raw)


def _parse_eval_result(
    value: Any,
) -> tuple[bool, str]:
    if (
        not isinstance(value, (list, tuple))
        or len(value) < 2
    ):
        return (
            False,
            "REDIS_CAS_INVALID_RESPONSE",
        )

    status = value[0]
    reason = value[1]

    if isinstance(reason, bytes):
        try:
            reason = reason.decode("utf-8")
        except Exception:
            return (
                False,
                "REDIS_CAS_INVALID_RESPONSE",
            )

    try:
        ok = int(status) == 1
    except Exception:
        return (
            False,
            "REDIS_CAS_INVALID_RESPONSE",
        )

    return bool(ok), str(reason)


def persist_strategy_b_lifecycle_state_atomic(
    redis_client: Any,
    proposed: StrategyBLifecycleState,
) -> StrategyBLifecycleStateRedisResult:
    if not isinstance(
        proposed,
        StrategyBLifecycleState,
    ):
        raise TypeError(
            "StrategyBLifecycleState required"
        )

    key = lifecycle_state_key(
        proposed.thesis_id
    )

    # Exact-key read only. The exact serialized value
    # becomes the CAS token.
    raw_current = redis_client.get(key)

    try:
        current_text = _text(
            raw_current
        )

        current = _decode_current(
            raw_current
        )

    except Exception:
        return _result(
            False,
            "CURRENT_STATE_CORRUPT",
            key,
        )

    # The authoritative Strategy-B external lifecycle
    # begins only after the selected expression reaches
    # ENTRY_READY. A missing state key must therefore
    # never bootstrap directly into an advanced state.
    if current is None:
        if proposed.current_state != "ENTRY_READY":
            return _result(
                False,
                "INITIAL_STATE_MUST_BE_ENTRY_READY",
                key,
            )

        # ENTRY_READY is the bootstrap state, not a
        # reconstructed later transition.
        if proposed.last_transition is not None:
            return _result(
                False,
                "INITIAL_STATE_MUST_BE_ENTRY_READY",
                key,
            )

    decision = (
        validate_strategy_b_lifecycle_state_replacement(
            current,
            proposed,
        )
    )

    if not decision.allowed:
        return _result(
            False,
            decision.reason,
            key,
        )

    if decision.identical:
        return _result(
            True,
            "IDENTICAL_STATE",
            key,
        )

    replacement = (
        serialize_strategy_b_lifecycle_state(
            proposed
        )
    )

    expected_token = (
        _EXPECT_MISSING
        if current_text is None
        else current_text
    )

    response = redis_client.eval(
        _ATOMIC_CAS_LUA,
        1,
        key,
        expected_token,
        replacement,
        STRATEGY_B_LIFECYCLE_STATE_TTL_SECONDS,
    )

    ok, reason = _parse_eval_result(
        response
    )

    if not ok:
        return _result(
            False,
            reason,
            key,
        )

    if reason != "COMMITTED":
        return _result(
            False,
            "REDIS_CAS_INVALID_RESPONSE",
            key,
        )

    return _result(
        True,
        "COMMITTED",
        key,
    )


__all__ = [
    "STRATEGY_B_REDIS_ROOT",
    "STRATEGY_B_LIFECYCLE_STATE_PREFIX",
    "STRATEGY_B_LIFECYCLE_STATE_TTL_SECONDS",
    "StrategyBLifecycleStateRedisResult",
    "lifecycle_state_key",
    "load_strategy_b_lifecycle_state",
    "persist_strategy_b_lifecycle_state_atomic",
]
