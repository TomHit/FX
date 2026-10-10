from __future__ import annotations

from dataclasses import dataclass
from typing import Any


STRATEGY_B_EXECUTION_UID_KEY = (
    "xtl:strategy_b:execution:uid"
)


@dataclass(frozen=True)
class StrategyBExecutionUidRedisResult:
    ok: bool
    reason: str
    uid: str | None


def _normalize_uid(value: Any) -> str:
    if value is None:
        return ""

    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8")
        except Exception:
            return ""

    if not isinstance(value, str):
        return ""

    return value.strip()


def load_strategy_b_execution_uid(
    redis_client: Any,
) -> str | None:
    """
    Exact-key read of the explicitly configured Strategy-B execution UID.

    No user selection, profile/account/device resolution, enumeration,
    risk, reservation, order, broker, MT5, or Strategy-A mutation.
    """
    raw = redis_client.get(
        STRATEGY_B_EXECUTION_UID_KEY
    )

    uid = _normalize_uid(raw)

    return uid or None


def store_strategy_b_execution_uid(
    redis_client: Any,
    uid: str,
) -> StrategyBExecutionUidRedisResult:
    """
    Explicit configuration write only.

    This does not prove that the UID is currently enabled. Runtime
    validation remains owned by validate_strategy_b_execution_uid().
    """
    normalized = _normalize_uid(uid)

    if not normalized:
        return StrategyBExecutionUidRedisResult(
            ok=False,
            reason="STRATEGY_B_EXECUTION_UID_REQUIRED",
            uid=None,
        )

    redis_client.set(
        STRATEGY_B_EXECUTION_UID_KEY,
        normalized,
    )

    return StrategyBExecutionUidRedisResult(
        ok=True,
        reason="STRATEGY_B_EXECUTION_UID_STORED",
        uid=normalized,
    )


def clear_strategy_b_execution_uid(
    redis_client: Any,
) -> StrategyBExecutionUidRedisResult:
    redis_client.delete(
        STRATEGY_B_EXECUTION_UID_KEY
    )

    return StrategyBExecutionUidRedisResult(
        ok=True,
        reason="STRATEGY_B_EXECUTION_UID_CLEARED",
        uid=None,
    )


__all__ = [
    "STRATEGY_B_EXECUTION_UID_KEY",
    "StrategyBExecutionUidRedisResult",
    "load_strategy_b_execution_uid",
    "store_strategy_b_execution_uid",
    "clear_strategy_b_execution_uid",
]
