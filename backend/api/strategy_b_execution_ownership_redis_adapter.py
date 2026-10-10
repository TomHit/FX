from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Mapping

from api.strategy_b_execution_ownership import (
    StrategyBExecutionOwnership,
    validate_strategy_b_execution_ownership_replacement,
)


STRATEGY_B_EXECUTION_OWNERSHIP_SCHEMA = (
    "xtl.strategy_b.execution_ownership"
)
STRATEGY_B_EXECUTION_OWNERSHIP_SCHEMA_VERSION = 1

STRATEGY_B_EXECUTION_OWNERSHIP_PREFIX = (
    "xtl:strategy_b:execution:ownership"
)

_EXPECT_MISSING = "__XTL_STRATEGY_B_EXPECT_MISSING__"


_FIELDS = {
    "schema",
    "schema_version",
    "thesis_id",
    "selected_capture_id",
    "symbol",
    "side",
    "uid",
    "profile_id",
    "device_id",
    "account_login",
    "account_server",
    "broker_company",
    "observation_only",
    "risk_reservation_authority",
    "order_authority",
    "broker_authority",
    "execution_authority",
    "strategy_a_mutation",
}


_ATOMIC_CAS_LUA = r"""
local key = KEYS[1]
local expected = ARGV[1]
local replacement = ARGV[2]
local missing = ARGV[3]

local current = redis.call("GET", key)

if expected == missing then
    if current then
        return {0, "CAS_CONFLICT"}
    end
else
    if not current or current ~= expected then
        return {0, "CAS_CONFLICT"}
    end
end

redis.call("SET", key, replacement)
return {1, "COMMITTED"}
"""


@dataclass(frozen=True)
class StrategyBExecutionOwnershipRedisResult:
    ok: bool
    reason: str
    key: str


def _result(
    ok: bool,
    reason: str,
    key: str,
) -> StrategyBExecutionOwnershipRedisResult:
    return StrategyBExecutionOwnershipRedisResult(
        ok=bool(ok),
        reason=str(reason),
        key=str(key),
    )


def _text(raw: Any) -> str | None:
    if raw is None:
        return None

    if isinstance(raw, bytes):
        return raw.decode("utf-8")

    if isinstance(raw, str):
        return raw

    raise TypeError("ownership payload must be bytes/str")


def execution_ownership_key(thesis_id: str) -> str:
    tid = str(thesis_id or "").strip()

    if not tid:
        raise ValueError("THESIS_ID_REQUIRED")

    return (
        f"{STRATEGY_B_EXECUTION_OWNERSHIP_PREFIX}:{tid}"
    )


def _payload(
    ownership: StrategyBExecutionOwnership,
) -> dict[str, Any]:
    if not isinstance(
        ownership,
        StrategyBExecutionOwnership,
    ):
        raise TypeError(
            "StrategyBExecutionOwnership required"
        )

    return {
        "schema": STRATEGY_B_EXECUTION_OWNERSHIP_SCHEMA,
        "schema_version": (
            STRATEGY_B_EXECUTION_OWNERSHIP_SCHEMA_VERSION
        ),
        "thesis_id": ownership.thesis_id,
        "selected_capture_id": ownership.selected_capture_id,
        "symbol": ownership.symbol,
        "side": ownership.side,
        "uid": ownership.uid,
        "profile_id": ownership.profile_id,
        "device_id": ownership.device_id,
        "account_login": ownership.account_login,
        "account_server": ownership.account_server,
        "broker_company": ownership.broker_company,
        "observation_only": ownership.observation_only,
        "risk_reservation_authority": (
            ownership.risk_reservation_authority
        ),
        "order_authority": ownership.order_authority,
        "broker_authority": ownership.broker_authority,
        "execution_authority": ownership.execution_authority,
        "strategy_a_mutation": ownership.strategy_a_mutation,
    }


def serialize_strategy_b_execution_ownership(
    ownership: StrategyBExecutionOwnership,
) -> str:
    return json.dumps(
        _payload(ownership),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def deserialize_strategy_b_execution_ownership(
    raw: str,
) -> StrategyBExecutionOwnership:
    if not isinstance(raw, str) or not raw:
        raise ValueError("MALFORMED_OWNERSHIP_PAYLOAD")

    try:
        value = json.loads(raw)
    except Exception as exc:
        raise ValueError(
            "MALFORMED_OWNERSHIP_PAYLOAD"
        ) from exc

    if not isinstance(value, Mapping):
        raise ValueError("MALFORMED_OWNERSHIP_PAYLOAD")

    if set(value.keys()) != _FIELDS:
        raise ValueError("MALFORMED_OWNERSHIP_PAYLOAD")

    if (
        value["schema"]
        != STRATEGY_B_EXECUTION_OWNERSHIP_SCHEMA
    ):
        raise ValueError("UNKNOWN_OWNERSHIP_SCHEMA")

    if (
        value["schema_version"]
        != STRATEGY_B_EXECUTION_OWNERSHIP_SCHEMA_VERSION
    ):
        raise ValueError(
            "UNKNOWN_OWNERSHIP_SCHEMA_VERSION"
        )

    try:
        ownership = StrategyBExecutionOwnership(
            thesis_id=value["thesis_id"],
            selected_capture_id=value[
                "selected_capture_id"
            ],
            symbol=value["symbol"],
            side=value["side"],
            uid=value["uid"],
            profile_id=value["profile_id"],
            device_id=value["device_id"],
            account_login=value["account_login"],
            account_server=value["account_server"],
            broker_company=value["broker_company"],
            observation_only=value["observation_only"],
            risk_reservation_authority=value[
                "risk_reservation_authority"
            ],
            order_authority=value["order_authority"],
            broker_authority=value["broker_authority"],
            execution_authority=value[
                "execution_authority"
            ],
            strategy_a_mutation=value[
                "strategy_a_mutation"
            ],
        )
    except Exception as exc:
        raise ValueError(
            "MALFORMED_OWNERSHIP_PAYLOAD"
        ) from exc

    # Serialized ownership must remain non-authoritative.
    if (
        ownership.observation_only is not True
        or ownership.risk_reservation_authority is not False
        or ownership.order_authority is not False
        or ownership.broker_authority is not False
        or ownership.execution_authority is not False
        or ownership.strategy_a_mutation is not False
    ):
        raise ValueError("OWNERSHIP_AUTHORITY_CORRUPT")

    # Reject empty or structurally invalid identity.
    required = (
        ownership.thesis_id,
        ownership.selected_capture_id,
        ownership.symbol,
        ownership.side,
        ownership.uid,
        ownership.profile_id,
        ownership.device_id,
        ownership.account_login,
        ownership.account_server,
        ownership.broker_company,
    )

    if not all(
        isinstance(v, str) and v.strip()
        for v in required
    ):
        raise ValueError("MALFORMED_OWNERSHIP_PAYLOAD")

    if ownership.side not in ("BUY", "SELL"):
        raise ValueError("MALFORMED_OWNERSHIP_PAYLOAD")

    return ownership


def _decode_current(
    raw: Any,
) -> StrategyBExecutionOwnership | None:
    text = _text(raw)

    if text is None:
        return None

    return deserialize_strategy_b_execution_ownership(
        text
    )


def load_strategy_b_execution_ownership(
    redis_client: Any,
    thesis_id: str,
) -> StrategyBExecutionOwnership | None:
    key = execution_ownership_key(thesis_id)
    raw = redis_client.get(key)

    try:
        return _decode_current(raw)
    except Exception as exc:
        raise ValueError(
            "CURRENT_OWNERSHIP_CORRUPT"
        ) from exc


def _parse_eval_result(
    value: Any,
) -> tuple[bool, str]:
    if (
        not isinstance(value, (list, tuple))
        or len(value) < 2
    ):
        return False, "REDIS_CAS_INVALID_RESPONSE"

    status, reason = value[0], value[1]

    if isinstance(reason, bytes):
        try:
            reason = reason.decode("utf-8")
        except Exception:
            return False, "REDIS_CAS_INVALID_RESPONSE"

    try:
        ok = int(status) == 1
    except Exception:
        return False, "REDIS_CAS_INVALID_RESPONSE"

    return bool(ok), str(reason)


def persist_strategy_b_execution_ownership_atomic(
    redis_client: Any,
    proposed: StrategyBExecutionOwnership,
) -> StrategyBExecutionOwnershipRedisResult:
    if not isinstance(
        proposed,
        StrategyBExecutionOwnership,
    ):
        raise TypeError(
            "StrategyBExecutionOwnership required"
        )

    key = execution_ownership_key(
        proposed.thesis_id
    )

    # Exact-key read. No SCAN/enumeration.
    raw_current = redis_client.get(key)

    try:
        current_text = _text(raw_current)
        current = _decode_current(raw_current)
    except Exception:
        return _result(
            False,
            "CURRENT_OWNERSHIP_CORRUPT",
            key,
        )

    decision = (
        validate_strategy_b_execution_ownership_replacement(
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

    if decision.reason == "IDENTICAL_OWNERSHIP":
        return _result(
            True,
            "IDENTICAL_OWNERSHIP",
            key,
        )

    replacement = (
        serialize_strategy_b_execution_ownership(
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
        _EXPECT_MISSING,
    )

    ok, reason = _parse_eval_result(response)

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
    "STRATEGY_B_EXECUTION_OWNERSHIP_SCHEMA",
    "STRATEGY_B_EXECUTION_OWNERSHIP_SCHEMA_VERSION",
    "STRATEGY_B_EXECUTION_OWNERSHIP_PREFIX",
    "StrategyBExecutionOwnershipRedisResult",
    "execution_ownership_key",
    "serialize_strategy_b_execution_ownership",
    "deserialize_strategy_b_execution_ownership",
    "load_strategy_b_execution_ownership",
    "persist_strategy_b_execution_ownership_atomic",
]
