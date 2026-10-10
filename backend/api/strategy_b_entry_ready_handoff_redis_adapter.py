from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from typing import Any, Mapping

from api.strategy_b_entry_ready_handoff import (
    StrategyBEntryReadyHandoff,
    validate_strategy_b_entry_ready_handoff_replacement,
)


STRATEGY_B_ENTRY_READY_HANDOFF_SCHEMA = (
    "xtl.strategy_b.entry_ready_handoff"
)

STRATEGY_B_ENTRY_READY_HANDOFF_SCHEMA_VERSION = 1

STRATEGY_B_ENTRY_READY_HANDOFF_PREFIX = (
    "xtl:strategy_b:entry_ready:handoff"
)

_SAFE_THESIS_ID = re.compile(
    r"^[A-Za-z0-9._-]+$"
)

_EXPECT_MISSING = (
    "__XTL_STRATEGY_B_EXPECT_MISSING__"
)


_FIELDS = {
    "schema",
    "schema_version",
    "strategy",
    "thesis_id",
    "selected_capture_id",
    "symbol",
    "side",
    "setup_entry_price",
    "structural_invalidation_price",
    "structural_buffer_price",
    "created_at_ms",
    "observation_only",
    "risk_reservation_authority",
    "broker_lot_normalization_authority",
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
class StrategyBEntryReadyHandoffRedisResult:
    ok: bool
    reason: str
    key: str


def _result(
    ok: bool,
    reason: str,
    key: str,
) -> StrategyBEntryReadyHandoffRedisResult:
    return StrategyBEntryReadyHandoffRedisResult(
        ok=bool(ok),
        reason=str(reason),
        key=str(key),
    )


def _text(raw: Any) -> str | None:
    if raw is None:
        return None

    if isinstance(raw, bytes):
        try:
            return raw.decode("utf-8")
        except Exception as exc:
            raise ValueError(
                "CURRENT_HANDOFF_CORRUPT"
            ) from exc

    if isinstance(raw, str):
        return raw

    raise ValueError(
        "CURRENT_HANDOFF_CORRUPT"
    )


def entry_ready_handoff_key(
    thesis_id: str,
) -> str:
    if not isinstance(thesis_id, str):
        raise TypeError(
            "thesis_id must be str"
        )

    tid = thesis_id.strip()

    if not tid:
        raise ValueError(
            "THESIS_ID_REQUIRED"
        )

    if not _SAFE_THESIS_ID.fullmatch(tid):
        raise ValueError(
            "UNSAFE_THESIS_ID"
        )

    key = (
        f"{STRATEGY_B_ENTRY_READY_HANDOFF_PREFIX}:"
        f"{tid}"
    )

    if not key.startswith(
        "xtl:strategy_b:"
    ):
        raise ValueError(
            "STRATEGY_B_NAMESPACE_VIOLATION"
        )

    return key


def _payload(
    handoff: StrategyBEntryReadyHandoff,
) -> dict[str, Any]:
    if not isinstance(
        handoff,
        StrategyBEntryReadyHandoff,
    ):
        raise TypeError(
            "StrategyBEntryReadyHandoff required"
        )

    return {
        "schema": (
            STRATEGY_B_ENTRY_READY_HANDOFF_SCHEMA
        ),
        "schema_version": (
            STRATEGY_B_ENTRY_READY_HANDOFF_SCHEMA_VERSION
        ),
        "strategy": handoff.strategy,
        "thesis_id": handoff.thesis_id,
        "selected_capture_id": (
            handoff.selected_capture_id
        ),
        "symbol": handoff.symbol,
        "side": handoff.side,
        "setup_entry_price": (
            handoff.setup_entry_price
        ),
        "structural_invalidation_price": (
            handoff.structural_invalidation_price
        ),
        "structural_buffer_price": (
            handoff.structural_buffer_price
        ),
        "created_at_ms": handoff.created_at_ms,
        "observation_only": (
            handoff.observation_only
        ),
        "risk_reservation_authority": (
            handoff.risk_reservation_authority
        ),
        "broker_lot_normalization_authority": (
            handoff.broker_lot_normalization_authority
        ),
        "order_authority": (
            handoff.order_authority
        ),
        "broker_authority": (
            handoff.broker_authority
        ),
        "execution_authority": (
            handoff.execution_authority
        ),
        "strategy_a_mutation": (
            handoff.strategy_a_mutation
        ),
    }


def serialize_strategy_b_entry_ready_handoff(
    handoff: StrategyBEntryReadyHandoff,
) -> str:
    return json.dumps(
        _payload(handoff),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def deserialize_strategy_b_entry_ready_handoff(
    raw: str,
) -> StrategyBEntryReadyHandoff:
    if not isinstance(raw, str) or not raw:
        raise ValueError(
            "MALFORMED_HANDOFF_PAYLOAD"
        )

    try:
        value = json.loads(raw)
    except Exception as exc:
        raise ValueError(
            "MALFORMED_HANDOFF_PAYLOAD"
        ) from exc

    if not isinstance(value, Mapping):
        raise ValueError(
            "MALFORMED_HANDOFF_PAYLOAD"
        )

    if set(value.keys()) != _FIELDS:
        raise ValueError(
            "MALFORMED_HANDOFF_PAYLOAD"
        )

    if (
        value["schema"]
        != STRATEGY_B_ENTRY_READY_HANDOFF_SCHEMA
    ):
        raise ValueError(
            "UNKNOWN_HANDOFF_SCHEMA"
        )

    if (
        value["schema_version"]
        != STRATEGY_B_ENTRY_READY_HANDOFF_SCHEMA_VERSION
    ):
        raise ValueError(
            "UNKNOWN_HANDOFF_SCHEMA_VERSION"
        )

    try:
        handoff = StrategyBEntryReadyHandoff(
            strategy=value["strategy"],
            thesis_id=value["thesis_id"],
            selected_capture_id=value[
                "selected_capture_id"
            ],
            symbol=value["symbol"],
            side=value["side"],
            setup_entry_price=float(
                value["setup_entry_price"]
            ),
            structural_invalidation_price=float(
                value["structural_invalidation_price"]
            ),
            structural_buffer_price=float(
                value["structural_buffer_price"]
            ),
            created_at_ms=int(
                value["created_at_ms"]
            ),
            observation_only=value[
                "observation_only"
            ],
            risk_reservation_authority=value[
                "risk_reservation_authority"
            ],
            broker_lot_normalization_authority=value[
                "broker_lot_normalization_authority"
            ],
            order_authority=value[
                "order_authority"
            ],
            broker_authority=value[
                "broker_authority"
            ],
            execution_authority=value[
                "execution_authority"
            ],
            strategy_a_mutation=value[
                "strategy_a_mutation"
            ],
        )
    except Exception as exc:
        raise ValueError(
            "MALFORMED_HANDOFF_PAYLOAD"
        ) from exc

    if handoff.strategy != "B":
        raise ValueError(
            "HANDOFF_STRATEGY_CORRUPT"
        )

    if (
        handoff.observation_only is not True
        or handoff.risk_reservation_authority is not False
        or handoff.broker_lot_normalization_authority
        is not False
        or handoff.order_authority is not False
        or handoff.broker_authority is not False
        or handoff.execution_authority is not False
        or handoff.strategy_a_mutation is not False
    ):
        raise ValueError(
            "HANDOFF_AUTHORITY_CORRUPT"
        )

    required_text = (
        handoff.thesis_id,
        handoff.selected_capture_id,
        handoff.symbol,
        handoff.side,
    )

    if not all(
        isinstance(v, str) and v.strip()
        for v in required_text
    ):
        raise ValueError(
            "MALFORMED_HANDOFF_PAYLOAD"
        )

    if handoff.side not in ("BUY", "SELL"):
        raise ValueError(
            "MALFORMED_HANDOFF_PAYLOAD"
        )

    numeric = (
        handoff.setup_entry_price,
        handoff.structural_invalidation_price,
        handoff.structural_buffer_price,
    )

    if not all(
        math.isfinite(v) and v > 0
        for v in numeric
    ):
        raise ValueError(
            "MALFORMED_HANDOFF_PAYLOAD"
        )

    if handoff.created_at_ms <= 0:
        raise ValueError(
            "MALFORMED_HANDOFF_PAYLOAD"
        )

    return handoff


def _decode_current(
    raw: Any,
) -> StrategyBEntryReadyHandoff | None:
    text = _text(raw)

    if text is None:
        return None

    return (
        deserialize_strategy_b_entry_ready_handoff(
            text
        )
    )


def load_strategy_b_entry_ready_handoff(
    redis_client: Any,
    thesis_id: str,
) -> StrategyBEntryReadyHandoff | None:
    key = entry_ready_handoff_key(
        thesis_id
    )

    raw = redis_client.get(key)

    try:
        return _decode_current(raw)
    except Exception as exc:
        raise ValueError(
            "CURRENT_HANDOFF_CORRUPT"
        ) from exc


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


def persist_strategy_b_entry_ready_handoff_atomic(
    redis_client: Any,
    proposed: StrategyBEntryReadyHandoff,
) -> StrategyBEntryReadyHandoffRedisResult:
    if not isinstance(
        proposed,
        StrategyBEntryReadyHandoff,
    ):
        raise TypeError(
            "StrategyBEntryReadyHandoff required"
        )

    key = entry_ready_handoff_key(
        proposed.thesis_id
    )

    # Exact-key read only. No SCAN / KEYS / wildcard.
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
            "CURRENT_HANDOFF_CORRUPT",
            key,
        )

    decision = (
        validate_strategy_b_entry_ready_handoff_replacement(
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

    if (
        current is not None
        and decision.reason == "IDENTICAL_HANDOFF"
    ):
        return _result(
            True,
            "IDENTICAL_HANDOFF",
            key,
        )

    replacement = (
        serialize_strategy_b_entry_ready_handoff(
            proposed
        )
    )

    expected = (
        _EXPECT_MISSING
        if current_text is None
        else current_text
    )

    try:
        raw_result = redis_client.eval(
            _ATOMIC_CAS_LUA,
            1,
            key,
            expected,
            replacement,
            _EXPECT_MISSING,
        )
    except Exception:
        return _result(
            False,
            "REDIS_CAS_FAILED",
            key,
        )

    ok, reason = _parse_eval_result(
        raw_result
    )

    return _result(
        ok,
        reason,
        key,
    )
