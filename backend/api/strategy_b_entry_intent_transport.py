from __future__ import annotations

import json
from typing import Any, Mapping


STRATEGY_B_ENTRY_INTENT_INDEX_KEY = "xtl:strategy_b:entry_intent:index"
STRATEGY_B_ENTRY_INTENT_PREFIX = "xtl:strategy_b:entry_intent:"
STRATEGY_B_ENTRY_INTENT_TTL_SEC = 24 * 60 * 60
STRATEGY_B_ENTRY_INTENT_MAX_ACTIVE = 100


def _txt(value: Any) -> str:
    return str(value or "").strip()


def _upper(value: Any) -> str:
    return _txt(value).upper()


def _positive_float(value: Any) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return 0.0
    return out if out > 0.0 else 0.0


def _positive_int(value: Any) -> int:
    try:
        out = int(value)
    except (TypeError, ValueError):
        return 0
    return out if out > 0 else 0


def strategy_b_entry_intent_key(thesis_id: str) -> str:
    thesis = _txt(thesis_id)
    if not thesis:
        raise ValueError("STRATEGY_B_ENTRY_INTENT_THESIS_REQUIRED")
    return f"{STRATEGY_B_ENTRY_INTENT_PREFIX}{thesis}"


def build_strategy_b_entry_intent(
    *,
    thesis_id: str,
    selected_capture_id: str,
    symbol: str,
    side: str,
    setup_entry_price: Any,
    structural_invalidation_price: Any,
    structural_buffer_price: Any,
    expected_dxy_direction: str,
    created_at_ms: Any,
) -> dict:
    thesis = _txt(thesis_id)
    capture = _txt(selected_capture_id)
    symbol_u = _upper(symbol)
    side_u = _upper(side)
    dxy_u = _upper(expected_dxy_direction)

    setup_px = _positive_float(setup_entry_price)
    invalidation_px = _positive_float(structural_invalidation_price)
    buffer_px = _positive_float(structural_buffer_price)
    created_ms = _positive_int(created_at_ms)

    if not thesis:
        raise ValueError("STRATEGY_B_ENTRY_INTENT_THESIS_REQUIRED")
    if not capture:
        raise ValueError("STRATEGY_B_ENTRY_INTENT_CAPTURE_REQUIRED")
    if not symbol_u:
        raise ValueError("STRATEGY_B_ENTRY_INTENT_SYMBOL_REQUIRED")
    if side_u not in {"BUY", "SELL"}:
        raise ValueError("STRATEGY_B_ENTRY_INTENT_SIDE_INVALID")
    if setup_px <= 0:
        raise ValueError("STRATEGY_B_ENTRY_INTENT_SETUP_PRICE_REQUIRED")
    if invalidation_px <= 0:
        raise ValueError("STRATEGY_B_ENTRY_INTENT_INVALIDATION_REQUIRED")
    if buffer_px <= 0:
        raise ValueError("STRATEGY_B_ENTRY_INTENT_BUFFER_REQUIRED")
    if dxy_u not in {"BULLISH", "BEARISH"}:
        raise ValueError("STRATEGY_B_ENTRY_INTENT_DXY_DIRECTION_INVALID")
    if created_ms <= 0:
        raise ValueError("STRATEGY_B_ENTRY_INTENT_CREATED_AT_REQUIRED")

    return {
        "schema_version": "1",
        "strategy": "B",
        "thesis_id": thesis,
        "selected_capture_id": capture,
        "symbol": symbol_u,
        "side": side_u,
        "setup_entry_price": setup_px,
        "structural_invalidation_price": invalidation_px,
        "structural_buffer_price": buffer_px,
        "expected_dxy_direction": dxy_u,
        "created_at_ms": created_ms,
    }


def publish_strategy_b_entry_intent(
    redis_client,
    *,
    intent: Mapping[str, Any],
) -> dict:
    canonical = build_strategy_b_entry_intent(
        thesis_id=intent.get("thesis_id"),
        selected_capture_id=intent.get("selected_capture_id"),
        symbol=intent.get("symbol"),
        side=intent.get("side"),
        setup_entry_price=intent.get("setup_entry_price"),
        structural_invalidation_price=intent.get(
            "structural_invalidation_price"
        ),
        structural_buffer_price=intent.get(
            "structural_buffer_price"
        ),
        expected_dxy_direction=intent.get(
            "expected_dxy_direction"
        ),
        created_at_ms=intent.get("created_at_ms"),
    )

    thesis_id = canonical["thesis_id"]
    key = strategy_b_entry_intent_key(thesis_id)
    payload = json.dumps(
        canonical,
        sort_keys=True,
        separators=(",", ":"),
    )

    created = bool(
        redis_client.set(
            key,
            payload,
            nx=True,
            ex=STRATEGY_B_ENTRY_INTENT_TTL_SEC,
        )
    )

    if created:
        redis_client.zadd(
            STRATEGY_B_ENTRY_INTENT_INDEX_KEY,
            {thesis_id: canonical["created_at_ms"]},
        )

        # Bounded global index. Intent payloads themselves are TTL-bound.
        count = int(
            redis_client.zcard(
                STRATEGY_B_ENTRY_INTENT_INDEX_KEY
            )
            or 0
        )
        overflow = count - STRATEGY_B_ENTRY_INTENT_MAX_ACTIVE
        if overflow > 0:
            redis_client.zremrangebyrank(
                STRATEGY_B_ENTRY_INTENT_INDEX_KEY,
                0,
                overflow - 1,
            )

    return {
        "ok": True,
        "created": created,
        "reason": (
            "STRATEGY_B_ENTRY_INTENT_PUBLISHED"
            if created
            else "STRATEGY_B_ENTRY_INTENT_ALREADY_EXISTS"
        ),
        "thesis_id": thesis_id,
        "key": key,
    }


def load_strategy_b_entry_intents(
    redis_client,
    *,
    limit: int = 40,
) -> list[dict]:
    bounded_limit = max(
        1,
        min(int(limit or 1), STRATEGY_B_ENTRY_INTENT_MAX_ACTIVE),
    )

    thesis_ids = redis_client.zrevrange(
        STRATEGY_B_ENTRY_INTENT_INDEX_KEY,
        0,
        bounded_limit - 1,
    ) or []

    out: list[dict] = []

    for raw_thesis in thesis_ids:
        thesis_id = (
            raw_thesis.decode("utf-8", "ignore")
            if isinstance(raw_thesis, (bytes, bytearray))
            else _txt(raw_thesis)
        )
        thesis_id = _txt(thesis_id)
        if not thesis_id:
            continue

        raw = redis_client.get(
            strategy_b_entry_intent_key(thesis_id)
        )

        if raw is None:
            # Stale bounded index member; exact cleanup only.
            redis_client.zrem(
                STRATEGY_B_ENTRY_INTENT_INDEX_KEY,
                thesis_id,
            )
            continue

        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", "ignore")

        try:
            parsed = json.loads(raw)
        except Exception:
            continue

        if not isinstance(parsed, dict):
            continue

        try:
            canonical = build_strategy_b_entry_intent(
                thesis_id=parsed.get("thesis_id"),
                selected_capture_id=parsed.get(
                    "selected_capture_id"
                ),
                symbol=parsed.get("symbol"),
                side=parsed.get("side"),
                setup_entry_price=parsed.get(
                    "setup_entry_price"
                ),
                structural_invalidation_price=parsed.get(
                    "structural_invalidation_price"
                ),
                structural_buffer_price=parsed.get(
                    "structural_buffer_price"
                ),
                expected_dxy_direction=parsed.get(
                    "expected_dxy_direction"
                ),
                created_at_ms=parsed.get("created_at_ms"),
            )
        except Exception:
            continue

        if canonical["thesis_id"] != thesis_id:
            continue

        out.append(canonical)

    return out


__all__ = [
    "STRATEGY_B_ENTRY_INTENT_INDEX_KEY",
    "STRATEGY_B_ENTRY_INTENT_MAX_ACTIVE",
    "STRATEGY_B_ENTRY_INTENT_TTL_SEC",
    "build_strategy_b_entry_intent",
    "load_strategy_b_entry_intents",
    "publish_strategy_b_entry_intent",
    "strategy_b_entry_intent_key",
]
