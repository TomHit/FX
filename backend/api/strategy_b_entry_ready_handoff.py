from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True)
class StrategyBEntryReadyHandoff:
    strategy: str
    thesis_id: str
    selected_capture_id: str
    symbol: str
    side: str

    setup_entry_price: float
    structural_invalidation_price: float
    structural_buffer_price: float

    created_at_ms: int

    observation_only: bool = True
    risk_reservation_authority: bool = False
    broker_lot_normalization_authority: bool = False
    order_authority: bool = False
    broker_authority: bool = False
    execution_authority: bool = False
    strategy_a_mutation: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class StrategyBEntryReadyHandoffDecision:
    allowed: bool
    reason: str
    handoff: StrategyBEntryReadyHandoff | None


def _text(value: Any) -> str:
    return str(value or "").strip()


def _upper(value: Any) -> str:
    return _text(value).upper()


def _positive_finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None

    if not math.isfinite(number) or number <= 0:
        return None

    return number


def _identity_tuple(
    value: StrategyBEntryReadyHandoff,
) -> tuple[Any, ...]:
    return (
        value.strategy,
        value.thesis_id,
        value.selected_capture_id,
        value.symbol,
        value.side,
        value.setup_entry_price,
        value.structural_invalidation_price,
        value.structural_buffer_price,
        value.created_at_ms,
    )


def build_strategy_b_entry_ready_handoff(
    *,
    thesis_id: Any,
    selected_capture_id: Any,
    symbol: Any,
    side: Any,
    setup_entry_price: Any,
    structural_invalidation_price: Any,
    structural_buffer_price: Any,
    created_at_ms: Any,
) -> StrategyBEntryReadyHandoffDecision:
    """
    Build immutable Strategy-B ENTRY_READY execution evidence.

    This contract carries only causal Strategy-B setup identity and structural
    inputs. It does not select an execution account, size risk, normalize lots,
    reserve risk, generate TP, place orders, contact MT5, or mutate Strategy A.

    The final proposed stop remains owned by Strategy-B Point-3.
    """
    thesis = _text(thesis_id)
    capture = _text(selected_capture_id)
    sym = _upper(symbol)
    direction = _upper(side)

    setup = _positive_finite(setup_entry_price)
    invalidation = _positive_finite(
        structural_invalidation_price
    )
    buffer_price = _positive_finite(
        structural_buffer_price
    )

    try:
        created = int(created_at_ms)
    except (TypeError, ValueError):
        created = 0

    if not thesis:
        return StrategyBEntryReadyHandoffDecision(
            False,
            "THESIS_ID_REQUIRED",
            None,
        )

    if not capture:
        return StrategyBEntryReadyHandoffDecision(
            False,
            "SELECTED_CAPTURE_ID_REQUIRED",
            None,
        )

    if not sym:
        return StrategyBEntryReadyHandoffDecision(
            False,
            "SYMBOL_REQUIRED",
            None,
        )

    if direction not in ("BUY", "SELL"):
        return StrategyBEntryReadyHandoffDecision(
            False,
            "SIDE_INVALID",
            None,
        )

    if setup is None:
        return StrategyBEntryReadyHandoffDecision(
            False,
            "SETUP_ENTRY_PRICE_INVALID",
            None,
        )

    if invalidation is None:
        return StrategyBEntryReadyHandoffDecision(
            False,
            "STRUCTURAL_INVALIDATION_PRICE_INVALID",
            None,
        )

    if buffer_price is None:
        return StrategyBEntryReadyHandoffDecision(
            False,
            "STRUCTURAL_BUFFER_PRICE_INVALID",
            None,
        )

    if created <= 0:
        return StrategyBEntryReadyHandoffDecision(
            False,
            "CREATED_AT_MS_INVALID",
            None,
        )

    handoff = StrategyBEntryReadyHandoff(
        strategy="B",
        thesis_id=thesis,
        selected_capture_id=capture,
        symbol=sym,
        side=direction,
        setup_entry_price=setup,
        structural_invalidation_price=invalidation,
        structural_buffer_price=buffer_price,
        created_at_ms=created,
    )

    return StrategyBEntryReadyHandoffDecision(
        True,
        "ENTRY_READY_HANDOFF_READY",
        handoff,
    )


def validate_strategy_b_entry_ready_handoff_replacement(
    current: StrategyBEntryReadyHandoff | None,
    proposed: StrategyBEntryReadyHandoff,
) -> StrategyBEntryReadyHandoffDecision:
    """
    Immutable replacement contract.

    Missing -> proposed:
        allowed.

    Existing identical -> proposed:
        allowed as idempotent.

    Existing different -> proposed:
        rejected. ENTRY_READY causal evidence cannot be rewritten.
    """
    if not isinstance(
        proposed,
        StrategyBEntryReadyHandoff,
    ):
        return StrategyBEntryReadyHandoffDecision(
            False,
            "PROPOSED_HANDOFF_REQUIRED",
            None,
        )

    if current is None:
        return StrategyBEntryReadyHandoffDecision(
            True,
            "HANDOFF_AVAILABLE",
            proposed,
        )

    if not isinstance(
        current,
        StrategyBEntryReadyHandoff,
    ):
        return StrategyBEntryReadyHandoffDecision(
            False,
            "CURRENT_HANDOFF_INVALID",
            None,
        )

    if _identity_tuple(current) == _identity_tuple(proposed):
        return StrategyBEntryReadyHandoffDecision(
            True,
            "IDENTICAL_HANDOFF",
            current,
        )

    return StrategyBEntryReadyHandoffDecision(
        False,
        "ENTRY_READY_HANDOFF_IMMUTABLE",
        current,
    )
