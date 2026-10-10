from __future__ import annotations

from typing import Any, Mapping, Optional


STRATEGY_A = "A"
STRATEGY_B = "B"
VALID_EXECUTION_STRATEGIES = frozenset({
    STRATEGY_A,
    STRATEGY_B,
})


def normalize_execution_strategy(value: Any) -> Optional[str]:
    """
    Normalize explicit execution-strategy ownership.

    Backward compatibility:
      missing / None / blank -> Strategy A

    Accepted:
      A -> A
      B -> B

    Unknown values fail closed:
      return None
    """
    if value is None:
        return STRATEGY_A

    strategy = str(value).strip().upper()

    if not strategy:
        return STRATEGY_A

    if strategy in VALID_EXECUTION_STRATEGIES:
        return strategy

    return None


def execution_strategy_from_record(
    record: Mapping[str, Any] | None,
) -> Optional[str]:
    """
    Resolve strategy ownership from an event/trade record.

    Legacy records without a strategy field remain Strategy A.
    Unknown explicit ownership fails closed.
    """
    if not isinstance(record, Mapping):
        return None

    return normalize_execution_strategy(
        record.get("strategy")
    )


def is_strategy_a_record(
    record: Mapping[str, Any] | None,
) -> bool:
    return execution_strategy_from_record(record) == STRATEGY_A


def is_strategy_b_record(
    record: Mapping[str, Any] | None,
) -> bool:
    return execution_strategy_from_record(record) == STRATEGY_B
