"""
Strategy-B external lifecycle evidence contracts.

These immutable values describe externally observed lifecycle facts.

They DO NOT:
- place, modify, or cancel orders;
- open or close broker positions;
- write Redis or other persistence;
- reserve risk;
- mutate Strategy-A;
- provide execution or broker authority.

The canonical Strategy-B research identity is carried unchanged through
external lifecycle boundaries by:

    thesis_id
    selected_capture_id
    symbol
    side

The selected_capture_id is the causal ENTRY_READY capture selected for the
expression and must not be replaced by later observations.
"""

from dataclasses import dataclass
from typing import Any


_VALID_SIDES = frozenset({"BUY", "SELL"})


def _required_text(name: str, value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    return value.strip()


def _side(value: Any) -> str:
    side = _required_text("side", value).upper()
    if side not in _VALID_SIDES:
        raise ValueError("side must be BUY or SELL")
    return side


def _positive_ms(name: str, value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _finite_number(name: str, value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be numeric")

    result = float(value)

    if result != result or result in (float("inf"), float("-inf")):
        raise ValueError(f"{name} must be finite")

    return result


def _positive_price(name: str, value: Any) -> float:
    result = _finite_number(name, value)
    if result <= 0.0:
        raise ValueError(f"{name} must be positive")
    return result


@dataclass(frozen=True)
class StrategyBOrderAcceptedEvidence:
    """
    External evidence that a Strategy-B order was accepted.

    Constructing this value does not submit an order.
    """

    thesis_id: str
    selected_capture_id: str
    symbol: str
    side: str
    order_intent_id: str
    broker_order_id: str
    accepted_at_ms: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "thesis_id", _required_text("thesis_id", self.thesis_id))
        object.__setattr__(
            self,
            "selected_capture_id",
            _required_text("selected_capture_id", self.selected_capture_id),
        )
        object.__setattr__(self, "symbol", _required_text("symbol", self.symbol).upper())
        object.__setattr__(self, "side", _side(self.side))
        object.__setattr__(
            self,
            "order_intent_id",
            _required_text("order_intent_id", self.order_intent_id),
        )
        object.__setattr__(
            self,
            "broker_order_id",
            _required_text("broker_order_id", self.broker_order_id),
        )
        object.__setattr__(
            self,
            "accepted_at_ms",
            _positive_ms("accepted_at_ms", self.accepted_at_ms),
        )


@dataclass(frozen=True)
class StrategyBPositionOpenedEvidence:
    """
    External evidence that the corresponding broker position exists.

    Constructing this value does not open a position.
    """

    thesis_id: str
    selected_capture_id: str
    symbol: str
    side: str
    broker_order_id: str
    broker_position_id: str
    opened_at_ms: int
    entry_price: float

    def __post_init__(self) -> None:
        object.__setattr__(self, "thesis_id", _required_text("thesis_id", self.thesis_id))
        object.__setattr__(
            self,
            "selected_capture_id",
            _required_text("selected_capture_id", self.selected_capture_id),
        )
        object.__setattr__(self, "symbol", _required_text("symbol", self.symbol).upper())
        object.__setattr__(self, "side", _side(self.side))
        object.__setattr__(
            self,
            "broker_order_id",
            _required_text("broker_order_id", self.broker_order_id),
        )
        object.__setattr__(
            self,
            "broker_position_id",
            _required_text("broker_position_id", self.broker_position_id),
        )
        object.__setattr__(
            self,
            "opened_at_ms",
            _positive_ms("opened_at_ms", self.opened_at_ms),
        )
        object.__setattr__(
            self,
            "entry_price",
            _positive_price("entry_price", self.entry_price),
        )


@dataclass(frozen=True)
class StrategyBPositionClosedEvidence:
    """
    External evidence that the corresponding broker position is closed.

    Constructing this value does not close a position and does not write
    the Strategy-B calibration OUTCOME record.
    """

    thesis_id: str
    selected_capture_id: str
    symbol: str
    side: str
    broker_position_id: str
    closed_at_ms: int
    entry_price: float
    exit_price: float
    close_reason: str
    net_profit: float
    net_r: float
    mfe_r: float
    mae_r: float

    def __post_init__(self) -> None:
        object.__setattr__(self, "thesis_id", _required_text("thesis_id", self.thesis_id))
        object.__setattr__(
            self,
            "selected_capture_id",
            _required_text("selected_capture_id", self.selected_capture_id),
        )
        object.__setattr__(self, "symbol", _required_text("symbol", self.symbol).upper())
        object.__setattr__(self, "side", _side(self.side))
        object.__setattr__(
            self,
            "broker_position_id",
            _required_text("broker_position_id", self.broker_position_id),
        )
        object.__setattr__(
            self,
            "closed_at_ms",
            _positive_ms("closed_at_ms", self.closed_at_ms),
        )
        object.__setattr__(
            self,
            "entry_price",
            _positive_price("entry_price", self.entry_price),
        )
        object.__setattr__(
            self,
            "exit_price",
            _positive_price("exit_price", self.exit_price),
        )
        object.__setattr__(
            self,
            "close_reason",
            _required_text("close_reason", self.close_reason),
        )
        object.__setattr__(
            self,
            "net_profit",
            _finite_number("net_profit", self.net_profit),
        )
        object.__setattr__(self, "net_r", _finite_number("net_r", self.net_r))
        object.__setattr__(self, "mfe_r", _finite_number("mfe_r", self.mfe_r))
        object.__setattr__(self, "mae_r", _finite_number("mae_r", self.mae_r))


__all__ = [
    "StrategyBOrderAcceptedEvidence",
    "StrategyBPositionOpenedEvidence",
    "StrategyBPositionClosedEvidence",
]
