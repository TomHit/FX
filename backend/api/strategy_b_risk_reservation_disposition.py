"""
Pure Strategy-B pending-risk reservation disposition contract.

This module decides whether an existing Strategy-B pending-risk reservation
must remain protected, is eligible for authoritative release, or is eligible
for transfer out of the pending ledger because the exact broker-live position
is already represented by a fresh account-risk snapshot.

It does NOT:
- read or write Redis;
- reserve or release risk;
- place, modify, or cancel orders;
- open or close broker positions;
- mutate lifecycle state;
- mutate Strategy-A;
- provide execution, order, broker, or Redis authority.
"""

from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Any, Optional


_VALID_SIDES = frozenset({"BUY", "SELL"})
_TERMINAL_ORDER_STATUSES = frozenset(
    {"REJECTED", "CANCELLED", "EXPIRED"}
)


def _required_text(name: str, value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    return value.strip()


def _side(value: Any) -> str:
    value = _required_text("side", value).upper()
    if value not in _VALID_SIDES:
        raise ValueError("side must be BUY or SELL")
    return value


def _positive_ms(name: str, value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _positive_number(name: str, value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be numeric")

    result = float(value)

    if not math.isfinite(result) or result <= 0.0:
        raise ValueError(f"{name} must be positive and finite")

    return result


@dataclass(frozen=True)
class StrategyBOrderTerminalEvidence:
    thesis_id: str
    selected_capture_id: str
    symbol: str
    side: str
    broker_order_id: str
    terminal_status: str
    terminal_at_ms: int
    position_opened: bool

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "thesis_id",
            _required_text("thesis_id", self.thesis_id),
        )
        object.__setattr__(
            self,
            "selected_capture_id",
            _required_text(
                "selected_capture_id",
                self.selected_capture_id,
            ),
        )
        object.__setattr__(
            self,
            "symbol",
            _required_text("symbol", self.symbol).upper(),
        )
        object.__setattr__(self, "side", _side(self.side))
        object.__setattr__(
            self,
            "broker_order_id",
            _required_text(
                "broker_order_id",
                self.broker_order_id,
            ),
        )

        status = _required_text(
            "terminal_status",
            self.terminal_status,
        ).upper()

        if status not in _TERMINAL_ORDER_STATUSES:
            raise ValueError("unsupported terminal_status")

        object.__setattr__(self, "terminal_status", status)
        object.__setattr__(
            self,
            "terminal_at_ms",
            _positive_ms(
                "terminal_at_ms",
                self.terminal_at_ms,
            ),
        )

        if not isinstance(self.position_opened, bool):
            raise TypeError("position_opened must be bool")


@dataclass(frozen=True)
class StrategyBBrokerRiskRepresentation:
    broker_position_id: str
    symbol: str
    side: str
    represented_risk_usd: float
    snapshot_at_ms: int
    now_ms: int
    snapshot_max_age_ms: int

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "broker_position_id",
            _required_text(
                "broker_position_id",
                self.broker_position_id,
            ),
        )
        object.__setattr__(
            self,
            "symbol",
            _required_text("symbol", self.symbol).upper(),
        )
        object.__setattr__(self, "side", _side(self.side))
        object.__setattr__(
            self,
            "represented_risk_usd",
            _positive_number(
                "represented_risk_usd",
                self.represented_risk_usd,
            ),
        )
        object.__setattr__(
            self,
            "snapshot_at_ms",
            _positive_ms(
                "snapshot_at_ms",
                self.snapshot_at_ms,
            ),
        )
        object.__setattr__(
            self,
            "now_ms",
            _positive_ms("now_ms", self.now_ms),
        )
        object.__setattr__(
            self,
            "snapshot_max_age_ms",
            _positive_ms(
                "snapshot_max_age_ms",
                self.snapshot_max_age_ms,
            ),
        )


@dataclass(frozen=True)
class StrategyBReservationDisposition:
    disposition: str
    reason: str
    release_authorized: bool

    observation_only: bool = True
    execution_authority: bool = False
    order_authority: bool = False
    broker_authority: bool = False
    redis_authority: bool = False
    strategy_a_mutation: bool = False


def _result(
    disposition: str,
    reason: str,
    *,
    release_authorized: bool = False,
) -> StrategyBReservationDisposition:
    return StrategyBReservationDisposition(
        disposition=disposition,
        reason=reason,
        release_authorized=release_authorized,
    )


def decide_strategy_b_reservation_disposition(
    *,
    thesis_id: str,
    selected_capture_id: str,
    symbol: str,
    side: str,
    reservation_id: str,
    reserved_risk_usd: Optional[float] = None,
    decision_now_ms: Optional[int] = None,
    broker_order_id: Optional[str] = None,
    order_accepted: bool = False,
    broker_position_id: Optional[str] = None,
    position_opened: bool = False,
    broker_risk_representation: Optional[
        StrategyBBrokerRiskRepresentation
    ] = None,
    terminal_order_evidence: Optional[
        StrategyBOrderTerminalEvidence
    ] = None,
    timeout_elapsed: bool = False,
    restart_reconstruction: bool = False,
    aggregate_open_risk_usd: Optional[float] = None,
) -> StrategyBReservationDisposition:

    thesis_id = _required_text("thesis_id", thesis_id)
    selected_capture_id = _required_text(
        "selected_capture_id",
        selected_capture_id,
    )
    symbol = _required_text("symbol", symbol).upper()
    side = _side(side)
    _required_text("reservation_id", reservation_id)

    if reserved_risk_usd is not None:
        reserved_risk_usd = _positive_number(
            "reserved_risk_usd",
            reserved_risk_usd,
        )

    if decision_now_ms is not None:
        decision_now_ms = _positive_ms(
            "decision_now_ms",
            decision_now_ms,
        )

    if not isinstance(order_accepted, bool):
        raise TypeError("order_accepted must be bool")
    if not isinstance(position_opened, bool):
        raise TypeError("position_opened must be bool")
    if not isinstance(timeout_elapsed, bool):
        raise TypeError("timeout_elapsed must be bool")
    if not isinstance(restart_reconstruction, bool):
        raise TypeError("restart_reconstruction must be bool")

    if broker_order_id is not None:
        broker_order_id = _required_text(
            "broker_order_id",
            broker_order_id,
        )

    if broker_position_id is not None:
        broker_position_id = _required_text(
            "broker_position_id",
            broker_position_id,
        )

    # Aggregate account risk is deliberately not sufficient to transfer
    # ownership. Validate it if supplied, but never use it as proof.
    if aggregate_open_risk_usd is not None:
        _positive_number(
            "aggregate_open_risk_usd",
            aggregate_open_risk_usd,
        )

    # ------------------------------------------------------------
    # Authoritative terminal-before-fill evidence.
    # ------------------------------------------------------------
    if terminal_order_evidence is not None:
        t = terminal_order_evidence

        if broker_order_id is None:
            return _result(
                "KEEP",
                "TERMINAL_ORDER_ID_NOT_PROVIDED",
            )

        if (
            t.thesis_id != thesis_id
            or t.selected_capture_id != selected_capture_id
            or t.symbol != symbol
            or t.side != side
            or t.broker_order_id != broker_order_id
        ):
            return _result(
                "KEEP",
                "TERMINAL_ORDER_IDENTITY_MISMATCH",
            )

        if (
            decision_now_ms is not None
            and t.terminal_at_ms > decision_now_ms
        ):
            return _result(
                "KEEP",
                "TERMINAL_ORDER_EVIDENCE_FUTURE",
            )

        if t.position_opened or position_opened:
            return _result(
                "KEEP",
                "TERMINAL_ORDER_AFTER_FILL_NOT_RELEASEABLE",
            )

        return _result(
            "RELEASE",
            f"ORDER_{t.terminal_status}_BEFORE_FILL",
            release_authorized=True,
        )

    # ------------------------------------------------------------
    # Position exists: transfer only with exact fresh broker-risk proof.
    # ------------------------------------------------------------
    if position_opened:
        if not order_accepted:
            return _result(
                "KEEP",
                "POSITION_OPENED_WITHOUT_ACCEPTED_ORDER",
            )

        if broker_position_id is None:
            return _result(
                "KEEP",
                "BROKER_POSITION_ID_NOT_PROVIDED",
            )

        if broker_order_id is None:
            return _result(
                "KEEP",
                "ORDER_ACCEPTED_WITHOUT_BROKER_ORDER_ID",
            )

        if broker_risk_representation is None:
            return _result(
                "KEEP",
                "BROKER_RISK_TRANSFER_NOT_PROVEN",
            )

        if reserved_risk_usd is None:
            return _result(
                "KEEP",
                "RESERVED_RISK_NOT_PROVIDED",
            )

        proof = broker_risk_representation

        if (
            proof.broker_position_id != broker_position_id
            or proof.symbol != symbol
            or proof.side != side
        ):
            return _result(
                "KEEP",
                "BROKER_RISK_IDENTITY_MISMATCH",
            )

        age_ms = proof.now_ms - proof.snapshot_at_ms

        if age_ms < 0:
            return _result(
                "KEEP",
                "BROKER_RISK_SNAPSHOT_FUTURE",
            )

        if age_ms > proof.snapshot_max_age_ms:
            return _result(
                "KEEP",
                "BROKER_RISK_SNAPSHOT_STALE",
            )

        if proof.represented_risk_usd < reserved_risk_usd:
            return _result(
                "KEEP",
                "BROKER_RISK_BELOW_RESERVED_RISK",
            )

        return _result(
            "TRANSFER_READY",
            "BROKER_LIVE_RISK_PROVEN",
        )

    # ------------------------------------------------------------
    # Accepted order remains pending exposure.
    # ------------------------------------------------------------
    if order_accepted:
        if broker_order_id is None:
            return _result(
                "KEEP",
                "ORDER_ACCEPTED_WITHOUT_BROKER_ORDER_ID",
            )

        return _result(
            "KEEP",
            "ORDER_ACCEPTED_POSITION_NOT_PROVEN",
        )

    # Timeout never manufactures terminal broker evidence.
    if timeout_elapsed:
        return _result(
            "KEEP",
            "TIMEOUT_WITHOUT_AUTHORITATIVE_TERMINAL_EVIDENCE",
        )

    # Restart reconstruction also never manufactures evidence.
    if restart_reconstruction:
        return _result(
            "KEEP",
            "EXTERNAL_EVIDENCE_NOT_AVAILABLE",
        )

    return _result(
        "KEEP",
        "EXTERNAL_EVIDENCE_NOT_AVAILABLE",
    )


__all__ = [
    "StrategyBOrderTerminalEvidence",
    "StrategyBBrokerRiskRepresentation",
    "StrategyBReservationDisposition",
    "decide_strategy_b_reservation_disposition",
]
