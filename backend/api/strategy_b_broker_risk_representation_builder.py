"""
Strategy-B broker exact-position risk representation builder.

Pure / observation-only.

This module does not calculate broker risk, read Redis, call MT5,
enqueue orders, mutate Strategy-A, or provide execution authority.

A broker position can become a StrategyBBrokerRiskRepresentation only
when an explicit exact-position risk evidence object is correlated to
that same broker position.

The evidence source label is a contract/provenance label. This module
does not itself establish how an external adapter obtains the exact
broker-position risk.
"""

from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Optional


_ALLOWED_RISK_EVIDENCE_SOURCE = "BROKER_EXACT_POSITION_RISK"


@dataclass(frozen=True)
class StrategyBBrokerPositionSnapshot:
    broker_position_id: str
    symbol: str
    side: str
    entry_price: float
    stop_loss: float
    lots: float
    snapshot_at_ms: int


@dataclass(frozen=True)
class StrategyBBrokerPositionRiskEvidence:
    broker_position_id: str
    symbol: str
    side: str
    entry_price: float
    stop_loss: float
    lots: float
    represented_risk_usd: float
    evidence_at_ms: int
    source: str


@dataclass(frozen=True)
class StrategyBBrokerRiskRepresentation:
    broker_position_id: str
    symbol: str
    side: str
    represented_risk_usd: float
    snapshot_at_ms: int
    now_ms: int
    snapshot_max_age_ms: int


@dataclass(frozen=True)
class StrategyBBrokerRiskBuildResult:
    proven: bool
    reason: str
    representation: Optional[StrategyBBrokerRiskRepresentation]

    observation_only: bool = True
    execution_authority: bool = False
    order_authority: bool = False
    broker_authority: bool = False
    redis_authority: bool = False
    strategy_a_mutation: bool = False


def _fail(reason: str) -> StrategyBBrokerRiskBuildResult:
    return StrategyBBrokerRiskBuildResult(
        proven=False,
        reason=reason,
        representation=None,
    )


def _clean_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _clean_symbol(value: object) -> str:
    return _clean_text(value).upper()


def _clean_side(value: object) -> str:
    return _clean_text(value).upper()


def _positive_finite(value: object) -> Optional[float]:
    if isinstance(value, bool):
        return None

    try:
        number = float(value)
    except (TypeError, ValueError):
        return None

    if not math.isfinite(number) or number <= 0.0:
        return None

    return number


def _strict_nonnegative_int(value: object) -> Optional[int]:
    if isinstance(value, bool):
        return None

    if not isinstance(value, int):
        return None

    if value < 0:
        return None

    return value


def _valid_age(
    *,
    observed_at_ms: object,
    now_ms: object,
    max_age_ms: object,
    future_reason: str,
    stale_reason: str,
    invalid_reason: str,
) -> Optional[str]:
    observed = _strict_nonnegative_int(observed_at_ms)
    now = _strict_nonnegative_int(now_ms)
    max_age = _strict_nonnegative_int(max_age_ms)

    if observed is None or now is None or max_age is None:
        return invalid_reason

    if observed > now:
        return future_reason

    if now - observed > max_age:
        return stale_reason

    return None


def build_strategy_b_broker_risk_representation(
    *,
    position_snapshot: StrategyBBrokerPositionSnapshot,
    risk_evidence: StrategyBBrokerPositionRiskEvidence,
    expected_broker_position_id: str,
    expected_symbol: str,
    expected_side: str,
    now_ms: int,
    snapshot_max_age_ms: int,
    risk_evidence_max_age_ms: int,
) -> StrategyBBrokerRiskBuildResult:

    if not isinstance(
        position_snapshot,
        StrategyBBrokerPositionSnapshot,
    ):
        return _fail("BROKER_POSITION_SNAPSHOT_INVALID")

    if not isinstance(
        risk_evidence,
        StrategyBBrokerPositionRiskEvidence,
    ):
        return _fail("RISK_EVIDENCE_INVALID")

    expected_position_id = _clean_text(
        expected_broker_position_id
    )
    expected_symbol_u = _clean_symbol(expected_symbol)
    expected_side_u = _clean_side(expected_side)

    if not expected_position_id:
        return _fail("EXPECTED_POSITION_ID_INVALID")

    if not expected_symbol_u:
        return _fail("EXPECTED_SYMBOL_INVALID")

    if expected_side_u not in {"BUY", "SELL"}:
        return _fail("EXPECTED_SIDE_INVALID")

    position_id = _clean_text(
        position_snapshot.broker_position_id
    )
    symbol = _clean_symbol(position_snapshot.symbol)
    side = _clean_side(position_snapshot.side)

    if not position_id:
        return _fail("BROKER_POSITION_ID_INVALID")

    if not symbol:
        return _fail("BROKER_POSITION_SYMBOL_INVALID")

    if side not in {"BUY", "SELL"}:
        return _fail("BROKER_POSITION_SIDE_INVALID")

    if position_id != expected_position_id:
        return _fail("BROKER_POSITION_ID_MISMATCH")

    if symbol != expected_symbol_u:
        return _fail("BROKER_POSITION_SYMBOL_MISMATCH")

    if side != expected_side_u:
        return _fail("BROKER_POSITION_SIDE_MISMATCH")

    entry = _positive_finite(position_snapshot.entry_price)
    stop = _positive_finite(position_snapshot.stop_loss)
    lots = _positive_finite(position_snapshot.lots)

    if entry is None or stop is None or lots is None:
        return _fail("BROKER_POSITION_GEOMETRY_INVALID")

    # Structural stop geometry is mandatory.
    if side == "BUY":
        if not stop < entry:
            return _fail("BROKER_POSITION_GEOMETRY_INVALID")
    else:
        if not stop > entry:
            return _fail("BROKER_POSITION_GEOMETRY_INVALID")

    snapshot_age_error = _valid_age(
        observed_at_ms=position_snapshot.snapshot_at_ms,
        now_ms=now_ms,
        max_age_ms=snapshot_max_age_ms,
        future_reason="BROKER_POSITION_SNAPSHOT_FUTURE",
        stale_reason="BROKER_POSITION_SNAPSHOT_STALE",
        invalid_reason="BROKER_POSITION_SNAPSHOT_TIME_INVALID",
    )

    if snapshot_age_error is not None:
        return _fail(snapshot_age_error)

    evidence_position_id = _clean_text(
        risk_evidence.broker_position_id
    )
    evidence_symbol = _clean_symbol(risk_evidence.symbol)
    evidence_side = _clean_side(risk_evidence.side)

    if evidence_position_id != position_id:
        return _fail("RISK_EVIDENCE_POSITION_ID_MISMATCH")

    if evidence_symbol != symbol:
        return _fail("RISK_EVIDENCE_SYMBOL_MISMATCH")

    if evidence_side != side:
        return _fail("RISK_EVIDENCE_SIDE_MISMATCH")

    evidence_entry = _positive_finite(
        risk_evidence.entry_price
    )
    evidence_stop = _positive_finite(
        risk_evidence.stop_loss
    )
    evidence_lots = _positive_finite(
        risk_evidence.lots
    )

    if (
        evidence_entry is None
        or evidence_stop is None
        or evidence_lots is None
    ):
        return _fail("RISK_EVIDENCE_GEOMETRY_INVALID")

    if evidence_entry != entry:
        return _fail("RISK_EVIDENCE_ENTRY_MISMATCH")

    if evidence_stop != stop:
        return _fail("RISK_EVIDENCE_STOP_MISMATCH")

    if evidence_lots != lots:
        return _fail("RISK_EVIDENCE_LOTS_MISMATCH")

    source = _clean_text(risk_evidence.source)

    if source != _ALLOWED_RISK_EVIDENCE_SOURCE:
        return _fail("RISK_EVIDENCE_SOURCE_INVALID")

    represented_risk = _positive_finite(
        risk_evidence.represented_risk_usd
    )

    if represented_risk is None:
        return _fail("REPRESENTED_RISK_INVALID")

    evidence_age_error = _valid_age(
        observed_at_ms=risk_evidence.evidence_at_ms,
        now_ms=now_ms,
        max_age_ms=risk_evidence_max_age_ms,
        future_reason="RISK_EVIDENCE_FUTURE",
        stale_reason="RISK_EVIDENCE_STALE",
        invalid_reason="RISK_EVIDENCE_TIME_INVALID",
    )

    if evidence_age_error is not None:
        return _fail(evidence_age_error)

    return StrategyBBrokerRiskBuildResult(
        proven=True,
        reason="BROKER_POSITION_RISK_PROVEN",
        representation=StrategyBBrokerRiskRepresentation(
            broker_position_id=position_id,
            symbol=symbol,
            side=side,
            represented_risk_usd=represented_risk,
            snapshot_at_ms=position_snapshot.snapshot_at_ms,
            now_ms=now_ms,
            snapshot_max_age_ms=snapshot_max_age_ms,
        ),
    )
