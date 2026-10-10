"""
Strategy-B frozen D1 macro-thesis validity.

Purpose
-------
Pure, fail-closed evaluator for the lifetime of one Strategy-B macro thesis.

Authority
---------
D1 owns macro structural invalidation.

H4 remains qualification/context authority elsewhere.
H1 remains reversal-proof authority elsewhere.
M15 remains timing authority elsewhere.

This module:
- does not read/write Redis
- does not access a broker
- does not place/cancel orders
- does not mutate Strategy A
- does not apply time-based thesis expiry
- does not treat H1/M15 failure as macro invalidation
- does not treat a D1 wick through the origin as terminal

A frozen old BULLISH DXY leg is structurally invalidated only when a
completed D1 candle CLOSES below its frozen confirmed D1 swing-low origin.

A frozen old BEARISH DXY leg is structurally invalidated only when a
completed D1 candle CLOSES above its frozen confirmed D1 swing-high origin.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping, Optional, Sequence


DIRECTIONS = {"BULLISH", "BEARISH"}

STATE_ACTIVE = "ACTIVE"
STATE_INVALID = "INVALID"
STATE_WAIT = "WAIT"


def _txt(value: Any) -> str:
    return str(value or "").strip()


def _upper(value: Any) -> str:
    return _txt(value).upper()


def _num(value: Any) -> Optional[float]:
    try:
        if value is None or isinstance(value, bool):
            return None
        out = float(value)
        return out
    except (TypeError, ValueError):
        return None


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _bar_open_ms(bar: Mapping[str, Any]) -> int:
    value = (
        bar.get("t_open_ms")
        or bar.get("t")
        or bar.get("time")
        or 0
    )
    out = _int(value)

    # Native loaders normally expose t_open_ms and also normalize t to
    # seconds. Keep this pure evaluator tolerant of either representation.
    if 0 < out < 10_000_000_000:
        out *= 1000

    return out


def _bar_close_ms(bar: Mapping[str, Any]) -> int:
    """
    Return the completed D1 candle confirmation timestamp.

    Prefer an explicit close timestamp when it is valid. The canonical
    REAL_DXY loader guarantees t_open_ms but does not guarantee t_close_ms,
    so D1 close time otherwise derives from open + 24 hours.
    """
    open_ms = _bar_open_ms(bar)

    value = (
        bar.get("t_close_ms")
        or bar.get("tCloseMs")
        or bar.get("close_time_ms")
        or 0
    )
    close_ms = _int(value)

    if 0 < close_ms < 10_000_000_000:
        close_ms *= 1000

    if close_ms > open_ms > 0:
        return close_ms

    if open_ms > 0:
        return open_ms + 86_400_000

    return 0


def _bar_close(bar: Mapping[str, Any]) -> Optional[float]:
    value = bar.get("c")
    if value is None:
        value = bar.get("close")
    return _num(value)


@dataclass(frozen=True)
class MacroThesisValidity:
    state: str
    valid: bool
    execution_dead: bool
    reason: str

    old_direction: str
    origin_type: str
    origin_price: Optional[float]
    origin_time_ms: int
    origin_confirmed_time_ms: int
    frozen_at_ms: int

    checked_at_ms: int
    invalidated_at_ms: int = 0
    invalidating_d1_bar_ms: int = 0
    invalidating_d1_close: Optional[float] = None

    def to_dict(self) -> dict:
        return asdict(self)


def build_frozen_d1_contract(
    *,
    d1_macro_leg: Mapping[str, Any],
    old_direction: str,
    frozen_at_ms: int,
) -> MacroThesisValidity:
    """
    Build the immutable structural identity for one macro thesis.

    The caller persists this result. Later evaluations must use the frozen
    origin rather than replacing it with a newer D1 pivot.
    """
    leg = (
        d1_macro_leg
        if isinstance(d1_macro_leg, Mapping)
        else {}
    )

    direction = _upper(old_direction)
    leg_direction = _upper(leg.get("direction"))
    origin_type = _upper(leg.get("origin_type"))
    origin_price = _num(leg.get("origin_price"))
    origin_time_ms = _int(leg.get("origin_time_ms"))
    origin_confirmed_time_ms = _int(
        leg.get("origin_confirmed_time_ms")
    )
    frozen_ms = _int(frozen_at_ms)

    if direction not in DIRECTIONS:
        return MacroThesisValidity(
            state=STATE_WAIT,
            valid=False,
            execution_dead=False,
            reason="D1_THESIS_OLD_DIRECTION_UNKNOWN",
            old_direction=direction,
            origin_type=origin_type,
            origin_price=origin_price,
            origin_time_ms=origin_time_ms,
            origin_confirmed_time_ms=origin_confirmed_time_ms,
            frozen_at_ms=frozen_ms,
            checked_at_ms=frozen_ms,
        )

    if leg.get("data_ok") is not True:
        return MacroThesisValidity(
            state=STATE_WAIT,
            valid=False,
            execution_dead=False,
            reason="D1_THESIS_MACRO_LEG_UNAVAILABLE",
            old_direction=direction,
            origin_type=origin_type,
            origin_price=origin_price,
            origin_time_ms=origin_time_ms,
            origin_confirmed_time_ms=origin_confirmed_time_ms,
            frozen_at_ms=frozen_ms,
            checked_at_ms=frozen_ms,
        )

    if leg_direction != direction:
        return MacroThesisValidity(
            state=STATE_WAIT,
            valid=False,
            execution_dead=False,
            reason="D1_THESIS_DIRECTION_MISMATCH",
            old_direction=direction,
            origin_type=origin_type,
            origin_price=origin_price,
            origin_time_ms=origin_time_ms,
            origin_confirmed_time_ms=origin_confirmed_time_ms,
            frozen_at_ms=frozen_ms,
            checked_at_ms=frozen_ms,
        )

    expected_origin = (
        "LOW"
        if direction == "BULLISH"
        else "HIGH"
    )

    if origin_type != expected_origin:
        return MacroThesisValidity(
            state=STATE_WAIT,
            valid=False,
            execution_dead=False,
            reason="D1_THESIS_ORIGIN_TYPE_INVALID",
            old_direction=direction,
            origin_type=origin_type,
            origin_price=origin_price,
            origin_time_ms=origin_time_ms,
            origin_confirmed_time_ms=origin_confirmed_time_ms,
            frozen_at_ms=frozen_ms,
            checked_at_ms=frozen_ms,
        )

    if (
        origin_price is None
        or origin_price <= 0
        or origin_time_ms <= 0
        or origin_confirmed_time_ms <= 0
        or frozen_ms <= 0
    ):
        return MacroThesisValidity(
            state=STATE_WAIT,
            valid=False,
            execution_dead=False,
            reason="D1_THESIS_ORIGIN_INCOMPLETE",
            old_direction=direction,
            origin_type=origin_type,
            origin_price=origin_price,
            origin_time_ms=origin_time_ms,
            origin_confirmed_time_ms=origin_confirmed_time_ms,
            frozen_at_ms=frozen_ms,
            checked_at_ms=frozen_ms,
        )

    # The structural origin must already have been confirmed when the
    # catalyst established this macro thesis.  Never attach an old
    # catalyst to a newer D1 pivot discovered on a later reevaluation.
    if origin_confirmed_time_ms > frozen_ms:
        return MacroThesisValidity(
            state=STATE_WAIT,
            valid=False,
            execution_dead=False,
            reason="D1_THESIS_ORIGIN_NOT_CAUSAL_AT_CATALYST",
            old_direction=direction,
            origin_type=origin_type,
            origin_price=origin_price,
            origin_time_ms=origin_time_ms,
            origin_confirmed_time_ms=origin_confirmed_time_ms,
            frozen_at_ms=frozen_ms,
            checked_at_ms=frozen_ms,
        )

    return MacroThesisValidity(
        state=STATE_ACTIVE,
        valid=True,
        execution_dead=False,
        reason="D1_THESIS_FROZEN",
        old_direction=direction,
        origin_type=origin_type,
        origin_price=origin_price,
        origin_time_ms=origin_time_ms,
        origin_confirmed_time_ms=origin_confirmed_time_ms,
        frozen_at_ms=frozen_ms,
        checked_at_ms=frozen_ms,
    )


def evaluate_frozen_d1_contract(
    *,
    contract: Mapping[str, Any],
    d1_bars: Sequence[Mapping[str, Any]],
    as_of_ms: int,
) -> MacroThesisValidity:
    """
    Evaluate an already-frozen D1 contract.

    Only completed D1 closes are terminal. The canonical REAL_DXY loader
    already supplies completed bars, but this evaluator also rejects any row
    explicitly marked incomplete.

    Bars before the origin was confirmed cannot invalidate a thesis that did
    not yet causally own that confirmed origin.
    """
    row = contract if isinstance(contract, Mapping) else {}

    direction = _upper(row.get("old_direction"))
    origin_type = _upper(row.get("origin_type"))
    origin_price = _num(row.get("origin_price"))
    origin_time_ms = _int(row.get("origin_time_ms"))
    origin_confirmed_time_ms = _int(
        row.get("origin_confirmed_time_ms")
    )
    frozen_at_ms = _int(row.get("frozen_at_ms"))
    checked_at_ms = _int(as_of_ms)

    # Terminal latch is immutable.
    if bool(row.get("execution_dead")):
        return MacroThesisValidity(
            state=STATE_INVALID,
            valid=False,
            execution_dead=True,
            reason=(
                _txt(row.get("reason"))
                or "D1_THESIS_EXECUTION_DEAD"
            ),
            old_direction=direction,
            origin_type=origin_type,
            origin_price=origin_price,
            origin_time_ms=origin_time_ms,
            origin_confirmed_time_ms=origin_confirmed_time_ms,
            frozen_at_ms=frozen_at_ms,
            checked_at_ms=checked_at_ms,
            invalidated_at_ms=_int(
                row.get("invalidated_at_ms")
            ),
            invalidating_d1_bar_ms=_int(
                row.get("invalidating_d1_bar_ms")
            ),
            invalidating_d1_close=_num(
                row.get("invalidating_d1_close")
            ),
        )

    expected_origin = (
        "LOW"
        if direction == "BULLISH"
        else (
            "HIGH"
            if direction == "BEARISH"
            else ""
        )
    )

    if (
        direction not in DIRECTIONS
        or not expected_origin
        or origin_type != expected_origin
        or origin_price is None
        or origin_price <= 0
        or origin_time_ms <= 0
        or origin_confirmed_time_ms <= 0
        or frozen_at_ms <= 0
    ):
        return MacroThesisValidity(
            state=STATE_WAIT,
            valid=False,
            execution_dead=False,
            reason="D1_THESIS_FROZEN_CONTRACT_INVALID",
            old_direction=direction,
            origin_type=origin_type,
            origin_price=origin_price,
            origin_time_ms=origin_time_ms,
            origin_confirmed_time_ms=origin_confirmed_time_ms,
            frozen_at_ms=frozen_at_ms,
            checked_at_ms=checked_at_ms,
        )

    usable = []

    for bar in d1_bars or ():
        if not isinstance(bar, Mapping):
            continue

        if bar.get("complete") is False:
            continue

        bar_open_ms = _bar_open_ms(bar)
        bar_close_ms = _bar_close_ms(bar)
        close = _bar_close(bar)

        if (
            bar_open_ms <= 0
            or bar_close_ms <= bar_open_ms
            or close is None
            or close <= 0
        ):
            continue

        # Structural invalidation belongs to this thesis only after the
        # catalyst/freeze boundary.  Bars before the thesis existed cannot
        # retrospectively invalidate it.
        if bar_close_ms <= origin_confirmed_time_ms:
            continue

        if bar_close_ms <= frozen_at_ms:
            continue

        # Never evaluate a candle whose confirmation lies in the future.
        if checked_at_ms > 0 and bar_close_ms > checked_at_ms:
            continue

        usable.append((bar_close_ms, close))

    usable.sort(key=lambda item: item[0])

    if not usable:
        return MacroThesisValidity(
            state=STATE_WAIT,
            valid=False,
            execution_dead=False,
            reason="D1_THESIS_NO_CAUSAL_COMPLETED_BARS",
            old_direction=direction,
            origin_type=origin_type,
            origin_price=origin_price,
            origin_time_ms=origin_time_ms,
            origin_confirmed_time_ms=origin_confirmed_time_ms,
            frozen_at_ms=frozen_at_ms,
            checked_at_ms=checked_at_ms,
        )

    for bar_ms, close in usable:
        invalid = (
            close < origin_price
            if direction == "BULLISH"
            else close > origin_price
        )

        if invalid:
            return MacroThesisValidity(
                state=STATE_INVALID,
                valid=False,
                execution_dead=True,
                reason=(
                    "D1_BULLISH_ORIGIN_CLOSE_INVALIDATED"
                    if direction == "BULLISH"
                    else "D1_BEARISH_ORIGIN_CLOSE_INVALIDATED"
                ),
                old_direction=direction,
                origin_type=origin_type,
                origin_price=origin_price,
                origin_time_ms=origin_time_ms,
                origin_confirmed_time_ms=origin_confirmed_time_ms,
                frozen_at_ms=frozen_at_ms,
                checked_at_ms=checked_at_ms,
                invalidated_at_ms=bar_ms,
                invalidating_d1_bar_ms=bar_ms,
                invalidating_d1_close=close,
            )

    return MacroThesisValidity(
        state=STATE_ACTIVE,
        valid=True,
        execution_dead=False,
        reason="D1_THESIS_STRUCTURE_INTACT",
        old_direction=direction,
        origin_type=origin_type,
        origin_price=origin_price,
        origin_time_ms=origin_time_ms,
        origin_confirmed_time_ms=origin_confirmed_time_ms,
        frozen_at_ms=frozen_at_ms,
        checked_at_ms=checked_at_ms,
    )
