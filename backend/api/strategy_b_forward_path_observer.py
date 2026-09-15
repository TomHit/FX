"""
XauTrendLab Strategy B - Forward Path Observer
==============================================

Pure observation-only forward market-path measurement for Strategy-B
calibration.

Purpose
-------
Measure what price did AFTER an approved causal H1 break without inventing:

- WIN / LOSS labels;
- TP / SL outcomes;
- R-multiples;
- thesis invalidation policy;
- production buffer policy;
- broker/order/risk authority.

The observer consumes already-loaded native H1 bars. It performs no Redis reads,
no Redis SCAN, no broker/HTTP fetch, and no Strategy-A mutation.

Causality
---------
Only H1 bars whose CLOSE time satisfies:

    bar_close_ms > h1_break_ms
    and
    bar_close_ms <= as_of_ms

may contribute to forward-path measurements.

A raw structural-pivot breach is recorded as evidence only. It does NOT
terminate the observation because the executable Strategy-B stop ultimately
requires an explicit calibrated structural buffer.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping, Optional, Sequence


_H1_MS = 3_600_000


def _upper(v: Any) -> str:
    return str(v or "").strip().upper()


def _num(v: Any) -> Optional[float]:
    try:
        x = float(v)
        return x if x == x else None
    except Exception:
        return None


def _ms(v: Any) -> int:
    """
    Normalize epoch seconds / milliseconds / microseconds / nanoseconds
    to milliseconds.
    """
    try:
        n = int(float(v or 0))
    except Exception:
        return 0

    if n <= 0:
        return 0

    if n < 10_000_000_000:          # seconds
        return n * 1000

    if n < 10_000_000_000_000:      # milliseconds
        return n

    if n < 10_000_000_000_000_000:  # microseconds
        return n // 1000

    return n // 1_000_000            # nanoseconds+


def _bar_open_ms(bar: Mapping[str, Any]) -> int:
    return _ms(
        bar.get("t_open_ms")
        or bar.get("tOpenMs")
        or bar.get("open_time_ms")
        or bar.get("t")
        or bar.get("time")
    )


def _bar_close_meta(bar: Mapping[str, Any]) -> tuple[int, str]:
    explicit = _ms(
        bar.get("t_close_ms")
        or bar.get("tCloseMs")
        or bar.get("close_time_ms")
    )
    if explicit > 0:
        return explicit, "EXPLICIT_CLOSE_TIME"

    open_ms = _bar_open_ms(bar)
    if open_ms > 0:
        return open_ms + _H1_MS, "DERIVED_FROM_H1_OPEN_TIME"

    return 0, "CLOSE_TIME_MISSING"


@dataclass(frozen=True)
class StrategyBForwardPathObservation:
    schema_version: int
    model: str

    symbol: str
    side: str
    as_of_ms: int

    h1_break_ms: int
    reference_price: Optional[float]
    reference_atr: Optional[float]

    selected_invalidation_timeframe: Optional[str]
    selected_invalidation_price: Optional[float]
    raw_pivot_available: bool

    input_bar_count: int
    eligible_bar_count: int
    ignored_pre_break_or_equal_count: int
    ignored_future_count: int
    ignored_timestamp_missing_count: int
    derived_close_time_count: int

    first_observed_bar_close_ms: int
    last_observed_bar_close_ms: int

    max_favorable_excursion_price: Optional[float]
    max_adverse_excursion_price: Optional[float]
    max_favorable_excursion_atr: Optional[float]
    max_adverse_excursion_atr: Optional[float]

    favorable_extreme_price: Optional[float]
    adverse_extreme_price: Optional[float]

    raw_pivot_touch_count: int
    raw_pivot_wick_breach_count: int
    raw_pivot_close_beyond_count: int

    raw_pivot_touched: bool
    raw_pivot_wick_breached: bool
    raw_pivot_close_beyond: bool

    first_raw_pivot_touch_ms: int
    first_raw_pivot_wick_breach_ms: int
    first_raw_pivot_close_beyond_ms: int

    max_raw_pivot_penetration_price: Optional[float]
    max_raw_pivot_penetration_atr: Optional[float]

    recovered_inside_after_wick_breach: bool
    recovered_inside_after_close_beyond: bool

    observation_state: str
    reason: str
    reasons: tuple[str, ...]

    observation_only: bool = True
    execution_authority: bool = False
    risk_authority: bool = False
    strategy_a_mutation: bool = False
    redis_scan_used: bool = False
    broker_http_used: bool = False
    future_data_used: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def observe_strategy_b_forward_path(
    *,
    symbol: str,
    side: str,
    as_of_ms: Any,
    h1_break_ms: Any,
    reference_price: Any,
    reference_atr: Any,
    selected_invalidation_price: Any,
    selected_invalidation_timeframe: Any = None,
    native_h1_bars: Sequence[Mapping[str, Any]] | None,
) -> StrategyBForwardPathObservation:
    sym = _upper(symbol)
    side_u = _upper(side)

    now_ms = _ms(as_of_ms)
    break_ms = _ms(h1_break_ms)

    ref = _num(reference_price)
    atr = _num(reference_atr)

    pivot = _num(selected_invalidation_price)
    pivot_tf = _upper(selected_invalidation_timeframe) or None

    reasons: list[str] = []

    if not sym:
        reasons.append("SYMBOL_MISSING")

    if side_u not in ("BUY", "SELL"):
        reasons.append("SIDE_NOT_ACTIONABLE")

    if now_ms <= 0:
        reasons.append("AS_OF_TIME_MISSING")

    if break_ms <= 0:
        reasons.append("H1_BREAK_TIME_MISSING")

    if ref is None or ref <= 0:
        reasons.append("REFERENCE_PRICE_INVALID")

    if atr is None or atr <= 0:
        reasons.append("REFERENCE_ATR_INVALID")

    raw_pivot_available = bool(
        pivot is not None and pivot > 0
    )

    if not raw_pivot_available:
        reasons.append("RAW_PIVOT_UNAVAILABLE")

    bars = [
        b
        for b in (native_h1_bars or ())
        if isinstance(b, Mapping)
    ]

    if not bars:
        reasons.append("NATIVE_H1_BARS_MISSING")

    eligible: list[tuple[Mapping[str, Any], int]] = []

    ignored_pre = 0
    ignored_future = 0
    ignored_missing_ts = 0
    derived_close_count = 0

    for bar in bars:
        close_ms, close_source = _bar_close_meta(bar)

        if close_ms <= 0:
            ignored_missing_ts += 1
            continue

        if close_source == "DERIVED_FROM_H1_OPEN_TIME":
            derived_close_count += 1

        if break_ms > 0 and close_ms <= break_ms:
            ignored_pre += 1
            continue

        if now_ms > 0 and close_ms > now_ms:
            ignored_future += 1
            continue

        eligible.append((bar, close_ms))

    eligible.sort(key=lambda x: x[1])

    mfe = None
    mae = None

    favorable_extreme = None
    adverse_extreme = None

    touch_count = 0
    wick_breach_count = 0
    close_beyond_count = 0

    first_touch_ms = 0
    first_wick_breach_ms = 0
    first_close_beyond_ms = 0

    max_penetration = None

    recovered_after_wick = False
    recovered_after_close = False

    wick_breach_seen = False
    close_beyond_seen = False

    for bar, close_ms in eligible:
        hi = _num(
            bar.get("h")
            if bar.get("h") is not None
            else bar.get("high")
        )
        lo = _num(
            bar.get("l")
            if bar.get("l") is not None
            else bar.get("low")
        )
        close = _num(
            bar.get("c")
            if bar.get("c") is not None
            else bar.get("close")
        )

        if hi is None or lo is None:
            continue

        if side_u == "BUY" and ref is not None:
            favorable = max(0.0, hi - ref)
            adverse = max(0.0, ref - lo)

            favorable_extreme = (
                hi
                if favorable_extreme is None
                else max(favorable_extreme, hi)
            )
            adverse_extreme = (
                lo
                if adverse_extreme is None
                else min(adverse_extreme, lo)
            )

            if pivot is not None:
                touched = lo <= pivot
                breached = lo < pivot
                close_beyond = close is not None and close < pivot
                penetration = max(0.0, pivot - lo)

                if touched:
                    touch_count += 1
                    if first_touch_ms <= 0:
                        first_touch_ms = close_ms

                if breached:
                    wick_breach_count += 1
                    wick_breach_seen = True
                    if first_wick_breach_ms <= 0:
                        first_wick_breach_ms = close_ms

                if close_beyond:
                    close_beyond_count += 1
                    close_beyond_seen = True
                    if first_close_beyond_ms <= 0:
                        first_close_beyond_ms = close_ms

                if wick_breach_seen and close is not None and close > pivot:
                    recovered_after_wick = True

                if close_beyond_seen and close is not None and close > pivot:
                    recovered_after_close = True

                max_penetration = (
                    penetration
                    if max_penetration is None
                    else max(max_penetration, penetration)
                )

        elif side_u == "SELL" and ref is not None:
            favorable = max(0.0, ref - lo)
            adverse = max(0.0, hi - ref)

            favorable_extreme = (
                lo
                if favorable_extreme is None
                else min(favorable_extreme, lo)
            )
            adverse_extreme = (
                hi
                if adverse_extreme is None
                else max(adverse_extreme, hi)
            )

            if pivot is not None:
                touched = hi >= pivot
                breached = hi > pivot
                close_beyond = close is not None and close > pivot
                penetration = max(0.0, hi - pivot)

                if touched:
                    touch_count += 1
                    if first_touch_ms <= 0:
                        first_touch_ms = close_ms

                if breached:
                    wick_breach_count += 1
                    wick_breach_seen = True
                    if first_wick_breach_ms <= 0:
                        first_wick_breach_ms = close_ms

                if close_beyond:
                    close_beyond_count += 1
                    close_beyond_seen = True
                    if first_close_beyond_ms <= 0:
                        first_close_beyond_ms = close_ms

                if wick_breach_seen and close is not None and close < pivot:
                    recovered_after_wick = True

                if close_beyond_seen and close is not None and close < pivot:
                    recovered_after_close = True

                max_penetration = (
                    penetration
                    if max_penetration is None
                    else max(max_penetration, penetration)
                )

        else:
            continue

        mfe = favorable if mfe is None else max(mfe, favorable)
        mae = adverse if mae is None else max(mae, adverse)

    first_bar_ms = eligible[0][1] if eligible else 0
    last_bar_ms = eligible[-1][1] if eligible else 0

    mfe_atr = (
        mfe / atr
        if mfe is not None and atr is not None and atr > 0
        else None
    )

    mae_atr = (
        mae / atr
        if mae is not None and atr is not None and atr > 0
        else None
    )

    penetration_atr = (
        max_penetration / atr
        if max_penetration is not None
        and atr is not None
        and atr > 0
        else None
    )

    fatal_input_reasons = {
        "SYMBOL_MISSING",
        "SIDE_NOT_ACTIONABLE",
        "AS_OF_TIME_MISSING",
        "H1_BREAK_TIME_MISSING",
        "REFERENCE_PRICE_INVALID",
        "REFERENCE_ATR_INVALID",
        "NATIVE_H1_BARS_MISSING",
    }

    if any(x in fatal_input_reasons for x in reasons):
        state = "INPUT_INCOMPLETE"
    elif not eligible:
        state = "NO_FUTURE_COMPLETED_BARS"
        reasons.append("NO_H1_BAR_CLOSED_AFTER_CAUSAL_BREAK")
    else:
        state = "OBSERVING"
        if raw_pivot_available:
            reasons.append("FORWARD_PATH_OBSERVED")
        else:
            reasons.append(
                "FORWARD_PATH_OBSERVED_RAW_PIVOT_UNAVAILABLE"
            )

    return StrategyBForwardPathObservation(
        schema_version=1,
        model="STRATEGY_B_FORWARD_PATH_OBSERVER_V1",

        symbol=sym,
        side=side_u,
        as_of_ms=now_ms,

        h1_break_ms=break_ms,
        reference_price=ref,
        reference_atr=atr,

        selected_invalidation_timeframe=pivot_tf,
        selected_invalidation_price=pivot,
        raw_pivot_available=raw_pivot_available,

        input_bar_count=len(bars),
        eligible_bar_count=len(eligible),
        ignored_pre_break_or_equal_count=ignored_pre,
        ignored_future_count=ignored_future,
        ignored_timestamp_missing_count=ignored_missing_ts,
        derived_close_time_count=derived_close_count,

        first_observed_bar_close_ms=first_bar_ms,
        last_observed_bar_close_ms=last_bar_ms,

        max_favorable_excursion_price=mfe,
        max_adverse_excursion_price=mae,
        max_favorable_excursion_atr=mfe_atr,
        max_adverse_excursion_atr=mae_atr,

        favorable_extreme_price=favorable_extreme,
        adverse_extreme_price=adverse_extreme,

        raw_pivot_touch_count=touch_count,
        raw_pivot_wick_breach_count=wick_breach_count,
        raw_pivot_close_beyond_count=close_beyond_count,

        raw_pivot_touched=touch_count > 0,
        raw_pivot_wick_breached=wick_breach_count > 0,
        raw_pivot_close_beyond=close_beyond_count > 0,

        first_raw_pivot_touch_ms=first_touch_ms,
        first_raw_pivot_wick_breach_ms=first_wick_breach_ms,
        first_raw_pivot_close_beyond_ms=first_close_beyond_ms,

        max_raw_pivot_penetration_price=max_penetration,
        max_raw_pivot_penetration_atr=penetration_atr,

        recovered_inside_after_wick_breach=recovered_after_wick,
        recovered_inside_after_close_beyond=recovered_after_close,

        observation_state=state,
        reason=reasons[0] if reasons else state,
        reasons=tuple(dict.fromkeys(reasons)),

        observation_only=True,
        execution_authority=False,
        risk_authority=False,
        strategy_a_mutation=False,
        redis_scan_used=False,
        broker_http_used=False,
        future_data_used=False,
    )


__all__ = [
    "StrategyBForwardPathObservation",
    "observe_strategy_b_forward_path",
]
