"""
XauTrendLab Strategy B - Structural Buffer Calibration Observer
===============================================================

Pure observation-only evidence collector for structural-stop buffer calibration.

It DOES NOT choose a production buffer and DOES NOT place/modify/close orders.

For the already-resolved structural owner (D1/H4/H1), this observer measures:
- entry-to-pivot distance;
- pivot ATR;
- bars observed after the confirmed pivot;
- maximum wick breach beyond the structural pivot after that pivot formed;
- breach size in price and native-TF ATR;
- first/last breach timestamps and breach count.

No threshold, percentile, pip amount, or ATR multiplier is invented here.
The output is intended for JSONL/replay calibration and later real-trade outcome
attachment.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping, Optional, Sequence


_TF_MS = {
    "D1": 86_400_000,
    "H4": 14_400_000,
    "H1": 3_600_000,
}


def _mapping(v: Any) -> Mapping[str, Any]:
    return v if isinstance(v, Mapping) else {}


def _upper(v: Any) -> str:
    return str(v or "").strip().upper()


def _num(v: Any) -> Optional[float]:
    try:
        x = float(v)
        return x if x == x else None
    except Exception:
        return None


def _ms(v: Any) -> int:
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


def _bar_close_ms(bar: Mapping[str, Any], tf: str) -> int:
    explicit = _ms(
        bar.get("t_close_ms")
        or bar.get("tCloseMs")
        or bar.get("close_time_ms")
    )
    if explicit > 0:
        return explicit
    o = _bar_open_ms(bar)
    return o + _TF_MS.get(tf, 0) if o > 0 else 0


def _candidate_by_tf(
    invalidation_observation: Mapping[str, Any],
    tf: str,
) -> Mapping[str, Any]:
    tf_u = _upper(tf)
    for row in invalidation_observation.get("candidates") or ():
        if isinstance(row, Mapping) and _upper(row.get("timeframe")) == tf_u:
            return row
    return {}


@dataclass(frozen=True)
class StrategyBStructuralBufferCalibrationObservation:
    schema_version: int
    model: str

    symbol: str
    side: str
    as_of_ms: int

    owner_resolution_decision: str
    owner_resolution_reason: str
    owner_timeframe: Optional[str]

    entry_price: Optional[float]
    pivot_price: Optional[float]
    pivot_ms: int
    pivot_atr: Optional[float]

    entry_to_pivot_distance_price: Optional[float]
    entry_to_pivot_distance_atr: Optional[float]

    native_bar_count: int
    bars_after_pivot_count: int

    post_pivot_extreme_price: Optional[float]
    max_pivot_breach_price: Optional[float]
    max_pivot_breach_atr: Optional[float]
    breach_count: int
    first_breach_ms: int
    last_breach_ms: int

    buffer_calibration_ready: bool
    proposed_structural_buffer_price: Optional[float]
    reason: str
    reasons: tuple[str, ...]

    observation_only: bool = True
    execution_authority: bool = False
    risk_authority: bool = False
    strategy_a_mutation: bool = False
    redis_scan_used: bool = False
    future_data_used: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def observe_strategy_b_structural_buffer_calibration(
    *,
    symbol: str,
    side: str,
    as_of_ms: Any,
    invalidation_observation: Mapping[str, Any] | None,
    owner_resolution: Mapping[str, Any] | None,
    native_bars: Mapping[str, Sequence[Mapping[str, Any]]] | None,
) -> StrategyBStructuralBufferCalibrationObservation:
    sym = _upper(symbol)
    side_u = _upper(side)
    inv = _mapping(invalidation_observation)
    owner = _mapping(owner_resolution)
    bars_map = _mapping(native_bars)

    now_ms = _ms(as_of_ms)
    decision = _upper(owner.get("decision")) or "UNKNOWN"
    owner_reason = str(owner.get("reason") or "UNKNOWN")
    owner_tf = _upper(owner.get("preferred_htf_owner")) or None

    reasons: list[str] = []

    entry = _num(inv.get("entry_price"))

    if side_u not in ("BUY", "SELL"):
        reasons.append("SIDE_NOT_ACTIONABLE")

    if decision != "HTF_OWNER_RESOLVED":
        reasons.append("STRUCTURAL_OWNER_NOT_RESOLVED")

    if owner_tf not in ("D1", "H4", "H1"):
        reasons.append("OWNER_TIMEFRAME_NOT_ACTIONABLE")

    candidate = (
        _candidate_by_tf(inv, owner_tf)
        if owner_tf in ("D1", "H4", "H1")
        else {}
    )

    pivot_price = _num(candidate.get("price"))
    pivot_ms = _ms(candidate.get("pivot_ms"))
    pivot_atr = _num(candidate.get("atr"))

    if entry is None:
        reasons.append("ENTRY_PRICE_MISSING")
    if pivot_price is None:
        reasons.append("PIVOT_PRICE_MISSING")
    if pivot_ms <= 0:
        reasons.append("PIVOT_TIME_MISSING")
    if pivot_atr is None or pivot_atr <= 0:
        reasons.append("PIVOT_ATR_MISSING")

    entry_dist = None
    entry_dist_atr = None
    if entry is not None and pivot_price is not None:
        if side_u == "BUY" and pivot_price < entry:
            entry_dist = entry - pivot_price
        elif side_u == "SELL" and pivot_price > entry:
            entry_dist = pivot_price - entry
        else:
            reasons.append("PIVOT_NOT_CORRECT_SIDE_OF_ENTRY")

    if entry_dist is not None and pivot_atr is not None and pivot_atr > 0:
        entry_dist_atr = entry_dist / pivot_atr

    raw_bars = bars_map.get(owner_tf) if owner_tf else None
    bars = [
        b for b in (raw_bars or ())
        if isinstance(b, Mapping)
    ]
    if not bars:
        reasons.append("OWNER_NATIVE_BARS_MISSING")

    # Use only bars that CLOSED after the pivot and by as_of_ms.
    # This prevents future leakage in replay/historical captures.
    post: list[Mapping[str, Any]] = []
    for bar in bars:
        close_ms = _bar_close_ms(bar, owner_tf or "")
        if close_ms <= 0:
            continue
        if pivot_ms > 0 and close_ms <= pivot_ms:
            continue
        if now_ms > 0 and close_ms > now_ms:
            continue
        post.append(bar)

    breach_count = 0
    first_breach_ms = 0
    last_breach_ms = 0
    max_breach = None
    extreme = None

    if pivot_price is not None and side_u in ("BUY", "SELL"):
        for bar in post:
            hi = _num(bar.get("h") if bar.get("h") is not None else bar.get("high"))
            lo = _num(bar.get("l") if bar.get("l") is not None else bar.get("low"))
            close_ms = _bar_close_ms(bar, owner_tf or "")

            if side_u == "BUY":
                if lo is None:
                    continue
                extreme = lo if extreme is None else min(extreme, lo)
                breach = max(0.0, pivot_price - lo)
            else:
                if hi is None:
                    continue
                extreme = hi if extreme is None else max(extreme, hi)
                breach = max(0.0, hi - pivot_price)

            if breach > 0:
                breach_count += 1
                if first_breach_ms <= 0:
                    first_breach_ms = close_ms
                last_breach_ms = close_ms
                max_breach = breach if max_breach is None else max(max_breach, breach)

    # Zero is observed evidence, not an inferred production recommendation.
    observed_breach = (
        float(max_breach)
        if max_breach is not None
        else 0.0
        if post and pivot_price is not None
        else None
    )
    observed_breach_atr = (
        observed_breach / pivot_atr
        if observed_breach is not None
        and pivot_atr is not None
        and pivot_atr > 0
        else None
    )

    # Deliberately fail closed: a single observation never authorizes a buffer.
    ready = False
    proposed = None
    if not reasons:
        reasons.append("BUFFER_CALIBRATION_SAMPLE_CAPTURED")
        reasons.append("PRODUCTION_BUFFER_POLICY_NOT_CALIBRATED")

    reason = (
        reasons[0]
        if reasons
        else "PRODUCTION_BUFFER_POLICY_NOT_CALIBRATED"
    )

    return StrategyBStructuralBufferCalibrationObservation(
        schema_version=1,
        model="STRATEGY_B_STRUCTURAL_BUFFER_CALIBRATION_OBSERVER_V1",
        symbol=sym,
        side=side_u,
        as_of_ms=now_ms,
        owner_resolution_decision=decision,
        owner_resolution_reason=owner_reason,
        owner_timeframe=owner_tf,
        entry_price=entry,
        pivot_price=pivot_price,
        pivot_ms=pivot_ms,
        pivot_atr=pivot_atr,
        entry_to_pivot_distance_price=entry_dist,
        entry_to_pivot_distance_atr=entry_dist_atr,
        native_bar_count=len(bars),
        bars_after_pivot_count=len(post),
        post_pivot_extreme_price=extreme,
        max_pivot_breach_price=observed_breach,
        max_pivot_breach_atr=observed_breach_atr,
        breach_count=breach_count,
        first_breach_ms=first_breach_ms,
        last_breach_ms=last_breach_ms,
        buffer_calibration_ready=ready,
        proposed_structural_buffer_price=proposed,
        reason=reason,
        reasons=tuple(dict.fromkeys(reasons)),
    )


__all__ = [
    "StrategyBStructuralBufferCalibrationObservation",
    "observe_strategy_b_structural_buffer_calibration",
]
