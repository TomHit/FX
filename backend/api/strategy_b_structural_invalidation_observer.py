"""
XauTrendLab Strategy B - Structural Invalidation Observer V2
============================================================

Adds Strategy-B-only confirmed pivot timestamps/indices to existing structural
invalidation candidates without changing bias_engine.py or Strategy A.

The pivot timestamps are derived from the same canonical left/right swing
definition used by bias_engine.detect_swing_structure(). This module remains
observation-only and does not select the final trade stop, add a buffer, size
risk, place orders, write Redis, or mutate Strategy A.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping, Optional, Sequence

from api.bias_engine import DEFAULT_CONFIG as BIAS_DEFAULT_CONFIG


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


def _int(v: Any) -> int:
    try:
        return int(v or 0)
    except Exception:
        return 0


def _positive(v: Any) -> Optional[float]:
    x = _num(v)
    if x is None or x <= 0:
        return None
    return x


def _pivot_highs(
    bars: Sequence[Mapping[str, Any]],
    left: int,
    right: int,
) -> list[int]:
    out: list[int] = []
    for i in range(left, len(bars) - right):
        v = float(bars[i]["h"])
        if all(v > float(bars[j]["h"]) for j in range(i - left, i)) and all(
            v >= float(bars[j]["h"]) for j in range(i + 1, i + right + 1)
        ):
            out.append(i)
    return out


def _pivot_lows(
    bars: Sequence[Mapping[str, Any]],
    left: int,
    right: int,
) -> list[int]:
    out: list[int] = []
    for i in range(left, len(bars) - right):
        v = float(bars[i]["l"])
        if all(v < float(bars[j]["l"]) for j in range(i - left, i)) and all(
            v <= float(bars[j]["l"]) for j in range(i + 1, i + right + 1)
        ):
            out.append(i)
    return out


def _latest_confirmed_pivot_meta(
    *,
    bars: Sequence[Mapping[str, Any]],
    side: str,
    left: int,
    right: int,
) -> dict[str, Any]:
    """
    Return metadata for the same latest confirmed pivot used by the structural
    candidate semantics:
      BUY  -> latest confirmed swing low
      SELL -> latest confirmed swing high
    """
    if not bars:
        return {}

    idxs = (
        _pivot_lows(bars, left, right)
        if side == "BUY"
        else _pivot_highs(bars, left, right)
        if side == "SELL"
        else []
    )
    if not idxs:
        return {}

    i = int(idxs[-1])
    b = bars[i]
    return {
        "index": i,
        "t": _int(
            b.get("t")
            or b.get("t_open_ms")
            or b.get("open_time_ms")
            or b.get("time")
        ),
        "price": (
            _num(b.get("l")) if side == "BUY"
            else _num(b.get("h")) if side == "SELL"
            else None
        ),
        "bars_since_pivot": max(0, len(bars) - 1 - i),
    }


@dataclass(frozen=True)
class StrategyBInvalidationCandidate:
    timeframe: str
    source: str
    kind: str
    price: Optional[float]
    atr: Optional[float]
    correct_side_of_entry: bool
    distance_price: Optional[float]
    distance_atr: Optional[float]
    structure_state: str
    high_pattern: Optional[str]
    low_pattern: Optional[str]
    data_ok: bool
    fresh: Optional[bool]

    # V2 causal/pivot metadata.
    pivot_index: Optional[int]
    pivot_ms: int
    pivot_age_bars: Optional[int]
    pivot_price_matches_structure: Optional[bool]
    pivot_metadata_ok: bool

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class StrategyBStructuralInvalidationObservation:
    schema_version: int
    model: str

    symbol: str
    side: str
    entry_price: Optional[float]

    data_ok: bool
    reason: str
    reasons: tuple[str, ...]

    candidates: tuple[StrategyBInvalidationCandidate, ...]
    correct_side_candidates: tuple[str, ...]
    invalid_side_candidates: tuple[str, ...]

    candidate_count: int
    correct_side_count: int

    structural_invalidation_observed: bool

    selected_timeframe: Optional[str]
    selected_source: Optional[str]
    structural_invalidation_price: Optional[float]
    structural_invalidation_valid: bool

    observation_only: bool = True
    execution_authority: bool = False
    risk_authority: bool = False
    strategy_a_mutation: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _candidate_from_tf(
    *,
    tf: str,
    side: str,
    entry: float,
    tf_row: Mapping[str, Any],
    bars: Sequence[Mapping[str, Any]],
    left: int,
    right: int,
) -> StrategyBInvalidationCandidate:
    structure = _mapping(tf_row.get("structure"))
    features = _mapping(tf_row.get("features"))

    if side == "BUY":
        kind = f"{tf}_LAST_SWING_LOW"
        price = _positive(structure.get("last_low"))
        correct = bool(price is not None and price < entry)
    elif side == "SELL":
        kind = f"{tf}_LAST_SWING_HIGH"
        price = _positive(structure.get("last_high"))
        correct = bool(price is not None and price > entry)
    else:
        kind = f"{tf}_UNKNOWN"
        price = None
        correct = False

    atr = _positive(features.get("atr"))
    distance_price = abs(entry - price) if correct and price is not None else None
    distance_atr = (
        distance_price / atr
        if distance_price is not None and atr is not None and atr > 0
        else None
    )

    pivot = _latest_confirmed_pivot_meta(
        bars=bars,
        side=side,
        left=left,
        right=right,
    )
    pivot_price = _num(pivot.get("price"))
    match = None
    if price is not None and pivot_price is not None:
        # Exact bar highs/lows should match the structure price. Small tolerance
        # only protects float representation.
        tol = max(abs(price), 1.0) * 1e-10
        match = abs(float(price) - float(pivot_price)) <= tol

    return StrategyBInvalidationCandidate(
        timeframe=tf,
        source=f"PAIR_BIAS_{tf}_CONFIRMED_SWING",
        kind=kind,
        price=price,
        atr=atr,
        correct_side_of_entry=correct,
        distance_price=distance_price,
        distance_atr=distance_atr,
        structure_state=_upper(structure.get("state")) or "UNKNOWN",
        high_pattern=(
            _upper(structure.get("high_pattern"))
            if structure.get("high_pattern") is not None
            else None
        ),
        low_pattern=(
            _upper(structure.get("low_pattern"))
            if structure.get("low_pattern") is not None
            else None
        ),
        data_ok=bool(price is not None and atr is not None),
        fresh=(
            bool(tf_row.get("fresh"))
            if isinstance(tf_row.get("fresh"), bool)
            else None
        ),
        pivot_index=(
            int(pivot["index"]) if isinstance(pivot.get("index"), int) else None
        ),
        pivot_ms=_int(pivot.get("t")),
        pivot_age_bars=(
            int(pivot["bars_since_pivot"])
            if isinstance(pivot.get("bars_since_pivot"), int)
            else None
        ),
        pivot_price_matches_structure=match,
        pivot_metadata_ok=bool(
            pivot
            and _int(pivot.get("t")) > 0
            and match is True
        ),
    )


def observe_strategy_b_structural_invalidation(
    *,
    symbol: str,
    side: str,
    # Preferred V2 contract:
    symbol_snapshot: Mapping[str, Any] | None = None,
    # Backward-compatible V1 contract:
    entry_price: Any = None,
    pair_bias: Mapping[str, Any] | None = None,
    # Optional explicit native bars supplied by the V6 live observer.
    native_bars: Mapping[str, Any] | None = None,
) -> StrategyBStructuralInvalidationObservation:
    sym = _upper(symbol)
    side_u = _upper(side)
    snap = _mapping(symbol_snapshot)

    entry = _positive(
        snap.get("price") if snap else entry_price
    )
    reasons: list[str] = []

    if side_u not in ("BUY", "SELL"):
        reasons.append("SIDE_NOT_ACTIONABLE")
    if entry is None:
        reasons.append("ENTRY_PRICE_INVALID")

    # Prefer the serialized Strategy-B snapshot when present, otherwise preserve
    # the historical V1 pair_bias call contract.
    tfs = _mapping(
        snap.get("timeframes")
        if snap.get("timeframes") is not None
        else _mapping(pair_bias).get("timeframes")
    )
    bars_map = _mapping(
        native_bars
        if native_bars is not None
        else snap.get("native_bars")
    )

    left = int(BIAS_DEFAULT_CONFIG.pivot_left)
    right = int(BIAS_DEFAULT_CONFIG.pivot_right)

    rows: list[StrategyBInvalidationCandidate] = []

    if entry is not None and side_u in ("BUY", "SELL"):
        for tf in ("D1", "H4", "H1"):
            tf_row = _mapping(tfs.get(tf))
            bars = bars_map.get(tf)
            if not isinstance(bars, (list, tuple)):
                bars = ()
            rows.append(
                _candidate_from_tf(
                    tf=tf,
                    side=side_u,
                    entry=float(entry),
                    tf_row=tf_row,
                    bars=bars,
                    left=left,
                    right=right,
                )
            )

    correct = tuple(
        x.timeframe
        for x in rows
        if x.correct_side_of_entry and x.data_ok
    )
    invalid = tuple(
        x.timeframe
        for x in rows
        if not (x.correct_side_of_entry and x.data_ok)
    )

    if not rows:
        reasons.append("INVALIDATION_INPUT_INCOMPLETE")
    elif not correct:
        reasons.append("NO_VALID_STRUCTURAL_INVALIDATION_CANDIDATE")
    else:
        reasons.append("INVALIDATION_CANDIDATES_OBSERVED_AWAIT_POLICY")

    for row in rows:
        if row.data_ok and not row.pivot_metadata_ok:
            reasons.append(f"{row.timeframe}_PIVOT_METADATA_NOT_CONFIRMED")

    data_ok = bool(
        rows
        and side_u in ("BUY", "SELL")
        and entry is not None
        and all(
            (not x.data_ok) or x.pivot_metadata_ok
            for x in rows
        )
    )

    return StrategyBStructuralInvalidationObservation(
        schema_version=2,
        model="STRATEGY_B_STRUCTURAL_INVALIDATION_OBSERVER_V2",
        symbol=sym,
        side=side_u,
        entry_price=entry,
        data_ok=data_ok,
        reason=reasons[0] if reasons else "UNKNOWN",
        reasons=tuple(dict.fromkeys(reasons)),
        candidates=tuple(rows),
        correct_side_candidates=correct,
        invalid_side_candidates=invalid,
        candidate_count=len(rows),
        correct_side_count=len(correct),
        structural_invalidation_observed=bool(correct),
        selected_timeframe=None,
        selected_source=None,
        structural_invalidation_price=None,
        structural_invalidation_valid=False,
    )


__all__ = [
    "StrategyBInvalidationCandidate",
    "StrategyBStructuralInvalidationObservation",
    "observe_strategy_b_structural_invalidation",
]
