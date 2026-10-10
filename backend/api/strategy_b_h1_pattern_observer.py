"""
XauTrendLab Strategy B - H1 Pattern Observer
=============================================

Pure, observation-only DXY H1 structural pattern observer for Strategy-B.

Design rules:
- Reuse bias_engine bar normalization and confirmed swing structure.
- H1 owns structural reversal proof; M15 remains timing only.
- No broker/order/risk authority.
- Historical pre-break H1 invalidation is pattern analytics only and is
  never Strategy-B trade-stop ownership.
- No Strategy-A mutation.
- No Redis I/O.
- No guessed production thresholds.

The observer exposes deterministic structural facts immediately, but pattern
claims that require distance/strength/timing tolerances remain fail-closed until
their calibration fields are explicitly supplied.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping, Optional, Sequence

from api.bias_engine import (
    DEFAULT_CONFIG as BIAS_DEFAULT_CONFIG,
    detect_swing_structure,
    normalize_bars,
)


DIRECTIONS = ("BULLISH", "BEARISH")


def _upper(v: Any) -> str:
    return str(v or "").strip().upper()


def _mapping(v: Any) -> Mapping[str, Any]:
    return v if isinstance(v, Mapping) else {}


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


@dataclass(frozen=True)
class StrategyBH1PatternConfig:
    # Reuse canonical bias-engine swing-pivot definition.
    pivot_left: int = int(BIAS_DEFAULT_CONFIG.pivot_left)
    pivot_right: int = int(BIAS_DEFAULT_CONFIG.pivot_right)
    min_h1_bars: int = int(BIAS_DEFAULT_CONFIG.min_bars_h1)

    # Displacement magnitude remains an observation/analytics field.
    # Production does not require a guessed minimum ATR threshold.
    min_displacement_atr: Optional[float] = None

    # Legacy compatibility only. Structural retest does not use proximity
    # tolerance or an arbitrary post-break bar expiry.
    retest_tolerance_atr: Optional[float] = None
    max_retest_bars: Optional[int] = None

    # Optional quality veto only; structural sweep existence needs no maximum.
    max_sweep_excess_atr: Optional[float] = None

    # Reuse XTL's canonical equal-level structural tolerance.
    # The Strategy-B detector remains stricter: confirmed pivots + confirmed
    # intervening neckline + causal completed-H1 neckline break.
    double_top_bottom_tolerance_atr: Optional[float] = 0.15

    # Legacy optional numeric no-chase aid. Structural no-chase flags remain
    # authoritative independently of this value.
    max_move_without_retest_atr: Optional[float] = None


@dataclass(frozen=True)
class StrategyBH1PatternObservation:
    schema_version: int
    model: str
    expected_direction: str
    data_ok: bool
    reason: str
    reasons: tuple[str, ...]

    bars_used: int
    last_bar_ms: int
    atr: Optional[float]

    h1_choch_bos: bool
    break_direction: str
    break_ms: int
    break_bar_index: Optional[int]
    break_level: Optional[float]
    break_close: Optional[float]
    pre_break_structure_state: str

    displacement_atr: Optional[float]
    h1_displacement: bool

    retest_seen: bool
    retest_ms: int
    retest_distance_atr: Optional[float]
    h1_retest: bool

    retest_failure: bool
    double_top_bottom: bool
    h1_order_block_validated: bool
    h1_order_block_retest: bool
    liquidity_sweep_rejection: bool
    failed_breakout_reacceptance: bool
    lower_high_higher_low: bool
    ma_structure_reclaim: bool

    # Historical pattern-break invalidation reference.
    #
    # IMPORTANT: this is NOT Strategy-B trade-stop/risk ownership. It is the
    # opposite confirmed H1 swing that existed immediately before the detected
    # structural break, used only for pattern quality / no-chase analytics.
    #
    # The legacy names below remain for compatibility.
    structural_invalidation_price: Optional[float]
    distance_to_invalidation_atr: Optional[float]

    # Explicit semantic aliases for new callers.
    pattern_break_invalidation_price: Optional[float]
    pattern_break_distance_to_invalidation_atr: Optional[float]
    pattern_invalidation_source: str
    risk_invalidation_authority: bool

    move_consumed_atr: Optional[float]
    structural_room_atr: Optional[float]

    too_far_from_invalidation: bool
    move_already_consumed: bool
    price_ran_without_retest: bool

    calibration_complete_for_retest_failure: bool
    diagnostics: Mapping[str, Any]

    observation_only: bool = True
    execution_authority: bool = False
    strategy_a_mutation: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_entry_pattern_observations(self) -> dict[str, Any]:
        """Contract consumed by strategy_b_entry_patterns.py."""
        return {
            "retest_failure": self.retest_failure,
            "double_top_bottom": self.double_top_bottom,
            "h1_choch_bos": self.h1_choch_bos,
            "h1_displacement": self.h1_displacement,
            "h1_retest": self.h1_retest,
            "h1_order_block_validated": self.h1_order_block_validated,
            "h1_order_block_retest": self.h1_order_block_retest,
            "liquidity_sweep_rejection": self.liquidity_sweep_rejection,
            "failed_breakout_reacceptance": self.failed_breakout_reacceptance,
            "lower_high_higher_low": self.lower_high_higher_low,
            "ma_structure_reclaim": self.ma_structure_reclaim,
            "too_far_from_invalidation": self.too_far_from_invalidation,
            "move_already_consumed": self.move_already_consumed,
            "price_ran_without_retest": self.price_ran_without_retest,
            # Legacy analytics field retained for compatibility.
            "distance_to_invalidation_atr": self.distance_to_invalidation_atr,

            # Explicit pattern-only semantics. These fields must never be used
            # as Strategy-B trade-stop/risk ownership.
            "pattern_break_invalidation_price":
                self.pattern_break_invalidation_price,
            "pattern_break_distance_to_invalidation_atr":
                self.pattern_break_distance_to_invalidation_atr,
            "pattern_invalidation_source": self.pattern_invalidation_source,
            "risk_invalidation_authority": self.risk_invalidation_authority,

            "move_consumed_atr": self.move_consumed_atr,
            "structural_room_atr": self.structural_room_atr,
            "_strategy_b_pattern_observer": {
                "data_ok": self.data_ok,
                "reason": self.reason,
                "reasons": list(self.reasons),
                "break_ms": self.break_ms,
                "break_level": self.break_level,
                "calibration_complete_for_retest_failure":
                    self.calibration_complete_for_retest_failure,
                "observation_only": True,
                "execution_authority": False,
            },
        }


def _atr_from_bias(dxy_bias: Mapping[str, Any]) -> Optional[float]:
    h1 = _mapping(_mapping(dxy_bias).get("timeframes")).get("H1")
    h1 = _mapping(h1)
    features = _mapping(h1.get("features"))
    atr = _num(features.get("atr"))
    return atr if atr is not None and atr > 0 else None


def _room_atr(
    dxy_structure: Optional[Mapping[str, Any]],
    expected_direction: str,
) -> Optional[float]:
    s = _mapping(dxy_structure)
    direct = _num(s.get("directional_room_atr"))
    if direct is not None:
        return direct
    if expected_direction == "BULLISH":
        return _num(s.get("room_up_atr"))
    if expected_direction == "BEARISH":
        return _num(s.get("room_down_atr"))
    return None


def _find_latest_structural_break(
    bars: Sequence[Mapping[str, float | int]],
    *,
    expected_direction: str,
    catalyst_first_seen_ms: int,
    left: int,
    right: int,
) -> dict[str, Any]:
    """
    Locate the first causal close beyond a previously-confirmed H1 swing level.

    The swing definition itself is delegated to bias_engine.detect_swing_structure.
    We evaluate each completed prefix causally: the current bar may break only a
    swing level that existed before that bar.

    Once the first qualifying post-catalyst break is found, its geometry owns this
    H1 causal episode. Later qualifying breaks must not replace it.
    """
    # Need enough bars for two confirmed highs/lows plus current breaker.
    minimum_prefix = max(2 * (left + right + 1), left + right + 5)

    for i in range(minimum_prefix, len(bars)):
        current = bars[i]
        current_ms = _int(current.get("t"))
        if catalyst_first_seen_ms > 0 and current_ms < catalyst_first_seen_ms:
            continue

        prior = bars[:i]
        structure = detect_swing_structure(
            prior,
            left=left,
            right=right,
        )
        state = _upper(structure.get("state")) or "UNKNOWN"
        close = float(current["c"])

        if expected_direction == "BEARISH":
            level = _num(structure.get("last_low"))
            invalidation = _num(structure.get("last_high"))
            broken = level is not None and close < level
        elif expected_direction == "BULLISH":
            level = _num(structure.get("last_high"))
            invalidation = _num(structure.get("last_low"))
            broken = level is not None and close > level
        else:
            continue

        if not broken:
            continue

        return {
            "index": i,
            "ms": current_ms,
            "level": level,
            "close": close,
            "pre_break_structure_state": state,
            "invalidation": invalidation,
            "structure": structure,
        }

    return {}


def _find_retest(
    bars: Sequence[Mapping[str, float | int]],
    *,
    break_index: int,
    break_level: float,
    expected_direction: str,
    atr: float,
) -> dict[str, Any]:
    """
    Find the first causal completed H1 interaction with the broken level.

    Retest existence is structural:

    BEARISH:
        high >= break_level

    BULLISH:
        low <= break_level

    The first actual interaction owns the retest attempt. A later candle
    cannot replace a failed first interaction.

    No ATR proximity tolerance and no arbitrary post-break bar expiry are
    used here. D1/H4 remain the owners of macro-thesis validity.
    """
    direction = _upper(expected_direction)

    if direction not in DIRECTIONS:
        return {}

    if atr <= 0:
        return {}

    for i in range(break_index + 1, len(bars)):
        b = bars[i]

        high = float(b["h"])
        low = float(b["l"])
        close = float(b["c"])

        if direction == "BEARISH":
            interacted = high >= break_level
            failure_close = close < break_level
            sweep_excess = max(0.0, high - break_level)
            distance = max(0.0, break_level - high)
        else:
            interacted = low <= break_level
            failure_close = close > break_level
            sweep_excess = max(0.0, break_level - low)
            distance = max(0.0, low - break_level)

        if not interacted:
            continue

        return {
            "index": i,
            "ms": _int(b.get("t")),
            "distance_atr": (
                abs(distance) / atr
                if atr > 0
                else None
            ),
            "failure_close": bool(failure_close),
            "sweep_excess_atr": (
                sweep_excess / atr
                if atr > 0
                else None
            ),
            "high": high,
            "low": low,
            "close": close,
        }

    return {}

def _find_double_top_bottom(
    bars: Sequence[Mapping[str, float | int]],
    *,
    expected_direction: str,
    catalyst_first_seen_ms: int,
    atr: float,
    tolerance_atr: float,
    left: int,
    right: int,
) -> dict[str, Any]:
    """
    Detect a causal confirmed H1 double top / double bottom.

    BEARISH:
      confirmed swing high #1
      -> confirmed intervening swing low = neckline
      -> confirmed swing high #2 near high #1
      -> completed H1 close below neckline

    BULLISH:
      confirmed swing low #1
      -> confirmed intervening swing high = neckline
      -> confirmed swing low #2 near low #1
      -> completed H1 close above neckline

    Every pivot uses the same canonical left/right pivot semantics as the
    bias engine. Pivot #2 and the neckline pivot must already be confirmed
    before the neckline-break candle may confirm the pattern.

    The second structural test must not predate the catalyst. The first
    structural test may predate it; this permits a fresh catalyst to produce
    the second test/rejection of an already-existing H1 level.

    The first fully confirmed pattern owns the causal episode.
    """
    direction = _upper(expected_direction)
    if direction not in DIRECTIONS:
        return {}

    if atr <= 0:
        return {}

    left = max(1, int(left))
    right = max(1, int(right))
    tolerance = max(0.0, float(tolerance_atr)) * float(atr)

    if len(bars) < (left + right + 3):
        return {}

    pivot_highs: list[dict[str, Any]] = []
    pivot_lows: list[dict[str, Any]] = []

    # Use the same pivot comparison semantics as bias_engine:
    #
    # high:
    #   strictly greater than bars on the left
    #   greater-than-or-equal to bars on the right
    #
    # low:
    #   strictly lower than bars on the left
    #   lower-than-or-equal to bars on the right
    #
    # A pivot at i becomes known only when i + right exists.
    for i in range(left, len(bars) - right):
        center = bars[i]

        high = float(center["h"])
        high_left = [
            float(bars[j]["h"])
            for j in range(i - left, i)
        ]
        high_right = [
            float(bars[j]["h"])
            for j in range(i + 1, i + right + 1)
        ]

        if (
            all(high > x for x in high_left)
            and all(high >= x for x in high_right)
        ):
            confirm_index = i + right
            pivot_highs.append(
                {
                    "index": i,
                    "ms": _int(center.get("t")),
                    "price": high,
                    "confirm_index": confirm_index,
                    "confirm_ms": _int(
                        bars[confirm_index].get("t")
                    ),
                }
            )

        low = float(center["l"])
        low_left = [
            float(bars[j]["l"])
            for j in range(i - left, i)
        ]
        low_right = [
            float(bars[j]["l"])
            for j in range(i + 1, i + right + 1)
        ]

        if (
            all(low < x for x in low_left)
            and all(low <= x for x in low_right)
        ):
            confirm_index = i + right
            pivot_lows.append(
                {
                    "index": i,
                    "ms": _int(center.get("t")),
                    "price": low,
                    "confirm_index": confirm_index,
                    "confirm_ms": _int(
                        bars[confirm_index].get("t")
                    ),
                }
            )

    if direction == "BEARISH":
        same_side = pivot_highs
        opposite = pivot_lows
        pattern_name = "DOUBLE_TOP"
    else:
        same_side = pivot_lows
        opposite = pivot_highs
        pattern_name = "DOUBLE_BOTTOM"

    if len(same_side) < 2:
        return {}

    # Consecutive confirmed same-side pivots define candidate pairs.
    for p in range(1, len(same_side)):
        first = same_side[p - 1]
        second = same_side[p]

        first_index = int(first["index"])
        second_index = int(second["index"])

        if second_index <= first_index + 1:
            continue

        second_ms = int(second["ms"])
        if (
            catalyst_first_seen_ms > 0
            and second_ms < int(catalyst_first_seen_ms)
        ):
            continue

        first_price = float(first["price"])
        second_price = float(second["price"])
        separation = abs(second_price - first_price)

        if separation > tolerance:
            continue

        # Require a genuinely confirmed opposite H1 swing between the two
        # same-side pivots. Do not manufacture the neckline from an arbitrary
        # intrabar min/max.
        between_opposite = [
            x
            for x in opposite
            if first_index < int(x["index"]) < second_index
        ]

        if not between_opposite:
            continue

        # Use the latest confirmed opposite pivot between the two tests.
        # It is the structural neckline immediately preceding test #2.
        neckline_pivot = between_opposite[-1]

        neckline_index = int(neckline_pivot["index"])
        neckline_confirm_index = int(
            neckline_pivot["confirm_index"]
        )
        neckline = float(neckline_pivot["price"])

        # The neckline must have become knowable no later than the point at
        # which pivot #2 itself becomes confirmed.
        second_confirm_index = int(second["confirm_index"])

        if neckline_confirm_index > second_confirm_index:
            continue

        # Confirmation cannot begin until every required structural fact is
        # causally available.
        confirmation_start = max(
            second_confirm_index,
            neckline_confirm_index,
        ) + 1

        confirmed: dict[str, Any] = {}

        for i in range(confirmation_start, len(bars)):
            close = float(bars[i]["c"])

            if direction == "BEARISH":
                broke_neckline = close < neckline
            else:
                broke_neckline = close > neckline

            if not broke_neckline:
                continue

            confirmed = {
                "confirmation_index": i,
                "confirmation_ms": _int(
                    bars[i].get("t")
                ),
                "confirmation_close": close,
            }
            break

        if not confirmed:
            continue

        return {
            "pattern": pattern_name,
            "first_pivot_index": first_index,
            "first_pivot_ms": int(first["ms"]),
            "first_pivot_confirm_index": int(
                first["confirm_index"]
            ),
            "first_pivot_confirm_ms": int(
                first["confirm_ms"]
            ),
            "first_pivot_price": first_price,
            "neckline_pivot_index": neckline_index,
            "neckline_pivot_ms": int(
                neckline_pivot["ms"]
            ),
            "neckline_pivot_confirm_index":
                neckline_confirm_index,
            "neckline_pivot_confirm_ms": int(
                neckline_pivot["confirm_ms"]
            ),
            "neckline": neckline,
            "second_pivot_index": second_index,
            "second_pivot_ms": second_ms,
            "second_pivot_confirm_index":
                second_confirm_index,
            "second_pivot_confirm_ms": int(
                second["confirm_ms"]
            ),
            "second_pivot_price": second_price,
            "pivot_separation_atr": (
                separation / float(atr)
                if atr > 0
                else None
            ),
            **confirmed,
        }

    return {}

def observe_strategy_b_h1_patterns(
    *,
    h1_bars: Sequence[Mapping[str, Any]],
    expected_direction: str,
    catalyst_first_seen_ms: int,
    dxy_bias: Mapping[str, Any],
    dxy_structure: Optional[Mapping[str, Any]] = None,
    as_of_ms: Optional[int] = None,
    config: Optional[StrategyBH1PatternConfig] = None,
) -> StrategyBH1PatternObservation:
    cfg = config or StrategyBH1PatternConfig()
    direction = _upper(expected_direction)
    reasons: list[str] = []

    bars = normalize_bars(
        h1_bars,
        as_of_ms=as_of_ms,
        max_bars=max(int(cfg.min_h1_bars), int(BIAS_DEFAULT_CONFIG.max_bars_h1)),
    )
    atr = _atr_from_bias(dxy_bias)
    room = _room_atr(dxy_structure, direction)

    if direction not in DIRECTIONS:
        reasons.append("EXPECTED_DIRECTION_UNKNOWN")
    if len(bars) < int(cfg.min_h1_bars):
        reasons.append(f"INSUFFICIENT_H1_BARS:{len(bars)}<{int(cfg.min_h1_bars)}")
    if atr is None:
        reasons.append("H1_ATR_UNAVAILABLE_FROM_CANONICAL_BIAS")
    if catalyst_first_seen_ms <= 0:
        reasons.append("CATALYST_FIRST_SEEN_TIME_MISSING")

    data_ok = not any(
        r == "EXPECTED_DIRECTION_UNKNOWN"
        or r.startswith("INSUFFICIENT_H1_BARS:")
        or r == "H1_ATR_UNAVAILABLE_FROM_CANONICAL_BIAS"
        or r == "CATALYST_FIRST_SEEN_TIME_MISSING"
        for r in reasons
    )

    latest_structure = (
        detect_swing_structure(
            bars,
            left=int(cfg.pivot_left),
            right=int(cfg.pivot_right),
        )
        if bars
        else {}
    )

    lower_higher = False
    if direction == "BEARISH":
        lower_higher = _upper(latest_structure.get("high_pattern")) == "LH"
    elif direction == "BULLISH":
        lower_higher = _upper(latest_structure.get("low_pattern")) == "HL"

    break_row: dict[str, Any] = {}
    if data_ok:
        break_row = _find_latest_structural_break(
            bars,
            expected_direction=direction,
            catalyst_first_seen_ms=int(catalyst_first_seen_ms),
            left=int(cfg.pivot_left),
            right=int(cfg.pivot_right),
        )

    h1_choch_bos = bool(break_row)
    if not h1_choch_bos and data_ok:
        reasons.append("NO_POST_CATALYST_H1_PIVOT_BREAK")

    break_index = (
        int(break_row["index"])
        if isinstance(break_row.get("index"), int)
        else None
    )
    break_level = _num(break_row.get("level"))
    break_close = _num(break_row.get("close"))

    displacement_atr = None
    if (
        atr is not None
        and break_level is not None
        and break_close is not None
    ):
        displacement_atr = abs(break_close - break_level) / atr

    # A causal structural break already proves directional displacement:
    # the completed H1 close crossed the previously confirmed swing level.
    #
    # Keep displacement_atr as measured evidence, but do not require an
    # arbitrary minimum ATR magnitude. Phase-7 causal forward-path analysis
    # did not support a stable monotonic production cutoff.
    displacement_calibrated = True
    h1_displacement = bool(
        h1_choch_bos
        and displacement_atr is not None
        and displacement_atr > 0
    )

    # Retest existence is structural, not calibration-owned.
    #
    # The first completed post-break candle that actually interacts with
    # the broken H1 level owns the attempt. Near misses do not count and
    # there is no arbitrary bar-count expiry.
    retest: dict[str, Any] = {}

    if (
        h1_choch_bos
        and atr is not None
        and atr > 0
        and break_index is not None
        and break_level is not None
    ):
        retest = _find_retest(
            bars,
            break_index=break_index,
            break_level=break_level,
            expected_direction=direction,
            atr=atr,
        )

    retest_seen = bool(retest)
    h1_retest = bool(retest_seen and retest.get("failure_close") is True)

    sweep_excess = _num(retest.get("sweep_excess_atr"))

    # LIQUIDITY_SWEEP_REJECTION is a structural observation:
    #
    #   1. The first causal post-break retest trades through the broken level.
    #   2. The completed H1 candle closes back on the breakout side.
    #
    # The existence of the sweep does NOT depend on an arbitrary maximum
    # sweep-depth threshold. max_sweep_excess_atr, when configured, is an
    # optional quality veto for an excessively deep sweep.
    liquidity_sweep_structural = bool(
        h1_retest
        and sweep_excess is not None
        and sweep_excess > 0
    )

    liquidity_sweep_too_deep = bool(
        liquidity_sweep_structural
        and cfg.max_sweep_excess_atr is not None
        and sweep_excess is not None
        and sweep_excess > float(cfg.max_sweep_excess_atr)
    )

    liquidity_sweep = bool(
        liquidity_sweep_structural
        and not liquidity_sweep_too_deep
    )

    # RETEST_FAILURE is structurally defined and requires no guessed
    # ATR proximity or post-break bar-window calibration.
    calibration_complete = True

    # RETEST_FAILURE:
    # A causal H1 structural break already exists. The first causal return
    # approaches the broken level, attempts to recover/re-enter the old
    # structure, but the completed H1 candle closes back on the breakout side.
    #
    # Displacement is deliberately NOT required here. Requiring displacement
    # would make RETEST_FAILURE semantically identical to
    # H1_CHOCH_BOS_DISPLACEMENT_RETEST.
    retest_failure = bool(
        h1_choch_bos
        and h1_retest
    )

    # H1_CHOCH_BOS_DISPLACEMENT_RETEST remains a separate strong pattern.
    # It is assembled downstream from:
    #
    #     causal h1_choch_bos
    #     + positive structural break displacement
    #     + causal structural h1_retest
    #
    # h1_displacement means the completed break close is genuinely beyond the
    # broken structural level. It is not a minimum-ATR strength threshold.

    # These require additional explicit evidence not safely inferable from the
    # existing bias-engine structure contract. Keep fail-closed.
    double_top_bottom_calibrated = (
        cfg.double_top_bottom_tolerance_atr is not None
    )

    double_top_bottom_row: dict[str, Any] = {}

    if (
        data_ok
        and double_top_bottom_calibrated
        and atr is not None
    ):
        double_top_bottom_row = _find_double_top_bottom(
            bars,
            expected_direction=direction,
            catalyst_first_seen_ms=int(catalyst_first_seen_ms),
            atr=float(atr),
            tolerance_atr=float(
                cfg.double_top_bottom_tolerance_atr
            ),
            left=int(cfg.pivot_left),
            right=int(cfg.pivot_right),
        )

    double_top_bottom = bool(double_top_bottom_row)

    if data_ok and not double_top_bottom_calibrated:
        reasons.append(
            "DOUBLE_TOP_BOTTOM_TOLERANCE_NOT_CALIBRATED"
        )

    # These still require additional explicit evidence not safely inferable
    # from the existing structural contract. Keep fail-closed.
    order_block_validated = False
    
    order_block_retest = False
    failed_breakout_reacceptance = False
    ma_structure_reclaim = False

    if h1_choch_bos:
        
        reasons.append("ORDER_BLOCK_NOT_INFERRED_FROM_LAST_OPPOSITE_CANDLE")
        reasons.append("FAILED_BREAKOUT_REACCEPTANCE_NOT_YET_WIRED")
        reasons.append("MA_STRUCTURE_RECLAIM_NOT_YET_WIRED")

    invalidation = _num(break_row.get("invalidation"))
    last_close = float(bars[-1]["c"]) if bars else None

    distance_to_invalidation_atr = None
    if (
        atr is not None and atr > 0
        and invalidation is not None
        and last_close is not None
    ):
        distance_to_invalidation_atr = abs(last_close - invalidation) / atr

    move_consumed_atr = None
    if (
        atr is not None and atr > 0
        and break_close is not None
        and last_close is not None
    ):
        move_consumed_atr = abs(last_close - break_close) / atr

    # No-chase threshold is also a calibration item. We only assert it when the
    # caller explicitly supplies max_move_without_retest_atr.
    price_ran_without_retest = bool(
        h1_choch_bos
        and not retest_seen
        and cfg.max_move_without_retest_atr is not None
        and move_consumed_atr is not None
        and move_consumed_atr > float(cfg.max_move_without_retest_atr)
    )

    if (
        h1_choch_bos
        and not retest_seen
        and cfg.max_move_without_retest_atr is None
    ):
        reasons.append("NO_CHASE_MOVE_THRESHOLD_NOT_CALIBRATED")

    if not reasons:
        reasons.append("H1_PATTERN_OBSERVATION_OK")

    if not data_ok:
        reason = reasons[0]
    elif retest_failure:
        reason = "RETEST_FAILURE_CONFIRMED"
    elif h1_choch_bos and h1_retest and h1_displacement:
        reason = "H1_DISPLACEMENT_RETEST_CONFIRMED"
    elif h1_choch_bos:
        reason = "H1_BREAK_OBSERVED_AWAIT_PATTERN_CONFIRMATION"
    else:
        reason = "WAIT_H1_PATTERN"

    return StrategyBH1PatternObservation(
        schema_version=1,
        model="STRATEGY_B_H1_PATTERN_OBSERVER_V1",
        expected_direction=direction or "UNKNOWN",
        data_ok=data_ok,
        reason=reason,
        reasons=tuple(dict.fromkeys(reasons)),
        bars_used=len(bars),
        last_bar_ms=_int(bars[-1].get("t")) if bars else 0,
        atr=atr,
        h1_choch_bos=h1_choch_bos,
        break_direction=direction if h1_choch_bos else "UNKNOWN",
        break_ms=_int(break_row.get("ms")),
        break_bar_index=break_index,
        break_level=break_level,
        break_close=break_close,
        pre_break_structure_state=_upper(
            break_row.get("pre_break_structure_state")
        ) or "UNKNOWN",
        displacement_atr=displacement_atr,
        h1_displacement=h1_displacement,
        retest_seen=retest_seen,
        retest_ms=_int(retest.get("ms")),
        retest_distance_atr=_num(retest.get("distance_atr")),
        h1_retest=h1_retest,
        retest_failure=retest_failure,
        double_top_bottom=double_top_bottom,
        h1_order_block_validated=order_block_validated,
        h1_order_block_retest=order_block_retest,
        liquidity_sweep_rejection=liquidity_sweep,
        failed_breakout_reacceptance=failed_breakout_reacceptance,
        lower_high_higher_low=lower_higher,
        ma_structure_reclaim=ma_structure_reclaim,
        # Compatibility aliases. The source is the PRE-BREAK confirmed H1
        # opposite swing, not the current trade-stop owner.
        structural_invalidation_price=invalidation,
        distance_to_invalidation_atr=distance_to_invalidation_atr,

        # Explicit semantics for all new Strategy-B callers.
        pattern_break_invalidation_price=invalidation,
        pattern_break_distance_to_invalidation_atr=(
            distance_to_invalidation_atr
        ),
        pattern_invalidation_source=(
            "PRE_BREAK_CONFIRMED_H1_SWING"
            if invalidation is not None
            else "UNAVAILABLE"
        ),
        risk_invalidation_authority=False,

        move_consumed_atr=move_consumed_atr,
        structural_room_atr=room,
        too_far_from_invalidation=False,
        move_already_consumed=False,
        price_ran_without_retest=price_ran_without_retest,
        calibration_complete_for_retest_failure=calibration_complete,
        diagnostics={
            "latest_structure": dict(latest_structure),
            "break": dict(break_row),
            "retest": dict(retest),
            "config": asdict(cfg),
            "thresholds_invented": False,
            "uses_bias_engine_swing_structure": True,
            "order_block_fail_closed": True,
            "double_top_bottom_fail_closed":
                not bool(double_top_bottom_calibrated),
            "failed_breakout_fail_closed": True,
            "double_top_bottom": dict(double_top_bottom_row),
            "double_top_bottom_calibrated":
                bool(double_top_bottom_calibrated),
            "ma_reclaim_fail_closed": True,
            "pattern_invalidation_semantics": (
                "PRE_BREAK_CONFIRMED_H1_SWING_ANALYTICS_ONLY"
            ),
            "risk_invalidation_authority": False,
            "liquidity_sweep": {
                "structural": bool(liquidity_sweep_structural),
                "sweep_excess_atr": sweep_excess,
                "max_sweep_excess_atr":
                    cfg.max_sweep_excess_atr,
                "too_deep": bool(liquidity_sweep_too_deep),
                "accepted": bool(liquidity_sweep),
            },
            "trade_stop_owner_module": (
                "strategy_b_structural_invalidation_observer"
            ),
        },
    )


__all__ = [
    "StrategyBH1PatternConfig",
    "StrategyBH1PatternObservation",
    "observe_strategy_b_h1_patterns",
]
