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

    # Research/calibration items. Deliberately None by default.
    min_displacement_atr: Optional[float] = None
    retest_tolerance_atr: Optional[float] = None
    max_retest_bars: Optional[int] = None
    max_sweep_excess_atr: Optional[float] = None
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
    Locate the latest close beyond a previously-confirmed H1 swing level.

    The swing definition itself is delegated to bias_engine.detect_swing_structure.
    We evaluate each completed prefix causally: the current bar may break only a
    swing level that existed before that bar.
    """
    latest: dict[str, Any] = {}

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

        latest = {
            "index": i,
            "ms": current_ms,
            "level": level,
            "close": close,
            "pre_break_structure_state": state,
            "invalidation": invalidation,
            "structure": structure,
        }

    return latest


def _find_retest(
    bars: Sequence[Mapping[str, float | int]],
    *,
    break_index: int,
    break_level: float,
    expected_direction: str,
    atr: float,
    tolerance_atr: float,
    max_retest_bars: int,
) -> dict[str, Any]:
    tolerance = max(0.0, float(tolerance_atr)) * atr
    stop = min(len(bars), break_index + 1 + max(1, int(max_retest_bars)))

    best: dict[str, Any] = {}
    for i in range(break_index + 1, stop):
        b = bars[i]
        high = float(b["h"])
        low = float(b["l"])
        close = float(b["c"])

        if expected_direction == "BEARISH":
            distance = max(0.0, break_level - high)
            approached = high >= break_level - tolerance
            failure_close = close < break_level
            sweep_excess = max(0.0, high - break_level)
        else:
            distance = max(0.0, low - break_level)
            approached = low <= break_level + tolerance
            failure_close = close > break_level
            sweep_excess = max(0.0, break_level - low)

        if approached:
            best = {
                "index": i,
                "ms": _int(b.get("t")),
                "distance_atr": abs(distance) / atr if atr > 0 else None,
                "failure_close": bool(failure_close),
                "sweep_excess_atr": sweep_excess / atr if atr > 0 else None,
                "high": high,
                "low": low,
                "close": close,
            }
            # First causal retest is the relevant retest attempt.
            break

    return best


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

    displacement_calibrated = cfg.min_displacement_atr is not None
    h1_displacement = bool(
        h1_choch_bos
        and displacement_calibrated
        and displacement_atr is not None
        and displacement_atr >= float(cfg.min_displacement_atr)
    )
    if h1_choch_bos and not displacement_calibrated:
        reasons.append("DISPLACEMENT_THRESHOLD_NOT_CALIBRATED")

    retest_calibrated = (
        cfg.retest_tolerance_atr is not None
        and cfg.max_retest_bars is not None
    )
    retest: dict[str, Any] = {}
    if (
        h1_choch_bos
        and retest_calibrated
        and atr is not None
        and break_index is not None
        and break_level is not None
    ):
        retest = _find_retest(
            bars,
            break_index=break_index,
            break_level=break_level,
            expected_direction=direction,
            atr=atr,
            tolerance_atr=float(cfg.retest_tolerance_atr),
            max_retest_bars=int(cfg.max_retest_bars),
        )
    elif h1_choch_bos and not retest_calibrated:
        reasons.append("RETEST_TOLERANCE_OR_WINDOW_NOT_CALIBRATED")

    retest_seen = bool(retest)
    h1_retest = bool(retest_seen and retest.get("failure_close") is True)

    sweep_calibrated = cfg.max_sweep_excess_atr is not None
    sweep_excess = _num(retest.get("sweep_excess_atr"))
    liquidity_sweep = bool(
        h1_retest
        and sweep_calibrated
        and sweep_excess is not None
        and sweep_excess > 0
        and sweep_excess <= float(cfg.max_sweep_excess_atr)
    )

    calibration_complete = bool(
        displacement_calibrated and retest_calibrated
    )
    retest_failure = bool(
        h1_choch_bos
        and h1_displacement
        and h1_retest
        and calibration_complete
    )

    # These require additional explicit evidence not safely inferable from the
    # existing bias-engine structure contract. Keep fail-closed.
    double_top_bottom = False
    order_block_validated = False
    order_block_retest = False
    failed_breakout_reacceptance = False
    ma_structure_reclaim = False

    if h1_choch_bos:
        reasons.append("DOUBLE_TOP_BOTTOM_NOT_INFERRED_FROM_PIVOT_BREAK_ALONE")
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
    elif h1_choch_bos and not calibration_complete:
        reason = "H1_BREAK_OBSERVED_AWAIT_PATTERN_CALIBRATION"
    elif h1_choch_bos:
        reason = "H1_BREAK_OBSERVED_AWAIT_RETEST_FAILURE"
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
            "double_top_bottom_fail_closed": True,
            "failed_breakout_fail_closed": True,
            "ma_reclaim_fail_closed": True,
            "pattern_invalidation_semantics": (
                "PRE_BREAK_CONFIRMED_H1_SWING_ANALYTICS_ONLY"
            ),
            "risk_invalidation_authority": False,
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
