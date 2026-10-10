"""XauTrendLab canonical PO3/AMD market-phase engine.

Production-shaped, strategy-neutral and execution-neutral.

Design contracts
----------------
* H1 owns structural/consolidation context.
* M15 owns official transition evidence.
* Only candles completed by ``as_of_ms`` are visible to the engine.
* Market classification is independent of Strategy A/B and trade side.
* Measurements are separated from policy/classification so research and live
  shadow execution use the exact same causal calculations.
* No PLACE/WAIT/BLOCK/CLOSE, order, risk, sizing or SL/TP authority exists here.

The initial release intentionally exposes raw measurements and conservative
candidate states. Final PO3 qualification thresholds should be frozen only
from causal replay/prospective evidence, not fitted trade-by-trade.
"""
from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple
import hashlib
import json
import math

SCHEMA_VERSION = "XTL_PO3_V1_SHADOW"
H1_MS = 60 * 60 * 1000
M15_MS = 15 * 60 * 1000


@dataclass(frozen=True)
class PO3Config:
    # Measurement windows are fixed research parameters, not winner-fitted.
    h1_lookback_bars: int = 8
    h1_atr_period: int = 14
    m15_atr_period: int = 14
    m15_transition_lookback_bars: int = 12
    touch_tolerance_atr: float = 0.10
    acceptance_tolerance_atr: float = 0.05
    fvg_min_gap_atr: float = 0.10

    # Legacy master classifier remains OFF.  PO3 phases are promoted
    # independently so each lifecycle stage can be replay-validated before
    # receiving semantic authority.
    enable_phase_classifier: bool = False

    # Accumulation V1 may classify/freeze a structural generation.
    # Manipulation and Distribution remain disabled until separately validated.
    enable_accumulation_classifier: bool = True
    enable_manipulation_classifier: bool = False
    enable_distribution_classifier: bool = False

    # Provisional thresholds are isolated here for later promotion. They do
    # nothing while enable_phase_classifier=False.
    accumulation_max_width_atr: float = 3.0
    accumulation_min_overlap_ratio: float = 0.45
    accumulation_max_efficiency_ratio: float = 0.55
    accumulation_min_boundary_touches: int = 1
    manipulation_min_sweep_atr: float = 0.05
    manipulation_max_reclaim_bars: int = 2
    distribution_min_displacement_atr: float = 0.60
    distribution_min_body_ratio: float = 0.55
    # Distribution maturity V1 research landmarks.
    # Measurement/shadow only. These do not grant phase or execution authority.
    distribution_developing_atr: float = 0.50
    distribution_mature_atr: float = 1.50
    distribution_extended_atr: float = 2.00


def _f(v: Any, default: float = 0.0) -> float:
    try:
        x = float(v)
        return x if math.isfinite(x) else default
    except Exception:
        return default


def _i(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _bar(b: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        "t_open_ms": _i(b.get("t_open_ms") or b.get("tOpenMs") or b.get("open_time_ms") or (_i(b.get("t")) * 1000)),
        "o": _f(b.get("o", b.get("open"))),
        "h": _f(b.get("h", b.get("high"))),
        "l": _f(b.get("l", b.get("low"))),
        "c": _f(b.get("c", b.get("close"))),
        "v": _f(b.get("v", b.get("volume"))),
    }


def completed_bars(bars: Iterable[Mapping[str, Any]], *, as_of_ms: int, tf_ms: int) -> List[Dict[str, Any]]:
    """Return sorted bars that were definitely closed at as_of_ms."""
    out = []
    for raw in bars or []:
        b = _bar(raw)
        t = b["t_open_ms"]
        if t > 0 and t + tf_ms <= int(as_of_ms):
            out.append(b)
    out.sort(key=lambda x: x["t_open_ms"])
    return out


def true_range(cur: Mapping[str, Any], prev_close: Optional[float]) -> float:
    h, l = _f(cur.get("h")), _f(cur.get("l"))
    if prev_close is None:
        return max(0.0, h - l)
    return max(0.0, h - l, abs(h - prev_close), abs(l - prev_close))


def atr(bars: Sequence[Mapping[str, Any]], period: int = 14) -> float:
    if not bars:
        return 0.0
    trs: List[float] = []
    prev: Optional[float] = None
    for b in bars:
        trs.append(true_range(b, prev))
        prev = _f(b.get("c"))
    tail = trs[-max(1, int(period)):]
    return sum(tail) / len(tail) if tail else 0.0


def _overlap(a: Mapping[str, Any], b: Mapping[str, Any]) -> float:
    lo = max(_f(a.get("l")), _f(b.get("l")))
    hi = min(_f(a.get("h")), _f(b.get("h")))
    union_lo = min(_f(a.get("l")), _f(b.get("l")))
    union_hi = max(_f(a.get("h")), _f(b.get("h")))
    union = max(0.0, union_hi - union_lo)
    return max(0.0, hi - lo) / union if union > 0 else 0.0


def measure_h1_consolidation(h1: Sequence[Mapping[str, Any]], cfg: PO3Config) -> Dict[str, Any]:
    if len(h1) < max(3, cfg.h1_lookback_bars):
        return {"available": False, "reason": "INSUFFICIENT_H1_BARS", "bars_available": len(h1)}

    window = list(h1[-cfg.h1_lookback_bars:])
    prior = list(h1[:-cfg.h1_lookback_bars])
    a = atr(h1, cfg.h1_atr_period)
    hi = max(_f(x["h"]) for x in window)
    lo = min(_f(x["l"]) for x in window)
    width = max(0.0, hi - lo)
    mid = (hi + lo) / 2.0

    overlaps = [_overlap(window[j - 1], window[j]) for j in range(1, len(window))]
    overlap_ratio = sum(overlaps) / len(overlaps) if overlaps else 0.0

    # Directional efficiency across the complete observed H1 path.
    #
    # The numerator and denominator must use the same starting point.
    # Include the first candle's open->close movement, then all subsequent
    # close->close movement.  By the triangle inequality this keeps the
    # metric bounded to [0, 1]:
    #
    #   ~0 = rotational / overlapping movement
    #   ~1 = efficient directional movement
    first_open = _f(window[0]["o"])
    first_close = _f(window[0]["c"])

    path = abs(first_close - first_open)
    path += sum(
        abs(_f(window[j]["c"]) - _f(window[j - 1]["c"]))
        for j in range(1, len(window))
    )

    net = abs(_f(window[-1]["c"]) - first_open)
    efficiency = min(1.0, net / path) if path > 0 else 0.0

    ranges = [max(0.0, _f(x["h"]) - _f(x["l"])) for x in window]
    avg_range = sum(ranges) / len(ranges) if ranges else 0.0
    prior_tail = prior[-cfg.h1_lookback_bars:] if prior else []
    prior_ranges = [max(0.0, _f(x["h"]) - _f(x["l"])) for x in prior_tail]
    prior_avg = sum(prior_ranges) / len(prior_ranges) if prior_ranges else 0.0
    compression = avg_range / prior_avg if prior_avg > 0 else None

    tol = cfg.touch_tolerance_atr * a if a > 0 else 0.0
    hi_touches = sum(1 for x in window if hi - _f(x["h"]) <= tol)
    lo_touches = sum(1 for x in window if _f(x["l"]) - lo <= tol)

    return {
        "available": True,
        "window_bars": len(window),
        "window_start_ms": window[0]["t_open_ms"],
        "window_end_ms": window[-1]["t_open_ms"] + H1_MS,
        "range_high": hi,
        "range_low": lo,
        "range_mid": mid,
        "range_width": width,
        "atr": a,
        "range_width_atr": width / a if a > 0 else None,
        "boundary_touches_high": hi_touches,
        "boundary_touches_low": lo_touches,

        # The bars establishing range_high/range_low already count as
        # boundary touches.  Revisit counts remove that baseline touch so
        # repeated boundary participation is measurable explicitly.
        "boundary_revisits_high": max(0, hi_touches - 1),
        "boundary_revisits_low": max(0, lo_touches - 1),
        "two_sided_boundary_revisit": (
            hi_touches >= 2 and lo_touches >= 2
        ),
        "overlap_ratio": overlap_ratio,
        "efficiency_ratio": efficiency,
        "avg_bar_range_atr": avg_range / a if a > 0 else None,
        "atr_compression_ratio": compression,
    }


def measure_m15_transition(
    m15: Sequence[Mapping[str, Any]],
    h1_range: Mapping[str, Any],
    cfg: PO3Config,
) -> Dict[str, Any]:
    """Measure M15 interaction with the structural PO3 boundaries.

    Raw boundary crossings and qualified sweep/reclaim evidence are kept
    separate.  This function measures evidence only; it does not change
    PO3 phase state.
    """
    if not h1_range.get("available") or not m15:
        return {
            "available": False,
            "reason": "RANGE_OR_M15_UNAVAILABLE",
        }

    hi = _f(h1_range.get("range_high"))
    lo = _f(h1_range.get("range_low"))
    # Current ATR remains useful as a snapshot-level diagnostic only.
    # Historical event qualification must NOT use this value because it
    # changes as later M15 candles arrive.
    a = atr(m15, cfg.m15_atr_period)

    tail_start = max(
        0,
        len(m15) - cfg.m15_transition_lookback_bars,
    )

    tail = list(m15[tail_start:])

    events: List[Dict[str, Any]] = []

    def add_event(
        *,
        idx: int,
        side: str,
        depth: float,
    ) -> None:
        b = tail[idx]

        # Causal immutable normalization:
        # ATR for this sweep is calculated using only completed M15 bars
        # available through the sweep candle itself.
        absolute_idx = tail_start + idx

        event_history = m15[
            : absolute_idx + 1
        ]

        event_atr = atr(
            event_history,
            cfg.m15_atr_period,
        )

        depth_atr = (
            depth / event_atr
            if event_atr > 0
            else None
        )

        sweep_qualified = bool(
            depth_atr is not None
            and depth_atr
                >= cfg.manipulation_min_sweep_atr
        )

        # A sweep candle may itself reject/reclaim the boundary by closing
        # back inside.  Preserve that as SAME_BAR_RECLAIM rather than
        # silently treating it as an ordinary later-bar reclaim.
        reclaim_idx = None

        for k in range(
            idx,
            min(
                len(tail),
                idx
                + cfg.manipulation_max_reclaim_bars
                + 1,
            ),
        ):
            close = _f(tail[k]["c"])

            reclaimed = (
                close <= hi
                if side == "HIGH"
                else close >= lo
            )

            if reclaimed:
                reclaim_idx = k
                break

        reclaim_confirmed = (
            reclaim_idx is not None
        )

        reclaim_bars = (
            reclaim_idx - idx
            if reclaim_idx is not None
            else None
        )

        reclaim_ms = (
            tail[reclaim_idx]["t_open_ms"]
            + M15_MS
            if reclaim_idx is not None
            else None
        )

        reclaim_close_location = (
            (
                _f(tail[reclaim_idx]["c"]) - lo
            )
            / (hi - lo)
            if (
                reclaim_idx is not None
                and hi > lo
            )
            else None
        )

        if reclaim_idx is None:
            reclaim_type = None
        elif reclaim_idx == idx:
            reclaim_type = "SAME_BAR_RECLAIM"
        else:
            reclaim_type = "LATER_BAR_RECLAIM"

        direction = (
            "BEARISH"
            if side == "HIGH"
            else "BULLISH"
        )

        events.append(
            {
                "side": side,
                "direction": direction,

                # Raw factual observation.
                "boundary_crossed": True,

                "sweep_ms": (
                    b["t_open_ms"] + M15_MS
                ),
                "depth": depth,
                "depth_atr": depth_atr,

                # ATR frozen to information available when this sweep
                # candle completed.  This must remain invariant across
                # later evaluations of the same event.
                "event_atr": event_atr,
                "event_atr_period": (
                    cfg.m15_atr_period
                ),

                # Qualification is explicit and independent
                # from the existence of the crossing.
                "sweep_qualified": sweep_qualified,

                "reclaim_confirmed": (
                    reclaim_confirmed
                ),
                "reclaim_bars": reclaim_bars,
                "reclaim_ms": reclaim_ms,
                "reclaim_type": reclaim_type,
                "reclaim_close_location": (
                    reclaim_close_location
                ),

                # A manipulation candidate requires BOTH
                # meaningful sweep depth and a causal reclaim.
                "manipulation_candidate": bool(
                    sweep_qualified
                    and reclaim_confirmed
                ),
            }
        )

    for idx, b in enumerate(tail):
        sweep_high = max(
            0.0,
            _f(b["h"]) - hi,
        )

        sweep_low = max(
            0.0,
            lo - _f(b["l"]),
        )

        if sweep_high > 0:
            add_event(
                idx=idx,
                side="HIGH",
                depth=sweep_high,
            )

        if sweep_low > 0:
            add_event(
                idx=idx,
                side="LOW",
                depth=sweep_low,
            )

    latest = (
        events[-1]
        if events
        else None
    )

    qualified = [
        e for e in events
        if e.get("sweep_qualified")
    ]

    manipulation_candidates = [
        e for e in events
        if e.get("manipulation_candidate")
    ]

    return {
        "available": True,
        "atr": a,
        "events": events,

        # Backward-compatible raw latest event.
        "latest": latest,

        # Explicit semantic views for future state logic.
        "latest_raw_crossing": latest,
        "latest_qualified_sweep": (
            qualified[-1]
            if qualified
            else None
        ),
        "latest_manipulation_candidate": (
            manipulation_candidates[-1]
            if manipulation_candidates
            else None
        ),

        "raw_crossing_count": len(events),
        "qualified_sweep_count": len(qualified),
        "manipulation_candidate_count": len(
            manipulation_candidates
        ),
    }


def _bar_displacement(b: Mapping[str, Any], a: float) -> Dict[str, Any]:
    rng = max(0.0, _f(b["h"]) - _f(b["l"]))
    body = abs(_f(b["c"]) - _f(b["o"]))
    direction = "BULLISH" if _f(b["c"]) > _f(b["o"]) else "BEARISH" if _f(b["c"]) < _f(b["o"]) else "NEUTRAL"
    return {"direction": direction, "range_atr": rng / a if a > 0 else None,
            "body_atr": body / a if a > 0 else None, "body_ratio": body / rng if rng > 0 else 0.0}


def detect_fvgs(m15: Sequence[Mapping[str, Any]], cfg: PO3Config) -> List[Dict[str, Any]]:
    """Causal FVG events. Fill state is evaluated only with supplied completed bars."""
    if len(m15) < 3:
        return []
    a = atr(m15, cfg.m15_atr_period)
    out: List[Dict[str, Any]] = []
    for j in range(2, len(m15)):
        b1, b2, b3 = m15[j-2], m15[j-1], m15[j]
        direction = None; low = high = 0.0
        if _f(b3["l"]) > _f(b1["h"]):
            direction, low, high = "BULLISH", _f(b1["h"]), _f(b3["l"])
        elif _f(b3["h"]) < _f(b1["l"]):
            direction, low, high = "BEARISH", _f(b3["h"]), _f(b1["l"])
        if not direction:
            continue
        gap = high - low
        if a > 0 and gap / a < cfg.fvg_min_gap_atr:
            continue
        mid = (low + high) / 2.0
        status = "OPEN"; first_retest = None; max_fill = 0.0
        for future in m15[j+1:]:
            if direction == "BULLISH":
                penetration = max(0.0, high - _f(future["l"]))
            else:
                penetration = max(0.0, _f(future["h"]) - low)
            if penetration > 0 and first_retest is None:
                first_retest = future["t_open_ms"] + M15_MS
            max_fill = max(max_fill, min(1.0, penetration / gap if gap > 0 else 0.0))
            if max_fill >= 1.0:
                status = "FILLED"; break
            if max_fill >= 0.5:
                status = "PARTIAL"
        out.append({"direction": direction, "created_ms": b3["t_open_ms"] + M15_MS,
                    "low": low, "high": high, "mid": mid, "gap": gap,
                    "gap_atr": gap / a if a > 0 else None, "status": status,
                    "first_retest_ms": first_retest, "max_fill_ratio": max_fill})
    return out


def measure_distribution(
    m15: Sequence[Mapping[str, Any]],
    h1_range: Mapping[str, Any],
    cfg: PO3Config,
) -> Dict[str, Any]:
    """Measure causal M15 distribution evidence.

    Historical displacement normalization is frozen to information
    available when each M15 candle completed.

    This function measures evidence only.  It does not grant
    Distribution phase authority.
    """
    if not h1_range.get("available") or not m15:
        return {
            "available": False,
            "reason": "RANGE_OR_M15_UNAVAILABLE",
        }

    hi = _f(h1_range.get("range_high"))
    lo = _f(h1_range.get("range_low"))

    # Snapshot-level ATR only.  Historical rows must use event_atr.
    current_atr = atr(
        m15,
        cfg.m15_atr_period,
    )

    tail_start = max(
        0,
        len(m15) - cfg.m15_transition_lookback_bars,
    )

    tail = list(m15[tail_start:])

    rows: List[Dict[str, Any]] = []

    for idx, b in enumerate(tail):
        absolute_idx = tail_start + idx

        # Immutable ATR as known at this candle close.
        event_history = m15[
            : absolute_idx + 1
        ]

        event_atr = atr(
            event_history,
            cfg.m15_atr_period,
        )

        high = _f(b["h"])
        low = _f(b["l"])
        open_ = _f(b["o"])
        close = _f(b["c"])

        bar_range = max(
            0.0,
            high - low,
        )

        body = abs(
            close - open_
        )

        direction = (
            "BULLISH"
            if close > open_
            else "BEARISH"
            if close < open_
            else "NEUTRAL"
        )

        range_atr = (
            bar_range / event_atr
            if event_atr > 0
            else None
        )

        body_atr = (
            body / event_atr
            if event_atr > 0
            else None
        )

        body_ratio = (
            body / bar_range
            if bar_range > 0
            else 0.0
        )

        if close > hi:
            outside_direction = "BULLISH"
            outside_distance = close - hi
        elif close < lo:
            outside_direction = "BEARISH"
            outside_distance = lo - close
        else:
            outside_direction = None
            outside_distance = 0.0

        outside_distance_atr = (
            outside_distance / event_atr
            if event_atr > 0
            else None
        )

        # V1 displacement evidence:
        # direction must agree with the boundary break and the candle
        # itself must show sufficient normalized range and body quality.
        displacement_qualified = bool(
            outside_direction is not None
            and direction == outside_direction
            and range_atr is not None
            and range_atr
                >= cfg.distribution_min_displacement_atr
            and body_ratio
                >= cfg.distribution_min_body_ratio
        )

        follow = False
        follow_ms = None

        # Follow-through is causal because this next bar exists here only
        # after completed_bars() has admitted it at the current as_of_ms.
        if (
            outside_direction is not None
            and idx + 1 < len(tail)
        ):
            n = tail[idx + 1]

            if outside_direction == "BULLISH":
                follow = (
                    _f(n["h"]) > high
                )
            else:
                follow = (
                    _f(n["l"]) < low
                )

            if follow:
                follow_ms = (
                    _i(n["t_open_ms"])
                    + M15_MS
                )

        rows.append(
            {
                "close_ms": (
                    _i(b["t_open_ms"])
                    + M15_MS
                ),

                "direction": direction,

                "range": bar_range,
                "body": body,

                "event_atr": event_atr,
                "event_atr_period": (
                    cfg.m15_atr_period
                ),

                "range_atr": range_atr,
                "body_atr": body_atr,
                "body_ratio": body_ratio,

                "close_outside_boundary": bool(
                    outside_direction
                ),
                "outside_direction": (
                    outside_direction
                ),
                "outside_distance": (
                    outside_distance
                ),
                "outside_distance_atr": (
                    outside_distance_atr
                ),

                "displacement_qualified": (
                    displacement_qualified
                ),

                "follow_through_new_extreme": (
                    follow
                ),
                "follow_through_ms": (
                    follow_ms
                ),
            }
        )

    qualified = [
        row for row in rows
        if row.get("displacement_qualified")
    ]

    return {
        "available": True,

        # Current diagnostic only; never use this to reinterpret
        # historical displacement rows.
        "atr": current_atr,

        "bars": rows,
        "latest": (
            rows[-1]
            if rows
            else None
        ),

        "latest_qualified_displacement": (
            qualified[-1]
            if qualified
            else None
        ),

        "qualified_displacement_count": len(
            qualified
        ),
    }



def measure_outside_episodes(
    m15: Sequence[Mapping[str, Any]],
    h1_range: Mapping[str, Any],
    distribution: Mapping[str, Any],
    cfg: PO3Config,
    *,
    generation_id: Optional[str] = None,
    generation_start_ms: Optional[int] = None,
    previous_state: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """Advance causal outside-episode lifecycle for one frozen generation.

    Episode identity is persistent across evaluations.  Only completed M15
    candles newer than last_processed_m15_close_ms are processed.

    This function measures lifecycle evidence only.  It grants no PO3 phase
    authority.
    """
    if not h1_range.get("available") or not m15:
        return {
            "available": False,
            "reason": "RANGE_OR_M15_UNAVAILABLE",
            "state": {
                "generation_id": generation_id,
                "last_processed_m15_close_ms": 0,
                "active": None,
                "completed": [],
            },
        }

    hi = _f(h1_range.get("range_high"))
    lo = _f(h1_range.get("range_low"))

    gid = str(
        generation_id
        or h1_range.get("generation_id")
        or ""
    ).strip()

    # Causal ownership boundary for this ACTIVE generation.
    #
    # A generation may only own M15 transition evidence that
    # completes strictly AFTER the generation was confirmed.
    # Evidence at or before confirmation is pre-generation
    # context and must not create lifecycle state.
    lifecycle_start_ms = _i(
        generation_start_ms
    )

    # ------------------------------------------------------------
    # Restore state only when it belongs to this exact generation.
    # A new generation must never inherit an old generation's
    # departure episode.
    # ------------------------------------------------------------
    restored = None

    if isinstance(previous_state, Mapping):
        ls = previous_state.get("lifecycle_state")

        if isinstance(ls, Mapping):
            oe = ls.get("outside_episode")

            if (
                isinstance(oe, Mapping)
                and str(
                    oe.get("generation_id") or ""
                ).strip() == gid
            ):
                restored = oe

    if restored is not None:
        last_processed = _i(
            restored.get(
                "last_processed_m15_close_ms"
            )
        )

        old_active = restored.get("active")
        active = (
            dict(old_active)
            if isinstance(old_active, Mapping)
            else None
        )

        completed = [
            dict(x)
            for x in (
                restored.get("completed") or []
            )
            if isinstance(x, Mapping)
        ]
    else:
        # Fresh ACTIVE generation:
        # do not replay M15 transition evidence from before the
        # generation causally existed.
        #
        # Processing below uses:
        #     close_ms <= last_processed -> skip
        #
        # therefore seeding the cursor at confirmed_ms makes the
        # first eligible M15 candle:
        #     close_ms > confirmed_ms
        last_processed = (
            lifecycle_start_ms
            if gid and lifecycle_start_ms > 0
            else 0
        )
        active = None
        completed = []

    # Bounded lifecycle history.  Enough for research/diagnostics,
    # but never allow state to grow without limit.
    max_completed_history = 32

    dist_bars = (
        list(distribution.get("bars") or [])
        if isinstance(distribution, Mapping)
        else []
    )

    qualified_by_close = {
        _i(row.get("close_ms")): row
        for row in dist_bars
        if row.get("displacement_qualified")
        and row.get("close_ms") is not None
    }
    

    def relation(close: float) -> str:
        if close > hi:
            return "ABOVE"
        if close < lo:
            return "BELOW"
        return "INSIDE"

    def start_episode(
        close_ms: int,
        rel: str,
    ) -> Optional[Dict[str, Any]]:
        d = qualified_by_close.get(close_ms)

        if not d:
            return None

        direction = str(
            d.get("outside_direction") or ""
        ).upper()

        expected = (
            "ABOVE"
            if direction == "BULLISH"
            else "BELOW"
            if direction == "BEARISH"
            else None
        )

        if expected is None or rel != expected:
            return None

        return {
            "episode_id": (
                f"{gid}:{direction}:{close_ms}"
            ),
            "generation_id": gid,
            "direction": direction,
            "boundary": (
                "HIGH"
                if direction == "BULLISH"
                else "LOW"
            ),
            "started_ms": close_ms,
            "initiation_ms": close_ms,

            "initiation_range_atr":
                d.get("range_atr"),
            "initiation_body_ratio":
                d.get("body_ratio"),
            "initiation_outside_distance_atr":
                d.get("outside_distance_atr"),

            "acceptance_confirmed": False,
            "acceptance_ms": None,
            "acceptance_bars": 0,

            "outside_close_count": 1,
            "consecutive_outside_closes": 1,
            "max_consecutive_outside_closes": 1,

            "last_outside_ms": close_ms,

            # Structural progress from the frozen boundary.
            "max_outside_distance": (
                max(0.0, close - hi)
                if direction == "BULLISH"
                else max(0.0, lo - close)
            ),
            "max_outside_distance_atr":
                d.get("outside_distance_atr"),
            "max_outside_ms": close_ms,

            # Lifecycle duration.  Initiation candle = bar 1.
            "duration_bars": 1,
            "accepted_duration_bars": 0,

            "returned_inside": False,
            "returned_inside_ms": None,

            "termination_relation": None,
            "termination_close": None,
            "termination_distance_atr": None,

            "ended_ms": None,

            "status": "OUTSIDE_CANDIDATE",

            # Distribution maturity V1 causal state.
            "reached_050_ms": (close_ms if d.get("outside_distance_atr") is not None and _f(d.get("outside_distance_atr")) >= 0.50 else None),
            "reached_100_ms": (close_ms if d.get("outside_distance_atr") is not None and _f(d.get("outside_distance_atr")) >= 1.00 else None),
            "reached_150_ms": (close_ms if d.get("outside_distance_atr") is not None and _f(d.get("outside_distance_atr")) >= 1.50 else None),
            "reached_200_ms": (close_ms if d.get("outside_distance_atr") is not None and _f(d.get("outside_distance_atr")) >= 2.00 else None),
            "last_new_extreme_ms": close_ms,
            "accepted_outside_close_count": 0,
            "current_outside_distance": (
                max(0.0, close - hi)
                if direction == "BULLISH"
                else max(0.0, lo - close)
            ),
            "current_outside_distance_atr": d.get("outside_distance_atr"),
            "retracement_from_max": 0.0,
            "retracement_from_max_atr": 0.0,
            "bars_since_start": 0,
            "bars_since_acceptance": None,
            "bars_since_last_new_extreme": 0,
            "distribution_maturity": "CANDIDATE",
            "distribution_maturity_before_end": None,
        }

    def update_distribution_maturity(episode: Dict[str, Any]) -> None:
        """Assign descriptive Distribution maturity; no phase/execution authority."""
        accepted = bool(episode.get("acceptance_confirmed"))
        max_progress = _f(episode.get("max_outside_distance_atr"))

        if not accepted:
            maturity = "CANDIDATE"
        elif max_progress >= cfg.distribution_extended_atr:
            maturity = "EXTENDED"
        elif max_progress >= cfg.distribution_mature_atr:
            maturity = "MATURE"
        elif max_progress >= cfg.distribution_developing_atr:
            maturity = "DEVELOPING"
        else:
            maturity = "ACCEPTED"

        episode["distribution_maturity"] = maturity

    # ------------------------------------------------------------
    # Process each newly completed M15 candle exactly once.
    # ------------------------------------------------------------
    new_bars = []

    for b in m15:
        close_ms = (
            _i(b["t_open_ms"])
            + M15_MS
        )

        if close_ms <= last_processed:
            continue

        new_bars.append(b)

    for b in new_bars:
        close_ms = (
            _i(b["t_open_ms"])
            + M15_MS
        )

        close = _f(b["c"])
        rel = relation(close)

        # Causal ATR belonging to this completed M15 candle.
        absolute_idx = next(
            (
                i
                for i, x in enumerate(m15)
                if (
                    _i(x.get("t_open_ms"))
                    + M15_MS
                    == close_ms
                )
            ),
            None,
        )

        if absolute_idx is not None:
            event_atr = atr(
                m15[:absolute_idx + 1],
                cfg.m15_atr_period,
            )
        else:
            event_atr = 0.0

        if active is None:
            active = start_episode(
                close_ms,
                rel,
            )

            last_processed = close_ms
            continue

        expected = (
            "ABOVE"
            if active.get("direction") == "BULLISH"
            else "BELOW"
        )

        active["duration_bars"] = (
            _i(active.get("duration_bars"))
            + 1
        )

        if rel == expected:
            active["outside_close_count"] = (
                _i(
                    active.get(
                        "outside_close_count"
                    )
                )
                + 1
            )

            active[
                "consecutive_outside_closes"
            ] = (
                _i(
                    active.get(
                        "consecutive_outside_closes"
                    )
                )
                + 1
            )

            active[
                "max_consecutive_outside_closes"
            ] = max(
                _i(
                    active.get(
                        "max_consecutive_outside_closes"
                    )
                ),
                _i(
                    active.get(
                        "consecutive_outside_closes"
                    )
                ),
            )

            active["last_outside_ms"] = (
                close_ms
            )

            # Distance of this close beyond the frozen boundary.
            outside_distance = (
                max(0.0, close - hi)
                if active.get("direction") == "BULLISH"
                else max(0.0, lo - close)
            )

            outside_distance_atr = (
                outside_distance / event_atr
                if event_atr > 0
                else None
            )
            active["current_outside_distance"] = (
                outside_distance
            )
            active["current_outside_distance_atr"] = (
                outside_distance_atr
            )

            # Causal threshold timestamps are immutable once reached.
            if outside_distance_atr is not None:
                if outside_distance_atr >= 0.50 and not active.get("reached_050_ms"):
                    active["reached_050_ms"] = close_ms
                if outside_distance_atr >= 1.00 and not active.get("reached_100_ms"):
                    active["reached_100_ms"] = close_ms
                if outside_distance_atr >= 1.50 and not active.get("reached_150_ms"):
                    active["reached_150_ms"] = close_ms
                if outside_distance_atr >= 2.00 and not active.get("reached_200_ms"):
                    active["reached_200_ms"] = close_ms

            old_max = _f(
                active.get("max_outside_distance")
            )

            if outside_distance > old_max:
                active["max_outside_distance"] = (
                    outside_distance
                )
                active[
                    "max_outside_distance_atr"
                ] = outside_distance_atr
                active["max_outside_ms"] = close_ms
                active["last_new_extreme_ms"] = close_ms

            max_distance = _f(active.get("max_outside_distance"))
            retracement = max(0.0, max_distance - outside_distance)
            active["retracement_from_max"] = retracement
            active["retracement_from_max_atr"] = (
                retracement / event_atr if event_atr > 0 else None
            )
            active["bars_since_start"] = max(
                0, int((close_ms - _i(active.get("started_ms"))) / M15_MS)
            )
            last_extreme_ms = _i(active.get("last_new_extreme_ms"))
            active["bars_since_last_new_extreme"] = (
                max(0, int((close_ms - last_extreme_ms) / M15_MS))
                if last_extreme_ms > 0 else None
            )

            if not active.get(
                "acceptance_confirmed"
            ):
                active[
                    "acceptance_confirmed"
                ] = True

                active["acceptance_ms"] = (
                    close_ms
                )

                active["acceptance_bars"] = int(
                    (
                        close_ms
                        - _i(
                            active.get(
                                "initiation_ms"
                            )
                        )
                    )
                    / M15_MS
                )

            if active.get(
                "acceptance_confirmed"
            ):
                active["accepted_duration_bars"] = (
                    _i(
                        active.get(
                            "accepted_duration_bars"
                        )
                    )
                    + 1
                )
                active["accepted_outside_close_count"] = (
                    _i(active.get("accepted_outside_close_count")) + 1
                )
                acceptance_ms = _i(active.get("acceptance_ms"))
                active["bars_since_acceptance"] = (
                    max(0, int((close_ms - acceptance_ms) / M15_MS))
                    if acceptance_ms > 0 else None
                )

            update_distribution_maturity(active)

            active["status"] = (
                "OUTSIDE_ACCEPTED"
                if active.get(
                    "acceptance_confirmed"
                )
                else "OUTSIDE_CANDIDATE"
            )

            last_processed = close_ms
            continue

        # --------------------------------------------------------
        # Any completed close no longer on the episode's outside
        # side terminates this episode.
        #
        # This includes a close inside the frozen range or through
        # the opposite boundary.
        # --------------------------------------------------------
        active["returned_inside"] = True
        active["returned_inside_ms"] = (
            close_ms
        )
        active["ended_ms"] = close_ms

        direction = str(
            active.get("direction") or ""
        ).upper()

        if rel == "INSIDE":
            termination_relation = (
                "RETURNED_INSIDE_RANGE"
            )
            termination_distance = 0.0

        elif (
            direction == "BULLISH"
            and rel == "BELOW"
        ):
            termination_relation = (
                "CROSSED_OPPOSITE_BOUNDARY"
            )
            termination_distance = max(
                0.0,
                lo - close,
            )

        elif (
            direction == "BEARISH"
            and rel == "ABOVE"
        ):
            termination_relation = (
                "CROSSED_OPPOSITE_BOUNDARY"
            )
            termination_distance = max(
                0.0,
                close - hi,
            )

        else:
            termination_relation = (
                "LEFT_OUTSIDE_SIDE"
            )
            termination_distance = 0.0

        active["termination_relation"] = (
            termination_relation
        )
        active["termination_close"] = close
        active[
            "termination_distance_atr"
        ] = (
            termination_distance / event_atr
            if event_atr > 0
            else None
        )

        active["distribution_maturity_before_end"] = active.get(
            "distribution_maturity"
        )
        active["distribution_maturity"] = (
            "COMPLETE" if active.get("acceptance_confirmed") else "FAILED"
        )

        active["status"] = (
            "ACCEPTED_THEN_RETURNED"
            if active.get(
                "acceptance_confirmed"
            )
            else "FAILED_BREAK"
        )

        completed.append(
            dict(active)
        )

        if len(completed) > max_completed_history:
            completed = completed[
                -max_completed_history:
            ]

        active = None

        # Same completed candle may legitimately initiate a new
        # opposite-direction departure.
        active = start_episode(
            close_ms,
            rel,
        )

        last_processed = close_ms

    state = {
        "generation_id": gid,
        "last_processed_m15_close_ms":
            last_processed,
        "active": (
            dict(active)
            if active is not None
            else None
        ),
        "completed": completed,
    }

    episodes = (
        list(completed)
        + (
            [dict(active)]
            if active is not None
            else []
        )
    )

    accepted = [
        e
        for e in episodes
        if e.get("acceptance_confirmed")
    ]

    failed = [
        e
        for e in completed
        if e.get("status") == "FAILED_BREAK"
    ]

    accepted_returned = [
        e
        for e in completed
        if e.get("status")
        == "ACCEPTED_THEN_RETURNED"
    ]

    active_maturity = (
        active.get("distribution_maturity")
        if active is not None
        else None
    )

    maturity_view = (
        {
            "state": active.get("distribution_maturity"),
            "episode_id": active.get("episode_id"),
            "direction": active.get("direction"),
            "acceptance_confirmed": active.get("acceptance_confirmed"),
            "current_outside_distance_atr": active.get("current_outside_distance_atr"),
            "max_outside_distance_atr": active.get("max_outside_distance_atr"),
            "bars_since_start": active.get("bars_since_start"),
            "bars_since_acceptance": active.get("bars_since_acceptance"),
            "accepted_outside_close_count": active.get("accepted_outside_close_count"),
            "bars_since_last_new_extreme": active.get("bars_since_last_new_extreme"),
            "retracement_from_max_atr": active.get("retracement_from_max_atr"),
            "reached_050_ms": active.get("reached_050_ms"),
            "reached_100_ms": active.get("reached_100_ms"),
            "reached_150_ms": active.get("reached_150_ms"),
            "reached_200_ms": active.get("reached_200_ms"),
        }
        if active is not None
        else None
    )

    return {
        "available": True,
        "range_high": hi,
        "range_low": lo,
        "generation_id": gid,

        "episodes": episodes,

        "episode_count": len(episodes),
        "completed_episode_count":
            len(completed),
        "accepted_episode_count":
            len(accepted),
        "failed_break_count":
            len(failed),
        "accepted_then_returned_count":
            len(accepted_returned),

        "latest_episode": (
            episodes[-1]
            if episodes
            else None
        ),

        "active_episode": (
            dict(active)
            if active is not None
            else None
        ),

        "active_distribution_maturity": active_maturity,
        "distribution_maturity": maturity_view,

        "state": state,
    }


def measure_manipulation_distribution_links(
    m15: Sequence[Mapping[str, Any]],
    h1_range: Mapping[str, Any],
    transition: Mapping[str, Any],
    outside_episodes: Mapping[str, Any],
    *,
    generation_id: Optional[str] = None,
    generation_start_ms: Optional[int] = None,
    previous_state: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """Causally link qualified Manipulation events to Distribution episodes.

    Task-2 shadow analytics only.  A Manipulation event is frozen only after a
    qualified sweep has a causal reclaim.  Distribution remains independently
    owned by ``measure_outside_episodes``; this function references those
    episode identities and never changes their lifecycle or maturity.
    """
    gid = str(generation_id or h1_range.get("generation_id") or "").strip()
    lifecycle_start_ms = _i(generation_start_ms)

    if not gid or not h1_range.get("available"):
        return {
            "available": False,
            "reason": "ACTIVE_GENERATION_REQUIRED",
            "generation_id": gid,
            "events": [],
            "links": [],
            "state": {
                "generation_id": gid,
                "events": [],
            },
        }

    restored_events: List[Dict[str, Any]] = []
    if isinstance(previous_state, Mapping):
        ls = previous_state.get("lifecycle_state")
        if isinstance(ls, Mapping):
            old = ls.get("manipulation_distribution")
            if (
                isinstance(old, Mapping)
                and str(old.get("generation_id") or "").strip() == gid
            ):
                restored_events = [
                    dict(x)
                    for x in (old.get("events") or [])
                    if isinstance(x, Mapping)
                ]

    by_id: Dict[str, Dict[str, Any]] = {
        str(x.get("manipulation_id")): dict(x)
        for x in restored_events
        if x.get("manipulation_id")
    }

    tr_events = (
        list(transition.get("events") or [])
        if isinstance(transition, Mapping)
        else []
    )

    # A two-sided sweep on the same completed candle is ambiguous.  Preserve
    # the factual events, but do not treat either side as a clean M->D source.
    sides_by_ms: Dict[int, set] = {}
    for e in tr_events:
        if not isinstance(e, Mapping):
            continue
        if not e.get("manipulation_candidate"):
            continue
        sweep_ms = _i(e.get("sweep_ms"))
        if sweep_ms <= lifecycle_start_ms:
            continue
        sides_by_ms.setdefault(sweep_ms, set()).add(
            str(e.get("side") or "").upper()
        )

    close_by_ms = {
        _i(b.get("t_open_ms")) + M15_MS: _f(b.get("c"))
        for b in m15
    }
    hi = _f(h1_range.get("range_high"))
    lo = _f(h1_range.get("range_low"))

    for e in tr_events:
        if not isinstance(e, Mapping) or not e.get("manipulation_candidate"):
            continue

        sweep_ms = _i(e.get("sweep_ms"))
        reclaim_ms = _i(e.get("reclaim_ms"))
        if sweep_ms <= lifecycle_start_ms or reclaim_ms <= 0:
            continue

        side = str(e.get("side") or "").upper()
        direction = str(e.get("direction") or "").upper()
        mid = f"{gid}:{side}:{sweep_ms}"
        ambiguous = len(sides_by_ms.get(sweep_ms, set())) > 1

        # Freeze immutable event facts at first confirmation.  Later causal
        # evaluations may rediscover the event but must not rewrite history.
        if mid not in by_id:
            depth = _f(e.get("depth"))
            sweep_price = (
                hi + depth if side == "HIGH"
                else lo - depth if side == "LOW"
                else None
            )
            by_id[mid] = {
                "manipulation_id": mid,
                "generation_id": gid,
                "status": "M_AMBIGUOUS" if ambiguous else "M_CONFIRMED_NO_D",
                "ambiguity": "TWO_SIDED" if ambiguous else None,
                "sweep_side": side,
                "sweep_ms": sweep_ms,
                "sweep_price": sweep_price,
                "sweep_depth": e.get("depth"),
                "sweep_depth_atr": e.get("depth_atr"),
                "event_atr": e.get("event_atr"),
                "reclaim_ms": reclaim_ms,
                "reclaim_bars": e.get("reclaim_bars"),
                "reclaim_type": e.get("reclaim_type"),
                "reclaim_close": close_by_ms.get(reclaim_ms),
                "reclaim_close_location": e.get("reclaim_close_location"),
                "implied_direction": direction,
            }
        elif ambiguous:
            by_id[mid]["ambiguity"] = "TWO_SIDED"
            by_id[mid]["status"] = "M_AMBIGUOUS"

    events = sorted(
        by_id.values(),
        key=lambda x: (_i(x.get("sweep_ms")), str(x.get("manipulation_id") or "")),
    )[-32:]

    episodes = (
        list(outside_episodes.get("episodes") or [])
        if isinstance(outside_episodes, Mapping)
        else []
    )
    episodes = [
        e for e in episodes
        if isinstance(e, Mapping)
        and str(e.get("generation_id") or "").strip() == gid
    ]

    links: List[Dict[str, Any]] = []
    linked_ids = set()

    for ep in episodes:
        started_ms = _i(ep.get("started_ms"))
        if started_ms <= lifecycle_start_ms:
            continue

        prior = [
            m for m in events
            if _i(m.get("reclaim_ms")) > 0
            and _i(m.get("reclaim_ms")) <= started_ms
        ]
        source = prior[-1] if prior else None

        ep_direction = str(ep.get("direction") or "").upper()
        accepted = bool(ep.get("acceptance_confirmed"))
        ep_status = str(ep.get("status") or "")

        if source is None:
            provenance = "DIRECT_DISTRIBUTION"
            link_status = "DIRECT_DISTRIBUTION"
            direction_relation = None
        elif source.get("ambiguity") == "TWO_SIDED":
            provenance = "AMBIGUOUS_MANIPULATION"
            link_status = "M_TO_D_AMBIGUOUS"
            direction_relation = "AMBIGUOUS"
        else:
            provenance = "MANIPULATION_LINKED"
            implied = str(source.get("implied_direction") or "").upper()
            direction_relation = (
                "ALIGNED" if implied == ep_direction else "OPPOSITE"
            )
            if direction_relation == "OPPOSITE" and accepted:
                link_status = "M_TO_D_OPPOSITE"
            elif ep_status == "FAILED_BREAK":
                link_status = "M_TO_D_FAILED_BREAK"
            elif accepted:
                link_status = "M_TO_D_CONFIRMED"
            else:
                link_status = "M_TO_D_CANDIDATE"

        reclaim_ms = _i(source.get("reclaim_ms")) if source else 0
        sweep_ms = _i(source.get("sweep_ms")) if source else 0
        acceptance_ms = _i(ep.get("acceptance_ms"))

        link = {
            "generation_id": gid,
            "provenance": provenance,
            "link_status": link_status,
            "manipulation_id": source.get("manipulation_id") if source else None,
            "distribution_episode_id": ep.get("episode_id"),
            "implied_direction": source.get("implied_direction") if source else None,
            "distribution_direction": ep_direction,
            "direction_relation": direction_relation,
            "sweep_side": source.get("sweep_side") if source else None,
            "sweep_ms": sweep_ms or None,
            "reclaim_ms": reclaim_ms or None,
            "distribution_started_ms": started_ms,
            "distribution_acceptance_ms": acceptance_ms or None,
            "bars_sweep_to_reclaim": (
                int((reclaim_ms - sweep_ms) / M15_MS)
                if sweep_ms > 0 and reclaim_ms >= sweep_ms else None
            ),
            "bars_reclaim_to_distribution_start": (
                int((started_ms - reclaim_ms) / M15_MS)
                if reclaim_ms > 0 and started_ms >= reclaim_ms else None
            ),
            "bars_reclaim_to_distribution_acceptance": (
                int((acceptance_ms - reclaim_ms) / M15_MS)
                if reclaim_ms > 0 and acceptance_ms >= reclaim_ms else None
            ),
            "bars_sweep_to_distribution_acceptance": (
                int((acceptance_ms - sweep_ms) / M15_MS)
                if sweep_ms > 0 and acceptance_ms >= sweep_ms else None
            ),
            "sweep_to_reclaim_ms": (
                reclaim_ms - sweep_ms
                if sweep_ms > 0 and reclaim_ms >= sweep_ms else None
            ),
            "reclaim_to_distribution_start_ms": (
                started_ms - reclaim_ms
                if reclaim_ms > 0 and started_ms >= reclaim_ms else None
            ),
            "reclaim_to_distribution_acceptance_ms": (
                acceptance_ms - reclaim_ms
                if reclaim_ms > 0 and acceptance_ms >= reclaim_ms else None
            ),
            # Distribution lifecycle remains owned by Task 1.
            "distribution_status": ep.get("status"),
            "distribution_maturity": ep.get("distribution_maturity"),
            "distribution_maturity_before_end": ep.get("distribution_maturity_before_end"),
            "max_outside_distance_atr": ep.get("max_outside_distance_atr"),
            "reached_050_ms": ep.get("reached_050_ms"),
            "reached_100_ms": ep.get("reached_100_ms"),
            "reached_150_ms": ep.get("reached_150_ms"),
            "reached_200_ms": ep.get("reached_200_ms"),
            "execution_authority": False,
        }
        links.append(link)
        if source is not None:
            linked_ids.add(str(source.get("manipulation_id") or ""))

    # Event status is a compact current view.  Immutable sweep/reclaim facts
    # remain frozen; status may advance as later Distribution evidence arrives.
    link_by_mid: Dict[str, List[Dict[str, Any]]] = {}
    for link in links:
        mid = str(link.get("manipulation_id") or "")
        if mid:
            link_by_mid.setdefault(mid, []).append(link)

    for m in events:
        if m.get("ambiguity") == "TWO_SIDED":
            m["status"] = "M_AMBIGUOUS"
            continue
        ml = link_by_mid.get(str(m.get("manipulation_id") or ""), [])
        if not ml:
            m["status"] = "M_CONFIRMED_NO_D"
        elif any(x.get("link_status") == "M_TO_D_CONFIRMED" for x in ml):
            m["status"] = "M_TO_D_CONFIRMED"
        elif any(x.get("link_status") == "M_TO_D_OPPOSITE" for x in ml):
            m["status"] = "M_TO_D_OPPOSITE"
        elif any(x.get("link_status") == "M_TO_D_FAILED_BREAK" for x in ml):
            m["status"] = "M_TO_D_FAILED_BREAK"
        else:
            m["status"] = "M_TO_D_CANDIDATE"

    latest_link = links[-1] if links else None
    latest_event = events[-1] if events else None

    return {
        "available": True,
        "generation_id": gid,
        "events": events,
        "event_count": len(events),
        "latest_event": latest_event,
        "links": links,
        "link_count": len(links),
        "latest_link": latest_link,
        "direct_distribution_count": sum(
            1 for x in links if x.get("provenance") == "DIRECT_DISTRIBUTION"
        ),
        "aligned_confirmed_count": sum(
            1 for x in links
            if x.get("link_status") == "M_TO_D_CONFIRMED"
            and x.get("direction_relation") == "ALIGNED"
        ),
        "opposite_confirmed_count": sum(
            1 for x in links if x.get("link_status") == "M_TO_D_OPPOSITE"
        ),
        "failed_break_count": sum(
            1 for x in links if x.get("link_status") == "M_TO_D_FAILED_BREAK"
        ),
        "ambiguous_link_count": sum(
            1 for x in links if x.get("link_status") == "M_TO_D_AMBIGUOUS"
        ),
        "execution_authority": False,
        "state": {
            "generation_id": gid,
            "events": [dict(x) for x in events],
        },
    }


def _generation_id(symbol: str, r: Mapping[str, Any]) -> Optional[str]:
    if not r.get("available"):
        return None
    raw = f"{symbol.upper()}:{_i(r.get('window_start_ms'))}:{_f(r.get('range_low')):.10f}:{_f(r.get('range_high')):.10f}"
    return hashlib.sha1(raw.encode()).hexdigest()[:16]


def _active_generation_from_state(
    previous_state: Optional[Mapping[str, Any]],
) -> Optional[Dict[str, Any]]:
    """Return a validated frozen ACTIVE generation from previous_state.

    Raw discovery candidates are deliberately NOT treated as generations.
    Only an explicitly ACTIVE generation owns/fixes structural boundaries.
    """
    if not isinstance(previous_state, Mapping):
        return None

    g = previous_state.get("generation")
    if not isinstance(g, Mapping):
        return None

    if str(g.get("status") or "").upper() != "ACTIVE":
        return None

    gid = str(g.get("id") or "").strip()
    hi = _f(g.get("range_high"))
    lo = _f(g.get("range_low"))

    if not gid or hi <= lo:
        return None

    mid = _f(g.get("range_mid"), (hi + lo) / 2.0)

    return {
        "id": gid,
        "status": "ACTIVE",
        "range_high": hi,
        "range_low": lo,
        "range_mid": mid,
        "started_ms": _i(g.get("started_ms")),
        "confirmed_ms": _i(g.get("confirmed_ms")),
        "source_window_start_ms": _i(g.get("source_window_start_ms")),
        "source_window_end_ms": _i(g.get("source_window_end_ms")),
    }


def _frozen_range_from_generation(
    generation: Mapping[str, Any],
    discovery: Mapping[str, Any],
) -> Dict[str, Any]:
    """Build the structural range used by transition measurements.

    Preserve current causal ATR/context measurements where available, while
    replacing structural boundaries with the generation-owned frozen values.
    """
    hi = _f(generation.get("range_high"))
    lo = _f(generation.get("range_low"))

    out = dict(discovery) if isinstance(discovery, Mapping) else {}
    out.update({
        "available": True,
        "range_high": hi,
        "range_low": lo,
        "range_mid": _f(generation.get("range_mid"), (hi + lo) / 2.0),
        "window_start_ms": _i(generation.get("source_window_start_ms")),
        "window_end_ms": _i(generation.get("source_window_end_ms")),
        "range_width": max(0.0, hi - lo),
        "range_source": "FROZEN_ACTIVE_GENERATION",
        "generation_id": str(generation.get("id") or ""),
    })

    a = _f(out.get("atr"))
    out["range_width_atr"] = (hi - lo) / a if a > 0 else None

    return out


def _new_active_generation(
    symbol: str,
    discovery: Mapping[str, Any],
    *,
    confirmed_ms: int,
) -> Optional[Dict[str, Any]]:
    """Create an immutable generation from a QUALIFIED accumulation.

    This helper does not decide qualification.  The classifier/state machine
    must explicitly call it only after Accumulation qualification succeeds.
    """
    gid = _generation_id(symbol, discovery)
    if not gid or not discovery.get("available"):
        return None

    hi = _f(discovery.get("range_high"))
    lo = _f(discovery.get("range_low"))
    if hi <= lo:
        return None

    return {
        "id": gid,
        "status": "ACTIVE",
        "range_high": hi,
        "range_low": lo,
        "range_mid": (hi + lo) / 2.0,
        "started_ms": _i(discovery.get("window_start_ms")),
        "confirmed_ms": int(confirmed_ms),
        "source_window_start_ms": _i(discovery.get("window_start_ms")),
        "source_window_end_ms": _i(discovery.get("window_end_ms")),
    }


def _accumulation_qualifies(
    h: Mapping[str, Any],
    cfg: PO3Config,
) -> bool:
    """Accumulation V1 discovery qualification.

    The H1 lookback is a minimum discovery/confirmation observation window,
    not the lifetime of an ACTIVE generation.

    Compression remains diagnostic evidence only.
    """
    if not h.get("available"):
        return False

    return (
        _i(h.get("window_bars")) >= cfg.h1_lookback_bars
        and _f(h.get("range_width_atr"), 999.0)
            <= cfg.accumulation_max_width_atr
        and _f(h.get("overlap_ratio"))
            >= cfg.accumulation_min_overlap_ratio
        and _f(h.get("efficiency_ratio"), 999.0)
            <= cfg.accumulation_max_efficiency_ratio
        and _i(h.get("boundary_revisits_high")) >= 1
        and _i(h.get("boundary_revisits_low")) >= 1
    )


def _classify(
    measurements: Mapping[str, Any],
    cfg: PO3Config,
    *,
    active_generation: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """Execution-neutral staged PO3 classifier.

    V1 grants semantic authority only to Accumulation.  Manipulation and
    Distribution switches intentionally remain independent and OFF.
    """
    h = measurements["h1_consolidation"]

    # An already ACTIVE generation remains Accumulation until a later,
    # separately validated phase-transition classifier changes it.
    #
    # Crucially, a rolling H1 window does NOT have to re-qualify every hour.
    if active_generation is not None:
        return {
            "name": "ACCUMULATION",
            "direction": "NEUTRAL",
            "candidate": False,
            "reason_codes": [
                "PO3_ACTIVE_ACCUMULATION_GENERATION_PRESERVED"
            ],
        }

    if not cfg.enable_accumulation_classifier:
        return {
            "name": "UNCLASSIFIED",
            "direction": "NEUTRAL",
            "candidate": True,
            "reason_codes": [
                "PO3_ACCUMULATION_CLASSIFIER_DISABLED"
            ],
        }

    if not h.get("available"):
        return {
            "name": "UNCLASSIFIED",
            "direction": "NEUTRAL",
            "candidate": True,
            "reason_codes": ["PO3_H1_UNAVAILABLE"],
        }

    if not _accumulation_qualifies(h, cfg):
        return {
            "name": "UNCLASSIFIED",
            "direction": "NEUTRAL",
            "candidate": True,
            "reason_codes": [
                "PO3_ACCUMULATION_NOT_QUALIFIED"
            ],
        }

    # M/D are deliberately NOT evaluated here yet.
    return {
        "name": "ACCUMULATION",
        "direction": "NEUTRAL",
        "candidate": False,
        "reason_codes": [
            "PO3_ACCUMULATION_V1_QUALIFIED"
        ],
    }



def measure_po3_shadow_semantics(
    generation: Mapping[str, Any],
    outside_episodes: Mapping[str, Any],
    manipulation_distribution: Mapping[str, Any],
    *,
    previous_state: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """Interpret validated PO3 lifecycle evidence as a causal shadow semantic state.

    Task-3 is deliberately additive.  It consumes Task-1 Distribution episodes
    and Task-2 Manipulation->Distribution links exactly as produced; it does not
    recalculate, mutate, qualify, or grant authority to either lifecycle.
    """
    gid = str(generation.get("id") or "").strip()
    active = str(generation.get("status") or "").upper() == "ACTIVE" and bool(gid)

    if not active:
        return {
            "available": False,
            "reason": "ACTIVE_GENERATION_REQUIRED",
            "generation_id": gid or None,
            "state": None,
            "substate": None,
            "provenance": None,
            "direction": "NEUTRAL",
            "evidence_ms": None,
            "manipulation_id": None,
            "distribution_episode_id": None,
            "distribution_maturity": None,
            "link_status": None,
            "history": [],
            "execution_authority": False,
            "state_store": {"generation_id": gid, "history": []},
        }

    events = [
        dict(x) for x in (manipulation_distribution.get("events") or [])
        if isinstance(x, Mapping) and str(x.get("generation_id") or "").strip() == gid
    ]
    links = [
        dict(x) for x in (manipulation_distribution.get("links") or [])
        if isinstance(x, Mapping) and str(x.get("generation_id") or "").strip() == gid
    ]
    episodes = [
        dict(x) for x in (outside_episodes.get("episodes") or [])
        if isinstance(x, Mapping) and str(x.get("generation_id") or "").strip() == gid
    ]

    # Restore semantic history only for this exact frozen generation.
    history: List[Dict[str, Any]] = []
    if isinstance(previous_state, Mapping):
        ls = previous_state.get("lifecycle_state")
        if isinstance(ls, Mapping):
            old = ls.get("shadow_semantics")
            if (
                isinstance(old, Mapping)
                and str(old.get("generation_id") or "").strip() == gid
            ):
                history = [
                    dict(x) for x in (old.get("history") or [])
                    if isinstance(x, Mapping)
                ][-64:]

    latest_m = max(events, key=lambda x: (_i(x.get("reclaim_ms")), _i(x.get("sweep_ms"))), default=None)
    latest_d = max(episodes, key=lambda x: _i(x.get("started_ms")), default=None)

    m_ms = _i((latest_m or {}).get("reclaim_ms"))
    d_ms = _i((latest_d or {}).get("started_ms"))

    # Semantic state is selected by the latest causally established lifecycle
    # evidence.  A newly confirmed M after an older D therefore becomes the
    # current semantic observation; a subsequent D becomes DISTRIBUTION.
    if latest_d is not None and d_ms >= m_ms and d_ms > 0:
        state = "DISTRIBUTION"
        episode_id = latest_d.get("episode_id")
        link = next(
            (x for x in reversed(links) if x.get("distribution_episode_id") == episode_id),
            None,
        )
        link_status = str((link or {}).get("link_status") or "DIRECT_DISTRIBUTION")
        provenance = (
            "A_D" if link_status == "DIRECT_DISTRIBUTION" else "A_M_D"
        )
        substate = link_status
        direction = str(latest_d.get("direction") or "NEUTRAL").upper()
        evidence_ms = d_ms
        manipulation_id = (link or {}).get("manipulation_id")
        distribution_episode_id = episode_id
        distribution_maturity = latest_d.get("distribution_maturity")
    elif latest_m is not None and m_ms > 0:
        state = "MANIPULATION"
        provenance = "A_M"
        substate = str(latest_m.get("status") or "M_CONFIRMED_NO_D")
        direction = str(latest_m.get("implied_direction") or "NEUTRAL").upper()
        evidence_ms = m_ms
        manipulation_id = latest_m.get("manipulation_id")
        distribution_episode_id = None
        distribution_maturity = None
        link_status = None
    else:
        state = "ACCUMULATION"
        provenance = "A_ONLY"
        substate = "ACTIVE_ACCUMULATION"
        direction = "NEUTRAL"
        evidence_ms = _i(generation.get("confirmed_ms")) or None
        manipulation_id = None
        distribution_episode_id = None
        distribution_maturity = None
        link_status = None

    snapshot = {
        "state": state,
        "substate": substate,
        "provenance": provenance,
        "direction": direction,
        "evidence_ms": evidence_ms,
        "manipulation_id": manipulation_id,
        "distribution_episode_id": distribution_episode_id,
        "distribution_maturity": distribution_maturity,
        "link_status": link_status,
    }

    # Append only genuine semantic changes.  Existing rows are immutable.
    last = history[-1] if history else None
    identity = (
        state, substate, provenance, direction, evidence_ms,
        manipulation_id, distribution_episode_id, distribution_maturity, link_status,
    )
    last_identity = (
        (last or {}).get("state"), (last or {}).get("substate"),
        (last or {}).get("provenance"), (last or {}).get("direction"),
        (last or {}).get("evidence_ms"), (last or {}).get("manipulation_id"),
        (last or {}).get("distribution_episode_id"),
        (last or {}).get("distribution_maturity"), (last or {}).get("link_status"),
    )
    if identity != last_identity:
        history.append(dict(snapshot))
        history = history[-64:]

    return {
        "available": True,
        "generation_id": gid,
        **snapshot,
        "history": history,
        "execution_authority": False,
        "state_store": {"generation_id": gid, "history": history},
    }

def evaluate_po3_market_phase(*, symbol: str, h1_bars: Sequence[Mapping[str, Any]],
                              m15_bars: Sequence[Mapping[str, Any]], as_of_ms: int,
                              previous_state: Optional[Mapping[str, Any]] = None,
                              config: Optional[PO3Config] = None) -> Dict[str, Any]:
    """Canonical strategy-neutral causal PO3 evaluation."""
    cfg = config or PO3Config()
    h1_all = completed_bars(h1_bars, as_of_ms=as_of_ms, tf_ms=H1_MS)
    m15 = completed_bars(m15_bars, as_of_ms=as_of_ms, tf_ms=M15_MS)

    # Prevent transition leakage into the structural range.  The H1 range must
    # exist BEFORE the recent M15 transition-evidence horizon; otherwise a
    # sweep/displacement candle can enlarge the range that it is supposed to
    # sweep/break.  Stateful production use will normally carry a frozen
    # generation range in previous_state; this causal discovery fallback uses
    # only H1 bars closed before the M15 transition window begins.
    transition_tail = m15[-cfg.m15_transition_lookback_bars:]
    structural_cutoff_ms = (
        transition_tail[0]["t_open_ms"]
        if transition_tail
        else int(as_of_ms)
    )
    h1 = [b for b in h1_all if b["t_open_ms"] + H1_MS <= structural_cutoff_ms]

    # Discovery measurements remain free to evolve until Accumulation is
    # actually qualified.  A raw H1 window is a candidate, NOT a generation.
    discovery_h = measure_h1_consolidation(h1, cfg)
    candidate_id = _generation_id(symbol, discovery_h)

    # Once a generation is ACTIVE, its structural boundaries are immutable.
    # Subsequent M15 transition/distribution measurements must use those
    # frozen boundaries rather than a newly rolling H1 discovery window.
    active_generation = _active_generation_from_state(previous_state)

    if active_generation is not None:
        structural_h = _frozen_range_from_generation(
            active_generation,
            discovery_h,
        )
        generation = dict(active_generation)
        generation["boundary_source"] = "PREVIOUS_STATE_FROZEN"
    else:
        structural_h = discovery_h
        generation = {
            "id": None,
            "status": "DISCOVERY",
            "candidate_id": candidate_id,
            "range_high": None,
            "range_low": None,
            "range_mid": None,
            "started_ms": None,
            "confirmed_ms": None,
            "source_window_start_ms": None,
            "source_window_end_ms": None,
            "boundary_source": "UNFROZEN_DISCOVERY",
        }

    tr = measure_m15_transition(m15, structural_h, cfg)
    dist = measure_distribution(m15, structural_h, cfg)
    outside = measure_outside_episodes(
        m15,
        structural_h,
        dist,
        cfg,
        generation_id=(
            generation.get("id")
            if generation.get("status") == "ACTIVE"
            else None
        ),
        generation_start_ms=(
            generation.get("confirmed_ms")
            if generation.get("status") == "ACTIVE"
            else None
        ),
        previous_state=previous_state,
    )
    md_links = measure_manipulation_distribution_links(
        m15,
        structural_h,
        tr,
        outside,
        generation_id=(
            generation.get("id")
            if generation.get("status") == "ACTIVE"
            else None
        ),
        generation_start_ms=(
            generation.get("confirmed_ms")
            if generation.get("status") == "ACTIVE"
            else None
        ),
        previous_state=previous_state,
    )
    shadow_semantics = measure_po3_shadow_semantics(
        generation,
        outside,
        md_links,
        previous_state=previous_state,
    )

    fvgs = detect_fvgs(
        m15[-max(60, cfg.m15_transition_lookback_bars + 3):],
        cfg,
    )

    measurements = {
        "h1_consolidation": discovery_h,
        "structural_range": structural_h,
        "m15_transition": tr,
        "distribution": dist,
        "outside_episodes": outside,
        "manipulation_distribution": md_links,
        "shadow_semantics": shadow_semantics,
        "fvg": {
            "events": fvgs,
            "latest": fvgs[-1] if fvgs else None,
        },
    }

    # Stage-specific classifier remains execution-neutral.
    #
    # If discovery qualifies as Accumulation V1, promote exactly once into
    # an ACTIVE generation and freeze its structural boundaries.  Later
    # evaluations preserve that generation without requiring each rolling
    # 8-H1 discovery window to qualify again.
    phase = _classify(
        measurements,
        cfg,
        active_generation=active_generation,
    )

    if (
        active_generation is None
        and phase.get("name") == "ACCUMULATION"
        and cfg.enable_accumulation_classifier
    ):
        promoted = _new_active_generation(
            symbol,
            discovery_h,
            confirmed_ms=int(as_of_ms),
        )

        if promoted is not None:
            generation = promoted
            generation["boundary_source"] = (
                "QUALIFIED_ACCUMULATION_V1"
            )

            # Transition evidence for this evaluation must also reference
            # the newly frozen generation, not an independently drifting
            # discovery structure.
            structural_h = _frozen_range_from_generation(
                generation,
                discovery_h,
            )

            tr = measure_m15_transition(m15, structural_h, cfg)
            dist = measure_distribution(m15, structural_h, cfg)
            outside = measure_outside_episodes(
                m15,
                structural_h,
                dist,
                cfg,
                generation_id=generation.get("id"),
                generation_start_ms=(
                    generation.get("confirmed_ms")
                ),
                previous_state=None,
            )

            md_links = measure_manipulation_distribution_links(
                m15,
                structural_h,
                tr,
                outside,
                generation_id=generation.get("id"),
                generation_start_ms=generation.get("confirmed_ms"),
                previous_state=None,
            )
            shadow_semantics = measure_po3_shadow_semantics(
                generation,
                outside,
                md_links,
                previous_state=None,
            )

            measurements["structural_range"] = structural_h
            measurements["m15_transition"] = tr
            measurements["distribution"] = dist
            measurements["outside_episodes"] = outside
            measurements["manipulation_distribution"] = md_links
            measurements["shadow_semantics"] = shadow_semantics

    # ------------------------------------------------------------
    # Generation Lifecycle V1 — successor Accumulation SHADOW.
    #
    # Research-only / execution-neutral.
    #
    # A frozen ACTIVE generation remains the production owner here.
    # We merely measure whether the normal rolling Accumulation V1
    # has produced a causally independent successor after this
    # generation already achieved an accepted EXTENDED Distribution.
    #
    # IMPORTANT:
    #   * no generation replacement
    #   * no phase change
    #   * no Point-A / execution authority
    #   * no new accumulation thresholds
    # ------------------------------------------------------------
    successor_generation_shadow = {
        "schema_version": 1,
        "available": False,
        "qualified": False,
        "reason": "NO_ACTIVE_GENERATION",
        "execution_authority": False,
        "current_generation_id": (
            generation.get("id")
            if str(generation.get("status") or "").upper() == "ACTIVE"
            else None
        ),
        "distribution_extended_ms": None,
        "candidate_id": candidate_id,
        "candidate_window_start_ms": (
            discovery_h.get("window_start_ms")
            if isinstance(discovery_h, Mapping)
            else None
        ),
        "candidate_window_end_ms": (
            discovery_h.get("window_end_ms")
            if isinstance(discovery_h, Mapping)
            else None
        ),
        "candidate_accumulation_qualified": False,
        "candidate_full_post_extended": False,
        "successor_generation": None,
    }

    if (
        active_generation is not None
        and str(generation.get("status") or "").upper() == "ACTIVE"
    ):
        successor_generation_shadow["available"] = True

        # Find the immutable first causal point at which this generation
        # reached >= 2.00 ATR after Distribution acceptance.
        #
        # Search both completed and active episode state.  A Distribution
        # may later be COMPLETE, so its current maturity label must not be
        # used to erase the earlier EXTENDED landmark.
        _oe = measurements.get("outside_episodes") or {}
        _oe_state = (
            _oe.get("state")
            if isinstance(_oe, Mapping)
            else {}
        ) or {}

        _episodes = []

        _completed = _oe_state.get("completed") or []
        if isinstance(_completed, list):
            _episodes.extend(
                x for x in _completed
                if isinstance(x, Mapping)
            )

        _active_ep = _oe_state.get("active")
        if isinstance(_active_ep, Mapping):
            _episodes.append(_active_ep)

        _gid = str(generation.get("id") or "")
        _extended_candidates = []

        for _ep in _episodes:
            if str(_ep.get("generation_id") or "") != _gid:
                continue

            # EXTENDED requires acceptance.  reached_200_ms is the
            # immutable causal timestamp, not the later snapshot time.
            if not bool(_ep.get("acceptance_confirmed")):
                continue

            _r200 = _i(_ep.get("reached_200_ms"))
            _acceptance_ms = _i(_ep.get("acceptance_ms"))

            # EXTENDED first exists only when BOTH conditions are true:
            #   1. Distribution acceptance has been confirmed; and
            #   2. progress has reached >= 2.00 ATR.
            #
            # reached_200_ms may precede acceptance_ms, so using r200
            # alone would leak the eventual acceptance backward in time.
            if _r200 > 0 and _acceptance_ms > 0:
                _extended_candidates.append(
                    max(_r200, _acceptance_ms)
                )

        _d1_extended_ms = (
            min(_extended_candidates)
            if _extended_candidates
            else 0
        )

        successor_generation_shadow[
            "distribution_extended_ms"
        ] = (
            _d1_extended_ms
            if _d1_extended_ms > 0
            else None
        )

        _candidate_qualified = bool(
            cfg.enable_accumulation_classifier
            and _accumulation_qualifies(
                discovery_h,
                cfg,
            )
        )

        successor_generation_shadow[
            "candidate_accumulation_qualified"
        ] = _candidate_qualified

        _candidate_start_ms = _i(
            discovery_h.get("window_start_ms")
            if isinstance(discovery_h, Mapping)
            else None
        )

        _full_post_extended = bool(
            _d1_extended_ms > 0
            and _candidate_start_ms >= _d1_extended_ms
        )

        successor_generation_shadow[
            "candidate_full_post_extended"
        ] = _full_post_extended

        if _d1_extended_ms <= 0:
            successor_generation_shadow[
                "reason"
            ] = "CURRENT_GENERATION_NOT_EXTENDED"

        elif not _candidate_qualified:
            successor_generation_shadow[
                "reason"
            ] = "SUCCESSOR_ACCUMULATION_NOT_QUALIFIED"

        elif not _full_post_extended:
            successor_generation_shadow[
                "reason"
            ] = "SUCCESSOR_WINDOW_NOT_FULLY_POST_EXTENDED"

        else:
            _successor = _new_active_generation(
                symbol,
                discovery_h,
                confirmed_ms=int(as_of_ms),
            )

            if _successor is None:
                successor_generation_shadow[
                    "reason"
                ] = "SUCCESSOR_PROMOTION_BUILD_FAILED"

            else:
                _successor["boundary_source"] = (
                    "SUCCESSOR_ACCUMULATION_V1_SHADOW"
                )

                successor_generation_shadow[
                    "qualified"
                ] = True
                successor_generation_shadow[
                    "reason"
                ] = "SUCCESSOR_ACCUMULATION_QUALIFIED"
                successor_generation_shadow[
                    "successor_generation"
                ] = _successor

    measurements[
        "successor_generation_shadow"
    ] = successor_generation_shadow

    # Age is observational only.  It does not expire an Accumulation.
    #
    # age_h1_bars counts completed H1 bars whose close is after the
    # generation's structural start and no later than this causal as_of.
    if str(generation.get("status") or "").upper() == "ACTIVE":
        started_ms = _i(generation.get("started_ms"))

        generation["age_h1_bars"] = sum(
            1
            for b in h1_all
            if _i(b.get("t_open_ms")) + H1_MS > started_ms
        )
    else:
        generation["age_h1_bars"] = 0

    lifecycle_state = {
        "outside_episode": dict(
            (
                measurements.get(
                    "outside_episodes"
                )
                or {}
            ).get("state") or {}
        ),
        "manipulation_distribution": dict(
            (
                measurements.get(
                    "manipulation_distribution"
                )
                or {}
            ).get("state") or {}
        ),
        "shadow_semantics": dict(
            (
                measurements.get(
                    "shadow_semantics"
                )
                or {}
            ).get("state_store") or {}
        ),
    }

    return {
        "schema_version": SCHEMA_VERSION,
        "engine_role": "MARKET_INTELLIGENCE_ONLY",
        "execution_authority": False,
        "symbol": str(symbol or "").upper(),
        "as_of_ms": int(as_of_ms),
        "causality": {"h1_completed_bars": len(h1_all), "h1_structural_bars": len(h1),
                      "m15_completed_bars": len(m15), "structural_cutoff_ms": structural_cutoff_ms,
                      "h1_cutoff_rule_ms": H1_MS, "m15_cutoff_rule_ms": M15_MS},
        "generation": generation,
        "phase": phase,
        "shadow_semantics": measurements.get("shadow_semantics") or {},
        "lifecycle_state": lifecycle_state,
        "measurements": measurements,
        "config": asdict(cfg),
    }


def relation_to_trade(po3: Mapping[str, Any], side: str) -> Dict[str, Any]:
    """Strategy adapter helper; does not alter canonical market classification."""
    wanted = "BULLISH" if str(side).upper() == "BUY" else "BEARISH" if str(side).upper() == "SELL" else "NEUTRAL"
    got = str((po3.get("phase") or {}).get("direction") or "NEUTRAL").upper()
    relation = "NEUTRAL" if got == "NEUTRAL" or wanted == "NEUTRAL" else "ALIGNED" if got == wanted else "OPPOSED"
    return {"trade_side": str(side).upper(), "trade_direction": wanted, "phase_direction": got, "relation": relation}


__all__ = ["PO3Config", "SCHEMA_VERSION", "completed_bars", "measure_manipulation_distribution_links", "measure_po3_shadow_semantics", "evaluate_po3_market_phase", "relation_to_trade"]
