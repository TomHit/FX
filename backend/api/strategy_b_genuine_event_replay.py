"""
XauTrendLab Strategy-B genuine-event causal replay.

Phase 8B research/observation component.

This module has NO:
- broker authority
- execution authority
- risk reservation authority
- Strategy-A mutation
- production DXY state mutation
- Redis persistence

The first implementation provides a pure in-memory replay of the existing
REAL_DXY H1 early-turn detector.

Production calculation is reused from dxy_tracker._dxy_turn_metrics().
Persistence/state mutation from dxy_tracker._persist_turn_snapshot() is
intentionally NOT called.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Optional, Sequence

from api.dxy_tracker import _dxy_turn_metrics
from api.xtl_analytics import _atr_from_bars


MODEL = "STRATEGY_B_GENUINE_EVENT_REPLAY_V1"
OBSERVATION_ONLY = True
EXECUTION_AUTHORITY = False
STRATEGY_A_MUTATION = False


def _num(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _int(value: Any) -> Optional[int]:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _bar_close_ms(bar: Mapping[str, Any]) -> int:
    """
    Read canonical close time without manufacturing a timestamp.

    Strategy-B replay must receive bars carrying an explicit close-time field.
    """
    for key in (
        "t_close_ms",
        "tCloseMs",
        "close_ms",
        "bar_close_ms",
        "broker_bar_close_ms",
    ):
        value = _int(bar.get(key))
        if value is not None and value > 0:
            return value

    return 0


@dataclass(frozen=True)
class ReplayH1TurnEvent:
    change_ms: int
    direction: str
    from_short_state: str
    confidence: int
    recent_net_atr: Optional[float]
    prior_net_atr: Optional[float]
    up_steps: int
    down_steps: int
    bullish_bodies: int
    bearish_bodies: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "change_ms": self.change_ms,
            "bar_close_ms": self.change_ms,
            "direction": self.direction,
            "from_short_state": self.from_short_state,
            "confidence": self.confidence,
            "recent_net_atr": self.recent_net_atr,
            "prior_net_atr": self.prior_net_atr,
            "up_steps": self.up_steps,
            "down_steps": self.down_steps,
            "bullish_bodies": self.bullish_bodies,
            "bearish_bodies": self.bearish_bodies,
        }


@dataclass(frozen=True)
class ReplayH1State:
    as_of_ms: int
    short_state: str
    last_turn_direction: Optional[str]
    last_turn_ms: Optional[int]
    last_evaluated_bar_close_ms: Optional[int]
    confidence: int
    recent_net_atr: Optional[float]
    prior_net_atr: Optional[float]
    up_steps: int
    down_steps: int
    bullish_bodies: int
    bearish_bodies: int
    evaluations: int
    events: tuple[ReplayH1TurnEvent, ...]

    def to_dxy_h1_turn(self) -> dict[str, Any]:
        """
        Exact field family consumed by Strategy-B macro _h1_reversal().
        """
        return {
            "source": "REAL_DXY",
            "short_state": self.short_state,
            "last_turn_direction": self.last_turn_direction,
            "last_turn_ms": self.last_turn_ms,
            "last_evaluated_bar_close_ms": (
                self.last_evaluated_bar_close_ms
            ),
            "confidence": self.confidence,
            "recent_net_atr": self.recent_net_atr,
            "prior_net_atr": self.prior_net_atr,
            "up_steps": self.up_steps,
            "down_steps": self.down_steps,
            "bullish_bodies": self.bullish_bodies,
            "bearish_bodies": self.bearish_bodies,
            "replay_model": MODEL,
            "observation_only": True,
            "execution_authority": False,
            "strategy_a_mutation": False,
        }


def replay_h1_turn_state(
    bars: Sequence[Mapping[str, Any]],
    *,
    as_of_ms: int,
) -> ReplayH1State:
    """
    Pure in-memory equivalent of the chronological H1 turn-state portion of
    dxy_tracker bootstrap/runtime evaluation.

    Important production semantics preserved:
    - evaluate historical prefixes chronologically
    - ATR recalculated from each prefix
    - use production _dxy_turn_metrics()
    - NEUTRAL is an arming/reset state
    - directional event emitted when BULLISH/BEARISH != previous short state
    - last_turn_* changes only on directional event
    - no Redis reads/writes
    """

    cutoff = int(as_of_ms)

    causal: list[dict[str, Any]] = []

    for raw in bars:
        if not isinstance(raw, Mapping):
            continue

        row = dict(raw)
        close_ms = _bar_close_ms(row)

        if close_ms <= 0 or close_ms > cutoff:
            continue

        causal.append(row)

    causal.sort(key=_bar_close_ms)

    previous_direction = "NEUTRAL"
    last_turn_direction: Optional[str] = None
    last_turn_ms: Optional[int] = None
    last_eval_ms: Optional[int] = None

    latest_metrics: dict[str, Any] = {}
    evaluations = 0
    events: list[ReplayH1TurnEvent] = []

    for idx in range(6, len(causal)):
        prefix = causal[: idx + 1]

        atr = _atr_from_bars(prefix)
        metrics = _dxy_turn_metrics(prefix, atr) if atr else {}

        if not metrics:
            continue

        evaluations += 1
        latest_metrics = dict(metrics)

        direction = str(
            metrics.get("direction") or "NEUTRAL"
        ).upper()

        close_ms = _bar_close_ms(causal[idx])
        last_eval_ms = close_ms

        if (
            direction in ("BULLISH", "BEARISH")
            and direction != previous_direction
        ):
            event = ReplayH1TurnEvent(
                change_ms=close_ms,
                direction=direction,
                from_short_state=previous_direction,
                confidence=int(metrics.get("confidence") or 0),
                recent_net_atr=_num(metrics.get("recent_net_atr")),
                prior_net_atr=_num(metrics.get("prior_net_atr")),
                up_steps=int(metrics.get("up_steps") or 0),
                down_steps=int(metrics.get("down_steps") or 0),
                bullish_bodies=int(
                    metrics.get("bullish_bodies") or 0
                ),
                bearish_bodies=int(
                    metrics.get("bearish_bodies") or 0
                ),
            )

            events.append(event)
            last_turn_direction = direction
            last_turn_ms = close_ms

        # Preserve production bootstrap semantics:
        # NEUTRAL also becomes the previous short state.
        previous_direction = direction

    return ReplayH1State(
        as_of_ms=cutoff,
        short_state=previous_direction,
        last_turn_direction=last_turn_direction,
        last_turn_ms=last_turn_ms,
        last_evaluated_bar_close_ms=last_eval_ms,
        confidence=int(latest_metrics.get("confidence") or 0),
        recent_net_atr=_num(latest_metrics.get("recent_net_atr")),
        prior_net_atr=_num(latest_metrics.get("prior_net_atr")),
        up_steps=int(latest_metrics.get("up_steps") or 0),
        down_steps=int(latest_metrics.get("down_steps") or 0),
        bullish_bodies=int(
            latest_metrics.get("bullish_bodies") or 0
        ),
        bearish_bodies=int(
            latest_metrics.get("bearish_bodies") or 0
        ),
        evaluations=evaluations,
        events=tuple(events),
    )


__all__ = [
    "MODEL",
    "OBSERVATION_ONLY",
    "EXECUTION_AUTHORITY",
    "STRATEGY_A_MUTATION",
    "ReplayH1TurnEvent",
    "ReplayH1State",
    "replay_h1_turn_state",
]


# ---------------------------------------------------------------------------
# Phase 8B genuine-event checkpoint orchestration.
#
# This mirrors the existing Strategy-B production decision sequence while
# replacing only the mutable/current H1 turn-state input with the validated
# causal in-memory replay state.
#
# NO Redis access.
# NO persistence.
# NO broker/execution/risk authority.
# ---------------------------------------------------------------------------


def _to_plain_dict(value: Any) -> dict[str, Any]:
    if value is None:
        return {}

    if isinstance(value, Mapping):
        return dict(value)

    fn = getattr(value, "to_dict", None)
    if callable(fn):
        row = fn()
        if isinstance(row, Mapping):
            return dict(row)

    return {}


def replay_m15_turn_state(
    bars: Sequence[Mapping[str, Any]],
    *,
    as_of_ms: int,
    source: str,
    device_id: str,
    source_device_id: Optional[str] = None,
    binding: Optional[Mapping[str, Any]] = None,
    offset_min: int = 0,
    detected_at_ms: Optional[int] = None,
) -> dict[str, Any]:
    """
    Pure causal replay of the existing production M15 lifecycle.

    The production context builder and transition primitive are reused
    directly. Replay state is local to this function:

      - no Redis state seed
      - no Redis history seed
      - no Redis reads/writes
      - no persistence
      - historical=True only
      - chronological completed bars only

    This function does not manufacture an M15 history event. It returns
    the replay state that existed at or before as_of_ms.
    """
    from api.dxy_m15_tracker import (
        MIN_FEATURE_BARS,
        TF_MS,
        _build_m15_turn_context_pure,
        _completed_bars_at_or_before,
        _transition_m15_turn_pure,
    )

    cutoff = int(as_of_ms)
    if cutoff <= 0:
        raise ValueError("as_of_ms must be > 0")

    source_u = str(source or "").upper().strip()
    if source_u not in ("REAL_DXY", "SYNTHETIC_DXY"):
        raise ValueError("M15_REPLAY_SOURCE_INVALID")

    consumer_device_id = str(device_id or "").strip()
    if not consumer_device_id:
        raise ValueError("M15_REPLAY_DEVICE_ID_MISSING")

    source_dev = str(
        source_device_id or consumer_device_id
    ).strip()

    replay_binding = dict(binding or {})
    broker_offset = int(offset_min)

    raw_bars = [
        dict(row)
        for row in bars
        if isinstance(row, Mapping)
    ]

    causal = _completed_bars_at_or_before(
        raw_bars,
        cutoff_ms=cutoff,
        tf_ms=TF_MS,
    )

    if len(causal) < int(MIN_FEATURE_BARS):
        return {
            "schema_version": 1,
            "model": "STRATEGY_B_M15_CAUSAL_REPLAY_V1",
            "available": False,
            "reason": "INSUFFICIENT_M15_BARS",
            "as_of_ms": cutoff,
            "bar_count": len(causal),
            "state": {},
            "events": [],
            "observation_only": True,
            "execution_authority": False,
            "strategy_a_mutation": False,
            "production_state_mutation": False,
        }

    # Production update semantics use one detected_at_ms for the batch.
    # For historical research callers may provide that exact value.
    # Otherwise use the causal checkpoint itself; never wall-clock now.
    replay_detected_at_ms = int(
        detected_at_ms
        if detected_at_ms is not None
        else cutoff
    )

    old: dict[str, Any] = {}
    events: list[dict[str, Any]] = []
    evaluated = 0
    last_context: dict[str, Any] = {}

    for idx in range(
        int(MIN_FEATURE_BARS) - 1,
        len(causal),
    ):
        context = _build_m15_turn_context_pure(
            source=source_u,
            device_id=consumer_device_id,
            source_device_id=source_dev,
            bars=causal,
            index=idx,
            old=old,
            detected_at_ms=replay_detected_at_ms,
            offset_min=broker_offset,
            historical=True,
        )

        last_context = dict(context)

        last_confirmed_direction = (
            old.get("last_confirmed_direction")
            or old.get("previous_direction")
        )

        previous_direction = (
            old.get("previous_direction")
            or last_confirmed_direction
        )

        transition = _transition_m15_turn_pure(
            old=old,
            last_confirmed_direction=last_confirmed_direction,
            previous_direction=previous_direction,
            source=source_u,
            device_id=consumer_device_id,
            binding=replay_binding,
            close_ms=context["close_ms"],
            broker_close_ms=context["broker_close_ms"],
            offset_min=broker_offset,
            detected_at_ms=replay_detected_at_ms,
            current_close=context["current_close"],
            current_atr=context["current_atr"],
            bull_score=context["bull_score"],
            bear_score=context["bear_score"],
            score_direction=context["score_direction"],
            score_margin=context["score_margin"],
            qualification_reason=context["qualification_reason"],
            features=context["features"],
            market_flow=context["market_flow"],
            structure=context["structure"],
            structure_targets=context["structure_targets"],
            historical=True,
        )

        state = transition.get("state")
        if not isinstance(state, Mapping):
            raise RuntimeError(
                "M15_REPLAY_TRANSITION_STATE_MISSING"
            )

        event = transition.get("event_record")
        if isinstance(event, Mapping):
            events.append(dict(event))

        old = dict(state)
        evaluated += 1

    return {
        "schema_version": 1,
        "model": "STRATEGY_B_M15_CAUSAL_REPLAY_V1",
        "available": bool(old),
        "reason": (
            "CAUSAL_STATE_RECONSTRUCTED"
            if old
            else "NO_CAUSAL_STATE"
        ),
        "as_of_ms": cutoff,
        "bar_count": len(causal),
        "evaluations": evaluated,
        "state": old,
        "events": events,
        "last_context": last_context,
        "observation_only": True,
        "execution_authority": False,
        "strategy_a_mutation": False,
        "production_state_mutation": False,
    }



def build_scheduled_catalyst_replay_evidence(
    scheduled_event: Mapping[str, Any],
    *,
    catalyst_first_seen_ms: int,
):
    """
    Build observation-only MacroEvidence for a historical scheduled catalyst
    that has no accepted canonical MT5 result/release.

    Replay semantics:
      - scheduled event proves catalyst existence/timing only
      - no actual/forecast surprise is manufactured
      - fundamental direction remains UNKNOWN
      - catalyst_first_seen_ms is supplied explicitly by the replay harness
      - for event-time causal replay this should normally be event T0
      - no Redis access, persistence, broker authority, or execution authority
    """
    if not isinstance(scheduled_event, Mapping):
        raise TypeError("scheduled_event must be a mapping")

    outer = dict(scheduled_event)
    event = outer.get("event")
    if not isinstance(event, Mapping):
        raise ValueError("SCHEDULED_EVENT_PAYLOAD_MISSING")

    event = dict(event)

    event_id = str(
        outer.get("event_id")
        or event.get("event_id")
        or ""
    ).strip()

    value_id = str(
        outer.get("value_id")
        or event.get("value_id")
        or ""
    ).strip()

    owner_id = str(
        outer.get("calendar_owner_id")
        or ""
    ).strip()

    event_time_ms = int(
        outer.get("event_time_utc_ms")
        or event.get("event_time_utc_ms")
        or 0
    )

    first_seen_ms = int(catalyst_first_seen_ms or 0)

    if not event_id:
        raise ValueError("SCHEDULED_EVENT_ID_MISSING")
    if not value_id:
        raise ValueError("SCHEDULED_VALUE_ID_MISSING")
    if not owner_id:
        raise ValueError("SCHEDULED_CALENDAR_OWNER_MISSING")
    if event_time_ms <= 0:
        raise ValueError("SCHEDULED_EVENT_TIME_MISSING")
    if first_seen_ms <= 0:
        raise ValueError("CATALYST_FIRST_SEEN_TIME_MISSING")
    if first_seen_ms < event_time_ms:
        raise ValueError("CATALYST_FIRST_SEEN_PRECEDES_EVENT_T0")

    from api.macro_event_intelligence_strategy_b_thesis_v1 import (
        MacroDirection,
        MacroEvidence,
        MarketReaction,
        StrategyBEffect,
        ThesisRelationship,
    )

    evidence_id = (
        "strategy_b_replay_scheduled:"
        f"{owner_id}:{value_id}:{event_time_ms}"
    )

    return MacroEvidence(
        evidence_id=evidence_id,
        source="MT5_CALENDAR_SCHEDULE_REPLAY",
        source_id=value_id,
        currency=str(
            outer.get("currency")
            or event.get("currency")
            or ""
        ).upper().strip(),
        title=str(
            outer.get("event_name")
            or event.get("event_name")
            or event.get("title")
            or ""
        ).strip(),
        event_time_ms=event_time_ms,
        published_at_ms=0,
        first_seen_at_ms=first_seen_ms,
        observed_at_ms=first_seen_ms,
        importance=str(
            outer.get("importance")
            or event.get("importance")
            or ""
        ).upper().strip(),
        actual=None,
        forecast=None,
        previous=None,
        revised_previous=None,
        fundamental_direction=MacroDirection.UNKNOWN.value,
        market_reaction=MarketReaction.UNKNOWN.value,
        thesis_relationship=ThesisRelationship.UNKNOWN.value,
        effect=StrategyBEffect.NO_CHANGE.value,
        confidence=0.0,
        reasons=(
            "SCHEDULED_CATALYST_NO_ACCEPTED_RESULT",
            "FUNDAMENTAL_DIRECTION_NOT_INFERRED",
            "REPLAY_CATALYST_TIME_EXPLICIT",
        ),
        metadata={
            "calendar_owner_id": owner_id,
            "event_id": event_id,
            "value_id": value_id,
            "scheduled_event_time_ms": event_time_ms,
            "catalyst_first_seen_ms": first_seen_ms,
            "scheduled_source": str(
                outer.get("source")
                or event.get("source")
                or ""
            ),
            "time_mode": str(event.get("time_mode") or ""),
            "observation_only": True,
            "execution_authority": False,
            "strategy_a_mutation": False,
            "replay_only": True,
        },
    )

def historical_m15_event_to_state(
    event: Mapping[str, Any],
) -> dict[str, Any]:
    """
    Convert one canonical historical REAL_DXY M15 turn-history event into
    the production-shaped dxy_m15 contract consumed by Strategy-B.

    Pure replay adapter:
      - no Redis access
      - no writes
      - no lifecycle inference
      - preserves the historical outer status/direction
      - preserves the nested features.market_flow / features.structure
    """
    if not isinstance(event, Mapping):
        raise TypeError("historical M15 event must be a mapping")

    row = dict(event)

    if str(row.get("event_type") or "").upper() != "DXY_M15_MARKET_STATE_TURN":
        raise ValueError("M15_HISTORY_EVENT_TYPE_INVALID")

    direction = str(row.get("direction") or "").upper().strip()
    status = str(row.get("status") or "").upper().strip()

    if direction not in ("BULLISH", "BEARISH"):
        raise ValueError("M15_HISTORY_DIRECTION_INVALID")

    if not status:
        raise ValueError("M15_HISTORY_STATUS_MISSING")

    features = row.get("features")
    if not isinstance(features, Mapping):
        raise ValueError("M15_HISTORY_FEATURES_MISSING")

    market_flow = features.get("market_flow")
    if not isinstance(market_flow, Mapping):
        raise ValueError("M15_HISTORY_MARKET_FLOW_MISSING")

    structure = features.get("structure")
    if not isinstance(structure, Mapping):
        raise ValueError("M15_HISTORY_STRUCTURE_MISSING")

    # Do not manufacture confirmation or reinterpret lifecycle state.
    # Strategy-B's qualifier consumes the historical status directly.
    row["status"] = status
    row["direction"] = direction

    return row


def evaluate_genuine_event_checkpoint(
    *,
    evidence: Any,
    checkpoint_ms: int,
    dxy_m15: Mapping[str, Any],
    dxy_bias: Mapping[str, Any],
    dxy_structure: Optional[Mapping[str, Any]],
    h1_bars: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """
    Evaluate one Strategy-B historical checkpoint causally.

    Inputs must already represent the historical evidence available at
    checkpoint_ms. This function performs no Redis discovery and no writes.

    Production sequence mirrored:
      1. reconstruct causal H1 early-turn state
      2. Strategy-B macro qualification
      3. Strategy-B H1 structural pattern observation
      4. Strategy-B entry-pattern decision

    This remains observation-only regardless of the resulting shadow decision.
    """

    checkpoint = int(checkpoint_ms)

    if checkpoint <= 0:
        raise ValueError("checkpoint_ms must be > 0")

    from api.strategy_b_macro_qualification import (
        evaluate_strategy_b_macro_qualification,
    )
    from api.strategy_b_h1_pattern_observer import (
        observe_strategy_b_h1_patterns,
    )
    from api.strategy_b_entry_patterns import (
        evaluate_strategy_b_entry_patterns,
    )

    # ---------------------------------------------------------------
    # 1. Validated pure H1 early-turn reconstruction.
    # ---------------------------------------------------------------
    replay_h1 = replay_h1_turn_state(
        h1_bars,
        as_of_ms=checkpoint,
    )

    dxy_h1_turn = replay_h1.to_dxy_h1_turn()

    # ---------------------------------------------------------------
    # 2. Frozen/current Strategy-B macro qualification.
    # ---------------------------------------------------------------
    macro = evaluate_strategy_b_macro_qualification(
        evidence,
        dxy_bias=dxy_bias,
        dxy_m15=dxy_m15,
        dxy_h1_turn=dxy_h1_turn,
        dxy_structure=dxy_structure,
    )

    macro_row = _to_plain_dict(macro)

    if (
        int(macro_row.get("schema_version") or 0) < 2
        or "market_reaction_confirmed" not in macro_row
    ):
        raise RuntimeError(
            "STRATEGY_B_MACRO_QUALIFICATION_V2_REQUIRED"
        )

    # ---------------------------------------------------------------
    # 3. Existing production H1 structural observer.
    #
    # It owns its own as_of_ms normalization. Pass the original
    # canonical H1 sequence and the replay checkpoint exactly as the
    # live service passes h1_bars + now_ms.
    # ---------------------------------------------------------------
    catalyst_first_seen_ms = int(
        getattr(macro, "catalyst_first_seen_ms", 0) or 0
    )

    pattern_observer = observe_strategy_b_h1_patterns(
        h1_bars=h1_bars,
        expected_direction=macro.expected_dxy_direction,
        catalyst_first_seen_ms=catalyst_first_seen_ms,
        dxy_bias=dxy_bias,
        dxy_structure=dxy_structure,
        as_of_ms=checkpoint,
    )

    pattern_observations = (
        pattern_observer.to_entry_pattern_observations()
    )

    # ---------------------------------------------------------------
    # 4. Existing production downstream entry-pattern evaluator.
    # ---------------------------------------------------------------
    downstream_entry = evaluate_strategy_b_entry_patterns(
        macro,
        pattern_observations=pattern_observations,
        m15_state=dxy_m15,
    )

    pattern_row = _to_plain_dict(pattern_observer)
    entry_row = _to_plain_dict(downstream_entry)

    # ---------------------------------------------------------------
    # Observation-only replay envelope.
    # ENTRY_READY_SHADOW remains telemetry only here.
    # ---------------------------------------------------------------
    return {
        "schema_version": 1,
        "model": "STRATEGY_B_GENUINE_EVENT_CHECKPOINT_REPLAY_V1",
        "checkpoint_ms": checkpoint,
        "observation_only": True,
        "execution_authority": False,
        "strategy_a_mutation": False,
        "production_state_mutation": False,
        "h1_turn": dxy_h1_turn,
        "h1_turn_event_count": len(replay_h1.events),
        "macro": macro_row,
        "h1_pattern_observer": pattern_row,
        "entry_pattern_observations": dict(
            pattern_observations
            if isinstance(pattern_observations, Mapping)
            else {}
        ),
        "entry": entry_row,
    }
