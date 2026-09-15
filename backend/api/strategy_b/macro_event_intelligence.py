"""
XauTrendLab Strategy B - Macro Event Intelligence
=================================================

Phase 1: observation-only intelligence layer.

Purpose
-------
Turn already-acquired macro evidence into Strategy-B-owned, auditable
evidence. This module has NO broker/order authority and does not mutate
Strategy-A state.

Design constraints
------------------
- No Redis SCAN.
- No full-table polling.
- No Finnhub HTTP calls here.
- No MT5 order calls here.
- No Strategy-A Redis writes.
- One macro interpretation per evidence item, not one per symbol.
- Preserve causal timestamps (published/first-seen/result-seen).
- Deterministic rule layer first; market reaction is supplied separately.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
import hashlib
import json
import math
import time
from typing import Any, Mapping, Optional, Sequence


SCHEMA_VERSION = 1
SOURCE_MODULE = "strategy_b.macro_event_intelligence"


class EvidenceSource(str, Enum):
    MT5_EVENT = "MT5_EVENT"
    FINNHUB = "FINNHUB"


class MacroDirection(str, Enum):
    USD_BULLISH = "USD_BULLISH"
    USD_BEARISH = "USD_BEARISH"
    MIXED = "MIXED"
    NEUTRAL = "NEUTRAL"
    UNKNOWN = "UNKNOWN"


class MarketReaction(str, Enum):
    PENDING = "PENDING"
    CONFIRMED = "CONFIRMED"
    REJECTED = "REJECTED"
    MIXED = "MIXED"
    UNKNOWN = "UNKNOWN"


class ThesisRelationship(str, Enum):
    SUPPORTS = "SUPPORTS"
    OPPOSES = "OPPOSES"
    NEUTRAL = "NEUTRAL"
    MIXED = "MIXED"
    UNKNOWN = "UNKNOWN"


class StrategyBEffect(str, Enum):
    NO_CHANGE = "NO_CHANGE"
    REACTION_PENDING = "REACTION_PENDING"
    ENTRY_SUPPORT = "ENTRY_SUPPORT"
    ENTRY_DAMAGE = "ENTRY_DAMAGE"
    HOLD = "HOLD"
    PROTECT = "PROTECT"
    EXIT_REVIEW = "EXIT_REVIEW"


class DxyThesisState(str, Enum):
    """Observation-only Strategy-B DXY thesis lifecycle."""

    OBSERVING = "OBSERVING"
    CATALYST_DETECTED = "CATALYST_DETECTED"
    REACTION_PENDING = "REACTION_PENDING"
    MARKET_REACTION_CONFIRMED = "MARKET_REACTION_CONFIRMED"
    REVERSAL_CONFIRMED = "REVERSAL_CONFIRMED"
    STRUCTURALLY_CONSTRAINED = "STRUCTURALLY_CONSTRAINED"
    REJECTED = "REJECTED"


class StructuralCapacity(str, Enum):
    """Room available in the evidence-implied DXY direction."""

    OPEN = "OPEN"
    CAUTION = "CAUTION"
    LIMITED = "LIMITED"
    BLOCKED = "BLOCKED"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True)
class NumericValue:
    raw: Any = None
    value: Optional[float] = None


@dataclass(frozen=True)
class MacroEvidence:
    evidence_id: str
    source: str
    source_id: str
    currency: str
    title: str

    event_time_ms: int = 0
    published_at_ms: int = 0
    first_seen_at_ms: int = 0
    observed_at_ms: int = 0

    importance: str = ""
    topics: tuple[str, ...] = ()

    actual: Optional[float] = None
    forecast: Optional[float] = None
    previous: Optional[float] = None
    revised_previous: Optional[float] = None

    fundamental_direction: str = MacroDirection.UNKNOWN.value
    market_reaction: str = MarketReaction.UNKNOWN.value
    thesis_relationship: str = ThesisRelationship.UNKNOWN.value
    effect: str = StrategyBEffect.NO_CHANGE.value

    confidence: float = 0.0
    reasons: tuple[str, ...] = ()
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        row["topics"] = list(self.topics)
        row["reasons"] = list(self.reasons)
        row["metadata"] = dict(self.metadata)
        row["schema_version"] = SCHEMA_VERSION
        row["source_module"] = SOURCE_MODULE
        return row


@dataclass(frozen=True)
class DxyMacroThesis:
    """
    Strategy-B-owned, observation-only DXY thesis snapshot.

    This object consumes already-produced canonical evidence. It does not load
    Redis, calculate SR, call MT5/Finnhub, or create broker intent.
    """

    thesis_id: str
    evidence_id: str
    as_of_ms: int
    state: str

    fundamental_direction: str
    expected_dxy_direction: str
    market_reaction: str

    d1_direction: str = "UNKNOWN"
    h4_direction: str = "UNKNOWN"
    h1_direction: str = "UNKNOWN"
    m15_flow_direction: str = "NEUTRAL"
    m15_turn_status: str = "UNKNOWN"
    m15_turn_direction: str = "NEUTRAL"

    structure_capacity: str = StructuralCapacity.UNKNOWN.value
    structure_pressure: str = "UNKNOWN"
    structure_pressure_reason: str = "NO_DIRECTIONAL_STRUCTURE"
    directional_room_atr: Optional[float] = None
    inside_blocking_zone: bool = False

    h1_reversal_confirmed: bool = False
    confidence: float = 0.0
    reasons: tuple[str, ...] = ()
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        row["reasons"] = list(self.reasons)
        row["metadata"] = dict(self.metadata)
        row["schema_version"] = SCHEMA_VERSION
        row["source_module"] = SOURCE_MODULE
        row["observation_only"] = True
        row["execution_authority"] = False
        return row


def _now_ms() -> int:
    return int(time.time() * 1000)


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _upper(value: Any) -> str:
    return _clean_text(value).upper()


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError, OverflowError):
        return 0


def _safe_float(value: Any) -> Optional[float]:
    """
    Accept scalar numbers or normalized XTL objects:
        {"raw": "...", "value": 0.3}
    """
    if isinstance(value, Mapping):
        value = value.get("value")

    if value is None or isinstance(value, bool):
        return None

    try:
        out = float(value)
    except (TypeError, ValueError, OverflowError):
        return None

    return out if math.isfinite(out) else None


def _stable_id(*parts: Any) -> str:
    material = "|".join(_clean_text(p) for p in parts)
    return hashlib.sha256(material.encode("utf-8")).hexdigest()[:32]


def _coerce_topics(value: Any) -> tuple[str, ...]:
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return ()
        try:
            decoded = json.loads(text)
            if isinstance(decoded, list):
                value = decoded
            else:
                value = [text]
        except Exception:
            value = [text]

    if not isinstance(value, (list, tuple, set)):
        return ()

    out: list[str] = []
    seen: set[str] = set()
    for item in value:
        topic = _upper(item)
        if topic and topic not in seen:
            seen.add(topic)
            out.append(topic)
    return tuple(out)


def _relationship(
    thesis_direction: MacroDirection,
    evidence_direction: MacroDirection,
) -> ThesisRelationship:
    if evidence_direction in (MacroDirection.UNKNOWN, MacroDirection.NEUTRAL):
        return ThesisRelationship.NEUTRAL

    if evidence_direction == MacroDirection.MIXED:
        return ThesisRelationship.MIXED

    if thesis_direction in (MacroDirection.UNKNOWN, MacroDirection.NEUTRAL):
        return ThesisRelationship.UNKNOWN

    if thesis_direction == MacroDirection.MIXED:
        return ThesisRelationship.MIXED

    return (
        ThesisRelationship.SUPPORTS
        if thesis_direction == evidence_direction
        else ThesisRelationship.OPPOSES
    )


def _effect(
    relationship: ThesisRelationship,
    reaction: MarketReaction,
    *,
    trade_active: bool,
    profitable: bool,
) -> StrategyBEffect:
    """
    Observation-only intent.

    Important:
    This NEVER means "place/close an order". A later Strategy-B lifecycle
    engine must combine this evidence with DXY/HTF/H1/M15 structure and
    deterministic risk gates.
    """
    if reaction in (MarketReaction.UNKNOWN, MarketReaction.PENDING):
        return StrategyBEffect.REACTION_PENDING

    if relationship == ThesisRelationship.SUPPORTS:
        if reaction == MarketReaction.CONFIRMED:
            return (
                StrategyBEffect.HOLD
                if trade_active
                else StrategyBEffect.ENTRY_SUPPORT
            )
        if reaction == MarketReaction.REJECTED:
            return (
                StrategyBEffect.PROTECT
                if trade_active and profitable
                else StrategyBEffect.ENTRY_DAMAGE
            )

    if relationship == ThesisRelationship.OPPOSES:
        if reaction == MarketReaction.CONFIRMED:
            return (
                StrategyBEffect.EXIT_REVIEW
                if trade_active
                else StrategyBEffect.ENTRY_DAMAGE
            )
        if reaction == MarketReaction.REJECTED:
            return (
                StrategyBEffect.HOLD
                if trade_active
                else StrategyBEffect.ENTRY_SUPPORT
            )

    return StrategyBEffect.NO_CHANGE


# ---------------------------------------------------------------------------
# Scheduled MT5 event interpretation
# ---------------------------------------------------------------------------

# Direction of a POSITIVE surprise (actual > forecast) for USD.
# This is intentionally narrow for Phase 1. Unknown releases remain UNKNOWN
# rather than guessing. Add rules only after explicit validation.
_USD_POSITIVE_SURPRISE_DIRECTION: dict[str, MacroDirection] = {
    "CPI": MacroDirection.USD_BULLISH,
    "CORE CPI": MacroDirection.USD_BULLISH,
    "PCE": MacroDirection.USD_BULLISH,
    "CORE PCE": MacroDirection.USD_BULLISH,
    "GDP": MacroDirection.USD_BULLISH,
    "RETAIL SALES": MacroDirection.USD_BULLISH,
    "NONFARM": MacroDirection.USD_BULLISH,
    "NON-FARM": MacroDirection.USD_BULLISH,
    "NFP": MacroDirection.USD_BULLISH,
    "AVERAGE HOURLY EARNINGS": MacroDirection.USD_BULLISH,
    "UNEMPLOYMENT RATE": MacroDirection.USD_BEARISH,
    "UNEMPLOYMENT CLAIMS": MacroDirection.USD_BEARISH,
    "INITIAL JOBLESS CLAIMS": MacroDirection.USD_BEARISH,
}


def _match_usd_release_rule(event_name: str) -> Optional[MacroDirection]:
    name = _upper(event_name)
    # Longest phrase first prevents "CPI" winning before "CORE CPI".
    for phrase in sorted(
        _USD_POSITIVE_SURPRISE_DIRECTION,
        key=len,
        reverse=True,
    ):
        if phrase in name:
            return _USD_POSITIVE_SURPRISE_DIRECTION[phrase]
    return None


def classify_mt5_event_direction(
    event: Mapping[str, Any],
) -> tuple[MacroDirection, float, tuple[str, ...]]:
    """
    Conservative first-pass scheduled-release classifier.

    It deliberately does not infer a direction when:
      - currency is not USD,
      - actual/forecast are absent,
      - the event has no reviewed semantic rule,
      - actual == forecast.

    Market reaction is a separate input. A hot CPI can therefore be
    USD_BULLISH fundamentally while the observed USD reaction is REJECTED.
    """
    currency = _upper(event.get("currency"))
    name = _clean_text(event.get("event_name") or event.get("title"))

    if currency != "USD":
        return (
            MacroDirection.UNKNOWN,
            0.0,
            ("NON_USD_EVENT_NOT_CLASSIFIED_PHASE1",),
        )

    actual = _safe_float(event.get("actual"))
    forecast = _safe_float(event.get("forecast"))

    if actual is None:
        return (
            MacroDirection.UNKNOWN,
            0.0,
            ("ACTUAL_NOT_AVAILABLE",),
        )

    if forecast is None:
        return (
            MacroDirection.UNKNOWN,
            0.0,
            ("FORECAST_NOT_AVAILABLE",),
        )

    positive_direction = _match_usd_release_rule(name)
    if positive_direction is None:
        return (
            MacroDirection.UNKNOWN,
            0.0,
            ("NO_REVIEWED_EVENT_SEMANTIC_RULE",),
        )

    delta = actual - forecast
    if math.isclose(delta, 0.0, rel_tol=0.0, abs_tol=1e-12):
        return (
            MacroDirection.NEUTRAL,
            0.55,
            ("ACTUAL_EQUALS_FORECAST",),
        )

    if delta > 0:
        direction = positive_direction
        reason = "ACTUAL_ABOVE_FORECAST"
    else:
        direction = (
            MacroDirection.USD_BEARISH
            if positive_direction == MacroDirection.USD_BULLISH
            else MacroDirection.USD_BULLISH
        )
        reason = "ACTUAL_BELOW_FORECAST"

    return (
        direction,
        0.75,
        (
            reason,
            f"EVENT_RULE={_upper(name)}",
        ),
    )


def build_mt5_event_evidence(
    event: Mapping[str, Any],
    *,
    calendar_owner_id: str,
    first_result_seen_ms: int,
    market_reaction: MarketReaction = MarketReaction.PENDING,
    thesis_direction: MacroDirection = MacroDirection.UNKNOWN,
    trade_active: bool = False,
    profitable: bool = False,
    metadata: Optional[Mapping[str, Any]] = None,
) -> MacroEvidence:
    direction, confidence, reasons = classify_mt5_event_direction(event)

    event_id = _clean_text(event.get("event_id"))
    value_id = _clean_text(event.get("value_id"))
    event_time_ms = _safe_int(
        event.get("event_time_utc_ms")
        or event.get("scheduled_time_utc_ms")
    )

    source_id = value_id or _stable_id(event_id, event_time_ms)
    evidence_id = _stable_id(
        EvidenceSource.MT5_EVENT.value,
        calendar_owner_id,
        source_id,
        event_time_ms,
    )

    relationship = _relationship(thesis_direction, direction)
    effect = _effect(
        relationship,
        market_reaction,
        trade_active=trade_active,
        profitable=profitable,
    )

    meta = dict(metadata or {})
    meta.update({
        "calendar_owner_id": _clean_text(calendar_owner_id),
        "event_id": event_id,
        "value_id": value_id,
    })

    return MacroEvidence(
        evidence_id=evidence_id,
        source=EvidenceSource.MT5_EVENT.value,
        source_id=source_id,
        currency=_upper(event.get("currency")),
        title=_clean_text(event.get("event_name") or event.get("title")),
        event_time_ms=event_time_ms,
        first_seen_at_ms=_safe_int(first_result_seen_ms),
        observed_at_ms=_safe_int(first_result_seen_ms) or _now_ms(),
        importance=_upper(event.get("importance")),
        actual=_safe_float(event.get("actual")),
        forecast=_safe_float(event.get("forecast")),
        previous=_safe_float(event.get("previous")),
        revised_previous=_safe_float(event.get("revised_previous")),
        fundamental_direction=direction.value,
        market_reaction=market_reaction.value,
        thesis_relationship=relationship.value,
        effect=effect.value,
        confidence=confidence,
        reasons=reasons,
        metadata=meta,
    )


# ---------------------------------------------------------------------------
# Finnhub interpretation adapter
# ---------------------------------------------------------------------------

def _finnhub_direction_from_topics(
    row: Mapping[str, Any],
) -> tuple[MacroDirection, float, tuple[str, ...]]:
    """
    Phase-1 adapter.

    Existing Finnhub collection/classification remains authoritative for
    acquisition. We intentionally avoid inventing a universal trading
    direction from headline keywords here.

    If an upstream reviewed classifier later supplies a macro direction,
    this function accepts it. Otherwise the evidence remains UNKNOWN and
    awaits a dedicated Strategy-B news interpretation rule/model.
    """
    supplied = _upper(
        row.get("macro_direction")
        or row.get("usd_direction")
        or row.get("direction")
    )

    try:
        direction = MacroDirection(supplied)
    except ValueError:
        direction = MacroDirection.UNKNOWN

    if direction != MacroDirection.UNKNOWN:
        return (
            direction,
            0.65,
            ("UPSTREAM_REVIEWED_MACRO_DIRECTION",),
        )

    topics = _coerce_topics(row.get("topics"))
    if topics:
        return (
            MacroDirection.UNKNOWN,
            0.0,
            ("TOPICS_PRESENT_DIRECTION_NOT_INFERRED",),
        )

    return (
        MacroDirection.UNKNOWN,
        0.0,
        ("NO_DIRECTIONAL_FINNHUB_CLASSIFICATION",),
    )


def build_finnhub_evidence(
    row: Mapping[str, Any],
    *,
    market_reaction: MarketReaction = MarketReaction.PENDING,
    thesis_direction: MacroDirection = MacroDirection.UNKNOWN,
    trade_active: bool = False,
    profitable: bool = False,
    metadata: Optional[Mapping[str, Any]] = None,
) -> MacroEvidence:
    direction, confidence, reasons = _finnhub_direction_from_topics(row)

    provider = _upper(row.get("provider") or "FINNHUB")
    provider_article_id = _clean_text(
        row.get("provider_article_id")
        or row.get("id")
    )
    first_seen_ms = _safe_int(
        row.get("first_seen_at_ms")
        or row.get("first_seen_ms")
    )
    published_ms = _safe_int(
        row.get("published_at_ms")
        or row.get("published_ms")
    )

    source_id = provider_article_id or _stable_id(
        provider,
        published_ms,
        row.get("headline") or row.get("title"),
    )
    evidence_id = _stable_id(
        EvidenceSource.FINNHUB.value,
        provider,
        source_id,
    )

    relationship = _relationship(thesis_direction, direction)
    effect = _effect(
        relationship,
        market_reaction,
        trade_active=trade_active,
        profitable=profitable,
    )

    meta = dict(metadata or {})
    meta["provider"] = provider

    return MacroEvidence(
        evidence_id=evidence_id,
        source=EvidenceSource.FINNHUB.value,
        source_id=source_id,
        currency=_upper(row.get("currency")),
        title=_clean_text(row.get("headline") or row.get("title")),
        published_at_ms=published_ms,
        first_seen_at_ms=first_seen_ms,
        observed_at_ms=first_seen_ms or _now_ms(),
        importance=_upper(row.get("importance")),
        topics=_coerce_topics(row.get("topics")),
        fundamental_direction=direction.value,
        market_reaction=market_reaction.value,
        thesis_relationship=relationship.value,
        effect=effect.value,
        confidence=confidence,
        reasons=reasons,
        metadata=meta,
    )


# ---------------------------------------------------------------------------
# Reaction update
# ---------------------------------------------------------------------------

def apply_market_reaction(
    evidence: MacroEvidence,
    *,
    reaction: MarketReaction,
    thesis_direction: MacroDirection,
    trade_active: bool = False,
    profitable: bool = False,
    reaction_reasons: Sequence[str] = (),
    reaction_metadata: Optional[Mapping[str, Any]] = None,
) -> MacroEvidence:
    """
    Return a new immutable evidence snapshot after DXY/cross-market/symbol
    reaction has been evaluated elsewhere.

    This module does not calculate DXY structure itself.
    """
    try:
        direction = MacroDirection(evidence.fundamental_direction)
    except ValueError:
        direction = MacroDirection.UNKNOWN

    relationship = _relationship(thesis_direction, direction)
    effect = _effect(
        relationship,
        reaction,
        trade_active=trade_active,
        profitable=profitable,
    )

    meta = dict(evidence.metadata)
    if reaction_metadata:
        meta["market_reaction"] = dict(reaction_metadata)

    reasons = tuple(evidence.reasons) + tuple(
        _clean_text(x)
        for x in reaction_reasons
        if _clean_text(x)
    )

    return MacroEvidence(
        **{
            **asdict(evidence),
            "market_reaction": reaction.value,
            "thesis_relationship": relationship.value,
            "effect": effect.value,
            "reasons": reasons,
            "metadata": meta,
        }
    )


# ---------------------------------------------------------------------------
# DXY macro thesis aggregation
# ---------------------------------------------------------------------------


def _canonical_dxy_direction(value: Any) -> str:
    direction = _upper(value)
    if direction in ("UP", "BULL", "BULLISH", "LONG", "BUY"):
        return "BULLISH"
    if direction in ("DOWN", "BEAR", "BEARISH", "SHORT", "SELL"):
        return "BEARISH"
    if direction == "NEUTRAL":
        return "NEUTRAL"
    return "UNKNOWN"


def _expected_dxy_direction(direction: MacroDirection) -> str:
    if direction == MacroDirection.USD_BULLISH:
        return "BULLISH"
    if direction == MacroDirection.USD_BEARISH:
        return "BEARISH"
    return "UNKNOWN"


def _bias_tf_direction(dxy_bias: Mapping[str, Any], tf: str) -> str:
    timeframes = dxy_bias.get("timeframes")
    if not isinstance(timeframes, Mapping):
        return "UNKNOWN"
    row = timeframes.get(tf)
    if not isinstance(row, Mapping):
        return "UNKNOWN"
    if row.get("data_ok") is False:
        return "UNKNOWN"
    return _canonical_dxy_direction(row.get("direction"))


def _m15_fields(dxy_m15: Mapping[str, Any]) -> tuple[str, str, str]:
    market_flow = dxy_m15.get("market_flow")
    if not isinstance(market_flow, Mapping):
        market_flow = {}

    flow_direction = _canonical_dxy_direction(
        market_flow.get("direction")
        or dxy_m15.get("market_flow_direction")
    )

    turn_status = _upper(
        dxy_m15.get("turn_status")
        or dxy_m15.get("status")
        or "UNKNOWN"
    )

    turn_direction = _canonical_dxy_direction(
        dxy_m15.get("turn_direction")
        or dxy_m15.get("candidate_direction")
        or dxy_m15.get("direction")
    )

    return flow_direction, turn_status, turn_direction


def _structure_for_expected_direction(
    structure: Mapping[str, Any],
    expected_dxy_direction: str,
) -> tuple[StructuralCapacity, str, str, Optional[float], bool]:
    """
    Interpret existing canonical trend_sr structure in the evidence-implied
    DXY direction. No SR is calculated here.

    Thresholds mirror trend_sr.build_market_structure_state():
      <= 0.20 ATR -> EXTREME
      <= 0.50 ATR -> HIGH
      <= 1.00 ATR -> MEDIUM
      >  1.00 ATR -> LOW
    """
    expected = _canonical_dxy_direction(expected_dxy_direction)

    if expected == "BULLISH":
        room = _safe_float(structure.get("room_up_atr"))
        inside = bool(structure.get("inside_resistance"))
        inside_reason = "INSIDE_RESISTANCE"
    elif expected == "BEARISH":
        room = _safe_float(structure.get("room_down_atr"))
        inside = bool(structure.get("inside_support"))
        inside_reason = "INSIDE_SUPPORT"
    else:
        return (
            StructuralCapacity.UNKNOWN,
            "UNKNOWN",
            "NO_DIRECTIONAL_STRUCTURE",
            None,
            False,
        )

    if inside:
        return (
            StructuralCapacity.BLOCKED,
            "EXTREME",
            inside_reason,
            0.0,
            True,
        )

    if room is None:
        return (
            StructuralCapacity.UNKNOWN,
            "UNKNOWN",
            "NO_DIRECTIONAL_STRUCTURE",
            None,
            False,
        )

    if room <= 0.20:
        return (
            StructuralCapacity.BLOCKED,
            "EXTREME",
            "ROOM_LE_0_20_ATR",
            room,
            False,
        )
    if room <= 0.50:
        return (
            StructuralCapacity.LIMITED,
            "HIGH",
            "ROOM_LE_0_50_ATR",
            room,
            False,
        )
    if room <= 1.00:
        return (
            StructuralCapacity.CAUTION,
            "MEDIUM",
            "ROOM_LE_1_00_ATR",
            room,
            False,
        )

    return (
        StructuralCapacity.OPEN,
        "LOW",
        "ROOM_GT_1_00_ATR",
        room,
        False,
    )


def infer_dxy_market_reaction(
    evidence: MacroEvidence,
    dxy_m15: Mapping[str, Any],
) -> tuple[MarketReaction, tuple[str, ...]]:
    """
    Infer only the immediate DXY reaction from the existing M15 state.

    A formally CONFIRMED M15 turn in the catalyst-implied direction confirms
    immediate market acceptance. A formally CONFIRMED opposite turn rejects
    it. Current flow without a confirmed turn remains PENDING; this prevents
    one fast flow snapshot from being promoted into full Strategy-B reversal
    confirmation.
    """
    try:
        fundamental = MacroDirection(evidence.fundamental_direction)
    except ValueError:
        fundamental = MacroDirection.UNKNOWN

    expected = _expected_dxy_direction(fundamental)
    if expected == "UNKNOWN":
        return MarketReaction.UNKNOWN, ("NO_DIRECTIONAL_CATALYST",)

    flow_direction, turn_status, turn_direction = _m15_fields(dxy_m15)

    if turn_status == "CONFIRMED":
        if turn_direction == expected:
            return (
                MarketReaction.CONFIRMED,
                ("M15_CONFIRMED_TURN_ALIGNS_WITH_CATALYST",),
            )
        if turn_direction in ("BULLISH", "BEARISH") and turn_direction != expected:
            return (
                MarketReaction.REJECTED,
                ("M15_CONFIRMED_TURN_OPPOSES_CATALYST",),
            )

    if turn_status in ("REJECTED", "INVALIDATED") and turn_direction == expected:
        return (
            MarketReaction.REJECTED,
            (f"M15_{turn_status}_CATALYST_DIRECTION",),
        )

    if flow_direction == expected:
        return (
            MarketReaction.PENDING,
            ("M15_FLOW_ALIGNS_AWAIT_FORMAL_CONFIRMATION",),
        )

    if flow_direction in ("BULLISH", "BEARISH") and flow_direction != expected:
        return (
            MarketReaction.PENDING,
            ("M15_FLOW_CURRENTLY_OPPOSES_CATALYST",),
        )

    return MarketReaction.PENDING, ("M15_REACTION_NOT_DIRECTIONAL_YET",)


def build_dxy_macro_thesis(
    evidence: MacroEvidence,
    *,
    dxy_bias: Mapping[str, Any],
    dxy_m15: Mapping[str, Any],
    dxy_structure: Optional[Mapping[str, Any]] = None,
    h1_reversal_confirmed: bool = False,
    h1_reversal_reason: str = "",
    as_of_ms: Optional[int] = None,
    metadata: Optional[Mapping[str, Any]] = None,
) -> DxyMacroThesis:
    """
    Build an observation-only Strategy-B DXY thesis from canonical inputs.

    Inputs are intentionally supplied by the caller:
      - MacroEvidence from this module.
      - canonical REAL_DXY D1/H4/H1 bias output.
      - canonical REAL_DXY M15 tracker state.
      - canonical trend_sr structure (or dxy_m15['structure']).
      - optional explicit H1 reversal/transition confirmation produced by a
        later approved Strategy-B H1 pattern/structure detector.

    This function never reads Redis, never calls MT5/Finnhub, never calculates
    SR, and never creates PLACE/HOLD/EXIT broker authority.
    """
    now_ms = int(as_of_ms or _now_ms())

    try:
        fundamental = MacroDirection(evidence.fundamental_direction)
    except ValueError:
        fundamental = MacroDirection.UNKNOWN

    expected = _expected_dxy_direction(fundamental)

    d1_direction = _bias_tf_direction(dxy_bias, "D1")
    h4_direction = _bias_tf_direction(dxy_bias, "H4")
    h1_direction = _bias_tf_direction(dxy_bias, "H1")

    flow_direction, turn_status, turn_direction = _m15_fields(dxy_m15)

    structure = (
        dxy_structure
        if isinstance(dxy_structure, Mapping)
        else dxy_m15.get("structure")
        if isinstance(dxy_m15.get("structure"), Mapping)
        else {}
    )

    (
        capacity,
        structure_pressure,
        structure_reason,
        directional_room_atr,
        inside_blocking_zone,
    ) = _structure_for_expected_direction(structure, expected)

    try:
        reaction = MarketReaction(evidence.market_reaction)
    except ValueError:
        reaction = MarketReaction.UNKNOWN

    reaction_reasons: tuple[str, ...] = ()
    if reaction in (MarketReaction.UNKNOWN, MarketReaction.PENDING):
        reaction, reaction_reasons = infer_dxy_market_reaction(evidence, dxy_m15)

    reasons: list[str] = []
    reasons.extend(evidence.reasons)
    reasons.extend(reaction_reasons)

    if expected == "UNKNOWN":
        state = DxyThesisState.OBSERVING
        reasons.append("FUNDAMENTAL_DIRECTION_NOT_ACTIONABLE")

    elif reaction == MarketReaction.REJECTED:
        state = DxyThesisState.REJECTED
        reasons.append("MARKET_REJECTED_CATALYST_DIRECTION")

    elif capacity == StructuralCapacity.BLOCKED:
        state = DxyThesisState.STRUCTURALLY_CONSTRAINED
        reasons.append(f"STRUCTURE_BLOCKED:{structure_reason}")

    elif reaction == MarketReaction.CONFIRMED and bool(h1_reversal_confirmed):
        state = DxyThesisState.REVERSAL_CONFIRMED
        reasons.append("MARKET_REACTION_AND_H1_REVERSAL_CONFIRMED")
        if _clean_text(h1_reversal_reason):
            reasons.append(f"H1_CONFIRMATION:{_clean_text(h1_reversal_reason)}")

    elif reaction == MarketReaction.CONFIRMED:
        state = DxyThesisState.MARKET_REACTION_CONFIRMED
        reasons.append("M15_REACTION_CONFIRMED_AWAIT_H1_REVERSAL_PROOF")

    elif reaction == MarketReaction.PENDING:
        if flow_direction in ("BULLISH", "BEARISH"):
            state = DxyThesisState.REACTION_PENDING
        else:
            state = DxyThesisState.CATALYST_DETECTED
        reasons.append("CATALYST_PRESENT_REACTION_INCOMPLETE")

    else:
        state = DxyThesisState.CATALYST_DETECTED
        reasons.append("CATALYST_PRESENT_REACTION_UNKNOWN")

    if h1_direction == expected:
        reasons.append("H1_BIAS_ALIGNS_WITH_CATALYST")
    elif h1_direction in ("BULLISH", "BEARISH") and expected in ("BULLISH", "BEARISH"):
        reasons.append("H1_BIAS_NOT_YET_ALIGNED_WITH_CATALYST")

    if h4_direction == expected:
        reasons.append("H4_BIAS_ALIGNS_WITH_CATALYST")
    if d1_direction == expected:
        reasons.append("D1_BIAS_ALIGNS_WITH_CATALYST")

    meta = dict(metadata or {})
    meta.update({
        "evidence_source": evidence.source,
        "evidence_source_id": evidence.source_id,
        "dxy_bias_source": _clean_text(dxy_bias.get("source")),
        "dxy_bias_device_id": _clean_text(
            dxy_bias.get("canonical_device_id")
            or dxy_bias.get("device_id")
        ),
        "dxy_m15_source": _clean_text(dxy_m15.get("source")),
        "dxy_m15_device_id": _clean_text(dxy_m15.get("device_id")),
        "structure_source": _clean_text(
            dxy_m15.get("structure_source")
            or structure.get("source")
        ),
    })

    thesis_id = _stable_id(
        "DXY_MACRO_THESIS",
        evidence.evidence_id,
        expected,
        now_ms,
    )

    return DxyMacroThesis(
        thesis_id=thesis_id,
        evidence_id=evidence.evidence_id,
        as_of_ms=now_ms,
        state=state.value,
        fundamental_direction=fundamental.value,
        expected_dxy_direction=expected,
        market_reaction=reaction.value,
        d1_direction=d1_direction,
        h4_direction=h4_direction,
        h1_direction=h1_direction,
        m15_flow_direction=flow_direction,
        m15_turn_status=turn_status,
        m15_turn_direction=turn_direction,
        structure_capacity=capacity.value,
        structure_pressure=structure_pressure,
        structure_pressure_reason=structure_reason,
        directional_room_atr=directional_room_atr,
        inside_blocking_zone=inside_blocking_zone,
        h1_reversal_confirmed=bool(h1_reversal_confirmed),
        confidence=float(evidence.confidence),
        reasons=tuple(dict.fromkeys(reasons)),
        metadata=meta,
    )


def thesis_json(thesis: DxyMacroThesis) -> str:
    """Compact deterministic serialization for thesis snapshots."""
    return json.dumps(
        thesis.to_dict(),
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def evidence_json(evidence: MacroEvidence) -> str:
    """Compact deterministic serialization for JSONL/DB payloads."""
    return json.dumps(
        evidence.to_dict(),
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


__all__ = [
    "EvidenceSource",
    "MacroDirection",
    "MarketReaction",
    "ThesisRelationship",
    "StrategyBEffect",
    "DxyThesisState",
    "StructuralCapacity",
    "MacroEvidence",
    "DxyMacroThesis",
    "classify_mt5_event_direction",
    "build_mt5_event_evidence",
    "build_finnhub_evidence",
    "apply_market_reaction",
    "infer_dxy_market_reaction",
    "build_dxy_macro_thesis",
    "evidence_json",
    "thesis_json",
]
