
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping, Optional

DIRECTIONS = ("BULLISH", "BEARISH")


def _txt(v: Any) -> str:
    return str(v or "").strip()


def _upper(v: Any) -> str:
    return _txt(v).upper()


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


def _mapping(v: Any) -> Mapping[str, Any]:
    return v if isinstance(v, Mapping) else {}


def _field(obj: Any, name: str, default: Any = None) -> Any:
    if isinstance(obj, Mapping):
        return obj.get(name, default)
    return getattr(obj, name, default)


def _expected_dxy_direction_from_fundamental(fundamental: Any) -> str:
    f = _upper(fundamental)
    if f == "USD_BULLISH":
        return "BULLISH"
    if f == "USD_BEARISH":
        return "BEARISH"
    return "UNKNOWN"


def _opposite(direction: str) -> str:
    d = _upper(direction)
    if d == "BULLISH":
        return "BEARISH"
    if d == "BEARISH":
        return "BULLISH"
    return "UNKNOWN"

def _strategy_b_macro_directions_from_d1_leg(
    dxy_bias: Mapping[str, Any],
):
    """
    Strategy-B macro-direction authority.

    The existing D1 structural macro-leg establishes the OLD/current
    macro-leg direction.  Strategy B is reversal-after-maturity, so the
    expected reversal direction is the opposite of that established leg.

    H1/M15 do not create the macro thesis here.  They remain downstream
    reversal/reaction confirmation.
    """
    leg = _mapping(
        _mapping(dxy_bias).get(
            "strategy_b_d1_macro_leg"
        )
    )

    leg_data_ok = bool(leg.get("data_ok") is True)
    old_direction = (
        _upper(leg.get("direction"))
        or "UNKNOWN"
    )

    diag = {
        "model": "D1_STRUCTURAL_MACRO_LEG_DIRECTION",
        "data_ok": leg_data_ok,
        "old_macro_direction": old_direction,
        "state": _upper(leg.get("state")) or "NOT_ESTABLISHED",
        "structural_leg_atr": _num(
            leg.get("structural_leg_atr")
        ),
        "reason": leg.get("reason"),
    }

    if not leg_data_ok:
        return (
            "UNKNOWN",
            "UNKNOWN",
            "D1_STRUCTURAL_MACRO_LEG_UNAVAILABLE",
            diag,
        )

    if old_direction not in DIRECTIONS:
        return (
            "UNKNOWN",
            "UNKNOWN",
            "D1_STRUCTURAL_MACRO_DIRECTION_UNKNOWN",
            diag,
        )

    expected = _opposite(old_direction)

    return (
        old_direction,
        expected,
        "D1_STRUCTURAL_MACRO_LEG_ESTABLISHES_REVERSAL_DIRECTION",
        diag,
    )

def _phase8_market_direction(
    *,
    fundamental: Any,
    dxy_m15: Mapping[str, Any],
):
    """
    Strategy-B Phase 8A shadow direction establishment.

    Fundamental direction remains authoritative when available.

    When fundamental direction is UNKNOWN, market expression may establish
    a research/qualification direction only from a formally CONFIRMED M15
    turn whose direction agrees with canonical market-flow direction.

    Market-flow alone is never sufficient.
    """
    fundamental_expected = _expected_dxy_direction_from_fundamental(fundamental)

    if fundamental_expected in DIRECTIONS:
        return (
            fundamental_expected,
            "FUNDAMENTAL",
            "FUNDAMENTAL_DIRECTION_AVAILABLE",
            {
                "fundamental_direction": _upper(fundamental) or "UNKNOWN",
                "fundamental_expected_direction": fundamental_expected,
            },
        )

    m15 = _mapping(dxy_m15)
    turn = _mapping(m15.get("turn"))

    status = _upper(
        m15.get("status")
        or m15.get("turn_status")
        or turn.get("status")
    ) or "UNKNOWN"

    turn_direction = _upper(
        m15.get("direction")
        or m15.get("turn_direction")
        or turn.get("direction")
    ) or "UNKNOWN"

    flow_direction = _upper(
        _mapping(_mapping(m15.get("features")).get("market_flow")).get("direction")
        or m15.get("flow_direction")
    ) or "UNKNOWN"

    diag = {
        "fundamental_direction": _upper(fundamental) or "UNKNOWN",
        "fundamental_expected_direction": fundamental_expected,
        "m15_status": status,
        "m15_turn_direction": turn_direction,
        "m15_flow_direction": flow_direction,
    }

    if (
        status == "CONFIRMED"
        and turn_direction in DIRECTIONS
        and flow_direction == turn_direction
    ):
        return (
            turn_direction,
            "MARKET_EXPRESSION",
            "M15_CONFIRMED_TURN_AND_FLOW_ESTABLISH_CANDIDATE_DIRECTION",
            diag,
        )

    if (
        status == "CONFIRMED"
        and turn_direction in DIRECTIONS
        and flow_direction in DIRECTIONS
        and flow_direction != turn_direction
    ):
        return (
            "UNKNOWN",
            "NONE",
            "M15_CONFIRMED_TURN_FLOW_CONFLICT",
            diag,
        )

    if status in ("REJECTED", "INVALIDATED"):
        return (
            "UNKNOWN",
            "NONE",
            f"M15_{status}_CANNOT_ESTABLISH_DIRECTION",
            diag,
        )

    if flow_direction in DIRECTIONS:
        return (
            "UNKNOWN",
            "NONE",
            "M15_FLOW_ONLY_AWAIT_FORMAL_CONFIRMATION",
            diag,
        )

    return (
        "UNKNOWN",
        "NONE",
        "MARKET_DIRECTION_NOT_ESTABLISHED",
        diag,
    )

@dataclass(frozen=True)
class StrategyBMacroConfig:
    d1_extended_net_atr: Optional[float] = None
    h4_extended_net_atr: Optional[float] = None
    d1_mature_net_atr: Optional[float] = None
    h4_mature_net_atr: Optional[float] = None
    require_d1_h4_old_leg_alignment: bool = True
    require_h1_swing_alignment: bool = True
    h1_catalyst_pre_tolerance_ms: int = 0


@dataclass(frozen=True)
class StrategyBMacroQualification:
    schema_version: int
    model: str
    expected_dxy_direction: str
    direction_source: str
    direction_reason: str
    old_macro_direction: str
    macro_leg_established: bool
    macro_leg_reason: str
    maturity_state: str
    maturity_qualified: bool
    maturity_reason: str
    location_qualified: bool
    location_reason: str
    location_tf: Optional[str]
    location_role: Optional[str]
    location_distance_atr: Optional[float]
    market_reaction_confirmed: bool
    market_reaction_reason: str
    market_reaction_status: str
    market_reaction_direction: str
    reaction_capacity_ok: bool
    reaction_capacity_state: str
    reaction_capacity_reason: str
    h1_reversal_confirmed: bool
    h1_reversal_reason: str
    h1_turn_ms: int
    h1_turn_direction: str
    h1_short_state: str
    h1_confidence: int
    h1_swing_state: str
    catalyst_first_seen_ms: int
    macro_qualified: bool
    qualification_state: str
    reasons: tuple[str, ...]
    diagnostics: Mapping[str, Any]
    observation_only: bool = True
    execution_authority: bool = False
    strategy_a_mutation: bool = False
    

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _tf_row(dxy_bias: Mapping[str, Any], tf: str) -> Mapping[str, Any]:
    return _mapping(_mapping(dxy_bias).get("timeframes")).get(tf, {}) or {}


def _tf_direction(dxy_bias: Mapping[str, Any], tf: str) -> str:
    return _upper(_mapping(_tf_row(dxy_bias, tf)).get("direction")) or "UNKNOWN"


def _tf_features(dxy_bias: Mapping[str, Any], tf: str) -> Mapping[str, Any]:
    return _mapping(_mapping(_tf_row(dxy_bias, tf)).get("features"))


def _tf_structure_state(dxy_bias: Mapping[str, Any], tf: str) -> str:
    return _upper(_mapping(_mapping(_tf_row(dxy_bias, tf)).get("structure")).get("state")) or "UNKNOWN"


def _signed_net_atr(dxy_bias: Mapping[str, Any], tf: str) -> Optional[float]:
    return _num(_tf_features(dxy_bias, tf).get("net_atr"))


def _direction_matches_signed_move(direction: str, value: Optional[float]) -> bool:
    if value is None:
        return False
    d = _upper(direction)
    return (d == "BULLISH" and value > 0) or (d == "BEARISH" and value < 0)


def _macro_leg(dxy_bias: Mapping[str, Any], *, old_direction: str, cfg: StrategyBMacroConfig):
    d1 = _tf_row(dxy_bias, "D1")
    h4 = _tf_row(dxy_bias, "H4")
    d1_dir = _tf_direction(dxy_bias, "D1")
    h4_dir = _tf_direction(dxy_bias, "H4")
    d1_ok = bool(d1.get("data_ok") is True and d1_dir == old_direction)
    h4_ok = bool(h4.get("data_ok") is True and h4_dir == old_direction)
    diag = {
        "d1_direction": d1_dir,
        "h4_direction": h4_dir,
        "d1_data_ok": bool(d1.get("data_ok") is True),
        "h4_data_ok": bool(h4.get("data_ok") is True),
    }

    if old_direction not in DIRECTIONS:
        return False, "OLD_MACRO_DIRECTION_UNKNOWN", diag

    if cfg.require_d1_h4_old_leg_alignment:
        ok = d1_ok and h4_ok
        return ok, ("D1_H4_OLD_MACRO_LEG_ALIGNED" if ok else "D1_H4_OLD_MACRO_LEG_NOT_ALIGNED"), diag

    ok = h4_ok and d1_dir != _opposite(old_direction)
    return ok, ("H4_OLD_LEG_D1_NOT_OPPOSING" if ok else "OLD_MACRO_LEG_NOT_ESTABLISHED"), diag


def _maturity(
    dxy_bias: Mapping[str, Any],
    *,
    old_direction: str,
    macro_leg_established: bool,
    cfg: StrategyBMacroConfig,
):
    leg = _mapping(
        _mapping(dxy_bias).get(
            "strategy_b_d1_macro_leg"
        )
    )

    leg_state = _upper(leg.get("state")) or "NOT_ESTABLISHED"
    leg_direction = (
        _upper(leg.get("direction"))
        or "UNKNOWN"
    )
    leg_atr = _num(leg.get("structural_leg_atr"))
    leg_data_ok = bool(leg.get("data_ok") is True)

    diag = {
        "model": "D1_STRUCTURAL_MACRO_LEG",
        "data_ok": leg_data_ok,
        "state": leg_state,
        "direction": leg_direction,
        "structural_leg_atr": leg_atr,
        "origin_type": leg.get("origin_type"),
        "origin_index": leg.get("origin_index"),
        "origin_time_ms": leg.get("origin_time_ms"),
        "origin_price": leg.get("origin_price"),
        "origin_confirmed_index": (
            leg.get("origin_confirmed_index")
        ),
        "origin_confirmed_time_ms": (
            leg.get("origin_confirmed_time_ms")
        ),
        "current_time_ms": leg.get("current_time_ms"),
        "current_price": leg.get("current_price"),
        "atr": leg.get("atr"),
        "calculator_reason": leg.get("reason"),
        "thresholds": {
            "mature_atr": 3.0,
            "extended_atr": 4.0,
        },
    }

    if not leg_data_ok:
        return (
            "NOT_ESTABLISHED",
            False,
            "D1_STRUCTURAL_MACRO_LEG_UNAVAILABLE",
            diag,
        )

    if not macro_leg_established:
        return (
            "NOT_ESTABLISHED",
            False,
            "OLD_MACRO_LEG_NOT_ESTABLISHED",
            diag,
        )

    if old_direction not in DIRECTIONS:
        return (
            "NOT_ESTABLISHED",
            False,
            "OLD_MACRO_DIRECTION_UNKNOWN",
            diag,
        )

    if leg_direction != old_direction:
        return (
            "NOT_ESTABLISHED",
            False,
            "D1_STRUCTURAL_LEG_DIRECTION_MISMATCH",
            diag,
        )

    if leg_atr is None:
        return (
            "NOT_ESTABLISHED",
            False,
            "D1_STRUCTURAL_LEG_ATR_MISSING",
            diag,
        )

    if leg_state == "EXTENDED":
        return (
            "EXTENDED",
            True,
            "D1_STRUCTURAL_LEG_EXTENDED",
            diag,
        )

    if leg_state == "MATURE":
        return (
            "MATURE",
            True,
            "D1_STRUCTURAL_LEG_MATURE",
            diag,
        )

    if leg_state == "EXPANDING":
        return (
            "EXPANDING",
            False,
            "D1_STRUCTURAL_LEG_NOT_YET_MATURE",
            diag,
        )

    return (
        "NOT_ESTABLISHED",
        False,
        "D1_STRUCTURAL_LEG_STATE_INVALID",
        diag,
    )


def _level_tf_role(level: Any):
    row = _mapping(level)
    if not row:
        return None, None, None
    tf = _upper(row.get("tf")) or None
    role = _upper(row.get("role") or row.get("kind")) or None
    dist = _num(row.get("distance_atr"))
    return tf, role, dist


def _location(structure: Mapping[str, Any], *, expected_direction: str):
    s = _mapping(structure)
    if expected_direction == "BEARISH":
        candidates = [s.get("inside_resistance"), s.get("nearest_resistance"), s.get("best_resistance")]
        wanted_role = "RESISTANCE"
    elif expected_direction == "BULLISH":
        candidates = [s.get("inside_support"), s.get("nearest_support"), s.get("best_support")]
        wanted_role = "SUPPORT"
    else:
        return False, "EXPECTED_DIRECTION_UNKNOWN", None, None, None, {}

    seen = []
    for candidate in candidates:
        tf, role, dist = _level_tf_role(candidate)
        if not tf:
            continue
        seen.append({"tf": tf, "role": role, "distance_atr": dist})
        if tf in ("D1", "H4") and role in (wanted_role, None):
            return True, f"MACRO_LOCATION_{tf}_{wanted_role}", tf, wanted_role, dist, {"candidates": seen}

    return False, f"NO_D1_H4_{wanted_role}_LOCATION", None, wanted_role, None, {"candidates": seen}


def _catalyst_first_seen_ms(evidence: Any) -> int:
    return _int(
        _field(evidence, "first_seen_at_ms")
        or _field(evidence, "first_result_seen_ms")
        or _field(evidence, "observed_at_ms")
        or _field(evidence, "event_time_ms")
    )


def _reaction_directional_capacity(
    structure: Mapping[str, Any],
    *,
    expected_direction: str,
):
    """
    Strategy-B Phase 8A.2 reaction directional-capacity gate.

    This is separate from the existing D1/H4 macro reversal-location gate.

    Purpose:
      After a candidate event-expression direction exists, determine whether
      canonical DXY structure gives that direction immediate structural room.

    V1 intentionally has no calibrated positive ATR threshold.

    Hard BLOCK only when:
      - price is inside the blocking opposite zone, AND
      - directional room is known and <= 0.

    Everything else is either CLEAR, CONSTRAINED, or UNKNOWN for telemetry
    and later genuine-event calibration.
    """
    expected = _upper(expected_direction)
    s = _mapping(structure)

    if expected not in DIRECTIONS:
        return False, "UNKNOWN", "REACTION_DIRECTION_UNKNOWN", {
            "expected_direction": expected,
            "directional_room_atr": None,
            "inside_blocking_zone": None,
            "structure_pressure": "UNKNOWN",
            "structure_reason": None,
        }

    # Canonical structure may expose these directly or through a nested
    # structure/safety object. Read only; do not calculate SR here.
    nested = _mapping(s.get("structure"))
    safety = _mapping(s.get("sr_safety"))

    def first(*values):
        for value in values:
            if value is not None:
                return value
        return None

    if expected == "BULLISH":
        room_raw = first(
            s.get("room_up_atr"),
            nested.get("room_up_atr"),
            safety.get("room_up_atr"),
            s.get("bullish_room_atr"),
            nested.get("bullish_room_atr"),
        )
        inside_raw = first(
            s.get("inside_resistance"),
            nested.get("inside_resistance"),
            safety.get("inside_resistance"),
            _mapping(s.get("nearest_resistance")).get("inside"),
            _mapping(nested.get("nearest_resistance")).get("inside"),
        )
        blocking_role = "RESISTANCE"
    else:
        room_raw = first(
            s.get("room_down_atr"),
            nested.get("room_down_atr"),
            safety.get("room_down_atr"),
            s.get("bearish_room_atr"),
            nested.get("bearish_room_atr"),
        )
        inside_raw = first(
            s.get("inside_support"),
            nested.get("inside_support"),
            safety.get("inside_support"),
            _mapping(s.get("nearest_support")).get("inside"),
            _mapping(nested.get("nearest_support")).get("inside"),
        )
        blocking_role = "SUPPORT"

    pressure = _upper(
        first(
            s.get("structure_pressure"),
            nested.get("structure_pressure"),
            safety.get("structure_pressure"),
        )
    ) or "UNKNOWN"

    structure_reason = first(
        s.get("reason"),
        nested.get("reason"),
        safety.get("reason"),
    )

    try:
        room = float(room_raw) if room_raw is not None else None
    except (TypeError, ValueError):
        room = None

    if isinstance(inside_raw, Mapping):
        # Production trend_sr/live-input contract:
        # inside_support / inside_resistance are level mappings.
        inside_blocking = bool(inside_raw)
    elif isinstance(inside_raw, bool):
        # Retain boolean compatibility for deterministic fixtures and
        # any explicitly normalized future producer.
        inside_blocking = inside_raw
    elif inside_raw is None:
        inside_blocking = None
    else:
        inside_blocking = _upper(inside_raw) in (
            "1", "TRUE", "YES", "Y", "INSIDE",
        )

    diag = {

        "blocking_level": (
            dict(inside_raw)
            if isinstance(inside_raw, Mapping)
            else None
        ),
        "expected_direction": expected,
        "blocking_role": blocking_role,
        "directional_room_atr": room,
        "inside_blocking_zone": inside_blocking,
        "structure_pressure": pressure,
        "structure_reason": structure_reason,
    }

    

    # Deterministic hard-block only. No invented positive ATR threshold.
    if inside_blocking is True and room is not None and room <= 0.0:
        return (
            False,
            "BLOCKED",
            f"REACTION_{expected}_BLOCKED_INSIDE_{blocking_role}_NO_ROOM",
            diag,
        )

    # Missing structural evidence must fail closed.
    if room is None or inside_blocking is None:
        return (
            False,
            "UNKNOWN",
            "REACTION_DIRECTIONAL_CAPACITY_INCOMPLETE",
            diag,
        )

    # Preserve pressure as calibration evidence. Do not turn EXTREME into
    # another hard veto until genuine-event evidence calibrates it.
    if inside_blocking is True:
        return (
            True,
            "CONSTRAINED",
            f"REACTION_{expected}_INSIDE_{blocking_role}_WITH_POSITIVE_ROOM",
            diag,
        )

    return (
        True,
        "CLEAR",
        f"REACTION_{expected}_HAS_DIRECTIONAL_ROOM",
        diag,
    )

def _m15_market_reaction(*, expected_direction: str, dxy_m15: Mapping[str, Any]):
    m15 = _mapping(dxy_m15)
    turn = _mapping(m15.get("turn"))

    status = _upper(m15.get("status") or m15.get("turn_status") or turn.get("status")) or "UNKNOWN"
    direction = _upper(m15.get("direction") or m15.get("turn_direction") or turn.get("direction")) or "UNKNOWN"
    flow_direction = _upper(
        _mapping(_mapping(m15.get("features")).get("market_flow")).get("direction")
        or m15.get("flow_direction")
    ) or "UNKNOWN"

    diag = {"status": status, "direction": direction, "flow_direction": flow_direction}

    if expected_direction not in DIRECTIONS:
        return False, "MARKET_REACTION_EXPECTED_DIRECTION_UNKNOWN", status, direction, diag
    if status == "CONFIRMED" and direction == expected_direction:
        return True, "M15_CONFIRMED_TURN_ACCEPTS_CATALYST", status, direction, diag
    if status == "CONFIRMED" and direction in DIRECTIONS and direction != expected_direction:
        return False, "M15_CONFIRMED_TURN_REJECTS_CATALYST", status, direction, diag
    if status in ("REJECTED", "INVALIDATED") and direction == expected_direction:
        return False, f"M15_{status}_CATALYST_DIRECTION", status, direction, diag
    if flow_direction == expected_direction:
        return False, "M15_FLOW_ALIGNS_AWAIT_FORMAL_REACTION_CONFIRMATION", status, direction, diag
    if flow_direction in DIRECTIONS and flow_direction != expected_direction:
        return False, "M15_FLOW_CURRENTLY_OPPOSES_CATALYST", status, direction, diag
    return False, "M15_MARKET_REACTION_PENDING", status, direction, diag


def _h1_reversal(*, expected_direction: str, evidence: Any, dxy_bias: Mapping[str, Any], dxy_h1_turn: Mapping[str, Any], cfg: StrategyBMacroConfig):
    turn = _mapping(dxy_h1_turn)
    short_state = _upper(turn.get("short_state")) or "UNKNOWN"
    last_turn_direction = _upper(turn.get("last_turn_direction")) or "UNKNOWN"
    last_turn_ms = _int(turn.get("last_turn_ms"))
    confidence = _int(turn.get("confidence"))
    h1_swing_state = _tf_structure_state(dxy_bias, "H1")
    catalyst_ms = _catalyst_first_seen_ms(evidence)

    diag = {
        "short_state": short_state,
        "last_turn_direction": last_turn_direction,
        "last_turn_ms": last_turn_ms,
        "confidence": confidence,
        "recent_net_atr": _num(turn.get("recent_net_atr")),
        "prior_net_atr": _num(turn.get("prior_net_atr")),
        "up_steps": _int(turn.get("up_steps")),
        "down_steps": _int(turn.get("down_steps")),
        "bullish_bodies": _int(turn.get("bullish_bodies")),
        "bearish_bodies": _int(turn.get("bearish_bodies")),
        "last_evaluated_bar_close_ms": _int(turn.get("last_evaluated_bar_close_ms")),
        "h1_swing_state": h1_swing_state,
        "catalyst_first_seen_ms": catalyst_ms,
    }

    if expected_direction not in DIRECTIONS:
        return False, "H1_EXPECTED_DIRECTION_UNKNOWN", diag
    if short_state != expected_direction:
        return False, "H1_CURRENT_TURN_NOT_ALIGNED", diag
    if last_turn_direction != expected_direction:
        return False, "H1_LAST_TURN_NOT_ALIGNED", diag
    if last_turn_ms <= 0:
        return False, "H1_TURN_TIME_MISSING", diag
    if catalyst_ms <= 0:
        return False, "CATALYST_FIRST_SEEN_TIME_MISSING", diag

    earliest_allowed = catalyst_ms - max(0, int(cfg.h1_catalyst_pre_tolerance_ms))
    if last_turn_ms < earliest_allowed:
        return False, "H1_TURN_PREDATES_CATALYST", diag

    if cfg.require_h1_swing_alignment and h1_swing_state != expected_direction:
        return False, "H1_SWING_STRUCTURE_NOT_ALIGNED", diag

    return True, "H1_EARLY_TURN_PLUS_SWING_STRUCTURE_CONFIRMED_AFTER_CATALYST", diag


def evaluate_strategy_b_macro_qualification(
    evidence: Any,
    *,
    dxy_bias: Mapping[str, Any],
    dxy_m15: Mapping[str, Any],
    dxy_h1_turn: Mapping[str, Any],
    dxy_structure: Optional[Mapping[str, Any]] = None,
    config: Optional[StrategyBMacroConfig] = None,
) -> StrategyBMacroQualification:
    cfg = config or StrategyBMacroConfig()
    reasons: list[str] = []

    fundamental = _field(
        evidence,
        "fundamental_direction",
        "UNKNOWN",
    )

    (
        old_direction,
        expected,
        direction_reason,
        direction_diag,
    ) = _strategy_b_macro_directions_from_d1_leg(
        dxy_bias
    )

    direction_source = (
        "D1_STRUCTURAL_MACRO_LEG"
        if expected in DIRECTIONS
        else "NONE"
    )

    catalyst_expected = (
        _expected_dxy_direction_from_fundamental(
            fundamental
        )
    )

    direction_diag = {
        **dict(direction_diag),
        "fundamental_direction": (
            _upper(fundamental)
            or "UNKNOWN"
        ),
        "fundamental_expected_direction": (
            catalyst_expected
        ),
        "expected_reversal_direction": expected,
    }

    catalyst_ms = _catalyst_first_seen_ms(
        evidence
    )

    reasons.append(direction_reason)

    if expected not in DIRECTIONS:
        catalyst_direction_ok = False
        catalyst_direction_reason = (
            "REVERSAL_DIRECTION_NOT_ESTABLISHED"
        )

    elif catalyst_ms <= 0:
        catalyst_direction_ok = False
        catalyst_direction_reason = (
            "CATALYST_FIRST_SEEN_TIME_MISSING"
        )

    elif (
        catalyst_expected in DIRECTIONS
        and catalyst_expected != expected
    ):
        catalyst_direction_ok = False
        catalyst_direction_reason = (
            "CATALYST_DIRECTION_OPPOSES_EXPECTED_REVERSAL"
        )

    elif catalyst_expected in DIRECTIONS:
        catalyst_direction_ok = True
        catalyst_direction_reason = (
            "CATALYST_DIRECTION_ALIGNED_WITH_EXPECTED_REVERSAL"
        )

    else:
        catalyst_direction_ok = True
        catalyst_direction_reason = (
            "CATALYST_PRESENT_DIRECTION_PENDING_MARKET_CONFIRMATION"
        )

    reasons.append(catalyst_direction_reason)

    direction_diag = {
        **dict(direction_diag),
        "catalyst_direction_ok": (
            catalyst_direction_ok
        ),
        "catalyst_direction_reason": (
            catalyst_direction_reason
        ),
    }

    macro_leg_ok, macro_leg_reason, macro_diag = _macro_leg(dxy_bias, old_direction=old_direction, cfg=cfg)
    reasons.append(macro_leg_reason)

    maturity_state, maturity_ok, maturity_reason, maturity_diag = _maturity(
        dxy_bias,
        old_direction=old_direction,
        macro_leg_established=macro_leg_ok,
        cfg=cfg,
    )
    reasons.append(maturity_reason)

    structure = _mapping(dxy_structure) if isinstance(dxy_structure, Mapping) else _mapping(_mapping(dxy_m15).get("structure"))
    location_ok, location_reason, location_tf, location_role, location_distance_atr, location_diag = _location(
        structure,
        expected_direction=expected,
    )
    reasons.append(location_reason)

    reaction_ok, reaction_reason, reaction_status, reaction_direction, reaction_diag = _m15_market_reaction(
        expected_direction=expected,
        dxy_m15=dxy_m15,
    )
    reasons.append(reaction_reason)

    capacity_ok, capacity_state, capacity_reason, capacity_diag = (
        _reaction_directional_capacity(
            structure,
            expected_direction=expected,
        )
    )
    reasons.append(capacity_reason)

    h1_ok, h1_reason, h1_diag = _h1_reversal(
        expected_direction=expected,
        evidence=evidence,
        dxy_bias=dxy_bias,
        dxy_h1_turn=dxy_h1_turn,
        cfg=cfg,
    )
    reasons.append(h1_reason)

    if expected not in DIRECTIONS:
        state = "WAIT_MACRO_DIRECTION"

    elif not macro_leg_ok:
        state = "WAIT_MACRO_LEG"

    elif not maturity_ok:
        state = (
            "WAIT_MATURITY_CALIBRATION"
            if maturity_state == "CALIBRATION_REQUIRED"
            else "WAIT_MATURITY"
        )

    elif not location_ok:
        state = "WAIT_HTF_LOCATION"

    elif not catalyst_direction_ok:
        state = "WAIT_CATALYST_DIRECTION"

    elif not reaction_ok:
        state = "WAIT_MARKET_REACTION"

    elif not capacity_ok:
        state = "WAIT_REACTION_LOCATION"

    elif not h1_ok:
        state = "WAIT_H1_REVERSAL"

    else:
        state = "MACRO_QUALIFIED"

    macro_qualified = bool(
        expected in DIRECTIONS
        and macro_leg_ok
        and maturity_ok
        and location_ok
        and catalyst_direction_ok
        and reaction_ok
        and capacity_ok
        and h1_ok
    )

    return StrategyBMacroQualification(
        schema_version=3,
        model="STRATEGY_B_MACRO_QUALIFICATION_V3_PHASE8A2",
        expected_dxy_direction=expected,
        direction_source=direction_source,
        direction_reason=direction_reason,
        old_macro_direction=old_direction,
        macro_leg_established=macro_leg_ok,
        macro_leg_reason=macro_leg_reason,
        maturity_state=maturity_state,
        maturity_qualified=maturity_ok,
        maturity_reason=maturity_reason,
        location_qualified=location_ok,
        location_reason=location_reason,
        location_tf=location_tf,
        location_role=location_role,
        location_distance_atr=location_distance_atr,
        market_reaction_confirmed=reaction_ok,
        market_reaction_reason=reaction_reason,
        market_reaction_status=reaction_status,
        market_reaction_direction=reaction_direction,
        reaction_capacity_ok=capacity_ok,
        reaction_capacity_state=capacity_state,
        reaction_capacity_reason=capacity_reason,
        h1_reversal_confirmed=h1_ok,
        h1_reversal_reason=h1_reason,
        h1_turn_ms=_int(h1_diag.get("last_turn_ms")),
        h1_turn_direction=_upper(h1_diag.get("last_turn_direction")) or "UNKNOWN",
        h1_short_state=_upper(h1_diag.get("short_state")) or "UNKNOWN",
        h1_confidence=_int(h1_diag.get("confidence")),
        h1_swing_state=_upper(h1_diag.get("h1_swing_state")) or "UNKNOWN",
        catalyst_first_seen_ms=catalyst_ms,
        macro_qualified=macro_qualified,
        qualification_state=state,
        reasons=tuple(dict.fromkeys(reasons)),
        diagnostics={
            "macro_leg": macro_diag,
            "direction": direction_diag,
            "maturity": maturity_diag,
            "location": location_diag,
            "market_reaction": reaction_diag,
            "reaction_capacity": capacity_diag,
            "h1_reversal": h1_diag,
        },
    )


__all__ = [
    "StrategyBMacroConfig",
    "StrategyBMacroQualification",
    "evaluate_strategy_b_macro_qualification",
]
