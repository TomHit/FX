
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


def _maturity(dxy_bias: Mapping[str, Any], *, old_direction: str, macro_leg_established: bool, cfg: StrategyBMacroConfig):
    d1_net = _signed_net_atr(dxy_bias, "D1")
    h4_net = _signed_net_atr(dxy_bias, "H4")
    diag = {
        "d1_net_atr": d1_net,
        "h4_net_atr": h4_net,
        "d1_regime": _upper(_tf_row(dxy_bias, "D1").get("regime")) or "UNKNOWN",
        "h4_regime": _upper(_tf_row(dxy_bias, "H4").get("regime")) or "UNKNOWN",
        "thresholds": {
            "d1_extended_net_atr": cfg.d1_extended_net_atr,
            "h4_extended_net_atr": cfg.h4_extended_net_atr,
            "d1_mature_net_atr": cfg.d1_mature_net_atr,
            "h4_mature_net_atr": cfg.h4_mature_net_atr,
        },
    }

    if not macro_leg_established:
        return "NOT_ESTABLISHED", False, "OLD_MACRO_LEG_NOT_ESTABLISHED", diag

    thresholds = (
        cfg.d1_extended_net_atr,
        cfg.h4_extended_net_atr,
        cfg.d1_mature_net_atr,
        cfg.h4_mature_net_atr,
    )
    if any(v is None for v in thresholds):
        return "CALIBRATION_REQUIRED", False, "STRATEGY_B_MATURITY_THRESHOLDS_NOT_CALIBRATED", diag

    if not (_direction_matches_signed_move(old_direction, d1_net) and _direction_matches_signed_move(old_direction, h4_net)):
        return "NOT_ESTABLISHED", False, "D1_H4_NET_ATR_NOT_OLD_LEG_DIRECTION", diag

    d1_abs = abs(float(d1_net))
    h4_abs = abs(float(h4_net))

    if d1_abs >= float(cfg.d1_mature_net_atr) and h4_abs >= float(cfg.h4_mature_net_atr):
        return "MATURE", True, "D1_H4_MATURE_THRESHOLDS_MET", diag
    if d1_abs >= float(cfg.d1_extended_net_atr) and h4_abs >= float(cfg.h4_extended_net_atr):
        return "EXTENDED", False, "D1_H4_EXTENDED_AWAIT_MATURITY", diag
    return "EXPANDING", False, "D1_H4_MOVE_NOT_YET_EXTENDED", diag


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

    fundamental = _field(evidence, "fundamental_direction", "UNKNOWN")
    expected = _expected_dxy_direction_from_fundamental(fundamental)
    old_direction = _opposite(expected)
    catalyst_ms = _catalyst_first_seen_ms(evidence)

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

    h1_ok, h1_reason, h1_diag = _h1_reversal(
        expected_direction=expected,
        evidence=evidence,
        dxy_bias=dxy_bias,
        dxy_h1_turn=dxy_h1_turn,
        cfg=cfg,
    )
    reasons.append(h1_reason)

    if expected not in DIRECTIONS:
        state = "WAIT_CATALYST_DIRECTION"
    elif not macro_leg_ok:
        state = "WAIT_MACRO_LEG"
    elif not maturity_ok:
        state = "WAIT_MATURITY_CALIBRATION" if maturity_state == "CALIBRATION_REQUIRED" else "WAIT_MATURITY"
    elif not location_ok:
        state = "WAIT_HTF_LOCATION"
    elif not reaction_ok:
        state = "WAIT_MARKET_REACTION"
    elif not h1_ok:
        state = "WAIT_H1_REVERSAL"
    else:
        state = "MACRO_QUALIFIED"

    macro_qualified = bool(
        expected in DIRECTIONS
        and macro_leg_ok
        and maturity_ok
        and location_ok
        and reaction_ok
        and h1_ok
    )

    return StrategyBMacroQualification(
        schema_version=2,
        model="STRATEGY_B_MACRO_QUALIFICATION_V2",
        expected_dxy_direction=expected,
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
            "maturity": maturity_diag,
            "location": location_diag,
            "market_reaction": reaction_diag,
            "h1_reversal": h1_diag,
        },
    )


__all__ = [
    "StrategyBMacroConfig",
    "StrategyBMacroQualification",
    "evaluate_strategy_b_macro_qualification",
]
