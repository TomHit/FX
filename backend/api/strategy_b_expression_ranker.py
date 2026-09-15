
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping, Optional, Sequence


USD_BEARISH_EXPRESSIONS = {
    "EURUSD": "BUY",
    "GBPUSD": "BUY",
    "XAUUSD": "BUY",
    "USDJPY": "SELL",
    "USDCHF": "SELL",
    "USDCAD": "SELL",
}

USD_BULLISH_EXPRESSIONS = {
    symbol: ("SELL" if side == "BUY" else "BUY")
    for symbol, side in USD_BEARISH_EXPRESSIONS.items()
}


def _upper(v: Any) -> str:
    return str(v or "").strip().upper()


def _num(v: Any) -> Optional[float]:
    try:
        x = float(v)
        return x if x == x else None
    except Exception:
        return None


def _clip01(v: Any) -> Optional[float]:
    x = _num(v)
    if x is None:
        return None
    return max(0.0, min(1.0, x))


def _mapping(v: Any) -> Mapping[str, Any]:
    return v if isinstance(v, Mapping) else {}


@dataclass(frozen=True)
class StrategyBExpressionRankConfig:
    min_directional_room_atr: Optional[float] = None
    weight_htf_location: Optional[float] = None
    weight_major_structure: Optional[float] = None
    weight_h1_transition: Optional[float] = None
    weight_volatility: Optional[float] = None
    weight_directional_room: Optional[float] = None
    weight_move_remaining: Optional[float] = None
    weight_liquidity: Optional[float] = None
    weight_spread_efficiency: Optional[float] = None
    weight_non_usd_driver: Optional[float] = None
    room_full_score_atr: Optional[float] = None
    move_consumed_full_penalty_atr: Optional[float] = None
    spread_full_penalty_atr: Optional[float] = None


@dataclass(frozen=True)
class RankedExpression:
    rank: Optional[int]
    symbol: str
    side: str
    eligible: bool
    score: Optional[float]
    veto_reasons: tuple[str, ...]
    reasons: tuple[str, ...]
    components: Mapping[str, Any]
    raw: Mapping[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class StrategyBExpressionRanking:
    schema_version: int
    model: str
    expected_dxy_direction: str
    usd_thesis: str
    calibrated: bool
    calibration_reason: str
    best_symbol: Optional[str]
    best_side: Optional[str]
    best_score: Optional[float]
    decision: str
    reason: str
    ranked: tuple[RankedExpression, ...]
    observation_only: bool = True
    execution_authority: bool = False
    strategy_a_mutation: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _approved_map(expected_dxy_direction: str) -> tuple[str, Mapping[str, str]]:
    d = _upper(expected_dxy_direction)
    if d == "BEARISH":
        return "USD_BEARISH", USD_BEARISH_EXPRESSIONS
    if d == "BULLISH":
        return "USD_BULLISH", USD_BULLISH_EXPRESSIONS
    return "UNKNOWN", {}


def _config_complete(cfg: StrategyBExpressionRankConfig) -> tuple[bool, str]:
    required = {
        "min_directional_room_atr": cfg.min_directional_room_atr,
        "weight_htf_location": cfg.weight_htf_location,
        "weight_major_structure": cfg.weight_major_structure,
        "weight_h1_transition": cfg.weight_h1_transition,
        "weight_volatility": cfg.weight_volatility,
        "weight_directional_room": cfg.weight_directional_room,
        "weight_move_remaining": cfg.weight_move_remaining,
        "weight_liquidity": cfg.weight_liquidity,
        "weight_spread_efficiency": cfg.weight_spread_efficiency,
        "weight_non_usd_driver": cfg.weight_non_usd_driver,
        "room_full_score_atr": cfg.room_full_score_atr,
        "move_consumed_full_penalty_atr": cfg.move_consumed_full_penalty_atr,
        "spread_full_penalty_atr": cfg.spread_full_penalty_atr,
    }
    missing = [k for k, v in required.items() if v is None]
    if missing:
        return False, "CALIBRATION_MISSING:" + ",".join(missing)

    weights = [
        cfg.weight_htf_location,
        cfg.weight_major_structure,
        cfg.weight_h1_transition,
        cfg.weight_volatility,
        cfg.weight_directional_room,
        cfg.weight_move_remaining,
        cfg.weight_liquidity,
        cfg.weight_spread_efficiency,
        cfg.weight_non_usd_driver,
    ]
    if any(float(w) < 0 for w in weights):
        return False, "CALIBRATION_INVALID_NEGATIVE_WEIGHT"
    if sum(float(w) for w in weights) <= 0:
        return False, "CALIBRATION_INVALID_ZERO_WEIGHT_SUM"
    if float(cfg.min_directional_room_atr) < 0:
        return False, "CALIBRATION_INVALID_MIN_ROOM"
    if float(cfg.room_full_score_atr) <= 0:
        return False, "CALIBRATION_INVALID_ROOM_CAP"
    if float(cfg.move_consumed_full_penalty_atr) <= 0:
        return False, "CALIBRATION_INVALID_MOVE_CAP"
    if float(cfg.spread_full_penalty_atr) <= 0:
        return False, "CALIBRATION_INVALID_SPREAD_CAP"
    return True, "OK"


def _normalized_components(row: Mapping[str, Any], cfg: StrategyBExpressionRankConfig) -> Optional[dict[str, float]]:
    loc = _clip01(row.get("htf_location_quality"))
    struct = _clip01(row.get("major_structure_quality"))
    h1 = _clip01(row.get("h1_transition_quality"))
    vol = _clip01(row.get("volatility_quality"))
    liq = _clip01(row.get("liquidity_quality"))
    nonusd_raw = _num(row.get("non_usd_driver_alignment"))
    room = _num(row.get("directional_room_atr"))
    consumed = _num(row.get("move_consumed_atr"))
    spread = _num(row.get("spread_cost_atr"))

    mandatory = (loc, struct, h1, vol, liq, nonusd_raw, room, consumed, spread)
    if any(v is None for v in mandatory):
        return None

    nonusd = max(0.0, min(1.0, (float(nonusd_raw) + 1.0) / 2.0))
    room_score = max(0.0, min(1.0, float(room) / float(cfg.room_full_score_atr)))
    move_remaining = 1.0 - max(
        0.0,
        min(1.0, float(consumed) / float(cfg.move_consumed_full_penalty_atr)),
    )
    spread_eff = 1.0 - max(
        0.0,
        min(1.0, float(spread) / float(cfg.spread_full_penalty_atr)),
    )

    return {
        "htf_location": float(loc),
        "major_structure": float(struct),
        "h1_transition": float(h1),
        "volatility": float(vol),
        "directional_room": room_score,
        "move_remaining": move_remaining,
        "liquidity": float(liq),
        "spread_efficiency": spread_eff,
        "non_usd_driver": nonusd,
    }


def _score(components: Mapping[str, float], cfg: StrategyBExpressionRankConfig) -> float:
    weights = {
        "htf_location": float(cfg.weight_htf_location),
        "major_structure": float(cfg.weight_major_structure),
        "h1_transition": float(cfg.weight_h1_transition),
        "volatility": float(cfg.weight_volatility),
        "directional_room": float(cfg.weight_directional_room),
        "move_remaining": float(cfg.weight_move_remaining),
        "liquidity": float(cfg.weight_liquidity),
        "spread_efficiency": float(cfg.weight_spread_efficiency),
        "non_usd_driver": float(cfg.weight_non_usd_driver),
    }
    denom = sum(weights.values())
    return round(
        100.0 * sum(components[k] * weights[k] for k in weights) / denom,
        4,
    )


def _evaluate_candidate(
    row: Mapping[str, Any],
    *,
    approved: Mapping[str, str],
    cfg: StrategyBExpressionRankConfig,
    calibrated: bool,
) -> RankedExpression:
    symbol = _upper(row.get("symbol"))
    side = _upper(row.get("side"))
    veto: list[str] = []

    expected_side = approved.get(symbol)
    if expected_side is None:
        veto.append("SYMBOL_NOT_APPROVED_FOR_V1_EXPRESSION_SET")
    elif side != expected_side:
        veto.append(f"SIDE_MISMATCH_EXPECTED_{expected_side}")

    # V2 asymmetric HTF location contract:
    # a canonical hard block is an explicit veto even before ranking
    # calibration exists.  Absence of a hard block is NOT qualification.
    if row.get("htf_location_hard_blocked") is True:
        block_reason = _upper(
            row.get("htf_location_block_reason")
        ) or "UNKNOWN"
        veto.append(
            f"HTF_LOCATION_HARD_BLOCKED:{block_reason}"
        )

    location_status = _upper(row.get("htf_location_status"))
    if location_status == "HTF_LOCATION_INPUT_INCOMPLETE":
        veto.append("HTF_LOCATION_INPUT_INCOMPLETE")

    if row.get("htf_location_qualified") is not True:
        veto.append("POOR_OR_UNQUALIFIED_HTF_LOCATION")

    ownership_status = _upper(
        row.get("invalidation_ownership_status")
    )
    if ownership_status == "H1_ONLY_INVALIDATION_REQUIRES_PROOF":
        veto.append("H1_ONLY_INVALIDATION_REQUIRES_PROOF")
    elif ownership_status == "NO_STRUCTURAL_INVALIDATION":
        veto.append("NO_STRUCTURAL_INVALIDATION")
    elif ownership_status == "INVALIDATION_INPUT_INCOMPLETE":
        veto.append("INVALIDATION_INPUT_INCOMPLETE")

    if row.get("structural_invalidation_valid") is not True:
        veto.append("STRUCTURAL_INVALIDATION_NOT_VALID")

    room = _num(row.get("directional_room_atr"))
    if calibrated:
        if room is None:
            veto.append("DIRECTIONAL_ROOM_MISSING")
        elif room < float(cfg.min_directional_room_atr):
            veto.append("INSUFFICIENT_DIRECTIONAL_ROOM")

    if row.get("symbol_data_fresh") is False:
        veto.append("SYMBOL_DATA_STALE")

    components = _normalized_components(row, cfg) if calibrated else None
    if calibrated and components is None:
        veto.append("RANKING_INPUT_INCOMPLETE")

    eligible = not veto
    score = _score(components, cfg) if eligible and components is not None else None

    return RankedExpression(
        rank=None,
        symbol=symbol,
        side=side,
        eligible=eligible,
        score=score,
        veto_reasons=tuple(veto),
        reasons=("MANDATORY_SYMBOL_GATES_PASS", "RANKABLE") if eligible else tuple(veto),
        components=components or {},
        raw=dict(row),
    )


def rank_strategy_b_expressions(
    expected_dxy_direction: str,
    *,
    candidates: Sequence[Mapping[str, Any]],
    config: Optional[StrategyBExpressionRankConfig] = None,
) -> StrategyBExpressionRanking:
    cfg = config or StrategyBExpressionRankConfig()
    usd_thesis, approved = _approved_map(expected_dxy_direction)
    calibrated, calibration_reason = _config_complete(cfg)

    if not approved:
        return StrategyBExpressionRanking(
            1, "STRATEGY_B_EXPRESSION_RANKER_V3", _upper(expected_dxy_direction) or "UNKNOWN",
            "UNKNOWN", calibrated, calibration_reason, None, None, None,
            "WAIT", "EXPECTED_DXY_DIRECTION_NOT_ACTIONABLE", ()
        )

    evaluated = [
        _evaluate_candidate(_mapping(row), approved=approved, cfg=cfg, calibrated=calibrated)
        for row in candidates if isinstance(row, Mapping)
    ]

    if not calibrated:
        return StrategyBExpressionRanking(
            1, "STRATEGY_B_EXPRESSION_RANKER_V3", _upper(expected_dxy_direction),
            usd_thesis, False, calibration_reason, None, None, None,
            "WAIT", "RANKING_CALIBRATION_REQUIRED", tuple(evaluated)
        )

    eligible = [x for x in evaluated if x.eligible and x.score is not None]
    eligible.sort(key=lambda x: (-float(x.score), x.symbol, x.side))
    rank_by_key = {(x.symbol, x.side): i + 1 for i, x in enumerate(eligible)}

    ranked_all = tuple(
        sorted(
            (
                RankedExpression(
                    rank=rank_by_key.get((item.symbol, item.side)),
                    symbol=item.symbol,
                    side=item.side,
                    eligible=item.eligible,
                    score=item.score,
                    veto_reasons=item.veto_reasons,
                    reasons=item.reasons,
                    components=item.components,
                    raw=item.raw,
                )
                for item in evaluated
            ),
            key=lambda x: (
                0 if x.eligible else 1,
                x.rank if x.rank is not None else 999,
                x.symbol,
                x.side,
            ),
        )
    )

    if not eligible:
        return StrategyBExpressionRanking(
            1, "STRATEGY_B_EXPRESSION_RANKER_V3", _upper(expected_dxy_direction),
            usd_thesis, True, "OK", None, None, None,
            "WAIT", "NO_ELIGIBLE_EXPRESSION", ranked_all
        )

    best = eligible[0]
    return StrategyBExpressionRanking(
        1, "STRATEGY_B_EXPRESSION_RANKER_V3", _upper(expected_dxy_direction),
        usd_thesis, True, "OK", best.symbol, best.side, best.score,
        "BEST_EXPRESSION_SELECTED_SHADOW",
        "ONE_BEST_ELIGIBLE_EXPRESSION_SELECTED",
        ranked_all,
    )


__all__ = [
    "USD_BEARISH_EXPRESSIONS",
    "USD_BULLISH_EXPRESSIONS",
    "StrategyBExpressionRankConfig",
    "RankedExpression",
    "StrategyBExpressionRanking",
    "rank_strategy_b_expressions",
]
