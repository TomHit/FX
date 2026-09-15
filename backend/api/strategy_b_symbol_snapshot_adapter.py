"""
XauTrendLab Strategy B - Symbol Snapshot Adapter
=================================================

Pure observation-only adapter for Strategy-B symbol expression analysis.

Inputs:
- native symbol D1/H4/H1 completed bars (already loaded by caller)
- canonical existing SR bundle from trend_sr
- symbol, side, price, as_of_ms

Reuses:
- api.bias_engine.compute_pair_bias()
- api.trend_sr.build_market_structure_state()

Does NOT:
- fetch broker/Redis data
- calculate a second SR model
- synthesize D1 from H4
- invent D1 SR
- invent Strategy-B structural invalidation
- invent expression-ranking quality scores
- reserve risk or place/modify/close orders
- mutate Strategy A
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Callable, Mapping, Optional, Sequence


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


def _bars_present(v: Any) -> bool:
    return isinstance(v, Sequence) and not isinstance(v, (str, bytes, bytearray)) and len(v) > 0


def _tf_row(pair_bias: Mapping[str, Any], tf: str) -> Mapping[str, Any]:
    return _mapping(_mapping(pair_bias.get("timeframes")).get(tf))


def _tf_data_ok(pair_bias: Mapping[str, Any], tf: str) -> bool:
    return _tf_row(pair_bias, tf).get("data_ok") is True


def _tf_structure_state(pair_bias: Mapping[str, Any], tf: str) -> str:
    row = _tf_row(pair_bias, tf)
    structure = _mapping(row.get("structure"))
    state = _upper(structure.get("state"))
    if state:
        return state
    return _upper(row.get("direction")) or "UNKNOWN"


def _side_flow(side: str) -> str:
    s = _upper(side)
    if s == "BUY":
        return "BULLISH"
    if s == "SELL":
        return "BEARISH"
    return "NEUTRAL"


@dataclass(frozen=True)
class StrategyBSymbolSnapshot:
    schema_version: int
    model: str
    symbol: str
    side: str
    as_of_ms: int
    price: Optional[float]

    data_ok: bool
    reason: str
    reasons: tuple[str, ...]

    # Canonical D1/H4/H1 pair analytics.
    pair_bias: Mapping[str, Any]
    timeframes: Mapping[str, Any]
    d1_structure_state: str
    h4_structure_state: str
    h1_structure_state: str

    # Existing H4/H1 canonical SR normalization.
    sr: Mapping[str, Any]
    directional_room_atr: Optional[float]
    structure_pressure: str
    structure_pressure_reason: str

    # Deliberately fail-closed until Strategy-B symbol location /
    # invalidation semantics are formally approved.
    htf_location_qualified: bool
    structural_invalidation_price: Optional[float]
    structural_invalidation_valid: bool
    symbol_data_fresh: bool

    evidence_sources: Mapping[str, Any]

    observation_only: bool = True
    execution_authority: bool = False
    strategy_a_mutation: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_expression_observer_input(self) -> dict[str, Any]:
        """
        Contract consumed by strategy_b_expression_candidate_observer.py.
        The explicit False gates are intentional and authoritative:
        the observer must not infer them from H4/H1 SR alone.
        """
        return {
            "symbol": self.symbol,
            "side": self.side,
            "price": self.price,
            "timeframes": dict(self.timeframes),
            "sr": dict(self.sr),
            "directional_room_atr": self.directional_room_atr,
            "htf_location_qualified": self.htf_location_qualified,
            "structural_invalidation_price":
                self.structural_invalidation_price,
            "structural_invalidation_valid":
                self.structural_invalidation_valid,
            "symbol_data_fresh": self.symbol_data_fresh,
            "_strategy_b_symbol_snapshot": {
                "schema_version": self.schema_version,
                "model": self.model,
                "data_ok": self.data_ok,
                "reason": self.reason,
                "reasons": list(self.reasons),
                "d1_structure_state": self.d1_structure_state,
                "h4_structure_state": self.h4_structure_state,
                "h1_structure_state": self.h1_structure_state,
                "structure_pressure": self.structure_pressure,
                "structure_pressure_reason":
                    self.structure_pressure_reason,
                "evidence_sources": dict(self.evidence_sources),
                "observation_only": True,
                "execution_authority": False,
            },
        }


def build_strategy_b_symbol_snapshot(
    *,
    symbol: str,
    side: str,
    bars_d1: Sequence[Mapping[str, Any]] | None,
    bars_h4: Sequence[Mapping[str, Any]] | None,
    bars_h1: Sequence[Mapping[str, Any]] | None,
    sr_bundle: Mapping[str, Any] | None,
    price: Any,
    as_of_ms: int,
    pair_bias_fn: Optional[Callable[..., Mapping[str, Any]]] = None,
    structure_builder: Optional[Callable[..., Mapping[str, Any]]] = None,
) -> StrategyBSymbolSnapshot:
    """
    Build one observation-only Strategy-B symbol snapshot.

    Native D1 is mandatory. There is deliberately NO H4->D1 resampling here.
    """
    sym = _upper(symbol)
    side_u = _upper(side)
    px = _num(price)
    reasons: list[str] = []

    if side_u not in ("BUY", "SELL"):
        reasons.append("SIDE_NOT_ACTIONABLE")

    if not _bars_present(bars_d1):
        reasons.append("NATIVE_D1_BARS_MISSING")
    if not _bars_present(bars_h4):
        reasons.append("NATIVE_H4_BARS_MISSING")
    if not _bars_present(bars_h1):
        reasons.append("NATIVE_H1_BARS_MISSING")
    if px is None or px <= 0:
        reasons.append("SYMBOL_PRICE_INVALID")
    if not isinstance(sr_bundle, Mapping) or not sr_bundle:
        reasons.append("CANONICAL_SR_BUNDLE_MISSING")

    if reasons:
        return StrategyBSymbolSnapshot(
            schema_version=1,
            model="STRATEGY_B_SYMBOL_SNAPSHOT_ADAPTER_V1",
            symbol=sym,
            side=side_u,
            as_of_ms=int(as_of_ms or 0),
            price=px,
            data_ok=False,
            reason=reasons[0],
            reasons=tuple(reasons),
            pair_bias={},
            timeframes={},
            d1_structure_state="UNKNOWN",
            h4_structure_state="UNKNOWN",
            h1_structure_state="UNKNOWN",
            sr={},
            directional_room_atr=None,
            structure_pressure="UNKNOWN",
            structure_pressure_reason="INPUTS_INCOMPLETE",
            htf_location_qualified=False,
            structural_invalidation_price=None,
            structural_invalidation_valid=False,
            symbol_data_fresh=False,
            evidence_sources={
                "D1_structure": "PAIR_BIAS_NATIVE_D1",
                "H4_structure": "PAIR_BIAS_NATIVE_H4",
                "H1_structure": "PAIR_BIAS_NATIVE_H1",
                "HTF_SR": "CANONICAL_H4_H1_SR",
                "D1_SR_available": False,
                "native_d1_required": True,
            },
        )

    if pair_bias_fn is None:
        from api.bias_engine import compute_pair_bias
        pair_bias_fn = compute_pair_bias

    if structure_builder is None:
        from api.trend_sr import build_market_structure_state
        structure_builder = build_market_structure_state

    try:
        pair_bias = pair_bias_fn(
            symbol=sym,
            bars_d1=bars_d1,
            bars_h4=bars_h4,
            bars_h1=bars_h1,
            as_of_ms=int(as_of_ms or 0),
        )
    except Exception as exc:
        pair_bias = {}
        reasons.append(
            f"PAIR_BIAS_ERROR:{type(exc).__name__}"
        )

    pair_bias = dict(_mapping(pair_bias))
    if pair_bias.get("data_ok") is not True:
        reasons.append("PAIR_BIAS_NOT_OK")

    try:
        structure = structure_builder(
            dict(sr_bundle),
            price=float(px),
            flow_direction=_side_flow(side_u),
        )
    except Exception as exc:
        structure = {}
        reasons.append(
            f"CANONICAL_SR_STRUCTURE_ERROR:{type(exc).__name__}"
        )

    structure = dict(_mapping(structure))
    if not structure:
        reasons.append("CANONICAL_SR_STRUCTURE_MISSING")

    # Native pair-bias freshness/quality is the only symbol freshness
    # authority here. No wall-clock freshness thresholds are invented.
    symbol_data_fresh = bool(
        pair_bias.get("data_ok") is True
        and _tf_data_ok(pair_bias, "D1")
        and _tf_data_ok(pair_bias, "H4")
        and _tf_data_ok(pair_bias, "H1")
    )
    if not symbol_data_fresh:
        reasons.append("SYMBOL_D1_H4_H1_DATA_NOT_OK")

    room = _num(structure.get("directional_room_atr"))
    if room is None:
        # Defensive fallback to the direction-specific canonical fields.
        room = _num(
            structure.get(
                "room_up_atr" if side_u == "BUY"
                else "room_down_atr"
            )
        )
    if room is None:
        reasons.append("DIRECTIONAL_ROOM_NOT_AVAILABLE")

    pressure = _upper(
        structure.get("structure_pressure")
    ) or "UNKNOWN"
    pressure_reason = str(
        structure.get("structure_pressure_reason")
        or "UNKNOWN"
    )

    # IMPORTANT:
    # Current canonical SR is H4/H1 only. D1 structure exists through
    # compute_pair_bias, but D1 SR/location has not been proven.
    # Therefore do NOT promote H4/H1 SR into a full Strategy-B
    # D1/H4 location qualification.
    htf_location_qualified = False
    reasons.append(
        "STRATEGY_B_D1_HTF_LOCATION_NOT_YET_PROVEN"
    )

    # IMPORTANT:
    # Existing analytics do not define the exact Strategy-B thesis
    # invalidation boundary. Do not choose nearest SR, zone edge,
    # or a buffer here.
    structural_invalidation_price = None
    structural_invalidation_valid = False
    reasons.append(
        "STRATEGY_B_STRUCTURAL_INVALIDATION_NOT_YET_DEFINED"
    )

    data_ok = bool(
        pair_bias.get("data_ok") is True
        and symbol_data_fresh
        and bool(structure)
        and room is not None
    )

    reason = (
        "OBSERVED_FAIL_CLOSED_AWAIT_D1_LOCATION_AND_INVALIDATION"
        if data_ok
        else (reasons[0] if reasons else "INPUTS_NOT_OK")
    )

    return StrategyBSymbolSnapshot(
        schema_version=1,
        model="STRATEGY_B_SYMBOL_SNAPSHOT_ADAPTER_V1",
        symbol=sym,
        side=side_u,
        as_of_ms=int(as_of_ms or 0),
        price=px,
        data_ok=data_ok,
        reason=reason,
        reasons=tuple(dict.fromkeys(reasons)),
        pair_bias=pair_bias,
        timeframes=dict(
            _mapping(pair_bias.get("timeframes"))
        ),
        d1_structure_state=_tf_structure_state(
            pair_bias, "D1"
        ),
        h4_structure_state=_tf_structure_state(
            pair_bias, "H4"
        ),
        h1_structure_state=_tf_structure_state(
            pair_bias, "H1"
        ),
        sr=structure,
        directional_room_atr=room,
        structure_pressure=pressure,
        structure_pressure_reason=pressure_reason,
        htf_location_qualified=htf_location_qualified,
        structural_invalidation_price=
            structural_invalidation_price,
        structural_invalidation_valid=
            structural_invalidation_valid,
        symbol_data_fresh=symbol_data_fresh,
        evidence_sources={
            "D1_structure": "PAIR_BIAS_NATIVE_D1",
            "H4_structure": "PAIR_BIAS_NATIVE_H4",
            "H1_structure": "PAIR_BIAS_NATIVE_H1",
            "HTF_SR": "CANONICAL_H4_H1_SR",
            "D1_SR_available": False,
            "native_d1_required": True,
            "d1_synthesized": False,
        },
    )


__all__ = [
    "StrategyBSymbolSnapshot",
    "build_strategy_b_symbol_snapshot",
]
