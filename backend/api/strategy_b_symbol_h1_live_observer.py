"""
XauTrendLab Strategy B - Live Symbol H1 Observer
================================================

Observation-only live wiring for selected-symbol H1 reversal evidence.

Reuses:
- strategy_b_symbol_live_loader V3 exact Redis OHLC path
- strategy_b_h1_pattern_observer structural facts
- strategy_b_symbol_h1_proof proof aggregation

No broker/order/risk authority. No Strategy-A mutation. No Redis SCAN.
No new thresholds are invented here.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping, Optional
import time

from api.strategy_b_h1_pattern_observer import (
    StrategyBH1PatternConfig,
    observe_strategy_b_h1_patterns,
)
from api.strategy_b_symbol_h1_proof import (
    evaluate_strategy_b_symbol_h1_proof,
)
from api.strategy_b_symbol_live_loader import (
    load_strategy_b_symbol_snapshot_live,
    _load_native_symbol_tf_from_redis,
)
from api.strategy_b_catalyst_time import (
    load_strategy_b_catalyst_time,
)


def _mapping(v: Any) -> Mapping[str, Any]:
    return v if isinstance(v, Mapping) else {}


def _upper(v: Any) -> str:
    return str(v or "").strip().upper()


@dataclass(frozen=True)
class StrategyBLiveSymbolH1Observation:
    schema_version: int
    model: str
    symbol: str
    side: str
    as_of_ms: int
    catalyst_first_seen_ms: int
    evidence_id: Optional[str]
    catalyst_source_field: Optional[str]
    schedule_consistent: Optional[bool]

    ok: bool
    reason: str

    loader_reason: str
    h1_load_reason: str
    h1_bar_count: int
    h1_device_id: Optional[str]

    pattern: Optional[Mapping[str, Any]]
    proof: Optional[Mapping[str, Any]]

    observation_only: bool = True
    execution_authority: bool = False
    strategy_a_mutation: bool = False
    redis_scan_used: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def observe_strategy_b_symbol_h1_live(
    R: Any,
    *,
    symbol: str,
    side: str,
    evidence_id: Optional[str] = None,
    catalyst_first_seen_ms: Optional[int] = None,
    as_of_ms: Optional[int] = None,
    pattern_config: Optional[StrategyBH1PatternConfig] = None,
    native_tf_cache: Optional[dict[tuple[str, str], dict[str, Any]]] = None,
) -> StrategyBLiveSymbolH1Observation:
    """
    Observe selected-symbol H1 structure causally.

    Production/runtime callers should pass evidence_id. The observer resolves
    the first-known catalyst time from canonical Strategy-B evidence/context.

    catalyst_first_seen_ms remains only as a backwards-compatible replay/test
    override when evidence_id is absent. It is never preferred over a supplied
    evidence_id.
    """
    sym = _upper(symbol)
    side_u = _upper(side)
    now_ms = int(as_of_ms or int(time.time() * 1000))

    resolved_evidence_id: Optional[str] = None
    catalyst_source_field: Optional[str] = None
    schedule_consistent: Optional[bool] = None
    catalyst_ms = int(catalyst_first_seen_ms or 0)

    if evidence_id:
        resolved_evidence_id = str(evidence_id).strip()
        catalyst = load_strategy_b_catalyst_time(
            R,
            evidence_id=resolved_evidence_id,
        )
        if catalyst.ok is not True:
            return StrategyBLiveSymbolH1Observation(
                schema_version=2,
                model="STRATEGY_B_LIVE_SYMBOL_H1_OBSERVER_V2",
                symbol=sym,
                side=side_u,
                as_of_ms=now_ms,
                catalyst_first_seen_ms=0,
                evidence_id=resolved_evidence_id,
                catalyst_source_field=None,
                schedule_consistent=catalyst.schedule_consistent,
                ok=False,
                reason=f"CATALYST_TIME_NOT_AVAILABLE:{catalyst.reason}",
                loader_reason="NOT_ATTEMPTED",
                h1_load_reason="NOT_ATTEMPTED",
                h1_bar_count=0,
                h1_device_id=None,
                pattern=None,
                proof=None,
            )
        catalyst_ms = int(catalyst.catalyst_first_seen_ms or 0)
        catalyst_source_field = catalyst.source_field
        schedule_consistent = catalyst.schedule_consistent
    else:
        if catalyst_ms <= 0:
            return StrategyBLiveSymbolH1Observation(
                schema_version=2,
                model="STRATEGY_B_LIVE_SYMBOL_H1_OBSERVER_V2",
                symbol=sym,
                side=side_u,
                as_of_ms=now_ms,
                catalyst_first_seen_ms=0,
                evidence_id=None,
                catalyst_source_field=None,
                schedule_consistent=None,
                ok=False,
                reason="CATALYST_FIRST_SEEN_TIME_MISSING",
                loader_reason="NOT_ATTEMPTED",
                h1_load_reason="NOT_ATTEMPTED",
                h1_bar_count=0,
                h1_device_id=None,
                pattern=None,
                proof=None,
            )
        catalyst_source_field = "DIRECT_REPLAY_OR_TEST_OVERRIDE"

    _snapshot_kwargs = {
        "symbol": sym,
        "side": side_u,
        "as_of_ms": now_ms,
    }
    if native_tf_cache is not None:
        _snapshot_kwargs["native_tf_cache"] = native_tf_cache

    loaded = load_strategy_b_symbol_snapshot_live(
        R,
        **_snapshot_kwargs,
    )
    loaded = dict(_mapping(loaded))

    if loaded.get("ok") is not True:
        return StrategyBLiveSymbolH1Observation(
            schema_version=2,
            model="STRATEGY_B_LIVE_SYMBOL_H1_OBSERVER_V2",
            symbol=sym,
            side=side_u,
            as_of_ms=now_ms,
            catalyst_first_seen_ms=catalyst_ms,
            evidence_id=resolved_evidence_id,
            catalyst_source_field=catalyst_source_field,
            schedule_consistent=schedule_consistent,
            ok=False,
            reason="SYMBOL_SNAPSHOT_NOT_AVAILABLE",
            loader_reason=str(loaded.get("reason") or "UNKNOWN"),
            h1_load_reason="NOT_ATTEMPTED",
            h1_bar_count=0,
            h1_device_id=None,
            pattern=None,
            proof=None,
        )

    snap = _mapping(loaded.get("snapshot"))
    pair_bias = _mapping(snap.get("pair_bias"))
    structure = _mapping(snap.get("sr"))

    _h1_kwargs = {
        "symbol": sym,
        "tf": "H1",
    }
    if native_tf_cache is not None:
        _h1_kwargs["native_tf_cache"] = native_tf_cache

    h1row = _load_native_symbol_tf_from_redis(
        R,
        **_h1_kwargs,
    )
    h1row = dict(_mapping(h1row))

    if h1row.get("ok") is not True:
        return StrategyBLiveSymbolH1Observation(
            schema_version=2,
            model="STRATEGY_B_LIVE_SYMBOL_H1_OBSERVER_V2",
            symbol=sym,
            side=side_u,
            as_of_ms=now_ms,
            catalyst_first_seen_ms=catalyst_ms,
            evidence_id=resolved_evidence_id,
            catalyst_source_field=catalyst_source_field,
            schedule_consistent=schedule_consistent,
            ok=False,
            reason="H1_OHLC_NOT_AVAILABLE",
            loader_reason=str(loaded.get("reason") or "UNKNOWN"),
            h1_load_reason=str(h1row.get("reason") or "UNKNOWN"),
            h1_bar_count=int(h1row.get("bar_count") or 0),
            h1_device_id=(
                str(h1row.get("device_id"))
                if h1row.get("device_id")
                else None
            ),
            pattern=None,
            proof=None,
        )

    expected_direction = (
        "BULLISH" if side_u == "BUY"
        else "BEARISH" if side_u == "SELL"
        else "UNKNOWN"
    )

    pattern = observe_strategy_b_h1_patterns(
        h1_bars=list(h1row.get("bars") or []),
        expected_direction=expected_direction,
        catalyst_first_seen_ms=catalyst_ms,
        dxy_bias=pair_bias,      # legacy parameter name; symbol bias here
        dxy_structure=structure, # legacy parameter name; symbol structure here
        as_of_ms=now_ms,
        config=pattern_config,
    )

    pattern_dict = pattern.to_dict()
    entry_obs = pattern.to_entry_pattern_observations()

    proof = evaluate_strategy_b_symbol_h1_proof(
        symbol=sym,
        side=side_u,
        pattern_observations=entry_obs,
    )
    proof_dict = proof.to_dict()

    ok = bool(pattern_dict.get("data_ok") is True)
    reason = (
        str(proof_dict.get("status") or "UNKNOWN")
        if ok
        else str(pattern_dict.get("reason") or "H1_PATTERN_INPUT_NOT_OK")
    )

    return StrategyBLiveSymbolH1Observation(
        schema_version=2,
        model="STRATEGY_B_LIVE_SYMBOL_H1_OBSERVER_V2",
        symbol=sym,
        side=side_u,
        as_of_ms=now_ms,
        catalyst_first_seen_ms=catalyst_ms,
        evidence_id=resolved_evidence_id,
        catalyst_source_field=catalyst_source_field,
        schedule_consistent=schedule_consistent,
        ok=ok,
        reason=reason,
        loader_reason=str(loaded.get("reason") or "UNKNOWN"),
        h1_load_reason=str(h1row.get("reason") or "UNKNOWN"),
        h1_bar_count=int(h1row.get("bar_count") or 0),
        h1_device_id=(
            str(h1row.get("device_id"))
            if h1row.get("device_id")
            else None
        ),
        pattern=pattern_dict,
        proof=proof_dict,
    )


__all__ = [
    "StrategyBLiveSymbolH1Observation",
    "observe_strategy_b_symbol_h1_live",
]
