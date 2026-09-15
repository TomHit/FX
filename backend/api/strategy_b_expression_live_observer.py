"""
XauTrendLab Strategy B - Live Expression Basket Observer V6
=========================================================

Observation-only multi-symbol expression acquisition/composition.

Flow:
    expected DXY reversal direction
        -> approved Strategy-B expression map (owned by expression ranker)
        -> live symbol snapshot loader for each approved symbol/side
        -> expression candidate observer
        -> structural invalidation observer/classifier
        -> HTF invalidation owner resolver (shadow)
        -> structural buffer calibration observer (shadow)
        -> structural invalidation finalizer (shadow)
        -> expression ranker

This module does NOT:
- invent or duplicate the approved symbol/side map;
- invent ranking calibration, weights, quality scores, D1 SR, or invalidation;
- write Redis;
- use Redis SCAN;
- call broker/order APIs;
- reserve risk;
- invent D1-vs-H4 owner precedence or a structural buffer;
- mutate Strategy A;
- select a production order.

The top-level result remains fail-closed if any approved expression could not be
observed. Even with a complete basket, the existing ranker remains authoritative
and currently returns WAIT until ranking calibration is explicitly approved.
"""
from __future__ import annotations

import time
from dataclasses import asdict, dataclass
from typing import Any, Callable, Mapping, Optional


def _upper(v: Any) -> str:
    return str(v or "").strip().upper()


def _mapping(v: Any) -> Mapping[str, Any]:
    return v if isinstance(v, Mapping) else {}


def _approved_expression_map(
    expected_dxy_direction: str,
) -> Mapping[str, str]:
    """
    Reuse the ranker's public expression maps; do not duplicate basket policy.
    """
    from api.strategy_b_expression_ranker import (
        USD_BEARISH_EXPRESSIONS,
        USD_BULLISH_EXPRESSIONS,
    )

    d = _upper(expected_dxy_direction)
    if d == "BEARISH":
        return USD_BEARISH_EXPRESSIONS
    if d == "BULLISH":
        return USD_BULLISH_EXPRESSIONS
    return {}


def _snapshot_to_expression_input(
    snapshot: Mapping[str, Any],
) -> dict[str, Any]:
    """
    Recreate the explicit contract emitted by
    StrategyBSymbolSnapshot.to_expression_observer_input() from its serialized
    dict returned by the live loader.

    Explicit False gates remain authoritative. We deliberately do not allow the
    downstream observer to infer D1/H4 location or invalidation from H4/H1 SR.
    """
    s = _mapping(snapshot)

    return {
        "symbol": s.get("symbol"),
        "side": s.get("side"),
        "price": s.get("price"),
        "timeframes": dict(_mapping(s.get("timeframes"))),
        "sr": dict(_mapping(s.get("sr"))),
        "directional_room_atr": s.get("directional_room_atr"),
        "htf_location_qualified": s.get(
            "htf_location_qualified"
        ),
        "structural_invalidation_price": s.get(
            "structural_invalidation_price"
        ),
        "structural_invalidation_valid": s.get(
            "structural_invalidation_valid"
        ),
        "symbol_data_fresh": s.get("symbol_data_fresh"),
        "_strategy_b_symbol_snapshot": {
            "schema_version": s.get("schema_version"),
            "model": s.get("model"),
            "data_ok": s.get("data_ok"),
            "reason": s.get("reason"),
            "reasons": list(s.get("reasons") or []),
            "d1_structure_state": s.get("d1_structure_state"),
            "h4_structure_state": s.get("h4_structure_state"),
            "h1_structure_state": s.get("h1_structure_state"),
            "structure_pressure": s.get("structure_pressure"),
            "structure_pressure_reason": s.get(
                "structure_pressure_reason"
            ),
            "evidence_sources": dict(
                _mapping(s.get("evidence_sources"))
            ),
            "observation_only": True,
            "execution_authority": False,
        },
    }


@dataclass(frozen=True)
class StrategyBLiveExpressionCandidate:
    symbol: str
    side: str
    acquisition_ok: bool
    acquisition_reason: str
    sr_source: Optional[str]
    sr_fallback_used: bool
    bars_available: Mapping[str, Any]
    observation: Optional[Mapping[str, Any]]
    htf_location: Optional[Mapping[str, Any]]
    structural_invalidation: Optional[Mapping[str, Any]]
    invalidation_ownership: Optional[Mapping[str, Any]]
    htf_invalidation_owner_resolution: Optional[Mapping[str, Any]]
    structural_buffer_calibration: Optional[Mapping[str, Any]]
    structural_invalidation_finalization: Optional[Mapping[str, Any]]
    ranker_candidate: Optional[Mapping[str, Any]]

    # Analytics-only causal market path after the frozen H1 break.
    forward_path_observation: Optional[Mapping[str, Any]] = None

    observation_only: bool = True
    execution_authority: bool = False
    strategy_a_mutation: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class StrategyBLiveExpressionBasket:
    schema_version: int
    model: str
    expected_dxy_direction: str
    as_of_ms: int

    approved_count: int
    observed_count: int
    failed_count: int
    complete: bool

    decision: str
    reason: str

    candidates: tuple[StrategyBLiveExpressionCandidate, ...]
    ranking: Optional[Mapping[str, Any]]

    observation_only: bool = True
    execution_authority: bool = False
    strategy_a_mutation: bool = False
    redis_scan_used: bool = False
    broker_order_authority: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def observe_strategy_b_expression_basket_live(
    R: Any,
    *,
    expected_dxy_direction: str,
    as_of_ms: Optional[int] = None,
    live_loader: Optional[Callable[..., Mapping[str, Any]]] = None,
    observer_fn: Optional[Callable[..., Any]] = None,
    ranker_fn: Optional[Callable[..., Any]] = None,
    htf_location_fn: Optional[Callable[..., Any]] = None,
    structural_invalidation_fn: Optional[Callable[..., Any]] = None,
    invalidation_ownership_fn: Optional[Callable[..., Any]] = None,
    structural_invalidation_finalizer_fn: Optional[Callable[..., Any]] = None,
    htf_owner_resolver_fn: Optional[Callable[..., Any]] = None,
    structural_buffer_calibration_fn: Optional[Callable[..., Any]] = None,
    catalyst_first_seen_ms: Any = None,
    preferred_htf_owner: Optional[str] = None,
    structural_buffer_price: Any = None,
    h1_proof_by_candidate: Optional[
        Mapping[tuple[str, str], Mapping[str, Any]]
    ] = None,
    h1_break_ms_by_candidate: Optional[
        Mapping[tuple[str, str], Any]
    ] = None,
    h1_break_close_by_candidate: Optional[
        Mapping[tuple[str, str], Any]
    ] = None,
    h1_reference_atr_by_candidate: Optional[
        Mapping[tuple[str, str], Any]
    ] = None,
    forward_path_observer_fn: Optional[Callable[..., Any]] = None,
) -> StrategyBLiveExpressionBasket:
    """
    Observe every approved Strategy-B expression for one DXY direction.

    This function intentionally does not persist anything. It is suitable for
    manual observation/replay now and can later be wrapped by a separately
    reviewed persistence/manager layer.
    """
    expected = _upper(expected_dxy_direction)
    now_ms = int(as_of_ms or int(time.time() * 1000))
    approved = dict(_approved_expression_map(expected))

    if not approved:
        return StrategyBLiveExpressionBasket(
            schema_version=1,
            model="STRATEGY_B_LIVE_EXPRESSION_BASKET_V6",
            expected_dxy_direction=expected or "UNKNOWN",
            as_of_ms=now_ms,
            approved_count=0,
            observed_count=0,
            failed_count=0,
            complete=False,
            decision="WAIT",
            reason="EXPECTED_DXY_DIRECTION_NOT_ACTIONABLE",
            candidates=(),
            ranking=None,
        )

    if live_loader is None:
        from api.strategy_b_symbol_live_loader import (
            load_strategy_b_symbol_snapshot_live,
        )
        live_loader = load_strategy_b_symbol_snapshot_live

    if observer_fn is None:
        from api.strategy_b_expression_candidate_observer import (
            observe_strategy_b_expression_candidate,
        )
        observer_fn = observe_strategy_b_expression_candidate

    if ranker_fn is None:
        from api.strategy_b_expression_ranker import (
            rank_strategy_b_expressions,
        )
        ranker_fn = rank_strategy_b_expressions

    if htf_location_fn is None:
        from api.strategy_b_htf_location_observer import (
            observe_strategy_b_htf_location,
        )
        htf_location_fn = observe_strategy_b_htf_location

    _default_structural_invalidation = structural_invalidation_fn is None
    if structural_invalidation_fn is None:
        from api.strategy_b_structural_invalidation_observer import (
            observe_strategy_b_structural_invalidation,
        )
        structural_invalidation_fn = (
            observe_strategy_b_structural_invalidation
        )

    if invalidation_ownership_fn is None:
        from api.strategy_b_invalidation_ownership_observer import (
            observe_strategy_b_invalidation_ownership,
        )
        invalidation_ownership_fn = (
            observe_strategy_b_invalidation_ownership
        )

    if structural_invalidation_finalizer_fn is None:
        from api.strategy_b_structural_invalidation_finalizer import (
            finalize_strategy_b_structural_invalidation,
        )
        structural_invalidation_finalizer_fn = (
            finalize_strategy_b_structural_invalidation
        )

    if htf_owner_resolver_fn is None:
        from api.strategy_b_htf_invalidation_owner_resolver import (
            resolve_strategy_b_htf_invalidation_owner,
        )
        htf_owner_resolver_fn = (
            resolve_strategy_b_htf_invalidation_owner
        )

    if structural_buffer_calibration_fn is None:
        from api.strategy_b_structural_buffer_calibration import (
            observe_strategy_b_structural_buffer_calibration,
        )
        structural_buffer_calibration_fn = (
            observe_strategy_b_structural_buffer_calibration
        )

    if forward_path_observer_fn is None:
        from api.strategy_b_forward_path_observer import (
            observe_strategy_b_forward_path,
        )
        forward_path_observer_fn = observe_strategy_b_forward_path

    # V6.1: exact Redis native D1/H4/H1 bars are needed only for Strategy-B
    # pivot timestamp metadata. Reuse the existing symbol live-loader helper;
    # no HTTP broker fetch and no Redis SCAN.
    from api.strategy_b_symbol_live_loader import (
        _load_native_symbol_tf_from_redis,
    )

    h1_proofs = (
        h1_proof_by_candidate
        if isinstance(h1_proof_by_candidate, Mapping)
        else {}
    )
    h1_breaks = (
        h1_break_ms_by_candidate
        if isinstance(h1_break_ms_by_candidate, Mapping)
        else {}
    )
    h1_break_closes = (
        h1_break_close_by_candidate
        if isinstance(h1_break_close_by_candidate, Mapping)
        else {}
    )
    h1_reference_atrs = (
        h1_reference_atr_by_candidate
        if isinstance(h1_reference_atr_by_candidate, Mapping)
        else {}
    )

    rows: list[StrategyBLiveExpressionCandidate] = []
    ranker_candidates: list[Mapping[str, Any]] = []
    failed_count = 0

    # Preserve the ranker's canonical public mapping order.
    for symbol, side in approved.items():
        try:
            loaded = live_loader(
                R,
                symbol=symbol,
                side=side,
                as_of_ms=now_ms,
            )
        except Exception as exc:
            loaded = {
                "ok": False,
                "reason": (
                    "SYMBOL_LIVE_LOADER_EXCEPTION:"
                    + type(exc).__name__
                ),
            }

        loaded = dict(_mapping(loaded))
        acquisition_ok = loaded.get("ok") is True
        acquisition_reason = str(
            loaded.get("reason") or "UNKNOWN"
        )

        observation_dict: Optional[dict[str, Any]] = None
        htf_location_dict: Optional[dict[str, Any]] = None
        invalidation_dict: Optional[dict[str, Any]] = None
        ownership_dict: Optional[dict[str, Any]] = None
        owner_resolution_dict: Optional[dict[str, Any]] = None
        buffer_calibration_dict: Optional[dict[str, Any]] = None
        finalization_dict: Optional[dict[str, Any]] = None
        forward_path_dict: Optional[dict[str, Any]] = None
        candidate_dict: Optional[dict[str, Any]] = None

        snap = _mapping(loaded.get("snapshot"))
        if acquisition_ok and snap:
            try:
                obs = observer_fn(
                    expected_dxy_direction=expected,
                    symbol_snapshot=_snapshot_to_expression_input(
                        snap
                    ),
                )
                observation_dict = (
                    obs.to_dict()
                    if hasattr(obs, "to_dict")
                    else dict(obs)
                    if isinstance(obs, Mapping)
                    else None
                )
                candidate_dict = (
                    obs.to_ranker_candidate()
                    if hasattr(obs, "to_ranker_candidate")
                    else None
                )

                htf = htf_location_fn(
                    symbol=symbol,
                    side=side,
                    price=snap.get("price"),
                    pair_bias=_mapping(snap.get("pair_bias")),
                    canonical_structure=_mapping(snap.get("sr")),
                )
                htf_location_dict = (
                    htf.to_dict()
                    if hasattr(htf, "to_dict")
                    else dict(htf)
                    if isinstance(htf, Mapping)
                    else None
                )

                _native: dict[str, list[Mapping[str, Any]]] = {}

                if _default_structural_invalidation:
                    # Real V6 runtime path:
                    # load exact Redis D1/H4/H1 bars only to recover the
                    # timestamps/indices of the already-computed confirmed
                    # swing candidates. No broker HTTP and no Redis SCAN.
                    _native = {}
                    for _tf in ("D1", "H4", "H1"):
                        _row = _load_native_symbol_tf_from_redis(
                            R,
                            symbol=symbol,
                            tf=_tf,
                        )
                        if _mapping(_row).get("ok") is not True:
                            raise RuntimeError(
                                f"STRATEGY_B_NATIVE_{_tf}_BARS_UNAVAILABLE:"
                                f"{_mapping(_row).get('reason') or 'UNKNOWN'}"
                            )
                        _native[_tf] = list(
                            _mapping(_row).get("bars") or []
                        )

                    inval = structural_invalidation_fn(
                        symbol=symbol,
                        side=side,
                        entry_price=snap.get("price"),
                        pair_bias=_mapping(snap.get("pair_bias")),
                        native_bars=_native,
                    )
                else:
                    # Backward-compatible injected observer path used by the
                    # existing V5/V6 tests and offline callers. Do not force a
                    # new keyword argument onto legacy callables.
                    inval = structural_invalidation_fn(
                        symbol=symbol,
                        side=side,
                        entry_price=snap.get("price"),
                        pair_bias=_mapping(snap.get("pair_bias")),
                    )
                invalidation_dict = (
                    inval.to_dict()
                    if hasattr(inval, "to_dict")
                    else dict(inval)
                    if isinstance(inval, Mapping)
                    else None
                )

                owner = invalidation_ownership_fn(
                    symbol=symbol,
                    side=side,
                    invalidation_observation=invalidation_dict,
                )
                ownership_dict = (
                    owner.to_dict()
                    if hasattr(owner, "to_dict")
                    else dict(owner)
                    if isinstance(owner, Mapping)
                    else None
                )

                # V6.3 shadow owner resolution.
                #
                # Generic basket observation is intentionally not evidence-
                # specific. Callers may provide canonical catalyst/H1 proof
                # inputs when available. With missing causal inputs, dual HTF
                # ownership remains WAIT. Single usable HTF ownership may still
                # resolve by availability.
                h1_proof = _mapping(
                    h1_proofs.get((symbol, side))
                )
                h1_break_ms = h1_breaks.get((symbol, side))

                resolved_owner = htf_owner_resolver_fn(
                    symbol=symbol,
                    side=side,
                    invalidation_observation=invalidation_dict,
                    catalyst_first_seen_ms=catalyst_first_seen_ms,
                    h1_proof=h1_proof,
                    h1_break_ms=h1_break_ms,
                )
                owner_resolution_dict = (
                    resolved_owner.to_dict()
                    if hasattr(resolved_owner, "to_dict")
                    else dict(resolved_owner)
                    if isinstance(resolved_owner, Mapping)
                    else None
                )

                # V6.4 buffer-calibration evidence.
                #
                # This observer measures post-pivot wick excursion only. It
                # NEVER supplies structural_buffer_price to the finalizer and
                # therefore cannot promote structural_invalidation_valid.
                buffer_obs = structural_buffer_calibration_fn(
                    symbol=symbol,
                    side=side,
                    as_of_ms=now_ms,
                    invalidation_observation=invalidation_dict,
                    owner_resolution=owner_resolution_dict,
                    native_bars=_native,
                )
                buffer_calibration_dict = (
                    buffer_obs.to_dict()
                    if hasattr(buffer_obs, "to_dict")
                    else dict(buffer_obs)
                    if isinstance(buffer_obs, Mapping)
                    else None
                )

                # Explicit caller override still wins when supplied. Otherwise
                # use only a successfully resolved shadow owner.
                _resolved_preferred = (
                    str(preferred_htf_owner).upper().strip()
                    if preferred_htf_owner is not None
                    else None
                )
                if not _resolved_preferred and isinstance(
                    owner_resolution_dict, Mapping
                ):
                    if (
                        owner_resolution_dict.get("decision")
                        == "HTF_OWNER_RESOLVED"
                    ):
                        _resolved_preferred = (
                            owner_resolution_dict.get(
                                "preferred_htf_owner"
                            )
                        )

                # Buffer remains explicit. With structural_buffer_price=None,
                # even resolved ownership cannot promote the structural gate.
                finalized = structural_invalidation_finalizer_fn(
                    symbol=symbol,
                    side=side,
                    invalidation_observation=invalidation_dict,
                    ownership_observation=ownership_dict,
                    h1_proof=h1_proof,
                    structural_buffer_price=structural_buffer_price,
                    preferred_htf_owner=_resolved_preferred,
                )
                finalization_dict = (
                    finalized.to_dict()
                    if hasattr(finalized, "to_dict")
                    else dict(finalized)
                    if isinstance(finalized, Mapping)
                    else None
                )

                # V6.5 analytics-only forward market path.
                #
                # The origin is immutable:
                #   time  = evidence-specific H1 break_ms
                #   price = same H1 pattern break_close
                #   ATR   = same H1 pattern ATR
                #
                # No new Redis read is performed: _native["H1"] was already
                # loaded for structural invalidation / buffer observation.
                try:
                    _fp = forward_path_observer_fn(
                        symbol=symbol,
                        side=side,
                        as_of_ms=now_ms,
                        h1_break_ms=h1_break_ms,
                        reference_price=h1_break_closes.get(
                            (symbol, side)
                        ),
                        reference_atr=h1_reference_atrs.get(
                            (symbol, side)
                        ),
                        selected_invalidation_price=(
                            finalization_dict.get(
                                "structural_invalidation_price"
                            )
                            if isinstance(finalization_dict, Mapping)
                            else None
                        ),
                        selected_invalidation_timeframe=(
                            finalization_dict.get("selected_timeframe")
                            if isinstance(finalization_dict, Mapping)
                            else None
                        ),
                        native_h1_bars=list(_native.get("H1") or []),
                    )
                    forward_path_dict = (
                        _fp.to_dict()
                        if hasattr(_fp, "to_dict")
                        else dict(_fp)
                        if isinstance(_fp, Mapping)
                        else None
                    )
                except Exception as _fp_exc:
                    # Analytics must never make the expression basket fail.
                    forward_path_dict = {
                        "schema_version": 1,
                        "model": "STRATEGY_B_FORWARD_PATH_OBSERVER_V1",
                        "symbol": symbol,
                        "side": side,
                        "observation_state": "INPUT_INCOMPLETE",
                        "reason": (
                            "FORWARD_PATH_OBSERVER_EXCEPTION:"
                            + type(_fp_exc).__name__
                        ),
                        "reasons": [
                            "FORWARD_PATH_OBSERVER_EXCEPTION:"
                            + type(_fp_exc).__name__
                        ],
                        "observation_only": True,
                        "execution_authority": False,
                        "risk_authority": False,
                        "strategy_a_mutation": False,
                        "redis_scan_used": False,
                        "broker_http_used": False,
                        "future_data_used": False,
                    }
            except Exception as exc:
                acquisition_ok = False
                acquisition_reason = (
                    "EXPRESSION_OBSERVER_EXCEPTION:"
                    + type(exc).__name__
                )

        if (
            acquisition_ok
            and isinstance(candidate_dict, Mapping)
            and isinstance(htf_location_dict, Mapping)
            and isinstance(invalidation_dict, Mapping)
            and isinstance(ownership_dict, Mapping)
            and isinstance(owner_resolution_dict, Mapping)
            and isinstance(buffer_calibration_dict, Mapping)
            and isinstance(finalization_dict, Mapping)
        ):
            candidate_dict = dict(candidate_dict)

            # V3: propagate explicit HTF location observation into the
            # ranker contract without promoting qualification.
            #
            # This is intentionally asymmetric:
            # - a known canonical hard block is authoritative evidence;
            # - "not blocked" is NOT equivalent to "qualified";
            # - the existing htf_location_qualified field remains False
            #   until Strategy-B location calibration is explicitly approved.
            candidate_dict["htf_location_status"] = (
                htf_location_dict.get("htf_location_status")
            )
            candidate_dict["htf_location_observed"] = (
                htf_location_dict.get("htf_location_observed") is True
            )
            candidate_dict["htf_location_hard_blocked"] = (
                htf_location_dict.get("htf_location_hard_blocked") is True
            )
            candidate_dict["htf_location_block_reason"] = (
                htf_location_dict.get("htf_location_block_reason")
            )
            candidate_dict["d1_anchor_distance_atr"] = (
                htf_location_dict.get("d1_anchor_distance_atr")
            )
            candidate_dict["d1_opposing_room_atr"] = (
                htf_location_dict.get("d1_opposing_room_atr")
            )
            candidate_dict["candidate_macro_invalidation_price"] = (
                htf_location_dict.get(
                    "candidate_macro_invalidation_price"
                )
            )

            candidate_dict["structural_invalidation_observed"] = (
                invalidation_dict.get(
                    "structural_invalidation_observed"
                ) is True
            )
            candidate_dict["invalidation_observation_reason"] = (
                invalidation_dict.get("reason")
            )
            candidate_dict["invalidation_correct_side_candidates"] = list(
                invalidation_dict.get("correct_side_candidates") or []
            )
            # V6: propagate the finalizer result. With default V6 inputs this
            # remains fail-closed because owner precedence and buffer are None.
            # The finalizer itself is the only module allowed to turn this
            # candidate-level structural gate True.
            candidate_dict["structural_invalidation_valid"] = (
                finalization_dict.get("structural_invalidation_valid") is True
            )
            candidate_dict["structural_invalidation_price"] = (
                finalization_dict.get("structural_invalidation_price")
            )
            candidate_dict["structural_invalidation_finalizer_decision"] = (
                finalization_dict.get("decision")
            )
            candidate_dict["structural_invalidation_finalizer_reason"] = (
                finalization_dict.get("reason")
            )
            candidate_dict["selected_invalidation_timeframe"] = (
                finalization_dict.get("selected_timeframe")
            )
            candidate_dict["selected_invalidation_source"] = (
                finalization_dict.get("selected_source")
            )
            candidate_dict["owner_selection_policy"] = (
                finalization_dict.get("owner_selection_policy")
            )
            candidate_dict["proposed_structural_stop_price"] = (
                finalization_dict.get("proposed_stop_price")
            )
            candidate_dict["structural_buffer_price"] = (
                finalization_dict.get("structural_buffer_price")
            )
            candidate_dict["htf_owner_resolution_decision"] = (
                owner_resolution_dict.get("decision")
            )
            candidate_dict["htf_owner_resolution_reason"] = (
                owner_resolution_dict.get("reason")
            )
            candidate_dict["resolved_preferred_htf_owner"] = (
                owner_resolution_dict.get("preferred_htf_owner")
            )
            candidate_dict["htf_owner_selection_policy"] = (
                owner_resolution_dict.get("owner_selection_policy")
            )

            # V6.4 analytics-only buffer calibration fields.
            candidate_dict["buffer_calibration_reason"] = (
                buffer_calibration_dict.get("reason")
            )
            candidate_dict["buffer_calibration_owner_timeframe"] = (
                buffer_calibration_dict.get("owner_timeframe")
            )
            candidate_dict["buffer_calibration_max_breach_price"] = (
                buffer_calibration_dict.get("max_pivot_breach_price")
            )
            candidate_dict["buffer_calibration_max_breach_atr"] = (
                buffer_calibration_dict.get("max_pivot_breach_atr")
            )
            candidate_dict["buffer_calibration_breach_count"] = (
                buffer_calibration_dict.get("breach_count")
            )
            candidate_dict["buffer_calibration_ready"] = (
                buffer_calibration_dict.get("buffer_calibration_ready") is True
            )
            candidate_dict["buffer_calibration_proposed_price"] = (
                buffer_calibration_dict.get(
                    "proposed_structural_buffer_price"
                )
            )

            candidate_dict["invalidation_ownership_status"] = (
                ownership_dict.get("status")
            )
            candidate_dict["htf_invalidation_available"] = (
                ownership_dict.get("htf_candidate_available") is True
            )
            candidate_dict["h1_only_invalidation_requires_proof"] = (
                ownership_dict.get("h1_only_requires_proof") is True
            )

            meta = candidate_dict.get(
                "_strategy_b_expression_observer"
            )
            meta = dict(meta) if isinstance(meta, Mapping) else {}
            meta["htf_location_status"] = (
                candidate_dict["htf_location_status"]
            )
            meta["htf_location_hard_blocked"] = (
                candidate_dict["htf_location_hard_blocked"]
            )
            meta["htf_location_block_reason"] = (
                candidate_dict["htf_location_block_reason"]
            )
            meta["htf_location_observed"] = (
                candidate_dict["htf_location_observed"]
            )
            meta["structural_invalidation_finalizer_decision"] = (
                candidate_dict["structural_invalidation_finalizer_decision"]
            )
            meta["structural_invalidation_finalizer_reason"] = (
                candidate_dict["structural_invalidation_finalizer_reason"]
            )
            meta["owner_selection_policy"] = (
                candidate_dict["owner_selection_policy"]
            )
            meta["structural_invalidation_valid"] = (
                candidate_dict["structural_invalidation_valid"]
            )
            meta["htf_owner_resolution_decision"] = (
                candidate_dict["htf_owner_resolution_decision"]
            )
            meta["htf_owner_resolution_reason"] = (
                candidate_dict["htf_owner_resolution_reason"]
            )
            meta["resolved_preferred_htf_owner"] = (
                candidate_dict["resolved_preferred_htf_owner"]
            )
            meta["buffer_calibration_reason"] = (
                candidate_dict["buffer_calibration_reason"]
            )
            meta["buffer_calibration_max_breach_atr"] = (
                candidate_dict["buffer_calibration_max_breach_atr"]
            )
            meta["buffer_calibration_ready"] = (
                candidate_dict["buffer_calibration_ready"]
            )
            candidate_dict["_strategy_b_expression_observer"] = meta

            ranker_candidates.append(candidate_dict)
        else:
            failed_count += 1

        rows.append(
            StrategyBLiveExpressionCandidate(
                symbol=symbol,
                side=side,
                acquisition_ok=acquisition_ok,
                acquisition_reason=acquisition_reason,
                sr_source=(
                    str(loaded.get("sr_source"))
                    if loaded.get("sr_source") is not None
                    else None
                ),
                sr_fallback_used=bool(
                    loaded.get("sr_fallback_used") is True
                ),
                bars_available=dict(
                    _mapping(loaded.get("bars_available"))
                ),
                observation=observation_dict,
                htf_location=htf_location_dict,
                structural_invalidation=invalidation_dict,
                invalidation_ownership=ownership_dict,
                htf_invalidation_owner_resolution=owner_resolution_dict,
                structural_buffer_calibration=buffer_calibration_dict,
                structural_invalidation_finalization=finalization_dict,
                ranker_candidate=(
                    dict(candidate_dict)
                    if isinstance(candidate_dict, Mapping)
                    else None
                ),
                forward_path_observation=(
                    dict(forward_path_dict)
                    if isinstance(forward_path_dict, Mapping)
                    else None
                ),
            )
        )

    complete = failed_count == 0 and len(rows) == len(approved)

    ranking_dict: Optional[dict[str, Any]] = None
    if complete:
        try:
            ranking = ranker_fn(
                expected,
                candidates=ranker_candidates,
            )
            ranking_dict = (
                ranking.to_dict()
                if hasattr(ranking, "to_dict")
                else dict(ranking)
                if isinstance(ranking, Mapping)
                else None
            )
        except Exception as exc:
            return StrategyBLiveExpressionBasket(
                schema_version=1,
                model="STRATEGY_B_LIVE_EXPRESSION_BASKET_V6",
                expected_dxy_direction=expected,
                as_of_ms=now_ms,
                approved_count=len(approved),
                observed_count=len(ranker_candidates),
                failed_count=0,
                complete=True,
                decision="WAIT",
                reason=(
                    "EXPRESSION_RANKER_EXCEPTION:"
                    + type(exc).__name__
                ),
                candidates=tuple(rows),
                ranking=None,
            )

    if not complete:
        decision = "WAIT"
        reason = "EXPRESSION_BASKET_INCOMPLETE"
    elif not isinstance(ranking_dict, Mapping):
        decision = "WAIT"
        reason = "EXPRESSION_RANKING_RESULT_MISSING"
    else:
        # The existing ranker is authoritative. Today this should remain WAIT /
        # RANKING_CALIBRATION_REQUIRED until explicit calibration is approved.
        decision = str(ranking_dict.get("decision") or "WAIT")
        reason = str(
            ranking_dict.get("reason")
            or "EXPRESSION_RANKING_REASON_MISSING"
        )

    return StrategyBLiveExpressionBasket(
        schema_version=1,
        model="STRATEGY_B_LIVE_EXPRESSION_BASKET_V6",
        expected_dxy_direction=expected,
        as_of_ms=now_ms,
        approved_count=len(approved),
        observed_count=len(ranker_candidates),
        failed_count=failed_count,
        complete=complete,
        decision=decision,
        reason=reason,
        candidates=tuple(rows),
        ranking=ranking_dict,
    )


__all__ = [
    "StrategyBLiveExpressionCandidate",
    "StrategyBLiveExpressionBasket",
    "observe_strategy_b_expression_basket_live",
]
