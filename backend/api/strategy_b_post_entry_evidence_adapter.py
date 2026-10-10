from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping, Optional


def _mapping(v: Any) -> Mapping[str, Any]:
    return v if isinstance(v, Mapping) else {}


def _upper(v: Any) -> str:
    return str(v or "").strip().upper()


def _num(v: Any) -> Optional[float]:
    try:
        n = float(v)
    except (TypeError, ValueError):
        return None
    return n


def _true(v: Any) -> bool:
    return v is True


@dataclass(frozen=True)
class StrategyBPostEntryEvidence:
    schema_version: int
    model: str
    ready: bool
    reason: str
    reasons: tuple[str, ...]
    thesis_evidence: Mapping[str, Any]
    event_evidence: Mapping[str, Any]
    account_safety: Mapping[str, Any]
    observation_only: bool = True
    execution_authority: bool = False
    broker_modify_authority: bool = False
    broker_close_authority: bool = False
    strategy_a_mutation: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def build_strategy_b_post_entry_evidence(
    *,
    trade: Mapping[str, Any],
    symbol_observation: Mapping[str, Any],
    live_inputs: Any,
    macro_qualification: Any,
    event_evidence: Optional[Mapping[str, Any]] = None,
    account_safety: Optional[Mapping[str, Any]] = None,
) -> StrategyBPostEntryEvidence:
    """
    Pure Strategy-B post-entry evidence adapter.

    This function does not read Redis, inspect the broker, reserve/release risk,
    modify/close a position, or mutate Strategy-A state.

    Critical rule:
    missing/unknown mandatory observations make the adapter NOT_READY.
    They are never converted into false thesis-validity booleans.
    """
    tr = _mapping(trade)
    sym_obs = _mapping(symbol_observation)
    event = dict(_mapping(event_evidence))
    safety = dict(_mapping(account_safety))
    reasons: list[str] = []

    strategy = _upper(tr.get("strategy"))
    if strategy != "B":
        reasons.append("NOT_STRATEGY_B")

    symbol = _upper(tr.get("symbol"))
    side = _upper(tr.get("side") or tr.get("direction"))

    if not symbol:
        reasons.append("SYMBOL_MISSING")
    if side not in ("BUY", "SELL"):
        reasons.append("SIDE_NOT_ACTIONABLE")

    thesis_id = str(tr.get("thesis_id") or "").strip()
    selected_capture_id = str(
        tr.get("selected_capture_id") or ""
    ).strip()

    if not thesis_id:
        reasons.append("THESIS_ID_MISSING")
    if not selected_capture_id:
        reasons.append("SELECTED_CAPTURE_ID_MISSING")

    if sym_obs.get("ok") is not True:
        reasons.append("SYMBOL_OBSERVATION_NOT_READY")

    snapshot = _mapping(sym_obs.get("snapshot"))
    if not snapshot:
        reasons.append("SYMBOL_SNAPSHOT_MISSING")

    live_data_ok = getattr(live_inputs, "data_ok", None)
    if live_data_ok is not True:
        reasons.append("DXY_LIVE_INPUTS_NOT_READY")

    # Post-entry thesis validity must not be inferred from the pre-entry
    # macro_qualified aggregate. That aggregate may become False for temporary
    # WAIT conditions and therefore is not equivalent to thesis invalidation.
    #
    # Runtime must provide explicit post-entry validity facts after evaluating
    # current evidence with the dedicated post-entry policy.
    macro_thesis_valid = getattr(
        macro_qualification,
        "post_entry_macro_thesis_valid",
        None,
    )
    if not isinstance(macro_thesis_valid, bool):
        reasons.append("MACRO_THESIS_VALIDITY_UNKNOWN")

    structural_price = _num(
        tr.get("structural_invalidation_price")
        or tr.get("initial_sl")
    )
    if structural_price is None or structural_price <= 0:
        reasons.append("STRUCTURAL_INVALIDATION_PRICE_MISSING")

    current_price = _num(
        snapshot.get("price")
        or sym_obs.get("price")
    )
    if current_price is None or current_price <= 0:
        reasons.append("CURRENT_SYMBOL_PRICE_MISSING")

    symbol_structure_valid: Optional[bool] = None
    if (
        structural_price is not None
        and structural_price > 0
        and current_price is not None
        and current_price > 0
        and side in ("BUY", "SELL")
    ):
        if side == "BUY":
            symbol_structure_valid = current_price > structural_price
        else:
            symbol_structure_valid = current_price < structural_price

    if not isinstance(symbol_structure_valid, bool):
        reasons.append("SYMBOL_STRUCTURE_VALIDITY_UNKNOWN")

    dxy_structure = _mapping(
        getattr(live_inputs, "dxy_structure", {})
    )
    dxy_structure_valid: Optional[bool] = None

    # This adapter deliberately does not invent a new DXY structural model.
    # Until an explicit current DXY structural-validity fact is supplied by
    # the canonical live pipeline, the post-entry thesis evaluator must not
    # receive a fabricated True/False value.
    explicit_dxy_valid = dxy_structure.get(
        "strategy_b_thesis_structure_valid"
    )
    if isinstance(explicit_dxy_valid, bool):
        dxy_structure_valid = explicit_dxy_valid
    else:
        reasons.append("DXY_STRUCTURE_VALIDITY_UNKNOWN")

    favorable = snapshot.get(
        "favorable_h1_h4_structure_confirmed"
    )
    weakened = snapshot.get(
        "thesis_materially_weakened"
    )

    thesis_evidence: dict[str, Any] = {}

    ready = not reasons

    if ready:
        thesis_evidence = {
            "macro_thesis_valid": bool(macro_thesis_valid),
            "symbol_structure_valid": bool(
                symbol_structure_valid
            ),
            "dxy_structure_valid": bool(
                dxy_structure_valid
            ),
            "favorable_h1_h4_structure_confirmed": (
                _true(favorable)
            ),
            "thesis_materially_weakened": _true(weakened),
            "thesis_id": thesis_id,
            "selected_capture_id": selected_capture_id,
            "symbol": symbol,
            "side": side,
            "current_price": current_price,
            "structural_invalidation_price": structural_price,
        }

    return StrategyBPostEntryEvidence(
        schema_version=1,
        model="STRATEGY_B_POST_ENTRY_EVIDENCE_ADAPTER_V1",
        ready=ready,
        reason=(
            "POST_ENTRY_EVIDENCE_READY"
            if ready
            else reasons[0]
        ),
        reasons=tuple(dict.fromkeys(reasons)),
        thesis_evidence=thesis_evidence,
        event_evidence=event,
        account_safety=safety,
    )



def build_strategy_b_post_entry_evidence_from_validity(
    *,
    trade: Mapping[str, Any],
    symbol_observation: Mapping[str, Any],
    live_inputs: Any,
    validity: Any,
    event_evidence: Optional[Mapping[str, Any]] = None,
    account_safety: Optional[Mapping[str, Any]] = None,
) -> StrategyBPostEntryEvidence:
    """
    Compose the pure post-entry validity policy with the pure evidence adapter.

    NOT_READY validity is never converted into False thesis evidence.
    No Redis, broker, risk, or Strategy-A authority exists here.
    """
    validity_ready = getattr(validity, "ready", None)

    if validity_ready is not True:
        reason = str(
            getattr(validity, "reason", None)
            or "POST_ENTRY_VALIDITY_NOT_READY"
        )

        return StrategyBPostEntryEvidence(
            schema_version=1,
            model="STRATEGY_B_POST_ENTRY_EVIDENCE_ADAPTER_V1",
            ready=False,
            reason=reason,
            reasons=(reason,),
            thesis_evidence={},
            event_evidence=dict(_mapping(event_evidence)),
            account_safety=dict(_mapping(account_safety)),
        )

    macro_valid = getattr(validity, "macro_thesis_valid", None)
    dxy_valid = getattr(validity, "dxy_structure_valid", None)

    if not isinstance(macro_valid, bool):
        reason = "MACRO_THESIS_VALIDITY_UNKNOWN"
        return StrategyBPostEntryEvidence(
            schema_version=1,
            model="STRATEGY_B_POST_ENTRY_EVIDENCE_ADAPTER_V1",
            ready=False,
            reason=reason,
            reasons=(reason,),
            thesis_evidence={},
            event_evidence=dict(_mapping(event_evidence)),
            account_safety=dict(_mapping(account_safety)),
        )

    if not isinstance(dxy_valid, bool):
        reason = "DXY_STRUCTURE_VALIDITY_UNKNOWN"
        return StrategyBPostEntryEvidence(
            schema_version=1,
            model="STRATEGY_B_POST_ENTRY_EVIDENCE_ADAPTER_V1",
            ready=False,
            reason=reason,
            reasons=(reason,),
            thesis_evidence={},
            event_evidence=dict(_mapping(event_evidence)),
            account_safety=dict(_mapping(account_safety)),
        )

    class _MacroPostEntryView:
        post_entry_macro_thesis_valid = macro_valid

    base_structure = dict(
        _mapping(getattr(live_inputs, "dxy_structure", {}))
    )
    base_structure["strategy_b_thesis_structure_valid"] = dxy_valid

    class _LivePostEntryView:
        data_ok = getattr(live_inputs, "data_ok", None)
        dxy_structure = base_structure

    return build_strategy_b_post_entry_evidence(
        trade=trade,
        symbol_observation=symbol_observation,
        live_inputs=_LivePostEntryView(),
        macro_qualification=_MacroPostEntryView(),
        event_evidence=event_evidence,
        account_safety=account_safety,
    )


__all__ = [
    "StrategyBPostEntryEvidence",
    "build_strategy_b_post_entry_evidence",
    "build_strategy_b_post_entry_evidence_from_validity",
]
