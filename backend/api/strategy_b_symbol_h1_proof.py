"""
XauTrendLab Strategy B - Symbol H1 Reversal Proof
=================================================

Pure aggregation layer for selected-symbol H1 reversal evidence.

Outputs:
    H1_REVERSAL_PROVEN
    H1_REVERSAL_DEVELOPING
    H1_REVERSAL_NOT_PROVEN

This module does not require CHOCH on every valid reversal, does not create a
second swing detector, does not invent thresholds, does not use M15 as reversal
authority, and has no broker/risk/Redis/Strategy-A authority.
"""

from __future__ import annotations
from dataclasses import asdict, dataclass
from typing import Any, Mapping, Optional

PROVEN = "H1_REVERSAL_PROVEN"
DEVELOPING = "H1_REVERSAL_DEVELOPING"
NOT_PROVEN = "H1_REVERSAL_NOT_PROVEN"

def _mapping(v: Any) -> Mapping[str, Any]:
    return v if isinstance(v, Mapping) else {}

def _bool(v: Any) -> bool:
    return v is True

def _upper(v: Any) -> str:
    return str(v or "").strip().upper()

@dataclass(frozen=True)
class StrategyBSymbolH1Proof:
    schema_version: int
    model: str
    symbol: str
    side: str
    data_ok: bool
    status: str
    h1_reversal_proven: bool
    h1_reversal_developing: bool
    primary_proof_path: Optional[str]
    proven_paths: tuple[str, ...]
    developing_evidence: tuple[str, ...]
    supporting_evidence: tuple[str, ...]
    reason: str
    reasons: tuple[str, ...]
    observation_only: bool = True
    execution_authority: bool = False
    strategy_a_mutation: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_macro_confirmation_fields(self) -> dict[str, Any]:
        return {
            "h1_reversal_confirmed": self.h1_reversal_proven,
            "h1_reversal_reason": self.reason,
            "h1_confirmation_source": self.primary_proof_path or self.status,
            "_strategy_b_symbol_h1_proof": self.to_dict(),
        }

def evaluate_strategy_b_symbol_h1_proof(
    *,
    symbol: str,
    side: str,
    pattern_observations: Mapping[str, Any],
) -> StrategyBSymbolH1Proof:
    sym = _upper(symbol)
    side_u = _upper(side)
    o = _mapping(pattern_observations)
    meta = _mapping(o.get("_strategy_b_pattern_observer"))

    reasons: list[str] = []
    proven: list[str] = []
    developing: list[str] = []
    supporting: list[str] = []

    if not sym:
        reasons.append("SYMBOL_MISSING")
    if side_u not in ("BUY", "SELL"):
        reasons.append("SIDE_NOT_ACTIONABLE")

    upstream_data_ok = meta.get("data_ok")
    if upstream_data_ok is False:
        reasons.append("H1_PATTERN_INPUT_INCOMPLETE")

    data_ok = bool(sym) and side_u in ("BUY", "SELL") and upstream_data_ok is not False

    choch = _bool(o.get("h1_choch_bos"))
    displacement = _bool(o.get("h1_displacement"))
    retest = _bool(o.get("h1_retest"))
    retest_failure = _bool(o.get("retest_failure"))
    double_top_bottom = _bool(o.get("double_top_bottom"))
    ob_validated = _bool(o.get("h1_order_block_validated"))
    ob_retest = _bool(o.get("h1_order_block_retest"))
    sweep_rejection = _bool(o.get("liquidity_sweep_rejection"))
    failed_breakout = _bool(o.get("failed_breakout_reacceptance"))
    lh_hl = _bool(o.get("lower_high_higher_low"))
    ma_reclaim = _bool(o.get("ma_structure_reclaim"))
    prior_reversal = _bool(o.get("prior_reversal_established")) or _bool(
        o.get("h1_reversal_already_established")
    )

    # Strong proof paths. CHOCH is one path, not a universal prerequisite.
    if retest_failure:
        proven.append("RETEST_FAILURE")
    if choch and displacement and retest:
        proven.append("H1_CHOCH_BOS_DISPLACEMENT_RETEST")
    if ob_validated and ob_retest and displacement:
        proven.append("VALIDATED_H1_ORDER_BLOCK_RETEST")
    if sweep_rejection:
        proven.append("LIQUIDITY_SWEEP_REJECTION")
    if failed_breakout:
        proven.append("FAILED_BREAKOUT_REACCEPTANCE")
    if double_top_bottom:
        proven.append("DOUBLE_TOP_BOTTOM_RETEST_FAILURE")
    if lh_hl and prior_reversal:
        proven.append("LOWER_HIGH_HIGHER_LOW_CONTINUATION")
    elif lh_hl:
        supporting.append("LOWER_HIGH_HIGHER_LOW")
    if ma_reclaim:
        supporting.append("MA_STRUCTURE_RECLAIM")

    # Partial path evidence => DEVELOPING, never PROVEN.
    if choch:
        developing.append("H1_CHOCH_BOS")
    if displacement:
        developing.append("H1_DISPLACEMENT")
    if retest:
        developing.append("H1_RETEST")
    if ob_validated:
        developing.append("H1_ORDER_BLOCK_VALIDATED")
    if ob_retest:
        developing.append("H1_ORDER_BLOCK_RETEST")

    if data_ok and proven:
        status = PROVEN
        reason = "APPROVED_H1_REVERSAL_PROOF"
    elif data_ok and (developing or supporting):
        status = DEVELOPING
        reason = "H1_REVERSAL_EVIDENCE_DEVELOPING"
    else:
        status = NOT_PROVEN
        reason = "H1_PATTERN_INPUT_INCOMPLETE" if not data_ok else "NO_APPROVED_H1_REVERSAL_PROOF"

    if not reasons:
        reasons.append(reason)

    return StrategyBSymbolH1Proof(
        schema_version=1,
        model="STRATEGY_B_SYMBOL_H1_PROOF_V1",
        symbol=sym,
        side=side_u,
        data_ok=data_ok,
        status=status,
        h1_reversal_proven=(status == PROVEN),
        h1_reversal_developing=(status == DEVELOPING),
        primary_proof_path=proven[0] if proven else None,
        proven_paths=tuple(dict.fromkeys(proven)),
        developing_evidence=tuple(dict.fromkeys(developing)),
        supporting_evidence=tuple(dict.fromkeys(supporting)),
        reason=reason,
        reasons=tuple(dict.fromkeys(reasons)),
    )

__all__ = [
    "PROVEN",
    "DEVELOPING",
    "NOT_PROVEN",
    "StrategyBSymbolH1Proof",
    "evaluate_strategy_b_symbol_h1_proof",
]
