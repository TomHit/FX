"""
XauTrendLab Strategy B - Structural Invalidation Finalizer
==========================================================

Fail-closed final owner + structural-stop geometry for Strategy B.

Source-of-truth rules preserved:
- Initial stop authority is H4/D1 structure; H1 is allowed only when valid.
- No nearest/widest/default D1-vs-H4 rule is invented.
- If both D1 and H4 are valid, caller must explicitly provide preferred_htf_owner.
- H1-only fallback requires approved H1 reversal proof.
- Buffer is mandatory and explicit; there is no default.
- Only after owner + correct-side price + explicit buffer produce a valid stop do
  we set structural_invalidation_valid=True.

This module does not size lots, reserve risk, place/modify orders, write Redis,
use Redis SCAN, or mutate Strategy A.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping, Optional


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


def _candidate_by_tf(
    invalidation_observation: Mapping[str, Any],
    tf: str,
) -> Mapping[str, Any]:
    tf_u = _upper(tf)
    for row in invalidation_observation.get("candidates") or ():
        if not isinstance(row, Mapping):
            continue
        if _upper(row.get("timeframe")) == tf_u:
            return row
    return {}


@dataclass(frozen=True)
class StrategyBStructuralInvalidationFinalization:
    schema_version: int
    model: str

    symbol: str
    side: str
    entry_price: Optional[float]

    decision: str
    reason: str
    reasons: tuple[str, ...]

    ownership_status: str

    selected_timeframe: Optional[str]
    selected_source: Optional[str]
    selected_kind: Optional[str]
    structural_invalidation_price: Optional[float]

    structural_buffer_price: Optional[float]
    proposed_stop_price: Optional[float]
    stop_distance_price: Optional[float]

    h1_reversal_proven: bool
    h1_primary_proof_path: Optional[str]

    preferred_htf_owner: Optional[str]
    owner_selection_policy: str

    structural_invalidation_valid: bool

    observation_only: bool = True
    execution_authority: bool = False
    risk_reservation_authority: bool = False
    strategy_a_mutation: bool = False
    redis_scan_used: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _build_stop(
    *,
    side: str,
    entry: float,
    invalidation: float,
    buffer_price: float,
) -> tuple[Optional[float], Optional[float], str]:
    if buffer_price < 0:
        return None, None, "STRUCTURAL_BUFFER_INVALID"

    if side == "BUY":
        if invalidation >= entry:
            return None, None, "BUY_INVALIDATION_NOT_BELOW_ENTRY"
        stop = invalidation - buffer_price
        if stop <= 0 or stop >= entry:
            return None, None, "BUY_STOP_INVALID"
        return float(stop), float(entry - stop), "STRUCTURAL_STOP_OK"

    if side == "SELL":
        if invalidation <= entry:
            return None, None, "SELL_INVALIDATION_NOT_ABOVE_ENTRY"
        stop = invalidation + buffer_price
        if stop <= entry:
            return None, None, "SELL_STOP_INVALID"
        return float(stop), float(stop - entry), "STRUCTURAL_STOP_OK"

    return None, None, "SIDE_NOT_ACTIONABLE"


def finalize_strategy_b_structural_invalidation(
    *,
    symbol: str,
    side: str,
    invalidation_observation: Mapping[str, Any] | None,
    ownership_observation: Mapping[str, Any] | None,
    h1_proof: Mapping[str, Any] | None,
    structural_buffer_price: Any,
    preferred_htf_owner: Optional[str] = None,
) -> StrategyBStructuralInvalidationFinalization:
    sym = _upper(symbol)
    side_u = _upper(side)
    inv = _mapping(invalidation_observation)
    own = _mapping(ownership_observation)
    proof = _mapping(h1_proof)

    reasons: list[str] = []

    entry = _num(inv.get("entry_price"))
    buffer_price = _num(structural_buffer_price)

    ownership_status = _upper(own.get("status")) or "UNKNOWN"
    preferred = _upper(preferred_htf_owner) or None

    h1_proven = proof.get("h1_reversal_proven") is True
    primary_path = (
        str(proof.get("primary_proof_path")).strip()
        if proof.get("primary_proof_path") is not None
        else None
    )

    selected_tf: Optional[str] = None
    selected: Mapping[str, Any] = {}
    policy = "FAIL_CLOSED_NO_DEFAULT_HTF_PRECEDENCE"

    if side_u not in ("BUY", "SELL"):
        reasons.append("SIDE_NOT_ACTIONABLE")
    if entry is None or entry <= 0:
        reasons.append("ENTRY_PRICE_INVALID")
    if not inv:
        reasons.append("INVALIDATION_OBSERVATION_MISSING")
    if not own:
        reasons.append("INVALIDATION_OWNERSHIP_OBSERVATION_MISSING")

    if preferred is not None and preferred not in ("D1", "H4"):
        reasons.append("PREFERRED_HTF_OWNER_INVALID")

    d1 = _candidate_by_tf(inv, "D1")
    h4 = _candidate_by_tf(inv, "H4")
    h1 = _candidate_by_tf(inv, "H1")

    def usable(row: Mapping[str, Any]) -> bool:
        return bool(
            row
            and row.get("correct_side_of_entry") is True
            and row.get("data_ok") is True
            and row.get("fresh") is not False
            and _num(row.get("price")) is not None
        )

    d1_ok = usable(d1)
    h4_ok = usable(h4)
    h1_ok = usable(h1)

    if not reasons:
        if ownership_status == "HTF_INVALIDATION_AVAILABLE":
            if d1_ok and h4_ok:
                if preferred is None:
                    reasons.append("HTF_OWNER_POLICY_REQUIRED_D1_OR_H4")
                elif preferred == "D1":
                    selected_tf, selected = "D1", d1
                    policy = "EXPLICIT_PREFERRED_HTF_OWNER_D1"
                elif preferred == "H4":
                    selected_tf, selected = "H4", h4
                    policy = "EXPLICIT_PREFERRED_HTF_OWNER_H4"
            elif d1_ok:
                selected_tf, selected = "D1", d1
                policy = "ONLY_USABLE_HTF_CANDIDATE_D1"
            elif h4_ok:
                selected_tf, selected = "H4", h4
                policy = "ONLY_USABLE_HTF_CANDIDATE_H4"
            else:
                reasons.append("HTF_STATUS_WITHOUT_USABLE_D1_H4_CANDIDATE")

        elif ownership_status == "H1_ONLY_INVALIDATION_REQUIRES_PROOF":
            if not h1_ok:
                reasons.append("H1_ONLY_STATUS_WITHOUT_USABLE_H1_CANDIDATE")
            elif not h1_proven:
                reasons.append("H1_ONLY_INVALIDATION_REQUIRES_APPROVED_H1_PROOF")
            else:
                selected_tf, selected = "H1", h1
                policy = "H1_ONLY_AFTER_APPROVED_H1_REVERSAL_PROOF"

        elif ownership_status == "NO_STRUCTURAL_INVALIDATION":
            reasons.append("NO_STRUCTURAL_INVALIDATION")
        elif ownership_status == "INVALIDATION_INPUT_INCOMPLETE":
            reasons.append("INVALIDATION_INPUT_INCOMPLETE")
        else:
            reasons.append("INVALIDATION_OWNERSHIP_STATUS_NOT_ACTIONABLE")

    invalidation_price = _num(selected.get("price")) if selected else None
    selected_source = (
        str(selected.get("source")).strip()
        if selected.get("source") is not None
        else None
    )
    selected_kind = (
        str(selected.get("kind")).strip()
        if selected.get("kind") is not None
        else None
    )

    proposed_stop = None
    stop_distance = None

    if selected_tf is not None and invalidation_price is not None:
        if buffer_price is None:
            reasons.append("STRUCTURAL_BUFFER_NOT_CALIBRATED")
        else:
            proposed_stop, stop_distance, stop_reason = _build_stop(
                side=side_u,
                entry=float(entry),
                invalidation=float(invalidation_price),
                buffer_price=float(buffer_price),
            )
            reasons.append(stop_reason)

    valid = bool(
        selected_tf is not None
        and invalidation_price is not None
        and buffer_price is not None
        and proposed_stop is not None
        and stop_distance is not None
        and stop_distance > 0
        and "STRUCTURAL_STOP_OK" in reasons
        and not any(
            r in reasons
            for r in (
                "HTF_OWNER_POLICY_REQUIRED_D1_OR_H4",
                "H1_ONLY_INVALIDATION_REQUIRES_APPROVED_H1_PROOF",
                "STRUCTURAL_BUFFER_NOT_CALIBRATED",
                "PREFERRED_HTF_OWNER_INVALID",
            )
        )
    )

    if valid:
        decision = "STRUCTURAL_INVALIDATION_READY"
        reason = "OWNER_AND_BUFFER_RESOLVED"
    else:
        decision = "WAIT"
        reason = reasons[0] if reasons else "STRUCTURAL_INVALIDATION_NOT_READY"

    return StrategyBStructuralInvalidationFinalization(
        schema_version=1,
        model="STRATEGY_B_STRUCTURAL_INVALIDATION_FINALIZER_V1",
        symbol=sym,
        side=side_u,
        entry_price=entry,
        decision=decision,
        reason=reason,
        reasons=tuple(dict.fromkeys(reasons)),
        ownership_status=ownership_status,
        selected_timeframe=selected_tf,
        selected_source=selected_source,
        selected_kind=selected_kind,
        structural_invalidation_price=invalidation_price,
        structural_buffer_price=buffer_price,
        proposed_stop_price=proposed_stop,
        stop_distance_price=stop_distance,
        h1_reversal_proven=h1_proven,
        h1_primary_proof_path=primary_path,
        preferred_htf_owner=preferred,
        owner_selection_policy=policy,
        structural_invalidation_valid=valid,
    )


__all__ = [
    "StrategyBStructuralInvalidationFinalization",
    "finalize_strategy_b_structural_invalidation",
]
