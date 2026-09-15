"""
XauTrendLab Strategy B - HTF Structural Invalidation Owner Resolver
===================================================================

Conservative, fail-closed D1/H4 owner resolver for Strategy B.

Purpose
-------
Resolve a preferred HTF invalidation owner only when the structural/catalyst
sequence makes that ownership deterministic enough to hand to the existing
structural invalidation finalizer.

Rules
-----
1) If only one usable HTF candidate exists -> that timeframe owns by availability.
2) If D1 and H4 are both usable:
   - never choose by nearest, widest, newest, or timeframe precedence alone;
   - normalize pivot timestamps to milliseconds;
   - require canonical catalyst_first_seen_ms;
   - require approved H1 reversal proof + h1_break_ms;
   - classify each HTF pivot against the causal window:
         catalyst_first_seen_ms <= pivot_ms <= h1_break_ms
   - if EXACTLY ONE of D1/H4 falls inside that causal window -> select it;
   - if both or neither qualify -> WAIT / unresolved.

This module does not add a stop buffer, size risk, reserve risk, place orders,
write Redis, use Redis SCAN, or mutate Strategy A.
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


def _ms(v: Any) -> int:
    """
    Normalize epoch seconds / milliseconds / microseconds into milliseconds.
    """
    try:
        n = int(float(v or 0))
    except Exception:
        return 0
    if n <= 0:
        return 0
    # seconds
    if n < 10_000_000_000:
        return n * 1000
    # milliseconds
    if n < 10_000_000_000_000:
        return n
    # microseconds
    if n < 10_000_000_000_000_000:
        return n // 1000
    # nanoseconds or larger
    return n // 1_000_000


def _candidate(
    invalidation_observation: Mapping[str, Any],
    tf: str,
) -> Mapping[str, Any]:
    tf_u = _upper(tf)
    for row in invalidation_observation.get("candidates") or ():
        if isinstance(row, Mapping) and _upper(row.get("timeframe")) == tf_u:
            return row
    return {}


def _usable(row: Mapping[str, Any]) -> bool:
    return bool(
        row
        and row.get("correct_side_of_entry") is True
        and row.get("data_ok") is True
        and row.get("fresh") is not False
        and _num(row.get("price")) is not None
        and row.get("pivot_metadata_ok") is True
        and _ms(row.get("pivot_ms")) > 0
    )


@dataclass(frozen=True)
class StrategyBHTFInvalidationOwnerResolution:
    schema_version: int
    model: str

    symbol: str
    side: str

    decision: str
    reason: str
    reasons: tuple[str, ...]

    catalyst_first_seen_ms: int
    h1_break_ms: int
    h1_reversal_proven: bool
    h1_primary_proof_path: Optional[str]

    d1_usable: bool
    h4_usable: bool

    d1_pivot_ms: int
    h4_pivot_ms: int

    d1_relation_to_catalyst: str
    h4_relation_to_catalyst: str

    d1_in_causal_reversal_window: bool
    h4_in_causal_reversal_window: bool

    preferred_htf_owner: Optional[str]
    owner_selection_policy: str

    observation_only: bool = True
    execution_authority: bool = False
    risk_authority: bool = False
    strategy_a_mutation: bool = False
    redis_scan_used: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _relation(pivot_ms: int, catalyst_ms: int) -> str:
    if pivot_ms <= 0:
        return "PIVOT_TIME_MISSING"
    if catalyst_ms <= 0:
        return "CATALYST_TIME_MISSING"
    if pivot_ms < catalyst_ms:
        return "PRE_CATALYST"
    if pivot_ms == catalyst_ms:
        return "AT_CATALYST"
    return "POST_CATALYST"


def resolve_strategy_b_htf_invalidation_owner(
    *,
    symbol: str,
    side: str,
    invalidation_observation: Mapping[str, Any] | None,
    catalyst_first_seen_ms: Any,
    h1_proof: Mapping[str, Any] | None,
    h1_break_ms: Any = None,
) -> StrategyBHTFInvalidationOwnerResolution:
    sym = _upper(symbol)
    side_u = _upper(side)
    inv = _mapping(invalidation_observation)
    proof = _mapping(h1_proof)

    catalyst_ms = _ms(catalyst_first_seen_ms)
    break_ms = _ms(
        h1_break_ms
        if h1_break_ms is not None
        else proof.get("break_ms")
    )

    h1_proven = proof.get("h1_reversal_proven") is True
    primary_path = (
        str(proof.get("primary_proof_path")).strip()
        if proof.get("primary_proof_path") is not None
        else None
    )

    d1 = _candidate(inv, "D1")
    h4 = _candidate(inv, "H4")

    d1_ok = _usable(d1)
    h4_ok = _usable(h4)

    d1_pivot_ms = _ms(d1.get("pivot_ms"))
    h4_pivot_ms = _ms(h4.get("pivot_ms"))

    d1_rel = _relation(d1_pivot_ms, catalyst_ms)
    h4_rel = _relation(h4_pivot_ms, catalyst_ms)

    reasons: list[str] = []
    owner: Optional[str] = None
    policy = "FAIL_CLOSED_NO_DEFAULT_HTF_PRECEDENCE"

    if side_u not in ("BUY", "SELL"):
        reasons.append("SIDE_NOT_ACTIONABLE")

    # Availability alone is deterministic when only one HTF candidate is usable.
    if not reasons:
        if d1_ok and not h4_ok:
            owner = "D1"
            policy = "ONLY_USABLE_HTF_CANDIDATE_D1"
        elif h4_ok and not d1_ok:
            owner = "H4"
            policy = "ONLY_USABLE_HTF_CANDIDATE_H4"
        elif not d1_ok and not h4_ok:
            reasons.append("NO_USABLE_HTF_INVALIDATION_CANDIDATE")

    d1_window = False
    h4_window = False

    if not reasons and d1_ok and h4_ok:
        if catalyst_ms <= 0:
            reasons.append("CATALYST_FIRST_SEEN_TIME_REQUIRED")
        elif not h1_proven:
            reasons.append("DUAL_HTF_OWNER_REQUIRES_APPROVED_H1_PROOF")
        elif break_ms <= 0:
            reasons.append("DUAL_HTF_OWNER_REQUIRES_H1_BREAK_TIME")
        elif break_ms < catalyst_ms:
            reasons.append("H1_BREAK_PRECEDES_CATALYST")
        else:
            d1_window = bool(
                catalyst_ms <= d1_pivot_ms <= break_ms
            )
            h4_window = bool(
                catalyst_ms <= h4_pivot_ms <= break_ms
            )

            if d1_window and not h4_window:
                owner = "D1"
                policy = "ONLY_D1_PIVOT_IN_CATALYST_TO_H1_BREAK_WINDOW"
            elif h4_window and not d1_window:
                owner = "H4"
                policy = "ONLY_H4_PIVOT_IN_CATALYST_TO_H1_BREAK_WINDOW"
            elif d1_window and h4_window:
                reasons.append("BOTH_D1_H4_IN_CAUSAL_REVERSAL_WINDOW")
            else:
                reasons.append("NO_D1_H4_PIVOT_IN_CAUSAL_REVERSAL_WINDOW")

    if owner is not None:
        decision = "HTF_OWNER_RESOLVED"
        reason = policy
    else:
        decision = "WAIT"
        reason = reasons[0] if reasons else "HTF_OWNER_UNRESOLVED"

    return StrategyBHTFInvalidationOwnerResolution(
        schema_version=1,
        model="STRATEGY_B_HTF_INVALIDATION_OWNER_RESOLVER_V1",
        symbol=sym,
        side=side_u,
        decision=decision,
        reason=reason,
        reasons=tuple(dict.fromkeys(reasons)),
        catalyst_first_seen_ms=catalyst_ms,
        h1_break_ms=break_ms,
        h1_reversal_proven=h1_proven,
        h1_primary_proof_path=primary_path,
        d1_usable=d1_ok,
        h4_usable=h4_ok,
        d1_pivot_ms=d1_pivot_ms,
        h4_pivot_ms=h4_pivot_ms,
        d1_relation_to_catalyst=d1_rel,
        h4_relation_to_catalyst=h4_rel,
        d1_in_causal_reversal_window=d1_window,
        h4_in_causal_reversal_window=h4_window,
        preferred_htf_owner=owner,
        owner_selection_policy=policy,
    )


__all__ = [
    "StrategyBHTFInvalidationOwnerResolution",
    "resolve_strategy_b_htf_invalidation_owner",
]
