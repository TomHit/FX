"""
XauTrendLab Strategy B - Structural Invalidation Ownership Observer
==================================================================

Pure observation-only classification of structural invalidation readiness.

Inputs
------
Consumes the output of strategy_b_structural_invalidation_observer.py.

States
------
HTF_INVALIDATION_AVAILABLE
    At least one correct-side D1 and/or H4 structural invalidation candidate
    exists.

H1_ONLY_INVALIDATION_REQUIRES_PROOF
    No correct-side D1/H4 candidate exists, but an H1 candidate exists.
    This is NOT promoted to a valid stop automatically.

NO_STRUCTURAL_INVALIDATION
    Structural candidates were observed, but none are on the correct side.

INVALIDATION_INPUT_INCOMPLETE
    Upstream invalidation observation is missing/incomplete.

Important
---------
This module does NOT:
- select D1 vs H4 vs H1;
- choose nearest/widest invalidation;
- add a stop buffer;
- set structural_invalidation_valid=True;
- size risk;
- place orders;
- mutate Strategy A.

It only classifies readiness so downstream diagnostics can distinguish a usable
HTF structural candidate from an H1-only fallback or a complete absence of
valid structural invalidation.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping, Optional


def _mapping(v: Any) -> Mapping[str, Any]:
    return v if isinstance(v, Mapping) else {}


def _upper(v: Any) -> str:
    return str(v or "").strip().upper()


@dataclass(frozen=True)
class StrategyBInvalidationOwnershipObservation:
    schema_version: int
    model: str

    symbol: str
    side: str

    data_ok: bool
    status: str
    reason: str
    reasons: tuple[str, ...]

    htf_candidate_available: bool
    d1_candidate_available: bool
    h4_candidate_available: bool
    h1_candidate_available: bool
    h1_only_requires_proof: bool

    correct_side_candidates: tuple[str, ...]
    invalid_side_candidates: tuple[str, ...]

    # Intentionally unresolved.
    selected_timeframe: Optional[str]
    structural_invalidation_price: Optional[float]
    structural_invalidation_valid: bool

    observation_only: bool = True
    execution_authority: bool = False
    risk_authority: bool = False
    strategy_a_mutation: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def observe_strategy_b_invalidation_ownership(
    *,
    symbol: str,
    side: str,
    invalidation_observation: Mapping[str, Any] | None,
) -> StrategyBInvalidationOwnershipObservation:
    sym = _upper(symbol)
    side_u = _upper(side)
    inv = _mapping(invalidation_observation)

    reasons: list[str] = []

    if not inv:
        status = "INVALIDATION_INPUT_INCOMPLETE"
        reasons.append("INVALIDATION_OBSERVATION_MISSING")
        correct = ()
        invalid = ()
        d1 = h4 = h1 = False
        htf = False
        h1_only = False
        data_ok = False
    else:
        correct = tuple(
            _upper(x)
            for x in (inv.get("correct_side_candidates") or ())
            if _upper(x)
        )
        invalid = tuple(
            _upper(x)
            for x in (inv.get("invalid_side_candidates") or ())
            if _upper(x)
        )

        d1 = "D1" in correct
        h4 = "H4" in correct
        h1 = "H1" in correct
        htf = bool(d1 or h4)
        h1_only = bool(h1 and not htf)

        upstream_observed = (
            inv.get("structural_invalidation_observed") is True
        )

        if htf:
            status = "HTF_INVALIDATION_AVAILABLE"
            reasons.append("D1_OR_H4_CORRECT_SIDE_CANDIDATE_AVAILABLE")
            data_ok = True
        elif h1_only:
            status = "H1_ONLY_INVALIDATION_REQUIRES_PROOF"
            reasons.append("NO_D1_H4_CORRECT_SIDE_CANDIDATE")
            reasons.append("H1_CORRECT_SIDE_CANDIDATE_AVAILABLE")
            reasons.append("H1_INVALIDATION_NOT_AUTO_APPROVED")
            data_ok = True
        elif upstream_observed:
            # Defensive: upstream says observed but candidate lists do not
            # contain a recognized timeframe.
            status = "INVALIDATION_INPUT_INCOMPLETE"
            reasons.append("INVALIDATION_CANDIDATE_TIMEFRAME_UNRECOGNIZED")
            data_ok = False
        elif inv.get("data_ok") is False:
            # Distinguish true no-valid-candidate from malformed/missing input.
            if (
                inv.get("reason")
                == "NO_VALID_STRUCTURAL_INVALIDATION_CANDIDATE"
            ):
                status = "NO_STRUCTURAL_INVALIDATION"
                reasons.append("NO_CORRECT_SIDE_STRUCTURAL_CANDIDATE")
                data_ok = True
            else:
                status = "INVALIDATION_INPUT_INCOMPLETE"
                reasons.append(
                    str(inv.get("reason") or "INVALIDATION_INPUT_INCOMPLETE")
                )
                data_ok = False
        else:
            status = "NO_STRUCTURAL_INVALIDATION"
            reasons.append("NO_CORRECT_SIDE_STRUCTURAL_CANDIDATE")
            data_ok = True

    # This observer classifies readiness only. Final owner/buffer remain open.
    return StrategyBInvalidationOwnershipObservation(
        schema_version=1,
        model="STRATEGY_B_INVALIDATION_OWNERSHIP_OBSERVER_V1",
        symbol=sym,
        side=side_u,
        data_ok=data_ok,
        status=status,
        reason=status,
        reasons=tuple(dict.fromkeys(reasons)),
        htf_candidate_available=htf,
        d1_candidate_available=d1,
        h4_candidate_available=h4,
        h1_candidate_available=h1,
        h1_only_requires_proof=h1_only,
        correct_side_candidates=correct,
        invalid_side_candidates=invalid,
        selected_timeframe=None,
        structural_invalidation_price=None,
        structural_invalidation_valid=False,
    )


__all__ = [
    "StrategyBInvalidationOwnershipObservation",
    "observe_strategy_b_invalidation_ownership",
]
