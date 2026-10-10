"""Versioned policy registry (Day 2, brief sections 4, 5, 12).

IN-MEMORY. DETERMINISTIC. SIDE-EFFECT FREE.
No Redis, database, HTTP, filesystem, environment variables or clock reads.

Resolution semantics
--------------------
* Scope = (firm_id, product, program, phase). Matching is exact.
* The evaluation date is the UTC calendar date of the supplied aware datetime.
* A version is effective on D when effective_from <= D and
  (effective_to is None or D <= effective_to). Both bounds are inclusive.
* Registration rejects: invalid policies, a duplicate (scope, version), and any
  version whose effective period overlaps another version of the same scope.
* Resolution returns exactly one of RESOLVED, NO_POLICY, NOT_EFFECTIVE,
  AMBIGUOUS. AMBIGUOUS (more than one effective version) never selects one.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Iterable, Optional

from ..domain._validation import ContractError, require_aware
from ..domain.account import Profile
from ..domain.enums import Phase, StrEnum
from ..domain.policy import FirmPolicy, PolicyContext
from .validation import validate_firm_policy


class PolicyRegistrationError(ContractError):
    pass


class PolicyAmbiguityError(ContractError):
    pass


@dataclass(frozen=True)
class PolicyScope:
    firm_id: str
    product: str
    program: str
    phase: Phase

    @classmethod
    def of_policy(cls, policy: FirmPolicy) -> "PolicyScope":
        return cls(policy.firm_id, policy.product, policy.program, policy.phase)

    @classmethod
    def of_profile(cls, profile: Profile) -> "PolicyScope":
        return cls(profile.firm_id, profile.product, profile.program, profile.phase)

    @property
    def label(self) -> str:
        return f"{self.firm_id}/{self.product}/{self.program}/{self.phase}"


class ResolutionStatus(StrEnum):
    RESOLVED = "RESOLVED"
    NO_POLICY = "NO_POLICY"
    NOT_EFFECTIVE = "NOT_EFFECTIVE"
    AMBIGUOUS = "AMBIGUOUS"


@dataclass(frozen=True)
class PolicyResolution:
    status: ResolutionStatus
    scope: PolicyScope
    on_date: date
    reason: str
    policy: Optional[FirmPolicy] = None

    @property
    def resolved(self) -> bool:
        return self.status is ResolutionStatus.RESOLVED


def evaluation_date(at: datetime) -> date:
    """UTC calendar date used for effective-period checks."""
    require_aware("at", at)
    return at.astimezone(timezone.utc).date()


def periods_overlap(a: FirmPolicy, b: FirmPolicy) -> bool:
    a_end = a.effective_to or date.max
    b_end = b.effective_to or date.max
    return a.effective_from <= b_end and b.effective_from <= a_end


def _order(p: FirmPolicy) -> tuple[date, str]:
    return (p.effective_from, p.version)


def resolve_from(policies: Iterable[FirmPolicy], scope: PolicyScope, at: datetime) -> PolicyResolution:
    """Pure resolution over any policy collection. Never picks among overlaps."""
    on = evaluation_date(at)
    candidates = sorted((p for p in policies if PolicyScope.of_policy(p) == scope), key=_order)
    if not candidates:
        return PolicyResolution(ResolutionStatus.NO_POLICY, scope, on,
                                f"no policy registered for {scope.label}")

    effective = [p for p in candidates if p.is_effective(on)]
    if len(effective) > 1:
        versions = ", ".join(p.version for p in effective)
        return PolicyResolution(ResolutionStatus.AMBIGUOUS, scope, on,
                                f"{len(effective)} versions effective on {on} for {scope.label} "
                                f"({versions}); refusing to select one")
    if len(effective) == 1:
        p = effective[0]
        return PolicyResolution(ResolutionStatus.RESOLVED, scope, on,
                                f"resolved {scope.label} version {p.version} for {on}", p)

    upcoming = [p for p in candidates if p.effective_from > on]
    if upcoming:
        nxt = upcoming[0]
        reason = (f"no version of {scope.label} effective on {on}; next version {nxt.version} "
                  f"is effective from {nxt.effective_from}")
    else:
        last = max(candidates, key=lambda p: p.effective_to or date.max)
        reason = (f"no version of {scope.label} effective on {on}; latest version {last.version} "
                  f"expired after {last.effective_to}")
    return PolicyResolution(ResolutionStatus.NOT_EFFECTIVE, scope, on, reason)


class PolicyRegistry:
    """In-memory, deterministic, side-effect-free store of versioned policies.

    Registration is atomic: if any policy in a batch is rejected, the registry is
    left unchanged. Not thread-safe for concurrent registration; build it once,
    then evaluate.
    """

    def __init__(self, policies: Iterable[FirmPolicy] = ()) -> None:
        self._by_scope: dict[PolicyScope, tuple[FirmPolicy, ...]] = {}
        self.register_all(policies)

    def register(self, policy: FirmPolicy) -> None:
        self.register_all((policy,))

    # Day-1 compatible name.
    add = register

    def register_all(self, policies: Iterable[FirmPolicy]) -> None:
        staged: dict[PolicyScope, list[FirmPolicy]] = {k: list(v) for k, v in self._by_scope.items()}
        for policy in policies:
            validate_firm_policy(policy)
            scope = PolicyScope.of_policy(policy)
            existing = staged.setdefault(scope, [])
            for other in existing:
                if other.version == policy.version:
                    raise PolicyRegistrationError(
                        f"duplicate policy {scope.label} version {policy.version}")
                if periods_overlap(other, policy):
                    raise PolicyRegistrationError(
                        f"policy {scope.label} version {policy.version} "
                        f"({policy.effective_from}..{policy.effective_to or 'open'}) overlaps version "
                        f"{other.version} ({other.effective_from}..{other.effective_to or 'open'})")
            existing.append(policy)
        self._by_scope = {k: tuple(sorted(v, key=_order)) for k, v in staged.items()}

    def versions(self, scope: PolicyScope) -> tuple[FirmPolicy, ...]:
        return self._by_scope.get(scope, ())

    def scopes(self) -> tuple[PolicyScope, ...]:
        return tuple(sorted(self._by_scope, key=lambda s: (s.firm_id, s.product, s.program, s.phase.value)))

    def __len__(self) -> int:
        return sum(len(v) for v in self._by_scope.values())

    def resolve_scope(self, scope: PolicyScope, at: datetime) -> PolicyResolution:
        return resolve_from(self._by_scope.get(scope, ()), scope, at)

    def resolve(self, profile: Profile, context: PolicyContext) -> Optional[FirmPolicy]:
        """Day-1 compatible lookup. Returns the effective version or None.
        Raises PolicyAmbiguityError rather than guessing."""
        resolution = self.resolve_scope(PolicyScope.of_profile(profile), context.now)
        if resolution.status is ResolutionStatus.AMBIGUOUS:
            raise PolicyAmbiguityError(resolution.reason)
        return resolution.policy
