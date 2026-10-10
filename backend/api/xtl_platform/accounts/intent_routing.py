"""Day-10 deterministic account routing - REFERENCE ONLY - NON-PRODUCTION.

EXECUTION AUTHORITY = FALSE. BROKER AUTHORITY = FALSE.
Pure function of (intent, explicitly supplied candidates). No discovery of any kind:
no Redis, database, network, broker or persistence. No clock, no RNG, no threads.

Routing answers ONE question: given this intent and these candidates, can the platform
deterministically identify THE intended account?

    candidates -> domain (Venue) -> environment -> INFRA_TEST acceptance ->
    strategy permission -> explicit target -> enabled -> exactly one?

Outcomes follow the frozen decision conventions (an explicit code plus a reason). Each
decision accepts only its own reason codes (DECISION_REASONS), enforced by the result
contract, so a decision can never be reported with a reason from another outcome:

    ROUTED     exactly one eligible account
    WAIT       a compatible account exists but is currently disabled (transient)
    AMBIGUOUS  more than one equally valid destination and no explicit discriminator
    REJECT     structurally incompatible, invalid input, or an explicit target that is not eligible

This is NOT fan-out: one intent yields at most ONE account. No copy trading, no
replication, no allocation, no weighting, no portfolio logic. Day 10 never invokes
the Day-9 risk authority; a routed account is only a destination, not an authorization.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar, Optional

from ..domain._validation import ContractError
from ..domain.account import AccountIdentity
from ..domain.enums import StrEnum
from .intent_contracts import (
    INFRA_TEST_ENVIRONMENTS,
    CanonicalStrategyIntent,
    RoutableAccount,
    intent_errors,
)


class RoutingDecision(StrEnum):
    ROUTED = "ROUTED"
    WAIT = "WAIT"
    AMBIGUOUS = "AMBIGUOUS"
    REJECT = "REJECT"


class RoutingReason(StrEnum):
    ROUTED_SINGLE_ELIGIBLE = "ROUTED_SINGLE_ELIGIBLE"
    ROUTED_EXPLICIT_TARGET = "ROUTED_EXPLICIT_TARGET"
    INVALID_INTENT = "INVALID_INTENT"
    INVALID_CANDIDATES = "INVALID_CANDIDATES"
    NO_CANDIDATES = "NO_CANDIDATES"
    NO_DOMAIN_MATCH = "NO_DOMAIN_MATCH"
    NO_ENVIRONMENT_MATCH = "NO_ENVIRONMENT_MATCH"
    INFRA_TEST_NOT_ACCEPTED = "INFRA_TEST_NOT_ACCEPTED"
    STRATEGY_NOT_PERMITTED = "STRATEGY_NOT_PERMITTED"
    EXPLICIT_TARGET_NOT_ELIGIBLE = "EXPLICIT_TARGET_NOT_ELIGIBLE"
    ALL_ELIGIBLE_DISABLED = "ALL_ELIGIBLE_DISABLED"
    AMBIGUOUS_CANDIDATES = "AMBIGUOUS_CANDIDATES"


class CandidateVerdict(StrEnum):
    ELIGIBLE = "ELIGIBLE"
    DISABLED = "DISABLED"
    DOMAIN_MISMATCH = "DOMAIN_MISMATCH"
    ENVIRONMENT_MISMATCH = "ENVIRONMENT_MISMATCH"
    INFRA_TEST_NOT_ACCEPTED = "INFRA_TEST_NOT_ACCEPTED"
    STRATEGY_NOT_PERMITTED = "STRATEGY_NOT_PERMITTED"
    NOT_EXPLICIT_TARGET = "NOT_EXPLICIT_TARGET"


@dataclass(frozen=True)
class CandidateAssessment:
    profile_id: str
    verdict: CandidateVerdict
    reason: str


#: The only reason codes each decision may carry. Any other combination is a contract error.
DECISION_REASONS: dict[RoutingDecision, frozenset] = {
    RoutingDecision.ROUTED: frozenset({RoutingReason.ROUTED_SINGLE_ELIGIBLE,
                                       RoutingReason.ROUTED_EXPLICIT_TARGET}),
    RoutingDecision.WAIT: frozenset({RoutingReason.ALL_ELIGIBLE_DISABLED}),
    RoutingDecision.AMBIGUOUS: frozenset({RoutingReason.AMBIGUOUS_CANDIDATES}),
    RoutingDecision.REJECT: frozenset({RoutingReason.INVALID_INTENT, RoutingReason.INVALID_CANDIDATES,
                                       RoutingReason.NO_CANDIDATES, RoutingReason.NO_DOMAIN_MATCH,
                                       RoutingReason.NO_ENVIRONMENT_MATCH, RoutingReason.INFRA_TEST_NOT_ACCEPTED,
                                       RoutingReason.STRATEGY_NOT_PERMITTED,
                                       RoutingReason.EXPLICIT_TARGET_NOT_ELIGIBLE}),
}


@dataclass(frozen=True)
class RoutingResult:
    """Deterministic routing outcome. Grants no authority of any kind."""

    EXECUTION_AUTHORITY: ClassVar[bool] = False
    BROKER_AUTHORITY: ClassVar[bool] = False
    GRANTS_RISK_AUTHORIZATION: ClassVar[bool] = False

    intent_id: str
    decision: RoutingDecision
    reason_code: RoutingReason
    reason: str
    selected_profile_id: Optional[str]
    selected_account: Optional[AccountIdentity]
    assessments: tuple[CandidateAssessment, ...]
    errors: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.decision, RoutingDecision) or not isinstance(self.reason_code, RoutingReason):
            raise ContractError("decision and reason_code must be Day-10 enums")
        routed = self.decision is RoutingDecision.ROUTED
        if routed != (self.selected_profile_id is not None) or routed != (self.selected_account is not None):
            raise ContractError("ROUTED if and only if exactly one account is selected")
        permitted = DECISION_REASONS[self.decision]
        if self.reason_code not in permitted:
            raise ContractError(
                f"{self.decision} may only carry {sorted(r.value for r in permitted)}, got {self.reason_code}")

    @property
    def routed(self) -> bool:
        return self.decision is RoutingDecision.ROUTED


def _assess(intent: CanonicalStrategyIntent, account: RoutableAccount) -> CandidateAssessment:
    V = CandidateVerdict
    if account.venue is not intent.venue:
        return CandidateAssessment(account.profile_id, V.DOMAIN_MISMATCH,
                                   f"account domain {account.venue} != intent {intent.venue}")
    if account.environment is not intent.environment:
        return CandidateAssessment(account.profile_id, V.ENVIRONMENT_MISMATCH,
                                   f"account environment {account.environment} != intent {intent.environment}")
    if intent.is_infrastructure_test:
        if not account.accepts_infrastructure_tests or account.environment not in INFRA_TEST_ENVIRONMENTS:
            return CandidateAssessment(account.profile_id, V.INFRA_TEST_NOT_ACCEPTED,
                                       "account does not accept infrastructure-test intents")
    elif intent.strategy_ref not in account.permitted_strategy_refs:
        return CandidateAssessment(account.profile_id, V.STRATEGY_NOT_PERMITTED,
                                   f"{intent.strategy_ref} is not permitted on this account")
    if intent.target_profile_id is not None and account.profile_id != intent.target_profile_id:
        return CandidateAssessment(account.profile_id, V.NOT_EXPLICIT_TARGET,
                                   f"intent explicitly targets {intent.target_profile_id}")
    if not account.enabled:
        return CandidateAssessment(account.profile_id, V.DISABLED, "account is not enabled")
    return CandidateAssessment(account.profile_id, V.ELIGIBLE, "eligible destination")


def route_intent(intent: CanonicalStrategyIntent,
                 candidates: tuple[RoutableAccount, ...]) -> RoutingResult:
    """Pure, deterministic routing. At most ONE account is ever selected."""
    if not isinstance(intent, CanonicalStrategyIntent):
        raise TypeError("intent must be a CanonicalStrategyIntent")

    def result(decision: RoutingDecision, reason_code: RoutingReason, reason: str,
               assessments=(), selected: Optional[RoutableAccount] = None, errors=()) -> RoutingResult:
        return RoutingResult(intent_id=intent.intent_id if isinstance(intent.intent_id, str) else "",
                             decision=decision, reason_code=reason_code, reason=reason,
                             selected_profile_id=selected.profile_id if selected else None,
                             selected_account=selected.account if selected else None,
                             assessments=tuple(assessments), errors=tuple(errors))

    errors = intent_errors(intent)
    if errors:
        return result(RoutingDecision.REJECT, RoutingReason.INVALID_INTENT,
                      "intent is not valid: " + "; ".join(errors), errors=errors)
    if not isinstance(candidates, tuple) or any(not isinstance(a, RoutableAccount) for a in candidates):
        return result(RoutingDecision.REJECT, RoutingReason.INVALID_CANDIDATES,
                      "candidates must be a tuple of RoutableAccount")
    profile_ids = [a.profile_id for a in candidates]
    identities = [a.account for a in candidates]
    if len(set(profile_ids)) != len(profile_ids) or len(set(identities)) != len(identities):
        return result(RoutingDecision.REJECT, RoutingReason.INVALID_CANDIDATES,
                      "candidate profile ids and account identities must be unique")
    if not candidates:
        return result(RoutingDecision.REJECT, RoutingReason.NO_CANDIDATES, "no candidate accounts were supplied")

    assessments = tuple(sorted((_assess(intent, a) for a in candidates), key=lambda x: x.profile_id))
    by_profile = {a.profile_id: a for a in candidates}
    eligible = [by_profile[a.profile_id] for a in assessments if a.verdict is CandidateVerdict.ELIGIBLE]

    if len(eligible) == 1:
        account = eligible[0]
        explicit = intent.target_profile_id is not None
        return result(RoutingDecision.ROUTED,
                      RoutingReason.ROUTED_EXPLICIT_TARGET if explicit else RoutingReason.ROUTED_SINGLE_ELIGIBLE,
                      f"routed to {account.profile_id}", assessments, selected=account)
    if len(eligible) > 1:
        return result(RoutingDecision.AMBIGUOUS, RoutingReason.AMBIGUOUS_CANDIDATES,
                      f"{len(eligible)} equally valid destinations ({', '.join(a.profile_id for a in eligible)}); "
                      "no explicit discriminator", assessments)

    verdicts = {a.verdict for a in assessments}
    V, R = CandidateVerdict, RoutingReason
    if intent.target_profile_id is not None:
        return result(RoutingDecision.REJECT, R.EXPLICIT_TARGET_NOT_ELIGIBLE,
                      f"explicit target {intent.target_profile_id} is not an eligible destination", assessments)
    if V.DISABLED in verdicts:
        return result(RoutingDecision.WAIT, R.ALL_ELIGIBLE_DISABLED,
                      "every otherwise-compatible account is disabled", assessments)
    for verdict, reason_code, message in (
            (V.STRATEGY_NOT_PERMITTED, R.STRATEGY_NOT_PERMITTED, "no candidate permits this strategy identity"),
            (V.INFRA_TEST_NOT_ACCEPTED, R.INFRA_TEST_NOT_ACCEPTED, "no candidate accepts infrastructure-test intents"),
            (V.ENVIRONMENT_MISMATCH, R.NO_ENVIRONMENT_MATCH, "no candidate matches the intent environment"),
            (V.DOMAIN_MISMATCH, R.NO_DOMAIN_MATCH, "no candidate matches the intent market domain")):
        if verdict in verdicts:
            return result(RoutingDecision.REJECT, reason_code, message, assessments)
    return result(RoutingDecision.REJECT, R.NO_CANDIDATES, "no eligible destination", assessments)
