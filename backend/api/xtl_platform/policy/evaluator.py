"""Deterministic generic policy evaluation (Day 2, brief sections 6-10, 13).

The evaluator decides RULE ELIGIBILITY only. It does not size, reserve risk,
place orders, mutate lifecycle state, or perform any I/O.

Determinism: `evaluated_at` is the PolicyContext time, never the wall clock, so
identical input always produces an identical PolicyDecision.

Check order (first failing check wins):
  1. policy present and valid           -> BLOCK_NO_POLICY
  2. policy scope == profile scope       -> BLOCK_PROGRAM_MISMATCH
  3. policy effective on evaluation date -> BLOCK_POLICY_NOT_EFFECTIVE
  4. automation PROHIBITED               -> BLOCK_AUTOMATION_PROHIBITED
  5. automation not ALLOWED + verified   -> BLOCK_AUTOMATION_UNVERIFIED
  6. canonical symbol allowed            -> BLOCK_INSTRUMENT
  7. own-account copy                    -> BLOCK_OWN_ACCOUNT_COPY
  8. cross-firm copy                     -> BLOCK_CROSS_FIRM_COPY
  9. opposite exposure at same firm      -> BLOCK_OPPOSITE_EXPOSURE
 10. firm_specific_checks hook           -> BLOCK_FIRM_RULE
Otherwise                                -> ELIGIBLE (with exact policy version)
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional

from ..domain._validation import ContractError
from ..domain.account import Profile
from ..domain.enums import Permission, PolicyDecisionCode as P
from ..domain.policy import FirmPolicy, PolicyContext, PolicyDecision
from ..domain.thesis import ChildExecutionIntent
from .registry import PolicyRegistry, PolicyScope, ResolutionStatus, evaluation_date
from .validation import policy_validation_errors

_RESOLUTION_BLOCK_CODES = {
    ResolutionStatus.NO_POLICY: P.BLOCK_NO_POLICY,
    ResolutionStatus.NOT_EFFECTIVE: P.BLOCK_POLICY_NOT_EFFECTIVE,
    ResolutionStatus.AMBIGUOUS: P.BLOCK_POLICY_AMBIGUOUS,
}


class PolicyIdentityError(ContractError):
    """Intent and profile do not describe the same frozen account identity."""


def _check_identity(intent: ChildExecutionIntent, profile: Profile) -> None:
    if (intent.profile_id != profile.profile_id
            or intent.firm_id != profile.firm_id
            or intent.account != profile.identity):
        raise PolicyIdentityError("intent/profile identity mismatch; identity must be frozen")


class PolicyAdapter(ABC):
    @abstractmethod
    def evaluate(
        self,
        intent: ChildExecutionIntent,
        profile: Profile,
        policy: Optional[FirmPolicy],
        context: PolicyContext,
    ) -> PolicyDecision: ...


class BasePolicyEvaluator(PolicyAdapter):
    """Generic, data-driven checks shared by every firm. Fails closed.

    Firm-specific extension point: subclass and override `firm_specific_checks`.
    No firm subclasses exist at Day 2.
    """

    def evaluate(self, intent, profile, policy, context) -> PolicyDecision:
        _check_identity(intent, profile)
        tid, at = intent.child_trade_id, context.now

        def decide(code: P, reason: str, version: Optional[str]) -> PolicyDecision:
            return PolicyDecision(tid, code, reason, version, at)

        if policy is None:
            return decide(P.BLOCK_NO_POLICY, "no policy supplied for this firm/product/program/phase", None)
        errors = policy_validation_errors(policy)
        if errors:
            return decide(P.BLOCK_NO_POLICY, "policy failed validation: " + "; ".join(errors), None)

        v = policy.version
        policy_scope, profile_scope = PolicyScope.of_policy(policy), PolicyScope.of_profile(profile)
        if policy_scope != profile_scope:
            return decide(P.BLOCK_PROGRAM_MISMATCH,
                          f"policy scope {policy_scope.label} does not match profile scope {profile_scope.label}", v)

        on = evaluation_date(at)
        if not policy.is_effective(on):
            return decide(P.BLOCK_POLICY_NOT_EFFECTIVE,
                          f"policy {v} effective {policy.effective_from}..{policy.effective_to or 'open'} "
                          f"is not effective on {on}", v)

        if policy.automation is Permission.PROHIBITED:
            return decide(P.BLOCK_AUTOMATION_PROHIBITED, "program prohibits automated trading", v)
        if policy.automation is not Permission.ALLOWED or policy.automation_verified is not True:
            return decide(P.BLOCK_AUTOMATION_UNVERIFIED,
                          f"automation is {policy.automation} with verified={policy.automation_verified}; "
                          "only ALLOWED and verified may proceed", v)

        if intent.canonical_symbol not in policy.allowed_symbols:
            return decide(P.BLOCK_INSTRUMENT, f"{intent.canonical_symbol} is not an allowed instrument", v)

        siblings = context.approved_sibling_firm_ids
        if policy.own_account_copy is not Permission.ALLOWED and profile.firm_id in siblings:
            return decide(P.BLOCK_OWN_ACCOUNT_COPY,
                          f"own-account copy is {policy.own_account_copy}; same thesis already approved at "
                          f"{profile.firm_id}", v)
        other_firms = sorted({f for f in siblings if f != profile.firm_id})
        if policy.cross_firm_copy is not Permission.ALLOWED and other_firms:
            return decide(P.BLOCK_CROSS_FIRM_COPY,
                          f"cross-firm copy is {policy.cross_firm_copy}; same thesis already approved at "
                          f"{', '.join(other_firms)}", v)

        if not policy.allows_opposite_exposure_across_accounts:
            for exp in context.open_exposures:
                if (exp.firm_id == profile.firm_id
                        and exp.canonical_symbol == intent.canonical_symbol
                        and exp.side is intent.side.opposite):
                    return decide(P.BLOCK_OPPOSITE_EXPOSURE,
                                  f"opposite {exp.side} {exp.canonical_symbol} exposure on {exp.profile_id} "
                                  f"at {exp.firm_id}", v)

        return self._run_firm_specific(intent, profile, policy, context, decide)

    def _run_firm_specific(self, intent, profile, policy, context, decide) -> PolicyDecision:
        v = policy.version
        try:
            result = self.firm_specific_checks(intent, profile, policy, context)
        except Exception as exc:  # noqa: BLE001 - a broken firm rule must fail closed
            return decide(P.BLOCK_FIRM_RULE, f"firm-specific check raised {type(exc).__name__}; failing closed", v)
        if result is None:
            return decide(P.ELIGIBLE, "all policy checks passed", v)
        if (not isinstance(result, PolicyDecision)
                or result.code is not P.BLOCK_FIRM_RULE
                or result.child_trade_id != intent.child_trade_id):
            return decide(P.BLOCK_FIRM_RULE, "firm-specific check returned an invalid result; failing closed", v)
        # Normalise version and timestamp so the hook cannot misreport them.
        return decide(P.BLOCK_FIRM_RULE, result.reason, v)

    def firm_specific_checks(
        self, intent: ChildExecutionIntent, profile: Profile, policy: FirmPolicy, context: PolicyContext
    ) -> Optional[PolicyDecision]:
        """Override for rules that cannot be expressed as FirmPolicy data.

        Return None to pass, or a BLOCK_FIRM_RULE decision (see `firm_rule_block`).
        Any other return value, or an exception, blocks with BLOCK_FIRM_RULE.
        """
        return None

    @staticmethod
    def firm_rule_block(intent: ChildExecutionIntent, policy: FirmPolicy,
                        context: PolicyContext, reason: str) -> PolicyDecision:
        return PolicyDecision(intent.child_trade_id, P.BLOCK_FIRM_RULE, reason, policy.version, context.now)


class PolicyEvaluator:
    """Registry-backed evaluation: resolve the exact version, then apply rules."""

    def __init__(self, registry: PolicyRegistry, rules: Optional[BasePolicyEvaluator] = None) -> None:
        if not isinstance(registry, PolicyRegistry):
            raise TypeError("registry must be a PolicyRegistry")
        if rules is not None and not isinstance(rules, BasePolicyEvaluator):
            raise TypeError("rules must be a BasePolicyEvaluator")
        self._registry = registry
        self._rules = rules or BasePolicyEvaluator()

    def evaluate(self, intent: ChildExecutionIntent, profile: Profile, context: PolicyContext) -> PolicyDecision:
        _check_identity(intent, profile)
        resolution = self._registry.resolve_scope(PolicyScope.of_profile(profile), context.now)
        if not resolution.resolved:
            return PolicyDecision(intent.child_trade_id, _RESOLUTION_BLOCK_CODES[resolution.status],
                                  resolution.reason, None, context.now)
        return self._rules.evaluate(intent, profile, resolution.policy, context)
