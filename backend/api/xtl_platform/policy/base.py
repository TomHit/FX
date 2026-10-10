"""Prop Policy Authority (PRD section 6) - Day-1 compatible entry point.

Day 2 split the implementation into:
  validation.py  fail-closed FirmPolicy validation
  registry.py    versioned, effective-date resolution
  evaluator.py   deterministic generic rule evaluation

Existing imports from `policy.base` keep working.
"""
from .evaluator import BasePolicyEvaluator, PolicyAdapter, PolicyEvaluator, PolicyIdentityError
from .registry import PolicyRegistry

__all__ = ["BasePolicyEvaluator", "PolicyAdapter", "PolicyEvaluator", "PolicyIdentityError", "PolicyRegistry"]
