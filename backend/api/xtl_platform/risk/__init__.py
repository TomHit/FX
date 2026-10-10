"""Shared risk authority contract.

Only the interface is exported. The reference implementation in
`reference_test_only` is REFERENCE / TEST ONLY and deliberately not exported.
"""
from .authority import RiskAuthority

__all__ = ["RiskAuthority"]
