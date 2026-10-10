"""Fixtures for the XTL platform suite. Builders live in `_builders`."""
from __future__ import annotations

from datetime import timedelta

import pytest

from api.xtl_platform.accounts.router import AccountRouter
from api.xtl_platform.domain.account import ExecutionSet
from api.xtl_platform.risk.reference_test_only import ReferenceTestOnlyRiskAuthority
from api.xtl_platform.tests._builders import USER, D, make_device, make_profile


@pytest.fixture
def authority() -> ReferenceTestOnlyRiskAuthority:
    return ReferenceTestOnlyRiskAuthority(
        reservation_ttl=timedelta(seconds=30),
        max_snapshot_age=timedelta(seconds=10),
        acknowledge_reference_test_only=True,
    )


@pytest.fixture
def router() -> AccountRouter:
    return AccountRouter({"dev-1": make_device()}, max_heartbeat_age=timedelta(seconds=30))


@pytest.fixture
def execution_set() -> ExecutionSet:
    return ExecutionSet(USER, (
        make_profile("ftmo-100k-01"),
        make_profile("ftmo-100k-02"),
        make_profile("ftmo-10k-01", account_size=D("10000")),
    ))
