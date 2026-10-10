"""Shared test builders for the XTL platform suite.

Imported by canonical absolute name `api.xtl_platform.tests._builders`.
No sys.path manipulation: resolution comes from api/xtl_platform/pytest.ini.
"""
from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal

from api.xtl_platform.domain.account import AccountIdentity, Device, ExecutionSet, Profile
from api.xtl_platform.domain.enums import LossModel, Permission, Phase, Side, StrategyId, Venue
from api.xtl_platform.domain.policy import FirmPolicy
from api.xtl_platform.domain.risk import AccountRiskLimits, AccountRiskSnapshot
from api.xtl_platform.domain.thesis import Thesis

NOW = datetime(2026, 9, 16, 12, 0, tzinfo=timezone.utc)
USER = "user-1"


def D(x) -> Decimal:
    return Decimal(str(x))


def make_thesis(**kw) -> Thesis:
    base = dict(
        thesis_id="A-20260916-XAU-BUY",
        strategy_id=StrategyId.STRATEGY_A,
        venue=Venue.MT5_CFD,
        symbol="XAUUSD",
        side=Side.BUY,
        entry=D("2650.00"),
        stop=D("2640.00"),
        target=D("2680.00"),
        created_at=NOW - timedelta(minutes=1),
        expires_at=NOW + timedelta(minutes=30),
    )
    base.update(kw)
    return Thesis(**base)


def make_profile(profile_id: str, **kw) -> Profile:
    base = dict(
        profile_id=profile_id,
        owner_user_id=USER,
        firm_id="FTMO",
        product="CFD",
        program="2-Step",
        phase=Phase.CHALLENGE,
        account_size=D("100000"),
        venue=Venue.MT5_CFD,
        identity=AccountIdentity(login=f"login-{profile_id}", server="FTMO-Demo", company="FTMO", currency="USD"),
        device_id="dev-1",
        enabled=True,
        symbol_map={"XAUUSD": "XAUUSD"},
    )
    base.update(kw)
    return Profile(**base)


def make_device(device_id: str = "dev-1", **kw) -> Device:
    base = dict(device_id=device_id, owner_user_id=USER, healthy=True,
                venues=frozenset({Venue.MT5_CFD}), last_heartbeat=NOW - timedelta(seconds=5))
    base.update(kw)
    return Device(**base)


#: Every FirmPolicy built here is synthetic example data.
TEST_FIXTURE_LABEL = "TEST FIXTURE - NOT VERIFIED PRODUCTION FIRM RULE"


def make_policy(**kw) -> FirmPolicy:
    """TEST FIXTURE - NOT VERIFIED PRODUCTION FIRM RULE.

    Firm names and numbers are placeholders for exercising the engine. They are
    not taken from, and must not be used as, any firm's official rules.
    """
    base = dict(
        firm_id="FTMO", product="CFD", program="2-Step", phase=Phase.CHALLENGE,
        version="ftmo-cfd-2step-challenge-2026.09", effective_from=date(2026, 9, 1), effective_to=None,
        source_url="test-fixture://NOT-VERIFIED-PRODUCTION-FIRM-RULE",
        automation=Permission.ALLOWED, automation_verified=True,
        own_account_copy=Permission.ALLOWED, cross_firm_copy=Permission.PROHIBITED,
        allows_opposite_exposure_across_accounts=False, max_accounts=None,
        daily_loss_model=LossModel.EQUITY_BASED, daily_loss_limit_pct=D("5"),
        max_loss_model=LossModel.STATIC, max_loss_limit_pct=D("10"),
        daily_reset_tz="Europe/Prague", daily_reset_time=time(0, 0),
        allowed_symbols=frozenset({"XAUUSD", "EURUSD"}),
    )
    base.update(kw)
    return FirmPolicy(**base)


def make_snapshot(profile_id: str, **kw) -> AccountRiskSnapshot:
    base = dict(profile_id=profile_id, balance=D("100000"), equity=D("100000"),
                daily_loss_used_usd=D("0"), max_loss_used_usd=D("0"), halted=False,
                as_of=NOW - timedelta(seconds=2))
    base.update(kw)
    return AccountRiskSnapshot(**base)


def make_limits(profile_id: str, **kw) -> AccountRiskLimits:
    base = dict(profile_id=profile_id, daily_loss_limit_usd=D("5000"), max_loss_limit_usd=D("10000"),
                max_open_risk_usd=D("2000"), max_positions=5)
    base.update(kw)
    return AccountRiskLimits(**base)
