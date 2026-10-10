"""Isolation gates for the XTL platform package (PRD sections 3, 21; CSA Day-1).

Static (AST) gates over every PRODUCTION module in api/xtl_platform
(everything except tests/):

  * no Strategy A/B or existing XTL lifecycle modules
    (oppt_executor, zone_entry_gate, position_manager, trend_endpoints, strategy_b, ...)
  * no imports from the existing `api.strategy` package; only prefixes listed in
    APPROVED_SHARED_INFRASTRUCTURE may be imported, and that list is empty today
  * no third-party dependencies at all (so no Redis, MT5, Tradovate,
    NinjaTrader or HTTP broker clients)
  * no stdlib I/O, network, persistence or dynamic-import modules, and no
    open()/__import__()/exec()/eval()/compile() calls
  * no relative import that escapes api.xtl_platform

Plus behavioural isolation between Strategy A and Strategy B children.
"""
from __future__ import annotations

import ast
import sys
from pathlib import Path

import pytest

from api.xtl_platform.domain.enums import Side, StrategyId
from api.xtl_platform.domain.risk import RiskRequest
from api.xtl_platform.domain.thesis import ChildExecutionIntent
from api.xtl_platform.tests._builders import NOW, D, make_limits, make_profile, make_snapshot, make_thesis

PKG_ROOT = Path(__file__).resolve().parents[1]
PACKAGE = ("api", "xtl_platform")

# Dotted prefixes of shared infrastructure explicitly approved for import by
# the platform. Adding an entry is a reviewed architecture change. None today.
APPROVED_SHARED_INFRASTRUCTURE: tuple[str, ...] = ()

# Any dotted component equal to one of these is forbidden (case-insensitive).
FORBIDDEN_COMPONENTS = frozenset({
    # existing XTL strategy / lifecycle production modules
    "oppt_executor", "zone_entry_gate", "position_manager", "trend_endpoints",
    "strategy_a", "strategy_b",
    # broker / venue clients
    "metatrader5", "mt5", "tradovate", "ninjatrader", "order_send",
    # persistence / network clients
    "redis", "aioredis", "requests", "httpx", "aiohttp", "urllib3",
    # Redis command / scripting surface (Day 4)
    "evalsha", "setnx", "hset", "rpush", "lpush", "setex", "lua", "redis_lock",
    "websocket", "websockets", "grpc", "sqlalchemy", "psycopg2", "pymongo",
})

# Stdlib modules that imply I/O, network, persistence or dynamic imports.
FORBIDDEN_STDLIB = frozenset({
    "os", "io", "pathlib", "shutil", "tempfile", "glob", "fileinput", "builtins",
    "sqlite3", "shelve", "dbm", "pickle", "marshal",
    "socket", "ssl", "http", "urllib", "ftplib", "smtplib", "asyncio", "selectors",
    "subprocess", "multiprocessing", "importlib", "ctypes",
})

FORBIDDEN_CALLS = frozenset({"open", "__import__", "exec", "eval", "compile"})

# Day 8 (CSA-authorized): the ONLY production module allowed a persistence import,
# and only the named stdlib modules. Every other file keeps the Day-1 rules.
APPROVED_PERSISTENCE_BOUNDARY = {
    "risk/lifecycle_repository_sqlite.py": frozenset({"sqlite3"}),      # Day 8
    "risk/durable_transaction_sqlite.py": frozenset({"sqlite3"}),       # Day 9 (durable transaction only)
}

PRODUCTION_MANIFEST = frozenset({
    "__init__.py",
    "domain/__init__.py", "domain/_validation.py", "domain/enums.py", "domain/thesis.py",
    "domain/account.py", "domain/policy.py", "domain/risk.py", "domain/execution.py",
    "policy/__init__.py", "policy/base.py",
    "policy/registry.py", "policy/evaluator.py", "policy/validation.py",  # Day 2
    "risk/__init__.py", "risk/authority.py", "risk/reference_test_only.py",
    "risk/contracts.py", "risk/validation.py", "risk/calculator.py", "risk/evaluator.py",  # Day 3
    "risk/atomic_contracts.py", "risk/atomic_reference.py",  # Day 4 (REFERENCE ONLY)
    "risk/lifecycle_contracts.py", "risk/lifecycle_reference.py",  # Day 5 (REFERENCE ONLY)
    "risk/persistence_contracts.py", "risk/persistence_repository.py", "risk/persistence_codec.py",  # Day 6
    "risk/persistent_reference.py", "risk/reconciliation_contracts.py", "risk/reconciliation_reference.py",  # Day 6
    "risk/resolution_contracts.py", "risk/resolution_reference.py",  # Day 7 (REFERENCE ONLY)
    "risk/lifecycle_repository_sqlite.py",  # Day 8 (REFERENCE ONLY durable repository)
    "risk/durable_authority_contracts.py", "risk/durable_transaction_sqlite.py",  # Day 9
    "risk/durable_authority.py",  # Day 9 (REFERENCE ONLY durable account risk authority)
    "accounts/__init__.py", "accounts/router.py",
    "accounts/intent_contracts.py", "accounts/intent_routing.py",  # Day 10 (canonical intent + routing)
    "execution/__init__.py", "execution/adapter.py",
    "execution/order_intent.py",  # Day 11 (canonical order intent; contract only)
})


# ---- scanner ----------------------------------------------------------------

def _module_violation(dotted: str) -> str | None:
    parts = dotted.split(".")
    lowered = [p.lower() for p in parts]
    hit = next((c for c in lowered if c in FORBIDDEN_COMPONENTS), None)
    if hit:
        return f"forbidden module component {hit!r} in {dotted}"

    top = parts[0]
    if top == "api":
        if tuple(parts[:2]) == PACKAGE:
            return None
        if any(dotted == p or dotted.startswith(p + ".") for p in APPROVED_SHARED_INFRASTRUCTURE):
            return None
        if parts[1:2] == ["strategy"]:
            return f"import from api.strategy is not approved: {dotted}"
        return f"import from api outside xtl_platform is not approved: {dotted}"
    if top == "strategy":
        return f"import from strategy package is not approved: {dotted}"
    if top == "__future__":
        return None
    if top in sys.stdlib_module_names:
        return f"forbidden stdlib I/O/persistence module: {dotted}" if top in FORBIDDEN_STDLIB else None
    return f"third-party dependency not allowed at P1: {dotted}"


def scan_source(source: str, package_parts: tuple[str, ...]) -> list[str]:
    """Return violations for one module whose containing package is `package_parts`."""
    violations: list[str] = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            for alias in node.names:
                v = _module_violation(alias.name)
                if v:
                    violations.append(v)
        elif isinstance(node, ast.ImportFrom):
            if node.level:
                up = node.level - 1
                if up >= len(package_parts):
                    violations.append(f"relative import escapes filesystem root (level {node.level})")
                    continue
                base = list(package_parts[: len(package_parts) - up])
                resolved = ".".join(base + (node.module.split(".") if node.module else []))
                if tuple(resolved.split(".")[:2]) != PACKAGE:
                    violations.append(f"relative import escapes api.xtl_platform: {resolved}")
                    continue
            else:
                resolved = node.module or ""
            for candidate in [resolved] + [f"{resolved}.{a.name}" for a in node.names if a.name != "*"]:
                v = _module_violation(candidate)
                if v:
                    violations.append(v)
                    break
        elif isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in FORBIDDEN_CALLS:
            violations.append(f"forbidden call {node.func.id}()")
        elif isinstance(node, ast.Attribute) and node.attr.lower() in FORBIDDEN_COMPONENTS:
            violations.append(f"forbidden attribute .{node.attr}")
    return violations


def _production_files() -> list[Path]:
    return sorted(p for p in PKG_ROOT.rglob("*.py")
                  if "tests" not in p.relative_to(PKG_ROOT).parts and "__pycache__" not in p.parts)


# ---- gates over real production code ------------------------------------

def test_production_scan_is_not_vacuous():
    assert len(_production_files()) == len(PRODUCTION_MANIFEST)


def test_production_modules_match_manifest():
    """No Redis authority, coordinator, firm adapter or futures adapter slipped in."""
    actual = {p.relative_to(PKG_ROOT).as_posix() for p in _production_files()}
    assert actual == PRODUCTION_MANIFEST


def test_production_code_has_no_forbidden_imports_or_io():
    offenders = []
    for py in _production_files():
        rel = py.relative_to(PKG_ROOT).as_posix()
        pkg = PACKAGE + py.relative_to(PKG_ROOT).parent.parts
        allowed = {f"forbidden stdlib I/O/persistence module: {m}" for m in APPROVED_PERSISTENCE_BOUNDARY.get(rel, ())}
        offenders += [f"{rel}: {v}" for v in scan_source(py.read_text(), pkg) if v not in allowed]
    assert offenders == []


def test_persistence_boundary_is_only_the_approved_sqlite_files():
    """sqlite3 is approved in exactly the listed persistence-boundary files (Day 8 + Day 9);
    everywhere else it is still forbidden."""
    assert APPROVED_PERSISTENCE_BOUNDARY == {"risk/lifecycle_repository_sqlite.py": frozenset({"sqlite3"}),
                                             "risk/durable_transaction_sqlite.py": frozenset({"sqlite3"})}
    users = []
    for py in _production_files():
        rel = py.relative_to(PKG_ROOT).as_posix()
        if any("sqlite3" in v for v in scan_source(py.read_text(), PACKAGE + py.relative_to(PKG_ROOT).parent.parts)):
            users.append(rel)
    assert users == ["risk/durable_transaction_sqlite.py", "risk/lifecycle_repository_sqlite.py"]
    assert scan_source("import sqlite3", ("api", "xtl_platform", "policy"))


def test_no_shared_infrastructure_is_approved_today():
    assert APPROVED_SHARED_INFRASTRUCTURE == ()


# ---- the scanner itself must catch violations -----------------------------

RISK_PKG = ("api", "xtl_platform", "risk")


@pytest.mark.parametrize("snippet", [
    "import MetaTrader5 as mt5",
    "terminal.order_send(request)",
    "client.evalsha(sha, 1, key)",
    "client.setnx(key, value)",
    "import lua",
    "from api.strategy.oppt_executor import place_order",
    "from api.strategy import zone_entry_gate",
    "import api.strategy.position_manager",
    "from api.strategy.trend_endpoints import router",
    "from api.strategy import shared_helpers",
    "import api.strategy",
    "from api.trend_endpoints import router",
    "from api.services.position_manager import manage",
    "from strategy_b.state import StrategyBState",
    "import strategy.lifecycle",
    "from ...strategy import oppt_executor",
    "from .... import anything",
    "import redis",
    "from redis.asyncio import Redis",
    "import requests",
    "import httpx",
    "from tradovate import Client",
    "import ninjatrader",
    "import sqlite3",
    "from pathlib import Path",
    "import importlib",
    "open('ledger.json', 'w')",
    "__import__('redis')",
])
def test_scanner_rejects(snippet):
    assert scan_source(snippet, RISK_PKG), f"scanner missed: {snippet}"


@pytest.mark.parametrize("snippet", [
    "from ..domain.enums import Side",
    "from .authority import RiskAuthority",
    "from api.xtl_platform.domain.thesis import Thesis",
    "import threading",
    "from decimal import Decimal",
    "from __future__ import annotations",
])
def test_scanner_accepts(snippet):
    assert scan_source(snippet, RISK_PKG) == []


# ---- reference authority classification ----------------------------------

def test_reference_authority_is_classified_test_only():
    from api.xtl_platform.risk.reference_test_only import ReferenceTestOnlyRiskAuthority as Ref
    assert Ref.GRANTS_PRODUCTION_AUTHORITY is False
    assert Ref.AUTHORITY_CLASSIFICATION == "REFERENCE_TEST_ONLY"
    assert "TEST ONLY" in (Ref.__doc__ or "")


def test_reference_authority_requires_explicit_acknowledgement():
    from datetime import timedelta
    from api.xtl_platform.risk.reference_test_only import ReferenceTestOnlyRiskAuthority as Ref
    with pytest.raises(RuntimeError, match="TEST ONLY"):
        Ref(timedelta(seconds=30), timedelta(seconds=10))


def test_reference_authority_not_exported_from_risk_package():
    import api.xtl_platform.risk as risk_pkg
    assert risk_pkg.__all__ == ["RiskAuthority"]
    assert not hasattr(risk_pkg, "ReferenceTestOnlyRiskAuthority")


def test_production_modules_do_not_import_reference_authority():
    offenders = []
    for py in _production_files():
        if py.name == "reference_test_only.py":
            continue
        for node in ast.walk(ast.parse(py.read_text())):
            names = []
            if isinstance(node, ast.ImportFrom):
                names = [node.module or ""] + [a.name for a in node.names]
            elif isinstance(node, ast.Import):
                names = [a.name for a in node.names]
            if any("reference_test_only" in n or n == "ReferenceTestOnlyRiskAuthority" for n in names):
                offenders.append(py.relative_to(PKG_ROOT).as_posix())
    assert offenders == []


# ---- behavioural A/B isolation -------------------------------------------

def _child(strategy, thesis_id, profile_id="ftmo-100k-01", side=Side.BUY, **kw):
    t = make_thesis(thesis_id=thesis_id, strategy_id=strategy, side=side, **kw)
    return ChildExecutionIntent.from_thesis(t, make_profile(profile_id), "XAUUSD")


def test_children_carry_their_own_strategy_identity():
    a = _child(StrategyId.STRATEGY_A, "A-1")
    b = _child(StrategyId.STRATEGY_B, "B-1")
    assert a.strategy_id is StrategyId.STRATEGY_A
    assert b.strategy_id is StrategyId.STRATEGY_B
    assert a.child_trade_id != b.child_trade_id


def test_releasing_b_does_not_touch_a_on_same_account(authority):
    pid = "ftmo-100k-01"
    snap, lim = make_snapshot(pid), make_limits(pid)
    ra = authority.authorize_and_reserve(RiskRequest(_child(StrategyId.STRATEGY_A, "A-1"), D("500"), D("1"), "v1"), snap, lim, NOW)
    rb = authority.authorize_and_reserve(RiskRequest(_child(StrategyId.STRATEGY_B, "B-1"), D("500"), D("1"), "v1"), snap, lim, NOW)
    assert ra.authorized and rb.authorized
    assert authority.release(rb.reservation.authorization_id, "B closed", NOW)
    assert [r.strategy_id for r in authority.active_reservations(pid)] == [StrategyId.STRATEGY_A]


def test_a_and_b_share_one_account_risk_truth(authority):
    """Isolated lifecycles, but one capacity ledger per account."""
    pid = "ftmo-100k-01"
    snap, lim = make_snapshot(pid), make_limits(pid, max_open_risk_usd=D("1000"))
    ra = authority.authorize_and_reserve(RiskRequest(_child(StrategyId.STRATEGY_A, "A-1"), D("700"), D("1"), "v1"), snap, lim, NOW)
    rb = authority.authorize_and_reserve(RiskRequest(_child(StrategyId.STRATEGY_B, "B-1"), D("700"), D("1"), "v1"), snap, lim, NOW)
    assert ra.authorized
    assert rb.code.value == "BLOCK_OPEN_RISK"


def test_duplicate_trade_id_returns_existing_reservation_not_a_second_one(authority):
    pid = "ftmo-100k-01"
    req = RiskRequest(_child(StrategyId.STRATEGY_A, "A-1"), D("500"), D("1"), "v1")
    first = authority.authorize_and_reserve(req, make_snapshot(pid), make_limits(pid), NOW)
    second = authority.authorize_and_reserve(req, make_snapshot(pid), make_limits(pid), NOW)
    assert second.code.value == "DUPLICATE_TRADE_ID"
    assert second.reservation.authorization_id == first.reservation.authorization_id
    assert len(authority.active_reservations(pid)) == 1


def test_release_is_exactly_once(authority):
    pid = "ftmo-100k-01"
    res = authority.authorize_and_reserve(
        RiskRequest(_child(StrategyId.STRATEGY_A, "A-1"), D("500"), D("1"), "v1"),
        make_snapshot(pid), make_limits(pid), NOW).reservation
    assert authority.release(res.authorization_id, "closed", NOW) is True
    assert authority.release(res.authorization_id, "closed again", NOW) is False
