from __future__ import annotations

"""
Strategy-B atomic pending-risk Redis adapter.

Boundary:
- accepts one already-formed Strategy-B risk request + capacity snapshot;
- delegates pure validation/reservation construction to the frozen
  strategy_b_atomic_risk_authority contract;
- performs exactly one Redis EVAL for an eligible commit attempt;
- stores only Strategy-B pending reservations;
- does not place orders, contact MT5, mutate Strategy A, or grant execution
  authority.

The capacity snapshot carries authoritative external/account exposure.
The Redis ledger protects concurrent Strategy-B pending reservations that
are not yet represented by that external snapshot.

Real-Redis concurrency proof is a later step.
"""

from dataclasses import dataclass
import json
import math
from typing import Any, Optional

from api.strategy_b_atomic_risk_authority import (
    StrategyBRiskAuthorizationRequest,
    StrategyBRiskCapacity,
    authorize_and_reserve_strategy_b_risk,
)


_ALLOWED_BLOCK_REASONS = frozenset(
    {
        "PROFILE_DISABLED",
        "PROFILE_ID_INVALID",
        "DEVICE_ID_INVALID",
        "ACCOUNT_IDENTITY_INVALID",
        "CAPACITY_IDENTITY_MISMATCH",
        "REQUEST_IDENTITY_INVALID",
        "RISK_CAPACITY_TIME_INVALID",
        "RISK_CAPACITY_STALE",
        "RISK_USD_INVALID",
        "LOTS_INVALID",
        "RISK_PCT_INVALID",
        "PRICE_INPUT_INVALID",
        "RISK_CAPACITY_INVALID",
        "DAILY_LOSS_LIMIT_EXCEEDED",
        "MAX_LOSS_LIMIT_EXCEEDED",
        "MAX_OPEN_POSITIONS_REACHED",
        "MAX_OPEN_RISK_EXCEEDED",
        "RESERVATION_IDENTITY_CONFLICT",
        "PENDING_LEDGER_INCONSISTENT",
    }
)

_SUCCESS_REASONS = frozenset(
    {
        "COMMITTED",
        "IDENTICAL_RESERVATION",
    }
)


_ATOMIC_COMMIT_LUA = r"""
local ledger_key = KEYS[1]
local total_key = KEYS[2]

local reservation_id = ARGV[1]
local payload = ARGV[2]
local request_risk = tonumber(ARGV[3])
local external_open_risk = tonumber(ARGV[4])
local max_open_risk = tonumber(ARGV[5])
if not reservation_id or reservation_id == "" then
    return {"RESERVATION_IDENTITY_CONFLICT", "", "0", "0"}
end

if not request_risk or request_risk <= 0 then
    return {"RISK_USD_INVALID", "", "0", "0"}
end

if not external_open_risk or external_open_risk < 0 then
    return {"RISK_CAPACITY_INVALID", "", "0", "0"}
end

if not max_open_risk or max_open_risk < 0 then
    return {"RISK_CAPACITY_INVALID", "", "0", "0"}
end

local existing = redis.call("HGET", ledger_key, reservation_id)

local ledger_count = redis.call("HLEN", ledger_key)
local total_raw = redis.call("GET", total_key)

if ledger_count == 0 then
    if total_raw and tonumber(total_raw) ~= 0 then
        return {"PENDING_LEDGER_INCONSISTENT", "", "0", "0"}
    end
else
    if not total_raw then
        return {"PENDING_LEDGER_INCONSISTENT", "", "0", "0"}
    end
end

local pending_total = tonumber(total_raw or "0")
if not pending_total or pending_total < 0 then
    return {"PENDING_LEDGER_INCONSISTENT", "", "0", "0"}
end

if existing then
    local identical = (existing == payload)

    if not identical then
        local ok_existing, existing_obj = pcall(cjson.decode, existing)
        local ok_proposed, proposed_obj = pcall(cjson.decode, payload)

        if ok_existing and ok_proposed
           and type(existing_obj) == "table"
           and type(proposed_obj) == "table" then

            -- reserved_at_ms is immutable creation metadata.
            -- A later retry may naturally carry a newer capacity.now_ms,
            -- so compare the reservation contract without that one field.
            existing_obj["reserved_at_ms"] = nil
            proposed_obj["reserved_at_ms"] = nil

            identical = (
                cjson.encode(existing_obj)
                == cjson.encode(proposed_obj)
            )
        end
    end

    if identical then
        local projected = external_open_risk + pending_total
        redis.call("PERSIST", ledger_key)
        redis.call("PERSIST", total_key)
        return {
            "IDENTICAL_RESERVATION",
            reservation_id,
            tostring(request_risk),
            tostring(projected)
        }
    end

    return {
        "RESERVATION_IDENTITY_CONFLICT",
        "",
        "0",
        tostring(external_open_risk + pending_total)
    }
end

local projected = external_open_risk + pending_total + request_risk

if projected > max_open_risk then
    return {
        "MAX_OPEN_RISK_EXCEEDED",
        "",
        "0",
        tostring(projected)
    }
end

redis.call("HSET", ledger_key, reservation_id, payload)
redis.call("SET", total_key, tostring(pending_total + request_risk))
redis.call("PERSIST", ledger_key)
redis.call("PERSIST", total_key)

return {
    "COMMITTED",
    reservation_id,
    tostring(request_risk),
    tostring(projected)
}
"""


@dataclass(frozen=True)
class StrategyBAtomicRiskCommitResult:
    committed: bool
    reason: str
    reservation_id: Optional[str]
    reserved_risk_usd: float
    projected_open_risk_usd: float

    observation_only: bool = True
    execution_authority: bool = False
    order_authority: bool = False
    broker_authority: bool = False
    strategy_a_mutation: bool = False


def _blocked(
    reason: str,
    *,
    projected_open_risk_usd: float = 0.0,
) -> StrategyBAtomicRiskCommitResult:
    return StrategyBAtomicRiskCommitResult(
        committed=False,
        reason=reason,
        reservation_id=None,
        reserved_risk_usd=0.0,
        projected_open_risk_usd=projected_open_risk_usd,
    )


def _decode(value: Any) -> str:
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", "strict")
    return str(value)


def _finite_nonnegative(value: Any) -> Optional[float]:
    if isinstance(value, bool):
        return None

    try:
        number = float(value)
    except (TypeError, ValueError):
        return None

    if not math.isfinite(number) or number < 0.0:
        return None

    return number


def _ledger_keys(
    request: StrategyBRiskAuthorizationRequest,
) -> tuple[str, str]:
    # No SCAN-based ownership. Exact profile/account-scoped keys only.
    uid = str(request.uid).strip()
    profile_id = str(request.profile_id).strip()
    device_id = str(request.device_id).strip()
    account_login = str(request.account_login).strip()

    parts = (uid, profile_id, device_id, account_login)
    if any((not part) or (":" in part) for part in parts):
        raise ValueError("invalid Strategy-B risk ledger identity")

    base = (
        f"xtl:strategy_b:risk:pending:"
        f"{uid}:{profile_id}:{device_id}:{account_login}"
    )

    return f"{base}:reservations", f"{base}:total"


def _payload(reservation: Any) -> str:
    body = {
        "schema_version": 1,
        "strategy": reservation.strategy,
        "reservation_id": reservation.reservation_id,
        "uid": reservation.uid,
        "profile_id": reservation.profile_id,
        "thesis_id": reservation.thesis_id,
        "selected_capture_id": reservation.selected_capture_id,
        "symbol": reservation.symbol,
        "side": reservation.side,
        "risk_usd": reservation.risk_usd,
        "risk_pct": reservation.risk_pct,
        "entry": reservation.entry,
        "sl": reservation.sl,
        "tp": reservation.tp,
        "lots": reservation.lots,
        "device_id": reservation.device_id,
        "account_login": reservation.account_login,
        "account_server": reservation.account_server,
        "broker_company": reservation.broker_company,
        "status": reservation.status,
        "reserved_at_ms": reservation.reserved_at_ms,
    }

    return json.dumps(
        body,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def _parse_server_response(
    response: Any,
) -> StrategyBAtomicRiskCommitResult:
    if not isinstance(response, (list, tuple)):
        return _blocked("ATOMIC_RISK_RESPONSE_INVALID")

    if len(response) != 4:
        return _blocked("ATOMIC_RISK_RESPONSE_INVALID")

    try:
        reason = _decode(response[0]).strip()
        reservation_id = _decode(response[1]).strip()

        reserved = _finite_nonnegative(_decode(response[2]))
        projected = _finite_nonnegative(_decode(response[3]))
    except Exception:
        return _blocked("ATOMIC_RISK_RESPONSE_INVALID")

    if reserved is None or projected is None:
        return _blocked("ATOMIC_RISK_RESPONSE_INVALID")

    if reason in _SUCCESS_REASONS:
        if not reservation_id or reserved <= 0.0:
            return _blocked("ATOMIC_RISK_RESPONSE_INVALID")

        return StrategyBAtomicRiskCommitResult(
            committed=True,
            reason=reason,
            reservation_id=reservation_id,
            reserved_risk_usd=reserved,
            projected_open_risk_usd=projected,
        )

    if reason in _ALLOWED_BLOCK_REASONS:
        if reservation_id:
            return _blocked("ATOMIC_RISK_RESPONSE_INVALID")

        return _blocked(
            reason,
            projected_open_risk_usd=projected,
        )

    return _blocked("ATOMIC_RISK_RESPONSE_INVALID")


def commit_strategy_b_risk_reservation(
    redis_client: Any,
    *,
    request: StrategyBRiskAuthorizationRequest,
    capacity: StrategyBRiskCapacity,
) -> StrategyBAtomicRiskCommitResult:
    """
    Validate locally, then perform one atomic server-side pending-risk commit.

    This adapter has reservation authority only. A committed reservation is
    not itself permission to place an order.
    """

    try:
        pure = authorize_and_reserve_strategy_b_risk(
            request=request,
            capacity=capacity,
            existing_reservation=None,
        )
    except Exception:
        return _blocked("ATOMIC_RISK_COMMIT_ERROR")

    if not pure.authorized or pure.reservation is None:
        reason = str(pure.reason or "").strip()

        if reason not in _ALLOWED_BLOCK_REASONS:
            reason = "ATOMIC_RISK_COMMIT_ERROR"

        return _blocked(reason)

    reservation = pure.reservation

    try:
        ledger_key, total_key = _ledger_keys(request)
        payload = _payload(reservation)

        response = redis_client.eval(
            _ATOMIC_COMMIT_LUA,
            2,
            ledger_key,
            total_key,
            reservation.reservation_id,
            payload,
            repr(float(reservation.risk_usd)),
            repr(float(capacity.open_risk_usd)),
            repr(float(capacity.max_open_risk_usd)),
        )
    except Exception:
        return _blocked("ATOMIC_RISK_COMMIT_ERROR")

    parsed = _parse_server_response(response)

    if not parsed.committed:
        # Give semantically specific fail-closed reasons for malformed
        # success payloads where possible.
        if isinstance(response, (list, tuple)) and len(response) == 4:
            try:
                response_reason = _decode(response[0]).strip()
            except Exception:
                response_reason = ""

            if response_reason in _SUCCESS_REASONS:
                try:
                    response_reserved = float(_decode(response[2]))
                except Exception:
                    response_reserved = float("nan")

                try:
                    response_projected = float(_decode(response[3]))
                except Exception:
                    response_projected = float("nan")

                if (
                    not math.isfinite(response_reserved)
                    or response_reserved <= 0.0
                    or not math.isclose(
                        response_reserved,
                        float(reservation.risk_usd),
                        rel_tol=0.0,
                        abs_tol=1e-9,
                    )
                ):
                    return _blocked(
                        "ATOMIC_RISK_RESPONSE_RISK_MISMATCH"
                    )

                if (
                    not math.isfinite(response_projected)
                    or response_projected
                    < float(capacity.open_risk_usd)
                ):
                    return _blocked(
                        "ATOMIC_RISK_RESPONSE_PROJECTED_INVALID"
                    )

        return parsed

    # Validate numeric semantics before response identity so malformed
    # server accounting receives its precise fail-closed reason.
    if not math.isclose(
        parsed.reserved_risk_usd,
        float(reservation.risk_usd),
        rel_tol=0.0,
        abs_tol=1e-9,
    ):
        return _blocked(
            "ATOMIC_RISK_RESPONSE_RISK_MISMATCH"
        )

    if (
        not math.isfinite(parsed.projected_open_risk_usd)
        or parsed.projected_open_risk_usd
        < float(capacity.open_risk_usd)
    ):
        return _blocked(
            "ATOMIC_RISK_RESPONSE_PROJECTED_INVALID"
        )

    if parsed.reservation_id != reservation.reservation_id:
        return _blocked(
            "ATOMIC_RISK_RESPONSE_IDENTITY_MISMATCH"
        )

    return parsed
