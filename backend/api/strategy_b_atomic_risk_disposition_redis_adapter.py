from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any

from api.strategy_b_atomic_risk_disposition_contract import (
    StrategyBRiskDispositionCommitRequest,
    validate_disposition_request,
)


_RECEIPT_TTL_SECONDS = 21 * 24 * 60 * 60


@dataclass(frozen=True)
class StrategyBRiskDispositionCommitResult:
    committed: bool
    reason: str
    reservation_id: str
    disposition: str
    released_risk_usd: float | None = None
    pending_total_after_usd: float | None = None
    receipt_written: bool = False

    observation_only: bool = True
    execution_authority: bool = False
    order_authority: bool = False
    broker_authority: bool = False
    strategy_a_mutation: bool = False


def _clean_component(value: object, name: str) -> str:
    text = str(value or "").strip()

    if not text:
        raise ValueError(f"{name}_EMPTY")

    if ":" in text:
        raise ValueError(f"{name}_INVALID_SEPARATOR")

    return text


def _ledger_keys(
    request: StrategyBRiskDispositionCommitRequest,
) -> tuple[str, str, str]:

    uid = _clean_component(request.uid, "UID")
    profile = _clean_component(request.profile_id, "PROFILE_ID")
    device = _clean_component(request.device_id, "DEVICE_ID")
    login = _clean_component(request.account_login, "ACCOUNT_LOGIN")

    prefix = (
        f"xtl:strategy_b:risk:pending:"
        f"{uid}:{profile}:{device}:{login}"
    )

    return (
        f"{prefix}:reservations",
        f"{prefix}:total",
        f"{prefix}:dispositions",
    )


# Redis numbers are currently stored in USD by the frozen reservation
# adapter.  Lua converts their decimal text to exact integer micro-USD
# before arithmetic, then serializes the result back as USD.
_LUA = r'''
local reservation_key = KEYS[1]
local total_key       = KEYS[2]
local receipt_key     = KEYS[3]

local reservation_id  = ARGV[1]
local disposition     = ARGV[2]
local decided_at_ms   = ARGV[3]
local receipt_ttl     = tonumber(ARGV[4])

local function fail(reason)
    return {0, reason, "", "", "0"}
end

local function usd_to_micro(v)
    if v == nil then
        return nil
    end

    local s = tostring(v)

    -- Strict non-negative decimal only.
    -- No exponent, sign, NaN or infinity.
    if string.match(s, "^%d+$") then
        s = s .. "."
    end

    local whole, frac = string.match(s, "^(%d+)%.(%d*)$")

    if whole == nil then
        return nil
    end

    if string.len(frac) > 6 then
        return nil
    end

    while string.len(frac) < 6 do
        frac = frac .. "0"
    end

    if frac == "" then
        frac = "000000"
    end

    local w = tonumber(whole)
    local f = tonumber(frac)

    if w == nil or f == nil then
        return nil
    end

    -- Redis Lua numbers are doubles. Keep arithmetic inside exact
    -- integer range used by this risk ledger.
    local micro = (w * 1000000) + f

    if micro < 0 or micro > 9007199254740991 then
        return nil
    end

    return micro
end

local function micro_to_usd(micro)
    if micro == nil or micro < 0 then
        return nil
    end

    local whole = math.floor(micro / 1000000)
    local frac  = micro - (whole * 1000000)

    if frac == 0 then
        return tostring(whole)
    end

    local frac_s = string.format("%06d", frac)
    frac_s = string.gsub(frac_s, "0+$", "")

    return tostring(whole) .. "." .. frac_s
end

-- ----------------------------------------------------------
-- Receipt first:
-- reservation may already have been removed by a prior commit.
-- ----------------------------------------------------------

local existing_receipt = redis.call(
    "HGET",
    receipt_key,
    reservation_id
)

if existing_receipt then
    local ok, receipt = pcall(cjson.decode, existing_receipt)

    if not ok or type(receipt) ~= "table" then
        return fail("MALFORMED_DISPOSITION_RECEIPT")
    end

    if tostring(receipt["reservation_id"] or "") ~= reservation_id then
        return fail("DISPOSITION_RECEIPT_IDENTITY_CONFLICT")
    end

    if tostring(receipt["disposition"] or "") ~= disposition then
        return fail("DISPOSITION_CONFLICT")
    end

    local risk_usd = tostring(receipt["stored_risk_usd"] or "")
    local after_usd = tostring(receipt["pending_total_after_usd"] or "")

    if risk_usd == "" or after_usd == "" then
        return fail("MALFORMED_DISPOSITION_RECEIPT")
    end

    return {
        1,
        "IDENTICAL_DISPOSITION",
        risk_usd,
        after_usd,
        "0"
    }
end

-- ----------------------------------------------------------
-- Reservation must still exist if no receipt exists.
-- ----------------------------------------------------------

local raw = redis.call(
    "HGET",
    reservation_key,
    reservation_id
)

if not raw then
    return fail("RESERVATION_NOT_FOUND")
end

local ok, reservation = pcall(cjson.decode, raw)

if not ok or type(reservation) ~= "table" then
    return fail("MALFORMED_RESERVATION")
end

if tostring(reservation["reservation_id"] or "") ~= reservation_id then
    return fail("RESERVATION_IDENTITY_CONFLICT")
end

if tostring(reservation["status"] or "") ~= "RESERVED" then
    return fail("RESERVATION_STATUS_INVALID")
end

local risk_raw = reservation["risk_usd"]

if risk_raw == nil then
    return fail("RESERVATION_RISK_MISSING")
end

local risk_micro = usd_to_micro(risk_raw)

if risk_micro == nil then
    return fail("RESERVATION_RISK_INVALID")
end

-- ----------------------------------------------------------
-- Ledger consistency.
-- ----------------------------------------------------------

local hlen = redis.call("HLEN", reservation_key)
local total_raw = redis.call("GET", total_key)

if hlen <= 0 then
    return fail("PENDING_LEDGER_INCONSISTENT")
end

if not total_raw then
    return fail("PENDING_LEDGER_INCONSISTENT")
end

local total_micro = usd_to_micro(total_raw)

if total_micro == nil then
    return fail("PENDING_TOTAL_INVALID")
end

if risk_micro > total_micro then
    return fail("PENDING_TOTAL_UNDERFLOW")
end

local after_micro = total_micro - risk_micro
local after_usd = micro_to_usd(after_micro)
local risk_usd = micro_to_usd(risk_micro)

if not after_usd or not risk_usd then
    return fail("PENDING_ARITHMETIC_INVALID")
end

-- ----------------------------------------------------------
-- Immutable receipt generated from server/stored facts.
-- Caller supplied no decrement.
-- ----------------------------------------------------------

local receipt = {
    schema_version = 1,
    reservation_id = reservation_id,
    disposition = disposition,
    stored_risk_usd = risk_usd,
    pending_total_after_usd = after_usd,
    decided_at_ms = decided_at_ms,
    status = "COMMITTED"
}

local receipt_json = cjson.encode(receipt)

-- Atomic mutation.
redis.call("HDEL", reservation_key, reservation_id)

if after_micro == 0 then
    redis.call("DEL", total_key)
else
    redis.call("SET", total_key, after_usd)
end

redis.call(
    "HSET",
    receipt_key,
    reservation_id,
    receipt_json
)

if receipt_ttl and receipt_ttl > 0 then
    redis.call("EXPIRE", receipt_key, receipt_ttl)
end

return {
    1,
    "COMMITTED",
    risk_usd,
    after_usd,
    "1"
}
'''


def _decode(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


def commit_strategy_b_risk_disposition(
    redis_client: Any,
    *,
    request: StrategyBRiskDispositionCommitRequest,
) -> StrategyBRiskDispositionCommitResult:

    validate_disposition_request(request)

    reservation_key, total_key, receipt_key = _ledger_keys(request)

    raw = redis_client.eval(
        _LUA,
        3,
        reservation_key,
        total_key,
        receipt_key,
        request.reservation_id,
        request.disposition,
        str(request.decided_at_ms),
        str(_RECEIPT_TTL_SECONDS),
    )

    if not isinstance(raw, (list, tuple)) or len(raw) != 5:
        return StrategyBRiskDispositionCommitResult(
            committed=False,
            reason="MALFORMED_REDIS_RESPONSE",
            reservation_id=request.reservation_id,
            disposition=request.disposition,
        )

    try:
        committed = int(raw[0]) == 1
        reason = _decode(raw[1])
        risk_text = _decode(raw[2])
        after_text = _decode(raw[3])
        receipt_written = _decode(raw[4]) == "1"

        risk = float(risk_text) if risk_text else None
        after = float(after_text) if after_text else None
    except Exception:
        return StrategyBRiskDispositionCommitResult(
            committed=False,
            reason="MALFORMED_REDIS_RESPONSE",
            reservation_id=request.reservation_id,
            disposition=request.disposition,
        )

    return StrategyBRiskDispositionCommitResult(
        committed=committed,
        reason=reason,
        reservation_id=request.reservation_id,
        disposition=request.disposition,
        released_risk_usd=risk,
        pending_total_after_usd=after,
        receipt_written=receipt_written,
    )
