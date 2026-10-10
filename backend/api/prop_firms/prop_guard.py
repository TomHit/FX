# /opt/xauapi/api/prop_firms/prop_guard.py

import math
from .prop_config import PROP_FIRM_RULES, SYMBOL_SPECS


def floor_to_step(value: float, step: float) -> float:
    return math.floor(value / step) * step


def compute_loss_per_lot_at_stop(
    *,
    symbol: str,
    entry: float,
    sl: float,
):
    """
    Pure USD loss-per-lot conversion for one entry/stop geometry.

    This is the shared conversion primitive used by prop sizing and may be
    consumed by observation-only Strategy-B risk proposal wiring.

    It does not calculate requested risk, normalize lots, enforce prop limits,
    reserve risk, place orders, contact MT5, or mutate Strategy A.
    """
    symbol = str(symbol or "").upper().strip()

    spec = SYMBOL_SPECS.get(symbol)
    if not spec:
        return {
            "data_ok": False,
            "reason": "UNSUPPORTED_SYMBOL",
            "source": None,
            "loss_per_lot_at_stop": None,
        }

    try:
        entry_f = float(entry)
        sl_f = float(sl)
    except (TypeError, ValueError):
        return {
            "data_ok": False,
            "reason": "PRICE_INPUT_INVALID",
            "source": None,
            "loss_per_lot_at_stop": None,
        }

    if not math.isfinite(entry_f) or not math.isfinite(sl_f):
        return {
            "data_ok": False,
            "reason": "PRICE_INPUT_INVALID",
            "source": None,
            "loss_per_lot_at_stop": None,
        }

    sl_dist = abs(entry_f - sl_f)
    if sl_dist <= 0:
        return {
            "data_ok": False,
            "reason": "SL_DISTANCE_INVALID",
            "source": None,
            "loss_per_lot_at_stop": None,
        }

    loss_per_lot_quote = sl_dist * spec["contract_size"]

    base_ccy = symbol[:3] if len(symbol) >= 6 else ""
    quote_ccy = symbol[3:6] if len(symbol) >= 6 else ""

    if symbol == "XAUUSD":
        base_ccy = "XAU"
        quote_ccy = "USD"

    if quote_ccy == "USD":
        conversion_rate = 1.0
        loss_per_lot_usd = loss_per_lot_quote
        conversion_method = "QUOTE_USD"

    elif base_ccy == "USD":
        if entry_f <= 0:
            return {
                "data_ok": False,
                "reason": "ENTRY_INVALID_FOR_USD_BASE_CONVERSION",
                "source": None,
                "loss_per_lot_at_stop": None,
            }

        conversion_rate = entry_f
        loss_per_lot_usd = loss_per_lot_quote / conversion_rate
        conversion_method = "BASE_USD_DIV_ENTRY"

    else:
        return {
            "data_ok": False,
            "reason": "UNSUPPORTED_USD_CONVERSION",
            "source": None,
            "loss_per_lot_at_stop": None,
        }

    if not math.isfinite(loss_per_lot_usd) or loss_per_lot_usd <= 0:
        return {
            "data_ok": False,
            "reason": "LOSS_PER_LOT_AT_STOP_INVALID",
            "source": None,
            "loss_per_lot_at_stop": None,
        }

    return {
        "data_ok": True,
        "reason": "LOSS_PER_LOT_AT_STOP_READY",
        "source": conversion_method,
        "loss_per_lot_at_stop": float(loss_per_lot_usd),
        "loss_per_lot_quote": float(loss_per_lot_quote),
        "conversion_rate": float(conversion_rate),
        "base_ccy": base_ccy,
        "quote_ccy": quote_ccy,
        "contract_size": float(spec["contract_size"]),
        "stop_distance_price": float(sl_dist),
    }


def normalize_lots_for_symbol(
    *,
    symbol: str,
    raw_lots: float,
):
    """
    Pure shared lot-normalization primitive.

    Uses the same SYMBOL_SPECS, floor-to-step policy, and minimum-lot rule
    already used by compute_prop_check().

    It does not calculate risk, enforce prop limits, reserve risk, place
    orders, contact MT5, or mutate any strategy state.
    """
    symbol = str(symbol or "").upper().strip()

    spec = SYMBOL_SPECS.get(symbol)
    if not spec:
        return {
            "data_ok": False,
            "reason": "UNSUPPORTED_SYMBOL",
            "lots": None,
        }

    try:
        raw_lots_f = float(raw_lots)
        lot_step = float(spec["lot_step"])
        min_lot = float(spec["min_lot"])
    except (TypeError, ValueError, KeyError):
        return {
            "data_ok": False,
            "reason": "LOT_INPUT_INVALID",
            "lots": None,
        }

    if (
        not math.isfinite(raw_lots_f)
        or raw_lots_f <= 0
        or not math.isfinite(lot_step)
        or lot_step <= 0
        or not math.isfinite(min_lot)
        or min_lot <= 0
    ):
        return {
            "data_ok": False,
            "reason": "LOT_INPUT_INVALID",
            "lots": None,
        }

    lots = floor_to_step(raw_lots_f, lot_step)

    if not math.isfinite(lots) or lots <= 0:
        return {
            "data_ok": False,
            "reason": "NORMALIZED_LOTS_INVALID",
            "lots": None,
        }

    if lots < min_lot:
        return {
            "data_ok": False,
            "reason": "LOT_SIZE_BELOW_BROKER_MINIMUM",
            "lots": None,
            "raw_lots": float(raw_lots_f),
            "lot_step": float(lot_step),
            "min_lot": float(min_lot),
        }

    return {
        "data_ok": True,
        "reason": "LOTS_READY",
        "lots": float(lots),
        "raw_lots": float(raw_lots_f),
        "lot_step": float(lot_step),
        "min_lot": float(min_lot),
    }


def validate_prop_capacity(
    *,
    firm: str,
    phase: str,
    account_size: float,
    proposed_risk_usd: float,
    daily_loss_used: float = 0.0,
    max_loss_used: float = 0.0,
    open_risk_usd: float = 0.0,
    open_positions_count: int = 0,
    max_open_risk_pct: float = 3.0,
    max_open_positions: int = 3,
):
    """
    Pure shared prop-capacity validator for an already-sized proposal.

    The caller owns stop geometry, USD loss-per-lot conversion, lot sizing,
    and broker lot normalization. This function only validates the resulting
    proposed USD risk against the existing account-level prop limits.

    It does not calculate or modify SL, TP, lots, requested risk, reserve
    risk, place orders, contact MT5, or mutate strategy state.
    """
    firm = str(firm or "").lower().strip()
    phase = str(phase or "").lower().strip()

    if firm not in PROP_FIRM_RULES:
        return {
            "verdict": "BLOCK",
            "reasons": [f"Unknown prop firm: {firm}"],
        }

    firm_cfg = PROP_FIRM_RULES[firm]

    if phase not in firm_cfg["phases"]:
        return {
            "verdict": "BLOCK",
            "reasons": [
                f"Unknown phase '{phase}' for firm '{firm}'"
            ],
        }

    try:
        account_size_f = float(account_size)
        proposed_risk_f = float(proposed_risk_usd)
        daily_loss_used_f = float(daily_loss_used)
        max_loss_used_f = float(max_loss_used)
        open_risk_usd_f = float(open_risk_usd)
        max_open_risk_pct_f = float(max_open_risk_pct)
        open_positions_count_i = int(open_positions_count)
        max_open_positions_i = int(max_open_positions)
    except (TypeError, ValueError):
        return {
            "verdict": "BLOCK",
            "reasons": ["Invalid prop capacity input"],
        }

    numeric_values = (
        account_size_f,
        proposed_risk_f,
        daily_loss_used_f,
        max_loss_used_f,
        open_risk_usd_f,
        max_open_risk_pct_f,
    )

    if not all(math.isfinite(v) for v in numeric_values):
        return {
            "verdict": "BLOCK",
            "reasons": ["Invalid prop capacity input"],
        }

    if account_size_f <= 0:
        return {
            "verdict": "BLOCK",
            "reasons": ["Invalid account size"],
        }

    if proposed_risk_f <= 0:
        return {
            "verdict": "BLOCK",
            "reasons": ["Invalid proposed risk"],
        }

    if max_open_risk_pct_f <= 0:
        return {
            "verdict": "BLOCK",
            "reasons": ["Invalid max open risk percent"],
        }

    if open_positions_count_i < 0 or max_open_positions_i <= 0:
        return {
            "verdict": "BLOCK",
            "reasons": ["Invalid open position capacity"],
        }

    rules = firm_cfg["phases"][phase]

    daily_limit = account_size_f * (
        rules["daily_loss_pct"] / 100.0
    )
    max_loss_limit = account_size_f * (
        rules["max_loss_pct"] / 100.0
    )
    max_open_risk_limit = account_size_f * (
        max_open_risk_pct_f / 100.0
    )

    reasons = []
    verdict = "OK"

    if open_positions_count_i >= max_open_positions_i:
        verdict = "BLOCK"
        reasons.append(
            "Maximum open positions reached "
            f"({open_positions_count_i}/{max_open_positions_i})"
        )

    if daily_loss_used_f + proposed_risk_f > daily_limit:
        verdict = "BLOCK"
        reasons.append("Daily loss limit would be breached")

    if max_loss_used_f + proposed_risk_f > max_loss_limit:
        verdict = "BLOCK"
        reasons.append("Maximum loss limit would be breached")

    if open_risk_usd_f + proposed_risk_f > max_open_risk_limit:
        verdict = "BLOCK"
        reasons.append("Internal open-risk limit would be breached")

    risk_per_idea_pct = rules.get("risk_per_idea_pct")
    if risk_per_idea_pct is not None:
        idea_limit = account_size_f * (
            float(risk_per_idea_pct) / 100.0
        )
        if proposed_risk_f > idea_limit:
            verdict = "BLOCK"
            reasons.append(
                "FundingPips risk-per-idea limit would be breached"
            )

    return {
        "firm": firm,
        "phase": phase,
        "account_size": float(account_size_f),
        "proposed_risk_usd": float(proposed_risk_f),
        "daily_limit_usd": float(daily_limit),
        "daily_room_usd": float(
            daily_limit - daily_loss_used_f
        ),
        "max_loss_limit_usd": float(max_loss_limit),
        "max_loss_room_usd": float(
            max_loss_limit - max_loss_used_f
        ),
        "max_open_risk_limit_usd": float(
            max_open_risk_limit
        ),
        "open_risk_after_usd": float(
            open_risk_usd_f + proposed_risk_f
        ),
        "open_positions_count": int(
            open_positions_count_i
        ),
        "max_open_positions": int(
            max_open_positions_i
        ),
        "verdict": verdict,
        "reasons": reasons or ["OK to place"],
    }


def compute_prop_check(
    *,
    firm: str,
    phase: str,
    account_size: float,
    symbol: str,
    side: str,
    entry: float,
    sl: float,
    risk_pct: float = 1.0,
    target_rr: float = 2.0,
    daily_loss_used: float = 0.0,
    max_loss_used: float = 0.0,
    open_risk_usd: float = 0.0,
    open_positions_count: int = 0,
    max_open_risk_pct: float = 3.0,
    max_open_positions: int = 3,
):
    firm = firm.lower()
    phase = phase.lower()
    symbol = symbol.upper()
    side = side.upper()
    if firm not in PROP_FIRM_RULES:
        return {
            "verdict": "BLOCK",
            "reasons": [f"Unknown prop firm: {firm}"],
        }

    firm_cfg = PROP_FIRM_RULES[firm]

    if phase not in firm_cfg["phases"]:
        return {
            "verdict": "BLOCK",
            "reasons": [f"Unknown phase '{phase}' for firm '{firm}'"],
        }

    if side not in ("BUY", "SELL"):
        return {
            "verdict": "BLOCK",
            "reasons": [f"Invalid side: {side}"],
        }

    if account_size <= 0:
        return {
            "verdict": "BLOCK",
            "reasons": ["Invalid account size"],
        }

    if risk_pct <= 0:
        return {
            "verdict": "BLOCK",
            "reasons": ["Invalid risk percent"],
        }
    if target_rr <= 0:
        return {
            "verdict": "BLOCK",
            "reasons": ["Invalid target RR"],
        }

    rules = firm_cfg["phases"][phase]

    
    spec = SYMBOL_SPECS.get(symbol)
    if not spec:
        return {
            "verdict": "BLOCK",
            "reasons": [f"Unsupported symbol: {symbol}"]
        }

    sl_dist = abs(entry - sl)
    if sl_dist <= 0:
        return {
            "verdict": "BLOCK",
            "reasons": ["Invalid SL distance"],
        }

    risk_usd = account_size * (risk_pct / 100.0)

    conversion = compute_loss_per_lot_at_stop(
        symbol=symbol,
        entry=entry,
        sl=sl,
    )

    if not conversion.get("data_ok"):
        reason = str(conversion.get("reason") or "")

        if reason == "ENTRY_INVALID_FOR_USD_BASE_CONVERSION":
            reasons = ["Invalid entry for USD-base conversion"]
        elif reason == "UNSUPPORTED_USD_CONVERSION":
            reasons = [
                f"Unsupported cross-currency symbol for USD risk conversion: {symbol}"
            ]
        elif reason == "LOSS_PER_LOT_AT_STOP_INVALID":
            reasons = ["Invalid USD loss per lot"]
        else:
            reasons = [reason or "Invalid USD loss per lot"]

        return {
            "verdict": "BLOCK",
            "reasons": reasons,
        }

    loss_per_lot_quote = float(conversion["loss_per_lot_quote"])
    loss_per_lot_usd = float(conversion["loss_per_lot_at_stop"])
    base_ccy = str(conversion["base_ccy"])
    quote_ccy = str(conversion["quote_ccy"])
    conversion_method = str(conversion["source"])
    conversion_rate = float(conversion["conversion_rate"])

    raw_lots = risk_usd / loss_per_lot_usd
    lots = floor_to_step(raw_lots, spec["lot_step"])

    if lots < spec["min_lot"]:
        return {
            "verdict": "BLOCK",
            "reasons": ["Lot size below broker minimum"],
            "debug": {
                "risk_usd": round(risk_usd, 2),
                "sl_dist": sl_dist,
                "loss_per_lot_quote": round(loss_per_lot_quote, 5),
                "loss_per_lot_usd": round(loss_per_lot_usd, 5),
                "raw_lots": round(raw_lots, 5),
                "min_lot": spec["min_lot"],
                "base_ccy": base_ccy,
                "quote_ccy": quote_ccy,
                "conversion_method": conversion_method,
                "conversion_rate": round(conversion_rate, 5),
            },
        }

    actual_risk_usd = lots * loss_per_lot_usd
    actual_risk_pct = (actual_risk_usd / account_size) * 100.0

    if side == "BUY":
        tp = entry + (target_rr * sl_dist)
    else:
        tp = entry - (target_rr * sl_dist)

    daily_limit = account_size * (rules["daily_loss_pct"] / 100.0)
    max_loss_limit = account_size * (rules["max_loss_pct"] / 100.0)
    max_open_risk_limit = account_size * (max_open_risk_pct / 100.0)

    reasons = []
    verdict = "OK"
    if open_positions_count >= max_open_positions:
        verdict = "BLOCK"
        reasons.append(
            f"Maximum open positions reached ({open_positions_count}/{max_open_positions})"
        )

    if daily_loss_used + actual_risk_usd > daily_limit:
        verdict = "BLOCK"
        reasons.append("Daily loss limit would be breached")

    if max_loss_used + actual_risk_usd > max_loss_limit:
        verdict = "BLOCK"
        reasons.append("Maximum loss limit would be breached")

    if open_risk_usd + actual_risk_usd > max_open_risk_limit:
        verdict = "BLOCK"
        reasons.append("Internal open-risk limit would be breached")

    risk_per_idea_pct = rules.get("risk_per_idea_pct")
    if risk_per_idea_pct is not None:
        idea_limit = account_size * (risk_per_idea_pct / 100.0)
        if actual_risk_usd > idea_limit:
            verdict = "BLOCK"
            reasons.append("FundingPips risk-per-idea limit would be breached")

    target_pct = rules.get("target_pct")
    target_usd = account_size * (target_pct / 100.0) if target_pct else None

    return {
        "firm": firm,
        "phase": phase,
        "symbol": symbol,
        "side": side,
        "entry": round(entry, 5),
        "sl": round(sl, 5),
        "tp": round(tp, 5),
        "target_rr": target_rr,
        "lots": round(lots, 2),
        "risk_usd": round(actual_risk_usd, 2),
        "risk_pct": round(actual_risk_pct, 3),
        "risk_requested_usd": round(risk_usd, 2),
        "raw_lots": round(raw_lots, 4),
        "loss_per_lot_quote": round(loss_per_lot_quote, 5),
        "loss_per_lot_usd": round(loss_per_lot_usd, 5),
        "base_ccy": base_ccy,
        "quote_ccy": quote_ccy,
        "conversion_method": conversion_method,
        "conversion_rate": round(conversion_rate, 5),
        "daily_limit_usd": round(daily_limit, 2),
        "daily_room_usd": round(daily_limit - daily_loss_used, 2),
        "max_loss_limit_usd": round(max_loss_limit, 2),
        "max_loss_room_usd": round(max_loss_limit - max_loss_used, 2),
        "open_risk_usd": round(open_risk_usd + actual_risk_usd, 2),
        "target_usd": round(target_usd, 2) if target_usd else None,
        "verdict": verdict,
        "reasons": reasons or ["OK to place"],
    }