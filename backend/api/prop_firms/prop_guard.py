# /opt/xauapi/api/prop_firms/prop_guard.py

import math
from .prop_config import PROP_FIRM_RULES, SYMBOL_SPECS


def floor_to_step(value: float, step: float) -> float:
    return math.floor(value / step) * step


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
    loss_per_lot = sl_dist * spec["contract_size"]

    raw_lots = risk_usd / loss_per_lot
    lots = floor_to_step(raw_lots, spec["lot_step"])

    if lots < spec["min_lot"]:
        return {
            "verdict": "BLOCK",
            "reasons": ["Lot size below broker minimum"],
        }

    actual_risk_usd = lots * loss_per_lot
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
        "daily_limit_usd": round(daily_limit, 2),
        "daily_room_usd": round(daily_limit - daily_loss_used, 2),
        "max_loss_limit_usd": round(max_loss_limit, 2),
        "max_loss_room_usd": round(max_loss_limit - max_loss_used, 2),
        "open_risk_usd": round(open_risk_usd + actual_risk_usd, 2),
        "target_usd": round(target_usd, 2) if target_usd else None,
        "verdict": verdict,
        "reasons": reasons or ["OK to place"],
    }