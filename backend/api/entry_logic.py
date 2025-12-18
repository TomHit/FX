from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Union
import math


def entry_decision_m5(
    sym: str,
    direction: str,
    basis_price: float,
    target_price: float,
    alert_created_ms: int,
    now_ms: int,
    candles: Union[List[Dict[str, Any]], "Any"],  # list[dict] or pandas.DataFrame
    spread: Optional[float] = None,              # absolute price spread (e.g., 0.20 for XAUUSD)
    profiles: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """
    Decide M5 entry timing for an existing H1 opportunity snapshot.

    Inputs
    - direction: "BUY"/"SELL" or "UP"/"DOWN"
    - candles: last N M5 candles (needs >= 8), CLOSED bars only
      Each candle dict should have: open/high/low/close (or o/h/l/c)
    - spread: absolute spread; if None, spread filters are skipped

    Returns dict:
      {
        "ok": bool,
        "mode": "MOMENTUM"|"PULLBACK"|None,
        "reason": str,
        "entry_trigger": "CLOSE"|"BREAK_CONFIRM_HIGH"|"BREAK_CONFIRM_LOW"|None,
        "entry_price": float|None,     # suggested trigger level (break level) OR close price
        "tp_distance": float,
        "tp_pct": float,
        "age_min": float,
        "debug": {...}
      }
    """
    sym_u = (sym or "").upper().strip()

    # ---------------- profile ----------------
    DEFAULT = {
        "max_age_min": 30,
        "min_remaining_tp_frac": 0.50,
        "max_traveled_tp_frac": 0.35,

        "impulse_range_mult": 1.30,   # range_now >= mult * avg_range
        "impulse_body_frac": 0.60,    # body/range >= this
        "impulse_min_tp_frac": 0.15,  # abs(close - basis) >= this * tp_distance

        "pullback_min": 0.20,         # retrace fraction of impulse
        "pullback_max": 0.45,
        "pullback_reject": 0.60,

        "spread_tp_mult": 3.0,        # require tp_distance >= spread_tp_mult * spread
        "body_spread_mult": 1.2,      # require last_body >= body_spread_mult * spread (for entry bar)
        "prefer_mode": "PULLBACK",    # "PULLBACK" or "MOMENTUM"
        "use_break_trigger": True,    # if True: suggest break of confirm candle; else enter at close
    }
    cfg = dict(DEFAULT)
    if profiles:
        cfg.update(profiles.get("DEFAULT", {}) or {})
        cfg.update(profiles.get(sym_u, {}) or {})

    # ---------------- normalize direction ----------------
    d = (direction or "").upper().strip()
    if d == "UP":
        d = "BUY"
    elif d == "DOWN":
        d = "SELL"
    if d not in ("BUY", "SELL"):
        return {"ok": False, "mode": None, "reason": "bad_direction", "entry_trigger": None, "entry_price": None}

    # ---------------- basic distances ----------------
    if not (isinstance(basis_price, (int, float)) and isinstance(target_price, (int, float))):
        return {"ok": False, "mode": None, "reason": "bad_basis_or_target", "entry_trigger": None, "entry_price": None}

    basis = float(basis_price)
    target = float(target_price)
    tp_distance = abs(target - basis)
    tp_pct = (tp_distance / basis * 100.0) if basis else 0.0

    age_min = (max(0, int(now_ms) - int(alert_created_ms)) / 60_000.0) if alert_created_ms else 0.0

    # ---------------- candles adapter ----------------
    def _get_row(i: int) -> Dict[str, float]:
        if hasattr(candles, "iloc"):
            r = candles.iloc[i]
            o = float(r.get("open", r.get("o")))
            h = float(r.get("high", r.get("h")))
            l = float(r.get("low", r.get("l")))
            c = float(r.get("close", r.get("c")))
            return {"o": o, "h": h, "l": l, "c": c}
        else:
            r = candles[i]
            o = float(r.get("open", r.get("o")))
            h = float(r.get("high", r.get("h")))
            l = float(r.get("low", r.get("l")))
            c = float(r.get("close", r.get("c")))
            return {"o": o, "h": h, "l": l, "c": c}

    try:
        n = len(candles)
    except Exception:
        return {"ok": False, "mode": None, "reason": "bad_candles", "entry_trigger": None, "entry_price": None}

    if n < 8:
        return {"ok": False, "mode": None, "reason": "need_8_m5_bars", "entry_trigger": None, "entry_price": None}

    last = _get_row(n - 1)
    last_price = last["c"]

    # ---------------- pre-filters (universal) ----------------
    # remaining % to TP from current price (approx)
    remaining = abs(target - last_price)
    traveled = abs(last_price - basis)

    # late
    if age_min > float(cfg["max_age_min"]):
        return {
            "ok": False, "mode": None, "reason": f"too_late_age_min>{cfg['max_age_min']}",
            "entry_trigger": None, "entry_price": None,
            "tp_distance": tp_distance, "tp_pct": tp_pct, "age_min": age_min,
        }

    # not enough remaining room
    if tp_distance > 0 and remaining < float(cfg["min_remaining_tp_frac"]) * tp_distance:
        return {
            "ok": False, "mode": None, "reason": "too_close_to_target",
            "entry_trigger": None, "entry_price": None,
            "tp_distance": tp_distance, "tp_pct": tp_pct, "age_min": age_min,
        }

    # already moved too much away from basis (late chase)
    if tp_distance > 0 and traveled > float(cfg["max_traveled_tp_frac"]) * tp_distance:
        return {
            "ok": False, "mode": None, "reason": "already_moved_too_far_from_basis",
            "entry_trigger": None, "entry_price": None,
            "tp_distance": tp_distance, "tp_pct": tp_pct, "age_min": age_min,
        }

    # spread filters (optional)
    if isinstance(spread, (int, float)) and float(spread) > 0:
        sp = float(spread)
        if tp_distance < float(cfg["spread_tp_mult"]) * sp:
            return {
                "ok": False, "mode": None, "reason": "tp_too_small_vs_spread",
                "entry_trigger": None, "entry_price": None,
                "tp_distance": tp_distance, "tp_pct": tp_pct, "age_min": age_min,
            }

    # ---------------- helpers ----------------
    def _range(r: Dict[str, float]) -> float:
        return max(0.0, r["h"] - r["l"])

    def _body(r: Dict[str, float]) -> float:
        return abs(r["c"] - r["o"])

    def _body_frac(r: Dict[str, float]) -> float:
        rr = _range(r)
        return (_body(r) / rr) if rr > 0 else 0.0

    def _sign_dir() -> int:
        return +1 if d == "BUY" else -1

    sgn = _sign_dir()

    # last 5 ranges excluding the last bar for averaging
    ranges = []
    for j in range(n - 6, n - 1):
        r = _get_row(j)
        ranges.append(_range(r))
    avg_range = sum(ranges) / max(1, len(ranges))
    range_now = _range(last)

    # ---------------- MOMENTUM check ----------------
    # Requirements:
    # - range expansion
    # - good body fraction
    # - displacement from basis (min fraction of TP)
    # - candle direction consistent
    displacement_ok = (tp_distance > 0 and abs(last_price - basis) >= float(cfg["impulse_min_tp_frac"]) * tp_distance)

    dir_ok = (last["c"] > last["o"]) if d == "BUY" else (last["c"] < last["o"])
    momentum_ok = (
        avg_range > 0
        and range_now >= float(cfg["impulse_range_mult"]) * avg_range
        and _body_frac(last) >= float(cfg["impulse_body_frac"])
        and displacement_ok
        and dir_ok
    )

    # ---------------- PULLBACK check ----------------
    # We look for:
    # 1) an impulse bar recently (use bar n-2 as "impulse candidate")
    # 2) pullback (bar n-1) retracing 20-45% of impulse from basis
    # 3) confirmation (last bar) with reversal + break condition

    # Define impulse candidate as the previous bar (n-2), pullback as (n-1), confirm as last (n)
    imp = _get_row(n - 2)
    pb = _get_row(n - 1)
    cf = last  # confirm candle is the last closed

    # impulse direction should match
    imp_dir_ok = (imp["c"] > imp["o"]) if d == "BUY" else (imp["c"] < imp["o"])

    # impulse "exists" if it moved at least 10% of TP away from basis (close-to-basis displacement)
    imp_disp = abs(imp["c"] - basis)
    impulse_exists = (tp_distance > 0 and imp_disp >= 0.10 * tp_distance and imp_dir_ok)

    # pullback retrace fraction: how much pb moved against impulse (using prices around impulse close)
    # For BUY: impulse close above basis; pullback low dips below impulse close
    # For SELL: impulse close below basis; pullback high rises above impulse close
    retr_ok = False
    retr_frac = None
    if impulse_exists:
        if d == "BUY":
            impulse_up = max(0.0, imp["c"] - basis)
            retr = max(0.0, imp["c"] - pb["l"])
            retr_frac = (retr / impulse_up) if impulse_up > 0 else None
        else:
            impulse_dn = max(0.0, basis - imp["c"])
            retr = max(0.0, pb["h"] - imp["c"])
            retr_frac = (retr / impulse_dn) if impulse_dn > 0 else None

        if retr_frac is not None:
            if float(cfg["pullback_min"]) <= retr_frac <= float(cfg["pullback_max"]):
                retr_ok = True
            elif retr_frac > float(cfg["pullback_reject"]):
                retr_ok = False

    # confirmation candle: must reverse back in direction and close beyond pb in that direction
    # BUY confirm: bullish + close > pb high
    # SELL confirm: bearish + close < pb low
    cf_dir_ok = (cf["c"] > cf["o"]) if d == "BUY" else (cf["c"] < cf["o"])
    cf_strength_ok = _body_frac(cf) >= 0.5  # simple strength filter
    confirm_break_ok = (cf["c"] > pb["h"]) if d == "BUY" else (cf["c"] < pb["l"])
    pullback_ok = bool(impulse_exists and retr_ok and cf_dir_ok and cf_strength_ok and confirm_break_ok)

    # spread/body filter on confirmation bar (optional)
    if isinstance(spread, (int, float)) and float(spread) > 0:
        sp = float(spread)
        if _body(cf) < float(cfg["body_spread_mult"]) * sp:
            # don't allow entries on micro bodies vs spread
            momentum_ok = False
            pullback_ok = False

    # ---------------- choose mode ----------------
    prefer = str(cfg.get("prefer_mode") or "PULLBACK").upper()
    chosen = None
    if prefer == "PULLBACK":
        if pullback_ok:
            chosen = "PULLBACK"
        elif momentum_ok:
            chosen = "MOMENTUM"
    else:
        if momentum_ok:
            chosen = "MOMENTUM"
        elif pullback_ok:
            chosen = "PULLBACK"

    if not chosen:
        reason = "no_setup"
        if impulse_exists and retr_frac is not None and not retr_ok:
            reason = f"pullback_retrace_bad({retr_frac:.2f})"
        elif impulse_exists and retr_ok and not confirm_break_ok:
            reason = "pullback_no_confirm_break"
        elif not displacement_ok:
            reason = "momentum_no_displacement"
        elif avg_range <= 0:
            reason = "avg_range_zero"
        return {
            "ok": False,
            "mode": None,
            "reason": reason,
            "entry_trigger": None,
            "entry_price": None,
            "tp_distance": tp_distance,
            "tp_pct": tp_pct,
            "age_min": age_min,
            "debug": {
                "momentum_ok": momentum_ok,
                "pullback_ok": pullback_ok,
                "avg_range": avg_range,
                "range_now": range_now,
                "displacement_ok": displacement_ok,
                "impulse_exists": impulse_exists,
                "retr_frac": retr_frac,
            },
        }

    # ---------------- entry trigger suggestion ----------------
    use_break = bool(cfg.get("use_break_trigger", True))

    if chosen == "MOMENTUM":
        if use_break:
            # break of impulse high/low
            trig = "BREAK_CONFIRM_HIGH" if d == "BUY" else "BREAK_CONFIRM_LOW"
            entry_px = float(last["h"] if d == "BUY" else last["l"])
        else:
            trig = "CLOSE"
            entry_px = float(last["c"])
        return {
            "ok": True,
            "mode": "MOMENTUM",
            "reason": "momentum_confirmed",
            "entry_trigger": trig,
            "entry_price": entry_px,
            "tp_distance": tp_distance,
            "tp_pct": tp_pct,
            "age_min": age_min,
            "debug": {
                "avg_range": avg_range,
                "range_now": range_now,
                "body_frac": _body_frac(last),
                "displacement": abs(last["c"] - basis),
            },
        }

    # PULLBACK
    if use_break:
        trig = "BREAK_CONFIRM_HIGH" if d == "BUY" else "BREAK_CONFIRM_LOW"
        entry_px = float(cf["h"] if d == "BUY" else cf["l"])
    else:
        trig = "CLOSE"
        entry_px = float(cf["c"])

    return {
        "ok": True,
        "mode": "PULLBACK",
        "reason": "pullback_confirmed",
        "entry_trigger": trig,
        "entry_price": entry_px,
        "tp_distance": tp_distance,
        "tp_pct": tp_pct,
        "age_min": age_min,
        "debug": {
            "retr_frac": retr_frac,
            "imp_close": imp["c"],
            "pb_high": pb["h"],
            "pb_low": pb["l"],
            "cf_close": cf["c"],
            "cf_body_frac": _body_frac(cf),
        },
    }
