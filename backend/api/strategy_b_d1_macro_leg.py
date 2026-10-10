from __future__ import annotations

from typing import Any, Mapping, Sequence

from api.bias_engine import DEFAULT_CONFIG, _pivot_highs, _pivot_lows


# Strategy-B D1 structural macro-leg calibration.
#
# These thresholds describe extension/maturity of the CURRENT
# structural D1 leg. They are NOT entry triggers.
STRATEGY_B_D1_MATURE_ATR = 3.0
STRATEGY_B_D1_EXTENDED_ATR = 4.0


def _number(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _time_ms(bar: Mapping[str, Any]) -> int | None:
    for key in (
        "t_open_ms",
        "tOpenMs",
        "open_time_ms",
        "time_ms",
        "t",
        "time",
    ):
        value = bar.get(key)

        if value is None:
            continue

        try:
            x = int(float(value))

            if x < 10_000_000_000:
                x *= 1000

            return x
        except (TypeError, ValueError):
            continue

    return None


def _d1_features(
    dxy_bias: Mapping[str, Any],
) -> Mapping[str, Any]:
    # Canonical bias_engine contract:
    # dxy_bias["timeframes"]["D1"]
    timeframes = dxy_bias.get("timeframes")

    if isinstance(timeframes, Mapping):
        value = timeframes.get("D1")

        if isinstance(value, Mapping):
            return value

    # Compatibility for focused/unit-test inputs.
    value = dxy_bias.get("D1")

    if not isinstance(value, Mapping):
        value = dxy_bias.get("d1")

    if not isinstance(value, Mapping):
        return {}

    return value


def _maturity_state(structural_leg_atr: float) -> str:
    if structural_leg_atr >= STRATEGY_B_D1_EXTENDED_ATR:
        return "EXTENDED"

    if structural_leg_atr >= STRATEGY_B_D1_MATURE_ATR:
        return "MATURE"

    return "EXPANDING"


def calculate_strategy_b_d1_macro_leg(
    bars_d1: Sequence[Mapping[str, Any]],
    dxy_bias: Mapping[str, Any],
) -> dict[str, Any]:
    """
    Pure Strategy-B D1 structural macro-leg calculator.

    Origin rule:
      BULLISH -> latest causally confirmed D1 2/2 LOW
      BEARISH -> latest causally confirmed D1 2/2 HIGH

    Magnitude:
      favorable distance from structural origin to latest D1 close
      divided by the canonical D1 ATR supplied by dxy_bias.

    No Redis access.
    No broker access.
    No execution authority.
    No H4/H1/M15 qualification.
    """

    out: dict[str, Any] = {
        "data_ok": False,
        "state": "NOT_ESTABLISHED",
        "reason": "D1_STRUCTURAL_LEG_NOT_ESTABLISHED",
        "direction": "UNKNOWN",
        "origin_type": None,
        "origin_index": None,
        "origin_time_ms": None,
        "origin_price": None,
        "origin_confirmed_index": None,
        "origin_confirmed_time_ms": None,
        "current_time_ms": None,
        "current_price": None,
        "atr": None,
        "move": None,
        "structural_leg_atr": None,
        "pivot_left": int(DEFAULT_CONFIG.pivot_left),
        "pivot_right": int(DEFAULT_CONFIG.pivot_right),
    }

    bars = list(bars_d1 or [])

    if not bars:
        out["reason"] = "D1_BARS_MISSING"
        return out

    d1 = _d1_features(dxy_bias)

    if not d1:
        out["reason"] = "D1_BIAS_MISSING"
        return out

    if not bool(d1.get("data_ok")):
        out["reason"] = "D1_BIAS_DATA_NOT_OK"
        return out

    direction = str(
        d1.get("direction") or "UNKNOWN"
    ).upper()

    out["direction"] = direction

    if direction not in {"BULLISH", "BEARISH"}:
        out["reason"] = "D1_DIRECTION_NOT_DIRECTIONAL"
        return out

    features = d1.get("features")

    if not isinstance(features, Mapping):
        features = {}

    atr = _number(features.get("atr"))

    if atr is None:
        atr = _number(d1.get("atr"))

    if atr is None or atr <= 0:
        out["reason"] = "D1_ATR_INVALID"
        return out

    current_price = _number(
        bars[-1].get("c", bars[-1].get("close"))
    )

    if current_price is None:
        out["reason"] = "D1_CURRENT_CLOSE_MISSING"
        return out

    left = int(DEFAULT_CONFIG.pivot_left)
    right = int(DEFAULT_CONFIG.pivot_right)

    if direction == "BULLISH":
        pivots = _pivot_lows(
            bars,
            left,
            right,
        )

        if not pivots:
            out["reason"] = "D1_CONFIRMED_LOW_MISSING"
            return out

        origin_index = int(pivots[-1])
        origin_type = "LOW"

        origin_price = _number(
            bars[origin_index].get(
                "l",
                bars[origin_index].get("low"),
            )
        )

        if origin_price is None:
            out["reason"] = "D1_ORIGIN_PRICE_MISSING"
            return out

        move = current_price - origin_price

    else:
        pivots = _pivot_highs(
            bars,
            left,
            right,
        )

        if not pivots:
            out["reason"] = "D1_CONFIRMED_HIGH_MISSING"
            return out

        origin_index = int(pivots[-1])
        origin_type = "HIGH"

        origin_price = _number(
            bars[origin_index].get(
                "h",
                bars[origin_index].get("high"),
            )
        )

        if origin_price is None:
            out["reason"] = "D1_ORIGIN_PRICE_MISSING"
            return out

        move = origin_price - current_price

    confirmed_index = origin_index + right

    # Defensive causal guard. Production pivot functions should
    # already guarantee this.
    if confirmed_index >= len(bars):
        out["reason"] = "D1_PIVOT_NOT_YET_CONFIRMED"
        return out

    structural_leg_atr = move / atr

    # A canonical direction can occasionally lag enough that
    # price has crossed the selected origin. Do not classify
    # such a state as a valid expanding/mature macro leg.
    if structural_leg_atr < 0:
        out.update(
            {
                "origin_type": origin_type,
                "origin_index": origin_index,
                "origin_time_ms": _time_ms(
                    bars[origin_index]
                ),
                "origin_price": origin_price,
                "origin_confirmed_index": confirmed_index,
                "origin_confirmed_time_ms": _time_ms(
                    bars[confirmed_index]
                ),
                "current_time_ms": _time_ms(bars[-1]),
                "current_price": current_price,
                "atr": atr,
                "move": move,
                "structural_leg_atr": structural_leg_atr,
                "reason": "D1_STRUCTURAL_LEG_DIRECTION_MISMATCH",
            }
        )
        return out

    state = _maturity_state(structural_leg_atr)

    out.update(
        {
            "data_ok": True,
            "state": state,
            "reason": "D1_STRUCTURAL_LEG_OK",
            "origin_type": origin_type,
            "origin_index": origin_index,
            "origin_time_ms": _time_ms(
                bars[origin_index]
            ),
            "origin_price": origin_price,
            "origin_confirmed_index": confirmed_index,
            "origin_confirmed_time_ms": _time_ms(
                bars[confirmed_index]
            ),
            "current_time_ms": _time_ms(bars[-1]),
            "current_price": current_price,
            "atr": atr,
            "move": move,
            "structural_leg_atr": structural_leg_atr,
        }
    )

    return out
