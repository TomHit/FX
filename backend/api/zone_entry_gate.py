
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import logging
log = logging.getLogger(__name__)
import time
from typing import Any, Dict, List, Optional, Tuple





def _to_ms_any(x) -> int:
    try:
        if x is None:
            return 0
        if isinstance(x, (int, float)):
            v = int(x)
        else:
            v = int(float(str(x).strip()))
        # seconds -> ms
        if v > 0 and v < 10_000_000_000:
            return v * 1000
        return v
    except Exception:
        return 0


def _bar_f(b: dict, *keys: str) -> Optional[float]:
    for k in keys:
        if k in b and b.get(k) is not None:
            try:
                return float(b.get(k))
            except Exception:
                return None
    return None

def _closed_bars_before_or_at(bars: list, cutoff_ms: int, limit: int = 4) -> list:
    """Return chronological bar snapshots available at prediction time.

    The final row may be the currently forming touch candle. Its OHLC values
    represent only information available when the prediction was frozen.
    """
    rows = []
    for b in bars or []:
        if not isinstance(b, dict):
            continue
        if b.get("complete") is False:
            continue
        om = _to_ms_any(
            b.get("t_open_ms")
            or b.get("tOpenMs")
            or b.get("open_time_ms")
            or b.get("t")
            or b.get("time")
        )
        cm = _to_ms_any(b.get("t_close_ms"))
        if not cm and om:
            cm = om + 3_600_000
        if cutoff_ms and cm and cm > cutoff_ms:
            continue
        if _bar_f(b, "o") is None or _bar_f(b, "h") is None or _bar_f(b, "l") is None or _bar_f(b, "c") is None:
            continue
        rows.append((om or cm, b))
    rows.sort(key=lambda x: x[0])
    return [b for _, b in rows[-max(1, int(limit or 4)):]]


def _build_expected_setup_analysis(
    watch: dict,
    zone: dict,
    direction: str,
    bars: list | None = None,
    atr: float | None = None,
) -> dict:
    """Freeze a conservative prediction of market behavior at first zone touch.

    This is analytics-only. It does not select, block or modify a trade.
    The prediction is based only on bars available at/before the initial touch.
    It must not be rebuilt after REV_OK, because that would leak later evidence.
    """
    w = watch if isinstance(watch, dict) else {}
    z = zone if isinstance(zone, dict) else {}
    side = str(direction or w.get("direction") or "").upper().strip()
    zone_side = str(
        z.get("kind")
        or ("support" if side == "BUY" else "resistance" if side == "SELL" else "")
    ).upper().strip()

    zone_low = _bar_f(z, "low", "level")
    zone_high = _bar_f(z, "high", "level")
    touch_close_ms = _to_ms_any(w.get("touch_close_ms"))
    sample = _closed_bars_before_or_at(bars or [], touch_close_ms, 4)

    try:
        atr_f = float(atr) if atr is not None else None
    except Exception:
        atr_f = None
    if not atr_f or atr_f <= 0:
        atr_f = None
    prediction_server_ms = int(time.time() * 1000)
    evidence = {
        "zone_side": zone_side or None,
        "zone_source": z.get("zone_source"),
        "zone_role": z.get("zone_role"),
        "source_type": z.get("source_type"),
        "zone_low": zone_low,
        "zone_high": zone_high,
        "zone_level": _bar_f(z, "level"),
        "zone_tf": z.get("tf"),
        "touch_open_ms": _to_ms_any(w.get("touch_open_ms")),
        "touch_close_ms": touch_close_ms,
        "bars_used": len(sample),
        "prediction_basis": "FIRST_LIVE_ZONE_TOUCH",
        "touch_bar_was_forming": bool(
            touch_close_ms > prediction_server_ms
        ),
        "prediction_server_ms": prediction_server_ms,
        "arrival_net_atr": None,
        "arrival_last_range_atr": None,
        "arrival_last_body_fraction": None,
        "arrival_direction": None,
        "directional_bars_toward_zone": 0,
        "touch_close_location": None,
    }

    predicted = "UNCLASSIFIED"
    stage = "OBSERVING"
    reasons = []
    continuation = {
        "momentum_present": False,
        "pressure_into_zone": False,
        "zone_break_confirmed": False,
        "retest_present": False,
    }

    if len(sample) >= 2 and zone_low is not None and zone_high is not None:
        first_c = _bar_f(sample[0], "c")
        last = sample[-1]
        last_o = _bar_f(last, "o")
        last_h = _bar_f(last, "h")
        last_l = _bar_f(last, "l")
        last_c = _bar_f(last, "c")
        if None not in (first_c, last_o, last_h, last_l, last_c):
            net = float(last_c) - float(first_c)
            rng = max(0.0, float(last_h) - float(last_l))
            body = abs(float(last_c) - float(last_o))
            body_frac = body / rng if rng > 0 else 0.0
            net_atr = net / atr_f if atr_f else None
            range_atr = rng / atr_f if atr_f else None
            arrival_dir = "UP" if net > 0 else "DOWN" if net < 0 else "FLAT"
            toward_sign = -1 if zone_side == "SUPPORT" else 1 if zone_side == "RESISTANCE" else 0
            directional = 0
            for b in sample[-3:]:
                bo = _bar_f(b, "o"); bc = _bar_f(b, "c")
                if bo is None or bc is None:
                    continue
                if toward_sign < 0 and bc < bo:
                    directional += 1
                elif toward_sign > 0 and bc > bo:
                    directional += 1

            if last_c < zone_low:
                close_loc = "BELOW_ZONE"
            elif last_c > zone_high:
                close_loc = "ABOVE_ZONE"
            else:
                close_loc = "INSIDE_ZONE"

            evidence.update({
                "arrival_net_atr": round(net_atr, 3) if net_atr is not None else None,
                "arrival_last_range_atr": round(range_atr, 3) if range_atr is not None else None,
                "arrival_last_body_fraction": round(body_frac, 3),
                "arrival_direction": arrival_dir,
                "directional_bars_toward_zone": directional,
                "touch_close_location": close_loc,
            })

            toward_zone = (
                (zone_side == "SUPPORT" and net < 0)
                or (zone_side == "RESISTANCE" and net > 0)
            )
            strong_net = bool(net_atr is not None and abs(net_atr) >= 0.75)
            strong_last = bool(range_atr is not None and range_atr >= 0.90 and body_frac >= 0.50)
            pressure = bool(toward_zone and directional >= 2)
            momentum = bool(toward_zone and sum((strong_net, strong_last, pressure)) >= 2)
            broke = bool(
                (zone_side == "SUPPORT" and last_c < zone_low)
                or (zone_side == "RESISTANCE" and last_c > zone_high)
            )
            rejected = bool(
                (zone_side == "SUPPORT" and last_l <= zone_high and last_c > zone_high)
                or (zone_side == "RESISTANCE" and last_h >= zone_low and last_c < zone_low)
            )

            continuation.update({
                "momentum_present": momentum,
                "pressure_into_zone": pressure,
                "zone_break_confirmed": broke,
            })

            if broke and momentum:
                predicted = "CONTINUATION"
                stage = "ZONE_BREAK"
                reasons = ["MOMENTUM_INTO_ZONE", "ZONE_CLOSE_THROUGH"]
            elif momentum:
                predicted = "CONTINUATION"
                stage = "MOMENTUM_APPROACH"
                reasons = ["MOMENTUM_INTO_ZONE", "PRESSURE_INTO_ZONE"]
            elif rejected and not momentum:
                predicted = "REVERSAL"
                stage = "TOUCH_REJECTION"
                reasons = ["ZONE_REJECTION_CLOSE", "NO_STRONG_CONTINUATION_PRESSURE"]
            elif pressure:
                predicted = "UNCLASSIFIED"
                stage = "PRESSURE_PENDING"
                reasons = ["PRESSURE_INTO_ZONE", "MOMENTUM_THRESHOLD_NOT_MET"]
            else:
                predicted = "UNCLASSIFIED"
                stage = "OBSERVING"
                reasons = ["INSUFFICIENT_DIRECTIONAL_EVIDENCE"]

    if not reasons:
        reasons = ["INSUFFICIENT_ENTRY_BARS"]

    return {
        "schema_version": 2,
        "analytics_only": True,
        "immutable_prediction": True,
        "prediction_classifier_version": "market_behavior_prediction_v2",
        "prediction_frozen_at_ms": prediction_server_ms,
        "predicted_market_behavior": predicted,
        "prediction_stage": stage,
        "predicted_direction": (
            "SELL" if predicted == "CONTINUATION" and zone_side == "SUPPORT"
            else "BUY" if predicted == "CONTINUATION" and zone_side == "RESISTANCE"
            else "BUY" if predicted == "REVERSAL" and zone_side == "SUPPORT"
            else "SELL" if predicted == "REVERSAL" and zone_side == "RESISTANCE"
            else None
        ),
        "reason_codes": reasons,
        "continuation_sequence": continuation,
        "evidence_at_prediction": evidence,
        "selected_production_strategy": "ZONE_REVERSAL",
        "selected_production_strategy_version": "zone_reversal_v1",
    }

def _safe_deepcopy_json(value):
    """Create an isolated JSON-safe copy for immutable analytics."""
    try:
        return json.loads(
            json.dumps(
                value,
                separators=(",", ":"),
                default=str,
            )
        )
    except Exception:
        return None


def _zone_quality_sr_analytics(
    zone: dict,
    direction: str,
    entry_price: float | None,
    atr: float | None,
) -> dict:
    """
    Analytics-only zone-quality and local-SR opinion.

    This does not block or change a trade.
    No weighted score is created.
    """
    z = zone if isinstance(zone, dict) else {}
    side = str(direction or "").upper().strip()

    def _float_or_none(value):
        try:
            return float(value) if value is not None else None
        except Exception:
            return None

    def _int_or_zero(value):
        try:
            return int(value or 0)
        except Exception:
            return 0

    zone_low = _float_or_none(z.get("low"))
    zone_high = _float_or_none(z.get("high"))
    zone_level = _float_or_none(z.get("level"))

    if zone_low is None:
        zone_low = zone_level

    if zone_high is None:
        zone_high = zone_level

    touches = _int_or_zero(z.get("touches"))
    strength = _int_or_zero(z.get("strength"))
    sr_score = _float_or_none(z.get("sr_score"))
    distance_atr = _float_or_none(
        z.get("distance_atr")
        if z.get("distance_atr") is not None
        else z.get("dist_atr")
    )

    zone_tf = str(
        z.get("tf")
        or z.get("source_tf")
        or ""
    ).upper().strip()

    zone_role = str(
        z.get("zone_role")
        or ""
    ).upper().strip()

    source_type = str(
        z.get("source_type")
        or ""
    ).upper().strip()

    reason_codes = []

    if z.get("stale") is True:
        reason_codes.append("ZONE_STALE")

    if z.get("side_ok") is False:
        reason_codes.append("ZONE_SIDE_INVALID")

    if touches >= 3:
        reason_codes.append("MULTIPLE_ZONE_TOUCHES")
    elif touches == 2:
        reason_codes.append("TWO_ZONE_TOUCHES")
    elif touches == 1:
        reason_codes.append("SINGLE_ZONE_TOUCH")

    if strength >= 8:
        reason_codes.append("HIGH_ZONE_STRENGTH")
    elif strength >= 6:
        reason_codes.append("GOOD_ZONE_STRENGTH")
    elif strength > 0:
        reason_codes.append("LOW_ZONE_STRENGTH")

    if sr_score is not None:
        if sr_score >= 10:
            reason_codes.append("HIGH_SR_QUALITY")
        elif sr_score >= 7:
            reason_codes.append("ACCEPTABLE_SR_QUALITY")
        else:
            reason_codes.append("LOW_SR_QUALITY")

    if zone_tf == "H4":
        reason_codes.append("H4_ZONE")
    elif zone_tf == "H1":
        reason_codes.append("H1_ZONE")

    if source_type:
        reason_codes.append(
            f"SOURCE_{source_type}"
        )

    invalid = bool(
        z.get("stale") is True
        or z.get("side_ok") is False
        or zone_level is None
    )

    quality_evidence = bool(
        touches >= 2
        or strength >= 6
        or (
            sr_score is not None
            and sr_score >= 7
        )
    )

    if invalid:
        status = "FAIL"
    elif quality_evidence:
        status = "PASS"
    else:
        status = "NEUTRAL"

    if not reason_codes:
        reason_codes.append(
            "ZONE_QUALITY_DATA_LIMITED"
        )

    return {
        "schema_version": 1,
        "analytics_only": True,
        "status": status,
        "reason_codes": reason_codes,
        "snapshot": {
            "direction": side or None,
            "zone_side": (
                "SUPPORT"
                if side == "BUY"
                else "RESISTANCE"
                if side == "SELL"
                else None
            ),
            "zone_source": z.get("zone_source"),
            "selection_model": z.get(
                "selection_model"
            ),
            "zone_role": zone_role or None,
            "source_type": source_type or None,
            "zone_tf": zone_tf or None,
            "zone_low": zone_low,
            "zone_high": zone_high,
            "zone_level": zone_level,
            "touches": touches,
            "strength": strength,
            "sr_score": sr_score,
            "distance_atr": distance_atr,
            "entry_price": _float_or_none(
                entry_price
            ),
            "atr": _float_or_none(atr),
            "stale": bool(
                z.get("stale") is True
            ),
            "side_ok": (
                None
                if z.get("side_ok") is None
                else bool(z.get("side_ok"))
            ),
        },
    }


def _dxy_float_or_none(value):
    try:
        return float(value) if value is not None else None
    except Exception:
        return None

def _dxy_opposing_sr_next_three(
    sr: dict,
    *,
    required_direction: str | None,
    current_price: float | None = None,
    atr: float | None = None,
) -> dict:
    """Select the nearest meaningful opposing DXY SR from the immediate next three."""
    direction = str(required_direction or "").upper().strip()
    role = "supports" if direction == "BEARISH" else "resistances" if direction == "BULLISH" else None
    out = {
        "available": False,
        "required_direction": direction or None,
        "opposing_role": role[:-1].upper() if role else None,
        "candidate_count": 0,
        "selected": None,
        "next_3_opposing_sr": [],
    }
    if not isinstance(sr, dict) or not role:
        return out

    candidates = []

    # ---------------------------------------------------------
    # LIVE REAL_DXY tracker structure
    #
    # Current structure contains:
    #   support_path / resistance_path
    #   active_supports / active_resistances
    #
    # For required BEARISH DXY movement:
    #   supports below price are opposing SR.
    #
    # For required BULLISH DXY movement:
    #   resistances above price are opposing SR.
    # ---------------------------------------------------------
    live_rows = []

    if direction == "BEARISH":
        if isinstance(sr.get("support_path"), list):
            live_rows = sr.get("support_path") or []
        elif isinstance(sr.get("active_supports"), list):
            live_rows = sr.get("active_supports") or []

    elif direction == "BULLISH":
        if isinstance(sr.get("resistance_path"), list):
            live_rows = sr.get("resistance_path") or []
        elif isinstance(sr.get("active_resistances"), list):
            live_rows = sr.get("active_resistances") or []

    for z in live_rows:
        if not isinstance(z, dict):
            continue

        if z.get("stale") is True:
            continue

        if z.get("side_ok") is False:
            continue

        try:
            level = float(
                z.get("level")
                if z.get("level") is not None
                else z.get("price")
            )
        except Exception:
            continue

        # Keep only SR in the direction DXY must travel.
        if current_price is not None:
            if (
                direction == "BEARISH"
                and level >= float(current_price)
            ):
                continue

            if (
                direction == "BULLISH"
                and level <= float(current_price)
            ):
                continue

        distance_atr = _dxy_float_or_none(
            z.get("distance_atr")
            if z.get("distance_atr") is not None
            else z.get("dist_atr")
        )

        if (
            distance_atr is None
            and current_price is not None
            and atr
            and atr > 0
        ):
            distance_atr = (
                abs(float(current_price) - level)
                / float(atr)
            )

        if distance_atr is None:
            continue

        candidates.append({
            "level": level,
            "tf": str(
                z.get("tf") or ""
            ).upper().strip(),
            "strength": (
                _dxy_float_or_none(
                    z.get("strength")
                )
                or 0.0
            ),
            "sr_score": (
                _dxy_float_or_none(
                    z.get("sr_score")
                )
                or 0.0
            ),
            "touches": (
                _dxy_float_or_none(
                    z.get("touches")
                )
                or 0.0
            ),
            "distance_atr": max(
                0.0,
                float(distance_atr),
            ),
            "room_class": (
                str(
                    z.get("room_class") or ""
                ).upper().strip()
                or None
            ),
        })

    # ---------------------------------------------------------
    # Backward-compatible old SR schema.
    #
    # Keep this so analytics/older snapshots using:
    #   h1.supports / h4.resistances etc.
    # still work.
    # ---------------------------------------------------------
    containers = [sr]

    structure_context = sr.get(
        "structure_context"
    )

    if isinstance(
        structure_context,
        dict,
    ):
        containers.append(
            structure_context
        )

    for container in containers:
        for tf_name in ("H1", "H4"):
            tf = container.get(
                tf_name.lower()
            )

            if not isinstance(tf, dict):
                continue

            rows = []

            for key in (
                role,
                f"{role}_near",
                f"{role}_major",
            ):
                if isinstance(
                    tf.get(key),
                    list,
                ):
                    rows.extend(
                        tf.get(key) or []
                    )

            for z in rows:
                if not isinstance(z, dict):
                    continue

                if z.get("stale") is True:
                    continue

                if z.get("side_ok") is False:
                    continue

                try:
                    level = float(
                        z.get("level")
                    )
                except Exception:
                    continue

                if current_price is not None:
                    if (
                        direction == "BEARISH"
                        and level >= float(
                            current_price
                        )
                    ):
                        continue

                    if (
                        direction == "BULLISH"
                        and level <= float(
                            current_price
                        )
                    ):
                        continue

                distance_atr = (
                    _dxy_float_or_none(
                        z.get("distance_atr")
                        if z.get(
                            "distance_atr"
                        ) is not None
                        else z.get("dist_atr")
                    )
                )

                if (
                    distance_atr is None
                    and current_price is not None
                    and atr
                    and atr > 0
                ):
                    distance_atr = (
                        abs(
                            float(current_price)
                            - level
                        )
                        / float(atr)
                    )

                if distance_atr is None:
                    continue

                candidates.append({
                    "level": level,
                    "tf": tf_name,
                    "strength": (
                        _dxy_float_or_none(
                            z.get("strength")
                        )
                        or 0.0
                    ),
                    "sr_score": (
                        _dxy_float_or_none(
                            z.get("sr_score")
                        )
                        or 0.0
                    ),
                    "touches": (
                        _dxy_float_or_none(
                            z.get("touches")
                        )
                        or 0.0
                    ),
                    "distance_atr": max(
                        0.0,
                        float(distance_atr),
                    ),
                    "room_class": (
                        str(
                            z.get(
                                "room_class"
                            )
                            or ""
                        ).upper().strip()
                        or None
                    ),
                })

    candidates.sort(key=lambda x: float(x["distance_atr"]))
    unique = []
    for candidate in candidates:
        duplicate = next(
            (
                i for i, existing in enumerate(unique)
                if abs(float(existing["level"]) - float(candidate["level"]))
                <= max(1e-10, abs(float(candidate["level"])) * 1e-10)
            ),
            None,
        )
        rank = lambda x: (
            float(x.get("strength") or 0.0),
            float(x.get("sr_score") or 0.0),
            float(x.get("touches") or 0.0),
            1 if x.get("tf") == "H4" else 0,
        )
        if duplicate is None:
            unique.append(candidate)
        elif rank(candidate) > rank(unique[duplicate]):
            unique[duplicate] = candidate

    unique.sort(
        key=lambda x: float(
            x["distance_atr"]
        )
    )

    next_three = unique[:3]

    # ---------------------------------------------------------
    # Point-A entry-room authority:
    #
    # The immediate obstacle in DXY's required direction must
    # be the NEAREST MEANINGFUL opposing SR.
    #
    # Previously we selected the strongest SR among the next 3.
    # That could skip a nearer valid support/resistance and make
    # Point-A believe DXY had more room than it actually had.
    #
    # Example seen 17-08-2026:
    # a farther 99.2225 support was selected over nearer support
    # because its strength / score was higher.
    #
    # Keep the same "meaningful SR" definition already used by
    # the downstream sr_risk calculation:
    #   strength >= 8
    #   OR sr_score >= 10
    #   OR touches >= 3
    #   OR H4
    # ---------------------------------------------------------
    def _is_meaningful_opposing_sr(
        candidate: dict,
    ) -> bool:
        return bool(
            float(
                candidate.get(
                    "strength"
                )
                or 0.0
            )
            >= 8.0
            or float(
                candidate.get(
                    "sr_score"
                )
                or 0.0
            )
            >= 10.0
            or float(
                candidate.get(
                    "touches"
                )
                or 0.0
            )
            >= 3.0
            or str(
                candidate.get(
                    "tf"
                )
                or ""
            ).upper().strip()
            == "H4"
        )

    meaningful_next_three = [
        candidate
        for candidate in next_three
        if _is_meaningful_opposing_sr(
            candidate
        )
    ]

    # Because next_three is already ordered by distance,
    # index 0 is the nearest meaningful obstacle.
    selected = (
        meaningful_next_three[0]
        if meaningful_next_three
        else (
            next_three[0]
            if next_three
            else None
        )
    )

    # Retain strongest-of-next-three separately for diagnostics
    # and later analytics. It must NOT control Point-A room.
    strongest_next_three = (
        max(
            next_three,
            key=lambda x: (
                float(
                    x.get(
                        "strength"
                    )
                    or 0.0
                ),
                float(
                    x.get(
                        "sr_score"
                    )
                    or 0.0
                ),
                float(
                    x.get(
                        "touches"
                    )
                    or 0.0
                ),
                1
                if str(
                    x.get(
                        "tf"
                    )
                    or ""
                ).upper().strip()
                == "H4"
                else 0,
                -float(
                    x.get(
                        "distance_atr"
                    )
                    or 0.0
                ),
            ),
        )
        if next_three
        else None
    )

    out.update({
        "available": bool(
            selected
        ),
        "candidate_count": len(
            next_three
        ),
        "selected": selected,
        "next_3_opposing_sr": (
            next_three
        ),
        "strongest_next_three": (
            strongest_next_three
        ),
        "selection_model": (
            "NEAREST_MEANINGFUL_OPPOSING_SR"
        ),
    })
    return out

# ---------------------------------------------------------
# Point-A DXY M15 entry-health threshold.
#
# The DXY tracker remains the authority for directional
# confirmation/revocation.
#
# This threshold does NOT revoke or flip DXY direction.
# It only prevents a NEW trade from entering while an
# otherwise-confirmed DXY direction is materially
# deteriorating.
#
# Existing tracker evidence:
#   >= 0.50 ATR adverse move = 30 revoke points
#
# Historical Point-A replay:
#   healthy confirmed cases remained below 30;
#   Friday 14-08-2026 EURUSD BUY deterioration was 42.
# ---------------------------------------------------------
DXY_M15_ENTRY_REVOKE_WAIT_THRESHOLD = 30

# ---------------------------------------------------------
# Point-A DXY mature-move / opposing-SR protection.
#
# A confirmed DXY direction can still be correct while being
# a poor place for a NEW correlated trade if:
#
#   1. DXY has already travelled materially in that direction
#   2. a meaningful opposing SR is now close
#
# This is NOT directional revocation.
# It temporarily routes new entries to WAIT.
#
# 1.50 ATR:
#   requires a clearly developed directional move.
#
# 0.75 ATR:
#   same practical "good room" boundary already used by
#   Point-A symbol-room classification.
# ---------------------------------------------------------
DXY_M15_MATURE_MOVE_ATR = 1.50
DXY_M15_MATURE_SR_WAIT_ATR = 0.75

# ---------------------------------------------------------
# POINT-A DXY H1 MARKET-FLOW AUTHORITY
#
# H1 uses broad evidence, not the strict M15-style
# candidate lifecycle.
#
# Production audit over 380 REAL_DXY H1 bars:
#   winning evidence score >= 40
#   directional margin      >= 20
#
# Anything weaker is treated as H1 NEUTRAL / developing.
# ---------------------------------------------------------
DXY_H1_FLOW_MIN_SCORE = 40
DXY_H1_FLOW_MIN_MARGIN = 20

# H1 entry snapshot freshness already uses a 2-hour window.
DXY_H1_FLOW_FRESH_MS = 2 * 60 * 60 * 1000

DXY_M15_MATURE_MOVE_ATR = 1.50

# Strong recent displacement itself can identify an already
# extended move even if lifecycle excursion history is missing
# or has just been reset.
DXY_M15_EXTENDED_RECENT_MOVE_ATR = 1.80

# Once the DXY move is mature/extended, require materially
# more room before allowing a fresh correlated entry.
DXY_M15_MATURE_SR_WAIT_ATR = 0.75


def _dxy_sr_confirmation_analytics(
    *,
    R,
    device_id: str | None,
    symbol: str,
    side: str,
    entry_ms: int,
    profile_id: str | None = None,
) -> dict:
    """
    Read the existing unified DXY M15 entry snapshot.

    Analytics only:
    - no trade blocking
    - no trade timing change
    - no score
    """
    sym_u = str(symbol or "").upper().strip()
    side_u = str(side or "").upper().strip()
    
    

    # ---------------------------------------------------------
    # POINT-A LIVE DXY SOURCE
    #
    # Live strategy must consume canonical REAL_DXY only.
    # Do NOT use the per-trade analytics snapshot here and
    # do NOT fall back to SYNTHETIC_DXY.
    # ---------------------------------------------------------
    try:
        from api.dxy_m15_tracker import read_dxy_m15_state

        

        raw_canonical = R.get("xtl:dxy:canonical")
        canonical = {}

        if raw_canonical:
            if isinstance(raw_canonical, (bytes, bytearray)):
                raw_canonical = raw_canonical.decode(
                    "utf-8",
                    "replace",
                )

            canonical = json.loads(raw_canonical)

            if not isinstance(canonical, dict):
                canonical = {}

        canonical_source = str(
            canonical.get("source") or ""
        ).upper().strip()

        real_dev = str(
            canonical.get("device_id")
            or canonical.get("real_device_id")
            or ""
        ).strip()

        if canonical_source != "REAL_DXY" or not real_dev:
            return {
                "schema_version": 1,
                "analytics_only": False,
                "status": "UNAVAILABLE",
                "reason_codes": [
                    "DXY_CANONICAL_REAL_DEVICE_MISSING",
                ],
                "snapshot": {
                    "selected_source": None,
                    "selected_device_id": None,
                    "canonical": canonical,
                },
            }

        selected = read_dxy_m15_state(
            R,
            "REAL_DXY",
            real_dev,
        )

        if not isinstance(selected, dict) or not selected:
            return {
                "schema_version": 1,
                "analytics_only": False,
                "status": "UNAVAILABLE",
                "reason_codes": [
                    "DXY_REAL_M15_STATE_MISSING",
                ],
                "snapshot": {
                    "selected_source": "REAL_DXY",
                    "selected_device_id": real_dev,
                },
            }

        # Normalize into the small structure expected by the
        # existing code below this block.
        dxy = {
            "selected_source": "REAL_DXY",
            "selected_device_id": real_dev,
            "fallback_used": False,
            "fallback_reason": None,
            "selected": selected,
        }

    except Exception as exc:
        return {
            "schema_version": 1,
            "analytics_only": False,
            "status": "UNAVAILABLE",
            "reason_codes": [
                "DXY_LIVE_REAL_READ_FAILED",
            ],
            "snapshot": {
                "error": (
                    f"{type(exc).__name__}:"
                    f"{exc}"
                ),
            },
         }

    # ---------------------------------------------------------
    # POINT-A CANONICAL REAL_DXY H1 MARKET FLOW
    #
    # H1 direction is evidence-based:
    #
    #   winner >= 40 AND margin >= 20
    #
    # We intentionally do NOT require H1 candidate_direction
    # or candidate_confidence. Those fields identify unusually
    # strong impulse/reversal events and were NEUTRAL on ~87%
    # of stored H1 bars.
    #
    # No synthetic fallback here. Point-A must use the same
    # canonical REAL_DXY device as M15.
    # ---------------------------------------------------------
    h1_feature = {}

    try:
        h1_key = (
            "xtl:dxy:features:latest:H1:"
            f"REAL_DXY:{real_dev}"
        )

        raw_h1 = R.get(h1_key)

        if isinstance(raw_h1, (bytes, bytearray)):
            raw_h1 = raw_h1.decode(
                "utf-8",
                "replace",
            )

        if raw_h1:
            _h1_obj = json.loads(raw_h1)

            if isinstance(_h1_obj, dict):
                h1_feature = _h1_obj

    except Exception:
        h1_feature = {}

    h1_close_ms = int(
        h1_feature.get("bar_close_ms")
        or h1_feature.get("broker_bar_close_ms")
        or 0
    )

    h1_age_ms = (
        max(
            0,
            int(entry_ms) - int(h1_close_ms),
        )
        if h1_close_ms > 0
        else None
    )

    h1_fresh = bool(
        h1_close_ms > 0
        and h1_close_ms <= int(entry_ms)
        and h1_age_ms is not None
        and h1_age_ms <= DXY_H1_FLOW_FRESH_MS
    )

    h1_bull_score = int(
        h1_feature.get("bull_evidence_score")
        or 0
    )

    h1_bear_score = int(
        h1_feature.get("bear_evidence_score")
        or 0
    )

    h1_evidence_margin = abs(
        h1_bull_score - h1_bear_score
    )

    if not h1_fresh:
        h1_direction = "NEUTRAL"
        h1_direction_reason = (
            "H1_UNAVAILABLE_OR_STALE"
        )

    elif (
        h1_bull_score >= DXY_H1_FLOW_MIN_SCORE
        and (
            h1_bull_score - h1_bear_score
        ) >= DXY_H1_FLOW_MIN_MARGIN
    ):
        h1_direction = "BULLISH"
        h1_direction_reason = (
            "H1_BULL_EVIDENCE"
        )

    elif (
        h1_bear_score >= DXY_H1_FLOW_MIN_SCORE
        and (
            h1_bear_score - h1_bull_score
        ) >= DXY_H1_FLOW_MIN_MARGIN
    ):
        h1_direction = "BEARISH"
        h1_direction_reason = (
            "H1_BEAR_EVIDENCE"
        )

    else:
        h1_direction = "NEUTRAL"
        h1_direction_reason = (
            "H1_EVIDENCE_INSUFFICIENT"
        )
    if not isinstance(dxy, dict):
        return {
            "schema_version": 1,
            "analytics_only": True,
            "status": "UNAVAILABLE",
            "reason_codes": [
                "DXY_ENTRY_SNAPSHOT_INVALID",
            ],
            "snapshot": {},
        }

    selected = (
        dxy.get("selected")
        if isinstance(
            dxy.get("selected"),
            dict,
        )
        else {}
    )

    features = (
        selected.get("features")
        if isinstance(
            selected.get("features"),
            dict,
        )
        else {}
    )

    # LIVE REAL_DXY tracker schema.
    #
    # Current tracker structure is:
    #
    #   selected["features"]["market_flow"]["structure"]
    #
    # Do NOT use features["sr"]; that belongs to the old analytics
    # snapshot schema.
    market_flow = (
        features.get("market_flow")
        if isinstance(
            features.get("market_flow"),
            dict,
        )
        else {}
    )

    structure = (
        market_flow.get("structure")
        if isinstance(
            market_flow.get("structure"),
            dict,
        )
        else {}
    )

    # _dxy_opposing_sr_next_three() expects the SR container.
    # The live structure already contains active_supports,
    # active_resistances, support_path, resistance_path, etc.
    sr = structure

    reasoning = {}

    direction = str(
        selected.get("direction")
        or "NEUTRAL"
    ).upper().strip()

    lifecycle_status = str(
        selected.get("status")
        or "UNAVAILABLE"
    ).upper().strip()

    # ---------------------------------------------------------
    # REAL_DXY observation availability
    #
    # Availability means:
    #   "Do we have a usable tracker observation?"
    #
    # It must NOT depend on whether the current directional
    # opinion is BULLISH/BEARISH.
    #
    # NEUTRAL is a valid observed state and is handled later as
    # DEVELOPING / WAIT by the H1+M15 direction contract.
    #
    # COMPLETED / WEAK_COMPLETION are also valid observations.
    #
    # Freshness remains a separate check below.
    # ---------------------------------------------------------
    available = bool(
        selected
        and lifecycle_status not in (
            "",
            "UNAVAILABLE",
        )
    )

    # Freshness is based on the latest evaluated REAL_DXY M15 bar.
    last_bar_ms = _to_ms_any(
        selected.get("last_evaluated_bar_close_ms")
        or selected.get("broker_bar_close_ms")
        or features.get("broker_bar_close_ms")
        or features.get("bar_close_ms")
    )

    now_ms = int(time.time() * 1000)

    # Allow 2 completed M15 intervals.
    fresh = bool(
        last_bar_ms
        and now_ms >= last_bar_ms
        and (now_ms - last_bar_ms) <= 30 * 60 * 1000
    )

    # Current feature qualification describes whether the latest M15
    # feature is itself a new qualifying directional candidate.
    # It must NOT invalidate an already CONFIRMED tracker direction.
    latest_feature_qualified = bool(
        selected.get("latest_feature_qualified")
        or features.get("candidate_qualified")
    )

    
    alignment = str(
        selected.get("trade_alignment")
        or "NEUTRAL"
    ).upper().strip()

    analysis_direction = str(
        reasoning.get("analysis_direction")
        or ""
    ).upper().strip()

    room_class = str(
        reasoning.get("room_class")
        or ""
    ).upper().strip()

    reversal_risk = str(
        reasoning.get("reversal_risk")
        or ""
    ).upper().strip()

    structure_conflict = bool(
        reasoning.get("structure_conflict")
    )

    candidate_qualified = bool(
        lifecycle_status == "CONFIRMED"
        or (
            lifecycle_status == "PENDING"
            and latest_feature_qualified
            
        )
    )
    # ---------------------------------------------------------
    # Point-A M15 entry-health state.
    #
    # Important:
    # revoke_score is evidence AGAINST the currently tracked
    # DXY direction. It is NOT an independent direction signal.
    #
    # Therefore:
    #   CONFIRMED + score < 30
    #       -> confirmed direction remains executable
    #
    #   CONFIRMED + score >= 30
    #       -> direction remains confirmed, but NEW entries WAIT
    #
    # Actual tracker revocation / opposite confirmation remains
    # authoritative for directional invalidation.
    # ---------------------------------------------------------
    try:
        m15_revoke_score = int(
            selected.get("revoke_score") or 0
        )
    except Exception:
        m15_revoke_score = 0

    m15_revoke_reasons = (
        list(selected.get("revoke_reasons") or [])
        if isinstance(
            selected.get("revoke_reasons"),
            list,
        )
        else []
    )

    m15_revoke_wait_threshold = int(
        DXY_M15_ENTRY_REVOKE_WAIT_THRESHOLD
    )

    # ---------------------------------------------------------
    # DXY directional maturity inputs.
    #
    # Final maturity classification is intentionally delayed
    # until required_dxy_direction / favorable_direction are
    # known below.
    # ---------------------------------------------------------
    dxy_directional_move_atr = (
        _dxy_float_or_none(
            selected.get(
                "directional_move_atr"
            )
        )
        or 0.0
    )

    dxy_max_favorable_atr = (
        _dxy_float_or_none(
            selected.get(
                "max_favorable_atr"
            )
        )
        or 0.0
    )

    dxy_recent_net_atr = (
        _dxy_float_or_none(
            features.get(
                "recent_net_atr"
            )
        )
        or 0.0
    )

    inside_support = (
        structure.get("inside_support")
        if isinstance(
            structure.get("inside_support"),
            dict,
        )
        else {}
    )

    inside_resistance = (
        structure.get("inside_resistance")
        if isinstance(
            structure.get("inside_resistance"),
            dict,
        )
        else {}
    )

    nearest_support = (
        structure.get("nearest_support")
        if isinstance(
            structure.get("nearest_support"),
            dict,
        )
        else {}
    )

    nearest_resistance = (
        structure.get("nearest_resistance")
        if isinstance(
            structure.get("nearest_resistance"),
            dict,
        )
        else {}
    )

    near_h1_support = bool(
        nearest_support
        and str(
            nearest_support.get("tf") or ""
        ).upper() == "H1"
        and (
            bool(nearest_support.get("inside"))
            or (
                _dxy_float_or_none(
                    nearest_support.get("distance_atr")
                ) is not None
                and _dxy_float_or_none(
                    nearest_support.get("distance_atr")
                ) <= 0.5
            )
        )
    )

    near_h1_resistance = bool(
        nearest_resistance
        and str(
            nearest_resistance.get("tf") or ""
        ).upper() == "H1"
        and (
            bool(nearest_resistance.get("inside"))
            or (
                _dxy_float_or_none(
                    nearest_resistance.get("distance_atr")
                ) is not None
                and _dxy_float_or_none(
                    nearest_resistance.get("distance_atr")
                ) <= 0.5
            )
        )
    )

    inside_h4_support = bool(
        inside_support
        and str(
            inside_support.get("tf") or ""
        ).upper() == "H4"
    )

    inside_h4_resistance = bool(
        inside_resistance
        and str(
            inside_resistance.get("tf") or ""
        ).upper() == "H4"
    )

    required_dxy_direction = None

    # USD relationship for the six current XTL symbols.
    if sym_u in (
        "EURUSD",
        "GBPUSD",
        "XAUUSD",
    ):
        required_dxy_direction = (
            "BEARISH"
            if side_u == "BUY"
            else "BULLISH"
        )

    elif sym_u in (
        "USDJPY",
        "USDCHF",
        "USDCAD",
    ):
        required_dxy_direction = (
            "BULLISH"
            if side_u == "BUY"
            else "BEARISH"
        )

    # ---------------------------------------------------------
    # CANONICAL TREND_SR DIRECTIONAL-ROOM SAFETY
    # ---------------------------------------------------------
    canonical_structure_pressure = str(
        structure.get("structure_pressure") or ""
    ).upper().strip()

    canonical_structure_reason = str(
        structure.get("structure_pressure_reason") or ""
    ).upper().strip()

    canonical_directional_room_atr = _dxy_float_or_none(
        structure.get("directional_room_atr")
    )

    canonical_inside_opposing = bool(
        (
            required_dxy_direction == "BULLISH"
            and canonical_structure_reason == "INSIDE_RESISTANCE"
        )
        or (
            required_dxy_direction == "BEARISH"
            and canonical_structure_reason == "INSIDE_SUPPORT"
        )
    )

    canonical_extreme_pressure = bool(
        canonical_structure_pressure == "EXTREME"
    )

    canonical_very_low_room = bool(
        canonical_directional_room_atr is not None
        and canonical_directional_room_atr <= 0.20
    )

    canonical_sr_risk = bool(
        canonical_inside_opposing
        or canonical_extreme_pressure
        or canonical_very_low_room
    )

    # Use the same Point-A SR philosophy on DXY: immediate next three
    # opposing levels in the direction DXY must move, de-duplicate them,
    # then choose by strength, sr_score, touches, H4, and distance.

    dxy_sr_current_price = _dxy_float_or_none(
        structure.get("price")
        if structure.get("price") is not None
        else selected.get("candidate_start_price")
    )

   
    opposing_sr = _dxy_opposing_sr_next_three(
        sr,
        required_direction=required_dxy_direction,
        current_price=dxy_sr_current_price,
        atr=_dxy_float_or_none(
            structure.get("atr")
            if structure.get("atr") is not None
            else features.get("atr")
        ),
    )
    selected_opposing_sr = (
        opposing_sr.get("selected")
        if isinstance(opposing_sr.get("selected"), dict)
        else {}
    )
    selected_room_class = str(
        selected_opposing_sr.get("room_class") or room_class or ""
    ).upper().strip()

    # ---------------------------------------------------------
    # POINT-A M15 + H1 DIRECTION CONTRACT
    #
    # AGREE:
    #   M15 BULLISH + H1 BULLISH
    #   M15 BEARISH + H1 BEARISH
    #
    # CONFLICT:
    #   directional M15 and H1 disagree
    #
    # DEVELOPING:
    #   either timeframe is NEUTRAL / unavailable / stale
    #
    # Only AGREE creates a confirmed DXY direction.
    # ---------------------------------------------------------
    m15_direction = (
        direction
        if direction in ("BULLISH", "BEARISH")
        else "NEUTRAL"
    )

    if (
        m15_direction in ("BULLISH", "BEARISH")
        and h1_direction in ("BULLISH", "BEARISH")
        and m15_direction == h1_direction
    ):
        dxy_m15_h1_state = "AGREE"
        combined_dxy_direction = m15_direction

    elif (
        m15_direction in ("BULLISH", "BEARISH")
        and h1_direction in ("BULLISH", "BEARISH")
        and m15_direction != h1_direction
    ):
        dxy_m15_h1_state = "CONFLICT"
        combined_dxy_direction = "NEUTRAL"

    else:
        dxy_m15_h1_state = "DEVELOPING"
        combined_dxy_direction = "NEUTRAL"

    favorable_direction = bool(
        dxy_m15_h1_state == "AGREE"
        and required_dxy_direction
        and combined_dxy_direction
        == required_dxy_direction
    )
    # ---------------------------------------------------------
    # Direction-aware DXY move maturity.
    #
    # A large move matters only if it occurred in the same
    # direction required by the proposed XTL trade.
    #
    # Examples:
    #
    # required BEARISH:
    #     recent_net_atr <= -1.80
    #
    # required BULLISH:
    #     recent_net_atr >= +1.80
    #
    # max_favorable_atr remains the preferred lifecycle metric
    # when available.
    # ---------------------------------------------------------
    dxy_recent_move_extended = bool(
        (
            required_dxy_direction == "BEARISH"
            and dxy_recent_net_atr
            <= -DXY_M15_EXTENDED_RECENT_MOVE_ATR
        )
        or (
            required_dxy_direction == "BULLISH"
            and dxy_recent_net_atr
            >= DXY_M15_EXTENDED_RECENT_MOVE_ATR
        )
    )

    dxy_mature_move = bool(
        lifecycle_status == "CONFIRMED"
        and favorable_direction
        and (
            dxy_max_favorable_atr
            >= DXY_M15_MATURE_MOVE_ATR
            or dxy_recent_move_extended
        )
    )

    # Hard opposite is valid ONLY when H1 and M15 agree.
    #
    # M15 opposite + H1 conflicting/neutral must WAIT,
    # never terminally BLOCK the setup.
    opposite_direction = bool(
        dxy_m15_h1_state == "AGREE"
        and required_dxy_direction
        and combined_dxy_direction
        in ("BULLISH", "BEARISH")
        and combined_dxy_direction
        != required_dxy_direction
    )
    # ---------------------------------------------------------
    # Entry-facing M15 classification.
    #
    # Symmetric for all XTL symbols:
    # it applies whenever the currently CONFIRMED DXY direction
    # is the direction required by the proposed trade.
    # ---------------------------------------------------------
    confirmed_required_direction = bool(
        lifecycle_status == "CONFIRMED"
        and favorable_direction
    )

    m15_deteriorating = bool(
        confirmed_required_direction
        and m15_revoke_score
        >= m15_revoke_wait_threshold
    )

    if m15_deteriorating:
        m15_entry_state = "DETERIORATING"

    elif confirmed_required_direction:
        m15_entry_state = "ALIGNED"

    elif (
        lifecycle_status == "CONFIRMED"
        and opposite_direction
    ):
        m15_entry_state = "OPPOSITE_CONFIRMED"

    elif lifecycle_status == "PENDING":
        m15_entry_state = "PENDING"

    else:
        m15_entry_state = "NEUTRAL"

    selected_sr_distance_atr = _dxy_float_or_none(
        selected_opposing_sr.get("distance_atr")
    )

    selected_sr_strength = (
        _dxy_float_or_none(
            selected_opposing_sr.get("strength")
        )
        or 0.0
    )

    selected_sr_score = (
        _dxy_float_or_none(
            selected_opposing_sr.get("sr_score")
        )
        or 0.0
    )

    selected_sr_touches = (
        _dxy_float_or_none(
            selected_opposing_sr.get("touches")
        )
        or 0.0
    )

    selected_sr_tf = str(
        selected_opposing_sr.get("tf") or ""
    ).upper().strip()

    # Selected strongest SR among the immediate next 3
    # must be both meaningful and genuinely too close
    # before DXY structure blocks the direction.
    selected_sr_strong = bool(
        selected_sr_strength >= 8.0
        or selected_sr_score >= 10.0
        or selected_sr_touches >= 3.0
        or selected_sr_tf == "H4"
    )

    selected_sr_too_close = bool(
        selected_sr_distance_atr is not None
        and selected_sr_distance_atr <= 0.35
    )

    sr_risk = bool(
        (
            selected_opposing_sr
            and selected_sr_strong
            and selected_sr_too_close
        )
        or canonical_sr_risk
    )
    # ---------------------------------------------------------
    # Mature directional move approaching opposing SR.
    #
    # Example:
    #
    #   DXY CONFIRMED BEARISH
    #   max favorable move already >= 1.50 ATR
    #   nearest meaningful SUPPORT <= 0.75 ATR
    #
    # The bearish thesis may still be correct, but initiating a
    # new XAUUSD BUY / EURUSD BUY / GBPUSD BUY / USDCAD SELL
    # here is late-cycle continuation risk.
    #
    # Symmetric logic applies to mature bullish DXY approaching
    # meaningful resistance.
    # ---------------------------------------------------------
    dxy_mature_sr_risk = bool(
        confirmed_required_direction
        and dxy_mature_move
        and selected_opposing_sr
        and selected_sr_strong
        and selected_sr_distance_atr is not None
        and selected_sr_distance_atr
        <= DXY_M15_MATURE_SR_WAIT_ATR
    )

    reason_codes = []
    if canonical_extreme_pressure:
        reason_codes.append(
            "DXY_CANONICAL_STRUCTURE_EXTREME"
        )

    if canonical_very_low_room:
        reason_codes.append(
            "DXY_CANONICAL_ROOM_LE_0_20_ATR"
        )

    if canonical_inside_opposing:
        reason_codes.append(
            "DXY_CANONICAL_INSIDE_OPPOSING_SR"
        )
    # ---------------------------------------------------------
    # H1 / M15 direction diagnostics
    # ---------------------------------------------------------
    reason_codes.append(
        f"DXY_H1_DIRECTION_{h1_direction}"
    )

    reason_codes.append(
        f"DXY_H1_REASON_{h1_direction_reason}"
    )

    reason_codes.append(
        f"DXY_M15_H1_{dxy_m15_h1_state}"
    )

    if dxy_m15_h1_state == "CONFLICT":
        reason_codes.append(
            "DXY_DIRECTION_TIMEFRAME_CONFLICT"
        )

    elif dxy_m15_h1_state == "DEVELOPING":
        reason_codes.append(
            "DXY_DIRECTION_TIMEFRAME_DEVELOPING"
        )

    if not available:
        reason_codes.append(
            "DXY_UNAVAILABLE"
        )

    if available and not fresh:
        reason_codes.append(
            "DXY_STALE"
        )

    if lifecycle_status in (
        "CONFIRMED",
        "COMPLETED",
        "WEAK_COMPLETION",
    ):
        reason_codes.append(
            f"DXY_{lifecycle_status}"
        )
    else:
        reason_codes.append(
            f"DXY_STATUS_{lifecycle_status}"
        )

    if favorable_direction:
        reason_codes.append(
            "DXY_DIRECTION_FAVORS_TRADE"
        )

    if opposite_direction:
        reason_codes.append(
            "DXY_DIRECTION_OPPOSES_TRADE"
        )

    if alignment == "ALIGNED":
        reason_codes.append(
            "DXY_ALIGNMENT_FAVORS_TRADE"
        )
    elif alignment == "AGAINST":
        reason_codes.append(
            "DXY_ALIGNMENT_OPPOSES_TRADE"
        )
    else:
        reason_codes.append(
            "DXY_ALIGNMENT_NEUTRAL"
        )

    if candidate_qualified:
        reason_codes.append(
            "DXY_CANDIDATE_QUALIFIED"
        )
    else:
        reason_codes.append(
            "DXY_CANDIDATE_NOT_QUALIFIED"
        )
    if m15_deteriorating:
        reason_codes.append(
            "DXY_M15_DETERIORATING"
        )
    
    if dxy_recent_move_extended:
        reason_codes.append(
            "DXY_M15_EXTENDED_RECENT_MOVE"
        )
    if dxy_mature_move:
        reason_codes.append(
            "DXY_M15_MATURE_MOVE"
        )

    if dxy_mature_sr_risk:
        reason_codes.append(
            "DXY_M15_MATURE_MOVE_NEAR_OPPOSING_SR"
        )

    reason_codes.append(
        f"DXY_M15_ENTRY_STATE_{m15_entry_state}"
    )

    if sr_risk:
        reason_codes.append(
            "DXY_DIRECTIONAL_SR_RISK"
        )

    if structure_conflict:
        reason_codes.append(
            "DXY_STRUCTURE_CONFLICT"
        )

    if room_class:
        reason_codes.append(
            f"DXY_ROOM_{room_class}"
        )

    # Analytics opinion only.
    #
    # PASS:
    # qualified/final DXY direction supports trade
    # and DXY is not moving directly into nearby SR.
    #
    # FAIL:
    # qualified/final DXY direction opposes trade,
    # or DXY's own SR structure strongly conflicts.
    #
    # NEUTRAL:
    # no qualified directional opinion.
    qualified_lifecycle = (
        lifecycle_status
        in (
            "CONFIRMED",
            "COMPLETED",
            "WEAK_COMPLETION",
        )
        
    )

    if not available or not fresh:
        status = "UNAVAILABLE"

    elif (
        qualified_lifecycle
        and favorable_direction
        and not m15_deteriorating
        and not sr_risk
        and not dxy_mature_sr_risk
        and not structure_conflict
    ):
        status = "PASS"

    elif (
        qualified_lifecycle
        and favorable_direction
        and m15_deteriorating
    ):
        # Direction itself remains valid.
        # Entry timing is temporarily unhealthy.
        #
        # NEUTRAL deliberately routes Point-A to WAIT,
        # never to terminal BLOCK.
        status = "NEUTRAL"

    elif (
        qualified_lifecycle
        and favorable_direction
        and dxy_mature_sr_risk
    ):
        # DXY still supports the trade direction, but the
        # move is already mature/extended and is approaching
        # the nearest meaningful opposing SR.
        #
        # This is late-continuation timing risk, not a
        # directional invalidation, therefore Point-A WAITs
        # instead of terminally blocking the setup.
        status = "NEUTRAL"

    elif dxy_m15_h1_state != "AGREE":
        # H1/M15 conflict or one timeframe is neutral.
        #
        # This is incomplete directional confirmation,
        # not terminal invalidation.
        #
        # Point-A must WAIT/HOLD.
        status = "NEUTRAL"

    elif (
        qualified_lifecycle
        and opposite_direction
    ):
        status = "FAIL"

    elif (
        sr_risk
        or structure_conflict
    ):
        status = "FAIL"


    else:
        status = "NEUTRAL"

    return {
        "schema_version": 1,
        "analytics_only": True,
        "status": status,
        "reason_codes": reason_codes,
        "snapshot": {
            "selected_source": dxy.get(
                "selected_source"
            ),
            "selected_device_id": dxy.get(
                "selected_device_id"
            ),
            "fallback_used": bool(
                dxy.get("fallback_used")
            ),
            "fallback_reason": dxy.get(
                "fallback_reason"
            ),
            "available": available,
            "fresh": fresh,
            "lifecycle_status": (
                lifecycle_status
            ),
            "direction": direction,
            "m15_direction": (
                m15_direction
            ),
            "h1_direction": (
                h1_direction
            ),
            "h1_direction_reason": (
                h1_direction_reason
            ),
            "h1_fresh": bool(
                h1_fresh
            ),
            "h1_close_ms": (
                int(h1_close_ms)
                if h1_close_ms > 0
                else None
            ),
            "h1_age_ms": (
                int(h1_age_ms)
                if h1_age_ms is not None
                else None
            ),
            "h1_bull_score": int(
                h1_bull_score
            ),
            "h1_bear_score": int(
                h1_bear_score
            ),
            "h1_evidence_margin": int(
                h1_evidence_margin
            ),
            "h1_raw_candidate_direction": (
                str(
                    h1_feature.get(
                        "candidate_direction"
                    )
                    or "NEUTRAL"
                ).upper()
            ),
            "h1_candidate_confidence": int(
                h1_feature.get(
                    "candidate_confidence"
                )
                or 0
            ),
            "m15_h1_state": (
                dxy_m15_h1_state
            ),
            "combined_dxy_direction": (
                combined_dxy_direction
            ),
            "required_direction_for_trade": (
                required_dxy_direction
            ),
            "trade_alignment": alignment,
            "candidate_qualified": (
                candidate_qualified
            ),
            "m15_entry_state": (
                m15_entry_state
            ),
            "m15_deteriorating": bool(
                m15_deteriorating
            ),
            "m15_revoke_score": int(
                m15_revoke_score
            ),
            "m15_revoke_reasons": (
                list(m15_revoke_reasons)
            ),
            "m15_revoke_wait_threshold": int(
                m15_revoke_wait_threshold
            ),
            "dxy_directional_move_atr": (
                float(dxy_directional_move_atr)
            ),
            "dxy_max_favorable_atr": (
                float(dxy_max_favorable_atr)
            ),
            "dxy_recent_net_atr": (
                float(dxy_recent_net_atr)
            ),
            "dxy_recent_move_extended": bool(
                dxy_recent_move_extended
            ),
            "dxy_mature_move": bool(
                dxy_mature_move
            ),
            "dxy_mature_sr_risk": bool(
                dxy_mature_sr_risk
            ),
            "dxy_mature_move_atr_threshold": float(
                DXY_M15_MATURE_MOVE_ATR
            ),
            "dxy_extended_recent_move_atr_threshold": float(
                DXY_M15_EXTENDED_RECENT_MOVE_ATR
            ),
            "dxy_mature_sr_wait_atr_threshold": float(
                DXY_M15_MATURE_SR_WAIT_ATR
            ),
            "analysis_direction": (
                analysis_direction or None
            ),
            "room_class": (
                selected_room_class or None
            ),
            "reversal_risk": (
                reversal_risk or None
            ),
            "canonical_structure_pressure": (
                canonical_structure_pressure or None
            ),
            "canonical_structure_pressure_reason": (
                canonical_structure_reason or None
            ),
            "canonical_directional_room_atr": (
                canonical_directional_room_atr
            ),
            "canonical_inside_opposing": (
                canonical_inside_opposing
            ),
            "canonical_sr_risk": (
                canonical_sr_risk
            ),
            "structure_conflict": (
                structure_conflict
            ),
            "near_h1_support": (
                near_h1_support
            ),
            "near_h1_resistance": (
                near_h1_resistance
            ),
            "inside_h4_support": (
                inside_h4_support
            ),
            "inside_h4_resistance": (
                inside_h4_resistance
            ),
            "opposing_sr_selection": (
                _safe_deepcopy_json(opposing_sr)
            ),
            "dxy_opposing_sr_distance_atr": (
                selected_sr_distance_atr
            ),
            "dxy_opposing_sr_strength": (
                selected_sr_strength
            ),
            "dxy_opposing_sr_score": (
                selected_sr_score
            ),
            "dxy_opposing_sr_touches": (
                selected_sr_touches
            ),
            "dxy_opposing_sr_tf": (
                selected_sr_tf or None
            ),
            "available_downside_atr": (
                structure.get("room_down_atr")
            ),
            "available_upside_atr": (
                structure.get("room_up_atr")
            ),
            "reasoning": _safe_deepcopy_json(
                reasoning
            ),
            "selected_state": (
                _safe_deepcopy_json(
                    selected
                )
            ),
        },
    }


def _build_entry_validation_analytics(
    *,
    R,
    watch: dict,
    zone: dict,
    direction: str,
    symbol: str,
    entry_price: float | None,
    atr: float | None,
    device_id: str | None,
    profile_id: str | None,
    confirmed_at_ms: int,
) -> dict:
    """
    Freeze the three analytics opinions together:

    1. first-touch market prediction
    2. zone quality + local SR
    3. DXY direction + DXY SR

    This remains shadow-only.
    """
    w = watch if isinstance(watch, dict) else {}

    setup = (
        w.get("setup_analysis")
        if isinstance(
            w.get("setup_analysis"),
            dict,
        )
        else {}
    )

    predicted_behavior = str(
        setup.get(
            "predicted_market_behavior"
        )
        or "UNCLASSIFIED"
    ).upper().strip()

    predicted_direction = str(
        setup.get("predicted_direction")
        or ""
    ).upper().strip()

    production_direction = str(
        direction or ""
    ).upper().strip()

    if (
        predicted_direction
        in ("BUY", "SELL")
        and production_direction
        in ("BUY", "SELL")
    ):
        prediction_relationship = (
            "AGREES"
            if predicted_direction
            == production_direction
            else "CONFLICTS"
        )
    else:
        prediction_relationship = (
            "UNCLASSIFIED"
        )

    zone_confirmation = (
        _zone_quality_sr_analytics(
            zone,
            production_direction,
            entry_price,
            atr,
        )
    )

    dxy_confirmation = (
        _dxy_sr_confirmation_analytics(
            R=R,
            device_id=device_id,
            symbol=symbol,
            side=production_direction,
            entry_ms=int(
                confirmed_at_ms or 0
            ),
            profile_id=profile_id,
        )
    )

    return {
        "schema_version": 1,
        "analytics_only": True,
        "immutable_entry_validation": True,
        "created_at_ms": int(
            time.time() * 1000
        ),
        "validation_stage": (
            "REVERSAL_CONFIRMED"
        ),
        "production_unchanged": True,
        "prediction": {
            "predicted_market_behavior": (
                predicted_behavior
            ),
            "predicted_direction": (
                predicted_direction or None
            ),
            "production_direction": (
                production_direction or None
            ),
            "relationship_to_production": (
                prediction_relationship
            ),
            "reason_codes": (
                _safe_deepcopy_json(
                    setup.get("reason_codes")
                )
                or []
            ),
        },
        "zone_quality_sr": (
            zone_confirmation
        ),
        "dxy_sr": dxy_confirmation,
        "research_hypothesis": {
            "all_three_support_reversal": bool(
                prediction_relationship
                == "AGREES"
                and zone_confirmation.get(
                    "status"
                )
                == "PASS"
                and dxy_confirmation.get(
                    "status"
                )
                == "PASS"
            ),
            "prediction_conflict": bool(
                prediction_relationship
                == "CONFLICTS"
            ),
            "shadow_only": True,
        },
    }

def _find_xtl_broker_position(
    R,
    device_id: str,
    symbol: str,
    account_type: str = "demo",
    ticket: int | None = None,
) -> dict | None:
    """
    Read one explicitly selected MT5 device snapshot.

    Never scan all connected MT5 devices.
    """
    dev = str(device_id or "").strip()
    sym = str(symbol or "").upper().strip()
    acct = str(account_type or "demo").lower().strip()

    if not dev or not sym:
        return None

    try:
        raw = R.get(
            f"xtl:mt5:pos:{dev}:{acct}"
        )

        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode(
                "utf-8",
                "ignore",
            )

        positions = (
            json.loads(raw)
            if raw
            else []
        )

        if not isinstance(positions, list):
            return None

        wanted_ticket = int(ticket or 0)

        for bp in positions:
            if not isinstance(bp, dict):
                continue

            if (
                str(
                    bp.get("symbol") or ""
                ).upper().strip()
                != sym
            ):
                continue

            try:
                broker_ticket = int(
                    bp.get("ticket") or 0
                )
            except Exception:
                broker_ticket = 0

            if (
                wanted_ticket > 0
                and broker_ticket != wanted_ticket
            ):
                continue

            try:
                magic = int(
                    bp.get("magic") or 0
                )
            except Exception:
                magic = 0

            comment = str(
                bp.get("comment") or ""
            ).upper().strip()

            if (
                magic == 20251227
                or comment.startswith("XTL")
            ):
                return dict(bp)

    except Exception:
        log.exception(
            "[ZONE_GATE] BROKER_POSITION_READ_FAILED "
            "device=%s account=%s symbol=%s ticket=%s",
            dev,
            acct,
            sym,
            ticket,
        )

    return None

def _level_to_zone(lvl: float, tf: str, sym_u: str, atr: float | None) -> dict:
    # pip_factor: XAU/JPY wider than FX majors
    pip = 0.01 if sym_u == "XAUUSD" else (0.01 if sym_u.endswith("JPY") else 0.0001)

    # half-width: use ATR if available, else pip-based fallback
    # Keep it modest: 0.10 * ATR for FX/JPY tends to be reasonable; clamp to min.
    if atr is not None and atr > 0:
        half = max(3.0 * pip, 0.10 * float(atr))
    else:
        half = 5.0 * pip

    low = float(lvl) - float(half)
    high = float(lvl) + float(half)

    return {
        "level": float(lvl),
        "low": low,
        "high": high,
        "tf": str(tf or "H1").upper(),
        "type": "ZONE_FROM_LEVEL",
        "half": float(half),
    }

def _pick_last_closed_bar_from_bars(
    bars: List[dict],
    now_ms: int,
    tf_ms: int,
) -> Tuple[Optional[dict], Optional[dict]]:
    """
    Pick the last CLOSED bar.
    Priority: complete=True ,then clock fallback
    MT5 sends future bars complete=True  filter by open_ms <= system time.
    """
    import time as _t
    try:
        if not isinstance(bars, list) or len(bars) < 2:
            return (None, None)
        tf_ms = int(tf_ms or 0)
        if tf_ms <= 0:
            return (None, None)
        sys_now = int(now_ms or 0)
        if sys_now <= 0:
            sys_now = int(_t.time() * 1000)
        bs = [b for b in bars if isinstance(b, dict)]
        if len(bs) < 2:
            return (None, None)
        def _om(b):
            for k in ("t_open_ms","tOpenMs","open_time_ms","ts_ms","t","time","ts"):
                v = _to_ms_any(b.get(k))
                if v > 0: return int(v)
            return 0
        has_ts = any(_om(b) > 0 for b in bs[-5:])
        if not has_ts:
            return (bs[-2], bs[-3]) if len(bs) >= 3 else (None, None)
        bs_sorted = sorted(bs, key=lambda b: _om(b) or 0)
        for i in range(len(bs_sorted) - 1, -1, -1):
            b = bs_sorted[i]
            if b.get("complete") is False:
                continue
            om = _om(b)
            if om <= 0:
                continue 
            # MT5 marks FUTURE bars complete=True, so `complete` alone must not
            # qualify a bar as closed. The clock is authoritative: a bar is closed
            # only once its close time has actually passed.
            closed_by_clock = (om + tf_ms) <= sys_now
            if not closed_by_clock:
                continue  # future or still-forming — not closed
            prev = bs_sorted[i-1] if i-1 >= 0 else None
            return (b, prev)
        return (None, None)
    except Exception:
        return (None, None)

def _pick_level_from_lists(levels: List[Any], direction: str, cl: float) -> Optional[float]:
    vals: List[float] = []
    for x in (levels or []):
        try:
            # SR bundle levels are often dicts like {"level": 152.79, ...}
            if isinstance(x, dict):
                v = x.get("level")
            else:
                v = x
            if v is None:
                continue
            vals.append(float(v))
        except Exception:
            continue

    if not vals:
        return None

    if direction == "BUY":
        below = [v for v in vals if v <= cl]
        return max(below) if below else None
    else:
        above = [v for v in vals if v >= cl]
        return min(above) if above else None

def _pick_best_scored_zone(sr_all: dict, direction: str, cl: float) -> dict | None:
    if not isinstance(sr_all, dict):
        return None

    dir_u = str(direction or "").upper().strip()
    key = "best_support" if dir_u == "BUY" else "best_resistance"

    z = sr_all.get(key)
    if not isinstance(z, dict):
        return None

    try:
        lvl = float(z.get("level"))
        low = float(z.get("low"))
        high = float(z.get("high"))
        px = float(cl)
    except Exception:
        return None

    if lvl <= 0 or low >= high:
        return None

    if z.get("side_ok") is False or z.get("stale") is True:
        return None

    if dir_u == "BUY" and high > px:
        return None

    if dir_u == "SELL" and low < px:
        return None
    

    out = dict(z)
    out["zone_source"] = "BEST_SCORED_SR"
    out["selection_model"] = "BEST_SR_THEN_H1_H4_MAJOR_FALLBACK"
    out["zone_role"] = "BEST_SUPPORT" if dir_u == "BUY" else "BEST_RESISTANCE"
    return out

def _pick_zone_from_sr(sr_all: dict, direction: str, cl: float, atr: float, tf_tag: str) -> dict | None:
    """
    Strong SR zone picker with quality filtering.

    Accepts BOTH shapes:
      A) full payload: {"h1": {...}, "h4": {...}, ...}
      B) TF-sliced: {"supports":[...], "supports_near":[...], "supports_major":[...], ...}

    Priority system (caps are dynamic; FX uses wider caps):
    1. H4 major (strength>=8 OR touches>=4 OR sr_score>=10) within cap_h4_major ATR
    2. H1 strong (strength>=6 OR touches>=3 OR sr_score>=9) within cap_h1_strong ATR
    3. H1 acceptable (strength>=3 OR touches>=2 OR sr_score>=6) within cap_h1_acc ATR
    4. H1 any (touches>=1 if tight+sr_score>=4, else touches>=2) within cap_h1_min ATR

    Side rules:
    - Normal: BUY prefers supports BELOW price; SELL prefers resistances ABOVE price.
    - Reversal-watch use-case: if price has crossed the zone (BUY below support / SELL above resistance),
      allow the crossed zone as a reclaim target if the cross distance is within max_cross_atr.

    Returns zone with highest composite score, or None if no valid zone.
    """
    if not isinstance(sr_all, dict):
        return None

    dir_u = str(direction or "").upper().strip()
    if dir_u not in ("BUY", "SELL"):
        return None

    try:
        cl = float(cl)
    except Exception:
        return None

    try:
        atr = float(atr)
    except Exception:
        atr = 1.0

    if atr <= 0:
        atr = 1.0  # fallback

    tfu = str(tf_tag or "H1").upper().strip()
    tfk = tfu.lower()

    sym_u = str(sr_all.get("symbol") or "").upper().strip()
    is_fx = bool(sym_u) and (sym_u != "XAUUSD")

    cap_h4_major = 3.0
    cap_h1_strong = 2.5
    cap_h1_acc = 2.0
    cap_h1_min = 1.5

    # FX pairs: ATR is tiny; allow wider ATR distance to avoid false "no support" blocks
    if is_fx:
        cap_h4_major = 5.0
        cap_h1_strong = 4.0
        cap_h1_acc = 3.5
        cap_h1_min = 3.0

    # crossed-zone allowance (reversal-watch): tuneable
    max_cross_atr = 0.75

    # Strength thresholds
    def _is_strong_h4(z: dict) -> bool:
        return (
            int(z.get("strength") or 0) >= 8
            or int(z.get("touches") or 0) >= 4
            or float(z.get("sr_score") or 0) >= 10.0
        )

    def _is_strong_h1(z: dict) -> bool:
        return (
            int(z.get("strength") or 0) >= 6
            or int(z.get("touches") or 0) >= 3
            or float(z.get("sr_score") or 0) >= 9.0
        )

    def _is_acceptable_h1(z: dict) -> bool:
        return (
            int(z.get("strength") or 0) >= 3
            or int(z.get("touches") or 0) >= 2
            or float(z.get("sr_score") or 0) >= 6.0
        )

    def _is_minimum(z: dict) -> bool:
        touches = int(z.get("touches") or 0)
        if touches >= 2:
            return True
        if touches == 1:
            # Accept 1-touch only if zone is tight and has structural significance
            band_type = str(z.get("band_type") or "")
            sr_score = float(z.get("sr_score") or 0)
            return sr_score >= 4.0 and "wide" not in band_type
        return False

    def _composite_score(z: dict, dist_atr: float, tf: str) -> float:
        touches = int(z.get("touches") or 0)
        strength = int(z.get("strength") or 0)
        sr_score = float(z.get("sr_score") or 0)
        tf_bonus = 10.0 if tf == "H4" else 5.0
        return touches * 2.0 + strength * 1.5 + sr_score * 1.0 + tf_bonus - dist_atr * 2.0

    def _get_levels(sr_tf: dict, key: str) -> list:
        v = sr_tf.get(key)
        return v if isinstance(v, list) else []

    def _cross_ok(lvl: float) -> bool:
       """
       SIMPLE EXECUTION MODEL

       BUY:
          support must be BELOW or near current price

       SELL:
          resistance must be ABOVE or near current price

       No reclaim logic.
       """

       if dir_u == "BUY":
           return float(lvl) <= float(cl)

       return float(lvl) >= float(cl)

    # --- Accept TF-sliced SR directly ---
    is_tf_sliced = any(
        k in sr_all
        for k in (
            "supports",
            "resistances",
            "supports_near",
            "resistances_near",
            "supports_major",
            "resistances_major",
        )
    )

    if is_tf_sliced:
        h1 = sr_all
        h4 = {}
    else:
        # prefer tf_tag bucket if present; fallback to h1
        h1 = sr_all.get(tfk) if isinstance(sr_all.get(tfk), dict) else {}
        if not h1 and isinstance(sr_all.get("h1"), dict):
            h1 = sr_all.get("h1") or {}
        h4 = sr_all.get("h4") if isinstance(sr_all.get("h4"), dict) else {}

    # ------------------------------------------------------------
    # PHASE 1: Build BOTH major zones: H1 primary + H4 fallback
    # ------------------------------------------------------------
    # Final SR model:
    #   BUY  -> H1 Major Demand first, then H4 Major Demand
    #   SELL -> H1 Major Supply first, then H4 Major Supply
    # No H1 normal execution here. H1/H4 major zones are kept visible
    # so later phases can track: H1 missed -> continue watching H4.

    major_key = "supports_major" if dir_u == "BUY" else "resistances_major"

    def _ensure_zone_band(zone: dict, tf_for_band: str) -> dict:
        """Return a copied zone with guaranteed low/high band."""
        z = dict(zone or {})
        if not isinstance(z, dict) or z.get("level") is None:
            return z
        try:
            zl = z.get("low")
            zh = z.get("high")
            if zl is None or zh is None or float(zl) >= float(zh):
                ztmp = _level_to_zone(
                    float(z["level"]),
                    str(tf_for_band or z.get("tf") or tfu).upper(),
                    str(sr_all.get("symbol") or "").upper().strip() or "XAUUSD",
                    float(atr),
                )
                z["low"] = float(ztmp["low"])
                z["high"] = float(ztmp["high"])
                z["half"] = float(ztmp.get("half") or (abs(z["high"] - z["low"]) / 2.0))
        except Exception:
            pass
        return z

    def _pick_best_major(rows: list, tf_name: str) -> tuple[dict | None, dict]:
        """
        Pick one best major zone for a timeframe.
        Important: this intentionally does NOT reject a valid major zone just
        because it is far. Distance guard later decides WAIT_ZONE_TOUCH.
        """
        scored = []
        for z in rows or []:
            if not isinstance(z, dict):
                continue
            if z.get("side_ok") is False:
                continue
            if z.get("stale") is True:
                continue
            try:
                lvl = float(z.get("level"))
            except Exception:
                continue

            # Keep simple side rule only: BUY support at/below price,
            # SELL resistance at/above price. No reclaim/cross execution.
            if not _cross_ok(lvl):
                continue

            dist_atr = abs(float(cl) - float(lvl)) / float(atr)
            zz = _ensure_zone_band(z, tf_name)
            try:
                zl_pick = float(zz.get("low") if zz.get("low") is not None else zz.get("level"))
                zh_pick = float(zz.get("high") if zz.get("high") is not None else zz.get("level"))
                if zl_pick > zh_pick:
                    zl_pick, zh_pick = zh_pick, zl_pick

                if float(cl) < zl_pick:
                    band_dist = zl_pick - float(cl)
                elif float(cl) > zh_pick:
                    band_dist = float(cl) - zh_pick
                else:
                    band_dist = 0.0

                max_pick_dist = min(max(2.0 * float(atr), 3.0), 12.0) if str(sym_u).upper() == "XAUUSD" else 2.0 * float(atr)

                if band_dist > max_pick_dist:
                    continue
            except Exception:
                pass
            zz["tf"] = str(tf_name).upper()
            zz["kind"] = "support" if dir_u == "BUY" else "resistance"
            zz["zone_role"] = "H1_PRIMARY" if str(tf_name).upper() == "H1" else "H4_FALLBACK"
            zz["dist_atr"] = float(dist_atr)
            zz["distance"] = float(abs(float(cl) - float(lvl)))
            scored.append({
                "zone": zz,
                "score": _composite_score(zz, dist_atr, str(tf_name).upper()),
                "dist_atr": float(dist_atr),
            })

        if not scored:
            return None, {"count": 0}

        # Prefer high quality, then nearer zone.
        scored.sort(key=lambda x: (-float(x.get("score") or 0.0), float(x.get("dist_atr") or 1e9)))
        best = scored[0]["zone"]
        return best, {
            "count": int(len(scored)),
            "best_level": float(best.get("level")),
            "best_dist_atr": float(best.get("dist_atr") or 0.0),
        }
    
    
    
    h1_major_zone, h1_dbg = _pick_best_major(_get_levels(h1, major_key), "H1")
    h4_major_zone, h4_dbg = _pick_best_major(_get_levels(h4, major_key), "H4")

    # Active zone remains H1-first for Phase 1. If H1 major is absent, use H4.
    # PHASE-1 SAFETY:
    # Do not execute H4 zone using H1 reversal candles.
    # H4 execution requires H4 candle picker, handled in Phase 2.
    zone = h1_major_zone

    if not isinstance(zone, dict) or zone.get("level") is None:
        return None

    zone = dict(zone)
    zone["selection_model"] = "H1_MAJOR_THEN_H4_MAJOR_PHASE1"
    zone["h1_major_zone"] = h1_major_zone if isinstance(h1_major_zone, dict) else None
    zone["h4_major_zone"] = h4_major_zone if isinstance(h4_major_zone, dict) else None
    zone["primary_zone"] = h1_major_zone if isinstance(h1_major_zone, dict) else None
    zone["secondary_zone"] = h4_major_zone if isinstance(h4_major_zone, dict) else None
    zone["zone_stage"] = "H1" if isinstance(h1_major_zone, dict) else "H4"
    zone["zone_pair_debug"] = {"h1": h1_dbg, "h4": h4_dbg}

    return zone

def _nearest_levels_from_sr(
    sr_all: dict,
    price: float,
    atr: float,
    *,
    pip_factor: float = 0.01,
    cross_buf: float = 0.0,
) -> dict:
    """
    Compute nearest_support / nearest_resistance using CURRENT price.
    Major-first (H4+H1), then Near, then All.
    Side-aware with soft buffer to tolerate sweep/liquidity wick.
    """
    out = {
        "nearest_support": None,
        "nearest_resistance": None,
        "buf": None,
        "src_support": None,
        "src_resistance": None,
    }

    if not isinstance(sr_all, dict):
        return out

    try:
        px = float(price)
    except Exception:
        return out

    try:
        atr = float(atr)
    except Exception:
        atr = 0.0

    try:
        pip_factor = float(pip_factor)
    except Exception:
        pip_factor = 0.01

    try:
        cross_buf = float(cross_buf)
    except Exception:
        cross_buf = 0.0

    # sweep tolerance (price units)
    try:
        buf = max(cross_buf, 0.10 * (atr or 0.0), 5.0 * (pip_factor or 0.0))
    except Exception:
        buf = cross_buf or 0.0

    out["buf"] = float(buf)

    px_for_support = px + buf  # allow support slightly ABOVE px
    px_for_resist  = px - buf  # allow resistance slightly BELOW px

    def _tf_bucket(sr: dict, key: str) -> list:
        v = sr.get(key)
        return v if isinstance(v, list) else []

    # Accept both shapes:
    # A) full: {"h1":{...}, "h4":{...}, ...}
    # B) tf-sliced: {"supports":[...], "supports_major":[...], ...}
    is_tf_sliced = any(
        k in sr_all
        for k in ("supports", "resistances", "supports_near", "resistances_near", "supports_major", "resistances_major")
    )

    if is_tf_sliced:
        h1 = sr_all
        h4 = {}
    else:
        h1 = sr_all.get("h1") if isinstance(sr_all.get("h1"), dict) else {}
        h4 = sr_all.get("h4") if isinstance(sr_all.get("h4"), dict) else {}

    def _nearest_support_from(rows: list) -> float | None:
        vals = []
        for r in rows or []:
            if not isinstance(r, dict):
                continue
            try:
                lvl = float(r.get("level"))
            except Exception:
                continue
            if lvl <= px_for_support:
                vals.append(lvl)
        return max(vals) if vals else None

    def _nearest_res_from(rows: list) -> float | None:
        vals = []
        for r in rows or []:
            if not isinstance(r, dict):
                continue
            try:
                lvl = float(r.get("level"))
            except Exception:
                continue
            if lvl >= px_for_resist:
                vals.append(lvl)
        return min(vals) if vals else None

    # Major-first
    major_supp = _tf_bucket(h4, "supports_major") + _tf_bucket(h1, "supports_major")
    major_res  = _tf_bucket(h4, "resistances_major") + _tf_bucket(h1, "resistances_major")
    ns = _nearest_support_from(major_supp)
    nr = _nearest_res_from(major_res)
    if ns is not None:
        out["nearest_support"] = float(ns)
        out["src_support"] = "major"
    if nr is not None:
        out["nearest_resistance"] = float(nr)
        out["src_resistance"] = "major"

    # Near fallback
    if out["nearest_support"] is None:
        near_supp = _tf_bucket(h4, "supports_near") + _tf_bucket(h1, "supports_near")
        ns = _nearest_support_from(near_supp)
        if ns is not None:
            out["nearest_support"] = float(ns)
            out["src_support"] = "near"

    if out["nearest_resistance"] is None:
        near_res = _tf_bucket(h4, "resistances_near") + _tf_bucket(h1, "resistances_near")
        nr = _nearest_res_from(near_res)
        if nr is not None:
            out["nearest_resistance"] = float(nr)
            out["src_resistance"] = "near"

    # All fallback
    if out["nearest_support"] is None:
        all_supp = _tf_bucket(h4, "supports") + _tf_bucket(h1, "supports")
        ns = _nearest_support_from(all_supp)
        if ns is not None:
            out["nearest_support"] = float(ns)
            out["src_support"] = "all"

    if out["nearest_resistance"] is None:
        all_res = _tf_bucket(h4, "resistances") + _tf_bucket(h1, "resistances")
        nr = _nearest_res_from(all_res)
        if nr is not None:
            out["nearest_resistance"] = float(nr)
            out["src_resistance"] = "all"

    return out

def _pick_display_zones_from_sr(sr_all: dict, price: float, atr: float, tf_tag: str, sym_u: str) -> dict:
    """
    Display-only zones:
    - next valid H1/H4 support below/near price
    - next valid H1/H4 resistance above/near price
    - ignores stale / side_ok false
    """
    out = {
        "h1_buy_zone": None,
        "h4_buy_zone": None,
        "h1_sell_zone": None,
        "h4_sell_zone": None,
    }

    if not isinstance(sr_all, dict):
        return out

    h1 = sr_all.get("h1") if isinstance(sr_all.get("h1"), dict) else {}
    h4 = sr_all.get("h4") if isinstance(sr_all.get("h4"), dict) else {}

    def _valid(z):
        return (
            isinstance(z, dict)
            and z.get("stale") is not True
            and z.get("side_ok") is not False
            and z.get("level") is not None
        )

    def _band(z, tf):
        zz = dict(z)
        try:
            if zz.get("low") is None or zz.get("high") is None or float(zz["low"]) >= float(zz["high"]):
                ztmp = _level_to_zone(float(zz["level"]), tf, sym_u, atr)
                zz["low"] = ztmp["low"]
                zz["high"] = ztmp["high"]
        except Exception:
            pass
        zz["tf"] = tf
        return zz

    def _support(tf_obj, tf):
        rows = [z for z in (tf_obj.get("supports_major") or []) if _valid(z)]

        # Recalculate live-side validity. Do not trust cached side_ok.
        rows = [
            z for z in rows
            if float(z["level"]) <= float(price)
        ]
        rows.sort(key=lambda z: abs(float(price) - float(z["level"])))
        return _band(rows[0], tf) if rows else None

    def _resistance(tf_obj, tf):
        rows = [z for z in (tf_obj.get("resistances_major") or []) if _valid(z)]

        # Recalculate live-side validity. Do not trust cached side_ok.
        rows = [
            z for z in rows
            if float(z["level"]) >= float(price)
        ]
        rows.sort(key=lambda z: abs(float(price) - float(z["level"])))
        return _band(rows[0], tf) if rows else None

    out["h1_buy_zone"] = _support(h1, "H1")
    out["h4_buy_zone"] = _support(h4, "H4")
    out["h1_sell_zone"] = _resistance(h1, "H1")
    out["h4_sell_zone"] = _resistance(h4, "H4")
    out["h1_buy_status"] = "VALID" if out["h1_buy_zone"] else "NO_SUPPORT_BELOW_PRICE"
    out["h4_buy_status"] = "VALID" if out["h4_buy_zone"] else "NO_SUPPORT_BELOW_PRICE"
    out["h1_sell_status"] = "VALID" if out["h1_sell_zone"] else "NO_RESISTANCE_ABOVE_PRICE"
    out["h4_sell_status"] = "VALID" if out["h4_sell_zone"] else "NO_RESISTANCE_ABOVE_PRICE"

    return out

from api.tenant_keys import (
    zone_watch_key,
    zone_watch_set,
    zone_watch_delete,
    break_state_key,
    delete_latest_entry_claim,
)
def _watch_key(
    uid: str,
    sym: str,
    direction: str,
    tf_tag: str = "H1",
) -> str:
    return zone_watch_key(
        uid,
        sym,
        direction,
        tf_tag,
    )

def _zone_cooldown_key(
    uid: str,
    profile_id: str,
    sym: str,
    direction: str,
    tf_tag: str,
) -> str:
    uid_u = str(uid or "").strip()
    profile_u = str(profile_id or "").strip().lower()
    sym_u = str(sym or "").upper().strip()
    direction_u = str(direction or "").upper().strip()
    tf_u = str(tf_tag or "H1").upper().strip()

    return (
        f"xtl:zone:cooldown:"
        f"{uid_u}:{profile_u}:{sym_u}:{direction_u}:{tf_u}"
    )

def _json_load(raw):
    try:
        if raw is None:
            return None
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", "ignore")
        raw = str(raw).strip()
        if not raw:
            return None
        import json
        return json.loads(raw)
    except Exception:
        return None

def _f(v):
    try:
        vv = float(v)
        return vv if vv > 0 else None
    except Exception:
        return None

def _bar_get(b: dict, *keys):
    # alias to existing bar reader if present
    try:
        return _bar_f(b, *keys)
    except Exception:
        try:
            for k in keys:
                if k in b and b.get(k) is not None:
                    return float(b.get(k))
        except Exception:
            pass
    return None

def _atr14_from_bars(bars: list) -> float | None:
    """
    Compute ATR(14) from bars list with keys o/h/l/c (or open/high/low/close).
    Returns ATR in price units.
    """
    try:
        if not isinstance(bars, list) or len(bars) < 20:
            return None

        trs = []
        prev_close = None

        for b in bars[-60:]:  # enough history; keep it light
            if not isinstance(b, dict):
                continue
            h = _bar_get(b, "h", "high")
            l = _bar_get(b, "l", "low")
            c = _bar_get(b, "c", "close")
            if h is None or l is None or c is None:
                continue

            if prev_close is None:
                tr = float(h - l)
            else:
                tr = max(float(h - l), abs(float(h - prev_close)), abs(float(l - prev_close)))

            trs.append(tr)
            prev_close = float(c)

        if len(trs) < 15:
            return None

        # ATR14 as simple moving average of last 14 TRs
        w = trs[-14:]
        atr = sum(w) / float(len(w))
        if atr <= 0:
            return None
        return float(atr)
    except Exception:
        return None


def zone_reversal_gate(
    *,
    R,
    uid: str,
    sym: str,
    direction: str,
    row_h1: dict,
    sr: dict | None,
    now_ms: int,
    tf_tag: str = "H1",
    pinned_device: str | None = None,
    x_device_id: str | None = None,
    live_px: float | None = None,
    debug_gate: bool = False,
    move_away_atr: float = 2.0,
    hard_close_bars: int = 2,
    **_kwargs,
    
) -> Tuple[bool, dict]:
    """
    Zone-only entry gate (single writer).

    Returns: (allowed, gate_meta)
    allowed=True only when READY_REV_OK (reclaim confirmed).
    """
    sym_u = (sym or "").upper().strip()
    dir_u = (direction or "").upper().strip()
    tfu = (tf_tag or "H1").upper()
    uid_u = str(uid or "").strip()

    if not uid_u:
        return False, {
            "blocked": True,
            "stage": "ZONE_GATE",
            "reason": "UID_REQUIRED_FOR_ZONE_GATE",
            "tf": tfu,
            "rev_ok": False,
            "zone": None,
            "planned_zone": None,
            "zone_used": None,
            "rev_state": None,
        }

    gate: Dict[str, Any] = {"blocked": True, "reason": "unknown", "tf": tfu}
    now_ms_pick = int(now_ms)

    
    # 0) bars (prefer attached; else fetch from Redis snap using device)
    bars = None
    try:
        bars = (row_h1 or {}).get("bars")
    except Exception:
        bars = None

    
    # If missing bars, pull from Redis snap: xtl:ohlc:snap:<DEV>:<SYM>:H1
    # PHASE-1 FIX:
    # Always prefer latest device Redis snap.
    # row_h1 bars may be stale from trend_endpoints snapshot.
    if True:
        dev = (str(x_device_id or "").strip() or str(pinned_device or "").strip())
        if dev:
            try:
                k = f"xtl:ohlc:snap:{dev}:{sym_u}:{tfu}"
                raw = R.get(k) if R is not None else None
                js = _json_load(raw)
                b2 = js.get("bars") if isinstance(js, dict) else None
                if not isinstance(b2, list):
                    b2 = js.get("ohlc") if isinstance(js, dict) else None

                if isinstance(b2, list) and len(b2) >= 2:
                    bars = b2

                    # --- FIX: use snap clock for "closed bar" logic (defensive) ---
                    snap_last_closed = 0
                    snap_server_now = 0
                    try:
                        snap_last_closed = int(js.get("lastClosedTs") or 0) if isinstance(js, dict) else 0
                    except Exception:
                        snap_last_closed = 0
                    try:
                        snap_server_now = int(js.get("serverNow") or 0) if isinstance(js, dict) else 0
                    except Exception:
                        snap_server_now = 0

                    # lastClosedTs must not be ahead of serverNow by a large margin
                    # IMPORTANT:
                    # Use serverNow/current clock to decide which candle is closed.
                    # Do NOT use lastClosedTs as now_ms_pick, otherwise picker can lag by 1-2 candles.
                    # Fix: serverNow can be stale (MT5 bridge doesn't update it every tick)
                    # Use max of serverNow, last bar close time, and system now
                    # This prevents the bar picker from treating recent closed bars as "future"
                    _snap_server_now = int(snap_server_now) if snap_server_now > 0 else 0
                    _last_bar_close_ms = 0
                    try:
                        if isinstance(bars, list) and bars:
                            _lb = bars[-1]
                            _lb_t = _to_ms_any(
                                _lb.get("t_close_ms") or _lb.get("tCloseMs") or
                                _lb.get("t") or _lb.get("ts") or _lb.get("time") or 0
                            )
                            if _lb_t and int(_lb_t) > 0:
                                # If bar key is open time, add tf_ms to get close time
                                _lb_close = int(_lb_t)
                                if _lb_close < int(now_ms) - tf_ms:
                                    # looks like open time — add tf_ms
                                    _lb_close = _lb_close + int(tf_ms)
                                _last_bar_close_ms = _lb_close
                    except Exception:
                        _last_bar_close_ms = 0

                    # Use the freshest REAL clock (snap serverNow or system now).
                    # Do NOT max in _last_bar_close_ms: MT5 emits future bars whose
                    # close time would inflate now_ms_pick into the future, poisoning
                    # started_ms / last_checked_closed_ms / rev_ok_ms and causing the
                    # executor (true UTC) to reject every RC as "future".
                    now_ms_pick = max(
                        _snap_server_now,
                        int(now_ms or 0)
                    )
                    if now_ms_pick <= 0:
                        now_ms_pick = int(now_ms)
                    snap_repaired = False

                    # lastClosedTs is debug/reference only
                    if snap_last_closed > 0 and snap_server_now > 0 and snap_last_closed > (snap_server_now + 120_000):
                        snap_repaired = True

                    if debug_gate:
                        gate["dbg_h1_bars_src"] = "dev_snap"
                        gate["dbg_h1_snap_key"] = k
                        gate["dbg_h1_bars_n"] = int(len(bars))
                        gate["dbg_h1_snap_serverNow"] = (js.get("serverNow") if isinstance(js, dict) else None)
                        gate["dbg_lastClosedTs"] = (js.get("lastClosedTs") if isinstance(js, dict) else None)
                        gate["dbg_h1_snap_clock_delta_ms"] = int((snap_last_closed or 0) - (snap_server_now or 0))
                        gate["dbg_h1_snap_clock_repaired"] = bool(snap_repaired)
                else:
                    if debug_gate:
                        gate["dbg_h1_bars_src"] = "dev_snap_empty"
                        gate["dbg_h1_snap_key"] = k
            except Exception as e:
                if debug_gate:
                    gate["dbg_h1_bars_src"] = "dev_snap_exc"
                    gate["dbg_h1_bars_exc_type"] = type(e).__name__
                    gate["dbg_h1_bars_exc"] = str(e)

    if not isinstance(bars, list) or not bars:
        gate["reason"] = "no_h1_bars"
        gate["stage"] = "H1_BARS"
        return False, gate


    # 0B) last closed bar - CRITICAL FIX: Always use tuple unpacking safely
    tf_ms = {
        "M15": 15 * 60 * 1000,
        "H1": 60 * 60 * 1000,
        "H4": 4 * 60 * 60 * 1000,
    }.get(str(tfu).upper(), 60 * 60 * 1000)
    c, p = (None, None)  # Default to tuple
    try:
        # Prefer MT5-agent completed bars. The agent already marks H1 candles
        # complete=True. Do not reject the newest completed bar just because
        # broker candle timestamps are ahead of repaired server clock; that
        # makes gate lag 1-2 candles and miss zone touches.
        # A bar is only truly closed once its close-time has actually passed.
        # MT5 marks FUTURE bars complete=True, so trusting `complete` alone makes the
        # picker select a future bar as the RC candle (future rev_ok_ms -> entry limbo).
        # Apply the same clock check the fallback picker uses. Rejecting future bars
        # does NOT reintroduce lag: a just-closed bar (close-time already passed) is
        # still accepted immediately; only genuinely-future bars are excluded.
        def _ob_ms(b):
            for _k in ("t_open_ms", "tOpenMs", "open_time_ms", "ts_ms", "t", "time", "ts"):
                _v = _to_ms_any(b.get(_k))
                if _v > 0:
                    return int(_v)
            return 0

        # ------------------------------------------------------------
        # FIX:
        # Use broker snap's own lastClosedTs as candle cutoff.
        # Do NOT compare broker-time candle ms to server UTC time.time().
        # FTMO/MT5 broker candles are broker-time shifted, so server clock
        # can make gate pick one candle behind.
        # ------------------------------------------------------------
        try:
            _snap_cutoff_ms = _to_ms_any(
                snap_last_closed or 0
            )
        except Exception:
            _snap_cutoff_ms = 0

        

        
        complete_bars = [
            b for b in (bars or [])
            if isinstance(b, dict)
            and bool(b.get("complete")) is True
            and _ob_ms(b) > 0
            and (
                _snap_cutoff_ms <= 0
                or (_ob_ms(b) + int(tf_ms)) <= _snap_cutoff_ms
            )
        ]
        if debug_gate:
            try:
                gate["dbg_complete_bars_last3"] = [
                    {
                        "open_ms": _ob_ms(x),
                        "close_ms": _ob_ms(x) + int(tf_ms),
                        "o": x.get("o"),
                        "h": x.get("h"),
                        "l": x.get("l"),
                        "c": x.get("c"),
                        "complete": x.get("complete"),
                    }
                    for x in complete_bars[-3:]
                ]
                gate["dbg_snap_cutoff_ms"] = int(_snap_cutoff_ms or 0)
            except Exception:
                pass
        if complete_bars:
            c = complete_bars[-1]
            p = complete_bars[-2] if len(complete_bars) >= 2 else None
            if debug_gate:
                gate["dbg_pick_model"] = "LATEST_COMPLETE_TRUE_BAR"
        else:
            result = _pick_last_closed_bar_from_bars(bars, int(now_ms_pick), int(tf_ms))
            if result is not None and isinstance(result, tuple) and len(result) == 2:
                c, p = result
            else:
                c, p = (None, None)
            if debug_gate:
                gate["dbg_pick_model"] = "TIME_BASED_PICKER_FALLBACK"
    except Exception as e:
        if debug_gate:
            gate["dbg_pick_bar_exc"] = f"{type(e).__name__}:{e}"
        c, p = (None, None)
    
    if not isinstance(c, dict):
        gate["reason"] = "no_h1_closed_bar"
        gate["stage"] = "H1_PICK"
        gate["bars_n"] = int(len(bars) if isinstance(bars, list) else 0)
        return False, gate

    if debug_gate:
        try:
            t_open_ms_dbg = _to_ms_any(c.get("t_open_ms") or c.get("ts_ms") or c.get("t"))
        except Exception:
            t_open_ms_dbg = 0
        try:
            t_close_ms_dbg = _to_ms_any(c.get("t_close_ms") or c.get("tCloseMs") or c.get("t_close"))
        except Exception:
            t_close_ms_dbg = 0
        if not t_close_ms_dbg and t_open_ms_dbg:
            t_close_ms_dbg = int(t_open_ms_dbg + int(tf_ms))

        gate["dbg_now_ms_pick"] = int(now_ms_pick)
        gate["dbg_pick_bar_start_ms"] = int(t_open_ms_dbg)
        gate["dbg_pick_bar_close_ms"] = int(t_close_ms_dbg)
        gate["dbg_pick_bar_cl"] = float(_bar_f(c, "c", "close") or 0.0)

    # 0C) compute closed_ms for the selected closed bar (needed for watch started_ms)
    closed_ms = 0
    try:
        closed_ms = _to_ms_any(
            c.get("t_close_ms")
            or c.get("tCloseMs")
            or c.get("t_close")
        )

        t_open_ms = _to_ms_any(
            c.get("t_open_ms")
            or c.get("tOpenMs")
            or c.get("open_time_ms")
            or c.get("ts_ms")
            or c.get("t")
            or c.get("time")
            or c.get("ts")
        )

        # Some snapshots incorrectly set t_close_ms equal to t_open_ms.
        # A completed H1 candle close must be open time + tf_ms.
        if (
            t_open_ms
            and (
                not closed_ms
                or int(closed_ms) <= int(t_open_ms)
            )
        ):
            closed_ms = int(t_open_ms + tf_ms)

    except Exception:
        closed_ms = 0


    cl = _bar_f(c, "c", "close")
    lo = _bar_f(c, "l", "low")
    hi = _bar_f(c, "h", "high")

    gate["picked_closed_bar"] = {
        "tf": str(tfu),
        "tf_ms": int(tf_ms),
        "closed_ms": int(closed_ms),
        "open": float(_bar_f(c, "o", "open") or 0),
        "high": float(hi or 0),
        "low": float(lo or 0),
        "close": float(cl or 0),
        "now_ms_pick": int(now_ms_pick),
    }
    if cl is None or lo is None or hi is None:
        gate["reason"] = "bad_h1_bar"
        gate["stage"] = "H1_OHLC"
        return False, gate
    # ------------------------------------------------------------
    # Use LIVE price for SR direction + zone selection
    # ------------------------------------------------------------
    try:
        decision_px = (
            float(live_px)
            if live_px is not None and float(live_px) > 0
            else float(cl)
        )
    except Exception:
        decision_px = float(cl)

    gate["decision_px"] = float(decision_px)
    gate["h1_closed_cl"] = float(cl)

    # ATR: prefer provided/row value; fallback compute from bars
    atr = None
    try:
        # prefer explicit
        atr = _f((row_h1 or {}).get("atr"))
    except Exception:
        atr = None

    if atr is None:
        try:
            # sometimes row has atr_h1 or similar name
            atr = _f((row_h1 or {}).get("atr_h1"))
        except Exception:
            atr = None

    if atr is None:
        atr = _atr14_from_bars(bars)

    if atr is None:
        gate["reason"] = "no_atr"
        gate["stage"] = "ATR"
        gate["blocked"] = True
        return False, gate

    if debug_gate:
        gate["dbg_atr_src"] = "bars_atr14" if (row_h1 or {}).get("atr") is None else "row"
        gate["atr"] = float(atr)
    # Price-aware nearest SR (DO NOT trust cached sr.nearest_*)
    try:
        pip_factor = float((sr or {}).get("pip_factor") or 0.01)
    except Exception:
        pip_factor = 0.01
    try:
        cross_buf = float((sr or {}).get("cross_buf") or 0.0)
    except Exception:
        cross_buf = 0.0

    nearest = _nearest_levels_from_sr(
        sr or {},
        float(decision_px),
        float(atr),
        pip_factor=pip_factor,
        cross_buf=cross_buf,
    )
    if debug_gate:
        gate["dbg_nearest_sr"] = nearest

    display_zones = _pick_display_zones_from_sr(
        sr or {},
        float(decision_px),
        float(atr),
        tfu,
        sym_u,
    )
    def _zone_band_dist_local(z: dict, px: float) -> float | None:
        try:
            if not isinstance(z, dict):
                return None
            zl = float(z.get("low") if z.get("low") is not None else z.get("level"))
            zh = float(z.get("high") if z.get("high") is not None else z.get("level"))
            if zl > zh:
                zl, zh = zh, zl
            px = float(px)
            if zl <= px <= zh:
                return 0.0
            if px < zl:
                return float(zl - px)
            return float(px - zh)
        except Exception:
            return None

    def _actionable_cap_local(sym_u: str, atr: float) -> float:
        s = str(sym_u or "").upper().strip()
        base = float(move_away_atr) * float(atr)

        if s == "XAUUSD":
           return min(max(base, 3.0), 12.0)
        if s.endswith("JPY"):
           return min(max(base, 0.08), 0.25)
        return min(max(base, 0.0008), 0.0025)

    gate["display_zones"] = display_zones
    gate["h1_buy_zone"] = display_zones.get("h1_buy_zone")
    gate["h4_buy_zone"] = display_zones.get("h4_buy_zone")
    gate["h1_sell_zone"] = display_zones.get("h1_sell_zone")
    gate["h4_sell_zone"] = display_zones.get("h4_sell_zone")
    gate["h1_buy_status"] = display_zones.get("h1_buy_status")
    gate["h4_buy_status"] = display_zones.get("h4_buy_status")
    gate["h1_sell_status"] = display_zones.get("h1_sell_status")
    gate["h4_sell_status"] = display_zones.get("h4_sell_status")

    

    # ------------------------------------------------------------
    # Direction resolver from nearest ACTIONABLE zone band
    # BUY  = price near/inside support zone
    # SELL = price near/inside resistance zone
    # WATCHING = no nearby actionable zone
    # ------------------------------------------------------------
    resolved_dir = "WATCHING"
    preferred_zone = None

    def _zone_band_dist(z: dict, px: float) -> float | None:
        try:
            if not isinstance(z, dict):
                return None
            zl = float(z.get("low") if z.get("low") is not None else z.get("level"))
            zh = float(z.get("high") if z.get("high") is not None else z.get("level"))
            if zl > zh:
                zl, zh = zh, zl

            px = float(px)

            if zl <= px <= zh:
                return 0.0
            if px < zl:
                return float(zl - px)
            return float(px - zh)
        except Exception:
            return None

    def _actionable_cap(sym_u: str, atr: float) -> float:
        base = float(move_away_atr) * float(atr)
        s = str(sym_u or "").upper().strip()

        if s == "XAUUSD":
            floor = 6.0
            ceiling = 12.0
        elif s.endswith("JPY"):
            floor = 0.15
            ceiling = 0.25
        else:
            floor = 0.0015
            ceiling = 0.0025

        return min(max(base, floor), ceiling)

    try:
        cap = _actionable_cap(sym_u, float(atr))

        candidates = []

        # ------------------------------------------------------------
        # Best scored SR feeds direction resolver first.
        # H4 is confirmation only, not execution.
        # Fallback to legacy H1 display zones only if best scored zone missing.
        # ------------------------------------------------------------
        best_buy_zone = (sr or {}).get("best_support") if isinstance(sr, dict) else None
        best_sell_zone = (sr or {}).get("best_resistance") if isinstance(sr, dict) else None

        candidate_sources = []

        # BUY candidates:
        # Include BOTH best scored support and nearest H1 display support.
        # Do not let a far/H4 best_support hide a nearer valid H1 support.
        if isinstance(best_buy_zone, dict):
            bz = dict(best_buy_zone)
            bz["tf"] = str(bz.get("tf") or "H1").upper()
            bz["kind"] = "support"
            bz["zone_source"] = bz.get("zone_source") or "BEST_SCORED_SR"
            bz["selection_model"] = "BEST_SR_DIRECTION_RESOLVER"
            candidate_sources.append(("BUY", bz))

        if isinstance(display_zones.get("h1_buy_zone"), dict):
            bz = dict(display_zones.get("h1_buy_zone"))
            bz["tf"] = "H1"
            bz["kind"] = "support"
            bz["zone_source"] = bz.get("zone_source") or "H1_DISPLAY_ZONE"
            bz["selection_model"] = "H1_DISPLAY_DIRECTION_RESOLVER"
            candidate_sources.append(("BUY", bz))

        # SELL candidates:
        # Include BOTH best scored resistance and nearest H1 display resistance.
        # Do not let a far/H4 best_resistance hide a nearer valid H1 resistance.
        if isinstance(best_sell_zone, dict):
            bz = dict(best_sell_zone)
            bz["tf"] = str(bz.get("tf") or "H1").upper()
            bz["kind"] = "resistance"
            bz["zone_source"] = bz.get("zone_source") or "BEST_SCORED_SR"
            bz["selection_model"] = "BEST_SR_DIRECTION_RESOLVER"
            candidate_sources.append(("SELL", bz))

        if isinstance(display_zones.get("h1_sell_zone"), dict):
            bz = dict(display_zones.get("h1_sell_zone"))
            bz["tf"] = "H1"
            bz["kind"] = "resistance"
            bz["zone_source"] = bz.get("zone_source") or "H1_DISPLAY_ZONE"
            bz["selection_model"] = "H1_DISPLAY_DIRECTION_RESOLVER"
            candidate_sources.append(("SELL", bz))
        for side, z in candidate_sources:
            if not isinstance(z, dict):
                continue

            d0 = _zone_band_dist(z, float(decision_px))
            if d0 is None:
                continue

            zz = dict(z)
            zz["kind"] = "support" if side == "BUY" else "resistance"

            # PHASE-1: H1 execution only.
            # H4 may be displayed separately, but must not win direction resolver
            # or become the executable gate zone.
            if str(zz.get("tf") or "").upper() != "H1":
                continue

            zz["actionable_dist"] = float(d0)

            candidates.append({
                "side": side,
                "zone": zz,
                "dist": float(d0),
                "tf_rank": 0,
            })

        candidates.sort(key=lambda x: (float(x["dist"]), int(x["tf_rank"])))

        if candidates and float(candidates[0]["dist"]) <= float(cap):
            resolved_dir = candidates[0]["side"]
            preferred_zone = candidates[0]["zone"]
        else:
            resolved_dir = "WATCHING"
        if resolved_dir in ("BUY", "SELL"):
            best_key = "best_support" if resolved_dir == "BUY" else "best_resistance"
            best_zone = sr.get(best_key) if isinstance(sr, dict) else None

            if isinstance(best_zone, dict):
                try:
                    bz = dict(best_zone)
                    _bz_dist = _zone_band_dist(bz, float(decision_px))
                    if (
                        bz.get("level") is not None
                        and bz.get("low") is not None
                        and bz.get("high") is not None
                        and float(bz.get("low")) < float(bz.get("high"))
                        and bz.get("side_ok") is not False
                        and bz.get("stale") is not True
                        and str(bz.get("tf") or "").upper() == "H1"
                        and _bz_dist is not None
                        and float(_bz_dist) <= float(cap)
                    ):
                        bz["zone_source"] = "BEST_SCORED_SR"
                        bz["selection_model"] = "BEST_SR_DIRECTION_RESOLVED"
                        bz["execution_tf"] = "H1"
                        bz["zone_role"] = "BEST_SUPPORT" if resolved_dir == "BUY" else "BEST_RESISTANCE"
                        bz["actionable_dist"] = float(_bz_dist)
                        preferred_zone = bz
                except Exception:
                    pass

        gate["resolved_dir"] = resolved_dir
        gate["dir_input"] = dir_u
        gate["actionable_cap"] = float(cap)
        gate["direction_model"] = "NEAREST_ACTIONABLE_ZONE_BAND"
        gate["actionable_candidates"] = [
            {
                "side": x["side"],
                "tf": x["zone"].get("tf"),
                "level": x["zone"].get("level"),
                "low": x["zone"].get("low"),
                "high": x["zone"].get("high"),
                "dist": x["dist"],
                "zone_source": x["zone"].get("zone_source"),
                "selection_model": x["zone"].get("selection_model"),
                "quality_score": x["zone"].get("quality_score"),
            }
            for x in candidates[:4]
        ]

    except Exception as e:
        resolved_dir = "WATCHING"
        if debug_gate:
            gate["dbg_direction_resolver_exc"] = f"{type(e).__name__}:{e}"


    
    # ------------------------------------------------------------
    # 1) load existing watch FIRST
    # IMPORTANT:
    # Do NOT trust freshly resolved_dir if a frozen BUY/SELL watch already exists.
    # Price may temporarily close beyond zone_low/zone_high and make resolved_dir=WATCHING.
    # Frozen watch must remain active until:
    #   - entry triggers
    #   - OR 2 consecutive invalidation closes happen
    # ------------------------------------------------------------
    watch = None
    zone_used = None
    wkey = None

    def _is_protected_watch_state(st: str) -> bool:
        st_u = str(st or "").upper().strip()
        return (
            st_u in ("REV_OK", "ENTRY_READY", "ORDER_PENDING", "TRADE_ACTIVE")
            or st_u.startswith("ENTRY_BLOCKED")
        )

    def _load_watch_for_dir(d: str):
        try:
            k = _watch_key(
                uid_u,
                sym_u,
                d,
                tfu,
            )
            raw = R.get(k)
            if not raw:
                return None, k
            if isinstance(raw, (bytes, bytearray)):
                raw = raw.decode("utf-8", "ignore")
            obj = json.loads(raw) if isinstance(raw, str) else raw
            return (obj if isinstance(obj, dict) else None), k
        except Exception:
            return None, _watch_key(uid_u,sym_u, d, tfu)

    # Prefer active frozen watch over newly resolved direction
    for d0 in ("BUY", "SELL"):
        w0, k0 = _load_watch_for_dir(d0)
        if isinstance(w0, dict) and isinstance(w0.get("zone_used"), dict):
            watch = w0
            wkey = k0
            zone_used = w0.get("zone_used")
            resolved_dir = str(w0.get("direction") or d0).upper()
            gate["resolved_dir"] = resolved_dir
            gate["watch_key"] = str(wkey)
            gate["watch_reused"] = True
            break
    

    # If no frozen watch exists, use newly resolved direction
    if watch is None:
        wkey = _watch_key(uid_u,sym_u, resolved_dir, tfu)
        try:
            raw = R.get(wkey)
            if raw:
                if isinstance(raw, (bytes, bytearray)):
                    raw = raw.decode("utf-8", "ignore")
                watch = json.loads(raw) if isinstance(raw, str) else raw
        except Exception:
            watch = None

        if isinstance(watch, dict) and isinstance(watch.get("zone_used"), dict):
            zone_used = watch.get("zone_used")
    # ------------------------------------------------------------
    # ------------------------------------------------------------
    # Preserve TRADE_ACTIVE until executor completes broker-close
    # reconciliation.
    #
    # A missing broker position does NOT mean the trade lifecycle
    # is complete. The position may have just closed at TP/SL/manual
    # exit, while the executor is still waiting for or applying the
    # MT5 deal record.
    #
    # Ownership:
    #   zone_entry_gate.py -> display/preserve active state only
    #   oppt_executor.py   -> close ledger, release risk, set cooldown,
    #                        then clear/reset the watch
    # ------------------------------------------------------------
    try:
        if isinstance(watch, dict):
            _watch_state = str(
                watch.get("state") or ""
            ).upper().strip()

            _watch_trade_state = str(
                watch.get("trade_state") or ""
            ).upper().strip()

            # -------------------------------------------------
            # A broker close was already confirmed previously.
            # Do not allow the normal gate logic below to turn
            # this watch into WATCH / REV_OK again.
            # -------------------------------------------------
            if (
                _watch_state == "BROKER_CLOSE_PENDING"
                or _watch_trade_state
                == "BROKER_CLOSE_PENDING"
            ):
                return False, {
                    "blocked": True,
                    "stage": "BROKER_CLOSE_PENDING",
                    "reason": (
                        "BROKER_DEAL_PENDING_"
                        "EXECUTOR_RECONCILIATION"
                    ),
                    "trade_state": "BROKER_CLOSE_PENDING",
                    "broker_position_present": False,
                    "broker_deal_present": True,
                    "broker_reconciliation_pending": True,
                    "broker_close_reason": watch.get(
                        "broker_close_reason"
                    ),
                    "broker_closed_at_ms": watch.get(
                        "broker_closed_at_ms"
                    ),
                    "mt5_ticket": (
                        watch.get("mt5_ticket")
                        or watch.get("broker_ticket")
                        or watch.get("position_ticket")
                    ),
                    "device_id": watch.get("device_id"),
                    "rev_ok": False,
                    "zone": watch.get("zone"),
                    "planned_zone": watch.get(
                        "planned_zone"
                    ),
                    "zone_used": watch.get("zone_used"),
                    "rev_state": watch,
                }

            if (
                _watch_state == "TRADE_ACTIVE"
                or _watch_trade_state == "TRADE_ACTIVE"
            ):
                _watch_device_id = str(
                    dev
                    or watch.get("device_id")
                    or watch.get("broker_device_id")
                    or ""
                ).strip()

                if _watch_device_id:
                    watch["device_id"] = _watch_device_id

                try:
                    _watch_ticket = int(
                        watch.get("mt5_ticket")
                        or watch.get("broker_ticket")
                        or watch.get("position_ticket")
                        or 0
                    )
                except Exception:
                    _watch_ticket = 0

                # Use the account namespace stored on the watch.
                # Demo/challenge accounts normally publish under
                # Use the broker snapshot namespace stored on the watch.
                # The namespace follows actual MT5 trade mode:
                # demo/contest -> "demo", real -> "live".
                _watch_account_type = str(
                    watch.get("mt5_account")
                    or watch.get("account_type")
                    or watch.get("broker_account_type")
                    or ""
                ).lower().strip()

                if _watch_account_type not in (
                    "demo",
                    "live",
                ):
                    log.error(
                        "[WATCHLIST] ACTIVE_TRADE_ACCOUNT_TYPE_MISSING "
                        "sym=%s side=%s key=%s ticket=%s",
                        sym_u,
                        str(resolved_dir).upper(),
                        wkey,
                        _watch_ticket,
                    )

                    return False, {
                        "blocked": True,
                        "stage": "TRADE_ACTIVE_RECON_ERROR",
                        "reason": "MT5_ACCOUNT_TYPE_MISSING",
                        "trade_state": "TRADE_ACTIVE",
                        "mt5_ticket": _watch_ticket,
                        "device_id": _watch_device_id,
                        "rev_ok": False,
                        "zone": watch.get("zone"),
                        "planned_zone": watch.get(
                            "planned_zone"
                        ),
                        "zone_used": watch.get("zone_used"),
                        "rev_state": watch,
                    }
                _active_bp = _find_xtl_broker_position(
                    R=R,
                    device_id=_watch_device_id,
                    symbol=sym_u,
                    account_type=_watch_account_type,
                    ticket=(
                        _watch_ticket
                        if _watch_ticket > 0
                        else None
                    ),
                )

                _broker_active = bool(_active_bp)

                # ---------------------------------------------
                # Broker position still exists.
                # Keep the watch blocked as TRADE_ACTIVE and
                # do not allow normal zone/RC logic to continue.
                # ---------------------------------------------
                if _broker_active:
                    watch["state"] = "TRADE_ACTIVE"
                    watch["trade_state"] = "TRADE_ACTIVE"
                    watch["broker_position_present"] = True
                    watch["broker_deal_present"] = False
                    watch[
                        "broker_reconciliation_pending"
                    ] = False
                    watch["broker_active_seen_ms"] = int(
                        time.time() * 1000
                    )

                    return False, {
                        "blocked": True,
                        "stage": "TRADE_ACTIVE",
                        "reason": "SAME_SYMBOL_ACTIVE",
                        "trade_state": "TRADE_ACTIVE",
                        "broker_position_present": True,
                        "broker_deal_present": False,
                        "broker_reconciliation_pending": False,
                        "mt5_ticket": _watch_ticket,
                        "device_id": _watch_device_id,
                        "rev_ok": False,
                        "zone": watch.get("zone"),
                        "planned_zone": watch.get(
                            "planned_zone"
                        ),
                        "zone_used": watch.get("zone_used"),
                        "rev_state": watch,
                    }

                # ---------------------------------------------
                # Broker position is absent. Look for the MT5
                # deal record before deciding what to display.
                # ---------------------------------------------
                _ticket = _watch_ticket
                _deal_exists = False
                _deal_obj = {}

                if _ticket > 0:
                    try:
                        _deal_raw = R.get(
                            f"xtl:mt5:deal:{_ticket}"
                        )

                        if isinstance(
                            _deal_raw,
                            (bytes, bytearray),
                        ):
                            _deal_raw = _deal_raw.decode(
                                "utf-8",
                                "ignore",
                            )

                        _deal_obj = (
                            json.loads(_deal_raw)
                            if _deal_raw
                            else {}
                        )

                        _deal_exists = bool(
                            isinstance(_deal_obj, dict)
                            and _deal_obj.get("ok")
                        )

                    except Exception:
                        _deal_exists = False
                        _deal_obj = {}

                # ---------------------------------------------
                # Deal confirms broker close.
                # Mark reconciliation pending and stop here.
                # Executor remains responsible for:
                # closed ledger, PnL, risk release and cooldown.
                # ---------------------------------------------
                if _deal_exists:
                    _deal_reason = str(
                        _deal_obj.get("broker_reason")
                        or _deal_obj.get("exit_reason")
                        or "BROKER_CLOSED"
                    ).upper().strip()

                    try:
                        _deal_closed_at_ms = int(
                            _deal_obj.get("close_time_ms")
                            or _deal_obj.get("closed_at_ms")
                            or int(time.time() * 1000)
                        )
                    except Exception:
                        _deal_closed_at_ms = int(
                            time.time() * 1000
                        )

                    watch["state"] = (
                        "BROKER_CLOSE_PENDING"
                    )
                    watch["trade_state"] = (
                        "BROKER_CLOSE_PENDING"
                    )
                    watch["entry_triggered"] = False
                    watch[
                        "broker_position_present"
                    ] = False
                    watch["broker_deal_present"] = True
                    watch[
                        "broker_reconciliation_pending"
                    ] = True
                    watch[
                        "broker_close_reason"
                    ] = _deal_reason
                    watch[
                        "broker_closed_at_ms"
                    ] = _deal_closed_at_ms
                    watch["last_updated_ms"] = int(
                        time.time() * 1000
                    )

                    try:
                        R.set(
                            wkey,
                            json.dumps(
                                watch,
                                separators=(",", ":"),
                                default=str,
                            ),
                        )
                    except Exception:
                        log.exception(
                            "[WATCHLIST] "
                            "BROKER_CLOSE_PENDING_SAVE_FAILED "
                            "sym=%s side=%s key=%s "
                            "ticket=%s",
                            sym_u,
                            str(resolved_dir).upper(),
                            wkey,
                            _ticket,
                        )

                    log.warning(
                        "[WATCHLIST] "
                        "TRADE_ACTIVE_TO_"
                        "BROKER_CLOSE_PENDING "
                        "sym=%s side=%s key=%s "
                        "ticket=%s deal_reason=%s "
                        "closed_at_ms=%s device_id=%s "
                        "account_type=%s",
                        sym_u,
                        str(resolved_dir).upper(),
                        wkey,
                        _ticket,
                        _deal_reason,
                        _deal_closed_at_ms,
                        _watch_device_id,
                        _watch_account_type,
                    )

                    return False, {
                        "blocked": True,
                        "stage": "BROKER_CLOSE_PENDING",
                        "reason": (
                            "BROKER_DEAL_PENDING_"
                            "EXECUTOR_RECONCILIATION"
                        ),
                        "trade_state": (
                            "BROKER_CLOSE_PENDING"
                        ),
                        "broker_position_present": False,
                        "broker_deal_present": True,
                        "broker_reconciliation_pending": True,
                        "broker_close_reason": _deal_reason,
                        "broker_closed_at_ms": (
                            _deal_closed_at_ms
                        ),
                        "mt5_ticket": _ticket,
                        "device_id": _watch_device_id,
                        "rev_ok": False,
                        "zone": watch.get("zone"),
                        "planned_zone": watch.get(
                            "planned_zone"
                        ),
                        "zone_used": watch.get("zone_used"),
                        "rev_state": watch,
                    }

                # ---------------------------------------------
                # Broker position absent, but deal has not
                # arrived yet. Preserve the active lifecycle
                # temporarily and block all new RC processing.
                # ---------------------------------------------
                log.warning(
                    "[WATCHLIST] TRADE_ACTIVE_PRESERVED "
                    "sym=%s side=%s key=%s ticket=%s "
                    "broker_position=False "
                    "deal_exists=False device_id=%s "
                    "account_type=%s "
                    "reason=WAIT_EXECUTOR_RECON",
                    sym_u,
                    str(resolved_dir).upper(),
                    wkey,
                    _ticket,
                    _watch_device_id,
                    _watch_account_type,
                )

                return False, {
                    "blocked": True,
                    "stage": "TRADE_ACTIVE",
                    "reason": "WAIT_EXECUTOR_RECON",
                    "trade_state": "TRADE_ACTIVE",
                    "broker_position_present": False,
                    "broker_deal_present": False,
                    "broker_reconciliation_pending": True,
                    "mt5_ticket": _ticket,
                    "device_id": _watch_device_id,
                    "rev_ok": False,
                    "zone": watch.get("zone"),
                    "planned_zone": watch.get(
                        "planned_zone"
                    ),
                    "zone_used": watch.get("zone_used"),
                    "rev_state": watch,
                }

    except Exception as _e:
        log.exception(
            "[WATCHLIST] TRADE_ACTIVE_PRESERVE_EXC "
            "sym=%s side=%s key=%s err=%r",
            sym_u,
            str(resolved_dir).upper(),
            wkey,
            _e,
        )

        return False, {
            "blocked": True,
            "stage": "TRADE_ACTIVE_RECON_ERROR",
            "reason": "TRADE_ACTIVE_RECON_ERROR",
            "rev_ok": False,
            "zone": (
                watch.get("zone")
                if isinstance(watch, dict)
                else None
            ),
            "planned_zone": (
                watch.get("planned_zone")
                if isinstance(watch, dict)
                else None
            ),
            "zone_used": (
                watch.get("zone_used")
                if isinstance(watch, dict)
                else None
            ),
            "rev_state": (
                watch
                if isinstance(watch, dict)
                else None
            ),
        }
    
    # ------------------------------------------------------------
    # WATCH INTEGRITY REPAIR
    # If rev_ok and state are inconsistent (one set but not the other),
    # repair them so the REV_OK early-return lock fires correctly.
    # This prevents the gate from falling through and re-evaluating
    # on every tick after REV_OK was already confirmed.
    # ------------------------------------------------------------
    if isinstance(watch, dict):
        w_state = str(watch.get("state") or "").upper()
        w_rev_ok = bool(watch.get("rev_ok"))
        w_trade_state = str(watch.get("trade_state") or "").upper()
        w_missed = bool(watch.get("missed_breakout"))

        # ------------------------------------------------------------
        # Point-A terminal states.
        # ------------------------------------------------------------
        point_a_terminal_state = (
            w_state
            if w_state in (
                "ENTRY_BLOCKED_POINT_A_BLOCK",
                "ENTRY_BLOCKED_POINT_A_EXPIRED",
            )
            else w_trade_state
            if w_trade_state in (
                "ENTRY_BLOCKED_POINT_A_BLOCK",
                "ENTRY_BLOCKED_POINT_A_EXPIRED",
            )
            else ""
        )

        if point_a_terminal_state:
            # --------------------------------------------------------
            # P0 POINT-A TERMINAL STATE NORMALIZATION
            #
            # state / trade_state are redundant lifecycle fields.
            # If either says BLOCK/EXPIRED, persist one authoritative
            # terminal state so executor and gate cannot oscillate
            # between REV_OK and Point-A terminal state.
            # --------------------------------------------------------
            _terminal_changed = bool(
                w_state != point_a_terminal_state
                or w_trade_state != point_a_terminal_state
                or bool(watch.get("rev_ok"))
                or not bool(watch.get("entry_blocked"))
            )

            watch["state"] = point_a_terminal_state
            watch["trade_state"] = point_a_terminal_state
            watch["rev_ok"] = False
            watch["entry_ready"] = False
            watch["entry_triggered"] = False
            watch["entry_blocked"] = True

            if _terminal_changed:
                try:
                    R.set(
                        wkey,
                        json.dumps(
                            watch,
                            separators=(",", ":"),
                            default=str,
                        ),
                        ex=7 * 24 * 3600,
                    )

                    log.warning(
                        "[POINT_A] GATE_TERMINAL_STATE_NORMALIZED "
                        "sym=%s state=%s key=%s",
                        sym_u,
                        point_a_terminal_state,
                        wkey,
                    )

                except Exception:
                    log.exception(
                        "[POINT_A] GATE_TERMINAL_STATE_NORMALIZE_FAILED "
                        "sym=%s state=%s key=%s",
                        sym_u,
                        point_a_terminal_state,
                        wkey,
                    )
            gate["blocked"] = True
            gate["stage"] = (
                "POINT_A_BLOCKED"
                if point_a_terminal_state == "ENTRY_BLOCKED_POINT_A_BLOCK"
                else "POINT_A_EXPIRED"
            )
            gate["reason"] = str(
                watch.get("point_a_reason")
                or watch.get("entry_block_reason")
                or gate["stage"]
            )
            gate["watch_key"] = str(wkey)
            gate["watch_reused"] = True
            gate["resolved_dir"] = str(
                watch.get("direction")
                or resolved_dir
                or ""
            ).upper()
            gate["rev_ok"] = False
            gate["zone"] = dict(watch.get("zone_used") or {})
            gate["planned_zone"] = dict(watch.get("zone_used") or {})
            gate["zone_used"] = dict(watch.get("zone_used") or {})
            gate["rev_state"] = dict(watch)

            
            # --------------------------------------------------------
            # P0 POINT-A TERMINAL OWNERSHIP CONTRACT
            #
            # BLOCK and EXPIRED are both terminal for THIS frozen
            # direction / zone / RC-cross opportunity.
            #
            # They are retained here only as temporary fail-closed
            # tombstones while the executor completes terminal cleanup.
            #
            # IMPORTANT:
            #   - never restore REV_OK
            #   - never refresh/re-arm RC
            #   - never migrate to a newer RC on this frozen setup
            #   - never change direction inside this terminal watch
            #
            # Once executor cleanup removes this watch, the next gate
            # evaluation performs normal fresh discovery from current
            # price / SR structure.
            # --------------------------------------------------------
            watch.pop(
                "_point_a_terminal_rc_refresh",
                None,
            )

            gate["rev_state"] = dict(watch)

            return False, gate
        # ------------------------------------------------------------
        # Point-A WAIT presentation.
        #
        # Executor owns the WAIT lifecycle. The gate must preserve and
        # expose that state instead of repairing it back to REV_OK.
        # ------------------------------------------------------------
        point_a_wait_state = (
            w_state == "ENTRY_BLOCKED_POINT_A_WAIT"
            or w_trade_state == "ENTRY_BLOCKED_POINT_A_WAIT"
        )

        if point_a_wait_state:
            # --------------------------------------------------------
            # P0 POINT-A WAIT STATE NORMALIZATION
            #
            # An active WAIT owns its crossed opportunity.
            # Persist both lifecycle fields consistently and keep RC
            # confirmed while the executor re-evaluates Point-A.
            # --------------------------------------------------------
            _wait_changed = bool(
                w_state != "ENTRY_BLOCKED_POINT_A_WAIT"
                or w_trade_state != "ENTRY_BLOCKED_POINT_A_WAIT"
                or not bool(watch.get("rev_ok"))
                or not bool(watch.get("entry_blocked"))
            )

            watch["state"] = "ENTRY_BLOCKED_POINT_A_WAIT"
            watch["trade_state"] = "ENTRY_BLOCKED_POINT_A_WAIT"
            watch["rev_ok"] = True
            watch["entry_ready"] = False
            watch["entry_triggered"] = False
            watch["entry_blocked"] = True

            if _wait_changed:
                try:
                    R.set(
                        wkey,
                        json.dumps(
                            watch,
                            separators=(",", ":"),
                            default=str,
                        ),
                        ex=7 * 24 * 3600,
                    )

                    log.warning(
                        "[POINT_A] GATE_WAIT_STATE_NORMALIZED "
                        "sym=%s key=%s",
                        sym_u,
                        wkey,
                    )

                except Exception:
                    log.exception(
                        "[POINT_A] GATE_WAIT_STATE_NORMALIZE_FAILED "
                        "sym=%s key=%s",
                        sym_u,
                        wkey,
                    )
            _pa_reason = str(
                watch.get("point_a_reason")
                or watch.get("entry_block_reason")
                or "POINT_A_WAIT"
            )

            try:
                _pa_wait_bars = int(
                    watch.get("point_a_wait_m15_bars") or 0
                )
            except Exception:
                _pa_wait_bars = 0

            try:
                _pa_max_wait_bars = int(
                    watch.get("point_a_max_wait_m15_bars") or 16
                )
            except Exception:
                _pa_max_wait_bars = 16

            try:
                _pa_disp = float(
                    watch.get("point_a_displacement_atr") or 0.0
                )
            except Exception:
                _pa_disp = 0.0

            gate["blocked"] = True
            gate["stage"] = "POINT_A_WAIT"

            gate["reason"] = (
                f"POINT-A WAIT | {_pa_reason} "
                f"| M15 {_pa_wait_bars}/{_pa_max_wait_bars} "
                f"| DISP {_pa_disp:.2f} ATR"
            )

            gate["watch_key"] = str(wkey)
            gate["watch_reused"] = True

            gate["resolved_dir"] = str(
                watch.get("direction")
                or resolved_dir
                or ""
            ).upper()

            # RC itself remains valid while Point-A is waiting.
            gate["rev_ok"] = True

            gate["zone"] = dict(
                watch.get("zone_used") or {}
            )
            gate["planned_zone"] = dict(
                watch.get("zone_used") or {}
            )
            gate["zone_used"] = dict(
                watch.get("zone_used") or {}
            )

            gate["rev_state"] = dict(watch)

            return False, gate
        if (
            w_state in (
                "MISSED_BREAKOUT",
                "ORDER_FAILED",
                "ZONE_INVALIDATED",
            )
            or w_trade_state in (
                "MISSED_BREAKOUT",
                "ORDER_FAILED",
                "ZONE_INVALIDATED",
            )
            or w_missed
        ):
            terminal_state = (
                w_trade_state
                if w_trade_state in (
                    "MISSED_BREAKOUT",
                    "ORDER_FAILED",
                    "ZONE_INVALIDATED",
                )
                else w_state
                if w_state in (
                    "MISSED_BREAKOUT",
                    "ORDER_FAILED",
                    "ZONE_INVALIDATED",
                )
                else "MISSED_BREAKOUT"
            )

            watch["state"] = terminal_state
            watch["trade_state"] = terminal_state

            try:
                R.set(
                    wkey,
                    json.dumps(
                        watch,
                        separators=(",", ":"),
                    ),
                    ex=7 * 24 * 3600,
                )
            except Exception:
                pass

            gate["blocked"] = True
            gate["stage"] = terminal_state
            gate["reason"] = str(
                watch.get("missed_breakout_reason")
                or terminal_state
            )
            gate["watch_key"] = str(wkey)
            gate["watch_reused"] = True
            gate["resolved_dir"] = str(
                watch.get("direction")
                or resolved_dir
                or ""
            ).upper()
            gate["zone"] = None
            gate["planned_zone"] = None
            gate["zone_used"] = (
                dict(watch.get("zone_used"))
                if isinstance(
                    watch.get("zone_used"),
                    dict,
                )
                else None
            )
            gate["rev_state"] = dict(watch)

            return False, gate

        _point_a_terminal_rc_refresh = bool(
            isinstance(watch, dict)
            and watch.get("_point_a_terminal_rc_refresh")
        )

        if (
            not _point_a_terminal_rc_refresh
            and w_rev_ok
            and w_state != "REV_OK"
        ):
            watch["state"] = "REV_OK"

        if (
            not _point_a_terminal_rc_refresh
            and w_state == "REV_OK"
            and not w_rev_ok
        ):
            watch["rev_ok"] = True

        # Also ensure rev_ok_bar_hi/lo exist if state is REV_OK.
        # Do not touch the old expired RC while terminal re-arm scanning is active.
        if (
            not _point_a_terminal_rc_refresh
            and (w_state == "REV_OK" or w_rev_ok)
        ):
            if not watch.get("rev_ok_bar_hi") and watch.get("last_checked_high"):
                watch["rev_ok_bar_hi"] = float(watch["last_checked_high"])

            if not watch.get("rev_ok_bar_lo") and watch.get("last_checked_low"):
                watch["rev_ok_bar_lo"] = float(watch["last_checked_low"])
        # ------------------------------------------------------------
        # RC CANDLE VALIDITY CHECK
        # If watch is in REV_OK state but the RC candle close time
        # is in the future (forming candle was incorrectly used as RC),
        # auto-repair: roll back to REV_WATCH, clear only RC fields.
        # Keep zone_used, started_ms, direction — zone freeze is valid.
        # ------------------------------------------------------------
        if w_rev_ok or w_state == "REV_OK":
            rev_ok_ms = int(watch.get("rev_ok_ms") or 0)
            # Validate RC against latest completed broker candle, not server now_ms_pick.
            # Broker candle timestamps can be ahead of server clock. If rev_ok_ms is <=
            # latest completed candle close_ms, RC is valid.
            latest_complete_close_ms = 0
            try:
                for _b in (bars or []):
                    if isinstance(_b, dict) and bool(_b.get("complete")) is True:
                        _open_ms = _to_ms_any(_b.get("t_open_ms") or _b.get("tOpenMs") or _b.get("t"))
                        if _open_ms and _open_ms < 10_000_000_000:
                            _open_ms *= 1000
                        _close_ms = _to_ms_any(_b.get("t_close_ms") or _b.get("tCloseMs") or _b.get("t_close"))
                        if (
                            _open_ms > 0
                            and _close_ms <= _open_ms
                        ):
                            _close_ms = int(_open_ms + tf_ms)
                        latest_complete_close_ms = max(int(latest_complete_close_ms or 0), int(_close_ms or 0))
            except Exception:
                latest_complete_close_ms = int(closed_ms or 0)

            if rev_ok_ms > 0 and latest_complete_close_ms > 0 and rev_ok_ms > latest_complete_close_ms:
                # RC candle close time is in the future — forming candle was used
                # Roll back to REV_WATCH cleanly, clearing EVERY field the
                # confirmation path wrote (not just rev_ok_bar_*), so no
                # half-armed REV_WATCH state with a live trigger_level
                # survives. None-valued fields are dropped by the
                # `if v is not None` persist filter below.
                watch["state"] = "REV_WATCH"
                watch["rev_ok"] = False
                watch["rev_ok_ms"] = 0
                for _rc_k in (
                    "rev_ok_bar_hi", "rev_ok_bar_lo", "rev_ok_bar_close",
                    "rc_open_ms", "rc_close_ms",
                    "rc_high", "rc_low", "rc_close",
                    "trigger_level", "rc_is_touch_candle",
                ):
                    watch[_rc_k] = None
                watch.pop("discord_rc_trigger_sent", None)
                watch.pop("discord_rc_trigger_sent_ms", None)
                watch.pop("discord_rc_trigger_price", None)
                watch.pop("discord_rc_trigger_error", None)
                # Persist the rollback immediately
                try:
                    zone_watch_set(
                        R,
                        uid_u,
                        sym_u,
                        str(watch.get("direction") or resolved_dir).upper(),
                        json.dumps(
                            {k: v for k, v in watch.items() if v is not None},
                            separators=(",", ":"),
                        ),
                        tf=tfu,
                        ex=7 * 24 * 3600
                    )
                except Exception:
                    pass
                if debug_gate:
                    gate["dbg_rc_rollback"] = {
                        "reason": "forming_candle_used_as_rc",
                        "rev_ok_ms": rev_ok_ms,
                        "now_ms_pick": int(now_ms_pick or 0),
                        "latest_complete_close_ms": int(latest_complete_close_ms or 0),
                        "rolled_back_to": "REV_WATCH",
                    }   
                w_rev_ok = False
                w_state = "REV_WATCH"  

        # ------------------------------------------------------------
        # TERMINAL WATCH STATE: MISSED_BREAKOUT
        # Keep the watch visible for UI/audit. Do not rediscover,
        # do not re-evaluate REV_OK, and do not allow executor entry.
        # Manual cleanup can delete the watch key after review.
        # ------------------------------------------------------------
        if str(watch.get("state") or "").upper() == "MISSED_BREAKOUT":
            gate["blocked"] = True
            gate["stage"] = "MISSED_BREAKOUT"
            gate["reason"] = (
                f"MISSED_BREAKOUT | trigger={float(watch.get('missed_breakout_trigger_level') or 0):.5f} "
                f"| live={float(watch.get('missed_breakout_live_price') or 0):.5f} "
                f"| no fresh cross"
            )
            gate["watch_key"] = str(wkey)
            gate["watch_reused"] = True
            gate["resolved_dir"] = str(watch.get("direction") or resolved_dir or "").upper()
            gate["zone"] = dict(watch.get("zone_used")) if isinstance(watch.get("zone_used"), dict) else None
            gate["zone_used"] = dict(watch.get("zone_used")) if isinstance(watch.get("zone_used"), dict) else None
            gate["rev_state"] = dict(watch)
            return False, gate

    # Normalize frozen zone_used: old watches may be level-only.
    # Priority: rehydrate from SR -> else synthesize via _level_to_zone (never collapse).
    if isinstance(zone_used, dict):
        # parse level once
        try:
            lvl0 = float(zone_used.get("level")) if zone_used.get("level") is not None else None
        except Exception:
            lvl0 = None

        def _rehydrate_band_from_sr_level(sr_all: dict, tf_tag: str, kind: str, lvl: float):
            if not isinstance(sr_all, dict) or lvl is None:
                return None

            tfk = str(tf_tag or "H1").lower()
            tf_obj = sr_all.get(tfk) if isinstance(sr_all.get(tfk), dict) else None
            if not isinstance(tf_obj, dict):
                return None

            if kind == "support":
                cand = (tf_obj.get("supports") or []) + (tf_obj.get("supports_major") or []) + (tf_obj.get("supports_near") or [])
            else:
                cand = (tf_obj.get("resistances") or []) + (tf_obj.get("resistances_major") or []) + (tf_obj.get("resistances_near") or [])

            best = None
            best_d = 1e18
            for r in cand:
                if not isinstance(r, dict):
                    continue
                try:
                    lv = float(r.get("level"))
                    d = abs(lv - float(lvl))
                except Exception:
                    continue
                if d < best_d:
                    best_d = d
                    best = r

            if isinstance(best, dict):
                lo = best.get("low")
                hi = best.get("high")
                try:
                    if lo is not None and hi is not None and float(lo) < float(hi):
                        return float(lo), float(hi)
                except Exception:
                    return None
            return None

        # do we need a band repair?
        # FREEZE RULE: if zone_used already has a valid low/high band, NEVER touch it.
        # Only repair truly missing or collapsed bands (legacy watches).
        need_band = False
        try:
            zl = zone_used.get("low")
            zh = zone_used.get("high")
            if zl is None or zh is None or float(zl) >= float(zh):
                need_band = True
            # Valid band exists — lock it, do not rehydrate from SR
            else:
                need_band = False
        except Exception:
            need_band = True

        # 1) try rehydrate from SR — ONLY for legacy watches missing a band
        if need_band and lvl0 is not None:
            kind0 = str(zone_used.get("kind") or ("support" if resolved_dir == "BUY" else "resistance")).lower()
            band = _rehydrate_band_from_sr_level(sr or {}, tfu, kind0, float(lvl0))
            if band is not None:
                zone_used["low"] = float(band[0])
                zone_used["high"] = float(band[1])
                need_band = False

        # 2) if still missing/collapsed, synthesize from ATR (last resort only)
        if need_band and lvl0 is not None:
            try:
                ztmp = _level_to_zone(float(lvl0), tfu, sym_u, float(atr))
                zone_used["low"] = float(ztmp["low"])
                zone_used["high"] = float(ztmp["high"])
            except Exception:
                pass


    # ------------------------------------------------------------
    # 2) if watch exists -> freeze zone_used (DO NOT re-pick zone)
    # ------------------------------------------------------------
    if isinstance(zone_used, dict) and zone_used.get("level") is not None:
        zone = dict(zone_used)
        gate["zone"] = dict(zone_used)
        gate["zone_used"] = dict(zone_used)
    else:
        zone = None

    # ------------------------------------------------------------
    # HARD STOP: active MT5 trade must not enter rediscovery/DIST_GUARD.
    # This is the ONLY gate-level TRADE_ACTIVE return path.
    # Zone is preserved from watch/zone_used, not overwritten by broker guard.
    # ------------------------------------------------------------
    try:
        _trade_state = str((watch or {}).get("trade_state") or "").upper()
        _state = str((watch or {}).get("state") or "").upper()

        if _trade_state == "TRADE_ACTIVE" or _state == "TRADE_ACTIVE":
            _zu = (
                (watch or {}).get("zone_used")
                or (watch or {}).get("planned_zone")
                or (zone_used if isinstance(zone_used, dict) else None)
                or (zone if isinstance(zone, dict) else None)
            )

            gate["blocked"] = False
            gate["reason"] = "TRADE_ACTIVE"
            gate["stage"] = "MANAGE_TRADE"
            gate["trade_state"] = "TRADE_ACTIVE"
            gate["resolved_dir"] = str((watch or {}).get("direction") or resolved_dir or "").upper()
            gate["watch_key"] = str(wkey or "")
            gate["watch_reused"] = True

            gate["zone"] = dict(_zu) if isinstance(_zu, dict) else None
            gate["zone_used"] = dict(_zu) if isinstance(_zu, dict) else None
            gate["planned_zone"] = dict(_zu) if isinstance(_zu, dict) else None

            gate["entry_triggered"] = bool((watch or {}).get("entry_triggered"))
            gate["entry_price"] = (watch or {}).get("entry_price")
            gate["entry_ts_ms"] = (watch or {}).get("entry_ts_ms")
            gate["mt5_job_id"] = (watch or {}).get("mt5_job_id")
            gate["mt5_ticket"] = (watch or {}).get("mt5_ticket") or (watch or {}).get("broker_ticket")
            gate["broker_ticket"] = (watch or {}).get("broker_ticket") or (watch or {}).get("mt5_ticket")
            gate["rev_state"] = dict(watch or {})

            return True, gate
    except Exception:
        pass
    # Persist repaired band ONLY when we truly repaired a legacy watch (once)
    try:
        did_repair = False

        # only if watch existed and had zone_used originally
        if isinstance(watch, dict) and isinstance(watch.get("zone_used"), dict) and isinstance(zone_used, dict):
            old = watch.get("zone_used") or {}

            old_lo = old.get("low")
            old_hi = old.get("high")

            # legacy = missing or collapsed
            legacy = False
            try:
                if old_lo is None or old_hi is None or float(old_lo) >= float(old_hi):
                    legacy = True
            except Exception:
                legacy = True

            # new = real band
            new_ok = False
            try:
                zl = zone_used.get("low")
                zh = zone_used.get("high")
                if zl is not None and zh is not None and float(zl) < float(zh):
                    new_ok = True
            except Exception:
                new_ok = False

            if legacy and new_ok:
                watch["zone_used"] = dict(zone_used)
                zone_watch_set(
                    R,
                    uid_u,
                    sym_u,
                    str(watch.get("direction") or resolved_dir).upper(),
                    json.dumps(watch, separators=(",", ":")),
                    tf=tfu,
                    ex=7 * 24 * 3600,
                )
                did_repair = True

        if debug_gate:
            gate["dbg_watch_band_repaired"] = bool(did_repair)
    except Exception:
        pass


    # ------------------------------------------------------------
    # FALLBACK: watch key missing but open registry has active trade.
    # Read only this authenticated user's open registry and match ownership.
    # ------------------------------------------------------------
    try:
        if (
            not isinstance(watch, dict)
            or str(
                watch.get("trade_state") or ""
            ).upper() != "TRADE_ACTIVE"
        ):
            #
            # Ownership isolation:
            # read only this authenticated user's open-trade ledger.
            # Never scan xtl:strategy:oppt:open:* globally.
            #
            open_key = (
                f"xtl:strategy:oppt:open:{uid_u}"
            )

            open_map = R.hgetall(open_key) or {}

            gate_profile = str(
                _kwargs.get("profile_id") or ""
            ).strip().lower()

            gate_device = (
                str(x_device_id or "").strip()
                or str(pinned_device or "").strip()
            )

            for _k, _v in open_map.items():
                try:
                    if isinstance(
                        _v,
                        (bytes, bytearray),
                    ):
                        _v = _v.decode(
                            "utf-8",
                            "ignore",
                        )

                    tr = (
                        json.loads(_v)
                        if isinstance(_v, str)
                        else _v
                    )

                    if not isinstance(tr, dict):
                        continue

                    #
                    # Optional profile isolation.
                    # Enforce it whenever the caller supplied profile_id.
                    #
                    trade_profile = str(
                        tr.get("profile_id") or ""
                    ).strip().lower()

                    if (
                        gate_profile
                        and trade_profile != gate_profile
                    ):
                        continue

                    #
                    # Device isolation.
                    # A trade from another MT5 device/account must not
                    # become TRADE_ACTIVE in this gate.
                    #
                    trade_device = str(
                        tr.get("device_id") or ""
                    ).strip()

                    if (
                        gate_device
                        and trade_device
                        and trade_device != gate_device
                    ):
                        continue

                    if (
                        str(
                            tr.get("symbol") or ""
                        ).upper().strip()
                        != sym_u
                    ):
                        continue

                    if (
                        str(
                            tr.get("trade_state") or ""
                        ).upper().strip()
                        != "TRADE_ACTIVE"
                    ):
                        continue

                    _side = str(
                        tr.get("side") or ""
                    ).upper().strip()

                    if _side not in ("BUY", "SELL"):
                        continue

                    _zu = tr.get("entry_zone")

                    if not isinstance(_zu, dict):
                        _zu = {
                            "level": tr.get(
                                "entry_zone_level"
                            ),
                            "low": tr.get(
                                "entry_zone_low"
                            ),
                            "high": tr.get(
                                "entry_zone_high"
                            ),
                            "tf": (
                                tr.get("entry_zone_tf")
                                or tfu
                            ),
                            "kind": tr.get(
                                "entry_zone_kind"
                            ),
                        }

                    gate["blocked"] = False
                    gate["reason"] = "TRADE_ACTIVE"
                    gate["stage"] = "MANAGE_TRADE"
                    gate["trade_state"] = (
                        "TRADE_ACTIVE"
                    )
                    gate["resolved_dir"] = _side

                    gate["zone"] = (
                        dict(_zu)
                        if isinstance(_zu, dict)
                        else None
                    )
                    gate["zone_used"] = (
                        dict(_zu)
                        if isinstance(_zu, dict)
                        else None
                    )
                    gate["planned_zone"] = (
                        dict(_zu)
                        if isinstance(_zu, dict)
                        else None
                    )

                    gate["entry_triggered"] = True
                    gate["entry_price"] = tr.get(
                        "entry_price"
                    )
                    gate["entry_ts_ms"] = tr.get(
                        "opened_at_ms"
                    )
                    gate["mt5_job_id"] = tr.get(
                        "mt5_job_id"
                    )
                    gate["mt5_ticket"] = (
                        tr.get("mt5_ticket")
                        or tr.get("broker_ticket")
                    )
                    gate["broker_ticket"] = (
                        tr.get("broker_ticket")
                        or tr.get("mt5_ticket")
                    )
                    gate["rev_state"] = dict(tr)

                    return True, gate

                except Exception:
                    continue

    except Exception:
        log.exception(
            "[WATCHLIST] OPEN_REGISTRY_FALLBACK_FAILED "
            "uid=%s profile=%s device=%s sym=%s",
            uid_u,
            str(
                _kwargs.get("profile_id") or ""
            ).strip().lower(),
            (
                str(x_device_id or "").strip()
                or str(pinned_device or "").strip()
            ),
            sym_u,
        )
    
    
    # ------------------------------------------------------------
    # 3) pick zone from SR ONLY if no frozen zone
    # ------------------------------------------------------------
    # If watch exists with frozen zone_used, DO NOT re-pick (prevents moving-zone + tap drift)
    if isinstance(watch, dict) and isinstance(watch.get("zone_used"), dict) and watch.get("zone_used", {}).get("level") is not None:
        zone = dict(watch["zone_used"])
    else:
        # help SR picker know the symbol (for FX cap widening)
        if isinstance(sr, dict) and "symbol" not in sr:
            try:
                sr["symbol"] = sym_u
            except Exception:
                pass
        if resolved_dir == "WATCHING":
            gate["reason"] = "WATCHING_NO_NEAR_MAJOR_SR"
            gate["stage"] = "DIRECTION_RESOLVE"
            gate["blocked"] = False

            # show next valid zones, but do not trade
            # No actionable near/major SR.
            # Do NOT expose far/display zone as executable gate zone.
            gate["zone"] = None
            gate["planned_zone"] = None
            gate["zone_used"] = None

            # Optional display-only fields for UI/debug, never used by executor.
            # Optional display-only fields for UI/debug, never used by executor.
            # Direction-aware: never show opposite-side zone in strategy row.
            if dir_u == "BUY":
                gate["display_zone"] = (
                    display_zones.get("h1_buy_zone")
                    or display_zones.get("h4_buy_zone")
                )
            elif dir_u == "SELL":
                gate["display_zone"] = (
                    display_zones.get("h1_sell_zone")
                    or display_zones.get("h4_sell_zone")
                )
            else:
                gate["display_zone"] = None

            return False, gate
        if isinstance(preferred_zone, dict) and preferred_zone.get("level") is not None:
            zone = dict(preferred_zone)
            # PHASE-1 SAFETY:
            # Do not execute H4 zone with H1 reversal candle.
            # H4 zones are display/watch only until H4 candle execution is implemented.
            if str(zone.get("tf") or "").upper() == "H4":
                gate["reason"] = "WATCHING_NO_H1_EXECUTION_ZONE"
                gate["stage"] = "ZONE_PICK"
                gate["blocked"] = False

                # H4 is display-only in Phase 1.
                # Never expose H4 as executable zone/planned_zone.
                gate["zone"] = None
                gate["planned_zone"] = None
                gate["zone_used"] = None
                gate["display_zone"] = dict(zone)

                gate["resolved_dir"] = "WATCHING"
                return False, gate
        else:
            gate["reason"] = "WATCHING_NO_NEAR_ACTIONABLE_ZONE"
            gate["stage"] = "ZONE_PICK"
            gate["blocked"] = False
            gate["zone"] = None
            gate["planned_zone"] = None
            gate["zone_used"] = None
            gate["resolved_dir"] = "WATCHING"
            return False, gate

    if not isinstance(zone, dict) or zone.get("level") is None:
        # If a watch exists, never hard-fail with no_buy/no_sell.
        # Continue WATCH; rely on frozen zone (or wait for SR refresh) and invalidation rules.
        if isinstance(watch, dict):
            gate["reason"] = "WATCH_ZONE_MISSING"
            gate["stage"] = "ZONE_PICK"
            gate["blocked"] = False
            gate["zone"] = None
            gate["zone_used"] = (watch.get("zone_used") if isinstance(watch.get("zone_used"), dict) else None)
            gate["rev_state"] = watch
            return False, gate

        # SR exists but zone selection failed (often because nearest zone is too far by filters).
        # Prefer reporting distance to nearest SR instead of "no_buy_support_below_price".
        nearest = None
        try:
            sym_u2 = str(sym_u or "").upper().strip()
            pip_factor = 0.01 if sym_u2 == "XAUUSD" else (0.01 if sym_u2.endswith("JPY") else 0.0001)

            nearest = _nearest_levels_from_sr(
                sr or {},
                float(cl),
                float(atr),
                pip_factor=float(pip_factor),
                cross_buf=0.0,
            )
        except Exception:
            nearest = None

        lvl = None
        if isinstance(nearest, dict):
            lvl = nearest.get("nearest_support") if resolved_dir == "BUY" else nearest.get("nearest_resistance")

        if isinstance(lvl, (int, float)) and float(lvl) > 0:
            dist_far = abs(float(cl) - float(lvl))
            dist_far_atr = (dist_far / float(atr)) if float(atr) > 0 else None

            gate["reason"] = "WAIT_ZONE_TOUCH"
            gate["stage"] = "ZONE_FAR"
            gate["blocked"] = False
            planned_zone = _level_to_zone(
                float(lvl),
                tfu,
                sym_u,
                float(atr),
            )

            planned_zone["source"] = "nearest_major_fallback"
            planned_zone["kind"] = "support" if resolved_dir == "BUY" else "resistance"

            gate["zone"] = planned_zone
            gate["planned_zone"] = planned_zone
            gate["zone_used"] = None

            gate["nearest_level"] = float(lvl)
            gate["dist"] = float(dist_far)
            gate["dist_atr"] = float(dist_far_atr) if dist_far_atr is not None else None
            gate["nearest"] = nearest
            return False, gate

        gate["reason"] = (
            "no_buy_support_below_price"
            if resolved_dir == "BUY"
            else "no_sell_resistance_above_price"
        )
        gate["stage"] = "ZONE_PICK"
        gate["zone"] = None
        gate["zone_used"] = None
        return False, gate

    
    # distance info ONLY (do not gate / do not return)
    # distance from zone using BOTH live price and last closed candle range
    dist_live = _zone_band_dist(zone, float(decision_px))

    try:
       zl_tmp = float(zone.get("low") if zone.get("low") is not None else zone.get("level"))
       zh_tmp = float(zone.get("high") if zone.get("high") is not None else zone.get("level"))
       if zl_tmp > zh_tmp:
           zl_tmp, zh_tmp = zh_tmp, zl_tmp

       # If last closed candle touched/entered zone, distance is actionable
       candle_touched_zone = bool(float(lo) <= zh_tmp and float(hi) >= zl_tmp)

       if candle_touched_zone:
           dist = 0.0
       else:
           dist = dist_live
    except Exception:
        dist = dist_live

    if dist is None:
        dist = abs(float(decision_px) - float(zone.get("level")))

    sym_u2 = str(sym_u or "").upper().strip()
    zone_tf = str(zone.get("tf") or "").upper()

    # hard maximum actionable distance from zone BAND
    if sym_u2 == "XAUUSD":
        hard_cap = 12.0
    elif sym_u2.endswith("JPY"):
        hard_cap = 0.25
    else:
        hard_cap = 0.0025  # 25 pips

    # minimum tolerance so price inside/near zone is not rejected
    if sym_u2 == "XAUUSD":
        min_cap = 3.0
    elif sym_u2.endswith("JPY"):
        min_cap = 0.08
    else:
        min_cap = 0.0008  # 8 pips

    max_dist = min(max(float(move_away_atr) * float(atr), min_cap), hard_cap)
    eps = 0.02 * float(atr)

    gate["hard_cap"] = float(hard_cap)
    gate["min_cap"] = float(min_cap)
    gate["zone_tf"] = zone_tf
    gate["dist_gate_model"] = "ZONE_BAND_ACTIONABLE_DISTANCE_CAP"

    # Protect CONFIRMED/ACTIVE states from the too-far reset — these zones
    # are held until invalidation/confirmation, never reset on distance.
    # Unconfirmed WATCH/REV_WATCH are NOT protected: a zone price has fled
    # far from, before any confirmation, should reset for rediscovery.
    _frozen_state = False
    if isinstance(watch, dict):
        _wst = str(watch.get("state") or "").upper()
        _frozen_state = _wst in (
            "REV_OK", "ENTRY_READY", "ORDER_PENDING", "TRADE_ACTIVE",
        )

    if dist > (max_dist + eps) and not _frozen_state:
        # ------------------------------------------------------------
        # FAR-ZONE DISCOVERY RESET:
        # Runs only when no CONFIRMED/ACTIVE watch exists. Clears the far
        # watch (incl. unconfirmed WATCH/REV_WATCH) and allows fresh
        # nearest H1 zone discovery on next cycle.
        # ------------------------------------------------------------
        try:
            if isinstance(watch, dict):
                _st_del = str(watch.get("state") or "").upper().strip()
                if not _is_protected_watch_state(_st_del):
                    zone_watch_delete(
                        R,
                        uid_u,
                        sym_u,
                        str(watch.get("direction") or resolved_dir).upper(),
                        tf=tfu,
                    )
                else:
                    if debug_gate:
                        gate["dbg_delete_skipped_protected_watch"] = {
                            "key": str(wkey),
                            "state": _st_del,
                        }
        except Exception:
            pass
        gate["reason"] = "ZONE_TOO_FAR_RESET_FOR_REDISCOVERY"
        gate["stage"] = "DIST_GUARD"
        gate["blocked"] = False
        gate["zone"] = dict(zone)
        gate["planned_zone"] = dict(zone)
        gate["zone_used"] = None
        gate["dist"] = float(dist)
        gate["max_dist"] = float(max_dist)
        gate["over"] = float(dist - max_dist)
        gate["eps"] = float(eps)
        gate["dist_atr"] = float(dist / float(atr)) if float(atr) > 0 else None
        gate["rediscovery_required"] = True
        # hard reset stale zone/watch — but NEVER reset a frozen watch
        try:
            _st = str((watch or {}).get("state") or "").upper()
            # Also check raw Redis key — watch may be loaded without zone_used
            _raw_watch = None
            try:
                if wkey:
                    _raw = R.get(str(wkey))
                    if _raw:
                        _raw_watch = json.loads(_raw) if isinstance(_raw, str) else _raw
                        if isinstance(_raw_watch, dict) and not _st:
                            _st = str(_raw_watch.get("state") or "").upper()
            except Exception:
                pass
            _has_frozen = _st in ("WATCH", "REV_WATCH", "REV_OK", "ENTRY_READY", "ORDER_PENDING", "TRADE_ACTIVE")

            if not _has_frozen:
                gate["zone"] = None
                gate["planned_zone"] = None
                gate["zone_used"] = None
                gate["resolved_dir"] = "WATCHING"
                gate["h1_buy_zone"] = None
                gate["h4_buy_zone"] = None
                gate["h1_sell_zone"] = None
                gate["h4_sell_zone"] = None
                gate["rev_state"] = None
                gate["rev_basis"] = None
                gate["touch_basis"] = None
                if wkey:
                    try:
                        for _s in ("BUY", "SELL"):
                            for _t in ("H1", "H4"):
                                _dk = _watch_key(
                                    uid_u,
                                    sym_u,
                                    _s,
                                    _t,
                                )
                                _dj = _json_load(R.get(_dk)) or {}
                                _dst = str(_dj.get("state") or "").upper().strip() if isinstance(_dj, dict) else ""
                                if not _is_protected_watch_state(_dst):
                                    R.delete(_dk)
                                else:
                                    if debug_gate:
                                        gate.setdefault("dbg_delete_skipped_protected_watches", []).append({
                                            "key": _dk,
                                            "state": _dst,
                                        })
                    except Exception:
                        pass
            else:
                # frozen watch active — preserve zone and direction
                gate["zone"] = dict(watch.get("zone_used") or {})
                gate["zone_used"] = dict(watch.get("zone_used") or {})
                gate["planned_zone"] = dict(watch.get("zone_used") or {})
                gate["resolved_dir"] = str((watch or {}).get("direction") or "WATCHING").upper()
                gate["rev_state"] = watch
        except Exception:
            pass
        return False, gate
    # Optional: keep visibility that we were near the threshold
    gate["dist"] = float(dist)
    gate["max_dist"] = float(max_dist)
    gate["over"] = float(dist - max_dist)
    gate["eps"] = float(eps)

    gate["dist_info_only"] = True

    # ------------------------------------------------------------
    # 3.9) closed_ms for the CURRENT closed bar (needed for WATCH start)
    # ------------------------------------------------------------
    closed_ms = 0
    try:
        # Prefer explicit close-time if present
        closed_ms = _to_ms_any(c.get("t_close_ms") or c.get("tCloseMs") or c.get("t_close"))
        if not closed_ms:
            # Fallback: open-time + tf_ms
            t_open_ms = _to_ms_any(c.get("t_open_ms") or c.get("ts_ms") or c.get("t"))
            closed_ms = int(t_open_ms + tf_ms) if t_open_ms else 0
    except Exception:
        closed_ms = 0





    # ------------------------------------------------------------
    # 4) interaction rule (touch detection)
    # ------------------------------------------------------------
    zl = float(zone.get("low") or zone["level"])
    zh = float(zone.get("high") or zone["level"])

    try:
        px_live = float(live_px) if live_px is not None else float(cl)
    except Exception:
        px_live = float(cl)

    try:
        z_level = float(zone.get("level") or 0.0)
    except Exception:
        z_level = float(zone["level"])

    # Touch condition must prove price actually entered the frozen zone.
    # BUY support touch:
    #   - current/last candle low <= zone_high, OR live price is inside/below zone_high
    # SELL resistance touch:
    #   - current/last candle high >= zone_low, OR live price is inside/above zone_low
    #
    # IMPORTANT:
    # Do not freeze just because direction resolved near a zone.
    # REV_OK must never happen before actual touch.
    try:
        _candle_touched_buy = bool(float(lo) <= float(zh) and float(hi) >= float(zl))
        _candle_touched_sell = bool(float(hi) >= float(zl) and float(lo) <= float(zh))
    except Exception:
        _candle_touched_buy = False
        _candle_touched_sell = False

    try:
        _live_inside_zone = bool(float(zl) <= float(px_live) <= float(zh))
    except Exception:
        _live_inside_zone = False

    # STRICT H1 TOUCH + LIVE TOUCH FREEZE:
    # - CLOSED H1 candle range touch is accepted.
    # - LIVE price inside zone is also accepted so REV_WATCH starts immediately.
    # - REV_OK still waits for closed H1 candle later.
    try:
        _closed_bar_touched = bool(
            float(lo) <= float(zh)
            and float(hi) >= float(zl)
        )
    except Exception:
        _closed_bar_touched = False

    try:
        _live_touched = bool(
            float(zl) <= float(px_live) <= float(zh)
        )
    except Exception:
        _live_touched = False

    touched = bool(_closed_bar_touched or _live_touched)

    if debug_gate:
        gate["touch_basis"] = {
            "live_px": float(px_live),
            "cl": float(cl),
            "lo": float(lo),
            "hi": float(hi),
            "zone_low": zl,
            "zone_high": zh,
            "zone_level": float(zone.get("level") or 0.0),
            "closed_bar_touched": bool(_closed_bar_touched),
            "live_touched": bool(_live_touched),
            "touched_now": bool(touched),
            "touch_method": "CLOSED_BAR_OR_LIVE_PRICE_VS_ZONE_BOUNDARIES",
        }

        if gate.get("dbg_h1_bars_n") is not None:
            gate["dbg_gate_h1_snap_bars_n"] = gate.get("dbg_h1_bars_n")
        if gate.get("dbg_h1_snap_serverNow") is not None:
            gate["dbg_gate_h1_snap_serverNow"] = gate.get("dbg_h1_snap_serverNow")

    # ------------------------------------------------------------
    # 5) start watch if not started yet
    # ------------------------------------------------------------
    # FREEZE GUARD: never re-enter the freeze block if a watch already exists.
    # If watch.started_ms is set, the zone was already frozen in a previous tick.
    # Re-entering would reset started_ms and wipe the invalidation clock.
    _watch_already_started = (
        isinstance(watch, dict)
        and bool(watch.get("started_ms"))
        and isinstance(watch.get("zone_used"), dict)
    )
    if zone_used is None and not _watch_already_started:
        if not touched:
            gate["reason"] = "WAIT_ZONE_TOUCH"
            gate["stage"] = "TOUCH"
            gate["blocked"] = False
           
            # show planned major zone before touch
            gate["zone"] = dict(zone)
            gate["planned_zone"] = dict(zone)
            gate["zone_used"] = None
            return False, gate

        zone_used = dict(zone)
        

        # Ensure boundaries exist (SR zones may be level-only)
        try:
            if isinstance(zone_used, dict):
                lvl0 = float(zone_used.get("level") or 0.0)
                zl0 = zone_used.get("low")
                zh0 = zone_used.get("high")
                if zl0 is None or zh0 is None or float(zl0) >= float(zh0):
                    ztmp = _level_to_zone(lvl0, tfu, sym_u, float(atr))
                    zone_used["low"] = float(ztmp["low"])
                    zone_used["high"] = float(ztmp["high"])
        except Exception:
            pass

        try:
            import time as _t2
            _sys_now_touch = int(_t2.time() * 1000)
            _forming_open_ms = 0
            try:
                if isinstance(bars, list) and bars:
                    for _fb in reversed(bars):
                        _fb_t = _to_ms_any(
                            _fb.get("t_open_ms") or _fb.get("tOpenMs") or
                            _fb.get("open_time_ms") or _fb.get("t") or 0
                        )
                        if _fb_t and int(_fb_t) > 0 and int(_fb_t) <= _sys_now_touch:
                            _forming_open_ms = int(_fb_t)
                            break
            except Exception:
                _forming_open_ms = 0
            if _forming_open_ms > 0:
                touch_open_ms  = int(_forming_open_ms)
            else:
                touch_open_ms  = int((_sys_now_touch // tf_ms) * tf_ms)
            touch_close_ms = int(touch_open_ms + tf_ms)
        except Exception:
            touch_open_ms  = int((int(now_ms or now_ms_pick) // tf_ms) * tf_ms)
            touch_close_ms = int(touch_open_ms + tf_ms)

        # Extract touch candle open_ms from the closed bar for precise RC boundary.
        # RC candle must have opened at or after touch_candle_open_ms.
        # This rejects any candle already forming when the zone was frozen
        # (including the big drop/touch candle itself).
        _touch_bar_open_ms = 0
        try:
            for _ok in ("t_open_ms", "tOpenMs", "open_time_ms", "ts_ms", "t", "time", "ts"):
                _v = _to_ms_any((c or {}).get(_ok))
                if _v and int(_v) > 0:
                    _touch_bar_open_ms = int(_v)
                    break
        except Exception:
            _touch_bar_open_ms = int(touch_open_ms)

        # Fallback: use computed touch_open_ms if bar timestamp not found
        if not _touch_bar_open_ms:
            _touch_bar_open_ms = int(touch_open_ms)
        try:
            cd_key = _zone_cooldown_key(
                uid,
                profile_id,
                sym_u,
                resolved_dir,
                tfu,
            )
            cd_raw = R.get(cd_key) if R is not None else None
            if cd_raw:
                ttl = R.ttl(cd_key)
                gate["reason"] = f"ZONE_COOLDOWN_AFTER_CLOSE | {ttl}s"
                gate["stage"] = "ZONE_COOLDOWN"
                gate["blocked"] = False
                gate["zone_cooldown_key"] = cd_key
                gate["zone_cooldown_ttl_sec"] = int(ttl or 0)
                gate["resolved_dir"] = resolved_dir
                return False, gate
        except Exception:
            pass

        watch = {
            "state": "WATCH",
            "started_ms": int(now_ms_pick),
            "touch_open_ms": int(touch_open_ms),
            "touch_close_ms": int(touch_close_ms),
            "touch_candle_open_ms": int(_touch_bar_open_ms),  # ? NEW: RC boundary
            "min_reclaim_close_ms": int(touch_close_ms),
            "direction": resolved_dir,
            "tf": tfu,
            "zone_used": zone_used,
            "atr": float(atr or 0.0),
            # Set to now_ms_pick so only FUTURE closed candles are evaluated
            # Prevents old closed candles from being used as RC on fresh watch
            "watch_created_ms": int(now_ms_pick or 0),
            "last_checked_closed_ms": int(now_ms_pick or 0),
            "touch_source": "LIVE_TOUCH",
        }
        watch["setup_analysis"] = _build_expected_setup_analysis(
            watch,
            zone_used,
            resolved_dir,
            bars=bars,
            atr=atr,
        )
        try:
            zone_watch_set(
                R,
                uid_u,
                sym_u,
                resolved_dir,
                json.dumps(
                    watch,
                    separators=(",", ":"),
                ),
                tf=tfu,
                ex=7 * 24 * 3600,
            )
        except Exception:
             pass
        gate["zone_used"] = zone_used
        gate["watch_key"] = str(wkey)
        gate["rev_state"] = {
            "state": "WATCH",
            "started_ms": int(watch.get("started_ms") or now_ms_pick),
            "touch_open_ms": int(watch.get("touch_open_ms") or 0),
            "touch_close_ms": int(watch.get("touch_close_ms") or 0),
            "min_reclaim_close_ms": int(watch.get("min_reclaim_close_ms") or 0),
            "direction": resolved_dir,
            "tf": tfu,
        }
        gate["reason"] = "REV_WATCH | LIVE_TOUCH_STARTED | WAIT_TOUCH_CANDLE_CLOSE"
        gate["stage"] = "WATCH"
        gate["blocked"] = False
        return False, gate
        
      

    # ------------------------------------------------------------
    # SAFETY: zone_used must be a dict from here on
    # ------------------------------------------------------------
    if not isinstance(zone_used, dict):
        gate["reason"] = "WATCH_ZONE_MISSING"
        gate["stage"] = "WATCH"
        gate["blocked"] = False
        gate["zone_used"] = None
        if debug_gate:
            gate["dbg_zone_used_type"] = str(type(zone_used).__name__)
            gate["dbg_watch_type"] = str(type(watch).__name__)
        return False, gate



    
    
    # 5) reversal confirmation: reclaim only (CLOSED candle after watch started)
    zl = float(zone_used.get("low") or zone_used.get("level") or 0.0)
    zh = float(zone_used.get("high") or zone_used.get("level") or 0.0)
    started_ms = int((watch or {}).get("started_ms") or 0)
    # ------------------------------------------------------------
    # LOCK REV_OK:
    # REV_OK remains armed, but latest valid RC can refresh trigger.
    # This prevents stale/yesterday RC from staying locked forever.
    # ------------------------------------------------------------
    _watch_state_u = str((watch or {}).get("state") or "").upper()

    _point_a_terminal_rc_refresh = bool(
        isinstance(watch, dict)
        and watch.get("_point_a_terminal_rc_refresh")
        and _watch_state_u
        == "ENTRY_BLOCKED_POINT_A_BLOCK"
    )

    _is_rc_locked_state = (
        isinstance(watch, dict)
        and (
            (
                bool(watch.get("rev_ok"))
                and (
                    _watch_state_u == "REV_OK"
                    or _watch_state_u.startswith("ENTRY_BLOCKED")
                )
            )
            or _point_a_terminal_rc_refresh
        )
    )
    if _is_rc_locked_state:
        # ------------------------------------------------------------
        # LATEST RC ALWAYS WINS
        # If watch is already REV_OK, still allow a newer closed candle
        # to replace the old RC before returning RC_LOCKED.
        # ------------------------------------------------------------
        try:
            _cur_closed_ms = int(closed_ms or 0)
            _old_rev_ms = int(watch.get("rev_ok_ms") or 0)

            _newer_rc = False
            _rc_reject_reason = None
            _best_rc = None

            _dir = str(watch.get("direction") or resolved_dir).upper()

            # P1 FIX:
            # Do not check only the latest/current closed candle.
            # Scan all completed bars after old rev_ok_ms and pick the latest valid RC.
            try:
                for _b in (bars or []):
                    if not isinstance(_b, dict):
                        continue
                    if not bool(_b.get("complete", True)):
                        continue

                    _b_open_ms = _to_ms_any(
                        _b.get("t_open_ms")
                        or _b.get("tOpenMs")
                        or _b.get("open_time_ms")
                        or _b.get("t")
                        or _b.get("time")
                        or _b.get("ts")
                        or 0
                    )
                    _b_open_ms = int(
                        _b_open_ms or 0
                    )

                    _b_close_ms = _to_ms_any(
                        _b.get("t_close_ms")
                        or _b.get("tCloseMs")
                        or _b.get("close_time_ms")
                        or 0
                    )
                    _b_close_ms = int(
                        _b_close_ms or 0
                    )

                    # H1 snapshots may omit t_close_ms or incorrectly copy
                    # t_open_ms into it. Normalize both cases to open + tf_ms.
                    if (
                        _b_open_ms > 0
                        and _b_close_ms <= _b_open_ms
                    ):
                        _b_close_ms = (
                            _b_open_ms + int(tf_ms)
                        )

                    # rev_ok_ms is the previous RC CLOSE time.
                    # Therefore compare candidate CLOSE time,
                    # not candidate open time.
                    if (
                        _snap_cutoff_ms > 0
                        and _b_close_ms > _snap_cutoff_ms
                    ):
                        continue
                    if (
                        _b_close_ms
                        <= int(_old_rev_ms or 0)
                    ):
                        continue

                    _bh = float(
                        _b.get("h")
                        or _b.get("high")
                        or 0.0
                    )
                    _bl = float(
                        _b.get("l")
                        or _b.get("low")
                        or 0.0
                    )
                    _bc = float(
                        _b.get("c")
                        or _b.get("close")
                        or 0.0
                    )

                    if _dir == "SELL":
                        _valid = bool(
                            _bh >= float(zl)
                            and _bc < float(zl)
                        )
                    else:
                        _valid = bool(
                            _bl <= float(zh)
                            and _bc > float(zh)
                        )

                    if _valid:
                        _candidate_rc = {
                            "open_ms": int(_b_open_ms),
                            "close_ms": int(_b_close_ms),
                            "hi": float(_bh),
                            "lo": float(_bl),
                            "cl": float(_bc),
                        }

                        # Latest valid RC wins regardless of snapshot bar order.
                        if (
                            _best_rc is None
                            or int(_candidate_rc["close_ms"])
                            > int(_best_rc["close_ms"])
                        ):
                            _best_rc = _candidate_rc
            except Exception as _scan_exc:
                if debug_gate:
                    gate["dbg_latest_rc_scan_exc"] = f"{type(_scan_exc).__name__}:{_scan_exc}"

            if _best_rc:
                _newer_rc = True

                _cur_open_ms = int(
                    _best_rc["open_ms"]
                )
                _cur_closed_ms = int(
                    _best_rc["close_ms"]
                )

                hi = float(_best_rc["hi"])
                lo = float(_best_rc["lo"])
                cl = float(_best_rc["cl"])
                log.warning(
                    "[RC_PICK] sym=%s "
                    "rc_open_ms=%s rc_close_ms=%s "
                    "hi=%s lo=%s cl=%s "
                    "snap_last_closed=%s "
                    "dbg_h1_snap_key=%s",
                    sym_u,
                    _cur_open_ms,
                    _cur_closed_ms,
                    hi,
                    lo,
                    cl,
                    snap_last_closed,
                    k if "k" in locals() else None,
                )
            else:
                _rc_reject_reason = {
                    "need": "latest valid RC after old_rev_ok_ms",
                    "old_rev_ok_ms": int(_old_rev_ms),
                    "latest_closed_ms": int(_cur_closed_ms),
                    "direction": str(_dir),
                    "zone_low": float(zl),
                    "zone_high": float(zh),
                }

            if debug_gate and _rc_reject_reason:
                gate["dbg_latest_rc_not_refreshed"] = _rc_reject_reason
            if _newer_rc:
                _prev_state_for_rc_refresh = str(watch.get("state") or "").upper()
                # ---------------------------------------------------------
                # RC refresh latency instrumentation
                # ---------------------------------------------------------
                try:
                    # ---------------------------------------------------------
                    # RC refresh timing instrumentation
                    #
                    # Measure only server receive -> gate refresh latency.
                    # Do not subtract broker candle timestamps from server UTC.
                    # ---------------------------------------------------------
                    _rc_refresh_server_ms = int(time.time() * 1000)

                    try:
                        _snap_server_received_ms = int(
                            (snap or {}).get("server_received_ms")
                            or (snap or {}).get("serverNow")
                            or 0
                        )
                    except Exception:
                        _snap_server_received_ms = 0

                    if _snap_server_received_ms > 0:
                        _refresh_latency_ms = max(
                            0,
                            int(_rc_refresh_server_ms)
                            - int(_snap_server_received_ms),
                        )
                    else:
                        _refresh_latency_ms = None

                    log.warning(
                        "[WATCHLIST] RC_REFRESH "
                        "sym=%s side=%s "
                        "old_rev_ok_ms=%s "
                        "new_rev_ok_ms=%s "
                        "rc_open_ms=%s "
                        "broker_last_closed_ms=%s "
                        "snapshot_received_ms=%s "
                        "refresh_server_ms=%s "
                        "latency_ms=%s",
                        sym_u,
                        _dir,
                        int(_old_rev_ms),
                        int(_cur_closed_ms),
                        int(_cur_open_ms),
                        int(snap_last_closed or 0),
                        int(_snap_server_received_ms or 0),
                        int(_rc_refresh_server_ms),
                        _refresh_latency_ms,
                    )

                except Exception:
                    pass
                if _prev_state_for_rc_refresh.startswith("ENTRY_BLOCKED_POINT_A"):
                    watch["state"] = "REV_OK"
                    watch["trade_state"] = ""
                    watch["entry_blocked"] = False
                    watch.pop("entry_block_reason", None)
                    watch.pop("next_retry_ms", None)
                    watch.pop("late_entry_max_move", None)
                    for _pa_key in (
                        "point_a",
                        "point_a_decision",
                        "point_a_action",
                        "point_a_reason",
                        "point_a_reason_codes",
                        "point_a_wait_started_ms",
                        "point_a_last_eval_ms",
                        "point_a_rc_rev_ok_ms",
                        "point_a_trigger_level",
                        "point_a_cross_latched",
                        "point_a_cross_ms",
                        "point_a_cross_price",
                        "point_a_trigger_atr",
                        "point_a_displacement_atr",
                        "point_a_max_displacement_atr",

                        # M15 WAIT lifecycle
                        "point_a_wait_start_m15_bucket",
                        "point_a_wait_m15_bars",
                        "point_a_max_wait_m15_bars",
                        "point_a_fallback_max_wait_ms",

                        # Terminal BLOCK / EXPIRE markers
                        "point_a_terminal",
                        "point_a_terminal_reason",
                    ):
                        watch.pop(_pa_key, None)
                else:
                    watch["state"] = _prev_state_for_rc_refresh if _prev_state_for_rc_refresh.startswith("ENTRY_BLOCKED") else "REV_OK"
                watch.pop("_point_a_terminal_rc_refresh", None)
                watch["rev_ok"] = True
                watch["rev_ok_ms"] = int(_cur_closed_ms)
                watch["last_checked_closed_ms"] = int(_cur_closed_ms)
                watch["last_checked_close"] = float(cl)
                watch["last_checked_high"] = float(hi)
                watch["last_checked_low"] = float(lo)
                watch["atr"] = float(atr or watch.get("atr") or 0.0)

                watch["rev_ok_bar_hi"] = float(hi)
                watch["rev_ok_bar_lo"] = float(lo)
                watch["rev_ok_bar_close"] = float(cl)
                watch["rc_high"] = float(hi)
                watch["rc_low"] = float(lo)
                watch["rc_close"] = float(cl)
                # Full REV_OK snapshot for audit/UI/executor consistency
                _dir_for_trigger = str(watch.get("direction") or resolved_dir).upper()
                watch["rc_open_ms"] = int(
                    _cur_open_ms
                )
                watch["rc_close_ms"] = int(
                    _cur_closed_ms
                )
                watch["trigger_level"] = float(lo) if _dir_for_trigger == "SELL" else float(hi)
                # Refreshed RC closed after old rev_ok_ms, so it can never
                # be the original touch candle — keep the flag accurate.
                watch["rc_is_touch_candle"] = False
                # Preserve the immutable first-touch market prediction.
                # REV_OK is later evidence and must not rewrite history.
                # Never reconstruct the first-touch prediction here.
                # REV_OK contains later evidence and would introduce look-ahead bias.
                if not isinstance(watch.get("setup_analysis"), dict):
                    watch["setup_analysis"] = {
                        "schema_version": 2,
                        "analytics_only": True,
                        "immutable_prediction": True,
                        "prediction_classifier_version": "market_behavior_prediction_v2",
                        "prediction_frozen_at_ms": None,
                        "predicted_market_behavior": "UNCLASSIFIED",
                        "prediction_stage": "MISSING_FIRST_TOUCH_PREDICTION",
                        "predicted_direction": None,
                        "reason_codes": [
                            "FIRST_TOUCH_PREDICTION_NOT_CAPTURED"
                        ],
                        "continuation_sequence": {
                            "momentum_present": False,
                            "pressure_into_zone": False,
                            "zone_break_confirmed": False,
                            "retest_present": False,
                        },
                        "evidence_at_prediction": {},
                        "selected_production_strategy": "ZONE_REVERSAL",
                        "selected_production_strategy_version": "zone_reversal_v1",
                    }
                watch["entry_confirmation"] = {
                    "schema_version": 1,
                    "selected_production_strategy": "ZONE_REVERSAL",
                    "confirmed_at_ms": int(_cur_closed_ms),
                    "rc_high": float(hi),
                    "rc_low": float(lo),
                    "rc_close": float(cl),
                    "trigger_level": float(watch.get("trigger_level") or 0),
                    "rc_is_touch_candle": False,
                }
                # Freeze analytics-only validation at RC confirmation.
                #
                # This records:
                #   1. first-touch prediction
                #   2. selected-zone quality + local SR
                #   3. DXY direction + DXY SR
                #
                # It does not block, delay, score or modify execution.
                if not isinstance(
                    watch.get("entry_validation"),
                    dict,
                ):
                    try:
                        watch["entry_validation"] = (
                            _build_entry_validation_analytics(
                                R=R,
                                watch=watch,
                                zone=(
                                    watch.get("zone_used")
                                    if isinstance(
                                        watch.get(
                                            "zone_used"
                                        ),
                                        dict,
                                    )
                                    else zone_used
                                ),
                                direction=_dir_for_trigger,
                                symbol=sym_u,
                                entry_price=float(
                                    live_px
                                    if live_px is not None
                                    else cl
                                ),
                                atr=float(atr),
                                device_id=(
                                    str(
                                        x_device_id
                                        or pinned_device
                                        or ""
                                    ).strip()
                                    or None
                                ),
                                profile_id=(
                                    str(
                                        profile_id
                                        or ""
                                    ).strip()
                                    or None
                                ),
                                confirmed_at_ms=int(
                                    _cur_closed_ms
                                ),
                            )
                        )

                    except Exception as exc:
                        log.warning(
                            "[ZONE_GATE] "
                            "ENTRY_VALIDATION_CAPTURE_FAILED "
                            "uid=%s sym=%s side=%s "
                            "err=%r",
                            uid_u,
                            sym_u,
                            _dir_for_trigger,
                            exc,
                        )

                        watch["entry_validation"] = {
                            "schema_version": 1,
                            "analytics_only": True,
                            "immutable_entry_validation": True,
                            "created_at_ms": int(
                                time.time() * 1000
                            ),
                            "validation_stage": (
                                "REVERSAL_CONFIRMED"
                            ),
                            "production_unchanged": True,
                            "capture_error": (
                                f"{type(exc).__name__}:"
                                f"{exc}"
                            ),
                        }
                    log.warning(
                        "[ZONE_GATE] RC_ANALYTICS_CAPTURE "
                        "sym=%s side=%s "
                        "setup=%s "
                        "confirmation=%s "
                        "validation=%s",
                        sym_u,
                        _dir_for_trigger,
                        isinstance(watch.get("setup_analysis"), dict),
                        isinstance(watch.get("entry_confirmation"), dict),
                        isinstance(watch.get("entry_validation"), dict),
                    )

                # New/refreshed RC must be allowed to send one new trigger alert.
                watch["discord_rc_trigger_sent"] = False
                watch.pop("discord_rc_trigger_sent_ms", None)
                watch.pop("discord_rc_trigger_price", None)
                watch.pop("discord_rc_trigger_error", None)
                watch["frozen_at_ms"] = int(now_ms_pick)
                watch["last_price"] = float((live_px if "live_px" in locals() else 0) or cl)
                watch["updated_at_ms"] = int(now_ms_pick)
                watch["dbg_latest_rc_refreshed"] = int(_cur_open_ms)
                watch["dbg_rc_refresh_server_ms"] = int(
                    _rc_refresh_server_ms
                )
                watch["dbg_rc_snapshot_received_ms"] = int(
                    _snap_server_received_ms or 0
                )
                watch["dbg_rc_refresh_latency_ms"] = (
                    int(_refresh_latency_ms)
                    if _refresh_latency_ms is not None
                    else None
                )
                watch["dbg_rc_refresh_latency_basis"] = (
                    "SERVER_RECEIVE_TIME"
                    if _snap_server_received_ms > 0
                    else "UNAVAILABLE"
                )

                _refresh_set_ok = False  # set by final persist after RC cleanup below

                # RC changed => old breakout state / old entry claim is no longer valid.
                # New RC must require a fresh break of the new trigger.
                try:
                    _rc_side = str(watch.get("direction") or resolved_dir).upper()
                    R.delete(
                        break_state_key(
                            uid_u,
                            sym_u,
                            _rc_side,
                            "H1",
                        )
                    )
                    delete_latest_entry_claim(
                        R,
                        uid_u,
                        sym_u,
                        _rc_side,
                        tf="H1",
                    )
                    watch["entry_triggered"] = False
                    watch["entry_ready"] = False
                    watch["rc_trigger_crossed"] = False
                    watch.pop("rc_trigger_crossed_ms", None)
                    watch.pop("rc_trigger_cross_price", None)
                    watch.pop("rc_trigger_cross_level", None)
                    watch.pop("rc_trigger_cross_rev_ok_ms", None)
                    watch.pop("entry_ready_price", None)
                    watch.pop("entry_ready_ts_ms", None)
                    watch.pop("entry_price", None)
                    watch.pop("entry_ts_ms", None)
                    _refresh_set_ok = bool(
                        zone_watch_set(
                            R,
                            uid_u,
                            sym_u,
                            str(watch.get("direction") or resolved_dir).upper(),
                            json.dumps(watch, separators=(",", ":")),
                            tf=tfu,
                            ex=7 * 24 * 3600,
                        )
                    )
                except Exception:
                    pass
                log.warning(
                    "[WATCHLIST] LATEST_RC_REFRESH_PERSIST sym=%s side=%s key=%s set_ok=%s old_rev=%s new_rev=%s new_lo=%s new_hi=%s",
                    sym_u, str(watch.get("direction") or resolved_dir).upper(), wkey,
                    bool(_refresh_set_ok), int(_old_rev_ms), int(_cur_closed_ms),
                    float(lo), float(hi)
                )

                gate["dbg_latest_rc_refreshed"] = {
                    "old_rev_ok_ms": int(_old_rev_ms),
                    "new_rev_ok_ms": int(_cur_closed_ms),
                    "close": float(cl),
                    "high": float(hi),
                    "low": float(lo),
                    "direction": str(watch.get("direction") or resolved_dir).upper(),
                }

        except Exception as e:
            if debug_gate:
                gate["dbg_latest_rc_refresh_exc"] = f"{type(e).__name__}:{e}"

        # ------------------------------------------------------------
        # RE-VALIDATE STORED RC CANDLE
        # Even though REV_OK is locked, verify the stored RC actually
        # touched the zone. If not ? auto-rollback to REV_WATCH.
        # This runs on every tick so bad RCs are self-healing without
        # needing Redis deletes.
        # ------------------------------------------------------------
        _stored_rc_valid = True
        try:
            _stored_zl = float((watch.get("zone_used") or {}).get("low") or zl or 0)
            _stored_zh = float((watch.get("zone_used") or {}).get("high") or zh or 0)
            _stored_rc_hi = float(watch.get("rev_ok_bar_hi") or 0)
            _stored_rc_lo = float(watch.get("rev_ok_bar_lo") or 0)
            _stored_direction = str(watch.get("direction") or resolved_dir).upper()
            _stored_rc_ms = int(watch.get("rev_ok_ms") or 0)
            _stored_started_ms = int(watch.get("started_ms") or 0)
            _stored_watch_created_ms = int(watch.get("watch_created_ms") or _stored_started_ms or 0)

            if _stored_direction == "SELL":
                # RC candle high must have reached zone_low
                if _stored_rc_hi > 0 and _stored_zh > 0:
                    if _stored_rc_hi < _stored_zl:
                        _stored_rc_valid = False
            else:  # BUY
                
                # BUY RC candle low must have reached zone_high
                if _stored_rc_lo > 0 and _stored_zh > 0:
                    if _stored_rc_lo > _stored_zh:
                       _stored_rc_valid = False

            # RC candle close time must be after watch creation,
            # UNLESS the RC is the touch candle itself (accepted by
            # policy in the confirmation path: touch candle closing
            # beyond the zone is a valid RC). Without this exception
            # the acceptance and revalidation rules contradict each
            # other and every touch-candle RC oscillates:
            # REV_OK -> rollback -> REV_WATCH -> REV_OK -> ...
            _stored_rc_is_touch = bool(watch.get("rc_is_touch_candle")) or (
                _stored_rc_ms > 0
                # Legacy watches persisted before rc_is_touch_candle
                # existed: re-derive from stored touch_close_ms.
                and _stored_rc_ms == int(watch.get("touch_close_ms") or 0)
            )
            if _stored_rc_ms > 0 and _stored_watch_created_ms > 0:
                if _stored_rc_ms <= _stored_watch_created_ms and not _stored_rc_is_touch:
                    _stored_rc_valid = False

            # Stored RC must also have CLOSED beyond the zone — the same
            # price rule acceptance enforces. Applies to EVERY RC,
            # including the touch candle: the touch-candle exception
            # relaxes only the TIMING rule, never the price rule.
            # BUY:  close must be > zone_high
            # SELL: close must be < zone_low
            _stored_rc_cl = float(
                watch.get("rev_ok_bar_close")
                or watch.get("rc_close")
                or 0
            )
            if _stored_rc_cl > 0 and _stored_zl > 0 and _stored_zh > 0:
                if _stored_direction == "SELL":
                    if _stored_rc_cl >= _stored_zl:
                        _stored_rc_valid = False
                else:  # BUY
                    if _stored_rc_cl <= _stored_zh:
                        _stored_rc_valid = False

        except Exception:
            _stored_rc_valid = True  # validation error — don't block

        if not _stored_rc_valid:
            # Auto-rollback — clear EVERY field the confirmation path
            # wrote, roll back to REV_WATCH. Leaving rc_high/rc_low/
            # trigger_level behind creates a half-armed watch: state
            # says REV_WATCH but downstream consumers keying off
            # trigger_level can still arm an entry from a dead RC.
            # (None-valued fields are dropped from the persisted JSON
            # by the `if v is not None` filter below, so this deletes
            # them from Redis.)
            watch["state"] = "REV_WATCH"
            watch["rev_ok"] = False
            watch["rev_ok_ms"] = 0
            for _rc_k in (
                "rev_ok_bar_hi", "rev_ok_bar_lo", "rev_ok_bar_close",
                "rc_open_ms", "rc_close_ms",
                "rc_high", "rc_low", "rc_close",
                "trigger_level", "rc_is_touch_candle",
            ):
                watch[_rc_k] = None
            # Stale RC alert bookkeeping must not suppress the alert
            # for the next legitimately confirmed RC.
            watch.pop("discord_rc_trigger_sent", None)
            watch.pop("discord_rc_trigger_sent_ms", None)
            watch.pop("discord_rc_trigger_price", None)
            watch.pop("discord_rc_trigger_error", None)
            try:
                _rollback_payload = {k: v for k, v in watch.items() if v is not None}
                _rollback_json = json.dumps(_rollback_payload, separators=(",", ":"))
                _set_ok = bool(
                    zone_watch_set(
                        R,
                        uid_u,
                        sym_u,
                        str(watch.get("direction") or resolved_dir).upper(),
                        _rollback_json,
                        tf=tfu,
                        ex=7 * 24 * 3600,
                    )
                )

                if debug_gate:
                    gate["dbg_rc_rollback_persist"] = {
                        "wkey": str(wkey),
                        "set_ok": bool(_set_ok),
                        "state_written": _rollback_payload.get("state"),
                        "rev_ok_written": _rollback_payload.get("rev_ok"),
                        "rev_ok_ms_written": _rollback_payload.get("rev_ok_ms"),
                    }
            except Exception as e:
                if debug_gate:
                    gate["dbg_rc_rollback_persist_exc"] = f"{type(e).__name__}:{e}"
            if debug_gate:
                gate["dbg_rc_revalidation_rollback"] = {
                    "reason": "stored_rc_did_not_touch_zone",
                    "stored_rc_hi": float(_stored_rc_hi or 0),
                    "stored_rc_lo": float(_stored_rc_lo or 0),
                    "zone_low": _stored_zl,
                    "zone_high": _stored_zh,
                    "direction": _stored_direction,
                    "rolled_back_to": "REV_WATCH",
                }
            # Redis updated — return REV_WATCH cleanly
            # Next tick will re-evaluate with fresh closed candle
            gate["reason"] = (
                f"REV_WATCH | FZ {float(zl):.5f}-{float(zh):.5f}"
                f" | RC_INVALID_ROLLBACK | WAIT_VALID_RC"
                f" | TF={tfu}"
            )
            gate["stage"] = "REV_WATCH"
            gate["blocked"] = False
            gate["rev_ok"] = False
            gate["zone_used"] = zone_used
            return False, gate
                

        
        # ------------------------------------------------------------

        gate["zone_used"] = zone_used
        gate["rev_ok"] = True
        gate["rev_ok_ms"] = int((watch or {}).get("rev_ok_ms") or closed_ms)
        gate["watch_key"] = str(wkey)
        gate["rev_state"] = {
            "state": "REV_OK",
            "started_ms": int(watch.get("started_ms") or started_ms or now_ms_pick),
            "rev_ok_ms": int(watch.get("rev_ok_ms") or 0),
            "direction": str(watch.get("direction") or resolved_dir),
            "tf": str(watch.get("tf") or tfu),
            "rev_ok_bar_hi": float(watch.get("rev_ok_bar_hi") or 0.0),
            "rev_ok_bar_lo": float(watch.get("rev_ok_bar_lo") or 0.0),
            "rev_ok_bar_close": float(watch.get("rev_ok_bar_close") or watch.get("last_checked_close") or 0.0),
        }
        gate["rev_trigger"] = {
            "entry_above": float(watch.get("rev_ok_bar_hi") or 0.0),
            "entry_below": float(watch.get("rev_ok_bar_lo") or 0.0),
        }

        try:
            import datetime
            _tz_offset = datetime.timedelta(hours=-1)
            _freeze_dt = (datetime.datetime.utcfromtimestamp(
                int(watch.get("started_ms") or started_ms or 0) / 1000
            ) + _tz_offset).strftime("%m/%d %H:%M")
            _rc_dt = (datetime.datetime.utcfromtimestamp(
                int(watch.get("rev_ok_ms") or 0) / 1000
            ) + _tz_offset).strftime("%m/%d %H:%M")
        except Exception:
            _freeze_dt = "?"
            _rc_dt = "?"

        _w_dir = str(watch.get("direction") or resolved_dir).upper()
        gate["reason"] = (
            f"REV_OK | FZ {float(zl):.5f}-{float(zh):.5f} "
            f"| FREEZE@{_freeze_dt} | RC@{_rc_dt} "
            f"| RC {float(watch.get('rev_ok_bar_close') or watch.get('last_checked_close') or 0.0):.5f} "
            f"| ENTRY < {float(watch.get('rev_ok_bar_lo') or 0.0):.5f}"
            if _w_dir == "SELL"
            else
            f"REV_OK | FZ {float(zl):.5f}-{float(zh):.5f} "
            f"| FREEZE@{_freeze_dt} | RC@{_rc_dt} "
            f"| RC {float(watch.get('rev_ok_bar_close') or watch.get('last_checked_close') or 0.0):.5f} "
            f"| ENTRY > {float(watch.get('rev_ok_bar_hi') or 0.0):.5f}"
        )
        gate["reason"] = f"{gate['reason']} | RC_LOCKED | LIVE_BREAKOUT_ONLY | TF={tfu}"
        gate["stage"] = "REV_LOCKED"
        gate["blocked"] = False

        # Check invalidation even in REV_OK state
        # Same logic as main invalidation — trust complete=True
        try:
            import time as _t5
            _sys_now_roi = int(_t5.time() * 1000)
            _freeze_roi = int(watch.get("started_ms") or 0)
            _inv_consec = 0
            _bs_roi = sorted(
                [b for b in (bars or []) if isinstance(b, dict)],
                key=lambda b: _to_ms_any(b.get("t_open_ms") or b.get("tOpenMs") or
                                          b.get("open_time_ms") or b.get("t") or 0) or 0
            )
            for _cb in reversed(_bs_roi):
                _om_roi = _to_ms_any(
                    _cb.get("t_open_ms") or _cb.get("tOpenMs") or
                    _cb.get("open_time_ms") or _cb.get("t") or 0
                )
                if not _om_roi or int(_om_roi) <= int(_freeze_roi or 0):
                    break
                _is_comp = _cb.get("complete") is True
                _clk_roi = (int(_om_roi) + int(tf_ms)) <= _sys_now_roi
                if not _is_comp and not _clk_roi:
                    continue
                _cv = _bar_f(_cb, "c", "close")
                if _cv is None:
                    break
                _cv = float(_cv)
                if resolved_dir == "SELL":
                    _bad = _cv >= float(zh)
                else:
                    _bad = _cv <= float(zl)
                if _bad:
                    _inv_consec += 1
                else:
                    break
                if _inv_consec >= int(hard_close_bars):
                    break
            if _inv_consec >= int(hard_close_bars):
                try:
                    for _s in ("BUY", "SELL"):
                        for _t in ("H1", "H4"):
                            _wk_inv = _watch_key(
                                uid_u,
                                sym_u,
                                _s,
                                _t,
                            )
                            # GUARD: if a live/confirmed trade is riding this watch,
                            # do NOT delete it — that orphans the open broker position
                            # and forces a source-stripping BROKER_REPAIR. The trade
                            # rides to its SL/TP under broker management; the watch
                            # must persist until the trade actually closes.
                            try:
                                _wj = _json_load(R.get(_wk_inv)) or {}
                                _wst_inv = str(_wj.get("state") or "").upper()
                            except Exception:
                                _wst_inv = ""
                            if _wst_inv in ("ORDER_PENDING", "TRADE_ACTIVE"):
                                continue
                            R.delete(_wk_inv)
                except Exception:
                    pass
                gate["reason"] = f"INVALIDATED | REV_OK_CANCELLED | {_inv_consec} closes beyond zone | FZ {float(zl):.5f}-{float(zh):.5f}"
                gate["stage"] = "INVALIDATED"
                gate["blocked"] = True
                gate["rev_ok"] = False
                return False, gate
        except Exception:
            pass

        return True, gate

   
   
    # ------------------------------------------------------------
    # PHASE-1 FIX:
    # A frozen REV_WATCH must NOT stay stuck on an old candle.
    # Every new CLOSED candle must be evaluated against frozen zone.
    # IMPORTANT: only update last_checked fields — never touch
    # rev_ok / state / zone_used / rev_ok_bar_* here.
    # REV_OK state is written only in the rev_ok confirmation block below.
    # ------------------------------------------------------------
    try:
        last_checked_ms = int((watch or {}).get("last_checked_closed_ms") or 0)
    except Exception:
        last_checked_ms = 0

    # Only write candle refresh if this is genuinely a new closed candle
    # and the watch is NOT already in REV_OK state (REV_OK lock handles its own write)
    _watch_state_u2 = str((watch or {}).get("state") or "").upper()
    _watch_is_rc_locked = (
        isinstance(watch, dict)
        and bool(watch.get("rev_ok"))
        and (
            _watch_state_u2 == "REV_OK"
            or _watch_state_u2.startswith("ENTRY_BLOCKED")
        )
    )
    if not _watch_is_rc_locked:
        try:
            if isinstance(watch, dict) and int(closed_ms or 0) >= int(last_checked_ms or 0):
                watch["last_checked_closed_ms"] = int(closed_ms or 0)
                watch["last_checked_close"] = float(cl)
                watch["last_checked_high"] = float(hi)
                watch["last_checked_low"] = float(lo)
                # Do NOT persist here.
                # This is only an in-memory closed-candle refresh.
                # Redis writes are allowed only on lifecycle transitions:
                # WATCH_CREATE, REV_OK_REFRESH, REV_OK_CONFIRM, RC_ROLLBACK.
                pass
        except Exception:
            pass
    if debug_gate:
        gate["dbg_watch_candle_refresh"] = {
            "last_checked_ms_before": int(last_checked_ms),
            "current_closed_ms": int(closed_ms or 0),
            "new_closed_candle": bool(int(closed_ms or 0) > int(last_checked_ms or 0)),
            "current_close": float(cl),
        }

    

    if debug_gate:
        gate["rev_basis"] = {
            "closed_ms": int(closed_ms),
            "started_ms": int(started_ms),
            "cl": float(cl),
            "zl": float(zl),
            "zh": float(zh),
        }
    try:
        min_reclaim_close_ms = int((watch or {}).get("min_reclaim_close_ms") or 0)
    except Exception:
        min_reclaim_close_ms = 0
    # HARD RULE:
    # Old closed candle must never become RC after live touch.
    # BUT the same touch candle is allowed as RC if it closes reclaiming the zone.
    touch_close_ms = int((watch or {}).get("touch_close_ms") or 0)
    same_touch_candle = bool(
        touch_close_ms > 0
        and int(closed_ms or 0) == int(touch_close_ms)
    )

    if int(closed_ms or 0) <= int(started_ms or 0) and not same_touch_candle:
        gate["reason"] = "REV_WATCH | WAIT_TOUCH_CANDLE_CLOSE"
        gate["stage"] = "WATCH"
        gate["blocked"] = False
        gate["zone_used"] = zone_used
        gate["watch_key"] = str(wkey)
        gate["rev_state"] = {
            "state": "WATCH",
            "started_ms": int(started_ms or 0),
            "touch_close_ms": int(touch_close_ms or 0),
            "same_touch_candle": bool(same_touch_candle),
            "current_closed_ms": int(closed_ms or 0),
            "min_reclaim_close_ms": int(min_reclaim_close_ms or 0),
            "direction": resolved_dir,
            "tf": tfu,
        }
        return False, gate

    if min_reclaim_close_ms > 0 and int(closed_ms or 0) < int(min_reclaim_close_ms):
        gate["reason"] = "REV_WATCH | WAIT_TOUCH_CANDLE_CLOSE"
        gate["stage"] = "WATCH"
        gate["blocked"] = False
        gate["zone_used"] = zone_used
        gate["watch_key"] = str(wkey)
        gate["rev_state"] = {
            "state": "WATCH",
            "started_ms": int(started_ms or 0),
            "touch_open_ms": int((watch or {}).get("touch_open_ms") or 0),
            "touch_close_ms": int((watch or {}).get("touch_close_ms") or 0),
            "min_reclaim_close_ms": int(min_reclaim_close_ms),
            "current_closed_ms": int(closed_ms or 0),
            "direction": resolved_dir,
            "tf": tfu,
        }
        return False, gate

    # RC CANDLE HARD RULES:
    # 1. Same touch candle CAN become RC if it closes reclaiming the frozen zone.
    # 2. Older candles before touch/freeze must never become RC.
    # 3. Candle must be closed (complete=True OR open+tf_ms <= sys_now).
    # 4. Candle close must be >= min_reclaim_close_ms unless it is same_touch_candle.
    _bar_open_ms = 0
    try:
        for _ok in ("t_open_ms", "tOpenMs", "open_time_ms", "ts_ms", "t", "time", "ts"):
            _v = _to_ms_any((c or {}).get(_ok))
            if _v and int(_v) > 0:
                _bar_open_ms = int(_v)
                break
    except Exception:
        _bar_open_ms = 0

    import time as _t3
    _sys_now_rc = int(_t3.time() * 1000)
    _is_complete_rc = (c or {}).get("complete") is True
    _closed_by_clock_rc = (_bar_open_ms > 0 and (_bar_open_ms + int(tf_ms)) <= _sys_now_rc)
    _bar_is_closed = _is_complete_rc or _closed_by_clock_rc
    _min_reclaim = int((watch or {}).get("min_reclaim_close_ms") or 0)
    _watch_created = int((watch or {}).get("watch_created_ms") or started_ms or 0)

    _touch_close_ms_for_rc = int((watch or {}).get("touch_close_ms") or 0)
    _same_touch_candle_for_rc = bool(
        _touch_close_ms_for_rc > 0
        and int(closed_ms or 0) == int(_touch_close_ms_for_rc)
    )

    _rc_time_valid = (
        _bar_is_closed
        and _bar_open_ms > 0
        and int(closed_ms or 0) >= int(_touch_close_ms_for_rc or 0)
        and (
            _same_touch_candle_for_rc
            or int(closed_ms or 0) > int(_watch_created or 0)
        )
        and (_min_reclaim <= 0 or int(closed_ms or 0) >= int(_min_reclaim))
    )
    # Scan ALL closed bars after freeze for RC — not just last picked bar
    # This finds the FIRST bar after freeze that meets RC condition
    import time as _t6
    _sys_now_scan = int(_t6.time() * 1000)
    _rc_bar = None
    _rc_bar_close = None
    _rc_bar_open_ms = 0
    _rc_bar_closed_ms = 0
    try:
        _bs_scan = sorted(
            [b for b in (bars or []) if isinstance(b, dict)],
            key=lambda b: _to_ms_any(b.get("t_open_ms") or b.get("tOpenMs") or
                                      b.get("open_time_ms") or b.get("t") or 0) or 0
        )
        for _sb in _bs_scan:
            _sb_om = _to_ms_any(
                _sb.get("t_open_ms") or _sb.get("tOpenMs") or
                _sb.get("open_time_ms") or _sb.get("t") or 0
            )
            if not _sb_om or int(_sb_om) <= 0:
                continue
            # Same-touch candle is valid:
            # its open can be BEFORE freeze, but its close must be the touch_close_ms.
            _sb_cm = int(_sb_om) + int(tf_ms)
            _touch_close_ms_scan = int((watch or {}).get("touch_close_ms") or 0)
            _same_touch_scan = bool(
                _touch_close_ms_scan > 0
                and int(_sb_cm) == int(_touch_close_ms_scan)
            )

            # For later RC candles, require open after freeze.
            # For same-touch candle, allow open before freeze.
            if not _same_touch_scan and int(_sb_om) <= int(started_ms or 0):
                continue

            # Must be closed
            _sb_comp = _sb.get("complete") is True
            _sb_clk = (int(_sb_om) + int(tf_ms)) <= _sys_now_scan
            if not _sb_comp and not _sb_clk:
                continue

            # Must close >= min_reclaim
            if _min_reclaim > 0 and _sb_cm < int(_min_reclaim):
                continue
            _sb_cl = _bar_f(_sb, "c", "close")
            if _sb_cl is None:
                continue
            _sb_cl = float(_sb_cl)
            # -- RC TOUCH GATE (strict, additive) -------------------
            # RC candle must have REACHED the zone before closing beyond it.
            # SELL: high must reach zone low (rejection off resistance).
            # BUY:  low must reach zone high (rejection off support).
            _sb_hi = _bar_f(_sb, "h", "high")
            _sb_lo = _bar_f(_sb, "l", "low")
            if resolved_dir == "SELL":
                if _sb_hi is None or float(_sb_hi) < float(zl):
                    continue
            else:
                if _sb_lo is None or float(_sb_lo) > float(zh):
                    continue
            # -- END RC TOUCH GATE ----------------------------------
            if resolved_dir == "BUY" and _sb_cl > float(zh):
                _rc_bar = _sb
                _rc_bar_close = _sb_cl
                _rc_bar_open_ms = int(_sb_om)
                _rc_bar_closed_ms = _sb_cm
                break
            elif resolved_dir == "SELL" and _sb_cl < float(zl):
                _rc_bar = _sb
                _rc_bar_close = _sb_cl
                _rc_bar_open_ms = int(_sb_om)
                _rc_bar_closed_ms = _sb_cm
                break
    except Exception:
        _rc_bar = None

    if _rc_bar is not None:
        rev_ok = True
        # Override c, cl, hi, lo, closed_ms with RC bar values
        c = _rc_bar
        cl = _rc_bar_close
        hi = float(_bar_f(_rc_bar, "h", "high") or 0)
        lo = float(_bar_f(_rc_bar, "l", "low") or 0)
        closed_ms = _rc_bar_closed_ms
    elif resolved_dir == "BUY":
        rev_ok = bool(_rc_time_valid and float(cl) > float(zh)
                      and lo is not None and float(lo) <= float(zh))
    else:
        rev_ok = bool(_rc_time_valid and float(cl) < float(zl)
                      and hi is not None and float(hi) >= float(zl))

    gate["rev_ok"] = bool(rev_ok)
    # ------------------------------------------------------------
    # DEBUG: identify exact reversal candle
    # ------------------------------------------------------------
    try:
        gate["reversal_candidate"] = {
            "closed_ms": int(closed_ms),
            "open": float(_bar_f(c, "o", "open") or 0.0),
            "high": float(hi),
            "low": float(lo),
            "close": float(cl),
            "zone_low": float(zl),
            "zone_high": float(zh),
            "rule": (
                "BUY_CLOSE_ABOVE_ZONE_HIGH"
                if resolved_dir == "BUY"
                else "SELL_CLOSE_BELOW_ZONE_LOW"
            ),
            "rev_ok": bool(rev_ok),
        }
    except Exception:
        pass
    try:
        gate["frozen_zone_ui"] = (
            f"FROZEN {resolved_dir} "
            f"{float(zl):.2f}-{float(zh):.2f} "
            f"since={int(started_ms)}"
        )

        gate["reversal_ui"] = (
            f"CANDLE C={float(cl):.2f} "
            f"Z={float(zl):.2f}-{float(zh):.2f} "
            f"OK={bool(rev_ok)}"
        )
    except Exception:
        pass

    if rev_ok:
        # SINGLE SOURCE OF TRUTH:
        # Any RC shown to API/UI must first be persisted into Redis watch.
        # BREAK_CHECK/executor then use exactly the same RC trigger.
        try:
            if not isinstance(watch, dict):
                watch = {}

            watch.setdefault("started_ms", int(started_ms or now_ms_pick or 0))
            watch.setdefault("watch_created_ms", int((watch or {}).get("started_ms") or started_ms or now_ms_pick or 0))
            watch.setdefault("direction", str(resolved_dir).upper())
            watch.setdefault("tf", str(tfu))
            if isinstance(zone_used, dict):
                watch["zone_used"] = zone_used

            watch["state"] = "REV_OK"
            watch["rev_ok"] = True
            watch["rev_ok_ms"] = int(closed_ms)
            watch["last_checked_closed_ms"] = int(closed_ms or 0)
            watch["last_checked_close"] = float(cl)
            watch["last_checked_high"] = float(hi)
            watch["last_checked_low"] = float(lo)

            watch["frozen_zone_low"] = float(zl)
            watch["frozen_zone_high"] = float(zh)
            watch["frozen_zone_tf"] = str(tfu)

            watch["rev_ok_bar_hi"] = float(hi)
            watch["rev_ok_bar_lo"] = float(lo)
            watch["rev_ok_bar_close"] = float(cl)

            _dir_for_trigger = str(watch.get("direction") or resolved_dir).upper()
            _rc_open_ms = _to_ms_any(
                c.get("t_open_ms")
                or c.get("tOpenMs")
                or c.get("open_time_ms")
                or c.get("t")
                or c.get("time")
                or c.get("ts")
                or 0
            )

            watch["rc_open_ms"] = int(
                _rc_open_ms or 0
            )
            watch["rc_close_ms"] = int(
                closed_ms
            )

            watch["rc_high"] = float(hi)
            watch["rc_low"] = float(lo)
            watch["rc_close"] = float(cl)
            watch["trigger_level"] = float(lo) if _dir_for_trigger == "SELL" else float(hi)
            # POLICY: the touch candle itself is a valid RC if it closed
            # reclaiming the zone, even though its close time is <= watch
            # creation time. Persist WHY this RC was accepted so the
            # stored-RC revalidation path honors the same exception
            # instead of rolling it back on the next tick.
            watch["rc_is_touch_candle"] = bool(
                int((watch or {}).get("touch_close_ms") or 0) > 0
                and int(closed_ms or 0) == int((watch or {}).get("touch_close_ms") or 0)
            )
            # New RC must be allowed to send one new trigger alert.
            watch["discord_rc_trigger_sent"] = False
            watch.pop("discord_rc_trigger_sent_ms", None)
            watch.pop("discord_rc_trigger_price", None)
            watch.pop("discord_rc_trigger_error", None)
            watch["frozen_at_ms"] = int(now_ms_pick)
            watch["last_price"] = float((live_px if "live_px" in locals() else 0) or cl)
            watch["updated_at_ms"] = int(now_ms_pick)
            # -------------------------------------------------
            # Broker-truth guard:
            # Do not persist REV_OK while broker already has an
            # active XTL position for this symbol.
            # -------------------------------------------------
            try:
                _gate_device_id = str(
                    dev
                    or watch.get("device_id")
                    or watch.get("broker_device_id")
                    or ""
                ).strip()

                if _gate_device_id:
                    watch["device_id"] = _gate_device_id

                # Resolve the broker snapshot namespace for this device.
                # MT5 trade mode:
                #   0 = demo
                #   1 = contest
                #   2 = real
                _gate_account_type = str(
                    watch.get("mt5_account")
                    or watch.get("account_type")
                    or watch.get("broker_account_type")
                    or ""
                ).lower().strip()

                if _gate_account_type not in (
                    "demo",
                    "live",
                ):
                    try:
                        _trade_mode_raw = R.hget(
                            f"device:{_gate_device_id}",
                            "mt5_account_trade_mode",
                        )

                        _trade_mode = int(
                            _trade_mode_raw
                        )

                        if _trade_mode == 2:
                            _gate_account_type = "live"
                        elif _trade_mode in (0, 1):
                            _gate_account_type = "demo"
                        else:
                            _gate_account_type = ""

                    except Exception:
                        _gate_account_type = ""

                if _gate_account_type not in (
                    "demo",
                    "live",
                ):
                    log.error(
                        "[WATCHLIST] REV_OK_ACCOUNT_TYPE_MISSING "
                        "sym=%s side=%s device_id=%s key=%s",
                        sym_u,
                        dir_u,
                        _gate_device_id,
                        wkey,
                    )

                    gate["blocked"] = True
                    gate["stage"] = "BROKER_ACTIVE_GUARD"
                    gate["reason"] = "MT5_ACCOUNT_TYPE_MISSING"
                    gate["rev_ok"] = False
                    gate["rev_state"] = watch
                    return False, gate

                watch["mt5_account"] = _gate_account_type

                _active_bp = _find_xtl_broker_position(
                    R=R,
                    device_id=_gate_device_id,
                    symbol=sym_u,
                    account_type=_gate_account_type,
                )
                if _active_bp:
                    _active_side = str(_active_bp.get("side") or "").upper().strip()
                    if _active_side not in ("BUY", "SELL"):
                        try:
                            _active_side = "BUY" if int(_active_bp.get("type") or -1) == 0 else "SELL"
                        except Exception:
                            _active_side = ""

                    _clear_side = "SELL" if _active_side == "BUY" else "BUY"

                    try:
                        R.delete(
                            _watch_key(
                            uid_u,
                            sym_u,
                            _clear_side,
                            "H1",
                        )
                    )
                        R.delete(
                            break_state_key(
                                uid_u,
                                sym_u,
                                _clear_side,
                                "H1",
                            )
                        )
                    except Exception:
                        pass

                    try:
                        _active_ticket = int(
                            _active_bp.get("ticket") or 0
                        )
                    except Exception:
                        _active_ticket = 0

                    log.warning(
                        "[WATCHLIST] SKIP_REV_OK_ACTIVE_POSITION "
                        "sym=%s side=%s active_side=%s ticket=%s "
                        "cleared_side=%s device_id=%s "
                        "account_type=%s",
                        sym_u,
                        dir_u,
                        _active_side,
                        _active_ticket,
                        _clear_side,
                        _gate_device_id,
                        _gate_account_type,
                    )

                    gate["blocked"] = True
                    gate["stage"] = "BROKER_ACTIVE_GUARD"
                    gate["reason"] = "BROKER_POSITION_ACTIVE"
                    gate["trade_state"] = "TRADE_ACTIVE"
                    gate["active_trade_side"] = _active_side
                    gate["mt5_ticket"] = _active_ticket
                    gate["rev_ok"] = False
                    gate["rev_state"] = watch
                    return False, gate

            except Exception as _e:
                log.warning(
                    "[WATCHLIST] ACTIVE_POSITION_REV_OK_GUARD_EXC "
                    "sym=%s side=%s device_id=%s err=%r",
                    sym_u,
                    _dir_for_trigger,
                    (
                        _gate_device_id
                        if "_gate_device_id" in locals()
                        else ""
                    ),
                    _e,
                )

            _set_ok = bool(
                zone_watch_set(
                    R,
                    uid_u,
                    sym_u,
                    str(watch.get("direction") or resolved_dir).upper(),
                    json.dumps(watch, separators=(",", ":")),
                    tf=tfu,
                    ex=7 * 24 * 3600,
                )
            )
            try:
                _bs_key = break_state_key(
                    uid_u,
                    sym_u,
                    _dir_for_trigger,
                    "H1",
                )
                _prev_px = float(watch.get("rev_ok_bar_close") or watch.get("last_checked_close") or cl)

                R.set(
                    _bs_key,
                    json.dumps({
                        "symbol": sym_u,
                        "side": _dir_for_trigger,
                        "tf": "H1",
                        "prev": _prev_px,
                        "prev_price": _prev_px,
                        "trigger": float(watch.get("trigger_level") or 0),
                        "rev_ok_ms": int(watch.get("rev_ok_ms") or 0),
                        "watch_key": str(wkey),
                        "updated_at_ms": int(now_ms_pick),
                        "source": "REV_OK_INIT",
                    }, separators=(",", ":")),
                    ex=7 * 24 * 3600,
                )

                log.warning(
                    "[WATCHLIST] BREAK_STATE_INIT sym=%s side=%s key=%s prev=%s trigger=%s",
                    sym_u, _dir_for_trigger, _bs_key, _prev_px, watch.get("trigger_level")
                )
            except Exception as _e:
                log.warning(
                    "[WATCHLIST] BREAK_STATE_INIT_EXC sym=%s side=%s err=%r",
                    sym_u, _dir_for_trigger, _e
                )
            log.warning(
                "[WATCHLIST] REV_OK_SINGLE_TRUTH_PERSIST sym=%s side=%s key=%s set_ok=%s rev_ok_ms=%s trigger=%s",
                sym_u, _dir_for_trigger, wkey, bool(_set_ok), int(watch.get("rev_ok_ms") or 0), watch.get("trigger_level")
            )
        except Exception as e:
            log.warning(
                "[WATCHLIST] REV_OK_SINGLE_TRUTH_PERSIST_EXC sym=%s side=%s key=%s err=%r",
                sym_u, str(resolved_dir).upper(), wkey, e
            )

        gate["rev_ok"] = True
        gate["watch_key"] = str(wkey)
        gate["rev_state"] = {
            "state": "REV_OK",
            "started_ms": int((watch or {}).get("started_ms") or now_ms_pick),
            "rev_ok_ms": int(closed_ms),
            "direction": str((watch or {}).get("direction") or resolved_dir),
            "tf": str((watch or {}).get("tf") or tfu),
            "rev_ok_bar_hi": float(hi),
            "rev_ok_bar_lo": float(lo),
            "rev_ok_bar_close": float(cl),
        }
        gate["rev_trigger"] = {
            "entry_above": float(hi),
            "entry_below": float(lo),
        }
        gate["frozen_zone"] = {
            "low": float((watch or {}).get("frozen_zone_low", zl)),
            "high": float((watch or {}).get("frozen_zone_high", zh)),
            "tf": str((watch or {}).get("frozen_zone_tf", tfu)),
        }

        try:
            import datetime
            _freeze_dt = datetime.datetime.utcfromtimestamp(
                int((watch or {}).get("started_ms") or now_ms_pick or 0) / 1000
            ).strftime("%m/%d %H:%M")
            _rc_dt = datetime.datetime.utcfromtimestamp(
                int(closed_ms or 0) / 1000
            ).strftime("%m/%d %H:%M")
        except Exception:
            _freeze_dt = "?"
            _rc_dt = "?"

        try:
            _fzl = float((watch or {}).get("frozen_zone_low", zl))
            _fzh = float((watch or {}).get("frozen_zone_high", zh))
            if resolved_dir == "BUY":
                gate["reason"] = (
                    f"REV_OK | FZ {_fzl:.5f}-{_fzh:.5f} "
                    f"| FREEZE@{_freeze_dt} | RC@{_rc_dt} "
                    f"| RC {float(cl):.5f} "
                    f"| ENTRY > {float(hi):.5f}"
                )
            else:
                gate["reason"] = (
                    f"REV_OK | FZ {_fzl:.5f}-{_fzh:.5f} "
                    f"| FREEZE@{_freeze_dt} | RC@{_rc_dt} "
                    f"| RC {float(cl):.5f} "
                    f"| ENTRY < {float(lo):.5f}"
                )
        except Exception:
            gate["reason"] = "REV_OK"
        gate["reason"] = (
            f"{gate.get('reason')} "
            f"| RC_LOCKED | LIVE_BREAKOUT_ONLY "
            f"| TF={tfu}"
        )
        gate["stage"] = "REV"
        gate["blocked"] = False
        return True, gate




    
    
    
    # 6) INVALIDATION: 2 consecutive closed candles beyond zone boundary
    # Uses complete=True as primary signal, clock as fallback
    # Only counts candles that opened AFTER freeze (started_ms)
    consec = 0
    try:
        import time as _t4
        _sys_now_inv = int(_t4.time() * 1000)
        _freeze_ms_inv = int((watch or {}).get("started_ms") or 0)
        _bs_inv = sorted(
            [b for b in (bars or []) if isinstance(b, dict)],
            key=lambda b: _to_ms_any(b.get("t_open_ms") or b.get("tOpenMs") or
                                      b.get("open_time_ms") or b.get("t") or 0) or 0
        )
        for _cb in reversed(_bs_inv):
            _om_inv = _to_ms_any(
                _cb.get("t_open_ms") or _cb.get("tOpenMs") or
                _cb.get("open_time_ms") or _cb.get("t") or 0
            )
            if not _om_inv or int(_om_inv) <= 0:
                continue
            # Only bars opened AFTER freeze
            if int(_om_inv) <= int(_freeze_ms_inv or 0):
                break
            # Bar must be closed
            _is_comp_inv = _cb.get("complete") is True
            _clk_inv = (int(_om_inv) + int(tf_ms)) <= _sys_now_inv
            if not _is_comp_inv and not _clk_inv:
                continue
            _cv_inv = _bar_f(_cb, "c", "close")
            if _cv_inv is None:
                break
            _cv_inv = float(_cv_inv)
            if resolved_dir == "SELL":
                _bad_inv = _cv_inv >= float(zh)
            else:
                _bad_inv = _cv_inv <= float(zl)
            if _bad_inv:
                consec += 1
            else:
                break
            if consec >= int(hard_close_bars):
                break
    except Exception:
        consec = 0

    if consec >= int(hard_close_bars):
        try:
            for _s in ("BUY", "SELL"):
                for _t in ("H1", "H4"):
                    _wk_inv2 = _watch_key(
                        uid_u,
                        sym_u,
                        _s,
                        _t,
                    ) 
                    # GUARD: never delete a watch with a live/confirmed trade.
                    try:
                        _wj2 = _json_load(R.get(_wk_inv2)) or {}
                        _wst_inv2 = str(_wj2.get("state") or "").upper()
                    except Exception:
                        _wst_inv2 = ""
                    if _wst_inv2 in ("REV_OK", "ENTRY_READY", "ORDER_PENDING", "TRADE_ACTIVE"):
                        continue
                    R.delete(_wk_inv2)
        except Exception:
            pass
        gate["reason"] = (
            f"ZONE_INVALIDATED | FZ {float(zl):.5f}-{float(zh):.5f}"
            f" | {consec}/{int(hard_close_bars)} closes beyond zone | TF={tfu}"
        )
        gate["stage"] = "ZONE_INVALIDATED"
        gate["blocked"] = True
        gate["rev_ok"] = False
        gate["zone_used"] = zone_used
        return False, gate

    # If we reach here: watch active but not rev_ok and not invalidated
    try:
        import datetime
        _freeze_dt = datetime.datetime.utcfromtimestamp(
            int((watch or {}).get("started_ms") or started_ms or 0) / 1000
        ).strftime("%m/%d %H:%M")
        _candle_dt = datetime.datetime.utcfromtimestamp(
            int(closed_ms or 0) / 1000
        ).strftime("%m/%d %H:%M")
        _watch_created_dt = datetime.datetime.utcfromtimestamp(
            int((watch or {}).get("watch_created_ms") or 0) / 1000
        ).strftime("%m/%d %H:%M")
        gate["reason"] = (
            f"REV_WATCH | FZ {float(zl):.5f}-{float(zh):.5f} "
            f"| FREEZE@{_freeze_dt} | CREATED@{_watch_created_dt} "
            f"| C@{_candle_dt} {float(cl):.5f} "
            f"| NEED > {float(zh):.5f}"
            if resolved_dir == "BUY"
            else
            f"REV_WATCH | FZ {float(zl):.5f}-{float(zh):.5f} "
            f"| FREEZE@{_freeze_dt} | CREATED@{_watch_created_dt} "
            f"| C@{_candle_dt} {float(cl):.5f} "
            f"| NEED < {float(zl):.5f}"
        )
    except Exception:
        gate["reason"] = "REV_WATCH"
    gate["stage"] = "WATCH"
    gate["blocked"] = False
    return False, gate
