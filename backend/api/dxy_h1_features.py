# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import logging
import time
from typing import Any

log = logging.getLogger("uvicorn.error")

TF = "H1"
TF_MS = 60 * 60 * 1000

SOURCE_VALUES = (
    "REAL_DXY",
    "SYNTHETIC_DXY",
)

FEATURES_PREFIX = "xtl:dxy:features:H1"
LATEST_PREFIX = "xtl:dxy:features:latest:H1"
TICK_LOCK_KEY = "xtl:dxy:h1:features:tick_lock"

TICK_LOCK_SEC = 20
FEATURE_TTL_SEC = 180 * 24 * 3600
LATEST_TTL_SEC = 7 * 24 * 3600


def _json_load(value: Any, default=None):
    """Safely decode a Redis JSON value without affecting the update loop."""
    if default is None:
        default = {}
    try:
        if value is None:
            return default
        if isinstance(value, (bytes, bytearray)):
            value = value.decode("utf-8", "ignore")
        obj = json.loads(value) if isinstance(value, str) else value
        if isinstance(obj, str):
            obj = json.loads(obj)
        return obj
    except Exception:
        return default

MAX_BARS = 300
MIN_FEATURE_BARS = 16


def _to_ms(value: Any) -> int:
    try:
        result = int(float(value or 0))
    except Exception:
        return 0

    if result <= 0:
        return 0

    return (
        result * 1000
        if result < 10_000_000_000
        else result
    )


def _bar_open_ms(bar: dict) -> int:
    if not isinstance(bar, dict):
        return 0

    return _to_ms(
        bar.get("t_open_ms")
        or bar.get("t")
        or bar.get("time")
        or 0
    )


def _bar_close_ms(bar: dict) -> int:
    if not isinstance(bar, dict):
        return 0

    close_ms = _to_ms(
        bar.get("t_close_ms")
        or bar.get("t_close")
        or 0
    )

    if close_ms > 0:
        return close_ms

    open_ms = _bar_open_ms(bar)

    return (
        open_ms + TF_MS
        if open_ms > 0
        else 0
    )


def _broker_offset_minutes(
    R,
    device_id: str,
) -> int:
    try:
        raw = R.hget(
            f"device:{device_id}",
            "broker_tz_offset_min",
        )

        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode(
                "utf-8",
                "ignore",
            )

        return int(float(raw or 0))

    except Exception:
        return 0


def _broker_to_utc_ms(
    broker_ms: int,
    offset_min: int,
) -> int:
    """Unix bar epochs are already UTC; broker offset is display metadata."""
    del offset_min
    return int(broker_ms or 0)


def _feature_key(
    source: str,
    device_id: str,
    broker_close_ms: int,
) -> str:
    return (
        f"{FEATURES_PREFIX}:"
        f"{source}:"
        f"{device_id}:"
        f"{int(broker_close_ms)}"
    )


def _latest_key(
    source: str,
    device_id: str,
) -> str:
    return (
        f"{LATEST_PREFIX}:"
        f"{source}:"
        f"{device_id}"
    )


def _load_source_bars(
    source: str,
    device_id: str,
) -> list[dict]:
    from api.xtl_analytics import (
        build_synthetic_dxy_h1_bars,
        load_real_dxy_bars,
    )

    if source == "REAL_DXY":
        return (
            load_real_dxy_bars(
                device_id=device_id,
                tf="H1",
                max_bars=MAX_BARS,
            )
            or []
        )

    return (
        build_synthetic_dxy_h1_bars(
            device_id=device_id,
            max_bars=MAX_BARS,
        )
        or []
    )


def _completed_bars(
    bars: list[dict],
) -> list[dict]:
    result = []

    for bar in bars or []:
        if not isinstance(bar, dict):
            continue

        # Never classify an explicitly forming candle.
        if bar.get("complete") is False:
            continue

        open_ms = _bar_open_ms(bar)
        close_ms = _bar_close_ms(bar)

        if (
            open_ms <= 0
            or close_ms <= open_ms
        ):
            continue

        result.append(bar)

    result.sort(
        key=lambda bar: _bar_open_ms(bar)
    )

    return result[-MAX_BARS:]


def _build_h1_snapshot(
    *,
    bars: list[dict],
    source: str,
    device_id: str,
    detected_at_ms: int,
    offset_min: int,
) -> dict:
    # Reuse the tested directional feature mathematics only.
    #
    # We deliberately do not reuse M15 Redis state, candidate lifecycle,
    # bootstrap, history, event claims or turn confirmation.
    from api.dxy_m15_tracker import (
        _feature_snapshot,
        _evidence_scores,
    )

    features = _feature_snapshot(bars)

    bull_score, bear_score, evidence_detail = (
        _evidence_scores(features)
    )

    if bull_score > bear_score:
        score_direction = "BULLISH"
    elif bear_score > bull_score:
        score_direction = "BEARISH"
    else:
        score_direction = "NEUTRAL"

    score_margin = abs(
        int(bull_score)
        - int(bear_score)
    )

    broker_close_ms = _bar_close_ms(
        bars[-1]
    )

    utc_close_ms = _broker_to_utc_ms(
        broker_close_ms,
        offset_min,
    )

    candidate_direction = str(
        features.get("candidate_direction")
        or "NEUTRAL"
    ).upper().strip()

    candidate_confidence = int(
        features.get("candidate_confidence")
        or 0
    )

    features.update({
        "schema_version": 1,
        "model": (
            "DXY_H1_DIRECTIONAL_FEATURES_V1_"
            "SHADOW_ANALYTICS"
        ),
        "timeframe": TF,
        "source": source,
        "device_id": device_id,

        "broker_bar_open_ms": _bar_open_ms(
            bars[-1]
        ),
        "broker_bar_close_ms": (
            broker_close_ms
        ),
        "bar_close_ms": utc_close_ms,
        "timestamp_basis": "UTC_EPOCH",
        "broker_offset_minutes": int(
            offset_min
        ),
        "detected_at_ms": int(
            detected_at_ms
        ),

        "candidate_direction": (
            candidate_direction
        ),
        "candidate_confidence": (
            candidate_confidence
        ),

        "bull_evidence_score": int(
            bull_score
        ),
        "bear_evidence_score": int(
            bear_score
        ),
        "evidence_direction": (
            score_direction
        ),
        "evidence_margin": int(
            score_margin
        ),
        "evidence_detail": (
            evidence_detail
        ),

        "bars_used": len(bars),

        # Explicit safety metadata.
        "shadow_only": True,
        "execution_wired": False,
        "entry_gate_wired": False,
        "risk_wired": False,
        "turn_lifecycle_enabled": False,

        # H1 SR will be wired separately through the shared trend_sr
        # integration. Do not mislabel M15 SR audit fields as native H1.
        "sr_audit": None,
        "sr_status": "NOT_YET_WIRED",
    })

    return features


def update_global_dxy_h1_features(
    *,
    R,
    now_ms: int | None = None,
) -> dict:
    detected_at_ms = int(
        now_ms
        or time.time() * 1000
    )

    stats = {
        "bindings": 0,
        "devices": 0,
        "real_available": 0,
        "synthetic_available": 0,
        "published": 0,
        "catchup_published": 0,
        "legacy_repaired": 0,
        "unchanged": 0,
        "errors": 0,
        "lock": False,
    }

    try:
        locked = R.set(
            TICK_LOCK_KEY,
            str(detected_at_ms),
            nx=True,
            ex=TICK_LOCK_SEC,
        )

        if not locked:
            return stats

    except Exception:
        return stats

    stats["lock"] = True

    try:
        from api.dxy_tracker import (
            _load_bindings,
        )

        bindings = _load_bindings(R) or []

        stats["bindings"] = len(bindings)
        stats["devices"] = len(bindings)

        for binding in bindings:
            device_id = str(
                binding.get("device_id")
                or ""
            ).strip()

            if not device_id:
                continue

            offset_min = (
                _broker_offset_minutes(
                    R,
                    device_id,
                )
            )

            for source in SOURCE_VALUES:
                try:
                    bars = _completed_bars(
                        _load_source_bars(
                            source,
                            device_id,
                        )
                    )

                    if len(bars) < MIN_FEATURE_BARS:
                        continue

                    if source == "REAL_DXY":
                        stats[
                            "real_available"
                        ] += 1
                    else:
                        stats[
                            "synthetic_available"
                        ] += 1

                    # Publish every unseen completed H1 bar, not only bars[-1].
                    # Each snapshot receives only its causal prefix.
                    unseen = []
                    for idx in range(MIN_FEATURE_BARS - 1, len(bars)):
                        broker_close_ms = _bar_close_ms(bars[idx])
                        if broker_close_ms <= 0:
                            continue
                        key = _feature_key(source, device_id, broker_close_ms)
                        if R.exists(key):
                            existing = _json_load(R.get(key), {})
                            existing_is_utc = (
                                isinstance(existing, dict)
                                and existing.get("timestamp_basis") == "UTC_EPOCH"
                                and int(existing.get("bar_close_ms") or 0)
                                == int(broker_close_ms)
                            )
                            if existing_is_utc:
                                stats["unchanged"] += 1
                                continue
                            # Repair only records carrying the proven legacy
                            # double-offset timestamp. Correct records remain
                            # immutable.
                            stats["legacy_repaired"] += 1
                        unseen.append((idx, broker_close_ms, key))

                    for unseen_pos, (idx, broker_close_ms, key) in enumerate(unseen):
                        snapshot = _build_h1_snapshot(
                            bars=bars[:idx + 1],
                            source=source,
                            device_id=device_id,
                            detected_at_ms=detected_at_ms,
                            offset_min=offset_min,
                        )
                        payload = json.dumps(
                            snapshot,
                            separators=(",", ":"),
                            allow_nan=False,
                        )
                        pipe = R.pipeline(transaction=False)
                        pipe.set(key, payload, ex=FEATURE_TTL_SEC)
                        pipe.set(
                            _latest_key(source, device_id),
                            payload,
                            ex=LATEST_TTL_SEC,
                        )
                        pipe.execute()
                        stats["published"] += 1
                        if unseen_pos < len(unseen) - 1:
                            stats["catchup_published"] += 1
                        log.info(
                            "[DXY_H1] FEATURE_PUBLISHED "
                            "source=%s device=%s broker_close_ms=%s "
                            "direction=%s confidence=%s bull=%s bear=%s",
                            source, device_id, broker_close_ms,
                            snapshot.get("candidate_direction"),
                            snapshot.get("candidate_confidence"),
                            snapshot.get("bull_evidence_score"),
                            snapshot.get("bear_evidence_score"),
                        )

                except Exception:
                    stats["errors"] += 1

                    log.exception(
                        "[DXY_H1] source update failed "
                        "source=%s device=%s",
                        source,
                        device_id,
                    )

    except Exception:
        stats["errors"] += 1

        log.exception(
            "[DXY_H1] global update failed"
        )

    return stats
