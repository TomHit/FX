# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import logging
import os
import signal
import time
from typing import Any

import redis

from api.strategy.po3_market_phase import evaluate_po3_market_phase


LOG = logging.getLogger("xtl.po3_shadow_worker")

REDIS_URL = str(os.getenv("REDIS_URL", "")).strip()

SYMBOLS = tuple(
    x.strip().upper()
    for x in os.getenv(
        "XTL_PO3_SHADOW_SYMBOLS",
        "XAUUSD,EURUSD,USDJPY,GBPUSD,USDCAD,USDCHF",
    ).split(",")
    if x.strip()
)

POLL_S = max(
    2.0,
    float(os.getenv("XTL_PO3_SHADOW_POLL_S", "5")),
)

STATE_TTL_S = max(
    86400,
    int(os.getenv("XTL_PO3_SHADOW_STATE_TTL_S", str(14 * 86400))),
)

LATEST_TTL_S = max(
    3600,
    int(os.getenv("XTL_PO3_SHADOW_LATEST_TTL_S", str(48 * 3600))),
)

STOP_REQUESTED = False


def _stop(signum: int, _frame: Any) -> None:
    global STOP_REQUESTED
    STOP_REQUESTED = True
    LOG.warning(
        "[PO3_SHADOW] STOP_REQUESTED signal=%s",
        signum,
    )


def _loads(raw: Any, default: Any = None) -> Any:
    if raw is None:
        return default
    try:
        return json.loads(raw)
    except Exception:
        return default


def _i(v: Any) -> int:
    try:
        return int(v or 0)
    except Exception:
        return 0


def _state_key(dev: str, sym: str) -> str:
    return f"xtl:po3:shadow:state:{dev}:{sym}"


def _latest_key(dev: str, sym: str) -> str:
    return f"xtl:po3:shadow:latest:{dev}:{sym}"


def _cursor_key(dev: str, sym: str) -> str:
    return f"xtl:po3:shadow:cursor:{dev}:{sym}"


def _ohlc_key(dev: str, sym: str, tf: str) -> str:
    return f"xtl:ohlc:snap:{dev}:{sym}:{tf}"


def _latest_dev_key(sym: str, tf: str) -> str:
    return f"xtl:ohlc:latest:{sym}:{tf}"


def _completed_bars(
    snap: dict,
    as_of_ms: int,
    tf_ms: int,
) -> list[dict]:
    out = []

    for b in snap.get("bars") or []:
        if not isinstance(b, dict):
            continue

        if b.get("complete") is False:
            continue

        close_ms = _i(b.get("t_close_ms"))

        if close_ms <= 0:
            open_ms = _i(b.get("t_open_ms"))

            if open_ms <= 0:
                open_ms = _i(b.get("t")) * 1000

            if open_ms <= 0:
                continue

            close_ms = open_ms + int(tf_ms)

        if close_ms <= as_of_ms:
            out.append(b)

    return out


def _projection(
    *,
    sym: str,
    dev: str,
    as_of_ms: int,
    po3: dict,
) -> dict:
    measurements = (
        po3.get("measurements")
        if isinstance(po3.get("measurements"), dict)
        else {}
    )

    semantics = (
        po3.get("shadow_semantics")
        if isinstance(po3.get("shadow_semantics"), dict)
        else {}
    )

    outside = (
        measurements.get("outside_episodes")
        if isinstance(
            measurements.get("outside_episodes"),
            dict,
        )
        else {}
    )

    active_src = (
        outside.get("active_episode")
        if isinstance(outside.get("active_episode"), dict)
        else None
    )

    active_distribution = None

    if active_src:
        raw_current = active_src.get(
            "current_outside_distance"
        )
        raw_max = active_src.get(
            "max_outside_distance"
        )

        retention = None
        giveback = None
        below_high_water = None

        try:
            cur = float(raw_current)
            mx = float(raw_max)

            if mx > 0:
                retention = cur / mx
                giveback = 1.0 - retention
                below_high_water = cur < mx
        except Exception:
            pass

        active_distribution = {
            "episode_id": active_src.get("episode_id"),
            "generation_id": active_src.get("generation_id"),
            "direction": active_src.get("direction"),
            "status": active_src.get("status"),
            "maturity": active_src.get(
                "distribution_maturity"
            ),
            "started_ms": active_src.get("started_ms"),
            "acceptance_ms": active_src.get(
                "acceptance_ms"
            ),
            "last_new_extreme_ms": active_src.get(
                "last_new_extreme_ms"
            ),
            "max_outside_ms": active_src.get(
                "max_outside_ms"
            ),
            "current_outside_distance": raw_current,
            "max_outside_distance": raw_max,
            "progress_retention": retention,
            "giveback_fraction": giveback,
            "below_high_water": below_high_water,
            "bars_since_acceptance": active_src.get(
                "bars_since_acceptance"
            ),
            "bars_since_extreme": active_src.get(
                "bars_since_last_new_extreme"
            ),
            "outside_close_count": active_src.get(
                "outside_close_count"
            ),
            "accepted_outside_close_count": active_src.get(
                "accepted_outside_close_count"
            ),
        }

    generation = (
        po3.get("generation")
        if isinstance(po3.get("generation"), dict)
        else {}
    )

    generation_id = (
        generation.get("id")
        or semantics.get("generation_id")
    )

    return {
        "schema_version": 1,
        "dataset": "PO3_LIVE_SHADOW",
        "analytics_only": True,
        "execution_authority": False,
        "symbol": sym,
        "device_id": dev,
        "as_of_ms": int(as_of_ms),
        "published_at_ms": int(time.time() * 1000),

        "generation_id": generation_id,

        "semantic": {
            "available": semantics.get("available"),
            "state": semantics.get("state"),
            "substate": semantics.get("substate"),
            "direction": semantics.get("direction"),
            "evidence_ms": semantics.get("evidence_ms"),
            "provenance": semantics.get("provenance"),
            "link_status": semantics.get("link_status"),
            "manipulation_id": semantics.get(
                "manipulation_id"
            ),
            "distribution_episode_id": semantics.get(
                "distribution_episode_id"
            ),
            "distribution_maturity": semantics.get(
                "distribution_maturity"
            ),
            "reason": semantics.get("reason"),
        },

        "active_distribution": active_distribution,
    }


def _evaluate_symbol(
    R: redis.Redis,
    sym: str,
) -> None:
    dev_m15 = str(
        R.get(_latest_dev_key(sym, "M15")) or ""
    ).strip()

    dev_h1 = str(
        R.get(_latest_dev_key(sym, "H1")) or ""
    ).strip()

    if not dev_m15 or not dev_h1:
        return

    # Never mix broker/device streams.
    if dev_m15 != dev_h1:
        LOG.warning(
            "[PO3_SHADOW] DEVICE_MISMATCH "
            "sym=%s m15=%s h1=%s",
            sym,
            dev_m15,
            dev_h1,
        )
        return

    dev = dev_m15

    raw_m15, raw_h1 = R.mget(
        _ohlc_key(dev, sym, "M15"),
        _ohlc_key(dev, sym, "H1"),
    )

    m15 = _loads(raw_m15, {})
    h1 = _loads(raw_h1, {})

    if not isinstance(m15, dict) or not isinstance(h1, dict):
        return

    newest_m15_close_ms = _i(m15.get("lastClosedTs"))
    h1_close_ms = _i(h1.get("lastClosedTs"))

    if newest_m15_close_ms <= 0 or h1_close_ms <= 0:
        return

    cursor_key = _cursor_key(dev, sym)
    previous_cursor = _i(R.get(cursor_key))

    if previous_cursor >= newest_m15_close_ms:
        return

    state_key = _state_key(dev, sym)
    previous_state = _loads(
        R.get(state_key),
        {},
    )

    if not isinstance(previous_state, dict):
        previous_state = {}

    # ---------------------------------------------------------
    # Build the exact causal M15 boundaries available in the
    # retained snapshot. Never synthesize missing timestamps.
    #
    # Bootstrap:
    #   no cursor -> evaluate newest completed M15 only.
    #
    # Restart/catch-up:
    #   cursor exists -> evaluate every retained completed M15
    #   close after the cursor, in chronological order.
    # ---------------------------------------------------------
    if previous_cursor > 0:
        completed_all = _completed_bars(
            m15,
            newest_m15_close_ms,
            15 * 60 * 1000,
        )

        boundaries = []

        for b in completed_all:
            close_ms = _i(b.get("t_close_ms"))

            if close_ms <= 0:
                open_ms = _i(b.get("t_open_ms"))

                if open_ms <= 0:
                    open_ms = _i(b.get("t")) * 1000

                if open_ms <= 0:
                    continue

                close_ms = (
                    open_ms
                    + 15 * 60 * 1000
                )

            if (
                close_ms > previous_cursor
                and close_ms <= newest_m15_close_ms
            ):
                boundaries.append(close_ms)

        boundaries = sorted(set(boundaries))

        if not boundaries:
            LOG.warning(
                "[PO3_SHADOW] CATCHUP_NO_BOUNDARIES "
                "sym=%s dev=%s cursor=%s newest=%s",
                sym,
                dev,
                previous_cursor,
                newest_m15_close_ms,
            )
            return

        # The first retained boundary after a persisted cursor
        # must be the immediately following M15 close. If not,
        # the snapshot has a causal history gap; do not jump it.
        expected_first = (
            previous_cursor
            + 15 * 60 * 1000
        )

        if boundaries[0] != expected_first:
            LOG.warning(
                "[PO3_SHADOW] CATCHUP_GAP "
                "sym=%s dev=%s cursor=%s "
                "first=%s newest=%s",
                sym,
                dev,
                previous_cursor,
                boundaries[0],
                newest_m15_close_ms,
            )
            return

        # Every retained boundary must remain contiguous.
        for prev_ms, next_ms in zip(
            boundaries,
            boundaries[1:],
        ):
            if (
                next_ms - prev_ms
                != 15 * 60 * 1000
            ):
                LOG.warning(
                    "[PO3_SHADOW] CATCHUP_GAP "
                    "sym=%s dev=%s prev=%s "
                    "next=%s newest=%s",
                    sym,
                    dev,
                    prev_ms,
                    next_ms,
                    newest_m15_close_ms,
                )
                return
    else:
        boundaries = [newest_m15_close_ms]

    working_state = previous_state
    final_po3 = None
    final_as_of_ms = 0
    final_h1_count = 0
    final_m15_count = 0

    for as_of_ms in boundaries:
        h1_bars = _completed_bars(
            h1,
            as_of_ms,
            60 * 60 * 1000,
        )

        m15_bars = _completed_bars(
            m15,
            as_of_ms,
            15 * 60 * 1000,
        )

        if len(h1_bars) < 20 or len(m15_bars) < 20:
            LOG.warning(
                "[PO3_SHADOW] BARS_SHORT "
                "sym=%s dev=%s as_of_ms=%s "
                "h1=%s m15=%s",
                sym,
                dev,
                as_of_ms,
                len(h1_bars),
                len(m15_bars),
            )
            return

        po3 = evaluate_po3_market_phase(
            symbol=sym,
            h1_bars=h1_bars,
            m15_bars=m15_bars,
            as_of_ms=as_of_ms,
            previous_state=working_state,
        )

        if not isinstance(po3, dict):
            raise RuntimeError(
                "PO3_RESULT_NOT_DICT"
            )

        # Preserve exactly the state contract expected by
        # the engine for the next causal boundary.
        working_state = {
            "generation": (
                po3.get("generation")
                if isinstance(
                    po3.get("generation"),
                    dict,
                )
                else None
            ),
            "lifecycle_state": (
                po3.get("lifecycle_state")
                if isinstance(
                    po3.get("lifecycle_state"),
                    dict,
                )
                else {}
            ),
        }

        final_po3 = po3
        final_as_of_ms = as_of_ms
        final_h1_count = len(h1_bars)
        final_m15_count = len(m15_bars)

    if not isinstance(final_po3, dict):
        return

    projection = _projection(
        sym=sym,
        dev=dev,
        as_of_ms=final_as_of_ms,
        po3=final_po3,
    )

    # One atomic checkpoint after the complete causal chain.
    # If any intermediate evaluation raises/fails, Redis keeps
    # the old cursor/state and the next poll retries the chain.
    pipe = R.pipeline(transaction=True)

    pipe.setex(
        state_key,
        STATE_TTL_S,
        json.dumps(
            working_state,
            separators=(",", ":"),
            default=str,
        ),
    )

    pipe.setex(
        _latest_key(dev, sym),
        LATEST_TTL_S,
        json.dumps(
            projection,
            separators=(",", ":"),
            default=str,
        ),
    )

    # Cursor last: state/projection correspond to this boundary.
    pipe.setex(
        cursor_key,
        STATE_TTL_S,
        str(final_as_of_ms),
    )

    pipe.execute()

    LOG.info(
        "[PO3_SHADOW] EVALUATED "
        "sym=%s dev=%s as_of_ms=%s "
        "catchup_steps=%s h1=%s m15=%s "
        "generation=%s semantic=%s",
        sym,
        dev,
        final_as_of_ms,
        len(boundaries),
        final_h1_count,
        final_m15_count,
        projection.get("generation_id"),
        (
            projection.get("semantic") or {}
        ).get("state"),
    )


def main() -> int:
    if not REDIS_URL:
        raise RuntimeError("REDIS_URL_NOT_SET")

    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format=(
            "%(asctime)s %(levelname)s "
            "%(name)s %(message)s"
        ),
    )

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    R = redis.Redis.from_url(
        REDIS_URL,
        decode_responses=True,
        socket_timeout=5,
        socket_connect_timeout=5,
        health_check_interval=30,
    )

    R.ping()

    LOG.info(
        "[PO3_SHADOW] START symbols=%s poll_s=%s",
        ",".join(SYMBOLS),
        POLL_S,
    )

    while not STOP_REQUESTED:
        started = time.monotonic()

        for sym in SYMBOLS:
            if STOP_REQUESTED:
                break

            try:
                _evaluate_symbol(R, sym)
            except Exception:
                LOG.exception(
                    "[PO3_SHADOW] EVALUATE_FAILED sym=%s",
                    sym,
                )

        elapsed = time.monotonic() - started
        sleep_s = max(0.25, POLL_S - elapsed)

        end = time.monotonic() + sleep_s

        while (
            not STOP_REQUESTED
            and time.monotonic() < end
        ):
            time.sleep(
                min(
                    0.25,
                    max(0.0, end - time.monotonic()),
                )
            )

    LOG.warning("[PO3_SHADOW] STOPPED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
