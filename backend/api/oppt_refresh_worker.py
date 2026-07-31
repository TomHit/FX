# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import logging
import os
import signal
import sys
import time
from typing import Any
from urllib.parse import urlencode

import httpx
import redis


LOG = logging.getLogger(
    "xtl.oppt_refresh_worker"
)

COMPUTE_BASE_URL = str(
    os.getenv(
        "OPPT_COMPUTE_BASE_URL",
        "http://127.0.0.1:8010",
    )
).rstrip("/")

INTERNAL_TOKEN = str(
    os.getenv(
        "OPPT_INTERNAL_TOKEN",
        "",
    )
).strip()

REDIS_URL = str(
    os.getenv(
        "REDIS_URL",
        "",
    )
).strip()

REQUEST_TIMEOUT_S = float(
    os.getenv(
        "OPPT_REFRESH_TIMEOUT_S",
        "180",
    )
)

BETWEEN_SCOPES_S = float(
    os.getenv(
        "OPPT_REFRESH_BETWEEN_SCOPES_S",
        "3",
    )
)

BETWEEN_CYCLES_S = float(
    os.getenv(
        "OPPT_REFRESH_BETWEEN_CYCLES_S",
        "5",
    )
)

HEARTBEAT_KEY = str(
    os.getenv(
        "OPPT_REFRESH_HEARTBEAT_KEY",
        "xtl:oppt:refresh_worker:heartbeat",
    )
).strip()

HEARTBEAT_TTL_S = int(
    os.getenv(
        "OPPT_REFRESH_HEARTBEAT_TTL_S",
        "120",
    )
)

DEFAULT_SYMBOLS = (
    "XAUUSD,EURUSD,USDJPY,"
    "GBPUSD,USDCAD,USDCHF"
)

STOP_REQUESTED = False


def _handle_stop(
    signum: int,
    _frame: Any,
) -> None:
    global STOP_REQUESTED

    STOP_REQUESTED = True

    LOG.warning(
        "[OPPT_REFRESH_WORKER] "
        "STOP_REQUESTED signal=%s",
        signum,
    )


def _decode_json_string(
    value: str | None,
) -> str:
    raw = str(
        value or ""
    ).strip()

    if not raw:
        return ""

    try:
        parsed = json.loads(raw)

        if isinstance(parsed, str):
            return parsed.strip()

    except Exception:
        pass

    return raw.strip('"').strip()


def _discover_scopes(
    redis_client: redis.Redis,
) -> list[dict]:
    scopes: list[dict] = []

    pattern = (
        "xtl:prop:*:profile:active"
    )

    for key in redis_client.scan_iter(
        match=pattern,
        count=100,
    ):
        key_s = str(
            key or ""
        ).strip()

        parts = key_s.split(":")

        #
        # Expected:
        # xtl:prop:<uid>:profile:active
        #
        if len(parts) != 5:
            continue

        if (
            parts[0] != "xtl"
            or parts[1] != "prop"
            or parts[3] != "profile"
            or parts[4] != "active"
        ):
            continue

        uid = str(
            parts[2] or ""
        ).strip()

        if not uid:
            continue

        active_raw = redis_client.get(
            key_s
        )

        profile_id = _decode_json_string(
            active_raw
        ).lower()

        if not profile_id:
            LOG.warning(
                "[OPPT_REFRESH_WORKER] "
                "ACTIVE_PROFILE_EMPTY "
                "uid=%s key=%s",
                uid,
                key_s,
            )
            continue

        profile_key = (
            f"xtl:prop:{uid}:"
            f"profile:{profile_id}"
        )

        profile_raw = redis_client.get(
            profile_key
        )

        if not profile_raw:
            LOG.error(
                "[OPPT_REFRESH_WORKER] "
                "ACTIVE_PROFILE_CONFIG_MISSING "
                "uid=%s profile=%s key=%s",
                uid,
                profile_id,
                profile_key,
            )
            continue

        try:
            profile = json.loads(
                profile_raw
            )
        except Exception:
            LOG.exception(
                "[OPPT_REFRESH_WORKER] "
                "PROFILE_JSON_INVALID "
                "uid=%s profile=%s",
                uid,
                profile_id,
            )
            continue

        if not isinstance(
            profile,
            dict,
        ):
            continue

        enabled = bool(
            profile.get(
                "enabled",
                False,
            )
        )

        if not enabled:
            LOG.info(
                "[OPPT_REFRESH_WORKER] "
                "ACTIVE_PROFILE_DISABLED "
                "uid=%s profile=%s",
                uid,
                profile_id,
            )
            continue

        account_login = str(
            profile.get(
                "account_login"
            )
            or ""
        ).strip()

        account_server = str(
            profile.get(
                "account_server"
            )
            or ""
        ).strip()

        broker_company = str(
            profile.get(
                "broker_company"
            )
            or ""
        ).strip()

        if (
            not account_login
            or not account_server
            or not broker_company
        ):
            LOG.error(
                "[OPPT_REFRESH_WORKER] "
                "ACTIVE_PROFILE_BROKER_IDENTITY_MISSING "
                "uid=%s profile=%s",
                uid,
                profile_id,
            )
            continue

        scopes.append(
            {
                "uid": uid,
                "profile_id": profile_id,
                "tf": "H1",
                "symbols": DEFAULT_SYMBOLS,
            }
        )

    scopes.sort(
        key=lambda row: (
            row["uid"],
            row["profile_id"],
        )
    )

    return scopes


def _heartbeat(
    redis_client: redis.Redis,
    *,
    status: str,
    scope: dict | None = None,
    duration_ms: int | None = None,
    error: str | None = None,
    scope_count: int | None = None,
) -> None:
    payload = {
        "pid": os.getpid(),
        "status": status,
        "ts_ms": int(
            time.time() * 1000
        ),
        "uid": (
            scope.get("uid")
            if isinstance(scope, dict)
            else None
        ),
        "profile_id": (
            scope.get("profile_id")
            if isinstance(scope, dict)
            else None
        ),
        "duration_ms": duration_ms,
        "scope_count": scope_count,
        "error": error,
    }

    try:
        redis_client.setex(
            HEARTBEAT_KEY,
            max(
                30,
                HEARTBEAT_TTL_S,
            ),
            json.dumps(
                payload,
                separators=(",", ":"),
                default=str,
            ),
        )

    except Exception:
        LOG.exception(
            "[OPPT_REFRESH_WORKER] "
            "HEARTBEAT_WRITE_FAILED"
        )


def _refresh_scope(
    client: httpx.Client,
    scope: dict,
) -> tuple[bool, int, str | None]:
    params = {
        "tf": scope["tf"],
        "symbols": scope["symbols"],
        "refresh_uid": scope["uid"],
        "refresh_profile_id": (
            scope["profile_id"]
        ),
    }

    url = (
        f"{COMPUTE_BASE_URL}"
        f"/trend/opportunities?"
        f"{urlencode(params)}"
    )

    started = time.monotonic()

    try:
        response = client.get(
            url,
            headers={
                "X-Internal-Refresh": (
                    INTERNAL_TOKEN
                ),
            },
        )

        duration_ms = int(
            (
                time.monotonic()
                - started
            )
            * 1000
        )

        if response.status_code != 200:
            return (
                False,
                duration_ms,
                (
                    f"HTTP_{response.status_code}:"
                    f"{response.text[:300]}"
                ),
            )

        payload = response.json()

        if not isinstance(
            payload,
            dict,
        ):
            return (
                False,
                duration_ms,
                "RESPONSE_NOT_OBJECT",
            )

        if not bool(
            payload.get("ok")
        ):
            return (
                False,
                duration_ms,
                (
                    "RESPONSE_NOT_OK:"
                    + str(
                        payload.get("reason")
                        or payload.get("detail")
                        or ""
                    )
                ),
            )

        rows = payload.get("rows")

        if not isinstance(
            rows,
            list,
        ):
            return (
                False,
                duration_ms,
                "ROWS_NOT_LIST",
            )

        if len(rows) != 6:
            return (
                False,
                duration_ms,
                (
                    "UNEXPECTED_ROW_COUNT:"
                    f"{len(rows)}"
                ),
            )

        response_uid = str(
            payload.get(
                "overlay_uid"
            )
            or ""
        ).strip()

        response_profile = str(
            payload.get(
                "overlay_profile_id"
            )
            or ""
        ).strip().lower()

        if response_uid != scope["uid"]:
            return (
                False,
                duration_ms,
                (
                    "RESPONSE_UID_MISMATCH:"
                    f"got={response_uid}"
                ),
            )

        if (
            response_profile
            != scope["profile_id"]
        ):
            return (
                False,
                duration_ms,
                (
                    "RESPONSE_PROFILE_MISMATCH:"
                    f"got={response_profile}"
                ),
            )

        return (
            True,
            duration_ms,
            None,
        )

    except Exception as exc:
        duration_ms = int(
            (
                time.monotonic()
                - started
            )
            * 1000
        )

        return (
            False,
            duration_ms,
            (
                f"{type(exc).__name__}:"
                f"{exc}"
            ),
        )


def _sleep_interruptibly(
    seconds: float,
) -> None:
    deadline = (
        time.monotonic()
        + max(
            0.0,
            float(seconds),
        )
    )

    while (
        not STOP_REQUESTED
        and time.monotonic()
        < deadline
    ):
        time.sleep(0.25)


def main() -> int:
    if not INTERNAL_TOKEN:
        LOG.error(
            "[OPPT_REFRESH_WORKER] "
            "OPPT_INTERNAL_TOKEN_MISSING"
        )
        return 2

    if not REDIS_URL:
        LOG.error(
            "[OPPT_REFRESH_WORKER] "
            "REDIS_URL_MISSING"
        )
        return 2

    redis_client = redis.Redis.from_url(
        REDIS_URL,
        decode_responses=True,
        socket_connect_timeout=5,
        socket_timeout=5,
    )

    try:
        redis_client.ping()
    except Exception:
        LOG.exception(
            "[OPPT_REFRESH_WORKER] "
            "REDIS_CONNECT_FAILED"
        )
        return 2

    LOG.warning(
        "[OPPT_REFRESH_WORKER] "
        "START pid=%s base_url=%s",
        os.getpid(),
        COMPUTE_BASE_URL,
    )

    timeout = httpx.Timeout(
        connect=10.0,
        read=REQUEST_TIMEOUT_S,
        write=10.0,
        pool=10.0,
    )

    limits = httpx.Limits(
        max_connections=1,
        max_keepalive_connections=1,
        keepalive_expiry=10.0,
    )

    with httpx.Client(
        timeout=timeout,
        limits=limits,
    ) as client:
        cycle_number = 0

        while not STOP_REQUESTED:
            cycle_number += 1

            try:
                scopes = _discover_scopes(
                    redis_client
                )
            except Exception:
                LOG.exception(
                    "[OPPT_REFRESH_WORKER] "
                    "SCOPE_DISCOVERY_FAILED"
                )

                _heartbeat(
                    redis_client,
                    status="DISCOVERY_ERROR",
                    error=(
                        "SCOPE_DISCOVERY_FAILED"
                    ),
                )

                _sleep_interruptibly(
                    BETWEEN_CYCLES_S
                )
                continue

            if not scopes:
                LOG.warning(
                    "[OPPT_REFRESH_WORKER] "
                    "NO_ACTIVE_SCOPES"
                )

                _heartbeat(
                    redis_client,
                    status="NO_ACTIVE_SCOPES",
                    scope_count=0,
                )

                _sleep_interruptibly(
                    BETWEEN_CYCLES_S
                )
                continue

            LOG.warning(
                "[OPPT_REFRESH_WORKER] "
                "SCOPES_DISCOVERED "
                "cycle=%s scopes=%s",
                cycle_number,
                [
                    (
                        row["uid"],
                        row["profile_id"],
                    )
                    for row in scopes
                ],
            )

            for scope in scopes:
                if STOP_REQUESTED:
                    break

                _heartbeat(
                    redis_client,
                    status="COMPUTING",
                    scope=scope,
                    scope_count=len(scopes),
                )

                LOG.warning(
                    "[OPPT_REFRESH_WORKER] "
                    "REFRESH_START "
                    "cycle=%s uid=%s profile=%s",
                    cycle_number,
                    scope["uid"],
                    scope["profile_id"],
                )

                ok, duration_ms, error = (
                    _refresh_scope(
                        client,
                        scope,
                    )
                )

                if ok:
                    LOG.warning(
                        "[OPPT_REFRESH_WORKER] "
                        "REFRESH_OK "
                        "cycle=%s uid=%s "
                        "profile=%s duration_ms=%s",
                        cycle_number,
                        scope["uid"],
                        scope["profile_id"],
                        duration_ms,
                    )

                    _heartbeat(
                        redis_client,
                        status="OK",
                        scope=scope,
                        duration_ms=duration_ms,
                        scope_count=len(scopes),
                    )

                else:
                    LOG.error(
                        "[OPPT_REFRESH_WORKER] "
                        "REFRESH_FAILED "
                        "cycle=%s uid=%s "
                        "profile=%s duration_ms=%s "
                        "error=%s",
                        cycle_number,
                        scope["uid"],
                        scope["profile_id"],
                        duration_ms,
                        error,
                    )

                    _heartbeat(
                        redis_client,
                        status="ERROR",
                        scope=scope,
                        duration_ms=duration_ms,
                        error=error,
                        scope_count=len(scopes),
                    )

                _sleep_interruptibly(
                    BETWEEN_SCOPES_S
                )

            _heartbeat(
                redis_client,
                status="CYCLE_COMPLETE",
                scope_count=len(scopes),
            )

            _sleep_interruptibly(
                BETWEEN_CYCLES_S
            )

    _heartbeat(
        redis_client,
        status="STOPPED",
    )

    LOG.warning(
        "[OPPT_REFRESH_WORKER] "
        "STOPPED pid=%s",
        os.getpid(),
    )

    return 0


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s "
            "%(levelname)s "
            "%(name)s "
            "%(message)s"
        ),
    )

    signal.signal(
        signal.SIGTERM,
        _handle_stop,
    )

    signal.signal(
        signal.SIGINT,
        _handle_stop,
    )

    sys.exit(main())
