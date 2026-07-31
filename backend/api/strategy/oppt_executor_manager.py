import os
import threading
import time
import logging

from .oppt_executor import tick_all_enabled_users, EXECUTOR_SLEEP_SEC, R

log = logging.getLogger("uvicorn.error")

_started = False
_thread: threading.Thread | None = None

# NEW: avoid SCAN loops when nothing is enabled
ENABLED_USERS_KEY = "xtl:strategy:oppt:enabled_users"


def _sync_enabled_users_on_startup() -> None:
    """
    Rebuild the enabled-users set from persisted strategy states.

    This repairs missing and stale set membership after an API restart.
    Runtime strategy enable/disable must also update the set immediately.
    """
    try:
        import json

        enabled_uids = set()

        for key in R.scan_iter(
            match="xtl:strategy:oppt:state:*",
            count=200,
        ):
            try:
                key_s = (
                    key.decode("utf-8", "ignore")
                    if isinstance(key, (bytes, bytearray))
                    else str(key)
                )

                uid = key_s.rsplit(":", 1)[-1].strip()

                if not uid:
                    continue

                raw = R.get(key)

                if not raw:
                    continue

                if isinstance(raw, (bytes, bytearray)):
                    raw = raw.decode("utf-8", "replace")

                state = json.loads(raw)

                if (
                    isinstance(state, dict)
                    and bool(state.get("enabled"))
                ):
                    enabled_uids.add(uid)

            except Exception:
                log.exception(
                    "[OPPT] startup enabled-user read failed "
                    "key=%r",
                    key,
                )

        pipe = R.pipeline(transaction=True)

        pipe.delete(ENABLED_USERS_KEY)

        if enabled_uids:
            pipe.sadd(
                ENABLED_USERS_KEY,
                *sorted(enabled_uids),
            )

        pipe.execute()

        log.warning(
            "[OPPT] startup enabled-users rebuilt "
            "count=%s users=%s",
            len(enabled_uids),
            sorted(enabled_uids),
        )

    except Exception:
        log.exception(
            "[OPPT] startup enabled-users rebuild failed"
        )

def start_oppt_executor_manager() -> None:
    """
    Starts one background thread per API process.

    Safe with multi-workers because per-user locks prevent double execution.

    Perf fixes:
      - No Redis SCAN in steady-state (uses ENABLED_USERS_KEY set maintained by _load_state()).
      - Adaptive sleep: when no enabled users, backs off hard (reduces CPU/log spam).
      - Heartbeat includes enabled/ticked counts for debugging.
    """
    global _started, _thread

    # If thread already exists and is alive, nothing to do
    if _thread is not None and _thread.is_alive():
        _started = True
        return
    if _started:
        return

    _started = True
    _sync_enabled_users_on_startup()

    # Repair unresolved analytics rows before the first close sweep. This is
    # safe when the agent was offline: reconciliation uses broker deal truth,
    # while the later sweep refuses unverified broker snapshots.
    try:
        from api.xtl_analytics import (
            reconcile_pending_broker_truth,
            reseed_pending_broker_truth,
        )

        _seed = reseed_pending_broker_truth() or {}
        _recon = reconcile_pending_broker_truth() or {}
        log.info(
            "[ANALYTICS] startup reseed=%s reconcile_upgraded=%s errors=%s",
            _seed.get("reseeded"),
            _recon.get("upgraded"),
            int(_seed.get("errors") or 0) + int(_recon.get("errors") or 0),
        )
    except Exception:
        log.exception("[ANALYTICS] startup reconciliation failed")

    def loop() -> None:
        pid = os.getpid()
        log.info("[OPPT] manager loop ENTER pid=%s base_sleep=%ss", pid, EXECUTOR_SLEEP_SEC)

        hb_every = 10  # seconds
        last_hb = 0
        last_stats = {"enabled": 0, "ticked": 0}

        while True:
            now = int(time.time())
            now_ms = now * 1000

            # enabled count (cheap: SCARD on a set)
            enabled_n = 0
            try:
                enabled_n = int(R.scard(ENABLED_USERS_KEY) or 0)
            except Exception:
                enabled_n = 0

            # Heartbeat (throttled)
            if now - last_hb >= hb_every:
                last_hb = now
                try:
                    # include enabled + last tick stats for debugging
                    R.set(
                        "xtl:strategy:oppt:executor_heartbeat",
                        f"{now_ms}|pid={pid}|enabled={enabled_n}|ticked={int(last_stats.get('ticked') or 0)}",
                        ex=60,
                    )
                except Exception:
                    pass

            # If nobody enabled -> do nothing, sleep longer
            # ---------------------------------------------------------
            # Global, device-aligned DXY state tracking.
            #
            # This must run before the enabled-user early exit because
            # DXY direction changes are market events, not trade events.
            # ---------------------------------------------------------
            try:
                from api.dxy_tracker import (
                    update_global_dxy_state,
                )

                _dxy_stats = update_global_dxy_state(
                    R=R,
                    now_ms=int(now_ms),
                )

                if (
                    _dxy_stats
                    and (
                        int(
                            _dxy_stats.get(
                                "initialized"
                            )
                            or 0
                        ) > 0
                        or int(
                            _dxy_stats.get(
                                "changed"
                            )
                            or 0
                        ) > 0
                        or int(
                            _dxy_stats.get(
                                "errors"
                            )
                            or 0
                        ) > 0
                    )
                ):
                    log.info(
                        "[DXY_TRACKER] tick "
                        "devices=%s real=%s synthetic=%s "
                        "initialized=%s changed=%s errors=%s",
                        _dxy_stats.get("devices"),
                        _dxy_stats.get(
                            "real_available"
                        ),
                        _dxy_stats.get(
                            "synthetic_available"
                        ),
                        _dxy_stats.get(
                            "initialized"
                        ),
                        _dxy_stats.get("changed"),
                        _dxy_stats.get("errors"),
                    )

            except Exception:
                log.exception(
                    "[DXY_TRACKER] manager update failed "
                    "pid=%s",
                    pid,
                )

            # ---------------------------------------------------------
            # M15 DXY turn detector (shadow analytics only).
            # Independent lock/state/history; never affects trade execution.
            # ---------------------------------------------------------
            try:
                from api.dxy_m15_tracker import (
                    update_global_dxy_m15_state,
                )

                _dxy_m15_stats = update_global_dxy_m15_state(
                    R=R,
                    now_ms=int(now_ms),
                )

                if (
                    _dxy_m15_stats
                    and (
                        int(_dxy_m15_stats.get("bootstrapped") or 0) > 0
                        or int(_dxy_m15_stats.get("evaluated") or 0) > 0
                        or int(_dxy_m15_stats.get("candidate_events") or 0) > 0
                        or int(_dxy_m15_stats.get("errors") or 0) > 0
                    )
                ):
                    log.info(
                        "[DXY_M15] tick devices=%s real=%s synthetic=%s "
                        "series=%s bootstrapped=%s evaluated=%s "
                        "candidate_events=%s errors=%s",
                        _dxy_m15_stats.get("devices"),
                        _dxy_m15_stats.get("real_available"),
                        _dxy_m15_stats.get("synthetic_available"),
                        _dxy_m15_stats.get("series_built"),
                        _dxy_m15_stats.get("bootstrapped"),
                        _dxy_m15_stats.get("evaluated"),
                        _dxy_m15_stats.get("candidate_events"),
                        _dxy_m15_stats.get("errors"),
                    )

            except Exception:
                log.exception(
                    "[DXY_M15] manager update failed pid=%s",
                    pid,
                )

            # Analytics truth maintenance is independent of whether strategy
            # execution is enabled. Reconcile first, then sweep. The sweep itself
            # skips owners whose broker-open snapshot is unavailable/unverified.
            try:
                from api.xtl_analytics import (
                    reconcile_pending_broker_truth,
                    sweep_closed_trades,
                )

                _recon = reconcile_pending_broker_truth() or {}
                _sw = sweep_closed_trades() or {}

                if (
                    int(_recon.get("upgraded") or 0) > 0
                    or int(_sw.get("finalized") or 0) > 0
                    or int(_sw.get("skipped_unverified") or 0) > 0
                    or int(_recon.get("errors") or 0) > 0
                    or int(_sw.get("errors") or 0) > 0
                ):
                    log.info(
                        "[ANALYTICS] reconcile upgraded=%s checked=%s "
                        "sweep finalized=%s checked=%s skipped_unverified=%s errors=%s",
                        _recon.get("upgraded"),
                        _recon.get("checked"),
                        _sw.get("finalized"),
                        _sw.get("checked"),
                        _sw.get("skipped_unverified"),
                        int(_recon.get("errors") or 0) + int(_sw.get("errors") or 0),
                    )
            except Exception:
                log.exception("[ANALYTICS] lifecycle maintenance error pid=%s", pid)

            # Strategy execution can sleep longer when nobody is enabled.
            # Global DXY and analytics maintenance above have already run.
            if enabled_n <= 0:
                time.sleep(10)
                continue

            # Work cycle
            try:
                # tick_all_enabled_users should NOT scan keys; it should read SMEMBERS of ENABLED_USERS_KEY
                # and return stats like {"enabled": N, "ticked": M}
                last_stats = tick_all_enabled_users(max_users=min(500, enabled_n)) or {"enabled": enabled_n, "ticked": 0}
            except Exception:
                log.exception("[OPPT] manager loop error pid=%s", pid)
                last_stats = {"enabled": enabled_n, "ticked": 0}
            # Adaptive sleep:
            # - base sleep when enabled users exist
            # - if nothing ticked (locks busy / transient), add a little backoff
            sleep_s = max(1, int(EXECUTOR_SLEEP_SEC))
            try:
                ticked = int(last_stats.get("ticked") or 0)
            except Exception:
                ticked = 0
            if ticked <= 0:
                sleep_s = max(sleep_s, 3)

            time.sleep(sleep_s)

    _thread = threading.Thread(target=loop, name="oppt_executor", daemon=True)
    _thread.start()
    log.info(
        "[OPPT] executor manager started pid=%s thread_alive=%s",
        os.getpid(),
        _thread.is_alive(),
    )


def oppt_executor_debug() -> dict:
    """Optional: use in a debug endpoint to verify loop status."""
    t = _thread
    return {
        "pid": os.getpid(),
        "started": _started,
        "thread_alive": bool(t and t.is_alive()),
        "thread_name": getattr(t, "name", None) if t else None,
        "sleep_sec": int(EXECUTOR_SLEEP_SEC),
    }
