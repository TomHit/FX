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
    """Scan all user state keys on startup and populate enabled_users set."""	
    try:
        import json
        keys = R.keys("xtl:strategy:oppt:state:*") or []
        for key in keys:
            try:
                uid = str(key).split(":")[-1]
                raw = R.get(key)
                if not raw:
                    continue
                st = json.loads(raw)
                if isinstance(st, dict) and st.get("enabled"):
                    R.sadd(ENABLED_USERS_KEY, uid)
                    log.info("[OPPT] startup: added uid=%s to enabled_users", uid)
            except Exception:
                pass
    except Exception as e:
        log.warning("[OPPT] startup sync failed: %r", e)


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
            if enabled_n <= 0:
                # hard backoff to avoid log spam + CPU
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
