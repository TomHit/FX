"""
mt5_launcher.py -- safe MT5 auto-launch for XTL.

Import from the supervisor loop ONLY. Never call from mt5_fetch_rates or any
per-poll code path.

Background
----------
On 2026-08-13 a terminal was spawned from an environment with no APPDATA. MT5
expanded %APPDATA%\\MetaQuotes\\Terminal\\<hash> to \\MetaQuotes\\Terminal\\<hash>,
which resolved to C:\\MetaQuotes\\... -- unwritable at the drive root. Every
login then failed at "initialization of month base by time failed", dropped the
connection, and retried ~1/sec. Result: 5,547 auth/sync cycles against the
broker in one day, and a hyperactivity warning.

Guards
------
1. Never launch if a terminal is already running.
2. Never launch from a service (LocalSystem) context -- no usable profile.
3. Refuse unless APPDATA resolves to a real folder containing a MetaQuotes
   profile.
4. After launch, read terminal_info().data_path and KILL the process if it is
   not under APPDATA. This is the direct check for the failure above.
5. Consecutive-failure backoff, persisted to disk so a crash-restart loop
   cannot reset it. Successes cost nothing and reset the counter to zero.
"""

import json
import os
import subprocess
import time
from pathlib import Path

try:
    from .mt5_client import _log, _mt5_running, reg_get
except ImportError:
    from mt5_client import _log, _mt5_running, reg_get

# ---------------------------------------------------------------- limits ----
# NO daily cap. A launch that succeeds and passes the data_path check is
# harmless, so successes are not counted. Only CONSECUTIVE FAILURES count, and
# any verified success resets them to zero. Restarting the agent, however many
# times a day, never consumes anything.
_FAIL_BACKOFF_S = [300, 600, 1200, 2400, 3600]   # 5m, 10m, 20m, 40m, 60m
_FAIL_LIMIT = len(_FAIL_BACKOFF_S)               # then stop entirely
_CONNECT_TIMEOUT_S = 90                          # cold start + auto-login

_STATE_FILE = (
    Path(os.environ.get("ProgramData", r"C:\ProgramData"))
    / "XTL"
    / "mt5_launch_state.json"
)

# The exact failure mode we are defending against.
_GHOST_PREFIX = r"C:\MetaQuotes".lower()


# ------------------------------------------------- persisted fail state ----
def _load_state() -> dict:
    try:
        return json.loads(_STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {"fail_count": 0, "last_fail_ts": 0.0}


def _save_state(st: dict) -> None:
    try:
        _STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        _STATE_FILE.write_text(json.dumps(st), encoding="utf-8")
    except Exception as e:
        _log(f"[mt5_launch] state write failed: {e}")


def _may_attempt() -> bool:
    st = _load_state()
    n = int(st.get("fail_count", 0))

    if n == 0:
        return True                                   # clean slate

    if n >= _FAIL_LIMIT:
        _log(
            f"[mt5_launch] refused: {n} consecutive failures; launcher "
            f"disabled. Fix the cause, then call reset_launch_state()."
        )
        return False

    wait = _FAIL_BACKOFF_S[n - 1]
    since = time.time() - float(st.get("last_fail_ts") or 0)
    if since < wait:
        _log(
            f"[mt5_launch] refused: backoff after {n} failure(s), "
            f"{int(wait - since)}s remaining"
        )
        return False
    return True


def _record_failure() -> None:
    st = _load_state()
    st["fail_count"] = int(st.get("fail_count", 0)) + 1
    st["last_fail_ts"] = time.time()
    _save_state(st)
    _log(f"[mt5_launch] failure #{st['fail_count']} recorded")


def _record_success() -> None:
    _save_state({"fail_count": 0, "last_fail_ts": 0.0})


def reset_launch_state() -> None:
    """Operator escape hatch after the breaker trips."""
    _save_state({"fail_count": 0, "last_fail_ts": 0.0})
    _log("[mt5_launch] failure state manually reset")


def launcher_state() -> dict:
    """Expose state for health endpoints / logging."""
    st = _load_state()
    n = int(st.get("fail_count", 0))
    return {
        "fail_count": n,
        "disabled": n >= _FAIL_LIMIT,
        "last_fail_ts": st.get("last_fail_ts", 0.0),
    }


# ----------------------------------------------- environment validation ----
def _verified_env():
    """
    Return (env, appdata) only if this environment can produce a correct MT5
    data folder. Returns (None, reason_string) otherwise.
    """
    env = os.environ.copy()

    appdata = (env.get("APPDATA") or "").strip()
    if not appdata:
        prof = (env.get("USERPROFILE") or "").strip()
        if not prof:
            return None, "APPDATA and USERPROFILE both empty"
        appdata = os.path.join(prof, "AppData", "Roaming")
        env["APPDATA"] = appdata
        env.setdefault("LOCALAPPDATA", os.path.join(prof, "AppData", "Local"))
        _log(f"[mt5_launch] APPDATA reconstructed -> {appdata}")

    if not os.path.isdir(appdata):
        return None, f"APPDATA does not exist: {appdata}"

    # An existing MetaQuotes profile proves this is the right roaming folder.
    mq = os.path.join(appdata, "MetaQuotes", "Terminal")
    if not os.path.isdir(mq):
        return None, f"no MetaQuotes profile under {mq}"

    return env, appdata


# ---------------------------------------------- post-launch validation ----
def _data_path_is_sane(appdata: str) -> bool:
    """
    Read the terminal's ACTUAL data folder and reject anything not under
    APPDATA. Direct check for the C:\\MetaQuotes failure.
    """
    try:
        import MetaTrader5 as MT5

        ti = MT5.terminal_info()
        dp = str(getattr(ti, "data_path", "") or "")
    except Exception as e:
        _log(f"[mt5_launch] data_path check failed: {e}")
        return False

    if not dp:
        _log("[mt5_launch] data_path empty; treating as unsafe")
        return False

    low = dp.lower()
    if low.startswith(_GHOST_PREFIX):
        _log(f"[mt5_launch] REJECT ghost data folder: {dp}")
        return False
    if not low.startswith(appdata.lower()):
        _log(f"[mt5_launch] REJECT data_path outside APPDATA: {dp}")
        return False

    _log(f"[mt5_launch] data_path OK: {dp}")
    return True


def _kill(proc) -> None:
    try:
        proc.terminate()
        proc.wait(timeout=10)
    except Exception:
        try:
            proc.kill()
        except Exception:
            pass


# ------------------------------------------------------- public entry ----
def try_launch_mt5() -> bool:
    """
    Launch MT5 once, safely.

    Returns True only if a terminal is up, connected, and using the correct
    data folder. Internally rate-limited, so it is safe to call on every
    supervisor tick; a False return is normal and needs no handling.
    """
    import MetaTrader5 as MT5

    # --- guard 1: already running -----------------------------------------
    if _mt5_running():
        return True

    # --- guard 2: never launch from a service context ----------------------
    try:
        try:
            from .mt5_client import _is_localsystem
        except ImportError:
            from mt5_client import _is_localsystem
        if _is_localsystem():
            _log(
                "[mt5_launch] refused: running as LocalSystem, not a user "
                "session"
            )
            return False
    except Exception:
        pass

    # --- guard 3: backoff / breaker ---------------------------------------
    if not _may_attempt():
        return False

    # --- guard 4: environment must be sane --------------------------------
    env, appdata = _verified_env()
    if env is None:
        _log(f"[mt5_launch] refused: {appdata}")   # appdata holds the reason
        return False

    # Prefer the env var the service exports; fall back to the registry.
    exe = (
        os.environ.get("XTL_MT5_PATH")
        or reg_get("MT5.TerminalPath")
        or reg_get("MT5Path")
        or ""
    ).strip()
    if not exe or not os.path.isfile(exe):
        _log(f"[mt5_launch] refused: terminal path invalid: '{exe}'")
        return False

    _log(f"[mt5_launch] launching {exe}")
    try:
        proc = subprocess.Popen(
            [exe],
            env=env,
            cwd=os.path.dirname(exe),
            close_fds=True,
        )
    except Exception as e:
        _log(f"[mt5_launch] Popen failed: {e}")
        _record_failure()
        return False

    # Wait for the terminal to come up, log in, and connect.
    deadline = time.time() + _CONNECT_TIMEOUT_S
    connected = False
    while time.time() < deadline:
        time.sleep(3.0)
        try:
            if MT5.initialize(path=exe):
                ti = MT5.terminal_info()
                if ti and getattr(ti, "connected", False):
                    connected = True
                    break
        except Exception:
            pass

    if not connected:
        _log(
            f"[mt5_launch] no connection within {_CONNECT_TIMEOUT_S}s; "
            f"killing child"
        )
        _kill(proc)
        _record_failure()
        return False

    # --- guard 5: verify where it actually landed -------------------------
    if not _data_path_is_sane(appdata):
        _log("[mt5_launch] wrong data folder; killing child")
        try:
            MT5.shutdown()
        except Exception:
            pass
        _kill(proc)
        _record_failure()
        return False

    _record_success()   # clears any accumulated failures
    _log("[mt5_launch] MT5 launched, connected, data folder verified")
    return True
