# -*- coding: utf-8 -*-

# agent_ohlc.py
"""
XTL Agent — MT5 OHLC uplink (robust version)
- Normalizes MT5 rates to list[dict] to avoid NumPy/Pandas truthiness errors.
- Skips duplicate pushes using last bar 't' per (symbol, timeframe).
- Keeps logging quiet and informative.
"""
from __future__ import annotations

import os
import time
import json
import logging
import urllib.parse
from typing import List, Optional, Tuple
import sys
from pathlib import Path

# at module top (once):
_last_sent_bar: dict[tuple[str, str], int] = {}  # (symbol, TF) -> last 't' sent


# Force-pack critical modules under PyInstaller
try:
    import unicodedata, charset_normalizer, idna, urllib3  # noqa: F401
except Exception:
    pass

import atexit
try:
    import MetaTrader5 as MT5
    atexit.register(lambda: MT5.shutdown())
except Exception:
    pass

try:
    # Running as package (PyInstaller / pip-style)
    from xtl.mt5_client import mt5_init, mt5_fetch_rates
except ImportError:
    # Running directly from source folder
    try:
        from .mt5_client import mt5_init, mt5_fetch_rates  # type: ignore
    except Exception:

        sys.path.append(os.path.dirname(__file__))
        from mt5_client import mt5_init, mt5_fetch_rates  # type: ignore

DEFAULT_TFS = ["M1"]
# Self-contained registry getter (prefers registry, falls back to env)
def reg_get(name: str) -> Optional[str]:
    try:
        import winreg
        # 1) LocalSystem hive (service)
        with winreg.OpenKey(winreg.HKEY_USERS, r"S-1-5-18\Software\XTL") as k:
            try:
                v, _ = winreg.QueryValueEx(k, name)
                if v is not None and str(v).strip() != "":
                    return str(v)
            except FileNotFoundError:
                pass
    except Exception:
        pass
    try:
        import winreg
        # 2) HKLM (machine-wide defaults)
        with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, r"Software\XTL") as k:
            try:
                v, _ = winreg.QueryValueEx(k, name)
                if v is not None and str(v).strip() != "":
                    return str(v)
            except FileNotFoundError:
                pass
    except Exception:
        pass
    try:
        import winreg
        # 3) HKCU (interactive user; helpful when running manually)
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, r"Software\XTL") as k:
            try:
                v, _ = winreg.QueryValueEx(k, name)
                if v is not None and str(v).strip() != "":
                    return str(v)
            except FileNotFoundError:
                pass
    except Exception:
        pass
    # 4) ENV as last fallback (keeps existing override behavior)
    return os.environ.get(name) or ""


def _good_ca(p: str, min_bytes: int = 100_000) -> bool:
    try:
        if not (p and os.path.isfile(p) and os.path.getsize(p) >= min_bytes):
            return False
        with open(p, "r", encoding="utf-8", errors="ignore") as f:
            return "-----BEGIN CERTIFICATE-----" in f.read(256)
    except Exception:
        return False

def _find_bundled_ca() -> Optional[str]:
    # Prefer _internal\certifi\cacert.pem beside the running exe
    here = Path(sys.argv[0]).resolve().parent
    candidates = [
        here / "_internal" / "certifi" / "cacert.pem",
        Path(os.environ.get("REQUESTS_CA_BUNDLE", "")),
        Path(os.environ.get("SSL_CERT_FILE", "")),
        ]
    for p in candidates:
        try:
            if _good_ca(str(p)):
                return str(p)
        except Exception:
            pass
    # try certifi as last resort
    try:
        import certifi
        c = certifi.where()
        if _good_ca(c):
            return c
    except Exception:
        pass
    return None

TF_SEC = {"M1":60, "M5":300, "M15":900, "H1":3600, "H4":14400}

def aggregate_from_m1(m1_bars, tf_label, broker_offset_min=0, max_out=200):
    """
    m1_bars: [{t_open_ms,o,h,l,c}] CLOSED, ascending by t_open_ms
    returns closed TF bars with t_open_ms/t_close_ms aligned to broker offset.
    """
    tf_sec = TF_SEC[tf_label]
    off_ms = int(broker_offset_min) * 60_000
    if not m1_bars:
        return []

    last_close_ms = m1_bars[-1]["t_open_ms"] + 60_000
    last_bucket_close = ((last_close_ms + off_ms)//(tf_sec*1000))*(tf_sec*1000) - off_ms

    lookback_ms = max_out * tf_sec * 1000
    start_ms = last_bucket_close - lookback_ms
    m1 = [b for b in m1_bars if b["t_open_ms"] >= start_ms]
    if not m1:
        return []

    out = []
    # first bucket close after the first m1 bar (aligned)
    bucket_close = (((m1[0]["t_open_ms"] + off_ms)//(tf_sec*1000))*(tf_sec*1000) - off_ms) + tf_sec*1000
    i, n = 0, len(m1)

    while bucket_close <= last_bucket_close:
        bucket_open = bucket_close - tf_sec*1000
        seg = []
        while i < n and m1[i]["t_open_ms"] < bucket_close:
            if m1[i]["t_open_ms"] >= bucket_open:
                seg.append(m1[i])
            i += 1
        if seg:
            o = seg[0]["o"]; c = seg[-1]["c"]
            h = max(x["h"] for x in seg); l = min(x["l"] for x in seg)
            out.append({
                "t_open_ms": bucket_open,
                "t_close_ms": bucket_close,
                "o": o, "h": h, "l": l, "c": c
            })
        bucket_close += tf_sec*1000
    return out[-max_out:]

def api_post(api_base: str, path: str, payload: dict, token: str, timeout: int = 20):
    import requests
    url = api_base.rstrip("/") + "/" + path.lstrip("/")
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    ca = _find_bundled_ca()
    verify: object = ca if ca else True  # True = system trust as last fallback
    try:
        which = verify if isinstance(verify, str) else "system"
        log.info("api_post: verify=%s", which)
    except Exception:
        pass

    try:
        r = requests.post(url, headers=headers, json=payload, timeout=timeout, verify=verify)
        tail = (token or "")[-6:]
        log.info("OHLC POST url=%s code=%s token_tail=%s bytes=%s",
                 url, getattr(r, "status_code", "?"), tail,
                 len((getattr(r, "text","") or "").encode("utf-8")))
        if getattr(r, "status_code", 0) != 200:
            log.warning("OHLC POST FAIL %s\n%s", url, (r.text or "")[:500])
        return r
    except Exception as e:
        log.warning("OHLC POST EXC %s: %s", url, e)
        class _R: status_code = 0; ok = False; text = str(e)
        return _R()

log = logging.getLogger("xtl.agent")
log.info("agent_ohlc.py loaded from: %s", __file__)
try:
    import numpy as _np; import pandas as _pd
    log.info("numpy=%s pandas=%s", getattr(_np, "__version__", "?"), getattr(_pd, "__version__", "?"))
except Exception:
    pass




# Track last pushed bar per (symbol, tf) to avoid duplicate uploads


# ----------------------- small utilities -----------------------
def _broker_tz_meta() -> dict:
    """
    Broker TZ metadata for the server snapshot.
    Priority:
      1) Registry overrides: Broker.TzName / Broker.TzOffsetMin
      2) Fallback to local OS timezone (name + offset minutes)
    """
    try:
        # 1) Registry overrides (if installer or UI has set them)
        tn = (reg_get("Broker.TzName") or "").strip()
        toff = reg_get("Broker.TzOffsetMin")
        if toff is not None and str(toff).strip() != "":
            try:
                off_min = int(str(toff).strip())
            except Exception:
                off_min = None
        else:
            off_min = None

        # 2) Fallback to OS local TZ if overrides are missing/incomplete
        if (not tn) or (off_min is None):
            import time
            # name
            if not tn:
                try:
                    tn = (time.tzname[time.localtime().tm_isdst] or time.tzname[0] or "").strip()
                except Exception:
                    tn = ""
            # offset (minutes east of UTC)
            try:
                # Windows: time.timezone is seconds WEST of UTC (non-DST);
                # use altzone when DST is active, then flip sign to get minutes EAST of UTC.
                if time.daylight and time.localtime().tm_isdst:
                    offset_s = -time.altzone
                else:
                    offset_s = -time.timezone
                off_min = int(offset_s // 60)
            except Exception:
                if off_min is None:
                    off_min = 0

        return {"tz_name": tn or None, "tz_offset_min": off_min}
    except Exception:
        # Never block OHLC pushes because of TZ meta
        return {}
import winreg

def ensure_registry_defaults():
    """Create default Symbols/Timeframes/IncludeLatest in HKU\S-1-5-18\Software\XTL if absent."""
    path = r"S-1-5-18\Software\XTL"
    defaults = {
        "Symbols":       "XAUUSD,EURUSD,USDJPY,GBPUSD,USDCAD,USDCHF",
        "Timeframes":    "M1",
        "IncludeLatest": "0",
    }
    try:
        with winreg.CreateKeyEx(winreg.HKEY_USERS, path, 0, winreg.KEY_SET_VALUE | winreg.KEY_QUERY_VALUE) as k:
            for name, val in defaults.items():
                try:
                    winreg.QueryValueEx(k, name)  # exists?
                except FileNotFoundError:
                    winreg.SetValueEx(k, name, 0, winreg.REG_SZ, val)
    except Exception as e:
        log.debug("ensure_registry_defaults: %s", e)


# --- Install/Config versioning (bump on each installer build) ---
CONFIG_VERSION = os.environ.get("XTL_CONFIG_VERSION", "2025-11-10")  # installer can override

def _xtl_reg_path():
    return r"S-1-5-18\Software\XTL"  # LocalSystem hive (service)

def _reg_set_value(root, subkey, name, value):
    import winreg
    with winreg.CreateKeyEx(root, subkey, 0, winreg.KEY_SET_VALUE | winreg.KEY_QUERY_VALUE) as k:
        winreg.SetValueEx(k, name, 0, winreg.REG_SZ, str(value))

def _reg_get_value(root, subkey, name, default=None):
    import winreg
    try:
        with winreg.OpenKey(root, subkey, 0, winreg.KEY_QUERY_VALUE) as k:
            v, _ = winreg.QueryValueEx(k, name)
            return v
    except Exception:
        return default

def _reg_delete_value(root, subkey, name):
    import winreg
    try:
        with winreg.OpenKey(root, subkey, 0, winreg.KEY_SET_VALUE) as k:
            try:
                winreg.DeleteValue(k, name)
            except FileNotFoundError:
                pass
    except Exception:
        pass

def reset_registry_tf_symbols(include_latest="0", symbols="XAUUSD,EURUSD,USDJPY,GBPUSD,USDCAD,USDCHF", timeframes="M1"):
    """
    Hard reset the three user-tunable keys under HKU\S-1-5-18\Software\XTL.
    Called on 'new installation' or when XTL_RESET_REGISTRY=1.
    """
    import winreg
    subkey = _xtl_reg_path()
    # nuke specific values (do NOT delete the whole key to avoid permissions issues)
    _reg_delete_value(winreg.HKEY_USERS, subkey, "Symbols")
    _reg_delete_value(winreg.HKEY_USERS, subkey, "Timeframes")
    _reg_delete_value(winreg.HKEY_USERS, subkey, "IncludeLatest")
    # write fresh defaults
    _reg_set_value(winreg.HKEY_USERS, subkey, "Symbols", symbols)
    _reg_set_value(winreg.HKEY_USERS, subkey, "Timeframes", timeframes)
    _reg_set_value(winreg.HKEY_USERS, subkey, "IncludeLatest", include_latest)

def maybe_reset_registry_on_new_install():
    """
    If ConfigVersion != CONFIG_VERSION (or XTL_RESET_REGISTRY=1), reset keys and stamp new version.
    """
    import winreg
    subkey = _xtl_reg_path()
    cur_ver = _reg_get_value(winreg.HKEY_USERS, subkey, "ConfigVersion", "")
    force   = (os.environ.get("XTL_RESET_REGISTRY","").strip() in ("1","true","TRUE","yes","YES"))
    if force or str(cur_ver) != str(CONFIG_VERSION):
        # reset to our intended defaults (M1-only + closed bars by default)
        reset_registry_tf_symbols(include_latest="0", timeframes="M1")
        _reg_set_value(winreg.HKEY_USERS, subkey, "ConfigVersion", CONFIG_VERSION)
        try:
            log.info("registry reset applied (ConfigVersion %s -> %s, force=%s)", cur_ver, CONFIG_VERSION, force)
        except Exception:
            pass


def _agent_pull_cfg():
    """
    Resolve symbols/timeframes and include_latest flag from registry (no JSON file).
    Keys (REG_SZ):
      - Symbols:       comma-separated, e.g. "XAUUSD,EURUSD,GBPUSD,USDJPY,USDCHF,USDCAD"
      - Timeframes:    comma-separated, e.g. "M1,M5,M10,M15,H1,H4"
      - IncludeLatest: "0" or "1" (append forming bar as complete=False)
    Fallback defaults are safe for your current XAU-only flow.
    """
    try:
        syms_raw = (reg_get("Symbols") or "").strip()
        tfs_raw  = (reg_get("Timeframes") or "").strip()
        inc_raw  = (reg_get("IncludeLatest") or "0").strip()
    except Exception:
        syms_raw, tfs_raw, inc_raw = "", "", "0"

    # Defaults if not present
    if not syms_raw:
        syms_raw = "XAUUSD,EURUSD,GBPUSD,USDJPY,USDCHF,USDCAD"  # 6 instruments
    if not tfs_raw:
        tfs_raw = "M1,M5,M10,M15,H1,H4"

    syms = [s.strip().upper() for s in syms_raw.split(",") if s.strip()]
    tf_set = {"M1","M15"}  # allow future toggles
    tfs = [t.upper().strip() for t in tfs_raw.split(",") if t.upper().strip() in tf_set]
    if not tfs:
        tfs = ["M1", "M15"]  # safe fallback, preserves your M1 default

    include_latest = inc_raw in ("1", "true", "TRUE", "yes", "YES")
    return syms, tfs, include_latest

def _join_api(api_base: str, path: str) -> str:
    """
    Normalize api_base (strip trailing slash and accidental '/api' suffix)
    and join with a leading-slash path.
    """
    base = (api_base or "").strip().rstrip("/")
    if base.lower().endswith("/api"):
        base = base[:-4]  # drop '/api'
    if not path.startswith("/"):
        path = "/" + path
    return f"{base}{path}"


def _is_empty(x) -> bool:
    if x is None:
        return True
    try:
        import numpy as _np  # lazy import
        if isinstance(x, _np.ndarray):
            return x.size == 0
    except Exception:
        pass
    try:
        return len(x) == 0
    except Exception:
        return False

def _normalize_rates(arr_raw):
    """
    Returns (ok: bool, list_of_dicts, err_msg|None)
    Each dict has keys: t,o,h,l,c,v (ints/floats). Guarantees closed bars only.
    """
    import numpy as np

    if arr_raw is None:
        return False, [], "no data"

    # If it’s a NumPy structured array, convert
    if isinstance(arr_raw, np.ndarray):
        if arr_raw.size == 0:
            return False, [], "empty"
        names = tuple(arr_raw.dtype.names or ())
        def _num(row, field, default=0.0):
            try:
                if field in names: return float(row[field])
            except Exception:
                pass
            return float(default)
        out = [{
            "t": int(r["time"]),
            "o": float(r["open"]),
            "h": float(r["high"]),
            "l": float(r["low"]),
            "c": float(r["close"]),
            "v": _num(r, "tick_volume", _num(r, "real_volume", 0.0)),
        } for r in arr_raw]
        return True, out, None

    # If it’s already a list of dicts with required fields
    if isinstance(arr_raw, (list, tuple)) and arr_raw and isinstance(arr_raw[0], dict):
        # validate minimal schema and coerce
        out = []
        for r in arr_raw:
            try:
                out.append({
                    "t": int(r.get("t")),
                    "o": float(r.get("o")),
                    "h": float(r.get("h")),
                    "l": float(r.get("l")),
                    "c": float(r.get("c")),
                    "v": float(r.get("v", 0.0)),
                })
            except Exception:
                return False, [], "schema error"
        return True, out, None

    # Anything else
    try:
        # attempt len() to distinguish empty containers
        if hasattr(arr_raw, "__len__") and len(arr_raw) == 0:
            return False, [], "empty"
    except Exception:
        pass
    return False, [], f"unsupported type: {type(arr_raw).__name__}"

import time

def push_rates_batch(api_base, device_id, token, symbol, tf, bars, include_latest=False, **kw):
    """
    Send ONLY closed candles to /devices/{device_id}/ohlc, and (optionally) attach
    the current forming slot as `latest_bar` (complete=False) for live UI/nowcast.

    - Accepts bars with 't' as UTC (sec/ms/us/ns).
    - Computes close boundary using tf -> ms.
    - Filters out any forming candle from the historical `bars` list.
    - Preserves OHLC exactly as provided.
    - If include_latest=True and the tail bar is forming (or marked complete=False),
      attach it to payload.latest_bar (not inserted into historical list).
    """

    import time

    def _to_ms(t):
        try:
            t = int(t or 0)
            if t >= 1_000_000_000_000_000:  # ns -> ms
                return t // 1_000_000
            if t >= 1_000_000_000_000_000 // 1000:          # # µs (≈1e15) -> ms
                return t // 1_000
            if t >= 1_000_000_000_000:          # ms (≈1e12) -> ms
                return t
            return t * 1000                      # sec (≈1e9) -> ms
        except Exception:
            return 0

    # --- timeframe -> ms (supports M1/M5/M10/M15/H1/H4; tolerant to lowercase) ---
    tf_s = (tf or "").upper()
    TF_MS = {
        "M1": 1 * 60 * 1000,
        "M5": 5 * 60 * 1000,
        "M10": 10 * 60 * 1000,
        "M15": 15 * 60 * 1000,
        "H1": 60 * 60 * 1000,
        "H4": 4 * 60 * 60 * 1000,
    }
    tf_ms = TF_MS.get(tf_s, 0)
    if not tf_ms:
        # unknown TF; don't post malformed data
        return False
    # --- anchor to broker TF grid rather than local clock ---
    # (prevents off-by-one-bar when OS TZ != broker TZ)
    bmeta = _broker_tz_meta() or {}
    try:
        off_min = int(bmeta.get("tz_offset_min") or 0)
    except Exception:
        off_min = 0
    off_ms = off_min * 60_000

    now_ms = int(time.time() * 1000)
    slot_ms = ((now_ms + off_ms) // tf_ms) * tf_ms - off_ms  # open of *current* bar in broker time


    arr_closed = []
    latest_bar = None  # optional live forming bar (kept separate from history)

    n = len(bars or [])
    for i, b in enumerate(bars or []):
        # Normalize inputs
        t_open_ms = _to_ms(b.get("t"))
        if not t_open_ms:
            continue  # skip malformed rows

        t_close_ms = t_open_ms + tf_ms

        # Decide if this bar is forming
        explicit_complete = b.get("complete")
        if explicit_complete is not None:
            # If MT5 (or the fetch wrapper) told us explicitly, trust it.
            is_forming = (explicit_complete is False)
        else:
             # Fallback: use broker-grid boundary only when 'complete' not provided
             is_forming = (t_close_ms > slot_ms)


        # If forming ,and it's the tail and include_latest=True, capture as latest_bar (NOT in history)
        if is_forming and include_latest and (i == n - 1):
            latest_bar = {
                "t": int(t_open_ms // 1000),        # seconds (server can also rely on t_open_ms)
                "t_open_ms": int(t_open_ms),
                "t_close_ms": int(t_close_ms),
                "o": float(b.get("o", 0)),
                "h": float(b.get("h", 0)),
                "l": float(b.get("l", 0)),
                "c": float(b.get("c", 0)),
                "v": int(b.get("v", 0)),
                "complete": False,
            }
            continue  # do not insert into historical list

        # Only closed bars go to history
        if is_forming:
            continue


        # Append a normalized closed bar
        arr_closed.append({
            # legacy field 't' kept for compatibility (open time in ms)
            "t": int(t_open_ms // 1000),
            # explicit fields used by server/UI
            "t_open_ms": int(t_open_ms),
            "t_close_ms": int(t_close_ms),
            # exact OHLC (no rounding beyond float())
            "o": float(b.get("o", 0)),
            "h": float(b.get("h", 0)),
            "l": float(b.get("l", 0)),
            "c": float(b.get("c", 0)),
            # optional volume
            "v": int(b.get("v", 0)),
            "complete": True,
        })

    # Optional tail limit if caller passed bars count
    max_count = int(kw.get("max_count") or kw.get("count") or 0)
    if max_count and len(arr_closed) > max_count:
        arr_closed = arr_closed[-max_count:]

    payload = {
        "symbol": (symbol or "").upper(),
        "timeframe": tf_s,                 # "M1" | "M5" | "M10" | "M15" | "H1" | "H4"
        "bars": arr_closed,                # CLOSED-only, normalized list
        "count": len(arr_closed),
        "written_at": now_ms,              # REQUIRED
        "source": "broker",                # optional but useful
        "broker": _broker_tz_meta(),
    }
    if latest_bar is not None:
        payload["latest_bar"] = latest_bar

    r = api_post(api_base, f"/devices/{device_id}/ohlc", payload, token, timeout=20)
    return bool(getattr(r, "ok", False))

import threading, traceback

def start_ohlc_worker(api_base, device_id, token, symbols, tfs, bars=300, period_sec=60):
    log.info(f"OHLC: starting worker symbols={symbols} tfs={tfs} bars={bars} every {period_sec}s")
    th = threading.Thread(
        target=_ohlc_loop,
        args=(api_base, device_id, token, symbols, tfs, bars, period_sec),
        name="ohlc-worker",
        daemon=True
    )
    th.start()
    return th

def _ohlc_loop(api_base, device_id, token, symbols, tfs, bars, period_sec):
    next_run = time.time()
    while True:
        try:
            tick_start = time.time()
            log.info("OHLC: tick begin")
            _push_ohlc_once_safe(api_base, device_id, token, symbols, tfs, bars)
            took = time.time() - tick_start
            log.info(f"OHLC: tick done in {took:.2f}s")
        except Exception as e:
            log.info(f"OHLC: tick exception: {e}\n{traceback.format_exc()}")
        # drift-free scheduling
        next_run += period_sec
        time.sleep(max(0, next_run - time.time()))

def _push_ohlc_once_safe(api_base, device_id, token, symbols, tfs, bars):
    """
    Worker path: fetch `bars` CLOSED candles for each (symbol, tf),
    optionally attach the current forming candle as latest_bar (registry: IncludeLatest),
    de-dup on LAST CLOSED bar only, and POST via push_rates_batch(...).
    """
    import time

    # --- ensure defaults exist (no-op if already present) ---
    try:
        ensure_registry_defaults()
    except Exception:
        pass
    # --- new-install reset (based on ConfigVersion or env toggle) ---
    try:
        maybe_reset_registry_on_new_install()
    except Exception:
        pass


    # --- include_latest from registry (service path has no CLI kw) ---
    reg_inc = (reg_get("IncludeLatest") or "0").strip() in ("1", "true", "TRUE", "yes", "YES")
    include_latest = bool(reg_inc)

    # --- normalize API base ONCE and reuse ---
    base = (api_base or "").strip().rstrip("/")
    if base.lower().endswith("/api"):
        base = base[:-4]

    # --- merge CLI + Registry; allow empty CLI to fully defer to registry ---
    try:
        try:
            reg_syms, reg_tfs, _ = _agent_pull_cfg()
        except Exception:
            reg_syms = [s.strip().upper() for s in (reg_get("Symbols") or "").split(",") if s.strip()]
            reg_tfs  = [t.strip().upper() for t in (reg_get("Timeframes") or "").split(",") if t.strip()]

        cli_syms = [s.strip().upper() for s in (symbols or []) if (s or "").strip()]
        cli_tfs  = [str(tf or "").upper().strip() for tf in (tfs or []) if (tf or "").strip()]

        # union while preserving order
        syms   = list(dict.fromkeys((cli_syms or []) + (reg_syms or [])))
        tflist = list(dict.fromkeys((cli_tfs  or []) + (reg_tfs  or [])))
        # Force M1 only for worker plan
        tflist = ["M1"]

    except Exception as e:
        log.warning("worker: registry merge failed (%s); using CLI only", e)
        syms   = [s.strip().upper() for s in (symbols or []) if (s or "").strip()]
        tflist = [str(tf or "").upper().strip() for tf in (tfs or []) if (tf or "").strip()]

    # fallbacks if everything is empty
    if not syms:
        syms = ["XAUUSD", "EURUSD", "USDJPY", "GBPUSD", "USDCAD", "USDCHF"]
    if not tflist:
        tflist = ["M1"]

    log.info("worker plan: symbols=%s tfs=%s include_latest=%s", syms, tflist, include_latest)

    # ensure dedupe map exists
    try:
        _ = _last_sent_bar  # noqa: F401
    except NameError:
        globals()["_last_sent_bar"] = {}

    def _to_sec(t_any):
        try:
            t = int(t_any or 0)
            return (t // 1000) if t >= 1_000_000_000_000 else t  # ms→s else already s
        except Exception:
            return 0

    for sym in syms:
        for tf in tflist:
            # --- fetch with guard (closed bars + optional forming tail) ---
            try:
                rates = mt5_fetch_rates(sym, tf, count=int(bars or 300), include_latest=include_latest)
                n_raw = (len(rates) if hasattr(rates, "__len__") else 0)
                log.info("worker/fetch %s/%s -> %s rows", sym, tf, n_raw)
            except Exception as e:
                import traceback
                log.info("worker/fetch EXC %s/%s: %s\n%s", sym, tf, e, traceback.format_exc())
                continue

            if not rates:
                log.info("worker: skip — empty fetch for %s/%s", sym, tf)
                continue

            # --- de-dup by LAST CLOSED bar (ignore a trailing forming bar) ---
            last_closed = next((b for b in reversed(rates) if b.get("complete", True)), None)
            if not last_closed:
                log.info("worker: skip — no CLOSED bars for %s/%s (all forming?)", sym, tf)
                continue

            last_t_s = _to_sec(last_closed.get("t"))
            key = (sym, tf)
            if _last_sent_bar.get(key) == last_t_s:
                log.debug("worker: up-to-date %s/%s (last_closed=%s)", sym, tf, last_t_s)
                continue

            # --- unified post (closed -> bars[], forming -> latest_bar) ---
            try:
                sent = push_rates_batch(
                    base, device_id, token,
                    sym, tf, rates,
                    include_latest=include_latest,
                    count=bars  # soft cap; push_rates_batch trims if needed
                )
                if sent:
                    _last_sent_bar[key] = last_t_s
                    pushed_closed = sum(1 for b in rates if b.get("complete", True))
                    log.info(
                        "worker: pushed %s CLOSED bars for %s/%s (last_closed=%s)",
                        pushed_closed, sym, tf, last_t_s
                    )
                else:
                    log.warning("worker: POST failed for %s/%s (push_rates_batch=False)", sym, tf)
            except Exception as e:
                log.warning("worker: post failed for %s/%s: %s", sym, tf, e)
    # --- EXTRA: M15 history sync on NEW close (M1 path unchanged) ------------
    try:
        TFU = "M15"

        for sym in syms:
            # Fetch CLOSED 15m bars (native from MT5)
            try:
                rates = mt5_fetch_rates(sym, TFU, count=300, include_latest=False)
                n_raw = (len(rates) if hasattr(rates, "__len__") else 0)
                log.info("worker/fetch %s/%s -> %s rows", sym, TFU, n_raw)
            except Exception as e:
                import traceback
                log.info("worker/fetch EXC %s/%s: %s\n%s", sym, TFU, e, traceback.format_exc())
                continue

            if not rates:
                log.info("worker: skip — empty fetch for %s/%s", sym, TFU)
                continue

            # --- de-dup by LAST CLOSED bar (ignore a trailing forming bar) ---
            last_closed = next((b for b in reversed(rates) if b.get("complete", True)), None)
            if not last_closed:
                log.info("worker: skip — no CLOSED bars for %s/%s (all forming?)", sym, TFU)
                continue

            last_t_s = int(last_closed.get("t") or 0)  # bar OPEN (epoch-sec) of last CLOSED M15
            key = (sym.upper(), TFU)  # force uppercase for consistency
            if _last_sent_bar.get(key) == last_t_s:
                # Already posted this CLOSED 15m bar; skip until next :00/:15/:30/:45
                log.debug("worker: up-to-date %s/%s (last_closed=%s)", sym, TFU, last_t_s)
                continue

            # --- post CLOSED bars only ---
            try:
                sent = push_rates_batch(
                    base, device_id, token,
                    sym, TFU, rates,
                    include_latest=False,
                    count=300  # server trims if needed
                )
                if sent:
                    _last_sent_bar[key] = last_t_s
                    pushed_closed = sum(1 for b in rates if b.get("complete", True))
                    log.info(
                        "worker: pushed %s CLOSED bars for %s/%s (last_closed=%s)",
                        pushed_closed, sym, TFU, last_t_s
                    )
                else:
                    log.warning("worker: POST failed for %s/%s (push_rates_batch=False)", sym, TFU)
            except Exception as e:
                log.warning("worker: post failed for %s/%s: %s", sym, TFU, e)
    except Exception as e:
        log.warning("OHLC 15m path failed: %s", e)



def push_ohlc_once(api_base, device_id, token, symbols=None, tfs=None, bars=300, **kw):
    """
    One-shot push:
      - Merge CLI lists with registry (HKU\S-1-5-18\Software\XTL: Symbols, Timeframes, IncludeLatest)
      - Fetch CLOSED bars (+ optional forming tail) for each (symbol, tf)
      - De-dup on LAST CLOSED bar only
      - POST via push_rates_batch(...)  -> bars[] (closed), latest_bar (forming)
    """
    import time

    # --- optional: seed defaults if missing (safe no-op if you don't have it) ---
    try:
        ensure_registry_defaults()  # if defined elsewhere; otherwise harmless
    except Exception:
        pass
    # --- new-install reset (based on ConfigVersion or env toggle) ---
    try:
        maybe_reset_registry_on_new_install()
    except Exception:
        pass


    # --- include_latest flag (CLI kw overrides registry) ---
    reg_inc = (reg_get("IncludeLatest") or "0").strip() in ("1","true","TRUE","yes","YES")
    include_latest = bool(kw.get("include_latest", reg_inc))

    # --- normalize API base ONCE and reuse ---
    base = (api_base or "").strip().rstrip("/")
    if base.lower().endswith("/api"):
        base = base[:-4]

    # --- merge CLI + Registry for symbols/timeframes (order-preserving union) ---
    try:
        # Prefer a central helper if present
        try:
            reg_syms, reg_tfs, _ = _agent_pull_cfg()
        except Exception:
            reg_syms = [s.strip().upper() for s in (reg_get("Symbols") or "").split(",") if s.strip()]
            reg_tfs  = [t.strip().upper() for t in (reg_get("Timeframes") or "").split(",") if t.strip()]

        cli_syms = [s.strip().upper() for s in (symbols or []) if (s or "").strip()]
        cli_tfs  = [str(tf or "").upper().strip() for tf in (tfs or []) if (tf or "").strip()]

        syms   = list(dict.fromkeys((cli_syms or []) + (reg_syms or [])))
        allowed = {"M1","M15"}
        tflist = [t for t in dict.fromkeys((cli_tfs or []) + (reg_tfs or [])) if t in allowed]
        if not tflist:
            tflist = ["M1", "M15"]  # keep your M1-only fallback if nothing valid provided

        if not syms:
            syms = ["XAUUSD","EURUSD","USDJPY","GBPUSD","USDCAD","USDCHF"]

    except Exception as e:
        log.warning("normalize inputs failed; using defaults (%s)", e)
        syms   = ["XAUUSD","EURUSD","USDJPY","GBPUSD","USDCAD","USDCHF"]
        tflist = ["M1"]

    log.info("OHLC plan: symbols=%s tfs=%s bars=%s include_latest=%s", syms, tflist, bars, include_latest)

    # --- helper for dedupe key (seconds) ---
    def _to_sec(t_any):
        try:
            t = int(t_any or 0)
            return (t // 1000) if t >= 1_000_000_000_000 else t  # ms→s else already s
        except Exception:
            return 0

    total_pushed = 0
    for s in syms:
        for tfu in tflist:
            # fetch CLOSED bars (+ tail if include_latest=True)
            try:
                arr_raw = mt5_fetch_rates(s, tfu, count=int(bars or 300), include_latest=include_latest)
                n_raw = (len(arr_raw) if hasattr(arr_raw, "__len__") else 0)
                log.info("OHLC fetch: %s/%s -> %s rows", s, tfu, n_raw)
            except Exception as e:
                import traceback
                log.error("OHLC: fetch crash %s/%s: %s\n%s", s, tfu, e, traceback.format_exc())
                continue

            if not arr_raw:
                log.info("OHLC: skip — empty fetch for %s/%s", s, tfu)
                continue

            # de-dup by LAST CLOSED bar (ignore any trailing forming bar)
            last_closed = next((b for b in reversed(arr_raw) if b.get("complete", True)), None)
            if not last_closed:
                log.info("OHLC: skip — no CLOSED bars for %s/%s (all forming?)", s, tfu)
                continue

            last_t_s = _to_sec(last_closed.get("t"))
            key = (s, tfu)
            try:
                _force = bool(kw.get("force", False))
                if not _force and _last_sent_bar.get(key) == last_t_s:
                    log.debug("ohlc: up-to-date %s/%s (last_closed=%s)", s, tfu, last_t_s)
                    continue
                elif _force:
                    log.debug("ohlc: FORCE bypass dedup for %s/%s (last_closed=%s)", s, tfu, last_t_s)
            except NameError:
                # if _last_sent_bar doesn't exist, create it
                globals()["_last_sent_bar"] = {}
                _force = bool(kw.get("force", False))

            # unified post (closed -> bars[], forming -> latest_bar)
            try:
                sent = push_rates_batch(
                    base, device_id, token,
                    s, tfu, arr_raw,
                    include_latest=include_latest,
                    count=bars  # soft cap; push_rates_batch trims if needed
                )
                if sent:
                    _last_sent_bar[key] = last_t_s
                    pushed_closed = sum(1 for b in arr_raw if b.get("complete", True))
                    total_pushed += pushed_closed
                    log.info("ohlc: pushed %s CLOSED bars for %s/%s (last_closed=%s)",
                             pushed_closed, s, tfu, last_t_s)
                else:
                    log.warning("ohlc: POST failed for %s/%s (push_rates_batch=False)", s, tfu)
            except Exception as e:
                log.warning("ohlc: post failed for %s/%s: %s", s, tfu, e)

    if total_pushed == 0:
        log.debug("ohlc: nothing new to push this cycle")