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
import requests
import uuid
import logging
log = logging.getLogger("xtl.agent")
# at module top (once):
_last_sent_bar: dict[tuple[str, str], int] = {}  # (symbol, TF) -> last 't' sent
from xtl.mt5_client import mt5_init, mt5_fetch_rates, get_mt5_tick_price_and_ts, mt5_get_open_positions

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
    from xtl.mt5_client import mt5_init, mt5_fetch_rates, get_mt5_tick_price_and_ts, mt5_get_open_positions
except ImportError:
    # Running directly from source folder
    try:
        from .mt5_client import mt5_init, mt5_fetch_rates, get_mt5_tick_price_and_ts, mt5_get_open_positions
    except Exception:

        sys.path.append(os.path.dirname(__file__))
        from mt5_client import mt5_init, mt5_fetch_rates, mt5_get_open_positions

DEFAULT_TFS = ["M1","M15","H1","H4"]
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

TF_SEC = {"M1":60, "M5":300, "M15":900, "H1":3600,"H2": 7200,"H4":14400}

def push_mt5_positions_once(api_base: str, dev_id: str, token: str, mt5_account: str = "demo") -> bool:
    try:
        positions = mt5_get_open_positions()
        log.warning("MT5_POS_PUSH_START dev=%s acct=%s positions=%s", dev_id, mt5_account, len(positions or []))

        payload = {
            "device_id": dev_id,
            "mt5_account": mt5_account,
            "positions": positions,
            "ts_ms": int(time.time() * 1000),
        }

        r = api_post(
            api_base,
            f"/devices/{dev_id}/mt5/positions",
            payload,
            token=token,
            timeout=10,
        )

        return bool(getattr(r, "status_code", 0) == 200)
    except Exception as e:
        try:
            log.warning("push_mt5_positions_once failed: %s", e)
        except Exception:
            pass
        return False
        
def push_mt5_account_once(api_base: str, dev_id: str, token: str, mt5_account: str = "demo") -> bool:
    try:
        account = _mt5_account_meta()
        if not account:
            log.warning("MT5_ACCOUNT_PUSH_SKIP empty account meta dev=%s acct=%s", dev_id, mt5_account)
            return False

        payload = {
            "device_id": dev_id,
            "mt5_account": mt5_account,
            "account": account,
            "ts_ms": int(time.time() * 1000),
        }

        r = api_post(
            api_base,
            f"/devices/{dev_id}/mt5/account",
            payload,
            token=token,
            timeout=10,
        )

        log.warning(
            "MT5_ACCOUNT_PUSH dev=%s acct=%s balance=%s equity=%s margin=%s free=%s pnl=%s code=%s",
            dev_id,
            mt5_account,
            account.get("balance"),
            account.get("equity"),
            account.get("margin"),
            account.get("free_margin"),
            account.get("floating_pnl"),
            getattr(r, "status_code", 0),
        )

        return bool(getattr(r, "status_code", 0) == 200)
    except Exception as e:
        try:
            log.warning("push_mt5_account_once failed: %s", e)
        except Exception:
            pass
        return False

def _mt5_account_meta() -> dict:
        """
        MT5 account identity so backend can validate demo/live before trading.

        Uses MetaTrader5.account_info() when available.
        Never throws; returns {} on any failure.
        """
        try:
            import MetaTrader5 as mt5
        except Exception:
            return {}

        try:
           ai = mt5.account_info()
        except Exception:
           ai = None

        if not ai:
            return {}

        # ai is typically a namedtuple-like object. Use getattr safely.
        login = getattr(ai, "login", None)
        server = getattr(ai, "server", None)
        company = getattr(ai, "company", None)
        currency = getattr(ai, "currency", None)
        leverage = getattr(ai, "leverage", None)
        balance = getattr(ai, "balance", None)
        equity = getattr(ai, "equity", None)
        trade_mode = getattr(ai, "trade_mode", None)  # numeric (when exposed)
        margin = getattr(ai, "margin", None)
        free_margin = getattr(ai, "margin_free", None)
        margin_level = getattr(ai, "margin_level", None)
        profit = getattr(ai, "profit", None)
        credit = getattr(ai, "credit", None)

        # --- robust demo/live detection ---
        is_demo = None
        account_type = None

        # 1) Prefer MT5 constants when present
        try:
            tm = int(trade_mode) if trade_mode is not None else None
        except Exception:
            tm = None

        try:
            TM_DEMO = getattr(mt5, "ACCOUNT_TRADE_MODE_DEMO", None)
            TM_REAL = getattr(mt5, "ACCOUNT_TRADE_MODE_REAL", None)
            TM_CONTEST = getattr(mt5, "ACCOUNT_TRADE_MODE_CONTEST", None)

            if tm is not None and (TM_DEMO is not None or TM_REAL is not None or TM_CONTEST is not None):
                if TM_DEMO is not None and tm == int(TM_DEMO):
                    is_demo = True
                    account_type = "DEMO"
                elif TM_REAL is not None and tm == int(TM_REAL):
                    is_demo = False
                    account_type = "LIVE"
                elif TM_CONTEST is not None and tm == int(TM_CONTEST):
                    # contest behaves like demo for risk purposes (no live trading)
                    is_demo = True
                    account_type = "CONTEST"
        except Exception:
            pass

        # 2) Fallback heuristic (server text) only if still unknown
        if is_demo is None:
            try:
                s = str(server or "").lower()
                if s:
                     if "demo" in s:
                         is_demo = True
                         account_type = account_type or "DEMO"
                     elif "real" in s or "live" in s:
                         is_demo = False
                         account_type = account_type or "LIVE"
            except Exception:
                pass

        if account_type is None:
            account_type = "UNKNOWN"

        out = {
            "login": int(login) if login is not None else None,
            "server": str(server) if server is not None else None,
            "company": str(company) if company is not None else None,
            "currency": str(currency) if currency is not None else None,
            "leverage": int(leverage) if leverage is not None else None,

            # Prop firm source of truth
            "balance": float(balance) if balance is not None else None,
            "equity": float(equity) if equity is not None else None,
            "margin": float(margin) if margin is not None else None,
            "free_margin": float(free_margin) if free_margin is not None else None,
            "margin_level": float(margin_level) if margin_level is not None else None,
            "floating_pnl": float(profit) if profit is not None else None,
            "credit": float(credit) if credit is not None else None,

            "trade_mode": int(tm) if tm is not None else None,
            "is_demo": is_demo,
            "account_type": account_type,
        }

        # remove None values to keep payload small/clean
        return {k: v for k, v in out.items() if v is not None}


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
import requests
from requests.adapters import HTTPAdapter

_SESS: requests.Session | None = None
# ======================
# API CIRCUIT BREAKER (prevents crash during network timeouts)
# ======================
import threading as _th
_API_LOCK = _th.Lock()
_api_offline_until = 0.0
_api_fail_count = 0

def _api_allowed() -> bool:
    try:
        now = time.time()
        with _API_LOCK:
            return now >= _api_offline_until
    except Exception:
        return True  # fail-open

def _api_mark_ok() -> None:
    global _api_offline_until, _api_fail_count
    try:
        with _API_LOCK:
            _api_fail_count = 0
            _api_offline_until = 0.0
    except Exception:
        pass

def _api_mark_fail() -> None:
    global _api_offline_until, _api_fail_count
    try:
        with _API_LOCK:
            _api_fail_count = min(_api_fail_count + 1, 6)
            backoff = min(60, 5 * (2 ** (_api_fail_count - 1)))
            _api_offline_until = time.time() + backoff
    except Exception:
        pass


def _http_session() -> requests.Session:
    global _SESS
    if _SESS is None:
        s = requests.Session()
        # Increase pool to avoid urllib3 "Connection pool is full"
        adapter = HTTPAdapter(pool_connections=50, pool_maxsize=50, max_retries=0,pool_block=True)
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        _SESS = s
    return _SESS
# -------------------------------------------------------------------
# ?? DEPRECATED
# Price publishing is handled ONLY by agent_price.py
# This function must never be started.
# -------------------------------------------------------------------
def price_push_loop(api_base: str, dev_id: str, token: str, symbols: list[str], interval_sec: float = 2.0):
    raise RuntimeError("price_push_loop is deprecated. Use agent_price.py")
    import time

    while True:
        for sym in symbols:
            try:
                px, ts_ms = get_mt5_tick_price_and_ts(sym)
                if px is None or ts_ms is None:
                    continue

                payload = {"symbol": sym, "price": float(px), "ts_ms": int(ts_ms)}

                # uses your existing api_post() with Authorization Bearer token
                api_post(api_base, f"/devices/{dev_id}/price", payload, token=token, timeout=5)

            except Exception:
                try:
                    log.exception("price_push failed sym=%s", sym)
                except Exception:
                    pass

        time.sleep(interval_sec)


def api_post(api_base: str, path: str, payload: dict, token: str, timeout: int = 20):
    import requests
    url = api_base.rstrip("/") + "/" + path.lstrip("/")
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    # If API is currently marked offline (recent timeouts), skip sending for now.
    if not _api_allowed():
        class _R:
            status_code = 0
            ok = False
            text = "skipped_offline"
        return _R()

    ca = _find_bundled_ca()
    verify: object = ca if ca else True  # True = system trust as last fallback

    r = None
    try:
        s = _http_session()
        r = s.post(url, headers=headers, json=payload, timeout=timeout, verify=verify)
        # Mark API health based on response
        try:
            code = int(getattr(r, "status_code", 0) or 0)
        except Exception:
            code = 0

        if 200 <= code < 300:
            _api_mark_ok()
        else:
             _api_mark_fail()

        tail = (token or "")[-6:]
        tag = "ACK" if "/mt5/ack" in url else ("NEXT" if "/mt5/next" in url else "POST")
        log.info("%s url=%s code=%s token_tail=%s bytes=%s",
                 tag, url, getattr(r, "status_code", "?"), tail,
                 len((getattr(r, "text","") or "").encode("utf-8")))

        if getattr(r, "status_code", 0) != 200:
            log.warning("%s FAIL url=%s\n%s", tag, url, (r.text or "")[:500])
        return r

    except Exception as e:
        _api_mark_fail()
        log.warning("OHLC POST EXC %s: %s", url, e)
        class _R: status_code = 0; ok = False; text = str(e)
        return _R()

    finally:
        # CRITICAL: release connection back to urllib3 pool
        try:
            if r is not None:
                r.close()
        except Exception:
            pass

def api_get(api_base: str, path: str, token: str, timeout: int = 15):
    url = api_base.rstrip("/") + "/" + path.lstrip("/")
    headers = {"Authorization": f"Bearer {token}"}
    ca = _find_bundled_ca()
    verify = ca if ca else True

    r = None
    try:
        s = _http_session()
        r = s.get(url, headers=headers, timeout=timeout, verify=verify)
        return r
    except Exception as e:
        class _R:
            status_code = 0
            ok = False
            text = str(e)
        return _R()
    finally:
        # CRITICAL: release connection back to urllib3 pool
        try:
            if r is not None:
                r.close()
        except Exception:
            pass


# Track last pushed bar per (symbol, tf) to avoid duplicate uploads


# ----------------------- small utilities -----------------------
def _convert_utc_to_broker_ms(utc_ms, offset_min):
    if not utc_ms:
        return 0
    from datetime import datetime, timezone, timedelta
    dt_utc = datetime.fromtimestamp(utc_ms / 1000, tz=timezone.utc)
    dt_broker = dt_utc.astimezone(timezone(timedelta(minutes=offset_min)))
    return int(dt_broker.timestamp() * 1000)


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
        "Timeframes":    "M1,M5,M15,H1,H2,H4",
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

def reset_registry_tf_symbols(include_latest="0", symbols="XAUUSD,EURUSD,USDJPY,GBPUSD,USDCAD,USDCHF", timeframes="M1,M15,H1,H4"):
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
        reset_registry_tf_symbols(include_latest="0", timeframes="M1,M15,H1,H4")
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
        tfs_raw = "M1,M15,H1,H2,H4"

    syms = [s.strip().upper() for s in syms_raw.split(",") if s.strip()]
    tf_set = {"M1", "M15", "H1", "H4"}  # allow future toggles
    tfs = [t.upper().strip() for t in tfs_raw.split(",") if t.upper().strip() in tf_set]
    if not tfs:
        tfs = ["M1", "M15", "H1", "H4"]  # safe fallback, preserves your M1 default

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
        """
        Normalize epoch-like value to milliseconds.

        - MT5 'time' is in seconds since epoch (˜1e9) -> we multiply by 1000
        - If something is already large (>=1e12), treat as milliseconds
        """
        try:
            t = int(t or 0)
            if t <= 0:
               return 0

            # If already very large, assume it's in milliseconds (or finer) and keep it.
            # 1e12 ms ˜ year 2001, so any normal ms timestamp will be >= this.
            if t >= 1_000_000_000_000:
               return t  # already ms (or bigger; we don't expect µs/ns here)

            # Otherwise treat as seconds
            return t * 1000
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
        "H2": 2 * 60 * 60 * 1000,
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
    #slot_ms = ((now_ms + off_ms) // tf_ms) * tf_ms - off_ms  # open of *current* bar in broker time
    slot_ms = (now_ms // tf_ms) * tf_ms
    # --- build extra features for backend reasoning (RVOL, USD basket, probs) ---
    extras = {}
    raw_last = (bars or [])[-1] if (bars or []) else None

    
    def _safe_float(x):
        try:
            return float(x)
        except Exception:
            return None

    if isinstance(raw_last, dict):
        # 1) RVOL (15m) if present on raw bar
        rv = raw_last.get("rvol15") or raw_last.get("feat_rvol15")
        rv_val = _safe_float(rv)
        if rv_val is not None:
            extras["feat_rvol15"] = rv_val

        # 2) USD basket / macro tilt if present
        ub = raw_last.get("usd_basket") or raw_last.get("feat_usd_basket")
        ub_val = _safe_float(ub)
        if ub_val is not None:
            extras["feat_usd_basket"] = ub_val

        # 3) Probability fields, if the agent ever attaches them
        pu = _safe_float(raw_last.get("prob_up"))
        if pu is not None:
            extras["prob_up"] = pu

        pu1 = _safe_float(raw_last.get("prob_up_1h"))
        if pu1 is not None:
            extras["prob_up_1h"] = pu1


    arr_closed = []
    latest_bar = None  # optional live forming bar (kept separate from history)

    n = len(bars or [])
    for i, b in enumerate(bars or []):
        # Normalize inputs
        t_utc_ms = _to_ms(b.get("t"))
        #t_open_ms = _convert_utc_to_broker_ms(t_utc_ms, off_min)
        t_open_ms = t_utc_ms  # MT5 rates.time is epoch (UTC sec)

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


    acct = {}
    try:
        acct = _mt5_account_meta() or {}
    except Exception:
        acct = {}

    # Compute serverNow as max of system clock and last bar close time
    # This prevents gate from treating recent closed bars as "future" candles
    _server_now = int(now_ms)
    try:
        if arr_closed:
            _lb = arr_closed[-1]
            _lb_open = int(
                _lb.get("t_open_ms") or _lb.get("t", 0) * 1000
                if _lb.get("t_open_ms") or _lb.get("t")
                else 0
            )
            _lb_close = int(_lb.get("t_close_ms") or (_lb_open + tf_ms) if _lb_open else 0)
            if _lb_close > _server_now:
                _server_now = _lb_close
    except Exception:
        pass

    payload = {
        "symbol": (symbol or "").upper(),
        "timeframe": tf_s,
        "bars": arr_closed,
        "count": len(arr_closed),
        "written_at": now_ms,
        "serverNow": _server_now,        # ? ADD: used by gate bar picker
        "lastClosedTs": _server_now,     # ? ADD: reference for gate diagnostics
        "device_id": str(device_id),
        "source": "broker",
        "broker": _broker_tz_meta(),
        "account": acct,
        "extra": extras or {},
    }
    # terminal info is optional; only add if present
    term = {}
    try:
       import MetaTrader5 as mt5
       ti = mt5.terminal_info()
       if ti:
           # Prefer full dict if available
           try:
               term = ti._asdict()
           except Exception:
               term = {}
           v = getattr(ti, "version", None)
           if v is None:
               v = getattr(ti, "build", None)
           p = getattr(ti, "path", None)
           if v is not None:
               term["mt5_version"] = v
           if p:
               term["terminal_path"] = p
    except Exception:
       pass

    if term:
        payload["terminal"] = term
    if latest_bar is not None:
        payload["latest_bar"] = latest_bar

    r = api_post(api_base, f"/devices/{device_id}/ohlc", payload, token, timeout=20)
    return bool(getattr(r, "ok", False))





def _assert_mt5_account(expected: str = "demo") -> tuple[bool, str, dict]:
    """
    expected: "demo" | "live"
    Returns (ok, err, meta)
    """
    try:
        import MetaTrader5 as mt5
    except Exception as e:
        return False, f"mt5_import:{e}", {}

    ai = None
    try:
        ai = mt5.account_info()
    except Exception:
        ai = None

    if not ai:
        return False, f"account_info_none:{mt5.last_error()}", {}

    tm = getattr(ai, "trade_mode", None)
    login = getattr(ai, "login", None)
    server = getattr(ai, "server", None)

    meta = {"login": login, "server": server, "trade_mode": tm}

    # MT5 trade_mode commonly: 0=DEMO, 1=CONTEST, 2=REAL
    try:
        tm_i = int(tm) if tm is not None else None
    except Exception:
        tm_i = None

    exp = (expected or "demo").strip().lower()
    if exp == "demo":
        if tm_i not in (0, 1):  # treat CONTEST like demo for safety
            return False, f"expected_demo_got_trade_mode:{tm_i}", meta
    elif exp == "live":
        if tm_i != 2:
            return False, f"expected_live_got_trade_mode:{tm_i}", meta

    return True, "", meta

def _run_with_timeout(fn, args=(), kwargs=None, timeout_sec=10):
    import threading
    out = {"done": False, "ret": None, "err": None}
    if kwargs is None: kwargs = {}
    def _t():
        try:
            out["ret"] = fn(*args, **kwargs)
        except Exception as e:
            out["err"] = e
        out["done"] = True
    th = threading.Thread(target=_t, daemon=True)
    th.start()
    th.join(timeout_sec)
    if not out["done"]:
        return {"ok": False, "error": f"timeout_after_{timeout_sec}s"}
    if out["err"] is not None:
        return {"ok": False, "error": f"exception:{type(out['err']).__name__}:{out['err']}"}
    return out["ret"]

import threading, traceback
def _mt5_send_market_order(cmd: dict) -> dict:
    """
    Execute MT5 market order.
    cmd keys: symbol, side, volume, sl, tp, comment
    """
    try:
        import MetaTrader5 as mt5
    except Exception as e:
        return {"ok": False, "error": f"mt5_import:{e}"}

    # -------------------- NEW: demo/live safety guard --------------------
    expected_acct = (cmd.get("mt5_account") or "demo").strip().lower()
    try:
        ai = mt5.account_info()
    except Exception:
        ai = None

    if not ai:
        return {"ok": False, "error": f"account_info_none:{mt5.last_error()}"}

    tm = getattr(ai, "trade_mode", None)   # 0=DEMO, 1=CONTEST, 2=REAL
    login = getattr(ai, "login", None)
    server = getattr(ai, "server", None)

    try:
        tm_i = int(tm) if tm is not None else None
    except Exception:
        tm_i = None

    # treat CONTEST (1) as non-live; still safe
    if expected_acct == "demo":
        if tm_i not in (0, 1):
            return {
                "ok": False,
                "error": f"acct_guard_expected_demo_got:{tm_i}",
                "meta": {"login": login, "server": server, "trade_mode": tm_i},
            }
    elif expected_acct == "live":
        if tm_i != 2:
            return {
                "ok": False,
                "error": f"acct_guard_expected_live_got:{tm_i}",
                "meta": {"login": login, "server": server, "trade_mode": tm_i},
            }

    # audit log (best-effort)
    try:
        import logging
        logging.getLogger("xtl.agent").info(
            "[MT5] acct ok | expected=%s | login=%s | server=%s | trade_mode=%s",
            expected_acct, login, server, tm_i
        )
    except Exception:
        pass
    # -------------------- END NEW BLOCK --------------------

    symbol = cmd.get("symbol")
    side = cmd.get("side")
    volume = float(cmd.get("volume") or 0)
    sl = cmd.get("sl")
    tp = cmd.get("tp")
    comment = cmd.get("comment") or "XTL"

    if not symbol or volume <= 0:
        return {"ok": False, "error": "invalid_symbol_or_volume"}

    if not mt5.symbol_select(symbol, True):
        return {"ok": False, "error": f"symbol_select_failed:{symbol}"}

    tick = mt5.symbol_info_tick(symbol)
    if not tick:
        return {"ok": False, "error": "no_tick"}

    if side == "BUY":
        order_type = mt5.ORDER_TYPE_BUY
        price = tick.ask
    else:
        order_type = mt5.ORDER_TYPE_SELL
        price = tick.bid

    request = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": symbol,
        "volume": volume,
        "type": order_type,
        "price": price,
        "sl": float(sl) if sl else 0.0,
        "tp": float(tp) if tp else 0.0,
        "deviation": 20,
        "magic": 20251227,
        "comment": comment,
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_FOK,
    }

    result = mt5.order_send(request)
    if not result:
        return {"ok": False, "error": "order_send_none"}

    if result.retcode != mt5.TRADE_RETCODE_DONE:
        return {
            "ok": False,
            "error": f"retcode:{result.retcode}",
            "comment": getattr(result, "comment", ""),
        }

    return {
        "ok": True,
        "ticket": result.order,
        "price": result.price,
        "volume": result.volume,
    }

def _mt5_close_position(cmd: dict) -> dict:
    """
    Close an open MT5 position by *ticket* (works for hedging AND netting).
    Command expected:
      { "type":"close_position", "ticket":123, "symbol":"EURUSD" (optional), "deviation":20 (optional) }
    """
    try:
        import MetaTrader5 as mt5
    except Exception as e:
        return {"ok": False, "error": f"mt5_import_failed:{e}"}

    try:
        ticket = int(cmd.get("ticket") or 0)
    except Exception:
        ticket = 0
    if ticket <= 0:
        return {"ok": False, "error": "missing_ticket"}

    deviation = int(cmd.get("deviation") or 20)

    pos = None
    try:
        ps = mt5.positions_get(ticket=ticket)
        if ps and len(ps) > 0:
            pos = ps[0]
    except Exception:
        pos = None

    if not pos:
        return {"ok": False, "error": "position_not_found", "ticket": ticket}

    symbol = str(getattr(pos, "symbol", "") or (cmd.get("symbol") or "")).upper()
    if not symbol:
        return {"ok": False, "error": "missing_symbol", "ticket": ticket}

    vol = float(getattr(pos, "volume", 0.0) or 0.0)
    if vol <= 0:
        return {"ok": False, "error": "bad_volume", "ticket": ticket, "symbol": symbol}

    # Determine close side (opposite of position type)
    # mt5.POSITION_TYPE_BUY / mt5.POSITION_TYPE_SELL
    ptype = int(getattr(pos, "type", -1))
    if ptype == mt5.POSITION_TYPE_BUY:
        order_type = mt5.ORDER_TYPE_SELL
    elif ptype == mt5.POSITION_TYPE_SELL:
        order_type = mt5.ORDER_TYPE_BUY
    else:
        return {"ok": False, "error": f"bad_position_type:{ptype}", "ticket": ticket, "symbol": symbol}

    tick = mt5.symbol_info_tick(symbol)
    if not tick:
        return {"ok": False, "error": "no_tick", "ticket": ticket, "symbol": symbol}

    price = float(tick.bid) if order_type == mt5.ORDER_TYPE_SELL else float(tick.ask)

    req = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": symbol,
        "volume": vol,
        "type": order_type,
        "position": ticket,     # <-- critical: closes this specific position ticket
        "price": price,
        "deviation": deviation,
        "comment": "XTL close_position",
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_FOK,
    }

    try:
        r = mt5.order_send(req)
    except Exception as e:
        return {"ok": False, "error": f"order_send_exc:{e}", "ticket": ticket, "symbol": symbol}

    if not r:
        return {"ok": False, "error": "order_send_none", "ticket": ticket, "symbol": symbol, "last_error": str(mt5.last_error())}

    # retcode 10009 / 10008 are common success codes depending on broker
    ret = int(getattr(r, "retcode", -1))
    ok = ret in (mt5.TRADE_RETCODE_DONE, mt5.TRADE_RETCODE_PLACED)

    return {
        "ok": bool(ok),
        "ticket": ticket,
        "symbol": symbol,
        "volume": vol,
        "close_type": "SELL" if order_type == mt5.ORDER_TYPE_SELL else "BUY",
        "price": price,
        "retcode": ret,
        "comment": str(getattr(r, "comment", "") or ""),
        "request_id": int(getattr(r, "request_id", 0) or 0),
    }

def start_ohlc_worker(api_base, device_id, token, symbols, tfs, bars=300, period_sec=10):
    log.info(f"OHLC: starting worker symbols={symbols} tfs={tfs} bars={bars} every {period_sec}s")
    th = threading.Thread(
        target=_ohlc_loop,
        args=(api_base, device_id, token, symbols, tfs, bars, period_sec),
        name="ohlc-worker",
        daemon=True
    )
    th.start()
    return th

def start_mt5_cmd_worker(api_base, device_id, token, poll_sec=2):
    log.info("MT5 CMD: starting worker")
    th = threading.Thread(
        target=_mt5_cmd_loop,
        args=(api_base, device_id, token, poll_sec),
        name="mt5-cmd-worker",
        daemon=True,
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

            try:
                push_mt5_positions_once(api_base, device_id, token, mt5_account="demo")
            except Exception as e:
                log.warning("MT5 positions push failed: %s", e)

            took = time.time() - tick_start
            log.info(f"OHLC: tick done in {took:.2f}s")
        except Exception as e:
            log.info(f"OHLC: tick exception: {e}\n{traceback.format_exc()}")

        next_run += period_sec
        time.sleep(max(0, next_run - time.time()))


def _mt5_cmd_loop(api_base, device_id, token, poll_sec):
    while True:
        try:
            r = api_get(api_base, f"/devices/{device_id}/mt5/next", token)
            if getattr(r, "status_code", 0) != 200:
                time.sleep(poll_sec)
                continue

            data = r.json() if hasattr(r, "json") else {}
            cmd = data.get("cmd")
            if not cmd:
                time.sleep(poll_sec)
                continue

            job_id = cmd.get("job_id")
            log.info("MT5 CMD: got cmd job=%s type=%s sym=%s side=%s vol=%s",
                     cmd.get("job_id"), cmd.get("type"), cmd.get("symbol"), cmd.get("side"), cmd.get("volume"))
            expected_acct = (cmd.get("mt5_account") or "demo")
            cmd_type = str(cmd.get("type") or "").strip().lower()

            if cmd_type == "market_order":
                result = _run_with_timeout(_mt5_send_market_order, args=(cmd,), timeout_sec=15)

            elif cmd_type == "close_position":
                result = _run_with_timeout(_mt5_close_position, args=(cmd,), timeout_sec=15)

            else:
                 result = {"ok": False, "error": f"unknown_cmd_type:{cmd_type}"}

            if not isinstance(result, dict):
                result = {"ok": False, "error": f"bad_result_type:{type(result).__name__}"}


            log.info("MT5 CMD: posting ack job=%s ok=%s err=%s",
                     job_id, bool(result.get("ok")), result.get("error"))
            # normalize result
            res = result if isinstance(result, dict) else {"ok": False, "error": "bad_result"}

            # ?? embed user_id INSIDE result (this is what backend stores)
            res["user_id"] = cmd.get("user_id")
            ack = {
                "job_id": job_id,
                "ok": bool(res.get("ok")),
                "mt5_account": expected_acct,                # NEW (echo back)
                "kind": cmd.get("kind"),                     # NEW (optional)
                "symbol": cmd.get("symbol"),                 # NEW (optional)
                "side": cmd.get("side"),                     # NEW (optional)
                "result": res,
                "error": res.get("error"),
                "meta": res.get("meta"),
            }

            resp = api_post(
                api_base,
                f"/devices/{device_id}/mt5/ack",
                ack,
                token,
                timeout=10,
            )

            try:
                code = getattr(resp, "status_code", 0)
                body = (getattr(resp, "text", "") or "")[:500]
                log.info("MT5 ACK POST code=%s body=%s", code, body)
            except Exception:
                pass

        except Exception as e:
            log.warning("MT5 CMD loop error: %s", e)

        time.sleep(poll_sec)
def _push_ohlc_once_safe(api_base, device_id, token, symbols, tfs, bars):
    """
    Worker path: fetch `bars` CLOSED candles for each (symbol, tf),
    optionally attach the current forming candle as latest_bar (registry: IncludeLatest),
    de-dup on LAST CLOSED bar only, and POST via push_rates_batch(...)
    .
    """
    import time

    # --- ensure defaults exist (no-op \if already present) ---
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

        # Timeframes: ignore registry list, rely on CLI or our fixed default.
        base_tf = [str(tf or "").upper().strip() for tf in (tfs or []) if (tf or "").strip()]
        # If nothing was passed via CLI, use fixed worker plan:
        #  - M1 / M15 for short-term
        #  - H1 / H2 / H4 for horizon
        if not base_tf:
            base_tf = ["M1", "M15", "H1", "H2", "H4"]

        tflist = base_tf



    except Exception as e:
        log.warning("worker: registry merge failed (%s); using CLI only", e)
        syms   = [s.strip().upper() for s in (symbols or []) if (s or "").strip()]
        base_tf = [str(tf or "").upper().strip() for tf in (tfs or []) if (tf or "").strip()]
        if not base_tf:
           base_tf = ["M1", "M15", "H1", "H2", "H4"]
        tflist = base_tf

    # fallbacks if everything is empty
    if not syms:
        syms = ["XAUUSD", "EURUSD", "USDJPY", "GBPUSD", "USDCAD", "USDCHF"]

    log.info("worker plan: symbols=%s tfs=%s include_latest=%s", syms, tflist, include_latest)

    # ensure dedupe map exists
    try:
        _ = _last_sent_bar  # noqa: F401
    except NameError:
        globals()["_last_sent_bar"] = {}

    def _to_sec(t_any):
        try:
            t = int(t_any or 0)
            return (t // 1000) if t >= 1_000_000_000_000 else t  # ms?s else already s
        except Exception:
            return 0

    for sym in syms:
        for tf in tflist:
            # --- fetch with guard (closed bars + optional forming tail) ---
            try:
                tf_bars = 1500 if tf.upper() == "H1" else int(bars or 300)
                rates = mt5_fetch_rates(sym, tf, count=tf_bars, include_latest=include_latest)
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
                    count=tf_count  # soft cap; push_rates_batch trims if needed
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


def push_ohlc_once(
        api_base: str,
        device_id: str,
        token: str,
        symbols: list[str] | None = None,
        tfs: list[str] | None = None,
        bars: int = 300,
        **kw,
) -> None:
    """
    Fetch OHLC for each symbol/tf,
    optionally attach the current forming candle as latest_bar (registry: IncludeLatest),
    de-dup on LAST CLOSED bar only, and POST via push_rates_batch(.).
    """
    import time

    # We ALWAYS want the full basket here, independent of registry / HB hint:
    #  - M1: live / dashboard / preview
    #  - M15: model update cadence
    #  - H1 / H2 / H4: horizon for prediction meter
    FIXED_TFS = ["M1", "M15", "H1", "H4"]

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

    # --- resolve symbols from CLI + registry; IGNORE any timeframe hints ---
    try:
        try:
            reg_syms, reg_tfs, _ = _agent_pull_cfg()
        except Exception:
            reg_syms = [s.strip().upper() for s in (reg_get("Symbols") or "").split(",") if s.strip()]

        cli_syms = [s.strip().upper() for s in (symbols or []) if (s or "").strip()]

        # union while preserving order
        syms = list(dict.fromkeys((cli_syms or []) + (reg_syms or [])))

        if not syms:
            syms = ["XAUUSD", "EURUSD", "USDJPY", "GBPUSD", "USDCAD", "USDCHF"]

    except Exception as e:
        log.warning("normalize inputs failed; using defaults (%s)", e)
        syms = ["XAUUSD", "EURUSD", "USDJPY", "GBPUSD", "USDCAD", "USDCHF"]

    # Timeframes: hard-wire our basket; do NOT depend on registry or CLI tfs
    tflist = FIXED_TFS[:]

    log.info(
        "OHLC plan: symbols=%s tfs=%s bars=%s include_latest=%s",
        syms,
        tflist,
        bars,
        include_latest,
    )

    # --- helper for dedupe key (seconds) ---
    def _to_sec(t_any):
        try:
            t = int(t_any or 0)
            return (t // 1000) if t >= 1_000_000_000_000 else t  # ms?s else already s
        except Exception:
            return 0

    total_pushed = 0
    for s in syms:
        for tfu in tflist:
            # fetch CLOSED bars (+ tail if include_latest=True)
            try:
                tf_count = 1500 if str(tfu).upper() == "H1" else int(bars or 300)
                arr_raw = mt5_fetch_rates(s, tfu, count=tf_count, include_latest=include_latest)
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
                log.info("OHLC: skip — no CLOSED bar for %s/%s", s, tfu)
                continue

            last_t = _to_sec(last_closed.get("t"))
            key = (s, tfu)
            prev = _last_sent_bar.get(key)
            if prev and prev >= last_t and not kw.get("force"):
                log.info("OHLC: skip — already sent last_closed=%s for %s/%s", prev, s, tfu)
                continue

            _last_sent_bar[key] = last_t

            # POST the batch
            try:
                push_rates_batch(
                    base,
                    device_id,
                    token,
                    s,
                    tfu,
                    arr_raw,
                    include_latest=include_latest,
                )
                total_pushed += 1
            except Exception as e:
                import traceback
                log.error("OHLC: POST crash %s/%s: %s\n%s", s, tfu, e, traceback.format_exc())
                continue

    log.info("OHLC: push_once done; total series posted=%s", total_pushed)
