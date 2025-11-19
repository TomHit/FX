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
# at module top (once):
_last_sent_bar: dict[tuple[str, str], int] = {}  # (symbol, TF) -> last 't' sent


# Third-party (present in the agent bundle)
import MetaTrader5 as MT5
# --- MT5 imports (works in both packaged and source modes) ---
try:
    # Running as package (PyInstaller / pip-style)
    from xtl.mt5_client import mt5_init, mt5_fetch_rates
except ImportError:
    # Running directly from source folder
    try:
        from .mt5_client import mt5_init, mt5_fetch_rates  # type: ignore
    except Exception:
        import sys, os
        sys.path.append(os.path.dirname(__file__))
        from mt5_client import mt5_init, mt5_fetch_rates  # type: ignore


# Prefer real helpers from the installer/runtime; fall back to simple versions when importing standalone
try:
    from xtl_installer import reg_get, api_post  # type: ignore
except Exception:  # pragma: no cover
    import requests  # type: ignore
    def reg_get(name: str) -> Optional[str]:
        return os.environ.get(name) or ""

    def api_post(api_base: str, path: str, payload: dict, token: str, timeout: int = 20):
        url = api_base.rstrip('/') + path
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        return requests.post(url, headers=headers, data=json.dumps(payload), timeout=timeout)

log = logging.getLogger("xtl.agent")
log.info("agent_ohlc.py loaded from: %s", __file__)
try:
    import numpy as _np; import pandas as _pd
    log.info("numpy=%s pandas=%s", getattr(_np, "__version__", "?"), getattr(_pd, "__version__", "?"))
except Exception:
    pass


# Track last pushed bar per (symbol, tf) to avoid duplicate uploads
_last_sent_bar: dict[Tuple[str, str], int] = {}

# ----------------------- small utilities -----------------------

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

def push_rates_batch(api_base, device_id, token, symbol, tf, rates):
    payload = {
        "symbol": symbol,
        "timeframe": tf,
        "count": len(rates),
        "written_at": int(time.time() * 1000),
        "bars": rates,
    }
    r = api_post(api_base, f"/devices/{urllib.parse.quote(device_id)}/ohlc",
                 payload=payload, token=token, timeout=20)
    return getattr(r, "ok", False)

import threading, time, traceback

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
    # tolerate both param names
    if not tfs:
        tfs = []
    for sym in (symbols or []):
        for tf in (tfs or []):
            rates = mt5_fetch_rates(sym, tf, bars)
            # explicit checks (avoid NumPy truthiness)
            if rates is None or len(rates) == 0:
                log.info(f"ohlc batch {sym}/{tf} -> no data")
                continue
            # send in one batch (your existing uploader)
            try:
               # Ensure device is attached (auto-claim if unpaired)
                try:
                   from xtl_installer import ensure_device_attached  # safe import
                   ensure_device_attached(api_base)
                except Exception:
                   pass
                sent = push_rates_batch(api_base, device_id, token, sym, tf, rates)
                log.info(f"ohlc batch {sym}/{tf} -> count={len(rates)} sent={sent}")
            except Exception as e:
                log.info(f"uplink {sym}/{tf} ERROR: {e}")


def push_ohlc_once(
    api_base: str,
    device_id: str,
    token: str,
    symbols: List[str],
    tfs: List[str] | None = None,
    bars: int = 300,
    **kw
) -> None:
    """
    Poll MT5 for the requested symbols/TFs and push OHLC to the server.
    - Always returns CLOSED candles (handles holidays via historical fetch).
    - Skips duplicates (same last bar 't' as previously sent).
    - Robust to NumPy arrays and empty slices.
    """
    import os, time, urllib.parse

    # tolerate legacy kw param name (must happen AFTER docstring)
    if tfs is None:
        tfs = kw.get("tf_names") or []

    mt5_path = (reg_get("MT5.TerminalPath") or reg_get("MT5Path") or "").strip()
    if not mt5_path or not os.path.isfile(mt5_path):
        log.info("OHLC: MT5 path missing or invalid; skipping poll")
        return

    if not mt5_init():  # do not pass mt5_path; mt5_init() resolves internally
        log.info("OHLC: MT5 initialize failed; will retry later")
        return

    # Ensure terminal has an account session before fetching data
    try:
        ti = MT5.terminal_info()
        acc = MT5.account_info()
        if not ti or not getattr(ti, "connected", False) or not acc:
            log.warning("OHLC: MT5 terminal not connected (no account session). Will retry.")
            return
    except Exception:
        # If MT5.* raises, treat as not connected this tick
        log.warning("OHLC: could not verify MT5 connection state; skipping this tick")
        return

    total_pushed = 0

    # normalize inputs
    syms = [s.strip() for s in (symbols or []) if (s or "").strip()]
    tfset = {"M1","M5","M15","M30","H1","H4","D1","W1","MN1"}
    tflist = [tfu for tfu in ((tf or "").upper().strip() for tf in (tfs or [])) if tfu in tfset]

    for s in syms:
        for tfu in tflist:
            # fetch CLOSED bars (mt5_fetch_rates enforces closed with fallback)
            arr_raw = mt5_fetch_rates(s, tfu, bars)
            log.debug("rates-shape %s/%s -> %s", s, tfu, type(arr_raw).__name__)

            try:
                ok_norm, arr, err = _normalize_rates(arr_raw)
            except Exception as e:
                import traceback
                log.error("NORMALIZE CRASH %s/%s: %s\n%s", s, tfu, e, traceback.format_exc())
                continue

            if (not ok_norm) or (arr is None) or (len(arr) == 0):
                log.warning("ohlc batch err %s/%s: %s", s, tfu, err or "normalize failed")
                continue

            # de-dup by last closed timestamp
            last_t = int(arr[-1].get("t", 0))
            key = (s, tfu)
            if _last_sent_bar.get(key) == last_t:
                log.debug("ohlc: up-to-date %s/%s (last=%s)", s, tfu, last_t)
                continue

            payload = {
                "symbol": s,
                "timeframe": tfu,
                "count": len(arr),
                "written_at": int(time.time() * 1000),
                "bars": arr,  # normalized list[dict] (t,o,h,l,c,v)
            }

            try:
                r = api_post(
                    api_base,
                    f"/devices/{urllib.parse.quote(device_id)}/ohlc",
                    payload=payload,
                    token=token,
                    timeout=20,
                )
                ok = getattr(r, "ok", False)
                status = getattr(r, "status_code", 0)
                if ok:
                    _last_sent_bar[key] = last_t
                    total_pushed += len(arr)
                    log.info("ohlc: pushed %s bars for %s/%s (last=%s) status=%s",
                             len(arr), s, tfu, last_t, status)
                else:
                    body = (getattr(r, "text", "") or "")[:240].replace("\n", " ")
                    log.warning("ohlc: http %s for %s/%s -> %s", status, s, tfu, body)
            except Exception as e:
                log.warning("ohlc: post failed for %s/%s: %s", s, tfu, e)

    if total_pushed == 0:
        log.debug("ohlc: nothing new to push this cycle")
