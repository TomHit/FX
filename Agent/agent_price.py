import os
import time
import json
import threading
from typing import Dict, Optional

import MetaTrader5 as mt5
import requests

import logging
log = logging.getLogger("xtl.agent")
from .mt5_client import mt5_init

# ======================
# CONFIG
# ======================
PRICE_PUBLISH_INTERVAL_SEC = 1.0   # per-symbol throttle
HTTP_TIMEOUT_SEC = 8.0
_last_ts_ms: Dict[str, int] = {}
SYMBOLS = [
    "XAUUSD",
    "EURUSD",
    "USDJPY",
    "GBPUSD",
    "USDCAD",
    "USDCHF",
]

# Example: https://api.xautrendlab.com
API_BASE = "https://api.xautrendlab.com"

# API endpoint: POST /_api/devices/{device_id}/price
# Body: {"symbol":"XAUUSD","price":1234.5,"ts_ms":...}
PRICE_PUSH_PATH = "/_api/devices/{device_id}/price"

# ======================
# INTERNAL STATE
# ======================
_last_pub_ts: Dict[str, float] = {}
_last_price: Dict[str, float] = {}

_sess: Optional[requests.Session] = None

# light, rate-limited warnings
_last_warn_ts: Dict[str, float] = {}
# ======================
# API CIRCUIT BREAKER (prevents crash during network timeouts)
# ======================
_API_LOCK = threading.Lock()
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

            #  CRITICAL: reset poisoned HTTP session
            if _api_fail_count >= 3:
                _reset_http_session()
    except Exception:
        pass


def _warn_throttled(key: str, msg: str, every_sec: float = 60.0) -> None:
    """Print/log at most once per `every_sec` per key."""
    try:
        now = time.time()
        last = _last_warn_ts.get(key, 0.0)
        if (now - last) < every_sec:
            return
        _last_warn_ts[key] = now
        try:
            log.warning(msg)
        except Exception:
            print(msg)
    except Exception:
        pass


def _load_device_creds() -> tuple[str, str]:
    # mirror agent_ohlc registry/env approach
    dev_id = os.environ.get("DeviceId") or os.environ.get("DEVICE_ID") or ""
    dev_tok = os.environ.get("DeviceToken") or os.environ.get("DEVICE_TOKEN") or ""
    return (dev_id.strip(), dev_tok.strip())


def load_device_token() -> str:
    """Compatibility shim for older code."""
    _id, tok = _load_device_creds()
    return tok or ""


def load_device_id() -> str:
    """Compatibility shim for older code."""
    _id, _tok = _load_device_creds()
    return _id or ""


def _rq_session() -> requests.Session:
    global _sess
    if _sess is None:
        s = requests.Session()
        s.headers.update({"Content-Type": "application/json"})
        # IMPORTANT: enlarge pool to avoid urllib3 "pool is full"
        from requests.adapters import HTTPAdapter
        adapter = HTTPAdapter(pool_connections=50, pool_maxsize=50, max_retries=0, pool_block=True)
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        _sess = s
    return _sess


def _get_mid_price(tick) -> float | None:
    """Prefer mid price if bid/ask available. Fallback to last."""
    try:
        bid = getattr(tick, "bid", None)
        ask = getattr(tick, "ask", None)
        if bid and ask and bid > 0 and ask > 0:
            return (float(bid) + float(ask)) / 2.0

        last = getattr(tick, "last", None)
        if last and last > 0:
            return float(last)
    except Exception:
        pass
    return None


def _should_publish(symbol: str, price: float, ts_ms: int) -> bool:
    """Throttle per symbol and ignore synthetic bar ticks (non-advancing ts)."""
    now = time.time()

    last_px = _last_price.get(symbol)
    last_ts = _last_ts_ms.get(symbol, 0)
    last_pub = _last_pub_ts.get(symbol, 0.0)

    # Ignore synthetic/duplicate ticks: timestamp did not advance
    if ts_ms <= last_ts:
        return False

    # Throttle: publish at most once per interval
    if (now - last_pub) < PRICE_PUBLISH_INTERVAL_SEC:
        return False

    _last_pub_ts[symbol] = now
    _last_price[symbol] = price
    _last_ts_ms[symbol] = ts_ms
    return True

def _push_price_http(symbol: str, price: float, ts_ms: int | None) -> bool:
    """Push live price to API (API writes Redis + WS). Returns True on successful publish."""
    if not ts_ms or ts_ms <= 0:
        return False

    dev_id, dev_tok = _load_device_creds()
    if not dev_id or not dev_tok:
        _warn_throttled(
            "missing_creds",
            "[agent_price] missing DeviceId/DeviceToken (env). price publish skipped.",
            every_sec=60.0,
        )
        return False

    sym_u = (symbol or "").upper().strip()
    if not sym_u:
        return False

    # If API is currently marked offline (recent timeouts), skip sending for now.
    try:
        if not _api_allowed():
            return False
    except Exception:
        return False

    url = API_BASE.rstrip("/") + PRICE_PUSH_PATH.format(device_id=dev_id)
    payload = {"symbol": sym_u, "price": float(price), "ts_ms": int(ts_ms)}

    r = None
    ok = False
    try:
        s = _rq_session()
        headers = {
            "Authorization": f"Bearer {dev_tok}",
            "X-Device-Id": dev_id,
            "X-Device-Token": dev_tok,
            "Content-Type": "application/json",
        }
        # (connect_timeout, read_timeout)
        r = s.post(
            url,
            headers=headers,
            data=json.dumps(payload),
            timeout=(12.0, 12.0),
        )


        # Mark API health based on response
        try:
            code = int(getattr(r, "status_code", 0) or 0)
        except Exception:
            code = 0

        if 200 <= code < 300:
            _api_mark_ok()
            ok = True
        elif code in (401, 403):
            # auth issue – real failure
            _api_mark_fail()
        elif code >= 500:
            # server issue – treat as failure
            _api_mark_fail()
        else:
            # 3xx / 4xx like 404 / 409 / 429 → do NOT poison the circuit
            _api_mark_ok()

        if code >= 300:
            _warn_throttled(
                "http_non2xx",
                f"[agent_price] publish http={code} body={getattr(r,'text','')[:160]}",
                every_sec=15.0,
            )

    except Exception as e:
        _api_mark_fail()
        _warn_throttled(
            "http_exc",
            f"[agent_price] publish exception: {type(e).__name__}: {e}",
            every_sec=15.0,
        )
        ok = False

    finally:
        # CRITICAL: release the connection back to urllib3 pool
        try:
            if r is not None:
                r.close()
        except Exception:
            pass

    return ok


def _reset_http_session():
    global _sess
    try:
        if _sess is not None:
            _sess.close()
    except Exception:
        pass
    _sess = None


def _resolve_broker_symbol(base: str) -> str:
    """
    Resolve broker-specific symbol name for tick fetching.
    Keeps API payload symbol canonical (EURUSD, XAUUSD, etc).
    """
    base_u = (base or "").upper().strip()
    if not base_u:
        return base

    # Exact match first
    try:
        info = mt5.symbol_info(base_u)
        if info is not None:
            try:
                mt5.symbol_select(base_u, True)
            except Exception:
                pass
            return base_u
    except Exception:
        pass

    # Wildcard match (EURUSD*, XAUUSD*, etc)
    try:
        cands = mt5.symbols_get(f"{base_u}*") or []
        for s in cands:
            name = getattr(s, "name", "")
            if not name:
                continue
            try:
                mt5.symbol_select(name, True)
            except Exception:
                pass
            return name
    except Exception:
        pass

    return base_u


def price_loop() -> None:
    """Main live price loop. Safe to run in its own thread."""
    mt5_init()

    # resolve broker-specific symbol names once (EURUSDm / XAUUSD. etc)
    resolved: Dict[str, str] = {}
    for sym in SYMBOLS:
        try:
            rname = _resolve_broker_symbol(sym)
            resolved[sym] = rname
            try:
                mt5.symbol_select(rname, True)
            except Exception:
                pass
        except Exception:
            resolved[sym] = sym
            try:
                mt5.symbol_select(sym, True)
            except Exception:
                pass

    # --- NEW: periodic MT5 re-init guard (MT5 can degrade after hours) ---
    last_mt5_reinit = time.time()

    # --- NEW: watchdog for successful publish ---
    last_ok_ts = time.time()   # ⭐ REQUIRED

    while True:
        # --- if API is marked offline, don't hammer; sleep a bit and retry later ---
        try:
            if not _api_allowed():
                time.sleep(0.8)
                continue
        except Exception:
            time.sleep(0.5)
            continue

        # --- periodic MT5 re-init every ~10 minutes ---
        try:
            if (time.time() - last_mt5_reinit) > 600:
                try:
                    mt5_init()
                except Exception:
                    pass
                last_mt5_reinit = time.time()
        except Exception:
            pass

        for sym in SYMBOLS:
            try:
                tick_sym = resolved.get(sym, sym)
                tick = mt5.symbol_info_tick(tick_sym)
                if not tick:
                    # light debug (once per minute)
                    if int(time.time()) % 60 == 0:
                        _warn_throttled(
                            f"no_tick:{sym}",
                            f"[agent_price] no tick: {sym} resolved={tick_sym}",
                            every_sec=60.0,
                        )
                    continue

                price = _get_mid_price(tick)
                if price is None or price <= 0:
                    continue

                try:
                    ts_ms = int(getattr(tick, "time_msc", 0) or 0)
                    if ts_ms <= 0:
                        ts_ms = int(getattr(tick, "time", 0) * 1000)
                except Exception:
                    continue

                if _should_publish(sym, price, ts_ms):
                    if _push_price_http(sym, price, ts_ms):
                        last_ok_ts = time.time()   # ⭐ SUCCESS TRACKED

            except Exception:
                continue

        # --- HARD RECOVERY: no successful publish for 5 minutes ---
        try:
            if time.time() - last_ok_ts > 300:
                _warn_throttled(
                    "price_stall",
                    "[agent_price] no successful publish for 5m → force reset",
                    every_sec=300.0,
                )
                _reset_http_session()
                _api_mark_ok()
                last_ok_ts = time.time()
        except Exception:
            pass

        # --- sleep once per full cycle (prevents hot-loop + pool saturation) ---
        time.sleep(0.15)


def start_price_publisher(
        api_base: str,
        dev_id: str,
        dev_token: str,
        symbols: list[str],
        interval_sec: float = 0.25,
):
    """Call this from agent main()."""
    global API_BASE, SYMBOLS, PRICE_PUBLISH_INTERVAL_SEC
    API_BASE = api_base
    SYMBOLS = list(symbols or SYMBOLS)
    PRICE_PUBLISH_INTERVAL_SEC = float(max(0.1, min(interval_sec, 5.0)))

    # CRITICAL: ensure creds exist for _load_device_creds() in service mode
    try:
        if dev_id:
            os.environ["DeviceId"] = dev_id
            os.environ["DEVICE_ID"] = dev_id
        if dev_token:
            os.environ["DeviceToken"] = dev_token
            os.environ["DEVICE_TOKEN"] = dev_token
    except Exception:
        pass

    t = threading.Thread(target=price_loop, name="agent_price_loop", daemon=True)
    t.start()
    return t
