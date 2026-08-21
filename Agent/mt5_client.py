# xtl/mt5_client.py — robust MT5 init + closed-bar fetch (safe update)
from __future__ import annotations
import os, time, subprocess
import threading
from functools import wraps

from pathlib import Path
from typing import List, Dict, Optional


import MetaTrader5 as MT5

TRIED_LOG: list[str] = []
# cache: once MT5 is initialized and connected, don't re-init every cycle
_MT5_READY = False


# One MT5 terminal/IPC connection is shared by several Agent threads.
# Serialize only broker API access; HTTP, Redis and analytics remain parallel.
MT5_API_LOCK = threading.RLock()


def mt5_locked(fn):
    """
    Serialize a complete critical MT5 transaction.

    RLock is required because a protected function may call another protected
    function, for example mt5_get_open_positions() -> mt5_init().
    """
    @wraps(fn)
    def wrapper(*args, **kwargs):
        with MT5_API_LOCK:
            return fn(*args, **kwargs)

    return wrapper

# Attach-only guard: detect if MT5 is already running.

def _mt5_running() -> bool:
    try:
        import os

        out = (
            os.popen('tasklist /FI "IMAGENAME eq terminal64.exe" /FO CSV /NH')
            .read()
            .lower()
        )
        if "terminal64.exe" in out and "no tasks" not in out:
            return True
        out2 = (
            os.popen('tasklist /FI "IMAGENAME eq terminal.exe" /FO CSV /NH')
            .read()
            .lower()
        )
        return ("terminal.exe" in out2) and ("no tasks" not in out2)
    except Exception:
        return False


# Returns full path of the running MT5 terminal (terminal64.exe/terminal.exe), or None.
def _find_running_mt5_exe() -> str | None:
    import os

    try:
        ps = r'powershell -NoProfile -Command "(Get-Process terminal64 -ErrorAction SilentlyContinue | Select-Object -First 1).Path"'
        out = os.popen(ps).read().strip().strip('"')
        if out and out.lower().endswith(("\\terminal64.exe", "\\terminal.exe")):
            return out
        ps2 = r'powershell -NoProfile -Command "(Get-Process terminal -ErrorAction SilentlyContinue | Select-Object -First 1).Path"'
        out2 = os.popen(ps2).read().strip().strip('"')
        if out2 and out2.lower().endswith(("\\terminal64.exe", "\\terminal.exe")):
            return out2
    except Exception:
        pass
    try:
        out = os.popen(
            "wmic process where \"name='terminal64.exe'\" get ExecutablePath /value"
        ).read()
        for line in out.splitlines():
            if line.lower().startswith("executablepath="):
                p = line.split("=", 1)[1].strip()
                if p:
                    return p
        out2 = os.popen(
            "wmic process where \"name='terminal.exe'\" get ExecutablePath /value"
        ).read()
        for line in out2.splitlines():
            if line.lower().startswith("executablepath="):
                p = line.split("=", 1)[1].strip()
                if p:
                    return p
    except Exception:
        pass
    return None


@mt5_locked
def mt5_get_open_positions() -> list[dict] | None:
    """
    Return:
      list[dict] - valid broker snapshot, including [] when genuinely no positions
      None       - MT5/IPC read failure; caller must not publish it as empty
    """
    if not mt5_init():
        _log(
            "[mt5_positions] snapshot invalid "
            "reason=MT5_INIT_FAILED"
        )
        return None

    try:
        positions = MT5.positions_get()

        if positions is None:
            _log(
                "[mt5_positions] snapshot invalid "
                f"reason=POSITIONS_GET_FAILED last_error={MT5.last_error()}"
            )
            return None

        out = []
        symbol_meta_cache: dict[str, dict] = {}

        for p in positions:
            try:
                symbol = str(getattr(p, "symbol", "") or "").upper().strip()
                if not symbol:
                    continue

                if symbol not in symbol_meta_cache:
                    info = MT5.symbol_info(symbol)

                    if info is not None:
                        try:
                            digits = int(getattr(info, "digits", 0) or 0)
                        except Exception:
                            digits = 0

                        try:
                            point = float(getattr(info, "point", 0.0) or 0.0)
                        except Exception:
                            point = 0.0

                        try:
                            trade_tick_size = float(
                                getattr(info, "trade_tick_size", 0.0) or 0.0
                            )
                        except Exception:
                            trade_tick_size = 0.0

                        # Some brokers may not expose trade_tick_size.
                        # point remains the valid broker precision fallback.
                        if trade_tick_size <= 0:
                            trade_tick_size = point

                        symbol_meta_cache[symbol] = {
                            "digits": digits,
                            "point": point,
                            "trade_tick_size": trade_tick_size,
                        }
                    else:
                        symbol_meta_cache[symbol] = {
                            "digits": None,
                            "point": None,
                            "trade_tick_size": None,
                        }

                symbol_meta = symbol_meta_cache[symbol]

                position_type = int(getattr(p, "type", 0) or 0)

                out.append(
                    {
                        "ticket": int(getattr(p, "ticket", 0) or 0),
                        "symbol": symbol,
                        "type": position_type,
                        "side": "BUY" if position_type == 0 else "SELL",
                        "volume": float(getattr(p, "volume", 0.0) or 0.0),
                        "price_open": float(
                            getattr(p, "price_open", 0.0) or 0.0
                        ),
                        "price_current": float(
                            getattr(p, "price_current", 0.0) or 0.0
                        ),
                        "profit": float(getattr(p, "profit", 0.0) or 0.0),
                        "sl": float(getattr(p, "sl", 0.0) or 0.0),
                        "tp": float(getattr(p, "tp", 0.0) or 0.0),
                        "magic": int(getattr(p, "magic", 0) or 0),
                        "comment": str(getattr(p, "comment", "") or ""),
                        "time": int(getattr(p, "time", 0) or 0),
                        "time_msc": int(getattr(p, "time_msc", 0) or 0),

                        # Broker-native symbol precision.
                        "digits": symbol_meta.get("digits"),
                        "point": symbol_meta.get("point"),
                        "trade_tick_size": symbol_meta.get(
                            "trade_tick_size"
                        ),
                    }
                )
            except Exception:
                continue

        return out
    except Exception as e:
        _log(
            "[mt5_positions] snapshot invalid "
            f"reason=EXCEPTION error={type(e).__name__}:{e}"
        )
        return None


def _history_deals_get_locked(dt_from, dt_to):
    """One short serialized MT5 history read; caller sleeps/retries outside lock."""
    with MT5_API_LOCK:
        return MT5.history_deals_get(dt_from, dt_to)


def _history_deals_get_by_position_locked(position_id: int):
    """
    Read complete MT5 deal history for one broker position.

    Position-specific history is the primary recovery source because some
    terminals may return incomplete results from a broad date-range query
    even when the closing deal is available by position ID.
    """
    with MT5_API_LOCK:
        return MT5.history_deals_get(
            position=int(position_id),
        )

def mt5_get_deal_summary(position_id: int, days_back: int = 7) -> dict:
    """
    Passive MT5 deal-history reader.

    Safe Phase-1:
      - only reads MT5 history
      - does not place/close orders
      - does not change lifecycle
      - caller can write result to Redis later as xtl:mt5:deal:{position_id}
    """
    with MT5_API_LOCK:
        init_ok = mt5_init()
    if not init_ok:
        return {
            "ok": False,
            "error": "mt5_init_failed",
            "position_id": int(position_id or 0),
        }

    try:
        from datetime import datetime, timedelta
        import MetaTrader5 as MT5

        pid = int(position_id or 0)
        if pid <= 0:
            return {"ok": False, "error": "bad_position_id", "position_id": pid}

        # MT5 broker/deal time can be ahead of local PC time.
        # Use a forward buffer so freshly closed deals are included.
        dt_to = datetime.now() + timedelta(days=1)
        dt_from = datetime.now() - timedelta(days=int(days_back or 7))

        # ---------------------------------------------------------
        # Primary recovery path: direct position-specific lookup.
        #
        # Some MT5 terminals can return an incomplete result from
        # history_deals_get(from, to), while
        # history_deals_get(position=pid) returns the complete IN/OUT
        # lifecycle for the same position.
        # ---------------------------------------------------------
        deals = _history_deals_get_by_position_locked(pid)
        history_source = "POSITION_LOOKUP"

        if deals is None:
            position_lookup_error = MT5.last_error()

            # Fallback to the previous broad date-range lookup.
            deals = _history_deals_get_locked(
                dt_from,
                dt_to,
            )
            history_source = "DATE_RANGE_FALLBACK"

            if deals is None:
                return {
                    "ok": False,
                    "error": "history_deals_get_failed",
                    "position_id": pid,
                    "position_lookup_error": position_lookup_error,
                    "last_error": MT5.last_error(),
                }

        matched = []

        for d in deals or []:
            try:
                deal_position_id = int(
                    getattr(d, "position_id", 0) or 0
                )
                deal_order = int(
                    getattr(d, "order", 0) or 0
                )
                deal_ticket = int(
                    getattr(d, "ticket", 0) or 0
                )

                if (
                    deal_position_id == pid
                    or deal_order == pid
                    or deal_ticket == pid
                ):
                    matched.append(d)

            except Exception:
                continue

        
        if not matched:
            # MT5 history can lag briefly after a close or reconnect.
            # Retry the position-specific query first on every attempt.
            for _retry in range(5):
                time.sleep(1.0)

                retry_deals = _history_deals_get_by_position_locked(
                    pid
                )

                if retry_deals is not None:
                    deals = retry_deals
                    history_source = "POSITION_LOOKUP_RETRY"

                    matched = []

                    for d in deals or []:
                        try:
                            deal_position_id = int(
                                getattr(
                                    d,
                                    "position_id",
                                    0,
                                )
                                or 0
                            )
                            deal_order = int(
                                getattr(d, "order", 0)
                                or 0
                            )
                            deal_ticket = int(
                                getattr(d, "ticket", 0)
                                or 0
                            )

                            if (
                                deal_position_id == pid
                                or deal_order == pid
                                or deal_ticket == pid
                            ):
                                matched.append(d)

                        except Exception:
                            continue

                if matched:
                    break

            # Final fallback to the broad date-range query.
            if not matched:
                deals = _history_deals_get_locked(
                    dt_from,
                    datetime.now() + timedelta(days=1),
                )
                history_source = "DATE_RANGE_RETRY_FALLBACK"

                if deals is not None:
                    matched = []

                    for d in deals or []:
                        try:
                            deal_position_id = int(
                                getattr(
                                    d,
                                    "position_id",
                                    0,
                                )
                                or 0
                            )
                            deal_order = int(
                                getattr(d, "order", 0)
                                or 0
                            )
                            deal_ticket = int(
                                getattr(d, "ticket", 0)
                                or 0
                            )

                            if (
                                deal_position_id == pid
                                or deal_order == pid
                                or deal_ticket == pid
                            ):
                                matched.append(d)

                        except Exception:
                            continue

            if not matched:
                return {
                    "ok": False,
                    "error": "no_deals_found",
                    "position_id": pid,
                    "days_back": int(days_back or 7),
                    "history_source": history_source,
                    "last_error": MT5.last_error(),
                }
        in_deals = []
        out_deals = []
        all_rows = []

        for d in matched:
            try:
                entry = int(getattr(d, "entry", -1))
                row = {
                    "ticket": int(getattr(d, "ticket", 0) or 0),
                    "order": int(getattr(d, "order", 0) or 0),
                    "time": int(getattr(d, "time", 0) or 0),
                    "time_msc": int(getattr(d, "time_msc", 0) or 0),
                    "type": int(getattr(d, "type", -1)),
                    "entry": entry,
                    "position_id": int(getattr(d, "position_id", 0) or 0),
                    "volume": float(getattr(d, "volume", 0.0) or 0.0),
                    "price": float(getattr(d, "price", 0.0) or 0.0),
                    "commission": float(getattr(d, "commission", 0.0) or 0.0),
                    "swap": float(getattr(d, "swap", 0.0) or 0.0),
                    "profit": float(getattr(d, "profit", 0.0) or 0.0),
                    "fee": float(getattr(d, "fee", 0.0) or 0.0),
                    "symbol": str(getattr(d, "symbol", "") or "").upper(),
                    "comment": str(getattr(d, "comment", "") or ""),
                    "reason": int(getattr(d, "reason", -1)),
                    "magic": int(getattr(d, "magic", 0) or 0),
                    "external_id": str(getattr(d, "external_id", "") or ""),
                }
                all_rows.append(row)

                # MT5 constants are usually:
                # DEAL_ENTRY_IN=0, DEAL_ENTRY_OUT=1, DEAL_ENTRY_INOUT=2
                if entry == getattr(MT5, "DEAL_ENTRY_IN", 0):
                    in_deals.append(row)
                elif entry == getattr(MT5, "DEAL_ENTRY_OUT", 1):
                    out_deals.append(row)
                elif entry == getattr(MT5, "DEAL_ENTRY_INOUT", 2):
                    out_deals.append(row)
            except Exception:
                continue

        open_deal = in_deals[0] if in_deals else (all_rows[0] if all_rows else {})
        close_deals = out_deals or []
        # A position is not confirmed closed unless MT5 returned at
        # least one exit deal. Never construct a broker-close payload
        # from the opening deal alone.
        if not close_deals:
            return {
                "ok": False,
                "error": "no_exit_deal_found",
                "position_id": pid,
                "history_source": history_source,
                "deal_count": len(all_rows),
                "entry_deal_count": len(in_deals),
                "exit_deal_count": 0,
                "last_error": MT5.last_error(),
            }

        closed_volume = sum(float(x.get("volume") or 0.0) for x in close_deals)
        if closed_volume > 0:
            close_price = (
                sum(
                    float(x.get("price") or 0.0) * float(x.get("volume") or 0.0)
                    for x in close_deals
                )
                / closed_volume
            )
        else:
            close_price = float((all_rows[-1] or {}).get("price") or 0.0)

        gross_profit = sum(float(x.get("profit") or 0.0) for x in close_deals)
        commission = sum(float(x.get("commission") or 0.0) for x in all_rows)
        swap = sum(float(x.get("swap") or 0.0) for x in all_rows)
        fee = sum(float(x.get("fee") or 0.0) for x in all_rows)
        net_profit = gross_profit + commission + swap + fee

        close_time_ms = 0
        if close_deals:
            close_time_ms = max(int(x.get("time_msc") or 0) for x in close_deals)
        if close_time_ms <= 0 and all_rows:
            close_time_ms = int(all_rows[-1].get("time_msc") or 0)
            
        close_deal = close_deals[-1] if close_deals else {}

        close_reason_code = int(close_deal.get("reason", -1)) if close_deal else -1
        close_comment = str(close_deal.get("comment") or "").lower()

        broker_reason = "BROKER_CLOSED"

        # Prefer broker comment because MT5 writes clear text like:
        # [tp 1.33659] / [sl 1.42022]
        if "tp" in close_comment or "take profit" in close_comment:
            broker_reason = "TP"
        elif "sl" in close_comment or "stop loss" in close_comment:
            broker_reason = "SL"
        elif "so" in close_comment or "stop out" in close_comment:
            broker_reason = "STOP_OUT"
        elif "manual" in close_comment:
            broker_reason = "MANUAL"

        # Fallback to MT5 reason code if comment is empty/non-standard.
        # Observed in your stored deals: SL=4, TP=5.
        elif close_reason_code == 5:
            broker_reason = "TP"
        elif close_reason_code == 4:
            broker_reason = "SL"

        summary = {
            "ok": True,
            "position_id": pid,
            "position_ticket": pid,
            "history_source": history_source,
            "broker_reason": broker_reason,
            "close_reason_code": close_reason_code,
            "symbol": str(
                open_deal.get("symbol")
                or (all_rows[0].get("symbol") if all_rows else "")
            ).upper(),
            "open_price": float(open_deal.get("price") or 0.0),
            "open_time_ms": int(open_deal.get("time_msc") or 0),
            "close_price": round(float(close_price or 0.0), 6),
            "close_time_ms": int(close_time_ms or 0),
            "volume": round(float(closed_volume or open_deal.get("volume") or 0.0), 4),
            "gross_profit": round(float(gross_profit or 0.0), 2),
            "commission": round(float(commission or 0.0), 2),
            "swap": round(float(swap or 0.0), 2),
            "fee": round(float(fee or 0.0), 2),
            "net_profit": round(float(net_profit or 0.0), 2),
            "deal_count": len(all_rows),

            "entry_deal_ticket": int(open_deal.get("ticket") or 0),

            "exit_deal_ticket": (
                int(close_deals[-1].get("ticket") or 0) if close_deals else 0
            ),

            "close_order_ticket": (
                int(close_deals[-1].get("order") or 0) if close_deals else 0
            ),

            # ----- New broker close metadata -----
            "exit_deal_reason": close_reason_code,
            "exit_comment": (
                str(close_deal.get("comment") or "")
                if close_deal else ""
            ),
            "exit_magic": (
                int(close_deal.get("magic") or 0)
                if close_deal else 0
            ),
            # -------------------------------------

            "comment": str(
                close_deals[-1].get("comment") or open_deal.get("comment") or ""
            ),

            "raw_deals": all_rows,
            "created_at_ms": int(time.time() * 1000),
        }

        return summary

    except Exception as e:
        return {
            "ok": False,
            "error": f"deal_summary_exc:{type(e).__name__}:{e}",
            "position_id": int(position_id or 0),
            
        }

@mt5_locked
def mt5_calc_order_margin(
    symbol: str,
    side: str,
    volume: float,
    price: float | None = None,
) -> dict:
    """
    Broker-native margin calculation using MT5 OrderCalcMargin().
    Returns the exact margin the broker would require.
    """

    try:
        import MetaTrader5 as mt5
    except Exception as e:
        return {
            "ok": False,
            "error": f"mt5_import_failed:{e}",
        }

    try:
        if not mt5_init():
            return {
                "ok": False,
                "error": "mt5_init_failed",
            }

        symbol = str(symbol or "").upper().strip()
        side = str(side or "").upper().strip()
        volume = float(volume or 0)

        broker_symbol = _resolve_broker_symbol(symbol)

        if not mt5.symbol_select(broker_symbol, True):
            return {
                "ok": False,
                "error": "symbol_select_failed",
                "symbol": symbol,
                "broker_symbol": broker_symbol,
            }

        tick = mt5.symbol_info_tick(broker_symbol)
        if tick is None:
            return {
                "ok": False,
                "error": "tick_not_found",
            }

        if price is None or float(price) <= 0:
            if side == "BUY":
                price = tick.ask
            else:
                price = tick.bid

        order_type = mt5.ORDER_TYPE_BUY if side == "BUY" else mt5.ORDER_TYPE_SELL

        required_margin = mt5.order_calc_margin(
            order_type,
            broker_symbol,
            float(volume),
            float(price),
        )

        if required_margin is None:
            return {
                "ok": False,
                "error": "order_calc_margin_failed",
                "last_error": mt5.last_error(),
            }

        acct = mt5.account_info()

        free_margin = float(acct.margin_free)
        equity = float(acct.equity)
        balance = float(acct.balance)
        leverage = int(acct.leverage)

        return {
            "ok": True,
            "required_margin": round(required_margin, 2),
            "free_margin": round(free_margin, 2),
            "remaining_margin": round(
                free_margin - required_margin,
                2,
            ),
            "shortfall": round(
                max(0.0, required_margin - free_margin),
                2,
            ),
            "balance": round(balance, 2),
            "equity": round(equity, 2),
            "leverage": leverage,
            "broker_symbol": broker_symbol,
            "enough_margin": required_margin <= free_margin,
        }

    except Exception as e:
        return {
            "ok": False,
            "error": str(e),
        }


# ---------- logging ----------
def _ts():
    return time.strftime("%Y-%m-%d %H:%M:%S")


def _log(msg: str):
    try:
        import logging

        logging.getLogger("xtl.agent").info(msg)
    except Exception:
        print(f"{_ts()} [mt5] {msg}", flush=True)


# ---------- registry helpers (Windows) ----------

# --- Registry helpers (prefer LocalSystem HKU\S-1-5-18, then HKLM, then HKCU) ---
import sys

try:
    import winreg as _winreg
except Exception:
    _winreg = None  # non-Windows safeguard

import os


# --- robust identity detection (LocalSystem = S-1-5-18) ---
def _current_user_sid() -> str | None:
    import ctypes, ctypes.wintypes as wt

    adv = ctypes.WinDLL("advapi32", use_last_error=True)
    ker = ctypes.WinDLL("kernel32", use_last_error=True)

    GetCurrentProcess = ker.GetCurrentProcess
    OpenProcessToken = adv.OpenProcessToken
    GetTokenInformation = adv.GetTokenInformation
    ConvertSidToStringSidW = adv.ConvertSidToStringSidW
    LocalFree = ker.LocalFree

    TOKEN_QUERY = 0x0008
    TokenUser = 1

    class SID_AND_ATTRIBUTES(ctypes.Structure):
        _fields_ = [("Sid", wt.LPVOID), ("Attributes", wt.DWORD)]

    class TOKEN_USER(ctypes.Structure):
        _fields_ = [("User", SID_AND_ATTRIBUTES)]

    hProc = GetCurrentProcess()
    hTok = wt.HANDLE()
    if not OpenProcessToken(hProc, TOKEN_QUERY, ctypes.byref(hTok)):
        return None

    # first call to get size
    needed = wt.DWORD(0)
    GetTokenInformation(hTok, TokenUser, None, 0, ctypes.byref(needed))
    buf = ctypes.create_string_buffer(needed.value)
    if not GetTokenInformation(hTok, TokenUser, buf, needed, ctypes.byref(needed)):
        return None

    tu = ctypes.cast(buf, ctypes.POINTER(TOKEN_USER)).contents
    out = wt.LPWSTR()
    if not ConvertSidToStringSidW(tu.User.Sid, ctypes.byref(out)):
        return None
    try:
        return out.value
    finally:
        if out:
            LocalFree(out)


# --- robust LocalSystem detection ---
def _is_localsystem() -> bool:
    """
    Use GetUserNameW (returns 'SYSTEM' for LocalSystem). If that fails, fall back to env hints.
    """
    try:
        import ctypes
        from ctypes import wintypes as wt

        GetUserNameW = ctypes.windll.advapi32.GetUserNameW
        GetUserNameW.argtypes = [wt.LPWSTR, ctypes.POINTER(wt.DWORD)]
        GetUserNameW.restype = wt.BOOL

        buf_len = wt.DWORD(256)
        buf = ctypes.create_unicode_buffer(buf_len.value)
        if GetUserNameW(buf, ctypes.byref(buf_len)):
            uname = (buf.value or "").strip().upper()
            if uname in ("SYSTEM", "LOCAL SYSTEM"):
                return True
    except Exception:
        pass

    # fallbacks (not authoritative but helpful)
    u = (os.environ.get("USERNAME", "") or "").upper()
    d = (os.environ.get("USERDOMAIN", "") or "").upper()
    if u in ("SYSTEM", "LOCAL SYSTEM"):
        return True
    if d in ("NT AUTHORITY",):
        return True
    # last hint: services usually run with SESSIONNAME='Services'
    if (os.environ.get("SESSIONNAME", "") or "").lower() == "services":
        return True
    return False


_XTL_SUBKEY = r"Software\XTL"


def _reg_read_value(name: str):
    try:
        import winreg as _wr
    except Exception:
        return None, None  # (value, source)

    WOW64_64 = getattr(_wr, "KEY_WOW64_64KEY", 0x0100)
    WOW64_32 = getattr(_wr, "KEY_WOW64_32KEY", 0x0200)

    # Prefer HKU\S-1-5-18 when running as LocalSystem; otherwise HKCU first.
    if _is_localsystem():
        order = [
            (_wr.HKEY_USERS, r"S-1-5-18\Software\XTL", WOW64_64, "HKU\\S-1-5-18 64"),
            (_wr.HKEY_USERS, r"S-1-5-18\Software\XTL", WOW64_32, "HKU\\S-1-5-18 32"),
            (_wr.HKEY_LOCAL_MACHINE, r"Software\XTL", WOW64_64, "HKLM 64"),
            (_wr.HKEY_LOCAL_MACHINE, r"Software\XTL", WOW64_32, "HKLM 32"),
            (_wr.HKEY_CURRENT_USER, r"Software\XTL", WOW64_64, "HKCU 64"),
            (_wr.HKEY_CURRENT_USER, r"Software\XTL", WOW64_32, "HKCU 32"),
        ]
    else:
        order = [
            (_wr.HKEY_CURRENT_USER, r"Software\XTL", WOW64_64, "HKCU 64"),
            (_wr.HKEY_CURRENT_USER, r"Software\XTL", WOW64_32, "HKCU 32"),
            (_wr.HKEY_LOCAL_MACHINE, r"Software\XTL", WOW64_64, "HKLM 64"),
            (_wr.HKEY_LOCAL_MACHINE, r"Software\XTL", WOW64_32, "HKLM 32"),
            (_wr.HKEY_USERS, r"S-1-5-18\Software\XTL", WOW64_64, "HKU\\S-1-5-18 64"),
            (_wr.HKEY_USERS, r"S-1-5-18\Software\XTL", WOW64_32, "HKU\\S-1-5-18 32"),
        ]

    for hive, subkey, view, tag in order:
        try:
            with _wr.ConnectRegistry(None, hive) as reg:
                with _wr.OpenKey(reg, subkey, 0, _wr.KEY_READ | view) as h:
                    val, _typ = _wr.QueryValueEx(h, name)
                    return val, tag
        except Exception:
            continue
    return None, None


def _broker_meta_from_registry():
    # read offset + name + report source for debugging
    off, off_src = _reg_read_value("Broker.TzOffsetMin")
    nm, nm_src = _reg_read_value("Broker.TzName")
    try:
        off = int(off) if off not in (None, "") else None
    except Exception:
        off = None
    try:
        nm = str(nm) if nm not in (None, "") else None
    except Exception:
        nm = None
    try:
        if off_src:
            _log(f"[reg_read] Broker.TzOffsetMin={off} from {off_src}")
    except Exception:
        pass
    return (nm, off)


def _reg_get_xtl(name):
    """
    Read XTL value from registry. Search order:
      1) HKU\S-1-5-18\Software\XTL        (LocalSystem / Session-0)
      2) HKLM\Software\XTL                (machine-level)
      3) HKCU\Software\XTL                (interactive user)
    Try both 64-bit and 32-bit views where applicable.
    """
    if _winreg is None or sys.platform != "win32":
        return None

    # Windows registry views
    WOW64_64 = getattr(_winreg, "KEY_WOW64_64KEY", 0x0100)
    WOW64_32 = getattr(_winreg, "KEY_WOW64_32KEY", 0x0200)

    # 1) LocalSystem hive
    hku = getattr(_winreg, "HKEY_USERS", None)
    if hku is not None:
        for wow in (WOW64_64, WOW64_32):
            val = _reg_read_value(hku, r"S-1-5-18\{}".format(_XTL_SUBKEY), name, wow)
            if val not in (None, ""):
                return val

    # 2) HKLM
    hklm = getattr(_winreg, "HKEY_LOCAL_MACHINE", None)
    if hklm is not None:
        for wow in (WOW64_64, WOW64_32):
            val = _reg_read_value(hklm, _XTL_SUBKEY, name, wow)
            if val not in (None, ""):
                return val

    # 3) HKCU
    hkcu = getattr(_winreg, "HKEY_CURRENT_USER", None)
    if hkcu is not None:
        for wow in (WOW64_64, WOW64_32):
            val = _reg_read_value(hkcu, _XTL_SUBKEY, name, wow)
            if val not in (None, ""):
                return val

    return None


def _read_reg(root: str, key: str, value: str) -> Optional[str]:
    try:
        import winreg

        hive = winreg.HKEY_LOCAL_MACHINE if root == "HKLM" else winreg.HKEY_CURRENT_USER
        with winreg.OpenKey(hive, key) as k:
            val, _ = winreg.QueryValueEx(k, value)
            return str(val)
    except Exception:
        return None


def _reg_get(name: str) -> str:
    """Prefer HKCU\Software\XTL; fallback to LocalSystem hive HKU\S-1-5-18\Software\XTL."""
    try:
        import winreg

        # HKCU
        try:
            with winreg.OpenKey(winreg.HKEY_CURRENT_USER, r"Software\XTL") as k:
                v, _ = winreg.QueryValueEx(k, name)
                if v:
                    return str(v).strip()
        except Exception:
            pass
        # LocalSystem hive
        try:
            with winreg.OpenKey(winreg.HKEY_USERS, r"S-1-5-18\Software\XTL") as k:
                v, _ = winreg.QueryValueEx(k, name)
                if v:
                    return str(v).strip()
        except Exception:
            pass
    except Exception:
        pass
    return ""


# --- Registry helper (safe, works under LocalSystem + user) ---
def reg_get(name: str, root=None, default=None):
    """Fetch REG_SZ from HKCU/HKLM/HKU\S-1-5-18\Software\XTL in order."""
    import winreg

    keys = [
        (winreg.HKEY_CURRENT_USER, r"Software\XTL"),
        (winreg.HKEY_LOCAL_MACHINE, r"Software\XTL"),
        (winreg.HKEY_USERS, r"S-1-5-18\Software\XTL"),
    ]
    for root, path in keys:
        try:
            with winreg.OpenKey(root, path) as k:
                val, _ = winreg.QueryValueEx(k, name)
                return val
        except Exception:
            continue
    return default


# ---------- MT5 terminal path discovery ----------
def _guess_mt5_path() -> Optional[str]:
    # 1) App registry hints (future-proof if installer writes them)
    for r, k, v in [
        ("HKLM", r"Software\XTL", "MT5Path"),
        ("HKLM", r"Software\XauTrendLab", "MT5Path"),
        ("HKLM", r"Software\XTL", "MT5.TerminalPath"),
    ]:
        p = _read_reg(r, k, v)
        if p and Path(p).is_file():
            return p

    # 2) Common installs
    candidates = [
        r"C:\Program Files\MetaTrader 5\terminal64.exe",
        r"C:\Program Files\MetaTrader 5\terminal.exe",
        r"C:\Program Files (x86)\MetaTrader 5\terminal64.exe",
        r"C:\Program Files (x86)\MetaTrader 5\terminal.exe",
        r"C:\Program Files\MetaTrader 5 Terminal\terminal64.exe",
        r"C:\Program Files (x86)\MetaTrader 5 Terminal\terminal64.exe",
        r"C:\Program Files\RoboForex MT5 Terminal\terminal64.exe",
        r"C:\Program Files (x86)\RoboForex MT5 Terminal\terminal64.exe",
    ]
    for c in candidates:
        if Path(c).is_file():
            return c

    # 3) Try to detect a running terminal (best-effort)
    try:
        out = subprocess.check_output(
            [
                "wmic",
                "process",
                "where",
                "name='terminal64.exe'",
                "get",
                "ExecutablePath",
                "/value",
            ],
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=3,
        )
        for line in out.splitlines():
            if line.startswith("ExecutablePath="):
                exe = line.split("=", 1)[1].strip()
                if exe and Path(exe).is_file():
                    return exe
    except Exception:
        pass
    return None


# ---------- init ----------
def mt5_init() -> bool:
    """
    Attach-only MT5 init:
      - Requires user-opened terminal; never spawns MT5.
      - Attaches to the actually running terminal's EXE to avoid IPC -10003.
      - Logs attempts via TRIED_LOG and returns False until connected.
    """
    import os, sys

    global _MT5_READY

    if _MT5_READY:
        return True
    TRIED_LOG.clear()
    try:
        from datetime import datetime

        _log(
            f"[whoami] is_local_system={_is_localsystem()} sid={_current_user_sid()} now={datetime.utcnow().isoformat()}Z"
        )
    except Exception:
        pass

    # --- ATTACH-ONLY: require a running terminal and resolve its EXE path ---

    if not _mt5_running():
        _log(
            "MT5 attach-only: no terminal64.exe/terminal.exe process detected. "
            "Please open RoboForex MT5 manually and keep it logged in."
        )
        return False
    exe = _find_running_mt5_exe()
    if not exe:
        _log(
            "MT5 attach-only: MT5 is running but path could not be resolved; "
            "trying default MT5.initialize attach..."
        )
        # Try a safe, no-path probe (lets MT5 attach to the already running terminal)
        if _probe(None):
            try:
                tin = MT5.terminal_info()
                ain = MT5.account_info()
                try:
                    srv = str(getattr(tin, "server", "") or "")
                    tpth = str(getattr(tin, "path", "") or "")
                    acc = getattr(ain, "login", None)
                    _log(f"[mt5_init] server={srv} path={tpth} login={acc}")
                except Exception:
                    pass

                # --- Require RoboForex server to avoid MetaQuotes/demo feed drift ---
                try:
                    srv = str(getattr(tin, "server", "") or "")
                    _log(f"[mt5_init] attached server={srv}")
                    if "RoboForex-Pro" not in srv:
                        _log(
                            "MT5: wrong server attached (not RoboForex-Pro). "
                            "Keep RoboForex terminal open & logged in; aborting init."
                        )
                        return False
                except Exception:
                    pass

                _log(
                    "MT5 fallback OK; connected=%s server=%s build=%s login=%s"
                    % (
                        getattr(tin, "connected", None),
                        getattr(tin, "server", None),
                        getattr(tin, "build", None),
                        getattr(ain, "login", None) if ain else None,
                    )
                )
                if getattr(tin, "connected", 0) != 1:
                    _log("MT5: terminal not connected (keep MT5 open and logged in).")
                    return False

                # auto-detect broker TZ from last closed M1 and persist to registry
                try:
                    detect_and_write_broker_tz_any(
                        ["XAUUSD", "EURUSD", "USDJPY", "GBPUSD", "USDCAD", "USDCHF"]
                    )
                    _ensure_broker_offset_fresh()
                except Exception:
                    pass

                _MT5_READY = True
                return True
            except Exception as e:
                _log(f"MT5: fallback init succeeded but info check failed: {e}")
                return False

        # still no luck
        return False
    # NOTE: We intentionally ignore env/CLI/registry/scan for attach-only mode.
    # Attaching to the *running* EXE fixes MetaTrader IPC -10003 reliably.
    candidates: list[str] = [exe]

    # --- try candidate with robust _probe() (non-portable first inside) ----
    ok = False
    for cand in candidates:
        if _probe(cand):
            ok = True
            break

    if not ok:
        try:
            err = MT5.last_error()
        except Exception:
            err = None
        _log(
            "MT5: initialize failed; tried: "
            + (" | ".join(TRIED_LOG) or "<no attempts>")
            + f" | last_error={err}"
        )
        return False

    # --- success: log and require a real logged-in session before 'ready' ---
    try:
        tin = MT5.terminal_info()
        ain = MT5.account_info()
        try:
            srv = str(getattr(tin, "server", "") or "")
            tpth = str(getattr(tin, "path", "") or "")
            acc = getattr(ain, "login", None)
            _log(f"[mt5_init] server={srv} path={tpth} login={acc}")
        except Exception:
            pass

        _log(
            "MT5 init OK; connected=%s server=%s build=%s login=%s"
            % (
                getattr(tin, "connected", None),
                getattr(tin, "server", None),
                getattr(tin, "build", None),
                getattr(ain, "login", None) if ain else None,
            )
        )
        _log(
            "MT5 paths: exe=%s  data=%s"
            % (getattr(tin, "path", "?"), getattr(tin, "data_path", "?"))
        )
        if getattr(tin, "connected", 0) != 1:
            _log("MT5: terminal not connected (keep MT5 open and logged in).")
            return False
    except Exception as e:
        _log(f"MT5: terminal/account info error: {e}")
        return False
    # auto-detect broker TZ from last closed M1 and persist to registry
    try:
        detect_and_write_broker_tz_any(
            ["XAUUSD", "EURUSD", "USDJPY", "GBPUSD", "USDCAD", "USDCHF"]
        )
        _ensure_broker_offset_fresh()
    except Exception:
        pass

    _MT5_READY = True
    return True


def get_mt5_tick_price_and_ts(symbol: str):
    import MetaTrader5 as mt5

    tick = mt5.symbol_info_tick(symbol)
    if not tick:
        return None, None

    # Pick a consistent field (bid is best for FX; last sometimes 0)
    px = None
    try:
        if getattr(tick, "bid", 0) and tick.bid > 0:
            px = float(tick.bid)
        elif getattr(tick, "last", 0) and tick.last > 0:
            px = float(tick.last)
        elif getattr(tick, "ask", 0) and tick.ask > 0:
            px = float(tick.ask)
    except Exception:
        px = None

    if px is None:
        return None, None

    # Broker tick time
    try:
        ts_ms = int(getattr(tick, "time_msc", 0) or 0)
        if ts_ms <= 0:
            ts_ms = int(tick.time * 1000)
    except Exception:
        ts_ms = None

    if not ts_ms:
        return None, None

    return px, ts_ms


def _probe(path_or_none: Optional[str]) -> bool:
    """
    Try variants so MT5.initialize succeeds without showing first-run wizard:
    - path + portable=False (reuse existing profile, preferred)
    - path + portable=True  (fallback, separate data dir)
    - terminal.exe vs terminal64.exe (fallback)
    - default resolver portable False/True as last resort
    """
    import time

    def _try_init(p: Optional[str], portable: bool) -> bool:
        try:
            ok = (
                MT5.initialize(p, portable=portable)
                if p
                else MT5.initialize(portable=portable)
            )
        except Exception as e:
            TRIED_LOG.append(f"init({p or 'default'}, portable={portable}) EXC {e}")
            return False
        if ok:
            TRIED_LOG.append(f"init({p or 'default'}, portable={portable}) -> True")
            return True
        try:
            err = MT5.last_error()
        except Exception:
            err = None
        TRIED_LOG.append(
            f"init({p or 'default'}, portable={portable}) -> False last_error={err}"
        )
        return False

    p = (path_or_none or "").strip() or None

    # 1) path + portable=False (use cached login/profile)
    if _try_init(p, False):
        return True

    # 2) path + portable=True (separate data dir; can show first-run if no login)
    if _try_init(p, True):
        return True

    # 3) If path is terminal64.exe, try terminal.exe (some brokers)
    if p and p.lower().endswith("terminal64.exe"):
        alt = p[: -len("terminal64.exe")] + "terminal.exe"
        time.sleep(0.5)
        if _try_init(alt, False):  # non-portable first
            return True
        time.sleep(0.5)
        if _try_init(alt, True):
            return True

    # 4) Default resolver: non-portable, then portable
    time.sleep(0.5)
    if _try_init(None, False):
        return True
    time.sleep(0.5)
    if _try_init(None, True):
        return True

    return False


# --- safe row accessor for numpy structured rows / dicts / objects ---
def _ff(row, key, default=0):
    try:
        if isinstance(row, dict):
            return row.get(key, default)
        # numpy structured/recarray?
        if (
            hasattr(row, "dtype")
            and getattr(row.dtype, "names", None)
            and key in row.dtype.names
        ):
            return row[key]
        # object with attribute
        return getattr(row, key, default)
    except Exception:
        return default


# ---------- timeframe helpers ----------
def _tf_seconds(tf: str) -> int:
    tf = (tf or "").upper()
    return {
        "M1": 60,
        "M5": 300,
        "M15": 900,
        "M30": 1800,
        "H1": 3600,
        "H2": 7200,
        "H4": 14400,
        "D1": 86400,
        "W1": 604800,
        "MN1": 2592000,
    }.get(tf, 3600)


def _map_tf(name: str):
    """
    Map a string timeframe label (e.g. "M15", "H1") to its MetaTrader5 constant.
    Returns 0 if MT5 is not initialized or label is not recognized.
    """
    if MT5 is None:
        return 0
    n = (name or "").upper()
    m = {
        "M1": MT5.TIMEFRAME_M1,
        "M5": MT5.TIMEFRAME_M5,
        "M10": getattr(MT5, "TIMEFRAME_M10", MT5.TIMEFRAME_M5),
        "M15": MT5.TIMEFRAME_M15,
        "M30": MT5.TIMEFRAME_M30,
        "H1": MT5.TIMEFRAME_H1,
        # H2 exists in newer MT5 builds; fall back to H1 if missing
        "H2": getattr(MT5, "TIMEFRAME_H2", MT5.TIMEFRAME_H1),
        "H4": MT5.TIMEFRAME_H4,
        "D1": MT5.TIMEFRAME_D1,
    }
    return m.get(n, 0)


# --- helpers: broker id + current stored offset ---
def _current_broker_id() -> str:
    try:
        import MetaTrader5 as MT5

        ti = MT5.terminal_info()
        company = getattr(ti, "company", "") or ""
        server = getattr(ti, "server", "") or ""
        return f"{company}|{server}"
    except Exception:
        return ""


def _reg_read(name: str) -> str | None:
    val, _src = _reg_read_value(
        name
    )  # uses identity-aware order + logs via _broker_meta_from_registry
    try:
        return str(val) if val not in (None, "") else None
    except Exception:
        return None


# ---------- symbol resolution ----------
# --- Helper: resolve actual broker symbol name (handles suffix variants) ---
# Cache logical symbol -> broker-native symbol.
# Resolution is stable for the lifetime of the connected MT5 session.
_BROKER_SYMBOL_CACHE: dict[str, str] = {}


def _resolve_broker_symbol(base: str) -> str:
    """
    Resolve a canonical XTL symbol to the actual broker-native MT5 symbol.

    Normal instruments:
      - prefer exact symbol
      - otherwise support broker suffixes such as XAUUSD.a

    Canonical DXY:
      - resolve per broker/device
      - examples: DXY.cash, DXY, USDX, USDIndex
      - never borrow another device's feed
      - return the actual symbol available in this MT5 terminal

    The caller continues publishing under the canonical XTL symbol supplied
    to mt5_fetch_rates(), while the bars come from the resolved broker symbol.
    """

    logical = str(base or "").strip()
    logical_u = logical.upper()

    if not logical:
        return logical

    cached = _BROKER_SYMBOL_CACHE.get(logical_u)
    if cached:
        try:
            if MT5.symbol_info(cached) is not None:
                return cached
        except Exception:
            pass

        _BROKER_SYMBOL_CACHE.pop(logical_u, None)

    try:
        # -------------------------------------------------
        # Special canonical DXY resolution.
        # -------------------------------------------------
        if logical_u == "DXY":
            exact_candidates = [
                "DXY.cash",   # FTMO confirmed
                "DXY",
                "USDX",
                "USDIndex",
                "USDINDEX",
                
            ]

            for candidate in exact_candidates:
                try:
                    info = MT5.symbol_info(candidate)
                except Exception:
                    info = None

                if info is None:
                    continue

                try:
                    MT5.symbol_select(candidate, True)
                except Exception:
                    pass

                _BROKER_SYMBOL_CACHE[logical_u] = candidate

                _log(
                    "[dxy_resolve] "
                    f"logical=DXY broker_symbol={candidate} "
                    f"description={getattr(info, 'description', '')!r}"
                )

                return candidate

            # Broker suffix/prefix discovery.
            discovered = []

            for pattern in (
                "DXY*",
                "*DXY*",
                "USDX*",
                "*USDX*",
                "USDIndex*",
                "*USDIndex*",
            ):
                try:
                    symbols = MT5.symbols_get(pattern) or []
                except Exception:
                    symbols = []

                for item in symbols:
                    name = str(
                        getattr(item, "name", None)
                        or getattr(item, "symbol", None)
                        or ""
                    ).strip()

                    if name and name not in discovered:
                        discovered.append(name)

            # Last discovery fallback: inspect descriptions for Dollar Index.
            if not discovered:
                try:
                    for item in MT5.symbols_get() or []:
                        name = str(
                            getattr(item, "name", None)
                            or getattr(item, "symbol", None)
                            or ""
                        ).strip()

                        description = str(
                            getattr(item, "description", None)
                            or ""
                        ).lower()

                        if (
                            name
                            and (
                                "dollar index" in description
                                or "us dollar index" in description
                            )
                        ):
                            discovered.append(name)
                except Exception:
                    pass

            for candidate in discovered:
                try:
                    if not MT5.symbol_select(candidate, True):
                        continue
                except Exception:
                    continue

                info = MT5.symbol_info(candidate)

                _BROKER_SYMBOL_CACHE[logical_u] = candidate

                _log(
                    "[dxy_resolve] "
                    f"logical=DXY broker_symbol={candidate} "
                    f"description={getattr(info, 'description', '')!r}"
                )

                return candidate

            _log(
                "[dxy_resolve] logical=DXY result=NOT_FOUND"
            )

            # Returning DXY lets the normal select path fail safely.
            return logical

        # -------------------------------------------------
        # Existing normal-symbol resolution.
        # -------------------------------------------------
        info = MT5.symbol_info(logical)

        if info is not None:
            try:
                MT5.symbol_select(logical, True)
            except Exception:
                pass

            _BROKER_SYMBOL_CACHE[logical_u] = logical
            return logical

        candidates = MT5.symbols_get(f"{logical}*") or []

        if not candidates:
            return logical

        # Preserve the existing special preference for the spot-gold feed.
        if logical_u == "XAUUSD":
            for item in candidates:
                description = str(
                    getattr(item, "description", None)
                    or ""
                ).lower()

                if (
                    "gold vs us dollar (spot)" in description
                    or "gold vs usd (spot)" in description
                ):
                    name = str(
                        getattr(item, "name", None)
                        or getattr(item, "symbol", None)
                        or ""
                    ).strip()

                    if name:
                        try:
                            MT5.symbol_select(name, True)
                        except Exception:
                            pass

                        _BROKER_SYMBOL_CACHE[logical_u] = name
                        return name

        for item in candidates:
            if not getattr(item, "visible", False):
                continue

            name = str(
                getattr(item, "name", None)
                or getattr(item, "symbol", None)
                or ""
            ).strip()

            if name:
                _BROKER_SYMBOL_CACHE[logical_u] = name
                return name

        name = str(
            getattr(candidates[0], "name", None)
            or getattr(candidates[0], "symbol", None)
            or logical
        ).strip()

        if name:
            try:
                MT5.symbol_select(name, True)
            except Exception:
                pass

            _BROKER_SYMBOL_CACHE[logical_u] = name
            return name

    except Exception as exc:
        _log(
            "[symbol_resolve] "
            f"logical={logical} err={type(exc).__name__}:{exc}"
        )

    return logical

def _mt5_last_error():
    """Return (code, message) from MT5.last_error() safely."""
    try:
        import MetaTrader5 as MT5

        return MT5.last_error()
    except Exception:
        return (None, "unknown")

# ---------- reconnect throttle / circuit breaker ----------
# Prevents the per-symbol poll path from hammering MT5 (and therefore the
# broker) with shutdown+initialize pairs while the terminal is disconnected.
_RECONNECT_LAST_ATTEMPT   = 0.0     # monotonic timestamp of last attempt
_RECONNECT_FAIL_COUNT     = 0       # consecutive failures
_RECONNECT_BACKOFF_S      = 30.0    # current cooldown, doubles on failure
_RECONNECT_BACKOFF_MIN_S  = 30.0
_RECONNECT_BACKOFF_MAX_S  = 900.0   # 15 min ceiling
_RECONNECT_FAIL_LIMIT     = 10      # trip the breaker after this many
_RECONNECT_TRIPPED        = False


def mt5_reconnect_state() -> dict:
    """Expose breaker state for health endpoints / logging."""
    return {
        "tripped":    _RECONNECT_TRIPPED,
        "fail_count": _RECONNECT_FAIL_COUNT,
        "backoff_s":  _RECONNECT_BACKOFF_S,
    }


def mt5_reconnect_reset() -> None:
    """Manually clear the breaker (call after operator intervention)."""
    global _RECONNECT_LAST_ATTEMPT, _RECONNECT_FAIL_COUNT
    global _RECONNECT_BACKOFF_S, _RECONNECT_TRIPPED
    _RECONNECT_LAST_ATTEMPT = 0.0
    _RECONNECT_FAIL_COUNT   = 0
    _RECONNECT_BACKOFF_S    = _RECONNECT_BACKOFF_MIN_S
    _RECONNECT_TRIPPED      = False
    _log("[mt5] reconnect breaker manually reset")

@mt5_locked
def _mt5_reconnect():
    """
    Attach-only hard reconnect.

    NEVER spawns a terminal. MT5.initialize(path=...) launches terminal64.exe
    when it cannot attach; a terminal spawned from this process inherits our
    environment, and if APPDATA is absent MT5 builds its data folder at
    C:\\MetaQuotes\\... which it cannot write to. That produced 5,272
    auth/sync cycles against the broker on 2026-08-13.

    Guards, in order:
      1. circuit breaker  -- stop entirely after N consecutive failures
      2. attach-only      -- refuse unless a terminal is already running
      3. cooldown         -- one attempt per backoff window, doubling
    """
    global _RECONNECT_LAST_ATTEMPT, _RECONNECT_FAIL_COUNT
    global _RECONNECT_BACKOFF_S, _RECONNECT_TRIPPED

    import time as _time
    import MetaTrader5 as MT5

    # --- guard 1: circuit breaker -------------------------------------
    if _RECONNECT_TRIPPED:
        return False

    # --- guard 2: attach-only -----------------------------------------
    if not _mt5_running():
        _log("[mt5] reconnect refused: no terminal64.exe/terminal.exe running. "
             "Open MT5 manually and keep it logged in.")
        return False

    # --- guard 3: cooldown --------------------------------------------
    now = _time.monotonic()
    if _RECONNECT_LAST_ATTEMPT and (now - _RECONNECT_LAST_ATTEMPT) < _RECONNECT_BACKOFF_S:
        return False
    _RECONNECT_LAST_ATTEMPT = now

    def _fail(reason: str) -> bool:
        global _RECONNECT_FAIL_COUNT, _RECONNECT_BACKOFF_S, _RECONNECT_TRIPPED
        _RECONNECT_FAIL_COUNT += 1
        _RECONNECT_BACKOFF_S = min(_RECONNECT_BACKOFF_S * 2.0,
                                   _RECONNECT_BACKOFF_MAX_S)
        _log(f"[mt5] reconnect failed ({reason}); "
             f"attempt={_RECONNECT_FAIL_COUNT} "
             f"next_wait={_RECONNECT_BACKOFF_S:.0f}s")
        if _RECONNECT_FAIL_COUNT >= _RECONNECT_FAIL_LIMIT:
            _RECONNECT_TRIPPED = True
            _log(f"[mt5] ALERT breaker TRIPPED after {_RECONNECT_FAIL_COUNT} "
                 f"failures; no further attempts until restart or "
                 f"mt5_reconnect_reset()")
        return False

    try:
        MT5.shutdown()
    except Exception:
        pass

    exe = (reg_get("MT5.TerminalPath") or reg_get("MT5Path") or "").strip()
    ok = False
    try:
        ok = MT5.initialize(path=exe) if exe else MT5.initialize()
    except Exception as e:
        return _fail(f"initialize EXC {e}")

    if not ok:
        return _fail(f"initialize False path='{exe}' err={_mt5_last_error()}")

    # Wait for IPC to come up (~3s)
    for _ in range(12):
        try:
            ti = MT5.terminal_info()
            ai = MT5.account_info()
            if ti and getattr(ti, "connected", False):
                _RECONNECT_FAIL_COUNT = 0
                _RECONNECT_BACKOFF_S  = _RECONNECT_BACKOFF_MIN_S
                _log("[mt5] reconnect OK; terminal connected server=%s login=%s"
                     % (getattr(ti, "server", None),
                        getattr(ai, "login", None) if ai else None))
                # refresh broker TZ if possible (best effort)
                try:
                    detect_and_write_broker_tz_any(
                        ["XAUUSD", "EURUSD", "USDJPY",
                         "GBPUSD", "USDCAD", "USDCHF"])
                    _ensure_broker_offset_fresh()
                except Exception:
                    pass
                return True
        except Exception:
            pass
        _time.sleep(0.25)

    return _fail(f"IPC timeout err={_mt5_last_error()}")

def _is_trusted_broker_tz(off: int | None, source: str | None = None) -> bool:
    try:
        off = int(off)
    except Exception:
        return False

    if off < -720 or off > 900:
        return False

    src = (source or "").lower()

    # 330 was the historical IST/PC-time bug.
    # Only allow 330 if it was actually auto-detected by MT5.
    if off == 330 and src != "auto_detected":
        return False

    return src == "auto_detected"


def _probe_broker_offset_min() -> int | None:
    """
    Runtime TZ probe is now registry-only.

    We no longer try to infer the offset from M1 bars vs local clock,
    because that was occasionally giving the wrong value (e.g. +60
    instead of +120) and causing 1-hour / 1-day shifts on the UI.

    Return the integer Broker.TzOffsetMin if present, else None.
    """
    try:
        raw = _reg_read("Broker.TzOffsetMin")
    except Exception:
        raw = None

    if raw is None or raw == "":
        return None

    try:
        off = int(raw)
        if -720 <= off <= 900:
            return off
    except Exception:
        pass
    return None


# cache for this process
_BROKER_OFF_MIN_CACHE: int | None = None


def _broker_offset_min() -> int:
    """
    Broker minutes EAST of UTC. Prefer in-process cache → live probe → registry → env → 0.
    """
    global _BROKER_OFF_MIN_CACHE

    # 1) cache
    if _BROKER_OFF_MIN_CACHE is not None:
        return int(_BROKER_OFF_MIN_CACHE)

    # 2) probe (cheap: 1 M1 bar)
    try:
        p = _probe_broker_offset_min()
        src = _reg_read("Broker.TzSource") or ""
        if _is_trusted_broker_tz(p, src):
            _BROKER_OFF_MIN_CACHE = int(p)
            return _BROKER_OFF_MIN_CACHE
    except Exception:
        pass

    # 3) registry
    try:
        tz_name, off_min = _broker_meta_from_registry()
        src = _reg_read("Broker.TzSource") or ""
        if _is_trusted_broker_tz(off_min, src):
            _BROKER_OFF_MIN_CACHE = int(off_min)
            return int(off_min)
    except Exception:
        pass

    # 4) env
    try:
        env_off = os.getenv("FORCE_TZ_OFFSET_MIN")
        if env_off not in (None, ""):
            return int(str(env_off).strip())
    except Exception:
        pass

    # 5) default
    raise RuntimeError("broker timezone offset is not trusted/auto_detected")


def _ensure_broker_offset_fresh():
    """
    Load trusted Broker.TzOffsetMin into cache.
    Only auto_detected broker timezone is trusted.
    """
    try:
        raw = _reg_read("Broker.TzOffsetMin")
    except Exception:
        raw = None

    if raw is None or raw == "":
        _log("[mt5_tz] Broker.TzOffsetMin missing; waiting for auto-detection.")
        return

    try:
        off = int(raw)
        src = _reg_read("Broker.TzSource") or ""

        if _is_trusted_broker_tz(off, src):
            global _BROKER_OFF_MIN_CACHE
            _BROKER_OFF_MIN_CACHE = off
            _log(f"[mt5_tz] using trusted broker offset {off} minutes source={src}")
        else:
            _log(f"[mt5_tz] untrusted broker offset ignored off={off} source={src}")

    except Exception as e:
        _log(f"[mt5_tz] invalid Broker.TzOffsetMin '{raw}': {e}")


# --- Broker timezone: detect from MT5 bars and persist to registry ---

import time as _time

try:
    import winreg as _wr
except Exception:
    _wr = None


# --- replace _xtl_reg_write_all with this version ---
def _xtl_reg_write_all(name: str, value: str) -> None:
    try:
        import winreg as _wr
    except Exception:
        return

    WOW64_64 = getattr(_wr, "KEY_WOW64_64KEY", 0x0100)
    WOW64_32 = getattr(_wr, "KEY_WOW64_32KEY", 0x0200)

    if _is_localsystem():
        targets = [
            (_wr.HKEY_USERS, r"S-1-5-18\Software\XTL", WOW64_64, "HKU\\S-1-5-18 64"),
            (_wr.HKEY_USERS, r"S-1-5-18\Software\XTL", WOW64_32, "HKU\\S-1-5-18 32"),
            (_wr.HKEY_LOCAL_MACHINE, r"Software\XTL", WOW64_64, "HKLM 64"),
            (_wr.HKEY_LOCAL_MACHINE, r"Software\XTL", WOW64_32, "HKLM 32"),
            (_wr.HKEY_CURRENT_USER, r"Software\XTL", WOW64_64, "HKCU 64"),
            (_wr.HKEY_CURRENT_USER, r"Software\XTL", WOW64_32, "HKCU 32"),
        ]
    else:
        targets = [
            (_wr.HKEY_CURRENT_USER, r"Software\XTL", WOW64_64, "HKCU 64"),
            (_wr.HKEY_CURRENT_USER, r"Software\XTL", WOW64_32, "HKCU 32"),
            (_wr.HKEY_LOCAL_MACHINE, r"Software\XTL", WOW64_64, "HKLM 64"),
            (_wr.HKEY_LOCAL_MACHINE, r"Software\XTL", WOW64_32, "HKLM 32"),
            (_wr.HKEY_USERS, r"S-1-5-18\Software\XTL", WOW64_64, "HKU\\S-1-5-18 64"),
            (_wr.HKEY_USERS, r"S-1-5-18\Software\XTL", WOW64_32, "HKU\\S-1-5-18 32"),
        ]

    for hive, subkey, view, tag in targets:
        try:
            with _wr.ConnectRegistry(None, hive) as reg:
                try:
                    h = _wr.CreateKeyEx(reg, subkey, 0, _wr.KEY_SET_VALUE | view)
                except OSError:
                    h = _wr.OpenKey(reg, subkey, 0, _wr.KEY_SET_VALUE | view)
                with h:
                    _wr.SetValueEx(h, name, 0, _wr.REG_SZ, value)
                    try:
                        _log(f"[reg_write] {name}='{value}' -> {tag}")
                    except Exception:
                        pass
        except Exception as e:
            try:
                _log(f"[reg_write] {name}='{value}' -> {tag} FAILED: {e!r}")
            except Exception:
                pass


def _xtl_reg_delete_all(name: str) -> None:
    """Delete a value across HKU\S-1-5-18, HKLM, HKCU (both 64/32 views). Safe if missing."""
    try:
        _wr  # ensure winreg alias exists
    except NameError:
        return
    WOW64_64 = getattr(_wr, "KEY_WOW64_64KEY", 0x0100)
    WOW64_32 = getattr(_wr, "KEY_WOW64_32KEY", 0x0200)
    targets = [
        (_wr.HKEY_USERS, r"S-1-5-18\Software\XTL", WOW64_64),
        (_wr.HKEY_USERS, r"S-1-5-18\Software\XTL", WOW64_32),
        (_wr.HKEY_LOCAL_MACHINE, r"Software\XTL", WOW64_64),
        (_wr.HKEY_LOCAL_MACHINE, r"Software\XTL", WOW64_32),
        (_wr.HKEY_CURRENT_USER, r"Software\XTL", WOW64_64),
        (_wr.HKEY_CURRENT_USER, r"Software\XTL", WOW64_32),
    ]
    for hive, subkey, view in targets:
        try:
            with _wr.ConnectRegistry(None, hive) as reg:
                with _wr.OpenKey(reg, subkey, 0, (_wr.KEY_SET_VALUE | view)) as h:
                    try:
                        _wr.DeleteValue(h, name)
                    except FileNotFoundError:
                        pass
                    except Exception:
                        pass
        except Exception:
            pass


def _tz_label(mins: int) -> str:
    s = "+" if mins >= 0 else "-"
    m = abs(mins)
    return f"UTC{s}{m//60:02d}:{m%60:02d}"


# --- bootstrap helpers ---
def _pick_best_known_offset() -> tuple[int | None, str | None]:
    """
    Return (off_min, source_tag) from any hive that has a valid value.
    Prefers: if running as LocalSystem we try to reuse HKCU; if User, we try S-1-5-18.
    Skips None/''/out-of-range and 330 (IST) unless your broker is actually IST.
    """
    off_cu, src_cu = _reg_read_value(
        "Broker.TzOffsetMin"
    )  # typically HKCU when run as user
    off_ls, src_ls = None, None
    # try reading S-1-5-18 directly (even if we aren't LocalSystem)
    try:
        import winreg as _wr

        WOW64_64 = getattr(_wr, "KEY_WOW64_64KEY", 0x0100)
        with _wr.ConnectRegistry(None, _wr.HKEY_USERS) as reg:
            with _wr.OpenKey(
                reg, r"S-1-5-18\Software\XTL", 0, _wr.KEY_READ | WOW64_64
            ) as h:
                off_ls, _ = _wr.QueryValueEx(h, "Broker.TzOffsetMin")
                src_ls = "HKU\\S-1-5-18 64"
    except Exception:
        pass

    def _coerce(v):
        try:
            return int(v)
        except:
            return None

    cand = []
    for val, tag in [(off_cu, src_cu), (off_ls, src_ls)]:
        iv = _coerce(val)
        if iv is not None and -720 <= iv <= 900 and iv != 330:
            cand.append((iv, tag))
    return cand[0] if cand else (None, None)


def _tz_bootstrap_to_identity_hive(off_min: int) -> None:
    """Write off_min + label into the identity-correct hive and log."""
    label = f"UTC{('+' if off_min>=0 else '')}{off_min//60:02d}:{abs(off_min)%60:02d}"
    _xtl_reg_write_all("Broker.TzOffsetMin", str(off_min))
    _xtl_reg_write_all("Broker.TzName", label)
    try:
        _log(f"[tz_bootstrap] wrote off_min={off_min} label='{label}' to identity hive")
    except Exception:
        pass


def detect_and_write_broker_tz_any(symbols: list[str]) -> None:
    """
    Detect broker UTC offset (minutes) from last CLOSED M1 bar and persist it.
    - Tries each symbol in order (ensures visible in Market Watch).
    - Normalizes numpy arrays safely (no 'ambiguous truth value' errors).
    - If direct read fails, falls back to grid-scan against now().
    - Writes both Broker.TzOffsetMin and Broker.TzName to all XTL hives.
    """
    import MetaTrader5 as MT5
    from datetime import datetime, timezone, timedelta
    import time as _time

    def _norm(arr):
        if arr is None:
            return []
        try:
            return list(arr)
        except Exception:
            return [arr]

    # Try each candidate symbol
    for base in symbols or []:
        try:
            # Resolve and ensure visible in Market Watch
            resolved = _resolve_broker_symbol(base)
            try:
                MT5.symbol_select(resolved, True)
            except Exception:
                pass

            # Get the last CLOSED M1 bar via POS (previous bar)
            arr = MT5.copy_rates_from_pos(resolved, MT5.TIMEFRAME_M1, 1, 1)
            bars = _norm(arr)
            if not bars:
                continue

            r = bars[0]
            t_open_sec = int(_ff(r, "time", 0))
            if not t_open_sec:
                continue

            # Build candidate offsets: prefer exact nearest 15-min grid
            now_ms = int(_time.time() * 1000)
            tf_ms = 60_000  # M1
            t_open_ms = t_open_sec * 1000
            t_close_ms = t_open_ms + tf_ms

            best_off = None
            best_err = 10**18
            # scan -12h .. +15h in 15-min steps
            for off_min in range(-720, 901, 15):
                off_ms = off_min * 60_000
                # next close boundary for "now" at this offset:
                # floor((now+off)/tf)*tf - off
                slot_try = ((now_ms + off_ms) // tf_ms) * tf_ms - off_ms
                err = abs(slot_try - t_close_ms)
                if err < best_err:
                    best_err = err
                    best_off = off_min
                    if err <= 1500:
                        break

            # If grid-scan didn’t converge tightly, still accept within 5s
            if best_off is None or best_err > 5000:
                # fallback: infer offset directly from local time vs bar time
                # broker ~= (t_open_ms + tf_ms) - now  -> round to 15 min.
                delta_min = round(((t_close_ms - now_ms) / 60_000) / 15) * 15
                best_off = int(delta_min)

            # Hard bounds safety
            if not (-720 <= best_off <= 900):
                continue

            # FRESHNESS GUARD: only persist if the M1 bar is recent (market open).
            # Stale bar (market closed) -> this symbol's offset is unreliable; try next symbol.
            bar_age_min = (now_ms - t_close_ms) / 60_000
            if bar_age_min > 5:
                _log(
                    f"[mt5_detect_tz] {base}: M1 bar stale ({bar_age_min:.0f}min) - skip, try next symbol"
                )
                continue

            # Persist (RE-ENABLED with freshness guard + source tag)
            global _BROKER_OFF_MIN_CACHE

            if not (-720 <= int(best_off) <= 900):
                _log(f"[P0_TZ_BLOCK] ignored out-of-range detected offset={best_off}")
                continue

            _xtl_reg_write_all("Broker.TzOffsetMin", str(int(best_off)))
            _xtl_reg_write_all("Broker.TzName", _tz_label(int(best_off)))
            _xtl_reg_write_all("Broker.TzSource", "auto_detected")
            _BROKER_OFF_MIN_CACHE = int(best_off)
            try:
                _log("[mt5_detect_tz] verifying registry writes after auto_detected")
                for _name in ("Broker.TzOffsetMin", "Broker.TzName", "Broker.TzSource"):
                    _val = _reg_read(_name)
                    _log(f"[mt5_detect_tz] verify {_name}={_val}")
            except Exception as e:
                _log(f"[mt5_detect_tz] verify registry failed: {e}")
            _log(
                f"[mt5_detect_tz] {resolved}: DETECTED off_min={best_off} ({_tz_label(best_off)}) age={bar_age_min:.0f}min err_ms={int(best_err)}"
            )
            return

        except Exception as e:
            _log(f"[mt5_detect_tz] {base} detection error: {e}")
            continue

    _log(
        "[mt5_detect_tz] unable to detect broker offset from any candidate (market closed?)"
    )


# ---------- rates -> dicts ----------
def _np_to_dicts(rates) -> List[Dict]:
    out: List[Dict] = []
    if rates is None:
        return out
    try:
        import numpy as np  # noqa

        if isinstance(rates, np.ndarray) and rates.size > 0:
            names = tuple(rates.dtype.names or ())
            for row in rates:
                try:
                    t = int(row["time"] if "time" in names else row[0])
                    o = float(row["open"] if "open" in names else row[1])
                    h = float(row["high"] if "high" in names else row[2])
                    l = float(row["low"] if "low" in names else row[3])
                    c = float(row["close"] if "close" in names else row[4])
                    v = int(
                        row["tick_volume"]
                        if "tick_volume" in names
                        else (row[5] if len(names) >= 6 else 0)
                    )
                    out.append({"t": t, "o": o, "h": h, "l": l, "c": c, "v": v})
                except Exception:
                    continue
    except Exception:
        try:
            for row in rates:
                t = int(getattr(row, "time", row[0]))
                o = float(getattr(row, "open", row[1]))
                h = float(getattr(row, "high", row[2]))
                l = float(getattr(row, "low", row[3]))
                c = float(getattr(row, "close", row[4]))
                v = int(getattr(row, "tick_volume", row[5] if len(row) > 5 else 0))
                out.append({"t": t, "o": o, "h": h, "l": l, "c": c, "v": v})
        except Exception:
            return []
    return out


def _assert_tail_parity(sym, tf_code, tf_ms, rows ,broker_off_ms=0,):
    try:
        probe = MT5.copy_rates_from_pos(sym, tf_code, 1, 1)
        if probe is None:
            return
        try:
            probe = list(probe)
        except:
            probe = [probe]
        if len(probe) != 1 or not rows:
            return
        t_ms = (
            int(probe[0]["time"]) * 1000
            - int(broker_off_ms or 0)
)
        if rows[-1]["t_open_ms"] != t_ms:
            raise RuntimeError(
                f"Tail parity fail: rows[-1]={rows[-1]['t_open_ms']} vs MT5.prev={t_ms}"
            )
    except Exception as e:
        _log(f"[mt5_assert] {e}")


# ---------- PUBLIC: fetch last N CLOSED bars ----------
def mt5_fetch_rates(
    sym: str, timeframe, count: int = 300, include_latest: bool = False
):
    """
    Return exactly the last `count` CLOSED bars aligned to the broker TF grid.
    If include_latest=True, also append the *previous closed* slot (still complete=True).
    """
    import time as _time
    from datetime import datetime
    import MetaTrader5 as MT5

    # --- ensure we have a live MT5 connection before doing anything ---
    try:
        ti = MT5.terminal_info()
    except Exception:
        ti = None

    if not (ti and getattr(ti, "connected", False)):
        if not _mt5_reconnect():
            _log(f"[mt5_fetch_rates] MT5 not connected; aborting for {sym}/{timeframe}")
            return []

    # ---------- SAFE FIELD ACCESSOR (dict or numpy.void) ----------
    def _f(r, name, default=0):
        try:
            if isinstance(r, dict):
                return r.get(name, default)
            if hasattr(r, "dtype") and getattr(r.dtype, "names", None):
                if name in r.dtype.names:
                    return r[name]
            return getattr(r, name, default)
        except Exception:
            return default

    # --- timeframe sizes ---
    TF_SEC_MAP = {
        "M1": 60,
        "M5": 5 * 60,
        "M10": 10 * 60,
        "M15": 15 * 60,
        "H1": 60 * 60,
        "H4": 4 * 60 * 60,
        "H2": 2 * 60 * 60,
    }
    tf_label = (
        str(timeframe)
        if isinstance(timeframe, str)
        else getattr(timeframe, "name", "H1")
    )
    tf_label = tf_label.upper()
    if tf_label.startswith("TIMEFRAME_"):
        tf_label = tf_label.split("TIMEFRAME_", 1)[1]
    tf_sec = TF_SEC_MAP.get(tf_label, 60 * 60)
    tf_ms = tf_sec * 1000
    # --- ensure broker offset is fresh before any logging/formatting ---
    try:
        probe = _probe_broker_offset_min()
    except Exception:
        probe = None

    stored_raw = _reg_read("Broker.TzOffsetMin")
    try:
        stored_int = int(stored_raw) if stored_raw not in (None, "") else None
    except Exception:
        stored_int = None

    # If probe succeeded and differs (or stored missing) -> write and cache now
    try:
        if (
            probe is not None
            and -720 <= int(probe) <= 900
            and (stored_int is None or int(probe) != stored_int)
        ):
            global _BROKER_OFF_MIN_CACHE
            src = _reg_read("Broker.TzSource") or ""
            if _is_trusted_broker_tz(probe, src):
                _BROKER_OFF_MIN_CACHE = int(probe)
            else:
                _log(
                    f"[P0_TZ_BLOCK] ignored untrusted probe offset={probe} source={src}"
                )

                _log(
                    f"[mt5_tz] refreshed: stored_off={stored_int} -> probe_off={int(probe)}"
                )
    except Exception as _e:
        _log(f"[mt5_tz] refresh error: {getattr(_e,'args',_e)}")

    # Final chosen offset for this call: cache → probe → stored → 0
    try:
        off_min_fresh = _BROKER_OFF_MIN_CACHE
    except NameError:
        off_min_fresh = None
    if off_min_fresh is None:
        src = _reg_read("Broker.TzSource") or ""
        if _is_trusted_broker_tz(probe, src):
            off_min_fresh = int(probe)
        elif _is_trusted_broker_tz(stored_int, src):
            off_min_fresh = int(stored_int)
        else:
            raise RuntimeError(
                f"broker timezone not trusted: probe={probe} stored={stored_int} source={src}"
            )
    try:
        _log(f"[mt5_tz] probe={probe} stored={stored_int} chosen_off={off_min_fresh}")
    except Exception:
        pass
    
    # If probe failed (MT5 not connected in this session), bootstrap from any good hive
    # If probe failed, bootstrap only from trusted auto_detected hive.
    if probe is None:
        try:
            best_off, best_src = _pick_best_known_offset()
            src = _reg_read("Broker.TzSource") or ""

            if _is_trusted_broker_tz(best_off, src):
                _tz_bootstrap_to_identity_hive(best_off)
                _BROKER_OFF_MIN_CACHE = int(best_off)
                off_min_fresh = int(best_off)
                _log(
                    f"[tz_bootstrap] adopted trusted off_min={best_off} from {best_src}"
                )
            else:
                _log(
                    f"[P0_TZ_BLOCK] ignored bootstrap offset={best_off} source={src} from={best_src}"
                )

        except Exception:
            pass
    # ------------------------------------------------------------
    # Canonical timestamp conversion.
    # off_min_fresh is final at this point, including bootstrap.
    # ------------------------------------------------------------
    broker_off_ms = int(off_min_fresh) * 60_000

    def _broker_epoch_to_utc_ms(value):
        try:
            raw = int(value or 0)
            if raw <= 0:
                return 0

            raw_ms = (
                raw
                if raw >= 1_000_000_000_000
                else raw * 1000
            )

            return raw_ms - broker_off_ms

        except Exception:
            return 0
    
    # Candle completion is determined by canonical UTC wall time.
    # Never advance completion using a broker-shifted tick timestamp.
    local_now_ms = int(_time.time() * 1000)
    now_ms = local_now_ms

    # Current forming-bar open, aligned to the broker timeframe grid,
    # but expressed as canonical UTC epoch.
    slot_ms = (
        ((now_ms + broker_off_ms) // tf_ms) * tf_ms
        - broker_off_ms
    )
    need = int(count or 300)

    try:
        _log(f"[mt5_fetch_rates] now_ms={now_ms} slot_ms={slot_ms} tf_ms={tf_ms}")
    except Exception:
        pass

    # --- resolve/select symbol ---
    resolved_sym = _resolve_broker_symbol(sym)
    try:
        _log(f"[mt5_resolve] base={sym} resolved={resolved_sym}")
        info = MT5.symbol_info(resolved_sym)
        if info:
            _log(
                f"[mt5_symbol] name={resolved_sym} visible={getattr(info,'visible',None)} desc={(getattr(info,'description','') or '')}"
            )
    except Exception:
        info = None  # ensure defined

    def _try_select(name: str) -> bool:
        try:
            return bool(MT5.symbol_select(name, True))
        except Exception:
            return False

    sel_ok = False
    for _ in range(3):
        if _try_select(resolved_sym):
            sel_ok = True
            break
        _time.sleep(0.25)

    if not sel_ok:
        code, msg = _mt5_last_error()
        _log(
            f"[mt5_fetch_rates] symbol_select failed for {sym} (resolved={resolved_sym}) err=({code}, '{msg}')"
        )
        if code == -10004 and _mt5_reconnect():
            for _ in range(3):
                if _try_select(resolved_sym):
                    sel_ok = True
                    break
                _time.sleep(0.25)

    if not sel_ok:
        try:
            cands = MT5.symbols_get(f"{sym}*") or []
            for s in cands:
                name = getattr(s, "name", None) or getattr(s, "symbol", None)
                if name and _try_select(name):
                    _log(f"[mt5_fetch_rates] fuzzy selected '{name}' for base '{sym}'")
                    resolved_sym = name
                    sel_ok = True
                    break
        except Exception:
            pass

    if not sel_ok:
        _log(f"[mt5_fetch_rates] giving up select for {sym} (resolved={resolved_sym})")
        return []

    
    # Freeze canonical broker-grid boundary for this fetch.
    slot0_ms = slot_ms

    # Tick is diagnostic only. It must not move the completion boundary.
    try:
        _tick2 = MT5.symbol_info_tick(resolved_sym)
        raw_tick_ms = int(getattr(_tick2, "time_msc", 0) or 0)
        canonical_tick_ms = (
            _broker_epoch_to_utc_ms(raw_tick_ms)
            if raw_tick_ms > 0
            else 0
        )

        _log(
            "[mt5_fetch_rates] "
            f"tick_raw_ms={raw_tick_ms} "
            f"tick_utc_ms={canonical_tick_ms} "
            f"server_utc_ms={now_ms} "
            f"slot0_utc_ms={slot0_ms} "
            f"broker_off_min={off_min_fresh}"
        )
    except Exception:
        pass

    # ------------------------------------------------------------
    # M1 anchor is diagnostic only.
    # It must never advance or replace the canonical slot0_ms.
    # ------------------------------------------------------------
    try:
        _log(
            f"[mt5_fetch_rates] slot0_ms(frozen)={slot0_ms} "
            f"tf_ms={tf_ms} tf={tf_label}"
        )

        _arr = MT5.copy_rates_from_pos(
            resolved_sym,
            MT5.TIMEFRAME_M1,
            1,
            1,
        )

        if _arr is None:
            _arr = []
        else:
            try:
                _arr = list(_arr)
            except Exception:
                _arr = [_arr]

        if len(_arr) >= 1:
            _rr = _arr[-1]

            _m1_open_ms = _broker_epoch_to_utc_ms(
                _f(_rr, "time", 0)
            )
            _m1_close_ms = (
                _m1_open_ms + 60_000
                if _m1_open_ms > 0
                else 0
            )

            _log(
                "[mt5_fetch_rates] M1_ANCHOR_DIAG "
                f"open_utc_ms={_m1_open_ms} "
                f"close_utc_ms={_m1_close_ms} "
                f"slot0_utc_ms={slot0_ms} "
                f"tf={tf_label}"
            )
        else:
            _log(
                "[mt5_fetch_rates] M1_ANCHOR_DIAG "
                f"skipped=no_closed_m1 slot0_utc_ms={slot0_ms} "
                f"tf={tf_label}"
            )

    except Exception as _e_anchor:
        _log(
            "[mt5_fetch_rates] M1_ANCHOR_DIAG_FAIL "
            f"slot0_utc_ms={slot0_ms} "
            f"tf={tf_label} err={_e_anchor}"
        )

    # --- MT5 readiness & TF mapping ---
    try:
        _ = MT5.TIMEFRAME_M1
    except Exception as e:
        _log(f"[mt5_fetch_rates] MT5 module not ready: {e}")
        return []

    if not callable(_map_tf):
        _log("[mt5_fetch_rates] _map_tf is not callable")
        return []

    tf_code = _map_tf(tf_label)
    if tf_code is None:
        _log(f"[mt5_fetch_rates] unsupported timeframe: {tf_label}")
        return []

    # --- helper: fetch a range ending at slot0_ms, starting back 'back_slots' bars ---
    def _fetch_range(back_slots: int):
        from_dt = datetime.utcfromtimestamp((slot0_ms - back_slots * tf_ms) // 1000)
        to_dt = datetime.utcfromtimestamp(slot0_ms // 1000)
        try:
            _log(
                f"[mt5_fetch_rates] fetch_range_utc=({int(from_dt.timestamp())},{int(to_dt.timestamp())}) back={back_slots}"
            )
        except Exception:
            pass
        try:
            arr = MT5.copy_rates_range(resolved_sym, tf_code, from_dt, to_dt)
        except Exception as e:
            code, msg = _mt5_last_error()
            _log(
                f"[mt5_fetch_rates] copy_rates_range EXC {resolved_sym}/{tf_label}: {e} last_err=({code}, '{msg}')"
            )
            if code == -10004 and _mt5_reconnect():
                try:
                    arr = MT5.copy_rates_range(resolved_sym, tf_code, from_dt, to_dt)
                except Exception as e2:
                    _log(
                        f"[mt5_fetch_rates] copy_rates_range retry EXC {resolved_sym}/{tf_label}: {e2} last_err={_mt5_last_error()}"
                    )
                    arr = None
            else:
                arr = None

        if arr is None:
            return []
        try:
            return list(arr)
        except Exception:
            return [arr]

    # --- prefer pos-based CLOSED tail first (exactly like test.py) ---
    try:
        need = int(count or 300)
    except Exception:
        need = 300

    rows = []
    try:
        tail = MT5.copy_rates_from_pos(resolved_sym, tf_code, 1, need + 64)
        if tail is None:
            tail_list = []
        else:
            try:
                tail_list = list(tail)
            except Exception:
                tail_list = [tail]

        if tail_list:
            try:
                digits = (
                    int(getattr(info, "digits", 5))
                    if ("info" in locals() and info)
                    else 5
                )
            except Exception:
                digits = 5

            for r in tail_list:
                raw_t_sec = int(_f(r, "time", 0))
                t_open_ms = _broker_epoch_to_utc_ms(raw_t_sec)
                t_close_ms = t_open_ms + tf_ms

                if (
                    t_open_ms > 0
                    and t_close_ms <= slot0_ms
                    and t_close_ms <= now_ms + 10_000
                ):
                    rows.append(
                        {
                            "t": int(t_open_ms // 1000),
                            "o": float(_f(r, "open", 0.0)),
                            "h": float(_f(r, "high", 0.0)),
                            "l": float(_f(r, "low", 0.0)),
                            "c": float(_f(r, "close", 0.0)),
                            "v": int(
                                _f(
                                    r,
                                    "tick_volume",
                                    _f(r, "real_volume", 0),
                                )
                            ),
                            "t_open_ms": int(t_open_ms),
                            "t_close_ms": int(t_close_ms),
                            "complete": True,
                        }
                    )

                elif t_close_ms > now_ms + 10_000:
                    _log(
                        "[P0_FUTURE_BAR_REJECT] "
                        f"symbol={sym} tf={tf_label} "
                        f"raw_t={raw_t_sec} "
                        f"open_utc_ms={t_open_ms} "
                        f"close_utc_ms={t_close_ms} "
                        f"server_utc_ms={now_ms} "
                        f"ahead_ms={t_close_ms - now_ms} "
                        f"broker_off_min={off_min_fresh}"
                    )
            rows = rows[-need:]  # clamp to requested count
    except Exception as e:
        _log(f"[mt5_fetch_rates] pos-tail fetch error (will fallback to range): {e}")

    if rows:
        try:
            last = rows[-1]
            _log(
                f"[mt5_fetch_rates] (pos) last CLOSED open_ms={last['t_open_ms']} close_ms={last['t_close_ms']} "
                f"OHLC={last['o']},{last['h']},{last['l']},{last['c']} complete={last['complete']}"
            )
            from datetime import datetime, timezone, timedelta

            off_min = int(off_min_fresh)
            t_sec = int(last["t"])
            t_utc = datetime.fromtimestamp(t_sec, tz=timezone.utc)
            t_broker = t_utc.astimezone(timezone(timedelta(minutes=off_min))).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            t_local = t_utc.astimezone().strftime("%Y-%m-%d %H:%M:%S")
            _log(
                f"[mt5_fetch_rates] (pos) last CLOSED: epoch={t_sec} broker_time={t_broker} (off_min={off_min}) local_time={t_local}"
            )
        except Exception:
            pass
            # --- sanity: force tail to match MT5's previous CLOSED bar ---
        try:
            _probe = MT5.copy_rates_from_pos(
                resolved_sym,
                tf_code,
                1,
                1,
            )

            if _probe is None:
                _probe = []
            else:
                try:
                    _probe = list(_probe)
                except Exception:
                    _probe = [_probe]

            if len(_probe) == 1 and rows:
                r = _probe[0]

                probe_open_ms = _broker_epoch_to_utc_ms(
                    _f(r, "time", 0)
                )
                probe_close_ms = probe_open_ms + tf_ms

                if (
                    probe_open_ms > 0
                    and probe_close_ms <= slot0_ms
                    and probe_close_ms <= now_ms + 10_000
                    and rows[-1]["t_open_ms"] != probe_open_ms
                ):
                    rows[-1].update(
                        {
                            "t": int(probe_open_ms // 1000),
                            "o": float(_f(r, "open", 0.0)),
                            "h": float(_f(r, "high", 0.0)),
                            "l": float(_f(r, "low", 0.0)),
                            "c": float(_f(r, "close", 0.0)),
                            "v": int(
                                _f(
                                    r,
                                    "tick_volume",
                                    _f(r, "real_volume", 0),
                                )
                            ),
                            "t_open_ms": int(probe_open_ms),
                            "t_close_ms": int(probe_close_ms),
                            "complete": True,
                        }
                    )

        except Exception as exc:
            _log(
                "[mt5_fetch_rates] "
                f"PARITY_PROBE_FAIL "
                f"symbol={sym} "
                f"tf={tf_label} "
                f"err={exc}"
            )

        _assert_tail_parity(
            resolved_sym,
            tf_code,
            tf_ms,
            rows,
            broker_off_ms,
        )

        return rows

    # --- main slice logic ---
    back_slots = max(need + 128, int(need * 1.25))
    for attempt in (0, 1):
        rates = _fetch_range(back_slots)
        if len(rates) == 0:
            if attempt == 0:
                back_slots = need + 512
                continue
            _log("[mt5_fetch_rates] no raw rates returned by MT5; returning []")
            return []

        opens_ms = [
            _broker_epoch_to_utc_ms(
                _f(r, "time", 0)
            )
            for r in rates
        ]
        closes_ms = [o + tf_ms for o in opens_ms]

        # --- opportunistic broker-TZ detection (LOG ONLY; no more registry writes) ---
        try:
            last_close = None
            try:
                arr2 = MT5.copy_rates_from_pos(resolved_sym, MT5.TIMEFRAME_M1, 1, 2)
            except Exception:
                arr2 = []
            # normalize and avoid NumPy truthiness ambiguity
            try:
                arr2_list = list(arr2)
            except Exception:
                arr2_list = [arr2] if arr2 is not None else []
            if len(arr2_list) >= 1:
                rr = arr2_list[-1]
                t_open_ms2 = _broker_epoch_to_utc_ms(
                    _ff(rr, "time", 0)
                )
                last_close = (
                    t_open_ms2 + 60_000
                    if t_open_ms2 > 0
                    else None
                )

            if last_close is None and closes_ms:
                last_close = closes_ms[-1]

            if last_close is not None:
                best_off, best_err = None, 1e18
                for off_min in range(-720, 901, 15):  # -12..+15h scan
                    off_try_ms = off_min * 60_000
                    slot_try = ((now_ms + off_try_ms) // tf_ms) * tf_ms - off_try_ms
                    err = abs(slot_try - last_close)
                    if err < best_err:
                        best_err, best_off = err, off_min
                        if err <= 1500:
                            break
                try:
                    _, reg_off_dbg = _broker_meta_from_registry()
                    _log(
                        f"[mt5_detect_tz/opportunistic] grid best_off={best_off} "
                        f"err_ms={int(best_err)} reg_off={reg_off_dbg} sym={resolved_sym}"
                    )
                except Exception:
                    pass
                # NOTE: we deliberately do NOT write to registry here anymore.
        except Exception:
            pass
        # --- end-index at frozen slot ---
        end_idx = None
        for i in range(len(closes_ms) - 1, -1, -1):
            if closes_ms[i] == slot0_ms:
                end_idx = i
                break
        if end_idx is None:
            for i in range(len(closes_ms) - 1, -1, -1):
                if closes_ms[i] < slot0_ms:
                    end_idx = i
                    break
        if end_idx is None:
            lc = closes_ms[-1]
            if 0 < (lc - slot0_ms) <= tf_ms:
                end_idx = len(closes_ms) - 1

        if end_idx is None:
            if attempt == 0:
                _log("[mt5_fetch_rates] no bar ≤ slot; refetching deeper")
                back_slots = need + 512
                continue
            _log("[mt5_fetch_rates] no bar ≤ slot even after refetch; returning []")
            return []

        start_idx = max(0, end_idx - need + 1)
        picked = rates[start_idx : end_idx + 1]

        if len(picked) < need and attempt == 0:
            _log(
                f"[mt5_fetch_rates] picked={len(picked)} < need={need}; refetching deeper"
            )
            back_slots = need + 512
            continue

        # --- rows ≤ slot0_ms; round to broker digits ---
        try:
            digits = (
                int(getattr(info, "digits", 5)) if ("info" in locals() and info) else 5
            )
        except Exception:
            digits = 5

        rows = []
        for r in picked:
            t_open_ms = _broker_epoch_to_utc_ms(
                _f(r, "time", 0)
            )
            t_close_ms = t_open_ms + tf_ms

            if (
                t_open_ms <= 0
                or t_close_ms > slot0_ms
                or t_close_ms > now_ms + 10_000
            ):
                continue
            rows.append(
                {
                    "t": int(t_open_ms // 1000),
                    "o": float(_f(r, "open", 0.0)),  # ✅ no round()
                    "h": float(_f(r, "high", 0.0)),
                    "l": float(_f(r, "low", 0.0)),
                    "c": float(_f(r, "close", 0.0)),
                    "v": int(_f(r, "tick_volume", _f(r, "real_volume", 0))),
                    "t_open_ms": t_open_ms,
                    "t_close_ms": t_close_ms,
                    "complete": True,
                }
            )

        rows = rows[-need:]
        if len(rows) == 0:
            if attempt == 0:
                _log("[mt5_fetch_rates] empty after index-slice; refetching deeper")
                back_slots = need + 512
                continue
            _log("[mt5_fetch_rates] still empty after deep refetch; returning []")
            return []

        # --- optional closed tail (never running) anchored to the previous slot ---
        if include_latest:
            try:
                from_dt = datetime.utcfromtimestamp(int((slot0_ms - tf_ms) // 1000))
                to_dt = datetime.utcfromtimestamp(int(slot0_ms // 1000))
                tail = MT5.copy_rates_range(resolved_sym, tf_code, from_dt, to_dt)
                if tail is None:
                    tail_list = []
                else:
                    try:
                        tail_list = list(tail)
                    except Exception:
                        tail_list = [tail]
                if len(tail_list) >= 1:
                    rr = tail_list[-1]
                    t_open_ms2 = _broker_epoch_to_utc_ms(
                        _f(rr, "time", 0)
                    )
                    t_close_ms2 = t_open_ms2 + tf_ms

                    if (
                        t_open_ms2 > 0
                        and t_close_ms2 <= slot0_ms
                        and t_close_ms2 <= now_ms + 10_000
                    ):
                        # avoid duplicate of last row
                        dup = (
                            rows
                            and rows[-1]["t_open_ms"] == t_open_ms2
                        )

                        if not dup:
                            rows.append(
                                {
                                    "t": int(t_open_ms2 // 1000),
                                    "o": float(
                                        _f(
                                            rr,
                                            "open",
                                            rows[-1]["c"] if rows else 0.0,
                                        )
                                    ),
                                    "h": float(
                                        _f(
                                            rr,
                                            "high",
                                            rows[-1]["c"] if rows else 0.0,
                                        )
                                    ),
                                    "l": float(
                                        _f(
                                            rr,
                                            "low",
                                            rows[-1]["c"] if rows else 0.0,
                                        )
                                    ),
                                    "c": float(
                                        _f(
                                            rr,
                                            "close",
                                            rows[-1]["c"] if rows else 0.0,
                                        )
                                    ),
                                    "v": int(
                                        _f(
                                            rr,
                                            "tick_volume",
                                            _f(rr, "real_volume", 0),
                                        )
                                    ),
                                    "t_open_ms": int(t_open_ms2),
                                    "t_close_ms": int(t_close_ms2),
                                    "complete": True,
                                }
                            )
                        
            except Exception:
                pass

        # --- HARD CLAMP: last bar must be a CLOSED bar not beyond slot0_ms ---
        while rows and (
            rows[-1].get("complete") is not True or rows[-1]["t_close_ms"] > slot0_ms
        ):
            rows.pop()
        rows = rows[-need:]
        if rows and rows[-1]["t_close_ms"] > slot0_ms:
            rows.pop()
        if not rows:
            _log("[mt5_fetch_rates] all rows clamped away; returning []")
            return []

        try:
            last = rows[-1]
            _log(
                f"[mt5_fetch_rates] last (closed/tail) open_ms={last['t_open_ms']} close_ms={last['t_close_ms']} "
                f"OHLC={last['o']},{last['h']},{last['l']},{last['c']} complete={last['complete']}"
            )
        except Exception:
            pass
        from datetime import datetime, timezone, timedelta

        try:
            last = rows[-1]
            off_min = int(off_min_fresh)  # e.g. 120 for RoboForex (UTC+2)
            t_sec = int(last["t"])

            t_utc = datetime.fromtimestamp(t_sec, tz=timezone.utc)
            t_broker = t_utc.astimezone(timezone(timedelta(minutes=off_min))).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            t_local = t_utc.astimezone().strftime("%Y-%m-%d %H:%M:%S")

            _log(
                f"[mt5_fetch_rates] last CLOSED:"
                f" epoch={t_sec} broker_time={t_broker} (off_min={off_min})"
                f" local_time={t_local} OHLC={last['o']},{last['h']},{last['l']},{last['c']} complete={last['complete']}"
            )
        except Exception:
            pass

        # --- sanity: force tail to match MT5's previous CLOSED bar ---
        try:
            _probe = MT5.copy_rates_from_pos(
                resolved_sym,
                tf_code,
                1,
                1,
            )

            if _probe is None:
                _probe = []
            else:
                try:
                    _probe = list(_probe)
                except Exception:
                    _probe = [_probe]

            if len(_probe) == 1 and rows:
                r = _probe[0]

                probe_open_ms = _broker_epoch_to_utc_ms(
                    _f(r, "time", 0)
                )
                probe_close_ms = probe_open_ms + tf_ms

                if (
                    probe_open_ms > 0
                    and probe_close_ms <= slot0_ms
                    and probe_close_ms <= now_ms + 10_000
                    and rows[-1]["t_open_ms"] != probe_open_ms
                ):
                    rows[-1].update(
                        {
                            "t": int(probe_open_ms // 1000),
                            "o": float(_f(r, "open", 0.0)),
                            "h": float(_f(r, "high", 0.0)),
                            "l": float(_f(r, "low", 0.0)),
                            "c": float(_f(r, "close", 0.0)),
                            "v": int(
                                _f(
                                    r,
                                    "tick_volume",
                                    _f(r, "real_volume", 0),
                                )
                            ),
                            "t_open_ms": int(probe_open_ms),
                            "t_close_ms": int(probe_close_ms),
                            "complete": True,
                        }
                    )

        except Exception as exc:
            _log(
                "[mt5_fetch_rates] "
                f"PARITY_PROBE_FAIL "
                f"symbol={sym} "
                f"tf={tf_label} "
                f"err={exc}"
            )

        _assert_tail_parity(
            resolved_sym,
            tf_code,
            tf_ms,
            rows,
            broker_off_ms,
        )

        return rows
    _log("[mt5_fetch_rates] unexpected fallthrough; returning []")
    return []
