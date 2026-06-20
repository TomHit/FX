# xtl/mt5_client.py
from __future__ import annotations
import os, sys, time, json, ctypes, subprocess
from pathlib import Path
from typing import List, Dict, Optional, Tuple

try:
    import MetaTrader5 as MT5
except Exception as e:
    MT5 = None

# ---------- logging ----------
def _ts():
    return time.strftime("%Y-%m-%d %H:%M:%S")

def _log(msg: str):
    try:
        # match your existing agent log pattern
        print(f"{_ts()} [mt5] {msg}", flush=True)
    except Exception:
        pass

# ---------- discover terminal ----------
def _read_reg(root: str, key: str, value: str) -> Optional[str]:
    # Windows-only helper
    try:
        import winreg
        hive = winreg.HKEY_LOCAL_MACHINE if root == "HKLM" else winreg.HKEY_CURRENT_USER
        with winreg.OpenKey(hive, key) as k:
            val, _ = winreg.QueryValueEx(k, value)
            return str(val)
    except Exception:
        return None

def _guess_mt5_path() -> Optional[str]:
    # 1) App-provided registry overrides (what your installer writes if you add it later)
    for r, k, v in [
        ("HKLM", r"Software\XTL", "MT5Path"),
        ("HKLM", r"Software\XauTrendLab", "MT5Path"),
    ]:
        p = _read_reg(r, k, v)
        if p and Path(p).is_file():
            return p

    # 2) Common default install locations
    candidates = [
        r"C:\Program Files\MetaTrader 5\terminal64.exe",
        r"C:\Program Files\MetaTrader 5\terminal.exe",
        r"C:\Program Files (x86)\MetaTrader 5\terminal64.exe",
        r"C:\Program Files (x86)\MetaTrader 5\terminal.exe",
    ]
    for c in candidates:
        if Path(c).is_file():
            return c

    # 3) Try wmic (may be disabled on modern Windows, but harmless)
    try:
        out = subprocess.check_output(
            ["wmic","process","where","name='terminal64.exe'","get","ExecutablePath","/value"],
            stderr=subprocess.DEVNULL, text=True, timeout=3
        )
        for line in out.splitlines():
            if line.startswith("ExecutablePath="):
                exe = line.split("=",1)[1].strip()
                if exe and Path(exe).is_file():
                    return exe
    except Exception:
        pass

    return None
# ---------- persist MT5 path to HKLM (best-effort) ----------
def _persist_mt5_path_hklm(path: str) -> None:
    """Save terminal path for future runs (ignored if no admin)."""
    try:
        import winreg
        with winreg.CreateKeyEx(
            winreg.HKEY_LOCAL_MACHINE, r"Software\XTL", 0,
            winreg.KEY_SET_VALUE | winreg.KEY_WOW64_64KEY
        ) as k:
            winreg.SetValueEx(k, "MT5.TerminalPath", 0, winreg.REG_SZ, path)
            winreg.SetValueEx(k, "MT5Path",          0, winreg.REG_SZ, path)
    except Exception:
        # Not fatal if we can't write (e.g., no admin). We’ll still use `path` this run.
        pass


# ---------- API ----------
def mt5_init() -> bool:
    """
    Initialize MetaTrader5 (portable=True so the terminal's own data/profile is used).
    This makes saved logins/history available to the service.
    """
    if MT5 is None:
        _log("ERROR: MetaTrader5 module is not available.")
        return False

    # If already initialized, treat as ok
    try:
        acc = MT5.account_info()
        if acc is not None:
            return True
    except Exception:
        pass

    path = _guess_mt5_path()
    try:
        if path:
            _persist_mt5_path_hklm(path)
            _log(f"attempting MT5.initialize(path='{path}', portable=True)")
            ok = MT5.initialize(path, portable=True)
        else:
            _log("attempting MT5.initialize(portable=True) with default path")
            ok = MT5.initialize(portable=True)
    except Exception as e:
        _log(f"ERROR: MT5.initialize raised: {e}")
        return False

    if not ok:
        _log(f"ERROR: MT5.initialize failed: {MT5.last_error()}")
        return False

    # Optional: programmatic login if no saved session
    try:
        acc = MT5.account_info()
        if acc is None:
            import os
            login    = os.environ.get("XTL_MT5_LOGIN")
            password = os.environ.get("XTL_MT5_PASSWORD")
            server   = os.environ.get("XTL_MT5_SERVER")
            if login and password and server:
                if MT5.login(login=int(login), password=password, server=server):
                    _log(f"login ok: {login}@{server}")
                else:
                    _log(f"ERROR: MT5.login failed: {MT5.last_error()}")
            else:
                _log("WARN: not logged in and no XTL_MT5_* credentials provided (relying on saved MT5 session)")
        else:
            _log(f"account: {getattr(acc,'login',None)} server={getattr(acc,'server',None)}")
    except Exception as e:
        _log(f"WARN: MT5.login check failed: {e}")

    return True


def _map_tf(name: str):
    n = (name or "").upper()
    m = {
        "M1": MT5.TIMEFRAME_M1,
        "M5": MT5.TIMEFRAME_M5,
        "M15": MT5.TIMEFRAME_M15,
        "M30": MT5.TIMEFRAME_M30,
        "H1": MT5.TIMEFRAME_H1,
        "H4": MT5.TIMEFRAME_H4,
        "D1": MT5.TIMEFRAME_D1,
    }
    return m.get(n)


def _resolve_symbol(base: str) -> str | None:
    """
    Return a broker symbol matching base (e.g., XAUUSD, XAUUSD.r), preferring visible ones.
    """
    base = (base or "").strip()
    try:
        # If exact name exists, prefer visible
        syms = MT5.symbols_get(base)
        if syms:
            # visible first, then the first hit
            syms = sorted(syms, key=lambda s: (not getattr(s, "visible", False), s.name))
            return syms[0].name

        # Wildcard search (e.g., XAUUSD.*)
        cand = MT5.symbols_get(base + "*") or MT5.symbols_get("*" + base + "*")
        if cand:
            cand = sorted(cand, key=lambda s: (not getattr(s, "visible", False), len(s.name), s.name))
            return cand[0].name
    except Exception as e:
        _log(f"WARN: symbols_get failed for '{base}': {e}")
    return None


def mt5_fetch_rates(symbol: str, tf_name: str, bars: int = 300):
    """
    Returns list of dicts with OHLCV for the last `bars` *closed* bars.
    Strategy:
      1) Try exact symbol, then a resolved alias (e.g., XAUUSD.r).
      2) Ensure symbol is selected (visible) before pulling.
      3) Primary fetch: copy_rates_from_pos(pos=1, count=bars)  -> closed bars only.
      4) Fallback fetch (if empty): copy_rates_from_pos(pos=0, count=bars+1) and drop the last (potentially open) bar.
      5) Rich logs: candidates, selection visibility, last_error on misses.
    """
    if MT5 is None:
        _log("ERROR: MetaTrader5 module not available.")
        return []

    if not mt5_init():
        return []

    tf = _map_tf(tf_name)
    if tf is None:
        _log(f"ERROR: unknown timeframe '{tf_name}'")
        return []

    # Candidate symbols: exact then resolved
    candidates = []
    s0 = (symbol or "").strip()
    if s0:
        candidates.append(s0)
    resolved = _resolve_symbol(symbol)
    if resolved and resolved not in candidates:
        candidates.append(resolved)

    _log(f"[mt5] candidates for '{symbol}': {candidates}")

    last_err = None
    for sym in candidates:
        # Ensure selectable/visible
        try:
            if not MT5.symbol_select(sym, True):
                last_err = MT5.last_error()
                _log(f"[mt5] symbol_select({sym}) failed; last_error={last_err}")
                continue
        except Exception as e:
            _log(f"[mt5] symbol_select({sym}) exception: {e}")
            continue

        try:
            info = MT5.symbol_info(sym)
            _log(f"[mt5] selected: {sym} (visible={getattr(info,'visible',None) if info else None})")
        except Exception:
            pass

        # ---- Primary fetch: closed bars only (pos=1)
        try:
            arr = MT5.copy_rates_from_pos(sym, tf, 1, max(1, int(bars)))
        except Exception as e:
            _log(f"[mt5] copy_rates_from_pos({sym},{tf_name},pos=1) exception: {e}")
            arr = None

        if arr is None or len(arr) == 0:
            last_err = MT5.last_error()
            _log(f"[mt5] no data {sym}/{tf_name} (pos=1); last_error={last_err}")

            # ---- Fallback: include current bar slice then drop last to keep closed bars
            try:
                arr2 = MT5.copy_rates_from_pos(sym, tf, 0, max(2, int(bars) + 1))
            except Exception as e:
                _log(f"[mt5] fallback copy_rates_from_pos({sym},{tf_name},pos=0) exception: {e}")
                arr2 = None

            if arr2 is not None and len(arr2) > 1:
                # Drop the last bar (may be still forming)
                arr = arr2[:-1]
                _log(f"[mt5] fallback succeeded for {sym}/{tf_name}: got {len(arr)} closed bars")

        if arr is not None and len(arr) > 0:
            # Convert numpy structured array -> plain dicts
            out = []
            names = tuple(getattr(arr, "dtype", ()).names or ())

            def _num(row, field, default=0.0):
                try:
                    if field in names:
                        return float(row[field])
                except Exception:
                    pass
                return float(default)

            for r in arr:
                out.append({
                    "t": int(r["time"]),
                    "o": float(r["open"]),
                    "h": float(r["high"]),
                    "l": float(r["low"]),
                    "c": float(r["close"]),
                    "v": _num(r, "tick_volume", _num(r, "real_volume", 0.0)),
                })

            # Success path for this symbol
            return out

    if last_err:
        _log(f"[mt5] fetch failed for {symbol}/{tf_name}; last_error={last_err}")
    return []
