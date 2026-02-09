# -*- coding: utf-8 -*-

from __future__ import annotations
import ctypes, json, os, re, shutil, subprocess, sys, time, threading, queue, signal
import typing as t

import threading


from typing import Optional, Tuple
import inspect
# --- bootstrap log (helps before APP_DIR exists) ---
from pathlib import Path
import os, time, traceback
import winreg

import logging
LOGGER = logging.getLogger("xtl.installer")
if not LOGGER.handlers:
    logging.basicConfig(level=logging.INFO)

BOOTLOG = Path(os.environ.get("TEMP", r"C:\Windows\Temp")) / "xtl_install_bootstrap.log"
def blog(msg: str) -> None:
    try:
        BOOTLOG.parent.mkdir(parents=True, exist_ok=True)
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        with open(BOOTLOG, "a", encoding="utf-8", errors="ignore") as f:
            f.write(f"[{ts}] {msg}\n")
    except Exception:
        pass  # never crash on logging
_PROCESS_START_TS = time.time()
# cadence memory for trend pushes: (symbol, tf) -> last_push_epoch_s
_LAST_PUSH: dict[tuple[str, str], int] = {}


# packaged layout: xtl/agent_ohlc.py
from xtl.agent_ohlc import push_ohlc_once as agent_push_ohlc_once
from xtl.agent_price import start_price_publisher
# ---------- constants / paths ----------
APP_NAME = "XTL"
DEFAULT_API_BASE = "https://api.xautrendlab.com"
PF = Path(os.environ.get("ProgramFiles", r"C:\Program Files"))
APP_DIR = PF / APP_NAME / "dist" / "xtl"
SERVICE_CANON = "XTLAgent"
WINSW_EXE = APP_DIR / f"{SERVICE_CANON}.exe"
WINSW_XML = APP_DIR / f"{SERVICE_CANON}.xml"
LOG_FILE = (PF / APP_NAME / "installer.log")
AGENT_LOG = (APP_DIR / "xtl_agent.log")
BIND_HINT = (APP_DIR / "bind_status.txt")
HKLM_XTL = r"SOFTWARE\XTL"
HKLM_XTL_MIRROR = r"SOFTWARE\XauTrendLab"
HKU_LS = r"S-1-5-18\Software\XTL"
# Disable any installer-side OHLC test pushes (agent will own posting)
ENABLE_INSTALLER_OHLC_TEST = False
# Program Files target: C:\Program Files\XTL\dist\xtl
try:
    _PF = os.environ.get("PROGRAMFILES", r"C:\Program Files")
except Exception:
    _PF = r"C:\Program Files"
APP_DIR = Path(_PF) / "XTL" / "dist" / "xtl"

# --- XTL convenience: delete and set across all hives we read from ---
def _xtl_del_all_roots(name: str) -> None:
    try: _hku_ls_del(name)
    except Exception: pass
    try: _reg_del(r"HKLM\Software\XTL", name)
    except Exception: pass
    try: _reg_del(r"HKCU\Software\XTL", name)
    except Exception: pass

# --- Write to all relevant hives/views (HKU LocalSystem + HKLM + HKCU, 64/32) ---
def _xtl_set_all_roots(name: str, value: str) -> None:
    import winreg as _wr
    WOW64_64 = getattr(_wr, "KEY_WOW64_64KEY", 0x0100)
    WOW64_32 = getattr(_wr, "KEY_WOW64_32KEY", 0x0200)
    targets = [
        (_wr.HKEY_USERS,          r"S-1-5-18\Software\XTL", WOW64_64),
        (_wr.HKEY_USERS,          r"S-1-5-18\Software\XTL", WOW64_32),
        (_wr.HKEY_LOCAL_MACHINE,  r"Software\XTL",          WOW64_64),
        (_wr.HKEY_LOCAL_MACHINE,  r"Software\XTL",          WOW64_32),
        (_wr.HKEY_CURRENT_USER,   r"Software\XTL",          WOW64_64),
        (_wr.HKEY_CURRENT_USER,   r"Software\XTL",          WOW64_32),
    ]
    for hive, subkey, view in targets:
        try:
            with _wr.ConnectRegistry(None, hive) as reg:
                try:
                    h = _wr.CreateKeyEx(reg, subkey, 0, _wr.KEY_SET_VALUE | view)
                except OSError:
                    h = _wr.OpenKey(reg, subkey, 0, _wr.KEY_SET_VALUE | view)
                with h:
                    _wr.SetValueEx(h, name, 0, _wr.REG_SZ, str(value))
        except Exception:
            pass

def _tz_label(off_min: int) -> str:
    sign = "+" if off_min >= 0 else "-"
    hh = abs(off_min) // 60
    mm = abs(off_min) % 60
    return f"UTC{sign}{hh:02d}:{mm:02d}"


def _reg_set(key_path: str, name: str, value, kind: str = "REG_SZ") -> None:
    """
    Create/overwrite a registry value.
    key_path like: r"HKCU\\Software\\XauTrendLab" or r"HKLM\\Software\\XTL"
    kind: "REG_SZ" | "REG_DWORD" | "REG_QWORD" | "REG_MULTI_SZ" | "REG_EXPAND_SZ" | "REG_BINARY"
    """
    root_map = {
        "HKLM": winreg.HKEY_LOCAL_MACHINE,
        "HKEY_LOCAL_MACHINE": winreg.HKEY_LOCAL_MACHINE,
        "HKCU": winreg.HKEY_CURRENT_USER,
        "HKEY_CURRENT_USER": winreg.HKEY_CURRENT_USER,
        "HKU": winreg.HKEY_USERS,
        "HKEY_USERS": winreg.HKEY_USERS,
    }
    hive_name, subkey = key_path.split("\\", 1)
    hive = root_map.get(hive_name.upper(), winreg.HKEY_LOCAL_MACHINE)

    access = (
            winreg.KEY_SET_VALUE
            | winreg.KEY_CREATE_SUB_KEY
            | getattr(winreg, "KEY_WOW64_64KEY", 0)
    )
    with winreg.CreateKeyEx(hive, subkey, 0, access) as k:
        ktype = {
            "REG_SZ": winreg.REG_SZ,
            "REG_EXPAND_SZ": winreg.REG_EXPAND_SZ,
            "REG_MULTI_SZ": winreg.REG_MULTI_SZ,
            "REG_DWORD": winreg.REG_DWORD,
            "REG_QWORD": winreg.REG_QWORD,
            "REG_BINARY": winreg.REG_BINARY,
        }[kind.upper()]

        if ktype in (winreg.REG_DWORD, winreg.REG_QWORD):
            try:
                value = int(value)
            except Exception:
                value = 0
        elif ktype == winreg.REG_MULTI_SZ:
            if not isinstance(value, (list, tuple)):
                value = [str(value)]
            value = [str(x) for x in value]
        elif ktype == winreg.REG_BINARY:
            if isinstance(value, str):
                value = value.encode("utf-8", "ignore")
        else:
            value = str(value)

        winreg.SetValueEx(k, name, 0, ktype, value)
# --- Generic delete of a registry value (supports HKCU/HKLM/HKU) ---
def _reg_del(key_path: str, name: str) -> None:
    """
    Delete REG value 'name' under root\subkey given by key_path (e.g., 'HKCU\\Software\\XTL').
    No-op if the key or value does not exist.
    """
    try:
        import winreg as w
        roots = {
            "HKCU": w.HKEY_CURRENT_USER,
            "HKLM": w.HKEY_LOCAL_MACHINE,
            "HKU":  w.HKEY_USERS,
        }
        root_token, subkey = key_path.split("\\", 1)
        root = roots.get(root_token.upper())
        if not root:
            return
        with w.ConnectRegistry(None, root) as reg:
            with w.OpenKey(reg, subkey, 0, w.KEY_SET_VALUE) as h:
                try:
                    w.DeleteValue(h, name)
                except FileNotFoundError:
                    pass
    except Exception:
        # silent no-op on missing hive/permissions/non-Windows
        pass

def _hklm_get(name: str) -> Optional[str]:
    try:
        for root in (r"Software\XTL", r"Software\XauTrendLab"):
            with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, root, 0, winreg.KEY_READ) as k:
                v, _ = winreg.QueryValueEx(k, name)
                return str(v)
    except Exception:
        return None
def _hku_ls_set(name: str, val: str) -> None:
    with winreg.CreateKeyEx(winreg.HKEY_USERS, HKU_LS, 0, winreg.KEY_SET_VALUE) as k:
        winreg.SetValueEx(k, name, 0, winreg.REG_SZ, val)
def _hku_ls_get(name: str) -> Optional[str]:
    try:
        with winreg.OpenKey(winreg.HKEY_USERS, HKU_LS, 0, winreg.KEY_READ) as k:
            v,_ = winreg.QueryValueEx(k, name)
            return str(v)
    except Exception:
        return None
def _hklm_set_json(value: dict) -> None:

    blob = json.dumps(value, separators=(",",":"))
    for root in (HKLM_XTL, HKLM_XTL_MIRROR):
        with winreg.CreateKeyEx(winreg.HKEY_LOCAL_MACHINE, root, 0, winreg.KEY_SET_VALUE) as k:
            winreg.SetValueEx(k, "ConfigJson", 0, winreg.REG_SZ, blob)
def _hklm_get_json() -> dict:
    for root in (HKLM_XTL, HKLM_XTL_MIRROR):
        try:
            with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, root, 0, winreg.KEY_READ) as k:
                v,_ = winreg.QueryValueEx(k, "ConfigJson")
                return json.loads(v)
        except Exception:
            pass
    return {}
def _hku_ls_del(name: str) -> None:
    """
    Delete a value from LocalSystem hive:
    HKU\S-1-5-18\Software\XTL\<name>
    No-op if the value or key doesn't exist.
    """
    try:
        import winreg
        with winreg.ConnectRegistry(None, winreg.HKEY_USERS) as reg:
            with winreg.OpenKey(reg, r"S-1-5-18\Software\XTL", 0, winreg.KEY_SET_VALUE) as key:
                try:
                    winreg.DeleteValue(key, name)
                except FileNotFoundError:
                    pass
    except Exception:
        # Silent no-op on missing hive/permissions/non-Windows
        pass


def _compute_local_tz_offset_min() -> int:
    # minutes EAST of UTC; Windows exposes seconds WEST
    import time
    isdst = time.localtime().tm_isdst
    seconds_west = time.altzone if (isdst and time.daylight) else time.timezone
    return -int(seconds_west // 60)

def _write_broker_meta_from_env_or_local() -> None:
    """
    Seed Broker.TzOffsetMin / Broker.TzName into all hives so the
    LocalSystem service (HKU\S-1-5-18) has a correct baseline
    even before the agent runs MT5 detection.
    Priority: env -> HKLM -> keep existing.
    """
    import os, winreg as _wr
    # 1) ENV override (optional)
    import os, time, winreg as _wr

    # 1) ENV override (optional)
    env_off = os.environ.get("FORCE_TZ_OFFSET_MIN")
    try:
        off = int(env_off) if env_off not in (None, "") else None
    except Exception:
        off = None

    # 2) HKLM fallback (machine config; either 64 or WOW6432Node)
    if off is None:
        try:
           with _wr.OpenKey(_wr.HKEY_LOCAL_MACHINE, r"Software\XTL",
                         0, _wr.KEY_READ | getattr(_wr, "KEY_WOW64_64KEY", 0x0100)) as k:
               v, _ = _wr.QueryValueEx(k, "Broker.TzOffsetMin")
               off = int(v)
        except Exception:
            try:
                with _wr.OpenKey(_wr.HKEY_LOCAL_MACHINE, r"Software\WOW6432Node\XTL",
                             0, _wr.KEY_READ | getattr(_wr, "KEY_WOW64_32KEY", 0x0200)) as k:
                    v, _ = _wr.QueryValueEx(k, "Broker.TzOffsetMin")
                    off = int(v)
            except Exception:
                off = None

    # 3) If still unknown, seed from local OS timezone (minutes EAST of UTC)
    if off is None or off < -720 or off > 900:
        try:
            import time as _t
            isdst = _t.localtime().tm_isdst
            seconds_west = _t.altzone if (isdst and _t.daylight) else _t.timezone
            off = -int(seconds_west // 60)
        except Exception:
            off = 0

    # Normalize a friendly label; allow FORCE_TZ_NAME to override
    name = os.environ.get("FORCE_TZ_NAME") or _tz_label(off)

    # --- CRITICAL: clean legacy values in *all* views before writing fresh ones
    try:
       _xtl_del_all_roots("Broker.TzOffsetMin")
       _xtl_del_all_roots("Broker.TzName")
    except Exception:
       pass

    # Write consistent values to LS (HKU\S-1-5-18), HKLM (64+32), and HKCU (64+32)
    _xtl_set_all_roots("Broker.TzOffsetMin", str(off))
    _xtl_set_all_roots("Broker.TzName", name)

    try:
        LOGGER.info("installer: seeded broker tz off_min=%s name=%s across all hives", off, name)
    except Exception:
        pass



def _get_active_console_session_id():
    WTSGetActiveConsoleSessionId = ctypes.windll.kernel32.WTSGetActiveConsoleSessionId
    WTSGetActiveConsoleSessionId.restype = ctypes.wintypes.DWORD
    return int(WTSGetActiveConsoleSessionId())

def _process_session_id(pid=None):
    pid = pid or os.getpid()
    sid = ctypes.wintypes.DWORD(0)
    ok = ctypes.windll.kernel32.ProcessIdToSessionId(ctypes.wintypes.DWORD(pid),
                                                     ctypes.byref(sid))
    return int(sid.value) if ok else -1

def _in_session0():
    try:
        return _process_session_id() == 0
    except Exception:
        return False

def _sid_string_from_token(hTok):
    """Return SID string (e.g., 'S-1-5-21-...') for a token using ctypes."""
    import ctypes
    from ctypes import wintypes as wt
    advapi32 = ctypes.WinDLL("advapi32", use_last_error=True)

    TokenUser = 1
    GetTokenInformation = advapi32.GetTokenInformation
    GetTokenInformation.restype = wt.BOOL

    # First call to get required buffer size
    needed = wt.DWORD(0)
    GetTokenInformation(hTok, TokenUser, None, 0, ctypes.byref(needed))
    buf = ctypes.create_string_buffer(needed.value)
    if not GetTokenInformation(hTok, TokenUser, buf, needed, ctypes.byref(needed)):
        return None

    # TOKEN_USER struct -> SID*
    class SID_AND_ATTRIBUTES(ctypes.Structure):
        _fields_ = [("Sid", wt.LPVOID), ("Attributes", wt.DWORD)]
    class TOKEN_USER(ctypes.Structure):
        _fields_ = [("User", SID_AND_ATTRIBUTES)]

    tu = TOKEN_USER.from_buffer_copy(buf)
    pSid = tu.User.Sid

    # Convert SID to string
    ConvertSidToStringSidW = advapi32.ConvertSidToStringSidW
    ConvertSidToStringSidW.argtypes = [wt.LPVOID, ctypes.POINTER(wt.LPWSTR)]
    ConvertSidToStringSidW.restype  = wt.BOOL

    lpStringSid = wt.LPWSTR()
    if not ConvertSidToStringSidW(pSid, ctypes.byref(lpStringSid)):
        return None
    try:
        return lpStringSid.value
    finally:
        ctypes.windll.kernel32.LocalFree(lpStringSid)

def _mirror_ls_creds_to_user_hkcu(user_sid_str):
    """
    Copy selected values from HKU\S-1-5-18\Software\XTL to HKU\<userSID>\Software\XTL
    so the user-session agent can read DeviceId/DeviceToken/ApiBase/etc.
    """
    import ctypes, os
    from ctypes import wintypes as wt

    advapi32 = ctypes.WinDLL("advapi32", use_last_error=True)

    # --- Reg types & constants (use DWORD for samDesired; wintypes has no REGSAM) ---
    HKEY_USERS = wt.HANDLE(0x80000003)  # predefined root
    KEY_READ   = 0x20019
    KEY_WRITE  = 0x20006
    REG_SZ     = 1
    DWORD      = wt.DWORD

    RegOpenKeyExW = advapi32.RegOpenKeyExW
    RegOpenKeyExW.argtypes = [wt.HANDLE, wt.LPCWSTR, DWORD, DWORD, ctypes.POINTER(wt.HANDLE)]
    RegOpenKeyExW.restype  = wt.LONG

    RegCreateKeyExW = advapi32.RegCreateKeyExW
    RegCreateKeyExW.argtypes = [wt.HANDLE, wt.LPCWSTR, DWORD, wt.LPWSTR, DWORD,
                                DWORD, wt.LPVOID, ctypes.POINTER(wt.HANDLE), ctypes.POINTER(DWORD)]
    RegCreateKeyExW.restype  = wt.LONG

    RegSetValueExW = advapi32.RegSetValueExW
    RegSetValueExW.argtypes  = [wt.HANDLE, wt.LPCWSTR, DWORD, DWORD, wt.LPCVOID, DWORD]
    RegSetValueExW.restype   = wt.LONG

    RegQueryValueExW = advapi32.RegQueryValueExW
    RegQueryValueExW.argtypes = [wt.HANDLE, wt.LPCWSTR, ctypes.POINTER(DWORD), ctypes.POINTER(DWORD),
                                 wt.LPBYTE, ctypes.POINTER(DWORD)]
    RegQueryValueExW.restype  = wt.LONG

    RegCloseKey = advapi32.RegCloseKey

    # --- Small helpers ---
    def _open(hroot, subkey, sam):
        h = wt.HANDLE()
        if RegOpenKeyExW(hroot, subkey, DWORD(0), DWORD(sam), ctypes.byref(h)) == 0:
            return h
        return None

    def _create(hroot, subkey, sam):
        h = wt.HANDLE()
        disp = DWORD(0)
        if RegCreateKeyExW(hroot, subkey, DWORD(0), None, DWORD(0), DWORD(sam), None, ctypes.byref(h), ctypes.byref(disp)) == 0:
            return h
        return None

    def _get_sz(hkey, name):
        data_type = DWORD(0)
        cb = DWORD(0)
        # Probe for size
        if RegQueryValueExW(hkey, name, None, ctypes.byref(data_type), None, ctypes.byref(cb)) != 0:
            return None
        if data_type.value != REG_SZ or cb.value == 0:
            return None
        buf = (wt.WCHAR * (cb.value // ctypes.sizeof(wt.WCHAR)))()
        if RegQueryValueExW(hkey, name, None, ctypes.byref(data_type),
                            ctypes.cast(buf, wt.LPBYTE), ctypes.byref(cb)) != 0:
            return None
        return ctypes.wstring_at(buf)

    def _set_sz(hkey, name, value):
        if value is None:
            return
        cb = (len(value) + 1) * ctypes.sizeof(wt.WCHAR)  # include terminating NUL
        RegSetValueExW(hkey, name, DWORD(0), DWORD(REG_SZ), value, DWORD(cb))

    # --- Source (LocalSystem HKCU) and destination (interactive user HKCU) ---
    src_path = r"S-1-5-18\Software\XTL"
    dst_path = rf"{user_sid_str}\Software\XTL"

    src = _open(HKEY_USERS, src_path, KEY_READ)
    if not src:
        try:
            alog("bridge(ctypes): LS hive has no Software\\XTL; nothing to mirror")
        except Exception:
            pass
        return

    dst = _create(HKEY_USERS, dst_path, KEY_WRITE)
    if not dst:
        RegCloseKey(src)
        try:
            alog(f"bridge(ctypes): cannot create HKU\\{user_sid_str}\\Software\\XTL")
        except Exception:
            pass
        return

    try:
        # Minimal set needed by the agent
        names = [
            "ApiBase",
            "DeviceId",
            "DeviceToken",
            "MT5.TerminalPath",
            "MT5Path",
            "Broker.TZ",
        ]
        copied = []
        for n in names:
            v = _get_sz(src, n)
            if v:
                _set_sz(dst, n, v)
                copied.append(n)
        try:
            alog(f"bridge(ctypes): mirrored {copied} → HKU\\{user_sid_str}\\Software\\XTL")
        except Exception:
            pass
    finally:
        RegCloseKey(dst)
        RegCloseKey(src)


def _launch_user_session_ctypes(exe_path, args="run", workdir=None, hidden=True):
    """
    pywin32-free fallback:
      - get active console session
      - WTSQueryUserToken
      - DuplicateTokenEx (PRIMARY)
      - CreateEnvironmentBlock (+ overlay PATH/PYTHONHOME/PYTHONPATH/SSL_CERT_FILE)
      - CreateProcessAsUserW
    Returns child PID or None.
    """
    import ctypes, os, time, traceback
    from ctypes import wintypes as wt

    # Load DLLs
    kernel32 = ctypes.WinDLL("kernel32",  use_last_error=True)
    advapi32 = ctypes.WinDLL("advapi32",  use_last_error=True)
    wtsapi32 = ctypes.WinDLL("wtsapi32",  use_last_error=True)
    userenv  = ctypes.WinDLL("userenv",   use_last_error=True)

    # Prototypes we will use
    WTSGetActiveConsoleSessionId = kernel32.WTSGetActiveConsoleSessionId
    WTSGetActiveConsoleSessionId.restype = wt.DWORD

    WTSQueryUserToken = wtsapi32.WTSQueryUserToken
    WTSQueryUserToken.argtypes = [wt.ULONG, ctypes.POINTER(wt.HANDLE)]
    WTSQueryUserToken.restype  = wt.BOOL

    DuplicateTokenEx = advapi32.DuplicateTokenEx
    DuplicateTokenEx.argtypes = [wt.HANDLE, wt.DWORD, wt.LPVOID, wt.DWORD, wt.DWORD, ctypes.POINTER(wt.HANDLE)]
    DuplicateTokenEx.restype  = wt.BOOL

    CreateEnvironmentBlock = userenv.CreateEnvironmentBlock
    CreateEnvironmentBlock.argtypes = [ctypes.POINTER(wt.LPVOID), wt.HANDLE, wt.BOOL]
    CreateEnvironmentBlock.restype  = wt.BOOL

    DestroyEnvironmentBlock = userenv.DestroyEnvironmentBlock
    DestroyEnvironmentBlock.argtypes = [wt.LPVOID]
    DestroyEnvironmentBlock.restype  = wt.BOOL

    # Define STARTUPINFO & PROCESS_INFORMATION (ctypes.wintypes does not provide them)
    class STARTUPINFO(ctypes.Structure):
        _fields_ = [
            ("cb",            wt.DWORD),
            ("lpReserved",    wt.LPWSTR),
            ("lpDesktop",     wt.LPWSTR),
            ("lpTitle",       wt.LPWSTR),
            ("dwX",           wt.DWORD),
            ("dwY",           wt.DWORD),
            ("dwXSize",       wt.DWORD),
            ("dwYSize",       wt.DWORD),
            ("dwXCountChars", wt.DWORD),
            ("dwYCountChars", wt.DWORD),
            ("dwFillAttribute", wt.DWORD),
            ("dwFlags",       wt.DWORD),
            ("wShowWindow",   wt.WORD),
            ("cbReserved2",   wt.WORD),
            ("lpReserved2",   ctypes.POINTER(ctypes.c_byte)),
            ("hStdInput",     wt.HANDLE),
            ("hStdOutput",    wt.HANDLE),
            ("hStdError",     wt.HANDLE),
        ]

    class PROCESS_INFORMATION(ctypes.Structure):
        _fields_ = [
            ("hProcess",   wt.HANDLE),
            ("hThread",    wt.HANDLE),
            ("dwProcessId", wt.DWORD),
            ("dwThreadId", wt.DWORD),
        ]

    advapi32.CreateProcessAsUserW.argtypes = [
        wt.HANDLE,          # hToken
        wt.LPCWSTR,         # lpApplicationName
        wt.LPWSTR,          # lpCommandLine
        wt.LPVOID,          # lpProcessAttributes
        wt.LPVOID,          # lpThreadAttributes
        wt.BOOL,            # bInheritHandles
        wt.DWORD,           # dwCreationFlags
        wt.LPVOID,          # lpEnvironment
        wt.LPCWSTR,         # lpCurrentDirectory
        ctypes.POINTER(STARTUPINFO),           # lpStartupInfo
        ctypes.POINTER(PROCESS_INFORMATION),   # lpProcessInformation
    ]
    advapi32.CreateProcessAsUserW.restype = wt.BOOL

    # Constants
    MAXIMUM_ALLOWED            = 0x02000000
    SecurityImpersonation      = 2    # SECURITY_IMPERSONATION_LEVEL
    TokenPrimary               = 1    # TOKEN_TYPE
    CREATE_UNICODE_ENVIRONMENT = 0x00000400
    CREATE_NEW_CONSOLE         = 0x00000010
    STARTF_USESHOWWINDOW       = 0x00000001
    SW_HIDE                    = 0

    # 1) Find interactive session
    sess = int(WTSGetActiveConsoleSessionId())
    if sess in (0xFFFFFFFF, -1, None):
        alog("bridge(ctypes): no active user session found")
        return None
    alog(f"bridge(ctypes): active console session id = {sess}")

    # 2) Get user token
    hUser = wt.HANDLE()
    if not WTSQueryUserToken(wt.ULONG(sess), ctypes.byref(hUser)):
        alog("bridge(ctypes): WTSQueryUserToken failed")
        return None

    # 3) Duplicate PRIMARY token
    hTok = wt.HANDLE()
    if not DuplicateTokenEx(hUser, MAXIMUM_ALLOWED, None, SecurityImpersonation, TokenPrimary, ctypes.byref(hTok)):
        kernel32.CloseHandle(hUser)
        alog("bridge(ctypes): DuplicateTokenEx failed")
        return None
    kernel32.CloseHandle(hUser)
    # Mirror LocalSystem creds to this user’s HKCU so child can read DeviceId/Token
    sid = _sid_string_from_token(hTok)
    if sid: _mirror_ls_creds_to_user_hkcu(sid)


    # 4) Environment block from user + overlay our vars
    env_block = wt.LPVOID()
    if not CreateEnvironmentBlock(ctypes.byref(env_block), hTok, False):
        kernel32.CloseHandle(hTok)
        alog("bridge(ctypes): CreateEnvironmentBlock failed")
        return None

    try:
        env = dict(os.environ)
        # Parse MULTI_SZ env_block into dict
        try:
            p = ctypes.cast(env_block, wt.LPWSTR)
            offs = 0
            while True:
                s = ctypes.wstring_at(p + offs)
                if not s:
                    break
                if "=" in s:
                    k, v = s.split("=", 1)
                    if k:
                        env[k] = v
                offs += len(s) + 1
        except Exception:
            pass

        app_dir  = os.path.dirname(exe_path) or None
        internal = os.path.join(app_dir, "_internal") if app_dir else ""
        # Build PATH safely (strings only; skip empties)
        path_parts = [
            env.get("PATH", ""),
            internal,
            os.path.join(internal, "pywin32_system32") if internal else "",
            os.path.join(internal, "win32") if internal else "",
            app_dir,
        ]
        env["PATH"] = os.pathsep.join(p for p in path_parts if p)

        # Critical: do NOT set these for a PyInstaller app
        env.pop("PYTHONHOME", None)
        env.pop("PYTHONPATH", None)

        env["XTL_HOME"] = app_dir
        # Prefer bundled CA; else fall back to ssl_cert if provided and valid
        def _good_ca(p: str, min_bytes: int = 100_000) -> bool:
            try:
               if not (p and os.path.isfile(p) and os.path.getsize(p) >= min_bytes):
                   return False
               with open(p, "r", encoding="utf-8", errors="ignore") as fh:
                   head = fh.read(256)
               return "-----BEGIN CERTIFICATE-----" in head
            except Exception:
               return False

        ca_path   = os.path.join(app_dir or "", "_internal", "certifi", "cacert.pem")
        chosen_ca = ca_path if _good_ca(ca_path) else None

        if chosen_ca:
            env["SSL_CERT_FILE"]     = chosen_ca
            env["REQUESTS_CA_BUNDLE"] = chosen_ca
            # Make the PEM visible to THIS process too so installer api_post() uses it now
            os.environ["SSL_CERT_FILE"]      = chosen_ca
            os.environ["REQUESTS_CA_BUNDLE"] = chosen_ca
            try:
                sz = os.path.getsize(chosen_ca)
                alog(f"bridge: CA bundle {chosen_ca} size={sz} bytes")
            except Exception:
                pass

        # Encode dict → MULTI_SZ (wide) for CreateProcessAsUserW
        items = [f"{k}={v}" for k,v in env.items()]
        env_w = "\x00".join(items) + "\x00\x00"
        env_ptr = ctypes.create_unicode_buffer(env_w)
    finally:
        try: DestroyEnvironmentBlock(env_block)
        except Exception: pass

    # 5) STARTUPINFO / PROCESS_INFORMATION
    si = STARTUPINFO()
    si.cb = ctypes.sizeof(STARTUPINFO)
    if hidden:
        si.dwFlags = STARTF_USESHOWWINDOW
        si.wShowWindow = SW_HIDE
    else:
        si.dwFlags = 0
        si.wShowWindow = 0

    pi = PROCESS_INFORMATION()

    # 6) Create the process
    # Build log path first (no f-string expr with backslashes)
    pd = os.environ.get('ProgramData', r'C:\ProgramData')
    log_dir = os.path.join(pd, 'XTL', 'logs')
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, 'xtl_agent.log')

    # Then build the command line
    cmd = f'cmd.exe /c ""{exe_path}" {args} >> "{log_file}" 2>&1"'

    cwd = workdir or (os.path.dirname(exe_path) or None)
    alog(f"bridge(ctypes): CreateProcessAsUserW cmd=\"{exe_path}\" {args} cwd={cwd}")

    ok = advapi32.CreateProcessAsUserW(
        hTok,
        None,
        cmd,
        None,
        None,
        False,
        CREATE_UNICODE_ENVIRONMENT | CREATE_NEW_CONSOLE,
        ctypes.cast(env_ptr, wt.LPVOID),     # LPVOID env
        cwd,
        ctypes.byref(si),
        ctypes.byref(pi),
        )

    if not ok:
        err = ctypes.get_last_error()
        alog(f"bridge(ctypes): CreateProcessAsUserW failed (err={err})")
        kernel32.CloseHandle(hTok)
        return None

    # Clean up & return PID
    # Clean up & return PID (peek exit code before closing process handle)
    try:
        kernel32.CloseHandle(pi.hThread)
        time.sleep(0.5)

        # Check if child exited immediately (helps diagnose env issues)
        exit_code = wt.DWORD()
        kernel32.GetExitCodeProcess(pi.hProcess, ctypes.byref(exit_code))
        alog(f"bridge(ctypes): launched child in session {sess} pid={int(pi.dwProcessId)}")
        if exit_code.value != 259:  # 259 == STILL_ACTIVE
            alog(f"bridge(ctypes): child exited immediately with code={exit_code.value}")

        kernel32.CloseHandle(pi.hProcess)
    except Exception as e:
        alog(f"bridge(ctypes): post-launch check warn: {e}")
    kernel32.CloseHandle(hTok)
    return int(pi.dwProcessId)


def launch_in_active_user_session(exe_path, args="run", workdir=None, hidden=True):
    """
    Run `exe_path args` in the currently logged-in user's interactive console session.
    Returns the child PID (int) or None on failure, with reasons logged.
    """
    import os, time, traceback, ctypes, ctypes.wintypes as wt
    try:
        import win32ts, win32con, win32process, win32profile, win32security, win32api
    except Exception as e:
        alog(f"bridge: pywin32 import failed: {e}")
        return  _launch_user_session_ctypes(exe_path, args=args, workdir=workdir, hidden=hidden)

    # --- Enable required privileges on the service process token ---
    try:
        hProc = win32api.GetCurrentProcess()
        TOKEN_ADJUST_PRIVILEGES = 0x20
        TOKEN_QUERY = 0x8
        hTok = win32security.OpenProcessToken(hProc, TOKEN_ADJUST_PRIVILEGES | TOKEN_QUERY)
        for priv in ("SeIncreaseQuotaPrivilege", "SeAssignPrimaryTokenPrivilege"):
            try:
                luid = win32security.LookupPrivilegeValue(None, priv)
                win32security.AdjustTokenPrivileges(hTok, False, [(luid, win32con.SE_PRIVILEGE_ENABLED)])
            except Exception as pe:
                alog(f"bridge: AdjustTokenPrivileges {priv} failed: {pe}")
        try:
            win32api.CloseHandle(hTok)
        except Exception:
            pass
    except Exception as pe:
        alog(f"bridge: privilege setup failed: {pe}")

    # 1) find active console session
    try:
        sess = ctypes.windll.kernel32.WTSGetActiveConsoleSessionId()
    except Exception as e:
        alog(f"bridge: WTSGetActiveConsoleSessionId failed: {e}")
        return None

    if sess in (0xFFFFFFFF, -1, None):
        alog("bridge: no active user session found (no one logged in?)")
        return None

    alog(f"bridge: active console session id = {int(sess)}")

    # 2) get a user token for that session
    try:
        hUser = win32ts.WTSQueryUserToken(int(sess))
    except Exception as e:
        alog(f"bridge: WTSQueryUserToken failed (session={int(sess)}): {e}")
        return None

    # 3) Duplicate to a PRIMARY token (CORRECT 5-arg signature)
    # 3) Duplicate to a PRIMARY token (robust across pywin32 builds)
    dupTok = None
    used_variant = None

    # (A) Try pywin32 5-arg signature variants first
    try:
        sa = win32security.SECURITY_ATTRIBUTES()
        dupTok = win32security.DuplicateTokenEx(
            hUser,
            win32con.MAXIMUM_ALLOWED,
            sa,
            win32security.SecurityImpersonation,
            win32security.TokenPrimary
        )
        used_variant = "pywin32:SECURITY_ATTRIBUTES()"
    except Exception as e1:
        alog(f"bridge: DuplicateTokenEx with SECURITY_ATTRIBUTES failed: {e1}")
        try:
            dupTok = win32security.DuplicateTokenEx(
                hUser,
                win32con.MAXIMUM_ALLOWED,
                0,  # some builds expect integer ptr
                win32security.SecurityImpersonation,
                win32security.TokenPrimary
            )
            used_variant = "pywin32:attr=0"
        except Exception as e2:
            alog(f"bridge: DuplicateTokenEx with attr=0 failed: {e2}")
            try:
                dupTok = win32security.DuplicateTokenEx(
                    hUser,
                    win32con.MAXIMUM_ALLOWED,
                    None,
                    win32security.SecurityImpersonation,
                    win32security.TokenPrimary
                )
                used_variant = "pywin32:attr=None"
            except Exception as e3:
                alog(f"bridge: DuplicateTokenEx with None failed: {e3}")
                dupTok = None

    # (B) If pywin32 path failed, fall back to native advapi32 via ctypes
    if dupTok is None:
        try:
            import ctypes, ctypes.wintypes as wt
            advapi = ctypes.windll.advapi32
            kernel = ctypes.windll.kernel32

            # constants
            TokenPrimary = 1
            SecurityImpersonation = 2
            MAXIMUM_ALLOWED = 0x02000000

            class SECURITY_ATTRIBUTES(ctypes.Structure):
                _fields_ = [
                    ("nLength", wt.DWORD),
                    ("lpSecurityDescriptor", wt.LPVOID),
                    ("bInheritHandle", wt.BOOL),
                ]

            # prepare args
            DesiredAccess = MAXIMUM_ALLOWED
            TokenAttributes = None  # pass NULL
            ImpersonationLevel = SecurityImpersonation
            TokenType = TokenPrimary

            # output handle
            newTok = wt.HANDLE()

            # DuplicateTokenEx(HANDLE, DWORD, LPSECURITY_ATTRIBUTES, SECURITY_IMPERSONATION_LEVEL, TOKEN_TYPE, PHANDLE)
            advapi.DuplicateTokenEx.argtypes = [
                wt.HANDLE, wt.DWORD, wt.LPVOID, ctypes.c_int, ctypes.c_int, ctypes.POINTER(wt.HANDLE)
            ]
            advapi.DuplicateTokenEx.restype = wt.BOOL

            ok = advapi.DuplicateTokenEx(
                int(hUser), DesiredAccess, None, ImpersonationLevel, TokenType, ctypes.byref(newTok)
            )
            if not ok or not newTok.value:
                raise OSError("ctypes DuplicateTokenEx failed")

            # Convert raw HANDLE -> PyHANDLE so pywin32 APIs accept it
            hProc = win32api.GetCurrentProcess()
            DUPLICATE_SAME_ACCESS = 0x2
            pyDup = win32api.DuplicateHandle(
                hProc, int(newTok.value), hProc, 0, False, DUPLICATE_SAME_ACCESS
            )

            # close the raw handle (we now own pyDup)
            kernel.CloseHandle(newTok.value)

            dupTok = pyDup
            used_variant = "ctypes DuplicateTokenEx -> DuplicateHandle(PyHANDLE)"
        except Exception as e4:
            alog(f"bridge: ctypes DuplicateTokenEx fallback failed: {e4}")
            try:
                win32api.CloseHandle(hUser)
            except Exception:
                pass
            return None

    alog(f"bridge: DuplicateTokenEx succeeded using variant: {used_variant}")


    # 4) create a writable log dir under ProgramData and build a clean env
    try:
        progdata = os.environ.get("ProgramData", r"C:\ProgramData")
        log_dir  = os.path.join(progdata, "XTL", "logs")
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, "xtl_agent.log")
    except Exception:
        log_dir  = None
        log_file = None

    # Start from the *user* environment block, but convert to a dict so we can edit
    try:
        user_env_block = win32profile.CreateEnvironmentBlock(dupTok, False)
    except Exception as e:
        alog(f"bridge: CreateEnvironmentBlock failed: {e}")
        try:
            win32api.CloseHandle(dupTok); win32api.CloseHandle(hUser)
        except Exception: pass
        return None

    # Convert the block to a dict by layering over current process env
    env = dict(os.environ)
    try:
        # On most pywin32 builds CreateEnvironmentBlock is a NUL-separated string of "KEY=VAL"
        if isinstance(user_env_block, str):
            for kv in user_env_block.split("\x00"):
                if not kv or "=" not in kv:
                    continue
                k, v = kv.split("=", 1)
                if k:
                    env[k] = v
    except Exception as e:
        alog(f"bridge: WARN could not merge user env block: {e}")

    # Ensure embedded runtime + TLS bundle are available to the child
    try:
        import certifi  # bundled in installer
        ssl_cert = certifi.where()
    except Exception:
        ssl_cert = ""

    app_dir     = os.path.dirname(exe_path) or ""
    internal = os.path.join(app_dir, "_internal") if app_dir else ""

    parts = [
        env.get("PATH", ""),
        internal,
        os.path.join(internal, "pywin32_system32") if internal else "",
        os.path.join(internal, "win32") if internal else "",
        app_dir ,
    ]

    # Keep only non-empty strings and join with Windows PATH separator
    env["PATH"] = os.pathsep.join(p for p in parts if p)


    # Critical: do NOT set these for a PyInstaller app
    env.pop("PYTHONHOME", None)
    env.pop("PYTHONPATH", None)
    # App home for the child
    env["XTL_HOME"] = app_dir

    # Prefer our bundled CA bundle; else fall back to ssl_cert (if provided)
    ca_path = os.path.join(app_dir, "_internal", "certifi", "cacert.pem")
    def _good_ca(p: str, min_bytes: int = 100_000) -> bool:
        try:
            if not (p and os.path.isfile(p) and os.path.getsize(p) >= min_bytes):
                return False
            with open(p, "r", encoding="utf-8", errors="ignore") as f:
                head = f.read(64)
            return head.startswith("-----BEGIN CERTIFICATE-----")
        except Exception:
            return False

    chosen_ca = None
    if _good_ca(ca_path):
       chosen_ca = ca_path
    elif ssl_cert and _good_ca(ssl_cert):
       chosen_ca = ssl_cert

    if chosen_ca:
        env["SSL_CERT_FILE"] = chosen_ca
        env["REQUESTS_CA_BUNDLE"] = chosen_ca
        # Make it visible to THIS process too so installer api_post() uses it now
        os.environ["SSL_CERT_FILE"] = chosen_ca
        os.environ["REQUESTS_CA_BUNDLE"] = chosen_ca
        try:
           sz = os.path.getsize(chosen_ca)
           alog(f"bridge: CA bundle {chosen_ca} size={sz} bytes")
        except Exception:
           pass
    # Optional: let the agent know where to log
    if log_file:
       env["XTL_LOG_FILE"] = log_file
    # 5) startup info and flags
    si = win32process.STARTUPINFO()
    if hidden:
        si.dwFlags |= win32con.STARTF_USESHOWWINDOW
        si.wShowWindow = 0  # SW_HIDE

    cmd = f'cmd.exe /c ""{exe_path}" {args} >> "{log_file}" 2>&1"'
    cwd = workdir or app_dir or None
    flags = (win32con.CREATE_UNICODE_ENVIRONMENT)

    alog(f"bridge: CreateProcessAsUser cmd=\"{exe_path}\" {args} cwd={cwd}")
    # 6) launch
    try:
        hp, ht, pid, tid = win32process.CreateProcessAsUser(
            dupTok, None, cmd, None, None, False, flags, env, cwd, si
        )
        alog(f"bridge: launched child in session {int(sess)} pid={pid}")

        # Close thread, then peek the process exit code before closing process handle,
        # so we can see if it died immediately (same diagnostic we added for ctypes path).
        try:
            win32api.CloseHandle(ht)
            time.sleep(0.5)

            import win32process, win32con
            STILL_ACTIVE = 259
            try:
                code = win32process.GetExitCodeProcess(hp)
            except Exception:
                code = STILL_ACTIVE

            if code != STILL_ACTIVE:
                alog(f"bridge: child exited immediately with code={code}")

            win32api.CloseHandle(hp)
        except Exception as e:
            alog(f"bridge: post-launch check warn: {e}")

        return int(pid)

    except Exception as e:
        alog(f"bridge: CreateProcessAsUser failed: {e}\n{traceback.format_exc(limit=1)}")
        return None
    finally:
        try:
            win32api.CloseHandle(dupTok); win32api.CloseHandle(hUser)
        except Exception:
            pass

def _is_pid_running(pid: int) -> bool:
    """Check if a PID is alive without psutil (Win32 API)."""
    try:
        k32 = ctypes.windll.kernel32
        PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
        handle = k32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, int(pid))
        if not handle:
            return False
        STILL_ACTIVE = 259
        exit_code = ctypes.wintypes.DWORD(0)
        ok = k32.GetExitCodeProcess(handle, ctypes.byref(exit_code))
        k32.CloseHandle(handle)
        if not ok:
            return False
        return int(exit_code.value) == STILL_ACTIVE
    except Exception:
        return False

def service_supervise_user_agent(agent_exe, args="run", restart_backoff_s=10, ping_interval_s=5):
    """
    Service-side supervisor loop: ensure a user-session agent is running.
    If it exits or we can't launch yet (no user session), retry with backoff.
    """
    import time, os

    # Prepare runtime/env once
    ensure_internal_runtime_complete()   # make sure _internal (python310.dll, base_library.zip) exists
    _ensure_cert_bundle()                # set REQUESTS_CA_BUNDLE/SSL_CERT_FILE if we bundled certs

    # --- Force the intended MT5 terminal (Program Files) on every launch ---
    try:
        mt5_exe = (
                _hku_ls_get("MT5.TerminalPath") or _hku_ls_get("MT5Path")
                or reg_get("MT5.TerminalPath")  or reg_get("MT5Path") or ""
        )
        mt5_exe = (mt5_exe or "").strip()
    except Exception:
        mt5_exe = ""

    mt5_dir = os.path.dirname(mt5_exe) if mt5_exe else str(APP_DIR)

    # Pass both CLI flag and env hint (harmless if agent ignores them)
    child_args = "run" + (f' --mt5-path="{mt5_exe}"' if mt5_exe else "")
    try:
        if mt5_exe:
            os.environ["XTL_MT5_PATH"] = mt5_exe
        else:
            os.environ.pop("XTL_MT5_PATH", None)
    except Exception:
        pass

    last_log = 0.0
    while True:
        pid = launch_in_active_user_session(
            str(APP_DIR / "xtl.exe"),
            args=child_args,
            workdir=mt5_dir,   # start inside the MT5 install folder to avoid roaming fallback
            hidden=True,
        )

        if not pid:
            # avoid log spam every second
            now = time.time()
            if now - last_log > 5:
                alog("supervisor: launch returned None (no session or token failure); retrying")
                last_log = now
            time.sleep(restart_backoff_s)
            continue

        alog(f"supervisor: supervising user-session agent pid={pid}")

        # Poll the child; if it dies, relaunch
        while True:
            time.sleep(ping_interval_s)
            if not _is_pid_running(pid):
                alog("supervisor: user-session agent exited; relaunching after backoff")
                time.sleep(restart_backoff_s)
                try:
                    ensure_internal_runtime_complete()
                except Exception as e:
                    alog(f"supervisor: runtime verify failed before relaunch: {e}")
                break

def _bind_from_registry(api_base: str) -> bool:
    """
    Robust binding flow:
      1) Read bind_token from LS hive (HKU\S-1-5-18\Software\XTL) or HKLM\ConfigJson.
      2) Ensure we have a device via /devices/pair/start (persist DeviceId/DeviceToken in LS hive).
      3) Bind via /devices/pair/bind { device_id, bind_token }.
      4) Persist creds, scrub bind_token, VERIFY with a heartbeat (server 200).
      5) On any failure: mark backoff + scrub bind_token to avoid retry storms.
    """
    import json, time
    import requests

    def _j(obj):
        try:
            return obj.json()
        except Exception:
            return None

    api = (api_base or "").strip().rstrip("/")
    if not api:
        alog("bind: missing api_base")
        return False

    # Already bound?
    dev_id  = (_hku_ls_get("DeviceId") or "").strip()
    dev_tok = (_hku_ls_get("DeviceToken") or "").strip()
    if ENABLE_INSTALLER_OHLC_TEST and dev_id and dev_tok:
        alog("bind: already bound (DeviceId/DeviceToken present)")
        return True

    # 1) Find bind_token (LS hive preferred; fallback to HKLM\ConfigJson)
    bind_token = (_hku_ls_get("BindToken") or "").strip()
    if not bind_token:
        try:
            cfg = _hklm_get_json() or {}
        except Exception:
            cfg = {}
        bind_token = (cfg.get("bind_token") or "").strip()

    if not bind_token:
        alog("bind: no bind_token found in LS hive or HKLM; will not bind")
        return False

    # 2) Ensure we have a device (pair/start) if needed
    if not (dev_id and dev_tok):
        try:
            pr = requests.post(
                f"{api}/devices/pair/start",
                headers={"Content-Type": "application/json"},
                timeout=15
            )
            alog(f"bind: /pair/start -> rc={pr.status_code} body={pr.text[:160]}")
            if not pr.ok:
                alog("bind: pair/start failed")
                raise RuntimeError(f"pair/start rc={pr.status_code}")

            data = _j(pr) or {}
            dev_id  = (data.get("device_id") or "").strip()
            dev_tok = (data.get("device_token") or "").strip()
            if not (dev_id and dev_tok):
                raise RuntimeError("pair/start missing device_id or device_token")

            _hku_ls_set("DeviceId", dev_id)
            _hku_ls_set("DeviceToken", dev_tok)
            alog(f"bind: created pending device {dev_id} via pair/start")

        except Exception as e:
            alog(f"bind: pair/start error {e}")
            # failure hygiene
            try:
                _hku_ls_set("BindTokenStatus", "backoff")
                _hku_ls_set("BindRetryAfter", str(int(time.time()) + 1800))
            except Exception:
                pass
            try: _hku_ls_del("BindToken")
            except Exception: pass
            try:
                d = (_hklm_get_json() or {})
                d["api_base"] = api
                d.pop("bind_token", None)
                _hklm_set_json(d)
            except Exception:
                pass
            return False

    # 3) Bind the device to user with the bind_token
    try:
        rb = requests.post(
            f"{api}/devices/pair/bind",
            headers={"Content-Type": "application/json"},
            json={"device_id": dev_id, "bind_token": bind_token},
            timeout=15
        )
        alog(f"bind: /pair/bind -> rc={rb.status_code} body={rb.text[:160]}")
        if not rb.ok:
            raise RuntimeError(f"/devices/pair/bind rc={rb.status_code}")

        alog(f"bind: device {dev_id} is now bound")

        # scrub bind token (success path)
        try: _hku_ls_del("BindToken")
        except Exception: pass
        try:
            d = (_hklm_get_json() or {})
            d["api_base"] = api
            d.pop("bind_token", None)
            _hklm_set_json(d)
        except Exception:
            pass
        try:
            _hku_ls_set("BindTokenStatus", "ok")
            _hku_ls_set("BindRetryAfter", "0")
        except Exception:
            pass

        # --- Persist check (re-read after writes) ---
        try:
            _dev_id_chk = (_hku_ls_get("DeviceId") or "").strip()
            _dev_tok_chk = (_hku_ls_get("DeviceToken") or "").strip()
            alog(f"bind: persisted creds dev_id={bool(_dev_id_chk)} dev_tok={bool(_dev_tok_chk)}")
        except Exception:
            _dev_id_chk, _dev_tok_chk = dev_id, dev_tok

        # 4) VERIFY on server by posting a heartbeat using the device token
        try:
            url = f"{api}/devices/{dev_id}/heartbeat"
            payload = {"label": "bind-verify", "version": "bindcheck", "uptime_s": 0, "mt5_ok": False}
            headers = {"Authorization": f"Bearer {dev_tok}", "Content-Type": "application/json"}
            rv = requests.post(url, headers=headers, data=json.dumps(payload), timeout=10)
            alog(f"bind: heartbeat verify -> rc={rv.status_code} body={rv.text[:160]}")
            if rv.status_code != 200:
                alog("bind: verification did not return 200; device may not be usable yet")
            else:
                alog("bind: VERIFIED on server (heartbeat 200).")
        except Exception as _ve:
            alog(f"bind: verify step failed: {_ve}")

        return True

    except Exception as e:
        alog(f"bind: bind call error {e}")

    # 5) Failure hygiene — avoid retry storms; allow fresh installer reprovision
    try:
        _hku_ls_set("BindTokenStatus", "backoff")
        _hku_ls_set("BindRetryAfter", str(int(time.time()) + 1800))
    except Exception:
        pass
    try: _hku_ls_del("BindToken")
    except Exception: pass
    try:
        d = (_hklm_get_json() or {})
        d["api_base"] = api
        d.pop("bind_token", None)
        _hklm_set_json(d)
    except Exception:
        pass
    return False

def api_post(api_base: str, path: str, payload: dict, token: str, timeout: int = 20):
    import requests, os
    url = api_base.rstrip('/') + '/' + path.lstrip('/')
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    # Prefer env-configured CA file (set by the launcher); else default True
    def _good_file(p: str, min_bytes=100_000) -> bool:
        try:
            return p and os.path.isfile(p) and os.path.getsize(p) >= min_bytes and \
                open(p, "r", encoding="utf-8", errors="ignore").read(64).startswith("-----BEGIN CERTIFICATE-----")
        except Exception:
            return False
    # 1) Prefer env PEM if it's a real file
    env_ca = os.environ.get("REQUESTS_CA_BUNDLE") or os.environ.get("SSL_CERT_FILE")

    if _good_file(env_ca or ""):
        verify: object = env_ca
    else:
        # 2) Try certifi.where()
        try:
            import certifi
            cpath = certifi.where()
            verify = cpath if _good_file(cpath) else True
        except Exception:
            # 3) Last fallback: system trust
            verify = True

    try:
        # small breadcrumb so we can see which verify is used
        which = verify if isinstance(verify, str) else ("system" if verify is True else str(verify))
        LOGGER.info("api_post: verify=%s", which)
    except Exception:
        pass

    try:
        r = requests.post(url, headers=headers, json=payload, timeout=timeout, verify=verify)
        tail = (token or "")[-6:]
        LOGGER.info("OHLC POST url=%s code=%s token_tail=%s bytes=%s",
                    url, getattr(r, "status_code", "?"), tail,
                    len((getattr(r, "text","") or "").encode("utf-8")))
        if getattr(r, "status_code", 0) != 200:
            LOGGER.warning("OHLC POST FAIL %s\n%s", url, (r.text or "")[:500])
        return r
    except Exception as e:
        LOGGER.warning("OHLC POST EXC %s: %s", url, e)
        class _R: status_code = 0; ok = False; text = str(e)
        return _R()

def ensure_device_attached(api_base: str, username_hint: str | None = None) -> None:
    """
    Safe attach logic:
      - If we already have DeviceId/DeviceToken, do NOT create a new device.
      - If owner token exists, try a single claim; on 404/409, set a sticky flag and stop.
      - If no creds at all, start pair *once* (sticky guard across LS/HKLM/HKCU).
    """
    try:
        import requests, json, time

        def _set_once(name: str, val: str) -> None:
            # Mirror the guard in LS + HKLM + HKCU so it survives different contexts
            try: _hku_ls_set(name, val)
            except Exception: pass
            try: _reg_set(r"HKLM\Software\XTL", name, val)
            except Exception: pass
            try: _reg_set(r"HKCU\Software\XTL", name, val)
            except Exception: pass

        def _get_once(name: str) -> str:
            v = (_hku_ls_get(name) or
                 _reg_get(r"HKLM\Software\XTL", name) or
                 _reg_get(r"HKCU\Software\XTL", name) or "")
            return (v or "").strip()

        api = api_base.rstrip("/")
        dev_id  = (_hku_ls_get("DeviceId") or "").strip()
        dev_tok = (_hku_ls_get("DeviceToken") or "").strip()

        # Case A: No device creds at all -> allow a single /pair/start
        if not (dev_id and dev_tok):
            if not _get_once("PairStartOnce"):
                try:
                    pr = requests.post(f"{api}/devices/pair/start",
                                       headers={"Content-Type": "application/json"},
                                       timeout=10)
                    if pr.ok:
                        data = pr.json() or {}
                        new_id  = (data.get("device_id") or "").strip()
                        new_tok = (data.get("device_token") or "").strip()
                        pair_code = (data.get("pair_code") or data.get("code") or "").strip()
                        if new_id and new_tok:
                            _hku_ls_set("DeviceId", new_id)
                            _hku_ls_set("DeviceToken", new_tok)
                            dev_id, dev_tok = new_id, new_tok
                            alog(f"attach: paired new device {new_id}")
                        if pair_code:
                            alog(f"PAIR CODE: {pair_code} (enter in UI to attach)")
                    else:
                        alog(f"attach: pair/start rc={pr.status_code}")
                except Exception as e:
                    alog(f"attach: pair/start error: {e}")
                _set_once("PairStartOnce", time.strftime("%Y-%m-%d %H:%M:%S"))
            return  # Nothing else to do in no-creds path

        # Case B: We DO have device creds -> never create a new device
        # Optional owner token (user/org) if the installer stored it
        owner_tok = ((_hku_ls_get("OwnerToken") or
                      _reg_get(r"HKCU\Software\XTL", "OwnerToken") or "")).strip()

        # Probe device existence/attachment using device token
        r = None
        info = {}
        try:
            r = requests.get(f"{api}/devices/{dev_id}",
                             headers={"Authorization": f"Bearer {dev_tok}"},
                             timeout=10)
            if r.ok:
                info = r.json() or {}
        except Exception:
            pass

        # If server says "not found" for this device, do NOT auto-pair again.
        # Stick a one-time marker and exit quietly (likely env/tenant mismatch or server cleanup).
        if r is not None and r.status_code == 404:
            if not _get_once("Claim404Seen"):
                alog("attach: device not found on server (404) — suppressing auto-pair; set HKLM\\Software\\XTL\\AllowRePair=1 to permit re-pair")
                _set_once("Claim404Seen", time.strftime("%Y-%m-%d %H:%M:%S"))
            return

        # If device exists, check if already attached to the expected user (when hinted)
        owner = (info.get("user") or info.get("username") or "").strip()
        if owner and (not username_hint or owner == username_hint):
            return  # already attached

        # If we have an owner token and haven't seen a claim failure, try a single claim
        if owner_tok and not _get_once("ClaimTriedOnce"):
            hdr_user = {"Authorization": f"Bearer {owner_tok}",
                        "Content-Type": "application/json"}

            # 1) /devices/{id}/claim
            try:
                resp = requests.post(
                    f"{api}/devices/{dev_id}/claim",
                    headers=hdr_user,
                    json={"device_token": dev_tok, **({"username": username_hint} if username_hint else {})},
                    timeout=10
                )
                alog(f"attach: claim (user) rc={resp.status_code}")
                if resp.ok:
                    _set_once("ClaimTriedOnce", time.strftime("%Y-%m-%d %H:%M:%S"))
                    return
                if resp.status_code in (404, 409):
                    _set_once("Claim404Seen", time.strftime("%Y-%m-%d %H:%M:%S"))
                    _set_once("ClaimTriedOnce", time.strftime("%Y-%m-%d %H:%M:%S"))
                    return
            except Exception as e:
                alog(f"attach: claim error: {e}")

            # 2) fallback: /devices/claim
            try:
                resp2 = requests.post(
                    f"{api}/devices/claim",
                    headers=hdr_user,
                    json={"device_id": dev_id, "device_token": dev_tok, **({"username": username_hint} if username_hint else {})},
                    timeout=10
                )
                alog(f"attach: claim2 (user) rc={resp2.status_code}")
                # Regardless of success, mark once to avoid loops; next beat will re-check attachment
                _set_once("ClaimTriedOnce", time.strftime("%Y-%m-%d %H:%M:%S"))
                if resp2.status_code in (404, 409):
                    _set_once("Claim404Seen", time.strftime("%Y-%m-%d %H:%M:%S"))
                return
            except Exception as e:
                alog(f"attach: claim2 error: {e}")
                _set_once("ClaimTriedOnce", time.strftime("%Y-%m-%d %H:%M:%S"))
                return

        # No owner token, or claim already attempted: do nothing further.
        # Crucially: DO NOT call /pair/start here; that causes churn.
        return

    except Exception as e:
        alog(f"attach: WARN ensure_device_attached failed: {e}")

# Read a value from HKLM\Software\XTL (and mirror) if present

# --- COMPAT: push_ohlc_once wrapper to accept legacy kwargs ---

def push_ohlc_once_compat(api_base: str, device_id: str | None = None, token: str | None = None,
                          symbols: list[str] | None = None, tfs: list[str] | None = None,
                          bars: int = 300, **kw) -> None:
    # persist creds for legacy paths…
    try:
        if token: _hku_ls_set("DeviceToken", str(token))
        if device_id: _hku_ls_set("DeviceId", str(device_id))
    except Exception:
        pass

    if agent_push_ohlc_once is None:
        alog("push_ohlc_once import missing (xtl.agent_ohlc or agent_ohlc not found) — skipping push")
        return

    # match callee signature dynamically
    import inspect
    sig = inspect.signature(agent_push_ohlc_once)
    allowed = set(sig.parameters.keys())
    call_kw = {"api_base": api_base, "symbols": symbols or [], "tfs": (tfs or kw.get("tf_names") or []), "bars": bars}
    if "device_id" in allowed and device_id: call_kw["device_id"] = device_id
    if "token" in allowed and token: call_kw["token"] = token
    for k, v in kw.items():
        if k in allowed: call_kw[k] = v

    return agent_push_ohlc_once(**call_kw)


# Compatibility shims used throughout this file
def reg_get(name: str) -> Optional[str]:
    # Prefer LocalSystem runtime hive, then installer mirrors
    v = _hku_ls_get(name)
    if v is not None and str(v).strip() != "":
        return v
    return _hklm_get(name)
def reg_set(name: str, val: str) -> None:
    # We always write to LocalSystem hive for runtime values
    _hku_ls_set(name, val)
def _persist_mt5_path_hklm(path: str) -> None:
    """Save terminal path for future runs under HKLM\Software\XTL (best-effort)."""
    try:

        with winreg.CreateKeyEx(winreg.HKEY_LOCAL_MACHINE, r"Software\XTL", 0,
                                winreg.KEY_SET_VALUE | winreg.KEY_WOW64_64KEY) as k:
            winreg.SetValueEx(k, "MT5.TerminalPath", 0, winreg.REG_SZ, path)
            winreg.SetValueEx(k, "MT5Path",          0, winreg.REG_SZ, path)
    except Exception:
        # not fatal if we lack rights; the current run can still use the discovered path
        pass
def _pick_mt5_exe_gui(initial: Optional[Path] = None) -> Optional[str]:
    try:
        import tkinter as _tk
        from tkinter import filedialog as _fd
        root = _tk.Tk()
        root.withdraw()
        start_dir = str(initial or Path("C:/Program Files"))
        path = _fd.askopenfilename(
            title="Select MetaTrader 5 terminal (terminal64.exe)",
            initialdir=start_dir,
            filetypes=[("MetaTrader 5 terminal", "terminal64.exe"),
                       ("Executables", "*.exe"), ("All files", "*.*")]
        )
        try:
            root.destroy()
        except Exception:
            pass
        if not path:
            return None
        p = Path(path)
        return str(p.resolve()) if p.is_file() and p.suffix.lower()==".exe" else None
    except Exception:
        return None
import subprocess, shlex, winreg, os
from pathlib import Path
SERVICE_CANON = "XTLAgent"




def _svc_reg_path(name: str) -> str:


    return fr"SYSTEM\CurrentControlSet\Services\{name}"

def _svc_exists(name: str = SERVICE_CANON) -> bool:
    import winreg
    try:
        with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, _svc_reg_path(name)):
            return True
    except OSError:
        return False
def _svc_start_mode_ok(name: str = SERVICE_CANON) -> bool:
    """
    Return True iff service is set to Automatic (with or without DelayedAutoStart).
    HKLM\SYSTEM\CurrentControlSet\Services\<name>\Start:
        2 = Automatic
        3 = Manual
        4 = Disabled
    DelayedAutoStart may be missing or 0/1; all are acceptable if Start==2.
    """
    try:
        import winreg
        with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, _svc_reg_path(name)) as k:
            start, _ = winreg.QueryValueEx(k, "Start")
            if int(start) != 2:
                return False
            # DelayedAutoStart is optional; both 0 and 1 are fine if Start==2
            try:
                delayed, _ = winreg.QueryValueEx(k, "DelayedAutoStart")
                _ = int(delayed)  # parse only; any value ok
            except FileNotFoundError:
                pass
        return True
    except Exception:
        return False




def _svc_image_ok(expected_exe: Path = WINSW_EXE) -> bool:
    import winreg
    try:
        with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, _svc_reg_path()) as k:
            image, _ = winreg.QueryValueEx(k, "ImagePath")
        return expected_exe.name.lower() in image.lower()
    except OSError:
        return False

def _remove_legacy_xtl_service() -> None:
    """
    If an old 'xtl' service exists, stop & delete it so only XTLAgent remains.
    """
    import subprocess
    try:
        out = subprocess.check_output(["sc", "query", "xtl"], text=True, stderr=subprocess.STDOUT)
        # stop if running; ignore failures
        subprocess.run(["sc", "stop", "xtl"], capture_output=True, text=True)
        time.sleep(1)
        subprocess.run(["sc", "delete", "xtl"], capture_output=True, text=True)
    except Exception:
        pass  # nothing to remove

def ensure_service_installed() -> None:
    _write_broker_meta_from_env_or_local()  # seed & normalize broker tz in all hives first
    ensure_winsw_binary()     # make sure it writes/renames to WINSW_EXE
    write_winsw_xml()
    _remove_legacy_xtl_service()
    import subprocess
    subprocess.run([str(WINSW_EXE), "stop"], capture_output=True, text=True)
    subprocess.run([str(WINSW_EXE), "uninstall"], capture_output=True, text=True)
    subprocess.run([str(WINSW_EXE), "install"], capture_output=True, text=True)
    subprocess.run([str(WINSW_EXE), "start"], capture_output=True, text=True)




def autostart_capability() -> bool:


    # returns True when the XTL service exists and is set to AutoStart


    try:


        import winreg


        name = "XTLAgent"


        path = fr"SYSTEM\CurrentControlSet\Services\{name}"


        with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, path) as k:


            start, _ = winreg.QueryValueEx(k, "Start")


            return int(start) == 2  # 2=Automatic (DelayedAutoStart optional)


    except Exception:


        return False





def _mt5_caps():


    try:


        c = detect_mt5_cap() or {}


        return bool(c.get("mt5")), c.get("mt5_path") or ""


    except Exception:


        return False, ""


def _persist_mt5_path_all(path: str) -> None:
    """Persist MT5 path in all places the agent might read: HKLM, HKU\S-1-5-18, and HKCU."""
    try:
        # Machine-wide (requires admin during install)
        _reg_set(r"HKLM\Software\XTL", "MT5Path", path)
        _reg_set(r"HKLM\Software\XTL", "MT5.TerminalPath", path)
    except Exception:
        pass
    try:
        # LocalSystem hive (this is what the service actually uses)
        _hku_ls_set("MT5Path", path)
        _hku_ls_set("MT5.TerminalPath", path)
    except Exception:
        pass
    try:
        # Interactive user (helpful for manual runs)
        _reg_set(r"HKCU\Software\XTL", "MT5Path", path)
        _reg_set(r"HKCU\Software\XTL", "MT5.TerminalPath", path)
    except Exception:
        pass
    try:
        # Environment fallback for the current process tree
        os.environ["XTL_MT5_PATH"] = path
    except Exception:
        pass

def _write_atomic_bytes(dst: Path, data: bytes) -> None:
    tmp = dst.with_suffix(dst.suffix + ".tmp")
    dst.parent.mkdir(parents=True, exist_ok=True)
    with open(tmp, "wb") as f:
        f.write(data); f.flush(); os.fsync(f.fileno())
    tmp.replace(dst)

def _has_pem_headers(p: Path) -> bool:
    try:
        if not p.is_file() or p.stat().st_size < 1000: return False
        with p.open("r", encoding="utf-8", errors="ignore") as fh:
            head = fh.read(4096)
        return "-----BEGIN CERTIFICATE-----" in head
    except Exception:
        return False

# ---- TLS CA bootstrap (certifi) ----
def _ensure_cert_bundle() -> None:
    """Point requests/ssl to the bundled cert without blocking."""
    try:
        import certifi  # local import so missing certifi doesn't break startup
    except Exception:
        certifi = None
    try:
        import shutil  # ensure available if you copy the bundle
        base = Path(getattr(sys, "_MEIPASS", Path(sys.executable).parent))
        # Prefer the library bundle if available
        target = Path(certifi.where()) if certifi else None
        if not (target and target.exists()):
            # If already inside ...\_internal\, don't append another _internal
            cert_dir = (base / "certifi") if base.name.lower() == "_internal" else (base / "_internal" / "certifi")
            cert_dir.mkdir(parents=True, exist_ok=True)
            if certifi:
                src = Path(certifi.where())
                if src.exists():
                    dst = cert_dir / "cacert.pem"
                    if not dst.exists():
                        shutil.copyfile(src, dst)
                    target = dst
            if not (target and target.exists()):
                for p in (cert_dir / "cacert.pem", base / "certifi" / "cacert.pem", base / "_internal" / "certifi" / "cacert.pem"):
                    if p.exists():
                        target = p
                        break
        if target and Path(target).exists():
            os.environ["REQUESTS_CA_BUNDLE"] = str(target)
            os.environ["SSL_CERT_FILE"] = str(target)
            alog(f"cert bundle set -> {target}")  # use logger, no blocking
        else:
            alog("WARN: no CA bundle found; HTTPS may fail")
    except Exception as e:
        alog(f"cert bundle setup skipped: {e}")

# ---- end TLS CA bootstrap ----

# ---------- silent subprocess ----------
CREATE_NO_WINDOW = 0x08000000
STARTF_USESHOWWINDOW = 0x00000001
SW_HIDE = 0
def _run_hidden(cmd: list[str], timeout: Optional[int]=None) -> Tuple[int,str,str]:
    si = subprocess.STARTUPINFO(); si.dwFlags |= STARTF_USESHOWWINDOW; si.wShowWindow = SW_HIDE
    p = subprocess.run(cmd, startupinfo=si, creationflags=CREATE_NO_WINDOW,
                       capture_output=True, text=True, timeout=timeout)
    return p.returncode, p.stdout or "", p.stderr or ""



# ---- Vendor lookup (put near the top with other constants) ----




VENDOR_DIR = Path(sys.argv[0]).resolve().parent / "vendor"



def _reg_get_dword(key_path: str, name: str, default: int = 0) -> int:
    """
    Read a DWORD value from the Windows Registry.
    key_path examples:
      - r"HKLM\SOFTWARE\Microsoft\VisualStudio\14.0\VC\Runtimes\x64"
    Returns `default` if the key/value is missing or not a DWORD.
    """
    root_map = {
        "HKLM": winreg.HKEY_LOCAL_MACHINE,
        "HKEY_LOCAL_MACHINE": winreg.HKEY_LOCAL_MACHINE,
        "HKCU": winreg.HKEY_CURRENT_USER,
        "HKEY_CURRENT_USER": winreg.HKEY_CURRENT_USER,
        "HKU": winreg.HKEY_USERS,
        "HKEY_USERS": winreg.HKEY_USERS,
    }
    try:
        hive_name, subkey = key_path.split("\\", 1)
        hive = root_map.get(hive_name.upper(), winreg.HKEY_LOCAL_MACHINE)
        # Force 64-bit view so we detect 64-bit VC++ runtime on 64-bit Windows
        access = winreg.KEY_READ | getattr(winreg, "KEY_WOW64_64KEY", 0)
        with winreg.OpenKey(hive, subkey, 0, access) as k:
            val, typ = winreg.QueryValueEx(k, name)
            if isinstance(val, int):
                return val
            # Some installers store as string; attempt parse
            try:
                return int(val)
            except Exception:
                return default
    except FileNotFoundError:
        return default
    except OSError:
        return default
def _dlls_present() -> bool:
    """
    Detects if the VC++ 2015–2022 x64 runtime DLLs are present.
    We check common DLLs in System32 (and SysWOW64 just in case).
    Returns True if the core pair(s) are found.
    """
    try:
        win = os.environ.get("SystemRoot", r"C:\Windows")
        sys32 = Path(win) / "System32"
        wow64 = Path(win) / "SysWOW64"   # checked opportunistically

        # Core DLLs for the 2015–2022 runtime (VS 2015-2022 VC14)
        # vcruntime140_1.dll was added later; some systems may miss it.
        candidates = {
            "vcruntime140.dll",
            "vcruntime140_1.dll",
            "msvcp140.dll",
            "msvcp140_1.dll",
            "concrt140.dll",
        }

        def _exists(p: Path) -> bool:
            try:
                return p.is_file()
            except Exception:
                return False

        present = set()
        for name in candidates:
            if _exists(sys32 / name) or _exists(wow64 / name):
                present.add(name)

        # Heuristic: having vcruntime140.dll AND msvcp140.dll is sufficient.
        # If both are present, we consider the runtime installed.
        has_core = ("vcruntime140.dll" in present) and ("msvcp140.dll" in present)

        # Some newer apps need vcruntime140_1.dll; if it's present, even better.
        # But don't fail just because it's missing on older boxes.
        return has_core
    except Exception:
        return False

# ---- VC++ (x64) silent install if missing ----


def _ensure_vcredist_runtime() -> None:
    """
    Ensure VC++ 2015–2022 x64 runtime is present. Never blocks install.
    Relies on: _dlls_present(), _reg_get_dword(), _run_hidden(), log(),
               and constants Path, sys, VENDOR_DIR, APP_DIR.
    """
    try:
        def _reg_present() -> bool:
            try:
                # HKLM\SOFTWARE\Microsoft\VisualStudio\14.0\VC\Runtimes\x64 with Installed=1
                return _reg_get_dword(
                    r"HKLM\SOFTWARE\Microsoft\VisualStudio\14.0\VC\Runtimes\x64",
                    "Installed",
                    default=0,
                ) == 1
            except Exception:
                return False

        # Already present? exit quickly
        if _dlls_present() or _reg_present():
            return

        # Locate a bundled installer
        candidates = [
            Path(sys.argv[0]).resolve().parent / "VC_redist.x64.exe",
            VENDOR_DIR / "VC_redist.x64.exe",
            APP_DIR / "VC_redist.x64.exe",
            ]
        exe = next((p for p in candidates if p and p.exists()), None)

        if not exe:
            log("vcredist: WARN missing VC_redist.x64.exe (runtime not found, continuing anyway)")
            return

        # Run quietly, don't block forever
        # Acceptable return codes:
        #   0    = success
        #   1638 = another version already installed
        #   3010 = success, reboot required
        rc, out, err = _run_hidden([str(exe), "/install", "/quiet", "/norestart"], timeout=240)
        log(f"vcredist: rc={rc} out={out.strip()} err={err.strip()}")

        if rc not in (0, 1638, 3010):
            log(f"vcredist: WARN installer returned rc={rc}")

        # Re-check presence after attempt
        if not (_dlls_present() or _reg_present()):
            log("vcredist: WARN VC++ 2015-2022 x64 runtime still not detected after install attempt")

        # Do not raise - allow the rest of the install to proceed

    except Exception as e:
        # Never block the install on vcredist issues
        log(f"vcredist: WARN exception {e}")



# ---- _internal population / repair ----


def ensure_internal_runtime_complete() -> None:
    """
    Ensure the embedded Python runtime is fully present under APP_DIR/_internal.

    Steps:
      1) Create APP_DIR/_internal if missing.
      2) Merge a ready-made _internal from common staging locations (beside xtl.exe, CWD, dist/xtl/_internal).
      3) If pieces are still missing, pull python310.dll/python3.dll/base_library.zip from beside xtl.exe if present.
      4) If base_library.zip remains missing or too small, try to fetch Python 3.10 embeddable package and
         repurpose python310.zip -> base_library.zip.
      5) As a last resort, synthesize python3.dll from python310.dll (some loaders look for both names).
      6) Hard-fail if critical files are still missing/corrupt.
    """
    import shutil, zipfile, io, urllib.request
    from pathlib import Path

    def _log(msg: str) -> None:
        try:
            LOGGER.info(msg)
        except Exception:
            try:
                blog(msg)
            except Exception:
                pass

    here = Path(sys.argv[0]).resolve().parent
    app_internal = APP_DIR / "_internal"
    app_internal.mkdir(parents=True, exist_ok=True)

    # --- helpers -------------------------------------------------------------
    def _ok_file(p: Path, min_bytes: int = 1) -> bool:
        try:
            return p.is_file() and p.stat().st_size >= min_bytes
        except Exception:
            return False

    def _try_copy(src: Path, dst: Path) -> bool:
        try:
            if src.is_file():
                dst.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(src, dst)
                _log(f"runtime: copied {src} -> {dst}")
                return True
        except Exception as e:
            _log(f"runtime: WARN copy {src} -> {dst}: {e}")
        return False

    # --- 1) Stage from common _internal folders ------------------------------
    candidates_internal = [
        here / "_internal",
        Path.cwd() / "_internal",
        here / "dist" / "xtl" / "_internal",
        ]
    for src_internal in candidates_internal:
        if src_internal.is_dir():
            try:
                shutil.copytree(src_internal, app_internal, dirs_exist_ok=True)
                _log(f"runtime: merged staged _internal from {src_internal}")
            except Exception as e:
                _log(f"runtime: WARN copytree from {src_internal} failed: {e}")

    # --- 2) Ensure critical files exist (and are sane) ----------------------
    py310 = app_internal / "python310.dll"
    py3   = app_internal / "python3.dll"
    blz   = app_internal / "base_library.zip"

    if not _ok_file(py310):
        _try_copy(here / "python310.dll", py310)

    if not _ok_file(py3):
        _try_copy(here / "python3.dll", py3)

    # Try local base_library.zip copies first (require >1MB to avoid partials)
    if not _ok_file(blz, min_bytes=1_000_000):
        for src_blz in (
                here / "base_library.zip",
                here / "_internal" / "base_library.zip",
                Path.cwd() / "_internal" / "base_library.zip",
                here / "dist" / "xtl" / "_internal" / "base_library.zip",
                VENDOR_DIR / "base_library.zip",
                VENDOR_DIR / "_internal" / "base_library.zip",
        ):
            if _ok_file(src_blz, min_bytes=1_000_000) and _try_copy(src_blz, blz):
                break

    # --- 3) Network fallback: use python310.zip from official embeddable ----
    # Only if still missing/too small
    if not _ok_file(blz, min_bytes=1_000_000):
        try:
            # Pick a stable 3.10 release (adjust if you standardize on another)
            py_embed_urls = [
                # Primary (3.10.11 embeddable, amd64)
                "https://www.python.org/ftp/python/3.10.11/python-3.10.11-embed-amd64.zip",
                # Mirror/fallbacks could be added here if needed
            ]
            for url in py_embed_urls:
                try:
                    _log(f"runtime: fetching embeddable Python from {url}")
                    with urllib.request.urlopen(url, timeout=20) as resp:
                        data = resp.read()
                    with zipfile.ZipFile(io.BytesIO(data)) as zf:
                        # The embeddable package contains 'python310.zip'
                        name_candidates = [n for n in zf.namelist() if n.lower().endswith("python310.zip")]
                        if name_candidates:
                            with zf.open(name_candidates[0], "r") as src:
                                payload = src.read()
                            # Write as base_library.zip
                            blz.parent.mkdir(parents=True, exist_ok=True)
                            with open(blz, "wb") as f:
                                f.write(payload)
                            _log(f"runtime: created {blz} from embeddable python310.zip (size={len(payload)} bytes)")
                            break
                except Exception as e_url:
                    _log(f"runtime: WARN fetch/extract failed from {url}: {e_url}")
        except Exception as e_net:
            _log(f"runtime: WARN network fallback skipped/failed: {e_net}")

    # --- 4) Last resort DLL symmetry ----------------------------------------
    if _ok_file(py310) and not _ok_file(py3):
        _try_copy(py310, py3)
    if _ok_file(py3) and not _ok_file(py310):
        _try_copy(py3, py310)

    # --- 5) Ensure a valid CA bundle on disk ----------------------------------

    try:
       import certifi, shutil
       dst_cacert = app_internal / "certifi" / "cacert.pem"
       # Prefer the certifi bundle; if missing or too small, rewrite it
       def _size(p)-> int:
           try: return p.stat().st_size
           except Exception: return 0

       src_path = Path(getattr(certifi, "where", lambda: "")() or "")
       ok = False
       if src_path.is_file() and _size(src_path) >= 100_000:

           dst_cacert.parent.mkdir(parents=True, exist_ok=True)
           shutil.copy2(src_path, dst_cacert)
           LOGGER.info(f"cert bundle set -> {dst_cacert} (size={_size(dst_cacert)} bytes)")
           # Make accidental truncation less likely on locked-down hosts
           try:
               os.chmod(dst_cacert, 0o444)  # read-only
           except Exception:
               pass

           ok = True
       else:
           # Last-ditch: if a staged bundle exists in the package, use it
           staged = here / "_internal" / "certifi" / "cacert.pem"
           if staged.is_file() and _size(staged) >= 100_000:
              dst_cacert.parent.mkdir(parents=True, exist_ok=True)
              shutil.copy2(staged, dst_cacert)
              LOGGER.info(f"cert bundle staged -> {dst_cacert} (size={_size(dst_cacert)} bytes)")
              ok = True
       if not ok:
           LOGGER.warning("cert bundle unavailable; HTTPS will fall back to system trust")
    except Exception as e:
           LOGGER.warning(f"runtime: WARN ensuring CA bundle failed: {e}")
           # Harden: if base_library.zip is OK, make it read-only to avoid accidental truncation
           try:
               if _ok_file(blz, min_bytes=1_000_000): os.chmod(blz, 0o444)
           except Exception:
               pass

    # --- 6) Final verification ----------------------------------------------
    missing = []
    if not (_ok_file(py310) or _ok_file(py3)):
        missing.append("python310.dll|python3.dll")
    # We still enforce >1MB to avoid partial zips; embeddable python310.zip is ~9–10MB
    if not _ok_file(blz, min_bytes=1_000_000):
        try:
            size = blz.stat().st_size if blz.exists() else 0
        except Exception:
            size = 0
        missing.append(f"base_library.zip(>1MB, found={size} bytes)")

    if missing:
        raise RuntimeError("Runtime incomplete after repair: missing/corrupt " + ", ".join(missing))
    # Enforce a valid CA bundle (prevent silent TLS failures later)
    dst_cacert = app_internal / "certifi" / "cacert.pem"

    try:
        ca_sz = dst_cacert.stat().st_size if dst_cacert.exists() else 0
    except Exception:

       ca_sz = 0
    if ca_sz < 100_000:
           raise RuntimeError(f"CA bundle missing/corrupt: {dst_cacert} (size={ca_sz} bytes)")



# ---------- logging ----------
def _write_line(p: Path, msg: str) -> None:
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        with p.open("a", encoding="utf-8") as f:
            f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} {msg}\n")
    except Exception:
        pass
def log(msg: str) -> None: _write_line(LOG_FILE, msg)

def alog(msg: str) -> None: _write_line(AGENT_LOG, msg)
# ---------- admin ----------
def require_admin() -> None:
    try:
        if ctypes.windll.shell32.IsUserAnAdmin() == 0:
            raise PermissionError("Installer must be run as Administrator")
    except Exception:
        pass
# ---------- registry (HKLM / HKU LS) ----------
import winreg
# --- Bind TTL helpers (60 min) ---
def _bind_ttl_secs() -> int:
    # Hard TTL: 60 minutes
    return 60 * 60
def _now_s() -> int:
    try:
        return int(time.time())
    except Exception:
        return int(time.time())
def _bind_mark_seen_now():

    try:
        if (reg_get("BindIssuedAt") or "").strip():
            return
        reg_set("BindIssuedAt", str(_now_s()))
    except Exception:
        pass
def _bind_remaining_secs() -> int:
    try:
        issued = int(reg_get("BindIssuedAt") or "0")
    except Exception:
        issued = 0
    if issued <= 0:
        # If we never marked, start TTL now.
        _bind_mark_seen_now()
        issued = _now_s()
    ttl = _bind_ttl_secs()
    rem = (issued + ttl) - _now_s()
    return rem if rem > 0 else 0
def _bind_mark_seen_now_cfg():
    cfg = _hklm_get_json() or {}
    if not cfg.get("bind_issued_at"):
        cfg["bind_issued_at"] = _now_s()
        _hklm_set_json(cfg)
def _bind_remaining_secs_cfg() -> int:
    cfg = _hklm_get_json() or {}
    issued = int(cfg.get("bind_issued_at") or 0)
    if issued <= 0:
        _bind_mark_seen_now_cfg()
        issued = _now_s()
    rem = (issued + _bind_ttl_secs()) - _now_s()
    return rem if rem > 0 else 0
def _bind_clear_token_terminal_expired():
    cfg = _hklm_get_json() or {}
    cfg["bind_token"] = ""
    cfg["bind_issued_at"] = 0
    _hklm_set_json(cfg)
    _hku_ls_set("BindTokenStatus", "terminal_expired")
    _hku_ls_set("BindTokenLastError", "ttl_expired")
# one-time auth log guard
_auth_logged = False
def _auth_headers(extra: dict | None = None) -> dict:


    h = {"User-Agent": "xtl-agent/1.0"}


    dev_id = (_hku_ls_get("DeviceId") or "").strip()


    dev_tok = (_hku_ls_get("DeviceToken") or "").strip()


    if dev_tok:


        h["Authorization"]   = f"Bearer {dev_tok}"


        h["X-Device-Token"]  = dev_tok


    if dev_id:


        h["X-Device-Id"]     = dev_id


    if extra:


        h.update(extra)


    return h

# ---------- http session (requests or urllib) ----------
def _rq_session():
    try:
        import requests, certifi  # noqa
        s = requests.Session(); s.verify = certifi.where(); return s
    except Exception:
        import urllib.request, urllib.error
        class _R:
            def post(self, url, json=None, timeout=20):
                data = json and __import__("json").dumps(json).encode("utf-8") or b""
                req = urllib.request.Request(url, data=data, headers={"Content-Type":"application/json"}, method="POST")
                try:
                    with urllib.request.urlopen(req, timeout=timeout) as r:
                        return type("Resp", (), {
                            "status_code": r.getcode(),
                            "text": r.read().decode("utf-8","ignore"),
                            "ok": 200 <= r.getcode() < 300,
                            "json": lambda self: __import__("json").loads(self.text or "{}")
                        })()
                except urllib.error.HTTPError as e:
                    txt = e.read().decode("utf-8","ignore")
                    return type("Resp", (), {
                        "status_code": e.code,
                        "text": txt,
                        "ok": False,
                        "json": lambda self: __import__("json").loads(txt or "{}")
                    })()
        return _R()
def _join(base: str, path: str) -> str:
    return base.rstrip("/") + "/" + path.lstrip("/")
# ---- MT5 terminal auto-detect (registry, common paths, running process) ----
def find_mt5_terminal() -> Optional[str]:
    """
    Try to locate a MetaTrader 5 terminal executable:
    1) Registry keys under HKCU/HKLM (MetaQuotes)
    2) Common install folders (Program Files, Program Files (x86))
    3) Running process query via WMIC (best-effort)
    Returns a filesystem path or None.
    """
    import winreg, os
    candidates = []
    # 1) Registry - user and machine
    reg_keys = [
        (winreg.HKEY_CURRENT_USER,  r"Software\MetaQuotes\MetaTrader 5"),
        (winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\MetaQuotes\MetaTrader 5"),
        (winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\WOW6432Node\MetaQuotes\MetaTrader 5"),
    ]
    names = ["InstallPath", "TerminalPath", "Path"]
    for root, sub in reg_keys:
        try:
            with winreg.OpenKey(root, sub, 0, winreg.KEY_READ) as k:
                for nm in names:
                    try:
                        v, _ = winreg.QueryValueEx(k, nm)
                        if v and os.path.isfile(v): candidates.append(v)
                        elif v and os.path.isdir(v):
                            exe = os.path.join(v, "terminal64.exe")
                            if os.path.isfile(exe): candidates.append(exe)
                    except Exception:
                        pass
        except Exception:
            pass
    # 2) Common folders
    pf  = Path(os.environ.get("ProgramFiles", r"C:\Program Files"))
    pf86= Path(os.environ.get("ProgramFiles(x86)", r"C:\Program Files (x86)"))
    for base in (pf, pf86):
        for sub in (
            "MetaTrader 5", "MetaTrader5", "MetaQuotes\MetaTrader 5", "MetaQuotes\Terminal",
        ):
            p1 = (base / sub / "terminal64.exe")
            p2 = (base / sub / "terminal.exe")
            if p1.is_file(): candidates.append(p1.as_posix())
            if p2.is_file(): candidates.append(p2.as_posix())
    # 3) Running process via WMIC (best-effort; may be disabled on new Windows)
    try:
        rc, out, err = _run_hidden(
            ["wmic","process","where","name='terminal64.exe'","get","ExecutablePath","/value"],
            timeout=3
       )
        for line in (out or "").splitlines():
            if line.strip().startswith("ExecutablePath="):
                exe = line.split("=",1)[1].strip().strip('"')
                if exe and os.path.isfile(exe): candidates.append(exe)
    except Exception:
        pass
    # Prefer 64-bit terminal if multiple
    for c in candidates:
        if c.lower().endswith("terminal64.exe"):
            return c
    return candidates[0] if candidates else None
# ---- Capability probe: is MT5 usable on this machine? ----
def detect_mt5_cap() -> dict:
    """
    Returns a small capability dict for heartbeats and logs:
      { "mt5": "ok"|"missing", "mt5_path": "<path-or-empty>", "mt5_module": true|false }
    - ok      => MetaTrader5 module import works AND we can detect a terminal path
    - missing => either the module is missing or no terminal path is found
    """
    mt5_module = False
    try:
        import MetaTrader5  # type: ignore
        mt5_module = True
    except Exception:
        pass
    mt5_path = find_mt5_terminal() or ""
    status = "ok" if (mt5_module and mt5_path) else "missing"
    return {"mt5": status, "mt5_path": mt5_path, "mt5_module": bool(mt5_module)}
# ---- UI prompt helpers (safe, optional) ----
def _is_interactive_session() -> bool:

    """
    Best-effort: show UI only if we're running in a user session, not as a service.
    We also allow forcing silent via env XTL_NO_UI=1.
    """
    if os.environ.get("XTL_NO_UI") == "1":
        return False
    try:
        import ctypes
        user32 = ctypes.windll.user32
        # If there's a foreground window, assume UI is OK
        return bool(user32.GetForegroundWindow())
    except Exception:
        return False
def _msgbox_yesno(title: str, text: str) -> bool:
    """
    Shows a topmost Yes/No information dialog. Returns True if 'Yes'.
    Never throws (fails closed to False).
    """
    try:
        import ctypes
        MB_ICONINFORMATION = 0x40
        MB_YESNO          = 0x04
        MB_TOPMOST        = 0x40000
        IDYES             = 6
        rc = ctypes.windll.user32.MessageBoxW(
            None, text, title, MB_ICONINFORMATION | MB_YESNO | MB_TOPMOST
        )
        return rc == IDYES
    except Exception:
        return False
def _open_url(url: str) -> None:
    try:
        os.startfile(url)  # best on Windows; falls back below if blocked
    except Exception:
        try:
            subprocess.Popen(["cmd", "/c", "start", "", url], close_fds=True)
        except Exception:
            pass
# ---------- utils ----------
def _vcpp_ok() -> bool:
   sys32 = Path(os.environ.get("WINDIR", r"C:\Windows")) / "System32"
   return (sys32/"vcruntime140.dll").is_file() and (sys32/"vcruntime140_1.dll").is_file()
def _copytree(src: Path, dst: Path) -> None:
    """Idempotent copy: create dirs if missing; overwrite files; never delete existing content."""
    dst.mkdir(parents=True, exist_ok=True)
    for root, dirs, files in os.walk(src):
        rel = Path(root).relative_to(src)
        (dst / rel).mkdir(parents=True, exist_ok=True)
        for f in files:
            s = Path(root) / f
            d = dst / rel / f
            try:
                shutil.copy2(s, d)
            except Exception:
                # best-effort; continue on individual file errors
                pass
def _sc_query(name: str) -> str:
    rc, out, err = _run_hidden(["sc","query",name])
    return (out+err)
def _sc_delete(name: str) -> None:
    _run_hidden(["sc","stop",name]); _run_hidden(["sc","delete",name])





def deploy_files(src_exe: Path) -> None:
    """
    Copy the app payload into APP_DIR:
      - xtl.exe
      - sibling WinSW binaries/xml if present
      - the ENTIRE _internal/ runtime tree (python310.dll, python3.dll, base_library.zip, certs, etc.)
      - ensure a valid certifi\cacert.pem (skip overwrite if already good; refresh otherwise)
    """
    import shutil, os
    from pathlib import Path

    def _same_file(a: Path, b: Path) -> bool:
        try:
            sa, sb = a.stat(), b.stat()
            return (sa.st_size == sb.st_size) and (int(sa.st_mtime) == int(sb.st_mtime))
        except Exception:
            return False

    here = src_exe.parent
    APP_DIR.mkdir(parents=True, exist_ok=True)

    # 1) xtl.exe
    dst_exe = APP_DIR / "xtl.exe"
    try:
        if (not dst_exe.exists()) or (not _same_file(src_exe, dst_exe)):
            blog(f"deploy_files: copying xtl.exe -> {dst_exe}")
            shutil.copy2(src_exe, dst_exe)
    except Exception as e:
        blog(f"deploy_files: copy xtl.exe ERROR {e}")
        raise

    # 2) _internal runtime (prefer sibling _internal; fall back to common staging locations)
    candidates = [
        here / "_internal",
        Path.cwd() / "_internal",
        here / "dist" / "xtl" / "_internal",
        ]
    src_internal = next((p for p in candidates if p.exists()), None)
    blog(f"deploy_files: src_internal={src_internal}")
    if src_internal:
        try:
            shutil.copytree(src_internal, APP_DIR / "_internal", dirs_exist_ok=True)
        except Exception as e:
            blog(f"deploy_files: copy _internal WARN {e}")

    # 2b) CA bundle guard/refresh (prevents zero-byte cacert.pem)
    dst_cacert = APP_DIR / "_internal" / "certifi" / "cacert.pem"
    try:
        current_len = dst_cacert.stat().st_size if dst_cacert.exists() else 0
    except Exception:
        current_len = 0

    if current_len >= 100_000:
        # Keep a known-good bundle; do NOT overwrite
        blog(f"deploy_files: keeping existing CA bundle (size={current_len} bytes)")
    else:
        # Try certifi.where() first; fall back to staged source under src_internal
        src_ca = None
        try:
            import certifi  # type: ignore
            from pathlib import Path as _P
            cpath = getattr(certifi, "where", lambda: "")() or ""
            if cpath:
                p = _P(cpath)
                if p.is_file() and p.stat().st_size >= 100_000:
                    src_ca = p
        except Exception:
            src_ca = None

        if not src_ca and src_internal:
            staged = src_internal / "certifi" / "cacert.pem"
            try:
                if staged.is_file() and staged.stat().st_size >= 100_000:
                    src_ca = staged
            except Exception:
                pass

        if src_ca:
            try:
                dst_cacert.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(src_ca, dst_cacert)
                sz = dst_cacert.stat().st_size if dst_cacert.exists() else 0
                blog(f"deploy_files: refreshed CA bundle -> {dst_cacert} (size={sz} bytes)")
                # Make accidental truncation less likely
                try:
                    os.chmod(dst_cacert, 0o444)
                except Exception:
                    pass
            except Exception as e:
                blog(f"deploy_files: WARN failed to copy CA bundle: {e}")
        else:
            blog("deploy_files: WARN no valid CA bundle source found; HTTPS will use system trust")

    # 3) Optional sidecars that may sit next to the exe (copy if present)
    sidecars = [
        "XTLAgent.exe", "XTLAgent.xml", "XTLAgent.wrapper",
        "winsw.exe", "WinSW-x64.exe",
        "Run-XTL.bat", "install.cmd",
        "README.txt", "xtl.cfg",
    ]
    for name in sidecars:
        s = here / name
        if not s.exists():
            continue
        d = APP_DIR / name
        try:
            if (not d.exists()) or (not _same_file(s, d)):
                shutil.copy2(s, d)
        except Exception as e:
            blog(f"deploy_files: copy {name} WARN {e}")


def preflight() -> None:
    require_admin()
    py310 = APP_DIR / "_internal" / "python310.dll"
    py3   = APP_DIR / "_internal" / "python3.dll"
    if not (py310.is_file() and py3.is_file()):
        raise RuntimeError("Runtime incomplete after repair: _internal/python310.dll and python3.dll required")
    sys32 = Path(os.environ.get("WINDIR", r"C:\Windows")) / "System32"
    if not ((sys32/"vcruntime140.dll").is_file() and (sys32/"vcruntime140_1.dll").is_file()):
        raise RuntimeError("VC++ 2015-2022 x64 missing after install attempt")
def upsert_config(api_base: str, bind_token: Optional[str]) -> dict:
    d = _hklm_get_json() or {}
    d["api_base"] = api_base or DEFAULT_API_BASE
    d.setdefault("device_id",""); d.setdefault("device_token","")
    d["bind_token"] = (bind_token or d.get("bind_token") or "")
    _hklm_set_json(d)
    return d
def ensure_winsw_binary() -> None:
    r"""
    Ensure WinSW is available at WINSW_EXE by preferring *bundled* copies.
    We DO NOT download here. If nothing is found, fail fast with a clear log.
    Search order:
      1) next to xtl.exe (dist\xtl\)
      2) dist\xtl\_internal\  (PyInstaller collected data)
      3) dist\_internal\      (some packers unzip one level up)
      4) APP_DIR\ and APP_DIR\vendor\
      5) VENDOR_DIR\
      6) PyInstaller temp dir sys._MEIPASS (runtime)
    """
    here = Path(sys.argv[0]).resolve().parent
    candidates: list[Path] = []
    # 1) next to the executable
    candidates += [
        here / "WinSW-x64.exe",
        here / "winsw.exe",
        here / "XTLAgent.exe",  # some builds rename the binary already
    ]
    # 2) typical PyInstaller collected-data folder beside exe
    for base in [here / "_internal", here.parent / "_internal"]:
        candidates += [
            base / "WinSW-x64.exe",
            base / "winsw.exe",
            base / "vendor" / "WinSW-x64.exe",
            base / "winsw" / "WinSW-x64.exe",
        ]
    # 3) APP_DIR and vendor under it
    candidates += [
        APP_DIR / "WinSW-x64.exe",
        APP_DIR / "winsw.exe",
        APP_DIR / "XTLAgent.exe",
        APP_DIR / "vendor" / "WinSW-x64.exe",
    ]
    # 4) VENDOR_DIR explicitly (if your build defines it)
    candidates += [
        VENDOR_DIR / "WinSW-x64.exe",
        VENDOR_DIR / "winsw.exe",
    ]
    # 5) PyInstaller runtime unpack dir
    meipass = getattr(sys, "_MEIPASS", None)
    if meipass:
        mp = Path(meipass)
        candidates += [
            mp / "WinSW-x64.exe",
            mp / "winsw.exe",
            mp / "vendor" / "WinSW-x64.exe",
        ]
    # pick the first existing path
    src = next((p for p in candidates if p.exists()), None)
    if not src:
        log("winsw: no bundled WinSW found; searched these locations:")
        for p in candidates:
            log(f"  - {p}")
        raise RuntimeError(
            "WinSW binary not bundled. Place WinSW-x64.exe (or winsw.exe) "
            "next to xtl.exe or under _internal/ or vendor/."
        )
    # make sure destination exists and copy only if the content differs
    WINSW_EXE.parent.mkdir(parents=True, exist_ok=True)
    try:
        if (not WINSW_EXE.exists()) or (WINSW_EXE.read_bytes() != src.read_bytes()):
            shutil.copy2(src, WINSW_EXE)
        log(f"winsw: using bundled {src.name} -> {WINSW_EXE}")
    except PermissionError as e:
        # service may still be holding a handle from a previous run
        log(f"winsw: destination in use; keeping existing binary. ({e})")
def write_winsw_xml(service_name: str | None = None) -> None:
    """
    Emit WinSW XML. 'service_name' is optional and ignored unless provided;
    we always pin to SERVICE_CANON to avoid name flip-flops.
    """
    name = SERVICE_CANON  # hard-pin
    xml = f"""<service>
  <id>{name}</id>
  <name>{name}</name>
  <description>XauTrendLab Agent</description>

  <executable>xtl.exe</executable>
  <arguments>service</arguments>
  <workingdirectory>{APP_DIR.as_posix()}</workingdirectory>

  <startmode>Automatic</startmode>
  <delayedAutoStart>true</delayedAutoStart>

  <log mode="roll-by-size">
    <sizeThreshold>1048576</sizeThreshold>
    <keepFiles>5</keepFiles>
  </log>

  <onfailure action="restart" delay="5 sec"/>
  <onfailure action="restart" delay="10 sec"/>
  <onfailure action="restart" delay="20 sec"/>

  <stoptimeout>20 sec</stoptimeout>
</service>
"""
    WINSW_XML.write_text(xml, encoding="utf-8")
def _choose_service_id():
    import re
    did = (_hku_ls_get("DeviceId") or "").strip()
    suf = re.sub(r'[^A-Za-z0-9]', '', did)[-6:] or "DEV"
    sid = f"XTLAgent_{suf}"
    return sid, sid

def install_service_idempotent() -> str:
    """
    Install the agent service via WinSW. Try canonical, then alt, and if both
    appear blocked (delete-pending) pick a unique name. Keep XML writing in
    write_winsw_xml(candidate).
    """
    ensure_winsw_binary()  # make sure WinSW exe is present

    def _try(candidate: str) -> bool:
        # Best-effort cleanup of a stale entry.
        try:
            _sc_delete(candidate)
            time.sleep(0.8)
        except Exception:
            pass

        # If currently marked for delete (1072), SKIP this name.
        out = _sc_query(candidate)
        if "1072" in out:
            log(f"{candidate} marked for deletion (1072) — skipping")
            return False

        # Write XML for this candidate and attempt install.
        write_winsw_xml(candidate)
        rc, stdout, stderr = _run_hidden([str(WINSW_EXE), "install"], timeout=20)
        if rc != 0:
            log(f"WinSW install rc={rc} out={stdout.strip()} err={stderr.strip()}")
            return False

        # Ensure auto start.
        _ensure_service_auto_start(candidate)
        _run_hidden(["sc", "config", candidate, "start=", "auto"])
        return True

    # 1) Try canonical, then alt
    if _try(SERVICE_CANON):
        return SERVICE_CANON

    # 2) Fallback to a unique, device-suffixed id (no XML in cmd_install; still via write_winsw_xml)
    uniq, _ = _choose_service_id()
    if _try(uniq):
        return uniq

    # 3) Still blocked -> surface clear guidance
    raise RuntimeError("Could not install service (names unavailable). If an uninstall is pending, reboot and rerun.")

def start_service_and_wait(name: str, timeout_s: int=30) -> None:
    _run_hidden(["sc", "start", name])
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        out = _sc_query(name)
        if re.search(r"STATE\s+:\s+4\s+RUNNING", out): return
        time.sleep(1.0)
    wrap = APP_DIR / f"{name}.wrapper.log"
    if not wrap.exists():
        wrap = APP_DIR / "XTLAgent.wrapper.log"
        if not wrap.exists(): wrap = APP_DIR / "wrapper.log"
    if wrap.exists():
        try: log("Wrapper tail:\n" + "\n".join(wrap.read_text(errors="ignore").splitlines()[-100:]))
        except Exception: pass
    raise RuntimeError("Service did not reach RUNNING in time. If wrapper shows 'Failed to start embedded python', install VC++ and ensure _internal is complete.")
def _ensure_service_auto_start(name: str) -> None:
    try:
        _run_hidden(["sc", "config", name, "start=", "auto"])
    except Exception as e:
        log(f"ensure service auto-start failed: {e}")

def _configure_service_recovery(service_name: str = "XTLAgent") -> None:
    """Make SCM restart our service after failures and use delayed auto-start."""
    try:
        import subprocess
        # 3 restarts with 5s delay; reset failure count after 60s
        subprocess.run(["sc", "failure", service_name,
                        "reset=", "60",
                        "actions=", "restart/5000/restart/5000/restart/5000"],
                       check=False, capture_output=True)
        # delayed auto-start helps race conditions during boot
        subprocess.run(["sc", "config", service_name, "start=", "delayed-auto"],
                       check=False, capture_output=True)
        alog("service: applied recovery + delayed-auto configuration")
    except Exception as e:
        alog(f"service: recovery config warn: {e}")

def write_bind_hint(msg: str) -> None:
    try: BIND_HINT.write_text(msg+"\n", encoding="utf-8")
    except Exception: pass

def pair_start(api_base: str) -> Tuple[str,str]:
    s = _rq_session()
    r = s.post(_join(api_base, "/devices/pair/start"), json={})
    if getattr(r,"status_code",0) != 200:
        raise RuntimeError(f"pair/start http {getattr(r,'status_code',0)} {getattr(r,'text','')[:160]}")
    j = r.json() if hasattr(r,"json") else {}
    dev = (j.get("device_id") or j.get("id") or "").strip()
    tok = (j.get("device_token") or j.get("token") or "").strip()
    if not (dev and tok): raise RuntimeError("pair/start returned malformed payload")
    return dev, tok
def verify_post_start() -> None:
    api_ok = _hku_ls_get("ApiBase") is not None
    dev_ok = bool(_hku_ls_get("DeviceId") and _hku_ls_get("DeviceToken"))
    bts_ok = _hku_ls_get("BindTokenStatus") is not None
    if not api_ok or not (dev_ok or bts_ok):
        raise RuntimeError("agent failed before init - check runtime/VC++/workingdirectory")
# -------------------- agent (service/run) - minimal but functional --------------------
_stop = threading.Event()
def _graceful(signum=None, frame=None):
    _stop.set()
for _sig in (getattr(signal, "SIGTERM", None), getattr(signal, "SIGINT", None), getattr(signal, "SIGBREAK", None)):
    if _sig:
        try: signal.signal(_sig, _graceful)
        except Exception: pass
def _auto_bind_if_needed(api_base: str) -> None:
    """
    If a bind_token exists in HKLM config and the device is not yet bound in the
    LocalSystem hive, attempt to bind. On success, persist DeviceId/DeviceToken
    so subsequent heartbeats authenticate and the worker runs normally.
    This is safe to call on every start; it no-ops when already bound or token absent.
    """
    try:
        # Already bound? (device creds present in LocalSystem hive)
        if (_hku_ls_get("DeviceId") or "") and (_hku_ls_get("DeviceToken") or ""):
            return
        cfg = _hklm_get_json() or {}
        bind_tok = (cfg.get("bind_token") or "").strip()
        if not bind_tok:
            # Nothing to do; installer may have paired already.
            return
        # Track token use (your existing TTL/seen helpers)
        try:
            _bind_mark_seen_now_cfg()
        except Exception:
            pass
        try:
            if _bind_remaining_secs_cfg() <= 0:
                alog("auto-bind: token TTL expired; clearing token")
                try:
                    _bind_clear_token_terminal_expired()
                except Exception:
                    pass
                write_bind_hint("Auto-bind token expired (TTL). Please regenerate a new token.")
                return
        except Exception:
            # If TTL helpers misbehave, proceed anyway (best-effort bind).
            pass
        # Make the bind call
        s = _rq_session()
        r = s.post(_join(api_base, "/devices/pair/bind"), json={"bind_token": bind_tok})
        if getattr(r, "ok", False):
            # Persist returned credentials (accept common field variants)
            dev_id = ""
            dev_tok = ""
            try:
                j = r.json() if hasattr(r, "json") else {}
                dev_id = (j.get("device_id") or j.get("id") or "").strip()
                dev_tok = (j.get("device_token") or j.get("token") or "").strip()
            except Exception as e:
                alog(f"auto-bind: response parse error: {e!s}")
            if ENABLE_INSTALLER_OHLC_TEST and dev_id and dev_tok:
                try:
                    _hku_ls_set("ApiBase", api_base)
                except Exception:
                    pass
                _hku_ls_set("DeviceId", dev_id)
                _hku_ls_set("DeviceToken", dev_tok)
                _hku_ls_set("BindTokenStatus", "success")
                write_bind_hint("Auto-bind success: device credentials persisted.")
                alog("auto-bind: success")
                return
            else:
                # Successful HTTP but missing creds - back off and retry later.
                _hku_ls_set("BindTokenStatus", "backoff")
                write_bind_hint("Auto-bind response missing credentials; will retry.")
                alog("auto-bind: success HTTP but no credentials; will retry")
                return
        # Non-2xx HTTP - mark backoff (agent loop can retry)
        _hku_ls_set("BindTokenStatus", "backoff")
        sc = getattr(r, "status_code", 0)
        write_bind_hint(f"Auto-bind failed ({sc}); agent will retry.")
        alog(f"auto-bind: HTTP {sc}")
    except Exception as e:
        # Network or unexpected errors - mark backoff, keep running
        try:
            _hku_ls_set("BindTokenStatus", "backoff")
        except Exception:
            pass
        write_bind_hint(f"Auto-bind exception: {e!s}")
        alog(f"auto-bind: exception {e!r}")
def push_ohlc_once_legacy(api_base: str, symbols: list[str], tf_names: list[str], bars: int = 301) -> None:
    """
    Pull the most recent `bars` candles from MT5 per symbol/timeframe and POST each bar.
    - Assumes MetaTrader5 has been initialized already by caller.
    - `tf_names` are strings like ["M15", "H1", "H4"].
    """
    try:
        import MetaTrader5 as mt5  # type: ignore
    except Exception:
        alog("push_ohlc_once: MT5 module not available")
        return
    # map textual TF names to MT5 constants
    tf_map = {
        "M1": mt5.TIMEFRAME_M1,
        "M5": mt5.TIMEFRAME_M5,
        "M15": mt5.TIMEFRAME_M15,
        "M30": mt5.TIMEFRAME_M30,
        "H1": mt5.TIMEFRAME_H1,
        "H4": mt5.TIMEFRAME_H4,
        "D1": mt5.TIMEFRAME_D1,
    }
    tfs = [(name, tf_map.get(name)) for name in tf_names if tf_map.get(name)]
    if not tfs:
        tfs = [("M15", mt5.TIMEFRAME_M15)]  # safe default
    s = _rq_session()
    for sym in symbols:
        for name, tf in tfs:
            try:
                # copy last N bars (MT5 returns numpy-structured array; index 0..bars-1)
                rates = mt5.copy_rates_from_pos(sym, tf, 0, bars)
                if not rates:
                   alog(f"ohlc batch {sym}/{name} -> no data")
                   continue
                last_code = None
                # Send newest ? oldest (or invert if your API expects ascending)
                for r in reversed(rates):
                    bar = {
                        "symbol": sym,
                        "tf": name,
                        "t": int(r["time"]),      # epoch seconds (UTC per MT5)
                        "o": float(r["open"]),
                        "h": float(r["high"]),
                        "l": float(r["low"]),
                        "c": float(r["close"]),
                        "v": int(r["tick_volume"] if "tick_volume" in r.dtype.names else r["real_volume"] if "real_volume" in r.dtype.names else 0),
                    }
                    resp = s.post(_join(api_base, "/ohlc/ingest"), json=bar)
                    last_code = getattr(resp, "status_code", None)
                    # optional: throttle a tiny bit to avoid flooding
                alog(f"ohlc batch {sym}/{name} -> last={last_code} count={len(rates)}")
            except Exception as e:
               alog(f"ohlc batch err {sym}/{name}: {e!s}")
import json, time, os


import urllib.request
AGENT_VERSION = os.environ.get("XTL_AGENT_VERSION", "1.0.2")
def _reg_get(hive_path: str, name: str) -> str:


    # minimal helper - replace with your existing reg_get


    try:


        import winreg


        hive, subkey = hive_path.split("\\", 1)


        hive_obj = {"HKU": winreg.HKEY_USERS, "HKLM": winreg.HKEY_LOCAL_MACHINE}[hive]


        with winreg.OpenKey(hive_obj, subkey) as k:


            val, _ = winreg.QueryValueEx(k, name)


            return str(val)


    except Exception:


        return ""



# --- One-shot OHLC pusher that the HB loop calls --------------------------------
def agent_push_ohlc_once(api_base: str, symbols: list[str], tfs: list[str], bars: int = 300) -> bool:
    """
    Resolves DeviceId/Token from the LocalSystem hive and calls the real agent
    pusher (xtl.agent_ohlc.push_ohlc_once). Adds loud logging around the call.
    Returns True if a POST was attempted, False if skipped.
    """
    dev_id = (_hku_ls_get("DeviceId") or "").strip()
    tok    = (_hku_ls_get("DeviceToken") or "").strip()
    if not dev_id or not tok:
        alog("OHLC: no DeviceId/DeviceToken in HKU\\S-1-5-18\\Software\\XTL — skip push_once")
        return False

    # lazy import with alias to avoid packaging issues
    try:
        from xtl.agent_ohlc import push_ohlc_once as _push_once_core
    except Exception as e:
        alog(f"OHLC: cannot import xtl.agent_ohlc.push_ohlc_once: {e!s}")
        return False

    # log plan
    alog(f"OHLC: POST plan -> /devices/{dev_id}/ohlc symbols={symbols} tfs={tfs} bars={bars}")

    # attempt the real push (new signature)
    try:
        _push_once_core(
            api_base=api_base,
            device_id=dev_id,
            token=tok,
            symbols=symbols,
            tfs=tfs,
            bars=bars,
            force=True,  # ensure at least one send even if dedup would skip
        )
    except TypeError:
        # fallback to positional signature if local agent is older
        try:
            _push_once_core(api_base, dev_id, tok, symbols, tfs, bars)
        except Exception as e2:
            alog(f"OHLC: legacy push_once failed: {e2!s}")
            return False
    except Exception as e:
        alog(f"OHLC: push_once error: {e!s}")
        return False

    # --- FORCE at least one POST per TF so we can see it server-side ---
    if ENABLE_INSTALLER_OHLC_TEST:
        try:
            for sym in (symbols or []):
                 for tf in (tfs or []):
                     payload_min = {
                         "symbol": sym,
                         "timeframe": tf,
                         "bars": [],   # empty is OK; route should still log and hydrate keys
                         "count": 0,
                         "written_at": int(time.time() * 1000),
                     }
                     alog(f"OHLC: CALL api_post smoke dev={dev_id} sym={sym} tf={tf}")
                     r = api_post(api_base, f"/devices/{dev_id}/ohlc", payload_min, tok, timeout=20)
                     status = getattr(r, "status_code", 0)
                     ok = bool(getattr(r, "ok", False))
                     body = (getattr(r, "text", "") or "")[:200]
                     alog(f"OHLC: RESULT dev={dev_id} sym={sym} tf={tf} status={status} ok={ok} body={body}")
        except Exception as e:
            alog(f"OHLC: smoke post exception: {e!s}")
    # --- END FORCE ---

    alog("OHLC: push_once attempt completed (see agent_ohlc logs for batch details)")
    return True


def load_device_creds() -> tuple[str, str]:


    # Prefer LocalSystem hive where the service runs


    dev_id = _reg_get(r"HKU\S-1-5-18\Software\XTL", "DeviceId") or ""


    token  = _reg_get(r"HKU\S-1-5-18\Software\XTL", "DeviceToken") or ""


    return dev_id.strip(), token.strip()


def post_device_heartbeat(api_base: str,
                          status: str = "running",
                          mt5_ok: bool = True,
                          api_ok: bool = True,
                          autostart_ok: bool = True,
                          version: str = AGENT_VERSION,
                          last_error: str | None = None) -> tuple[int, str]:
    """
    POST /devices/<dev_id>/heartbeat with Authorization: Bearer <DeviceToken>.
    Sends a schema-safe superset of fields to satisfy stricter validators.
    """
    dev_id, token = load_device_creds()
    if not dev_id or not token:
        return 401, "missing device credentials (DeviceId/DeviceToken)"

    # Minimal + safe extras
    try:
        import platform
        label = (_hku_ls_get("DeviceLabel") or platform.node() or "").strip()[:64]
    except Exception:
        label = ""
    uptime_s = max(0, int(time.time() - _PROCESS_START_TS))
    caps = detect_mt5_cap() or {}
    mt5_path = caps.get("mt5_path") or ""
    mt5_ok = bool(mt5_ok)

    body = {
        "status": str(status or "online"),
        "version": str(version or ""),
        "label": label,
        "uptime_s": int(uptime_s),
        "mt5_ok": bool(mt5_ok),
        "mt5_path": mt5_path,
        "api_ok": bool(api_ok),
        "autostart_ok": bool(autostart_ok),
        # Optional: include 'platform' & 'tz' that backends often accept
        "platform": "windows",
        "tz": (time.tzname[0] if time.tzname else ""),
    }
    if last_error:
        body["last_error"] = str(last_error)

    url = f"{api_base.rstrip('/')}/devices/{dev_id}/heartbeat"
    try:
        import requests
        r = requests.post(url, json=body, headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        }, timeout=20)
        # Return code + text (so 422 shows precise validation error)
        return int(getattr(r, "status_code", 599)), (getattr(r, "text", "") or "")
    except Exception as e:
        return 599, f"{type(e).__name__}: {e}"


def _heartbeat_loop(api_base: str, interval_sec: int = 60) -> None:
    s = _rq_session()

    def _creds() -> tuple[str, str]:
        return (_hku_ls_get("DeviceId") or "", _hku_ls_get("DeviceToken") or "")

    # --- NEW: local cadence & last-push scheduler ---
    poll_every = max(15, min(interval_sec or 60, 600))  # start from hb arg, clamp 15s..10m
    next_ohlc_at = 0.0  # force an early push after first HB parse
    last_symbols = ["XAUUSD"]
    last_tfs = ["M15","H1","H4"]

    made_online = False
    attempts_with_online_flag = 0  # send a few explicit "online" edges early

    # helper: should we push now for (sym, tf) given cadence?
    def _due(sym: str, tfu: str, every_s: int, now_s: int) -> bool:
        key = (sym.upper(), tfu.upper())
        last = _LAST_PUSH.get(key, 0)
        return (now_s - last) >= max(15, int(every_s))

    # helper: mark we just pushed
    def _mark_pushed(sym: str, tfu: str, now_s: int) -> None:
        _LAST_PUSH[(sym.upper(), tfu.upper())] = now_s

    while not _stop.is_set():
        try:
            device_id, device_token = _creds()
            now_s = int(time.time())  # seconds

            # base payload for hb
            payload: dict[str, t.Any] = {
                "ts": now_s,
                "device_id": device_id,
                "device_token": device_token,
                "pid": os.getpid(),
                "version": "1.0.2",
            }

            # caps + flags
            try:
                caps = detect_mt5_cap() or {}
            except Exception:
                caps = {}
            payload["caps"] = {"mt5": caps.get("mt5"), "mt5_path": caps.get("mt5_path")}
            payload["mt5_ok"]       = bool(payload["caps"].get("mt5"))
            payload["mt5_path"]     = payload["caps"].get("mt5_path") or ""
            payload["autostart_ok"] = autostart_capability()
            payload["api_ok"]       = True
            payload["platform"]     = "windows"
            payload["tz"]           = (time.tzname[0] if time.tzname else "")
            payload["uptime_s"]     = max(0, int(time.time() - _PROCESS_START_TS))

            # headers (bearer optional; route is tolerant)
            headers = _auth_headers()
            if device_token:
                headers["X-Device-Token"] = device_token
            if device_id:
                headers["X-Device-Id"] = device_id

            # early online hint a few times
            send_online_hint = (not made_online) and (attempts_with_online_flag < 3)
            if send_online_hint:
                payload["status"] = "running"
                payload["online"] = True
                payload["state"]  = "running"
                attempts_with_online_flag += 1

            # --- canonical heartbeat ---
            mt5_ok_flag = bool(payload.get("mt5_ok", False))
            code, resp = post_device_heartbeat(
                api_base,
                status="running",
                mt5_ok=mt5_ok_flag,
                api_ok=bool(payload.get("api_ok", True)),
                autostart_ok=bool(payload.get("autostart_ok", True)),
                version=str(payload.get("version", "")),
            )

            # auto-attach if needed
            if code and 200 <= code < 300:
                try:
                    ensure_device_attached(api_base)
                except Exception as e:
                    alog(f"attach: ensure_device_attached error: {e}")

            dev_for_log = device_id or (_hku_ls_get("DeviceId") or "")
            alog(f"heartbeat /devices/{dev_for_log or '<id>'}/heartbeat {code} {(resp or '')[:160]}")
            if code and 200 <= code < 300 and (
                    ('"status":"running"' in (resp or "")) or
                    ('"online":true'    in (resp or "")) or
                    ('"ok":true'        in (resp or "")) ):
                made_online = True
            if code in (401, 403):
                _hku_ls_set("BindTokenStatus", "backoff")

            # --- parse trend hints and act ---
            push_now = False
            trend_active = False
            symbols: list[str] = []
            tfs: list[str] = []
            cadence = 60

            try:
                js = json.loads(resp or "{}")
                tr = (js or {}).get("trend") or {}
                push_now = bool(tr.get("push_now", False))
                trend_active = bool(tr.get("active", False))
                symbols = [str(x).strip() for x in (tr.get("symbols") or []) if str(x).strip()]
                tfs = [str(x).strip().upper() for x in (tr.get("tfs") or []) if str(x).strip()]
                cadence = int(tr.get("interval_sec") or 60)
            except Exception:
                pass

            # default symbols/tfs if server is silent
            if not symbols:
                symbols = ["XAUUSD"]
            if not tfs:
                tfs = ["M15", "H1", "H4"]

            # One-shot push on nudge
            if push_now:
                alog(f"HB: trend push_now -> pushing OHLC once symbols={symbols} tfs={tfs}")
                try:
                    push_ohlc_once_compat(
                        api_base=api_base,
                        device_id=device_id,
                        token=device_token,
                        symbols=symbols,
                        tfs=tfs,
                        bars=300,
                        force=True,
                    )
                    for sym in symbols:
                        for tfu in tfs:
                            _mark_pushed(sym, tfu, now_s)
                except Exception as e:
                    alog(f"HB: push_now ohlc error: {e!s}")

            # Cadence push while active
            if trend_active and mt5_ok_flag:
                for sym in symbols:
                    for tfu in tfs:
                        if _due(sym, tfu, cadence, now_s):
                            alog(f"HB: cadence push -> {sym}/{tfu}")
                            try:
                                push_ohlc_once_compat(
                                    api_base=api_base,
                                    device_id=device_id,
                                    token=device_token,
                                    symbols=[sym],
                                    tfs=[tfu],
                                    bars=300,
                                )
                                _mark_pushed(sym, tfu, now_s)
                            except Exception as e:
                                alog(f"HB: cadence ohlc error {sym}/{tfu}: {e!s}")

            _stop.wait(interval_sec)
            continue

        except Exception as e:
            alog(f"heartbeat error: {e!s}")

        _stop.wait(interval_sec)

def read_config() -> dict:
    """
    Load optional config from xtl.cfg / xtl.json.
    Search order (first match wins):
      1) beside the running exe (Path(sys.argv[0]).parent)
      2) APP_DIR (if defined)
      3) current working directory
      4) %ProgramData%\XTL
      5) %LocalAppData%\XTL
    Returns {} on any error. Guarantees a dict (may include 'mt5' sub-dict).
    """
    def _try_load(p: Path) -> dict | None:
        try:
            if not p or not p.exists() or not p.is_file():
                return None
            raw = p.read_text(encoding="utf-8", errors="ignore")
            # strip simple comments (//... or #...) at line start
            lines = []
            for ln in raw.splitlines():
                s = ln.lstrip()
                if s.startswith("//") or s.startswith("#"):
                    continue
                lines.append(ln)
            data = json.loads("\n".join(lines))
            return data if isinstance(data, dict) else {}
        except Exception:
            return None

    candidates: list[Path] = []
    try:
        here = Path(sys.argv[0]).resolve().parent
        candidates += [here / "xtl.cfg", here / "xtl.json"]
    except Exception:
        pass

    # APP_DIR is common in your installer; include if present
    try:
        if "APP_DIR" in globals():
            candidates += [APP_DIR / "xtl.cfg", APP_DIR / "xtl.json"]
    except Exception:
        pass

    # CWD
    try:
        cwd = Path.cwd()
        candidates += [cwd / "xtl.cfg", cwd / "xtl.json"]
    except Exception:
        pass

    # ProgramData + LocalAppData fallbacks
    try:
        pd = Path(os.environ.get("ProgramData", r"C:\ProgramData")) / "XTL"
        la = Path(os.environ.get("LocalAppData", "")) / "XTL" if os.environ.get("LocalAppData") else None
        candidates += [pd / "xtl.cfg", pd / "xtl.json"]
        if la:
            candidates += [la / "xtl.cfg", la / "xtl.json"]
    except Exception:
        pass

    for p in candidates:
        d = _try_load(p)
        if d is not None:
            # normalize mt5 section minimally to avoid KeyErrors elsewhere
            mt5 = d.get("mt5") if isinstance(d, dict) else {}
            if not isinstance(mt5, dict):  # guard
                mt5 = {}
            d["mt5"] = mt5
            return d

    return {}


def _maybe_mt5_worker(api_base: str) -> None:
    """
    MT5 worker:
    - OHLC push loop delegates to agent_ohlc.push_ohlc_once_compat()
    - ALSO starts MT5 command worker that polls /devices/{device_id}/mt5/next and acks
    """
    try:
        # Read config (fallback to {})
        cfg = read_config() if "read_config" in globals() else {}
        mt5_cfg = (cfg.get("mt5") if isinstance(cfg, dict) else {}) or {}

        # Ensure MT5 terminal path (auto-detect if missing)
        mt5_path = (reg_get("MT5.TerminalPath") or reg_get("MT5Path") or "").strip()
        if not mt5_path:
            guess = find_mt5_terminal() if "find_mt5_terminal" in globals() else None
            if guess:
                mt5_path = guess
                alog(f"MT5: auto-detected terminal at {mt5_path}")
            else:
                alog("MT5: no terminal detected; worker disabled (this is OK).")
                return  # graceful skip

            # Persist MT5 path for service + user + machine (HKU LS + HKCU + HKLM)
            _persist_mt5_path_all(mt5_path)

        # Persist MT5 path for the service (LocalSystem) + current user + env
        try:
            _hku_ls_set("MT5Path", mt5_path)                               # LocalSystem hive
            _reg_set(r"HKCU\Software\XTL", "MT5Path", mt5_path)            # interactive user
            os.environ["XTL_MT5_PATH"] = mt5_path                          # env fallback
        except Exception as _e:
            alog(f"MT5: WARN failed to persist terminal path: {_e}")

        # Symbols/TFs
        symbols = mt5_cfg.get("symbols") or ["XAUUSD"]
        tfs = mt5_cfg.get("tfs") or ["M15", "H1", "H4"]
        try:
            bars = int(mt5_cfg.get("bars") or 300)
            bars = min(300, max(30, bars))
        except Exception:
            bars = 300

        # Device creds (LocalSystem hive)
        device_id, device_token = "", ""

        # Wait up to ~10 minutes for bind to complete (service just triggered it)
        for _ in range(600):  # 600 * 1s = 10 minutes
            device_id = (_hku_ls_get("DeviceId") or "").strip()
            device_token = (_hku_ls_get("DeviceToken") or "").strip()
            if device_id and device_token:
                break
            alog("OHLC: waiting for bind (no DeviceId/DeviceToken yet). Will retry in 1s…")
            time.sleep(1)

        if not device_id or not device_token:
            alog("OHLC: still not bound after wait; worker exiting (supervisor will respawn).")
            return



        # ---------------- NEW: start MT5 command worker ----------------
        # This polls /devices/{device_id}/mt5/next and posts /mt5/ack.
        try:
            # Start only once (avoid duplicate polling threads if worker restarts)
            global _MT5_CMD_STARTED
            try:
                _MT5_CMD_STARTED
            except Exception:
                _MT5_CMD_STARTED = False

            if not _MT5_CMD_STARTED:
                start_mt5_cmd_worker = None

                # 1) normal import (preferred)
                try:
                    from xtl.agent_ohlc import start_mt5_cmd_worker  # type: ignore
                except Exception:
                    start_mt5_cmd_worker = None

                # 2) fallback: load by file path (your layout: wizard\xtl\agent_ohlc.py)
                if start_mt5_cmd_worker is None:
                    import importlib.util
                    here = os.path.dirname(os.path.abspath(__file__))

                    candidates = [
                        os.path.join(here, "xtl", "agent_ohlc.py"),
                        os.path.join(here, "agent_ohlc.py"),
                    ]
                    p = next((x for x in candidates if os.path.exists(x)), None)
                    if not p:
                        raise RuntimeError(f"agent_ohlc.py not found. Tried: {candidates}")

                    spec = importlib.util.spec_from_file_location("agent_ohlc", p)
                    mod = importlib.util.module_from_spec(spec)  # type: ignore
                    assert spec and spec.loader
                    spec.loader.exec_module(mod)  # type: ignore
                    start_mt5_cmd_worker = getattr(mod, "start_mt5_cmd_worker", None)

                if not callable(start_mt5_cmd_worker):
                    raise RuntimeError("start_mt5_cmd_worker not callable")

                start_mt5_cmd_worker(
                    api_base=api_base,
                    device_id=device_id,
                    token=device_token,
                    poll_sec=2,
                )
                _MT5_CMD_STARTED = True
                alog("MT5 CMD: worker started (polling /mt5/next)")
            else:
                alog("MT5 CMD: worker already started; skipping")
        except Exception as e:
            alog(f"MT5 CMD: failed to start worker: {type(e).__name__}: {e}")
        # ---------------- END NEW BLOCK ----------------




        # Cadence
        s_per_cycle = int(mt5_cfg.get("period_sec") or 60)
        if s_per_cycle < 15:
            s_per_cycle = 15

        alog(f"OHLC: starting worker symbols={symbols} tfs={tfs} bars={bars} every {s_per_cycle}s")

        # Main loop
        while not _stop.is_set():
            try:
                push_ohlc_once_compat(
                    api_base=api_base,
                    device_id=device_id,
                    token=device_token,
                    symbols=symbols,
                    tfs=tfs,
                    bars=bars,
                )
                alog("OHLC: push cycle done")
            except Exception as e:
                alog(f"OHLC: push cycle error: {e}")
            finally:
                for _ in range(s_per_cycle):
                    if _stop.is_set():
                        break
                    time.sleep(1)

        alog("OHLC: worker exiting")

    except Exception as e:
        alog(f"OHLC: worker failed to start: {e}")

def _hb_interval_sec_default():
    try:
        v = int((_hklm_get_json() or {}).get("HeartbeatSec") or 60)
        return min(300, max(30, v))  # clamp
    except Exception:
        return 60

def agent_main_foreground() -> None:
    """
    Foreground entrypoint:
      - read config (HKLM)
      - persist ApiBase to LocalSystem hive
      - attempt auto-bind (writes DeviceId/DeviceToken to HKU if bind_token present)
      - one-shot OHLC push after bind (lights up Trend immediately)
      - start heartbeat + MT5 worker threads
      - watchdog to respawn threads if they die
    """
    import threading

    # 1) Load api_base (prefer LS hive override; else HKLM ConfigJson; else default)
    try:
        ls_api = (_hku_ls_get("ApiBase") or "").strip()
    except Exception:
        ls_api = ""

    cfg = {}
    try:
        raw = _hklm_get_json()
        cfg = raw if isinstance(raw, dict) else ({} if raw is None else {})
    except Exception:
        cfg = {}

    api_base = (ls_api or cfg.get("api_base") or DEFAULT_API_BASE or "").strip() or DEFAULT_API_BASE

    # Persist ApiBase to LocalSystem hive for post-start diagnostics/UI
    try:
        _hku_ls_set("ApiBase", api_base)
    except Exception:
        pass

    # 2) Auto-bind (no-op if already bound / no bind_token set)
    try:
        _bind_from_registry(api_base)
    except Exception as e:
        alog(f"bind bootstrap error: {e}")

    # Give auto-bind a short window to populate DeviceId/DeviceToken (~5s total)
    for _ in range(5):
        try:
            dev_id = (_hku_ls_get("DeviceId") or "").strip()
            dev_tok = (_hku_ls_get("DeviceToken") or "").strip()
            if ENABLE_INSTALLER_OHLC_TEST and dev_id and dev_tok:
                break
        except Exception:
            pass
        _stop.wait(1.0)

    # Re-read creds once (final)
    try:
        dev_id = (_hku_ls_get("DeviceId") or "").strip()
        dev_tok = (_hku_ls_get("DeviceToken") or "").strip()
    except Exception:
        dev_id = ""
        dev_tok = ""
    alog(f"run: foreground ready; dev_id={dev_id[:8]}..., api_base={api_base}")

    # 3) Immediate one-shot OHLC push after bind (helps first-time startup)
    #    Safe if not bound yet; just logs and moves on.
    try:
        if ENABLE_INSTALLER_OHLC_TEST and dev_id and dev_tok:
            # Defaults that match your server Trend settings
            _symbols = ["XAUUSD"]
            _tfs     = ["M15", "H1", "H4"]
            alog("run: pushing one-shot OHLC after bind")
            from xtl.agent_ohlc import push_ohlc_once
            push_ohlc_once_compat(api_base=api_base, device_id=dev_id, token=dev_tok,
                                  symbols=_symbols, tfs=_tfs, bars=300)
        else:
            alog("run: device not bound (no DeviceId/DeviceToken) — skipping one-shot push")
    except Exception as _e:
        alog(f"run: one-shot push failed: {_e}")

    # 4) Thread builders
    def _spawn_hb() -> threading.Thread:
        t = threading.Thread(
            target=_heartbeat_loop,
            args=(api_base, _hb_interval_sec_default()),
            daemon=True,
            name="hb",
        )
        t.start()
        return t

    def _spawn_price() -> threading.Thread:
        # Use bound creds and registry symbols (same as OHLC)
        try:
            dev_id = (_hku_ls_get("DeviceId") or "").strip()
            dev_tok = (_hku_ls_get("DeviceToken") or "").strip()
        except Exception:
            dev_id, dev_tok = "", ""

        syms = []
        try:
            syms_raw = (reg_get("Symbols") or "").strip()
            syms = [s.strip().upper() for s in syms_raw.split(",") if s.strip()]
        except Exception:
            syms = []

        if not syms:
            syms = ["XAUUSD", "EURUSD", "USDJPY", "GBPUSD", "USDCAD", "USDCHF"]

        t = threading.Thread(
            target=start_price_publisher,
            args=(api_base, dev_id, dev_tok, syms),
            kwargs={"interval_sec": 0.25},
            daemon=True,
            name="price",
        )
        t.start()
        return t

    def _spawn_mt5() -> threading.Thread:
        t = threading.Thread(
            target=_maybe_mt5_worker,
            args=(api_base,),
            daemon=True,
            name="mt5",
        )
        t.start()
        return t

    # 5) Start threads
    th_hb = _spawn_hb()
    th_price = _spawn_price()
    th_mt5 = _spawn_mt5()

    # 6) Lightweight watchdog: if a worker dies unexpectedly, respawn it
    while not _stop.is_set():
        if not th_hb.is_alive():
            alog("watchdog: heartbeat thread died; respawning")
            th_hb = _spawn_hb()
        if not th_price.is_alive():
            alog("watchdog: price thread died; respawning")
            th_price = _spawn_price()
        if not th_mt5.is_alive():
            alog("watchdog: mt5 worker thread died; respawning")
            th_mt5 = _spawn_mt5()
        _stop.wait(1.0)


def agent_main_service():
    """
    Service entrypoint (Session 0).
    Do NOT run MT5 here — supervise a real user-session child (xtl.exe run).
    """
    alog("service: Session 0 entry; supervising user-session agent")

    # --- bind once in Session-0 so child starts with ready creds ---
    try:
        api = (reg_get("ApiBase") or DEFAULT_API_BASE).strip()
    except Exception:
        api = DEFAULT_API_BASE
    try:
        if not (_hku_ls_get("DeviceId") and _hku_ls_get("DeviceToken")):
            alog("service: attempting bind from Session-0 before launching child")
            ok = _bind_from_registry(api)
            alog(f"service: bind result -> {ok}")


        # Always start the supervisor (even if we were already bound)
        alog("service: starting user-session supervisor")
        # Read MT5 path once here (from LS hive or HKLM) and pass to child so it never falls back to roaming MetaQuotes
        mt5_exe = (_hku_ls_get("MT5.TerminalPath")
           or _hku_ls_get("MT5Path")
           or reg_get("MT5.TerminalPath")
           or reg_get("MT5Path")
           or "").strip()



        # Append a safe hint flag + env to force the exact terminal; also set cwd for the child
        child_args = "run" + (f' --mt5-path="{mt5_exe}"' if mt5_exe else "")
        # Export env var for the child; remove if unset
        try:
            if mt5_exe:
               os.environ["XTL_MT5_PATH"] = mt5_exe
            else:
               os.environ.pop("XTL_MT5_PATH", None)
        except Exception:
            pass
        service_supervise_user_agent(
           str(APP_DIR / "xtl.exe"),
           args=child_args,
           restart_backoff_s=10,
           ping_interval_s=5,
        )

    except Exception as _e:
        alog(f"service: bind preflight error: {_e}")

# --------------------------- installer commands ---------------------------





















































def _is_admin() -> bool:





















































    try:





















































        return ctypes.windll.shell32.IsUserAnAdmin() != 0





















































    except Exception:





















































        return False





















































def _elevate_and_exit(args: list[str]) -> None:
    """
    Relaunch this executable elevated (UAC) and exit current process.
    No extra cmd window; direct ShellExecuteW on this exe.
    """
    import ctypes, shlex
    exe = str(Path(sys.argv[0]).resolve())
    params = " ".join(shlex.quote(a) for a in args)
    try:
        rc = ctypes.windll.shell32.ShellExecuteW(
            None, "runas", exe, params, None, 1  # SW_SHOWNORMAL
        )
        # ShellExecuteW returns >32 on success
        if rc <= 32:
            alog(f"elevate failed rc={rc} exe={exe} args={params}")
    except Exception as e:
        alog(f"elevate exception: {e}")
    sys.exit(0)


def _read_sidecar_cfg() -> dict:
    """
    Best-effort read of xtl.cfg living next to the installer binary.
    Format: {"api_base":"...","bind_token":"..."} (JSON)
    """
    try:
        here = Path(sys.argv[0]).resolve().parent
        cfg_path = here / "xtl.cfg"
        if cfg_path.is_file():
            return json.loads(cfg_path.read_text(encoding="utf-8", errors="ignore"))
    except Exception:
        pass
    return {}

def _read_embedded_cfg_from_self() -> dict:
    """
    If you've embedded a JSON blob inside the exe (optional), pull it out.
    Safe no-op if not present.
    """
    try:
        # Many builds place an embedded file under _internal; keep it tiny & optional.
        p = APP_DIR / "_internal" / "xtl_embedded.cfg"
        if p.is_file():
            return json.loads(p.read_text(encoding="utf-8", errors="ignore"))
    except Exception:
        pass
    return {}

def _resolve_bind_inputs() -> tuple[str, str]:
    """
    Returns (api_base, bind_token) by probing, in priority order:
      CLI api= / token=  → ENV (XTL_API_BASE / XTL_BIND_TOKEN)
      → HKCU\Software\XTL\BindToken (if onboarding page staged it)
      → xtl.cfg (sidecar)  → embedded cfg
      → HKLM\ConfigJson (for api_base fallback only; never for token)
    """
    # CLI
    api_cli, tok_cli = _parse_cli_kv(sys.argv[2:])
    # ENV
    api_env = os.environ.get("XTL_API_BASE") or ""
    tok_env = os.environ.get("XTL_BIND_TOKEN") or ""
    # User-staged token (HKCU)
    try:
        tok_hkcu = _reg_get(r"HKCU\Software\XTL", "BindToken") or ""
    except Exception:
        tok_hkcu = ""
    # Sidecar / embedded
    side = _read_sidecar_cfg()
    emb  = _read_embedded_cfg_from_self()

    api = (api_cli or side.get("api_base") or emb.get("api_base") or api_env
           or _hklm_get_json().get("api_base") or DEFAULT_API_BASE)
    tok = (tok_cli or tok_env or tok_hkcu or side.get("bind_token") or emb.get("bind_token") or "")

    return api.strip(), tok.strip()
def cmd_mt5_prompt() -> int:
    """One-shot GUI to select MT5 terminal exe, then persist to HKLM + LS hive."""
    try:
        # If we can auto-detect, do it silently
        try:
            ok, mt5_path = _mt5_caps()  # your existing helper, returns (ok, path)
        except Exception:
            ok, mt5_path = False, None

        if not mt5_path:
            # Fallback: tiny Tk file picker (non-blocking for installer since we spawn this cmd separately)
            import tkinter as tk
            from tkinter import filedialog, messagebox
            root = tk.Tk()
            root.withdraw()
            messagebox.showinfo("XTL Agent", "Please locate your MetaTrader 5 terminal (terminal64.exe).")
            mt5_path = filedialog.askopenfilename(
                title="Select MetaTrader 5 terminal",
                filetypes=[("terminal64.exe", "terminal64.exe"), ("All files", "*.*")]
            )
            root.destroy()

        if not mt5_path:
            alog("mt5-prompt: user cancelled")
            return 1

        mt5_path = str(Path(mt5_path).resolve())
        if not Path(mt5_path).exists():
            alog(f"mt5-prompt: path not found: {mt5_path}")
            return 1

        _persist_mt5_path_all(mt5_path)  # your existing function writing HKLM + HKU\LS
        alog(f"mt5-prompt: saved path -> {mt5_path}")
        return 0
    except Exception as e:
        alog(f"mt5-prompt: ERROR {e}")
        return 1



def cmd_install(api_base: Optional[str] = None, bind_token: Optional[str] = None) -> int:
    """
    Install the XTL agent as a Windows service:
      - Elevate if needed
      - Copy payload into APP_DIR (Program Files layout)
      - Ensure embedded runtime + VC++ are present
      - Noninteractive preflight (no UI)
      - Upsert config (api_base; bind_token staged in LS hive only)
      - Create/update service idempotently, set AutoStart
      - Start service (and attempt auto-bind if token staged), verify
    """
    import os, time  # time used for brief post-start settle

    alog("install: begin")
    try:
        if not _is_admin():
            _elevate_and_exit(["install"])
            return 0  # child continues

        _ensure_cert_bundle()

        # Best-effort MT5 autodetect very early; persist paths if found
        try:
            ok, mt5_path = _mt5_caps()
            if mt5_path:
                _persist_mt5_path_all(mt5_path)
        except Exception:
            pass

        log("install: ensure_winsw_binary()...")
        ensure_winsw_binary()

        log("install: deploy_files()...")
        deploy_files(Path(sys.argv[0]).resolve())

        # Resolve APP_DIR (destination where the service runs)
        def _resolve_app_dir() -> Path:
            candidates = [
                Path(os.environ.get("ProgramFiles", r"C:\Program Files")) / "XTL" / "dist" / "xtl",
                Path(os.environ.get("ProgramFiles(x86)", r"C:\Program Files")) / "XTL" / "dist" / "xtl",
                ]
            for c in candidates:
                if c.exists():
                    return c
            return candidates[0]

        global APP_DIR
        APP_DIR = _resolve_app_dir()

        log("install: ensure_internal_runtime_complete()...")
        ensure_internal_runtime_complete()

        # Optional visibility to the embedded runtime landing
        try:
            names = [p.name for p in (APP_DIR / "_internal").glob("*")]
            alog(f"install: APP_DIR={APP_DIR} _internal={names}")
        except Exception:
            pass

        # Noninteractive preflight (avoid any GUI prompts)
        os.environ["XTL_NONINTERACTIVE"] = "1"
        log("install: preflight skipped (noninteractive)")

        # ----------------- SERVICE + CONFIG PHASE (single, idempotent) -----------------
        # Resolve inputs (cli/env/registry) and normalize base URL
        in_api, in_token = _resolve_bind_inputs()
        if api_base is None:
            api_base = in_api
        if bind_token is None:
            bind_token = in_token

        api_eff = (api_base or DEFAULT_API_BASE).strip().rstrip("/")
        if api_eff.lower().endswith("/api"):
            api_eff = api_eff[:-4]

        # 1) HKLM baseline config (api_base only)
        cfg = upsert_config(api_base=api_eff, bind_token="")
        # Ensure broker meta is present and fresh each install/repair
        _write_broker_meta_from_env_or_local()


        # 2) Hygiene: clear LocalSystem creds from any prior attempt
        try:
            _hku_ls_del("DeviceId"); _hku_ls_del("DeviceToken")
        except Exception:
            pass

        # 3) Persist ApiBase into LS hive for the running service
        try:
            _hku_ls_set("ApiBase", cfg.get("api_base", api_eff))
        except Exception:
            pass

        # 4) Stage BindToken only in LS hive; scrub any user-scoped token
        if bind_token:
            _hku_ls_set("BindToken", bind_token)
            alog(r"install: saved BindToken to HKU\S-1-5-18\Software\XTL (HKLM has api_base only)")
            try:
                _reg_del(r"HKCU\Software\XTL", "BindToken")
            except Exception:
                pass
        else:
            alog("install: no bind_token provided; HKLM has api_base only")

        # 5) Fire a best-effort one-shot MT5 worker (non-blocking; safe if MT5 not ready yet)
        # Run a one-shot MT5 worker ONLY if already bound; otherwise skip to avoid wait-loop
        try:
           if (_hku_ls_get("DeviceId") and _hku_ls_get("DeviceToken")):
               _maybe_mt5_worker(api_eff)
           else:
               alog("install: skipping _maybe_mt5_worker (device not bound yet)")
        except Exception:
           pass


        # 6) Create/update service and mark AutoStart
        svc_name = install_service_idempotent()
        _ensure_service_auto_start(svc_name)
        _configure_service_recovery(svc_name)

        # 7) Start service and wait for RUNNING
        log("install: start_service_and_wait()...")
        start_service_and_wait(svc_name, timeout_s=30)
        # Re-seed broker meta now that the service (LocalSystem profile) exists for sure
        _write_broker_meta_from_env_or_local()


        # 8) Brief settle + optional auto-bind attempt (harmless if already bound)
        time.sleep(2)
        try:
            _auto_bind_if_needed(api_eff)
        except Exception as e:
            alog(f"install: auto-bind helper skipped: {e}")

        # 9) Post-start verification (tolerate soft failures)
        try:
            verify_post_start()
        except Exception as e:
            log(f"verify_post_start: WARN {e}")

        # 10) If MT5 path still missing, launch detached picker once
        try:
            need_mt5 = not (reg_get("MT5.TerminalPath") or reg_get("MT5Path"))
            if need_mt5:
                exe = str(Path(sys.argv[0]).resolve())
                import subprocess
                DETACHED_PROCESS = 0x00000008
                subprocess.Popen([exe, "mt5-prompt"], close_fds=True, creationflags=DETACHED_PROCESS)
                alog("install: launched mt5-prompt in background")
        except Exception as e:
            alog(f"install: mt5-prompt launch skipped: {e}")

        log("install: success")
        return 0

    except Exception as e:
        log(f"install: ERROR {e}")
        return 1


def cmd_repair() -> int:
    if not _is_admin():
       _elevate_and_exit(["repair"])
    return cmd_install()
# ------------------------------- CLI entry --------------------------------
def _parse_cli_kv(args: list[str]) -> tuple[Optional[str], Optional[str]]:
    a = None; t = None
    for x in args:
        if x.startswith("api="): a = x.split("=",1)[1]
        elif x.startswith("token="): t = x.split("=",1)[1]
    return a,t
# ---- CLI verb wrappers ----
def cmd_run() -> int:
    # foreground agent (no service)
    _ensure_cert_bundle()
    return agent_main_foreground()

def cmd_start() -> int:
    """
    Start the canonical Windows service (XTLAgent) if it isn't already running.
    No fallback to legacy names.
    """
    name = SERVICE_CANON  # hard-pin

    try:
        # Is it already running (or starting)?
        try:
            q = _sc_query(name)
            if "STATE" in q and ("RUNNING" in q or "START_PENDING" in q):
                log(f"start: {name} already running")
                return 0
        except Exception:
            # if query fails, we will attempt to start anyway
            pass

        # Start and wait until running
        start_service_and_wait(name, timeout_s=30)
        return 0

    except Exception as e:
        log(f"start: ERROR {e}")
        return 1

def cmd_stop() -> int:
    # graceful stop of the Windows service
    try:
        _graceful("stop")  # best-effort
        return 0
    except Exception as e:
        log(f"stop: ERROR {e}")
        return 1
def _print_help() -> None:
    print(
        "XTL Agent\n"
        "Usage:\n"
        "  xtl.exe install      Install/repair files and register service\n"
        "  xtl.exe repair       Repair files in place (no re-register)\n"
        "  xtl.exe start        Start the Windows service\n"
        "  xtl.exe stop         Stop the Windows service\n"
        "  xtl.exe run          Run in foreground (no service)\n"
        "  xtl.exe service      Service entry (WinSW)\n"
        "  xtl.exe help         Show this help\n",
        flush=True
    )
def main(argv=None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if not args:
        args = ["install"]

    cmd = (args[0] or "").lower().strip()
    if   cmd == "install":  return cmd_install()
    elif cmd == "repair":   return cmd_repair()
    elif cmd == "start":    return cmd_start()
    elif cmd == "stop":     return cmd_stop()
    elif cmd == "run":      return cmd_run()
    elif cmd == "service":  return agent_main_service()
    elif cmd == "mt5-prompt":  return cmd_mt5_prompt()
    elif cmd == "help":     _print_help(); return 0
    else:
        print(f"Unknown command: {cmd}", flush=True)
        _print_help()
        return 2
if __name__ == "__main__":
    raise SystemExit(main())
