
# xtl_installer.py — native service first; WinSW auto-download fallback (no base64 needed)
#
# What it does:
# 1) Installs native Windows service: "xtl.exe service" (your current behavior), sets Auto + Delayed start
# 2) If not RUNNING, automatically lays down WinSW (auto-download from GitHub if no local copy),
#    writes XTLAgent.xml (with <arguments>service</arguments>), then runs "winsw install" + "winsw start".
# 3) Leaves heartbeat + broker OHLC behavior unchanged (same working directory beside xtl.exe).
#
# Run as Administrator: xtl.exe install  (or python xtl_installer.py install for dev).

import os, sys, time, subprocess, shutil, ctypes, hashlib
from pathlib import Path

APP_NAME = "XTLAgent"
DISPLAY  = "XTL Agent"
DESC     = "XauTrendLab Windows Worker (silent, service-managed)"
WINSW_NAME = "winsw.exe"
WINSW_XML  = "XTLAgent.xml"
WINSW_RELEASE = "v3.0.0"
WINSW_URL = f"https://github.com/winsw/winsw/releases/download/{WINSW_RELEASE}/WinSW-x64.exe"

LOG_PATH = Path("installer.log")

def logi(msg: str):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    try:
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(f"{ts} {msg}\n")
    except Exception:
        pass
    print(msg, flush=True)

def is_admin() -> bool:
    try:
        return ctypes.windll.shell32.IsUserAnAdmin()
    except Exception:
        return False

def ensure_elevated_or_relaunch():
    if is_admin():
        return
    # relaunch elevated
    try:
        params = " ".join([f'"{p}"' for p in sys.argv])
        ctypes.windll.shell32.ShellExecuteW(None, "runas", sys.executable, params, None, 1)
        sys.exit(0)
    except Exception as e:
        logi(f"[auth] Elevation failed: {e}")
        sys.exit(1)

def exe_root() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent

def xtl_home() -> Path:
    # final folder beside exe: dist/xtl
    root = exe_root()
    home = root / "dist" / "xtl"
    home.mkdir(parents=True, exist_ok=True)
    return home

def locate_xtl_exe() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve()
    here = Path(__file__).resolve().parent
    for c in (here/"xtl.exe", here/"dist"/"xtl"/"xtl.exe"):
        if c.exists():
            return c
    return here/"dist"/"xtl"/"xtl.exe"  # default target path

# --- process helpers (hidden window) ---
def _run_hidden(cmd, cwd=None):
    si = subprocess.STARTUPINFO()
    si.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    si.wShowWindow = 0
    CREATE_NO_WINDOW = 0x08000000
    try:
        p = subprocess.run(cmd, cwd=cwd, startupinfo=si, creationflags=CREATE_NO_WINDOW,
                           capture_output=True, text=True, timeout=60)
    except Exception as e:
        logi(f"[run] {cmd} -> EXC {e}")
        raise
    if p.stdout:
        logi(f"[run] out: {p.stdout.strip()}")
    if p.stderr:
        logi(f"[run] err: {p.stderr.strip()}")
    return p

def _popen_hidden(cmd, cwd=None):
    si = subprocess.STARTUPINFO()
    si.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    si.wShowWindow = 0
    CREATE_NO_WINDOW = 0x08000000
    return subprocess.Popen(cmd, cwd=cwd, startupinfo=si, creationflags=CREATE_NO_WINDOW,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

# --- WinSW helpers ---
def winsw_path(dst_dir: Path) -> Path:
    return dst_dir / WINSW_NAME

def write_winsw(dst_dir: Path) -> Path:
    """
    Ensure winsw.exe exists in dst_dir.
    Order: (1) local copy next to installer/exe; (2) auto-download from GitHub.
    """
    dst = winsw_path(dst_dir)
    if dst.exists() and dst.stat().st_size > 0:
        logi(f"[winsw] present: {dst}")
        return dst

    here = exe_root()
    for cand in (here/"winsw.exe", here/"WinSW-x64.exe"):
        if cand.exists() and cand.stat().st_size > 0:
            shutil.copyfile(cand, dst)
            logi(f"[winsw] copied local {cand} -> {dst}")
            return dst

    # download fallback
    try:
        import urllib.request
        logi(f"[winsw] downloading {WINSW_URL}")
        with urllib.request.urlopen(WINSW_URL, timeout=30) as r, open(dst, "wb") as f:
            shutil.copyfileobj(r, f)
        logi(f"[winsw] downloaded: {dst} ({dst.stat().st_size} bytes)")
    except Exception as e:
        logi(f"[winsw] download failed: {e}")
        raise
    return dst

def write_winsw_xml(dst_dir: Path, exe_name="xtl.exe") -> Path:
    xml = f"""<service>
  <id>{APP_NAME}</id>
  <name>{DISPLAY}</name>
  <description>{DESC}</description>
  <executable>{exe_name}</executable>
  <arguments>service</arguments>
  <workingdirectory>{dst_dir.as_posix()}</workingdirectory>
  <log mode="roll" />
  <onfailure action="restart" delay="5 sec" />
</service>
"""
    p = dst_dir / WINSW_XML
    p.write_text(xml, encoding="utf-8")
    logi(f"[winsw] wrote XML: {p}")
    return p

def install_start_winsw(dst_dir: Path) -> bool:
    w = write_winsw(dst_dir)
    write_winsw_xml(dst_dir, exe_name="xtl.exe")
    p1 = _run_hidden([str(w), "install"], cwd=str(dst_dir))
    if p1.returncode != 0 and "already exists" not in (p1.stdout + p1.stderr):
        logi(f"[winsw] install rc={p1.returncode}")
        return False
    _run_hidden([str(w), "start"], cwd=str(dst_dir))
    q = _run_hidden(["sc", "query", APP_NAME])
    ok = ("STATE" in q.stdout and "RUNNING" in q.stdout) or ("RUNNING" in q.stderr)
    logi(f"[winsw] RUNNING={ok}")
    return ok

# --- Native SCM (your existing behavior) ---
def install_start_native(exe_path: Path) -> bool:
    svc = APP_NAME
    binpath = f'"{str(exe_path)}" service'
    def _sc(args):
        return _run_hidden(["sc"] + args)
    # Try create
    p = _sc(["create", svc, "binPath=", binpath, "DisplayName=", DISPLAY, "start=", "auto"])
    if p.returncode != 0 and "already exists" not in (p.stdout + p.stderr):
        logi(f"[svc] create failed rc={p.returncode}")
    # Ensure config + delayed start
    _sc(["config", svc, "binPath=", binpath, "start=", "auto"])
    _sc(["config", svc, "start=", "delayed-auto"])
    # Start
    _sc(["start", svc])
    # Query
    q = _sc(["query", svc])
    ok = ("STATE" in q.stdout and "RUNNING" in q.stdout)
    logi(f"[svc] RUNNING={ok}")
    return ok

# --- Main commands ---
def cmd_install():
    ensure_elevated_or_relaunch()
    home = xtl_home()
    xtl = locate_xtl_exe()
    # ensure xtl.exe present (dev mode can still proceed to lay down winsw/xml)
    if not xtl.exists():
        # If running unfrozen, you might copy or build later; proceed to prep service files.
        logi(f"[warn] xtl.exe not found at {xtl}; continuing to prep WinSW files")
    # proactively place winsw files (harmless if unused)
    try:
        write_winsw(home)
        write_winsw_xml(home, exe_name="xtl.exe")
    except Exception as e:
        logi(f"[winsw] prep non-fatal: {e}")

    # Try native service first (your current behavior)
    ok = install_start_native(xtl if xtl.exists() else (home/"xtl.exe"))
    if ok:
        print("\nRESULT: OK — native service installed & RUNNING")
        return
    # Fallback to WinSW
    logi("[svc] native path not RUNNING; trying WinSW fallback")
    ok2 = install_start_winsw(home)
    if ok2:
        print("\nRESULT: OK — WinSW service installed & RUNNING")
        return
    print("\nRESULT: FAILED — neither native nor WinSW path reached RUNNING; see installer.log")

def cmd_uninstall():
    ensure_elevated_or_relaunch()
    # Try stop/uninstall native
    def _sc(args):
        return _run_hidden(["sc"] + args)
    _sc(["stop", APP_NAME])
    time.sleep(1)
    _sc(["delete", APP_NAME])
    # Try WinSW uninstall
    home = xtl_home()
    w = winsw_path(home)
    if w.exists():
        _run_hidden([str(w), "stop"], cwd=str(home))
        _run_hidden([str(w), "uninstall"], cwd=str(home))
    print("RESULT: Uninstall attempted for both native and WinSW (see installer.log)")

def main():
    if len(sys.argv) <= 1:
        print("Usage: xtl_installer.py [install|uninstall]")
        return
    cmd = sys.argv[1].lower()
    if cmd == "install":
        cmd_install()
    elif cmd == "uninstall":
        cmd_uninstall()
    else:
        print("Unknown command")

if __name__ == "__main__":
    if os.name != "nt":
        print("Windows only.")
        sys.exit(1)
    # fresh log
    try:
        if LOG_PATH.exists(): LOG_PATH.unlink()
    except Exception:
        pass
    main()
