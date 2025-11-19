# -*- coding: utf-8 -*-
# xtl_worker.py
# Minimal script worker for XauTrendLab:
#  - reads provision.json (api_base, device_token or pairing_code, device_name, optional terminal_path)
#  - ensures device token (token-first; fallback to pairing)
#  - heartbeats
#  - long-polls for jobs
#  - on "backtest" job -> generates a small CSV, uploads to /upload/{job_id}, marks done

import json, os, time, platform, tempfile, csv, requests, glob, shutil
import argparse

# Optional Windows registry (ignored on non-Windows)
try:
    import winreg  # type: ignore
except Exception:
    winreg = None

PROVISION = "provision.json"
HB_INTERVAL = 25

# ----------------- Provision helpers -----------------
def load_provision():
    with open(PROVISION, "r", encoding="utf-8") as f:
        return json.load(f)

def save_provision(prov: dict):
    tmp = PROVISION + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(prov, f, indent=2)
    os.replace(tmp, PROVISION)

# ----------------- HTTP helpers -----------------
def api_post(api, path, **kw):
    r = requests.post(api + path, timeout=60, **kw)
    if r.status_code >= 400:
        raise RuntimeError(f"POST {path} -> {r.status_code} {r.text}")
    return r

def api_get(api, path, **kw):
    r = requests.get(api + path, timeout=60, **kw)
    if r.status_code >= 400:
        raise RuntimeError(f"GET {path} -> {r.status_code} {r.text}")
    return r

# ----------------- Device auth -----------------
def claim_with_code(api, code, device_name, verbose=False):
    if verbose: print(f"[worker] Claiming with code: {code}")
    r = api_post(api, "/worker/claim", json={"code": code, "device_name": device_name})
    j = r.json()
    return j["device_id"], j["device_token"]

def heartbeat_once(api, token, preflight=None, verbose=False):
    hdr = {"Authorization": f"Bearer {token}"}
    body = {}
    if preflight is not None:
        body["preflight"] = preflight
    api_post(api, "/worker/heartbeat", headers=hdr, json=body)
    if verbose: print("[worker] heartbeat ok")

def long_poll_next(api, token, timeout_s=30):
    hdr = {"Authorization": f"Bearer {token}"}
    r = requests.post(api + "/worker/next", headers=hdr, timeout=timeout_s + 5)
    if r.status_code == 204:
        return None
    if r.status_code >= 400:
        raise RuntimeError(f"/worker/next {r.status_code} {r.text}")
    return r.json()

def progress(api, token, job_id, **fields):
    hdr = {"Authorization": f"Bearer {token}"}
    body = {"job_id": job_id, **fields}
    api_post(api, "/worker/progress", headers=hdr, json=body)

def done(api, token, job_id, **fields):
    hdr = {"Authorization": f"Bearer {token}"}
    body = {"job_id": job_id, **fields}
    api_post(api, "/worker/done", headers=hdr, json=body)

def ensure_token(api, prov, verbose=False):
    """
    Returns (device_id, token). Uses device_token if present, else claims with pairing_code.
    Persists token/device_id to provision.json for future runs.
    """
    device_name = (prov.get("device_name") or "MyPC").strip()

    # token-first?
    token = prov.get("device_token")
    dev_id = prov.get("device_id")

    if token:
        if verbose: print(f"[worker] Using existing token for device: {dev_id or '(unknown)'}")
        try:
            preflight = {
                "os": platform.platform(),
                "python": platform.python_version(),
                "hostname": platform.node(),
            }
            heartbeat_once(api, token, preflight=preflight, verbose=verbose)
            return dev_id or "unknown", token
        except Exception as e:
            print("[worker] existing token failed:", e)
            # fall through and try pairing code if available

    # legacy code flow
    code = prov.get("pairing_code")
    if not code:
        raise RuntimeError("No device_token and no pairing_code in provision.json")

    dev_id, token = claim_with_code(api, code, device_name, verbose=verbose)
    if verbose: print(f"[worker] Claimed device: {dev_id}")

    # persist so next run is token-first
    prov["device_token"] = token
    prov["device_id"] = dev_id
    save_provision(prov)

    # first heartbeat (with preflight)
    preflight = {
        "os": platform.platform(),
        "python": platform.python_version(),
        "hostname": platform.node(),
    }
    heartbeat_once(api, token, preflight=preflight, verbose=verbose)
    return dev_id, token

# ----------------- MT5 discovery (optional) -----------------
import glob

def find_mt5_install(verbose: bool = False):
    """
    Return (best_path, candidates_list). Searches common places for terminal64.exe.
    Prefers AppData\\Roaming\\MetaQuotes\\Terminal instances.
    """
    candidates: set[str] = set()

    patterns = [
        r"%APPDATA%\MetaQuotes\Terminal\*\terminal64.exe",
        r"%LOCALAPPDATA%\Programs\MetaTrader 5\terminal64.exe",
        r"%PROGRAMFILES%\MetaTrader 5\terminal64.exe",
        r"%PROGRAMFILES(X86)%\MetaTrader 5\terminal64.exe",
        r"%PROGRAMFILES%\*\MetaTrader 5*\terminal64.exe",
        r"%PROGRAMFILES(X86)%\*\MetaTrader 5*\terminal64.exe",
    ]

    for pat in patterns:
        for p in glob.glob(os.path.expandvars(pat)):
            if os.path.isfile(p):
                candidates.add(os.path.normpath(p))

    # Last-chance walk (bounded) under Roaming\MetaQuotes\Terminal
    roaming = os.path.join(os.environ.get("APPDATA", ""), "MetaQuotes", "Terminal")
    if os.path.isdir(roaming):
        base_depth = roaming.count(os.sep)
        for root, dirs, files in os.walk(roaming):
            # limit depth to keep it quick
            if root.count(os.sep) - base_depth > 3:
                dirs[:] = []
                continue
            if "terminal64.exe" in [f.lower() for f in files]:
                candidates.add(os.path.join(root, "terminal64.exe"))

    if not candidates:
        if verbose:
            print("[find_mt5_install] no candidates")
        return None, []

    # prefer Roaming/MetaQuotes installs
    preferred = [p for p in candidates if "\\MetaQuotes\\Terminal\\" in p]
    best = sorted(preferred or candidates)[0]
    if verbose:
        print("[find_mt5_install] best:", best)
    return best, sorted(candidates)

def choose_terminal_path(prov: dict, verbose: bool = False):
    """Use provision.json override if valid, else auto-detect."""
    tp = prov.get("terminal_path")
    if tp and os.path.isfile(tp):
        if verbose:
            print("[MT5] using provisioned terminal_path:", tp)
        return tp
    best, _ = find_mt5_install(verbose=verbose)
    if best and verbose:
        print("[MT5] auto-detected terminal:", best)
    return best
# Placeholder - wire up real MT5 checks later
def test_mt5_connection(terminal_path: str, login=None, password=None, server=None, verbose=False):
    """
    Return True/False and a message. For now, just checks that terminal exists on disk.
    """
    if not terminal_path or not os.path.isfile(terminal_path):
        return False, "terminal64.exe not found"
    return True, "MT5 present (basic check)"

# ----------------- Job runner -----------------
def make_demo_csv(rows=100):
    fd, path = tempfile.mkstemp(prefix="xtl_", suffix=".csv")
    os.close(fd)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["index", "value"])
        for i in range(rows):
            w.writerow([i, i * 0.01])
    return path

def upload_csv(api, token, job_id, path):
    hdr = {"Authorization": f"Bearer {token}"}
    with open(path, "rb") as f:
        files = {"file": ("backtest.csv", f, "text/csv")}
        r = requests.post(api + f"/upload/{job_id}", headers=hdr, files=files, timeout=120)
    if r.status_code >= 400:
        raise RuntimeError(f"upload failed {r.status_code} {r.text}")
# --- add this helper above run_job ---
def handle_test_mt5(api, token, job_id, verbose=False):
    try:
        if verbose: print("[test_mt5] starting")
        progress(api, token, job_id, status="running", progress=10, message="Checking MT5 installation")
        best, candidates = find_mt5_install()
        if not best:
            progress(api, token, job_id, status="error", progress=100, message="MT5 terminal64.exe not found")
            done(api, token, job_id, status="error", progress=100, message="MT5 not found")
            if verbose: print("[test_mt5] not found")
            return
        msg = f"Found MT5 at: {best}"
        if verbose: print("[test_mt5]", msg)
        progress(api, token, job_id, status="running", progress=60, message=msg)
        done(api, token, job_id, status="done", progress=100, message="MT5 OK")
    except Exception as e:
        progress(api, token, job_id, status="error", error=str(e))
        done(api, token, job_id, status="error", progress=100, message="Test failed")
        if verbose: print("[test_mt5] error:", e)

def handle_backtest_demo(api, token, job_id, verbose=False):
    if verbose: print("[backtest] demo start")
    progress(api, token, job_id, status="running", progress=5, message="Preparing")
    time.sleep(0.5)
    progress(api, token, job_id, status="running", progress=40, message="Running backtest")
    path = make_demo_csv()
    time.sleep(0.5)
    progress(api, token, job_id, status="running", progress=80, message="Uploading")
    upload_csv(api, token, job_id, path)
    done(api, token, job_id, status="done", progress=100, message="Done")
    try: os.remove(path)
    except: pass
    if verbose: print("[backtest] demo done")

# --- replace your existing run_job with this ---
def run_job(api, token, job, prov=None, **kwargs):
    verbose = bool(kwargs.get("verbose"))
    job_id = job.get("job_id") or "unknown"
    typ = job.get("type", "")
    if verbose: print("[job] received:", job)

    if typ == "test_mt5":
        return handle_test_mt5(api, token, job_id, verbose=verbose)
    elif typ == "backtest":
        return handle_backtest_demo(api, token, job_id, verbose=verbose)
    else:
        progress(api, token, job_id, status="error", error=f"Unsupported job type: {typ}")
        done(api, token, job_id, status="error", progress=100, message="Unsupported job")
        if verbose: print("[job] unsupported type:", typ)

# ----------------- Main loop -----------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()
    verbose = bool(args.verbose or os.getenv("XTL_VERBOSE"))

    prov = load_provision()
    api = prov["api_base"].rstrip("/")

    # ensure token (token-first; else claim via pairing_code)
    dev_id, token = ensure_token(api, prov, verbose=verbose)

    last_hb = time.time()
    while True:
        try:
            # long-poll next job
            job = long_poll_next(api, token, timeout_s=30)
            if job:
                try:
                    run_job(api, token, job, prov, verbose=verbose)
                except TypeError as e:
                   # If an older worker without the 'verbose' kw is still on disk/pycache,
                   # fall back and call it without the kw so jobs still run.
                  if "unexpected keyword argument 'verbose'" in str(e):
                      run_job(api, token, job, prov)
                  else:
                      raise

        except Exception as e:
            msg = str(e)
            print("[worker] loop error:", msg)
            # If token went bad (401), try to re-ensure (may re-claim via pairing_code)
            if "401" in msg:
                try:
                    prov = load_provision()
                    dev_id, token = ensure_token(api, prov, verbose=verbose)
                    print("[worker] token refreshed")
                except Exception as e2:
                    print("[worker] token refresh failed:", e2)
                    time.sleep(5)
            else:
                time.sleep(2)

        # periodic heartbeat
        if time.time() - last_hb > HB_INTERVAL:
            try:
                heartbeat_once(api, token, preflight=None, verbose=verbose)
            except Exception as e:
                print("[worker] heartbeat error:", e)
            last_hb = time.time()

        time.sleep(0.2)

if __name__ == "__main__":
    main()
