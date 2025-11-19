# api/routes_worker.py
from __future__ import annotations

import os, time, json
from typing import Optional

import redis
from fastapi import APIRouter, Depends, Header, HTTPException, Response
from pydantic import BaseModel, Field

r = APIRouter(prefix="/worker", tags=["worker"])

# ------------ Config ------------
REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
R = redis.from_url(REDIS_URL)

DASHBOARD_ACTIVE_MINUTES = int(os.getenv("DASHBOARD_ACTIVE_MINUTES", "20"))
SINGLE_ACTIVE_PER_HOST = os.getenv("SINGLE_ACTIVE_PER_HOST", "1") == "1"
GLOBAL_FALLBACK_QUEUE = os.getenv("GLOBAL_FALLBACK_QUEUE", "queue:global")
DEVICE_QUEUE_PREFIX = os.getenv("DEVICE_QUEUE_PREFIX", "queue:dev:")  # per-device queue key = prefix + device_id
LONGPOLL_SECONDS = int(os.getenv("WORKER_LONGPOLL_SECONDS", "20"))

# ------------ Helpers ------------

def _hkey(device_id: str) -> str:
    return f"device:{device_id}"

def _host_index_key(host_id: str) -> str:
    return f"host:{host_id}:devices"

def _now() -> int:
    return int(time.time())

def _decode(b: Optional[bytes]) -> Optional[str]:
    return b.decode() if isinstance(b, (bytes, bytearray)) else b

def _device_status(api_ok: Optional[bool], mt5_ok: Optional[bool], autostart_ok: Optional[bool]) -> str:
    # Healthy if all provided flags are true; degraded if any false; unknown if none provided
    flags = [f for f in (api_ok, mt5_ok, autostart_ok) if f is not None]
    if not flags:
        return "unknown"
    return "healthy" if all(flags) else "degraded"

# ------------ Dependency ------------

def require_device(
    device_id: str = Header(..., alias="X-Device-Id"),
    device_token: str = Header(..., alias="X-Device-Token"),
) -> dict:
    """
    Validate device headers against Redis.
    Supports either:
      - HGET device:{id} -> field "device_token"
      - GET devtoken:{token} -> device_id mapping (legacy/new token index)
    Returns the device hash (as dict) if valid; raises 401 otherwise.
    """
    dkey = _hkey(device_id)
    if not R.exists(dkey):
        # Try the token->id mapping to allow devices that only know token
        mapped = _decode(R.get(f"devtoken:{device_token}"))
        if mapped != device_id:
            raise HTTPException(status_code=401, detail="Invalid device credentials")
        # Create a minimal record if missing
        R.hset(dkey, mapping={"device_id": device_id, "active": 1, "created_at": _now()})

    stored_token = _decode(R.hget(dkey, "device_token"))
    if stored_token:
        if stored_token != device_token:
            raise HTTPException(status_code=401, detail="Invalid device credentials")
    else:
        # allow via token index
        mapped = _decode(R.get(f"devtoken:{device_token}"))
        if mapped != device_id:
            raise HTTPException(status_code=401, detail="Invalid device credentials")

    # fetch snapshot to pass downstream
    data = { _decode(k): _decode(v) for k, v in R.hgetall(dkey).items() }
    data.setdefault("device_id", device_id)
    return data

# ------------ Models ------------

class HeartbeatIn(BaseModel):
    api_ok: Optional[bool] = True
    mt5_ok: Optional[bool] = None
    autostart_ok: Optional[bool] = None
    version: Optional[str] = None
    mt5_build: Optional[str] = None
    host_id: Optional[str] = Field(None, description="Stable fingerprint (hostname + BIOS UUID)")
    label: Optional[str] = Field(None, description="Human label, e.g. RoboForex_<tail>")
    last_error: Optional[str] = None

# ------------ Routes ------------

@r.post("/heartbeat", dependencies=[Depends(require_device)])
def heartbeat(hb: HeartbeatIn, dev: dict = Depends(require_device)):
    """
    Worker sends heartbeat ~every 30s with:
      { api_ok, mt5_ok, autostart_ok, host_id, label, version, mt5_build, last_error }
    """
    device_id = dev["device_id"]
    dkey = _hkey(device_id)
    now = _now()

    mapping = {
         
       "last_heartbeat": now,
       "version": hb.version or "",
       "mt5_ok": "1" if hb.mt5_ok else "0",
       "api_ok": "1" if hb.api_ok else "0",
       "autostart_ok": "1" if hb.autostart_ok else "0",
       "status": "online",
       "last_error": hb.last_error or "",
    }
    R.hset(f"device:{hb.device_id}", mapping=mapping)

    # optional: one-time seeding on first sight
    R.hsetnx(f"device:{hb.device_id}", "active", "1")  # only if missing
    
     


    if hb.api_ok is not None:        mapping["api_ok"] = int(bool(hb.api_ok))
    if hb.mt5_ok is not None:        mapping["mt5_ok"] = int(bool(hb.mt5_ok))
    if hb.autostart_ok is not None:  mapping["autostart_ok"] = int(bool(hb.autostart_ok))
    if hb.version is not None:       mapping["version"] = hb.version
    if hb.mt5_build is not None:     mapping["mt5_build"] = hb.mt5_build
    if hb.last_error is not None:    mapping["last_error"] = hb.last_error

    # host + label
    host_id = hb.host_id or dev.get("host_id")
    if host_id: mapping["host_id"] = host_id
    label = hb.label or dev.get("label")
    if label: mapping["label"] = label

    # status rollup
    status = _device_status(hb.api_ok, hb.mt5_ok, hb.autostart_ok)
    if status != "unknown":
        mapping["status"] = status

    pipe = R.pipeline()
    pipe.hset(dkey, mapping=mapping)

    # maintain reverse index for host->devices
    if host_id:
        pipe.sadd(_host_index_key(host_id), device_id)

    # SINGLE_ACTIVE_PER_HOST semantics: mark others on same host not_in_use
    if host_id and SINGLE_ACTIVE_PER_HOST:
        others = R.smembers(_host_index_key(host_id))
        for raw in others:
            other = _decode(raw)
            if other and other != device_id:
                pipe.hset(_hkey(other), mapping={"active": 0, "status": "not_in_use"})
    pipe.execute()

    return {"ok": True, "device_id": device_id, "status": mapping.get("status", dev.get("status", "unknown"))}

@r.post("/next")
def next_job(response: Response, dev: dict = Depends(require_device)):
    """
    Long-poll for the next job. Order:
      1) If device.active != 1 -> 204 (not in use)
      2) BRPOP device queue
      3) BRPOP global queue
    Returns 204 No Content if nothing ready.
    """
    device_id = dev["device_id"]
    dkey = _hkey(device_id)

    # Check active flag
    active = R.hget(dkey, "active")
    if not active or _decode(active) not in ("1", "true", "True"):
        return Response(status_code=204)

    dev_queue = f"{DEVICE_QUEUE_PREFIX}{device_id}"

    # Try device queue first, then global, with a shared timeout budget
    # We use two BRPOP attempts, each with half of LONGPOLL_SECONDS.
    t_half = max(1, LONGPOLL_SECONDS // 2)

    item = R.brpop(dev_queue, timeout=t_half)
    if not item:
        item = R.brpop(GLOBAL_FALLBACK_QUEUE, timeout=t_half)

    if not item:
        return Response(status_code=204)

    # item = (queue_key, job_bytes)
    _, job_bytes = item
    try:
        payload = json.loads(job_bytes)
    except Exception:
        payload = {"raw": _decode(job_bytes)}

    return {"job": payload}
