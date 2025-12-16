
# /opt/xauapi/api/main.py

from fastapi import FastAPI, Request, UploadFile, File, HTTPException, BackgroundTasks, Depends, status, Header, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse,HTMLResponse
from pathlib import Path
import os, time, json, uuid, redis, asyncio, anyio,gzip,base64
from datetime import datetime, timezone, timedelta
from io import BytesIO
import zipfile
from api.routes_auth import r as auth_router
from api.routes_devices import r as devices_router
from api.deps import get_current_user, require_admin,get_current_user_relaxed
from api.routes_worker import r as worker_router
from starlette.middleware.sessions import SessionMiddleware
from api.routes_oidc import r as google_oidc_router
from api.routes_mfa import r as mfa_router
from api.routes_oidc import r as oidc_router
from api.security import require_auth_and_mfa
from api.routes.predict import router as predict_router
import logging
import hashlib
from api.security import require_user_relaxed
from api.trend import router as trend_router
from api.strategy_endpoints import router as strategy_router


try:
    from api.routes_admin import r as admin_router
except Exception:
    admin_router = None
log = logging.getLogger("uvicorn.error")



# ----------------- ENV -----------------
import os, logging
log = logging.getLogger("uvicorn.error")

# Optional .env loaders for dev/local (won't override systemd)
try:
    from dotenv import load_dotenv
    # 1) local dev file in repo
    load_dotenv("runtime.env", override=False)
    # 2) server-level env (prod/staging) – do NOT override existing env
    if os.path.exists("/etc/xauapi.env"):
        load_dotenv("/etc/xauapi.env", override=False)
except Exception:
    pass  # python-dotenv not required in prod

def _split_csv(val: str | None) -> list[str]:
    return [s.strip() for s in (val or "").split(",") if s.strip()]

APP_ENV = os.getenv("APP_ENV", "prod")

# URLs (must be provided by env in prod)
PUBLIC_API_BASE = os.getenv("PUBLIC_API_BASE", "")
API_BASE        = os.getenv("API_BASE", "http://127.0.0.1:8000")

# Strongly prefer explicit REDIS_URL from env; no risky defaults.
REDIS_URL = os.getenv("REDIS_URL")
if not REDIS_URL:
    raise RuntimeError("REDIS_URL not set. Configure it in /etc/xauapi.env or runtime.env")

QUEUE_KEY   = os.getenv("QUEUE_KEY", "mt5-jobs")
JOB_NS      = os.getenv("JOB_HASH_NS", "job")

OUT_DIR     = Path(os.getenv("OUT_DIR", "/opt/xauapi/out"))
OUT_DIR.mkdir(parents=True, exist_ok=True)

PAIR_TTL_SEC = int(os.getenv("PAIR_TTL_SEC", "600"))
PREPROVISION_IN_BUNDLE   = os.getenv("PREPROVISION_IN_BUNDLE", "1") == "1"
DASHBOARD_ACTIVE_MINUTES = int(os.getenv("DASHBOARD_ACTIVE_MINUTES", "20"))
DEVICE_RETENTION_DAYS    = int(os.getenv("DEVICE_RETENTION_DAYS", "30"))
CLEANUP_AGE_DAYS         = int(os.getenv("CLEANUP_AGE_DAYS", "14"))
SINGLE_ACTIVE_PER_HOST   = int(os.getenv("SINGLE_ACTIVE_PER_HOST", "1"))
EPOCH_UTC = datetime(1970, 1, 1, tzinfo=timezone.utc)

# Secrets / sessions
SESSION_SECRET = os.getenv("SESSION_SECRET")
if not SESSION_SECRET:
    # Dev-only fallback to avoid crashing locally
    import secrets
    SESSION_SECRET = secrets.token_urlsafe(48)
    log.warning("[BOOT] SESSION_SECRET missing; using ephemeral dev secret (local)")

# CORS
CORS_ORIGINS = _split_csv(os.getenv("CORS_ORIGINS", ""))




# Fallback/global queue name (can override via env)
GLOBAL_FALLBACK_QUEUE = os.getenv("GLOBAL_FALLBACK_QUEUE", "queue:global")

def gqueue() -> str:
    return GLOBAL_FALLBACK_QUEUE

def hostkey(host_id: str) -> str:
    return f"host:{host_id}:devices"
def _b2s(x):
    return x.decode() if isinstance(x, (bytes, bytearray)) else x

# Sessions
app = FastAPI(docs_url="/_api/docs", openapi_url="/_api/openapi.json")
from starlette.middleware.base import BaseHTTPMiddleware

class _StripApiPrefix(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        path = request.scope.get("path", "")

        # Do NOT rewrite FastAPI's own docs endpoints
        if path in ("/_api/docs", "/_api/redoc") or path.startswith("/_api/openapi"):
            return await call_next(request)

        # Rewrite everything else that starts with /_api/
        if path.startswith("/_api/"):
            request.scope["path"] = path[5:] or "/"

        return await call_next(request)

app.add_middleware(_StripApiPrefix)


app.add_middleware(SessionMiddleware, secret_key=SESSION_SECRET)


# CORS
if CORS_ORIGINS:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=CORS_ORIGINS,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )


SESSION_SECRET = os.getenv("SESSION_SECRET")
log.info("[BOOT] session_secret_digest=%s",
         hashlib.sha256(SESSION_SECRET.encode()).hexdigest()[:12])
assert SESSION_SECRET, "SESSION_SECRET not set"
app.add_middleware(
    SessionMiddleware,
    secret_key=SESSION_SECRET,
    session_cookie="session",
    same_site="none",
    https_only=True,                 # set True in prod (you are on HTTPS)
    max_age=2592000,       # 30 days
    domain=".xautrendlab.com",       # share across subdomains
)

# --- Public (no session required) ---
app.include_router(auth_router, tags=["auth"])            # /user/login, /user/signup, /auth/options
app.include_router(oidc_router, tags=["auth"])            # /auth/oidc/*
app.include_router(google_oidc_router, tags=["auth"])     # /auth/oidc/google/*
app.include_router(devices_router, prefix="")
app.include_router(predict_router)

# --- Logged-in but MFA NOT required (for enrollment) ---
# All /user/mfa/* endpoints need a user session but should work before MFA is enabled.
app.include_router(mfa_router, tags=["auth"], dependencies=[Depends(get_current_user_relaxed)])
app.include_router(trend_router, tags=["trend"])
app.include_router(strategy_router, tags=["strategy"])


# --- Worker endpoints (device heartbeats, next job, etc.) ---
# These validate device headers themselves; do not gate with user session.
app.include_router(worker_router, tags=["worker"])


# --- Admin-only ---
if admin_router is not None:
    app.include_router(
        admin_router,
        tags=["admin"],
        dependencies=[Depends(require_admin)]
    )


# ----- Auth dependency -----
API_KEY = os.getenv("API_KEY", "").strip()


APP_ORIGIN = os.getenv("PUBLIC_API_BASE", "https://api.xautrendlab.com")

def _api_key_from_headers(request: Request) -> str:
    # Prefer X-API-Key; fall back to Authorization: Bearer <key>
    key = (request.headers.get("X-API-Key") or "").strip()
    if key:
        return key
    auth = (request.headers.get("Authorization") or "").strip()
    if auth.lower().startswith("bearer "):
        return auth[7:].strip()
    return ""

def require_api_key(request: Request):
    expected = os.getenv("API_KEY", "")
    provided = (
        request.headers.get("x-api-key")
        or request.cookies.get("api_key")
        or request.query_params.get("api_key")  # optional, helps testing
    )
    if not expected or provided != expected:
        raise HTTPException(status_code=401, detail="Authentication required.")
from fastapi.responses import RedirectResponse, HTMLResponse

@app.get("/admin/login")
def admin_login(key: str = ""):
    expected = os.getenv("API_KEY", "")
    if not expected:
        return HTMLResponse("API_KEY not configured", status_code=500)
    if key != expected:
        return HTMLResponse("Bad key", status_code=400)
    resp = RedirectResponse(url="/dashboard", status_code=302)
    # HttpOnly=false so browser JS sends it automatically; keep behind Access/HTTPS
    resp.set_cookie("api_key", key, max_age=86400, secure=True, httponly=False, samesite="Lax")
    return resp
@app.get("/admin/logout")
def admin_logout():
    resp = RedirectResponse(url="/dashboard", status_code=302)
    resp.delete_cookie("api_key", path="/")
    return resp

# Redis client
r = redis.Redis.from_url(REDIS_URL, decode_responses=True)

def jkey(job_id: str) -> str:
    return f"{JOB_NS}:{job_id}"

def jchan(job_id: str) -> str:
    return f"job:{job_id}:events"

def now_ts() -> int:
    return int(time.time())



def _sse_event(event: str, data: str) -> str:
    # SSE framing; data MUST be a single line (we publish JSON strings).
    return f"event: {event}\ndata: {data}\n\n"

def _publish_update(job_id: str, **fields):
    """
    Persist fields into job hash and publish a single-line JSON update on the job channel.
    All values are stored as strings in Redis hash.
    """
    fields.setdefault("job_id", job_id)
    fields["updated_at"] = str(now_ts())
    # persist
    r.hset(jkey(job_id), mapping={k: ("" if v is None else str(v)) for k, v in fields.items()})
    # publish
    r.publish(jchan(job_id), json.dumps(fields))

def _auth_ok(authorization: str | None, x_api_key: str | None, key_query: str | None) -> bool:
    # Accept Authorization: Bearer <token> OR X-API-Key header OR ?key=<token>
    token = None
    if authorization:
        parts = authorization.split()
        if len(parts) == 2 and parts[0].lower() == "bearer":
            token = parts[1]
    if not token and x_api_key:
        token = x_api_key
    if not token and key_query:
        token = key_query
    return bool(API_KEY and token == API_KEY)


# ----------------- ROUTES -----------------
@app.get("/healthz")
def healthz():
    try:
        r.ping()
        return {"ok": True}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.post("/mt5/test", dependencies=[Depends(require_api_key)])
async def mt5_test(req: Request):
    """
    Enqueue a 'test_mt5' job for a specific device. No credentials are sent to the server;
    the worker uses its local provision.json for MT5 login/server.
    Body: { "device_id": "dev_xxx" }
    """
    body = await req.json()
    device_id = str(body.get("device_id", "")).strip()
    if not device_id:
        raise HTTPException(400, "device_id required")

    job_id = str(uuid.uuid4())
    created = now_ts()
    payload = {
        "type": "test_mt5",
        "job_id": job_id,
        "created_at": created,
        "device_id": device_id,
    }

    r.rpush(dqueue(device_id), json.dumps(payload))
    _publish_update(
        job_id,
        status="queued",
        progress="0.00",
        message="Testing MT5 connectivity",
        out_csv="",
        out_csv_api="",
        created_at=str(created),
        finished_at=""
    )
    return {"job_id": job_id}

@app.post("/backtest", dependencies=[Depends(require_api_key)])
async def create_backtest(req: Request):
    """
    Enqueue a backtest job. Expecting JSON body like:
    {
      "symbol": "XAUUSD",
      "tz": "Asia/Kolkata",
      "start": "2025-09-16",
      "end":   "2025-09-16",
      "assumed_spread": 0.2,
      // optional:
      // "device_id": "dev_xxx",               <-- NEW: target a specific device queue
      // "login": 123456,
      // "password": "...",
      // "server": "RoboForex-Pro",
      // "terminal_path": "C:\\...\\terminal64.exe",
      // "out_csv": "D:\\Algo\\Gold\\out\\my.csv"
    }
    """
    body = await req.json()
    required = ["symbol","tz","start","end","assumed_spread"]
    for k in required:
        if k not in body:
            raise HTTPException(400, f"Missing field: {k}")

    job_id = str(uuid.uuid4())
    created = now_ts()

    # optional device targeting
    device_id = str(body.get("device_id", "")).strip() or None

    payload = {
        "type": "backtest",
        "job_id": job_id,
        "created_at": created,
        "symbol": body["symbol"],
        "tz": body["tz"],
        "start": body["start"],
        "end": body["end"],
        "assumed_spread": float(body["assumed_spread"]),
    }
    if device_id:
        payload["device_id"] = device_id  # useful for auditing/metrics

    # pass-through optionals for MT5 session/runner
    for k in ("login","password","server","terminal_path","out_csv"):
        if k in body:
            payload[k] = body[k]

    # queue selection: per-device queue if provided, else the shared queue
    queue_key = dqueue(device_id) if device_id else QUEUE_KEY
    r.rpush(queue_key, json.dumps(payload))

    # initialize full snapshot and publish immediately for SSE clients
    _publish_update(job_id,
        status="queued",
        progress="0.00",
        message="Queued",
        out_csv="",
        out_csv_api="",
        created_at=str(created),
        finished_at=""
    )

    return {"job_id": job_id}


@app.get("/job/{job_id}", dependencies=[Depends(require_api_key)])
def get_job(job_id: str, since: int | None = None):
    key = jkey(job_id)
    data = r.hgetall(key)
    if not data:
        raise HTTPException(404, "Unknown job_id")

    # updated_at: unix seconds — maintain this everywhere we hset()
    updated_at = int(data.get("updated_at") or 0)
    if since is not None and updated_at <= int(since):
        return Response(status_code=204)

    return {
        "job_id": job_id,
        "status": data.get("status", "unknown"),
        "progress": data.get("progress", ""),
        "out_csv": data.get("out_csv", ""),
        "out_csv_api": data.get("out_csv_api", ""),
        "error": data.get("error", ""),
        "message": data.get("message", ""),
        "created_at": data.get("created_at", ""),
        "finished_at": data.get("finished_at", ""),
        "updated_at": str(updated_at),
        # convenience for the UI: enable button as soon as API path is present
        "ready": bool(data.get("out_csv_api")),
    }


def _require_any_token(
    authorization: str | None = Header(default=None),
    x_api_key: str | None = Header(default=None),
):
    """
    Allow either:
      - API key (Authorization: Bearer <API_KEY> or X-API-Key: <API_KEY>)
      - Device token (Authorization: Bearer <device_token>)
    """
    # API key via header
    if API_KEY:
        if x_api_key == API_KEY:
            return "api"
        if authorization:
            parts = authorization.split()
            if len(parts) == 2 and parts[0].lower() == "bearer" and parts[1] == API_KEY:
                return "api"

    # Device token path
    if authorization:
        parts = authorization.split()
        if len(parts) == 2 and parts[0].lower() == "bearer":
            tok = parts[1]
            if r.get(tkey(tok)):   # maps to a device
                return "device"

    raise HTTPException(status_code=401, detail="Authentication required.")


@app.post("/upload/{job_id}", dependencies=[Depends(_require_any_token)])
async def upload_csv(job_id: str, file: UploadFile = File(...)):
    meta = r.hgetall(jkey(job_id))
    if not meta:
        raise HTTPException(404, "Unknown job_id")

    dest = OUT_DIR / f"{job_id}.csv"
    try:
        with dest.open("wb") as f:
            while True:
                chunk = await file.read(1024 * 1024)  # 1MB
                if not chunk:
                    break
                f.write(chunk)
    finally:
        await file.close()

    # Expose via API route, not a filesystem path
    api_path = f"/download/{job_id}"

    # Update hash + publish update so the UI enables the button immediately
    _publish_update(job_id,
        status=meta.get("status", "running"),   # do not force "done" here; worker will
        message="File uploaded",
        out_csv=str(dest),
        out_csv_api=api_path
    )

    return {"saved": True, "path": str(dest), "out_csv_api": api_path}


def _check_api_key_from_query(key: str | None):
    # for SSE only (EventSource often cannot set headers)
    if not API_KEY or key != API_KEY:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required.",
        )


@app.get("/events/{job_id}")
def job_events(job_id: str, key: str | None = None):
    """
    Server-Sent Events for a single job_id.
    UI connects once and receives JSON lines whenever the worker publishes.
    Auth: ?key=<API_KEY> (EventSource may not send headers)
    """
    _check_api_key_from_query(key)
    channel = jchan(job_id)
    pubsub = r.pubsub()
    pubsub.subscribe(channel)

    async def gen():
        try:
            # 1) Emit the current snapshot immediately so the UI can enable
            snap = r.hgetall(jkey(job_id))
            if snap:
                # add 'ready' convenience like /job
                snap["ready"] = bool(snap.get("out_csv_api"))
                yield _sse_event("update", json.dumps(snap))

            # 2) Stream pubsub updates
            last_heartbeat = time.time()
            while True:
                # redis-py pubsub.get_message is blocking; offload to a thread
                msg = await anyio.to_thread.run_sync(lambda: pubsub.get_message(ignore_subscribe_messages=True, timeout=30.0))
                if msg and msg.get("type") == "message":
                    data = msg["data"]
                    if isinstance(data, bytes):
                        data = data.decode("utf-8")
                    # normalize: add ready flag if not present
                    try:
                        obj = json.loads(data)
                        if "ready" not in obj:
                            obj["ready"] = bool(obj.get("out_csv_api"))
                        data = json.dumps(obj)
                    except Exception:
                        pass
                    yield _sse_event("update", data)
                    last_heartbeat = time.time()
                else:
                    # heartbeat every ~15s
                    if time.time() - last_heartbeat > 15:
                        yield _sse_event("ping", json.dumps({"t": now_ts()}))
                        last_heartbeat = time.time()
                await asyncio.sleep(0)
        finally:
            try:
                pubsub.unsubscribe(channel)
            except Exception:
                pass
            pubsub.close()

    headers = {
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",  # for proxies like nginx
    }
    return StreamingResponse(gen(), media_type="text/event-stream", headers=headers)


@app.get("/download/{job_id}", dependencies=[Depends(require_api_key)])
def download_csv(job_id: str, bg: BackgroundTasks):
    """
    Streams the CSV (if available) and schedules deletion right after the response is sent.
    """
    info = r.hgetall(jkey(job_id)) or {}
    api_path = info.get("out_csv_api")
    fs_path  = info.get("out_csv")  # stored by /upload
    if not api_path or not fs_path or not os.path.exists(fs_path):
        raise HTTPException(404, "File not available")

    def _cleanup(p: str, key: str):
        try:
            os.remove(p)
        except FileNotFoundError:
            pass
        r.hdel(jkey(key), "out_csv_api")
        r.hdel(jkey(key), "out_csv")

    bg.add_task(_cleanup, fs_path, job_id)
    filename = os.path.basename(fs_path)
    return FileResponse(fs_path, media_type="text/csv", filename=filename)
# ----------------- BYO-WORKER ENDPOINTS -----------------
# Model: Pair on the website ? worker claims with pairing code ? gets a device_token for auth.
# Jobs for a device go to queue:device:{device_id} so only that user/device can pull them.

from pydantic import BaseModel
import secrets
from fastapi import Depends

PAIR_TTL_SEC = 10 * 60       # pairing code valid 10 minutes
DEVICE_TOKEN_TTL_SEC = 30 * 24 * 3600  # (optional) if you later add TTL/refresh

def dkey(dev_id: str) -> str: return f"device:{dev_id}"
def tkey(tok: str) -> str:    return f"devtoken:{tok}"
def pkey(code: str) -> str:   return f"paircode:{code}"
def dqueue(dev_id: str) -> str: return f"queue:device:{dev_id}"
def ikey(dev_id): return f"incidents:{dev_id}"
def devices_set(): return "devices"
def _now_utc(): return datetime.now(timezone.utc)



class Incident(BaseModel):
    device_id: str
    kind: str
    message: str
    ts: str | None = None
    log_tail_gz_b64: str | None = None


@app.post("/worker/incident")
def worker_incident(inc: Incident):
    dev_id = inc.device_id
    inc.ts = inc.ts or _now_iso()
    item = inc.model_dump()
    lst = ikey(dev_id)
    r.lpush(lst, json.dumps(item))
    r.ltrim(lst, 0, 49)  # keep last 50
    # also keep last error on device record
    r.hset(dkey(dev_id), mapping={"last_error": inc.message, "last_error_at": inc.ts})
    return {"ok": True}

def _status_from(h: dict) -> str:
    age = _age_seconds(h.get("last_heartbeat", ""))
    if age > DASHBOARD_ACTIVE_MINUTES * 60:
        return "offline"
    if h.get("mt5_ok") == "1" and h.get("api_ok") == "1" and h.get("autostart_ok") == "1":
        return "healthy"
    return "degraded"

def _age_seconds(iso_ts: str) -> float:
    try:
        t = datetime.fromisoformat(iso_ts.replace("Z", "+00:00"))
        return (_now_utc() - t).total_seconds()
    except Exception:

        return 1e12


def _parse_iso_safe(ts: str | None) -> datetime:
    """Parse ISO-8601 safely. Returns epoch UTC on any issue."""
    if not ts:
        return EPOCH_UTC
    s = str(ts).strip()
    if not s:
        return EPOCH_UTC
    # Accept trailing Z
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except Exception:
        # Best-effort fallback: trim to seconds and assume UTC
        try:
            dt = datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
        except Exception:
            return EPOCH_UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt

def _status_of(rec: dict) -> str:
    """Return one of: healthy / degraded / offline / not_in_use. Never throws."""
    # inactive wins
    if str(rec.get("active", "1")) != "1":
        return "not_in_use"

    last = _parse_iso_safe(rec.get("last_heartbeat"))
    fresh = (datetime.now(timezone.utc) - last) <= timedelta(minutes=DASHBOARD_ACTIVE_MINUTES)
    if not fresh:
        return "offline"

    mt5_ok       = str(rec.get("mt5_ok", "0")).lower() in ("1", "true")
    api_ok       = str(rec.get("api_ok", "1")).lower() in ("1", "true")
    autostart_ok = str(rec.get("autostart_ok", "0")).lower() in ("1", "true")
    return "healthy" if (mt5_ok and api_ok and autostart_ok) else "degraded"


@app.get("/devices")
def list_devices(active_only: int = 0):
    items = []
    all_ids = r.smembers(devices_set()) or []

    for raw in all_ids:
        dev_id = _b2s(raw)
        h = r.hgetall(dkey(dev_id)) or {}

        # decode bytes -> str if needed
        if h and isinstance(next(iter(h.keys()), ""), (bytes, bytearray)):
            h = { _b2s(k): _b2s(v) for k, v in h.items() }

        # derive status safely
        st = _status_of(h)

        # filter: active_only=1 hides offline rows
        if active_only and st == "offline":
            continue

        items.append({
            "device_id": dev_id,
            "label": h.get("label", ""),
            "version": h.get("version", ""),
            "mt5_ok": h.get("mt5_ok", "0"),
            "api_ok": h.get("api_ok", "1"),
            "autostart_ok": h.get("autostart_ok", "0"),
            "last_heartbeat": h.get("last_heartbeat", ""),
            "last_error": h.get("last_error", h.get("last_error_code", "")),
            "status": st,
            "active": h.get("active", "1"),
        })

    # newest heartbeat first (empty strings sort last)
    items.sort(key=lambda x: x["last_heartbeat"] or "", reverse=True)
    return {"devices": items}


def _delete_device(device_id: str):
    h = r.hgetall(dkey(device_id)) or {}
    tok = h.get("token")
    uid = h.get("user_id")
    pipe = r.pipeline()
    pipe.delete(dkey(device_id))
    if tok: pipe.delete(f"token:{tok}")
    pipe.srem(devices_set(), device_id)
    if uid: pipe.srem(f"user_devices:{uid}", device_id)
    pipe.delete(ikey(device_id))  # incidents
    pipe.execute()

@app.delete("/devices/{device_id}")
def delete_device_api(device_id: str, _=Depends(require_api_key)):
    if not r.exists(dkey(device_id)):
        raise HTTPException(404, "Not found")
    _delete_device(device_id)
    return {"deleted": True}
def _parse_iso(ts: str | None):
    if not ts: return None
    try: return datetime.fromisoformat(ts.replace("Z","+00:00"))
    except Exception: return None
@app.post("/admin/cleanup-devices")
def cleanup_devices(_=Depends(require_api_key)):
    retention_days = int(os.getenv("DEVICE_RETENTION_DAYS", "14"))
    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
    removed = []
    for dev_id in list(r.smembers(devices_set())):
        h  = r.hgetall(dkey(dev_id)) or {}
        hb = _parse_iso(h.get("last_heartbeat"))
        ca = _parse_iso(h.get("created_at"))
        last_active = hb or ca
        if last_active and last_active < cutoff:
            _delete_device(dev_id)
            removed.append(dev_id)
    return {"deleted": removed}
@app.post("/admin/cleanup-devices")
def admin_cleanup(_: Request, __=Depends(require_api_key)):
    cutoff = datetime.now(timezone.utc) - timedelta(days=CLEANUP_AGE_DAYS)
    deleted = []
    for raw in r.smembers(devices_set()) or []:
        dev_id = raw.decode() if isinstance(raw,(bytes,bytearray)) else str(raw)
        m = r.hgetall(dkey(dev_id))
        # choose last_heartbeat or created_at
        ts = (m.get("last_heartbeat") or m.get("created_at") or "").strip()
        try:
            dt = datetime.fromisoformat(ts)
            if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
        except Exception:
            dt = datetime(1970,1,1,tzinfo=timezone.utc)
        if dt < cutoff:
            # remove host mapping if present
            host_id = (m.get("host_id") or "").strip()
            if host_id:
                r.srem(f"host:{host_id}:devices", dev_id)
            # delete device record
            r.delete(dkey(dev_id))
            r.srem(devices_set(), dev_id)
            deleted.append(dev_id)
    return {"deleted": deleted}

@app.get("/devices")
def list_devices():
    out = []
    for dev_id in sorted(r.smembers(devices_set())):
        h = r.hgetall(dkey(dev_id)) or {}
        out.append({
            "device_id": dev_id,
            "status": _status_from(h),
            "last_heartbeat": h.get("last_heartbeat",""),
            "version": h.get("version",""),
            "mt5_ok": h.get("mt5_ok")=="1",
            "autostart_ok": h.get("autostart_ok")=="1",
            "last_error": h.get("last_error",""),
            "mt5_path": h.get("mt5_path",""),
        })
    return {"devices": out}

@app.get("/devices/{device_id}")
def device_detail(device_id: str):
    h = r.hgetall(dkey(device_id)) or {}
    h["status"] = _status_from(h)
    return h

@app.get("/devices/{device_id}/incidents")
def device_incidents(device_id: str, limit: int = 20):
    items = r.lrange(ikey(device_id), 0, max(0, limit-1))
    return [json.loads(x) for x in items]

from fastapi.responses import HTMLResponse

@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(_: Request):
    return HTMLResponse("""
<!doctype html><meta charset="utf-8"><title>XTL Devices</title>
<style>
  body{font-family:system-ui,Segoe UI,Arial;margin:20px}
  h2{margin:0 0 12px}
  .controls{margin:10px 0 14px; display:flex; gap:12px; align-items:center}
  table{border-collapse:collapse;width:100%}
  th,td{border:1px solid #ddd;padding:8px} th{background:#f5f5f5;text-align:left}
  .dot{display:inline-block;width:10px;height:10px;border-radius:50%;margin-right:6px;vertical-align:middle}
  .healthy{background:#16a34a}.degraded{background:#f59e0b}.offline{background:#ef4444}.not_in_use{background:#9ca3af}
  .sub{color:#777;font-size:12px}
  button{padding:6px 10px;border:1px solid #ccc;border-radius:6px;background:#fff;cursor:pointer}
</style>

<h2>Devices</h2>
<div class="controls">
  <label><input id="showOffline" type="checkbox" checked> Show offline</label>
  <button id="cleanupBtn">Cleanup old (&gt; 14d)</button>
</div>

<table id="t"><thead><tr>
  <th>Status</th><th>Device</th><th>Version</th><th>MT5</th><th>Last heartbeat</th><th>Last error</th><th>Actions</th>
</tr></thead><tbody></tbody></table>

<script>
function statusClass(d){
  // prefer explicit status; otherwise infer
  if(d.status) return d.status;
  if(String(d.active) !== '1') return 'not_in_use';
  if(d.mt5_ok && d.api_ok && d.autostart_ok) return 'healthy';
  return (d.last_heartbeat ? 'degraded' : 'offline');
}
function statusText(d){ return statusClass(d).replace('_',' '); }
function mt5Text(d){ return (String(d.mt5_ok)==='1'||d.mt5_ok===true) ? 'OK' : '—'; }

async function load(){
  // when checked, include offline (active_only=0). When unchecked, show active only (active_only=1).
  const showOffline = document.querySelector('#showOffline').checked ? 0 : 1;
  const res = await fetch(`/devices?active_only=${showOffline}`);
  if(!res.ok){ alert('Failed to load: '+res.status); return; }
  const data = await res.json();
  const devices = data.devices || data; // support both shapes
  const tbody = document.querySelector('#t tbody'); tbody.innerHTML='';

  for(const d of devices){
    const tr = document.createElement('tr');
    const name = d.label || d.device_id;
    const lastErr = (d.last_error || d.last_error_code || '') + '';
    tr.innerHTML = `
      <td><span class="dot ${statusClass(d)}"></span>${statusText(d)}</td>
      <td><div><strong>${name}</strong></div><div class="sub">${d.device_id}</div></td>
      <td>${d.version || ''}</td>
      <td>${mt5Text(d)}</td>
      <td>${d.last_heartbeat || '—'}</td>
      <td>${lastErr.slice(0,120)}</td>
      <td>
        <button onclick="selftest('${d.device_id}')">Run self-test</button>
        <button onclick="inc('${d.device_id}')">Incidents</button>
        <button onclick="delDev('${d.device_id}')">Delete</button>
      </td>`;
    tbody.appendChild(tr);
  }
}

async function selftest(deviceId){
  try{
    const r = await fetch('/mt5/test', {
      method:'POST',
      headers:{ 'Content-Type':'application/json' }, // cookie carries auth
      body: JSON.stringify({ device_id: deviceId })
    });
    if(!r.ok){ const t = await r.text().catch(()=> ''); alert('Enqueue failed: '+r.status+' '+t); return; }
    const { job_id } = await r.json();
    alert('Self-test started: '+job_id);
  }catch(e){ alert('Error: '+e); }
}

async function inc(id){
  const r = await fetch(`/devices/${id}/incidents?limit=10`);
  if(!r.ok){ alert('Load incidents failed: '+r.status); return; }
  const items = await r.json();
  alert((items||[]).map(x=>`[${x.ts}] ${x.kind}: ${x.message}`).join('\\n') || 'No incidents');
}

async function delDev(id){
  if(!confirm('Delete device '+id+'?')) return;
  const r = await fetch('/devices/'+id, { method:'DELETE' });
  if(!r.ok){ alert('Delete failed: '+r.status+' '+await r.text()); return; }
  load();
}

document.querySelector('#showOffline').onchange = load;
document.querySelector('#cleanupBtn').onclick = async () => {
  const r = await fetch('/admin/cleanup-devices', { method:'POST' }); // cookie auth
  if(!r.ok){ alert('Cleanup failed: '+r.status+' '+await r.text()); return; }
  const data = await r.json();
  alert('Deleted: '+ (Array.isArray(data.deleted) ? data.deleted.length : (data.deleted||0)));
  load();
};

load(); setInterval(load, 10000);
</script>
""")




def cmdkey(device_id): return f"cmds:{device_id}"

@app.post("/devices/{device_id}/command")
def device_command(device_id: str, action: str):
    # action: "selftest" | "open_mt5" | "reinstall_autostart" | ...
    r.rpush(cmdkey(device_id), json.dumps({"action": action, "ts": _now_iso()}))
    return {"queued": True}

@app.get("/worker/command")
def worker_command(request: Request, device_id: str):
    # The worker polls this every ~30s
    # pop one command (FIFO). If none, return empty.
    raw = r.lpop(cmdkey(device_id))
    if not raw:
        return {"command": None}
    return {"command": json.loads(raw)}



# ---- Helpers ----
def gen_code(n=8) -> str:
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    return "".join(secrets.choice(alphabet) for _ in range(n))

def new_id(prefix="dev") -> str:
    return f"{prefix}_{uuid.uuid4().hex[:10]}"


def _auth_device_token(authorization: str | None = Header(default=None)) -> dict:
    """
    Accepts Authorization: Bearer <device_token>
    Returns the device hash as dict on success.
    """
    if not authorization:
        raise HTTPException(401, "Missing Authorization")
    parts = authorization.split()
    if len(parts) != 2 or parts[0].lower() != "bearer":
        raise HTTPException(401, "Bad Authorization header")
    token = parts[1]
    dev_id = r.get(tkey(token))
    if not dev_id:
        raise HTTPException(401, "Invalid device token")
    info = r.hgetall(dkey(dev_id))
    if not info:
        raise HTTPException(401, "Unknown device")
    info["device_id"] = dev_id
    info["device_token"] = token
    return info

def new_device_for_user(user_id: str, name: str | None) -> tuple[str, str]:
    dev_id = new_id("dev")
    token = secrets.token_urlsafe(32)
    r.hset(dkey(dev_id), mapping={
        "user_id": user_id,
        "name": name or dev_id,
        "created_at": str(now_ts()),
        "last_heartbeat": "0",
        "status": "provisioned",
        "token": token,
    })
    r.set(tkey(token), dev_id)
    return dev_id, token


def render_status(rec: dict[str, str]) -> str:
    # online/offline as you already do (using last_heartbeat age) ...
    online = ...  # your existing calc
    if not online: return "offline"
    if str(rec.get("active", "1")) != "1":  # NEW
        return "not_in_use"
    healthy = (rec.get("mt5_ok")=="1" and rec.get("api_ok")=="1" and rec.get("autostart_ok")=="1")
    return "healthy" if healthy else "degraded"


# ---- 1) Website generates pairing code ----
class PairingCreate(BaseModel):
    user_id: str
    device_name: str | None = None

@app.post("/worker/pairing-codes", dependencies=[Depends(require_api_key)])
def create_pairing_code(req: PairingCreate):
    code = gen_code(8)
    code_id = new_id("pc")
    expires = now_ts() + PAIR_TTL_SEC
    r.hset(pkey(code), mapping={
        "code_id": code_id, "user_id": req.user_id, "device_name": req.device_name or "",
        "expires_at": str(expires), "used": "0"
    })
    r.expire(pkey(code), PAIR_TTL_SEC + 60)  # cleanup
    return {"code": code, "code_id": code_id, "expires_in": PAIR_TTL_SEC}

# ---- 2) Worker claims pairing code ? gets device token ----
class ClaimReq(BaseModel):
    code: str
    device_name: str | None = None

@app.post("/worker/claim")
def worker_claim(req: ClaimReq):
    rec = r.hgetall(pkey(req.code))
    if not rec:
        raise HTTPException(400, "Invalid or expired code")
    if rec.get("used") == "1":
        raise HTTPException(400, "Code already used")
    if now_ts() > int(rec.get("expires_at", "0")):
        raise HTTPException(400, "Code expired")

    dev_id = new_id("dev")
    token = secrets.token_urlsafe(32)
    r.hset(dkey(dev_id), mapping={
        "user_id": rec["user_id"],
        "name": req.device_name or rec.get("device_name") or dev_id,
        "created_at": str(now_ts()),
        "last_heartbeat": "0",
	"status": "paired",
        "token": token,
    })
    r.set(tkey(token), dev_id)
    r.hset(pkey(req.code), "used", "1")
    return {"device_id": dev_id, "device_token": token}

# --- /device-login: mints device token and redirects to the installer (no UI/paste)
# --- add near your other imports
import os, uuid, secrets
from datetime import datetime, timezone
from urllib.parse import urlencode
from fastapi import Request
from fastapi.responses import RedirectResponse, HTMLResponse


def _now_iso():
    return datetime.now(tz=timezone.utc).isoformat()

# helper keys (or reuse your own)
# helpers (one place only)
def dkey(dev_id: str) -> str: return f"device:{dev_id}"
def tkey(tok: str)    -> str: return f"devtoken:{tok}"   # <— canonical

from urllib.parse import urlencode, urlparse
from fastapi.responses import RedirectResponse, HTMLResponse

@app.get("/device-login")
def device_login(request: Request, redirect_uri: str, client: str = "xtl_installer"):
    # allow only loopback redirects
    try:
        u = urlparse(redirect_uri)
        if (u.scheme not in ("http","https")) or (u.hostname not in ("127.0.0.1","localhost")):
            return HTMLResponse("Invalid redirect_uri (must be 127.0.0.1/localhost)", status_code=400)
    except Exception:
        return HTMLResponse("Bad redirect_uri", status_code=400)

    device_id    = "dev_" + uuid.uuid4().hex
    device_token = secrets.token_urlsafe(32)

    r.hset(dkey(device_id), mapping={
        "client": client,
        "token": device_token,
        "created_at": _now_iso(),
        "last_heartbeat": "",
        "status": "paired",
        "ip": (request.client.host if request.client else ""),
        "version": "",
        "label": f"Device {device_id[-6:]}",
        "active": "1",
        "host_id": "",
    })
    r.sadd("devices", device_id)
    r.set(tkey(device_token), device_id)

    api_base = APP_ORIGIN.rstrip("/")
    qs = urlencode({"device_token": device_token, "device_id": device_id, "api_base": api_base})
    cb = f"{redirect_uri}{'&' if '?' in redirect_uri else '?'}{qs}"
    return RedirectResponse(cb, status_code=302)


# ---- 3) Worker heartbeat ----

class Heartbeat(BaseModel):
    device_id: str
    version: str | None = None
    uptime_s: int | None = 0
    mt5_ok: bool | None = False
    mt5_path: str | None = ""
    mt5_build: int | None = None
    autostart_ok: bool | None = False
    api_ok: bool | None = True
    last_job_status: str | None = None
    last_error_code: str | None = None
    platform: str | None = "windows"
    tz: str | None = ""

    # NEW
    host_id: str | None = None
    label: str | None = None


def _extract_device_auth(req: Request) -> tuple[str, str]:
    auth = (req.headers.get("authorization") or "").strip()
    xdev = (req.headers.get("x-device-token") or "").strip()
    tok = ""
    if auth.lower().startswith("bearer "):
        tok = auth[7:].strip()
    if not tok:
        tok = xdev
    dev_id = (req.headers.get("x-device-id") or "").strip()
    return dev_id, tok

def _validate_device(dev_id: str, tok: str) -> str:
    if not tok:
        raise HTTPException(status_code=401, detail="Missing device token")
    # Example SQL – adapt table/column names to your schema
    with db() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT id, token
            FROM devices
            WHERE token = %s
        """, (tok,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=401, detail="Invalid device token")
        db_dev_id, db_tok = str(row[0]), str(row[1])
        if dev_id and dev_id != db_dev_id:
            # header had an id but doesn't match token owner
            raise HTTPException(status_code=401, detail="Device id mismatch")
        return db_dev_id

@app.post("/worker/heartbeat")
def worker_heartbeat(hb: Heartbeat, request: Request):
    dev_id = (hb.device_id or "").strip()
    if not dev_id:
        raise HTTPException(status_code=422, detail="device_id required")

    key = dkey(dev_id)
    label = (hb.label or "").strip()[:64]  # cap to 64 chars

    # persist heartbeat
    r.hset(key, mapping={
        "last_heartbeat": _now_iso(),
        "version": hb.version or "",
        "uptime_s": int(hb.uptime_s or 0),
        "mt5_ok": 1 if hb.mt5_ok else 0,
        "mt5_path": hb.mt5_path or "",
        "mt5_build": int(hb.mt5_build or 0),
        "autostart_ok": 1 if hb.autostart_ok else 0,
        "api_ok": 1 if hb.api_ok else 0,
        "last_job_status": (hb.last_job_status or "idle"),
        "last_error_code": (hb.last_error_code or ""),
        "platform": hb.platform or "",
        "tz": hb.tz or "",
        "ip": (request.client.host if request.client else ""),
        "active": 1,
        "label": label,
        "host_id": (hb.host_id or ""),
    })
    r.sadd(devices_set(), dev_id)

    # optional: only one active device per host
    host_id = (hb.host_id or "").strip()
    if host_id and SINGLE_ACTIVE_PER_HOST:
        r.sadd(hostkey(host_id), dev_id)
        for raw in (r.smembers(hostkey(host_id)) or []):
            oid = raw.decode() if isinstance(raw, (bytes, bytearray)) else str(raw)
            if oid and oid != dev_id:
                r.hset(dkey(oid), "active", 0)

    return {"ok": True}


# ---- 4) Long-poll next job for this device ----

@app.post("/worker/next")
async def worker_next(dev=Depends(_auth_device_token)):
    dev_id = dev["device_id"]
    rec = r.hgetall(dkey(dev_id))
    if rec and str(rec.get("active", "1")) != "1":
        return Response(status_code=204)

    q_dev, q_glb = dqueue(dev_id), gqueue()
    start = time.time()
    while True:
        item = r.rpop(q_dev) or r.rpop(q_glb)
        if item:
            if isinstance(item, (bytes, bytearray)):
                item = item.decode("utf-8", "ignore")
            try:
                job = json.loads(item)
            except Exception:
                job = {"raw": item}
            job.setdefault("device_id", dev_id)
            return job
        if time.time() - start > 30:
            return Response(status_code=204)
        await asyncio.sleep(0.5)

# ---- 5) Worker progress/done hooks (publish SSE) ----
class ProgressReq(BaseModel):
    job_id: str
    status: str | None = None
    progress: float | None = None
    message: str | None = None
    out_csv_api: str | None = None
    error: str | None = None

@app.post("/worker/progress")
def worker_progress(p: ProgressReq, dev=Depends(_auth_device_token)):
    if not r.exists(jkey(p.job_id)):
        raise HTTPException(404, "Unknown job")
    fields = {}
    if p.status is not None: fields["status"] = p.status
    if p.progress is not None: fields["progress"] = f"{float(p.progress):.2f}"
    if p.message is not None: fields["message"] = p.message
    if p.out_csv_api is not None: fields["out_csv_api"] = p.out_csv_api
    if p.error is not None: fields["error"] = p.error
    fields["updated_at"] = str(now_ts())
    r.hset(jkey(p.job_id), mapping=fields)
    r.publish(jchan(p.job_id), json.dumps({"job_id": p.job_id, **fields}))
    return {"ok": True}

@app.post("/worker/done")
def worker_done(p: ProgressReq, dev=Depends(_auth_device_token)):
    if not r.exists(jkey(p.job_id)):
        raise HTTPException(404, "Unknown job")
    fields = {
        "status": p.status or "done",
        "progress": f"{float(p.progress or 100):.2f}",
        "message": p.message or "Done",
        "updated_at": str(now_ts()),
        "finished_at": str(now_ts())
    }
    if p.error: fields["error"] = p.error
    r.hset(jkey(p.job_id), mapping=fields)
    r.publish(jchan(p.job_id), json.dumps({"job_id": p.job_id, **fields}))
    return {"ok": True}

# add near other worker endpoints in /opt/xauapi/api/main.py
class DevicesResp(BaseModel):
    device_id: str
    name: str
    online: bool
    last_heartbeat: int
    preflight: dict | None = None

@app.get("/worker/devices", dependencies=[Depends(require_api_key)])
def list_devices(user_id: str):
    items: list[dict] = []
    for key in r.scan_iter("device:*"):
        info = r.hgetall(key)
        if not info or info.get("user_id") != user_id:
            continue
        dev_id = key.split(":", 1)[1]
        last = int(info.get("last_heartbeat") or 0)
        online = (now_ts() - last) <= 90
        pre = info.get("preflight")
        items.append({
            "device_id": dev_id,
            "name": info.get("name", dev_id),
            "online": online,
            "last_heartbeat": last,
            "preflight": json.loads(pre) if pre else None,
        })
    # online first
    items.sort(key=lambda d: (not d["online"], d["name"].lower()))
    return {"devices": items}


# --- Config where your binaries live on the API host ---
WORKER_BIN = Path(os.getenv("WORKER_BIN", "/opt/xauapi/bin/xtl-worker.exe"))
NSSM_BIN   = Path(os.getenv("NSSM_BIN",   "/opt/xauapi/bin/nssm.exe"))

class BundleExistingReq(BaseModel):
    user_id: str
    device_id: str

@app.post("/worker/bundle-existing", dependencies=[Depends(require_api_key)])
def worker_bundle_existing(req: BundleExistingReq, request: Request):
    api_base = (os.getenv("PUBLIC_API_BASE", "").strip()
                or str(request.base_url).rstrip("/"))
    info = r.hgetall(dkey(req.device_id))
    if not info or info.get("user_id") != req.user_id:
        raise HTTPException(404, "Unknown device")
    tok = info.get("token")
    if not tok:
        raise HTTPException(400, "Device has no stored token")

    mem = BytesIO()
    with zipfile.ZipFile(mem, "w", zipfile.ZIP_DEFLATED) as z:
        if WORKER_BIN.exists(): z.write(WORKER_BIN, arcname="xtl-worker.exe")
        if NSSM_BIN.exists():   z.write(NSSM_BIN,   arcname="nssm.exe")
        if WORKER_SCRIPT.exists(): z.write(WORKER_SCRIPT, arcname="xtl_worker.py")
        if START_BAT.exists():  z.write(START_BAT, arcname="start.bat")

        provision = {
            "api_base": api_base,
            "device_token": tok,
            "device_id": req.device_id,
            "device_name": info.get("name", req.device_id),
        }
        z.writestr("provision.json", json.dumps(provision, indent=2))
        z.writestr("README.txt", "Unzip and run start.bat\n")

    mem.seek(0)
    headers = {"Content-Disposition": f'attachment; filename=\"xtl-worker-{req.device_id}.zip\"'}
    return StreamingResponse(mem, media_type="application/zip", headers=headers)


# in /opt/xauapi/api/main.py (alongside other worker endpoints)
class TestMT5Req(BaseModel):
    user_id: str
    device_id: str

@app.post("/worker/test-mt5", dependencies=[Depends(require_api_key)])
def test_mt5_enqueue(req: TestMT5Req):
    job_id = str(uuid.uuid4())
    created = now_ts()
    payload = {
        "type": "test_mt5",
        "job_id": job_id,
        "created_at": created,
        "device_id": req.device_id,
    }
    r.rpush(dqueue(req.device_id), json.dumps(payload))

    # let the UI show something instantly
    _publish_update(job_id,
        status="queued",
        progress="0.00",
        message="Queued MT5 connectivity test",
        out_csv="",
        out_csv_api="",
        created_at=str(created),
        finished_at=""
    )
    return {"job_id": job_id}

class BundleReq(BaseModel):
    user_id: str
    device_name: str | None = None

from io import BytesIO
import zipfile
from fastapi.responses import StreamingResponse
WORKER_SCRIPT = Path("/opt/xauapi/bin/xtl_worker.py")
START_BAT     = Path("/opt/xauapi/bin/start.bat")

@app.post("/worker/bundle", dependencies=[Depends(require_api_key)])
def worker_bundle(req: BundleReq, request: Request):
    api_base = (os.getenv("PUBLIC_API_BASE", "").strip()
                or str(request.base_url).rstrip("/"))

    # Pre-provision device+token (no pairing UX) if enabled
    dev_id = token = None
    if PREPROVISION_IN_BUNDLE:
        dev_id, token = new_device_for_user(req.user_id, req.device_name)

    # Fallback pairing code (only used by very old workers)
    code = gen_code(8)
    code_id = new_id("pc")
    expires = now_ts() + PAIR_TTL_SEC
    r.hset(pkey(code), mapping={
        "code_id": code_id, "user_id": req.user_id, "device_name": req.device_name or "",
        "expires_at": str(expires), "used": "0",
    })
    r.expire(pkey(code), PAIR_TTL_SEC + 60)

    mem = BytesIO()
    with zipfile.ZipFile(mem, "w", zipfile.ZIP_DEFLATED) as z:
        if WORKER_BIN.exists():
            z.write(WORKER_BIN, arcname="xtl-worker.exe")
        if NSSM_BIN.exists():
            z.write(NSSM_BIN, arcname="nssm.exe")
        if WORKER_SCRIPT.exists():
            z.write(WORKER_SCRIPT, arcname="xtl_worker.py")
        else:
            z.writestr("xtl_worker.py", "# missing on server; contact admin\n")
        if START_BAT.exists():
            z.write(START_BAT, arcname="start.bat")
        else:
            z.writestr("start.bat",
                "@echo off\r\npy -3 -m pip install --user requests\r\npy -3 xtl_worker.py\r\npause\r\n"
            )

        # Provisioning the worker will read on first start
        provision = {
            "api_base": api_base,
            "device_name": req.device_name or "",
        }
        if token:
            # new flow: token-first (no pairing step)
            provision["device_token"] = token
            provision["device_id"] = dev_id
        else:
            # legacy fallback: pairing code
            provision["pairing_code"] = code

        z.writestr("provision.json", json.dumps(provision, indent=2))
        z.writestr("README.txt",
                   "1) Unzip\n2) Double-click start.bat\n3) Agent will appear online and process jobs.\n")

    mem.seek(0)
    headers = {"Content-Disposition": f'attachment; filename="xtl-worker-{code_id}.zip"'}
    return StreamingResponse(mem, media_type="application/zip", headers=headers)

# OPTIONAL: keep the old JSON root at /info
@app.get("/info")
def info():
    return {"service": "XauTrendLab API", "docs": "/docs", "health": "/healthz"}

# NEW: built-in wizard at

@app.get("/", response_class=HTMLResponse)
def wizard():
    return """
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>XauTrendLab - Backtest UI</title>
<style>
  body { font-family: system-ui, -apple-system, Segoe UI, Roboto, "Helvetica Neue", Arial, sans-serif; max-width: 960px; margin: 2rem auto; padding: 0 1rem; }
  h1 { font-weight: 700; margin-bottom: .5rem; }
  small { color: #666; }
  label { display:block; margin-top: .75rem; font-weight:600; }
  input, select, button { font-size: 14px; padding: .5rem .6rem; }
  input, select { width: 100%; max-width: 420px; }
  .row { display:flex; gap: 1rem; flex-wrap: wrap; align-items:flex-end; }
  .row > div { flex: 1 1 260px; }
  button { cursor:pointer; }
  .btn { background:#111; color:#fff; border:0; border-radius:8px; padding:.6rem .9rem; }
  .btn:disabled { opacity:.5; cursor:not-allowed; }
  .muted { color:#666; }
  .ok { color:#09834a; }
  .err { color:#b00020; white-space: pre-wrap; }
  .card { border:1px solid #eee; border-radius: 12px; padding:1rem; margin-top:1rem; }
  .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, "Liberation Mono", monospace; }
</style>
</head>
<body>
  <h1>XauTrendLab - Backtest UI</h1>
  <small>API base: <span id="apiBase" class="mono"></span></small>

  <div class="card">
    <div class="row">
      <div>
        <label>API password</label>
        <input id="apiKey" type="password" placeholder="Paste API key" />
      </div>
      <div>
        <label>User ID</label>
        <input id="userId" placeholder="e.g. u_demo" />
      </div>
      <div style="display:flex; gap:.5rem;">
        <button class="btn" id="refreshDevicesBtn">Refresh devices</button>
        <button class="btn" id="pairBtn">Pair & Download</button>
        <button class="btn" id="testBtn">Test MT5 connection</button>
      </div>
    </div>
    <div id="authMsg" class="muted" style="margin-top:.5rem;"></div>
  </div>

  <div class="card">
    <label>Device</label>
    <select id="deviceSelect"></select>
    <div class="muted" style="margin-top:.25rem;">Newly paired devices appear here after the worker starts (heartbeats every ~30s)</div>
  </div>

  <div class="card">
    <div class="row">
      <div><label>Symbol</label><input id="symbol" value="XAUUSD"/></div>
      <div><label>Time zone (IANA)</label><input id="tz" value="Asia/Kolkata"/></div>
    </div>
    <div class="row">
      <div><label>Start (YYYY-MM-DD)</label><input id="start" placeholder="2025-09-18"/></div>
      <div><label>End (YYYY-MM-DD)</label><input id="end" placeholder="2025-09-18"/></div>
      <div><label>Assumed spread</label><input id="spread" value="0.20"/></div>
    </div>
    <div style="margin-top:.75rem;">
      <button class="btn" id="runBtn">Run backtest</button>
      <span id="runError" class="err"></span>
    </div>
  </div>

  <div class="card">
    <h3>Job status</h3>
    <div>Job ID: <span id="jobId" class="mono">-</span></div>
    <div>Status: <span id="jobStatus">idle</span></div>
    <div>Progress: <span id="jobProgress">0.00</span></div>
    <div>Message: <span id="jobMsg" class="muted"></span></div>
    <div id="downloadWrap" style="margin-top:.75rem;"></div>
    <div id="sseMsg" class="muted" style="margin-top:.5rem;"></div>
  </div>

<script>
(() => {
  const apiBase = window.location.origin;
  document.getElementById('apiBase').textContent = apiBase;
  const $ = (id) => document.getElementById(id);
  const apiKeyInput = $('apiKey');
  const userIdInput = $('userId');
  const deviceSelect = $('deviceSelect');
  const authMsg = $('authMsg');
  apiKeyInput.value = localStorage.getItem('xtl_api_key') || '';
  userIdInput.value = localStorage.getItem('xtl_user_id') || '';
  function savePrefs(){ localStorage.setItem('xtl_api_key', apiKeyInput.value); localStorage.setItem('xtl_user_id', userIdInput.value); }
  function headers(){ return { 'X-API-Key': apiKeyInput.value.trim(), 'Content-Type': 'application/json' }; }
  async function pingAuth(){ try { const r = await fetch(apiBase + '/healthz'); if (!r.ok) throw 0; authMsg.textContent='Ready. Use your API key for protected calls.'; authMsg.className='ok'; } catch(e){ authMsg.textContent='Health check failed.'; authMsg.className='err'; } }
  async function refreshDevices(){
    savePrefs();
    const uid = userIdInput.value.trim();
    if (!uid) { alert('Enter user id'); return; }
    deviceSelect.innerHTML = '<option>(loading...)</option>';
    try {
      const r = await fetch(apiBase + '/worker/devices?user_id=' + encodeURIComponent(uid), { headers: headers() });
      if (r.status === 401) throw new Error('401 Unauthorized - check API key');
      const data = await r.json();
      deviceSelect.innerHTML = '';
      if (!data.devices || !data.devices.length) { deviceSelect.innerHTML='<option>(no devices yet)</option>'; return; }
      data.devices.forEach(d => {
        const o = document.createElement('option');
        o.value = d.device_id;
        o.text  = (d.online ? '* ' : '  ') + d.name + ' [' + d.device_id + ']';
        deviceSelect.add(o);
      });
    } catch(e) { deviceSelect.innerHTML='<option>(error)</option>'; authMsg.textContent = e.message; authMsg.className='err'; }
  }
  async function pairAndDownload(){
    savePrefs();
    const uid = userIdInput.value.trim();
    if (!uid) { alert('Enter user id'); return; }
    const name = prompt('Device name to show in UI?', 'MyPC') || 'MyPC';
    try {
      const r = await fetch(apiBase + '/worker/bundle', { method:'POST', headers: headers(), body: JSON.stringify({ user_id: uid, device_name: name }) });
      if (!r.ok) { const t = await r.text().catch(()=> ''); throw new Error('bundle failed: ' + r.status + ' ' + t); }
      const blob = await r.blob();
      const cd = r.headers.get('Content-Disposition') || '';
      const fname = (cd.split('filename=').pop() || 'xtl-worker.zip').replace(/^"+|"+$/g, '');
      const url = URL.createObjectURL(blob); const a = document.createElement('a');
      a.href=url; a.download=fname; document.body.appendChild(a); a.click(); a.remove(); setTimeout(()=> URL.revokeObjectURL(url), 5000);
    } catch(e){ alert(e.message); }
  }
  let es = null;
  function attachSSE(jobId){
    if (es){ try{ es.close(); }catch(_){} }
    const k = encodeURIComponent(apiKeyInput.value.trim());
    es = new EventSource(apiBase + '/events/' + jobId + '?key=' + k);
    $('sseMsg').textContent = 'Listening for updates...';
    es.addEventListener('update', ev => {
      try {
        const obj = JSON.parse(ev.data);
        $('jobStatus').textContent = obj.status || '';
        $('jobProgress').textContent = obj.progress || '';
        $('jobMsg').textContent = obj.message || '';
        if (obj.out_csv_api){ showDownload(obj.out_csv_api); }
      } catch(_){}
    });
    es.addEventListener('error', () => { $('sseMsg').textContent = 'SSE connection closed.'; });
  }
  async function testConnection(){
    savePrefs();
    const uid = userIdInput.value.trim();
    const deviceId = deviceSelect.value;

    if (!uid) { alert('Enter user id'); return; }
    if (!deviceId || deviceId.startsWith('(')) { alert('Select a device first'); return; }

    try {
      const r = await fetch(apiBase + '/worker/test-mt5', {
        method: 'POST',
        headers: headers(),
        body: JSON.stringify({ user_id: uid, device_id: deviceId })
      });
      if (!r.ok) {
        const t = await r.text().catch(()=> '');
        throw new Error('test enqueue failed: ' + r.status + ' ' + t);
      }
      const { job_id } = await r.json();

      // Reuse the status panel + SSE stream
      $('jobId').textContent = job_id || '-';
      $('jobStatus').textContent = 'queued';
      $('jobProgress').textContent = '0.00';
      $('jobMsg').textContent = 'Testing MT5 connectivity...';
      $('downloadWrap').innerHTML = '';

      attachSSE(job_id);
    } catch (e) {
      alert(e.message);
    }
  }
  function showDownload(apiPath) {
  const wrap = $('downloadWrap');
  wrap.innerHTML =
    '<button class="btn" id="dlBtn">Download CSV</button> ' +
    '<span id="dlHint" class="muted">Link expires after download.</span>';

  $('dlBtn').onclick = async (e) => {
    e.preventDefault();
    try {
      const r = await fetch(apiBase + apiPath, { headers: headers() });
      if (!r.ok) {
        if (r.status === 404) {
          wrap.innerHTML = '<span class="muted">File no longer available. Re-run backtest to generate a fresh CSV.</span>';
          return;
        }
        throw new Error('download failed: ' + r.status);
      }
      const blob = await r.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url; a.download = 'backtest.csv';
      document.body.appendChild(a); a.click(); a.remove();
      setTimeout(() => URL.revokeObjectURL(url), 5000);
      wrap.innerHTML = '<span class="ok">Downloaded &#10003;</span>';
    } catch (err) {
      wrap.innerHTML = '<span class="err">' + err.message + '</span>';
    }
  };
}

  async function runBacktest(){
    savePrefs();
    $('runError').textContent = '';
    const deviceId = $('deviceSelect').value;
    if (!deviceId || deviceId.startsWith('(')) { $('runError').textContent = 'Select a device first'; return; }
    const payload = {
      symbol: $('symbol').value.trim(),
      tz: $('tz').value.trim(),
      start: $('start').value.trim(),
      end: $('end').value.trim(),
      assumed_spread: parseFloat(($('spread').value || '0').trim()),
      device_id: deviceId
    };
    try {
      const r = await fetch(apiBase + '/backtest', { method:'POST', headers: headers(), body: JSON.stringify(payload) });
      if (r.status === 401) throw new Error('Authentication required.');
      const data = await r.json();
      $('jobId').textContent = data.job_id || '-';
      $('jobStatus').textContent = 'queued';
      $('jobProgress').textContent = '0.00';
      $('jobMsg').textContent = 'Queued';
      $('downloadWrap').innerHTML = '';
      attachSSE(data.job_id);
    } catch(e){ $('runError').textContent = e.message; }
  }
  $('refreshDevicesBtn').onclick = refreshDevices;
  $('pairBtn').onclick = pairAndDownload;
  $('runBtn').onclick = runBacktest;
  $('testBtn').onclick = testConnection;
  pingAuth();
})();
</script>
</body>
</html>
"""
