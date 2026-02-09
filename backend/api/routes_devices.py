# routes_devices.py — Devices API (silent-bind ready, robust imports)
from __future__ import annotations

import os, io, json, time, uuid, secrets, random, string, zipfile
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, Literal,List

import redis
import psycopg2
import psycopg2.extras as _extras
from fastapi import APIRouter, Depends, HTTPException, Header, Request, Query, Body
from fastapi.responses import Response, StreamingResponse, JSONResponse, RedirectResponse
from contextlib import contextmanager
from pathlib import Path
import shutil, tempfile, subprocess
from pydantic import BaseModel, Field, ConfigDict
import httpx, math




import logging

log = logging.getLogger("uvicorn.error")

TF_SEC = {"M1":60,"M5":300,"M15":900,"M30":1800,"H1":3600,"H4":14400,"D1":86400}

# ---- DB / Auth / CSRF (robust imports: try package, then local) -------------
import os

try:
    # package imports (preferred)
    from api.deps import db, get_current_user, csrf_protect, require_perm
    from api.security import require_auth_and_mfa
    # relaxed (session) auth — may or may not exist here; shim below will backfill if missing
    try:
        from api.deps import get_current_user_relaxed  # type: ignore
    except Exception:
        get_current_user_relaxed = None  # filled by shim below
except ImportError:
    # fallback when running directly
    import sys
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from db import db
    from deps import get_current_user, csrf_protect, require_perm  # type: ignore
    from security import require_auth_and_mfa  # type: ignore
    try:
        from deps import get_current_user_relaxed  # type: ignore
    except Exception:
        get_current_user_relaxed = None  # filled by shim below

# --- Shim: ensure get_current_user_relaxed is available under that exact name
if get_current_user_relaxed is None:
    try:
        # some codebases named it require_user_relaxed
        from api.security import require_user_relaxed as get_current_user_relaxed  # type: ignore
    except Exception:
        try:
            from security import require_user_relaxed as get_current_user_relaxed  # type: ignore
        except Exception:
            # last resort: alias strict auth (not ideal, but prevents import crash)
            get_current_user_relaxed = get_current_user  # type: ignore

# ---- Redis ------------------------------------------------------------------
REDIS_URL = "redis://default:xau12345@10.0.0.132:6379/0"
R = redis.from_url(REDIS_URL, decode_responses=True)
# import-time smoke test: which Redis is this process using?
try:
    import time as _t
    R.setex("xtl:debug:devices_import", 600, f"loaded:{int(_t.time())}")
except Exception:
    pass
log.info(f"[ROUTES] module={__file__}")
log.info(f"[ROUTES] REDIS_URL={REDIS_URL}")
DEVICE_PREFIX = os.getenv("XTL_DEVICE_KEY_PREFIX", "device:")

DEFAULT_TFS = ["M15","H1","H4"]
DEFAULT_TFS_CSV = ",".join(DEFAULT_TFS)


@contextmanager
def db():
    # Build DSN from env; use DATABASE_URL if set, else PG* vars
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        host = os.getenv("PGHOST", "127.0.0.1")
        port = os.getenv("PGPORT", "5432")
        name = os.getenv("PGDATABASE", "postgres")
        user = os.getenv("PGUSER", "postgres")
        pwd  = os.getenv("PGPASSWORD", "")
        dsn = f"postgresql://{user}:{pwd}@{host}:{port}/{name}"

    conn = psycopg2.connect(dsn)

    class _ConnProxy:
        def __init__(self, _c): self._c = _c
        def cursor(self, *args, **kwargs):
            if "cursor_factory" not in kwargs:
                kwargs["cursor_factory"] = psycopg2.extras.DictCursor
            return self._c.cursor(*args, **kwargs)
        # expose what the code expects
        def commit(self): return self._c.commit()
        def rollback(self): return self._c.rollback()

    proxy = _ConnProxy(conn)

    try:
        yield proxy
        conn.commit()
    except Exception:
        try: conn.rollback()
        except Exception: pass
        raise
    finally:
        conn.close()



def _decode(b):
    if isinstance(b, bytes):
        try: return b.decode("utf-8")
        except Exception: return ""
    return b
def _hkey(dev_id: str) -> str:
    return f"{DEVICE_PREFIX}{dev_id}"

# ---- MT5 command queue (device pulls) -----------------------------------------

def _mt5_cmdq_key(dev_id: str) -> str:
    return f"xtl:mt5:cmdq:{dev_id}"


def _mt5_ack_key(job_id: str) -> str:
    job_id = (job_id or "").strip()
    return f"xtl:mt5:ack:{job_id}"

def _redis_device_state(dev_id: str) -> dict:
    """Return a merged device dict with normalized heartbeat timestamps."""
    h = R.hgetall(_hkey(dev_id)) or {}
    # bytes ? str if needed
    h = { (k.decode() if isinstance(k, bytes) else k) :
          (v.decode() if isinstance(v, bytes) else v)
          for k, v in h.items() }

    status = h.get("status") or "offline"
    mt5_ok = (h.get("mt5_ok") in ("1", "true", "True", True))

    raw = h.get("last_heartbeat") or h.get("last_seen") or h.get("last_seen_at")
    ts_sec = None
    if raw:
        try:
            ts_sec = int(float(raw))
        except Exception:
            ts_sec = None

    payload = {
        "device_id": dev_id,
        "status": status,
        "mt5_ok": mt5_ok,
    }

    if ts_sec:
        payload["last_heartbeat"] = ts_sec                  # seconds
        payload["last_heartbeat_ms"] = ts_sec * 1000        # milliseconds
        payload["last_heartbeat_iso"] = datetime.utcfromtimestamp(ts_sec).isoformat() + "Z"

    return payload

FRESH_MS = int(os.getenv("OFFLINE_AFTER_MS", "120000"))

def _hgetall_str(key: str) -> dict[str, str]:
    raw = R.hgetall(key) or {}
    # redis-py can return bytes -> decode
    out = {}
    for k, v in raw.items():
        if isinstance(k, bytes): k = k.decode("utf-8", "ignore")
        if isinstance(v, bytes): v = v.decode("utf-8", "ignore")
        out[k] = v
    return out

def _parse_hb_ms(meta: dict[str,str]) -> int | None:
    # prefer ms, then ISO, then seconds
    if "last_heartbeat_ms" in meta:
        try: return int(meta["last_heartbeat_ms"])
        except: pass
    if "last_heartbeat_iso" in meta:
        try:
            dt = datetime.fromisoformat(meta["last_heartbeat_iso"].replace("Z","+00:00"))
            return int(dt.timestamp()*1000)
        except: pass
    if "last_heartbeat" in meta:
        try:
            sec = int(meta["last_heartbeat"])
            return sec*1000 if sec < 10**12 else sec
        except: pass
    return None

def _truthy(meta: dict[str,str], key: str) -> bool:
    v = (meta.get(key) or "").lower()
    return v in ("1","true","yes","ok")


def _user_devices_key(user_id: str) -> str:
    return f"user:{user_id}:devices"

def _iso_from_epoch(v: Optional[str | int]) -> Optional[str]:
    if v is None:
        return None
    try:
        iv = int(v)
        return datetime.fromtimestamp(iv, tz=timezone.utc).isoformat()
    except Exception:
        return None
# Normalize whatever the auth dependency returns into a string user_id
def _uid(u):
    if isinstance(u, dict):
        val = u.get("id") or u.get("user_id") or u.get("sub")
        if val: return str(val)
    if hasattr(u, "id"):
        return str(getattr(u, "id"))
    if isinstance(u, (str, int)):
        return str(u)
    raise HTTPException(status_code=401, detail="Invalid auth context")

def _uid_from(u):
    """
    Accepts dict- or object-shaped user. Returns a stable user_id or None.
    """
    if not u:
        return None
    # dict-style
    if isinstance(u, dict):
        return (
            u.get("user_id")
            or u.get("id")
            or u.get("uid")
            or u.get("sub")
        )
    # object-style
    for attr in ("user_id", "id", "uid", "sub"):
        v = getattr(u, attr, None)
        if v:
            return v
    return None

import logging
log = logging.getLogger("xtl")

def _ensure_schema(cur):
    # tables/columns we depend on; safe to call every request
    cur.execute("""
        CREATE TABLE IF NOT EXISTS device_claims(
          device_id   text PRIMARY KEY,
          token       text NOT NULL,
          code        text NOT NULL,
          status      text NOT NULL,
          user_id     text,
          expires_at  timestamptz NOT NULL,
          created_at  timestamptz NOT NULL DEFAULT now(),
          updated_at  timestamptz NOT NULL DEFAULT now()
        )
    """)
    # keep devices columns we read/write
    cur.execute("""
        DO $$
        BEGIN
          IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='devices' AND column_name='user_id'
          ) THEN
            ALTER TABLE devices
              ADD COLUMN user_id         text,
              ADD COLUMN status          text,
              ADD COLUMN device_token    text,
              ADD COLUMN pair_code       text,
              ADD COLUMN pair_expires_at timestamptz,
              ADD COLUMN created_at      timestamptz DEFAULT now(),
              ADD COLUMN updated_at      timestamptz DEFAULT now();
          END IF;
        END$$;
    """)



def _session_user(request: Request) -> Optional[dict]:
    """Return {'id': <uid>, 'mfa_ok': bool} from Starlette session, or None."""
    sess = getattr(request, "session", {}) or {}
    uid = sess.get("user_id")
    if not uid:
        return None
    return {"id": str(uid), "mfa_ok": bool(sess.get("mfa_ok", False))}

def require_session_user(request: Request):
    u = _session_user(request)
    if not u:
        raise HTTPException(status_code=401, detail="Authentication required.")
    return u


def _try_user(request: Request) -> Optional[Any]:
    """Best-effort resolver: never raises; returns a user or None."""
    u = _session_user(request)
    if u:
        return u
    try:
        return get_current_user(request)  # your strict/MFA path, if imported
    except Exception:
        return None


def require_user(request: Request):
    u = _session_user(request)
    if u:
        return u
    try:
        u2 = get_current_user(request)  # strict path, if available
        if u2:
            return u2
    except Exception:
        pass
    uid = _uid_from(request)
    if uid:
        return {"id": uid}
    raise HTTPException(status_code=401, detail="Unauthorized")


def _assert_device_token(device_id: str, token: str):
    with db() as conn, conn.cursor() as cur:
        cur.execute("SELECT device_token FROM devices WHERE id=%s", (device_id,))
        row = cur.fetchone()
        if not row or (row[0] or "") != token:
            raise HTTPException(status_code=401, detail="Bad device token")

def _is_fresh(hb_ms: int | None, now_ms: int) -> bool:
    if hb_ms is None:
        return False
    age = now_ms - hb_ms
    return (age >= 0) and (age <= FRESH_MS)


def _present(dev_id: str, h: Dict[str, str]) -> dict:
    last_hb_iso = _decode(h.get("last_heartbeat_iso") or "")
    last_hb = _iso_from_epoch(h.get("last_heartbeat")) or last_hb_iso or ""
    
    now_ms = int(time.time() * 1000)
    hb_ms = None
    if h.get("last_heartbeat_ms"):
       try: hb_ms = int(h["last_heartbeat_ms"])
       except: hb_ms = None
    elif last_hb_iso:
       try:
          from datetime import datetime, timezone
          hb_ms = int(datetime.fromisoformat(last_hb_iso.replace("Z","+00:00")).timestamp() * 1000)
       except: hb_ms = None
    fresh = _is_fresh(hb_ms, now_ms)
    def b(name, default="0"):
        v = h.get(name, default)
        return (str(v).lower() in ("1","true","t","yes"))
    return {
        "device_id": dev_id,
        "label": _decode(h.get("label") or "") or "",
        "version": _decode(h.get("version") or "") or "",
        "mt5_ok":       "1" if (fresh and b("mt5_ok"))       else "0",
        "api_ok":       "1" if (fresh and b("api_ok", "1"))  else "0",
        "autostart_ok": "1" if (fresh and b("autostart_ok")) else "0",
        "last_heartbeat": last_hb,             # ISO
        "last_heartbeat_iso": last_hb_iso,     # ISO (as written by heartbeat)
        "last_error": _decode(h.get("last_error") or "") or "",
        "status": "online" if fresh else "offline",
        "active": "1" if b("active", "1") else "0",
    }

def _bind_device_to_user(user_id: str, dev_id: str) -> None:
    # DB: ensure the device is owned and marked active
    with db() as conn, conn.cursor() as cur:
        cur.execute("""
            UPDATE devices
               SET user_id=%s,
                   status='active',
                   updated_at=now()
             WHERE id=%s
        """, (user_id, dev_id))
        conn.commit()
    # Redis: ensure membership set contains this device
    try:
        R.sadd(f"xtl:user:{user_id}:devices", dev_id)
    except Exception as e:
        # optional: replace print with your logger
        print(f"[WARN] SADD user-device set failed: {e}")


def ensure_owner(cur, device_id: str, user_id: str) -> dict:
    cur.execute("""
        SELECT id, user_id, name, status, last_heartbeat_at, mt5_ok, created_at, updated_at
        FROM devices
        WHERE id = %s
        LIMIT 1
    """, (device_id,))
    row = cur.fetchone()
    if not row or str(row["user_id"]) != str(user_id):
        raise HTTPException(status_code=404, detail="Device not found")
    return row

TF_SEC_CMP = {"M15": 15*60, "H1": 60*60, "H4": 4*60*60}

def _round_to_digits(x: float, digits: int) -> float:
    scale = 10 ** digits
    return math.floor(float(x) * scale + 0.5) / scale



async def _cmp_fetch_broker_bars(symbol: str, tf: str, limit: int, price: str = "bid", agent_base: str | None = None):
    base = (agent_base or os.getenv("AGENT_BASE_URL", "")).rstrip("/")
    if not base:
        raise HTTPException(status_code=424, detail="AGENT_BASE_URL not configured")

    # Probe these paths in order
    candidates = [
        f"{base}/broker/ohlc",
        f"{base}/ohlc",
        f"{base}/api/ohlc",
    ]

    # If base is https to localhost, allow self-signed during testing
    insecure_verify = base.startswith("https://127.0.0.1") or base.startswith("https://localhost")

    last_err = None
    async with httpx.AsyncClient(timeout=10, verify=(False if insecure_verify else True)) as cli:
        for url in candidates:
            try:
                res = await cli.get(url, params={"symbol": symbol, "tf": tf, "limit": limit, "price": price})
                # Some servers accept and immediately close (empty reply) -> httpx raises here as well
                if res.status_code == 200:
                    js = res.json()
                    if isinstance(js, list):
                        return js
                    # sometimes APIs wrap in {"bars":[...]}; unwrap
                    if isinstance(js, dict) and isinstance(js.get("bars"), list):
                        return js["bars"]
                # Try next candidate on 404/empty/garbage
                last_err = f"{url} -> {res.status_code} {res.text[:120]}"
            except Exception as e:
                last_err = f"{url} -> {e.__class__.__name__}: {e}"

    raise HTTPException(status_code=502, detail=f"Agent fetch failed: {last_err or 'no valid response'}")


def _cmp_load_app_bars_for_user(user_id: str, symbol: str, tf: str, limit: int):
    """Read the same snapshot your /devices OHLC writer produces for this user."""
    sym = symbol.upper()
    tfU = tf.upper()
    snap_key = f"xtl:trend:snap:{user_id}:{sym}:{tfU}"
    raw = R.get(snap_key)
    if not raw:
        return []
    try:
        snap = json.loads(raw)
    except Exception:
        return []
    if not isinstance(snap, dict) or "bars" not in snap:
        return []

    tf_sec = TF_SEC_CMP.get(tfU, 3600)
    now_slot = (int(time.time()) // tf_sec) * tf_sec
    out = []
    for b in snap["bars"][-(limit+5):]:
        try:
            t = int(b.get("t", 0))
            if t >= 10**12:        # ms -> sec
                t //= 1000
        except Exception:
            continue
        if t >= now_slot:          # drop forming
            continue
        if (t % tf_sec) != 0:      # enforce grid
            continue
        out.append({"t": t, "o": float(b["o"]), "h": float(b["h"]), "l": float(b["l"]), "c": float(b["c"])})
    return out[-limit:]


# ---- Router -----------------------------------------------------------------
r = APIRouter(prefix="/devices", tags=["devices"])

# routes_devices.py
from typing import List, Optional, Annotated
from pydantic import BaseModel
from fastapi import APIRouter, Body, Header, HTTPException

class Mt5AckPayload(BaseModel):
    job_id: str
    ok: bool = True
    error: Optional[str] = None
    result: Optional[Dict[str, Any]] = None
    # NEW: preserve who this ack belongs to
    user_id: Optional[str] = None
    model_config = ConfigDict(extra="ignore")


@r.get("/{dev_id}/mt5/next")
def mt5_next(
    dev_id: str,
    authorization: Optional[str] = Header(default=None),
):
    # device auth (same style as post_ohlc)
    token = ""
    if authorization:
        parts = authorization.split()
        token = parts[-1] if parts else authorization.strip()
    if not token:
        raise HTTPException(status_code=401, detail="missing token")
    _assert_device_token(dev_id, token)

    try:
        raw = R.lpop(_mt5_cmdq_key(dev_id))
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"redis_error:{type(e).__name__}")

    if not raw:
        return {"ok": True, "cmd": None}

    try:
        cmd = json.loads(raw)
    except Exception:
        cmd = {"type": "unknown", "raw": raw}

    return {"ok": True, "cmd": cmd}


@r.post("/{dev_id}/mt5/ack")
def mt5_ack(
    dev_id: str,
    payload: Mt5AckPayload,
    request: Request,
    authorization: Optional[str] = Header(default=None),
):
    token = ""
    if authorization:
        parts = authorization.split()
        token = parts[-1] if parts else authorization.strip()
    if not token:
        raise HTTPException(status_code=401, detail="missing token")
    _assert_device_token(dev_id, token)

    job_id = (payload.job_id or "").strip()
    if not job_id:
        raise HTTPException(status_code=400, detail="missing job_id")
    owner = (payload.user_id or (payload.result or {}).get("user_id") or "").strip()

    ack = {
        "job_id": job_id,
        "ok": bool(payload.ok),
        "error": payload.error,
        "result": payload.result or {},
        "user_id": owner,
        "acked_at_ms": int(time.time() * 1000),
    }

    try:
       # ack by job_id (1 hour)
       R.setex(_mt5_ack_key(job_id), 3600, json.dumps(ack))

       # ALSO store per-device+job (1 hour) ? super useful for debugging
       R.setex(f"xtl:mt5:ack:{dev_id}:{job_id}", 3600, json.dumps(ack))

       # last ack by device (1 day)
       R.setex(f"xtl:mt5:last_ack:{dev_id}", 86400, json.dumps(ack))
    except Exception as e:
       raise HTTPException(status_code=503, detail=f"redis_error:{type(e).__name__}")

    return {"ok": True}

@r.get("/{dev_id}/mt5/ack/{job_id}")
def mt5_get_ack(
    dev_id: str,
    job_id: str,
    authorization: Optional[str] = Header(default=None),
):
    token = ""
    if authorization:
        parts = authorization.split()
        token = parts[-1] if parts else authorization.strip()
    if not token:
        raise HTTPException(status_code=401, detail="missing token")
    _assert_device_token(dev_id, token)

    jid = (job_id or "").strip()
    if not jid:
        raise HTTPException(status_code=400, detail="missing job_id")

    try:
        raw = R.get(_mt5_ack_key(jid))
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"redis_error:{type(e).__name__}")

    if not raw:
        return {"ok": True, "ack": None}

    try:
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", "ignore")
        ack = json.loads(raw)
    except Exception:
        ack = {"raw": raw}

    return {"ok": True, "ack": ack}


@r.get("/{dev_id}/mt5/last-ack")
def mt5_last_ack(
    dev_id: str,
    authorization: Optional[str] = Header(default=None),
):
    token = ""
    if authorization:
        parts = authorization.split()
        token = parts[-1] if parts else authorization.strip()
    if not token:
        raise HTTPException(status_code=401, detail="missing token")
    _assert_device_token(dev_id, token)

    try:
        raw = R.get(f"xtl:mt5:last_ack:{dev_id}")
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"redis_error:{type(e).__name__}")

    if not raw:
        return {"ok": True, "ack": None}

    try:
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", "ignore")
        ack = json.loads(raw)
    except Exception:
        ack = {"raw": raw}

    return {"ok": True, "ack": ack}




class OhlcBar(BaseModel):
    t: int   # epoch seconds
    o: float
    h: float
    l: float
    c: float
    v: int = 0
    complete: Optional[bool] = True
    # make sure we never reject unknown/extra fields
    model_config = ConfigDict(extra="ignore")

class Mt5CmdResp(BaseModel):
    ok: bool = True
    cmd: Optional[dict] = None

class Mt5AckReq(BaseModel):
    job_id: str
    ok: bool = True
    result: dict = {}


class OhlcPayload(BaseModel):
    symbol: str
    timeframe: str
    count: int
    written_at: int
    bars: List[OhlcBar]
    broker: Optional[Dict[str, Any]] = None
    # tolerate extra top-level keys like device_id
    account: Optional[Dict[str, Any]] = None
    terminal: Optional[Dict[str, Any]] = None
    
    model_config = ConfigDict(extra="ignore")



# IMPORTANT for Pydantic v2: rebuild to resolve any forward refs
try:
    OhlcBar.model_rebuild()
    OhlcPayload.model_rebuild()
except Exception:
    pass

@r.post("/{dev_id}/ohlc", summary="Ingest OHLC from agent")
def post_ohlc(

    dev_id: str,
    payload: Annotated[OhlcPayload, Body(...)],
    authorization: Optional[str] = Header(default=None),
):
    try:
        # payload may be Pydantic model OR dict; log both safely
        if hasattr(payload, "dict"):
            p = payload.dict()
        elif isinstance(payload, dict):
            p = payload
        else:
            p = None

        log.error(
            "POST_OHLC PAYLOAD dev_id=%s payload_type=%s keys=%s",
            dev_id,
            type(payload).__name__,
            list(p.keys()) if isinstance(p, dict) else None,
        )
    except Exception:
        log.exception("POST_OHLC PAYLOAD log failed dev_id=%s", dev_id)

    import os, json, time

    # --- breadcrumbs: prove route is hit ---
    try:
        R.setex(f"xtl:debug:last_ohlc_hit:{dev_id}", 300, str(int(time.time())))
    except Exception:
        pass
    log.info("[OHLC] enter handler")

    # --- token parse ---
    token = ""
    if authorization:
        parts = authorization.split()
        token = parts[-1] if parts else authorization.strip()
    if not token:
        raise HTTPException(status_code=401, detail="missing token")

    # --- verify token + find owner (DB); non-fatal if DB read fails ---
    owner_id: Optional[str] = None
    try:
        with db() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT user_id::text, device_token FROM devices WHERE id=%s",
                (dev_id,),
            )
            row = cur.fetchone()
            if (not row) or (str(row[1]) != token):
                raise HTTPException(status_code=401, detail="invalid token")
            owner_id = row[0]
    except HTTPException:
        raise
    except Exception as e:
        log.info(f"[OHLC] token verify error (continuing): {e}")
        owner_id = None

    
    # --- persist minimal device facts (non-fatal) ---
    try:
        now_ms = int(time.time() * 1000)

        # derive from normalized payload dict (never from attributes)
        p = payload.dict() if hasattr(payload, "dict") else payload if isinstance(payload, dict) else {}
        bars_p = p.get("bars") or []

        last_t = 0
        if isinstance(bars_p, list) and bars_p:
            b_last = bars_p[-1]
            # prefer explicit ms fields, fallback to seconds
            last_t = (
                b_last.get("t_open_ms")
                or b_last.get("t")
                or 0
            )

        R.hset(
            _hkey(dev_id),
            mapping={
                "last_ohlc_symbol": p.get("symbol"),
                "last_ohlc_tf": (p.get("timeframe") or "").upper(),
                "last_ohlc_count": str(p.get("count", len(bars_p))),
                "last_ohlc_t": str(int(last_t)),
                "last_ohlc_written_at": str(now_ms),
            },
        )
        R.expire(_hkey(dev_id), 600)
    except Exception:
        pass

    # --- SAFETY NET: Treat OHLC as heartbeat (prevents offline during heavy work / market closed) ---
    try:
        now_sec = int(time.time())
        now_ms = now_sec * 1000
        R.hset(_hkey(dev_id), mapping={
            "status": "online",
            "last_heartbeat": str(now_sec),
            "last_heartbeat_ms": str(now_ms),
            "last_heartbeat_iso": datetime.utcfromtimestamp(now_sec).isoformat() + "Z",
        })
        R.expire(_hkey(dev_id), 600)

        # optional debug breadcrumb (helps prove this path runs)
        R.setex(f"xtl:debug:last_hb:{dev_id}", 300, str(now_ms))
    except Exception:
        pass

    
    
    # Also track membership set for hydration
    try:
        if owner_id:
            R.sadd(f"xtl:user:{owner_id}:devices", dev_id)
    except Exception:
        pass

    # ------------- BUILD SNAPSHOT -------------
    try:
        sym = (payload.symbol or "").upper()
        tf  = (payload.timeframe or "H1").upper()
        bar_count = len(payload.bars or [])
        log.info(f"[OHLC] owner_id={owner_id} dev_id={dev_id} sym={sym} tf={tf} bars={bar_count}")

        # breadcrumb: what the server thinks bar_count is
        try:
            R.setex(f"xtl:debug:ohlc:{dev_id}:pre", 300, f"sym={sym} tf={tf} bars={bar_count}")
        except Exception:
            pass

        # timeframe length in ms
        TF_MS = {
            "M1": 60_000, "M5": 300_000, "M15": 900_000, "M30": 1_800_000,
            "H1": 3_600_000, "H4": 14_400_000, "D1": 86_400_000
        }
        tf_ms = TF_MS.get(tf, 3_600_000)
        server_now_ms = int(time.time() * 1000)

        # --- ensure we know the owner; if missing, resolve from device hash ---
        if not owner_id:
            try:
                prefix = os.getenv("XTL_DEVICE_KEY_PREFIX", "device:")
                meta = R.hgetall(f"{prefix}{dev_id}") or {}
                owner_id = meta.get("owner_id") or meta.get("user_id")
            except Exception:
                owner_id = None

        # track membership if we just learned the owner
        if owner_id:
            try:
                R.sadd(f"xtl:user:{owner_id}:devices", dev_id)
            except Exception:
                pass

        # If agent posted ZERO bars, still seed both device + user snaps so UI hydrates
        # If agent posted ZERO bars -> DO NOT overwrite existing snapshots.
        if not bar_count:
           try:
              R.setex(
                  f"xtl:debug:ohlc:{dev_id}:empty",
                  300,
                  f"sym={sym} tf={tf} bars=0 (skipped snap overwrite)"
              )
           except Exception:
              pass
           return {"ok": True, "received": 0, "snap": "empty-skip"}

        # --- normalize incoming bars; keep storage in SECONDS ---
        # --- normalize incoming bars; keep storage in SECONDS ---
        def _to_ms(x) -> int:
            try:
               v = int(x or 0)
               return v if v >= 10_000_000_000 else v * 1000  # if seconds, -> ms
            except Exception:
               return 0

        bars_sec: list[dict] = []
        last_close_ms_seen = 0
        last_open_ms_for_last_close = 0
        last_close_px = None

        for b in payload.bars:
            # prefer explicit ms fields if present (agent may send them)
            t_open_ms  = int(getattr(b, "t_open_ms", 0)) or _to_ms(getattr(b, "t", 0))
            t_close_ms = int(getattr(b, "t_close_ms", 0)) or (t_open_ms + tf_ms)

            # store legacy 't' as **seconds** (your existing snapshot shape)
            d = {
                "t": int(t_open_ms // 1000),         # seconds (OPEN)
                "o": float(getattr(b, "o", 0.0)),
                "h": float(getattr(b, "h", 0.0)),
                "l": float(getattr(b, "l", 0.0)),
                "c": float(getattr(b, "c", 0.0)),
                "v": int(getattr(b, "v", 0) or 0),
                "complete": True,                    # agent sends CLOSED bars
            }
            bars_sec.append(d)

            if t_close_ms > last_close_ms_seen:
                last_close_ms_seen = t_close_ms
                last_open_ms_for_last_close = t_open_ms
                try:
                   last_close_px = float(getattr(b, "c", 0.0))
                except Exception:
                   last_close_px = None
        # trim to a sane maximum (defensive)
        if len(bars_sec) > 1000:
            bars_sec = bars_sec[-1000:]
        # -------------------------------
        # NEW: validate bars before overwriting snapshots
        # Prevent SR flicker when MT5 fetch returns short/dirty series during reconnect.
        # -------------------------------
        def _is_ok_bar(d: dict) -> bool:
            try:
                o = float(d.get("o", 0.0))
                h = float(d.get("h", 0.0))
                l = float(d.get("l", 0.0))
                c = float(d.get("c", 0.0))
                t = int(d.get("t", 0))
                if t <= 0:
                    return False
                if not (o > 0 and h > 0 and l > 0 and c > 0):
                    return False
                if h < l:
                    return False
                return True
            except Exception:
                return False

        valid_bars = [d for d in bars_sec if _is_ok_bar(d)]

        MIN_BARS_BY_TF = {
            "H1": 120,   # ~5 days of H1 bars
            "H4": 80,    # ~13 days of H4 bars
            "M15": 200,
            "M5": 300,
            "M1": 240,
            "D1": 30,
        }
        min_bars = int(MIN_BARS_BY_TF.get(tf, 80))

        # If series is too short/dirty, DO NOT overwrite last-good snaps
        if len(valid_bars) < min_bars:
            try:
                R.setex(
                    f"xtl:debug:ohlc:{dev_id}:skip_short",
                    600,
                    f"sym={sym} tf={tf} raw={len(bars_sec)} valid={len(valid_bars)} min={min_bars}",
                )
            except Exception:
                pass

            log.warning(
                "[OHLC] skip overwrite snaps sym=%s tf=%s raw=%s valid=%s min=%s (kept last-good)",
                sym, tf, len(bars_sec), len(valid_bars), min_bars,
            )
            return {"ok": True, "received": len(payload.bars or []), "snap": "skip-short"}

        # use validated bars from here onward
        bars_sec = valid_bars

        # ==================================================
        # ==================================================
        # PHASE 2: WS PRICE PUBLISH + CACHE (JSON)
        # ==================================================
        # 
        # Publishes: xtl:pub:price:{owner_id}
        # Caches:    xtl:price:{dev_id}:{SYMBOL}  (preferred)
        #            xtl:price:{SYMBOL}           (fallback)
        # Value:     {"price": <float>, "ts_ms": <int>}
        # ts_ms must be LAST CLOSED BAR CLOSE TIME (not server time)
        
        # ==================================================
        # ==================================================
        # PHASE 2: WS PRICE PUBLISH + CACHE (FINAL, SAFE)
        # ==================================================
        try:
            if isinstance(bars_sec, list) and bars_sec:
                sym_u = (sym or "").upper().strip()

                # last CLOSED bar close
                # Choose ONE meaning and keep it consistent:
                # If you want MT5 candle label time -> OPEN time:
                try:
                    live_px = float(last_close_px) if last_close_px is not None else None
                except Exception:
                    live_px = None

                # timestamp = bar CLOSE time (not server time)
                # timestamp = LAST CLOSED BAR OPEN time (MT5 candle time)
                # (If instead you want "close boundary time", use last_close_ms_seen)
                # ts_ms = int(last_close_ms_seen) if last_close_ms_seen else None
                try:
                    ts_ms = int(last_open_ms_for_last_close) if last_open_ms_for_last_close else None
                except Exception:
                    ts_ms = None

                if sym_u and isinstance(live_px, (int, float)) and live_px > 0 and ts_ms:
                    val = json.dumps({"price": live_px, "ts_ms": ts_ms, "src": f"ohlc_{tf.lower()}_close"}, separators=(",", ":"), ensure_ascii=False)

                    ttl = 7 * 24 * 3600  # 7 days persistence

                    # device-scoped (preferred)
                    def _set_if_newer(key: str, new_ts_ms: int) -> None:
                        old = R.get(key)
                        if old:
                            try:
                                oldj = json.loads(old)
                                old_ts = int(oldj.get("ts_ms") or 0)
                                if old_ts > 0 and old_ts >= new_ts_ms:
                                    return  # keep newer existing
                            except Exception:
                                pass
                        R.setex(key, ttl, val)

                    # device-scoped (preferred) — only overwrite if ts_ms is newer
                    _set_if_newer(f"xtl:price:{dev_id}:{sym_u}", int(ts_ms))


                    # DO NOT write global price from OHLC.
                    # Global xtl:price:{SYMBOL} must come only from /price (tick).
                    # R.setex(f"xtl:price:{sym_u}", ttl, val)
                    # pubsub (only if owner exists)
                    if owner_id:
                        R.publish(
                            f"xtl:pub:price:{owner_id}",
                            json.dumps(
                                {
                                    "type": "ohlc_price",
                                    "symbol": sym_u,
                                    "price": live_px,
                                    "device_id": dev_id,
                                    "ts_ms": ts_ms,
                                    "src": f"ohlc_{tf.lower()}_close",
                                },
                                separators=(",", ":"),
                                ensure_ascii=False,
                            ),
                        )

                    log.info(
                        "PH2 price_cache OK dev=%s sym=%s px=%s ts=%s owner=%s",
                        dev_id,
                        sym_u,
                        live_px,
                        ts_ms,
                        owner_id,
                    )
        except Exception:
            log.exception("PH2 price_cache failed dev=%s sym=%s", dev_id, sym)


        # derive last/next in ms using the **CLOSE** boundary
        last_closed_ms = int(last_close_ms_seen)
        next_close_ms  = last_closed_ms + tf_ms


        # device freshness breadcrumb
        try:
            R.hset(_hkey(dev_id), mapping={
                "last_ohlc_t": str(last_closed_ms),
                "last_ohlc_written_at": str(server_now_ms),
            })
            R.expire(_hkey(dev_id), 600)
        except Exception:
            pass

        snap_val = {
            "serverNow": server_now_ms,
            "lastClosedTs": last_closed_ms,
            "nextCloseTs": next_close_ms,
            "bars": bars_sec,                              # <<< seconds here
            "broker": (getattr(payload, "broker", None) or {}),
            "account": (getattr(payload, "account", None) or {}),
            "terminal": (getattr(payload, "terminal", None) or {}),
        }
        # persist broker tz into the device hash so /trend/state2 can fall back to it
        try:
           b = getattr(payload, "broker", None) or {}
           tz_name = str(b.get("tz_name", "")).strip()
           tz_off  = int(b.get("tz_offset_min", 0))
           R.hset(_hkey(dev_id), mapping={
               "broker_tz_name": tz_name,
               "broker_tz_offset_min": str(tz_off),
           })
           R.expire(_hkey(dev_id), 600)
        except Exception:
           pass

        # --- NEW: also persist MT5 account/terminal (agent sends top-level account/terminal) ---
        try:
            acct = getattr(payload, "account", None) or {}
            term = getattr(payload, "terminal", None) or {}
            

            def _s(x, n=200):
                try:
                    return str(x)[:n]
                except Exception:
                    return ""

            mapping3 = {}
            if isinstance(acct, dict) and acct:
                for k in (
                    "login", "server", "currency", "trade_mode", "leverage",
                    "balance", "equity", "profit", "trade_allowed", "trade_expert"
                ):
                    if k in acct and acct.get(k) is not None:
                        mapping3[f"mt5_account_{k}"] = _s(acct.get(k), 256)

            if isinstance(term, dict) and term:
                for k in (
                    "name", "company", "connected", "trade_allowed", "dlls_allowed",
                    "build", "path", "data_path"
                ):
                    if k in term and term.get(k) is not None:
                        mapping3[f"mt5_terminal_{k}"] = _s(term.get(k), 512)
            # --- ensure these core fields always exist for Devices UI ---
            try:
                if getattr(payload, "version", None):
                    mapping3["version"] = _s(getattr(payload, "version"), 64)
            except Exception:
                pass

            try:
                if getattr(payload, "label", None):
                    mapping3["label"] = _s(getattr(payload, "label"), 128)
            except Exception:
                pass

            # mt5_ok: derive from terminal.connected if not explicitly provided
            try:
                mt5_ok_val = getattr(payload, "mt5_ok", None)
                if mt5_ok_val is None:
                    mt5_ok_val = bool(term.get("connected")) if isinstance(term, dict) else None
                if mt5_ok_val is not None:
                    mapping3["mt5_ok"] = "1" if bool(mt5_ok_val) else "0"
            except Exception:
                pass


            if mapping3:
                mapping3["mt5_snapshot_at_ms"] = str(int(time.time() * 1000))
                R.hset(_hkey(dev_id), mapping=mapping3)
                R.expire(_hkey(dev_id), 600)
        except Exception:
            log.exception("[OHLC] persist mt5 account/terminal failed dev=%s", dev_id)

        # write device snapshot (primary writer)
        R.set(f"xtl:ohlc:snap:{dev_id}:{sym}:{tf}", json.dumps(snap_val))
        # --- NEW: latest pointer (kills Redis SCAN in infer_rt.py) ---
        try:
            R.set(f"xtl:ohlc:latest:{sym}:{tf}", dev_id)
        except Exception:
            pass



        
        # --- remember which device last pushed OHLC for this user/symbol/tf ---
        # owner_id is already resolved from DB earlier (may be None)
        if owner_id:


            # (A1) Sticky pointer with NO expiry (persists across hours/days)
            R.set(f"xtl:sticky_device:{owner_id}:{sym}:{tf}", dev_id)

            # (A2) Freshness pointer with short TTL (useful for tie-breaking / recency)
            R.setex(f"xtl:last_push_device:{owner_id}:{sym}:{tf}", 7200, dev_id)

            # (A3) Optional metadata for debugging/inspection
            R.hset(
                f"xtl:last_push_device_meta:{owner_id}:{sym}:{tf}",
                mapping={"device_id": dev_id, "written_at": str(int(time.time() * 1000))}
            )


        # write user snapshot (what Trend reads)
        if owner_id:
            kuser = f"xtl:trend:snap:{owner_id}:{sym}:{tf}"
            R.set(kuser, json.dumps(snap_val))
            R.setex(
                f"xtl:broker:meta:{owner_id}:{sym}",
                3600,
                json.dumps({"symbol": sym, "timeframe": tf, "source": "broker"}),
            )
            try:
                R.setex(f"xtl:user:{owner_id}:trend:leader", 180, dev_id)
            except Exception:
                pass

        log.info(f"[SNAPWRITE] key_user={owner_id and kuser} sym={sym} tf={tf} bars={len(bars_sec)}")

    except Exception as e:
        log.info(f"[OHLC] snapshot write error (ignored): {e}")

    return {"ok": True, "received": len(payload.bars or [])}

@r.post("/{dev_id}/price", summary="Ingest live price from agent (compat)")
def post_price(
    dev_id: str,
    payload: dict = Body(...),
    authorization: Optional[str] = Header(default=None),
    x_device_token: Optional[str] = Header(default=None, alias="X-Device-Token"),
    device_token_hdr: Optional[str] = Header(default=None, alias="Device-Token"),
    x_device_id: Optional[str] = Header(default=None, alias="X-Device-Id"),
):
    import os, json, time

    # normalize device id (some agents send X-Device-Id)
    dev = (x_device_id or dev_id or "").strip()

    # gather token from multiple possible headers
    token = ""
    for candidate in (authorization, x_device_token, device_token_hdr):
        if not candidate:
            continue
        parts = str(candidate).split()
        token = parts[-1].strip() if parts else str(candidate).strip()
        if token:
            break

    if not token:
        # breadcrumb: missing token (so we know why 401)
        try:
            R.setex(f"xtl:debug:price_auth:{dev}", 120, "missing_token")
        except Exception:
            pass
        raise HTTPException(status_code=401, detail="missing token")

    owner_id: Optional[str] = None

    # ---- (A) Primary auth: Postgres devices table ----
    try:
        with db() as conn, conn.cursor() as cur:
            cur.execute("SELECT user_id::text, device_token FROM devices WHERE id=%s", (dev,))
            row = cur.fetchone()
            if row and str(row[1]) == token:
                owner_id = row[0]
            else:
                owner_id = None
    except Exception:
        owner_id = None

    # ---- (B) Fallback auth: Redis device hash (prefix) ----
    # This matches how some of your agent/device metadata is stored.
    if owner_id is None:
        try:
            prefix = os.getenv("XTL_DEVICE_KEY_PREFIX", "device:")
            meta = R.hgetall(f"{prefix}{dev}") or {}
            # meta could contain device_token + owner_id/user_id depending on your implementation
            meta_token = (meta.get("device_token") or meta.get("token") or "")
            meta_owner = (meta.get("owner_id") or meta.get("user_id") or None)
            if meta_token and str(meta_token) == token and meta_owner:
                owner_id = str(meta_owner)
        except Exception:
            pass

    if owner_id is None:
        # breadcrumb: token mismatch (so we can inspect quickly)
        try:
            R.setex(f"xtl:debug:price_auth:{dev}", 120, "bad_token_or_unknown_device")
        except Exception:
            pass
        raise HTTPException(status_code=401, detail="invalid token")

    sym_u = str(payload.get("symbol") or "").upper().strip()
    try:
        price = float(payload.get("price") or 0.0)
    except Exception:
        price = 0.0
    # --- ts_ms: prefer tick time from agent; fallback to server time ---
    server_now_ms = int(time.time() * 1000)

    def _coerce_ts_ms(p: dict, default_ms: int) -> int:
        # Prefer ms fields
        for k in ("ts_ms", "time_msc", "tick_ts_ms"):
            try:
                v = p.get(k)
                if v is None:
                    continue
                v = int(v)
                if v > 10_000_000_000:  # ms
                    return v
            except Exception:
                pass

        # Seconds ? ms
        for k in ("ts", "time"):
            try:
                v = p.get(k)
                if v is None:
                    continue
                v = int(v)
                if 1_000_000_000 <= v <= 10_000_000_000:
                    return v * 1000
            except Exception:
                pass

        return int(default_ms)

    ts_ms = _coerce_ts_ms(payload if isinstance(payload, dict) else {}, server_now_ms)

    if not sym_u or price <= 0:
        return {"ok": True, "ignored": True}

    # Weekend-safe persistence (keep last tick)
    ttl = 7 * 24 * 3600  # 7 days

    val = json.dumps({"price": float(price), "ts_ms": int(ts_ms), "src": "tick"}, separators=(",", ":"), ensure_ascii=False)

    # cache (state) — only overwrite if newer
    try:
        def _set_if_newer(key: str) -> None:
            old = R.get(key)
            if old:
                try:
                    oldj = json.loads(old)
                    old_ts = int(oldj.get("ts_ms") or 0)
                    if old_ts > 0 and old_ts >= ts_ms:
                        return  # keep newer existing
                except Exception:
                    pass
            R.setex(key, ttl, val)

        _set_if_newer(f"xtl:price:{dev}:{sym_u}")
        _set_if_newer(f"xtl:price:{sym_u}")
    except Exception:
        pass


    # pubsub (event)
    try:
        evt = {
            "type": "price",
            "symbol": sym_u,
            "price": float(price),
            "device_id": dev,
            "ts_ms": int(ts_ms),
            "src": "agent_price",
        }
        R.publish(f"xtl:pub:price:{owner_id}", json.dumps(evt, separators=(",", ":"), ensure_ascii=False))
    except Exception:
        pass

    # membership (optional)
    try:
        R.sadd(f"xtl:user:{owner_id}:devices", dev)
    except Exception:
        pass

    # breadcrumb: auth OK
    try:
        R.setex(f"xtl:debug:price_auth:{dev}", 120, "ok")
    except Exception:
        pass

    return {"ok": True}


@r.post("/{dev_id}/heartbeat", summary="Agent heartbeat; nudges a push when user snapshot is missing/stale")
def device_heartbeat(
    dev_id: str,
    payload: dict = Body(default={}),
    authorization: Optional[str] = Header(default=None),
):
    import json, time
    from datetime import datetime

    now_ms = int(time.time() * 1000)

    # --- breadcrumb: last heartbeat seen ---
    try:
        R.setex(f"xtl:debug:last_hb:{dev_id}", 300, str(now_ms // 1000))
    except Exception:
        pass

    # --- persist minimal heartbeat facts (non-fatal) ---
    try:
        R.hset(_hkey(dev_id), mapping={
            "last_heartbeat_ms": str(now_ms),
            "last_heartbeat_iso": datetime.utcfromtimestamp(now_ms/1000).isoformat() + "Z",
            "last_heartbeat": str(now_ms // 1000),
            "status": "online",
            "version": str(payload.get("version", "")),
            "label": (payload.get("label") or "")[:64],
            "mt5_ok": "1" if str(payload.get("mt5_ok", "")).lower() in ("1","true","yes") else "0",
            "autostart_ok": "1" if str(payload.get("autostart_ok", "")).lower() in ("1","true","yes") else "0",
        })
        R.expire(_hkey(dev_id), 600)
    except Exception:
        pass
    
    # --- NEW: persist MT5 account/terminal snapshot (best-effort) ---
    try:
       # prefer explicit payload fields; fallback to payload.broker if you ever nest it
       acct = getattr(payload, "account", None) or {}
       term = getattr(payload, "terminal", None) or {}
       if (not acct) and isinstance(getattr(payload, "broker", None), dict):
           acct = (payload.broker or {}).get("account") or {}
       if (not term) and isinstance(getattr(payload, "broker", None), dict):
           term = (payload.broker or {}).get("terminal") or {}
       def _s(x, n=256):
           try:
              return str(x)[:n]
           except Exception:
              return ""

       mapping3 = {}

       # ---- account_info ----
       if isinstance(acct, dict) and acct:
           # common keys from MetaTrader5.account_info()._asdict()
           for k in (
               "login", "server", "currency", "trade_mode", "leverage",
               "balance", "equity", "profit", "trade_allowed", "trade_expert"
           ):
               if k in acct and acct.get(k) is not None:
                    mapping3[f"mt5_account_{k}"] = _s(acct.get(k), 256)

       # ---- terminal_info ----
       if isinstance(term, dict) and term:
           for k in (
               "name", "company", "connected", "trade_allowed", "dlls_allowed",
               "build", "path", "data_path"
           ):
               if k in term and term.get(k) is not None:
                  mapping3[f"mt5_terminal_{k}"] = _s(term.get(k), 512)

       # raw JSON (optional, handy for debugging)
       if mapping3:
            mapping3["mt5_snapshot_at_ms"] = str(int(time.time() * 1000))

            R.hset(_hkey(dev_id), mapping=mapping3)
            R.expire(_hkey(dev_id), 600)
    except Exception:
        pass


    # --- who owns this device? ---
    owner_id: Optional[str] = None
    try:
        with db() as conn, conn.cursor() as cur:
            cur.execute("SELECT user_id::text FROM devices WHERE id=%s", (dev_id,))
            row = cur.fetchone()
            if row:
                owner_id = row[0]
    except Exception:
        owner_id = None
    

    # --- decide whether we need an immediate push from the agent ---
    # prefer last seen symbol; fall back to payload or XAUUSD
    sym = ((R.hget(_hkey(dev_id), "last_ohlc_symbol") or payload.get("symbol") or "XAUUSD")).upper()

    # default TFs if not provided by agent
    tfs = payload.get("tfs") or ["M15", "H1", "H4"]
    # --- best-effort hydration from the FRESHEST device snapshot for this user ---
    try:
        if owner_id:
            # get all devices for this user (include current dev first for efficiency)
            devs = [dev_id]
            try:
                others = [d.decode() if isinstance(d, bytes) else str(d)
                          for d in (R.smembers(f"xtl:user:{owner_id}:devices") or [])]
                for d in others:
                    if d and d != dev_id:
                        devs.append(d)
            except Exception:
                pass

            for tf in tfs:
                symU = sym.upper()
                tfU  = tf.upper()

                best_raw   = None
                best_fresh = -1
                best_dev   = None

                for d in devs:
                    # snapshot payload
                    kdev = f"xtl:ohlc:snap:{d}:{symU}:{tfU}"
                    raw  = R.get(kdev)
                    if not raw:
                        continue

                    # freshness from device hash (cheap), with JSON fallback
                    fresh = 0
                    try:
                        hk = f"xtl:device:{d}"
                        w  = R.hget(hk, "last_ohlc_written_at")
                        t  = R.hget(hk, "last_ohlc_t")
                        wv = int(w) if w else 0
                        tv = int(t) if t else 0
                        fresh = max(fresh, wv, tv)
                    except Exception:
                        pass

                    if fresh == 0:
                        # fallback: peek minimally into JSON (serverNow/lastClosedTs are ms)
                        try:
                            import json as _json
                            js = _json.loads(raw)
                            fresh = max(int(js.get("serverNow", 0)), int(js.get("lastClosedTs", 0)))
                        except Exception:
                            fresh = 0

                    if fresh > best_fresh:
                        best_fresh = fresh
                        best_raw   = raw
                        best_dev   = d

                # if we found any snapshot, copy it to the user key the UI reads
                if best_raw:
                    kuser = f"xtl:trend:snap:{owner_id}:{symU}:{tfU}"
                    # only write if missing/tiny or older (len check is a cheap guard)
                    try:
                        cur_len = R.strlen(kuser)
                    except Exception:
                        cur_len = 0
                    if (not cur_len) or cur_len < 1000:
                        R.setex(kuser,best_raw)

                    # seed tiny broker meta the UI fetches
                    meta = {"symbol": symU, "timeframe": tfU, "source": "broker", "from_device": best_dev}
                    R.setex(f"xtl:broker:meta:{owner_id}:{symU}", 3600, json.dumps(meta))
                    R.setex(f"xtl:last_push_device:{owner_id}:{symU}:{tfU}", 3600, best_dev or dev_id)

    except Exception:
        pass



    TF_MS = {"M1": 60_000, "M5": 300_000, "M15": 900_000, "M30": 1_800_000,
             "H1": 3_600_000, "H4": 14_400_000, "D1": 86_400_000}

    
    need_push = False
    used_global_flag = False
    used_device_flag = False

    try:
       # owner unknown => ask for a seed once
       if not owner_id:
           need_push = True
       else:
           # one-shot overrides (checked before looking at snapshots)
           try:
              if R.get("xtl:trend:push_now"):
                  need_push = True
                  used_global_flag = True
                  R.delete("xtl:trend:push_now")  # consume global one-shot
           except Exception:
               pass
           try:
              if R.get(f"xtl:trend:push_once:{dev_id}"):
                  need_push = True
                  used_device_flag = True
                  R.delete(f"xtl:trend:push_once:{dev_id}")  # consume device one-shot
           except Exception:
               pass

           # evaluate snapshot health per timeframe, unless already decided
           if not need_push:
               for tf in tfs:
                   snap_key = f"xtl:trend:snap:{owner_id}:{sym}:{tf}"
                   raw = R.get(snap_key)
                   # missing/empty/expired key => push
                   try:
                      ttl = R.ttl(snap_key)
                      if (not raw) or (ttl is None) or (ttl <= 0) or (R.strlen(snap_key) == 0):
                          need_push = True
                          break
                   except Exception:
                       need_push = True
                       break

                   # parse and run your existing heuristics
                   try:
                      js = json.loads(raw)
                      bars = js.get("bars") or []
                      server_now = int(js.get("serverNow", 0))
                      last_closed = int(js.get("lastClosedTs", 0))

                      # too few bars for indicators
                      if len(bars) < 205:
                          need_push = True
                          break

                      # clearly stale vs timeframe (allow 2x tf len)
                      if server_now and last_closed:
                          if (server_now - last_closed) > 2 * TF_MS.get(tf, 3_600_000):
                              need_push = True
                              break
                   except Exception:
                       need_push = True
                       break
    except Exception:
        need_push = True  # be safe—ask once

    # --- build response the agent understands ---
    trend_cfg = {
        "active": True,
        "symbols": [sym],
        "tfs": tfs,
        "interval_sec": 60,
        "push_now": bool(need_push),
    }

    # tiny breadcrumb to see what we asked the agent to do
    try:
        trail = dict(trend_cfg)
        if used_global_flag:
            trail["used_global_flag"] = True
        if used_device_flag:
            trail["used_device_flag"] = True
        R.setex(f"xtl:debug:last_hb_push:{dev_id}", 300, json.dumps(trail))
    except Exception:
        pass

    return {"ok": True, "trend": trend_cfg}


@r.get("/compare_ohlc", summary="Compare my bars vs broker (session user)")
async def devices_compare_ohlc(
    symbol: str = "XAUUSD",
    tf: Literal["M15","H1","H4"] = "M15",
    n: int = 50,
    price: Literal["bid","ask","mid"] = "bid",
    digits: Optional[int] = None,
    agent: Optional[str] = None,
    user = Depends(require_session_user),
):
    try:
        uid = _uid(user)
        tf_sec = TF_SEC_CMP[tf]

        # 1) Broker (ground truth) — drop forming bar and enforce TF grid
        broker = await _cmp_fetch_broker_bars(symbol, tf, n + 5, price=price, agent_base=agent)
        now_slot = (int(time.time()) // tf_sec) * tf_sec
        broker = [b for b in broker if int(b["t"]) < now_slot and (int(b["t"]) % tf_sec) == 0][-n:]
        if not broker:
            raise HTTPException(status_code=424, detail="No broker bars fetched")

        # 2) App snapshot for this user — same trimming rules
        app = _cmp_load_app_bars_for_user(uid, symbol, tf, n)
        if not app:
            raise HTTPException(status_code=424, detail="No app bars available")

        # 3) Decide rounding digits (default from broker last close)
        if digits is None:
            sample = str(broker[-1]["c"])
            frac = sample.split(".")[1] if "." in sample else ""
            digits = min(max(len(frac), 2), 6)

        def norm(bs: list[dict]) -> list[dict]:
            out = []
            for b in bs:
                out.append({
                    "t": int(b["t"]),
                    "o": _round_to_digits(b["o"], digits),
                    "h": _round_to_digits(b["h"], digits),
                    "l": _round_to_digits(b["l"], digits),
                    "c": _round_to_digits(b["c"], digits),
                })
            return out

        bN, aN = norm(broker), norm(app)

        # 4) Compare by timestamp
        b_idx = {b["t"]: b for b in bN}
        a_idx = {a["t"]: a for a in aN}

        diffs: list[dict[str, object]] = []
        matched = 0

        for t, bb in b_idx.items():
            aa = a_idx.get(t)
            if not aa:
                continue  # we'll account for missing sets below
            unequal = []
            for k in ("o", "h", "l", "c"):
                if aa[k] != bb[k]:
                    unequal.append({"field": k, "app": aa[k], "broker": bb[k]})
            if unequal:
                diffs.append({"t": t, "diffs": unequal})
            else:
                matched += 1

        # 5) Missing timestamps on each side and overlap count
        def _sec(v: int) -> int:
            return int(v // 1000) if v > 2_000_000_000 else int(v)

        b_ts = {_sec(t) for t in b_idx.keys()}
        a_ts = {_sec(t) for t in a_idx.keys()}

        missing_in_app_ts    = sorted(b_ts - a_ts)   # broker has, app missing
        missing_in_broker_ts = sorted(a_ts - b_ts)   # app has, broker missing
        checked = len(b_ts & a_ts)

        return {
            "symbol": symbol,
            "tf": tf,
            "tfSec": tf_sec,
            "price": price,
            "digits": digits,
            "checked": len(bN),                 # broker bars considered
            "matched": matched,
            "mismatched": len(diffs),
            "missingInApp": len(missing_in_app_ts),
            "missingInBroker": len(missing_in_broker_ts),
            "diffs": diffs[:100],
            "checkedOverlap": checked,          # (new) overlap size
            "missingInAppTS": missing_in_app_ts,
            "missingInBrokerTS": missing_in_broker_ts,
        }
    except HTTPException:
        raise
    except Exception as e:
        log.exception("devices/compare_ohlc failed")
        raise HTTPException(status_code=500, detail=f"compare_ohlc failed: {e.__class__.__name__}: {e}")


# ---- Pairing / Binding (installer flow) -------------------------------------



def _new_device_id() -> str: return "dev_" + uuid.uuid4().hex
def _new_token() -> str:     return secrets.token_urlsafe(32)
def _new_code(n=6) -> str:   return ''.join(random.choices(string.digits, k=n))
def _now() -> datetime:      return datetime.now(timezone.utc)


@r.get("/{dev_id}/debug", summary="Debug a device (owner & live state)")
def device_debug(dev_id: str, user = Depends(require_session_user)):
    now_ms = int(time.time() * 1000)

    # DB owner
    db_owner = None
    db_name  = ""
    with db() as conn, conn.cursor(cursor_factory=_extras.DictCursor) as cur:
        cur.execute("SELECT id, user_id, COALESCE(name,'') AS name, status, updated_at FROM devices WHERE id=%s", (dev_id,))
        row = cur.fetchone()
        if row:
            db_owner = str(row["user_id"]) if row["user_id"] is not None else None
            db_name  = row["name"]

    # Redis meta
    meta = _hgetall_str(_hkey(dev_id)) or {}
    ms = None
    ms_str = (meta.get("last_heartbeat_ms") or "").strip()
    if ms_str.isdigit():
        ms = int(ms_str)
    else:
        sec_str = (meta.get("last_heartbeat") or "").strip()
        if sec_str:
            try:
                ms = int(float(sec_str) * 1000.0)
            except ValueError:
                ms = None
    fresh = bool(ms and (now_ms - ms) <= FRESH_MS)

    # Redis membership sets that include this device (scan a few typical ones)
    sets = {}
    try:
        # If you know your own user id:
        uid = _uid(user)
        if uid:
            sets[f"xtl:user:{uid}:devices"] = bool(R.sismember(f"xtl:user:{uid}:devices", dev_id))
    except Exception:
        pass

    return {
        "device_id": dev_id,
        "db_owner_id": db_owner,
        "db_name": db_name,
        "redis_owner_id": meta.get("owner_id"),
        "in_user_devices_set": sets,
        "last_heartbeat_ms": ms,
        "fresh_online": fresh,
        "raw_meta": meta,
    }


@r.get("/ping", summary="Liveness")
def devices_ping():
    return {"ok": True, "ts": int(time.time())}

@r.post("/pair/start", summary="Create pending device (installer)")
def devices_pair_start():
    """No-auth endpoint called by the Windows installer."""
    dev_id = _new_device_id()
    token  = _new_token()
    code   = _new_code(6)
    exp    = _now() + timedelta(minutes=10)

    with db() as conn, conn.cursor() as cur:
        # 0) Ensure schema bits we rely on exist (idempotent)
        #    - device_claims table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS device_claims(
            device_id   TEXT PRIMARY KEY,
            token       TEXT NOT NULL,
            code        TEXT NOT NULL,
            status      TEXT NOT NULL DEFAULT 'pending',  -- pending | bound | expired
            user_id     TEXT NULL,                        -- set at bind time
            expires_at  TIMESTAMPTZ NOT NULL,
            created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS device_claims_code_idx  ON device_claims(code)")
        cur.execute("CREATE INDEX IF NOT EXISTS device_claims_token_idx ON device_claims(token)")

        #    - devices columns we write to (safe if already present)
        cur.execute("""
        DO $$
        BEGIN
          IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='devices' AND column_name='status'
          ) THEN
            ALTER TABLE devices
              ADD COLUMN user_id         TEXT,
              ADD COLUMN status          TEXT,
              ADD COLUMN device_token    TEXT,
              ADD COLUMN pair_code       TEXT,
              ADD COLUMN pair_expires_at TIMESTAMPTZ,
              ADD COLUMN created_at      TIMESTAMPTZ DEFAULT now(),
              ADD COLUMN updated_at      TIMESTAMPTZ DEFAULT now();
          END IF;
        END$$;
        """)

        # 1) Insert/refresh pending device row
        cur.execute("""
        INSERT INTO devices (
            id, user_id, status, device_token, pair_code, pair_expires_at, created_at, updated_at
        ) VALUES (
            %s, NULL, 'pending', %s, %s, %s, now(), now()
        )
        ON CONFLICT (id) DO UPDATE SET
            status          = 'pending',
            device_token    = EXCLUDED.device_token,
            pair_code       = EXCLUDED.pair_code,
            pair_expires_at = EXCLUDED.pair_expires_at,
            updated_at      = now()
        """, (dev_id, token, code, exp))

        # 2) Upsert claim used by binder
        cur.execute("""
        INSERT INTO device_claims (
            device_id, token, code, status, user_id, expires_at, created_at, updated_at
        ) VALUES (
            %s, %s, %s, 'pending', NULL, %s, now(), now()
        )
        ON CONFLICT (device_id) DO UPDATE SET
            token      = EXCLUDED.token,
            code       = EXCLUDED.code,
            status     = 'pending',
            user_id    = NULL,
            expires_at = EXCLUDED.expires_at,
            updated_at = now()
        """, (dev_id, token, code, exp))

        conn.commit()

    # 3) Seed live state (best effort)
    try:
        R.hset(_hkey(dev_id), mapping={"status": "pending", "active": "1"})
    except Exception:
        pass  # don't fail pairing because Redis is momentarily unavailable

    return {
        "ok": True,
        "device_id": dev_id,
        "device_token": token,
        "pair_code": code,
        "expires_in": int((exp - _now()).total_seconds()),
    }

def _ensure_token_table(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS user_download_tokens(
            token TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            expires_at TIMESTAMPTZ NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
    """)


# Backwards-compatible installer link minting: both paths, both methods
@r.post("/user/installer-url", tags=["devices"])
@r.get ("/user/installer-url", tags=["devices"])
@r.post("/devices/user/installer-url", tags=["devices"])
@r.get ("/devices/user/installer-url", tags=["devices"])
def get_installer_url(
    user = Depends(require_auth_and_mfa),
    protected: bool = Query(True, description="Return password-protected ZIP link (default)"),
):
    # 0) Preflight: base exe must exist (same as before)
    base_exe = os.getenv("XTL_BASE_EXE", os.path.abspath(os.path.join(os.getcwd(), "dist", "xtl", "xtl.exe")))
    if not os.path.exists(base_exe):
        raise HTTPException(status_code=503, detail="Installer is not available right now. Please try again later.")

    # 1) Resolve canonical user id (keep yesterday’s behavior)
    raw_uid = str(_uid(user) or "").strip()
    if not raw_uid:
        raise HTTPException(status_code=401, detail="Invalid auth context")

    uname = email = None
    try:
        if isinstance(user, dict):
            uname = (user.get("username") or "").strip() or None
            email = (user.get("email") or "").strip() or None
        else:
            uname = (getattr(user, "username", "") or "").strip() or None
            email = (getattr(user, "email", "") or "").strip() or None
    except Exception:
        pass

    tok = secrets.token_urlsafe(32)

    # Generate a password only if protected=1
    zip_password = None
    if protected:
        import string
        alphabet = string.ascii_letters + string.digits
        zip_password = "".join(secrets.choice(alphabet) for _ in range(12))

    with db() as conn, conn.cursor() as cur:
        # Ensure table and column exist (idempotent)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS user_download_tokens(
                token        text PRIMARY KEY,
                user_id      uuid NOT NULL,
                expires_at   timestamptz NOT NULL,
                created_at   timestamptz NOT NULL
            )
        """)
        cur.execute("ALTER TABLE user_download_tokens ADD COLUMN IF NOT EXISTS zip_password text")

        # Canonicalize user id
        canon_uid = None
        cur.execute("SELECT id::text FROM users WHERE id::text=%s LIMIT 1", (raw_uid,))
        row = cur.fetchone()
        if row: canon_uid = row[0]
        if not canon_uid and uname:
            cur.execute("SELECT id::text FROM users WHERE username=%s LIMIT 1", (uname,))
            row = cur.fetchone()
            if row: canon_uid = row[0]
        if not canon_uid and email:
            cur.execute("SELECT id::text FROM users WHERE email=%s LIMIT 1", (email,))
            row = cur.fetchone()
            if row: canon_uid = row[0]
        if not canon_uid:
            raise HTTPException(status_code=401, detail="Unable to resolve canonical user id")

        # Store token (+ optional password)
        cur.execute(
            """
            INSERT INTO user_download_tokens(token, user_id, expires_at, created_at, zip_password)
            VALUES (%s, %s, now() + interval '15 minute', now(), %s)
            """,
            (tok, canon_uid, zip_password),
        )
        conn.commit()

    # 2) Return proxied URL + password (if any)
    base = "/_api/devices/download/xtl.zip"
    url  = f"{base}?t={tok}&p=1" if protected else f"{base}?t={tok}"
    return {
        "url": url,
        "protected": bool(protected),
        "password": zip_password,      # <- UI shows this
        "expires_in_minutes": 15,
    }


def _uid_from_cookie_db(session_id: str) -> Optional[str]:
    """
    Resolve a user_id from a raw session cookie by checking common session tables.
    Tries multiple schemas/column patterns; returns None if not found.
    """
    if not session_id:
        return None

    # (table, sql, params) attempts – ordered; harmless if a table doesn't exist
    attempts = [
        ("sessions[id,user_id,expires_at]",
         "SELECT user_id FROM sessions WHERE id=%s AND (expires_at IS NULL OR expires_at>now())",
         (session_id,)),
        ("sessions[session_id,user_id,expires_at]",
         "SELECT user_id FROM sessions WHERE session_id=%s AND (expires_at IS NULL OR expires_at>now())",
         (session_id,)),
        ("user_sessions[id,user_id,expires_at]",
         "SELECT user_id FROM user_sessions WHERE id=%s AND (expires_at IS NULL OR expires_at>now())",
         (session_id,)),
        ("user_sessions[session_id,user_id,expires_at]",
         "SELECT user_id FROM user_sessions WHERE session_id=%s AND (expires_at IS NULL OR expires_at>now())",
         (session_id,)),
        ("web_sessions[id,user_id,expires_at]",
         "SELECT user_id FROM web_sessions WHERE id=%s AND (expires_at IS NULL OR expires_at>now())",
         (session_id,)),
        ("auth_sessions[id,user_id,expires_at]",
         "SELECT user_id FROM auth_sessions WHERE id=%s AND (expires_at IS NULL OR expires_at>now())",
         (session_id,)),
    ]

    try:
        with db() as conn, conn.cursor() as cur:
            for label, sql, params in attempts:
                try:
                    cur.execute(sql, params)
                    row = cur.fetchone()
                    if row and row[0]:
                        return str(row[0])
                except psycopg2.Error:
                    # table/column might not exist; continue
                    conn.rollback()
                    continue
    except Exception:
        return None

    return None



@r.api_route(
    "/download/self/xtl.zip",
    methods=["GET", "HEAD"],
    summary="Download per-user ZIP using session (no explicit token)",
    tags=["devices"],
)
def download_self_zip(request: Request, protected: int = 0):
    """
    Resolve the logged-in user from (in order):
      1) request.state.user / your helper _uid_from(request)
      2) common session cookie names (env + well-known)
      3) ANY cookie the browser sent (iterate all)
      4) relaxed/current user helpers (your existing fallbacks)
    Then mint a short-lived token and 307-redirect to /devices/download/xtl.zip?t=...
    """
    uid = None

    # 1) try state.user via your helper
    try:
        uid = _uid_from(request)
    except Exception:
        uid = None

    # 2) try specific cookie names (env + common variants)
    if not uid:
        import os
        cookie_names = list(dict.fromkeys([
            os.getenv("SESSION_COOKIE_NAME", "session"),
            "session", "sid", "sessionid",
        ]))
        for name in cookie_names:
            sid = (request.cookies.get(name) or "").strip()
            if not sid:
                continue
            try:
                uid = _uid_from_cookie_db(sid)
            except Exception:
                uid = None
            if uid:
                break

    # 3) last cookie-based fallback: try *every* cookie value the browser sent
    if not uid:
        for _, val in (request.cookies or {}).items():
            sid = (val or "").strip()
            if not sid:
                continue
            try:
                uid = _uid_from_cookie_db(sid)
            except Exception:
                uid = None
            if uid:
                break

    # 4) your existing relaxed/current-user helpers
    if not uid:
        try:
            user = get_current_user(request)
            uid = _uid(user) if user else None
        except Exception:
            pass
    if not uid:
         u = _session_user(request)
         uid = _uid(u) if u else None
    if not uid:
        try:
            user = require_user(request)
            uid = _uid(user) if user else None
        except Exception:
            pass

    if not uid:
        # still nothing: return your existing 401
        raise HTTPException(status_code=401, detail="Signin required")

    # --- mint short-lived download token (unchanged) ---
    tok = secrets.token_urlsafe(32)
    with db() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO user_download_tokens(token, user_id, expires_at, created_at) "
            "VALUES (%s, %s, now() + interval '60 minute', now())",
            (tok, uid),
        )
        conn.commit()

    p = 1 if str(protected) == "1" else 0
    if request.method == "HEAD":
        return Response(status_code=204)

    return RedirectResponse(
        url=f"/devices/download/xtl.zip?t={tok}&p={p}",
        status_code=307,
    )


@r.api_route(
    "/download/xtl.zip",
    methods=["GET", "HEAD"],
    summary="Download per-user ZIP (token auth; optional password)",
    tags=["devices"],
)
def download_zip_by_token(request: Request):
    # --- query + token resolve (unchanged) ---
    qp = request.query_params
    t = (qp.get("t") or "").strip()
    p = 1 if qp.get("p") == "1" else 0  # p=1 => password-protected ZIP

    if not t:
        uid = _uid_from(request) or _uid(_session_user(request) or {}) if _session_user(request) else None
        if not uid:
            raise HTTPException(status_code=401, detail="Signin required")
        t = secrets.token_urlsafe(32)
        with db() as conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO user_download_tokens(token, user_id, expires_at, created_at) "
                "VALUES (%s, %s, now() + interval '15 minute', now())",
                (t, uid),
            )
            conn.commit()

    with db() as conn, conn.cursor(cursor_factory=_extras.DictCursor) as cur:
        cur.execute(
            "SELECT user_id, zip_password FROM user_download_tokens "
            "WHERE token=%s AND expires_at>now()",
            (t,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Invalid or expired token")
        owner_id   = str(row[0])
        zip_pwd_db = (row[1] or "").strip()

    base_exe = os.getenv(
        "XTL_BASE_EXE",
        os.path.abspath(os.path.join(os.getcwd(), "dist", "xtl", "xtl.exe")),
    )
    exe_path = Path(base_exe)
    if not exe_path.exists():
        if request.method == "HEAD":
            raise HTTPException(status_code=503, detail="Installer not available")
        raise HTTPException(status_code=500, detail="Base installer not found on server")

    is_onedir = exe_path.parent.name.lower() == exe_path.stem.lower()
    base_dir = exe_path.parent if is_onedir else None

    def _norm(v: str) -> str:
        v = (v or "").strip()
        if v.startswith("http://"):
            v = "https://" + v[len("http://") :]
        v = v.rstrip("/")
        host = v.split("://", 1)[1] if "://" in v else v
        if host.startswith("app."):
            host = "api." + host[4:]
        if not host.startswith("api."):
            host = "api." + host
        return "https://" + host

    api_base = _norm(os.getenv("XTL_API_BASE", "https://api.xautrendlab.com"))
    version  = os.getenv("XTL_VERSION", "1.0.0")
    top      = f"xtl_agent_{version}"
    cfg_bytes = json.dumps({"api_base": api_base, "bind_token": t},
                           separators=(",", ":")).encode("utf-8")

    if request.method == "HEAD":
        return Response(status_code=200)

    # --- single builder to avoid duplicates ---
    def _emit_zip(protected: bool) -> Response:
        buf = io.BytesIO()

        import pyzipper  # ensure available in your env
        with pyzipper.AESZipFile(
            buf, "w",
            compression=pyzipper.ZIP_DEFLATED,
            encryption=pyzipper.WZ_AES
        ) as z:
            if protected:
                pwd = (zip_pwd_db or f"XTL-{t[:6]}{t[-4:]}").encode("utf-8")
                z.setpassword(pwd)
                z.setencryption(pyzipper.WZ_AES, nbits=256)

            # Copy payload (skip any existing launchers)
            if is_onedir:
                for fs_path in base_dir.rglob("*"):
                    if fs_path.is_dir():
                        continue
                    rel = fs_path.relative_to(base_dir).as_posix()
                    nm = rel.replace("\\", "/").lower()
                    if nm.endswith("/run-xtl.bat") or nm.endswith("/run-xtl.cmd") or nm == "run-xtl.bat" or nm == "run-xtl.cmd":
                        continue  # <-- critical: skip duplicates from build output
                    if rel.lower() == "xtl.exe":
                        z.write(fs_path, f"{top}/xtl.bin")
                    else:
                        z.write(fs_path, f"{top}/{rel}")
            else:
                z.write(exe_path, f"{top}/xtl.bin")

            # Sidecars (write ONCE)
            z.writestr(f"{top}/xtl.cfg", cfg_bytes)

            # The long, elevated bootstrap (write ONCE)
            launcher = (
                '@echo off\r\n'
                'setlocal EnableExtensions EnableDelayedExpansion\r\n'
                'cd /d "%~dp0"\r\n'
                '\r\n'
                'set "LOG=%TEMP%\\xtl_install_bootstrap.log"\r\n'
                'echo ==== %date% %time% Run-XTL.bat start ====>>"%LOG%"\r\n'
                'echo cwd=%cd%>>"%LOG%"\r\n'
                '\r\n'
                '>nul 2>&1 net session\r\n'
                'if not "%errorlevel%"=="0" (\r\n'
                '  echo Elevating via PowerShell...>>"%LOG%"\r\n'
                '  powershell -NoProfile -ExecutionPolicy Bypass -Command ^\r\n'
                '    "Start-Process -FilePath \'%~f0\' -Verb RunAs"\r\n'
                '  exit /b\r\n'
                ')\r\n'
                '\r\n'
                'if not exist "xtl.exe" if exist "xtl.bin" ren "xtl.bin" "xtl.exe"\r\n'
                'if not exist "xtl.exe" (\r\n'
                '  echo ERROR: xtl.exe/xtl.bin not found beside this script.>>"%LOG%"\r\n'
                '  echo ERROR: xtl.exe/xtl.bin not found beside this script.\r\n'
                '  pause & exit /b 1\r\n'
                ')\r\n'
                '\r\n'
                'powershell -NoProfile -ExecutionPolicy Bypass -Command ^\r\n'
                '  "if (Get-Command Unblock-File -EA SilentlyContinue) { Unblock-File -Path \'%~dp0xtl.exe\' }"\r\n'
                '\r\n'
                '"%~dp0xtl.exe" install || (echo INSTALL FAILED & pause & exit /b 1)\r\n'
                '"%~dp0xtl.exe" start   || (echo START FAILED   & pause & exit /b 1)\r\n'
                '\r\n'
                'echo SUCCESS: XTL installed and started.\r\n'
                'echo Log: C:\\Program Files\\XTL\\dist\\xtl\\xtl_agent.log\r\n'
                'pause\r\n'
            )
            z.writestr(f"{top}/Run-XTL.bat", launcher)

            z.writestr(
                f"{top}/README.txt",
                "1) Extract the ZIP\r\n2) Double-click Run-XTL.bat (Run as admin)\r\n",
            )

        data = buf.getvalue()
        headers = {
            "Content-Disposition": f'attachment; filename="{top}.zip"',
            "Content-Type": "application/octet-stream",
            "Content-Length": str(len(data)),
            "Cache-Control": "no-store, no-transform",
            "Pragma": "no-cache",
            "Expires": "0",
            "X-Content-Type-Options": "nosniff",
            "X-Download-Options": "noopen",
        }
        return Response(content=data, media_type="application/octet-stream", headers=headers)

    # Choose ONE branch and return immediately
    if p == 1:
        return _emit_zip(protected=True)
    else:
        return _emit_zip(protected=False)





class BindBody(BaseModel):
    device_id: str
    bind_token: str

@r.post("/pair/bind", summary="Bind a device to the user who owns bind_token")
def devices_pair_bind(body: BindBody):
    device_id = (body.device_id or "").strip()
    token     = (body.bind_token or "").strip()
    if not device_id or not token:
        raise HTTPException(status_code=400, detail="device_id and bind_token are required")

    # 1) Resolve token -> user_id (must be valid and unexpired)
    with db() as conn, conn.cursor(cursor_factory=_extras.DictCursor) as cur:
        cur.execute("""
            SELECT user_id::text
              FROM user_download_tokens
             WHERE token=%s AND expires_at>now()
             LIMIT 1
        """, (token,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=403, detail="Invalid or expired bind token")
        user_id = str(row[0])

        # 2) Ensure device exists, then bind it (idempotent if already bound to same user)
        cur.execute("SELECT id, user_id FROM devices WHERE id=%s LIMIT 1", (device_id,))
        rowd = cur.fetchone()
        if not rowd:
            raise HTTPException(status_code=404, detail="Device not found")
        if rowd["user_id"] and str(rowd["user_id"]) != user_id:
            raise HTTPException(status_code=409, detail="Device is bound to another user")

        cur.execute("""
            UPDATE devices
               SET user_id=%s, status='active', updated_at=now()
             WHERE id=%s
        """, (user_id, device_id))
        # Optionally consume the token so it can't be reused
        cur.execute("DELETE FROM user_download_tokens WHERE token=%s", (token,))
        conn.commit()

    # 3) Annotate Redis (best effort)
    try:
        R.hset(_hkey(device_id), mapping={"status":"active","active":"1","owner_id":user_id})
        R.sadd(f"xtl:user:{user_id}:devices", device_id)
    except Exception:
        pass

    return {"ok": True, "bound": True, "user_id": user_id, "device_id": device_id}


@r.get("/pair/status", summary="Installer poll: is device bound?")
def devices_pair_status(device_id: str):
    if not device_id or not device_id.startswith("dev_"):
        raise HTTPException(status_code=400, detail="Invalid device_id")
    with db() as conn, conn.cursor() as cur:
        cur.execute("SELECT user_id FROM devices WHERE id=%s", (device_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Unknown device")
        paired = bool(row[0])
    return {"paired": paired}


@r.post("/pair/confirm", summary="Manual 6-digit code (kept for future, not used in silent path)")
def devices_pair_confirm(code: str, user = Depends(require_auth_and_mfa)):
    code_norm = (code or "").replace("-", "").strip().upper()
    if not code_norm or len(code_norm) < 6:
        raise HTTPException(status_code=400, detail="Invalid code")
    user_id = str(user if isinstance(user, (str, int)) else (user.get("id") or user.get("user_id") or user.get("sub")))
    with db() as conn, conn.cursor() as cur:
        cur.execute("""
            UPDATE devices
               SET user_id = %s, status = 'active',
                   pair_code=NULL, pair_expires_at=NULL, updated_at=now()
             WHERE pair_code = %s
               AND pair_expires_at > now()
               AND status = 'pending'
         RETURNING id
        """, (user_id, code_norm))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Invalid or expired code")
        dev_id = row[0]
    _bind_device_to_user(user_id, dev_id)
    return {"ok": True, "device_id": dev_id}

# ---- Agent heartbeat ---------------------------------------------------------

def _now_ms() -> int:
    return int(time.time() * 1000)

def _bool1(v: bool) -> str:
    return "1" if v else "0"






# ---- Devices UI (list / get / rename / toggle / delete) ----------------------


@r.post("/claim_recent", summary="Bind recent device from my IP to me")
def claim_recent(request: Request):
    """
    Find the most recent device that posted a heartbeat from this IP in the last 3 minutes
    and bind it to the signed-in user.
    """
    # 1) identify caller and IP
    try:
        user = getattr(request.state, "user", None)
        owner_id = getattr(user, "id", None) or getattr(user, "user_id", None)	
    except Exception:
        owner_id = None
    if not owner_id:
        return {"ok": False, "error": "not_authenticated"}

    caller_ip = ""
    try:
        caller_ip = request.client.host or ""
    except Exception:
        pass
    if not caller_ip:
        return {"ok": False, "error": "no_ip"}

    # 2) scan recent devices (narrow scan: only device hashes)
    now_ms = int(time.time() * 1000)
    horizon = 3 * 60 * 1000  # 3 minutes
    best = None
    best_ts = -1

    # NOTE: if you keep a separate index of device ids, iterate that here.
    # Otherwise, a small scan/smembers against your known device keys is fine.
    for key in R.scan_iter(match=f"{DEVICE_PREFIX}*", count=500):
        try:
            h = R.hgetall(key)
            if not h: 
                continue
            ip = (h.get(b"last_seen_ip") or b"").decode("utf-8", "ignore")
            if ip != caller_ip:
                continue
            tsb = h.get(b"last_heartbeat_ms")
            ts = int(tsb.decode() if isinstance(tsb, (bytes, bytearray)) else int(tsb)) if tsb else 0
            if ts <= 0: 
                continue
            age = now_ms - ts
            if 0 <= age <= horizon and ts > best_ts:
                best_ts = ts
                best = key
        except Exception:
            continue

    if not best:
        return {"ok": False, "error": "no_recent_device_from_ip"}

    # 3) bind it to this user
    try:
        raw = best.decode() if isinstance(best, (bytes, bytearray)) else str(best)
        dev_id = raw.split(DEVICE_PREFIX, 1)[-1]
        _bind_device_to_user(owner_id, dev_id)
        return {"ok": True, "device_id": dev_id}
    except Exception as e:
        return {"ok": False, "error": f"bind_failed: {e}"}


class DeviceHeartbeat(BaseModel):
    mt5_ok: Optional[bool] = None
    api_ok: Optional[bool] = None
    autostart_ok: Optional[bool] = None
    version: Optional[str] = None
    last_error: Optional[str] = None
    status: Optional[Literal["ok","running","idle","busy","error"]] = None

    # Pydantic v2 style config
    model_config = ConfigDict(extra="ignore")

# Force resolution of any refs (prevents “class-not-fully-defined”)
DeviceHeartbeat.model_rebuild()




@r.post("/{dev_id}/trend", summary="Toggle/Configure Trend")
def set_trend(
    dev_id: str,
    payload: dict = Body(...),                         # {active, symbols, tfs, interval_sec, push_now}
    authorization: Optional[str] = Header(default=None)
):
    # --- auth: same as heartbeat ---
    token = ""
    if authorization:
        parts = authorization.split()
        token = parts[-1] if parts else authorization.strip()
    if not token:
        raise HTTPException(status_code=401, detail="missing token")

    # verify device + token, and grab owner for authorization
    owner_id: Optional[str] = None
    try:
        with db() as conn, conn.cursor() as cur:
            cur.execute("SELECT user_id, device_token FROM devices WHERE id=%s", (dev_id,))
            row = cur.fetchone()
            if (not row) or (str(row[1]) != token):
                raise HTTPException(status_code=401, detail="invalid token")
            owner_id = str(row[0]) if row[0] else None
    except HTTPException:
        raise
    except Exception:
        pass

    # --- parse & clamp inputs safely ---
    active = bool(payload.get("active", False))
    symbols = payload.get("symbols") or []
    tfs     = payload.get("tfs") or []
    iv      = payload.get("interval_sec", 60)
    push_now = bool(payload.get("push_now", False))

    try:
        iv = int(iv)
    except Exception:
        iv = 60
    iv = max(15, min(iv, 600))                         # 15s .. 10m

    if not isinstance(symbols, list):
        symbols = []
    symbols = [str(s).strip() for s in symbols if str(s).strip()]

    VALID_TFS = {"M1","M5","M15","M30","H1","H4","D1","W1","MN1"}
    if not isinstance(tfs, list):
        tfs = []
    tfs = [str(t).strip().upper() for t in tfs if str(t).strip()]
    tfs = [t for t in tfs if t in VALID_TFS]

    # --- persist to Redis (read by heartbeat response) ---
    mapping = {
        "trend_active": "1" if active else "0",
        "trend_symbols": ",".join(symbols) if symbols else "",
        "trend_tfs": ",".join(tfs) if tfs else DEFAULT_TFS_CSV,
        "trend_interval_sec": str(iv),
    }
    if push_now:
        mapping["trend_push_now"] = "1"

    R.hset(_hkey(dev_id), mapping=mapping)
    R.expire(_hkey(dev_id), 600)

    return {"ok": True}

@r.get("",  include_in_schema=False)
@r.get("/", summary="List devices for current user")
def list_devices(user = Depends(require_session_user)):
    user_id = _uid(user)
    if not user_id:
        raise HTTPException(status_code=401, detail="Unauthorized")

    now_ms = int(time.time() * 1000)

    # 1) DB roster for this user (label map + order; UUID-safe compare)
    with db() as conn, conn.cursor(cursor_factory=_extras.DictCursor) as cur:
        cur.execute("""
            SELECT id::text AS id, COALESCE(name,'')::text AS name
              FROM devices
             WHERE user_id::text = %s
          ORDER BY updated_at DESC NULLS LAST, id ASC
        """, (user_id,))
        rows = cur.fetchall() or []
    label_by_id = {r["id"]: (r["name"] or "") for r in rows}
    db_ids = list(label_by_id.keys())

    # 2) Redis membership set (added on bind/heartbeat)
    try:
        rids = R.smembers(f"xtl:user:{user_id}:devices") or set()
        # normalize bytes -> str
        rids = {x.decode() if isinstance(x, (bytes, bytearray)) else str(x) for x in rids if x}
    except Exception:
        rids = set()

    # 3) Recent device hashes that claim this owner (fresh only)
    try:
        extra = set()
        for key in R.scan_iter(f"{DEVICE_PREFIX}*", count=500):
            # key like "xtl:dev:<id>"
            rawk = key.decode() if isinstance(key, (bytes, bytearray)) else str(key)
            if not rawk:
                continue
            meta = _hgetall_str(key)
            if not meta:
                continue
            if str(meta.get("owner_id") or "") != str(user_id):
                continue

            ms = None
            ms_str = (meta.get("last_heartbeat_ms") or "").strip()
            if ms_str.isdigit():
                ms = int(ms_str)
            else:
                sec_str = (meta.get("last_heartbeat") or "").strip()
                if sec_str:
                    try:
                        ms = int(float(sec_str) * 1000.0)
                    except ValueError:
                        ms = None
            if not ms or (now_ms - ms) > FRESH_MS:
                continue

            # extract device id after prefix
            dev_id = rawk.split(DEVICE_PREFIX, 1)[-1]
            if dev_id:
                extra.add(dev_id)

        rids |= extra
    except Exception:
        pass

    # 4) Union (keep DB order; append Redis-only)
    all_ids = list(dict.fromkeys(db_ids + [i for i in rids if i not in db_ids]))

    def as01(b: bool) -> str: return "1" if b else "0"

    devices = []
    for dev_id in all_ids:
        lbl = label_by_id.get(dev_id, "")
        meta = _hgetall_str(_hkey(dev_id)) or {}

        # resolve last heartbeat (ms or sec fallback)
        ms = None
        ms_str = (meta.get("last_heartbeat_ms") or "").strip()
        if ms_str.isdigit():
            ms = int(ms_str)
        else:
            sec_str = (meta.get("last_heartbeat") or "").strip()
            if sec_str:
                try:
                    ms = int(float(sec_str) * 1000.0)
                except ValueError:
                    ms = None

        fresh = _is_fresh(ms, now_ms)

        devices.append({
            "device_id": dev_id,
            "label": lbl,
            "mt5_ok":       as01(_truthy(meta, "mt5_ok")       and fresh),
            "api_ok":       as01(_truthy(meta, "api_ok")       and fresh),
            "autostart_ok": as01(_truthy(meta, "autostart_ok") and fresh),
            "last_heartbeat_ms": ms if ms is not None else None,
            "status": "online" if fresh else "offline",
            "active": "1",
            "version": meta.get("version") or None,
            "last_error": meta.get("last_error") or None,
        })

    return {"devices": devices}


class RenameBody(BaseModel):
    label: str = Field(..., min_length=1, max_length=120)

@r.patch("/{dev_id}", summary="Rename a device")
def rename_device(dev_id: str, body: RenameBody, user = Depends(require_session_user)):
    user_id = _uid(user)
    if not user_id:
        raise HTTPException(status_code=401, detail="Unauthorized")
    user_id = str(user_id)

    new_label = (body.label or "").strip()
    if not new_label:
        raise HTTPException(status_code=422, detail="label is required")

    try:
        with db() as conn, conn.cursor() as cur:
            # 404 if device id doesn’t exist
            cur.execute("SELECT 1 FROM devices WHERE id=%s", (dev_id,))
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="Device not found")

            # update only if owned by this user; ::text avoids uuid/text mismatch
            cur.execute("""
                UPDATE devices
                   SET name=%s, updated_at=NOW()
                 WHERE id=%s AND user_id::text=%s
            """, (new_label, dev_id, user_id))
            if cur.rowcount == 0:
                raise HTTPException(status_code=403, detail="Forbidden: device not owned by user")
            conn.commit()
    except HTTPException:
        raise
    except Exception as e:
        print(f"[ERROR] rename_device failed for {dev_id}: {e}")
        raise HTTPException(status_code=500, detail="Rename failed")

    try:
        R.hset(_hkey(dev_id), mapping={"name": new_label})
    except Exception:
        pass

    return {"ok": True, "device_id": dev_id, "label": new_label}
from urllib.parse import urlparse



def _same_origin(request: Request) -> bool:
    ref = request.headers.get("Origin") or request.headers.get("Referer")
    if not ref:
        return False
    r = urlparse(ref); here = urlparse(str(request.url))
    return (r.scheme, r.netloc) == (here.scheme, here.netloc)

@r.delete("/{device_id}", dependencies=[Depends(csrf_protect), Depends(require_perm("devices:write"))])
def delete_device(device_id: str, user = Depends(require_session_user)):
    user_id = _uid(user)
    if not user_id:
        raise HTTPException(status_code=401, detail="Unauthorized")

    # verify ownership, delete from DB
    with db() as conn, conn.cursor(cursor_factory=_extras.DictCursor) as cur:
        _ = ensure_owner(cur, device_id, user_id)  # raises 404 if not owned
        cur.execute("DELETE FROM devices WHERE id=%s", (device_id,))
        conn.commit()

    # best-effort Redis cleanup so UI updates immediately
    try:
        R.srem(_user_devices_key(user_id), device_id)
        R.delete(_hkey(device_id))
    except Exception:
        pass

    return {"ok": True}

# ---- Per-user installer download (silent bind; no config file left behind) ---

def _normalize_api_host(v: str) -> str:
    v = (v or "").strip()
    if v.startswith("http://"): v = "https://" + v[len("http://"):]
    v = v.rstrip("/")
    host = v.split("://",1)[1] if "://" in v else v
    if host.startswith("app."): host = "api." + host[4:]
    if not host.startswith("api."): host = "api." + host
    return "https://" + host

def _make_embedded_config(user_id: str, token: str, api_base: str) -> bytes:
    cfg = {"api_base": api_base, "bind_token": token}
    blob = json.dumps(cfg, separators=(",",":")).encode("utf-8")
    return b"\n#XTL_CFG_START\n" + blob + b"\n#XTL_CFG_END\n"

@r.get("/user/installer", summary="Download per-user installer (EXE with embedded bind_token)")
def download_user_installer(user = Depends(require_auth_and_mfa)):
    base_exe = os.getenv("XTL_BASE_EXE", os.path.abspath(os.path.join(os.getcwd(), "dist", "xtl.exe")))
    if not os.path.exists(base_exe):
        raise HTTPException(status_code=500, detail="Base installer not found on server")

    # Detect OneDir: parent folder name equals exe stem (e.g., dist/xtl/xtl.exe)
    exe_path = Path(base_exe)
    is_onedir = exe_path.parent.name.lower() == exe_path.stem.lower()
    if is_onedir:
        # Returning 409 nudges the frontend to call the ZIP route
        return JSONResponse(
            status_code=409,
            content={"detail": "OneDir base detected; use /devices/user/installer.zip"}
        )

    api_base = _normalize_api_host(os.getenv("XTL_API_BASE", "")) or _normalize_api_host("https://api.xautrendlab.com")
    uid = _uid_from(user)
    if not uid:
        raise HTTPException(status_code=401, detail="Invalid auth context")

    tok = secrets.token_urlsafe(32)
    with db() as conn, conn.cursor() as cur:
        _ensure_token_table(cur)
        cur.execute(
            "INSERT INTO user_download_tokens(token, user_id, expires_at, created_at) "
            "VALUES (%s,%s,now()+interval '15 minute', now())",
            (tok, uid),
        )

    embedded = _make_embedded_config(uid, tok, api_base)

    def _iter():
        with open(base_exe, "rb") as f:
            while True:
                chunk = f.read(1024 * 1024)
                if not chunk:
                    break
                yield chunk
        yield embedded

    version = os.getenv("XTL_VERSION", "1.0.0")
    headers = {
        "Content-Disposition": f'attachment; filename="xtl-{version}.exe"',
        "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
        "Pragma": "no-cache",
        "Expires": "0",
    }
    return StreamingResponse(_iter(), media_type="application/octet-stream", headers=headers)

# --- Per-user ZIP (wraps the tokenized EXE) ---
import io, zipfile

import io, zipfile, json, secrets, os
from pathlib import Path
from fastapi.responses import StreamingResponse

@r.get("/user/installer.zip", summary="Per-user ZIP (xtl.exe inside, token embedded)")
def download_user_installer_zip(user = Depends(get_current_user_relaxed)):
    base_exe = os.getenv("XTL_BASE_EXE", os.path.abspath(os.path.join(os.getcwd(), "dist", "xtl", "xtl.exe")))
    if not os.path.exists(base_exe):
        raise HTTPException(status_code=500, detail="Base installer not found on server")

    exe_path = Path(base_exe)
    is_onedir = exe_path.parent.name.lower() == exe_path.stem.lower()
    base_dir = exe_path.parent if is_onedir else None

    api_base = _normalize_api_host(os.getenv("XTL_API_BASE", "https://api.xautrendlab.com"))
    uid = _uid_from(user)
    if not uid:
        raise HTTPException(status_code=401, detail="Invalid auth context")

    # Mint short-lived bind token
    tok = secrets.token_urlsafe(32)
    with db() as conn, conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS user_download_tokens(
                token TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                expires_at TIMESTAMPTZ NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
        """)
        cur.execute(
            "INSERT INTO user_download_tokens(token,user_id,expires_at,created_at) "
            "VALUES (%s,%s,now()+interval '15 minute', now())",
            (tok, uid),
        )
        conn.commit()

    cfg = {"api_base": api_base, "bind_token": tok}
    top = f"xtl_agent_{os.getenv('XTL_VERSION','1.0.0')}"

    # Long, elevated bootstrap (single, canonical launcher)
    CURATED_LAUNCHER_CONTENTS = (
        '@echo off\r\n'
        'setlocal EnableExtensions EnableDelayedExpansion\r\n'
        'cd /d "%~dp0"\r\n'
        '\r\n'
        'set "LOG=%TEMP%\\xtl_install_bootstrap.log"\r\n'
        'echo ==== %date% %time% Run-XTL.bat start ====>>"%LOG%"\r\n'
        'echo cwd=%cd%>>"%LOG%"\r\n'
        '\r\n'
        '>nul 2>&1 net session\r\n'
        'if not "%errorlevel%"=="0" (\r\n'
        '  powershell -NoProfile -ExecutionPolicy Bypass -Command ^\r\n'
        '    "Start-Process -FilePath \'%~f0\' -Verb RunAs"\r\n'
        '  exit /b\r\n'
        ')\r\n'
        '\r\n'
        'if not exist "xtl.exe" if exist "xtl.bin" ren "xtl.bin" "xtl.exe"\r\n'
        'if not exist "xtl.exe" (\r\n'
        '  echo ERROR: xtl.exe/xtl.bin not found beside this script.\r\n'
        '  pause & exit /b 1\r\n'
        ')\r\n'
        '\r\n'
        'powershell -NoProfile -ExecutionPolicy Bypass -Command ^\r\n'
        '  "if (Get-Command Unblock-File -EA SilentlyContinue) { Unblock-File -Path \'%~dp0xtl.exe\' }"\r\n'
        '\r\n'
        '"%~dp0xtl.exe" install || (echo INSTALL FAILED & pause & exit /b 1)\r\n'
        '"%~dp0xtl.exe" start   || (echo START FAILED   & pause & exit /b 1)\r\n'
        '\r\n'
        'echo SUCCESS: XTL installed and started.\r\n'
        'echo Log: C:\\Program Files\\XTL\\dist\\xtl\\xtl_agent.log\r\n'
        'pause\r\n'
    )

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as z:
        if is_onedir:
            for fs_path in base_dir.rglob("*"):
                if fs_path.is_dir():
                    continue
                rel = fs_path.relative_to(base_dir).as_posix()
                name_low = rel.lower()
                # Skip any pre-existing launcher in the build output
                if name_low in ("run-xtl.bat", "run-xtl.cmd"):
                    continue
                if fs_path.name.lower() == exe_path.name.lower():
                    z.write(fs_path, f"{top}/xtl.bin")
                else:
                    z.write(fs_path, f"{top}/{rel}")
        else:
            z.write(exe_path, f"{top}/xtl.bin")

        # include cfg and README
        z.writestr(f"{top}/xtl.cfg", json.dumps(cfg, separators=(",",":")))
        z.writestr(
            f"{top}/README.txt",
            "1) Extract the ZIP\r\n2) Double-click Run-XTL.bat (Run as admin)\r\n",
        )
        # write exactly one curated launcher
        z.writestr(f"{top}/Run-XTL.bat", CURATED_LAUNCHER_CONTENTS)

    data = buf.getvalue()
    headers = {
        "Content-Disposition": f'attachment; filename="{top}.zip"',
        "Content-Type": "application/octet-stream",
        "Content-Length": str(len(data)),
        "Cache-Control": "no-store, no-transform",
        "Pragma": "no-cache",
        "Expires": "0",
        "X-Content-Type-Options": "nosniff",
        "X-Download-Options": "noopen",
    }
    return Response(content=data, media_type="application/octet-stream", headers=headers)

