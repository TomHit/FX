import os, secrets, time, json, uuid
from typing import Optional, Tuple
from argon2 import PasswordHasher, exceptions as argon_exc
import redis as redis_lib
from itsdangerous import TimestampSigner, BadSignature, SignatureExpired
from uuid import uuid4
from fastapi import HTTPException, Request, Cookie, Response
from types import SimpleNamespace

# -------------------------
# Redis / Session settings
# -------------------------
REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
r = redis_lib.from_url(REDIS_URL, decode_responses=True, health_check_interval=30)

SESSION_TTL = int(os.environ.get("SESSION_TTL", 60 * 60 * 24 * 30))  # 30 days
SESSION_SECRET = os.getenv("SESSION_SECRET") or secrets.token_urlsafe(48)

SESSION_COOKIE = "session"

# Password hasher (1GB VM safe defaults)
ph = PasswordHasher(time_cost=2, memory_cost=19456, parallelism=1, hash_len=32)

signer = TimestampSigner(SESSION_SECRET)

# -------------------------
# Helpers
# -------------------------
def _sess_key(sid: str) -> str:
    return f"sess:{sid}"

def _is_uuid(val: str | None) -> bool:
    try:
        uuid.UUID(val)
        return True
    except Exception:
        return False

def _delete_all_session_cookies(response: Response):
    # Kill legacy + duplicate cookies across scopes
    response.delete_cookie(SESSION_COOKIE, domain=".xautrendlab.com", path="/")
    response.delete_cookie(SESSION_COOKIE, domain="app.xautrendlab.com", path="/")
    response.delete_cookie(SESSION_COOKIE, domain="api.xautrendlab.com", path="/")

# -------------------------
# Password utilities
# -------------------------
def hash_password(pw: str) -> str:
    return ph.hash(pw)

def verify_and_upgrade(pw: str, stored: str) -> Tuple[bool, Optional[str]]:
    """Return (ok, new_hash_if_upgrade_needed). Supports legacy plaintext."""
    if stored.startswith("$argon2"):
        try:
            ok = ph.verify(stored, pw)
            if ok and ph.check_needs_rehash(stored):
                return True, ph.hash(pw)
            return ok, None
        except argon_exc.VerifyMismatchError:
            return False, None

    # legacy plaintext
    if pw == stored:
        return True, ph.hash(pw)

    return False, None

# -------------------------
# Session management
# -------------------------
def new_sid() -> str:
    return str(uuid4())

def set_session(response: Response, user_id: str, mfa_ok: bool = False) -> str:
    """
    Create a Redis-backed session and set cookie.
    Also hard-cleans any legacy cookies.
    """
    sid = new_sid()
    data = {"uid": user_id, "mfa_ok": bool(mfa_ok), "iat": int(time.time())}

    r.setex(_sess_key(sid), SESSION_TTL, json.dumps(data))

    # ?? IMPORTANT: remove any legacy/duplicate cookies first
    _delete_all_session_cookies(response)

    # Set canonical session cookie
    response.set_cookie(
        key=SESSION_COOKIE,
        value=sid,
        domain=".xautrendlab.com",
        path="/",
        secure=True,
        httponly=True,
        samesite="None",   # REQUIRED for app ? api fetch
        max_age=SESSION_TTL,
    )

    return sid

def get_session_data(sid: str | None) -> dict | None:
    if not sid or not _is_uuid(sid):
        return None
    raw = r.get(_sess_key(sid))
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None

def end_session(sid: str | None):
    if sid and _is_uuid(sid):
        r.delete(_sess_key(sid))

# -------------------------
# CSRF (unchanged)
# -------------------------
def set_csrf_for_sid(sid: str) -> str:
    tok = secrets.token_urlsafe(24)
    r.setex(f"csrf:{sid}", int(SESSION_TTL / 10), tok)
    return tok

def unsign_cookie(cookie_val: Optional[str]) -> Optional[str]:
    if not cookie_val:
        return None
    try:
        return signer.unsign(cookie_val, max_age=SESSION_TTL * 2).decode()
    except (BadSignature, SignatureExpired):
        return None

def mark_mfa_ok(sid: str):
    data = get_session_data(sid)
    if not data:
        return
    data["mfa_ok"] = True
    r.setex(_sess_key(sid), SESSION_TTL, json.dumps(data))

# -------------------------
# Auth dependencies
# -------------------------
def require_auth_and_mfa(request: Request) -> str:
    sid = request.cookies.get(SESSION_COOKIE)
    data = get_session_data(sid)
    if not data:
        raise HTTPException(status_code=401, detail="Authentication required.")
    if not data.get("mfa_ok"):
        raise HTTPException(status_code=403, detail="MFA enrollment required")
    return data["uid"]

def get_user_id_from_cookie(session_cookie: str | None) -> str | None:
    data = get_session_data(session_cookie)
    return data["uid"] if data and "uid" in data else None

def require_user_mfa(session: str | None = Cookie(default=None)):
    """Strict: login + MFA required."""
    data = get_session_data(session)
    if not data:
        raise HTTPException(status_code=401, detail="Authentication required.")
    if not data.get("mfa_ok", False):
        raise HTTPException(status_code=401, detail="MFA enrollment required")
    return SimpleNamespace(id=data["uid"], mfa_ok=True)

def require_user_relaxed(session: str | None = Cookie(default=None)):
    """Login required, MFA NOT required."""
    data = get_session_data(session)
    if not data:
        raise HTTPException(status_code=401, detail="Authentication required.")
    return SimpleNamespace(id=data["uid"], mfa_ok=bool(data.get("mfa_ok")))

# -------------------------
# Rate limiting (unchanged)
# -------------------------
def rate_limit_login(ip: str, username: str, limit: int = 20, seconds: int = 300) -> bool:
    key = f"rl:login:{ip}:{username.lower()}"
    val = r.incr(key)
    if val == 1:
        r.expire(key, seconds)
    return val <= limit
