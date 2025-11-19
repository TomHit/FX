import os, secrets,time,json
from typing import Optional, Tuple
from argon2 import PasswordHasher, exceptions as argon_exc
import redis as redis_lib
from itsdangerous import TimestampSigner, BadSignature, SignatureExpired
from uuid import uuid4
from fastapi import HTTPException, Request,Cookie
from types import SimpleNamespace

REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
r = redis_lib.from_url(REDIS_URL, decode_responses=True, health_check_interval=30)

SESSION_TTL = int(os.environ.get("SESSION_TTL", 60*60*24*30))  # 30d
SESSION_SECRET = os.getenv("SESSION_SECRET") or secrets.token_urlsafe(48)

# Args tuned for 1GB VM. You can raise later.
ph = PasswordHasher(time_cost=2, memory_cost=19456, parallelism=1, hash_len=32)
r = redis_lib.from_url(REDIS_URL, decode_responses=True)
signer = TimestampSigner(SESSION_SECRET)

def _sess_key(sid: str) -> str:
    return f"sess:{sid}"

def hash_password(pw: str) -> str:
    return ph.hash(pw)

def set_csrf_for_sid(sid: str) -> str:
    tok = secrets.token_urlsafe(24)
    r.setex(f"csrf:{sid}", int(SESSION_TTL/10), tok)  # e.g., 3 days if TTL is 30d
    return tok

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
    # legacy plaintext path
    if pw == stored:
        return True, ph.hash(pw)
    return False, None

def new_sid() -> str:
    return secrets.token_urlsafe(32)

def set_session(response, user_id: str, mfa_ok: bool = False) -> str:
    """Create session with MFA flag."""
    sid = str(uuid4())
    data = {"uid": user_id, "mfa_ok": bool(mfa_ok), "iat": int(time.time())}
    r.setex(_sess_key(sid), SESSION_TTL, json.dumps(data))
    # your existing cookie attributes are fine
    response.set_cookie("session", sid, domain=".xautrendlab.com",
                        secure=True, httponly=True, samesite="Lax", max_age=SESSION_TTL, path="/")
    return sid

def get_session_data(sid: str | None) -> dict | None:
    if not sid: return None
    raw = r.get(_sess_key(sid))
    if not raw: return None
    try:
        return json.loads(raw)
    except Exception:
        return None

def end_session(sid: str):
    if sid:
        r.delete(_sess_key(sid))

def unsign_cookie(cookie_val: Optional[str]) -> Optional[str]:
    if not cookie_val:
        return None
    try:
        return signer.unsign(cookie_val, max_age=SESSION_TTL*2).decode()
    except (BadSignature, SignatureExpired):
        return None

def mark_mfa_ok(sid: str):
    """Upgrade an existing session to mfa_ok=true."""
    data = get_session_data(sid)
    if not data: return
    data["mfa_ok"] = True
    r.setex(_sess_key(sid), SESSION_TTL, json.dumps(data))

def require_auth_and_mfa(request: Request) -> str:
    sid = request.cookies.get("session")
    data = get_session_data(sid)
    if not data:
        raise HTTPException(status_code=401, detail="Authentication required.")
    if not data.get("mfa_ok"):
        # frontend can redirect when it sees this
        raise HTTPException(status_code=403, detail="MFA enrollment required")
    return data["uid"]

def get_user_id_from_cookie(session_cookie: str | None) -> str | None:
    data = get_session_data(session_cookie)
    return data["uid"] if data and "uid" in data else None

def rate_limit_login(ip: str, username: str, limit: int = 20, seconds: int = 300) -> bool:
    """Return True if allowed; False if blocked."""
    key = f"rl:login:{ip}:{username.lower()}"
    val = r.incr(key)
    if val == 1:
        r.expire(key, seconds)
    return val <= limit

def require_user_mfa(session: str | None = Cookie(default=None)):
    """Strict: requires login AND mfa_ok=True."""
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