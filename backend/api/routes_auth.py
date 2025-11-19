import os
from typing import Optional
from datetime import datetime, timezone
from app.utils.qr import qr_svg,qr_png_b64
import psycopg2, psycopg2.extras
import pyotp
from fastapi import APIRouter, HTTPException, Response, Request, Cookie,Depends
from pydantic import BaseModel
from api.deps import get_current_user, _uid, _resolve_perms
import logging
log = logging.getLogger("uvicorn.error")

# session / security helpers you already have
from .security import (
    verify_and_upgrade, hash_password, set_session, end_session,
    rate_limit_login, set_csrf_for_sid, get_session_data
)

DB_DSN = os.environ["DATABASE_URL"]  # via pgbouncer

r = APIRouter()


# ---------- DB ----------
def db():
    return psycopg2.connect(DB_DSN)


# ---------- Models ----------
class LoginIn(BaseModel):
    username_or_email: str
    password: str
    totp: Optional[str] = None   # 6-digit code when MFA enabled


class SignupIn(BaseModel):
    username: str
    email: str
    password: str


class CodeIn(BaseModel):
    code: str  # 6-digit TOTP


# ---------- Helpers ----------
def _compute_mfa_state(mfa_secret: Optional[str], mfa_enabled: Optional[bool]) -> str:
    if not mfa_secret:
        return "disabled"
    return "enabled" if bool(mfa_enabled) else "pending"


def _get_user_by_id(conn, user_id: str):
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(
            """
            select id, username, email, role, status, mfa_enabled, mfa_secret
            from users
            where id=%s
            """,
            (user_id,),
        )
        return cur.fetchone()


# ---------- /user/* (login/signup/me/logout/options) ----------

@r.get("/auth/debug_session", tags=["auth"])
def debug_session(request: Request):
    sess = getattr(request, "session", None) or getattr(request.state, "session", {}) or {}
    return {
        "cookies_seen": list(request.cookies.keys()),
        "has_session_cookie": bool(request.cookies.get("session")),
        "session_keys": list(sess.keys()),
    }

@r.post("/user/login", tags=["auth"])
def user_login(inp: LoginIn, request: Request, response: Response):
    # --- rate limit per IP+username (20 tries / 5 min) ---
    ip = request.headers.get("x-forwarded-for") or request.headers.get("x-real-ip")
    if not ip:
        ip = request.client.host if request.client else "0.0.0.0"
    ip = ip.split(",")[0].strip()
    if not rate_limit_login(ip, inp.username_or_email):
        raise HTTPException(status_code=429, detail="Too many attempts, try again later.")

    q = inp.username_or_email.strip().lower()
    with db() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(
            """
            select id, username, email, password_hash, status, role, mfa_enabled, mfa_secret
            from users
            where lower(username)=%s or lower(email)=%s
            limit 1
            """,
            (q, q),
        )
        row = cur.fetchone()
        if not row or row["status"] != "active":
            raise HTTPException(status_code=401, detail="Invalid credentials")

        ok, new_hash = verify_and_upgrade(inp.password, row["password_hash"])
        if not ok:
            raise HTTPException(status_code=401, detail="Invalid credentials")

        # --- MFA gate ---
        if row.get("mfa_enabled"):
            totp_code = (getattr(inp, "totp", None) or request.headers.get("x-totp") or "").strip()
            if not totp_code:
                raise HTTPException(status_code=401, detail="TOTP required")
            if not pyotp.TOTP(row["mfa_secret"]).verify(totp_code, valid_window=1):
                raise HTTPException(status_code=401, detail="Invalid TOTP")
            mfa_ok = True
        else:
            mfa_ok = False  # limited session; redirect to setup

        if new_hash:
            cur.execute("update users set password_hash=%s where id=%s", (new_hash, row["id"]))
        cur.execute("update users set last_login_at=now() where id=%s", (row["id"],))

    # --- Starlette session (SessionMiddleware will set the signed "session" cookie) ---
    request.session.clear()
    request.session["user_id"]  = str(row["id"])
    request.session["username"] = row["username"]
    request.session["email"]    = row["email"]
    request.session["role"]     = row["role"]
    request.session["mfa_ok"]   = bool(mfa_ok)
    log.info(f"[LOGIN] session set keys={list(request.session.keys())} for user={row['username']}")

    # (Optional) if you truly need a separate CSRF for form posts, you can set it here;
    # not required for your axios JSON calls.
    # response.set_cookie("csrf", generate_csrf(), domain=".xautrendlab.com",
    #                     secure=True, samesite="None", httponly=False, path="/", max_age=3*24*3600)

    return {
        "ok": True,
        "redirect": "https://app.xautrendlab.com/" if mfa_ok else "https://app.xautrendlab.com/mfa-setup.html",
    }


@r.post("/user/signup", tags=["auth"])
def user_signup(inp: SignupIn):
    if len(inp.username.strip()) < 3 or "@" not in inp.email:
        raise HTTPException(status_code=400, detail="Invalid username/email")
    pw_hash = hash_password(inp.password)
    with db() as conn, conn.cursor() as cur:
        cur.execute(
            """
            insert into users (username,email,password_hash,role,status)
            values (%s,%s,%s,'user','active')
            on conflict (username) do nothing
            """,
            (inp.username.strip(), inp.email.strip().lower(), pw_hash),
        )
        if cur.rowcount == 0:
            raise HTTPException(status_code=409, detail="Username already exists")
    return {"ok": True}


@r.get("/user/me", tags=["auth"])
def user_me(request: Request):
    sess = getattr(request, "session", {}) or {}
    user_id = sess.get("user_id")
    if not user_id:
        raise HTTPException(status_code=401, detail="Authentication required.")

    with db() as conn:
        row = _get_user_by_id(conn, str(user_id))
        if not row:
            raise HTTPException(status_code=401, detail="Authentication required.")
        return {
            "id": str(row["id"]),
            "username": row["username"],
            "email": row["email"],
            "role": row.get("role") or "user",
            "mfa_enabled": bool(row.get("mfa_enabled")),
            "mfa_ok": bool(sess.get("mfa_ok")),
        }


@r.post("/user/logout", tags=["auth"])
def user_logout(request: Request, response: Response):
    try:
        request.session.clear()
    except Exception:
        pass
    # You may still delete by name to force a clear on the client:
    response.delete_cookie("session", domain=".xautrendlab.com", path="/")
    response.delete_cookie("csrf",    domain=".xautrendlab.com", path="/")
    return {"ok": True}

@r.get("/auth/options", tags=["auth"])
def auth_options():
    google_on = bool(os.getenv("GOOGLE_CLIENT_ID") and os.getenv("GOOGLE_CLIENT_SECRET"))
    return {"local": True, "google": google_on, "microsoft": False}


# ---------- /auth/* (MFA API used by mfa-setup page) ----------
@r.get("/auth/me", tags=["auth"])
def auth_me(response: Response, request: Request):
    sess = getattr(request, "session", {}) or {}
    user_id = sess.get("user_id")
    if not user_id:
        raise HTTPException(status_code=401, detail="Authentication required.")
    user_id = str(user_id)

    # Load user row
    with db() as conn:
        row = _get_user_by_id(conn, user_id)
        if not row:
            raise HTTPException(status_code=401, detail="Authentication required.")

    state = _compute_mfa_state(row.get("mfa_secret"), row.get("mfa_enabled"))

    try:
        perms = sorted(list(_resolve_perms(user_id)))
    except Exception:
        perms = []

    response.headers["Cache-Control"] = "no-store"
    role = (row["role"] or "").lower()
    if not perms:
        perms = {"devices:view", "devices:write"} if role in ("admin","owner") else {"devices:view"}

    return {
        "id": user_id,
        "username": row.get("username"),
        "email": row.get("email"),
        "role": row.get("role") or "user",
        "status": row.get("status"),
        "mfa_state": state,
        "mfa_enabled": (state == "enabled"),
        "permissions": perms,
    }


@r.post("/auth/mfa/totp/begin", tags=["auth"])
def begin_totp(response: Response, session: str | None = Cookie(default=None)):
    data = get_session_data(session)
    if not data:
        raise HTTPException(status_code=401, detail="Authentication required.")
    user_id = data["uid"]

    # fetch state; refuse to begin if already enabled or has a permanent secret
    with db() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(
            "SELECT email, COALESCE(mfa_enabled,false) AS enabled, (mfa_secret IS NOT NULL) AS has_secret "
            "FROM users WHERE id=%s",
            (user_id,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=401, detail="Authentication required.")
        email = row["email"] or "user@example.com"

        if row["enabled"]:
            raise HTTPException(status_code=400, detail="MFA already enabled")

        # issue a TEMPORARY enrollment secret (do NOT touch mfa_secret/mfa_enabled)
        secret = pyotp.random_base32()
        cur.execute(
            "UPDATE users SET mfa_temp_secret=%s, mfa_temp_set_at=NOW() WHERE id=%s",
            (secret, user_id),
        )
        conn.commit()

    issuer = "XauTrendLab"
    label = f"{issuer}:{email}"
    otpauth_uri = pyotp.TOTP(secret).provisioning_uri(name=label, issuer_name=issuer)

    # generate QR (prefer PNG; include SVG as fallback)
    svg = qr_svg(otpauth_uri, scale=8, border=1)
    png_b64 = qr_png_b64(otpauth_uri, scale=6, border=1)

    response.headers["Cache-Control"] = "no-store"
    return {
        "secret": secret,
        "otpauth": otpauth_uri,
        "otpauth_uri": otpauth_uri,
        "qr_png_b64": png_b64,
        "qr_png": png_b64,
        "qr_svg": svg,
    }


@r.post("/auth/mfa/totp/verify", tags=["auth"])
def verify_totp_api(body: CodeIn, response: Response, session: str | None = Cookie(default=None)):
    data = get_session_data(session)
    if not data:
        raise HTTPException(status_code=401, detail="Authentication required.")
    user_id = data["uid"]

    # read temp secret, verify, then promote to permanent + enable
    with db() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(
            "SELECT mfa_temp_secret, mfa_temp_set_at FROM users WHERE id=%s",
            (user_id,),
        )
        row = cur.fetchone()
        if not row or not row["mfa_temp_secret"]:
            raise HTTPException(status_code=400, detail="No pending setup")

        # (optional) expiry window, e.g., 15 minutes
        if row["mfa_temp_set_at"] and (datetime.now(timezone.utc) - row["mfa_temp_set_at"]) \
                .total_seconds() > 15 * 60:
            cur.execute(
                "UPDATE users SET mfa_temp_secret=NULL, mfa_temp_set_at=NULL WHERE id=%s",
                (user_id,),
            )
            conn.commit()
            raise HTTPException(status_code=400, detail="MFA setup expired, start again")

        # verify 6-digit code (allow small clock drift)
        if not pyotp.TOTP(row["mfa_temp_secret"]).verify(body.code, valid_window=1):
            raise HTTPException(status_code=400, detail="Invalid code")

        # promote temp -> permanent and enable
        cur.execute(
            """
            UPDATE users
               SET mfa_secret      = convert_to(mfa_temp_secret, 'UTF8'),
                   mfa_enabled     = TRUE,
                   mfa_temp_secret = NULL,
                   mfa_temp_set_at = NULL
             WHERE id = %s
            """,
            (user_id,),
        )
        conn.commit()

    # ? make the current session MFA-complete
    try:
        end_session(session)  # rotate the SID
    except Exception:
        pass

    new_sid = set_session(response, str(user_id), mfa_ok=True)

    # (optional) refresh CSRF cookie to match the new session
    csrf = set_csrf_for_sid(new_sid)
    response.set_cookie(
        key="csrf",
        value=csrf,
        domain=".xautrendlab.com",
        secure=True,
        samesite="Lax",
        max_age=60 * 60 * 24 * 3,
        path="/",
    )

    response.headers["Cache-Control"] = "no-store"
    return {"ok": True, "enabled": True, "redirect": "https://app.xautrendlab.com/react/dashboard"}

@r.post("/auth/mfa/totp/disable", tags=["auth"])
def disable_totp(response: Response, session: str | None = Cookie(default=None)):
    data = get_session_data(session)
    if not data:
        raise HTTPException(status_code=401, detail="Authentication required.")
    user_id = data["uid"]

    with db() as conn, conn.cursor() as cur:
        cur.execute("update users set mfa_secret=NULL, mfa_enabled=false where id=%s", (user_id,))
        conn.commit()

    response.headers["Cache-Control"] = "no-store"
    return {"ok": True}
router = r
