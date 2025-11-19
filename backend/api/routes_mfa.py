# /opt/xauapi/api/routes_mfa.py
from fastapi import APIRouter, Depends, HTTPException, Cookie, Response,Request
from pydantic import BaseModel
from datetime import datetime, timedelta, timezone
import psycopg2, psycopg2.extras

from api.security_mfa import verify_totp
from api.routes_auth import db
from api.deps import get_current_user  # your existing auth dep
from .security import mark_mfa_ok

r = APIRouter(prefix="/user/mfa", tags=["mfa"])

class ConfirmIn(BaseModel):
    code: str


@r.get("/status")
def mfa_status(user=Depends(get_current_user)):
    with db() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(
            "SELECT COALESCE(mfa_enabled,false) AS enabled, (mfa_secret IS NOT NULL) AS has_secret "
            "FROM users WHERE id=%s",
            (user["id"],),
        )
        row = cur.fetchone() or {"enabled": False, "has_secret": False}
    return {"enabled": bool(row["enabled"] and row["has_secret"])}

@r.post("/setup/confirm", tags=["auth"])
def mfa_setup_confirm(
    body: ConfirmIn,
    request: Request,
    response: Response,
    session: str | None = Cookie(default=None),
    sid: str | None = Cookie(default=None),
    user = Depends(get_current_user),
):
    with db() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        # lock row to avoid races
        cur.execute(
            "SELECT id, mfa_temp_secret, mfa_temp_set_at, mfa_secret, mfa_enabled "
            "FROM users WHERE id=%s FOR UPDATE",
            (user["id"],),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=400, detail="No pending MFA enrollment")

        uid           = row["id"]
        temp_secret   = row["mfa_temp_secret"]
        temp_set_at   = row["mfa_temp_set_at"]
        stored_secret = row["mfa_secret"]
        enabled       = bool(row["mfa_enabled"])

        # already enabled -> OK
        if enabled and stored_secret:
            return {"ok": True, "enabled": True}

        # pick secret to check
        secret_for_check = None
        if temp_secret:
            if temp_set_at and (datetime.now(timezone.utc) - temp_set_at) > timedelta(minutes=15):
                cur.execute("UPDATE users SET mfa_temp_secret=NULL, mfa_temp_set_at=NULL WHERE id=%s", (uid,))
                conn.commit()
                raise HTTPException(status_code=400, detail="MFA setup expired, start again")
            secret_for_check = temp_secret
        elif stored_secret and not enabled:
            # legacy/partial state
            secret_for_check = stored_secret

        if not secret_for_check:
            raise HTTPException(status_code=400, detail="No pending MFA enrollment")

        # verify code
        if not verify_totp(secret_for_check, body.code):
            raise HTTPException(status_code=401, detail="Invalid code")

        # persist: promote temp -> permanent if needed, and enable
        final_secret = stored_secret or temp_secret
        cur.execute(
            """
            UPDATE users
               SET mfa_enabled = TRUE,
                   mfa_secret  = %s,
                   mfa_temp_secret = NULL,
                   mfa_temp_set_at = NULL
             WHERE id = %s
            """,
            (final_secret, uid),
        )
        if cur.rowcount != 1:
            conn.rollback()
            raise HTTPException(status_code=500, detail="Failed to enable MFA")

        conn.commit()

    # best-effort: mark current session MFA-OK
    for token in filter(None, [session, sid, request.cookies.get("session"), request.cookies.get("sid"), request.cookies.get("sessionid")]):
        try:
            mark_mfa_ok(token)
            break
        except Exception:
            pass

    return {"ok": True, "enabled": True}

@r.post("/disable")
def mfa_disable(user=Depends(get_current_user)):
    # keep simple (admin UI can protect this later with password re-check)
    with db() as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE users SET mfa_enabled=FALSE, mfa_secret=NULL WHERE id=%s",
            (user["id"],),
        )
        conn.commit()
    return {"ok": True}
