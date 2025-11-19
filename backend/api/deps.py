# api/deps.py
# -*- coding: utf-8 -*-

from fastapi import Depends, HTTPException, Cookie, Header, Request,status
from typing import Optional,Set
import psycopg2, psycopg2.extras, os



ENFORCE_MFA = os.getenv("ENFORCE_MFA", "true").lower() == "true"

DB_DSN = os.environ["DATABASE_URL"]

def _db():
    return psycopg2.connect(DB_DSN)

def _uid(user) -> Optional[str]:
    try:
        v = user.get("id")
        return str(v) if v else None
    except Exception:
        return None

def get_current_user_id_relaxed(request: Request) -> str:
    sess = request.session or {}
    uid = sess.get("user_id")
    if not uid:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authentication required.")
    return str(uid)

def get_current_user_relaxed(user_id: str = Depends(get_current_user_id_relaxed)):
    with _db() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(
            "SELECT id, username, email, role, status FROM users WHERE id=%s LIMIT 1",
            (user_id,)
        )
        row = cur.fetchone()
        if (not row) or (row["status"] != "active"):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Auth required")
        return dict(row)
def get_current_user_id(request: Request) -> str:
    """
    Pull the authenticated user's UUID from the Starlette session cookie
    set by SessionMiddleware in /user/login.
    """
    sess = request.session or {}
    user_id = sess.get("user_id")
    if not user_id:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authentication required.")
    if ENFORCE_MFA and not sess.get("mfa_ok", False):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="MFA required.")
    return str(user_id)

def get_current_user(user_id: str = Depends(get_current_user_id)):
    """
    Load the active user row and return a dict for handlers/routers.
    """
    with _db() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(
            "SELECT id, username, email, role, status FROM users WHERE id=%s LIMIT 1",
            (user_id,)
        )
        row = cur.fetchone()
        if (not row) or (row["status"] != "active"):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Auth required")
        return dict(row)

def require_user(user = Depends(get_current_user)):
    # simple alias so other modules can depend on an authenticated user
    return user

def require_admin(user = Depends(get_current_user)):
    if user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Admin only")
    return user




from typing import Set

def _resolve_perms(user_id: str) -> Set[str]:
    with _db() as conn, conn.cursor() as cur:
        cur.execute("""
            WITH role_cte AS (
                SELECT COALESCE(
                    (SELECT id FROM roles WHERE name = u.role LIMIT 1),
                    (SELECT id FROM roles WHERE name = 'user' LIMIT 1)
                ) AS rid
                FROM users u
                WHERE u.id = %s
            ),
            base AS (
                SELECT
                    p.id AS page_id,
                    COALESCE(upo.can_view,  rpp.can_view)  AS can_view,
                    COALESCE(upo.can_write, rpp.can_write) AS can_write
                FROM pages p
                CROSS JOIN role_cte r
                LEFT JOIN role_page_perms rpp
                  ON rpp.role_id = r.rid AND rpp.page_id = p.id
                LEFT JOIN user_page_overrides upo
                  ON upo.user_id = %s AND upo.page_id = p.id
            )
            SELECT page_id, can_view, can_write
            FROM base
        """, (user_id, user_id))

        perms: Set[str] = set()
        for page_id, can_view, can_write in cur.fetchall():
            if can_view:
                perms.add(f"{page_id}:view")
            if can_write:
                perms.add(f"{page_id}:write")
        return perms


def has_perm(user, perm: str) -> bool:
    uid = _uid(user)
    return bool(uid and (perm in _resolve_perms(uid)))

def require_perm(perm: str):
    # FIX: depend on get_current_user (require_user isn�t defined here)
    def _dep(user = Depends(get_current_user)):
        if not has_perm(user, perm):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="forbidden")
        return user
    return _dep

# --- CSRF protection for state-changing requests (cookie + header double-submit) ---

# deps.py
from fastapi import Request, HTTPException
from urllib.parse import urlparse

SAFE_METHODS = {"GET", "HEAD", "OPTIONS"}

def _same_origin_ok(request: Request) -> bool:
    # Accept if Origin or Referer exactly matches this app's origin
    ref = request.headers.get("Origin") or request.headers.get("Referer")
    if not ref:
        return False
    r = urlparse(ref)
    here = urlparse(str(request.url))
    return (r.scheme, r.netloc) == (here.scheme, here.netloc)

def csrf_protect(request: Request) -> None:
    """
    Same-origin CSRF: for unsafe methods, allow if:
      - X-CSRF-Token matches the 'csrf' cookie (double-submit), OR
      - request is same-origin (Origin/Referer == this host)
    Safe methods pass through.
    """
    method = request.method.upper()
    if method in SAFE_METHODS:
        return

    token_hdr = request.headers.get("X-CSRF-Token") or request.headers.get("x-csrf-token")
    token_cky = request.cookies.get("csrf")

    # Accept double-submit if present
    if token_hdr and token_cky and token_hdr == token_cky:
        return

    # Otherwise require strict same-origin for browser requests
    if _same_origin_ok(request):
        return

    raise HTTPException(status_code=403, detail="CSRF blocked")
