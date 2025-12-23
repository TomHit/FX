import os,base64, hmac, hashlib, json, time
from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse, JSONResponse
from fastapi import Cookie

router = APIRouter()

def _b64url_decode(s: str) -> bytes:
    s = s.replace("-", "+").replace("_", "/")
    pad = "=" * ((4 - (len(s) % 4)) % 4)
    return base64.b64decode(s + pad)

def _verify_preview_token(token: str, secret: str) -> dict | None:
    try:
        payload_b64, sig = token.split(".", 1)
    except Exception:
        return None

    mac = hmac.new(secret.encode("utf-8"), payload_b64.encode("utf-8"), hashlib.sha256).digest()
    sig2 = base64.urlsafe_b64encode(mac).decode("utf-8").rstrip("=")

    if not hmac.compare_digest(sig, sig2):
        return None

    payload = json.loads(_b64url_decode(payload_b64).decode("utf-8"))
    now = int(time.time())

    exp = int(payload.get("exp") or 0)
    if exp <= 0 or now >= exp:
        return None

    # Must be preview/paid token with scope
    scope = payload.get("scope") or []
    if not isinstance(scope, list):
        scope = []

    return payload

@router.get("/preview/consume")
def preview_consume(request: Request, token: str):
    # Read from ENV (matches your current app style)
    secret = (os.getenv("PREVIEW_TOKEN_SECRET") or "").strip()
    if not secret:
        return JSONResponse({"ok": False, "error": "preview_secret_missing"}, status_code=500)

    payload = _verify_preview_token(token, secret)
    if not payload:
        # send user back to marketing pricing page
        return RedirectResponse("https://xautrendlab.com/pricing?err=invalid_or_expired", status_code=302)

    # IMPORTANT: redirect to APP domain, not API domain
    resp = RedirectResponse(
        url="https://app.xautrendlab.com/react/dashboard?mode=preview",
        status_code=302,
    )

    # IMPORTANT: cookie must be visible to app.xautrendlab.com too
    resp.set_cookie(
        key="xtl_preview",
        value=token,
        httponly=True,
        secure=True,
        samesite="none",          # cross-subdomain cookie
        domain=".xautrendlab.com",# share across api/app/marketing
        max_age=max(60, int(payload["exp"]) - int(time.time())),
        path="/",
    )
    return resp
@router.get("/preview/whoami")
def preview_whoami(request: Request, xtl_preview: str | None = Cookie(default=None)):
    secret = (os.getenv("PREVIEW_TOKEN_SECRET") or "").strip()
    if not secret:
        return JSONResponse({"ok": False, "mode": "none"}, status_code=200)

    if not xtl_preview:
        return JSONResponse({"ok": False, "mode": "none"}, status_code=200)

    payload = _verify_preview_token(xtl_preview, secret)
    if not payload:
        return JSONResponse({"ok": False, "mode": "none"}, status_code=200)

    return JSONResponse({
        "ok": True,
        "mode": "preview",
        "sub": payload.get("sub"),
        "plan": payload.get("plan"),
        "scope": payload.get("scope", []),
        "exp": payload.get("exp"),
    })

@router.get("/preview/me")
async def preview_me(request: Request):
    token = request.cookies.get("xtl_preview")
    if not token:
        return JSONResponse({"ok": True, "preview": False})

    secret = (os.getenv("PREVIEW_TOKEN_SECRET") or "").strip()

    # If you store it via env, simplest:
    # secret = os.environ.get("PREVIEW_TOKEN_SECRET")

    if not secret:
        return JSONResponse({"ok": False, "error": "missing_secret"}, status_code=500)

    payload = _verify_preview_token(token, secret)
    if not payload:
        return JSONResponse({"ok": True, "preview": False})

    return JSONResponse({"ok": True, "preview": True, "payload": payload})

