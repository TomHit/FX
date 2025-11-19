# api/routes_oidc.py
import os
from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import RedirectResponse
from authlib.integrations.starlette_client import OAuth

r = APIRouter(prefix="/auth/oidc/google", tags=["auth"])

GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")
GOOGLE_DISCOVERY = os.getenv("GOOGLE_DISCOVERY", "https://accounts.google.com/.well-known/openid-configuration")
PUBLIC_APP_BASE = os.getenv("PUBLIC_APP_BASE", "https://app.xautrendlab.com")

if not (GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET):
    @r.get("/start")
    def not_configured():
        raise HTTPException(status_code=503, detail="OIDC not configured")
else:
    oauth = OAuth()
    oauth.register(
        name="google",
        server_metadata_url=GOOGLE_DISCOVERY,
        client_id=GOOGLE_CLIENT_ID,
        client_secret=GOOGLE_CLIENT_SECRET,
        client_kwargs={"scope": "openid email profile"},
    )

    @r.get("/start")
    async def google_start(request: Request):
        # Authlib will generate & store PKCE verifier, state, and nonce in request.session
        redirect_uri = request.url_for("google_callback")
        return await oauth.google.authorize_redirect(
            request,
            redirect_uri,
            code_challenge_method="S256",   # PKCE
            prompt="select_account",        # optional: account chooser
        )

    @r.get("/callback", name="google_callback")
    async def google_callback(request: Request):
        # This reads code_verifier/state/nonce back from the same session cookie
        token = await oauth.google.authorize_access_token(request)
        # Prefer ID Token for identity; Authlib will validate signature & claims
        id_token = token.get("userinfo") or await oauth.google.parse_id_token(request, token)

        sub = id_token.get("sub")
        email = id_token.get("email")
        email_verified = bool(id_token.get("email_verified"))

        if not sub:
            raise HTTPException(status_code=400, detail="Invalid ID token")

        # TODO: link/create user -> set your own login session cookie
        # e.g., set_session(response, user_id) or however your app does it.
        # Since we return a redirect, use a response object if needed.
        resp = RedirectResponse(PUBLIC_APP_BASE + "/dashboard.html")
        # example: if you have a helper set_session(resp, user_id)
        # set_session(resp, user_id)
        return resp
