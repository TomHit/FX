from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Set, Tuple
from api.routes_devices import R
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from starlette.websockets import WebSocketState

try:
    import redis.asyncio as redis  # redis-py >= 4.2
except Exception:  # pragma: no cover
    redis = None  # type: ignore


router = APIRouter()

# ----------------------------
# Config / wiring
# ----------------------------

REDIS_URL = None  # you should set from your existing settings/env
R = None          # will be a redis client

# If you already have auth helpers, plug them here.
# Keep these functions tiny so this module can move to a standalone gateway later.
async def _auth_user_from_ws(ws: WebSocket) -> Optional[str]:
    """
    Returns uid if authenticated, else None.

    Supports:
      - Cookie-based session (if your existing stack uses it): plug here
      - Bearer token: Authorization header or ?token=
    """
    # 1) token in query
    token = (ws.query_params.get("token") or "").strip()
    if not token:
        # 2) Authorization: Bearer ...
        authz = (ws.headers.get("authorization") or "").strip()
        if authz.lower().startswith("bearer "):
            token = authz.split(" ", 1)[1].strip()

    # If you have a token verifier, call it here and return uid.
    # Example placeholder:
    if token:
        uid = await _verify_access_token(token)
        return uid

    # If you have cookie-session auth, implement here.
    uid2 = await _verify_cookie_session(ws)
    return uid2


async def _verify_access_token(token: str) -> Optional[str]:
    # TODO: integrate with your real token verification.
    # Return user_id string on success, else None.
    return None


async def _verify_cookie_session(ws: WebSocket) -> Optional[str]:
    # TODO: integrate with your real cookie/session verification.
    return None


def _now_ms() -> int:
    return int(time.time() * 1000)


def _json_dumps(obj: Any) -> str:
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False)


# ----------------------------
# Subscription model
# ----------------------------

@dataclass
class ClientSubs:
    uid: str
    device: str = ""
    opp_tfs: Set[str] = field(default_factory=set)          # e.g. {"H1","M15"}
    price_syms: Set[str] = field(default_factory=set)       # e.g. {"XAUUSD","EURUSD"}

    # Redis pubsub tasks
    task_opp: Optional[asyncio.Task] = None
    task_price: Optional[asyncio.Task] = None


# ----------------------------
# Redis helpers
# ----------------------------

async def _redis_get_json(key: str) -> Optional[Any]:
    if R is None:
        return None
    raw = await R.get(key)
    if not raw:
        return None
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8", "ignore")
    try:
        return json.loads(raw)
    except Exception:
        return None


async def _redis_get_float(key: str) -> Optional[float]:
    if R is None:
        return None
    raw = await R.get(key)
    if not raw:
        return None
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8", "ignore")
    try:
        v = float(raw)
        return v if v > 0 else None
    except Exception:
        # allow JSON {"p":..., "ts":...}
        try:
            obj = json.loads(raw)
            p = obj.get("p")
            v = float(p) if p is not None else None
            return v if v and v > 0 else None
        except Exception:
            return None


# ----------------------------
# PubSub fanout loops
# ----------------------------

async def _pubsub_loop(ws: WebSocket, channels: Set[str], handler) -> None:
    """
    Generic pubsub loop. `handler(msg_dict)` is called for each pubsub message payload.
    """
    if R is None:
        return
    pub = R.pubsub()
    try:
        await pub.subscribe(*channels)
        async for m in pub.listen():
            if ws.client_state != WebSocketState.CONNECTED:
                break
            if not isinstance(m, dict):
                continue
            if m.get("type") != "message":
                continue
            data = m.get("data")
            if not data:
                continue
            if isinstance(data, (bytes, bytearray)):
                data = data.decode("utf-8", "ignore")
            try:
                obj = json.loads(data) if isinstance(data, str) else data
            except Exception:
                obj = {"raw": data}
            await handler(obj)
    except Exception:
        # swallow loop exceptions; ws disconnect will stop it
        pass
    finally:
        try:
            await pub.close()
        except Exception:
            pass


async def _start_opp_pubsub(ws: WebSocket, subs: ClientSubs) -> None:
    # One task that listens to all opp tfs channels (small set)
    async def _on_opp_msg(obj: Any) -> None:
        # When opp changes, send a compact event; UI can request snapshot or we can push snapshot.
        try:
            await ws.send_text(_json_dumps({
                "type": "opp_update",
                "uid": subs.uid,
                "tf": obj.get("tf"),
                "ver": obj.get("ver"),
                "ts": obj.get("ts") or _now_ms(),
            }))
        except Exception:
            pass

    channels = set()
    for tf in subs.opp_tfs:
        channels.add(f"xtl:pub:opp:{subs.uid}:{tf}")

    if not channels:
        return

    subs.task_opp = asyncio.create_task(_pubsub_loop(ws, channels, _on_opp_msg))


async def _start_price_pubsub(ws: WebSocket, subs: ClientSubs) -> None:
    # One price channel per device; filter symbols client-side
    async def _on_price_msg(obj: Any) -> None:
        try:
            sym = str(obj.get("sym") or "").upper().strip()
            if subs.price_syms and sym and sym not in subs.price_syms:
                return
            await ws.send_text(_json_dumps({
                "type": "price",
                "sym": sym,
                "p": obj.get("p"),
                "ts": obj.get("ts") or _now_ms(),
                "device": obj.get("device") or subs.device,
            }))
        except Exception:
            pass

    dev = (subs.device or "").strip()
    if not dev:
        return

    channels = {f"xtl:pub:price:{dev}"}
    subs.task_price = asyncio.create_task(_pubsub_loop(ws, channels, _on_price_msg))


async def _stop_tasks(subs: ClientSubs) -> None:
    for t in (subs.task_opp, subs.task_price):
        if t and not t.done():
            t.cancel()
    subs.task_opp = None
    subs.task_price = None


# ----------------------------
# WebSocket endpoint
# ----------------------------

@router.websocket("/ws")
async def ws_gateway(ws: WebSocket):
    await ws.accept()

    uid = await _auth_user_from_ws(ws)
    if not uid:
        await ws.send_text(_json_dumps({"type": "error", "code": "UNAUTH"}))
        await ws.close(code=4401)
        return

    subs = ClientSubs(uid=uid)

    # allow device passed via query for now
    subs.device = (ws.query_params.get("device") or "").strip()

    # welcome
    await ws.send_text(_json_dumps({"type": "welcome", "uid": uid, "ts": _now_ms()}))

    # Simple command loop
    try:
        while True:
            msg = await ws.receive_text()
            try:
                m = json.loads(msg)
            except Exception:
                await ws.send_text(_json_dumps({"type": "error", "code": "BAD_JSON"}))
                continue

            typ = str(m.get("type") or "").lower().strip()

            if typ in ("ping",):
                await ws.send_text(_json_dumps({"type": "pong", "ts": m.get("ts") or _now_ms()}))
                continue

            if typ in ("hello",):
                # update device if provided
                dev = (m.get("device") or "").strip()
                if dev:
                    subs.device = dev
                await ws.send_text(_json_dumps({"type": "hello_ok", "device": subs.device, "ts": _now_ms()}))
                continue

            if typ == "sub":
                stream = str(m.get("stream") or "").lower().strip()

                if stream == "opp":
                    tf = str(m.get("tf") or "").upper().strip() or "H1"
                    subs.opp_tfs.add(tf)

                    # (re)start opp pubsub task
                    await _stop_tasks(subs)
                    await _start_opp_pubsub(ws, subs)
                    await _start_price_pubsub(ws, subs)

                    # optional immediate snapshot push
                    snap = await _redis_get_json(f"xtl:opp:snap:{uid}:{tf}")
                    if snap is not None:
                        await ws.send_text(_json_dumps({"type": "snapshot", "stream": "opp", "tf": tf, "data": snap}))
                    else:
                        await ws.send_text(_json_dumps({"type": "sub_ok", "stream": "opp", "tf": tf}))
                    continue

                if stream == "price":
                    syms = m.get("symbols") or []
                    if isinstance(syms, list):
                        for s in syms:
                            su = str(s or "").upper().strip()
                            if su:
                                subs.price_syms.add(su)

                    await _stop_tasks(subs)
                    await _start_opp_pubsub(ws, subs)
                    await _start_price_pubsub(ws, subs)

                    # optional: send immediate price snapshot for requested symbols
                    if subs.device and subs.price_syms:
                        out = {}
                        for su in list(subs.price_syms)[:50]:
                            p = await _redis_get_float(f"xtl:price:{subs.device}:{su}")
                            if p is not None:
                                out[su] = p
                        await ws.send_text(_json_dumps({"type": "snapshot", "stream": "price", "device": subs.device, "data": out}))
                    else:
                        await ws.send_text(_json_dumps({"type": "sub_ok", "stream": "price"}))
                    continue

                await ws.send_text(_json_dumps({"type": "error", "code": "BAD_STREAM"}))
                continue

            if typ == "unsub":
                stream = str(m.get("stream") or "").lower().strip()

                if stream == "opp":
                    tf = str(m.get("tf") or "").upper().strip()
                    if tf and tf in subs.opp_tfs:
                        subs.opp_tfs.remove(tf)
                    await _stop_tasks(subs)
                    await _start_opp_pubsub(ws, subs)
                    await _start_price_pubsub(ws, subs)
                    await ws.send_text(_json_dumps({"type": "unsub_ok", "stream": "opp", "tf": tf}))
                    continue

                if stream == "price":
                    syms = m.get("symbols") or []
                    if isinstance(syms, list):
                        for s in syms:
                            su = str(s or "").upper().strip()
                            subs.price_syms.discard(su)
                    await _stop_tasks(subs)
                    await _start_opp_pubsub(ws, subs)
                    await _start_price_pubsub(ws, subs)
                    await ws.send_text(_json_dumps({"type": "unsub_ok", "stream": "price"}))
                    continue

                await ws.send_text(_json_dumps({"type": "error", "code": "BAD_STREAM"}))
                continue

            await ws.send_text(_json_dumps({"type": "error", "code": "BAD_TYPE"}))

    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        await _stop_tasks(subs)
        try:
            await ws.close()
        except Exception:
            pass
