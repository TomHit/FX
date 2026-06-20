# ws_prices.py
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from typing import Optional

import redis.asyncio as aioredis
from fastapi import APIRouter, WebSocket, WebSocketDisconnect

log = logging.getLogger("uvicorn.error")

REDIS_URL = os.getenv("REDIS_URL") or "redis://default:xau12345@10.0.0.132:6379/0"
R = aioredis.from_url(REDIS_URL, decode_responses=True)

router = APIRouter()


def _uid_from_session(ws: WebSocket) -> Optional[str]:
    sess = ws.scope.get("session") or {}
    for k in ("uid", "user_id", "sub"):
        v = sess.get(k)
        if v:
            return str(v)
    u = sess.get("user")
    if isinstance(u, dict):
        for k in ("id", "uid", "user_id"):
            v = u.get(k)
            if v:
                return str(v)
    return None


async def _load_snapshot(device: Optional[str], symbols: list[str]) -> dict:
    out: dict[str, float] = {}
    for s in symbols:
        keys = []
        if device:
            keys.append(f"xtl:price:{device}:{s}")
        keys.append(f"xtl:price:{s}")
        for k in keys:
            try:
                raw = await R.get(k)
                if not raw:
                    continue
                try:
                    obj = json.loads(raw)
                    px = obj.get("price") if isinstance(obj, dict) else None
                except Exception:
                    px = None
                if px is None:
                    continue
                out[s] = float(px)
                break
            except Exception:
                continue
    return out

async def _ws_prices_impl(ws: WebSocket) -> None:
    await ws.accept()

    qp = ws.query_params
    symbols = (qp.get("symbols") or "XAUUSD").strip()
    device = (qp.get("device") or "").strip() or None
    uid = (qp.get("uid") or "").strip() or None

    uid0 = uid or _uid_from_session(ws)
    if not uid0:
        try:
            await ws.send_json({"type": "err", "err": "missing_uid"})
        except Exception:
            pass
        await ws.close(code=4401)
        return

    sym_set = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    if not sym_set:
        sym_set = ["XAUUSD"]
    sym_allow = set(sym_set)

    log.info("WS prices connected uid=%s symbols=%s device=%s", uid0, sym_set, device)

    # send snapshot immediately
    try:
        snap = await _load_snapshot(device, sym_set)
        await ws.send_json({"type": "snapshot", "uid": uid0, "ts_ms": int(time.time() * 1000), "prices": snap})
    except Exception:
        pass

    ch = f"xtl:pub:price:{uid0}"
    pub = R.pubsub(ignore_subscribe_messages=True)

    try:
        await pub.subscribe(ch)
    except Exception as e:
        log.exception("WS prices subscribe failed uid=%s ch=%s err=%s", uid0, ch, e)
        try:
            await ws.send_json({"type": "err", "err": "redis_subscribe_failed"})
        except Exception:
            pass
        await ws.close(code=1011)
        return

    last_ping = time.time()

    try:
        while True:
            # send heartbeat every 15s so the connection stays “alive”
            now = time.time()
            if now - last_ping >= 15:
                last_ping = now
                try:
                    await ws.send_json({"type": "ping", "ts_ms": int(now * 1000)})
                except Exception:
                    pass

            msg = None
            try:
                msg = await pub.get_message(timeout=1.0)
            except Exception:
                msg = None

            if msg and msg.get("type") == "message":
                try:
                    data = json.loads(msg.get("data") or "{}")
                except Exception:
                    data = None

                if isinstance(data, dict):
                    s = str(data.get("symbol") or "").upper()
                    if s in sym_allow or "ALL" in sym_allow:
                       try:
                           await ws.send_json(data)
                       except (WebSocketDisconnect, RuntimeError):
                           # client closed; stop loop
                           break
                       except Exception:
                           # any other send error -> stop loop (avoid spam logs)
                           break


            await asyncio.sleep(0.05)

    except WebSocketDisconnect:
        pass
    except Exception as e:
        log.exception("WS prices loop error uid=%s err=%s", uid0, e)
    finally:
        try:
            await pub.unsubscribe(ch)
        except Exception:
            pass
        try:
            await pub.close()
        except Exception:
            pass
        try:
            await ws.close()
        except Exception:
            pass


@router.websocket("/ws/prices")
async def ws_prices(ws: WebSocket):
    await _ws_prices_impl(ws)


# ✅ Alias to match your browser calling "prices?..."
@router.websocket("/prices")
async def ws_prices_alias(ws: WebSocket):
    await _ws_prices_impl(ws)
