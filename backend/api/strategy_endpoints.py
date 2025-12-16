# -*- coding: utf-8 -*-
from __future__ import annotations

"""
strategy_endpoints.py

Keeps the "Strategy / My Bots" API separate from trend_endpoints.py (which is already bulky).

This file intentionally mirrors the BOT STATE storage model already used in trend_endpoints.py:
- Redis key:  xtl:bot:state:{user_id|anon}
- Payload: {"enabled": bool, "strategy_type": str, "config": dict, "updated_ms": int}

So the UI can be moved to hit /strategy/* without breaking existing stored state.
"""

from typing import Any, Dict, Optional, Literal
import os
import json
import time
import logging

import redis
from fastapi import APIRouter, Depends, Request, HTTPException
from pydantic import BaseModel, Field

log = logging.getLogger("xtl.strategy")

router = APIRouter(prefix="/strategy", tags=["strategy"])

# --------------------------
# Redis
# --------------------------
REDIS_URL = os.getenv("REDIS_URL", "redis://default:xau12345@10.0.0.132:6379/0")
R = redis.from_url(REDIS_URL, decode_responses=True)

# --------------------------
# Optional auth (session/relaxed) shim
# --------------------------
from types import SimpleNamespace

try:
    # preferred
    from api.routes_devices import _session_user, _uid as _uid_hard, _uid_from as _uid_soft  # type: ignore
except Exception:
    try:
        from routes_devices import _session_user, _uid as _uid_hard, _uid_from as _uid_soft  # type: ignore
    except Exception:
        def _session_user(_req):  # type: ignore
            return None

        def _uid_hard(u):  # type: ignore
            if isinstance(u, dict):
                return u.get("id") or u.get("user_id") or u.get("sub")
            for k in ("id", "user_id", "uid", "sub"):
                v = getattr(u, k, None)
                if v:
                    return v
            return None

        def _uid_soft(_u):  # type: ignore
            return None

try:
    from api.deps import get_current_user_relaxed  # type: ignore
except Exception:
    try:
        from deps import get_current_user_relaxed  # type: ignore
    except Exception:
        get_current_user_relaxed = None  # type: ignore


def require_auth_optional(request: Request):
    """
    Best-effort user resolver for UI endpoints:
      1) session user (routes_devices._session_user)
      2) relaxed user (api.deps.get_current_user_relaxed), if present
      3) fallback: anonymous {user_id: None}
    Always returns an object with .user_id (string or None).
    """
    # 1) session
    try:
        u = _session_user(request)
        if u:
            uid = _uid_hard(u) or _uid_soft(u)
            return SimpleNamespace(user_id=(str(uid) if uid else None))
    except Exception:
        pass

    # 2) relaxed
    if get_current_user_relaxed:
        try:
            u2 = get_current_user_relaxed(request)  # may return dict/object
            if u2:
                uid = _uid_hard(u2) or _uid_soft(u2)
                return SimpleNamespace(user_id=(str(uid) if uid else None))
        except Exception:
            pass

    # 3) anonymous
    return SimpleNamespace(user_id=None)


# --------------------------
# Bot state (per-user)
# --------------------------
BOT_STATE_PREFIX = "xtl:bot:state:"  # key = xtl:bot:state:{user_id}


def _bot_state_key(user_id: str | None) -> str:
    uid = (user_id or "").strip() or "anon"
    return f"{BOT_STATE_PREFIX}{uid}"


def _default_bot_state() -> dict[str, Any]:
    now_ms = int(time.time() * 1000)
    return {
        "enabled": False,
        "strategy_type": "opportunity",  # "indicator" | "priceAction" | "opportunity"
        "config": {"execution": {"mode": "paper","require_live_ack": true},"risk": {"qty": 1,"max_positions": 1,"risk_mode": "qty","risk_pct": 1},"entry": {"side_mode": "follow","entry_type": "market","limit_price": null,"confirm_pullback": true,"pullback": {"zone": "vwap","max_retrace_pct": 0.8,"reversal": "close_reclaim"}},"exits": {"sl": {"mode": "pips","value": 120,"atr_mult": 1.2},"targets": {"mode": "single","list": [{"id": "tp1","kind": "r","value": 1.5,"qty_pct": 100,"runner": false}]},"trailing": {"enabled": true,"kind": "step","step_pips": 80,"step_lock_pips": 40,"atr_mult": 1.0,"activate_after_r": 1.0},"breakeven": {"enabled": true,"at_r": 1.0,"buffer_pips": 10}},"guards": {"stale_bar_sec": 180,"disable_weekends": true,"only_if_recent_bar": true}},
        "updated_ms": now_ms,
    }


def _load_bot_state(user_id: str | None) -> dict[str, Any]:
    key = _bot_state_key(user_id)
    try:
        raw = R.get(key)
    except Exception as e:
        log.warning("[BOT] load state failed key=%s err=%r", key, e)
        return _default_bot_state()

    if not raw:
        return _default_bot_state()

    try:
        data = json.loads(raw)
        if not isinstance(data, dict):
            return _default_bot_state()
    except Exception as e:
        log.warning("[BOT] json decode failed key=%s err=%r", key, e)
        return _default_bot_state()

    base = _default_bot_state()
    base.update({k: v for k, v in data.items() if v is not None})
    return base


def _save_bot_state(user_id: str | None, state: dict[str, Any]) -> None:
    key = _bot_state_key(user_id)
    payload = dict(state)
    payload["updated_ms"] = int(time.time() * 1000)
    try:
        R.set(key, json.dumps(payload))
    except Exception as e:
        log.warning("[BOT] save state failed key=%s err=%r", key, e)


# --------------------------
# API Models
# --------------------------
StrategyType = Literal["indicator", "priceAction", "opportunity"]


class BotState(BaseModel):
    enabled: bool = False
    strategy_type: StrategyType = "opportunity"
    config: Dict[str, Any] = Field(default_factory=dict)
    updated_ms: int = 0


class BotStateUpdate(BaseModel):
    enabled: Optional[bool] = None
    strategy_type: Optional[StrategyType] = None
    config: Optional[Dict[str, Any]] = None


class ToggleReq(BaseModel):
    enabled: bool = Field(..., description="true to enable bot, false to disable")


# --------------------------
# Endpoints
# --------------------------
@router.get("/bot/state", response_model=BotState)
def get_bot_state(user=Depends(require_auth_optional)):
    st = _load_bot_state(getattr(user, "user_id", None))
    # defensive normalization
    try:
        st["updated_ms"] = int(st.get("updated_ms") or 0)
    except Exception:
        st["updated_ms"] = 0
    if "config" not in st or not isinstance(st["config"], dict):
        st["config"] = {}
    return st


@router.post("/bot/state", response_model=BotState)
def update_bot_state(req: BotStateUpdate, user=Depends(require_auth_optional)):
    uid = getattr(user, "user_id", None)

    st = _load_bot_state(uid)

    if req.enabled is not None:
        st["enabled"] = bool(req.enabled)

    if req.strategy_type is not None:
        st["strategy_type"] = str(req.strategy_type)

    if req.config is not None:
        # Replace config blob (UI sends full blob). If you want merge, do it in UI.
        st["config"] = dict(req.config)

    _save_bot_state(uid, st)
    return st


@router.post("/bot/toggle", response_model=BotState)
def toggle_bot(req: ToggleReq, user=Depends(require_auth_optional)):
    uid = getattr(user, "user_id", None)
    st = _load_bot_state(uid)
    st["enabled"] = bool(req.enabled)
    _save_bot_state(uid, st)
    return st


@router.post("/bot/reset", response_model=BotState)
def reset_bot_state(user=Depends(require_auth_optional)):
    uid = getattr(user, "user_id", None)
    st = _default_bot_state()
    _save_bot_state(uid, st)
    return st


# Convenience alias (some UI might prefer /strategy/state)
@router.get("/state", response_model=BotState)
def get_state_alias(user=Depends(require_auth_optional)):
    return get_bot_state(user=user)