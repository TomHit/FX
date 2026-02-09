import json
import time
from typing import Any, Dict, Optional
import os
from fastapi import APIRouter, Depends, HTTPException, Request,Body
from pydantic import BaseModel, Field
import redis
from api.strategy.oppt_executor import _enqueue_mt5_market_order
from redis.exceptions import AuthenticationError, ConnectionError, TimeoutError
# Import your existing optional auth resolver
# Adjust this import path to match your repo structure:
from .trend_endpoints import require_auth_optional

REDIS_URL = (os.getenv("REDIS_URL") or "").strip()
R = redis.Redis.from_url(REDIS_URL, decode_responses=True) if REDIS_URL else None
router = APIRouter(prefix="/strategy/oppt", tags=["strategy-oppt"])

STATE_KEY = "xtl:strategy:oppt:state:{uid}"
STATE_TTL_SEC = 30 * 24 * 3600  # 30 days

# paper store keys (must match oppt_executor.py)
OPEN_KEY = "xtl:strategy:oppt:open:{uid}"
CLOSED_KEY = "xtl:strategy:oppt:closed:{uid}"
EXECUTED_KEY = "xtl:strategy:oppt:executed:{uid}"
LOCK_KEY = "xtl:strategy:oppt:lock:{uid}"
COOLDOWN_MATCH = "xtl:strategy:oppt:cooldown:{uid}:*"

@router.post("/paper/clear")
def clear_paper_trades(
    request: Request,
    user=Depends(require_auth_optional),
):
    uid = None
    try:
        uid = str((user or {}).get("sub") or "").strip()
    except Exception:
        uid = None

    if not uid:
        raise HTTPException(status_code=401, detail="Unauthorized")

    if R is None:
        raise HTTPException(status_code=503, detail="Redis unavailable")

    # delete core ledgers
    try:
        R.delete(
            OPEN_KEY.format(uid=uid),
            CLOSED_KEY.format(uid=uid),
            EXECUTED_KEY.format(uid=uid),
            LOCK_KEY.format(uid=uid),
        )
    except Exception as e:
        raise HTTPException(status_code=503, detail="Redis unavailable") from e

    # delete cooldown keys
    try:
        for k in R.scan_iter(match=COOLDOWN_MATCH.format(uid=uid), count=500):
            try:
                R.delete(k)
            except Exception:
                pass
    except Exception:
        pass

    return {"ok": True}


# ---------------------------
# Models
# ---------------------------
class OpptStrategyState(BaseModel):
    enabled: bool = False

    # execution
    execution_mode: str = Field(default="paper", pattern="^(paper|mt5)$")
    mt5_account: str = Field(default="demo", pattern="^(demo|live)$")

    # rails
    qty: float = 1.0
    max_positions: int = 1
    qty_fx: float = 0.0
    qty_metals: float = 0.0
    cooldown_min: int = 0

    # NEW: per-symbol sizing (UI uses this)
    risk_mode: str = Field(default="qty_by_symbol", pattern="^(qty|qty_fx_metal|qty_by_symbol)$")
    qty_by_symbol: dict = Field(default_factory=dict)

    # filters
    min_score: float = 0.0
    min_confidence: str = Field(default="medium", pattern="^(low|medium|high)$")

    # debug (last enqueue results)
    last_enqueue: Optional[dict] = None
    last_exit_enqueue: Optional[dict] = None


    # meta
    started_at_ms: Optional[int] = None
    updated_at_ms: Optional[int] = None


class OpptStrategyPatch(BaseModel):
    enabled: Optional[bool] = None

    execution_mode: Optional[str] = Field(default=None, pattern="^(paper|mt5)$")
    mt5_account: Optional[str] = Field(default=None, pattern="^(demo|live)$")

    qty: Optional[float] = None
    max_positions: Optional[int] = None
    qty_fx: Optional[float] = None
    qty_metals: Optional[float] = None
    risk_mode: Optional[str] = Field(default=None, pattern="^(qty|qty_fx_metal|qty_by_symbol)$")
    qty_by_symbol: Optional[dict] = None

    cooldown_min: Optional[int] = None

    min_score: Optional[float] = None
    min_confidence: Optional[str] = Field(default=None, pattern="^(low|medium|high)$")

class Mt5TestOrderReq(BaseModel):
    symbol: str = Field(default="XAUUSD")
    side: str = Field(default="BUY", pattern="^(BUY|SELL)$")
    volume: float = Field(default=0.01, gt=0)
    sl: Optional[float] = None
    tp: Optional[float] = None
    comment: Optional[str] = "XTL TEST"

class Mt5PlaceOrderReq(BaseModel):
    symbol: str
    side: str  # BUY/SELL
    volume: float = Field(gt=0)
    sl: Optional[float] = None
    tp: Optional[float] = None
    comment: Optional[str] = "XTL UI"

@router.post("/mt5/order")
def mt5_place_order(req: Mt5PlaceOrderReq, request: Request, user=Depends(require_auth_optional)):
    uid = _require_uid(user)

    st = _sanitize_state(_get_state(uid))
    if str(st.get("execution_mode") or "").lower() != "mt5":
        raise HTTPException(status_code=400, detail="Set execution_mode=mt5 first.")

    mt5_account = str(st.get("mt5_account") or "demo").lower().strip()
    if mt5_account not in ("demo", "live"):
        mt5_account = "demo"

    sym = (req.symbol or "").upper().strip()
    side = (req.side or "").upper().strip()
    if side not in ("BUY", "SELL"):
        raise HTTPException(status_code=400, detail="side must be BUY or SELL")

    enq = _enqueue_mt5_market_order(
        user_id=uid,
        sym=sym,
        side=side,
        volume=float(req.volume),
        sl=float(req.sl) if req.sl is not None else None,
        tp=float(req.tp) if req.tp is not None else None,
        comment=req.comment or "XTL UI",
        kind="ENTRY",
        mt5_account=mt5_account,
    )

    if not enq.get("ok"):
        raise HTTPException(status_code=400, detail=enq.get("error") or "enqueue_failed")

    return {
        "ok": True,
        "job_id": enq.get("job_id"),
        "device_id": enq.get("device_id"),
        "mt5_account": mt5_account,
        "symbol": sym,
        "side": side,
        "volume": float(req.volume),
    }

@router.get("/mt5/status")
def mt5_status(job_id: str, user=Depends(require_auth_optional)):
    uid = _require_uid(user)
    job = (job_id or "").strip()
    if not job:
        raise HTTPException(status_code=400, detail="missing job_id")

    key = f"xtl:mt5:ack:{job}"
    raw = R.get(key)
    if not raw:
        return {"ok": True, "found": False, "job_id": job, "ack": None}

    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8", "ignore")
    try:
        ack = json.loads(raw)
    except Exception:
        ack = {"raw": raw}

    return {"ok": True, "found": True, "job_id": job, "ack": ack}


@router.post("/mt5/test-order")
def mt5_test_order(req: Mt5TestOrderReq, request: Request, user=Depends(require_auth_optional)):
    uid = _require_uid(user)

    # must be demo + mt5 mode (safety)
    st = _sanitize_state(_get_state(uid))
    if str(st.get("execution_mode") or "").lower() != "mt5":
        raise HTTPException(status_code=400, detail="Set execution_mode=mt5 first.")
    if str(st.get("mt5_account") or "").lower() != "demo":
        raise HTTPException(status_code=400, detail="Refusing: test-order is demo-only.")

    sym = (req.symbol or "").upper().strip()
    side = (req.side or "").upper().strip()

    enq = _enqueue_mt5_market_order(
        user_id=uid,
        sym=sym,
        side=side,
        volume=float(req.volume),
        sl=float(req.sl) if req.sl is not None else None,
        tp=float(req.tp) if req.tp is not None else None,
        comment=req.comment or "XTL TEST",
        kind="ENTRY",
        mt5_account="demo",
    )

    if not enq.get("ok"):
        raise HTTPException(status_code=400, detail=enq.get("error") or "enqueue_failed")

    return {
        "ok": True,
        "job_id": enq.get("job_id"),
        "device_id": enq.get("device_id"),
        "symbol": sym,
        "side": side,
        "volume": float(req.volume),
    }

@router.get("/mt5/ack/{job_id}")
def mt5_ack_status(job_id: str, user=Depends(require_auth_optional)):
    uid = _require_uid(user)

    job = (job_id or "").strip()
    if not job:
        raise HTTPException(status_code=400, detail="missing job_id")

    try:
        raw = R.get(f"xtl:mt5:ack:{job}")
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"redis_error:{type(e).__name__}") from e

    if not raw:
        return {"ok": True, "job_id": job, "ack": None}

    try:
        ack = json.loads(raw)
    except Exception:
        ack = {"raw": raw}
    owner = (ack.get("user_id") or (ack.get("result") or {}).get("user_id") or "").strip()
    if owner and owner != uid:
        raise HTTPException(status_code=403, detail="forbidden")

    # optional: safety check job belongs to user (since your ack stores no user_id)
    # if ack.get("user_id") != uid: forbid — BUT you'd need to include user_id in ack writes.

    return {"ok": True, "job_id": job, "ack": ack}

# ---------------------------
# Helpers
# ---------------------------
def _now_ms() -> int:
    return int(time.time() * 1000)


def _json_load(raw: Any) -> dict:
    if raw is None:
        return {}
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8", "ignore")
    if isinstance(raw, dict):
        return raw
    if not isinstance(raw, str):
        return {}

    raw = raw.strip()
    if not raw:
        return {}

    # 1st decode
    try:
        v = json.loads(raw)
    except Exception:
        return {}

    # If it was double-encoded, decode again
    if isinstance(v, str):
        s = v.strip()
        if (s.startswith("{") and s.endswith("}")) or (s.startswith("[") and s.endswith("]")):
            try:
                v2 = json.loads(s)
                if isinstance(v2, dict):
                    return v2
            except Exception:
                return {}

    return v if isinstance(v, dict) else {}


def _sanitize_state(st: Dict[str, Any]) -> Dict[str, Any]:
    out = {
        "enabled": bool(st.get("enabled", False)),
        "execution_mode": st.get("execution_mode") if st.get("execution_mode") in ("paper", "mt5") else "paper",
        "mt5_account": st.get("mt5_account") if st.get("mt5_account") in ("demo", "live") else "demo",

        "qty": float(st.get("qty", 1.0) or 1.0),
        "max_positions": int(st.get("max_positions", 1) or 1),

        # ✅ keep these (your executor uses them)
        "qty_fx": float(st.get("qty_fx", 0.0) or 0.0),
        "qty_metals": float(st.get("qty_metals", 0.0) or 0.0),

        # ✅ NEW: persist sizing mode + per-symbol qty
        "risk_mode": st.get("risk_mode") if st.get("risk_mode") in ("qty", "qty_fx_metal", "qty_by_symbol") else "qty_by_symbol",
        "qty_by_symbol": st.get("qty_by_symbol") if isinstance(st.get("qty_by_symbol"), dict) else {},

        "cooldown_min": int(st.get("cooldown_min", 0) or 0),

        "min_score": float(st.get("min_score", 0.0) or 0.0),
        "min_confidence": st.get("min_confidence") if st.get("min_confidence") in ("low", "medium", "high") else "medium",

        # ✅ preserve debug enqueue info
        "last_enqueue": st.get("last_enqueue", None),
        "last_exit_enqueue": st.get("last_exit_enqueue", None),

        "started_at_ms": int(st.get("started_at_ms", 0) or 0),
        "updated_at_ms": int(st.get("updated_at_ms", 0) or 0),
    }

    # --- NEW: normalize qty_by_symbol (uppercase keys + positive floats only) ---
    norm_qbs = {}
    qbs = out.get("qty_by_symbol") or {}
    if isinstance(qbs, dict):
        for k, v in qbs.items():
            sym = str(k or "").strip().upper()
            try:
                q = float(v)
            except Exception:
                continue
            if sym and q > 0:
                norm_qbs[sym] = q
    out["qty_by_symbol"] = norm_qbs

    # Clamp to sane ranges
    if out["qty"] <= 0:
        out["qty"] = 1.0
    if out["max_positions"] < 1:
        out["max_positions"] = 1
    if out["max_positions"] > 20:
        out["max_positions"] = 20
    if out["cooldown_min"] < 0:
        out["cooldown_min"] = 0
    if out["cooldown_min"] > 24 * 60:
        out["cooldown_min"] = 24 * 60
    if out["min_score"] < 0:
        out["min_score"] = 0.0

    return out


def _get_state(uid: str) -> Dict[str, Any]:
    key = STATE_KEY.format(uid=uid)
    if R is None:
       raise RuntimeError("REDIS_URL not set")

    try:
        raw = R.get(key)
    except (AuthenticationError, ConnectionError, TimeoutError) as e:
        # Surface a clear error instead of 500s
        raise HTTPException(status_code=503, detail=f"Redis unavailable: {type(e).__name__}") from e
    except Exception as e:
        raise HTTPException(status_code=503, detail="Redis unavailable") from e

    if not raw:
        return {}

    st = _json_load(raw)
    if isinstance(st, dict):
        try:
            if R.ttl(key) < 0:
                R.expire(key, STATE_TTL_SEC)
        except Exception:
            pass
        return st

    return {}


def _save_state(uid: str, st: Dict[str, Any]) -> Dict[str, Any]:
    key = STATE_KEY.format(uid=uid)
    try:
        R.set(key, json.dumps(st), ex=STATE_TTL_SEC)
    except (AuthenticationError, ConnectionError, TimeoutError) as e:
        raise HTTPException(status_code=503, detail=f"Redis unavailable: {type(e).__name__}") from e
    except Exception as e:
        raise HTTPException(status_code=503, detail="Redis unavailable") from e
    return st


def _require_uid(user) -> str:
    # require_auth_optional guarantees .user_id
    uid = getattr(user, "user_id", None)
    if not uid:
        raise HTTPException(status_code=401, detail="Login required")
    return str(uid)


# ---------------------------
# Routes (use same auth pattern)
# ---------------------------
@router.get("/state", response_model=OpptStrategyState)
def get_state(request: Request, user=Depends(require_auth_optional)):
    uid = getattr(user, "user_id", None)
    if not uid:
        # public/anonymous -> defaults
        return OpptStrategyState()

    st = _sanitize_state(_get_state(str(uid)))
    return OpptStrategyState(**st)



@router.post("/start")
def start_strategy(
    patch: OpptStrategyPatch,   # <-- NEW: accept payload like {execution_mode, qty, ...}
    request: Request,
    user=Depends(require_auth_optional),
):
    uid = _require_uid(user)

    # Load current state (raw), then apply patch, then force enabled=true
    st = _get_state(uid)

    # Apply patch fields (same logic as /patch)
    for k, v in patch.model_dump(exclude_unset=True).items():
        st[k] = v

    # Force enabled ON for /start
    st["enabled"] = True

    now = int(time.time() * 1000)
    st["updated_at_ms"] = now
    if not st.get("started_at_ms"):
        st["started_at_ms"] = now

    _save_state(uid, st)

    # Return sanitized state
    return _sanitize_state(st)


@router.post("/stop", response_model=OpptStrategyState)
def stop_strategy(request: Request, user=Depends(require_auth_optional)):
    uid = _require_uid(user)

    existing = _sanitize_state(_get_state(uid))
    now = _now_ms()

    merged = dict(existing)
    merged["enabled"] = False
    merged["updated_at_ms"] = now

    merged = _sanitize_state(merged)
    _save_state(uid, merged)
    return OpptStrategyState(**merged)


@router.post("/patch", response_model=OpptStrategyState)
def patch_strategy(patch: OpptStrategyPatch, request: Request, user=Depends(require_auth_optional)):
    uid = _require_uid(user)

    existing = _sanitize_state(_get_state(uid))
    now = _now_ms()

    merged = dict(existing)

    p = patch.dict(exclude_unset=True)
    for k, v in p.items():
        merged[k] = v

    # If enabling from disabled, stamp started_at_ms
    if bool(merged.get("enabled")) and not bool(existing.get("enabled")):
        merged["started_at_ms"] = now

    merged["updated_at_ms"] = now
    merged = _sanitize_state(merged)
    _save_state(uid, merged)
    return OpptStrategyState(**merged)

@router.get("/paper/trades")
def get_paper_trades(
    limit: int = 50,
    user=Depends(require_auth_optional),
):
    if not user.user_id:
        raise HTTPException(status_code=401, detail="Login required")

    uid = str(user.user_id)

    open_key = f"xtl:strategy:oppt:open:{uid}"
    closed_key = f"xtl:strategy:oppt:closed:{uid}"

    open_trades = []
    for _, v in R.hgetall(open_key).items():
        try:
            open_trades.append(json.loads(v))
        except Exception:
            pass

    closed_raw = R.lrange(closed_key, 0, max(0, limit - 1))
    closed_trades = []
    for v in closed_raw:
        try:
            closed_trades.append(json.loads(v))
        except Exception:
            pass

    return {
        "open": open_trades,
        "closed": closed_trades,
    }

