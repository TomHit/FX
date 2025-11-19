# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Literal, List, Tuple, Optional,Any,Dict
from fastapi import APIRouter, HTTPException, Depends,Query,Request,Header
from pydantic import BaseModel, Field, validator
import os, json, time as _time, logging,redis,re,time
import math
import httpx
from .db import db
from fastapi.responses import JSONResponse
from pathlib import Path
import xgboost as xgb
import csv
from typing import Any, Dict, List
log = logging.getLogger("xtl.trend")


REG_PATH = Path("/opt/xauapi/api/trend/models/xgb_reg.json")
CLS_PATH = Path("/opt/xauapi/api/trend/models/xgb_cls.json")

REG_MODEL: xgb.Booster | None = None
CLS_MODEL: xgb.Booster | None = None


BROKER_DIGITS = int(os.getenv("BROKER_DIGITS", "3"))  # set 3 if your XAU broker uses 3 digits
FORCE_TZ_OFFSET_MIN = os.getenv("FORCE_TZ_OFFSET_MIN")  # e.g., "0", "120", "180"

# Make sure this matches the writer (routes_devices.py)
REDIS_URL = os.getenv("REDIS_URL", "redis://default:xau12345@10.0.0.132:6379/0")
R = redis.from_url(REDIS_URL, decode_responses=True)
log.info(f"[TREND]  module={__file__}")
log.info(f"[TREND] REDIS_URL={REDIS_URL}")



# Optional per-symbol (or global) calibration for regressor output.
# Read env like CALIB_X=10.0 or CALIB_EURUSD_X=8.0 (percent scaler).
def _calib_multiplier(sym: str) -> float:
    s = (sym or "").upper()
    try:
        val = os.getenv(f"CALIB_{s}_X") or os.getenv("CALIB_X") or "1.0"
        return float(val)
    except Exception:
        return 1.0

def _pct_decimals(sym: str, value: float | None = None) -> int:
    s = sym.upper()
    if s.endswith("JPY"):
        # JPY: show more detail by default; even more if minuscule
        if isinstance(value, (int, float)) and abs(value) < 0.15:
            return 4
        return 3
    # Majors (EURUSD, GBPUSD, etc.): bump precision for small moves
    if isinstance(value, (int, float)):
        a = abs(value)
        if a < 0.01:   # < 0.01% -> 4 dp (prevents 0.00%)
            return 4
        if a < 0.1:    # < 0.10% -> 3 dp
            return 3
    return 2

def _normalize_pct(sym: str, v: float | None) -> float | None:
    """
    Ensure v is in PERCENT units.
    If a fractional input sneaks in (e.g., 0.0004 meaning 0.04%),
    scale it to percent. Keep symbol-aware rounding.
    """
    if not isinstance(v, (int, float)):
        return None
    x = float(v)
    # Treat any |x| < 1.0 as fraction-of-1 and scale to percent
    if abs(x) < 1.0:
        x *= 100.0
    return _round_pct(sym, x)



def _build_reasons(sym: str, label: str, p_up: float, extra: Dict[str, Any]) -> List[str]:
    reasons: List[str] = []
    lbl = (label or "").lower()

    # 1) Trend / structure
    if lbl == "bullish":
        reasons.append("Trend engine sees a bullish 1h structure")
    elif lbl == "bearish":
        reasons.append("Trend engine sees a bearish 1h structure")

    # 2) Relative volume (RVOL on M15)
    rv = extra.get("feat_rvol15")
    try:
        rv_val = float(rv) if rv is not None else None
    except Exception:
        rv_val = None

    if isinstance(rv_val, (int, float)):
        if rv_val >= 2.0:
            reasons.append(f"Volume ~{rv_val:.1f}x normal (RVOL)")
        elif rv_val >= 1.3:
            reasons.append("Volume above normal (RVOL > 1.3x)")
        elif rv_val <= 0.6:
            reasons.append("Volume below normal (RVOL < 0.6x)")
        # 0.6–1.3x -> treated as normal; no extra sentence

    # 3) USD basket tilt (proxy for DXY / macro USD tone)
    basket = extra.get("feat_usd_basket")
    try:
        basket_val = float(basket) if basket is not None else None
    except Exception:
        basket_val = None

    if isinstance(basket_val, (int, float)) and abs(basket_val) >= 0.10:
        # basket_val > 0  => broad USD strength
        # basket_val < 0  => broad USD weakness
        inv_pairs = {"EURUSD", "GBPUSD", "AUDUSD"}  # USD is quote
        s = sym.upper()

        if basket_val > 0:
            # USD stronger
            if s in inv_pairs:
                reasons.append("Broad USD strength, a headwind for the pair")
            else:
                reasons.append("Broad USD strength supporting the move")
        else:
            # USD weaker
            if s in inv_pairs:
                reasons.append("Broad USD weakness supporting the move")
            else:
                reasons.append("Broad USD weakness, a headwind for the pair")

    # 4) Model confidence band
    try:
        p_val = float(p_up)
    except Exception:
        p_val = None

    if isinstance(p_val, (int, float)):
        if p_val >= 0.70:
            reasons.append(f"Model confidence is high (ProbUp {p_val:.2f})")
        elif p_val >= 0.60:
            reasons.append(f"Model edge with decent confidence (ProbUp {p_val:.2f})")
        elif p_val <= 0.40:
            reasons.append(f"Model leans down (ProbUp {p_val:.2f})")

    # 5) Fallback to any raw reason from detection if nothing else
    raw_reason = extra.get("reason")
    if not reasons and isinstance(raw_reason, str) and raw_reason:
        reasons.append(raw_reason)

    # De-duplicate while preserving order
    out: List[str] = []
    for r in reasons:
        if r not in out:
            out.append(r)
    return out


def _round_pct(sym: str, v: float) -> float:
    return round(float(v), _pct_decimals(sym, float(v)))

def _fmt_mtime(p: Path) -> str:
    try:
        return _time.strftime("%Y-%m-%d %H:%M:%S", _time.localtime(p.stat().st_mtime))
    except Exception:
        return "unknown"

def load_models_if_needed() -> None:
    """Idempotent: load once, log status."""
    global REG_MODEL, CLS_MODEL

    # REG booster
    if REG_MODEL is None:
        try:
            if REG_PATH.exists():
                mtime = _fmt_mtime(REG_PATH)
                booster = xgb.Booster()
                booster.load_model(str(REG_PATH))          # loads JSON
                REG_MODEL = booster
                log.info("loaded xgb_reg.json  size=%d  mtime=%s", REG_PATH.stat().st_size, mtime)
            else:
                log.warning("xgb_reg.json not found at %s", REG_PATH)
        except Exception as e:
            REG_MODEL = None
            log.exception("failed to load xgb_reg.json: %s", e)

    # CLS booster
    if CLS_MODEL is None:
        try:
            if CLS_PATH.exists():
                mtime = _fmt_mtime(CLS_PATH)
                booster = xgb.Booster()
                booster.load_model(str(CLS_PATH))
                CLS_MODEL = booster
                log.info("loaded xgb_cls.json  size=%d  mtime=%s", CLS_PATH.stat().st_size, mtime)
            else:
                log.warning("xgb_cls.json not found at %s", CLS_PATH)
        except Exception as e:
            CLS_MODEL = None
            log.exception("failed to load xgb_cls.json: %s", e)


router = APIRouter()

@router.on_event("startup")
async def _startup_models():
    load_models_if_needed()

PRED_LOG = Path("/opt/xauapi/api/trend/out/predict_log.csv")
PRED_RAW_LOG = PRED_LOG.with_name("predict_reg_debug.csv")


def _log_prediction(row: dict, last_close: float) -> None:
    try:
        is_new = not PRED_LOG.exists()
        with PRED_LOG.open("a", newline="") as f:
            w = csv.writer(f)
            if is_new:
                w.writerow([
                    "computed_at_ms","symbol","tf",
                    "last_close","p_up","expected_move_pct_1h",
                    "decision","target_price_1h",
                    "target_close_ts","horizon",
                ])
            w.writerow([
                int(row.get("server_now_ms", 0)),
                row["symbol"],
                row.get("update_tf","M15"),
                float(last_close) if last_close is not None else "",
                float(row.get("p_up")) if row.get("p_up") is not None else "",
                float(row.get("expected_move_pct_1h")) if row.get("expected_move_pct_1h") is not None else "",
                row.get("decision",""),
                float(row.get("target_price_1h")) if row.get("target_price_1h") is not None else "",
                int(row.get("target_close_ts",0)),
                row.get("horizon",""),
            ])
    except Exception:
        pass

@router.get("/predict/eval/ready")
def predict_eval_ready(limit: int = 500):
    """
    For all logged predictions whose target_close_ts has passed,
    compute realized outcome using device OHLC (M15/H1) and report metrics.
    """
    import pandas as pd
    from api.trend.infer_rt import pull_latest_m15  # uses agent-pushed OHLC

    if not PRED_LOG.exists():
        return {"ok": False, "reason": "no_log"}

    df = pd.read_csv(PRED_LOG)
    if df.empty:
        return {"ok": False, "reason": "empty_log"}

    now_ms = int(time.time()*1000)
    ready = df[df["target_close_ts"] <= now_ms].copy()
    if ready.empty:
        return {"ok": True, "n_ready": 0, "metrics": {}}

    rows = []
    for _, r in ready.tail(limit).iterrows():
        sym = str(r["symbol"])
        last_close = float(r["last_close"])
        target_close_ts = int(r["target_close_ts"])
        try:
            # get recent M15 bars and find the bar whose close matches target_close_ts
            dff = pull_latest_m15(sym)
            if dff is None or dff.empty:
                continue
            # dff['t'] in epoch seconds; compute t_close_ms per row: (t_open_ms + 15m)
            # If your DF already has close time, adapt accordingly.
            dff = dff.copy()
            dff["t_open_ms"] = (dff["t"].astype("int64") * 1000)
            dff["t_close_ms"] = dff["t_open_ms"] + (15*60*1000)
            hit = dff.loc[dff["t_close_ms"] == target_close_ts]
            if hit.empty:
                # tolerate slight clock skews: nearest within ±1 min
                hit = dff.iloc[(dff["t_close_ms"] - target_close_ts).abs().argsort()[:1]]
            close1h = float(hit["close"].iloc[0])

            move_real_pct = ((close1h / last_close) - 1.0) * 100.0
            dir_real = "BUY" if move_real_pct > 0 else "SELL" if move_real_pct < 0 else "FLAT"
            dir_pred = str(r.get("decision","")).upper()

            rows.append({
                "symbol": sym,
                "computed_at_ms": int(r["computed_at_ms"]),
                "target_close_ts": target_close_ts,
                "p_up": float(r["p_up"]) if r["p_up"] == r["p_up"] else None,
                "move_pred_pct": float(r["expected_move_pct_1h"]) if r["expected_move_pct_1h"] == r["expected_move_pct_1h"] else None,
                "move_real_pct": move_real_pct,
                "dir_pred": dir_pred,
                "dir_real": dir_real,
                "dir_hit": (dir_pred == dir_real and dir_pred in ("BUY","SELL")),
                "mae_pct": abs((float(r["target_price_1h"]) - close1h) / last_close * 100.0) if r["target_price_1h"] == r["target_price_1h"] else None,
            })
        except Exception:
            continue

    if not rows:
        return {"ok": True, "n_ready": 0, "metrics": {}}

    import statistics as st
    hits = [x["dir_hit"] for x in rows if x["dir_pred"] in ("BUY","SELL")]
    maes = [x["mae_pct"] for x in rows if x["mae_pct"] is not None]
    mean_mae = (sum(maes)/len(maes)) if maes else None

    return {
        "ok": True,
        "n_ready": len(rows),
        "acc_directional": (sum(hits)/len(hits)) if hits else None,
        "mae_pct": mean_mae,
        "samples": rows[:20],  # top few to eyeball
    }


# --- Model version string (derived from model file mtimes; fallback 'unknown') ---
MODEL_VERSION = "unknown"
try:
    from pathlib import Path
    _reg_p = Path("/opt/xauapi/api/trend/models/xgb_reg.json")
    _cls_p = Path("/opt/xauapi/api/trend/models/xgb_cls.json")
    if _reg_p.exists() and _cls_p.exists():
        # use millisecond mtime for readability
        _reg_v = int(_reg_p.stat().st_mtime * 1000)
        _cls_v = int(_cls_p.stat().st_mtime * 1000)
        MODEL_VERSION = f"reg_{_reg_v}_cls_{_cls_v}"
except Exception:
    pass


def _next_boundary_ms(tf_sec: int, now_ms: int, off_min: int) -> int:
    off_ms = off_min * 60_000
    tf_ms = tf_sec * 1000
    return (((now_ms + off_ms) // tf_ms) + 1) * tf_ms - off_ms

# --- per-symbol meta (configs/symbol_meta.json) -------------------------------
import os, json, time
from typing import Optional

_META_PATH = os.path.join(os.path.dirname(__file__), "..", "configs", "symbol_meta.json")
_META_PATH = os.path.abspath(_META_PATH)

class _MetaCache:
    data: dict[str, dict] = {}
    mtime: float = 0.0

    @classmethod
    def load(cls, force: bool = False):
        try:
            mt = os.path.getmtime(_META_PATH)
        except OSError:
            return
        if not force and mt <= cls.mtime:
            return
        with open(_META_PATH, "r", encoding="utf-8") as f:
            raw = json.load(f)
        # accept dict or list
        out: dict[str, dict] = {}
        if isinstance(raw, dict):
            for k, v in raw.items():
                d = dict(v or {})
                d.setdefault("symbol", k)
                out[k.upper()] = d
        elif isinstance(raw, list):
            for it in raw:
                if not isinstance(it, dict): 
                    continue
                sym = str(it.get("symbol", "")).upper()
                if sym:
                    out[sym] = dict(it)
        cls.data = out
        cls.mtime = mt

def _get_meta(sym: str) -> dict:
    if not _MetaCache.data:
        _MetaCache.load(force=True)
    return _MetaCache.data.get(sym.upper(), {
        "symbol": sym.upper(),
        "tau": 0.55,
        "abstain_band": 0.02,
        "p_hi": 0.7,
        "spread_bp": 3.0,
        "min_rvol": 0.8,
        "target_atr": {"mult": 0.8, "floor_pips": 0.0},
        "reasons": {"DXY": -1, "UST10Y": -1, "USD_SHORT_RATE": -1, "RVOL": 1, "VIX": -1},
    })

def _policy_decision(sym: str, p_up: float, atr_val: float | None = None):
    """
    Map p_up to decision using per-symbol meta.
    Returns: decision (BUY/SELL/ABSTAIN), target_pips (float), confidence (low/med/high)
    """
    m = _get_meta(sym)
    tau = float(m.get("tau", 0.55))
    band = float(m.get("abstain_band", 0.02))
    p_hi = float(m.get("p_hi", 0.7))
    spread_bp = float(m.get("spread_bp", 3.0))
    tgt = m.get("target_atr", {}) or {}
    mult = float(tgt.get("mult", 0.8))
    floor_pips = float(tgt.get("floor_pips", 0.0))

    # abstain band around 0.5
    if abs(p_up - 0.5) < band:
        return "ABSTAIN", 0.0, "low"

    side = "BUY" if p_up >= tau else "SELL"
    conf = "high" if (p_up >= p_hi or (1.0 - p_up) >= p_hi) else "med"

    # target: ATR-based if given; else tiny floor from spread
    if atr_val is None:
        target = max(floor_pips, mult * (spread_bp / 10_000.0))
    else:
        target = max(floor_pips, mult * float(atr_val))
    return side, float(target), conf


# ---- Optional auth (session/relaxed) shim for /trend/* routes ----------------

from types import SimpleNamespace

# Try to import session + uid helpers from routes_devices (your project already has them)
try:
    from api.routes_devices import _session_user, _uid as _uid_hard, _uid_from as _uid_soft  # preferred
except Exception:  # fall back to local import if package path differs
    try:
        from routes_devices import _session_user, _uid as _uid_hard, _uid_from as _uid_soft
    except Exception:
        # final fallbacks (no session helpers available)
        def _session_user(_req): return None
        def _uid_hard(u): 
            # minimal version: try common shapes
            if isinstance(u, dict):
                return u.get("id") or u.get("user_id") or u.get("sub")
            for k in ("id","user_id","uid","sub"):
                v = getattr(u, k, None)
                if v: return v
            return None
        def _uid_soft(_u): return None

# relaxed current-user (if your deps provide it)
try:
    from api.deps import get_current_user_relaxed  # type: ignore
except Exception:
    try:
        from deps import get_current_user_relaxed  # type: ignore
    except Exception:
        get_current_user_relaxed = None  # not available in this env

# --- UI price formatting (display only) ---
DISPLAY_DIGITS = {
    "XAUUSD": 2,   # 4110.04
    "EURUSD": 5,
    "GBPUSD": 5,
    "USDCAD": 5,
    "USDCHF": 5,
    "USDJPY": 3,   # 154.067
}
def _fmt_price(symbol: str, p: float, broker: dict | None) -> float:
    # If the snapshot carried broker.digits, prefer that; else fall back to table above.
    bd = None
    try:
        bd = int((broker or {}).get("digits"))
    except Exception:
        bd = None
    digits = bd if isinstance(bd, int) else DISPLAY_DIGITS.get(symbol.upper(), int(os.getenv("BROKER_DIGITS", "5")))
    try:
        return round(float(p), digits)
    except Exception:
        return float(p)
TF_SEC_MAP = {"M1": 60, "M5": 300, "M15": 900, "H1": 3600, "H4": 14400}

def _pick_last_closed_bar(snap: dict, tf: str, now_ms: int) -> dict | None:
    """
    snap: {"bars":[{"t": <epoch seconds>, "o":..., "h":..., "l":..., "c":...}, ...]}
    Return the last CLOSED bar (dict) or None.
    A bar with start time t (sec) is closed when now_ms >= (t + TF_SEC) * 1000.
    """
    try:
        bars = snap.get("bars") or []
        if not bars:
            return None
        tf_ms = TF_SEC_MAP.get(tf, 60) * 1000
        # Traverse from the end until we find a closed one
        for b in reversed(bars):
            t_ms = int(b["t"]) * 1000  # t is in seconds in our snapshots
            if now_ms >= t_ms + tf_ms:
                return b
        # None closed? then no result
        return None
    except Exception:
        return None



# read a specific device snapshot for symbol/tf
def _read_snap_for_device(device_id: str, symbol: str, tf: str):
    try:
        key = f"xtl:ohlc:snap:{device_id}:{symbol}:{tf}"
        raw = R.get(key)
        if not raw:
            return None, None
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", "ignore")
        snap = json.loads(raw)

        # optional: broker meta from device hash if you keep it there
        b = None
        try:
            h = R.hgetall(f"device:{device_id}")
            if h:
                # normalize bytes?str
                b = { (k.decode() if isinstance(k,(bytes,bytearray)) else str(k)) :
                      (v.decode() if isinstance(v,(bytes,bytearray)) else str(v))
                      for k,v in h.items() }
        except Exception:
            pass
        return snap, b
    except Exception:
        return None, None


def require_auth_optional(request: Request):
    """
    Best-effort user resolver for public-ish endpoints:
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


# ------------------------------------------------------------------------------
# Models (mirror your UI controls)
# ------------------------------------------------------------------------------
def _tf_ms_from_u(tf_u: str) -> int:
    # tf_u is like "M15" | "H1" | "H4"
    tf_u = (tf_u or "").upper()
    if tf_u == "M15": return 15 * 60 * 1000
    if tf_u == "H1":  return 60 * 60 * 1000
    if tf_u == "H4":  return 4  * 60 * 60 * 1000
    return 60 * 60 * 1000  # default H1

def _align_next_close_ms(now_ms: int, tf_ms: int, tz_offset_min: int | None) -> int:
    off_ms = int(tz_offset_min or 0) * 60_000
    # shift into broker TZ, align, then shift back
    return (( (now_ms + off_ms) // tf_ms ) + 1) * tf_ms - off_ms


def _is_uuid(s: str) -> bool:
    import re
    return bool(re.fullmatch(
        r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}", s))


def _resolve_user_id(user_key: str) -> str:
    # Already a UUID?
    if _is_uuid(user_key):
        return user_key

    # 1) Fast path: Redis usermap
    try:
        uid = R.get(f"xtl:usermap:{user_key}")
        if uid and _is_uuid(uid):
            return uid
    except Exception:
        pass

    # 2) DB fallback: map username/email -> canonical UUID
    try:
        # Adjust SQL to your schema/driver:
        # db.fetch_one should return a dict with 'id' (UUID as text)
        row = db.fetch_one(
            "SELECT id FROM users WHERE username = %s OR email = %s LIMIT 1",
            (user_key, user_key),
        )
        if row and row.get("id"):
            uid = str(row["id"])
            # cache for a day
            try:
                R.setex(f"xtl:usermap:{user_key}", 86400, uid)
            except Exception:
                pass
            return uid
    except Exception as e:
        log.info(f"[AUTH] DB resolve error for {user_key}: {e}")

    # 3) Last resort: return the original key
    return user_key


import os, json, redis
from fastapi import Request

def get_user_id(request: Request) -> str:
    """
    Canonical UUID for current user:
    JWT -> session -> (optional) X-User-Key header -> (optional) demo fallback.
    Non-UUIDs are resolved via Redis usermap in _resolve_user_id().
    """
    allow_demo = os.getenv("ALLOW_DEMO_USER", "false").lower() == "true"
    allow_hdr  = os.getenv("ALLOW_X_USER_KEY", "false").lower() == "true"

    # 1) JWT (stub; plug in real decode if you use JWTs)
    authz = request.headers.get("authorization")
    if authz and authz.lower().startswith("bearer "):
        token = authz.split(None, 1)[1]
        try:
            claims = {}  # TODO: decode(token)
            user_key = claims.get("sub") or claims.get("email") or claims.get("username")
            if user_key:
                uid = _resolve_user_id(str(user_key))
                log.info(f"[AUTH] via JWT key={user_key} -> {uid}")
                return uid
        except Exception as e:
            log.info(f"[AUTH] JWT decode error: {e}")

    # 2) Session (set by SessionMiddleware)
    sess = getattr(request, "session", None) or getattr(request.state, "session", {}) or {}
    user_key = sess.get("user_id") or sess.get("uuid") or sess.get("username")
    if user_key:
        uid = _resolve_user_id(str(user_key))
        log.info(f"[AUTH] via session key={user_key} -> {uid}")
        return uid

    # 3) (Optional) X-User-Key header for CLI/local testing
    if allow_hdr:
        hdr_key = (
            request.headers.get("x-user-key")
            or request.headers.get("X-User-Key")
            or request.headers.get("X_User_Key")
            or request.headers.get("x_user_key")
        )
        if hdr_key:
            hdr_key = str(hdr_key).strip()
            uid = _resolve_user_id(hdr_key)
            log.info(f"[AUTH] via X-User-Key={hdr_key} -> {uid}")
            return uid

    # 4) (Optional) demo fallback
    if allow_demo:
        uid = _resolve_user_id("user_demo")
        log.info(f"[AUTH] demo fallback -> {uid}")
        return uid

    # minimal signal without dumping headers/cookies
    has_xuk = any(h in request.headers for h in ("x-user-key","X-User-Key","X_User_Key","x_user_key"))
    log.info(f"[AUTH] no credentials; rejecting (x-user-key-present={has_xuk})")
    raise HTTPException(status_code=401, detail="Auth required")


# --- Prediction feed (lightweight; 1-min refresh) ---
SYMBOLS_ALL = ["XAUUSD","EURUSD","USDJPY","GBPUSD","USDCAD","USDCHF"]

def _latest_from_user_snap(uid: str, sym: str, tfu: str):
    """
    Read last CLOSED bar from the user snapshot the agent writes:
    xtl:trend:snap:{user_id}:{SYM}:{TF} with bars stored in **seconds**.
    """
    key = f"xtl:trend:snap:{uid}:{sym}:{tfu}"
    raw = R.get(key)
    if not raw:
        return None
    try:
        js = json.loads(raw)
        bars = js.get("bars") or []
        if not bars:
            return None
        b = bars[-1]
        # 't' is OPEN in **seconds** in these snapshots
        t_s = int(b.get("t", 0))
        # form a quote-ish payload; price basis = last close
        return {
            "t_ms": (t_s * 1000),
            "o": float(b.get("o", 0)),
            "h": float(b.get("h", 0)),
            "l": float(b.get("l", 0)),
            "c": float(b.get("c", 0)),
        }
    except Exception:
        return None
# --- Price from latest CLOSED M1 bar -----------------------------------------
TF_MS = {"M1": 60_000}

def _ms_from_t(v):
    # supports t (sec) or t_open_ms/ms
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return int(v * 1000) if v < 10_000_000_000 else int(v)
    return None

def _read_freshest_snap_for_user_or_any(uid, sym_u: str, tfu: str):
    """
    Try user's devices first; else scan any device. Returns (snap, broker).
    snap shape expected: {"bars":[{t|t_open_ms,o,h,l,c,complete?},...], "broker": {...}}
    """
    import json, time
    now_ms = int(time.time() * 1000)

    # 1) try user's devices (if you store them e.g. set user:{uid}:devices)
    dev_ids = []
    try:
        dev_ids = list(R.smembers(f"user:{uid}:devices")) if uid else []
    except Exception:
        dev_ids = []
    candidates = []
    for dev in dev_ids:
        try:
            raw = R.get(f"xtl:ohlc:snap:{dev.decode() if isinstance(dev, (bytes,bytearray)) else dev}:{sym_u}:{tfu}")
            if not raw: 
                continue
            snap = json.loads(raw)
            candidates.append(snap)
        except Exception:
            pass

    # 2) fallback: any device with freshest update (light scan by symbol+tf)
    if not candidates:
        try:
            # NOTE: if you index keys elsewhere, use that; SCAN pattern is fine on small sets
            pattern = f"xtl:ohlc:snap:*:{sym_u}:{tfu}"
            cur = 0
            import json
            while True:
                cur, keys = R.scan(cur, match=pattern, count=50)
                for k in keys:
                    try:
                        raw = R.get(k)
                        if raw:
                            candidates.append(json.loads(raw))
                    except Exception:
                        pass
                if cur == 0:
                    break
        except Exception:
            pass

    if not candidates:
        return None, None

    # pick the one with latest closed bar
    def last_closed_ts_ms(snap):
        bars = snap.get("bars") or []
        if not bars:
            return -1
        # pick last truly CLOSED 1-min bar
        for b in reversed(bars):
            t_ms = _ms_from_t(b.get("t_open_ms") or b.get("t"))
            if t_ms is None: 
                continue
            if b.get("complete") is True or (t_ms + TF_MS["M1"] <= now_ms):
                return t_ms
        return -1

    best = max(candidates, key=last_closed_ts_ms)
    return best, (best.get("broker") if isinstance(best, dict) else None)

@router.get("/price/all")
def price_all(
    tf: str = "M1",
    symbols: str = "XAUUSD,EURUSD,USDJPY,GBPUSD,USDCAD,USDCHF",
    device: str | None = Query(None),
    x_device_id: str | None = Header(None, convert_underscores=False),
    user = Depends(require_auth_optional),   # optional auth; prefer user's device when not pinned
):
    tfu = (tf or "M1").upper()                      # display price is from M1; we keep param for future
    syms = [s.strip().upper() for s in (symbols or "").split(",") if s.strip()]
    rows: list[dict] = []
    broker = None

    # 0) resolve which device to use
    user_id = getattr(user, "user_id", None) if user else None
    pinned_device = device or x_device_id or getattr(user, "device_id", None) or getattr(user, "deviceId", None)
    device_used = None

    # 1) build rows
    import time
    now_ms = int(time.time() * 1000)

    for sym_u in syms:
        # strictly use pinned device if provided; otherwise fallback to your existing helper
        if pinned_device:
            snap, bmeta = _read_snap_for_device(pinned_device, sym_u, "M1")
            device_used = pinned_device
        else:
            snap, bmeta = _read_freshest_snap_for_user_or_any(user_id, sym_u, "M1")
            # ^ this may pick “any” device; we’ll expose which one below if your helper sets it,
            # otherwise leave device_used None

        if not snap:
            rows.append({"symbol": sym_u, "price": None, "lastTs": None})
            continue

        bars = snap.get("bars") or []
        last = None

        # pick the last CLOSED bar (complete==True OR elapsed >= 60s)
        for bbar in reversed(bars):
            t_ms = _ms_from_t(bbar.get("t_open_ms") or bbar.get("t"))
            if t_ms is None:
                continue
            if bbar.get("complete") is True or (t_ms + TF_MS["M1"] <= now_ms):
                last = {**bbar, "t_open_ms": t_ms}
                break

        if last:
            price_c = float(last.get("c")) if last.get("c") is not None else None
            rows.append({
                "symbol": sym_u,
                "price": _fmt_price(sym_u, price_c, bmeta),
                "lastTs": last["t_open_ms"],
            })
            if bmeta and not broker:
                broker = bmeta
        else:
            rows.append({"symbol": sym_u, "price": None, "lastTs": None})

    return {
        "ok": True,
        "tf": tfu,
        "rows": rows,
        "broker": broker or {},
        "device": device_used or (pinned_device or "auto")  # helpful for debugging
    }

def _uid_from_request(request: Request) -> str | None:
    # Try session/JWT helpers already used elsewhere in this file
    try:
        # If you already have a helper, prefer that:
        #   return get_user_id(request)
        # Fallback to cookie ? map ? UUID
        ukey = (request.cookies.get("uid") or request.cookies.get("session_user") or "").strip()
        if ukey:
            return _resolve_user_id(ukey)
    except Exception:
        pass
    return None

def _read_user_snap(uid: str, sym: str, tfu: str):
    key = f"xtl:trend:snap:{uid}:{sym}:{tfu}"
    raw = R.get(key)
    if not raw:
        return None, None
    try:
        js = json.loads(raw)
        bars = js.get("bars") or []
        if not bars:
            return None, None
        last = bars[-1]
        price = float(last.get("c", 0.0))
        t_s   = int(last.get("t", 0))
        t_ms  = (t_s * 1000) if t_s < 10_000_000_000 else t_s  # sec or ms
        return {"price": price, "t_ms": t_ms}, js.get("broker")
    except Exception:
        return None, None

def _scan_freshest_device_snap(sym: str, tfu: str):
    # look across all devices; choose snapshot with max freshness
    best = None
    best_dev = "-"
    best_fresh = -1
    cursor = 0
    pattern = f"xtl:ohlc:snap:dev_*:{sym}:{tfu}"
    while True:
        cursor, keys = R.scan(cursor, match=pattern, count=200)
        for k in keys:
            raw = R.get(k)
            if not raw:
                continue
            try:
                js = json.loads(raw)
            except Exception:
                continue
            bars = js.get("bars") or []
            if not bars:
                continue
            last = bars[-1]
            price = float(last.get("c", 0.0))
            t_s   = int(last.get("t", 0))
            t_ms  = (t_s * 1000) if t_s < 10_000_000_000 else t_s
            # freshness: prefer serverNow/lastClosedTs if present
            fresh = max(int(js.get("serverNow") or 0), int(js.get("lastClosedTs") or 0), t_ms)
            if fresh > best_fresh:
                best_fresh = fresh
                best = {"price": price, "t_ms": t_ms}
                # k = xtl:ohlc:snap:dev_<id>:<sym>:<tf>
                parts = k.split(":")
                if len(parts) >= 3:
                    best_dev = parts[2]  # dev_<id>
        if cursor == 0:
            break
    return best, best_dev

@router.get("/predict/all")
def predict_all(
    tf: str = "M15",
    symbols: str = "XAUUSD,EURUSD,USDJPY,GBPUSD,USDCAD,USDCHF",
    device: str | None = Query(None),
    x_device_id: str | None = Header(None, convert_underscores=False),
    user = Depends(require_auth_optional),
):
    tfu = (tf or "M15").upper()
    syms = [s.strip().upper() for s in (symbols or "").split(",") if s.strip()]

    # prefer user's device if present; fall back to freshest-any
    user_id = getattr(user, "user_id", None) if user else None
    load_models_if_needed()
    if REG_MODEL is None or CLS_MODEL is None:
        return {"ok": False, "reason": "model_not_loaded"}

    rows = []
    for sym in syms:
        snap, broker = _read_freshest_snap_for_user_or_any(user_id, sym, tfu)
        if not snap:
            rows.append({"symbol": sym, "label": "Neutral", "score": 0.0, "reason": "no_data"})
            continue

        bars = snap.get("bars") or []
        if not bars:
            rows.append({"symbol": sym, "label": "Neutral", "score": 0.0, "reason": "empty_bars"})
            continue

        # Build close/high/low arrays from CLOSED bars only
        closes, highs, lows = [], [], []
        now_ms = int(time.time() * 1000)
        off_min = int(((broker or {}).get("tz_offset_min")) or 0)

        m15_next_ms = _next_boundary_ms(15*60, now_ms, off_min)   # UI: next 15m close (“Next bar in”)
        h1_next_ms  = _next_boundary_ms(60*60, now_ms, off_min)   # Horizon: next H1 close
        best, best_dev = _scan_freshest_device_snap(sym, tfu)   # returns {'price', 't_ms'}, 'dev_<id>'
        last_closed_ms = m15_next_ms - (15 * 60 * 1000)     

        TF_MS = {"M15": 15*60*1000, "H1": 60*60*1000, "H4": 4*60*60*1000}
        tf_ms = TF_MS.get(tfu, 60*60*1000)

        for b in bars:
            t_ms = _ms_from_t(b.get("t_open_ms") or b.get("t"))
            if t_ms is None:
                continue
            is_closed = (b.get("complete") is True) or (t_ms + tf_ms <= now_ms)
            if not is_closed:
                continue
            closes.append(float(b.get("c", 0.0)))
            highs.append(float(b.get("h", 0.0)))
            lows.append(float(b.get("l", 0.0)))

        if len(closes) < 20:
            rows.append({"symbol": sym, "label": "Neutral", "score": 0.0, "reason": "insufficient_bars"})
            continue

        # Use your existing detection params + logic
        params = DetectParams(
            ma=MAParams(fast=10 if tfu == "M15" else 20, slow=20 if tfu == "M15" else 50, type="ema"),
            slope=SlopeParams(period=10 if tfu == "M15" else 20, threshold=0.30),
            structure=StructureParams(atrMult=1.5, zigzagPct=0.6),
            strength=StrengthParams(adxMin=20, lookback=14, useDIbias=True),
        )
        label, score, extra = compute_label_and_score(closes, highs, lows, params)

        
        # --- NEW: get p_up via preloaded models; fallback to infer_rt; then to rule score ---
        # --- Use preloaded Booster models for p_up (and optional 1h target move) ---
        p_up = None
        move_pct = None
        feat_rvol15 = None
        feat_usd_basket = None

        try:
           load_models_if_needed()
           if CLS_MODEL is not None and REG_MODEL is not None:
               # Build features exactly as before
               from api.trend.infer_rt import pull_latest_m15, compute_usd_basket, build_features_m15

               need_syms = ["XAUUSD","EURUSD","GBPUSD","AUDUSD","USDJPY","USDCHF","USDCAD"]
               now_frames = {s: pull_latest_m15(s) for s in need_syms}
               df_sym = now_frames.get(sym)

               if df_sym is not None and not df_sym.empty and len(df_sym) >= 8:
                   basket = compute_usd_basket(now_frames)
                   X = build_features_m15(df_sym, basket).astype("float32")
                   
                   try:
                       row_feat = X.iloc[0]
                       if isinstance(extra, dict):
                           extra.setdefault("feat_rvol15", float(row_feat.get("rvol15", 0.0)))
                           extra.setdefault("feat_ret_15m", float(row_feat.get("ret_15m", 0.0)))
                           extra.setdefault("feat_usd_basket", float(row_feat.get("usd_basket_d1h_pct", 0.0)))
                   except Exception:
                       pass

                   # DMatrix for Booster prediction
                   dmat = xgb.DMatrix(X)


                   # Classifier: probability of class 1
                   probs = CLS_MODEL.predict(dmat)  # shape (n,) for binary:logistic or (n, k) for softprob
                   if probs.ndim == 1:
                       p_up = float(probs[0])
                   else:
                       # softprob: take prob of positive class (index 1)
                       p_up = float(probs[0, 1])

                   # Regressor: signed percent move (e.g., +0.40)
                   pred = REG_MODEL.predict(dmat)
                   move_pct = float(pred[0])
                   


        except Exception:
            p_up = None
            move_pct = None

        if p_up is None:
            # Fallback: infer_rt or rule-based
            try:
               from api.trend.infer_rt import predict_next_hour
               pr = predict_next_hour(sym)
               if isinstance(pr, dict):
                   if pr.get("p_up") is not None:
                       p_up = float(pr["p_up"])
                   if pr.get("move_pct") is not None:
                       move_pct = float(pr["move_pct"])
            except Exception:
                p_up = None
                move_pct = None

        if p_up is None:
            p_up = 0.5 * (score + 1.0)

        # (optional) quick ATR estimate for target sizing (existing policy sizing)
        atr_val = None
        try:
            lb = max(5, params.strength.lookback)
            _atr_vals = atr(highs, lows, closes, lb)
            atr_val = float(_atr_vals[-1]) if _atr_vals is not None and len(_atr_vals) else None
        except Exception:
            atr_val = None

        # Existing policy decision (direction/size based on prob & ATR)
        decision, target_pips, confidence = _policy_decision(sym, p_up, atr_val)
        # --- Derive display fields for 1h target ---
        # --- Derive display fields for 1h target (single source of truth) ---
        def _price_decimals(sym: str) -> int:
            s = sym.upper()
            if s.endswith("JPY"):
                return 3
            if s == "XAUUSD":
                return 2
            return 5

        def _pip(sym_: str) -> float:
            s = sym_.upper()
            if s == "XAUUSD":
                return 0.1
            if s.endswith("JPY"):
                return 0.01
            return 0.0001

        last_px = float(closes[-1]) if closes else None
        decimals = _price_decimals(sym)

        
        # reasons / extras
               
        row_extra: Dict[str, Any] = {}

        if isinstance(extra, dict):
            # Merge raw extras + any pre-existing reasons
            merged_for_reasons = dict(extra or {})

            # If caller already attached reasons / reason, preserve them as base
            base_reasons: List[str] = []
            if extra.get("reasons"):
                base_reasons = list(extra["reasons"])
            elif extra.get("reason"):
                base_reasons = [extra["reason"]]

            if base_reasons:
                merged_for_reasons.setdefault("base_reasons", base_reasons)

            # Single call to reason engine (trend + RVOL + USD basket + prob)
            reasons = _build_reasons(sym, label, p_up, merged_for_reasons)
            if reasons:
                row_extra["reasons"] = reasons
            elif base_reasons:
                row_extra["reasons"] = base_reasons

        # Always provide a list (UI expects array)
        if "reasons" not in row_extra:
            row_extra["reasons"] = []


        # -------- unified expected_move_pct_1h + target_price_1h ----------
        final_pct = None   # percent units, signed (e.g. +0.04 means +0.04%)
        final_target = None

        
        # 1) Prefer regressor output (normalize, align sign with label, clamp) + logging
        if isinstance(move_pct, (int, float)) and isinstance(last_px, (int, float)):
            raw = float(move_pct)            # raw model output (fraction or percent)
            mlt = _calib_multiplier(sym)     # ENV-based scaler (CALIB_X or CALIB_<SYM>_X)
            scaled = raw * mlt               # apply calibration (no-op if 1.0)

            # DEBUG/telemetry: write lightweight CSV row (ts,sym,tf,raw,mlt)
            try:
                # journalctl-friendly
                log.info("reg_raw sym=%s tf=%s raw=%.8f mul=%.4f", sym, tfu, raw, mlt)
                PRED_RAW_LOG.parent.mkdir(parents=True, exist_ok=True)
                with PRED_RAW_LOG.open("a", buffering=1) as f:
                   f.write(f"{int(time.time()*1000)},{sym},{tfu},raw,{raw:.8f},{mlt:.4f}\n")
            except Exception:
                pass

            # Convert to PERCENT units (handles fraction->percent automatically)
            pct_n = _normalize_pct(sym, scaled)
            if isinstance(pct_n, (int, float)):
                lbl = (label or "").lower()

                # Align sign with classifier label (direction)
                if lbl == "bearish" and pct_n > 0:
                    pct_n = -pct_n
                elif lbl == "bullish" and pct_n < 0:
                    pct_n = abs(pct_n)

                # Clamp outliers
                pct_n = max(min(pct_n, 5.0), -5.0)

                # Final write
                final_pct = float(pct_n)
                final_target = round(float(last_px) * (1.0 + final_pct / 100.0), decimals)

                # Debug after normalization & alignment
                try:
                   log.info(
                      "reg_norm sym=%s tf=%s pct=%.6f last=%.6f target=%s label=%s",
                      sym, tfu, final_pct, float(last_px), str(final_target), lbl
                   )
                   with PRED_RAW_LOG.open("a", buffering=1) as f:
                      f.write(
                          f"{int(time.time()*1000)},{sym},{tfu},norm,{final_pct:.6f},"
                          f"{float(last_px):.6f},{final_target}\n"
                      )
                except Exception:
                   pass

            
        # 2) Fallback: derive from policy pips when regressor missing
        if final_pct is None and isinstance(target_pips, (int, float)) and isinstance(last_px, (int, float)):
            pip = _pip(sym)
            abs_raw = abs(float(target_pips))
            S = sym.upper()
            # interpret policy value (your code stored: JPY/majors as price delta; XAU as pip-count)
            if S == "XAUUSD":
                delta_abs = abs_raw * pip         # pip-count ? price delta
            elif S.endswith("JPY"):
                delta_abs = abs_raw                # already price delta
            else:
                delta_abs = abs_raw                # majors: already price delta

            # sign from decision
            delta = delta_abs if str(decision).upper() == "BUY" else -delta_abs
            final_target = round(float(last_px) + delta, decimals)
            final_pct = (delta / float(last_px)) * 100.0

        # 3) Write fields if available
        if isinstance(final_pct, (int, float)):
            row_extra["expected_move_pct_1h"] = float(_round_pct(sym, final_pct))
        if isinstance(final_target, (int, float)):
            row_extra["target_price_1h"] = float(final_target)


        row = {
            "symbol": sym,
            "label": label,
            "score": score,
            "p_up": round(float(p_up), 4) if p_up is not None else None,
            "prob_up": round(float(p_up), 4) if p_up is not None else None,
            "prob_up_1h": round(float(p_up), 4) if p_up is not None else None,
            "decision": decision,
            "confidence": confidence,
            # keep policy target sizing (as before)
            "target_pips": None if target_pips is None else round(float(target_pips), 6),
            # add regression outputs (UI can use directly)
            "expected_move_pct_1h": row_extra.get("expected_move_pct_1h"),
            "target_price_1h": row_extra.get("target_price_1h"),
            **row_extra,
            # timing/meta
            "update_tf": "M15",
            "next_update_ts": m15_next_ms,
            "next_update_eta_ms": max(0, m15_next_ms - now_ms),
            "horizon": "H1",
            "target_close_ts": h1_next_ms,
            "eta_ms": max(0, h1_next_ms - now_ms),
            "server_now_ms": now_ms,
            "device_id": best_dev,
            "updated_broker_ms": int(last_closed_ms),

        }

        # attach timing/meta for countdowns and horizon
        row.update({
            "update_tf": "M15",
            "next_update_ts": m15_next_ms,
            "next_update_eta_ms": max(0, m15_next_ms - now_ms),

            "horizon": "H1",
            "target_close_ts": h1_next_ms,
            "eta_ms": max(0, h1_next_ms - now_ms),

            "server_now_ms": now_ms,
        })
        # --- DIAGNOSTIC: one line per symbol so you can tail logs ---
        log.info(
            "predict[m15] sym=%s p_up=%.3f move_pct=%s decision=%s target_px=%s ver=%s last_close=%.5f",
            sym,
            float(row["p_up"]),
            ("%.2f" % row["expected_move_pct_1h"]) if row.get("expected_move_pct_1h") is not None else "None",
            row.get("decision"),
            ( "%.5f" % row["target_price_1h"] ) if row.get("target_price_1h") is not None else "None",
            MODEL_VERSION,
            float(closes[-1]) if closes else float("nan"),
        )
        # append to CSV log
        try:
            _log_prediction(row, closes[-1] if closes else None)
        except Exception:
            pass



        rows.append(row)

   

    
    now_ms = int(_time.time() * 1000)

    # pick a safe boundary for UI refresh:
    # 1) prefer the earliest per-row next_update_ts we already computed
    # 2) fallback: snap to next 15-minute boundary from server clock (UTC)
    TF_MS = 15 * 60 * 1000
    try:
       next_update_ts = min(
           int(r.get("next_update_ts"))
           for r in rows
           if isinstance(r.get("next_update_ts"), (int, float)) and r.get("next_update_ts") > 0
       )
    except Exception:
        
        next_update_ts = ((now_ms // TF_MS) + 1) * TF_MS

    # UI should wake a hair after the boundary; clamp to [2s, 60s]
    poll_after_ms = max(2_000, min(60_000, int(next_update_ts - now_ms + 500)))
    
    # --- Compute 'computed_at' (when this prediction snapshot was prepared) ---
    # Prefer a per-row timestamp if present; else use server 'now'
    computed_at = None
    try:
       computed_at = next(
          int(r["server_now_ms"])
          for r in rows
          if isinstance(r.get("server_now_ms"), (int, float))
       )
    except Exception:
       computed_at = int(_time.time() * 1000)
    # summarize timing/meta for top-level hints
    poll_after_ms = min((int(r.get("next_update_eta_ms", 0)) for r in rows), default=0)
    next_update_ts = min((int(r.get("next_update_ts", 0)) for r in rows), default=0)
    computed_at = next(
        (int(r["server_now_ms"]) for r in rows if isinstance(r.get("server_now_ms"), (int, float))),
        int(_time.time() * 1000)
    )

    return {
        "ok": True,
        "tf": tfu,
        "rows": rows,
        "next_update_ts": int(next_update_ts),
        "poll_after_ms": int(poll_after_ms),
        "model_version": MODEL_VERSION,
        "computed_at": int(computed_at),
    }



@router.get("/predict/health")
def predict_health():
    load_models_if_needed()
    reg_ok = REG_MODEL is not None
    cls_ok = CLS_MODEL is not None
    from pathlib import Path
    return {
        "ok": bool(reg_ok and cls_ok),
        "classifier_loaded": cls_ok,
        "regressor_loaded": reg_ok,
        "model_version": MODEL_VERSION,
        "reg_path_exists": Path(str(REG_PATH)).exists(),
        "cls_path_exists": Path(str(CLS_PATH)).exists(),
    }

def _broker_bars_sync(symbol: str, tf: str, limit: int = 300, price: str = "bid") -> list[dict]:
    """
    Pull bars straight from the agent (BID by default) using a sync client.
    Returns [{"t": epoch_sec, "o":..., "h":..., "l":..., "c":...}, ...]
    """
    import os, httpx
    base = (os.getenv("AGENT_BASE_URL", "") or "").rstrip("/")
    if not base:
        return []
    candidates = ("/broker/ohlc", "/ohlc", "/api/ohlc")
    insecure = base.startswith("https://127.0.0.1") or base.startswith("https://localhost")
    try:
        with httpx.Client(timeout=10, verify=(False if insecure else True)) as cli:
            for path in candidates:
                try:
                    r = cli.get(base + path, params={
                        "symbol": symbol, "tf": tf, "limit": limit, "price": price
                    })
                    if r.status_code != 200:
                        continue
                    js = r.json()
                    if isinstance(js, list):
                        return js
                    if isinstance(js, dict) and isinstance(js.get("bars"), list):
                        return js["bars"]
                except Exception:
                    continue
    except Exception:
        pass
    return []


def load_snapshot(user_id: str, symbol: str, tf: Literal["M15","H1","H4"]) -> Optional[dict]:
    key = f"xtl:trend:snap:{str(user_id)}:{str(symbol).upper()}:{str(tf).upper()}"
    try:
        raw = R.get(key)
        return json.loads(raw) if raw else None
    except Exception:
        return None
# --- helper: get broker tz from snapshot OR user device registry, with env fallback ---
from typing import Optional  # ensure this import exists at top

# --- guarantee tz fields even if snapshot/device is missing or malformed ---


def _safe_broker_meta(b: dict | None) -> dict:
    """
    Return a minimal, safe broker meta dict.
    - Never override a valid device/snapshot offset with env; env only fills missing offset.
    - Clamp tz_offset_min to sane range.
    - Synthesize tz_name from offset when missing.
    - Pass through optional fields (price_basis, phase, digits) if present.
    """
    b = b or {}
    name = b.get("tz_name") or None

    # parse offset if present
    off = None
    try:
        if b.get("tz_offset_min") is not None:
            off = int(b["tz_offset_min"])
    except Exception:
        off = None

    # if offset still missing, allow env as a *fallback* (do not override existing)
    if off is None:
        env_off = os.getenv("FORCE_TZ_OFFSET_MIN")
        try:
            off = int(env_off) if env_off not in (None, "") else None
        except Exception:
            off = None

    # clamp to [-12h, +14h] in minutes
    if off is not None:
        off = max(-720, min(840, off))

    # synthesize tz_name if missing but offset known
    if not name and off is not None:
        sign = "+" if off >= 0 else "-"
        m = abs(off)
        name = f"UTC{sign}{m // 60:02d}:{m % 60:02d}"

    out = {}
    if name:
        out["tz_name"] = name
    if off is not None:
        out["tz_offset_min"] = off

    # pass-through optional fields without mutating semantics
    for k in ("price_basis", "phase", "digits"):
        if k in b:
            out[k] = b[k]
    return out

    
def _load_broker_meta(uid: str, snap_broker: dict | None) -> Optional["BrokerMeta"]:
    """
    Decide which broker tz meta to use.
    Priority (device-first):
      1) the user's most-recent device (by last_heartbeat)
      2) broker from the snapshot
      3) None
    """
    def _to_int_or_none(x):
        try:
            if x is None: return None
            if isinstance(x, (bytes, bytearray)): x = x.decode(errors="ignore")
            x = str(x).strip()
            if not x: return None
            return int(float(x))
        except Exception:
            return None

    def _clamp_offset(mins: Optional[int]) -> Optional[int]:
        if mins is None: return None
        return max(-720, min(840, mins))  # [-12h, +14h]

    # 1) Device (most recent heartbeat wins)
    try:
        devs = list(R.smembers(f"xtl:user:{uid}:devices") or [])
        if devs:
            prefix_env = (os.getenv("XTL_DEVICE_KEY_PREFIX", "") or "").strip()

            def _read_dev(dev_id: bytes | str):
                did = dev_id.decode() if isinstance(dev_id, (bytes, bytearray)) else dev_id
                meta = {}
                for pref in ([prefix_env] if prefix_env else []) + ["devices:", "device:"]:
                    try:
                        m = R.hgetall(f"{pref}{did}") or {}
                    except Exception:
                        m = {}
                    if m:
                        meta = m
                        break

                def _get(field: str) -> str:
                    v = meta.get(field) or meta.get(field.encode()) or b""
                    if isinstance(v, (bytes, bytearray)):
                        v = v.decode(errors="ignore")
                    return (v or "").strip()

                # last_heartbeat can be ms, sec, or ISO
                hb = _get("last_heartbeat")
                hb_ms = 0
                if hb:
                    try:
                        f = float(hb)
                        hb_ms = int(f if f > 1e12 else f * 1000.0)
                    except Exception:
                        try:
                            from datetime import datetime
                            hb_ms = int(datetime.fromisoformat(hb.replace("Z", "")).timestamp() * 1000)
                        except Exception:
                            hb_ms = 0

                tz_name = _get("Broker.TzName") or _get("broker_tz_name") or None
                off_raw = _get("Broker.TzOffsetMin") or _get("broker_tz_offset_min")
                tz_off = _to_int_or_none(off_raw)

                return hb_ms, tz_name, tz_off

            best = max((_read_dev(d) for d in devs), key=lambda t: t[0], default=(0, None, None))
            _, tz_name, tz_off = best
            if tz_name or tz_off is not None:
                return BrokerMeta(tz_name=tz_name, tz_offset_min=_clamp_offset(tz_off))
    except Exception:
        pass

    # 2) Snapshot fallback
    try:
        if isinstance(snap_broker, dict):
            off = _clamp_offset(_to_int_or_none(snap_broker.get("tz_offset_min")))
            name = (snap_broker.get("tz_name") or "").strip() or None
            if (off is not None) or name:
                return BrokerMeta(
                    tz_name=name,
                    tz_offset_min=off,
                    price_basis=snap_broker.get("price_basis"),
                    phase=snap_broker.get("phase"),
                    digits=_to_int_or_none(snap_broker.get("digits")),
                )
    except Exception:
        pass

    # 3) Nothing
    return None


class PreviewBar(BaseModel):
    t_open_ms: int   # broker bar OPEN time (ms since epoch, UTC)
    t_close_ms: int  # broker bar CLOSE time (ms since epoch, UTC)
    o: float
    h: float
    l: float
    c: float

class PreviewPayload(BaseModel):
    symbol: str
    tf: str
    bars: List[PreviewBar] = []
    lastClosedTs: Optional[int] = None  # ms
    probe: Optional[dict] = None  

class BrokerMeta(BaseModel):
    price_basis: Optional[str] = None
    phase: Optional[dict] = None
    tz_name: Optional[str] = None         
    tz_offset_min: Optional[int] = None 


class DetectResp(BaseModel):
    label: str
    score: float
    serverNow: int
    lastClosedTs: int
    nextCloseTs: int
    diagnostics: dict
    stale: bool
    preview: Optional[PreviewPayload] = None
    broker: Optional[BrokerMeta] = None
    adx: Optional[float] = None
    slope: Optional[float] = None
    structure: Optional[str] = None
    pollAfterMs: Optional[int] = None
    usingDevice: Optional[str] = None

class MAParams(BaseModel):
    fast: int = Field(50, ge=2, le=500)
    slow: int = Field(200, ge=2, le=1000)
    type: Literal["ema", "sma"] = "ema"

class SlopeParams(BaseModel):
    period: int = Field(20, ge=2, le=200)
    threshold: float = Field(0.30)  # percent; e.g. 0.30 = 0.30%

class StructureParams(BaseModel):
    atrMult: float = Field(1.5, ge=0.1, le=5.0)
    zigzagPct: float = Field(0.6, ge=0.1, le=10.0)  # min swing %

class StrengthParams(BaseModel):
    adxMin: int = Field(20, ge=5, le=60)
    lookback: int = Field(14, ge=5, le=50)
    useDIbias: bool = True

class DetectParams(BaseModel):
    ma: MAParams
    slope: SlopeParams
    structure: StructureParams
    strength: StrengthParams

    @validator("ma")
    def _clamp_ma(cls, v: MAParams) -> MAParams:
        # enforce fast < slow (auto-bump slow if needed)
        if v.fast >= v.slow:
            v = MAParams(fast=v.fast, slow=max(v.fast + 1, v.slow + 1), type=v.type)
        return v

class DetectReq(BaseModel):
    symbol: str
    tf: Literal["M15","H1", "H4"]
    params: DetectParams



# ------------------------------------------------------------------------------
# Utilities: indicators (no third-party deps)
# ------------------------------------------------------------------------------

def ema(series: List[float], period: int) -> List[float]:
    if period <= 1 or not series:
        return series[:]
    k = 2.0 / (period + 1.0)
    out: List[float] = []
    s = series[0]
    out.append(s)
    for x in series[1:]:
        s = x * k + s * (1.0 - k)
        out.append(s)
    return out

def sma(series: List[float], period: int) -> List[float]:
    out: List[float] = []
    s = 0.0
    q: List[float] = []
    for x in series:
        q.append(x)
        s += x
        if len(q) > period:
            s -= q.pop(0)
        out.append(s / len(q))
    return out

def true_range(h: List[float], l: List[float], c: List[float]) -> List[float]:
    tr: List[float] = []
    prev_c = c[0]
    for i in range(len(c)):
        cur_h, cur_l = h[i], l[i]
        tr.append(max(cur_h - cur_l, abs(cur_h - prev_c), abs(cur_l - prev_c)))
        prev_c = c[i]
    return tr

def atr(h: List[float], l: List[float], c: List[float], period: int) -> List[float]:
    tr = true_range(h, l, c)
    if period <= 1:
        return tr
    # Wilder smoothing
    out: List[float] = []
    s = sum(tr[:period]) / float(period)
    out.extend([s] * period)  # seed
    alpha = 1.0 / period
    for x in tr[period:]:
        s = (s * (period - 1) + x) * alpha
        out.append(s)
    # Ensure lengths match
    while len(out) < len(c):
        out.append(out[-1])
    return out

def adx(h: List[float], l: List[float], c: List[float], period: int) -> List[float]:
    # +DM / -DM / TR
    plus_dm: List[float] = [0.0]
    minus_dm: List[float] = [0.0]
    tr = [0.0]
    for i in range(1, len(c)):
        up = h[i] - h[i-1]
        dn = l[i-1] - l[i]
        p_dm = up if (up > dn and up > 0) else 0.0
        m_dm = dn if (dn > up and dn > 0) else 0.0
        plus_dm.append(p_dm)
        minus_dm.append(m_dm)
        tr.append(max(h[i]-l[i], abs(h[i]-c[i-1]), abs(l[i]-c[i-1])))

    # Wilder smoothing
    def wilder(seq: List[float], p: int) -> List[float]:
        if p <= 1 or not seq:
            return seq[:]
        out: List[float] = []
        s = sum(seq[:p])
        out.extend([s] + [0.0] * (p - 1))  # seed at index p-1, keep length alignment
        alpha = 1.0 / p
        for x in seq[p:]:
            s = (s - (s * (1 - alpha))) + x  # equivalent to s = s*(p-1)/p + x
            out.append(s)
        while len(out) < len(seq):
            out.append(out[-1] if out else 0.0)
        return out

    pDM = wilder(plus_dm, period)
    mDM = wilder(minus_dm, period)
    TRs = wilder(tr, period)

    pDI: List[float] = []
    mDI: List[float] = []
    for pdm, mdm, t in zip(pDM, mDM, TRs):
        if t <= 1e-12:
            pDI.append(0.0); mDI.append(0.0)
        else:
            pDI.append(100.0 * (pdm / t))
            mDI.append(100.0 * (mdm / t))

    dx: List[float] = []
    for p, m in zip(pDI, mDI):
        s = p + m
        dx.append(0.0 if s == 0.0 else 100.0 * abs(p - m) / s)

    # ADX = Wilder smoothing of DX
    adx_vals = []
    if period < len(dx):
        seed = sum(dx[:period]) / period
        adx_vals.extend([seed] * period)
        for x in dx[period:]:
            seed = ((seed * (period - 1)) + x) / period
            adx_vals.append(seed)
    else:
        adx_vals = dx
    while len(adx_vals) < len(c):
        adx_vals.append(adx_vals[-1] if adx_vals else 0.0)
    return adx_vals

def _normalize_ohlc(rows):
    """
    rows: iterable of dicts with keys t(o,h,l,c) and optional complete.
    Returns ascending-by-time normalized list.
    """
    out = []
    for b in rows or []:
        try:
            # accept either ms or sec; normalize to **seconds**
            t_raw = int(b.get("t", 0))
            # if >= 1e13 it's almost certainly milliseconds -> convert to seconds
            t_sec = t_raw // 1000 if t_raw > 10_000_000_000 else t_raw
            out.append({
                "t": int(t_sec),  # epoch seconds (bar OPEN)
                "o": float(b["o"]),
                "h": float(b["h"]),
                "l": float(b["l"]),
                "c": float(b["c"]),
                "complete": bool(b.get("complete", True)),
            })
        except Exception:
            continue
    out.sort(key=lambda r: r["t"])
    return out


def zigzag_pivots(c: List[float], pct: float) -> List[int]:
    """Simple percent ZigZag pivot indexes. pct in % (e.g. 0.6)."""
    if not c:
        return []
    thresh = (pct / 100.0) if pct > 1e-9 else 0.006  # fallback 0.6%
    pivots: List[int] = [0]
    last_p = 0
    last_ext = c[0]
    direction = 0  # 1 up, -1 down, 0 unknown
    for i in range(1, len(c)):
        change = (c[i] - last_ext) / last_ext if last_ext else 0.0
        if direction >= 0:  # seeking up move
            if change >= thresh:
                direction = 1
                pivots.append(i); last_ext = c[i]; last_p = i
            elif change <= -thresh and direction == 1:
                direction = -1
                pivots.append(i); last_ext = c[i]; last_p = i
            else:
                if (direction == 1 and c[i] > last_ext) or (direction <= 0 and c[i] < last_ext):
                    last_ext = c[i]
        else:  # seeking down move
            if change <= -thresh:
                direction = -1
                pivots.append(i); last_ext = c[i]; last_p = i
            elif change >= thresh and direction == -1:
                direction = 1
                pivots.append(i); last_ext = c[i]; last_p = i
            else:
                if (direction == -1 and c[i] < last_ext) or (direction >= 0 and c[i] > last_ext):
                    last_ext = c[i]
    if pivots[-1] != len(c) - 1:
        pivots.append(len(c) - 1)
    return sorted(set(pivots))


# ------------------------------------------------------------------------------
# Snapshot access (replace with your real store)
# ------------------------------------------------------------------------------




# ------------------------------------------------------------------------------
# Core detection logic
# ------------------------------------------------------------------------------

def compute_label_and_score(
    closes: List[float],
    highs: List[float],
    lows: List[float],
    params: DetectParams
) -> Tuple[str, float, dict]:
    n = len(closes)
    req = max(params.ma.slow + 5, params.slope.period + 5, params.strength.lookback + 5)
    if n < req:
        # Always return a tuple so the caller can unpack safely
        return "Neutral", 0.0, {"reason": f"insufficient_bars:{n}<{req}"}

    # --- Moving averages (use the module-level ema/sma already defined) ---
    ma_type = (params.ma.type or "ema").lower()
    fast_p = int(params.ma.fast)
    slow_p = int(params.ma.slow)

    if ma_type == "sma":
        fastMA = sma(closes, fast_p)
        slowMA = sma(closes, slow_p)
    else:
        fastMA = ema(closes, fast_p)
        slowMA = ema(closes, slow_p)

    # Sanity guard: lengths must match
    m = min(len(fastMA), len(slowMA), n)
    if m == 0:
        return "Neutral", 0.0, {"reason": "ma_empty"}

    # Trim to common length if needed
    if len(fastMA) != m: fastMA = fastMA[-m:]
    if len(slowMA) != m: slowMA = slowMA[-m:]
    if len(closes)  != m:
        closes = closes[-m:]; highs = highs[-m:]; lows = lows[-m:]

    # --- Slope precompute (uses fastMA) ---
    sp = max(2, min(params.slope.period, m - 2))
    prev = fastMA[-1 - sp] if m > sp else fastMA[0]
    slope_pct = 0.0 if prev == 0 else (fastMA[-1] - prev) / prev * 100.0

    # --- ATR + ADX precompute ---
    lb = max(5, params.strength.lookback)
    _atr = atr(highs, lows, closes, lb)
    _adx = adx(highs, lows, closes, lb)


    # Direction
    bull_dir = fastMA[-1] > slowMA[-1]
    bear_dir = fastMA[-1] < slowMA[-1]
    base = (1.0 if bull_dir else (-1.0 if bear_dir else 0.0))
    # Slope (% over period) using fast MA (fallback if window too small)
    sp = max(2, min(params.slope.period, n - 2))
    prev = fastMA[-1 - sp] if n > sp else fastMA[0]
    slope_pct = 0.0 if prev == 0 else (fastMA[-1] - prev) / prev * 100.0
    thr = params.slope.threshold if params.slope.threshold > 1e-6 else params.slope.threshold * 100.0
    slope_ok_bull = slope_pct >= thr
    slope_ok_bear = slope_pct <= -thr

    # --- ATR + ADX (with optional DI gating) ---
    lb = max(5, params.strength.lookback)
    _atr = atr(highs, lows, closes, lb)
    _adx = adx(highs, lows, closes, lb)
    adx_ok = _adx[-1] >= params.strength.adxMin

    # Compute latest DI bias (+1 bull, -1 bear, 0 tie)
    plus_dm = [0.0]; minus_dm = [0.0]; TR = [0.0]
    for i in range(1, n):
        up = highs[i] - highs[i-1]
        dn = lows[i-1] - lows[i]
        plus_dm.append(up if (up > dn and up > 0) else 0.0)
        minus_dm.append(dn if (dn > up and dn > 0) else 0.0)
        TR.append(max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1])))

    def _wilder_last(seq, p):
        s = sum(seq[:p])
        if len(seq) <= p:
            return s
        alpha = 1.0 / p
        for x in seq[p:]:
            s = (s * (p - 1) + x) * alpha
        return s

    sTR = _wilder_last(TR, lb)
    sPD = _wilder_last(plus_dm, lb)
    sMD = _wilder_last(minus_dm, lb)
    if sTR <= 1e-12:
        pDI_last = mDI_last = 0.0
    else:
        pDI_last = 100.0 * (sPD / sTR)
        mDI_last = 100.0 * (sMD / sTR)
    di_bias = 1 if pDI_last > mDI_last else (-1 if mDI_last > pDI_last else 0)

    # ADX term (respect DI bias when enabled)
    if adx_ok:
        if not params.strength.useDIbias:
            adx_s = 1.0 if base != 0 else 0.0
        else:
            adx_s = 1.0 if (base > 0 and di_bias > 0) else (-1.0 if (base < 0 and di_bias < 0) else 0.0)
    else:
        adx_s = 0.0

    # --- ZigZag structure (use last 4 pivots when available) ---
  
    # --- ZigZag structure (use last 4 pivots when available) ---
    piv = zigzag_pivots(closes, params.structure.zigzagPct)

    last_swing_dir = 0          # +1 up, -1 down, 0 unknown
    structure_label = "Consolidation"  # default for mixed case
    struct_sign = 0.0           # raw structure direction before regime tie-in

    if len(piv) >= 2:
        # Fallback: last leg direction from the most recent two pivots
        last_swing_dir = 1 if closes[piv[-1]] > closes[piv[-2]] else -1
        structure_label = "HH/HL" if last_swing_dir > 0 else "LH/LL"
        struct_sign = 1.0 if last_swing_dir > 0 else -1.0

    if len(piv) >= 4:
        # Last four pivots
        i1, i2, i3, i4 = piv[-4], piv[-3], piv[-2], piv[-1]

        # Determine parity of these four pivots from the leg direction between i1->i2.
        # If closes[i2] > closes[i1] the sequence is L,H,L,H; else H,L,H,L.
        seq_is_LHLH = closes[i2] > closes[i1]

        if seq_is_LHLH:
            # i1=L, i2=H, i3=L, i4=H ? compare H4>H2 and L3>L1
            h2, h4 = highs[i2], highs[i4]
            l1, l3 = lows[i1],  lows[i3]
        else:
            # i1=H, i2=L, i3=H, i4=L ? compare H3>H1 and L4>L2 (rename as h4/h2, l3/l1)
            h2, h4 = highs[i1], highs[i3]
            l1, l3 = lows[i2],  lows[i4]

        up   = (h4 > h2) and (l3 > l1)   # HH + HL
        down = (h4 < h2) and (l3 < l1)   # LH + LL

        if up:
            structure_label = "HH/HL"
            struct_sign = 1.0
            last_swing_dir = 1
        elif down:
            structure_label = "LH/LL"
            struct_sign = -1.0
            last_swing_dir = -1
        else:
            structure_label = "Consolidation"
            struct_sign = 0.0
            last_swing_dir = 0

    # Expose exact 4-pivot verdict for UI (HH/HL, LH/LL, or Consolidation)
    structure4 = None
    if len(piv) >= 4:
        i1, i2, i3, i4 = piv[-4], piv[-3], piv[-2], piv[-1]
        seq_is_LHLH = closes[i2] > closes[i1]
        if seq_is_LHLH:
            h2, h4 = highs[i2], highs[i4]
            l1, l3 = lows[i1],  lows[i3]
        else:
            h2, h4 = highs[i1], highs[i3]
            l1, l3 = lows[i2],  lows[i4]
        up   = (h4 > h2) and (l3 > l1)
        down = (h4 < h2) and (l3 < l1)
        structure4 = {
            "p1": float(l1),   # for debugging only
            "p2": float(h2),
            "p3": float(l3),
            "p4": float(h4),
            "up": bool(up),
            "down": bool(down),
            "label": structure_label,
        }




    # Structure pass/fail vs regime (ties structure to current MA bias)
    struct_ok_bull = struct_sign > 0
    struct_ok_bear = struct_sign < 0

    # --- Compose score [-1..+1] (do NOT overwrite adx_s computed above) ---
    slope_s  = 1.0 if (bull_dir and slope_ok_bull) else (-1.0 if (bear_dir and slope_ok_bear) else 0.0)
    struct_s = 1.0 if (bull_dir and struct_ok_bull) else (-1.0 if (bear_dir and struct_ok_bear) else 0.0)

    score = 0.4 * base + 0.3 * slope_s + 0.2 * adx_s + 0.1 * struct_s
    score = max(-1.0, min(1.0, score))

    # --- Map to label ---
    if score >= 0.75:
        label = "Strong Bullish"
    elif score >= 0.25:
        label = "Bullish"
    elif score <= -0.75:
        label = "Strong Bearish"
    elif score <= -0.25:
        label = "Bearish"
    else:
        label = "Neutral"

    diag = {
        "emaFast": fastMA[-200:],
        "emaSlow": slowMA[-200:],
        "adx": _adx[-200:],
        "pivots": piv[-50:],
        "slopePct": round(slope_pct, 3),
        "lastSwingDir": last_swing_dir,
        "structureLabel": structure_label,  # "HH/HL", "LH/LL", or "Consolidation"
        "structure4": structure4,   
    }
    return label, float(round(score, 4)), diag


    




# ------------------------------------------------------------------------------
# Route
# ------------------------------------------------------------------------------
def _epoch_to_ms_any(t: int | float | None) -> int:
    """Normalize epoch t (sec/ms/us/ns) to milliseconds."""
    t = int(t or 0)
    if t >= 1_000_000_000_000_000_000:  # nanoseconds
        return t // 1_000_000
    if t >= 1_000_000_000_000_000:      # microseconds
        return t // 1_000
    if t >= 1_000_000_000_000:          # milliseconds
        return t
    return t * 1000                      # seconds -> ms



def _nudge_agent(user_id: str, sym: str, tfu: str, ttl_sec: int = 45):
    try:
        R.setex(f"xtl:trend:push_now:{user_id}:{sym}:{tfu}", ttl_sec, "1")
    except Exception:
        pass
@router.get("/state2", response_model=DetectResp)
def trend_state2(
    request: Request,
    symbol: str = Query(..., min_length=3),
    tf: Literal["M15", "H1", "H4"] = "H1",
    user_id_override: Optional[str] = Query(None),
    adxPeriod: Optional[int] = Query(None, ge=5, le=50),
    adxMin: Optional[int] = Query(None, ge=5, le=60),
    useDIbias: Optional[bool] = Query(None),
    n: Optional[int] = Query(60, ge=30, le=500),
):
    import os, json, time
    

    # ---------- helpers ----------
    def _to_ms_any(x) -> int:
        """Normalize any epoch-like value to milliseconds without raising."""
        try:
            xi = int(x or 0)
        except Exception:
            return 0
        if xi >= 1_000_000_000_000_000_000:  # ns
            return xi // 1_000_000
        if xi >= 1_000_000_000_000_000:      # µs
            return xi // 1_000
        if xi >= 1_000_000_000_000:          # ms
            return xi
        return xi * 1000 if xi > 0 else 0     # seconds

    TF_MS = {"M15": 15*60*1000, "H1": 60*60*1000, "H4": 4*60*60*1000}

    # breadcrumb: entered
    try:
        R.setex("xtl:debug:state2:entered", 300, "1")
    except Exception:
        pass

    # ---------- resolve user ----------
    allow_hdr = os.getenv("ALLOW_X_USER_KEY", "false").lower() == "true"
    hdr_key = (
        request.headers.get("x-user-key")
        or request.headers.get("X-User-Key")
        or request.headers.get("X_User_Key")
        or request.headers.get("x_user_key")
        if allow_hdr else None
    )
    requested = user_id_override or (str(hdr_key).strip() if hdr_key else get_user_id(request))
    uid = _resolve_user_id(str(requested))

    sym = symbol.upper()
    tfu = tf.upper()
    tf_ms = TF_MS.get(tfu, 60*60*1000)

    key_user = f"xtl:trend:snap:{uid}:{sym}:{tfu}"
    key_last = f"xtl:trend:last:{sym}:{tfu}"

    try:
        R.setex("xtl:debug:state2:last", 300, f"uid={uid} sym={sym} tf={tfu} key={key_user}")
    except Exception:
        pass

    # ---------- broker meta from device registry (to enrich if missing) ----------
    device_broker = {}
    try:
        devs_list = list(R.smembers(f"xtl:user:{uid}:devices") or [])
        prefix = os.getenv("XTL_DEVICE_KEY_PREFIX", "device:")
        for dev_id in devs_list:
            meta = R.hgetall(f"{prefix}{dev_id}") or {}
            tz_name = (meta.get("broker_tz_name") or "").strip()
            off_raw = meta.get("broker_tz_offset_min")
            if isinstance(off_raw, str):
                off_raw = off_raw.strip()
            if tz_name or (off_raw not in (None, "")):
                device_broker = {"tz_name": (tz_name or None)}
                try:
                    device_broker["tz_offset_min"] = int(off_raw) if off_raw not in (None, "") else None
                except Exception:
                    device_broker["tz_offset_min"] = None
                break
    except Exception:
        device_broker = {}
    # --- Build broker meta safely (device > snapshot) ---

    # --- choose device deterministically: prefer sticky, then recent ---
    key_sticky = f"xtl:sticky_device:{uid}:{sym}:{tf}"
    key_recent = f"xtl:last_push_device:{uid}:{sym}:{tf}"

    try:
       dev_from_sticky = R.get(key_sticky)
       dev_from_recent = R.get(key_recent)
       raw = dev_from_sticky or dev_from_recent
       if isinstance(raw, (bytes, bytearray)):
           raw = raw.decode(errors="ignore")
       prefer_dev = (dev_from_sticky or dev_from_recent or b"").decode().strip() or None
    except Exception:
       prefer_dev = None

    # also honor the active device chosen by the Detect button (if present)
    if not prefer_dev:
        try:
           prefer_dev = R.get(f"xtl:user:active_device:{uid}:{sym}")
           if isinstance(prefer_dev, (bytes, bytearray)):
               prefer_dev = prefer_dev.decode().strip() or None
        except Exception:
           prefer_dev = None

    # --- load broker meta from the chosen device if available (covers registry->Redis mirror) ---
    device_broker = None
    if prefer_dev:
        for hk in (
            f"device:{prefer_dev}:broker_meta",   # new style
            f"devices:{prefer_dev}",              # legacy plural
            f"device:{prefer_dev}",               # flat device hash (registry mirror)
        ):
            try:
                m = R.hgetall(hk) or {}
            except Exception:
                m = {}
            if not m:
                continue

            tz_name = (m.get("Broker.TzName") or m.get("broker_tz_name") or "")
            if isinstance(tz_name, (bytes, bytearray)):
                tz_name = tz_name.decode(errors="ignore")
            tz_name = tz_name.strip() or None

            off_raw = (m.get("Broker.TzOffsetMin") or m.get("broker_tz_offset_min"))
            if isinstance(off_raw, (bytes, bytearray)):
                off_raw = off_raw.decode(errors="ignore")
            try:
                off = int(off_raw) if off_raw not in (None, "") else None
            except Exception:
                off = None

            device_broker = {"tz_name": tz_name, "tz_offset_min": off}
            break

    



    # ---------- 1) try user snapshot ----------
    raw = R.get(key_user)

    # ---------- 2) hydrate user snapshot if missing ----------
    if not raw:
        # Sources to try, in order:
        #  a) membership devices
        #  b) recorded leader
        #  c) wildcard scan for any device snap for this sym/tf (bounded)
        dev_ids: list[str] = []
        try:
            dev_ids = list(R.smembers(f"xtl:user:{uid}:devices") or [])
        except Exception:
            dev_ids = []

        if not dev_ids:
            try:
                leader = R.get(f"xtl:user:{uid}:trend:leader")
                if leader:
                    if isinstance(leader, (bytes, bytearray)):
                        leader = leader.decode("utf-8")
                    dev_ids = [leader]
            except Exception:
                pass

        # last resort: scan a few matching device snaps (bounded)
        scanned_keys: list[str] = []
        if not dev_ids:
            try:
                # limit to max 10 snaps to avoid heavy scans
                it = R.scan_iter(match=f"xtl:ohlc:snap:*:{sym}:{tfu}", count=10)
                for dkey in it:
                    if isinstance(dkey, (bytes, bytearray)):
                        dkey = dkey.decode("utf-8")
                    scanned_keys.append(dkey)
            except Exception:
                scanned_keys = []

        hydrated = False

        # helper to promote one device snap to user
        def _promote_device_snap(dkey: str) -> bool:
            try:
                draw = R.get(dkey)
                if not draw:
                    return False
                if isinstance(draw, (bytes, bytearray)):
                    draw = draw.decode("utf-8")
                snap_dev = json.loads(draw)

                # normalize snapshot ms fields; KEEP bars in seconds
                snap_dev["serverNow"]    = _to_ms_any(snap_dev.get("serverNow"))
                snap_dev["lastClosedTs"] = _to_ms_any(snap_dev.get("lastClosedTs"))
                snap_dev["nextCloseTs"]  = _to_ms_any(snap_dev.get("nextCloseTs"))

                bars = snap_dev.get("bars") or []
                if bars:
                    bars[-1]["complete"] = bool(bars[-1].get("complete", True))
                # trim if needed
                if len(bars) > 1000:
                    bars = bars[-1000:]
                snap_dev["bars"] = bars  # t remains seconds

                # enrich broker if missing
                if not snap_dev.get("broker") and device_broker:
                    snap_dev["broker"] = device_broker

                R.setex(key_user, 900, json.dumps(snap_dev))
                try:
                    R.setex("xtl:debug:state2:hydrated", 300, f"{key_user} <= {dkey}")
                except Exception:
                    pass
                return True
            except Exception as _e:
                try:
                    R.setex("xtl:debug:state2:hydrate_err", 300, f"{dkey}:{_e}")
                except Exception:
                    pass
                return False

        # a) membership devices
        for did in dev_ids:
            dkey = f"xtl:ohlc:snap:{did}:{sym}:{tfu}"
            if _promote_device_snap(dkey):
                hydrated = True
                break

        # b) wildcard scanned keys
        if not hydrated and scanned_keys:
            for dkey in scanned_keys:
                if _promote_device_snap(dkey):
                    hydrated = True
                    break

        raw = R.get(key_user) if hydrated else None
       

        if not raw:
            # still warming
            server_now_ms = int(time.time() * 1000)
            # Align a sane next boundary for this TF from *server clock*
            next_close_ms = ((server_now_ms // tf_ms) * tf_ms) + tf_ms
            broker_obj = _load_broker_meta(uid, device_broker)
            _nudge_agent(uid, sym, tfu)
            return {
                "label": "Warming",
                "score": 0.0,
                "serverNow": server_now_ms,
                "lastClosedTs": 0,
                "nextCloseTs": int(next_close_ms),
                "stale": True,
                "diagnostics": {
                    "warming": True,
                    "reason": "Warming up - awaiting bars",
                    "expected_key": key_user,
                },
                "preview": {
                    "symbol": sym,
                    "tf": tfu,
                    "bars": [],
                    "lastClosedTs": None,
                },
                "broker": _safe_broker_meta(broker_obj.dict() if broker_obj else (device_broker or {})),
                "pollAfterMs": int(max(1200, min((next_close_ms - server_now_ms + 250), 60000))),
                "usingDevice": prefer_dev, 
            }

    # ---------- parse user snapshot ----------
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8")

    try:
        snap = json.loads(raw)
        # --- TF normalize (one source of truth) ---
        tfu = (tf or "M15").upper()
        if tfu not in ("M15", "H1", "H4"):
            tfu = "M15"
        TF_MS  = {"M15": 900_000, "H1": 3_600_000, "H4": 14_400_000}[tfu]
        TF_SEC = TF_MS // 1000
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Bad snapshot JSON for {key_user}: {e}")
    device_broker = (snap or {}).get("broker")  # may be None; that's OK
    candidate = device_broker or snap_broker
    broker_obj = _load_broker_meta(uid,candidate)
    # ------------------------------------------
    # --- heal stale user snapshot by promoting freshest device snapshot ---
    try:
       # current user snapshot bars + last bar OPEN (sec)
       _bars_user = (snap or {}).get("bars") or []
       _last_user_s = 0
       if _bars_user:
          try:
              _traw = int(_bars_user[-1].get("t", 0))
              _last_user_s = (_traw // 1000) if _traw > 10_000_000_000 else _traw
          except Exception:
              _last_user_s = 0

       # find a candidate device snapshot
       _dev_id = (snap or {}).get("deviceId") or (snap or {}).get("device_id")
       if not _dev_id:
           _leader = R.get(f"xtl:user:{uid}:trend:leader")
           if isinstance(_leader, (bytes, bytearray)):
               _leader = _leader.decode("utf-8", "ignore")
           _dev_id = _leader or None
           if not _dev_id:
              _set = R.smembers(f"xtl:user:{uid}:devices") or []
              if _set:
                  _any = next(iter(_set))
                  _dev_id = _any.decode("utf-8", "ignore") if isinstance(_any, (bytes, bytearray)) else str(_any)

       if _dev_id:
          _dkey = f"xtl:ohlc:snap:{_dev_id}:{sym}:{tfu}"
          _draw = R.get(_dkey)
          if _draw:
             _dstr = _draw.decode("utf-8", "ignore") if isinstance(_draw, (bytes, bytearray)) else _draw
             _dsnap = json.loads(_dstr)
             _dbars = _dsnap.get("bars") or []

             # last device OPEN (sec)
             _last_dev_s = 0
             if _dbars:
                try:
                    _traw = int(_dbars[-1].get("t", 0))
                    _last_dev_s = (_traw // 1000) if _traw > 10_000_000_000 else _traw
                except Exception:
                    _last_dev_s = 0

             # if device is newer by at least one TF, promote it
             if _last_dev_s and (_last_dev_s > _last_user_s + (TF_SEC or 0)):
                 R.setex(key_user, 900, _dstr)              # persist for next call
                 snap = _dsnap                               # use it now
                 device_broker = (snap or {}).get("broker")  # refresh broker source
    except Exception:
       pass


    
    # --- canonical timestamps so all branches are safe ---
    server_now = int(time.time() * 1000)
    last_closed_ts = _to_ms_any((snap or {}).get("lastClosedTs"))
    tf_ms = TF_MS  # normalized TF size in ms

    


    # Candidates:
    #  1) Agent/snapshot-provided nextCloseTs (if any)
    #  2) Sequence after last_closed (strict TF step)
    #  3) TF-aligned next boundary from server_now
    snap_next = _to_ms_any((snap or {}).get("nextCloseTs"))
    seq_next  = (last_closed_ts + tf_ms) if last_closed_ts else None
    base_next = ((server_now // tf_ms) * tf_ms) + tf_ms

    # Pick the max of available candidates to stay monotonic
    cands = [c for c in (snap_next, seq_next, base_next) if c]
    next_close_ts = max(cands) if cands else base_next

    # Ensure strictly future (guard skew / race at boundary)
    if next_close_ts <= server_now + 250:
       next_close_ts += tf_ms

    # Stable client cushion: one fetch right after boundary
    poll_after_ms = max(1200, min(next_close_ts - server_now + 250, 60000))



    
    
    # Prefer user snapshot bars; if empty, hydrate once from device cache and persist
    bars = (snap.get("bars") or []) if isinstance(snap, dict) else []
    if not bars:
       try:
          # figure out which device snapshot to read
          dev_id = (snap or {}).get("deviceId") or (snap or {}).get("device_id")
          if not dev_id:
              dv = R.get(f"xtl:user:{uid}:trend:leader")
              if isinstance(dv, (bytes, bytearray)):
                  dv = dv.decode("utf-8", "ignore")
              dev_id = dv or None
              if not dev_id:
                  ds = R.smembers(f"xtl:user:{uid}:devices") or []
                  if ds:
                      any_id = next(iter(ds))
                      dev_id = any_id.decode("utf-8", "ignore") if isinstance(any_id, (bytes, bytearray)) else str(any_id)

          if dev_id:
              key_dev = f"xtl:ohlc:snap:{dev_id}:{sym}:{tfu}"
              raw_dev = R.get(key_dev)
              if raw_dev:
                  raw_str = raw_dev.decode("utf-8", "ignore") if isinstance(raw_dev, (bytes, bytearray)) else raw_dev
                  dev_snap = json.loads(raw_str)
                  dev_bars = dev_snap.get("bars") or []
                  if dev_bars:
                      # persist hydration to the user-facing key so next call isn’t “Warming”
                      key_user = f"xtl:trend:snap:{uid}:{sym}:{tfu}"
                      R.setex(key_user, 900, raw_str)
                      # use hydrated snapshot for this response too
                      snap = dev_snap
                      bars = dev_bars
                      if not (snap.get("broker") if isinstance(snap, dict) else None) and dev_snap.get("broker"):
                          snap = {**snap, "broker": dev_snap.get("broker")}
       except Exception:
          pass


    # If still nothing, proceed to the warming nudger below as before
    warming_payload = None
    if not isinstance(bars, list) or not bars:
        server_now_ms = int(time.time() * 1000)
        snap_last_ms = _to_ms_any((snap or {}).get("lastClosedTs"))
        snap_next_ms = _to_ms_any((snap or {}).get("nextCloseTs"))
        last_closed_ms = int(snap_last_ms or 0)
        next_close_ms  = int(snap_next_ms or ((server_now_ms // tf_ms) * tf_ms + tf_ms))
        _nudge_agent(uid, sym, tfu)
        return {
            "label": "Warming",
            "score": 0.0,
            "serverNow": server_now_ms,
            "lastClosedTs": last_closed_ms,
            "nextCloseTs": next_close_ms,
            "stale": True,
            "diagnostics": {
                "warming": True,
                "reason": "Warming up - awaiting closed bars",
                "expected_key": key_user,
                "snap_has_bars": bool(bars),
            },
            "preview": {"symbol": sym, "tf": tfu, "bars": [], "lastClosedTs": last_closed_ms or None},
            "broker": broker_safe,
            "pollAfterMs": int(max(1200, min((next_close_ms - server_now_ms + 250), 60000))),
            "usingDevice": prefer_dev,
    }

    if not isinstance(bars, list) or not bars:
        # ---- One-shot fallback to agent: pull closed bars directly from the device ----
        try:
           agent_rows = _broker_bars_sync(sym, tfu, limit=180)
        except Exception:
           agent_rows = None

        if agent_rows:
            if (not isinstance(bars, list) or not bars) and warming_payload:
               return warming_payload
            # Treat agent rows as CLOSED bars and continue normally (no early return)
            bars = [
                 {
                    "t": r.get("t"),  # seconds or ms; later normalization handles both
                    "o": float(r["o"]),
                    "h": float(r["h"]),
                    "l": float(r["l"]),
                    "c": float(r["c"]),
                    "complete": True,
                 }
                 for r in agent_rows
            ]
        else:
            # No agent rows either -> return warming with proper scheduling
            server_now_ms = int(time.time() * 1000)
            snap_last_ms = _to_ms_any((snap or {}).get("lastClosedTs"))
            snap_next_ms = _to_ms_any((snap or {}).get("nextCloseTs"))
            last_closed_ms = int(snap_last_ms or 0)
            next_close_ms  = int(snap_next_ms or ((server_now_ms // tf_ms) * tf_ms + tf_ms))
            _nudge_agent(uid, sym, tfu)
            return {
                "label": "Warming",
                "score": 0.0,
                "serverNow": server_now_ms,
                "lastClosedTs": last_closed_ms,
                "nextCloseTs": next_close_ms,
                "stale": True,
                "diagnostics": {
                    "warming": True,
                    "reason": "Warming up - awaiting closed bars",
                    "expected_key": key_user,
                    "snap_has_bars": bool(bars),
                },
                "preview": {"symbol": sym, "tf": tfu, "bars": [], "lastClosedTs": last_closed_ms or None},
                "broker": broker_safe,
                "pollAfterMs": int(max(1200, min((next_close_ms - server_now_ms + 250), 60000))),
                "usingDevice": prefer_dev, 
            }


    # ---------- closed-bar filter (compare in ms; keep bars.t in seconds) ----------
    # ---------- closed-bar filter (time-based; t + TF_SEC <= now_s) ----------
    server_now_ms = int(time.time() * 1000)
    now_s = int(server_now_ms // 1000)
    TF_SEC = int((TF_MS // 1000) if isinstance(TF_MS, int) else (TF_MS))  # ensure seconds

    closed: list[dict] = []
    for b in bars or []:
        try:
            t_s = int(_to_ms_any(b.get("t")) // 1000)  # normalize to seconds
            if (t_s + TF_SEC) <= now_s:  # bar is fully closed if its CLOSE is in the past
                closed.append({
                    "t": t_s,
                    "o": float(b["o"]), "h": float(b["h"]),
                    "l": float(b["l"]), "c": float(b["c"]),
                    "complete": True,
                })
        except Exception:
            continue

    # If still empty but we have bars, treat all except the last as closed so the UI can render.
    if not closed and bars:
        _nudge_agent(uid, sym, tfu)
        base = bars[:-1] if len(bars) > 1 else bars
        closed = []
        for b in base:
            try:
                t_s = int(_to_ms_any(b.get("t")) // 1000)
                closed.append({
                    "t": t_s,
                    "o": float(b["o"]), "h": float(b["h"]),
                    "l": float(b["l"]), "c": float(b["c"]),
                    "complete": True,
                })
            except Exception:
                continue


    # ---------- success ----------
    # --- Detection params tuned per TF (supports UI overrides) ---
    qp = request.query_params

    def _qint(name: str, lo: int | None = None, hi: int | None = None):
        v = qp.get(name)
        if v is None:
            return None
        try:
            iv = int(float(v))
            if lo is not None:
                iv = max(lo, iv)
            if hi is not None:
                iv = min(hi, iv)
            return iv
        except Exception:
            return None

    def _qfloat(name: str, lo: float | None = None, hi: float | None = None):
        v = qp.get(name)
        if v is None:
            return None
        try:
            fv = float(v)
            if lo is not None:
                fv = max(lo, fv)
            if hi is not None:
                fv = min(hi, fv)
            return fv
        except Exception:
            return None

    def _qbool(name: str):
        v = qp.get(name)
        if v is None:
            return None
        return str(v).lower() in ("1", "true", "yes", "on")

    # sensible defaults per TF
    default_ma_fast  = 10 if tfu == "M15" else 20
    default_ma_slow  = 20 if tfu == "M15" else 50
    default_ma_type  = "ema"
    default_slope_p  = 10 if tfu == "M15" else 20
    default_slope_th = 0.30
    default_adx_min  = 20
    default_adx_lb   = 14
    default_use_di   = True

    params = DetectParams(
        ma=MAParams(
            fast=_qint("maFast", 2, 400) or default_ma_fast,
            slow=_qint("maSlow", 3, 600) or default_ma_slow,
            type=((qp.get("maType") or default_ma_type).lower()),
        ),
        slope=SlopeParams(
            period=_qint("slopePeriod", 3, 200) or default_slope_p,
            threshold=(
                _qfloat("slopeThreshold", 0.0, 5.0)
                if _qfloat("slopeThreshold", 0.0, 5.0) is not None
                else default_slope_th
            ),
        ),
        structure=StructureParams(atrMult=1.5, zigzagPct=0.6),
        strength=StrengthParams(
            adxMin=_qint("adxMin", 5, 60) or default_adx_min,
            lookback=_qint("adxPeriod", 5, 50) or default_adx_lb,
            useDIbias=(
                _qbool("useDIbias")
                if _qbool("useDIbias") is not None
                else default_use_di
            ),
        ),
    )

    # OPEN of last closed bar
    _last_open_ms = _to_ms_any(closed[-1].get("t"))
    # TRUE close time of the last closed bar
    last_closed_ms = _last_open_ms + tf_ms
    # Next boundary is one TF after the last close (or snapshot hint)
    next_close_ms = snap_next or (last_closed_ms + tf_ms if last_closed_ms else server_now_ms + tf_ms)

    # --- compute real label/score using your indicator logic ---
    try:
       c = [float(b["c"]) for b in closed]
       h = [float(b["h"]) for b in closed]
       l = [float(b["l"]) for b in closed]
       
       try:
          label, score, diagnostics = compute_label_and_score(c, h, l, params)
       except Exception as e:
          label, score, diagnostics = ("Neutral", 0.0, {"error": str(e)})
       adx_val = None
       slope_val = None
       structure_val = None
       if isinstance(diagnostics, dict):
           # ADX is a list -> take the latest value
           adx_series = diagnostics.get("adx")
           if isinstance(adx_series, list) and adx_series:
               adx_val = adx_series[-1]
           elif isinstance(adx_series, (int, float)):
               adx_val = adx_series

           # slope stored as percentage under slopePct
           slope_val = diagnostics.get("slopePct")

           # structure label name
           structure_val = diagnostics.get("structureLabel") or "-"
       
    except Exception as e:
        # Fallback if something goes wrong in computation
        label, score, adx_val, slope_val, structure_val, diagnostics = (
            "Neutral", 0.0, 0.0, 0.0, "-", {"error": str(e)}
        )
    # --- adaptive polling hint for UI ---
    # how long until next close, based on the current server_now_ms we computed earlier
    remain_ms = max(0, int((next_close_ms or 0) - server_now_ms))

    # default gentle polling if boundary isn't known
    poll_after_ms = 10_000

    # if we know the boundary, wake slightly after it; clamp to [2s, 60s]
    if next_close_ms and server_now_ms:
        poll_after_ms = max(2_000, min(60_000,(next_close_ms - server_now_ms) + 500))


    
    

    # Need enough bars for the chosen params (no hard 50-bar floor)
    min_needed = max(
        params.ma.slow + 5,
        params.slope.period + 5,
        params.strength.lookback + 5,
    )
    min_needed = min(60, min_needed)

    if len(closed) < min_needed:
       # Do NOT block preview; degrade gracefully and continue.
       # Compute a neutral label later; keep a hint in diagnostics.
       try:
          diagnostics = {**(diagnostics or {}), "warming": True,
                       "reason": f"insufficient_bars:{len(closed)}<{min_needed}"}
       except NameError:
          diagnostics = {"warming": True, "reason": f"insufficient_bars:{len(closed)}<{min_needed}"}




    # --- Build series for detection ---
    try:
        c = [float(b["c"]) for b in closed]
        h = [float(b["h"]) for b in closed]
        l = [float(b["l"]) for b in closed]
    except Exception:
        raise HTTPException(status_code=400, detail="Bars missing c/h/l fields")

    # Compute label/score/diagnostics (your existing function)
    label, score, diagnostics = compute_label_and_score(c, h, l, params)

    

    
    # --- Timestamps & staleness (single source of truth) ---
    
    TF_MS = {"M15": 15*60*1000, "H1": 60*60*1000, "H4": 4*60*60*1000}
    tf_ms = int(TF_MS.get(tfu, 60*60*1000))
    server_now_ms = int(time.time() * 1000)

    
    
    if not closed:
        # Do NOT return. Use whatever we have as provisional closed bars so the UI can render.
        _nudge_agent(uid, sym, tfu)
        # take up to N most recent bars and mark as complete to allow preview
        N = max(30, min(int(n or 60), 500))
        closed = [
            {
                "t": int(_to_ms_any(b.get("t"))) // 1000,  # seconds
                "o": float(b["o"]), "h": float(b["h"]),
                "l": float(b["l"]), "c": float(b["c"]),
                "complete": True,
            }
            for b in (bars[-N:] if bars else [])
        ]


    # last truly-closed bar (ms)
    last_closed_ts = int((_to_ms_any(closed[-1].get("t")) or 0) + tf_ms)

    # last_closed_ts: CLOSE of the last completed bar (ms, UTC)
    # TF_MS: normalized timeframe in ms
    # server_now_ms: current server time in ms

    # Compute next close aligned to tf_ms
    if last_closed_ts <= 0:
        # warming fallback: next close is the next TF boundary from "now"
        next_close_ts = ((server_now_ms // tf_ms) + 1) * tf_ms
    else:
        next_close_ts = last_closed_ts + tf_ms


    # roll forward if we’re already past it (covers missed bars, clock skew, etc.)
    EPS = 500  # ms cushion
    while next_close_ts <= server_now_ms - EPS:
        next_close_ts += tf_ms

    # optional: tiny epsilon nudge if we are *just* at/behind boundary
    if server_now_ms >= next_close_ts - EPS:
        next_close_ts += tf_ms


    # Staleness (weekend-aware): don't block preview; just flag
    import datetime as _dt
    is_weekend = _dt.datetime.utcnow().weekday() >= 5  # 5=Sat, 6=Sun
    age_ms = server_now_ms - last_closed_ts
    max_age_ms = (3 * tf_ms) if not is_weekend else (72 * 60 * 60 * 1000)
    stale = age_ms > max_age_ms

    # ---- Time diagnostics log (AFTER the vars are defined) ----
    ist = _dt.timezone(_dt.timedelta(hours=5, minutes=30))
    def _iso(ms): return "-" if ms is None else _dt.datetime.utcfromtimestamp(ms/1000).isoformat()+"Z"
    def _iso_ist(ms): return "-" if ms is None else _dt.datetime.fromtimestamp(ms/1000, tz=ist).isoformat()


    log.info(
        f"[TREND] timecheck sym={sym} tf={tfu} "
        f"serverNow_utc={_iso(server_now)} lastClosed_utc={_iso(last_closed_ts)} nextClose_utc={_iso(next_close_ts)} "
        f"serverNow_ist={_iso_ist(server_now)} lastClosed_ist={_iso_ist(last_closed_ts)} nextClose_ist={_iso_ist(next_close_ts)}"
    )
    # --- Prefer live broker bars (BID) when agent is reachable; fallback to snapshot ---
    prev_rows_override = None
    try:
        raw_broker = _broker_bars_sync(symbol.upper(), tf, limit=300, price="bid")
    except Exception:
        raw_broker = []

    if raw_broker:
        tf_sec = 900 if tf == "M15" else (3600 if tf == "H1" else 14400)
        now_s = int(time.time())
        rows = []
        for b in raw_broker[-305:]:
            try:
                t_raw = int(b.get("t", 0))
                # normalize to **seconds** for a fair compare against now_s (which is seconds)
                t_sec = (t_raw // 1000) if t_raw > 10_000_000_000 else t_raw
                o = float(b["o"]); h = float(b["h"]); l = float(b["l"]); c = float(b["c"])
                is_forming = (now_s < (t_sec + tf_sec))
                rows.append({"t": t_sec, "o": o, "h": h, "l": l, "c": c, "complete": (not is_forming)})
            except Exception:
                continue
        if rows:
            # keep only closed bars so preview = last CLOSED candle like MT5
            closed_rows = [r for r in rows if r.get("complete")]
            prev_rows_override = closed_rows[-300:] if closed_rows else []
    # choose source (broker first, else snapshot)
    rows_src = (prev_rows_override or [])
    # Fallback to snapshot 'closed' bars so we still render via the unified path
    if not rows_src:
        rows_src = [
            {
                "t": int(_to_ms_any(b.get("t")))//1000,  # seconds (MT5 bar time = OPEN)
                "o": float(b["o"]), "h": float(b["h"]),
                "l": float(b["l"]), "c": float(b["c"]),
                "complete": True,  # only closed bars
            }
            for b in closed
        ]



    # normalize + keep 'complete' flag when present
    # --- Normalize OHLC before tailing ---
    # --- Build broker meta safely (device > snapshot) ---
    snap_broker = (snap or {}).get("broker") if isinstance(snap, dict) else None
    candidate = device_broker or snap_broker
    broker_obj = _load_broker_meta(uid, candidate)
    broker_safe: dict = _safe_broker_meta(
       broker_obj.dict() if broker_obj else (candidate or {})
    )

    norm = _normalize_ohlc(rows_src)
    
    # ensure chronological order before taking tails/last
    try:
        norm = sorted(norm, key=lambda r: int(_epoch_to_ms_any(r.get("t"))))
    except Exception:
        pass
    norm = [r for r in norm if r.get("complete")] or []

    rows_src_len = len(rows_src or [])
    norm_closed_len = len(norm or [])
    log.info(
        f"[TREND] preview-branch check: rows_src={len(rows_src or [])} "
        f"norm_closed={len(norm or [])} prev_rows_override={len(prev_rows_override or [])} "
        f"closed_snapshot={len(closed or [])} tz_off={(broker_safe or {}).get('tz_offset_min')}"
    )

    if not norm:
        server_now_ms = int(time.time() * 1000)
        try:
           last_closed_ms = int(_to_ms_any(snap.get("lastClosedTs")) or 0)
        except Exception:
           last_closed_ms = 0

        try:
           nc_hint = int(_to_ms_any(snap.get("nextCloseTs")) or 0)
        except Exception:
           nc_hint = 0

        tf_ms = int(tf_ms)  # ensure int

        next_close_ms = nc_hint if nc_hint > 0 else (last_closed_ms + tf_ms if last_closed_ms > 0 else 0)
    
        return {
            "ok": True,
            "label": "Warming",
            "score": 0.0,
            "serverNow": server_now_ms,
            "lastClosedTs": int(last_closed_ms or 0),
            "nextCloseTs": int(next_close_ms or 0),
            "stale": True,
           
            "pollAfterMs": 1000,
            "usingDevice": prefer_dev, 
            "diagnostics": {
                "warming": True,
                "reason": "No bars after broker normalization",
                "rows_src_len": len(rows_src or []),
                "prev_rows_override_len": len(prev_rows_override or []),
                "closed_snapshot_len": len(closed or []),
            },
            "preview": {"symbol": sym, "tf": tfu, "bars": [], "lastClosedTs": None},
            "broker": broker_safe,
       
        }
    last = norm[-1]  # last CLOSED broker row (what we intend to render)
   

    off_ms = int((broker_safe or {}).get("tz_offset_min") or 0) * 60_000
    last_open_ms  = (((_epoch_to_ms_any(last["t"]) + off_ms) // (TF_SEC * 1000)) * (TF_SEC * 1000)) - off_ms
    last_close_ms = last_open_ms + (TF_SEC * 1000)

    previewProbe = {
        "broker_tz_offset_min": (broker_safe or {}).get("tz_offset_min"),
        "tf_sec": TF_SEC,
        "agent_bar": {
            "t": int(last["t"]),
            "o": float(last["o"]), "h": float(last["h"]),
            "l": float(last["l"]), "c": float(last["c"]),
        },
        "render_bar": {
            "t_open_ms": int(last_open_ms),
            "t_close_ms": int(last_close_ms),
        },
    }

    # --- Include forming bar if present so preview matches MT5 "now" ---
    include_forming = bool(norm and (norm[-1].get("complete") is False))

    # --- Apply tailing limit (default 60; clamp 30–500) ---
    N = max(30, min(int(n or 60), 500))
    tail = norm[-N:]


    # choose digits: prefer digits from snapshot -> fallback env BROKER_DIGITS
    digits = BROKER_DIGITS
    try:
        snap_broker = (snap or {}).get("broker") or {}
        b_digits = (broker_safe or {}).get("digits")
        if isinstance(b_digits, (int, float)):
            digits = int(b_digits)
        elif isinstance(snap_broker.get("digits"), (int, float)):
            digits = int(snap_broker["digits"])
    except Exception:
        pass

    # round to broker digits
    for r in tail:
        r["o"] = round(r["o"], digits)
        r["h"] = round(r["h"], digits)
        r["l"] = round(r["l"], digits)
        r["c"] = round(r["c"], digits)

    prev_rows = tail  # <- do NOT overwrite later

    # compute preview lastClosedTs based on whether the last row is forming
    if prev_rows:
        last_open = prev_rows[-1]["t"]
        preview_last_closed_ts = (
            last_open * 1000 if include_forming else (last_open + TF_SEC) * 1000
        )
    else:
        preview_last_closed_ts = None

    # build preview payload
    # --- Build probe for last CLOSED bar (already computed: last, TF_SEC, broker_safe, norm) ---
    off_ms = int((broker_safe or {}).get("tz_offset_min") or 0) * 60_000  # agent broker TZ shift (ms)
    last_open_ms  = (((_epoch_to_ms_any(last["t"]) + off_ms) // (TF_SEC * 1000)) * (TF_SEC * 1000)) - off_ms
    last_close_ms = last_open_ms + (TF_SEC * 1000)

    previewProbe = {
        "broker_tz_offset_min": (broker_safe or {}).get("tz_offset_min"),
        "tf_sec": TF_SEC,
        "agent_bar": {
             "t": int(last["t"]),
             "o": float(last["o"]), "h": float(last["h"]),
             "l": float(last["l"]), "c": float(last["c"]),
        },
        "render_bar": {
            "t_open_ms": int(last_open_ms),
            "t_close_ms": int(last_close_ms),
        },
    }

    # expose probe in diagnostics
    # enrich diagnostics (replace the single-line assignment with this block)
    diagnostics = {
        **(diagnostics or {}),
        "previewProbe": previewProbe,
        "rows_src_len": len(rows_src or []),
        "norm_closed_len": len(norm or []),
        "tz_off_used_min": (broker_safe or {}).get("tz_offset_min"),
        "tf_sec": TF_SEC,
        "compare": {
            "agent_last": {
                "t": int(last["t"]),
                "o": float(last["o"]), "h": float(last["h"]),
                "l": float(last["l"]), "c": float(last["c"]),
            },
            "render_last": {
                "t_open_ms": int(last_open_ms),
                "t_close_ms": int(last_close_ms),
                "o": float(last["o"]), "h": float(last["h"]),
                "l": float(last["l"]), "c": float(last["c"]),
            },
        },
    }


    # --- Build preview payload (repeat open/close calc per row; don't reference t_open_ms) ---
    # Anchor each bar to the broker TF grid using tz_offset_min
    off_ms = int((broker_safe or {}).get("tz_offset_min") or 0) * 60_000
    bars_tail = []
    for r in norm[-int(n or 60):]:
        open_ms = (((_epoch_to_ms_any(r["t"]) + off_ms) // (TF_SEC * 1000)) * (TF_SEC * 1000)) - off_ms
        bars_tail.append(
            PreviewBar(
                t_open_ms=int(open_ms),
                t_close_ms=int(open_ms + (TF_SEC * 1000)),
                o=r["o"], h=r["h"], l=r["l"], c=r["c"],
            )
        )


    # --- Build preview payload (use MT5 UTC bar open; UI adds broker offset for display) ---   
    preview = PreviewPayload(
    symbol=sym,
    tf=tfu,
    bars=[
       PreviewBar(
           # broker-grid anchoring using existing TF_SEC and broker_safe
           t_open_ms = int((((( _epoch_to_ms_any(r["t"]) + int((broker_safe or {}).get("tz_offset_min") or 0) * 60000 )
                               // (TF_SEC * 1000) ) * (TF_SEC * 1000))
                             - int((broker_safe or {}).get("tz_offset_min") or 0) * 60000)),
           t_close_ms = int((((( _epoch_to_ms_any(r["t"]) + int((broker_safe or {}).get("tz_offset_min") or 0) * 60000 )
                               // (TF_SEC * 1000) ) * (TF_SEC * 1000))
                             - int((broker_safe or {}).get("tz_offset_min") or 0) * 60000) + (TF_SEC * 1000)),
           o=r["o"], h=r["h"], l=r["l"], c=r["c"],

       )
       for r in norm[-int(n or 60):]
    ],
    lastClosedTs=int(last_closed_ms or 0),
    probe=previewProbe,
)
    try:
       preview_out = preview.dict()
    except Exception:
       preview_out = dict(preview)
    preview_out["broker"] = broker_safe
    # ---- Final return ----
    try:
        broker_obj_final = BrokerMeta(**(broker_safe or {})) if broker_safe else None
    except Exception:
        broker_obj_final = None

    # prefer the computed last_closed_ts / next_close_ts; fall back to preview/easy hints
    _last_closed_out = int((locals().get("last_closed_ts")
                        or locals().get("last_closed_ms")
                        or (preview.lastClosedTs if hasattr(preview, "lastClosedTs") else 0)) or 0)
    _next_close_out  = int((locals().get("next_close_ts")
                        or locals().get("next_close_ms")
                        or _align_next_close_ms(int(time.time()*1000),
                                                int(TF_MS if isinstance(TF_MS, int) else TF_MS),
                                                (broker_safe or {}).get("tz_offset_min"))) or 0)

    return {
        "label":        str(label or "Neutral"),
        "score":        float(score or 0.0),
        "serverNow":    int(server_now_ms),
        "lastClosedTs": _last_closed_out,
        "nextCloseTs":  _next_close_out,
        "diagnostics":  (diagnostics or {}),
        "stale":        bool(stale),
        "preview":      preview_out,                 # PreviewPayload object is fine; FastAPI will serialize
        "broker":       broker_obj_final,        # may be None if not available
        "adx":          (locals().get("adx_val")),
        "slope":        (locals().get("slope_val")),
        "structure":    (locals().get("structure_val")),
        "pollAfterMs":  int(locals().get("poll_after_ms") or max(2000, min((_next_close_out - server_now_ms) + 500, 60000))),
        "usingDevice": prefer_dev,
    }




    # debug: which source and whether forming included
    try:
        if prev_rows:
            last = prev_rows[-1]
            src_used = "broker" if prev_rows_override else "snapshot"
            log.info(
                f"[TREND] preview {src_used} tf={tf} include_forming={include_forming} "
                f"last_open_utc={last['t']} OHLC={last['o']},{last['h']},{last['l']},{last['c']} "
                f"digits={digits}"
            )
    except Exception:
        pass


    
    
    # --- Build broker_obj safely from snapshot broker (with device-registry fallback) ---
    snap_broker = (snap or {}).get("broker") 
    broker_obj  = _load_broker_meta(uid, snap_broker)
    # Recompute the next boundary strictly from server clock + TF
    # (prefer the device broker already loaded above; do NOT overwrite with local/IST here)
    EPS = 500  # ms cushion so we don't schedule in the past
    if last_closed_ts and last_closed_ts > 0:
        next_close_ts = last_closed_ts + tf_ms
    else:
        # If we have no lastClosedTs, snap to the next TF boundary from server_now
        next_close_ts = ((server_now // tf_ms) + 1) * tf_ms

    # Ensure the boundary is in the future
    while next_close_ts <= server_now - EPS:
        next_close_ts += tf_ms


       



    diagnostics = {
        **(diagnostics or {}),
        "previewProbe": previewProbe,
        "counts": {"bars_total": len(bars), "bars_closed": len(closed)},
        "server_now": server_now,
        "last_closed_ts": last_closed_ts,
        "next_close_ts": next_close_ts,
        "is_weekend": is_weekend,
        "timeMeta": {
            "tfMinutes": int(tf_ms // 60000),
            "serverNowUtcISO": _dt.datetime.utcfromtimestamp(server_now / 1000).isoformat() + "Z",
            "lastClosedUtcISO": _dt.datetime.utcfromtimestamp(last_closed_ts / 1000).isoformat() + "Z",
            "nextCloseUtcISO": _dt.datetime.utcfromtimestamp(next_close_ts / 1000).isoformat() + "Z",
        },
    }

    return DetectResp(
       label=label,
       score=float(score or 0.0),
       adx=float(adx_val or 0.0),
       slope=float(slope_val or 0.0),
       structure=structure_val or "-",
       serverNow=server_now_ms,
       lastClosedTs=int(last_closed_ms or 0),
       nextCloseTs=int(next_close_ms or 0),
       stale=False if stale is None else bool(stale),
       pollAfterMs=int(poll_after_ms or 0),
       diagnostics=diagnostics,          # includes "previewProbe"
       preview=preview,                  # broker-TZ anchored bars
       broker=BrokerMeta(**broker_safe), # built from device/snapshot
       usingDevice= prefer_dev,
    )



# --- Minimal, safe /trend/detect (drop-in) -----------------------------------
@router.post("/detect", response_model=DetectResp)
def trend_detect(req: DetectReq, user_id: str = Depends(get_user_id)) -> DetectResp:
    """
    Lightweight detect endpoint:
    - Normalizes TF
    - Reads device/user snapshot if present
    - Returns 'warming' when snapshot isn't ready
    - Includes broker meta via _load_broker_meta (safe)
    """
    import time, json

    # 1) Normalize inputs
    sym = (req.symbol or "XAUUSD").upper()
    tfu = (req.tf or "M15").upper()
    if tfu not in ("M15", "H1", "H4"):
        tfu = "M15"

    TF_MS = {"M15": 15 * 60 * 1000, "H1": 60 * 60 * 1000, "H4": 4 * 60 * 60 * 1000}[tfu]
    server_now_ms = int(time.time() * 1000)

    # 2) Try user snapshot first, then device snapshot (both optional)
    snap = None
    raw = None
    try:
        kuser = f"xtl:trend:snap:{user_id}:{sym}:{tfu}"
        raw = R.get(kuser)
        if not raw:
            # fall back to last device snapshot (optional; best-effort)
            # If you track a current device ID per user, you can fetch it; otherwise leave this out.
            pass
        if raw:
            snap = json.loads(raw)
    except Exception:
        snap = None  # treat as warming

    # 3) Build broker meta safely (from snapshot if present)
    broker_obj = _load_broker_meta(user_id, (snap or {}).get("broker"))
    broker_safe = _safe_broker_meta(broker_obj.dict() if broker_obj else ((snap or {}).get("broker") or {}))

    # 4) If no snapshot yet -> warming response
    if not snap:
        next_close = ( (server_now_ms // TF_MS) + 1 ) * TF_MS
        if next_close - server_now_ms < 1000:
            next_close += TF_MS
        return DetectResp(
            ok=True,
            warming=True,
            message="Warming up - awaiting bars",
            serverNow=server_now_ms,
            lastClosedTs=0,
            nextCloseTs=next_close,
            tf_ms=TF_MS,
            preview={"bars": []},
            broker=BrokerMeta(**broker_safe),
        )

    # 5) Snapshot present -> normalize minimal fields
    last_closed = int(snap.get("lastClosedTs") or 0)
    next_close = int(snap.get("nextCloseTs") or ((server_now_ms // TF_MS) + 1) * TF_MS)
    if next_close - server_now_ms < 1000:
        next_close += TF_MS

    # preview bars: accept either top-level "bars" or nested "preview": {"bars":[...]}
    if isinstance(snap.get("preview"), dict) and isinstance(snap["preview"].get("bars"), list):
        preview = {"bars": snap["preview"]["bars"]}
    else:
        preview = {"bars": (snap.get("bars") or [])}

    # 6) Return stable payload
    return DetectResp(
        ok=True,
        warming=False,
        message="OK",
        serverNow=server_now_ms,
        lastClosedTs=last_closed,
        nextCloseTs=next_close,
        tf_ms=TF_MS,
        preview=preview,
        broker=BrokerMeta(**broker_safe),
    )
