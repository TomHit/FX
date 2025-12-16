# -*- coding: utf-8 -*-
from __future__ import annotations



from typing import Literal, List, Tuple, Optional, Any, Dict
from fastapi import APIRouter, HTTPException, Depends, Query, Request, Header
from pydantic import BaseModel, Field, validator
import os
import json
import time as _time
import logging
import redis
import re
import time
import math
import httpx
from .db import db
from fastapi.responses import JSONResponse
from pathlib import Path
import xgboost as xgb
from api.macro_state import get_macro_snapshot
import csv
from api.trend.infer_rt import (
    predict_next_hour,
    predict_next_4h,
    pull_latest_h1,
    pull_latest_h4,
)
from .trend_sr import summarize_sr_multi_tf

from api.trend.infer_tth import predict_tth
from openai import OpenAI
client = OpenAI()


log = logging.getLogger("xtl.trend")

# --- H4 model toggle (default OFF to avoid slow path/504s) ---
ENABLE_H4_MODEL = os.getenv("ENABLE_H4_MODEL", "false").lower() == "true"
log.info(f"[TREND] ENABLE_H4_MODEL={ENABLE_H4_MODEL}")


REG_PATH = Path("/opt/xauapi/api/trend/models/xgb_reg.json")
CLS_PATH = Path("/opt/xauapi/api/trend/models/xgb_cls.json")



# --- Opportunity thresholds & helpers (H1 + H4) ---

# H1: "immediate" room thresholds (in percent)
ROOM_THRESHOLDS_H1: dict[str, float] = {
    "XAUUSD": 0.02,   # gold typically ~0.5-1.3% daily, tune later
    "EURUSD": 0.02,
    "GBPUSD": 0.02,
    "USDJPY": 0.02,
    "USDCHF": 0.02,
    "USDCAD": 0.02,
}

# H4: larger-structure room thresholds (can be equal or slightly higher than H1)
ROOM_THRESHOLDS_H4: dict[str, float] = {
    "XAUUSD": 0.03,
    "EURUSD": 0.03,
    "GBPUSD": 0.03,
    "USDJPY": 0.03,
    "USDCHF": 0.03,
    "USDCAD": 0.03,
}

# Minimum overall opportunity score (0-100 scale) before we surface an item.
# Can be tuned or overridden via env var: XTREND_OPP_SCORE_MIN
try:
    OPP_SCORE_MIN: float = float(os.getenv("XTREND_OPP_SCORE_MIN", "40.0"))
except Exception:
    OPP_SCORE_MIN = 40.0

def _extract_tf_sr(sr_summary: dict | None, tf_key: str) -> dict[str, float | str | None]:
    """
    From a multi-TF SR summary, extract a compact view for one TF.

    Expected shapes (defensive):
    - sr_summary["H1"]["nearest"] / sr_summary["H4"]["nearest"]
    - or sr_summary["by_tf"]["H1"]["nearest"]
    - or simply sr_summary["H1"] / ["H4"] being a nearest-zone dict.
    """
    out: dict[str, float | str | None] = {
        "side": None,
        "level": None,
        "dist_pct": None,
    }
    if not isinstance(sr_summary, dict):
        return out

    # Try direct TF block: sr_summary["H1"] or ["H4"]
    tf_block = sr_summary.get(tf_key) if isinstance(sr_summary.get(tf_key), dict) else None

    # Fallback: by_tf structure
    if not tf_block and isinstance(sr_summary.get("by_tf"), dict):
        tf_block = sr_summary["by_tf"].get(tf_key) if isinstance(
            sr_summary["by_tf"].get(tf_key), dict
        ) else None

    if not isinstance(tf_block, dict):
        return out

    # Nearest zone object  allow multiple key names
    nearest = tf_block.get("nearest") or tf_block.get("nearest_zone") or tf_block
    if not isinstance(nearest, dict):
        return out

    side = nearest.get("kind") or nearest.get("side") or None
    level = nearest.get("level")
    dist = nearest.get("distance_pct") or nearest.get("dist_pct")

    try:
        out["side"] = str(side) if side is not None else None
    except Exception:
        out["side"] = None

    try:
        out["level"] = float(level) if isinstance(level, (int, float)) else None
    except Exception:
        out["level"] = None

    try:
        out["dist_pct"] = float(dist) if isinstance(dist, (int, float)) else None
    except Exception:
        out["dist_pct"] = None

    return out


def _room_thr_h1(sym: str) -> float:
    """
    Per-symbol 1h opportunity threshold.
    If symbol not listed, default to 1.0% so nothing explodes.
    """
    if not sym:
        return 1.0
    return float(ROOM_THRESHOLDS_H1.get(sym.upper(), 1.0))


def _room_thr_h4(sym: str) -> float:
    """
    Per-symbol 4h "structure" threshold.
    If symbol not listed, fall back to its own H1 threshold.
    """
    if not sym:
        return 1.0
    return float(ROOM_THRESHOLDS_H4.get(sym.upper(), _room_thr_h1(sym)))


# Where we keep per-symbol frozen H1 opportunity snapshots in Redis

# Where we keep per-symbol frozen H1 opportunity snapshots in Redis
OPP_SNAPSHOT_PREFIX = "xtl:trend:opp:h1"


def _opp_snapshot_key(sym: str, opp_dir: str) -> str:
    s = (sym or "").upper()
    d = (opp_dir or "").strip().upper()
    if d in ("BUY", "LONG", "UP", "BULL", "BULLISH"):
        d = "UP"
    elif d in ("SELL", "SHORT", "DOWN", "BEAR", "BEARISH"):
        d = "DOWN"
    return f"{OPP_SNAPSHOT_PREFIX}:{s}:{d}"


def _freeze_or_snapshot_opp(sym: str, row: dict[str, Any], now_ms: int) -> dict[str, Any]:
    """
    Takes an opportunity row and ensures:
    - Stable alert_created_ms / alert_id for 1 hour horizon
    - Live snapshot in Redis per symbol/direction
    - Alert history entry in ALERT_HASH_PREFIX + ALERT_INDEX_KEY
    - Moves to 'expired' after 1 hour and stops returning in 'rows'
    """

    sym_u = (sym or "").upper()
    opp_dir = (row.get("opp_direction") or row.get("direction") or "").upper()
    if opp_dir not in ("UP", "DOWN"):
        # no direction = nothing to freeze
        row["status"] = row.get("status") or "none"
        return row

    snap_key = _opp_snapshot_key(sym_u, opp_dir)

    # Helper to safely json-load snapshot fields
    def _snap_get(d: dict[str, Any], key: str, default=None):
        v = d.get(key)
        if v is None:
            return default
        try:
            return json.loads(v)
        except Exception:
            return v

    # Try to load existing snapshot
    snap = {}
    try:
        snap = R.hgetall(snap_key) or {}
    except Exception:
        snap = {}

    # --------------------------------------------------
    # Existing snapshot: check status / horizon
    # --------------------------------------------------
    if snap:
        status = _snap_get(snap, "status", "active") or "active"
        alert_ms = int(_snap_get(snap, "alert_created_ms", now_ms) or now_ms)
        horizon_min = _snap_get(snap, "horizon_min", 60)
        try:
            horizon_ms = int(float(horizon_min) * 60_000)
        except Exception:
            horizon_ms = 60 * 60_000

        expire_ts = _snap_get(snap, "opp_expire_ts")
        if not isinstance(expire_ts, (int, float)):
            expire_ts = alert_ms + horizon_ms

        alert_id = _snap_get(snap, "alert_id")

        # If already final, just mirror status into row and do NOT show in live list
        if status in ("hit", "expired"):
            row["status"] = status
            return row

        # Time-based expiry: after 1 hour, mark as expired + move to history
        if now_ms >= int(expire_ts):
            if alert_id:
                try:
                    _mark_alert_expired(alert_id, now_ms)
                except Exception:
                    pass
            try:
                R.hset(
                    snap_key,
                    mapping={
                        "status": json.dumps("expired"),
                        "opp_expire_ts": json.dumps(int(expire_ts)),
                    },
                )
            except Exception:
                pass

            # Don't return as active
            row["status"] = "expired"
            return row

        # Still active ? reuse stable fields from snapshot
        # Still active ? reuse stable fields from snapshot
        row["status"] = "active"
        row.setdefault("alert_created_ms", alert_ms)
        row.setdefault("alert_id", alert_id)

        # --- Horizon (TTH-driven, NOT hard-coded) ---
        # Prefer snapshot horizon if present, otherwise keep computed horizon_min
        snap_horizon = _snap_get(snap, "horizon_min")
        if snap_horizon is not None:
            row.setdefault("horizon_min", int(snap_horizon))
        else:
            row.setdefault("horizon_min", int(horizon_min))

        # --- Basis / Target ---
        # New canonical fields (time-agnostic)
        basis = _snap_get(snap, "basis_price")
        target = (
            _snap_get(snap, "target_price")        # preferred (new)
            or _snap_get(snap, "target_price_1h")  # backward compatibility
        )

        if basis is not None:
            row.setdefault("basis_price", basis)
            # legacy field (do not use for logic)
            row.setdefault("basis_price_1h", basis)

        if target is not None:
            row.setdefault("target_price", target)
            # legacy field (do not use for logic)
            row.setdefault("target_price_1h", target)

        # --- Expiry (derived from horizon, NOT fixed 1h) ---
        row.setdefault(
            "opp_expire_ts",
            row["alert_created_ms"] + int(row["horizon_min"]) * 60_000
        )

        return row


    # --------------------------------------------------
    # No existing snapshot ? create a fresh alert
    # --------------------------------------------------
    try:
        basis_f = float(row.get("basis_price_1h") or row.get("basis_price") or 0.0)
    except Exception:
        basis_f = 0.0

    try:
        move_f = float(row.get("expected_move_pct_1h") or row.get("opp_expected_move_pct_1h") or 0.0)
    except Exception:
        move_f = 0.0

    try:
        target_f = basis_f * (1.0 + move_f / 100.0)
    except Exception:
        target_f = basis_f

    tth = predict_tth(sym_u)
    # ---------- TTH HARD GATE ----------
    if not isinstance(tth, dict) or not tth.get("ok"):
        return None

    p_dir = max(float(tth.get("p_up", 0.0) or 0.0), float(tth.get("p_down", 0.0) or 0.0))
    if p_dir < 0.65:
        return None



    horizon_min = int(tth["horizon_min"])
    horizon_ms = horizon_min * 60_000
    expire_ts = alert_ms + horizon_ms


    alert_id = f"{sym_u}-{alert_ms}-{opp_dir}"

    # Store back onto row for UI
    row["alert_created_ms"] = alert_ms
    row["horizon_min"] = horizon_min
    row["status"] = "active"
    row["basis_price_1h"] = basis_f
    row["target_price_1h"] = target_f
    row["alert_id"] = alert_id

    # ---- 1) Save into alert history hash/index (for history section) ----
    try:
        payload = {
            "symbol": sym_u,
            "direction": opp_dir,
            "opp_direction": opp_dir,
            "alert_id": alert_id,
            "alert_created_ms": alert_ms,
            "horizon_min": horizon_min,
            "opp_expire_ts": int(expire_ts),
            "basis_price": basis_f,
            "alert_price_1h": basis_f,
            "target_price_1h": target_f,
            "expected_move_pct": move_f,
            "expected_move_pct_1h": move_f,
            "opp_expected_move_pct_1h": move_f,
            "status": "active",
            "p_up": row.get("p_up"),
        }
        _save_alert_snapshot(sym_u, payload)
    except Exception as e:
        log.warning("[OPP] _save_alert_snapshot failed sym=%s dir=%s err=%r", sym_u, opp_dir, e)

    # ---- 2) Live per-symbol snapshot for this horizon -------------------
    snap_payload = {
        "symbol": sym_u,
        "direction": opp_dir,
        "opp_direction": opp_dir,
        "alert_created_ms": alert_ms,
        "basis_price": basis_f,
        "alert_price_1h": basis_f,
        "opp_expected_move_pct_1h": move_f,
        "target_price_1h": target_f,
        "status": "active",
        "horizon_min": horizon_min,
        "opp_expire_ts": expire_ts,
        "alert_id": alert_id,
    }

    try:
        mapping = {k: json.dumps(v) for k, v in snap_payload.items()}
        R.hset(snap_key, mapping=mapping)
    except Exception:
        # snapshot is UX; do not break endpoint
        pass

    return row



import json as _json

def _append_opp_history(sym: str, opp_dir: str, snap: dict[str, Any]) -> None:
    """
    Append one final alert record to Redis history.
    Stored newest-first.
    """
    try:
        now_ms = int(time.time() * 1000)
    except Exception:
        now_ms = 0

    try:
        open_ts = int(float(snap.get("alert_created_ms", now_ms)))
    except Exception:
        open_ts = now_ms

    hmin = snap.get("horizon_min")
    if hmin:
         exp_ts = open_ts + int(hmin) * 60_000
    else:
         exp_ts = snap.get("opp_expire_ts")

    alert_id = f"{sym}-{open_ts}"

    item = {
        "id": alert_id,
        "symbol": sym,
        "direction": opp_dir.lower(),
        "alert_created_ms": open_ts,
        "opp_expire_ts": exp_ts,
    }

    for field in ("expected_move_pct_1h", "target_price_1h", "basis_price_1h", "p_up"):
        v = snap.get(field)
        if v is None:
            continue
        try:
            item[field] = float(v)
        except Exception:
            pass

    try:
        key = "xtl:trend:opp:history"
        R.lpush(key, _json.dumps(item))
        R.ltrim(key, 0, 199)  # keep last 200 alerts
    except Exception as e:
        log.warning("[OPP] history lpush failed sym=%s dir=%s err=%r", sym, opp_dir, e)





def _delta_thr_h1(sym: str, thr1: float) -> float:
    """
    Minimum change in 1h forecast (in percent) before we treat it as a fresh
    opportunity signal.

    Default rule:
    - 0.5 * room threshold, but not less than 0.10%.
      e.g. if room_thr_h1 = 0.4%, delta_thr = 0.20%;
           if room_thr_h1 = 1.0%, delta_thr = 0.50%.
    """
    try:
        base = float(thr1)
    except (TypeError, ValueError):
        base = 1.0
    # optional env override per symbol if you ever need it
    env_key = f"XTREND_DELTA_THR_{(sym or '').upper()}"
    env_val = os.getenv(env_key)
    if env_val:
        try:
            return float(env_val)
        except ValueError:
            pass
    # default: half of room, floored at 0.10%
    return max(0.2, 0.5 * base)


def _compute_opp_score(sym: str, row: dict, m1: float | None, thr1: float) -> float:
    """
    Composite opportunity score on a 0-100 scale.

    Components / max weight:
      - Room vs threshold (H1 expected move)             35
      - Trend alignment ST + HT                          30
      - Model probability (ProbUp distance from 0.5)     10
      - Volume (RVOL vs min_rvol)                        10
      - Volatility vs spread (target size / spread)      5
      - Liquidity zone (SR alignment + proximity)        10

    If some components are missing (no RVOL / SR, etc.) they simply
    contribute 0 and we fall back to room + trend + prob.
    """
    # -------- basic helpers ----------
    def _sf(v, default=0.0) -> float:
        try:
            return float(v)
        except (TypeError, ValueError):
            return float(default)

    def _sfn(v):
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    # -------- 0) basic sanity on move / threshold --------
    move = _sfn(m1)
    if move is None or move == 0:
        return 0.0

    base_thr = _sfn(thr1)
    if base_thr is None or base_thr <= 0.0:
        return 0.0

    direction = 1.0 if move > 0 else -1.0
    abs_move = abs(move)

    # Per-symbol meta (spread, min_rvol, macro config, etc.)
    meta = _get_meta(sym) or {}
    min_rvol = _sf(meta.get("min_rvol", 1.0), 1.0)
    base_spread_bp = _sf(meta.get("spread_bp", 0.0), 0.0)

    # =========================================================
    # 1) Room score (035)
    # =========================================================
    # ratio = 1.0 -> just at threshold -> 0 pts
    # ratio = 2.0 or more -> full weight
    ratio = abs_move / base_thr
    if ratio <= 1.0:
        room_score = 0.0
    elif ratio >= 2.0:
        room_score = 35.0
    else:
        room_score = 35.0 * (ratio - 1.0)  # linear 1..2 -> 0..35

    # =========================================================
    # 2) Trend alignment ST/HT (030)
    # =========================================================
    st_tr = _sf(row.get("st_trend_score"), 0.0)
    ht_tr = _sf(row.get("ht_trend_score"), 0.0)

    # For longs we like positive scores, for shorts negative.
    support = 0.0
    if direction > 0:
        support += max(st_tr, 0.0) + max(ht_tr, 0.0)
    else:
        support += max(-st_tr, 0.0) + max(-ht_tr, 0.0)

    # st_tr/ht_tr are already in [-1, 1], so support in [0, 2]
    # 0 -> 0, 2 -> 30
    trend_score = max(0.0, min(30.0, 15.0 * support))

    # =========================================================
    # 3) Probability confidence (010)
    # =========================================================
    p_up_val = _sfn(row.get("p_up", row.get("prob_up")))
    if p_up_val is None:
        p_up_val = 0.5

    spread_p = abs(p_up_val - 0.5)  # 0..0.5
    if spread_p <= 0.05:
        prob_score = 0.0
    elif spread_p >= 0.20:
        prob_score = 10.0
    else:
        prob_score = 10.0 * (spread_p - 0.05) / 0.15

    # =========================================================
    # 4) Volume (RVOL) (010)
    # =========================================================
    # Try a few places: flattened or inside extra_h1/features.
    rvol_val = None
    if isinstance(row.get("feat_rvol15"), (int, float)):
        rvol_val = _sfn(row.get("feat_rvol15"))
    else:
        extra_h1 = row.get("extra_h1")
        if isinstance(extra_h1, dict):
            feats = extra_h1.get("features") if isinstance(extra_h1.get("features"), dict) else extra_h1
            rv = feats.get("feat_rvol15") if isinstance(feats, dict) else None
            rvol_val = _sfn(rv)

    volume_score = 0.0
    if rvol_val is not None and rvol_val > 0 and min_rvol > 0:
        rv_ratio = rvol_val / min_rvol
        # Below ~0.7x baseline: no score (too quiet)
        # Around 12x: good participation
        # Very extreme >3x: cap
        if rv_ratio <= 0.7:
            volume_score = 0.0
        elif rv_ratio >= 3.0:
            volume_score = 10.0
        else:
            # 0.7 -> 0, 1.0 -> ~4, 2.0 -> ~8, 3.0 -> 10
            volume_score = 10.0 * (rv_ratio - 0.7) / (3.0 - 0.7)

    # =========================================================
    # 5) Volatility vs spread (05)
    # =========================================================
    # Use target_pips (approx ATR * multiplier) vs spread in bp.
    target_pips = _sfn(row.get("target_pips"))
    vol_score = 0.0
    if target_pips is not None and target_pips > 0 and base_spread_bp > 0:
        vol_ratio = target_pips / base_spread_bp
        # If target is barely larger than spread, opportunity is weak.
        if vol_ratio <= 1.5:
            vol_score = 0.0
        elif vol_ratio >= 4.0:
            vol_score = 5.0
        else:
            # 1.5 -> 0, 4.0 -> 5
            vol_score = 5.0 * (vol_ratio - 1.5) / (4.0 - 1.5)

    # =========================================================
    # 6) Liquidity zone / SR alignment (010)
    # =========================================================
    sr = row.get("sr")
    sr_score = 0.0
    if isinstance(sr, dict):
        # We expect something like:
        # sr["nearest"] = {"kind": "support"/"resistance", "distance_pct": ...}
        nearest = sr.get("nearest") or sr.get("nearest_zone") or {}
        if isinstance(nearest, dict):
            kind = str(nearest.get("kind") or nearest.get("side") or "").lower()
            dist_pct = _sfn(nearest.get("distance_pct") or nearest.get("dist_pct"), 0.0)

            if dist_pct > 0.0:
                # Proximity: best if we are ~0.150.8% away from level
                if dist_pct < 0.05:
                    prox = 0.4   # sitting right on the level -> noisy
                elif dist_pct <= 0.8:
                    prox = 1.0
                elif dist_pct <= 1.5:
                    prox = 0.7
                else:
                    prox = 0.4   # too far, level less relevant

                # Alignment: longs prefer support, shorts prefer resistance.
                align = 0.5  # neutral if we can't decide
                if direction > 0:   # UP
                    if kind == "support":
                        align = 1.0
                    elif kind == "resistance":
                        align = 0.0
                else:               # DOWN
                    if kind == "resistance":
                        align = 1.0
                    elif kind == "support":
                        align = 0.0

                sr_score = 10.0 * prox * align

    # =========================================================
    # Combine everything
    # =========================================================
    total = (
        room_score
        + trend_score
        + prob_score
        + volume_score
        + vol_score
        + sr_score
    )

    if total < 0.0:
        total = 0.0
    if total > 100.0:
        total = 100.0
    return total

def _sign(v: float | None) -> int:
    """Return +1 / -1 / 0 for a numeric value."""
    try:
        x = float(v)
    except (TypeError, ValueError):
        return 0
    if x > 0:
        return 1
    if x < 0:
        return -1
    return 0

REG_MODEL: xgb.Booster | None = None
CLS_MODEL: xgb.Booster | None = None

BROKER_DIGITS = int(os.getenv("BROKER_DIGITS", "3"))  # set 3 if your XAU broker uses 3 digits
FORCE_TZ_OFFSET_MIN = os.getenv("FORCE_TZ_OFFSET_MIN")  # e.g., "0", "120", "180"

# Make sure this matches the writer (routes_devices.py)
REDIS_URL = os.getenv("REDIS_URL", "redis://default:xau12345@10.0.0.132:6379/0")
R = redis.from_url(REDIS_URL, decode_responses=True)
log.info(f"[TREND]  module={__file__}")
log.info(f"[TREND] REDIS_URL={REDIS_URL}")

# --------------------------
# BOT STATE (per-user, Redis)
# --------------------------

BOT_STATE_PREFIX = "xtl:bot:state:"  # key = xtl:bot:state:{user_id}


def _bot_state_key(user_id: str | None) -> str:
    uid = (user_id or "").strip() or "anon"
    return f"{BOT_STATE_PREFIX}{uid}"


def _default_bot_state() -> dict[str, Any]:
    # Single primary bot for now; can be extended later to multi-bot.
    now_ms = int(time.time() * 1000)
    return {
        "enabled": False,                  # auto-trading off by default
        "strategy_type": "opportunity",    # "indicator" | "priceAction" | "opportunity"
        "config": {},                      # raw config blob from Strategy / My Bots UI
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

    # Merge with defaults so new fields appear automatically.
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


# Store last-perceived H1 move in Redis so we can compute prediction delta
PRED_DELTA_KEY_FMT = "xtl:trend:last_move_pct_h1:%s"



#def _write_alert_history(row: dict):
    # prepend or append depending on UI preferences
    #R.lpush("opp:history", json.dumps(row))
    #R.ltrim("opp:history", 0, 200)


def _delete_live_snapshot(sym: str, opp_dir: str | None = None):
    """
    Remove the live per-symbol snapshot from Redis.

    If opp_dir is None, delete both directions + legacy key.
    """
    try:
        # Legacy key (cleanup)
        R.delete(f"opp:snap:{sym}")

        # New per-direction snapshots
        if opp_dir:
            R.delete(_opp_snapshot_key(sym, opp_dir))
        else:
            for d in ("UP", "DOWN"):
                R.delete(_opp_snapshot_key(sym, d))
    except Exception as e:
        log.warning("[OPP] _delete_live_snapshot failed sym=%s err=%r", sym, e)



# --------------------------
# ALERT HISTORY HELPERS (Redis)
# --------------------------


ALERT_HASH_PREFIX = "xtl:trend:opp:h1:"
ALERT_INDEX_KEY = "xtl:trend:opp:h1:index"




def _save_alert_snapshot(symbol: str, payload: dict[str, Any]) -> str:
    sym = (symbol or payload.get("symbol") or "").upper()
    direction = str(
        payload.get("opp_direction") or payload.get("direction") or ""
    ).upper()

    alert_id = str(payload.get("alert_id") or "").strip()

    if not alert_id:
        ts = int(payload.get("alert_created_ms") or int(time.time() * 1000))
        alert_id = f"{ts}:{sym}:{direction or 'NA'}"
        payload["alert_id"] = alert_id

    # ? REQUIRED
    if "alert_created_ms" not in payload:
        payload["alert_created_ms"] = int(time.time() * 1000)

    # ? RECOMMENDED
    if "status" not in payload:
        payload["status"] = "active"

    key = f"{ALERT_HASH_PREFIX}{alert_id}"

    try:
        mapping = {k: json.dumps(v) for k, v in payload.items()}
        R.hset(key, mapping=mapping)
        R.lrem(ALERT_INDEX_KEY, 0, alert_id)
        R.lpush(ALERT_INDEX_KEY, alert_id)
        R.ltrim(ALERT_INDEX_KEY, 0, 99)
    except Exception as e:
        log.warning("[OPP] _save_alert_snapshot failed id=%s err=%r", alert_id, e)

    return alert_id

def _load_opp_history(limit: int = 50) -> list[dict[str, Any]]:
    """
    Return the latest *completed* alert history from Redis LIST index.

    Reads from xtl:trend:opp:h1:index and per-alert hashes
    xtl:trend:opp:h1:{alert_id}, but only returns alerts whose status
    is "hit" or "expired".
    """
    out: list[dict[str, Any]] = []

    try:
        ids = R.lrange(ALERT_INDEX_KEY, 0, max(0, limit - 1))
    except Exception as e:
        log.warning("[OPP] _load_opp_history index read failed err=%r", e)
        return out

    seen_ids: set[str] = set()

    for raw_id in ids:
        if not raw_id:
            continue

        aid = raw_id.decode("utf-8", "ignore") if isinstance(raw_id, bytes) else str(raw_id)
        aid = aid.strip()
        if not aid or aid in seen_ids:
            continue
        seen_ids.add(aid)

        key = f"{ALERT_HASH_PREFIX}{aid}"
        try:
            h = R.hgetall(key)
        except Exception as e:
            log.warning("[OPP] _load_opp_history hgetall failed key=%s err=%r", key, e)
            continue

        if not h:
            continue

        decoded: dict[str, Any] = {}
        for k, v in h.items():
            k_dec = k.decode("utf-8", "ignore") if isinstance(k, bytes) else str(k)
            v_str = v.decode("utf-8", "ignore") if isinstance(v, bytes) else str(v)
            try:
                decoded[k_dec] = json.loads(v_str)
            except Exception:
                decoded[k_dec] = v_str

        decoded["alert_id"] = aid

        # ---------- UI normalization ----------

        # alert_time_ms (UI expects this)
        try:
            decoded["alert_time_ms"] = int(
                decoded.get("alert_time_ms")
                or decoded.get("alert_created_ms")
                or 0
            )
        except Exception:
            decoded["alert_time_ms"] = 0

        # expected_move_pct (distance only; legacy fields allowed)
        try:
            decoded["expected_move_pct"] = float(
                decoded.get("expected_move_pct")
                or decoded.get("expected_move_pct_1h")
                or decoded.get("opp_expected_move_pct_1h")
                or 0.0
            )
        except Exception:
            decoded["expected_move_pct"] = 0.0

        # ---------- horizon_min (NO hard-coded default) ----------
        hmin = decoded.get("horizon_min")
        if hmin is None:
            try:
                alert_ms = int(decoded.get("alert_created_ms") or 0)
                expire_ms = int(decoded.get("opp_expire_ts") or 0)
                if alert_ms > 0 and expire_ms > alert_ms:
                    decoded["horizon_min"] = (expire_ms - alert_ms) // 60_000
                else:
                    decoded["horizon_min"] = None
            except Exception:
                decoded["horizon_min"] = None
        else:
            try:
                decoded["horizon_min"] = int(hmin)
            except Exception:
                decoded["horizon_min"] = None

        # direction
        if "direction" not in decoded:
            decoded["direction"] = decoded.get("opp_direction") or decoded.get("decision")

        # status
        st = decoded.get("status") or "active"
        decoded["status"] = str(st).lower()

        # hit_target (UI convenience)
        if "hit_target" not in decoded:
            if decoded["status"] == "hit":
                decoded["hit_target"] = True
            elif decoded["status"] == "expired":
                decoded["hit_target"] = False
            else:
                decoded["hit_target"] = None

        # only completed alerts in history
        if decoded["status"] not in ("hit", "expired"):
            continue

        # defaults
        decoded.setdefault("realized_move_pct", None)
        decoded.setdefault("max_drawdown_pct", None)
        decoded.setdefault("expired_ts", None)
        decoded.setdefault("hit_ts", None)

        out.append(decoded)

    out.sort(key=lambda d: d.get("alert_created_ms") or 0, reverse=True)
    return out


# --------------------------
# ALERT STATUS UPDATE HELPERS
# --------------------------

def _mark_alert_hit(alert_id: str, realized_move_pct: float, now_ms: int):
    """Mark a stored alert as hit."""
    key = f"xtl:trend:opp:h1:{alert_id}"
    try:
        if not R.exists(key):
            return

        R.hset(key, mapping={
            "status": json.dumps("hit"),
            "hit_ts": json.dumps(now_ms),
            "realized_move_pct": json.dumps(realized_move_pct),
        })
    except Exception as e:
        log.warning("[OPP] _mark_alert_hit failed id=%s err=%r", alert_id, e)


def _mark_alert_expired(alert_id: str, now_ms: int):
    """Mark a stored alert as expired (time horizon reached)."""
    key = f"xtl:trend:opp:h1:{alert_id}"
    try:
        if not R.exists(key):
            return

        R.hset(key, mapping={
            "status": json.dumps("expired"),
            "expired_ts": json.dumps(now_ms),
        })
    except Exception as e:
        log.warning("[OPP] _mark_alert_expired failed id=%s err=%r", alert_id, e)



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
        if a < 0.10:   # < 0.10% -> 3 dp
            return 3
    return 2


def _price_decimals(sym: str) -> int:
    s = sym.upper()
    if s.endswith("JPY"):
        return 3
    if s == "XAUUSD":
        return 2
    return 5


def _pip(sym: str) -> float:
    s = sym.upper()
    if s == "XAUUSD":
        return 0.1
    if s.endswith("JPY"):
        return 0.01
    return 0.0001

def _round_pct(sym: str, v: float) -> float:
    return round(float(v), _pct_decimals(sym, float(v)))



def _normalize_pct(sym: str, v: float | None) -> float | None:
    """
    Ensure v is in PERCENT units.
    If a fractional input sneaks in (for example 0.0004 meaning 0.04%),
    scale it to percent. Then apply a symbol-specific stretch
    for XAUUSD so that displayed moves are closer to historical reality.
    """
    if not isinstance(v, (int, float)):
        return None

    x = float(v)

    # 1) If the model gave a fraction (0.0004 => 0.04%), convert to percent
    if abs(x) < 1.0:
        x *= 100.0

    S = sym.upper()
    if S == "XAUUSD":
        # Quick-fix stretch for gold:
        # median |move_1h_pct| ~ 0.16, current preds ~ 0.02 ? ~8x too small
        x *= 8.0

    return _round_pct(sym, x)



def _build_reasons(sym: str, label: str, p_up: float, extra: Dict[str, Any]) -> List[str]:
    """
    Build human-readable reasons for the dashboard / prediction meter.

    Inputs:
      sym   - symbol (for example XAUUSD)
      label - trend label from detection ("Bullish", "Bearish", "Strong Bullish", etc.)
      p_up  - model probability of up move
      extra - feature bag; expected keys (if available):
              - tf_scope: "H1" or "H4" (for ST vs HT reasons)
              - base_reasons: List[str] from infer_rt / rule engine
              - feat_rvol15: float, relative volume on M15
              - feat_usd_basket: float, USD basket tilt
              - macro_dxy_z: float, DXY z-score
              - macro_yield_z: float, 10Y yield z-score
              - macro_usd_rate_z: float, short-rate z-score
              - macro_vix_z: float, VIX z-score
    """
    reasons: List[str] = []

    # per-symbol macro config from symbol_meta.json, if present
    meta = _get_meta(sym)
    macro_cfg = (meta.get("macro") or {}) if isinstance(meta, dict) else {}

    # Which timeframe are we explaining? (H1 = ST, H4 = HT)
    tf_scope = (extra.get("tf_scope") or "").upper()
    if tf_scope == "H4":
        tf_txt = "4h"
    elif tf_scope == "H1":
        tf_txt = "1h"
    else:
        tf_txt = "1h/4h"

    # 0) Base reasons from upstream (infer_rt / rule engine), if any
    base_reasons: List[str] = []
    br = extra.get("base_reasons")
    if isinstance(br, list):
        base_reasons = [str(r) for r in br if isinstance(r, str) and r.strip()]
    elif isinstance(br, str) and br.strip():
        base_reasons = [br.strip()]

    if base_reasons:
        reasons.extend(base_reasons)

    lbl = (label or "").lower()

    # 1) Structure reason (explicitly tag H1 or H4)
    if "bullish" in lbl or "bearish" in lbl:
        strong = "strong " if "strong" in lbl else ""
        dir_txt = "bullish" if "bullish" in lbl else "bearish"
        struct_reason = f"{strong.capitalize()}{dir_txt} {tf_txt} structure"
        reasons.append(struct_reason)

    # 2) Relative volume (if present)
    rvol = extra.get("feat_rvol15")
    if isinstance(rvol, (int, float)):
        try:
            rv = float(rvol)
            if rv > 1.3:
                reasons.append(f"RVOL ~{rv:.1f}x (elevated)")
            elif rv < 0.7:
                reasons.append(f"RVOL ~{rv:.1f}x (low)")
        except Exception:
            pass

    # 3) USD basket / macro tone
    usd_tilt = extra.get("feat_usd_basket")
    if isinstance(usd_tilt, (int, float)):
        try:
            ut = float(usd_tilt)
            if abs(ut) >= 0.2:
                tone = "supportive" if ut < 0 else "headwind"
                reasons.append(f"USD tone {tone} ({ut:+.2f}%)")
        except Exception:
            pass

    # 4) Macro z-scores (optional, only if configured for symbol)
    dxy_z = extra.get("macro_dxy_z")
    if isinstance(dxy_z, (int, float)) and macro_cfg.get("use_dxy"):
        try:
            dz = float(dxy_z)
            if abs(dz) >= 1.0:
                side = "risk-off" if dz > 0 else "risk-on"
                reasons.append(f"DXY {side} (z={dz:+.1f})")
        except Exception:
            pass

    vix_z = extra.get("macro_vix_z")
    if isinstance(vix_z, (int, float)) and macro_cfg.get("use_vix"):
        try:
            vz = float(vix_z)
            if abs(vz) >= 1.0:
                tone = "higher volatility" if vz > 0 else "calmer volatility"
                reasons.append(f"VIX signals {tone} (z={vz:+.1f})")
        except Exception:
            pass

    # 5) Model confidence wording (probability)
    if isinstance(p_up, (int, float)):
        try:
            pu = float(p_up)
            if pu >= 0.7:
                reasons.append(f"Model up-bias (ProbUp {pu:.2f})")
            elif pu <= 0.3:
                reasons.append(f"Model down-bias (ProbUp {pu:.2f})")
        except Exception:
            pass

    return reasons

def _compute_weighted_status(
    sym: str,
    tech_score: float | None,
    p_up: float | None,
    extra: Dict[str, Any] | None,
) -> Tuple[float, str, float, float, float]:
    """
    Combine technical trend, model probability and macro backdrop
    into a single [-1, 1] score and label bucket.

    Returns:
        combined_score, label, tech_component, model_component, macro_component
    """
    # 1) technical component (already in [-1, 1] from trend engine)
    try:
        t = float(tech_score)
    except Exception:
        t = 0.0
    t = max(min(t, 1.0), -1.0)

    # 2) model component: map p_up in [0,1] to [-1,1]
    m = 0.0
    if isinstance(p_up, (int, float)):
        m = float(p_up)
        m = max(min(m, 1.0), 0.0)
        # 0.5 -> 0, 1.0 -> +1, 0.0 -> -1
        m = (m - 0.5) * 2.0

    # 3) macro component from per-symbol config and macro z-scores
    macro_val = 0.0
    if isinstance(extra, dict):
        meta = _get_meta(sym)
        macro_cfg = (meta.get("macro") or {}) if isinstance(meta, dict) else {}

        agg = 0.0
        w_sum = 0.0

        def _add_macro(key_cfg: str, extra_key: str) -> None:
            nonlocal agg, w_sum
            if extra_key not in extra:
                return
            try:
                z = float(extra.get(extra_key))
            except Exception:
                return

            cfg = macro_cfg.get(key_cfg) or {}
            sign = float(cfg.get("sign", 1.0))
            weight = float(cfg.get("weight", 0.0))
            z_on = float(cfg.get("z_on", 0.0))

            if weight == 0.0:
                return

            # ignore tiny moves; treat half of z_on as "small"
            if abs(z) < max(0.3, z_on * 0.5):
                return

            # squash z to [-1, 1] by dividing by 3 sigmas
            z_norm = max(min(z / 3.0, 1.0), -1.0)
            contrib = sign * z_norm * weight
            agg += contrib
            w_sum += abs(weight)

        # DXY, 10Y, short rate, VIX
        _add_macro("dxy", "macro_dxy_z")
        _add_macro("yield", "macro_yield_z")
        _add_macro("usd_rate", "macro_usd_rate_z")
        _add_macro("vix", "macro_vix_z")

        # Optional: RVOL as macro-style driver (deviation from 1.0x)
        rv = extra.get("feat_rvol15")
        try:
            rv_val = float(rv) if rv is not None else None
        except Exception:
            rv_val = None

        if isinstance(rv_val, (int, float)):
            cfg = macro_cfg.get("rvol") or {}
            sign = float(cfg.get("sign", 1.0))
            weight = float(cfg.get("weight", 0.0))
            if weight != 0.0:
                # deviation from 1.0, scaled so +/- 1.5x maps near +/-1
                delta = rv_val - 1.0
                if abs(delta) >= 0.1:
                    rv_norm = max(min(delta / 1.5, 1.0), -1.0)
                    agg += sign * rv_norm * weight
                    w_sum += abs(weight)

        if w_sum > 0.0:
            macro_val = max(min(agg / w_sum, 1.0), -1.0)

    # Weights for components
    W_TECH = 0.5
    W_MODEL = 0.3
    W_MACRO = 0.2

    combined = (W_TECH * t) + (W_MODEL * m) + (W_MACRO * macro_val)
    combined = max(min(combined, 1.0), -1.0)

    # Map to label buckets
    if combined >= 0.6:
        lbl = "Strong Bullish"
    elif combined >= 0.2:
        lbl = "Bullish"
    elif combined <= -0.6:
        lbl = "Strong Bearish"
    elif combined <= -0.2:
        lbl = "Bearish"
    else:
        lbl = "Neutral"

    return combined, lbl, t, m, macro_val



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

OPP_LOG = PRED_LOG.with_name("opportunities_log.csv")


def _log_opportunity(row: dict) -> None:
    """
    Append one opportunity row to opportunities_log.csv.

    This lets you later evaluate:
    - when it appeared (opp_open_ts)
    - when it expired (opp_expire_ts)
    - whether the realized move hit the expected room.
    """
    try:
        is_new = not OPP_LOG.exists()
        with OPP_LOG.open("a", newline="") as f:
            w = csv.writer(f)
            if is_new:
                w.writerow([
                    "opp_open_ts",          # when we surfaced this opportunity
                    "symbol",
                    "opp_direction",       # "UP"/"DOWN"
                    "opp_confidence",      # "high"/"medium"
                    "expected_move_pct_1h",
                    "target_price_1h",
                    "basis_price_1h",
                    "opp_min_room_h1",
                    "opp_min_room_h4",
                    "opp_h4_agree",        # True/False/None
                    "opp_expire_ts",       # when the 1h horizon ends
                    "target_close_ts",     # same as opp_expire_ts (for now)
                    "decision",            # BUY/SELL/ABSTAIN from headline
                ])
            w.writerow([
                int(row.get("opp_open_ts", 0)),
                row.get("symbol", ""),
                row.get("opp_direction", ""),
                row.get("opp_confidence", ""),
                float(row.get("expected_move_pct_1h", 0.0)),
                float(row.get("target_price_1h", 0.0))
                    if row.get("target_price_1h") not in (None, "") else "",
                float(row.get("basis_price_1h", 0.0))
                    if row.get("basis_price_1h") not in (None, "") else "",
                float(row.get("opp_min_room_h1", 0.0)),
                float(row.get("opp_min_room_h4", 0.0)),
                row.get("opp_h4_agree", ""),
                int(row.get("opp_expire_ts", 0)),
                int(row.get("target_close_ts", 0)),
                row.get("decision", ""),
            ])
    except Exception:
        # logging must never break API
        pass


def _sweep_opp_snapshots(symbols_csv: str, now_ms: int) -> None:
    syms = []
    for s in (symbols_csv or "").split(","):
        s = s.strip().upper()
        if s:
            syms.append(s)

    for sym in syms:
        for d in ("UP", "DOWN"):
            try:
                snap_key = _opp_snapshot_key(sym, d)
                snap = R.hgetall(snap_key) or {}
                if not snap:
                    continue

                # evaluate (may mark hit/expired + may delete)
                _evaluate_alert_outcome(sym, snap, {}, now_ms)

                # re-read snapshot status AFTER evaluation
                snap2 = R.hgetall(snap_key) or {}
                if not snap2:
                    continue

                raw_status = snap2.get("status")
                if isinstance(raw_status, (bytes, bytearray)):
                    raw_status = raw_status.decode("utf-8", "ignore")
                try:
                    st = json.loads(raw_status) if raw_status is not None else "active"
                except Exception:
                    st = str(raw_status or "active")

                if st in ("hit", "expired"):
                    _delete_live_snapshot(sym, d)

            except Exception:
                pass

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
                # tolerate slight clock skews: nearest within +/- 1 min
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


# Lock short-term (H1) forecast per symbol+horizon so it does not flip every refresh
_ST_H1_LOCK: dict[str, dict[str, Any]] = {}

# Lock higher-timeframe (H4) forecast per symbol+horizon so it does not flip every refresh
_HT_H4_LOCK: dict[str, dict[str, Any]] = {}


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
            # ^ this may pick any device; we will expose which one below if your helper sets it,
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

@router.get("/predict/ping")
def predict_ping():
    return {"ok": True, "msg": "predict router alive"}



@router.get("/predict/all")
def predict_all(
    tf: str = "M15",
    symbols: str = "XAUUSD,EURUSD,USDJPY,GBPUSD,USDCAD,USDCHF",
    device: str | None = Query(None),
    x_device_id: str | None = Header(None, convert_underscores=False),
    user = Depends(require_auth_optional),
):
    """
    Main prediction feed.

    ST trend = 1-hour structure (H1) + H1 model + macro
    HT trend = 4-hour structure (H4) + H4 model + macro

    Returns:
      - expected_move_pct_1h / target_price_1h from H1 model (if available)
      - expected_move_pct_4h / target_price_4h from H4 model (if available)
      - reasons_h1 / reasons_h4 (separate) + reasons (H1 for backward compat)
      - updated_broker_ts (server timestamp) + broker tz info if available
    """

    tfu = (tf or "M15").upper()
    syms = [s.strip().upper() for s in (symbols or "").split(",") if s.strip()]

    # prefer user's device if present; fall back to freshest-any for OHLC snapshots
    user_id = getattr(user, "user_id", None) if user else None
    now_ms = int(_time.time() * 1000)

    # ---- imports kept inside to avoid startup import failures ----
    # H1/H4 inference
    try:
        from api.trend.infer_rt import predict_next_hour, predict_next_4h, pull_latest_h1, pull_latest_h4
    except Exception:
        try:
            # fallback (older layout)
            from .infer_rt import predict_next_hour, predict_next_4h, pull_latest_h1, pull_latest_h4
        except Exception:
            predict_next_hour = None  # type: ignore
            predict_next_4h = None    # type: ignore
            pull_latest_h1 = None     # type: ignore
            pull_latest_h4 = None     # type: ignore

    # TTH inference (dynamic horizon)
    try:
        from api.trend.infer_tth import predict_tth  # preferred
    except Exception:
        try:
            from ml.infer_tth import predict_tth  # user stated infer_tth is under /ml
        except Exception:
            predict_tth = None  # type: ignore

    # Macro snapshot once per request (shared by H1/H4)
    try:
        macro = get_macro_snapshot()
    except Exception:
        macro = None

    # build frames once per request to reduce redis scans inside infer_rt
    now_frames_h1 = None
    now_frames_h4 = None
    try:
        if callable(pull_latest_h1):
            need_syms = ["XAUUSD", "EURUSD", "GBPUSD", "AUDUSD", "USDJPY", "USDCHF", "USDCAD"]
            now_frames_h1 = {s: pull_latest_h1(s) for s in need_syms}
        if ENABLE_H4_MODEL and callable(pull_latest_h4):
            need_syms = ["XAUUSD", "EURUSD", "GBPUSD", "AUDUSD", "USDJPY", "USDCHF", "USDCAD"]
            now_frames_h4 = {s: pull_latest_h4(s) for s in need_syms}
    except Exception:
        now_frames_h1 = None
        now_frames_h4 = None

    rows: list[dict] = []

    TF_MS_LOCAL = {"M15": 15 * 60_000, "H1": 60 * 60_000, "H4": 4 * 60 * 60_000}
    tf_ms = TF_MS_LOCAL.get(tfu, 15 * 60_000)

    def _score_to_label(s: float) -> str:
        if s >= 0.6:
            return "Strong Bullish"
        if s >= 0.2:
            return "Bullish"
        if s <= -0.6:
            return "Strong Bearish"
        if s <= -0.2:
            return "Bearish"
        return "Neutral"

    def _safe_p_up(pr: dict, fallback: float = 0.5) -> float:
        try:
            return float(pr.get("p_up", pr.get("probUp", fallback)))
        except Exception:
            return fallback

    def _safe_move_pct(pr: dict) -> float:
        # treat move_pct / predMovePct as already in PERCENT
        raw = pr.get("move_pct", pr.get("predMovePct"))
        try:
            return abs(float(raw)) if raw is not None else 0.0
        except Exception:
            return 0.0

    for sym in syms:
        # --- A) H1/H4 model inference -------------------------------------
        pr_h1: dict = {"ok": False, "reason": "h1_not_loaded"}
        pr_h4: dict = {"ok": False, "reason": "h4_not_loaded"}

        if callable(predict_next_hour):
            try:
                pr_h1 = predict_next_hour(sym, now_frames=now_frames_h1)  # type: ignore[arg-type]
            except Exception as e:
                log.exception("[predict_all] predict_next_hour EXC sym=%s", sym)
                pr_h1 = {"ok": False, "reason": "infer_exc_h1", "detail": str(e)}
        else:
            pr_h1 = {"ok": False, "reason": "infer_rt_missing_h1"}

        if ENABLE_H4_MODEL:
            if callable(predict_next_4h):
                try:
                    pr_h4 = predict_next_4h(sym, now_frames=now_frames_h4)  # type: ignore[arg-type]
                except Exception as e:
                    log.exception("[predict_all] predict_next_4h EXC sym=%s", sym)
                    pr_h4 = {"ok": False, "reason": "infer_exc_h4", "detail": str(e)}
            else:
                pr_h4 = {"ok": False, "reason": "infer_rt_missing_h4"}
        else:
            pr_h4 = {"ok": False, "reason": "h4_disabled"}

        if not isinstance(pr_h1, dict):
            pr_h1 = {"ok": False, "reason": "infer_not_dict_h1"}
        if not isinstance(pr_h4, dict):
            pr_h4 = {"ok": False, "reason": "infer_not_dict_h4"}

        ok_h1 = bool(pr_h1.get("ok", False))
        ok_h4 = bool(pr_h4.get("ok", False))

        
        # --- B) Dynamic horizon (TTH) -------------------------------------
        # If TTH is OK, choose horizon from bucket_idx (NOT tth.horizon_min which is 0 in your logs)
        horizon_min = 60
        p_dir = None
        tth_raw = None

        TTH_BUCKET_MIN = [15, 30, 60, 120, 240]  # tune later

        if callable(predict_tth):
            try:
                tth = predict_tth(sym)  # type: ignore[misc]
                tth_raw = tth
                if isinstance(tth, dict) and tth.get("ok"):
                    bidx = tth.get("bucket_idx", None)
                    try:
                       bidx_i = int(bidx) if bidx is not None else 2  # default to 60m
                    except Exception:
                       bidx_i = 2
                    bidx_i = max(0, min(bidx_i, len(TTH_BUCKET_MIN) - 1))
                    horizon_min = int(TTH_BUCKET_MIN[bidx_i])

                    # directional probability summary (optional)
                    try:
                       p_dir = max(float(tth.get("p_up", 0.0)), float(tth.get("p_down", 0.0)))
                    except Exception:
                       p_dir = None
                # else keep fallback 60m
            except Exception as e:
                log.exception("[predict_all] predict_tth EXC sym=%s", sym)
                tth_raw = {"ok": False, "reason": "tth_exc", "detail": str(e)}

        

        # --- C) Latest OHLC + broker meta ---------------------------------
        snap, broker = _read_freshest_snap_for_user_or_any(user_id, sym, tfu)
        bars = (snap or {}).get("bars") or []

        last_closed = None
        for b in reversed(bars):
            t_ms = _ms_from_t(b.get("t_open_ms") or b.get("t"))
            if t_ms is None:
                continue
            is_closed = (b.get("complete") is True) or (t_ms + tf_ms <= now_ms)
            if is_closed:
                last_closed = {**b, "t_open_ms": t_ms}
                break

        # basis for targets: prefer M1 close if present, else last_closed close, else model lastClose
        last_price_m1 = None
        try:
            snap_m1, _ = _read_freshest_snap_for_user_or_any(user_id, sym, "M1")
        except Exception:
            snap_m1 = None

        bars_m1 = (snap_m1 or {}).get("bars") or []
        for b in reversed(bars_m1):
            c = b.get("c")
            if isinstance(c, (int, float)):
                last_price_m1 = float(c)
                break

        last_close = None
        if isinstance(pr_h1.get("lastClose"), (int, float)):
            last_close = float(pr_h1["lastClose"])
        elif isinstance(pr_h4.get("lastClose"), (int, float)):
            last_close = float(pr_h4["lastClose"])
        elif isinstance(last_closed, dict) and isinstance(last_closed.get("c"), (int, float)):
            last_close = float(last_closed["c"])

        price_for_targets = last_price_m1 if isinstance(last_price_m1, (int, float)) else last_close

        # broker tz for horizon timestamps
        off_min = 0
        try:
            off_min = int((broker or {}).get("tz_offset_min") or 0)
        except Exception:
            off_min = 0

        target_close_ts_h1 = _next_boundary_ms(60 * 60, now_ms, off_min)
        target_close_ts_4h = _next_boundary_ms(4 * 60 * 60, now_ms, off_min)
        # dynamic target close based on TTH-selected horizon
        target_close_ts_dyn = _next_boundary_ms(int(horizon_min) * 60, now_ms, off_min)

        # --- D) build expected moves + targets ----------------------------
        p_up_h1 = _safe_p_up(pr_h1, 0.5)
        p_up_h4 = _safe_p_up(pr_h4, p_up_h1)

        mag_pct_h1 = _safe_move_pct(pr_h1)
        mag_pct_h4 = _safe_move_pct(pr_h4)

        direction_sign_h1 = 1.0 if p_up_h1 >= 0.5 else -1.0
        direction_sign_h4 = 1.0 if p_up_h4 >= 0.5 else -1.0

        signed_pct_1h = mag_pct_h1 * direction_sign_h1
        signed_pct_4h = mag_pct_h4 * direction_sign_h4

        decimals = _price_decimals(sym)

        try:
            expected_move_pct_1h = round(float(signed_pct_1h), 2)
        except Exception:
            expected_move_pct_1h = 0.0

        try:
            expected_move_pct_4h = round(float(signed_pct_4h), 2)
        except Exception:
            expected_move_pct_4h = 0.0

        target_price_1h = None
        target_price_4h = None
        if isinstance(price_for_targets, (int, float)):
            try:
                target_price_1h = round(float(price_for_targets) * (1.0 + expected_move_pct_1h / 100.0), decimals)
            except Exception:
                target_price_1h = None
            try:
                target_price_4h = round(float(price_for_targets) * (1.0 + expected_move_pct_4h / 100.0), decimals)
            except Exception:
                target_price_4h = None

        # --- E) structure scores (tech-only) ------------------------------
        st_thr = 0.35
        ht_thr = 0.70
        st_tech = max(min((signed_pct_1h / st_thr) if st_thr else 0.0, 1.0), -1.0)
        ht_tech = max(min((signed_pct_4h / ht_thr) if ht_thr else 0.0, 1.0), -1.0)

        # --- F) reasons + weighted status ---------------------------------
        base_reasons_h1: list[str] = []
        r_raw = pr_h1.get("reasons") or pr_h1.get("reason")
        if isinstance(r_raw, list):
            base_reasons_h1 = [str(x) for x in r_raw if x]
        elif isinstance(r_raw, str) and r_raw:
            base_reasons_h1 = [str(r_raw)]

        base_reasons_h4: list[str] = []
        r_raw = pr_h4.get("reasons") or pr_h4.get("reason")
        if isinstance(r_raw, list):
            base_reasons_h4 = [str(x) for x in r_raw if x]
        elif isinstance(r_raw, str) and r_raw:
            base_reasons_h4 = [str(r_raw)]

        extra_h1: Dict[str, Any] = {
            "base_reasons": base_reasons_h1,
            "feat_rvol15": pr_h1.get("rvol15"),
            "feat_usd_basket": pr_h1.get("usd_basket_d1h_pct"),
            "tf_scope": "H1",
        }
        extra_h4: Dict[str, Any] = {
            "base_reasons": base_reasons_h4,
            "feat_rvol15": pr_h4.get("rvol15"),
            "feat_usd_basket": pr_h4.get("usd_basket_d1h_pct"),
            "tf_scope": "H4",
        }
        if isinstance(macro, dict):
            for d in (extra_h1, extra_h4):
                d["macro_dxy_z"] = macro.get("dxy_z")
                d["macro_yield_z"] = macro.get("us10y_z")
                d["macro_usd_rate_z"] = macro.get("usd_short_rate_z")
                d["macro_vix_z"] = macro.get("vix_z")

        st_combined, st_label_w, st_t, st_m, st_macro = _compute_weighted_status(sym, st_tech, p_up_h1, extra_h1)
        ht_combined, ht_label_w, ht_t, ht_m, ht_macro = _compute_weighted_status(sym, ht_tech, p_up_h4, extra_h4)

        if not st_label_w:
            st_label_w = _score_to_label(st_tech)
        if not ht_label_w:
            ht_label_w = _score_to_label(ht_tech)

        # headline selection
        if ENABLE_H4_MODEL and ok_h4:
            combined_score = ht_combined
            combined_label = ht_label_w
        else:
            combined_score = st_combined
            combined_label = st_label_w

        label = combined_label
        if label in ("Strong Bullish", "Bullish"):
            decision = "BUY"
        elif label in ("Strong Bearish", "Bearish"):
            decision = "SELL"
        else:
            decision = "ABSTAIN"

        spread = abs(p_up_h1 - 0.5)
        if spread >= 0.20:
            confidence = "high"
        elif spread >= 0.05:
            confidence = "medium"
        else:
            confidence = "low"

        row: Dict[str, Any] = {
            "symbol": sym,

            "label": label,
            "score": float(combined_score),
            "decision": decision,
            "confidence": confidence,

            "p_up": p_up_h1,
            "prob_up": p_up_h1,
            "prob_up_h1": p_up_h1,
            "prob_up_h4": p_up_h4,

            "st_trend_label": st_label_w,
            "st_trend_score": float(st_combined),
            "ht_trend_label": ht_label_w,
            "ht_trend_score": float(ht_combined),

            "st_tech_component": float(st_t),
            "st_model_component": float(st_m),
            "st_macro_component": float(st_macro),
            "ht_tech_component": float(ht_t),
            "ht_model_component": float(ht_m),
            "ht_macro_component": float(ht_macro),

            "expected_move_pct_1h": expected_move_pct_1h,
            "target_price_1h": target_price_1h,
            "expected_move_pct_4h": expected_move_pct_4h,
            "target_price_4h": target_price_4h,
            "basis_price_1h": price_for_targets,

            "horizon": f"{int(horizon_min)}m",
            "target_close_ts": target_close_ts_dyn,
            "update_tf": tfu,
            "server_now_ms": now_ms,
            "updated_broker_ts": now_ms,

            # dynamic horizon (from TTH) for UI expiry + future logic
            "horizon_min": int(horizon_min) if horizon_min is not None else 60,
            "tth_p_dir": p_dir,
            "tth_raw": tth_raw,

            "raw": pr_h1,
            "raw_h4": pr_h4,
        }
        # --- UI canonical fields (model-driven, no _1h suffix) ---
        row["basis_price"] = price_for_targets
        row["target_price"] = target_price_1h
        row["expected_move_pct"] = expected_move_pct_1h
        row["model_source"] = "ml" if ok_h1 else "na"


        if not ok_h1:
            row.setdefault("reason_h1_error", pr_h1.get("reason", "model_error_h1"))
        if not ok_h4:
            row.setdefault("reason_h4_error", pr_h4.get("reason", "model_error_h4"))

        reasons_h1 = _build_reasons(sym, st_label_w, p_up_h1, extra_h1)
        reasons_h4 = _build_reasons(sym, ht_label_w, p_up_h4, extra_h4)

        if reasons_h1:
            row["reasons_h1"] = reasons_h1
        if reasons_h4:
            row["reasons_h4"] = reasons_h4
        row["reasons"] = reasons_h1 or reasons_h4 or []

        # broker/device meta (if available)
        if isinstance(broker, dict):
            if "tz_offset_min" in broker:
                row["broker_tz_offset_min"] = broker.get("tz_offset_min")
                row["tz_offset_min"] = broker.get("tz_offset_min")
            if "tz_abbr" in broker:
                row["broker_tz_abbr"] = broker.get("tz_abbr")
        if isinstance(snap, dict) and "using_device" in snap:
            row["using_device"] = snap.get("using_device")

        rows.append(row)

    return {"ok": True, "tf": tfu, "rows": rows}

def _redis_get_text(key: str) -> str | None:
    try:
        v = R.get(key)
        if v is None:
            return None
        if isinstance(v, (bytes, bytearray)):
            v = v.decode("utf-8", "ignore")
        return str(v)
    except Exception:
        return None

def _redis_set_text(key: str, value: str, ttl_sec: int) -> None:
    try:
        R.setex(key, int(ttl_sec), value)
    except Exception:
        pass
SYSTEM_PROMPT = (
    "You are XauTrendLab Forecast Assistant. "
    "Only use the provided JSON. Do not invent prices, levels, or times. "
    "Return 2-4 short sentences: direction, target, time window, and why (from reasons)."
)

def call_llm_commentary(payload: dict) -> str:
    resp = client.chat.completions.create(
        model=os.getenv("XTL_COMMENTARY_MODEL", "gpt-4.1-mini"),
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
        ],
        temperature=0.2,
    )
    return (resp.choices[0].message.content or "").strip()


def build_commentary_payload(row: dict) -> dict:
    """
    Build a STRICT, LLM-safe commentary payload from ML prediction row.
    LLM must only narrate this data (no new numbers).
    """
    symbol = row.get("symbol")
    horizon = row.get("horizon", "H1")
    horizon_min = row.get("horizon_min")

    payload = {
        "instrument": symbol,
        "direction": row.get("decision"),
        "bias_label": row.get("label"),
        "confidence": row.get("confidence"),
        "horizon": f"{horizon_min} minutes" if horizon_min else horizon,
        "prices": {
            "basis_price": row.get("basis_price_1h"),
            "target_price": row.get("target_price_1h"),
            "expected_move_pct": row.get("expected_move_pct_1h"),
        },
        "structure": {
            "short_term": row.get("st_trend_label"),
            "higher_timeframe": row.get("ht_trend_label"),
        },
        "time_to_hit": {
            "directional_probability": row.get("tth_p_dir"),
            "p_up": (row.get("tth_raw") or {}).get("p_up"),
            "p_down": (row.get("tth_raw") or {}).get("p_down"),
            "target_close_ts": row.get("target_close_ts"),
        },
        "reasons": {
            "h1": row.get("reasons_h1", []) or [],
            "h4": row.get("reasons_h4", []) or [],
        },
        # keep this as “hint text only” until you wire real SR numbers
        "support_resistance_hint": {
            "support": "near recent intraday lows",
            "resistance": "near prior supply zone",
        },
        "meta": {
            "updated_broker_ts": row.get("updated_broker_ts"),
            "tz_offset_min": row.get("broker_tz_offset_min"),
            "model_version": "xtl-tth-v2",
        },
    }
    return payload

@router.get("/commentary")
@router.get("/trend/commentary")
def trend_commentary(
    symbol: str,
    tf: str = "H1",
    user = Depends(require_auth_optional),
):
    
    """
    AI commentary for ML forecast.
    Generated on-demand, cached per candle.
    """
    if os.getenv("XTL_ENABLE_COMMENTARY", "false").lower() != "true":
         return {"ok": False, "reason": "commentary_disabled"}
    tfu = (tf or "H1").upper()
    sym_u = (symbol or "").upper().strip()
    if not sym_u:
        return {"ok": False, "reason": "missing_symbol"}

    # Reuse existing ML prediction feed
    resp = predict_all(tf=tfu, symbols=sym_u, user=user)
    if not isinstance(resp, dict) or not resp.get("ok"):
        return {"ok": False, "reason": "prediction_failed"}

    row = None
    for r in (resp.get("rows") or []):
        if (r.get("symbol") or "").upper() == sym_u:
            row = r
            break
    if not row:
        return {"ok": False, "reason": "symbol_not_found"}

    cache_key = f"xtl:trend:commentary:{sym_u}:{tfu}:{row.get('target_close_ts') or 0}"
    cached = _redis_get_text(cache_key)
    if cached:
        return {"ok": True, "cached": True, "commentary": cached}

    payload = build_commentary_payload(row)
    try:
        txt = call_llm_commentary(payload)
        if not txt:
            txt = "Commentary unavailable for this candle."
    except Exception as e:
        txt = f"Commentary unavailable ({type(e).__name__})."


    now_ms = int(time.time() * 1000)

    target_close_ts = row.get("target_close_ts")
    buffer_sec = 10 * 60  # 10 min safety buffer

    if isinstance(target_close_ts, (int, float)) and target_close_ts > now_ms:
        ttl_sec = int((target_close_ts - now_ms) / 1000) + buffer_sec
    else:
        # fallback: short TTL to avoid stale cache
        ttl_sec = 15 * 60

    _redis_set_text(cache_key, txt, ttl_sec=ttl_sec)

    return {
        "ok": True,
        "cached": False,
        "commentary": txt,
        "meta": {
            "symbol": sym_u,
            "tf": tfu,
            "target_close_ts": row.get("target_close_ts"),
            "ttl_sec": ttl_sec
        },
    }


def call_llm_commentary(payload: dict) -> str:
    """
    Calls LLM to narrate ML forecast.
    """

    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {
            "role": "user",
            "content": json.dumps(payload, ensure_ascii=False)
        }
    ]

    resp = client.chat.completions.create(
        model="gpt-4.1-mini",  # or your choice
        messages=messages,
        temperature=0.2
    )

    return resp.choices[0].message.content.strip()


@router.get("/opportunities")
def trend_opportunities(
    tf: str = "M15",
    symbols: str = "XAUUSD,EURUSD,USDJPY,GBPUSD,USDCAD,USDCHF",
    device: str | None = Query(None),
    x_device_id: str | None = Header(None, convert_underscores=False),
    loose: bool = Query(False),
    debug_force: bool = Query(False),
    debug_top: int = Query(0, ge=0, le=10),
    debug_persist: bool = Query(False),
    user = Depends(require_auth_optional),
):
    """
    Live opportunities feed.

    Logic:
    - Primary trigger is H1 expected move (expected_move_pct_1h).
    - H4 expected move (expected_move_pct_4h) is used as confirmation / filter.

    Combinations:

    1) |H1| < H1_threshold
       -> no opportunity, even if H4 is big.

    2) |H1| >= H1_threshold and sign(H1) == sign(H4) and |H4| >= H4_threshold
       -> opportunity, opp_confidence = "high".

    3) |H1| >= H1_threshold and sign(H1) == sign(H4) but |H4| < H4_threshold
       -> opportunity, opp_confidence = "medium".

    4) |H1| >= H1_threshold and H4 missing
       -> opportunity, opp_confidence = "high" if |H1| is much bigger than
          threshold, else "medium".

    5) |H1| >= H1_threshold and sign(H1) != sign(H4)
       -> drop (no opportunity) - we do not want countertrend / chop.

    This does not represent trade signals, only "room to move".
    """

    tfu = (tf or "M15").upper()

    # Reuse main prediction logic (H1/H4 model + structure + macro)
    base = predict_all(
        tf=tfu,
        symbols=symbols,
        device=device,
        x_device_id=x_device_id,
        user=user,
    )

    if not isinstance(base, dict):
        return {"ok": False, "reason": "predict_all_not_dict"}

    if not base.get("ok", True):
        # Bubble underlying reason (e.g. models not loaded, no_data, etc.)
        return base

    rows_in = base.get("rows") or []
    opp_rows: list[dict[str, Any]] = []
    debug_pool: list[dict[str, Any]] = []


    now_ms = int(_time.time() * 1000)
    _sweep_opp_snapshots(symbols, now_ms)
    st_thr = 0.35   # H1 threshold (%)
    ht_thr = 0.70   # H4 confirm threshold (%)

    if loose:
        st_thr = 0.06
        ht_thr = 0.12

    for row in rows_in:
        sym = str(row.get("symbol") or "").upper()
        if not sym:
            continue
        # --- DEBUG FORCE: always emit test opportunities even if models are flat/missing ---
        if debug_force and debug_top > 0:
            thr1 = _room_thr_h1(sym)
            thr4 = _room_thr_h4(sym)

            # derive direction from any available field; default BUY
            dec = str(row.get("decision") or row.get("opp_direction") or row.get("direction") or "BUY").upper()
            s = 1 if dec in ("BUY", "UP", "LONG") else -1

            # fabricate moves just above thresholds so it looks realistic
            m1 = float(s) * max(thr1, st_thr) * 1.6
            m4 = float(s) * max(thr4, ht_thr) * 1.6

            out = dict(row)
            out["expected_move_pct_1h"] = m1
            out["expected_move_pct_4h"] = m4

            now_ms_local = int(_time.time() * 1000)
            hour_ms = 60 * 60 * 1000
            bucket_open_ts = (now_ms_local // hour_ms) * hour_ms

            opp_dir = "UP" if s > 0 else "DOWN"
            out["opp_id"] = f"{sym}-H1-{opp_dir}-{bucket_open_ts}"
            out["opp_direction"] = opp_dir
            out["opp_confidence"] = "high"
            out["opp_horizon"] = "H1"
            out["opp_open_ts"] = bucket_open_ts
            out["opp_expire_ts"] = bucket_open_ts + hour_ms
            out["opp_min_room_h1"] = thr1
            out["opp_min_room_h4"] = thr4
            out["opp_score"] = 99.0
            out["opp_reason"] = "DEBUG_FORCE (fabricated opportunity for UI testing)"
            out.setdefault("update_tf", tfu)
            out.setdefault("server_now_ms", now_ms_local)

            # IMPORTANT: do NOT write snapshots in debug force (keeps it clean)
            debug_pool.append(out)
            continue


        thr1 = _room_thr_h1(sym)
        thr4 = _room_thr_h4(sym)

        # Extract H1 / H4 expected move (%)
        try:
            m1 = float(row.get("expected_move_pct_1h"))
        except (TypeError, ValueError):
            m1 = None

        try:
            m4 = float(row.get("expected_move_pct_4h"))
        except (TypeError, ValueError):
            m4 = None

        s1 = _sign(m1)
        s4 = _sign(m4)

        # --------------------------------------------------
        # 0) Enrich raw row with useful features (RVOL/ATR/spread)
        #     so both _compute_opp_score and the UI can use them.
        # --------------------------------------------------
        extra_h1 = row.get("extra_h1") or {}
        feats_h1 = (
            extra_h1.get("features")
            if isinstance(extra_h1.get("features"), dict)
            else extra_h1
        )
        if isinstance(feats_h1, dict):
            rv = feats_h1.get("feat_rvol15")
            if isinstance(rv, (int, float)):
                row["feat_rvol15"] = float(rv)

            atr_bp = feats_h1.get("feat_atr_bp")
            if isinstance(atr_bp, (int, float)):
                row["feat_atr_bp"] = float(atr_bp)

            sp_bp = feats_h1.get("feat_spread_bp") or feats_h1.get("spread_bp")
            if isinstance(sp_bp, (int, float)):
                row["spread_bp"] = float(sp_bp)

        # --------------------------------------------------
        # 1) Check if we already have an ACTIVE snapshot
        #    (so we keep showing it even if room shrinks)
        # --------------------------------------------------
        has_active_snapshot = False
        active_snap_row: dict[str, Any] | None = None

        try:
           for d in ("UP", "DOWN"):
               snap_key = _opp_snapshot_key(sym, d)
               snap = R.hgetall(snap_key)
               if not snap:
                   continue

               _evaluate_alert_outcome(sym, snap, row, now_ms)

               raw_status = snap.get("status")
               if raw_status is None:
                   status = "active"
               else:
                   if isinstance(raw_status, bytes):
                       raw_status = raw_status.decode("utf-8", "ignore")
                   try:
                       status = json.loads(raw_status)
                   except Exception:
                       status = str(raw_status)

               status = str(status).lower()

               if status in ("active", "new", "open"):
                   has_active_snapshot = True

                   # convert redis hash -> dict
                   snap_row = {}
                   for k, v in snap.items():
                       kk = k.decode() if isinstance(k, bytes) else k
                       vv = v.decode() if isinstance(v, bytes) else v
                       try:
                          snap_row[kk] = json.loads(vv)
                       except Exception:
                          snap_row[kk] = vv

                   active_snap_row = snap_row
                   break
        except Exception:
            has_active_snapshot = False
            active_snap_row = None

        # --------------------------------------------------
        # 2) Primary trigger: H1 must have enough room
        #    BUT ONLY IF there is NO active snapshot.
        #    If a snapshot is already open, we keep the
        #    row visible until it hits or expires.
        # --------------------------------------------------
        if has_active_snapshot and active_snap_row:
            active_snap_row.setdefault("update_tf", tfu)
            active_snap_row.setdefault("server_now_ms", now_ms)

            # optional live price refresh
            lp = row.get("last_price") or row.get("price")
            if isinstance(lp, (int, float)):
                active_snap_row["last_price"] = lp

            opp_rows.append(active_snap_row)
            continue

        if (not has_active_snapshot) and (
            not isinstance(m1, (int, float)) or s1 == 0 or abs(m1) < thr1
        ):
            continue

        
        # --- Layer A: prediction delta gate -------------------------------
        # Only surface a fresh opportunity if the forecast actually changed
        # enough compared to the last seen 1h move for this symbol.

        delta_pct = None
        delta_thr = _delta_thr_h1(sym, thr1)

        # If m1 is missing/non-numeric (can happen when we keep a row due to an active snapshot),
        # skip delta tracking/gating safely.
        if isinstance(m1, (int, float)):
            try:
                key = PRED_DELTA_KEY_FMT % sym
                prev_str = R.get(key)

                if prev_str is not None:
                    try:
                        prev_val = float(prev_str)
                        delta_pct = abs(float(m1) - prev_val)
                    except (TypeError, ValueError):
                        delta_pct = None

                # Always update stored value for next call; 90-minute TTL is enough
                R.set(key, f"{float(m1):.6f}", ex=90 * 60)

            except Exception:
                # Redis issues must not break the endpoint
                delta_pct = None

        # If we have a previous forecast and the change is too small,
        # do not treat this as a new opportunity.
        if delta_pct is not None and delta_pct < delta_thr and not has_active_snapshot:
            continue


        # Base direction from H1
        opp_dir = "UP" if s1 > 0 else "DOWN"
        opp_conf = "medium"
        h4_agree: bool | None = None

        # --- Combine with H4 when available (structure confirmation) ---
        if isinstance(m4, (int, float)) and s4 != 0:
            if s1 == s4:
                # Same direction -> confirmation
                h4_agree = True

                # Strong confirm = H4 also has decent room
                if abs(m4) >= thr4:
                    opp_conf = "high"
                else:
                    opp_conf = "medium"
            else:
                # H4 conflicts with H1 -> drop this entirely (no opportunity)
                continue
        else:
            # No usable H4 -> rely purely on H1 magnitude for confidence
            if abs(m1) >= 1.5 * thr1:
                opp_conf = "high"
            else:
                opp_conf = "medium"

        # --- Overall opportunity score (room + trend + confidence + SR/RVOL/etc) ---
        opp_score = _compute_opp_score(sym, row, m1, thr1)

        # Once an alert is opened, keep it visible until it hits or the 1h window expires.
        # Once an alert is opened, keep it visible until it hits or the 1h window expires.
        # DEBUG: if nothing qualifies, allow returning top candidates for UI testing
        if opp_score < OPP_SCORE_MIN and not has_active_snapshot:
            if debug_top > 0 and (debug_force or loose):
                out_dbg = dict(row)

                hour_ms = 60 * 60 * 1000
                bucket_open_ts = (now_ms // hour_ms) * hour_ms
                opp_open_ts = bucket_open_ts
                opp_expire_ts = bucket_open_ts + hour_ms
                opp_id = f"{sym}-H1-{('UP' if s1 > 0 else 'DOWN')}-{opp_open_ts}"

                out_dbg["opp_id"] = opp_id
                out_dbg["opp_direction"] = "UP" if s1 > 0 else "DOWN"
                out_dbg["opp_confidence"] = opp_conf
                out_dbg["opp_horizon"] = "H1"
                out_dbg["opp_h4_agree"] = h4_agree
                out_dbg["opp_open_ts"] = opp_open_ts
                out_dbg["opp_expire_ts"] = opp_expire_ts
                out_dbg["opp_min_room_h1"] = thr1
                out_dbg["opp_min_room_h4"] = thr4
                out_dbg["opp_score"] = round(float(opp_score), 1)

                # Tag as debug so UI can render clearly; do NOT snapshot/history
                out_dbg["debug_only"] = True
                out_dbg["status"] = "debug"
                out_dbg["opp_reason"] = out_dbg.get("opp_reason") or "debug candidate (below OPP_SCORE_MIN)"

                debug_pool.append(out_dbg)
            continue


        # --- Construct enriched opportunity row for UI + logging ------------
        out = dict(row)  # start from predict_all row

        # Use a 1h bucket so the opportunity ID is stable within that hour
        hour_ms = 60 * 60 * 1000
        bucket_open_ts = (now_ms // hour_ms) * hour_ms

        # When opportunity appears (open) = start of that hour bucket
        opp_open_ts = bucket_open_ts

        # Expiry = one hour after bucket open (logical H1 horizon)
        opp_expire_ts = bucket_open_ts + hour_ms

        # Deterministic ID: symbol + horizon + direction + hour-bucket
        opp_id = f"{sym}-H1-{opp_dir}-{opp_open_ts}"

        out["opp_id"] = opp_id
        out["opp_direction"] = opp_dir            # "UP" / "DOWN"
        out["opp_confidence"] = opp_conf          # "high" / "medium"
        out["opp_horizon"] = "H1"
        out["opp_h4_agree"] = h4_agree            # True / False / None
        out["opp_open_ts"] = opp_open_ts
        out["opp_expire_ts"] = opp_expire_ts
        out["opp_min_room_h1"] = thr1
        out["opp_min_room_h4"] = thr4
        out["opp_score"] = round(float(opp_score), 1)

        
        
        # --- SR summary for UI (H1 + H4 nearest zones) --------------------
        sr = row.get("sr")
        if isinstance(sr, dict):

            def _attach_tf(tf_key: str, side_key: str, dist_key: str, level_key: str) -> None:
                """
                Attach nearest SR info for a given TF key (e.g. 'H1', 'H4')
                into the outgoing row as side/dist/level fields.
                """
                zone = sr.get(tf_key) or {}
                if isinstance(zone, dict):
                    nearest = zone.get("nearest") or zone.get("nearest_zone") or zone
                else:
                    nearest = {}
                if not isinstance(nearest, dict):
                    return

                kind = (nearest.get("kind") or nearest.get("side") or "").lower()
                dist_pct = nearest.get("distance_pct") or nearest.get("dist_pct")
                level = nearest.get("level")

                if kind:
                    out[side_key] = kind          # e.g. "support" / "resistance"
                if isinstance(dist_pct, (int, float)):
                    out[dist_key] = float(dist_pct)
                if isinstance(level, (int, float)):
                    out[level_key] = float(level)

            # Per-TF SR: H1 + H4 (if present in sr)
            _attach_tf("H1", "sr_h1_side", "sr_h1_dist_pct", "sr_h1_level")
            _attach_tf("H4", "sr_h4_side", "sr_h4_dist_pct", "sr_h4_level")

            # Legacy overall nearest (for generic use / scoring)
            nearest = sr.get("nearest") or sr.get("nearest_zone") or {}
            if isinstance(nearest, dict):
                kind = (nearest.get("kind") or nearest.get("side") or "").lower()
                if kind:
                    out["sr_side"] = kind  # e.g. "support" / "resistance"
                dist_pct = nearest.get("distance_pct") or nearest.get("dist_pct")
                if isinstance(dist_pct, (int, float)):
                    out["sr_dist_pct"] = float(dist_pct)


        # Expose prediction delta so UI / logs can show how "fresh" it is
        if delta_pct is not None:
            out["opp_delta_pct"] = delta_pct
            out["opp_delta_thr"] = delta_thr

        # Small textual summary for debugging / UI
        tag_bits: list[str] = []
        tag_bits.append(f"H1 move {m1:.3f}% (thr {thr1:.3f}%)")
        if isinstance(m4, (int, float)):
            tag_bits.append(f"H4 move {m4:.3f}% (thr {thr4:.3f}%)")
            if h4_agree:
                tag_bits.append("H1+H4 aligned")
            else:
                tag_bits.append("H1/H4 conflict")
        tag_bits.append(f"opp_direction={opp_dir}")
        tag_bits.append(f"opp_confidence={opp_conf}")
        out.setdefault("opp_reason", "; ".join(tag_bits))

        # Ensure time fields exist for UI refresh, even if predict_all stubbed
        out.setdefault("update_tf", tfu)
        out.setdefault("server_now_ms", now_ms)

        # Freeze / update alert snapshot (status, hit/expired, etc.)
        out = _freeze_or_snapshot_opp(sym, out, now_ms)
        status = (out.get("status") or "active")
        status = (str(status) or "").strip().lower()
        if status in ("active", "new", "open"):
           opp_rows.append(out)


        

    history = _load_opp_history(limit=50)
    # If no real opportunities, return debug candidates for UI testing
    if (not opp_rows) and debug_top > 0 and debug_pool:
        debug_pool.sort(key=lambda x: float(x.get("opp_score") or 0.0), reverse=True)
        opp_rows = debug_pool[:debug_top]

    

    return {
        "ok": True,
        "tf": tfu,
        "rows": opp_rows,
        "history": history,
    }



@router.get("/opportunities/history")
def opportunities_history(limit: int = 100):
    """
    TEMP: history via CSV is deprecated.
    Frontend now uses in-session history only.
    This endpoint returns an empty list to keep compatibility.
    """
    return {"ok": True, "rows": []}




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
    sr: Optional[Dict[str, Any]] = None

class BotStateUpdate(BaseModel):
    """
    Partial update from UI. All fields optional; we merge into existing state.
    """
    enabled: Optional[bool] = None
    strategy_type: Optional[Literal["indicator", "priceAction", "opportunity"]] = None
    config: Optional[Dict[str, Any]] = None


class BotStateResp(BaseModel):
    ok: bool = True
    state: Dict[str, Any]

@router.get("/bot/state", response_model=BotStateResp)
def get_bot_state(user = Depends(require_auth_optional)):
    user_id = getattr(user, "user_id", None) if user else None
    if not user_id:
        raise HTTPException(status_code=401, detail="Login required")

    state = _load_bot_state(user_id)
    return BotStateResp(ok=True, state=state)


@router.post("/bot/state", response_model=BotStateResp)
def update_bot_state(payload: BotStateUpdate, user = Depends(require_auth_optional)):
    user_id = getattr(user, "user_id", None) if user else None
    if not user_id:
        raise HTTPException(status_code=401, detail="Login required")

    current = _load_bot_state(user_id)
    patch = payload.dict(exclude_unset=True)

    for k, v in patch.items():
        if v is None:
            continue
        if k == "config":
            cfg = dict(current.get("config") or {})
            if isinstance(v, dict):
                cfg.update(v)
            current["config"] = cfg
        else:
            current[k] = v

    _save_bot_state(user_id, current)
    return BotStateResp(ok=True, state=current)



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
        if xi >= 1_000_000_000_000_000:      # microseconds
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
    raw = None

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
                "pollAfterMs": int(max(1200, min((next_close_ms - server_now_ms + 250), 5000))),
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
        # ---- PATCH: derive lastClosedTs / nextCloseTs from preview bars ----
        server_now_ms = int(time.time() * 1000)

        # Collect preview bars from snapshot (prefer nested preview.bars, else top-level bars)
        preview_bars = None
        if isinstance(snap.get("preview"), dict):
            preview_bars = snap["preview"].get("bars")
        if not isinstance(preview_bars, list):
            preview_bars = snap.get("bars") or []
        preview_bars = list(preview_bars)

        # Find the last fully-closed bar in preview_bars
        preview_last_closed_ts = 0
        for b in reversed(preview_bars):
            # Prefer explicit close time if present
            t_close_ms = b.get("t_close_ms")
            if not isinstance(t_close_ms, (int, float)):
                # Fallback: t_open_ms or t (seconds) + TF_MS
                t_open_raw = b.get("t_open_ms") or b.get("t")
                if t_open_raw is None:
                   continue
                try:
                   # _ms_from_t handles both sec and ms
                   from_ms = _ms_from_t(t_open_raw)
                except Exception:
                   continue
                t_close_ms = from_ms + TF_MS

            try:
                t_close_ms = int(t_close_ms)
            except Exception:
                continue

            # Treat as closed if its close time is already in the past
            if t_close_ms <= server_now_ms:
                preview_last_closed_ts = t_close_ms
                break

        # Ensure preview object exists and holds the bars
        if not isinstance(snap.get("preview"), dict):
            snap["preview"] = {}
        snap["preview"]["bars"] = preview_bars

        # If we found a closed bar, override snapshot lastClosedTs / nextCloseTs
        if preview_last_closed_ts:
            snap["preview"]["lastClosedTs"] = preview_last_closed_ts
            snap["lastClosedTs"] = preview_last_closed_ts

            # Canonical next close = one TF after lastClosedTs, nudged into the future
            next_close_guess = preview_last_closed_ts + TF_MS
            if next_close_guess - server_now_ms < 1000:
                next_close_guess += TF_MS
            snap["nextCloseTs"] = int(next_close_guess)

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
                      # persist hydration to the user-facing key so next call is not Warming
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
            "pollAfterMs": int(max(1200, min((next_close_ms - server_now_ms + 250), 5000))),
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
                "pollAfterMs": int(max(1200, min((next_close_ms - server_now_ms + 250), 5000))),
                "usingDevice": prefer_dev, 
            }


    
    
   
    
    
    # ---------- closed-bar filter (use device lastClosedTs) ----------
    server_now_ms = int(time.time() * 1000)
    TF_SEC = int((TF_MS // 1000) if isinstance(TF_MS, int) else TF_MS)  # seconds

    # Prefer device-supplied lastClosedTs from the snapshot.
    # This is the close timestamp (ms) of the last FULLY closed bar in broker time.
    try:
        snap_last_closed_ms = int(js.get("lastClosedTs") or 0)
    except Exception:
        snap_last_closed_ms = 0

    # Fallback: if snapshot didn't give us anything, fall back to server clock
    if snap_last_closed_ms <= 0:
        snap_last_closed_ms = server_now_ms

    closed: list[dict] = []

    for b in (bars or []):
        try:
            # Normalise open / close times to ms
            t_open_ms = _to_ms_any(b.get("t_open_ms") or b.get("t"))
            if t_open_ms is None:
                continue

            t_close_ms = _to_ms_any(b.get("t_close_ms"))
            if t_close_ms is None:
                t_close_ms = t_open_ms + TF_SEC * 1000

            # Bar is closed only if its CLOSE is <= lastClosedTs
            if t_close_ms > snap_last_closed_ms:
                # still forming
                continue

            t_s = int(t_open_ms // 1000)

            closed.append({
                # legacy open-time in seconds (used by some paths)
                "t": t_s,
                # explicit broker-grid times in ms
                "t_open_ms": int(t_open_ms),
                "t_close_ms": int(t_close_ms),
                # OHLC
                "o": float(b["o"]),
                "h": float(b["h"]),
                "l": float(b["l"]),
                "c": float(b["c"]),
                "complete": True,
            })
        except Exception:
            continue

    # Fallback: if still nothing but we have bars, use all except the very last
    # (which is most likely the currently-forming bar) so the UI can render.
    if not closed and bars:
        base = bars[:-1] if len(bars) > 1 else bars
        closed = []
        for b in base:
            try:
                t_open_ms = _to_ms_any(b.get("t_open_ms") or b.get("t"))
                if t_open_ms is None:
                    continue
                t_s = int(t_open_ms // 1000)
                closed.append({
                    "t": t_s,
                    "t_open_ms": int(t_open_ms),
                    "t_close_ms": int(t_open_ms + TF_SEC * 1000),
                    "o": float(b["o"]),
                    "h": float(b["h"]),
                    "l": float(b["l"]),
                    "c": float(b["c"]),
                    "complete": True,
                })
            except Exception:
                continue
    
    # --- NEW: normalize explicit open/close ms from canonical open time ---
    # At this point `closed` is our source of truth and `t` is the bar OPEN
    # in epoch seconds. Make t_open_ms / t_close_ms consistent with that.
    if closed:
        try:
            tf_sec = int(TF_SEC)
        except Exception:
            tf_sec = int(TF_MS // 1000)

        for row in closed:
            try:
                t_s = int(row.get("t") or 0)
                if t_s <= 0:
                    continue
                t_open_ms = t_s * 1000
                row["t_open_ms"] = t_open_ms
                row["t_close_ms"] = t_open_ms + tf_sec * 1000
            except Exception:
                continue


    # --- NEW: resync OHLC from broker bars when available ---
    # --- NEW: resync & EXTEND OHLC from broker bars when available ---
    try:
        # Pull a reasonable window of recent closed bars from the agent/broker.
        # Limit is small to avoid heavy load but large enough to cover our closed[]
        limit = max(60, min(180, len(closed) + 10))
        broker_rows = _broker_bars_sync(sym, tfu, limit=limit)
    except Exception:
        broker_rows = None

    if broker_rows:
        # Index existing closed[] by OPEN time in seconds
        closed_by_t: dict[int, dict] = {}
        for row in closed:
            try:
                tt = int(row.get("t") or 0)
            except Exception:
                tt = 0
            if tt > 0:
                closed_by_t[tt] = row

        last_closed_t = max(closed_by_t.keys()) if closed_by_t else 0

        # Broker bars indexed by OPEN time in seconds
        broker_by_t: dict[int, dict] = {}
        for r in broker_rows:
            try:
                t_ms = _to_ms_any(r.get("t"))
                if t_ms is None:
                    continue
                t_s = int(t_ms // 1000)
                broker_by_t[t_s] = {
                    "t_ms": int(t_ms),
                    "o": float(r.get("o", 0.0)),
                    "h": float(r.get("h", 0.0)),
                    "l": float(r.get("l", 0.0)),
                    "c": float(r.get("c", 0.0)),
                }
            except Exception:
                continue

        # 1) Overwrite OHLC for bars we already have
        for t_s, bt in broker_by_t.items():
            row = closed_by_t.get(t_s)
            if not row:
                continue
            row["o"] = bt["o"]
            row["h"] = bt["h"]
            row["l"] = bt["l"]
            row["c"] = bt["c"]

        # 2) Append any NEW fully-closed broker bars missing from snapshot
        try:
            now_ms = server_now_ms
        except NameError:
            now_ms = int(time.time() * 1000)

        try:
            tf_sec = int(TF_SEC if isinstance(TF_SEC, int) else TF_MS // 1000)
        except Exception:
            tf_sec = 60 * 60  # safe fallback = 1h

        cushion_ms = 5_000  # 5s cushion so we never use forming bar

        for t_s in sorted(broker_by_t.keys()):
            # only append bars strictly after our last snapshot bar
            if closed_by_t and t_s <= last_closed_t:
                continue

            bt = broker_by_t[t_s]
            t_open_ms = bt["t_ms"]
            t_close_ms = t_open_ms + tf_sec * 1000

            # only treat as closed if its close is not in the future
            if t_close_ms > now_ms + cushion_ms:
                continue

            new_row = {
                "t": int(t_s),
                "t_open_ms": int(t_open_ms),
                "t_close_ms": int(t_close_ms),
                "o": bt["o"],
                "h": bt["h"],
                "l": bt["l"],
                "c": bt["c"],
                "complete": True,
            }
            closed.append(new_row)
            closed_by_t[t_s] = new_row
            last_closed_t = t_s


        # Overwrite OHLC in closed[] where we have a broker match on t
        if broker_by_t:
            for row in closed:
                t_s = int(row.get("t") or 0)
                bt = broker_by_t.get(t_s)
                if not bt:
                    continue
                row["o"] = bt["o"]
                row["h"] = bt["h"]
                row["l"] = bt["l"]
                row["c"] = bt["c"]



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
    # Broker timezone offset (minutes) for boundary calculation
    try:
        broker_tz_offset_min = int((broker_safe or {}).get("tz_offset_min") or 0)
    except Exception:
        broker_tz_offset_min = 0
    next_close_ts = _next_boundary_ms(TF_SEC, server_now_ms, broker_tz_offset_min or 0)

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
    # --- adaptive polling hint for UI ---
    # Ensure we have a sane next_close_ms even if earlier computation skipped
    if (
        "next_close_ms" not in locals()
        or not isinstance(next_close_ms, (int, float))
        or next_close_ms <= 0
    ):
        try:
            off_min = int((broker_safe or {}).get("tz_offset_min") or 0)
        except Exception:
            off_min = 0
        # Align next boundary on broker grid: one TF after the current server_now_ms
        base_next = ((server_now_ms + off_min * 60_000) // tf_ms) * tf_ms + tf_ms - off_min * 60_000
        next_close_ms = int(base_next)

    # how long until next close, based on the current server_now_ms we computed earlier
    remain_ms = max(0, int(next_close_ms - server_now_ms))

    # default gentle polling if boundary isn't known
    poll_after_ms = 10_000

    # if we know the boundary, wake slightly after it; clamp to [2s, 60s]
    if next_close_ms and server_now_ms:
        poll_after_ms = max(2_000, min(60_000, (next_close_ms - server_now_ms) + 500))


    
    

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


    # roll forward if we are already past it (covers missed bars, clock skew, etc.)
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
    # --- Build preview rows from snapshot closed bars only (temporarily disable direct broker fetch) ---
    prev_rows_override = None
    rows_src = [
        {
            "t": int(_to_ms_any(b.get("t"))) // 1000,  # seconds (MT5 bar time = OPEN)
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
   

    TF_MS = TF_SEC * 1000
    last_open_ms = int(_epoch_to_ms_any(last["t"]))
    last_close_ms = last_open_ms + TF_MS

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

    

    # --- Apply tailing limit (default 60; clamp 30 to 500) ---
    N = max(30, min(int(n or 60), 500))

    # Prefer raw agent MT5 bars (with t_open_ms / t_close_ms) for preview,
    # fall back to normalized rows if something is missing.
    closed_raw: list[dict] = []
    for b in rows_src or []:
        try:
            # keep only CLOSED candles; agent marks forming bar with complete=false
            if b.get("complete") is False:
                continue
            closed_raw.append(b)
        except Exception:
            continue

    # sort by agent's bar-open time (t_open_ms preferred, else t)
    try:
        closed_raw.sort(
            key=lambda b: _epoch_to_ms_any(
                b.get("t_open_ms") if "t_open_ms" in b else b.get("t")
            )
        )
    except Exception:
        pass

    if closed_raw:
        # use the last N CLOSED raw bars from the agent
        tail = closed_raw[-N:]
    else:
        # safety fallback: use normalized rows if raw is unavailable
        tail = norm[-N:]

        # --- HARD OVERRIDE: prefer direct broker/agent bars for preview tail ---
        # This ensures preview (time + OHLC) always tracks the latest closed MT5 bar,
        # even if the Redis snapshot lags.
        try:
           agent_rows = _broker_bars_sync(sym, tfu, limit=N)
        except Exception:
           agent_rows = None

        if agent_rows:
           direct_tail: list[dict] = []
           for r in agent_rows:
               try:
                  # r["t"] may be seconds or ms; the preview builder later normalizes it.
                  direct_tail.append(
                      {
                         "t": r.get("t"),
                         "o": float(r.get("o", 0.0)),
                         "h": float(r.get("h", 0.0)),
                         "l": float(r.get("l", 0.0)),
                         "c": float(r.get("c", 0.0)),
                         # Treat as closed; agent/MT5 side only gives fully closed bars here.
                         "complete": True,
                      }
                  )
               except Exception:
                  continue

           # Only override if we actually got something sensible
           if direct_tail:
               tail = direct_tail




    
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

    # --- extra safety: never coarsen prices for FX ---
    # infer decimals from the latest close and ensure digits is AT LEAST that
    try:
        if tail:
            sample = abs(float(tail[-1]["c"]))
            s = f"{sample:.8f}".rstrip("0").rstrip(".")
            if "." in s:
                inferred = len(s.split(".")[1])
                if inferred > digits:
                    digits = inferred
    except Exception:
        pass

    # round to final digits (only if > 0), but now digits >= actual precision
    if digits and digits > 0:
        for r in tail:
            r["o"] = round(float(r["o"]), digits)
            r["h"] = round(float(r["h"]), digits)
            r["l"] = round(float(r["l"]), digits)
            r["c"] = round(float(r["c"]), digits)

    prev_rows = tail  # <- do NOT modify after this


    
    # --- Compute preview lastClosedTs & probe using agent-aligned times ----
    last_open_ms = last_close_ms = None

    if prev_rows:
        last_row = prev_rows[-1]
        # trust agent_ohlc's alignment
        last_open_ms = int(
            last_row.get("t_open_ms") or _epoch_to_ms_any(last_row["t"])
        )
        last_close_ms = int(
            last_row.get("t_close_ms") or (last_open_ms + TF_SEC * 1000)
        )

        # lastClosedTs should be the CLOSE of the last bar
        preview_last_closed_ts = last_close_ms
        
    else:
        preview_last_closed_ts = None


    previewProbe = {
        "broker_tz_offset_min": (broker_safe or {}).get("tz_offset_min"),
        "tf_sec": TF_SEC,
        "agent_bar": {
             "t": int(last["t"]),
             "o": float(last["o"]), "h": float(last["h"]),
             "l": float(last["l"]), "c": float(last["c"]),
        },
        "render_bar": {
            "t_open_ms": int(last_open_ms) if last_open_ms is not None else None,
            "t_close_ms": int(last_close_ms) if last_close_ms is not None else None,
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


    
    # Anchor each bar to the broker TF grid using tz_offset_min
    # --- Build preview payload (use raw MT5 UTC bar open; UI applies broker offset) ---
    
    # --- Build preview payload using agent-aligned broker-grid timestamps ---
    TF_MS = TF_SEC * 1000

    bars_tail: list[PreviewBar] = []

    # safety: ensure tail is defined even if earlier branch skipped
    if "tail" not in locals():
        try:
            tail = norm[-N:]
        except Exception:
            tail = []

    for r in tail:
        try:
            # Prefer agent-supplied broker-grid times if present
            t_open_ms = r.get("t_open_ms")
            t_close_ms = r.get("t_close_ms")

            if t_open_ms is None:
                # fallback: derive from legacy 't' (seconds or ms)
                t_open_ms = _epoch_to_ms_any(r.get("t"))
            else:
                t_open_ms = _epoch_to_ms_any(t_open_ms)

            if t_close_ms is None:
                t_close_ms = int(t_open_ms + TF_MS)
            else:
                t_close_ms = _epoch_to_ms_any(t_close_ms)

            if t_open_ms is None or t_close_ms is None:
                continue

            bars_tail.append(
                PreviewBar(
                    t_open_ms=int(t_open_ms),
                    t_close_ms=int(t_close_ms),
                    o=float(r["o"]),
                    h=float(r["h"]),
                    l=float(r["l"]),
                    c=float(r["c"]),
                )
            )
        except Exception:
            continue


    preview = PreviewPayload(
        symbol=sym,
        tf=tfu,
        bars=bars_tail,
        # use preview_last_closed_ts which we computed from prev_rows
        lastClosedTs=int(preview_last_closed_ts or 0),
        probe=previewProbe,
    )

    try:
        preview_out = preview.dict()
    except Exception:
        preview_out = dict(preview)
    preview_out["broker"] = broker_safe

    

    # --- SR summary (H4 + H1) for this symbol ---
    sr_summary = None
    try:
        def _rows_to_df(rows):
            if not rows:
                return None
            data = []
            for r in rows:
                try:
                    data.append(
                        {
                            "t": _epoch_to_ms_any(r.get("t")),
                            "o": float(r["o"]),
                            "h": float(r["h"]),
                            "l": float(r["l"]),
                            "c": float(r["c"]),
                        }
                    )
                except Exception:
                    continue
            if not data:
                return None
            return pd.DataFrame(data)

        # Always compute SR from true H1 and H4 broker bars
        try:
            h1_rows = _broker_bars_sync(sym, "H1", limit=300)
        except Exception:
            h1_rows = None
        try:
            h4_rows = _broker_bars_sync(sym, "H4", limit=300)
        except Exception:
            h4_rows = None

        h1_df = _rows_to_df(h1_rows)
        h4_df = _rows_to_df(h4_rows)

        # Last price from preview bars (what UI is showing)
        last_price = None
        if prev_rows:
            try:
                last_price = float(prev_rows[-1]["c"])
            except Exception:
                last_price = None

        # pip factor per symbol (rough, can refine later)
        pip_factor = 0.1 if sym == "XAUUSD" else 0.0001

        if last_price and (h1_df is not None or h4_df is not None):
            sr_summary = summarize_sr_multi_tf(
                symbol=sym,
                price=last_price,
                h4_df=h4_df,
                h1_df=h1_df,
                pip_factor=pip_factor,
            )
    except Exception as e:
        sr_summary = {"error": f"sr_failed: {e}"}

    # --- Canonical next-bar timing based on preview_last_closed_ts ---
    # Use the last CLOSED bar from preview as the single source of truth,

    
    # --- Canonical next-bar timing based on preview_last_closed_ts ---
    # Use the last CLOSED bar from preview as the single source of truth,
    # but always compute countdown in server time using broker_tz_offset_min.
    TF_MS = int(TF_SEC * 1000)
    server_now_ms = int(time.time() * 1000)

    # last_closed_ts = CLOSE time of the last fully closed bar (ms, broker wall-clock)
    last_closed_ts = int(preview_last_closed_ts or 0)

    if TF_MS <= 0:
        # safety fallback: default to 1h
        TF_MS = 60 * 60 * 1000

    # Broker offset (minutes -> ms)
    try:
        off_min = int((broker_safe or {}).get("tz_offset_min") or 0)
    except Exception:
        off_min = 0
    off_ms = off_min * 60_000

    tf_ms_int = int(TF_MS)
    # Convert server clock to broker wall-clock
    now_broker_ms = server_now_ms + off_ms

    if last_closed_ts > 0:
        # last_closed_ts is already broker wall-clock close time
        next_close_broker = last_closed_ts + tf_ms_int

        # Ensure next close is in the future in broker time
        if next_close_broker <= now_broker_ms:
            slots_ahead = (now_broker_ms - last_closed_ts) // tf_ms_int + 1
            next_close_broker = last_closed_ts + slots_ahead * tf_ms_int
    else:
        # No last_closed_ts (warming): align from broker clock grid
        next_close_broker = ((now_broker_ms // tf_ms_int) + 1) * tf_ms_int

    # Convert broker close time back to server ms (for UI countdown)
    next_close_ts = int(next_close_broker - off_ms)

    # How long until next close, with a small cushion
    remain_ms = max(0, next_close_ts - server_now_ms)
    poll_after_ms = max(2_000, min(remain_ms + 500, 5_000))

    # ---- Final return ----
    try:
        broker_obj_final = BrokerMeta(**(broker_safe or {})) if broker_safe else None
    except Exception:
        broker_obj_final = None

    # prefer the computed last_closed_ts / next_close_ts; fall back to preview/easy hints
    # Use the canonically computed last_closed_ts / next_close_ts from the block above
    # Use the canonically computed last_closed_ts / next_close_ts from the block above
    try:
        _last_closed_out = int(
            last_closed_ts
            or locals().get("last_closed_ms")
            or (preview.lastClosedTs if hasattr(preview, "lastClosedTs") else 0)
            or 0
        )
    except Exception:
        _last_closed_out = 0

    # We've already computed the correct next_close_ts in server ms
    _next_close_out = int(next_close_ts)

    return {
        "label":        str(label or "Neutral"),
        "score":        float(score or 0.0),
        "serverNow":    int(server_now_ms),
        "lastClosedTs": _last_closed_out,
        "nextCloseTs":  _next_close_out,
        "diagnostics":  (diagnostics or {}),
        "stale":        bool(stale),
        "preview":      preview_out,                 # PreviewPayload object is fine; FastAPI will serialize
        "broker":       broker_obj_final,            # may be None if not available
        "adx":          (locals().get("adx_val")),
        "slope":        (locals().get("slope_val")),
        "structure":    (locals().get("structure_val")),
        "sr":           sr_summary,
        "pollAfterMs":  int(poll_after_ms),
        "usingDevice": prefer_dev,
    }




    # debug: which source and whether forming included
    try:
        if prev_rows:
            last = prev_rows[-1]
            src_used = "broker" if prev_rows_override else "snapshot"
            log.info(
                f"[TREND] preview {src_used} tf={tf} "
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

    # preview bars: accept either top-level "bars" or nested "preview": {"bars":[...]}
    if isinstance(snap.get("preview"), dict) and isinstance(snap["preview"].get("bars"), list):
        preview_bars = snap["preview"]["bars"] or []
    else:
        preview_bars = snap.get("bars") or []

    preview = {"bars": preview_bars}

    # --- NEW: trust preview bars for lastClosedTs if they are fresher ---
    try:
        latest_closed_from_preview = 0

        # walk from tail to find the last *closed* bar
        for b in reversed(preview_bars):
            if not isinstance(b, dict):
                continue

            # ignore explicitly-forming bars
            if b.get("complete") is False:
                continue

            t_close_ms = int(b.get("t_close_ms") or 0)
            if not t_close_ms:
                # fallback: derive from open time + TF
                t_open_ms = int(b.get("t_open_ms") or 0)
                if t_open_ms:
                    t_close_ms = t_open_ms + TF_MS

            if t_close_ms:
                latest_closed_from_preview = t_close_ms
                break

        # if preview has a newer closed bar than the snapshot header, use it
        if latest_closed_from_preview and latest_closed_from_preview > last_closed:
            last_closed = latest_closed_from_preview
            next_close = last_closed + TF_MS

    except Exception:
        # never break the endpoint because of a bad bar
        pass

    # keep next_close slightly ahead of "now" so countdown doesn't go negative
    if next_close - server_now_ms < 1000:
        next_close += TF_MS

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


@router.get("/predict/4h_debug")
def predict_4h_debug(
    symbol: str = Query("EURUSD"),
    user = Depends(require_auth_optional),
):
    """
    Debug helper: compare H4 model move_pct vs recent H4 realised volatility.
    """
    sym = (symbol or "EURUSD").upper()
    user_id = getattr(user, "user_id", None) if user else None

    # 1) Get the latest H4 bars from snap (same mechanism as other endpoints)
    snap, broker = _read_freshest_snap_for_user_or_any(user_id, sym, "H4")
    if not snap:
        return {"ok": False, "reason": "no_h4_snap"}

    bars = snap.get("bars") or []
    if not bars:
        return {"ok": False, "reason": "empty_h4_bars"}

    tf_ms = _tf_ms_from_u("H4")
    now_ms = int(time.time() * 1000)

    opens: list[float] = []
    closes: list[float] = []
    ranges_pct: list[float] = []

    # Use ONLY CLOSED bars, and treat bars as dicts: {t,o,h,l,c,complete}
    for b in bars:
        t_ms = _ms_from_t(b.get("t_open_ms") or b.get("t"))
        if t_ms is None:
            continue

        is_closed = (b.get("complete") is True) or (t_ms + tf_ms <= now_ms)
        if not is_closed:
            continue

        try:
            o = float(b.get("o"))
            h = float(b.get("h"))
            l = float(b.get("l"))
            c = float(b.get("c"))
        except (TypeError, ValueError):
            continue

        opens.append(o)
        closes.append(c)
        if o:
            ranges_pct.append(100.0 * abs(h - l) / abs(o))

    if len(closes) < 20 or len(opens) < 20:
        return {
            "ok": False,
            "reason": "not_enough_closed_h4_bars",
            "bars": len(closes),
        }

    # Only last 20 closed bars
    opens_tail = opens[-20:]
    closes_tail = closes[-20:]
    last_close = closes_tail[-1]

    # Realised candle body moves in %
    moves_pct = [100.0 * (c - o) / o for o, c in zip(opens_tail, closes_tail)]
    max_abs_move = max(abs(m) for m in moves_pct)

    # ATR-like avg range %
    if ranges_pct:
        ranges_tail = ranges_pct[-20:]
        avg_range_pct = sum(ranges_tail) / len(ranges_tail)
    else:
        avg_range_pct = None

    # 2) Get model prediction for H4
    try:
        from api.trend.infer_rt import predict_next_4h
        pr4 = predict_next_4h(sym)
    except Exception as e:
        return {"ok": False, "reason": "h4_model_error", "detail": str(e)}

    mv4 = pr4.get("move_pct") or pr4.get("movePct")
    tp4 = pr4.get("targetPrice") or pr4.get("target_price")

    return {
        "ok": True,
        "symbol": sym,
        "last_close": last_close,
        "model_move_pct": mv4,
        "model_target_price": tp4,
        "max_abs_move_pct_last_20": max_abs_move,
        "avg_range_pct_last_20": avg_range_pct,
        "raw": pr4,
    }

def _evaluate_alert_outcome(sym: str, snap: dict, row: dict, now_ms: int):
    """
    Evaluates whether an opportunity HIT target or EXPIRED.
    Snapshot values from Redis are JSON-encoded (and often bytes).
    Direction is UP/DOWN (not BUY/SELL).
    """

    def _sj(key: str, default=None):
        v = snap.get(key)
        if v is None:
            return default
        if isinstance(v, (bytes, bytearray)):
            v = v.decode("utf-8", "ignore")
        try:
            return json.loads(v)
        except Exception:
            return v

    direction = (_sj("opp_direction") or _sj("direction") or "").upper()
    if direction not in ("UP", "DOWN"):
        return

    alert_id = _sj("alert_id")
    if not alert_id:
        return

    try:
        target = float(_sj("target_price_1h"))
    except Exception:
        target = None

    try:
        basis = float(_sj("alert_price_1h") or _sj("basis_price"))
    except Exception:
        basis = None

    try:
        alert_ms = int(_sj("alert_created_ms") or 0)
    except Exception:
        alert_ms = 0

    try:
        horizon_min = int(_sj("horizon_min") or 60)
    except Exception:
        horizon_min = 60

    # last_price preference: snapshot -> row -> basis
    last_price = None
    try:
        lp = _sj("last_price")
        if isinstance(lp, (int, float)):
            last_price = float(lp)
    except Exception:
        last_price = None

    if last_price is None and isinstance(row.get("last_close"), (int, float)):
        last_price = float(row["last_close"])

    if last_price is None and isinstance(basis, (int, float)):
        last_price = float(basis)

    if last_price is None:
        last_price = 0.0

    realized_move_pct = None
    if isinstance(basis, (int, float)) and basis:
        realized_move_pct = (last_price - basis) / basis * 100.0

    sym_u = (sym or "").upper()
    snap_key = _opp_snapshot_key(sym_u, direction)

    # HIT
    hit = False
    if isinstance(target, (int, float)):
        if direction == "UP" and last_price >= target:
            hit = True
        elif direction == "DOWN" and last_price <= target:
            hit = True

    if hit:
        try:
            _mark_alert_hit(alert_id, realized_move_pct, now_ms)

            key = f"{ALERT_HASH_PREFIX}{alert_id}"
            extra = {
                "status": json.dumps("hit"),
                "hit_target": json.dumps(True),
                "hit_ts": json.dumps(now_ms),
                "time_to_target_min": json.dumps((now_ms - alert_ms) / 60000.0 if alert_ms else None),
                "last_price": json.dumps(last_price),
            }
            R.hset(key, mapping=extra)

            R.hset(snap_key, mapping={"status": json.dumps("hit")})
        except Exception:
            pass

        try:
            _delete_live_snapshot(sym_u, direction)
        except Exception:
            pass
        return

    # EXPIRED
    if alert_ms and (now_ms - alert_ms >= horizon_min * 60_000):
        try:
            _mark_alert_expired(alert_id, now_ms)

            key = f"{ALERT_HASH_PREFIX}{alert_id}"
            extra = {
                "status": json.dumps("expired"),
                "hit_target": json.dumps(False),
                "expired_ts": json.dumps(now_ms),
                "time_to_target_min": json.dumps(float(horizon_min)),
                "last_price": json.dumps(last_price),
            }
            if realized_move_pct is not None:
                extra["realized_move_pct"] = json.dumps(realized_move_pct)
            R.hset(key, mapping=extra)

            R.hset(snap_key, mapping={"status": json.dumps("expired")})
        except Exception:
            pass

        try:
            _delete_live_snapshot(sym_u, direction)
        except Exception:
            pass
        return
