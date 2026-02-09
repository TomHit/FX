

# -*- coding: utf-8 -*-
from __future__ import annotations

# --- EARLY MS NORMALIZER (must be above any circular imports) ---
def _to_ms_any(x) -> int:
    """
    Normalize epoch-ish values to milliseconds.
    Safe under partial/circular imports (no external deps).
    Accepts sec/ms/us/ns.
    """
    try:
        xi = int(x or 0)
    except Exception:
        return 0
    if xi <= 0:
        return 0
    # ns -> ms
    if xi >= 1_000_000_000_000_000_000:
        return xi // 1_000_000
    # us -> ms
    if xi >= 1_000_000_000_000_000:
        return xi // 1_000
    # ms
    if xi >= 1_000_000_000_000:
        return xi
    # sec -> ms
    return xi * 1000

def _pick_last_closed_bar_from_bars(bars_in, now_ms: int, tf_ms: int):
    """
    Return (c, p) where:
      c = last safely CLOSED bar (dict)
      p = previous bar (dict)
    bars_in is expected newest last (sorted), but we still walk backward robustly.

    Supports both schemas:
      - close-time bars: {"t_close_ms": <ms>, "complete": true}
      - open-time bars:  {"t": <sec>, ...}  -> close computed as (t*1000 + tf_ms)
    """
    if not isinstance(bars_in, list) or len(bars_in) < 2:
        return None, None

    now_ms = int(now_ms or 0)
    tf_ms = int(tf_ms or 0) or (60 * 60 * 1000)

    # ---- inline to-ms (avoid any global rebind/closure surprises) ----
    def _to_ms(v):
        try:
            if v is None:
                t_ms = 0
            elif isinstance(v, (int, float)):
                t_ms = int(v)
            else:
                sv = str(v).strip()
                t_ms = int(float(sv)) if sv else 0

            # normalize units -> ms
            if 0 < t_ms < 10_000_000_000:              # seconds
                t_ms *= 1000
            elif t_ms > 10_000_000_000_000_000:        # ns
                t_ms //= 1_000_000
            elif t_ms > 10_000_000_000_000:            # us
                t_ms //= 1000
            return int(t_ms)
        except Exception:
            return 0

    for i in range(len(bars_in) - 1, 0, -1):
        b = bars_in[i]
        if not isinstance(b, dict):
            continue

        # ignore explicit forming bar
        if b.get("complete") is False:
            continue

        # 1) prefer close-time fields
        t_close_raw = b.get("t_close_ms") or b.get("tCloseMs") or b.get("tClose") or b.get("t_close")
        t_close_ms = _to_ms(t_close_raw)

        # 2) else compute close from open-time fields (t usually in seconds)
        if t_close_ms <= 0:
            t_open_raw = b.get("t") or b.get("time") or b.get("ts")
            t_open_ms = _to_ms(t_open_raw)
            if t_open_ms > 0:
                t_close_ms = int(t_open_ms + tf_ms)

        # 3) final fallback (legacy)
        if t_close_ms <= 0:
            t_raw = (
                b.get("t_close_ms") or b.get("tClose") or b.get("t_close") or b.get("ts")
                or b.get("t") or b.get("time") or 0
            )
            t_close_ms = _to_ms(t_raw)

        if t_close_ms <= 0:
            continue

        # must be safely closed (tiny buffer)
        if t_close_ms > now_ms - max(5_000, int(0.05 * tf_ms)):
            continue

        # too stale => treat as missing (RETURN TUPLE)
        if (now_ms - t_close_ms) > int(3.0 * tf_ms):
            return None, None

        return b, bars_in[i - 1]

    return None, None

from typing import Literal, List, Tuple, Optional, Any, Dict
from fastapi import APIRouter, HTTPException, Depends, Query, Request, Header
from pydantic import BaseModel, Field, validator
from api.pulse import build_pulse
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
import traceback
from pathlib import Path
import xgboost as xgb
from api.macro_state import get_macro_snapshot
import csv
import pandas as pd
from datetime import datetime, timezone
from api.entry_logic import entry_decision_m1
from api.trend.infer_rt import (
    predict_next_hour,
    predict_next_4h,
    pull_latest_h1,
    pull_latest_h4,
)
from .trend_sr import summarize_sr_multi_tf
from api.trend.infer_tth import predict_tth
router = APIRouter(prefix="/trend")

log = logging.getLogger("xtl.trend")

# ---- LOAD MARKER (TEMP) ----
try:
    import hashlib, inspect
    _src = open(__file__, "rb").read()
    _sha = hashlib.sha1(_src).hexdigest()[:12]
    log.error("TREND_ENDPOINTS_LOADED file=%s sha=%s pick_line=%s to_ms_line=%s",
              __file__, _sha,
              getattr(_pick_last_closed_bar_from_bars, "__code__", None).co_firstlineno if hasattr(_pick_last_closed_bar_from_bars, "__code__") else None,
              getattr(_to_ms_any, "__code__", None).co_firstlineno if hasattr(_to_ms_any, "__code__") else None)
except Exception as _e:
    try:
        log.error("TREND_ENDPOINTS_LOADED marker_failed err=%s", _e)
    except Exception:
        pass
# ---- /LOAD MARKER ----


REG_PATH = Path("/opt/xauapi/api/trend/models/xgb_reg.json")
CLS_PATH = Path("/opt/xauapi/api/trend/models/xgb_cls.json")
# --- Optional OpenAI (commentary only). NEVER fail module import if missing ---
try:
    from openai import OpenAI  # type: ignore
except Exception:
    OpenAI = None  # type: ignore

# --------------------------
# Discord webhook (optional)
# --------------------------
# Set in /etc/xauapi.env (or systemd EnvironmentFile):
#   DISCORD_WEBHOOK_URL="https://discord.com/api/webhooks/...."
# If not set, Discord notifications are simply skipped.
DISCORD_WEBHOOK_URL = (os.getenv("DISCORD_WEBHOOK_URL") or os.getenv("XTL_DISCORD_WEBHOOK_URL") or "").strip()

def _fmt_price(x: Any) -> str:
    try:
        v = float(x)
    except Exception:
        return "NA"
    # Keep reasonable precision across FX + XAU
    if abs(v) >= 1000:
        return f"{v:.2f}"
    if abs(v) >= 100:
        return f"{v:.3f}"
    return f"{v:.5f}"

import os

def is_h4_enabled() -> bool:
    return str(os.getenv("ENABLE_H4_MODEL", "0")).strip().lower() in (
        "1", "true", "yes", "y", "on"
    )

ENABLE_H4_MODEL = is_h4_enabled()
log.info(f"[TREND] ENABLE_H4_MODEL={ENABLE_H4_MODEL}")

def _discord_post(content: str) -> bool:
    """Best-effort Discord webhook post. Never raises."""
    if not DISCORD_WEBHOOK_URL:
        return False
    try:
        import urllib.request
        data = json.dumps({"content": content}).encode("utf-8")
        req = urllib.request.Request(
            DISCORD_WEBHOOK_URL,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=4) as resp:
            _ = resp.read()
        return True
    except Exception:
        return False

def _discord_entry_msg(sym: str, sig: str, entry_price: Any, tp_price: Any, sl_price: Any, ts_ms: int, reason: str | None = None) -> str:
    ts_s = ""
    try:
        if ts_ms:
            ts_s = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts_ms / 1000.0))
    except Exception:
        ts_s = ""
    parts = [
        f"**{sym}** - **{sig}**",
        f"Entry: `{_fmt_price(entry_price)}`",
        f"TP: `{_fmt_price(tp_price)}`",
        f"SL: `{_fmt_price(sl_price)}`",
    ]
    if ts_s:
        parts.append(f"Time: `{ts_s}`")
    if reason:
        parts.append(f"Reason: `{reason}`")
    return " | ".join(parts)

def _discord_status_msg(sym: str, status: str, last_price: Any, realized_move_pct: Any, ts_ms: int) -> str:
    ts_s = ""
    try:
        if ts_ms:
            ts_s = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts_ms / 1000.0))
    except Exception:
        ts_s = ""
    st = status.upper()
    parts = [
        f"**{sym}** - **{st}**",
        f"Last: `{_fmt_price(last_price)}`",
    ]
    try:
        if realized_move_pct is not None:
            parts.append(f"Move: `{float(realized_move_pct):+.2f}%`")
    except Exception:
        pass
    if ts_s:
        parts.append(f"Time: `{ts_s}`")
    return " | ".join(parts)


_LASTGOOD_H1_KEY = "xtl:trend:lastgood:h1:{sym}"
_LASTGOOD_H4_KEY = "xtl:trend:lastgood:h4:{sym}"

def _rg_lastgood(sym: str, scope: str) -> dict | None:
    try:
        k = (_LASTGOOD_H1_KEY if scope == "H1" else _LASTGOOD_H4_KEY).format(sym=sym)
        raw = R.get(k)
        if not raw:
            return None
        d = json.loads(raw) if isinstance(raw, str) else None
        return d if isinstance(d, dict) else None
    except Exception:
        return None

def _rs_lastgood(sym: str, scope: str, pr: dict, ttl_sec: int = 3600) -> None:
    try:
        k = (_LASTGOOD_H1_KEY if scope == "H1" else _LASTGOOD_H4_KEY).format(sym=sym)
        R.setex(k, int(ttl_sec), json.dumps(pr, default=str))
    except Exception:
        pass

def _is_transient_insufficient(pr: dict) -> bool:
    """
    True for temporary model failures we should NOT show to UI if we have last-good.
    Examples: insufficient_data, h1_not_loaded, missing frames, etc.
    """
    try:
        if not isinstance(pr, dict):
            return False
        if bool(pr.get("ok", False)):
            return False
        r = str(pr.get("reason") or pr.get("detail") or "").lower()
        return ("insufficient" in r) or ("not_loaded" in r) or ("missing" in r)
    except Exception:
        return False


def _log_trade_outcome(payload: dict) -> None:
    """
    Append a compact outcome record into Redis for quick stats.
    Keeps last N outcomes per day.
    """
    try:
        sym = str(payload.get("symbol") or "").upper().strip() or "NA"
        status = str(payload.get("status") or "").lower().strip() or "na"
        uid = str(payload.get("user_id") or payload.get("uid") or "global")
        day = time.strftime("%Y%m%d", time.gmtime(int(payload.get("updated_ms") or payload.get("hit_ts_ms") or payload.get("expired_ts_ms") or payload.get("sl_hit_ts_ms") or time.time()*1000)/1000.0))
        key = f"xtl:outcomes:{uid}:{day}"

        rec = {
            "ts_ms": int(payload.get("updated_ms") or payload.get("hit_ts_ms") or payload.get("expired_ts_ms") or payload.get("sl_hit_ts_ms") or time.time()*1000),
            "symbol": sym,
            "status": status,  # hit | sl_hit | expired
            "direction": str(payload.get("opp_direction") or payload.get("direction") or ""),
            "entry_signal": payload.get("entry_signal"),
            "entry_price": payload.get("entry_price"),
            "tp_price": payload.get("tp_price"),
            "sl_price": payload.get("sl_price"),
            "last_price": payload.get("last_price"),
            "realized_move_pct": payload.get("realized_move_pct"),
            "alert_id": payload.get("alert_id"),
        }

        R.rpush(key, json.dumps(rec))
        # keep last 2000 records/day
        R.ltrim(key, -2000, -1)
        # expire in 14 days
        R.expire(key, 14 * 24 * 3600)
    except Exception:
        pass
def _json_load_maybe(x):
    if x is None:
        return None
    if isinstance(x, (bytes, bytearray)):
        x = x.decode("utf-8", "ignore")
    try:
        return json.loads(x)
    except Exception:
        return x

def _json_load_twice(x):
    y = _json_load_maybe(x)
    if isinstance(y, str):
        return _json_load_maybe(y)
    return y

def _pick_entry_sr_levels(
    sr: dict,
    px: float | None,
    top_n: int = 4,
    atr: float | None = None,
    
) -> dict:
    """
    Entry SR selection (price-aware) 

    Primary selection (strict):
      SUPPORT (below px): H1 supports_near -> H1 supports_major -> H4 supports_near -> H4 supports_major
      RESIST (above px):  H1 resist_near   -> H1 resist_major   -> H4 resist_near   -> H4 resist_major
      Fallback: flipped levels.

    Schema support:
      - Works with BOTH {supports_near/supports_major/resistances_near/resistances_major}
        and legacy {supports/resistances}

    Band logic (for UI + gating):
      
      - collects levels that fall inside [px-band_w, px+band_w]
      - includes flipped levels inside band too
    """

    out = {
        "entry_support": None,
        "entry_support_tf": None,
        "entry_support_kind": None,
        "entry_support_near_levels": [],
        "entry_support_major_levels": [],
        "entry_support_flipped_levels": [],

        "entry_resistance": None,
        "entry_resistance_tf": None,
        "entry_resistance_kind": None,
        "entry_resistance_near_levels": [],
        "entry_resistance_major_levels": [],
        "entry_resistance_flipped_levels": [],

        
    }

    if not isinstance(sr, dict) or not sr:
        return out

    try:
        px0 = float(px) if px is not None else None
    except Exception:
        px0 = None
    if not px0 or px0 <= 0:
        return out

    top_n = max(0, int(top_n))

    

    # ---------- helpers ----------
    def _levels_from_bucket(xs) -> list[float]:
        """Accepts list[dict{'level':...}] or list[float]."""
        vals: list[float] = []
        for x in xs or []:
            try:
                if isinstance(x, dict):
                    v = x.get("level")
                else:
                    v = x
                if v is None:
                    continue
                vals.append(float(v))
            except Exception:
                continue
        return vals

    def _get_levels(tf_obj: dict, *keys: str) -> list[float]:
        vals: list[float] = []
        if not isinstance(tf_obj, dict):
            return vals
        for k in keys:
            vals += _levels_from_bucket(tf_obj.get(k) or [])
        # unique
        return sorted(set(vals))

    def _below_levels(levels: list[float]) -> list[float]:
        vals = sorted({v for v in levels if v < px0}, reverse=True)  # nearest below first
        return vals[:top_n]

    def _above_levels(levels: list[float]) -> list[float]:
        vals = sorted({v for v in levels if v > px0})  # nearest above first
        return vals[:top_n]

    

    # flipped:
    # - resistances now BELOW px can behave like support after reclaim
    # - supports now ABOVE px can behave like resistance after breakdown
    def _flipped_support_from_res(tf_obj: dict) -> list[float]:
        levels = _get_levels(tf_obj, "resistances_major", "resistances_near", "resistances")
        return _below_levels(levels)

    def _flipped_res_from_supp(tf_obj: dict) -> list[float]:
        levels = _get_levels(tf_obj, "supports_major", "supports_near", "supports")
        return _above_levels(levels)

    # ---------- pull tf objects ----------
    h1 = sr.get("h1") if isinstance(sr.get("h1"), dict) else {}
    h4 = sr.get("h4") if isinstance(sr.get("h4"), dict) else {}

    # ---------- strict candidates (prefer near/major if present, else fall back to legacy) ----------
    h1_supp_near_levels_all  = _get_levels(h1, "supports_near", "supports")
    h1_supp_major_levels_all = _get_levels(h1, "supports_major", "supports")

    h4_supp_near_levels_all  = _get_levels(h4, "supports_near", "supports")
    h4_supp_major_levels_all = _get_levels(h4, "supports_major", "supports")

    h1_res_near_levels_all   = _get_levels(h1, "resistances_near", "resistances")
    h1_res_major_levels_all  = _get_levels(h1, "resistances_major", "resistances")

    h4_res_near_levels_all   = _get_levels(h4, "resistances_near", "resistances")
    h4_res_major_levels_all  = _get_levels(h4, "resistances_major", "resistances")

    h1_supp_near  = _below_levels(h1_supp_near_levels_all)
    h1_supp_major = _below_levels(h1_supp_major_levels_all)
    h4_supp_near  = _below_levels(h4_supp_near_levels_all)
    h4_supp_major = _below_levels(h4_supp_major_levels_all)

    h1_res_near   = _above_levels(h1_res_near_levels_all)
    h1_res_major  = _above_levels(h1_res_major_levels_all)
    h4_res_near   = _above_levels(h4_res_near_levels_all)
    h4_res_major  = _above_levels(h4_res_major_levels_all)

    # ---------- flipped ----------
    h1_flip_supp = _flipped_support_from_res(h1)
    h4_flip_supp = _flipped_support_from_res(h4)
    h1_flip_res  = _flipped_res_from_supp(h1)
    h4_flip_res  = _flipped_res_from_supp(h4)


    # -------------------------
    # SUPPORT ladder (strict first)
    # -------------------------
    if h1_supp_near:
        out["entry_support_tf"] = "H1"
        out["entry_support_kind"] = "near"
        out["entry_support_near_levels"] = h1_supp_near
        out["entry_support_major_levels"] = h1_supp_major
        out["entry_support"] = h1_supp_near[0]
    elif h1_supp_major:
        out["entry_support_tf"] = "H1"
        out["entry_support_kind"] = "major"
        out["entry_support_near_levels"] = h1_supp_near
        out["entry_support_major_levels"] = h1_supp_major
        out["entry_support"] = h1_supp_major[0]
    elif h4_supp_near:
        out["entry_support_tf"] = "H4"
        out["entry_support_kind"] = "near"
        out["entry_support_near_levels"] = h4_supp_near
        out["entry_support_major_levels"] = h4_supp_major
        out["entry_support"] = h4_supp_near[0]
    elif h4_supp_major:
        out["entry_support_tf"] = "H4"
        out["entry_support_kind"] = "major"
        out["entry_support_near_levels"] = h4_supp_near
        out["entry_support_major_levels"] = h4_supp_major
        out["entry_support"] = h4_supp_major[0]
    else:
        # fallback: flipped supports (H1 then H4)
        if h1_flip_supp:
            out["entry_support_tf"] = "H1"
            out["entry_support_kind"] = "flipped"
            out["entry_support"] = h1_flip_supp[0]
        elif h4_flip_supp:
            out["entry_support_tf"] = "H4"
            out["entry_support_kind"] = "flipped"
            out["entry_support"] = h4_flip_supp[0]
        

    # -------------------------
    # RESISTANCE ladder (strict first)
    # -------------------------
    if h1_res_near:
        out["entry_resistance_tf"] = "H1"
        out["entry_resistance_kind"] = "near"
        out["entry_resistance_near_levels"] = h1_res_near
        out["entry_resistance_major_levels"] = h1_res_major
        out["entry_resistance"] = h1_res_near[0]
    elif h1_res_major:
        out["entry_resistance_tf"] = "H1"
        out["entry_resistance_kind"] = "major"
        out["entry_resistance_near_levels"] = h1_res_near
        out["entry_resistance_major_levels"] = h1_res_major
        out["entry_resistance"] = h1_res_major[0]
    elif h4_res_near:
        out["entry_resistance_tf"] = "H4"
        out["entry_resistance_kind"] = "near"
        out["entry_resistance_near_levels"] = h4_res_near
        out["entry_resistance_major_levels"] = h4_res_major
        out["entry_resistance"] = h4_res_near[0]
    elif h4_res_major:
        out["entry_resistance_tf"] = "H4"
        out["entry_resistance_kind"] = "major"
        out["entry_resistance_near_levels"] = h4_res_near
        out["entry_resistance_major_levels"] = h4_res_major
        out["entry_resistance"] = h4_res_major[0]
    else:
        # fallback: flipped resistances (H1 then H4)
        if h1_flip_res:
            out["entry_resistance_tf"] = "H1"
            out["entry_resistance_kind"] = "flipped"
            out["entry_resistance"] = h1_flip_res[0]
        elif h4_flip_res:
            out["entry_resistance_tf"] = "H4"
            out["entry_resistance_kind"] = "flipped"
            out["entry_resistance"] = h4_flip_res[0]
        

    # -------------------------
    # flipped lists aligned to chosen TF (UI/debug)
    # -------------------------
    supp_tf = out.get("entry_support_tf")
    res_tf  = out.get("entry_resistance_tf")

    if supp_tf == "H1":
        out["entry_support_flipped_levels"] = h1_flip_supp or []
    elif supp_tf == "H4":
        out["entry_support_flipped_levels"] = h4_flip_supp or []
    else:
        out["entry_support_flipped_levels"] = h1_flip_supp or h4_flip_supp or []

    if res_tf == "H1":
        out["entry_resistance_flipped_levels"] = h1_flip_res or []
    elif res_tf == "H4":
        out["entry_resistance_flipped_levels"] = h4_flip_res or []
    else:
        out["entry_resistance_flipped_levels"] = h1_flip_res or h4_flip_res or []

    return out


def _disable_tp_sl_fields(r: dict) -> None:
    if not isinstance(r, dict):
        return
    r["tp_price"] = None
    r["sl_price"] = None
    r["target_price"] = None
    r["target_price_1h"] = None
    r["stop_loss"] = None
    r["stop_loss_1h"] = None



def _snap_key(dev: str, sym: str, tf: str) -> str:
    dev = str(dev or "").strip().strip('"').strip("'").replace("\n", "").replace("\r", "")
    sym = str(sym or "").upper().strip()
    tf  = str(tf or "").upper().strip()
    return f"xtl:ohlc:snap:{dev}:{sym}:{tf}"


def _load_device_h1_bars(sym: str, dev_id: str) -> tuple[list[dict], str]:
    """
    Hard source of truth for H1 gate:
    - reads ONLY device-scoped key
    - supports STRING or HASH storage (via _snap_get_raw_json)
    - returns normalized + sorted bars (ms)
    """
    sym_u = (sym or "").upper().strip()
    dev = (dev_id or "").strip()
    if not sym_u or not dev:
        return [], ""

    key = f"xtl:ohlc:snap:{dev}:{sym_u}:H1"
    raw = _snap_get_raw_json(key)
    if not raw:
        return [], key

    try:
        obj = json.loads(raw)
    except Exception:
        return [], key

    bars = None
    if isinstance(obj, dict):
        bars = obj.get("bars") or obj.get("ohlc")
    elif isinstance(obj, list):
        bars = obj

    bars = bars if isinstance(bars, list) else []
    if not bars:
        return [], key

    try:
        nb = _normalize_snap_bars_to_ms(bars, 60 * 60 * 1000)
    except Exception:
        nb = bars

    # enforce sort by close time
    out = []
    for b in (nb or []):
        if not isinstance(b, dict):
            continue
        if not all(k in b for k in ("o", "h", "l", "c")):
            continue
        tcm = b.get("t_close_ms") or b.get("tClose") or b.get("t") or 0
        try:
            tcm = int(tcm)
        except Exception:
            tcm = 0
        if 0 < tcm < 10_000_000_000:
            tcm *= 1000
        b["t_close_ms"] = int(tcm)
        out.append(b)

    out.sort(key=lambda x: int(x.get("t_close_ms") or 0))
    return out, key

def _get_sr_bundle(sym: str, prefer_dev: str | None = None, return_src: bool = False):
    """
    SR bundle getter:
      - Prefer Redis cache: last_good -> last
      - If missing: compute from OHLC snaps (H1/H4) and write caches.
      - IMPORTANT: if prefer_dev is provided, try device-scoped OHLC snaps from that device first.
    Returns:
      - dict (default)
      - OR (dict, src_str) if return_src=True
    """
    src = None
    try:
        sym_u = (sym or "").upper().strip()
        if not sym_u:
            return ({}, "empty_symbol") if return_src else {}

        # 1) prefer last_good, fallback to last
        for k in (f"xtl:sr:bundle:last_good:{sym_u}", f"xtl:sr:bundle:last:{sym_u}"):
            try:
                raw = R.get(k)
            except Exception:
                raw = None
            if not raw:
                continue
            js = _json_load_twice(raw)
            if isinstance(js, dict) and js:
                src = f"cache:{k}"
                return (js, src) if return_src else js

        # helper: read TF bars from a given device
        def _read_tf_bars_from_dev(dev_id: str, tfu: str):
            snap_key = f"xtl:ohlc:snap:{dev_id}:{sym_u}:{tfu}"
            try:
                raw = R.get(snap_key)

                # DEBUG: ensure we can see what's going on
                if not raw:
                    return None, snap_key, "raw=None"

                try:
                    s = _json_load_twice(raw)
                except Exception as e:
                    return None, snap_key, f"json_exc:{type(e).__name__}:{e}"

                if not isinstance(s, dict):
                    return None, snap_key, f"json_not_dict:{type(s).__name__}"

                bars = s.get("bars") or s.get("ohlc")
                if not isinstance(bars, list) or not bars:
                    return None, snap_key, f"bars_missing_or_empty:{type(bars).__name__}"

                try:
                    tf_ms = 60 * 60 * 1000 if tfu == "H1" else 4 * 60 * 60 * 1000
                    nb = _normalize_snap_bars_to_ms(bars, tf_ms)
                    if isinstance(nb, tuple):
                        nb = nb[0]
                except Exception as e:
                    return None, snap_key, f"norm_exc:{type(e).__name__}:{e}"

                if not isinstance(nb, list) or not nb:
                    return None, snap_key, "norm_empty"

                return nb, snap_key, "ok"

            except Exception as e:
                return None, snap_key, f"exc:{type(e).__name__}:{e}"
        # 2A) Try preferred device first (THIS is the key fix)
        pd = (prefer_dev or "").strip()
        if pd:
            h1_bars, h1_key, h1_dbg = _read_tf_bars_from_dev(pd, "H1")
            h4_bars, h4_key, h4_dbg = _read_tf_bars_from_dev(pd, "H4")
            if (isinstance(h1_bars, list) and h1_bars) or (isinstance(h4_bars, list) and h4_bars):
                # build df-ish via existing converters you already use elsewhere
                try:
                    h1_df = _rows_to_df(h1_bars) if h1_bars else None
                except Exception:
                    h1_df = None
                try:
                    h4_df = _rows_to_df(h4_bars) if h4_bars else None
                except Exception:
                    h4_df = None

                # price: try live price from that same device (consistent)
                px = None
                try:
                    px, _ts = _get_live_price(sym_u, pd)
                    px = float(px) if isinstance(px, (int, float)) else None
                except Exception:
                    px = None

                pip_factor = 0.01 if sym_u == "XAUUSD" else (0.01 if sym_u.endswith("JPY") else 0.0001)
                b = summarize_sr_multi_tf(
                    symbol=sym_u,
                    price=px,
                    h4_df=_to_hlc(h4_df),
                    h1_df=_to_hlc(h1_df),
                    pip_factor=float(pip_factor),
                    cache=R,
                    cache_ttl_sec=900,
                    good_ttl_sec=7 * 24 * 3600,
                )
                if isinstance(b, dict) and b:
                    src = f"compute:prefer_dev:{pd}|h1={h1_key}|h4={h4_key}"
                    return (b, src) if return_src else b
                # if preferred device had bars but SR still empty, keep going to fallback
                src = f"compute_empty:prefer_dev:{pd}|h1={h1_key}|h4={h4_key}"

        # 2B) Fallback: Pick an online device (best heartbeat)
        dev = None
        try:
            best_dev = None
            best_hb = -1
            for key in R.scan_iter("device:dev_*"):
                try:
                    h = R.hgetall(key) or {}
                except Exception:
                    h = {}
                if not h:
                    continue

                status = h.get(b"status") or h.get("status")
                if isinstance(status, (bytes, bytearray)):
                    status = status.decode("utf-8", "ignore")
                if (status or "").strip().lower() != "online":
                    continue

                hb = h.get(b"last_heartbeat_ms") or h.get("last_heartbeat_ms")
                if isinstance(hb, (bytes, bytearray)):
                    hb = hb.decode("utf-8", "ignore").strip()
                try:
                    hb_i = int(hb) if hb not in (None, "") else -1
                except Exception:
                    hb_i = -1

                key_s = key.decode("utf-8", "ignore") if isinstance(key, (bytes, bytearray)) else str(key)
                dev_id = key_s.replace("device:", "").strip()

                if hb_i > best_hb:
                    best_hb = hb_i
                    best_dev = dev_id

            dev = best_dev
        except Exception:
            dev = None

        if not dev:
            src = src or "no_online_device"
            return ({}, src) if return_src else {}

        h1_bars, h1_key, h1_dbg = _read_tf_bars_from_dev(dev, "H1")
        h4_bars, h4_key, h4_dbg = _read_tf_bars_from_dev(dev, "H4")

        if not ((isinstance(h1_bars, list) and h1_bars) or (isinstance(h4_bars, list) and h4_bars)):
            src = f"no_bars:any_dev:{dev}|h1={h1_key}|h1dbg={h1_dbg}|h4={h4_key}|h4dbg={h4_dbg}"
            return ({}, src) if return_src else {}

        try:
            h1_df = _rows_to_df(h1_bars) if h1_bars else None
        except Exception:
            h1_df = None
        try:
            h4_df = _rows_to_df(h4_bars) if h4_bars else None
        except Exception:
            h4_df = None

        px = None
        try:
            px, _ts = _get_live_price(sym_u, dev)
            px = float(px) if isinstance(px, (int, float)) else None
        except Exception:
            px = None

        pip_factor = 0.01 if sym_u == "XAUUSD" else (0.01 if sym_u.endswith("JPY") else 0.0001)
        b = summarize_sr_multi_tf(
            symbol=sym_u,
            price=px,
            h4_df=_to_hlc(h4_df),
            h1_df=_to_hlc(h1_df),
            pip_factor=float(pip_factor),
            cache=R,
            cache_ttl_sec=900,
            good_ttl_sec=7 * 24 * 3600,
        )
        if isinstance(b, dict) and b:
            src = f"compute:any_dev:{dev}|h1={h1_key}|h4={h4_key}"
            return (b, src) if return_src else b

        src = f"compute_empty:any_dev:{dev}|h1={h1_key}|h4={h4_key}"
        return ({}, src) if return_src else {}

    except Exception as e:
        src = f"exc:{type(e).__name__}:{e}"
        return ({}, src) if return_src else {}



def _get_closed_h1_bars(sym: str, dev: str | None) -> list[dict]:
    try:
        sym_u = (sym or "").upper().strip()
        dev = (dev or "").strip()
        if not sym_u or not dev:
            return []

        key = f"xtl:ohlc:snap:{dev}:{sym_u}:H1"

        js = None

        # 1) Try string JSON
        try:
            raw = R.get(key)
            js = _json_load_twice(raw) if raw else None
        except Exception:
            js = None

        # 2) If not JSON, try hash payload (HGETALL)
        if not isinstance(js, dict):
            try:
                h = R.hgetall(key) or {}
                # decode bytes -> str + json
                d = {}
                for k, v in h.items():
                    if isinstance(k, (bytes, bytearray)):
                        k = k.decode("utf-8", "ignore")
                    if isinstance(v, (bytes, bytearray)):
                        v = v.decode("utf-8", "ignore")
                    d[str(k)] = _json_load_twice(v)
                js = d if d else None
            except Exception:
                js = None

        if not isinstance(js, dict):
            return []

        bars = js.get("bars") or js.get("ohlc") or []
        if not isinstance(bars, list):
            return []

        out = []
        for b in bars:
            if not isinstance(b, dict):
                continue

            # keep only CLOSED bars
            if b.get("complete") is False:
                continue

            try:
                o = b.get("o") if b.get("o") is not None else b.get("open")
                h = b.get("h") if b.get("h") is not None else b.get("high")
                l = b.get("l") if b.get("l") is not None else b.get("low")
                c = b.get("c") if b.get("c") is not None else b.get("close")
                if o is None or h is None or l is None or c is None:
                    continue

                t_open = b.get("t_open_ms") or b.get("tOpen") or 0
                t_close = b.get("t_close_ms") or b.get("tClose") or b.get("t") or 0

                # if `t` is seconds, convert to ms
                try:
                    if isinstance(t_close, (int, float)) and 0 < float(t_close) < 10_000_000_000:
                        t_close = int(float(t_close) * 1000)
                except Exception:
                    pass

                out.append(
                    {
                        "t_open_ms": int(t_open) if t_open else 0,
                        "t_close_ms": int(t_close) if t_close else 0,
                        "o": float(o),
                        "h": float(h),
                        "l": float(l),
                        "c": float(c),
                        "complete": True,
                    }
                )
            except Exception:
                continue

        return out
    except Exception:
        return []

def _atr14_from_hlc(bars: list[dict]) -> float | None:
    try:
        if not isinstance(bars, list) or len(bars) < 20:
            return None

        trs = []
        prev_c = None
        for b in bars:
            h = float(b["h"]); l = float(b["l"]); c = float(b["c"])
            if prev_c is None:
                tr = h - l
            else:
                tr = max(h - l, abs(h - prev_c), abs(l - prev_c))
            trs.append(tr)
            prev_c = c

        if len(trs) < 14:
            return None

        return float(sum(trs[-14:]) / 14.0)
    except Exception:
        return None


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
def _bars_to_hlc(bars: list[dict]) -> list[dict]:
    out = []
    if not isinstance(bars, list):
        return out
    for b in bars:
        if not isinstance(b, dict):
            continue
        if not b.get("complete", True):
            continue

        def _pick(*keys):
            for k in keys:
                v = b.get(k)
                if isinstance(v, (int, float)):
                    return float(v)
            return None

        h = _pick("h", "high", "H")
        l = _pick("l", "low", "L")
        c = _pick("c", "close", "C")
        if h is None or l is None or c is None:
            continue
        out.append({"h": h, "l": l, "c": c})
    return out


# ==========================================================
# FINAL STRATEGY CONFIG (easy to tweak)
# ==========================================================
ZONE_ATR_WIDTH = float(os.getenv("XTL_ZONE_ATR_WIDTH", "0.15"))
ZONE_MIN_PIPS  = float(os.getenv("XTL_ZONE_MIN_PIPS", "8")) / 10000
MOVE_AWAY_ATR  = float(os.getenv("XTL_MOVE_AWAY_ATR", "0.25"))

MAX_TAP_BARS    = int(os.getenv("XTL_MAX_TAP_BARS", "20"))
MAX_TAP2_AGE_MS = int(os.getenv("XTL_MAX_TAP2_AGE_MS", str(12 * 60 * 60 * 1000)))  # 

# Maximum number of distinct "fresh" taps we allow on a zone before considering it weakened.
# Allow 1/2/3 taps; >3 taps => block.
MAX_TAPS = int(os.getenv("XTL_MAX_TAPS", "3"))

# ==========================================================
# ZONE AGING (H1 bars)
# ==========================================================
ZONE_MAX_AGE_BARS       = int(os.getenv("XTL_ZONE_MAX_AGE_BARS", "30"))
ZONE_AGE_PENALTY_AFTER = int(os.getenv("XTL_ZONE_AGE_PENALTY_AFTER", "15"))

# ==========================================================
# VOLUME CONFIRMATION
# ==========================================================
VOL_LOOKBACK      = int(os.getenv("XTL_VOL_LOOKBACK", "20"))
VOL_MIN_MULT      = float(os.getenv("XTL_VOL_MIN_MULT", "1.20"))
VOL_BLOCK_IF_FAIL = os.getenv("XTL_VOL_BLOCK_IF_FAIL", "0") == "1"

# ==========================================================
# SESSION WEIGHTING
# ==========================================================
SESSION_BOOST_LONDON = float(os.getenv("XTL_SESSION_BOOST_LONDON", "1.15"))
SESSION_BOOST_NY     = float(os.getenv("XTL_SESSION_BOOST_NY", "1.20"))
SESSION_PENALTY_ASIA = float(os.getenv("XTL_SESSION_PENALTY_ASIA", "0.85"))

def _sweep_break_state(
    *,
    direction: str,            # "BUY" or "SELL"
    bars: list[dict],          # closed bars, newest last
    zone_low: float,
    zone_high: float,
    zone_level: float,
    atr: float,
    soft_wick_atr: float = 0.15,
    hard_close_atr: float = 0.10,
    hard_break_atr: float = 0.35,
    max_soft_bars: int = 3,
    hard_close_bars: int = 2,
) -> dict:
    d = (direction or "").upper()
    if not bars or atr is None or atr <= 0:
        return {"state": "OK", "reclaimed": False, "hard_break": False, "details": {"reason": "no_bars_or_atr"}}

    tol_soft = soft_wick_atr * atr
    tol_close = hard_close_atr * atr
    tol_hard = hard_break_atr * atr

    tail = bars[-max(5, max_soft_bars + 2):]

    def _get(b, k):
        v = b.get(k)
        return float(v) if isinstance(v, (int, float)) else None

    # last close
    last_c = _get(tail[-1], "c")
    if last_c is None:
        return {"state": "OK", "reclaimed": False, "hard_break": False, "details": {"reason": "no_last_close"}}

    # common reclaim predicate (inside band + correct side of level)
    def _reclaim_ok(c: float) -> bool:
        inside = (c >= zone_low) and (c <= zone_high)
        if not inside:
            return False
        if d == "BUY":
            return c >= zone_level
        return c <= zone_level

    # ================= BUY =================
    if d == "BUY":
        # HARD BREAK: deep low below zone_low - tol_hard
        for b in tail[-max_soft_bars:]:
            lo = _get(b, "l")
            if lo is not None and lo < (zone_low - tol_hard):
                return {"state": "HARD_BREAK", "reclaimed": False, "hard_break": True, "details": {"why": "deep_below_zone", "lo": lo}}

        # HARD BREAK: consecutive closes below zone_low - tol_close
        consec = 0
        for b in reversed(tail):
            c = _get(b, "c")
            if c is None:
                continue
            if c < (zone_low - tol_close):
                consec += 1
                if consec >= hard_close_bars:
                    return {"state": "HARD_BREAK", "reclaimed": False, "hard_break": True, "details": {"why": "consec_break_closes", "n": consec}}
            else:
                break

        # detect sweep (wick below zone_low in last max_soft_bars)
        sweep_depth = 0.0
        swept = False
        for b in tail[-max_soft_bars:]:
            lo = _get(b, "l")
            if lo is not None and lo < zone_low:
                swept = True
                sweep_depth = max(sweep_depth, zone_low - lo)

        if not swept:
            return {"state": "OK", "reclaimed": False, "hard_break": False, "details": {"why": "no_sweep"}}

        # if sweep too deep beyond hard tolerance => treat as HARD_BREAK
        if sweep_depth > tol_hard:
            return {"state": "HARD_BREAK", "reclaimed": False, "hard_break": True, "details": {"why": "sweep_beyond_hard", "sweep_depth": sweep_depth, "tol_hard": tol_hard}}

        # reclaimed?
        if _reclaim_ok(last_c):
            return {"state": "OK", "reclaimed": True, "hard_break": False, "details": {"sweep_depth": sweep_depth, "tol_soft": tol_soft, "deep_sweep": bool(sweep_depth > tol_soft)}}

        return {"state": "WAIT_RECLAIM", "reclaimed": False, "hard_break": False, "details": {"sweep_depth": sweep_depth, "tol_soft": tol_soft}}

    # ================= SELL =================
    # HARD BREAK: deep high above zone_high + tol_hard
    for b in tail[-max_soft_bars:]:
        hi = _get(b, "h")
        if hi is not None and hi > (zone_high + tol_hard):
            return {"state": "HARD_BREAK", "reclaimed": False, "hard_break": True, "details": {"why": "deep_above_zone", "hi": hi}}

    # HARD BREAK: consecutive closes above zone_high + tol_close
    consec = 0
    for b in reversed(tail):
        c = _get(b, "c")
        if c is None:
            continue
        if c > (zone_high + tol_close):
            consec += 1
            if consec >= hard_close_bars:
                return {"state": "HARD_BREAK", "reclaimed": False, "hard_break": True, "details": {"why": "consec_break_closes", "n": consec}}
        else:
            break

    # detect sweep (wick above zone_high)
    sweep_depth = 0.0
    swept = False
    for b in tail[-max_soft_bars:]:
        hi = _get(b, "h")
        if hi is not None and hi > zone_high:
            swept = True
            sweep_depth = max(sweep_depth, hi - zone_high)

    if not swept:
        return {"state": "OK", "reclaimed": False, "hard_break": False, "details": {"why": "no_sweep"}}

    if sweep_depth > tol_hard:
        return {"state": "HARD_BREAK", "reclaimed": False, "hard_break": True, "details": {"why": "sweep_beyond_hard", "sweep_depth": sweep_depth, "tol_hard": tol_hard}}

    if _reclaim_ok(last_c):
        return {"state": "OK", "reclaimed": True, "hard_break": False, "details": {"sweep_depth": sweep_depth, "tol_soft": tol_soft, "deep_sweep": bool(sweep_depth > tol_soft)}}

    return {"state": "WAIT_RECLAIM", "reclaimed": False, "hard_break": False, "details": {"sweep_depth": sweep_depth, "tol_soft": tol_soft}}

def _bos_confirmed(
    *,
    direction: str,      # "BUY" or "SELL"
    bars: list[dict],    # closed bars, newest last
    lookback: int = 10,
    require_close: bool = True,
) -> dict:
    """
    Returns: {"ok": bool, "level": float|None, "why": str}
    BUY: close breaks above prior swing high
    SELL: close breaks below prior swing low
    """
    d = (direction or "").upper()
    if not bars or len(bars) < (lookback + 2):
        return {"ok": False, "level": None, "why": "need_more_bars"}

    tail = bars[-(lookback + 2):]
    prev = tail[:-1]
    last = tail[-1]

    def _f(b, k):
        v = b.get(k)
        return float(v) if isinstance(v, (int, float)) else None

    highs = [ _f(b,"h") for b in prev if _f(b,"h") is not None ]
    lows  = [ _f(b,"l") for b in prev if _f(b,"l") is not None ]
    if not highs or not lows:
        return {"ok": False, "level": None, "why": "bad_bar_fields"}

    ref_high = max(highs)
    ref_low  = min(lows)

    c = _f(last, "c")
    h = _f(last, "h")
    l = _f(last, "l")
    if c is None:
        return {"ok": False, "level": None, "why": "no_last_close"}

    if d == "BUY":
        broke = (c > ref_high) if require_close else ((h is not None) and (h > ref_high))
        return {"ok": bool(broke), "level": ref_high, "why": "close_break_high" if require_close else "wick_break_high"}
    else:
        broke = (c < ref_low) if require_close else ((l is not None) and (l < ref_low))
        return {"ok": bool(broke), "level": ref_low, "why": "close_break_low" if require_close else "wick_break_low"}

def _tp_structure_exit(
    *,
    sym_u: str,
    entry_sig: str,       # "BUY" | "SELL"
    bars: list[dict],     # closed bars, newest last
    now_ms: int,
) -> dict:
    """
    Structure TP (both sides), stateful:
      1) Detect BOS in trade direction -> ARM tp_state in Redis
      2) Require >= MIN_BARS_AFTER_BOS closed bars after BOS
      3) Then exit on exhaustion (2-bar reversal)

    Returns: {"ok": bool, "reason": str|None, "meta": dict}
    """
    sig = (entry_sig or "").upper().strip()
    if sig not in ("BUY", "SELL"):
        return {"ok": False, "reason": None, "meta": {}}

    if not bars or len(bars) < 15:
        return {"ok": False, "reason": None, "meta": {"why": "need_more_bars"}}

    # how many closed bars must pass after BOS before we allow exhaustion exit
    try:
        min_after = int(os.getenv("XTL_TP_MIN_BARS_AFTER_BOS", "1"))
    except Exception:
        min_after = 1
    if min_after < 1:
        min_after = 1

    # try to use timestamps if present
    def _tclose(b):
        try:
            v = b.get("t_close_ms") or b.get("tClose") or b.get("t")
            return int(v) if v is not None else 0
        except Exception:
            return 0

    # Load tp state
    st = _load_tp_state(sym_u, sig)
    armed = bool(st.get("armed"))
    bos_tclose = int(st.get("bos_t_close_ms") or 0)
    bos_level = st.get("bos_level")

    # ------------------------------
    # Step-1: BOS arm (only once)
    # ------------------------------
    if not armed:
        bos = _bos_confirmed(direction=sig, bars=bars, lookback=10, require_close=True)
        if not isinstance(bos, dict) or not bos.get("ok"):
            return {"ok": False, "reason": None, "meta": {"why": "waiting_bos", "wait": True, "ui_state": "WAIT", "bos": bos}}

        # freeze BOS at current last closed bar
        t_last = _tclose(bars[-1]) or int(now_ms)
        st2 = {
            "armed": True,
            "bos_level": float(bos.get("level")) if isinstance(bos.get("level"), (int, float)) else None,
            "bos_why": str(bos.get("why") or ""),
            "bos_t_close_ms": int(t_last),
            "armed_ts_ms": int(now_ms),
        }
        _save_tp_state(sym_u, sig, st2)

        # IMPORTANT: do NOT allow exhaustion on same cycle as arm
        return {"ok": False, "reason": None, "meta": {"why": "bos_armed", "wait": True, "ui_state": "WAIT", "bos": bos, "tp_state": st2}}


    # ------------------------------
    # Step-2: require bars after BOS
    # ------------------------------
    bars_after = 0
    if bos_tclose > 0:
        for b in bars:
            if _tclose(b) > bos_tclose:
                bars_after += 1
    else:
        # fallback if no timestamps: count evaluator checks
        try:
            bars_after = int(st.get("checks_after_bos") or 0)
        except Exception:
            bars_after = 0
        bars_after += 1
        st["checks_after_bos"] = bars_after
        _save_tp_state(sym_u, sig, st)

    if bars_after < min_after:
        return {
            "ok": False,
            "reason": None,
            "meta": {
                "why": "waiting_after_bos",
                "bars_after": bars_after,
                "min_after": min_after,
                "bos_level": bos_level,
            },
        }

    # ------------------------------
    # Step-3: exhaustion exit
    # ------------------------------
    try:
        c2 = float(bars[-1].get("c"))
        c1 = float(bars[-2].get("c"))
        c0 = float(bars[-3].get("c"))
    except Exception:
        return {"ok": False, "reason": None, "meta": {"why": "bad_close_fields"}}

    if sig == "BUY":
        exhausted = (c2 < c1) and (c1 < c0)
    else:
        exhausted = (c2 > c1) and (c1 > c0)

    if exhausted:
        # Clear tp state once we decide to exit
        _clear_tp_state(sym_u, sig)
        return {
            "ok": True,
            "reason": "tp_structure_exhaust",
            "meta": {
                "bos_level": bos_level,
                "bos_t_close_ms": bos_tclose,
                "bars_after_bos": bars_after,
                "c2": c2, "c1": c1, "c0": c0,
                "server_now_ms": int(now_ms),
            },
        }

    return {
        "ok": False,
        "reason": None,
        "meta": {
            "why": "no_exhaust",
            "bos_level": bos_level,
            "bars_after_bos": bars_after,
            "min_after": min_after,
        },
    }

def _zone_reversal_gate(
    *,
    sym: str,
    direction: str,   
    row_h1: dict,
    sr: dict | None,
    now_ms: int,
    tf_tag: str = "H1",
    pinned_device: str | None = None,
    debug_gate: bool = False,
    x_device_id: str | None = None,
) -> tuple[bool, dict]:
    """
    Final opportunity gate:
    - Resolve zone (SR -> fallback -> provisional)
    - Require SECOND TAP + reversal candle
    - Maintain tap state in Redis

    Added:
      5A) Soft sweep vs hard break (do NOT kill on sweep; kill on hard break)
      5B) BOS double-check (H1 + optional M15 if provided)
      6)  Volume confirmation
      7)  Session weighting
      8)  Zone aging
      9)  Final confidence
    """
    

    cl = None
    opn = None
    hi = None
    lo = None

    direction = str(direction or "").upper().strip()
    if direction not in ("BUY", "SELL"):
        return False, {"reason": "bad_direction"}

    sym_u = (sym or "").upper().strip()
    zone_tf = str(tf_tag or "H1").upper()


    
    # -------------------------------
    # 0) Load H1 bars (prefer attached; else backfill from Redis snap)
    # -------------------------------
    bars = row_h1.get("bars") or row_h1.get("ohlc") or []
    if not isinstance(bars, list):
        bars = []

    x_device_id_hdr = (x_device_id or "").strip()
    if (not x_device_id_hdr) and pinned_device:
        x_device_id_hdr = str(pinned_device).strip()
    dev = str((x_device_id_hdr or pinned_device or x_device_id or "")).strip()

    dbg_src = None
    dbg_err = None

    if len(bars) < 2 and sym_u:
        try:
            js = None
            bars_any = None

            # 1) try deterministic device (header first)
            if dev:
                js, _ = _read_snap_for_device(dev, sym_u, "H1", header_device=x_device_id_hdr)
                dbg_src = f"read_snap_for_device:{dev}"

                # --- FIX: if helper returned empty/invalid, do direct GET of STRING snap ---
                try:
                    if isinstance(js, dict):
                        bars_any = js.get("bars")
                        if not isinstance(bars_any, list):
                            bars_any = js.get("ohlc")

                    if not (isinstance(bars_any, list) and len(bars_any) >= 2):
                        R0 = _r()
                        k0_dev = x_device_id_hdr or dev
                        k0 = f"xtl:ohlc:snap:{k0_dev}:{sym_u}:H1"
                        raw0 = R0.get(k0)  # STRING JSON
                        js0 = _json_load_twice(raw0) if raw0 else None
                        if isinstance(js0, dict):
                            js = js0
                            dbg_src = (dbg_src or "") + f" | direct_get_string_snap:{k0_dev}"

                    # recompute bars_any after js replacement
                    if isinstance(js, dict):
                        bars_any = js.get("bars")
                        if not isinstance(bars_any, list):
                            bars_any = js.get("ohlc")

                except Exception as e:
                    dbg_err = (dbg_err or "") + f" | direct_get:{type(e).__name__}:{e}"

                if debug_gate:
                    try:
                        R0 = _r()
                        ck = getattr(getattr(R0, "connection_pool", None), "connection_kwargs", {}) or {}
                        k_dbg_dev = x_device_id_hdr or dev
                        k_dbg = f"xtl:ohlc:snap:{k_dbg_dev}:{sym_u}:H1"

                        t_dbg = R0.type(k_dbg)
                        if isinstance(t_dbg, (bytes, bytearray)):
                            t_dbg = t_dbg.decode("utf-8", "ignore")
                        t_dbg = str(t_dbg or "").lower()

                        dbg_src = (
                            (dbg_src or "")
                            + f" | redis={ck.get('host')}:{ck.get('port')}/db{ck.get('db')}"
                            + f" | key={k_dbg} | type={t_dbg}"
                        )

                        if isinstance(js, dict):
                            ba = js.get("bars")
                            if not isinstance(ba, list):
                                ba = js.get("ohlc")

                            bn = len(ba) if isinstance(ba, list) else -1
                            k0 = (
                                list(ba[0].keys())
                                if isinstance(ba, list) and ba and isinstance(ba[0], dict)
                                else None
                            )
                            dbg_src = (dbg_src or "") + f" | bars_any_n={bn} | bar0_keys={k0}"
                        else:
                            dbg_src = (dbg_src or "") + " | snap_decode=None"

                    except Exception as e:
                        dbg_err = (dbg_err or "") + f" | redis_dbg:{type(e).__name__}:{e}"

            # 3) Normalize + attach to row_h1
            if isinstance(bars_any, list) and len(bars_any) >= 2:
                norm = []
                tf_ms = int(TF_MS.get(tf_tag.upper(), 60 * 60 * 1000))

                def _get(b, *keys):
                    for k in keys:
                        v = b.get(k) if isinstance(b, dict) else None
                        if v is not None:
                            return v
                    return None

                for b in bars_any:
                    if not isinstance(b, dict):
                        continue
                    try:
                        t = _get(b, "t", "ts", "time")
                        if t is None:
                            t_open_ms = _get(b, "t_open_ms")
                            if not isinstance(t_open_ms, (int, float)) or t_open_ms <= 0:
                                continue
                            t_open_ms = int(t_open_ms)
                            t_close_ms = _get(b, "t_close_ms")
                            if not isinstance(t_close_ms, (int, float)) or t_close_ms <= 0:
                                t_close_ms = t_open_ms + tf_ms
                            else:
                                t_close_ms = int(t_close_ms)
                        else:
                            t_open_ms = _to_ms_any(int(t))
                            if t_open_ms <= 0:
                                continue
                            t_close_ms = t_open_ms + tf_ms

                        o_ = _get(b, "o", "open")
                        h_ = _get(b, "h", "high")
                        l_ = _get(b, "l", "low")
                        c_ = _get(b, "c", "close")
                        if o_ is None or h_ is None or l_ is None or c_ is None:
                            continue

                        norm.append({
                            "t_open_ms": int(t_open_ms),
                            "t_close_ms": int(t_close_ms),
                            "o": float(o_), "h": float(h_), "l": float(l_), "c": float(c_),
                            "v": _get(b, "v", "volume"),
                            "complete": bool(b.get("complete", True)),
                        })
                    except Exception:
                        continue

                norm.sort(key=lambda x: int(x.get("t_close_ms") or 0))
                if len(norm) >= 2:
                    bars = norm
                    row_h1["bars"] = bars

        except Exception as e:
            dbg_err = (dbg_err or "") + f" | snap_read:{type(e).__name__}:{e}"

    

    # HARD FAIL EARLY if still no bars
    if len(bars) < 2:
        meta = {
            "reason": "no_h1_bars",
            "bars_n": len(bars),
            "dev": dev,
            "sym": sym_u,
        }
        if debug_gate:
            meta["dbg_h1_src"] = dbg_src
            meta["dbg_h1_err"] = dbg_err
        return False, meta


    def _pick_side_level(sr: dict, px: float, direction: str):
        def _levels(tf, key):
            return [
                float(x["level"]) for x in (tf.get(key) or [])
                if isinstance(x, dict) and "level" in x
            ]

        order = ["h1", "h4"]
        kinds = ["supports_near", "supports_major"] if direction == "BUY" \
            else ["resistances_near", "resistances_major"]

        for tfk in order:
            tf = sr.get(tfk) or {}
            for k in kinds:
                lvls = _levels(tf, k)
                if direction == "BUY":
                    below = [v for v in lvls if v < px]
                    if below:
                        return max(below), tfk.upper()
                else:
                    above = [v for v in lvls if v > px]
                    if above:
                        return min(above), tfk.upper()

        return None, None

    def _bar_f(b: dict, *keys, default=None):
        for k in keys:
            if isinstance(b, dict) and k in b and b.get(k) is not None:
                return b.get(k)
        return default
    # -------------------------------
    # 0B) Pick last safely CLOSED bar + define OHLC (cl/o/h/l) ONCE
    # -------------------------------
    try:
        tf_ms = int(TF_MS.get(str(tf_tag or "H1").upper(), 60 * 60 * 1000))
    except Exception:
        tf_ms = 60 * 60 * 1000

    try:
        c, p = _pick_last_closed_bar_from_bars(bars, int(now_ms), int(tf_ms))
    except Exception:
        c, p = None, None

    if not isinstance(c, dict) or not isinstance(p, dict):
        return False, {"reason": "no_h1_closed_bar", "bars_n": int(len(bars)), "stage": "H1_PICK"}

    try:
        o = float(_bar_f(c, "o", "open"))
        h = float(_bar_f(c, "h", "high"))
        l = float(_bar_f(c, "l", "low"))
        cl = float(_bar_f(c, "c", "close"))
    except Exception:
        return False, {
            "reason": "bad_h1_ohlc",
            "stage": "H1_PICK",
            "keys": list(c.keys()) if isinstance(c, dict) else None,
        }
    
    # -------------------------------
    # 0C) Reference price for SR/zone selection
    # -------------------------------
    # IMPORTANT: Zone selection must use the freshest price available, otherwise
    # we can pick a wrong-side / outdated level (especially on fast moves).

    px = None
    px_src = None
    px_ts_ms = None

    # 0C-1) Try to read a live price from Redis (device-scoped)
    try:
        if pinned_device:
            pkey = f"xtl:price:{pinned_device}:{sym_u}"
            rawp = R.get(pkey)
            if rawp:
                try:
                    j = _json_load_twice(rawp)
                except Exception:
                    j = None

                if isinstance(j, dict):
                    # accept common field names
                    for k in ("price", "p", "last", "mid", "bid", "ask"):
                        if j.get(k) is not None:
                            try:
                                px = float(j.get(k))
                                px_src = f"redis:{pkey}:{k}"
                                break
                            except Exception:
                                pass
                    # timestamp (optional)
                    for tk in ("ts_ms", "t_ms", "ts", "t"):
                        if j.get(tk) is not None:
                            try:
                                v = int(j.get(tk))
                                # if seconds -> convert
                                px_ts_ms = v * 1000 if v < 10_000_000_000 else v
                                break
                            except Exception:
                                pass
                else:
                    # raw numeric string
                    try:
                        px = float(rawp)
                        px_src = f"redis:{pkey}:raw"
                    except Exception:
                        pass
    except Exception:
        pass

    # 0C-2) Fallback to last CLOSED close
    if px is None:
        try:
            px = float(cl)
            px_src = "h1_close"
        except Exception:
            px = None
            px_src = None

    # Always surface what we used in debug output
    #try:
    #    debug_out["price"] = float(px) if px is not None else None
    #    debug_out["price_src"] = px_src
    #    debug_out["price_ts_ms"] = int(px_ts_ms) if px_ts_ms is not None else None
    #except Exception:
    #    pass
    # NOTE: debug_out is created later (after tap persistence).
    # We'll attach price fields right after debug_out is built.




    # --- Freshness guard (block only if stale, not TTL) ---
    # --- Freshness guard (block only if stale, not TTL) ---
    if isinstance(bars, list) and len(bars) >= 2:
        try:
            tf_ms0 = int(TF_MS.get(tf_tag.upper(), 60 * 60 * 1000))

            c = bars[-1]
            if isinstance(c, dict) and (c.get("complete") is False):
                c = bars[-2]

            # 1) Prefer snap-level lastClosedTs (ms)
            # 1) DO NOT trust snap-level lastClosedTs unless it is sane
            last_close = 0
            try:
                lc0 = int((row_h1 or {}).get("lastClosedTs") or 0)
                # accept only if it is not in the future and not absurdly old
                if 0 < lc0 <= int(now_ms):
                    last_close = lc0
            except Exception:
                last_close = 0


            # 2) Else prefer normalized bar close
            if last_close <= 0 and isinstance(c, dict):
                last_close = int(c.get("t_close_ms") or 0)

            # 3) Else infer from normalized open
            if last_close <= 0 and isinstance(c, dict):
                t_open_ms = int(c.get("t_open_ms") or 0)
                if t_open_ms > 0:
                    last_close = t_open_ms + tf_ms0

            # 4) Else infer from raw 't' seconds/ms
            if last_close <= 0 and isinstance(c, dict):
                t_raw = int(c.get("t") or 0)
                if t_raw > 0:
                    t_open_ms = _to_ms_any(t_raw)
                    if t_open_ms > 0:
                        last_close = t_open_ms + tf_ms0

            age_ms = int(now_ms - last_close) if last_close > 0 else 0

            max_age_ms = 3 * tf_ms0
            utc_weekday = datetime.now(timezone.utc).weekday()
            is_weekend = utc_weekday >= 5
            if is_weekend:
                 max_age_ms = 72 * 60 * 60 * 1000

            if last_close > 0 and age_ms > max_age_ms:
                meta = {
                    "reason": "stale_h1",
                    "blocked": True,
                    "dev": dev,
                    "sym": sym_u,
                    "bars_n": int(len(bars)),
                    "age_ms": int(age_ms),
                    "tf_ms": int(tf_ms0),
                    "max_age_ms": int(max_age_ms),
                    "last_close_ms": int(last_close),
                    "is_weekend": bool(is_weekend),
                }
                if debug_gate:
                    meta["dbg_h1_src"] = dbg_src
                    meta["dbg_h1_err"] = dbg_err
                return False, meta

        except Exception as e:
            dbg_err = (dbg_err or "") + f" | stale_chk:{type(e).__name__}:{e}"




    # -------------------------------
    # 1) Resolve ATR (feature -> fallback compute)
    # -------------------------------
    atr = None
    extra_h1 = row_h1.get("extra_h1") or {}
    feats = extra_h1.get("features") if isinstance(extra_h1.get("features"), dict) else extra_h1
    try:
        atr = feats.get("feat_atr")
    except Exception:
        atr = None

    if not isinstance(atr, (int, float)) or atr <= 0:
        try:
            ATR_LEN = int(os.getenv("XTL_ATR_LEN", "14"))
        except Exception:
            ATR_LEN = 14

        try:
            bars_for_atr = []
            for b in (bars or []):
                if not isinstance(b, dict):
                    continue
                if b.get("complete", True) is False:
                    continue
                try:
                    h0 = float(_bar_f(b, "h", "high"))
                    l0 = float(_bar_f(b, "l", "low"))
                    c0 = float(_bar_f(b, "c", "close"))
                    bars_for_atr.append({"h": h0, "l": l0, "c": c0})
                except Exception:
                    continue

            if len(bars_for_atr) >= (ATR_LEN + 1):
                trs = []
                start = len(bars_for_atr) - ATR_LEN
                for i in range(start, len(bars_for_atr)):
                    cur = bars_for_atr[i]
                    prev_c = bars_for_atr[i - 1]["c"]
                    tr = max(
                        cur["h"] - cur["l"],
                        abs(cur["h"] - prev_c),
                        abs(cur["l"] - prev_c),
                    )
                    trs.append(float(tr))
                atr = sum(trs) / max(len(trs), 1)
        except Exception:
            atr = None

    if not isinstance(atr, (int, float)) or atr <= 0:
        return False, {"reason": "no_atr", "bars_n": len(bars)}

    atr = float(atr)

    # zone half-width
    zone_half = max(float(ZONE_ATR_WIDTH) * atr, float(ZONE_MIN_PIPS))

    # -------------------------------
    # 2) Resolve zone (SR -> fallback -> provisional)
    # -------------------------------
    zone = None
    zone_type = None
    zone_tf = None

    try:
        if isinstance(sr, dict):
            lvl, zone_tf = _pick_side_level(sr, px, direction)

            # If SR missing, DO NOT hard-fail; allow fallback provisional zone.
            if isinstance(lvl, (int, float)):
                lvl = float(lvl)

                if direction == "BUY":
                    # support must be <= reference price
                    if px is not None and lvl <= float(px):
                        zone = {"level": lvl, "low": lvl - zone_half, "high": lvl + zone_half, "tf": zone_tf}
                        zone_type = "SR_CONFIRMED"
                else:
                    # resistance must be >= reference price
                    if px is not None and lvl >= float(px):
                        zone = {"level": lvl, "low": lvl - zone_half, "high": lvl + zone_half, "tf": zone_tf}
                        zone_type = "SR_CONFIRMED"

            # If SR picker chose a zone but did not provide TF, infer from bundle keys
            if zone and not zone_tf:
                try:
                    # sr keys are expected to be {"h1": {...}, "h4": {...}}
                    if isinstance(sr.get("h4"), dict):
                        zone_tf = "H4" if (sr.get("h4") or {}) else None
                    if not zone_tf and isinstance(sr.get("h1"), dict):
                        zone_tf = "H1" if (sr.get("h1") or {}) else None
                except Exception:
                    pass

            if zone_type == "SR_CONFIRMED" and not zone_tf:
                zone_tf = str(tf_tag or "H1").upper()

    except Exception:
        pass


    if not zone:
        try:
            tail = [b for b in bars[-30:] if isinstance(b, dict)]
            if len(tail) >= 10:
                if direction == "BUY":
                    lvl = min(float(_bar_f(b, "l", "low")) for b in tail if _bar_f(b, "l", "low") is not None)
                else:
                    lvl = max(float(_bar_f(b, "h", "high")) for b in tail if _bar_f(b, "h", "high") is not None)
                zone = {"level": float(lvl), "low": float(lvl) - zone_half, "high": float(lvl) + zone_half, "tf": str(tf_tag or "H1").upper()}
                zone_type = "ZONE_PROVISIONAL"
        except Exception:
            pass

    if not zone:
        return False, {"reason": "no_zone"}
    # --- ensure zone_tf always exists (single source of truth for “what TF created this zone”) ---
    try:
        # preserve zone_tf if already set (e.g., from _pick_side_level)
        if not zone_tf and isinstance(zone, dict):
            zone_tf = zone.get("tf") or zone.get("zone_tf") or zone.get("tf_tag")

        if not zone_tf:
            zone_tf = str(tf_tag or "H1").upper()

        if isinstance(zone, dict):
            zone["tf"] = str(zone_tf).upper()

    except Exception:
        pass


    
    
   	
    # ==========================================================
    # 5A) SOFT SWEEP vs HARD BREAK
    # ==========================================================
    try:
        SWEEP_ATR = float(os.getenv("XTL_SWEEP_ATR", "0.25"))
    except Exception:
        SWEEP_ATR = 0.25

    try:
        HARD_BREAK_ATR = float(os.getenv("XTL_HARD_BREAK_ATR", "0.60"))
    except Exception:
        HARD_BREAK_ATR = 0.60

    try:
        RECLAIM_MAX_BARS = int(os.getenv("XTL_RECLAIM_MAX_BARS", "3"))
    except Exception:
        RECLAIM_MAX_BARS = 3

    try:
        RECLAIM_TIGHTEN_ATR = float(os.getenv("XTL_RECLAIM_TIGHTEN_ATR", "0.35"))
    except Exception:
        RECLAIM_TIGHTEN_ATR = 0.35
    

    def _break_state_close_only(cl_val: float) -> tuple[str, dict]:
        zl = float(zone["low"])
        zh = float(zone["high"])
        zlv = float(zone["level"])
        buf_sweep = float(SWEEP_ATR) * atr
        buf_hard = float(HARD_BREAK_ATR) * atr

        if direction == "BUY":
            if cl_val < zl:
                if cl_val >= (zl - buf_sweep):
                    return "SWEEP", {"side": "DOWN", "cl": cl_val, "edge": zl, "buf": buf_sweep}
            if cl_val < (zl - buf_hard):
                return "HARD_BREAK", {"side": "DOWN", "cl": cl_val, "edge": zl, "buf": buf_hard}
        else:
            if cl_val > zh:
                if cl_val <= (zh + buf_sweep):
                    return "SWEEP", {"side": "UP", "cl": cl_val, "edge": zh, "buf": buf_sweep}
                if cl_val > (zh + buf_hard):
                    return "HARD_BREAK", {"side": "UP", "cl": cl_val, "edge": zh, "buf": buf_hard}

        return "OK", {"cl": cl_val, "zone_low": zl, "zone_high": zh, "zone_level": zlv}
    
    def _pick_zone_from_sr(sr_all: dict, direction: str, cl: float, atr: float) -> dict | None:
        if not isinstance(sr_all, dict):
            return None

        # Accept BOTH shapes:
        # A) full SR payload: {"h1": {...}, "h4": {...}, ...}
        # B) TF-sliced payload: {"supports":[...], "resistances":[...], ...}
        sr_tf = None
        picked_tf = None

        # TF-sliced shape
        if isinstance(sr_all, dict) and (
            ("supports" in sr_all) or ("resistances" in sr_all) or
            ("supports_major" in sr_all) or ("resistances_major" in sr_all) or
            ("supports_near" in sr_all) or ("resistances_near" in sr_all)
        ):
            sr_tf = sr_all
            picked_tf = str(tf_tag or "H1").upper()
        else:
            # Full payload -> prefer H1, fallback H4
            if isinstance(sr_all.get("h1"), dict):
                sr_tf = sr_all["h1"]
                picked_tf = "H1"
            elif isinstance(sr_all.get("h4"), dict):
                sr_tf = sr_all["h4"]
                picked_tf = "H4"
            else:
                return None

        if not isinstance(sr_tf, dict):
            return None

        supp  = sr_tf.get("supports_near") or sr_tf.get("support_near") or []
        supp2 = sr_tf.get("supports_major") or sr_tf.get("supports") or []
        res   = sr_tf.get("resistances_near") or sr_tf.get("resistance_near") or []
        res2  = sr_tf.get("resistances_major") or sr_tf.get("resistances") or []

        def _as_items(a):
            out = []
            for x in (a or []):
                try:
                    if isinstance(x, dict):
                        lv = float(x.get("level"))
                        out.append({
                            "level": lv,
                            "touches": int(x.get("touches") or 0),
                            "strength": float(x.get("strength") or 0),
                            "sr_score": float(x.get("sr_score") or 0),
                        })
                    else:
                        lv = float(x)
                        out.append({
                            "level": lv,
                            "touches": 0,
                            "strength": 0.0,
                            "sr_score": 0.0,
                        })
                except Exception:
                    pass
            return out

        def _is_strong(it: dict) -> bool:
            try:
                thr_strength = int(os.getenv("XTL_STRONG_ZONE_STRENGTH", "8"))
                thr_touches  = int(os.getenv("XTL_STRONG_ZONE_TOUCHES", "4"))
                thr_score    = float(os.getenv("XTL_STRONG_ZONE_SR_SCORE", "9"))
                return (
                    float(it.get("strength") or 0) >= thr_strength
                    or int(it.get("touches") or 0) >= thr_touches
                    or float(it.get("sr_score") or 0) >= thr_score
                )
            except Exception:
                return False


        if direction == "SELL":
            cands = _as_items(res) + _as_items(res2)
            cands = [it for it in cands if it["level"] > cl]
            if not cands:
                return None

            # nearest above price, then stronger levels
            cands.sort(key=lambda it: (
                it["level"],
                -float(it.get("strength", 0)),
                -float(it.get("sr_score", 0)),
                -int(it.get("touches", 0))
            ))
            pick = cands[0]

        else:  # BUY
            cands = _as_items(supp) + _as_items(supp2)
            cands = [it for it in cands if it["level"] < cl]
            if not cands:
                return None

            # nearest below price, then stronger levels
            cands.sort(key=lambda it: (
                -it["level"],
                -float(it.get("strength", 0)),
                -float(it.get("sr_score", 0)),
                -int(it.get("touches", 0))
            ))
            pick = cands[0]

        half = max(atr * ZONE_ATR_WIDTH, ZONE_MIN_PIPS)

        return {
            "level": float(pick["level"]),
            "low": float(pick["level"] - half),
            "high": float(pick["level"] + half),
            "type": "SR_CONFIRMED",
            "tf": picked_tf,

            # --- NEW META (for strong old zone logic) ---
            "touches": int(pick.get("touches") or 0),
            "strength": float(pick.get("strength") or 0.0),
            "sr_score": float(pick.get("sr_score") or 0.0),
            "is_strong": bool(_is_strong(pick)),
        }

    def _attach_zone_strength_meta(zone: dict, sr_all: dict) -> None:
        """
        For ZONE_PROVISIONAL / flipped zones: find nearest SR dict item and attach
        touches/strength/sr_score/is_strong so bootstrap-days can expand.
        """
        if not isinstance(zone, dict) or not isinstance(sr_all, dict):
            return
        try:
            zlvl = float(zone.get("level") or 0.0)
        except Exception:
            return

        # pick TF slice (prefer H1 then H4)
        tf_obj = None
        if isinstance(sr_all.get("h1"), dict):
            tf_obj = sr_all["h1"]
        elif isinstance(sr_all.get("h4"), dict):
            tf_obj = sr_all["h4"]
        elif any(k in sr_all for k in ("supports", "resistances", "supports_near", "resistances_near", "supports_major", "resistances_major")):
            tf_obj = sr_all

        if not isinstance(tf_obj, dict):
            return

        buckets = []
        for k in ("supports_near","supports_major","supports","resistances_near","resistances_major","resistances"):
            xs = tf_obj.get(k) or []
            if isinstance(xs, list) and xs:
                buckets.append(xs)

        best = None
        best_d = None

        for x in xs:
            # allow dict items OR raw float levels
            if isinstance(x, dict):
                lv0 = x.get("level")
                if lv0 is None:
                    continue
                try:
                    lv = float(lv0)
                except Exception:
                    continue
                meta_src = x  # may contain touches/strength/sr_score
            else:
                # raw float level list
                try:
                    lv = float(x)
                except Exception:
                    continue
                meta_src = None  # no meta available


                d = abs(lv - zlvl)
                if best_d is None or d < best_d:
                    best_d = d
                    best = {"level": lv} if meta_src is None else meta_src

        if not isinstance(best, dict) or best_d is None:
            return

        # only accept if "close enough" to represent the same SR level
        tol = None
        try:
            tol = abs(float(zone.get("high")) - float(zone.get("level")))
            if not tol or tol <= 0:
                tol = None
        except Exception:
            tol = None

        if tol is None:
            try:
                tol = float(os.getenv("XTL_ZONE_META_MATCH_TOL", "2.0"))
            except Exception:
                tol = 2.0

        if best_d <= tol:
            zone["touches"] = int(best.get("touches") or 0)
            zone["strength"] = float(best.get("strength") or 0.0)
            zone["sr_score"] = float(best.get("sr_score") or 0.0)

            try:
                thr_strength = int(os.getenv("XTL_STRONG_ZONE_STRENGTH", "8"))
            except Exception:
                thr_strength = 8
            try:
                thr_touches = int(os.getenv("XTL_STRONG_ZONE_TOUCHES", "4"))
            except Exception:
                thr_touches = 4
            try:
                thr_score = float(os.getenv("XTL_STRONG_ZONE_SR_SCORE", "9"))
            except Exception:
                thr_score = 9.0

            zone["is_strong"] = (
                float(zone.get("strength") or 0) >= thr_strength
                or int(zone.get("touches") or 0) >= thr_touches
                or float(zone.get("sr_score") or 0) >= thr_score
            )

    
    # ---- If zone is wrong side / too far, replace it ----
    try:
        cl_val = float(cl)
    except Exception:
        cl_val = None

    # Prefer live/last price for zone side validation; fall back to last closed close
    try:
        ref_px = float(px)
    except Exception:
        ref_px = cl_val
    # --- FORCE pick zone from SR based on live price (prevents stale SR_CONFIRMED zone) ---
    if ref_px is not None and isinstance(sr, dict):
        z_force = _pick_zone_from_sr(sr, direction, ref_px, float(atr or 0.0))
        if z_force:
            zone = z_force

    if ref_px is not None and isinstance(sr, dict) and isinstance(zone, dict):
        far = float(MOVE_AWAY_ATR) * float(atr or 0.0)

        try:
            zl = float(zone.get("low"))
            zh = float(zone.get("high"))
        except Exception:
            zl = None
            zh = None

        if zl is not None and zh is not None:

            # --- HARD GUARD: do not proceed with wrong-side zone ---
            # --- HARD GUARD: truly wrong-side zone (allow when price is INSIDE the zone) ---
            # BUY support is wrong-side only if the whole zone is ABOVE price
            if direction == "BUY" and zl > ref_px:
                return False, {
                    "reason": "zone_wrong_side_buy",
                    "stage": "ZONE_VALIDATE",
                    "blocked": False,
                    "price": float(ref_px),
                    "zone": zone,
                }

            # SELL resistance is wrong-side only if the whole zone is BELOW price
            if direction == "SELL" and zh < ref_px:
                return False, {
                    "reason": "zone_wrong_side_sell",
                    "stage": "ZONE_VALIDATE",
                    "blocked": False,
                    "price": float(ref_px),
                    "zone": zone,
                }


            if direction == "SELL":
                # Resistance must be ABOVE current price
                wrong_side = (zh < ref_px)
                too_far    = (zl > ref_px + far) if far > 0 else False

                
                if wrong_side or too_far:
                    # Try to re-pick using SR at current live price
                    z2 = _pick_zone_from_sr(sr, "SELL", ref_px, atr)
                    if z2:
                        zone = z2
                        try:
                            zone_tf = str(z2.get("tf") or zone_tf or tf_tag or "H1").upper()
                        except Exception:
                            pass
                    else:
                        # IMPORTANT: do NOT fail/return just because zone is far.
                        # If SR has a nearest_resistance above price, keep current zone and wait for tap+reversal.
                        nr = None
                        try:
                            nr = float(sr.get("nearest_resistance")) if isinstance(sr, dict) and sr.get("nearest_resistance") is not None else None
                        except Exception:
                            nr = None

                        if nr is not None and nr > float(ref_px):
                            # Keep existing zone (may be far). Continue to tap logic; it will only arm on touch.
                            # Attach a soft reason for debug visibility.
                            gate_meta = {
                                "reason": "sell_zone_far_wait_tap",
                                "stage": "ZONE_PICK",
                                "blocked": False,
                                "price": float(ref_px),
                                "note": "Resistance exists but is far; waiting for tap+reversal.",
                                "zone":zone,
                            }
                        else:
                            # Truly no resistance above price in SR
                            return False, {
                                "reason": "no_sell_resistance_above_price",
                                "stage": "ZONE_PICK",
                                "blocked": False,
                                "price": float(ref_px),
                                "note": "No resistance above price in SR; cannot run SELL tap gate now.",
                            }


            else:  # BUY
                # Support must be BELOW current price
                wrong_side = (zl > ref_px)
                too_far    = (zh < ref_px - far) if far > 0 else False

                if wrong_side or too_far:
                    z2 = _pick_zone_from_sr(sr, "BUY", ref_px, atr)
                    if z2:
                        zone = z2
                        try:
                            zone_tf = str(z2.get("tf") or zone_tf or tf_tag or "H1").upper()
                        except Exception:
                            pass
                    else:
                        ns = None
                        try:
                            ns = float(sr.get("nearest_support")) if isinstance(sr, dict) and sr.get("nearest_support") is not None else None
                        except Exception:
                            ns = None

                        if ns is not None and ns < float(ref_px):
                            gate_meta = {
                                "reason": "buy_zone_far_wait_tap",
                                "stage": "ZONE_PICK",
                                "blocked": False,
                                "price": float(ref_px),
                                "note": "Support exists but is far; waiting for tap+reversal.",
                                "zone":zone,
                            }
                        else:
                            return False, {
                                "reason": "no_buy_support_below_price",
                                "stage": "ZONE_PICK",
                                "blocked": False,
                                "price": float(ref_px),
                                "note": "No support below price in SR; cannot run BUY tap gate now.",
                            }


    # keep close-only break state based on close (cl)
    break_state, break_meta = _break_state_close_only(float(cl))
    # --- attach strength meta for provisional/flipped zones (so bootstrap can extend) ---
    try:
        _attach_zone_strength_meta(zone, sr)
    except Exception:
        pass
    # after _attach_zone_strength_meta(zone, sr)
    try:
        half_local = None
        try:
            zl = float(zone.get("low"))
            zh = float(zone.get("high"))
            half_local = abs(zh - zl) / 2.0
        except Exception:
            half_local = float(zone.get("half") or 0.0)
        if isinstance(gate_meta, dict) and isinstance(zone, dict):
            gate_meta["zone_used"] = {
                "level": float(zone.get("level") or 0.0),
                "low": float(zone.get("low") or 0.0),
                "high": float(zone.get("high") or 0.0),
                "touches": int(zone.get("touches") or 0),
                "strength": float(zone.get("strength") or 0.0),
                "sr_score": float(zone.get("sr_score") or 0.0),
                "is_strong": bool(zone.get("is_strong") or False),
                "tf": str(zone.get("tf") or zone_tf or tf_tag or ""),
                "type": str(zone.get("type") or zone_type or ""),
                "half": float(half_local or 0.0),
                "atr": float(atr or 0.0),
            }
    except Exception:
        pass




    
    # ==========================================================
    # 5B) BOS double-check
    # ==========================================================
    BOS_BLOCK = os.getenv("XTL_BOS_BLOCK", "1") == "1"
    try:
        BOS_MAX_AGE_BARS = int(os.getenv("XTL_BOS_MAX_AGE_BARS", "48"))
    except Exception:
        BOS_MAX_AGE_BARS = 48
    try:
        BOS_PIVOT_LR = int(os.getenv("XTL_BOS_PIVOT_LR", "2"))
    except Exception:
        BOS_PIVOT_LR = 2
    try:
        BOS_MIN_ATR = float(os.getenv("XTL_BOS_MIN_ATR", "0.15"))
    except Exception:
        BOS_MIN_ATR = 0.15

    def _detect_last_bos(bars_in: list[dict], *, atr_val: float | None, max_age_bars: int) -> dict | None:
        try:
            lr = max(1, int(BOS_PIVOT_LR))
            n0 = len(bars_in)
            if n0 < (2 * lr + 6):
                return None

            def _hlc(i: int):
                b = bars_in[i]
                h_ = float(b.get("h") or b.get("high"))
                l_ = float(b.get("l") or b.get("low"))
                c_ = float(b.get("c") or b.get("close"))
                return h_, l_, c_

            piv_hi: list[tuple[int, float]] = []
            piv_lo: list[tuple[int, float]] = []

            for i in range(lr, n0 - lr):
                h_i, l_i, _ = _hlc(i)
                ok_hi = True
                ok_lo = True
                for k in range(1, lr + 1):
                    h_l, l_l, _ = _hlc(i - k)
                    h_r, l_r, _ = _hlc(i + k)
                    if h_i <= h_l or h_i <= h_r:
                        ok_hi = False
                    if l_i >= l_l or l_i >= l_r:
                        ok_lo = False
                    if not ok_hi and not ok_lo:
                        break
                if ok_hi:
                    piv_hi.append((i, h_i))
                if ok_lo:
                    piv_lo.append((i, l_i))

            if not piv_hi and not piv_lo:
                return None

            last_i = n0 - 1
            _, _, last_close = _hlc(last_i)

            bos = None
            if piv_lo:
                pi, lvl = piv_lo[-1]
                if last_close < lvl:
                    bos = {"ok": True, "dir": "DOWN", "level": float(lvl), "pivot_i": int(pi)}

            if piv_hi:
                pi, lvl = piv_hi[-1]
                if last_close > lvl:
                    up = {"ok": True, "dir": "UP", "level": float(lvl), "pivot_i": int(pi)}
                    if (bos is None) or (up["pivot_i"] > bos["pivot_i"]):
                        bos = up

            if not bos:
                return None

            age_bars = int(last_i - int(bos["pivot_i"]))
            bos["break_i"] = int(last_i)
            bos["age_bars"] = age_bars
            bos["blocked"] = False

            if age_bars > int(max_age_bars):
                return bos

            if isinstance(atr_val, (int, float)) and float(atr_val) > 0:
                dist = abs(float(last_close) - float(bos["level"]))
                if dist < float(BOS_MIN_ATR) * float(atr_val):
                    return bos

            want = "UP" if direction == "BUY" else "DOWN"
            if BOS_BLOCK and bos["dir"] != want:
                # Opposite BOS blocks ONLY if price is still holding beyond the BOS level.
                try:
                    lvl = float(bos["level"])
                    lc = float(last_close)
                    buf = 0.0
                    if isinstance(atr_val, (int, float)) and float(atr_val) > 0:
                        buf = float(BOS_MIN_ATR) * float(atr_val)
                except Exception:
                    lvl, lc, buf = 0.0, 0.0, 0.0

                if bos["dir"] == "UP":
                    # BOS UP opposes SELL: block only while price remains above level (+buf)
                    bos["blocked"] = (lc > (lvl + buf))
                else:
                    # BOS DOWN opposes BUY: block only while price remains below level (-buf)
                    bos["blocked"] = (lc < (lvl - buf))

            return bos

        except Exception:
            return None

    bos_meta_h1 = None
    try:
        bos_meta_h1 = _detect_last_bos(bars, atr_val=float(atr), max_age_bars=int(BOS_MAX_AGE_BARS))
    except Exception:
        bos_meta_h1 = None

    bos_meta_m15 = None
    try:
        bars_m15 = row_h1.get("bars_m15") or []
        if isinstance(bars_m15, list) and len(bars_m15) >= 30:
            try:
                M15_MAX_AGE = int(os.getenv("XTL_BOS_M15_MAX_AGE_BARS", "24"))
            except Exception:
                M15_MAX_AGE = 24
            bos_meta_m15 = _detect_last_bos(bars_m15, atr_val=None, max_age_bars=int(M15_MAX_AGE))
    except Exception:
        bos_meta_m15 = None

    

    def _bootstrap_taps_from_history(bars_in, zone, direction, atr, zone_half, now_ms, lookback_days, tf_ms):
        norm = _normalize_snap_bars_to_ms(bars_in or [], int(tf_ms))
        if not norm:
            return {"tap_count": 0, "moved_away": True, "last_ts": 0, "first_ts": 0,
                    "zone_level": float(zone.get("level") or 0.0), "last_tap_bar_ms": 0,
                    "sweep_ts": 0, "sweep_side": None, "sweep_edge": None,
                    "bootstrapped": True, "boot_lb_days": int(lookback_days),
                    "boot_cutoff_ms": 0, "boot_scanned_bars": 0}

        zone_low = float(zone["low"]); zone_high = float(zone["high"])
        move_req = float(MOVE_AWAY_ATR) * float(atr)

        # cutoff
        cutoff = int(now_ms) - int(lookback_days) * 24 * 60 * 60 * 1000
        scanned = 0

        tap_count = 0
        moved_away = True
        first_ts = 0
        last_ts = 0
        last_tap_bar_ms = 0

        for b in norm:
            t = int(b.get("t_close_ms") or 0)
            if t <= 0 or t < cutoff:
                continue
            scanned += 1

            h = float(b.get("h") or 0.0)
            l = float(b.get("l") or 0.0)
            c = float(b.get("c") or 0.0)

            overlap = (l <= zone_high) and (h >= zone_low)

            # away condition (directional)
            if direction == "BUY":
                away = (c - zone_high) >= move_req
            else:
                away = (zone_low - c) >= move_req

            if away:
                moved_away = True

            # Count a tap only on entering overlap while moved_away is True
            if overlap and moved_away:
                tap_count += 1
                moved_away = False
                last_tap_bar_ms = t
                last_ts = int(t)
                if first_ts <= 0:
                    first_ts = int(t)

        return {
            "tap_count": int(tap_count),
            "moved_away": bool(moved_away),
            "last_ts": int(last_ts or 0),
            "first_ts": int(first_ts or 0),
            "zone_level": float(zone.get("level") or 0.0),
            "zone_low": float(zone_low),
            "zone_high": float(zone_high),
            "last_tap_bar_ms": int(last_tap_bar_ms or 0),
            "sweep_ts": 0, "sweep_side": None, "sweep_edge": None,
            "bootstrapped": True,
            "boot_lb_days": int(lookback_days),
            "boot_cutoff_ms": int(cutoff),
            "boot_scanned_bars": int(scanned),
        }



    def _tap_key_for_zone(sym_u: str, direction: str, zone: dict, tf_tag: str, *, zone_tf: str | None) -> str:
        """
        Tap state must be scoped to the zone identity, not request TF.
        Otherwise taps leak between different zones and between H1/H4 sources.
        """
        ztf = str(zone_tf or tf_tag or "H1").upper()
        try:
            lvl = float(zone.get("level") or 0.0)
        except Exception:
            lvl = 0.0

        # keep it stable and short; 3 decimals is enough for FX/XAU SR
        # Keep key stable but precise enough to avoid collisions:
        # - FX pairs: 5 decimals (pip=0.0001)
        # - JPY pairs: 3 decimals (pip=0.01)
        # - XAUUSD: 2 decimals (0.01)
        try:
            if sym_u == "XAUUSD":
                dec = 2
            elif sym_u.endswith("JPY"):
                dec = 3
            else:
                dec = 5
        except Exception:
            dec = 5

        lvl_s = (f"{lvl:.{dec}f}".rstrip("0").rstrip(".") if lvl > 0 else "0")


        return f"xtl:zone:tap:{sym_u}:{direction}:{ztf}@{lvl_s}"

    # -------------------------------
    # 4) Tap detection + state (Redis)
    # -------------------------------
    R = _r()
    # zone identity key (prevents tap leakage across zones / TF)
    key = _tap_key_for_zone(sym_u, direction, zone, tf_tag, zone_tf=zone_tf)


    tapped = (float(l) <= float(zone["high"])) and (float(h) >= float(zone["low"]))
    # ------------------------------------------------------------
    # derive last CLOSED bar close-ms once for this gate (c_ms)
    # used for dedup + debug + session weighting
    # ------------------------------------------------------------
    c_ms = 0
    try:
        _bars0 = row_h1.get("bars") or row_h1.get("ohlc") or []
    except Exception:
        _bars0 = []

    try:
        c0, _p0 = _pick_last_closed_bar_from_bars(_bars0, int(now_ms), int(tf_ms))
    except Exception:
        c0, _p0 = None, None

    try:
        if isinstance(c0, dict):
            c_ms = int(c0.get("t_close_ms") or c0.get("t") or 0)
            # normalize seconds -> ms if needed
            if 0 < c_ms < 10_000_000_000:
                c_ms *= 1000
    except Exception:
        c_ms = 0

    if debug_gate:	
        try:
            rk = f"xtl:zone:tap:DBG:{sym_u}:{direction}:{tf_tag.upper()}"
            R.setex(rk, 600, str(int(now_ms)))
        except Exception:
            pass

    

    # 4A) Load existing tap-state; if missing -> bootstrap from last N days of H1 bars
    raw_st = None
    try:
        raw_st = _json_load_twice(R.get(key))
    except Exception:
        raw_st = None
    # 4A.1) Reset tap-state if the zone moved materially (prevents stale tap_count on a new zone)
    try:
        zone_low_now = float(zone.get("low") or 0.0)
        zone_high_now = float(zone.get("high") or 0.0)
        zone_level_now = float(zone.get("level") or 0.0)

        # "Material" threshold: half-zone width (robust, direction-agnostic)
        zone_half_now = abs(zone_high_now - zone_low_now) / 2.0
        if zone_half_now <= 0:
            zone_half_now = 0.0

        if isinstance(raw_st, dict) and raw_st:
            prev_low = float(raw_st.get("zone_low") or 0.0)
            prev_high = float(raw_st.get("zone_high") or 0.0)
            prev_level = float(raw_st.get("zone_level") or 0.0)

            # Reset if any edge/level drifted by >= 1 * half-zone width
            zone_changed = (
                (zone_half_now > 0.0) and (
                    abs(prev_low - zone_low_now) >= zone_half_now or
                    abs(prev_high - zone_high_now) >= zone_half_now or
                    abs(prev_level - zone_level_now) >= zone_half_now
                )
            )

            if zone_changed:
                try:
                    R.delete(key)  # wipe old tap-state (single source of truth)
                except Exception:
                    pass
                raw_st = None  # forces bootstrap / fresh state
    except Exception:
        # never block gate due to reset logic
        pass


    if (not isinstance(raw_st, dict)) or (not raw_st):
        # Bootstrap taps from history so bot-start-late still respects earlier taps
        try:
            lookback_days = int(os.getenv("XTL_TAP_BOOTSTRAP_DAYS", "3"))
        except Exception:
            lookback_days = 3

        try:
            if isinstance(zone, dict) and bool(zone.get("is_strong")):
                lookback_days = max(
                    int(lookback_days),
                    int(os.getenv("XTL_STRONG_ZONE_LOOKBACK_DAYS", "15")),
                )
        except Exception:
            pass


        # build a "fresh" state inferred from recent bars
        try:
            tf_ms0 = int(TF_MS.get(tf_tag.upper(), 60 * 60 * 1000))
        except Exception:
            tf_ms0 = 60 * 60 * 1000

        # IMPORTANT: bootstrap should NOT include the current bar 'c' you are evaluating now.
        bars_hist = bars[:-1] if isinstance(bars, list) and len(bars) > 2 else (bars or [])
        try:
            st = _bootstrap_taps_from_history(
                bars_hist,
                zone=zone,
                direction=direction,
                atr=float(atr),
                zone_half=float(zone_half),
                now_ms=int(now_ms),
                lookback_days=int(lookback_days),
                tf_ms=int(tf_ms0),
            )
            # --- FIX: bootstrap should set first_ts/last_ts to real tap history times, not now() ---
            try:
                # if bootstrap returns a tap timeline, use it; else derive from last_tap_bar_ms
                ltb = int(st.get("last_tap_bar_ms", 0) or 0)
                if ltb > 0 and int(st.get("first_ts", 0) or 0) <= 0:
                    st["first_ts"] = ltb  # fallback: at least not "now"
                if ltb > 0:
                    st["last_ts"] = int(st.get("last_ts", 0) or 0) or ltb
            except Exception:
                pass

            if debug_gate:
                try:
                    log.warning(
                        "[TAPBOOT] sym=%s dir=%s tf=%s days=%s tap_count=%s moved_away=%s last_ts=%s",
                        sym_u, direction, tf_tag, lookback_days,
                        int(st.get("tap_count", 0) or 0),
                        bool(st.get("moved_away", True)),
                        int(st.get("last_ts", 0) or 0),
                    )
                except Exception:
                    pass
        except Exception:
            st = {}
    else:
        st = raw_st

    # Ensure required defaults always exist
    st = st or {}
    st.setdefault("tap_count", 0)
    st.setdefault("moved_away", True)
    st.setdefault("last_ts", 0)
    st.setdefault("first_ts", 0)
    st.setdefault("last_tap_bar_ms", 0)
    st.setdefault("sweep_ts", 0)
    st.setdefault("sweep_side", None)
    st.setdefault("sweep_edge", None)
    # -------------------------------
    # Reset if zone changed materially
    # IMPORTANT: must run BEFORE overwriting st["zone_*"]
    # -------------------------------
    try:
        prev_level = float(st.get("zone_level") or 0.0)
        prev_low   = float(st.get("zone_low") or 0.0)
        prev_high  = float(st.get("zone_high") or 0.0)

        cur_level = float(zone.get("level") or 0.0)
        cur_low   = float(zone.get("low") or 0.0)
        cur_high  = float(zone.get("high") or 0.0)

        prev_w = max(0.0, prev_high - prev_low)
        cur_w  = max(0.0, cur_high - cur_low)

        # material threshold: half current zone width (with tiny floor)
        eps = max(cur_w * 0.50, 1e-6)

        zone_changed = (
            (prev_level > 0.0) and (
                abs(cur_level - prev_level) > eps or
                abs(cur_low   - prev_low)   > eps or
                abs(cur_high  - prev_high)  > eps or
                abs(cur_w     - prev_w)     > eps
            )
        )

        if zone_changed:
            st["tap_count"] = 0
            st["moved_away"] = True
            st["last_ts"] = 0
            st["first_ts"] = 0
            st["last_tap_bar_ms"] = 0
            st["sweep_ts"] = 0
            st["sweep_side"] = None
            st["sweep_edge"] = None
    except Exception:
        pass

    # FORCE current zone params into state (single source of truth)
    st["zone_level"] = float(zone.get("level") or 0.0)
    st["zone_low"]   = float(zone.get("low") or 0.0)
    st["zone_high"]  = float(zone.get("high") or 0.0)
    

    # move-away reset (directional)
    try:
        move_req = float(MOVE_AWAY_ATR) * float(atr)
        if direction == "BUY":
            moved = (float(cl) - float(zone["high"])) >= move_req
        else:
            moved = (float(zone["low"]) - float(cl)) >= move_req
        if moved:
            st["moved_away"] = True
    except Exception:
        pass


    # ---- DEBUG (tap state) ----
    try:
        if bool(debug_gate):
            # directional moved-away distance
            if direction == "BUY":
                dist = float(cl) - float(zone["high"])
            else:
                dist = float(zone["low"]) - float(cl)

            move_req = float(MOVE_AWAY_ATR) * float(atr)

            # show once per bar close to avoid spam
            cbar = int(c_ms or 0)
            last_dbg = int(st.get("last_dbg_bar_ms") or 0)
            if cbar > 0 and cbar != last_dbg:
                st["last_dbg_bar_ms"] = cbar
                try:
                    rk2 = f"xtl:zone:tap:DBG:{sym_u}:{direction}:{tf_tag.upper()}"
                    R.setex(rk2, 600, json.dumps(st, separators=(",", ":")))

                except Exception:
                    pass

                log.warning(
                    "[TAPDBG] sym=%s dir=%s tf=%s tapped=%s tap_count=%s moved_away=%s dist=%.6f req=%.6f zone=[%.6f..%.6f] cl=%.6f",
                    sym, direction, tf_tag, bool(tapped),
                    int(st.get("tap_count", 0) or 0),
                    bool(st.get("moved_away", True)),
                    float(dist), float(move_req),
                    float(zone["low"]), float(zone["high"]),
                    float(cl),
                )
    except Exception:
        pass



    
    # ----------------------------------------
    # TAP STATE + PERSIST (single source of truth)
    # ----------------------------------------
    

    # Start from stored count
    tap_count = int(st.get("tap_count", 0) or 0)

    # moved_away comes from state; default True until we see a valid tap
    moved_away = bool(st.get("moved_away", True))
    try:
        zone_age_days = (int(now_ms) - int(st.get("first_ts") or now_ms)) / (24 * 3600 * 1000)
    except Exception:
        zone_age_days = 0

    try:
        FRESH_DAYS = int(os.getenv("XTL_FRESH_ZONE_DAYS", "3"))  # start with 3
    except Exception:
         FRESH_DAYS = 3
    FRESH_MS = int(FRESH_DAYS) * 24 * 3600 * 1000

    # ----------------------------
    # Dynamic max taps (fresh/old + volatility)
    # ----------------------------
    try:
        FRESH_MAX_TAPS = int(os.getenv("XTL_FRESH_MAX_TAPS", "3"))
    except Exception:
        FRESH_MAX_TAPS = 3

    if zone_age_days <= float(FRESH_DAYS):
        max_taps_dynamic = int(FRESH_MAX_TAPS)
    else:
        max_taps_dynamic = int(MAX_TAPS)


    # Volatility bump: if ATR% is higher, allow a couple more taps (H1 FX needs this)
    try:
        # ATR as fraction of price (e.g., 0.0010 = 0.10%)
        atr_pct = abs(float(atr or 0.0)) / max(abs(float(cl or 0.0)), 1e-9)
    except Exception:
        atr_pct = 0.0

    try:
        TAP_VOL1 = float(os.getenv("XTL_TAP_VOL1", "0.0008"))  # +1 tap above this ATR%
    except Exception:
        TAP_VOL1 = 0.0008
    try:
        TAP_VOL2 = float(os.getenv("XTL_TAP_VOL2", "0.0012"))  # +2 taps above this ATR%
    except Exception:
        TAP_VOL2 = 0.0012
    try:
        MAX_TAPS_CAP = int(os.getenv("XTL_MAX_TAPS_CAP", "7"))  # hard safety cap
    except Exception:
        MAX_TAPS_CAP = 7

    if atr_pct >= TAP_VOL1:
        max_taps_dynamic += 1
    if atr_pct >= TAP_VOL2:
        max_taps_dynamic += 1

    # clamp
    max_taps_dynamic = max(3, min(int(max_taps_dynamic), int(MAX_TAPS_CAP)))


    # Count a tap once per closed H1 bar (avoid double count)
    if tapped:
        last_bar = int(st.get("last_tap_bar_ms", 0) or 0)
        # derive current closed bar close-ms for tap dedup (no c_ms variable in this scope)
        try:
            _bars = row_h1.get("bars") or row_h1.get("ohlc") or []
        except Exception:
            _bars = []

        try:
            c, p = _pick_last_closed_bar_from_bars(_bars, now_ms, tf_ms)
        except Exception:
            c, p = None, None

        this_bar = int((c or {}).get("t_close_ms") or 0)
        # normalize seconds->ms if needed
        if 0 < this_bar < 10_000_000_000:
            this_bar *= 1000

        # --- repair: poisoned state (last_bar saved earlier but tap_count stayed 0) ---
        if tap_count < 1 and last_bar > 0 and this_bar > 0 and this_bar == last_bar:
            tap_count = 1
            st["tap_count"] = tap_count
            st["first_ts"] = int(st.get("first_ts") or now_ms)
            st["last_ts"] = int(now_ms)
            st["last_tap_ms"] = int(now_ms)
            st["moved_away"] = False

        if this_bar > 0 and this_bar != last_bar:
            # ONLY count a new tap if price had moved away enough since the last tap
            if bool(st.get("moved_away", True)):
                tap_count = min(int(MAX_TAPS) + 1, tap_count + 1)
                st["tap_count"] = tap_count
                st["last_ts"] = int(now_ms)
                st["last_tap_bar_ms"] = this_bar
                st["last_tap_ms"] = int(now_ms)
                st["moved_away"] = False  # must move away again before next tap counts
                if tap_count == 1 and int(st.get("first_ts") or 0) <= 0:
                    st["first_ts"] = int(now_ms)
            else:
                # touched again but never moved away -> do NOT increment tap_count
                st["last_ts"] = int(now_ms)  # optional: keep last_ts fresh for debugging

    else:
        # no tap this bar; keep moved_away state
        if moved_away:
            st["moved_away"] = True

    # Apply break effects (key is defined now)
    if break_state == "HARD_BREAK":
        try:
            R.delete(key)
        except Exception:
            pass
        return False, {
            "reason": "hard_break",
            "break": break_meta,
            "zone": {**zone, "type": zone_type},
            "bos_h1": bos_meta_h1,
            "bos_m15": bos_meta_m15,
            "tap_key": key,
            "tap_count": int(tap_count),
            "stage": "ZONE_GATE",
            "blocked": True,
        }

    if break_state == "SWEEP":
        st["sweep_ts"] = int(now_ms)
        st["sweep_side"] = str(break_meta.get("side") or "")
        st["sweep_edge"] = float(break_meta.get("edge") or 0.0)

    # Persist TAP state (ALWAYS attempt)
    persist_ok = False
    persist_exists = None
    persist_ttl = None
    persist_len = None
    persist_exc_type = None
    persist_exc = None

    try:
        ttl_sec = int(MAX_TAP_BARS) * 3600
        # refuse to persist empty/invalid state
        if not isinstance(st, dict) or not st:
            st = {
               "tap_count": 0,
               "moved_away": True,
               "last_ts": 0,
               "first_ts": 0,
               "zone_level": float(zone.get("level") or 0.0),
               "last_tap_bar_ms": 0,
               "sweep_ts": 0,
               "sweep_side": None,
               "sweep_edge": None,
            }
        payload = json.dumps(st, separators=(",", ":"))
        if len(payload) <= 2:  # "{}"
            raise ValueError("Refusing to persist empty tap state")
        R.set(key, payload)
        
        # verify by reading the value back
        val = R.get(key)
        persist_len = len(val) if isinstance(val, (bytes, bytearray, str)) else 0
        persist_ttl = int(R.ttl(key) or -999)
        persist_exists = int(R.exists(key) or 0)
        persist_ok = (persist_len > 2) and (persist_exists == 1) and (persist_ttl in (-1,) or persist_ttl > 0)
    except Exception as e:
        persist_exc_type = type(e).__name__
        persist_exc = str(e)
        

    # Debug payload (ALWAYS available in return dicts)
    debug_out = {
        "tap_key": key,
        "tap_count": int(tap_count),
        "tapped": bool(tapped),
        "moved_away": bool(st.get("moved_away", True)),
        "persist_ok": bool(persist_ok),
        "persist_exists": persist_exists,
        "persist_ttl_sec": persist_ttl,
        "persist_len": persist_len,
        "persist_exc_type": persist_exc_type,
        "persist_exc": persist_exc,
        "__tap_marker__": "TAP_PERSIST_RAN",
        "__tap_dbg_gate__": bool(debug_gate),
    }
    # --- post-persist verification (catch deletes after persist) ---
    debug_out["tap_key_repr"] = repr(key)
    # --- attach reference price used for SR/zone selection ---
    try:
        debug_out["price"] = float(px) if px is not None else None
        debug_out["price_src"] = px_src
        debug_out["price_ts_ms"] = int(px_ts_ms) if px_ts_ms is not None else None
    except Exception:
        pass
    
    # --- attach zone used for tap logic (so jq can show it) ---
    try:
        zu = None
        if isinstance(gate_meta, dict):
            zu = gate_meta.get("zone_used")

        if isinstance(zu, dict) and zu:
            debug_out["zone_used"] = dict(zu)  # single source of truth
        else:
            debug_out["zone_used"] = {
                "level": float(zone.get("level") or 0.0),
                "low": float(zone.get("low") or 0.0),
                "high": float(zone.get("high") or 0.0),
                "touches": int(zone.get("touches") or 0),
                "strength": float(zone.get("strength") or 0.0),
                "sr_score": float(zone.get("sr_score") or 0.0),
                "is_strong": bool(zone.get("is_strong") or False),
                "tf": str(zone.get("tf") or ""),
                "type": str(zone.get("type") or ""),
                "half": float(zone_half or 0.0),
                "atr": float(atr or 0.0),
            }
    except Exception:
        pass

    # --- attach stored tap-state snapshot (what we loaded / bootstrapped) ---
    try:
        debug_out["tap_state"] = {
            "tap_count_st": int(st.get("tap_count", 0) or 0),
            "moved_away_st": bool(st.get("moved_away", True)),
            "first_ts": int(st.get("first_ts", 0) or 0),
            "last_ts": int(st.get("last_ts", 0) or 0),
            "last_tap_bar_ms": int(st.get("last_tap_bar_ms", 0) or 0),
            "last_tap_ms": int(st.get("last_tap_ms", 0) or 0),
            "zone_level_st": float(st.get("zone_level") or 0.0),
            "zone_low_st": float(st.get("zone_low") or 0.0),
            "zone_high_st": float(st.get("zone_high") or 0.0),
            "bootstrapped": bool(st.get("bootstrapped", False)),
            "boot_lb_days": int(st.get("boot_lb_days", 0) or 0),
            "boot_scanned_bars": int(st.get("boot_scanned_bars", 0) or 0),
        }
    except Exception:
        pass

    # --- tap decision basis (no extra bar data needed) ---
    try:
        debug_out["tap_basis"] = {
            "tapped_now": bool(tapped),
            "dedup_last_tap_bar_ms": int(st.get("last_tap_bar_ms", 0) or 0),
            "moved_away_required": bool(st.get("moved_away", True)),
            "max_taps_dynamic": int(max_taps_dynamic),
            "fresh_days": int(FRESH_DAYS) if "FRESH_DAYS" in locals() else None,
        }
    except Exception:
        pass


    
    try:
        debug_out["post_exists"] = int(R.exists(key) or 0)
        debug_out["post_ttl"] = int(R.ttl(key) or -999)
        debug_out["post_type"] = str(R.type(key) or "")
        
    except Exception as _e:
        debug_out["post_check_exc"] = f"{type(_e).__name__}:{_e}"

    try:
        kw = getattr(getattr(R, "connection_pool", None), "connection_kwargs", {}) or {}
        debug_out["__tap_redis_host__"] = kw.get("host")
        debug_out["__tap_redis_port__"] = kw.get("port")
        debug_out["__tap_redis_db__"] = kw.get("db")
    except Exception:
        pass
    
    # --- BOS override (DEBUG ONLY) must be BEFORE the early return ---
    if debug_gate and os.getenv("XTL_DEBUG_IGNORE_BOS", "0") == "1":
        try:
            if isinstance(bos_meta_h1, dict):
                bos_meta_h1["blocked"] = False
                bos_meta_h1["__bos_ignored__"] = True
        except Exception:
            pass
    if isinstance(bos_meta_h1, dict) and bool(bos_meta_h1.get("blocked")):
        return False, {"reason": "bos_opposite_h1", "bos_h1": bos_meta_h1, "zone": {**zone, "type": zone_type},"stage": "ZONE_GATE",
        "blocked": True,
        **debug_out,}
    


    if isinstance(bos_meta_m15, dict) and bool(bos_meta_m15.get("blocked")):
        return False, {"reason": "bos_opposite_m15", "bos_m15": bos_meta_m15, "zone": {**zone, "type": zone_type},"stage": "ZONE_GATE",
        "blocked": True,
        **debug_out,}

    # IMPORTANT: return paths MUST include debug_out so curl/jq proves it ran
    if tap_count < 1:
        return False, {
            "reason": "tap_lt_1",
            "zone": {**zone, "type": zone_type},
            "break_state": break_state,
            "break": break_meta,
            "stage": "ZONE_GATE",
            "blocked": True,
            **debug_out,
        }

    # -------------------------------
    # Fresh/Old zone policy
    # -------------------------------
    

    try:
        first_ts0 = int(st.get("first_ts", 0) or 0)
    except Exception:
        first_ts0 = 0

    zone_is_fresh = (first_ts0 > 0) and (int(now_ms) - first_ts0 <= FRESH_MS)


    # "old powerful" heuristic: higher TF OR strong zone_type/strength/touches
    try:
        ztf0 = str(zone.get("tf") or zone_tf or tf_tag or "H1").upper()
    except Exception:
        ztf0 = str(tf_tag or "H1").upper()

    try:
        ztype0 = str(zone.get("type") or zone_type or "").upper()
    except Exception:
        ztype0 = ""

    try:
        z_strength0 = float(zone.get("strength") or 0.0)
    except Exception:
        z_strength0 = 0.0

    try:
        z_touches0 = int(zone.get("touches") or 0)
    except Exception:
        z_touches0 = 0

    old_powerful = (
        (ztf0 in ("H4", "D", "DAILY", "W1")) or
        ("MAJOR" in ztype0) or
        (z_strength0 >= 8.0) or
        (z_touches0 >= 6)
    )
    # --- debug: show computed tap allowance ---
    try:
        debug_out["tap_dyn"] = {
            "tap_count": int(tap_count),
            "max_taps_dynamic": int(max_taps_dynamic),
            "atr_pct": float(atr_pct),
        }
    except Exception:
        pass


    if tap_count > int(max_taps_dynamic):
        # Your rule: if zone is fresh and already over-tapped -> discard this zone
        if zone_is_fresh:
            # IMPORTANT: discard should NOT look like a hard "blocked" gate
            return False, {
                "reason": "discard_fresh_zone_too_many_taps",
                "fresh_days": int(FRESH_DAYS),
                "max_taps_fresh": int(max_taps_dynamic),
                "zone": {**zone, "type": zone_type},
                "stage": "ZONE_GATE",
                "blocked": False,
                **debug_out,
            }

        # Old + powerful zones: allow even if over-tapped (do not hard block)
        if old_powerful:
            # Continue gate evaluation (reversal logic decides entry)
            pass
        else:
            # Old but not powerful -> keep current behavior (block)
            return False, {
                "reason": "too_many_taps",
                "max_taps_fresh": int(max_taps_dynamic),
                "fresh_days": int(FRESH_DAYS),
                "zone": {**zone, "type": zone_type},
                "stage": "ZONE_GATE",
                "blocked": True,
                **debug_out,
            }


    # Tap2 staleness
    try:
        tap_age_ms = int(now_ms) - int(st.get("last_ts", 0) or 0)
    except Exception:
        tap_age_ms = int(MAX_TAP2_AGE_MS) + 1

    if tap_age_ms > int(MAX_TAP2_AGE_MS):
        try:
            R.delete(key)
        except Exception:
            pass
        return False, {"reason": "tap_stale", "tap_count": tap_count}

    # If sweep armed: require reclaim window
    try:
        sweep_ts = int(st.get("sweep_ts") or 0)
    except Exception:
        sweep_ts = 0

    if sweep_ts > 0:
        try:
            ref_ms = int(c_ms or now_ms)
            sweep_age_bars = int((ref_ms - sweep_ts) / (60 * 60 * 1000))
            if sweep_age_bars < 0:
                sweep_age_bars = 0
        except Exception:
            sweep_age_bars = 99

        if sweep_age_bars > int(RECLAIM_MAX_BARS):
            try:
                R.delete(key)
            except Exception:
                pass
            return False, {
                "reason": "sweep_no_reclaim",
                "sweep_age_bars": sweep_age_bars,
                "reclaim_max_bars": int(RECLAIM_MAX_BARS),
                "zone": {**zone, "type": zone_type},
                "break": {"state": "SWEEP", **break_meta},
            }

        inside = (float(cl) >= float(zone["low"])) and (float(cl) <= float(zone["high"]))
        if direction == "BUY":
            reclaim_ok = inside and (float(cl) >= float(zone["level"]))
        else:
            reclaim_ok = inside and (float(cl) <= float(zone["level"]))

        if not reclaim_ok:
            return True, {
                "reason": "ARMED_SWEEP_WAIT_RECLAIM",
                "zone": {**zone, "type": zone_type},
                "tap_count": tap_count,
                "break_state": "SWEEP",
                "break": break_meta,
                "sweep_age_bars": sweep_age_bars,
                "reclaim_needed": True,
                "bos_h1": bos_meta_h1,
                "bos_m15": bos_meta_m15,
            }

        try:
            if abs(float(cl) - float(zone["level"])) > (float(RECLAIM_TIGHTEN_ATR) * float(atr)):
                return True, {
                    "reason": "ARMED_RECLAIM_TOO_FAR",
                    "zone": {**zone, "type": zone_type},
                    "tap_count": tap_count,
                    "break_state": "SWEEP",
                    "break": break_meta,
                    "reclaim_needed": False,
                    "tighten_atr": float(RECLAIM_TIGHTEN_ATR),
                    "bos_h1": bos_meta_h1,
                    "bos_m15": bos_meta_m15,
                }
        except Exception:
            pass

        # reclaim succeeded -> clear sweep marker
        st["sweep_ts"] = 0
        st["sweep_side"] = None
        st["sweep_edge"] = None
        try:
            R.set(key, json.dumps(st))
        except Exception:
            pass

    # -------------------------------
    # 5) Reversal confirmation candle (tap2)
    # -------------------------------
    body = abs(float(cl) - float(o))
    rng = max(float(h) - float(l), 1e-6)

    if direction == "BUY":
        lower_wick = min(float(o), float(cl)) - float(l)
        pin_ok = (float(cl) > float(o)) and ((lower_wick / rng) >= 0.45) and ((body / rng) <= 0.40)
    else:
        upper_wick = float(h) - max(float(o), float(cl))
        pin_ok = (float(cl) < float(o)) and ((upper_wick / rng) >= 0.45) and ((body / rng) <= 0.40)

    eng_ok = False
    try:
        po = float(_bar_f(p, "o", "open", default=None) or _bar_f(p, "c", "close"))
        pc = float(_bar_f(p, "c", "close"))
        if direction == "BUY":
            eng_ok = (float(cl) > float(o)) and (float(o) <= pc) and (float(cl) >= po)
        else:
            eng_ok = (float(cl) < float(o)) and (float(o) >= pc) and (float(cl) <= po)
    except Exception:
        eng_ok = False

    if direction == "BUY":
        close_ok = (float(cl) >= (float(l) + 0.60 * rng)) or (float(cl) >= float(zone["level"]))
        body_ok = (body / rng) >= 0.20
        close_reject_ok = bool(close_ok and body_ok and (float(cl) > float(o)))
    else:
        close_ok = (float(cl) <= (float(h) - 0.60 * rng)) or (float(cl) <= float(zone["level"]))
        body_ok = (body / rng) >= 0.20
        close_reject_ok = bool(close_ok and body_ok and (float(cl) < float(o)))

    rev_ok = bool(pin_ok or eng_ok or close_reject_ok)
    if not rev_ok:
        conf_tmp = "medium" if str(zone_type).lower() in ("sr", "sr_confirmed") else "low"
        return True, {
            "reason": "ARMED_TAP",
            "zone": {**zone, "type": zone_type},
            "tap_count": tap_count,
            "rev_ok": False,
            "pin_ok": bool(pin_ok),
            "eng_ok": bool(eng_ok),
            "close_reject_ok": bool(close_reject_ok),
            "body_rng": (float(body) / float(rng)) if rng else None,
            "confidence": conf_tmp,
            "break_state": break_state,
            "break": break_meta,
            "bos_h1": bos_meta_h1,
            "bos_m15": bos_meta_m15,
        }

    if pin_ok:
        rev_path = "pin"
    elif eng_ok:
        rev_path = "engulf"
    else:
        rev_path = "close_reject"

    # -------------------------------
    # 6) Volume confirmation (confidence-only by default)
    # -------------------------------
    try:
        VOL_LOOKBACK = int(os.getenv("XTL_VOL_LOOKBACK", "20"))
    except Exception:
        VOL_LOOKBACK = 20
    try:
        VOL_MIN_MULT = float(os.getenv("XTL_VOL_MIN_MULT", "1.25"))
    except Exception:
        VOL_MIN_MULT = 1.25
    VOL_BLOCK_IF_FAIL = os.getenv("XTL_VOL_BLOCK_IF_FAIL", "0") == "1"

    vol_ok = None
    vol_ratio = None
    try:
        vols = []
        for b in bars[-int(VOL_LOOKBACK):]:
            if not isinstance(b, dict):
                continue
            v = b.get("v") or b.get("vol") or b.get("volume")
            if isinstance(v, (int, float)):
                vols.append(float(v))
        if len(vols) >= 5:
            avg_vol = sum(vols[:-1]) / max(len(vols) - 1, 1)
            cur_vol = vols[-1]
            if avg_vol > 0:
                vol_ratio = cur_vol / avg_vol
                vol_ok = bool(vol_ratio >= float(VOL_MIN_MULT))
    except Exception:
        vol_ok = None
        vol_ratio = None

    if vol_ok is False and VOL_BLOCK_IF_FAIL:
        return False, {"reason": "low_volume", "vol_ratio": vol_ratio, "zone": {**zone, "type": zone_type}}

    # -------------------------------
    # 7) Session weighting
    # -------------------------------
    try:
        SESSION_BOOST_LONDON = float(os.getenv("XTL_SESSION_BOOST_LONDON", "1.10"))
    except Exception:
        SESSION_BOOST_LONDON = 1.10
    try:
        SESSION_BOOST_NY = float(os.getenv("XTL_SESSION_BOOST_NY", "1.10"))
    except Exception:
        SESSION_BOOST_NY = 1.10
    try:
        SESSION_PENALTY_ASIA = float(os.getenv("XTL_SESSION_PENALTY_ASIA", "0.95"))
    except Exception:
        SESSION_PENALTY_ASIA = 0.95

    def _session_weight(ms: int) -> float:
        try:
            utc_h = datetime.utcfromtimestamp(ms / 1000.0).hour
        except Exception:
            return 1.0
        if 7 <= utc_h <= 12:
            return float(SESSION_BOOST_LONDON)
        if 13 <= utc_h <= 17:
            return float(SESSION_BOOST_NY)
        return float(SESSION_PENALTY_ASIA)

    sess_w = float(_session_weight(int(c_ms or now_ms)))

    # -------------------------------
    # 8) Zone aging score
    # -------------------------------
    try:
        ZONE_MAX_AGE_BARS = int(os.getenv("XTL_ZONE_MAX_AGE_BARS", "30"))
    except Exception:
        ZONE_MAX_AGE_BARS = 30
    try:
        ZONE_AGE_PENALTY_AFTER = int(os.getenv("XTL_ZONE_AGE_PENALTY_AFTER", "18"))
    except Exception:
        ZONE_AGE_PENALTY_AFTER = 18

    zone_age_bars = 0
    try:
        if int(st.get("first_ts") or 0) > 0:
            zone_age_bars = int((int(now_ms) - int(st["first_ts"])) / (60 * 60 * 1000))
    except Exception:
        zone_age_bars = 0

    age_penalize = bool(zone_age_bars > int(ZONE_AGE_PENALTY_AFTER))

    # -------------------------------
    # 9) Final confidence
    # -------------------------------
    conf = "high" if str(zone_type).lower() in ("sr", "sr_confirmed") else "medium"
    if age_penalize and conf == "high":
        conf = "medium"
    if vol_ok is False and conf == "high":
        conf = "medium"
    if sess_w < 1.0 and conf == "high":
        conf = "medium"
    elif sess_w < 1.0 and conf == "medium":
        conf = "low"

    # Too old: allow but don’t boost (you can choose to block if you want)
    if zone_age_bars > int(ZONE_MAX_AGE_BARS):
        pass

    return True, {
        "zone": {**zone, "type": zone_type},
        "tap_count": int(tap_count),
        "zone_age_bars": int(zone_age_bars),
        "vol_ratio": vol_ratio,
        "vol_ok": vol_ok,
        "session_weight": sess_w,
        "confidence": conf,
        "rev_ok": True,
        "rev_path": rev_path,
        "body_rng": float(body / rng),
        "reason": "REVERSAL_OK",
        "break_state": break_state,
        "break": break_meta,
        "bos_h1": bos_meta_h1,
        "bos_m15": bos_meta_m15,
    }


# Minimum overall opportunity score (0-100 scale) before we surface an item.
# Can be tuned or overridden via env var: XTREND_OPP_SCORE_MIN
OPP_SCORE_MIN: float = float(os.getenv("XTREND_OPP_SCORE_MIN", "35.0") or 35.0)


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

def _oppt_cfg(sym: str, tfu: str) -> dict:
    m = _get_meta(sym)
    tfu = (tfu or "").upper()

    # per-symbol override: meta["oppt_tf"][TF]
    v = None
    try:
        ot = m.get("oppt_tf")
        if isinstance(ot, dict):
            v = ot.get(tfu)
    except Exception:
        v = None

    # common default: _MetaCache.common["oppt_tf"][TF]
    if not isinstance(v, dict):
        try:
            c = getattr(_MetaCache, "common", {}) or {}
            ot2 = c.get("oppt_tf") if isinstance(c, dict) else None
            if isinstance(ot2, dict):
                v = ot2.get(tfu)
        except Exception:
            v = None

    return v if isinstance(v, dict) else {}


def _room_thr_h1(sym: str) -> float:
    cfg = _oppt_cfg(sym, "H1")
    v = cfg.get("min_room_pct")
    if isinstance(v, (int, float)) and float(v) > 0:
        return float(v)
    return float(ROOM_THRESHOLDS_H1.get(sym.upper(), 0.23))

def _room_thr_h4(sym: str) -> float:
    cfg = _oppt_cfg(sym, "H4")
    v = cfg.get("min_room_pct")
    if isinstance(v, (int, float)) and float(v) > 0:
        return float(v)
    return float(ROOM_THRESHOLDS_H4.get(sym.upper(), 0.40))

# Where we keep per-symbol frozen H1 opportunity snapshots in Redis

# Where we keep per-symbol frozen H1 opportunity snapshots in Redis
OPP_SNAPSHOT_PREFIX = "xtl:trend:opp:h1"

def _uid_from_user(user) -> str | None:
    if not user:
        return None
    return (
        getattr(user, "id", None)
        or getattr(user, "user_id", None)
        or getattr(user, "uid", None)
    )


def _opp_snapshot_key(sym: str, opp_dir: str) -> str:
    s = (sym or "").upper()
    d = (opp_dir or "").strip().upper()
    if d in ("BUY", "LONG", "UP", "BULL", "BULLISH"):
        d = "UP"
    elif d in ("SELL", "SHORT", "DOWN", "BEAR", "BEARISH"):
        d = "DOWN"
    return f"{OPP_SNAPSHOT_PREFIX}:{s}:{d}"
def _persist_entry_meta_to_snapshot(sym: str, out: dict) -> None:
    """
    Persist frozen entry fields into the active snapshot hash so entry survives across requests.

    Rules:
      - No-op unless entry_triggered=True
      - Never overwrite an existing frozen entry_price/entry_ts_ms if present
      - BUT: if tp/sl are missing (or non-finite) in snapshot and we have good values now,
        we *do* fill them (this fixes "TP/SL moving" caused by partial persistence)
    """
    try:
        sym_u = (sym or "").upper().strip()
        if not sym_u or not isinstance(out, dict) or not bool(out.get("entry_triggered")):
            return

        # direction stored on snapshot is UP/DOWN
        d = str(out.get("opp_direction") or out.get("direction") or "").upper()
        if d in ("BUY", "UP"):
            d = "UP"
        elif d in ("SELL", "DOWN"):
            d = "DOWN"
        if d not in ("UP", "DOWN"):
            return

        snap_key = _opp_snapshot_key(sym_u, d)

        def _j(x):
            # decode json-ish fields
            if x is None:
                return None
            if isinstance(x, (bytes, bytearray)):
                x = x.decode("utf-8", "ignore")
            try:
                return json.loads(x) if isinstance(x, str) else x
            except Exception:
                return x

        def _to_float(x):
            try:
                v = float(x)
                # reject NaN/inf
                if not (v == v) or v in (float("inf"), float("-inf")):
                    return None
                return v
            except Exception:
                return None

        # current snapshot (if exists)
        snap = {}
        try:
            snap = R.hgetall(snap_key) or {}
        except Exception:
            snap = {}

        def _snap_get(k: str):
            v = None
            try:
                v = snap.get(k)
                if v is None:
                    v = snap.get(k.encode("utf-8"))
            except Exception:
                v = None
            return _j(v)

        snap_entry_triggered = bool(_snap_get("entry_triggered"))
        snap_entry_ts = _snap_get("entry_ts_ms")
        snap_entry_px = _to_float(_snap_get("entry_price"))
        snap_tp = _to_float(_snap_get("tp_price"))
        snap_sl = _to_float(_snap_get("sl_price"))

        # new values from 'out'
        out_entry_ts = out.get("entry_ts_ms")
        try:
            out_entry_ts = int(out_entry_ts) if out_entry_ts is not None else None
        except Exception:
            out_entry_ts = None

        out_entry_px = _to_float(out.get("entry_price"))
        out_tp = _to_float(out.get("tp_price"))
        out_sl = _to_float(out.get("sl_price"))

        # If snapshot already frozen and has entry core, do NOT overwrite those.
        # But we *can* fill TP/SL if missing.
        mapping = {}

        # Always ensure entry_triggered True is present
        if not snap_entry_triggered:
            mapping["entry_triggered"] = True

        # freeze core once (first writer wins)
        if snap_entry_px is None and out_entry_px is not None:
            mapping["entry_price"] = out_entry_px
        if (snap_entry_ts is None or int(snap_entry_ts or 0) <= 0) and out_entry_ts is not None:
            mapping["entry_ts_ms"] = out_entry_ts

        # fill TP/SL only if snapshot missing AND out has good values
        if snap_tp is None and out_tp is not None:
            mapping["tp_price"] = out_tp
        if snap_sl is None and out_sl is not None:
            mapping["sl_price"] = out_sl

        # these are “nice to have” and safe to overwrite
        if out.get("entry_signal"):
            mapping["entry_signal"] = str(out.get("entry_signal"))
        if out.get("entry_reason"):
            mapping["entry_reason"] = str(out.get("entry_reason"))

        tp_orig = _to_float(out.get("tp_price_orig"))
        sl_orig = _to_float(out.get("sl_price_orig"))
        if tp_orig is not None:
            mapping["tp_price_orig"] = tp_orig
        if sl_orig is not None:
            mapping["sl_price_orig"] = sl_orig

        mapping["last_status_ms"] = int(out.get("server_now_ms") or out_entry_ts or int(_time.time() * 1000))
        mapping["opp_direction"] = d
        mapping["decision"] = "BUY" if d == "UP" else "SELL"

        # Nothing new to write?
        if not mapping:
            return

        R.hset(snap_key, mapping={k: json.dumps(v) for k, v in mapping.items()})

        # keep snapshot alive post-entry (7d default)
        ttl = int(os.getenv("XTL_OPP_POST_ENTRY_TTL_SEC", str(7 * 24 * 3600)))
        R.expire(snap_key, ttl)
    except Exception:
        pass


def _freeze_or_snapshot_opp(sym: str, row: dict, now_ms: int) -> dict:
    """
    Maintain a stable opportunity snapshot per symbol+direction in Redis.

    - Source of truth for MANUAL trading
    - Entry / TP / SL are frozen and cost-aware
    """

    sym_u = (sym or "").upper().strip()
    if not sym_u:
        return row

    def _sj(x, default=None):
        if x is None:
            return default
        if isinstance(x, (bytes, bytearray)):
            x = x.decode("utf-8", "ignore")
        try:
            return json.loads(x)
        except Exception:
            return x
    import json

    

    def _snap_get_raw_json(key: str) -> str | None:
        """
        Return snapshot as JSON string regardless of storage type:
        - STRING: returns the string
        - HASH: reads fields and returns json.dumps(dict)
        """
        R = _r()
        try:
            t = R.type(key)
            if isinstance(t, (bytes, bytearray)):
                t = t.decode("utf-8", "ignore")
            t = str(t or "").lower()

            if t == "string":
                s = R.get(key)
                if not s:
                    return None
                if isinstance(s, (bytes, bytearray)):
                    s = s.decode("utf-8", "ignore")
                s = s.strip()

                # If this is a device-id pointer, DO NOT treat as JSON
                if s.startswith("dev_") and not s.startswith("{") and not s.startswith("["):
                    return s  # caller must dereference
                return s

            if t == "hash":
                h = R.hgetall(key) or {}
                if not h:
                    return None

                out: dict[str, object] = {}
                for k, v in h.items():
                    kk = k.decode("utf-8", "ignore") if isinstance(k, (bytes, bytearray)) else str(k)
                    vv = v.decode("utf-8", "ignore") if isinstance(v, (bytes, bytearray)) else v

                    # if vv looks like JSON, decode it; else keep string
                    if isinstance(vv, str) and vv and vv[0] in "[{":
                        try:
                            out[kk] = json.loads(vv)
                        except Exception:
                            out[kk] = vv
                    else:
                        out[kk] = vv

                return json.dumps(out, ensure_ascii=False, separators=(",", ":"))

            return None
        except Exception:
            return None

    
    def _hgetall_json(key: str) -> dict:
        raw = _snap_get_raw_json(key)
        if not raw:
            return {}
        try:
            obj = json.loads(raw) if isinstance(raw, str) else None
            return obj if isinstance(obj, dict) else {}
        except Exception:
            return {}
    # ---------------- config knobs ----------------
    horizon_min_default = int(os.getenv("XTL_OPP_HORIZON_MIN", "60"))

    # ---- trade economics (manual trading safety) ----
    tp_fraction = float(os.getenv("XTL_OPP_TP_FRACTION", "0.35"))             # 35% of forecast
    min_net_edge_pct = float(os.getenv("XTL_OPP_MIN_NET_EDGE_PCT", "0.06"))   # costs+slippage
    min_rrr = float(os.getenv("XTL_OPP_MIN_RRR", "1.3"))

    # ---------------- direction normalize ----------------
    want_dir = (row.get("opp_direction") or row.get("direction") or "").upper()
    if want_dir not in ("UP", "DOWN"):
        dec = str(row.get("decision") or "").upper()
        want_dir = "UP" if dec == "BUY" else "DOWN" if dec == "SELL" else ""

    if want_dir == "UP":
        row["decision"] = "BUY"
    elif want_dir == "DOWN":
        row["decision"] = "SELL"

    # ---------------- horizon window ----------------
    horizon_min = horizon_min_default
    horizon_ms = horizon_min * 60_000

    def _live_px_from_row(r: dict):
        lp = (
            r.get("last_price")
            or r.get("live")          # <-- ADD
            or r.get("live_price")    # <-- ADD
            or r.get("price")
            or r.get("last_close")
            or r.get("lastClose")
            or r.get("mid")
        )
        try:
            return float(lp)
        except Exception:
            return None

    # ---------------- entry state helpers ----------------
    def _is_entered(s: dict) -> bool:
        """
        True once entry has triggered (manual trading).
        We treat this as "LOCKED" => no time expiry + no replacement.
        """
        try:
            if bool(s.get("entry_triggered")):
                return True
            # common fallbacks used across codepaths
            if s.get("entry_ts_ms") and int(s.get("entry_ts_ms") or 0) > 0:
                return True
            
            
            e1 = s.get("entry_1m")
            if isinstance(e1, dict) and (bool(e1.get("triggered")) or bool(e1.get("entry_triggered"))):
                return True
        except Exception:
            pass
        return False

    def _as_float(x, default=None):
        try:
            return float(x)
        except Exception:
            return default

    def _should_replace_pre_entry(existing: dict, incoming: dict) -> bool:
        """
        Replace only when NOT entered.
        Heuristic: prefer higher opp_score; otherwise replace if forecast move improved.
        """
        ex_score = _as_float(existing.get("opp_score"))
        in_score = _as_float(incoming.get("opp_score"))
        if (in_score is not None) and (ex_score is not None):
            # small margin to prevent churn
            if in_score >= (ex_score + 0.5):
                return True

        ex_mv = _as_float(existing.get("forecast_move_pct_1h") or existing.get("expected_move_pct_1h") or existing.get("expected_move_pct"))
        in_mv = _as_float(incoming.get("expected_move_pct_1h") or incoming.get("expected_move_pct"))
        if (in_mv is not None) and (ex_mv is not None):
            if abs(in_mv) >= abs(ex_mv) * 1.10:  # 10% better room
                return True

        # fallback: if incoming has a different alert/opp id, allow replace (pre-entry only)
        ex_id = str(existing.get("alert_id") or existing.get("opp_id") or "").strip()
        in_id = str(incoming.get("alert_id") or incoming.get("opp_id") or "").strip()
        if in_id and ex_id and in_id != ex_id:
            return True

        return False

    # ---------------- EXISTING ACTIVE SNAPSHOT ----------------
    for d in ("UP", "DOWN"):
        snap_key = _opp_snapshot_key(sym_u, d)
        snap = _hgetall_json(snap_key)
        if not snap:
            continue

        st = str(snap.get("status") or "active").lower()
        if st not in ("active", "new", "open"):
            continue

        # inject last_price (best effort)
        lp = _live_px_from_row(row or {})
        if lp is not None:
            snap["last_price"] = float(lp)

        # IMPORTANT: evaluate using decoded snap (not raw hgetall)
        try:
            _evaluate_alert_outcome(sym_u, snap, row or {}, now_ms)
        except Exception:
            pass

        # reload after evaluator
        snap = _hgetall_json(snap_key)
        if not snap:
            continue

        st2 = str(snap.get("status") or "").lower()
        if st2 in ("hit", "expired", "exit"):
            try:
                _delete_live_snapshot(sym_u, d)
            except Exception:
                pass
            continue

        
        # ---------------- entered? lock snapshot (NO TIME EXPIRY + NO REPLACE) ----------------
        entered = _is_entered(snap)

        # If an active snapshot exists in the *other* direction, and we're NOT entered,
        # allow replacement: delete the old opposite snapshot and continue searching/creating.
        if (not entered) and (want_dir in ("UP", "DOWN")) and (d != want_dir):
            try:
                # pre-entry replacement => remove old snapshot so new dir can surface
                R.hset(snap_key, mapping={
                    "status": json.dumps("expired"),
                    "expired_ts": json.dumps(now_ms),
                    "last_status_ms": json.dumps(now_ms),
                })
            except Exception:
                pass
            try:
                _delete_live_snapshot(sym_u, d)
            except Exception:
                pass
            continue

        # If same direction snapshot exists, still allow replacement pre-entry if incoming is better/newer
        if (not entered) and (want_dir in ("UP", "DOWN")) and (d == want_dir):
            try:
                if _should_replace_pre_entry(snap, row or {}):
                    try:
                        R.hset(snap_key, mapping={
                            "status": json.dumps("expired"),
                            "expired_ts": json.dumps(now_ms),
                            "last_status_ms": json.dumps(now_ms),
                        })
                    except Exception:
                        pass
                    try:
                        _delete_live_snapshot(sym_u, d)
                    except Exception:
                        pass
                    # continue loop; after loop ends we'll CREATE NEW SNAPSHOT
                    continue
            except Exception:
                pass

        # -------- time expiry only when NOT entered --------
        if not entered:
            try:
                exp_ts = snap.get("opp_expire_ts")
                exp_ts = int(exp_ts) if isinstance(exp_ts, (int, float)) else 0
            except Exception:
                exp_ts = 0

            if not exp_ts:
                try:
                    created = snap.get("alert_created_ms")
                    created = int(created) if isinstance(created, (int, float)) else 0
                except Exception:
                    created = 0
                if created:
                    exp_ts = created + horizon_ms

            if exp_ts and now_ms >= exp_ts:
                try:
                    aid = str(snap.get("alert_id") or snap.get("opp_id") or "").strip()
                    if aid:
                        key = f"{ALERT_HASH_PREFIX}{aid}"
                        R.hset(key, mapping={
                            "status": json.dumps("expired"),
                            "hit_target": json.dumps(False),
                            "expired_ts": json.dumps(now_ms),
                            "last_status_ms": json.dumps(now_ms),
                        })
                except Exception:
                    pass

                try:
                    R.hset(snap_key, mapping={
                        "status": json.dumps("expired"),
                        "expired_ts": json.dumps(now_ms),
                        "last_status_ms": json.dumps(now_ms),
                    })
                except Exception:
                    pass

                try:
                    _delete_live_snapshot(sym_u, d)
                except Exception:
                    pass
                continue

        # If entered, keep the redis key alive longer (do NOT expire from horizon)
        if entered:
            try:
                R.expire(snap_key, int(os.getenv("XTL_OPP_POST_ENTRY_TTL_SEC", str(7 * 24 * 3600))))
            except Exception:
                pass

        # still active ? return frozen
        out = dict(snap)
        lp2 = _live_px_from_row(row or {})
        if lp2 is not None:
            out["last_price"] = float(lp2)

        out["server_now_ms"] = int(now_ms)
        out["decision"] = "BUY" if d == "UP" else "SELL"
        out["opp_direction"] = d
        

        return out

    # ---------------- CREATE NEW SNAPSHOT ----------------
    if want_dir not in ("UP", "DOWN"):
        row["status"] = "filtered"
        return row

    # ---- basis price (ENTRY) ----
    basis = None
    for k in ("alert_price_1h", "basis_price_1h", "basis_price", "last_price", "price", "last_close", "lastClose", "mid"):
        v = row.get(k)
        if isinstance(v, (int, float)) and v > 0:
            basis = float(v)
            break

    pct_1h = row.get("expected_move_pct_1h") or row.get("expected_move_pct")

    # coerce pct to float if it's a numeric string
    try:
        if isinstance(pct_1h, str):
            pct_1h = float(pct_1h.strip())
    except Exception:
        pass

    # basis must be float
    try:
        if isinstance(basis, str):
            basis = float(basis.strip())
    except Exception:
        pass

    if not isinstance(pct_1h, (int, float)) or basis is None:
        row["status"] = "filtered"
        return row


    # ---- COST-AWARE TP / SL ----
    forecast_pct = abs(float(pct_1h))               # %
    candidate_tp_pct = forecast_pct * tp_fraction
    trade_tp_pct = max(candidate_tp_pct, min_net_edge_pct)
    trade_tp_pct = min(trade_tp_pct, forecast_pct * 0.8)

    if trade_tp_pct < min_net_edge_pct:
        row["status"] = "filtered"
        return row

    dir_sign = +1 if want_dir == "UP" else -1
    target = basis * (1.0 + dir_sign * trade_tp_pct / 100.0)
    sl_pct = trade_tp_pct / min_rrr
    stop_loss = basis * (1.0 - dir_sign * sl_pct / 100.0)

    # ---- SNAPSHOT ----
    # IMPORTANT: horizon is from creation time, NOT hour bucket
    open_ts = int(now_ms)
    opp_id = f"{sym_u}-H1-{want_dir}-{open_ts}"

    snap = dict(row)
    snap.update({
        "status": "active",
        "opp_id": opp_id,
        "alert_id": row.get("alert_id") or opp_id,
        "alert_created_ms": int(now_ms),

        "horizon_min": int(horizon_min),
        "opp_open_ts": int(open_ts),
        "opp_expire_ts": int(open_ts + horizon_ms),

        # ENTRY / TP / SL
        "basis_price_1h": float(basis),
        "alert_price_1h": float(basis),
        "target_price_1h": float(target),
        "stop_loss_1h": float(stop_loss),

        # economics
        "forecast_move_pct_1h": float(pct_1h),
        "trade_tp_pct_1h": float(trade_tp_pct) * (1.0 if want_dir == "UP" else -1.0),
        "rrr": round(abs(trade_tp_pct / sl_pct), 2),

        "opp_direction": want_dir,
        "decision": "BUY" if want_dir == "UP" else "SELL",
        "server_now_ms": int(now_ms),
    })

    snap_key = _opp_snapshot_key(sym_u, want_dir)

    try:
        alert_id = _save_alert_snapshot(sym_u, snap)
        snap["alert_id"] = alert_id
    except Exception:
        pass

    try:
        R.hset(snap_key, mapping={k: json.dumps(v) for k, v in snap.items()})
        R.expire(snap_key, int((horizon_ms / 1000) + 3600))
    except Exception:
        pass

    return snap

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

    def _sfn(x, default=0.0) -> float:
        try:
            if x is None:
                return float(default)
            return float(x)
        except (TypeError, ValueError):
            return float(default)

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
            dist_pct = _sfn((nearest.get("distance_pct") or nearest.get("dist_pct") or 0.0))

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

def _r():
    global R
    try:
        url = os.getenv("REDIS_URL")
        if not url:
            return R
        # Always rebuild if current R doesn't match env host/port/db
        if not R:
            R = redis.from_url(url, decode_responses=True)
            return R
        ck = getattr(getattr(R, "connection_pool", None), "connection_kwargs", {}) or {}
        env_r = redis.from_url(url, decode_responses=True)
        ck2 = getattr(getattr(env_r, "connection_pool", None), "connection_kwargs", {}) or {}
        if (ck.get("host"), ck.get("port"), ck.get("db")) != (ck2.get("host"), ck2.get("port"), ck2.get("db")):
            R = env_r
    except Exception:
        pass
    return R

log.info(f"[TREND]  module={__file__}")
log.info(f"[TREND] REDIS_URL={REDIS_URL}")

# --------------------------
# DISCORD WEBHOOK (alerts)
# --------------------------
# Put this in /etc/xauapi.env (recommended):
#   DISCORD_WEBHOOK_URL="https://discord.com/api/webhooks/...."
DISCORD_WEBHOOK_URL = (os.getenv("DISCORD_WEBHOOK_URL") or "").strip()
DISCORD_MENTION_EVERYONE = (os.getenv("XTL_DISCORD_MENTION_EVERYONE") or "1").strip().lower() in ("1","true","yes","on")

def _discord_dedupe_key(event: str, k: str) -> str:
    kk = (k or "").strip()
    if not kk:
        kk = "na"
    return f"xtl:discord:sent:{event}:{kk}"

def _discord_should_send(event: str, k: str, ttl_sec: int = 24 * 3600) -> bool:
    """
    Return True only once per (event,k) in ttl window.
    Uses Redis SET NX EX to dedupe.
    """
    if not DISCORD_WEBHOOK_URL:
        return False
    try:
        dk = _discord_dedupe_key(event, k)
        ok = R.set(dk, "1", nx=True, ex=int(ttl_sec))
        return bool(ok)
    except Exception:
        # If Redis fails, default to NOT sending (avoid spam)
        return False

def _discord_post(content: str, embeds: list[dict] | None = None) -> None:
    """
    Fire-and-forget Discord webhook post.
    """
    if not DISCORD_WEBHOOK_URL:
        return
    payload = {"content": (content or "").strip()[:1900]}
    if embeds:
        payload["embeds"] = embeds
    try:
        httpx.post(DISCORD_WEBHOOK_URL, json=payload, timeout=5.0)
    except Exception as e:
        log.warning("[DISCORD] post failed err=%r", e)

def _fmt_px(x) -> str:
    try:
        return f"{float(x):,.3f}"
    except Exception:
        return "NA"

def _discord_notify_entry(row: dict) -> None:
    """
    Send ENTRY notification to Discord (with @everyone).
    De-duped via Redis so it fires only once per alert.
    """

    sym = str(row.get("symbol") or "").upper().strip()
    sig = str(row.get("entry_signal") or "").upper().strip()
    if sig not in ("BUY", "SELL"):
        return

    # Stable alert key for dedupe
    alert_key = (
        str(row.get("alert_id") or "").strip()
        or str(row.get("opp_id") or "").strip()
        or f"{sym}:{sig}:{int(row.get('alert_created_ms') or row.get('opp_open_ts') or 0)}"
    )

    # Dedupe for 48 hours
    if not _discord_should_send("entry", alert_key, ttl_sec=48 * 3600):
        return

    entry_px = (
        row.get("entry_price")
        or row.get("last_price")
        or row.get("basis_price")
        or row.get("basis_price_1h")
    )

    tp = row.get("target_price") or row.get("target_price_1h")
    sl = row.get("sl_price")  # may be None for now

    reason = str(
        row.get("entry_reason")
        or row.get("signal_reason")
        or "signal_triggered"
    )

    # Time (UTC, consistent for all users)
    ts_ms = int(row.get("entry_ts_ms") or time.time() * 1000)
    ts_utc = time.strftime("%H:%M UTC", time.gmtime(ts_ms / 1000))

    msg = (
        f"@everyone ?? **ENTRY {sig} - {sym}**\n\n"
        f"?? Time: `{ts_utc}`\n"
        f"?? Timeframe: `{row.get('tf', 'NA')}`\n\n"
        f"Entry: `{_fmt_px(entry_px)}`\n"
        f"Target: `{_fmt_px(tp)}`\n"
        f"Stop Loss: `{_fmt_px(sl) if sl is not None else 'TBD'}`\n\n"
        f"Reason: `{reason}`\n"
        f"Alert ID: `{alert_key}`\n\n"
        f"?? Manual trade - manage risk accordingly."
    )

    _discord_post(msg)

def _discord_notify_outcome(event: str, payload: dict) -> None:
    """
    event: 'hit' | 'expired' | 'sl_hit'
    payload: data from _evaluate_alert_outcome
    """
    sym = str(payload.get("symbol") or "").upper().strip() or "NA"
    direction = str(payload.get("opp_direction") or payload.get("direction") or "").upper().strip()
    status = str(payload.get("status") or event).lower().strip()

    alert_key = (
        str(payload.get("alert_id") or "").strip()
        or str(payload.get("opp_id") or "").strip()
        or f"{sym}:{direction}:{int(payload.get('alert_created_ms') or 0)}"
    )

    # Dedup by final status + alert key
    if not _discord_should_send(status, alert_key, ttl_sec=7 * 24 * 3600):
        return

    last_px = payload.get("last_price")
    rmove = payload.get("realized_move_pct")

    if status == "hit":
        emoji = "??"
        title = "HIT"
    elif status in ("sl_hit", "stop", "stopped", "stop_loss"):
        emoji = "??"
        title = "SL HIT"
    else:
        emoji = "?"
        title = status.upper() if status else "UPDATE"

    entry_sig = str(payload.get("entry_signal") or "").upper().strip()
    entry_px = payload.get("entry_price")
    tp_px = payload.get("tp_price")
    sl_px = payload.get("sl_price")

    extra_lines = []
    if entry_sig in ("BUY", "SELL") and entry_px is not None:
        extra_lines.append(f"Entry: `{entry_sig}` @ `{_fmt_px(entry_px)}`")
    if tp_px is not None:
        extra_lines.append(f"TP: `{_fmt_px(tp_px)}`")
    if sl_px is not None:
        extra_lines.append(f"SL: `{_fmt_px(sl_px)}`")

    extra = ("\n" + "\n".join(extra_lines)) if extra_lines else ""

    move_val = 0.0
    try:
        if isinstance(rmove, (int, float)):
            move_val = float(rmove)
    except Exception:
        move_val = 0.0

    mention = "@everyone " if (DISCORD_MENTION_EVERYONE and status in ("hit","sl_hit")) else ""

    msg = (
        f"{mention}{emoji} **{title}** - **{sym}** ({direction})\n"
        f"Last: `{_fmt_px(last_px)}`\n"
        f"Move: `{move_val:+.2f}%`"
        f"{extra}\n"
        f"Alert: `{alert_key}`"
    )
    _discord_post(msg)


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
        sym_u = (sym or "").upper().strip()
        if not sym_u:
            return

        R.delete(f"opp:snap:{sym_u}")

        if opp_dir:
            d = str(opp_dir).upper().strip()
            if d in ("BUY", "UP"):
                d = "UP"
            elif d in ("SELL", "DOWN"):
                d = "DOWN"
            R.delete(_opp_snapshot_key(sym_u, d))
        else:
            for d in ("UP", "DOWN"):
                R.delete(_opp_snapshot_key(sym_u, d))

    except Exception as e:
        log.warning("[OPP] _delete_live_snapshot failed sym=%s err=%r", sym, e)



# --------------------------
# ALERT HISTORY HELPERS (Redis)
# --------------------------


ALERT_HASH_PREFIX = "xtl:trend:opp:h1:"
ALERT_INDEX_KEY = "xtl:trend:opp:h1:index"




def _save_alert_snapshot(symbol: str, payload: dict[str, Any]) -> str:
    sym = (symbol or payload.get("symbol") or "").upper()
    direction = str(payload.get("opp_direction") or payload.get("direction") or "").upper()
    if direction not in ("UP", "DOWN"):
        direction = "NA"

    # Ensure direction fields exist for downstream logic
    payload.setdefault("symbol", sym)
    payload.setdefault("opp_direction", direction)
    payload.setdefault("direction", direction)

    alert_id = str(payload.get("alert_id") or "").strip()

    if not alert_id:
        ts = int(payload.get("alert_created_ms") or int(time.time() * 1000))
        alert_id = f"{ts}:{sym}:{direction}"
        payload["alert_id"] = alert_id

    if "alert_created_ms" not in payload:
        payload["alert_created_ms"] = int(time.time() * 1000)

    if "status" not in payload:
        payload["status"] = "active"

    key = f"{ALERT_HASH_PREFIX}{alert_id}"

    try:
        # ---------- NEW: protect frozen entry metadata ----------
        if payload.get("entry_triggered"):
            existing = R.hgetall(key) or {}

            def _get_existing(k):
                v = existing.get(k.encode() if isinstance(k, str) else k)
                if v is None:
                    return None
                try:
                    return json.loads(v)
                except Exception:
                    return None

            # Do NOT overwrite once set
            payload["entry_triggered"] = True
            payload["entry_signal"] = (
                payload.get("entry_signal")
                or _get_existing("entry_signal")
            )
            payload["entry_reason"] = (
                payload.get("entry_reason")
                or _get_existing("entry_reason")
            )
            payload["entry_ts_ms"] = (
                payload.get("entry_ts_ms")
                or _get_existing("entry_ts_ms")
            )
            payload["entry_price"] = (
                payload.get("entry_price")
                or _get_existing("entry_price")
            )
            payload["tp_price"] = (
                payload.get("tp_price")
                or _get_existing("tp_price")
            )
            payload["sl_price"] = (
                payload.get("sl_price")
                or _get_existing("sl_price")
            )
            payload["discord_entry_sent"] = (
                payload.get("discord_entry_sent")
                or _get_existing("discord_entry_sent")
                or False
            )

        # ---------- END NEW ----------

        mapping = {k: json.dumps(v) for k, v in payload.items()}
        R.hset(key, mapping=mapping)
        

        # add:
        try:
            # keep alerts for 120h by default (or tie to horizon if you want)
            ttl = int(payload.get("oppt_ttl_sec") or 5 * 24 * 3600)
            if R.ttl(key) < 0:      # only set once
                R.expire(key, ttl)

        except Exception:
            pass

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
        decoded["alertTimeMs"] = decoded["alert_time_ms"]

        # expected_move_pct (distance only; legacy fields allowed)
        try:
            decoded["expected_move_pct"] = float(
                decoded.get("trade_tp_pct_1h")
                or decoded.get("expected_move_pct")
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
            elif decoded["status"] in ("expired", "sl_hit"):
                decoded["hit_target"] = False
            else:
                decoded["hit_target"] = None
        decoded["hitTarget"] = decoded.get("hit_target")


        # only completed alerts in history
        if decoded["status"] not in ("hit", "expired","sl_hit"):
            continue
        decoded["status"] = str(decoded.get("status") or "").lower()

        # defaults
        decoded.setdefault("realized_move_pct", None)
        decoded.setdefault("max_drawdown_pct", None)
        decoded.setdefault("expired_ts", None)
        decoded.setdefault("hit_ts", None)
        # ---------- NEW: normalize entry + outcome fields for UI ----------
        # entry meta (frozen when entry triggers)
        decoded.setdefault("entry_triggered", False)
        decoded.setdefault("entry_signal", None)
        decoded.setdefault("entry_reason", None)
        decoded.setdefault("entry_ts_ms", decoded.get("entry_ts_ms") or decoded.get("entry_ts"))
        decoded.setdefault("entry_price", decoded.get("entry_price"))

        # outcome timestamps (ms aliases)
        decoded.setdefault("hit_ts_ms", decoded.get("hit_ts_ms") or decoded.get("hit_ts"))
        decoded.setdefault("sl_hit_ts_ms", decoded.get("sl_hit_ts_ms") or decoded.get("sl_hit_ts"))
        decoded.setdefault("expired_ts_ms", decoded.get("expired_ts_ms") or decoded.get("expired_ts"))
        decoded.setdefault("updated_ms", decoded.get("updated_ms") or decoded.get("last_status_ms"))
        # ---------- END NEW ----------


        out.append(decoded)

    out.sort(key=lambda d: d.get("alert_created_ms") or 0, reverse=True)
    return out


# --------------------------
# ALERT STATUS UPDATE HELPERS
# --------------------------

def _mark_alert_hit(alert_id: str, realized_move_pct: float, now_ms: int):
    """Mark a stored alert as hit."""
    key = f"{ALERT_HASH_PREFIX}{alert_id}"
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
    key = f"{ALERT_HASH_PREFIX}{alert_id}"
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


#router = APIRouter()

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



@router.get("/pulse")
def trend_pulse(
    symbol: str = Query(...),
    tf: str = "M15",
    device: str | None = Query(None),
    x_device_id: str | None = Header(None, convert_underscores=False),
    user=Depends(require_auth_optional),
):
    """
    Rich per-symbol “Pulse” payload:
      - SR (H1/H4) summary
      - Fib levels (derived from H1 range, fallback H4)
      - short deterministic pulse_text

    This endpoint is separate from /predict/all by design.
    """
    sym_u = (symbol or "").upper().strip()
    if not sym_u:
        raise HTTPException(status_code=400, detail="symbol required")

    tfu = (tf or "M15").upper().strip()

    # ---- 1) reuse your forecast path for prob/decision/target (minimal) ----
    # If you already have a helper that builds the single “row” like predict_all does,
    # call it here. Otherwise: read last row from redis (fast) and use it as forecast snapshot.
    # (You already write lastrow in predict_all) :contentReference[oaicite:5]{index=5}
    row = None
    try:
        raw = _redis_get_text(f"xtl:pred:lastrow:{sym_u}")
        row = json.loads(raw) if raw else None
    except Exception:
        row = None

    # fallback-safe fields
    decision = str((row or {}).get("decision") or "ABSTAIN").upper()
    prob_up = (row or {}).get("prob_up")
    expected_move_pct = (row or {}).get("expected_move_pct")
    target_price = (row or {}).get("target_price")

    # live-ish price (prefer existing value from row; else redis price)
    # live-ish price (prefer lastrow fields; else redis live price)
    price = None
    try:
        for k in ("last_price", "price", "mid", "close"):
            v = (row or {}).get(k)
            if isinstance(v, (int, float)):
                price = float(v)
                break
    except Exception:
        price = None

    pinned_device = (
        (device or "").strip()
        or (x_device_id or "").strip()
        or (getattr(user, "device_id", None) or "")
        or (getattr(user, "deviceId", None) or "")
    )
    # --- NEW: auto-select active device when none is pinned (fixes price=null in /pulse) ---
    if not pinned_device and R is not None:
        try:
            best_dev = None
            best_hb = -1

            for key in R.scan_iter("device:dev_*"):
                try:
                    h = R.hgetall(key) or {}
                except Exception:
                    h = {}
                if not h:
                    continue

                status = h.get(b"status") or h.get("status")
                if isinstance(status, (bytes, bytearray)):
                    status = status.decode("utf-8", "ignore")
                if (status or "").strip().lower() != "online":
                    continue

                hb = h.get(b"last_heartbeat_ms") or h.get("last_heartbeat_ms")
                if isinstance(hb, (bytes, bytearray)):
                    hb = hb.decode("utf-8", "ignore").strip()
                try:
                    hb_i = int(hb) if hb not in (None, "") else -1
                except Exception:
                    hb_i = -1

                if isinstance(key, (bytes, bytearray)):
                    key_s = key.decode("utf-8", "ignore")
                else:
                    key_s = str(key)
                dev_id = key_s.replace("device:", "").strip()

                if hb_i > best_hb:
                    best_hb = hb_i
                    best_dev = dev_id

            if best_dev:
                pinned_device = best_dev
        except Exception:
            pass
    if price is None:
        try:
            # IMPORTANT: use the device-scoped key
            px, _ts = _get_live_price(sym_u, pinned_device)
            if isinstance(px, (int, float)):
                price = float(px)
        except Exception:
            price = None


    # ---- 2) fetch H1/H4 bars for SR + Fib ----
    # Use your existing pull_latest_h1/pull_latest_h4 (already imported) :contentReference[oaicite:6]{index=6}
    h1_df = None
    h4_df = None
    try:
        h1_df = pull_latest_h1(sym_u)  # should return df-like
    except Exception:
        h1_df = None
    try:
        h4_df = pull_latest_h4(sym_u)
    except Exception:
        h4_df = None

    h1_df = _to_hlc(h1_df)
    h4_df = _to_hlc(h4_df)
    try:
        if h1_df is not None and not h1_df.empty:
            h1_df = h1_df.dropna(subset=["h", "l", "c"])
        if h4_df is not None and not h4_df.empty:
            h4_df = h4_df.dropna(subset=["h", "l", "c"])
    except Exception:
        pass
    

    # pip_factor for SR distance (keep it simple)
    pip_factor = 0.01 if sym_u == "XAUUSD" else (0.01 if sym_u.endswith("JPY") else 0.0001)

    sr_summary = summarize_sr_multi_tf(
        symbol=sym_u,
        price=(lambda v: (float(v) if v is not None and str(v).strip() != "" else None))(price),
        h4_df=h4_df,
        h1_df=h1_df,
        pip_factor=float(pip_factor),
        cache=R,
        cache_ttl_sec=0, 
        good_ttl_sec=7*24*3600,
    )
    # ----------------------------------------------------------
    # SR zones for UI overlays (derived from sr_summary bundle)
    # sr_summary shape: {"h4": {...supports_major/resistances_major...}, "h1": {...}}
    # Also: summarize_sr_multi_tf() already uses Redis last_good bundle key:
    #   xtl:sr:bundle:last_good:{SYMBOL}
    # ----------------------------------------------------------

    


    # NOTE: do NOT use your custom "xtl:sr:lastgood:{sym}:H1H4" key.
    # summarize_sr_multi_tf() already caches & falls back using:
    #   xtl:sr:bundle:last_good:{SYMBOL}
    sr_zones = _build_sr_zones_from_summary(
        sr_summary,
        sym=sym_u,
        pip_factor=float(pip_factor),
        atr=None,
    )
    
    
    # ---- 3) delegate “pulse composition” to api/pulse.py ----
    pulse = build_pulse(
         symbol=sym_u,
         tf=tfu,
         price=price,
         decision=decision,
         prob_up=prob_up if isinstance(prob_up, (int, float)) else None,
         expected_move_pct=expected_move_pct if isinstance(expected_move_pct, (int, float)) else None,
         target_price=target_price if isinstance(target_price, (int, float)) else None,
         sr_summary=sr_summary,
         h1_df=h1_df,
         h4_df=h4_df,
    )

    
    # Attach SR + zones so UI can draw shaded blocks
    try:
        if isinstance(pulse, dict):
            chart = pulse.setdefault("chart", {})
            overlays = chart.setdefault("overlays", {})

            overlays["sr_zones"] = sr_zones if isinstance(sr_zones, list) else []
            pulse["sr"] = sr_summary if isinstance(sr_summary, dict) else {}
    except Exception:
        pass

    return pulse

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

_META_PATH = os.path.join(os.path.dirname(__file__), "configs", "symbol_meta.json")
_META_PATH = os.path.abspath(_META_PATH)


# Lock short-term (H1) forecast per symbol+horizon so it does not flip every refresh
_ST_H1_LOCK: dict[str, dict[str, Any]] = {}

# Lock higher-timeframe (H4) forecast per symbol+horizon so it does not flip every refresh
_HT_H4_LOCK: dict[str, dict[str, Any]] = {}


class _MetaCache:
    data: dict[str, dict] = {}
    mtime: float = 0.0
    raw: dict = {}
    common: dict = {}

    @classmethod
    def load(cls, force: bool = False):
        try:
            mt = os.path.getmtime(_META_PATH)
        except OSError:
            return
        if not force and mt <= cls.mtime:
            return

        try:
            with open(_META_PATH, "r", encoding="utf-8") as f:
                raw = json.load(f)
        except Exception as e:
            # keep old cache if JSON is broken
            try:
                log.warning("[META] failed to load %s: %s", _META_PATH, e)
            except Exception:
                pass
            return

        cls.raw = raw if isinstance(raw, dict) else {}
        cls.common = cls.raw.get("common", {}) if isinstance(cls.raw.get("common"), dict) else {}

        out: dict[str, dict] = {}

        if isinstance(raw, dict):
            # NEW SHAPE: {"common": {...}, "symbols": {...}}
            if isinstance(raw.get("symbols"), dict):
                syms = raw.get("symbols") or {}
                for k, v in syms.items():
                    if not isinstance(v, dict):
                        continue
                    d = dict(v)
                    d.setdefault("symbol", k)
                    out[str(k).upper()] = d
            else:
                # OLD SHAPE: {"XAUUSD": {...}, "EURUSD": {...}}
                for k, v in raw.items():
                    if not isinstance(v, dict):
                        continue
                    d = dict(v)
                    d.setdefault("symbol", k)
                    out[str(k).upper()] = d

        elif isinstance(raw, list):
            for it in raw:
                if not isinstance(it, dict):
                    continue
                sym = str(it.get("symbol", "")).upper()
                if sym:
                    out[sym] = dict(it)

        cls.data = out
        try:
            ex = cls.data.get("XAUUSD") or {}
            log.warning("[META_LOAD] path=%s keys_common=%s", _META_PATH, sorted(list((cls.common or {}).keys()))[:50])
            log.warning("[META_LOAD] sym=XAUUSD keys=%s", sorted(list(ex.keys()))[:80])
            if isinstance(ex.get("oppt_min_move_pct"), dict):
                log.warning("[META_LOAD] sym=XAUUSD oppt_min_move_pct=%s", ex.get("oppt_min_move_pct"))
            if isinstance(cls.common.get("oppt_tf"), dict):
                log.warning("[META_LOAD] common.oppt_tf keys=%s", sorted(list(cls.common.get("oppt_tf").keys()))[:20])
        except Exception:
            pass

        cls.mtime = mt


def _get_meta(sym: str) -> dict:
    # Always check mtime and reload if file changed (cheap)
    _MetaCache.load(force=False)

    # If cache is still empty (first boot / load failed), force a load once
    if not _MetaCache.data:
        _MetaCache.load(force=True)

    s = (sym or "").upper().strip()
    m = dict(_MetaCache.data.get(s) or {})

    # merge common defaults (non-destructive)
    try:
        if isinstance(_MetaCache.common, dict):
            for k, v in _MetaCache.common.items():
                m.setdefault(k, v)
    except Exception:
        pass

    if m:
        m.setdefault("symbol", s)
        return m

    return {
        "symbol": s,
        "tau": 0.55,
        "abstain_band": 0.02,
        "p_hi": 0.7,
        "spread_bp": 3.0,
        "min_rvol": 0.8,
        "target_atr": {"mult": 0.8, "floor_pips": 0.0},
        "reasons": {"DXY": -1, "UST10Y": -1, "USD_SHORT_RATE": -1, "RVOL": 1, "VIX": -1},
    }

def _oppt_min_move_pct(sym: str, tf: str) -> float:
    m = _get_meta(sym) or {}
    tfu = (tf or "").upper()

    # ---------------------------------------------------------
    # NEW: normalize meta root (support both shapes)
    # - some configs are stored under m["common"]
    # - others are stored top-level
    # ---------------------------------------------------------
    root = m
    try:
        if isinstance(m.get("common"), dict):
            root = m["common"]
    except Exception:
        root = m

    # 1) Preferred: explicit thresholds in meta
    #    oppt_min_move_pct: { "H1": 0.30, "H4": 0.60 }
    for src in (m, root):
        try:
            d = src.get("oppt_min_move_pct")
            if isinstance(d, dict):
                v = d.get(tfu)
                if isinstance(v, (int, float)) and v > 0:
                    return float(v)
        except Exception:
            pass

    # 2) Alternate: per-TF config bucket
    #    oppt_tf: { "H1": {"min_move_pct": 0.30}, "H4": {"min_move_pct": 0.60} }
    for src in (m, root):
        try:
            ot = src.get("oppt_tf")
            if isinstance(ot, dict):
                cfg = ot.get(tfu)
                if isinstance(cfg, dict):
                    v = cfg.get("min_move_pct") or cfg.get("min_move") or cfg.get("thr_pct")
                    if isinstance(v, (int, float)) and v > 0:
                        return float(v)
        except Exception:
            pass

    # 3) Fallback: tau-based heuristic
    try:
        tau = float((root.get("tau") if isinstance(root, dict) else None) or m.get("tau") or 0.55)
    except Exception:
        tau = 0.55

    cfg = _oppt_cfg(sym, tfu)
    try:
        frac = float(cfg.get("min_move_frac_tau", 0.60))
    except Exception:
        frac = 0.60

    thr = max(0.0, frac * tau)

    # NEW: safety clamp for metals so you don't get crazy 0.45+ accidentally
    # (tweak these later, but this fixes your immediate “no opps” problem)
    if sym.upper() == "XAUUSD" and tfu == "H1":
        thr = min(thr, 0.30)

    return thr


def _oppt_min_prob(sym: str, tf: str) -> float:
    cfg = _oppt_cfg(sym, tf)
    return float(cfg.get("min_prob", 0.52))


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

def _build_sr_zones_from_summary(
    sr_summary: dict | None,
    *,
    sym: str,
    pip_factor: float,
    atr: float | None = None,
) -> list[dict]:
    """Build drawable SR zones (low/high bands) from sr_summary.

    Accepts multiple schema variants:
      - sr_summary["h1"]/["h4"] or sr_summary["H1"]/["H4"]
      - sr_summary["by_tf"]["H1"]/["H4"]
      - level lists under supports_major/supports/support_levels, resistances_major/...
    """
    if not isinstance(sr_summary, dict):
        return []
    
    # If caller already provided drawable zones (e.g., fallback pivots), use them directly.
    # Expected shape: list[dict] with at least low/high (and ideally tf/kind/level).
    z0 = sr_summary.get("sr_zones") or sr_summary.get("zones")
    if isinstance(z0, list):
        z_ok = [z for z in z0 if isinstance(z, dict) and z.get("low") is not None and z.get("high") is not None]
        if z_ok:
            return z_ok

    def _pick_tf(sr: dict, key: str) -> dict:
        for k in (key, key.upper(), key.lower()):
            v = sr.get(k)
            if isinstance(v, dict):
                return v
        by_tf = sr.get("by_tf")
        if isinstance(by_tf, dict):
            for k in (key, key.upper(), key.lower()):
                v = by_tf.get(k)
                if isinstance(v, dict):
                    return v
        return {}

    def _to_float(x):
        try:
            return float(x)
        except Exception:
            return None

    def _levels(d: dict, side: str) -> list[dict]:
        if not isinstance(d, dict):
            return []
        if side == "support":
            return d.get("supports_major") or d.get("supports") or d.get("support_levels") or []
        return d.get("resistances_major") or d.get("resistances") or d.get("resistance_levels") or []

    # half-width logic: keep visible for XAU + FX
    def _half_width() -> float:
        try:
            if sym == "XAUUSD":
                min_half = float(os.getenv("XTL_ZONE_MIN_PX_XAU", "0.8"))
            else:
                min_half = float(os.getenv("XTL_ZONE_MIN_PX_FX", "0.0008"))
        except Exception:
            min_half = 0.8 if sym == "XAUUSD" else 0.0008

        half = min_half
        if isinstance(atr, (int, float)):
            try:
                half = max(min_half, float(atr) * float(ZONE_ATR_WIDTH))
            except Exception:
                pass

        # If not XAU, ensure at least a few pips so it is visible
        if sym != "XAUUSD":
            try:
                half = max(half, 3.0 * float(pip_factor))
            except Exception:
                pass
        return float(half)

    half = _half_width()

    zones: list[dict] = []
    for tf_label, tf_key in (("H4", "h4"), ("H1", "h1")):
        d = _pick_tf(sr_summary, tf_label) or _pick_tf(sr_summary, tf_key)
        for side in ("support", "resistance"):
            rows = _levels(d, side)
            for r in (rows or []):
                lvl = _to_float((r or {}).get("level"))
                if lvl is None:
                    continue
                kind = (r or {}).get("kind") or (r or {}).get("side") or side
                zones.append(
                    {
                        "tf": tf_label,
                        "kind": str(kind).lower(),
                        "low": float(lvl - half),
                        "high": float(lvl + half),
                        "level": float(lvl),
                        "strength": (r or {}).get("strength"),
                        "touches": (r or {}).get("touches"),
                        "zone_tap_count": (r or {}).get("touches"),
                    }
                )

    # Mark strong zones when H1 and H4 levels overlap (same kind within tolerance).
    try:
        atr = _to_float((sr_summary or {}).get("atr")) or 0.0
        pip_factor = _to_float((sr_summary or {}).get("pip_factor")) or 0.0
        overlap_tol = max(0.20 * float(atr or 0.0), 5.0 * float(pip_factor or 0.0), float(half) * 2.0)
        # Pre-index by kind + tf
        by_kind_tf: dict[tuple[str, str], list[dict]] = {}
        for z in zones:
            k = (str(z.get("kind") or "").lower(), str(z.get("tf") or "").upper())
            by_kind_tf.setdefault(k, []).append(z)

        for kind in ("support", "resistance"):
            h1 = by_kind_tf.get((kind, "H1"), [])
            h4 = by_kind_tf.get((kind, "H4"), [])
            for a in h1:
                la = _to_float(a.get("level"))
                if la is None:
                    continue
                for b in h4:
                    lb = _to_float(b.get("level"))
                    if lb is None:
                        continue
                    if abs(float(la) - float(lb)) <= overlap_tol:
                        a["strong_zone"] = True
                        b["strong_zone"] = True
        # default flag
        for z in zones:
            z.setdefault("strong_zone", False)
    except Exception:
        for z in zones:
            z.setdefault("strong_zone", False)

    return zones
 
def _to_hlc(df):
    if df is None or getattr(df, "empty", True):
        return df

    # normalize column names (supports: high/low/close/open/time OR H/L/C/O/T or mixed)
    cols = {str(c).lower(): c for c in df.columns}
    ren = {}

    # map to short names used by SR/Fib logic
    if "high" in cols:  ren[cols["high"]] = "h"
    if "h" in cols:     ren[cols["h"]] = "h"

    if "low" in cols:   ren[cols["low"]] = "l"
    if "l" in cols:     ren[cols["l"]] = "l"

    if "close" in cols: ren[cols["close"]] = "c"
    if "c" in cols:     ren[cols["c"]] = "c"

    if "open" in cols:  ren[cols["open"]] = "o"
    if "o" in cols:     ren[cols["o"]] = "o"

    if "time" in cols:  ren[cols["time"]] = "t"
    if "t" in cols:     ren[cols["t"]] = "t"

    df = df.rename(columns=ren, errors="ignore")
    return df


TF_SEC_MAP = {"M1": 60, "M5": 300, "M15": 900, "H1": 3600, "H4": 14400}

def _pick_last_closed_bar(snap: dict, tf: str, now_ms: int) -> dict | None:
    try:
        snap = snap or {}
        bars = snap.get("bars") or []
        tf_ms = int(TF_SEC_MAP.get(str(tf or "").upper(), 60) * 1000)

        # prefer snap server time (matches device candle stream)
        use_now_ms = int(snap.get("serverNow") or snap.get("server_now_ms") or now_ms or 0)

        c, _p = _pick_last_closed_bar_from_bars(bars, use_now_ms, tf_ms)
        return c
    except Exception:
        return None
def _read_freshest_snap_any_device(sym_u: str, tf: str):
    """
    Try to find freshest device-scoped snap:
      xtl:ohlc:snap:{dev}:{sym}:{tf}

    If none exist (or none valid), fallback to:
      xtl:ohlc:latest:{sym}:{tf}

    NOTE: latest may be JSON OR a device-id pointer (e.g. "dev_...").
    If pointer, dereference to device-scoped snap key.

    Returns (snap_dict, dev) or (None, None)
    """
    try:
        R = _r()
        sym_u = str(sym_u or "").upper().strip()
        tf_u = str(tf or "").upper().strip()

        # 1) legacy scan
        pattern = f"xtl:ohlc:snap:*:{sym_u}:{tf_u}"
        best_dev = None
        best_snap = None
        best_ms = -1

        for k in R.scan_iter(match=pattern, count=200):
            key = k.decode("utf-8", "ignore") if isinstance(k, (bytes, bytearray)) else str(k)
            parts = key.split(":")
            if len(parts) < 6:
                continue
            dev = parts[3]

            snap, _ = _read_snap_for_device(dev, sym_u, tf_u)
            if not isinstance(snap, dict):
                continue

            ms = snap.get("updated_ms") or snap.get("ts_ms") or 0
            try:
                ms = int(float(ms))
            except Exception:
                ms = 0

            bars = snap.get("bars") or snap.get("ohlc")
            if not (isinstance(bars, list) and len(bars) >= 2):
                continue

            if ms > best_ms:
                best_ms = ms
                best_dev = dev
                best_snap = snap

        if best_snap:
            return best_snap, best_dev

        # 2) fallback: global latest key (may be JSON OR a device pointer)
        key2 = f"xtl:ohlc:latest:{sym_u}:{tf_u}"
        raw = _snap_get_raw_json(key2)
        if not raw:
            return (None, None)

        # If latest is a device-id pointer, dereference it
        if isinstance(raw, str):
            s = raw.strip()
            if s.startswith("dev_") and (not s.startswith("{")) and (not s.startswith("[")):
                key3 = _snap_key(s, sym_u, tf_u)
                raw2 = _snap_get_raw_json(key3)
                if raw2:
                    raw = raw2  # now raw should be JSON snapshot content

        # raw may already be dict (because _snap_get_raw_json may decode hashes)
        snap2 = raw if isinstance(raw, dict) else None
        if snap2 is None:
            try:
                snap2 = json.loads(raw) if isinstance(raw, str) else None
            except Exception:
                snap2 = None

        if not isinstance(snap2, dict):
            return (None, None)

        bars2 = snap2.get("bars") or snap2.get("ohlc")
        if not (isinstance(bars2, list) and len(bars2) >= 2):
            return (None, None)

        return (snap2, None)
    except Exception:
        return (None, None)


# read a specific device snapshot for symbol/tf
def _read_snap_for_device(device_id: str, symbol: str, tf: str, *, header_device: str | None = None):
    try:
        R = _r()
        sym_u = str(symbol or "").upper().strip()
        tf_u = str(tf or "").upper().strip()

        # Prefer header device if provided, then fallback to passed device_id
        hdr = str(header_device or "").strip()
        dev0 = str(device_id or "").strip()

        # -------------------------------
        # 1) device-scoped snap key (try header first, then device_id)
        # -------------------------------
        raw = None
        key = None
        used_dev = None

        for dev_try in (hdr, dev0):
            if not dev_try:
                continue
            k = _snap_key(dev_try, sym_u, tf_u)
            r = _snap_get_raw_json(k)  # works for STRING and HASH
            if r:
                raw = r
                key = k
                used_dev = dev_try
                break

        # -------------------------------
        # 2) fallback: global latest key (may be JSON OR a device pointer)
        # -------------------------------
        if not raw and sym_u and tf_u:
            key2 = f"xtl:ohlc:latest:{sym_u}:{tf_u}"
            raw = _snap_get_raw_json(key2)

            # If latest is a device-id pointer, dereference it
            if isinstance(raw, str):
                s = raw.strip()
                if s.startswith("dev_") and (not s.startswith("{")) and (not s.startswith("[")):
                    key3 = _snap_key(s, sym_u, tf_u)
                    raw2 = _snap_get_raw_json(key3)
                    if raw2:
                        raw = raw2
                        key = key3
                        used_dev = s

        if not raw:
            return None, None

        # raw may already be dict (because _snap_get_raw_json may decode hashes)
        snap = raw if isinstance(raw, dict) else None
        if snap is None:
            try:
                snap = json.loads(raw) if isinstance(raw, str) else None
            except Exception:
                snap = None

        if not isinstance(snap, dict):
            return None, None

        # optional: broker meta from device hash if you keep it there
        b = None
        try:
            dev_meta = used_dev or dev0 or hdr
            if dev_meta:
                h = R.hgetall(f"device:{dev_meta}")
                if h:
                    b = {
                        (k.decode() if isinstance(k, (bytes, bytearray)) else str(k)):
                        (v.decode() if isinstance(v, (bytes, bytearray)) else str(v))
                        for k, v in h.items()
                    }
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
def _unlock_inflight() -> None:
    try:
        if inflight_lock_key and inflight_got_lock:
            R.delete(inflight_lock_key)
    except Exception:
        pass

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

def _get_live_price_with_ts(sym: str, device: str | None) -> tuple[float | None, int | None, str | None]:
    """
    Read live-ish price from Redis.

    Accepts either:
      - old format: "4318.12"
      - new format: {"price": 4318.12, "ts_ms": 1767..., "src": "tick" | "ohlc_m1_close" | ...}

    Keys tried:
      xtl:price:<dev>:<SYMBOL>
      xtl:price:<SYMBOL>
    """
    try:
        sym_u = (sym or "").upper().strip()
        if not sym_u:
            return (None, None, None)

        dev = (device or "").strip()
        if dev.lower() == "auto":
            dev = ""

        keys: list[str] = []
        if dev:
            dev_key = dev if dev.startswith("dev_") else f"dev_{dev}"
            keys.append(f"xtl:price:{dev_key}:{sym_u}")
        keys.append(f"xtl:price:{sym_u}")
        # --- NEW: if no device pinned, try freshest device-scoped price for this symbol ---
        if not dev:
            try:
                best_key = None
                best_ts = -1

                for kk in R.scan_iter(f"xtl:price:dev_*:{sym_u}", count=50):
                    try:
                        raw_k = R.get(kk)
                    except Exception:
                        raw_k = None
                    if not raw_k:
                        continue

                    if isinstance(raw_k, (bytes, bytearray)):
                        raw_k = raw_k.decode("utf-8", "ignore")
                    ss = str(raw_k).strip()
                    if not ss.startswith("{"):
                        continue

                    try:
                        d = json.loads(ss)
                        ts = int(d.get("ts_ms") or 0)
                        if ts > best_ts:
                            best_ts = ts
                            best_key = kk
                    except Exception:
                        continue

                if best_key is not None:
                    if isinstance(best_key, (bytes, bytearray)):
                        best_key = best_key.decode("utf-8", "ignore")
                    keys.insert(0, str(best_key))
            except Exception:
                pass


        for k in keys:
            try:
                raw = R.get(k)
            except Exception:
                raw = None
            if not raw:
                continue

            if isinstance(raw, (bytes, bytearray)):
                raw = raw.decode("utf-8", "ignore")

            s = str(raw).strip()
            if not s:
                continue

          

            # 1) JSON format
            if s.startswith("{"):
                try:
                    d = json.loads(s)
                    if isinstance(d, dict) and d.get("price") is not None:
                        px = float(d.get("price"))
                        ts = d.get("ts_ms") or d.get("ts") or None
                        try:
                            ts_ms = int(ts) if ts is not None else None
                        except Exception:
                            ts_ms = None

                        src = d.get("src")
                        if src is not None:
                            try:
                                src = str(src)
                            except Exception:
                                src = None

                        return (px, ts_ms, src)
                except Exception:
                    pass

            # 2) plain float format (no timestamp/source)
            try:
                return (float(s), None, None)
            except Exception:
                continue

    except Exception:
        pass

    return (None, None, None)

# Optional backward-compatible wrapper (ONLY if other code still calls _get_live_price)
def _get_live_price(sym: str, device: str | None) -> tuple[float | None, int | None]:
    px, ts, _src = _get_live_price_with_ts(sym, device)
    return (px, ts)



def _get_device_tz_offset_min(dev: str | None) -> int | None:
    import os
    try:
        if not dev:
            return None

        d = dev if str(dev).startswith("dev_") else f"dev_{dev}"

        def _to_str(x):
            if x is None:
                return ""
            if isinstance(x, (bytes, bytearray)):
                return x.decode("utf-8", "ignore")
            return str(x)

        # 1) device hash (matches routes_devices.py fallback auth)
        prefix = os.getenv("XTL_DEVICE_KEY_PREFIX", "device:")
        meta = R.hgetall(f"{prefix}{d}") or {}
        if meta:
            # normalize to str->str
            m = {_to_str(k): _to_str(v) for k, v in meta.items()}

            for key in (
                "tz_offset_min",
                "tzOffsetMin",
                "broker_tz_offset_min",
                "Broker.TzOffsetMin",
                "mt5_broker_tz_offset_min",
            ):
                v = m.get(key)
                if v:
                    try:
                        return int(float(v))
                    except Exception:
                        pass

        # 2) explicit string key fallback
        for k in (
            f"xtl:device:{d}:tz_offset_min",
            f"xtl:device:{d}:Broker.TzOffsetMin",
        ):
            v2 = R.get(k)
            if v2:
                try:
                    return int(float(_to_str(v2).strip()))
                except Exception:
                    pass

    except Exception:
        pass
    return None


@router.get("/price/all")
def price_all(
    tf: str = "M1",
    symbols: str = "XAUUSD,EURUSD,USDJPY,GBPUSD,USDCAD,USDCHF",
    device: str | None = Query(None),
    x_device_id: str | None = Header(None, convert_underscores=False),
    user=Depends(require_auth_optional),  # optional auth; prefer user's device when not pinned
):
    tfu = (tf or "M1").upper()  # display price is from M1; keep param for future
    syms = [s.strip().upper() for s in (symbols or "").split(",") if s.strip()]
    rows: list[dict] = []
    broker = None

    import time
    now_ms = int(time.time() * 1000)

    user_id = _uid_from_user(user)

    pinned_device = device or x_device_id or getattr(user, "device_id", None) or getattr(user, "deviceId", None)

    # AUTO-SELECT active device when none is pinned
    if not pinned_device and R is not None:
        try:
            best_dev = None
            best_hb = -1

            for key in R.scan_iter("device:dev_*"):
                try:
                    h = R.hgetall(key) or {}
                except Exception:
                    h = {}
                if not h:
                    continue

                # status (bytes -> str)
                status = h.get(b"status") or h.get("status")
                if isinstance(status, (bytes, bytearray)):
                    status = status.decode("utf-8", "ignore")
                status_s = (status or "").strip().lower()
                if status_s != "online":
                    continue

                # Prefer devices that actually have broker tz fields
                tz_v = h.get(b"broker_tz_offset_min") or h.get("broker_tz_offset_min") or h.get(b"Broker.TzOffsetMin") or h.get("Broker.TzOffsetMin")
                has_tz = tz_v is not None and tz_v != b"" and tz_v != ""

                # last_heartbeat_ms (bytes -> str -> int)
                hb = h.get(b"last_heartbeat_ms") or h.get("last_heartbeat_ms")
                if isinstance(hb, (bytes, bytearray)):
                    hb = hb.decode("utf-8", "ignore").strip()
                try:
                    hb_i = int(hb) if hb is not None and hb != "" else -1
                except Exception:
                    hb_i = -1

                # decode key -> device id
                if isinstance(key, (bytes, bytearray)):
                    key_s = key.decode("utf-8", "ignore")
                else:
                    key_s = str(key)
                dev_id = key_s.replace("device:", "").strip()

                # ranking: prefer higher heartbeat; if tie, prefer has_tz
                if hb_i > best_hb:
                    best_hb = hb_i
                    best_dev = dev_id
                elif hb_i == best_hb and best_dev and has_tz:
                    # if current best doesn't have tz but this one does, prefer it
                    # (safe “tie-breaker”)
                    best_dev = dev_id

            if best_dev:
                pinned_device = best_dev
        except Exception:
            pass

    device_used = pinned_device or "auto"
    tz_off_min = _get_device_tz_offset_min(pinned_device)

    for sym_u in syms:
        px, ts_ms, src = _get_live_price_with_ts(sym_u, pinned_device)
        if px is None or ts_ms is None:
            rows.append({"symbol": sym_u, "price": None, "lastTs": None, "price_source": "none"})
            continue
        age_ms = now_ms - int(ts_ms)
        src_s = (src or "").strip().lower()

        # Honest labeling + freshness:
        # - tick is only "tick" if it truly came from tick and is <= 10s old
        # - otherwise label as whatever OHLC wrote (e.g. ohlc_m1_close)
        if src_s == "tick" and age_ms <= 10_000:
            price_source = "tick"
        else:
            price_source = src or "ohlc"

        rows.append(
            {
                "symbol": sym_u,
                "price": _fmt_price(sym_u, px, broker),
                "lastTs": int(ts_ms),
                "price_source": price_source,
            }
        )

    # Keep response contract stable
    out_broker = {"tz_offset_min": int(tz_off_min)} if tz_off_min is not None else {}
    return {
        "ok": True,
        "tf": tfu,
        "rows": rows,
        "broker": out_broker,
        "device": device_used,
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

def _rows_to_df(rows):
    """
    Convert normalized OHLC rows (dicts) to a DataFrame with columns: t,o,h,l,c
    Accepts time keys: t_close_ms / t_open_ms / t (sec or ms).
    """
    if not rows or not isinstance(rows, list):
        return None
    data = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        try:
            data.append(
                {
                    "t": _epoch_to_ms_any(r.get("t_close_ms") or r.get("t_open_ms") or r.get("t")),
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
    try:
        import pandas as pd
        return pd.DataFrame(data)
    except Exception:
        return None

def _normalize_snap_bars_to_ms(bars: Any, tf_ms: int) -> list[dict]:
    """
    Normalize snapshot bars into a standard list of dict bars with:
      - t_close_ms (ms)
      - t_open_ms (ms) = t_close_ms - tf_ms
      - o,h,l,c,v,complete
    Also: sort by t_close_ms and drop invalid/future bars.
    """
    if not isinstance(bars, list):
        return []

    out: list[dict] = []
    for b in bars:
        if not isinstance(b, dict):
            continue

        # Accept multiple possible timestamp fields
        t_close = (
            b.get("t_close_ms")
            or b.get("tClose")
            or b.get("t_close")
            or b.get("t")
            or 0
        )

        # Convert seconds -> ms if needed
        try:
            t_close = int(t_close)
        except Exception:
            t_close = 0
        if 0 < t_close < 10_000_000_000:  # looks like seconds
            t_close *= 1000

        # Build normalized bar
        nb = {
            "t_close_ms": t_close,
            "o": b.get("o"),
            "h": b.get("h"),
            "l": b.get("l"),
            "c": b.get("c"),
            "v": b.get("v") if b.get("v") is not None else b.get("vol"),
            "complete": b.get("complete", True),
        }

        # If any OHLC missing, skip (prevents BOS/ATR weirdness)
        if nb["t_close_ms"] <= 0:
            continue
        if nb["c"] is None or nb["h"] is None or nb["l"] is None:
            continue
        if nb["o"] is None:
            nb["o"] = nb["c"]

        # ✅ CRITICAL FIX: ensure t_open_ms is always correct (never 0)
        nb["t_open_ms"] = int(nb["t_close_ms"] - tf_ms)

        out.append(nb)

    # Sort and remove duplicates by t_close_ms
    out.sort(key=lambda x: x.get("t_close_ms", 0))
    dedup: list[dict] = []
    seen = set()
    for b in out:
        tc = b["t_close_ms"]
        if tc in seen:
            continue
        seen.add(tc)
        dedup.append(b)

    return dedup


@router.get("/predict/ping")
def predict_ping():
    return {"ok": True, "msg": "predict router alive"}

def _scan_freshest_device_snap(sym: str, tfu: str, uid: str | None = None):
    """
    NO-SCAN version.
    Try known devices from:
      1) xtl:user:{uid}:devices (preferred)
      2) xtl:devices            (optional global set)
    Returns (best_quote, best_dev)
    """
    best = None
    best_dev = "-"
    best_fresh = -1

    sym = (sym or "").upper().strip()
    tfu = (tfu or "").upper().strip()
    if not sym or not tfu:
        return None, "-"

    # Candidate devices (no SCAN)
    devs = []

    # 1) user devices set (already used elsewhere in this file)
    if uid:
        try:
            ds = R.smembers(f"xtl:user:{uid}:devices") or []
            for d in ds:
                devs.append(d.decode("utf-8", "ignore") if isinstance(d, (bytes, bytearray)) else str(d))
        except Exception:
            pass

    # 2) optional global devices set (if you add it on agent writes)
    if not devs:
        try:
            ds = R.smembers("xtl:devices") or []
            for d in ds:
                devs.append(d.decode("utf-8", "ignore") if isinstance(d, (bytes, bytearray)) else str(d))
        except Exception:
            pass

    # If we don't know any devices, do NOT scan; return fast
    if not devs:
        return None, "-"

    # Probe deterministic keys: xtl:ohlc:snap:{dev}:{sym}:{tf}
    for dev in devs:
        if not dev:
            continue
        k = f"xtl:ohlc:snap:{dev}:{sym}:{tfu}"
        raw = None
        try:
            raw = R.get(k)
        except Exception:
            raw = None
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
        try:
            price = float(last.get("c", 0.0))
        except Exception:
            continue

        try:
            t_s = int(last.get("t", 0))
        except Exception:
            t_s = 0
        t_ms = (t_s * 1000) if t_s < 10_000_000_000 else t_s

        # freshness: prefer serverNow/lastClosedTs if present
        try:
            fresh = max(int(js.get("serverNow") or 0), int(js.get("lastClosedTs") or 0), int(t_ms or 0))
        except Exception:
            fresh = int(t_ms or 0)

        if fresh > best_fresh:
            best_fresh = fresh
            best = {"price": price, "t_ms": t_ms}
            best_dev = dev

    return best, best_dev




@router.get("/predict/all")
def predict_all(
    tf: str = "M15",  # keep default for page-load convenience
    symbols: str = "XAUUSD,EURUSD,USDJPY,GBPUSD,USDCAD,USDCHF",
    device: str | None = Query(None),
    x_device_id: str | None = Header(None, convert_underscores=False),
    user=Depends(require_auth_optional),
):
    """
    Main prediction feed (TF-STRICT, PER-TF FETCH).

    Locked contract:
      1) Forecast is bar-based only (no tick influence).
      2) `price` is tick/live display only.
      3) Target basis is last closed TF close (fallback: last closed M1 close). Never tick.
      4) Per-TF fetch: response contains only requested TF forecast fields.
      5) Freeze: if stale, do not recompute; serve last-good cached row for that TF/symbol.
      6) M15 is paused => returns ok=false model_not_trained.
    """

    # ---------------- STRICT TF VALIDATION ----------------
    tfu = (tf or "").upper().strip()
    if not tfu:
        tfu = "M15"
    if tfu not in ("M15", "H1", "H4"):
        raise HTTPException(status_code=400, detail=f"Invalid tf '{tf}'. Allowed: M15, H1, H4")

    syms = [s.strip().upper() for s in (symbols or "").split(",") if s.strip()]
    user_id = _uid_from_user(user)
    now_ms = int(_time.time() * 1000)

    TF_MS_LOCAL = {"M15": 15 * 60_000, "H1": 60 * 60_000, "H4": 4 * 60 * 60_000}
    tf_ms = TF_MS_LOCAL.get(tfu, 15 * 60_000)

    # ---- imports kept inside to avoid startup import failures ----
    predict_next_hour = None
    predict_next_4h = None
    pull_latest_h1 = None
    pull_latest_h4 = None
    try:
        from api.trend.infer_rt import predict_next_hour, predict_next_4h, pull_latest_h1, pull_latest_h4
    except Exception:
        try:
            from .infer_rt import predict_next_hour, predict_next_4h, pull_latest_h1, pull_latest_h4
        except Exception:
            pass

    # ---- Macro snapshot once per request (IMPORTANT: macro ALWAYS defined) ----
    macro = None
    try:
        macro = get_macro_snapshot()
    except Exception:
        macro = None

    # Debug log (safe for dict or MacroSnapshot)
    try:
        def _mg(x, k):
            return x.get(k) if isinstance(x, dict) else getattr(x, k, None)

        log.warning(
            "[predict_all] tf=%s macro_type=%s dxy_z=%s us10y_z=%s usd_rate_z=%s vix_z=%s",
            tfu,
            type(macro).__name__ if macro is not None else "None",
            _mg(macro, "dxy_z"),
            _mg(macro, "us10y_z"),
            _mg(macro, "usd_rate_z"),
            _mg(macro, "vix_z"),
        )
    except Exception:
        pass

    # Build frames once per request ONLY for the requested TF
    now_frames = None
    try:
        if tfu == "H1" and callable(pull_latest_h1):
            need_syms = ["XAUUSD", "EURUSD", "GBPUSD", "AUDUSD", "USDJPY", "USDCHF", "USDCAD"]
            now_frames = {s: pull_latest_h1(s) for s in need_syms}
        if tfu == "H4" and ENABLE_H4_MODEL and callable(pull_latest_h4):
            need_syms = ["XAUUSD", "EURUSD", "GBPUSD", "AUDUSD", "USDJPY", "USDCHF", "USDCAD"]
            now_frames = {s: pull_latest_h4(s) for s in need_syms}
    except Exception:
        now_frames = None

    # ---- Freeze config (forecast only; tick price is independent) ----
    STALE_MS = 5 * 60_000  # 5 min: used to decide recompute vs freeze
    ROW_LAST_KEY = "xtl:pred:lastrow:{tf}:{sym}"  # Per-symbol, per-TF last-good row cache

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
        raw = pr.get("move_pct", pr.get("predMovePct"))
        try:
            return abs(float(raw)) if raw is not None else 0.0
        except Exception:
            return 0.0

    def _conf_from_p(p: float) -> str:
        try:
            spread = abs(float(p) - 0.5)
        except Exception:
            spread = 0.0
        if spread >= 0.20:
            return "high"
        if spread >= 0.05:
            return "medium"
        return "low"

    def _macro_chips_for_symbol(sym_u_: str, macro_: object | None, p_up_: float | None, extra_: dict | None) -> list[str]:
        """
        Returns up to 4 compact macro chips like: DXY↓, US10Y↓, VIX↑, RVOL↑
        Works with both dict macro and MacroSnapshot object.
        """

        def _get(obj: object | None, key: str):
            try:
                if obj is None:
                    return None
                if isinstance(obj, dict):
                    return obj.get(key)
                return getattr(obj, key, None)
            except Exception:
                return None
        def _get_extra(key: str):
            try:
                return extra_.get(key) if isinstance(extra_, dict) else None
            except Exception:
                return None
        try:
            CHIP_Z_ON = float(os.getenv("XTL_MACRO_CHIP_Z_ON", "0.10"))
        except Exception:
            CHIP_Z_ON = 0.10


        def _sgn(x) -> int:
            try:
                v = float(x)
            except Exception:
                return 0
            if v > CHIP_Z_ON:
                return 1
            if v < -CHIP_Z_ON:
                return -1
            return 0

        def _arrow(sign: int) -> str:
            return "↑" if sign > 0 else ("↓" if sign < 0 else "→")

        
        # z-scores (support both dict keys and MacroSnapshot attrs)
        # IMPORTANT: fallback to z-scores already placed into extra_ by predict_all()
        dxy_z = _get(macro_, "dxy_z")
        if dxy_z is None:
            dxy_z = _get_extra("macro_dxy_z")

        y10_z = _get(macro_, "us10y_z") or _get(macro_, "yield_z")
        if y10_z is None:
            y10_z = _get_extra("macro_yield_z")

        sr_z = _get(macro_, "usd_rate_z") or _get(macro_, "usd_short_rate_z")
        if sr_z is None:
            sr_z = _get_extra("macro_usd_rate_z")

        vix_z = _get(macro_, "vix_z")
        if vix_z is None:
            vix_z = _get_extra("macro_vix_z")


        # Optional: RVOL from extra/pr (not macro snapshot)
        rvol = None
        try:
            if isinstance(extra_, dict):
                rvol = extra_.get("feat_rvol15")
        except Exception:
            rvol = None

        chips: list[tuple[str, int, float]] = []

        is_xau = str(sym_u_ or "").upper().startswith("XAU")

        dxy_s = _sgn(dxy_z)
        if is_xau:
            dxy_s = -dxy_s
        if dxy_s != 0 and dxy_z is not None:
            chips.append(("DXY", dxy_s, abs(float(dxy_z))))

        y10_s = _sgn(y10_z)
        if is_xau:
            y10_s = -y10_s
        if y10_s != 0 and y10_z is not None:
            chips.append(("US10Y", y10_s, abs(float(y10_z))))

        sr_s = _sgn(sr_z)
        if is_xau:
            sr_s = -sr_s
        if sr_s != 0 and sr_z is not None:
            chips.append(("USDRATE", sr_s, abs(float(sr_z))))

        vix_s = _sgn(vix_z)
        if vix_s != 0 and vix_z is not None:
            chips.append(("VIX", vix_s, abs(float(vix_z))))

        try:
            rv = float(rvol) if rvol is not None else None
        except Exception:
            rv = None
        if rv is not None:
            try:
                RVOL_HI = float(os.getenv("XTL_MACRO_RVOL_HI", "1.2"))
                RVOL_LO = float(os.getenv("XTL_MACRO_RVOL_LO", "0.8"))
            except Exception:
                RVOL_HI, RVOL_LO = 1.2, 0.8

                if rv >= RVOL_HI:
                    chips.append(("RVOL", 1, rv))
                elif rv <= RVOL_LO:
                    chips.append(("RVOL", -1, 1.0 - rv))


        chips.sort(key=lambda t: t[2], reverse=True)
        out: list[str] = []
        for k, s, _mag in chips[:4]:
            out.append(f"{k}{_arrow(s)}")
        return out

    def _snap_last_closed_open_ts_ms(bars: list, tf_ms_: int, now_ms_: int) -> int | None:
        for b in reversed(bars or []):
            t_ms = _ms_from_t(b.get("t_open_ms") or b.get("t"))
            if t_ms is None:
                continue
            if b.get("complete") is True or (t_ms + tf_ms_ <= now_ms_):
                return int(t_ms)
        return None

    def _last_closed_close_price(bars: list, tf_ms_: int, now_ms_: int) -> float | None:
        for b in reversed(bars or []):
            t_ms = _ms_from_t(b.get("t_open_ms") or b.get("t"))
            if t_ms is None:
                continue
            if b.get("complete") is True or (t_ms + tf_ms_ <= now_ms_):
                c = b.get("c")
                if isinstance(c, (int, float)):
                    return float(c)
        return None

    def _read_live_price_with_ts(sym: str) -> tuple[float | None, int | None]:
        try:
            sym_u = (sym or "").upper().strip()
            if not sym_u:
                return (None, None)

            dev = (device or x_device_id or "").strip()
            keys: list[str] = []
            if dev:
                dev_key = dev if dev.startswith("dev_") else f"dev_{dev}"
                keys.append(f"xtl:price:{dev_key}:{sym_u}")
            keys.append(f"xtl:price:{sym_u}")

            for k in keys:
                try:
                    v = R.get(k)
                except Exception:
                    v = None
                if not v:
                    continue

                if isinstance(v, (bytes, bytearray)):
                    try:
                        v = v.decode("utf-8", "ignore")
                    except Exception:
                        v = str(v)

                if isinstance(v, str) and v.strip().startswith("{"):
                    try:
                        import json
                        obj = json.loads(v)
                        px = obj.get("price")
                        ts = obj.get("ts_ms")
                        px_f = float(px) if px is not None else None
                        ts_i = int(ts) if ts is not None else None
                        return (px_f, ts_i)
                    except Exception:
                        pass

                try:
                    return (float(v), None)
                except Exception:
                    continue

        except Exception:
            return (None, None)

        return (None, None)

    def _bar_cache_key(sym_u_: str, tf_: str, now_ms_: int, broker_off_min_: int) -> tuple[int, int, str]:
        tf_ms_ = TF_MS_LOCAL.get(tf_, 60 * 60_000)
        off_ms = int(broker_off_min_) * 60_000
        slot0_ms = ((now_ms_ + off_ms) // tf_ms_) * tf_ms_ - off_ms
        last_close_ms = int(slot0_ms)
        last_open_ms = int(slot0_ms - tf_ms_)
        ck = f"xtl:pred:bar:{tf_}:{sym_u_}:{last_close_ms}"
        return last_open_ms, last_close_ms, ck

    def _read_lastrow(sym_u_: str) -> dict | None:
        try:
            import json
            raw = R.get(ROW_LAST_KEY.format(tf=tfu, sym=sym_u_))
            if not raw:
                return None
            if isinstance(raw, (bytes, bytearray)):
                raw = raw.decode("utf-8", "ignore")
            obj = json.loads(raw)
            return obj if isinstance(obj, dict) else None
        except Exception:
            return None

    def _write_lastrow(sym_u_: str, row: dict, ttl_sec: int = 7 * 24 * 3600) -> None:
        try:
            import json
            R.setex(ROW_LAST_KEY.format(tf=tfu, sym=sym_u_), ttl_sec, json.dumps(row, ensure_ascii=False))
        except Exception:
            pass

    def _mk_row(
        sym_u: str,
        ok: bool,
        reason: str | None,
        price_px: float | None,
        price_ts_ms: int | None,
        broker_off_min: int,
        feed_last_ts_ms: int,
        frozen: bool,
        p_up: float | None,
        expected_move_pct: float | None,
        target_price: float | None,
        decision: str | None,
        confidence: str | None,
        label: str | None,
        score: float | None,
        basis_close: float | None,
        basis_source: str,
        bar_open_ms: int | None,
        bar_close_ms: int | None,
        reasons: list[str] | None,
        macro_reasons: list[str] | None = None,
    ) -> dict:
        structure_reason = None
        try:
            rr = reasons or []
            if isinstance(rr, list) and rr:
                structure_reason = str(rr[0])[:120]
        except Exception:
            structure_reason = None

        legacy_reasons = [structure_reason] if structure_reason else []

        return {
            "symbol": sym_u,
            "tf": tfu,

            "price": float(price_px) if isinstance(price_px, (int, float)) else None,
            "price_ts_ms": price_ts_ms,
            "price_source": "live" if isinstance(price_px, (int, float)) else "na",

            "ok": bool(ok),
            "reason": reason,
            "p_up": float(p_up) if isinstance(p_up, (int, float)) else 0.5,
            "prob_up": float(p_up) if isinstance(p_up, (int, float)) else 0.5,
            "expected_move_pct": float(expected_move_pct) if isinstance(expected_move_pct, (int, float)) else 0.0,
            "target_price": float(target_price) if isinstance(target_price, (int, float)) else None,
            "decision": decision or ("ABSTAIN" if not ok else "ABSTAIN"),
            "confidence": confidence or ("low" if not ok else "low"),
            "label": label or ("Unavailable" if not ok else "Neutral"),
            "score": float(score) if isinstance(score, (int, float)) else 0.0,

            "frozen": bool(frozen),
            "feed_last_ts_ms": int(feed_last_ts_ms) if isinstance(feed_last_ts_ms, (int, float)) else 0,
            "server_now_ms": now_ms,
            "resp_ts_ms": now_ms,
            "broker_tz_offset_min": int(broker_off_min),

            "basis_close": float(basis_close) if isinstance(basis_close, (int, float)) else None,
            "basis_source": basis_source,

            "bar_open_ms": bar_open_ms,
            "bar_close_ms": bar_close_ms,

            "reasons": legacy_reasons,
            "structure_reason": structure_reason,
            "macro_reasons": macro_reasons if isinstance(macro_reasons, list) else [],
        }

    rows: list[dict] = []

    # ---------------- MAIN LOOP ----------------
    for sym in syms:
        sym_u = sym.upper().strip()

        live_px, live_ts_ms = _read_live_price_with_ts(sym_u)

        if tfu == "M15":
            row = _mk_row(
                sym_u=sym_u,
                ok=False,
                reason="model_not_trained",
                price_px=live_px,
                price_ts_ms=live_ts_ms,
                broker_off_min=0,
                feed_last_ts_ms=0,
                frozen=False,
                p_up=0.5,
                expected_move_pct=0.0,
                target_price=None,
                decision="ABSTAIN",
                confidence="low",
                label="Unavailable",
                score=0.0,
                basis_close=None,
                basis_source="na",
                bar_open_ms=None,
                bar_close_ms=None,
                reasons=["model_not_trained"],
            )
            rows.append(row)
            _write_lastrow(sym_u, row)
            continue

        if tfu == "H4" and not ENABLE_H4_MODEL:
            row = _mk_row(
                sym_u=sym_u,
                ok=False,
                reason="h4_disabled",
                price_px=live_px,
                price_ts_ms=live_ts_ms,
                broker_off_min=0,
                feed_last_ts_ms=0,
                frozen=False,
                p_up=0.5,
                expected_move_pct=0.0,
                target_price=None,
                decision="ABSTAIN",
                confidence="low",
                label="Unavailable",
                score=0.0,
                basis_close=None,
                basis_source="na",
                bar_open_ms=None,
                bar_close_ms=None,
                reasons=["h4_disabled"],
                macro_reasons=[],
            )
            rows.append(row)
            _write_lastrow(sym_u, row)
            continue

        snap, broker = _read_freshest_snap_for_user_or_any(user_id, sym_u, tfu)
        bars_tf = (snap or {}).get("bars") or []
        try:
            snap_m1, _b1 = _read_freshest_snap_for_user_or_any(user_id, sym_u, "M1")
        except Exception:
            snap_m1 = None
        bars_m1 = (snap_m1 or {}).get("bars") or []

        broker_off_min = 0
        try:
            if isinstance(broker, dict):
                broker_off_min = int(broker.get("tz_offset_min") or broker.get("broker_tz_offset_min") or 0)
        except Exception:
            broker_off_min = 0

        tf_last_open_ms = _snap_last_closed_open_ts_ms(bars_tf, tf_ms, now_ms)
        m1_last_open_ms = _snap_last_closed_open_ts_ms(bars_m1, 60_000, now_ms)

        feed_last_ts_ms = 0
        for x in (tf_last_open_ms, m1_last_open_ms):
            if x is not None:
                try:
                    feed_last_ts_ms = max(feed_last_ts_ms, int(x))
                except Exception:
                    pass

        feed_is_stale = (feed_last_ts_ms <= 0) or ((now_ms - feed_last_ts_ms) > STALE_MS)

        if feed_is_stale:
            cached = _read_lastrow(sym_u)
            if isinstance(cached, dict) and (cached.get("symbol") or "").upper() == sym_u and cached.get("tf") == tfu:
                cached2 = dict(cached)
                cached2["frozen"] = True
                cached2["reason"] = cached2.get("reason") or "feed_stale"
                cached2["feed_last_ts_ms"] = int(feed_last_ts_ms) if feed_last_ts_ms else 0
                cached2["resp_ts_ms"] = now_ms
                cached2["server_now_ms"] = now_ms
                cached2["price"] = float(live_px) if isinstance(live_px, (int, float)) else None
                cached2["price_ts_ms"] = live_ts_ms
                cached2["price_source"] = "live" if isinstance(live_px, (int, float)) else "na"
                rows.append(cached2)
                continue

            row = _mk_row(
                sym_u=sym_u,
                ok=False,
                reason="feed_stale",
                price_px=live_px,
                price_ts_ms=live_ts_ms,
                broker_off_min=broker_off_min,
                feed_last_ts_ms=feed_last_ts_ms,
                frozen=True,
                p_up=0.5,
                expected_move_pct=0.0,
                target_price=None,
                decision="ABSTAIN",
                confidence="low",
                label="Unavailable",
                score=0.0,
                basis_close=None,
                basis_source="na",
                bar_open_ms=None,
                bar_close_ms=None,
                reasons=["feed_stale"],
                macro_reasons=_macro_chips_for_symbol(sym_u, macro, None, None),
            )
            rows.append(row)
            _write_lastrow(sym_u, row)
            continue

        bar_open_ms, bar_close_ms, bar_ck = _bar_cache_key(sym_u, tfu, now_ms, broker_off_min)

        pr: dict = {"ok": False, "reason": "not_loaded"}
        try:
            import json
            raw = R.get(bar_ck)
            if raw:
                if isinstance(raw, (bytes, bytearray)):
                    raw = raw.decode("utf-8", "ignore")
                tmp = json.loads(raw)
                if isinstance(tmp, dict):
                    pr = tmp
        except Exception:
            pass

        if not bool(pr.get("ok", False)):
            if tfu == "H1":
                if callable(predict_next_hour):
                    try:
                        pr = predict_next_hour(sym_u, now_frames=now_frames)  # type: ignore[arg-type]
                    except Exception as e:
                        log.exception("[predict_all] predict_next_hour EXC sym=%s", sym_u)
                        pr = {"ok": False, "reason": "infer_exc_h1", "detail": str(e)}
                else:
                    pr = {"ok": False, "reason": "infer_rt_missing_h1"}

            elif tfu == "H4":
                if callable(predict_next_4h):
                    try:
                        pr = predict_next_4h(sym_u, now_frames=now_frames)  # type: ignore[arg-type]
                    except Exception as e:
                        log.exception("[predict_all] predict_next_4h EXC sym=%s", sym_u)
                        pr = {"ok": False, "reason": "infer_exc_h4", "detail": str(e)}
                else:
                    pr = {"ok": False, "reason": "infer_rt_missing_h4"}

            try:
                import json
                if isinstance(pr, dict) and pr.get("ok"):
                    pr = {**pr, "bar_open_ms": bar_open_ms, "bar_close_ms": bar_close_ms}
                    R.setex(bar_ck, 3 * 24 * 3600, json.dumps(pr, ensure_ascii=False))
            except Exception:
                pass

        if not isinstance(pr, dict):
            pr = {"ok": False, "reason": "infer_not_dict"}

        ok = bool(pr.get("ok", False))

        try:
            if not ok and _is_transient_insufficient(pr):
                lg = _rg_lastgood(sym_u, tfu)
                if isinstance(lg, dict) and lg.get("ok"):
                    pr = {**lg, "stale": True, "stale_reason": pr.get("reason")}
                    ok = True
        except Exception:
            pass

        try:
            if ok:
                _rs_lastgood(sym_u, tfu, pr, ttl_sec=3600)
        except Exception:
            pass

        p_up = _safe_p_up(pr, 0.5)
        mag_pct = _safe_move_pct(pr)

        direction_sign = 1.0 if p_up >= 0.5 else -1.0
        signed_pct = mag_pct * direction_sign
        try:
            expected_move_pct = round(float(signed_pct), 2)
        except Exception:
            expected_move_pct = 0.0

        conf = _conf_from_p(p_up)

        basis_close_tf = None
        try:
            if isinstance(pr.get("lastClose"), (int, float)):
                basis_close_tf = float(pr["lastClose"])
        except Exception:
            basis_close_tf = None

        if not isinstance(basis_close_tf, (int, float)):
            basis_close_tf = _last_closed_close_price(bars_tf, tf_ms, now_ms)

        basis_close_m1 = _last_closed_close_price(bars_m1, 60_000, now_ms)

        if isinstance(basis_close_tf, (int, float)):
            basis_price = float(basis_close_tf)
            basis_source = "tf_close"
        elif isinstance(basis_close_m1, (int, float)):
            basis_price = float(basis_close_m1)
            basis_source = "m1_close"
        else:
            basis_price = None
            basis_source = "na"

        target_price = None
        if isinstance(basis_price, (int, float)):
            decimals = _price_decimals(sym_u)
            try:
                target_price = round(float(basis_price) * (1.0 + expected_move_pct / 100.0), decimals)
            except Exception:
                target_price = None

        base_reasons: list[str] = []
        r_raw = pr.get("reasons") or pr.get("reason")
        if isinstance(r_raw, list):
            base_reasons = [str(x) for x in r_raw if x]
        elif isinstance(r_raw, str) and r_raw:
            base_reasons = [str(r_raw)]

        extra: Dict[str, Any] = {
            "base_reasons": base_reasons,
            "feat_rvol15": pr.get("rvol15"),
            "feat_usd_basket": pr.get("usd_basket_d1h_pct"),
            "tf_scope": tfu,
        }

        # Macro fields into extra (dict or MacroSnapshot safe)
        try:
            def _mg(x, k):
                return x.get(k) if isinstance(x, dict) else getattr(x, k, None)

            extra["macro_dxy_z"] = _mg(macro, "dxy_z")
            extra["macro_yield_z"] = _mg(macro, "us10y_z")
            extra["macro_usd_rate_z"] = _mg(macro, "usd_rate_z") or _mg(macro, "usd_short_rate_z")
            extra["macro_vix_z"] = _mg(macro, "vix_z")
        except Exception:
            pass

        st_thr = 0.35
        ht_thr = 0.70
        tech = signed_pct / (st_thr if tfu == "H1" else ht_thr if tfu == "H4" else 1.0)
        try:
            tech = max(min(float(tech), 1.0), -1.0)
        except Exception:
            tech = 0.0

        label_w = None
        combined_score = None
        try:
            combined_score, label_w, *_ = _compute_weighted_status(sym_u, tech, p_up, extra)
        except Exception:
            combined_score = tech
            label_w = _score_to_label(tech)

        if not label_w:
            label_w = _score_to_label(tech)

        decision = "BUY" if p_up >= 0.5 else "SELL"

        row = _mk_row(
            sym_u=sym_u,
            ok=ok,
            reason=None if ok else str(pr.get("reason", "model_error")),
            price_px=live_px,
            price_ts_ms=live_ts_ms,
            broker_off_min=broker_off_min,
            feed_last_ts_ms=feed_last_ts_ms,
            frozen=False,
            p_up=p_up,
            expected_move_pct=expected_move_pct if ok else 0.0,
            target_price=target_price if ok else None,
            decision=decision if ok else "ABSTAIN",
            confidence=conf if ok else "low",
            label=label_w if ok else "Unavailable",
            score=float(combined_score) if combined_score is not None else 0.0,
            basis_close=float(basis_price) if isinstance(basis_price, (int, float)) else None,
            basis_source=basis_source,
            bar_open_ms=bar_open_ms,
            bar_close_ms=bar_close_ms,
            reasons=_build_reasons(sym_u, label_w, p_up, extra) if ok else base_reasons,
            macro_reasons=_macro_chips_for_symbol(sym_u, macro, p_up, extra) if ok else [],
        )

        rows.append(row)
        _write_lastrow(sym_u, row)

    return {"ok": True, "tf": tfu, "server_now_ms": now_ms, "rows": rows}


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
        # keep this as hint text only until you wire real SR numbers
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
    # hard gate
    if os.getenv("XTL_ENABLE_COMMENTARY", "false").lower() != "true":
        return {"ok": False, "reason": "commentary_disabled"}

    # validate inputs
    tfu = (tf or "").upper().strip()
    sym_u = (symbol or "").upper().strip()
    if not sym_u:
        return {"ok": False, "reason": "missing_symbol"}
    if tfu not in ("M15", "H1", "H4"):
        tfu = "H1"

    # require OpenAI only here (NOT at module import)
    if OpenAI is None:
        return {"ok": False, "reason": "openai_not_installed"}

    api_key = (os.getenv("OPENAI_API_KEY") or "").strip()
    if not api_key:
        return {"ok": False, "reason": "openai_key_missing"}

    # reuse prediction feed (your existing behavior)
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

    # If ML is unavailable, do not generate commentary
    tfs = row.get("tfs") or {}
    h1_ok = bool((tfs.get("H1") or {}).get("ok"))
    h4_ok = bool((tfs.get("H4") or {}).get("ok"))
    if not (h1_ok or h4_ok):
        return {"ok": False, "reason": "no_model_data"}

    # build strict payload (no new numbers)
    payload = build_commentary_payload({
        "symbol": sym_u,
        "horizon": tfu,
        "horizon_min": 60 if tfu == "H1" else (240 if tfu == "H4" else 15),

        "decision": (tfs.get(tfu) or {}).get("decision"),
        "label": (tfs.get(tfu) or {}).get("label"),
        "confidence": (tfs.get(tfu) or {}).get("confidence"),

        "basis_price_1h": row.get("basis_price"),
        "target_price_1h": (tfs.get(tfu) or {}).get("target_price"),
        "expected_move_pct_1h": (tfs.get(tfu) or {}).get("expected_move_pct"),

        "st_trend_label": (tfs.get("H1") or {}).get("label"),
        "ht_trend_label": (tfs.get("H4") or {}).get("label"),

        "reasons_h1": row.get("reasons_h1") or [],
        "reasons_h4": row.get("reasons_h4") or [],

        "updated_broker_ts": row.get("feed_last_ts_ms"),
        "broker_tz_offset_min": row.get("broker_tz_offset_min"),
        "tth_raw": None,
        "tth_p_dir": None,
        "target_close_ts": None,
    })

    # call OpenAI
    try:
        client = OpenAI(api_key=api_key)
        # (keep your existing model/prompt; below is just a safe skeleton)
        out = client.responses.create(
            model=os.getenv("XTL_COMMENTARY_MODEL", "gpt-4.1-mini"),
            input=[
                {
                    "role": "system",
                    "content": (
                        "You are a trading assistant. "
                        "Only narrate the provided JSON. "
                        "Do NOT invent numbers, prices, probabilities, or targets."
                    ),
                },
                {"role": "user", "content": json.dumps(payload, separators=(",", ":"), ensure_ascii=False)},
            ],
        )
        text = getattr(out, "output_text", None) or ""
        return {"ok": True, "symbol": sym_u, "tf": tfu, "commentary": text, "payload": payload}
    except Exception as e:
        return {"ok": False, "reason": "openai_error", "detail": str(e)}

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



@router.get("/opportunities/history")
def opportunities_history(limit: int = 100):
    """
    TEMP: history via CSV is deprecated.
    Frontend now uses in-session history only.
    This endpoint returns an empty list to keep compatibility.
    """
    return {"ok": True, "rows": []}




@router.get("/opportunities")
def trend_opportunities(
    request: Request,
    tf: str = "M15",
    symbols: str = "XAUUSD,EURUSD,USDJPY,GBPUSD,USDCAD,USDCHF",
    device: str | None = Query(None),
    x_device_id: str | None = Header(None, alias="X-Device-Id"),
    loose: bool = Query(False),
    debug_force: bool = Query(False),
    debug_top: int = Query(0, ge=0, le=10),
    debug_persist: bool = Query(False),
    debug_gate: int = Query(0),
    user=Depends(require_auth_optional),
):
    """
    Live opportunities feed.

    OPPT rules (as per UI requirement):
    - Create only from forecast-based trigger.
    - Once created, keep visible until HIT or EXPIRED.
    - No "bias flip" auto-expire here (to avoid 5-10 min flip noise).
    - Weekend: do NOT create new opps.
    """
     
    from collections import defaultdict
    


    gate_stats = defaultdict(int)
    tfu = (tf or "M15").upper()
    now_ms = int(_time.time() * 1000)
    # ---- debug_gate: normalize ONCE (do not re-parse later) ----
    try:
        debug_gate_on = bool(int(debug_gate))
    except Exception:
        # allow "true/yes/on" style if someone passes it weirdly
        try:
            debug_gate_on = str(debug_gate).strip().lower() in ("1", "true", "t", "yes", "y", "on")
        except Exception:
            debug_gate_on = False


    # -------------------------------------------------
    # defaults so debug_gate/debug_force never 500s
    # -------------------------------------------------
    cache_key: str | None = None
    cache_ttl_s: int = 0
    inflight_lock_key: str | None = None
    inflight_got_lock: bool = False


    # ---------- helpers ----------
    def _sym_list(s: str) -> list[str]:
        out = []
        for x in (s or "").split(","):
            xx = x.strip().upper()
            if xx:
                out.append(xx)
        return out

   

    def _redis_hash_to_dict(h: dict) -> dict:
        out = {}
        for k, v in (h or {}).items():
            kk = k.decode("utf-8", "ignore") if isinstance(k, (bytes, bytearray)) else str(k)
            vv = v.decode("utf-8", "ignore") if isinstance(v, (bytes, bytearray)) else v
            out[kk] = _json_load_maybe(vv)
        return out

    


    
    # ---------- auth gate (entry logic requires login) ----------
    def _uid_from_user(u):
        try:
           if u is None:
               return None
           # dict user (common)
           if isinstance(u, dict):
               return u.get("id") or u.get("user_id") or u.get("uid") or u.get("sub")
           # object user
           return (
               getattr(u, "id", None)
               or getattr(u, "user_id", None)
               or getattr(u, "uid", None)
               or getattr(u, "sub", None)
           )
        except Exception:
           return None

    uid_for_entry = _uid_from_user(user)
    # -------------------------------------------------
    # -------------------------------------------------
    # FIX 2: Redis snapshot cache (2s TTL) to avoid 504
    # + anti-stampede lock (prevents concurrent predict_all storms)
    # -------------------------------------------------
    cache_ttl_s = 2  # tiny TTL keeps it fresh but collapses burst polling
    inflight_lock_key = None
    inflight_got_lock = False
    sym_key = ",".join(_sym_list(symbols))
    dev_key = (x_device_id or device or "").strip() or "nodev"
    uid_key = str(uid_for_entry or "nouid")
    cache_key = f"xtl:oppt:cache:{tfu}:{uid_key}:{dev_key}:{sym_key}"
    # ---- DEBUG: bypass cache so gate diagnostics are always live ----
    if debug_gate or debug_force or (debug_top and debug_top > 0) or debug_persist:
        cache_key = None
        cache_ttl_s = 0

    # 1) fast-path: serve cache immediately
    if cache_key and (not (debug_force or debug_gate or loose)):
        try:
           cached = R.get(cache_key)
           if cached:
               js = _json_load_twice(cached)
               if isinstance(js, dict) and js.get("ok"):
                   js["cached"] = True
                   js["cache_ttl_s"] = cache_ttl_s
                   return js
        except Exception:
           pass

        # 2) anti-stampede: only one request computes for ~6s
        inflight_lock_key = cache_key + ":lock"
        try:
           inflight_got_lock = bool(R.set(inflight_lock_key, "1", nx=True, ex=6))
        except Exception:
           inflight_got_lock = False

        # If we didn't get lock, someone else is computing → serve stale if available
        if not inflight_got_lock:
            try:
                cached2 = R.get(cache_key)
                if cached2:
                    js2 = _json_load_twice(cached2)
                    if isinstance(js2, dict) and js2.get("ok"):
                        js2["cached"] = True
                        js2["cache_ttl_s"] = cache_ttl_s
                        js2["cache_note"] = "served_stale_while_inflight"
                        return js2
            except Exception:
                pass


    
    # Allow entry logic if user is logged-in OR device id header is present
    # ------------------------------------------------------------
    # Device resolution (single source of truth)
    # ------------------------------------------------------------
    # We may get device id from multiple places:
    #   - request header X-Device-Id (preferred)
    #   - query param / injected x_device_id
    #   - existing "device" variable (older flows)
    #
    # Goal:
    #   - Always end up with ONE effective_device and use it everywhere
    #     (predict_all, snapshot reads, zone gate).
    # ------------------------------------------------------------
    pinned_device = ""
    try:
        pinned_device = (getattr(user, "pinned_device", None) or getattr(user, "pinnedDevice", None) or "").strip()
    except Exception:
        pinned_device = ""
    pinned_device = (pinned_device or "").strip()
    device = (device or "").strip()

    # Prefer FastAPI-parsed header param, but fallback to raw headers too
    try:
        _hdr_raw = request.headers.get("x-device-id") or request.headers.get("X-Device-Id")
    except Exception:
        _hdr_raw = None
    x_device_id_hdr = (x_device_id or _hdr_raw or "").strip()


    resolved_device = (x_device_id_hdr or pinned_device or device).strip()
    effective_device = resolved_device
    dev_for_gate = resolved_device
    
    if effective_device:
        x_device_id = x_device_id_hdr or effective_device
        device = device or effective_device
        pinned_device = pinned_device or effective_device

    # --- HARD FALLBACK: auto-select an online device if none is pinned ---
    # Step 2: HARD fallback only if still none
    if not resolved_device and R is not None:
        try:
            best_dev = None
            best_hb = -1
            for key in R.scan_iter("device:dev_*"):
                try:
                    h = R.hgetall(key) or {}
                except Exception:
                    h = {}
                if not h:
                    continue

                status = h.get(b"status") or h.get("status")
                if isinstance(status, (bytes, bytearray)):
                    status = status.decode("utf-8", "ignore")
                if (status or "").strip().lower() != "online":
                    continue

                hb = h.get(b"last_heartbeat_ms") or h.get("last_heartbeat_ms")
                if isinstance(hb, (bytes, bytearray)):
                    hb = hb.decode("utf-8", "ignore").strip()
                try:
                    hb_i = int(hb) if hb not in (None, "") else -1
                except Exception:
                    hb_i = -1

                key_s = key.decode("utf-8", "ignore") if isinstance(key, (bytes, bytearray)) else str(key)
                dev_id = key_s.replace("device:", "").strip()

                if hb_i > best_hb:
                    best_hb = hb_i
                    best_dev = dev_id

            if best_dev:
                resolved_device = best_dev
        except Exception:
            pass

    # Step 3: propagate resolved device consistently everywhere downstream
    effective_device = resolved_device
    dev_for_gate = resolved_device

    if effective_device:
        # Ensure downstream calls all see the same resolved value
        x_device_id = effective_device
        device = effective_device
        pinned_device = pinned_device or effective_device

    auth_ok = bool(uid_for_entry) or bool(effective_device)
    # --- DEBUG BYPASS: allow zone-gate evaluation using X-Device-Id even without login ---
    # This does NOT place orders; it only computes and returns entry_gate metadata.
    if (not auth_ok) and (debug_gate or debug_force) and x_device_id_hdr:
        effective_device = x_device_id_hdr
        dev_for_gate = x_device_id_hdr
        x_device_id = x_device_id_hdr
        device = x_device_id_hdr
        pinned_device = pinned_device or x_device_id_hdr
        auth_ok = True
        if oppt_dev_dbg is not None:
            oppt_dev_dbg["dbg_auth_bypass"] = True


    print(
        "[OPPT_AUTH]",
        {
            "uid_for_entry": uid_for_entry,
            "pinned_device": pinned_device,
            "x_device_id_hdr": x_device_id_hdr,
            "device_qs": device,
            "effective_device": effective_device,
            "dev_for_gate": dev_for_gate,
            "auth_ok": auth_ok,
        },
    )

    # Build debug dict here; attach to rows later (row doesn't exist yet)
    oppt_dev_dbg = None

    if debug_gate_on:
        # show real header values (previous code accidentally echoed x_device_id twice)
        try:
            hdr_l = request.headers.get("x-device-id")
            hdr_u = request.headers.get("X-Device-Id")
            hdr_keys_head = list(request.headers.keys())[:12]
        except Exception:
            hdr_l = None
            hdr_u = None
            hdr_keys_head = None

        try:
            scope_has_x_device_id = any(
                (k.decode("utf-8", "ignore").lower() == "x-device-id")
                for (k, _v) in (request.scope.get("headers") or [])
                if isinstance(k, (bytes, bytearray))
            )
        except Exception:
            scope_has_x_device_id = None

        oppt_dev_dbg = {
             "dbg_x_device_id_hdr": x_device_id_hdr,
             "dbg_hdr_x_device_id_l": hdr_l,
             "dbg_hdr_x_device_id_u": hdr_u,
             "dbg_hdr_keys_head": hdr_keys_head,
             "dbg_scope_has_x_device_id": scope_has_x_device_id,
             "dbg_pinned_device": pinned_device,
             "dbg_effective_device": effective_device,
             "dbg_dev_for_gate": dev_for_gate,
             "dbg_auth_ok": auth_ok,
        }
        print("[OPPT_DEV]", oppt_dev_dbg)
    
    
    # ---------- load CLOSED M1 candles from user snapshot (agent pushed) ----------
    def _get_closed_m1(sym: str) -> list[dict]:
        """
        Reads CLOSED M1 bars pushed by agent.

        Primary:
          xtl:trend:snap:{uid}:{SYMBOL}:M1

        Fallback (when user snapshot missing or uid missing):
          xtl:ohlc:snap:{device_id}:{SYMBOL}:M1
          (and hydrate user snapshot when uid exists)

        Returns list of {o,h,l,c} dicts (entry_logic compatible).
        """
        try:
            sym_u = (sym or "").upper().strip()
            if not sym_u:
                return []

            uid = _uid_from_user(user)

            raw = None
            key_user = None

            # ----- primary: user snapshot -----
            if uid:
                key_user = f"xtl:trend:snap:{uid}:{sym_u}:M1"
                raw = R.get(key_user)

            # ----- fallback: device snapshot + hydrate user snapshot -----
            if not raw:
                dev = ""
                try:
                    dev = str(effective_device or x_device_id or device or "").strip()
                except Exception:
                    dev = ""

                # If we have uid but no explicit dev header, try leader + registered devices
                if (not dev) and uid:
                    try:
                        leader = _json_load_twice(R.get(f"xtl:user:{uid}:trend:leader")) or {}
                        if isinstance(leader, dict):
                            dev = (
                                leader.get("device_id")
                                or leader.get("id")
                                or leader.get("device")
                                or ""
                            ).strip()
                        elif isinstance(leader, str):
                            dev = leader.strip()
                    except Exception:
                        dev = ""

                # Fallback to any registered device (only if we have uid)
                if (not dev) and uid:
                    try:
                        ds = list(R.smembers(f"xtl:user:{uid}:devices") or [])
                        if ds:
                            d0 = ds[0]
                            if isinstance(d0, (bytes, bytearray)):
                                d0 = d0.decode("utf-8", "ignore")
                            dev = str(d0).strip()
                    except Exception:
                        dev = ""

                if dev:
                    try:
                        raw2 = R.get(f"xtl:ohlc:snap:{dev}:{sym_u}:M1")
                        if raw2:
                            # Hydrate user snapshot so next call hits the primary key (only if uid exists)
                            if uid and key_user:
                                try:
                                    R.setex(key_user, 3600, raw2)
                                except Exception:
                                    pass
                            raw = raw2
                    except Exception:
                        pass

            js = _json_load_twice(raw) if raw else None
            if not isinstance(js, dict):
                return []

            bars = js.get("bars") or []
            if not isinstance(bars, list):
                return []

            out: list[dict] = []
            for b in bars:
                if not isinstance(b, dict):
                    continue
                if not b.get("complete", True):
                    continue
                try:
                    out.append(
                        {"o": float(b["o"]), "h": float(b["h"]), "l": float(b["l"]), "c": float(b["c"])}
                    )
                except Exception:
                    continue

            return out[-60:]
        except Exception:
            return []

   

    def _force_hydrate_m1(sym: str) -> None:
        
        try:
           uid = _uid_from_user(user)
           if not uid:
               return

           sym_u = (sym or "").upper().strip()
           if not sym_u:
               return

           user_key = f"xtl:trend:snap:{uid}:{sym_u}:M1"
           if R.exists(user_key):
               return

           leader = _json_load_twice(R.get(f"xtl:user:{uid}:trend:leader")) or {}
           dev = ""
           if isinstance(leader, dict):
               dev = (leader.get("device_id") or "").strip()

           if not dev:
               return

           raw = R.get(f"xtl:ohlc:snap:{dev}:{sym_u}:M1")
           if raw:
               R.setex(user_key, 3600, raw)
        except Exception:
           pass
       
  
    def _price_from_ohlc_snap(raw: str) -> float | None:
        try:
           obj = json.loads(raw)
        except Exception:
           return None

        bars = obj.get("bars") if isinstance(obj, dict) else None
        if not isinstance(bars, list) or not bars:
            return None

        # Prefer last COMPLETE bar close
        for b in reversed(bars):
            try:
               if bool(b.get("complete")):
                   c = b.get("c", None)
                   return float(c) if c is not None else None
            except Exception:
               continue

        # Fallback: last bar close
        try:
           c = bars[-1].get("c", None)
           return float(c) if c is not None else None
        except Exception:
           return None
     
    

    def _attach_entry_1m(row: dict) -> None:
        try:
            sym = str(row.get("symbol") or "").upper()
            if not sym:
                return
            if not auth_ok:
                row["entry_1m"] = {"ok": False, "reason": "auth_required"}
                row["signal"] = "WAIT"
                row["signal_reason"] = "auth_required"
                return             

            direction = str(
                row.get("decision")
                or row.get("opp_direction")
                or row.get("direction")
                or ""
            ).upper()

            if direction in ("UP", "BUY"):
                direction = "BUY"
            elif direction in ("DOWN", "SELL"):
                direction = "SELL"
            else:
                row["entry_1m"] = {"ok": False, "reason": "bad_direction"}
                row["signal"] = "WAIT"
                row["signal_reason"] = "bad_direction"
                return
            # Ensure we have M1 bars in user snapshot (hydrates from leader/registered device if needed)
            _force_hydrate_m1(sym)

            candles = _get_closed_m1(sym)
            # ==========================================================
            # TEMP DEBUG: FORCE ENTRY (validate TP/SL lifecycle end-to-end)
            # Enable with env: XTL_FORCE_ENTRY=1
            # ==========================================================
            if os.getenv("XTL_FORCE_ENTRY", "0") == "1":
                lp = row.get("last_price") or row.get("price") or row.get("mid")
                try:
                    lp = float(lp) if isinstance(lp, (int, float)) else None
                except Exception:
                    lp = None

                row["entry_1m"] = {
                    "ok": True,
                    "reason": "FORCED_DEBUG",
                    "mode": "FORCE",
                    "entry_trigger": "CLOSE",
                    "entry_price": lp,
                }

                row["signal"] = direction
                row["signal_reason"] = "FORCED_DEBUG"
                return

            try:
                if candles:
                    c_last = candles[-1].get("c") or candles[-1].get("close")
                    if c_last is not None:
                       row["last_price"] = float(c_last)
            except Exception:
                pass

            # ---- NORMAL LOGIC CONTINUES BELOW ----
            if len(candles) < 8:
                # Try one more hydrate + read (covers race where device posted just now)
                try:
                    _force_hydrate_m1(sym)
                except Exception:
                    pass

                candles = _get_closed_m1(sym)
                if len(candles) < 8:
                    row["entry_1m"] = {"ok": False, "reason": "need_8_bars", "bars": len(candles)}
                    row["signal"] = "WAIT"
                    row["signal_reason"] = "need_8_bars"
                    return

            # --- ENTRY PROFILE SWITCH ---
            # DEFAULT is production; set XTL_ENTRY_PROFILE=TEST only when you want relaxed gating
            active_profile = os.getenv("XTL_ENTRY_PROFILE", "DEFAULT").upper().strip()
            if active_profile not in ("DEFAULT", "TEST"):
                active_profile = "DEFAULT"

            
            # Base production defaults (these are "overrides" applied on top of entry_decision_m1 DEFAULT)
            base_default_overrides = {
                "max_age_min": 120,
                "min_remaining_tp_frac": 0.15,
                "max_traveled_tp_frac": 0.70,

                "impulse_range_mult": 1.10,
                "impulse_body_frac": 0.40,
                "impulse_min_tp_frac": 0.04,

                "pullback_min": 0.08,
                "pullback_max": 0.65,
                "pullback_reject": 0.80,

                "prefer_mode": "MOMENTUM",
                "use_break_trigger": False,
            }

            # Optional relaxed overrides for lifecycle testing
            test_default_overrides = {
                "max_age_min": 240,
                "min_remaining_tp_frac": 0.05,
                "max_traveled_tp_frac": 0.90,

                "impulse_range_mult": 0.90,
                "impulse_body_frac": 0.25,
                "impulse_min_tp_frac": 0.01,

                "pullback_min": 0.02,
                "pullback_max": 0.80,
                "pullback_reject": 0.95,

                "prefer_mode": "MOMENTUM",
                "use_break_trigger": False,
            }
            profiles = {
                "_active": active_profile,
                "DEFAULT": (test_default_overrides if active_profile == "TEST" else base_default_overrides),

                # Per-symbol tweaks (apply regardless of active profile)
                "XAUUSD": {"spread_tp_mult": 1.6, "body_spread_mult": 0.9},
                "USDJPY": {"spread_tp_mult": 1.6, "body_spread_mult": 0.9},
                "EURUSD": {"spread_tp_mult": 1.6, "body_spread_mult": 0.9},
            }

            row["entry_1m"] = entry_decision_m1(
                sym=sym,
                direction=direction,
                basis_price=float(row.get("basis_price") or row.get("basis_price_1h") or row.get("alert_price_1h") or 0.0),
                target_price=float(row.get("target_price") or row.get("target_price_1h") or 0.0),
                alert_created_ms=int(row.get("alert_created_ms") or row.get("opp_open_ts") or 0),
                now_ms=now_ms,
                candles=candles,
                spread=None,
                profiles=profiles,
            )

            e = row.get("entry_1m") or {}
            if bool(e.get("ok")):
                row["signal"] = direction
                row["signal_reason"] = str(e.get("reason") or "entry_ok")
            else:
                row["signal"] = "WAIT"
                row["signal_reason"] = str(e.get("reason") or "entry_wait")

        except Exception as e:
            row["entry_1m"] = {"ok": False, "reason": f"exc:{type(e).__name__}"}
            row["signal"] = "WAIT"
            row["signal_reason"] = f"exc:{type(e).__name__}"

    def _set_signal_from_entry(row: dict) -> None:
        """
        Manual trading signal:
        - WAIT until entry gate triggers (entry_1m.ok True)
        - Once it triggers BUY/SELL, keep that same BUY/SELL until HIT or EXPIRED
          (no flip back to WAIT)
        - Freeze entry meta: entry_ts_ms, entry_price
        """
        # Use server_now_ms if present, else current time
        try:
            now_ms = int(row.get("server_now_ms") or 0)
        except Exception:
            now_ms = 0

        if now_ms <= 0:
            try:
                now_ms = int(_time.time() * 1000)
            except Exception:
                now_ms = 0

        # ------------------------------------------------------------
        # If snapshot already has a frozen entry signal, honor it.
        # ------------------------------------------------------------
        try:
            if bool(row.get("entry_triggered")):
                sig0 = str(row.get("entry_signal") or "").upper()
                if sig0 in ("BUY", "SELL"):
                    row["signal"] = sig0
                    row["signal_text"] = sig0
                    row["signal_reason"] = str(row.get("entry_reason") or "entry_triggered")

                    # TP/SL disabled always
                    _disable_tp_sl_fields(row)

                    return 
                    
                    
        except Exception:
            pass

        # ------------------------------------------------------------
        # If auth is required and entry was blocked upstream, keep it explicit.
        # ------------------------------------------------------------
        ed = row.get("entry_1m") or {}
        if isinstance(ed, dict) and str(ed.get("reason") or "") == "auth_required":
            row["signal"] = "WAIT"
            row["signal_text"] = "LOGIN"
            row["signal_reason"] = "auth_required"
            
            _disable_tp_sl_fields(row)
            return

        # ------------------------------------------------------------
        # If entry gate says OK, emit BUY/SELL and FREEZE it (persist fields into snapshot)
        # ------------------------------------------------------------
        eg = row.get("entry_gate") or row.get("gate") or {}
        eg_reason = str(eg.get("reason") or "").upper()
        try:
            tap_n = int(float(eg.get("tap_count") or 0))
        except Exception:
            tap_n = 0

        rev_ok = bool(eg.get("rev_ok"))

        # HARD BLOCK: do not freeze/enter unless reversal is confirmed
        # Allow entry evaluation once TAP is armed (tap_count 1..3)
        armed = False
        if eg_reason in ("REVERSAL_OK", "LOOSE_BYPASS"):
            armed = True
        elif eg_reason == "ARMED_TAP" and 1 <= tap_n <= 3:
            armed = True

        if not armed:
            row["signal"] = "WAIT"
            row["signal_text"] = "WAIT"
            row["signal_reason"] = f"waiting_gate:{eg_reason.lower() or 'missing'}"
            return


        if isinstance(ed, dict) and ed.get("ok") is True:
            dec = str(
                row.get("decision")
                or row.get("opp_direction")
                or row.get("direction")
                or ""
            ).upper()

            if dec in ("UP", "LONG", "BULLISH", "BULL"):
                dec = "BUY"
            elif dec in ("DOWN", "SHORT", "BEARISH", "BEAR"):
                dec = "SELL"

            sig = dec if dec in ("BUY", "SELL") else "WAIT"
            row["signal"] = sig
            row["signal_text"] = sig
            row["signal_reason"] = f"ENTRY_OK:{ed.get('mode') or 'NA'}:{ed.get('entry_trigger') or 'NA'}"

            # --------------------------------------------------------
            # ENTRY ACCEPT / FREEZE (only after late-entry gating passes)
            # --------------------------------------------------------
            if sig in ("BUY", "SELL"):
                # best-effort entry price
                ep = (
                    (ed.get("entry_price") if isinstance(ed, dict) else None)
                    or row.get("last_price")
                    or row.get("live")
                    or row.get("live_price")
                    or row.get("price")
                    or row.get("mid")
                    or row.get("lastClose")
                    or row.get("basis_price")
                    or row.get("basis_price_1h")
                )
                try:
                    ep0 = float(ep)
                except Exception:
                    ep0 = None

                # Preserve original model levels
                #tp_orig = None
                #sl_orig = None

                #try:
                #    tp_orig = float(tp_orig) if tp_orig is not None else None
                #except Exception:
                #    tp_orig = None

                #try:
                #    sl_orig = float(sl_orig) if sl_orig is not None else None
                #except Exception:
                #    sl_orig = None

                #row["tp_price_orig"] = tp_orig
                #row["sl_price_orig"] = sl_orig

                #tp = None
                #sl = None

                # TP pct override (testing) -> else fallback to model move
                #tp_pct = None
                #try:
                 #  tp_env = float(os.getenv("XTL_ENTRY_TP_PCT", "0.0"))
                  # if tp_env and tp_env > 0:
                   #   tp_pct = tp_env / 100.0
                #except Exception:
                #   tp_pct = None

                # fallback: TP pct from model move
                #if tp_pct is None:
                #    for k in ("trade_tp_pct_1h", "expected_move_pct_1h", "expected_move_pct", "move_pct_1h"):
                #        v = row.get(k)
                #        if isinstance(v, (int, float)):
                #            try:
                #               tp_pct = abs(float(v)) / 100.0
                #               break
                #            except Exception:
                #               tp_pct = None

                # SL pct fallback
                #try:
                #    sl_pct = float(os.getenv("XTL_ENTRY_SL_PCT", "0.15")) / 100.0
                #except Exception:
                #    sl_pct = 0.0015

                #use_rrr_sl = os.getenv("XTL_ENTRY_USE_RRR_SL", "1") == "1"
                #try:
                #    rrr = float(os.getenv("XTL_ENTRY_RRR", "1.20"))
                #   if rrr <= 0:
                #        rrr = 1.20
                #except Exception:
                #    rrr = 1.20

                # Compute TP/SL anchored to entry if possible
                #if ep0 is not None and ep0 > 0:
                #    if tp_pct is not None and tp_pct > 0:
                #        if sig == "BUY":
                #            tp = ep0 * (1.0 + tp_pct)
                #            reward = abs(tp - ep0)
                #            if use_rrr_sl:
                #                risk = reward / rrr
                #                sl = ep0 - risk
                #            else:
                #                sl = ep0 * (1.0 - sl_pct)
                #        else:  # SELL
                #            tp = ep0 * (1.0 - tp_pct)
                #            reward = abs(ep0 - tp)
                #            if use_rrr_sl:
                #                risk = reward / rrr
                #                sl = ep0 + risk
                #            else:
                #                sl = ep0 * (1.0 + sl_pct)

                #    if tp is None and tp_orig is not None:
                #        tp = float(tp_orig)
                #   if sl is None and sl_orig is not None:
                #        sl = float(sl_orig)

                #    if sl is None and tp is not None:
                #        if sig == "BUY":
                #            sl = ep0 * (1.0 - sl_pct)
                #        else:
                #            sl = ep0 * (1.0 + sl_pct)

                
                # -------- late-entry gating REMOVED (WAIT-FOREVER policy) --------
                # We still compute remaining room for debugging/UI,
                # but we do NOT block entry once entry_1m.ok=True.
                basis0 = (
                    row.get("basis_price_1h")
                    or row.get("basis_price")
                    or (row.get("raw") or {}).get("lastClose")
                    or (row.get("raw") or {}).get("basis_price_1h")
                )
                try:
                    basis0 = float(basis0) if basis0 is not None else None
                except Exception:
                    basis0 = None

                

                # -------- NOW FREEZE (entry accepted) --------
                row["entry_triggered"] = True
                row["entry_signal"] = sig
                row["entry_reason"] = row.get("signal_reason") or "entry_triggered"
                row["entry_ts_ms"] = int(now_ms) if now_ms > 0 else int(_time.time() * 1000)
                row["entry_price"] = float(ep0) if ep0 is not None else None
                # ---- freeze entry zone (MANDATORY for exit consistency) ----
                try:
                    gm = row.get("entry_gate") or row.get("gate") or row.get("gate_meta") or {}
                    z = None
                    if isinstance(gm, dict):
                        z = gm.get("zone") or gm.get("zone_meta") 
                    if not isinstance(z, dict):
                        # fallback: some pipelines attach zone directly
                        z = row.get("zone") if isinstance(row.get("zone"), dict) else None

                    if isinstance(z, dict):
                        zl = z.get("low"); zh = z.get("high"); zv = z.get("level")
                        if isinstance(zl, (int, float)) and isinstance(zh, (int, float)) and isinstance(zv, (int, float)):
                            row["entry_zone_low"] = float(zl)
                            row["entry_zone_high"] = float(zh)
                            row["entry_zone_level"] = float(zv)
                            row["entry_zone_type"] = str(z.get("type") or z.get("zone_type") or "")
                except Exception:
                    pass
                _disable_tp_sl_fields(row)

                

                # Discord notify (best-effort)
                try:
                    _maybe_discord_entry(row=row, sig=sig, tp=None, sl=None, now_ms=now_ms)
                except Exception:
                    pass
                
                return
             
            

        # ------------------------------------------------------------
        # Not triggered yet -> WAIT
        # ------------------------------------------------------------
        row["signal"] = "WAIT"
        row["signal_text"] = "WAIT"
        row["signal_reason"] = ed.get("reason") if isinstance(ed, dict) else "no_entry"

        # --- Make entry gating visible to UI/debug ---
        row["entry_reason"] = row.get("signal_reason")
        if isinstance(ed, dict):
            row["entry_debug"] = {
                "ok": bool(ed.get("ok")),
                "reason": ed.get("reason"),
                "age_min": ed.get("age_min"),
                "mode": ed.get("mode"),
                "notes": ed.get("notes"),
            }
        # --- NEW: blank TP/SL while WAIT ---
        row["tp_price"] = None
        row["sl_price"] = None
        row["target_price"] = None
        row["target_price_1h"] = None
        row["stop_loss"] = None
        row["stop_loss_1h"] = None


    def _tf_view(row: dict, tfu: str) -> dict:
        """
        Merge per-tf view (row["tfs"][TF]) into the base row so downstream code
        can keep using row.get("decision"), row.get("expected_move_pct"), etc.
        """
        try:
            tfs = row.get("tfs")
            if isinstance(tfs, dict):
                v = tfs.get(tfu)
                if isinstance(v, dict):
                    merged = dict(row)
                    merged.update(v)
                    return merged
        except Exception:
            pass
        return row


    def _get_float(d: dict, *keys: str):
        for k in keys:
            try:
                v = d.get(k)
                if isinstance(v, (int, float)):
                    return float(v)
                if isinstance(v, str) and v.strip() != "":
                    return float(v)
            except Exception:
                pass
        return None
    def _sr_gate_view(sr_any: Any) -> dict:
        """
        Normalize SR bundle to what _zone_reversal_gate expects:
        nearest_support / nearest_resistance (floats).
        Accepts multi-tf sr bundles (H1/H4) or nearest dict formats.
        """
        out: dict = {}
        sr = sr_any if isinstance(sr_any, dict) else {}

        # 1) If sr already has nearest_support/resistance, keep it.
        ns = sr.get("nearest_support")
        nr = sr.get("nearest_resistance")
        if isinstance(ns, (int, float)):
            out["nearest_support"] = float(ns)
        if isinstance(nr, (int, float)):
            out["nearest_resistance"] = float(nr)

        # 2) Try multi-tf: prefer H1, then H4
        for tfk in ("H1", "h1", "H4", "h4"):
            z = sr.get(tfk)
            if not isinstance(z, dict):
                continue
            nearest = z.get("nearest") or z.get("nearest_zone") or z
            if not isinstance(nearest, dict):
                continue
            kind = str(nearest.get("kind") or nearest.get("side") or "").lower()
            lvl = nearest.get("level")
            if not isinstance(lvl, (int, float)):
                continue
            lvl = float(lvl)
            if kind == "support" and "nearest_support" not in out:
                out["nearest_support"] = lvl
            if kind == "resistance" and "nearest_resistance" not in out:
                out["nearest_resistance"] = lvl

        # 3) Last resort: sr["nearest"] (if present)
        nearest2 = sr.get("nearest") or sr.get("nearest_zone")
        if isinstance(nearest2, dict):
            kind = str(nearest2.get("kind") or nearest2.get("side") or "").lower()
            lvl = nearest2.get("level")
            if isinstance(lvl, (int, float)):
                lvl = float(lvl)
                if kind == "support" and "nearest_support" not in out:
                    out["nearest_support"] = lvl
                if kind == "resistance" and "nearest_resistance" not in out:
                    out["nearest_resistance"] = lvl

        return out


    def _run_entry_only_if_armed(r: dict) -> None:
        try:
            # If already triggered, always show signal from entry (don’t require "armed")
            if bool(r.get("entry_triggered")):
                _set_signal_from_entry(r)
                return
            gm = r.get("entry_gate") or r.get("gate") or r.get("gate_meta") or {}
            reason = str((gm or {}).get("reason") or "").upper()

            if reason in ("REVERSAL_OK","ARMED_TAP", "LOOSE_BYPASS"):
                _attach_entry_1m(r)
                _set_signal_from_entry(r)
                return

            # not armed => do not compute M1 entry
            r["entry_1m"] = {"ok": False, "reason": f"not_armed:{reason.lower() or 'missing'}"}
            r["signal"] = "WAIT"
            r["signal_reason"] = f"not_armed:{reason.lower() or 'missing'}"
        except Exception:
            r["entry_1m"] = {"ok": False, "reason": "not_armed:exc"}
            r["signal"] = "WAIT"
            r["signal_reason"] = "not_armed:exc"

    def _load_active_snapshots(symbols_csv: str) -> list[dict]:
        res: list[dict] = []
        user_id = _uid_from_user(user)
        pinned_device = device or x_device_id

        for sym in _sym_list(symbols_csv):
            sym_u = (sym or "").upper().strip()
            if not sym_u:
                continue

            for d in ("UP", "DOWN"):
                snap_key = _opp_snapshot_key(sym_u, d)

                # Build row FIRST so outcome checker can use live-ish price
                raw = None
                try:
                    raw = _snap_get_raw_json(snap_key)   # works for STRING or HASH
                except Exception:
                    raw = None

                if not raw:
                    continue

                # optional: only fetch hash fields if you still need snap dict later
                snap = None
                try:
                    snap = R.hgetall(snap_key) or None
                except Exception:
                    snap = None

                row0 = {}
                if raw:
                    try:
                        row0 = json.loads(raw) if isinstance(raw, str) else (raw or {})
                    except Exception:
                        row0 = {}
                else:
                    # fallback if snap is already a dict/hash payload
                    try:
                        row0 = _redis_hash_to_dict(snap)
                    except Exception:
                        row0 = {}
                row0.setdefault("symbol", sym_u)

                
                # attach a live price for HIT detection (prefer device-scoped price; fallback to last closed M1)
                lp = _get_live_price(sym_u, pinned_device)
                if lp is None:
                    lp = _last_closed_m1_price(sym_u, user_id, pinned_device, now_ms)

                # Log only if missing OR non-numeric (avoid spam)
                if lp is None:
                    try:
                       log.warning("PH1 lp_missing sym=%s dev=%s", sym_u, pinned_device)
                    except Exception:
                       pass
                else:
                     try:
                        lp_f = float(lp)
                     except Exception:
                        lp_f = None
                        try:
                           log.warning("PH1 lp_bad sym=%s dev=%s lp=%r", sym_u, pinned_device, lp)
                        except Exception:
                           pass

                     if lp_f is not None and lp_f > 0:
                         row0["last_price"] = lp_f
                         row0["live_price"] = lp_f
                     else:
                          try:
                             log.warning("PH1 lp_nonpos sym=%s dev=%s lp=%r", sym_u, pinned_device, lp)
                          except Exception:
                             pass


                # evaluate HIT/EXPIRED and cleanup if needed
                try:
                   _evaluate_alert_outcome(sym_u, snap or {}, row0, now_ms)
                except Exception:
                   pass

                # re-read after evaluation (may be deleted)
                snap = R.hgetall(snap_key)
                if not snap:
                    continue

                # ✅ FIX: correct status read (bytes + str hashes)
                try:
                    status_raw = snap.get(b"status")
                    if status_raw is None:
                        status_raw = snap.get("status")
                    status = str(_json_load_maybe(status_raw) or "active").lower()
                except Exception:
                    status = "active"

                if status in ("active", "new", "open"):
                    row = _redis_hash_to_dict(snap)
                    row["symbol"] = sym_u
                    if debug_gate and isinstance(oppt_dev_dbg, dict):
                        try:
                            row.update(oppt_dev_dbg)
                        except Exception:
                            pass

                    row.setdefault("update_tf", tfu)
                    row.setdefault("server_now_ms", now_ms)
                    # attach dev debug to each returned row (so curl/jq can see it)
                    if debug_gate and isinstance(oppt_dev_dbg, dict):
                        try:
                            row.update(oppt_dev_dbg)
                        except Exception:
                            pass

                    # keep last_price consistent in row too (helps UI + entry/exit checks)
                    try:
                        if lp is not None:
                            row["last_price"] = float(lp)
                            row["live_price"] = float(lp)
                    except Exception:
                        pass

                    _force_hydrate_m1(row.get("symbol"))
                    _run_entry_only_if_armed(row)
                    # NEW: persist entry freeze so TP/SL stops moving across polls
                    try:
                        if bool(row.get("entry_triggered")):
                           _persist_entry_meta_to_snapshot(sym_u, row)
                    except Exception:
                        pass

                    res.append(row)

        # ✅ keep only one active snapshot per symbol (prevents same symbol double-appearing)
        try:
            best: dict[str, dict] = {}

            def _rank(x: dict):
                et = 1 if bool(x.get("entry_triggered")) else 0
                sc = float(x.get("opp_score") or x.get("score") or 0.0)
                ts = int(x.get("opp_open_ts") or x.get("alert_created_ms") or 0)
                return (et, sc, ts)

            for r in res:
                s = str(r.get("symbol") or "").upper().strip()
                if not s:
                    continue
                if s not in best or _rank(r) > _rank(best[s]):
                    best[s] = r

            res = list(best.values())
        except Exception:
            pass

        return res
    # ---------- weekend rule: NO NEW opportunities ----------
    utc_weekday = datetime.now(timezone.utc).weekday()  # 0=Mon ... 5=Sat 6=Sun
    is_weekend = utc_weekday >= 5

    # Always sweep (so HIT/EXPIRED snapshots are cleaned)
    try:
        _sweep_opp_snapshots(symbols, now_ms)
    except Exception:
        pass

    if is_weekend and not (debug_force or debug_gate):
        
        # show only active snapshots + history; do NOT call predict_all (no new creation)
        history = _load_opp_history(limit=50)
        rows = _load_active_snapshots(symbols)
        # DROP null/empty symbol rows (prevents {"symbol": null} in UI)
        rows = [r for r in rows if str((r or {}).get("symbol") or "").strip()]
        payload = {"ok": True, "tf": tfu, "rows": rows, "history": history}
        

        # cache + unlock (best effort)
        if cache_key:
            try:
               R.setex(cache_key, cache_ttl_s, json.dumps(payload))
            except Exception:
               pass
        try:
            if inflight_lock_key and inflight_got_lock:
                R.delete(inflight_lock_key)
        except Exception:
            pass
       
       

        return payload


    
    # ---------- Reuse main prediction logic (need H1 + H4 because predict_all is TF-STRICT) ----------
    base_h1 = predict_all(
        tf="H1",
        symbols=symbols,
        device=device,
        x_device_id=x_device_id,
        user=user,
    )

    base_h4 = predict_all(
        tf="H4",
        symbols=symbols,
        device=device,
        x_device_id=x_device_id,
        user=user,
    )

    if not isinstance(base_h1, dict):
        _unlock_inflight()
        return {"ok": False, "reason": "predict_all_h1_not_dict"}
    if not base_h1.get("ok", True):
        _unlock_inflight()
        return base_h1

    # H4 is optional: if it fails, we still allow H1-only opps
    h4_rows = []
    if isinstance(base_h4, dict) and base_h4.get("ok", True):
        h4_rows = base_h4.get("rows") or []

    h4_by_sym: dict[str, dict] = {}
    for r in h4_rows:
        s = str((r or {}).get("symbol") or "").upper().strip()
        if s:
            h4_by_sym[s] = r

    rows_in = base_h1.get("rows") or []
  

    
    opp_rows: list[dict[str, Any]] = []
    debug_pool: list[dict[str, Any]] = []
    res = debug_pool  # alias: debug_gate uses res.append(...)
    def _push_blocked(sym: str, base_row: dict, *, stage: str, reason: str, meta: dict | None = None):
        if not debug_gate_on:
            return
        out_dbg = dict(base_row or {})
        out_dbg["symbol"] = sym
        out_dbg["debug_only"] = True
        out_dbg["status"] = "blocked"
        out_dbg["blocked_at"] = stage
        out_dbg["blocked_reason"] = reason
        if isinstance(meta, dict):
            out_dbg["blocked_meta"] = meta
        out_dbg.setdefault("update_tf", tfu)
        out_dbg.setdefault("server_now_ms", now_ms)

        # keep price visible in UI
        try:
            lp = out_dbg.get("last_price") or out_dbg.get("price")
            if isinstance(lp, (int, float)):
                out_dbg["last_price"] = float(lp)
        except Exception:
            pass

        debug_pool.append(out_dbg)


    for row0 in rows_in:
        sym = str((row0 or {}).get("symbol") or "").upper()
        if not sym:
            continue

        # ---- IMPORTANT: per-TF view merge (predict_all now returns values under row["tfs"][TF]) ----
        row_h1 = dict(row0 or {})   # frozen H1 view for gate (do NOT alias row0)
        # --- TRACE row_h1 attach (row0 alias) ---
        try:
            _bars0 = (row_h1.get("bars") or row_h1.get("ohlc") or [])
        except Exception:
            _bars0 = None

        log.error(
           "TRACE[row_h1= row0] sym=%s tf=%s row0_type=%s row0_keys=%s bars_n=%s last=%s",
           (sym or ""),
           (tf or ""),
           type(row0).__name__,
           list(row0.keys()) if isinstance(row0, dict) else None,
           (len(_bars0) if isinstance(_bars0, list) else None),
           (_bars0[-1] if isinstance(_bars0, list) and _bars0 else None),
        )
        row0_h4 = h4_by_sym.get(sym) or {}
        row_h4 = row0_h4  # already H4


        # Use H1 view as the working row (keeps the rest of this function consistent)
        row = row_h1
        # Attach request/device debug to every output row (so curl/jq can see it)
        if debug_gate_on and isinstance(oppt_dev_dbg, dict):
            try:
                row.update(oppt_dev_dbg)
            except Exception:
                pass

        # ---- attach a live-ish price for UI + hit detection ----
        lp = (
            row.get("last_price")
            or row.get("price")
            or row.get("mid")
            or row.get("lastClose")
            or row.get("last_close")
            or row.get("close")
        )
        if isinstance(lp, (int, float)):
            row["last_price"] = float(lp)
        # ==========================================================
        # DEBUG: attach SR early so later gates can show SR values
        # even if blocked earlier (DELTA, MIN_PROB, etc.)
        # ==========================================================
        if debug_gate_on and (not isinstance(row.get("sr"), dict) or not row.get("sr")):
            try:
                b, bsrc = _get_sr_bundle(sym, prefer_dev=x_device_id_hdr, return_src=True)
                b = b or {}
                row["dbg_sr_src"] = bsrc

                

                row["sr"] = b if isinstance(b, dict) else {}

                price = row.get("last_price") or row.get("price")
                p = float(price) if isinstance(price, (int, float)) else None
                h1 = (b.get("H1") or b.get("h1")) if isinstance(b, dict) else None
                if debug_gate_on:
                    row["dbg_sr_bundle_keys"] = list(b.keys()) if isinstance(b, dict) else []
                    row["dbg_sr_h1_keys"] = list((h1 or {}).keys()) if isinstance(h1, dict) else []

                if isinstance(h1, dict) and p is not None:
                    def _levels(arr):
                        out = []
                        for z in (arr or []):
                            if isinstance(z, dict) and isinstance(z.get("level"), (int, float)):
                                out.append(float(z["level"]))
                        return sorted(set(out))

                    sup_lvls = _levels(h1.get("supports") or [])
                    res_lvls = _levels(h1.get("resistances") or [])

                    ns = nr = None
                    below = [x for x in sup_lvls if x <= p]
                    if below:
                        ns = max(below)
                    above = [x for x in res_lvls if x >= p]
                    if above:
                        nr = min(above)

                    # fallback if nothing on correct side
                    if ns is None and sup_lvls:
                        ns = max(sup_lvls)
                    if nr is None and res_lvls:
                        nr = min(res_lvls)

                    # attach at top-level for gate/debug/UI
                    if isinstance(row["sr"], dict):
                        if isinstance(ns, (int, float)):
                            row["sr"]["nearest_support"] = float(ns)
                        if isinstance(nr, (int, float)):
                            row["sr"]["nearest_resistance"] = float(nr)
            except Exception:
                pass
        # --- DEBUG FORCE: emit fabricated opp candidates (no snapshot writes) ---
        if debug_force and debug_top > 0:
            thr1 = _oppt_min_move_pct(sym, "H1")
            thr4 = _oppt_min_move_pct(sym, "H4")
            



            dec = str(row.get("decision") or row.get("opp_direction") or row.get("direction") or "BUY").upper()
            s = 1 if dec in ("BUY", "UP", "LONG") else -1

            m1 = float(s) * max(thr1, 0.01) * 1.6
            m4 = float(s) * max(thr4, 0.01) * 1.6

            out = dict(row)
            out["expected_move_pct_1h"] = m1
            out["expected_move_pct_4h"] = m4

            hour_ms = 60 * 60 * 1000
            bucket_open_ts = (now_ms // hour_ms) * hour_ms

            opp_dir = "UP" if s > 0 else "DOWN"
            out["opp_id"] = f"{sym}-H1-{opp_dir}-{bucket_open_ts}"
            out["opp_direction"] = opp_dir
            out["decision"] = "BUY" if opp_dir == "UP" else "SELL"
            out["opp_confidence"] = "high"
            out["opp_horizon"] = "H1"
            out["opp_open_ts"] = bucket_open_ts
            out["opp_expire_ts"] = bucket_open_ts + hour_ms
            out["opp_min_room_h1"] = thr1
            out["opp_min_room_h4"] = thr4
            
            
            out.setdefault("update_tf", tfu)
            out.setdefault("server_now_ms", now_ms)

            # attach entry + signal in debug too (helps UI)
            _run_entry_only_if_armed(out)
            
            
            # ---------------------------------------------------------
            # DEBUG_PERSIST: write the fabricated opp into live snapshot
            # so UI (no-debug) can see it on the next poll.
            # ---------------------------------------------------------
            if debug_persist:
                try:
                    
                    out["debug_force"] = True
                    out["debug_force_ts_ms"] = int(now_ms)
                    out["status"] = "active"
                    # --- normalize reason fields so UI/debug show same reason key ---
                    try:
                        if out.get("opp_reason") in (None, "", "missing"):
                            eg = out.get("entry_gate")
                            gate_reason = eg.get("reason") if isinstance(eg, dict) else None

                            out["opp_reason"] = (
                                gate_reason
                                or out.get("signal_reason")
                                or out.get("structure_reason")
                                or (out.get("reasons")[0] if isinstance(out.get("reasons"), list) and out.get("reasons") else None)
                            )

                        if out.get("reason") in (None, "", "missing"):
                            out["reason"] = out.get("opp_reason")
                    except Exception:
                        pass


                    # Persist into the same snapshot store used by normal path
                    _freeze_or_snapshot_opp(sym, out, now_ms)

                    # Make debug snapshots self-cleaning
                    try:
                        snap_key = _opp_snapshot_key(sym, out.get("opp_direction") or out.get("decision") or "UP")
                        ttl_sec = int(os.getenv("XTL_DEBUG_OPP_TTL_SEC", "0"))  # 2 hours default
                        if ttl_sec > 0:
                            R.expire(snap_key, ttl_sec)
                        
                    except Exception:
                        pass
                except Exception:
                    pass

            debug_pool.append(out)
            continue

            # --- DEBUG_FORCE: attach entry_context chart overlay so UI can render ---
            try:
                sr0 = out.get("sr")
                if isinstance(sr0, dict) and sr0:
                    atr0 = out.get("atr") or out.get("atr14") or out.get("atr_h1")
                    px0 = out.get("basis_price") or out.get("last_price") or out.get("price") or out.get("mid")

                    ent = _pick_entry_sr_levels(sr0, px0, top_n=4, atr=atr0)
                    if isinstance(ent, dict) and ent:
                        out.update(ent)

                    ch = out.get("chart")
                    if not isinstance(ch, dict):
                        ch = {}
                        out["chart"] = ch

                    ov = ch.get("overlays")
                    if not isinstance(ov, dict):
                        ov = {}
                        ch["overlays"] = ov

                    ec = ov.get("entry_context")
                    if not isinstance(ec, dict):
                        ec = {}
                        ov["entry_context"] = ec

                    ec["px"] = px0
                    ec["entry_support"] = ent.get("entry_support")
                    ec["entry_support_tf"] = ent.get("entry_support_tf")
                    ec["entry_support_kind"] = ent.get("entry_support_kind")
                    ec["entry_resistance"] = ent.get("entry_resistance")
                    ec["entry_resistance_tf"] = ent.get("entry_resistance_tf")

                    ec["entry_support_near_levels"] = ent.get("entry_support_near_levels") or []
                    ec["entry_support_major_levels"] = ent.get("entry_support_major_levels") or []
                    ec["entry_resistance_near_levels"] = ent.get("entry_resistance_near_levels") or []
                    ec["entry_resistance_major_levels"] = ent.get("entry_resistance_major_levels") or []
                    ec["entry_support_flipped_levels"] = ent.get("entry_support_flipped_levels") or []
                    ec["entry_resistance_flipped_levels"] = ent.get("entry_resistance_flipped_levels") or []
            except Exception:
                pass

           



      

        # Extract H1/H4 expected move (%)
        # H1 uses row_h1 (merged), H4 uses row_h4 (merged)
        m1 = _get_float(
            row_h1,
            "expected_move_pct_1h", "expected_move_pct", "move_pct_1h", "move_pct",
        )
        m4 = _get_float(
            row_h4,
            "expected_move_pct_4h", "expected_move_pct", "move_pct_4h", "move_pct",
        )

        s1 = _sign(m1)
        s4 = _sign(m4)
        # ---- REQUIRED: thresholds must exist in normal path (not just debug_force) ----
        thr1 = _oppt_min_move_pct(sym, "H1")
        thr4 = _oppt_min_move_pct(sym, "H4")

        # ---- DEBUG: expose gate inputs to the API response ----
        if debug_gate:
            row["dbg_m1"] = m1
            row["dbg_m4"] = m4
            row["dbg_thr1"] = thr1
            row["dbg_thr4"] = thr4
            row["dbg_s1"] = s1
            row["dbg_s4"] = s4


        # --- Enrich H1 features for scoring/UI (optional) ---

        extra_h1 = row.get("extra_h1") or {}
        feats_h1 = extra_h1.get("features") if isinstance(extra_h1.get("features"), dict) else extra_h1
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
        # 1) If ACTIVE snapshot exists, keep it visible until HIT/EXPIRED
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

                # re-read after evaluation (may be deleted)
                snap = R.hgetall(snap_key)
                if not snap:
                    continue

                try:
                   status_raw = snap.get(b"status")
                   if status_raw is None:
                        status_raw = snap.get("status")
                   status = str(_json_load_maybe(status_raw) or "active").lower()
                except Exception:
                   status = "active"


                if status in ("active", "new", "open"):
                    has_active_snapshot = True
                    active_snap_row = _redis_hash_to_dict(snap)
                    active_snap_row["symbol"] = sym
                    break
        except Exception:
            has_active_snapshot = False
            active_snap_row = None

        if has_active_snapshot and active_snap_row:
            # Attach request/device debug to snapshot row too
            if debug_gate_on and isinstance(oppt_dev_dbg, dict):
                try:
                    active_snap_row.update(oppt_dev_dbg)
                except Exception:
                    pass
            active_snap_row.setdefault("update_tf", tfu)
            active_snap_row.setdefault("server_now_ms", now_ms)

            lp2 = row.get("last_price") or row.get("price")
            if isinstance(lp2, (int, float)):
                active_snap_row["last_price"] = float(lp2)

            _run_entry_only_if_armed(active_snap_row)
            # NEW: persist entry freeze so TP/SL stops moving across polls
            try:
                if bool(active_snap_row.get("entry_triggered")):
                   _persist_entry_meta_to_snapshot(sym, active_snap_row)
            except Exception:
                pass

            # ---- ensure SR + entry SR fields exist even for ACTIVE SNAPSHOT rows ----
            try:
                # active_snap_row may already carry "sr" (or may not)
                sr0 = active_snap_row.get("sr")

                if not isinstance(sr0, dict) or not sr0:
                    sr_bundle, bsrc = _get_sr_bundle(sym, prefer_dev=x_device_id_hdr, return_src=True)
                    if isinstance(sr_bundle, dict) and sr_bundle:
                        active_snap_row["sr"] = sr_bundle
                        if debug_gate_on:
                            active_snap_row["dbg_sr_src"] = f"active_snap|{bsrc}"
                    else:
                        if debug_gate_on:
                            active_snap_row["dbg_sr_src"] = f"active_snap|missing|{bsrc}"
                else:
                    if debug_gate_on and "dbg_sr_src" not in active_snap_row:
                        active_snap_row["dbg_sr_src"] = "active_snap|carried"

                sr0 = active_snap_row.get("sr")
                if isinstance(sr0, dict) and sr0:
                    atr0 = None
                    try:
                        atr0 = (
                             active_snap_row.get("atr")
                             or active_snap_row.get("atr14")
                             or active_snap_row.get("atr_h1")
                             or row.get("atr")
                             or row.get("atr14")
                             or row.get("atr_h1")
                        )
                    except Exception:
                        atr0 = None
                    px0 = (
                        active_snap_row.get("basis_price")
                        or active_snap_row.get("last_price")
                        or active_snap_row.get("price")
                        or active_snap_row.get("mid")
                        or row.get("basis_price")
                        or row.get("last_price")
                        or row.get("price")
                        or row.get("mid")
                    )

                    ent = _pick_entry_sr_levels(sr0, px0, top_n=4, atr=atr0)
                    if isinstance(ent, dict) and ent:
                        active_snap_row.update(ent)

                    # optional: also expose in chart overlay for UI labels
                    try:
                        ch = active_snap_row.get("chart")
                        if not isinstance(ch, dict):
                            ch = {}
                            active_snap_row["chart"] = ch

                        ov = ch.get("overlays")
                        if not isinstance(ov, dict):
                            ov = {}
                            ch["overlays"] = ov

                        ec = ov.get("entry_context")
                        if not isinstance(ec, dict):
                            ec = {}
                            ov["entry_context"] = ec
                        ec["px"] = px0
                        # single picks
                        ec["entry_support"] = ent.get("entry_support")
                        ec["entry_support_tf"] = ent.get("entry_support_tf")
                        ec["entry_support_kind"] = ent.get("entry_support_kind")

                        ec["entry_resistance"] = ent.get("entry_resistance")
                        ec["entry_resistance_tf"] = ent.get("entry_resistance_tf")
                        ec["entry_resistance_kind"] = ent.get("entry_resistance_kind")

                        # lists (near + major)
                        ec["entry_support_near_levels"] = ent.get("entry_support_near_levels") or []
                        ec["entry_support_major_levels"] = ent.get("entry_support_major_levels") or []

                        ec["entry_resistance_near_levels"] = ent.get("entry_resistance_near_levels") or []
                        ec["entry_resistance_major_levels"] = ent.get("entry_resistance_major_levels") or []
                        # NEW: flipped lists
                        ec["entry_support_flipped_levels"] = ent.get("entry_support_flipped_levels") or []
                        ec["entry_resistance_flipped_levels"] = ent.get("entry_resistance_flipped_levels") or []
                       



                    except Exception as e:
                        if debug_gate:
                            active_snap_row["dbg_entry_context_exc"] = f"{type(e).__name__}:{e}"

            except Exception as e:
                if debug_gate_on:
                    active_snap_row["dbg_entry_sr_exc"] = f"{type(e).__name__}:{e}"
            # ensure sym_u exists in this active-snapshot branch (prevents UnboundLocalError)
            sym_u = str(active_snap_row.get("symbol") or active_snap_row.get("sym") or sym or "").upper().strip()
            if not sym_u:
                sym_u = str(sym or "").upper().strip()
            try:
                allowed, gate_meta = _zone_reversal_gate(
                    sym=sym_u,
                    direction=("BUY" if str(active_snap_row.get("opp_direction") or active_snap_row.get("decision") or "").upper() in ("UP", "BUY") else "SELL"),
                    row_h1=row_h1,
                    sr=_sr_gate_view(active_snap_row.get("sr")),
                    now_ms=now_ms,
                    tf_tag="H1",
                    pinned_device=dev_for_gate,
                    x_device_id=dev_for_gate,
                    debug_gate=bool(debug_gate_on),
                )
                gm = gate_meta if isinstance(gate_meta, dict) else {"reason": "gate_meta_not_dict"}
                # (optional but useful) reflect allow/deny at row level too
                # Respect explicit gm["blocked"] if gate provided it (needed for soft-discard states)
                if "blocked" in gm:
                    gm["blocked"] = bool(gm.get("blocked"))
                else:
                    gm["blocked"] = (not bool(allowed))


                # callsite marker (debug only)
                if debug_gate_on:
                    gm["__callsite_marker__"] = "AFTER_ZONE_GATE_CALL"
                    gm["__callsite_debug_gate__"] = True

                active_snap_row["entry_gate"] = gm

            except Exception as e:
                active_snap_row["entry_gate"] = {"reason": "ZONE_GATE_EXCEPTION", "blocked": True, "exc_type": type(e).__name__, "exc": str(e)}


            opp_rows.append(active_snap_row)
            continue

        # Weekend rule: do NOT open new ones (but can show existing above)
        if is_weekend and not (debug_force or debug_gate):
            continue

        # --------------------------------------------------
        # 2) New opportunity gate: H1 room must pass threshold
        # --------------------------------------------------
        if not loose:
            if (not isinstance(m1, (int, float))) or s1 == 0 or abs(m1) < thr1:
                _push_blocked(sym, row, stage="H1_ROOM", reason=f"m1={m1} thr1={thr1}")
                continue
        # --------------------------------------------------
        # 2b) New opportunity gate: min_prob by TF (from meta.common.oppt_tf)
        # --------------------------------------------------
        if not loose:
            try:
                cfg_tf = (_get_meta(sym) or {}).get("oppt_tf") or {}
                cfg_h1 = cfg_tf.get("H1") if isinstance(cfg_tf, dict) else None
                min_prob = float(cfg_h1.get("min_prob")) if isinstance(cfg_h1, dict) and isinstance(cfg_h1.get("min_prob"), (int, float)) else None
            except Exception:
                min_prob = None

            if min_prob is not None:
                p = _get_float(row_h1, "p_up", "prob_up")
                if (not isinstance(p, (int, float))) or float(p) < float(min_prob):
                    _push_blocked(sym, row, stage="MIN_PROB", reason=f"p={p} min_prob={min_prob}")
                    continue


        # --- prediction delta gate (optional anti-spam) ---
        delta_pct = None
        delta_thr = _delta_thr_h1(sym, thr1)
        if isinstance(m1, (int, float)):
            try:
                key = PRED_DELTA_KEY_FMT % sym
                prev = R.get(key)
                if isinstance(prev, (bytes, bytearray)):
                    prev = prev.decode("utf-8", "ignore")
                if prev is not None:
                    try:
                        prev_val = float(prev)
                        delta_pct = abs(float(m1) - prev_val)
                    except (TypeError, ValueError):
                        delta_pct = None
                R.set(key, f"{float(m1):.6f}", ex=90 * 60)
            except Exception:
                delta_pct = None

        # STRICT behavior: block only in normal mode.
        # DEBUG behavior: report, but do NOT block progression.
        if (not loose) and (delta_pct is not None) and (delta_pct < delta_thr):
            _push_blocked(
                sym,
                row,
                stage="DELTA",
                reason=f"delta={delta_pct:.3f} thr={delta_thr:.3f}",
                meta={"delta_pct": delta_pct, "delta_thr": delta_thr, "m1": float(m1)},
            )

            if not debug_gate_on:
                continue



        # --------------------------------------------------
        # 3) H4 confirmation logic
        # --------------------------------------------------
        opp_dir = "UP" if s1 > 0 else "DOWN"
        opp_conf = "medium"
        h4_agree: bool | None = None

        # ------------------------------------------------------------
        # H4 confirmation / conflict handling
        # Policy:
        #   - If H4 agrees with H1: boost confidence (high/medium based on H4 strength)
        #   - If H4 conflicts:
        #       * strict mode (loose=0): BLOCK only when H4 is a *strong* opposite signal (abs(m4) >= thr4)
        #       * otherwise: ALLOW but downgrade confidence (medium if H1 is very strong, else low)
        #   - If no usable H4 signal: confidence based on H1 strength
        # ------------------------------------------------------------
        if isinstance(m4, (int, float)) and s4 != 0:
            if s1 == s4:
                # H4 agrees with H1
                h4_agree = True
                opp_conf = "high" if abs(m4) >= thr4 else "medium"
            else:
                # H4 conflicts with H1
                h4_agree = False

                # strict mode: only block if H4 is a strong opposite signal
                strong_h4_opp = abs(m4) >= max(thr4 * 1.5, 0.60)

                if (not loose) and strong_h4_opp:
                    _push_blocked(
                       sym,
                       row,
                       stage="H4_CONFLICT",
                       reason=f"m1={m1} m4={m4} thr4={thr4} (strong H4 opp)",
                       meta={"m1": m1, "m4": m4, "thr1": thr1, "thr4": thr4, "strong_h4_opp": True},
                    )
                    # DO NOT continue — keep evaluating (ZONE_GATE will decide quality)
                    opp_conf = "low"                     
                 
                else:
                     # H4 conflicts but not "strong" -> allow with downgrade
                     opp_conf = "medium" if abs(m1) >= (1.5 * thr1) else "low"

        else:       
            # No usable H4 signal => confidence based on H1 strength only
            opp_conf = "high" if abs(m1) >= 1.5 * thr1 else "medium"

        opp_score = _compute_opp_score(sym, row, m1, thr1)


        if (not loose) and opp_score < OPP_SCORE_MIN:
            if debug_top > 0 and (debug_force or loose):
                out_dbg = dict(row)
                hour_ms = 60 * 60 * 1000
                bucket_open_ts = (now_ms // hour_ms) * hour_ms
                opp_open_ts = bucket_open_ts
                opp_expire_ts = bucket_open_ts + hour_ms
                out_dbg["opp_id"] = f"{sym}-H1-{opp_dir}-{opp_open_ts}"
                out_dbg["opp_direction"] = opp_dir
                out_dbg["decision"] = "BUY" if opp_dir == "UP" else "SELL"
                out_dbg["opp_confidence"] = opp_conf
                out_dbg["opp_horizon"] = "H1"
                out_dbg["opp_h4_agree"] = h4_agree
                out_dbg["opp_open_ts"] = opp_open_ts
                out_dbg["opp_expire_ts"] = opp_expire_ts
                out_dbg["opp_min_room_h1"] = thr1
                out_dbg["opp_min_room_h4"] = thr4
                out_dbg["opp_score"] = round(float(opp_score), 1)
                out_dbg["debug_only"] = True
                out_dbg["status"] = "debug"
                out_dbg["opp_reason"] = out_dbg.get("opp_reason") or "debug candidate (below OPP_SCORE_MIN)"
                out_dbg.setdefault("update_tf", tfu)
                out_dbg.setdefault("server_now_ms", now_ms)

                _attach_entry_1m(out_dbg)
                _set_signal_from_entry(out_dbg)

                debug_pool.append(out_dbg)
            _push_blocked(sym, row, stage="OPP_SCORE", reason=f"opp_score={opp_score} min={OPP_SCORE_MIN}")
            if not debug_gate:
                continue
            # debug_gate=1 -> do NOT stop here; keep going so ZONE_GATE can run and report bars/device

        
        # --------------------------------------------------
        # FINAL STRATEGY GATE: ZONE + SECOND TAP + REVERSAL
        # --------------------------------------------------
        if loose:
            allowed = True
            gate_meta = {
                "reason": "LOOSE_BYPASS",
                "confidence": opp_conf,
                "zone": None,
            }
            try:
                bsrc = row_h1.get("bars") or row_h1.get("ohlc") or []
                row["bars_h1"] = bsrc if isinstance(bsrc, list) else []
            except Exception:
                row["bars_h1"] = []
        else:
            # ---- ensure ATR exists for zone gate + SR zone rendering ----
            try:
                atr = (
                    row.get("atr_1h")
                    or row.get("atr")
                    or row.get("atr14")
                    or row.get("atr14_1h")
                    or row_h1.get("atr_1h")
                    or row_h1.get("atr")
                    or row_h1.get("atr14")
                    or row_h1.get("atr14_1h")
                )
                atr = float(atr) if isinstance(atr, (int, float)) else None
            except Exception:
                atr = None

            # Pull H1 snap once (also used to attach bars for the gate)
            sym_u = (sym or "").upper().strip()
            snap_any = None
            snap_tf = None

            # 1) device-scoped snaps (prefer H1, then M15, then M5, then M1)
            x_device_id_hdr = (x_device_id or "").strip()
            if debug_gate_on:
                row["dbg_hdr_x_device_id"] = x_device_id_hdr
                row["dbg_hdr_present"] = bool(x_device_id_hdr)

            if x_device_id_hdr:
                x_device_id = x_device_id_hdr

            dev_for_snap = str((x_device_id_hdr or x_device_id or effective_device or "")).strip()
            if debug_gate_on:
                row["dbg_dev_for_snap"] = dev_for_snap
                row["dbg_sym_u"] = sym_u
                row["dbg_snap_try_tfs"] = ["H1", "M15", "M5", "M1"]
            if dev_for_snap and sym_u:
                for tf_try in ("H1",):
                    try:
                        snap_key = f"xtl:ohlc:snap:{dev_for_snap}:{sym_u}:{tf_try}"

                        raw = R.get(snap_key)

                        if debug_gate_on:
                            row["dbg_R_id"] = id(R)
                            row["dbg_snap_key_try"] = snap_key
                            row["dbg_snap_raw_len"] = (len(raw) if isinstance(raw, str) else (len(raw) if raw else 0))
                        if debug_gate_on:
                            print(f"[DBG_SNAP] hdr={x_device_id_hdr!r} x_device_id={x_device_id!r} effective={effective_device!r} dev_for_snap={dev_for_snap!r} key={snap_key} raw_len={(len(raw) if raw else 0)}", flush=True)



                        if debug_gate_on and snap_any is None:
                            row["dbg_snap_key_try"] = snap_key
                            row["dbg_snap_raw_len"] = len(raw) if raw else 0

                        s = _json_load_twice(raw) if raw else None
                        bars = s.get("bars") if isinstance(s, dict) else None

                        if debug_gate_on and snap_any is None:
                            row["dbg_snap_tf"] = tf_try
                            row["dbg_snap_bars_len"] = len(bars) if isinstance(bars, list) else 0
                        if isinstance(bars, list) and bars:
                            snap_any = s
                            snap_tf = tf_try
                            break
                    except Exception:
                        continue

            

            # 3) broker-direct fallback (H1 only) when snaps are missing
            # This fixes cases like XAUUSD where tick price exists but ohlc snaps are not being published.
            if snap_any is None and sym_u:
                try:
                    # Pull a reasonable tail so ATR14 + tap logic works
                    agent_rows = _broker_bars_sync(sym_u, "H1", limit=240) or []
                    bars_b: list[dict] = []

                    # _broker_bars_sync rows are typically {t_open_ms,t_close_ms,o,h,l,c,complete}
                    for b in agent_rows:
                        if not isinstance(b, dict):
                            continue
                        try:
                            bars_b.append(
                                {
                                    "t_open_ms": int(b.get("t_open_ms") or b.get("tOpen") or b.get("t") or 0),
                                    "t_close_ms": int(b.get("t_close_ms") or b.get("tClose") or b.get("t") or 0),
                                    "o": float(b["o"]),
                                    "h": float(b["h"]),
                                    "l": float(b["l"]),
                                    "c": float(b["c"]),
                                    "complete": True,
                                }
                            )
                        except Exception:
                            continue

                    if bars_b:
                        snap_any = {"bars": bars_b}
                        snap_tf = "H1:broker"
                except Exception:
                    pass

            
            # Attach bars so _zone_reversal_gate can compute taps/reversal reliably
            # Attach H1 bars so _zone_reversal_gate can compute taps/reversal reliably
            # Source order (PERMANENT):
            #   1) device-scoped snap   xtl:ohlc:snap:{dev}:{sym}:H1
            #   2) global latest        xtl:ohlc:latest:{sym}:H1
            #   3) otherwise -> no_h1_bars (do NOT evaluate gate on stale data)
            try:
                dev = str(pinned_device or x_device_id or row.get("device") or row.get("device_id") or "").strip()

                k_dev = f"xtl:ohlc:snap:{dev}:{sym_u}:H1" if dev else ""
                k_latest = f"xtl:ohlc:latest:{sym_u}:H1"

                raw = _snap_get_raw_json(k_dev) if k_dev else None
                src = "dev_snap" if raw else None

                if not raw:
                    raw = _snap_get_raw_json(k_latest)
                    if raw:
                        src = "latest"

                js = _json_load_twice(raw) if raw else None

                bars = None
                if isinstance(js, dict):
                    bars = js.get("bars") or js.get("ohlc") or js.get("data")
                elif isinstance(js, list):
                    bars = js

                if not isinstance(bars, list) or len(bars) < 2:
                    if debug_gate:
                        row["dbg_h1_src"] = "missing"
                        row["dbg_h1_key_dev"] = k_dev
                        row["dbg_h1_key_latest"] = k_latest

                    row["entry_gate"] = {
                        "reason": "no_h1_bars",
                        "bars_n": 0,
                        "stage": "ZONE_GATE",
                        "dev_used": dev,
                        "stage": "ZONE_GATE",
                        "blocked": True,
                    }
                    continue

                nb = _normalize_snap_bars_to_ms(bars, 60 * 60 * 1000)  # H1

                def _tc(b):
                    v = b.get("t_close_ms") or b.get("tClose") or b.get("t") or 0
                    v = int(float(v)) if v is not None else 0
                    if 0 < v < 10_000_000_000:
                        v *= 1000
                    return v

                nb = [
                    b for b in nb
                    if isinstance(b, dict) and all(k in b for k in ("o", "h", "l", "c"))
                ]
                nb.sort(key=_tc)

                row_h1["bars"] = nb

                if debug_gate_on:
                    row["dbg_h1_src"] = src
                    row["dbg_h1_key_dev"] = k_dev
                    row["dbg_h1_key_latest"] = k_latest
                    row["dbg_h1_bars_n"] = len(nb)
                    lastb = nb[-1]
                    row["dbg_h1_last_close_ms"] = _tc(lastb)
                    row["dbg_h1_last_c"] = float(lastb.get("c"))

            except Exception:
                pass

            # ALSO expose closed H1 bars on the opp row so _evaluate_alert_outcome can run structure TP/exit
            try:
                bsrc = row_h1.get("bars") or row_h1.get("ohlc") or []
                row["bars_h1"] = bsrc if isinstance(bsrc, list) else []
            except Exception:
                row["bars_h1"] = []


            if atr is None:
                # 1) Prefer bars already attached to row_h1 (most reliable)
                try:
                    bsrc = row_h1.get("bars") if isinstance(row_h1, dict) else None
                    if isinstance(bsrc, list) and len(bsrc) >= 20:
                        atr = _atr14_from_hlc(bsrc)
                except Exception:
                    atr = None

            if atr is None:
                # 2) Fallback: device snap from Redis
                try:
                    dev = str(pinned_device or x_device_id or row.get("device") or row.get("device_id") or "").strip()
                    if dev:
                        raw = R.get(f"xtl:ohlc:snap:{dev}:{sym_u}:H1")
                        js = _json_load_twice(raw) if raw else None
                        if isinstance(js, dict):
                            h1_bars = js.get("bars") or []
                            if isinstance(h1_bars, list) and len(h1_bars) >= 20:
                                h1_bars = _normalize_snap_bars_to_ms(h1_bars, 60 * 60 * 1000)
                                atr = _atr14_from_hlc(h1_bars)
                except Exception:
                    atr = None


            if atr is not None:
                # write to all common names (covers whatever _zone_reversal_gate expects)
                for k in ("atr_1h", "atr", "atr14", "atr14_1h"):
                    row[k] = atr
                    row_h1[k] = atr

                # IMPORTANT: inject into the exact place _zone_reversal_gate reads
                try:
                    extra = row_h1.get("extra_h1")
                    if not isinstance(extra, dict):
                        extra = {}
                    feats = extra.get("features")
                    if not isinstance(feats, dict):
                        feats = {}
                    feats["feat_atr"] = float(atr)
                    extra["features"] = feats
                    row_h1["extra_h1"] = extra
                except Exception:
                    pass

                # ALSO provide ATR in bp (basis points) in case gate expects it
                try:
                    px = float(
                        row.get("last_price")
                        or row.get("price")
                        or row.get("basis_price")
                        or row.get("mid")
                        or 0.0
                    )
                except Exception:
                    px = 0.0

                atr_bp = None
                if px > 0:
                    atr_bp = (atr / px) * 10000.0
                    for k in ("feat_atr_bp", "atr_bp", "atr14_bp", "atr_1h_bp"):
                        row[k] = atr_bp
                        row_h1[k] = atr_bp

                # ==========================================================
                # DEBUG: ATR + BARS VISIBILITY (Point-1)
                # ==========================================================
                if debug_gate_on:
                    row["dbg_h1_bars_n"] = len(row_h1.get("bars") or [])
                    row["dbg_atr_1h"] = atr
                    row["dbg_atr_src"] = (
                        "from_row_fields"
                        if (
                            row.get("atr_1h")
                            or row.get("atr")
                            or row.get("atr14")
                            or row.get("atr14_1h")
                        )
                        else "computed_from_bars"
                    )
                    if atr_bp is not None:
                        row["dbg_atr_bp"] = atr_bp

            # ---- ensure SR exists for zone gate ----
            if not isinstance(row.get("sr"), dict) or not row.get("sr"):
                try:
                    sr_bundle, bsrc = _get_sr_bundle(
                        sym,
                        prefer_dev=(dev or pinned_device or x_device_id_hdr),
                        return_src=True,
                    )
                    if isinstance(sr_bundle, dict) and sr_bundle:
                        row["sr"] = sr_bundle
                    if debug_gate_on:
                        row["dbg_sr_src"] = bsrc if (isinstance(sr_bundle, dict) and sr_bundle) else f"missing|{bsrc}"
                except Exception as _e:
                    if debug_gate_on:
                        row["dbg_sr_src"] = f"exc|{type(_e).__name__}"
            
            # ---- after SR is attached to row ----
            try:
                sr0 = row.get("sr")
                if isinstance(sr0, dict) and sr0:
                    atr0 = None
                    try:
                         atr0 = (
                              active_snap_row.get("atr")
                              or active_snap_row.get("atr14")
                              or active_snap_row.get("atr_h1")
                              or row.get("atr")
                              or row.get("atr14")
                              or row.get("atr_h1")
                         )
                    except Exception:
                         atr0 = None
  
                    px0 = (
                         active_snap_row.get("basis_price")
                         or active_snap_row.get("last_price")
                         or active_snap_row.get("price")
                         or active_snap_row.get("mid")
                         or row.get("basis_price")
                         or row.get("last_price")
                         or row.get("price")
                         or row.get("mid")
                    )
 
                    ent = _pick_entry_sr_levels(sr0, px0, top_n=4, atr=atr0)
                    if isinstance(ent, dict) and ent:
                        row.update(ent)

                    # Also pass into chart overlay for UI labels (robust to bad chart shape)
                    try:
                        ch = row.get("chart")
                        if not isinstance(ch, dict):
                            ch = {}
                            row["chart"] = ch

                        ov = ch.get("overlays")
                        if not isinstance(ov, dict):
                            ov = {}
                            ch["overlays"] = ov

                        ec = ov.get("entry_context")
                        if not isinstance(ec, dict):
                            ec = {}
                            ov["entry_context"] = ec
                        ec["px"] = px0

                        ec["entry_support"] = ent.get("entry_support")
                        ec["entry_support_tf"] = ent.get("entry_support_tf")
                        ec["entry_support_kind"] = ent.get("entry_support_kind")
                        ec["entry_resistance"] = ent.get("entry_resistance")
                        ec["entry_resistance_tf"] = ent.get("entry_resistance_tf")

                        # lists (near + major)
                        ec["entry_support_near_levels"] = ent.get("entry_support_near_levels") or []
                        ec["entry_support_major_levels"] = ent.get("entry_support_major_levels") or []

                        ec["entry_resistance_near_levels"] = ent.get("entry_resistance_near_levels") or []
                        ec["entry_resistance_major_levels"] = ent.get("entry_resistance_major_levels") or []
                        # NEW: flipped lists
                        ec["entry_support_flipped_levels"] = ent.get("entry_support_flipped_levels") or []
                        ec["entry_resistance_flipped_levels"] = ent.get("entry_resistance_flipped_levels") or []
                        


                    except Exception as e:
                        if debug_gate_on:
                            row["dbg_entry_context_exc"] = f"{type(e).__name__}:{e}"

            except Exception as e:
                if debug_gate_on:
                    row["dbg_entry_sr_exc"] = f"{type(e).__name__}:{e}"


            # ---------------- DEBUG: what the zone gate actually sees ----------------
            
            sr_for_gate = _sr_gate_view(row.get("sr"))
            if debug_gate_on:
                b0 = None
                if isinstance(row_h1, dict):
                    b0 = row_h1.get("bars") or row_h1.get("ohlc") or []

                row["dbg_gate_bars_n"] = len(b0) if isinstance(b0, list) else 0
                if isinstance(b0, list) and b0 and isinstance(b0[-1], dict):
                    z = b0[-1]
                    row["dbg_gate_bar_keys"] = sorted(list(z.keys()))[:40]
                    t_ms = z.get("t_close_ms") or z.get("tClose") or z.get("t_close") or z.get("ts")
                    t_sec = z.get("t") or z.get("time")

                    if t_ms is None and t_sec is not None:
                        try:
                            t_ms = int(float(t_sec) * 1000.0)
                        except Exception:
                            t_ms = None

                    row["dbg_gate_last_t_close_ms"] = t_ms
                    row["dbg_gate_last_t_close_sec"] = t_sec

            
            
            # Single source of truth: use effective_device computed earlier
            dev_for_gate = str((x_device_id or effective_device or "")).strip()

            # ---- HARD FALLBACK: if still empty, recover from known values (NO request here) ----
            if not dev_for_gate:
                dev_for_gate = str(x_device_id or "").strip()

            # ---- CHANGE 2: if still empty, recover from leader / registered devices ----
            if not dev_for_gate:
                try:
                    uid = _uid_from_user(user)
                except Exception:
                    uid = None

                if (not dev_for_gate) and uid and R is not None:
                    # 1) leader device (best)
                    try:
                        leader = _json_load_twice(R.get(f"xtl:user:{uid}:trend:leader")) or {}
                        if isinstance(leader, dict):
                            dev_for_gate = str(
                                leader.get("device_id") or leader.get("id") or leader.get("device") or ""
                            ).strip()
                        elif isinstance(leader, str):
                            dev_for_gate = leader.strip()
                    except Exception:
                        pass

                    # 2) any registered device (fallback)
                    if not dev_for_gate:
                        try:
                            ds = list(R.smembers(f"xtl:user:{uid}:devices") or [])
                            if ds:
                                d0 = ds[0]
                                if isinstance(d0, (bytes, bytearray)):
                                    d0 = d0.decode("utf-8", "ignore")
                                dev_for_gate = str(d0).strip()
                        except Exception:
                            pass

            # keep pinned_device consistent for any later logic
            if dev_for_gate and not pinned_device:
                pinned_device = dev_for_gate

            # (optional) keep x_device_id_hdr only for debug visibility
            x_device_id_hdr = str(x_device_id or "").strip()



            if debug_gate_on:
                row["dbg_pinned_device"] = str(pinned_device or "").strip()
                row["dbg_x_device_id_hdr"] = x_device_id_hdr
                row["dbg_dev_for_gate"] = str(dev_for_gate or "").strip()
                row["dbg_effective_device"] = str(effective_device or "").strip()
                row["dbg_auth_ok"] = bool(uid_for_entry) or bool(effective_device)
            
            # --- ensure H1 bars exist for zone gate (rehydrate from device-scoped store) ---
            try:
                b0 = row_h1.get("bars") or row_h1.get("ohlc") or []
            except Exception:
                b0 = []

            bad_shape = True
            if isinstance(b0, list) and len(b0) >= 2 and isinstance(b0[-1], dict):
                # require at least H/L/C (and ideally O)
                bad_shape = not all(k in b0[-1] for k in ("h", "l", "c"))

            if (not isinstance(b0, list)) or (len(b0) < 2) or bad_shape:
                try:
                    
                    # --------- NEW: fallback to latest-pointer device if this device has no H1 snap ----------
                    dev_h1 = str(dev_for_gate or "").strip()

                    if R is not None and sym_u:
                       try:
                           # If this device has no H1 snap key, try latest pointer
                           if dev_h1:
                               k = f"xtl:ohlc:snap:{dev_h1}:{sym_u}:H1"
                               if not R.exists(k):
                                   ptr = R.get(f"xtl:ohlc:latest:{sym_u}:H1")
                                   if isinstance(ptr, (bytes, bytearray)):
                                       ptr = ptr.decode("utf-8", "ignore")
                                   ptr = str(ptr or "").strip()
                                   if ptr:
                                       dev_h1 = ptr
                                       if debug_gate:
                                           row["dbg_h1_ptr_used"] = dev_h1
                           else:
                                # No dev_for_gate at all -> try latest pointer directly
                                ptr = R.get(f"xtl:ohlc:latest:{sym_u}:H1")
                                if isinstance(ptr, (bytes, bytearray)):
                                    ptr = ptr.decode("utf-8", "ignore")
                                ptr = str(ptr or "").strip()
                                if ptr:
                                    dev_h1 = ptr
                                    if debug_gate:
                                        row["dbg_h1_ptr_used"] = dev_h1
                       except Exception:
                           pass
                    bars_h1 = _get_closed_h1_bars(sym_u, dev_h1) if dev_h1 else []
                    if debug_gate_on:
                        row["dbg_h1_bars_n"] = len(bars_h1) if isinstance(bars_h1, list) else 0
                        row["dbg_attach_bars_tf"] = "H1" if row["dbg_h1_bars_n"] >= 2 else None
                        row["dbg_h1_dev_used"] = dev_h1

                    bars_h1 = bars_h1 if isinstance(bars_h1, list) else []
                    if len(bars_h1) >= 2:
                        row_h1["bars"] = bars_h1
                        if debug_gate_on:
                            row["dbg_h1_bars_src"] = "rehydrated:_get_closed_h1_bars"
                            row["dbg_h1_bars_n2"] = len(bars_h1)
                    else:
                        if debug_gate_on:
                            row["dbg_h1_bars_src"] = "rehydrated_empty"
                            row["dbg_h1_bars_n2"] = len(bars_h1)
                except Exception:
                    if debug_gate_on:
                        row["dbg_h1_bars_src"] = "rehydrate_exception"
            
            # --- REFRESH STALE H1 BARS (even if shape is valid) ---
            try:
                tf_ms = 60 * 60 * 1000
                bcur = row_h1.get("bars") or row_h1.get("ohlc") or []
                bcur = bcur if isinstance(bcur, list) else []

                def _tc(b):
                    v = b.get("t_close_ms") or b.get("tClose") or b.get("t") or 0
                    try:
                        v = int(float(v)) if v is not None else 0
                    except Exception:
                        v = 0
                    if 0 < v < 10_000_000_000:
                        v *= 1000
                    return v

                last_close_ms = _tc(bcur[-1]) if (bcur and isinstance(bcur[-1], dict)) else 0
                # pick the SAME device we used for rehydration (pointer-aware)
                dev_h1_used = None
                try:
                    dev_h1_used = row.get("dbg_h1_dev_used") or row.get("dbg_h1_ptr_used")
                except Exception:
                    dev_h1_used = None
                dev_h1_used = str(dev_h1_used or dev_for_gate or "").strip()
                # --- NEW: compare with Redis snap last close (deterministic refresh) ---
                snap_last_close_ms = 0
                try:
                    raw0 = R.get(f"xtl:ohlc:snap:{dev_h1_used}:{sym_u}:H1") \
                         if (R is not None and dev_h1_used and sym_u) else None
                    js0 = json.loads(raw0) if raw0 else None
                    bars0 = (js0.get("bars") if isinstance(js0, dict) else None) or []
                    if isinstance(bars0, list) and bars0 and isinstance(bars0[-1], dict):
                        t_last = bars0[-1].get("t") or 0  # seconds
                        if isinstance(t_last, (int, float)) and t_last > 1_000_000_000:
                            snap_last_close_ms = int(t_last * 1000 + tf_ms)
                except Exception:
                    snap_last_close_ms = 0

                if debug_gate_on:
                    row["dbg_h1_last_close_ms_before"] = last_close_ms
                    row["dbg_h1_snap_last_close_ms"] = snap_last_close_ms

                


                # stale if last close is too old compared to now (buffer allows slight delays)
                # refresh if Redis snap is newer than attached bars
                if snap_last_close_ms > 0 and snap_last_close_ms > last_close_ms:

                    raw = R.get(f"xtl:ohlc:snap:{dev_h1_used}:{sym_u}:H1") if (R is not None and dev_h1_used and sym_u) else None
                    # parse JSON safely (no _json_load_twice dependency)
                    js = None
                    if raw:
                        try:
                            if isinstance(raw, (bytes, bytearray)):
                                raw = raw.decode("utf-8", "ignore")
                            js = json.loads(raw)
                        except Exception:
                            js = None
                    
                    bars = (js.get("bars") if isinstance(js, dict) else None) or []
                    if isinstance(bars, list) and len(bars) >= 2:
                        nb = _normalize_snap_bars_to_ms(bars, tf_ms)
                        nb = [b for b in nb if isinstance(b, dict) and all(k in b for k in ("o", "h", "l", "c"))]
                        nb.sort(key=_tc)
                        if len(nb) >= 2:
                            row_h1["bars"] = nb
                            if debug_gate_on:
                                row["dbg_h1_refresh"] = True
                                row["dbg_h1_last_close_ms"] = _tc(nb[-1])
                                row["dbg_h1_refresh_dev_used"] = dev_h1_used
            except Exception as e:
                if debug_gate_on:
                    row["dbg_h1_refresh_exc_type"] = type(e).__name__
                    row["dbg_h1_refresh_exc"] = str(e)

            zone_exc_type = None
            zone_exc = None
            zone_tb = None
            if debug_gate_on:
                try:
                    b1 = row_h1.get("bars") or row_h1.get("ohlc") or []
                    row["dbg_gate_bars_n"] = len(b1) if isinstance(b1, list) else 0
                    if isinstance(b1, list) and b1 and isinstance(b1[-1], dict):
                        row["dbg_gate_bar_keys"] = list(b1[-1].keys())
                        def _tc_dbg(b):
                            v = b.get("t_close_ms") or b.get("t") or 0
                            try:
                                v = int(float(v)) if v is not None else 0
                            except Exception:
                                v = 0
                            if 0 < v < 10_000_000_000:
                                v *= 1000
                            return v

                        if isinstance(b1, list) and b1:
                            last_bar = max((x for x in b1 if isinstance(x, dict)), key=_tc_dbg, default=None)
                            row["dbg_gate_last_t_close_ms"] = _tc_dbg(last_bar) if last_bar else None

                except Exception:
                    pass
            if debug_gate_on:
                try:
                    fn = _pick_last_closed_bar_from_bars
                    code = getattr(fn, "__code__", None)
                    row["dbg_pick_fn"] = {
                        "obj": str(fn),
                        "firstlineno": getattr(code, "co_firstlineno", None),
                        "file": getattr(code, "co_filename", None),
                    }
                except Exception:
                    pass
            # --- Build TF-sliced SR lists for zone gate ---
            try:
                ec = (((row.get("chart") or {}).get("overlays") or {}).get("entry_context") or {})
            except Exception:
                ec = {}

            sr_for_gate = {
                "h1": {
                    "supports_near": ec.get("entry_support_near_levels") or [],
                    "supports_major": ec.get("entry_support_major_levels") or [],
                    "resistances_near": ec.get("entry_resistance_near_levels") or [],
                    "resistances_major": ec.get("entry_resistance_major_levels") or [],
                },
                "h4": {},  # optional; safe placeholder
            }

            

           
            

            try:
                allowed, gate_meta = _zone_reversal_gate(
                    sym=sym_u,
                    direction=("BUY" if opp_dir == "UP" else "SELL"),
                    row_h1=row_h1,
                    sr=sr_for_gate,
                    now_ms=now_ms,
                    tf_tag="H1",
                    pinned_device=dev_for_gate,
                    x_device_id=dev_for_gate,
                    debug_gate=bool(debug_gate_on),
                )

                # --- CALLSITE DEBUG MARKER ---
                if isinstance(gate_meta, dict):
                    try:
                        gate_meta["__callsite_marker__"] = "AFTER_ZONE_GATE_CALL"
                        gate_meta["__callsite_debug_gate__"] = bool(debug_gate_on)
                        gate_meta["__callsite_file__"] = __file__
                        if debug_gate_on:
                            gate_meta["sr"] = sr_for_gate
                            gate_meta["dbg_sr_for_gate_top"] = {
                                "supp_near_n": len(sr_for_gate.get("supports_near") or []),
                                "supp_major_n": len(sr_for_gate.get("supports_major") or []),
                                "res_near_n": len(sr_for_gate.get("resistances_near") or []),
                                "res_major_n": len(sr_for_gate.get("resistances_major") or []),
                                "supp_near_1": (sr_for_gate.get("supports_near") or [None])[0],
                                "res_near_1": (sr_for_gate.get("resistances_near") or [None])[0],
                            }
                    except Exception:
                        pass

                try:
                    row["entry_gate"] = gate_meta
                except Exception:
                    pass


                # (optional) convenience field
                try:
                    if isinstance(gate_meta, dict):
                        row["entry_zone"] = gate_meta.get("zone")
                except Exception:
                    pass


                # If gate returns an error dict (not raising), surface it in debug output
                if debug_gate_on and isinstance(gate_meta, dict) and (gate_meta.get("exc") or gate_meta.get("exc_type")):
                     row["dbg_zone_gate_meta"] = {
                         "reason": gate_meta.get("reason"),
                         "stage": gate_meta.get("stage"),
                         "exc_type": gate_meta.get("exc_type"),
                         "exc": gate_meta.get("exc"),
                         "dev_used": gate_meta.get("dev_used"),
                     }

            except Exception as e:
                
                
                zone_exc_type = type(e).__name__
                zone_exc = str(e)
                zone_tb = traceback.format_exc(limit=10)
                allowed = False
                gate_meta = {
                    "reason": "ZONE_GATE_EXCEPTION",
                    "blocked": True,
                    "stage": "ZONE_GATE",
                    "exc_type": zone_exc_type,
                    "exc": zone_exc,
                    "tb": zone_tb,
                    "dev_used": dev_for_gate,
                }
            if isinstance(gate_meta, dict):
                gate_meta["dev_used"] = dev_for_gate
                gate_meta.setdefault("stage", "ZONE_GATE")
            if debug_gate_on:
                row["dbg_zone_gate_exc_type"] = zone_exc_type
                row["dbg_zone_gate_exc"] = zone_exc
                row["dbg_dev_for_gate"] = dev_for_gate
                if zone_tb:
                    row["dbg_zone_gate_tb"] = zone_tb


            if not allowed:
                reason = gate_meta.get("reason") if isinstance(gate_meta, dict) else None
                gate_stats[str(reason or "unknown")] += 1

                

                _push_blocked(
                    sym,
                    row,
                    stage="ZONE_GATE",
                    reason=str(reason or "unknown"),
                    meta={**(gate_meta if isinstance(gate_meta, dict) else {}), "dev": dev_for_gate},
                )

                # SOFT MODE: keep the opportunity visible, but mark entry as gated
                if not isinstance(gate_meta, dict):
                    gate_meta = {}
                gate_meta["blocked"] = True
                gate_meta.setdefault("reason", str(reason or "unknown"))
                gate_meta["stage"] = "ZONE_GATE"

                # downgrade confidence so UI reflects it's not “clean”
                opp_conf = "low"

                





        # --------------------------------------------------
        # 4) Build opportunity row
        # --------------------------------------------------
        out = dict(row)
        # ---- NEW: horizon-based expiry (default 6h) ----
        try:
            horizon_min = int(float(out.get("horizon_min") or row.get("horizon_min") or 540))
        except Exception:
            horizon_min = 540
        hour_ms = 60 * 60 * 1000
        bucket_open_ts = (now_ms // hour_ms) * hour_ms
        opp_open_ts = bucket_open_ts
        opp_expire_ts = opp_open_ts + horizon_min * 60_000
        opp_id = f"{sym}-H1-{opp_dir}-{opp_open_ts}"

        out["opp_id"] = opp_id
        out["id"] = opp_id
        out["opp_direction"] = opp_dir
        out["decision"] = "BUY" if opp_dir == "UP" else "SELL"
        out.setdefault("id", opp_id)
        out.setdefault("status", "active")
        out.setdefault("blocked", False)
        out["opp_confidence"] = opp_conf
        out["opp_horizon"] = "H1"
        out["opp_h4_agree"] = h4_agree
        out["opp_open_ts"] = opp_open_ts
        out["opp_expire_ts"] = opp_expire_ts
        out["opp_min_room_h1"] = thr1
        out["opp_min_room_h4"] = thr4
        out["opp_score"] = round(float(opp_score), 1)
        gm = gate_meta if isinstance(gate_meta, dict) else None

        out["entry_gate"] = gm
        out["gate_meta"] = gm  # keep a stable copy for _run_entry_only_if_armed + UI/debug

        if gm:
            out["zone"] = gm.get("zone")
            out["opp_confidence"] = gm.get("confidence", opp_conf)
            out["opp_reason"] = gm.get("reason", out.get("opp_reason"))
        else:
            out["zone"] = None

        out["horizon_min"] = horizon_min

        # SR summary (same as your code)
        sr = row.get("sr")
        if isinstance(sr, dict):

            def _attach_tf(tf_key: str, side_key: str, dist_key: str, level_key: str) -> None:
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
                    out[side_key] = kind
                if isinstance(dist_pct, (int, float)):
                    out[dist_key] = float(dist_pct)
                if isinstance(level, (int, float)):
                    out[level_key] = float(level)

            _attach_tf("H1", "sr_h1_side", "sr_h1_dist_pct", "sr_h1_level")
            _attach_tf("H4", "sr_h4_side", "sr_h4_dist_pct", "sr_h4_level")

            nearest = sr.get("nearest") or sr.get("nearest_zone") or {}
            if isinstance(nearest, dict):
                kind = (nearest.get("kind") or nearest.get("side") or "").lower()
                if kind:
                    out["sr_side"] = kind
                dist_pct = nearest.get("distance_pct") or nearest.get("dist_pct")
                if isinstance(dist_pct, (int, float)):
                    out["sr_dist_pct"] = float(dist_pct)

        if delta_pct is not None:
            out["opp_delta_pct"] = delta_pct
            out["opp_delta_thr"] = delta_thr

        out.setdefault("opp_reason", f"H1 {m1:.3f}% thr {thr1:.3f}%; H4 {m4:.3f}% thr {thr4:.3f}%")
        out.setdefault("update_tf", tfu)
        out.setdefault("server_now_ms", now_ms)
        # ------------------------------------------------------------
        # DEBUG: always compute zone gate when debug_gate is on
        # (so entry_gate/zone/tap info is visible even if not armed)
        # ------------------------------------------------------------
        if debug_gate_on and (not isinstance(out.get("entry_gate"), dict) or not out.get("entry_gate")):
            try:
                allowed_dbg, gm_dbg = _zone_reversal_gate(
                    sym=sym_u,
                    direction=("BUY" if opp_dir == "UP" else "SELL"),
                    row_h1=row_h1,
                    sr=sr_for_gate,
                    now_ms=now_ms,
                    tf_tag="H1",
                    pinned_device=dev_for_gate,
                    x_device_id=dev_for_gate,
                    debug_gate=True,
                )
                if isinstance(gm_dbg, dict):
                    # keep it consistent with other callsite behavior
                    gm_dbg.setdefault("blocked", (not bool(allowed_dbg)))
                    gm_dbg["__callsite_marker__"] = "DEBUG_FORCE_GATE"
                    out["entry_gate"] = gm_dbg
                    out["zone"] = gm_dbg.get("zone")
                    # optional: keep a copy under "gate" for UI inspection
                    out["gate"] = gm_dbg
            except Exception as e:
                out["dbg_force_gate_exc"] = f"{type(e).__name__}:{e}"

        _run_entry_only_if_armed(out)
        # ------------------------------------------------------------
        # Self-heal stale pointer-hash gates (prevents permanent lock)
        # ------------------------------------------------------------
        try:
            pointer_key = f"xtl:trend:opp:h1:{sym_u}:{opp_dir}"
            eg = out.get("entry_gate")
            if isinstance(eg, str):      
                eg = _json_load_twice(eg)
            reason = eg.get("reason") if isinstance(eg, dict) else None

            if reason in ("no_h1_bars", "no_atr"):
                bars_h1 = _get_closed_h1_bars(sym_u, dev_h1) if dev_h1 else []
                if debug_gate_on:
                    row["dbg_h1_dev_used"] = dev_h1

                if isinstance(bars_h1, list) and len(bars_h1) >= 2:
                    # patch existing row_h1 instead of replacing it
                    try:
                        if isinstance(row_h1, dict):
                            row_h1["bars"] = bars_h1
                    except Exception:
                        pass

                    allowed, gate_meta2 = _zone_reversal_gate(
                        sym=sym_u,
                        direction=("BUY" if opp_dir == "UP" else "SELL"),
                        row_h1=row_h1,
                        sr=sr_for_gate,
                        now_ms=now_ms,
                        tf_tag="H1",
                        pinned_device=dev_for_gate,
                        x_device_id=dev_for_gate,
                        debug_gate=bool(debug_gate_on),

                    )

                    out["entry_gate"] = gate_meta2
                    # re-sync derived fields from healed gate
                    try:
                        gm2 = gate_meta2 if isinstance(gate_meta2, dict) else None
                        if gm2:
                            out["zone"] = gm2.get("zone")
                            out["opp_reason"] = gm2.get("reason", out.get("opp_reason"))
                            out["opp_confidence"] = gm2.get("confidence", out.get("opp_confidence"))
                            if debug_gate_on:
                                out["gate"] = gm2
                    except Exception:
                        pass
                    try:
                        _run_entry_only_if_armed(out)
                    except Exception:
                        pass


                    out["signal_reason"] = (
                        "armed" if allowed else "not_armed:" + str(gate_meta2.get("reason") or "unknown")
                    )

                    R.hset(
                        pointer_key, 
                        mapping={
                            "entry_gate": json.dumps(gate_meta2, separators=(",", ":")),
                            "signal_reason": out["signal_reason"],
                            "resp_ts_ms": str(now_ms),
                            "opp_id": json.dumps(out.get("opp_id") or "", separators=(",", ":")),
                        }
                    )

                    if debug_gate_on:
                        out["dbg_h1_bars_src"] = "self_heal_pointer_hash"
                        out["dbg_h1_bars_n2"] = len(bars_h1)
        except Exception:
            pass


        # freeze snapshot (keeps it visible until hit/expired)
        # freeze snapshot (keeps it visible until hit/expired)
        # BUT: never freeze a gate exception (otherwise you keep returning stale exceptions forever)
        try:
            is_gate_exc = (
                isinstance(gate_meta, dict)
                and (
                    gate_meta.get("reason") == "ZONE_GATE_EXCEPTION"
                    or gate_meta.get("exc_type")
                    or gate_meta.get("exc")
                )
            )
        except Exception:
            is_gate_exc = False

        if is_gate_exc:
            # best-effort cleanup so next request recomputes cleanly
            try:
                tfu0 = (out.get("tf") or tfu or "H1").upper()
                dir0 = "UP" if (out.get("opp_dir") or opp_dir) == "UP" else "DOWN"
                pointer_key = f"xtl:trend:opp:{tfu0.lower()}:{sym_u}:{dir0}"
                R.delete(pointer_key)
                # delete frozen snapshots too
                for k in R.scan_iter(match=f"xtl:trend:opp:{tfu0.lower()}:{sym_u}-{tfu0}-{dir0}-*"):
                    R.delete(k)
                if debug_gate_on:
                    out["dbg_no_snapshot"] = "gate_exception_no_freeze"
            except Exception:
                pass
        else:
            # In debug, NEVER serve frozen/cached opps — always recompute so gate + Redis truth is visible
            if debug_force or debug_gate:
                try:
                    tfu0 = (out.get("tf") or tfu or "H1").upper()
                    dir0 = "UP" if (out.get("opp_dir") or opp_dir) == "UP" else "DOWN"
                    pointer_key = f"xtl:trend:opp:{tfu0.lower()}:{sym_u}:{dir0}"
                    R.delete(pointer_key)
                except Exception:
                    pass
                # skip freezing
            else:
                out = _freeze_or_snapshot_opp(sym, out, now_ms)


        #out = _freeze_or_snapshot_opp(sym, out, now_ms)
        # ALWAYS keep latest gate result (snapshot may h#ave stale/blank dev)
        try:
            eg_final = out.get("entry_gate")
            if isinstance(eg_final, str):
                eg_final = _json_load_twice(eg_final)
                out["entry_gate"] = eg_final

            if isinstance(eg_final, dict):
                # enforce dev in final gate meta
                if dev_for_gate and not eg_final.get("dev"):
                    eg_final["dev"] = dev_for_gate
        except Exception:
            pass
        # Set status for UI (so blocked opps still appear)
        try:
            eg0 = out.get("entry_gate")
            is_blocked = isinstance(eg0, dict) and bool(eg0.get("blocked"))

            # Always "active" so UI doesn't filter it out
            out["status"] = "active"

            # Keep gate metadata for UI badges
            if is_blocked:
                out["blocked_reason"] = out.get("blocked_reason") or eg0.get("reason")
                out["blocked_at"] = out.get("blocked_at") or now_ms
            else:
                out["blocked_reason"] = None

            # UI-facing decision (WAIT until armed)
            out["decision_raw"] = out.get("decision")  # BUY/SELL original
            out["decision"] = out["decision_raw"] if not is_blocked else "WAIT"
            out["is_armed"] = (not is_blocked)
            out["ui_state"] = "armed" if not is_blocked else "waiting"
        except Exception:
            out["status"] = out.get("status") or "active"
            out.setdefault("decision_raw", out.get("decision"))
            out.setdefault("is_armed", True)
            out.setdefault("ui_state", "armed")


        # ------------------------------------------------------------
        # DEBUG: preserve dbg_* fields even if snapshot returns a cached hash
        # ------------------------------------------------------------
        if debug_gate_on:
            try:
                # "row" is the live working row that has dbg_* keys
                for k, v in (row or {}).items():
                    if isinstance(k, str) and k.startswith("dbg_"):
                        out[k] = v
            except Exception:
                pass

            # also preserve pinned/header device visibility (useful in jq)
            try:
                out["dbg_dev_for_gate"] = str(dev_for_gate or "").strip()
                out["dbg_x_device_id_hdr"] = str(x_device_id_hdr or "").strip()
                out["dbg_pinned_device"] = str(pinned_device or "").strip()
                out["dbg_effective_device"] = str(effective_device or "").strip()
            except Exception:
                pass

        # Persist entry freeze into Redis snapshot so it never flips back to WAIT
        _persist_entry_meta_to_snapshot(sym, out)
        
        # ---- chart overlays for UI ----
        try:
            overlays = {}

            # SR overlay (use the SR you already attached to out/row)
            # SR zones (shared helper; works for H1/H4 and h1/h4 bundles)
            try:
                atr = (
                    out.get("atr_1h")
                    or out.get("atr")
                    or out.get("atr14")
                    or out.get("atr14_1h")
                )
                atr = float(atr) if isinstance(atr, (int, float)) else None

                overlays["sr_zones"] = _build_sr_zones_from_summary(
                    out.get("sr"),
                    sym=sym,
                    pip_factor=float(pip_factor),
                    atr=atr,
                )
            except Exception:
                overlays["sr_zones"] = []

            # entry context (gate meta)
            gm = out.get("entry_gate")
            if isinstance(gm, dict):
                overlays["entry_context"] = gm

            # trade overlay (use final frozen fields)
            overlays["trade"] = {
                "decision": out.get("signal") or out.get("decision"),
                "entry_price": out.get("entry_price"),
                "tp_price": out.get("tp_price") or out.get("target_price"),
                "sl_price": out.get("sl_price") or out.get("stop_loss"),
                "entry_ts_ms": out.get("entry_ts_ms"),
            }

            out.setdefault("chart", {})
            if isinstance(out["chart"], dict):
                out["chart"]["overlays"] = overlays
        except Exception:
            pass

        status = str(out.get("status") or "").strip().lower()
        if status in ("active", "new", "open", "blocked", ""):
            opp_rows.append(out)

    history = _load_opp_history(limit=50)

    if (not opp_rows) and debug_top > 0 and debug_pool:
        debug_pool.sort(key=lambda x: float(x.get("opp_score") or 0.0), reverse=True)
        opp_rows = debug_pool[:debug_top]

    
    

    

    # heavy work happens here (predict_all + build opp_rows/history/payload) ...
    # DROP null/empty symbol rows (safety)
    opp_rows = [r for r in opp_rows if str((r or {}).get("symbol") or "").strip()]
    payload = {"ok": True, "tf": tfu, "rows": opp_rows, "history": history}
    if debug_gate and gate_stats:
        payload["gate_stats"] = dict(sorted(gate_stats.items(), key=lambda x: -x[1]))
    # -------------------------------------------------
    # FIX 2 (finish): write cache + unlock
    # -------------------------------------------------
    # write fresh cache + unlock
    if cache_key and (not (debug_force or debug_gate or loose)):
            
        try:
            R.setex(cache_key, cache_ttl_s, json.dumps(payload))
        except Exception:
            pass

           
        try:
           if inflight_lock_key and inflight_got_lock:
               R.delete(inflight_lock_key)
        except Exception:
           pass
       
    # ----------------------------------------
    # Emit per-run opportunity gate statistics
    # ----------------------------------------
    if gate_stats:
        try:
            log.info(
               "[OPP_GATE_STATS] %s",
               dict(sorted(gate_stats.items(), key=lambda x: -x[1]))
            )
        except Exception:
            pass
    return payload



@router.get("/opportunities/stats")
def opportunities_stats(
    day: str | None = None,
    user = Depends(require_auth_optional),
):
    """
    Returns counts of hit/sl_hit/expired for the day (UTC) from Redis outcomes list.
    day format: YYYYMMDD (UTC). default=today UTC.
    """
    try:
        uid = _uid_from(user) if user else None
    except Exception:
        uid = None

    uid = str(uid or "global")
    if not day:
        day = time.strftime("%Y%m%d", time.gmtime())

    key = f"xtl:outcomes:{uid}:{day}"
    items = R.lrange(key, 0, -1) or []

    counts = {"hit": 0, "sl_hit": 0, "expired": 0, "other": 0}
    by_symbol: dict[str, dict] = {}

    for s in items:
        try:
            rec = json.loads(s)
        except Exception:
            continue

        st = str(rec.get("status") or "").lower().strip() or "other"
        if st not in counts:
            st = "other"
        counts[st] += 1

        sym = str(rec.get("symbol") or "NA")
        if sym not in by_symbol:
            by_symbol[sym] = {"hit": 0, "sl_hit": 0, "expired": 0, "other": 0}
        by_symbol[sym][st] = by_symbol[sym].get(st, 0) + 1

    total = sum(counts.values())

    # rough win rate = hits / (hits + sl_hit) ignoring expired
    denom = counts["hit"] + counts["sl_hit"]
    win_rate = (counts["hit"] / denom) if denom > 0 else None

    return {
        "ok": True,
        "day": day,
        "uid": uid,
        "total_closed": total,
        "counts": counts,
        "win_rate_vs_sl": win_rate,
        "by_symbol": by_symbol,
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

def _as_bool(x) -> bool:
    try:
        if isinstance(x, bool):
            return x
        if x is None:
            return False
        s = str(x).strip().lower()
        return s in ("1", "true", "yes", "y", "on")
    except Exception:
        return False


def _as_int(x, default=0) -> int:
    try:
        return int(float(x))
    except Exception:
        return default


def _find_tradable_devices_for_user(user_id: str) -> list[dict]:
    """
    Looks for device hashes: device:{dev_id}
    Filters to tradable devices for this user.
    Returns list of dicts with key device_id + a few fields.
    """
    out = []
    try:
        # scan_iter is safe; does not block Redis like KEYS
        for key in R.scan_iter(match="device:dev_*", count=200):
            try:
                dev_id = key.split("device:", 1)[1]
            except Exception:
                dev_id = key

            try:
                h = R.hgetall(key) or {}
            except Exception:
                continue

            if str(h.get("owner_id") or "") != str(user_id):
                continue

            if str(h.get("status") or "").lower() != "online":
                continue

            if _as_int(h.get("mt5_ok"), 0) != 1:
                continue

            if not _as_bool(h.get("mt5_terminal_connected")):
                continue

            if not _as_bool(h.get("mt5_terminal_trade_allowed")):
                continue

            out.append({
                "device_id": dev_id,
                "label": h.get("label"),
                "version": h.get("version"),
                "mt5_account_login": h.get("mt5_account_login"),
                "mt5_account_server": h.get("mt5_account_server"),
                "mt5_account_trade_mode": h.get("mt5_account_trade_mode"),
            })
    except Exception:
        pass

    return out


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
    overlays: Optional[Dict[str, Any]] = None 
    broker: Optional[Dict[str, Any]] = None   

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
    user_id = _uid_from_user(user)
    if not user_id:
        raise HTTPException(status_code=401, detail="Login required")

    state = _load_bot_state(user_id)
    return BotStateResp(ok=True, state=state)


@router.post("/bot/state", response_model=BotStateResp)
def update_bot_state(payload: BotStateUpdate, user = Depends(require_auth_optional)):
    user_id = _uid_from_user(user)
    if not user_id:
        raise HTTPException(status_code=401, detail="Login required")

    current = _load_bot_state(user_id)
    patch = payload.dict(exclude_unset=True)

    # --- NEW: guard when enabling bot ---
    # Only check if this call will make enabled True (or keep it True)
    enabling = False
    try:
        if "enabled" in patch:
            enabling = bool(patch.get("enabled")) and not bool(current.get("enabled"))
        else:
            enabling = False
    except Exception:
        enabling = False

    if enabling:
        tradable = _find_tradable_devices_for_user(user_id)
        if not tradable:
            raise HTTPException(
                status_code=409,
                detail=(
                    "No tradable MT5 device available. Ensure at least one device is ONLINE, "
                    "mt5_ok=1, terminal connected=True, and terminal trade_allowed=True "
                    "(turn ON Algo Trading in MT5)."
                ),
            )

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
    user=Depends(require_auth_optional),
):
    import os, json, time
    

    # ---------- helpers ----------
    

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
    # optional-auth safe: prefer override/header, else use optional user, else "public"
    requested = user_id_override or (str(hdr_key).strip() if hdr_key else (_uid_from_user(user) if user else None)) or "public"
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
                "sr": {},
                "preview": {
                    "symbol": sym,
                    "tf": tfu,
                    "bars": [],
                    "lastClosedTs": None,
                    "overlays": {"sr": {}, "sr_zones": []},
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
            "sr": {},
            "preview": {"symbol": sym, "tf": tfu, "bars": [], "lastClosedTs": last_closed_ms or None,"overlays": {"sr": {}, "sr_zones": []},},
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
                "sr": {},
                "preview": {"symbol": sym, "tf": tfu, "bars": [], "lastClosedTs": last_closed_ms or None,"overlays": {"sr": {}, "sr_zones": []},},
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
            "sr": {},
            "preview": {"symbol": sym, "tf": tfu, "bars": [], "lastClosedTs": None,"overlays": {"sr": {}, "sr_zones": []},},
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
                            "t": _epoch_to_ms_any(r.get("t_close_ms") or r.get("t_open_ms") or r.get("t")),
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
        # --- PATCH: fallback to already-built closed bars when broker H1/H4 fetch fails ---
        # If user requested tf=H1 and we can't fetch H1 rows, reuse `closed` (already correct bars for this tf)
        if (h1_df is None or getattr(h1_df, "empty", True)) and tfu == "H1" and closed:
            h1_df = _rows_to_df(closed)

        # If user requested tf=H4 and we can't fetch H4 rows, reuse `closed`
        if (h4_df is None or getattr(h4_df, "empty", True)) and tfu == "H4" and closed:
            h4_df = _rows_to_df(closed)


        # Last price from preview bars (what UI is showing)
        last_price = float(prev_rows[-1].get("c")) if (prev_rows and isinstance(prev_rows[-1], dict) and prev_rows[-1].get("c") is not None) else None
        if last_price is None and closed:
            try:
                last_price = float(closed[-1].get("c"))
            except Exception:
                last_price = None


        # pip factor per symbol (rough, can refine later)
        pip_factor = 0.01 if sym == "XAUUSD" else (0.01 if sym.endswith("JPY") else 0.0001)

        # Always attempt SR compute when we have a price.
        # summarize_sr_multi_tf already:
        # - falls back to last_good if frames missing
        # - always writes last (short TTL) when it runs
        sr_summary = {}
        try:
            px0 = float(last_price) if last_price is not None else None
        except Exception:
            px0 = None
        sr_summary = summarize_sr_multi_tf(
            symbol=sym,
            price=px0,
            h4_df=h4_df,
            h1_df=h1_df,
            pip_factor=float(pip_factor),
            cache=R,
            cache_ttl_sec=900,
            good_ttl_sec=7 * 24 * 3600,
        )
    except Exception as e:
        sr_summary = {"error": f"sr_failed: {e}"}
        # ---- SR fallback: if compute failed or returned empty, load last-good bundle ----
        try:
            bad = (not isinstance(sr_summary, dict)) or (len(sr_summary or {}) == 0) or bool(sr_summary.get("error"))
            if bad and R is not None:
                raw_lg = _redis_get_text(f"xtl:sr:bundle:last_good:{sym}")  # summarize_sr_multi_tf canonical key
                if raw_lg:
                    lg = json.loads(raw_lg)
                    if isinstance(lg, dict) and len(lg) > 0:
                        sr_summary = lg
        except Exception:
            pass
    # ---- fallback SR if summarize_sr_multi_tf returns {} ----
    sr_fallback_zones = None
    try:
        if isinstance(sr_summary, dict) and len(sr_summary) == 0:
            def _pivot_levels(df: pd.DataFrame, w: int = 2):
                if df is None or df.empty or len(df) < (w * 2 + 5):
                    return ([], [])
                hh = df["h"].rolling(w * 2 + 1, center=True).max()
                ll = df["l"].rolling(w * 2 + 1, center=True).min()
                piv_hi = df.loc[df["h"] == hh, "h"].dropna().tolist()
                piv_lo = df.loc[df["l"] == ll, "l"].dropna().tolist()
                return (piv_lo, piv_hi)

            def _cluster(levels: list[float], bin_size: float):
               if not levels:
                   return []
               out = {}
               for x in levels:
                   try:
                       k = round(float(x) / bin_size) * bin_size
                       out.setdefault(k, 0)
                       out[k] += 1
                   except Exception:
                       continue
               # return sorted by touches desc
               return sorted(out.items(), key=lambda kv: kv[1], reverse=True)

            def _mk_zones(level_counts: list[tuple[float, int]], tf_label: str, kind: str, half: float, topn: int = 6):
                z = []
                for lvl, touches in (level_counts or [])[:topn]:
                    z.append({
                        "tf": tf_label,
                        "low": float(lvl) - half,
                        "high": float(lvl) + half,
                        "kind": kind,
                        "touches": int(touches),
                        "level": float(lvl),
                        "strength": float(min(1.0, 0.15 * touches)),
                    })
                return z

            # zone half width: use a stable minimum (works for XAU)
            half = float(os.getenv("XTL_ZONE_MIN_PX_XAU", "0.8")) if sym == "XAUUSD" else max(3.0 * float(pip_factor), float(os.getenv("XTL_ZONE_MIN_PX_FX", "0.0008")))
            bin_size = max(half, 1e-9)

            z_all = []

            if h4_df is not None and not h4_df.empty:
                lo, hi = _pivot_levels(h4_df, w=2)
                z_all += _mk_zones(_cluster(lo, bin_size), "H4", "support", half)
                z_all += _mk_zones(_cluster(hi, bin_size), "H4", "resistance", half)

            if h1_df is not None and not h1_df.empty:
                lo, hi = _pivot_levels(h1_df, w=2)
                z_all += _mk_zones(_cluster(lo, bin_size), "H1", "support", half)
                z_all += _mk_zones(_cluster(hi, bin_size), "H1", "resistance", half)

            sr_fallback_zones = z_all

            # also expose something non-empty under .sr
            sr_summary = {"method": "fallback_pivots", "zones": sr_fallback_zones}
    except Exception as _e:
        # keep sr_summary as-is; don't break endpoint
        pass

    # ---- attach overlays into preview for UI chart ----
    try:
        if isinstance(preview_out, dict):
            overlays = preview_out.get("overlays")
            if not isinstance(overlays, dict):
                overlays = {}
                preview_out["overlays"] = overlays

            
            # SR overlay + SR zones (for UI)
            overlays["sr"] = sr_summary if isinstance(sr_summary, dict) else {}

            # SR ZONES overlay (what Trend.tsx actually draws)
            try:
                overlays["sr_zones"] = _build_sr_zones_from_summary(
                    sr_summary if isinstance(sr_summary, dict) else {},
                    sym=sym,
                    pip_factor=float(pip_factor),
                    atr=None,
                )
            except Exception:
                pass

            # Gate overlay (if you have gate/entry_gate dict available)
            g = locals().get("entry_gate") or locals().get("gate")
            if isinstance(g, dict) and g:
                overlays["gate"] = {
                    "reason": g.get("reason"),
                    "confidence": g.get("confidence"),
                    "zone": g.get("zone"),
                }

            # Trade overlay (entry/SL/TP)
            overlays["trade"] = {
                "entry_price": locals().get("entry_price"),
                "sl_price": locals().get("sl_price"),
                "tp_price": locals().get("tp_price"),
                "decision": locals().get("decision") or locals().get("signal"),
                "entry_ts_ms": locals().get("entry_ts_ms"),
            }
    except Exception:
        pass

  

    # --- Canonical next-bar timing based on preview_last_closed_ts ---
    # Use the last CLOSED bar from preview as the single source of truth,

    
    # --- Canonical next-bar timing based on preview_last_closed_ts ---
    # Use the last CLOSED bar from preview as the single source of truth,
    # but always compute countdown in server time using broker_tz_offset_min.
    # --- Canonical next-bar timing (SERVER UTC ms only; monotonic) ---
    TF_MS = int(TF_SEC * 1000) if TF_SEC else 60 * 60 * 1000
    server_now_ms = int(time.time() * 1000)

    # preview_last_closed_ts is CLOSE time (ms) of last closed bar
    last_closed_ts = int(preview_last_closed_ts or 0)

    # Guard: if last_closed_ts is in the future, clamp to current server TF grid.
    if last_closed_ts > server_now_ms + 5_000:
        last_closed_ts = server_now_ms - (server_now_ms % TF_MS)

    # next close = one TF after last close; roll forward if needed
    next_close_ts = (
        last_closed_ts + TF_MS
        if last_closed_ts > 0
        else ((server_now_ms // TF_MS) + 1) * TF_MS
    )
    while next_close_ts <= server_now_ms + 250:
        next_close_ts += TF_MS

    remain_ms = max(0, next_close_ts - server_now_ms)
    poll_after_ms = max(2_000, min(remain_ms + 500, 60_000))

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
        "sr":           sr_summary if isinstance(sr_summary, dict) else {},
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
       label=str(label or "Neutral"),
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
       preview=preview_out,                  # broker-TZ anchored bars
       broker=BrokerMeta(**broker_safe), # built from device/snapshot
       sr=(sr_summary if isinstance(sr_summary, dict) else {}),
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
            label="Warming",
            sr={},
            preview={"bars": [], "overlays": {"sr": {}, "sr_zones": []}},
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
    # ensure overlays always exist for UI
    _z = []
    if isinstance(sr_summary, dict):
        _z = sr_summary.get("sr_zones") or sr_summary.get("zones") or []
    preview["overlays"] = {"sr": (sr_summary if isinstance(sr_summary, dict) else {}), "sr_zones": (_z if isinstance(_z, list) else [])}


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
        label=label,
        sr=sr_summary if isinstance(sr_summary, dict) else {},
        serverNow=server_now_ms,
        lastClosedTs=last_closed,
        nextCloseTs=next_close,
        tf_ms=TF_MS,
        preview=preview_out,
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
    user_id = _uid_from_user(user)

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

def _pos_tp_key(sym: str, sig: str) -> str:
    sym_u = (sym or "").upper().strip()
    s = (sig or "").upper().strip()

    # normalize synonyms
    if s in ("LONG", "UP"):
        s = "BUY"
    elif s in ("SHORT", "DOWN"):
        s = "SELL"

    if not sym_u or not s:
        return "xtl:pos:tp:INVALID"

    return f"xtl:pos:tp:{sym_u}:{s}"

def _load_tp_state(sym: str, sig: str) -> dict:
    try:
        k = _pos_tp_key(sym, sig)
        raw = R.get(k)
        if not raw:
            return {}
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", "ignore")
        js = _json_load_twice(raw)
        return js if isinstance(js, dict) else {}
    except Exception:
        return {}

def _save_tp_state(sym: str, sig: str, st: dict, ttl_sec: int = 7 * 24 * 3600) -> None:
    try:
        k = _pos_tp_key(sym, sig)
        try:
            st.setdefault("version", "tp_v1")
            st.setdefault("server_now_ms", int(time.time() * 1000))
        except Exception:
            pass
        R.setex(k, int(ttl_sec), json.dumps(st, default=str))
    except Exception:
        pass

def _clear_tp_state(sym: str, sig: str) -> None:
    try:
        R.delete(_pos_tp_key(sym, sig))
    except Exception:
        pass

def _pos_exit_key(sym: str, sig: str) -> str:
    sym_u = (sym or "").upper().strip()
    s = (sig or "").upper().strip()

    # normalize synonyms
    if s in ("LONG", "UP"):
        s = "BUY"
    elif s in ("SHORT", "DOWN"):
        s = "SELL"

    if not sym_u or not s:
        # last-resort key to avoid polluting redis; caller should handle {}
        return "xtl:pos:exit:INVALID"

    return f"xtl:pos:exit:{sym_u}:{s}"

def _load_exit_state(sym: str, sig: str) -> dict:
    try:
        k = _pos_exit_key(sym, sig)
        raw = R.get(k)
        if not raw:
            return {}

        # redis often returns bytes
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", "ignore")

        js = _json_load_twice(raw)
        return js if isinstance(js, dict) else {}
    except Exception:
        return {}

def _save_exit_state(sym: str, sig: str, st: dict, ttl_sec: int = 7 * 24 * 3600) -> None:
    try:
        k = _pos_exit_key(sym, sig)

        # small debug helpers (safe)
        try:
            st.setdefault("version", "exit_v1")
            st.setdefault("server_now_ms", int(time.time() * 1000))
        except Exception:
            pass

        R.setex(k, int(ttl_sec), json.dumps(st, default=str))
    except Exception:
        pass

def _clear_exit_state(sym: str, sig: str) -> None:
    try:
        R.delete(_pos_exit_key(sym, sig))
    except Exception:
        pass


def _evaluate_alert_outcome(sym: str, snap: dict, row: dict, now_ms: int):
    """
    Evaluates whether an opportunity HIT target, STRUCTURE-EXIT (post-entry), or EXPIRED.

    - Direction is UP/DOWN (not BUY/SELL) for target-hit evaluation
    - STRUCTURE-EXIT is evaluated post-entry using frozen entry zone + sweep->reclaim logic
    - Close snapshot ONLY when hit OR exit OR expired (time; only when NOT entered)
    - Works even if alert_id is missing (uses opp_id as fallback)
    - Computes target from trade_tp_pct_1h / expected_move_pct_1h if target_price_1h is missing
    """

    def _sj(key: str, default=None):
        v = snap.get(key)
        if v is None:
            try:
                v = snap.get(key.encode("utf-8"))
            except Exception:
                v = None
        if v is None:
            return default
        if isinstance(v, (bytes, bytearray)):
            v = v.decode("utf-8", "ignore")
        if isinstance(v, (int, float, bool, dict, list)):
            return v
        try:
            return json.loads(v)
        except Exception:
            return v

    def _entry_meta_from_snap() -> dict:
        """
        Pull frozen entry metadata so it survives into history.
        """
        def _pick(key: str, default=None):
            v = _sj(key, None)
            if v is None and isinstance(row, dict):
                v = row.get(key)
            return v if v is not None else default

        return {
            "entry_triggered": bool(_pick("entry_triggered", False)),
            "entry_signal": _pick("entry_signal", None),
            "entry_reason": _pick("entry_reason", None),
            "entry_ts_ms": _pick("entry_ts_ms", None),
            "entry_price": _pick("entry_price", None),
            "tp_price": _pick("tp_price", None),
            "sl_price": _pick("sl_price", None),  # kept for UI only (NOT used for exit)
            "discord_entry_sent": _pick("discord_entry_sent", None),

            # ---- frozen entry zone for structure exit ----
            "entry_zone_low": _pick("entry_zone_low", None),
            "entry_zone_high": _pick("entry_zone_high", None),
            "entry_zone_level": _pick("entry_zone_level", None),
            "atr_1h": _pick("atr_1h", None),
            "atr": _pick("atr", None),
            "atr14": _pick("atr14", None),
            "atr14_1h": _pick("atr14_1h", None),

            # optional device hint (if you store it)
            "device": _pick("device", None),
            "pinned_device": _pick("pinned_device", None),
            "device_id": _pick("device_id", None),
        }

    # ------------------------------------------------------------------

    sym_u = (sym or "").upper().strip()
    if not sym_u:
        return

    direction = str((_sj("opp_direction") or _sj("direction") or "")).upper()
    if direction not in ("UP", "DOWN"):
        return

    alert_id = _sj("alert_id")
    opp_id = _sj("opp_id")
    event_id = str(alert_id or opp_id or "").strip()
    has_alert = bool(alert_id)

    # ---------------- basis ----------------
    basis = None
    for k in ("alert_price_1h", "basis_price_1h", "basis_price", "alert_price", "basisPrice"):
        v = _sj(k)
        if isinstance(v, (int, float)) and float(v) > 0:
            basis = float(v)
            break

    # ---------------- target ----------------
    target = None
    for k in ("target_price_1h", "target_price", "targetPrice"):
        v = _sj(k)
        if isinstance(v, (int, float)) and float(v) > 0:
            target = float(v)
            break

    # ---------------- move pct ----------------
    move_pct_1h = None
    for k in ("trade_tp_pct_1h", "expected_move_pct_1h", "expected_move_pct", "move_pct_1h"):
        v = _sj(k)
        if isinstance(v, (int, float)):
            move_pct_1h = float(v)
            break

    # ---------------- times ----------------
    alert_ms = int(_sj("alert_created_ms") or 0)
    opp_expire_ts = int(_sj("opp_expire_ts") or 0)
    horizon_min = int(_sj("horizon_min") or 360)  # 6h default
    # If opp_expire_ts missing, set it from horizon (keeps one source of truth)
    if not opp_expire_ts and alert_ms:
        opp_expire_ts = alert_ms + horizon_min * 60_000


    # ---------------- last price (baseline) ----------------
    last_price = None
    lp = _sj("last_price")
    if isinstance(lp, (int, float)) and float(lp) > 0:
        last_price = float(lp)

    if last_price is None and isinstance(row, dict):
        for rk in ("last_price", "live_price", "price", "lastClose", "close", "mid", "bid", "ask"):
            v = row.get(rk)
            if isinstance(v, (int, float)) and float(v) > 0:
                last_price = float(v)
                break

        if last_price is None:
            raw = row.get("raw")
            if isinstance(raw, dict):
                for rk in ("lastClose", "close", "mid", "bid", "ask"):
                    v = raw.get(rk)
                    if isinstance(v, (int, float)) and float(v) > 0:
                        last_price = float(v)
                        break

    if last_price is None and isinstance(basis, (int, float)) and float(basis) > 0:
        last_price = float(basis)

    snap_key = _opp_snapshot_key(sym_u, direction)

    # ---------- fresh price (prefer row live fields over snap/basis) ----------
    def _fresh_price(row_obj, snap_obj, fallback):
        if isinstance(row_obj, dict):
            for k in (
                "live", "live_price", "last_price", "price",
                "mid", "bid", "ask",
                "lastClose", "close",
            ):
                v = row_obj.get(k)
                try:
                    if v is not None:
                        vv = float(v)
                        if vv > 0:
                            return vv
                except Exception:
                    pass

            raw = row_obj.get("raw")
            if isinstance(raw, dict):
                for k in ("lastClose", "close", "mid", "bid", "ask"):
                    v = raw.get(k)
                    try:
                        if v is not None:
                            vv = float(v)
                            if vv > 0:
                                return vv
                    except Exception:
                        pass

        if isinstance(snap_obj, dict):
            for k in ("last_price", "price", "mid", "bid", "ask", "lastClose", "close"):
                v = snap_obj.get(k)
                if v is None:
                    try:
                        v = snap_obj.get(k.encode("utf-8"))
                    except Exception:
                        v = None
                try:
                    if v is not None:
                        vv = float(v)
                        if vv > 0:
                            return vv
                except Exception:
                    pass

        try:
            if fallback is not None:
                vv = float(fallback)
                return vv if vv > 0 else None
        except Exception:
            pass

        return None

    px = _fresh_price(row_obj=row, snap_obj=snap, fallback=last_price)
    if px is not None:
        last_price = px

    if last_price is None:
        return

    # ---------------- compute target if missing ----------------
    if target is None and basis and move_pct_1h is not None:
        try:
            pct = abs(float(move_pct_1h)) / 100.0
            if float(basis) > 0 and pct > 0:
                target = float(basis) * (1.0 + pct) if direction == "UP" else float(basis) * (1.0 - pct)
        except Exception:
            target = None

    # ---------------- compute realized move (basis-based, legacy) ----------------
    realized_move_pct = None
    if basis:
        try:
            realized_move_pct = (float(last_price) - float(basis)) / float(basis) * 100.0
        except Exception:
            realized_move_pct = None

    # ====================== ENTRY META (frozen) ======================
    meta = _entry_meta_from_snap()
    entry_sig = str(meta.get("entry_signal") or "").upper().strip()
    tp_price = meta.get("tp_price", None)
    sl_price = meta.get("sl_price", None)  # UI only
    entered = bool(meta.get("entry_triggered"))

    # ==========================================================
    # STRUCTURE EXIT (post-entry): sweep -> reclaim -> exit
    # SL is NOT a fixed price anymore.
    # Runs AFTER entry and BEFORE TP checks.
    # ==========================================================
    if entered and entry_sig in ("BUY", "SELL"):
        try:
            zl = meta.get("entry_zone_low")
            zh = meta.get("entry_zone_high")
            zv = meta.get("entry_zone_level")

            zl = float(zl) if isinstance(zl, (int, float)) else None
            zh = float(zh) if isinstance(zh, (int, float)) else None
            zv = float(zv) if isinstance(zv, (int, float)) else None

            if zl is not None and zh is not None and zv is not None:
                atr = (
                    meta.get("atr_1h") or meta.get("atr") or meta.get("atr14") or meta.get("atr14_1h")
                )
                atr = float(atr) if isinstance(atr, (int, float)) else None

                if atr is not None and atr > 0:
                    dev = str(meta.get("pinned_device") or meta.get("device") or meta.get("device_id") or "").strip()
                    bars = _get_closed_h1_bars(sym_u, dev) if dev else []
                    if not isinstance(bars, list):
                        bars = []

                    


                    if bars:
                        soft_wick_atr = float(os.getenv("XTL_EXIT_SOFT_WICK_ATR", "0.25"))
                        hard_close_atr = float(os.getenv("XTL_EXIT_HARD_CLOSE_ATR", "0.10"))
                        hard_break_atr = float(os.getenv("XTL_EXIT_HARD_BREAK_ATR", "0.60"))
                        max_soft_bars = int(os.getenv("XTL_EXIT_MAX_SOFT_BARS", "3"))
                        hard_close_bars = int(os.getenv("XTL_EXIT_HARD_CLOSE_BARS", "2"))
                        wait_bars = int(os.getenv("XTL_EXIT_RECLAIM_MAX_BARS", "3"))

                        ex = _load_exit_state(sym_u, entry_sig)
                        st = str(ex.get("state") or "OK").upper()

                        res = _sweep_break_state(
                            direction=entry_sig,   # BUY/SELL
                            bars=bars,
                            zone_low=float(zl),
                            zone_high=float(zh),
                            zone_level=float(zv),
                            atr=float(atr),
                            soft_wick_atr=soft_wick_atr,
                            hard_close_atr=hard_close_atr,
                            hard_break_atr=hard_break_atr,
                            max_soft_bars=max_soft_bars,
                            hard_close_bars=hard_close_bars,
                        )

                        state = str(res.get("state") or "").upper()

                        # HARD break => immediate exit
                        if state == "HARD_BREAK":
                            payload = {
                                "alert_id": event_id,
                                "symbol": sym_u,
                                "opp_direction": direction,
                                "direction": direction,
                                "alert_created_ms": alert_ms or now_ms,
                                "status": "exit",
                                "hit_target": False,
                                "exit_reason": "hard_break_exit",
                                "exit_ts": now_ms,
                                "exit_ts_ms": now_ms,
                                "last_status_ms": now_ms,
                                "updated_ms": now_ms,
                                "last_price": float(last_price),
                            }
                            payload.update(meta)
                            payload["tp_price"] = tp_price
                            payload["sl_price"] = sl_price

                            # realized move pct from ENTRY (directional for BUY/SELL)
                            try:
                                ep = meta.get("entry_price")
                                ep = float(ep) if ep is not None else None
                                lp0 = float(last_price)
                                if ep and ep > 0:
                                    mv = ((lp0 - ep) / ep) * 100.0
                                    if entry_sig == "SELL":
                                        mv = -mv
                                    payload["realized_move_pct"] = float(mv)
                                else:
                                    payload["realized_move_pct"] = None
                            except Exception:
                                payload["realized_move_pct"] = None

                            _save_alert_snapshot(sym_u, payload)
                            _log_trade_outcome(payload)
                            try:
                                _discord_notify_outcome("exit", payload)
                            except Exception:
                                pass

                            try:
                                R.hset(
                                    snap_key,
                                    mapping={
                                        "status": json.dumps("exit"),
                                        "exit_reason": json.dumps("hard_break_exit"),
                                        "exit_ts": json.dumps(now_ms),
                                        "last_status_ms": json.dumps(now_ms),
                                        "last_price": json.dumps(float(last_price)),
                                    },
                                )
                            except Exception:
                                pass

                            _clear_exit_state(sym_u, entry_sig)
                            _delete_live_snapshot(sym_u, direction)
                            _clear_tp_state(sym_u, entry_sig)
                            return

                        # WAIT for reclaim
                        if state == "WAIT_RECLAIM":
                            # initialize wait state
                            if st != "WAIT_RECLAIM":
                                ex = {
                                    "state": "WAIT_RECLAIM",
                                    "sweep_ts_ms": int(now_ms),
                                    "checks": 0,
                                    "wait_bars": int(wait_bars),
                                    "zone_low": float(zl),
                                    "zone_high": float(zh),
                                    "zone_level": float(zv),
                                    "entry_ts_ms": meta.get("entry_ts_ms"),
                                    "entry_price": meta.get("entry_price"),
                                }

                            ex["last_check_ms"] = int(now_ms)
                            ex["checks"] = int(ex.get("checks") or 0) + 1

                            # timeout => exit
                            if int(ex.get("checks") or 0) >= int(wait_bars):
                                payload = {
                                    "alert_id": event_id,
                                    "symbol": sym_u,
                                    "opp_direction": direction,
                                    "direction": direction,
                                    "alert_created_ms": alert_ms or now_ms,
                                    "status": "exit",
                                    "hit_target": False,
                                    "exit_reason": "sweep_no_reclaim_exit",
                                    "exit_ts": now_ms,
                                    "exit_ts_ms": now_ms,
                                    "last_status_ms": now_ms,
                                    "updated_ms": now_ms,
                                    "last_price": float(last_price),
                                }
                                payload.update(meta)
                                payload["tp_price"] = tp_price
                                payload["sl_price"] = sl_price

                                # realized move pct from ENTRY (directional for BUY/SELL)
                                try:
                                    ep = meta.get("entry_price")
                                    ep = float(ep) if ep is not None else None
                                    lp0 = float(last_price)
                                    if ep and ep > 0:
                                        mv = ((lp0 - ep) / ep) * 100.0
                                        if entry_sig == "SELL":
                                            mv = -mv
                                        payload["realized_move_pct"] = float(mv)
                                    else:
                                        payload["realized_move_pct"] = None
                                except Exception:
                                    payload["realized_move_pct"] = None

                                _save_alert_snapshot(sym_u, payload)
                                _log_trade_outcome(payload)
                                try:
                                    _discord_notify_outcome("exit", payload)
                                except Exception:
                                    pass

                                try:
                                    R.hset(
                                        snap_key,
                                        mapping={
                                            "status": json.dumps("exit"),
                                            "exit_reason": json.dumps("sweep_no_reclaim_exit"),
                                            "exit_ts": json.dumps(now_ms),
                                            "last_status_ms": json.dumps(now_ms),
                                            "last_price": json.dumps(float(last_price)),
                                        },
                                    )
                                except Exception:
                                    pass

                                _clear_exit_state(sym_u, entry_sig)
                                _delete_live_snapshot(sym_u, direction)
                                _clear_tp_state(sym_u, entry_sig)

                                return

                            _save_exit_state(sym_u, entry_sig, ex)
                        else:
                            # reclaimed / OK => clear wait state
                            if st == "WAIT_RECLAIM":
                                _clear_exit_state(sym_u, entry_sig)

        except Exception:
            pass
    # ====================== POST-ENTRY STRUCTURE TP (BOS -> exhaustion) ======================
    # Hybrid mode: this can exit earlier than tp_price.
    try:
        if entered and entry_sig in ("BUY", "SELL"):
            # You MUST feed closed bars here. Use whatever you already use for zone gate.
            # Example variable name: bars_h1 (newest last). Replace with your actual list.
            dev = str(meta.get("pinned_device") or meta.get("device") or meta.get("device_id") or "").strip()
            bars = _get_closed_h1_bars(sym_u, dev) if dev else []
            bars = bars if isinstance(bars, list) else []

            if bars:
                tp_struct = _tp_structure_exit(sym_u=sym_u, entry_sig=entry_sig, bars=bars, now_ms=now_ms)
                if isinstance(tp_struct, dict) and tp_struct.get("ok"):

                    payload = {
                        "alert_id": event_id,
                        "symbol": sym_u,
                        "opp_direction": direction,
                        "direction": direction,
                        "alert_created_ms": alert_ms or now_ms,
                        "status": "exit",
                        "hit_target": False,
                        "exit_reason": str(tp_struct.get("reason") or "tp_structure_exit"),
                        "exit_ts": now_ms,
                        "exit_ts_ms": now_ms,
                        "last_status_ms": now_ms,
                        "updated_ms": now_ms,
                        "last_price": float(last_price),
                        "tp_structure_meta": tp_struct.get("meta") or {},
                    }
                    payload.update(meta)
                    payload["tp_price"] = tp_price
                    payload["sl_price"] = sl_price

                    # realized move pct from ENTRY (directional)
                    try:
                        ep = meta.get("entry_price")
                        ep = float(ep) if ep is not None else None
                        lp0 = float(last_price)
                        if ep and ep > 0:
                            mv = ((lp0 - ep) / ep) * 100.0
                            if entry_sig == "SELL":
                                mv = -mv
                            payload["realized_move_pct"] = float(mv)
                        else:
                            payload["realized_move_pct"] = None
                    except Exception:
                        payload["realized_move_pct"] = None

                    _save_alert_snapshot(sym_u, payload)
                    _log_trade_outcome(payload)
                    try:
                        _discord_notify_outcome("exit", payload)
                    except Exception:
                        pass

                    try:
                        R.hset(
                            snap_key,
                            mapping={
                                "status": json.dumps("exit"),
                                "exit_reason": json.dumps(payload.get("exit_reason")),
                                "exit_ts": json.dumps(now_ms),
                                "last_status_ms": json.dumps(now_ms),
                                "last_price": json.dumps(float(last_price)),
                            },
                        )
                    except Exception:
                        pass

                    _clear_exit_state(sym_u, entry_sig)
                    _delete_live_snapshot(sym_u, direction)
                    _clear_tp_state(sym_u, entry_sig)

                    return
    except Exception:
        pass


    

    # ====================== EXPIRED ======================
    # Rule: expiry ONLY when NOT entered.
    expired = False
    try:
        if not entered:
            if opp_expire_ts and now_ms >= opp_expire_ts:
                expired = True
            elif alert_ms and (now_ms - alert_ms >= horizon_min * 60_000):
                expired = True
    except Exception:
        expired = False

    if expired:
        try:
            if has_alert:
                _mark_alert_expired(str(alert_id), now_ms)

            realized_from_entry = None
            try:
                if bool(meta.get("entry_triggered")) and meta.get("entry_price") is not None:
                    ep = float(meta["entry_price"])
                    lp0 = float(last_price)
                    if ep > 0:
                        mv = ((lp0 - ep) / ep) * 100.0
                        if entry_sig == "SELL":
                            mv = -mv
                        realized_from_entry = float(mv)
            except Exception:
                realized_from_entry = None

            payload = {
                "alert_id": event_id,
                "symbol": sym_u,
                "opp_direction": direction,
                "direction": direction,
                "alert_created_ms": alert_ms or now_ms,
                "status": "expired",
                "hit_target": False,
                "expired_ts": now_ms,
                "expired_ts_ms": now_ms,
                "last_status_ms": now_ms,
                "updated_ms": now_ms,
                "time_to_target_min": float(horizon_min),
                "last_price": float(last_price),
                "realized_move_pct": realized_from_entry if realized_from_entry is not None else realized_move_pct,
            }
            payload.update(meta)
            payload["tp_price"] = tp_price
            payload["sl_price"] = sl_price

            _save_alert_snapshot(sym_u, payload)
            _log_trade_outcome(payload)

            try:
                _discord_notify_outcome("expired", payload)
            except Exception:
                pass

            try:
                R.hset(
                    snap_key,
                    mapping={
                        "status": json.dumps("expired"),
                        "expired_ts": json.dumps(now_ms),
                        "last_status_ms": json.dumps(now_ms),
                        "last_price": json.dumps(float(last_price)),
                    },
                )
            except Exception:
                pass
        except Exception:
            pass

        _delete_live_snapshot(sym_u, direction)
        return
