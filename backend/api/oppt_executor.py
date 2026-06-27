# api/strategy/oppt_executor.py
# OPPT executor (paper trading end-to-end) - manager compatible
#
# Exports required by oppt_executor_manager.py:
#   - EXECUTOR_SLEEP_SEC
#   - tick_all_enabled_users(max_users=...)
#
# Paper trading:
#   - Opens positions on ENTRY (entry_triggered BUY/SELL with entry_price/tp/sl)
#   - Closes positions on HIT / SL_HIT / EXPIRED
#   - Stores open trades in Redis hash, closed trades in Redis list with PnL

from __future__ import annotations

import json
import os
import time
import logging
from typing import Any, Dict, List, Optional
from redis.exceptions import AuthenticationError, ConnectionError, TimeoutError
from api.prop_firms.prop_guard import compute_prop_check
from api.trend_endpoints import (
    _get_prop_config,
    _get_prop_risk_state,
    _reserve_prop_open_risk,
    _release_prop_open_risk,
)

from api.prop_firms.prop_config import SYMBOL_SPECS

import urllib.request


import redis
import uuid
log = logging.getLogger("uvicorn.error")

 

DISCORD_TRADE_WEBHOOK_URL = (
    os.getenv("DISCORD_TRADE_WEBHOOK_URL")
    or os.getenv("DISCORD_WEBHOOK_URL")
    or ""
).strip()


def _discord_trade_post(content: str) -> bool:
    if not DISCORD_TRADE_WEBHOOK_URL:
        return False
    try:
        data = json.dumps({"content": content}).encode("utf-8")
        req = urllib.request.Request(
            DISCORD_TRADE_WEBHOOK_URL,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=4) as resp:
            resp.read()
        return True
    except Exception:
        return False

def _sticky_dev_key(user_id: str, sym: str, tf: str = "M1") -> str:
    return f"xtl:sticky_device:{user_id}:{sym.upper()}:{tf.upper()}"

def _mt5_cmdq_key(dev_id: str) -> str:
    return f"xtl:mt5:cmdq:{dev_id}"

def _mt5_ack_key(job_id: str) -> str:
    return f"xtl:mt5:ack:{job_id}"
def _get_mt5_ack(job_id: str) -> dict | None:
    if not job_id:
        return None
    try:
        raw = R.get(_mt5_ack_key(job_id))
        if not raw:
            return None
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", "ignore")
        return json.loads(raw) if isinstance(raw, str) else raw
    except Exception:
        return None


def _zone_src_code(src) -> str:
    s = str(src or "").upper().strip()
    if not s:
        return "NA"
    if "BEST" in s or "BEST_SCORED_SR" in s or "BSR" in s:
        return "B"
    if "DISPLAY" in s or "H1_DISPLAY" in s or "DZ" in s:
        return "D"
    return "NA"
# -----------------------------------------------------------------------------
# Redis (AUTH SAFE)
# -----------------------------------------------------------------------------
# Use REDIS_URL from env (recommended). Example:
#   redis://:PASSWORD@127.0.0.1:6379/0
#   redis://default:PASSWORD@127.0.0.1:6379/0
REDIS_URL = os.getenv("REDIS_URL") or "redis://127.0.0.1:6379/0"
R = redis.from_url(REDIS_URL, decode_responses=True)

# -----------------------------------------------------------------------------
# Required by oppt_executor_manager.py
# -----------------------------------------------------------------------------
EXECUTOR_SLEEP_SEC = int(float(os.getenv("OPPT_EXECUTOR_SLEEP_SEC") or "2"))

# -----------------------------------------------------------------------------
# Keys
# -----------------------------------------------------------------------------
STATE_KEY = "xtl:strategy:oppt:state:{uid}"  # saved by routes_strategy_oppt.py
ENABLED_USERS_KEY = "xtl:strategy:oppt:enabled_users"
# OPPT alerts store (as used by trend_endpoints snapshots)
ALERT_INDEX_KEY = "xtl:trend:opp:h1:index"
ALERT_HASH_PREFIX = "xtl:trend:opp:h1:"  # + alert_id

# Paper trading store
OPEN_KEY = "xtl:strategy:oppt:open:{uid}"          # HASH: trade_id -> json
CLOSED_KEY = "xtl:strategy:oppt:closed:{uid}"      # LIST: json closed trades
EXECUTED_KEY = "xtl:strategy:oppt:executed:{uid}"  # SET: executed trade_id keys
LOCK_KEY = "xtl:strategy:oppt:lock:{uid}"          # lock per user
COOLDOWN_KEY = "xtl:strategy:oppt:cooldown:{uid}:{symbol}"  # exists => cooldown

ACTIVE_OPP_KEY = "xtl:trend:opp:active:{symbol}:{direction}"
ENTRY_CLAIM_KEY = "xtl:oppt:entry_claim:{alert_id}"


def _side_to_direction(side: str) -> str:
    s = str(side or "").upper().strip()
    if s == "BUY":
        return "UP"
    if s == "SELL":
        return "DOWN"
    return s

# -----------------------------------------------------------------------------
def now_ms() -> int:
    return int(time.time() * 1000)


def _sj(x: Any, default=None):
    if x is None:
        return default
    if isinstance(x, (bytes, bytearray)):
        x = x.decode("utf-8", "ignore")
    try:
        return json.loads(x)
    except Exception:
        return default


def _sf(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default

def _risk_usd_from_broker_position(symbol: str, entry: float, sl: float, lots: float) -> float:
    sym = str(symbol or "").upper().strip()
    spec = SYMBOL_SPECS.get(sym) or {}

    tick_size = float(
        spec.get("tick_size")
        or spec.get("point")
        or spec.get("pip_size")
        or 0
    )

    tick_value = float(
        spec.get("tick_value")
        or spec.get("pip_value_per_lot")
        or spec.get("pip_value")
        or 0
    )

    entry = float(entry or 0)
    sl = float(sl or 0)
    lots = float(lots or 0)

    if entry <= 0 or sl <= 0 or lots <= 0:
        return 0.0

    if tick_size <= 0 or tick_value <= 0:
        if sym == "XAUUSD":
            tick_size = 0.01
            tick_value = 1.0
        elif sym.endswith("JPY") and len(sym) == 6:
            tick_size = 0.01
            tick_value = 10.0
        elif sym.endswith("USD") and len(sym) == 6:
            tick_size = 0.0001
            tick_value = 10.0
        elif sym in ("USDCHF", "USDCAD"):
            tick_size = 0.0001
            tick_value = 10.0
        else:
            return 0.0

    return round((abs(entry - sl) / tick_size) * tick_value * lots, 2)

def _si(x: Any, default: int = 0) -> int:
    try:
        if x is None:
            return default
        return int(float(x))
    except Exception:
        return default


def _conf_rank(x: Optional[str]) -> int:
    s = (x or "").strip().lower()
    if s == "high":
        return 3
    if s == "medium":
        return 2
    if s == "low":
        return 1
    return 0


def _get_enabled_user_ids(limit: int = 500) -> list[str]:
    if R is None:
        return []
    try:
        raw = R.smembers(ENABLED_USERS_KEY) or set()
    except Exception:
        return []

    out: list[str] = []
    for x in raw:
        try:
            s = x.decode("utf-8", "ignore") if isinstance(x, (bytes, bytearray)) else str(x)
            s = s.strip()
            if s:
                out.append(s)
        except Exception:
            continue

    if limit and len(out) > limit:
        out = out[:limit]
    return out


def _zone_watch_key(sym: str, side: str, tf: str = "H1") -> str:
    return f"xtl:zone:watch:{(sym or '').upper().strip()}:{(side or '').upper().strip()}:{(tf or 'H1').upper().strip()}"


def _zone_cooldown_key(sym: str, side: str, tf: str = "H1") -> str:
    return f"xtl:zone:cooldown:{(sym or '').upper().strip()}:{(side or '').upper().strip()}:{(tf or 'H1').upper().strip()}"

def _clear_zone_watch_on_entry(sym: str, side: str, tf: str = "H1") -> None:
    return

def _pick_device_for_symbol(user_id: str, sym: str) -> str | None:
    sym_u = (sym or "").upper().strip()
    uid = str(user_id or "").strip()
    if not uid or not sym_u:
        return None

    def _clean_dev(x):
        if isinstance(x, (bytes, bytearray)):
            x = x.decode("utf-8", "ignore")
        return str(x or "").strip().strip('"').strip("'")

    def _device_is_online(dev_id: str) -> bool:
        dev_id = _clean_dev(dev_id)
        if not dev_id:
            return False
        try:
            h = R.hgetall(f"device:{dev_id}") or {}
        except Exception:
            return False
        if not h:
            return False

        def _hv(k):
            return h.get(k) or h.get(k.encode())

        status = _clean_dev(_hv("status")).lower()
        mt5_ok = _clean_dev(_hv("mt5_ok"))
        trade_allowed = _clean_dev(_hv("mt5_terminal_trade_allowed")).lower()
        try:
            hb = int(float(_clean_dev(_hv("last_heartbeat_ms")) or 0))
        except Exception:
            hb = 0

        # 3 minutes max age; enough for normal heartbeat jitter
        fresh = hb > 0 and (now_ms() - hb) <= 180000

        return bool(
            status == "online"
            and fresh
            and mt5_ok in ("1", "true", "True")
            and trade_allowed in ("true", "1", "yes")
        )

    # 1) HARD PRIORITY: current trend leader device.
    # This is the same device used by /trend/opportunities and zone gate.
    try:
        leader = _clean_dev(R.get(f"xtl:user:{uid}:trend:leader"))
        if leader and _device_is_online(leader):
            return leader
    except Exception:
        pass

    # 2) Sticky device is allowed only if still online/trade-ready.
    try:
        dev = _clean_dev(R.get(_sticky_dev_key(uid, sym_u, "M1")))
        if dev and _device_is_online(dev):
            return dev
    except Exception:
        pass

    # 3) Fallback: pick only online/trade-ready devices from user's set.
    try:
        devs = R.smembers(f"xtl:user:{uid}:devices") or set()
        best_dev = None
        best_hb = -1

        for x in devs:
            d = _clean_dev(x)
            if not d or not _device_is_online(d):
                continue

            try:
                h = R.hgetall(f"device:{d}") or {}
                hb = int(float(_clean_dev(h.get("last_heartbeat_ms") or h.get(b"last_heartbeat_ms")) or 0))
            except Exception:
                hb = 0

            if hb > best_hb:
                best_hb = hb
                best_dev = d

        if best_dev:
            return best_dev
    except Exception:
        pass

    return None


def _enqueue_mt5_market_order(
    user_id: str,
    sym: str,
    side: str,               # "BUY" | "SELL"
    volume: float,
    trade_id: str | None = None,
    sl: float | None = None,
    tp: float | None = None,
    comment: str = "XTL",
    kind: str = "ENTRY",     # "ENTRY" | "EXIT"
    exit_reason: str | None = None,
     mt5_account: str = "demo",
) -> dict:
    dev_id = _pick_device_for_symbol(user_id, sym)
    if not dev_id:
        return {"ok": False, "error": "no_device"}

    job_id = f"mt5_{uuid.uuid4().hex}"
    cmd = {
        "job_id": job_id,
        "type": "market_order",
        "mt5_account": (mt5_account or "demo"),
        "kind": kind,
        "exit_reason": exit_reason,
        "symbol": (sym or "").upper().strip(),
        "side": str(side or "").upper().strip(),
        "volume": float(volume or 0),
        "trade_id": trade_id,
        "sl": float(sl) if sl is not None else None,
        "tp": float(tp) if tp is not None else None,
        "comment": comment,
        "user_id": str(user_id),
        "created_at_ms": int(time.time() * 1000),
    }

    try:
        R.rpush(_mt5_cmdq_key(dev_id), json.dumps(cmd))
        # optional: keep queue from growing forever
        R.ltrim(_mt5_cmdq_key(dev_id), -200, -1)
    except Exception as e:
        return {"ok": False, "error": f"enqueue_failed:{type(e).__name__}"}

    return {"ok": True, "job_id": job_id, "device_id": dev_id}


def _enqueue_mt5_close_position(
    uid: str,
    symbol: str,
    ticket: int,
    qty: float,
    comment: str,
    trade_id: str,
    exit_reason: str,
    mt5_account: str,
) -> Dict[str, Any]:
    """Queue a hedging-safe close command (close by position ticket)."""
    dev_id = _pick_device_for_symbol(uid, symbol)
    if not dev_id:
        return {"ok": False, "error": "no_device"}
    try:
        ticket_i = int(ticket)
    except Exception:
        ticket_i = 0
    if ticket_i <= 0:
        return {"ok": False, "error": "missing_ticket"}

    payload = {
        "job_id": "mt5_" + uuid.uuid4().hex,
        "type": "close_position",
        "mt5_account": mt5_account,
        "symbol": symbol,
        "ticket": ticket_i,
        "qty": float(qty or 0.0),
        "comment": comment or "",
        "trade_id": trade_id or "",
        "exit_reason": exit_reason or "",
        "user_id": uid,
        "source": "oppt",
        "ts_ms": int(time.time() * 1000),
    }

    try:
        R.rpush(_mt5_cmdq_key(dev_id), json.dumps(payload, ensure_ascii=False))
        R.ltrim(_mt5_cmdq_key(dev_id), -200, -1)
    except Exception as e:
        return {"ok": False, "error": f"redis_rpush_failed:{type(e).__name__}"}

    return {"ok": True, "job_id": payload["job_id"], "device_id": dev_id}


def _state_defaults() -> dict:
    # must match your routes_strategy_oppt.py defaults
    return {
        "enabled": False,
        "execution_mode": "paper",   # paper | mt5
        "mt5_account": "demo",       # demo | live
        "qty": 1.0,
        "max_positions": 1,
        "cooldown_min": 0,
        "min_score": 0.0,
        "min_confidence": None,      # low|medium|high|None
        "started_at_ms": None,
        "updated_at_ms": None,
    }


def _load_state(uid: str) -> dict:
    key = STATE_KEY.format(uid=uid)
    raw = None
    try:
        raw = R.get(key)
    except Exception:
        raw = None
    if not raw:
        return _state_defaults()

    st = _sj(raw, {})
    if not isinstance(st, dict):
        return _state_defaults()

    base = _state_defaults()
    base.update(st)

    # normalize
    base["enabled"] = bool(base.get("enabled"))
    base["execution_mode"] = base.get("execution_mode") if base.get("execution_mode") in ("paper", "mt5") else "paper"
    base["mt5_account"] = base.get("mt5_account") if base.get("mt5_account") in ("demo", "live") else "demo"
    base["qty"] = _sf(base.get("qty"), 1.0) or 1.0
    base["max_positions"] = max(1, min(50, _si(base.get("max_positions"), 1)))
    base["cooldown_min"] = max(0, min(24 * 60, _si(base.get("cooldown_min"), 0)))
    base["min_score"] = max(0.0, _sf(base.get("min_score"), 0.0))
    mc = base.get("min_confidence")
    base["min_confidence"] = mc if mc in ("low", "medium", "high") else None
    # sync enabled set (CRITICAL)
    try:
        if base.get("enabled"):
            R.sadd(ENABLED_USERS_KEY, uid)
        else:
            R.srem(ENABLED_USERS_KEY, uid)
    except Exception:
        pass

    return base


# -----------------------------------------------------------------------------
# OPPT Alerts loader
# -----------------------------------------------------------------------------
def _load_recent_alert_rows(limit: int = 200) -> List[dict]:
    out: List[dict] = []
    try:
        ids = R.lrange(ALERT_INDEX_KEY, 0, max(0, limit - 1)) or []
    except Exception:
        return out

    seen: set[str] = set()
    for aid in ids:
        a = (aid or "").strip()
        if not a or a in seen:
            continue
        seen.add(a)

        key = f"{ALERT_HASH_PREFIX}{a}"
        try:
            h = R.hgetall(key) or {}
        except Exception:
            continue
        if not h:
            continue

        row: dict = {"alert_id": a}
        for k, v in h.items():
            # trend_endpoints usually stores values as json dumps
            row[k] = _sj(v, v)
        out.append(row)

    return out


def _alert_to_event(row: dict) -> Optional[dict]:
    """
    Normalizes an OPPT row into:
      - ENTRY event: {type:'ENTRY', trade_id, symbol, side, entry_price, tp_price, sl_price, score, confidence, uid?}
      - EXIT event:  {type:'EXIT',  trade_id, symbol, exit_reason, exit_price, uid?}

    Notes:
    - status typically: 'active' | 'hit' | 'expired' | 'sl_hit' (sometimes 'closed')
    - entry fields: entry_triggered, entry_signal, entry_price, entry_ts_ms
    """
    sym = (row.get("symbol") or "").upper().strip()
    if not sym:
        return None

    # if multi-user, keep uid if present; executor filters by uid when available
    uid = row.get("user_id") or row.get("uid") or row.get("owner_user_id") or None
    uid = str(uid) if uid not in (None, "", 0) else None

    status = str(row.get("status") or "").strip().lower()
    alert_id = str(row.get("alert_id") or "").strip()
    if not alert_id:
        return None

    # ---- pull raw/meta blocks once (used for fallbacks) ----
    raw = row.get("raw") if isinstance(row.get("raw"), dict) else {}
    entry_meta = row.get("entry_meta") if isinstance(row.get("entry_meta"), dict) else {}

    # ---- entry_ts: ensure stable trade_id even if entry_ts_ms missing ----
    entry_ts = _si(row.get("entry_ts_ms"), 0)
    if entry_ts <= 0:
        entry_ts = _si(
            row.get("alert_created_ms")
            or row.get("alert_created_ts_ms")
            or row.get("created_ms")
            or row.get("created_at_ms"),
            0,
        )

    trade_id = f"{alert_id}:{entry_ts}"  # stable per alert entry instance

    # ---- tp/sl: fall back to entry_meta/raw if needed ----
    tp = _sf(row.get("tp_price"), 0.0)
    if tp <= 0:
        tp = _sf(entry_meta.get("tp_price"), 0.0)
    if tp <= 0:
        tp = _sf(raw.get("tp_price"), 0.0)

    sl = _sf(row.get("sl_price"), 0.0)
    if sl <= 0:
        sl = _sf(entry_meta.get("sl_price"), 0.0)
    if sl <= 0:
        sl = _sf(raw.get("sl_price"), 0.0)

    eg = row.get("entry_gate") if isinstance(row.get("entry_gate"), dict) else {}
    zone_used = (
        eg.get("zone_used")
        or eg.get("zone")
        or row.get("zone_used")
        or row.get("active_zone")
        or {}
    )

    entry_zone = zone_used if isinstance(zone_used, dict) else {}

    entry_zone_meta = {
        "entry_zone": entry_zone or None,
        "entry_zone_low": _sf(entry_zone.get("low"), 0.0) if entry_zone else None,
        "entry_zone_high": _sf(entry_zone.get("high"), 0.0) if entry_zone else None,
        "entry_zone_level": _sf(entry_zone.get("level"), 0.0) if entry_zone else None,
        "entry_zone_tf": entry_zone.get("tf") if entry_zone else None,
        "entry_zone_kind": entry_zone.get("kind") if entry_zone else None,
        "entry_zone_source": entry_zone.get("zone_source") if entry_zone else None,
        "entry_zone_selection_model": entry_zone.get("selection_model") if entry_zone else None,
        "entry_gate_reason": eg.get("reason"),
        "trade_state": "ENTRY_READY",
    }

    # ---- EXIT ----
    if status in ("hit", "expired", "sl_hit", "closed"):
        reason = "HIT" if status == "hit" else ("SL_HIT" if status == "sl_hit" else "EXPIRED")

        exit_price = _sf(row.get("exit_price"), 0.0)

        last_price = _sf(
            row.get("last_price")
            or row.get("live")
            or row.get("live_price")
            or raw.get("lastClose")
            or raw.get("last_close"),
            0.0,
        )

        # For HIT/SL_HIT, prefer tp/sl if exit missing
        if reason == "HIT":
            if exit_price <= 0 and tp > 0:
                exit_price = tp
            if exit_price <= 0 and last_price > 0:
                exit_price = last_price

        elif reason == "SL_HIT":
            if exit_price <= 0 and sl > 0:
                exit_price = sl
            elif exit_price <= 0 and last_price > 0:
                exit_price = last_price

        else:  # EXPIRED (or closed)
            # Close at market (best-effort)
            if exit_price <= 0 and last_price > 0:
                exit_price = last_price

        # If still unknown, close at entry (0 pnl), but keep it consistent
        if exit_price <= 0:
            entry_price0 = _sf(row.get("entry_price"), 0.0)
            if entry_price0 <= 0:
                entry_price0 = _sf(entry_meta.get("entry_price"), 0.0)
            if entry_price0 > 0:
                exit_price = entry_price0

        return {
            "type": "EXIT",
            "uid": uid,
            "trade_id": trade_id,
            "symbol": sym,
            "exit_reason": reason,
            "exit_price": exit_price,
            "exit_meta": {
                "status": status,
                "used_last_price": (exit_price == last_price and last_price > 0),
                "last_price": last_price,
                "tp": tp,
                "sl": sl,
            },
        }

    
    # ---- ENTRY ----
    if status == "active":
        # Use resolved_dir from entry_gate (strategy direction) — not AI forecast decision
        eg = row.get("entry_gate") if isinstance(row.get("entry_gate"), dict) else {}
        _resolved = str(eg.get("resolved_dir") or "").upper().strip()
        side = str(row.get("entry_signal") or _resolved or row.get("decision") or "").upper().strip()
        raw = row.get("raw") if isinstance(row.get("raw"), dict) else {}

        last_price = _sf(
            raw.get("lastClose")
            or raw.get("last_close")
            or raw.get("ltp")
            or raw.get("price")
            or row.get("last_price")
            or row.get("price"),
            0.0,
        )
        # Override with live price from Redis if available
        try:
            _live_key = f"xtl:price:{sym}"
            _live_raw = R.get(_live_key)
            if _live_raw:
                _live_data = _sj(_live_raw, {})
                _live_px = _sf(_live_data.get("price"), 0.0)
                if _live_px > 0:
                    last_price = _live_px
        except Exception:
            pass

        # 1) Normal entry (already triggered upstream)
        if bool(row.get("entry_triggered")):
            entry_price = _sf(row.get("entry_price"), 0.0)
            if entry_price <= 0:
                entry_price = _sf(entry_meta.get("entry_price"), 0.0)

            if side not in ("BUY", "SELL") or entry_price <= 0:
                return None

            score = _sf(row.get("opp_score") or row.get("score"), 0.0)
            conf = str(row.get("opp_confidence") or row.get("confidence") or "").lower().strip()

            return {
                "type": "ENTRY",
                "uid": uid,
                "trade_id": trade_id,
                "symbol": sym,
                "side": side,
                "entry_price": entry_price,
                "tp_price": tp,
                "sl_price": sl,
                "score": score,
                "confidence": conf,
                "entry_ts_ms": entry_ts,
                **entry_zone_meta,
            }

        
        
        # 2) REV_OK = wait for live breakout only
        eg = row.get("entry_gate") if isinstance(row.get("entry_gate"), dict) else {}
        rs = eg.get("rev_state") if isinstance(eg.get("rev_state"), dict) else {}

        reason = str(eg.get("reason") or "").upper().strip()
        stage = str(eg.get("stage") or "").upper().strip()
        trade_state = str(row.get("trade_state") or "").upper().strip()

        # ------------------------------------------------------------
        # ENTRY_TIMEOUT: gate timed out — clear all stale RC fields
        # from snapshot so next cycle starts completely clean.
        # ------------------------------------------------------------
        if "ENTRY_TIMEOUT" in reason or stage == "ENTRY_TIMEOUT":
            try:
                hkey = f"{ALERT_HASH_PREFIX}{alert_id}"
                R.hset(hkey, mapping={
                    "rev_ok":            json.dumps(False),
                    "rev_ok_bar_hi":     json.dumps(0.0),
                    "rev_ok_bar_lo":     json.dumps(0.0),
                    "rev_ok_bar_close":  json.dumps(0.0),
                    "rev_ok_ms":         json.dumps(0),
                    "entry_triggered":   json.dumps(False),
                    "entry_price":       json.dumps(0.0),
                    "entry_signal":      json.dumps(""),
                    "entry_reason":      json.dumps(""),
                    "entry_ts_ms":       json.dumps(0),
                    "trade_state":       json.dumps("WATCH"),
                })
                R.expire(hkey, 7 * 24 * 3600)
            except Exception:
                pass
            return None  # no event — setup fully reset

        is_rev_ready = (
            "REV_OK" in reason
            or stage == "REV"
            or trade_state == "ENTRY_READY"
            or bool(eg.get("rev_ok"))
        )

        if is_rev_ready:
            trig_hi = _sf(rs.get("rev_ok_bar_hi") or eg.get("rev_ok_bar_hi"), 0.0)
            trig_lo = _sf(rs.get("rev_ok_bar_lo") or eg.get("rev_ok_bar_lo"), 0.0)

            crossed = False
            trig_level = trig_hi if side == "BUY" else trig_lo

            if trig_level > 0 and last_price > 0:
                bkey = _break_state_key(alert_id)

                prev_price = 0.0
                try:
                    raw_bs = R.get(bkey)
                    bs = _sj(raw_bs, {}) if raw_bs else {}
                    if isinstance(bs, dict):
                        prev_price = _sf(bs.get("last_price"), 0.0)
                except Exception:
                    prev_price = 0.0

                if side == "BUY":
                    crossed = bool(prev_price > 0 and prev_price < trig_level and last_price >= trig_level)
                elif side == "SELL":
                    crossed = bool(prev_price > 0 and prev_price > trig_level and last_price <= trig_level)

                try:
                    R.setex(
                       bkey,
                       24 * 3600,
                       json.dumps({
                           "alert_id": alert_id,
                           "symbol": sym,
                           "side": side,
                           "trigger_level": float(trig_level),
                           "last_price": float(last_price),
                           "prev_price": float(prev_price),
                           "crossed": bool(crossed),
                           "updated_ms": now_ms(),
                       }),
                    )
                except Exception:
                    pass

            if crossed:
                now_e = now_ms()

                try:
                    hkey = f"{ALERT_HASH_PREFIX}{alert_id}"
                    R.hset(
                        hkey,
                        mapping={
                            "entry_triggered": json.dumps(True),
                            "entry_signal": json.dumps(side),
                            "entry_price": json.dumps(float(last_price)),
                            "entry_ts_ms": json.dumps(int(now_e)),
                            "entry_reason": json.dumps(f"REV_OK_BREAK({float(trig_level)})"),
                            "entry_trigger_level": json.dumps(float(trig_level)),
                            "entry_trigger_type": json.dumps("REV_OK_BAR_BREAK"),
                            "entry_trigger_side": json.dumps("HIGH" if side == "BUY" else "LOW"),
                            "entry_live_px_at_trigger": json.dumps(float(last_price)),
                            "trade_state": json.dumps("ENTRY_READY"),
                            "entry_zone": json.dumps(entry_zone_meta.get("entry_zone")),
                            "entry_zone_low": json.dumps(entry_zone_meta.get("entry_zone_low")),
                            "entry_zone_high": json.dumps(entry_zone_meta.get("entry_zone_high")),
                            "entry_zone_level": json.dumps(entry_zone_meta.get("entry_zone_level")),
                            "entry_zone_tf": json.dumps(entry_zone_meta.get("entry_zone_tf")),
                            "entry_zone_kind": json.dumps(entry_zone_meta.get("entry_zone_kind")),
                            "entry_zone_source": json.dumps(entry_zone_meta.get("entry_zone_source")),
                            "entry_zone_selection_model": json.dumps(entry_zone_meta.get("entry_zone_selection_model")),
                        },
                    )
                    R.expire(hkey, 7 * 24 * 3600)
                except Exception:
                   pass

                try:
                    # Prefer stored watch_key; fallback to constructing it directly
                    # so zone is always released even if watch_key wasn't saved in snapshot
                    wkey = eg.get("watch_key") or rs.get("watch_key")
                    if not wkey and sym and side:
                        wkey = f"xtl:zone:watch:{sym.upper().strip()}:{side.upper().strip()}:H1"
                    if wkey:
                        try:
                            raw_w = R.get(str(wkey))
                            w = _sj(raw_w, {}) if raw_w else {}
                            if isinstance(w, dict):
                                w["state"] = "ENTRY_READY"
                                w["entry_ready"] = True
                                w["entry_ready_price"] = float(last_price)
                                w["entry_ready_ts_ms"] = int(now_e)
                                w["entry_signal"] = str(side)
                                w["entry_trigger_level"] = float(trig_level)
                                w["entry_trigger_type"] = "REV_OK_BAR_BREAK"
                                w["trade_state"] = "ENTRY_READY"
                                R.set(str(wkey), json.dumps(w))
                            else:
                                R.set(str(wkey), json.dumps({
                                    "state": "ORDER_PENDING",
                                    "direction": str(side),
                                    "tf": "H1",
                                    "entry_triggered": True,
                                    "entry_price": float(last_price),
                                    "entry_ts_ms": int(now_e),
                                    "entry_signal": str(side),
                                    "entry_trigger_level": float(trig_level),
                                    "entry_trigger_type": "REV_OK_BAR_BREAK",
                                    "trade_state": "ORDER_PENDING",
                                }))
                        except Exception:
                            pass
                except Exception:
                    pass

                score = _sf(row.get("opp_score") or row.get("score"), 0.0)
                conf = str(row.get("opp_confidence") or row.get("confidence") or "").lower().strip()

                return {
                    "type": "ENTRY",
                    "uid": uid,
                    "trade_id": trade_id,
                    "symbol": sym,
                    "side": side,
                    "entry_price": float(last_price),
                    "tp_price": tp,
                    "sl_price": sl,
                    "score": score,
                    "confidence": conf,
                    "entry_ts_ms": int(now_e),
                    "trigger_type": "REV_OK_BAR_BREAK",
                    "trigger_level": float(trig_level),
                    "trigger_side": "HIGH" if side == "BUY" else "LOW",
                    "live_px": float(last_price),
                    **entry_zone_meta,
                }

    return None


# -----------------------------------------------------------------------------
# Paper trading store helpers
# -----------------------------------------------------------------------------
def _list_open_trades(uid: str) -> List[dict]:
    try:
        raw = R.hgetall(OPEN_KEY.format(uid=uid)) or {}
    except Exception:
        raw = {}
    out: List[dict] = []
    for v in raw.values():
        j = _sj(v, None)
        if isinstance(j, dict):
            out.append(j)
    return out

def _break_state_key(alert_id: str) -> str:
    return f"xtl:oppt:break_state:{str(alert_id or '').strip()}"


def _open_trade(uid: str, pos: Dict[str, Any]) -> None:
    R.hset(OPEN_KEY.format(uid=uid), pos["trade_id"], json.dumps(pos))

def _clear_trade_lifecycle_keys(pos: Dict[str, Any]) -> None:
    try:
        sym = str(pos.get("symbol") or "").upper().strip()
        side = str(pos.get("side") or "").upper().strip()
        if not sym:
            return

        # clear both sides because opposite stale watch may exist
        for s in ("BUY", "SELL"):
            R.delete(_zone_watch_key(sym, s, "H1"))
            R.delete(_zone_watch_key(sym, s, "H4"))

        # clear active opportunity pointers
        R.delete(ACTIVE_OPP_KEY.format(symbol=sym, direction="UP"))
        R.delete(ACTIVE_OPP_KEY.format(symbol=sym, direction="DOWN"))

        
    except Exception:
        pass

def _remove_open_trade(uid: str, trade_id: str) -> None:
    try:
        R.hdel(OPEN_KEY.format(uid=uid), trade_id)
    except Exception:
        pass
def _save_state(uid: str, st: dict) -> None:
    key = STATE_KEY.format(uid=uid)
    st["updated_at_ms"] = now_ms()
    R.set(key, json.dumps(st))
    # Auto-manage enabled_users set
    try:
        if st.get("enabled"):
            R.sadd(ENABLED_USERS_KEY, uid)
        else:
            R.srem(ENABLED_USERS_KEY, uid)
    except Exception:
        pass

def _late_entry_max_move(sym: str) -> float:
    """
    Maximum allowed move from original/current RC trigger while waiting for
    prop/open-position capacity. Config later can override this; defaults:
      - normal FX: 3 pips
      - JPY pairs: 3 pips = 0.030
      - XAUUSD: $2.00
    """
    sym_u = str(sym or "").upper().strip()
    if sym_u == "XAUUSD":
        return float(os.getenv("XTL_LATE_ENTRY_XAUUSD_USD", "2.0"))
    if sym_u.endswith("JPY"):
        return float(os.getenv("XTL_LATE_ENTRY_JPY_PIPS", "3")) * 0.01
    return float(os.getenv("XTL_LATE_ENTRY_FX_PIPS", "3")) * 0.0001


def _entry_block_state(reason: str) -> str:
    r = str(reason or "").upper().strip()
    if "SAME_SYMBOL" in r:
        return "ENTRY_BLOCKED_SAME_SYMBOL"
    if "PROP" in r or "CAPACITY" in r or "MAX_OPEN" in r:
        return "ENTRY_BLOCKED_PROP"
    if "MARGIN" in r:
        return "ENTRY_BLOCKED_MARGIN"
    if "LOTS" in r:
        return "ENTRY_BLOCKED_LOTS"
    return "ENTRY_BLOCKED_PROP"


def _is_entry_blocked_state(state: str) -> bool:
    return str(state or "").upper().strip().startswith("ENTRY_BLOCKED")


def _clear_watchlist_entry_block(ev: dict, reason: str = "ENTRY_BLOCKED_CAPACITY") -> None:
    """
    Do NOT delete REV_OK watch on prop/same-symbol/capacity block.
    Preserve RC/zone/trigger and retry later with:
      - next_retry_ms throttle
      - future-RC protection
      - late-entry distance check
    """
    try:
        if str(ev.get("source") or "") != "watchlist":
            return

        sym = str(ev.get("symbol") or "").upper().strip()
        side = str(ev.get("side") or "").upper().strip()
        wk = str(ev.get("watch_key") or "").strip()
        if not wk and sym and side:
            wk = f"xtl:zone:watch:{sym}:{side}:H1"

        ck = str(ev.get("claim_key") or "").strip()
        if ck:
            R.delete(ck)

        if sym and side:
            R.delete(f"xtl:watch:break_state:{sym}:{side}:H1")

        block_state = _entry_block_state(reason)
        retry_sec = int(os.getenv("XTL_ENTRY_BLOCK_RETRY_SEC", "30"))
        max_move = _late_entry_max_move(sym)

        if wk:
            raw_w = R.get(wk)
            w = _sj(raw_w, {}) if raw_w else {}
            if isinstance(w, dict) and w:
                w["state"] = block_state
                w["trade_state"] = block_state
                w["entry_blocked"] = True
                w["entry_block_reason"] = str(reason)
                w["entry_blocked_at_ms"] = int(w.get("entry_blocked_at_ms") or now_ms())
                w["next_retry_ms"] = int(now_ms() + retry_sec * 1000)
                w["late_entry_max_move"] = float(max_move)
                w["entry_triggered"] = False
                w["entry_ready"] = False
                w.pop("entry_price", None)
                w.pop("entry_ts_ms", None)
                w.pop("entry_ready_price", None)
                w.pop("entry_ready_ts_ms", None)
                R.set(wk, json.dumps(w, separators=(",", ":")), ex=7 * 24 * 3600)

        log.warning(
            "[WATCHLIST] ENTRY_BLOCK_PRESERVE sym=%s side=%s state=%s reason=%s watch_key=%s claim_key=%s retry_sec=%s max_move=%s",
            sym, side, block_state, reason, wk, ck, retry_sec, max_move,
        )
    except Exception as e:
        log.warning("[WATCHLIST] ENTRY_BLOCK_PRESERVE_FAILED reason=%s err=%r", reason, e)


def _close_trade(uid: str, pos: Dict[str, Any], exit_price: float, reason: str, meta: Optional[dict] = None) -> None:
    side = str(pos.get("side") or "").upper().strip()
    qty = _sf(pos.get("qty"), 1.0)
    entry = _sf(pos.get("entry_price"), 0.0)

    pnl = 0.0
    if side == "BUY":
        pnl = (exit_price - entry) * qty
    elif side == "SELL":
        pnl = (entry - exit_price) * qty

    closed = dict(pos)
    closed["exit_price"] = float(exit_price)
    closed["exit_reason"] = str(reason)
    closed["pnl"] = float(pnl)
    closed["closed_at_ms"] = now_ms()
    closed["status"] = "closed"
    closed["trade_state"] = "EXITED"
    try:
        _release_prop_open_risk(
            trade_id=str(pos.get("trade_id") or ""),
            result=str(reason or "").lower(),
            pnl_usd=float(closed.get("pnl") or 0.0),
        )
    except Exception as e:
        log.warning(
            "[PROP] RELEASE_FAILED trade_id=%s reason=%s err=%r",
            pos.get("trade_id"),
            reason,
            e,
        )

    try:
        sym = str(pos.get("symbol") or "").upper().strip()
        side0 = str(pos.get("side") or "").upper().strip()
        if sym and side0 in ("BUY", "SELL"):
            R.setex(
                _zone_cooldown_key(sym, side0, "H1"),
                15 * 60,
                json.dumps({
                    "symbol": sym,
                    "side": side0,
                    "tf": "H1",
                    "reason": str(reason),
                    "closed_at_ms": now_ms(),
                    "trade_id": str(pos.get("trade_id") or ""),
                }),
            )
    except Exception:
        pass
    if meta and isinstance(meta, dict):
        closed["exit_meta"] = meta

    R.lpush(CLOSED_KEY.format(uid=uid), json.dumps(closed))
    # keep last 500
    try:
        R.ltrim(CLOSED_KEY.format(uid=uid), 0, 499)
    except Exception:
        pass

    _remove_open_trade(uid, str(pos.get("trade_id") or ""))
    _clear_trade_lifecycle_keys(pos)


# -----------------------------------------------------------------------------
# One user tick
# -----------------------------------------------------------------------------
def tick_user(uid: str) -> None:
    st = _load_state(uid)
    if not st.get("enabled"):
        return

    # execution mode
    exec_mode = str(st.get("execution_mode") or "paper").strip().lower()
    if exec_mode not in ("paper", "mt5"):
        exec_mode = "paper"

    mt5_account = str(st.get("mt5_account") or "demo").strip().lower()
    if mt5_account not in ("demo", "live"):
        mt5_account = "demo"


    qty = _sf(st.get("qty"), 1.0)
    qty_fx = _sf(st.get("qty_fx"), 0.0)
    qty_metals = _sf(st.get("qty_metals"), 0.0)

    def _is_fx_symbol(s: str) -> bool:
        s = (s or "").upper().strip()
        return len(s) == 6 and s.isalpha()

    def _is_metal_symbol(s: str) -> bool:
        s = (s or "").upper().strip()
        return s in ("XAUUSD", "XAGUSD")

    max_positions = max(1, min(50, int(st.get("max_positions") or 1)))
    cooldown_min = int(st.get("cooldown_min") or 0)
    min_score = _sf(st.get("min_score"), 0.0)
    min_conf = st.get("min_confidence")
    min_conf_r = _conf_rank(min_conf) if min_conf else 0

    open_trades = _list_open_trades(uid)
    # -------------------------------------------------
    # 0) MT5 ACK RECONCILIATION (update open trades)
    # -------------------------------------------------
    try:
        for pos in list(open_trades or []):
            if str(pos.get("execution_mode") or "").lower() != "mt5":
                continue
            if str(pos.get("status") or "").lower() not in ("sent", "pending"):
                continue

            job_id = str(pos.get("mt5_job_id") or "").strip()
            if not job_id:
                continue

            ack = _get_mt5_ack(job_id)
            if not ack:
                continue

            # attach ack to position for UI/debug
            pos["mt5_ack"] = ack
            pos["mt5_acked_at_ms"] = ack.get("acked_at_ms")

            if bool(ack.get("ok")):
                pos["status"] = "filled"
                pos["trade_state"] = "TRADE_ACTIVE"
                try:
                    sym0 = str(pos.get("symbol") or "").upper().strip()
                    side0 = str(pos.get("side") or "").upper().strip()
                    wk = _zone_watch_key(sym0, side0, "H1")
                    raw_w = R.get(wk)
                    w = _sj(raw_w, {}) if raw_w else {}
                    if isinstance(w, dict) and w:
                        w["state"] = "TRADE_ACTIVE"
                        w["trade_state"] = "TRADE_ACTIVE"
                        w["entry_triggered"] = True
                        w["mt5_ticket"] = pos.get("mt5_ticket")
                        w["mt5_fill_price"] = pos.get("mt5_fill_price")
                        w["mt5_acked_at_ms"] = pos.get("mt5_acked_at_ms")
                        R.set(wk, json.dumps(w))
                except Exception:
                    pass
                try:
                    res = ack.get("result") or {}
                    # optional: keep MT5 ticket/price if available
                    if isinstance(res, dict):
                        if res.get("ticket") is not None:
                            pos["mt5_ticket"] = res.get("ticket")
                        if res.get("price") is not None:
                            pos["mt5_fill_price"] =  res.get("price")
                            # IMPORTANT: for MT5-filled trades, store real fill as entry
                            try:
                                fp = float(res.get("price"))
                                if fp > 0:
                                    pos["entry_price"] = fp
                            except Exception:
                                pass
                except Exception:
                    pass

                _open_trade(uid, pos)  # update stored open trade

                # Durable ticket->zone map: persist entry zone keyed by MT5 ticket so that
                # broker_repair can recover the zone after an agent restart (when the watch
                # state may no longer hold zone_used). Independent of watch + trade record.
                try:
                    _tk = int(pos.get("mt5_ticket") or 0)
                    if _tk > 0:
                        _zmeta = {
                            "entry_zone":       pos.get("entry_zone"),
                            "entry_zone_low":   pos.get("entry_zone_low"),
                            "entry_zone_high":  pos.get("entry_zone_high"),
                            "entry_zone_level": pos.get("entry_zone_level"),
                            "entry_zone_tf":    pos.get("entry_zone_tf"),
                            "entry_zone_kind":  pos.get("entry_zone_kind"),
                        }
                        if _zmeta.get("entry_zone") or _zmeta.get("entry_zone_level"):
                            R.set(f"xtl:trade:zone_by_ticket:{_tk}",
                                  json.dumps(_zmeta), ex=7*24*3600)  # 7-day TTL
                except Exception:
                    pass

                # Do NOT clear zone watch after MT5 fill.
                # Watch must remain TRADE_ACTIVE until MT5 close reconciliation cleans it.
                # _clear_zone_watch_on_entry(pos.get("symbol"), pos.get("side"), "H1")


                # mark executed ONLY when MT5 ack ok (filled)
                try:
                    ex_key2 = EXECUTED_KEY.format(uid=uid)
                    tid2 = str(pos.get("trade_id") or "").strip()
                    if tid2:
                        R.sadd(ex_key2, tid2)
                        R.expire(ex_key2, 7 * 24 * 3600)
                except Exception:
                    pass


            else:
                pos["status"] = "failed"
                pos["mt5_error"] = ack.get("error") or (ack.get("result") or {}).get("error")

                # Close immediately in history (PnL=0) so UI doesn't keep it "open"
                try:
                    _close_trade(
                        uid,
                        pos,
                        float(pos.get("entry_price") or 0.0),
                        "ENTRY_FAIL",
                        meta={"mt5_ack": ack},
                    )
                finally:
                    _remove_open_trade(uid, str(pos.get("trade_id") or ""))
    except Exception:
        pass

        
       
       
    # refresh open_trades after ACK reconciliation
    # refresh open_trades after ACK reconciliation
    open_trades = _list_open_trades(uid)

    # -------------------------------------------------
    # 0b) MT5 POSITION RECONCILIATION (broker truth)
    # -------------------------------------------------
    try:
        if exec_mode == "mt5":
            tickets_by_dev = {}

            for pos in list(open_trades or []):
                if str(pos.get("execution_mode") or "").lower() != "mt5":
                    continue
                pos_status = str(pos.get("status") or "").lower().strip()
                if pos_status not in ("sent", "pending", "filled"):
                    continue

                dev_id = str(pos.get("device_id") or "").strip()
                if not dev_id:
                    continue

                try:
                    ticket = int(pos.get("mt5_ticket") or 0)
                except Exception:
                    continue
                if ticket <= 0:
                    continue

                keys_to_try = []
                if dev_id:
                    keys_to_try.append(f"xtl:mt5:pos:{dev_id}:{mt5_account}")

                try:
                    leader_dev = str(R.get(f"xtl:user:{uid}:trend:leader") or "").strip().strip('"')
                    if leader_dev:
                        keys_to_try.append(f"xtl:mt5:pos:{leader_dev}:{mt5_account}")
                except Exception:
                    pass

                try:
                    for k in R.scan_iter(f"xtl:mt5:pos:*:{mt5_account}"):
                        ks = str(k)
                        if ks not in keys_to_try:
                            keys_to_try.append(ks)
                except Exception:
                    pass
                open_tickets = set()

                raw = None
                key = None

                # Prefer snapshot that actually contains this ticket
                for k in keys_to_try:
                    r0 = R.get(k)
                    if not r0:
                        continue

                    arr0 = _sj(r0, [])
                    if not isinstance(arr0, list):
                        continue

                    found_ticket = False
                    for p0 in arr0:
                        if not isinstance(p0, dict):
                            continue
                        try:
                            if int(p0.get("ticket") or 0) == int(ticket):
                                found_ticket = True
                                break
                        except Exception:
                            pass

                    if found_ticket:
                        raw = r0
                        key = k
                        break

                # Fallback: any available snapshot
                if raw is None:
                    for k in keys_to_try:
                        r0 = R.get(k)
                        if r0:
                            raw = r0
                            key = k
                            break

                if raw is None:
                    log.warning(
                        "[OPPT] BROKER_RECON snapshot_unavailable uid=%s sym=%s ticket=%s keys=%s",
                        uid, pos.get("symbol"), ticket, keys_to_try
                    )

                    # Snapshot unavailable is NOT broker truth.
                    # Do not remove local open trades here.
                    # Only close/remove when snapshot exists and ticket is missing.
                    continue
                    
                
                for p in _sj(raw, []):
                    if isinstance(p, dict) and p.get("ticket") is not None:
                        try:
                            open_tickets.add(int(p["ticket"]))
                        except Exception:
                            pass

                # Broker snapshot exists and our ticket is missing.
                # Treat broker as source of truth.
                log.warning(
                    "[OPPT] BROKER_RECON uid=%s sym=%s ticket=%s broker_tickets=%s",
                    uid,
                    pos.get("symbol"),
                    ticket,
                    list(open_tickets),
                )
                if ticket in open_tickets:
                    try:
                        prop_cfg = _get_prop_config()
                        tid0 = str(pos.get("trade_id") or "").strip()

                        if bool(prop_cfg.get("enabled")) and tid0:
                            risk_state0 = _get_prop_risk_state()
                            
                            already_reserved = any(
                                str(x.get("trade_id") or "") == tid0
                                for x in (risk_state0.get("open_positions") or [])
                                if isinstance(x, dict)
                            )
                            

                            if not already_reserved:
                                sym0 = str(pos.get("symbol") or "").upper().strip()
                                side0 = str(pos.get("side") or "").upper().strip()
                                entry0 = _sf(pos.get("entry_price") or pos.get("mt5_fill_price"), 0.0)
                                sl0 = _sf(pos.get("sl_price"), 0.0)
                                lots0 = _sf(pos.get("qty"), 0.0)

                                risk_usd0 = _risk_usd_from_broker_position(
                                    sym0,
                                    entry0,
                                    sl0,
                                    lots0,
                                )

                                if risk_usd0 > 0:
                                    _reserve_prop_open_risk(
                                        tid0,
                                        {
                                            "trade_id": tid0,
                                            "symbol": sym0,
                                            "side": side0,
                                            "risk_usd": float(risk_usd0),
                                            "risk_pct": 0.0,
                                            "lots": float(lots0),
                                            "entry": float(entry0),
                                            "sl": float(sl0),
                                            "tp": float(pos.get("tp_price") or 0),
                                            "firm": prop_cfg.get("firm"),
                                            "phase": prop_cfg.get("phase"),
                                            "source": "broker_recon_missing_reserve",
                                            "mt5_ticket": ticket,
                                            "device_id": str(pos.get("device_id") or ""),
                                            "reserved_ts_ms": now_ms(),
                                        },
                                    )

                                    log.warning(
                                        "[PROP] BROKER_RECON_MISSING_RESERVE uid=%s sym=%s side=%s ticket=%s risk_usd=%s lots=%s",
                                        uid, sym0, side0, ticket, risk_usd0, lots0,
                                    )
                    except Exception as e:
                        log.warning(
                            "[PROP] BROKER_RECON_MISSING_RESERVE_FAILED uid=%s ticket=%s err=%r",
                            uid, ticket, e,
                        )
                if ticket not in open_tickets:
                    log.warning(
                        "[OPPT] BROKER_CLOSED uid=%s sym=%s ticket=%s status=%s",
                        uid, pos.get("symbol"), ticket, pos_status
                    )
                    try:
                        lp = _sf(pos.get("last_price"), 0.0) or _sf(pos.get("entry_price"), 0.0)
                        _close_trade(
                            uid,
                            pos,
                            float(lp),
                            "BROKER_CLOSED",
                            meta={
                                "source": "mt5_position_reconciliation",
                                "manual_close_detected": True,
                                "broker_snapshot_key": key,
                                "broker_open_tickets": list(open_tickets),
                                "local_status": pos_status,
                            },
                        )
                    finally:
                        _remove_open_trade(uid, str(pos.get("trade_id") or ""))
    except Exception:
        pass
    # -------------------------------------------------
    # 0c) MT5 ORPHAN POSITION REPAIR
    # Broker has position but Redis open registry is missing.
    # This repairs TRADE_ACTIVE gate after Redis cleanup/restart.
    # -------------------------------------------------
    try:
        if exec_mode == "mt5":
            open_trades_now = _list_open_trades(uid)
            known_tickets = set()
            for t in open_trades_now:
                try:
                    tk = int(t.get("mt5_ticket") or 0)
                    if tk > 0:
                        known_tickets.add(tk)
                except Exception:
                    pass

            pos_keys = list(R.scan_iter(f"xtl:mt5:pos:*:{mt5_account}"))

            for pk in pos_keys:
                rawp = R.get(pk)
                broker_positions = _sj(rawp, []) if rawp else []
                if not isinstance(broker_positions, list):
                    continue

                dev_from_key = str(pk).split(":")[3] if len(str(pk).split(":")) >= 5 else ""

                for bp in broker_positions:
                    if not isinstance(bp, dict):
                        continue

                    try:
                        ticket = int(bp.get("ticket") or 0)
                    except Exception:
                        ticket = 0
                    if ticket <= 0 or ticket in known_tickets:
                        continue

                    sym = str(bp.get("symbol") or "").upper().strip()
                    side = str(bp.get("side") or "").upper().strip()
                    if not sym or side not in ("BUY", "SELL"):
                        continue

                    # RACE GUARD: do not repair a broker position when a recently
                    # placed order for this sym/side is still awaiting its ack/ticket
                    # writeback. The fill ack round-trip can take minutes; without
                    # this guard the detector repairs the position before mt5_ticket
                    # lands, creating a duplicate record and double-reserving risk.
                    _inflight = False
                    try:
                        for _t in open_trades_now:
                            if (str(_t.get("symbol") or "").upper() == sym
                                    and str(_t.get("side") or "").upper() == side):
                                _st = str(_t.get("status") or "").lower()
                                _ts = str(_t.get("trade_state") or "").upper()
                                _op = int(_t.get("opened_at_ms") or 0)
                                if (_st in ("sent", "pending", "filled")
                                        or _ts in ("ORDER_PENDING", "TRADE_ACTIVE")
                                        or (_op > 0 and (now_ms() - _op) < 600000)):
                                    _inflight = True
                                    break
                    except Exception:
                        _inflight = False
                    if _inflight:
                        continue  # let the ack/writeback finish before repairing

                    comment = str(bp.get("comment") or "")
                    magic = int(bp.get("magic") or 0)

                    # only repair XTL trades, not random manual trades
                    if magic != 20251227 and not comment.upper().startswith("XTL"):
                        continue

                    entry_px = _sf(bp.get("price_open"), 0.0)
                    qty0 = _sf(bp.get("volume"), 0.0)
                    _entry_zone = None
                    _ez_from_ticket = {}
                    # 1) PREFERRED: durable ticket->zone map written at placement.
                    #    Survives agent restarts (unlike the watch's zone_used).
                    try:
                        _zr = R.get(f"xtl:trade:zone_by_ticket:{ticket}")
                        if _zr:
                            _ez_from_ticket = _sj(_zr, {}) or {}
                            _zz = _ez_from_ticket.get("entry_zone")
                            if isinstance(_zz, dict):
                                _entry_zone = _zz
                    except Exception:
                        _entry_zone = None
                    # 2) FALLBACK: watch zone_used (may be stale/gone after restart)
                    if _entry_zone is None:
                        try:
                            _wk = f"xtl:zone:watch:{sym}:{side}:H1"
                            _wr = R.get(_wk)
                            _wj = _sj(_wr, {}) if _wr else {}
                            if isinstance(_wj, dict):
                                _zu = _wj.get("zone_used") or _wj.get("planned_zone")
                                if isinstance(_zu, dict):
                                    _entry_zone = _zu
                        except Exception:
                            _entry_zone = None

                    repaired = {
                        "trade_id": f"BROKER_REPAIR:{sym}:{side}:{ticket}",
                        "symbol": sym,
                        "side": side,
                        "entry_price": float(entry_px),
                        "qty": float(qty0),
                        "tp_price": _sf(bp.get("tp"), 0.0) or None,
                        "sl_price": _sf(bp.get("sl"), 0.0) or None,
                        # Use XTL server UTC time for broker-repaired positions.
                        # MT5 bp.time is broker-server-time, not UTC, so using it directly creates a 3h shift.
                        "opened_at_ms": now_ms(),
                        "source": "broker_repair",
                        "execution_mode": "mt5",
                        "device_id": dev_from_key,
                        "status": "filled",
                        "trade_state": "TRADE_ACTIVE",
                        "mt5_ticket": ticket,
                        "mt5_fill_price": float(entry_px),
                        "broker_snapshot_key": str(pk),
                        "broker_comment": comment,
                        "broker_magic": magic,
                        "broker_profit": _sf(bp.get("profit"), 0.0),
                        "broker_price_current": _sf(bp.get("price_current"), 0.0),
                        "repaired_at_ms": now_ms(),
                        "entry_zone": _entry_zone,
                        "entry_zone_low": _sf((_entry_zone or {}).get("low"), 0.0) if isinstance(_entry_zone, dict) else None,
                        "entry_zone_high": _sf((_entry_zone or {}).get("high"), 0.0) if isinstance(_entry_zone, dict) else None,
                        "entry_zone_level": _sf((_entry_zone or {}).get("level"), 0.0) if isinstance(_entry_zone, dict) else None,
                        "entry_zone_tf": (_entry_zone or {}).get("tf") if isinstance(_entry_zone, dict) else None,
                        "entry_zone_kind": (_entry_zone or {}).get("kind") if isinstance(_entry_zone, dict) else None,
                        "entry_zone_missing": not isinstance(_entry_zone, dict),
                        "repair_source": "broker_snapshot",
                    }

                    _open_trade(uid, repaired)
                    known_tickets.add(ticket)
                    try:
                        prop_cfg = _get_prop_config()

                        if bool(prop_cfg.get("enabled")):
                            risk_usd = _risk_usd_from_broker_position(
                                sym,
                                entry_px,
                                float(repaired.get("sl_price") or 0),
                                qty0,
                            )

                            if risk_usd > 0:
                                _reserve_prop_open_risk(
                                    repaired["trade_id"],
                                    {
                                        "trade_id": repaired["trade_id"],
                                        "symbol": sym,
                                        "side": side,
                                        "risk_usd": float(risk_usd),
                                        "risk_pct": 0.0,
                                        "lots": float(qty0),
                                        "entry": float(entry_px),
                                        "sl": float(repaired.get("sl_price") or 0),
                                        "tp": float(repaired.get("tp_price") or 0),
                                        "firm": prop_cfg.get("firm"),
                                        "phase": prop_cfg.get("phase"),
                                        "source": "broker_repair",
                                        "mt5_ticket": ticket,
                                        "device_id": dev_from_key,
                                        "reserved_ts_ms": now_ms(),
                                    },
                            )

                            log.warning(
                                "[PROP] BROKER_REPAIR_RESERVED uid=%s sym=%s side=%s ticket=%s risk_usd=%s lots=%s",
                                uid, sym, side, ticket, risk_usd, qty0,
                            )
                        else:
                            log.warning(
                                "[PROP] BROKER_REPAIR_RESERVE_SKIPPED uid=%s sym=%s side=%s ticket=%s reason=no_sl_or_risk",
                                uid, sym, side, ticket,
                            )

                    except Exception as e:
                        log.warning(
                            "[PROP] BROKER_REPAIR_RESERVE_FAILED uid=%s sym=%s side=%s ticket=%s err=%r",
                            uid, sym, side, ticket, e,
                        )

                    log.warning(
                        "[OPPT] BROKER_REPAIR_OPEN uid=%s sym=%s side=%s ticket=%s key=%s",
                        uid, sym, side, ticket, pk
                    )
    except Exception as e:
        log.warning("[OPPT] BROKER_REPAIR_OPEN failed uid=%s err=%r", uid, e)

    # final authoritative state
    open_trades = _list_open_trades(uid)
    open_by_id = {t.get("trade_id"): t for t in open_trades if t.get("trade_id")}

    

   
    qty_by_symbol = st.get("qty_by_symbol") or {}
    has_overrides = isinstance(qty_by_symbol, dict) and len(qty_by_symbol) > 0
    # TEMP: allow fallback qty while stabilizing (set strict later)
    strict_overrides = bool(st.get("strict_qty_overrides"))  # default False if missing

    def _qty_for_symbol(sym: str) -> float:
        sym_u = (sym or "").upper().strip()
        
        # If overrides exist, require explicit per-symbol qty
        if has_overrides:
            v = qty_by_symbol.get(sym_u)
            try:
                v0 = float(v) if v is not None else 0.0
            except Exception:
                v0 = 0.0
            # If a valid override is provided, use it
            if v0 > 0:
                return float(v0)

            # If strict mode enabled, REQUIRE explicit per-symbol qty (old behavior)
            if strict_overrides:
                return 0.0
            # else: FALLBACK to class/default qty (stability mode)
        # class-based qty
        if _is_metal_symbol(sym_u) and qty_metals > 0:
            return float(qty_metals)
        if _is_fx_symbol(sym_u) and qty_fx > 0:
            return float(qty_fx)

        # default
        return float(qty)

    # load events
    
    rows = _load_recent_alert_rows(limit=50)
    events: List[dict] = []
    for r in rows:
        ev = _alert_to_event(r)
        if not ev:
            continue
        # SAFETY: in MT5 mode, never trade events missing uid
        if exec_mode == "mt5" and not ev.get("uid"):
            # TEMP: allow single-user setups while stabilizing
            # continue
            pass
      
        if ev.get("uid") and str(ev.get("uid")) != str(uid):
            continue
        events.append(ev)
    # -------------------------------------------------
    # WATCHLIST ENTRY EVENTS
    # Source of truth for live strategy execution.
    # OPPT rows are advisory/history only.
    # -------------------------------------------------
    try:
        for wkey in R.scan_iter("xtl:zone:watch:*:*:H1"):
            try:
                raw_w = R.get(wkey)
                watch = _sj(raw_w, {}) if raw_w else {}
                if not isinstance(watch, dict) or not watch:
                    continue

                parts = str(wkey).split(":")
                if len(parts) < 6:
                    continue

                sym_w = parts[3].upper().strip()
                side_w = parts[4].upper().strip()

                if side_w not in ("BUY", "SELL"):
                    continue

                state_w = str(watch.get("state") or "").upper().strip()
                # -------------------------------------------------
                # SELF-HEAL: stale ORDER_PENDING without MT5 job
                # This means old code marked pending before enqueue.
                # Do not delete the zone. Re-arm same RC/zone for execution.
                # -------------------------------------------------

                if state_w == "ORDER_PENDING":
                    job_id = str(watch.get("mt5_job_id") or "").strip()
                    ticket = str(watch.get("mt5_ticket") or "").strip()
                    entry_ts = _si(watch.get("entry_ts_ms"), 0)
                    pending_age_ms = (now_ms() - entry_ts) if entry_ts > 0 else 0

                    # Market orders should not remain ORDER_PENDING forever.
                    # If no broker ticket appears within 5 minutes, mark as failed.
                    stale_pending_timeout = (
                        entry_ts > 0
                        and pending_age_ms > (5 * 60 * 1000)
                        and not ticket
                    )

                    if stale_pending_timeout:
                        watch["state"] = "ORDER_FAILED"
                        watch["trade_state"] = "ORDER_FAILED"
                        watch["status"] = "expired"
                        watch["exit_reason"] = "ORDER_PENDING_TIMEOUT"
                        watch["closed_at_ms"] = now_ms()
                        watch["pending_age_ms"] = int(pending_age_ms)
                        watch["cleanup_source"] = "oppt_executor_watch_recon"

                        R.set(str(wkey), json.dumps(watch), ex=7 * 24 * 3600)

                        log.warning(
                            "[WATCHLIST] ORDER_PENDING_TIMEOUT sym=%s side=%s job_id=%s age_ms=%s key=%s",
                            sym_w, side_w, job_id, pending_age_ms, wkey
                        )

                        # --- FIX: cleanup failed MT5 market order completely ---
                        # Release prop reservation and remove stale open hash record.
                        # Use only the stored trade_id; never reconstruct it here.
                        try:
                            _tid_fail = str(watch.get("trade_id") or "").strip()
                            if _tid_fail:
                                _release_prop_open_risk(
                                    trade_id=_tid_fail,
                                    result="order_timeout",
                                    pnl_usd=0.0,
                                )
                                _remove_open_trade(uid, _tid_fail)
                                log.warning(
                                    "[OPPT] ORDER_FAIL_CLEANUP uid=%s tid=%s sym=%s side=%s reason=order_pending_timeout",
                                    uid, _tid_fail, sym_w, side_w,
                                )
                            else:
                                log.warning(
                                    "[OPPT] ORDER_FAIL_CLEANUP_NO_TID uid=%s sym=%s side=%s key=%s",
                                    uid, sym_w, side_w, wkey,
                                )
                        except Exception as e:
                            log.warning(
                                "[OPPT] ORDER_FAIL_CLEANUP_FAILED uid=%s sym=%s side=%s err=%r",
                                uid, sym_w, side_w, e,
                            )

                        continue

                    # Old legacy case: pending without MT5 job. Re-arm same RC/zone.
                    stale_pending_no_job = (
                        not job_id
                        and entry_ts > 0
                        and pending_age_ms > 120000
                    )

                    if stale_pending_no_job:
                        watch["state"] = "ENTRY_READY"
                        watch["trade_state"] = "ENTRY_READY"
                        watch["entry_triggered"] = False
                        watch.pop("entry_price", None)
                        watch.pop("entry_ts_ms", None)
                        watch.pop("mt5_job_id", None)
                        watch.pop("device_id", None)

                        R.set(str(wkey), json.dumps(watch), ex=7 * 24 * 3600)
                        state_w = "ENTRY_READY"

                        log.warning(
                            "[WATCHLIST] SELF_HEAL_STALE_PENDING sym=%s side=%s key=%s",
                            sym_w, side_w, wkey
                        )
                if state_w not in (
                    "REV_OK",
                    "ENTRY_READY",
                    "ENTRY_BLOCKED_PROP",
                    "ENTRY_BLOCKED_MAX_OPEN",
                    "ENTRY_BLOCKED_SAME_SYMBOL",
                    "ENTRY_BLOCKED_MARGIN",
                    "ENTRY_BLOCKED_LOTS",
                    "ENTRY_BLOCKED_BROKER",
                ):
                    continue

                if bool(watch.get("entry_triggered")):
                    continue

                trig_hi = _sf(watch.get("rev_ok_bar_hi"), 0.0)
                trig_lo = _sf(watch.get("rev_ok_bar_lo"), 0.0)

                # live price from current device-independent price key fallback
                # live price from selected/online trading device, not random stale scan key
                live_px = 0.0
                try:
                    dev_for_px = _pick_device_for_symbol(uid, sym_w)
                    if dev_for_px:
                        pk = f"xtl:price:{dev_for_px}:{sym_w}"
                        pr = _sj(R.get(pk), {})
                        if isinstance(pr, dict):
                            px0 = _sf(pr.get("price"), 0.0)
                            ts0 = _si(pr.get("ts_ms"), 0)

                            # reject stale price older than 2 minutes
                            if px0 > 0 and ts0 > 0 and (now_ms() - ts0) <= 120000:
                                live_px = px0

                    # fallback: choose freshest price across all devices
                    if live_px <= 0:
                        best_ts = 0
                        best_px = 0.0
                        for pk in R.scan_iter(f"xtl:price:*:{sym_w}"):
                            pr = _sj(R.get(pk), {})
                            if not isinstance(pr, dict):
                                continue

                            px0 = _sf(pr.get("price"), 0.0)
                            ts0 = _si(pr.get("ts_ms"), 0)

                            if px0 > 0 and ts0 > best_ts and (now_ms() - ts0) <= 120000:
                                best_ts = ts0
                                best_px = px0

                        live_px = best_px
                except Exception:
                    live_px = 0.0
                if live_px <= 0:
                    continue
                rev_ok_ms = _si(watch.get("rev_ok_ms"), 0)

                # RC must exist.
                # rev_ok_ms is broker-candle time. Normalize it to server UTC before
                # comparing with now_ms(), otherwise UTC+ broker candles look "future".
                if rev_ok_ms <= 0:
                    log.warning(
                        "[WATCHLIST] SKIP_ENTRY_MISSING_RC sym=%s side=%s rev_ok_ms=%s now=%s key=%s",
                        sym_w, side_w, rev_ok_ms, now_ms(), wkey
                    )
                    continue

                broker_offset_min = 0
                try:
                    br = watch.get("broker") if isinstance(watch.get("broker"), dict) else {}
                    broker_offset_min = int(br.get("tz_offset_min") or watch.get("broker_tz_offset_min") or 0)
                except Exception:
                    broker_offset_min = 0

                # If watch does not carry broker TZ, read it from OHLC snapshot.
                if not broker_offset_min:
                    try:
                        for _k in R.scan_iter(f"xtl:ohlc:snap:*:{sym_w}:H1", count=20):
                            _js = _sj(R.get(_k), {}) or {}
                            _br = _js.get("broker") if isinstance(_js.get("broker"), dict) else {}
                            _off = int(_br.get("tz_offset_min") or 0)
                            if _off:
                                broker_offset_min = _off
                                break
                    except Exception:
                        broker_offset_min = 0

                rc_utc_ms = int(rev_ok_ms) - int(broker_offset_min) * 60 * 1000
                rc_delta_ms = int(rc_utc_ms - now_ms())

                if rc_delta_ms > 5000:
                    log.warning(
                        "[WATCHLIST] SKIP_ENTRY_FUTURE_RC sym=%s side=%s rev_ok_ms=%s rc_utc_ms=%s now=%s delta_ms=%s broker_offset_min=%s key=%s",
                        sym_w, side_w, rev_ok_ms, rc_utc_ms, now_ms(), rc_delta_ms, broker_offset_min, wkey
                    )
                    continue

                trigger_level = trig_hi if side_w == "BUY" else trig_lo
                if trigger_level <= 0:
                    continue

                blocked_retry = _is_entry_blocked_state(state_w)
                if blocked_retry:
                    try:
                        nr = _si(watch.get("next_retry_ms"), 0)
                        if nr > 0 and now_ms() < nr:
                            continue
                    except Exception:
                        pass

                    max_move = _sf(watch.get("late_entry_max_move"), 0.0) or _late_entry_max_move(sym_w)

                    # If price has pulled back before trigger, return to REV_OK and wait for fresh cross.
                    if side_w == "BUY":
                        beyond_trigger = bool(live_px >= trigger_level)
                        late_move = max(0.0, float(live_px) - float(trigger_level))
                    else:
                        beyond_trigger = bool(live_px <= trigger_level)
                        late_move = max(0.0, float(trigger_level) - float(live_px))

                    if not beyond_trigger:
                        watch["state"] = "REV_OK"
                        watch["trade_state"] = ""
                        watch["entry_blocked"] = False
                        watch.pop("entry_block_reason", None)
                        watch.pop("next_retry_ms", None)
                        R.set(str(wkey), json.dumps(watch, separators=(",", ":")), ex=7 * 24 * 3600)
                        continue

                    if late_move > float(max_move):
                        log.warning(
                            "[WATCHLIST] MISSED_PROP_DELAY sym=%s side=%s trigger=%s live=%s late_move=%s max_move=%s key=%s",
                            sym_w, side_w, trigger_level, live_px, late_move, max_move, wkey
                        )
                        try:
                            R.delete(str(wkey))
                            R.delete(f"xtl:watch:break_state:{sym_w}:{side_w}:H1")
                            for _ck in R.scan_iter(f"xtl:watch:entry_claim:{sym_w}:{side_w}:H1:*", count=50):
                                R.delete(_ck)
                        except Exception:
                            pass
                        continue

                break_key = f"xtl:watch:break_state:{sym_w}:{side_w}:H1"

                prev_px = 0.0
                prev_ts = 0

                try:
                    bs = _sj(R.get(break_key), {}) or {}
                    if isinstance(bs, dict):
                        prev_px = _sf(bs.get("last_price"), 0.0)
                        prev_ts = _si(bs.get("updated_ms"), 0)
                except Exception:
                    pass

                prev_fresh = prev_px > 0 and prev_ts > 0 and (now_ms() - prev_ts) <= 120000

                if side_w == "BUY":
                    crossed = bool(live_px > trigger_level)
                    
                else:
                    crossed = bool(live_px < trigger_level)
                already_beyond = False
                    
                # -------------------------------------------------
                # DEBUG: breakout decision
                # -------------------------------------------------
                log.warning(
                    "[WATCHLIST] BREAK_CHECK sym=%s side=%s trigger=%s live=%s prev=%s prev_fresh=%s crossed=%s already_beyond=%s key=%s",
                    sym_w,
                    side_w,
                    trigger_level,
                    live_px,
                    prev_px,
                    prev_fresh,
                    crossed,
                    already_beyond,
                    wkey,
                )

                try:
                    R.setex(
                        break_key,
                        24 * 3600,
                        json.dumps({
                            "symbol": sym_w,
                            "side": side_w,
                            "trigger_level": float(trigger_level),
                            "last_price": float(live_px),
                            "prev_price": float(prev_px),
                            "prev_fresh": bool(prev_fresh),
                            "crossed": bool(crossed),
                            "already_beyond": bool(already_beyond),
                            "updated_ms": now_ms(),
                        }),
                    )
                except Exception:
                    pass

                if already_beyond:
                    try:
                        watch["state"] = "MISSED_BREAKOUT"
                        watch["trade_state"] = "MISSED_BREAKOUT"
                        watch["missed_breakout"] = True
                        watch["missed_breakout_ms"] = now_ms()
                        watch["missed_breakout_reason"] = "NO_FRESH_CROSS_PRICE_ALREADY_BEYOND_TRIGGER"
                        watch["missed_breakout_trigger_level"] = float(trigger_level)
                        watch["missed_breakout_live_price"] = float(live_px)
                        watch["missed_breakout_prev_price"] = float(prev_px or 0.0)
                        watch["missed_breakout_prev_fresh"] = bool(prev_fresh)

                        if side_w == "BUY":
                            watch["missed_breakout_distance"] = float(live_px - trigger_level)
                        else:
                            watch["missed_breakout_distance"] = float(trigger_level - live_px)

                        watch["entry_triggered"] = False
                        watch["entry_blocked"] = True
                        watch["entry_block_reason"] = "MISSED_BREAKOUT"

                        R.set(str(wkey), json.dumps(watch), ex=7 * 24 * 3600)

                        log.warning(
                            "[WATCHLIST] MISSED_BREAKOUT sym=%s side=%s trigger=%s live=%s prev=%s prev_fresh=%s key=%s",
                            sym_w, side_w, trigger_level, live_px, prev_px, prev_fresh, wkey
                        )
                    except Exception as e:
                        log.warning("[WATCHLIST] MISSED_BREAKOUT_MARK_FAILED key=%s err=%r", wkey, e)

                    continue

                       

                if not crossed:
                    continue
                log.warning(
                    "[WATCHLIST] AFTER_CROSS sym=%s side=%s entry_triggered=%s state=%s",
                    sym_w,
                    side_w,
                    watch.get("entry_triggered"),
                    watch.get("state"),
                )
                # -------------------------------------------------
                # Prevent repeated ENTRY_CAND generation
                # -------------------------------------------------
                if bool(watch.get("entry_triggered")):
                    continue

                state_now = str(watch.get("state") or "").upper().strip()

                if state_now in ("ORDER_PENDING", "TRADE_ACTIVE"):
                    continue

                claim_key = f"xtl:watch:entry_claim:{sym_w}:{side_w}:H1:{int(watch.get('rev_ok_ms') or watch.get('started_ms') or 0)}"
                claimed = R.set(claim_key, str(now_ms()), nx=True, ex=120)
                log.warning(
                    "[WATCHLIST] CLAIM_RESULT sym=%s side=%s claimed=%s claim_key=%s",
                    sym_w,
                    side_w,
                    claimed,
                    claim_key,
                )

                if not claimed:
                    continue
              

                now_e = now_ms()
                trade_id = f"WATCH:{sym_w}:{side_w}:H1:{int(watch.get('rev_ok_ms') or watch.get('started_ms') or now_e)}"

                zone = watch.get("zone_used") if isinstance(watch.get("zone_used"), dict) else {}
                # -------------------------------------------------
                # PROP-FIRM REQUIRED STRUCTURE SL / TP
                # -------------------------------------------------
                z_low = _sf(zone.get("low") if isinstance(zone, dict) else 0.0, 0.0)
                z_high = _sf(zone.get("high") if isinstance(zone, dict) else 0.0, 0.0)
                entry_px = float(live_px)

                # small symbol-aware SL buffer
                if sym_w == "XAUUSD":
                    sl_buf = 0.50
                elif sym_w.endswith("JPY"):
                    sl_buf = 0.03
                else:
                    sl_buf = 0.00030

                sl_price = 0.0
                tp_price = 0.0

                if side_w == "BUY" and z_low > 0:
                    sl_price = z_low - sl_buf
                    risk_dist = entry_px - sl_price
                    if risk_dist > 0:
                        tp_price = entry_px + (2.0 * risk_dist)

                elif side_w == "SELL" and z_high > 0:
                    sl_price = z_high + sl_buf
                    risk_dist = sl_price - entry_px
                    if risk_dist > 0:
                        tp_price = entry_px - (2.0 * risk_dist)

                if sl_price <= 0 or tp_price <= 0:
                    log.warning(
                        "[WATCHLIST] SKIP_ENTRY no_structure_sl sym=%s side=%s entry=%s zone=%s key=%s",
                        sym_w, side_w, entry_px, zone, wkey
                    )
                    try:
                        R.delete(claim_key)
                    except Exception:
                        pass
                    continue
                log.warning(
                   "[WATCHLIST] BUILDING_ENTRY_EVENT sym=%s side=%s trade_id=%s",
                   sym_w,
                   side_w,
                   trade_id,
                )

                events.append({
                    "type": "ENTRY",
                    "uid": uid,
                    "trade_id": trade_id,
                    "symbol": sym_w,
                    "side": side_w,
                    "entry_price": float(entry_px),
                    "tp_price": float(tp_price),
                    "sl_price": float(sl_price),
                    "score": _sf(zone.get("sr_score") if isinstance(zone, dict) else 0.0, 0.0),
                    "confidence": "",
                    "entry_ts_ms": int(now_e),
                    "entry_zone": zone,
                    "entry_zone_low": zone.get("low") if isinstance(zone, dict) else None,
                    "entry_zone_high": zone.get("high") if isinstance(zone, dict) else None,
                    "entry_zone_level": zone.get("level") if isinstance(zone, dict) else None,
                    "entry_zone_tf": zone.get("tf") if isinstance(zone, dict) else "H1",
                    "entry_zone_kind": zone.get("kind") if isinstance(zone, dict) else "",
                    "entry_zone_source": zone.get("zone_source") if isinstance(zone, dict) else None,
                    "entry_zone_selection_model": zone.get("selection_model") if isinstance(zone, dict) else None,
                    "trigger_level": float(trigger_level),
                    "trigger_type": "WATCHLIST_REV_OK_BAR_BREAK",
                    "watch_key": str(wkey),
                    "claim_key": str(claim_key),
                    "source": "watchlist",
                })

               
                log.warning(
                    "[WATCHLIST] ENTRY_EVENT_ADDED sym=%s side=%s",
                    sym_w,
                    side_w,
                )
                log.warning(
                    "[WATCHLIST] ENTRY_CAND sym=%s side=%s px=%s sl=%s tp=%s trigger=%s key=%s",
                    sym_w, side_w, entry_px, sl_price, tp_price, trigger_level, wkey
                )

            except Exception as e:
                log.warning("[WATCHLIST] entry_scan_err key=%r err=%r", wkey, e)
                continue
    except Exception as e:
        log.warning("[WATCHLIST] scan_err err=%r", e)
   

    # --- DEBUG: events summary ---
    try:
        n_rows = len(rows) if rows is not None else 0
        n_ev = len(events)
        n_entry = sum(1 for e in events if e.get("type") == "ENTRY")
        n_exit = sum(1 for e in events if e.get("type") == "EXIT")
        log.warning("[OPPT] uid=%s rows=%s events=%s entry=%s exit=%s exec_mode=%s",
                uid, n_rows, n_ev, n_entry, n_exit, exec_mode)
        # show a sample of what the executor is seeing
        if n_ev > 0:
           e0 = events[0]
           log.warning("[OPPT] uid=%s sample_event keys=%s type=%r sym=%r score=%r conf=%r entry_price=%r",
                    uid, list(e0.keys())[:20], e0.get("type"), e0.get("symbol"),
                    e0.get("score"), e0.get("confidence"), e0.get("entry_price"))
    except Exception:
        pass


    # -------------------------------------------------
    # 1) EXITS (paper only for now)
    # -------------------------------------------------
    for ev in events:
        if ev.get("type") != "EXIT":
            continue
        tid = ev.get("trade_id")
        if not tid or tid not in open_by_id:
            continue

        pos = open_by_id[tid]
        exit_price = _sf(ev.get("exit_price"), 0.0)
        if exit_price <= 0:
            exit_price = _sf(pos.get("tp_price") or pos.get("sl_price"), 0.0)
        if exit_price <= 0:
            continue
        
        # mt5 exit (best-effort): send opposite market order to flatten
        # mt5 exit handling:
        # - If broker SL/TP already closed (HIT / SL_HIT), DO NOT send any exit order.
        #   Just close locally using computed exit_price.
        # - Only attempt a real MT5 close for EXPIRED/manual exits.
        mt5_exit_ok = True
        if exec_mode == "mt5":
           
                mt5_exit_ok = False
                try:
                    exit_reason = str(ev.get("exit_reason") or "").upper().strip()
                    symbol = str(pos.get("symbol") or "").upper().strip()
                    ticket = pos.get("mt5_ticket") or pos.get("ticket") or pos.get("position_ticket")
                    if ticket:
                        enq2 = _enqueue_mt5_close_position(
                            uid=uid,
                            symbol=symbol,
                            ticket=int(ticket),
                            qty=float(pos.get("qty", qty) or qty),
                            comment=f"oppt exit:{exit_reason}",
                            trade_id=str(pos.get("trade_id") or ""),
                            exit_reason=exit_reason,
                            mt5_account=mt5_account,
                        )
                    else:
                        enq2 = _enqueue_mt5_market_order(
                            user_id=uid,
                            sym=symbol,
                            side=("SELL" if str(pos.get("side") or "").upper() == "BUY" else "BUY"),
                            volume=float(pos.get("qty", qty) or qty),
                           
                            comment=f"oppt exit:{exit_reason}",
                            trade_id=str(pos.get("trade_id") or ""),
                            kind="EXIT",
                            exit_reason=exit_reason,
                            mt5_account=mt5_account,
                        )
                    mt5_exit_ok = bool(enq2.get("ok"))
                except Exception:
                    mt5_exit_ok = False

        # If MT5 exit failed for EXPIRED/manual exits, do NOT close locally
        if exec_mode == "mt5" and not mt5_exit_ok:
            continue


        _close_trade(
            uid,
            pos,
            exit_price,
            str(ev.get("exit_reason") or "EXPIRED"),
            meta={"symbol": ev.get("symbol")},
        )

    # refresh open after exits
    open_trades = _list_open_trades(uid)
    open_by_id = {t.get("trade_id"): t for t in open_trades if t.get("trade_id")}

    # -------------------------------------------------
    # 2) ENTRIES
    # -------------------------------------------------
    ex_key = EXECUTED_KEY.format(uid=uid)
    dbg_n = 0  # add once before the `for ev in events:` loop (entries section)
    for ev in events:
        if ev.get("type") != "ENTRY":
            continue
        if dbg_n < 3:
            log.warning("[OPPT] ENTRY_CAND uid=%s tid=%r sym=%r side=%r score=%r conf=%r entry=%r tp=%r sl=%r",
                        uid, ev.get("trade_id"), ev.get("symbol"), ev.get("side"),
                        ev.get("score"), ev.get("confidence"),
                       ev.get("entry_price"), ev.get("tp_price"), ev.get("sl_price"))
            dbg_n += 1

        # Global max_positions should not block other symbols.
        # We enforce one active trade per symbol below.
        # Keep max_positions only as optional safety when > 0 and explicitly wanted.
        if False and len(open_trades) >= max_positions:
            break


        tid = str(ev.get("trade_id") or "").strip()
        sym = str(ev.get("symbol") or "").upper().strip()
        side = str(ev.get("side") or "").upper().strip()
        if not tid or not sym or side not in ("BUY", "SELL"):
            continue

        if tid in open_by_id:
            continue
        _same_sym_open = [
            t for t in open_trades
            if str(t.get("symbol") or "").upper().strip() == sym
            and str(t.get("execution_mode") or "paper").lower() == exec_mode
            and str(t.get("status") or "").lower() in ("sent", "pending", "filled")
        ]

        _broker_same_sym = []
        if exec_mode == "mt5":
            try:
                for pk in R.scan_iter(f"xtl:mt5:pos:*:{mt5_account}"):
                    rawp = R.get(pk)
                    arr = _sj(rawp, []) if rawp else []
                    if not isinstance(arr, list):
                        continue
                    for bp in arr:
                        if not isinstance(bp, dict):
                            continue
                        if str(bp.get("symbol") or "").upper().strip() != sym:
                            continue

                        magic = int(bp.get("magic") or 0)
                        comment = str(bp.get("comment") or "")
                        if magic == 20251227 or comment.upper().startswith("XTL"):
                            _broker_same_sym.append(bp)
            except Exception:
                pass

        if _same_sym_open or _broker_same_sym:
            log.warning(
                "[OPPT] SKIP_ENTRY same_symbol_active uid=%s sym=%s side=%s tid=%s redis_open=%s broker_open=%s",
                uid, sym, side, tid, len(_same_sym_open), len(_broker_same_sym)
            )
            _clear_watchlist_entry_block(ev, "SAME_SYMBOL_ACTIVE")
            continue
        try:
            score = float(ev.get("score") or 0.0)
        except Exception:
            score = 0.0

        conf = str(ev.get("confidence") or "").lower().strip()
        
        if score < min_score:
            log.warning("[OPPT] SKIP_ENTRY score_lt_min uid=%s sym=%s tid=%s score=%s min_score=%s", uid, sym, tid, score, min_score)
            continue
        # TEMP VALIDATION:
        # Disable confidence filter because zone-reversal ENTRY_CAND currently has conf=''.
        # We only want to validate REV_OK -> ENTRY_CAND -> MT5_ENQUEUE -> MT5 order.
        if False and min_conf_r > 0 and _conf_rank(conf) < min_conf_r:
            log.warning("[OPPT] SKIP_ENTRY conf_lt_min uid=%s sym=%s tid=%s conf=%r min_conf_r=%s", uid, sym, tid, conf, min_conf_r)
            continue

        cd_key = COOLDOWN_KEY.format(uid=uid, symbol=sym)
        try:
            if R.exists(cd_key):
                log.warning("[OPPT] SKIP_ENTRY cooldown uid=%s sym=%s tid=%s cd_key=%s", uid, sym, tid, cd_key)
                continue
        except Exception:
            pass

        try:
            if R.sismember(ex_key, tid):
                log.warning("[OPPT] SKIP_ENTRY already_executed uid=%s sym=%s tid=%s", uid, sym, tid)
                continue
        except Exception:
            pass

        entry_price = _sf(ev.get("entry_price"), 0.0)
        if entry_price <= 0:
            log.warning("[OPPT] SKIP_ENTRY bad_entry_price uid=%s sym=%s tid=%s entry_price=%s", uid, sym, tid, entry_price)
            continue

        tp_price = _sf(ev.get("tp_price"), 0.0)
        sl_price = _sf(ev.get("sl_price"), 0.0)
        log.warning("[OPPT] QTY_DECISION uid=%s sym=%s has_overrides=%s strict=%s qty_use_pre=%r qty_default=%r",
                    uid, sym, has_overrides, strict_overrides, qty_by_symbol.get(sym), qty)

        qty_use = float(_qty_for_symbol(sym))
        if qty_use <= 0:
            # overrides are enabled but symbol missing/invalid -> skip entry
            log.warning("[OPPT] SKIP_ENTRY qty_use<=0 uid=%s sym=%s tid=%s has_overrides=%s strict=%s",
                        uid, sym, tid, has_overrides, strict_overrides)
            continue

        # -------------------------------------------------
        # MT5 EXECUTION PATH
        # -------------------------------------------------
        # -------------------------------------------------
        

        
        # -------------------------------------------------
        # -------------------------------------------------
        # SAFETY VALIDATION
        # OPPT events use OPPT hash.
        # Watchlist events use watch key only.
        # -------------------------------------------------
        is_watchlist_event = str(ev.get("source") or "").lower() == "watchlist"
        alert_id = tid

        if not is_watchlist_event:
            try:
                parts = str(tid or "").split(":")
                alert_id = ":".join(parts[:-1]).strip() if len(parts) > 1 else str(tid or "").strip()
                hkey = f"{ALERT_HASH_PREFIX}{alert_id}"
                h = R.hgetall(hkey) or {}

                if not h:
                    log.warning("[OPPT] SKIP_ENTRY missing_opp_hash uid=%s sym=%s side=%s tid=%s alert_id=%s",
                                uid, sym, side, tid, alert_id)
                    continue

                status = str(_sj(h.get("status"), h.get("status")) or "").lower().strip()
                trade_state = str(_sj(h.get("trade_state"), h.get("trade_state")) or "").upper().strip()

                eg = _sj(h.get("entry_gate"), {}) if h.get("entry_gate") else {}
                reason = str((eg or {}).get("reason") or "").upper()

                if status != "active":
                    log.warning("[OPPT] SKIP_ENTRY stale_opp_status uid=%s sym=%s side=%s tid=%s status=%r",
                                uid, sym, side, tid, status)
                    continue

                if trade_state in ("ZONE_INVALIDATED", "INVALIDATED", "EXPIRED", "CLOSED"):
                    log.warning("[OPPT] SKIP_ENTRY stale_opp_state uid=%s sym=%s side=%s tid=%s trade_state=%r",
                                uid, sym, side, tid, trade_state)
                    continue

                if "ZONE_INVALIDATED" in reason or "INVALIDATED" in reason:
                    log.warning("[OPPT] SKIP_ENTRY invalidated_gate uid=%s sym=%s side=%s tid=%s reason=%r",
                                uid, sym, side, tid, reason)
                    continue

            except Exception as e:
                log.warning("[OPPT] SKIP_ENTRY opp_validation_exc uid=%s sym=%s tid=%s err=%r",
                            uid, sym, tid, e)
                continue

        else:
             watch_key = ev.get("watch_key") or f"xtl:zone:watch:{sym}:{side}:H1"
             watch = {}

             try:
                 raw_watch = R.get(str(watch_key))
                 if raw_watch:
                     watch = _sj(raw_watch, {})
             except Exception:
                 watch = {}

             if not isinstance(watch, dict) or not watch:
                 log.warning("[WATCHLIST] SKIP_ENTRY missing_watch uid=%s sym=%s side=%s tid=%s watch_key=%s",
                             uid, sym, side, tid, watch_key)
                 continue

             watch_state = str(watch.get("state") or "").upper().strip()

             if watch_state not in ("REV_OK", "ENTRY_READY"):
                 log.warning("[WATCHLIST] SKIP_ENTRY watch_not_ready uid=%s sym=%s side=%s tid=%s watch_state=%r",
                             uid, sym, side, tid, watch_state)
                 continue

        # common validation for both OPPT and watchlist
        has_valid_entry_snapshot = bool(
            entry_price > 0
            and ev.get("entry_ts_ms")
            and (
                ev.get("entry_zone")
                or ev.get("entry_zone_level")
                or ev.get("trigger_level")
            )
        )

        if not has_valid_entry_snapshot:
            log.warning("[OPPT] SKIP_ENTRY bad_entry_snapshot uid=%s sym=%s side=%s tid=%s",
                        uid, sym, side, tid)
            continue

        claim_key = ENTRY_CLAIM_KEY.format(alert_id=alert_id)
        claimed = R.set(claim_key, str(now_ms()), nx=True, ex=24 * 3600)

        if not claimed:
            log.warning("[OPPT] SKIP_ENTRY duplicate_entry_claim uid=%s sym=%s side=%s tid=%s alert_id=%s",
                        uid, sym, side, tid, alert_id)
            continue
        # -------------------------------------------------
        # PROP FIRM COMPLIANCE CHECK
        # Runs after ENTRY_CLAIM so only one executor reserves risk.
        # -------------------------------------------------
        prop_check = None
        prop_cfg = {}
        try:
            prop_cfg = _get_prop_config()
        except Exception:
            prop_cfg = {"enabled": False}

        if bool(prop_cfg.get("enabled")):
            try:
                risk_state = _get_prop_risk_state()

                prop_check = compute_prop_check(
                    firm=str(prop_cfg.get("firm") or "ftmo"),
                    phase=str(prop_cfg.get("phase") or "challenge"),
                    account_size=float(prop_cfg.get("account_size") or 25000),
                    symbol=sym,
                    side=side,
                    entry=float(entry_price),
                    sl=float(sl_price),
                    risk_pct=float(prop_cfg.get("risk_pct") or 1.0),
                    target_rr=float(prop_cfg.get("target_rr") or 2.0),
                    daily_loss_used=float(risk_state.get("daily_loss_used") or 0),
                    max_loss_used=float(risk_state.get("max_loss_used") or 0),
                    open_risk_usd=float(risk_state.get("open_risk_usd") or 0),
                    open_positions_count=len(risk_state.get("open_positions") or []),
                    max_open_risk_pct=float(prop_cfg.get("max_open_risk_pct") or 3.0),
                    max_open_positions=int(prop_cfg.get("max_open_positions") or 1),
                )

                if not isinstance(prop_check, dict) or prop_check.get("verdict") != "OK":
                    log.warning(
                        "[PROP] BLOCK_ENTRY uid=%s tid=%s sym=%s side=%s verdict=%s reasons=%s",
                        uid, tid, sym, side,
                        prop_check.get("verdict") if isinstance(prop_check, dict) else None,
                        prop_check.get("reasons") if isinstance(prop_check, dict) else None,
                    )
                    try:
                        R.delete(ENTRY_CLAIM_KEY.format(alert_id=alert_id))
                    except Exception:
                        pass
                    _clear_watchlist_entry_block(ev, "PROP_CAPACITY_BLOCK")
                    continue

                # Override executor sizing with prop-calculated values.
                if bool(prop_cfg.get("enabled")):
                    lots0 = float(prop_check.get("lots") or 0)
                    verdict0 = str(prop_check.get("verdict") or "").upper()

                    if verdict0 != "OK" or lots0 <= 0:
                        log.warning(
                            "[PROP] BLOCK_ENTRY_LOTS_MISSING sym=%s side=%s verdict=%s prop_check=%s",
                            sym,
                            side,
                            verdict0,
                            prop_check,
                        )
                        _clear_watchlist_entry_block(ev, "PROP_LOTS_MISSING")
                        continue

                    qty_use = lots0
                tp_price = float(prop_check.get("tp") or tp_price)
                sl_price = float(prop_check.get("sl") or sl_price)

                

                log.warning(
                    "[PROP] OK_ENTRY uid=%s tid=%s sym=%s side=%s lots=%s risk_usd=%s tp=%s sl=%s",
                    uid, tid, sym, side,
                    prop_check.get("lots"),
                    prop_check.get("risk_usd"),
                    prop_check.get("tp"),
                    prop_check.get("sl"),
                )
                try:
                    msg = (
                        f"**{sym} {side}**\n"
                        f"Entry: `{entry_price}` | SL: `{sl_price}` | TP: `{tp_price}`\n"
                        f"Lots: `{qty_use}`\n\n"
                        f"**PROP [{prop_check.get('firm')} - {prop_check.get('phase')}]**\n"
                        f"Risk: `${prop_check.get('risk_usd')}` "
                        f"({prop_check.get('risk_pct')}%)\n"
                        f"Daily room: `${prop_check.get('daily_room_usd')}` / "
                        f"`${prop_check.get('daily_limit_usd')}`\n"
                        f"Max room: `${prop_check.get('max_loss_room_usd')}` / "
                        f"`${prop_check.get('max_loss_limit_usd')}`\n"
                        f"Status: **OK TO PLACE MANUALLY**\n"
                        f"Trade ID: `{tid}`"
                    )
                    _discord_trade_post(msg)
                except Exception:
                    pass

            except Exception as e:
                log.warning(
                    "[PROP] SKIP_ENTRY prop_check_exc uid=%s tid=%s sym=%s side=%s err=%r",
                    uid, tid, sym, side, e,
                )
                try:
                    R.delete(ENTRY_CLAIM_KEY.format(alert_id=alert_id))
                except Exception:
                    pass
                continue         
        entry_zone_obj = ev.get("entry_zone") if isinstance(ev.get("entry_zone"), dict) else {}
        zone_src_for_comment = (
            ev.get("entry_zone_source")
            or ev.get("zone_source")
            or ev.get("entry_zone_selection_model")
            or ev.get("zone_selection_model")
            or entry_zone_obj.get("zone_source")
            or entry_zone_obj.get("source")
            or entry_zone_obj.get("selection_model")
            or entry_zone_obj.get("entry_zone_source")
        )
        zone_src_code = _zone_src_code(zone_src_for_comment)

        log.warning("[OPPT] MT5_ENQUEUE uid=%s tid=%s sym=%s side=%s qty=%s acct=%s zone_src=%r zone_code=%s",
            uid, tid, sym, side, qty_use, mt5_account, zone_src_for_comment, zone_src_code)

        if exec_mode == "mt5":
            enq = _enqueue_mt5_market_order(
                user_id=uid,
                sym=sym,
                side=side,
                volume=qty_use,
                trade_id=tid,
                sl=float(sl_price) if sl_price > 0 else None,
                tp=float(tp_price) if tp_price > 0 else None,
                comment=f"XTL {side} {sym} {zone_src_code}",
                mt5_account=mt5_account,
            )
            log.warning("[OPPT] MT5_ENQUEUE_RES uid=%s ok=%s job_id=%r device_id=%r err=%r",
            uid, bool(enq.get("ok")), enq.get("job_id"), enq.get("device_id"), enq.get("error"))

            # record last enqueue result (visible from /strategy/oppt/state)
            try:
                st["last_enqueue"] = {
                    "ts_ms": int(time.time() * 1000),
                    "symbol": sym,
                    "side": side,
                    "qty": qty_use,
                    "ok": bool(enq.get("ok")),
                    "error": enq.get("error"),
                    "job_id": enq.get("job_id"),
                }
                _save_state(uid, st)
            except Exception:
                pass

            if not enq.get("ok"):
                try:
                    R.delete(ENTRY_CLAIM_KEY.format(alert_id=alert_id))
                except Exception:
                    pass

                # release watchlist claim so it can retry next cycle
                try:
                    if is_watchlist_event:
                        for k in R.scan_iter(f"xtl:watch:entry_claim:{sym}:{side}:H1:*"):
                            R.delete(k)
                except Exception:
                    pass

                continue

            pos = {
                "trade_id": tid,
                "symbol": sym,
                "side": side,
                "entry_price": float(entry_price),
                "qty": float(qty_use),
                "tp_price": float(tp_price) if tp_price > 0 else None,
                "sl_price": float(sl_price) if sl_price > 0 else None,
                "opened_at_ms": now_ms(),
                "source": ev.get("source") or "oppt",
                "execution_mode": "mt5",
                "mt5_job_id": enq.get("job_id"),
                "device_id": enq.get("device_id"),
                "status": "sent",
                "trade_state": "ORDER_PENDING" if exec_mode == "mt5" else "TRADE_ACTIVE",
                "entry_zone": ev.get("entry_zone"),
                "entry_zone_low": ev.get("entry_zone_low"),
                "entry_zone_high": ev.get("entry_zone_high"),
                "entry_zone_level": ev.get("entry_zone_level"),
                "entry_zone_tf": ev.get("entry_zone_tf"),
                "entry_zone_kind": ev.get("entry_zone_kind"),
                "entry_gate_reason": ev.get("entry_gate_reason"),
                "trigger_type": ev.get("trigger_type"),
                "trigger_level": ev.get("trigger_level"),
                "prop_check": prop_check,
                "prop_firm": prop_check.get("firm") if isinstance(prop_check, dict) else None,
                "prop_phase": prop_check.get("phase") if isinstance(prop_check, dict) else None,
                "prop_risk_usd": prop_check.get("risk_usd") if isinstance(prop_check, dict) else None,
                "prop_risk_pct": prop_check.get("risk_pct") if isinstance(prop_check, dict) else None,
            }

            _open_trade(uid, pos)
            # -------------------------------------------------
            # PROP RISK RESERVE AFTER SUCCESSFUL MT5 ENQUEUE
            # Reserve only after MT5 enqueue succeeded and open trade is stored.
            # -------------------------------------------------
            try:
                if prop_check and bool(prop_cfg.get("enabled")):
                    _reserve_prop_open_risk(
                        tid,
                        {
                            "trade_id": tid,
                            "symbol": sym,
                            "side": side,
                            "risk_usd": float(prop_check.get("risk_usd") or 0),
                            "risk_pct": float(prop_check.get("risk_pct") or 0),
                            "lots": float(prop_check.get("lots") or qty_use),
                            "entry": float(entry_price),
                            "sl": float(sl_price),
                            "tp": float(tp_price),
                            "firm": prop_check.get("firm"),
                            "phase": prop_check.get("phase"),
                            "source": "oppt_executor_mt5_enqueued",
                            "mt5_job_id": enq.get("job_id"),
                            "device_id": enq.get("device_id"),
                            "reserved_ts_ms": int(time.time() * 1000),
                        },
                    )
                    log.warning(
                        "[PROP] RISK_RESERVED uid=%s tid=%s sym=%s risk_usd=%s lots=%s",
                        uid, tid, sym,
                        prop_check.get("risk_usd"),
                        prop_check.get("lots"),
                    )
            except Exception as e:
                log.warning(
                    "[PROP] RISK_RESERVE_FAILED uid=%s tid=%s sym=%s err=%r",
                    uid, tid, sym, e
                )
            if is_watchlist_event:
                try:
                    watch_key = ev.get("watch_key") or f"xtl:zone:watch:{sym}:{side}:H1"
                    raw_w = R.get(str(watch_key))
                    w = _sj(raw_w, {}) if raw_w else {}
                    if isinstance(w, dict):
                        w["state"] = "ORDER_PENDING"
                        w["entry_triggered"] = True
                        w["entry_price"] = float(entry_price)
                        w["entry_ts_ms"] = now_ms()
                        w["entry_signal"] = side
                        w["entry_trigger_level"] = ev.get("trigger_level")
                        w["trade_state"] = "ORDER_PENDING"
                        w["mt5_job_id"] = enq.get("job_id")
                        w["device_id"] = enq.get("device_id")
                        w["trade_id"] = tid  # persist exact reserved field for release-on-fail
                        R.set(str(watch_key), json.dumps(w))
                except Exception:
                    pass
            # Release the frozen zone watch on entry — same as paper path
            _clear_zone_watch_on_entry(sym, side, "H1")

            open_trades = _list_open_trades(uid)
            open_by_id = {t.get("trade_id"): t for t in open_trades if t.get("trade_id")}

        # -------------------------------------------------
        # PAPER EXECUTION PATH
        # -------------------------------------------------
        else:
            pos = {
                "trade_id": tid,
                "symbol": sym,
                "side": side,
                "entry_price": float(entry_price),
                "qty": float(qty_use),
                "tp_price": float(tp_price) if tp_price > 0 else None,
                "sl_price": float(sl_price) if sl_price > 0 else None,
                "opened_at_ms": now_ms(),
                "source": "oppt",
                "trade_state": "ORDER_PENDING" if exec_mode == "mt5" else "TRADE_ACTIVE",
                "entry_zone": ev.get("entry_zone"),
                "entry_zone_low": ev.get("entry_zone_low"),
                "entry_zone_high": ev.get("entry_zone_high"),
                "entry_zone_level": ev.get("entry_zone_level"),
                "entry_zone_tf": ev.get("entry_zone_tf"),
                "entry_zone_kind": ev.get("entry_zone_kind"),
                "entry_gate_reason": ev.get("entry_gate_reason"),
                "trigger_type": ev.get("trigger_type"),
                "trigger_level": ev.get("trigger_level"),
            }

            _open_trade(uid, pos)
            _clear_zone_watch_on_entry(sym, side, "H1")
            open_trades = _list_open_trades(uid)
            open_by_id = {t.get("trade_id"): t for t in open_trades if t.get("trade_id")}

        
        # mark executed:
        # - paper: immediately
        # - mt5: only after ack ok (handled in reconciliation)
        if exec_mode != "mt5":
            try:
                R.sadd(ex_key, tid)
                R.expire(ex_key, 7 * 24 * 3600)
            except Exception:
                pass


        if cooldown_min > 0:
            try:
                R.setex(cd_key, cooldown_min * 60, "1")
            except Exception:
                pass

        


# -----------------------------------------------------------------------------
# Enabled users scanning + manager entry
# -----------------------------------------------------------------------------




def tick_all_enabled_users(max_users: int = 500) -> dict:
    uids = _get_enabled_user_ids(limit=max_users)
    if not uids:
        return {"enabled": 0, "ticked": 0}

    ticked = 0
    for uid in uids:
        lk = LOCK_KEY.format(uid=uid)
        got = False
        try:
            got = bool(R.set(lk, "1", nx=True, ex=max(5, EXECUTOR_SLEEP_SEC * 3)))
        except Exception:
            got = False

        if not got:
            continue

        try:
            tick_user(uid)
            ticked += 1
        except Exception:
            log.exception("[OPPT] tick_user failed uid=%s", uid)
        finally:
            try:
                R.delete(lk)
            except Exception:
                pass

    return {"enabled": len(uids), "ticked": ticked}
