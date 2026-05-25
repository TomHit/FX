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
from typing import Any, Dict, List, Optional
from redis.exceptions import AuthenticationError, ConnectionError, TimeoutError

import redis
import uuid

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

def _clear_zone_watch_on_entry(sym: str, side: str, tf: str = "H1") -> None:
    try:
        wk = _zone_watch_key(sym, side, tf)
        R.delete(wk)
    except Exception:
        pass


def _pick_device_for_symbol(user_id: str, sym: str) -> str | None:
    sym_u = (sym or "").upper().strip()
    if not sym_u:
        return None

    # 1) Prefer sticky device from OHLC writer (per user+sym+tf)
    try:
        dev = R.get(_sticky_dev_key(user_id, sym_u, "M1"))
        if isinstance(dev, (bytes, bytearray)):
            dev = dev.decode("utf-8", "ignore")
        dev = (dev or "").strip()
        if dev:
            return dev
    except Exception:
        pass

    # 2) Prefer most-recently-seen device from user's set (heartbeat-based)
    try:
        devs = R.smembers(f"xtl:user:{user_id}:devices") or set()
        best_dev = None
        best_seen = -1

        for x in devs:
            d = x.decode() if isinstance(x, (bytes, bytearray)) else str(x)
            d = (d or "").strip()
            if not d:
                continue

            seen = -1
            # Try a few common "last seen" key patterns (safe no-op if missing)
            for k in (
                f"xtl:device:{d}:last_seen_ms",
                f"xtl:devices:{d}:last_seen_ms",
                f"xtl:dev:{d}:last_seen_ms",
                f"xtl:device:last_seen_ms:{d}",
            ):
                try:
                    v = R.get(k)
                    if isinstance(v, (bytes, bytearray)):
                        v = v.decode("utf-8", "ignore")
                    if v is not None and str(v).strip():
                        seen = int(float(str(v).strip()))
                        break
                except Exception:
                    continue

            if seen > best_seen:
                best_seen = seen
                best_dev = d

        if best_dev:
            return best_dev
    except Exception:
        pass

    # 3) Fallback: any device in user's set (arbitrary)
    try:
        devs = R.smembers(f"xtl:user:{user_id}:devices") or set()
        for x in devs:
            d = x.decode() if isinstance(x, (bytes, bytearray)) else str(x)
            d = (d or "").strip()
            if d:
                return d
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
        side = str(row.get("entry_signal") or row.get("decision") or "").upper().strip()
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

        
        # 2) ENTRY_READY / REV_OK = immediate execution
        eg = row.get("entry_gate") if isinstance(row.get("entry_gate"), dict) else {}
        reason = str(eg.get("reason") or "").upper().strip()
        stage = str(eg.get("stage") or "").upper().strip()
        trade_state = str(row.get("trade_state") or "").upper().strip()

        if (
            (reason in ("REV_OK", "ENTRY_READY") or stage == "REV" or trade_state == "ENTRY_READY")
            and side in ("BUY", "SELL")
            and last_price > 0
        ):
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
                        "entry_reason": json.dumps("ENTRY_READY_IMMEDIATE"),
                        "entry_trigger_type": json.dumps("ENTRY_READY_IMMEDIATE"),
                        "trade_state": json.dumps("ENTRY_READY"),
                    },
                )
                R.expire(hkey, 7 * 24 * 3600)
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
                "trigger_type": "ENTRY_READY_IMMEDIATE",
                "live_px": float(last_price),
                **entry_zone_meta,
            }
            trig_hi = _sf(rs.get("rev_ok_bar_hi"), 0.0)
            trig_lo = _sf(rs.get("rev_ok_bar_lo"), 0.0)

            crossed = False
            trig_level = 0.0
            if side == "BUY" and trig_hi > 0 and last_price >= trig_hi:
                crossed = True
                trig_level = trig_hi
            elif side == "SELL" and trig_lo > 0 and last_price <= trig_lo:
                crossed = True
                trig_level = trig_lo

            if crossed:
                now_e = now_ms()

                # Freeze entry into the alert hash so future polls show entry_triggered=true
                try:
                    hkey = f"{ALERT_HASH_PREFIX}{alert_id}"
                    R.hset(
                        hkey,
                        mapping={
                            "entry_triggered": json.dumps(True),
                            "entry_signal": json.dumps(side),
                            "entry_price": json.dumps(float(last_price)),  # LIVE price
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
                        },
                    )
                    R.expire(hkey, 7 * 24 * 3600)
                except Exception:
                    pass

                # IMPORTANT: delete watch key after entry (so next zone can be used later)
                try:
                    wkey = eg.get("watch_key") or rs.get("watch_key")
                    if wkey:
                        R.delete(str(wkey))
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


def _open_trade(uid: str, pos: Dict[str, Any]) -> None:
    R.hset(OPEN_KEY.format(uid=uid), pos["trade_id"], json.dumps(pos))


def _remove_open_trade(uid: str, trade_id: str) -> None:
    try:
        R.hdel(OPEN_KEY.format(uid=uid), trade_id)
    except Exception:
        pass
def _save_state(uid: str, st: dict) -> None:
    key = STATE_KEY.format(uid=uid)
    st["updated_at_ms"] = now_ms()
    R.set(key, json.dumps(st))


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
    if meta and isinstance(meta, dict):
        closed["exit_meta"] = meta

    R.lpush(CLOSED_KEY.format(uid=uid), json.dumps(closed))
    # keep last 500
    try:
        R.ltrim(CLOSED_KEY.format(uid=uid), 0, 499)
    except Exception:
        pass

    _remove_open_trade(uid, str(pos.get("trade_id") or ""))


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
                try:
                    _clear_zone_watch_on_entry(pos.get("symbol"), pos.get("side"), "H1")
                except Exception:
                    pass


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
                if str(pos.get("status") or "").lower() != "filled":
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

                key = f"xtl:mt5:pos:{dev_id}:{mt5_account}"
                raw = R.get(key)
                open_tickets = set()
                if raw:
                    for p in _sj(raw, []):
                        if isinstance(p, dict) and p.get("ticket") is not None:
                            try:
                                open_tickets.add(int(p["ticket"]))
                            except Exception:
                                pass

                if open_tickets and ticket not in open_tickets:
                    try:
                        lp = _sf(pos.get("last_price"), 0.0) or _sf(pos.get("entry_price"), 0.0)
                        _close_trade(uid, pos, float(lp), "BROKER_CLOSED")
                    finally:
                        _remove_open_trade(uid, str(pos.get("trade_id") or ""))
    except Exception:
        pass

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
    rows = _load_recent_alert_rows(limit=250)
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
    import logging
    log = logging.getLogger("uvicorn.error")

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

        if len(open_trades) >= max_positions:
            break


        tid = str(ev.get("trade_id") or "").strip()
        sym = str(ev.get("symbol") or "").upper().strip()
        side = str(ev.get("side") or "").upper().strip()
        if not tid or not sym or side not in ("BUY", "SELL"):
            continue

        if tid in open_by_id:
            continue
        if any(
            t.get("symbol") == sym
            and str(t.get("execution_mode") or "paper").lower() == exec_mode
            and str(t.get("status") or "").lower() in ("sent", "pending", "filled")
            for t in open_trades
        ):
            continue


        score = _sf(ev.get("score"), 0.0)
        conf = str(ev.get("confidence") or "").lower().strip()
        if score < min_score:
            continue
        if min_conf_r > 0 and _conf_rank(conf) < min_conf_r:
            continue

        cd_key = COOLDOWN_KEY.format(uid=uid, symbol=sym)
        try:
            if R.exists(cd_key):
                continue
        except Exception:
            pass

        try:
            if R.sismember(ex_key, tid):
                continue
        except Exception:
            pass

        entry_price = _sf(ev.get("entry_price"), 0.0)
        if entry_price <= 0:
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

        import logging
        log = logging.getLogger("uvicorn.error")
        log.warning("[OPPT] MT5_ENQUEUE uid=%s tid=%s sym=%s side=%s qty=%s acct=%s",
            uid, tid, sym, side, qty_use, mt5_account)

        if exec_mode == "mt5":
            enq = _enqueue_mt5_market_order(
                user_id=uid,
                sym=sym,
                side=side,
                volume=qty_use,
                trade_id=tid,
                sl=float(sl_price) if sl_price > 0 else None,
                tp=float(tp_price) if tp_price > 0 else None,
                comment=f"XTL {side} {sym}",
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
                "source": "oppt",
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
            }

            _open_trade(uid, pos)
            

            

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
import logging
log = logging.getLogger("uvicorn.error")




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
