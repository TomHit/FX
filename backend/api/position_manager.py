# api/strategy/position_manager.py
"""XTL live position-management engine.

Dedicated responsibility: manage already-open broker positions.

Initial production rule (2026-08-08):
- XAUUSD: move SL to broker entry price after +0.50R
- USDJPY: move SL to broker entry price after +0.75R
- USDCAD: move SL to broker entry price after +0.75R

No entry, sizing, TP, zone, RC, DXY, classifier, or exit-close decisions live here.
The original ``sl_price`` remains immutable and is always the R denominator.
"""
from __future__ import annotations

import json
import logging
import threading
import time
import uuid
from typing import Any

from api.trend_endpoints import _resolve_prop_profile_device

log = logging.getLogger("uvicorn.error")

OPEN_KEY_TEMPLATE = "xtl:strategy:oppt:open:{uid}"
OPEN_UIDS_KEY = "xtl:strategy:oppt:open_uids"
CMDQ_TEMPLATE = "xtl:mt5:cmdq:{device_id}"
ACK_TEMPLATE = "xtl:mt5:ack:{job_id}"

# Frozen first production thresholds. Symbols not listed here are untouched.
BE_TRIGGER_R: dict[str, float] = {
    "XAUUSD": 0.50,
    "USDJPY": 0.50,
    "USDCAD": 0.50,
    "USDCHF": 0.50,
    "EURUSD": 0.50,
    "GBPUSD": 0.50,


    
}
BE_BUFFER_R: dict[str, float] = {
    "XAUUSD": 0.05,
    "USDJPY": 0.05,
    "USDCAD": 0.05,
    "USDCHF": 0.05,
    "EURUSD": 0.05,
    "GBPUSD": 0.05,

}


POLL_SEC = 1.0
DEVICE_HEARTBEAT_MAX_AGE_MS = 180_000
PENDING_ACK_WAIT_MS = 60_000
PER_TICKET_LOCK_MS = 4_000
SWEEP_LOCK_MS = 900

_started = False
_thread: threading.Thread | None = None


def _now_ms() -> int:
    return int(time.time() * 1000)


def _decode(v: Any) -> str:
    if isinstance(v, (bytes, bytearray)):
        return v.decode("utf-8", "ignore")
    return str(v or "")


def _json(v: Any, default: Any):
    try:
        if isinstance(v, (bytes, bytearray)):
            v = v.decode("utf-8", "ignore")
        if isinstance(v, str):
            return json.loads(v)
        return v if v is not None else default
    except Exception:
        return default


def _f(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return float(default)


def _i(v: Any, default: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return int(default)


def _price_step(bp: dict) -> float:
    for field in ("trade_tick_size", "tick_size", "point"):
        x = _f(bp.get(field), 0.0)
        if x > 0:
            return x
    digits = _i(bp.get("digits"), -1)
    if 0 <= digits <= 10:
        return 10.0 ** (-digits)
    return 0.0


def _normalize_price(price: float, bp: dict) -> float:
    """
    Normalize a protection price to the broker-native executable price grid.

    Priority:
      1. trade_tick_size
      2. tick_size
      3. point
      4. digits fallback

    digits alone describe decimal display precision; they do not always
    describe the broker's valid trading increment.
    """
    px = float(price)

    step = _price_step(bp)

    if step > 0:
        px = round(px / step) * step

    digits = _i(bp.get("digits"), -1)

    if 0 <= digits <= 10:
        return round(px, digits)

    # Avoid binary-float residue when broker digits are unavailable.
    return round(px, 10)



def _device_fresh(R, device_id: str, now_ms: int) -> bool:
    """Match executor routing safety: online + MT5 OK + trade allowed + fresh heartbeat."""
    try:
        h = R.hgetall(f"device:{device_id}") or {}
        if not h:
            return False

        def _hv(name: str):
            return h.get(name) or h.get(name.encode())

        status = _decode(_hv("status")).strip().lower()
        mt5_ok = _decode(_hv("mt5_ok")).strip().lower()
        trade_allowed = _decode(_hv("mt5_terminal_trade_allowed")).strip().lower()
        hb = _i(_hv("last_heartbeat_ms"), 0)
        fresh = hb > 0 and 0 <= (now_ms - hb) <= DEVICE_HEARTBEAT_MAX_AGE_MS
        return bool(
            status == "online"
            and fresh
            and mt5_ok in ("1", "true", "yes")
            and trade_allowed in ("1", "true", "yes")
        )
    except Exception:
        return False


def _load_broker_position(R, device_id: str, mt5_account: str, ticket: int) -> dict | None:
    key = f"xtl:mt5:pos:{device_id}:{mt5_account}"
    try:
        rows = _json(R.get(key), [])
    except Exception:
        return None
    if not isinstance(rows, list):
        return None
    for row in rows:
        if not isinstance(row, dict):
            continue
        if _i(row.get("ticket"), 0) == int(ticket):
            out = dict(row)
            out["snapshot_key"] = key
            out["device_id"] = device_id
            return out
    return None


def _be_already_protected(side: str, current_sl: float, entry: float, step: float) -> bool:
    tol = max(step * 0.5, 1e-12)
    if current_sl <= 0:
        return False
    if side == "BUY":
        return current_sl >= entry - tol
    if side == "SELL":
        return current_sl <= entry + tol
    return False


def _pending_status(R, pm: dict, now_ms: int) -> str:
    """Return WAIT, RETRY, or NONE for a previously queued BE job."""
    job_id = str(pm.get("be_pending_job_id") or "").strip()
    if not job_id:
        return "NONE"

    requested_ms = _i(pm.get("be_requested_at_ms"), 0)
    ack = _json(R.get(ACK_TEMPLATE.format(job_id=job_id)), None)
    if isinstance(ack, dict):
        if bool(ack.get("ok")):
            # Do not declare final BE success from ACK alone.
            # Persist the Agent/MT5 evidence for forensic analysis,
            # then wait for the normal broker position snapshot.
            pm["be_ack_ok"] = True
            pm["be_ack_at_ms"] = now_ms

            ack_result = (
                ack.get("result")
                if isinstance(
                    ack.get("result"),
                    dict,
                )
                else {}
            )

            if ack_result:
                pm["be_ack_retcode"] = (
                    ack_result.get("retcode")
                )

                pm["be_ack_request_id"] = (
                    ack_result.get("request_id")
                )

                pm["be_ack_requested_sl"] = (
                    ack_result.get(
                        "requested_sl"
                    )
                )

                pm["be_ack_sent_sl"] = (
                    ack_result.get(
                        "sent_sl"
                    )
                )

                pm["be_ack_broker_verified"] = bool(
                    ack_result.get(
                        "broker_verified"
                    )
                )

                pm["be_ack_broker_sl"] = (
                    ack_result.get(
                        "broker_confirmed_sl"
                    )
                )

                pm["be_ack_broker_tp"] = (
                    ack_result.get(
                        "broker_confirmed_tp"
                    )
                )

                pm["be_ack_verify_error"] = (
                    ack_result.get(
                        "broker_verify_error"
                    )
                )

                pm["be_ack_trade_tick_size"] = (
                    ack_result.get(
                        "trade_tick_size"
                    )
                )

                pm["be_ack_digits"] = (
                    ack_result.get("digits")
                )

            return "WAIT"
        pm["be_ack_ok"] = False
        pm["be_last_error"] = str(
            ack.get("error")
            or (ack.get("result") or {}).get("error")
            or "MODIFY_ACK_FAILED"
        )
        pm["be_pending_job_id"] = None
        pm["be_retry_after_ms"] = now_ms + 5_000
        return "RETRY"

    # The Agent may have popped the command but not posted the ACK yet.
    if requested_ms > 0 and now_ms - requested_ms < PENDING_ACK_WAIT_MS:
        return "WAIT"

    # If it is still physically queued, never duplicate it.
    try:
        device_id = str(pm.get("be_pending_device_id") or "").strip()
        if device_id:
            qkey = CMDQ_TEMPLATE.format(device_id=device_id)
            for raw in (R.lrange(qkey, 0, -1) or []):
                row = _json(raw, {})
                if isinstance(row, dict) and str(row.get("job_id") or "") == job_id:
                    return "WAIT"
    except Exception:
        return "WAIT"  # fail closed when queue state is unknown

    pm["be_last_error"] = "ACK_TIMEOUT_RETRY"
    pm["be_pending_job_id"] = None
    pm["be_retry_after_ms"] = now_ms + 5_000
    return "RETRY"


def _enqueue_be_modify(
    R,
    *,
    uid: str,
    pos: dict,
    bp: dict,
    device_id: str,
    trigger_r: float,
    current_r: float,
    be_sl: float,
    now_ms: int,
) -> dict:
    ticket = _i(
        pos.get("mt5_ticket")
        or pos.get("broker_ticket")
        or pos.get("position_ticket"),
        0,
    )
    job_id = "mt5_" + uuid.uuid4().hex
    payload = {
        "job_id": job_id,
        "type": "modify_position_sltp",
        "kind": "POSITION_MANAGEMENT",
        "mt5_account": str(pos.get("mt5_account") or "demo").lower().strip(),
        "symbol": str(pos.get("symbol") or "").upper().strip(),
        "side": str(pos.get("side") or "").upper().strip(),
        "ticket": ticket,
        "sl": float(be_sl),
        # TP is intentionally omitted. Agent preserves broker-live TP.
        "trade_id": str(pos.get("trade_id") or ""),
        "user_id": uid,
        "profile_id": str(pos.get("profile_id") or ""),
        "device_id": device_id,
        "reason": "BREAK_EVEN",
        "trigger_r": float(trigger_r),
        "observed_r": round(float(current_r), 4),
        "original_entry": _f(pos.get("entry_price"), 0.0),
        "original_sl": _f(pos.get("sl_price"), 0.0),
        "broker_sl_before": _f(bp.get("sl"), 0.0),
        "broker_tp_before": _f(bp.get("tp"), 0.0),
        "created_at_ms": int(now_ms),
        "source": "position_manager",
    }
    qkey = CMDQ_TEMPLATE.format(device_id=device_id)
    R.rpush(qkey, json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
    R.ltrim(qkey, -200, -1)
    return {"ok": True, "job_id": job_id, "device_id": device_id}


def _manage_trade(R, uid: str, trade_id: str, pos: dict, now_ms: int) -> tuple[bool, str]:
    symbol = str(pos.get("symbol") or "").upper().strip()
    trigger_r = BE_TRIGGER_R.get(symbol)
    if trigger_r is None:
        return False, "SYMBOL_NOT_MANAGED"

    if str(pos.get("trade_state") or "").upper().strip() != "TRADE_ACTIVE":
        # Filled legacy rows are accepted only when a broker ticket exists.
        if str(pos.get("status") or "").lower().strip() != "filled":
            return False, "NOT_ACTIVE"

    ticket = _i(
        pos.get("mt5_ticket")
        or pos.get("broker_ticket")
        or pos.get("position_ticket"),
        0,
    )
    if ticket <= 0:
        return False, "NO_TICKET"

    side = str(pos.get("side") or "").upper().strip()
    if side not in ("BUY", "SELL"):
        return False, "BAD_SIDE"

    ledger_entry = _f(
        pos.get("entry_price")
        or pos.get("mt5_fill_price"),
        0.0,
    )

    initial_sl = _f(
        pos.get("original_sl_price")
        or pos.get("sl_price"),
        0.0,
    )

    if ledger_entry <= 0 or initial_sl <= 0:
        return False, "MISSING_ENTRY_OR_INITIAL_SL"

    profile_id = str(pos.get("profile_id") or "").strip().lower()
    if not profile_id:
        return False, "PROFILE_REQUIRED"

    try:
        resolved = _resolve_prop_profile_device(profile_id, uid)
    except Exception as exc:
        log.warning(
            "[POSITION_MGR] PROFILE_RESOLVE_FAIL uid=%s trade_id=%s ticket=%s err=%r",
            uid, trade_id, ticket, exc,
        )
        return False, "PROFILE_RESOLVE_FAIL"

    if not isinstance(resolved, dict) or not resolved.get("ok") or not resolved.get("device_id"):
        return False, "PROFILE_DEVICE_NOT_CONNECTED"

    device_id = str(resolved.get("device_id") or "").strip()
    if not _device_fresh(R, device_id, now_ms):
        return False, "DEVICE_STALE"

    mt5_account = str(pos.get("mt5_account") or "demo").lower().strip()
    if mt5_account not in ("demo", "live"):
        return False, "BAD_MT5_ACCOUNT"

    bp = _load_broker_position(R, device_id, mt5_account, ticket)
    if not bp:
        return False, "BROKER_POSITION_NOT_FOUND"

    # Broker ticket and symbol are authoritative; reject any accidental cross-route.
    broker_symbol = str(bp.get("symbol") or "").upper().strip()
    if broker_symbol and not broker_symbol.startswith(symbol):
        return False, "BROKER_SYMBOL_MISMATCH"

    broker_entry = _f(bp.get("price_open"), 0.0)

    # Live MT5 position is authoritative for position management.
    # Ledger entry remains only a fallback if price_open is unavailable.
    entry = broker_entry if broker_entry > 0 else ledger_entry

    initial_risk = abs(entry - initial_sl)
    if initial_risk <= 0:
        return False, "BAD_INITIAL_RISK"

    price = _f(bp.get("price_current"), 0.0)
    current_sl = _f(bp.get("sl"), 0.0)
    current_tp = _f(bp.get("tp"), 0.0)
    step = _price_step(bp)

    if price <= 0 or step <= 0:
        return False, "BROKER_PRICE_OR_PRECISION_MISSING"

    buffer_r = float(BE_BUFFER_R.get(symbol, 0.0))
    buffer_distance = initial_risk * buffer_r

    if side == "BUY":
        be_sl_raw = entry + buffer_distance
    else:
        be_sl_raw = entry - buffer_distance

    be_sl = _normalize_price(
        be_sl_raw,
        bp,
    )

    current_r = (
        (price - entry) / initial_risk
        if side == "BUY"
        else (entry - price) / initial_risk
    )

    pm = pos.get("position_management")
    if not isinstance(pm, dict):
        pm = {}
        pos["position_management"] = pm

    pm["engine"] = "XTL_BE_V1"
    pm["be_buffer_r"] = float(buffer_r)
    pm["be_initial_risk"] = float(
        initial_risk
    )
    pm["be_target_sl_raw"] = float(
        be_sl_raw
    )
    
    pm["be_price_step"] = float(step)
    pm["be_trade_tick_size"] = _f(
        bp.get("trade_tick_size"),
        0.0,
    )
    pm["be_point"] = _f(
        bp.get("point"),
        0.0,
    )
    pm["be_digits"] = _i(
        bp.get("digits"),
        -1,
    )
    pm["be_trigger_r"] = float(trigger_r)
    pm["be_target_sl"] = float(be_sl)
    pm["last_observed_r"] = round(float(current_r), 4)
    pm["last_observed_price"] = float(price)
    pm["last_broker_sl"] = float(current_sl)
    pm["last_broker_tp"] = float(current_tp)
    pm["last_checked_at_ms"] = int(now_ms)
    pm["max_r_seen"] = round(
        max(_f(pm.get("max_r_seen"), current_r), current_r),
        4,
    )

    # Source of truth: if broker already has BE or a better SL, this rule is done.
    if _be_already_protected(side, current_sl, be_sl, step):
        pm["be_applied"] = True
        pm["be_verified_from_broker"] = True
        pm["be_verified_at_ms"] = int(now_ms)
        pm["be_final_broker_sl"] = float(current_sl)
        pm["be_pending_job_id"] = None
        return True, "BE_ALREADY_OR_NOW_APPLIED"

    pending = _pending_status(R, pm, now_ms)
    if pending == "WAIT":
        return True, "PENDING"

    retry_after = _i(pm.get("be_retry_after_ms"), 0)
    if retry_after and now_ms < retry_after:
        return True, "RETRY_COOLDOWN"

    if current_r + 1e-9 < float(trigger_r):
        return True, "BE_NOT_REACHED"

    # At trigger time the market must still be beyond entry. Never chase a stale MFE.
    if side == "BUY" and price <= be_sl + step:
        return True, "PRICE_NO_LONGER_ABOVE_BE"
    if side == "SELL" and price >= be_sl - step:
        return True, "PRICE_NO_LONGER_BELOW_BE"

    # Per-ticket cross-process enqueue lock.
    lock_key = f"xtl:position_manager:be_lock:{ticket}"
    lock_token = uuid.uuid4().hex
    try:
        locked = bool(R.set(lock_key, lock_token, nx=True, px=PER_TICKET_LOCK_MS))
    except Exception:
        return True, "LOCK_ERROR"
    if not locked:
        return True, "LOCK_BUSY"

    try:
        # Re-read broker snapshot inside the lock so two API workers cannot race.
        bp2 = _load_broker_position(R, device_id, mt5_account, ticket)
        if not bp2:
            return True, "BROKER_POSITION_GONE_AFTER_LOCK"
        price2 = _f(bp2.get("price_current"), 0.0)
        sl2 = _f(bp2.get("sl"), 0.0)
        step2 = _price_step(bp2)
        if _be_already_protected(side, sl2, be_sl, step2):
            pm["be_applied"] = True
            pm["be_verified_from_broker"] = True
            pm["be_verified_at_ms"] = int(now_ms)
            pm["be_final_broker_sl"] = float(sl2)
            pm["be_pending_job_id"] = None
            return True, "BE_APPLIED_DURING_LOCK"

        current_r2 = (
            (price2 - entry) / initial_risk
            if side == "BUY"
            else (entry - price2) / initial_risk
        )
        if current_r2 + 1e-9 < float(trigger_r):
            return True, "TRIGGER_LOST_DURING_LOCK"

        enq = _enqueue_be_modify(
            R,
            uid=uid,
            pos=pos,
            bp=bp2,
            device_id=device_id,
            trigger_r=float(trigger_r),
            current_r=float(current_r2),
            be_sl=float(be_sl),
            now_ms=now_ms,
        )
        pm["be_pending_job_id"] = enq["job_id"]
        pm["be_pending_device_id"] = device_id
        pm["be_requested_at_ms"] = int(now_ms)
        pm["be_request_r"] = round(float(current_r2), 4)
        pm["be_request_price"] = float(price2)
        pm["be_attempts"] = _i(pm.get("be_attempts"), 0) + 1
        pm["be_last_error"] = None
        pm["be_applied"] = False
        log.warning(
            "[POSITION_MGR] BE_ENQUEUE uid=%s profile=%s trade_id=%s ticket=%s "
            "sym=%s side=%s trigger_r=%.2f observed_r=%.4f entry=%s old_sl=%s new_sl=%s tp=%s job=%s device=%s",
            uid,
            profile_id,
            trade_id,
            ticket,
            symbol,
            side,
            trigger_r,
            current_r2,
            entry,
            sl2,
            be_sl,
            bp2.get("tp"),
            enq["job_id"],
            device_id,
        )
        return True, "BE_ENQUEUED"
    except Exception as exc:
        pm["be_last_error"] = f"ENQUEUE_EXCEPTION:{type(exc).__name__}:{exc}"
        log.exception(
            "[POSITION_MGR] BE_ENQUEUE_FAIL uid=%s trade_id=%s ticket=%s sym=%s",
            uid, trade_id, ticket, symbol,
        )
        return True, "BE_ENQUEUE_FAIL"


def tick_position_manager(R) -> dict:
    """Sweep indexed open-trade UIDs. No global Redis keyspace SCAN."""
    stats = {
        "checked": 0,
        "managed": 0,
        "enqueued": 0,
        "verified": 0,
        "stale_uids": 0,
        "errors": 0,
    }

    now = _now_ms()
    token = uuid.uuid4().hex

    try:
        if not R.set(
            "xtl:position_manager:sweep_lock",
            token,
            nx=True,
            px=SWEEP_LOCK_MS,
        ):
            return stats
    except Exception:
        stats["errors"] += 1
        return stats

    try:
        try:
            raw_uids = R.smembers(OPEN_UIDS_KEY) or set()
        except Exception:
            stats["errors"] += 1
            log.exception("[POSITION_MGR] open UID index read failed")
            return stats

        for raw_uid in raw_uids:
            uid = _decode(raw_uid).strip()
            if not uid:
                continue

            key = OPEN_KEY_TEMPLATE.format(uid=uid)

            try:
                rows = R.hgetall(key) or {}
            except Exception:
                stats["errors"] += 1
                log.exception(
                    "[POSITION_MGR] open ledger read failed uid=%s",
                    uid,
                )
                continue

            # Cheap self-heal for stale positive index membership.
            # A missing/empty ledger means this UID no longer belongs
            # in OPEN_UIDS_KEY.
            if not rows:
                try:
                    R.srem(
                        OPEN_UIDS_KEY,
                        uid,
                    )
                    stats["stale_uids"] += 1
                    log.warning(
                        "[POSITION_MGR] stale open UID index removed uid=%s",
                        uid,
                    )
                except Exception:
                    stats["errors"] += 1
                    log.exception(
                        "[POSITION_MGR] stale UID index cleanup failed uid=%s",
                        uid,
                    )
                continue

            for raw_trade_id, raw in rows.items():
                trade_id = _decode(raw_trade_id)
                pos = _json(raw, {})

                if not isinstance(pos, dict):
                    continue

                if str(pos.get("symbol") or "").upper().strip() not in BE_TRIGGER_R:
                    continue

                stats["checked"] += 1

                try:
                    changed, reason = _manage_trade(
                        R,
                        uid,
                        trade_id,
                        pos,
                        now,
                    )

                    if not changed:
                        continue

                    R.hset(
                        key,
                        raw_trade_id,
                        json.dumps(
                            pos,
                            separators=(",", ":"),
                            default=str,
                        ),
                    )

                    stats["managed"] += 1

                    if reason == "BE_ENQUEUED":
                        stats["enqueued"] += 1

                    if (
                        reason.startswith("BE_ALREADY")
                        or reason == "BE_APPLIED_DURING_LOCK"
                    ):
                        stats["verified"] += 1

                except Exception:
                    stats["errors"] += 1
                    log.exception(
                        "[POSITION_MGR] trade sweep failed "
                        "uid=%s trade_id=%s",
                        uid,
                        trade_id,
                    )

    except Exception:
        stats["errors"] += 1
        log.exception("[POSITION_MGR] indexed sweep failed")

    return stats

def start_position_manager(R) -> None:
    """Start the dedicated live-position-management thread once per API process."""
    global _started, _thread
    if _thread is not None and _thread.is_alive():
        _started = True
        return
    if _started:
        return
    _started = True

    def _loop() -> None:
        log.warning(
            "[POSITION_MGR] START thresholds=%s poll_sec=%s",
            BE_TRIGGER_R,
            POLL_SEC,
        )
        while True:
            try:
                stats = tick_position_manager(R)
                if stats.get("enqueued") or stats.get("verified") or stats.get("errors"):
                    log.warning(
                        "[POSITION_MGR] TICK checked=%s managed=%s enqueued=%s verified=%s errors=%s",
                        stats.get("checked"),
                        stats.get("managed"),
                        stats.get("enqueued"),
                        stats.get("verified"),
                        stats.get("errors"),
                    )
            except Exception:
                log.exception("[POSITION_MGR] loop failure")
            time.sleep(POLL_SEC)

    _thread = threading.Thread(
        target=_loop,
        name="xtl_position_manager",
        daemon=True,
    )
    _thread.start()
