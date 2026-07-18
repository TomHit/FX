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
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from api.prop_firms.prop_guard import compute_prop_check
from api.trend_endpoints import (
    _get_prop_config_safe,
    _get_prop_risk_state,
    _reserve_prop_open_risk,
    _release_prop_open_risk,
    _resolve_prop_profile_device,
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


def _normalize_order_type(pos: dict) -> str:
    raw = str(
        pos.get("order_type")
        or pos.get("mt5_order_type")
        or pos.get("entry_order_type")
        or "MARKET"
    ).upper().strip()

    aliases = {
        "BUY": "MARKET",
        "SELL": "MARKET",
        "MARKET_ORDER": "MARKET",
        "BUY_LIMIT": "BUY_LIMIT",
        "SELL_LIMIT": "SELL_LIMIT",
        "BUY_STOP": "BUY_STOP",
        "SELL_STOP": "SELL_STOP",
        "BUY_STOP_LIMIT": "BUY_STOP_LIMIT",
        "SELL_STOP_LIMIT": "SELL_STOP_LIMIT",
        "LIMIT": "LIMIT",
        "STOP": "STOP",
        "STOP_LIMIT": "STOP_LIMIT",
    }

    return aliases.get(raw, raw or "MARKET")


def _is_market_order(pos: dict) -> bool:
    return _normalize_order_type(pos) == "MARKET"


def _is_broker_pending_order(pos: dict) -> bool:
    return _normalize_order_type(pos) in {
        "LIMIT",
        "STOP",
        "STOP_LIMIT",
        "BUY_LIMIT",
        "SELL_LIMIT",
        "BUY_STOP",
        "SELL_STOP",
        "BUY_STOP_LIMIT",
        "SELL_STOP_LIMIT",
    }


MARKET_ACK_TIMEOUT_MS = int(
    float(
        os.getenv(
            "XTL_MARKET_ACK_TIMEOUT_SEC",
            "300",
        )
    )
    * 1000
)


def _clear_exec_claim_for_trade(r, uid: str, trade: dict, reason: str) -> bool:
    """
    Delete the permanent execution-deduplication claim after a trade reaches
    a terminal state.

    Safe to call repeatedly. Redis DEL is idempotent.
    """
    trade_id = str(
        trade.get("trade_id")
        or trade.get("tid")
        or trade.get("opportunity_id")
        or ""
    ).strip()

    if not uid or not trade_id:
        log.warning(
            "[EXEC_CLAIM_CLEANUP_SKIP] reason=%s uid=%s trade_id=%s symbol=%s ticket=%s",
            reason,
            uid,
            trade_id,
            trade.get("symbol"),
            trade.get("mt5_ticket") or trade.get("ticket"),
        )
        return False

    claim_key = f"xtl:oppt:exec_claim:{uid}:{trade_id}"

    try:
        deleted = int(r.delete(claim_key) or 0)

        log.warning(
            "[EXEC_CLAIM_CLEANUP] reason=%s uid=%s trade_id=%s "
            "claim_key=%s deleted=%s symbol=%s ticket=%s",
            reason,
            uid,
            trade_id,
            claim_key,
            deleted,
            trade.get("symbol"),
            trade.get("mt5_ticket") or trade.get("ticket"),
        )
        return True

    except Exception:
        log.exception(
            "[EXEC_CLAIM_CLEANUP_FAIL] reason=%s uid=%s trade_id=%s "
            "claim_key=%s symbol=%s ticket=%s",
            reason,
            uid,
            trade_id,
            claim_key,
            trade.get("symbol"),
            trade.get("mt5_ticket") or trade.get("ticket"),
        )
        return False

def _find_broker_position_for_pending(
    pos: dict,
    mt5_account: str,
) -> tuple[dict | None, bool]:
    """
    Return:
        (matching_position, broker_check_reliable)
    """
    symbol = str(
        pos.get("symbol")
        or ""
    ).upper().strip()

    device_id = str(
        pos.get("device_id")
        or ""
    ).strip()

    profile_id = str(
        pos.get("profile_id")
        or ""
    ).strip()

    if not symbol:
        return None, False

    try:
        if profile_id:
            resolved = _resolve_prop_profile_device(
                profile_id
            )

            if (
                not isinstance(resolved, dict)
                or not resolved.get("ok")
            ):
                log.error(
                    "[OPPT] MARKET_TIMEOUT_BROKER_CHECK_UNAVAILABLE "
                    "profile=%s reason=%s",
                    profile_id,
                    (
                        resolved.get("reason")
                        if isinstance(resolved, dict)
                        else "BAD_RESOLVE_PAYLOAD"
                    ),
                )
                return None, False

        live = _broker_xtl_positions(
            account_type=mt5_account,
            profile_id=profile_id or None,
        )

    except Exception as exc:
        log.exception(
            "[OPPT] MARKET_TIMEOUT_BROKER_CHECK_EXC "
            "profile=%s sym=%s err=%r",
            profile_id,
            symbol,
            exc,
        )
        return None, False

    for bp in live or []:
        if not isinstance(bp, dict):
            continue

        if (
            str(
                bp.get("symbol")
                or ""
            ).upper().strip()
            != symbol
        ):
            continue

        bp_device = str(
            bp.get("device_id")
            or bp.get("snapshot_device_id")
            or ""
        ).strip()

        if (
            device_id
            and bp_device
            and bp_device != device_id
        ):
            continue

        return bp, True

    return None, True

def _reconcile_stale_order_pending(
    uid: str,
    open_trades: list,
    mt5_account: str,
) -> None:
    now = now_ms()

    for pos in list(open_trades or []):
        if not isinstance(pos, dict):
            continue

        if (
            str(pos.get("execution_mode") or "").lower()
            != "mt5"
        ):
            continue

        if (
            str(pos.get("trade_state") or "").upper()
            != "ORDER_PENDING"
        ):
            continue

        trade_id = str(pos.get("trade_id") or "").strip()
        if not trade_id:
            continue

        order_type = _normalize_order_type(pos)
        job_id = str(
            pos.get("mt5_job_id")
            or pos.get("job_id")
            or ""
        ).strip()

        opened_ms = int(
            pos.get("opened_at_ms")
            or pos.get("entry_ts_ms")
            or 0
        )

        age_ms = (
            now - opened_ms
            if opened_ms > 0
            else 0
        )

        ack = _get_mt5_ack(job_id) if job_id else None

        # -------------------------------------------------
        # Explicit ACK is always handled first.
        # -------------------------------------------------
        if isinstance(ack, dict):
            # Existing ACK reconciliation remains source of truth.
            continue

        # -------------------------------------------------
        # Future broker pending orders:
        # absence of a position is expected before fill.
        # Never apply the MARKET five-minute timeout.
        # -------------------------------------------------
        if _is_broker_pending_order(pos):
            broker_order_ticket = int(
                pos.get("broker_order_ticket")
                or 0
            )

            expiry_at_ms = int(
                pos.get("expiry_at_ms")
                or 0
            )

            if expiry_at_ms > 0 and now >= expiry_at_ms:
                log.warning(
                    "[OPPT] BROKER_PENDING_EXPIRED "
                    "uid=%s tid=%s sym=%s order_type=%s "
                    "broker_order_ticket=%s expiry_at_ms=%s",
                    uid,
                    trade_id,
                    pos.get("symbol"),
                    order_type,
                    broker_order_ticket,
                    expiry_at_ms,
                )

                # Future path:
                # request broker cancellation / verify order history,
                # then close as PENDING_ORDER_EXPIRED.
                # Do not silently HDEL here.
            continue

        # -------------------------------------------------
        # MARKET only from this point onward.
        # -------------------------------------------------
        if not _is_market_order(pos):
            log.error(
                "[OPPT] ORDER_PENDING_UNKNOWN_TYPE "
                "uid=%s tid=%s sym=%s order_type=%r",
                uid,
                trade_id,
                pos.get("symbol"),
                order_type,
            )
            continue

        deadline_ms = int(
            pos.get("ack_deadline_ms")
            or (
                opened_ms + MARKET_ACK_TIMEOUT_MS
                if opened_ms > 0
                else 0
            )
        )

        if deadline_ms <= 0 or now < deadline_ms:
            continue

        # Critical safety check:
        # the ACK may have been lost after the broker opened a position.
        broker_pos, broker_check_reliable = (
            _find_broker_position_for_pending(
                pos,
                mt5_account,
            )
        )

        if not broker_check_reliable:
            log.error(
                "[OPPT] MARKET_ACK_TIMEOUT_DEFER_BROKER_UNKNOWN "
                "uid=%s tid=%s sym=%s job_id=%s",
                uid,
                trade_id,
                pos.get("symbol"),
                job_id,
            )
            continue

        if broker_pos:
            log.error(
                "[OPPT] MARKET_ACK_TIMEOUT_BUT_BROKER_LIVE "
                "uid=%s tid=%s sym=%s job_id=%s ticket=%s",
                uid,
                trade_id,
                pos.get("symbol"),
                job_id,
                broker_pos.get("ticket"),
            )

            # Existing broker-repair/position reconciliation should attach
            # the ticket. Never release risk or delete this trade.
            continue

        # No ACK, no broker position, and no broker deal is possible
        # without a ticket. This MARKET submission is an orphan/failure.
        failed = dict(pos)
        failed.update({
            "status": "failed",
            "trade_state": "ORDER_FAILED",
            "exit_reason": "MARKET_ACK_TIMEOUT",
            "order_failure_reason": "NO_ACK_NO_BROKER_POSITION",
            "failed_at_ms": now,
            "closed_at_ms": now,
            "pending_age_ms": int(age_ms),
            "order_type": "MARKET",
            "cleanup_source": "market_order_pending_watchdog",
        })

        log.error(
            "[OPPT] MARKET_ORDER_PENDING_TIMEOUT "
            "uid=%s tid=%s sym=%s side=%s "
            "job_id=%s device_id=%s profile_id=%s age_ms=%s",
            uid,
            trade_id,
            pos.get("symbol"),
            pos.get("side"),
            job_id,
            pos.get("device_id"),
            pos.get("profile_id"),
            age_ms,
        )

        try:
            _close_trade(
                uid,
                failed,
                float(
                    pos.get("entry_price")
                    or pos.get("entry")
                    or 0.0
                ),
                "MARKET_ACK_TIMEOUT",
                meta={
                    "source": (
                        "market_order_pending_watchdog"
                    ),
                    "job_id": job_id,
                    "device_id": pos.get("device_id"),
                    "profile_id": pos.get("profile_id"),
                    "no_real_trade": True,
                },
            )
        except Exception:
            log.exception(
                "[OPPT] MARKET_ORDER_PENDING_TIMEOUT_CLOSE_FAILED "
                "uid=%s tid=%s",
                uid,
                trade_id,
            )
            continue

        
        

        
       

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


def _tick_prop_watchdog(
    uid: str,
    profile_id: str | None = None,
    mt5_account: str = "demo",
) -> None:
    """
    Generic prop-firm daily-loss watchdog.

    FTMO compatibility:
    - Existing _tick_ftmo_watchdog() wrapper remains.
    - Existing FTMO halt reason, comments and trade IDs remain unchanged.
    """
    try:
        prop_cfg, cfg_ok, cfg_err = _get_prop_config_safe(profile_id)

        if not cfg_ok or not isinstance(prop_cfg, dict):
            log.error(
                "[PROP] WATCHDOG_CFG_READ_FAIL "
                "uid=%s profile=%s err=%s",
                uid,
                profile_id,
                cfg_err,
            )
            return

        pid = str(
            prop_cfg.get("profile_id")
            or profile_id
            or "ftmo-main"
        ).strip().lower()

        firm = str(
            prop_cfg.get("firm")
            or "ftmo"
        ).strip().lower()

        firm_labels = {
            "ftmo": "FTMO",
            "fundednext": "FundedNext",
            "fundingpips": "FundingPips",
        }
        firm_label = firm_labels.get(
            firm,
            firm.replace("_", " ").title(),
        )

        risk = _get_prop_risk_state(pid)
        open_risk = float(risk.get("open_risk_usd") or 0.0)

        account_size = float(
            prop_cfg.get("account_size")
            or risk.get("broker_equity")
            or risk.get("broker_balance")
            or 0.0
        )

        max_open_risk_pct = float(
            prop_cfg.get("max_open_risk_pct") or 3.0
        )

        max_open_risk_usd = (
            account_size * max_open_risk_pct / 100.0
            if account_size > 0
            else 0.0
        )

        if not bool(risk.get("snapshot_valid", True)):
            log.error(
                "[PROP] WATCHDOG_SKIP_INVALID_SNAPSHOT "
                "uid=%s profile=%s firm=%s",
                uid,
                pid,
                firm,
            )
            return

        # Generic fields first; FTMO aliases remain fallback protection.
        limit = float(
            risk.get("daily_loss_limit")
            or risk.get("ftmo_daily_loss_limit")
            or 0.0
        )
        used = float(
            risk.get("daily_loss_used")
            or risk.get("ftmo_daily_loss_used")
            or 0.0
        )

        if limit <= 0:
            log.warning(
                "[PROP] WATCHDOG_SKIP_NO_LIMIT "
                "uid=%s profile=%s firm=%s limit=%s",
                uid,
                pid,
                firm,
                limit,
            )
            return

        used_pct = (used / limit) * 100.0
        open_risk_pct = (
            (open_risk / account_size) * 100.0
            if account_size > 0
            else 0.0
        )

        log.warning(
            "[PROP] WATCHDOG_TICK "
            "uid=%s profile=%s firm=%s "
            "used=%.2f limit=%.2f used_pct=%.2f "
            "open_risk=%.2f "
            "open_risk_pct=%.2f "
            "snapshot_valid=%s",
            uid,
            pid,
            firm,
            used,
            limit,
            used_pct,
            open_risk,
            open_risk_pct,
            risk.get("snapshot_valid"),
        )

        day = str(risk.get("day") or time.strftime("%Y%m%d", time.gmtime()))

        # Profile-scoped keys prevent FTMO/FundedNext/FundingPips collisions.
        alert70_key = (
            f"xtl:prop:watchdog:alert70:{pid}:{day}"
        )
        alert80_key = (
            f"xtl:prop:watchdog:alert80:{pid}:{day}"
        )
        halt_key = f"xtl:prop:watchdog:openrisk:{pid}:{day}"

        if (
            max_open_risk_usd > 0
            and open_risk > max_open_risk_usd
        ):
            from api.trend_endpoints import _prop_set_halt

            if R.set(halt_key, "1", nx=True, ex=36 * 3600):

               _prop_set_halt(
                   pid,
                   "PROP_OPEN_RISK_LIMIT_EXCEEDED",
                   {
                       "profile_id": pid,
                       "firm": firm,
                       "open_risk": round(open_risk, 2),
                       "open_risk_limit": round(max_open_risk_usd, 2),
                       "open_risk_pct": round(open_risk_pct, 2),
                       "day": day,
                   },
               )

               log.error(
                   "[PROP] OPEN_RISK_LIMIT_EXCEEDED "
                   "profile=%s firm=%s "
                   "risk=%.2f limit=%.2f pct=%.2f",
                   pid,
                   firm,
                   open_risk,
                   max_open_risk_usd,
                   open_risk_pct,
               )

               _discord_trade_post(
                   f"🚫 **{firm_label} Open Risk Limit Exceeded**\n"
                   f"Profile: `{pid}`\n"
                   f"Open Risk: `${open_risk:.2f}` / `${max_open_risk_usd:.2f}` "
                   f"({open_risk_pct:.2f}%)\n"
                   "Action: Trading halted until next daily reset."
               )

            return
        if used_pct >= 70.0:
            if R.set(alert70_key, "1", nx=True, ex=36 * 3600):
                _discord_trade_post(
                    f"⚠ **{firm_label} Daily Loss Warning**\n"
                    f"Profile: `{pid}`\n"
                    f"Used: `${used:.2f}` / `${limit:.2f}` "
                    f"(`{used_pct:.1f}%`)\n"
                    "Action: Warning only."
                )

        if used_pct >= 80.0:
            from api.trend_endpoints import _prop_set_halt

            # Preserve current FTMO lifecycle names exactly.
            if firm == "ftmo":
                halt_reason = "FTMO_DAILY_LOSS_80_EMERGENCY"
                close_comment = "XTL FTMO_DAILY_LOSS_80_EMERGENCY"
                close_trade_prefix = "FTMO_EMERGENCY_CLOSE"
            else:
                firm_code = (
                    firm.upper()
                    .replace("-", "_")
                    .replace(" ", "_")
                )
                halt_reason = (
                    f"{firm_code}_DAILY_LOSS_80_EMERGENCY"
                )
                close_comment = (
                    f"XTL {firm_code}_DAILY_LOSS_80_EMERGENCY"
                )
                close_trade_prefix = (
                    f"{firm_code}_EMERGENCY_CLOSE"
                )

            halt_meta = {
                "profile_id": pid,
                "firm": firm,
                "used": round(used, 2),
                "limit": round(limit, 2),
                "used_pct": round(used_pct, 2),
                "day": day,
            }

            _prop_set_halt(
                pid,
                halt_reason,
                halt_meta,
            )

            if R.set(alert80_key, "1", nx=True, ex=36 * 3600):
                _discord_trade_post(
                    f"🚨 **{firm_label} Emergency Protection Activated**\n"
                    f"Profile: `{pid}`\n"
                    f"Used: `${used:.2f}` / `${limit:.2f}` "
                    f"(`{used_pct:.1f}%`)\n"
                    "Action: Closing all XTL-managed positions "
                    "and halting trading."
                )

            for bp in _broker_xtl_positions(
                account_type=mt5_account,
                profile_id=pid,
            ):
                try:
                    ticket = int(bp.get("ticket") or 0)
                except Exception:
                    ticket = 0

                sym = str(
                    bp.get("symbol") or ""
                ).upper().strip()

                try:
                    qty = float(bp.get("volume") or 0.0)
                except Exception:
                    qty = 0.0

                if ticket <= 0 or not sym or qty <= 0:
                    continue

                close_claim_key = (
                    f"xtl:prop:emergency_closing:{pid}:{ticket}"
                )

                if not R.set(
                    close_claim_key,
                    "1",
                    nx=True,
                    ex=15 * 60,
                ):
                    log.warning(
                        "[PROP] SKIP_DUP_EMERGENCY_CLOSE "
                        "ticket=%s sym=%s uid=%s profile=%s firm=%s",
                        ticket,
                        sym,
                        uid,
                        pid,
                        firm,
                    )
                    continue

                close_res = _enqueue_mt5_close_position(
                    uid=uid,
                    symbol=sym,
                    ticket=ticket,
                    qty=qty,
                    comment=close_comment,
                    trade_id=f"{close_trade_prefix}:{ticket}",
                    exit_reason=halt_reason,
                    mt5_account=mt5_account,
                    profile_id=pid,
                )

                log.warning(
                    "[PROP] EMERGENCY_CLOSE_ENQUEUE "
                    "uid=%s profile=%s firm=%s sym=%s ticket=%s "
                    "ok=%s device_id=%s route=%s err=%s",
                    uid,
                    pid,
                    firm,
                    sym,
                    ticket,
                    bool(close_res.get("ok")),
                    close_res.get("device_id"),
                    close_res.get("profile_resolve_reason"),
                    close_res.get("error"),
                )

    except Exception as e:
        log.error(
            "[PROP] PROP_WATCHDOG_EXC "
            "uid=%s profile=%s err=%r",
            uid,
            profile_id,
            e,
        )


def _tick_ftmo_watchdog(
    uid: str,
    mt5_account: str = "demo",
) -> None:
    """
    Backward-compatible FTMO watchdog wrapper.

    Keep this function while existing callers and tests still reference it.
    """
    _tick_prop_watchdog(
        uid=uid,
        profile_id="ftmo-main",
        mt5_account=mt5_account,
    )
def _broker_price_step(bp: dict) -> float:
    """
    Return the broker's minimum tradable price step.

    Priority:
      1. trade_tick_size
      2. tick_size
      3. point
      4. digits-derived point
    """
    for field in ("trade_tick_size", "tick_size", "point"):
        try:
            value = float(bp.get(field) or 0.0)
        except Exception:
            value = 0.0

        if value > 0:
            return value

    try:
        digits = int(bp.get("digits"))
    except Exception:
        digits = -1

    if digits >= 0:
        return float(Decimal("1").scaleb(-digits))

    return 0.0


def _normalize_price_to_step(price: object, step: float) -> float:
    """
    Normalize a requested strategy price to the nearest broker tick.

    Decimal + ROUND_HALF_UP avoids Python float/banker's-rounding errors.
    """
    try:
        price_d = Decimal(str(price))
        step_d = Decimal(str(step))

        if step_d <= 0:
            return float(price_d)

        ticks = (price_d / step_d).quantize(
            Decimal("1"),
            rounding=ROUND_HALF_UP,
        )
        return float(ticks * step_d)
    except (InvalidOperation, TypeError, ValueError):
        return float(price or 0.0)


def _broker_price_was_changed(
    original_price: object,
    broker_price: float,
    step: float,
) -> bool:
    """
    True only when the broker price differs from the correctly normalized
    original strategy price by more than floating-point noise.
    """
    if broker_price <= 0 or original_price in (None, "", 0):
        return False

    normalized_original = _normalize_price_to_step(
        original_price,
        step,
    )

    # Tiny tolerance only for float serialization noise after normalization.
    epsilon = max(step * 1e-6, 1e-12)

    return abs(normalized_original - broker_price) > epsilon

def _update_prop_daily_outcome_on_close(pos: dict, pnl: float) -> None:
    try:
        from api.trend_endpoints import _prop_day_key, _prop_daily_key

        profile_id = str(pos.get("profile_id") or "ftmo-main")
        day = _prop_day_key(profile_id)
        daily_key = _prop_daily_key(profile_id, day)

        risk_usd = float(
            pos.get("risk_usd")
            or pos.get("reserved_risk_usd")
            or pos.get("prop_risk_usd")
            or 0.0
        )

        pnl_f = float(pnl or 0.0)
        r_delta = (pnl_f / risk_usd) if risk_usd > 0 else 0.0

        pipe = R.pipeline()
        if pnl_f > 0:
            pipe.hincrby(daily_key, "wins_today", 1)
        elif pnl_f < 0:
            pipe.hincrby(daily_key, "losses_today", 1)

        pipe.hincrbyfloat(daily_key, "daily_r", round(float(r_delta), 4))
        pipe.hset(daily_key, "last_closed_trade_id", str(pos.get("trade_id") or ""))
        pipe.hset(daily_key, "last_closed_symbol", str(pos.get("symbol") or ""))
        pipe.hset(daily_key, "last_closed_pnl", round(pnl_f, 2))
        pipe.hset(daily_key, "last_closed_r", round(float(r_delta), 4))
        pipe.expire(daily_key, 3 * 24 * 3600)
        pipe.execute()

        log.warning(
            "[PROP] DAILY_OUTCOME_UPDATE trade_id=%s sym=%s pnl=%s risk=%s r_delta=%s daily_key=%s",
            pos.get("trade_id"),
            pos.get("symbol"),
            round(pnl_f, 2),
            round(risk_usd, 2),
            round(float(r_delta), 4),
            daily_key,
        )
    except Exception as e:
        log.warning("[PROP] DAILY_OUTCOME_UPDATE_EXC err=%r", e)
def _make_repair_prop_check(
    *,
    prop_cfg: dict,
    symbol: str,
    side: str,
    entry: float,
    sl: float,
    tp: float,
    lots: float,
    risk_usd: float,
) -> dict:
    risk_pct = 0.0
    try:
        acct_size = float(prop_cfg.get("account_size") or 0.0)
        if acct_size > 0:
            risk_pct = (float(risk_usd) / acct_size) * 100.0
    except Exception:
        risk_pct = 0.0

    planned_rr = 0.0
    try:
        risk_dist = abs(float(entry) - float(sl))
        reward_dist = abs(float(tp) - float(entry))
        if risk_dist > 0 and reward_dist > 0:
            planned_rr = reward_dist / risk_dist
    except Exception:
        planned_rr = 0.0

    return {
        "verdict": "ALLOW",
        "source": "broker_repair",
        "firm": str(prop_cfg.get("firm") or ""),
        "phase": str(prop_cfg.get("phase") or ""),
        "symbol": str(symbol or "").upper(),
        "side": str(side or "").upper(),
        "entry": float(entry or 0.0),
        "sl": float(sl or 0.0),
        "tp": float(tp or 0.0),
        "lots": float(lots or 0.0),
        "risk_usd": round(float(risk_usd or 0.0), 2),
        "risk_pct": round(float(risk_pct or 0.0), 4),
        "target_rr": float(prop_cfg.get("target_rr") or 0.0),
        "planned_rr": round(float(planned_rr or 0.0), 4),
    }

def _sync_open_trade_broker_sl_tp(uid: str, bp: dict) -> None:
    """
    Sync live broker SL/TP/current price into XTL open trade ledger.

    Important:
    - Do NOT overwrite original sl_price/tp_price.
    - Store broker_current_sl / broker_current_tp separately for analytics.
    """
    try:
        ticket = int(bp.get("ticket") or 0)
        if ticket <= 0:
            return

        open_key = OPEN_KEY.format(uid=uid)
        vals = R.hgetall(open_key) or {}

        for trade_id, raw in vals.items():
            pos = _sj(raw, {})
            if not isinstance(pos, dict):
                continue

            mt5_ticket = int(
                pos.get("mt5_ticket")
                or pos.get("broker_ticket")
                or pos.get("position_ticket")
                or 0
            )

            if mt5_ticket != ticket:
                continue

            old_sl = pos.get("original_sl_price") or pos.get("sl_price")
            old_tp = pos.get("original_tp_price") or pos.get("tp_price")

            broker_sl = bp.get("sl")
            broker_tp = bp.get("tp")

            try:
                broker_sl_f = float(broker_sl) if broker_sl not in (None, "", 0) else 0.0
            except Exception:
                broker_sl_f = 0.0

            try:
                broker_tp_f = float(broker_tp) if broker_tp not in (None, "", 0) else 0.0
            except Exception:
                broker_tp_f = 0.0

            pos["broker_current_sl"] = broker_sl_f
            pos["broker_current_tp"] = broker_tp_f
            pos["broker_current_price"] = float(bp.get("price_current") or 0.0)
            pos["broker_floating_pnl"] = float(bp.get("profit") or 0.0)
            pos["broker_volume"] = float(bp.get("volume") or pos.get("qty") or 0.0)
            pos["broker_sl_tp_synced_at_ms"] = now_ms()
            pos["broker_snapshot_key"] = str(bp.get("snapshot_key") or "")
            pos["broker_device_id"] = str(bp.get("device_id") or "")

            broker_price_step = _broker_price_step(bp)

            if broker_price_step <= 0:
                log.error(
                    "[OPPT] BROKER_SLTP_PRECISION_MISSING "
                    "ticket=%s sym=%s digits=%s point=%s "
                    "trade_tick_size=%s tick_size=%s",
                    ticket,
                    pos.get("symbol"),
                    bp.get("digits"),
                    bp.get("point"),
                    bp.get("trade_tick_size"),
                    bp.get("tick_size"),
                )

                # Fail safe: missing broker precision must not create a false
                # claim that SL or TP was manually modified.
                pos["sl_changed_from_original"] = False
                pos["tp_changed_from_original"] = False
            else:
                try:
                    pos["sl_changed_from_original"] = (
                        _broker_price_was_changed(
                            original_price=old_sl,
                            broker_price=broker_sl_f,
                            step=broker_price_step,
                        )
                    )
                except Exception:
                    pos["sl_changed_from_original"] = False

                try:
                    pos["tp_changed_from_original"] = (
                        _broker_price_was_changed(
                            original_price=old_tp,
                            broker_price=broker_tp_f,
                            step=broker_price_step,
                        )
                    )
                except Exception:
                    pos["tp_changed_from_original"] = False

            pos["broker_price_step"] = broker_price_step

            R.hset(
                open_key,
                trade_id,
                json.dumps(pos, separators=(",", ":"), default=str),
            )

            log.warning(
                "[OPPT] BROKER_SLTP_SYNC "
                "uid=%s trade_id=%s ticket=%s sym=%s "
                "broker_sl=%s broker_tp=%s price_step=%s "
                "sl_changed=%s tp_changed=%s",
                uid,
                trade_id,
                ticket,
                pos.get("symbol"),
                broker_sl_f,
                broker_tp_f,
                broker_price_step,
                pos.get("sl_changed_from_original"),
                pos.get("tp_changed_from_original"),
            )
            return
    except Exception as e:
        log.warning("[OPPT] BROKER_SLTP_SYNC_FAILED uid=%s ticket=%s err=%r", uid, bp.get("ticket"), e)

def _broker_live_position_count(device_id: str, account_type: str = "demo") -> int:
    try:
        dev = str(device_id or "").strip()
        acct = str(account_type or "demo").strip().lower()
        if not dev:
            return 0

        raw = R.get(f"xtl:mt5:pos:{dev}:{acct}")
        arr = json.loads(raw) if raw else []
        if not isinstance(arr, list):
            return 0

        return len([p for p in arr if isinstance(p, dict)])
    except Exception:
        return 0



def _broker_xtl_positions(
    account_type: str = "demo",
    profile_id: str | None = None,
) -> list[dict]:
    """
    Return current broker-open XTL positions.

    Routing rules:
    - profile_id supplied: read only the MT5 snapshot strictly bound
      to that prop profile.
    - profile_id omitted: preserve legacy behavior and scan all devices.
    """
    out: list[dict] = []
    seen: set[int] = set()
    acct = str(account_type or "demo").strip().lower()

    keys: list[Any] = []

    if profile_id:
        try:
            resolved = _resolve_prop_profile_device(profile_id)
        except Exception as e:
            log.error(
                "[PROP] BROKER_POS_PROFILE_RESOLVE_EXC "
                "profile=%s err=%r",
                profile_id,
                e,
            )
            return []

        if not isinstance(resolved, dict) or not resolved.get("ok"):
            log.error(
                "[PROP] BROKER_POS_PROFILE_RESOLVE_FAILED "
                "profile=%s reason=%s",
                profile_id,
                (
                    resolved.get("reason")
                    if isinstance(resolved, dict)
                    else "BAD_RESOLVE_PAYLOAD"
                ),
            )
            return []

        dev_id = str(resolved.get("device_id") or "").strip()
        if not dev_id:
            log.error(
                "[PROP] BROKER_POS_PROFILE_DEVICE_MISSING "
                "profile=%s",
                profile_id,
            )
            return []

        keys = [f"xtl:mt5:pos:{dev_id}:{acct}"]

    else:
        # Legacy callers intentionally inspect all connected MT5 devices.
        try:
            keys = list(R.scan_iter(f"xtl:mt5:pos:*:{acct}"))
        except Exception:
            keys = []

    for key in keys:
        try:
            key_s = (
                key.decode("utf-8", "ignore")
                if isinstance(key, (bytes, bytearray))
                else str(key)
            )

            raw = R.get(key_s)
            arr = _sj(raw, []) if raw else []

            if not isinstance(arr, list):
                continue

            parts = key_s.split(":")
            dev_id = parts[3] if len(parts) >= 5 else ""

            for bp in arr:
                if not isinstance(bp, dict):
                    continue

                try:
                    ticket = int(bp.get("ticket") or 0)
                except Exception:
                    ticket = 0

                if ticket <= 0 or ticket in seen:
                    continue

                comment = str(bp.get("comment") or "")

                try:
                    magic = int(bp.get("magic") or 0)
                except Exception:
                    magic = 0

                # Only XTL-managed broker positions.
                if magic != 20251227 and not comment.upper().startswith("XTL"):
                    continue

                sym = str(bp.get("symbol") or "").upper().strip()
                side = str(bp.get("side") or "").upper().strip()

                if not sym:
                    continue

                if side not in ("BUY", "SELL"):
                    try:
                        side = (
                            "BUY"
                            if int(bp.get("type") or -1) == 0
                            else "SELL"
                        )
                    except Exception:
                        side = ""

                if side not in ("BUY", "SELL"):
                    continue

                bpc = dict(bp)
                bpc["ticket"] = ticket
                bpc["symbol"] = sym
                bpc["side"] = side
                bpc["device_id"] = dev_id
                bpc["snapshot_key"] = key_s
                bpc["profile_id"] = str(profile_id or "")

                out.append(bpc)
                seen.add(ticket)

        except Exception as e:
            log.warning(
                "[PROP] BROKER_POS_READ_EXC "
                "profile=%s key=%s err=%r",
                profile_id,
                key,
                e,
            )
            continue

    return out

def _broker_has_active_xtl_symbol(
    symbol: str,
    account_type: str = "demo",
    profile_id: str | None = None,
) -> dict | None:
    sym = str(symbol or "").upper().strip()
    if not sym:
        return None

    for bp in _broker_xtl_positions(
        account_type=account_type,
        profile_id=profile_id,
    ):
        if str(bp.get("symbol") or "").upper().strip() == sym:
            return bp

    return None


def _sync_watches_for_broker_active_position(bp: dict, reason: str = "BROKER_ACTIVE") -> None:
    """Broker-truth symbol guard.

    If broker has an active XTL position, no opposite-side REV_OK/ENTRY_READY
    watch may survive. The same-side watch, when present, is marked
    TRADE_ACTIVE so UI/gate reflects broker truth even if the open ledger is
    temporarily behind.
    """
    try:
        sym = str(bp.get("symbol") or "").upper().strip()
        side = str(bp.get("side") or "").upper().strip()
        ticket = int(bp.get("ticket") or 0)
        if not sym or side not in ("BUY", "SELL") or ticket <= 0:
            return

        opp_side = "SELL" if side == "BUY" else "BUY"

        # Opposite side must not remain REV_OK/ENTRY_READY while broker position exists.
        R.delete(_zone_watch_key(uid,sym, opp_side, "H1"))
        R.delete(f"xtl:watch:break_state:{sym}:{opp_side}:H1")
        for k in R.scan_iter(f"xtl:watch:entry_claim:{sym}:{opp_side}:H1:*"):
            R.delete(k)

        # Same side must always show broker-truth TRADE_ACTIVE.
        same_key = _zone_watch_key(sym, side, "H1")
        raw_w = R.get(same_key)
        w = _sj(raw_w, {}) if raw_w else {}

        if not isinstance(w, dict):
            w = {}

        w["state"] = "TRADE_ACTIVE"
        w["trade_state"] = "TRADE_ACTIVE"
        w["direction"] = side
        w["side"] = side
        w["symbol"] = sym
        w["tf"] = "H1"
        w["entry_triggered"] = True
        w["mt5_ticket"] = ticket
        w["broker_ticket"] = ticket
        w["broker_active_reason"] = str(reason or "BROKER_ACTIVE")
        w["broker_active_seen_ms"] = now_ms()
        w["device_id"] = str(bp.get("device_id") or w.get("device_id") or "")
        w["qty"] = float(bp.get("volume") or 0)
        w["entry_price"] = float(bp.get("price_open") or 0)
        w["last_price"] = float(bp.get("price_current") or 0)
        w["broker_profit"] = float(bp.get("profit") or 0)

        # ZONE PRESERVATION (fix: broker-scan watches lost the frozen band).
        # The broker scan rebuilds the watch from MT5 position truth, which has
        # no knowledge of the SR zone. If the pre-existing watch already carries
        # a zone_used band, keep it. Otherwise rehydrate from the independent
        # zone_by_ticket record written at entry time (keyed by broker ticket).
        # Merge only — never fabricate. If neither source has a band, leave it
        # absent rather than inventing one.
        def _has_band(z) -> bool:
            if not isinstance(z, dict):
                return False
            try:
                lo, hi = z.get("low"), z.get("high")
                return lo is not None and hi is not None and float(lo) < float(hi)
            except Exception:
                return False

        if not _has_band(w.get("zone_used")):
            try:
                _zraw = R.get(f"xtl:trade:zone_by_ticket:{ticket}")
                _zmeta = _sj(_zraw, {}) if _zraw else {}
            except Exception:
                _zmeta = {}
            if isinstance(_zmeta, dict) and _zmeta:
                _ez = _zmeta.get("entry_zone")
                if _has_band(_ez):
                    _rebuilt = dict(_ez)
                else:
                    # Reconstruct a band from the flat entry_zone_* fields.
                    _rebuilt = {}
                    _lo = _zmeta.get("entry_zone_low")
                    _hi = _zmeta.get("entry_zone_high")
                    if _lo is not None and _hi is not None:
                        try:
                            if float(_lo) < float(_hi):
                                _rebuilt["low"] = float(_lo)
                                _rebuilt["high"] = float(_hi)
                        except Exception:
                            _rebuilt = {}
                    if _zmeta.get("entry_zone_level") is not None:
                        try:
                            _rebuilt["level"] = float(_zmeta["entry_zone_level"])
                        except Exception:
                            pass
                    if _zmeta.get("entry_zone_tf"):
                        _rebuilt["tf"] = str(_zmeta["entry_zone_tf"])
                    if _zmeta.get("entry_zone_kind"):
                        _rebuilt["kind"] = str(_zmeta["entry_zone_kind"])
                # Only attach if we actually recovered a band or at least a level.
                if _has_band(_rebuilt) or _rebuilt.get("level") is not None:
                    _rebuilt.setdefault("zone_source", "REHYDRATED_FROM_TICKET")
                    w["zone_used"] = _rebuilt

        R.set(same_key, json.dumps(w, separators=(",", ":")), ex=7 * 24 * 3600)

        log.warning(
            "[WATCHLIST] BROKER_ACTIVE_SYMBOL_GUARD sym=%s active_side=%s ticket=%s cleared_side=%s reason=%s",
            sym, side, ticket, opp_side, reason,
        )
    except Exception as e:
        log.warning("[WATCHLIST] BROKER_ACTIVE_SYMBOL_GUARD_FAILED err=%r", e)

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

from api.tenant_keys import zone_watch_key

def _zone_watch_key(
    uid: str,
    sym: str,
    side: str,
    tf: str = "H1",
) -> str:
    return zone_watch_key(
        uid,
        sym,
        side,
        tf,
    )

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
    profile_id: str | None = None,
) -> dict:
    dev_id = None
    resolved_account = None
    resolve_reason = ""

    if profile_id:
        try:
            res = _resolve_prop_profile_device(profile_id)
            if isinstance(res, dict) and res.get("ok") and res.get("device_id"):
                dev_id = str(res.get("device_id"))
                resolved_account = res.get("account")
                resolve_reason = str(res.get("reason") or "")
        except Exception as e:
            resolve_reason = f"RESOLVE_EXC:{type(e).__name__}"

    if profile_id and not dev_id:
        return {
            "ok": False,
            "error": "profile_device_not_connected",
            "profile_id": str(profile_id),
            "profile_resolve_reason": (
                resolve_reason
                or "STRICT_PROP_ACCOUNT_NOT_CONNECTED"
            ),
        }

    if not profile_id and not dev_id:
        dev_id = _pick_device_for_symbol(user_id, sym)
        resolve_reason = "LEGACY_PICK_DEVICE_FOR_SYMBOL"

    if not dev_id:
        return {"ok": False, "error": "no_device", "profile_id": str(profile_id or ""), "profile_resolve_reason": resolve_reason}

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
        "profile_id": str(profile_id or ""),
        "profile_resolve_reason": resolve_reason,
        "profile_account_login": (
            str((resolved_account or {}).get("login") or "")
            if isinstance(resolved_account, dict)
            else ""
        ),
        "profile_account_server": (
            str((resolved_account or {}).get("server") or "")
            if isinstance(resolved_account, dict)
            else ""
        ),
    }

    try:
        R.rpush(_mt5_cmdq_key(dev_id), json.dumps(cmd))
        R.ltrim(_mt5_cmdq_key(dev_id), -200, -1)
    except Exception as e:
        return {"ok": False, "error": f"enqueue_failed:{type(e).__name__}", "profile_id": str(profile_id or ""), "profile_resolve_reason": resolve_reason}

    return {
        "ok": True,
        "job_id": job_id,
        "device_id": dev_id,
        "profile_id": str(profile_id or ""),
        "profile_resolve_reason": resolve_reason,
    }


def _enqueue_mt5_close_position(
    uid: str,
    symbol: str,
    ticket: int,
    qty: float,
    comment: str,
    trade_id: str,
    exit_reason: str,
    mt5_account: str,
    profile_id: str | None = None,
) -> Dict[str, Any]:
    """
    Queue a hedging-safe close command by broker position ticket.

    When profile_id is supplied, the close must route to that profile's
    strictly resolved device. Legacy callers without profile_id retain
    the previous symbol-based routing temporarily.
    """
    resolve_reason = ""

    if profile_id:
        try:
            resolved = _resolve_prop_profile_device(profile_id)
        except Exception as e:
            return {
                "ok": False,
                "error": (
                    "profile_device_resolve_exc:"
                    f"{type(e).__name__}"
                ),
                "profile_id": str(profile_id),
            }

        if not isinstance(resolved, dict) or not resolved.get("ok"):
            return {
                "ok": False,
                "error": "profile_device_not_connected",
                "profile_id": str(profile_id),
                "profile_resolve_reason": (
                    resolved.get("reason")
                    if isinstance(resolved, dict)
                    else "BAD_RESOLVE_PAYLOAD"
                ),
            }

        dev_id = str(resolved.get("device_id") or "").strip()
        resolve_reason = str(resolved.get("reason") or "")

    else:
        # Temporary backward compatibility for non-profile callers.
        dev_id = _pick_device_for_symbol(uid, symbol)
        resolve_reason = "LEGACY_PICK_DEVICE_FOR_SYMBOL"

    if not dev_id:
        return {
            "ok": False,
            "error": "no_device",
            "profile_id": str(profile_id or ""),
            "profile_resolve_reason": resolve_reason,
        }

    try:
        ticket_i = int(ticket)
    except Exception:
        ticket_i = 0

    if ticket_i <= 0:
        return {
            "ok": False,
            "error": "missing_ticket",
            "device_id": dev_id,
            "profile_id": str(profile_id or ""),
            "profile_resolve_reason": resolve_reason,
        }

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
        "profile_id": str(profile_id or ""),
        "device_id": dev_id,
        "profile_resolve_reason": resolve_reason,
        "ts_ms": int(time.time() * 1000),
    }

    try:
        R.rpush(
            _mt5_cmdq_key(dev_id),
            json.dumps(payload, ensure_ascii=False),
        )
        R.ltrim(_mt5_cmdq_key(dev_id), -200, -1)

    except Exception as e:
        return {
            "ok": False,
            "error": f"redis_rpush_failed:{type(e).__name__}",
            "device_id": dev_id,
            "profile_id": str(profile_id or ""),
            "profile_resolve_reason": resolve_reason,
        }

    return {
        "ok": True,
        "job_id": payload["job_id"],
        "device_id": dev_id,
        "profile_id": str(profile_id or ""),
        "profile_resolve_reason": resolve_reason,
    }


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
    try:
        if "original_sl_price" not in pos:
            pos["original_sl_price"] = pos.get("sl_price")
        if "original_tp_price" not in pos:
            pos["original_tp_price"] = pos.get("tp_price")
        if "original_entry_price" not in pos:
            pos["original_entry_price"] = pos.get("entry_price")
    except Exception:
        pass

    R.hset(OPEN_KEY.format(uid=uid), pos["trade_id"], json.dumps(pos))
def _clear_trade_lifecycle_keys(uid: str,pos: Dict[str, Any]) -> None:
    try:
        sym = str(pos.get("symbol") or "").upper().strip()
        side = str(pos.get("side") or "").upper().strip()
        if not sym:
            return

        # clear both sides because opposite stale watch may exist
        for s in ("BUY", "SELL"):
            R.delete(_zone_watch_key(uid, sym, s, "H1"))
            R.delete(_zone_watch_key(uid, sym, s, "H4"))

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


def _closed_ticket_key(ticket: int) -> str:
    return f"xtl:broker:closed_ticket:{int(ticket)}"

def _load_mt5_deal(ticket: int) -> dict:
    try:
        tk = int(ticket or 0)
        if tk <= 0:
            return {}
        raw = R.get(f"xtl:mt5:deal:{tk}")
        if not raw:
            return {}
        d = json.loads(raw)
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}

def _apply_broker_deal_to_closed(pos: dict, deal: dict, reason_fallback: str = "BROKER_CLOSED") -> tuple[float, str, dict]:
    """
    Broker deal is final truth for exit.
    Returns: exit_price, exit_reason, meta
    """
    exit_price = (
        _sf(deal.get("close_price"), 0.0)
        or _sf(pos.get("last_price"), 0.0)
        or _sf(pos.get("entry_price"), 0.0)
    )

    broker_reason = str(deal.get("broker_reason") or "").upper().strip()
    exit_reason = broker_reason if broker_reason in ("TP", "SL", "STOP_OUT", "MANUAL") else reason_fallback

    broker_reason_u = str(deal.get("broker_reason") or "").upper().strip()

    manual_close_detected = None
    if broker_reason_u in ("TP", "SL", "STOP_OUT"):
        manual_close_detected = False
    elif broker_reason_u in ("MANUAL", "MANUAL_CLOSE"):
        manual_close_detected = True
    elif broker_reason_u in ("BROKER_CLOSED", "CLOSED"):
        manual_close_detected = None

    meta = {
        "source": "broker_deal",
        "broker_deal": deal,
        "broker_close_price": deal.get("close_price"),
        "broker_close_time_ms": deal.get("close_time_ms"),
        "broker_open_time_ms": deal.get("open_time_ms"),
        "broker_net_profit": deal.get("net_profit"),
        "broker_reason": deal.get("broker_reason"),
        "exit_deal_ticket": deal.get("exit_deal_ticket"),
        "close_order_ticket": deal.get("close_order_ticket"),
        "manual_close_detected": manual_close_detected,
    }

    return float(exit_price or 0.0), exit_reason, meta


def _mark_ticket_closed(ticket: int, trade_id: str = "", reason: str = "") -> None:
    try:
        tk = int(ticket or 0)
        if tk > 0:
            R.setex(
                _closed_ticket_key(tk),
                3 * 24 * 3600,
                json.dumps({
                    "ticket": tk,
                    "trade_id": str(trade_id or ""),
                    "reason": str(reason or ""),
                    "closed_at_ms": now_ms(),
                }),
            )
    except Exception:
        pass


def _is_ticket_recently_closed(ticket: int) -> bool:
    try:
        tk = int(ticket or 0)
        return bool(tk > 0 and R.exists(_closed_ticket_key(tk)))
    except Exception:
        return False
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
    Maximum allowed move from original blocked ENTRY_CAND before retry.
    Strict defaults to avoid late/chasing entries:
      - FX: 2 pips
      - JPY: 2 pips
      - XAUUSD: $1.00
    """
    sym_u = str(sym or "").upper().strip()
    if sym_u == "XAUUSD":
        return float(os.getenv("XTL_LATE_ENTRY_XAUUSD_USD", "1.0"))
    if sym_u.endswith("JPY"):
        return float(os.getenv("XTL_LATE_ENTRY_JPY_PIPS", "2")) * 0.01
    return float(os.getenv("XTL_LATE_ENTRY_FX_PIPS", "2")) * 0.0001

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
                try:
                    _blocked_entry = float(ev.get("entry_price") or ev.get("live_px") or 0.0)
                    if _blocked_entry > 0:
                        w["blocked_entry_price"] = _blocked_entry
                        w["blocked_entry_side"] = str(side)
                        w["blocked_entry_trade_id"] = str(ev.get("trade_id") or "")
                        w["blocked_entry_at_ms"] = now_ms()
                except Exception:
                    pass
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
    qty = _sf(pos.get("qty") or pos.get("lots") or pos.get("volume"), 1.0)
    entry = _sf(pos.get("entry_price") or pos.get("entry"), 0.0)

    pnl = 0.0
    if side == "BUY":
        pnl = (exit_price - entry) * qty
    elif side == "SELL":
        pnl = (entry - exit_price) * qty
    
    # Broker deal is final truth for closed P/L when available.
    try:
        if meta and isinstance(meta, dict):
            bd = meta.get("broker_deal")
            if isinstance(bd, dict) and bd.get("net_profit") is not None:
                pnl = float(bd.get("net_profit") or 0.0)
    except Exception:
        pass

    closed = dict(pos)
    closed["exit_price"] = float(exit_price)
    closed["exit_reason"] = str(reason)
    closed["pnl"] = float(pnl)
    closed["closed_at_ms"] = now_ms()
    try:
        if meta and isinstance(meta, dict):
            closed["broker_net_profit"] = meta.get("broker_net_profit")
            closed["broker_close_price"] = meta.get("broker_close_price")
            closed["broker_close_time_ms"] = meta.get("broker_close_time_ms")
            closed["broker_open_time_ms"] = meta.get("broker_open_time_ms")
            closed["broker_reason"] = meta.get("broker_reason")
            if meta.get("broker_close_time_ms"):
                closed["closed_at_ms"] = int(meta.get("broker_close_time_ms"))
    except Exception:
        pass
   
    closed["status"] = "closed"
    closed["trade_state"] = "EXITED"
    closed["close_lifecycle_version"] = "2.0"
    closed["close_finalizer"] = "_close_trade"
    closed["close_origin"] = str(
        pos.get("source")
        or "unknown"
    )
    closed["broker_deal_applied"] = bool(
        isinstance(meta, dict)
        and isinstance(meta.get("broker_deal"), dict)
        and meta["broker_deal"].get("ok")
    )
    

    try:
        _release_prop_open_risk(
            trade_id=str(pos.get("trade_id") or ""),
            result=str(reason or "").lower(),
            pnl_usd=float(closed.get("pnl") or 0.0),
            profile_id=str(
                pos.get("profile_id")
                or pos.get("account_id")
                or "ftmo-main"
            ),
        )
    except Exception as e:
        log.warning(
            "[PROP] RELEASE_FAILED trade_id=%s reason=%s err=%r",
            pos.get("trade_id"),
            reason,
            e,
        )

    # Daily prop outcome accounting is handled inside _release_prop_open_risk().
    # Do not update wins_today/losses_today/daily_r here, otherwise the same
    # closed trade is counted twice.

        

    # -------------------------------------------------
    # Canonical zone cooldown after a real broker close.
    #
    # Applies equally to:
    #   - normal WATCH trades
    #   - BROKER_REPAIR trades
    #   - restart-recovered trades
    #
    # The broker position ticket and broker deal prove that
    # this was a real trade. Zone metadata is not required.
    # -------------------------------------------------
    try:
        sym = str(
            pos.get("symbol")
            or closed.get("symbol")
            or ""
        ).upper().strip()

        side0 = str(
            pos.get("side")
            or closed.get("side")
            or ""
        ).upper().strip()

        reason_u = str(reason or "").upper().strip()

        try:
            ticket0 = int(
                pos.get("mt5_ticket")
                or pos.get("broker_ticket")
                or pos.get("position_ticket")
                or closed.get("mt5_ticket")
                or closed.get("broker_ticket")
                or closed.get("position_ticket")
                or 0
            )
        except Exception:
            ticket0 = 0

        broker_deal_ok = bool(
            isinstance(meta, dict)
            and isinstance(meta.get("broker_deal"), dict)
            and meta["broker_deal"].get("ok")
        )

        trade_state0 = str(
            pos.get("trade_state")
            or closed.get("trade_state")
            or ""
        ).upper().strip()

        has_real_trade = bool(
            ticket0 > 0
            or broker_deal_ok
            or trade_state0 == "TRADE_ACTIVE"
        )

        cooldown_reasons = {
            "BROKER_CLOSED",
            "HIT",
            "SL_HIT",
            "MANUAL",
            "MANUAL_CLOSE",
            "TP",
            "SL",
            "STOP_OUT",
            "TRAILING_STOP",
        }

        broker_deal = (
            meta.get("broker_deal")
            if isinstance(meta, dict)
            and isinstance(meta.get("broker_deal"), dict)
            else {}
        )

        close_observed_ms = int(
            broker_deal.get("stored_at_ms")
            or broker_deal.get("created_at_ms")
            or now_ms()
        )

        broker_close_ms = int(
            closed.get("broker_close_time_ms")
            or 0
        )

        current_ms = now_ms()
        elapsed_sec = max(
            0,
            int((current_ms - close_observed_ms) / 1000),
        )

        configured_ttl_sec = 2 * 60 * 60
        remaining_ttl_sec = max(
            0,
            configured_ttl_sec - elapsed_sec,
        )
        log.warning(
                "[OPPT] ZONE_COOLDOWN_EVAL "
                "uid=%s sym=%s side=%s ticket=%s "
                "reason=%s state=%s real_trade=%s "
                "broker_deal_ok=%s close_observed_ms=%s "
                "broker_close_ms=%s elapsed_sec=%s ttl_sec=%s",
                uid,
                sym,
                side0,
                ticket0,
                reason_u,
                trade_state0,
                has_real_trade,
                broker_deal_ok,
                close_observed_ms,
                broker_close_ms,
                elapsed_sec,
                remaining_ttl_sec,
        )

        

        if (
            sym
            and side0 in ("BUY", "SELL")
            and has_real_trade
            and reason_u in cooldown_reasons
            and remaining_ttl_sec > 0
        ):
            cooldown_payload = {
                "symbol": sym,
                "side": side0,
                "tf": "H1",
                "ticket": ticket0,
                "reason": reason_u,
                "closed_at_ms": close_observed_ms,
                "broker_close_time_ms": broker_close_ms,
                "trade_id": str(pos.get("trade_id") or ""),
                "trade_source": str(pos.get("source") or ""),
                "broker_deal_ok": broker_deal_ok,
                "zone_missing": not bool(
                    pos.get("entry_zone")
                    or pos.get("entry_zone_low")
                    or pos.get("entry_zone_high")
                ),
                "elapsed_sec": elapsed_sec,
                "ttl_sec": remaining_ttl_sec,
            }

            R.setex(
                _zone_cooldown_key(sym, side0, "H1"),
                remaining_ttl_sec,
                json.dumps(
                    cooldown_payload,
                    separators=(",", ":"),
                ),
            )

            log.warning(
                "[ZONE_COOLDOWN_SET] "
                "trade_id=%s sym=%s side=%s ticket=%s "
                "reason=%s source=%s ttl_sec=%s",
                pos.get("trade_id"),
                sym,
                side0,
                ticket0,
                reason_u,
                pos.get("source"),
                remaining_ttl_sec,
            )
        else:
            log.warning(
                "[ZONE_COOLDOWN_SKIP] "
                "trade_id=%s sym=%s side=%s ticket=%s "
                "reason=%s state=%s real_trade=%s "
                "broker_deal_ok=%s ttl_sec=%s",
                pos.get("trade_id"),
                sym,
                side0,
                ticket0,
                reason_u,
                trade_state0,
                has_real_trade,
                broker_deal_ok,
                remaining_ttl_sec,
            )

    except Exception as cooldown_exc:
        log.exception(
            "[OPPT] ZONE_COOLDOWN_FAILED "
            "uid=%s trade_id=%s sym=%s reason=%s err=%r",
            uid,
            pos.get("trade_id"),
            pos.get("symbol"),
            reason,
            cooldown_exc,
        )

    # -------------------------------------------------
    # P0: same-symbol re-entry cooldown after close.
    # Blocks any new trade on the same symbol for 2 hours.
    # This is symbol-level, not side-level.
    # -------------------------------------------------
    try:
        sym_cd = str(pos.get("symbol") or closed.get("symbol") or "").upper().strip()
        reason_u = str(reason or "").upper().strip()
        _trade_state = str(pos.get("trade_state") or "").upper().strip()

        _has_real_trade = bool(
            pos.get("mt5_ticket")
            or pos.get("broker_ticket")
            or pos.get("position_ticket")
            or _trade_state == "TRADE_ACTIVE"
        )

        _cooldown_reasons = {
            "BROKER_CLOSED",
            "HIT",
            "SL_HIT",
            "MANUAL_CLOSE",
            "TP",
            "SL",
            "TRAILING_STOP",
        }
        

        if sym_cd and _has_real_trade and reason_u in _cooldown_reasons:
            _now_ms = now_ms()
            _broker_deal = (
                meta.get("broker_deal")
                if isinstance(meta, dict)
                and isinstance(meta.get("broker_deal"), dict)
                else {}
            )

            _closed_at_ms = int(
                _broker_deal.get("stored_at_ms")
                or _broker_deal.get("created_at_ms")
                or _now_ms
            )
            _elapsed_sec = max(0, int((_now_ms - _closed_at_ms) / 1000))
            _ttl_sec = max(0, (2 * 60 * 60) - _elapsed_sec)

            if _ttl_sec > 0:
                R.setex(
                    f"xtl:cooldown:symbol:{uid}:{sym_cd}",
                    _ttl_sec,
                    json.dumps({
                        "symbol": sym_cd,
                        "reason": str(reason),
                        "closed_at_ms": _closed_at_ms,
                        "trade_id": str(pos.get("trade_id") or ""),
                        "mt5_ticket": int(pos.get("mt5_ticket") or 0),
                        "elapsed_sec": _elapsed_sec,
                        "ttl_sec": _ttl_sec,
                    }),
                )
                log.warning(
                    "[OPPT] SAME_SYMBOL_COOLDOWN_SET uid=%s sym=%s ttl_sec=%s elapsed_sec=%s reason=%s trade_id=%s",
                    uid, sym_cd, _ttl_sec, _elapsed_sec, reason_u, pos.get("trade_id"),
                )
            else:
                log.warning(
                    "[OPPT] SAME_SYMBOL_COOLDOWN_SKIP_EXPIRED uid=%s sym=%s elapsed_sec=%s reason=%s trade_id=%s",
                    uid, sym_cd, _elapsed_sec, reason_u, pos.get("trade_id"),
                )
    except Exception as symbol_cooldown_exc:
        log.exception(
            "[OPPT] SAME_SYMBOL_COOLDOWN_FAILED "
            "uid=%s trade_id=%s sym=%s reason=%s err=%r",
            uid,
            pos.get("trade_id"),
            pos.get("symbol"),
            reason,
            symbol_cooldown_exc,
        )

    try:
        closed["final_broker_sl"] = closed.get("broker_current_sl")
        closed["final_broker_tp"] = closed.get("broker_current_tp")
        closed["sl_changed_from_original"] = bool(closed.get("sl_changed_from_original"))
        closed["tp_changed_from_original"] = bool(closed.get("tp_changed_from_original"))
    except Exception:
        pass

    if meta and isinstance(meta, dict):
        closed["exit_meta"] = meta

    

    R.lpush(CLOSED_KEY.format(uid=uid), json.dumps(closed))
    try:
        closed_ticket = int(
            pos.get("mt5_ticket")
            or pos.get("broker_ticket")
            or pos.get("position_ticket")
            or closed.get("mt5_ticket")
            or 0
        )

        if closed_ticket > 0:
            _mark_ticket_closed(
                closed_ticket,
                str(pos.get("trade_id") or ""),
                str(reason or ""),
            )

            log.warning(
                "[OPPT] CLOSED_TICKET_MARKED "
                "uid=%s ticket=%s trade_id=%s reason=%s",
                uid,
                closed_ticket,
                pos.get("trade_id"),
                reason,
            )
    except Exception as marker_exc:
        log.exception(
            "[OPPT] CLOSED_TICKET_MARK_FAILED "
            "uid=%s trade_id=%s err=%r",
            uid,
            pos.get("trade_id"),
            marker_exc,
        )
    try:
        R.ltrim(CLOSED_KEY.format(uid=uid), 0, 499)
    except Exception:
        pass

    _remove_open_trade(uid, str(pos.get("trade_id") or ""))
    _clear_trade_lifecycle_keys(pos)
    _clear_exec_claim_for_trade(
        r=R,
        uid=uid,
        trade=closed,
        reason=reason,
    )

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
    if exec_mode == "mt5":
        # Resolve the active prop profile inside _tick_prop_watchdog().
        # Missing active-profile state safely falls back to ftmo-main.
        _tick_prop_watchdog(
            uid=uid,
            profile_id=None,
            mt5_account=mt5_account,
        )


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
    # 0a) BROKER-TRUTH SYMBOL GUARD
    # If MT5 already has an active XTL position, keep watch state aligned
    # even if Redis open ledger is missing/delayed. This prevents opposite
    # REV_OK setups while a broker position is live.
    # -------------------------------------------------
    try:
        if exec_mode == "mt5":
            for _bp_active in _broker_xtl_positions(mt5_account):
                _sync_watches_for_broker_active_position(_bp_active, "BROKER_ACTIVE_PRE_ACK")
    except Exception as e:
        log.warning("[WATCHLIST] BROKER_ACTIVE_PRE_ACK_GUARD_FAILED uid=%s err=%r", uid, e)

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
                    wk = _zone_watch_key(
                       uid,
                       sym0,
                       side0,
                       "H1",
                    )
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
                # ── analytics: capture AFTER ticket + real fill are on pos ──
                try:
                    from api.xtl_analytics import capture_entry
                    capture_entry(pos, capture_source="normal")
                except Exception as _ax:
                    log.warning("analytics entry-snap skipped: %s", _ax)

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

                _ack_result = (
                    ack.get("result")
                    if isinstance(ack.get("result"), dict)
                    else {}
                )

                _ack_error = str(
                    ack.get("error")
                    or _ack_result.get("error")
                    or ""
                ).strip()

                _ack_comment = str(
                    _ack_result.get("comment")
                    or ack.get("comment")
                    or ""
                ).strip()

                pos["mt5_error"] = _ack_error

                _ack_text = (
                    f"{_ack_error} {_ack_comment}"
                ).lower()

                # Broker/infrastructure failure:
                # preserve the same frozen zone and RC.
                _recoverable_broker_failure = bool(
                    "10026" in _ack_text
                    or "autotrading disabled" in _ack_text
                )

                _tid_fail = str(
                    pos.get("trade_id") or ""
                ).strip()

                _sym_fail = str(
                    pos.get("symbol") or ""
                ).upper().strip()

                _side_fail = str(
                    pos.get("side") or ""
                ).upper().strip()

                _watch_key_fail = _zone_watch_key(
                    uid,
                    _sym_fail,
                    _side_fail,
                    "H1",
                )

                # Save the complete watch before _close_trade().
                # _close_trade may execute normal failed-entry cleanup.
                _saved_watch = {}

                if _recoverable_broker_failure:
                    try:
                        _saved_watch_raw = R.get(
                            _watch_key_fail
                        )

                        _saved_watch = (
                            _sj(_saved_watch_raw, {})
                            if _saved_watch_raw
                            else {}
                        )

                        if not isinstance(
                            _saved_watch,
                            dict,
                        ):
                            _saved_watch = {}

                    except Exception:
                        _saved_watch = {}

                # Close only the failed order attempt:
                # release risk, record ENTRY_FAIL and remove open ledger.
                try:
                    _close_trade(
                        uid,
                        pos,
                        float(
                            pos.get("entry_price") or 0.0
                        ),
                        "ENTRY_FAIL",
                        meta={"mt5_ack": ack},
                    )
                finally:
                    _remove_open_trade(
                        uid,
                        _tid_fail,
                    )

                if _recoverable_broker_failure:
                    # Release the executor idempotency claim so a
                    # controlled retry can execute again.
                    try:
                        if _tid_fail:
                            R.delete(
                                f"xtl:oppt:exec_claim:"
                                f"{uid}:{_tid_fail}"
                            )
                    except Exception:
                        pass

                    # Release the short watch entry claim.
                    try:
                        for _claim_key in R.scan_iter(
                            f"xtl:watch:entry_claim:"
                            f"{_sym_fail}:{_side_fail}:H1:*",
                            count=50,
                        ):
                            R.delete(_claim_key)
                    except Exception:
                        pass

                    # Restore the exact same watch, frozen zone and RC.
                    try:
                        if _saved_watch:
                            # Exponential broker retry backoff:
                            # failure 1 -> 30 seconds
                            # failure 2 -> 60 seconds
                            # failure 3 -> 120 seconds
                            # failure 4 -> 300 seconds
                            # failure 5+ -> 600 seconds
                            try:
                                _broker_retry_count = int(
                                    _saved_watch.get(
                                        "broker_retry_count"
                                    )
                                    or 0
                                ) + 1
                            except Exception:
                                _broker_retry_count = 1

                            _broker_retry_delays_ms = {
                                1: 30_000,
                                2: 60_000,
                                3: 120_000,
                                4: 300_000,
                            }

                            _broker_retry_delay_ms = int(
                                _broker_retry_delays_ms.get(
                                    _broker_retry_count,
                                    600_000,
                                )
                            )

                            _retry_ms = (
                                now_ms()
                                + _broker_retry_delay_ms
                            )

                            _saved_watch["state"] = (
                                "ENTRY_BLOCKED_BROKER"
                            )
                            _saved_watch["trade_state"] = (
                                "ENTRY_BLOCKED_BROKER"
                            )

                            _saved_watch[
                                "entry_triggered"
                            ] = False

                            _saved_watch[
                                "entry_blocked"
                            ] = True

                            _saved_watch[
                                "entry_block_reason"
                            ] = (
                                "BROKER_AUTOTRADING_DISABLED"
                            )

                            _saved_watch[
                                "broker_error"
                            ] = _ack_error

                            _saved_watch[
                                "broker_comment"
                            ] = _ack_comment

                            _saved_watch[
                                "broker_error_ms"
                            ] = now_ms()

                            _saved_watch[
                                "next_retry_ms"
                            ] = _retry_ms

                            _saved_watch[
                                "broker_retry_count"
                            ] = int(
                                _broker_retry_count
                            )

                            _saved_watch[
                                "broker_retry_delay_ms"
                            ] = int(
                                _broker_retry_delay_ms
                            )

                            _saved_watch[
                                "broker_last_retry_ms"
                            ] = now_ms()

                            # Remove only fields from the failed order.
                            # Do not remove zone_used or RC fields.
                            _saved_watch.pop(
                                "mt5_job_id",
                                None,
                            )
                            _saved_watch.pop(
                                "mt5_ticket",
                                None,
                            )
                            _saved_watch.pop(
                                "mt5_fill_price",
                                None,
                            )
                            _saved_watch.pop(
                                "device_id",
                                None,
                            )
                            _saved_watch.pop(
                                "entry_price",
                                None,
                            )
                            _saved_watch.pop(
                                "entry_ts_ms",
                                None,
                            )

                            R.set(
                                _watch_key_fail,
                                json.dumps(
                                    _saved_watch,
                                    separators=(",", ":"),
                                ),
                                ex=7 * 24 * 3600,
                            )

                            log.warning(
                                "[WATCHLIST] "
                                "ENTRY_BLOCKED_BROKER "
                                "sym=%s side=%s tid=%s "
                                "error=%r comment=%r "
                                "retry_count=%s "
                                "retry_delay_ms=%s "
                                "next_retry_ms=%s key=%s",
                                _sym_fail,
                                _side_fail,
                                _tid_fail,
                                _ack_error,
                                _ack_comment,
                                _broker_retry_count,
                                _broker_retry_delay_ms,
                                _retry_ms,
                                _watch_key_fail,
                            )

                        else:
                            log.error(
                                "[WATCHLIST] "
                                "ENTRY_BLOCKED_BROKER_"
                                "RESTORE_FAILED "
                                "sym=%s side=%s tid=%s "
                                "reason=watch_snapshot_missing",
                                _sym_fail,
                                _side_fail,
                                _tid_fail,
                            )

                    except Exception as _watch_err:
                        log.exception(
                            "[WATCHLIST] "
                            "ENTRY_BLOCKED_BROKER_"
                            "RESTORE_EXC "
                            "sym=%s side=%s tid=%s "
                            "err=%r",
                            _sym_fail,
                            _side_fail,
                            _tid_fail,
                            _watch_err,
                        )
    except Exception:
        pass

    # -------------------------------------------------
    # 0a) STALE MARKET ORDER_PENDING WATCHDOG
    #
    # Run only after normal ACK reconciliation has had
    # the first chance to process success/failure ACKs.
    #
    # LIMIT/STOP broker pending orders are excluded inside
    # _reconcile_stale_order_pending().
    # -------------------------------------------------
    try:
        # Refresh because ACK reconciliation may have updated,
        # closed, or removed open trades.
        open_trades = _list_open_trades(uid)

        _reconcile_stale_order_pending(
            uid=uid,
            open_trades=open_trades,
            mt5_account=mt5_account,
        )

        # Refresh again because the watchdog may have moved
        # stale MARKET rows from open -> closed.
        open_trades = _list_open_trades(uid)

    except Exception as e:
        log.exception(
            "[OPPT] STALE_ORDER_PENDING_RECON_FAILED "
            "uid=%s err=%r",
            uid,
            e,
        )

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
                    
                
                broker_pos = None

                for p in _sj(raw, []):
                    if isinstance(p, dict) and p.get("ticket") is not None:
                        try:
                            _pt = int(p["ticket"])
                            open_tickets.add(_pt)
                            if _pt == int(ticket):
                                broker_pos = p
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
                        if broker_pos:
                            _sync_open_trade_broker_sl_tp(uid, broker_pos)
                    except Exception as e:
                        log.warning(
                            "[OPPT] BROKER_SLTP_SYNC_INLINE_FAILED uid=%s ticket=%s err=%r",
                            uid,
                            ticket,
                            e,
                        )
                    try:
                        prop_cfg, prop_cfg_ok, prop_cfg_err = _get_prop_config_safe()
                        if not prop_cfg_ok:
                            log.error(
                                "[PROP] PROP_CFG_READ_FAIL_BROKER_RECON uid=%s sym=%s ticket=%s err=%s",
                                uid,
                                pos.get("symbol"),
                                ticket,
                                prop_cfg_err,
                            )
                            prop_cfg = {
                                "enabled": False,
                                "_read_ok": False,
                                "_read_error": prop_cfg_err,
                            }

                        prop_profile_id = "ftmo-main"
                        try:
                            prop_profile_id = str(
                                prop_cfg.get("profile_id")
                                or prop_cfg.get("account_id")
                                or prop_profile_id
                            )
                        except Exception:
                            pass
                        tid0 = str(pos.get("trade_id") or "").strip()

                        if bool(prop_cfg.get("enabled")) and tid0:
                            risk_state0 = _get_prop_risk_state(prop_profile_id)
                            
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
                                _repair_prop_check = _make_repair_prop_check(
                                    prop_cfg=prop_cfg,
                                    symbol=sym0,
                                    side=side0,
                                    entry=float(entry0 or 0.0),
                                    sl=float(sl0 or 0.0),
                                    tp=float(pos.get("tp_price") or 0.0),
                                    lots=float(lots0 or 0.0),
                                    risk_usd=float(risk_usd0 or 0.0),
                                )

                                if risk_usd0 > 0:
                                    _reserve_prop_open_risk(
                                        tid0,
                                        {
                                            "trade_id": tid0,
                                            "symbol": sym0,
                                            "side": side0,

                                            "risk_usd": float(risk_usd0),
                                            "risk_pct": float(_repair_prop_check["risk_pct"]),

                                            "lots": float(lots0),

                                            "entry": float(entry0),
                                            "sl": float(sl0),
                                            "tp": float(pos.get("tp_price") or 0),

                                            "planned_rr": float(_repair_prop_check["planned_rr"]),
                                            "target_rr": float(_repair_prop_check["target_rr"]),

                                            "prop_check": _repair_prop_check,
                                            "firm": prop_cfg.get("firm"),
                                            "phase": prop_cfg.get("phase"),
                                            "source": "broker_recon_missing_reserve",
                                            "mt5_ticket": ticket,
                                            "device_id": str(pos.get("device_id") or ""),
                                            "reserved_ts_ms": now_ms(),
                                            "profile_id": prop_profile_id,
                                        },
                                        profile_id=prop_profile_id
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
                    try:
                        _all_live = _broker_xtl_positions(mt5_account)
                    except Exception:
                        _all_live = []

                    _same_sym_live = [
                        p for p in (_all_live or [])
                        if str(p.get("symbol") or "").upper().strip()
                        == str(pos.get("symbol") or "").upper().strip()
                    ]
                    log.warning(
                        "[OPPT] BROKER_RECON_SAME_SYM_LIVE_CHECK uid=%s sym=%s ticket=%s same_sym_live=%s all_live=%s",
                        uid,
                        pos.get("symbol"),
                        ticket,
                        [p.get("ticket") for p in _same_sym_live],
                        [(p.get("symbol"), p.get("ticket"), p.get("snapshot_key")) for p in (_all_live or [])],
                    )

                    if _same_sym_live:
                        log.warning(
                            "[OPPT] SKIP_BROKER_CLOSED broker_still_active uid=%s sym=%s ticket=%s live_tickets=%s",
                            uid,
                            pos.get("symbol"),
                            ticket,
                            [p.get("ticket") for p in _same_sym_live],
                        )
                        continue

                   
                    broker_deal = _load_mt5_deal(ticket)

                    log.warning(
                        "[OPPT] BROKER_DEAL_LOAD uid=%s sym=%s ticket=%s deal_ok=%s deal_keys=%s",
                        uid,
                        pos.get("symbol"),
                        ticket,
                        broker_deal.get("ok") if isinstance(broker_deal, dict) else None,
                        list(broker_deal.keys()) if isinstance(broker_deal, dict) else type(broker_deal).__name__,
                    )

                    if not bool(broker_deal.get("ok")):
                        log.warning(
                            "[OPPT] SKIP_BROKER_CLOSED_NO_DEAL uid=%s sym=%s ticket=%s key=%s broker_tickets=%s",
                            uid,
                            pos.get("symbol"),
                            ticket,
                            key,
                            list(open_tickets),
                        )
                        continue

                    try:
                        lp, close_reason, broker_meta = _apply_broker_deal_to_closed(
                            pos,
                            broker_deal,
                            "BROKER_CLOSED",
                        )

                        broker_meta.update({
                            "broker_snapshot_key": key,
                            "broker_open_tickets": list(open_tickets),
                            "local_status": pos_status,
                        })

                        log.warning(
                            "[OPPT] BROKER_CLOSED uid=%s sym=%s ticket=%s status=%s reason=%s broker_tickets=%s",
                            uid,
                            pos.get("symbol"),
                            ticket,
                            pos_status,
                            close_reason,
                            list(open_tickets),
                        )

                        _close_trade(
                            uid,
                            pos,
                            float(lp),
                            close_reason,
                            meta=broker_meta,
                        )

                    except Exception:
                        log.exception(
                            "[OPPT] BROKER_CLOSED_APPLY_FAILED uid=%s sym=%s ticket=%s",
                            uid,
                            pos.get("symbol"),
                            ticket,
                        )

                    continue
                    
    except Exception:
        pass
    # -------------------------------------------------
    # 0c) MT5 ORPHAN POSITION REPAIR
    # Broker has position but Redis open registry is missing.
    # This repairs TRADE_ACTIVE gate after Redis cleanup/restart.
    # -------------------------------------------------
    try:
        if exec_mode == "mt5":
            try:
                repair_cfg, repair_cfg_ok, repair_cfg_err = _get_prop_config_safe()

                if not repair_cfg_ok or not isinstance(repair_cfg, dict):
                    log.warning(
                        "[OPPT] SKIP_BROKER_REPAIR_PROP_CFG_FAIL "
                        "uid=%s err=%s",
                        uid,
                        repair_cfg_err,
                    )
                    return

                repair_profile_id = str(
                    repair_cfg.get("profile_id")
                    or repair_cfg.get("account_id")
                    or ""
                ).strip()

                if not repair_profile_id:
                    log.warning(
                        "[OPPT] SKIP_BROKER_REPAIR_PROFILE_MISSING uid=%s",
                        uid,
                    )
                    return

                risk_state = _get_prop_risk_state(repair_profile_id)
                if not bool(risk_state.get("snapshot_valid", False)):
                    log.warning(
                        "[OPPT] SKIP_BROKER_REPAIR_STALE_SNAPSHOT uid=%s age_ms=%s",
                        uid,
                        risk_state.get("snapshot_age_ms"),
                    )
                    return
            except Exception as e:
                log.warning(
                    "[OPPT] SKIP_BROKER_REPAIR_RISK_STATE_EXC uid=%s err=%r",
                    uid,
                    e,
                )
                return

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
                    if _is_ticket_recently_closed(ticket):
                        broker_deal = _load_mt5_deal(ticket)

                        # True broker close confirmed.
                        if bool(broker_deal.get("ok")):
                            log.warning(
                                "[OPPT] SKIP_BROKER_REPAIR confirmed_closed "
                                "uid=%s ticket=%s",
                                uid,
                                ticket,
                            )
                            continue

                        # Broker still has the position and no broker deal.
                        # Earlier BROKER_CLOSED was probably false.
                        log.warning(
                            "[OPPT] FALSE_BROKER_CLOSE_CANDIDATE "
                            "uid=%s ticket=%s sym=%s",
                            uid,
                            ticket,
                            sym,
                        )

                        try:
                            R.delete(_closed_ticket_key(ticket))
                        except Exception:
                            pass

                        

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
                    tp0 = _sf(bp.get("tp"), 0.0)
                    sl0 = _sf(bp.get("sl"), 0.0)

                    _entry_zone = None
                    _ez_from_ticket = {}

                    # 1) PREFERRED: durable ticket->zone map written at placement.
                    try:
                        _zr = R.get(f"xtl:trade:zone_by_ticket:{ticket}")
                        if _zr:
                            _ez_from_ticket = _sj(_zr, {}) or {}
                            _zz = _ez_from_ticket.get("entry_zone")
                            if isinstance(_zz, dict):
                                _entry_zone = _zz
                    except Exception:
                        _entry_zone = None

                    # 2) FALLBACK: watch zone_used
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
                    entry_zone_source = ""
                    entry_zone_selection_model = ""
                    zone_src_code = ""
                    zone_src_for_comment = ""

                    try:
                        if isinstance(_entry_zone, dict):
                            entry_zone_source = str(_entry_zone.get("zone_source") or "")
                            entry_zone_selection_model = str(_entry_zone.get("selection_model") or "")
                            zone_src_for_comment = (
                                entry_zone_source
                                or str(_entry_zone.get("source") or "")
                                or entry_zone_selection_model
                                or str(_entry_zone.get("entry_zone_source") or "")
                            )
                            zone_src_code = _zone_src_code(zone_src_for_comment)
                    except Exception:
                        entry_zone_source = ""
                        entry_zone_selection_model = ""
                        zone_src_code = ""
                        zone_src_for_comment = ""

                    # -------------------------------------------------
                    # Prop metadata (must exist BEFORE repaired record)
                    # -------------------------------------------------
                    prop_cfg, prop_cfg_ok, prop_cfg_err = _get_prop_config_safe()

                    if not prop_cfg_ok:
                        log.error(
                            "[PROP] PROP_CFG_READ_FAIL_BROKER_REPAIR uid=%s sym=%s err=%s",
                            uid,
                            sym,
                            prop_cfg_err,
                        )
                        prop_cfg = {
                            "enabled": False,
                            "_read_ok": False,
                            "_read_error": prop_cfg_err,
                        }
                    prop_profile_id = "ftmo-main"
                    try:
                        prop_profile_id = str(
                            prop_cfg.get("profile_id")
                            or prop_cfg.get("account_id")
                            or prop_profile_id
                        )
                    except Exception:
                        pass

                    risk_usd = 0.0
                    if bool(prop_cfg.get("enabled")):
                        risk_usd = _risk_usd_from_broker_position(
                            sym,
                            entry_px,
                            sl0,
                            qty0,
                        )

                    _repair_prop_check = _make_repair_prop_check(
                        prop_cfg=prop_cfg,
                        symbol=sym,
                        side=side,
                        entry=float(entry_px or 0.0),
                        sl=float(sl0 or 0.0),
                        tp=float(tp0 or 0.0),
                        lots=float(qty0 or 0.0),
                        risk_usd=float(risk_usd or 0.0),
                    )

                    try:
                        _planned_rr = float(_repair_prop_check.get("planned_rr") or 0.0)
                        _min_rr = 1.90

                        if _planned_rr < _min_rr:
                            log.warning(
                                "[OPPT] SKIP_BROKER_REPAIR_LOW_RR uid=%s sym=%s side=%s ticket=%s planned_rr=%.4f min_rr=%.2f entry=%s sl=%s tp=%s",
                                uid, sym, side, ticket, _planned_rr, _min_rr, entry_px, sl0, tp0,
                            )
                            continue
                    except Exception as e:
                        log.warning(
                            "[OPPT] SKIP_BROKER_REPAIR_RR_CHECK_EXC uid=%s sym=%s side=%s ticket=%s err=%r",
                            uid, sym, side, ticket, e,
                        )
                        continue

                    

                    repaired = {
                        "trade_id": f"BROKER_REPAIR:{sym}:{side}:{ticket}",
                        "symbol": sym,
                        "side": side,

                        "entry_price": float(entry_px),
                        "qty": float(qty0),

                        "tp_price": float(tp0) if tp0 > 0 else None,
                        "sl_price": float(sl0) if sl0 > 0 else None,

                        "opened_at_ms": int(
                            bp.get("time_msc")
                            or (
                                int(bp.get("time") or 0) * 1000
                                if bp.get("time")
                                else 0
                            )
                            or now_ms()
                        ),
                        "broker_open_time_ms": int(
                            bp.get("time_msc")
                            or (
                                int(bp.get("time") or 0) * 1000
                                if bp.get("time")
                                else 0
                            )
                            or 0
                        ),
                        "repair_detected_at_ms": now_ms(),

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
                        "entry_zone_source": entry_zone_source,
                        "entry_zone_selection_model": entry_zone_selection_model,
                        "zone_src_code": zone_src_code,
                        "zone_src_for_comment": str(zone_src_for_comment or ""),

                        "repair_source": "broker_snapshot",

                        # -------- Prop metadata --------
                        "risk_usd": float(risk_usd),
                        "risk_pct": float(_repair_prop_check.get("risk_pct") or 0.0),
                        "planned_rr": float(_repair_prop_check.get("planned_rr") or 0.0),
                        "target_rr": float(_repair_prop_check.get("target_rr") or 0.0),
                        "profile_id": prop_profile_id,
                        "prop_check": _repair_prop_check,
                    }

                    _open_trade(uid, repaired)
                    try:
                        from api.xtl_analytics import capture_entry
                        capture_entry(repaired, capture_source="broker_repair")
                    except Exception as _ax:
                        log.warning("analytics repair-snap skipped: %s", _ax)
                    known_tickets.add(ticket)
                    try:
                        

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
                                        "risk_pct": float(_repair_prop_check["risk_pct"]),
                                        "lots": float(qty0),
                                        "entry": float(entry_px),
                                        "sl": float(repaired.get("sl_price") or 0),
                                        "tp": float(repaired.get("tp_price") or 0),
                                        "target_rr": float(_repair_prop_check.get("target_rr") or 0.0),
                                        "planned_rr": float(_repair_prop_check.get("planned_rr") or 0.0),
                                        "prop_check": _repair_prop_check,
                                        "firm": prop_cfg.get("firm"),
                                        "phase": prop_cfg.get("phase"),
                                        "source": "broker_repair",
                                        "mt5_ticket": ticket,
                                        "device_id": dev_from_key,
                                        "reserved_ts_ms": now_ms(),
                                        "profile_id": prop_profile_id,
                                    },
                                    profile_id=prop_profile_id
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

                # Broker-truth guard: while any active XTL broker position exists
                # for this symbol, no watch may progress to REV_OK/ENTRY_READY.
                # This blocks both same-side duplicate and opposite-side reversal
                # setups even when Redis open ledger is missing or delayed.
                try:
                    _bp_active = _broker_has_active_xtl_symbol(sym_w, mt5_account) if exec_mode == "mt5" else None
                    if _bp_active:
                        _sync_watches_for_broker_active_position(_bp_active, "BROKER_ACTIVE_WATCH_SCAN")
                        continue
                except Exception as e:
                    log.warning(
                        "[WATCHLIST] BROKER_ACTIVE_WATCH_SCAN_FAILED sym=%s side=%s err=%r",
                        sym_w, side_w, e,
                    )

                state_w = str(watch.get("state") or "").upper().strip()
                # -------------------------------------------------
                # SELF-HEAL: stale ORDER_PENDING without MT5 job
                # This means old code marked pending before enqueue.
                # Do not delete the zone. Re-arm same RC/zone for execution.
                # -------------------------------------------------

                if state_w == "ORDER_PENDING":
                    _watch_order_type = _normalize_order_type(
                        watch
                    )

                    # Broker-side LIMIT/STOP orders may remain pending
                    # for hours or days. Never apply MARKET timeout logic.
                    if _watch_order_type != "MARKET":
                        continue

                    job_id = str(
                        watch.get("mt5_job_id")
                        or ""
                    ).strip()
                    ticket = str(watch.get("mt5_ticket") or "").strip()
                    entry_ts = _si(watch.get("entry_ts_ms"), 0)
                    pending_age_ms = (now_ms() - entry_ts) if entry_ts > 0 else 0

                    # Market orders should not remain ORDER_PENDING forever.
                    # If no broker ticket appears within 5 minutes, mark as failed.
                    _watch_deadline_ms = int(
                        watch.get("ack_deadline_ms")
                        or (
                            entry_ts + MARKET_ACK_TIMEOUT_MS
                            if entry_ts > 0
                            else 0
                        )
                    )

                    stale_pending_timeout = (
                        _watch_deadline_ms > 0
                        and now_ms() >= _watch_deadline_ms
                        and not ticket
                    )

                    if stale_pending_timeout:
                        watch["state"] = "ORDER_FAILED"
                        watch["trade_state"] = "ORDER_FAILED"
                        watch["status"] = "expired"
                        watch["exit_reason"] = "MARKET_ACK_TIMEOUT"
                        watch["order_failure_reason"] = (
                            "NO_ACK_NO_BROKER_POSITION"
                        )
                        watch["order_type"] = "MARKET"
                        watch["closed_at_ms"] = now_ms()
                        watch["pending_age_ms"] = int(pending_age_ms)
                        watch["cleanup_source"] = "oppt_executor_watch_recon"

                        R.set(str(wkey), json.dumps(watch), ex=7 * 24 * 3600)

                        log.warning(
                            "[WATCHLIST] MARKET_ORDER_PENDING_TIMEOUT sym=%s side=%s job_id=%s age_ms=%s key=%s",
                            sym_w, side_w, job_id, pending_age_ms, wkey
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
                    prop_cfg_px, prop_cfg_px_ok, prop_cfg_px_err = _get_prop_config_safe()

                    dev_for_px = ""

                    if prop_cfg_px_ok and isinstance(prop_cfg_px, dict):
                        profile_id_px = str(
                            prop_cfg_px.get("profile_id")
                            or prop_cfg_px.get("account_id")
                            or ""
                        ).strip()

                        if profile_id_px:
                            try:
                                resolved_px = _resolve_prop_profile_device(profile_id_px)

                                if isinstance(resolved_px, dict) and resolved_px.get("ok"):
                                    dev_for_px = str(
                                        resolved_px.get("device_id") or ""
                                    ).strip()
                                else:
                                    log.warning(
                                        "[PROP] LIVE_PRICE_PROFILE_RESOLVE_FAILED "
                                        "uid=%s profile=%s sym=%s reason=%s",
                                        uid,
                                        profile_id_px,
                                        sym_w,
                                        (
                                           resolved_px.get("reason")
                                           if isinstance(resolved_px, dict)
                                           else "BAD_RESOLVE_PAYLOAD"
                                        ),
                                    )

                            except Exception as e:
                                log.warning(
                                    "[PROP] LIVE_PRICE_PROFILE_RESOLVE_EXC "
                                    "uid=%s profile=%s sym=%s err=%r",
                                    uid,
                                    profile_id_px,
                                    sym_w,
                                    e,
                                )
                    else:
                        log.warning(
                            "[PROP] LIVE_PRICE_CFG_READ_FAIL "
                            "uid=%s sym=%s err=%s",
                            uid,
                            sym_w,
                            prop_cfg_px_err,
                        )
                    if dev_for_px:
                        pk = f"xtl:price:{dev_for_px}:{sym_w}"
                        pr = _sj(R.get(pk), {})
                        if isinstance(pr, dict):
                            px0 = _sf(pr.get("price"), 0.0)
                            ts0 = _si(pr.get("ts_ms"), 0)

                            # reject stale price older than 2 minutes
                            if px0 > 0 and ts0 > 0 and (now_ms() - ts0) <= 120000:
                                live_px = px0

                    
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
                        watch.pop(
                            "broker_retry_count",
                            None,
                        )
                        watch.pop(
                            "broker_retry_delay_ms",
                            None,
                        )
                        watch.pop(
                            "broker_last_retry_ms",
                            None,
                        )
                        watch.pop(
                            "broker_error",
                            None,
                        )
                        watch.pop(
                            "broker_comment",
                            None,
                        )
                        watch.pop(
                            "broker_error_ms",
                            None,
                        )
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
        exit_profile_id = str(
            pos.get("profile_id")
            or pos.get("prop_profile_id")
            or ""
        ).strip()

        if not exit_profile_id:
            try:
                exit_cfg, exit_cfg_ok, _ = _get_prop_config_safe()
                if exit_cfg_ok and isinstance(exit_cfg, dict):
                    exit_profile_id = str(
                        exit_cfg.get("profile_id")
                        or exit_cfg.get("account_id")
                        or ""
                    ).strip()
            except Exception:
                exit_profile_id = ""
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
                            profile_id=exit_profile_id,
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
                            profile_id=exit_profile_id,
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
            try:
                if _broker_same_sym:
                    _bp0 = dict(_broker_same_sym[0])
                    _bp0.setdefault("symbol", sym)
                    _bp0.setdefault("side", str(_bp0.get("side") or "").upper().strip() or side)
                    _sync_watches_for_broker_active_position(_bp0, "BROKER_ACTIVE_ENTRY_SKIP")
            except Exception:
                pass
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
            try:
                from api.xtl_analytics import capture_rejection
                capture_rejection(ev, "score_lt_min", gate="OPPT", enrich=False)
            except Exception: pass
            continue
        # TEMP VALIDATION:
        # Disable confidence filter because zone-reversal ENTRY_CAND currently has conf=''.
        # We only want to validate REV_OK -> ENTRY_CAND -> MT5_ENQUEUE -> MT5 order.
        if False and min_conf_r > 0 and _conf_rank(conf) < min_conf_r:
            log.warning("[OPPT] SKIP_ENTRY conf_lt_min uid=%s sym=%s tid=%s conf=%r min_conf_r=%s", uid, sym, tid, conf, min_conf_r)
            try:
                from api.xtl_analytics import capture_rejection
                capture_rejection(ev, "conf_lt_min", gate="OPPT", enrich=False)
            except Exception: pass
            continue

        cd_key = COOLDOWN_KEY.format(uid=uid, symbol=sym)
        sym_cd_key = f"xtl:cooldown:symbol:{uid}:{sym}"

        try:
            if R.exists(cd_key) or R.exists(sym_cd_key):
                log.warning(
                    "[OPPT] SKIP_ENTRY cooldown uid=%s sym=%s tid=%s cd_key=%s sym_cd_key=%s",
                    uid, sym, tid, cd_key, sym_cd_key,
                )
                try:
                    from api.xtl_analytics import capture_rejection
                    capture_rejection(ev, "cooldown", gate="COOLDOWN", enrich=False)
                except Exception:
                    pass
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
        prop_cfg_pre, prop_cfg_pre_ok, prop_cfg_pre_err = _get_prop_config_safe()

        if not prop_cfg_pre_ok:
            log.error(
                "[PROP] BLOCK_ENTRY_PROP_CFG_READ_FAIL uid=%s sym=%s side=%s tid=%s err=%s",
                uid,
                sym,
                side,
                tid,
                prop_cfg_pre_err,
            )
            try:
                from api.xtl_analytics import capture_rejection
                capture_rejection(ev, "prop_block:PROP_CFG_READ_FAIL", gate="PROP", enrich=True)
            except Exception:
                pass
            _clear_watchlist_entry_block(ev, "PROP_CFG_READ_FAIL")
            continue

        if not bool(prop_cfg_pre.get("enabled")):
            log.error(
                "[PROP] BLOCK_ENTRY_PROP_DISABLED uid=%s sym=%s side=%s tid=%s profile=%s",
                uid,
                sym,
                side,
                tid,
                prop_cfg_pre.get("profile_id"),
            )
            try:
                from api.xtl_analytics import capture_rejection
                capture_rejection(ev, "prop_block:PROP_DISABLED", gate="PROP", enrich=True)
            except Exception:
                pass
            _clear_watchlist_entry_block(ev, "PROP_DISABLED")
            continue

        qty_use = 0.0
        log.warning(
            "[OPPT] QTY_DECISION uid=%s sym=%s side=%s tid=%s source=PROP_ONLY fixed_qty_ignored=%r has_overrides=%s strict=%s profile=%s",
            uid,
            sym,
            side,
            tid,
            qty_by_symbol.get(sym),
            has_overrides,
            strict_overrides,
            prop_cfg_pre.get("profile_id"),
        )

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
                    try:
                        from api.xtl_analytics import capture_rejection
                        capture_rejection(ev, f"invalidated_gate:{reason}", gate="ENTRY_GATE", enrich=True)
                    except Exception: pass
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

             retryable_blocked = _is_entry_blocked_state(watch_state)

             if watch_state not in ("REV_OK", "ENTRY_READY") and not retryable_blocked:
                 log.warning("[WATCHLIST] SKIP_ENTRY watch_not_ready uid=%s sym=%s side=%s tid=%s watch_state=%r",
                             uid, sym, side, tid, watch_state)
                 continue

             if retryable_blocked:
                 log.warning(
                     "[WATCHLIST] RETRY_ENTRY_BLOCKED_STATE_ALLOWED uid=%s sym=%s side=%s tid=%s watch_state=%s",
                     uid, sym, side, tid, watch_state,
                 )

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

        # Executor-level claim must be separate from watchlist claim.
        # Watchlist claim prevents duplicate ENTRY_EVENT creation.
        # Executor claim prevents duplicate MT5 order placement.
        # -------------------------------------------------
        # Executor-level placement claim.
        #
        # The claim prevents duplicate MT5 placement, but its existence
        # alone is not proof that an order/trade still exists.
        #
        # A completed/failed ORDER_PENDING lifecycle may remove the open
        # record while an old 24-hour claim survives. In that case, clear
        # the orphan after a safety grace period and reacquire atomically.
        # -------------------------------------------------
        exec_claim_key = f"xtl:oppt:exec_claim:{uid}:{tid}"
        _exec_claim_now_ms = now_ms()

        claimed = R.set(
            exec_claim_key,
            str(_exec_claim_now_ms),
            nx=True,
            ex=24 * 3600,
        )

        if not claimed:
            _claim_raw = None
            _claim_created_ms = 0
            _claim_age_ms = 0

            try:
                _claim_raw = R.get(exec_claim_key)

                if isinstance(_claim_raw, (bytes, bytearray)):
                    _claim_raw = _claim_raw.decode(
                        "utf-8",
                        "ignore",
                    )

                _claim_created_ms = int(
                    float(_claim_raw or 0)
                )

                if _claim_created_ms > 0:
                    _claim_age_ms = max(
                        0,
                        int(_exec_claim_now_ms)
                        - int(_claim_created_ms),
                    )

            except Exception:
                _claim_created_ms = 0
                _claim_age_ms = 0

            # open_by_id is the authoritative Redis open-ledger snapshot
            # already built earlier in this executor cycle.
            _matching_open_trade = None

            try:
                _matching_open_trade = open_by_id.get(tid)
            except Exception:
                _matching_open_trade = None

            _matching_lifecycle_active = False

            if isinstance(_matching_open_trade, dict):
                _matching_state = str(
                    _matching_open_trade.get("trade_state")
                    or _matching_open_trade.get("state")
                    or ""
                ).upper().strip()

                _matching_status = str(
                    _matching_open_trade.get("status")
                    or ""
                ).lower().strip()

                _matching_job_id = str(
                    _matching_open_trade.get("mt5_job_id")
                    or ""
                ).strip()

                _matching_ticket = str(
                    _matching_open_trade.get("mt5_ticket")
                    or _matching_open_trade.get("broker_ticket")
                    or ""
                ).strip()

                _matching_lifecycle_active = bool(
                    _matching_state in (
                        "ORDER_PENDING",
                        "TRADE_ACTIVE",
                    )
                    or _matching_status in (
                        "sent",
                        "pending",
                        "filled",
                        "open",
                    )
                    or _matching_job_id
                    or _matching_ticket
                )

            # Five minutes matches the market-order pending timeout.
            # During this grace period, preserve the claim even when the
            # open record is temporarily unavailable.
            _orphan_grace_ms = 5 * 60 * 1000

            if (
                not _matching_lifecycle_active
                and _claim_created_ms > 0
                and _claim_age_ms >= _orphan_grace_ms
            ):
                try:
                    R.delete(exec_claim_key)

                    log.warning(
                        "[OPPT] ORPHAN_EXEC_CLAIM_CLEARED "
                        "uid=%s sym=%s side=%s tid=%s "
                        "age_ms=%s key=%s",
                        uid,
                        sym,
                        side,
                        tid,
                        _claim_age_ms,
                        exec_claim_key,
                    )

                    # Reacquire atomically. If another worker wins this race,
                    # this worker must still skip.
                    _exec_claim_now_ms = now_ms()

                    claimed = R.set(
                        exec_claim_key,
                        str(_exec_claim_now_ms),
                        nx=True,
                        ex=24 * 3600,
                    )

                except Exception as _claim_repair_exc:
                    claimed = False

                    log.warning(
                        "[OPPT] ORPHAN_EXEC_CLAIM_REPAIR_FAILED "
                        "uid=%s sym=%s side=%s tid=%s "
                        "age_ms=%s key=%s err=%r",
                        uid,
                        sym,
                        side,
                        tid,
                        _claim_age_ms,
                        exec_claim_key,
                        _claim_repair_exc,
                    )

            if not claimed:
                log.warning(
                    "[OPPT] SKIP_ENTRY duplicate_exec_claim "
                    "uid=%s sym=%s side=%s tid=%s "
                    "age_ms=%s lifecycle_active=%s key=%s",
                    uid,
                    sym,
                    side,
                    tid,
                    _claim_age_ms,
                    _matching_lifecycle_active,
                    exec_claim_key,
                )
                continue
        
        
        # -------------------------------------------------
        # PROP FIRM COMPLIANCE CHECK
        # Runs after ENTRY_CLAIM so only one executor reserves risk.
        # -------------------------------------------------
        prop_check = None
        prop_cfg, prop_cfg_ok, prop_cfg_err = _get_prop_config_safe()
        prop_profile_id = "ftmo-main"

        if not prop_cfg_ok:
            log.error(
                "[PROP] BLOCK_ENTRY_PROP_CFG_READ_FAIL_AFTER_CLAIM uid=%s tid=%s sym=%s side=%s err=%s",
                uid,
                tid,
                sym,
                side,
                prop_cfg_err,
            )
            try:
                R.delete(ENTRY_CLAIM_KEY.format(alert_id=alert_id))
            except Exception:
                pass
            try:
                R.delete(exec_claim_key)
            except Exception:
                pass
            _clear_watchlist_entry_block(ev, "PROP_CFG_READ_FAIL")
            continue

        prop_profile_id = str(
            prop_cfg.get("profile_id")
            or prop_cfg.get("account_id")
            or "ftmo-main"
        )

        if not bool(prop_cfg.get("enabled")):
            log.error(
                "[PROP] BLOCK_ENTRY_PROP_DISABLED_AFTER_CLAIM uid=%s tid=%s sym=%s side=%s profile=%s",
                uid,
                tid,
                sym,
                side,
                prop_profile_id,
            )
            try:
                R.delete(ENTRY_CLAIM_KEY.format(alert_id=alert_id))
            except Exception:
                pass
            try:
                R.delete(exec_claim_key)
            except Exception:
                pass
            _clear_watchlist_entry_block(ev, "PROP_DISABLED")
            continue

        if bool(prop_cfg.get("enabled")):
            try:
                risk_state = _get_prop_risk_state(prop_profile_id)
                if bool(risk_state.get("trading_halted")):
                    halt_reason = str(risk_state.get("halt_reason") or "TRADING_HALTED")
                    log.warning(
                        "[PROP] BLOCK_ENTRY_HALTED uid=%s tid=%s sym=%s side=%s profile=%s reason=%s",
                        uid, tid, sym, side, prop_profile_id, halt_reason
                    )
                    try:
                        from api.xtl_analytics import capture_rejection
                        capture_rejection(ev, f"prop_block:{halt_reason}", gate="PROP", enrich=True)
                    except Exception:
                        pass
                    try:
                        R.delete(ENTRY_CLAIM_KEY.format(alert_id=alert_id))
                    except Exception:
                        pass

                    try:
                        R.delete(exec_claim_key)
                    except Exception:
                        pass

                    _clear_watchlist_entry_block(ev, halt_reason)
                    continue
                daily_r = float(risk_state.get("daily_r") or 0.0)
                daily_r_blocked = bool(risk_state.get("daily_r_blocked"))

                if daily_r_blocked or daily_r <= -2.0:
                    log.warning(
                        "[PROP] BLOCK_ENTRY_DAILY_R uid=%s tid=%s sym=%s side=%s daily_r=%s blocked=%s day=%s",
                        uid, tid, sym, side, daily_r, daily_r_blocked, risk_state.get("day")
                    )
                    try:
                        from api.xtl_analytics import capture_rejection
                        capture_rejection(ev, "prop_block:DAILY_R_LE_-2", gate="PROP", enrich=True)
                    except Exception:
                        pass
                    try:
                        R.delete(ENTRY_CLAIM_KEY.format(alert_id=alert_id))
                    except Exception:
                        pass

                    try:
                        R.delete(exec_claim_key)
                    except Exception:
                        pass

                    _clear_watchlist_entry_block(ev, "PROP_DAILY_R_BLOCK")
                    continue
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
                    max_open_positions=int(prop_cfg.get("max_open_positions") or 3),
                )

                if not isinstance(prop_check, dict) or prop_check.get("verdict") != "OK":
                    log.warning(
                        "[PROP] BLOCK_ENTRY uid=%s tid=%s sym=%s side=%s verdict=%s reasons=%s",
                        uid, tid, sym, side,
                        prop_check.get("verdict") if isinstance(prop_check, dict) else None,
                        prop_check.get("reasons") if isinstance(prop_check, dict) else None,
                    )
                    try:
                        from api.xtl_analytics import capture_rejection
                        _v = prop_check.get("verdict") if isinstance(prop_check, dict) else "NONE"
                        capture_rejection(ev, f"prop_block:{_v}", gate="PROP", enrich=True)
                    except Exception: pass
                    try:
                        R.delete(ENTRY_CLAIM_KEY.format(alert_id=alert_id))
                    except Exception:
                        pass

                    try:
                        R.delete(exec_claim_key)
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
                        try:
                            R.delete(exec_claim_key)
                        except Exception:
                            pass
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
                log.exception(
                    "[PROP] SKIP_ENTRY prop_check_exc uid=%s tid=%s sym=%s side=%s err=%r",
                    uid, tid, sym, side, e,
                )
                try:
                    R.delete(ENTRY_CLAIM_KEY.format(alert_id=alert_id))
                except Exception:
                    pass
                try:
                    R.delete(exec_claim_key)
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
        entry_zone_source = ""
        entry_zone_selection_model = ""
        try:
            if isinstance(entry_zone_obj, dict):
                entry_zone_source = str(entry_zone_obj.get("zone_source") or "")
                entry_zone_selection_model = str(entry_zone_obj.get("selection_model") or "")
        except Exception:
            entry_zone_source = ""
            entry_zone_selection_model = ""

        
        # -------------------------------------------------
        # P0 GUARD: broker-live max open positions
        # Redis open trades can lag during broker repair/reconciliation.
        # Before enqueue, use MT5 live position snapshot as source of truth.
        # -------------------------------------------------
        try:
            _max_open_positions = int(
                cfg.get("max_open_positions")
                or prop_cfg.get("max_open_positions")
                or 2
            )
        except Exception:
            _max_open_positions = 3

        try:
            _acct_type = str(acct or "demo").lower()
        except Exception:
            _acct_type = "demo"

        resolved = {}

        try:
            resolved = _resolve_prop_profile_device(prop_profile_id)
        except Exception as e:
            log.error(
                "[PROP] PROFILE_DEVICE_RESOLVE_EXC "
                "uid=%s profile=%s sym=%s side=%s err=%r",
                uid,
                prop_profile_id,
                sym,
                side,
                e,
            )
            resolved = {}

        device_id = ""

        if isinstance(resolved, dict) and resolved.get("ok"):
            device_id = str(
                resolved.get("device_id") or ""
            ).strip()

        if not device_id:
            resolve_reason = (
                resolved.get("reason")
                if isinstance(resolved, dict)
                else "BAD_RESOLVE_PAYLOAD"
            )

            log.warning(
                "[PROP] BLOCK_ENTRY profile_device_not_connected "
                "uid=%s profile=%s sym=%s side=%s reason=%s",
                uid,
                prop_profile_id,
                sym,
                side,
                resolve_reason,
            )

            try:
                R.delete(ENTRY_CLAIM_KEY.format(alert_id=alert_id))
            except Exception:
                pass

            try:
                R.delete(exec_claim_key)
            except Exception:
                pass

            _clear_watchlist_entry_block(
                ev,
                "PROFILE_DEVICE_NOT_CONNECTED",
            )

            continue

        _live_pos_count = _broker_live_position_count(device_id, _acct_type)

        if _max_open_positions > 0 and _live_pos_count >= _max_open_positions:
            log.warning(
                "[PROP] BLOCK_ENTRY uid=%s tid=%s sym=%s side=%s reason=MAX_OPEN_POSITIONS_REACHED "
                "broker_live_positions=%s max_open_positions=%s device_id=%s",
                uid,
                tid,
                sym,
                side,
                _live_pos_count,
                _max_open_positions,
                device_id,
            )

            try:
                pos["entry_blocked"] = True
                pos["entry_block_reason"] = "MAX_OPEN_POSITIONS_REACHED"
                pos["trade_state"] = "ENTRY_BLOCKED"
                pos["status"] = "blocked"
                pos["blocked_at_ms"] = int(time.time() * 1000)
                pos["broker_live_positions"] = int(_live_pos_count)
                pos["max_open_positions"] = int(_max_open_positions)
            except Exception:
                pass

            try:
                R.delete(ENTRY_CLAIM_KEY.format(alert_id=alert_id))
            except Exception:
                pass

            try:
                R.delete(exec_claim_key)
            except Exception:
                pass

            _clear_watchlist_entry_block(ev, "MAX_OPEN_POSITIONS_REACHED")
            continue
        # -------------------------------------------------
        # P0: Same-symbol cooldown after broker close.
        # Prevent immediate re-entry on the same symbol.
        # -------------------------------------------------
        try:
            _cool_key = f"xtl:cooldown:symbol:{uid}:{sym}"
            _ttl = int(R.ttl(_cool_key) or -2)

            if _ttl > 0:
                log.warning(
                    "[OPPT] BLOCK_ENTRY uid=%s tid=%s sym=%s side=%s "
                    "reason=SAME_SYMBOL_COOLDOWN ttl_sec=%s",
                    uid, tid, sym, side, _ttl,
                )

                _clear_watchlist_entry_block(ev, "SAME_SYMBOL_COOLDOWN")
                continue

        except Exception:
            pass

        # -------------------------------------------------
        # Late-entry guard:
        # If entry was previously blocked by prop capacity and
        # price has moved too far from original blocked entry,
        # reset the watch instead of chasing the move.
        # -------------------------------------------------
        try:
            _wk_late = str(ev.get("watch_key") or "").strip()
            if not _wk_late and sym and side:
                _wk_late = f"xtl:zone:watch:{sym}:{side}:H1"

            _w_late = {}
            if _wk_late:
                _raw_late = R.get(_wk_late)
                _w_late = _sj(_raw_late, {}) if _raw_late else {}

            if isinstance(_w_late, dict) and bool(_w_late.get("entry_blocked")):
                _blocked_entry = _sf(_w_late.get("blocked_entry_price"), 0.0)
                _current_entry = float(entry_price or 0.0)
                _max_move = float(_w_late.get("late_entry_max_move") or _late_entry_max_move(sym))

                if _blocked_entry > 0 and _current_entry > 0 and _max_move > 0:
                    _move = abs(_current_entry - _blocked_entry)

                    if _move > _max_move:
                        log.warning(
                            "[OPPT] SKIP_ENTRY late_entry_max_move_exceeded uid=%s tid=%s sym=%s side=%s blocked_entry=%s current_entry=%s move=%s max_move=%s",
                            uid, tid, sym, side, _blocked_entry, _current_entry, _move, _max_move,
                        )

                        try:
                            if _wk_late:
                                R.delete(_wk_late)
                            R.delete(f"xtl:watch:break_state:{sym}:{side}:H1")
                            for _ck in R.scan_iter(f"xtl:watch:entry_claim:{sym}:{side}:H1:*"):
                                R.delete(_ck)
                        except Exception:
                            pass

                        try:
                            R.delete(ENTRY_CLAIM_KEY.format(alert_id=alert_id))
                        except Exception:
                            pass

                        try:
                            R.delete(exec_claim_key)
                        except Exception:
                            pass

                        continue
        except Exception as e:
            log.warning(
                "[OPPT] LATE_ENTRY_GUARD_EXC uid=%s tid=%s sym=%s side=%s err=%r",
                uid, tid, sym, side, e,
            ) 
        if float(qty_use or 0.0) <= 0:
            log.error(
                "[PROP] BLOCK_ENTRY_QTY_ZERO uid=%s sym=%s side=%s tid=%s qty_use=%s reason=PROP_QTY_NOT_SET",
                uid,
                sym,
                side,
                tid,
                qty_use,
            )
            try:
                from api.xtl_analytics import capture_rejection
                capture_rejection(ev, "prop_block:PROP_QTY_ZERO", gate="PROP", enrich=True)
            except Exception:
                pass
            _clear_watchlist_entry_block(ev, "PROP_QTY_ZERO")
            continue   

        if exec_mode == "mt5":
            log.warning(
                "[OPPT] MT5_ENQUEUE uid=%s tid=%s sym=%s side=%s qty=%s acct=%s zone_src=%r zone_code=%s",
                uid, tid, sym, side, qty_use, mt5_account, zone_src_for_comment, zone_src_code
            )
        

            enq = _enqueue_mt5_market_order(
                user_id=uid,
                sym=sym,
                side=side,
                volume=qty_use,
                trade_id=tid,
                sl=float(sl_price) if sl_price > 0 else None,
                tp=float(tp_price) if tp_price > 0 else None,
                comment=f"XTL {zone_src_code}",
                mt5_account=mt5_account,
                profile_id=prop_profile_id,
            )
            log.warning(
                "[OPPT] MT5_ENQUEUE_RES uid=%s ok=%s job_id=%r device_id=%r profile=%r route=%r err=%r",
                uid, bool(enq.get("ok")), enq.get("job_id"), enq.get("device_id"),
                enq.get("profile_id"), enq.get("profile_resolve_reason"), enq.get("error")
            )
            

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
                    "device_id": enq.get("device_id"),
                    "profile_id": enq.get("profile_id"),
                    "profile_resolve_reason": enq.get("profile_resolve_reason"),
                }
                _save_state(uid, st)
            except Exception:
                pass

            if not enq.get("ok"):
                try:
                    R.delete(ENTRY_CLAIM_KEY.format(alert_id=alert_id))
                except Exception:
                    pass

                try:
                    R.delete(exec_claim_key)
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
            _opened_ms = now_ms()

            _order_type = _normalize_order_type({
                "order_type": (
                    ev.get("order_type")
                    or "MARKET"
                ),
            })
            pos = {
                "trade_id": tid,
                "symbol": sym,
                "side": side,
                "entry_price": float(entry_price),
                "qty": float(qty_use),
                "tp_price": float(tp_price) if tp_price > 0 else None,
                "sl_price": float(sl_price) if sl_price > 0 else None,
                "opened_at_ms": _opened_ms,
                "source": ev.get("source") or "oppt",
                "execution_mode": "mt5",
                "mt5_job_id": enq.get("job_id"),
                "device_id": enq.get("device_id"),
                "status": "sent",
                "trade_state": (
                    "ORDER_PENDING"
                    if exec_mode == "mt5"
                    else "TRADE_ACTIVE"
                ),

                
                # Explicit order lifecycle.
                "order_type": _order_type,

                # Only MARKET submissions expire when their ACK never arrives.
                # Broker-side LIMIT/STOP orders must remain pending until their
                # explicit fill/cancel/reject/expiry lifecycle completes.
                "ack_deadline_ms": (
                    _opened_ms + MARKET_ACK_TIMEOUT_MS
                    if (
                        exec_mode == "mt5"
                        and _order_type == "MARKET"
                    )
                    else None
                ),

                # Reserved for future broker-side pending-order support.
                "broker_order_ticket": None,
                "pending_order_state": None,
                "expiry_at_ms": None,
                "cancel_requested_at_ms": None,

               

                
                "entry_zone": ev.get("entry_zone"),
                "entry_zone_low": ev.get("entry_zone_low"),
                "entry_zone_high": ev.get("entry_zone_high"),
                "entry_zone_level": ev.get("entry_zone_level"),
                "entry_zone_tf": ev.get("entry_zone_tf"),
                "entry_zone_kind": ev.get("entry_zone_kind"),
                "entry_zone_source": entry_zone_source,
                "entry_zone_selection_model": entry_zone_selection_model,
                "zone_src_code": zone_src_code,
                "zone_src_for_comment": str(zone_src_for_comment or ""),
                "entry_gate_reason": ev.get("entry_gate_reason"),
                "trigger_type": ev.get("trigger_type"),
                "trigger_level": ev.get("trigger_level"),
                "prop_check": prop_check,
                "profile_id": prop_profile_id,
                "prop_firm": prop_check.get("firm") if isinstance(prop_check, dict) else None,
                "prop_phase": prop_check.get("phase") if isinstance(prop_check, dict) else None,
                "prop_risk_usd": prop_check.get("risk_usd") if isinstance(prop_check, dict) else None,
                "prop_risk_pct": prop_check.get("risk_pct") if isinstance(prop_check, dict) else None,
            }

            # -------------------------------------------------
            # CRITICAL: persist ORDER_PENDING immediately after
            # successful MT5 enqueue.
            #
            # This must happen before risk reserve / watch updates /
            # analytics, because broker may already place the order.
            # If Redis open ledger misses this write, gate can create
            # opposite-side REV_OK while broker position is active.
            # -------------------------------------------------
            _open_trade(uid, pos)

            try:
                _saved_raw = R.hget(OPEN_KEY.format(uid=uid), tid)
                if not _saved_raw:
                    log.error(
                        "[OPPT] OPEN_TRADE_SAVE_VERIFY_FAILED uid=%s tid=%s sym=%s side=%s job_id=%s",
                        uid, tid, sym, side, enq.get("job_id"),
                    )
                    try:
                        R.delete(exec_claim_key)
                    except Exception:
                        pass
                    continue

                log.warning(
                    "[OPPT] OPEN_TRADE_SAVED uid=%s tid=%s sym=%s side=%s state=%s job_id=%s device_id=%s",
                    uid, tid, sym, side, pos.get("trade_state"), enq.get("job_id"), enq.get("device_id"),
                )
            except Exception as e:
                log.exception(
                    "[OPPT] OPEN_TRADE_SAVE_VERIFY_EXC uid=%s tid=%s sym=%s side=%s err=%r",
                    uid, tid, sym, side, e,
                )
                try:
                    R.delete(exec_claim_key)
                except Exception:
                    pass
                continue

            # -------------------------------------------------
            # CRITICAL: same-symbol lifecycle guard.
            # Once one side is ORDER_PENDING/TRADE_ACTIVE, remove
            # the opposite watch immediately. BUY active must block SELL,
            # and SELL active must block BUY.
            # -------------------------------------------------
            try:
                opp_side = "SELL" if side == "BUY" else "BUY"
                R.delete(_zone_watch_key(uid,sym, opp_side, "H1"))
                R.delete(f"xtl:watch:break_state:{sym}:{opp_side}:H1")
                for k in R.scan_iter(f"xtl:watch:entry_claim:{sym}:{opp_side}:H1:*"):
                    R.delete(k)

                log.warning(
                    "[WATCHLIST] OPPOSITE_WATCH_CLEARED_ON_ENTRY sym=%s active_side=%s cleared_side=%s trade_id=%s",
                    sym, side, opp_side, tid,
                )
            except Exception as e:
                log.warning(
                    "[WATCHLIST] OPPOSITE_WATCH_CLEAR_FAILED sym=%s side=%s err=%r",
                    sym, side, e,
                )

            # -------------------------------------------------
            # PROP RISK RESERVE AFTER SUCCESSFUL MT5 ENQUEUE
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
                            "source": ev.get("source") or "watchlist",
                            "execution_source": "oppt_executor_mt5_enqueued",
                            "mt5_job_id": enq.get("job_id"),
                            "device_id": enq.get("device_id"),
                            "reserved_ts_ms": int(time.time() * 1000),
                            "profile_id": prop_profile_id,
                        },
                        profile_id=prop_profile_id,
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


        # -------------------------------------------------
        # Zone cooldown
        # Only apply after a REAL broker trade has completed.
        # ENTRY_FAIL / PROP_BLOCK / ORDER_REJECTED etc.
        # must NEVER create cooldown.
        # -------------------------------------------------
        if cooldown_min > 0:
            try:
                _trade_state = str(pos.get("trade_state") or "").upper()

                _has_real_trade = bool(
                    pos.get("mt5_ticket")
                    or pos.get("broker_ticket")
                    or pos.get("position_ticket")
                    or _trade_state == "TRADE_ACTIVE"
                )

                if _has_real_trade:
                    R.setex(cd_key, cooldown_min * 60, "1")
                    log.warning(
                        "[ZONE_COOLDOWN_SET] trade_id=%s sym=%s side=%s state=%s",
                        tid, sym, side, _trade_state
                    )
                else:
                    log.warning(
                        "[ZONE_COOLDOWN_SKIP] trade_id=%s sym=%s side=%s state=%s "
                        "reason=no_real_broker_trade",
                        tid, sym, side, _trade_state
                    )

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
