# -*- coding: utf-8 -*-

# agent_ohlc.py
"""
XTL Agent — MT5 OHLC uplink (robust version)
- Normalizes MT5 rates to list[dict] to avoid NumPy/Pandas truthiness errors.
- Skips duplicate pushes using last bar 't' per (symbol, timeframe).
- Keeps logging quiet and informative.
"""

from __future__ import annotations

import os
import time
import json
import logging
import urllib.parse
from typing import List, Optional, Tuple
import sys
from pathlib import Path
import requests
import uuid
import logging

log = logging.getLogger("xtl.agent")
# at module top (once):

_last_sent_bar: dict[tuple[str, str], int] = {}  # (symbol, TF) -> last 't' sent
_LAST_MT5_POSITIONS: dict[int, dict] = {}
_MT5_POS_STATE_LOADED = False

_PENDING_MT5_DEALS: dict[int, dict] = {}
_MT5_DEAL_STATE_LOADED = False

DEAL_RETRY_DELAYS_SEC = (
    60,
    120,
    300,
    600,
    1800,
    3600,
    6 * 3600,
    12 * 3600,
    24 * 3600,
)

DEAL_RETRY_OVERDUE_SEC = 24 * 3600

def _warn_ghost_datafolder() -> None:
    """C:\\MetaQuotes reappearing means a terminal was spawned without APPDATA."""
    import os
    ghost = r"C:\MetaQuotes\Terminal"
    try:
        if os.path.isdir(ghost):
            log.error(f"[mt5] ALERT ghost data folder present at {ghost} -- "
                      f"a terminal was spawned without APPDATA. "
                      f"Stop the agent and investigate before trading.")
    except Exception:
        pass

def _mt5_pos_state_path(dev_id: str, mt5_account: str = "demo") -> Path:
    base = os.environ.get("XTL_AGENT_STATE_DIR")

    if base:
        root = Path(base)
    elif os.name == "nt":
        root = Path(os.environ.get("PROGRAMDATA", r"C:\ProgramData")) / "XTL" / "state"
    else:
        root = Path.home() / ".xtl"

    return root / f"mt5_positions_{dev_id}_{mt5_account}.json"

def _load_last_mt5_positions(dev_id: str, mt5_account: str = "demo") -> dict[int, dict]:
    try:
        p = _mt5_pos_state_path(dev_id, mt5_account)
        if not p.exists():
            return {}

        data = json.loads(p.read_text(encoding="utf-8-sig") or "{}")
        positions = data.get("positions") or {}

        out = {}
        for k, v in positions.items():
            try:
                tk = int(k)
                if tk > 0 and isinstance(v, dict):
                    out[tk] = v
            except Exception:
                pass

        return out
    except Exception as e:
        try:
            log.warning("MT5_POS_STATE_LOAD_FAIL err=%s", e)
        except Exception:
            pass
        return {}




def _save_last_mt5_positions(
    dev_id: str,
    mt5_account: str,
    positions_by_ticket: dict[int, dict],
) -> None:
    p = _mt5_pos_state_path(dev_id, mt5_account)

    try:
        p.parent.mkdir(parents=True, exist_ok=True)

        payload = {
            "saved_at_ms": int(time.time() * 1000),
            "device_id": str(dev_id),
            "mt5_account": str(mt5_account),
            "positions": {
                str(k): v
                for k, v in (positions_by_ticket or {}).items()
            },
        }

        tmp = p.with_suffix(".tmp")

        tmp.write_text(
            json.dumps(
                payload,
                separators=(",", ":"),
                default=str,
            ),
            encoding="utf-8",
        )

        tmp.replace(p)

        log.warning(
            "MT5_POS_STATE_SAVED file=%s positions=%s",
            str(p),
            len(positions_by_ticket or {}),
        )

    except Exception as e:
        try:
            log.warning(
                "MT5_POS_STATE_SAVE_FAIL "
                "file=%s positions=%s err=%s",
                str(p),
                len(positions_by_ticket or {}),
                e,
            )
        except Exception:
            pass
# Force-pack critical modules under PyInstaller
try:
    import unicodedata, charset_normalizer, idna, urllib3  # noqa: F401
except Exception:
    pass

import atexit

try:
    import MetaTrader5 as MT5

    atexit.register(lambda: MT5.shutdown())
except Exception:
    pass

try:
    # Running as package / PyInstaller
    from xtl.mt5_client import (
        MT5_API_LOCK,
        mt5_locked,
        mt5_init,
        mt5_fetch_rates,
        get_mt5_tick_price_and_ts,
        mt5_get_open_positions,
        mt5_get_deal_summary,
        mt5_calc_order_margin,
        _resolve_broker_symbol,
        _broker_offset_min,
    )
except ImportError:
    try:
        # Running as package-relative source
        from .mt5_client import (
            MT5_API_LOCK,
            mt5_locked,
            mt5_init,
            mt5_fetch_rates,
            get_mt5_tick_price_and_ts,
            mt5_get_open_positions,
            mt5_get_deal_summary,
            mt5_calc_order_margin,
            _resolve_broker_symbol,
            _broker_offset_min,
        )
    except Exception:
        # Running directly from source folder
        sys.path.append(os.path.dirname(__file__))
        from mt5_client import (
            MT5_API_LOCK,
            mt5_locked,
            mt5_init,
            mt5_fetch_rates,
            get_mt5_tick_price_and_ts,
            mt5_get_open_positions,
            mt5_get_deal_summary,
            mt5_calc_order_margin,
            _resolve_broker_symbol,
            _broker_offset_min,
        )

DEFAULT_TFS = ["M1", "M15", "H1", "H4"]

# Canonical logical symbols published by the Agent.
#
# DXY is resolved by mt5_client._resolve_broker_symbol() to the
# broker-native symbol, for example:
#   FTMO         -> DXY.cash
#   another firm -> DXY / USDX / USDIndex / other supported alias
#
# The API/Redis symbol remains canonical "DXY", while the underlying
# OHLC comes from that device's own broker-native DXY instrument.
DEFAULT_SYMBOLS = [
    "XAUUSD",
    "EURUSD",
    "USDJPY",
    "GBPUSD",
    "USDCAD",
    "USDCHF",
    "DXY",
]

DEFAULT_SYMBOLS_CSV = ",".join(DEFAULT_SYMBOLS)
# Self-contained registry getter (prefers registry, falls back to env)



def _mt5_pending_deals_path(
    dev_id: str,
    mt5_account: str = "demo",
) -> Path:
    base = os.environ.get("XTL_AGENT_STATE_DIR")

    if base:
        root = Path(base)
    elif os.name == "nt":
        root = (
            Path(
                os.environ.get(
                    "PROGRAMDATA",
                    r"C:\ProgramData",
                )
            )
            / "XTL"
            / "state"
        )
    else:
        root = Path.home() / ".xtl"

    return root / (
        f"mt5_pending_deals_"
        f"{dev_id}_{mt5_account}.json"
    )


def _load_pending_mt5_deals(
    dev_id: str,
    mt5_account: str = "demo",
) -> dict[int, dict]:
    try:
        path = _mt5_pending_deals_path(
            dev_id,
            mt5_account,
        )

        if not path.exists():
            return {}

        payload = json.loads(
            path.read_text(
                encoding="utf-8-sig"
            )
            or "{}"
        )

        rows = payload.get("pending_deals") or {}

        result: dict[int, dict] = {}

        for raw_ticket, raw_row in rows.items():
            try:
                ticket = int(raw_ticket)

                if (
                    ticket > 0
                    and isinstance(raw_row, dict)
                ):
                    result[ticket] = dict(raw_row)

            except Exception:
                continue

        return result

    except Exception as exc:
        log.warning(
            "MT5_PENDING_DEALS_LOAD_FAIL "
            "dev=%s acct=%s err=%s",
            dev_id,
            mt5_account,
            exc,
        )
        return {}


def _save_pending_mt5_deals(
    dev_id: str,
    mt5_account: str,
    pending: dict[int, dict],
) -> None:
    path = _mt5_pending_deals_path(
        dev_id,
        mt5_account,
    )

    try:
        path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        payload = {
            "saved_at_ms": int(
                time.time() * 1000
            ),
            "device_id": str(dev_id),
            "mt5_account": str(mt5_account),
            "pending_deals": {
                str(ticket): row
                for ticket, row in (
                    pending or {}
                ).items()
            },
        }

        tmp = path.with_suffix(".tmp")

        tmp.write_text(
            json.dumps(
                payload,
                separators=(",", ":"),
                default=str,
            ),
            encoding="utf-8",
        )

        tmp.replace(path)

    except Exception as exc:
        log.warning(
            "MT5_PENDING_DEALS_SAVE_FAIL "
            "dev=%s acct=%s pending=%s err=%s",
            dev_id,
            mt5_account,
            len(pending or {}),
            exc,
        )


def _deal_retry_delay_ms(
    attempt_count: int,
) -> int:
    attempt = max(
        1,
        int(attempt_count or 1),
    )

    index = min(
        attempt - 1,
        len(DEAL_RETRY_DELAYS_SEC) - 1,
    )

    return int(
        DEAL_RETRY_DELAYS_SEC[index]
        * 1000
    )


def _queue_pending_mt5_deal(
    ticket: int,
    prev_position: dict,
    now_ms: int,
) -> None:
    ticket_i = int(ticket or 0)

    if ticket_i <= 0:
        return

    row = _PENDING_MT5_DEALS.get(
        ticket_i
    )

    if not isinstance(row, dict):
        row = {
            "ticket": ticket_i,
            "status": "PENDING_FETCH",
            "first_seen_ms": int(now_ms),
            "last_attempt_ms": None,
            "next_retry_ms": int(now_ms),
            "attempt_count": 0,
            "last_error": None,
            "prev_position": dict(
                prev_position or {}
            ),
            "deal_payload": None,
        }

        _PENDING_MT5_DEALS[
            ticket_i
        ] = row

    elif (
        not row.get("prev_position")
        and prev_position
    ):
        row["prev_position"] = dict(
            prev_position
        )


def _mark_pending_deal_failure(
    row: dict,
    error: str,
    now_ms: int,
) -> None:
    attempt_count = int(
        row.get("attempt_count")
        or 0
    ) + 1

    delay_ms = _deal_retry_delay_ms(
        attempt_count
    )

    row.update({
        "status": "PENDING_FETCH",
        "attempt_count": attempt_count,
        "last_attempt_ms": int(now_ms),
        "next_retry_ms": (
            int(now_ms) + delay_ms
        ),
        "retry_delay_ms": delay_ms,
        "last_error": str(
            error or "UNKNOWN"
        ),
        "deal_payload": None,
    })


def _mark_pending_deal_ready(
    row: dict,
    deal: dict,
    now_ms: int,
) -> None:
    row.update({
        "status": "READY_TO_PUSH",
        "last_attempt_ms": int(now_ms),
        "next_retry_ms": int(now_ms),
        "last_error": None,
        "deal_payload": dict(deal),
    })
def reg_get(name: str) -> Optional[str]:
    import os
    import winreg

    name_s = str(name or "")

    # Broker timezone is runtime MT5-detected state.
    # Never read HKLM/HKU for Broker.Tz* because installer_pending_detection
    # is only an install placeholder and must not block OHLC/RC logic.
    if name_s.startswith("Broker.Tz"):
        try:
            with winreg.OpenKey(winreg.HKEY_CURRENT_USER, r"Software\XTL") as k:
                v, _ = winreg.QueryValueEx(k, name_s)
                if v is not None and str(v).strip() != "":
                    return str(v)
        except Exception:
            pass

        return os.environ.get(name_s) or ""

    # Normal config still supports service/machine/user fallback.
    for root, path in (
        (winreg.HKEY_USERS, r"S-1-5-18\Software\XTL"),
        (winreg.HKEY_LOCAL_MACHINE, r"Software\XTL"),
        (winreg.HKEY_CURRENT_USER, r"Software\XTL"),
    ):
        try:
            with winreg.OpenKey(root, path) as k:
                v, _ = winreg.QueryValueEx(k, name_s)
                if v is not None and str(v).strip() != "":
                    return str(v)
        except Exception:
            pass

    return os.environ.get(name_s) or ""


def _good_ca(p: str, min_bytes: int = 100_000) -> bool:
    try:
        if not (p and os.path.isfile(p) and os.path.getsize(p) >= min_bytes):
            return False
        with open(p, "r", encoding="utf-8", errors="ignore") as f:
            return "-----BEGIN CERTIFICATE-----" in f.read(256)
    except Exception:
        return False


def _find_bundled_ca() -> Optional[str]:
    # Prefer _internal\certifi\cacert.pem beside the running exe
    here = Path(sys.argv[0]).resolve().parent
    candidates = [
        here / "_internal" / "certifi" / "cacert.pem",
        Path(os.environ.get("REQUESTS_CA_BUNDLE", "")),
        Path(os.environ.get("SSL_CERT_FILE", "")),
    ]
    for p in candidates:
        try:
            if _good_ca(str(p)):
                return str(p)
        except Exception:
            pass
    # try certifi as last resort
    try:
        import certifi

        c = certifi.where()
        if _good_ca(c):
            return c
    except Exception:
        pass
    return None


TF_SEC = {"M1": 60, "M5": 300, "M15": 900, "H1": 3600, "H2": 7200, "H4": 14400}


def push_mt5_positions_once(
    api_base: str,
    dev_id: str,
    token: str,
    mt5_account: str = "demo",
) -> bool:
    global _LAST_MT5_POSITIONS
    global _MT5_POS_STATE_LOADED
    global _PENDING_MT5_DEALS
    global _MT5_DEAL_STATE_LOADED

    try:
        now_ms = int(time.time() * 1000)

        # ---------------------------------------------------------
        # Load both persistent recovery states once after startup.
        # ---------------------------------------------------------
        if not _MT5_POS_STATE_LOADED:
            _LAST_MT5_POSITIONS = (
                _load_last_mt5_positions(
                    dev_id,
                    mt5_account,
                )
            )

            _MT5_POS_STATE_LOADED = True

            log.warning(
                "MT5_POS_STATE_LOADED "
                "dev=%s acct=%s "
                "previous_positions=%s",
                dev_id,
                mt5_account,
                len(
                    _LAST_MT5_POSITIONS
                    or {}
                ),
            )

        if not _MT5_DEAL_STATE_LOADED:
            _PENDING_MT5_DEALS = (
                _load_pending_mt5_deals(
                    dev_id,
                    mt5_account,
                )
            )

            _MT5_DEAL_STATE_LOADED = True

            log.warning(
                "MT5_PENDING_DEALS_LOADED "
                "dev=%s acct=%s pending=%s",
                dev_id,
                mt5_account,
                len(
                    _PENDING_MT5_DEALS
                    or {}
                ),
            )

        positions = mt5_get_open_positions()

        if positions is None:
            log.warning(
                "MT5_POS_PUSH_SKIP_INVALID_SNAPSHOT "
                "dev=%s acct=%s "
                "reason=MT5_POSITIONS_READ_FAILED",
                dev_id,
                mt5_account,
            )
            return False

        canonicalize_outbound(positions)

        current_positions: dict[int, dict] = {}

        for position in positions or []:
            try:
                ticket = int(
                    position.get("ticket")
                    or 0
                )

                symbol = str(
                    position.get("symbol")
                    or ""
                ).upper().strip()

                volume = float(
                    position.get("volume")
                    or 0.0
                )

                if (
                    ticket > 0
                    and symbol
                    and volume > 0
                ):
                    current_positions[
                        ticket
                    ] = dict(position)

            except Exception:
                continue

        # ---------------------------------------------------------
        # Any position that disappeared enters the durable queue.
        # It is NOT forgotten when the current-position baseline
        # advances.
        # ---------------------------------------------------------
        disappeared = sorted(
            set(
                _LAST_MT5_POSITIONS.keys()
            )
            - set(
                current_positions.keys()
            )
        )

        for ticket in disappeared:
            _queue_pending_mt5_deal(
                ticket,
                _LAST_MT5_POSITIONS.get(
                    ticket
                )
                or {},
                now_ms,
            )

        # Persist immediately so a crash after detection does not
        # lose the disappeared position.
        _save_pending_mt5_deals(
            dev_id,
            mt5_account,
            _PENDING_MT5_DEALS,
        )

        closed_deals = []
        ready_tickets = []

        # ---------------------------------------------------------
        # Process durable pending deal jobs.
        # ---------------------------------------------------------
        for ticket in sorted(
            list(
                _PENDING_MT5_DEALS.keys()
            )
        ):
            row = _PENDING_MT5_DEALS.get(
                ticket
            )

            if not isinstance(row, dict):
                continue

            status = str(
                row.get("status")
                or "PENDING_FETCH"
            ).upper().strip()

            prev_position = (
                row.get("prev_position")
                if isinstance(
                    row.get("prev_position"),
                    dict,
                )
                else {}
            )

            # A broker position became live again. This can happen
            # during an unreliable empty snapshot. Do not classify
            # it as closed.
            if ticket in current_positions:
                log.warning(
                    "MT5_PENDING_DEAL_CANCEL_LIVE "
                    "ticket=%s symbol=%s",
                    ticket,
                    current_positions[
                        ticket
                    ].get("symbol"),
                )

                _PENDING_MT5_DEALS.pop(
                    ticket,
                    None,
                )
                continue

            # Already recovered but not yet acknowledged by API:
            # resend the cached broker payload.
            cached_deal = row.get(
                "deal_payload"
            )

            if (
                status == "READY_TO_PUSH"
                and isinstance(
                    cached_deal,
                    dict,
                )
                and cached_deal.get("ok")
                is True
            ):
                deal = dict(cached_deal)

                deal["prev_position"] = (
                    prev_position
                )

                deal["recovery_source"] = (
                    deal.get(
                        "recovery_source"
                    )
                    or "agent_pending_queue"
                )

                canonicalize_outbound(deal)
                canonicalize_outbound(
                    deal.get("prev_position")
                )

                closed_deals.append(deal)
                ready_tickets.append(ticket)
                continue

            next_retry_ms = int(
                row.get("next_retry_ms")
                or 0
            )

            if (
                next_retry_ms > 0
                and now_ms < next_retry_ms
            ):
                continue

            try:
                deal = mt5_get_deal_summary(
                    int(ticket),
                    30,
                )

                if (
                    isinstance(deal, dict)
                    and deal.get("ok")
                    is True
                ):
                    deal["prev_position"] = (
                        prev_position
                    )

                    deal["recovery_source"] = (
                        "agent_pending_queue"
                    )

                    _mark_pending_deal_ready(
                        row,
                        deal,
                        now_ms,
                    )

                    canonicalize_outbound(deal)
                    canonicalize_outbound(
                        deal.get(
                            "prev_position"
                        )
                    )

                    closed_deals.append(deal)
                    ready_tickets.append(
                        ticket
                    )

                    log.warning(
                        "MT5_DEAL_RECOVERY_READY "
                        "ticket=%s attempts=%s "
                        "history_source=%s "
                        "broker_reason=%s "
                        "close_price=%s "
                        "net_profit=%s",
                        ticket,
                        row.get(
                            "attempt_count"
                        ),
                        deal.get(
                            "history_source"
                        ),
                        deal.get(
                            "broker_reason"
                        ),
                        deal.get(
                            "close_price"
                        ),
                        deal.get(
                            "net_profit"
                        ),
                    )

                else:
                    error = (
                        deal.get("error")
                        if isinstance(
                            deal,
                            dict,
                        )
                        else
                        "BAD_DEAL_PAYLOAD"
                    )

                    _mark_pending_deal_failure(
                        row,
                        error,
                        now_ms,
                    )

                    log.warning(
                        "MT5_DEAL_RECOVERY_RETRY "
                        "ticket=%s attempt=%s "
                        "error=%s "
                        "retry_delay_ms=%s "
                        "next_retry_ms=%s "
                        "prev_symbol=%s",
                        ticket,
                        row.get(
                            "attempt_count"
                        ),
                        row.get(
                            "last_error"
                        ),
                        row.get(
                            "retry_delay_ms"
                        ),
                        row.get(
                            "next_retry_ms"
                        ),
                        prev_position.get(
                            "symbol"
                        ),
                    )

            except Exception as exc:
                _mark_pending_deal_failure(
                    row,
                    (
                        f"{type(exc).__name__}:"
                        f"{exc}"
                    ),
                    now_ms,
                )

                log.warning(
                    "MT5_DEAL_RECOVERY_EXC "
                    "ticket=%s attempt=%s "
                    "next_retry_ms=%s err=%s",
                    ticket,
                    row.get(
                        "attempt_count"
                    ),
                    row.get(
                        "next_retry_ms"
                    ),
                    exc,
                )

        _save_pending_mt5_deals(
            dev_id,
            mt5_account,
            _PENDING_MT5_DEALS,
        )

        log.warning(
            "MT5_POS_PUSH_START "
            "dev=%s acct=%s positions=%s "
            "disappeared=%s pending=%s "
            "closed_deals=%s",
            dev_id,
            mt5_account,
            len(positions or []),
            len(disappeared),
            len(
                _PENDING_MT5_DEALS
                or {}
            ),
            len(closed_deals),
        )

        payload = {
            "device_id": dev_id,
            "mt5_account": mt5_account,
            "positions": positions,
            "closed_deals": closed_deals,
            "ts_ms": now_ms,
        }

        response = api_post(
            api_base,
            f"/devices/{dev_id}/mt5/positions",
            payload,
            token=token,
            timeout=6,
        )

        ok = bool(
            getattr(
                response,
                "status_code",
                0,
            )
            == 200
        )

        if ok:
            # The API has accepted these recovered deals. Only now
            # may they be removed from the durable queue.
            for ticket in ready_tickets:
                _PENDING_MT5_DEALS.pop(
                    int(ticket),
                    None,
                )

            _LAST_MT5_POSITIONS = dict(
                current_positions
            )

            _save_last_mt5_positions(
                dev_id,
                mt5_account,
                _LAST_MT5_POSITIONS,
            )

            _save_pending_mt5_deals(
                dev_id,
                mt5_account,
                _PENDING_MT5_DEALS,
            )

            if ready_tickets:
                log.warning(
                    "MT5_DEAL_RECOVERY_ACKED "
                    "dev=%s tickets=%s "
                    "remaining_pending=%s",
                    dev_id,
                    sorted(ready_tickets),
                    len(
                        _PENDING_MT5_DEALS
                        or {}
                    ),
                )

        else:
            # READY_TO_PUSH payloads remain cached and will be
            # resent. Nothing is removed on HTTP/API failure.
            log.warning(
                "MT5_POS_PUSH_API_FAILED "
                "dev=%s acct=%s status=%s "
                "ready_tickets=%s",
                dev_id,
                mt5_account,
                getattr(
                    response,
                    "status_code",
                    None,
                ),
                sorted(ready_tickets),
            )

        return ok

    except Exception as exc:
        log.exception(
            "MT5_POS_PUSH_EXC "
            "dev=%s acct=%s err=%s",
            dev_id,
            mt5_account,
            exc,
        )
        return False

def push_mt5_account_once(
    api_base: str, dev_id: str, token: str, mt5_account: str = "demo"
) -> bool:
    try:
        account = _mt5_account_meta()
        if not account:
            log.warning(
                "MT5_ACCOUNT_PUSH_SKIP empty account meta dev=%s acct=%s",
                dev_id,
                mt5_account,
            )
            return False

        payload = {
            "device_id": dev_id,
            "mt5_account": mt5_account,
            "account": account,
            "ts_ms": int(time.time() * 1000),
        }

        r = api_post(
            api_base,
            f"/devices/{dev_id}/mt5/account",
            payload,
            token=token,
            timeout=4,
        )

        log.warning(
            "MT5_ACCOUNT_PUSH "
            "dev=%s acct=%s balance=%s equity=%s margin=%s free=%s pnl=%s "
            "broker_tz=%s broker_offset=%s tz_source=%s code=%s",
            dev_id,
            mt5_account,
            account.get("balance"),
            account.get("equity"),
            account.get("margin"),
            account.get("free_margin"),
            account.get("floating_pnl"),
            account.get("broker_timezone"),
            account.get("broker_tz_offset_minutes"),
            account.get("broker_timezone_source"),
            getattr(r, "status_code", 0),
        )

        return bool(getattr(r, "status_code", 0) == 200)
    except Exception as e:
        try:
            log.warning("push_mt5_account_once failed: %s", e)
        except Exception:
            pass
        return False


@mt5_locked
def _log_mt5_symbol_specs_once() -> None:
    try:
        import MetaTrader5 as mt5
    except Exception as exc:
        log.warning(
            "[MT5_SPEC] IMPORT_FAILED err=%s",
            exc,
        )
        return

    symbols = (
        "XAUUSD",
        "EURUSD",
        "GBPUSD",
        "USDCAD",
        "USDCHF",
        "USDJPY",
    )

    for symbol in symbols:
        try:
            info = mt5.symbol_info(symbol)

            if info is None:
                log.warning(
                    "[MT5_SPEC] symbol=%s NOT_FOUND err=%s",
                    symbol,
                    mt5.last_error(),
                )
                continue

            log.warning(
                "[MT5_SPEC] symbol=%s "
                "volume_min=%s volume_step=%s volume_max=%s "
                "contract_size=%s",
                symbol,
                getattr(info, "volume_min", None),
                getattr(info, "volume_step", None),
                getattr(info, "volume_max", None),
                getattr(info, "trade_contract_size", None),
            )

        except Exception as exc:
            log.warning(
                "[MT5_SPEC] symbol=%s FAILED err=%s",
                symbol,
                exc,
            )
@mt5_locked
def _mt5_account_meta() -> dict:
    """
    MT5 account identity so backend can validate demo/live before trading.

    Uses MetaTrader5.account_info() when available.
    Never throws; returns {} on any failure.
    """
    try:
        import MetaTrader5 as mt5
    except Exception:
        return {}

    try:
        ai = mt5.account_info()
    except Exception:
        ai = None

    if not ai:
        return {}

    # ai is typically a namedtuple-like object. Use getattr safely.
    login = getattr(ai, "login", None)
    server = getattr(ai, "server", None)
    company = getattr(ai, "company", None)
    currency = getattr(ai, "currency", None)
    leverage = getattr(ai, "leverage", None)
    balance = getattr(ai, "balance", None)
    equity = getattr(ai, "equity", None)
    trade_mode = getattr(ai, "trade_mode", None)  # numeric (when exposed)
    margin = getattr(ai, "margin", None)
    free_margin = getattr(ai, "margin_free", None)
    margin_level = getattr(ai, "margin_level", None)
    profit = getattr(ai, "profit", None)
    credit = getattr(ai, "credit", None)

    # --- robust demo/live detection ---
    is_demo = None
    account_type = None

    # 1) Prefer MT5 constants when present
    try:
        tm = int(trade_mode) if trade_mode is not None else None
    except Exception:
        tm = None

    try:
        TM_DEMO = getattr(mt5, "ACCOUNT_TRADE_MODE_DEMO", None)
        TM_REAL = getattr(mt5, "ACCOUNT_TRADE_MODE_REAL", None)
        TM_CONTEST = getattr(mt5, "ACCOUNT_TRADE_MODE_CONTEST", None)

        if tm is not None and (
            TM_DEMO is not None or TM_REAL is not None or TM_CONTEST is not None
        ):
            if TM_DEMO is not None and tm == int(TM_DEMO):
                is_demo = True
                account_type = "DEMO"
            elif TM_REAL is not None and tm == int(TM_REAL):
                is_demo = False
                account_type = "LIVE"
            elif TM_CONTEST is not None and tm == int(TM_CONTEST):
                # contest behaves like demo for risk purposes (no live trading)
                is_demo = True
                account_type = "CONTEST"
    except Exception:
        pass

    # 2) Fallback heuristic (server text) only if still unknown
    if is_demo is None:
        try:
            s = str(server or "").lower()
            if s:
                if "demo" in s:
                    is_demo = True
                    account_type = account_type or "DEMO"
                elif "real" in s or "live" in s:
                    is_demo = False
                    account_type = account_type or "LIVE"
        except Exception:
            pass

    if account_type is None:
        account_type = "UNKNOWN"

    out = {
        "login": int(login) if login is not None else None,
        "server": str(server) if server is not None else None,
        "company": str(company) if company is not None else None,
        "currency": str(currency) if currency is not None else None,
        "leverage": int(leverage) if leverage is not None else None,
        # Prop firm source of truth
        "balance": float(balance) if balance is not None else None,
        "equity": float(equity) if equity is not None else None,
        "margin": float(margin) if margin is not None else None,
        "free_margin": float(free_margin) if free_margin is not None else None,
        "margin_level": float(margin_level) if margin_level is not None else None,
        "floating_pnl": float(profit) if profit is not None else None,
        "credit": float(credit) if credit is not None else None,
        "trade_mode": int(tm) if tm is not None else None,
        "is_demo": is_demo,
        "account_type": account_type,
    }
    # Broker timezone metadata (already detected by Agent)
    try:
        tz = _broker_tz_meta() or {}
    except Exception:
        tz = {}
    # Publish only trusted runtime-detected broker timezone.
    if bool(tz.get("tz_valid")):
        out["broker_timezone"] = str(
            tz.get("tz_name") or ""
        ).strip()

        out["broker_tz_offset_minutes"] = int(
            tz.get("tz_offset_min")
        )

        out["broker_timezone_source"] = str(
            tz.get("tz_source") or ""
        ).strip()
    

    # remove None values to keep payload small/clean
    # Remove None and empty-string values.
    return {
        k: v
        for k, v in out.items()
        if v is not None and v != ""
    }


def aggregate_from_m1(m1_bars, tf_label, broker_offset_min=0, max_out=200):
    """
    m1_bars: [{t_open_ms,o,h,l,c}] CLOSED, ascending by t_open_ms
    returns closed TF bars with t_open_ms/t_close_ms aligned to broker offset.
    """
    tf_sec = TF_SEC[tf_label]
    off_ms = int(broker_offset_min) * 60_000
    if not m1_bars:
        return []

    last_close_ms = m1_bars[-1]["t_open_ms"] + 60_000
    last_bucket_close = ((last_close_ms + off_ms) // (tf_sec * 1000)) * (
        tf_sec * 1000
    ) - off_ms

    lookback_ms = max_out * tf_sec * 1000
    start_ms = last_bucket_close - lookback_ms
    m1 = [b for b in m1_bars if b["t_open_ms"] >= start_ms]
    if not m1:
        return []

    out = []
    # first bucket close after the first m1 bar (aligned)
    bucket_close = (
        ((m1[0]["t_open_ms"] + off_ms) // (tf_sec * 1000)) * (tf_sec * 1000) - off_ms
    ) + tf_sec * 1000
    i, n = 0, len(m1)

    while bucket_close <= last_bucket_close:
        bucket_open = bucket_close - tf_sec * 1000
        seg = []
        while i < n and m1[i]["t_open_ms"] < bucket_close:
            if m1[i]["t_open_ms"] >= bucket_open:
                seg.append(m1[i])
            i += 1
        if seg:
            o = seg[0]["o"]
            c = seg[-1]["c"]
            h = max(x["h"] for x in seg)
            l = min(x["l"] for x in seg)
            out.append(
                {
                    "t_open_ms": bucket_open,
                    "t_close_ms": bucket_close,
                    "o": o,
                    "h": h,
                    "l": l,
                    "c": c,
                }
            )
        bucket_close += tf_sec * 1000
    return out[-max_out:]


import requests
from requests.adapters import HTTPAdapter

_SESS: requests.Session | None = None
# ======================
# API CIRCUIT BREAKER (prevents crash during network timeouts)
# ======================
import threading as _th

_API_LOCK = _th.Lock()
_api_offline_until = 0.0
_api_fail_count = 0


def _api_allowed() -> bool:
    try:
        now = time.time()
        with _API_LOCK:
            return now >= _api_offline_until
    except Exception:
        return True  # fail-open


def _api_mark_ok() -> None:
    global _api_offline_until, _api_fail_count
    try:
        with _API_LOCK:
            _api_fail_count = 0
            _api_offline_until = 0.0
    except Exception:
        pass


def _api_mark_fail() -> None:
    global _api_offline_until, _api_fail_count
    try:
        with _API_LOCK:
            _api_fail_count = min(_api_fail_count + 1, 6)
            backoff = min(60, 5 * (2 ** (_api_fail_count - 1)))
            _api_offline_until = time.time() + backoff
    except Exception:
        pass


def _http_session() -> requests.Session:
    global _SESS
    if _SESS is None:
        s = requests.Session()
        # Increase pool to avoid urllib3 "Connection pool is full"
        adapter = HTTPAdapter(
            pool_connections=50, pool_maxsize=50, max_retries=0, pool_block=True
        )
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        _SESS = s
    return _SESS


# -------------------------------------------------------------------
# ?? DEPRECATED
# Price publishing is handled ONLY by agent_price.py
# This function must never be started.
# -------------------------------------------------------------------
def price_push_loop(
    api_base: str,
    dev_id: str,
    token: str,
    symbols: list[str],
    interval_sec: float = 2.0,
):
    raise RuntimeError("price_push_loop is deprecated. Use agent_price.py")
    import time

    while True:
        for sym in symbols:
            try:
                px, ts_ms = get_mt5_tick_price_and_ts(sym)
                if px is None or ts_ms is None:
                    continue

                payload = {"symbol": sym, "price": float(px), "ts_ms": int(ts_ms)}

                # uses your existing api_post() with Authorization Bearer token
                api_post(
                    api_base,
                    f"/devices/{dev_id}/price",
                    payload,
                    token=token,
                    timeout=5,
                )

            except Exception:
                try:
                    log.exception("price_push failed sym=%s", sym)
                except Exception:
                    pass

        time.sleep(interval_sec)


def api_post(api_base: str, path: str, payload: dict, token: str, timeout: int = 20):
    import requests

    url = api_base.rstrip("/") + "/" + path.lstrip("/")
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    critical_broker_snapshot = (
        path.rstrip("/").endswith("/mt5/account")
        or path.rstrip("/").endswith("/mt5/positions")
    )

    # Non-critical traffic may use the shared backoff.
    # Account and position truth must still attempt delivery every cycle.
    if not critical_broker_snapshot and not _api_allowed():

        class _R:
            status_code = 0
            ok = False
            text = "skipped_offline"

        return _R()

    ca = _find_bundled_ca()
    verify: object = ca if ca else True  # True = system trust as last fallback

    r = None
    try:
        s = _http_session()
        r = s.post(url, headers=headers, json=payload, timeout=timeout, verify=verify)
        # Mark API health based on response
        try:
            code = int(getattr(r, "status_code", 0) or 0)
        except Exception:
            code = 0

        if 200 <= code < 300:
            _api_mark_ok()
        else:
            _api_mark_fail()

        tail = (token or "")[-6:]
        tag = "ACK" if "/mt5/ack" in url else ("NEXT" if "/mt5/next" in url else "POST")
        log.info(
            "%s url=%s code=%s token_tail=%s bytes=%s",
            tag,
            url,
            getattr(r, "status_code", "?"),
            tail,
            len((getattr(r, "text", "") or "").encode("utf-8")),
        )

        if getattr(r, "status_code", 0) != 200:
            log.warning("%s FAIL url=%s\n%s", tag, url, (r.text or "")[:500])
        return r

    except Exception as e:
        _api_mark_fail()
        log.warning("OHLC POST EXC %s: %s", url, e)

        class _R:
            status_code = 0
            ok = False
            text = str(e)

        return _R()

    finally:
        # CRITICAL: release connection back to urllib3 pool
        try:
            if r is not None:
                r.close()
        except Exception:
            pass


def api_get(api_base: str, path: str, token: str, timeout: int = 15):
    url = api_base.rstrip("/") + "/" + path.lstrip("/")
    headers = {"Authorization": f"Bearer {token}"}
    ca = _find_bundled_ca()
    verify = ca if ca else True

    r = None
    try:
        s = _http_session()
        r = s.get(url, headers=headers, timeout=timeout, verify=verify)
        return r
    except Exception as e:

        class _R:
            status_code = 0
            ok = False
            text = str(e)

        return _R()
    finally:
        # CRITICAL: release connection back to urllib3 pool
        try:
            if r is not None:
                r.close()
        except Exception:
            pass


# Track last pushed bar per (symbol, tf) to avoid duplicate uploads


# ----------------------- small utilities -----------------------
def _convert_utc_to_broker_ms(utc_ms, offset_min):
    if not utc_ms:
        return 0
    from datetime import datetime, timezone, timedelta

    dt_utc = datetime.fromtimestamp(utc_ms / 1000, tz=timezone.utc)
    dt_broker = dt_utc.astimezone(timezone(timedelta(minutes=offset_min)))
    return int(dt_broker.timestamp() * 1000)


def _tz_label(off_min: int) -> str:
    sign = "+" if int(off_min) >= 0 else "-"
    hh = abs(int(off_min)) // 60
    mm = abs(int(off_min)) % 60
    return f"UTC{sign}{hh:02d}:{mm:02d}"


def _is_trusted_broker_tz(off_min, source: str | None) -> bool:
    try:
        off = int(off_min)
    except Exception:
        return False

    src = (source or "").strip().lower()

    if off < -720 or off > 900:
        return False

    # historical bug: 330 came from IST/PC timezone
    if off == 330 and src != "auto_detected":
        return False

    return src == "auto_detected"


def _broker_tz_meta() -> dict:
    """
    P0: Broker timezone must come from MT5 runtime auto-detection only.
    Broker.Tz* is runtime state, not installer config.
    """
    src = ""
    off_min = None

    try:
        src = (reg_get("Broker.TzSource") or "").strip()
    except Exception:
        src = ""

    try:
        raw = reg_get("Broker.TzOffsetMin")
        off_min = int(str(raw).strip()) if str(raw or "").strip() else None
    except Exception:
        off_min = None

    # fallback only if HKCU runtime value is missing
    if off_min is None:
        try:
            off_min = int(_broker_offset_min())
        except Exception:
            off_min = None

    valid = _is_trusted_broker_tz(off_min, src)

    return {
        "tz_name": _tz_label(int(off_min)) if off_min is not None else None,
        "tz_offset_min": int(off_min) if off_min is not None else None,
        "tz_source": src or "unknown",
        "tz_valid": bool(valid),
    }


import winreg


def ensure_registry_defaults():
    """Create default Symbols/Timeframes/IncludeLatest in HKU\S-1-5-18\Software\XTL if absent."""
    path = r"S-1-5-18\Software\XTL"
    defaults = {
        "Symbols": DEFAULT_SYMBOLS_CSV,
        "Timeframes": "M1,M5,M15,H1,H2,H4",
        "IncludeLatest": "0",
    }
    try:
        with winreg.CreateKeyEx(
            winreg.HKEY_USERS, path, 0, winreg.KEY_SET_VALUE | winreg.KEY_QUERY_VALUE
        ) as k:
            for name, val in defaults.items():
                try:
                    winreg.QueryValueEx(k, name)  # exists?
                except FileNotFoundError:
                    winreg.SetValueEx(k, name, 0, winreg.REG_SZ, val)
    except Exception as e:
        log.debug("ensure_registry_defaults: %s", e)


# --- Install/Config versioning (bump on each installer build) ---
CONFIG_VERSION = os.environ.get(
    "XTL_CONFIG_VERSION", "2026-07-17-dxy-h1-v1",
)  # installer can override


def _xtl_reg_path():
    return r"S-1-5-18\Software\XTL"  # LocalSystem hive (service)


def _reg_set_value(root, subkey, name, value):
    import winreg

    with winreg.CreateKeyEx(
        root, subkey, 0, winreg.KEY_SET_VALUE | winreg.KEY_QUERY_VALUE
    ) as k:
        winreg.SetValueEx(k, name, 0, winreg.REG_SZ, str(value))


def _reg_get_value(root, subkey, name, default=None):
    import winreg

    try:
        with winreg.OpenKey(root, subkey, 0, winreg.KEY_QUERY_VALUE) as k:
            v, _ = winreg.QueryValueEx(k, name)
            return v
    except Exception:
        return default


def _reg_delete_value(root, subkey, name):
    import winreg

    try:
        with winreg.OpenKey(root, subkey, 0, winreg.KEY_SET_VALUE) as k:
            try:
                winreg.DeleteValue(k, name)
            except FileNotFoundError:
                pass
    except Exception:
        pass


def reset_registry_tf_symbols(
    include_latest="0",
    symbols=None,
    timeframes="M1,M15,H1,H4",
):
    """
    Hard reset the three user-tunable keys under
    HKU\\S-1-5-18\\Software\\XTL.

    Called on a new installation or when
    XTL_RESET_REGISTRY=1.
    """
    if symbols is None:
        symbols = DEFAULT_SYMBOLS_CSV
    """
    Hard reset the three user-tunable keys under HKU\S-1-5-18\Software\XTL.
    Called on 'new installation' or when XTL_RESET_REGISTRY=1.
    """
    import winreg

    subkey = _xtl_reg_path()
    # nuke specific values (do NOT delete the whole key to avoid permissions issues)
    _reg_delete_value(winreg.HKEY_USERS, subkey, "Symbols")
    _reg_delete_value(winreg.HKEY_USERS, subkey, "Timeframes")
    _reg_delete_value(winreg.HKEY_USERS, subkey, "IncludeLatest")
    # write fresh defaults
    _reg_set_value(winreg.HKEY_USERS, subkey, "Symbols", symbols)
    _reg_set_value(winreg.HKEY_USERS, subkey, "Timeframes", timeframes)
    _reg_set_value(winreg.HKEY_USERS, subkey, "IncludeLatest", include_latest)


def maybe_reset_registry_on_new_install():
    """
    If ConfigVersion != CONFIG_VERSION (or XTL_RESET_REGISTRY=1), reset keys and stamp new version.
    """
    import winreg

    subkey = _xtl_reg_path()
    cur_ver = _reg_get_value(winreg.HKEY_USERS, subkey, "ConfigVersion", "")
    force = os.environ.get("XTL_RESET_REGISTRY", "").strip() in (
        "1",
        "true",
        "TRUE",
        "yes",
        "YES",
    )
    if force or str(cur_ver) != str(CONFIG_VERSION):
        # reset to our intended defaults (M1-only + closed bars by default)
        reset_registry_tf_symbols(include_latest="0", timeframes="M1,M15,H1,H4")
        _reg_set_value(winreg.HKEY_USERS, subkey, "ConfigVersion", CONFIG_VERSION)
        try:
            log.info(
                "registry reset applied (ConfigVersion %s -> %s, force=%s)",
                cur_ver,
                CONFIG_VERSION,
                force,
            )
        except Exception:
            pass


def _agent_pull_cfg():
    """
    Resolve symbols/timeframes and include_latest flag from registry (no JSON file).
    Keys (REG_SZ):
      - Symbols:       comma-separated, e.g. "XAUUSD,EURUSD,GBPUSD,USDJPY,USDCHF,USDCAD"
      - Timeframes:    comma-separated, e.g. "M1,M5,M10,M15,H1,H4"
      - IncludeLatest: "0" or "1" (append forming bar as complete=False)
    Fallback defaults are safe for your current XAU-only flow.
    """
    try:
        syms_raw = (reg_get("Symbols") or "").strip()
        tfs_raw = (reg_get("Timeframes") or "").strip()
        inc_raw = (reg_get("IncludeLatest") or "0").strip()
    except Exception:
        syms_raw, tfs_raw, inc_raw = "", "", "0"

    # Defaults if not present
    if not syms_raw:
        syms_raw = DEFAULT_SYMBOLS_CSV  # 6 trading instruments + DXY
    if not tfs_raw:
        tfs_raw = "M1,M15,H1,H2,H4"

    syms = [s.strip().upper() for s in syms_raw.split(",") if s.strip()]
    tf_set = {"M1", "M15", "H1", "H4"}  # allow future toggles
    tfs = [t.upper().strip() for t in tfs_raw.split(",") if t.upper().strip() in tf_set]
    if not tfs:
        tfs = ["M1", "M15", "H1", "H4"]  # safe fallback, preserves your M1 default

    include_latest = inc_raw in ("1", "true", "TRUE", "yes", "YES")
    return syms, tfs, include_latest


def _join_api(api_base: str, path: str) -> str:
    """
    Normalize api_base (strip trailing slash and accidental '/api' suffix)
    and join with a leading-slash path.
    """
    base = (api_base or "").strip().rstrip("/")
    if base.lower().endswith("/api"):
        base = base[:-4]  # drop '/api'
    if not path.startswith("/"):
        path = "/" + path
    return f"{base}{path}"


def _is_empty(x) -> bool:
    if x is None:
        return True
    try:
        import numpy as _np  # lazy import

        if isinstance(x, _np.ndarray):
            return x.size == 0
    except Exception:
        pass
    try:
        return len(x) == 0
    except Exception:
        return False


def _normalize_rates(arr_raw):
    """
    Returns (ok: bool, list_of_dicts, err_msg|None)
    Each dict has keys: t,o,h,l,c,v (ints/floats). Guarantees closed bars only.
    """
    import numpy as np

    if arr_raw is None:
        return False, [], "no data"

    # If it’s a NumPy structured array, convert
    if isinstance(arr_raw, np.ndarray):
        if arr_raw.size == 0:
            return False, [], "empty"
        names = tuple(arr_raw.dtype.names or ())

        def _num(row, field, default=0.0):
            try:
                if field in names:
                    return float(row[field])
            except Exception:
                pass
            return float(default)

        out = [
            {
                "t": int(r["time"]),
                "o": float(r["open"]),
                "h": float(r["high"]),
                "l": float(r["low"]),
                "c": float(r["close"]),
                "v": _num(r, "tick_volume", _num(r, "real_volume", 0.0)),
            }
            for r in arr_raw
        ]
        return True, out, None

    # If it’s already a list of dicts with required fields
    if isinstance(arr_raw, (list, tuple)) and arr_raw and isinstance(arr_raw[0], dict):
        # validate minimal schema and coerce
        out = []
        for r in arr_raw:
            try:
                out.append(
                    {
                        "t": int(r.get("t")),
                        "o": float(r.get("o")),
                        "h": float(r.get("h")),
                        "l": float(r.get("l")),
                        "c": float(r.get("c")),
                        "v": float(r.get("v", 0.0)),
                    }
                )
            except Exception:
                return False, [], "schema error"
        return True, out, None

    # Anything else
    try:
        # attempt len() to distinguish empty containers
        if hasattr(arr_raw, "__len__") and len(arr_raw) == 0:
            return False, [], "empty"
    except Exception:
        pass
    return False, [], f"unsupported type: {type(arr_raw).__name__}"


import time


def push_rates_batch(
    api_base, device_id, token, symbol, tf, bars, include_latest=False, **kw
):
    """
    Send ONLY closed candles to /devices/{device_id}/ohlc, and (optionally) attach
    the current forming slot as `latest_bar` (complete=False) for live UI/nowcast.

    - Accepts bars with 't' as UTC (sec/ms/us/ns).
    - Computes close boundary using tf -> ms.
    - Filters out any forming candle from the historical `bars` list.
    - Preserves OHLC exactly as provided.
    - If include_latest=True and the tail bar is forming (or marked complete=False),
      attach it to payload.latest_bar (not inserted into historical list).
    """

    import time

    def _to_ms(t):
        """
        Normalize epoch-like value to milliseconds.

        - MT5 'time' is in seconds since epoch (˜1e9) -> we multiply by 1000
        - If something is already large (>=1e12), treat as milliseconds
        """
        try:
            t = int(t or 0)
            if t <= 0:
                return 0

            # If already very large, assume it's in milliseconds (or finer) and keep it.
            # 1e12 ms ˜ year 2001, so any normal ms timestamp will be >= this.
            if t >= 1_000_000_000_000:
                return t  # already ms (or bigger; we don't expect µs/ns here)

            # Otherwise treat as seconds
            return t * 1000
        except Exception:
            return 0

    # --- timeframe -> ms (supports M1/M5/M10/M15/H1/H4; tolerant to lowercase) ---
    tf_s = (tf or "").upper()
    TF_MS = {
        "M1": 1 * 60 * 1000,
        "M5": 5 * 60 * 1000,
        "M10": 10 * 60 * 1000,
        "M15": 15 * 60 * 1000,
        "H1": 60 * 60 * 1000,
        "H2": 2 * 60 * 60 * 1000,
        "H4": 4 * 60 * 60 * 1000,
    }
    tf_ms = TF_MS.get(tf_s, 0)
    if not tf_ms:
        # unknown TF; don't post malformed data
        return False
    # --- anchor to broker TF grid rather than local clock ---
    # (prevents off-by-one-bar when OS TZ != broker TZ)
    bmeta = _broker_tz_meta() or {}
    if not bmeta.get("tz_valid"):
        log.error(
            "P0_TZ_BLOCK: skip OHLC publish %s/%s; broker timezone not trusted meta=%s",
            symbol,
            tf,
            bmeta,
        )
        return False
    try:
        off_min = int(bmeta.get("tz_offset_min") or 0)
    except Exception:
        off_min = 0
    off_ms = off_min * 60_000

    now_ms = int(time.time() * 1000)
    # slot_ms = ((now_ms + off_ms) // tf_ms) * tf_ms - off_ms  # open of *current* bar in broker time
    slot_ms = (now_ms // tf_ms) * tf_ms
    # --- build extra features for backend reasoning (RVOL, USD basket, probs) ---
    extras = {}
    raw_last = (bars or [])[-1] if (bars or []) else None

    def _safe_float(x):
        try:
            return float(x)
        except Exception:
            return None

    if isinstance(raw_last, dict):
        # 1) RVOL (15m) if present on raw bar
        rv = raw_last.get("rvol15") or raw_last.get("feat_rvol15")
        rv_val = _safe_float(rv)
        if rv_val is not None:
            extras["feat_rvol15"] = rv_val

        # 2) USD basket / macro tilt if present
        ub = raw_last.get("usd_basket") or raw_last.get("feat_usd_basket")
        ub_val = _safe_float(ub)
        if ub_val is not None:
            extras["feat_usd_basket"] = ub_val

        # 3) Probability fields, if the agent ever attaches them
        pu = _safe_float(raw_last.get("prob_up"))
        if pu is not None:
            extras["prob_up"] = pu

        pu1 = _safe_float(raw_last.get("prob_up_1h"))
        if pu1 is not None:
            extras["prob_up_1h"] = pu1

    arr_closed = []
    latest_bar = None  # optional live forming bar (kept separate from history)

    n = len(bars or [])
    for i, b in enumerate(bars or []):
        # Normalize inputs
        t_utc_ms = _to_ms(b.get("t"))
        # t_open_ms = _convert_utc_to_broker_ms(t_utc_ms, off_min)
        t_open_ms = t_utc_ms  # MT5 rates.time is epoch (UTC sec)

        if not t_open_ms:
            continue  # skip malformed rows

        t_close_ms = t_open_ms + tf_ms

        # Decide if this bar is forming
        explicit_complete = b.get("complete")
        close_tolerance_ms = 10_000

        time_says_closed = (
            t_close_ms <= now_ms + close_tolerance_ms
        )

        if explicit_complete is False:
            is_forming = True
        else:
            # complete=True is a hint, not authority.
            is_forming = not time_says_closed

        if explicit_complete is True and not time_says_closed:
            log.error(
                "P0_FUTURE_COMPLETE_REJECT "
                "symbol=%s tf=%s "
                "open_ms=%s close_ms=%s "
                "server_now_ms=%s ahead_ms=%s",
                symbol,
                tf_s,
                t_open_ms,
                t_close_ms,
                now_ms,
                t_close_ms - now_ms,
            )

        # If forming ,and it's the tail and include_latest=True, capture as latest_bar (NOT in history)
        if is_forming and include_latest and (i == n - 1):
            latest_bar = {
                "t": int(
                    t_open_ms // 1000
                ),  # seconds (server can also rely on t_open_ms)
                "t_open_ms": int(t_open_ms),
                "t_close_ms": int(t_close_ms),
                "o": float(b.get("o", 0)),
                "h": float(b.get("h", 0)),
                "l": float(b.get("l", 0)),
                "c": float(b.get("c", 0)),
                "v": int(b.get("v", 0)),
                "complete": False,
            }
            continue  # do not insert into historical list

        # Only closed bars go to history
        if is_forming:
            continue

        # Append a normalized closed bar
        arr_closed.append(
            {
                # legacy field 't' kept for compatibility (open time in ms)
                "t": int(t_open_ms // 1000),
                # explicit fields used by server/UI
                "t_open_ms": int(t_open_ms),
                "t_close_ms": int(t_close_ms),
                # exact OHLC (no rounding beyond float())
                "o": float(b.get("o", 0)),
                "h": float(b.get("h", 0)),
                "l": float(b.get("l", 0)),
                "c": float(b.get("c", 0)),
                # optional volume
                "v": int(b.get("v", 0)),
                "complete": True,
            }
        )

    # Optional tail limit if caller passed bars count
    max_count = int(kw.get("max_count") or kw.get("count") or 0)
    if max_count and len(arr_closed) > max_count:
        arr_closed = arr_closed[-max_count:]

    acct = {}
    try:
        acct = _mt5_account_meta() or {}
    except Exception:
        acct = {}

    # Compute serverNow as max of system clock and last bar close time
    # This prevents gate from treating recent closed bars as "future" candles
    _server_now = int(now_ms)
    _last_closed_ms = 0
    try:
        if arr_closed:
            _last_closed_ms = int(
                arr_closed[-1].get("t_close_ms") or 0
            )
    except Exception:
        _last_closed_ms = 0

    payload = {
        "symbol": (symbol or "").upper(),
        "timeframe": tf_s,
        "bars": arr_closed,
        "count": len(arr_closed),
        "written_at": now_ms,
        "device_id": str(device_id),
        "source": "broker",
        "broker": _broker_tz_meta(),
        "account": acct,
        "extra": extras or {},
        "serverNow": _server_now,
        "lastClosedTs": _last_closed_ms,
    }
    # terminal info is optional; only add if present
    term = {}
    try:
        import MetaTrader5 as mt5

        ti = mt5.terminal_info()
        if ti:
            # Prefer full dict if available
            try:
                term = ti._asdict()
            except Exception:
                term = {}
            v = getattr(ti, "version", None)
            if v is None:
                v = getattr(ti, "build", None)
            p = getattr(ti, "path", None)
            if v is not None:
                term["mt5_version"] = v
            if p:
                term["terminal_path"] = p
    except Exception:
        pass

    if term:
        payload["terminal"] = term
    if latest_bar is not None:
        payload["latest_bar"] = latest_bar

    r = api_post(api_base, f"/devices/{device_id}/ohlc", payload, token, timeout=20)
    return bool(getattr(r, "ok", False))


def _assert_mt5_account(expected: str = "demo") -> tuple[bool, str, dict]:
    """
    expected: "demo" | "live"
    Returns (ok, err, meta)
    """
    try:
        import MetaTrader5 as mt5
    except Exception as e:
        return False, f"mt5_import:{e}", {}

    ai = None
    try:
        ai = mt5.account_info()
    except Exception:
        ai = None

    if not ai:
        return False, f"account_info_none:{mt5.last_error()}", {}

    tm = getattr(ai, "trade_mode", None)
    login = getattr(ai, "login", None)
    server = getattr(ai, "server", None)

    meta = {"login": login, "server": server, "trade_mode": tm}

    # MT5 trade_mode commonly: 0=DEMO, 1=CONTEST, 2=REAL
    try:
        tm_i = int(tm) if tm is not None else None
    except Exception:
        tm_i = None

    exp = (expected or "demo").strip().lower()
    if exp == "demo":
        if tm_i not in (0, 1):  # treat CONTEST like demo for safety
            return False, f"expected_demo_got_trade_mode:{tm_i}", meta
    elif exp == "live":
        if tm_i != 2:
            return False, f"expected_live_got_trade_mode:{tm_i}", meta

    return True, "", meta




def _resolve_market_filling_mode(mt5, symbol_info):
    """
    Resolve the broker-supported filling policy for a market order.

    symbol_info.filling_mode is a bitmask:
      1 = FOK supported
      2 = IOC supported

    mt5.ORDER_FILLING_* are order-request enum values:
      FOK    = 0
      IOC    = 1
      RETURN = 2
    """
    if symbol_info is None:
        raise RuntimeError("symbol_info_missing")

    filling_flags = int(
        getattr(symbol_info, "filling_mode", 0) or 0
    )

    execution_mode = int(
        getattr(symbol_info, "trade_exemode", -1)
    )

    # Prefer IOC where supported.
    # CTI XAUUSDC reports filling_flags=2, so this returns ORDER_FILLING_IOC.
    if filling_flags & 2:
        return mt5.ORDER_FILLING_IOC

    if filling_flags & 1:
        return mt5.ORDER_FILLING_FOK

    # RETURN is not valid for Market Execution.
    if execution_mode != mt5.SYMBOL_TRADE_EXECUTION_MARKET:
        return mt5.ORDER_FILLING_RETURN

    raise RuntimeError(
        "no_supported_market_filling_mode:"
        f"symbol={getattr(symbol_info, 'name', '')}:"
        f"filling_flags={filling_flags}:"
        f"execution_mode={execution_mode}"
    )

import threading, traceback



@mt5_locked
def _mt5_send_market_order(cmd: dict) -> dict:
    """
    Execute MT5 market order.
    cmd keys: symbol, side, volume, sl, tp, comment
    """
    try:
        import MetaTrader5 as mt5
    except Exception as e:
        return {"ok": False, "error": f"mt5_import:{e}"}
    # ------------------------------------------------------------
    # P0 MARKET ENTRY ABSOLUTE EXPIRY GUARD
    #
    # The API stamps new MARKET ENTRY commands with expires_at_ms.
    # Once expired, this Agent must NEVER execute that stale entry,
    # even if the command was dequeued just before server-side
    # cancellation.
    #
    # Applies only to:
    #   type = market_order
    #   kind = ENTRY
    #
    # It must NOT block close-position or SL/TP management commands.
    # ------------------------------------------------------------
    kind_u = str(
        cmd.get("kind") or ""
    ).upper().strip()

    if kind_u == "ENTRY":
        try:
            expires_at_ms = int(
                cmd.get("expires_at_ms")
                or 0
            )
        except Exception:
            expires_at_ms = 0

        now_ms = int(
            time.time() * 1000
        )

        # New API-generated ENTRY commands must carry an immutable
        # expires_at_ms. If present and already expired, reject before
        # any MT5 order_send / margin / price execution work.
        if (
            expires_at_ms > 0
            and now_ms >= expires_at_ms
        ):
            age_ms = None

            try:
                created_at_ms = int(
                    cmd.get("created_at_ms")
                    or 0
                )

                if created_at_ms > 0:
                    age_ms = max(
                        0,
                        now_ms - created_at_ms,
                    )
            except Exception:
                age_ms = None

            log.error(
                "[AGENT] MARKET_ENTRY_EXPIRED_REJECT "
                "job_id=%s trade_id=%s profile=%s "
                "symbol=%s side=%s "
                "created_at_ms=%s expires_at_ms=%s "
                "now_ms=%s age_ms=%s",
                cmd.get("job_id"),
                cmd.get("trade_id"),
                cmd.get("profile_id"),
                cmd.get("symbol"),
                cmd.get("side"),
                cmd.get("created_at_ms"),
                expires_at_ms,
                now_ms,
                age_ms,
            )

            return {
                "ok": False,
                "error": "MARKET_ENTRY_EXPIRED",
                "expired": True,
                "execution_blocked": True,
                "reason": "ABSOLUTE_MARKET_ENTRY_EXPIRY",
                "job_id": cmd.get("job_id"),
                "trade_id": cmd.get("trade_id"),
                "symbol": cmd.get("symbol"),
                "side": cmd.get("side"),
                "created_at_ms": cmd.get("created_at_ms"),
                "expires_at_ms": expires_at_ms,
                "rejected_at_ms": now_ms,
                "age_ms": age_ms,
            }

    # -------------------- NEW: demo/live safety guard --------------------
    expected_acct = (cmd.get("mt5_account") or "demo").strip().lower()
    try:
        ai = mt5.account_info()
    except Exception:
        ai = None

    if not ai:
        return {"ok": False, "error": f"account_info_none:{mt5.last_error()}"}

    tm = getattr(ai, "trade_mode", None)  # 0=DEMO, 1=CONTEST, 2=REAL
    login = getattr(ai, "login", None)
    server = getattr(ai, "server", None)

    try:
        tm_i = int(tm) if tm is not None else None
    except Exception:
        tm_i = None

    # treat CONTEST (1) as non-live; still safe
    if expected_acct == "demo":
        if tm_i not in (0, 1):
            return {
                "ok": False,
                "error": f"acct_guard_expected_demo_got:{tm_i}",
                "meta": {"login": login, "server": server, "trade_mode": tm_i},
            }
    elif expected_acct == "live":
        if tm_i != 2:
            return {
                "ok": False,
                "error": f"acct_guard_expected_live_got:{tm_i}",
                "meta": {"login": login, "server": server, "trade_mode": tm_i},
            }

    # audit log (best-effort)
    try:
        import logging

        logging.getLogger("xtl.agent").info(
            "[MT5] acct ok | expected=%s | login=%s | server=%s | trade_mode=%s",
            expected_acct,
            login,
            server,
            tm_i,
        )
    except Exception:
        pass
    # -------------------- END NEW BLOCK --------------------

    canonical_symbol = str(
        cmd.get("symbol") or ""
    ).upper().strip()

    side = str(
        cmd.get("side") or ""
    ).upper().strip()

    volume = float(cmd.get("volume") or 0)
    sl = cmd.get("sl")
    tp = cmd.get("tp")
    comment = cmd.get("comment") or "XTL"

    if not canonical_symbol or volume <= 0:
        return {
            "ok": False,
            "error": "invalid_symbol_or_volume",
            "symbol": canonical_symbol,
            "volume": volume,
        }

    # Resolve XTL canonical symbol to the symbol exposed by this broker.
    # CTI examples:
    # EURUSD -> EURUSDC
    # XAUUSD -> XAUUSDC
    broker_symbol = str(
        _resolve_broker_symbol(canonical_symbol)
        or ""
    ).strip()

    if not broker_symbol:
        return {
            "ok": False,
            "error": f"broker_symbol_not_found:{canonical_symbol}",
            "symbol": canonical_symbol,
        }

    if not mt5.symbol_select(broker_symbol, True):
        return {
            "ok": False,
            "error": f"symbol_select_failed:{broker_symbol}",
            "symbol": canonical_symbol,
            "broker_symbol": broker_symbol,
            "mt5_last_error": mt5.last_error(),
        }

    symbol_info = mt5.symbol_info(broker_symbol)

    if symbol_info is None:
        return {
            "ok": False,
            "error": f"symbol_info_failed:{broker_symbol}",
            "symbol": canonical_symbol,
            "broker_symbol": broker_symbol,
            "mt5_last_error": mt5.last_error(),
        }

    try:
        type_filling = _resolve_market_filling_mode(
            mt5,
            symbol_info,
        )
    except Exception as exc:
        return {
            "ok": False,
            "error": str(exc),
            "symbol": canonical_symbol,
            "broker_symbol": broker_symbol,
            "filling_flags": int(
                getattr(symbol_info, "filling_mode", 0) or 0
            ),
            "execution_mode": int(
                getattr(symbol_info, "trade_exemode", -1)
            ),
            "mt5_last_error": mt5.last_error(),
        }

    tick = mt5.symbol_info_tick(broker_symbol)

    if not tick:
        return {
            "ok": False,
            "error": f"no_tick:{broker_symbol}",
            "symbol": canonical_symbol,
            "broker_symbol": broker_symbol,
            "mt5_last_error": mt5.last_error(),
        }

    try:
        log.warning(
            "[AGENT] SYMBOL_RESOLVED "
            "job_id=%s requested=%s broker=%s",
            cmd.get("job_id"),
            canonical_symbol,
            broker_symbol,
        )

        log.warning(
            "[AGENT] FILLING_MODE_RESOLVED "
            "job_id=%s requested=%s broker=%s "
            "filling_flags=%s execution_mode=%s "
            "request_filling=%s",
            cmd.get("job_id"),
            canonical_symbol,
            broker_symbol,
            getattr(symbol_info, "filling_mode", None),
            getattr(symbol_info, "trade_exemode", None),
            type_filling,
        )
    except Exception:
        pass

    # All MT5 operations below must use the broker-native symbol.
    symbol = broker_symbol

    if side == "BUY":
        order_type = mt5.ORDER_TYPE_BUY
        price = tick.ask
    elif side == "SELL":
        order_type = mt5.ORDER_TYPE_SELL
        price = tick.bid
    else:
        return {
            "ok": False,
            "error": f"invalid_side:{side}",
            "symbol": canonical_symbol,
            "broker_symbol": broker_symbol,
        }

    request = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": symbol,
        "volume": volume,
        "type": order_type,
        "price": price,
        "sl": float(sl) if sl else 0.0,
        "tp": float(tp) if tp else 0.0,
        "deviation": 20,
        "magic": 20251227,
        "comment": comment,
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": type_filling,
    }
    # ------------------------------------------------------------
    # Broker-native margin pre-check before order_send.
    # ------------------------------------------------------------
    try:
        log.warning(
            "[AGENT] ORDER_MARGIN_START "
            "job_id=%s trade_id=%s profile=%s "
            "symbol=%s broker_symbol=%s side=%s volume=%s price=%s",
            cmd.get("job_id"),
            cmd.get("trade_id"),
            cmd.get("profile_id"),
            canonical_symbol,
            broker_symbol,
            side,
            volume,
            price,
        )
    except Exception:
        pass

    margin = mt5_calc_order_margin(
        symbol=symbol,
        side=side,
        volume=volume,
        price=price,
    )

    if not isinstance(margin, dict) or not margin.get("ok"):
        try:
            log.warning(
                "[AGENT] ORDER_MARGIN_ERROR job_id=%s trade_id=%s profile=%s symbol=%s side=%s volume=%s reason=%s margin=%s",
                cmd.get("job_id"),
                cmd.get("trade_id"),
                cmd.get("profile_id"),
                symbol,
                side,
                volume,
                margin.get("error") if isinstance(margin, dict) else "margin_not_dict",
                margin,
            )
        except Exception:
            pass

        return {
            "ok": False,
            "error": "MARGIN_CALC_FAILED",
            "symbol": canonical_symbol,
            "broker_symbol": broker_symbol,
            "margin": margin,
        }

    if not bool(margin.get("enough_margin")):
        try:
            log.warning(
                "[AGENT] ORDER_MARGIN_BLOCK job_id=%s trade_id=%s profile=%s symbol=%s broker_symbol=%s side=%s volume=%s required=%s free=%s shortfall=%s balance=%s equity=%s reason=INSUFFICIENT_FREE_MARGIN",
                cmd.get("job_id"),
                cmd.get("trade_id"),
                cmd.get("profile_id"),
                symbol,
                margin.get("broker_symbol"),
                side,
                volume,
                margin.get("required_margin"),
                margin.get("free_margin"),
                margin.get("shortfall"),
                margin.get("balance"),
                margin.get("equity"),
            )
        except Exception:
            pass

        return {
            "ok": False,
            "error": "INSUFFICIENT_FREE_MARGIN",
            "symbol": canonical_symbol,
            "broker_symbol": broker_symbol,
            "margin": margin,
        }

    try:
        log.warning(
            "[AGENT] ORDER_MARGIN_PASS job_id=%s trade_id=%s profile=%s symbol=%s broker_symbol=%s side=%s volume=%s required=%s free=%s remaining=%s balance=%s equity=%s leverage=%s",
            cmd.get("job_id"),
            cmd.get("trade_id"),
            cmd.get("profile_id"),
            symbol,
            margin.get("broker_symbol"),
            side,
            volume,
            margin.get("required_margin"),
            margin.get("free_margin"),
            margin.get("remaining_margin"),
            margin.get("balance"),
            margin.get("equity"),
            margin.get("leverage"),
        )
    except Exception:
        pass
    result = mt5.order_send(request)

    if not result:
        return {
            "ok": False,
            "error": "order_send_none",
            "symbol": canonical_symbol,
            "broker_symbol": broker_symbol,
            "mt5_last_error": mt5.last_error(),
            "request": request,
        }

    success_retcodes = {
        mt5.TRADE_RETCODE_DONE,
        mt5.TRADE_RETCODE_PLACED,
    }

    if result.retcode not in success_retcodes:
        return {
            "ok": False,
            "error": f"retcode:{result.retcode}",
            "symbol": canonical_symbol,
            "broker_symbol": broker_symbol,
            "comment": getattr(result, "comment", ""),
            "retcode": getattr(result, "retcode", None),
            "deal": getattr(result, "deal", None),
            "order": getattr(result, "order", None),
            "price": getattr(result, "price", None),
            "volume": getattr(result, "volume", None),
            "request_id": getattr(result, "request_id", None),
            "mt5_last_error": mt5.last_error(),
        }

    return {
        "ok": True,
        "ticket": int(
            getattr(result, "order", 0)
            or getattr(result, "deal", 0)
            or 0
        ),
        "order": int(getattr(result, "order", 0) or 0),
        "deal": int(getattr(result, "deal", 0) or 0),
        "retcode": int(getattr(result, "retcode", 0) or 0),
        "price": float(getattr(result, "price", 0.0) or 0.0),
        "volume": float(getattr(result, "volume", 0.0) or 0.0),

        # Keep XTL and broker identities separately.
        "symbol": canonical_symbol,
        "broker_symbol": broker_symbol,

        "margin": margin,
    }


@mt5_locked
def _mt5_modify_position_sltp(cmd: dict) -> dict:
    """
    Modify SL/TP for one existing MT5 position by exact broker ticket.

    Used by XTL Position Manager for broker-side break-even protection.
    This function does not calculate the BE trigger; it only applies the
    already-approved SL/TP modification.

    Expected command keys:
      type="modify_position_sltp"
      ticket=<broker position ticket>
      sl=<new SL>
      tp=<optional new TP; when omitted, preserve broker TP>
      mt5_account="demo"|"live"
    """
    try:
        import MetaTrader5 as mt5
    except Exception as e:
        return {"ok": False, "error": f"mt5_import_failed:{e}"}

    # Same demo/live safety boundary used by market-order execution.
    expected_acct = str(cmd.get("mt5_account") or "demo").strip().lower()
    try:
        ai = mt5.account_info()
    except Exception:
        ai = None

    if not ai:
        return {
            "ok": False,
            "error": f"account_info_none:{mt5.last_error()}",
        }

    try:
        tm_i = int(getattr(ai, "trade_mode", -1))
    except Exception:
        tm_i = -1

    if expected_acct == "demo" and tm_i not in (0, 1):
        return {
            "ok": False,
            "error": f"acct_guard_expected_demo_got:{tm_i}",
        }
    if expected_acct == "live" and tm_i != 2:
        return {
            "ok": False,
            "error": f"acct_guard_expected_live_got:{tm_i}",
        }

    try:
        ticket = int(cmd.get("ticket") or 0)
    except Exception:
        ticket = 0

    if ticket <= 0:
        return {"ok": False, "error": "missing_ticket"}

    try:
        positions = mt5.positions_get(ticket=ticket)
    except Exception as e:
        return {
            "ok": False,
            "error": f"positions_get_exc:{type(e).__name__}:{e}",
            "ticket": ticket,
        }

    if not positions:
        return {
            "ok": False,
            "error": "position_not_found",
            "ticket": ticket,
        }

    pos = positions[0]
    symbol = str(getattr(pos, "symbol", "") or "").strip()
    if not symbol:
        return {
            "ok": False,
            "error": "missing_symbol",
            "ticket": ticket,
        }

    info = mt5.symbol_info(symbol)
    if info is None:
        return {
            "ok": False,
            "error": f"symbol_info_failed:{symbol}",
            "ticket": ticket,
        }

    try:
        digits = int(getattr(info, "digits", 0) or 0)
    except Exception:
        digits = 0
        
    try:
        point = float(
            getattr(info, "point", 0.0)
            or 0.0
        )
    except Exception:
        point = 0.0

    try:
        trade_tick_size = float(
            getattr(info, "trade_tick_size", 0.0)
            or 0.0
        )
    except Exception:
        trade_tick_size = 0.0

    if trade_tick_size <= 0:
        trade_tick_size = point

    def _normalize_broker_price(value: float) -> float:
        px = float(value)

        if trade_tick_size > 0:
            px = (
                round(px / trade_tick_size)
                * trade_tick_size
            )

        if digits >= 0:
            px = round(px, digits)
        else:
            px = round(px, 10)

        return float(px)

    try:
        current_sl = float(getattr(pos, "sl", 0.0) or 0.0)
        current_tp = float(getattr(pos, "tp", 0.0) or 0.0)
        price_open = float(getattr(pos, "price_open", 0.0) or 0.0)
        ptype = int(getattr(pos, "type", -1))
    except Exception as e:
        return {
            "ok": False,
            "error": f"position_fields_invalid:{type(e).__name__}:{e}",
            "ticket": ticket,
            "symbol": symbol,
        }

    try:
        requested_sl = float(cmd.get("sl"))
    except Exception:
        requested_sl = 0.0

    if requested_sl <= 0:
        return {
            "ok": False,
            "error": "invalid_sl",
            "ticket": ticket,
            "symbol": symbol,
        }

    raw_tp = cmd.get("tp")
    if raw_tp in (None, ""):
        requested_tp = current_tp
    else:
        try:
            requested_tp = float(raw_tp or 0.0)
        except Exception:
            return {
                "ok": False,
                "error": "invalid_tp",
                "ticket": ticket,
                "symbol": symbol,
            }

    new_sl = _normalize_broker_price(
        requested_sl
    )

    new_tp = (
        _normalize_broker_price(requested_tp)
        if requested_tp > 0
        else 0.0
    )
    
    

    # Fail-safe: Position Manager may tighten protection, never loosen it.
    eps = 10 ** (-(digits or 8)) if digits >= 0 else 1e-8
    if ptype == mt5.POSITION_TYPE_BUY:
        if current_sl > 0 and new_sl < current_sl - eps:
            return {
                "ok": False,
                "error": "refuse_loosen_buy_sl",
                "ticket": ticket,
                "symbol": symbol,
                "current_sl": current_sl,
                "requested_sl": new_sl,
            }
    elif ptype == mt5.POSITION_TYPE_SELL:
        if current_sl > 0 and new_sl > current_sl + eps:
            return {
                "ok": False,
                "error": "refuse_loosen_sell_sl",
                "ticket": ticket,
                "symbol": symbol,
                "current_sl": current_sl,
                "requested_sl": new_sl,
            }
    else:
        return {
            "ok": False,
            "error": f"bad_position_type:{ptype}",
            "ticket": ticket,
            "symbol": symbol,
        }

    # No-op is a successful idempotent result. This prevents duplicate retries
    # from turning an already-applied BE shift into a failure.
    if abs(current_sl - new_sl) <= eps and abs(current_tp - new_tp) <= eps:
        return {
            "ok": True,
            "already_applied": True,
            "ticket": ticket,
            "symbol": symbol,
            "price_open": price_open,
            "old_sl": current_sl,
            "new_sl": current_sl,
            "old_tp": current_tp,
            "new_tp": current_tp,
            "retcode": None,
        }

    request = {
        "action": mt5.TRADE_ACTION_SLTP,
        "position": ticket,
        "symbol": symbol,
        "sl": float(new_sl),
        "tp": float(new_tp),
        "magic": 20251227,
        "comment": str(cmd.get("comment") or "XTL BE")[:31],
    }

    try:
        result = mt5.order_send(request)
    except Exception as e:
        return {
            "ok": False,
            "error": f"sltp_order_send_exc:{type(e).__name__}:{e}",
            "ticket": ticket,
            "symbol": symbol,
            "request": request,
        }

    if not result:
        return {
            "ok": False,
            "error": "sltp_order_send_none",
            "ticket": ticket,
            "symbol": symbol,
            "request": request,
            "mt5_last_error": mt5.last_error(),
        }

    retcode = int(getattr(result, "retcode", -1) or -1)
    no_changes = int(getattr(mt5, "TRADE_RETCODE_NO_CHANGES", 10025))
    success_retcodes = {
        int(mt5.TRADE_RETCODE_DONE),
        no_changes,
    }
    ok = retcode in success_retcodes
    # ---------------------------------------------------------
    # SL/TP observability:
    #
    # MT5 accepting TRADE_ACTION_SLTP is not the same evidence
    # as observing the resulting broker position state.
    #
    # Re-read the exact position immediately after a successful
    # modification so the ACK contains:
    #   requested -> sent -> broker-confirmed
    #
    # Verification failure is observational only. A successful
    # MT5 retcode remains successful; Position Manager still uses
    # subsequent broker snapshots as the final source of truth.
    # ---------------------------------------------------------
    broker_verified = False
    broker_confirmed_sl = None
    broker_confirmed_tp = None
    broker_verify_error = None

    if ok:
        try:
            verify_positions = mt5.positions_get(
                ticket=ticket
            )

            if verify_positions:
                verify_pos = verify_positions[0]

                broker_confirmed_sl = float(
                    getattr(
                        verify_pos,
                        "sl",
                        0.0,
                    )
                    or 0.0
                )

                broker_confirmed_tp = float(
                    getattr(
                        verify_pos,
                        "tp",
                        0.0,
                    )
                    or 0.0
                )

                verify_eps = max(
                    (
                        trade_tick_size * 0.5
                        if trade_tick_size > 0
                        else 0.0
                    ),
                    (
                        10 ** (-(digits or 8))
                        if digits >= 0
                        else 1e-8
                    ),
                    1e-12,
                )

                broker_verified = bool(
                    abs(
                        broker_confirmed_sl
                        - new_sl
                    )
                    <= verify_eps
                    and abs(
                        broker_confirmed_tp
                        - new_tp
                    )
                    <= verify_eps
                )

                if not broker_verified:
                    broker_verify_error = (
                        "BROKER_STATE_MISMATCH"
                    )

            else:
                broker_verify_error = (
                    "POSITION_NOT_FOUND_AFTER_SLTP"
                )

        except Exception as verify_exc:
            broker_verify_error = (
                f"{type(verify_exc).__name__}:"
                f"{verify_exc}"
            )

    if ok:
        log.warning(
            "[AGENT] POSITION_SLTP_MODIFIED "
            "job_id=%s ticket=%s symbol=%s "
            "price_open=%s "
            "old_sl=%s requested_sl=%s sent_sl=%s "
            "confirmed_sl=%s "
            "old_tp=%s sent_tp=%s confirmed_tp=%s "
            "tick_size=%s digits=%s "
            "retcode=%s broker_verified=%s "
            "verify_error=%s",
            cmd.get("job_id"),
            ticket,
            symbol,
            price_open,
            current_sl,
            requested_sl,
            new_sl,
            broker_confirmed_sl,
            current_tp,
            new_tp,
            broker_confirmed_tp,
            trade_tick_size,
            digits,
            retcode,
            broker_verified,
            broker_verify_error,
        )
    else:
        log.warning(
            "[AGENT] POSITION_SLTP_MODIFY_FAILED "
            "job_id=%s ticket=%s symbol=%s old_sl=%s new_sl=%s "
            "old_tp=%s new_tp=%s retcode=%s comment=%s",
            cmd.get("job_id"),
            ticket,
            symbol,
            current_sl,
            new_sl,
            current_tp,
            new_tp,
            retcode,
            str(getattr(result, "comment", "") or ""),
        )
    return {
        "ok": bool(ok),
        "ticket": ticket,
        "symbol": symbol,
        "price_open": price_open,

        # Requested by Position Manager.
        "requested_sl": float(requested_sl),

        # Broker-grid-normalized value actually sent to MT5.
        "old_sl": float(current_sl),
        "new_sl": float(new_sl),
        "sent_sl": float(new_sl),

        "old_tp": float(current_tp),
        "new_tp": float(new_tp),
        "sent_tp": float(new_tp),

        # Immediate broker position read after successful SLTP request.
        "broker_verified": bool(
            broker_verified
        ),
        "broker_confirmed_sl": (
            float(broker_confirmed_sl)
            if broker_confirmed_sl is not None
            else None
        ),
        "broker_confirmed_tp": (
            float(broker_confirmed_tp)
            if broker_confirmed_tp is not None
            else None
        ),
        "broker_verify_error": (
            broker_verify_error
        ),

        "trade_tick_size": float(
            trade_tick_size or 0.0
        ),
        "point": float(point or 0.0),
        "digits": int(digits),

        "retcode": retcode,
        "comment": str(
            getattr(result, "comment", "")
            or ""
        ),
        "request_id": int(
            getattr(result, "request_id", 0)
            or 0
        ),
        "mt5_last_error": (
            mt5.last_error()
            if not ok
            else None
        ),
    }

@mt5_locked
def _mt5_close_position(cmd: dict) -> dict:
    """
    Close an open MT5 position by *ticket* (works for hedging AND netting).
    Command expected:
      { "type":"close_position", "ticket":123, "symbol":"EURUSD" (optional), "deviation":20 (optional) }
    """
    try:
        import MetaTrader5 as mt5
    except Exception as e:
        return {"ok": False, "error": f"mt5_import_failed:{e}"}

    try:
        ticket = int(cmd.get("ticket") or 0)
    except Exception:
        ticket = 0
    if ticket <= 0:
        return {"ok": False, "error": "missing_ticket"}

    deviation = int(cmd.get("deviation") or 20)

    pos = None
    try:
        ps = mt5.positions_get(ticket=ticket)
        if ps and len(ps) > 0:
            pos = ps[0]
    except Exception:
        pos = None

    if not pos:
        return {"ok": False, "error": "position_not_found", "ticket": ticket}

    symbol = str(getattr(pos, "symbol", "") or (cmd.get("symbol") or "")).upper()
    if not symbol:
        return {"ok": False, "error": "missing_symbol", "ticket": ticket}

    vol = float(getattr(pos, "volume", 0.0) or 0.0)
    if vol <= 0:
        return {"ok": False, "error": "bad_volume", "ticket": ticket, "symbol": symbol}

    # Determine close side (opposite of position type)
    # mt5.POSITION_TYPE_BUY / mt5.POSITION_TYPE_SELL
    ptype = int(getattr(pos, "type", -1))
    if ptype == mt5.POSITION_TYPE_BUY:
        order_type = mt5.ORDER_TYPE_SELL
    elif ptype == mt5.POSITION_TYPE_SELL:
        order_type = mt5.ORDER_TYPE_BUY
    else:
        return {
            "ok": False,
            "error": f"bad_position_type:{ptype}",
            "ticket": ticket,
            "symbol": symbol,
        }

    tick = mt5.symbol_info_tick(symbol)
    if not tick:
        return {"ok": False, "error": "no_tick", "ticket": ticket, "symbol": symbol}

    price = float(tick.bid) if order_type == mt5.ORDER_TYPE_SELL else float(tick.ask)
    symbol_info = mt5.symbol_info(symbol)

    if symbol_info is None:
        return {
            "ok": False,
            "error": f"symbol_info_failed:{symbol}",
            "ticket": ticket,
        }

    try:
        type_filling = _resolve_market_filling_mode(
            mt5,
            symbol_info,
        )
    except Exception as exc:
        return {
            "ok": False,
            "error": str(exc),
            "ticket": ticket,
            "symbol": symbol,
        }

    req = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": symbol,
        "volume": vol,
        "type": order_type,
        "position": ticket,  # <-- critical: closes this specific position ticket
        "price": price,
        "deviation": deviation,
        "comment": "XTL close_position",
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": type_filling,
    }

    try:
        r = mt5.order_send(req)
    except Exception as e:
        return {
            "ok": False,
            "error": f"order_send_exc:{e}",
            "ticket": ticket,
            "symbol": symbol,
        }

    if not r:
        return {
            "ok": False,
            "error": "order_send_none",
            "ticket": ticket,
            "symbol": symbol,
            "last_error": str(mt5.last_error()),
        }

    # retcode 10009 / 10008 are common success codes depending on broker
    ret = int(getattr(r, "retcode", -1))
    ok = ret in (mt5.TRADE_RETCODE_DONE, mt5.TRADE_RETCODE_PLACED)

    return {
        "ok": bool(ok),
        "ticket": ticket,
        "symbol": symbol,
        "volume": vol,
        "close_type": "SELL" if order_type == mt5.ORDER_TYPE_SELL else "BUY",
        "price": price,
        "retcode": ret,
        "comment": str(getattr(r, "comment", "") or ""),
        "request_id": int(getattr(r, "request_id", 0) or 0),
    }


# -------------------------------------------------------------------
# Permanent worker supervision
# -------------------------------------------------------------------
_WORKER_HEALTH_LOCK = threading.RLock()
_WORKER_HEALTH: dict[str, dict] = {}
_WORKER_SPECS: dict[str, tuple] = {}
_SUPERVISOR_STARTED = False


def _worker_mark(name: str, event: str, **extra) -> None:
    now = time.monotonic()
    with _WORKER_HEALTH_LOCK:
        row = _WORKER_HEALTH.setdefault(name, {})
        row[event] = now
        row.update(extra)


def _start_supervised_thread(name: str, target, args: tuple):
    th = threading.Thread(target=target, args=args, name=name, daemon=True)
    with _WORKER_HEALTH_LOCK:
        _WORKER_SPECS[name] = (target, args)
        _WORKER_HEALTH[name] = {
            "thread": th,
            "started": time.monotonic(),
            "loop_started": 0.0,
            "loop_completed": time.monotonic(),
            "last_success": 0.0,
            "consecutive_failures": 0,
            "last_error": "",
        }
    th.start()
    return th


def _worker_supervisor_loop(check_sec: float = 2.0) -> None:
    # A blocked Python thread cannot be killed safely. For a critical stall,
    # terminate the Agent so the Windows service manager can restart it cleanly.
    critical_age = {
        "mt5-account-heartbeat": 60.0,
        "mt5-positions-heartbeat": 90.0,
        "ohlc-worker": 120.0,
        "mt5-cmd-worker": 90.0,
    }

    while True:
        time.sleep(max(1.0, float(check_sec)))
        now = time.monotonic()
        # --- MT5 presence: safe, rate-limited relaunch -----------------
        try:
            from mt5_launcher import try_launch_mt5
            from mt5_client import _mt5_running
            if not _mt5_running():
                try_launch_mt5()       # internally rate-limited
                _warn_ghost_datafolder()
        except Exception as e:
            log.warning(f"[mt5] supervisor launch check failed: {e}")

        with _WORKER_HEALTH_LOCK:
            snapshot = {k: dict(v) for k, v in _WORKER_HEALTH.items()}
            specs = dict(_WORKER_SPECS)

        for name, row in snapshot.items():
            th = row.get("thread")
            if th is not None and not th.is_alive():
                spec = specs.get(name)
                if spec is None:
                    continue
                log.error("WORKER_DEAD_RESTART name=%s", name)
                target, args = spec
                _start_supervised_thread(name, target, args)
                continue

            loop_started = float(row.get("loop_started") or 0.0)
            loop_completed = float(row.get("loop_completed") or 0.0)
            # Stuck means a cycle started and has not completed since.
            if loop_started > loop_completed:
                age = now - loop_started
                limit = float(critical_age.get(name, 120.0))
                if age >= limit:
                    log.critical(
                        "WORKER_STUCK_FATAL name=%s age_sec=%.1f limit_sec=%.1f "
                        "last_error=%s",
                        name,
                        age,
                        limit,
                        row.get("last_error") or "",
                    )
                    try:
                        for h in logging.getLogger().handlers:
                            h.flush()
                    except Exception:
                        pass
                    os._exit(70)


def _ensure_worker_supervisor() -> None:
    global _SUPERVISOR_STARTED
    with _WORKER_HEALTH_LOCK:
        if _SUPERVISOR_STARTED:
            return
        _SUPERVISOR_STARTED = True
    _warn_ghost_datafolder()
    threading.Thread(
        target=_worker_supervisor_loop,
        name="agent-worker-supervisor",
        daemon=True,
    ).start()


def start_ohlc_worker(
    api_base, device_id, token, symbols, tfs, bars=300, period_sec=10
):
    log.info(
        "OHLC: starting worker symbols=%s tfs=%s bars=%s every %ss",
        symbols, tfs, bars, period_sec,
    )
    _ensure_worker_supervisor()

    _start_supervised_thread(
        "mt5-account-heartbeat",
        _mt5_account_heartbeat_loop,
        (api_base, device_id, token, "demo", 5),
    )
    _start_supervised_thread(
        "mt5-positions-heartbeat",
        _mt5_positions_heartbeat_loop,
        (api_base, device_id, token, "demo", 10),
    )
    return _start_supervised_thread(
        "ohlc-worker",
        _ohlc_loop_target(),        
        (api_base, device_id, token, symbols, tfs, bars, period_sec),
    )


def start_mt5_cmd_worker(api_base, device_id, token, poll_sec=2):
    log.info("MT5 CMD: starting worker")
    _ensure_worker_supervisor()
    return _start_supervised_thread(
        "mt5-cmd-worker",
        _mt5_cmd_loop,
        (api_base, device_id, token, poll_sec),
    )


def _mt5_account_heartbeat_loop(
    api_base,
    device_id,
    token,
    mt5_account="demo",
    period_sec=5,
):
    name = threading.current_thread().name
    period = max(5.0, min(float(period_sec or 5), 15.0))
    next_run = time.monotonic()
    consecutive_failures = 0

    # One-shot broker symbol specification audit.
    # Runs once when this MT5 account worker starts.
    try:
        _log_mt5_symbol_specs_once()
    except Exception as exc:
        log.warning(
            "[MT5_SPEC] STARTUP_AUDIT_FAILED err=%s",
            exc,
        )

    while True:
        started = time.monotonic()
        _worker_mark(name, "loop_started", last_error="")
        ok = False
        try:
            ok = push_mt5_account_once(
                api_base, device_id, token, mt5_account=mt5_account
            )
            consecutive_failures = 0 if ok else consecutive_failures + 1
            if not ok:
                log.warning(
                    "MT5_ACCOUNT_HEARTBEAT_FAILED dev=%s acct=%s consecutive=%s",
                    device_id, mt5_account, consecutive_failures,
                )
        except Exception as exc:
            consecutive_failures += 1
            _worker_mark(name, "last_error_at", last_error=f"{type(exc).__name__}:{exc}")
            log.exception(
                "MT5_ACCOUNT_HEARTBEAT_EXC dev=%s acct=%s consecutive=%s",
                device_id, mt5_account, consecutive_failures,
            )
        finally:
            _worker_mark(
                name,
                "loop_completed",
                consecutive_failures=consecutive_failures,
                **({"last_success": time.monotonic()} if ok else {}),
            )

        log.info(
            "MT5_ACCOUNT_HEARTBEAT_DONE dev=%s acct=%s ok=%s took_ms=%s",
            device_id, mt5_account, ok,
            int((time.monotonic() - started) * 1000),
        )
        next_run += period
        delay = next_run - time.monotonic()
        if delay <= 0:
            next_run = time.monotonic() + period
            delay = period
        time.sleep(max(0.1, delay))


def _mt5_positions_heartbeat_loop(
    api_base,
    device_id,
    token,
    mt5_account="demo",
    period_sec=10,
):
    name = threading.current_thread().name
    period = max(5.0, min(float(period_sec or 10), 15.0))
    next_run = time.monotonic()
    consecutive_failures = 0

    while True:
        started = time.monotonic()
        _worker_mark(name, "loop_started", last_error="")
        ok = False
        try:
            ok = push_mt5_positions_once(
                api_base, device_id, token, mt5_account=mt5_account
            )
            consecutive_failures = 0 if ok else consecutive_failures + 1
            if not ok:
                log.warning(
                    "MT5_POS_HEARTBEAT_FAILED dev=%s acct=%s consecutive=%s",
                    device_id, mt5_account, consecutive_failures,
                )
        except Exception as exc:
            consecutive_failures += 1
            _worker_mark(name, "last_error_at", last_error=f"{type(exc).__name__}:{exc}")
            log.exception(
                "MT5_POS_HEARTBEAT_EXC dev=%s acct=%s consecutive=%s",
                device_id, mt5_account, consecutive_failures,
            )
        finally:
            _worker_mark(
                name,
                "loop_completed",
                consecutive_failures=consecutive_failures,
                **({"last_success": time.monotonic()} if ok else {}),
            )

        log.info(
            "MT5_POS_HEARTBEAT_DONE dev=%s acct=%s ok=%s took_ms=%s",
            device_id, mt5_account, ok,
            int((time.monotonic() - started) * 1000),
        )
        next_run += period
        delay = next_run - time.monotonic()
        if delay <= 0:
            next_run = time.monotonic() + period
            delay = period
        time.sleep(max(0.1, delay))


def _ohlc_loop(api_base, device_id, token, symbols, tfs, bars, period_sec):
    name = threading.current_thread().name
    period = max(1.0, float(period_sec or 10))
    next_run = time.monotonic()

    while True:
        started = time.monotonic()
        _worker_mark(name, "loop_started", last_error="")
        ok = False
        try:
            log.info("OHLC: tick begin")
            _push_ohlc_once_safe(api_base, device_id, token, symbols, tfs, bars)
            ok = True
            log.info("OHLC: tick done in %.2fs", time.monotonic() - started)
        except Exception as exc:
            _worker_mark(name, "last_error_at", last_error=f"{type(exc).__name__}:{exc}")
            log.exception("OHLC: tick exception")
        finally:
            _worker_mark(
                name,
                "loop_completed",
                **({"last_success": time.monotonic()} if ok else {}),
            )

        # Skip missed runs. Never hammer MT5 with zero-sleep catch-up cycles.
        next_run += period
        delay = next_run - time.monotonic()
        if delay <= 0:
            log.warning(
                "OHLC_SCHEDULE_OVERRUN took_ms=%s period_ms=%s skipped_catchup=1",
                int((time.monotonic() - started) * 1000),
                int(period * 1000),
            )
            next_run = time.monotonic() + period
            delay = period
        time.sleep(max(0.1, delay))


def _mt5_cmd_loop(api_base, device_id, token, poll_sec):
    name = threading.current_thread().name
    while True:
        _worker_mark(name, "loop_started", last_error="")
        try:
            r = api_get(api_base, f"/devices/{device_id}/mt5/next", token)
            _worker_mark(name, "loop_completed", last_success=time.monotonic())
            if getattr(r, "status_code", 0) != 200:
                time.sleep(poll_sec)
                continue

            data = r.json() if hasattr(r, "json") else {}
            cmd = data.get("cmd")
            if not cmd:
                time.sleep(poll_sec)
                continue

            job_id = cmd.get("job_id")
            log.info(
                "MT5 CMD: got cmd job=%s type=%s sym=%s side=%s vol=%s",
                cmd.get("job_id"),
                cmd.get("type"),
                cmd.get("symbol"),
                cmd.get("side"),
                cmd.get("volume"),
            )
            expected_acct = cmd.get("mt5_account") or "demo"
            cmd_type = str(cmd.get("type") or "").strip().lower()

            if cmd_type == "market_order":
                try:
                    result = _mt5_send_market_order(cmd)
                except Exception as e:
                    log.exception(
                        "MT5 CMD: market_order exception job=%s err=%s",
                        job_id,
                        e,
                    )
                    result = {
                        "ok": False,
                        "error": (
                            f"market_order_exception:"
                            f"{type(e).__name__}:{e}"
                        ),
                    }

            elif cmd_type == "close_position":
                try:
                    result = _mt5_close_position(cmd)
                except Exception as e:
                    log.exception(
                        "MT5 CMD: close_position exception job=%s err=%s",
                        job_id,
                        e,
                    )
                    result = {
                        "ok": False,
                        "error": (
                            f"close_position_exception:"
                            f"{type(e).__name__}:{e}"
                        ),
                    }

            elif cmd_type == "modify_position_sltp":
                try:
                    result = _mt5_modify_position_sltp(cmd)
                except Exception as e:
                    log.exception(
                        "MT5 CMD: modify_position_sltp exception "
                        "job=%s err=%s",
                        job_id,
                        e,
                    )
                    result = {
                        "ok": False,
                        "error": (
                            f"modify_position_sltp_exception:"
                            f"{type(e).__name__}:{e}"
                        ),
                    }

            else:
                result = {
                    "ok": False,
                    "error": f"unknown_cmd_type:{cmd_type}",
                }

            if not isinstance(result, dict):
                result = {
                    "ok": False,
                    "error": f"bad_result_type:{type(result).__name__}",
                }

            log.info(
                "MT5 CMD: posting ack job=%s ok=%s err=%s",
                job_id,
                bool(result.get("ok")),
                result.get("error"),
            )
            # normalize result
            res = (
                result
                if isinstance(result, dict)
                else {"ok": False, "error": "bad_result"}
            )

            # ?? embed user_id INSIDE result (this is what backend stores)
            res["user_id"] = cmd.get("user_id")
            ack = {
                "job_id": job_id,
                "ok": bool(res.get("ok")),
                "mt5_account": expected_acct,  # NEW (echo back)
                "kind": cmd.get("kind"),  # NEW (optional)
                "symbol": cmd.get("symbol"),  # NEW (optional)
                "side": cmd.get("side"),  # NEW (optional)
                "result": res,
                "error": res.get("error"),
                "meta": res.get("meta"),
            }

            resp = api_post(
                api_base,
                f"/devices/{device_id}/mt5/ack",
                ack,
                token,
                timeout=10,
            )

            try:
                code = getattr(resp, "status_code", 0)
                body = (getattr(resp, "text", "") or "")[:500]
                log.info("MT5 ACK POST code=%s body=%s", code, body)
            except Exception:
                pass

        except Exception as e:
            _worker_mark(name, "loop_completed", last_error=f"{type(e).__name__}:{e}")
            log.warning("MT5 CMD loop error: %s", e)

        time.sleep(poll_sec)


def _push_ohlc_once_safe(api_base, device_id, token, symbols, tfs, bars):
    """
    Worker path: fetch `bars` CLOSED candles for each (symbol, tf),
    optionally attach the current forming candle as latest_bar (registry: IncludeLatest),
    de-dup on LAST CLOSED bar only, and POST via push_rates_batch(...)
    .
    """
    import time

    # --- ensure defaults exist (no-op \if already present) ---
    global _REG_STARTUP_DONE
    if not _REG_STARTUP_DONE:
        try:
            ensure_registry_defaults()
        except Exception:
            pass
        # --- new-install reset (based on ConfigVersion or env toggle) ---
        try:
            maybe_reset_registry_on_new_install()
        except Exception:
            pass
        _REG_STARTUP_DONE = True
    # --- include_latest from registry (service path has no CLI kw) ---
    reg_inc = (reg_get("IncludeLatest") or "0").strip() in (
        "1",
        "true",
        "TRUE",
        "yes",
        "YES",
    )
    include_latest = bool(reg_inc)

    # --- normalize API base ONCE and reuse ---
    base = (api_base or "").strip().rstrip("/")
    if base.lower().endswith("/api"):
        base = base[:-4]

    # --- merge CLI + Registry; allow empty CLI to fully defer to registry ---
    try:
        try:
            reg_syms, reg_tfs, _ = _agent_pull_cfg()
        except Exception:
            reg_syms = [
                s.strip().upper()
                for s in (reg_get("Symbols") or "").split(",")
                if s.strip()
            ]
            reg_tfs = [
                t.strip().upper()
                for t in (reg_get("Timeframes") or "").split(",")
                if t.strip()
            ]

        cli_syms = [s.strip().upper() for s in (symbols or []) if (s or "").strip()]
        cli_tfs = [
            str(tf or "").upper().strip() for tf in (tfs or []) if (tf or "").strip()
        ]

        # union while preserving order
        syms = list(dict.fromkeys((cli_syms or []) + (reg_syms or [])))

        # Timeframes: ignore registry list, rely on CLI or our fixed default.
        base_tf = [
            str(tf or "").upper().strip() for tf in (tfs or []) if (tf or "").strip()
        ]
        # If nothing was passed via CLI, use fixed worker plan:
        #  - M1 / M15 for short-term
        #  - H1 / H2 / H4 for horizon
        if not base_tf:
            base_tf = ["M1", "M15", "H1", "H2", "H4"]

        tflist = base_tf

    except Exception as e:
        log.warning("worker: registry merge failed (%s); using CLI only", e)
        syms = [s.strip().upper() for s in (symbols or []) if (s or "").strip()]
        base_tf = [
            str(tf or "").upper().strip() for tf in (tfs or []) if (tf or "").strip()
        ]
        if not base_tf:
            base_tf = ["M1", "M15", "H1", "H2", "H4"]
        tflist = base_tf

    # fallbacks if everything is empty
    if not syms:
        syms = list(DEFAULT_SYMBOLS)

    log.info(
        "worker plan: symbols=%s tfs=%s include_latest=%s", syms, tflist, include_latest
    )

    # ensure dedupe map exists
    try:
        _ = _last_sent_bar  # noqa: F401
    except NameError:
        globals()["_last_sent_bar"] = {}

    def _to_sec(t_any):
        try:
            t = int(t_any or 0)
            return (t // 1000) if t >= 1_000_000_000_000 else t  # ms?s else already s
        except Exception:
            return 0

    for sym in syms:
        sym_u = str(sym or "").upper().strip()

        
        # All normal trading symbols keep their configured timeframes.
        
        # DXY now supplies native M15 flow plus H1/H4 structural context.
        symbol_tfs = [tf for tf in tflist if tf in ("M15", "H1", "H4")] if sym_u == "DXY" else tflist
        for tf in symbol_tfs:
            # --- fetch with guard (closed bars + optional forming tail) ---
            try:
                tf_bars = 1500 if tf.upper() == "H1" else int(bars or 300)
                rates = mt5_fetch_rates(
                    sym, tf, count=tf_bars, include_latest=include_latest
                )
                n_raw = len(rates) if hasattr(rates, "__len__") else 0
                log.info("worker/fetch %s/%s -> %s rows", sym, tf, n_raw)
            except Exception as e:
                import traceback

                log.info(
                    "worker/fetch EXC %s/%s: %s\n%s", sym, tf, e, traceback.format_exc()
                )
                continue

            if not rates:
                log.info("worker: skip — empty fetch for %s/%s", sym, tf)
                continue

            # --- de-dup by LAST CLOSED bar (ignore a trailing forming bar) ---
            last_closed = next(
                (b for b in reversed(rates) if b.get("complete", True)), None
            )
            if not last_closed:
                log.info(
                    "worker: skip — no CLOSED bars for %s/%s (all forming?)", sym, tf
                )
                continue

            last_t_s = _to_sec(last_closed.get("t"))
            key = (sym_u, tf.upper())
            if _last_sent_bar.get(key) == last_t_s:
                log.debug(
                    "worker: up-to-date %s/%s (last_closed=%s)", sym, tf, last_t_s
                )
                continue

            # --- unified post (closed -> bars[], forming -> latest_bar) ---
            try:
                sent = push_rates_batch(
                    base,
                    device_id,
                    token,
                    sym,
                    tf,
                    rates,
                    include_latest=include_latest,
                    count=tf_bars,  # soft cap; push_rates_batch trims if needed
                )
                if sent:
                    _last_sent_bar[key] = last_t_s
                    pushed_closed = sum(1 for b in rates if b.get("complete", True))
                    log.info(
                        "worker: pushed %s CLOSED bars for %s/%s (last_closed=%s)",
                        pushed_closed,
                        sym,
                        tf,
                        last_t_s,
                    )
                else:
                    log.warning(
                        "worker: POST failed for %s/%s (push_rates_batch=False)",
                        sym,
                        tf,
                    )
            except Exception as e:
                log.warning("worker: post failed for %s/%s: %s", sym, tf, e)


def push_ohlc_once(
    api_base: str,
    device_id: str,
    token: str,
    symbols: list[str] | None = None,
    tfs: list[str] | None = None,
    bars: int = 300,
    **kw,
) -> None:
    """
    Fetch OHLC for each symbol/tf,
    optionally attach the current forming candle as latest_bar (registry: IncludeLatest),
    de-dup on LAST CLOSED bar only, and POST via push_rates_batch(.).
    """
    import time

    # We ALWAYS want the full basket here, independent of registry / HB hint:
    #  - M1: live / dashboard / preview
    #  - M15: model update cadence
    #  - H1 / H2 / H4: horizon for prediction meter
    FIXED_TFS = ["M1", "M15", "H1", "H4"]

    # --- ensure defaults exist (no-op if already present) ---
    try:
        ensure_registry_defaults()
    except Exception:
        pass
    # --- new-install reset (based on ConfigVersion or env toggle) ---
    try:
        maybe_reset_registry_on_new_install()
    except Exception:
        pass

    # --- include_latest from registry (service path has no CLI kw) ---
    reg_inc = (reg_get("IncludeLatest") or "0").strip() in (
        "1",
        "true",
        "TRUE",
        "yes",
        "YES",
    )
    include_latest = bool(reg_inc)

    # --- normalize API base ONCE and reuse ---
    base = (api_base or "").strip().rstrip("/")
    if base.lower().endswith("/api"):
        base = base[:-4]

    # --- resolve symbols from CLI + registry; IGNORE any timeframe hints ---
    try:
        try:
            reg_syms, reg_tfs, _ = _agent_pull_cfg()
        except Exception:
            reg_syms = [
                s.strip().upper()
                for s in (reg_get("Symbols") or "").split(",")
                if s.strip()
            ]

        cli_syms = [s.strip().upper() for s in (symbols or []) if (s or "").strip()]

        # union while preserving order
        syms = list(dict.fromkeys((cli_syms or []) + (reg_syms or [])))

        if not syms:
            syms = list(DEFAULT_SYMBOLS)

    except Exception as e:
        log.warning("normalize inputs failed; using defaults (%s)", e)
        syms = list(DEFAULT_SYMBOLS)

    # Timeframes: hard-wire our basket; do NOT depend on registry or CLI tfs
    tflist = FIXED_TFS[:]

    log.info(
        "OHLC plan: symbols=%s tfs=%s bars=%s include_latest=%s",
        syms,
        tflist,
        bars,
        include_latest,
    )

    # --- helper for dedupe key (seconds) ---
    def _to_sec(t_any):
        try:
            t = int(t_any or 0)
            return (t // 1000) if t >= 1_000_000_000_000 else t  # ms?s else already s
        except Exception:
            return 0

    total_pushed = 0
    for s in syms:
        for tfu in tflist:
            # fetch CLOSED bars (+ tail if include_latest=True)
            try:
                tf_count = 1500 if str(tfu).upper() == "H1" else int(bars or 300)
                arr_raw = mt5_fetch_rates(
                    s, tfu, count=tf_count, include_latest=include_latest
                )
                n_raw = len(arr_raw) if hasattr(arr_raw, "__len__") else 0
                log.info("OHLC fetch: %s/%s -> %s rows", s, tfu, n_raw)
            except Exception as e:
                import traceback

                log.error(
                    "OHLC: fetch crash %s/%s: %s\n%s", s, tfu, e, traceback.format_exc()
                )
                continue

            if not arr_raw:
                log.info("OHLC: skip — empty fetch for %s/%s", s, tfu)
                continue

            # de-dup by LAST CLOSED bar (ignore any trailing forming bar)
            last_closed = next(
                (b for b in reversed(arr_raw) if b.get("complete", True)), None
            )
            if not last_closed:
                log.info("OHLC: skip — no CLOSED bar for %s/%s", s, tfu)
                continue

            last_t = _to_sec(last_closed.get("t"))
            key = (s, tfu)
            prev = _last_sent_bar.get(key)
            if prev and prev >= last_t and not kw.get("force"):
                log.info(
                    "OHLC: skip — already sent last_closed=%s for %s/%s", prev, s, tfu
                )
                continue

            # POST the batch
            try:
                sent = push_rates_batch(
                    base,
                    device_id,
                    token,
                    s,
                    tfu,
                    arr_raw,
                    include_latest=include_latest,
                )

                if sent:
                    _last_sent_bar[key] = last_t
                    total_pushed += 1
                else:
                    log.warning("OHLC: POST skipped/failed for %s/%s", s, tfu)
            except Exception as e:
                import traceback

                log.error(
                    "OHLC: POST crash %s/%s: %s\n%s", s, tfu, e, traceback.format_exc()
                )
                continue

    log.info("OHLC: push_once done; total series posted=%s", total_pushed)

# ============================================================================
# XTL PHASE 1 PATCH — append this entire block to the END of agent_ohlc.py
# Implements: 4.6 canonical writer, 4.1 bar-close-aligned scheduler,
#             4.2 delta-capable event loop (delta OFF by default, see guide).
# Enable with registry value  Agent.EventDriven = 1  (fallback: legacy loop).
# ============================================================================

# ---------------------------------------------------------------------------
# 4.6 — CANONICAL SYMBOL WRITER
# The ONLY place instrument names are normalized before leaving the agent.
# Broker-native names travel only in explicit broker_symbol fields.
# ---------------------------------------------------------------------------
_CANON_BASES = (
    "XAUUSD", "EURUSD", "GBPUSD", "USDJPY", "USDCHF", "USDCAD",
)
_DXY_PREFIXES = ("DXY", "USDX", "USDINDEX")


def canonical_symbol(s) -> str:
    """EURUSDC -> EURUSD, XAUUSD.a -> XAUUSD, DXY.cash -> DXY. Unknown -> unchanged."""
    u = str(s or "").upper().strip()
    if not u:
        return u
    for b in _CANON_BASES:
        if u == b or u.startswith(b):
            return b
    for p in _DXY_PREFIXES:
        if u.startswith(p):
            return "DXY"
    return u


def canonicalize_outbound(obj):
    """
    Normalize the 'symbol' field of a dict (or list of dicts) in place,
    preserving the broker name in 'broker_symbol'. Returns the same object.
    """
    def _one(d):
        if not isinstance(d, dict):
            return
        raw = str(d.get("symbol") or "")
        if not raw:
            return
        canon = canonical_symbol(raw)
        if canon != raw:
            d.setdefault("broker_symbol", raw)
            d["symbol"] = canon
    if isinstance(obj, dict):
        _one(obj)
    elif isinstance(obj, (list, tuple)):
        for it in obj:
            _one(it)
    return obj


# ---------------------------------------------------------------------------
# 4.1 — BAR-CLOSE-ALIGNED SCHEDULER + EVENT LOOP
# Fires per-TF shortly after each broker-grid bar boundary. Between boundaries
# the agent sleeps. Weekend/idle markets are detected and skipped cheaply.
# ---------------------------------------------------------------------------
_TF_SEC = {"M1": 60, "M15": 900, "H1": 3600, "H2": 7200, "H4": 14400}

_EVT_FULL_LAST: dict = {}          # (sym, tf) -> monotonic ts of last full-history push
_REG_STARTUP_DONE = False          # 4.5: registry defaults run once, not per tick


def _reg_float(name: str, default: float) -> float:
    try:
        v = (reg_get(name) or "").strip()
        return float(v) if v else float(default)
    except Exception:
        return float(default)


def _reg_flag(name: str, default: bool = False) -> bool:
    try:
        v = (reg_get(name) or "").strip().lower()
        if not v:
            return bool(default)
        return v in ("1", "true", "yes", "on")
    except Exception:
        return bool(default)


def _broker_off_ms_safe() -> int:
    """Broker offset in ms for boundary math. Never raises; 0 on unknown."""
    try:
        from . import mt5_client as _mc
    except Exception:
        import mt5_client as _mc  # type: ignore
    try:
        return int(_mc._broker_offset_min()) * 60_000
    except Exception:
        return 0


def _next_fire_ms(tf_sec: int, now_ms: int, off_ms: int, grace_ms: int) -> int:
    """Next bar boundary on the broker grid, expressed in UTC ms, plus grace."""
    tf_ms = tf_sec * 1000
    boundary = (((now_ms + off_ms) // tf_ms) + 1) * tf_ms - off_ms
    return boundary + grace_ms


def _market_idle(threshold_s: float = 600.0) -> bool:
    """True when no symbol has ticked within threshold (weekend / holiday)."""
    try:
        from . import mt5_client as _mc
    except Exception:
        import mt5_client as _mc  # type: ignore
    try:
        import MetaTrader5 as MT5
        newest = 0
        for base in ("EURUSD", "XAUUSD"):
            try:
                t = MT5.symbol_info_tick(_mc._resolve_broker_symbol(base))
                newest = max(newest, int(getattr(t, "time", 0) or 0))
            except Exception:
                continue
        if newest <= 0:
            return False           # unknown -> assume open (fail-safe)
        return (time.time() - newest) > threshold_s
    except Exception:
        return False


def _push_ohlc_for_tfs(api_base, device_id, token, symbols, only_tfs, bars,
                       include_latest, delta_on, delta_bars, full_refresh_s):
    """
    One event's worth of work: fetch + push ONLY the timeframes that just
    closed a bar. Same dedupe and push_rates_batch path as the legacy worker.
    """
    base = (api_base or "").strip().rstrip("/")
    if base.lower().endswith("/api"):
        base = base[:-4]

    try:
        _ = _last_sent_bar  # noqa: F841
    except NameError:
        globals()["_last_sent_bar"] = {}

    def _to_sec(t_any):
        try:
            t = int(t_any or 0)
            return (t // 1000) if t >= 1_000_000_000_000 else t
        except Exception:
            return 0

    now_mono = time.monotonic()
    for sym in symbols:
        sym_u = str(sym or "").upper().strip()
        sym_tfs = [tf for tf in only_tfs if str(tf).upper() in ("M15", "H1", "H4")] \
            if sym_u == "DXY" else only_tfs
        for tf in sym_tfs:
            tf_u = str(tf).upper()
            key = (sym_u, tf_u)

            # ---- 4.2 count selection: delta window vs full history ----
            full_count = 1500 if tf_u == "H1" else int(bars or 300)
            if delta_on:
                last_full = _EVT_FULL_LAST.get(key, 0.0)
                if (now_mono - last_full) >= full_refresh_s:
                    tf_count, is_full = full_count, True
                else:
                    tf_count, is_full = max(2, int(delta_bars)), False
            else:
                tf_count, is_full = full_count, True

            try:
                rates = mt5_fetch_rates(
                    sym, tf_u, count=tf_count, include_latest=include_latest
                )
            except Exception as e:
                log.warning("EVT fetch failed %s/%s: %s", sym_u, tf_u, e)
                continue
            if not rates:
                continue

            last_closed = next(
                (b for b in reversed(rates) if b.get("complete", True)), None
            )
            if not last_closed:
                continue
            last_t_s = _to_sec(last_closed.get("t"))
            if _last_sent_bar.get(key) == last_t_s:
                continue  # boundary fired but broker not finalized yet; sweep will catch

            try:
                sent = push_rates_batch(
                    base, device_id, token, sym, tf_u, rates,
                    include_latest=include_latest, count=tf_count,
                )
                if sent:
                    _last_sent_bar[key] = last_t_s
                    if is_full:
                        _EVT_FULL_LAST[key] = now_mono
                    log.info("EVT pushed %s/%s bars=%s last_closed=%s%s",
                             sym_u, tf_u, len(rates), last_t_s,
                             " (full)" if is_full else " (delta)")
                else:
                    log.warning("EVT POST failed %s/%s", sym_u, tf_u)
            except Exception as e:
                log.warning("EVT post exc %s/%s: %s", sym_u, tf_u, e)


def _ohlc_event_loop(api_base, device_id, token, symbols, tfs, bars, period_sec):
    """
    4.1 event-driven replacement for _ohlc_loop (same signature -> supervisor
    compatible). period_sec is reused as the safety-sweep interval floor.
    """
    global _REG_STARTUP_DONE
    name = threading.current_thread().name

    # ---- 4.5: registry defaults ONCE at startup, not per tick ----
    if not _REG_STARTUP_DONE:
        try:
            ensure_registry_defaults()
        except Exception:
            pass
        try:
            maybe_reset_registry_on_new_install()
        except Exception:
            pass
        _REG_STARTUP_DONE = True

    grace_ms = int(_reg_float("Agent.BarGraceSec", 2.0) * 1000)
    sweep_s = max(60.0, _reg_float("Agent.SafetySweepSec", 300.0))
    idle_thresh = _reg_float("Agent.IdleTickAgeSec", 600.0)
    delta_on = _reg_flag("Agent.DeltaPush", False)   # see guide before enabling
    delta_bars = int(_reg_float("Agent.DeltaBars", 5))
    full_refresh = _reg_float("Agent.FullRefreshSec", 3600.0)

    include_latest = (reg_get("IncludeLatest") or "0").strip() in (
        "1", "true", "TRUE", "yes", "YES",
    )

    # symbols/tfs: same resolution as legacy worker
    syms = [s.strip().upper() for s in (symbols or []) if (s or "").strip()]
    if not syms:
        try:
            reg_syms, _rt, _ = _agent_pull_cfg()
            syms = list(dict.fromkeys(reg_syms or []))
        except Exception:
            pass
    if not syms:
        syms = list(DEFAULT_SYMBOLS)
    tflist = [str(t or "").upper().strip() for t in (tfs or []) if str(t or "").strip()]
    tflist = [t for t in tflist if t in _TF_SEC] or ["M1", "M15", "H1", "H2", "H4"]

    log.info("EVT loop start symbols=%s tfs=%s grace_ms=%s delta=%s sweep_s=%s",
             syms, tflist, grace_ms, delta_on, sweep_s)

    # ---- startup backfill: one legacy full pass populates history + dedupe ----
    _worker_mark(name, "loop_started", last_error="")
    try:
        _push_ohlc_once_safe(api_base, device_id, token, syms, tflist, bars)
        _worker_mark(name, "loop_completed", last_success=time.monotonic())
    except Exception as exc:
        _worker_mark(name, "last_error_at", last_error=f"{type(exc).__name__}:{exc}")
        log.exception("EVT startup backfill failed")

    off_ms = _broker_off_ms_safe()
    now_ms = int(time.time() * 1000)
    fires = {tf: _next_fire_ms(_TF_SEC[tf], now_ms, off_ms, grace_ms) for tf in tflist}
    next_sweep = time.monotonic() + sweep_s
    next_off_refresh = time.monotonic() + 3600.0
    idle_logged = False

    while True:
        now_ms = int(time.time() * 1000)
        now_mono = time.monotonic()

        # periodic broker-offset refresh (registry may update after detection)
        if now_mono >= next_off_refresh:
            off_ms = _broker_off_ms_safe()
            next_off_refresh = now_mono + 3600.0

        due = [tf for tf in tflist if now_ms >= fires[tf]]

        # ---- safety sweep: catches anything a boundary fire missed ----
        if not due and now_mono >= next_sweep:
            _worker_mark(name, "loop_started", last_error="")
            try:
                _push_ohlc_for_tfs(api_base, device_id, token, syms, tflist,
                                   bars, include_latest, delta_on, delta_bars,
                                   full_refresh)
                _worker_mark(name, "loop_completed", last_success=time.monotonic())
            except Exception as exc:
                _worker_mark(name, "last_error_at",
                             last_error=f"{type(exc).__name__}:{exc}")
            next_sweep = time.monotonic() + sweep_s
            continue

        if not due:
            wait_ms = min(fires[tf] for tf in tflist) - now_ms
            wait_s = min(max(wait_ms / 1000.0, 0.2),
                         max(0.2, next_sweep - now_mono), 30.0)
            time.sleep(wait_s)
            continue

        # ---- idle market: don't fetch, roll boundaries forward, sleep long ----
        if _market_idle(idle_thresh):
            if not idle_logged:
                log.info("EVT market idle (weekend/holiday); sleeping in 60s steps")
                idle_logged = True
            for tf in due:
                fires[tf] = _next_fire_ms(_TF_SEC[tf], now_ms, off_ms, grace_ms)
            _worker_mark(name, "loop_completed", last_success=time.monotonic())
            time.sleep(60.0)
            continue
        idle_logged = False

        # ---- fire: fetch + push exactly the TFs whose bar just closed ----
        _worker_mark(name, "loop_started", last_error="")
        try:
            due_sorted = sorted(due, key=lambda t: _TF_SEC[t])
            _push_ohlc_for_tfs(api_base, device_id, token, syms, due_sorted,
                               bars, include_latest, delta_on, delta_bars,
                               full_refresh)
            _worker_mark(name, "loop_completed", last_success=time.monotonic())
        except Exception as exc:
            _worker_mark(name, "last_error_at",
                         last_error=f"{type(exc).__name__}:{exc}")
            log.exception("EVT tick exception")
        finally:
            for tf in due:
                fires[tf] = _next_fire_ms(_TF_SEC[tf], int(time.time() * 1000),
                                          off_ms, grace_ms)


# ---------------------------------------------------------------------------
# Flag-gated worker start. In start_ohlc_worker(), replace the line that
# passes _ohlc_loop to _start_supervised_thread with _ohlc_loop_target()
# (see PHASE1_PATCH_GUIDE.md, edit A2).
# ---------------------------------------------------------------------------
def _ohlc_loop_target():
    if _reg_flag("Agent.EventDriven", False):
        log.info("OHLC: EVENT-DRIVEN loop selected (Agent.EventDriven=1)")
        return _ohlc_event_loop
    log.info("OHLC: legacy fixed-interval loop selected")
    return _ohlc_loop
