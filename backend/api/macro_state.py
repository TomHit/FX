# -*- coding: utf-8 -*-
"""
macro_state.py (FX-only synthetic macro)

In-house macro snapshot provider for XauTrendLab.

This version does NOT call any external HTTP APIs and does NOT rely on
broker-provided macro CFDs like DXY/VIX/US10Y. Instead it derives synthetic
macro signals directly from the FX pairs that are already being captured
by the agent (EURUSD, GBPUSD, USDJPY, USDCHF, USDCAD, etc).

High-level design:

  - Build a synthetic "USD index" from a basket of major FX pairs.
    This plays the role of DXY:
        * EURUSD, GBPUSD: USD is quote -> rising pair = USD weaker
        * USDJPY, USDCHF, USDCAD: USD is base -> rising pair = USD stronger
    We work in log-returns and use sign-adjusted returns so that positive
    basket moves always mean "USD stronger".

  - Build a synthetic "FX risk index" as an average absolute return across
    the same basket (plus XAUUSD if available). This plays the role of VIX.

  - Compute z-scores for both synthetic indices over a trailing window of
    M15 closes (default 50 bars). Those z-scores feed the explanation layer
    in trend_endpoints._build_reasons as:
        * macro_dxy_z
        * macro_vix_z
        * macro_yield_z
        * macro_usd_rate_z

  - For now, US 10Y / USD short-rate are simple proxies derived from the
    USD basket (dxy_z). This keeps the reasoning consistent without needing
    real bond data.

All inputs come from Redis OHLC snapshots written by the agent:

  key: xtl:ohlc:snap:{device_id}:{SYMBOL}:{TF}
  val: JSON {
          "bars": [
              { "t_open_ms": ..., "t_close_ms": ..., "o": ..., "h": ..., "l": ..., "c": ..., "complete": true/false },
              ...
          ]
       }

This module exposes a single public function:

    get_macro_snapshot() -> MacroSnapshot

which is imported and used by api/trend_endpoints.py.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Dict, Any, List

import json
import logging
import math
import os
import time

import redis

log = logging.getLogger("xtl.macro")

# Redis wiring (same style as trend_endpoints)
REDIS_URL = os.getenv("REDIS_URL", "redis://default:xau12345@10.0.0.132:6379/0")
try:
    R = redis.from_url(REDIS_URL, decode_responses=True)
    log.info(f"[MACRO] using REDIS_URL={REDIS_URL}")
except Exception as e:
    log.error(f"[MACRO] failed to connect redis: {e}")
    R = None

# Cache key for snapshot and TTL
MACRO_CACHE_KEY = os.getenv("MACRO_CACHE_KEY", "xtl:macro:snapshot")
MACRO_TTL_SEC = int(os.getenv("MACRO_TTL_SEC", "60"))

# Timeframe and lookback for synthetic macro computation
MACRO_TF = os.getenv("MACRO_TF", "M15").upper()
MACRO_LOOKBACK_BARS = int(os.getenv("MACRO_LOOKBACK_BARS", "50"))

# FX basket configuration (symbol names must match what the agent uses)
# These are the pairs we will use to build synthetic USD index and risk index.
FX_USD_QUOTE = ["EURUSD", "GBPUSD"]            # USD is quote; rising price = USD weaker
FX_USD_BASE  = ["USDJPY", "USDCHF", "USDCAD"]  # USD is base; rising price = USD stronger
FX_EXTRA_VOL = ["XAUUSD"]                      # optional, only for risk/vol (not for USD index)


@dataclass
class MacroSnapshot:
    """
    Lightweight synthetic macro snapshot.

    dxy and vix here are not real DXY/VIX; they are synthetic indices derived
    from your FX basket. The z-values are what we actually care about for
    explanations.
    """

    ts_ms: int

    dxy: Optional[float] = None
    dxy_z: Optional[float] = None

    us10y: Optional[float] = None
    us10y_z: Optional[float] = None

    usd_rate: Optional[float] = None
    usd_rate_z: Optional[float] = None

    vix: Optional[float] = None
    vix_z: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ts_ms": self.ts_ms,
            "dxy": self.dxy,
            "dxy_z": self.dxy_z,
            "us10y": self.us10y,
            "us10y_z": self.us10y_z,
            "usd_rate": self.usd_rate,
            "usd_rate_z": self.usd_rate_z,
            "vix": self.vix,
            "vix_z": self.vix_z,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MacroSnapshot":
        return cls(
            ts_ms=int(data.get("ts_ms") or int(time.time() * 1000)),
            dxy=_safe_float(data.get("dxy")),
            dxy_z=_safe_float(data.get("dxy_z")),
            us10y=_safe_float(data.get("us10y")),
            us10y_z=_safe_float(data.get("us10y_z")),
            usd_rate=_safe_float(data.get("usd_rate")),
            usd_rate_z=_safe_float(data.get("usd_rate_z")),
            vix=_safe_float(data.get("vix")),
            vix_z=_safe_float(data.get("vix_z")),
        )


def _safe_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Redis OHLC helpers
# ---------------------------------------------------------------------------

def _scan_any_snap(sym_u: str, tfu: str) -> Optional[Dict[str, Any]]:
    """
    Scan Redis for the freshest OHLC snapshot for a given symbol+tf,
    regardless of device.
    """
    if R is None:
        return None

    pattern = f"xtl:ohlc:snap:*:{sym_u}:{tfu}"
    cur = 0
    best_snap = None
    best_ts = -1

    try:
        while True:
            cur, keys = R.scan(cur, match=pattern, count=50)
            for k in keys:
                try:
                    raw = R.get(k)
                    if not raw:
                        continue
                    snap = json.loads(raw)
                    if not isinstance(snap, dict):
                        continue
                    bars = snap.get("bars") or []
                    if not isinstance(bars, list) or not bars:
                        continue
                    # find last complete bar open time
                    t_ms = -1
                    for b in bars:
                        if not isinstance(b, dict):
                            continue
                        if not b.get("complete", True):
                            continue
                        t = b.get("t_open_ms") or b.get("t")
                        if t is None:
                            continue
                        if isinstance(t, (int, float)):
                            cur_ms = int(t) if t > 10_000_000_000 else int(t * 1000)
                        else:
                            continue
                        if cur_ms > t_ms:
                            t_ms = cur_ms
                    if t_ms > best_ts:
                        best_ts = t_ms
                        best_snap = snap
                except Exception:
                    continue
            if cur == 0:
                break
    except Exception as e:
        log.error(f"[MACRO] scan error for {sym_u}/{tfu}: {e}")
        return None

    return best_snap


def _extract_closes(snap: Dict[str, Any], max_bars: int) -> List[float]:
    bars = snap.get("bars") or []
    closes: List[float] = []
    for b in bars:
        if not isinstance(b, dict):
            continue
        if not b.get("complete", True):
            continue
        try:
            c = float(b.get("c"))
        except Exception:
            continue
        closes.append(c)
    if not closes:
        return []
    if max_bars > 0:
        closes = closes[-max_bars:]
    return closes


def _log_returns(closes: List[float]) -> List[float]:
    if not closes or len(closes) < 2:
        return []
    out: List[float] = []
    for i in range(1, len(closes)):
        p0 = closes[i - 1]
        p1 = closes[i]
        try:
            if p0 <= 0 or p1 <= 0:
                out.append(0.0)
                continue
            out.append(math.log(p1 / p0))
        except Exception:
            out.append(0.0)
    return out


def _z_from_series(vals: List[float]) -> Optional[float]:
    if not vals or len(vals) < 10:
        return None
    n = float(len(vals))
    mean = sum(vals) / n
    var = sum((x - mean) ** 2 for x in vals) / n
    if var <= 0:
        return None
    std = math.sqrt(var)
    if std == 0:
        return None
    last = vals[-1]
    return (last - mean) / std


# ---------------------------------------------------------------------------
# Snapshot cache helpers
# ---------------------------------------------------------------------------

def _load_cached_snapshot() -> Optional[MacroSnapshot]:
    if R is None:
        return None
    try:
        raw = R.get(MACRO_CACHE_KEY)
        if not raw:
            return None
        data = json.loads(raw)
        snap = MacroSnapshot.from_dict(data)
        age_sec = (int(time.time() * 1000) - snap.ts_ms) / 1000.0
        if age_sec > MACRO_TTL_SEC * 2:
            return None
        return snap
    except Exception as e:
        log.error(f"[MACRO] failed to load cached snapshot: {e}")
        return None


def _save_cached_snapshot(snap: MacroSnapshot) -> None:
    if R is None:
        return
    try:
        payload = json.dumps(snap.to_dict())
        R.set(MACRO_CACHE_KEY, payload, ex=MACRO_TTL_SEC)
    except Exception as e:
        log.error(f"[MACRO] failed to save cached snapshot: {e}")


# ---------------------------------------------------------------------------
# Core synthetic macro builder
# ---------------------------------------------------------------------------

def _build_synthetic_from_fx() -> MacroSnapshot:
    now_ms = int(time.time() * 1000)

    # Fetch closes for all configured symbols
    series_closes: Dict[str, List[float]] = {}

    all_syms = list(FX_USD_QUOTE) + list(FX_USD_BASE) + list(FX_EXTRA_VOL)
    all_syms_unique = sorted(set(all_syms))

    for sym in all_syms_unique:
        snap = _scan_any_snap(sym, MACRO_TF)
        if snap is None:
            log.debug(f"[MACRO] no OHLC snapshot for {sym}/{MACRO_TF}")
            continue
        closes = _extract_closes(snap, MACRO_LOOKBACK_BARS)
        if closes:
            series_closes[sym] = closes

    # Require at least EURUSD + USDJPY to build anything meaningful
    if "EURUSD" not in series_closes or "USDJPY" not in series_closes:
        log.warning("[MACRO] insufficient FX data for synthetic macro; returning empty snapshot")
        return MacroSnapshot(ts_ms=now_ms)

    # Align by minimum length across all series we actually have
    min_len = min(len(v) for v in series_closes.values())
    if min_len < 15:
        log.warning(f"[MACRO] not enough bars for synthetic macro (min_len={min_len})")
        return MacroSnapshot(ts_ms=now_ms)

    for k in list(series_closes.keys()):
        series_closes[k] = series_closes[k][-min_len:]

    # Compute log-returns for each series
    series_rets: Dict[str, List[float]] = {}
    for sym, closes in series_closes.items():
        rets = _log_returns(closes)
        if len(rets) >= 10:
            series_rets[sym] = rets

    # Align again on returns (one shorter than closes)
    if not series_rets:
        log.warning("[MACRO] no return series for synthetic macro")
        return MacroSnapshot(ts_ms=now_ms)

    min_ret_len = min(len(v) for v in series_rets.values())
    if min_ret_len < 10:
        log.warning(f"[MACRO] not enough return bars for synthetic macro (min_ret_len={min_ret_len})")
        return MacroSnapshot(ts_ms=now_ms)

    for k in list(series_rets.keys()):
        series_rets[k] = series_rets[k][-min_ret_len:]

    # Synthetic USD index (DXY-like) from USD base/quote pairs
    dxy_series: List[float] = []

    # weights (roughly DXY-like, renormalized without SEK)
    w_eur = 0.577
    w_jpy = 0.136
    w_gbp = 0.119
    w_cad = 0.091
    w_chf = 0.036
    total_w = w_eur + w_jpy + w_gbp + w_cad + w_chf
    w_eur /= total_w
    w_jpy /= total_w
    w_gbp /= total_w
    w_cad /= total_w
    w_chf /= total_w

    for i in range(min_ret_len):
        # sign-adjusted returns so that positive = USD stronger
        # quote pairs: EURUSD, GBPUSD -> USD strength = price down -> -return
        r_eur = -series_rets.get("EURUSD", [0.0] * min_ret_len)[i]
        r_gbp = -series_rets.get("GBPUSD", [0.0] * min_ret_len)[i]
        # base pairs: USDJPY, USDCHF, USDCAD -> USD strength = price up -> +return
        r_jpy = series_rets.get("USDJPY", [0.0] * min_ret_len)[i]
        r_chf = series_rets.get("USDCHF", [0.0] * min_ret_len)[i]
        r_cad = series_rets.get("USDCAD", [0.0] * min_ret_len)[i]

        basket_move = (
            w_eur * r_eur +
            w_gbp * r_gbp +
            w_jpy * r_jpy +
            w_cad * r_cad +
            w_chf * r_chf
        )
        dxy_series.append(basket_move)

    dxy_z = _z_from_series(dxy_series)
    # level here is just the last synthetic "return"; not used directly but
    # kept for completeness
    dxy_level = dxy_series[-1] if dxy_series else None

    # Synthetic risk index (VIX-like) from average absolute returns across basket
    risk_series: List[float] = []
    risk_syms = list(FX_USD_QUOTE) + list(FX_USD_BASE) + [s for s in FX_EXTRA_VOL if s in series_rets]

    for i in range(min_ret_len):
        vals: List[float] = []
        for sym in risk_syms:
            arr = series_rets.get(sym)
            if arr is None or i >= len(arr):
                continue
            vals.append(abs(arr[i]))
        if not vals:
            risk_series.append(0.0)
        else:
            risk_series.append(sum(vals) / float(len(vals)))

    vix_z = _z_from_series(risk_series)
    vix_level = risk_series[-1] if risk_series else None

    # For now, treat us10y/usd_rate as slower-moving proxies of the same USD basket.
    # This keeps reasons consistent until we introduce real rate instruments.
    us10y_level = dxy_level
    us10y_z = dxy_z
    usd_rate_level = dxy_level
    usd_rate_z = dxy_z

    snap = MacroSnapshot(
        ts_ms=now_ms,
        dxy=dxy_level,
        dxy_z=dxy_z,
        us10y=us10y_level,
        us10y_z=us10y_z,
        usd_rate=usd_rate_level,
        usd_rate_z=usd_rate_z,
        vix=vix_level,
        vix_z=vix_z,
    )
    return snap


# ---------------------------------------------------------------------------
# Public entrypoint
# ---------------------------------------------------------------------------

def get_macro_snapshot(force_refresh: bool = False) -> MacroSnapshot:
    """
    Main entrypoint for trend endpoints.

    - If force_refresh is False:
        * Try Redis cache first.
        * If not present / too old, rebuild from FX OHLC snapshots.
    - If force_refresh is True:
        * Always rebuild from FX OHLC snapshots.

    In all cases there are no external HTTP calls.
    """
    if not force_refresh:
        snap = _load_cached_snapshot()
        if snap is not None:
            return snap

    try:
        snap = _build_synthetic_from_fx()
        _save_cached_snapshot(snap)
        return snap
    except Exception as e:
        log.error(f"[MACRO] get_macro_snapshot synthetic build failed: {e}")
        now_ms = int(time.time() * 1000)
        return MacroSnapshot(ts_ms=now_ms)
