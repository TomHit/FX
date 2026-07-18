#!/usr/bin/env python3
"""
XTL Trade Analytics Engine — Phase 1 (Option B: H1-bar exit approximation)
==========================================================================
Captures every trade's FROZEN entry context + an approximated exit into a
permanent JSONL learning database. NO agent change, NO Windows risk.

Captures BOTH entry paths:
  - clean        : normal pipeline trade, snapshot fires at ACK (ticket lands)
  - broker_repair: reconstructed trade, snapshot fires at creation (ticket present)
Each tagged via `entry_provenance` so repaired trades (partial entry context) can
be filtered out of entry-quality studies while still counting for win/loss.

Contract (locked):
  - Snapshot written when mt5_ticket is known (irreplaceable entry context).
  - Close detected by polling: ticket absent from live open-positions set.
  - Orphan sweep mandatory.
  - Redis = in-flight lifecycle ; JSONL = permanent history.
  - Analytics NEVER blocks trading: every public fn swallows its own errors.

Exit precision is approximate now (h1_bar_approx / medium), exact later
(mt5_deal_history / high). Both `exit_source` and `exit_confidence` are stored so
pandas can separate or compare the two qualities of data.
"""

import json
import os
import time
import logging
import fcntl
import stat
import tempfile

from contextlib import contextmanager

log = logging.getLogger("xtl.analytics")

SNAP_PREFIX    = "xtl:analytics:trade:"
DEAL_WAIT_MS   = 180_000   # wait up to 3 min for the broker deal before approximating (fixes finalize-vs-deal race)
SNAP_TTL_SEC   = 14 * 24 * 3600
JSONL_PATH     = "/opt/xauapi/api/trend/out/trades.jsonl"
JSONL_LOCK_PATH = JSONL_PATH + ".lock"
PENDING_TRUTH_KEY = "xtl:analytics:pending_truth"
ORPHAN_AGE_MS  = 10 * 60 * 1000
SCHEMA_VERSION = "1.5"

# Live entry timestamps (now_ms) are TRUE UTC, so offset 0. (The historical parquet
# needed +3 because it was broker-encoded; the live clock is not.) VERIFY once against
# a known entry before trusting session analysis, then adjust here if needed.
LIVE_TZ_OFFSET_H = 0.0

# Static drift table from the 18-month profiler. Only |reliab|>=2 combos are reliable;
# everything else is noise and never flags against_drift. Regenerate monthly.
DRIFT_TABLE = {
    "XAUUSD": {"Asia":   {"signed": 4.3, "reliab": 2.56}},
    "USDCAD": {"London": {"signed": 2.1, "reliab": 2.24}},
}


# ── tiny helpers ─────────────────────────────────────────────────────────────

@contextmanager
def _trades_jsonl_lock():
    """Exclusive advisory lock shared by _append_jsonl and the reconciler."""
    lf = open(JSONL_LOCK_PATH, "a+", encoding="utf-8")   
    try:
        fcntl.flock(lf.fileno(), fcntl.LOCK_EX)
        yield
    finally:
        try:
            fcntl.flock(lf.fileno(), fcntl.LOCK_UN)
        finally:
            lf.close()



def _now_ms() -> int:
    return int(time.time() * 1000)


def _pip(symbol: str) -> float:
    s = (symbol or "").upper()
    if s == "XAUUSD" or s.endswith("JPY"):
        return 0.01
    return 0.0001


def _safe_float(x, d=None):
    try:
        if x is None:
            return d
        return float(x)
    except Exception:
        return d


def _safe_int(x, default=None):
    try:
        if x is None or x == "":
            return default
        return int(float(x))
    except Exception:
        return default




def analyze_liquidity_target_during_trade(
    snap: dict,
    bars_h1: list,
) -> dict:
    """
    Analyze whether the trade reached the opposing entry-frozen
    liquidity pool before exiting.

    BUY  -> target = entry-time BSL
    SELL -> target = entry-time SSL

    Only bars inside the actual trade lifetime are considered.
    Pure analytics only.
    """

    out = {
        "target_liquidity_type": None,
        "target_liquidity_level": None,
        "target_liquidity_touched": False,
        "target_liquidity_touch_bar_ms": None,
        "tp_beyond_target_liquidity": False,
        "target_liquidity_distance_r": None,
    }

    try:
        side = str(
            snap.get("side") or ""
        ).upper().strip()

        entry = _safe_float(
            snap.get("entry_price")
        )
        sl = _safe_float(
            snap.get("sl_price")
        )
        tp = _safe_float(
            snap.get("tp_price")
        )

        if side == "BUY":
            target = _safe_float(
                snap.get("bsl_level")
            )
            out["target_liquidity_type"] = "BSL"

        elif side == "SELL":
            target = _safe_float(
                snap.get("ssl_level")
            )
            out["target_liquidity_type"] = "SSL"

        else:
            return out

        if (
            entry is None
            or entry <= 0
            or sl is None
            or sl <= 0
            or target is None
            or target <= 0
        ):
            return out

        # Reject a liquidity level that is on the wrong side
        # of the entry price.
        if side == "BUY" and target <= entry:
            return out

        if side == "SELL" and target >= entry:
            return out

        out["target_liquidity_level"] = target

        risk_distance = abs(
            entry - sl
        )

        if risk_distance > 0:
            out["target_liquidity_distance_r"] = round(
                abs(target - entry) / risk_distance,
                3,
            )

        if tp is not None and tp > 0:
            if side == "BUY":
                out["tp_beyond_target_liquidity"] = bool(
                    tp > target
                )
            else:
                out["tp_beyond_target_liquidity"] = bool(
                    tp < target
                )

        entry_ms = _norm_ms(
            snap.get("broker_open_time_utc_ms")
            or snap.get("enqueue_timestamp")
            or 0
        )

        close_ms = _norm_ms(
            snap.get("broker_close_time_utc_ms")
            or snap.get("close_timestamp")
            or 0
        )

        for bar in bars_h1 or []:
            if not isinstance(bar, dict):
                continue

            # Prefer close time for lifetime filtering because an H1
            # bar can begin before entry but still contain post-entry price.
            bar_open_ms = _norm_ms(
                bar.get("t_open_ms")
                or bar.get("t")
                or 0
            )

            bar_close_ms = _norm_ms(
                bar.get("t_close_ms")
                or 0
            )

            if not bar_close_ms and bar_open_ms:
                bar_close_ms = (
                    bar_open_ms + 3_600_000
                )

            # Skip bars that finished before the trade opened.
            if (
                entry_ms
                and bar_close_ms
                and bar_close_ms <= entry_ms
            ):
                continue

            # Skip bars that opened after the trade closed.
            if (
                close_ms
                and bar_open_ms
                and bar_open_ms >= close_ms
            ):
                continue

            high = _safe_float(
                bar.get("h")
            )
            low = _safe_float(
                bar.get("l")
            )

            if high is None or low is None:
                continue

            touched = False

            if side == "BUY":
                touched = high >= target
            else:
                touched = low <= target

            if touched:
                out["target_liquidity_touched"] = True
                out["target_liquidity_touch_bar_ms"] = (
                    bar_open_ms
                    or bar_close_ms
                    or None
                )
                break

    except Exception as exc:
        log.warning(
            "analytics: liquidity target analysis failed "
            "ticket=%s err=%r",
            snap.get("mt5_ticket"),
            exc,
        )

    return out

def read_news_day_context(symbol: str, ts_ms: int) -> dict:
    out = {
        "news_day_has_high_impact": False,
        "news_day_events": [],
        "nearest_news_event": None,
        "nearest_news_time_ms": None,
        "nearest_news_distance_minutes": None,
        "nearest_news_relation": None,
    }

    try:
        R = from_app_R()
        raw = R.get("xtl:news:calendar:daily")
        if not raw:
            return out

        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", "ignore")

        data = json.loads(raw)
        events = data.get("events") if isinstance(data, dict) else []

        if not isinstance(events, list):
            return out

        currencies = _currencies_for_symbol(symbol)

        from datetime import datetime, timezone

        entry_day = datetime.fromtimestamp(
            int(ts_ms) / 1000,
            timezone.utc,
        ).date()

        matches = []

        for event in events:
            event_ms = int(event.get("time_ms") or 0)
            if event_ms <= 0:
                continue

            currency = str(event.get("currency") or "").upper()
            impact = str(event.get("impact") or "").upper()

            if currency not in currencies or impact != "HIGH":
                continue

            event_day = datetime.fromtimestamp(
                event_ms / 1000,
                timezone.utc,
            ).date()

            if event_day != entry_day:
                continue

            minutes_delta = round((event_ms - int(ts_ms)) / 60000, 1)

            if minutes_delta > 0:
                relation = "NEWS_IN_FUTURE"      # trade entered before the news
            elif minutes_delta < 0:
                relation = "NEWS_ALREADY_OCCURRED"       # trade entered after the news
            else:
                relation = "NEWS_NOW"

            matches.append({
                "event": event.get("event"),
                "currency": currency,
                "impact": impact,
                "time_ms": event_ms,

                # Always positive
                "distance_minutes": abs(minutes_delta),

                # Human readable
                "relation": relation,

                "pre_block_min": event.get("pre_block_min"),
                "post_block_min": event.get("post_block_min"),
                "stabilization_min": event.get("stabilization_min"),
            })

        matches.sort(
            key=lambda event: float(
                event.get("distance_minutes") or 0
            )
        )

        out["news_day_events"] = matches
        out["news_day_has_high_impact"] = bool(matches)

        if matches:
            nearest = matches[0]

            out["nearest_news_event"] = nearest.get("event")
            out["nearest_news_time_ms"] = nearest.get("time_ms")
            out["nearest_news_distance_minutes"] = nearest.get(
                "distance_minutes"
            )
            out["nearest_news_relation"] = nearest.get("relation")

    except Exception as exc:
        log.warning(
            "analytics: news day context failed symbol=%s err=%s",
            symbol,
            exc,
        )

    return out

def _session_for_ts_ms(ts_ms, tz_offset_h=0.0) -> str:
    try:
        corrected = int(ts_ms) - int(tz_offset_h * 3_600_000)
        h = (corrected // 3_600_000) % 24
        if 7 <= h < 12:  return "London"
        if 12 <= h < 16: return "Overlap"
        if 16 <= h < 21: return "NY_late"
        return "Asia"
    except Exception:
        return "Asia"


def _extract_ticket(p: dict):
    t = p.get("mt5_ticket")
    if t in (None, "", 0):
        ack = p.get("mt5_ack") if isinstance(p.get("mt5_ack"), dict) else {}
        res = ack.get("result") if isinstance(ack.get("result"), dict) else {}
        t = res.get("ticket")
    return t if t not in (None, "", 0) else None


def _bar_ms_any(b: dict) -> int:
    """Bar time in ms. This feed writes epoch SECONDS in 't'."""
    for k in ("t_close_ms", "t_open_ms", "t", "time"):
        v = b.get(k)
        if v:
            try:
                n = int(v)
            except (TypeError, ValueError):
                continue
            return n * 1000 if n < 100_000_000_000 else n
    return 0

def _drift_lookup(sym: str, session: str, side: str) -> dict:
    rec = (DRIFT_TABLE.get((sym or "").upper()) or {}).get(session)
    if not rec:
        return {"signed": None, "reliab": None, "direction": None, "against": False}
    signed = rec.get("signed"); reliab = rec.get("reliab")
    direction = "BUY" if (signed or 0) > 0 else "SELL"
    against = (abs(reliab or 0) >= 2.0) and (str(side or "").upper() != direction)
    return {"signed": signed, "reliab": reliab, "direction": direction, "against": against}


# ── host wiring (lazy imports so this module loads cleanly / no circular import) ─
def from_app_R():
    from api.trend_endpoints import R
    return R


def _open_tickets() -> set:
    try:
        from api.trend_endpoints import _live_broker_tickets_for_prop
        return _live_broker_tickets_for_prop() or set()
    except Exception as e:
        log.error("analytics: cannot read open tickets: %s", e)
        return set()


def _resolve_bar_device(
    symbol: str,
    prefer: str = None,
    *,
    allow_scan_fallback: bool = True,
) -> str:
    """
    Resolve the OHLC device for a symbol.

    Priority:
      1. Explicit preferred trade/execution device, when it has H1 bars.
      2. Optional fallback: choose the freshest matching H1 snapshot.

    Never select the first Redis SCAN result because stale/retired devices
    may still retain OHLC keys.
    """
    try:
        R = from_app_R()
        sym = str(symbol or "").upper().strip()
        preferred = str(prefer or "").strip()

        if not sym:
            return ""

        # -------------------------------------------------
        # 1. Strict preferred-device path.
        # This is the normal broker-confirmed entry path.
        # -------------------------------------------------
        if preferred:
            preferred_key = f"xtl:ohlc:snap:{preferred}:{sym}:H1"

            if R.exists(preferred_key):
                return preferred

            if not allow_scan_fallback:
                log.warning(
                    "analytics: preferred OHLC device missing "
                    "symbol=%s device=%s key=%s",
                    sym,
                    preferred,
                    preferred_key,
                )
                return ""

        if not allow_scan_fallback:
            return ""

        # -------------------------------------------------
        # 2. Diagnostic/legacy fallback.
        # Select the freshest snapshot, not the first SCAN key.
        # -------------------------------------------------
        best_device = ""
        best_freshness_ms = -1

        for key in R.scan_iter(
            f"xtl:ohlc:snap:*:{sym}:H1",
            count=200,
        ):
            key_s = (
                key.decode("utf-8", "ignore")
                if isinstance(key, (bytes, bytearray))
                else str(key)
            )

            parts = key_s.split(":")
            if len(parts) < 6:
                continue

            device = parts[3]
            raw = R.get(key_s)
            if not raw:
                continue

            try:
                if isinstance(raw, (bytes, bytearray)):
                    raw = raw.decode("utf-8", "ignore")

                snap = json.loads(raw)

                if isinstance(snap, str):
                    snap = json.loads(snap)

                if not isinstance(snap, dict):
                    continue

                freshness_ms = 0

                for field in (
                    "server_received_ms",
                    "received_at_ms",
                    "published_at_ms",
                    "updated_ts_ms",
                    "lastClosedTs",
                    "last_closed_ts",
                ):
                    value = _safe_int(
                        snap.get(field),
                        0,
                    ) or 0

                    if 0 < value < 10_000_000_000:
                        value *= 1000

                    freshness_ms = max(
                        freshness_ms,
                        int(value),
                    )

                # Fall back to newest bar timestamp when the snapshot
                # does not carry an ingest/publish timestamp.
                bars = snap.get("bars")
                if isinstance(bars, list) and bars:
                    bar_ms = _bar_ms_any(
                        bars[-1]
                        if isinstance(bars[-1], dict)
                        else {}
                    )
                    freshness_ms = max(
                        freshness_ms,
                        int(bar_ms or 0),
                    )

                if freshness_ms > best_freshness_ms:
                    best_freshness_ms = freshness_ms
                    best_device = device

            except Exception:
                continue

        if best_device:
            log.warning(
                "analytics: OHLC device fallback "
                "symbol=%s preferred=%s selected=%s freshness_ms=%s",
                sym,
                preferred or "-",
                best_device,
                best_freshness_ms,
            )

        return best_device

    except Exception as exc:
        log.warning(
            "analytics: _resolve_bar_device failed "
            "symbol=%s preferred=%s err=%s",
            symbol,
            prefer,
            exc,
        )
        return str(prefer or "").strip()
def read_regime_at_ack(symbol: str, device_id: str):
    """H1+H4+D1 regime recomputed live at the entry moment. None on failure.
    Resolves the bar-storing device (which may differ from the trade's device)."""
    try:
        if not symbol:
            return None
        from api.trend_endpoints import _get_closed_h1_bars, _get_closed_h4_bars
        from api.liq_structure import detect_regime
        dev = _resolve_bar_device(
            symbol,
            device_id,
            allow_scan_fallback=False,
        )
        if not dev:
            return None
        h1 = _get_closed_h1_bars(symbol, dev) or []
        h4 = _get_closed_h4_bars(symbol, dev) or []
        if not h1 and not h4:
            return None
        return detect_regime(h1, h4)
    except Exception as e:
        log.warning("analytics: regime read failed for %s: %s", symbol, e)
        return None


def read_sr_at_ack(symbol: str) -> dict:
    """SR levels from the cached bundle (cheap GET, no engine recompute).
    Pulls nearest + best-scored support/resistance, plus bundle atr/price."""
    out = {"best_resistance": None, "best_support": None,
           "nearest_resistance": None, "nearest_support": None,
           "sr_atr": None, "sr_price": None}
    try:
        if not symbol:
            return out
        R = from_app_R()
        sym = symbol.upper()
        raw = R.get(f"xtl:sr:bundle:last_good:{sym}") or R.get(f"xtl:sr:bundle:last:{sym}")
        if not raw:
            return out
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", "ignore")
        d = json.loads(raw)
        out["nearest_resistance"] = _level_of(d.get("nearest_resistance"))
        out["nearest_support"]    = _level_of(d.get("nearest_support"))
        out["best_resistance"]    = _best_scored(d.get("active_resistances"))
        out["best_support"]       = _best_scored(d.get("active_supports"))
        out["sr_atr"]   = _safe_float(d.get("atr"))
        out["sr_price"] = _safe_float(d.get("price"))
        return out
    except Exception as e:
        log.warning("analytics: SR read failed for %s: %s", symbol, e)
        return out


def _level_of(x):
    if isinstance(x, dict):
        return _safe_float(x.get("level") or x.get("price") or x.get("value"))
    return _safe_float(x)


def _best_scored(levels):
    """Pick the highest-scored level from an active_supports/resistances list."""
    try:
        if not isinstance(levels, list) or not levels:
            return None
        def _score(z):
            return _safe_float((z or {}).get("sr_score") or (z or {}).get("score"), 0.0) or 0.0
        best = max(levels, key=_score)
        return _level_of(best)
    except Exception:
        return None


def _atr_from_bars(bars: list, n: int = 14) -> float:
    """ATR(14) from o/h/l/c bars (entry-frozen normalizer). 0.0 if too few bars."""
    try:
        if not bars or len(bars) < n + 1:
            return 0.0
        trs = []
        for i in range(1, len(bars)):
            h = _safe_float(bars[i].get("h")); l = _safe_float(bars[i].get("l"))
            pc = _safe_float(bars[i - 1].get("c"))
            if h is None or l is None or pc is None:
                continue
            trs.append(max(h - l, abs(h - pc), abs(l - pc)))
        if len(trs) < n:
            return 0.0
        atr = sum(trs[:n]) / n
        for tr in trs[n:]:
            atr = (atr * (n - 1) + tr) / n
        return round(float(atr), 6)
    except Exception:
        return 0.0


def read_liquidity_at_ack(symbol, side, zone, device_id, entry_price):
    """Liquidity model + score + sweep flag, recomputed live at entry (no cache dep).
    Mirrors read_regime_at_ack: resolve device, fetch bars, call detect_liq_signals,
    derive the three flat fields the analysis needs. Returns dict; safe."""
    out = {"liquidity_model": None, "liquidity_score": None, "sweep_detected": None, "atr": None}
    try:
        if not symbol:
            return out
        from api.trend_endpoints import _get_closed_h1_bars, _get_closed_h4_bars
        from api.liq_structure import detect_liq_signals
        dev = _resolve_bar_device(
            symbol,
            device_id,
            allow_scan_fallback=False,
        )
        if not dev:
            return out
        h1 = _get_closed_h1_bars(symbol, dev) or []
        h4 = _get_closed_h4_bars(symbol, dev) or []
        atr = _atr_from_bars(h1)
        out["atr"] = atr or None
        if not h1 and not h4:
            return out
        zdict = zone if isinstance(zone, dict) else None
        res = detect_liq_signals(symbol, (side or ""), zdict, h1, h4,
                                 float(entry_price or 0.0), float(atr or 0.0)) or {}
        signals = res.get("signals") or []
        # liquidity_model: most salient signal label present (sweep/OB/FVG/BSL/SSL)
        out["liquidity_model"] = (signals[0] if signals else None)
        # liquidity_score: map the confidence string -> number, else count of signals
        conf = str(res.get("liq_confidence") or "").upper()
        conf_map = {"HIGH": 3, "MED": 2, "MEDIUM": 2, "LOW": 1}
        out["liquidity_score"] = conf_map.get(conf, len(signals) if signals else 0)
        # sweep_detected: any sweep evidence in bsl_ssl / signals
        bsl_ssl = res.get("bsl_ssl") or {}
        _ = bsl_ssl  # exposed below
        swept = False
        try:
            for k in ("bsl", "ssl"):
                v = bsl_ssl.get(k)
                if isinstance(v, dict) and (v.get("candles_since_sweep") is not None or v.get("swept")):
                    swept = True
        except Exception:
            pass
        if not swept:
            swept = any("SWEEP" in str(s).upper() or "SWEPT" in str(s).upper() for s in signals)
        out["sweep_detected"] = bool(swept)
        out["_detail"]  = res.get("liq_detail") or {}
        out["_bsl_ssl"] = bsl_ssl
        return out
    except Exception as e:
        log.warning("analytics: liquidity read failed for %s: %s", symbol, e)
        return out


def read_shadow_bias_at_ack(
    symbol: str,
    side: str,
    device_id: str,
    entry_price: float,
    entry_zone: dict | None = None,
    computed_ms: int | None = None,
) -> dict:
    """
    Compute and freeze the XTL Evidence Bias payload at broker-confirmed entry.

    Shadow analytics only:
      - no Redis writes
      - no gate/watch changes
      - no order/risk effects
      - any failure returns an UNKNOWN payload and never raises

    H1/H4 bars, SR bundle, liquidity and regime are all read at the same
    capture moment so the stored evidence is internally consistent.
    """
    sym = str(symbol or "").upper().strip()
    executed_side = str(side or "").upper().strip()

    fallback = {
        "bias_engine_version": None,
        "symbol": sym,
        "shadow_bias": "UNKNOWN",
        "shadow_bias_score": 0.0,
        "shadow_bias_confidence": "NONE",
        "shadow_bias_data_ok": False,
        "shadow_bias_actionable": False,
        "shadow_bias_actionability_reason": "CAPTURE_NOT_AVAILABLE",
        "shadow_bias_relation": "UNKNOWN",
        "executed_side": executed_side or None,
        "computed_ms": int(computed_ms or _now_ms()),
        "data_errors": ["BIAS_CAPTURE_NOT_AVAILABLE"],
    }

    try:
        if not sym:
            fallback["data_errors"] = ["BIAS_CAPTURE_MISSING_SYMBOL"]
            return fallback

        px = _safe_float(entry_price)
        if px is None or px <= 0:
            fallback["data_errors"] = ["BIAS_CAPTURE_INVALID_ENTRY_PRICE"]
            return fallback

        from api.trend_endpoints import (
            _get_closed_h1_bars,
            _get_closed_h4_bars,
        )
        from api.liq_structure import (
            detect_liq_signals,
            detect_regime,
        )
        from api.shadow_bias import (
            BIAS_ENGINE_VERSION,
            compute_shadow_bias,
        )

        dev = _resolve_bar_device(
            sym,
            device_id,
            allow_scan_fallback=False,
        )
        if not dev:
            fallback["bias_engine_version"] = BIAS_ENGINE_VERSION
            fallback["data_errors"] = ["BIAS_CAPTURE_NO_BAR_DEVICE"]
            return fallback

        bars_h1 = _get_closed_h1_bars(sym, dev) or []
        bars_h4 = _get_closed_h4_bars(sym, dev) or []

        # -------------------------------------------------
        # Load the complete SR evidence bundle.
        # Do not use read_sr_at_ack() here because it intentionally
        # flattens the bundle to four scalar levels.
        # -------------------------------------------------
        sr_bundle = {}
        try:
            R = from_app_R()
            raw_sr = (
                R.get(f"xtl:sr:bundle:last_good:{sym}")
                or R.get(f"xtl:sr:bundle:last:{sym}")
            )

            if raw_sr:
                if isinstance(raw_sr, (bytes, bytearray)):
                    raw_sr = raw_sr.decode("utf-8", "ignore")

                sr_bundle = json.loads(raw_sr)

                # Handle an accidentally double-encoded JSON value safely.
                if isinstance(sr_bundle, str):
                    sr_bundle = json.loads(sr_bundle)

                if not isinstance(sr_bundle, dict):
                    sr_bundle = {}

        except Exception as exc:
            log.warning(
                "analytics: shadow bias SR read failed for %s: %s",
                sym,
                exc,
            )
            sr_bundle = {}

        atr_h1 = _atr_from_bars(bars_h1)

        zone_for_liq = (
            dict(entry_zone)
            if isinstance(entry_zone, dict)
            else None
        )

        liquidity_context = {}
        try:
            liquidity_context = detect_liq_signals(
                sym,
                executed_side,
                zone_for_liq,
                bars_h1,
                bars_h4,
                float(px),
                float(atr_h1 or 0.0),
            ) or {}

            if not isinstance(liquidity_context, dict):
                liquidity_context = {}

        except Exception as exc:
            log.warning(
                "analytics: shadow bias liquidity read failed for %s: %s",
                sym,
                exc,
            )
            liquidity_context = {}

        regime_context = {}
        try:
            regime_context = detect_regime(
                bars_h1,
                bars_h4,
            ) or {}

            if not isinstance(regime_context, dict):
                regime_context = {}

        except Exception as exc:
            log.warning(
                "analytics: shadow bias regime read failed for %s: %s",
                sym,
                exc,
            )
            regime_context = {}

        result = compute_shadow_bias(
            symbol=sym,
            bars_h1=bars_h1,
            bars_h4=bars_h4,
            price=float(px),
            sr_bundle=sr_bundle,
            liquidity_context=liquidity_context,
            regime_context=regime_context,
            executed_side=executed_side,
            computed_ms=int(computed_ms or _now_ms()),
        )

        if not isinstance(result, dict):
            fallback["bias_engine_version"] = BIAS_ENGINE_VERSION
            fallback["data_errors"] = ["BIAS_ENGINE_NON_DICT_RESULT"]
            return fallback

        # Capture provenance for future forensic/replay work.
        result["capture_source"] = "xtl_analytics_entry_ack"
        result["bar_device_id"] = dev
        result["input_bar_counts"] = {
            "h1": len(bars_h1),
            "h4": len(bars_h4),
        }
        result["input_entry_price"] = float(px)
        result["input_executed_side"] = executed_side or None
        result["input_zone"] = (
            dict(entry_zone)
            if isinstance(entry_zone, dict)
            else None
        )

        return result

    except Exception as exc:
        log.warning(
            "analytics: shadow bias capture failed for %s: %s",
            sym,
            exc,
        )

        fallback["shadow_bias_actionability_reason"] = "CAPTURE_EXCEPTION"
        fallback["data_errors"] = [
            f"BIAS_CAPTURE_EXCEPTION:{type(exc).__name__}:{exc}"
        ]
        return fallback

def default_fetch_h1_bars(symbol: str, start_ms: int, end_ms: int, device_id: str = None):
    """Sweep adapter: recent closed H1 bars for the symbol's device."""
    try:
        if not symbol or not device_id:
            return []
        from api.trend_endpoints import _get_closed_h1_bars
        return _get_closed_h1_bars(symbol, device_id) or []
    except Exception as e:
        log.warning("analytics: default_fetch_h1_bars failed for %s: %s", symbol, e)
        return []


# ── Phase-F §13: reversal-candle OHLC reconstructed from H1 bar at rc_open_ms ─
def read_reversal_candle(symbol: str, device_id: str, pos: dict) -> dict:
    """Multi-source RC capture with provenance. Resolves rc_open_ms from the first
    available source (pos -> entry_gate.rev_state -> live watch -> touch fallback),
    records WHERE it came from (rc_source), then reconstructs OHLC from the H1 bar
    at that timestamp if the source didn't already carry high/low/open/close.
    Never assumes rc_open_ms exists; on total miss returns rc_source='missing'."""
    rc = {"rc_found": False, "rc_source": "missing", "rc_open_ms": None,
          "rc_open": None, "rc_high": None, "rc_low": None, "rc_close": None,
          "rc_body_pct": None, "rc_size_pips": None, "rc_direction": None,
          "rc_capture_note": "missing"}
    try:
        p = pos if isinstance(pos, dict) else {}
        sources = [("pos", p)]
        eg = p.get("entry_gate")
        if isinstance(eg, dict):
            rev = eg.get("rev_state")
            if isinstance(rev, dict):
                sources.append(("entry_gate.rev_state", rev))
        # live watch, if it still exists
        wk = str(p.get("watch_key") or "")
        if not wk:
            sym_u = (symbol or "").upper(); side = str(p.get("side") or "").upper()
            tf = p.get("entry_zone_tf") or "H1"
            if sym_u and side:
                wk = f"xtl:zone:watch:{sym_u}:{side}:{tf}"
        if wk:
            try:
                R = from_app_R()
                raw = R.get(wk)
                if raw:
                    if isinstance(raw, (bytes, bytearray)):
                        raw = raw.decode("utf-8", "ignore")
                    w = json.loads(raw)
                    if isinstance(w, dict):
                        sources.append(("watch", w))
            except Exception:
                pass

        chosen = None; chosen_src = None
        for name, s in sources:
            ts = (s.get("rc_open_ms") or s.get("rev_ok_bar_open_ms")
                  or s.get("touch_candle_open_ms") or s.get("touch_open_ms"))
            if ts:
                chosen = s; chosen_src = name
                rc["rc_open_ms"] = int(ts)
                # direct OHLC if the source carries it
                rc["rc_high"]  = _safe_float(s.get("rc_high") or s.get("rev_ok_bar_hi"))
                rc["rc_low"]   = _safe_float(s.get("rc_low")  or s.get("rev_ok_bar_lo"))
                rc["rc_open"]  = _safe_float(s.get("rc_open"))
                rc["rc_close"] = _safe_float(s.get("rc_close"))
                rc["rc_source"] = name
                note = "touch_fallback" if name == "pos" and not (s.get("rc_open_ms") or s.get("rev_ok_bar_open_ms")) else name
                rc["rc_capture_note"] = note
                rc["rc_found"] = True
                break

        if not rc["rc_found"]:
            return rc

        # reconstruct OHLC from the H1 bar at rc_open_ms if not supplied by source
        if None in (rc["rc_open"], rc["rc_high"], rc["rc_low"], rc["rc_close"]):
            dev = _resolve_bar_device(
                symbol,
                device_id,
                allow_scan_fallback=False,
            )
            if dev:
                from api.trend_endpoints import _get_closed_h1_bars
                bars = _get_closed_h1_bars(symbol, dev) or []
                bar = None
                H1_MS = 3_600_000
                _target = int(rc["rc_open_ms"] or 0)

                def _open_ms(b):
                    o = int(b.get("t_open_ms") or 0)
                    if o:
                        return o
                    c = int(b.get("t_close_ms") or 0)
                    return (c - H1_MS) if c else 0

                bar = None
                if _target:
                    for b in bars:
                        if _open_ms(b) == _target:
                            bar = b
                            break
                    if bar is None:
                        cands = [b for b in bars if _open_ms(b) and _open_ms(b) <= _target]
                        bar = max(cands, key=_open_ms) if cands else None
                if bar:
                    rc["rc_open"]  = _safe_float(bar.get("o"))
                    rc["rc_high"]  = _safe_float(bar.get("h"))
                    rc["rc_low"]   = _safe_float(bar.get("l"))
                    rc["rc_close"] = _safe_float(bar.get("c"))

        # derive body/size/direction if we have OHLC
        o, h, l, c = rc["rc_open"], rc["rc_high"], rc["rc_low"], rc["rc_close"]
        if None not in (o, h, l, c):
            rng = (h - l) or 0.0
            rc["rc_size_pips"] = round(rng / _pip(symbol), 1) if rng else 0.0
            rc["rc_body_pct"]  = round(abs(c - o) / rng * 100.0, 1) if rng else 0.0
            rc["rc_direction"] = "BULL" if c > o else ("BEAR" if c < o else "DOJI")
        return rc
    except Exception as e:
        log.warning("analytics: reversal_candle read failed for %s: %s", symbol, e)
        return rc


# ── Phase-F §14: full liquidity breakdown from liq_detail/bsl_ssl ────────────
def read_liquidity_detail(liq_res: dict, side: str) -> dict:
    """Extract EQH/EQL, session liquidity, swept flags, sweep direction from the
    detect_liq_signals result (already computed in read_liquidity_at_ack)."""
    out = {"equal_highs": None, "equal_lows": None, "liquidity_pool_count": None,
           "session_liquidity": None, "sweep_direction": None,
           "bsl_level": None, "ssl_level": None}
    try:
        det = (liq_res or {}).get("_detail") or {}
        eqs = det.get("eq_levels") or []
        eqh = [e for e in eqs if "EQH" in str(e.get("type","")).upper() or "BSL" in str(e.get("type","")).upper()]
        eql = [e for e in eqs if "EQL" in str(e.get("type","")).upper() or "SSL" in str(e.get("type","")).upper()]
        out["equal_highs"] = len(eqh) or None
        out["equal_lows"]  = len(eql) or None
        sess = det.get("session") or []
        out["liquidity_pool_count"] = (len(eqs) + len(sess)) or None
        out["session_liquidity"] = [
            {"level": s.get("level"), "session": s.get("session"),
             "swept": s.get("swept"), "swept_by": s.get("swept_by")}
            for s in sess[:6]
        ] or None
        bs = (liq_res or {}).get("_bsl_ssl") or {}
        bsl = bs.get("bsl") if isinstance(bs.get("bsl"), dict) else {}
        ssl = bs.get("ssl") if isinstance(bs.get("ssl"), dict) else {}
        out["bsl_level"] = _safe_float(bsl.get("level"))
        out["ssl_level"] = _safe_float(ssl.get("level"))
        # sweep direction: which side was swept
        bsl_swept = bool(bsl.get("swept")); ssl_swept = bool(ssl.get("swept"))
        if bsl_swept and not ssl_swept: out["sweep_direction"] = "BSL"   # buy-side taken
        elif ssl_swept and not bsl_swept: out["sweep_direction"] = "SSL" # sell-side taken
        elif bsl_swept and ssl_swept: out["sweep_direction"] = "BOTH"
    except Exception as e:
        log.warning("analytics: liquidity_detail failed: %s", e)
    return out


# ── setup quality flags — testable risk hypotheses frozen at entry ───────────
# NOTE: these are HYPOTHESES to validate against outcomes, not proven rules.
# 1H matters as REGIME (reversals need RANGE; TREND runs zones over).
# 4H matters as DIRECTION (fading against a strong higher-TF trend is risky).
QFLAG_WEAK_ZONE_SCORE   = 10.0   # zone_score below this = weak
QFLAG_LOW_LIQ_SCORE     = 2      # liquidity_score at/below this = thin
QFLAG_4H_ADX_TREND      = 25.0   # 4H ADX above this = meaningful trend


def _regime_dir_from(label_er_adx: dict, price_side_hint=None):
    """Rough 4H trend DIRECTION proxy: we only have label/er/adx, not slope, so we
    can't know up/down from regime alone. Return None (direction unknown) unless a
    caller supplies it. Kept simple: we flag 'strong 4H trend present', and the
    direction test is applied by the caller using zone_kind as a proxy."""
    return None


def compute_setup_quality_flags(snap_like: dict) -> list:
    """Compute risk flags from already-captured fields. Frozen at entry so a closed
    trade self-documents which quality concerns were present. Never raises."""
    flags = []
    try:
        d = snap_like or {}
        side = str(d.get("side") or "").upper()

        # ── 1H regime check: reversals want RANGE; TREND runs zones over ──
        r1 = d.get("regime_1h") or {}
        if isinstance(r1, dict) and str(r1.get("label") or "").upper() == "TREND":
            flags.append("1h_trending")            # zone likely run over (bad, any dir)

        # ── 4H direction check: fading against a strong 4H trend is the real veto ──
        r4 = d.get("regime_4h") or {}
        if isinstance(r4, dict) and str(r4.get("label") or "").upper() == "TREND":
            adx4 = _safe_float(r4.get("adx"), 0.0) or 0.0
            if adx4 >= QFLAG_4H_ADX_TREND:
                # direction proxy: a reversal SELL fades at resistance (betting price
                # falls) — risky if 4H trend is UP; a BUY fades at support — risky if
                # 4H trend is DOWN. We lack 4H slope here, so flag the STRONG-4H-trend
                # condition and let analysis confirm direction correlation.
                flags.append("strong_4h_trend")
                # zone_kind gives a weak direction hint: fading resistance in a strong
                # 4H uptrend, or support in a strong 4H downtrend, is the classic trap.
                zk = str(d.get("zone_kind") or "").lower()
                if (side == "SELL" and "resist" in zk) or (side == "BUY" and "support" in zk):
                    flags.append("reversal_vs_strong_4h")

        # ── zone quality ──
        zs = _safe_float(d.get("zone_score"))
        if zs is not None and zs < QFLAG_WEAK_ZONE_SCORE:
            flags.append("weak_zone")

        # ── liquidity / sweep ──
        ls = _safe_float(d.get("liquidity_score"))
        if ls is not None and ls <= QFLAG_LOW_LIQ_SCORE:
            flags.append("low_liquidity")
        if d.get("sweep_detected") is False:
            flags.append("no_sweep")

        # ── against reliable drift (already computed) ──
        if d.get("against_drift") is True:
            flags.append("against_drift")

        # ── entered far from zone (context; XAUUSD reads in 0.01 units) ──
        dz = _safe_float(d.get("dist_to_zone_pips"))
        if dz is not None and dz > 20 and str(d.get("symbol") or "").upper() != "XAUUSD":
            flags.append("far_from_zone")

    except Exception as e:
        log.warning("analytics: quality flags failed: %s", e)
    return flags


# ── Phase-F §11: news_block context (shadow read — observes, never blocks) ───
def read_news_at_ack(symbol: str, ts_ms: int) -> dict:
    """Record whether this entry sat near a scheduled high-impact event, using the
    canonical check_news_block() in SHADOW mode (reports, does not block). Captures
    the news context the trade was taken under — answers later 'do near-news entries
    underperform?'. Never raises."""
    out = {"news_block": None, "news_verdict": None, "news_event": None,
           "news_minutes_to_event": None, "news_window": None, "news_impact": None}
    try:
        if not symbol:
            return out
        from api.news_adapter import check_news_block
        R = from_app_R()
        res = check_news_block(symbol, int(ts_ms or _now_ms()), R, shadow_mode=True) or {}
        out["news_block"]            = bool(res.get("block"))
        out["news_verdict"]          = res.get("verdict")
        out["news_event"]            = res.get("event_name")
        out["news_minutes_to_event"] = res.get("minutes_to_event")
        out["news_window"]           = res.get("window")
        out["news_impact"]           = res.get("impact")
        # Freeze upcoming same-currency high-impact events (next 48h) so the
        # during-trade check at close is TTL-proof (calendar may expire by then).
        out["upcoming_events"] = _freeze_currency_events(symbol, int(ts_ms or _now_ms()))
    except Exception as e:
        log.warning("analytics: news read failed for %s: %s", symbol, e)
    return out


def _currencies_for_symbol(symbol: str) -> set:
    s = (symbol or "").upper()
    out = set()
    if len(s) >= 6:
        out.add(s[:3]); out.add(s[3:6])
    return out


def _freeze_currency_events(symbol: str, now_ms: int, horizon_h: int = 48) -> list:
    """Snapshot upcoming high-impact events for THIS symbol's currencies, so the
    during-trade news check at finalize doesn't depend on the calendar TTL."""
    frozen = []
    try:
        R = from_app_R()
        raw = R.get("xtl:news:calendar:daily")
        if not raw:
            return frozen
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", "ignore")
        data = json.loads(raw)
        evs = data.get("events") if isinstance(data, dict) else None
        if not isinstance(evs, list):
            return frozen
        ccys = _currencies_for_symbol(symbol)
        horizon = now_ms + horizon_h * 3_600_000
        for e in evs:
            cur = str(e.get("currency") or "").upper()
            t = int(e.get("time_ms") or 0)
            if cur in ccys and now_ms <= t <= horizon:
                frozen.append({
                    "event": e.get("event"), "currency": cur, "time_ms": t,
                    "impact": e.get("impact"),
                    "pre_block_min": e.get("pre_block_min"),
                    "post_block_min": e.get("post_block_min"),
                    "stabilization_min": e.get("stabilization_min"),
                })
    except Exception as e:
        log.warning("analytics: freeze events failed for %s: %s", symbol, e)
    return frozen


# ── Phase-F: FTMO risk state + account snapshot (canonical source) ───────────
def read_ftmo_state_at_ack(profile_id: str) -> dict:
    """Freeze the EXACT FTMO/risk state the engine used, from the canonical
    _get_prop_risk_state(). It is the single source of truth (no duplicate math).
    Its internal stale-reservation cleanup is engine housekeeping, not an
    analytics write. Returns a flat dict; never raises."""
    out = {}
    try:
        from api.trend_endpoints import _get_prop_risk_state
        rs = _get_prop_risk_state(profile_id) or {}
        # §7 FTMO state
        out["drawdown_pct"]            = rs.get("drawdown_pct")
        out["drawdown_band"]           = rs.get("drawdown_band")
        out["effective_risk_pct"]      = rs.get("effective_risk_pct")
        out["daily_r"]                 = rs.get("daily_r")
        out["daily_loss_used"]         = rs.get("daily_loss_used")
        out["daily_loss_remaining"]    = rs.get("ftmo_daily_loss_remaining")
        out["daily_loss_limit"]        = rs.get("ftmo_daily_loss_limit")
        out["daily_risk_reserved"]     = rs.get("daily_risk_reserved")
        out["projected_daily_loss_if_all_sl"] = rs.get("projected_daily_loss_if_all_sl")
        out["consecutive_losing_days"] = rs.get("consecutive_losing_days")
        out["trading_halted"]          = rs.get("trading_halted")
        out["halt_reason"]             = rs.get("halt_reason")
        out["wins_today"]              = rs.get("wins_today")
        out["losses_today"]            = rs.get("losses_today")
        # §8 account-core (engine's view) / §10 portfolio
        out["broker_balance"]          = rs.get("broker_balance")
        out["broker_equity"]           = rs.get("broker_equity")
        out["floating_pnl"]            = rs.get("floating_pnl")
        out["start_balance"]           = rs.get("start_balance")
        out["open_positions_count"]    = rs.get("open_positions_count")
        out["open_risk_usd"]           = rs.get("open_risk_usd")
    except Exception as e:
        log.warning("analytics: ftmo_state read failed: %s", e)
    return out


def read_account_at_ack(device_id: str, account_type: str = "demo") -> dict:
    """Full account snapshot (margin/leverage/login/server) from the device's
    account key. The :last: variant is a POINTER to the real key — resolve it."""
    out = {}
    try:
        if not device_id:
            return out
        R = from_app_R()
        acct = (account_type or "demo")
        # canonical key; fall back to resolving the :last: pointer
        raw = R.get(f"xtl:mt5:account:{device_id}:{acct}")
        if not raw:
            ptr = R.get(f"xtl:mt5:account:last:{device_id}:{acct}")
            if ptr:
                if isinstance(ptr, (bytes, bytearray)):
                    ptr = ptr.decode("utf-8", "ignore")
                raw = R.get(ptr.strip())
        if not raw:
            return out
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", "ignore")
        d = json.loads(raw)
        if isinstance(d, str):
            d = json.loads(d)
        bal = _safe_float(d.get("balance")); eq = _safe_float(d.get("equity"))
        mar = _safe_float(d.get("margin")); fm = _safe_float(d.get("free_margin"))
        out["account_login"]   = d.get("login")
        out["account_server"]  = d.get("server")
        out["account_currency"] = d.get("currency")
        out["leverage"]        = d.get("leverage")
        out["balance"]         = bal
        out["equity"]          = eq
        out["used_margin"]     = mar
        out["free_margin"]     = fm
        out["margin_level"]    = _safe_float(d.get("margin_level"))
        # margin utilization % = used / equity
        if eq and eq > 0 and mar is not None:
            out["margin_utilization_pct"] = round(mar / eq * 100.0, 2)
    except Exception as e:
        log.warning("analytics: account read failed for %s: %s", device_id, e)
    return out

# ─────────────────────────────────────────────────────────────────────────────
# H1 20-bar entry-direction helpers
#
# Paste both functions into /opt/xauapi/api/xtl_analytics.py directly ABOVE
#   def build_entry_snapshot(pos: dict, capture_source: str = "normal") -> dict:
#
# They depend only on _safe_float, which already exists in that module.
#
# Emitted fields (added to the entry snapshot):
#   h1_20_direction      gated label  : UP/DOWN only if |net|>=1 ATR AND r2>=gate, else SIDEWAYS
#   h1_20_direction_raw  ungated      : UP/DOWN/SIDEWAYS by net_atr sign alone
#   h1_20_net_atr        net close-to-close move over the window, in ATR units
#   h1_20_slope_atr      least-squares slope per bar, in ATR units
#   h1_20_r2             0..1, how linear/clean the move was (trend vs chop)
#   h1_20_bars_used      how many closes actually went into the calc
#   h1_20_vs_trade       WITH/AGAINST/NEUTRAL — trade side vs GATED direction
# ─────────────────────────────────────────────────────────────────────────────


def _h1_window_direction(bars_h1, atr, n=20, r2_gate=0.20):
    """Overall direction of the last n H1 candles at entry.

    Measures net displacement + regression slope (both ATR-normalized) rather
    than counting candle colors, so a clean trend and a choppy round-trip that
    ends flat are correctly distinguished. A weak move that only *happens* to
    close higher (low r2) is labeled SIDEWAYS by the gate, while the raw label
    preserves the pure net-sign read for analysis.

    bars_h1 : chronological, oldest -> newest; bars_h1[-1] is the entry bar.
              (These bars carry OHLC under 'o'/'h'/'l'/'c'; only 'c' is used.)
    atr     : entry-frozen H1 ATR (from liq.get('atr')).
    Returns {} if data is missing or too thin to be meaningful.
    """
    try:
        if not bars_h1 or atr is None or atr <= 0:
            return {}
        window = bars_h1[-n:]
        closes = [_safe_float(b.get("c")) for b in window]
        closes = [c for c in closes if c is not None]
        m = len(closes)
        if m < max(5, n // 2):          # need enough bars to mean anything
            return {}

        # 1) net close-to-close move over the window, in ATR units
        net_atr = (closes[-1] - closes[0]) / atr

        # 2) least-squares slope over the window (per bar), in ATR units
        xs = list(range(m))
        mx = sum(xs) / m
        my = sum(closes) / m
        sxx = sum((x - mx) ** 2 for x in xs)
        sxy = sum((xs[i] - mx) * (closes[i] - my) for i in range(m))
        slope = (sxy / sxx) if sxx else 0.0

        # 3) r^2 — how linear the move was (0 choppy .. 1 clean trend)
        ss_tot = sum((c - my) ** 2 for c in closes)
        if ss_tot > 0:
            ss_res = sum(
                (closes[i] - (my + slope * (xs[i] - mx))) ** 2
                for i in range(m)
            )
            r2 = max(0.0, 1.0 - ss_res / ss_tot)
        else:
            r2 = 0.0

        # raw label: pure net-sign; gated label: also requires a real trend (r2)
        raw = "UP" if net_atr >= 1.0 else ("DOWN" if net_atr <= -1.0 else "SIDEWAYS")
        gated = raw if (raw in ("UP", "DOWN") and r2 >= r2_gate) else "SIDEWAYS"
        # -------------------------------------------------
        # Trend tilt (analytics only)
        #
        # Unlike h1_20_direction, tilt does NOT require a
        # 1 ATR displacement or high R².
        #
        # It answers:
        # "If there is any directional bias at all,
        # which way is it leaning?"
        #
        # Examples:
        #   Direction = SIDEWAYS
        #   Tilt      = UP
        # -------------------------------------------------

        slope_atr = slope / atr

        if slope_atr >= 0.01:
            tilt = "UP"

        elif slope_atr <= -0.01:
            tilt = "DOWN"

        else:
            tilt = "FLAT"

        return {
            "h1_20_direction":     gated,
            "h1_20_direction_raw": raw,

            # Analytics-only directional lean.
            "h1_20_tilt":          tilt,

            "h1_20_net_atr":       round(net_atr, 2),
            "h1_20_slope_atr":     round(slope_atr, 4),
            "h1_20_r2":            round(r2, 2),
            "h1_20_bars_used":     m,
        }
    except Exception:
        return {}


def _h1_dir_vs_trade(direction, side):
    """Did the trade side align with the (gated) 20-bar H1 direction?

    UP + BUY  -> WITH      UP + SELL  -> AGAINST
    DOWN+ SELL-> WITH      DOWN+ BUY  -> AGAINST
    SIDEWAYS / unknown     -> NEUTRAL

    This is the field to group by for "am I entering with or against the
    H1 trend, and does AGAINST correlate with losses / streaks."
    """
    side = (side or "").upper()
    if direction not in ("UP", "DOWN"):
        return "NEUTRAL"
    if side == "BUY":
        return "WITH" if direction == "UP" else "AGAINST"
    if side == "SELL":
        return "WITH" if direction == "DOWN" else "AGAINST"
    return "NEUTRAL"



def capture_dxy_market_snapshot(
    device_id: str,
) -> dict:
    """
    Freeze real broker DXY H1 market context.

    Market-only function:
      - knows the device
      - reads DXY H1 bars from that exact device
      - calculates the last-20-H1 direction
      - does not know the traded symbol or trade side
      - never scans or falls back to another device

    Analytics only. Never blocks trading.
    """

    dev = str(device_id or "").strip()

    out = {
        "dxy_available": False,
        "dxy_source": "broker_mt5",
        "dxy_symbol": "DXY",
        "dxy_device_id": dev or None,

        "dxy_last_closed_h1_close": None,
        "dxy_last_closed_h1_close_ms": None,

        "dxy_h1_20_direction": None,
        "dxy_h1_20_direction_raw": None,
        "dxy_h1_20_net_atr": None,
        "dxy_h1_20_slope_atr": None,
        "dxy_h1_20_r2": None,
        "dxy_h1_20_bars_used": None,
        "dxy_h1_20_tilt": None,
        "dxy_unavailable_reason": None,
    }

    try:
        if not dev:
            out["dxy_unavailable_reason"] = "MISSING_DEVICE_ID"
            return out

        R = from_app_R()

        key = (
            f"xtl:ohlc:snap:{dev}:DXY:H1"
        )

        raw = R.get(key)

        if not raw:
            out["dxy_unavailable_reason"] = (
                "DXY_H1_SNAPSHOT_NOT_FOUND"
            )
            return out

        if isinstance(
            raw,
            (bytes, bytearray),
        ):
            raw = raw.decode(
                "utf-8",
                "ignore",
            )

        payload = json.loads(raw)

        # Safely handle accidentally double-encoded JSON.
        if isinstance(payload, str):
            payload = json.loads(payload)

        if not isinstance(payload, dict):
            out["dxy_unavailable_reason"] = (
                "INVALID_DXY_SNAPSHOT"
            )
            return out

        bars = payload.get("bars") or []

        if not isinstance(bars, list):
            out["dxy_unavailable_reason"] = (
                "INVALID_DXY_BARS"
            )
            return out

        closed_bars = [
            bar
            for bar in bars
            if (
                isinstance(bar, dict)
                and bool(
                    bar.get("complete", True)
                )
            )
        ]

        if len(closed_bars) < 20:
            out["dxy_unavailable_reason"] = (
                "INSUFFICIENT_DXY_H1_BARS"
            )
            return out

        atr = _atr_from_bars(
            closed_bars
        )

        if not atr or atr <= 0:
            out["dxy_unavailable_reason"] = (
                "INVALID_DXY_ATR"
            )
            return out

        direction = _h1_window_direction(
            closed_bars,
            atr,
            n=20,
        )

        if not direction:
            out["dxy_unavailable_reason"] = (
                "DXY_DIRECTION_CALC_FAILED"
            )
            return out

        last_bar = closed_bars[-1]

        last_close = _safe_float(
            last_bar.get("c")
        )

        # This is the latest completed DXY H1 candle time.
        # It remains in the feed's broker-encoded time convention.
        last_close_ms = _norm_ms(
            last_bar.get("t_close_ms")
            or last_bar.get("t_open_ms")
            or last_bar.get("t")
            or 0
        )

        out.update({
            "dxy_available": True,

            "dxy_last_closed_h1_close": (
                last_close
            ),
            "dxy_last_closed_h1_close_ms": (
                last_close_ms or None
            ),

            "dxy_h1_20_direction": (
                direction.get(
                    "h1_20_direction"
                )
            ),
            "dxy_h1_20_direction_raw": (
                direction.get(
                    "h1_20_direction_raw"
                )
            ),
            "dxy_h1_20_net_atr": (
                direction.get(
                    "h1_20_net_atr"
                )
            ),
            "dxy_h1_20_slope_atr": (
                direction.get(
                    "h1_20_slope_atr"
                )
            ),
            "dxy_h1_20_r2": (
                direction.get(
                    "h1_20_r2"
                )
            ),
            "dxy_h1_20_tilt": (
                direction.get(
                    "h1_20_tilt"
                )
            ),
            "dxy_h1_20_bars_used": (
                direction.get(
                    "h1_20_bars_used"
                )
            ),

            "dxy_unavailable_reason": None,
        })

        return out

    except Exception as exc:
        out["dxy_unavailable_reason"] = (
            "DXY_MARKET_CAPTURE_EXCEPTION:"
            f"{type(exc).__name__}:{exc}"
        )

        log.warning(
            "analytics: DXY market capture failed "
            "device=%s err=%r",
            device_id,
            exc,
        )

        return out



def build_synthetic_dxy_h1_bars(
    device_id: str,
    max_bars: int = 300,
) -> list:
    """
    Build a DXY-like H1 OHLC series from the same device's USD pairs.

    USD quote pairs are inverted:
      EURUSD, GBPUSD

    USD base pairs are kept:
      USDJPY, USDCHF, USDCAD

    Each component is normalized to 100 at its first usable close.
    At least 3 pair contributions are required for each synthetic bar.

    Analytics only. Never scans or borrows another device.
    """

    dev = str(device_id or "").strip()

    if not dev:
        return []

    pair_signs = {
        "EURUSD": -1,
        "GBPUSD": -1,
        "USDJPY": +1,
        "USDCHF": +1,
        "USDCAD": +1,
    }

    try:
        R = from_app_R()

        pair_maps = {}
        pair_bases = {}

        for symbol, sign in pair_signs.items():
            key = (
                f"xtl:ohlc:snap:{dev}:"
                f"{symbol}:H1"
            )

            raw = R.get(key)
            if not raw:
                continue

            if isinstance(raw, (bytes, bytearray)):
                raw = raw.decode(
                    "utf-8",
                    "ignore",
                )

            payload = json.loads(raw)

            if isinstance(payload, str):
                payload = json.loads(payload)

            if not isinstance(payload, dict):
                continue

            bars = payload.get("bars") or []
            if not isinstance(bars, list):
                continue

            bar_map = {}

            for bar in bars[-int(max_bars or 300):]:
                if not isinstance(bar, dict):
                    continue

                if not bool(
                    bar.get("complete", True)
                ):
                    continue

                ts_ms = _norm_ms(
                    bar.get("t_open_ms")
                    or bar.get("t")
                    or 0
                )

                open_px = _safe_float(
                    bar.get("o")
                )
                high_px = _safe_float(
                    bar.get("h")
                )
                low_px = _safe_float(
                    bar.get("l")
                )
                close_px = _safe_float(
                    bar.get("c")
                )

                if (
                    not ts_ms
                    or open_px is None
                    or high_px is None
                    or low_px is None
                    or close_px is None
                    or open_px <= 0
                    or high_px <= 0
                    or low_px <= 0
                    or close_px <= 0
                ):
                    continue

                bar_map[int(ts_ms)] = {
                    "o": open_px,
                    "h": high_px,
                    "l": low_px,
                    "c": close_px,
                }

            if not bar_map:
                continue

            first_ts = min(bar_map)
            first_close = _safe_float(
                bar_map[first_ts].get("c")
            )

            if (
                first_close is None
                or first_close <= 0
            ):
                continue

            pair_maps[symbol] = {
                "sign": sign,
                "bars": bar_map,
            }

            pair_bases[symbol] = first_close

        if len(pair_maps) < 3:
            return []

        all_timestamps = sorted({
            ts
            for rec in pair_maps.values()
            for ts in rec["bars"].keys()
        })

        synthetic = []

        for ts_ms in all_timestamps:
            open_components = []
            high_components = []
            low_components = []
            close_components = []
            contributors = []

            for symbol, rec in pair_maps.items():
                bar = rec["bars"].get(ts_ms)
                if not bar:
                    continue

                base = pair_bases.get(symbol)
                sign = int(rec.get("sign") or 0)

                if not base or sign not in (-1, +1):
                    continue

                o = _safe_float(bar.get("o"))
                h = _safe_float(bar.get("h"))
                l = _safe_float(bar.get("l"))
                c = _safe_float(bar.get("c"))

                if (
                    o is None
                    or h is None
                    or l is None
                    or c is None
                    or min(o, h, l, c) <= 0
                ):
                    continue

                if sign == +1:
                    # USD is base:
                    # rising pair means stronger USD.
                    component_o = 100.0 * (o / base)
                    component_h = 100.0 * (h / base)
                    component_l = 100.0 * (l / base)
                    component_c = 100.0 * (c / base)

                else:
                    # USD is quote:
                    # falling pair means stronger USD.
                    #
                    # Inversion reverses high and low:
                    # inverted high comes from original low.
                    component_o = 100.0 * (base / o)
                    component_h = 100.0 * (base / l)
                    component_l = 100.0 * (base / h)
                    component_c = 100.0 * (base / c)

                open_components.append(
                    component_o
                )
                high_components.append(
                    component_h
                )
                low_components.append(
                    component_l
                )
                close_components.append(
                    component_c
                )
                contributors.append(
                    symbol
                )

            if len(close_components) < 3:
                continue

            synthetic_open = (
                sum(open_components)
                / len(open_components)
            )
            synthetic_high = (
                sum(high_components)
                / len(high_components)
            )
            synthetic_low = (
                sum(low_components)
                / len(low_components)
            )
            synthetic_close = (
                sum(close_components)
                / len(close_components)
            )

            # Preserve valid OHLC ordering after averaging.
            synthetic_high = max(
                synthetic_high,
                synthetic_open,
                synthetic_close,
            )

            synthetic_low = min(
                synthetic_low,
                synthetic_open,
                synthetic_close,
            )

            synthetic.append({
                "t": int(ts_ms // 1000),
                "t_open_ms": int(ts_ms),
                "t_close_ms": int(
                    ts_ms + 3_600_000
                ),

                "o": round(
                    synthetic_open,
                    6,
                ),
                "h": round(
                    synthetic_high,
                    6,
                ),
                "l": round(
                    synthetic_low,
                    6,
                ),
                "c": round(
                    synthetic_close,
                    6,
                ),

                "complete": True,
                "synthetic": True,
                "synthetic_pair_count": len(
                    contributors
                ),
                "synthetic_pairs": contributors,
            })

        synthetic.sort(
            key=lambda bar: int(
                bar.get("t_open_ms") or 0
            )
        )

        return synthetic

    except Exception as exc:
        log.warning(
            "analytics: synthetic DXY build failed "
            "device=%s err=%r",
            device_id,
            exc,
        )
        return []

def compute_dxy_trade_alignment(
    trade_symbol: str,
    trade_side: str,
    dxy_direction: str,
) -> str:
    """
    Compare a trade direction with the frozen DXY direction.

    Contains no Redis or OHLC reads.

    DXY UP:
      EURUSD/GBPUSD BUY  -> ADVERSE
      EURUSD/GBPUSD SELL -> FAVORABLE

      USDJPY/USDCHF/USDCAD BUY  -> FAVORABLE
      USDJPY/USDCHF/USDCAD SELL -> ADVERSE

    DXY DOWN reverses those relationships.

    XAUUSD remains analytics-only for Phase 1.
    """

    try:
        symbol = str(
            trade_symbol or ""
        ).upper().strip()

        side = str(
            trade_side or ""
        ).upper().strip()

        direction = str(
            dxy_direction or ""
        ).upper().strip()

        if symbol == "XAUUSD":
            return "ANALYTICS_ONLY"

        if side not in (
            "BUY",
            "SELL",
        ):
            return "UNKNOWN"

        if direction not in (
            "UP",
            "DOWN",
        ):
            return "NEUTRAL"

        usd_base_pairs = {
            "USDJPY",
            "USDCHF",
            "USDCAD",
        }

        usd_quote_pairs = {
            "EURUSD",
            "GBPUSD",
        }

        if symbol in usd_base_pairs:
            favorable = (
                (
                    side == "BUY"
                    and direction == "UP"
                )
                or
                (
                    side == "SELL"
                    and direction == "DOWN"
                )
            )

        elif symbol in usd_quote_pairs:
            favorable = (
                (
                    side == "BUY"
                    and direction == "DOWN"
                )
                or
                (
                    side == "SELL"
                    and direction == "UP"
                )
            )

        else:
            return "UNKNOWN"

        return (
            "FAVORABLE"
            if favorable
            else "ADVERSE"
        )

    except Exception as exc:
        log.warning(
            "analytics: DXY alignment failed "
            "symbol=%s side=%s direction=%s err=%r",
            trade_symbol,
            trade_side,
            dxy_direction,
            exc,
        )

        return "UNKNOWN"

def capture_usd_reference_snapshot(
    device_id: str,
    trade_symbol: str,
    trade_side: str,
) -> dict:
    """
    Produce one unified USD-reference snapshot for a trade.

    Priority:
      1. Real broker DXY from the exact trade device.
      2. Synthetic DXY built from that same device's five USD pairs.

    Both sources use _h1_window_direction() with:
      - 20 H1 bars
      - ATR normalization
      - net displacement
      - regression slope
      - R² gate
      - trend tilt

    Analytics only. Never changes zones, gates or execution.
    """

    dev = str(device_id or "").strip()

    out = {
        "usd_reference_available": False,
        "usd_reference_source": None,
        "usd_reference_device_id": dev or None,

        "usd_reference_last_closed_h1_close": None,
        "usd_reference_last_closed_h1_close_ms": None,

        "usd_reference_direction": None,
        "usd_reference_direction_raw": None,
        "usd_reference_tilt": None,
        "usd_reference_net_atr": None,
        "usd_reference_slope_atr": None,
        "usd_reference_r2": None,
        "usd_reference_bars_used": None,

        "usd_reference_alignment_at_entry": "UNKNOWN",

        "usd_reference_synthetic_pair_count": None,
        "usd_reference_synthetic_pairs": None,

        "usd_reference_unavailable_reason": None,
    }

    try:
        if not dev:
            out["usd_reference_unavailable_reason"] = (
                "MISSING_DEVICE_ID"
            )
            return out

        # -------------------------------------------------
        # 1. Prefer real DXY from this exact device.
        # -------------------------------------------------
        real_dxy = capture_dxy_market_snapshot(
            device_id=dev,
        )

        if real_dxy.get("dxy_available"):
            direction = str(
                real_dxy.get(
                    "dxy_h1_20_direction"
                )
                or ""
            ).upper().strip()

            out.update({
                "usd_reference_available": True,
                "usd_reference_source": (
                    "REAL_BROKER_DXY"
                ),
                "usd_reference_device_id": dev,

                "usd_reference_last_closed_h1_close": (
                    real_dxy.get(
                        "dxy_last_closed_h1_close"
                    )
                ),
                "usd_reference_last_closed_h1_close_ms": (
                    real_dxy.get(
                        "dxy_last_closed_h1_close_ms"
                    )
                ),

                "usd_reference_direction": (
                    real_dxy.get(
                        "dxy_h1_20_direction"
                    )
                ),
                "usd_reference_direction_raw": (
                    real_dxy.get(
                        "dxy_h1_20_direction_raw"
                    )
                ),
                "usd_reference_tilt": (
                    real_dxy.get(
                        "dxy_h1_20_tilt"
                    )
                ),
                "usd_reference_net_atr": (
                    real_dxy.get(
                        "dxy_h1_20_net_atr"
                    )
                ),
                "usd_reference_slope_atr": (
                    real_dxy.get(
                        "dxy_h1_20_slope_atr"
                    )
                ),
                "usd_reference_r2": (
                    real_dxy.get(
                        "dxy_h1_20_r2"
                    )
                ),
                "usd_reference_bars_used": (
                    real_dxy.get(
                        "dxy_h1_20_bars_used"
                    )
                ),

                "usd_reference_alignment_at_entry": (
                    compute_dxy_trade_alignment(
                        trade_symbol=trade_symbol,
                        trade_side=trade_side,
                        dxy_direction=direction,
                    )
                ),

                "usd_reference_unavailable_reason": None,
            })

            return out

        # -------------------------------------------------
        # 2. Same-device synthetic DXY fallback.
        # -------------------------------------------------
        synthetic_bars = (
            build_synthetic_dxy_h1_bars(
                device_id=dev,
                max_bars=300,
            )
        )

        if len(synthetic_bars) < 20:
            out["usd_reference_source"] = (
                "SYNTHETIC_USD_BASKET"
            )
            out["usd_reference_unavailable_reason"] = (
                "INSUFFICIENT_SYNTHETIC_DXY_H1_BARS"
            )
            return out

        synthetic_atr = _atr_from_bars(
            synthetic_bars
        )

        if not synthetic_atr or synthetic_atr <= 0:
            out["usd_reference_source"] = (
                "SYNTHETIC_USD_BASKET"
            )
            out["usd_reference_unavailable_reason"] = (
                "INVALID_SYNTHETIC_DXY_ATR"
            )
            return out

        synthetic_direction = (
            _h1_window_direction(
                synthetic_bars,
                synthetic_atr,
                n=20,
            )
            or {}
        )

        if not synthetic_direction:
            out["usd_reference_source"] = (
                "SYNTHETIC_USD_BASKET"
            )
            out["usd_reference_unavailable_reason"] = (
                "SYNTHETIC_DXY_DIRECTION_CALC_FAILED"
            )
            return out

        last_bar = synthetic_bars[-1]

        direction = str(
            synthetic_direction.get(
                "h1_20_direction"
            )
            or ""
        ).upper().strip()

        out.update({
            "usd_reference_available": True,
            "usd_reference_source": (
                "SYNTHETIC_USD_BASKET"
            ),
            "usd_reference_device_id": dev,

            "usd_reference_last_closed_h1_close": (
                _safe_float(
                    last_bar.get("c")
                )
            ),
            "usd_reference_last_closed_h1_close_ms": (
                _norm_ms(
                    last_bar.get("t_close_ms")
                    or last_bar.get("t_open_ms")
                    or last_bar.get("t")
                    or 0
                )
            ),

            "usd_reference_direction": (
                synthetic_direction.get(
                    "h1_20_direction"
                )
            ),
            "usd_reference_direction_raw": (
                synthetic_direction.get(
                    "h1_20_direction_raw"
                )
            ),
            "usd_reference_tilt": (
                synthetic_direction.get(
                    "h1_20_tilt"
                )
            ),
            "usd_reference_net_atr": (
                synthetic_direction.get(
                    "h1_20_net_atr"
                )
            ),
            "usd_reference_slope_atr": (
                synthetic_direction.get(
                    "h1_20_slope_atr"
                )
            ),
            "usd_reference_r2": (
                synthetic_direction.get(
                    "h1_20_r2"
                )
            ),
            "usd_reference_bars_used": (
                synthetic_direction.get(
                    "h1_20_bars_used"
                )
            ),

            "usd_reference_alignment_at_entry": (
                compute_dxy_trade_alignment(
                    trade_symbol=trade_symbol,
                    trade_side=trade_side,
                    dxy_direction=direction,
                )
            ),

            "usd_reference_synthetic_pair_count": (
                last_bar.get(
                    "synthetic_pair_count"
                )
            ),
            "usd_reference_synthetic_pairs": (
                last_bar.get(
                    "synthetic_pairs"
                )
            ),

            "usd_reference_unavailable_reason": None,
        })

        return out

    except Exception as exc:
        out["usd_reference_unavailable_reason"] = (
            "USD_REFERENCE_CAPTURE_EXCEPTION:"
            f"{type(exc).__name__}:{exc}"
        )

        log.warning(
            "analytics: USD reference capture failed "
            "device=%s symbol=%s side=%s err=%r",
            device_id,
            trade_symbol,
            trade_side,
            exc,
        )

        return out

# ── ENTRY: build snapshot from a pos/repaired record ─────────────────────────
def build_entry_snapshot(pos: dict, capture_source: str = "normal") -> dict:
    """Map a pos (clean) or repaired record -> frozen entry snapshot.
    Provenance-aware; reads prop from the prop_check dict so it works for both the
    OK (clean) and ALLOW (repair) verdicts. Never raises."""
    try:
        p = pos or {}
        sym  = str(p.get("symbol") or "").upper()
        side = str(p.get("side") or "").upper()
        ticket = _extract_ticket(p)

        entry = _safe_float(p.get("entry_price")) or _safe_float(p.get("mt5_fill_price"))
        sl = _safe_float(p.get("sl_price"))
        tp = _safe_float(p.get("tp_price"))

        # zone: prefer nested entry_zone dict (carries score/touches), else flat fields
        z = p.get("entry_zone") if isinstance(p.get("entry_zone"), dict) else {}
        zone_level   = _safe_float(z.get("level") if z else p.get("entry_zone_level"))
        zone_low     = _safe_float(z.get("low")   if z else p.get("entry_zone_low"))
        zone_high    = _safe_float(z.get("high")  if z else p.get("entry_zone_high"))
        zone_score   = _safe_float(z.get("sr_score") or z.get("score")) if z else None
        zone_touches = z.get("touches") if z else None

        # prop: read from the prop_check dict (path-agnostic)
        pc = p.get("prop_check") if isinstance(p.get("prop_check"), dict) else {}
        risk_usd   = _safe_float(pc.get("risk_usd")  or p.get("risk_usd")  or p.get("prop_risk_usd"))
        risk_pct   = _safe_float(pc.get("risk_pct")  or p.get("risk_pct")  or p.get("prop_risk_pct"))
        target_rr  = _safe_float(pc.get("target_rr") or p.get("target_rr"))
        planned_rr = _safe_float(pc.get("planned_rr") or p.get("planned_rr"))

        # provenance: explicit from caller (capture_source), with field-based fallback
        cs = str(capture_source or "").lower()
        if cs in ("broker_repair", "repair"):
            provenance = "broker_repair"
        elif cs in ("normal", "clean"):
            provenance = "clean"
        else:
            provenance = ("broker_repair" if (str(p.get("source") or "") == "broker_repair"
                          or bool(p.get("repair_source"))
                          or str(p.get("trade_id") or "").startswith("BROKER_REPAIR:")) else "clean")
        is_repair = (provenance == "broker_repair")

        ets = int(p.get("opened_at_ms") or _now_ms())
        session = _session_for_ts_ms(ets, LIVE_TZ_OFFSET_H)

        dist_pips = None
        if entry is not None and zone_level:
            dist_pips = round(abs(entry - zone_level) / _pip(sym), 1)

        regime = read_regime_at_ack(
            sym,
            p.get("device_id"),
        )

        liq = read_liquidity_at_ack(
            sym,
            side,
            (z or None),
            p.get("device_id"),
            entry,
        )

        sr = read_sr_at_ack(sym)

        # -------------------------------------------------
        # XTL Evidence Bias — frozen at broker-confirmed entry.
        #
        # Shadow analytics only. This result never changes live
        # direction, zones, watches, gate, risk or execution.
        # -------------------------------------------------
        shadow_bias_snapshot = read_shadow_bias_at_ack(
            symbol=sym,
            side=side,
            device_id=p.get("device_id"),
            entry_price=entry,
            entry_zone=(z or None),
            computed_ms=ets,
        )

        drift = _drift_lookup(
            sym,
            session,
            side,
        )
        ftmo   = read_ftmo_state_at_ack(p.get("profile_id"))
        acct   = read_account_at_ack(p.get("device_id"),
                                     str(p.get("account_type") or "demo"))
        rc     = read_reversal_candle(sym, p.get("device_id"), p)
        liqdet = read_liquidity_detail(liq, side)
        news   = read_news_at_ack(sym, ets)
        news_day = read_news_day_context(sym, ets)

        # ── derivations (no new source) ──
        import datetime as _dt
        _d = _dt.datetime.fromtimestamp(ets / 1000.0, _dt.timezone.utc)
        weekday = _d.strftime("%A"); month = _d.month
        quarter = (month - 1) // 3 + 1; year = _d.year
        pipf = _pip(sym)
        stop_distance = round(abs(entry - sl) / pipf, 1) if (entry is not None and sl is not None) else None
        tp_distance   = round(abs(tp - entry) / pipf, 1) if (entry is not None and tp is not None) else None
        atr_pct = None
        _atrv = None
        try:
            _atrv = liq.get("atr")
            if _atrv and entry: atr_pct = round(_atrv / entry * 100.0, 4)
        except Exception:
            pass
        dist_res = dist_sup = None
        try:
            if entry is not None:
                _br = sr.get("nearest_resistance"); _bs = sr.get("nearest_support")
                if _br: dist_res = round(abs(_br - entry) / pipf, 1)
                if _bs: dist_sup = round(abs(entry - _bs) / pipf, 1)
        except Exception:
            pass
        # richer zone fields from the nested entry_zone item
        zone_width = _safe_float(z.get("band_width")) if z else None
        zone_strength = _safe_float(z.get("strength")) if z else None
        zone_merged_tfs = z.get("merged_tfs") if z else None
        zone_reaction = _safe_float((z.get("major_reason") or {}).get("strength")) if isinstance(z, dict) and isinstance(z.get("major_reason"), dict) else None
        prop_obj = p.get("prop_check") if isinstance(p.get("prop_check"), dict) else {}

        snap = {
            "schema_version":   SCHEMA_VERSION,
            "trade_id":         p.get("trade_id"),
            "mt5_ticket":       str(ticket) if ticket else None,
            "profile_id":       p.get("profile_id"),
            "symbol":           sym,
            "side":             side,
            "entry_provenance": provenance,
            "capture_source":   str(capture_source or "normal"),
            "enqueue_timestamp": ets,
            "session":          session,

            # ── location ──
            "entry_price":      entry,
            "zone_level":       zone_level,
            "zone_low":         zone_low,
            "zone_high":        zone_high,
            "zone_score":       zone_score,
            "zone_touches":     zone_touches,
            "zone_tf":          (z.get("tf")   if z else p.get("entry_zone_tf")),
            "zone_kind":        (z.get("kind") if z else p.get("entry_zone_kind")),
            "dist_to_zone_pips": dist_pips,

            # ── regime (H1 entry TF, H4 confirmation TF, D1 context) ──
            "regime_1h":        (regime or {}).get("h1"),
            "regime_4h":        (regime or {}).get("h4"),
            "regime_1d":        (regime or {}).get("d1"),
            # flat, sortable regime scalars (so analysis buckets by raw ADX/ER,
            # not just the TREND/MIXED label — lets data find the real threshold)
            "regime_1h_label":  ((regime or {}).get("h1") or {}).get("label"),
            "regime_1h_adx":    _safe_float(((regime or {}).get("h1") or {}).get("adx")),
            "regime_1h_er":     _safe_float(((regime or {}).get("h1") or {}).get("er")),
            "regime_4h_label":  ((regime or {}).get("h4") or {}).get("label"),
            "regime_4h_adx":    _safe_float(((regime or {}).get("h4") or {}).get("adx")),
            "regime_4h_er":     _safe_float(((regime or {}).get("h4") or {}).get("er")),
            "regime_1d_label":  ((regime or {}).get("d1") or {}).get("label"),
            "regime_1d_adx":    _safe_float(((regime or {}).get("d1") or {}).get("adx")),
            "regime_1d_er":     _safe_float(((regime or {}).get("d1") or {}).get("er")),

            # ── liquidity + entry-frozen ATR (recomputed at entry) ──
            "liquidity_model":  liq.get("liquidity_model"),
            "liquidity_score":  liq.get("liquidity_score"),
            "sweep_detected":   liq.get("sweep_detected"),
            "atr":              liq.get("atr"),

            # ── XTL Evidence Bias at entry (shadow analytics only) ──
            #
            # Keep the complete payload for forensic analysis and replay.
            # Also expose the most important fields flat for easy pandas/
            # dashboard filtering.
            "shadow_bias_snapshot": shadow_bias_snapshot,

            "shadow_bias": (
                shadow_bias_snapshot.get("shadow_bias")
                if isinstance(shadow_bias_snapshot, dict)
                else "UNKNOWN"
            ),
            "shadow_bias_score": (
                shadow_bias_snapshot.get("shadow_bias_score")
                if isinstance(shadow_bias_snapshot, dict)
                else 0.0
            ),
            "shadow_bias_confidence": (
                shadow_bias_snapshot.get("shadow_bias_confidence")
                if isinstance(shadow_bias_snapshot, dict)
                else "NONE"
            ),
            "shadow_bias_relation": (
                shadow_bias_snapshot.get("shadow_bias_relation")
                if isinstance(shadow_bias_snapshot, dict)
                else "UNKNOWN"
            ),
            "shadow_bias_actionable": (
                bool(
                    shadow_bias_snapshot.get(
                        "shadow_bias_actionable"
                    )
                )
                if isinstance(shadow_bias_snapshot, dict)
                else False
            ),
            "shadow_bias_actionability_reason": (
                shadow_bias_snapshot.get(
                    "shadow_bias_actionability_reason"
                )
                if isinstance(shadow_bias_snapshot, dict)
                else "CAPTURE_NOT_AVAILABLE"
            ),
            "shadow_bias_engine_version": (
                shadow_bias_snapshot.get(
                    "bias_engine_version"
                )
                if isinstance(shadow_bias_snapshot, dict)
                else None
            ),

            # ── support / resistance (cheap read from cached SR bundle) ──
            "best_resistance":   sr.get("best_resistance"),
            "best_support":      sr.get("best_support"),
            "nearest_resistance": sr.get("nearest_resistance"),
            "nearest_support":   sr.get("nearest_support"),
            "live_price":        sr.get("sr_price"),
            "distance_to_resistance": dist_res,
            "distance_to_support":    dist_sup,

            # ── timing derivations (§6) ──
            "weekday":   weekday,
            "month":     month,
            "quarter":   quarter,
            "year":      year,

            # ── position geometry (§9) ──
            "stop_distance": stop_distance,
            "tp_distance":   tp_distance,

            # ── richer zone (§12) ──
            "zone_width":     zone_width,
            "zone_strength":  zone_strength,
            "zone_merged_tfs": zone_merged_tfs,
            "zone_reaction":  zone_reaction,

            # ── market-context derivation (§11) ──
            "atr_pct":   atr_pct,

            # ── news context @ entry (§11, shadow — observed not blocking) ──
            "news_block":            news.get("news_block"),
            "news_verdict":          news.get("news_verdict"),
            "news_event":            news.get("news_event"),
            "news_minutes_to_event": news.get("news_minutes_to_event"),
            "news_window":           news.get("news_window"),
            "news_impact":           news.get("news_impact"),
            "upcoming_events":       news.get("upcoming_events"),
            "news_day_has_high_impact": (
                news_day.get("news_day_has_high_impact")
            ),
            "news_day_events": news_day.get("news_day_events"),
            "nearest_news_event": news_day.get("nearest_news_event"),
            "nearest_news_time_ms": news_day.get("nearest_news_time_ms"),
            "nearest_news_distance_minutes": (
                news_day.get("nearest_news_distance_minutes")
            ),
            "nearest_news_relation": news_day.get(
                "nearest_news_relation"
            ),

            # ── FTMO risk state @ entry (§7/§8-core/§10) — canonical source ──
            "ftmo":      ftmo,

            # ── account snapshot @ entry (§8) ──
            "account":   acct,

            # ── prop object, UNFLATTENED (§16) ──
            "prop_check": prop_obj,

            # ── reversal candle (§13) ──
            "rc_open":      rc.get("rc_open"),
            "rc_high":      rc.get("rc_high"),
            "rc_low":       rc.get("rc_low"),
            "rc_close":     rc.get("rc_close"),
            "rc_body_pct":  rc.get("rc_body_pct"),
            "rc_size_pips": rc.get("rc_size_pips"),
            "rc_direction": rc.get("rc_direction"),
            "rc_open_ms":   rc.get("rc_open_ms"),
            "rc_found":     rc.get("rc_found"),
            "rc_source":    rc.get("rc_source"),
            "rc_capture_note": rc.get("rc_capture_note"),

            # ── full liquidity breakdown (§14) ──
            "equal_highs":        liqdet.get("equal_highs"),
            "equal_lows":         liqdet.get("equal_lows"),
            "liquidity_pool_count": liqdet.get("liquidity_pool_count"),
            "session_liquidity":  liqdet.get("session_liquidity"),
            "sweep_direction":    liqdet.get("sweep_direction"),
            "bsl_level":          liqdet.get("bsl_level"),
            "ssl_level":          liqdet.get("ssl_level"),

            # ── gate detail (§17) ──
            "watch_key":       p.get("watch_key") or (f"xtl:zone:watch:{sym}:{side}:{p.get('entry_zone_tf') or 'H1'}"),
            "gate_reason":     p.get("entry_gate_reason"),
            "selection_model": (z.get("selection_model") if z else None) or p.get("selection_model"),
            "execution_tf":    p.get("entry_zone_tf") or p.get("execution_tf"),

            # ── completeness marker (Phase-F final rec) ──
            "broker_verified": False,
            "broker_truth_upgraded": False,
            "broker_holding_minutes": None,

            "capture_status": {
                "entry_snapshot_complete": True,
                "exit_snapshot_complete": False,
                "broker_verified": False,
                "analytics_schema_version": SCHEMA_VERSION,
                "shadow_bias_captured": bool(
                    isinstance(shadow_bias_snapshot, dict)
                    and shadow_bias_snapshot.get(
                        "bias_engine_version"
                    )
                ),
                "shadow_bias_data_ok": bool(
                    isinstance(shadow_bias_snapshot, dict)
                    and shadow_bias_snapshot.get(
                        "shadow_bias_data_ok"
                    )
                ),
            },

            # ── direction ──
            "trigger_type":     p.get("trigger_type"),     # None on repair
            "trigger_level":    p.get("trigger_level"),
            "drift_signed":     drift.get("signed"),
            "drift_reliab":     drift.get("reliab"),
            "drift_direction":  drift.get("direction"),
            "against_drift":    drift.get("against"),

            # ── entry style ──
            "entry_style":      p.get("trigger_type") or ("broker_repair" if is_repair else None),

            # ── risk / plan ──
            "sl_price":         sl,
            "tp_price":         tp,
            "planned_rr":       planned_rr,
            "target_rr":        target_rr,
            "lots":             _safe_float(p.get("qty")),
            "risk_usd":         risk_usd,
            "risk_pct":         risk_pct,
            "prop_verdict":     pc.get("verdict"),
            "prop_firm":        pc.get("firm")  or p.get("prop_firm"),
            "prop_phase":       pc.get("phase") or p.get("prop_phase"),

            # ── provenance detail ──
            "device_id":        p.get("device_id"),
            "source":           p.get("source"),
        }
        
        # compute quality flags from the assembled snapshot (self-documenting risk)
        try:
            snap["setup_quality_flags"] = compute_setup_quality_flags(snap)
            snap["setup_quality_flag_count"] = len(snap["setup_quality_flags"])
        except Exception:
            snap["setup_quality_flags"] = []
            snap["setup_quality_flag_count"] = 0

        # ── USD-strength / bias capture at ENTRY (broker-independent, analytics-only) ──
        # Records what the synthetic dollar + pair trend were AT the moment of entry.
        # Non-fatal: never let this break entry capture.
        try:
            from api.usd_strength import macro_bias_for_trade
            snap.update(
                macro_bias_for_trade(
                    from_app_R(),
                    sym,
                    side,
                    device_id=p.get("device_id"),
                )
            )
        except Exception as _me:
            log.warning("analytics: usd_strength capture failed: %s", _me)
        # ── Weekend/session GAP context at ENTRY (broker-independent) ──
        # Records the last market-closure gap and whether this trade is a
        # continuation of it or a fade. Lets you segment SL clusters by gap
        # later instead of guessing. Non-fatal.
        try:
            from api.gap_detect import gap_context_for_trade
            snap.update(gap_context_for_trade(from_app_R(), sym, side))
        except Exception as _ge:
            log.warning("analytics: gap capture failed: %s", _ge)
        # ── H1 20-bar overall direction at entry (shadow analytics; no new source) ──
        try:
            from api.trend_endpoints import _get_closed_h1_bars
            _dir_bars = _get_closed_h1_bars(sym, p.get("device_id")) or []
            _h1dir = _h1_window_direction(_dir_bars, liq.get("atr"), n=20)
            if _h1dir:
                _h1dir["h1_20_vs_trade"] = _h1_dir_vs_trade(
                    _h1dir.get("h1_20_direction"), side)
                snap.update(_h1dir)
        except Exception as _he:
            log.warning("analytics: h1_20 direction capture failed: %s", _he)
        
        
        # ── Real DXY or same-device synthetic DXY reference ──
        try:
            # Preserve raw real-DXY fields where available.
            dxy_market = capture_dxy_market_snapshot(
                device_id=p.get("device_id"),
            )

            snap.update(
                dxy_market
            )

            snap["dxy_alignment_at_entry"] = (
                compute_dxy_trade_alignment(
                    trade_symbol=sym,
                    trade_side=side,
                    dxy_direction=dxy_market.get(
                        "dxy_h1_20_direction"
                    ),
                )
            )

            # Unified source selected for cross-firm analysis.
            usd_reference = (
                capture_usd_reference_snapshot(
                    device_id=p.get("device_id"),
                    trade_symbol=sym,
                    trade_side=side,
                )
            )

            snap.update(
                usd_reference
            )

        except Exception as _de:
            log.warning(
                "analytics: USD reference capture failed: %s",
                _de,
            )

            snap.setdefault(
                "dxy_alignment_at_entry",
                "UNKNOWN",
            )

            snap.setdefault(
                "usd_reference_available",
                False,
            )

            snap.setdefault(
                "usd_reference_alignment_at_entry",
                "UNKNOWN",
            )
        return snap
        
    except Exception as e:
        log.error("analytics: build_entry_snapshot failed: %s", e)
        return {
            "schema_version": SCHEMA_VERSION,
            "trade_id": (pos or {}).get("trade_id"),
            "mt5_ticket": (str(_extract_ticket(pos or {}) or "") or None),
            "symbol": (pos or {}).get("symbol"),
            "side": (pos or {}).get("side"),
            "entry_provenance": "unknown",
            "capture_source": str(capture_source or "normal"),
            "enqueue_timestamp": _now_ms(),
            "_build_error": str(e),
        }


def write_entry_snapshot(snap: dict) -> bool:
    """Persist the frozen entry snapshot. Idempotent caller should check R.exists.
    Returns True on success; never raises."""
    try:
        ticket = str(snap.get("mt5_ticket") or "").strip()
        if not ticket:
            log.warning("analytics: snapshot missing mt5_ticket; skipped")
            return False
        snap.setdefault("schema_version", SCHEMA_VERSION)
        snap.setdefault("enqueue_timestamp", _now_ms())
        snap["_status"] = "open"
        from_app_R().set(SNAP_PREFIX + ticket,
                         json.dumps(snap, separators=(",", ":")),
                         ex=SNAP_TTL_SEC)
        return True
    except Exception as e:
        log.error("analytics: write_entry_snapshot failed: %s", e)
        return False


def capture_entry(pos: dict, capture_source: str = "normal") -> bool:
    """One-call entry capture for the hooks: build + write, idempotent.
    `capture_source` is passed explicitly by the caller ("normal" | "broker_repair")
    rather than inferred. Safe to call every cycle — writes once per ticket.
    Never blocks trading."""
    try:
        ticket = _extract_ticket(pos or {})
        if not ticket:
            return False
        R = from_app_R()
        if R.exists(SNAP_PREFIX + str(ticket)):
            return False
        return write_entry_snapshot(build_entry_snapshot(pos, capture_source=capture_source))
    except Exception as e:
        log.warning("analytics: capture_entry skipped: %s", e)
        return False


# ── EXIT (Option B: classify from H1 bars) ───────────────────────────────────
def approximate_exit(snap: dict, bars_h1: list) -> dict:
    out = {
        "exit_source": "h1_bar_approx",
        "exit_confidence": "medium",
        "exit_price": None,
        "exit_reason": "manual",
        "realized_r": None,
        "close_timestamp": _now_ms(),
    }
    try:
        side  = (snap.get("side") or "").upper()
        entry = _safe_float(snap.get("entry_price"))
        sl    = _safe_float(snap.get("sl_price"))
        tp    = _safe_float(snap.get("tp_price"))
        rr    = _safe_float(snap.get("target_rr") or snap.get("planned_rr"), 0.0)
        if entry is None or sl is None or not bars_h1:
            return out

        entry_ts = int(snap.get("enqueue_timestamp") or 0)
        hit = None; hit_ts = None
        for b in bars_h1:
            t = int(b.get("t_close_ms") or b.get("t_open_ms") or b.get("t") or 0)
            if entry_ts and t < entry_ts:
                continue
            hi = _safe_float(b.get("h")); lo = _safe_float(b.get("l"))
            if hi is None or lo is None:
                continue
            tp_hit = tp is not None and (hi >= tp if side == "BUY" else lo <= tp)
            sl_hit = (lo <= sl if side == "BUY" else hi >= sl)
            if tp_hit and sl_hit:       # same bar — can't order — conservative SL
                hit, hit_ts = "sl", t; break
            if sl_hit:
                hit, hit_ts = "sl", t; break
            if tp_hit:
                hit, hit_ts = "tp", t; break

        if hit == "tp":
            out.update(exit_reason="tp", exit_price=tp,
                       realized_r=round(rr, 2) if rr else None,
                       close_timestamp=hit_ts or out["close_timestamp"])
        elif hit == "sl":
            out.update(exit_reason="sl", exit_price=sl, realized_r=-1.0,
        close_timestamp=hit_ts or out["close_timestamp"])
        # neither -> manual/unknown, realized_r stays None (honest; backfill later)

        # Bound excursion to the trade's own lifetime. Bars are chronological, so
        # slice out anything at/after the exit before measuring MFE/MAE. Without
        # this, _excursion_r reads bars AFTER the trade closed and reports
        # favorable/adverse moves the trade never actually experienced —
        # inflating mfe_r AND mae_r (this is why SL trades showed mae_r < -1).
        _end_ts = _norm_ms(hit_ts or int(out.get("close_timestamp") or 0))
        if _end_ts:
            _win_bars = [
                b for b in bars_h1
                if _norm_ms(b.get("t_close_ms") or b.get("t_open_ms") or b.get("t") or 0) <= _end_ts
            ]
        else:
            _win_bars = bars_h1  # no exit timestamp resolvable -> fall back (rare)
        out.update(_excursion_r(snap, _win_bars, entry, sl))
        return out
    except Exception as e:
        log.error("analytics: approximate_exit failed: %s", e)
        return out



def _broker_offset_minutes_for_trade(
    snap: dict,
    deal: dict,
    R,
) -> tuple[int, str]:
    """
    Resolve the broker UTC offset used by MT5 wall-clock timestamps.

    Returns:
        (offset_minutes, source)

    Example:
        FTMO UTC+03:00 -> (180, "redis_account_snapshot")
    """
    candidates = []

    # Direct analytics/deal fields.
    for obj_name, obj in (
        ("snap", snap),
        ("deal", deal),
    ):
        if not isinstance(obj, dict):
            continue

        for key in (
            "broker_tz_offset_minutes",
            "broker_timezone_offset_minutes",
            "broker_offset_minutes",
            "tz_offset_minutes",
        ):
            if obj.get(key) not in (None, ""):
                candidates.append(
                    (
                        obj.get(key),
                        f"{obj_name}.{key}",
                    )
                )

    # Nested account snapshots captured by analytics.
    for obj_name in (
        "account_before",
        "account_after",
        "ftmo_before",
        "ftmo_after",
    ):
        obj = snap.get(obj_name)

        if not isinstance(obj, dict):
            continue

        for key in (
            "broker_tz_offset_minutes",
            "broker_timezone_offset_minutes",
            "broker_offset_minutes",
            "tz_offset_minutes",
        ):
            if obj.get(key) not in (None, ""):
                candidates.append(
                    (
                        obj.get(key),
                        f"{obj_name}.{key}",
                    )
                )

    # Current Redis MT5 account snapshot for the deal's device.
    try:
        device_id = str(
            deal.get("device_id")
            or snap.get("device_id")
            or ""
        ).strip()

        account_type = str(
            deal.get("mt5_account")
            or snap.get("account_type")
            or "demo"
        ).strip().lower()

        if device_id:
            account_key = (
                f"xtl:mt5:account:{device_id}:{account_type}"
            )

            raw = R.get(account_key)

            if isinstance(raw, (bytes, bytearray)):
                raw = raw.decode("utf-8", "ignore")

            account = (
                json.loads(raw)
                if isinstance(raw, str) and raw
                else {}
            )

            if isinstance(account, dict):
                for key in (
                    "broker_tz_offset_minutes",
                    "broker_timezone_offset_minutes",
                    "broker_offset_minutes",
                    "tz_offset_minutes",
                ):
                    if account.get(key) not in (None, ""):
                        candidates.append(
                            (
                                account.get(key),
                                f"redis_account_snapshot.{key}",
                            )
                        )
    except Exception as exc:
        log.warning(
            "analytics: broker offset account lookup failed "
            "ticket=%s err=%r",
            snap.get("mt5_ticket"),
            exc,
        )

    for value, source in candidates:
        try:
            offset = int(float(value))

            # Reject obviously corrupt offsets.
            if -14 * 60 <= offset <= 14 * 60:
                return offset, source
        except Exception:
            continue

    return 0, "missing"


def _normalize_broker_wall_ms(
    raw_ms: int,
    offset_minutes: int,
) -> int:
    """
    Convert MT5 broker-wall epoch milliseconds into UTC epoch
    milliseconds.

    FTMO example:
        raw broker time encoded as UTC = 06:59 UTC
        broker offset                = +180 min
        normalized actual UTC        = 03:59 UTC
    """
    raw_ms = _safe_int(raw_ms)

    if raw_ms <= 0:
        return 0

    offset_minutes = _safe_int(offset_minutes)

    if offset_minutes == 0:
        return raw_ms

    return raw_ms - (offset_minutes * 60_000)

def _exit_from_broker_deal(ticket: str, snap: dict, R) -> dict | None:
    """Resolve exit from the real MT5 deal (broker truth) instead of H1 replay.
    Returns None if no deal exists -> caller falls back to approximate_exit."""
    try:
        raw = R.get(f"xtl:mt5:deal:{ticket}")
        if not raw:
            return None
        deal = json.loads(raw)
        close_price = _safe_float(
            deal.get("close_price")
            or deal.get("price")
        )

        if close_price is None or close_price <= 0:
            log.warning(
                "analytics: broker deal missing valid close price ticket=%s",
                ticket,
            )
            return None

        raw_close_ms = _safe_int(
            deal.get("close_time_ms")
            or deal.get("close_timestamp")
            or 0
        )

        raw_open_ms = _safe_int(
            deal.get("open_time_ms")
            or 0
        )

        if raw_close_ms <= 0:
            log.warning(
                "analytics: broker deal missing valid close time "
                "ticket=%s",
                ticket,
            )
            return None

        broker_offset_min, broker_offset_source = (
            _broker_offset_minutes_for_trade(
                snap,
                deal,
                R,
            )
        )

        open_ms = _normalize_broker_wall_ms(
            raw_open_ms,
            broker_offset_min,
        )

        close_ms = _normalize_broker_wall_ms(
            raw_close_ms,
            broker_offset_min,
        )
        log.warning(
            "analytics: BROKER_TIME_NORMALIZED "
            "ticket=%s offset_min=%s source=%s "
            "raw_open=%s utc_open=%s "
            "raw_close=%s utc_close=%s",
            ticket,
            broker_offset_min,
            broker_offset_source,
            raw_open_ms,
            open_ms,
            raw_close_ms,
            close_ms,
        )


        if close_ms <= 0:
            log.warning(
                "analytics: broker deal missing valid close time ticket=%s",
                ticket,
            )
            return None
        net_profit = deal.get("net_profit")
        net_profit = (
            float(net_profit)
            if net_profit is not None
            else None
        )

        entry = float(snap.get("entry_price") or 0) or None
        sl    = float(snap.get("sl_price") or 0) or None
        tp    = float(snap.get("tp_price") or 0) or None
        side  = (snap.get("side") or "").upper()

        # realized_r from the REAL close price (never hardcoded)
        realized_r = None
        if entry and sl:
            risk = abs(entry - sl)
            if risk > 0:
                realized_r = ((close_price - entry) if side == "BUY"
                              else (entry - close_price)) / risk

        # classify: net_profit sign is the source of truth for win/loss;
        # proximity of close to TP/SL labels the reason.
        tol = (abs(entry - sl) * 0.10) if (entry and sl) else 0.0
        near_tp = tp is not None and abs(close_price - tp) <= tol
        near_sl = sl is not None and abs(close_price - sl) <= tol
        if near_tp:
            exit_reason = "tp"
        elif near_sl:
            exit_reason = "sl"
        elif net_profit is not None and net_profit > 0:
            exit_reason = "tp"      # profitable close not at a level = manual win, count as tp-side
        elif net_profit is not None and net_profit < 0:
            exit_reason = "manual"  # losing close not at SL
        else:
            exit_reason = "manual"

   

        # ── capture manual TP/SL modifications (original snapshot vs broker-final) ──
        _prev = deal.get("prev_position") or {}
        _final_tp = _prev.get("tp")
        _final_sl = _prev.get("sl")
        _final_tp = float(_final_tp) if _final_tp not in (None, 0) else None
        _final_sl = float(_final_sl) if _final_sl not in (None, 0) else None
        tp_modified = bool(_final_tp and tp and abs(_final_tp - tp) > 1e-9)
        sl_modified = bool(_final_sl and sl and abs(_final_sl - sl) > 1e-9)
        # R-impact of a TP move: what the original TP would have been in R vs actual
        tp_original_r = None
        if entry and sl and tp:
            _risk = abs(entry - sl)
            if _risk > 0:
                tp_original_r = ((tp - entry) if side == "BUY" else (entry - tp)) / _risk

        return {
            "exit_source": "broker_deal",
            "exit_confidence": "high",
            "exit_price": round(close_price, 5),
            "exit_reason": exit_reason,
            "realized_r": round(realized_r, 3) if realized_r is not None else None,
            "net_profit": net_profit,
            
            "tp_modified": tp_modified,
            "sl_modified": sl_modified,
            "tp_original": round(tp, 5) if tp else None,
            "tp_final": round(_final_tp, 5) if _final_tp else None,
            "sl_original": round(sl, 5) if sl else None,
            "sl_final": round(_final_sl, 5) if _final_sl else None,
            "tp_original_r": round(tp_original_r, 3) if tp_original_r is not None else None,
            # Existing broker-domain timestamps retained for compatibility
            # with OHLC bars, excursion analysis, and historical rows.
            "broker_open_time_ms": raw_open_ms,
            "broker_close_time_ms": raw_close_ms,
            "close_timestamp": raw_close_ms,

            # Explicit normalized UTC timestamps for news/session analysis.
            "broker_open_time_utc_ms": open_ms,
            "broker_close_time_utc_ms": close_ms,

            # Raw aliases retained for forensic clarity.
            "broker_open_time_raw_ms": raw_open_ms,
            "broker_close_time_raw_ms": raw_close_ms,

            "broker_tz_offset_minutes": broker_offset_min,
            "broker_timestamp_normalized": bool(
                broker_offset_min != 0
            ),
            "broker_timestamp_normalization_source": (
                broker_offset_source
            ),
        }
    except Exception as e:
        log.error("analytics: _exit_from_broker_deal failed for %s: %s",
                  snap.get("mt5_ticket"), e)
        return None

def _norm_ms(v) -> int:
    """Bars store 't' in seconds (10-digit); trade timestamps are ms (13-digit).
    Normalize any bar/trade time to milliseconds before comparing."""
    try:
        v = int(v or 0)
    except (TypeError, ValueError):
        return 0
    return v * 1000 if 0 < v < 10_000_000_000 else v

def _excursion_r(snap, bars_h1, entry, sl) -> dict:
    try:
        side = (snap.get("side") or "").upper()
        risk = abs(entry - sl)
        if risk <= 0:
            return {}
        entry_ts = _norm_ms(snap.get("enqueue_timestamp") or 0)
        pipf = _pip(snap.get("symbol"))
        worst = best = 0.0
        best_price = worst_price = None
        best_ts = worst_ts = None
        best_idx = worst_idx = None
        idx = -1
        for b in bars_h1:
            t = _norm_ms(b.get("t_close_ms") or b.get("t_open_ms") or b.get("t") or 0)
            if entry_ts and t < entry_ts:
                continue
            hi = _safe_float(b.get("h")); lo = _safe_float(b.get("l"))
            if hi is None or lo is None:
                continue
            idx += 1
            if side == "BUY":
                fav = (hi - entry) / risk
                adv = (lo - entry) / risk
                if fav > best:
                    best, best_price, best_ts, best_idx = fav, hi, t, idx
                if adv < worst:
                    worst, worst_price, worst_ts, worst_idx = adv, lo, t, idx
            else:
                fav = (entry - lo) / risk
                adv = (entry - hi) / risk
                if fav > best:
                    best, best_price, best_ts, best_idx = fav, lo, t, idx
                if adv < worst:
                    worst, worst_price, worst_ts, worst_idx = adv, hi, t, idx
        out = {"mfe_r": round(best, 2), "mae_r": round(worst, 2)}
        if best_price is not None:
            out["mfe_price"] = round(best_price, 5)
            out["mfe_pips"] = round(abs(best_price - entry) / pipf, 1) if pipf else None
            out["mfe_bars_after_entry"] = best_idx
            out["mfe_bar_ts_ms"] = best_ts
        if worst_price is not None:
            out["mae_price"] = round(worst_price, 5)
            out["mae_pips"] = round(abs(worst_price - entry) / pipf, 1) if pipf else None
            out["mae_bars_after_entry"] = worst_idx
            out["mae_bar_ts_ms"] = worst_ts
        return out
    except Exception:
        return {}

# ── FINALIZE + APPEND ────────────────────────────────────────────────────────
def _append_jsonl(record: dict) -> bool:
    try:
        with _trades_jsonl_lock():
            with open(JSONL_PATH, "a", encoding="utf-8") as f:   
                f.write(json.dumps(record, default=str) + "\n")   
                f.flush()
                os.fsync(f.fileno())
        return True
    except Exception as exc:
        log.error("analytics: JSONL append failed: %s", exc)      
        return False


def _holding_minutes(snap) -> int:
    try:
        open_ms = int(
            snap.get("broker_open_time_ms")
            or snap.get("opened_at_ms")
            or 0
        )

        close_ms = int(
            snap.get("broker_close_time_ms")
            or snap.get("close_timestamp")
            or 0
        )

        if open_ms > 0 and close_ms > open_ms:
            return int(round((close_ms - open_ms) / 60000.0))

        return None

    except Exception:
        return None


def _apply_news_during_trade(snap: dict) -> None:
    
    """
    Determine whether a relevant high-impact event occurred while
    the position was open.

    Calendar time_ms is UTC. Prefer explicitly normalized broker UTC
    timestamps because this MT5 feed encodes broker-local wall time
    into its raw deal timestamp fields.
    """
    
    snap["news_during_trade"] = False
    snap["news_event_during"] = None
    snap["news_event_during_ms"] = None
    snap["news_currency_during"] = None
    snap["news_impact_during"] = None
    snap["news_event_mins_after_entry"] = None
    

    try:
        entry_ms = int(
            snap.get("broker_open_time_utc_ms")
            or snap.get("enqueue_timestamp")
            or snap.get("ts_ms")
            or 0
        )

        close_ms = int(
            snap.get("broker_close_time_utc_ms")
            or snap.get("close_timestamp")
            or 0
        )

        if entry_ms <= 0 or close_ms <= entry_ms:
            return

        frozen = snap.get("upcoming_events") or []

        # Entry-time event freezing may have failed or the calendar may
        # have been temporarily unavailable. Rebuild from the current
        # calendar using the actual broker trade duration.
        if not frozen:
            duration_h = max(
                48,
                int((close_ms - entry_ms) / 3_600_000) + 2,
            )

            frozen = _freeze_currency_events(
                str(snap.get("symbol") or ""),
                entry_ms,
                horizon_h=duration_h,
            )

            snap["upcoming_events"] = frozen

        hit = None

        for event in frozen:
            if not isinstance(event, dict):
                continue

            event_ms = int(
                event.get("time_ms") or 0
            )

            if (
                event_ms > 0
                and entry_ms <= event_ms <= close_ms
            ):
                hit = event
                break

        if not hit:
            return

        event_ms = int(
            hit.get("time_ms") or 0
        )

        snap["news_during_trade"] = True
        snap["news_event_during"] = hit.get("event")
        snap["news_event_during_ms"] = event_ms
        snap["news_currency_during"] = hit.get("currency")
        snap["news_impact_during"] = hit.get("impact")
        snap["news_event_mins_after_entry"] = round(
            (event_ms - entry_ms) / 60000.0,
            1,
        )

    except Exception as exc:
        log.warning(
            "analytics: during-trade news check failed "
            "ticket=%s err=%r",
            snap.get("mt5_ticket"),
            exc,
        )
def finalize_ticket(ticket: str, bars_h1: list) -> bool:
    """Read snapshot, approximate exit, append JSONL, delete snapshot.
    Snapshot is deleted ONLY after the durable JSONL write succeeds."""
    try:
        ticket = str(ticket)
        R = from_app_R()
        raw = R.get(SNAP_PREFIX + ticket)
        if not raw:
            return False
        snap = json.loads(raw)
        if snap.get("_status") == "closed":
            return False
        broker_exit = _exit_from_broker_deal(
            ticket,
            snap,
            R,
        )

        if broker_exit:
            snap.update(broker_exit)

            # Broker truth was available on the first finalization pass.
            snap["broker_verified"] = True
            snap["broker_truth_upgraded"] = False
            snap["pending_broker_truth"] = False
            snap["exit_deal_timeout"] = False

        else:
            # ── Deal not present yet — it may still be arriving (the agent's deal
            #    push races this sweep). Defer finalize and retry on later sweeps;
            #    only approximate after a real timeout so nothing hangs forever. ──
            _now = _now_ms()
            _first = snap.get("_first_closed_seen_ms")
            if not _first:
                snap["_first_closed_seen_ms"] = _now
                R.set(SNAP_PREFIX + ticket, json.dumps(snap, default=str))
                return False   # keep snapshot OPEN, retry next sweep
            elif (_now - int(_first)) < DEAL_WAIT_MS:
                R.set(SNAP_PREFIX + ticket, json.dumps(snap, default=str))
                return False   # still within grace window, retry next sweep
            else:
                snap.update(
                    approximate_exit(
                        snap,
                        bars_h1,
                    )
                )

                snap["exit_deal_timeout"] = True
                snap["pending_broker_truth"] = True
                snap["broker_verified"] = False
                snap["broker_truth_upgraded"] = False

                try:
                    R.sadd(
                        PENDING_TRUTH_KEY,
                        ticket,
                    )
                except Exception:
                    pass

        # ── ALWAYS compute excursion (mfe/mae), independent of exit source ──
        # The broker-deal path resolves the exit but skips approximate_exit,
        # which is where excursion used to run. Compute it here for every trade,
        # time-bounded to the real close so post-exit bars can't inflate it.
        try:
            _entry = _safe_float(snap.get("entry_price"))
            _sl    = _safe_float(snap.get("sl_price"))
            _end   = _norm_ms(snap.get("close_timestamp") or 0)
            if _entry and _sl and bars_h1:
                if _end:
                    _win = [b for b in bars_h1
                            if _norm_ms(b.get("t_close_ms") or b.get("t_open_ms") or b.get("t") or 0) <= _end]
                else:
                    _win = bars_h1
                _exc = _excursion_r(snap, _win, _entry, _sl)
                if _exc:
                    snap.update(_exc)
        except Exception as _e:
            log.warning("analytics: excursion compute failed: %s", _e)
        # Analyze whether price reached the entry-frozen opposing
        # liquidity target during this trade.
        try:
            liq_analysis = analyze_liquidity_target_during_trade(
                snap,
                bars_h1,
            )
            snap.update(liq_analysis)

        except Exception as exc:
            log.warning(
                "analytics: liquidity target finalize failed "
                "ticket=%s err=%r",
                ticket,
                exc,
            )

        snap["_status"] = "closed"
       

        # ── §18: account-after / FTMO-after (no agent change — re-read at close) ──
        try:
            snap["ftmo_after"]    = read_ftmo_state_at_ack(snap.get("profile_id"))
            snap["account_after"] = read_account_at_ack(snap.get("device_id"),
                                                        str(snap.get("account_type") or "demo"))
        except Exception as _e:
            log.warning("analytics: close-side enrichment failed: %s", _e)

        
        # Recalculate news overlap using server/UTC-domain timestamps.
        _apply_news_during_trade(snap)
        snap["holding_minutes"] = _holding_minutes(snap)

        try:
            broker_open_ms = int(
                snap.get("broker_open_time_ms")
                or 0
            )

            broker_close_ms = int(
                snap.get("broker_close_time_ms")
                or 0
            )

            if (
                broker_open_ms > 0
                and broker_close_ms > broker_open_ms
            ):
                snap["broker_holding_minutes"] = int(
                    round(
                        (
                            broker_close_ms
                            - broker_open_ms
                        )
                        / 60000.0
                    )
                )
            else:
                snap["broker_holding_minutes"] = None

        except Exception:
            snap["broker_holding_minutes"] = None
        # ── §18: trade classification from data on hand ──
        try:
            rr = snap.get("realized_r")
            reason = (snap.get("exit_reason") or "").lower()
            if rr is None:
                outcome = "UNKNOWN"
            elif rr > 0.05:
                outcome = "WIN"
            elif rr < -0.05:
                outcome = "LOSS"
            else:
                outcome = "BREAK_EVEN"
            # exit_type maps the reason -> doc's classification set
            exit_type = {"tp": "TP", "sl": "SL", "manual": "MANUAL"}.get(reason, "OTHER")
            snap["outcome"]   = outcome
            snap["exit_type"] = exit_type
            # bars_held (H1 bars between entry and close)
            try:
                hm = snap.get("holding_minutes")
                snap["bars_held"] = int(hm // 60) if isinstance(hm, (int, float)) else None
            except Exception:
                snap["bars_held"] = None
            # efficiency: realized_r / mfe_r  (how much of the favourable move was captured)
            try:
                mfe = snap.get("mfe_r")
                # Only meaningful for winners with a non-trivial favourable excursion.
                # -1.01R / 0.05R = -20.22 is arithmetically true and analytically poison.
                snap["efficiency"] = (round(rr / mfe, 2)
                                      if (rr is not None and rr > 0 and mfe and mfe >= 0.1)
                                      else None)
            except Exception:
                snap["efficiency"] = None
            # heat: how close to the stop it ran (|mae_r|, 1.0 = touched stop)
            try:
                mae = snap.get("mae_r")
                snap["heat"] = round(abs(mae), 2) if mae is not None else None
            except Exception:
                snap["heat"] = None
        except Exception as _e:
            log.warning("analytics: classification failed: %s", _e)

        try:
            cs = (
                snap.get("capture_status")
                if isinstance(
                    snap.get("capture_status"),
                    dict,
                )
                else {}
            )

            cs["exit_snapshot_complete"] = True
            cs["broker_verified"] = bool(
                snap.get("broker_verified")
                and snap.get("exit_source")
                == "broker_deal"
                and snap.get("broker_open_time_ms")
                and snap.get("broker_close_time_ms")
                and snap.get("exit_price") is not None
                and snap.get("net_profit") is not None
            )

            snap["broker_verified"] = bool(
                cs["broker_verified"]
            )

            cs["analytics_schema_version"] = (
                SCHEMA_VERSION
            )

            snap["capture_status"] = cs

        except Exception as exc:
            log.warning(
                "analytics: capture_status finalize "
                "failed ticket=%s err=%s",
                ticket,
                exc,
            )
        
        if _append_jsonl(snap):
            R.delete(SNAP_PREFIX + ticket)
            return True
        return False
    except Exception as e:
        log.error("analytics: finalize_ticket %s failed: %s", ticket, e)
        return False


def reconcile_pending_broker_truth(fetch_h1_bars=None) -> dict:
    """Upgrade any approximated row whose broker deal has since arrived.

    Invariant: an h1_bar_approx / exit_deal_timeout row must NOT stay final once
    its broker deal exists. Runs every sweep; touches only tickets in the pending
    set (no full-file scan). Prefers repeating a repair over losing one.

    Correctness properties (from review):
      - shared lock with _append_jsonl (no lost concurrent appends)
      - atomic os.replace (no half-written file)
      - SREM only AFTER a successful replace (repair never silently lost)
      - file mode/owner preserved; temp file always cleaned up
      - resolver OWNS exit_source; we verify, never force it
      - upgrade built on a COPY; original row untouched on any reject path
      - missing rows kept pending and logged (surfaces upstream bugs)
      - H1 excursion labeled very_low/low, and only when actually computed
      - structured self-contained failure (never propagates to the sweep)
    """
    out = {"checked": 0, "upgraded": 0, "orphans": 0, "errors": 0}
    try:
        R = from_app_R()                                          
        try:
            pend = R.smembers(PENDING_TRUTH_KEY) or set()
        except Exception:
            return out
        pend = {t.decode() if isinstance(t, (bytes, bytearray)) else str(t) for t in pend}
        if not pend:
            return out

        # which pending tickets now actually have a broker deal?
        try:
            ready = {t for t in pend if R.get(f"xtl:mt5:deal:{t}")}
        except Exception:
            return out
        if not ready:
            return out

        fetch_h1_bars = fetch_h1_bars or default_fetch_h1_bars  
        upgraded_tickets = set()
        matched_tickets  = set()

        with _trades_jsonl_lock():
            original_stat = os.stat(JSONL_PATH)                   
            tmp = None
            try:
                rows = []
                with open(JSONL_PATH, encoding="utf-8") as f:     
                    for line in f:
                        line = line.rstrip("\n")
                        if not line.strip():
                            continue
                        try:
                            r = json.loads(line)                  
                        except Exception:
                            rows.append(line)     # preserve unparseable line verbatim
                            continue

                        tk = str(r.get("mt5_ticket") or "")
                        if tk in ready:
                            matched_tickets.add(tk)
                            out["checked"] += 1

                            be = _exit_from_broker_deal(
                                tk,
                                r,
                                R,
                            )

                            if isinstance(be, dict):
                                # Build on a COPY — never mutate the real row
                                # until the broker result is validated.
                                upgraded_row = dict(r)
                                upgraded_row.update(be)

                                if (
                                    str(
                                        upgraded_row.get("exit_source")
                                        or ""
                                    ).lower()
                                    != "broker_deal"
                                ):
                                    log.error(
                                        "reconcile: resolver returned unexpected "
                                        "source ticket=%s source=%r",
                                        tk,
                                        upgraded_row.get("exit_source"),
                                    )

                                    # Preserve the original approximate row.
                                    # Ticket remains pending for a later retry.
                                    rows.append(r)
                                    continue

                                # ---------------------------------------------
                                # Broker truth upgrade is now validated.
                                # ---------------------------------------------
                                upgraded_row["broker_verified"] = True
                                upgraded_row["broker_truth_upgraded"] = True
                                upgraded_row["pending_broker_truth"] = False
                                upgraded_row["exit_deal_timeout"] = False

                                upgraded_row["holding_minutes"] = (
                                    _holding_minutes(upgraded_row)
                                )

                                try:
                                    broker_open_ms = int(
                                        upgraded_row.get(
                                            "broker_open_time_ms"
                                        )
                                        or 0
                                    )

                                    broker_close_ms = int(
                                        upgraded_row.get(
                                            "broker_close_time_ms"
                                        )
                                        or 0
                                    )

                                    if (
                                        broker_open_ms > 0
                                        and broker_close_ms
                                        > broker_open_ms
                                    ):
                                        upgraded_row[
                                            "broker_holding_minutes"
                                        ] = int(
                                            round(
                                                (
                                                    broker_close_ms
                                                    - broker_open_ms
                                                )
                                                / 60000.0
                                            )
                                        )
                                    else:
                                        upgraded_row[
                                            "broker_holding_minutes"
                                        ] = None

                                except Exception:
                                    upgraded_row[
                                        "broker_holding_minutes"
                                    ] = None

                                capture_status = (
                                    upgraded_row.get("capture_status")
                                    if isinstance(
                                        upgraded_row.get(
                                            "capture_status"
                                        ),
                                        dict,
                                    )
                                    else {}
                                )

                                capture_status[
                                    "exit_snapshot_complete"
                                ] = True

                                capture_status[
                                    "broker_verified"
                                ] = bool(
                                    upgraded_row.get("exit_source")
                                    == "broker_deal"
                                    and upgraded_row.get(
                                        "broker_open_time_ms"
                                    )
                                    and upgraded_row.get(
                                        "broker_close_time_ms"
                                    )
                                    and upgraded_row.get(
                                        "exit_price"
                                    ) is not None
                                    and upgraded_row.get(
                                        "net_profit"
                                    ) is not None
                                )

                                capture_status[
                                    "analytics_schema_version"
                                ] = SCHEMA_VERSION

                                upgraded_row[
                                    "broker_verified"
                                ] = bool(
                                    capture_status[
                                        "broker_verified"
                                    ]
                                )

                                upgraded_row[
                                    "capture_status"
                                ] = capture_status
                                _apply_news_during_trade(
                                    upgraded_row
                                )
                                rows.append(upgraded_row)
                                upgraded_tickets.add(tk)
                                out["upgraded"] += 1

                                log.warning(
                                    "analytics: broker truth upgraded "
                                    "ticket=%s exit_reason=%s "
                                    "net_profit=%s holding_minutes=%s",
                                    tk,
                                    upgraded_row.get("exit_reason"),
                                    upgraded_row.get("net_profit"),
                                    upgraded_row.get(
                                        "broker_holding_minutes"
                                    ),
                                )

                            else:
                                # Deal key exists, but resolver could not yet
                                # produce valid broker truth. Preserve original
                                # row and keep the ticket pending.
                                rows.append(r)

                                log.warning(
                                    "reconcile: broker deal unresolved "
                                    "ticket=%s",
                                    tk,
                                )
                                

                                # excursion redo — approximate; label honestly and
                                # ONLY when actually computed
                                try:
                                    _entry = _safe_float(r.get("entry_price"))   
                                    _sl    = _safe_float(r.get("sl_price"))      
                                    _end   = int(r.get("close_timestamp")
                                                 or r.get("close_time_ms")
                                                 or _now_ms())                   
                                    try:
                                        bars = fetch_h1_bars(
                                            r.get("symbol"),
                                            int(r.get("enqueue_timestamp") or 0),
                                            _end, r.get("device_id")) or []
                                    except TypeError:
                                        bars = fetch_h1_bars(
                                            r.get("symbol"),
                                            int(r.get("enqueue_timestamp") or 0),
                                            _end) or []
                                    if _entry and _sl and bars:
                                        _exc = _excursion_r(r, bars, _entry, _sl)  
                                        if _exc:
                                            r.update(_exc)
                                            _hm = float(r.get("holding_minutes") or 0)
                                            r["excursion_source"]    = "h1_bar_approx"
                                            r["excursion_precision"] = "very_low" if _hm < 60 else "low"
                                except Exception as _ee:
                                    log.warning(                   
                                        "reconcile: excursion redo failed ticket=%s err=%s",
                                        tk, _ee)

                                upgraded_tickets.add(tk)
                        rows.append(r)

                # write to temp, preserve mode/owner, atomic replace
                fd, tmp = tempfile.mkstemp(
                    dir=os.path.dirname(JSONL_PATH),               
                    prefix=".trades_", suffix=".tmp")
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    for r in rows:
                        f.write((r if isinstance(r, str)
                                 else json.dumps(r, default=str, separators=(",", ":"))) + "\n")
                    f.flush()
                    os.fsync(f.fileno())

                os.chmod(tmp, stat.S_IMODE(original_stat.st_mode))
                try:
                    os.chown(tmp, original_stat.st_uid, original_stat.st_gid)
                except PermissionError:
                    pass

                os.replace(tmp, JSONL_PATH)                        
                tmp = None
            finally:
                if tmp and os.path.exists(tmp):
                    try:
                        os.unlink(tmp)
                    except Exception:
                        pass

        # ── file is durably written; NOW clear pending (only the upgraded ones) ──
        if upgraded_tickets:
            try:
                R.srem(PENDING_TRUTH_KEY, *sorted(upgraded_tickets))
            except Exception as exc:
                log.warning(                                       
                    "reconcile: JSONL upgraded but pending cleanup failed "
                    "tickets=%s err=%s", sorted(upgraded_tickets), exc)

        # deal exists but no JSONL row — a real upstream bug. Surface, keep pending.
        orphans = ready - matched_tickets
        for tk in sorted(orphans):
            log.error(                                             
                "reconcile: pending broker truth has no JSONL row ticket=%s", tk)

        out["orphans"]  = len(orphans)
        out["upgraded"] = len(upgraded_tickets)
        return out

    except Exception as exc:
        out["errors"] += 1
        log.exception(                                             
            "analytics: reconcile_pending_broker_truth failed: %s", exc)
        return out


# ── CLOSE DETECTION + ORPHAN SWEEP ───────────────────────────────────────────
def sweep_closed_trades(fetch_h1_bars=None) -> dict:
    """Diff in-flight snapshots against the live open-position set; finalize any
    whose ticket is gone (and old enough). Call once per BROKER_RECON cycle."""
    fetch_h1_bars = fetch_h1_bars or default_fetch_h1_bars
    stats = {"checked": 0, "finalized": 0, "errors": 0}
    try:
        R = from_app_R()
        open_tickets = _open_tickets()
        now = _now_ms()
        for key in R.scan_iter(SNAP_PREFIX + "*"):
            stats["checked"] += 1
            try:
                raw = R.get(key)
                if not raw:
                    continue
                snap = json.loads(raw)
                ticket = str(snap.get("mt5_ticket") or str(key).split(":")[-1])
                if ticket in open_tickets:
                    continue
                age = now - int(snap.get("enqueue_timestamp") or now)
                if age < ORPHAN_AGE_MS:
                    continue                       # too fresh; avoid just-placed race
                bars = []
                try:
                    bars = fetch_h1_bars(snap.get("symbol"),
                                         int(snap.get("enqueue_timestamp") or 0),
                                         now, snap.get("device_id")) or []
                except TypeError:
                    bars = fetch_h1_bars(snap.get("symbol"),
                                         int(snap.get("enqueue_timestamp") or 0), now) or []
                except Exception as e:
                    log.warning("analytics: bar fetch failed for %s: %s", snap.get("symbol"), e)
                if finalize_ticket(ticket, bars):
                    stats["finalized"] += 1
            except Exception as e:
                stats["errors"] += 1
                log.error("analytics: sweep item failed: %s", e)
    except Exception as e:
        log.error("analytics: sweep_closed_trades failed: %s", e)

    try:
        rec = reconcile_pending_broker_truth(
            fetch_h1_bars
        )

        if rec.get("upgraded"):
            log.info(
                "analytics: broker-truth upgraded %d row(s)",
                rec["upgraded"],
            )

        stats["truth_upgraded"] = rec.get(
            "upgraded",
            0,
        )

        stats["truth_orphans"] = rec.get(
            "orphans",
            0,
        )

    except Exception as e:
        log.error(
            "analytics: reconcile pass failed: %s",
            e,
        )
    return stats


REJECTED_JSONL = "/opt/xauapi/api/trend/out/trades_rejected.jsonl"


def capture_rejection(candidate: dict, reason: str, gate: str = None, enrich: bool = False) -> bool:
    """Log a rejected trade candidate. Light by default (cheap context only);
    enrich=True recomputes regime/SR for meaningful gate-blocks. Never raises."""
    try:
        c = candidate or {}
        sym  = str(c.get("symbol") or "").upper()
        side = str(c.get("side") or "").upper()
        ts   = int(c.get("opened_at_ms") or c.get("ts_ms") or _now_ms())
        z = c.get("entry_zone") if isinstance(c.get("entry_zone"), dict) else {}
        rec = {
            "schema_version":  SCHEMA_VERSION,
            "timestamp":       ts,
            "session":         _session_for_ts_ms(ts, LIVE_TZ_OFFSET_H),
            "symbol":          sym,
            "side":            side,
            "rejection_reason": reason,
            "gate":            gate,
            "trade_id":        c.get("trade_id"),
            "profile_id":      c.get("profile_id"),
            "trigger_type":    c.get("trigger_type"),
            "zone_level":      _safe_float(z.get("level") if z else c.get("entry_zone_level")),
            "entry_price":     _safe_float(c.get("entry_price")),
        }
        if enrich:
            try:
                rec["regime_1h"] = (read_regime_at_ack(sym, c.get("device_id")) or {}).get("h1")
                sr = read_sr_at_ack(sym)
                rec["best_resistance"] = sr.get("best_resistance")
                rec["best_support"]    = sr.get("best_support")
            except Exception:
                pass
        os.makedirs(os.path.dirname(REJECTED_JSONL), exist_ok=True)
        with open(REJECTED_JSONL, "a") as f:
            f.write(json.dumps(rec, separators=(",", ":")) + "\n")
            f.flush()
        return True
    except Exception as e:
        log.warning("analytics: capture_rejection failed: %s", e)
        return False


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # build_entry_snapshot — regime degrades to None (no host), drift/session computed
    clean_pos = {
        "trade_id": "WATCH:XAUUSD:SELL:H1:1782417600000", "symbol": "XAUUSD", "side": "SELL",
        "entry_price": 4008.32, "qty": 0.06, "sl_price": 4044.15, "tp_price": 3936.96,
        "opened_at_ms": 1782447439976,  # ~04:17 UTC -> Asia
        "source": "oppt", "device_id": "dev_x", "trigger_type": "WATCHLIST_REV_OK_BAR_BREAK",
        "trigger_level": 4023.75, "profile_id": "ftmo-main",
        "entry_zone": {"level": 4041.33, "low": 4034.60, "high": 4043.65, "touches": 2, "sr_score": 5.5, "tf": "H1", "kind": "resistance"},
        "prop_check": {"verdict": "OK", "firm": "ftmo", "phase": "challenge", "risk_usd": 214.4, "risk_pct": 0.858, "target_rr": 2.0, "planned_rr": 2.0},
        "mt5_ticket": 2104259779,
    }
    s = build_entry_snapshot(clean_pos)
    print("CLEAN provenance:", s["entry_provenance"], "| session:", s["session"],
          "| against_drift:", s["against_drift"], "(SELL vs", s["drift_direction"], "drift)",
          "| dist_to_zone:", s["dist_to_zone_pips"], "| regime_1h:", s["regime_1h"])

    repair_pos = {
        "trade_id": "BROKER_REPAIR:GBPUSD:SELL:2107942429", "symbol": "GBPUSD", "side": "SELL",
        "entry_price": 1.32255, "qty": 0.06, "sl_price": 1.32540, "tp_price": 1.31600,
        "opened_at_ms": 1782447439976, "source": "broker_repair", "repair_source": "broker_snapshot",
        "device_id": "dev_y", "profile_id": "ftmo-main", "mt5_ticket": 2107942429,
        "entry_zone": {"level": 1.32503, "low": 1.32370, "high": 1.32523, "touches": 14},
        "prop_check": {"verdict": "ALLOW", "source": "broker_repair", "firm": "ftmo", "phase": "challenge", "risk_usd": 251.4, "risk_pct": 0.86, "target_rr": 2.0, "planned_rr": 2.2},
    }
    r = build_entry_snapshot(repair_pos)
    print("REPAIR provenance:", r["entry_provenance"], "| trigger_type:", r["trigger_type"],
          "| prop_verdict:", r["prop_verdict"], "| entry_style:", r["entry_style"])

    print("\napproximate_exit (SELL hits SL):")
    bars = [{"t_close_ms": 1782447500000, "h": 4010, "l": 4005},
            {"t_close_ms": 1782451100000, "h": 4046, "l": 4030}]
    print(" ", approximate_exit(s, bars))
