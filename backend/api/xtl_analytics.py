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

log = logging.getLogger("xtl.analytics")

SNAP_PREFIX    = "xtl:analytics:trade:"
SNAP_TTL_SEC   = 14 * 24 * 3600
JSONL_PATH     = "/opt/xauapi/api/trend/out/trades.jsonl"
ORPHAN_AGE_MS  = 10 * 60 * 1000
SCHEMA_VERSION = "1.3"

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


def _resolve_bar_device(symbol: str, prefer: str = None) -> str:
    """Find a device that actually stores H1 bars for `symbol`. Prefer the given
    device if it has them; else scan ohlc:snap keys and pick one that does."""
    try:
        R = from_app_R()
        sym = (symbol or "").upper()
        # 1) preferred device, if it has bars
        if prefer and R.exists(f"xtl:ohlc:snap:{prefer}:{sym}:H1"):
            return prefer
        # 2) scan for any device holding this symbol's H1 bars
        for k in R.scan_iter(f"xtl:ohlc:snap:*:{sym}:H1", count=200):
            ks = k.decode() if isinstance(k, (bytes, bytearray)) else k
            parts = ks.split(":")
            if len(parts) >= 5:
                return parts[3]   # xtl:ohlc:snap:{dev}:{sym}:H1
    except Exception as e:
        log.warning("analytics: _resolve_bar_device failed for %s: %s", symbol, e)
    return prefer or ""


def read_regime_at_ack(symbol: str, device_id: str):
    """H1+H4+D1 regime recomputed live at the entry moment. None on failure.
    Resolves the bar-storing device (which may differ from the trade's device)."""
    try:
        if not symbol:
            return None
        from api.trend_endpoints import _get_closed_h1_bars, _get_closed_h4_bars
        from api.liq_structure import detect_regime
        dev = _resolve_bar_device(symbol, device_id)
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
        dev = _resolve_bar_device(symbol, device_id)
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
            dev = _resolve_bar_device(symbol, device_id)
            if dev:
                from api.trend_endpoints import _get_closed_h1_bars
                bars = _get_closed_h1_bars(symbol, dev) or []
                bar = None
                for b in bars:
                    if int(b.get("t_open_ms") or 0) == int(rc["rc_open_ms"]):
                        bar = b; break
                if bar is None:
                    cands = [b for b in bars if int(b.get("t_open_ms") or 0) <= int(rc["rc_open_ms"])]
                    bar = max(cands, key=lambda b: int(b.get("t_open_ms") or 0)) if cands else None
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

        regime = read_regime_at_ack(sym, p.get("device_id"))
        liq    = read_liquidity_at_ack(sym, side, (z or None), p.get("device_id"), entry)
        sr     = read_sr_at_ack(sym)
        drift  = _drift_lookup(sym, session, side)
        ftmo   = read_ftmo_state_at_ack(p.get("profile_id"))
        acct   = read_account_at_ack(p.get("device_id"),
                                     str(p.get("account_type") or "demo"))
        rc     = read_reversal_candle(sym, p.get("device_id"), p)
        liqdet = read_liquidity_detail(liq, side)
        news   = read_news_at_ack(sym, ets)

        # ── derivations (no new source) ──
        import datetime as _dt
        _d = _dt.datetime.fromtimestamp(ets / 1000.0, _dt.timezone.utc)
        weekday = _d.strftime("%A"); month = _d.month
        quarter = (month - 1) // 3 + 1; year = _d.year
        pipf = _pip(sym)
        stop_distance = round(abs(entry - sl) / pipf, 1) if (entry is not None and sl is not None) else None
        tp_distance   = round(abs(tp - entry) / pipf, 1) if (entry is not None and tp is not None) else None
        atr_pct = None
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
            "capture_status": {
                "entry_snapshot_complete": True,
                "exit_snapshot_complete":  False,
                "broker_verified":         bool(p.get("mt5_ticket")),
                "analytics_schema_version": SCHEMA_VERSION,
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
        out.update(_excursion_r(snap, bars_h1, entry, sl))
        return out
    except Exception as e:
        log.error("analytics: approximate_exit failed: %s", e)
        return out


def _excursion_r(snap, bars_h1, entry, sl) -> dict:
    try:
        side = (snap.get("side") or "").upper()
        risk = abs(entry - sl)
        if risk <= 0:
            return {}
        entry_ts = int(snap.get("enqueue_timestamp") or 0)
        worst = best = 0.0
        for b in bars_h1:
            t = int(b.get("t_close_ms") or b.get("t_open_ms") or b.get("t") or 0)
            if entry_ts and t < entry_ts:
                continue
            hi = _safe_float(b.get("h")); lo = _safe_float(b.get("l"))
            if hi is None or lo is None:
                continue
            if side == "BUY":
                best  = max(best,  (hi - entry) / risk)
                worst = min(worst, (lo - entry) / risk)
            else:
                best  = max(best,  (entry - lo) / risk)
                worst = min(worst, (entry - hi) / risk)
        return {"mfe_r": round(best, 2), "mae_r": round(worst, 2)}
    except Exception:
        return {}


# ── FINALIZE + APPEND ────────────────────────────────────────────────────────
def _append_jsonl(record: dict) -> bool:
    try:
        os.makedirs(os.path.dirname(JSONL_PATH), exist_ok=True)
        with open(JSONL_PATH, "a") as f:
            f.write(json.dumps(record, separators=(",", ":")) + "\n")
            f.flush()
        return True
    except Exception as e:
        log.error("analytics: JSONL append failed (snapshot kept for retry): %s", e)
        return False


def _holding_minutes(snap) -> int:
    try:
        a = int(snap.get("enqueue_timestamp") or 0)
        b = int(snap.get("close_timestamp") or 0)
        return int((b - a) / 60000) if a and b and b > a else None
    except Exception:
        return None


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
        snap.update(approximate_exit(snap, bars_h1))
        snap["_status"] = "closed"

        # ── §18: account-after / FTMO-after (no agent change — re-read at close) ──
        try:
            snap["ftmo_after"]    = read_ftmo_state_at_ack(snap.get("profile_id"))
            snap["account_after"] = read_account_at_ack(snap.get("device_id"),
                                                        str(snap.get("account_type") or "demo"))
        except Exception as _e:
            log.warning("analytics: close-side enrichment failed: %s", _e)

        # ── news DURING the trade: did a high-impact event hit while open? ──
        try:
            ev_entry = int(snap.get("enqueue_timestamp") or 0)
            ev_close = int(snap.get("close_timestamp") or 0)
            frozen = snap.get("upcoming_events") or []
            hit = None
            for e in frozen:
                t = int(e.get("time_ms") or 0)
                if ev_entry and ev_close and ev_entry <= t <= ev_close:
                    hit = e; break
            if hit:
                snap["news_during_trade"]    = True
                snap["news_event_during"]    = hit.get("event")
                snap["news_event_during_ms"] = hit.get("time_ms")
                snap["news_currency_during"] = hit.get("currency")
                # minutes from entry to the event (how far into the trade it struck)
                snap["news_event_mins_after_entry"] = (
                    round((int(hit.get("time_ms")) - ev_entry) / 60000.0, 1)
                    if ev_entry else None)
            else:
                snap["news_during_trade"] = False
                snap["news_event_during"] = None
        except Exception as _e:
            log.warning("analytics: during-trade news check failed: %s", _e)

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
                snap["efficiency"] = round(rr / mfe, 2) if (rr is not None and mfe and mfe > 0) else None
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
            cs = snap.get("capture_status") or {}
            cs["exit_snapshot_complete"] = True
            snap["capture_status"] = cs
        except Exception:
            pass
        snap["holding_minutes"] = _holding_minutes(snap)
        if _append_jsonl(snap):
            R.delete(SNAP_PREFIX + ticket)
            return True
        return False
    except Exception as e:
        log.error("analytics: finalize_ticket %s failed: %s", ticket, e)
        return False


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
