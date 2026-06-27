"""
liq_structure.py — Liquidity Structure Detection (OHLC only)

Phase 1: Pure observation layer — no gate logic, no scoring.
Detects 6 liquidity signals near a zone and returns a display string
for the UI LIQ column.

Signals:
  1. Equal Highs / Equal Lows  (EQL/EQH)
  2. Untouched Swing H/L       (SWING_FRESH / SWING_USED)
  3. Session High / Low        (ASIA / LONDON / NEWYORK)
  4. Order Block               (OB)
  5. Fair Value Gap            (FVG)
  6. Round Number              (RN)

All functions are safe to call in production — any exception returns
empty/default so no existing opportunity fields are affected.
"""

from __future__ import annotations
import logging
from datetime import datetime, timezone, timedelta
from typing import Any

log = logging.getLogger("xtl.liq")

# ─── ATR helper ──────────────────────────────────────────────────────────────

def _atr14(bars: list[dict]) -> float:
    """Compute ATR-14 from a list of {o,h,l,c} bar dicts. Returns 0.0 on error."""
    try:
        if not bars or len(bars) < 5:
            return 0.0
        trs = []
        prev_c = None
        for b in bars:
            try:
                h = float(b["h"]); l = float(b["l"]); c = float(b["c"])
            except Exception:
                continue
            if prev_c is None:
                tr = h - l
            else:
                tr = max(h - l, abs(h - prev_c), abs(l - prev_c))
            trs.append(tr)
            prev_c = c
        if len(trs) < 5:
            return 0.0
        window = trs[-14:] if len(trs) >= 14 else trs
        return float(sum(window) / len(window))
    except Exception:
        return 0.0


# ─── pip factor ──────────────────────────────────────────────────────────────

def _pip_factor(sym: str) -> float:
    sym = (sym or "").upper()
    if sym == "XAUUSD" or sym.endswith("JPY"):
        return 0.01
    return 0.0001


# ─── bar timestamp helper ────────────────────────────────────────────────────

def _bar_open_ms(b: dict) -> int:
    """Return open timestamp in ms."""
    try:
        t = int(b.get("t_open_ms") or b.get("t_close_ms") or b.get("t") or 0)
        # if stored in seconds convert
        if 0 < t < 10_000_000_000:
            t *= 1000
        return t
    except Exception:
        return 0


# ─── REGIME (trend vs range) — always-on, OHLC only ──────────────────────────
# Two cheap, well-worn measures combined:
#   - Kaufman Efficiency Ratio (ER): net move / total path. ~0=chop, ~1=clean trend.
#   - ADX (Wilder): trend-strength. >25 trending, <20 ranging.
# Label = TREND only if BOTH agree; RANGE if both weak; else MIXED.
# For a zone-REVERSAL strategy: RANGE = zones hold (favourable to take),
# TREND = zones get run over (reversals fail). This is the read to gate on.

REG_ER_TREND  = 0.45
REG_ER_RANGE  = 0.30
REG_ADX_TREND = 25.0
REG_ADX_RANGE = 20.0
# If the daily candle on your broker/chart does NOT start at 00:00 UTC, set this
# so resampled daily buckets line up with the chart you verify against.
REG_DAY_OFFSET_H = 0.0


def _efficiency_ratio(closes: list[float], n: int) -> float:
    """Kaufman ER over the last n steps. 0..1; higher = more directional."""
    try:
        if len(closes) < 3:
            return 0.0
        w = closes[-(n + 1):] if len(closes) > n else closes
        net = abs(w[-1] - w[0])
        noise = sum(abs(w[i] - w[i - 1]) for i in range(1, len(w)))
        return float(net / noise) if noise > 0 else 0.0
    except Exception:
        return 0.0


def _adx(bars: list[dict], n: int = 14) -> float:
    """Wilder ADX from o/h/l/c bars. Returns 0.0 if insufficient history."""
    try:
        if not bars or len(bars) < 2 * n + 1:
            return 0.0
        highs, lows, closes = [], [], []
        for b in bars:
            highs.append(float(b["h"])); lows.append(float(b["l"])); closes.append(float(b["c"]))

        tr, plus_dm, minus_dm = [], [], []
        for i in range(1, len(highs)):
            up = highs[i] - highs[i - 1]
            dn = lows[i - 1] - lows[i]
            plus_dm.append(up if (up > dn and up > 0) else 0.0)
            minus_dm.append(dn if (dn > up and dn > 0) else 0.0)
            tr.append(max(highs[i] - lows[i],
                          abs(highs[i] - closes[i - 1]),
                          abs(lows[i] - closes[i - 1])))

        def _wilder(seq, period):
            sm = [sum(seq[:period])]
            for v in seq[period:]:
                sm.append(sm[-1] - sm[-1] / period + v)
            return sm

        if len(tr) < 2 * n:
            return 0.0
        atr_s = _wilder(tr, n)
        pdm_s = _wilder(plus_dm, n)
        mdm_s = _wilder(minus_dm, n)

        dx = []
        for i in range(len(atr_s)):
            a = atr_s[i]
            if a <= 0:
                dx.append(0.0); continue
            pdi = 100.0 * pdm_s[i] / a
            mdi = 100.0 * mdm_s[i] / a
            s = pdi + mdi
            dx.append(100.0 * abs(pdi - mdi) / s if s > 0 else 0.0)

        if len(dx) < n:
            return 0.0
        adx = sum(dx[:n]) / n
        for v in dx[n:]:
            adx = (adx * (n - 1) + v) / n
        return float(adx)
    except Exception:
        return 0.0


def _regime_label(er: float, adx: float) -> str:
    if er >= REG_ER_TREND and adx >= REG_ADX_TREND:
        return "TREND"
    if er < REG_ER_RANGE and adx < REG_ADX_RANGE:
        return "RANGE"
    return "MIXED"


def _resample_to_d1(bars_h4: list[dict], offset_h: float = 0.0) -> list[dict]:
    """Build daily o/h/l/c from H4 bars by calendar-day bucket (UTC +/- offset)."""
    try:
        if not bars_h4:
            return []
        off_ms = int(offset_h * 3_600_000)
        days: dict[int, dict] = {}
        order: list[int] = []
        for b in bars_h4:
            t = _bar_open_ms(b)
            if t <= 0:
                continue
            day = (t - off_ms) // 86_400_000
            o = float(b["o"]); h = float(b["h"]); l = float(b["l"]); c = float(b["c"])
            if day not in days:
                days[day] = {"o": o, "h": h, "l": l, "c": c, "t_open_ms": t}
                order.append(day)
            else:
                d = days[day]
                d["h"] = max(d["h"], h)
                d["l"] = min(d["l"], l)
                d["c"] = c
        return [days[d] for d in order]
    except Exception:
        return []


def compute_regime(bars: list[dict], er_n: int) -> dict:
    """One timeframe -> {label, er, adx}. Safe; returns dashes on failure."""
    try:
        closes = [float(b["c"]) for b in bars] if bars else []
        if len(closes) < 5:
            return {"label": "—", "er": None, "adx": None}
        er  = _efficiency_ratio(closes, er_n)
        adx = _adx(bars, 14)
        return {"label": _regime_label(er, adx), "er": round(er, 2), "adx": round(adx, 1)}
    except Exception:
        return {"label": "—", "er": None, "adx": None}


def detect_regime(bars_h1: list[dict], bars_h4: list[dict],
                  bars_d1: list[dict] | None = None) -> dict:
    """
    Always-on regime read for H1 / H4 / D1. D1 uses bars_d1 if provided, else
    resamples from H4. Returns structured per-TF dict plus a compact UI string.
    Also serves as the live regime label for shadow-mode trade capture.
    """
    h1 = compute_regime(bars_h1, er_n=24)
    h4 = compute_regime(bars_h4, er_n=20)
    d1_bars = bars_d1 if bars_d1 else _resample_to_d1(bars_h4, REG_DAY_OFFSET_H)
    d1 = compute_regime(d1_bars, er_n=14)

    def _tag(tf, r):
        return f"{tf}:—" if r["label"] == "—" else f"{tf}:{r['label']}(ER{r['er']}/ADX{r['adx']})"

    text = "REG " + " ".join([_tag("1H", h1), _tag("4H", h4), _tag("1D", d1)])
    return {"h1": h1, "h4": h4, "d1": d1, "text": text}


# ─── sweep + overlap helpers (Phase 1 confirmation layer) ─────────────────────

def _is_swept(bars: list[dict], level: float, liq_type: str, lookback: int = 100) -> bool:
    """
    Return True if `level` was swept within the last `lookback` H1 bars.

    A sweep = liquidity pool got taken, so the level is spent (no longer a
    fresh target). Detected by wick-through + close-back-on-original-side:

      BSL (buy-side, sits ABOVE price): a bar's HIGH pierced the level
          (high > level) but it CLOSED back below (close < level).
      SSL (sell-side, sits BELOW price): a bar's LOW pierced the level
          (low < level) but it CLOSED back above (close > level).
    """
    try:
        if not bars or level <= 0:
            return False
        recent = bars[-lookback:] if len(bars) > lookback else bars
        is_bsl = str(liq_type or "").upper().startswith("BSL")
        for b in recent:
            try:
                h = float(b["h"]); l = float(b["l"]); c = float(b["c"])
            except Exception:
                continue
            if is_bsl:
                if h > level and c < level:
                    return True
            else:  # SSL
                if l < level and c > level:
                    return True
        return False
    except Exception:
        return False


def _sweep_info(
    bars: list[dict],
    level: float,
    liq_type: str,
    lookback: int = 100,
) -> dict[str, Any]:
    """
    Return sweep metadata for a BSL/SSL level.

    Keeps old sweep definition:
      BSL: high > level and close < level
      SSL: low < level and close > level

    Adds:
      swept
      swept_at_index
      candles_since_sweep
      sweep_wick
      reaction_after_sweep
    """
    out = {
        "swept": False,
        "swept_at_index": None,
        "candles_since_sweep": None,
        "sweep_wick": None,
        "reaction_after_sweep": None,
    }

    try:
        if not bars or level <= 0:
            return out

        recent = bars[-lookback:] if len(bars) > lookback else bars
        is_bsl = str(liq_type or "").upper().startswith("BSL")

        found_i = None
        sweep_wick = None

        for i, b in enumerate(recent):
            try:
                h = float(b["h"])
                l = float(b["l"])
                c = float(b["c"])
            except Exception:
                continue

            if is_bsl:
                if h > level and c < level:
                    found_i = i
                    sweep_wick = h - level
            else:
                if l < level and c > level:
                    found_i = i
                    sweep_wick = level - l

        if found_i is None:
            return out

        # reaction after latest sweep
        after = recent[found_i + 1:]
        reaction = 0.0

        if after:
            if is_bsl:
                # after BSL sweep, bearish reaction = level - lowest low after sweep
                lows = []
                for b in after:
                    try:
                        lows.append(float(b["l"]))
                    except Exception:
                        pass
                if lows:
                    reaction = max(0.0, level - min(lows))
            else:
                # after SSL sweep, bullish reaction = highest high after sweep - level
                highs = []
                for b in after:
                    try:
                        highs.append(float(b["h"]))
                    except Exception:
                        pass
                if highs:
                    reaction = max(0.0, max(highs) - level)

        out.update({
            "swept": True,
            "swept_at_index": int(found_i),
            "candles_since_sweep": int(len(recent) - 1 - found_i),
            "sweep_wick": round(float(sweep_wick or 0.0), 5),
            "reaction_after_sweep": round(float(reaction or 0.0), 5),
        })

        return out

    except Exception:
        return out
def _overlaps_zone(low: float, high: float, z_low: float, z_high: float) -> bool:
    """True if band [low,high] intersects the entry zone band [z_low,z_high]."""
    try:
        lo, hi = (low, high) if low <= high else (high, low)
        zlo, zhi = (z_low, z_high) if z_low <= z_high else (z_high, z_low)
        return lo <= zhi and hi >= zlo
    except Exception:
        return False


# ─── 1. Equal Highs / Equal Lows ─────────────────────────────────────────────

def find_equal_levels(
    bars: list[dict],
    atr: float,
    direction: str,
    tolerance_atr: float = 0.15,
    min_bar_gap: int = 3,
    max_bars: int = 500,
) -> list[dict]:
    """
    Detect equal highs (BSL) or equal lows (SSL) within tolerance_atr.

    For BUY direction  → look for SSL (equal lows below price)
    For SELL direction → look for BSL (equal highs above price)

    Returns list of {level, touches, type, label}
    """
    try:
        if not bars or atr <= 0:
            return []
        bars = bars[-max_bars:] if len(bars) > max_bars else bars

        tol = atr * tolerance_atr
        d = (direction or "").upper()
        is_buy = d in ("BUY", "UP")

        price = float(bars[-1].get("c") or 0)
        results: list[dict] = []
        seen: set[float] = set()

        if is_buy:
            # SSL — equal lows below price
            levels = [(i, float(b["l"])) for i, b in enumerate(bars)
                      if b.get("l") is not None]
            level_type = "SSL_EQL"
        else:
            # BSL — equal highs above price
            levels = [(i, float(b["h"])) for i, b in enumerate(bars)
                      if b.get("h") is not None]
            level_type = "BSL_EQH"

        for i, lv in levels:
            # side filter
            if is_buy and lv >= price:
                continue
            if not is_buy and lv <= price:
                continue

            # already recorded this cluster
            if any(abs(lv - s) <= tol for s in seen):
                continue

            # find all matching levels with minimum bar gap
            matches = [
                j for j, lv2 in levels
                if j != i
                and abs(lv - lv2) <= tol
                and abs(i - j) >= min_bar_gap
            ]
            if not matches:
                continue

            cluster_lv = round(
                sum([lv] + [levels[m][1] for m in matches if m < len(levels)])
                / (1 + len(matches)),
                5,
            )
            seen.add(cluster_lv)
            touches = 1 + len(matches)
            results.append({
                "level" : cluster_lv,
                "touches": touches,
                "type"  : level_type,
                "label" : f"EQL:{cluster_lv:.5f}({touches}x)" if is_buy
                           else f"EQH:{cluster_lv:.5f}({touches}x)",
            })

        # return closest to price first
        results.sort(key=lambda x: abs(x["level"] - price))
        return results[:3]
    except Exception as e:
        log.debug("find_equal_levels error: %s", e)
        return []


# ─── 2. Untouched Swing H/L ──────────────────────────────────────────────────

def find_untouched_swings(
    bars: list[dict],
    direction: str,
    lookback: int = 3,
    revisit_tolerance: float = 0.0005,
) -> list[dict]:
    """
    Find swing highs/lows that price has not revisited since formation.

    For BUY  → untouched swing lows below price  (SSL)
    For SELL → untouched swing highs above price (BSL)

    Returns list of {level, fresh, type, label}
    """
    try:
        if not bars or len(bars) < lookback * 2 + 2:
            return []

        d = (direction or "").upper()
        is_buy = d in ("BUY", "UP")
        n = len(bars)
        price = float(bars[-1].get("c") or 0)
        results: list[dict] = []

        for i in range(lookback, n - lookback):
            b = bars[i]
            try:
                h = float(b["h"]); l = float(b["l"])
            except Exception:
                continue

            if is_buy:
                # swing low
                is_swing = all(l <= float(bars[i - k]["l"]) for k in range(1, lookback + 1)) and \
                            all(l <= float(bars[i + k]["l"]) for k in range(1, lookback + 1))
                if not is_swing:
                    continue
                if l >= price:
                    continue
                revisited = any(
                    float(bars[j]["l"]) <= l * (1 + revisit_tolerance)
                    for j in range(i + 1, n)
                )
                results.append({
                    "level" : round(l, 5),
                    "fresh" : not revisited,
                    "type"  : "SSL_SWING",
                    "label" : f"SWING_{'FRESH' if not revisited else 'USED'}:{l:.5f}",
                })
            else:
                # swing high
                is_swing = all(h >= float(bars[i - k]["h"]) for k in range(1, lookback + 1)) and \
                            all(h >= float(bars[i + k]["h"]) for k in range(1, lookback + 1))
                if not is_swing:
                    continue
                if h <= price:
                    continue
                revisited = any(
                    float(bars[j]["h"]) >= h * (1 - revisit_tolerance)
                    for j in range(i + 1, n)
                )
                results.append({
                    "level" : round(h, 5),
                    "fresh" : not revisited,
                    "type"  : "BSL_SWING",
                    "label" : f"SWING_{'FRESH' if not revisited else 'USED'}:{h:.5f}",
                })

        # fresh first, then closest to price
        results.sort(key=lambda x: (not x["fresh"], abs(x["level"] - price)))
        return results[:3]
    except Exception as e:
        log.debug("find_untouched_swings error: %s", e)
        return []


# ─── 3. Session High / Low ───────────────────────────────────────────────────

# UTC session boundaries
_SESSIONS = {
    "ASIA"   : (0,  8),
    "LONDON" : (7, 16),
    "NEWYORK": (13, 22),
}
_SWEPT_BY = {
    "ASIA"   : "LONDON",
    "LONDON" : "NEWYORK",
    "NEWYORK": "ASIA",
}


def _filter_bars_by_date(bars: list[dict], target_date) -> list[dict]:
    """Filter bars to a specific UTC date."""
    result = []
    for b in bars:
        ms = _bar_open_ms(b)
        if ms <= 0:
            continue
        dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
        if dt.date() == target_date:
            result.append(b)
    return result


def find_session_liquidity(
    bars_h1: list[dict],
    direction: str,
) -> list[dict]:
    """
    Detect session high/low levels (Asia, London, NY) from today and yesterday.

    For BUY  → session lows are SSL targets
    For SELL → session highs are BSL targets

    Returns list of {level, session, day, type, swept_by, label}
    """
    try:
        if not bars_h1:
            return []

        d = (direction or "").upper()
        is_buy = d in ("BUY", "UP")
        price = float(bars_h1[-1].get("c") or 0)

        now_utc = datetime.now(timezone.utc)
        today     = now_utc.date()
        yesterday = (now_utc - timedelta(days=1)).date()

        results: list[dict] = []

        for day_label, target_date in [("TODAY", today), ("YDAY", yesterday)]:
            day_bars = _filter_bars_by_date(bars_h1, target_date)
            if not day_bars:
                continue

            for sess_name, (start_h, end_h) in _SESSIONS.items():
                sess_bars = [
                    b for b in day_bars
                    if start_h <= datetime.fromtimestamp(
                        _bar_open_ms(b) / 1000, tz=timezone.utc
                    ).hour < end_h
                ]
                if not sess_bars:
                    continue

                sess_high = max(float(b["h"]) for b in sess_bars)
                sess_low  = min(float(b["l"]) for b in sess_bars)

                if is_buy:
                    level = sess_low
                    liq_type = "SSL"
                    if level >= price:
                        continue
                else:
                    level = sess_high
                    liq_type = "BSL"
                    if level <= price:
                        continue

                # sweep status (50-bar lookback): spent pool vs fresh target
                swept = _is_swept(bars_h1, level, liq_type, lookback=50)
                _lbl = f"{sess_name}_{liq_type}:{level:.5f}"
                if swept:
                    _lbl += "(SWEPT)"

                results.append({
                    "level"   : round(level, 5),
                    "session" : sess_name,
                    "day"     : day_label,
                    "type"    : liq_type,
                    "swept"   : swept,
                    "swept_by": _SWEPT_BY[sess_name],
                    "label"   : _lbl,
                })

        # sort by distance to price
        results.sort(key=lambda x: abs(x["level"] - price))
        return results[:3]
    except Exception as e:
        log.debug("find_session_liquidity error: %s", e)
        return []


# ─── 4. Order Block ──────────────────────────────────────────────────────────

def find_order_blocks(
    bars: list[dict],
    direction: str,
    impulse_atr_mult: float = 1.5,
    atr: float = 0.0,
    max_bars: int = 300,
) -> list[dict]:
    """
    Stronger OB detection.

    BUY:
      last bearish candle before strong bullish impulse.
      impulse candle must close above OB high.

    SELL:
      last bullish candle before strong bearish impulse.
      impulse candle must close below OB low.

    This is still confirmation only.
    It must NOT create/select SR zones alone.
    """
    try:
        if not bars or len(bars) < 4:
            return []
        bars = bars[-max_bars:] if len(bars) > max_bars else bars

        d = (direction or "").upper()
        is_buy = d in ("BUY", "UP")
        _atr = atr if atr > 0 else _atr14(bars)
        if _atr <= 0:
            return []

        impulse_thr = _atr * impulse_atr_mult
        price = float(bars[-1].get("c") or 0)
        results: list[dict] = []

        for i in range(len(bars) - 2):
            ob = bars[i]
            imp = bars[i + 1]

            try:
                o = float(ob["o"]); h = float(ob["h"]); l = float(ob["l"]); c = float(ob["c"])
                io = float(imp["o"]); ih = float(imp["h"]); il = float(imp["l"]); ic = float(imp["c"])
            except Exception:
                continue

            ob_body = abs(c - o)
            ob_range = max(h - l, 1e-9)

            imp_body = abs(ic - io)
            imp_range = max(ih - il, 1e-9)

            strong_impulse = imp_body >= impulse_thr
            body_dominant = (imp_body / imp_range) >= 0.55

            if is_buy:
                # Bullish OB = bearish candle before bullish impulse
                if not (c < o):
                    continue
                if not (ic > io):
                    continue
                if not strong_impulse:
                    continue
                if not body_dominant:
                    continue
                # stronger validation: impulse closes above OB high
                if not (ic > h):
                    continue
                # OB should be below/near current price for BUY confirmation
                if l >= price:
                    continue

                mitigated = any(
                    float(bars[j]["l"]) <= h and float(bars[j]["h"]) >= l
                    for j in range(i + 2, len(bars))
                )

                quality = 0
                quality += 2 if ic > h else 0
                quality += 2 if imp_body >= (_atr * 2.0) else 1
                quality += 1 if (ob_body / ob_range) >= 0.40 else 0
                quality -= 1 if mitigated else 0

                results.append({
                    "high": round(h, 5),
                    "low": round(l, 5),
                    "open": round(o, 5),
                    "close": round(c, 5),
                    "type": "bullish_OB",
                    "ob_dir": "bull",
                    "role": "support",
                    "mitigated": bool(mitigated),
                    "quality": int(quality),
                    "impulse_body_atr": round(imp_body / _atr, 3),
                    "impulse_close_break": True,
                    "label": f"OB:{l:.5f}-{h:.5f}(BULL/SUP,Q{quality})",
                })

            else:
                # Bearish OB = bullish candle before bearish impulse
                if not (c > o):
                    continue
                if not (ic < io):
                    continue
                if not strong_impulse:
                    continue
                if not body_dominant:
                    continue
                # stronger validation: impulse closes below OB low
                if not (ic < l):
                    continue
                # OB should be above/near current price for SELL confirmation
                if h <= price:
                    continue

                mitigated = any(
                    float(bars[j]["h"]) >= l and float(bars[j]["l"]) <= h
                    for j in range(i + 2, len(bars))
                )

                quality = 0
                quality += 2 if ic < l else 0
                quality += 2 if imp_body >= (_atr * 2.0) else 1
                quality += 1 if (ob_body / ob_range) >= 0.40 else 0
                quality -= 1 if mitigated else 0

                results.append({
                    "high": round(h, 5),
                    "low": round(l, 5),
                    "open": round(o, 5),
                    "close": round(c, 5),
                    "type": "bearish_OB",
                    "ob_dir": "bear",
                    "role": "resistance",
                    "mitigated": bool(mitigated),
                    "quality": int(quality),
                    "impulse_body_atr": round(imp_body / _atr, 3),
                    "impulse_close_break": True,
                    "label": f"OB:{l:.5f}-{h:.5f}(BEAR/RES,Q{quality})",
                })

        results.sort(key=lambda x: (x.get("mitigated", False), -int(x.get("quality") or 0)))
        return results[:3]

    except Exception as e:
        log.debug("find_order_blocks error: %s", e)
        return []

# ─── 5. Fair Value Gap ───────────────────────────────────────────────────────

def find_fair_value_gaps(
    bars: list[dict],
    direction: str,
    min_gap_atr: float = 0.1,
    atr: float = 0.0,
    max_bars: int = 200,
) -> list[dict]:
    """
    Stronger FVG detection.

    Bullish FVG:
      b3.low > b1.high

    Bearish FVG:
      b3.high < b1.low

    Adds:
      - gap_size_atr
      - fill_status: OPEN / PARTIAL / FILLED
      - quality

    FVG is confirmation only.
    It must NOT create/select SR zones alone.
    """
    try:
        if not bars or len(bars) < 3:
            return []
        bars = bars[-max_bars:] if len(bars) > max_bars else bars

        d = (direction or "").upper()
        is_buy = d in ("BUY", "UP")
        _atr = atr if atr > 0 else _atr14(bars)
        if _atr <= 0:
            return []

        min_gap = _atr * min_gap_atr
        price = float(bars[-1].get("c") or 0)
        results: list[dict] = []

        for i in range(1, len(bars) - 1):
            b1, b2, b3 = bars[i - 1], bars[i], bars[i + 1]

            try:
                b1h = float(b1["h"]); b1l = float(b1["l"])
                b2o = float(b2["o"]); b2c = float(b2["c"])
                b3h = float(b3["h"]); b3l = float(b3["l"])
            except Exception:
                continue

            b2_body = abs(b2c - b2o)

            if is_buy:
                # Bullish FVG below/near price
                gap = b3l - b1h
                if gap < min_gap:
                    continue

                fvg_low = round(b1h, 5)
                fvg_high = round(b3l, 5)
                fvg_mid = round((fvg_low + fvg_high) / 2, 5)

                if fvg_high > price * 1.001:
                    continue

                future_lows = []
                for j in range(i + 2, len(bars)):
                    try:
                        future_lows.append(float(bars[j]["l"]))
                    except Exception:
                        pass

                fill_status = "OPEN"
                if future_lows:
                    min_future_low = min(future_lows)
                    if min_future_low <= fvg_low:
                        fill_status = "FILLED"
                    elif min_future_low <= fvg_mid:
                        fill_status = "PARTIAL"

                gap_size_atr = gap / _atr
                quality = 0
                quality += 2 if gap_size_atr >= 0.25 else 1
                quality += 1 if b2c > b2o and b2_body >= _atr else 0
                quality += 1 if fill_status == "OPEN" else 0
                quality -= 1 if fill_status == "FILLED" else 0

                results.append({
                    "high": fvg_high,
                    "low": fvg_low,
                    "mid": fvg_mid,
                    "type": "bullish_FVG",
                    "role": "support",
                    "fvg_dir": "bull",
                    "filled": fill_status == "FILLED",
                    "fill_status": fill_status,
                    "gap_size_atr": round(gap_size_atr, 3),
                    "quality": int(quality),
                    "label": f"FVG:{fvg_low:.5f}-{fvg_high:.5f}(BULL/SUP,{fill_status},Q{quality})",
                })

            else:
                # Bearish FVG above/near price
                gap = b1l - b3h
                if gap < min_gap:
                    continue

                fvg_low = round(b3h, 5)
                fvg_high = round(b1l, 5)
                fvg_mid = round((fvg_low + fvg_high) / 2, 5)

                if fvg_low < price * 0.999:
                    continue

                future_highs = []
                for j in range(i + 2, len(bars)):
                    try:
                        future_highs.append(float(bars[j]["h"]))
                    except Exception:
                        pass

                fill_status = "OPEN"
                if future_highs:
                    max_future_high = max(future_highs)
                    if max_future_high >= fvg_high:
                        fill_status = "FILLED"
                    elif max_future_high >= fvg_mid:
                        fill_status = "PARTIAL"

                gap_size_atr = gap / _atr
                quality = 0
                quality += 2 if gap_size_atr >= 0.25 else 1
                quality += 1 if b2c < b2o and b2_body >= _atr else 0
                quality += 1 if fill_status == "OPEN" else 0
                quality -= 1 if fill_status == "FILLED" else 0

                results.append({
                    "high": fvg_high,
                    "low": fvg_low,
                    "mid": fvg_mid,
                    "type": "bearish_FVG",
                    "role": "resistance",
                    "fvg_dir": "bear",
                    "filled": fill_status == "FILLED",
                    "fill_status": fill_status,
                    "gap_size_atr": round(gap_size_atr, 3),
                    "quality": int(quality),
                    "label": f"FVG:{fvg_low:.5f}-{fvg_high:.5f}(BEAR/RES,{fill_status},Q{quality})",
                })

        results.sort(key=lambda x: (
            x.get("fill_status") == "FILLED",
            x.get("fill_status") == "PARTIAL",
            -int(x.get("quality") or 0),
        ))
        return results[:3]

    except Exception as e:
        log.debug("find_fair_value_gaps error: %s", e)
        return []

# ─── 6. Round Numbers ─────────────────────────────────────────────────────────

def find_round_numbers(
    price: float,
    atr: float,
    sym: str,
    direction: str,
    proximity_atr: float = 1.5,
) -> list[dict]:
    """
    Find round number levels near price.

    XAUUSD / JPY pairs: $25 / 50-pip grid
    FX majors          : 50-pip grid (0.0050)

    For BUY  → round numbers below price (SSL — retail longs stop below round)
    For SELL → round numbers above price (BSL — retail shorts stop above round)

    Returns list of {level, type, label}
    """
    try:
        if price <= 0 or atr <= 0:
            return []

        d = (direction or "").upper()
        is_buy = d in ("BUY", "UP")
        proximity = atr * proximity_atr
        sym_u = (sym or "").upper()

        # grid step
        if sym_u == "XAUUSD":
            step = 25.0       # $25 for gold
        elif sym_u.endswith("JPY"):
            step = 0.50       # 50 pips for JPY
        else:
            step = 0.0050     # 50 pips for FX majors

        # build grid around price
        base = round(price / step) * step
        grid = [round(base + step * i, 5) for i in range(-6, 7)]

        liq_type = "SSL_RN" if is_buy else "BSL_RN"
        results = []
        for lv in grid:
            dist = price - lv if is_buy else lv - price
            if 0 < dist <= proximity:
                results.append({
                    "level": round(lv, 5),
                    "type" : liq_type,
                    "label": f"RN:{lv:.5f}",
                })

        results.sort(key=lambda x: abs(x["level"] - price))
        return results[:2]
    except Exception as e:
        log.debug("find_round_numbers error: %s", e)
        return []


# ─── PROXIMITY FILTER ─────────────────────────────────────────────────────────

def _near_zone(level: float, zone_low: float, zone_high: float, atr: float,
               proximity_atr: float = 3.0) -> bool:
    """Return True if level is within proximity_atr of the zone."""
    zone_mid = (zone_low + zone_high) / 2
    return abs(level - zone_mid) <= atr * proximity_atr


# ─── MAIN: detect_liq_signals ────────────────────────────────────────────────

def find_nearest_bsl_ssl(
    bars_h1: list[dict],
    price: float,
    atr: float,
) -> dict[str, Any]:
    """
    Direction-INDEPENDENT view: find resting BSL above price and SSL below price.

    Adds debug:
      - bsl_candidates_debug
      - ssl_candidates_debug

    Selection rule:
      1. Candidate must be on correct side of price with min distance.
      2. Prefer FRESH liquidity over SWEPT liquidity.
      3. Prefer stronger kind:
         EQH/EQL > SWING > SESSION
      4. Then nearest to price.
    """
    out = {
        "bsl": None,
        "ssl": None,
        "range_text": "—",
        "bsl_candidates_debug": [],
        "ssl_candidates_debug": [],
    }

    try:
        if not bars_h1 or price <= 0:
            return out

        highs: list[dict] = []
        lows: list[dict] = []

        # 1) Session highs/lows today + yesterday
        try:
            now_utc = datetime.now(timezone.utc)
            for target_date in (now_utc.date(), (now_utc - timedelta(days=1)).date()):
                day_bars = _filter_bars_by_date(bars_h1, target_date)
                if not day_bars:
                    continue
                for sess_name, (start_h, end_h) in _SESSIONS.items():
                    sess_bars = [
                        b for b in day_bars
                        if start_h <= datetime.fromtimestamp(
                            _bar_open_ms(b) / 1000, tz=timezone.utc
                        ).hour < end_h
                    ]
                    if not sess_bars:
                        continue

                    sh = max(float(b["h"]) for b in sess_bars)
                    sl = min(float(b["l"]) for b in sess_bars)

                    highs.append({"level": sh, "kind": f"{sess_name}_BSL", "source_rank": 3})
                    lows.append({"level": sl, "kind": f"{sess_name}_SSL", "source_rank": 3})
        except Exception:
            pass

        # 2) Untouched swings
        try:
            for s in find_untouched_swings(bars_h1, "SELL"):
                highs.append({
                    "level": float(s["level"]),
                    "kind": "SWING_BSL",
                    "source_rank": 2,
                    "fresh": bool(s.get("fresh")),
                })
            for s in find_untouched_swings(bars_h1, "BUY"):
                lows.append({
                    "level": float(s["level"]),
                    "kind": "SWING_SSL",
                    "source_rank": 2,
                    "fresh": bool(s.get("fresh")),
                })
        except Exception:
            pass

        # 3) Equal highs / lows
        try:
            _a = atr if atr > 0 else _atr14(bars_h1)
            for e in find_equal_levels(bars_h1, _a, "SELL"):
                highs.append({
                    "level": float(e["level"]),
                    "kind": "EQH",
                    "source_rank": 1,
                    "touches": int(e.get("touches") or 0),
                })
            for e in find_equal_levels(bars_h1, _a, "BUY"):
                lows.append({
                    "level": float(e["level"]),
                    "kind": "EQL",
                    "source_rank": 1,
                    "touches": int(e.get("touches") or 0),
                })
        except Exception:
            pass

        _atr = atr if atr > 0 else _atr14(bars_h1)
        _min_dist = float(_atr) * 0.5 if _atr > 0 else 0.0

        def _dedupe(xs: list[dict], side: str) -> list[dict]:
            """Merge near-identical liquidity levels."""
            out_x: list[dict] = []
            tol = max((_atr or 0.0) * 0.05, 1e-9)

            for x in sorted(xs, key=lambda r: float(r.get("level") or 0.0)):
                try:
                    lv = float(x.get("level"))
                except Exception:
                    continue

                existing = None
                for y in out_x:
                    if abs(float(y["level"]) - lv) <= tol:
                        existing = y
                        break

                if existing is None:
                    y = dict(x)
                    y["sources"] = [x.get("kind")]
                    out_x.append(y)
                else:
                    existing["sources"].append(x.get("kind"))
                    # keep better ranked source label
                    if int(x.get("source_rank") or 9) < int(existing.get("source_rank") or 9):
                        existing["kind"] = x.get("kind")
                        existing["source_rank"] = x.get("source_rank")
                    existing["touches"] = max(int(existing.get("touches") or 0), int(x.get("touches") or 0))
                    existing["fresh"] = bool(existing.get("fresh")) or bool(x.get("fresh"))

            return out_x

        highs = _dedupe(highs, "BSL")
        lows = _dedupe(lows, "SSL")

        def _decorate_bsl(x: dict) -> dict:
            y = dict(x)
            lv = float(y["level"])
            y["distance"] = round(float(lv - price), 5)
            y["distance_atr"] = round(float((lv - price) / _atr), 3) if _atr else None

            if lv <= price:
                y["eligible"] = False
                y["reject_reason"] = "below_or_at_price"
            elif lv <= price + _min_dist:
                y["eligible"] = False
                y["reject_reason"] = "too_close"
            else:
                y["eligible"] = True
                y["reject_reason"] = None

            sinfo = _sweep_info(bars_h1, lv, "BSL", lookback=100)
            y.update(sinfo)
            return y

        def _decorate_ssl(x: dict) -> dict:
            y = dict(x)
            lv = float(y["level"])
            y["distance"] = round(float(price - lv), 5)
            y["distance_atr"] = round(float((price - lv) / _atr), 3) if _atr else None

            if lv >= price:
                y["eligible"] = False
                y["reject_reason"] = "above_or_at_price"
            elif lv >= price - _min_dist:
                y["eligible"] = False
                y["reject_reason"] = "too_close"
            else:
                y["eligible"] = True
                y["reject_reason"] = None

            sinfo = _sweep_info(bars_h1, lv, "SSL", lookback=100)
            y.update(sinfo)
            return y

        bsl_dbg = [_decorate_bsl(x) for x in highs]
        ssl_dbg = [_decorate_ssl(x) for x in lows]

        def _score_liq(x: dict) -> tuple:
            """
            Lower tuple wins.
            Prefer:
              eligible
              fresh/not swept
              EQH/EQL over SWING over SESSION
              higher touches
              nearest
            """
            return (
                0 if x.get("eligible") else 1,
                0 if not x.get("swept") else 1,
                int(x.get("source_rank") or 9),
                -int(x.get("touches") or 0),
                float(x.get("distance_atr") or 1e9),
            )

        bsl_dbg.sort(key=_score_liq)
        ssl_dbg.sort(key=_score_liq)

        out["bsl_candidates_debug"] = bsl_dbg[:10]
        out["ssl_candidates_debug"] = ssl_dbg[:10]

        bsl_eligible = [x for x in bsl_dbg if x.get("eligible")]
        ssl_eligible = [x for x in ssl_dbg if x.get("eligible")]

        if bsl_eligible:
            out["bsl"] = bsl_eligible[0]
        if ssl_eligible:
            out["ssl"] = ssl_eligible[0]

        parts = []
        if out["bsl"]:
            if out["bsl"].get("swept"):
                cs = out["bsl"].get("candles_since_sweep")
                rx = out["bsl"].get("reaction_after_sweep")
                tag = f"(SWEPT,{cs}c,rx:{rx})"
            else:
                tag = "(FRESH)"
            parts.append(f"BSL↑ {out['bsl']['level']:.5f}{tag}")

        if out["ssl"]:
            if out["ssl"].get("swept"):
                cs = out["ssl"].get("candles_since_sweep")
                rx = out["ssl"].get("reaction_after_sweep")
                tag = f"(SWEPT,{cs}c,rx:{rx})"
            else:
                tag = "(FRESH)"
            parts.append(f"SSL↓ {out['ssl']['level']:.5f}{tag}")

        if parts:
            out["range_text"] = " | ".join(parts)

        return out

    except Exception as e:
        log.debug("find_nearest_bsl_ssl error: %s", e)
        return out

def find_major_liquidity_inventory(
    bars_h1: list[dict],
    price: float,
    atr: float,
) -> dict[str, Any]:
    """
    Historical liquidity inventory.

    Purpose:
      Used later for SR/zone quality scoring.

    Difference from find_nearest_bsl_ssl():
      - Does NOT only return next BSL/SSL target.
      - Keeps important historical EQH/EQL/SWING liquidity.
      - Shows whether levels are above/below price, swept/fresh, reaction.

    This must NOT create/select zones directly.
    """
    out = {
        "major_bsl": [],
        "major_ssl": [],
    }

    try:
        if not bars_h1 or price <= 0:
            return out

        _atr = atr if atr > 0 else _atr14(bars_h1)
        if _atr <= 0:
            return out

        highs: list[dict] = []
        lows: list[dict] = []

        # Equal highs/lows are very important liquidity pools
        try:
            for e in find_equal_levels(bars_h1, _atr, "SELL", max_bars=500):
                highs.append({
                    "level": float(e["level"]),
                    "kind": "EQH",
                    "source_rank": 1,
                    "touches": int(e.get("touches") or 0),
                })

            for e in find_equal_levels(bars_h1, _atr, "BUY", max_bars=500):
                lows.append({
                    "level": float(e["level"]),
                    "kind": "EQL",
                    "source_rank": 1,
                    "touches": int(e.get("touches") or 0),
                })
        except Exception:
            pass

        # Fresh/used swing liquidity
        try:
            for s in find_untouched_swings(bars_h1[-500:], "SELL"):
                highs.append({
                    "level": float(s["level"]),
                    "kind": "SWING_BSL",
                    "source_rank": 2,
                    "fresh": bool(s.get("fresh")),
                    "touches": 0,
                })

            for s in find_untouched_swings(bars_h1[-500:], "BUY"):
                lows.append({
                    "level": float(s["level"]),
                    "kind": "SWING_SSL",
                    "source_rank": 2,
                    "fresh": bool(s.get("fresh")),
                    "touches": 0,
                })
        except Exception:
            pass

        # Session highs/lows: lower priority, today/yesterday only
        try:
            now_utc = datetime.now(timezone.utc)
            for target_date in (now_utc.date(), (now_utc - timedelta(days=1)).date()):
                day_bars = _filter_bars_by_date(bars_h1, target_date)
                if not day_bars:
                    continue

                for sess_name, (start_h, end_h) in _SESSIONS.items():
                    sess_bars = [
                        b for b in day_bars
                        if start_h <= datetime.fromtimestamp(
                            _bar_open_ms(b) / 1000, tz=timezone.utc
                        ).hour < end_h
                    ]
                    if not sess_bars:
                        continue

                    sh = max(float(b["h"]) for b in sess_bars)
                    sl = min(float(b["l"]) for b in sess_bars)

                    highs.append({
                        "level": float(sh),
                        "kind": f"{sess_name}_BSL",
                        "source_rank": 3,
                        "touches": 0,
                    })

                    lows.append({
                        "level": float(sl),
                        "kind": f"{sess_name}_SSL",
                        "source_rank": 3,
                        "touches": 0,
                    })
        except Exception:
            pass

        def _decorate(x: dict, liq_type: str) -> dict:
            y = dict(x)
            lv = float(y["level"])

            if liq_type == "BSL":
                y["side"] = "above_price" if lv > price else "below_price"
                y["distance"] = round(float(lv - price), 5)
                y["distance_atr"] = round(float((lv - price) / _atr), 3)
                sinfo = _sweep_info(bars_h1, lv, "BSL", lookback=100)
            else:
                y["side"] = "below_price" if lv < price else "above_price"
                y["distance"] = round(float(price - lv), 5)
                y["distance_atr"] = round(float((price - lv) / _atr), 3)
                sinfo = _sweep_info(bars_h1, lv, "SSL", lookback=100)

            y.update(sinfo)

            # Inventory quality, not trade score.
            quality = 0

            # Liquidity type
            quality += 3 if y.get("kind") in ("EQH", "EQL") else 0
            quality += 2 if "SWING" in str(y.get("kind")) else 0
            quality += 1 if (
                "LONDON" in str(y.get("kind"))
                or "NEWYORK" in str(y.get("kind"))
            ) else 0

            # Touches
            quality += min(int(y.get("touches") or 0), 10)

            # Fresh liquidity bonus
            quality += 2 if y.get("fresh") else 0

            # Sweep / reaction quality
            try:
                rx = float(y.get("reaction_after_sweep") or 0.0)
                cs = int(y.get("candles_since_sweep") or 0)

                # Historical sweep that produced meaningful reaction
                if y.get("swept") and rx >= 0.5 * _atr:
                    quality += 3

                # Recently consumed liquidity with no reaction
                if y.get("swept"):
                    if cs <= 2 and rx <= 0:
                        quality -= 5
                    elif cs <= 5 and rx <= 0:
                        quality -= 3
                    elif rx <= 0:
                        quality -= 1

            except Exception:
                pass

            y["liq_quality"] = int(quality)
            return y

        bsl = [_decorate(x, "BSL") for x in highs]
        ssl = [_decorate(x, "SSL") for x in lows]

        # De-dupe near-identical levels
        def _dedupe(xs: list[dict]) -> list[dict]:
            tol = max(_atr * 0.05, 1e-9)
            kept: list[dict] = []

            for x in sorted(xs, key=lambda r: float(r.get("level") or 0.0)):
                lv = float(x.get("level") or 0.0)
                found = None

                for k in kept:
                    if abs(float(k.get("level") or 0.0) - lv) <= tol:
                        found = k
                        break

                if found is None:
                    y = dict(x)
                    y["sources"] = [x.get("kind")]
                    kept.append(y)
                else:
                    found.setdefault("sources", []).append(x.get("kind"))

                    if int(x.get("liq_quality") or 0) > int(found.get("liq_quality") or 0):
                        keep_sources = found.get("sources", [])
                        found.clear()
                        found.update(dict(x))
                        found["sources"] = keep_sources + [x.get("kind")]
                    else:
                        found["touches"] = max(int(found.get("touches") or 0), int(x.get("touches") or 0))
                        found["liq_quality"] = max(int(found.get("liq_quality") or 0), int(x.get("liq_quality") or 0))

            return kept

        bsl = _dedupe(bsl)
        ssl = _dedupe(ssl)

        bsl.sort(key=lambda x: (-int(x.get("liq_quality") or 0), abs(float(x.get("distance_atr") or 999))))
        ssl.sort(key=lambda x: (-int(x.get("liq_quality") or 0), abs(float(x.get("distance_atr") or 999))))

        out["major_bsl"] = bsl[:10]
        out["major_ssl"] = ssl[:10]

        return out

    except Exception as e:
        log.debug("find_major_liquidity_inventory error: %s", e)
        return out

def detect_liq_signals(
    sym: str,
    direction: str,
    zone: dict | None,
    bars_h1: list[dict],
    bars_h4: list[dict],
    price: float,
    atr: float,
    bars_d1: list[dict] | None = None,
) -> dict[str, Any]:
    """
    Run all 6 liquidity detections and return:
      - signals: list of label strings present
      - liq_text: pipe-separated string for UI LIQ column
      - liq_detail: full structured data per signal type

    Safe to call — any exception returns empty result.
    """
    _empty = {"signals": [], "liq_text": "—", "liq_confidence": "—",
              "bsl_ssl": {"bsl": None, "ssl": None, "range_text": "—"},
              "range_text": "—", "liq_detail": {},
              "regime": {"h1": {"label": "—"}, "h4": {"label": "—"},
                         "d1": {"label": "—"}, "text": "REG —"}}

    try:
        if not bars_h1 or price <= 0:
            return _empty

        _atr = atr if atr > 0 else _atr14(bars_h1)
        if _atr <= 0:
            return _empty

        d = (direction or "").upper()

        # zone boundaries for proximity filter
        if isinstance(zone, dict):
            z_low  = float(zone.get("low")  or zone.get("level") or price)
            z_high = float(zone.get("high") or zone.get("level") or price)
        else:
            z_low  = price - _atr * 3.0
            z_high = price + _atr * 3.0

        labels: list[str] = []
        detail: dict[str, Any] = {}

        # ── 1. Equal Highs / Lows ────────────────────────────────────────
        eq = find_equal_levels(bars_h1, _atr, d)
        eq_near = [e for e in eq if _near_zone(e["level"], z_low, z_high, _atr)]
        if eq_near:
            labels.append(eq_near[0]["label"])
        detail["eq_levels"] = eq_near

        # ── 2. Untouched Swings ──────────────────────────────────────────
        sw = find_untouched_swings(bars_h1, d)
        sw_near = [s for s in sw if _near_zone(s["level"], z_low, z_high, _atr)]
        if sw_near:
            labels.append(sw_near[0]["label"])
        detail["swing"] = sw_near

        # ── 3. Session Levels ────────────────────────────────────────────
        sess = find_session_liquidity(bars_h1, d)
        sess_near = [s for s in sess if _near_zone(s["level"], z_low, z_high, _atr)]
        for s in sess_near[:2]:
            labels.append(s["label"])
        detail["session"] = sess_near

        # ── 4. Order Block — H1 then H4 ─────────────────────────────────
        ob_h1 = find_order_blocks(bars_h1, d, atr=_atr)
        ob_h4 = find_order_blocks(bars_h4, d, atr=_atr) if bars_h4 else []

        ob_all = ob_h1 + ob_h4
        ob_near = [
            ob for ob in ob_all
            if _near_zone((ob["low"] + ob["high"]) / 2, z_low, z_high, _atr)
        ]
        # confluence: an OB band that actually OVERLAPS the entry zone is a
        # much stronger setup than one merely nearby.
        ob_in_zone = False
        for ob in ob_near:
            ob["in_zone"] = _overlaps_zone(ob["low"], ob["high"], z_low, z_high)
            if ob["in_zone"]:
                ob_in_zone = True
                if "(IN_ZONE)" not in ob["label"]:
                    ob["label"] += "(IN_ZONE)"
        if ob_near:
            _show_ob = next((o for o in ob_near if o.get("in_zone")), ob_near[0])
            labels.append(_show_ob["label"])
        detail["order_blocks"] = ob_near

        # ── 5. Fair Value Gap — H1 then H4 ──────────────────────────────
        fvg_h1 = find_fair_value_gaps(bars_h1, d, atr=_atr)
        fvg_h4 = find_fair_value_gaps(bars_h4, d, atr=_atr) if bars_h4 else []

        fvg_all = [f for f in fvg_h1 + fvg_h4 if not f.get("filled")]
        fvg_near = [
            f for f in fvg_all
            if _near_zone(f["mid"], z_low, z_high, _atr)
        ]
        # confluence: an FVG band overlapping the entry zone strengthens the setup.
        fvg_in_zone = False
        for f in fvg_near:
            f["in_zone"] = _overlaps_zone(f["low"], f["high"], z_low, z_high)
            if f["in_zone"]:
                fvg_in_zone = True
                if "(IN_ZONE)" not in f["label"]:
                    f["label"] += "(IN_ZONE)"
        if fvg_near:
            _show_fvg = next((f for f in fvg_near if f.get("in_zone")), fvg_near[0])
            labels.append(_show_fvg["label"])
        detail["fvg"] = fvg_near

        # ── 6. Round Numbers ─────────────────────────────────────────────
        rn = find_round_numbers(price, _atr, sym, d)
        rn_near = [r for r in rn if _near_zone(r["level"], z_low, z_high, _atr)]
        if rn_near:
            labels.append(rn_near[0]["label"])
        detail["round_numbers"] = rn_near

        liq_text = " | ".join(labels) if labels else "—"

        # ── REGIME (always-on) — appended to LIQ column for chart verification ──
        regime = detect_regime(bars_h1, bars_h4, bars_d1)
        liq_text = f"{liq_text}    {regime['text']}" if liq_text != "—" else regime["text"]

        # ── direction-independent BSL/SSL range (always both sides) ──────
        bsl_ssl = find_nearest_bsl_ssl(bars_h1, price, _atr)
        detail["bsl_ssl"] = bsl_ssl
        liq_inventory = find_major_liquidity_inventory(bars_h1, price, _atr)
        detail["liq_inventory"] = liq_inventory

        # ── confluence grade (display-only confirmation) ────────────────
        # OB-in-zone + FVG-in-zone => stronger reversal expectation at the zone.
        _conf_hits = int(ob_in_zone) + int(fvg_in_zone)
        if _conf_hits >= 2:
            liq_confidence = "HIGH"
        elif _conf_hits == 1:
            liq_confidence = "MEDIUM"
        else:
            liq_confidence = "—"
        detail["confluence"] = {
            "ob_in_zone" : ob_in_zone,
            "fvg_in_zone": fvg_in_zone,
            "grade"      : liq_confidence,
        }

        return {
            "signals"       : labels,
            "liq_text"      : liq_text,
            "liq_confidence": liq_confidence,
            "bsl_ssl"       : bsl_ssl,
            "range_text"    : bsl_ssl.get("range_text", "—"),
            "liq_inventory" : liq_inventory,
            "liq_detail"    : detail,
            "regime"        : regime,
        }

    except Exception as e:
        log.debug("detect_liq_signals error sym=%s err=%s", sym, e)
        return _empty


# ════════════════════════════════════════════════════════════════════
# SR × LIQUIDITY BRIDGE
# Scores each clean SR level (active_supports/active_resistances) by the
# liquidity evidence sitting on it. Enhances SR ranking; does not replace it.
# Produces the doc's additive quality_score + per-level evidence + reason.
# ════════════════════════════════════════════════════════════════════

def _zones_overlap(lo1, hi1, lo2, hi2):
    try:
        return float(lo1) <= float(hi2) and float(lo2) <= float(hi1)
    except Exception:
        return False


def _measure_reaction(bars, zone_low, zone_high, side, atr):
    """Largest favorable move (in ATR) after price touched the zone — the
    doc's 'reaction strength', measured whether or not a sweep occurred.
    side='support' -> measure up-move after a low touch; 'resistance' -> down."""
    try:
        if not bars or atr <= 0:
            return 0.0
        best = 0.0
        n = len(bars)
        look = 6
        for i in range(max(0, n - 200), n - 1):
            b = bars[i]
            lo = float(b.get("l")); hi = float(b.get("h"))
            touched = (lo <= zone_high and hi >= zone_low)
            if not touched:
                continue
            fut = bars[i + 1:i + 1 + look]
            if not fut:
                continue
            if side == "support":
                mv = max(float(f.get("h")) for f in fut) - zone_high
            else:
                mv = zone_low - min(float(f.get("l")) for f in fut)
            if mv > best:
                best = mv
        return round(float(best) / float(atr), 3)
    except Exception:
        return 0.0


def score_sr_with_liquidity(sym, sr_bundle, bars_h1, bars_h4, price, atr):
    """
    Bridge: enrich SR active levels with liquidity evidence + quality_score.

    Reads sr_bundle['active_supports'] / ['active_resistances'] (the clean,
    consolidated, sided, broken-checked levels from trend_sr).

    Returns:
      {
        "scored_supports":   [ {level, low, high, quality_score, evidence{...},
                                selection_reason, ...}, ... ],  # ranked best-first
        "scored_resistances":[ ... ],
        "best_support": {...} | None,
        "best_resistance": {...} | None,
      }
    """
    out = {"scored_supports": [], "scored_resistances": [],
           "best_support": None, "best_resistance": None}
    try:
        if not bars_h1 or price <= 0:
            return out
        _atr = atr if atr and atr > 0 else _atr14(bars_h1)
        if _atr <= 0:
            return out

        active_sup = (sr_bundle or {}).get("active_supports") or []
        active_res = (sr_bundle or {}).get("active_resistances") or []

        # ---- precompute liquidity objects once (both directions) ----
        ob_bull = find_order_blocks(bars_h1, "BUY", atr=_atr) + \
                  (find_order_blocks(bars_h4, "BUY", atr=_atr) if bars_h4 else [])
        ob_bear = find_order_blocks(bars_h1, "SELL", atr=_atr) + \
                  (find_order_blocks(bars_h4, "SELL", atr=_atr) if bars_h4 else [])
        fvg_bull = [f for f in find_fair_value_gaps(bars_h1, "BUY", atr=_atr) if not f.get("filled")]
        fvg_bear = [f for f in find_fair_value_gaps(bars_h1, "SELL", atr=_atr) if not f.get("filled")]
        eq_ssl = find_equal_levels(bars_h1, _atr, "BUY")   # equal lows
        eq_bsl = find_equal_levels(bars_h1, _atr, "SELL")  # equal highs
        sw_ssl = [s for s in find_untouched_swings(bars_h1, "BUY") if s.get("fresh")]
        sw_bsl = [s for s in find_untouched_swings(bars_h1, "SELL") if s.get("fresh")]
        rn_buy = find_round_numbers(price, _atr, sym, "BUY")
        rn_sell = find_round_numbers(price, _atr, sym, "SELL")

        def _near_level(objs, lo, hi, key="level", tol=None):
            t = tol if tol is not None else 0.20 * _atr
            hits = []
            for o in objs:
                try:
                    lv = float(o.get(key))
                    if (lo - t) <= lv <= (hi + t):
                        hits.append(o)
                except Exception:
                    pass
            return hits

        def _overlap_objs(objs, lo, hi):
            return [o for o in objs if _zones_overlap(lo, hi, o.get("low"), o.get("high"))]

        def _score_one(z, side):
            lo = float(z.get("low", z.get("level")))
            hi = float(z.get("high", z.get("level")))
            lvl = float(z.get("level"))
            base = float(z.get("sr_score") or 0.0)

            ev = {}
            score = base

            # reaction strength (capped contribution)
            react = _measure_reaction(bars_h1, lo, hi, side, _atr)
            ev["reaction_atr"] = react
            score += min(react, 4.0) * 2.0          # up to +8

            # OB overlap
            obs = _overlap_objs(ob_bull if side == "support" else ob_bear, lo, hi)
            if obs:
                q = max(int(o.get("quality") or 0) for o in obs)
                ev["ob_overlap"] = True
                ev["ob_quality"] = q
                score += min(q, 5) * 1.0            # up to +5
            else:
                ev["ob_overlap"] = False

            # FVG overlap (fresh only)
            fvgs = _overlap_objs(fvg_bull if side == "support" else fvg_bear, lo, hi)
            ev["fvg_overlap"] = bool(fvgs)
            if fvgs:
                score += 3.0

            # equal-level pool (SSL for support / BSL for resistance)
            pools = _near_level(eq_ssl if side == "support" else eq_bsl, lo, hi)
            if pools:
                tch = max(int(p.get("touches") or 0) for p in pools)
                ev["liq_pool"] = True
                ev["pool_touches"] = tch
                score += min(tch, 6) * 0.8          # up to ~+5
            else:
                ev["liq_pool"] = False

            # fresh swing nearby
            fsw = _near_level(sw_ssl if side == "support" else sw_bsl, lo, hi)
            ev["fresh_swing"] = bool(fsw)
            if fsw:
                score += 2.0

            # round number
            rns = _near_level(rn_buy if side == "support" else rn_sell, lo, hi, tol=0.10 * _atr)
            ev["round_number"] = bool(rns)
            if rns:
                score += 1.5

            # HTF confluence (already flagged by SR consolidation)
            ev["htf_confluence"] = bool(z.get("htf_confluence"))
            if ev["htf_confluence"]:
                score += 2.0

            # swept-and-reclaimed = strength (from SR broken-check)
            if z.get("swept"):
                ev["swept_reclaimed"] = True
                score += 2.5
            else:
                ev["swept_reclaimed"] = False

            # penalties
            if z.get("broken"):
                score -= 6.0
                ev["broken"] = True
            width = hi - lo
            if width > 1.5 * _atr:
                score -= 2.0
                ev["too_wide"] = True

            y = dict(z)
            y["quality_score"] = round(score, 2)
            y["evidence"] = ev

            # human-readable reason
            reasons = []
            if ev.get("htf_confluence"): reasons.append("H1+H4 confluence")
            if ev.get("ob_overlap"): reasons.append(f"OB overlap(Q{ev.get('ob_quality')})")
            if ev.get("fvg_overlap"): reasons.append("fresh FVG overlap")
            if ev.get("liq_pool"): reasons.append(f"liq pool({ev.get('pool_touches')}x)")
            if react >= 1.0: reasons.append(f"reaction {react}ATR")
            if ev.get("fresh_swing"): reasons.append("fresh swing")
            if ev.get("round_number"): reasons.append("round#")
            if ev.get("swept_reclaimed"): reasons.append("swept+reclaimed")
            if ev.get("broken"): reasons.append("BROKEN(-)")
            y["selection_reason"] = (f"score {y['quality_score']}: " +
                                     (", ".join(reasons) if reasons else "base SR only"))
            return y

        scored_sup = sorted((_score_one(z, "support") for z in active_sup),
                            key=lambda r: -r["quality_score"])
        scored_res = sorted((_score_one(z, "resistance") for z in active_res),
                            key=lambda r: -r["quality_score"])

        out["scored_supports"] = scored_sup
        out["scored_resistances"] = scored_res
        out["best_support"] = scored_sup[0] if scored_sup else None
        out["best_resistance"] = scored_res[0] if scored_res else None
        return out
    except Exception as e:
        log.debug("score_sr_with_liquidity error sym=%s err=%s", sym, e)
        return out
