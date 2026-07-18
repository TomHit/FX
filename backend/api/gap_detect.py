"""
gap_detect.py — broker-independent weekend/session gap detection.

WHY THIS EXISTS
---------------
You want to test the hypothesis: "do SL hits cluster after a gap open?"
To answer that honestly you need the gap recorded AT ENTRY on every trade,
so you can later segment win-rate by gap context instead of arguing about it.

HOW IT WORKS (and why not the clock)
------------------------------------
Broker candle-roll times differ by broker, server timezone and DST. TradingView
showed the Jul-13 break at 18:00 NY; your MT5 server rolls somewhere else. So we
do NOT hardcode a session time.

Instead we find the market-closure DISCONTINUITY in our own H1 bars: any place
where consecutive bar open-times are more than MIN_BREAK_H apart means the market
was shut. That is the gap boundary, by definition, in the data we actually trade.
Same principle as _freshest_device in usd_strength.py — derive it, don't assume it.

    gap = (first open AFTER the break) - (last close BEFORE the break)

WHAT IT DOES NOT DO
-------------------
- Does not detect intraday "gaps". In FX Mon-Thu the market is continuous; the
  daily candle closes and reopens on the same tick. The only real gaps are the
  weekend break and (rarely) a holiday break. If you ask this for a gap on a
  Wednesday you will correctly get the previous weekend's gap, with
  hours_since_gap telling you how stale it is.
- Does not judge whether the gap mattered. It records; you segment later.
"""

import json
import time
from typing import Any, Dict, Optional

# Reuse the self-healing freshest-device snapshot reader. If the feed rotates
# device ids (it does, roughly daily), this follows it automatically.
from api.usd_strength import _get_snapshot

# ── tunables ────────────────────────────────────────────────────────────────
MIN_BREAK_H = 3.0        # >3h with no bars = market was closed. Excludes the
                         # 1h daily broker maintenance break; catches weekends (~48h).
GAP_MIN_PCT = 0.0005     # 0.05% — below this the "gap" is noise, not an event.
GAP_RECENT_H = 24.0      # a trade opened within this many hours of the reopen
                         # is considered "near the gap" for segmentation.
MAX_LOOKBACK_BARS = 400  # don't scan the whole snapshot

# Pip size is a display convenience only; gap_pct is the comparable measure.
_PIP = {"XAUUSD": 1.0}
def _pip_size(symbol: str) -> float:
    s = (symbol or "").upper()
    if s in _PIP:
        return _PIP[s]
    return 0.01 if s.endswith("JPY") else 0.0001


def _now_ms() -> int:
    return int(time.time() * 1000)


def _bar_ms(b: Dict[str, Any]) -> int:
    """Bar open time in ms. This feed stores epoch SECONDS in 't'."""
    for k in ("t_open_ms", "t", "time", "open_ms"):
        v = b.get(k)
        if v:
            try:
                n = int(v)
            except (TypeError, ValueError):
                continue
            return n * 1000 if n < 100_000_000_000 else n
    return 0

def _f(v) -> Optional[float]:
    try:
        f = float(v)
        return f if f > 0 else None
    except (TypeError, ValueError):
        return None


def last_gap(R, symbol: str, tf: str = "H1") -> Optional[Dict[str, Any]]:
    """Most recent market-closure gap for this symbol, or None if no break is
    visible in the retained bars (they may have scrolled out)."""
    try:
        snap = _get_snapshot(R, symbol, tf)
        if not snap:
            return None
        bars = snap.get("bars") or []
        if len(bars) < 2:
            return None

        bars = [b for b in bars if _bar_ms(b)]
        bars.sort(key=_bar_ms)
        if len(bars) > MAX_LOOKBACK_BARS:
            bars = bars[-MAX_LOOKBACK_BARS:]

        break_ms = MIN_BREAK_H * 3600 * 1000

        # walk backwards: the first discontinuity we meet is the most recent one
        for i in range(len(bars) - 1, 0, -1):
            t_prev = _bar_ms(bars[i - 1])
            t_curr = _bar_ms(bars[i])
            if (t_curr - t_prev) <= break_ms:
                continue

            close_before = _f(bars[i - 1].get("c"))
            open_after = _f(bars[i].get("o"))
            if not (close_before and open_after):
                return None

            gap_abs = open_after - close_before
            gap_pct = gap_abs / close_before
            hours_since = (_now_ms() - t_curr) / 3_600_000.0

            return {
                "gap_abs": round(gap_abs, 6),
                "gap_pct": round(gap_pct, 6),
                "gap_pips": round(gap_abs / _pip_size(symbol), 1),
                "gap_direction": "up" if gap_abs > 0 else ("down" if gap_abs < 0 else "flat"),
                "gap_is_significant": abs(gap_pct) >= GAP_MIN_PCT,
                "gap_close_before": close_before,
                "gap_open_after": open_after,
                "gap_open_ms": t_curr,
                "gap_break_hours": round((t_curr - t_prev) / 3_600_000.0, 1),
                "hours_since_gap": round(hours_since, 1),
                "gap_recent": hours_since <= GAP_RECENT_H,
            }
        return None
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning("gap_detect: last_gap(%s) failed: %s", symbol, e)
        return None


def gap_context_for_trade(R, symbol: str, side: str) -> Dict[str, Any]:
    """Fields to stamp on an entry snapshot. Always returns a dict (never raises),
    so a gap-detection failure can never block a trade being captured.

    gap_trade_type is the field your hypothesis actually needs:
      continuation — trading WITH the gap (gap down + SELL)
      fade         — trading AGAINST the gap, i.e. betting on the gap filling
      n/a          — no recent gap, or no direction

    On 2026-07-13 all three losers were 'continuation' into a gap that then
    filled. If that repeats across many trades, you have a real finding.
    """
    out: Dict[str, Any] = {
        "gap_pct": None,
        "gap_pips": None,
        "gap_direction": None,
        "gap_is_significant": None,
        "hours_since_gap": None,
        "gap_recent": None,
        "gap_trade_type": "n/a",
    }
    try:
        g = last_gap(R, symbol)
        if not g:
            return out

        out.update({
            "gap_pct": g["gap_pct"],
            "gap_pips": g["gap_pips"],
            "gap_direction": g["gap_direction"],
            "gap_is_significant": g["gap_is_significant"],
            "hours_since_gap": g["hours_since_gap"],
            "gap_recent": g["gap_recent"],
        })

        # only classify the trade against the gap if the gap is recent AND real
        if not (g["gap_recent"] and g["gap_is_significant"]):
            return out

        s = (side or "").upper()
        trade_dir = 1 if s == "BUY" else (-1 if s == "SELL" else 0)
        gap_dir = 1 if g["gap_direction"] == "up" else (-1 if g["gap_direction"] == "down" else 0)
        if trade_dir and gap_dir:
            out["gap_trade_type"] = "continuation" if trade_dir == gap_dir else "fade"
        return out
    except Exception:
        return out


if __name__ == "__main__":
    import sys
    sys.path.insert(0, "/opt/xauapi")
    from api.trend_endpoints import R

    print("=== LAST GAP PER SYMBOL ===")
    for sym in ("EURUSD", "GBPUSD", "USDJPY", "USDCHF", "USDCAD", "XAUUSD"):
        g = last_gap(R, sym)
        if not g:
            print(f"{sym:8} no break found in retained bars")
            continue
        flag = "SIGNIFICANT" if g["gap_is_significant"] else "noise"
        print(f"{sym:8} {g['gap_direction']:5} {g['gap_pips']:>7.1f} pips "
              f"({g['gap_pct']*100:+.3f}%) {flag:12} "
              f"closed {g['gap_break_hours']}h, {g['hours_since_gap']}h ago")
