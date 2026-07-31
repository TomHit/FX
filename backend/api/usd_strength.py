"""
usd_strength.py — broker-independent synthetic USD-strength (DXY-style) signal.

WHY THIS EXISTS
---------------
Instead of depending on a broker's DXY symbol (DXY.cash on FTMO, DX future on
Apex/Tradovate, absent on some firms), we synthesize dollar direction from the
FX majors we already trade. This survives every prop-firm switch because it
needs nothing but the pairs already in the feed.

HONEST DESIGN (fixes the flaws of the old macro_state.py)
---------------------------------------------------------
- ONE signal, not one value copied into dxy/us10y/usd_rate (the old triple-count).
- Sign-aligned: USD-quote pairs inverted, USD-base pairs kept, so a rising value
  unambiguously means "dollar strengthening".
- Simple window return (close_now vs close_N_ago), not bar-by-bar log-return
  averaging (which saturated on noise in the old version).
- Slow timeframes (H4/H1) so the bias is a real lean, not 15-minute noise.
- STALE-GUARDED: rejects any snapshot older than MAX_AGE; excludes missing/stale
  pairs; returns 'flat' if too few pairs remain. Never computes off old data.
- Capture-only signal (for analytics). NOT wired into entry/exit.

Basket + alignment:
  EURUSD (quote) -> invert     GBPUSD (quote) -> invert
  USDJPY (base)  -> keep       USDCHF (base)  -> keep    USDCAD (base) -> keep
"""

from __future__ import annotations
import json
import time
import logging
from typing import Optional, Dict, List, Any

log = logging.getLogger("xtl.usd_strength")

# ── config ───────────────────────────────────────────────────────────────────
# No hardcoded device — we pick the freshest device per symbol/tf at read time,
# so the signal self-heals when the MT5 feed moves to a new device id.
_DEVICE_CACHE = {}          # (symbol,tf) -> (device_id, cached_at_ms)
_DEVICE_CACHE_TTL_MS = 60_000   # re-scan for freshest device at most once/min

# pair -> +1 if USD is the BASE (keep), -1 if USD is the QUOTE (invert)
BASKET = {
    "EURUSD": -1,
    "GBPUSD": -1,
    "USDJPY": +1,
    "USDCHF": +1,
    "USDCAD": +1,
}

# lookback bars per timeframe (D1 intentionally omitted for now)
LOOKBACK = {"H4": 30, "H1": 48}

# a bias is 'flat' unless |strength| exceeds this (fraction, e.g. 0.0015 = 0.15%)
FLAT_BAND = 0.0015

# reject a pair's snapshot if its last bar is older than this many hours
MAX_AGE_H = {"H4": 12.0, "H1": 4.0}

# need at least this many valid pairs to trust the average
MIN_PAIRS = 3


def _freshest_device(
    R,
    symbol: str,
    tf: str,
) -> Optional[str]:
    """
    Return the latest OHLC device pointer for symbol/tf.

    The OHLC writer maintains:

        xtl:ohlc:latest:<SYMBOL>:<TF>

    This avoids scanning the entire Redis keyspace.
    """

    symbol_u = str(
        symbol or ""
    ).upper().strip()

    tf_u = str(
        tf or ""
    ).upper().strip()

    if not symbol_u or not tf_u:
        return None

    now_ms = time.time() * 1000
    cache_key = (
        symbol_u,
        tf_u,
    )

    cached = _DEVICE_CACHE.get(
        cache_key
    )

    if (
        cached
        and (
            now_ms
            - cached[1]
        )
        < _DEVICE_CACHE_TTL_MS
    ):
        return cached[0]

    latest_device = None

    try:
        latest_device = R.get(
            f"xtl:ohlc:latest:"
            f"{symbol_u}:{tf_u}"
        )

        if isinstance(
            latest_device,
            (bytes, bytearray),
        ):
            latest_device = (
                latest_device.decode(
                    "utf-8",
                    errors="ignore",
                )
            )

        latest_device = str(
            latest_device or ""
        ).strip()

    except Exception:
        latest_device = ""

    if latest_device:
        _DEVICE_CACHE[
            cache_key
        ] = (
            latest_device,
            now_ms,
        )

        return latest_device

    # Preserve an already cached device during a
    # temporary Redis read failure or missing pointer.
    if cached:
        return cached[0]

    return None

def _get_snapshot(
    R,
    symbol: str,
    tf: str,
    device_id: str | None = None,
) -> Optional[Dict[str, Any]]:
    """
    Read one OHLC snapshot.

    When device_id is supplied:
      - read only that exact device
      - never scan or borrow another device

    When device_id is omitted:
      - preserve the existing global/freshest-device behaviour
      - keeps current UI and legacy callers backward-compatible
    """

    try:
        sym = str(symbol or "").upper().strip()
        timeframe = str(tf or "").upper().strip()
        dev = str(device_id or "").strip()

        if not sym or not timeframe:
            return None

        if not dev:
            dev = _freshest_device(
                R,
                sym,
                timeframe,
            ) or ""

        if not dev:
            return None

        raw = R.get(
            f"xtl:ohlc:snap:{dev}:{sym}:{timeframe}"
        )

        if not raw:
            return None

        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode(
                "utf-8",
                "ignore",
            )

        data = json.loads(raw)

        # Protect against accidentally double-encoded JSON.
        if isinstance(data, str):
            data = json.loads(data)

        return (
            data
            if isinstance(data, dict)
            else None
        )

    except Exception as exc:
        log.warning(
            "usd_strength: snapshot read failed "
            "symbol=%s tf=%s device=%s err=%r",
            symbol,
            tf,
            device_id,
            exc,
        )
        return None

def _closes(snap: Dict[str, Any]) -> List[float]:
    """Extract chronological complete-bar closes."""
    bars = snap.get("bars") or []
    out = []
    for b in bars:
        if not isinstance(b, dict):
            continue
        c = b.get("c")
        try:
            if c is not None:
                out.append(float(c))
        except (TypeError, ValueError):
            pass
    return out


def _is_fresh(snap: Dict[str, Any], tf: str) -> bool:
    """True if the snapshot's last bar is recent enough to trust."""
    ts = snap.get("lastClosedTs") or 0
    try:
        ts = float(ts)
    except (TypeError, ValueError):
        return False
    if ts <= 0:
        return False
    age_h = (time.time() * 1000 - ts) / 3_600_000.0
    # lastClosedTs can sit slightly in the future (next-close marker); allow it.
    return age_h <= MAX_AGE_H.get(tf, 12.0)


def _pair_return(closes: List[float], lookback: int) -> Optional[float]:
    """Simple % change over the lookback window. None if not enough bars."""
    if len(closes) < lookback + 1:
        return None
    now = closes[-1]
    then = closes[-1 - lookback]
    if then == 0:
        return None
    return (now - then) / then


def usd_strength_tf(
    R,
    tf: str,
    device_id: str | None = None,
) -> Dict[str, Any]:
    """
    Compute synthetic USD strength for one timeframe.
    Returns: {strength, bias, n_pairs, tf, stale_excluded}
      strength: float (sign-aligned avg return; + = dollar up)  or None
      bias: 'up' | 'down' | 'flat'
    """
    lookback = LOOKBACK.get(tf)
    if lookback is None:
        return {
            "tf": tf,
            "strength": None,
            "bias": "flat",
            "n_pairs": 0,
            "stale_excluded": [],
            "device_id": str(device_id or "").strip() or None,
            "source_scope": (
                "DEVICE"
                if device_id
                else "GLOBAL_FRESHEST"
            ),
        }
    aligned: List[float] = []
    stale: List[str] = []
    for sym, sign in BASKET.items():
        snap = _get_snapshot(R, sym, tf,device_id=device_id,)
        if snap is None or not _is_fresh(snap, tf):
            stale.append(sym)
            continue
        r = _pair_return(_closes(snap), lookback)
        if r is None:
            stale.append(sym)
            continue
        aligned.append(sign * r)  # sign-align to the dollar

    if len(aligned) < MIN_PAIRS:
        return {
            "tf": tf,
            "strength": None,
            "bias": "flat",
            "n_pairs": len(aligned),
            "stale_excluded": stale,
            "device_id": str(device_id or "").strip() or None,
            "source_scope": (
                "DEVICE"
                if device_id
                else "GLOBAL_FRESHEST"
            ),
        }

    strength = sum(aligned) / len(aligned)
    if strength > FLAT_BAND:
        bias = "up"
    elif strength < -FLAT_BAND:
        bias = "down"
    else:
        bias = "flat"

    return {
        "tf": tf,
        "strength": round(strength, 6),
        "bias": bias,
        "n_pairs": len(aligned),
        "stale_excluded": stale,
        "device_id": str(device_id or "").strip() or None,
        "source_scope": (
            "DEVICE"
            if device_id
            else "GLOBAL_FRESHEST"
        ),
    }


def usd_strength_all(
    R,
    device_id: str | None = None,
) -> Dict[str, Any]:
    """
    Compute synthetic USD strength on H4 and H1.

    device_id supplied:
      use only that broker/device's five USD pairs.

    device_id omitted:
      preserve current global/freshest-device behaviour.
    """

    dev = str(device_id or "").strip()

    out = {
        "ts_ms": int(time.time() * 1000),
        "usd_strength_device_id": dev or None,
        "usd_strength_source_scope": (
            "DEVICE"
            if dev
            else "GLOBAL_FRESHEST"
        ),
    }

    for tf in LOOKBACK:
        res = usd_strength_tf(
            R,
            tf,
            device_id=dev or None,
        )

        tf_l = tf.lower()

        out[f"usd_bias_{tf_l}"] = res.get(
            "bias"
        )
        out[f"usd_strength_{tf_l}"] = res.get(
            "strength"
        )
        out[f"usd_npairs_{tf_l}"] = res.get(
            "n_pairs"
        )
        out[f"usd_stale_excluded_{tf_l}"] = res.get(
            "stale_excluded"
        )

    return out


def pair_htf_trend(
    R,
    symbol: str,
    tf: str,
    device_id: str | None = None,
) -> str:
    """The traded pair's own trend on a timeframe: up/down/flat."""
    snap = _get_snapshot(
       R,
       symbol,
       tf,
       device_id=device_id,
    )
    if snap is None or not _is_fresh(snap, tf):
        return "flat"
    r = _pair_return(_closes(snap), LOOKBACK.get(tf, 30))
    if r is None:
        return "flat"
    if r > FLAT_BAND:
        return "up"
    if r < -FLAT_BAND:
        return "down"
    return "flat"


def macro_bias_for_trade(R, symbol: str, side: str,device_id: str | None = None,) -> Dict[str, Any]:
    """
    Build the full capture payload for a trade at entry.
    side: 'BUY' or 'SELL'
    Returns dict of fields to merge into the trade snapshot.
    """
    side = (side or "").upper()
    trade_dir = 1 if side == "BUY" else (-1 if side == "SELL" else 0)

    fields = usd_strength_all(
        R,
        device_id=device_id,
    )

    # pair's own trend per TF
    for tf in LOOKBACK:
        fields[f"pair_trend_{tf.lower()}"] = pair_htf_trend(
            R,
            symbol,
            tf,
            device_id=device_id,
        )

    # is USD-quote or USD-base? (for interpreting alignment on this pair)
    usd_sign = BASKET.get(symbol.upper())  # +1 base, -1 quote, None for e.g. XAUUSD

    # trade_vs_usd: does the trade profit if the dollar moves the way it's biased?
    # For a USD-quote pair (EURUSD): BUY profits if dollar WEAKENS (usd down).
    # For a USD-base pair (USDJPY): BUY profits if dollar STRENGTHENS (usd up).
    # Use H4 as the reference bias for alignment (slower = the 'macro' lean).
    ref_bias = fields.get("usd_bias_h4", "flat")
    trade_vs_usd = "n/a"
    if usd_sign is not None and trade_dir != 0 and ref_bias in ("up", "down"):
        usd_dir = 1 if ref_bias == "up" else -1
        # dollar direction that HELPS this trade:
        #   base pair  (usd_sign +1): BUY helped by usd up   -> helpful = trade_dir
        #   quote pair (usd_sign -1): BUY helped by usd down -> helpful = -trade_dir
        helpful_usd_dir = trade_dir * usd_sign
        trade_vs_usd = "aligned" if usd_dir == helpful_usd_dir else "against"
    fields["trade_vs_usd"] = trade_vs_usd

    # trade_vs_htf: is the trade with the pair's own H4 trend?
    pair_h4 = fields.get("pair_trend_h4", "flat")
    trade_vs_htf = "n/a"
    if trade_dir != 0 and pair_h4 in ("up", "down"):
        pair_dir = 1 if pair_h4 == "up" else -1
        trade_vs_htf = "aligned" if pair_dir == trade_dir else "against"
    fields["trade_vs_htf"] = trade_vs_htf

    return fields
