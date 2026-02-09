# api/pulse.py
from __future__ import annotations

from typing import Any, Dict, List, Optional
import math

# NOTE: build_pulse will be called from trend_endpoints.py
# It receives already-fetched frames and “forecast-like” fields.
# Keep it deterministic (no LLM). If later you want LLM commentary,
# add it as optional field.

FIB_PCTS = [0.236, 0.382, 0.5, 0.618, 0.786]

def _fib_levels(lo: float, hi: float) -> List[Dict[str, Any]]:
    if not (isinstance(lo, (int, float)) and isinstance(hi, (int, float))):
        return []
    if hi <= lo:
        return []
    rng = hi - lo
    out = []
    for p in FIB_PCTS:
        out.append({"pct": p, "level": float(hi - rng * p)})
    return out

def _pick_range(df, lookback: int) -> Optional[Dict[str, float]]:
    if df is None or getattr(df, "empty", True):
        return None
    try:
        tail = df.tail(int(lookback))
        hi = float(tail["h"].max())
        lo = float(tail["l"].min())
        if math.isfinite(hi) and math.isfinite(lo) and hi > lo:
            return {"hi": hi, "lo": lo}
    except Exception:
        return None
    return None

def _pulse_text(
    symbol: str,
    decision: str,
    p_up: Optional[float],
    sr: Dict[str, Any],
    fib_top: Optional[float],
    fib_mid: Optional[float],
) -> str:
    # short, deterministic, “commentary-like”
    pu = None
    try:
        pu = float(p_up) if p_up is not None else None
    except Exception:
        pu = None

    ns = sr.get("nearest_support")
    nr = sr.get("nearest_resistance")
    safety = sr.get("sr_safety") or "unknown"

    parts = []
    parts.append(f"{symbol}: {decision or '—'}" + (f" (p_up {pu:.2f})" if isinstance(pu, float) else ""))

    if ns or nr:
        parts.append(f"SR: S={ns if ns else '—'} | R={nr if nr else '—'} | safety={safety}")
    else:
        parts.append("SR: — (not enough swings / data)")

    if fib_top is not None and fib_mid is not None:
        parts.append(f"Fib (H1 range): key={fib_mid:.5f} next={fib_top:.5f}")
    return " • ".join(parts)


def _sr_zones_from_summary(sr: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Return SR zones for UI shading.

    Supports BOTH shapes:
    1) Legacy (older): sr["H1"|"H4"].zones = [{low,high,kind/side,strength,...}, ...]
    2) New unified SR engine: sr["h1"|"h4"] with supports_near/resistances_near + supports_major/resistances_major
       Each item carries {level, kind, tf, strength, touches, distance_atr, sr_score, ...}

    Output is normalized list of zones with explicit timeframe labels so UI can render H1 vs H4 clearly.
    """
    out: List[Dict[str, Any]] = []
    if not isinstance(sr, dict):
        return out

    # Prefer the new SR engine shape if present
    if isinstance(sr.get("h1"), dict) or isinstance(sr.get("h4"), dict):
        cross_buf = sr.get("cross_buf")
        try:
            band = float(cross_buf) if isinstance(cross_buf, (int, float)) else None
        except Exception:
            band = None

        def _band_around(level: float) -> tuple[float, float]:
            # If cross_buf is missing, fall back to a tiny band so it still renders.
            b = band
            if b is None or b <= 0:
                b = max(abs(level) * 0.00025, 1e-6)
            return (float(level - b), float(level + b))

        def _emit(items: list, tf: str, bucket: str):
            for r in items or []:
                if not isinstance(r, dict):
                    continue
                try:
                    lvl = float(r.get("level"))
                except Exception:
                    continue
                lo, hi = _band_around(lvl)

                kind = (r.get("kind") or r.get("side") or None)
                strength = r.get("strength")
                touches = r.get("touches")
                sr_score = r.get("sr_score")

                out.append(
                    {
                        "tf": tf,
                        "bucket": bucket,           # 'near' or 'major'
                        "low": lo,
                        "high": hi,
                        "level": lvl,
                        "kind": kind,
                        "strength": strength,
                        "zone_tap_count": touches,  # naming clarity: historical zone touches (NOT entry tap gate)
                        "sr_score": sr_score,
                        # Useful labels for UI legend/tooltips
                        "label": f"{tf}:{bucket}:{kind or 'sr'}",
                    }
                )

        for tfk, key in (("H1", "h1"), ("H4", "h4")):
            tf_obj = sr.get(key) or {}
            if not isinstance(tf_obj, dict):
                continue

            # Show H1 near (actionable) + H4 major (context) together is OK;
            # but we still emit both buckets for both TFs so UI can filter.
            _emit(tf_obj.get("supports_near"), tfk, "near")
            _emit(tf_obj.get("resistances_near"), tfk, "near")
            _emit(tf_obj.get("supports_major"), tfk, "major")
            _emit(tf_obj.get("resistances_major"), tfk, "major")

        return out

    # Fallback: legacy shape
    for tfk in ("H1", "H4"):
        tf_obj = sr.get(tfk) or {}
        if not isinstance(tf_obj, dict):
            continue
        zones = tf_obj.get("zones") or []
        if not isinstance(zones, list):
            continue

        for z in zones:
            if not isinstance(z, dict):
                continue
            lo = z.get("low")
            hi = z.get("high")
            if not (isinstance(lo, (int, float)) and isinstance(hi, (int, float))):
                continue
            out.append(
                {
                    "tf": tfk,
                    "bucket": (z.get("bucket") or "legacy"),
                    "low": float(lo),
                    "high": float(hi),
                    "level": float(z.get("level") or ((float(lo) + float(hi)) / 2.0)),
                    "kind": (z.get("kind") or z.get("side") or None),
                    "strength": z.get("strength"),
                    "zone_tap_count": z.get("zone_tap_count") or z.get("touches"),
                    "sr_score": z.get("sr_score"),
                    "label": z.get("label") or f"{tfk}:legacy:{z.get('kind') or z.get('side') or 'sr'}",
                }
            )

    return out

    for tfk in ("H1", "H4"):
        tf_obj = sr.get(tfk) or {}
        if not isinstance(tf_obj, dict):
            continue
        zones = tf_obj.get("zones") or []
        if not isinstance(zones, list):
            continue

        for z in zones:
            if not isinstance(z, dict):
                continue
            lo = z.get("low")
            hi = z.get("high")
            if not (isinstance(lo, (int, float)) and isinstance(hi, (int, float))):
                continue
            out.append(
                {
                    "tf": tfk,
                    "low": float(lo),
                    "high": float(hi),
                    "kind": (z.get("kind") or z.get("side") or None),
                    "strength": z.get("strength"),
                }
            )
    return out

def build_pulse(
    *,
    symbol: str,
    tf: str,
    price: Optional[float],
    decision: str,
    prob_up: Optional[float],
    expected_move_pct: Optional[float],
    target_price: Optional[float],
    sr_summary: Dict[str, Any],
    h1_df=None,
    h4_df=None,
) -> Dict[str, Any]:
    # Fib from H1 (fallback H4)
    rng = _pick_range(h1_df, lookback=96) or _pick_range(h4_df, lookback=200)
    fib = _fib_levels(rng["lo"], rng["hi"]) if rng else []

    # pick two “useful” fib refs for narrative
    fib_mid = None
    fib_top = None
    if fib:
        fib_mid = fib[3]["level"] if len(fib) > 3 else fib[0]["level"]
        fib_top = fib[1]["level"] if len(fib) > 1 else fib[0]["level"]

    # ---- NEW: chart overlays (SR zones + future trade lines) ----
    sr_zones = _sr_zones_from_summary(sr_summary)

    # trade overlay is optional; keep nulls if not provided (UI draws only if present)
    trade_overlay = {
        "decision": decision if decision else "WAIT",
        "entry_price": None,
        "tp_price": None,
        "sl_price": None,
        "entry_ts_ms": None,
    }

    return {
        "ok": True,
        "symbol": symbol,
        "tf": tf,
        "price": price,
        "decision": decision,
        "prob_up": prob_up,
        "expected_move_pct": expected_move_pct,
        "target_price": target_price,

        # “don’t hide anything even if off”
        "features": {
            "sr": True,
            "fib": True if fib else False,
            "commentary": True,
        },

        "sr": sr_summary,
        "fib": {
            "range": rng,
            "levels": fib,   # list[{pct, level}]
        },

        # ✅ NEW: what the UI chart should read
        "chart": {
            "overlays": {
                "sr_zones": sr_zones,     # shaded blocks
                "trade": trade_overlay,   # TP/SL/entry horizontal lines (placeholder)
            }
        },

        "pulse_text": _pulse_text(
            symbol=symbol,
            decision=decision,
            p_up=prob_up,
            sr=sr_summary,
            fib_top=fib_top,
            fib_mid=fib_mid,
        ),
    }
