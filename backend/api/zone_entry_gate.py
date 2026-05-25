
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import time
from typing import Any, Dict, List, Optional, Tuple




def _to_ms_any(x) -> int:
    try:
        if x is None:
            return 0
        if isinstance(x, (int, float)):
            v = int(x)
        else:
            v = int(float(str(x).strip()))
        # seconds -> ms
        if v > 0 and v < 10_000_000_000:
            return v * 1000
        return v
    except Exception:
        return 0


def _bar_f(b: dict, *keys: str) -> Optional[float]:
    for k in keys:
        if k in b and b.get(k) is not None:
            try:
                return float(b.get(k))
            except Exception:
                return None
    return None


def _level_to_zone(lvl: float, tf: str, sym_u: str, atr: float | None) -> dict:
    # pip_factor: XAU/JPY wider than FX majors
    pip = 0.01 if sym_u == "XAUUSD" else (0.01 if sym_u.endswith("JPY") else 0.0001)

    # half-width: use ATR if available, else pip-based fallback
    # Keep it modest: 0.10 * ATR for FX/JPY tends to be reasonable; clamp to min.
    if atr is not None and atr > 0:
        half = max(3.0 * pip, 0.10 * float(atr))
    else:
        half = 5.0 * pip

    low = float(lvl) - float(half)
    high = float(lvl) + float(half)

    return {
        "level": float(lvl),
        "low": low,
        "high": high,
        "tf": str(tf or "H1").upper(),
        "type": "ZONE_FROM_LEVEL",
        "half": float(half),
    }

def _pick_last_closed_bar_from_bars(
    bars: List[dict],
    now_ms: int,
    tf_ms: int,
) -> Tuple[Optional[dict], Optional[dict]]:
    """
    Canonical closed candle picker.

    Supports:
    - explicit close-time bars: t_close_ms / tCloseMs / t_close / tClose
    - explicit open-time bars: t_open_ms / tOpenMs / ts_ms / t / time
    - no timestamp bars: use second-last as closed
    """
    try:
        if not isinstance(bars, list) or len(bars) < 2:
            return (None, None)

        now_ms = int(now_ms or 0)
        tf_ms = int(tf_ms or 0)

        if now_ms <= 0 or tf_ms <= 0:
            return (None, None)

        bs = [b for b in bars if isinstance(b, dict)]
        if len(bs) < 2:
            return (None, None)

        def _close_ms(b: dict) -> int:
            # 1) explicit close time: do NOT add tf_ms
            for k in ("t_close_ms", "tCloseMs", "t_close", "tClose", "close_time_ms"):
                v = _to_ms_any(b.get(k))
                if v > 0:
                    return int(v)

            # 2) explicit open time: add tf_ms
            for k in ("t_open_ms", "tOpenMs", "open_time_ms", "ts_ms", "t", "time", "ts"):
                v = _to_ms_any(b.get(k))
                if v > 0:
                    return int(v + tf_ms)

            return 0

        def _sort_ms(b: dict) -> int:
            cm = _close_ms(b)
            return cm if cm > 0 else 0

        has_ts = any(_sort_ms(b) > 0 for b in bs[-5:])

        if not has_ts:
            if len(bs) < 3:
                return (None, None)
            return (bs[-2], bs[-3])

        bs.sort(key=_sort_ms)

        for i in range(len(bs) - 1, -1, -1):
            b = bs[i]

            if b.get("complete") is False:
                continue

            cm = _close_ms(b)
            if cm <= 0:
                continue

            # last closed candle only
            if cm > now_ms:
                continue

            prev = bs[i - 1] if i - 1 >= 0 else None
            return (b, prev)

        return (None, None)

    except Exception:
        return (None, None)

def _pick_level_from_lists(levels: List[Any], direction: str, cl: float) -> Optional[float]:
    vals: List[float] = []
    for x in (levels or []):
        try:
            # SR bundle levels are often dicts like {"level": 152.79, ...}
            if isinstance(x, dict):
                v = x.get("level")
            else:
                v = x
            if v is None:
                continue
            vals.append(float(v))
        except Exception:
            continue

    if not vals:
        return None

    if direction == "BUY":
        below = [v for v in vals if v <= cl]
        return max(below) if below else None
    else:
        above = [v for v in vals if v >= cl]
        return min(above) if above else None

def _pick_zone_from_sr(sr_all: dict, direction: str, cl: float, atr: float, tf_tag: str) -> dict | None:
    """
    Strong SR zone picker with quality filtering.

    Accepts BOTH shapes:
      A) full payload: {"h1": {...}, "h4": {...}, ...}
      B) TF-sliced: {"supports":[...], "supports_near":[...], "supports_major":[...], ...}

    Priority system (caps are dynamic; FX uses wider caps):
    1. H4 major (strength>=8 OR touches>=4 OR sr_score>=10) within cap_h4_major ATR
    2. H1 strong (strength>=6 OR touches>=3 OR sr_score>=9) within cap_h1_strong ATR
    3. H1 acceptable (strength>=3 OR touches>=2 OR sr_score>=6) within cap_h1_acc ATR
    4. H1 any (touches>=2) within cap_h1_min ATR

    Side rules:
    - Normal: BUY prefers supports BELOW price; SELL prefers resistances ABOVE price.
    - Reversal-watch use-case: if price has crossed the zone (BUY below support / SELL above resistance),
      allow the crossed zone as a reclaim target if the cross distance is within max_cross_atr.

    Returns zone with highest composite score, or None if no valid zone.
    """
    if not isinstance(sr_all, dict):
        return None

    dir_u = str(direction or "").upper().strip()
    if dir_u not in ("BUY", "SELL"):
        return None

    try:
        cl = float(cl)
    except Exception:
        return None

    try:
        atr = float(atr)
    except Exception:
        atr = 1.0

    if atr <= 0:
        atr = 1.0  # fallback

    tfu = str(tf_tag or "H1").upper().strip()
    tfk = tfu.lower()

    sym_u = str(sr_all.get("symbol") or "").upper().strip()
    is_fx = bool(sym_u) and (sym_u != "XAUUSD")

    cap_h4_major = 3.0
    cap_h1_strong = 2.5
    cap_h1_acc = 2.0
    cap_h1_min = 1.5

    # FX pairs: ATR is tiny; allow wider ATR distance to avoid false "no support" blocks
    if is_fx:
        cap_h4_major = 5.0
        cap_h1_strong = 4.0
        cap_h1_acc = 3.5
        cap_h1_min = 3.0

    # crossed-zone allowance (reversal-watch): tuneable
    max_cross_atr = 0.75

    # Strength thresholds
    def _is_strong_h4(z: dict) -> bool:
        return (
            int(z.get("strength") or 0) >= 8
            or int(z.get("touches") or 0) >= 4
            or float(z.get("sr_score") or 0) >= 10.0
        )

    def _is_strong_h1(z: dict) -> bool:
        return (
            int(z.get("strength") or 0) >= 6
            or int(z.get("touches") or 0) >= 3
            or float(z.get("sr_score") or 0) >= 9.0
        )

    def _is_acceptable_h1(z: dict) -> bool:
        return (
            int(z.get("strength") or 0) >= 3
            or int(z.get("touches") or 0) >= 2
            or float(z.get("sr_score") or 0) >= 6.0
        )

    def _is_minimum(z: dict) -> bool:
        return int(z.get("touches") or 0) >= 2

    def _composite_score(z: dict, dist_atr: float, tf: str) -> float:
        touches = int(z.get("touches") or 0)
        strength = int(z.get("strength") or 0)
        sr_score = float(z.get("sr_score") or 0)
        tf_bonus = 10.0 if tf == "H4" else 5.0
        return touches * 2.0 + strength * 1.5 + sr_score * 1.0 + tf_bonus - dist_atr * 2.0

    def _get_levels(sr_tf: dict, key: str) -> list:
        v = sr_tf.get(key)
        return v if isinstance(v, list) else []

    def _cross_ok(lvl: float) -> bool:
       """
       SIMPLE EXECUTION MODEL

       BUY:
          support must be BELOW or near current price

       SELL:
          resistance must be ABOVE or near current price

       No reclaim logic.
       """

       if dir_u == "BUY":
           return float(lvl) <= float(cl)

       return float(lvl) >= float(cl)

    # --- Accept TF-sliced SR directly ---
    is_tf_sliced = any(
        k in sr_all
        for k in (
            "supports",
            "resistances",
            "supports_near",
            "resistances_near",
            "supports_major",
            "resistances_major",
        )
    )

    if is_tf_sliced:
        h1 = sr_all
        h4 = {}
    else:
        # prefer tf_tag bucket if present; fallback to h1
        h1 = sr_all.get(tfk) if isinstance(sr_all.get(tfk), dict) else {}
        if not h1 and isinstance(sr_all.get("h1"), dict):
            h1 = sr_all.get("h1") or {}
        h4 = sr_all.get("h4") if isinstance(sr_all.get("h4"), dict) else {}

    # ------------------------------------------------------------
    # PHASE 1: Build BOTH major zones: H1 primary + H4 fallback
    # ------------------------------------------------------------
    # Final SR model:
    #   BUY  -> H1 Major Demand first, then H4 Major Demand
    #   SELL -> H1 Major Supply first, then H4 Major Supply
    # No H1 normal execution here. H1/H4 major zones are kept visible
    # so later phases can track: H1 missed -> continue watching H4.

    major_key = "supports_major" if dir_u == "BUY" else "resistances_major"

    def _ensure_zone_band(zone: dict, tf_for_band: str) -> dict:
        """Return a copied zone with guaranteed low/high band."""
        z = dict(zone or {})
        if not isinstance(z, dict) or z.get("level") is None:
            return z
        try:
            zl = z.get("low")
            zh = z.get("high")
            if zl is None or zh is None or float(zl) >= float(zh):
                ztmp = _level_to_zone(
                    float(z["level"]),
                    str(tf_for_band or z.get("tf") or tfu).upper(),
                    str(sr_all.get("symbol") or "").upper().strip() or "XAUUSD",
                    float(atr),
                )
                z["low"] = float(ztmp["low"])
                z["high"] = float(ztmp["high"])
                z["half"] = float(ztmp.get("half") or (abs(z["high"] - z["low"]) / 2.0))
        except Exception:
            pass
        return z

    def _pick_best_major(rows: list, tf_name: str) -> tuple[dict | None, dict]:
        """
        Pick one best major zone for a timeframe.
        Important: this intentionally does NOT reject a valid major zone just
        because it is far. Distance guard later decides WAIT_ZONE_TOUCH.
        """
        scored = []
        for z in rows or []:
            if not isinstance(z, dict):
                continue
            if z.get("side_ok") is False:
                continue
            if z.get("stale") is True:
                continue
            try:
                lvl = float(z.get("level"))
            except Exception:
                continue

            # Keep simple side rule only: BUY support at/below price,
            # SELL resistance at/above price. No reclaim/cross execution.
            if not _cross_ok(lvl):
                continue

            dist_atr = abs(float(cl) - float(lvl)) / float(atr)
            zz = _ensure_zone_band(z, tf_name)
            zz["tf"] = str(tf_name).upper()
            zz["kind"] = "support" if dir_u == "BUY" else "resistance"
            zz["zone_role"] = "H1_PRIMARY" if str(tf_name).upper() == "H1" else "H4_FALLBACK"
            zz["dist_atr"] = float(dist_atr)
            zz["distance"] = float(abs(float(cl) - float(lvl)))
            scored.append({
                "zone": zz,
                "score": _composite_score(zz, dist_atr, str(tf_name).upper()),
                "dist_atr": float(dist_atr),
            })

        if not scored:
            return None, {"count": 0}

        # Prefer high quality, then nearer zone.
        scored.sort(key=lambda x: (-float(x.get("score") or 0.0), float(x.get("dist_atr") or 1e9)))
        best = scored[0]["zone"]
        return best, {
            "count": int(len(scored)),
            "best_level": float(best.get("level")),
            "best_dist_atr": float(best.get("dist_atr") or 0.0),
        }

    h1_major_zone, h1_dbg = _pick_best_major(_get_levels(h1, major_key), "H1")
    h4_major_zone, h4_dbg = _pick_best_major(_get_levels(h4, major_key), "H4")

    # Active zone remains H1-first for Phase 1. If H1 major is absent, use H4.
    # Phase 2 will use h1_major_zone/h4_major_zone to implement H1_MISSED -> WATCH_H4.
    zone = h1_major_zone if isinstance(h1_major_zone, dict) else h4_major_zone

    if not isinstance(zone, dict) or zone.get("level") is None:
        return None

    zone = dict(zone)
    zone["selection_model"] = "H1_MAJOR_THEN_H4_MAJOR_PHASE1"
    zone["h1_major_zone"] = h1_major_zone if isinstance(h1_major_zone, dict) else None
    zone["h4_major_zone"] = h4_major_zone if isinstance(h4_major_zone, dict) else None
    zone["primary_zone"] = h1_major_zone if isinstance(h1_major_zone, dict) else None
    zone["secondary_zone"] = h4_major_zone if isinstance(h4_major_zone, dict) else None
    zone["zone_stage"] = "H1" if isinstance(h1_major_zone, dict) else "H4"
    zone["zone_pair_debug"] = {"h1": h1_dbg, "h4": h4_dbg}

    return zone

def _nearest_levels_from_sr(
    sr_all: dict,
    price: float,
    atr: float,
    *,
    pip_factor: float = 0.01,
    cross_buf: float = 0.0,
) -> dict:
    """
    Compute nearest_support / nearest_resistance using CURRENT price.
    Major-first (H4+H1), then Near, then All.
    Side-aware with soft buffer to tolerate sweep/liquidity wick.
    """
    out = {
        "nearest_support": None,
        "nearest_resistance": None,
        "buf": None,
        "src_support": None,
        "src_resistance": None,
    }

    if not isinstance(sr_all, dict):
        return out

    try:
        px = float(price)
    except Exception:
        return out

    try:
        atr = float(atr)
    except Exception:
        atr = 0.0

    try:
        pip_factor = float(pip_factor)
    except Exception:
        pip_factor = 0.01

    try:
        cross_buf = float(cross_buf)
    except Exception:
        cross_buf = 0.0

    # sweep tolerance (price units)
    try:
        buf = max(cross_buf, 0.10 * (atr or 0.0), 5.0 * (pip_factor or 0.0))
    except Exception:
        buf = cross_buf or 0.0

    out["buf"] = float(buf)

    px_for_support = px + buf  # allow support slightly ABOVE px
    px_for_resist  = px - buf  # allow resistance slightly BELOW px

    def _tf_bucket(sr: dict, key: str) -> list:
        v = sr.get(key)
        return v if isinstance(v, list) else []

    # Accept both shapes:
    # A) full: {"h1":{...}, "h4":{...}, ...}
    # B) tf-sliced: {"supports":[...], "supports_major":[...], ...}
    is_tf_sliced = any(
        k in sr_all
        for k in ("supports", "resistances", "supports_near", "resistances_near", "supports_major", "resistances_major")
    )

    if is_tf_sliced:
        h1 = sr_all
        h4 = {}
    else:
        h1 = sr_all.get("h1") if isinstance(sr_all.get("h1"), dict) else {}
        h4 = sr_all.get("h4") if isinstance(sr_all.get("h4"), dict) else {}

    def _nearest_support_from(rows: list) -> float | None:
        vals = []
        for r in rows or []:
            if not isinstance(r, dict):
                continue
            try:
                lvl = float(r.get("level"))
            except Exception:
                continue
            if lvl <= px_for_support:
                vals.append(lvl)
        return max(vals) if vals else None

    def _nearest_res_from(rows: list) -> float | None:
        vals = []
        for r in rows or []:
            if not isinstance(r, dict):
                continue
            try:
                lvl = float(r.get("level"))
            except Exception:
                continue
            if lvl >= px_for_resist:
                vals.append(lvl)
        return min(vals) if vals else None

    # Major-first
    major_supp = _tf_bucket(h4, "supports_major") + _tf_bucket(h1, "supports_major")
    major_res  = _tf_bucket(h4, "resistances_major") + _tf_bucket(h1, "resistances_major")
    ns = _nearest_support_from(major_supp)
    nr = _nearest_res_from(major_res)
    if ns is not None:
        out["nearest_support"] = float(ns)
        out["src_support"] = "major"
    if nr is not None:
        out["nearest_resistance"] = float(nr)
        out["src_resistance"] = "major"

    # Near fallback
    if out["nearest_support"] is None:
        near_supp = _tf_bucket(h4, "supports_near") + _tf_bucket(h1, "supports_near")
        ns = _nearest_support_from(near_supp)
        if ns is not None:
            out["nearest_support"] = float(ns)
            out["src_support"] = "near"

    if out["nearest_resistance"] is None:
        near_res = _tf_bucket(h4, "resistances_near") + _tf_bucket(h1, "resistances_near")
        nr = _nearest_res_from(near_res)
        if nr is not None:
            out["nearest_resistance"] = float(nr)
            out["src_resistance"] = "near"

    # All fallback
    if out["nearest_support"] is None:
        all_supp = _tf_bucket(h4, "supports") + _tf_bucket(h1, "supports")
        ns = _nearest_support_from(all_supp)
        if ns is not None:
            out["nearest_support"] = float(ns)
            out["src_support"] = "all"

    if out["nearest_resistance"] is None:
        all_res = _tf_bucket(h4, "resistances") + _tf_bucket(h1, "resistances")
        nr = _nearest_res_from(all_res)
        if nr is not None:
            out["nearest_resistance"] = float(nr)
            out["src_resistance"] = "all"

    return out

def _pick_display_zones_from_sr(sr_all: dict, price: float, atr: float, tf_tag: str, sym_u: str) -> dict:
    """
    Display-only zones:
    - next valid H1/H4 support below/near price
    - next valid H1/H4 resistance above/near price
    - ignores stale / side_ok false
    """
    out = {
        "h1_buy_zone": None,
        "h4_buy_zone": None,
        "h1_sell_zone": None,
        "h4_sell_zone": None,
    }

    if not isinstance(sr_all, dict):
        return out

    h1 = sr_all.get("h1") if isinstance(sr_all.get("h1"), dict) else {}
    h4 = sr_all.get("h4") if isinstance(sr_all.get("h4"), dict) else {}

    def _valid(z):
        return (
            isinstance(z, dict)
            and z.get("stale") is not True
            and z.get("side_ok") is not False
            and z.get("level") is not None
        )

    def _band(z, tf):
        zz = dict(z)
        try:
            if zz.get("low") is None or zz.get("high") is None or float(zz["low"]) >= float(zz["high"]):
                ztmp = _level_to_zone(float(zz["level"]), tf, sym_u, atr)
                zz["low"] = ztmp["low"]
                zz["high"] = ztmp["high"]
        except Exception:
            pass
        zz["tf"] = tf
        return zz

    def _support(tf_obj, tf):
        rows = [z for z in (tf_obj.get("supports_major") or []) if _valid(z)]

        # Recalculate live-side validity. Do not trust cached side_ok.
        rows = [
            z for z in rows
            if float(z["level"]) <= float(price)
        ]
        rows.sort(key=lambda z: abs(float(price) - float(z["level"])))
        return _band(rows[0], tf) if rows else None

    def _resistance(tf_obj, tf):
        rows = [z for z in (tf_obj.get("resistances_major") or []) if _valid(z)]

        # Recalculate live-side validity. Do not trust cached side_ok.
        rows = [
            z for z in rows
            if float(z["level"]) >= float(price)
        ]
        rows.sort(key=lambda z: abs(float(price) - float(z["level"])))
        return _band(rows[0], tf) if rows else None

    out["h1_buy_zone"] = _support(h1, "H1")
    out["h4_buy_zone"] = _support(h4, "H4")
    out["h1_sell_zone"] = _resistance(h1, "H1")
    out["h4_sell_zone"] = _resistance(h4, "H4")
    out["h1_buy_status"] = "VALID" if out["h1_buy_zone"] else "NO_SUPPORT_BELOW_PRICE"
    out["h4_buy_status"] = "VALID" if out["h4_buy_zone"] else "NO_SUPPORT_BELOW_PRICE"
    out["h1_sell_status"] = "VALID" if out["h1_sell_zone"] else "NO_RESISTANCE_ABOVE_PRICE"
    out["h4_sell_status"] = "VALID" if out["h4_sell_zone"] else "NO_RESISTANCE_ABOVE_PRICE"

    return out

def _watch_key(sym: str, direction: str, tf_tag: str) -> str:
    return f"xtl:zone:watch:{(sym or '').upper().strip()}:{(direction or '').upper().strip()}:{(tf_tag or 'H1').upper().strip()}"

def _json_load(raw):
    try:
        if raw is None:
            return None
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", "ignore")
        raw = str(raw).strip()
        if not raw:
            return None
        import json
        return json.loads(raw)
    except Exception:
        return None

def _f(v):
    try:
        vv = float(v)
        return vv if vv > 0 else None
    except Exception:
        return None

def _bar_get(b: dict, *keys):
    # alias to existing bar reader if present
    try:
        return _bar_f(b, *keys)
    except Exception:
        try:
            for k in keys:
                if k in b and b.get(k) is not None:
                    return float(b.get(k))
        except Exception:
            pass
    return None

def _atr14_from_bars(bars: list) -> float | None:
    """
    Compute ATR(14) from bars list with keys o/h/l/c (or open/high/low/close).
    Returns ATR in price units.
    """
    try:
        if not isinstance(bars, list) or len(bars) < 20:
            return None

        trs = []
        prev_close = None

        for b in bars[-60:]:  # enough history; keep it light
            if not isinstance(b, dict):
                continue
            h = _bar_get(b, "h", "high")
            l = _bar_get(b, "l", "low")
            c = _bar_get(b, "c", "close")
            if h is None or l is None or c is None:
                continue

            if prev_close is None:
                tr = float(h - l)
            else:
                tr = max(float(h - l), abs(float(h - prev_close)), abs(float(l - prev_close)))

            trs.append(tr)
            prev_close = float(c)

        if len(trs) < 15:
            return None

        # ATR14 as simple moving average of last 14 TRs
        w = trs[-14:]
        atr = sum(w) / float(len(w))
        if atr <= 0:
            return None
        return float(atr)
    except Exception:
        return None


def zone_reversal_gate(
    *,
    R,
    sym: str,
    direction: str,
    row_h1: dict,
    sr: dict,
    now_ms: int,
    tf_tag: str = "H1",
    pinned_device: str | None = None,
    x_device_id: str | None = None,
    live_px: float | None = None,
    debug_gate: bool = False,
    move_away_atr: float = 2.0,
    hard_close_bars: int = 2,
    **_kwargs,
    
) -> Tuple[bool, dict]:
    """
    Zone-only entry gate (single writer).

    Returns: (allowed, gate_meta)
    allowed=True only when READY_REV_OK (reclaim confirmed).
    """
    sym_u = (sym or "").upper().strip()
    dir_u = (direction or "").upper().strip()
    tfu = (tf_tag or "H1").upper()

    gate: Dict[str, Any] = {"blocked": True, "reason": "unknown", "tf": tfu}
    now_ms_pick = int(now_ms)

    
    # 0) bars (prefer attached; else fetch from Redis snap using device)
    bars = None
    try:
        bars = (row_h1 or {}).get("bars")
    except Exception:
        bars = None

    
    # If missing bars, pull from Redis snap: xtl:ohlc:snap:<DEV>:<SYM>:H1
    if not isinstance(bars, list) or len(bars) < 2:
        dev = (str(x_device_id or "").strip() or str(pinned_device or "").strip())
        if dev:
            try:
                k = f"xtl:ohlc:snap:{dev}:{sym_u}:{tfu}"
                raw = R.get(k) if R is not None else None
                js = _json_load(raw)
                b2 = js.get("bars") if isinstance(js, dict) else None
                if not isinstance(b2, list):
                    b2 = js.get("ohlc") if isinstance(js, dict) else None

                if isinstance(b2, list) and len(b2) >= 2:
                    bars = b2

                    # --- FIX: use snap clock for "closed bar" logic (defensive) ---
                    snap_last_closed = 0
                    snap_server_now = 0
                    try:
                        snap_last_closed = int(js.get("lastClosedTs") or 0) if isinstance(js, dict) else 0
                    except Exception:
                        snap_last_closed = 0
                    try:
                        snap_server_now = int(js.get("serverNow") or 0) if isinstance(js, dict) else 0
                    except Exception:
                        snap_server_now = 0

                    # lastClosedTs must not be ahead of serverNow by a large margin
                    # IMPORTANT:
                    # Use serverNow/current clock to decide which candle is closed.
                    # Do NOT use lastClosedTs as now_ms_pick, otherwise picker can lag by 1-2 candles.
                    if snap_server_now > 0:
                        now_ms_pick = int(snap_server_now)
                        snap_repaired = False
                    else:
                        now_ms_pick = int(now_ms)

                    # lastClosedTs is debug/reference only
                    if snap_last_closed > 0 and snap_server_now > 0 and snap_last_closed > (snap_server_now + 120_000):
                        snap_repaired = True

                    if debug_gate:
                        gate["dbg_h1_bars_src"] = "dev_snap"
                        gate["dbg_h1_snap_key"] = k
                        gate["dbg_h1_bars_n"] = int(len(bars))
                        gate["dbg_h1_snap_serverNow"] = (js.get("serverNow") if isinstance(js, dict) else None)
                        gate["dbg_lastClosedTs"] = (js.get("lastClosedTs") if isinstance(js, dict) else None)
                        gate["dbg_h1_snap_clock_delta_ms"] = int((snap_last_closed or 0) - (snap_server_now or 0))
                        gate["dbg_h1_snap_clock_repaired"] = bool(snap_repaired)
                else:
                    if debug_gate:
                        gate["dbg_h1_bars_src"] = "dev_snap_empty"
                        gate["dbg_h1_snap_key"] = k
            except Exception as e:
                if debug_gate:
                    gate["dbg_h1_bars_src"] = "dev_snap_exc"
                    gate["dbg_h1_bars_exc_type"] = type(e).__name__
                    gate["dbg_h1_bars_exc"] = str(e)

    if not isinstance(bars, list) or not bars:
        gate["reason"] = "no_h1_bars"
        gate["stage"] = "H1_BARS"
        return False, gate


    # 0B) last closed bar - CRITICAL FIX: Always use tuple unpacking safely
    tf_ms = {
        "M15": 15 * 60 * 1000,
        "H1": 60 * 60 * 1000,
        "H4": 4 * 60 * 60 * 1000,
    }.get(str(tfu).upper(), 60 * 60 * 1000)
    c, p = (None, None)  # Default to tuple
    try:
        result = _pick_last_closed_bar_from_bars(bars, int(now_ms_pick), int(tf_ms))
        if result is not None and isinstance(result, tuple) and len(result) == 2:
            c, p = result
        else:
            c, p = (None, None)
    except Exception as e:
        if debug_gate:
            gate["dbg_pick_bar_exc"] = f"{type(e).__name__}:{e}"
        c, p = (None, None)
    
    if not isinstance(c, dict):
        gate["reason"] = "no_h1_closed_bar"
        gate["stage"] = "H1_PICK"
        gate["bars_n"] = int(len(bars) if isinstance(bars, list) else 0)
        return False, gate

    if debug_gate:
        try:
            t_open_ms_dbg = _to_ms_any(c.get("t_open_ms") or c.get("ts_ms") or c.get("t"))
        except Exception:
            t_open_ms_dbg = 0
        try:
            t_close_ms_dbg = _to_ms_any(c.get("t_close_ms") or c.get("tCloseMs") or c.get("t_close"))
        except Exception:
            t_close_ms_dbg = 0
        if not t_close_ms_dbg and t_open_ms_dbg:
            t_close_ms_dbg = int(t_open_ms_dbg + int(tf_ms))

        gate["dbg_now_ms_pick"] = int(now_ms_pick)
        gate["dbg_pick_bar_start_ms"] = int(t_open_ms_dbg)
        gate["dbg_pick_bar_close_ms"] = int(t_close_ms_dbg)
        gate["dbg_pick_bar_cl"] = float(_bar_f(c, "c", "close") or 0.0)

    # 0C) compute closed_ms for the selected closed bar (needed for watch started_ms)
    closed_ms = 0
    try:
        closed_ms = _to_ms_any(c.get("t_close_ms") or c.get("tCloseMs") or c.get("t_close"))
        if not closed_ms:
            t_open_ms = _to_ms_any(c.get("t_open_ms") or c.get("ts_ms") or c.get("t"))
            closed_ms = int(t_open_ms + tf_ms) if t_open_ms else 0
    except Exception:
        closed_ms = 0


    cl = _bar_f(c, "c", "close")
    lo = _bar_f(c, "l", "low")
    hi = _bar_f(c, "h", "high")

    gate["picked_closed_bar"] = {
        "tf": str(tfu),
        "tf_ms": int(tf_ms),
        "closed_ms": int(closed_ms),
        "open": float(_bar_f(c, "o", "open") or 0),
        "high": float(hi or 0),
        "low": float(lo or 0),
        "close": float(cl or 0),
        "now_ms_pick": int(now_ms_pick),
    }
    if cl is None or lo is None or hi is None:
        gate["reason"] = "bad_h1_bar"
        gate["stage"] = "H1_OHLC"
        return False, gate
    # ------------------------------------------------------------
    # Use LIVE price for SR direction + zone selection
    # ------------------------------------------------------------
    try:
        decision_px = (
            float(live_px)
            if live_px is not None and float(live_px) > 0
            else float(cl)
        )
    except Exception:
        decision_px = float(cl)

    gate["decision_px"] = float(decision_px)
    gate["h1_closed_cl"] = float(cl)

    # ATR: prefer provided/row value; fallback compute from bars
    atr = None
    try:
        # prefer explicit
        atr = _f((row_h1 or {}).get("atr"))
    except Exception:
        atr = None

    if atr is None:
        try:
            # sometimes row has atr_h1 or similar name
            atr = _f((row_h1 or {}).get("atr_h1"))
        except Exception:
            atr = None

    if atr is None:
        atr = _atr14_from_bars(bars)

    if atr is None:
        gate["reason"] = "no_atr"
        gate["stage"] = "ATR"
        gate["blocked"] = True
        return False, gate

    if debug_gate:
        gate["dbg_atr_src"] = "bars_atr14" if (row_h1 or {}).get("atr") is None else "row"
        gate["atr"] = float(atr)
    # Price-aware nearest SR (DO NOT trust cached sr.nearest_*)
    try:
        pip_factor = float((sr or {}).get("pip_factor") or 0.01)
    except Exception:
        pip_factor = 0.01
    try:
        cross_buf = float((sr or {}).get("cross_buf") or 0.0)
    except Exception:
        cross_buf = 0.0

    nearest = _nearest_levels_from_sr(
        sr or {},
        float(decision_px),
        float(atr),
        pip_factor=pip_factor,
        cross_buf=cross_buf,
    )
    if debug_gate:
        gate["dbg_nearest_sr"] = nearest

    display_zones = _pick_display_zones_from_sr(
        sr or {},
        float(decision_px),
        float(atr),
        tfu,
        sym_u,
    )
    def _zone_band_dist_local(z: dict, px: float) -> float | None:
        try:
            if not isinstance(z, dict):
                return None
            zl = float(z.get("low") if z.get("low") is not None else z.get("level"))
            zh = float(z.get("high") if z.get("high") is not None else z.get("level"))
            if zl > zh:
                zl, zh = zh, zl
            px = float(px)
            if zl <= px <= zh:
                return 0.0
            if px < zl:
                return float(zl - px)
            return float(px - zh)
        except Exception:
            return None

    def _actionable_cap_local(sym_u: str, atr: float) -> float:
        s = str(sym_u or "").upper().strip()
        base = float(move_away_atr) * float(atr)

        if s == "XAUUSD":
           return min(max(base, 3.0), 12.0)
        if s.endswith("JPY"):
           return min(max(base, 0.08), 0.25)
        return min(max(base, 0.0008), 0.0025)

    gate["display_zones"] = display_zones
    gate["h1_buy_zone"] = display_zones.get("h1_buy_zone")
    gate["h4_buy_zone"] = display_zones.get("h4_buy_zone")
    gate["h1_sell_zone"] = display_zones.get("h1_sell_zone")
    gate["h4_sell_zone"] = display_zones.get("h4_sell_zone")
    gate["h1_buy_status"] = display_zones.get("h1_buy_status")
    gate["h4_buy_status"] = display_zones.get("h4_buy_status")
    gate["h1_sell_status"] = display_zones.get("h1_sell_status")
    gate["h4_sell_status"] = display_zones.get("h4_sell_status")

    

    # ------------------------------------------------------------
    # Direction resolver from nearest ACTIONABLE zone band
    # BUY  = price near/inside support zone
    # SELL = price near/inside resistance zone
    # WATCHING = no nearby actionable zone
    # ------------------------------------------------------------
    resolved_dir = "WATCHING"
    preferred_zone = None

    def _zone_band_dist(z: dict, px: float) -> float | None:
        try:
            if not isinstance(z, dict):
                return None
            zl = float(z.get("low") if z.get("low") is not None else z.get("level"))
            zh = float(z.get("high") if z.get("high") is not None else z.get("level"))
            if zl > zh:
                zl, zh = zh, zl

            px = float(px)

            if zl <= px <= zh:
                return 0.0
            if px < zl:
                return float(zl - px)
            return float(px - zh)
        except Exception:
            return None

    def _actionable_cap(sym_u: str, atr: float) -> float:
        base = float(move_away_atr) * float(atr)
        s = str(sym_u or "").upper().strip()

        if s == "XAUUSD":
            floor = 6.0
            ceiling = 12.0
        elif s.endswith("JPY"):
            floor = 0.15
            ceiling = 0.25
        else:
            floor = 0.0015
            ceiling = 0.0025

        return min(max(base, floor), ceiling)

    try:
        cap = _actionable_cap(sym_u, float(atr))

        candidates = []

        for side, z in (
            ("BUY", display_zones.get("h1_buy_zone")),
            ("BUY", display_zones.get("h4_buy_zone")),
            ("SELL", display_zones.get("h1_sell_zone")),
            ("SELL", display_zones.get("h4_sell_zone")),
        ):
            if not isinstance(z, dict):
                continue

            d0 = _zone_band_dist(z, float(decision_px))
            if d0 is None:
                continue

            zz = dict(z)
            zz["kind"] = "support" if side == "BUY" else "resistance"
            zz["actionable_dist"] = float(d0)

            candidates.append({
                "side": side,
                "zone": zz,
                "dist": float(d0),
                "tf_rank": 0 if str(zz.get("tf") or "").upper() == "H1" else 1,
            })

        candidates.sort(key=lambda x: (float(x["dist"]), int(x["tf_rank"])))

        if candidates and float(candidates[0]["dist"]) <= float(cap):
            resolved_dir = candidates[0]["side"]
            preferred_zone = candidates[0]["zone"]
        else:
            resolved_dir = "WATCHING"

        gate["resolved_dir"] = resolved_dir
        gate["dir_input"] = dir_u
        gate["actionable_cap"] = float(cap)
        gate["direction_model"] = "NEAREST_ACTIONABLE_ZONE_BAND"
        gate["actionable_candidates"] = [
            {
                "side": x["side"],
                "tf": x["zone"].get("tf"),
                "level": x["zone"].get("level"),
                "low": x["zone"].get("low"),
                "high": x["zone"].get("high"),
                "dist": x["dist"],
            }
            for x in candidates[:4]
        ]

    except Exception as e:
        resolved_dir = "WATCHING"
        if debug_gate:
            gate["dbg_direction_resolver_exc"] = f"{type(e).__name__}:{e}"


    
    # ------------------------------------------------------------
    # 1) load existing watch FIRST
    # IMPORTANT:
    # Do NOT trust freshly resolved_dir if a frozen BUY/SELL watch already exists.
    # Price may temporarily close beyond zone_low/zone_high and make resolved_dir=WATCHING.
    # Frozen watch must remain active until:
    #   - entry triggers
    #   - OR 2 consecutive invalidation closes happen
    # ------------------------------------------------------------
    watch = None
    zone_used = None
    wkey = None

    def _load_watch_for_dir(d: str):
        try:
            k = _watch_key(sym_u, d, tfu)
            raw = R.get(k)
            if not raw:
                return None, k
            if isinstance(raw, (bytes, bytearray)):
                raw = raw.decode("utf-8", "ignore")
            obj = json.loads(raw) if isinstance(raw, str) else raw
            return (obj if isinstance(obj, dict) else None), k
        except Exception:
            return None, _watch_key(sym_u, d, tfu)

    # Prefer active frozen watch over newly resolved direction
    for d0 in ("BUY", "SELL"):
        w0, k0 = _load_watch_for_dir(d0)
        if isinstance(w0, dict) and isinstance(w0.get("zone_used"), dict):
            watch = w0
            wkey = k0
            zone_used = w0.get("zone_used")
            resolved_dir = str(w0.get("direction") or d0).upper()
            gate["resolved_dir"] = resolved_dir
            gate["watch_key"] = str(wkey)
            gate["watch_reused"] = True
            break

    # If no frozen watch exists, use newly resolved direction
    if watch is None:
        wkey = _watch_key(sym_u, resolved_dir, tfu)
        try:
            raw = R.get(wkey)
            if raw:
                if isinstance(raw, (bytes, bytearray)):
                    raw = raw.decode("utf-8", "ignore")
                watch = json.loads(raw) if isinstance(raw, str) else raw
        except Exception:
            watch = None

        if isinstance(watch, dict) and isinstance(watch.get("zone_used"), dict):
            zone_used = watch.get("zone_used")


    # Normalize frozen zone_used: old watches may be level-only.
    # Priority: rehydrate from SR -> else synthesize via _level_to_zone (never collapse).
    if isinstance(zone_used, dict):
        # parse level once
        try:
            lvl0 = float(zone_used.get("level")) if zone_used.get("level") is not None else None
        except Exception:
            lvl0 = None

        def _rehydrate_band_from_sr_level(sr_all: dict, tf_tag: str, kind: str, lvl: float):
            if not isinstance(sr_all, dict) or lvl is None:
                return None

            tfk = str(tf_tag or "H1").lower()
            tf_obj = sr_all.get(tfk) if isinstance(sr_all.get(tfk), dict) else None
            if not isinstance(tf_obj, dict):
                return None

            if kind == "support":
                cand = (tf_obj.get("supports") or []) + (tf_obj.get("supports_major") or []) + (tf_obj.get("supports_near") or [])
            else:
                cand = (tf_obj.get("resistances") or []) + (tf_obj.get("resistances_major") or []) + (tf_obj.get("resistances_near") or [])

            best = None
            best_d = 1e18
            for r in cand:
                if not isinstance(r, dict):
                    continue
                try:
                    lv = float(r.get("level"))
                    d = abs(lv - float(lvl))
                except Exception:
                    continue
                if d < best_d:
                    best_d = d
                    best = r

            if isinstance(best, dict):
                lo = best.get("low")
                hi = best.get("high")
                try:
                    if lo is not None and hi is not None and float(lo) < float(hi):
                        return float(lo), float(hi)
                except Exception:
                    return None
            return None

        # do we need a band repair?
        need_band = False
        try:
            zl = zone_used.get("low")
            zh = zone_used.get("high")
            if zl is None or zh is None or float(zl) >= float(zh):
                need_band = True
        except Exception:
            need_band = True

        # 1) try rehydrate from SR
        band = None
        if need_band and lvl0 is not None:
            kind0 = str(zone_used.get("kind") or ("support" if resolved_dir == "BUY" else "resistance")).lower()
            band = _rehydrate_band_from_sr_level(sr or {}, tfu, kind0, float(lvl0))
            if band is not None:
                zone_used["low"] = float(band[0])
                zone_used["high"] = float(band[1])

        # 2) if still missing/collapsed, synthesize (never collapse)
        try:
            zl = zone_used.get("low")
            zh = zone_used.get("high")
            if lvl0 is not None and (zl is None or zh is None or float(zl) >= float(zh)):
                ztmp = _level_to_zone(float(lvl0), tfu, sym_u, float(atr))
                zone_used["low"] = float(ztmp["low"])
                zone_used["high"] = float(ztmp["high"])
        except Exception:
            pass



    # ------------------------------------------------------------
    # 2) if watch exists -> freeze zone_used (DO NOT re-pick zone)
    # ------------------------------------------------------------
    if isinstance(zone_used, dict) and zone_used.get("level") is not None:
        zone = dict(zone_used)
        gate["zone"] = dict(zone_used)
        gate["zone_used"] = dict(zone_used)
    else:
        zone = None
    
    # Persist repaired band ONLY when we truly repaired a legacy watch (once)
    try:
        did_repair = False

        # only if watch existed and had zone_used originally
        if isinstance(watch, dict) and isinstance(watch.get("zone_used"), dict) and isinstance(zone_used, dict):
            old = watch.get("zone_used") or {}

            old_lo = old.get("low")
            old_hi = old.get("high")

            # legacy = missing or collapsed
            legacy = False
            try:
                if old_lo is None or old_hi is None or float(old_lo) >= float(old_hi):
                    legacy = True
            except Exception:
                legacy = True

            # new = real band
            new_ok = False
            try:
                zl = zone_used.get("low")
                zh = zone_used.get("high")
                if zl is not None and zh is not None and float(zl) < float(zh):
                    new_ok = True
            except Exception:
                new_ok = False

            if legacy and new_ok:
                watch["zone_used"] = dict(zone_used)
                R.set(wkey, json.dumps(watch, separators=(",", ":")))
                did_repair = True

        if debug_gate:
            gate["dbg_watch_band_repaired"] = bool(did_repair)
    except Exception:
        pass


    
    
    
    # ------------------------------------------------------------
    # 3) pick zone from SR ONLY if no frozen zone
    # ------------------------------------------------------------
    # If watch exists with frozen zone_used, DO NOT re-pick (prevents moving-zone + tap drift)
    if isinstance(watch, dict) and isinstance(watch.get("zone_used"), dict) and watch.get("zone_used", {}).get("level") is not None:
        zone = dict(watch["zone_used"])
    else:
        # help SR picker know the symbol (for FX cap widening)
        if isinstance(sr, dict) and "symbol" not in sr:
            try:
                sr["symbol"] = sym_u
            except Exception:
                pass
        if resolved_dir == "WATCHING":
            gate["reason"] = "WATCHING_NO_NEAR_MAJOR_SR"
            gate["stage"] = "DIRECTION_RESOLVE"
            gate["blocked"] = False

            # show next valid zones, but do not trade
            gate["zone"] = (
                display_zones.get("h1_buy_zone")
                or display_zones.get("h4_buy_zone")
                or display_zones.get("h1_sell_zone")
                or display_zones.get("h4_sell_zone")
            )
            gate["planned_zone"] = gate["zone"]
            gate["zone_used"] = None
            return False, gate

        if isinstance(preferred_zone, dict) and preferred_zone.get("level") is not None:
            zone = dict(preferred_zone)
        else:
            zone = _pick_zone_from_sr(
              sr or {},
              resolved_dir,
              float(decision_px),
              float(atr),
              tfu,
            )

    if not isinstance(zone, dict) or zone.get("level") is None:
        # If a watch exists, never hard-fail with no_buy/no_sell.
        # Continue WATCH; rely on frozen zone (or wait for SR refresh) and invalidation rules.
        if isinstance(watch, dict):
            gate["reason"] = "WATCH_ZONE_MISSING"
            gate["stage"] = "ZONE_PICK"
            gate["blocked"] = False
            gate["zone"] = None
            gate["zone_used"] = (watch.get("zone_used") if isinstance(watch.get("zone_used"), dict) else None)
            gate["rev_state"] = watch
            return False, gate

        # SR exists but zone selection failed (often because nearest zone is too far by filters).
        # Prefer reporting distance to nearest SR instead of "no_buy_support_below_price".
        nearest = None
        try:
            sym_u2 = str(sym_u or "").upper().strip()
            pip_factor = 0.01 if sym_u2 == "XAUUSD" else (0.01 if sym_u2.endswith("JPY") else 0.0001)

            nearest = _nearest_levels_from_sr(
                sr or {},
                float(cl),
                float(atr),
                pip_factor=float(pip_factor),
                cross_buf=0.0,
            )
        except Exception:
            nearest = None

        lvl = None
        if isinstance(nearest, dict):
            lvl = nearest.get("nearest_support") if resolved_dir == "BUY" else nearest.get("nearest_resistance")

        if isinstance(lvl, (int, float)) and float(lvl) > 0:
            dist_far = abs(float(cl) - float(lvl))
            dist_far_atr = (dist_far / float(atr)) if float(atr) > 0 else None

            gate["reason"] = "WAIT_ZONE_TOUCH"
            gate["stage"] = "ZONE_FAR"
            gate["blocked"] = False
            planned_zone = _level_to_zone(
                float(lvl),
                tfu,
                sym_u,
                float(atr),
            )

            planned_zone["source"] = "nearest_major_fallback"
            planned_zone["kind"] = "support" if resolved_dir == "BUY" else "resistance"

            gate["zone"] = planned_zone
            gate["planned_zone"] = planned_zone
            gate["zone_used"] = None

            gate["nearest_level"] = float(lvl)
            gate["dist"] = float(dist_far)
            gate["dist_atr"] = float(dist_far_atr) if dist_far_atr is not None else None
            gate["nearest"] = nearest
            return False, gate

        gate["reason"] = (
            "no_buy_support_below_price"
            if resolved_dir == "BUY"
            else "no_sell_resistance_above_price"
        )
        gate["stage"] = "ZONE_PICK"
        gate["zone"] = None
        gate["zone_used"] = None
        return False, gate

    
    # distance info ONLY (do not gate / do not return)
    # distance from zone using BOTH live price and last closed candle range
    dist_live = _zone_band_dist(zone, float(decision_px))

    try:
       zl_tmp = float(zone.get("low") if zone.get("low") is not None else zone.get("level"))
       zh_tmp = float(zone.get("high") if zone.get("high") is not None else zone.get("level"))
       if zl_tmp > zh_tmp:
           zl_tmp, zh_tmp = zh_tmp, zl_tmp

       # If last closed candle touched/entered zone, distance is actionable
       candle_touched_zone = bool(float(lo) <= zh_tmp and float(hi) >= zl_tmp)

       if candle_touched_zone:
           dist = 0.0
       else:
           dist = dist_live
    except Exception:
        dist = dist_live

    if dist is None:
        dist = abs(float(decision_px) - float(zone.get("level")))

    sym_u2 = str(sym_u or "").upper().strip()
    zone_tf = str(zone.get("tf") or "").upper()

    # hard maximum actionable distance from zone BAND
    if sym_u2 == "XAUUSD":
        hard_cap = 12.0
    elif sym_u2.endswith("JPY"):
        hard_cap = 0.25
    else:
        hard_cap = 0.0025  # 25 pips

    # minimum tolerance so price inside/near zone is not rejected
    if sym_u2 == "XAUUSD":
        min_cap = 3.0
    elif sym_u2.endswith("JPY"):
        min_cap = 0.08
    else:
        min_cap = 0.0008  # 8 pips

    max_dist = min(max(float(move_away_atr) * float(atr), min_cap), hard_cap)
    eps = 0.02 * float(atr)

    gate["hard_cap"] = float(hard_cap)
    gate["min_cap"] = float(min_cap)
    gate["zone_tf"] = zone_tf
    gate["dist_gate_model"] = "ZONE_BAND_ACTIONABLE_DISTANCE_CAP"

    if dist > (max_dist + eps):
        gate["reason"] = "ZONE_TOO_FAR_NO_SETUP"
        gate["stage"] = "DIST_GUARD"
        gate["blocked"] = False

        # show planned major zone immediately on UI
        gate["zone"] = dict(zone)
        gate["planned_zone"] = dict(zone)
        gate["zone_used"] = None

        gate["dist"] = float(dist)
        gate["max_dist"] = float(max_dist)
        gate["over"] = float(dist - max_dist)
        gate["eps"] = float(eps)
        gate["dist_atr"] = float(dist / float(atr)) if float(atr) > 0 else None
        return False, gate

    # Optional: keep visibility that we were near the threshold
    gate["dist"] = float(dist)
    gate["max_dist"] = float(max_dist)
    gate["over"] = float(dist - max_dist)
    gate["eps"] = float(eps)

    gate["dist_info_only"] = True

    # ------------------------------------------------------------
    # 3.9) closed_ms for the CURRENT closed bar (needed for WATCH start)
    # ------------------------------------------------------------
    closed_ms = 0
    try:
        # Prefer explicit close-time if present
        closed_ms = _to_ms_any(c.get("t_close_ms") or c.get("tCloseMs") or c.get("t_close"))
        if not closed_ms:
            # Fallback: open-time + tf_ms
            t_open_ms = _to_ms_any(c.get("t_open_ms") or c.get("ts_ms") or c.get("t"))
            closed_ms = int(t_open_ms + tf_ms) if t_open_ms else 0
    except Exception:
        closed_ms = 0





    # ------------------------------------------------------------
    # 4) interaction rule (touch detection)
    # ------------------------------------------------------------
    zl = float(zone.get("low") or zone["level"])
    zh = float(zone.get("high") or zone["level"])

    try:
        px_live = float(live_px) if live_px is not None else float(cl)
    except Exception:
        px_live = float(cl)

    try:
        z_level = float(zone.get("level") or 0.0)
    except Exception:
        z_level = float(zone["level"])

    # Touch condition: candle must break into the zone
    # BUY: candle low breaks zone.high (enters support zone from above)
    # SELL: candle high breaks zone.low (enters resistance zone from below)
    if resolved_dir == "BUY":
        touched = (float(lo) <= zh)  # Low breaks zone.high
    else:
        touched = (float(hi) >= zl)  # High breaks zone.low

    if debug_gate:
        gate["touch_basis"] = {
            "live_px": float(px_live),
            "cl": float(cl),
            "lo": float(lo),
            "hi": float(hi),
            "zone_low": zl,
            "zone_high": zh,
            "zone_level": float(zone.get("level") or 0.0),
            "touched_now": bool(touched),
            "touch_method": "CLOSED_BAR_LOHI_VS_ZONE_BOUNDARIES",
        }

        if gate.get("dbg_h1_bars_n") is not None:
            gate["dbg_gate_h1_snap_bars_n"] = gate.get("dbg_h1_bars_n")
        if gate.get("dbg_h1_snap_serverNow") is not None:
            gate["dbg_gate_h1_snap_serverNow"] = gate.get("dbg_h1_snap_serverNow")

    # ------------------------------------------------------------
    # 5) start watch if not started yet
    # ------------------------------------------------------------
    if zone_used is None:
        if not touched:
            gate["reason"] = "WAIT_ZONE_TOUCH"
            gate["stage"] = "TOUCH"
            gate["blocked"] = False
           
            # show planned major zone before touch
            gate["zone"] = dict(zone)
            gate["planned_zone"] = dict(zone)
            gate["zone_used"] = None
            return False, gate

        zone_used = dict(zone)
        

        # Ensure boundaries exist (SR zones may be level-only)
        try:
            if isinstance(zone_used, dict):
                lvl0 = float(zone_used.get("level") or 0.0)
                zl0 = zone_used.get("low")
                zh0 = zone_used.get("high")
                if zl0 is None or zh0 is None or float(zl0) >= float(zh0):
                    ztmp = _level_to_zone(lvl0, tfu, sym_u, float(atr))
                    zone_used["low"] = float(ztmp["low"])
                    zone_used["high"] = float(ztmp["high"])
        except Exception:
            pass

        watch = {
            "state": "WATCH",
            "started_ms": int(closed_ms) if int(closed_ms or 0) > 0 else int(now_ms_pick),
            "direction": resolved_dir,
            "tf": tfu,
            "zone_used": zone_used,
        }

        try:
            R.set(wkey, json.dumps(watch, separators=(",", ":")), ex=7 * 24 * 3600)
        except Exception:
            pass
        # ------------------------------------------------------------
        # SAME-CANDLE RECLAIM FIX
        # If the candle that first touched/entered the zone also closes
        # back beyond the reclaim boundary, REV_OK is valid immediately.
        #
        # BUY: touched support and closed above zone_high
        # SELL: touched resistance and closed below zone_low
        # ------------------------------------------------------------
        try:
            zl_now = float(zone_used.get("low") or zone_used.get("level") or 0.0)
            zh_now = float(zone_used.get("high") or zone_used.get("level") or 0.0)

            same_candle_rev_ok = False
            if resolved_dir == "BUY":
                same_candle_rev_ok = bool(touched and float(cl) > float(zh_now))
            elif resolved_dir == "SELL":
                same_candle_rev_ok = bool(touched and float(cl) < float(zl_now))

            if same_candle_rev_ok:
                try:
                    watch["state"] = "REV_OK"
                    watch["rev_ok"] = True
                    watch["rev_ok_ms"] = int(closed_ms)
                    watch["rev_ok_bar_hi"] = float(hi)
                    watch["rev_ok_bar_lo"] = float(lo)
                    R.set(wkey, json.dumps(watch, separators=(",", ":")), ex=7 * 24 * 3600)
                except Exception:
                    pass

                gate["zone_used"] = zone_used
                gate["rev_ok"] = True
                gate["watch_key"] = str(wkey)
                gate["rev_state"] = {
                    "state": "REV_OK",
                    "started_ms": int(watch.get("started_ms") or now_ms_pick),
                    "rev_ok_ms": int(closed_ms),
                    "direction": resolved_dir,
                    "tf": tfu,
                    "rev_ok_bar_hi": float(hi),
                    "rev_ok_bar_lo": float(lo),
                }
                gate["rev_trigger"] = {
                    "entry_above": float(hi),
                    "entry_below": float(lo),
                }
                gate["reason"] = (
                    f"REV_OK | FZ {float(zl_now):.2f}-{float(zh_now):.2f} "
                    f"| RC {float(cl):.2f} "
                    f"| entry > {float(hi):.2f}"
                    if resolved_dir == "BUY"
                    else
                    f"REV_OK | FZ {float(zl_now):.5f}-{float(zh_now):.5f} "
                    f"| RC {float(cl):.5f} "
                    f"| entry < {float(lo):.5f}"
                )
                gate["reason"] = f"{gate.get('reason')} | PCB C={float(cl):.2f} tf={tfu}"
                gate["stage"] = "REV"
                gate["blocked"] = False
                return True, gate
        except Exception as e:
            if debug_gate:
                gate["dbg_same_candle_reclaim_exc"] = f"{type(e).__name__}:{e}"

    gate["zone_used"] = zone_used
    gate["rev_state"] = {
        "state": "WATCH",
        "started_ms": int((watch or {}).get("started_ms") or now_ms_pick),
        "direction": str((watch or {}).get("direction") or resolved_dir),
        "tf": str((watch or {}).get("tf") or tfu),
    }

    # ------------------------------------------------------------
    # SAFETY: zone_used must be a dict from here on
    # ------------------------------------------------------------
    if not isinstance(zone_used, dict):
        gate["reason"] = "WATCH_ZONE_MISSING"
        gate["stage"] = "WATCH"
        gate["blocked"] = False
        gate["zone_used"] = None
        if debug_gate:
            gate["dbg_zone_used_type"] = str(type(zone_used).__name__)
            gate["dbg_watch_type"] = str(type(watch).__name__)
        return False, gate



    
    
    # 5) reversal confirmation: reclaim only (CLOSED candle after watch started)
    zl = float(zone_used.get("low") or zone_used.get("level") or 0.0)
    zh = float(zone_used.get("high") or zone_used.get("level") or 0.0)


    started_ms = int((watch or {}).get("started_ms") or 0)

    

    if debug_gate:
        gate["rev_basis"] = {
            "closed_ms": int(closed_ms),
            "started_ms": int(started_ms),
            "cl": float(cl),
            "zl": float(zl),
            "zh": float(zh),
        }

    if resolved_dir == "BUY":
        rev_ok = (float(cl) > float(zh))
    else:
        rev_ok = (float(cl) < float(zl))

    gate["rev_ok"] = bool(rev_ok)
    # ------------------------------------------------------------
    # DEBUG: identify exact reversal candle
    # ------------------------------------------------------------
    try:
        gate["reversal_candidate"] = {
            "closed_ms": int(closed_ms),
            "open": float(_bar_f(c, "o", "open") or 0.0),
            "high": float(hi),
            "low": float(lo),
            "close": float(cl),
            "zone_low": float(zl),
            "zone_high": float(zh),
            "rule": (
                "BUY_CLOSE_ABOVE_ZONE_HIGH"
                if resolved_dir == "BUY"
                else "SELL_CLOSE_BELOW_ZONE_LOW"
            ),
            "rev_ok": bool(rev_ok),
        }
    except Exception:
        pass
    try:
        gate["frozen_zone_ui"] = (
            f"FROZEN {resolved_dir} "
            f"{float(zl):.2f}-{float(zh):.2f} "
            f"since={int(started_ms)}"
        )

        gate["reversal_ui"] = (
            f"CANDLE C={float(cl):.2f} "
            f"Z={float(zl):.2f}-{float(zh):.2f} "
            f"OK={bool(rev_ok)}"
        )
    except Exception:
        pass

    if rev_ok:
        # persist REV_OK so we don't show WATCH after confirm
        try:
            if isinstance(watch, dict):
                watch["state"] = "REV_OK"
                watch["rev_ok"] = True
                watch["rev_ok_ms"] = int(closed_ms)
                watch["rev_ok_bar_close"] = float(cl)

                watch["frozen_zone_low"] = float(zl)
                watch["frozen_zone_high"] = float(zh)
                watch["frozen_zone_tf"] = str(tfu)
                #store the REV_OK candle trigger levels (for live-break entry)
                try:
                    watch["rev_ok_bar_hi"] = float(hi)
                except Exception:
                    pass
                try:
                    watch["rev_ok_bar_lo"] = float(lo)
                except Exception:
                    pass
                R.set(wkey, json.dumps(watch, separators=(",", ":")), ex=7 * 24 * 3600)
        except Exception:
            pass

        gate["rev_ok"] = True
        gate["watch_key"] = str(wkey)
        gate["rev_state"] = {
            "state": "REV_OK",
            "started_ms": int((watch or {}).get("started_ms") or now_ms_pick),
            "rev_ok_ms": int(closed_ms),
            "direction": str((watch or {}).get("direction") or resolved_dir),
            "tf": str((watch or {}).get("tf") or tfu),
            "rev_ok_bar_hi": float(hi),
            "rev_ok_bar_lo": float(lo),
            "rev_ok_bar_close": float(cl),
        }
        gate["rev_trigger"] = {
            "entry_above": float(hi),
            "entry_below": float(lo),
        }
        gate["frozen_zone"] = {
            "low": float((watch or {}).get("frozen_zone_low", zl)),
            "high": float((watch or {}).get("frozen_zone_high", zh)),
            "tf": str((watch or {}).get("frozen_zone_tf", tfu)),
        }

        try:
            if resolved_dir == "BUY":
                gate["reason"] = (
                    f"REV_OK | FZ {float((watch or {}).get('frozen_zone_low', zl)):.2f}-"
                    f"{float((watch or {}).get('frozen_zone_high', zh)):.2f} "
                    f"| RC {float((watch or {}).get('rev_ok_bar_close', cl)):.2f} "
                    f"| entry > {float(hi):.2f}"
                )
            else:
                gate["reason"] = (
                    f"REV_OK | FZ {float((watch or {}).get('frozen_zone_low', zl)):.5f}-"
                    f"{float((watch or {}).get('frozen_zone_high', zh)):.5f} "
                    f"| RC {float((watch or {}).get('rev_ok_bar_close', cl)):.5f} "
                    f"| entry < {float(lo):.5f}"
                )
        except Exception:
            gate["reason"] = "REV_OK"
        gate["reason"] = f"{gate.get('reason')} | PCB C={float(cl):.2f} tf={tfu}"
        gate["stage"] = "REV"
        gate["blocked"] = False
        return True, gate




    
    
    
    # 6) invalidation: 2 consecutive COMPLETE closed candles beyond the zone boundary (after watch started)
    consec = 0
    checked = 0
    try:
        started_ms = int((watch or {}).get("started_ms") or 0)

        # Use the same "closed bar" definition as the picker: bar_start + tf_ms <= now_ms_pick
        def _tms(b):
            if not isinstance(b, dict):
                return 0
            return _to_ms_any(b.get("ts_ms") or b.get("t") or b.get("t_open_ms") or b.get("t_close_ms"))


        bs = [b for b in (bars or []) if isinstance(b, dict)]
        bs.sort(key=_tms)

        # closed bars after watch started
        closed_after = []
        for b in bs:
            t0 = _tms(b)
            if not t0:
                continue

            # prefer explicit close time if present
            t_close = 0
            try:
                t_close = _to_ms_any(
                    b.get("t_close_ms") or b.get("tCloseMs") or b.get("t_close") or b.get("tClose")
                )
            except Exception:
                t_close = 0

            if not t_close:
                
                try:
                    # timestamps are OPEN times in our feed
                    t_close = int(t0) + int(tf_ms)
                except Exception:
                    t_close = int(t0) + int(tf_ms)

            # must close AFTER watch started
            # Include the candle that started the watch.
            # If first touch candle itself closes beyond invalidation boundary,
            # it counts as invalidation close #1.
            if int(t_close) < int(started_ms):
                continue

            # must be safely closed vs our pick clock
            if int(t_close) > int(now_ms_pick):
                continue

            if b.get("complete") is False:
                continue

            closed_after.append(b)

        # IMPORTANT: if timestamps are missing, invalidation must still work (match picker behavior)
        if not closed_after:
            recent = bs[-5:] if len(bs) >= 5 else bs
            has_ts = any(_tms(x) > 0 for x in recent)
            if not has_ts:
                closed_after = bs[-max(6, int(hard_close_bars) + 2):]

        inv_trace = []  # DEBUG: record how consec was computed

        # scan in chronological order so "consecutive" is well-defined
        for cb in closed_after:
            cval = _bar_f(cb, "c", "close")
            if cval is None:
                continue
            checked += 1

            try:
                cv = float(cval)
            except Exception:
                continue

            if resolved_dir == "BUY":
                bad = (cv <= float(zl))  # BUY invalidates only if close <= zone_low
            else:
                bad = (cv >= float(zh))  # SELL invalidates only if close >= zone_high

            if bad:
                consec += 1
            else:
                consec = 0

            if debug_gate:
                try:
                    t0_cb = _tms(cb)
                except Exception:
                    t0_cb = 0

                # compute close time for trace using same heuristic
                t_close_cb = 0
                try:
                    t_close_cb = _to_ms_any(
                        cb.get("t_close_ms") or cb.get("tCloseMs") or cb.get("t_close") or cb.get("tClose")
                    )
                except Exception:
                    t_close_cb = 0

                if not t_close_cb:
                    try:
                        if int(tf_ms) > 0 and (int(t0_cb) % int(tf_ms)) == 0:
                            t_close_cb = int(t0_cb)
                        else:
                            t_close_cb = int(t0_cb) + int(tf_ms)
                    except Exception:
                        t_close_cb = int(t0_cb) + int(tf_ms)

                inv_trace.append({
                    "t_open": int(t0_cb),
                    "t_close": int(t_close_cb),
                    "close": float(cv),
                    "bad": bool(bad),
                    "consec": int(consec),
                })
                if len(inv_trace) > 12:
                    inv_trace = inv_trace[-12:]

            if consec >= int(hard_close_bars):
                break
    except Exception:
        consec = 0
        checked = 0
        inv_trace = []
    # ------------------------------------------------------------
    # HARD FALLBACK: count latest consecutive invalid closes
    # Some feeds/timestamps/complete flags can miss closed_after.
    # This guarantees:
    # BUY  = latest 2 closed candles below zone_low => INVALIDATED
    # SELL = latest 2 closed candles above zone_high => INVALIDATED 
    # ------------------------------------------------------------
    try:
        if consec < int(hard_close_bars):
            bs2 = [b for b in (bars or []) if isinstance(b, dict)]

            def _tclose2(b):
                t = _to_ms_any(
                    b.get("t_close_ms")
                    or b.get("tCloseMs")
                    or b.get("t_close")
                    or b.get("tClose")
                )
                if not t:
                    t0 = _to_ms_any(b.get("ts_ms") or b.get("t") or b.get("t_open_ms"))
                    if t0:
                        try:
                            if int(tf_ms) > 0 and int(t0) % int(tf_ms) == 0:
                                t = int(t0)
                            else:
                                t = int(t0) + int(tf_ms)
                        except Exception:
                            t = int(t0) + int(tf_ms)
                return int(t or 0)

            bs2.sort(key=_tclose2)

            pool = []
            for b in bs2:
                tc = _tclose2(b)

                if started_ms and tc and tc < int(started_ms):
                    continue

                if closed_ms and tc and tc > int(closed_ms):
                    continue

                # do not trust missing complete flag; only skip explicit incomplete future bars
                if b.get("complete") is False and tc > int(closed_ms or 0):
                    continue

                pool.append(b)

            # if timestamps are missing, use latest bars fallback
            if not pool:
               pool = bs2[-6:]

            consec2 = 0
            inv_trace2 = []

            for cb in reversed(pool):
                cv = _bar_f(cb, "c", "close")
                if cv is None:
                    continue

                cv = float(cv)

                if resolved_dir == "BUY":
                   bad2 = cv <= float(zl)
                else:
                   bad2 = cv >= float(zh)

                inv_trace2.append({
                    "close": float(cv),
                    "bad": bool(bad2),
                    "consec": int(consec2 + 1 if bad2 else 0),
                    "fallback": True,
                })

                if bad2:
                    consec2 += 1
                else:
                    break

                if consec2 >= int(hard_close_bars):
                    break

            if consec2 > consec:
                consec = int(consec2)
                checked = max(int(checked or 0), len(pool))
                inv_trace = list(reversed(inv_trace2[-12:]))

    except Exception as e:
        if debug_gate:
            gate["dbg_inv_fallback_exc"] = f"{type(e).__name__}:{e}"

    if debug_gate:
        gate["inv_basis"] = {
            "consec": int(consec),
            "checked": int(checked),
            "hard_close_bars": int(hard_close_bars),
            "started_ms": int((watch or {}).get("started_ms") or 0),
            "now_ms_pick": int(now_ms_pick),
            "zone_low": float(zl),
            "zone_high": float(zh),
            "inv_trace": inv_trace,
        }

    if consec >= int(hard_close_bars):
        gate["reason"] = "INVALIDATED"
        gate["invalidate_state"] = {"state": "INVALIDATED", "consec": int(consec)}
        gate["blocked"] = True
        try:
            R.delete(wkey)
        except Exception:
            pass
        return False, gate

    # If we reach here: watch active but not rev_ok and not invalidated
    try:
        gate["reason"] = (
            f"REV_WATCH | FZ {float(zl):.2f}-{float(zh):.2f} "
            f"| C {float(cl):.2f} "
            f"| need > {float(zh):.2f}"
            if resolved_dir == "BUY"
            else
            f"REV_WATCH | FZ {float(zl):.5f}-{float(zh):.5f} "
            f"| C {float(cl):.5f} "
            f"| need < {float(zl):.5f}"
        )
    except Exception:
        gate["reason"] = "REV_WATCH"
        gate["stage"] = "WATCH"
        gate["blocked"] = False
    return False, gate
