
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
    Pick the last CLOSED bar.
    Priority: complete=True > clock fallback
    MT5 sends future bars complete=True — filter by open_ms <= sys_now.
    """
    import time as _t
    try:
        if not isinstance(bars, list) or len(bars) < 2:
            return (None, None)
        tf_ms = int(tf_ms or 0)
        if tf_ms <= 0:
            return (None, None)
        sys_now = int(now_ms or 0)
        if sys_now <= 0:
            sys_now = int(_t.time() * 1000)
        bs = [b for b in bars if isinstance(b, dict)]
        if len(bs) < 2:
            return (None, None)
        def _om(b):
            for k in ("t_open_ms","tOpenMs","open_time_ms","ts_ms","t","time","ts"):
                v = _to_ms_any(b.get(k))
                if v > 0: return int(v)
            return 0
        has_ts = any(_om(b) > 0 for b in bs[-5:])
        if not has_ts:
            return (bs[-2], bs[-3]) if len(bs) >= 3 else (None, None)
        bs_sorted = sorted(bs, key=lambda b: _om(b) or 0)
        for i in range(len(bs_sorted) - 1, -1, -1):
            b = bs_sorted[i]
            if b.get("complete") is False:
                continue
            om = _om(b)
            if om <= 0:
                continue 
            is_complete = b.get("complete") is True
            closed_by_clock = (om + tf_ms) <= sys_now
            if not is_complete and not closed_by_clock:
                continue  # still forming
            prev = bs_sorted[i-1] if i-1 >= 0 else None
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

def _pick_best_scored_zone(sr_all: dict, direction: str, cl: float) -> dict | None:
    if not isinstance(sr_all, dict):
        return None

    dir_u = str(direction or "").upper().strip()
    key = "best_support" if dir_u == "BUY" else "best_resistance"

    z = sr_all.get(key)
    if not isinstance(z, dict):
        return None

    try:
        lvl = float(z.get("level"))
        low = float(z.get("low"))
        high = float(z.get("high"))
        px = float(cl)
    except Exception:
        return None

    if lvl <= 0 or low >= high:
        return None

    if z.get("side_ok") is False or z.get("stale") is True:
        return None

    if dir_u == "BUY" and high > px:
        return None

    if dir_u == "SELL" and low < px:
        return None
    

    out = dict(z)
    out["zone_source"] = "BEST_SCORED_SR"
    out["selection_model"] = "BEST_SR_THEN_H1_H4_MAJOR_FALLBACK"
    out["zone_role"] = "BEST_SUPPORT" if dir_u == "BUY" else "BEST_RESISTANCE"
    return out

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
    4. H1 any (touches>=1 if tight+sr_score>=4, else touches>=2) within cap_h1_min ATR

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
        touches = int(z.get("touches") or 0)
        if touches >= 2:
            return True
        if touches == 1:
            # Accept 1-touch only if zone is tight and has structural significance
            band_type = str(z.get("band_type") or "")
            sr_score = float(z.get("sr_score") or 0)
            return sr_score >= 4.0 and "wide" not in band_type
        return False

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
            try:
                zl_pick = float(zz.get("low") if zz.get("low") is not None else zz.get("level"))
                zh_pick = float(zz.get("high") if zz.get("high") is not None else zz.get("level"))
                if zl_pick > zh_pick:
                    zl_pick, zh_pick = zh_pick, zl_pick

                if float(cl) < zl_pick:
                    band_dist = zl_pick - float(cl)
                elif float(cl) > zh_pick:
                    band_dist = float(cl) - zh_pick
                else:
                    band_dist = 0.0

                max_pick_dist = min(max(2.0 * float(atr), 3.0), 12.0) if str(sym_u).upper() == "XAUUSD" else 2.0 * float(atr)

                if band_dist > max_pick_dist:
                    continue
            except Exception:
                pass
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
    # PHASE-1 SAFETY:
    # Do not execute H4 zone using H1 reversal candles.
    # H4 execution requires H4 candle picker, handled in Phase 2.
    zone = h1_major_zone

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

def _zone_cooldown_key(sym: str, direction: str, tf_tag: str) -> str:
    return f"xtl:zone:cooldown:{(sym or '').upper().strip()}:{(direction or '').upper().strip()}:{(tf_tag or 'H1').upper().strip()}"

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
    # PHASE-1 FIX:
    # Always prefer latest device Redis snap.
    # row_h1 bars may be stale from trend_endpoints snapshot.
    if True:
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
                    # Fix: serverNow can be stale (MT5 bridge doesn't update it every tick)
                    # Use max of serverNow, last bar close time, and system now
                    # This prevents the bar picker from treating recent closed bars as "future"
                    _snap_server_now = int(snap_server_now) if snap_server_now > 0 else 0
                    _last_bar_close_ms = 0
                    try:
                        if isinstance(bars, list) and bars:
                            _lb = bars[-1]
                            _lb_t = _to_ms_any(
                                _lb.get("t_close_ms") or _lb.get("tCloseMs") or
                                _lb.get("t") or _lb.get("ts") or _lb.get("time") or 0
                            )
                            if _lb_t and int(_lb_t) > 0:
                                # If bar key is open time, add tf_ms to get close time
                                _lb_close = int(_lb_t)
                                if _lb_close < int(now_ms) - tf_ms:
                                    # looks like open time — add tf_ms
                                    _lb_close = _lb_close + int(tf_ms)
                                _last_bar_close_ms = _lb_close
                    except Exception:
                        _last_bar_close_ms = 0

                    # Use the largest of: serverNow, last bar close, system now
                    # Ensures bar picker never skips a genuinely closed candle
                    now_ms_pick = max(
                        _snap_server_now,
                        _last_bar_close_ms,
                        int(now_ms or 0)
                    )
                    if now_ms_pick <= 0:
                        now_ms_pick = int(now_ms)
                    snap_repaired = False

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

        # ------------------------------------------------------------
        # Best scored SR feeds direction resolver first.
        # H4 is confirmation only, not execution.
        # Fallback to legacy H1 display zones only if best scored zone missing.
        # ------------------------------------------------------------
        best_buy_zone = (sr or {}).get("best_support") if isinstance(sr, dict) else None
        best_sell_zone = (sr or {}).get("best_resistance") if isinstance(sr, dict) else None

        candidate_sources = []

        if isinstance(best_buy_zone, dict):
            bz = dict(best_buy_zone)
            bz["tf"] = str(bz.get("tf") or "H1").upper()
            bz["kind"] = "support"
            bz["zone_source"] = "BEST_SCORED_SR"
            bz["selection_model"] = "BEST_SR_DIRECTION_RESOLVER"
            candidate_sources.append(("BUY", bz))
        else:
            candidate_sources.append(("BUY", display_zones.get("h1_buy_zone")))

        if isinstance(best_sell_zone, dict):
            bz = dict(best_sell_zone)
            bz["tf"] = str(bz.get("tf") or "H1").upper()
            bz["kind"] = "resistance"
            bz["zone_source"] = "BEST_SCORED_SR"
            bz["selection_model"] = "BEST_SR_DIRECTION_RESOLVER"
            candidate_sources.append(("SELL", bz))
        else:
            candidate_sources.append(("SELL", display_zones.get("h1_sell_zone")))

        for side, z in candidate_sources:
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
        if resolved_dir in ("BUY", "SELL"):
            best_key = "best_support" if resolved_dir == "BUY" else "best_resistance"
            best_zone = sr.get(best_key) if isinstance(sr, dict) else None

            if isinstance(best_zone, dict):
                try:
                    bz = dict(best_zone)
                    if (
                        bz.get("level") is not None
                        and bz.get("low") is not None
                        and bz.get("high") is not None
                        and float(bz.get("low")) < float(bz.get("high"))
                        and bz.get("side_ok") is not False
                        and bz.get("stale") is not True
                    ):
                        bz["zone_source"] = "BEST_SCORED_SR"
                        bz["selection_model"] = "BEST_SR_DIRECTION_RESOLVED"
                        bz["execution_tf"] = "H1"
                        bz["zone_role"] = "BEST_SUPPORT" if resolved_dir == "BUY" else "BEST_RESISTANCE"
                        bz["actionable_dist"] = _zone_band_dist(bz, float(decision_px)) or 0.0
                        preferred_zone = bz
                except Exception:
                    pass

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
                "zone_source": x["zone"].get("zone_source"),
                "selection_model": x["zone"].get("selection_model"),
                "quality_score": x["zone"].get("quality_score"),
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
    # ------------------------------------------------------------
    # WATCH INTEGRITY REPAIR
    # If rev_ok and state are inconsistent (one set but not the other),
    # repair them so the REV_OK early-return lock fires correctly.
    # This prevents the gate from falling through and re-evaluating
    # on every tick after REV_OK was already confirmed.
    # ------------------------------------------------------------
    if isinstance(watch, dict):
        w_state = str(watch.get("state") or "").upper()
        w_rev_ok = bool(watch.get("rev_ok"))
        if w_rev_ok and w_state != "REV_OK":
            watch["state"] = "REV_OK"
        if w_state == "REV_OK" and not w_rev_ok:
            watch["rev_ok"] = True
        # Also ensure rev_ok_bar_hi/lo exist if state is REV_OK
        if w_state == "REV_OK" or w_rev_ok:
            if not watch.get("rev_ok_bar_hi") and watch.get("last_checked_high"):
                watch["rev_ok_bar_hi"] = float(watch["last_checked_high"])
            if not watch.get("rev_ok_bar_lo") and watch.get("last_checked_low"):
                watch["rev_ok_bar_lo"] = float(watch["last_checked_low"])
        # ------------------------------------------------------------
        # RC CANDLE VALIDITY CHECK
        # If watch is in REV_OK state but the RC candle close time
        # is in the future (forming candle was incorrectly used as RC),
        # auto-repair: roll back to REV_WATCH, clear only RC fields.
        # Keep zone_used, started_ms, direction — zone freeze is valid.
        # ------------------------------------------------------------
        if w_rev_ok or w_state == "REV_OK":
            rev_ok_ms = int(watch.get("rev_ok_ms") or 0)
            if rev_ok_ms > 0 and rev_ok_ms > int(now_ms_pick or 0):
                # RC candle close time is in the future — forming candle was used
                # Roll back to REV_WATCH cleanly
                watch["state"] = "REV_WATCH"
                watch["rev_ok"] = False
                watch["rev_ok_ms"] = 0
                watch["rev_ok_bar_hi"] = None
                watch["rev_ok_bar_lo"] = None
                watch["rev_ok_bar_close"] = None
                # Persist the rollback immediately
                try:
                    R.set(wkey, json.dumps(
                        {k: v for k, v in watch.items() if v is not None},
                        separators=(",", ":")
                    ), ex=7 * 24 * 3600)
                except Exception:
                    pass
                if debug_gate:
                    gate["dbg_rc_rollback"] = {
                        "reason": "forming_candle_used_as_rc",
                        "rev_ok_ms": rev_ok_ms,
                        "now_ms_pick": int(now_ms_pick or 0),
                        "rolled_back_to": "REV_WATCH",
                    }     


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
        # FREEZE RULE: if zone_used already has a valid low/high band, NEVER touch it.
        # Only repair truly missing or collapsed bands (legacy watches).
        need_band = False
        try:
            zl = zone_used.get("low")
            zh = zone_used.get("high")
            if zl is None or zh is None or float(zl) >= float(zh):
                need_band = True
            # Valid band exists — lock it, do not rehydrate from SR
            else:
                need_band = False
        except Exception:
            need_band = True

        # 1) try rehydrate from SR — ONLY for legacy watches missing a band
        if need_band and lvl0 is not None:
            kind0 = str(zone_used.get("kind") or ("support" if resolved_dir == "BUY" else "resistance")).lower()
            band = _rehydrate_band_from_sr_level(sr or {}, tfu, kind0, float(lvl0))
            if band is not None:
                zone_used["low"] = float(band[0])
                zone_used["high"] = float(band[1])
                need_band = False

        # 2) if still missing/collapsed, synthesize from ATR (last resort only)
        if need_band and lvl0 is not None:
            try:
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

    # ------------------------------------------------------------
    # HARD STOP: active MT5 trade must not enter rediscovery/DIST_GUARD
    # ------------------------------------------------------------
    try:
        _trade_state = str((watch or {}).get("trade_state") or "").upper()
        _state = str((watch or {}).get("state") or "").upper()

        if _trade_state == "TRADE_ACTIVE" or _state == "TRADE_ACTIVE":
            _zu = (watch or {}).get("zone_used") or (watch or {}).get("planned_zone")

            gate["blocked"] = False
            gate["reason"] = "TRADE_ACTIVE"
            gate["stage"] = "MANAGE_TRADE"
            gate["trade_state"] = "TRADE_ACTIVE"
            gate["zone"] = dict(_zu) if isinstance(_zu, dict) else None
            gate["zone_used"] = dict(_zu) if isinstance(_zu, dict) else None
            gate["planned_zone"] = dict(_zu) if isinstance(_zu, dict) else None
            gate["entry_triggered"] = bool((watch or {}).get("entry_triggered"))
            gate["entry_price"] = (watch or {}).get("entry_price")
            gate["entry_ts_ms"] = (watch or {}).get("entry_ts_ms")
            gate["mt5_job_id"] = (watch or {}).get("mt5_job_id")
            gate["mt5_ticket"] = (watch or {}).get("mt5_ticket")
            gate["rev_state"] = dict(watch or {})

            return True, gate
    except Exception:
        pass
    
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
                R.set(wkey, json.dumps(watch, separators=(",", ":")), ex=7 * 24 * 3600)
                did_repair = True

        if debug_gate:
            gate["dbg_watch_band_repaired"] = bool(did_repair)
    except Exception:
        pass


    # ------------------------------------------------------------
    # FALLBACK: watch key missing but open registry has active trade.
    # Do not hardcode user_id; scan open registries and match symbol.
    # ------------------------------------------------------------
    try:
        if (
            not isinstance(watch, dict)
            or str(watch.get("trade_state") or "").upper() != "TRADE_ACTIVE"
        ):
            for open_key in R.scan_iter("xtl:strategy:oppt:open:*"):
                open_map = R.hgetall(open_key)
                for _k, _v in (open_map or {}).items():
                    if isinstance(_v, (bytes, bytearray)):
                        _v = _v.decode("utf-8", "ignore")
                    tr = json.loads(_v) if isinstance(_v, str) else _v
                    if not isinstance(tr, dict):
                        continue

                    if str(tr.get("symbol") or "").upper() != sym_u:
                        continue
                    if str(tr.get("trade_state") or "").upper() != "TRADE_ACTIVE":
                        continue

                    _side = str(tr.get("side") or "").upper()
                    if _side not in ("BUY", "SELL"):
                        continue

                    _zu = tr.get("entry_zone")
                    if not isinstance(_zu, dict):
                        _zu = {
                            "level": tr.get("entry_zone_level"),
                            "low": tr.get("entry_zone_low"),
                            "high": tr.get("entry_zone_high"),
                            "tf": tr.get("entry_zone_tf") or tfu,
                            "kind": tr.get("entry_zone_kind"),
                        }

                    gate["blocked"] = False
                    gate["reason"] = "TRADE_ACTIVE"
                    gate["stage"] = "MANAGE_TRADE"
                    gate["trade_state"] = "TRADE_ACTIVE"
                    gate["resolved_dir"] = _side
                    gate["zone"] = dict(_zu) if isinstance(_zu, dict) else None
                    gate["zone_used"] = dict(_zu) if isinstance(_zu, dict) else None
                    gate["planned_zone"] = dict(_zu) if isinstance(_zu, dict) else None
                    gate["entry_triggered"] = True
                    gate["entry_price"] = tr.get("entry_price")
                    gate["entry_ts_ms"] = tr.get("opened_at_ms")
                    gate["mt5_job_id"] = tr.get("mt5_job_id")
                    gate["mt5_ticket"] = tr.get("mt5_ticket")
                    gate["rev_state"] = tr

                    return True, gate
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
            # No actionable near/major SR.
            # Do NOT expose far/display zone as executable gate zone.
            gate["zone"] = None
            gate["planned_zone"] = None
            gate["zone_used"] = None

            # Optional display-only fields for UI/debug, never used by executor.
            # Optional display-only fields for UI/debug, never used by executor.
            # Direction-aware: never show opposite-side zone in strategy row.
            if dir_u == "BUY":
                gate["display_zone"] = (
                    display_zones.get("h1_buy_zone")
                    or display_zones.get("h4_buy_zone")
                )
            elif dir_u == "SELL":
                gate["display_zone"] = (
                    display_zones.get("h1_sell_zone")
                    or display_zones.get("h4_sell_zone")
                )
            else:
                gate["display_zone"] = None

            return False, gate
        if isinstance(preferred_zone, dict) and preferred_zone.get("level") is not None:
            zone = dict(preferred_zone)
            # PHASE-1 SAFETY:
            # Do not execute H4 zone with H1 reversal candle.
            # H4 zones are display/watch only until H4 candle execution is implemented.
            if str(zone.get("tf") or "").upper() == "H4":
                gate["reason"] = "H4_ZONE_EXECUTION_DISABLED_PHASE1"
                gate["stage"] = "ZONE_PICK"
                gate["blocked"] = False
                gate["zone"] = dict(zone)
                gate["planned_zone"] = dict(zone)
                gate["zone_used"] = None
                gate["resolved_dir"] = "WATCHING"
                return False, gate
        else:
            gate["reason"] = "WATCHING_NO_NEAR_ACTIONABLE_ZONE"
            gate["stage"] = "ZONE_PICK"
            gate["blocked"] = False
            gate["zone"] = None
            gate["planned_zone"] = None
            gate["zone_used"] = None
            gate["resolved_dir"] = "WATCHING"
            return False, gate

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
        # ------------------------------------------------------------
        # FAR-ZONE DISCOVERY RESET:
        # If selected zone is too far from live/current price, do not keep
        # old far zone as actionable. Clear non-REV watch and allow fresh
        # nearest H1 zone discovery on next cycle.
        # ------------------------------------------------------------
        try:
            if isinstance(watch, dict):
                st = str(watch.get("state") or "").upper()
                # Protect REV_WATCH and WATCH states — zone is frozen, do not delete
                if st not in ("WATCH", "REV_WATCH", "REV_OK", "ENTRY_READY", "ORDER_PENDING", "TRADE_ACTIVE"):
                    R.delete(wkey)
        except Exception:
            pass

        gate["reason"] = "ZONE_TOO_FAR_RESET_FOR_REDISCOVERY"
        gate["stage"] = "DIST_GUARD"
        gate["blocked"] = False

        gate["zone"] = dict(zone)
        gate["planned_zone"] = dict(zone)
        gate["zone_used"] = None

        gate["dist"] = float(dist)
        gate["max_dist"] = float(max_dist)
        gate["over"] = float(dist - max_dist)
        gate["eps"] = float(eps)
        gate["dist_atr"] = float(dist / float(atr)) if float(atr) > 0 else None
        gate["rediscovery_required"] = True
        # hard reset stale zone/watch — but NEVER reset a frozen watch
        try:
            _st = str((watch or {}).get("state") or "").upper()
            # Also check raw Redis key — watch may be loaded without zone_used
            _raw_watch = None
            try:
                if wkey:
                    _raw = R.get(str(wkey))
                    if _raw:
                        _raw_watch = json.loads(_raw) if isinstance(_raw, str) else _raw
                        if isinstance(_raw_watch, dict) and not _st:
                            _st = str(_raw_watch.get("state") or "").upper()
            except Exception:
                pass
            _has_frozen = _st in ("WATCH", "REV_WATCH", "REV_OK", "ENTRY_READY", "ORDER_PENDING", "TRADE_ACTIVE")

            if not _has_frozen:
                gate["zone"] = None
                gate["planned_zone"] = None
                gate["zone_used"] = None
                gate["resolved_dir"] = "WATCHING"
                gate["h1_buy_zone"] = None
                gate["h4_buy_zone"] = None
                gate["h1_sell_zone"] = None
                gate["h4_sell_zone"] = None
                gate["rev_state"] = None
                gate["rev_basis"] = None
                gate["touch_basis"] = None
                if wkey:
                    try:
                        for _s in ("BUY", "SELL"):
                            for _t in ("H1", "H4"):
                                R.delete(f"xtl:zone:watch:{sym_u}:{_s}:{_t}")
                    except Exception:
                        pass
            else:
                # frozen watch active — preserve zone and direction
                gate["zone"] = dict(watch.get("zone_used") or {})
                gate["zone_used"] = dict(watch.get("zone_used") or {})
                gate["planned_zone"] = dict(watch.get("zone_used") or {})
                gate["resolved_dir"] = str((watch or {}).get("direction") or "WATCHING").upper()
                gate["rev_state"] = watch
        except Exception:
            pass
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

    # Touch condition must prove price actually entered the frozen zone.
    # BUY support touch:
    #   - current/last candle low <= zone_high, OR live price is inside/below zone_high
    # SELL resistance touch:
    #   - current/last candle high >= zone_low, OR live price is inside/above zone_low
    #
    # IMPORTANT:
    # Do not freeze just because direction resolved near a zone.
    # REV_OK must never happen before actual touch.
    try:
        _candle_touched_buy = bool(float(lo) <= float(zh) and float(hi) >= float(zl))
        _candle_touched_sell = bool(float(hi) >= float(zl) and float(lo) <= float(zh))
    except Exception:
        _candle_touched_buy = False
        _candle_touched_sell = False

    try:
        _live_inside_zone = bool(float(zl) <= float(px_live) <= float(zh))
    except Exception:
        _live_inside_zone = False

    # STRICT H1 TOUCH + LIVE TOUCH FREEZE:
    # - CLOSED H1 candle range touch is accepted.
    # - LIVE price inside zone is also accepted so REV_WATCH starts immediately.
    # - REV_OK still waits for closed H1 candle later.
    try:
        _closed_bar_touched = bool(
            float(lo) <= float(zh)
            and float(hi) >= float(zl)
        )
    except Exception:
        _closed_bar_touched = False

    try:
        _live_touched = bool(
            float(zl) <= float(px_live) <= float(zh)
        )
    except Exception:
        _live_touched = False

    touched = bool(_closed_bar_touched or _live_touched)

    if debug_gate:
        gate["touch_basis"] = {
            "live_px": float(px_live),
            "cl": float(cl),
            "lo": float(lo),
            "hi": float(hi),
            "zone_low": zl,
            "zone_high": zh,
            "zone_level": float(zone.get("level") or 0.0),
            "closed_bar_touched": bool(_closed_bar_touched),
            "live_touched": bool(_live_touched),
            "touched_now": bool(touched),
            "touch_method": "CLOSED_BAR_OR_LIVE_PRICE_VS_ZONE_BOUNDARIES",
        }

        if gate.get("dbg_h1_bars_n") is not None:
            gate["dbg_gate_h1_snap_bars_n"] = gate.get("dbg_h1_bars_n")
        if gate.get("dbg_h1_snap_serverNow") is not None:
            gate["dbg_gate_h1_snap_serverNow"] = gate.get("dbg_h1_snap_serverNow")

    # ------------------------------------------------------------
    # 5) start watch if not started yet
    # ------------------------------------------------------------
    # FREEZE GUARD: never re-enter the freeze block if a watch already exists.
    # If watch.started_ms is set, the zone was already frozen in a previous tick.
    # Re-entering would reset started_ms and wipe the invalidation clock.
    _watch_already_started = (
        isinstance(watch, dict)
        and bool(watch.get("started_ms"))
        and isinstance(watch.get("zone_used"), dict)
    )
    if zone_used is None and not _watch_already_started:
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

        try:
            import time as _t2
            _sys_now_touch = int(_t2.time() * 1000)
            _forming_open_ms = 0
            try:
                if isinstance(bars, list) and bars:
                    for _fb in reversed(bars):
                        _fb_t = _to_ms_any(
                            _fb.get("t_open_ms") or _fb.get("tOpenMs") or
                            _fb.get("open_time_ms") or _fb.get("t") or 0
                        )
                        if _fb_t and int(_fb_t) > 0 and int(_fb_t) <= _sys_now_touch:
                            _forming_open_ms = int(_fb_t)
                            break
            except Exception:
                _forming_open_ms = 0
            if _forming_open_ms > 0:
                touch_open_ms  = int(_forming_open_ms)
            else:
                touch_open_ms  = int((_sys_now_touch // tf_ms) * tf_ms)
            touch_close_ms = int(touch_open_ms + tf_ms)
        except Exception:
            touch_open_ms  = int((int(now_ms or now_ms_pick) // tf_ms) * tf_ms)
            touch_close_ms = int(touch_open_ms + tf_ms)

        # Extract touch candle open_ms from the closed bar for precise RC boundary.
        # RC candle must have opened at or after touch_candle_open_ms.
        # This rejects any candle already forming when the zone was frozen
        # (including the big drop/touch candle itself).
        _touch_bar_open_ms = 0
        try:
            for _ok in ("t_open_ms", "tOpenMs", "open_time_ms", "ts_ms", "t", "time", "ts"):
                _v = _to_ms_any((c or {}).get(_ok))
                if _v and int(_v) > 0:
                    _touch_bar_open_ms = int(_v)
                    break
        except Exception:
            _touch_bar_open_ms = int(touch_open_ms)

        # Fallback: use computed touch_open_ms if bar timestamp not found
        if not _touch_bar_open_ms:
            _touch_bar_open_ms = int(touch_open_ms)
        try:
            cd_key = _zone_cooldown_key(sym_u, resolved_dir, tfu)
            cd_raw = R.get(cd_key) if R is not None else None
            if cd_raw:
                ttl = R.ttl(cd_key)
                gate["reason"] = f"ZONE_COOLDOWN_AFTER_CLOSE | {ttl}s"
                gate["stage"] = "ZONE_COOLDOWN"
                gate["blocked"] = False
                gate["zone_cooldown_key"] = cd_key
                gate["zone_cooldown_ttl_sec"] = int(ttl or 0)
                gate["resolved_dir"] = resolved_dir
                return False, gate
        except Exception:
            pass

        watch = {
            "state": "WATCH",
            "started_ms": int(now_ms_pick),
            "touch_open_ms": int(touch_open_ms),
            "touch_close_ms": int(touch_close_ms),
            "touch_candle_open_ms": int(_touch_bar_open_ms),  # ← NEW: RC boundary
            "min_reclaim_close_ms": int(touch_close_ms),
            "direction": resolved_dir,
            "tf": tfu,
            "zone_used": zone_used,
            # Set to now_ms_pick so only FUTURE closed candles are evaluated
            # Prevents old closed candles from being used as RC on fresh watch
            "watch_created_ms": int(now_ms_pick or 0),
            "last_checked_closed_ms": int(now_ms_pick or 0),
            "touch_source": "LIVE_TOUCH",
        }
        try:
            R.set(wkey, json.dumps(watch, separators=(",", ":")), ex=7 * 24 * 3600)
        except Exception:
            pass
        gate["zone_used"] = zone_used
        gate["watch_key"] = str(wkey)
        gate["rev_state"] = {
            "state": "WATCH",
            "started_ms": int(watch.get("started_ms") or now_ms_pick),
            "touch_open_ms": int(watch.get("touch_open_ms") or 0),
            "touch_close_ms": int(watch.get("touch_close_ms") or 0),
            "min_reclaim_close_ms": int(watch.get("min_reclaim_close_ms") or 0),
            "direction": resolved_dir,
            "tf": tfu,
        }
        gate["reason"] = "REV_WATCH | LIVE_TOUCH_STARTED | WAIT_TOUCH_CANDLE_CLOSE"
        gate["stage"] = "WATCH"
        gate["blocked"] = False
        return False, gate
        
      

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
    # ------------------------------------------------------------
    # LOCK REV_OK:
    # REV_OK remains armed, but latest valid RC can refresh trigger.
    # This prevents stale/yesterday RC from staying locked forever.
    # ------------------------------------------------------------
    if isinstance(watch, dict) and bool(watch.get("rev_ok")) and str(watch.get("state") or "").upper() == "REV_OK":
        # ------------------------------------------------------------
        # LATEST RC ALWAYS WINS
        # If watch is already REV_OK, still allow a newer closed candle
        # to replace the old RC before returning RC_LOCKED.
        # ------------------------------------------------------------
        try:
            _cur_closed_ms = int(closed_ms or 0)
            _old_rev_ms = int(watch.get("rev_ok_ms") or 0)

            _newer_rc = False
            _rc_reject_reason = None
 
            if _cur_closed_ms > _old_rev_ms:
                _dir = str(watch.get("direction") or resolved_dir).upper()
         
                if _dir == "SELL":
                    # Latest SELL RC wins only if candle touched/entered resistance zone
                    # and closed back below zone low.
                    _newer_rc = bool(float(hi) >= float(zl) and float(cl) < float(zl))
                    if not _newer_rc:
                        _rc_reject_reason = {
                            "need": "SELL: hi>=zone_low and close<zone_low",
                            "hi": float(hi),
                            "close": float(cl),
                            "zone_low": float(zl),
                            "zone_high": float(zh),
                        }
                else:
                     # Latest BUY RC wins only if candle touched/entered support zone
                     # and closed back above zone high.
                     _newer_rc = bool(float(lo) <= float(zh) and float(cl) > float(zh))
                     if not _newer_rc:
                         _rc_reject_reason = {
                             "need": "BUY: lo<=zone_high and close>zone_high",
                             "lo": float(lo),
                             "close": float(cl),
                             "zone_low": float(zl),
                             "zone_high": float(zh),
                         }
            else:
                _rc_reject_reason = {
                    "need": "closed candle newer than stored RC",
                    "cur_closed_ms": int(_cur_closed_ms),
                    "old_rev_ok_ms": int(_old_rev_ms),
                }

            if debug_gate and _rc_reject_reason:
                gate["dbg_latest_rc_not_refreshed"] = _rc_reject_reason

            if _newer_rc:
                watch["state"] = "REV_OK"
                watch["rev_ok"] = True
                watch["rev_ok_ms"] = int(_cur_closed_ms)
                watch["last_checked_closed_ms"] = int(_cur_closed_ms)
                watch["last_checked_close"] = float(cl)
                watch["last_checked_high"] = float(hi)
                watch["last_checked_low"] = float(lo)

                watch["rev_ok_bar_hi"] = float(hi)
                watch["rev_ok_bar_lo"] = float(lo)
                watch["rev_ok_bar_close"] = float(cl)

                R.set(
                    wkey,
                    json.dumps(watch, separators=(",", ":")),
                    ex=7 * 24 * 3600,
                )

                gate["dbg_latest_rc_refreshed"] = {
                    "old_rev_ok_ms": int(_old_rev_ms),
                    "new_rev_ok_ms": int(_cur_closed_ms),
                    "close": float(cl),
                    "high": float(hi),
                    "low": float(lo),
                    "direction": str(watch.get("direction") or resolved_dir).upper(),
                }

        except Exception as e:
            if debug_gate:
                gate["dbg_latest_rc_refresh_exc"] = f"{type(e).__name__}:{e}"

        # ------------------------------------------------------------
        # RE-VALIDATE STORED RC CANDLE
        # Even though REV_OK is locked, verify the stored RC actually
        # touched the zone. If not → auto-rollback to REV_WATCH.
        # This runs on every tick so bad RCs are self-healing without
        # needing Redis deletes.
        # ------------------------------------------------------------
        _stored_rc_valid = True
        try:
            _stored_zl = float((watch.get("zone_used") or {}).get("low") or zl or 0)
            _stored_zh = float((watch.get("zone_used") or {}).get("high") or zh or 0)
            _stored_rc_hi = float(watch.get("rev_ok_bar_hi") or 0)
            _stored_rc_lo = float(watch.get("rev_ok_bar_lo") or 0)
            _stored_direction = str(watch.get("direction") or resolved_dir).upper()
            _stored_rc_ms = int(watch.get("rev_ok_ms") or 0)
            _stored_started_ms = int(watch.get("started_ms") or 0)
            _stored_watch_created_ms = int(watch.get("watch_created_ms") or _stored_started_ms or 0)

            if _stored_direction == "SELL":
                # RC candle high must have reached zone_low
                if _stored_rc_hi > 0 and _stored_zh > 0:
                    if _stored_rc_hi < _stored_zl:
                        _stored_rc_valid = False
            else:  # BUY
                
                # BUY RC candle low must have reached zone_high
                if _stored_rc_lo > 0 and _stored_zh > 0:
                    if _stored_rc_lo > _stored_zh:
                       _stored_rc_valid = False

            # RC candle close time must be after watch creation
            if _stored_rc_ms > 0 and _stored_watch_created_ms > 0:
                if _stored_rc_ms <= _stored_watch_created_ms:
                    _stored_rc_valid = False

        except Exception:
            _stored_rc_valid = True  # validation error — don't block

        if not _stored_rc_valid:
            # Auto-rollback — clear RC fields, roll back to REV_WATCH
            watch["state"] = "REV_WATCH"
            watch["rev_ok"] = False
            watch["rev_ok_ms"] = 0
            watch["rev_ok_bar_hi"] = None
            watch["rev_ok_bar_lo"] = None
            watch["rev_ok_bar_close"] = None
            try:
                _rollback_payload = {k: v for k, v in watch.items() if v is not None}
                _rollback_json = json.dumps(_rollback_payload, separators=(",", ":"))
                _set_ok = R.set(wkey, _rollback_json, ex=7 * 24 * 3600)

                if debug_gate:
                    gate["dbg_rc_rollback_persist"] = {
                        "wkey": str(wkey),
                        "set_ok": bool(_set_ok),
                        "state_written": _rollback_payload.get("state"),
                        "rev_ok_written": _rollback_payload.get("rev_ok"),
                        "rev_ok_ms_written": _rollback_payload.get("rev_ok_ms"),
                    }
            except Exception as e:
                if debug_gate:
                    gate["dbg_rc_rollback_persist_exc"] = f"{type(e).__name__}:{e}"
            if debug_gate:
                gate["dbg_rc_revalidation_rollback"] = {
                    "reason": "stored_rc_did_not_touch_zone",
                    "stored_rc_hi": float(_stored_rc_hi or 0),
                    "stored_rc_lo": float(_stored_rc_lo or 0),
                    "zone_low": _stored_zl,
                    "zone_high": _stored_zh,
                    "direction": _stored_direction,
                    "rolled_back_to": "REV_WATCH",
                }
            # Redis updated — return REV_WATCH cleanly
            # Next tick will re-evaluate with fresh closed candle
            gate["reason"] = (
                f"REV_WATCH | FZ {float(zl):.5f}-{float(zh):.5f}"
                f" | RC_INVALID_ROLLBACK | WAIT_VALID_RC"
                f" | TF={tfu}"
            )
            gate["stage"] = "REV_WATCH"
            gate["blocked"] = False
            gate["rev_ok"] = False
            gate["zone_used"] = zone_used
            return False, gate
                

        
        # ------------------------------------------------------------

        gate["zone_used"] = zone_used
        gate["rev_ok"] = True
        gate["watch_key"] = str(wkey)
        gate["rev_state"] = {
            "state": "REV_OK",
            "started_ms": int(watch.get("started_ms") or started_ms or now_ms_pick),
            "rev_ok_ms": int(watch.get("rev_ok_ms") or 0),
            "direction": str(watch.get("direction") or resolved_dir),
            "tf": str(watch.get("tf") or tfu),
            "rev_ok_bar_hi": float(watch.get("rev_ok_bar_hi") or 0.0),
            "rev_ok_bar_lo": float(watch.get("rev_ok_bar_lo") or 0.0),
            "rev_ok_bar_close": float(watch.get("rev_ok_bar_close") or watch.get("last_checked_close") or 0.0),
        }
        gate["rev_trigger"] = {
            "entry_above": float(watch.get("rev_ok_bar_hi") or 0.0),
            "entry_below": float(watch.get("rev_ok_bar_lo") or 0.0),
        }

        try:
            import datetime
            _tz_offset = datetime.timedelta(hours=-1)
            _freeze_dt = (datetime.datetime.utcfromtimestamp(
                int(watch.get("started_ms") or started_ms or 0) / 1000
            ) + _tz_offset).strftime("%m/%d %H:%M")
            _rc_dt = (datetime.datetime.utcfromtimestamp(
                int(watch.get("rev_ok_ms") or 0) / 1000
            ) + _tz_offset).strftime("%m/%d %H:%M")
        except Exception:
            _freeze_dt = "?"
            _rc_dt = "?"

        _w_dir = str(watch.get("direction") or resolved_dir).upper()
        gate["reason"] = (
            f"REV_OK | FZ {float(zl):.5f}-{float(zh):.5f} "
            f"| FREEZE@{_freeze_dt} | RC@{_rc_dt} "
            f"| RC {float(watch.get('rev_ok_bar_close') or watch.get('last_checked_close') or 0.0):.5f} "
            f"| ENTRY < {float(watch.get('rev_ok_bar_lo') or 0.0):.5f}"
            if _w_dir == "SELL"
            else
            f"REV_OK | FZ {float(zl):.5f}-{float(zh):.5f} "
            f"| FREEZE@{_freeze_dt} | RC@{_rc_dt} "
            f"| RC {float(watch.get('rev_ok_bar_close') or watch.get('last_checked_close') or 0.0):.5f} "
            f"| ENTRY > {float(watch.get('rev_ok_bar_hi') or 0.0):.5f}"
        )
        gate["reason"] = f"{gate['reason']} | RC_LOCKED | LIVE_BREAKOUT_ONLY | TF={tfu}"
        gate["stage"] = "REV_LOCKED"
        gate["blocked"] = False

        # Check invalidation even in REV_OK state
        # Same logic as main invalidation — trust complete=True
        try:
            import time as _t5
            _sys_now_roi = int(_t5.time() * 1000)
            _freeze_roi = int(watch.get("started_ms") or 0)
            _inv_consec = 0
            _bs_roi = sorted(
                [b for b in (bars or []) if isinstance(b, dict)],
                key=lambda b: _to_ms_any(b.get("t_open_ms") or b.get("tOpenMs") or
                                          b.get("open_time_ms") or b.get("t") or 0) or 0
            )
            for _cb in reversed(_bs_roi):
                _om_roi = _to_ms_any(
                    _cb.get("t_open_ms") or _cb.get("tOpenMs") or
                    _cb.get("open_time_ms") or _cb.get("t") or 0
                )
                if not _om_roi or int(_om_roi) <= int(_freeze_roi or 0):
                    break
                _is_comp = _cb.get("complete") is True
                _clk_roi = (int(_om_roi) + int(tf_ms)) <= _sys_now_roi
                if not _is_comp and not _clk_roi:
                    continue
                _cv = _bar_f(_cb, "c", "close")
                if _cv is None:
                    break
                _cv = float(_cv)
                if resolved_dir == "SELL":
                    _bad = _cv >= float(zh)
                else:
                    _bad = _cv <= float(zl)
                if _bad:
                    _inv_consec += 1
                else:
                    break
                if _inv_consec >= int(hard_close_bars):
                    break
            if _inv_consec >= int(hard_close_bars):
                try:
                    for _s in ("BUY", "SELL"):
                        for _t in ("H1", "H4"):
                            R.delete(f"xtl:zone:watch:{sym_u}:{_s}:{_t}")
                except Exception:
                    pass
                gate["reason"] = f"INVALIDATED | REV_OK_CANCELLED | {_inv_consec} closes beyond zone | FZ {float(zl):.5f}-{float(zh):.5f}"
                gate["stage"] = "INVALIDATED"
                gate["blocked"] = True
                gate["rev_ok"] = False
                return False, gate
        except Exception:
            pass

        return True, gate

   
   
    # ------------------------------------------------------------
    # PHASE-1 FIX:
    # A frozen REV_WATCH must NOT stay stuck on an old candle.
    # Every new CLOSED candle must be evaluated against frozen zone.
    # IMPORTANT: only update last_checked fields — never touch
    # rev_ok / state / zone_used / rev_ok_bar_* here.
    # REV_OK state is written only in the rev_ok confirmation block below.
    # ------------------------------------------------------------
    try:
        last_checked_ms = int((watch or {}).get("last_checked_closed_ms") or 0)
    except Exception:
        last_checked_ms = 0

    # Only write candle refresh if this is genuinely a new closed candle
    # and the watch is NOT already in REV_OK state (REV_OK lock handles its own write)
    _watch_is_rev_ok = (
        isinstance(watch, dict)
        and bool(watch.get("rev_ok"))
        and str(watch.get("state") or "").upper() == "REV_OK"
    )
    if not _watch_is_rev_ok:
        try:
            if isinstance(watch, dict) and int(closed_ms or 0) >= int(last_checked_ms or 0):
                watch["last_checked_closed_ms"] = int(closed_ms or 0)
                watch["last_checked_close"] = float(cl)
                watch["last_checked_high"] = float(hi)
                watch["last_checked_low"] = float(lo)
                R.set(wkey, json.dumps(watch, separators=(",", ":")), ex=7 * 24 * 3600)
        except Exception:
            pass
    if debug_gate:
        gate["dbg_watch_candle_refresh"] = {
            "last_checked_ms_before": int(last_checked_ms),
            "current_closed_ms": int(closed_ms or 0),
            "new_closed_candle": bool(int(closed_ms or 0) > int(last_checked_ms or 0)),
            "current_close": float(cl),
        }

    

    if debug_gate:
        gate["rev_basis"] = {
            "closed_ms": int(closed_ms),
            "started_ms": int(started_ms),
            "cl": float(cl),
            "zl": float(zl),
            "zh": float(zh),
        }
    try:
        min_reclaim_close_ms = int((watch or {}).get("min_reclaim_close_ms") or 0)
    except Exception:
        min_reclaim_close_ms = 0
    # HARD RULE:
    # Old closed candle must never become RC after live touch.
    # BUT the same touch candle is allowed as RC if it closes reclaiming the zone.
    touch_close_ms = int((watch or {}).get("touch_close_ms") or 0)
    same_touch_candle = bool(
        touch_close_ms > 0
        and int(closed_ms or 0) == int(touch_close_ms)
    )

    if int(closed_ms or 0) <= int(started_ms or 0) and not same_touch_candle:
        gate["reason"] = "REV_WATCH | WAIT_TOUCH_CANDLE_CLOSE"
        gate["stage"] = "WATCH"
        gate["blocked"] = False
        gate["zone_used"] = zone_used
        gate["watch_key"] = str(wkey)
        gate["rev_state"] = {
            "state": "WATCH",
            "started_ms": int(started_ms or 0),
            "touch_close_ms": int(touch_close_ms or 0),
            "same_touch_candle": bool(same_touch_candle),
            "current_closed_ms": int(closed_ms or 0),
            "min_reclaim_close_ms": int(min_reclaim_close_ms or 0),
            "direction": resolved_dir,
            "tf": tfu,
        }
        return False, gate

    if min_reclaim_close_ms > 0 and int(closed_ms or 0) < int(min_reclaim_close_ms):
        gate["reason"] = "REV_WATCH | WAIT_TOUCH_CANDLE_CLOSE"
        gate["stage"] = "WATCH"
        gate["blocked"] = False
        gate["zone_used"] = zone_used
        gate["watch_key"] = str(wkey)
        gate["rev_state"] = {
            "state": "WATCH",
            "started_ms": int(started_ms or 0),
            "touch_open_ms": int((watch or {}).get("touch_open_ms") or 0),
            "touch_close_ms": int((watch or {}).get("touch_close_ms") or 0),
            "min_reclaim_close_ms": int(min_reclaim_close_ms),
            "current_closed_ms": int(closed_ms or 0),
            "direction": resolved_dir,
            "tf": tfu,
        }
        return False, gate

    # RC CANDLE HARD RULES:
    # 1. Same touch candle CAN become RC if it closes reclaiming the frozen zone.
    # 2. Older candles before touch/freeze must never become RC.
    # 3. Candle must be closed (complete=True OR open+tf_ms <= sys_now).
    # 4. Candle close must be >= min_reclaim_close_ms unless it is same_touch_candle.
    _bar_open_ms = 0
    try:
        for _ok in ("t_open_ms", "tOpenMs", "open_time_ms", "ts_ms", "t", "time", "ts"):
            _v = _to_ms_any((c or {}).get(_ok))
            if _v and int(_v) > 0:
                _bar_open_ms = int(_v)
                break
    except Exception:
        _bar_open_ms = 0

    import time as _t3
    _sys_now_rc = int(_t3.time() * 1000)
    _is_complete_rc = (c or {}).get("complete") is True
    _closed_by_clock_rc = (_bar_open_ms > 0 and (_bar_open_ms + int(tf_ms)) <= _sys_now_rc)
    _bar_is_closed = _is_complete_rc or _closed_by_clock_rc
    _min_reclaim = int((watch or {}).get("min_reclaim_close_ms") or 0)
    _watch_created = int((watch or {}).get("watch_created_ms") or started_ms or 0)

    _touch_close_ms_for_rc = int((watch or {}).get("touch_close_ms") or 0)
    _same_touch_candle_for_rc = bool(
        _touch_close_ms_for_rc > 0
        and int(closed_ms or 0) == int(_touch_close_ms_for_rc)
    )

    _rc_time_valid = (
        _bar_is_closed
        and _bar_open_ms > 0
        and int(closed_ms or 0) >= int(_touch_close_ms_for_rc or 0)
        and (
            _same_touch_candle_for_rc
            or int(closed_ms or 0) > int(_watch_created or 0)
        )
        and (_min_reclaim <= 0 or int(closed_ms or 0) >= int(_min_reclaim))
    )
    # Scan ALL closed bars after freeze for RC — not just last picked bar
    # This finds the FIRST bar after freeze that meets RC condition
    import time as _t6
    _sys_now_scan = int(_t6.time() * 1000)
    _rc_bar = None
    _rc_bar_close = None
    _rc_bar_open_ms = 0
    _rc_bar_closed_ms = 0
    try:
        _bs_scan = sorted(
            [b for b in (bars or []) if isinstance(b, dict)],
            key=lambda b: _to_ms_any(b.get("t_open_ms") or b.get("tOpenMs") or
                                      b.get("open_time_ms") or b.get("t") or 0) or 0
        )
        for _sb in _bs_scan:
            _sb_om = _to_ms_any(
                _sb.get("t_open_ms") or _sb.get("tOpenMs") or
                _sb.get("open_time_ms") or _sb.get("t") or 0
            )
            if not _sb_om or int(_sb_om) <= 0:
                continue
            # Same-touch candle is valid:
            # its open can be BEFORE freeze, but its close must be the touch_close_ms.
            _sb_cm = int(_sb_om) + int(tf_ms)
            _touch_close_ms_scan = int((watch or {}).get("touch_close_ms") or 0)
            _same_touch_scan = bool(
                _touch_close_ms_scan > 0
                and int(_sb_cm) == int(_touch_close_ms_scan)
            )

            # For later RC candles, require open after freeze.
            # For same-touch candle, allow open before freeze.
            if not _same_touch_scan and int(_sb_om) <= int(started_ms or 0):
                continue

            # Must be closed
            _sb_comp = _sb.get("complete") is True
            _sb_clk = (int(_sb_om) + int(tf_ms)) <= _sys_now_scan
            if not _sb_comp and not _sb_clk:
                continue

            # Must close >= min_reclaim
            if _min_reclaim > 0 and _sb_cm < int(_min_reclaim):
                continue
            _sb_cl = _bar_f(_sb, "c", "close")
            if _sb_cl is None:
                continue
            _sb_cl = float(_sb_cl)
            if resolved_dir == "BUY" and _sb_cl > float(zh):
                _rc_bar = _sb
                _rc_bar_close = _sb_cl
                _rc_bar_open_ms = int(_sb_om)
                _rc_bar_closed_ms = _sb_cm
                break
            elif resolved_dir == "SELL" and _sb_cl < float(zl):
                _rc_bar = _sb
                _rc_bar_close = _sb_cl
                _rc_bar_open_ms = int(_sb_om)
                _rc_bar_closed_ms = _sb_cm
                break
    except Exception:
        _rc_bar = None

    if _rc_bar is not None:
        rev_ok = True
        # Override c, cl, hi, lo, closed_ms with RC bar values
        c = _rc_bar
        cl = _rc_bar_close
        hi = float(_bar_f(_rc_bar, "h", "high") or 0)
        lo = float(_bar_f(_rc_bar, "l", "low") or 0)
        closed_ms = _rc_bar_closed_ms
    elif resolved_dir == "BUY":
        rev_ok = bool(_rc_time_valid and float(cl) > float(zh))
    else:
        rev_ok = bool(_rc_time_valid and float(cl) < float(zl))

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
                watch["last_checked_closed_ms"] = int(closed_ms or 0)
                watch["last_checked_close"] = float(cl)

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
                try:
                    watch["rev_ok_bar_close"] = float(cl)
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
            import datetime
            _freeze_dt = datetime.datetime.utcfromtimestamp(
                int((watch or {}).get("started_ms") or now_ms_pick or 0) / 1000
            ).strftime("%m/%d %H:%M")
            _rc_dt = datetime.datetime.utcfromtimestamp(
                int(closed_ms or 0) / 1000
            ).strftime("%m/%d %H:%M")
        except Exception:
            _freeze_dt = "?"
            _rc_dt = "?"

        try:
            _fzl = float((watch or {}).get("frozen_zone_low", zl))
            _fzh = float((watch or {}).get("frozen_zone_high", zh))
            if resolved_dir == "BUY":
                gate["reason"] = (
                    f"REV_OK | FZ {_fzl:.5f}-{_fzh:.5f} "
                    f"| FREEZE@{_freeze_dt} | RC@{_rc_dt} "
                    f"| RC {float(cl):.5f} "
                    f"| ENTRY > {float(hi):.5f}"
                )
            else:
                gate["reason"] = (
                    f"REV_OK | FZ {_fzl:.5f}-{_fzh:.5f} "
                    f"| FREEZE@{_freeze_dt} | RC@{_rc_dt} "
                    f"| RC {float(cl):.5f} "
                    f"| ENTRY < {float(lo):.5f}"
                )
        except Exception:
            gate["reason"] = "REV_OK"
        gate["reason"] = (
            f"{gate.get('reason')} "
            f"| RC_LOCKED | LIVE_BREAKOUT_ONLY "
            f"| TF={tfu}"
        )
        gate["stage"] = "REV"
        gate["blocked"] = False
        return True, gate




    
    
    
    # 6) INVALIDATION: 2 consecutive closed candles beyond zone boundary
    # Uses complete=True as primary signal, clock as fallback
    # Only counts candles that opened AFTER freeze (started_ms)
    consec = 0
    try:
        import time as _t4
        _sys_now_inv = int(_t4.time() * 1000)
        _freeze_ms_inv = int((watch or {}).get("started_ms") or 0)
        _bs_inv = sorted(
            [b for b in (bars or []) if isinstance(b, dict)],
            key=lambda b: _to_ms_any(b.get("t_open_ms") or b.get("tOpenMs") or
                                      b.get("open_time_ms") or b.get("t") or 0) or 0
        )
        for _cb in reversed(_bs_inv):
            _om_inv = _to_ms_any(
                _cb.get("t_open_ms") or _cb.get("tOpenMs") or
                _cb.get("open_time_ms") or _cb.get("t") or 0
            )
            if not _om_inv or int(_om_inv) <= 0:
                continue
            # Only bars opened AFTER freeze
            if int(_om_inv) <= int(_freeze_ms_inv or 0):
                break
            # Bar must be closed
            _is_comp_inv = _cb.get("complete") is True
            _clk_inv = (int(_om_inv) + int(tf_ms)) <= _sys_now_inv
            if not _is_comp_inv and not _clk_inv:
                continue
            _cv_inv = _bar_f(_cb, "c", "close")
            if _cv_inv is None:
                break
            _cv_inv = float(_cv_inv)
            if resolved_dir == "SELL":
                _bad_inv = _cv_inv >= float(zh)
            else:
                _bad_inv = _cv_inv <= float(zl)
            if _bad_inv:
                consec += 1
            else:
                break
            if consec >= int(hard_close_bars):
                break
    except Exception:
        consec = 0

    if consec >= int(hard_close_bars):
        try:
            for _s in ("BUY", "SELL"):
                for _t in ("H1", "H4"):
                    R.delete(f"xtl:zone:watch:{sym_u}:{_s}:{_t}")
        except Exception:
            pass
        gate["reason"] = (
            f"ZONE_INVALIDATED | FZ {float(zl):.5f}-{float(zh):.5f}"
            f" | {consec}/{int(hard_close_bars)} closes beyond zone | TF={tfu}"
        )
        gate["stage"] = "ZONE_INVALIDATED"
        gate["blocked"] = True
        gate["rev_ok"] = False
        gate["zone_used"] = zone_used
        return False, gate

    # If we reach here: watch active but not rev_ok and not invalidated
    try:
        import datetime
        _freeze_dt = datetime.datetime.utcfromtimestamp(
            int((watch or {}).get("started_ms") or started_ms or 0) / 1000
        ).strftime("%m/%d %H:%M")
        _candle_dt = datetime.datetime.utcfromtimestamp(
            int(closed_ms or 0) / 1000
        ).strftime("%m/%d %H:%M")
        _watch_created_dt = datetime.datetime.utcfromtimestamp(
            int((watch or {}).get("watch_created_ms") or 0) / 1000
        ).strftime("%m/%d %H:%M")
        gate["reason"] = (
            f"REV_WATCH | FZ {float(zl):.5f}-{float(zh):.5f} "
            f"| FREEZE@{_freeze_dt} | CREATED@{_watch_created_dt} "
            f"| C@{_candle_dt} {float(cl):.5f} "
            f"| NEED > {float(zh):.5f}"
            if resolved_dir == "BUY"
            else
            f"REV_WATCH | FZ {float(zl):.5f}-{float(zh):.5f} "
            f"| FREEZE@{_freeze_dt} | CREATED@{_watch_created_dt} "
            f"| C@{_candle_dt} {float(cl):.5f} "
            f"| NEED < {float(zl):.5f}"
        )
    except Exception:
        gate["reason"] = "REV_WATCH"
    gate["stage"] = "WATCH"
    gate["blocked"] = False
    return False, gate
