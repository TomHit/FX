#!/usr/bin/env python3
"""
Zone-watch staleness AUDIT — READ ONLY. Deletes nothing.

Scans every xtl:zone:watch:{SYM}:{SIDE}:{TF} key and reports, per key:
  state, frozen zone band, current price, price position vs band,
  distance to band, the symbol hard-cap, age, TTL, and the eviction
  verdict each proposed rule WOULD produce.

Run on the server with the app's venv so redis-py is available:
    /opt/xauapi/.venv/bin/python audit_zone_watches.py

Connection + password are read from env (REDIS_PASSWORD) with sane
defaults; adjust HOST/PORT/DB below if your Redis differs.

NOTHING is modified. To actually evict later we build a separate,
reviewed step — this script has no delete path at all.
"""

import os, sys, time, json

# ---- connection (adjust if needed) ----
HOST = os.getenv("REDIS_HOST", "127.0.0.1")
PORT = int(os.getenv("REDIS_PORT", "6379"))
DB   = int(os.getenv("REDIS_DB", "0"))
PWD  = os.getenv("REDIS_PASSWORD", "xau12345")

# ---- proposed rule parameters (TUNABLE — set from what this audit shows) ----
# Rule 1: hard-cap eviction. Absolute actionable ceiling per symbol class.
def hard_cap_for(sym_u: str) -> float:
    if sym_u == "XAUUSD":
        return 12.0
    if sym_u.endswith("JPY"):
        return 0.25
    return 0.0025  # 25 pips for 5-dp FX

# Rule 2: stale-by-age. REPORT ONLY for now (no threshold committed).
#   We print age so we can choose this number from real data.
MAX_AGE_HOURS_REPORT = 24  # purely for the "age>?" flag column; not an eviction decision yet

# Rule 4: states with skin in the game — NEVER eligible for distance/age eviction.
PROTECTED_STATES = {"ENTRY_READY", "ORDER_PENDING", "TRADE_ACTIVE"}
# States eligible for distance eviction under Rule 1:
EVICTABLE_STATES = {"WATCH", "REV_WATCH"}
# REV_OK is a judgement call (reversal confirmed, no order yet) — reported separately.
REVIEW_STATES = {"REV_OK"}

try:
    import redis
except Exception as e:
    print("redis-py not importable. Run with the app venv, e.g.:")
    print("  /opt/xauapi/.venv/bin/python audit_zone_watches.py")
    sys.exit(1)

R = redis.Redis(host=HOST, port=PORT, db=DB, password=PWD, decode_responses=True)

try:
    R.ping()
except Exception as e:
    print(f"Cannot connect to Redis at {HOST}:{PORT}/{DB}: {e}")
    sys.exit(1)

now_ms = int(time.time() * 1000)


def _to_float(x):
    try:
        return float(x)
    except Exception:
        return None


def _price_for(sym_u: str):
    """Best-effort current price for a symbol. Mirrors the endpoint's key
    preferences: global keys first, then any device-scoped key, freshest ts."""
    candidates = [f"xtl:price:{sym_u}", f"xtl:live:{sym_u}", f"xtl:tick:{sym_u}"]
    best_px, best_ts = None, None

    def _parse(raw):
        if raw is None:
            return None, None
        # try JSON dict
        try:
            obj = json.loads(raw)
        except Exception:
            obj = None
        if isinstance(obj, dict):
            px = None
            for k in ("price", "p", "bid", "ask", "mid", "c", "close", "last", "value"):
                if obj.get(k) is not None:
                    px = _to_float(obj.get(k)); 
                    if px is not None:
                        break
            ts = None
            for k in ("ts_ms", "t_ms", "ts", "time", "updated_ms"):
                if obj.get(k) is not None:
                    ts = _to_float(obj.get(k)); 
                    if ts is not None:
                        ts = int(ts); break
            return px, ts
        # raw scalar
        return _to_float(raw), None

    for k in candidates:
        try:
            px, ts = _parse(R.get(k))
        except Exception:
            px, ts = None, None
        if px is not None:
            if ts is None:
                if best_px is None:
                    best_px = px
            elif best_ts is None or ts > best_ts:
                best_px, best_ts = px, ts

    # device-scoped scan fallback
    if best_px is None:
        for pat in (f"xtl:price:*:{sym_u}", f"xtl:tick:*:{sym_u}"):
            try:
                for key in R.scan_iter(match=pat, count=200):
                    px, ts = _parse(R.get(key))
                    if px is None:
                        continue
                    if ts is None:
                        if best_px is None:
                            best_px = px
                    elif best_ts is None or ts > best_ts:
                        best_px, best_ts = px, ts
            except Exception:
                pass
            if best_px is not None:
                break
    return best_px, best_ts


def _band(zone_used):
    """Return (low, high) from a zone_used dict (band or level-only)."""
    if not isinstance(zone_used, dict):
        return None, None
    lo = _to_float(zone_used.get("low"))
    hi = _to_float(zone_used.get("high"))
    if lo is not None and hi is not None:
        return min(lo, hi), max(lo, hi)
    lv = _to_float(zone_used.get("level"))
    if lv is not None:
        return lv, lv
    return None, None


def _age_ms(w):
    for k in ("created_ms", "started_ms", "updated_ms", "ts_ms", "last_checked_ms"):
        v = _to_float(w.get(k))
        if v:
            return now_ms - int(v)
    return None


rows = []
n_keys = 0
for key in R.scan_iter(match="xtl:zone:watch:*", count=200):
    n_keys += 1
    parts = key.split(":")
    # xtl:zone:watch:SYM:SIDE:TF
    sym = parts[3] if len(parts) > 3 else "?"
    side = parts[4] if len(parts) > 4 else "?"
    tf = parts[5] if len(parts) > 5 else "?"
    try:
        raw = R.get(key)
        w = json.loads(raw) if raw else {}
    except Exception:
        w = {}
    if not isinstance(w, dict):
        w = {}

    state = str(w.get("state") or "").upper()
    lo, hi = _band(w.get("zone_used"))
    px, _ = _price_for(sym)
    try:
        ttl = R.ttl(key)
    except Exception:
        ttl = None
    age = _age_ms(w)

    # distance from price to band (0 if inside)
    dist = None
    pos = "?"
    if px is not None and lo is not None and hi is not None:
        if px < lo:
            dist = lo - px; pos = "BELOW"
        elif px > hi:
            dist = px - hi; pos = "ABOVE"
        else:
            dist = 0.0; pos = "INSIDE"

    cap = hard_cap_for(sym)

    # verdicts (NO ACTION — labels only)
    protected = state in PROTECTED_STATES
    review = state in REVIEW_STATES
    evictable = state in EVICTABLE_STATES
    r1_too_far = (evictable and dist is not None and dist > cap)
    age_flag = (age is not None and age > MAX_AGE_HOURS_REPORT * 3600 * 1000)

    if protected:
        verdict = "KEEP(protected)"
    elif r1_too_far:
        verdict = "WOULD_EVICT(too_far)"
    elif review and dist is not None and dist > cap:
        verdict = "REVIEW(REV_OK_too_far)"
    elif evictable:
        verdict = "keep(near)"
    else:
        verdict = f"keep({state or 'no_state'})"

    rows.append({
        "key": key, "sym": sym, "side": side, "tf": tf, "state": state or "-",
        "lo": lo, "hi": hi, "px": px, "pos": pos, "dist": dist, "cap": cap,
        "ttl": ttl, "age_h": (age / 3600000.0 if age is not None else None),
        "age_flag": age_flag, "verdict": verdict,
    })

# ---- report ----
def fnum(x, nd=3):
    return f"{x:.{nd}f}" if isinstance(x, (int, float)) else "-"

rows.sort(key=lambda r: (r["verdict"] != "WOULD_EVICT(too_far)", r["sym"]))

print(f"\nScanned {n_keys} watch key(s) at {HOST}:{PORT}/{DB}\n")
hdr = f"{'SYM':8} {'SIDE':5} {'TF':3} {'STATE':12} {'ZONE_LOW':>10} {'ZONE_HIGH':>10} {'PRICE':>10} {'POS':6} {'DIST':>9} {'CAP':>7} {'AGE_h':>7} {'TTL_s':>9}  VERDICT"
print(hdr)
print("-" * len(hdr))
n_evict = 0
for r in rows:
    if r["verdict"].startswith("WOULD_EVICT"):
        n_evict += 1
    print(f"{r['sym']:8} {r['side']:5} {r['tf']:3} {r['state']:12} "
          f"{fnum(r['lo']):>10} {fnum(r['hi']):>10} {fnum(r['px']):>10} {r['pos']:6} "
          f"{fnum(r['dist']):>9} {fnum(r['cap']):>7} "
          f"{(fnum(r['age_h'],1)):>7} {str(r['ttl']):>9}  {r['verdict']}"
          + ("  [age>%dh]" % MAX_AGE_HOURS_REPORT if r["age_flag"] else ""))

print("\n--- summary ---")
print(f"  total watches      : {n_keys}")
print(f"  WOULD_EVICT(too_far): {n_evict}")
print(f"  protected (skipped): {sum(1 for r in rows if r['verdict']=='KEEP(protected)')}")
print(f"  REV_OK review       : {sum(1 for r in rows if r['verdict'].startswith('REVIEW'))}")
print(f"  price-unknown       : {sum(1 for r in rows if r['px'] is None)}  (distance rules can't judge these)")
print("\nNOTHING was modified. This is an audit only.")
