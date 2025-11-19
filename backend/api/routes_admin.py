# api/routes_admin.py
from fastapi import APIRouter, HTTPException, Query
from typing import Literal, List, Dict, Any, Optional
import os, time, math, json
import httpx
import redis

r = APIRouter()  # main.py mounts this with require_admin already

TF_SEC = {"M15": 15 * 60, "H1": 60 * 60, "H4": 4 * 60 * 60}

def _redis():
    url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    return redis.Redis.from_url(url, decode_responses=True)

def _round_to_digits(x: float, digits: int) -> float:
    scale = 10 ** digits
    return math.floor(float(x) * scale + 0.5) / scale

async def _fetch_broker_bars(symbol: str, tf: str, limit: int, price: str = "bid") -> List[Dict[str, Any]]:
    """
    Ask the agent for 'ground truth' bars.
    Expect agent endpoint:
        GET /broker/ohlc?symbol=...&tf=...&limit=...&price=bid
    Response:
        [{"t": <utc seconds>, "o":..,"h":..,"l":..,"c":..}, ...]
    """
    base = os.getenv("AGENT_BASE_URL", "").rstrip("/")
    if not base:
        raise HTTPException(status_code=424, detail="AGENT_BASE_URL not configured")
    url = f"{base}/broker/ohlc"
    async with httpx.AsyncClient(timeout=15) as cli:
        res = await cli.get(url, params={"symbol": symbol, "tf": tf, "limit": limit, "price": price})
        if res.status_code != 200:
            raise HTTPException(status_code=502, detail=f"Agent fetch failed: {res.status_code} {res.text[:160]}")
        js = res.json()
        if not isinstance(js, list):
            raise HTTPException(status_code=502, detail="Agent returned non-list payload")
        return js

def _load_app_bars(symbol: str, tf: str, limit: int) -> List[Dict[str, Any]]:
    """
    Read what the UI consumes from Redis snapshot (snap/last).
    """
    rd = _redis()
    patterns = [f"xtl:trend:snap:*:{symbol}:{tf}", f"xtl:trend:last:{symbol}:{tf}"]
    snap = None
    for pat in patterns:
        keys = rd.keys(pat)
        if not keys:
            continue
        raw = rd.get(keys[0])
        if raw:
            try:
                snap = json.loads(raw)
                break
            except Exception:
                continue
    if not snap or "bars" not in snap:
        return []
    tf_sec = TF_SEC[tf]
    now_slot = (int(time.time()) // tf_sec) * tf_sec
    out = []
    for b in snap["bars"][-(limit+5):]:
        t = int(b.get("t", 0))
        if t >= now_slot:
            continue  # drop forming
        if (t % tf_sec) != 0:
            continue  # enforce grid
        out.append({
            "t": t,
            "o": float(b["o"]), "h": float(b["h"]), "l": float(b["l"]), "c": float(b["c"])
        })
    return out[-limit:]

@r.get("/admin/compare_ohlc")
async def compare_ohlc(
    symbol: str = Query("XAUUSD"),
    tf: Literal["M15","H1","H4"] = Query("M15"),
    n: int = Query(50, ge=5, le=500),
    price: Literal["bid","ask","mid"] = Query("bid"),
    digits: Optional[int] = Query(None),
):
    tf_sec = TF_SEC[tf]

    # 1) Broker (ground truth)
    broker = await _fetch_broker_bars(symbol, tf, n + 5, price=price)
    now_slot = (int(time.time()) // tf_sec) * tf_sec
    broker = [b for b in broker if int(b["t"]) < now_slot and int(b["t"]) % tf_sec == 0][-n:]
    if not broker:
        raise HTTPException(status_code=424, detail="No broker bars fetched")

    # 2) App
    app = _load_app_bars(symbol, tf, n)
    if not app:
        raise HTTPException(status_code=424, detail="No app bars available")

    # 3) Decide digits if not provided
    if digits is None:
        # infer from broker last close
        sample = str(broker[-1]["c"])
        after = sample.split(".")[1] if "." in sample else ""
        digits = min(max(len(after), 2), 6)

    def norm(bs):
        out = []
        for b in bs:
            out.append({
                "t": int(b["t"]),
                "o": _round_to_digits(b["o"], digits),
                "h": _round_to_digits(b["h"], digits),
                "l": _round_to_digits(b["l"], digits),
                "c": _round_to_digits(b["c"], digits),
            })
        return out

    bN, aN = norm(broker), norm(app)

    # 4) Compare by timestamp (closed bars only)
    b_idx = {b["t"]: b for b in bN}
    diffs = []
    matched = 0
    missing_in_app = 0
    missing_in_broker = 0

    for t, bb in b_idx.items():
        aa = next((x for x in aN if x["t"] == t), None)
        if not aa:
            missing_in_app += 1
            continue
        unequal = []
        for k in ("o","h","l","c"):
            if aa[k] != bb[k]:
                unequal.append({"field": k, "app": aa[k], "broker": bb[k]})
        if unequal:
            diffs.append({"t": t, "diffs": unequal})
        else:
            matched += 1

    # extra bars present in app
    a_idx = {a["t"]: a for a in aN}
    for t in a_idx.keys():
        if t not in b_idx:
            missing_in_broker += 1

    return {
        "symbol": symbol,
        "tf": tf,
        "tfSec": tf_sec,
        "price": price,
        "digits": digits,
        "checked": len(bN),
        "matched": matched,
        "mismatched": len(diffs),
        "missingInApp": missing_in_app,
        "missingInBroker": missing_in_broker,
        "diffs": diffs[:100],
    }
