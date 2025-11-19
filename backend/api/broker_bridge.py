# broker_bridge.py
import time, math
from typing import List, Dict
from fastapi import FastAPI, HTTPException, Query
from MetaTrader5 import MT5Initialize, MT5Shutdown, copy_rates_from_pos, TIMEFRAME_M15, TIMEFRAME_H1, TIMEFRAME_H4
import MetaTrader5 as mt5

app = FastAPI(title="XTL Broker Bridge")

TF_MAP = {"M15": TIMEFRAME_M15, "H1": TIMEFRAME_H1, "H4": TIMEFRAME_H4}
TF_SEC = {"M15": 15*60, "H1": 60*60, "H4": 4*60*60}

def round_digits(x: float, digits: int) -> float:
    scale = 10 ** digits
    return math.floor(float(x) * scale + 0.5) / scale

def infer_digits(symbol: str) -> int:
    info = mt5.symbol_info(symbol)
    if info and hasattr(info, "digits"):
        return int(info.digits)
    return 2

@app.on_event("startup")
def _up():
    if not mt5.initialize():
        raise RuntimeError("MT5Initialize failed")

@app.on_event("shutdown")
def _down():
    try:
        mt5.shutdown()
    except Exception:
        pass

@app.get("/broker/ohlc")
def broker_ohlc(
    symbol: str = Query("XAUUSD"),
    tf: str = Query("M15"),
    limit: int = Query(300, ge=1, le=1000),
    price: str = Query("bid")  # kept for compatibility; MT5 “rates” are bid-based
) -> List[Dict]:
    tf = tf.upper()
    if tf not in TF_MAP:
        raise HTTPException(400, f"unsupported tf {tf}")

    rates = copy_rates_from_pos(symbol, TF_MAP[tf], 0, limit + 5)
    if rates is None or len(rates) == 0:
        raise HTTPException(424, "no rates from MT5")

    tf_sec = TF_SEC[tf]
    now_slot = int(time.time() // tf_sec) * tf_sec

    # normalize: drop forming bar, enforce grid, map to {t,o,h,l,c}
    out = []
    for r in rates:
        t = int(r['time'])
        if t >= now_slot:            # drop current forming
            continue
        if (t % tf_sec) != 0:        # enforce clean TF grid
            continue
        out.append({
            "t": t,
            "o": float(r['open']),
            "h": float(r['high']),
            "l": float(r['low']),
            "c": float(r['close']),
        })

    # digits normalization (optional, server also rounds)
    digits = infer_digits(symbol)
    for b in out:
        for k in ("o","h","l","c"):
            b[k] = round_digits(b[k], digits)

    # keep last N only
    return out[-limit:]
