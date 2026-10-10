"""
XauTrendLab Strategy B - Live Symbol Snapshot Loader (V2)
=========================================================

Observation-only acquisition boundary for Strategy-B symbol expression analysis.

Production acquisition path:
    xtl:ohlc:latest:{SYMBOL}:{TF}
        -> device_id
    xtl:ohlc:snap:{device_id}:{SYMBOL}:{TF}
        -> native D1/H4/H1 bars

This matches the existing Agent -> Redis OHLC architecture.

Responsibilities:
- exact Redis GET only (NO SCAN)
- load native D1/H4/H1 broker-pushed snapshots
- read canonical symbol SR bundle by exact Redis key
- pass already-loaded inputs to the pure Strategy-B symbol snapshot adapter

Does NOT:
- call AGENT_BASE_URL / _broker_bars_sync
- compute a second SR model
- synthesize D1 from H4
- mutate Strategy A
- reserve risk
- place/modify/close broker orders
- decide ENTRY_READY
"""
from __future__ import annotations

import json
import time
from typing import Any, Callable, Mapping, Optional


def _upper(v: Any) -> str:
    return str(v or "").strip().upper()


def _num(v: Any) -> Optional[float]:
    try:
        x = float(v)
        return x if x == x else None
    except Exception:
        return None


def _decode(v: Any) -> Any:
    if isinstance(v, (bytes, bytearray)):
        return v.decode("utf-8", "replace")
    return v


def _json_obj(raw: Any) -> Optional[dict[str, Any]]:
    if raw is None:
        return None
    if isinstance(raw, Mapping):
        return dict(raw)
    raw = _decode(raw)
    try:
        obj = json.loads(raw)
    except Exception:
        return None
    return dict(obj) if isinstance(obj, Mapping) else None


def _last_bar_close(bars: Any) -> Optional[float]:
    if not isinstance(bars, list) or not bars:
        return None
    for row in reversed(bars):
        if not isinstance(row, Mapping):
            continue
        for key in ("c", "close", "price_close"):
            val = _num(row.get(key))
            if val is not None and val > 0:
                return val
    return None


def _read_canonical_symbol_sr(
    R: Any,
    symbol: str,
) -> tuple[Optional[dict[str, Any]], Optional[str], str]:
    """
    Existing canonical symbol SR bundle, exact-key only.

    Priority:
      1. xtl:sr:bundle:last_good:{SYMBOL}
      2. xtl:sr:bundle:last:{SYMBOL}
    """
    sym = _upper(symbol)
    if not sym:
        return None, None, "SYMBOL_MISSING"

    keys = (
        f"xtl:sr:bundle:last_good:{sym}",
        f"xtl:sr:bundle:last:{sym}",
    )

    for key in keys:
        try:
            raw = R.get(key)
        except Exception as exc:
            return (
                None,
                key,
                f"SR_REDIS_READ_ERROR:{type(exc).__name__}",
            )

        if raw is None:
            continue

        obj = _json_obj(raw)
        if obj is None:
            return None, key, "SR_BUNDLE_INVALID_JSON"

        return obj, key, "OK"

    return None, None, "SR_BUNDLE_MISSING"


def _load_native_symbol_tf_from_redis(
    R: Any,
    *,
    symbol: str,
    tf: str,
) -> dict[str, Any]:
    """
    Deterministic O(1) native OHLC lookup.

    1. GET xtl:ohlc:latest:{SYMBOL}:{TF}
    2. GET xtl:ohlc:snap:{device}:{SYMBOL}:{TF}

    No Redis SCAN and no cross-device searching.
    """
    sym = _upper(symbol)
    tf_u = _upper(tf)

    out = {
        "ok": False,
        "reason": "NOT_EVALUATED",
        "symbol": sym,
        "tf": tf_u,
        "device_id": None,
        "pointer_key": f"xtl:ohlc:latest:{sym}:{tf_u}",
        "snapshot_key": None,
        "bars": [],
        "bar_count": 0,
        "serverNow": None,
        "server_received_ms": None,
        "lastClosedTs": None,
        "nextCloseTs": None,
    }

    try:
        dev_raw = R.get(out["pointer_key"])
    except Exception as exc:
        out["reason"] = (
            f"OHLC_POINTER_REDIS_ERROR:{type(exc).__name__}"
        )
        return out

    dev = str(_decode(dev_raw) or "").strip()
    if not dev:
        out["reason"] = "OHLC_LATEST_POINTER_MISSING"
        return out

    out["device_id"] = dev
    snap_key = f"xtl:ohlc:snap:{dev}:{sym}:{tf_u}"
    out["snapshot_key"] = snap_key

    try:
        raw = R.get(snap_key)
    except Exception as exc:
        out["reason"] = (
            f"OHLC_SNAPSHOT_REDIS_ERROR:{type(exc).__name__}"
        )
        return out

    snap = _json_obj(raw)
    if snap is None:
        out["reason"] = (
            "OHLC_SNAPSHOT_MISSING"
            if raw is None
            else "OHLC_SNAPSHOT_INVALID_JSON"
        )
        return out

    bars = snap.get("bars")
    if not isinstance(bars, list):
        out["reason"] = "OHLC_SNAPSHOT_BARS_INVALID"
        return out

    # Preserve only actual bar dictionaries. No timeframe synthesis, no
    # transformation of candle semantics here.
    bars = [
        dict(row)
        for row in bars
        if isinstance(row, Mapping)
    ]

    if not bars:
        out["reason"] = "OHLC_SNAPSHOT_BARS_EMPTY"
        return out

    out.update({
        "ok": True,
        "reason": "OK",
        "bars": bars,
        "bar_count": len(bars),
        "serverNow": snap.get("serverNow"),
        "server_received_ms": snap.get("server_received_ms"),
        "lastClosedTs": snap.get("lastClosedTs"),
        "nextCloseTs": snap.get("nextCloseTs"),
    })
    return out


def load_strategy_b_symbol_snapshot_live(
    R: Any,
    *,
    symbol: str,
    side: str,
    as_of_ms: Optional[int] = None,
    snapshot_builder: Optional[Callable[..., Any]] = None,
) -> dict[str, Any]:
    """
    Acquire one live Strategy-B symbol observation from canonical Redis OHLC.

    Acquisition success is distinct from Strategy-B qualification:
    `ok=True` means canonical live inputs were obtained and normalized.
    The returned snapshot may still intentionally fail closed for
    D1 HTF location / structural invalidation / ranking calibration.
    """
    sym = _upper(symbol)
    side_u = _upper(side)
    now_ms = int(as_of_ms or int(time.time() * 1000))

    out: dict[str, Any] = {
        "schema_version": 2,
        "model": "STRATEGY_B_LIVE_SYMBOL_SNAPSHOT_LOADER_V2",
        "ok": False,
        "reason": "NOT_EVALUATED",
        "symbol": sym,
        "side": side_u,
        "as_of_ms": now_ms,
        "bars_available": {
            "D1": 0,
            "H4": 0,
            "H1": 0,
        },
        "ohlc_devices": {
            "D1": None,
            "H4": None,
            "H1": None,
        },
        "ohlc_pointer_keys": {},
        "ohlc_snapshot_keys": {},
        "sr_key": None,
        "price_source": None,
        "snapshot": None,
        "observation_only": True,
        "execution_authority": False,
        "strategy_a_mutation": False,
        "redis_scan_used": False,
        "agent_http_used": False,
        "d1_synthesized": False,
    }

    if not sym:
        out["reason"] = "SYMBOL_MISSING"
        return out

    if side_u not in ("BUY", "SELL"):
        out["reason"] = "SIDE_NOT_ACTIONABLE"
        return out

    tf_rows: dict[str, dict[str, Any]] = {}
    bars: dict[str, list[dict[str, Any]]] = {}

    # Deliberately evaluate all three for diagnostics rather than stopping
    # at the first miss.
    tf_failures: list[str] = []

    for tf in ("D1", "H4", "H1"):
        row = _load_native_symbol_tf_from_redis(
            R,
            symbol=sym,
            tf=tf,
        )
        tf_rows[tf] = row

        out["bars_available"][tf] = int(
            row.get("bar_count") or 0
        )
        out["ohlc_devices"][tf] = row.get("device_id")
        out["ohlc_pointer_keys"][tf] = row.get("pointer_key")
        out["ohlc_snapshot_keys"][tf] = row.get("snapshot_key")

        if row.get("ok") is True:
            bars[tf] = list(row.get("bars") or [])
        else:
            bars[tf] = []
            tf_failures.append(
                f"{tf}:{row.get('reason') or 'UNKNOWN'}"
            )

    if tf_failures:
        out["reason"] = "NATIVE_OHLC_INPUT_MISSING"
        out["ohlc_failures"] = tf_failures
        return out

    sr_bundle, sr_key, sr_reason = _read_canonical_symbol_sr(
        R,
        sym,
    )
    out["sr_key"] = sr_key
    if sr_bundle is None:
        out["reason"] = sr_reason
        return out

    # Preserve structural coherence with the canonical SR computation.
    # Prefer the bundle's own price; only fall back to native H1 close.
    price = _num(sr_bundle.get("price"))
    if price is not None and price > 0:
        price_source = "CANONICAL_SR_BUNDLE_PRICE"
    else:
        price = _last_bar_close(bars["H1"])
        price_source = (
            "LATEST_NATIVE_H1_CLOSE"
            if price is not None and price > 0
            else None
        )

    if price is None or price <= 0:
        out["reason"] = "SYMBOL_PRICE_UNAVAILABLE"
        return out

    out["price_source"] = price_source

    if snapshot_builder is None:
        from api.strategy_b_symbol_snapshot_adapter import (
            build_strategy_b_symbol_snapshot,
        )
        snapshot_builder = build_strategy_b_symbol_snapshot

    try:
        snap = snapshot_builder(
            symbol=sym,
            side=side_u,
            bars_d1=bars["D1"],
            bars_h4=bars["H4"],
            bars_h1=bars["H1"],
            sr_bundle=sr_bundle,
            price=price,
            as_of_ms=now_ms,
        )
    except Exception as exc:
        out["reason"] = (
            f"SYMBOL_SNAPSHOT_BUILD_ERROR:{type(exc).__name__}"
        )
        return out

    snap_dict = (
        snap.to_dict()
        if hasattr(snap, "to_dict")
        else dict(snap)
        if isinstance(snap, Mapping)
        else None
    )

    if not isinstance(snap_dict, dict):
        out["reason"] = "SYMBOL_SNAPSHOT_INVALID_RESULT"
        return out

    out["snapshot"] = snap_dict
    out["ok"] = True
    out["reason"] = "OBSERVATION_CAPTURED"
    return out


__all__ = [
    "_read_canonical_symbol_sr",
    "_load_native_symbol_tf_from_redis",
    "load_strategy_b_symbol_snapshot_live",
]
