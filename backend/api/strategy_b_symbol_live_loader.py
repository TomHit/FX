"""
XauTrendLab Strategy B - Live Symbol Snapshot Loader (V3)
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
- if that cache is absent, reuse the EXISTING trend_sr.summarize_sr_multi_tf()
  mathematics locally against already-loaded native H4/H1 bars
- pass already-loaded inputs to the pure Strategy-B symbol snapshot adapter

Does NOT:
- call AGENT_BASE_URL / _broker_bars_sync
- compute a second/competing SR model
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



class _NoopCache:
    """
    Cache facade for canonical trend_sr computation in Strategy-B observation.

    summarize_sr_multi_tf() owns canonical cache semantics, but Strategy-B must
    not mutate shared Strategy-A Redis state during observation/replay.  This
    facade therefore permits the canonical function to execute unchanged while
    discarding writes and returning no prior cached value.
    """

    def get(self, key: str) -> None:
        return None

    def setex(self, key: str, ttl_sec: int, value: Any) -> bool:
        return True

    def set(self, key: str, value: Any, *args: Any, **kwargs: Any) -> bool:
        return True


def _default_canonical_sr_fallback(
    *,
    symbol: str,
    price: float,
    h4_bars: list[dict[str, Any]],
    h1_bars: list[dict[str, Any]],
) -> dict[str, Any]:
    """
    Reuse the existing canonical XTL H4/H1 SR implementation.

    Important:
    - no D1 SR is invented here;
    - no shared Redis keys are written;
    - no alternate SR/pivot/zone mathematics are introduced.

    The same row->DataFrame and HLC normalization helpers already used by
    trend_endpoints are reused so Strategy-B does not create a parallel bar
    normalization contract.
    """
    from api.trend_endpoints import _rows_to_df, _to_hlc
    from api.trend_sr import summarize_sr_multi_tf

    sym = _upper(symbol)
    pip_factor = (
        0.01
        if sym == "XAUUSD" or sym.endswith("JPY")
        else 0.0001
    )

    h4_df = _rows_to_df(h4_bars) if h4_bars else None
    h1_df = _rows_to_df(h1_bars) if h1_bars else None

    bundle = summarize_sr_multi_tf(
        symbol=sym,
        price=float(price),
        h4_df=_to_hlc(h4_df),
        h1_df=_to_hlc(h1_df),
        pip_factor=float(pip_factor),
        cache=_NoopCache(),
    )
    return dict(bundle) if isinstance(bundle, Mapping) else {}


def _resolve_canonical_symbol_sr(
    R: Any,
    *,
    symbol: str,
    h4_bars: list[dict[str, Any]],
    h1_bars: list[dict[str, Any]],
    fallback_builder: Optional[Callable[..., Any]] = None,
) -> tuple[
    Optional[dict[str, Any]],
    Optional[str],
    str,
    Optional[float],
    Optional[str],
]:
    """
    Resolve canonical H4/H1 SR without making Strategy-B a shared-state writer.

    Order:
      1. exact Redis last_good / last
      2. only when both keys are genuinely absent, compute locally using the
         existing trend_sr.summarize_sr_multi_tf() implementation

    Redis errors / invalid cached JSON fail closed rather than being hidden by
    a fallback computation.
    """
    bundle, key, reason = _read_canonical_symbol_sr(R, symbol)

    if bundle is not None:
        price = _num(bundle.get("price"))
        if price is not None and price > 0:
            return (
                bundle,
                key,
                "CACHE",
                price,
                "CANONICAL_SR_BUNDLE_PRICE",
            )

        price = _last_bar_close(h1_bars)
        if price is None or price <= 0:
            return None, key, "CACHE_PRICE_MISSING", None, None

        return (
            bundle,
            key,
            "CACHE",
            price,
            "LATEST_NATIVE_H1_CLOSE",
        )

    # A Redis read error or malformed cached payload is an input-health fault.
    # Do not mask it by computing a fresh SR bundle.
    if reason != "SR_BUNDLE_MISSING":
        return None, key, reason, None, None

    price = _last_bar_close(h1_bars)
    if price is None or price <= 0:
        return None, None, "SYMBOL_PRICE_UNAVAILABLE", None, None

    builder = fallback_builder or _default_canonical_sr_fallback

    try:
        computed = builder(
            symbol=_upper(symbol),
            price=float(price),
            h4_bars=h4_bars,
            h1_bars=h1_bars,
        )
    except Exception as exc:
        return (
            None,
            None,
            f"CANONICAL_SR_LOCAL_COMPUTE_ERROR:{type(exc).__name__}",
            None,
            None,
        )

    if not isinstance(computed, Mapping) or not computed:
        return None, None, "CANONICAL_SR_LOCAL_COMPUTE_EMPTY", None, None

    computed = dict(computed)

    # summarize_sr_multi_tf() should echo price, but preserve the acquisition
    # price deterministically if an older implementation omits it.
    computed_price = _num(computed.get("price"))
    if computed_price is None or computed_price <= 0:
        computed["price"] = float(price)
        computed_price = float(price)

    return (
        computed,
        None,
        "LOCAL_CANONICAL_TREND_SR",
        computed_price,
        "LATEST_NATIVE_H1_CLOSE",
    )

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
    sr_fallback_builder: Optional[Callable[..., Any]] = None,
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
        "schema_version": 3,
        "model": "STRATEGY_B_LIVE_SYMBOL_SNAPSHOT_LOADER_V3",
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
        "sr_source": None,
        "sr_fallback_used": False,
        "sr_shared_cache_written": False,
        "d1_sr_available": False,
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

    (
        sr_bundle,
        sr_key,
        sr_source,
        price,
        price_source,
    ) = _resolve_canonical_symbol_sr(
        R,
        symbol=sym,
        h4_bars=bars["H4"],
        h1_bars=bars["H1"],
        fallback_builder=sr_fallback_builder,
    )

    out["sr_key"] = sr_key
    out["sr_source"] = sr_source
    out["sr_fallback_used"] = (
        sr_source == "LOCAL_CANONICAL_TREND_SR"
    )
    out["price_source"] = price_source

    if sr_bundle is None:
        out["reason"] = sr_source
        return out

    if price is None or price <= 0:
        out["reason"] = "SYMBOL_PRICE_UNAVAILABLE"
        return out

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
    "_resolve_canonical_symbol_sr",
    "_default_canonical_sr_fallback",
    "_load_native_symbol_tf_from_redis",
    "load_strategy_b_symbol_snapshot_live",
]
