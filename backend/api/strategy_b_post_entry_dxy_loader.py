"""
XauTrendLab Strategy B - Post-Entry DXY Loader
===============================================

Read-only acquisition boundary for Strategy-B post-entry thesis validation.

Important:
- exact Redis GET for canonical REAL_DXY identity and H1 turn state;
- reuses the existing production bias engine for canonical DXY bias;
- does not load or qualify M15;
- does not evaluate SR pressure, directional room, confidence thresholds,
  entry qualification, risk, or broker actions;
- missing/mixed evidence is not converted into thesis invalidation.
"""

from __future__ import annotations

import json
import time
from typing import Any, Mapping, Optional

from api.bias_engine import compute_dxy_bias
from api.xtl_analytics import load_real_dxy_bars


DXY_CANONICAL_CONFIG_KEY = "xtl:dxy:canonical"
DXY_H1_STATE_PREFIX = "xtl:dxy:turn:state:H1"


def _txt(value: Any) -> str:
    return str(value or "").strip()


def _upper(value: Any) -> str:
    return _txt(value).upper()


def _json_mapping(raw: Any) -> Optional[dict[str, Any]]:
    if raw is None:
        return None

    if isinstance(raw, bytes):
        try:
            raw = raw.decode("utf-8")
        except Exception:
            return None

    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            return None

    if not isinstance(raw, Mapping):
        return None

    return dict(raw)


def _canonical_h1_key(device_id: str) -> str:
    return (
        f"{DXY_H1_STATE_PREFIX}:REAL_DXY:"
        f"{device_id}"
    )


def load_strategy_b_post_entry_dxy(
    redis_client: Any,
    *,
    as_of_ms: Optional[int] = None,
) -> dict[str, Any]:
    """
    Load only the canonical DXY evidence required by the Strategy-B
    post-entry H1 validity contract.

    data_ok=True means:
      - canonical source is REAL_DXY;
      - canonical device exists;
      - H1 turn state exists and belongs to that device/source;
      - canonical DXY bias can be built and contains H1 evidence.

    It does not mean the thesis is valid or invalid.
    """
    now_ms = int(as_of_ms or int(time.time() * 1000))

    out: dict[str, Any] = {
        "schema_version": 1,
        "model": "STRATEGY_B_POST_ENTRY_DXY_LOADER_V1",
        "data_ok": False,
        "reason": "NOT_EVALUATED",
        "canonical_device_id": None,
        "dxy_h1_turn": {},
        "dxy_bias": {},
        "as_of_ms": now_ms,
        "observation_only": True,
        "execution_authority": False,
        "broker_close_authority": False,
        "strategy_a_mutation": False,
        "redis_scan_used": False,
        "m15_used": False,
    }

    try:
        raw_cfg = redis_client.get(
            DXY_CANONICAL_CONFIG_KEY
        )
    except Exception as exc:
        out["reason"] = (
            "DXY_CANONICAL_GET_FAILED:"
            f"{type(exc).__name__}"
        )
        return out

    cfg = _json_mapping(raw_cfg)
    if cfg is None:
        out["reason"] = "DXY_CANONICAL_CONFIG_INVALID"
        return out

    if _upper(cfg.get("source") or "REAL_DXY") != "REAL_DXY":
        out["reason"] = "DXY_CANONICAL_SOURCE_NOT_REAL"
        return out

    device_id = _txt(
        cfg.get("device_id")
        or cfg.get("real_device_id")
    )
    if not device_id:
        out["reason"] = "DXY_CANONICAL_DEVICE_MISSING"
        return out

    out["canonical_device_id"] = device_id

    try:
        raw_h1 = redis_client.get(
            _canonical_h1_key(device_id)
        )
    except Exception as exc:
        out["reason"] = (
            "DXY_H1_GET_FAILED:"
            f"{type(exc).__name__}"
        )
        return out

    h1_turn = _json_mapping(raw_h1)
    if h1_turn is None:
        out["reason"] = "DXY_H1_STATE_MISSING"
        return out

    if _upper(h1_turn.get("source")) != "REAL_DXY":
        out["reason"] = "DXY_H1_SOURCE_INVALID"
        return out

    state_device = _txt(h1_turn.get("device_id"))
    if state_device and state_device != device_id:
        out["reason"] = "DXY_H1_DEVICE_MISMATCH"
        return out

    out["dxy_h1_turn"] = h1_turn

    try:
        bars_d1 = (
            load_real_dxy_bars(
                device_id=device_id,
                tf="D1",
                max_bars=60,
            )
            or []
        )
        bars_h4 = (
            load_real_dxy_bars(
                device_id=device_id,
                tf="H4",
                max_bars=100,
            )
            or []
        )
        bars_h1 = (
            load_real_dxy_bars(
                device_id=device_id,
                tf="H1",
                max_bars=150,
            )
            or []
        )

        bias = compute_dxy_bias(
            bars_d1=bars_d1,
            bars_h4=bars_h4,
            bars_h1=bars_h1,
            as_of_ms=now_ms,
            source="REAL_DXY",
        )
    except Exception as exc:
        out["reason"] = (
            "DXY_BIAS_LOAD_FAILED:"
            f"{type(exc).__name__}"
        )
        return out

    if not isinstance(bias, Mapping):
        out["reason"] = "DXY_BIAS_RESULT_INVALID"
        return out

    bias = dict(bias)
    timeframes = bias.get("timeframes")
    if not isinstance(timeframes, Mapping):
        out["reason"] = "DXY_BIAS_TIMEFRAMES_MISSING"
        return out

    h1_bias = timeframes.get("H1")
    if not isinstance(h1_bias, Mapping) or not h1_bias:
        out["reason"] = "DXY_BIAS_H1_MISSING"
        return out

    # The frozen post-entry validity contract consumes the
    # timeframe mapping directly:
    #
    #     dxy_bias["H1"]
    #
    # Keep the broader bias-engine result out of that boundary so
    # the validity evaluator does not need to understand the
    # pre-entry bias-engine envelope.
    out["dxy_bias"] = dict(timeframes)
    out["data_ok"] = True
    out["reason"] = "POST_ENTRY_DXY_INPUTS_READY"
    return out


__all__ = [
    "DXY_CANONICAL_CONFIG_KEY",
    "DXY_H1_STATE_PREFIX",
    "load_strategy_b_post_entry_dxy",
]
