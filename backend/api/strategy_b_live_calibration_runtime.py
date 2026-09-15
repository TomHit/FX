"""
XauTrendLab Strategy B - Live Calibration Runtime Sink
======================================================

Persistent runtime wrapper for Strategy-B live calibration JSONL capture.

This module owns only the durable file destination and delegates:
- evidence-specific H1 observation/enrichment to strategy_b_live_calibration_capture
- validation and append-only persistence to strategy_b_calibration_jsonl

It does NOT build the expression basket, place/modify/close broker orders,
reserve risk, use Redis SCAN, or mutate Strategy A.

Multi-process safety:
- create a fresh StrategyBCalibrationJsonlStore for each capture call so its
  dedupe index is refreshed from disk;
- the underlying writer uses an OS file lock for durable append;
- the Strategy-B macro tick already has a cross-worker Redis lock, so the
  intended runtime caller remains serialized across workers.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Optional

DEFAULT_CALIBRATION_JSONL_PATH = (
    "/opt/xauapi/data/strategy_b/calibration/strategy_b_expression.jsonl"
)

CALIBRATION_JSONL_ENV = "XTL_STRATEGY_B_CALIBRATION_JSONL_PATH"


def strategy_b_calibration_jsonl_path() -> Path:
    raw = str(
        os.getenv(
            CALIBRATION_JSONL_ENV,
            DEFAULT_CALIBRATION_JSONL_PATH,
        )
        or ""
    ).strip()

    if not raw:
        raw = DEFAULT_CALIBRATION_JSONL_PATH

    return Path(raw)


def capture_strategy_b_runtime_calibration(
    *,
    R: Any,
    basket: Any,
    evidence_id: str,
    as_of_ms: Optional[int] = None,
    thesis_id: Optional[str] = None,
    release_id: Optional[str] = None,
    device_id: Optional[str] = None,
    account_ref: Optional[str] = None,
    profile_id: Optional[str] = None,
    environment: str = "DEMO",
    source: str = "LIVE",
    path: Optional[str | os.PathLike[str]] = None,
    h1_observer: Any = None,
) -> dict[str, Any]:
    """
    Persist one already-built live Strategy-B expression basket.

    Fail closed:
    - missing evidence_id -> no write;
    - H1 causal observation failure -> no write;
    - incomplete basket / mandatory evidence failure -> no write.

    The function intentionally creates a new store on each call so a worker
    always reloads the current append-only capture-id index before dedupe.
    """
    eid = str(evidence_id or "").strip()
    if not eid:
        return {
            "ok": False,
            "written": 0,
            "duplicate": 0,
            "reason": "EVIDENCE_ID_MISSING",
            "path": str(path or strategy_b_calibration_jsonl_path()),
            "rows": [],
        }

    target = Path(path) if path is not None else strategy_b_calibration_jsonl_path()

    from api.strategy_b_calibration_jsonl import (
        StrategyBCalibrationJsonlStore,
    )
    from api.strategy_b_live_calibration_capture import (
        capture_strategy_b_live_calibration_with_h1,
    )

    store = StrategyBCalibrationJsonlStore(target)

    try:
        capture_args = {
            "R": R,
            "store": store,
            "basket": basket,
            "evidence_id": eid,
            "as_of_ms": as_of_ms,
            "thesis_id": thesis_id,
            "release_id": release_id,
            "device_id": device_id,
            "account_ref": account_ref,
            "profile_id": profile_id,
            "environment": str(environment or "DEMO").upper(),
            "source": str(source or "LIVE").upper(),
        }
        # Reuse precomputed evidence-specific H1 observations when supplied,
        # avoiding a second six-symbol H1 acquisition in the macro service.
        if h1_observer is not None:
            capture_args["h1_observer"] = h1_observer

        out = capture_strategy_b_live_calibration_with_h1(
            **capture_args
        )
    except Exception as exc:
        return {
            "ok": False,
            "written": 0,
            "duplicate": 0,
            "reason": f"CALIBRATION_CAPTURE_EXCEPTION:{type(exc).__name__}",
            "error": str(exc),
            "path": str(target),
            "evidence_id": eid,
            "rows": [],
        }

    result = dict(out or {})
    result["path"] = str(target)
    result["evidence_id"] = eid
    return result


__all__ = [
    "DEFAULT_CALIBRATION_JSONL_PATH",
    "CALIBRATION_JSONL_ENV",
    "strategy_b_calibration_jsonl_path",
    "capture_strategy_b_runtime_calibration",
]
