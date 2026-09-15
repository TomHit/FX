
"""
XauTrendLab Strategy B - Shadow Persistence
===========================================

Additive Redis persistence for the composed Strategy-B shadow decision.

No Redis SCAN.
No broker/order/modify/close calls.
No Strategy-A writes.
No shared-risk reservation.

The module persists only exact Strategy-B keys and supports restart-safe
reconstruction from explicit per-evidence keys and bounded history.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, is_dataclass
from typing import Any, Mapping, Optional


SHADOW_LATEST_PREFIX = "xtl:strategy_b:shadow:latest"
SHADOW_HISTORY_PREFIX = "xtl:strategy_b:shadow:history"

DEFAULT_TTL_SEC = 21 * 86_400
DEFAULT_MAX_HISTORY = 100


def _txt(v: Any) -> str:
    return str(v or "").strip()


def _json_dump(v: Any) -> str:
    return json.dumps(v, separators=(",", ":"), sort_keys=True, default=str)


def _json_load(v: Any, default: Any = None) -> Any:
    if default is None:
        default = {}
    try:
        if v is None:
            return default
        if isinstance(v, (bytes, bytearray)):
            v = v.decode("utf-8", "ignore")
        if isinstance(v, str):
            return json.loads(v)
        return v
    except Exception:
        return default


def _sha256_json(v: Any) -> str:
    return hashlib.sha256(_json_dump(v).encode("utf-8")).hexdigest()


def _to_dict(obj: Any) -> dict:
    if isinstance(obj, Mapping):
        return dict(obj)
    if hasattr(obj, "to_dict"):
        return dict(obj.to_dict())
    if is_dataclass(obj):
        return asdict(obj)
    raise TypeError("shadow_decision must be mapping/dataclass/to_dict object")


def _latest_key(evidence_id: str) -> str:
    return f"{SHADOW_LATEST_PREFIX}:{evidence_id}"


def _history_key(evidence_id: str) -> str:
    return f"{SHADOW_HISTORY_PREFIX}:{evidence_id}"


def _material_projection(row: Mapping[str, Any]) -> dict:
    """
    Only fields that materially alter the decision/lifecycle are fingerprinted.
    Ephemeral timestamps and repeated evidence snapshots do not create duplicate
    history records every observer tick.
    """
    return {
        "mode": row.get("mode"),
        "current_state": row.get("current_state"),
        "derived_state": row.get("derived_state"),
        "decision": row.get("decision"),
        "reason": row.get("reason"),
        "blocking_component": row.get("blocking_component"),
        "transition_plan": row.get("transition_plan") or [],
        "transition_plan_valid": row.get("transition_plan_valid"),
        "selected_symbol": row.get("selected_symbol"),
        "selected_side": row.get("selected_side"),
        "proposed_risk_usd": row.get("proposed_risk_usd"),
        "proposed_lots_raw": row.get("proposed_lots_raw"),
    }


def persist_strategy_b_shadow_decision(
    R,
    *,
    evidence_id: str,
    calendar_owner_id: str,
    value_id: str,
    shadow_decision: Any,
    now_ms: int,
    ttl_sec: int = DEFAULT_TTL_SEC,
    max_history: int = DEFAULT_MAX_HISTORY,
) -> dict:
    evidence_id = _txt(evidence_id)
    if not evidence_id:
        raise ValueError("evidence_id required")

    row = _to_dict(shadow_decision)
    material = _material_projection(row)
    fingerprint = _sha256_json(material)

    latest_key = _latest_key(evidence_id)
    previous = _json_load(R.get(latest_key), {})
    previous_fp = _txt(previous.get("material_fingerprint")) if isinstance(previous, dict) else ""
    changed = previous_fp != fingerprint

    wrapper = {
        "schema_version": 1,
        "service": "strategy_b_shadow_persistence",
        "evidence_id": evidence_id,
        "calendar_owner_id": _txt(calendar_owner_id),
        "value_id": _txt(value_id),
        "material_fingerprint": fingerprint,
        "material_changed": bool(changed),
        "persisted_at_ms": int(now_ms),
        "shadow": row,
        "observation_only": True,
        "execution_authority": False,
        "risk_reservation_authority": False,
        "broker_order_authority": False,
        "broker_modify_authority": False,
        "broker_close_authority": False,
        "strategy_a_mutation": False,
    }

    ttl = max(60, int(ttl_sec))
    history_limit = max(1, int(max_history))

    if changed:
        hkey = _history_key(evidence_id)
        pipe = R.pipeline(transaction=True)
        pipe.setex(latest_key, ttl, _json_dump(wrapper))
        pipe.rpush(hkey, _json_dump(wrapper))
        pipe.ltrim(hkey, -history_limit, -1)
        pipe.expire(hkey, ttl)
        pipe.execute()
    else:
        refreshed = dict(previous)
        refreshed["last_observed_at_ms"] = int(now_ms)
        refreshed["material_changed"] = False
        R.setex(latest_key, ttl, _json_dump(refreshed))

    return {
        "changed": bool(changed),
        "fingerprint": fingerprint,
        "derived_state": row.get("derived_state"),
        "decision": row.get("decision"),
        "latest_key": latest_key,
        "history_key": _history_key(evidence_id),
    }


def load_strategy_b_shadow_latest(R, evidence_id: str) -> Optional[dict]:
    row = _json_load(R.get(_latest_key(_txt(evidence_id))), None)
    return row if isinstance(row, dict) else None


def load_strategy_b_shadow_history(
    R,
    evidence_id: str,
    *,
    limit: int = DEFAULT_MAX_HISTORY,
) -> list[dict]:
    n = max(1, int(limit))
    rows = R.lrange(_history_key(_txt(evidence_id)), -n, -1) or []
    out: list[dict] = []
    for raw in rows:
        row = _json_load(raw, None)
        if isinstance(row, dict):
            out.append(row)
    return out


def reconstruct_strategy_b_shadow_state(
    R,
    *,
    evidence_id: str,
    history_limit: int = DEFAULT_MAX_HISTORY,
) -> dict:
    """
    Exact-key restart reconstruction for one Strategy-B evidence/thesis.

    Returns latest composed state plus bounded persisted transition history.
    Does not infer missing states and does not scan Redis.
    """
    evidence_id = _txt(evidence_id)
    latest = load_strategy_b_shadow_latest(R, evidence_id)
    history = load_strategy_b_shadow_history(
        R,
        evidence_id,
        limit=history_limit,
    )

    if not latest:
        return {
            "schema_version": 1,
            "evidence_id": evidence_id,
            "found": False,
            "latest": None,
            "history": history,
            "history_count": len(history),
            "observation_only": True,
            "execution_authority": False,
            "uses_redis_scan": False,
        }

    shadow = latest.get("shadow") if isinstance(latest.get("shadow"), Mapping) else {}
    return {
        "schema_version": 1,
        "evidence_id": evidence_id,
        "found": True,
        "derived_state": shadow.get("derived_state"),
        "decision": shadow.get("decision"),
        "reason": shadow.get("reason"),
        "selected_symbol": shadow.get("selected_symbol"),
        "selected_side": shadow.get("selected_side"),
        "proposed_risk_usd": shadow.get("proposed_risk_usd"),
        "proposed_lots_raw": shadow.get("proposed_lots_raw"),
        "latest": latest,
        "history": history,
        "history_count": len(history),
        "observation_only": True,
        "execution_authority": False,
        "uses_redis_scan": False,
    }


__all__ = [
    "SHADOW_LATEST_PREFIX",
    "SHADOW_HISTORY_PREFIX",
    "persist_strategy_b_shadow_decision",
    "load_strategy_b_shadow_latest",
    "load_strategy_b_shadow_history",
    "reconstruct_strategy_b_shadow_state",
]
