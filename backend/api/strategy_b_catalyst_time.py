"""
XauTrendLab Strategy B - Catalyst Time Resolver
===============================================

Exact-key, fail-closed resolver for the first moment Strategy-B could know a
canonical catalyst result.

Authority order:
1) macro context.first_result_seen_ms
2) macro evidence.first_seen_at_ms

This module does not infer from event_time_ms, published_at_ms, observed_at_ms,
or current time. It never scans Redis and has no broker/order/risk authority.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping, Optional
import json


def _mapping(v: Any) -> Mapping[str, Any]:
    return v if isinstance(v, Mapping) else {}


def _int(v: Any) -> int:
    try:
        return int(v or 0)
    except Exception:
        return 0


def _decode_json(raw: Any) -> Optional[dict[str, Any]]:
    if raw is None:
        return None
    if isinstance(raw, Mapping):
        return dict(raw)
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8", "replace")
    try:
        obj = json.loads(raw)
    except Exception:
        return None
    return dict(obj) if isinstance(obj, Mapping) else None


@dataclass(frozen=True)
class StrategyBCatalystTime:
    schema_version: int
    model: str
    evidence_id: str

    ok: bool
    reason: str

    catalyst_first_seen_ms: int
    source_field: Optional[str]

    event_time_ms: int
    schedule_consistent: Optional[bool]

    evidence_key: str
    context_key: str
    evidence_present: bool
    context_present: bool

    observation_only: bool = True
    execution_authority: bool = False
    strategy_a_mutation: bool = False
    redis_scan_used: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def resolve_strategy_b_catalyst_time(
    *,
    evidence_id: str,
    evidence: Mapping[str, Any] | None,
    context: Mapping[str, Any] | None,
) -> StrategyBCatalystTime:
    eid = str(evidence_id or "").strip()
    ev = _mapping(evidence)
    ctx = _mapping(context)

    evidence_key = f"xtl:strategy_b:macro:evidence:{eid}"
    context_key = f"xtl:strategy_b:macro:context:{eid}"

    if not eid:
        return StrategyBCatalystTime(
            schema_version=1,
            model="STRATEGY_B_CATALYST_TIME_V1",
            evidence_id="",
            ok=False,
            reason="EVIDENCE_ID_MISSING",
            catalyst_first_seen_ms=0,
            source_field=None,
            event_time_ms=0,
            schedule_consistent=None,
            evidence_key=evidence_key,
            context_key=context_key,
            evidence_present=bool(ev),
            context_present=bool(ctx),
        )

    ctx_first = _int(ctx.get("first_result_seen_ms"))
    ev_first = _int(ev.get("first_seen_at_ms"))
    event_time = _int(ev.get("event_time_ms"))

    if ctx_first > 0:
        first_seen = ctx_first
        source = "context.first_result_seen_ms"
    elif ev_first > 0:
        first_seen = ev_first
        source = "evidence.first_seen_at_ms"
    else:
        return StrategyBCatalystTime(
            schema_version=1,
            model="STRATEGY_B_CATALYST_TIME_V1",
            evidence_id=eid,
            ok=False,
            reason="CATALYST_FIRST_SEEN_TIME_MISSING",
            catalyst_first_seen_ms=0,
            source_field=None,
            event_time_ms=event_time,
            schedule_consistent=None if event_time <= 0 else False,
            evidence_key=evidence_key,
            context_key=context_key,
            evidence_present=bool(ev),
            context_present=bool(ctx),
        )

    schedule_consistent = None
    if event_time > 0:
        schedule_consistent = first_seen >= event_time

    return StrategyBCatalystTime(
        schema_version=1,
        model="STRATEGY_B_CATALYST_TIME_V1",
        evidence_id=eid,
        ok=True,
        reason="CATALYST_FIRST_SEEN_RESOLVED",
        catalyst_first_seen_ms=first_seen,
        source_field=source,
        event_time_ms=event_time,
        schedule_consistent=schedule_consistent,
        evidence_key=evidence_key,
        context_key=context_key,
        evidence_present=bool(ev),
        context_present=bool(ctx),
    )


def load_strategy_b_catalyst_time(
    R: Any,
    *,
    evidence_id: str,
) -> StrategyBCatalystTime:
    eid = str(evidence_id or "").strip()
    evidence_key = f"xtl:strategy_b:macro:evidence:{eid}"
    context_key = f"xtl:strategy_b:macro:context:{eid}"

    if not eid:
        return resolve_strategy_b_catalyst_time(
            evidence_id=eid,
            evidence=None,
            context=None,
        )

    try:
        raw_evidence = R.get(evidence_key)
    except Exception as exc:
        return StrategyBCatalystTime(
            schema_version=1,
            model="STRATEGY_B_CATALYST_TIME_V1",
            evidence_id=eid,
            ok=False,
            reason=f"EVIDENCE_REDIS_READ_ERROR:{type(exc).__name__}",
            catalyst_first_seen_ms=0,
            source_field=None,
            event_time_ms=0,
            schedule_consistent=None,
            evidence_key=evidence_key,
            context_key=context_key,
            evidence_present=False,
            context_present=False,
        )

    try:
        raw_context = R.get(context_key)
    except Exception as exc:
        return StrategyBCatalystTime(
            schema_version=1,
            model="STRATEGY_B_CATALYST_TIME_V1",
            evidence_id=eid,
            ok=False,
            reason=f"CONTEXT_REDIS_READ_ERROR:{type(exc).__name__}",
            catalyst_first_seen_ms=0,
            source_field=None,
            event_time_ms=0,
            schedule_consistent=None,
            evidence_key=evidence_key,
            context_key=context_key,
            evidence_present=raw_evidence is not None,
            context_present=False,
        )

    if raw_evidence is not None and _decode_json(raw_evidence) is None:
        return StrategyBCatalystTime(
            schema_version=1,
            model="STRATEGY_B_CATALYST_TIME_V1",
            evidence_id=eid,
            ok=False,
            reason="EVIDENCE_INVALID_JSON",
            catalyst_first_seen_ms=0,
            source_field=None,
            event_time_ms=0,
            schedule_consistent=None,
            evidence_key=evidence_key,
            context_key=context_key,
            evidence_present=True,
            context_present=raw_context is not None,
        )

    if raw_context is not None and _decode_json(raw_context) is None:
        return StrategyBCatalystTime(
            schema_version=1,
            model="STRATEGY_B_CATALYST_TIME_V1",
            evidence_id=eid,
            ok=False,
            reason="CONTEXT_INVALID_JSON",
            catalyst_first_seen_ms=0,
            source_field=None,
            event_time_ms=0,
            schedule_consistent=None,
            evidence_key=evidence_key,
            context_key=context_key,
            evidence_present=raw_evidence is not None,
            context_present=True,
        )

    evidence = _decode_json(raw_evidence) or {}
    context = _decode_json(raw_context) or {}

    return resolve_strategy_b_catalyst_time(
        evidence_id=eid,
        evidence=evidence,
        context=context,
    )


__all__ = [
    "StrategyBCatalystTime",
    "resolve_strategy_b_catalyst_time",
    "load_strategy_b_catalyst_time",
]
