"""Strategy-B append-only JSONL calibration capture/replay (V3 H1 identity-safe).

Observation/analytics only. No broker authority, no Redis SCAN, no Strategy-A mutation.
Entry-time snapshots and future outcomes are stored as separate records to prevent
future leakage into decision evidence.
"""
from __future__ import annotations

from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Optional
import hashlib, json, os, time

try:
    import fcntl
except Exception:  # Windows/offline replay compatibility
    fcntl = None

SCHEMA_VERSION = 1
MODEL = "STRATEGY_B_CALIBRATION_JSONL_V1"
SNAPSHOT = "SNAPSHOT"
OUTCOME = "OUTCOME"


def _to_dict(v: Any) -> dict[str, Any]:
    if hasattr(v, "to_dict"):
        v = v.to_dict()
    elif is_dataclass(v):
        v = asdict(v)
    if not isinstance(v, Mapping):
        raise TypeError("expected mapping/dataclass/to_dict object")
    return dict(v)


def _canonical(v: Mapping[str, Any]) -> str:
    return json.dumps(v, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def _hash_id(prefix: str, payload: Mapping[str, Any]) -> str:
    return f"{prefix}_" + hashlib.sha256(_canonical(payload).encode()).hexdigest()[:32]


def build_snapshot_record(snapshot: Any, *, thesis_id: str | None = None,
                          evidence_id: str | None = None, release_id: str | None = None,
                          source: str = "LIVE", device_id: str | None = None,
                          account_ref: str | None = None, profile_id: str | None = None,
                          environment: str | None = None) -> dict[str, Any]:
    s = _to_dict(snapshot)
    if s.get("future_data_used") is True:
        raise ValueError("SNAPSHOT_REJECTED_FUTURE_DATA_USED")
    if s.get("outcome_attached") is True:
        raise ValueError("SNAPSHOT_REJECTED_OUTCOME_ALREADY_ATTACHED")

    identity = {
        "model": s.get("model"), "captured_at_ms": s.get("captured_at_ms"),
        "expected_dxy_direction": s.get("expected_dxy_direction"),
        "symbol": s.get("symbol"), "side": s.get("side"),
        "location_status": s.get("location_status"),
        "location_block_reason": s.get("location_block_reason"),
        "invalidation_ownership_status": s.get("invalidation_ownership_status"),
        "ranker_veto_reasons": list(s.get("ranker_veto_reasons") or ()),
        "basket_decision": s.get("basket_decision"), "basket_reason": s.get("basket_reason"),
        "thesis_id": thesis_id, "evidence_id": evidence_id, "release_id": release_id,
    }

    # V3: when an entry-time snapshot carries evidence-specific H1 causal
    # enrichment, include that causal identity in the deterministic capture id.
    # Unenriched V1/V2 snapshots keep their historical identity unchanged.
    if s.get("h1_observation_attached") is True:
        identity.update({
            "h1_evidence_id": s.get("h1_evidence_id"),
            "h1_catalyst_first_seen_ms": s.get("h1_catalyst_first_seen_ms"),
            "h1_catalyst_source_field": s.get("h1_catalyst_source_field"),
            "h1_break_ms": s.get("h1_break_ms"),
            "h1_proof_status": s.get("h1_proof_status"),
            "h1_sample_class": s.get("h1_sample_class"),
        })
    cid = _hash_id("sbc", identity)
    return {
        "schema_version": SCHEMA_VERSION, "model": MODEL, "record_type": SNAPSHOT,
        "capture_id": cid, "written_at_ms": int(time.time() * 1000),
        "source": str(source or "LIVE").upper(),
        "thesis_id": thesis_id, "evidence_id": evidence_id, "release_id": release_id,
        "device_id": device_id, "account_ref": account_ref, "profile_id": profile_id,
        "environment": environment, "symbol": s.get("symbol"), "side": s.get("side"),
        "captured_at_ms": s.get("captured_at_ms"), "payload": s,
        "observation_only": True, "execution_authority": False,
        "strategy_a_mutation": False, "future_data_used": False,
    }


def build_outcome_record(*, capture_id: str, outcome_label: str,
                         close_reason: str | None = None, closed_at_ms: int | None = None,
                         broker_ticket: int | None = None, entry_price: Any = None,
                         exit_price: Any = None, mfe_r: Any = None, mae_r: Any = None,
                         net_r: Any = None, net_profit: Any = None,
                         metadata: Mapping[str, Any] | None = None) -> dict[str, Any]:
    if not str(capture_id or "").strip():
        raise ValueError("OUTCOME_CAPTURE_ID_REQUIRED")
    if not str(outcome_label or "").strip():
        raise ValueError("OUTCOME_LABEL_REQUIRED")
    payload = {
        "capture_id": str(capture_id), "outcome_label": str(outcome_label).strip().upper(),
        "close_reason": str(close_reason).strip().upper() if close_reason is not None else None,
        "closed_at_ms": int(closed_at_ms) if closed_at_ms is not None else None,
        "broker_ticket": int(broker_ticket) if broker_ticket is not None else None,
        "entry_price": entry_price, "exit_price": exit_price,
        "mfe_r": mfe_r, "mae_r": mae_r, "net_r": net_r, "net_profit": net_profit,
        "metadata": dict(metadata or {}),
    }
    return {
        "schema_version": SCHEMA_VERSION, "model": MODEL, "record_type": OUTCOME,
        "outcome_id": _hash_id("sbo", payload), "capture_id": str(capture_id),
        "written_at_ms": int(time.time() * 1000), "payload": payload,
        "observation_only": True, "execution_authority": False, "strategy_a_mutation": False,
    }


class StrategyBCalibrationJsonlStore:
    def __init__(self, path: str | os.PathLike[str]):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._capture_ids: set[str] = set()
        self._outcome_ids: set[str] = set()
        self._outcomes_by_capture: set[str] = set()
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            return
        with self.path.open("r", encoding="utf-8") as fh:
            for n, raw in enumerate(fh, 1):
                if not raw.strip():
                    continue
                try:
                    rec = json.loads(raw)
                except Exception as exc:
                    raise ValueError(f"INVALID_JSONL line={n} err={exc!r}") from exc
                if rec.get("record_type") == SNAPSHOT and rec.get("capture_id"):
                    self._capture_ids.add(str(rec["capture_id"]))
                elif rec.get("record_type") == OUTCOME:
                    if rec.get("outcome_id"):
                        self._outcome_ids.add(str(rec["outcome_id"]))
                    if rec.get("capture_id"):
                        self._outcomes_by_capture.add(str(rec["capture_id"]))

    def _append(self, rec: Mapping[str, Any]) -> None:
        line = _canonical(rec) + "\n"
        with self.path.open("a", encoding="utf-8") as fh:
            if fcntl is not None:
                fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
            try:
                fh.write(line); fh.flush(); os.fsync(fh.fileno())
            finally:
                if fcntl is not None:
                    fcntl.flock(fh.fileno(), fcntl.LOCK_UN)

    def capture_snapshot(self, snapshot: Any, **kwargs: Any) -> tuple[dict[str, Any], bool]:
        rec = build_snapshot_record(snapshot, **kwargs)
        cid = rec["capture_id"]
        if cid in self._capture_ids:
            return rec, False
        self._append(rec); self._capture_ids.add(cid)
        return rec, True

    def capture_outcome(self, *, capture_id: str, allow_multiple_outcomes: bool = False,
                        **kwargs: Any) -> tuple[dict[str, Any], bool]:
        if capture_id not in self._capture_ids:
            raise ValueError("OUTCOME_UNKNOWN_CAPTURE_ID")
        rec = build_outcome_record(capture_id=capture_id, **kwargs)
        if (not allow_multiple_outcomes and capture_id in self._outcomes_by_capture) or rec["outcome_id"] in self._outcome_ids:
            return rec, False
        self._append(rec)
        self._outcome_ids.add(rec["outcome_id"]); self._outcomes_by_capture.add(capture_id)
        return rec, True

    def iter_records(self) -> Iterator[dict[str, Any]]:
        if not self.path.exists():
            return
        with self.path.open("r", encoding="utf-8") as fh:
            for n, raw in enumerate(fh, 1):
                if not raw.strip():
                    continue
                try:
                    yield json.loads(raw)
                except Exception as exc:
                    raise ValueError(f"INVALID_JSONL line={n} err={exc!r}") from exc

    def replay(self) -> dict[str, Any]:
        snapshots: dict[str, dict[str, Any]] = {}
        outcomes: dict[str, list[dict[str, Any]]] = {}
        for rec in self.iter_records() or ():
            cid = str(rec.get("capture_id") or "")
            if rec.get("record_type") == SNAPSHOT and cid:
                snapshots.setdefault(cid, rec)
            elif rec.get("record_type") == OUTCOME and cid:
                outcomes.setdefault(cid, []).append(rec)
        return {"schema_version": SCHEMA_VERSION, "model": MODEL, "path": str(self.path),
                "snapshot_count": len(snapshots), "outcome_capture_count": len(outcomes),
                "snapshots": snapshots, "outcomes": outcomes}


def capture_basket_snapshots_jsonl(*, store: StrategyBCalibrationJsonlStore,
                                   snapshots: Iterable[Any], **kwargs: Any) -> dict[str, Any]:
    rows = []; written = duplicate = 0
    for snap in snapshots:
        rec, ok = store.capture_snapshot(snap, **kwargs)
        rows.append({"capture_id": rec["capture_id"], "symbol": rec.get("symbol"),
                     "side": rec.get("side"), "written": ok})
        written += int(ok); duplicate += int(not ok)
    return {"written": written, "duplicate": duplicate, "rows": rows}


def capture_validated_basket_jsonl(
    *,
    store: StrategyBCalibrationJsonlStore,
    basket: Any,
    snapshot_builder: Any = None,
    **kwargs: Any,
) -> dict[str, Any]:
    """
    Fail-closed live/demo basket capture.

    This is the production-safe wrapper for Strategy-B expression calibration.
    It refuses to persist incomplete baskets or flattened snapshots that lost
    mandatory location/invalidation ownership evidence.

    The lower-level capture_basket_snapshots_jsonl() remains available for
    offline/replay callers that already own input validation.
    """
    if snapshot_builder is None:
        from api.strategy_b_expression_calibration_snapshot import (
            build_strategy_b_basket_calibration_snapshots,
        )
        snapshot_builder = build_strategy_b_basket_calibration_snapshots

    b = _to_dict(basket)

    if b.get("complete") is not True:
        return {
            "ok": False,
            "written": 0,
            "duplicate": 0,
            "reason": "EXPRESSION_BASKET_INCOMPLETE",
            "basket_decision": b.get("decision"),
            "basket_reason": b.get("reason"),
            "rows": [],
        }

    candidates = b.get("candidates")
    if not isinstance(candidates, (list, tuple)) or not candidates:
        return {
            "ok": False,
            "written": 0,
            "duplicate": 0,
            "reason": "EXPRESSION_BASKET_CANDIDATES_MISSING",
            "basket_decision": b.get("decision"),
            "basket_reason": b.get("reason"),
            "rows": [],
        }

    for row in candidates:
        c = _to_dict(row)

        symbol = str(c.get("symbol") or "").strip().upper()
        side = str(c.get("side") or "").strip().upper()

        if c.get("acquisition_ok") is not True:
            return {
                "ok": False,
                "written": 0,
                "duplicate": 0,
                "reason": "CANDIDATE_ACQUISITION_FAILED",
                "symbol": symbol,
                "side": side,
                "candidate_reason": c.get("acquisition_reason"),
                "rows": [],
            }

        for field, reason in (
            ("htf_location", "CANDIDATE_HTF_LOCATION_MISSING"),
            ("structural_invalidation", "CANDIDATE_STRUCTURAL_INVALIDATION_MISSING"),
            ("invalidation_ownership", "CANDIDATE_INVALIDATION_OWNERSHIP_MISSING"),
        ):
            if not isinstance(c.get(field), Mapping):
                return {
                    "ok": False,
                    "written": 0,
                    "duplicate": 0,
                    "reason": reason,
                    "symbol": symbol,
                    "side": side,
                    "rows": [],
                }

    snapshots = tuple(snapshot_builder(basket))

    if len(snapshots) != len(candidates):
        return {
            "ok": False,
            "written": 0,
            "duplicate": 0,
            "reason": "SNAPSHOT_COUNT_MISMATCH",
            "candidate_count": len(candidates),
            "snapshot_count": len(snapshots),
            "rows": [],
        }

    for snap in snapshots:
        s = _to_dict(snap)
        symbol = str(s.get("symbol") or "").strip().upper()
        side = str(s.get("side") or "").strip().upper()

        if str(s.get("location_status") or "").strip().upper() in ("", "UNKNOWN"):
            return {
                "ok": False,
                "written": 0,
                "duplicate": 0,
                "reason": "SNAPSHOT_LOCATION_UNKNOWN",
                "symbol": symbol,
                "side": side,
                "rows": [],
            }

        if str(s.get("invalidation_ownership_status") or "").strip().upper() in ("", "UNKNOWN"):
            return {
                "ok": False,
                "written": 0,
                "duplicate": 0,
                "reason": "SNAPSHOT_INVALIDATION_OWNERSHIP_UNKNOWN",
                "symbol": symbol,
                "side": side,
                "rows": [],
            }

        if s.get("future_data_used") is True:
            return {
                "ok": False,
                "written": 0,
                "duplicate": 0,
                "reason": "SNAPSHOT_FUTURE_DATA_USED",
                "symbol": symbol,
                "side": side,
                "rows": [],
            }

        if s.get("outcome_attached") is True:
            return {
                "ok": False,
                "written": 0,
                "duplicate": 0,
                "reason": "SNAPSHOT_OUTCOME_ALREADY_ATTACHED",
                "symbol": symbol,
                "side": side,
                "rows": [],
            }

    result = capture_basket_snapshots_jsonl(
        store=store,
        snapshots=snapshots,
        **kwargs,
    )
    return {
        "ok": True,
        "reason": "CAPTURED",
        "basket_decision": b.get("decision"),
        "basket_reason": b.get("reason"),
        **result,
    }

