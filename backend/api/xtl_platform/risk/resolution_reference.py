"""Day-7 Reconciliation Resolution Authority - REFERENCE ONLY - NON-PRODUCTION.

PURE and READ-ONLY. No wall clock, generated IDs, RNG, environment or I/O. It never
calls lifecycle transfer/release, authorization, registration, state replacement,
commit or repository save. Identical inputs yield byte-identical results.

build_reconciliation_evidence(state, document, snapshot)
    Binds the Day-6 report (recomputed here from state + snapshot) to the persisted
    document revision and content SHA-256. Fails if the in-memory account state is
    not exactly the persisted document.

resolve_reconciliation(request, current_state, current_document, policy)
    Verification (first failure wins, every failure -> REJECT):
      1  request/evidence/policy well-formed, supported schema version
      2  evidence_id matches evidence content            (EVIDENCE_INTEGRITY_FAILED)
      3  profile/account identity consistent             (ACCOUNT_MISMATCH)
      4  current persisted revision + digest == evidence (STALE_EVIDENCE)
      5  current in-memory state == persisted document   (STATE_NOT_DURABLE)
      6  Day-6 report recomputed now == evidence report  (EVIDENCE_MISMATCH)
      7  snapshot not after, nor older than policy age   (STALE_EVIDENCE)
      8  report not REJECTED                             (RECONCILIATION_REJECTED)
      9  no internally-owned child from another account  (FOREIGN_CHILD)
     10  unknown outcome                                 (UNKNOWN_OUTCOME)
    Then the per-item decision table (DECISION_TABLE) and aggregation.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any, Optional

from ..domain.account import AccountIdentity
from .lifecycle_contracts import ExposureOwner as EO, ReferenceLifecycleState
from .persistence_codec import canonical_json, content_digest, decode_account_document
from .persistence_contracts import PersistenceDecodeError, StoredDocument
from .reconciliation_contracts import (
    ExternalAccountSnapshot,
    ExternalObject,
    ReconciliationOutcome as O,
    ReconciliationReport,
    ReconciliationStatus,
)
from .reconciliation_reference import reconcile_account
from .resolution_contracts import (
    RESOLUTION_SCHEMA_VERSION,
    SUPPORTED_RESOLUTION_VERSIONS,
    ItemDecision as D,
    ItemResolution,
    ReconciliationEvidence,
    RepairAction as RA,
    RepairPlan,
    ResolutionDecision as RD,
    ResolutionPolicy,
    ResolutionReasonCode as RC,
    ResolutionRequest,
    ResolutionResult,
)
from .validation import is_aware

#: Day-6 outcome -> (item decision, repair action or None, required transition or None)
DECISION_TABLE: dict[O, tuple[D, Optional[RA], Optional[tuple[EO, EO]]]] = {
    O.MATCHED_ACTIVE_RESERVATION_PENDING_ORDER: (D.ACKNOWLEDGE, None, None),
    O.MATCHED_COMMITTED_POSITION: (D.ACKNOWLEDGE, None, None),
    O.MATCHED_RELEASED_ABSENT: (D.ACKNOWLEDGE, None, None),
    O.ACTIVE_RESERVATION_NOT_OBSERVED: (D.HOLD, None, None),
    O.ACTIVE_RESERVATION_OBSERVED_AS_POSITION: (D.REPAIR_REQUIRED, RA.TRANSFER_TO_COMMITTED_POSITION,
                                                 (EO.ACTIVE_RESERVATION, EO.COMMITTED_POSITION)),
    O.COMMITTED_POSITION_EXTERNALLY_MISSING: (D.REPAIR_REQUIRED, RA.INVESTIGATE_COMMITTED_POSITION_MISSING, None),
    O.COMMITTED_POSITION_OBSERVED_AS_PENDING_ORDER: (D.CONFLICT_BLOCK, RA.OWNERSHIP_CONFLICT_REVIEW, None),
    O.RELEASED_BUT_EXTERNALLY_PRESENT: (D.CONFLICT_BLOCK, RA.OWNERSHIP_CONFLICT_REVIEW, None),
    O.EXTERNAL_OBJECT_UNOWNED: (D.CONFLICT_BLOCK, RA.ADOPTION_POLICY_REQUIRED, None),
    O.EXTERNAL_OBJECT_OWNED_BY_OTHER_ACCOUNT: (D.CONFLICT_BLOCK, RA.OWNERSHIP_CONFLICT_REVIEW, None),
    O.DUPLICATE_EXTERNAL_OWNERSHIP: (D.CONFLICT_BLOCK, RA.OWNERSHIP_CONFLICT_REVIEW, None),
    O.OWNERSHIP_ATTRIBUTE_MISMATCH: (D.CONFLICT_BLOCK, RA.OWNERSHIP_CONFLICT_REVIEW, None),
}


class EvidenceBuildError(ValueError):
    pass


# ---- canonical encodings (deterministic, content-only) --------------------------------------

def _account(a: AccountIdentity) -> dict:
    return {"login": a.login, "server": a.server, "company": a.company, "currency": a.currency}


def _dt(v: Optional[datetime]) -> Optional[str]:
    return None if v is None else v.isoformat()


def encode_snapshot(s: ExternalAccountSnapshot) -> dict:
    return {"profile_id": s.profile_id, "account": _account(s.account), "observed_at": _dt(s.observed_at),
            "objects": [{"external_id": o.external_id, "kind": o.kind.value, "canonical_symbol": o.canonical_symbol,
                         "side": o.side.value, "child_trade_id": o.child_trade_id}
                        for o in sorted(s.objects, key=lambda o: o.external_id)]}


def encode_report(r: ReconciliationReport) -> dict:
    return {"profile_id": r.profile_id, "observed_at": _dt(r.observed_at), "status": r.status.value,
            "items": [{"child_trade_id": i.child_trade_id,
                       "internal_owner": None if i.internal_owner is None else i.internal_owner.value,
                       "external_ids": list(i.external_ids), "outcome": i.outcome.value,
                       "classification": i.classification.value, "reason": i.reason} for i in r.items],
            "rejection_reasons": list(r.rejection_reasons)}


def evidence_digest(e: ReconciliationEvidence) -> str:
    return content_digest({"schema_version": e.schema_version, "profile_id": e.profile_id,
                           "account": _account(e.account), "state_revision": e.state_revision,
                           "state_content_sha256": e.state_content_sha256,
                           "snapshot": encode_snapshot(e.snapshot), "report": encode_report(e.report)})


def encode_result(r: ResolutionResult, include_id: bool = True) -> dict:
    out: dict[str, Any] = {
        "schema_version": r.schema_version, "request_id": r.request_id, "profile_id": r.profile_id,
        "evidence_id": r.evidence_id, "state_revision": r.state_revision, "decision": r.decision.value,
        "reason_code": r.reason_code.value, "reason": r.reason,
        "items": [{"child_trade_id": i.child_trade_id, "external_ids": list(i.external_ids),
                   "outcome": i.outcome.value, "decision": i.decision.value, "plan_id": i.plan_id,
                   "reason": i.reason} for i in r.items],
        "repair_plans": [{"plan_id": p.plan_id, "evidence_id": p.evidence_id, "profile_id": p.profile_id,
                          "child_trade_id": p.child_trade_id,
                          "expected_current_owner": None if p.expected_current_owner is None else p.expected_current_owner.value,
                          "observed_external_state": [[x, k.value] for x, k in p.observed_external_state],
                          "required_action": p.required_action.value,
                          "required_transition": None if p.required_transition is None
                          else [p.required_transition[0].value, p.required_transition[1].value],
                          "reason": p.reason} for p in r.repair_plans],
    }
    if include_id:
        out["result_id"] = r.result_id
    return out


def _stored_content_sha256(document: StoredDocument) -> str:
    return json.loads(document.payload)["content_sha256"]


def _account_matches_document(state: ReferenceLifecycleState, document: StoredDocument) -> bool:
    decoded = decode_account_document(document)
    records = tuple(r for r in state.records if r.profile_id == document.profile_id)
    return (state.authority.ledger(document.profile_id) == decoded.ledger
            and records == tuple(r for _, r in decoded.records))


# ---- evidence ---------------------------------------------------------------------------------

def build_reconciliation_evidence(state: ReferenceLifecycleState, document: StoredDocument,
                                  snapshot: ExternalAccountSnapshot) -> ReconciliationEvidence:
    """Pure. Raises EvidenceBuildError if the evidence cannot be bound truthfully."""
    if not isinstance(state, ReferenceLifecycleState) or not isinstance(document, StoredDocument) \
            or not isinstance(snapshot, ExternalAccountSnapshot):
        raise EvidenceBuildError("state, document and snapshot must be the Day-5/Day-6 contract types")
    if snapshot.profile_id != document.profile_id:
        raise EvidenceBuildError("snapshot profile does not match the persisted document")
    try:
        if not _account_matches_document(state, document):
            raise EvidenceBuildError("in-memory account state is not the persisted document")
    except PersistenceDecodeError as exc:
        raise EvidenceBuildError(f"persisted document unreadable: {exc.code}") from exc
    ledger = state.authority.ledger(document.profile_id)
    report = reconcile_account(state, snapshot)
    draft = ReconciliationEvidence(RESOLUTION_SCHEMA_VERSION, document.profile_id, ledger.account_state.account,
                                   document.revision, _stored_content_sha256(document), snapshot, report, "")
    return ReconciliationEvidence(draft.schema_version, draft.profile_id, draft.account, draft.state_revision,
                                  draft.state_content_sha256, snapshot, report, evidence_digest(draft))


# ---- resolution ---------------------------------------------------------------------------------

def _result(request: ResolutionRequest, profile_id: str, evidence_id: str, revision: Optional[int],
            decision: RD, code: RC, reason: str, items=(), plans=()) -> ResolutionResult:
    draft = ResolutionResult(RESOLUTION_SCHEMA_VERSION, request.request_id if isinstance(request.request_id, str) else "",
                             profile_id, evidence_id, revision, decision, code, reason, tuple(items), tuple(plans), "")
    rid = content_digest(encode_result(draft, include_id=False))
    return ResolutionResult(draft.schema_version, draft.request_id, draft.profile_id, draft.evidence_id,
                            draft.state_revision, decision, code, reason, draft.items, draft.repair_plans, rid)


def resolve_reconciliation(request: ResolutionRequest, current_state: ReferenceLifecycleState,
                           current_document: Optional[StoredDocument], policy: ResolutionPolicy) -> ResolutionResult:
    if not isinstance(request, ResolutionRequest):
        raise TypeError("request must be a ResolutionRequest")
    ev = request.evidence
    pid = ev.profile_id if isinstance(ev, ReconciliationEvidence) and isinstance(ev.profile_id, str) else ""
    eid = ev.evidence_id if isinstance(ev, ReconciliationEvidence) and isinstance(ev.evidence_id, str) else ""
    rev = ev.state_revision if isinstance(ev, ReconciliationEvidence) and type(ev.state_revision) is int else None

    def reject(code: RC, reason: str) -> ResolutionResult:
        return _result(request, pid, eid, rev, RD.REJECT, code, reason)

    # 1. well-formed, supported
    if type(request.schema_version) is not int or request.schema_version not in SUPPORTED_RESOLUTION_VERSIONS:
        return reject(RC.UNSUPPORTED_VERSION, f"request schema_version {request.schema_version!r} unsupported")
    if not isinstance(ev, ReconciliationEvidence) or type(ev.schema_version) is not int \
            or ev.schema_version not in SUPPORTED_RESOLUTION_VERSIONS:
        return reject(RC.UNSUPPORTED_VERSION, "evidence missing or schema_version unsupported")
    malformed = []
    if not isinstance(request.request_id, str) or not request.request_id.strip():
        malformed.append("request_id must be a non-empty string")
    if not is_aware(request.resolution_time):
        malformed.append("resolution_time must be timezone-aware")
    if not isinstance(policy, ResolutionPolicy) or not isinstance(policy.max_evidence_age, timedelta) \
            or policy.max_evidence_age <= timedelta(0):
        malformed.append("policy.max_evidence_age must be a positive timedelta")
    if not isinstance(current_state, ReferenceLifecycleState):
        malformed.append("current_state must be a ReferenceLifecycleState")
    if not isinstance(ev.snapshot, ExternalAccountSnapshot) or not isinstance(ev.report, ReconciliationReport) \
            or not isinstance(ev.account, AccountIdentity) or type(ev.state_revision) is not int \
            or not isinstance(ev.state_content_sha256, str):
        malformed.append("evidence fields have invalid types")
    if malformed:
        return reject(RC.MALFORMED_REQUEST, "; ".join(malformed))

    # 2. integrity
    try:
        recomputed_id = evidence_digest(ev)
    except Exception as exc:  # noqa: BLE001 - unencodable evidence is malformed
        return reject(RC.MALFORMED_REQUEST, f"evidence not encodable ({type(exc).__name__})")
    if recomputed_id != ev.evidence_id:
        return reject(RC.EVIDENCE_INTEGRITY_FAILED, "evidence_id does not match evidence content")

    # 3. identity
    if not isinstance(current_document, StoredDocument) or current_document.profile_id != ev.profile_id \
            or ev.snapshot.profile_id != ev.profile_id or ev.report.profile_id != ev.profile_id \
            or ev.snapshot.account != ev.account:
        return reject(RC.ACCOUNT_MISMATCH, "evidence, snapshot, report and current document identities differ")
    ledger = current_state.authority.ledger(ev.profile_id)
    if ledger is None or ledger.account_state.account != ev.account:
        return reject(RC.ACCOUNT_MISMATCH, "evidence account is not the current account of this profile")

    # 4. revision binding
    try:
        current_digest = _stored_content_sha256(current_document)
    except Exception:  # noqa: BLE001
        return reject(RC.STATE_NOT_DURABLE, "current persisted document unreadable")
    if (current_document.revision, current_digest) != (ev.state_revision, ev.state_content_sha256):
        return reject(RC.STALE_EVIDENCE,
                      f"evidence bound to revision {ev.state_revision}; persisted revision is {current_document.revision}")

    # 5. in-memory == persisted
    try:
        durable = _account_matches_document(current_state, current_document)
    except PersistenceDecodeError as exc:
        return reject(RC.STATE_NOT_DURABLE, f"current persisted document rejected: {exc.code}")
    if not durable:
        return reject(RC.STATE_NOT_DURABLE, "current in-memory account state differs from the persisted document")

    # 6. report recomputed now
    if reconcile_account(current_state, ev.snapshot) != ev.report:
        return reject(RC.EVIDENCE_MISMATCH, "evidence report does not match reconciliation of current state")

    # 7. freshness from contracts only
    observed = ev.snapshot.observed_at
    if observed > request.resolution_time:
        return reject(RC.STALE_EVIDENCE, "snapshot observed after the resolution time")
    if request.resolution_time - observed > policy.max_evidence_age:
        return reject(RC.STALE_EVIDENCE, f"evidence age exceeds {policy.max_evidence_age}")

    # 8. rejected reconciliation
    if ev.report.status is ReconciliationStatus.REJECTED:
        return reject(RC.RECONCILIATION_REJECTED, "; ".join(ev.report.rejection_reasons))

    # 9. foreign internally-owned child (defense in depth)
    own_children = {r.child_trade_id for r in ledger.reservations} | \
                   {r.child_trade_id for r in current_state.records if r.profile_id == ev.profile_id}
    for item in ev.report.items:
        if item.internal_owner is not None and item.child_trade_id not in own_children:
            return reject(RC.FOREIGN_CHILD, f"{item.child_trade_id} is not owned by account {ev.profile_id}")

    # 10. decision table
    external = {o.external_id: o for o in ev.snapshot.objects}
    items: list[ItemResolution] = []
    plans: list[RepairPlan] = []
    for item in ev.report.items:
        row = DECISION_TABLE.get(item.outcome)
        if row is None:
            return reject(RC.UNKNOWN_OUTCOME, f"no decision rule for outcome {item.outcome!r}")
        decision, action, transition = row
        plan_id = None
        if action is not None:
            plan_id = content_digest({"evidence_id": ev.evidence_id, "child_trade_id": item.child_trade_id,
                                      "external_ids": list(item.external_ids), "action": action.value})
            observed_state = tuple((x, external[x].kind) for x in item.external_ids)
            plans.append(RepairPlan(plan_id, ev.evidence_id, ev.profile_id, item.child_trade_id,
                                    item.internal_owner, observed_state, action, transition,
                                    f"{item.outcome}: {action} (declarative only; not executed)"))
        items.append(ItemResolution(item.child_trade_id, item.external_ids, item.outcome, decision, plan_id,
                                    f"{item.outcome} -> {decision}; capacity unchanged"))

    decisions = {i.decision for i in items}
    if D.CONFLICT_BLOCK in decisions:
        top, code, reason = RD.REJECT, RC.HARD_CONFLICT, "hard conflict: explicit future repair/adoption policy required"
    elif D.REPAIR_REQUIRED in decisions:
        top, code, reason = RD.REPAIR_REQUIRED, RC.REPAIR_PLANNED, "declarative repair plan(s) produced; nothing executed"
    elif D.HOLD in decisions:
        top, code, reason = RD.HOLD, RC.UNCERTAINTY_HELD, "uncertainty held; no capacity released"
    else:
        top, code, reason = RD.ACCEPT, RC.CLEAN_RECONCILIATION, "clean reconciliation acknowledged; ownership and risk unchanged"
    return _result(request, ev.profile_id, ev.evidence_id, ev.state_revision, top, code, reason, items, plans)
