"""Day-7 reconciliation resolution CONTRACTS - REFERENCE ONLY - NON-PRODUCTION.

Converts Day-6 reconciliation findings into explicit, declarative decisions.
A resolution NEVER executes anything: it never releases capacity, never transfers
or mutates lifecycle ownership, never authorizes risk, never touches a broker.

Evidence binding: ReconciliationEvidence binds a Day-6 report to the exact
persisted account document it was derived from (revision + content SHA-256) and
to the exact external snapshot, under a content-derived evidence_id.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import ClassVar, Optional

from ..domain._validation import ContractError
from ..domain.account import AccountIdentity
from ..domain.enums import StrEnum
from .lifecycle_contracts import ExposureOwner
from .reconciliation_contracts import (
    ExternalAccountSnapshot,
    ExternalObjectKind,
    ReconciliationOutcome,
    ReconciliationReport,
)

RESOLUTION_SCHEMA_VERSION = 1
SUPPORTED_RESOLUTION_VERSIONS = frozenset({RESOLUTION_SCHEMA_VERSION})


class ResolutionDecision(StrEnum):
    """Top-level decision. Precedence: REJECT > REPAIR_REQUIRED > HOLD > ACCEPT."""

    ACCEPT = "ACCEPT"                    # clean reconciliation acknowledged; nothing changes
    HOLD = "HOLD"                        # uncertainty; capacity held; nothing changes
    REPAIR_REQUIRED = "REPAIR_REQUIRED"  # declarative repair plan(s); nothing executed
    REJECT = "REJECT"                    # hard conflict or invalid/stale/forged evidence


class ResolutionReasonCode(StrEnum):
    CLEAN_RECONCILIATION = "CLEAN_RECONCILIATION"
    UNCERTAINTY_HELD = "UNCERTAINTY_HELD"
    REPAIR_PLANNED = "REPAIR_PLANNED"
    HARD_CONFLICT = "HARD_CONFLICT"
    MALFORMED_REQUEST = "MALFORMED_REQUEST"
    UNSUPPORTED_VERSION = "UNSUPPORTED_VERSION"
    EVIDENCE_INTEGRITY_FAILED = "EVIDENCE_INTEGRITY_FAILED"
    ACCOUNT_MISMATCH = "ACCOUNT_MISMATCH"
    STALE_EVIDENCE = "STALE_EVIDENCE"
    STATE_NOT_DURABLE = "STATE_NOT_DURABLE"
    EVIDENCE_MISMATCH = "EVIDENCE_MISMATCH"
    FOREIGN_CHILD = "FOREIGN_CHILD"
    UNKNOWN_OUTCOME = "UNKNOWN_OUTCOME"
    RECONCILIATION_REJECTED = "RECONCILIATION_REJECTED"


class ItemDecision(StrEnum):
    ACKNOWLEDGE = "ACKNOWLEDGE"
    HOLD = "HOLD"
    REPAIR_REQUIRED = "REPAIR_REQUIRED"
    CONFLICT_BLOCK = "CONFLICT_BLOCK"


class RepairAction(StrEnum):
    TRANSFER_TO_COMMITTED_POSITION = "TRANSFER_TO_COMMITTED_POSITION"
    INVESTIGATE_COMMITTED_POSITION_MISSING = "INVESTIGATE_COMMITTED_POSITION_MISSING"
    ADOPTION_POLICY_REQUIRED = "ADOPTION_POLICY_REQUIRED"
    OWNERSHIP_CONFLICT_REVIEW = "OWNERSHIP_CONFLICT_REVIEW"


@dataclass(frozen=True)
class ReconciliationEvidence:
    """Immutable evidence bundle. `evidence_id` = SHA-256 of its canonical content."""

    schema_version: int
    profile_id: str
    account: AccountIdentity
    state_revision: int
    state_content_sha256: str
    snapshot: ExternalAccountSnapshot
    report: ReconciliationReport
    evidence_id: str


@dataclass(frozen=True)
class ResolutionPolicy:
    """Freshness comes through this contract, never from a wall clock."""

    max_evidence_age: timedelta


@dataclass(frozen=True)
class ResolutionRequest:
    schema_version: int
    request_id: str
    evidence: ReconciliationEvidence
    resolution_time: datetime        # caller-supplied decision time (no wall-clock reads)


@dataclass(frozen=True)
class RepairPlan:
    """Pure declarative description of a repair. EXECUTES is always False."""

    EXECUTES: ClassVar[bool] = False

    plan_id: str
    evidence_id: str
    profile_id: str
    child_trade_id: Optional[str]
    expected_current_owner: Optional[ExposureOwner]
    observed_external_state: tuple[tuple[str, ExternalObjectKind], ...]
    required_action: RepairAction
    required_transition: Optional[tuple[ExposureOwner, ExposureOwner]]
    reason: str


@dataclass(frozen=True)
class ItemResolution:
    child_trade_id: Optional[str]
    external_ids: tuple[str, ...]
    outcome: ReconciliationOutcome
    decision: ItemDecision
    plan_id: Optional[str]
    reason: str


@dataclass(frozen=True)
class ResolutionResult:
    """Immutable, deterministic resolution. Capacity effect is always NONE."""

    RELEASES_CAPACITY: ClassVar[bool] = False
    MUTATES_LIFECYCLE: ClassVar[bool] = False
    AUTHORIZES_RISK: ClassVar[bool] = False
    GRANTS_PRODUCTION_AUTHORITY: ClassVar[bool] = False

    schema_version: int
    request_id: str
    profile_id: str
    evidence_id: str
    state_revision: Optional[int]
    decision: ResolutionDecision
    reason_code: ResolutionReasonCode
    reason: str
    items: tuple[ItemResolution, ...]
    repair_plans: tuple[RepairPlan, ...]
    result_id: str

    def __post_init__(self) -> None:
        if not isinstance(self.decision, ResolutionDecision) or not isinstance(self.reason_code, ResolutionReasonCode):
            raise ContractError("decision and reason_code must be Day-7 enums")
        plan_ids = {p.plan_id for p in self.repair_plans}
        if len(plan_ids) != len(self.repair_plans):
            raise ContractError("plan_ids must be unique")
        for item in self.items:
            if item.plan_id is not None and item.plan_id not in plan_ids:
                raise ContractError("item references an unknown plan")
        if self.decision is ResolutionDecision.ACCEPT:
            if self.repair_plans or any(i.decision is not ItemDecision.ACKNOWLEDGE for i in self.items):
                raise ContractError("ACCEPT requires only acknowledged items and no plans")
        if any(i.decision is ItemDecision.CONFLICT_BLOCK for i in self.items) and \
                self.decision is not ResolutionDecision.REJECT:
            raise ContractError("any CONFLICT_BLOCK item forces REJECT")
