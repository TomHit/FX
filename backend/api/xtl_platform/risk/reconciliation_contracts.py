"""Day-6 reconciliation CONTRACTS - REFERENCE ONLY - NON-PRODUCTION.

External snapshots are INPUT DATA ONLY. Nothing here fetches MT5 or broker state.
A reconciliation report never releases capacity, never authorizes risk and never
mutates authority state.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import ClassVar, Optional

from ..domain._validation import ContractError
from ..domain.account import AccountIdentity
from ..domain.enums import Side, StrEnum
from .lifecycle_contracts import ExposureOwner


class ExternalObjectKind(StrEnum):
    PENDING_ORDER = "PENDING_ORDER"
    POSITION = "POSITION"


@dataclass(frozen=True)
class ExternalObject:
    """One externally observed order or position. `child_trade_id` is the ownership tag
    observed on the external object (None when untagged)."""

    external_id: str
    kind: ExternalObjectKind
    canonical_symbol: str
    side: Side
    child_trade_id: Optional[str]


@dataclass(frozen=True)
class ExternalAccountSnapshot:
    profile_id: str
    account: AccountIdentity
    observed_at: datetime
    objects: tuple[ExternalObject, ...]


class ReconciliationOutcome(StrEnum):
    MATCHED_ACTIVE_RESERVATION_PENDING_ORDER = "MATCHED_ACTIVE_RESERVATION_PENDING_ORDER"
    MATCHED_COMMITTED_POSITION = "MATCHED_COMMITTED_POSITION"
    MATCHED_RELEASED_ABSENT = "MATCHED_RELEASED_ABSENT"
    ACTIVE_RESERVATION_NOT_OBSERVED = "ACTIVE_RESERVATION_NOT_OBSERVED"
    ACTIVE_RESERVATION_OBSERVED_AS_POSITION = "ACTIVE_RESERVATION_OBSERVED_AS_POSITION"
    COMMITTED_POSITION_EXTERNALLY_MISSING = "COMMITTED_POSITION_EXTERNALLY_MISSING"
    COMMITTED_POSITION_OBSERVED_AS_PENDING_ORDER = "COMMITTED_POSITION_OBSERVED_AS_PENDING_ORDER"
    RELEASED_BUT_EXTERNALLY_PRESENT = "RELEASED_BUT_EXTERNALLY_PRESENT"
    EXTERNAL_OBJECT_UNOWNED = "EXTERNAL_OBJECT_UNOWNED"
    EXTERNAL_OBJECT_OWNED_BY_OTHER_ACCOUNT = "EXTERNAL_OBJECT_OWNED_BY_OTHER_ACCOUNT"
    DUPLICATE_EXTERNAL_OWNERSHIP = "DUPLICATE_EXTERNAL_OWNERSHIP"
    OWNERSHIP_ATTRIBUTE_MISMATCH = "OWNERSHIP_ATTRIBUTE_MISMATCH"


class ReconciliationClass(StrEnum):
    MATCHED = "MATCHED"
    UNRESOLVED = "UNRESOLVED"
    CONFLICT = "CONFLICT"


O = ReconciliationOutcome
OUTCOME_CLASS = {
    O.MATCHED_ACTIVE_RESERVATION_PENDING_ORDER: ReconciliationClass.MATCHED,
    O.MATCHED_COMMITTED_POSITION: ReconciliationClass.MATCHED,
    O.MATCHED_RELEASED_ABSENT: ReconciliationClass.MATCHED,
    O.ACTIVE_RESERVATION_NOT_OBSERVED: ReconciliationClass.UNRESOLVED,
    O.ACTIVE_RESERVATION_OBSERVED_AS_POSITION: ReconciliationClass.UNRESOLVED,
    O.COMMITTED_POSITION_EXTERNALLY_MISSING: ReconciliationClass.UNRESOLVED,
    O.COMMITTED_POSITION_OBSERVED_AS_PENDING_ORDER: ReconciliationClass.CONFLICT,
    O.RELEASED_BUT_EXTERNALLY_PRESENT: ReconciliationClass.CONFLICT,
    O.EXTERNAL_OBJECT_UNOWNED: ReconciliationClass.CONFLICT,
    O.EXTERNAL_OBJECT_OWNED_BY_OTHER_ACCOUNT: ReconciliationClass.CONFLICT,
    O.DUPLICATE_EXTERNAL_OWNERSHIP: ReconciliationClass.CONFLICT,
    O.OWNERSHIP_ATTRIBUTE_MISMATCH: ReconciliationClass.CONFLICT,
}


class ReconciliationStatus(StrEnum):
    CLEAN = "CLEAN"            # every item MATCHED
    UNRESOLVED = "UNRESOLVED"  # at least one UNRESOLVED, no CONFLICT
    CONFLICT = "CONFLICT"      # at least one CONFLICT
    REJECTED = "REJECTED"      # input invalid, unknown/mismatched/quarantined account


@dataclass(frozen=True)
class ReconciliationItem:
    child_trade_id: Optional[str]
    internal_owner: Optional[ExposureOwner]
    external_ids: tuple[str, ...]
    outcome: ReconciliationOutcome
    classification: ReconciliationClass
    reason: str

    def __post_init__(self) -> None:
        if OUTCOME_CLASS.get(self.outcome) is not self.classification:
            raise ContractError("classification does not match outcome")


@dataclass(frozen=True)
class ReconciliationReport:
    """REFERENCE ONLY. Capacity effect is always NONE."""

    RELEASES_CAPACITY: ClassVar[bool] = False
    AUTHORIZES_RISK: ClassVar[bool] = False
    MUTATES_STATE: ClassVar[bool] = False

    profile_id: str
    observed_at: Optional[datetime]
    status: ReconciliationStatus
    items: tuple[ReconciliationItem, ...]
    rejection_reasons: tuple[str, ...]

    def __post_init__(self) -> None:
        rejected = self.status is ReconciliationStatus.REJECTED
        if rejected != bool(self.rejection_reasons) or (rejected and self.items):
            raise ContractError("REJECTED if and only if rejection reasons are present (and no items)")
        if not rejected:
            classes = {i.classification for i in self.items}
            expected = (ReconciliationStatus.CONFLICT if ReconciliationClass.CONFLICT in classes
                        else ReconciliationStatus.UNRESOLVED if ReconciliationClass.UNRESOLVED in classes
                        else ReconciliationStatus.CLEAN)
            if self.status is not expected:
                raise ContractError(f"status must be {expected} for these items")
