"""Day-6 deterministic reconciliation - REFERENCE ONLY - NON-PRODUCTION.

Pure function of (restored ReferenceLifecycleState, ExternalAccountSnapshot).
No clock, no I/O, no mutation. Capacity is never released and risk is never
authorized by reconciliation; unresolved and conflicting items are reported for
a later, explicitly authorized resolution phase.

Outcome table (per internal child_trade_id on this account):

  internal owner       external (tagged with that child)   outcome                                        class
  ACTIVE_RESERVATION   none                                ACTIVE_RESERVATION_NOT_OBSERVED                UNRESOLVED
  ACTIVE_RESERVATION   1 PENDING_ORDER                     MATCHED_ACTIVE_RESERVATION_PENDING_ORDER       MATCHED
  ACTIVE_RESERVATION   1 POSITION                          ACTIVE_RESERVATION_OBSERVED_AS_POSITION        UNRESOLVED
  COMMITTED_POSITION   1 POSITION                          MATCHED_COMMITTED_POSITION                     MATCHED
  COMMITTED_POSITION   none                                COMMITTED_POSITION_EXTERNALLY_MISSING          UNRESOLVED
  COMMITTED_POSITION   1 PENDING_ORDER                     COMMITTED_POSITION_OBSERVED_AS_PENDING_ORDER   CONFLICT
  RELEASED             none                                MATCHED_RELEASED_ABSENT                        MATCHED
  RELEASED             any                                 RELEASED_BUT_EXTERNALLY_PRESENT                CONFLICT
  any                  >1 objects                          DUPLICATE_EXTERNAL_OWNERSHIP                   CONFLICT
  ACTIVE/COMMITTED     1 object, symbol or side differs    OWNERSHIP_ATTRIBUTE_MISMATCH                   CONFLICT

External objects:
  untagged, or tag unknown to the authority            EXTERNAL_OBJECT_UNOWNED                        CONFLICT
  tag owned by a different account                     EXTERNAL_OBJECT_OWNED_BY_OTHER_ACCOUNT         CONFLICT
"""
from __future__ import annotations

from ..domain.account import AccountIdentity
from ..domain.enums import Side
from .lifecycle_contracts import ExposureOwner, ReferenceLifecycleState
from .reconciliation_contracts import (
    OUTCOME_CLASS,
    ExternalAccountSnapshot,
    ExternalObject,
    ExternalObjectKind as K,
    ReconciliationClass,
    ReconciliationItem,
    ReconciliationOutcome as O,
    ReconciliationReport,
    ReconciliationStatus as S,
)
from .validation import is_aware


def snapshot_errors(snapshot: ExternalAccountSnapshot) -> list[str]:
    errors: list[str] = []
    if not isinstance(snapshot.profile_id, str) or not snapshot.profile_id.strip():
        errors.append("snapshot.profile_id must be a non-empty string")
    if not isinstance(snapshot.account, AccountIdentity):
        errors.append("snapshot.account must be an AccountIdentity")
    if not is_aware(snapshot.observed_at):
        errors.append("snapshot.observed_at must be timezone-aware")
    if not isinstance(snapshot.objects, tuple):
        errors.append("snapshot.objects must be a tuple")
        return errors
    seen: set[str] = set()
    for i, o in enumerate(snapshot.objects):
        p = f"snapshot.objects[{i}]"
        if not isinstance(o, ExternalObject):
            errors.append(f"{p} must be an ExternalObject")
            continue
        if not isinstance(o.external_id, str) or not o.external_id.strip():
            errors.append(f"{p}.external_id must be a non-empty string")
        elif o.external_id in seen:
            errors.append(f"{p}.external_id {o.external_id} is duplicated")
        else:
            seen.add(o.external_id)
        if not isinstance(o.kind, K):
            errors.append(f"{p}.kind must be an ExternalObjectKind")
        if not isinstance(o.canonical_symbol, str) or not o.canonical_symbol.strip():
            errors.append(f"{p}.canonical_symbol must be a non-empty string")
        if not isinstance(o.side, Side):
            errors.append(f"{p}.side must be a Side")
        if o.child_trade_id is not None and (not isinstance(o.child_trade_id, str) or not o.child_trade_id.strip()):
            errors.append(f"{p}.child_trade_id must be None or a non-empty string")
    return errors


def _rejected(snapshot: ExternalAccountSnapshot, reasons: list[str]) -> ReconciliationReport:
    pid = snapshot.profile_id if isinstance(snapshot.profile_id, str) else ""
    observed = snapshot.observed_at if is_aware(snapshot.observed_at) else None
    return ReconciliationReport(pid, observed, S.REJECTED, (), tuple(reasons))


def quarantined_report(snapshot: ExternalAccountSnapshot) -> ReconciliationReport:
    return _rejected(snapshot, [f"account {snapshot.profile_id} is quarantined after a rejected restore"])


def _item(child, owner, objs, outcome: O, reason: str) -> ReconciliationItem:
    return ReconciliationItem(child, owner, tuple(sorted(o.external_id for o in objs)), outcome,
                              OUTCOME_CLASS[outcome], reason)


def reconcile_account(state: ReferenceLifecycleState, snapshot: ExternalAccountSnapshot) -> ReconciliationReport:
    if not isinstance(state, ReferenceLifecycleState):
        raise TypeError("state must be a ReferenceLifecycleState")
    if not isinstance(snapshot, ExternalAccountSnapshot):
        raise TypeError("snapshot must be an ExternalAccountSnapshot")
    errors = snapshot_errors(snapshot)
    if errors:
        return _rejected(snapshot, errors)
    pid = snapshot.profile_id
    ledger = state.authority.ledger(pid)
    if ledger is None:
        return _rejected(snapshot, [f"no restored ledger for account {pid}"])
    if ledger.account_state.account != snapshot.account:
        return _rejected(snapshot, ["snapshot account identity does not match the restored ledger"])

    internal: dict[str, tuple[ExposureOwner, str, Side]] = {}
    for r in ledger.reservations:
        internal[r.child_trade_id] = (ExposureOwner.ACTIVE_RESERVATION, r.canonical_symbol, r.side)
    for rec in state.records:
        if rec.profile_id == pid:
            internal[rec.child_trade_id] = (rec.to_owner, rec.canonical_symbol, rec.side)
    other_account_children = {r.child_trade_id for l in state.authority.ledgers if l.profile_id != pid
                              for r in l.reservations}
    other_account_children |= {rec.child_trade_id for rec in state.records if rec.profile_id != pid}

    items: list[ReconciliationItem] = []
    claims: dict[str, list[ExternalObject]] = {}
    for obj in sorted(snapshot.objects, key=lambda o: o.external_id):
        tag = obj.child_trade_id
        if tag is not None and tag in internal:
            claims.setdefault(tag, []).append(obj)
        elif tag is not None and tag in other_account_children:
            items.append(_item(tag, None, [obj], O.EXTERNAL_OBJECT_OWNED_BY_OTHER_ACCOUNT,
                               f"{obj.external_id} is tagged {tag}, owned by another account"))
        else:
            items.append(_item(tag, None, [obj], O.EXTERNAL_OBJECT_UNOWNED,
                               f"{obj.external_id} has no internal owner; its exposure is not in capacity accounting"))

    for child in sorted(internal):
        owner, symbol, side = internal[child]
        objs = claims.get(child, [])
        if len(objs) > 1:
            items.append(_item(child, owner, objs, O.DUPLICATE_EXTERNAL_OWNERSHIP,
                               f"{len(objs)} external objects claim {child}"))
            continue
        obj = objs[0] if objs else None
        if obj is not None and owner is not ExposureOwner.RELEASED and (obj.canonical_symbol, obj.side) != (symbol, side):
            items.append(_item(child, owner, objs, O.OWNERSHIP_ATTRIBUTE_MISMATCH,
                               f"external {obj.canonical_symbol}/{obj.side} != internal {symbol}/{side}"))
            continue
        if owner is ExposureOwner.ACTIVE_RESERVATION:
            outcome = (O.ACTIVE_RESERVATION_NOT_OBSERVED if obj is None
                       else O.MATCHED_ACTIVE_RESERVATION_PENDING_ORDER if obj.kind is K.PENDING_ORDER
                       else O.ACTIVE_RESERVATION_OBSERVED_AS_POSITION)
        elif owner is ExposureOwner.COMMITTED_POSITION:
            outcome = (O.COMMITTED_POSITION_EXTERNALLY_MISSING if obj is None
                       else O.MATCHED_COMMITTED_POSITION if obj.kind is K.POSITION
                       else O.COMMITTED_POSITION_OBSERVED_AS_PENDING_ORDER)
        else:
            outcome = O.MATCHED_RELEASED_ABSENT if obj is None else O.RELEASED_BUT_EXTERNALLY_PRESENT
        items.append(_item(child, owner, objs, outcome, f"{owner} vs {obj.kind if obj else 'no external object'}; capacity unchanged"))

    items.sort(key=lambda i: (i.child_trade_id or "", i.external_ids, i.outcome.value))
    classes = {i.classification for i in items}
    status = (S.CONFLICT if ReconciliationClass.CONFLICT in classes
              else S.UNRESOLVED if ReconciliationClass.UNRESOLVED in classes else S.CLEAN)
    return ReconciliationReport(pid, snapshot.observed_at, status, tuple(items), ())
