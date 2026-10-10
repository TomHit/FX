"""Day-6 persistence CODEC - REFERENCE ONLY - NON-PRODUCTION.

Pure, strict, versioned encoding of ONE account's authoritative state.

Envelope (canonical JSON: sorted keys, no whitespace, ASCII):
    {"schema": SCHEMA_NAME, "schema_version": 1, "profile_id": str, "revision": int,
     "content_sha256": hex, "content": {...}}

Content (schema_version 1):
    account_state      committed (non-reserved) ReferenceAccountState incl. limits
    reservations       ACTIVE ReferenceReservations in ledger order, each with the
                       authorizing ChildExecutionIntent and Day-3 RiskEvaluation evidence
    lifecycle_records  ReservationLifecycleRecords with their authority-global sequence

NOT persisted (derived on restore): Day-3 snapshots, open-risk/slot totals,
exposure owners, request fingerprints, transferred risk/slots.

Decoding is strict: exact key sets, exact JSON types, finite Decimals encoded as
strings, timezone-aware ISO datetimes, known enum values. Any deviation raises
PersistenceDecodeError. Nothing is coerced or defaulted.
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Optional

from ..domain.account import AccountIdentity
from ..domain.enums import LossModel, Side, StrategyId, Venue
from ..domain.thesis import ChildExecutionIntent
from .atomic_contracts import ReferenceAccountState, ReferenceLedger, ReferenceReservation, ReferenceReservationStatus
from .contracts import (
    CapacityCheck,
    CapacityCheckName,
    CapacityUnit,
    FloatingPnlTreatment,
    RiskEvaluation,
    RiskEvaluationCode,
    RiskEvaluationLimits,
)
from .lifecycle_contracts import ExposureOwner, ReservationLifecycleRecord
from .validation import is_finite_decimal, limits_errors
from .persistence_contracts import (
    SCHEMA_NAME,
    SCHEMA_VERSION,
    SUPPORTED_SCHEMA_VERSIONS,
    PersistenceDecodeError,
    PersistenceEncodeError,
    RestoreRejectionCode as RC,
    StoredDocument,
)

_DECIMAL_RE = re.compile(r"^-?\d+(\.\d+)?([Ee][+-]?\d+)?$")
_ENVELOPE_KEYS = {"schema", "schema_version", "profile_id", "revision", "content_sha256", "content"}


def canonical_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def content_digest(content: Any) -> str:
    return hashlib.sha256(canonical_json(content).encode("ascii")).hexdigest()


# ---- value validation (applied symmetrically on encode and decode) ------------------------

def account_state_value_errors(s: ReferenceAccountState) -> list[str]:
    """Values a persisted account state must satisfy. The frozen Day-4 type does not
    range-check these; persistence refuses to write or restore state that Day 3 could
    never evaluate, so a bad value fails closed at the storage boundary."""
    errors: list[str] = []

    def positive(name, v, allow_zero=False):
        if not is_finite_decimal(v) or (v < 0 if allow_zero else v <= 0):
            errors.append(f"account_state.{name} must be a finite Decimal {'>= 0' if allow_zero else '> 0'}")

    positive("balance", s.balance)
    positive("equity", s.equity, allow_zero=True)
    if not is_finite_decimal(s.realized_daily_pnl):
        errors.append("account_state.realized_daily_pnl must be a finite Decimal")
    positive("committed_open_risk_usd", s.committed_open_risk_usd, allow_zero=True)
    if type(s.committed_open_positions) is not int or s.committed_open_positions < 0:
        errors.append("account_state.committed_open_positions must be an int >= 0")
    if s.max_loss_reference_balance is not None:
        positive("max_loss_reference_balance", s.max_loss_reference_balance)
    errors.extend(limits_errors(s.limits))
    return errors


# ---- encode -------------------------------------------------------------------------------

def _dec(v: Any, path: str) -> str:
    if not isinstance(v, Decimal) or not v.is_finite():
        raise PersistenceEncodeError(f"{path} must be a finite Decimal")
    return str(v)


def _opt_dec(v: Any, path: str) -> Optional[str]:
    return None if v is None else _dec(v, path)


def _dt(v: Any, path: str) -> str:
    if not isinstance(v, datetime) or v.tzinfo is None or v.utcoffset() is None:
        raise PersistenceEncodeError(f"{path} must be a timezone-aware datetime")
    return v.isoformat()


def _int(v: Any, path: str) -> int:
    if type(v) is not int:
        raise PersistenceEncodeError(f"{path} must be an int")
    return v


def _bool(v: Any, path: str) -> bool:
    if type(v) is not bool:
        raise PersistenceEncodeError(f"{path} must be a bool")
    return v


def _str(v: Any, path: str) -> str:
    if not isinstance(v, str):
        raise PersistenceEncodeError(f"{path} must be a string")
    return v


def _enum(v: Any, enum_type: type, path: str) -> str:
    if not isinstance(v, enum_type):
        raise PersistenceEncodeError(f"{path} must be a {enum_type.__name__}")
    return v.value


def _opt_enum(v: Any, enum_type: type, path: str) -> Optional[str]:
    return None if v is None else _enum(v, enum_type, path)


def _account(a: AccountIdentity, p: str) -> dict:
    if not isinstance(a, AccountIdentity):
        raise PersistenceEncodeError(f"{p} must be an AccountIdentity")
    return {"login": _str(a.login, p), "server": _str(a.server, p), "company": _str(a.company, p),
            "currency": _str(a.currency, p)}


def _limits(l: RiskEvaluationLimits, p: str) -> dict:
    return {
        "profile_id": _str(l.profile_id, p), "source_policy_version": _str(l.source_policy_version, p),
        "max_risk_per_trade_usd": _dec(l.max_risk_per_trade_usd, p), "max_positions": _int(l.max_positions, p),
        "max_open_risk_usd": _dec(l.max_open_risk_usd, p),
        "daily_loss_model": _enum(l.daily_loss_model, LossModel, p),
        "daily_loss_limit_usd": _opt_dec(l.daily_loss_limit_usd, p),
        "daily_floating_pnl": _opt_enum(l.daily_floating_pnl, FloatingPnlTreatment, p),
        "max_loss_model": _enum(l.max_loss_model, LossModel, p),
        "max_loss_limit_usd": _opt_dec(l.max_loss_limit_usd, p),
        "max_loss_floating_pnl": _opt_enum(l.max_loss_floating_pnl, FloatingPnlTreatment, p),
    }


def _account_state(s: ReferenceAccountState) -> dict:
    p = "account_state"
    return {
        "profile_id": _str(s.profile_id, p), "account": _account(s.account, p), "as_of": _dt(s.as_of, p),
        "balance": _dec(s.balance, p), "equity": _dec(s.equity, p),
        "realized_daily_pnl": _dec(s.realized_daily_pnl, p),
        "committed_open_risk_usd": _dec(s.committed_open_risk_usd, p),
        "committed_open_positions": _int(s.committed_open_positions, p),
        "account_halted": _bool(s.account_halted, p), "daily_halted": _bool(s.daily_halted, p),
        "max_loss_reference_balance": _opt_dec(s.max_loss_reference_balance, p),
        "limits": _limits(s.limits, p + ".limits"),
    }


def _intent(i: ChildExecutionIntent, p: str) -> dict:
    return {
        "thesis_id": _str(i.thesis_id, p), "strategy_id": _enum(i.strategy_id, StrategyId, p),
        "profile_id": _str(i.profile_id, p), "firm_id": _str(i.firm_id, p), "account": _account(i.account, p),
        "device_id": _str(i.device_id, p), "venue": _enum(i.venue, Venue, p),
        "canonical_symbol": _str(i.canonical_symbol, p), "venue_symbol": _str(i.venue_symbol, p),
        "side": _enum(i.side, Side, p), "entry": _dec(i.entry, p), "stop": _dec(i.stop, p),
        "target": _opt_dec(i.target, p), "created_at": _dt(i.created_at, p), "expires_at": _dt(i.expires_at, p),
    }


def _check(c: CapacityCheck, p: str) -> dict:
    return {
        "name": _enum(c.name, CapacityCheckName, p), "unit": _enum(c.unit, CapacityUnit, p),
        "limit": _dec(c.limit, p), "used": _opt_dec(c.used, p), "proposed": _dec(c.proposed, p),
        "projected": _dec(c.projected, p), "remaining_before": _dec(c.remaining_before, p),
        "remaining_after": _dec(c.remaining_after, p), "fits": _bool(c.fits, p),
        "components": [[_str(k, p), _dec(v, p)] for k, v in c.components],
    }


def _evaluation(e: RiskEvaluation, p: str) -> dict:
    return {
        "thesis_id": _str(e.thesis_id, p), "child_trade_id": _str(e.child_trade_id, p),
        "strategy_id": _enum(e.strategy_id, StrategyId, p), "profile_id": _str(e.profile_id, p),
        "account": _account(e.account, p), "canonical_symbol": _str(e.canonical_symbol, p),
        "side": _enum(e.side, Side, p), "code": _enum(e.code, RiskEvaluationCode, p), "reason": _str(e.reason, p),
        "evaluated_at": None if e.evaluated_at is None else _dt(e.evaluated_at, p),
        "snapshot_at": None if e.snapshot_at is None else _dt(e.snapshot_at, p),
        "proposed_risk_usd": _opt_dec(e.proposed_risk_usd, p),
        "source_policy_version": e.source_policy_version,
        "checks": [_check(c, f"{p}.checks") for c in e.checks],
        "skipped_checks": [_enum(n, CapacityCheckName, p) for n in e.skipped_checks],
        "decisive_check_index": None if e.decisive_check is None else e.checks.index(e.decisive_check),
        "input_errors": [_str(x, p) for x in e.input_errors],
    }


def _reservation(r: ReferenceReservation, p: str) -> dict:
    return {
        "reservation_id": _str(r.reservation_id, p), "risk_usd": _dec(r.risk_usd, p),
        "source_policy_version": _str(r.source_policy_version, p), "created_at": _dt(r.created_at, p),
        "status": _enum(r.status, ReferenceReservationStatus, p),
        "intent": _intent(r.request_fingerprint[0], p + ".intent"),
        "authorizing_evaluation": _evaluation(r.authorizing_evaluation, p + ".authorizing_evaluation"),
    }


def _record(sequence: int, r: ReservationLifecycleRecord, p: str) -> dict:
    return {
        "sequence": _int(sequence, p), "transition_id": _str(r.transition_id, p),
        "from_owner": _enum(r.from_owner, ExposureOwner, p), "to_owner": _enum(r.to_owner, ExposureOwner, p),
        "transitioned_at": _dt(r.transitioned_at, p), "reason": _str(r.reason, p),
        "reservation": _reservation(r.reservation, p + ".reservation"),
    }


def encode_account_document(ledger: ReferenceLedger, records: tuple[tuple[int, ReservationLifecycleRecord], ...],
                            revision: int) -> StoredDocument:
    profile_id = ledger.profile_id
    value_errors = account_state_value_errors(ledger.account_state)
    if value_errors:
        raise PersistenceEncodeError("; ".join(value_errors))
    for i, r in enumerate(ledger.reservations):
        if r.request_fingerprint != (r.request_fingerprint[0], r.risk_usd, r.source_policy_version):
            raise PersistenceEncodeError(f"reservations[{i}] fingerprint is not derivable; refusing to persist")
    content = {
        "account_state": _account_state(ledger.account_state),
        "reservations": [_reservation(r, f"reservations[{i}]") for i, r in enumerate(ledger.reservations)],
        "lifecycle_records": [_record(seq, r, f"lifecycle_records[{i}]") for i, (seq, r) in enumerate(records)],
    }
    envelope = {"schema": SCHEMA_NAME, "schema_version": SCHEMA_VERSION, "profile_id": profile_id,
                "revision": revision, "content_sha256": content_digest(content), "content": content}
    return StoredDocument(profile_id=profile_id, revision=revision, payload=canonical_json(envelope))


# ---- decode ---------------------------------------------------------------------------------

@dataclass(frozen=True)
class DecodedAccount:
    profile_id: str
    revision: int
    ledger: ReferenceLedger
    records: tuple[tuple[int, ReservationLifecycleRecord], ...]


class _Reader:
    def __init__(self) -> None:
        self.errors: list[str] = []

    def fail(self, path: str, msg: str) -> None:
        raise PersistenceDecodeError(RC.INVALID_RECORD, [f"{path}: {msg}"])

    def obj(self, v: Any, path: str, keys: set[str]) -> dict:
        if not isinstance(v, dict):
            self.fail(path, "must be an object")
        if set(v) != keys:
            missing, extra = sorted(keys - set(v)), sorted(set(v) - keys)
            self.fail(path, f"keys mismatch (missing={missing}, unexpected={extra})")
        return v

    def lst(self, v: Any, path: str) -> list:
        if not isinstance(v, list):
            self.fail(path, "must be a list")
        return v

    def s(self, v: Any, path: str) -> str:
        if not isinstance(v, str):
            self.fail(path, "must be a string")
        return v

    def i(self, v: Any, path: str) -> int:
        if type(v) is not int:
            self.fail(path, "must be an integer")
        return v

    def b(self, v: Any, path: str) -> bool:
        if type(v) is not bool:
            self.fail(path, "must be a boolean")
        return v

    def d(self, v: Any, path: str) -> Decimal:
        if not isinstance(v, str) or not _DECIMAL_RE.match(v):
            self.fail(path, "must be a decimal string")
        try:
            out = Decimal(v)
        except InvalidOperation:
            self.fail(path, "invalid decimal")
        if not out.is_finite():
            self.fail(path, "must be finite")
        return out

    def od(self, v: Any, path: str) -> Optional[Decimal]:
        return None if v is None else self.d(v, path)

    def dt(self, v: Any, path: str) -> datetime:
        if not isinstance(v, str):
            self.fail(path, "must be an ISO datetime string")
        try:
            out = datetime.fromisoformat(v)
        except ValueError:
            self.fail(path, "invalid ISO datetime")
        if out.tzinfo is None or out.utcoffset() is None:
            self.fail(path, "datetime must be timezone-aware")
        return out

    def odt(self, v: Any, path: str) -> Optional[datetime]:
        return None if v is None else self.dt(v, path)

    def e(self, v: Any, enum_type: type, path: str):
        if not isinstance(v, str):
            self.fail(path, f"must be a {enum_type.__name__} value")
        try:
            return enum_type(v)
        except ValueError:
            self.fail(path, f"unknown {enum_type.__name__} value {v!r}")

    def oe(self, v: Any, enum_type: type, path: str):
        return None if v is None else self.e(v, enum_type, path)

    def build(self, path: str, fn: Callable[[], Any]) -> Any:
        try:
            return fn()
        except PersistenceDecodeError:
            raise
        except Exception as exc:  # noqa: BLE001 - domain constructors validate; any failure is invalid data
            self.fail(path, f"{type(exc).__name__}: {exc}")


def _r_account(R: _Reader, v: Any, p: str) -> AccountIdentity:
    o = R.obj(v, p, {"login", "server", "company", "currency"})
    return R.build(p, lambda: AccountIdentity(R.s(o["login"], p), R.s(o["server"], p), R.s(o["company"], p),
                                              R.s(o["currency"], p)))


def _r_limits(R: _Reader, v: Any, p: str) -> RiskEvaluationLimits:
    o = R.obj(v, p, {"profile_id", "source_policy_version", "max_risk_per_trade_usd", "max_positions",
                     "max_open_risk_usd", "daily_loss_model", "daily_loss_limit_usd", "daily_floating_pnl",
                     "max_loss_model", "max_loss_limit_usd", "max_loss_floating_pnl"})
    return R.build(p, lambda: RiskEvaluationLimits(
        profile_id=R.s(o["profile_id"], p), source_policy_version=R.s(o["source_policy_version"], p),
        max_risk_per_trade_usd=R.d(o["max_risk_per_trade_usd"], p), max_positions=R.i(o["max_positions"], p),
        max_open_risk_usd=R.d(o["max_open_risk_usd"], p),
        daily_loss_model=R.e(o["daily_loss_model"], LossModel, p),
        daily_loss_limit_usd=R.od(o["daily_loss_limit_usd"], p),
        daily_floating_pnl=R.oe(o["daily_floating_pnl"], FloatingPnlTreatment, p),
        max_loss_model=R.e(o["max_loss_model"], LossModel, p),
        max_loss_limit_usd=R.od(o["max_loss_limit_usd"], p),
        max_loss_floating_pnl=R.oe(o["max_loss_floating_pnl"], FloatingPnlTreatment, p)))


def _r_account_state(R: _Reader, v: Any) -> ReferenceAccountState:
    p = "content.account_state"
    o = R.obj(v, p, {"profile_id", "account", "as_of", "balance", "equity", "realized_daily_pnl",
                     "committed_open_risk_usd", "committed_open_positions", "account_halted", "daily_halted",
                     "max_loss_reference_balance", "limits"})
    return R.build(p, lambda: ReferenceAccountState(
        profile_id=R.s(o["profile_id"], p), account=_r_account(R, o["account"], p + ".account"),
        as_of=R.dt(o["as_of"], p), balance=R.d(o["balance"], p), equity=R.d(o["equity"], p),
        realized_daily_pnl=R.d(o["realized_daily_pnl"], p),
        committed_open_risk_usd=R.d(o["committed_open_risk_usd"], p),
        committed_open_positions=R.i(o["committed_open_positions"], p),
        account_halted=R.b(o["account_halted"], p), daily_halted=R.b(o["daily_halted"], p),
        max_loss_reference_balance=R.od(o["max_loss_reference_balance"], p),
        limits=_r_limits(R, o["limits"], p + ".limits")))


def _r_intent(R: _Reader, v: Any, p: str) -> ChildExecutionIntent:
    o = R.obj(v, p, {"thesis_id", "strategy_id", "profile_id", "firm_id", "account", "device_id", "venue",
                     "canonical_symbol", "venue_symbol", "side", "entry", "stop", "target", "created_at",
                     "expires_at"})
    return R.build(p, lambda: ChildExecutionIntent(
        thesis_id=R.s(o["thesis_id"], p), strategy_id=R.e(o["strategy_id"], StrategyId, p),
        profile_id=R.s(o["profile_id"], p), firm_id=R.s(o["firm_id"], p),
        account=_r_account(R, o["account"], p + ".account"), device_id=R.s(o["device_id"], p),
        venue=R.e(o["venue"], Venue, p), canonical_symbol=R.s(o["canonical_symbol"], p),
        venue_symbol=R.s(o["venue_symbol"], p), side=R.e(o["side"], Side, p), entry=R.d(o["entry"], p),
        stop=R.d(o["stop"], p), target=R.od(o["target"], p), created_at=R.dt(o["created_at"], p),
        expires_at=R.dt(o["expires_at"], p)))


def _r_check(R: _Reader, v: Any, p: str) -> CapacityCheck:
    o = R.obj(v, p, {"name", "unit", "limit", "used", "proposed", "projected", "remaining_before",
                     "remaining_after", "fits", "components"})
    components = []
    for j, pair in enumerate(R.lst(o["components"], p + ".components")):
        pp = f"{p}.components[{j}]"
        if not isinstance(pair, list) or len(pair) != 2:
            R.fail(pp, "must be a [name, decimal] pair")
        components.append((R.s(pair[0], pp), R.d(pair[1], pp)))
    return R.build(p, lambda: CapacityCheck(
        name=R.e(o["name"], CapacityCheckName, p), unit=R.e(o["unit"], CapacityUnit, p), limit=R.d(o["limit"], p),
        used=R.od(o["used"], p), proposed=R.d(o["proposed"], p), projected=R.d(o["projected"], p),
        remaining_before=R.d(o["remaining_before"], p), remaining_after=R.d(o["remaining_after"], p),
        fits=R.b(o["fits"], p), components=tuple(components)))


def _r_evaluation(R: _Reader, v: Any, p: str) -> RiskEvaluation:
    o = R.obj(v, p, {"thesis_id", "child_trade_id", "strategy_id", "profile_id", "account", "canonical_symbol",
                     "side", "code", "reason", "evaluated_at", "snapshot_at", "proposed_risk_usd",
                     "source_policy_version", "checks", "skipped_checks", "decisive_check_index", "input_errors"})
    checks = tuple(_r_check(R, c, f"{p}.checks[{j}]") for j, c in enumerate(R.lst(o["checks"], p + ".checks")))
    idx = o["decisive_check_index"]
    if idx is not None and (type(idx) is not int or not 0 <= idx < len(checks)):
        R.fail(p + ".decisive_check_index", "out of range")
    version = o["source_policy_version"]
    if version is not None:
        R.s(version, p + ".source_policy_version")
    return R.build(p, lambda: RiskEvaluation(
        thesis_id=R.s(o["thesis_id"], p), child_trade_id=R.s(o["child_trade_id"], p),
        strategy_id=R.e(o["strategy_id"], StrategyId, p), profile_id=R.s(o["profile_id"], p),
        account=_r_account(R, o["account"], p + ".account"), canonical_symbol=R.s(o["canonical_symbol"], p),
        side=R.e(o["side"], Side, p), code=R.e(o["code"], RiskEvaluationCode, p), reason=R.s(o["reason"], p),
        evaluated_at=R.odt(o["evaluated_at"], p), snapshot_at=R.odt(o["snapshot_at"], p),
        proposed_risk_usd=R.od(o["proposed_risk_usd"], p), source_policy_version=version, checks=checks,
        skipped_checks=tuple(R.e(n, CapacityCheckName, p) for n in R.lst(o["skipped_checks"], p)),
        decisive_check=None if idx is None else checks[idx],
        input_errors=tuple(R.s(x, p) for x in R.lst(o["input_errors"], p))))


def _r_reservation(R: _Reader, v: Any, p: str) -> ReferenceReservation:
    o = R.obj(v, p, {"reservation_id", "risk_usd", "source_policy_version", "created_at", "status", "intent",
                     "authorizing_evaluation"})
    intent = _r_intent(R, o["intent"], p + ".intent")
    evaluation = _r_evaluation(R, o["authorizing_evaluation"], p + ".authorizing_evaluation")
    risk, version = R.d(o["risk_usd"], p), R.s(o["source_policy_version"], p)
    return R.build(p, lambda: ReferenceReservation(
        reservation_id=R.s(o["reservation_id"], p), child_trade_id=intent.child_trade_id,
        thesis_id=intent.thesis_id, strategy_id=intent.strategy_id, profile_id=intent.profile_id,
        account=intent.account, canonical_symbol=intent.canonical_symbol, side=intent.side, risk_usd=risk,
        source_policy_version=version, created_at=R.dt(o["created_at"], p),
        status=R.e(o["status"], ReferenceReservationStatus, p), authorizing_evaluation=evaluation,
        request_fingerprint=(intent, risk, version)))


def _r_record(R: _Reader, v: Any, p: str) -> tuple[int, ReservationLifecycleRecord]:
    o = R.obj(v, p, {"sequence", "transition_id", "from_owner", "to_owner", "transitioned_at", "reason",
                     "reservation"})
    seq = R.i(o["sequence"], p + ".sequence")
    if seq < 0:
        R.fail(p + ".sequence", "must be >= 0")
    res = _r_reservation(R, o["reservation"], p + ".reservation")
    to_owner = R.e(o["to_owner"], ExposureOwner, p)
    transition_id, reason = R.s(o["transition_id"], p), R.s(o["reason"], p)
    committed = to_owner is ExposureOwner.COMMITTED_POSITION
    record = R.build(p, lambda: ReservationLifecycleRecord(
        transition_id=transition_id, reservation=res, from_owner=R.e(o["from_owner"], ExposureOwner, p),
        to_owner=to_owner, transitioned_at=R.dt(o["transitioned_at"], p), reason=reason,
        request_fingerprint=(transition_id, res.child_trade_id, res.reservation_id, res.profile_id, res.account,
                             to_owner, reason),
        transferred_risk_usd=res.risk_usd if committed else Decimal("0"),
        transferred_position_slots=1 if committed else 0))
    return seq, record


def decode_account_document(stored: StoredDocument) -> DecodedAccount:
    """Strictly decode one stored account document or raise PersistenceDecodeError."""
    if not isinstance(stored, StoredDocument):
        raise PersistenceDecodeError(RC.MALFORMED_DOCUMENT, ["stored document has the wrong type"])
    try:
        envelope = json.loads(stored.payload)
    except (ValueError, TypeError) as exc:
        raise PersistenceDecodeError(RC.MALFORMED_DOCUMENT, [f"payload is not valid JSON: {type(exc).__name__}"])
    if not isinstance(envelope, dict) or set(envelope) != _ENVELOPE_KEYS:
        raise PersistenceDecodeError(RC.MALFORMED_DOCUMENT, ["envelope keys mismatch"])
    if envelope["schema"] != SCHEMA_NAME:
        raise PersistenceDecodeError(RC.MALFORMED_DOCUMENT, [f"unknown schema {envelope['schema']!r}"])
    version = envelope["schema_version"]
    if type(version) is not int or version not in SUPPORTED_SCHEMA_VERSIONS:
        raise PersistenceDecodeError(RC.UNSUPPORTED_SCHEMA_VERSION, [f"schema_version {version!r} is not supported"])
    if envelope["profile_id"] != stored.profile_id:
        raise PersistenceDecodeError(RC.PROFILE_MISMATCH, ["envelope profile_id != repository key"])
    if envelope["revision"] != stored.revision or type(envelope["revision"]) is not int:
        raise PersistenceDecodeError(RC.REVISION_MISMATCH, ["envelope revision != repository revision"])
    content = envelope["content"]
    if not isinstance(envelope["content_sha256"], str) or content_digest(content) != envelope["content_sha256"]:
        raise PersistenceDecodeError(RC.INTEGRITY_CHECK_FAILED, ["content_sha256 does not match content"])

    R = _Reader()
    c = R.obj(content, "content", {"account_state", "reservations", "lifecycle_records"})
    account_state = _r_account_state(R, c["account_state"])
    if account_state.profile_id != stored.profile_id:
        raise PersistenceDecodeError(RC.PROFILE_MISMATCH, ["account_state.profile_id != repository key"])
    value_errors = account_state_value_errors(account_state)
    if value_errors:
        raise PersistenceDecodeError(RC.INVALID_RECORD, value_errors)
    reservations = tuple(_r_reservation(R, r, f"content.reservations[{i}]")
                         for i, r in enumerate(R.lst(c["reservations"], "content.reservations")))
    records = tuple(_r_record(R, r, f"content.lifecycle_records[{i}]")
                    for i, r in enumerate(R.lst(c["lifecycle_records"], "content.lifecycle_records")))
    ledger = R.build("content", lambda: ReferenceLedger(account_state, reservations))
    for _, record in records:
        if (record.profile_id, record.account) != (account_state.profile_id, account_state.account):
            raise PersistenceDecodeError(RC.OWNERSHIP_CONFLICT, [f"record {record.transition_id} belongs to another account"])
    child_ids = [r.child_trade_id for r in reservations] + [r.child_trade_id for _, r in records]
    if len(set(child_ids)) != len(child_ids):
        raise PersistenceDecodeError(RC.OWNERSHIP_CONFLICT, ["a child_trade_id has more than one owner in this account"])
    tids = [r.transition_id for _, r in records]
    seqs = [s for s, _ in records]
    if len(set(tids)) != len(tids) or len(set(seqs)) != len(seqs):
        raise PersistenceDecodeError(RC.OWNERSHIP_CONFLICT, ["duplicate transition_id or sequence in this account"])
    return DecodedAccount(stored.profile_id, stored.revision, ledger, records)
