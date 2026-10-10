"""Common execution-adapter contract (PRD sections 10, 11, 21).

MT5 and futures adapters implement `_submit`, `cancel` and `fetch_positions`.
`place` is final: it enforces the guards every venue must share.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import ClassVar

from ..domain.enums import AckStatus, Venue
from ..domain.execution import BrokerPosition, ExecutionAck, ExecutionCommand
from ..risk.authority import RiskAuthority


class AdapterContractError(RuntimeError):
    pass


class ExecutionAdapter(ABC):
    venue: ClassVar[Venue]

    def __init__(self, authority: RiskAuthority) -> None:
        if not isinstance(getattr(type(self), "venue", None), Venue):
            raise AdapterContractError(f"{type(self).__name__} must declare a Venue")
        self._authority = authority

    def place(self, command: ExecutionCommand, now: datetime) -> ExecutionAck:
        res = command.reservation
        if res.venue is not self.venue:
            raise AdapterContractError(f"{self.venue} adapter received {res.venue} command")

        def reject(reason: str) -> ExecutionAck:
            return ExecutionAck(command.command_id, command.trade_id, AckStatus.REJECTED, None, reason, now)

        if now >= command.expires_at:
            return reject("command expired before send")
        if not self._authority.mark_sent(res.authorization_id, now):
            return reject("reservation not active; not sent")

        # From here the reservation is pinned. Any failure is an UNKNOWN
        # outcome that only broker reconciliation may resolve.
        try:
            ack = self._submit(command)
        except Exception as exc:  # noqa: BLE001 - transport errors must not crash fan-out
            return ExecutionAck(command.command_id, command.trade_id, AckStatus.NO_ACK, None,
                                f"transport error: {exc}", now)
        if ack.trade_id != command.trade_id or ack.command_id != command.command_id:
            raise AdapterContractError("adapter returned ACK for a different command")
        return ack

    @abstractmethod
    def _submit(self, command: ExecutionCommand) -> ExecutionAck: ...

    @abstractmethod
    def cancel(self, command: ExecutionCommand, now: datetime) -> ExecutionAck: ...

    @abstractmethod
    def fetch_positions(self, profile_id: str) -> list[BrokerPosition]: ...
