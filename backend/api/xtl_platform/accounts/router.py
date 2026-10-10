"""Account Router / fan-out (PRD sections 5 and 9).

Binds a thesis to account identities and devices. It makes no policy or risk
decisions; every child it creates still has to pass both.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Mapping, Optional

from ..domain.account import Device, ExecutionSet
from ..domain.enums import FanOutCode as F
from ..domain.thesis import ChildExecutionIntent, Thesis


@dataclass(frozen=True)
class FanOutResult:
    profile_id: str
    code: F
    reason: str
    intent: Optional[ChildExecutionIntent] = None

    @property
    def created(self) -> bool:
        return self.code is F.CHILD_CREATED


class AccountRouter:
    def __init__(self, devices: Mapping[str, Device], max_heartbeat_age: timedelta) -> None:
        self._devices = dict(devices)
        self._max_heartbeat_age = max_heartbeat_age

    def fan_out(self, thesis: Thesis, execution_set: ExecutionSet, now: datetime) -> tuple[FanOutResult, ...]:
        results: list[FanOutResult] = []
        # Deterministic order: sibling-dependent policy checks depend on it.
        for profile in sorted(execution_set.profiles, key=lambda p: p.profile_id):
            pid = profile.profile_id

            def r(code: F, reason: str) -> FanOutResult:
                return FanOutResult(pid, code, reason)

            if not profile.enabled:
                results.append(r(F.SKIP_PROFILE_DISABLED, "profile disabled"))
                continue
            if now >= thesis.expires_at:
                results.append(r(F.BLOCK_THESIS_EXPIRED, "thesis expired"))
                continue
            if profile.venue is not thesis.venue:
                results.append(r(F.BLOCK_VENUE_MISMATCH,
                                 f"thesis levels are {thesis.venue}, profile is {profile.venue}"))
                continue
            venue_symbol = profile.symbol_map.get(thesis.symbol)
            if not venue_symbol:
                results.append(r(F.BLOCK_SYMBOL_UNMAPPED, f"no mapping for {thesis.symbol}"))
                continue

            device = self._devices.get(profile.device_id)
            if device is None:
                results.append(r(F.BLOCK_DEVICE_NOT_FOUND, f"device {profile.device_id} not registered"))
                continue
            if device.owner_user_id != profile.owner_user_id:
                results.append(r(F.BLOCK_DEVICE_OWNER_MISMATCH, "device not owned by profile owner"))
                continue
            if not device.healthy:
                results.append(r(F.BLOCK_DEVICE_UNHEALTHY, "device reports unhealthy"))
                continue
            if now - device.last_heartbeat > self._max_heartbeat_age:
                results.append(r(F.BLOCK_DEVICE_STALE, "device heartbeat stale"))
                continue
            if profile.venue not in device.venues:
                results.append(r(F.BLOCK_DEVICE_VENUE_UNSUPPORTED, f"device cannot execute {profile.venue}"))
                continue

            intent = ChildExecutionIntent.from_thesis(thesis, profile, venue_symbol)
            results.append(FanOutResult(pid, F.CHILD_CREATED, "child intent created", intent))
        return tuple(results)
