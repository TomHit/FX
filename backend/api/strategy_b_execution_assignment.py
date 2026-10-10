from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping


_ALLOWED_SIDES = {"BUY", "SELL"}


@dataclass(frozen=True)
class StrategyBExecutionAssignment:
    uid: str
    profile_id: str
    device_id: str

    account_login: str
    account_server: str
    broker_company: str

    thesis_id: str
    selected_capture_id: str
    symbol: str
    side: str

    valid: bool
    decision: str
    reason: str

    observation_only: bool = True
    execution_authority: bool = False
    risk_reservation_authority: bool = False
    order_authority: bool = False
    broker_authority: bool = False
    strategy_a_mutation: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _text(value: Any) -> str:
    return str(value or "").strip()


def _profile(value: Any) -> str:
    return _text(value).lower()


def _identity(value: Any) -> str:
    return _text(value).lower()


def _invalid(
    *,
    uid: str = "",
    profile_id: str = "",
    device_id: str = "",
    account_login: str = "",
    account_server: str = "",
    broker_company: str = "",
    thesis_id: str = "",
    selected_capture_id: str = "",
    symbol: str = "",
    side: str = "",
    reason: str,
) -> StrategyBExecutionAssignment:
    return StrategyBExecutionAssignment(
        uid=uid,
        profile_id=profile_id,
        device_id=device_id,
        account_login=account_login,
        account_server=account_server,
        broker_company=broker_company,
        thesis_id=thesis_id,
        selected_capture_id=selected_capture_id,
        symbol=symbol,
        side=side,
        valid=False,
        decision="WAIT",
        reason=reason,
    )


def build_strategy_b_execution_assignment(
    *,
    uid: str,
    thesis_id: str,
    selected_capture_id: str,
    symbol: str,
    side: str,
    prop_config: Mapping[str, Any] | None,
    resolved_execution_context: Mapping[str, Any] | None,
) -> StrategyBExecutionAssignment:
    """
    Pure Strategy-B execution-assignment validator.

    This function does not:
      - read or write Redis,
      - resolve active profiles,
      - resolve MT5 devices/accounts,
      - calculate risk,
      - reserve risk,
      - create orders,
      - contact MT5/broker,
      - mutate Strategy A.

    The caller must provide the production-resolved prop configuration and
    execution context. This function only proves that the supplied Strategy-B
    thesis/capture can be bound consistently to that already-resolved identity.
    """

    uid_u = _text(uid)
    thesis_u = _text(thesis_id)
    capture_u = _text(selected_capture_id)
    symbol_u = _text(symbol).upper()
    side_u = _text(side).upper()

    if not uid_u:
        return _invalid(
            thesis_id=thesis_u,
            selected_capture_id=capture_u,
            symbol=symbol_u,
            side=side_u,
            reason="UID_MISSING",
        )

    if not thesis_u:
        return _invalid(
            uid=uid_u,
            selected_capture_id=capture_u,
            symbol=symbol_u,
            side=side_u,
            reason="THESIS_ID_MISSING",
        )

    if not capture_u:
        return _invalid(
            uid=uid_u,
            thesis_id=thesis_u,
            symbol=symbol_u,
            side=side_u,
            reason="SELECTED_CAPTURE_ID_MISSING",
        )

    if not symbol_u:
        return _invalid(
            uid=uid_u,
            thesis_id=thesis_u,
            selected_capture_id=capture_u,
            side=side_u,
            reason="SYMBOL_MISSING",
        )

    if side_u not in _ALLOWED_SIDES:
        return _invalid(
            uid=uid_u,
            thesis_id=thesis_u,
            selected_capture_id=capture_u,
            symbol=symbol_u,
            side=side_u,
            reason="SIDE_INVALID",
        )

    if not isinstance(prop_config, Mapping):
        return _invalid(
            uid=uid_u,
            thesis_id=thesis_u,
            selected_capture_id=capture_u,
            symbol=symbol_u,
            side=side_u,
            reason="PROP_CONFIG_MISSING",
        )

    profile_id = _profile(prop_config.get("profile_id"))

    if not profile_id:
        return _invalid(
            uid=uid_u,
            thesis_id=thesis_u,
            selected_capture_id=capture_u,
            symbol=symbol_u,
            side=side_u,
            reason="PROFILE_ID_MISSING",
        )

    if prop_config.get("enabled") is not True:
        return _invalid(
            uid=uid_u,
            profile_id=profile_id,
            thesis_id=thesis_u,
            selected_capture_id=capture_u,
            symbol=symbol_u,
            side=side_u,
            reason="PROP_PROFILE_DISABLED",
        )

    cfg_login = _text(prop_config.get("account_login"))
    cfg_server = _identity(prop_config.get("account_server"))
    cfg_company = _identity(prop_config.get("broker_company"))

    if not cfg_login or not cfg_server or not cfg_company:
        return _invalid(
            uid=uid_u,
            profile_id=profile_id,
            account_login=cfg_login,
            account_server=cfg_server,
            broker_company=cfg_company,
            thesis_id=thesis_u,
            selected_capture_id=capture_u,
            symbol=symbol_u,
            side=side_u,
            reason="PROP_ACCOUNT_IDENTITY_MISSING",
        )

    if not isinstance(resolved_execution_context, Mapping):
        return _invalid(
            uid=uid_u,
            profile_id=profile_id,
            account_login=cfg_login,
            account_server=cfg_server,
            broker_company=cfg_company,
            thesis_id=thesis_u,
            selected_capture_id=capture_u,
            symbol=symbol_u,
            side=side_u,
            reason="EXECUTION_CONTEXT_MISSING",
        )

    resolved_ok = resolved_execution_context.get("ok") is True
    resolved_profile = _profile(
        resolved_execution_context.get("profile_id")
    )
    device_id = _text(
        resolved_execution_context.get("device_id")
    )

    if not resolved_ok:
        return _invalid(
            uid=uid_u,
            profile_id=profile_id,
            device_id=device_id,
            account_login=cfg_login,
            account_server=cfg_server,
            broker_company=cfg_company,
            thesis_id=thesis_u,
            selected_capture_id=capture_u,
            symbol=symbol_u,
            side=side_u,
            reason="EXECUTION_CONTEXT_NOT_RESOLVED",
        )

    if not resolved_profile:
        return _invalid(
            uid=uid_u,
            profile_id=profile_id,
            device_id=device_id,
            account_login=cfg_login,
            account_server=cfg_server,
            broker_company=cfg_company,
            thesis_id=thesis_u,
            selected_capture_id=capture_u,
            symbol=symbol_u,
            side=side_u,
            reason="RESOLVED_PROFILE_ID_MISSING",
        )

    if resolved_profile != profile_id:
        return _invalid(
            uid=uid_u,
            profile_id=profile_id,
            device_id=device_id,
            account_login=cfg_login,
            account_server=cfg_server,
            broker_company=cfg_company,
            thesis_id=thesis_u,
            selected_capture_id=capture_u,
            symbol=symbol_u,
            side=side_u,
            reason="PROFILE_IDENTITY_MISMATCH",
        )

    if not device_id:
        return _invalid(
            uid=uid_u,
            profile_id=profile_id,
            account_login=cfg_login,
            account_server=cfg_server,
            broker_company=cfg_company,
            thesis_id=thesis_u,
            selected_capture_id=capture_u,
            symbol=symbol_u,
            side=side_u,
            reason="DEVICE_ID_MISSING",
        )

    account = resolved_execution_context.get("account")

    if not isinstance(account, Mapping):
        return _invalid(
            uid=uid_u,
            profile_id=profile_id,
            device_id=device_id,
            account_login=cfg_login,
            account_server=cfg_server,
            broker_company=cfg_company,
            thesis_id=thesis_u,
            selected_capture_id=capture_u,
            symbol=symbol_u,
            side=side_u,
            reason="RESOLVED_ACCOUNT_MISSING",
        )

    account_login = _text(
        account.get("login")
        or account.get("account_login")
    )
    account_server = _identity(
        account.get("server")
        or account.get("account_server")
    )
    broker_company = _identity(
        account.get("company")
        or account.get("broker_company")
    )

    if (
        not account_login
        or not account_server
        or not broker_company
    ):
        return _invalid(
            uid=uid_u,
            profile_id=profile_id,
            device_id=device_id,
            account_login=account_login,
            account_server=account_server,
            broker_company=broker_company,
            thesis_id=thesis_u,
            selected_capture_id=capture_u,
            symbol=symbol_u,
            side=side_u,
            reason="RESOLVED_ACCOUNT_IDENTITY_MISSING",
        )

    if account_login != cfg_login:
        return _invalid(
            uid=uid_u,
            profile_id=profile_id,
            device_id=device_id,
            account_login=account_login,
            account_server=account_server,
            broker_company=broker_company,
            thesis_id=thesis_u,
            selected_capture_id=capture_u,
            symbol=symbol_u,
            side=side_u,
            reason="ACCOUNT_LOGIN_MISMATCH",
        )

    if account_server != cfg_server:
        return _invalid(
            uid=uid_u,
            profile_id=profile_id,
            device_id=device_id,
            account_login=account_login,
            account_server=account_server,
            broker_company=broker_company,
            thesis_id=thesis_u,
            selected_capture_id=capture_u,
            symbol=symbol_u,
            side=side_u,
            reason="ACCOUNT_SERVER_MISMATCH",
        )

    if broker_company != cfg_company:
        return _invalid(
            uid=uid_u,
            profile_id=profile_id,
            device_id=device_id,
            account_login=account_login,
            account_server=account_server,
            broker_company=broker_company,
            thesis_id=thesis_u,
            selected_capture_id=capture_u,
            symbol=symbol_u,
            side=side_u,
            reason="BROKER_COMPANY_MISMATCH",
        )

    return StrategyBExecutionAssignment(
        uid=uid_u,
        profile_id=profile_id,
        device_id=device_id,
        account_login=account_login,
        account_server=account_server,
        broker_company=broker_company,
        thesis_id=thesis_u,
        selected_capture_id=capture_u,
        symbol=symbol_u,
        side=side_u,
        valid=True,
        decision="ASSIGNED",
        reason="EXECUTION_IDENTITY_PROVEN",
    )
