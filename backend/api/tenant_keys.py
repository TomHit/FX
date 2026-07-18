# /opt/xauapi/api/tenant_keys.py

def require_uid(uid) -> str:
    value = str(uid or "").strip()

    if not value:
        raise ValueError(
            "UID_REQUIRED_FOR_TENANT_TRADING_STATE"
        )

    return value


def zone_watch_key(
    uid,
    symbol,
    side,
    tf="H1",
) -> str:
    uid = require_uid(uid)

    return (
        f"xtl:zone:watch:{uid}:"
        f"{str(symbol).upper().strip()}:"
        f"{str(side).upper().strip()}:"
        f"{str(tf).upper().strip()}"
    )


def break_state_key(
    uid,
    symbol,
    side,
    tf="H1",
) -> str:
    uid = require_uid(uid)

    return (
        f"xtl:watch:break_state:{uid}:"
        f"{str(symbol).upper().strip()}:"
        f"{str(side).upper().strip()}:"
        f"{str(tf).upper().strip()}"
    )


def entry_claim_key(
    uid,
    symbol,
    side,
    rev_ms,
    tf="H1",
) -> str:
    uid = require_uid(uid)

    return (
        f"xtl:watch:entry_claim:{uid}:"
        f"{str(symbol).upper().strip()}:"
        f"{str(side).upper().strip()}:"
        f"{str(tf).upper().strip()}:"
        f"{int(rev_ms)}"
    )


def entry_claim_pattern(
    uid,
    symbol,
    side,
    tf="H1",
) -> str:
    uid = require_uid(uid)

    return (
        f"xtl:watch:entry_claim:{uid}:"
        f"{str(symbol).upper().strip()}:"
        f"{str(side).upper().strip()}:"
        f"{str(tf).upper().strip()}:*"
    )