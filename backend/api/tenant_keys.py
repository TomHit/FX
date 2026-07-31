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

def zone_watch_pattern(
    uid,
    tf="H1",
) -> str:
    uid = require_uid(uid)

    return (
        f"xtl:zone:watch:{uid}:"
        f"*:*:{str(tf or 'H1').upper().strip()}"
    )

def zone_watch_index_key(
    uid,
    tf="H1",
) -> str:
    uid = require_uid(uid)

    return (
        f"xtl:zone:watch:index:"
        f"{uid}:"
        f"{str(tf or 'H1').upper().strip()}"
    )


def zone_watch_index_add(
    redis_client,
    uid,
    watch_key: str,
    tf="H1",
) -> None:
    redis_client.sadd(
        zone_watch_index_key(
            uid,
            tf,
        ),
        str(watch_key),
    )


def zone_watch_index_remove(
    redis_client,
    uid,
    watch_key: str,
    tf="H1",
) -> None:
    redis_client.srem(
        zone_watch_index_key(
            uid,
            tf,
        ),
        str(watch_key),
    )


def zone_watch_set(
    redis_client,
    uid,
    symbol,
    side,
    value,
    tf="H1",
    ex=None,
) -> str:
    key = zone_watch_key(
        uid,
        symbol,
        side,
        tf,
    )

    pipe = redis_client.pipeline(
        transaction=True
    )

    if ex is None:
        pipe.set(
            key,
            value,
        )
    else:
        pipe.set(
            key,
            value,
            ex=int(ex),
        )

    pipe.sadd(
        zone_watch_index_key(
            uid,
            tf,
        ),
        key,
    )

    pipe.execute()

    return key


def zone_watch_delete(
    redis_client,
    uid,
    symbol,
    side,
    tf="H1",
) -> int:
    key = zone_watch_key(
        uid,
        symbol,
        side,
        tf,
    )

    pipe = redis_client.pipeline(
        transaction=True
    )

    pipe.delete(
        key
    )

    pipe.srem(
        zone_watch_index_key(
            uid,
            tf,
        ),
        key,
    )

    result = pipe.execute()

    try:
        return int(
            result[0] or 0
        )
    except Exception:
        return 0
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

def entry_claim_latest_key(
    uid,
    symbol,
    side,
    tf="H1",
) -> str:
    uid = require_uid(uid)

    return (
        f"xtl:watch:entry_claim:latest:"
        f"{uid}:"
        f"{str(symbol).upper().strip()}:"
        f"{str(side).upper().strip()}:"
        f"{str(tf or 'H1').upper().strip()}"
    )


def entry_claim_acquire(
    redis_client,
    uid,
    symbol,
    side,
    rev_ms,
    value,
    tf="H1",
    ex=None,
) -> bool:
    """
    Atomically:
      1. create the versioned entry claim with NX
      2. update the deterministic latest pointer

    The pointer is updated only when this caller successfully
    creates the claim.
    """
    uid = require_uid(uid)
    sym_u = str(symbol or "").upper().strip()
    side_u = str(side or "").upper().strip()
    tf_u = str(tf or "H1").upper().strip()
    rev_i = int(rev_ms)

    if not sym_u:
        raise ValueError("SYMBOL_REQUIRED_FOR_ENTRY_CLAIM")

    if side_u not in ("BUY", "SELL"):
        raise ValueError("INVALID_ENTRY_CLAIM_SIDE")

    claim_key = entry_claim_key(
        uid,
        sym_u,
        side_u,
        rev_i,
        tf_u,
    )

    latest_key = entry_claim_latest_key(
        uid,
        sym_u,
        side_u,
        tf_u,
    )

    ttl_seconds = int(ex or 0)

    lua = """
    local claim_key = KEYS[1]
    local latest_key = KEYS[2]

    local claim_value = ARGV[1]
    local rev_ms = ARGV[2]
    local ttl_seconds = tonumber(ARGV[3])

    local created

    if ttl_seconds and ttl_seconds > 0 then
        created = redis.call(
            "SET",
            claim_key,
            claim_value,
            "NX",
            "EX",
            ttl_seconds
        )
    else
        created = redis.call(
            "SET",
            claim_key,
            claim_value,
            "NX"
        )
    end

    if not created then
        return 0
    end

    if ttl_seconds and ttl_seconds > 0 then
        redis.call(
            "SET",
            latest_key,
            rev_ms,
            "EX",
            ttl_seconds
        )
    else
        redis.call(
            "SET",
            latest_key,
            rev_ms
        )
    end

    return 1
    """

    result = redis_client.eval(
        lua,
        2,
        claim_key,
        latest_key,
        str(value),
        str(rev_i),
        str(ttl_seconds),
    )

    return bool(int(result or 0))


def delete_latest_entry_claim(
    redis_client,
    uid,
    symbol,
    side,
    tf="H1",
) -> int:
    """
    Delete the exact claim referenced by the latest pointer,
    then delete that pointer atomically.

    Returns:
      1 when a pointer existed and cleanup ran
      0 when no pointer existed
    """
    uid = require_uid(uid)
    sym_u = str(symbol or "").upper().strip()
    side_u = str(side or "").upper().strip()
    tf_u = str(tf or "H1").upper().strip()

    if not sym_u:
        raise ValueError("SYMBOL_REQUIRED_FOR_ENTRY_CLAIM_DELETE")

    if side_u not in ("BUY", "SELL"):
        raise ValueError("INVALID_ENTRY_CLAIM_SIDE")

    latest_key = entry_claim_latest_key(
        uid,
        sym_u,
        side_u,
        tf_u,
    )

    claim_prefix = (
        f"xtl:watch:entry_claim:"
        f"{uid}:{sym_u}:{side_u}:{tf_u}:"
    )

    lua = """
    local latest_key = KEYS[1]
    local claim_prefix = ARGV[1]

    local rev_ms = redis.call(
        "GET",
        latest_key
    )

    if not rev_ms then
        return 0
    end

    local claim_key = claim_prefix .. rev_ms

    redis.call(
        "DEL",
        claim_key
    )

    redis.call(
        "DEL",
        latest_key
    )

    return 1
    """

    result = redis_client.eval(
        lua,
        1,
        latest_key,
        claim_prefix,
    )

    return int(result or 0)


# ============================================================
# Prop-firm tenant keys
#
# Every prop profile and its runtime state belongs to one UID.
# Never fall back to global prop keys from these helpers.
# ============================================================

def prop_profile_key(
    uid,
    profile_id,
) -> str:
    uid_u = require_uid(uid)

    pid = str(
        profile_id or ""
    ).strip().lower()

    if not pid:
        raise ValueError("PROFILE_ID_REQUIRED")

    return (
        f"xtl:prop:{uid_u}:"
        f"profile:{pid}"
    )


def prop_profiles_set_key(
    uid,
) -> str:
    uid_u = require_uid(uid)

    return f"xtl:prop:{uid_u}:profiles"


def prop_active_profile_key(
    uid,
) -> str:
    uid_u = require_uid(uid)

    return f"xtl:prop:{uid_u}:profile:active"


def prop_stats_key(
    uid,
    profile_id,
) -> str:
    uid_u = require_uid(uid)

    pid = str(
        profile_id or ""
    ).strip().lower()

    if not pid:
        raise ValueError("PROFILE_ID_REQUIRED")

    return (
        f"xtl:prop:{uid_u}:"
        f"stats:{pid}"
    )


def prop_daily_key(
    uid,
    profile_id,
    day,
) -> str:
    uid_u = require_uid(uid)

    pid = str(
        profile_id or ""
    ).strip().lower()

    day_u = str(
        day or ""
    ).strip()

    if not pid:
        raise ValueError("PROFILE_ID_REQUIRED")

    if not day_u:
        raise ValueError("PROP_DAY_REQUIRED")

    return (
        f"xtl:prop:{uid_u}:"
        f"daily:{pid}:{day_u}"
    )


def prop_open_risk_key(
    uid,
    profile_id,
) -> str:
    uid_u = require_uid(uid)

    pid = str(
        profile_id or ""
    ).strip().lower()

    if not pid:
        raise ValueError("PROFILE_ID_REQUIRED")

    return (
        f"xtl:prop:{uid_u}:"
        f"open_risk:{pid}"
    )