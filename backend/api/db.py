# /opt/xauapi/api/db.py
import os
from contextlib import contextmanager
import psycopg2
import psycopg2.pool
import psycopg2.extras

_POOL = None

def _dsn_from_env() -> str:
    # Prefer a single DATABASE_URL, else build DSN from PG* vars
    dsn = os.getenv("DATABASE_URL")
    if dsn:
        return dsn
    host = os.getenv("PGHOST", "127.0.0.1")
    port = os.getenv("PGPORT", "5432")
    db   = os.getenv("PGDATABASE", os.getenv("POSTGRES_DB", "postgres"))
    user = os.getenv("PGUSER", os.getenv("POSTGRES_USER", "postgres"))
    pwd  = os.getenv("PGPASSWORD", os.getenv("POSTGRES_PASSWORD", ""))
    return f"postgresql://{user}:{pwd}@{host}:{port}/{db}"

def _pool() -> psycopg2.pool.SimpleConnectionPool:
    global _POOL
    if _POOL is None:
        _POOL = psycopg2.pool.SimpleConnectionPool(
            minconn=1, maxconn=int(os.getenv("DB_MAX_CONN", "10")), dsn=_dsn_from_env()
        )
    return _POOL

@contextmanager
def db():
    """
    Usage:
        with db() as conn, conn.cursor() as cur:
            cur.execute("SELECT 1")
    - Commits on success, rollbacks on error.
    - cursor() defaults to DictCursor so row["col"] works.
    """
    pool = _pool()
    conn = pool.getconn()
    # Make cursor() default to DictCursor
    orig_cursor = conn.cursor
    def dict_cursor(*args, **kwargs):
        if "cursor_factory" not in kwargs:
            kwargs["cursor_factory"] = psycopg2.extras.DictCursor
        return orig_cursor(*args, **kwargs)
    conn.cursor = dict_cursor  # type: ignore[attr-defined]
    try:
        yield conn
        conn.commit()
    except Exception:
        try: conn.rollback()
        except Exception: pass
        raise
    finally:
        try: conn.cursor = orig_cursor  # restore
        except Exception: pass
        pool.putconn(conn)
