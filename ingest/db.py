"""DB connection helpers using psycopg3."""
from __future__ import annotations

import os
from contextlib import contextmanager

import psycopg

# TCP keepalive params — Neon/pgbouncer drop idle SSL conns after ~60-120s.
# Probe every 30s so the conn stays warm during slow upstream calls (SALIC API).
_KEEPALIVE_PARAMS = (
    "keepalives=1"
    "&keepalives_idle=30"
    "&keepalives_interval=10"
    "&keepalives_count=5"
)


def _with_keepalives(url: str) -> str:
    if "keepalives=" in url:
        return url
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}{_KEEPALIVE_PARAMS}"


def dsn() -> str:
    url = os.getenv("DATABASE_URL")
    if url:
        return _with_keepalives(url)
    user = os.environ["POSTGRES_USER"]
    pwd = os.environ["POSTGRES_PASSWORD"]
    db = os.environ["POSTGRES_DB"]
    host = os.getenv("POSTGRES_HOST", "127.0.0.1")
    port = os.getenv("POSTGRES_PORT", "5544")
    return (
        f"host={host} port={port} user={user} password={pwd} dbname={db} "
        f"keepalives=1 keepalives_idle=30 keepalives_interval=10 keepalives_count=5"
    )


def _open() -> psycopg.Connection:
    conn = psycopg.connect(dsn(), autocommit=False)
    with conn.cursor() as cur:
        cur.execute("SET search_path TO salic, public;")
        # Neon default idle_in_transaction_session_timeout = 5min kills slow API fetches.
        cur.execute("SET idle_in_transaction_session_timeout = '30min';")
    conn.commit()
    return conn


@contextmanager
def connect():
    conn = _open()
    try:
        yield conn
    finally:
        try:
            conn.close()
        except Exception:
            pass


def reconnect(old: psycopg.Connection | None) -> psycopg.Connection:
    """Close stale conn (best-effort) and open a fresh one."""
    if old is not None:
        try:
            old.close()
        except Exception:
            pass
    return _open()
