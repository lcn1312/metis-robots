"""Batch upsert helpers."""
from __future__ import annotations

import json
import time
from typing import Iterable

import psycopg
from psycopg import Connection
from psycopg.types.json import Jsonb

# Transient connection errors worth retrying with a reconnect.
_TRANSIENT_ERRORS = (
    psycopg.OperationalError,
    psycopg.InterfaceError,
)


def _upsert(
    conn: Connection,
    table: str,
    pk: list[str],
    cols: list[str],
    rows: Iterable[dict],
    batch_size: int = 500,
) -> int:
    placeholders = ", ".join(["%s"] * len(cols))
    col_list = ", ".join(cols)
    pk_list = ", ".join(pk)
    non_pk = [c for c in cols if c not in pk]
    set_clause = ", ".join(
        f"{c}=EXCLUDED.{c}" for c in non_pk
    ) or f"{pk[0]}=EXCLUDED.{pk[0]}"
    # Só reescreve a linha se algum valor não-pk mudou de fato: evita churn de
    # WAL/TOAST/dead-tuples reescrevendo linhas idênticas a cada carga (o ingest
    # é full-reload). A relação não-qualificada referencia a linha existente no
    # ON CONFLICT DO UPDATE.
    rel = table.split(".")[-1]
    change_guard = (
        " WHERE " + " OR ".join(f"{rel}.{c} IS DISTINCT FROM EXCLUDED.{c}" for c in non_pk)
        if non_pk
        else ""
    )
    if "updated_at" in _table_cols(conn, table):
        sql = (
            f"INSERT INTO {table} ({col_list}) VALUES ({placeholders}) "
            f"ON CONFLICT ({pk_list}) DO UPDATE SET {set_clause}, updated_at=now()"
            f"{change_guard}"
        )
    else:
        sql = (
            f"INSERT INTO {table} ({col_list}) VALUES ({placeholders}) "
            f"ON CONFLICT ({pk_list}) DO UPDATE SET {set_clause}"
            f"{change_guard}"
        )

    total = 0
    batch: list[tuple] = []
    with conn.cursor() as cur:
        for r in rows:
            if any(r.get(p) is None for p in pk):
                continue
            batch.append(tuple(_encode(r.get(c)) for c in cols))
            if len(batch) >= batch_size:
                cur.executemany(sql, batch)
                total += len(batch)
                batch.clear()
        if batch:
            cur.executemany(sql, batch)
            total += len(batch)
    conn.commit()
    return total


def _encode(v):
    if isinstance(v, (dict, list)):
        return Jsonb(v)
    return v


_TABLE_COLS_CACHE: dict[str, set[str]] = {}


def _table_cols(conn: Connection, table: str) -> set[str]:
    if table in _TABLE_COLS_CACHE:
        return _TABLE_COLS_CACHE[table]
    schema, name = ("salic", table) if "." not in table else table.split(".", 1)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema=%s AND table_name=%s",
            (schema, name),
        )
        cols = {r[0] for r in cur.fetchall()}
    _TABLE_COLS_CACHE[table] = cols
    return cols


def upsert(conn: Connection, table: str, pk: list[str], rows: Iterable[dict]) -> int:
    rows = list(rows)
    if not rows:
        return 0
    cols = list(rows[0].keys())
    return _upsert(conn, table, pk, cols, rows)


def safe_upsert(
    conn_ref: list,
    table: str,
    pk: list[str],
    rows: Iterable[dict],
    max_retries: int = 3,
) -> int:
    """Upsert with reconnect-on-disconnect.

    `conn_ref` is a 1-element list holding the current Connection so callers
    can swap it out after a reconnect. Returns total upserted rows.
    """
    from .db import reconnect

    rows = list(rows)
    if not rows:
        return 0
    last_err: Exception | None = None
    for attempt in range(max_retries):
        try:
            return upsert(conn_ref[0], table, pk, rows)
        except _TRANSIENT_ERRORS as e:
            last_err = e
            # Clear cached cols (server identity may differ) and reconnect.
            _TABLE_COLS_CACHE.clear()
            backoff = min(2 ** attempt, 10)
            print(
                f"  [upsert {table}] conn lost ({type(e).__name__}): "
                f"reconnect attempt {attempt + 1}/{max_retries} after {backoff}s",
                flush=True,
            )
            time.sleep(backoff)
            conn_ref[0] = reconnect(conn_ref[0])
    assert last_err is not None
    raise last_err
