"""Full reload ingest pipeline. Usage: python -m ingest.run [resource ...]"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timezone

from dotenv import load_dotenv

load_dotenv()

MAX_RECORDS = int(os.getenv("SALIC_MAX_RECORDS", "0"))  # 0 = ilimitado

import psycopg

from .db import connect, reconnect
from .salic_client import SalicClient
from .transform import (
    fornecedor_row,
    incentivador_row,
    projeto_locais,
    projeto_row,
    proponente_row,
    proposta_row,
)
from .upsert import safe_upsert, upsert

RESOURCES = {
    "proponentes": ("proponentes", proponente_row, ["cgccpf"]),
    "incentivadores": ("incentivadores", incentivador_row, ["cgccpf"]),
    "fornecedores": ("fornecedores", fornecedor_row, ["cgccpf"]),
    "propostas": ("propostas", proposta_row, ["id"]),
    "projetos": ("projetos", projeto_row, ["pronac"]),
}


def log_run(conn_ref, endpoint, started, rows_seen, rows_up, status, err=None):
    """Insert ingest_run row. Auto-reconnect if conn died mid-resource."""
    payload = (
        endpoint,
        started,
        datetime.now(timezone.utc),
        rows_seen,
        rows_up,
        status,
        err,
    )
    for attempt in range(3):
        try:
            with conn_ref[0].cursor() as cur:
                cur.execute(
                    "INSERT INTO ingest_run (endpoint, started_at, finished_at, "
                    "rows_seen, rows_upserted, status, error) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s)",
                    payload,
                )
            conn_ref[0].commit()
            return
        except (psycopg.OperationalError, psycopg.InterfaceError) as e:
            print(f"  [log_run] conn lost ({e}); reconnecting", flush=True)
            conn_ref[0] = reconnect(conn_ref[0])
    print(f"  [log_run] gave up after retries: status={status}", flush=True)


def ingest_resource(client: SalicClient, conn_ref: list, resource: str) -> None:
    table, mapper, pk = RESOURCES[resource]
    started = datetime.now(timezone.utc)
    seen = 0
    upserted = 0
    # Resume support: env START_OFFSET_<RESOURCE> ou START_OFFSET (global)
    start_offset = int(os.getenv(f"START_OFFSET_{resource.upper()}", os.getenv("START_OFFSET", "0")))
    if start_offset > 0:
        print(f"[{resource}] resume from offset={start_offset}", flush=True)
    try:
        batch: list[dict] = []
        locais_batch: list[tuple[str, list[dict]]] = []
        for raw in client.paginate(resource, embed_key=resource, start_offset=start_offset):
            seen += 1
            if MAX_RECORDS and seen > MAX_RECORDS:
                seen -= 1
                break
            row = mapper(raw)
            batch.append(row)
            if resource == "projetos":
                pron = row.get("pronac")
                if pron:
                    locs = projeto_locais(raw)
                    if locs:
                        locais_batch.append((pron, locs))
            if len(batch) >= 1000:
                upserted += safe_upsert(conn_ref, table, pk, batch)
                batch.clear()
                if locais_batch:
                    _flush_locais(conn_ref, locais_batch)
                    locais_batch.clear()
                print(f"  [{resource}] {seen} processed", flush=True)
        if batch:
            upserted += safe_upsert(conn_ref, table, pk, batch)
        if locais_batch:
            _flush_locais(conn_ref, locais_batch)
        log_run(conn_ref, resource, started, seen, upserted, "ok")
        print(f"[{resource}] DONE seen={seen} upserted={upserted}")
    except Exception as e:
        log_run(conn_ref, resource, started, seen, upserted, "error", str(e)[:1000])
        print(f"[{resource}] ERROR {e}", file=sys.stderr)
        raise


def _flush_locais(conn_ref: list, batch: list[tuple[str, list[dict]]]) -> None:
    rows = []
    for pron, locs in batch:
        for loc in locs:
            rows.append({"pronac": pron, **loc})
    if rows:
        safe_upsert(
            conn_ref,
            "projeto_local",
            ["pronac", "municipio_ibge", "municipio", "uf"],
            rows,
        )


def main() -> None:
    targets = sys.argv[1:] or list(RESOURCES.keys())
    unknown = [t for t in targets if t not in RESOURCES]
    if unknown:
        print(f"Unknown resources: {unknown}", file=sys.stderr)
        sys.exit(2)

    with SalicClient() as client, connect() as conn:
        conn_ref = [conn]
        for resource in targets:
            ingest_resource(client, conn_ref, resource)


if __name__ == "__main__":
    main()
