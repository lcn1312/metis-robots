"""Ingest projeto_fornecedor — vínculo fornecedor↔projeto.

API SALIC: GET /fornecedores?PRONAC=NNNN paginado.
Iteramos projetos com valor_captado > 0 (sem captação raramente há fornecedor).

Idempotente via ON CONFLICT (pronac, cgccpf) DO NOTHING.

Uso:
    cd "/Users/lucanpiai/Desktop/11COWORK/DB EDITAIS"
    . .venv/bin/activate
    python -m ingest.projeto_fornecedor          # full
    SALIC_MAX_RECORDS=100 python -m ingest.projeto_fornecedor  # smoke
"""

from __future__ import annotations

import logging
import os
import sys

from dotenv import load_dotenv

load_dotenv()

import psycopg
from psycopg.types.json import Jsonb

from .db import _with_keepalives
from .salic_client import SalicClient

DB_URL = os.environ.get(
    "DATABASE_URL", "postgres://salic:change_me_local_only@127.0.0.1:5544/salic"
)
PG_PASSWORD = os.environ.get("POSTGRES_PASSWORD")
if PG_PASSWORD and "change_me_local_only" in DB_URL:
    DB_URL = DB_URL.replace("change_me_local_only", PG_PASSWORD)
# Inject TCP keepalives so Neon doesn't drop idle SSL conns mid-pagination.
DB_URL = _with_keepalives(DB_URL)

MAX_RECORDS = int(os.getenv("SALIC_MAX_RECORDS", "0"))
PAGE_SIZE = int(os.getenv("SALIC_PAGE_SIZE", "100"))

logging.basicConfig(level=logging.INFO, format="[projeto_forn] %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
log = logging.getLogger(__name__)


def fetch_fornecedores_for_pronac(
    client: SalicClient, pronac: str
) -> tuple[list[dict], int]:
    """Retorna (fornecedores, status_final). status=404 = pronac sem fornecedores na API."""
    out: list[dict] = []
    offset = 0
    while True:
        try:
            data = client._get(
                "/fornecedores",
                params={"PRONAC": pronac, "limit": PAGE_SIZE, "offset": offset},
            )
        except Exception as e:
            msg = str(e).lower()
            if "404" in msg or "no supplier" in msg or "supplier" in msg:
                return out, 404
            raise
        items = data.get("_embedded", {}).get("fornecedores", []) or []
        if not items:
            return out, 200
        out.extend(items)
        if len(items) < PAGE_SIZE:
            return out, 200
        offset += len(items)
        if offset > 5000:
            log.warning(f"PRONAC {pronac}: >5000 fornecedores, parando paginação")
            return out, 200


def cache_status(conn, pronac: str, status: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO metis.salic_fetch_cache (kind, key, status, tried_at, retries)
            VALUES ('proj_fornecedor', %s, %s, now(), 1)
            ON CONFLICT (kind, key) DO UPDATE
              SET status = EXCLUDED.status,
                  tried_at = now(),
                  retries = metis.salic_fetch_cache.retries + 1
            """,
            (pronac, status),
        )
        conn.commit()


def upsert_batch(conn, rows: list[tuple]) -> int:
    if not rows:
        return 0
    sql = """
        INSERT INTO salic.projeto_fornecedor (pronac, cgccpf, raw)
        VALUES (%s, %s, %s)
        ON CONFLICT (pronac, cgccpf) DO NOTHING
    """
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
        n = cur.rowcount
    conn.commit()
    return n


def fetch_pronacs_to_process(conn) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT p.pronac FROM salic.projetos p
            WHERE p.valor_captado > 0
              AND NOT EXISTS (
                SELECT 1 FROM salic.projeto_fornecedor pf WHERE pf.pronac = p.pronac
              )
              AND NOT EXISTS (
                SELECT 1 FROM metis.salic_fetch_cache c
                WHERE c.kind = 'proj_fornecedor' AND c.key = p.pronac
                  AND c.status IN (404, 410)
              )
            ORDER BY p.valor_captado DESC
            """
        )
        return [r[0] for r in cur.fetchall()]


def main() -> None:
    log.info(f"DB: {DB_URL.split('@')[-1]}")
    conn = psycopg.connect(DB_URL)
    try:
        pronacs = fetch_pronacs_to_process(conn)
        log.info(f"projetos a processar: {len(pronacs)}")
        if MAX_RECORDS:
            pronacs = pronacs[:MAX_RECORDS]
            log.info(f"SALIC_MAX_RECORDS={MAX_RECORDS}, limitando")

        with SalicClient() as client:
            total_seen = 0
            total_upserted = 0
            buffer: list[tuple] = []
            BATCH = 500
            seen_keys: set[tuple[str, str]] = set()

            for i, pronac in enumerate(pronacs, 1):
                try:
                    fornecedores, status = fetch_fornecedores_for_pronac(client, pronac)
                except Exception as e:
                    log.warning(f"PRONAC {pronac} falhou: {e}")
                    continue

                # Cache 404 / 200-vazio pra próxima execução pular.
                if status in (404, 410) or (status == 200 and not fornecedores):
                    cache_status(conn, pronac, status)

                for f in fornecedores:
                    cgccpf = f.get("cgccpf")
                    if not cgccpf:
                        continue
                    key = (pronac, cgccpf)
                    if key in seen_keys:
                        continue
                    seen_keys.add(key)
                    buffer.append((pronac, cgccpf, Jsonb(f)))
                    total_seen += 1

                if len(buffer) >= BATCH:
                    total_upserted += upsert_batch(conn, buffer)
                    buffer.clear()
                    seen_keys.clear()

                if i % 100 == 0:
                    log.info(
                        f"  [projeto_forn] {i}/{len(pronacs)} projetos · "
                        f"{total_seen} fornecedores"
                    )

            if buffer:
                total_upserted += upsert_batch(conn, buffer)

            log.info(
                f"DONE projetos={len(pronacs)} seen={total_seen} upserted={total_upserted}"
            )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
    sys.exit(0)
