"""
Ingest reverso: doações de cada incentivador via /v1/incentivadores/{HASH}/doacoes.

Resolve gap: catálogo SALIC mostra total_doado>0 mas projeto_incentivador vazio
(quando doações foram pra PRONACs antigos não cobertos pelo ingest principal).

Fluxo:
  1. SELECT cgccpf, salic_hash FROM incentivadores
     WHERE total_doado > 0
       AND salic_hash IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM projeto_incentivador WHERE cgccpf = i.cgccpf)
  2. Pra cada um, GET /incentivadores/{hash}/doacoes (paginado)
  3. Cria PRONAC stub se não existir (FK)
  4. Insere em projeto_incentivador (ON CONFLICT DO NOTHING)

Idempotente. Resumable (NOT EXISTS pula quem já tem).

Uso:
  python -m ingest.incentivador_doacoes [--limit N] [--rps 2]
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time

from dotenv import load_dotenv

load_dotenv()

import psycopg
import httpx

from .db import _with_keepalives
from .salic_client import SalicClient

PAGE_SIZE = 100

DB_URL = os.environ.get(
    "DATABASE_URL",
    "postgres://salic:change_me_local_only@127.0.0.1:5544/salic",
)
PG_PASSWORD = os.environ.get("POSTGRES_PASSWORD")
if PG_PASSWORD and "change_me_local_only" in DB_URL:
    DB_URL = DB_URL.replace("change_me_local_only", PG_PASSWORD)
# Inject TCP keepalives so Neon doesn't drop idle SSL conns mid-pagination.
DB_URL = _with_keepalives(DB_URL)

logging.basicConfig(level=logging.INFO, format="[inc_doacoes] %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
log = logging.getLogger(__name__)


def list_pending(conn, limit: int) -> list[tuple[str, str]]:
    """[(cgccpf, salic_hash), ...] de incentivadores sem doações ingestadas e sem 404 cacheado."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT i.cgccpf, i.salic_hash
            FROM salic.incentivadores i
            WHERE i.total_doado > 0
              AND i.salic_hash IS NOT NULL
              AND i.salic_hash <> ''
              AND NOT EXISTS (
                SELECT 1 FROM salic.projeto_incentivador pi WHERE pi.cgccpf = i.cgccpf
              )
              AND NOT EXISTS (
                SELECT 1 FROM metis.salic_fetch_cache c
                WHERE c.kind = 'inc_doacoes' AND c.key = i.salic_hash
                  AND c.status IN (404, 410)
              )
            ORDER BY i.total_doado DESC NULLS LAST
            LIMIT %s
            """,
            (limit,),
        )
        return list(cur.fetchall())


def cache_status(conn, hash_: str, status: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO metis.salic_fetch_cache (kind, key, status, tried_at, retries)
            VALUES ('inc_doacoes', %s, %s, now(), 1)
            ON CONFLICT (kind, key) DO UPDATE
              SET status = EXCLUDED.status,
                  tried_at = now(),
                  retries = metis.salic_fetch_cache.retries + 1
            """,
            (hash_, status),
        )


def fetch_doacoes(client: SalicClient, hash_: str) -> tuple[list[dict], int]:
    """Pagina todas doações via SalicClient (retry de 401/403/429/5xx).
    Retorna (lista, último_status_http)."""
    out: list[dict] = []
    offset = 0
    while True:
        try:
            payload = client._get(
                f"/incentivadores/{hash_}/doacoes",
                params={"limit": PAGE_SIZE, "offset": offset},
            )
        except httpx.HTTPStatusError as e:
            sc = e.response.status_code
            if sc not in (404, 410):
                log.warning("hash=%s status=%s", hash_[:16], sc)
            return out, sc
        except httpx.HTTPError as e:
            log.warning("http erro hash=%s: %r", hash_[:16], e)
            return out, 0
        embedded = (payload.get("_embedded") or {}).get("doacoes") or []
        if not embedded:
            return out, 200
        out.extend(embedded)
        if len(embedded) < PAGE_SIZE:
            return out, 200
        offset += PAGE_SIZE
        if offset > 5000:
            log.warning("hash=%s overflow >5000", hash_[:16])
            return out, 200


def insert_doacoes(conn, cgccpf: str, doacoes: list[dict]) -> int:
    if not doacoes:
        return 0
    rows = []
    pronacs_seen: set[str] = set()
    for d in doacoes:
        pron = (d.get("PRONAC") or d.get("pronac") or "").strip()
        if not pron:
            continue
        try:
            valor = float(d.get("valor") or 0)
        except (TypeError, ValueError):
            valor = 0.0
        data = (d.get("data_recibo") or d.get("data") or "").strip()[:10] or None
        rows.append((pron, cgccpf, data, valor))
        pronacs_seen.add(pron)

    if not rows:
        return 0

    with conn.cursor() as cur:
        # Garante PRONAC stubs
        for pron in pronacs_seen:
            cur.execute("SELECT salic.ensure_projeto_stub(%s)", (pron,))
        # Insert doações
        cur.executemany(
            """
            INSERT INTO salic.projeto_incentivador (pronac, cgccpf, data_doacao, valor)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (pronac, cgccpf, data_doacao, valor) DO NOTHING
            """,
            rows,
        )
        return cur.rowcount


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=200000)
    ap.add_argument("--rps", type=float, default=2.0)
    args = ap.parse_args()

    interval = 1.0 / args.rps if args.rps > 0 else 0
    last = 0.0

    conn = psycopg.connect(DB_URL)
    pending = list_pending(conn, args.limit)
    log.info("pendentes=%d", len(pending))

    seen = 0
    fetched = 0
    inserted = 0
    stubs_created_estimate = 0
    errors = 0
    started = time.time()

    with SalicClient() as client:
        for cgccpf, hash_ in pending:
            seen += 1

            now = time.monotonic()
            elapsed = now - last
            if elapsed < interval:
                time.sleep(interval - elapsed)
            last = time.monotonic()

            try:
                doacoes, status = fetch_doacoes(client, hash_)
                fetched += 1
                if doacoes:
                    n = insert_doacoes(conn, cgccpf, doacoes)
                    inserted += n
                    stubs_created_estimate += len(doacoes)
                # Cacheia apenas 404/410/erro, evita re-bater. 200 com lista vazia também.
                if status in (404, 410) or (status == 200 and not doacoes):
                    cache_status(conn, hash_, status)
                conn.commit()
            except Exception as e:
                errors += 1
                log.warning("erro cgccpf=%s: %r", cgccpf, e)
                conn.rollback()

            if seen % 50 == 0:
                dur = time.time() - started
                rate = seen / dur if dur > 0 else 0
                eta = (len(pending) - seen) / rate if rate > 0 else 0
                log.info(
                    "%d/%d (fetched=%d inserted=%d errors=%d, %.1f r/s, ETA %.0fmin)",
                    seen, len(pending), fetched, inserted, errors, rate, eta / 60,
                )

    conn.close()
    log.info("DONE seen=%d inserted=%d errors=%d dur=%.0fs",
             seen, inserted, errors, time.time() - started)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
