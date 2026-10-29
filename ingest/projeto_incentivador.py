"""F4 desbloqueio — ingest projeto_incentivador.

API SALIC: GET /incentivadores?PRONAC=NNNN paginado.
Iteramos apenas projetos com valor_captado > 0 (sem captação não há incentivador).

Volume estimado: ~15-20k projetos × 1-3 incentivadores médio = 30-60k linhas.

Idempotente via ON CONFLICT (pronac, cgccpf, data_doacao, valor) DO NOTHING.

Uso:
    cd "/Users/lucanpiai/Desktop/11COWORK/DB EDITAIS"
    . .venv/bin/activate
    python -m ingest.projeto_incentivador          # full
    SALIC_MAX_RECORDS=100 python -m ingest.projeto_incentivador  # smoke
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timezone

from dotenv import load_dotenv
load_dotenv()

import psycopg
from psycopg.types.json import Jsonb

from .db import _with_keepalives
from .salic_client import SalicClient

DB_URL = os.environ.get("DATABASE_URL", "postgres://salic:change_me_local_only@127.0.0.1:5544/salic")
PG_PASSWORD = os.environ.get("POSTGRES_PASSWORD")
if PG_PASSWORD and "change_me_local_only" in DB_URL:
    DB_URL = DB_URL.replace("change_me_local_only", PG_PASSWORD)
# Inject TCP keepalives so Neon doesn't drop idle SSL conns mid-pagination.
DB_URL = _with_keepalives(DB_URL)

MAX_RECORDS = int(os.getenv("SALIC_MAX_RECORDS", "0"))  # 0 = ilimitado
PAGE_SIZE = int(os.getenv("SALIC_PAGE_SIZE", "100"))

logging.basicConfig(
    level=logging.INFO,
    format="[projeto_incent] %(message)s",
)
log = logging.getLogger(__name__)


def fetch_incentivadores_for_pronac(client: SalicClient, pronac: str) -> list[dict]:
    """Pagina incentivadores de um PRONAC."""
    out: list[dict] = []
    offset = 0
    while True:
        try:
            data = client._get(
                "/incentivadores",
                params={"PRONAC": pronac, "limit": PAGE_SIZE, "offset": offset},
            )
        except Exception as e:
            # 404/no_donator é normal — projeto sem captação real ainda
            msg = str(e)
            if "404" in msg or "donator" in msg.lower():
                return out
            raise
        items = data.get("_embedded", {}).get("incentivadores", []) or []
        if not items:
            return out
        out.extend(items)
        if len(items) < PAGE_SIZE:
            return out
        offset += len(items)
        if offset > 5000:
            log.warning(f"PRONAC {pronac}: >5000 incentivadores, parando paginação")
            return out


def connect() -> psycopg.Connection:
    """Abre conexão com timeout de transação folgado (Neon mata em 5min por padrão)."""
    conn = psycopg.connect(DB_URL)
    with conn.cursor() as cur:
        cur.execute("SET idle_in_transaction_session_timeout = '30min';")
    conn.commit()
    return conn


def upsert_batch(conn, rows: list[tuple]) -> int:
    """Bulk upsert em salic.projeto_incentivador."""
    if not rows:
        return 0
    sql = """
        INSERT INTO salic.projeto_incentivador (pronac, cgccpf, valor, data_doacao, raw)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (pronac, cgccpf, data_doacao, valor) DO NOTHING
    """
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
        n = cur.rowcount
    conn.commit()
    return n


def upsert_resilient(conn, rows: list[tuple]) -> tuple[psycopg.Connection, int, list[tuple]]:
    """Upsert tolerante a queda de conexão / linha inválida.

    Em erro: reconecta e reprocessa por bisseção até isolar a(s) linha(s) ruim(ns),
    que são descartadas com log. Retorna (conn, upsertadas, descartadas).
    """
    if not rows:
        return conn, 0, []
    try:
        return conn, upsert_batch(conn, rows), []
    except psycopg.Error as e:
        try:
            conn.close()
        except Exception:
            pass
        conn = connect()
        if len(rows) == 1:
            pronac, cgccpf = rows[0][0], rows[0][1]
            log.error(f"linha DESCARTADA pronac={pronac} cgccpf={cgccpf}: {e}")
            return conn, 0, list(rows)
        log.warning(f"batch de {len(rows)} falhou ({e}); bissectando")
        mid = len(rows) // 2
        conn, n_left, bad_left = upsert_resilient(conn, rows[:mid])
        conn, n_right, bad_right = upsert_resilient(conn, rows[mid:])
        return conn, n_left + n_right, bad_left + bad_right


def fetch_pronacs_to_process(conn, only_with_captacao: bool = True) -> list[str]:
    """Retorna PRONACs ainda não ingeridos OU todos se reprocessar."""
    with conn.cursor() as cur:
        if only_with_captacao:
            cur.execute(
                """
                SELECT p.pronac FROM salic.projetos p
                WHERE p.valor_captado > 0
                  AND NOT EXISTS (
                    SELECT 1 FROM salic.projeto_incentivador pi WHERE pi.pronac = p.pronac
                  )
                ORDER BY p.valor_captado DESC
                """
            )
        else:
            cur.execute("SELECT pronac FROM salic.projetos ORDER BY valor_captado DESC NULLS LAST")
        return [r[0] for r in cur.fetchall()]


def main() -> None:
    log.info(f"DB: {DB_URL.split('@')[-1]}")
    conn = connect()
    try:
        pronacs = fetch_pronacs_to_process(conn)
        log.info(f"projetos a processar: {len(pronacs)}")
        if MAX_RECORDS:
            pronacs = pronacs[:MAX_RECORDS]
            log.info(f"SALIC_MAX_RECORDS={MAX_RECORDS}, limitando")

        with SalicClient() as client:
            total_seen = 0
            total_upserted = 0
            total_descartadas = 0
            buffer: list[tuple] = []
            BATCH = int(os.getenv("SALIC_UPSERT_BATCH", "200"))

            for i, pronac in enumerate(pronacs, 1):
                try:
                    incentivadores = fetch_incentivadores_for_pronac(client, pronac)
                except Exception as e:
                    log.warning(f"PRONAC {pronac} falhou: {e}")
                    continue

                for inc in incentivadores:
                    cgccpf = inc.get("cgccpf")
                    valor = inc.get("total_doado") or 0
                    # API agrega por incentivador — sem data_doacao individual.
                    # Usamos data atual da ingestão como proxy + raw guarda original.
                    data_doacao = "1900-01-01"  # default schema permite
                    if not cgccpf:
                        continue
                    buffer.append((
                        pronac,
                        cgccpf,
                        valor,
                        data_doacao,
                        Jsonb(inc),
                    ))
                    total_seen += 1

                if len(buffer) >= BATCH:
                    conn, n_ok, descartadas = upsert_resilient(conn, buffer)
                    total_upserted += n_ok
                    total_descartadas += len(descartadas)
                    buffer.clear()

                if i % 100 == 0:
                    log.info(f"  [projeto_incent] {i}/{len(pronacs)} projetos · {total_seen} incentivadores")

            if buffer:
                conn, n_ok, descartadas = upsert_resilient(conn, buffer)
                total_upserted += n_ok
                total_descartadas += len(descartadas)

            log.info(
                f"DONE projetos={len(pronacs)} seen={total_seen} "
                f"upserted={total_upserted} descartadas={total_descartadas}"
            )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
    sys.exit(0)
