"""Carrega no banco o dump JSONL gerado por scripts/salic_dump.py.

Reusa o mesmo caminho do ingest normal — os mappers de `ingest.transform` e
`ingest.upsert.safe_upsert` — então a guarda `IS DISTINCT FROM` vale aqui também:
linha idêntica à que já está no banco não é reescrita (sem churn de WAL).

Por segurança **não escreve nada por padrão**. Rode primeiro sem flag para ver o
diagnóstico, depois com --apply.

    .venv/bin/python scripts/salic_load.py --resource projetos           # dry-run
    .venv/bin/python scripts/salic_load.py --resource projetos --apply   # escreve

Em `projetos` também repopula `salic.projeto_local` (vem aninhado em
`local_realizacao` no mesmo payload).

DATABASE_URL vem do .env (NEON_URL para prod). Nunca ecoar a senha.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

from ingest.db import connect  # noqa: E402
from ingest.transform import (  # noqa: E402
    fornecedor_row,
    incentivador_row,
    projeto_locais,
    projeto_row,
    proponente_row,
    proposta_row,
)
from ingest.upsert import safe_upsert  # noqa: E402

BATCH = 1000

# resource -> (tabela, mapper, coluna de PK)
SPECS = {
    "projetos": ("projetos", projeto_row, "pronac"),
    "propostas": ("propostas", proposta_row, "id"),
    "proponentes": ("proponentes", proponente_row, "cgccpf"),
    "incentivadores": ("incentivadores", incentivador_row, "cgccpf"),
    "fornecedores": ("fornecedores", fornecedor_row, "cgccpf"),
}

LOCAL_PK = ["pronac", "municipio_ibge", "municipio", "uf"]


def read_dump(resource: str, out_dir: Path, keep_raw: bool) -> dict:
    """Lê todos os .jsonl e devolve {pk: row} já deduplicado.

    As fatias podem se sobrepor: o catálogo muda embaixo da paginação por offset,
    então o mesmo registro pode aparecer em duas fatias. Última ocorrência vence.

    Com `keep_raw`, o valor vira (row, raw) — necessário em projetos, que carrega
    `local_realizacao` aninhado.
    """
    _, mapper, pk_col = SPECS[resource]
    files = sorted(out_dir.glob(f"{resource}-*.jsonl"))
    if not files:
        sys.exit(f"nenhum .jsonl de {resource} em {out_dir}")

    by_pk: dict = {}
    total_lines = 0
    bad = 0
    for f in files:
        n = 0
        with f.open(encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                total_lines += 1
                n += 1
                try:
                    raw = json.loads(line)
                    row = mapper(raw)
                except (json.JSONDecodeError, KeyError, TypeError, ValueError):
                    bad += 1
                    continue
                key = row.get(pk_col)
                if key in (None, ""):
                    bad += 1
                    continue
                by_pk[key] = (row, raw) if keep_raw else row
        print(f"  {f.name}: {n} linhas")

    print(f"total lido: {total_lines} linhas | únicos: {len(by_pk)} | descartados: {bad}")
    return by_pk


def _count(conn, table: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM salic.{table}")
        return cur.fetchone()[0]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--resource", required=True, choices=sorted(SPECS))
    ap.add_argument("--apply", action="store_true", help="escreve no banco (padrão: dry-run)")
    ap.add_argument("--dir", default=None, help="padrão: data/raw/<resource>")
    args = ap.parse_args()

    resource = args.resource
    table, _, pk_col = SPECS[resource]
    is_projetos = resource == "projetos"
    out_dir = Path(args.dir or f"data/raw/{resource}")

    entries = read_dump(resource, out_dir, keep_raw=is_projetos)

    with connect() as conn:
        conn_ref = [conn]
        before = _count(conn, table)
        print(f"banco tem hoje: {before} linhas")

        # Em fatias: 350k+ chaves num único ANY() vira um parâmetro de ~10MB.
        keys = list(entries.keys())
        existentes: set = set()
        with conn.cursor() as cur:
            for i in range(0, len(keys), 50_000):
                cur.execute(
                    f"SELECT {pk_col} FROM salic.{table} WHERE {pk_col} = ANY(%s)",
                    (keys[i:i + 50_000],),
                )
                existentes.update(r[0] for r in cur.fetchall())
        novos = len(entries) - len(existentes)
        print(f"do dump: {novos} novos, {len(existentes)} já existem")

        if not args.apply:
            print("\nDRY-RUN — nada escrito. Rode com --apply para gravar.")
            return

        started = datetime.now(timezone.utc)
        batch: list[dict] = []
        locais: list[dict] = []
        done = 0
        locais_done = 0

        def flush() -> None:
            nonlocal done, locais_done
            if batch:
                done += safe_upsert(conn_ref, table, [pk_col], batch)
                batch.clear()
            if locais:
                locais_done += safe_upsert(conn_ref, "projeto_local", LOCAL_PK, locais)
                locais.clear()

        for entry in entries.values():
            row, raw = entry if is_projetos else (entry, None)
            batch.append(row)
            if is_projetos:
                for loc in projeto_locais(raw):
                    locais.append({"pronac": row["pronac"], **loc})
            if len(batch) >= BATCH:
                flush()
                print(f"  {done}/{len(entries)}", flush=True)
        flush()

        after = _count(conn_ref[0], table)
        with conn_ref[0].cursor() as cur:
            cur.execute(
                "INSERT INTO salic.ingest_run (endpoint, started_at, finished_at, "
                "rows_seen, rows_upserted, status) VALUES (%s,%s,%s,%s,%s,%s)",
                (resource, started, datetime.now(timezone.utc),
                 len(entries), done, "ok"),
            )
        conn_ref[0].commit()
        print(f"FIM: {done} upserted | banco {before} -> {after} (+{after - before})")
        if is_projetos:
            print(f"projeto_local: {locais_done} upserted")


if __name__ == "__main__":
    main()
