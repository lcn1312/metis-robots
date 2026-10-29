"""Ingest fontes complementares MinC: PNAB, LPG, Cultura Afro, Bolsas, Prêmio Oliveira Silveira.
Suporta CSV/XLSX/ODS. Salva linha-a-linha em salic.fonte_dados (JSONB).

Uso:
  python -m ingest.extras                # tudo
  python -m ingest.extras pnab           # só PNAB
  python -m ingest.extras lpg afro       # múltiplos
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import sys
from typing import Iterable

import httpx
from dotenv import load_dotenv

load_dotenv()

from .db import connect

# fonte → [(recurso_nome, url)]
FONTES: dict[str, list[tuple[str, str]]] = {
    "pnab": [
        ("adesao", "https://dados.cultura.gov.br/dataset/3f1cb66b-390f-40d7-bc1d-c016a9930854/resource/a5b8954a-698c-4820-aabb-33d88135a854/download/adesao---politica-nacional-aldir-blanc.xlsx"),
        ("paar_ciclo1", "https://dados.cultura.gov.br/dataset/3f1cb66b-390f-40d7-bc1d-c016a9930854/resource/877c104d-306e-4a02-9d6a-4ab0c6237762/download/paar-ciclo-1---politica-nacional-aldir-blanc.xlsx"),
        ("execucao_financeira", "https://dados.cultura.gov.br/dataset/3f1cb66b-390f-40d7-bc1d-c016a9930854/resource/0cbcb665-6cfb-42bf-be2d-a107dfb1db02/download/execucao-financeira---politica-nacional-aldir-blanc.xlsx"),
        ("extratos_bancarios", "https://dados.cultura.gov.br/dataset/3f1cb66b-390f-40d7-bc1d-c016a9930854/resource/1bc51c02-cfe1-42cf-b446-509688251eee/download/extrato-bancario---politica-nacional-aldir-blanc.xlsx"),
    ],
    "lpg": [
        ("adesao_estados", "https://dados.cultura.gov.br/dataset/69255f1c-4ad4-4a3e-8621-3fe7ff7bc899/resource/f25d65d2-e1da-48e2-9e6c-f89800a56ea9/download/adesaoestadoslpg.csv"),
        ("adesao_municipios", "https://dados.cultura.gov.br/dataset/69255f1c-4ad4-4a3e-8621-3fe7ff7bc899/resource/c690c08f-21ad-4740-8617-fc3d45d507ee/download/adesaomunicipioslpg.csv"),
        ("execucao_estados", "https://dados.cultura.gov.br/dataset/69255f1c-4ad4-4a3e-8621-3fe7ff7bc899/resource/1b476b2e-880d-484c-bf10-9799683c912f/download/execucaofinanceiraestadoslpg.csv"),
        ("execucao_municipios", "https://dados.cultura.gov.br/dataset/69255f1c-4ad4-4a3e-8621-3fe7ff7bc899/resource/afa83cf2-2e3b-4ea0-bae7-00b2b756dbcf/download/execucaofinanceiramunicipioslpg.csv"),
        ("extratos_bancarios", "https://dados.cultura.gov.br/dataset/69255f1c-4ad4-4a3e-8621-3fe7ff7bc899/resource/4407e849-6418-4d43-947b-a916be10a9e4/download/extratobancariolpg.xlsx"),
        ("relatorio_gestao", "https://dados.cultura.gov.br/dataset/69255f1c-4ad4-4a3e-8621-3fe7ff7bc899/resource/4cdd3d4d-7273-4cea-b17a-ab659eb3ee80/download/relatoriogestaolpg.xlsx"),
    ],
    "cultura_afro": [
        ("projetos", "https://dados.cultura.gov.br/dataset/4ca8f809-376c-44a1-8d27-731c90ce866d/resource/b53a38c0-31ac-42fc-a864-7f4a682427da/download/copia-de-dadosabertosatualizado13062018revisao28.09.2018002.csv"),
    ],
    "bolsas": [
        ("incentivo", "https://dados.cultura.gov.br/dataset/ca700797-090c-4ea9-8718-12303be8765b/resource/922b66e0-3823-4813-9ae4-cbfc847de6bb/download/programadebolsascpincentivo29.11.2018.csv"),
        ("por_edital", "https://dados.cultura.gov.br/dataset/ca700797-090c-4ea9-8718-12303be8765b/resource/79bd6cd1-46ca-4329-923a-b63c527d9294/download/bolsaseditalprograma.csv"),
    ],
    "premio_os": [
        ("premiados", "https://dados.cultura.gov.br/dataset/aeabe1ea-0712-4996-8e1a-d8585c0c5216/resource/0d6f41b7-9a2c-43f4-b951-17ee6cedf5d5/download/premiooliveirasilveira.csv"),
    ],
}


UA_BROWSER = "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"

def fetch(url: str, attempts: int = 4) -> bytes:
    last_err = None
    for i in range(attempts):
        try:
            r = httpx.get(
                url,
                timeout=120.0,
                follow_redirects=True,
                headers={
                    "User-Agent": UA_BROWSER,
                    "Accept": "*/*",
                    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
                    "Referer": "https://dados.cultura.gov.br/",
                },
            )
            if r.status_code in (520, 521, 522, 523, 524, 502, 503, 504, 429):
                last_err = f"HTTP {r.status_code}"
                import time
                time.sleep(2 ** i)
                continue
            r.raise_for_status()
            return r.content
        except httpx.HTTPError as e:
            last_err = str(e)
            import time
            time.sleep(2 ** i)
    raise RuntimeError(f"falha após {attempts} tentativas: {last_err}")


def parse_csv(blob: bytes) -> list[dict]:
    for enc in ("utf-8", "utf-8-sig", "latin-1", "cp1252"):
        try:
            text = blob.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    else:
        raise RuntimeError("encoding desconhecido")
    sample = text[:4096]
    sniffer = csv.Sniffer()
    try:
        dialect = sniffer.sniff(sample, delimiters=",;\t|")
    except csv.Error:
        dialect = csv.excel
    reader = csv.DictReader(io.StringIO(text), dialect=dialect)
    rows = []
    for r in reader:
        rows.append({k.strip() if k else "_": (v.strip() if isinstance(v, str) else v) for k, v in r.items()})
    return rows


def parse_xlsx(blob: bytes) -> list[dict]:
    from openpyxl import load_workbook
    wb = load_workbook(io.BytesIO(blob), read_only=True, data_only=True)
    out = []
    for ws in wb.worksheets:
        all_rows = list(ws.iter_rows(values_only=True))
        # Detect header: first row with >= 3 non-empty cells where most are str-like
        header_idx = 0
        for i, r in enumerate(all_rows[:10]):
            non_empty = sum(1 for c in r if c is not None and str(c).strip() != "")
            if non_empty >= 3:
                header_idx = i
                break
        if header_idx >= len(all_rows):
            continue
        header_row = all_rows[header_idx]
        header = [str(c).strip() if c is not None else f"col{i}" for i, c in enumerate(header_row)]
        # Dedupe (planilhas podem repetir nome de coluna)
        seen: dict[str, int] = {}
        for j, h in enumerate(header):
            if h in seen:
                seen[h] += 1
                header[j] = f"{h}_{seen[h]}"
            else:
                seen[h] = 0
        for r in all_rows[header_idx + 1:]:
            if all(c is None or str(c).strip() == "" for c in r):
                continue
            d = {h: (str(v).strip() if v is not None else None) for h, v in zip(header, r)}
            d["_sheet"] = ws.title
            out.append(d)
    return out


def parse_any(url: str, blob: bytes) -> list[dict]:
    u = url.lower()
    if u.endswith(".csv"):
        return parse_csv(blob)
    if u.endswith(".xlsx"):
        return parse_xlsx(blob)
    if u.endswith(".ods"):
        try:
            from pyexcel_ods3 import get_data  # noqa
        except ImportError:
            from odf.opendocument import load
            from odf.table import Table, TableRow, TableCell
            doc = load(io.BytesIO(blob))
            out = []
            for sheet in doc.spreadsheet.getElementsByType(Table):
                rows = sheet.getElementsByType(TableRow)
                if not rows:
                    continue
                header = [str(c) for c in rows[0].getElementsByType(TableCell)]
                for tr in rows[1:]:
                    cells = [str(c) for c in tr.getElementsByType(TableCell)]
                    out.append(dict(zip(header, cells)))
            return out
    raise RuntimeError(f"formato não suportado: {url}")


def ingest_resource(conn, fonte: str, recurso: str, url: str) -> int:
    print(f"[{fonte}/{recurso}] download...")
    blob = fetch(url)
    sha = hashlib.sha256(blob).hexdigest()
    formato = url.rsplit(".", 1)[-1]
    print(f"[{fonte}/{recurso}] {len(blob):,} bytes ({formato}), parsing...")
    rows = parse_any(url, blob)
    print(f"[{fonte}/{recurso}] {len(rows)} linhas. Inserindo...")

    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM salic.fonte_dados WHERE fonte=%s AND recurso=%s",
            (fonte, recurso),
        )
        # batch insert
        BATCH = 500
        batch: list[tuple] = []
        from psycopg.types.json import Jsonb
        for r in rows:
            batch.append((fonte, recurso, Jsonb(r)))
            if len(batch) >= BATCH:
                cur.executemany(
                    "INSERT INTO salic.fonte_dados (fonte, recurso, data) VALUES (%s,%s,%s)",
                    batch,
                )
                batch.clear()
        if batch:
            cur.executemany(
                "INSERT INTO salic.fonte_dados (fonte, recurso, data) VALUES (%s,%s,%s)",
                batch,
            )

        cur.execute(
            "INSERT INTO salic.fonte_recurso (fonte, recurso, url, formato, rows, sha256) "
            "VALUES (%s,%s,%s,%s,%s,%s) "
            "ON CONFLICT (fonte, recurso) DO UPDATE SET url=EXCLUDED.url, formato=EXCLUDED.formato, rows=EXCLUDED.rows, sha256=EXCLUDED.sha256, ingested_at=now()",
            (fonte, recurso, url, formato, len(rows), sha),
        )
    conn.commit()
    print(f"[{fonte}/{recurso}] OK {len(rows)} rows")
    return len(rows)


def main():
    targets = sys.argv[1:] or list(FONTES.keys())
    unknown = [t for t in targets if t not in FONTES]
    if unknown:
        print(f"fontes desconhecidas: {unknown}", file=sys.stderr)
        print(f"válidas: {list(FONTES.keys())}", file=sys.stderr)
        sys.exit(2)

    total = 0
    with connect() as conn:
        for fonte in targets:
            for recurso, url in FONTES[fonte]:
                try:
                    total += ingest_resource(conn, fonte, recurso, url)
                except Exception as e:
                    print(f"[{fonte}/{recurso}] ERRO {e}", file=sys.stderr)
    print(f"\n=== TOTAL: {total:,} linhas ===")


if __name__ == "__main__":
    main()
