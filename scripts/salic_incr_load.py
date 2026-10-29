"""Loader do modo incremental: re-vincula projeto_incentivador / projeto_fornecedor
para os PRONACs buscados, lendo `links.jsonl` de salic_incr_dump.

Diferença para os ingests antigos (ingest/projeto_*.py): aqueles só olham PRONAC
SEM vínculo (NOT EXISTS) e nunca revisitam. Aqui cada PRONAC buscado tem o
conjunto de vínculos **reconciliado** com a API: entra o que é novo, sai o que a
API não lista mais. Só se mexe no que mudou (sem churn de WAL).

Regras de segurança:
  * 404 da API (sem vínculo) NÃO apaga nada — ausência pode ser instabilidade.
  * Resposta 200 vazia também não apaga.
  * Em projeto_incentivador só as linhas agregadas (data_doacao = 1900-01-01, as que
    vêm de /incentivadores?PRONAC) são reconciliadas. Linhas por doação (datas reais,
    de ingest.incentivador_doacoes) ficam intocadas.
  * Cadastros (salic.incentivadores / salic.fornecedores) só recebem CPF/CNPJ que
    ainda não existem. `total_doado` de /incentivadores?PRONAC é o valor NAQUELE
    projeto, não o total do incentivador — por isso não sobrescreve o cadastro.
  * PRONAC que não existe em salic.projetos é pulado (FK).
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path

AGG_DATE = "1900-01-01"


def _dec(v) -> Decimal:
    try:
        return Decimal(str(v if v is not None else 0))
    except (InvalidOperation, ValueError):
        return Decimal(0)


def read_links(path: Path) -> dict[tuple[str, str], dict]:
    """{(pronac, kind): registro}. Última ocorrência vence. Ignora linha ruim."""
    out: dict[tuple[str, str], dict] = {}
    if not path.exists():
        return out
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            try:
                rec = json.loads(line)
                out[(str(rec["pronac"]).strip(), rec["kind"])] = rec
            except (json.JSONDecodeError, KeyError, TypeError):
                continue
    return out


def desired_incentivadores(rec: dict) -> dict[tuple[str, Decimal], dict]:
    """{(cgccpf, valor): raw} — o conjunto agregado que a API diz existir."""
    out: dict[tuple[str, Decimal], dict] = {}
    for it in rec.get("items") or []:
        cg = str(it.get("cgccpf") or "").strip()
        if cg:
            out[(cg, _dec(it.get("total_doado")))] = it
    return out


def desired_fornecedores(rec: dict) -> dict[str, dict]:
    """{cgccpf: raw}. A API repete o fornecedor por produto; fica o primeiro."""
    out: dict[str, dict] = {}
    for it in rec.get("items") or []:
        cg = str(it.get("cgccpf") or "").strip()
        if cg and cg not in out:
            out[cg] = it
    return out


@dataclass
class Plan:
    pronac: str
    inc_add: dict = field(default_factory=dict)
    inc_del: set = field(default_factory=set)
    forn_add: dict = field(default_factory=dict)
    forn_del: set = field(default_factory=set)

    @property
    def empty(self) -> bool:
        return not (self.inc_add or self.inc_del or self.forn_add or self.forn_del)


def _usable(rec: dict | None) -> bool:
    return bool(rec and rec.get("status") == 200 and rec.get("items"))


def _complete(rec: dict) -> bool:
    """Veio tudo que a API anunciou? Lista truncada não pode apagar vínculo."""
    try:
        return len(rec["items"]) >= int(rec.get("total") or 0)
    except (TypeError, ValueError):
        return False


def plan_pronac(pronac: str, inc_rec: dict | None, forn_rec: dict | None,
                cur_inc: set, cur_forn: set) -> Plan:
    """Diferença entre o que a API lista e o que o banco tem. Função pura."""
    plan = Plan(pronac)
    if _usable(inc_rec):
        want = desired_incentivadores(inc_rec)
        plan.inc_add = {k: v for k, v in want.items() if k not in cur_inc}
        if _complete(inc_rec):
            plan.inc_del = {k for k in cur_inc if k not in want}
    if _usable(forn_rec):
        want_f = desired_fornecedores(forn_rec)
        plan.forn_add = {k: v for k, v in want_f.items() if k not in cur_forn}
        if _complete(forn_rec):
            plan.forn_del = {k for k in cur_forn if k not in want_f}
    return plan


# ---------------------------------------------------------------- banco

def _current(conn, pronacs: list[str]):
    """Vínculos atuais + quais PRONACs existem. Só leitura."""
    with conn.cursor() as cur:
        cur.execute("SELECT pronac FROM salic.projetos WHERE pronac = ANY(%s)", (pronacs,))
        exists = {r[0] for r in cur.fetchall()}
        cur.execute(
            "SELECT pronac, cgccpf, valor FROM salic.projeto_incentivador "
            f"WHERE pronac = ANY(%s) AND data_doacao = DATE '{AGG_DATE}'", (pronacs,))
        inc: dict[str, set] = {}
        for p, cg, v in cur.fetchall():
            inc.setdefault(p, set()).add((cg, _dec(v)))
        cur.execute(
            "SELECT pronac, cgccpf FROM salic.projeto_fornecedor WHERE pronac = ANY(%s)",
            (pronacs,))
        forn: dict[str, set] = {}
        for p, cg in cur.fetchall():
            forn.setdefault(p, set()).add(cg)
    return exists, inc, forn


def build_plans(conn, links: dict, chunk: int = 500) -> tuple[list[Plan], list[str]]:
    pronacs = sorted({p for p, _ in links})
    plans: list[Plan] = []
    missing: list[str] = []
    for i in range(0, len(pronacs), chunk):
        part = pronacs[i:i + chunk]
        exists, inc, forn = _current(conn, part)
        for p in part:
            if p not in exists:
                missing.append(p)
                continue
            plans.append(plan_pronac(p, links.get((p, "incentivadores")),
                                     links.get((p, "fornecedores")),
                                     inc.get(p, set()), forn.get(p, set())))
    return plans, missing


def summarize(plans: list[Plan]) -> dict:
    return {
        "pronacs_com_mudanca": sum(1 for p in plans if not p.empty),
        "incentivador_add": sum(len(p.inc_add) for p in plans),
        "incentivador_del": sum(len(p.inc_del) for p in plans),
        "fornecedor_add": sum(len(p.forn_add) for p in plans),
        "fornecedor_del": sum(len(p.forn_del) for p in plans),
    }


def apply_plans(conn, plans: list[Plan], batch: int = 50) -> int:
    """Aplica em transações de `batch` PRONACs. Devolve linhas inseridas."""
    from psycopg.types.json import Jsonb

    inserted = 0
    todo = [p for p in plans if not p.empty]
    for i in range(0, len(todo), batch):
        with conn.cursor() as cur:
            for pl in todo[i:i + batch]:
                if pl.inc_del:
                    cur.executemany(
                        "DELETE FROM salic.projeto_incentivador WHERE pronac=%s AND cgccpf=%s "
                        f"AND valor=%s AND data_doacao = DATE '{AGG_DATE}'",
                        [(pl.pronac, cg, v) for cg, v in pl.inc_del])
                if pl.inc_add:
                    cur.executemany(
                        "INSERT INTO salic.incentivadores (cgccpf, nome, tipo_pessoa, "
                        "responsavel, uf, municipio) VALUES (%s,%s,%s,%s,%s,%s) "
                        "ON CONFLICT (cgccpf) DO NOTHING",
                        [(cg, r.get("nome"), r.get("tipo_pessoa"), r.get("responsavel"),
                          r.get("UF"), r.get("municipio")) for (cg, _), r in pl.inc_add.items()])
                    cur.executemany(
                        "INSERT INTO salic.projeto_incentivador (pronac, cgccpf, valor, "
                        f"data_doacao, raw) VALUES (%s,%s,%s,DATE '{AGG_DATE}',%s) "
                        "ON CONFLICT DO NOTHING",
                        [(pl.pronac, cg, v, Jsonb(r)) for (cg, v), r in pl.inc_add.items()])
                    inserted += len(pl.inc_add)
                if pl.forn_del:
                    cur.executemany(
                        "DELETE FROM salic.projeto_fornecedor WHERE pronac=%s AND cgccpf=%s",
                        [(pl.pronac, cg) for cg in pl.forn_del])
                if pl.forn_add:
                    cur.executemany(
                        "INSERT INTO salic.fornecedores (cgccpf, nome, email) "
                        "VALUES (%s,%s,%s) ON CONFLICT (cgccpf) DO NOTHING",
                        [(cg, r.get("nome"), r.get("email")) for cg, r in pl.forn_add.items()])
                    cur.executemany(
                        "INSERT INTO salic.projeto_fornecedor (pronac, cgccpf, raw) "
                        "VALUES (%s,%s,%s) ON CONFLICT DO NOTHING",
                        [(pl.pronac, cg, Jsonb(r)) for cg, r in pl.forn_add.items()])
                    inserted += len(pl.forn_add)
        conn.commit()
    return inserted


def record_run(conn, started, finished, seen: int, upserted: int,
               status: str, error: str | None = None) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO salic.ingest_run (endpoint, started_at, finished_at, rows_seen, "
            "rows_upserted, status, error) VALUES (%s,%s,%s,%s,%s,%s,%s)",
            ("incremental_links", started, finished, seen, upserted, status,
             (error or "")[:2000] or None),
        )
    conn.commit()
