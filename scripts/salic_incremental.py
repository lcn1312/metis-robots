"""Modo incremental SALIC: re-vincula incentivadores/fornecedores dos PRONACs quentes.

Em vez de varrer o catálogo inteiro (fornecedores leva 25-33h), busca
`/incentivadores?PRONAC=X` e `/fornecedores?PRONAC=X` só para os projetos que
podem ter mudado, e reconcilia projeto_incentivador / projeto_fornecedor.
Quem é "quente": ver scripts/salic_incr_select.py.

Etapas (cada uma retomável, tudo em data/incr/<AAAA-MM-DD>/):

    select  -> pronacs.txt      (lê o banco, ou --pronacs-file)
    dump    -> links.jsonl      (só API + disco; checkpoint = o próprio JSONL)
    load    -> banco            (dry-run por padrão; --apply grava)
    run     =  select + dump + load, com pings de healthcheck

Exemplos:
    DATABASE_URL=... .venv/bin/python scripts/salic_incremental.py run            # dry-run
    DATABASE_URL=... .venv/bin/python scripts/salic_incremental.py run --apply
    .venv/bin/python scripts/salic_incremental.py run --pronacs-file lista.txt    # sem banco

DATABASE_URL é obrigatório para select/load (não lê .env sozinho — o wrapper
scripts/salic_incremental.sh exporta). Nunca é impresso; só o host aparece.

Healthcheck: HEALTHCHECK_URL_SALIC_INCREMENTAL (opcional, protocolo healthchecks.io).
Parar: touch data/incr/STOP (o dump termina o par corrente e sai).
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent))

import _healthcheck  # noqa: E402
import salic_incr_select as sel  # noqa: E402
from salic_dump import log  # noqa: E402
from salic_incr_dump import KINDS, dump_links  # noqa: E402
from salic_incr_load import (  # noqa: E402
    apply_plans, build_plans, read_links, record_run, summarize,
)

JOB = "SALIC_INCREMENTAL"
BASE_DIR = Path("data/incr")
STATE_PATH = BASE_DIR / "state.json"
STOP_PATH = BASE_DIR / "STOP"


def _connect():
    if not os.environ.get("DATABASE_URL"):
        sys.exit("DATABASE_URL ausente (use --pronacs-file para rodar sem banco)")
    from psycopg.conninfo import conninfo_to_dict

    from ingest.db import connect

    host = conninfo_to_dict(os.environ["DATABASE_URL"]).get("host", "?")
    log(f"[incr] banco: host={host}")
    return connect()


# ---------------------------------------------------------------- etapas

def cmd_select(args, run_dir: Path) -> list[str]:
    out = run_dir / "pronacs.txt"
    if args.pronacs_file:
        hot = sel.read_pronacs_file(Path(args.pronacs_file))
    elif out.exists() and not args.reselect:
        pronacs = [ln.split("\t")[0] for ln in out.read_text().splitlines() if ln.strip()]
        log(f"[incr] reusando seleção de {out} ({len(pronacs)} PRONACs)")
        return pronacs
    else:
        today = date.today()
        with _connect() as conn:
            since = (datetime.fromisoformat(args.since) if args.since
                     else sel.last_projetos_run(conn))
            cands = sel.fetch_candidates(conn, sel.months_ago(today, args.months), since)
        log(f"[incr] {len(cands)} candidatos | último refresh de projetos: {since}")
        hot = sel.select_hot(cands, today, since, sel.load_state(STATE_PATH),
                             max_pronacs=args.max_pronacs, months=args.months)
    if args.max_pronacs:
        hot = hot[:args.max_pronacs]
    run_dir.mkdir(parents=True, exist_ok=True)
    out.write_text("".join(f"{h.pronac}\t{h.reason}\n" for h in hot))
    by_reason: dict[str, int] = {}
    for h in hot:
        by_reason[h.reason] = by_reason.get(h.reason, 0) + 1
    log(f"[incr] selecionados {len(hot)} PRONACs {by_reason} -> {out}")
    return [h.pronac for h in hot]


def cmd_dump(args, run_dir: Path, pronacs: list[str]) -> dict:
    deadline = time.time() + args.budget_min * 60 if args.budget_min else None
    return dump_links(pronacs, run_dir, workers=args.workers,
                      deadline=deadline, stop_file=STOP_PATH)


def _fetched_pronacs(links: dict) -> dict[str, datetime]:
    """PRONACs com os dois tipos buscados -> instante do fetch mais antigo."""
    out: dict[str, datetime] = {}
    for p in {p for p, _ in links}:
        recs = [links.get((p, k)) for k in KINDS]
        if all(r and r.get("status") in (200, 404) for r in recs):
            out[p] = min(datetime.fromisoformat(r["fetched_at"]) for r in recs)
    return out


def cmd_load(args, run_dir: Path) -> str:
    links = read_links(run_dir / "links.jsonl")
    seen = sum(len(r.get("items") or []) for r in links.values())
    msg = f"{len(links)} pares no dump, {seen} vínculos lidos"
    log(f"[incr] {msg}")
    if not links:
        return msg
    if not os.environ.get("DATABASE_URL"):
        log("[incr] sem DATABASE_URL: só o resumo do disco (nada comparado com o banco)")
        return msg

    started = datetime.now(timezone.utc)
    with _connect() as conn:
        plans, missing = build_plans(conn, links)
        summary = summarize(plans)
        log(f"[incr] plano: {summary} | PRONACs fora de salic.projetos: {len(missing)}")
        if not args.apply:
            log("[incr] DRY-RUN — nada escrito. Rode com --apply para gravar.")
            return f"{msg}; dry-run {summary}"
        try:
            inserted = apply_plans(conn, plans)
        except Exception as e:
            conn.rollback()
            record_run(conn, started, datetime.now(timezone.utc), seen, 0, "error",
                       f"{type(e).__name__}: {e}")
            raise
        record_run(conn, started, datetime.now(timezone.utc), seen, inserted, "ok",
                   None)
    fetched = _fetched_pronacs(links)
    sel.save_state(STATE_PATH, sel.load_state(STATE_PATH), fetched)
    log(f"[incr] APLICADO: {inserted} vínculos inseridos | {summary}")
    return f"{msg}; aplicado {summary}"


# ---------------------------------------------------------------- CLI

def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("command", choices=("select", "dump", "load", "run"))
    ap.add_argument("--run-dir", default=None, help="padrão: data/incr/<hoje>")
    ap.add_argument("--pronacs-file", default=None, help="lista manual; dispensa o banco")
    ap.add_argument("--reselect", action="store_true", help="refaz a seleção do dia")
    ap.add_argument("--since", default=None, help="ISO; padrão: último run ok de projetos")
    ap.add_argument("--months", type=int, default=sel.HOT_MONTHS)
    ap.add_argument("--max-pronacs", type=int, default=0, help="0 = sem teto")
    ap.add_argument("--workers", type=int, default=2, help="conexões simultâneas (máx 4)")
    ap.add_argument("--budget-min", type=int, default=360, help="teto de tempo do dump")
    ap.add_argument("--apply", action="store_true", help="grava no banco (padrão: dry-run)")
    return ap


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    run_dir = Path(args.run_dir or BASE_DIR / date.today().isoformat())

    if args.command == "select":
        cmd_select(args, run_dir)
    elif args.command == "dump":
        cmd_dump(args, run_dir, cmd_select(args, run_dir))
    elif args.command == "load":
        cmd_load(args, run_dir)
    else:
        _healthcheck.ping(JOB, "start")
        try:
            pronacs = cmd_select(args, run_dir)
            stats = cmd_dump(args, run_dir, pronacs)
            result = cmd_load(args, run_dir)
        except BaseException as e:
            _healthcheck.ping(JOB, "fail", f"{type(e).__name__}: {e}")
            raise
        body = f"dump {stats}\n{result}"
        _healthcheck.ping(JOB, "success" if stats["errors"] == 0 else "fail", body)
        return 0 if stats["errors"] == 0 else 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
