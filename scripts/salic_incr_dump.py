"""Dump por PRONAC (incentivadores + fornecedores) para JSONL, com retomada.

Cada linha de `links.jsonl` é um registro completo de um par (pronac, kind):

    {"pronac": "247131", "kind": "fornecedores", "status": 200, "total": 905,
     "items": [...], "fetched_at": "2026-09-24T03:12:00+00:00"}

status 404 = a API não tem vínculo para aquele PRONAC (resposta normal da SALIC).
Falha definitiva vai para `errors.jsonl` e NÃO conta como feita — a próxima
execução tenta de novo.

Retomada: o próprio JSONL é o checkpoint. Na abertura, lemos as linhas válidas,
truncamos uma eventual linha parcial no fim (queda no meio do write) e pulamos os
pares já presentes.
"""
from __future__ import annotations

import json
import os
import queue
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from salic_dump import PAGE_SIZE, get_json, log

KINDS = ("incentivadores", "fornecedores")
MAX_OFFSET = 10_000  # trava de segurança: nenhum PRONAC real passa disso
MAX_WORKERS = 4  # >6 conexões = 503 em cascata; 4 é o teto seguro medido


def load_done(path: Path) -> set[tuple[str, str]]:
    """Pares (pronac, kind) já gravados; trunca linha parcial no fim do arquivo."""
    done: set[tuple[str, str]] = set()
    if not path.exists():
        return done
    good_end = 0
    with path.open("r+", encoding="utf-8") as f:
        while True:
            line = f.readline()
            if not line:
                break
            if not line.endswith("\n"):
                break  # linha parcial: escrita interrompida
            try:
                rec = json.loads(line)
                key = (str(rec["pronac"]), str(rec["kind"]))
            except (json.JSONDecodeError, KeyError, TypeError):
                break  # corrompida: tudo dali em diante é descartado
            if rec.get("status") in (200, 404):
                done.add(key)
            good_end = f.tell()
        f.seek(0, os.SEEK_END)
        if f.tell() != good_end:
            log(f"[incr] truncando {path.name} em {good_end} bytes (linha parcial)")
            f.truncate(good_end)
    return done


def fetch_links(pronac: str, kind: str) -> dict:
    """Todas as páginas de /<kind>?PRONAC=X. Levanta em erro definitivo."""
    items: list[dict] = []
    offset = 0
    total = None
    status = 200
    while offset <= MAX_OFFSET:
        sc, data = get_json(
            f"/{kind}", {"PRONAC": pronac, "limit": PAGE_SIZE, "offset": offset},
            tag=f"{kind[:4]}:{pronac}", allow_404=True,
        )
        if sc == 404:
            status = 404 if offset == 0 else 200
            break
        total = data.get("total", total)
        page = (data.get("_embedded") or {}).get(kind) or []
        items.extend(page)
        offset += len(page)
        if len(page) < PAGE_SIZE or (total is not None and offset >= int(total)):
            break
    return {
        "pronac": pronac,
        "kind": kind,
        "status": status,
        "total": total if total is not None else len(items),
        "items": items,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }


def dump_links(
    pronacs: list[str],
    run_dir: Path,
    workers: int = 2,
    deadline: float | None = None,
    stop_file: Path | None = None,
    fetch=fetch_links,
) -> dict:
    """Busca os pares pendentes. Devolve contadores. Seguro para relançar."""
    run_dir.mkdir(parents=True, exist_ok=True)
    out_path = run_dir / "links.jsonl"
    err_path = run_dir / "errors.jsonl"
    done = load_done(out_path)

    tasks: queue.Queue = queue.Queue()
    pending = [(p, k) for p in pronacs for k in KINDS if (p, k) not in done]
    for t in pending:
        tasks.put(t)
    log(f"[incr] {len(pronacs)} PRONACs | {len(done)} pares já feitos | "
        f"{len(pending)} pendentes | workers={workers}")

    lock = threading.Lock()
    stats = {"ok": 0, "not_found": 0, "errors": 0, "items": 0, "skipped_budget": 0}
    started = time.time()

    def should_stop() -> bool:
        if deadline is not None and time.time() >= deadline:
            return True
        return bool(stop_file and stop_file.exists())

    def worker() -> None:
        while True:
            try:
                pronac, kind = tasks.get_nowait()
            except queue.Empty:
                return
            if should_stop():
                with lock:
                    stats["skipped_budget"] += 1
                continue
            try:
                rec = fetch(pronac, kind)
                line, path = json.dumps(rec, ensure_ascii=False) + "\n", out_path
            except Exception as e:  # noqa: BLE001 — registra e segue
                rec = None
                line = json.dumps({"pronac": pronac, "kind": kind,
                                   "error": f"{type(e).__name__}: {str(e)[:300]}",
                                   "at": datetime.now(timezone.utc).isoformat()}) + "\n"
                path = err_path
            with lock:
                with path.open("a", encoding="utf-8") as f:
                    f.write(line)
                    f.flush()
                    os.fsync(f.fileno())
                if rec is None:
                    stats["errors"] += 1
                elif rec["status"] == 404:
                    stats["not_found"] += 1
                else:
                    stats["ok"] += 1
                    stats["items"] += len(rec["items"])
                n = stats["ok"] + stats["not_found"] + stats["errors"]
                if n % 50 == 0:
                    rate = n / max((time.time() - started) / 60, 1e-9)
                    log(f"[incr] {n}/{len(pending)} pares | {rate:.1f} pares/min | {stats}")

    n_workers = max(1, min(workers, MAX_WORKERS))
    threads = [threading.Thread(target=worker, name=f"w{i}") for i in range(n_workers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    stats["seconds"] = round(time.time() - started)
    stats["complete"] = stats["skipped_budget"] == 0 and stats["errors"] == 0
    log(f"[incr] dump FIM {stats}")
    return stats
