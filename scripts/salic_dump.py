"""Dump completo de um recurso da API SALIC para JSONL local.

Não toca no banco — só disco. Rode e depois carregue com scripts/salic_load.py.

Uso:
    .venv/bin/python scripts/salic_dump.py --resource projetos [--slices 4] [--out DIR]

Recursos: projetos, propostas, proponentes, incentivadores, fornecedores.

Retomável: cada fatia grava checkpoint com o próximo offset. Rodar de novo continua
de onde parou (linhas já gravadas são truncadas até o checkpoint).
"""
from __future__ import annotations

import argparse
import json
import os
import random
import sys
import threading
import time
import traceback
from dataclasses import dataclass
from pathlib import Path

import httpx

API_BASE = os.getenv("SALIC_API_BASE", "https://api.salic.cultura.gov.br/api/v1")
PAGE_SIZE = int(os.getenv("SALIC_PAGE_SIZE", "100"))  # 500 devolve 422
USER_AGENT = os.getenv("SALIC_USER_AGENT", "salic-local-research/0.1")

RESOURCES = ("projetos", "propostas", "proponentes", "incentivadores", "fornecedores")

# Timeout granular: o Cloudflare da SALIC derruba sockets sem FIN, deixando a
# conexão ESTABLISHED e pendurada pra sempre. Um read timeout curto mata o socket
# morto rápido e a retentativa (conexão nova) volta em segundos. Páginas frias
# levam 32-60s, então 120s de read dá folga sem desperdiçar minutos num socket zumbi.
TIMEOUT = httpx.Timeout(
    connect=float(os.getenv("SALIC_TIMEOUT_CONNECT", "15")),
    read=float(os.getenv("SALIC_TIMEOUT_READ", "120")),
    write=15.0,
    pool=15.0,
)

MAX_ATTEMPTS = int(os.getenv("SALIC_MAX_ATTEMPTS", "30"))
BACKOFF_MAX = float(os.getenv("SALIC_BACKOFF_MAX", "600"))  # 503 em cascata dura até 1h

_print_lock = threading.Lock()


def log(msg: str) -> None:
    with _print_lock:
        print(f"{time.strftime('%H:%M:%S')} {msg}", flush=True)


def _retryable(exc: BaseException) -> bool:
    """5xx/429/timeout/transporte + 401/403 (WAF transitório da SALIC).

    404/400/422 são definitivos — não retenta.
    """
    if isinstance(exc, (httpx.TimeoutException, httpx.TransportError)):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        sc = exc.response.status_code
        return sc >= 500 or sc in (401, 403, 429)
    return False


@dataclass(frozen=True)
class Slice:
    idx: int
    start: int
    end: int | None  # None = vai até a API secar


def _new_client() -> httpx.Client:
    return httpx.Client(
        base_url=API_BASE,
        timeout=TIMEOUT,
        headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
        follow_redirects=True,
    )


def get_json(path: str, params: dict, tag: str, allow_404: bool = False) -> tuple[int, dict]:
    """GET genérico com retry/backoff. Devolve (status, json). Levanta se estourar
    MAX_ATTEMPTS.

    Conexão nova a cada tentativa: o pool do httpx reusaria um socket que o
    Cloudflare já matou silenciosamente, pendurando toda tentativa seguinte.

    Com `allow_404`, 404 vira resposta normal: a SALIC responde 404 para filtro sem
    resultado (ex. `fornecedores?PRONAC=X` de projeto sem fornecedor).
    """
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            with _new_client() as client:
                r = client.get(path, params=params)
                if allow_404 and r.status_code == 404:
                    try:
                        return 404, r.json()
                    except ValueError:
                        return 404, {}
                r.raise_for_status()
                return r.status_code, r.json()
        except Exception as e:  # noqa: BLE001 — classificado em _retryable
            if not _retryable(e) or attempt == MAX_ATTEMPTS:
                raise
            wait = min(BACKOFF_MAX, 5 * 2 ** min(attempt, 7)) * (0.7 + 0.6 * random.random())
            log(f"[{tag}] {path} {params} tentativa {attempt}/{MAX_ATTEMPTS} falhou "
                f"({type(e).__name__}: {str(e)[:80]}); dorme {wait:.0f}s")
            time.sleep(wait)
    raise RuntimeError("unreachable")


def fetch_page(resource: str, offset: int, tag: str) -> list[dict]:
    """Uma página do catálogo completo, com retry/backoff."""
    _, data = get_json(f"/{resource}", {"limit": PAGE_SIZE, "offset": offset}, tag)
    return data.get("_embedded", {}).get(resource, []) or []


def _resume(out_path: Path, ckpt_path: Path) -> int:
    """Lê checkpoint e trunca o JSONL no ponto consistente. Devolve próximo offset."""
    if not ckpt_path.exists():
        return -1
    state = json.loads(ckpt_path.read_text())
    lines_done = int(state["lines"])
    next_offset = int(state["next_offset"])
    if out_path.exists():
        # readline() em vez de iterar o arquivo: f.tell() é proibido durante
        # iteração ("telling position disabled by next() call").
        with out_path.open("r+", encoding="utf-8") as f:
            pos = 0
            for _ in range(lines_done):
                if not f.readline():
                    break
                pos = f.tell()
            f.truncate(pos)
    return next_offset


def run_slice(resource: str, sl: Slice, out_dir: Path) -> int:
    tag = f"s{sl.idx}"
    out_path = out_dir / f"{resource}-{sl.idx}.jsonl"
    ckpt_path = out_dir / f"{resource}-{sl.idx}.ckpt.json"

    offset = _resume(out_path, ckpt_path)
    if offset >= 0:
        log(f"[{tag}] retomando de offset={offset}")
    else:
        offset = sl.start

    written = 0
    if ckpt_path.exists():
        written = int(json.loads(ckpt_path.read_text())["lines"])

    with out_path.open("a", encoding="utf-8") as out:
        while sl.end is None or offset < sl.end:
            limit = PAGE_SIZE
            if sl.end is not None:
                limit = min(PAGE_SIZE, sl.end - offset)
            items = fetch_page(resource, offset, tag)
            if not items:
                log(f"[{tag}] API secou em offset={offset}")
                break
            for it in items[:limit]:
                out.write(json.dumps(it, ensure_ascii=False) + "\n")
            written += min(len(items), limit)
            offset += len(items)
            out.flush()
            os.fsync(out.fileno())
            ckpt_path.write_text(json.dumps({"next_offset": offset, "lines": written}))
            if written % 1000 < PAGE_SIZE:
                log(f"[{tag}] {written} linhas | offset={offset}"
                    + (f"/{sl.end}" if sl.end else ""))
            if len(items) < PAGE_SIZE:
                log(f"[{tag}] página curta ({len(items)}) — fim")
                break

    log(f"[{tag}] FIM {written} linhas, offset final={offset}")
    return written


def get_total(resource: str) -> int:
    _, data = get_json(f"/{resource}", {"limit": 1, "offset": 0}, "total")
    return int(data.get("total", 0))


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--resource", required=True, choices=RESOURCES)
    ap.add_argument("--slices", type=int, default=4, help="conexões paralelas (teto seguro: 4)")
    ap.add_argument("--out", default=None, help="padrão: data/raw/<resource>")
    ap.add_argument("--max-rows", type=int, default=0,
                    help="teto de linhas (0 = tudo). Para teste de vazão, ex. 3000")
    ap.add_argument("--start-offset", type=int, default=0,
                    help="offset inicial. Teste de vazão: use offsets frios — a API "
                         "cacheia por offset e repetir um já buscado mede o cache")
    args = ap.parse_args(argv)

    resource = args.resource
    out_dir = Path(args.out or f"data/raw/{resource}")
    out_dir.mkdir(parents=True, exist_ok=True)

    total = get_total(resource)
    log(f"[{resource}] total na API: {total}")
    base = max(0, args.start_offset)
    span = max(0, total - base)
    capped = 0 < args.max_rows < span
    if capped:
        span = args.max_rows
        log(f"[{resource}] limitado a {span} linhas a partir de {base} (--max-rows)")

    n = max(1, args.slices)
    per = span // n
    last_end = base + span if (capped or base) else None
    slices = [
        Slice(i, base + i * per, base + (i + 1) * per if i < n - 1 else last_end)
        for i in range(n)
    ]
    for s in slices:
        log(f"fatia {s.idx}: {s.start} → {s.end if s.end is not None else 'fim'}")

    results: dict[int, int] = {}
    errors: dict[int, str] = {}

    def worker(s: Slice) -> None:
        try:
            results[s.idx] = run_slice(resource, s, out_dir)
        except Exception as e:  # noqa: BLE001
            errors[s.idx] = f"{type(e).__name__}: {e}"
            log(f"[s{s.idx}] ERRO FATAL {errors[s.idx]}\n{traceback.format_exc()}")

    started = time.time()
    threads = [threading.Thread(target=worker, args=(s,), name=f"s{s.idx}") for s in slices]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    dur = time.time() - started
    rows = sum(results.values())
    log(f"=== TOTAL {rows} linhas em {dur/3600:.2f}h "
        f"({rows / max(dur / 60, 1e-9):.0f} linhas/min)")
    if errors:
        log(f"=== FATIAS COM ERRO: {errors}")
        sys.exit(1)


if __name__ == "__main__":
    main()
