"""Pings no protocolo healthchecks.io — no-op se a URL não estiver configurada.

A URL de cada job vem de `HEALTHCHECK_URL_<JOB>` (ex. HEALTHCHECK_URL_SALIC_INCREMENTAL).
Eventos: "start" -> <url>/start, "success" -> <url>, "fail" -> <url>/fail.

A URL carrega o UUID do check (quem tem a URL consegue pingar), então nunca é
impressa. Falha de rede no ping nunca derruba o job — só vira uma linha de log.
"""
from __future__ import annotations

import os
import re
import sys

_SUFFIX = {"start": "/start", "success": "", "fail": "/fail"}
_MAX_BODY = 10_000  # healthchecks.io guarda até 10KB do corpo


def env_key(job: str) -> str:
    return "HEALTHCHECK_URL_" + re.sub(r"[^A-Z0-9]", "_", job.upper())


def ping_url(job: str, event: str) -> str | None:
    if event not in _SUFFIX:
        raise ValueError(f"evento inválido: {event}")
    base = os.environ.get(env_key(job), "").strip()
    if not base:
        return None
    return base.rstrip("/") + _SUFFIX[event]


def ping(job: str, event: str, body: str = "") -> bool:
    """Devolve True se pingou, False se não configurado ou se o ping falhou."""
    url = ping_url(job, event)
    if url is None:
        return False
    try:
        import httpx

        httpx.post(url, content=body[-_MAX_BODY:].encode("utf-8"), timeout=10)
        return True
    except Exception as e:  # noqa: BLE001 — ping é best-effort
        print(f"[healthcheck] ping {event} de {job} falhou ({type(e).__name__})",
              file=sys.stderr, flush=True)
        return False


if __name__ == "__main__":
    # Uso em shell: python scripts/_healthcheck.py <job> <start|success|fail> [corpo]
    if len(sys.argv) < 3:
        sys.exit("uso: _healthcheck.py <job> <start|success|fail> [corpo]")
    ping(sys.argv[1], sys.argv[2], sys.argv[3] if len(sys.argv) > 3 else "")
