"""SALIC API client with retry, backoff, pagination."""
from __future__ import annotations

import os
from typing import Iterator, Any

import httpx
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception,
)


def _should_retry(exc: BaseException) -> bool:
    """Retenta transientes: 5xx, 429, timeout, transport — e TAMBÉM 401/403.
    Empiricamente a SALIC devolve 401/403 TRANSITÓRIOS (WAF/throttle em IP de
    datacenter) que somem na retentativa. Tratá-los como fatais derruba o recurso
    inteiro no offset=0 (incidente 23/06: incentivadores 403 logo na 1ª página,
    enquanto outros endpoints respondiam 200 em paralelo).
    404/400/422 seguem fatais — esses são definitivos (não existe / requisição inválida)."""
    if isinstance(exc, (httpx.TimeoutException, httpx.TransportError)):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        sc = exc.response.status_code
        return sc >= 500 or sc in (401, 403, 429)
    return False

API_BASE = os.getenv("SALIC_API_BASE", "https://api.salic.cultura.gov.br/api/v1")
PAGE_SIZE = int(os.getenv("SALIC_PAGE_SIZE", "100"))
MAX_RETRIES = int(os.getenv("SALIC_MAX_RETRIES", "5"))
TIMEOUT = float(os.getenv("SALIC_TIMEOUT", "60"))
USER_AGENT = os.getenv(
    "SALIC_USER_AGENT", "salic-local-research/0.1"
)


class SalicClient:
    def __init__(self) -> None:
        self._client = httpx.Client(
            base_url=API_BASE,
            timeout=TIMEOUT,
            headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
            follow_redirects=True,
        )

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "SalicClient":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    @retry(
        reraise=True,
        stop=stop_after_attempt(MAX_RETRIES),
        wait=wait_exponential(multiplier=2, min=2, max=60),
        retry=retry_if_exception(_should_retry),
    )
    def _get(self, path: str, params: dict | None = None) -> dict:
        r = self._client.get(path, params=params)
        r.raise_for_status()
        return r.json()

    def paginate(self, resource: str, embed_key: str, start_offset: int = 0) -> Iterator[dict]:
        """Yield items from /<resource>?limit=&offset= using HAL _embedded."""
        offset = start_offset
        while True:
            data = self._get(
                f"/{resource}", params={"limit": PAGE_SIZE, "offset": offset}
            )
            items = data.get("_embedded", {}).get(embed_key, []) or []
            if not items:
                return
            for it in items:
                yield it
            count = data.get("count", len(items))
            total = data.get("total")
            offset += count
            if total is not None and offset >= total:
                return
            if count < PAGE_SIZE:
                return

    def get_total(self, resource: str) -> int:
        data = self._get(f"/{resource}", params={"limit": 1, "offset": 0})
        return int(data.get("total", 0))
