"""Seleção dos PRONACs "quentes" do modo incremental SALIC.

Quente = projeto com captação (valor_captado > 0) E pelo menos um de:
  * data_termino >= hoje - 18 meses (em execução ou prestando contas);
  * linha alterada no último refresh de `projetos` (updated_at >= início do run).

Prioridade (o orçamento da noite raramente cobre todos):
  0. alterado no último refresh e ainda não rebuscado depois da alteração;
  1. sem vínculo nenhum no banco (nunca teve incentivador/fornecedor);
  2. nunca buscado pelo incremental;
  3. o mais antigo buscado (rodízio).
Empate: maior valor_captado primeiro.

A regra é pura (`select_hot`) para ser testável sem banco; `fetch_candidates`
faz o pré-filtro grosso em SQL, só leitura.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

HOT_MONTHS = 18
MIN_REFETCH_DAYS = 7

TIER_CHANGED = 0
TIER_NO_LINKS = 1
TIER_NEVER_FETCHED = 2
TIER_ROTATION = 3
TIER_NAMES = {
    TIER_CHANGED: "alterado",
    TIER_NO_LINKS: "sem_vinculo",
    TIER_NEVER_FETCHED: "nunca_buscado",
    TIER_ROTATION: "rodizio",
}

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


@dataclass(frozen=True)
class Candidate:
    pronac: str
    data_termino: date | None
    valor_captado: float | None
    updated_at: datetime | None
    has_links: bool


@dataclass(frozen=True)
class Hot:
    pronac: str
    tier: int

    @property
    def reason(self) -> str:
        return TIER_NAMES[self.tier]


def months_ago(today: date, months: int) -> date:
    """`today` menos N meses, com o dia travado no fim do mês se precisar."""
    y, m = divmod(today.year * 12 + (today.month - 1) - months, 12)
    m += 1
    day = today.day
    while True:
        try:
            return date(y, m, day)
        except ValueError:
            day -= 1


def _aware(ts: datetime | None) -> datetime | None:
    if ts is None:
        return None
    return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)


def classify(
    c: Candidate,
    today: date,
    since: datetime | None,
    last_fetched: datetime | None,
    months: int = HOT_MONTHS,
    min_refetch_days: int = MIN_REFETCH_DAYS,
    require_captacao: bool = True,
) -> int | None:
    """Devolve o tier do candidato ou None se ele não entra hoje."""
    if require_captacao and not (c.valor_captado and c.valor_captado > 0):
        return None
    updated = _aware(c.updated_at)
    since = _aware(since)
    fetched = _aware(last_fetched)

    changed = bool(since and updated and updated >= since)
    recent = bool(c.data_termino and c.data_termino >= months_ago(today, months))
    if not (changed or recent):
        return None

    changed_after_fetch = changed and (fetched is None or updated > fetched)
    if changed_after_fetch:
        return TIER_CHANGED
    if fetched is not None:
        now = datetime.combine(today, datetime.min.time(), tzinfo=timezone.utc)
        if now - fetched < timedelta(days=min_refetch_days):
            return None  # buscado há pouco e nada mudou desde então
        return TIER_ROTATION
    if not c.has_links:
        return TIER_NO_LINKS
    return TIER_NEVER_FETCHED


def select_hot(
    candidates: list[Candidate],
    today: date,
    since: datetime | None,
    last_fetched: dict[str, datetime],
    max_pronacs: int | None = None,
    **kw,
) -> list[Hot]:
    ranked: list[tuple[tuple, Hot]] = []
    seen: set[str] = set()
    for c in candidates:
        pronac = c.pronac.strip()
        if not pronac or pronac in seen:
            continue
        seen.add(pronac)
        fetched = _aware(last_fetched.get(pronac))
        tier = classify(c, today, since, fetched, **kw)
        if tier is None:
            continue
        key = (tier, fetched or _EPOCH, -(c.valor_captado or 0), pronac)
        ranked.append((key, Hot(pronac, tier)))
    ranked.sort(key=lambda kv: kv[0])
    out = [h for _, h in ranked]
    return out[:max_pronacs] if max_pronacs else out


# ---------------------------------------------------------------- estado em disco

def load_state(path: Path) -> dict[str, datetime]:
    """{pronac: último fetch aplicado}. Arquivo ausente ou corrompido = vazio."""
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return {}
    out: dict[str, datetime] = {}
    for k, v in raw.items():
        try:
            out[k] = _aware(datetime.fromisoformat(v))
        except (TypeError, ValueError):
            continue
    return out


def save_state(path: Path, state: dict[str, datetime], fetched: dict[str, datetime]) -> None:
    """Grava estado novo (sem mutar o antigo) de forma atômica."""
    merged = {**state, **fetched}
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps({k: v.isoformat() for k, v in sorted(merged.items())}))
    tmp.replace(path)


# ---------------------------------------------------------------- banco (só leitura)

def last_projetos_run(conn) -> datetime | None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT max(started_at) FROM salic.ingest_run "
            "WHERE endpoint = 'projetos' AND status = 'ok'"
        )
        row = cur.fetchone()
    return row[0] if row else None


def fetch_candidates(conn, cutoff: date, since: datetime | None) -> list[Candidate]:
    sql = """
        SELECT p.pronac, p.data_termino, p.valor_captado::float8, p.updated_at,
               EXISTS (SELECT 1 FROM salic.projeto_incentivador pi WHERE pi.pronac = p.pronac)
            OR EXISTS (SELECT 1 FROM salic.projeto_fornecedor pf WHERE pf.pronac = p.pronac)
        FROM salic.projetos p
        WHERE p.valor_captado > 0
          AND (p.data_termino >= %(cutoff)s
               OR (%(since)s::timestamptz IS NOT NULL AND p.updated_at >= %(since)s))
    """
    with conn.cursor() as cur:
        cur.execute(sql, {"cutoff": cutoff, "since": since})
        return [Candidate(r[0], r[1], r[2], r[3], bool(r[4])) for r in cur.fetchall()]


def read_pronacs_file(path: Path) -> list[Hot]:
    """Lista manual (um PRONAC por linha, # comenta). Tudo entra como tier 0."""
    out: list[Hot] = []
    seen: set[str] = set()
    for line in path.read_text().splitlines():
        p = line.split("#", 1)[0].strip()
        if p and p not in seen:
            seen.add(p)
            out.append(Hot(p, TIER_CHANGED))
    return out
