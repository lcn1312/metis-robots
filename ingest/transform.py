"""Pure functions: API record -> row dict for each table."""
from __future__ import annotations

from typing import Any


def _num(v: Any) -> float | None:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _str(v: Any) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def _date(v: Any) -> str | None:
    s = _str(v)
    if not s:
        return None
    return s[:10]


def _hash_from_links(r: dict) -> str | None:
    """Extrai hash SALIC do _links.self (último segmento da URL)."""
    links = r.get("_links")
    if not isinstance(links, dict):
        return None
    self_url = links.get("self")
    if not isinstance(self_url, str) or "/" not in self_url:
        return None
    return self_url.rstrip("/").rsplit("/", 1)[-1] or None


def proponente_row(r: dict) -> dict:
    return {
        "cgccpf": _str(r.get("cgccpf")),
        "nome": _str(r.get("nome")),
        "tipo_pessoa": _str(r.get("tipo_pessoa")),
        "responsavel": _str(r.get("responsavel")),
        "uf": _str(r.get("UF")),
        "municipio": _str(r.get("municipio")),
        "total_captado": _num(r.get("total_captado")),
        "salic_hash": _hash_from_links(r),
    }


def incentivador_row(r: dict) -> dict:
    return {
        "cgccpf": _str(r.get("cgccpf")),
        "nome": _str(r.get("nome")),
        "tipo_pessoa": _str(r.get("tipo_pessoa")),
        "responsavel": _str(r.get("responsavel")),
        "uf": _str(r.get("UF")),
        "municipio": _str(r.get("municipio")),
        "total_doado": _num(r.get("total_doado")),
        "salic_hash": _hash_from_links(r),
    }


def fornecedor_row(r: dict) -> dict:
    return {
        "cgccpf": _str(r.get("cgccpf")),
        "nome": _str(r.get("nome")),
        "email": _str(r.get("email")),
        "salic_hash": _hash_from_links(r),
    }


def proposta_row(r: dict) -> dict:
    return {
        "id": int(r["id"]) if r.get("id") is not None else None,
        "nome": _str(r.get("nome")),
        "mecanismo": _str(r.get("mecanismo")),
        "data_inicio": _date(r.get("data_inicio")),
        "data_termino": _date(r.get("data_termino")),
        "data_aceite": _str(r.get("data_aceite")),
        "data_arquivamento": _str(r.get("data_arquivamento")),
        "objetivos": _str(r.get("objetivos")),
        "justificativa": _str(r.get("justificativa")),
        "resumo": _str(r.get("resumo")),
        "sinopse": _str(r.get("sinopse")),
        "etapa": _str(r.get("etapa")),
        "ficha_tecnica": _str(r.get("ficha_tecnica")),
        "estrategia_execucao": _str(r.get("estrategia_execucao")),
        "especificacao_tecnica": _str(r.get("especificacao_tecnica")),
        "acessibilidade": _str(r.get("acessibilidade")),
        "democratizacao": _str(r.get("democratizacao")),
        "impacto_ambiental": _str(r.get("impacto_ambiental")),
    }


def projeto_row(r: dict) -> dict:
    return {
        "pronac": _str(r.get("PRONAC")),
        "nome": _str(r.get("nome")),
        "ano_projeto": _str(r.get("ano_projeto")),
        "situacao": _str(r.get("situacao")),
        "providencia": _str(r.get("providencia")),
        "mecanismo": _str(r.get("mecanisnmo") or r.get("mecanismo")),
        "enquadramento": _str(r.get("enquadradmento") or r.get("enquadramento")),
        "tipicidade": _str(r.get("tipicidade")),
        "tipologia": _str(r.get("tipologia")),
        "segmento": _str(r.get("segmento")),
        "area": _str(r.get("area")),
        "cgccpf_proponente": _str(r.get("cgccpf")),
        "proponente_nome": _str(r.get("proponente")),
        "uf": _str(r.get("UF")),
        "municipio": _str(r.get("municipio")),
        "data_inicio": _date(r.get("data_inicio")),
        "data_termino": _date(r.get("data_termino")),
        "valor_solicitado": _num(r.get("valor_solicitado")),
        "valor_proposta": _num(r.get("valor_proposta")),
        "valor_projeto": _num(r.get("valor_projeto")),
        "valor_aprovado": _num(r.get("valor_aprovado")),
        "valor_captado": _num(r.get("valor_captado")),
        "outras_fontes": _num(r.get("outras_fontes")),
        "objetivos": _str(r.get("objetivos")),
        "justificativa": _str(r.get("justificativa")),
        "resumo": _str(r.get("resumo")),
        "sinopse": _str(r.get("sinopse")),
        "etapa": _str(r.get("etapa")),
        "ficha_tecnica": _str(r.get("ficha_tecnica")),
        "estrategia_execucao": _str(r.get("estrategia_execucao")),
        "especificacao_tecnica": _str(r.get("especificacao_tecnica")),
        "acessibilidade": _str(r.get("acessibilidade")),
        "democratizacao": _str(r.get("democratizacao")),
        "impacto_ambiental": _str(r.get("impacto_ambiental")),
    }


def projeto_locais(r: dict) -> list[dict]:
    out = []
    for loc in r.get("local_realizacao") or []:
        out.append(
            {
                "municipio_ibge": _str(loc.get("municipio_ibge")) or "",
                "municipio": _str(loc.get("municipio")) or "",
                "uf": _str(loc.get("UF") or loc.get("uf")) or "",
            }
        )
    return out
