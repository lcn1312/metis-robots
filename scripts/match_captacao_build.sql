-- match_captacao_build.sql
-- Recomputa o match VIVO×PRONTO. Rerunável: TRUNCATE + INSERT em transação.
-- Rodar: docker exec -e PGPASSWORD=... salic_pg psql -U salic -d salic -f -  < scripts/match_captacao_build.sql
-- (ou via cron/GH Actions apontando pro Neon).
--
-- Definições:
--   PROJETO VIVO  = situacao 'Autorizada a captação%' AND prazo aberto AND falta dinheiro
--   INCENTIVADOR PRONTO = doou a algum projeto ano_projeto >= 24 (ativo no ciclo atual)
--
-- Score (0..100), pesos calibráveis:
--   afinidade_uf  35  (gate: candidato compartilha UF preferido do projeto)
--   afinidade_seg 25  (mesmo segmento) | 12 (mesma área) | 0
--   ticket_fit    20  (doação mediana vs gap; 10% do gap = nota cheia)
--   recencia      12  (doou 2025) | 7 (2024)
--   habito         8  (ln(1+nº doações)*3, cap 8)

SET search_path TO metis, public;

BEGIN;

TRUNCATE metis.match_captacao;
TRUNCATE metis.match_captacao_projeto;

-- ── perfil dos incentivadores prontos (revealed preference) ───────────────
CREATE TEMP TABLE _ready ON COMMIT DROP AS
WITH ativos AS (
  SELECT DISTINCT i.cgccpf
  FROM salic.projeto_incentivador i
  JOIN salic.projetos p ON p.pronac = i.pronac
  WHERE p.ano_projeto ~ '^[0-9]+$' AND p.ano_projeto::int >= 24
)
SELECT
  i.cgccpf,
  CASE WHEN length(i.cgccpf) = 14 THEN 'PJ' ELSE 'PF' END        AS tipo_pessoa,
  mode() WITHIN GROUP (ORDER BY p.uf)                            AS pref_uf,
  mode() WITHIN GROUP (ORDER BY p.segmento)                      AS pref_seg,
  mode() WITHIN GROUP (ORDER BY p.area)                          AS pref_area,
  count(DISTINCT i.pronac)                                       AS n_doacoes,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY i.valor)           AS ticket_med,
  max(p.ano_projeto::int)                                        AS ult_ano
FROM salic.projeto_incentivador i
JOIN salic.projetos p ON p.pronac = i.pronac
WHERE i.cgccpf IN (SELECT cgccpf FROM ativos)
  AND p.ano_projeto ~ '^[0-9]+$'
GROUP BY i.cgccpf;

CREATE INDEX ON _ready (pref_uf);

-- ── projetos vivos ────────────────────────────────────────────────────────
CREATE TEMP TABLE _live ON COMMIT DROP AS
SELECT
  pronac, nome, uf, municipio, segmento, area, proponente_nome,
  valor_aprovado,
  COALESCE(valor_captado,0)                              AS valor_captado,
  (valor_aprovado - COALESCE(valor_captado,0))           AS gap,
  data_termino,
  (data_termino - CURRENT_DATE)                          AS dias_restantes
FROM salic.projetos
WHERE situacao LIKE 'Autorizada a captação%'
  AND data_termino >= CURRENT_DATE
  AND COALESCE(valor_captado,0) < valor_aprovado
  AND uf IS NOT NULL;

CREATE INDEX ON _live (uf);

-- ── scoring + rank top-30 por projeto ─────────────────────────────────────
-- Candidato = mesmo UF preferido E (mesmo segmento OU mesma área).
-- Ranqueia com colunas baratas; só enriquece (RFB/nome) APÓS o corte top-30.
CREATE TEMP TABLE _top ON COMMIT DROP AS
WITH cand AS (
  SELECT
    l.pronac, r.cgccpf, r.tipo_pessoa, r.n_doacoes, r.ticket_med, r.ult_ano,
    CASE WHEN r.pref_seg = l.segmento THEN 25.0 ELSE 12.0 END          AS afinidade_seg,
    LEAST(20.0, GREATEST(0.0, r.ticket_med / NULLIF(l.gap,0) * 200.0)) AS ticket_fit,
    CASE WHEN r.ult_ano >= 25 THEN 12.0 ELSE 7.0 END                   AS recencia_pts,
    LEAST(8.0, ln(1 + r.n_doacoes) * 3.0)                              AS habito_pts
  FROM _live l
  JOIN _ready r
    ON r.pref_uf = l.uf
   AND (r.pref_seg = l.segmento OR r.pref_area = l.area)
),
scored AS (
  SELECT *,
    round((35.0 + afinidade_seg + ticket_fit + recencia_pts + habito_pts)::numeric, 2) AS score
  FROM cand
),
ranked AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY pronac
      ORDER BY score DESC, n_doacoes DESC, ticket_med DESC
    ) AS rank
  FROM scored
)
SELECT * FROM ranked WHERE rank <= 30;

INSERT INTO metis.match_captacao (
  pronac, rank, incentivador_cgccpf, incentivador_nome, tipo_pessoa, score,
  afinidade_uf, afinidade_seg, ticket_fit, recencia_pts, habito_pts,
  n_doacoes, ticket_med, ult_ano,
  porte, razao_social, cnae_descricao, uf_incentivador, municipio_incent,
  telefone, email
)
SELECT
  t.pronac, t.rank, t.cgccpf, inc.nome, t.tipo_pessoa, t.score,
  35.0, t.afinidade_seg, t.ticket_fit, t.recencia_pts, t.habito_pts,
  t.n_doacoes, t.ticket_med, t.ult_ano,
  cr.porte, cr.razao_social, cr.cnae_descricao, cr.uf, cr.municipio_descricao,
  inc.telefone, NULLIF(inc.email, '')
FROM _top t
LEFT JOIN salic.incentivadores inc ON inc.cgccpf = t.cgccpf
LEFT JOIN metis.cnpj_receita    cr  ON cr.cnpj   = t.cgccpf;

-- ── cabeçalhos dos projetos vivos (todos, com nº de matches) ──────────────
INSERT INTO metis.match_captacao_projeto (
  pronac, nome, uf, municipio, segmento, area, proponente_nome,
  valor_aprovado, valor_captado, gap, pct_captado, data_termino, dias_restantes, n_matches
)
SELECT
  l.pronac, l.nome, l.uf, l.municipio, l.segmento, l.area, l.proponente_nome,
  l.valor_aprovado, l.valor_captado, l.gap,
  round((100.0 * l.valor_captado / NULLIF(l.valor_aprovado,0))::numeric, 2),
  l.data_termino, l.dias_restantes,
  COALESCE(mc.n, 0)
FROM _live l
LEFT JOIN (
  SELECT pronac, count(*) n FROM metis.match_captacao GROUP BY pronac
) mc ON mc.pronac = l.pronac;

COMMIT;

-- ── verificação ───────────────────────────────────────────────────────────
SELECT 'matches' tabela, count(*) linhas FROM metis.match_captacao
UNION ALL
SELECT 'projetos', count(*) FROM metis.match_captacao_projeto
UNION ALL
SELECT 'projetos_com_match', count(*) FROM metis.match_captacao_projeto WHERE n_matches > 0;
