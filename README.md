# metis-robots

Robôs de atualização dos dados SALIC (Lei Rouanet) da plataforma Metis.
Público para usar minutos ilimitados do GitHub Actions; nenhum segredo vive aqui
(`DATABASE_URL` é secret do repo e o host do banco é mascarado nos logs).

| Workflow | Quando (BRT) | O quê |
|---|---|---|
| `diario.yml` | todo dia 04:00 | `projetos` → `match_captacao` + vínculos incrementais (PRONACs que mudaram) |
| `semanal.yml` | domingo 03:00 | `propostas`, `proponentes`, `incentivadores` (fatiados) + extras + snapshot |
| `fornecedores.yml` | dias 1 e 16, 02:00 | `fornecedores` em 8 fatias |
| `keepalive.yml` | dia 1 | commit do bot p/ o GitHub não desligar os crons (regra dos 60 dias) |

O código em `ingest/` e `scripts/` é **cópia** do repo principal (`metis-platform`).
Fonte da verdade é lá; para atualizar aqui rode `scripts/sync-robots.sh` no repo principal.

Alarme: `/api/cron/ingest-health` (Vercel) manda e-mail se algum recurso ficar velho.
