# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Papel do assistente neste projeto

Este é um projeto de **portfólio e aprendizado**. O assistente deve agir como **engenheiro consultor sênior**: fazer perguntas que levem o desenvolvedor a raciocinar, mostrar o caminho da solução e responder dúvidas — **nunca alterar código sem pedido expresso**. Implementar apenas quando o usuário solicitar explicitamente ("implemente", "escreva", "crie" ou equivalente).

## Comandos

O projeto usa **Poetry** para gestão de dependências e **taskipy** como task runner.

```bash
# Instalar dependências (incluindo dev)
poetry install --with dev

# Formatar código
poetry run task lint

# Verificar formatação sem modificar (usado em CI)
poetry run task lint-check

# Scan de segurança (bandit)
poetry run task security

# Lint SQL (sqlfluff — dialeto Athena)
poetry run sqlfluff lint dbt_project/ --dialect ansi
poetry run sqlfluff fix dbt_project/ --dialect ansi

# Ativar pre-commit hooks
poetry run pre-commit install
poetry run pre-commit run --all-files
```

O projeto usa **Astro CLI** para rodar o Airflow localmente.

```bash
# Inicializar projeto Airflow (rodar uma vez dentro de airflow_project/)
astro dev init

# Subir o ambiente Airflow local (Webserver, Scheduler, Postgres, Triggerer)
astro dev start

# Parar o ambiente
astro dev stop

# Ver status dos containers
astro dev ps

# Consultar logs
astro dev logs
```

> `task lint` aponta para `src/` — esse diretório ainda não existe. Será criado na fase de implementação.

## Arquitetura

Pipeline de monitoramento de custos de plataforma de dados (FinOps), inspirado no ambiente de Governança de Dados do iFood. Padrão **medalhão** (Bronze → Silver → Gold):

```
Geração (Python) → S3/Parquet [Bronze]
    → dbt/Athena stg_job_logs [Silver]
    → dbt/Athena int_cost_by_domain
    → dbt/Athena mart_platform_efficiency [Gold]
    → dbt/Athena mart_anomalies (2σ, janela 7 dias via window functions SQL) [Gold]
    → Power BI (via ODBC Simba)
```

**Orquestração:** DAG diária no Airflow 2.8+ via **Astro CLI** (ambiente local gerenciado pelo Astronomer).
**Infra AWS:** S3 (Data Lake), Athena (engine SQL), Glue Data Catalog (schema registry), IAM (permissões mínimas).

### Estrutura de pastas prevista

```
airflow_project/               # projeto Astro CLI (astro dev init)
  dags/                        # DAG do pipeline diário
  plugins/                     # plugins customizados do Airflow
  include/                     # arquivos auxiliares (ex: SQL, configs)
  tests/                       # testes de integridade das DAGs
  requirements.txt             # dependências Python do Airflow
  Dockerfile                   # imagem customizada do Airflow
  .astro/                      # configurações internas do Astro CLI
dbt_project/
  models/
    staging/                   # stg_job_logs.sql
    intermediate/              # int_cost_by_domain.sql
    marts/                     # mart_platform_efficiency.sql, mart_anomalies.sql
  sources.yml
  schema.yml
src/                           # módulos Python reutilizáveis (alvo do task lint)
  ingestion/                   # generate_synthetic_data.py, upload_to_s3.py
  infra/                       # athena_setup.py
.env                           # nunca commitado — ver .env.example
```

### Schema principal (camada raw: `job_logs`)

Campos críticos: `job_id` (UUID), `execution_date`, `domain` (orders/payments/fintech/marketplace/logistics/restaurants), `job_name`, `duration_min`, `dbu_consumed`, `estimated_cost_usd`, `status` (success/failed/timeout), `cluster_type`, `created_at`.

### Lógica de anomalia

`estimated_cost_usd > média_7d + 2 * std_7d` por `(domain, job_name)`. Severidade: `warning` (2–3σ), `critical` (>3σ). Implementada via window functions SQL no model `mart_anomalies.sql`. O script de geração injeta 5% de outliers (multiplicador 3x–8x) para validação.

## Variáveis de ambiente

Todas as credenciais via `.env` (nunca commitado). Ver seção 5.3 do PRD (`docs/food-delivery-cost-monitor-PRD.md`) para a lista completa de variáveis AWS.

Permissões IAM mínimas necessárias: `s3:{PutObject,GetObject,ListBucket}`, `athena:{StartQueryExecution,GetQueryResults}`, `glue:{CreateTable,GetTable,UpdateTable}`.
