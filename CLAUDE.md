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
poetry run sqlfluff lint airflow_project/dbt_food_cost_monitor/ --dialect ansi
poetry run sqlfluff fix airflow_project/dbt_food_cost_monitor/ --dialect ansi

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

> `task lint`/`lint-check`/`security` apontam para `airflow_project/pipeline/`.

## Arquitetura

Pipeline de monitoramento de custos de plataforma de dados (FinOps), inspirado no ambiente de Governança de Dados do iFood. Padrão **medalhão** (Bronze → Silver → Gold):

```
Geração (Python) → S3/Parquet → Glue job_logs [raw]
    → dbt/Athena stg_job_logs         [cost_monitor_bronze — view]
    → dbt/Athena int_cost_by_domain   [cost_monitor_silver — table]
    → dbt/Athena mart_platform_efficiency  [cost_monitor_gold — table]
    → dbt/Athena mart_anomalies (2σ, janela 7 dias via window functions SQL) [cost_monitor_gold]
    → Power BI (Import via ODBC Simba, só a camada gold)
```

**Schemas por camada:** definidos em `dbt_project.yml` via `+schema: bronze|silver|gold`, que o dbt aplica como **sufixo** do `DBT_SCHEMA` (`cost_monitor`) — daí `cost_monitor_bronze` etc. A staging fica em `bronze` (e não em `silver`) porque é só uma view de rename/tipagem; o primeiro artefato analítico é o `int_cost_by_domain`.

**Grafo real dos models:** `mart_anomalies` lê de `stg_job_logs`, **não** de `int_cost_by_domain` nem de `mart_platform_efficiency` — precisa da granularidade de job individual, que a agregação por domínio já perdeu.

**Orquestração:** DAG diária `food_delivery_cost_monitor` (`dags/pipeline_daily.py`) no Airflow via **Astro CLI**. Usa a API `airflow.sdk` (Airflow 3.x) com decorators `@dag`/`@task`. Tasks: `cria_infraestrutura` → `gerador_dados` → `upload_s3` → `dbt_pipeline` (Cosmos `DbtTaskGroup`).
**Infra AWS:** S3 (Data Lake), Athena (engine SQL), Glue Data Catalog (schema registry), IAM (permissões mínimas).

### Convenção de nomenclatura das colunas

A raw preserva os nomes em inglês. **A partir de `stg_job_logs`, todas as colunas são renomeadas para português com prefixo de tipo** — ao escrever ou revisar SQL neste projeto, seguir sempre esta convenção:

| Prefixo | Tipo | Exemplo |
|---|---|---|
| `id_` | Identificador | `id_job` |
| `dt_` | Data / timestamp | `dt_execucao`, `dt_criacao` |
| `nm_` | Nome / texto categórico | `nm_dominio`, `nm_job`, `nm_severidade` |
| `vl_` | Valor monetário | `vl_estimativa_custo_usd`, `vl_total_custo_usd` |
| `nr_` | Número (contagem, duração, score) | `nr_total_jobs`, `nr_sigma_desvio` |
| `st_` | Status | `st_status` |
| `tp_` | Tipo | `tp_cluster` |

Nas medidas DAX do Power BI a convenção equivalente é `Vl.`, `Nr.`, `Pc.` e `Ds.`.

### Estrutura de pastas

`pipeline/` e o projeto dbt vivem dentro de `airflow_project/` (não na raiz do repo) para que o Dockerfile copie tudo direto do build context do Astro CLI, sem pasta `include/` de sincronização. A decisão sobre a integração Cosmos + venv dedicada está detalhada na **seção 8.3 do PRD** (`docs/food-delivery-cost-monitor-PRD.md`).

```
airflow_project/                 # projeto Astro CLI (astro dev init) — build context único
  dags/
    pipeline_daily.py            # DAG food_delivery_cost_monitor — implementada e funcional
  plugins/                       # plugins customizados do Airflow
  pipeline/                      # módulos Python reutilizáveis (alvo do task lint/lint-check/security)
    pipeline_config.py           # env vars + clientes boto3 (S3/Glue) + constantes de path
    ingestion/                   # generate_synthetic_data.py, upload_to_s3.py
    infra/                       # athena_setup.py, logi.py (logger compartilhado)
  dbt_food_cost_monitor/         # projeto dbt
    models/
      staging/                   # stg_job_logs.sql + _schema.yml (contract enforced) + _sources.yml
      intermediate/              # int_cost_by_domain.sql
      marts/                     # mart_platform_efficiency.sql, mart_anomalies.sql
    dbt_project.yml              # materializações e +schema por camada
    profiles.yml                 # credenciais via env_var (Plano A: ProfileMapping do Cosmos segue comentado na DAG)
  tests/                         # testes de integridade das DAGs (hoje só test_dag_example.py)
  requirements.txt               # dependências Python do Airflow (astronomer-cosmos)
  Dockerfile                     # imagem customizada do Airflow (venv dbt dedicada)
  airflow_settings.yaml          # Connections/Variables de dev local (gitignorado)
  .astro/                        # configurações internas do Astro CLI
  .env                           # ⚠️ é AQUI que o .env vive (não na raiz) — gitignorado
dashboard/                       # projeto Power BI (.pbip): Report + SemanticModel em TMDL
  assets/                        # prints do dashboard, logos, paleta
docs/
  food-delivery-cost-monitor-PRD.md
  dashboard.md                   # modelo semântico, medidas DAX, visuais
.env.example                     # template das variáveis (copiar para airflow_project/.env)
```

> **`.env` fica em `airflow_project/`**, não na raiz: é de lá que o Astro CLI injeta as variáveis nos containers, e o `load_dotenv()` do `pipeline_config.py` também as encontra ao rodar os scripts na mão. `S3_BUCKET` é lida sem default — se faltar, o import de `pipeline_config` levanta `ValueError`.

### Schema principal (camada raw: `job_logs`)

Campos críticos: `job_id` (UUID), `execution_date`, `domain` (orders/payments/fintech/marketplace/logistics/restaurants), `job_name`, `duration_min`, `dbu_consumed`, `estimated_cost_usd`, `status` (success/failed/timeout), `cluster_type`, `created_at`.

### Lógica de anomalia

`vl_estimativa_custo_usd > vl_medio_7d + 2 * vl_desvio_7d` por `(nm_dominio, nm_job)`. Severidade: `warning` (2–2.4σ), `critical` (**>2.4σ**, não 3σ — corte calibrado sobre os outliers do gerador, já que o `STDDEV_POP` de uma janela de 7 pontos é inflado pelo próprio outlier avaliado). Implementada via window functions SQL no model `mart_anomalies.sql`, que retorna **apenas** as linhas anômalas. O script de geração injeta 5% de outliers (multiplicador 3x–8x) para validação.

### Camada de visualização

Dashboard Power BI publicado (`dashboard/`), consumindo **só o schema `cost_monitor_gold`** em modo **Import** (não DirectQuery — evita cobrança de scan do Athena por interação). Modelo em estrela com `dim_calendario` ligada a `dt_execucao` dos dois marts. Detalhes em `docs/dashboard.md`.

## Variáveis de ambiente

Todas as credenciais via `airflow_project/.env` (nunca commitado). Ver seção 5.3 do PRD (`docs/food-delivery-cost-monitor-PRD.md`) para a lista completa de variáveis AWS.

`ATHENA_OUTPUT_LOCATION` **não** é variável de ambiente — é derivada em `pipeline_config.py` como `s3://{S3_BUCKET}/athena-results/`.

Permissões IAM mínimas necessárias: `s3:{PutObject,GetObject,ListBucket}`, `athena:{StartQueryExecution,GetQueryResults}`, `glue:{CreateTable,GetTable,UpdateTable}`.
