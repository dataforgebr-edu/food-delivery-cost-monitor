# food-delivery-cost-monitor

**Pipeline de Monitoramento de Custos de Plataforma de Dados**

> Documento de Requisitos do Produto (PRD)

---

**Campo** | **Valor**
--- | ---
Versão | 1.1
Data | Julho 2026 (v1.0: Maio 2026)
Status | **Entregue** — pipeline funcional de ponta a ponta, dashboard publicado
Contexto | Projeto portfolio — inspirado no ambiente de Governança de Dados do iFood
Stack principal | Python, SQL, dbt, Airflow, Astronomer Cosmos, AWS S3, AWS Athena, Docker, Power BI
Dashboard | [Link público ao vivo](https://app.powerbi.com/view?r=eyJrIjoiMWU2N2VkZDktOTg3MC00NTA2LTljYTgtYjUyNWU3OTNiMzZiIiwidCI6IjJiZDE5YWQ4LTcyZTUtNGY2ZC1hZmY1LWRhOTMwMTdmZGYxYiJ9)

> **Changelog v1.1 (Julho 2026)** — atualização do documento para refletir o que foi efetivamente construído. Mudanças em relação ao planejado na v1.0:
>
> - **§3** — estrutura de pastas reescrita: `pipeline/` e o projeto dbt passaram para dentro de `airflow_project/`; `docker-compose.yml` foi substituído pelo Astro CLI
> - **§4** — documentada a convenção de nomenclatura das colunas em português com prefixo de tipo (nova §4.1.1), adotada a partir da staging
> - **§4.2** — `execution_week` (DATE_TRUNC) foi implementado como `nr_semana` (número inteiro da semana); a staging ganhou contrato dbt
> - **§5.3** — `.env` mora em `airflow_project/`, não na raiz; `ATHENA_OUTPUT_LOCATION` deixou de ser variável de ambiente
> - **§7.1** — o corte de severidade `critical` ficou em **2.4σ**, não 3σ
> - **§9** — os KPIs e visuais construídos divergem dos planejados; seção reescrita com o dashboard real


# 1. Visão Geral
Este projeto simula o trabalho de um engenheiro de dados no time de Governança de Dados de uma plataforma de delivery em larga escala. O objetivo é construir um pipeline completo que coleta, transforma, analisa e visualiza dados de custo e utilização de recursos de dados — replicando o tipo de solução que times de FinOps e Eficiência de Plataforma operam diariamente.

*Contexto de negócio: uma plataforma de delivery processa centenas de milhões de pedidos por mês, distribuídos em múltiplos domínios de dados (pedidos, restaurantes, pagamentos, fintech, logistica). Cada domínio executa dezenas de jobs de processamento diariamente — e o custo total pode variar drasticamente sem visibilidade adequada.*

## 1.1 Problema que o projeto resolve
Sem monitoramento centralizado, times de engenharia não sabem:

- Quais domínios estão consumindo mais recursos e custo
- Quais jobs apresentaram crescimento anômalo de custo
- Qual e a tendência de custo ao longo do tempo por domínio
- Onde estão as maiores oportunidades de otimização

## 1.2 Objetivo do projeto
Construir um pipeline de dados end-to-end que:

1. Ingere logs simulados de execução de jobs de processamento de dados
2. Armazena os dados brutos em um Data Lake na AWS (S3)
3. Transforma e modela os dados com dbt usando AWS Athena como engine de query
4. Detecta anomalias de custo em SQL, via window functions no model `mart_anomalies` (desvio padrão sobre janela móvel de 7 dias)
5. Orquestra todo o fluxo com Apache Airflow, integrando o grafo do dbt via Astronomer Cosmos
6. Expõe métricas em um dashboard interativo no Power BI

# 2. Arquitetura do Pipeline
O pipeline segue a arquitetura medalhão (Bronze / Silver / Gold), adaptada para o contexto de monitoramento de custos:

**Camada**

**Tecnologia**

**Descrição**

Ingestão (Bronze)

Python \+ AWS S3

Geração de dados sintéticos e upload em formato Parquet particionado por data e domínio

Transformação (Silver)

dbt \+ AWS Athena

Staging models: limpeza, tipagem e padronização dos logs brutos

Agregação (Gold)

dbt \+ AWS Athena

Marts: métricas agregadas por domínio, job e período — prontas para consumo

Detecção de anomalias

dbt \+ AWS Athena

mart_anomalies: window functions SQL calculam desvio padrão por (domain, job_name) sobre janela de 7 dias, integrado ao pipeline dbt

Orquestração

Apache Airflow \+ Astronomer Cosmos

DAG diária com dependências entre tarefas, retry e alertas de falha. Cosmos converte o grafo de dependências do dbt (staging \> intermediate \> marts \> tests) em um TaskGroup nativo do Airflow, com uma task por model

Visualização

Power BI

Dashboard com KPIs de custo, tendências e lista de anomalias detectadas

Infra local

Astro CLI (Astronomer)

Airflow local gerenciado pelo Astro CLI (`astro dev start`), com imagem customizada via Dockerfile; Power BI Desktop instalado na máquina. **Substituiu o Docker Compose planejado na v1.0**

## 2.1 Fluxo de dados detalhado
- O script generate_synthetic_data.py cria N registros de logs para cada data do período configurado
- Os logs sao salvos em formato Parquet e enviados para S3 no caminho `s3://{S3_BUCKET}/raw/job_logs/date=YYYY-MM-DD/data.parquet`
- O AWS Glue Data Catalog registra a tabela externa apontando para o S3
- O dbt executa os models respeitando o grafo de dependências: `stg_job_logs` → `int_cost_by_domain` → `mart_platform_efficiency`, e `stg_job_logs` → `mart_anomalies`
- **`mart_anomalies` lê de `stg_job_logs`**, e não de `mart_platform_efficiency`: a detecção precisa da granularidade de job individual (`nm_job`), que a agregação por domínio já teria perdido. As window functions rodam em SQL, sem script Python adicional
- Cada camada é materializada em um schema Athena próprio: `cost_monitor_bronze` (staging, view), `cost_monitor_silver` (intermediate, table) e `cost_monitor_gold` (marts, table)
- O Power BI conecta-se ao Athena via ODBC (driver Simba) em modo **Import**, consumindo apenas o schema `cost_monitor_gold`

## 2.2 Diagrama de dependências da DAG
*cria_infraestrutura >> gerador_dados >> upload_s3 >> dbt_pipeline (Cosmos DbtTaskGroup: stg_job_logs >> int_cost_by_domain >> mart_platform_efficiency, e stg_job_logs >> mart_anomalies, + testes dbt inline) >> termino_pipeline*

> Implementado em `airflow_project/dags/pipeline_daily.py` com a API `airflow.sdk` (decorators `@dag`/`@task`). A task `cria_infraestrutura` — que roda o `athena_setup.py` de forma idempotente — não estava prevista na v1.0.

# 3. Estrutura de Pastas

> **Reescrita na v1.1.** A estrutura planejada na v1.0 (com `dags/`, `ingestion/`, `infra/` e `dbt_project/` na raiz do repositório) foi substituída: **`pipeline/` e o projeto dbt passaram para dentro de `airflow_project/`**, que é o projeto do Astro CLI. O motivo é que o `Dockerfile` do Astro copia arquivos a partir do próprio diretório do projeto — mantendo tudo ali dentro, a imagem é construída direto do build context, sem precisar de uma pasta `include/` sincronizando cópias do código.

**Caminho**

**Descrição**

airflow_project/dags/pipeline_daily.py

DAG principal do Airflow (`food_delivery_cost_monitor`) — orquestra todo o pipeline diario; integra os models dbt via Astronomer Cosmos (DbtTaskGroup)

airflow_project/pipeline/pipeline_config.py

Carrega as variáveis de ambiente, instancia os clientes boto3 (S3/Glue) e centraliza as constantes de path do Data Lake

airflow_project/pipeline/ingestion/generate_synthetic_data.py

Gera logs sintéticos com sazonalidade e outliers propositais

airflow_project/pipeline/ingestion/upload_to_s3.py

Faz upload incremental dos Parquets para o S3 com particionamento por data

airflow_project/pipeline/infra/athena_setup.py

Cria o database e a tabela externa no AWS Glue/Athena

airflow_project/pipeline/infra/logi.py

Logger compartilhado pelos módulos do pipeline

airflow_project/dbt_food_cost_monitor/models/staging/stg_job_logs.sql

Renomeação, tipagem e filtro de qualidade dos dados brutos

airflow_project/dbt_food_cost_monitor/models/staging/_sources.yml

Definição das fontes de dados (tabela raw no Athena) + testes de freshness

airflow_project/dbt_food_cost_monitor/models/staging/_schema.yml

Contrato (`contract: enforced`), documentação e testes da staging

airflow_project/dbt_food_cost_monitor/models/intermediate/int_cost_by_domain.sql

Agregação intermediária por domínio e dia

airflow_project/dbt_food_cost_monitor/models/marts/mart_platform_efficiency.sql

Tabela final com KPIs de eficiência

airflow_project/dbt_food_cost_monitor/models/marts/mart_anomalies.sql

Tabela de alertas: detecta jobs com custo acima de 2σ da média móvel de 7 dias via window functions SQL

airflow_project/dbt_food_cost_monitor/dbt_project.yml

Materialização e schema (`+schema: bronze|silver|gold`) de cada camada

airflow_project/dbt_food_cost_monitor/profiles.yml

Credenciais do adapter Athena, resolvidas via `env_var()`

airflow_project/Dockerfile

Imagem customizada do Airflow, com a venv dedicada do dbt instalada em build time

airflow_project/requirements.txt

Dependências Python do ambiente Airflow (astronomer-cosmos)

airflow_project/tests/

Testes de integridade das DAGs

airflow_project/.env

Variáveis de ambiente — **é aqui que o arquivo vive**, não na raiz (ver §5.3). Gitignorado

dashboard/

Projeto Power BI (`.pbip`): definição dos visuais (`.Report`) e do modelo semântico em TMDL (`.SemanticModel`)

dashboard/assets/

Prints do dashboard, logos e paleta de cores

docs/dashboard.md

Documentação da camada de visualização: modelo semântico, medidas DAX e visuais

pyproject.toml

Dependências e tasks do projeto Python (Poetry + taskipy)

.env.example

Template de variáveis de ambiente (AWS keys, bucket, region)

README.md

Documentação completa: setup, arquitetura, como rodar e prints do dashboard

# 4. Schema dos Dados
## 4.1 Dados sintéticos — job_logs (camada raw)
O script de geração simula logs de execução de jobs Spark/Databricks em 6 domínios de negocio, com sazonalidade diária (picos no almoco e jantar) e 5% de outliers de custo para alimentar o detector de anomalias.

**Campo**

**Tipo**

**Descrição**

**Exemplo**

job_id

STRING

Identificador único do job (UUID)

job_a1b2c3d4

execution_date

DATE

Data de execução

2026-05-01

domain

STRING

Domínio de negocio

orders, payments, fintech...

job_name

STRING

Nome do job de processamento

process_order_events

duration_min

FLOAT

Duracao da execução em minutos

12.5

dbu_consumed

FLOAT

Databricks Units consumidas (simulado)

4.2

estimated_cost_usd

FLOAT

Custo estimado em USD

0.84

status

STRING

Status de execução

success, failed, timeout

cluster_type

STRING

Tipo de cluster simulado

job_cluster, all_purpose

created_at

TIMESTAMP

Timestamp de criação do registro

2026-05-01 12:34:56

## 4.1.1 Convenção de nomenclatura das colunas (v1.1)
A tabela raw preserva os nomes originais em inglês — é a fonte, e não deve ser reescrita. **A partir da staging, todas as colunas são renomeadas para português com um prefixo que indica o tipo do dado.** A convenção deixa o tipo explícito na leitura do SQL, sem exigir consulta ao schema:

**Prefixo** | **Tipo** | **Exemplo**
--- | --- | ---
`id_` | Identificador | `id_job`
`dt_` | Data / timestamp | `dt_execucao`, `dt_criacao`
`nm_` | Nome / texto categórico | `nm_dominio`, `nm_job`, `nm_severidade`
`vl_` | Valor monetário | `vl_estimativa_custo_usd`, `vl_total_custo_usd`
`nr_` | Número (contagem, duração, score) | `nr_total_jobs`, `nr_duracao_minutos`, `nr_sigma_desvio`
`st_` | Status | `st_status`
`tp_` | Tipo | `tp_cluster`

A mesma convenção é reaproveitada nas medidas DAX do Power BI, com os prefixos `Vl.`, `Nr.`, `Pc.` (percentual) e `Ds.` (descrição/texto) — ver `docs/dashboard.md`.

## 4.2 Staging — stg_job_logs
Renomeação, tipagem e padronização dos dados brutos. Materializada como **view** no schema `cost_monitor_bronze`. Inclui filtro de qualidade e uma coluna derivada calculável linha a linha.

- Todos os campos do raw, renomeados conforme §4.1.1 e tipados com `CAST` + macros `dbt.type_*` (portabilidade entre adapters)
- Filtro `WHERE vl_estimativa_custo_usd > 0` — descarta custo negativo ou zerado
- `nr_semana`: `CAST(week(dt_execucao) AS INT)` — número inteiro da semana, derivado linha a linha
- **Contrato dbt** (`contract: enforced`) no `_schema.yml`: o build falha se qualquer coluna divergir do tipo declarado

> **Divergência vs. v1.0:** o planejado era `execution_week` via `DATE_TRUNC('week', execution_date)`, resultando em uma DATE. O implementado é `nr_semana`, o número inteiro da semana via `week()`. A escolha simplifica o agrupamento por semana no Power BI, mas perde a informação do ano — semanas de anos diferentes colidem. Como o dashboard hoje agrupa por data usando a `dim_calendario`, a limitação não é exercida; se `nr_semana` passar a ser usada em análises multi-ano, deve virar `dt_semana` (DATE_TRUNC) ou ganhar o ano na chave.

> **Nota arquitetural:** `cost_category` (classificação por quartil via NTILE) e `is_anomaly_candidate` (flag acima do percentil 95 do dia) foram removidos desta camada. Ambos exigem window functions de ranking ou agregação sobre múltiplos registros, o que viola o contrato da staging. `cost_category` pertence ao `int_cost_by_domain` ou `mart_platform_efficiency`; `is_anomaly_candidate` pertence ao `mart_anomalies`, após as estatísticas do dia já terem sido calculadas.

> **Sobre a validação de `status`:** a staging **não filtra** status inválido, ao contrário do que a v1.0 previa. A validação é feita pelo teste `accepted_values` (`success`, `failed`, `timeout`) declarado no `_schema.yml`. A diferença é intencional: um status inesperado deve quebrar o build e exigir investigação, não ser silenciosamente descartado.

## 4.3 Intermédiate — int_cost_by_domain
Agregação diaria por domínio, consumida pelos models finais e pelo detector de anomalias.

**Campo**

**Tipo**

**Descrição**

execution_date

DATE

Data de referência

domain

STRING

Domínio de negocio

total_jobs

INTEGER

Total de jobs executados no dia

successful_jobs

INTEGER

Jobs com status success

total_cost_usd

FLOAT

Custo total do domínio no dia

avg_cost_per_job

FLOAT

Custo medio por job

max_cost_job

FLOAT

Custo do job mais caro do dia

total_duration_min

FLOAT

Soma de duração de todos os jobs

avg_duration_min

FLOAT

Duracao média por job

success_rate

FLOAT

Taxa de sucesso (0 a 1)

## 4.4 Mart — mart_platform_efficiency
Tabela final com KPIs de eficiência da plataforma. Consumida diretamente pelo Power BI e pelo script de anomalias.

- Todos os campos do intermédiate
- cost_7d_avg: média movel de custo dos ultimos 7 dias (para o detector de anomalias)
- cost_7d_std: desvio padrão dos ultimos 7 dias
- cost_mtd: custo acumulado do mes (Month-to-Date)
- efficiency_score: score de 0 a 100 calculado a partir de taxa de sucesso, custo medio e tendencia
- cost_trend_pct: variacao percentual de custo em relacao ao dia anterior

## 4.5 Mart — mart_anomalies
**Campo**

**Tipo**

**Descrição**

job_id

STRING

Job com anomalia detectada

detected_at

TIMESTAMP

Momento da detecção

execution_date

DATE

Data do job anomalo

domain

STRING

Domínio do job

job_name

STRING

Nome do job

cost_observed

FLOAT

Custo real observado (USD)

cost_expected_avg

FLOAT

Média dos ultimos 7 dias

cost_expected_upper

FLOAT

Limite superior (média \+ 2 * std)

sigma_deviation

FLOAT

Quantos sigmas acima da média

severity

STRING

warning (2-3σ), critical (>3σ)

# 5. Requisitos Técnicos
## 5.1 Infra e dependências
**Componente**

**Versão**

**Justificativa**

Python

3.12.10

Compatibilidade com Airflow 2.x, pandas 2.x e tipagem moderna

Apache Airflow

2.8\+

Versão com TaskFlow API e melhor suporte a DockerOperator

dbt-athena-community

1.7\+

Adapter oficial do dbt para AWS Athena

AWS CLI

2.x

Configuração de credenciais e acesso ao S3/Athena

Astro CLI \+ Docker Desktop

Astro CLI 1.x

Orquestração local dos serviços via `astro dev start`, com imagem customizada (Dockerfile). **Substituiu o Docker Compose planejado na v1.0**

Power BI

Desktop 2.x / Service

Conexão ao Athena via ODBC (driver Simba); publicação no Power BI Service

boto3

1.34\+

SDK Python para AWS (S3, Athena, Glue)

pandas

2.x

Geração de dados e detecção de anomalias

pyarrow

14\+

Escrita de arquivos Parquet otimizados

## 5.2 Configuração AWS
O projeto usa os seguintes serviços AWS, todos disponíveis no Free Tier para volume de dados de desenvolvimento:

**Servico**

**Uso no projeto**

**Custo estimado (dev)**

Amazon S3

Data Lake — armazenamento de Parquets particionados

< $0.01/mes (< 1 GB)

AWS Athena

Engine de query SQL sobre os Parquets no S3

< $0.01/mes (< 10 GB scanned)

AWS Glue Data Catalog

Registro do schema da tabela externa

Gratuito ate 1M objetos

IAM

Usuario com permissões minimas (S3 \+ Athena \+ Glue)

Gratuito

*Permissoes IAM necessarias: s3:PutObject, s3:GetObject, s3:ListBucket, athena:StartQueryExecution, athena:GetQueryResults, glue:CreateTable, glue:GetTable, glue:UpdateTable.*

## 5.3 Variáveis de ambiente
Todas as credenciais e configurações sensíveis sao gerenciadas via arquivo `.env` (nunca commitado no repositorio).

> **Localização (v1.1):** o `.env` vive em **`airflow_project/.env`**, não na raiz do repositório. É de lá que o Astro CLI carrega as variáveis e as injeta nos containers do Airflow; e como o `pipeline_config.py` usa `load_dotenv()` — que procura o arquivo subindo a partir do diretório atual — o mesmo arquivo atende também a execução manual dos scripts em `airflow_project/pipeline/`. O template `.env.example` continua na raiz.

Variáveis lidas pelo `pipeline_config.py`:

*AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_REGION=us-east-1
S3_BUCKET=food-delivery-cost-monitor
ATHENA_DATABASE=cost_monitor*

Variáveis lidas pelo `profiles.yml` do dbt via `env_var()` — a ausência de qualquer uma delas faz o `dbt run` falhar no parse:

*DBT_DATABASE=awsdatacatalog
DBT_REGION_NAME=us-east-1
DBT_S3_DATA_DIR=
DBT_STAGING_DIR=
DBT_SCHEMA=cost_monitor
DBT_THREADS=4
DBT_TYPE=athena*

> **Correção vs. v1.0:** `ATHENA_OUTPUT_LOCATION` **não é** variável de ambiente. Ela é derivada em código, em `pipeline_config.py`, como `s3://{S3_BUCKET}/athena-results/`. Já `S3_BUCKET` é lida **sem valor default** — se faltar, o simples import do módulo levanta `ValueError`.

# 6. Models dbt — Detalhamento
## 6.1 sources.yml
Define a tabela raw do S3/Athena como fonte de dados para o dbt. Inclui testes de qualidade básicos: not_null nos campos criticos, accepted_values para status e domain.

## 6.2 stg_job_logs.sql
- SELECT com CAST explicito em todos os tipos, via macros `dbt.type_string()`, `dbt.type_float()`, `dbt.type_timestamp()`
- Renomeação de todas as colunas conforme a convenção da §4.1.1
- `WHERE vl_estimativa_custo_usd > 0` — remove custos negativos e zerados
- `CAST(week(dt_execucao) AS INT)` para `nr_semana`
- Estruturado em CTEs nomeadas por etapa (`change_cast_types` → `filter_negative_cost` → `create_new_columns`)

> A validação de `status` ficou por conta do teste `accepted_values`, não de um `WHERE` — ver nota em 4.2.

> `cost_category` e `is_anomaly_candidate` foram movidos para camadas posteriores — ver nota em 4.2.

## 6.3 int_cost_by_domain.sql
- GROUP BY execution_date, domain
- COUNT(*), COUNT(CASE WHEN status = 'success'), SUM, AVG, MAX
- CAST(successful_jobs AS DOUBLE) / total_jobs AS success_rate

## 6.4 mart_platform_efficiency.sql
- JOIN com subquery de janela móvel: AVG e STDDEV_POP sobre os ultimos 7 dias via window function ou self-join (Athena suporta window functions)
- SUM(...) OVER (PARTITION BY domain, DATE_TRUNC('month', execution_date)) para cost_mtd
- Formula do efficiency_score: (success_rate * 40) \+ (cost_score * 40) \+ (trend_score * 20)

## 6.5 Testes dbt (schema.yml)
- not_null: execution_date, domain, total_cost_usd
- unique: chave composta execution_date \+ domain
- accepted_values: domain in (orders, restaurants, payments, fintech, marketplace, logistics)
- dbt_utils.expression_is_true: total_cost_usd >= 0, success_rate between 0 and 1

# 7. Detecção de Anomalias
## 7.1 Lógica
A detecção de anomalias é implementada diretamente em SQL no model dbt `mart_anomalies.sql` — sem script Python adicional. Essa decisão mantém toda a lógica de transformação em uma única camada declarativa e testável.

1. Lê de **`stg_job_logs`** — e não de `mart_platform_efficiency`, como previa a v1.0. A detecção exige granularidade de job individual (`nm_job`), que a agregação por domínio do `int_cost_by_domain` já teria perdido
2. Aplica window functions particionadas por `(nm_dominio, nm_job)`: `AVG` e `STDDEV_POP` sobre os últimos 7 dias com `ROWS BETWEEN 6 PRECEDING AND CURRENT ROW`
3. Calcula `vl_limite_superior = vl_medio_7d + 2 * vl_desvio_7d` e `nr_sigma_desvio = (custo - média) / NULLIF(desvio, 0)` — o `NULLIF` protege a divisão para jobs de custo constante
4. Mantém no resultado **apenas** as linhas onde `vl_estimativa_custo_usd > vl_limite_superior` — o model é uma tabela de alertas, não um espelho da staging
5. Classifica a severidade: `warning` (2σ–2.4σ) ou `critical` (**>2.4σ**)

> **Corte de severidade: 2.4σ, não 3σ (v1.1).** A v1.0 previa `critical` acima de 3σ. Com uma janela de apenas 7 pontos, o `STDDEV_POP` é estimado sobre uma amostra pequena e acaba inflado pelo próprio outlier que está sendo avaliado — o que empurra picos genuinamente graves para baixo de 3σ e deixa a categoria `critical` quase sempre vazia. O corte foi calibrado em 2.4σ contra os outliers injetados pelo gerador (multiplicador 3x–8x), de modo que `critical` isole de fato os extremos. Uma alternativa mais robusta, não implementada, seria calcular a média e o desvio **excluindo a linha avaliada** (`ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING`), o que permitiria voltar ao corte clássico de 3σ.

*Decisão de design: mover a detecção para dbt elimina um script avulso, centraliza os testes de qualidade e demonstra maturidade de pipeline — desvio padrão clássico mantém a lógica explicável em entrevista técnica.*

## 7.2 Geração de outliers nos dados sintéticos
O script de geração de dados injeta anomalias propositais para garantir que o detector tenha casos reais para encontrar:

- 5% dos registros recebem um multiplicador aleatorio de 3x a 8x no custo
- As anomalias são distribuídas de forma nao-uniforme entre os domínios (fintech e payments tem mais)
- O script loga quais registros foram marcados como anomalias para facilitar validação

# 8. Orquestração com Airflow
## 8.1 DAG pipeline_daily
**Parametro**

**Valor**

dag_id

food_delivery_cost_monitor

schedule_interval

@daily

start_date

2026-05-01

catchup

False

max_active_runs

1

retries

2

retry_delay

5 minutos

tags

data-platform, cost-monitoring, finops

## 8.2 Tasks da DAG
**Task ID**

**Operador**

**Descrição**

generate_synthetic_data

PythonOperator

Executa generate_synthetic_data.py para a data de execução

upload_to_s3

PythonOperator

Faz upload dos Parquets para o S3 particionado

dbt_pipeline

Cosmos DbtTaskGroup

Gera dinamicamente uma task por model dbt (stg_job_logs, int_cost_by_domain, mart_platform_efficiency, mart_anomalies) \+ testes, replicando o grafo de dependências do projeto dbt dentro da DAG

notify_completion

PythonOperator

Loga resumo da execução (jobs processados, anomalias detectadas)

## 8.3 Integração dbt via Astronomer Cosmos
**Decisão (2026-06-19):** a orquestração das camadas dbt deixa de ser feita via um `BashOperator` por camada e passa a usar `astronomer-cosmos`, que converte o grafo de dependências do dbt (`stg_job_logs > int_cost_by_domain > mart_platform_efficiency, mart_anomalies`) em um `TaskGroup` nativo do Airflow — uma task por model/test, em vez de uma task por camada.

**Parametro**

**Valor**

execution_mode

`ExecutionMode.LOCAL`

dbt_executable_path

Aponta para o binário de uma virtualenv dedicada (ex.: `/usr/local/airflow/dbt_venv/bin/dbt`), e não para o ambiente Python do próprio Airflow

Onde a venv é criada

No `airflow_project/Dockerfile`, em build time — ex.: `python -m venv /usr/local/airflow/dbt_venv && /usr/local/airflow/dbt_venv/bin/pip install dbt-core==1.11.* dbt-athena==1.10.*`

Motivação

Evita conflito de dependências entre dbt-core/dbt-athena e os pacotes já fixados pelo Astro Runtime (Airflow 3.x), sem pagar o custo de criar a venv a cada execução de task — como ocorreria no `ExecutionMode.VIRTUALENV` dinâmico

Trade-off aceito

A imagem do Airflow fica maior (duas instalações Python) e qualquer atualização de versão do dbt exige rebuild da imagem — aceitável, já que o deploy via Astro CLI já rebuilda a imagem a cada alteração

ProfileConfig (pendente)

A decidir: apontar direto para o `profiles.yml` já existente em `dbt_food_cost_monitor/`, ou usar um `ProfileMapping` nativo do Cosmos para Athena (a confirmar se existe suporte oficial do Cosmos para esse adapter)

# 9. Dashboard Power BI

> **Seção reescrita na v1.1** para refletir o dashboard efetivamente construído. Documentação detalhada (modelo semântico, DAX, limitações) em `docs/dashboard.md`. [Link público ao vivo](https://app.powerbi.com/view?r=eyJrIjoiMWU2N2VkZDktOTg3MC00NTA2LTljYTgtYjUyNWU3OTNiMzZiIiwidCI6IjJiZDE5YWQ4LTcyZTUtNGY2ZC1hZmY1LWRhOTMwMTdmZGYxYiJ9).

## 9.0 Conexão e modelo semântico
- Conexão ODBC (driver Simba) via DSN `Amazon Athena ODBC`, catálogo `AwsDataCatalog`, schema `cost_monitor_gold`
- Modo **Import**, não DirectQuery: como a DAG roda `@daily`, consultar o Athena a cada interação do usuário só geraria custo de scan sem ganho de atualidade
- Modelo em estrela: `dim_calendario` (marcada como tabela de datas) ligada por `Data` ao `dt_execucao` de `mart_platform_efficiency` e `mart_anomalies`
- ~33 medidas DAX em uma tabela container `_measures`, organizadas em display folders por mart de origem
- Publicação via *Publish to web* — aceitável porque todos os dados são sintéticos

## 9.1 KPIs principais (cards de métricas) — implementado
**Card** | **Medida** | **Definição**
--- | --- | ---
Tendência custo | `Pc. Tendência Custo Card` | Variação % do custo vs. dia anterior, reaproveitando a coluna `vl_custo_dia_anterior` já calculada via `LAG()` no mart
Taxa sucesso | `Pc. Taxa Sucesso %` | `DIVIDE([Nr. Jobs com Sucesso], [Nr. Total Jobs])`
Custo médio Job | `Vl. Médio por Jobs` | `SUM(mart_platform_efficiency[vl_medio_por_job])`
Jobs críticos | `Nr. Jobs Criticos` | `CALCULATE(COUNT(mart_anomalies[id_job]), nm_severidade = "critical")`

> **Divergência vs. v1.0.** O plano original previa cards de custo total do dia, nº de jobs executados, nº de anomalias por severidade e domínio mais caro. A versão construída trocou métricas de **volume** por métricas de **eficiência** — mais alinhadas ao propósito FinOps do projeto, já que custo absoluto sem contexto de tendência não indica ação. A medida `Ds. Domínio mais Caro` existe no modelo mas não foi exposta em nenhum visual.

## 9.2 Gráficos e visualizações — implementado
Página única, com 3 slicers (dia, domínio, job), 4 KPI cards e 4 visuais:

**Visualização**

**Tipo**

**Fonte de dados**

Tendência de custo vs. benchmark (15 dias)

Line chart, 2 séries (`Vl. Total Custo USD (Últ. 15 dias)` e `Vl. Média 7d Eficiência (Últ. 15 dias)`)

mart_platform_efficiency

Custo acumulado do mes (MTD) por domínio

Donut chart (`Vl. Custo MTD`)

mart_platform_efficiency

Anomalias detectadas (ultimos 7 dias)

Tabela com badge de severidade por cor

mart_anomalies

Eficiência por domínio

Bar chart horizontal (`Pc. Score Eficiência`)

mart_platform_efficiency

**Não implementado:** o visual *Top 10 jobs mais caros (hoje)* foi descartado — exigiria expor `stg_job_logs` (camada bronze) ao Power BI, quebrando a regra de que o dashboard consome apenas a Gold.

**Parcialmente implementado:** a *banda de confiança* do gráfico de tendência tem as medidas prontas no modelo (`Vl. Limite Superior Tendência`, `Vl. Limite Inferior Tendência`, `Vl. Amplitude Banda`), mas ainda não está renderizada no visual.

As medidas de janela (`Últ. 7/15/30 dias`) ancoram na última data **com dado** — via `MAX(dt_execucao)` sobre `ALLSELECTED(dim_calendario[Data])` — e não em `TODAY()`, para que o relatório continue legível se o pipeline ficar dias sem rodar.

# 10. Plano de Execução (3 dias)

> **Nota v1.1:** o plano de 3 dias abaixo é o registro original da v1.0 e ficou preservado como tal. Na prática, a entrega levou de Maio a Julho de 2026 — a estimativa não previu o tempo gasto na migração para o Astro CLI, na resolução do conflito de dependências entre dbt-core e o Astro Runtime (§8.3) e na construção do modelo semântico do Power BI, que consumiu boa parte do esforço final.
## Dia 1 — Fundação
- Criar repositório no GitHub com estrutura de pastas completa
- Configurar Docker Compose: Airflow (webserver, scheduler, worker, postgres, redis)
- Criar bucket S3 e configurar permissões IAM minimas
- Implementar generate_synthetic_data.py com dados de 30 dias
- Implementar upload_to_s3.py com particionamento por data
- Executar athena_setup.py para criar database e tabela externa no Glue
- Validar query básica no Athena antes de continuar

## Dia 2 — Pipeline
- Inicializar projeto dbt com perfil Athena
- Implementar os 4 models dbt (stg, int, marts x2) com testes
- Rodar dbt run e dbt test manualmente — corrigir erros
- Implementar a DAG do Airflow e testar trigger manual
- Ajustar dependências e retry logic

## Dia 3 — Entrega
- Conectar Power BI Desktop ao Athena via ODBC (driver Simba)
- Construir as 6 visualizações e os 4 KPI cards
- Escrever README.md com: contexto, arquitetura, instruções de setup, prints do dashboard
- Tirar prints do dashboard e da DAG rodando para o post do LinkedIn
- Revisão final do código, remover credenciais, adicionar .gitignore
- Push final \+ publicar post no LinkedIn marcando o iFood

# 11. Critérios de Sucesso
## 11.1 Funcionais
- ✅ Pipeline roda de ponta a ponta sem erros via trigger manual no Airflow
- ✅ dbt test passa 100% (sem falhas de qualidade de dados)
- ✅ Anomalias detectadas correspondem aos outliers injetados na geração
- ✅ Dashboard exibe dados corretos para os ultimos 30 dias

## 11.2 De portfólio
- ✅ README explica a arquitetura em menos de 5 minutos de leitura
- ✅ Código está documentado com docstrings nas funções principais
- ✅ Nenhuma credencial AWS no repositório — `.env` coberto pelo `.gitignore` da raiz e pelo de `airflow_project/`; `bandit` roda no pre-commit
- ✅ Prints do dashboard demonstram as visualizacoes funcionando — em `dashboard/assets/`, incorporados ao README
- ✅ Dashboard publicado com link público interativo (extra, não previsto na v1.0)
- 🔜 Post no LinkedIn com link do repositorio e menção ao iFood

*Diferencial: o projeto demonstra exatamente as competências listadas na vaga — pipelines de dados, SQL/dbt, Python, AWS, monitoramento de custos, e interesse em FinOps. Cada componente mapeia diretamente para uma responsabilidade descrita no 'Cardapio Diario' da posição.*

# 12. Próximos Passos (pos-entrega)
Melhorias opcionais para enriquecer o portfolio após a entrega inicial:

- Adicionar Great Expectations para validação de qualidade além dos testes dbt
- Substituir dados sintéticos por dados reais de uso público (ex: NYC Taxi Dataset como proxy)
- Adicionar alertas via Slack/email quando anomalias criticas sao detectadas
- Implementar infrastructure as code com Terraform para os recursos AWS
- Adicionar unit tests para as funções Python (pytest) — hoje `airflow_project/tests/` só valida integridade de DAG
- Pipeline de CI (GitHub Actions) rodando `task lint-check`, `task security` e os testes a cada push
- Explorar compressao Parquet (snappy vs zstd) e seu impacto no custo do Athena

Específicos da camada de visualização (ver `docs/dashboard.md`):

- Refresh agendado do dashboard via gateway de dados apontando para o Athena (hoje é manual)
- Renderizar a banda de confiança ±2σ no gráfico de tendência (medidas já existem no modelo)
- Nomear a página do relatório (hoje `Page 1`) e remover a medida placeholder `Nr. Anomalias Criticas`, definida como a constante `1`
- Expor `Ds. Domínio mais Caro` em um visual — a medida existe mas não é usada
- Avaliar a janela de anomalia excluindo a linha avaliada (`ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING`) para permitir voltar ao corte clássico de 3σ (§7.1)
