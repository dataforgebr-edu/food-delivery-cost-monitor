# food-delivery-cost-monitor

**Pipeline de Monitoramento de Custos de Plataforma de Dados**

> Documento de Requisitos do Produto (PRD)

---

**Campo** | **Valor**
--- | ---
Versão | 1.0
Data | Maio 2026
Status | Em desenvolvimento
Contexto | Projeto portfolio — inspirado no ambiente de Governança de Dados do iFood
Stack principal | Python, SQL, dbt, Airflow, AWS S3, AWS Athena, Docker, Power BI


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
4. Detecta anomalias de custo com Python (desvio padrão)
5. Orquestra todo o fluxo com Apache Airflow
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

Python (pandas)

Cálculo de desvio padrão sobre janela móvel de 7 dias por job/domínio

Orquestração

Apache Airflow

DAG diária com dependências entre tarefas, retry e alertas de falha

Visualização

Power BI

Dashboard com KPIs de custo, tendências e lista de anomalias detectadas

Infra local

Docker Compose

Airflow em Docker local; Power BI Desktop instalado na máquina

## 2.1 Fluxo de dados detalhado
- O script generate_synthetic_data.py cria N registros de logs para cada data do período configuradoigurado
- Os logs sao salvos em formato Parquet e enviados para S3 no caminho s3://ifood-cost-monitor/raw/job_logs/date=YYYY-MM-DD/
- O AWS Glue Data Catalog registra a tabela externa apontando para o S3
- O dbt executa os models sequencialmente: stg_job_logs > int_cost_by_domain > mart_platform_efficiency
- O script detect_anomalies.py le o mart final via Athena, calcula os desvios e escreve os alertas na tabela mart_anomalies
- O Power BI Desktop conecta-se ao Athena via ODBC e exibe os dashboards em tempo real

## 2.2 Diagrama de dependências da DAG
*generate_data >> upload_to_s3 >> run_dbt_staging >> run_dbt_intermédiate >> run_dbt_marts >> detect_anomalies >> notify_completion*

# 3. Estrutura de Pastas
**Caminho**

**Descrição**

dags/pipeline_daily.py

DAG principal do Airflow — orquestra todo o pipeline diario

ingestion/generate_synthetic_data.py

Gera logs sintéticos com sazonalidade e outliers propositais

ingestion/upload_to_s3.py

Faz upload dos Parquets para o S3 com particionamento por data

dbt_project/models/staging/stg_job_logs.sql

Limpeza e tipagem dos dados brutos

dbt_project/models/intermédiate/int_cost_by_domain.sql

Agregação intermédiaria por domínio e dia

dbt_project/models/marts/mart_platform_efficiency.sql

Tabela final com KPIs de eficiência

dbt_project/models/marts/mart_anomalies.sql

Tabela de alertas de anomalias detectadas

dbt_project/sources.yml

Definição das fontes de dados (tabela raw no Athena)

dbt_project/schema.yml

Documentação e testes dos models (not_null, unique, accepted_values)

anomaly_detection/detect_anomalies.py

Detecta jobs com custo > 2 sigma da média movel de 7 dias

infra/docker-compose.yml

Sobe Airflow (webserver, scheduler, worker) — Power BI Desktop instalado localmente

infra/athena_setup.py

Cria o database e a tabela externa no AWS Glue/Athena

infra/requirements.txt

Dependências Python do projeto

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

## 4.2 Staging — stg_job_logs
Limpeza e padronização dos dados brutos. Inclui filtros de qualidade (remoção de registros com status invalido ou custo negativo) e adição de colunas derivadas.

- Todos os campos do raw, tipados corretamente
- cost_category: classificação do custo em low / medium / high / critical
- is_anomaly_candidate: flag booleana para jobs com custo acima do percentil 95 do dia
- execution_week: número da semana para agregacoes semanais

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

Docker \+ Docker Compose

v2\+

Orquestração local dos servicos

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
Todas as credenciais e configurações sensíveis sao gerenciadas via arquivo .env (nunca commitado no repositorio):

*AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_REGION=us-east-1
S3_BUCKET=food-delivery-cost-monitor
ATHENA_DATABASE=cost_monitor
ATHENA_OUTPUT_LOCATION=s3://food-delivery-cost-monitor/athena-results/*

# 6. Models dbt — Detalhamento
## 6.1 sources.yml
Define a tabela raw do S3/Athena como fonte de dados para o dbt. Inclui testes de qualidade básicos: not_null nos campos criticos, accepted_values para status e domain.

## 6.2 stg_job_logs.sql
- SELECT com CAST explicito em todos os tipos
- WHERE status IN ('success', 'failed', 'timeout') — remove registros invalidos
- WHERE estimated_cost_usd >= 0 — remove custos negativos
- CASE WHEN para criar cost_category
- DATE_TRUNC('week', execution_date) para execution_week

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
O script detect_anomalies.py implementa detecção estatistica simples e interpretavel — ideal para demonstrar em um portfolio sem complexidade excessiva:

1. Le o mart_platform_efficiency via Athena (boto3 \+ pandas)
2. Para cada combinacao de domain \+ job_name, calcula a média e o desvio padrão dos ultimos 7 dias
3. Marca como anomalia qualquer registro onde: custo_observado > média_7d \+ 2 * std_7d
4. Classifica a severidade: warning (2-3σ) ou critical (>3σ)
5. Escreve os resultados como Parquet no S3 e atualiza a tabela mart_anomalies no Athena

*Decisão de design: optou-se por desvio padrão classico (em vez de IQR ou algoritmos de ML) para manter a lógica explicavel em uma entrevista técnica — o que e um diferencial em contextos de FinOps e governanca.*

## 7.2 Geração de outliers nos dados sintéticos
O script de geração de dados injeta anomalias propositais para garantir que o detector tenha casos reais para encontrar:

- 5% dos registros recebem um multiplicador aleatorio de 3x a 8x no custo
- As anomalias são distribuídas de forma nao-uniforme entre os domínios (fintech e payments tem mais)
- O script loga quais registros foram marcados como anomalias para fácilitar validação

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

run_dbt_staging

BashOperator

dbt run --select staging.*

run_dbt_intermédiate

BashOperator

dbt run --select intermédiate.*

run_dbt_marts

BashOperator

dbt run --select marts.*

run_dbt_tests

BashOperator

dbt test — valida qualidade dos dados

detect_anomalies

PythonOperator

Executa detect_anomalies.py

notify_completion

PythonOperator

Loga resumo da execução (jobs processados, anomalias detectadas)

# 9. Dashboard Power BI
## 9.1 KPIs principais (cards de métricas)
- Custo total do dia (USD) — comparado com dia anterior
- Número de jobs executados — com taxa de sucesso
- Número de anomalias detectadas — com breakdown por severidade
- Domínio mais caro do dia

## 9.2 Gráficos e visualizações
**Visualização**

**Tipo**

**Fonte de dados**

Custo diario por domínio (ultimos 30 dias)

Line chart empilhado

mart_platform_efficiency

Top 10 jobs mais caros (hoje)

Bar chart horizontal

stg_job_logs

Eficiência por domínio

Heatmap ou bar chart

mart_platform_efficiency

Anomalias detectadas (ultimos 7 dias)

Tabela com badge de severidade

mart_anomalies

Custo acumulado do mes (MTD) por domínio

Donut chart

mart_platform_efficiency

Tendencia de custo vs benchmark 7d

Line chart com banda de confiança

mart_platform_efficiency

# 10. Plano de Execução (3 dias)
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
- Implementar detect_anomalies.py e validar com os dados do dia 1
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
- Pipeline roda de ponta a ponta sem erros via trigger manual no Airflow
- dbt test passa 100% (sem falhas de qualidade de dados)
- Anomalias detectadas correspondem aos outliers injetados na geração
- Dashboard exibe dados corretos para os ultimos 30 dias

## 11.2 De portfólio
- README explica a arquitetura em menos de 5 minutos de leitura
- Código está documentado com docstrings nas funções principais
- Nenhuma credencial AWS no repositório (validar com git-secrets ou similar)
- Prints do dashboard demonstram as visualizacoes funcionando
- Post no LinkedIn com link do repositorio e menção ao iFood

*Diferencial: o projeto demonstra exatamente as competências listadas na vaga — pipelines de dados, SQL/dbt, Python, AWS, monitoramento de custos, e interesse em FinOps. Cada componente mapeia diretamente para uma responsabilidade descrita no 'Cardapio Diario' da posição.*

# 12. Próximos Passos (pos-entrega)
Melhorias opcionais para enriquecer o portfolio após a entrega inicial:

- Adicionar Great Expectations para validação de qualidade além dos testes dbt
- Substituir dados sintéticos por dados reais de uso público (ex: NYC Taxi Dataset como proxy)
- Adicionar alertas via Slack/email quando anomalias criticas sao detectadas
- Implementar infrastructure as code com Terraform para os recursos AWS
- Adicionar unit tests para as funções Python (pytest)
- Explorar compressao Parquet (snappy vs zstd) e seu impacto no custo do Athena

