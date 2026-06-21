# Setup do ambiente Airflow + dbt + Astronomer Cosmos

Status | A implementar
Data | 2026-06-20 (revisado — ver decisões de reorganização de pastas e Connections/Variables)
Relacionado | `docs/food-delivery-cost-monitor-PRD.md` (seção 8.3)

Este documento é um roteiro de configuração — nenhum dos arquivos abaixo foi criado ainda. Serve de checklist para implementar a orquestração da camada dbt dentro da DAG do Airflow via Astronomer Cosmos.

## Decisões já tomadas

| Decisão | Escolha | Motivo |
|---|---|---|
| Onde vivem `pipeline/` e `dbt_food_cost_monitor/` em relação ao `airflow_project/` | Mover fisicamente para dentro de `airflow_project/` (sem script de cópia/sync) | É o padrão que a própria Astronomer recomenda para monorepo mantido por um único time: submódulo só compensa quando o código é mantido por outro time em outro repositório, e cópia/sync manual carrega risco de ficar desatualizado. Movendo de vez, o Dockerfile copia direto do build context e não existe risco de divergência |
| Como o Cosmos obtém config/credenciais do Athena (dbt) | Plano A: `AthenaAccessKeyProfileMapping` lendo uma Airflow Connection. Plano B (fallback): `ProfileConfig(profiles_yml_filepath=...)` reaproveitando o `profiles.yml` existente | Centraliza a credencial AWS em um único lugar (Connection do Airflow) em vez de duas cópias de `.env`. O bug conhecido da issue [astronomer-cosmos#996](https://github.com/astronomer/astronomer-cosmos/issues/996) só é disparado quando a Connection usa credenciais temporárias com session token (assume role/STS) — como a Connection aqui usa as mesmas access key/secret key estáticas do IAM user já usadas hoje, o risco é baixo. Se ainda assim falhar, cai para o Plano B sem retrabalho, pois já está especificado |
| Como as tasks de ingestão (`pipeline/`) obtêm config/credenciais | Camada de tradução dentro da própria task da DAG: lê a Connection + Variables do Airflow e injeta em `os.environ` antes de chamar o código de `pipeline/` | Mantém `pipeline/` agnóstico de Airflow e testável standalone via Poetry (sem precisar de `apache-airflow` instalado só para importar `pipeline_config.py`). Evita duplicar a mesma credencial AWS em `airflow_project/.env` |
| Modo de execução do dbt dentro da DAG | `ExecutionMode.LOCAL` com venv dbt dedicada, criada em build time no Dockerfile | Evita conflito de dependências com o Astro Runtime e evita o custo de criar venv a cada execução de task (alternativa: `ExecutionMode.VIRTUALENV` dinâmico, descartada) |

## 0. Visão geral do que falta

| # | O que | Onde |
|---|---|---|
| 1 | Mover `pipeline/` e `dbt_food_cost_monitor/` para dentro de `airflow_project/` | raiz → `airflow_project/pipeline/`, `airflow_project/dbt_food_cost_monitor/` |
| 2 | Atualizar caminhos que assumiam a raiz | `pyproject.toml`, `.pre-commit-config.yaml`, `CLAUDE.md` |
| 3 | Dependência Cosmos | `airflow_project/requirements.txt` |
| 4 | venv dbt isolada + `dbt deps` no build | `airflow_project/Dockerfile` |
| 5 | Ignorar lixo gerado | `airflow_project/.dockerignore` |
| 6 | Connection AWS + Variables (dev local) | `airflow_project/airflow_settings.yaml` |
| 7 | DAG com Cosmos + injeção de env vars pra `pipeline/` | `airflow_project/dags/pipeline_daily.py` (e apagar `exampledag.py`) |
| 8 | Validação local | `astro dev start` |

## 1. Reorganização de pastas

Mover (`git mv`, preservando histórico):
- `dbt_food_cost_monitor/` → `airflow_project/dbt_food_cost_monitor/`
- `pipeline/` → `airflow_project/pipeline/`

Com isso, o `airflow_project/` passa a ser o build context que já contém tudo que o Dockerfile precisa — sem `include/`, sem script de sincronização, sem risco de cópia desatualizada.

Ajustes necessários em arquivos que hoje assumem que essas pastas estão na raiz:

**`pyproject.toml`**
```toml
[tool.taskipy.tasks]
lint       = { cmd = "black --fast airflow_project/pipeline && isort airflow_project/pipeline", help = "Formata código" }
lint-check = { cmd = "black --fast --check airflow_project/pipeline && isort --check airflow_project/pipeline", help = "Verifica formatação (CI)" }
security   = { cmd = "bandit -r airflow_project/pipeline -lll", help = "Scan de segurança" }
athena_setup = { cmd = "python -m infra.athena_setup", help = "criação do banco e da tabela no glue" }

[tool.pyright]
include = ["airflow_project/pipeline"]
extraPaths = ["airflow_project/pipeline"]
```

**`.pre-commit-config.yaml`**
```yaml
  - repo: https://github.com/sqlfluff/sqlfluff
    rev: 4.2.1
    hooks:
      - id: sqlfluff-lint
        exclude: ^airflow_project/dbt_food_cost_monitor/
      - id: sqlfluff-fix
        exclude: ^airflow_project/dbt_food_cost_monitor/
```

**`CLAUDE.md`** — atualizar a seção "Estrutura de pastas prevista" e os comandos de `sqlfluff` (`poetry run sqlfluff lint airflow_project/dbt_food_cost_monitor/ --dialect ansi`) para os novos caminhos.

O Poetry (`.venv`, `pyproject.toml`, `poetry.lock`) continua na raiz do repo — só os caminhos referenciados dentro dos comandos mudam, o ambiente virtual em si não se move.

## 2. Dependências Python

**`airflow_project/requirements.txt`** (ambiente principal do Airflow, gerenciado pelo Astro CLI):
```
astronomer-cosmos>=1.11,<2.0
```

⚠️ A imagem base é `astrocrpublic.azurecr.io/runtime:3.2-4`. Há um issue em aberto no repositório do Cosmos sobre suporte a Airflow 3.2 (astronomer-cosmos#2403). **Validar com `astro dev start` se a build sobe sem erro de import** antes de seguir para a DAG; se travar, pode ser necessário fixar outra versão do Cosmos ou outro runtime.

**`pyproject.toml`** já tem `dbt-core` e `dbt-athena` pinados (uso local via Poetry) — não precisa mudar. Importante manter as mesmas faixas de versão na venv isolada do Dockerfile (passo 4), para não rodar uma versão de dbt local e outra dentro do Airflow.

## 3. `airflow_project/Dockerfile`

```dockerfile
FROM astrocrpublic.azurecr.io/runtime:3.2-4

# venv isolada só para o dbt (mesmas faixas de versão do pyproject.toml)
RUN python -m venv /usr/local/airflow/dbt_venv && \
    /usr/local/airflow/dbt_venv/bin/pip install --no-cache-dir \
        "dbt-core>=1.11.11,<2.0.0" \
        "dbt-athena>=1.10.1,<2.0.0"

# projeto dbt já vive dentro do build context (passo 1) — cópia direta, sem include/
COPY dbt_food_cost_monitor/ /usr/local/airflow/dbt_food_cost_monitor/

# dbt_packages é regenerado aqui dentro, não confiamos no que está no disco do dev
RUN /usr/local/airflow/dbt_venv/bin/dbt deps \
    --project-dir /usr/local/airflow/dbt_food_cost_monitor

# pacote pipeline/ reutilizado pelas tasks de ingestão
COPY pipeline/ /usr/local/airflow/pipeline/
ENV PYTHONPATH="${PYTHONPATH}:/usr/local/airflow/pipeline"
```

Otimização opcional (cache de layer): copiar só `packages.yml`/`package-lock.yml` antes do `dbt deps`, e o resto do projeto depois, para o `dbt deps` não rodar de novo a cada rebuild por mudança num model. Não é essencial no início — só vale se o build começar a ficar lento.

## 4. `airflow_project/.dockerignore`

```
astro
.git
.env
airflow_settings.yaml
logs/
.venv
airflow.db
airflow.cfg
dbt_food_cost_monitor/target
dbt_food_cost_monitor/logs
dbt_food_cost_monitor/dbt_packages
pipeline/__pycache__
pipeline/data
**/__pycache__
```

## 5. Connections e Variables (Airflow) — dev local via `airflow_settings.yaml`

O Astro CLI já inclui `airflow_project/airflow_settings.yaml` (gitignorado, equivalente ao `.env` mas para o modelo de objetos do Airflow). É nele que a credencial AWS e a config do Athena/dbt são cadastradas para o ambiente local — substituindo a necessidade de um `airflow_project/.env` com as variáveis duplicadas do `.env` da raiz.

```yaml
airflow:
  connections:
    - conn_id: aws_athena
      conn_type: aws
      conn_login: <AWS_ACCESS_KEY_ID>
      conn_password: <AWS_SECRET_ACCESS_KEY>
      conn_extra:
        region_name: us-east-1
        database: awsdatacatalog
        schema: cost_monitor
        s3_staging_dir: <DBT_STAGING_DIR>
  variables:
    - variable_name: s3_bucket
      variable_value: <S3_BUCKET>
    - variable_name: athena_database
      variable_value: cost_monitor
```

A mesma Connection `aws_athena` serve dois consumidores:
- **dbt (via Cosmos)**: `AthenaAccessKeyProfileMapping` lê `conn_login`/`conn_password` como `aws_access_key_id`/`aws_secret_access_key` e os campos de `conn_extra` (`region_name`, `database`, `schema`, `s3_staging_dir`) diretamente — não precisa de nenhum glue code, é o mapeamento nativo da classe.
- **pipeline (ingestão)**: a DAG lê essa mesma Connection + as Variables e injeta em `os.environ` antes de chamar o código de `pipeline/` (detalhe no passo 7).

⚠️ Isso é só para dev local. Em produção (deploy via Astro), a Connection e as Variables são cadastradas via Environment Variables/secrets do Astro Cloud — pendência para quando houver deploy real, não bloqueia o setup local (ver passo 8).

## 6. DAG — `airflow_project/dags/pipeline_daily.py`

Primeiro apagar `dags/exampledag.py` (placeholder do `astro dev init`, sem relação com o projeto).

Esqueleto (`RenderConfig(dbt_deps=False)` porque o `dbt deps` já rodou no build da imagem):

```python
import os

from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.sdk import dag, task
from pendulum import datetime

from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.constants import ExecutionMode
from cosmos.profiles import AthenaAccessKeyProfileMapping

DBT_PROJECT_DIR = "/usr/local/airflow/dbt_food_cost_monitor"
DBT_VENV_BIN = "/usr/local/airflow/dbt_venv/bin/dbt"
AWS_CONN_ID = "aws_athena"

# Plano A — AthenaAccessKeyProfileMapping (Connection do Airflow)
profile_config = ProfileConfig(
    profile_name="dbt_food_cost_monitor",
    target_name="dev",
    profile_mapping=AthenaAccessKeyProfileMapping(conn_id=AWS_CONN_ID),
)

# Plano B — se o profile mapping falhar (ex.: astronomer-cosmos#996), trocar pelo bloco abaixo:
# profile_config = ProfileConfig(
#     profile_name="dbt_food_cost_monitor",
#     target_name="dev",
#     profiles_yml_filepath=f"{DBT_PROJECT_DIR}/profiles.yml",
# )

execution_config = ExecutionConfig(
    execution_mode=ExecutionMode.LOCAL,
    dbt_executable_path=DBT_VENV_BIN,
)
render_config = RenderConfig(dbt_deps=False)


def _inject_aws_env_from_airflow() -> None:
    """Traduz Connection/Variables do Airflow para os.environ, mantendo pipeline/ agnóstico de Airflow."""
    conn = BaseHook.get_connection(AWS_CONN_ID)
    os.environ["AWS_ACCESS_KEY_ID"] = conn.login
    os.environ["AWS_SECRET_ACCESS_KEY"] = conn.password
    os.environ["AWS_REGION"] = conn.extra_dejson.get("region_name", "us-east-1")
    os.environ["S3_BUCKET"] = Variable.get("s3_bucket")
    os.environ["ATHENA_DATABASE"] = Variable.get("athena_database")


@dag(dag_id="food_delivery_cost_monitor", schedule="@daily", start_date=datetime(2026, 6, 1), catchup=False)
def pipeline_daily():

    @task
    def generate_data():
        _inject_aws_env_from_airflow()
        from ingestion.generate_synthetic_data import main  # ajustar ao entrypoint real
        main()

    @task
    def upload_to_s3():
        _inject_aws_env_from_airflow()
        from ingestion.upload_to_s3 import main  # ajustar ao entrypoint real
        main()

    dbt_pipeline = DbtTaskGroup(
        group_id="dbt_pipeline",
        project_config=ProjectConfig(dbt_project_path=DBT_PROJECT_DIR),
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=render_config,
    )

    @task
    def notify_completion():
        print("Pipeline concluído")

    generate_data() >> upload_to_s3() >> dbt_pipeline >> notify_completion()

pipeline_daily()
```

⚠️ Os nomes de função `main()` em `generate_synthetic_data`/`upload_to_s3` são um placeholder — confirmar o entrypoint real desses módulos. `generate_synthetic_data.py` usa `argparse`, então pode não ter uma função `main()` exportável; pode ser melhor chamar via `BashOperator` (`python -m ingestion.generate_synthetic_data ...`) em vez de importar — nesse caso, a injeção de `os.environ` do passo `_inject_aws_env_from_airflow` precisa ser feita via parâmetro `env=` do `BashOperator` em vez de `os.environ` direto no processo do worker.

## 7. Validação local

```bash
cd airflow_project
astro dev start                   # builda a imagem e sobe webserver/scheduler/postgres
```

Checklist:
- Build não falha no `dbt deps` nem na instalação do Cosmos.
- DAG `food_delivery_cost_monitor` aparece na UI sem erro de import (`Dag Import Errors`).
- `dbt_pipeline` aparece expandido como `TaskGroup` com uma task por model/test (sinal de que o Cosmos parseou o manifest certo).
- Rodar a DAG manualmente e confirmar que a task do primeiro model dbt (`stg_job_logs`) realmente conecta no Athena (erro de credencial apareceria aqui primeiro — se for o erro `InvalidClientTokenId`/"security token included in the request is invalid" da issue #996, trocar para o Plano B do passo 6).

## 8. Para depois (não bloqueia o setup inicial)

- Credenciais de produção no Astro Cloud (ver nota de produção no passo 5).
- Alertas de falha (`on_failure_callback` ou Slack/email no nível da DAG).

## Referências

- [Use Git submodules with an Astro project — Astronomer Documentation](https://www.astronomer.io/docs/astro/best-practices/git-submodules)
- [Deploy dbt projects to Astro — Astronomer Documentation](https://www.astronomer.io/docs/astro/deploy-dbt-project)
- [AthenaAccessKey — Astronomer Cosmos documentation](https://astronomer.github.io/astronomer-cosmos/profiles/AthenaAccessKey.html)
- [AthenaAccessKeyProfileMapping does not work as expected locally · Issue #996 · astronomer/astronomer-cosmos](https://github.com/astronomer/astronomer-cosmos/issues/996)
- [Add support to Airflow 3.2.0 · Issue #2403 · astronomer/astronomer-cosmos](https://github.com/astronomer/astronomer-cosmos/issues/2403)
- [Configure airflow_settings.yaml (local development only) — Astronomer Documentation](https://www.astronomer.io/docs/astro/cli/develop-project#configure-airflow_settingsyaml-local-development-only)
