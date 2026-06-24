# Setup do ambiente Airflow + dbt + Astronomer Cosmos

Status | A implementar
Data | 2026-06-23 (revisado — ver decisão de onde entram as deps do `pipeline/` e cache de camada do `dbt deps`)
Relacionado | `docs/food-delivery-cost-monitor-PRD.md` (seção 8.3)

Este documento é um roteiro de configuração — nenhum dos arquivos abaixo foi criado ainda. Serve de checklist para implementar a orquestração da camada dbt dentro da DAG do Airflow via Astronomer Cosmos.

## Decisões já tomadas

| Decisão | Escolha | Motivo |
|---|---|---|
| Onde vivem `pipeline/` e `dbt_food_cost_monitor/` em relação ao `airflow_project/` | Mover fisicamente para dentro de `airflow_project/` (sem script de cópia/sync) | É o padrão que a própria Astronomer recomenda para monorepo mantido por um único time: submódulo só compensa quando o código é mantido por outro time em outro repositório, e cópia/sync manual carrega risco de ficar desatualizado. Movendo de vez, o Dockerfile copia direto do build context e não existe risco de divergência |
| Como o Cosmos obtém config/credenciais do Athena (dbt) | `ProfileConfig(profiles_yml_filepath=...)` reaproveitando o `profiles.yml` existente, sem Connection do Airflow | **Revisado em 2026-06-23** — a versão original (Plano A, `AthenaAccessKeyProfileMapping` lendo uma Connection) partia da premissa de centralizar a credencial "em vez de duas cópias de `.env`", mas a própria Connection vira uma terceira cópia em texto plano dentro de `airflow_settings.yaml` — não elimina duplicação nenhuma. O `profiles.yml` não referencia `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` via `env_var()` (só `DBT_DATABASE`, `DBT_REGION_NAME` etc.) — a autenticação já cai no provider padrão do boto3, que lê essas variáveis do ambiente do processo. Bastando elas chegarem no container via `env_file` (passo 7), o Plano B funciona sem nenhuma mudança no `profiles.yml`, e evita por completo o risco da issue [astronomer-cosmos#996](https://github.com/astronomer/astronomer-cosmos/issues/996) (em vez de só mitigá-lo) |
| Como as tasks de ingestão (`pipeline/`) obtêm config/credenciais | Direto do ambiente do container, via `env_file` no `docker-compose.override.yml` apontando pro `.env` da raiz (passo 7) — sem Connection, sem Variable, sem camada de tradução na DAG | **Revisado em 2026-06-23** — `pipeline_config.py` já faz `load_dotenv()` + `os.getenv(...)` por conta própria; dentro do container o `load_dotenv()` não encontra nenhum `.env` (está no `.dockerignore`) e cai direto pro `os.getenv()`, já populado pelo `env_file`. Mantém `pipeline/` agnóstico de Airflow e testável standalone via Poetry, e elimina a necessidade de uma função de tradução (`_inject_aws_env_from_airflow`) na DAG |
| Modo de execução do dbt dentro da DAG | `ExecutionMode.LOCAL` com venv dbt dedicada, criada em build time no Dockerfile | Evita conflito de dependências com o Astro Runtime e evita o custo de criar venv a cada execução de task (alternativa: `ExecutionMode.VIRTUALENV` dinâmico, descartada) |
| Onde instalar as dependências Python do `pipeline/` (`boto3`, `pandas`, `pyarrow`, `python-dotenv`) | Ambiente principal da imagem via `airflow_project/requirements.txt`, sem venv isolada | Diferente do `dbt-core` (árvore de dependências grande e historicamente conflitante com o Airflow — ver issue #996 acima), esses pacotes são comuns e com pins soltos: risco de colisão baixo com o Astro Runtime/Cosmos. Isolar junto com a venv do dbt só acoplaria as duas árvores de dependência sem motivo funcional — e não resolveria a execução isolada por si só, já que tasks `@task` correm no processo do próprio worker/scheduler do Airflow, não num binário externo chamado via subprocess como o `dbt` |

## 0. Visão geral do que falta

| # | O que | Onde |
|---|---|---|
| 1 | Mover `pipeline/` e `dbt_food_cost_monitor/` para dentro de `airflow_project/` | raiz → `airflow_project/pipeline/`, `airflow_project/dbt_food_cost_monitor/` |
| 2 | Atualizar caminhos que assumiam a raiz | `pyproject.toml`, `.pre-commit-config.yaml`, `CLAUDE.md` |
| 3 | Dependências do ambiente principal (Cosmos + `pipeline/`) | `airflow_project/requirements.txt` |
| 4 | venv dbt isolada + `dbt deps` no build, com cache de camada via `packages.yml`/`package-lock.yml` | `airflow_project/Dockerfile` |
| 5 | Ignorar lixo gerado | `airflow_project/.dockerignore` |
| 6 | ~~Connection AWS + Variables~~ — descartado, credenciais vêm via `env_file` (passo 7) | `airflow_project/airflow_settings.yaml` |
| 7 | DAG com Cosmos (Plano B) | `airflow_project/dags/pipeline_daily.py` (e apagar `exampledag.py`) |
| 8 | Volumes locais pra dev sem rebuild + credenciais via `env_file` | `airflow_project/docker-compose.override.yml` |
| 9 | Validação local | `astro dev start` |

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
boto3>=1.43.18,<2.0.0
pandas>=3.0.3,<4.0.0
pyarrow>=24.0.0,<25.0.0
python-dotenv>=1.2.2,<2.0.0
```

As 4 últimas linhas são as dependências de runtime do `pipeline/` (hoje só copiado via `COPY` + `PYTHONPATH` no Dockerfile, sem instalação própria — sem isso o `import` dentro das tasks da DAG falha por módulo ausente). Mesmas faixas de versão do `pyproject.toml` da raiz, pra não rodar uma versão localmente via Poetry e outra dentro do Airflow.

⚠️ Antes de fixar `boto3`, checar se ele já não vem pré-instalado via os provider packages do Astro Runtime (`astro dev bash` + `pip show boto3`) — evita duplicar uma dependência que o Airflow/Cosmos já trazem.

⚠️ A imagem base é `astrocrpublic.azurecr.io/runtime:3.2-4`. Há um issue em aberto no repositório do Cosmos sobre suporte a Airflow 3.2 (astronomer-cosmos#2403). **Validar com `astro dev start` se a build sobe sem erro de import** antes de seguir para a DAG; se travar, pode ser necessário fixar outra versão do Cosmos ou outro runtime.

**`pyproject.toml`** já tem `dbt-core` e `dbt-athena` pinados (uso local via Poetry) — não precisa mudar. Importante manter as mesmas faixas de versão na venv isolada do Dockerfile (passo 3), para não rodar uma versão de dbt local e outra dentro do Airflow.

## 3. `airflow_project/Dockerfile`

```dockerfile
FROM astrocrpublic.azurecr.io/runtime:3.2-4

# venv isolada só para o dbt (mesmas faixas de versão do pyproject.toml)
RUN python -m venv /usr/local/airflow/dbt_venv && \
    /usr/local/airflow/dbt_venv/bin/pip install --no-cache-dir \
        "dbt-core>=1.11.11,<2.0.0" \
        "dbt-athena>=1.10.1,<2.0.0"

# só os manifestos de dependência primeiro — cache de camada: o dbt deps só reroda
# quando packages.yml/package-lock.yml mudam, não a cada edição de model
COPY dbt_food_cost_monitor/packages.yml dbt_food_cost_monitor/package-lock.yml /usr/local/airflow/dbt_food_cost_monitor/
RUN /usr/local/airflow/dbt_venv/bin/dbt deps \
    --project-dir /usr/local/airflow/dbt_food_cost_monitor

# resto do projeto dbt — já vive dentro do build context (passo 1), sem include/.
# dbt_packages está no .dockerignore (passo 4): essa COPY não sobrescreve o que
# o dbt deps acabou de instalar
COPY dbt_food_cost_monitor/ /usr/local/airflow/dbt_food_cost_monitor/

# pacote pipeline/ reutilizado pelas tasks de ingestão
COPY pipeline/ /usr/local/airflow/pipeline/
ENV PYTHONPATH="${PYTHONPATH}:/usr/local/airflow/pipeline"
```

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

## 5. Connections e Variables (Airflow) — descartado

**Revisado em 2026-06-23.** Este passo cadastraria uma Connection `aws_athena` e Variables `s3_bucket`/`athena_database` via `airflow_project/airflow_settings.yaml`, consumidas pelo Plano A do Cosmos e por uma função de tradução na DAG. Como o Plano B virou o caminho principal (ver decisão no topo) e `pipeline_config.py` já lê `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, `S3_BUCKET` e `ATHENA_DATABASE` direto do ambiente do processo, nenhuma dessas variáveis precisa de Connection nem Variable do Airflow — tudo chega via `env_file` no `docker-compose.override.yml` (passo 7), apontando pro `.env` da raiz que já existe e já é usado pelo Poetry.

`airflow_settings.yaml` continua existindo no projeto (gerado pelo `astro dev init`), só não tem nenhuma entrada cadastrada por enquanto. Revisitar isso se surgir algo que realmente precise do modelo de Connection nativo do Airflow — por exemplo, um secrets backend (AWS Secrets Manager etc.), que tem suporte mais direto via Connection do que via env var solta.

⚠️ Isso é só para dev local. Em produção (deploy via Astro), as mesmas variáveis (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, `S3_BUCKET`, `ATHENA_DATABASE`, `DBT_*`) são cadastradas via Environment Variables/secrets do Astro Cloud — pendência para quando houver deploy real, não bloqueia o setup local (ver passo 9).

## 6. DAG — `airflow_project/dags/pipeline_daily.py`

Primeiro apagar `dags/exampledag.py` (placeholder do `astro dev init`, sem relação com o projeto).

Esqueleto (`RenderConfig(dbt_deps=False)` porque o `dbt deps` já rodou no build da imagem):

```python
from pendulum import datetime

from airflow.sdk import dag, task

from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.constants import ExecutionMode

DBT_PROJECT_DIR = "/usr/local/airflow/dbt_food_cost_monitor"
DBT_VENV_BIN = "/usr/local/airflow/dbt_venv/bin/dbt"

# Plano B — reaproveita o profiles.yml existente (env_var-based), sem Connection do Airflow.
# As variáveis (AWS_*, DBT_*) chegam no ambiente do container via env_file no
# docker-compose.override.yml (passo 7), apontando pro .env da raiz.
profile_config = ProfileConfig(
    profile_name="dbt_food_cost_monitor",
    target_name="dev",
    profiles_yml_filepath=f"{DBT_PROJECT_DIR}/profiles.yml",
)

# Plano A — alternativa via Connection do Airflow, descartada em 2026-06-23 (ver decisão no topo).
# Reconsiderar só se precisar de um secrets backend nativo do Airflow no futuro:
# from cosmos.profiles import AthenaAccessKeyProfileMapping
# profile_config = ProfileConfig(
#     profile_name="dbt_food_cost_monitor",
#     target_name="dev",
#     profile_mapping=AthenaAccessKeyProfileMapping(conn_id="aws_athena"),
# )

execution_config = ExecutionConfig(
    execution_mode=ExecutionMode.LOCAL,
    dbt_executable_path=DBT_VENV_BIN,
)
render_config = RenderConfig(dbt_deps=False)


@dag(dag_id="food_delivery_cost_monitor", schedule="@daily", start_date=datetime(2026, 6, 1), catchup=False)
def pipeline_daily():

    @task
    def generate_data():
        from ingestion.generate_synthetic_data import main  # ajustar ao entrypoint real
        main()

    @task
    def upload_to_s3():
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

⚠️ Os nomes de função `main()` em `generate_synthetic_data`/`upload_to_s3` são um placeholder — confirmar o entrypoint real desses módulos. `generate_synthetic_data.py` usa `argparse`, então pode não ter uma função `main()` exportável; pode ser melhor chamar via `BashOperator` (`python -m ingestion.generate_synthetic_data ...`) em vez de importar. Como as credenciais já vêm do ambiente do container (`env_file`, passo 7), um `BashOperator` funciona sem nenhum parâmetro `env=` extra — subprocessos herdam o ambiente do processo pai automaticamente.

## 7. `airflow_project/docker-compose.override.yml` (volumes pra dev sem rebuild + credenciais)

O Astro CLI detecta e faz merge automático de um `docker-compose.override.yml` colocado na raiz do projeto (`astro dev start`, modo container — não funciona em modo standalone). Ele só vale pra dev local: o `astro deploy` builda a imagem a partir do `Dockerfile` e a sobe pro registry sem nenhum compose envolvido, então esse arquivo nunca afeta produção.

Sem ele, `pipeline/` e `dbt_food_cost_monitor/` só entram nos containers via `COPY` no build (passos 1/3) — qualquer alteração exige rodar `astro dev start` de novo (rebuild completo, incluindo `dbt deps`). Com o override, essas pastas passam a ser bind mounts: edição no disco aparece direto dentro do container rodando, sem rebuild. O mesmo arquivo também resolve como as credenciais chegam nos containers (ver decisão revisada no topo e passo 5).

```yaml
services:
  scheduler:
    env_file:
      - ../.env
    volumes:
      - ./pipeline:/usr/local/airflow/pipeline
      - ./dbt_food_cost_monitor:/usr/local/airflow/dbt_food_cost_monitor
      - dbt_packages:/usr/local/airflow/dbt_food_cost_monitor/dbt_packages
  dag-processor:
    env_file:
      - ../.env
    volumes:
      - ./dbt_food_cost_monitor:/usr/local/airflow/dbt_food_cost_monitor
      - dbt_packages:/usr/local/airflow/dbt_food_cost_monitor/dbt_packages

volumes:
  dbt_packages:
```

Por que esses serviços (Airflow 3 — `scheduler` / `dag-processor` / `api-server` / `triggerer`, sem `webserver`):
- **`scheduler`**: com `LocalExecutor` (padrão do Astro local) é quem de fato executa o código das tasks — onde `pipeline/` é importado dentro da função da task e onde o `dbt` (`ExecutionMode.LOCAL`) roda.
- **`dag-processor`**: o Cosmos lê o manifest do dbt **no parse da DAG** pra montar a `TaskGroup` (uma task por model) — isso roda no `dag-processor`, não no `scheduler`. Sem o volume aqui, a UI só refletiria um model novo depois de rebuild.
- `pipeline/` não precisa estar no `dag-processor`: o import de `ingestion.*` acontece dentro da função da task (lazy), não no nível do módulo da DAG — o parse não toca nesse código.

Por que o volume nomeado `dbt_packages` por cima do bind mount: essa pasta está no `.gitignore` (não existe no disco do dev a menos que ele já tenha rodado `dbt deps` localmente via Poetry) e no `.dockerignore` (passo 4). Sem esse volume extra, o bind mount da pasta pai (vinda do host, sem `dbt_packages`) mascararia o `dbt_packages` que o `RUN dbt deps` instalou na imagem (passo 3), e o `dbt run` quebraria por falta de packages. O Docker, ao criar um named volume novo num path em que a imagem já tem conteúdo, copia esse conteúdo da imagem pra dentro do volume na primeira vez — efeito prático: o `dbt_packages` do build continua visível dentro do container mesmo com o resto da pasta vindo do host.

⚠️ Essa cópia automática só acontece **uma vez**, na criação do volume. Se `packages.yml`/`package-lock.yml` mudar e a imagem for rebuildada com packages diferentes, o volume nomeado já existente continua com o conteúdo antigo — remover o volume (`docker volume rm` ou `astro dev kill`) antes do próximo `astro dev start` pra ele repopular.

⚠️ Os paths de destino dos volumes têm que casar exatamente com os do `COPY` no Dockerfile (`/usr/local/airflow/pipeline`, `/usr/local/airflow/dbt_food_cost_monitor`) — se esses paths mudarem no Dockerfile, esse arquivo precisa acompanhar.

**`env_file`** é uma diretiva diferente da substituição `${VAR}` dentro do próprio compose — ela carrega pares `KEY=VALUE` de um arquivo e injeta como variável de ambiente real dentro do container, igual `--env-file` do `docker run`. `../.env` aponta pro `.env` da raiz do repo (path resolvido relativo a este arquivo, que mora em `airflow_project/`) — o mesmo arquivo que o Poetry já usa localmente, sem segunda cópia. Com isso:
- O `dbt` (Plano B) se autentica do mesmo jeito que já funciona hoje via Poetry — boto3 lê `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` do ambiente automaticamente, sem o `profiles.yml` precisar referenciá-las.
- `pipeline_config.py` já faz `load_dotenv()` + `os.getenv(...)`; dentro do container o `load_dotenv()` não acha nenhum `.env` (está no `.dockerignore`) e cai pro `os.getenv()` puro, já populado pelo `env_file`.

⚠️ Isso torna esse arquivo, que começou como otimização opcional de rebuild, também necessário pras credenciais chegarem nos containers locais. Não é uma fragilidade nova: `airflow_settings.yaml` (descartado, passo 5) seria igualmente um artefato manual de dev local, gitignorado e nunca usado em produção — só estamos escolhendo qual desses arquivos carrega essa responsabilidade.

Validação: `astro dev bash --scheduler "printenv AWS_ACCESS_KEY_ID"` (confirma que a variável chegou) e `astro dev bash --scheduler "ls -al /usr/local/airflow/pipeline"` (confirma o volume), ou editar um arquivo local e checar se reflete sem rebuild.

## 8. Validação local

```bash
cd airflow_project
astro dev start                   # builda a imagem e sobe webserver/scheduler/postgres
```

Checklist:
- Build não falha no `dbt deps` nem na instalação do Cosmos.
- DAG `food_delivery_cost_monitor` aparece na UI sem erro de import (`Dag Import Errors`).
- `dbt_pipeline` aparece expandido como `TaskGroup` com uma task por model/test (sinal de que o Cosmos parseou o manifest certo).
- Rodar a DAG manualmente e confirmar que a task do primeiro model dbt (`stg_job_logs`) realmente conecta no Athena (erro de credencial apareceria aqui primeiro — nesse fluxo via `env_file` + Plano B, o suspeito mais provável é variável ausente no `.env` da raiz, não o `InvalidClientTokenId` da issue #996, que só afetava o Plano A já descartado).

## 9. Para depois (não bloqueia o setup inicial)

- Credenciais de produção no Astro Cloud (ver nota de produção no passo 5).
- Alertas de falha (`on_failure_callback` ou Slack/email no nível da DAG).

## Referências

- [Use Git submodules with an Astro project — Astronomer Documentation](https://www.astronomer.io/docs/astro/best-practices/git-submodules)
- [Deploy dbt projects to Astro — Astronomer Documentation](https://www.astronomer.io/docs/astro/deploy-dbt-project)
- [AthenaAccessKey — Astronomer Cosmos documentation](https://astronomer.github.io/astronomer-cosmos/profiles/AthenaAccessKey.html)
- [AthenaAccessKeyProfileMapping does not work as expected locally · Issue #996 · astronomer/astronomer-cosmos](https://github.com/astronomer/astronomer-cosmos/issues/996)
- [Add support to Airflow 3.2.0 · Issue #2403 · astronomer/astronomer-cosmos](https://github.com/astronomer/astronomer-cosmos/issues/2403)
- [Configure airflow_settings.yaml (local development only) — Astronomer Documentation](https://www.astronomer.io/docs/astro/cli/develop-project#configure-airflow_settingsyaml-local-development-only)
- [Run your Astro project in a local Airflow environment with the CLI — Astronomer Documentation](https://www.astronomer.io/docs/astro/cli/run-airflow-locally)
- [Volumes — Docker Docs](https://docs.docker.com/engine/storage/volumes/)
