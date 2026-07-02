FROM astrocrpublic.azurecr.io/runtime:3.2-4

# variáveis de ambiente que corrigem o problema de congelamento do numpy quando o scheduler faz o fork para rodar a dag
ENV OPENBLAS_NUM_THREADS=1
ENV OMP_NUM_THREADS=1
ENV MKL_NUM_THREADS=1
ENV NUMEXPR_NUM_THREADS=1

USER root
RUN apt-get update && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*

# venv isolada só para o dbt (mesmas faixas de versão do pyproject.toml)
USER astro
RUN python -m venv /usr/local/airflow/dbt_venv && \
    /usr/local/airflow/dbt_venv/bin/pip install --no-cache-dir \
    "dbt-core>=1.11.11,<2.0.0" \
    "dbt-athena>=1.10.1,<2.0.0"

# só os manifestos de dependência primeiro — cache de camada: o dbt deps só reroda
# quando packages.yml/package-lock.yml mudam, não a cada edição de model
COPY dbt_food_cost_monitor/packages.yml dbt_food_cost_monitor/package-lock.yml /usr/local/airflow/dbt_food_cost_monitor/
RUN /usr/local/airflow/dbt_venv/bin/dbt deps \
    --project-dir /usr/local/airflow/dbt_food_cost_monitor

# dbt_packages está no .dockerignore (passo 4): essa COPY não sobrescreve o que
# o dbt deps acabou de instalar
COPY dbt_food_cost_monitor/ /usr/local/airflow/dbt_food_cost_monitor/

# pacote pipeline/ reutilizado pelas tasks de ingestão
COPY pipeline/ /usr/local/airflow/pipeline/
ENV PYTHONPATH="${PYTHONPATH}:/usr/local/airflow/pipeline"
