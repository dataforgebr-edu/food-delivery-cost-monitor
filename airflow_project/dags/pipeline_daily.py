from airflow.sdk import dag, get_current_context, task
from cosmos import (
    DbtTaskGroup,
    ExecutionConfig,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
)
from cosmos.constants import ExecutionMode
from pendulum import datetime

DBT_PROJECT_DIR = "/usr/local/airflow/dbt_food_cost_monitor"
DBT_VENV_BIN = "/usr/local/airflow/dbt_venv/bin/dbt"

profile_config = ProfileConfig(
    profile_name="dbt_food_cost_monitor",
    target_name="dev",
    profiles_yml_filepath=f"{DBT_PROJECT_DIR}/profiles.yml",
)
# from cosmos.profiles import AthenaAccessKeyProfileMapping
# profile_mapping=AthenaAccessKeyProfileMapping(conn_id="aws_athena"),

execution_config = ExecutionConfig(
    execution_mode=ExecutionMode.LOCAL, dbt_executable_path=DBT_VENV_BIN
)
render_config = RenderConfig(dbt_deps=False)


@dag(
    dag_id="food_delivery_cost_monitor",
    schedule="@daily",
    start_date=datetime(2026, 6, 24),
    catchup=False,
)
def pipeline_daily():

    @task(task_id="cria_infraestrutura")
    def create_infra():
        from infra.athena_setup import main

        main()

    @task(task_id="gerador_dados")
    def generate_data():
        from ingestion.generate_synthetic_data import generate_for_date

        context = get_current_context()
        generate_for_date(context["logical_date"].date())

    @task(task_id="upload_s3")
    def upload_to_s3():
        from ingestion.upload_to_s3 import main

        main()

    dbt_pipeline = DbtTaskGroup(
        group_id="dbt_pipeline",
        project_config=ProjectConfig(dbt_project_path=DBT_PROJECT_DIR),
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=render_config,
        operator_args={
            "install_deps": False,
        },
    )

    @task(task_id="termino_pipeline")
    def notificacao_dag():
        print("Pipeline concluído!")

    (
        create_infra()
        >> generate_data()
        >> upload_to_s3()
        >> dbt_pipeline
        >> notificacao_dag()
    )


pipeline_daily()
