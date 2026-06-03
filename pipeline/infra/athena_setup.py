from typing import Any

import boto3
from botocore.exceptions import ClientError

from infra.logi import get_logger

from pipeline_config import (
    API_KEY,
    API_SECRET,
    API_REGION,
    ATHENA_DATABASE,
    TABLE_JOBS_LOGS,
    RAW_LAYER_LOCATION,
)

logger = get_logger(__name__)


def glue_check_database(database_name: str, client: Any) -> str | None:
    try:
        response = client.get_database(Name=database_name)
        return response["Database"]["Name"]
    except ClientError as e:
        if e.response["Error"]["Code"] == "EntityNotFoundException":
            return None
        else:
            raise


def glue_check_table(table_name: str, database_name: str, client: Any) -> str | None:
    try:
        response = client.get_table(DatabaseName=database_name, Name=table_name)
        return response["Table"]["Name"]
    except ClientError as e:
        if e.response["Error"]["Code"] == "EntityNotFoundException":
            return None
        else:
            raise


def glue_create_database(database_name: str, client: Any) -> None:
    try:
        client.create_database(
            DatabaseInput={
                "Name": database_name,
                "Description": "Database para monitoramento de custos de recursos de dados.",
            }
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "AlreadyExistsException":
            return None
        else:
            raise


def glue_create_raw_layer(
    table_name: str, database_name: str, storage_location: str, client: Any
) -> None:
    try:
        client.create_table(
            DatabaseName=database_name,
            TableInput={
                "Name": table_name,
                "TableType": "EXTERNAL_TABLE",
                "Parameters": {"classification": "parquet", "EXTERNAL": "TRUE"},
                "StorageDescriptor": {
                    "Columns": [
                        {"Name": "job_id", "Type": "string"},
                        {"Name": "execution_date", "Type": "date"},
                        {"Name": "domain", "Type": "string"},
                        {"Name": "job_name", "Type": "string"},
                        {"Name": "duration_min", "Type": "double"},
                        {"Name": "dbu_consumed", "Type": "double"},
                        {"Name": "estimated_cost_usd", "Type": "double"},
                        {"Name": "status", "Type": "string"},
                        {"Name": "cluster_type", "Type": "string"},
                        {"Name": "created_at", "Type": "timestamp"},
                    ],
                    "Location": storage_location,
                    "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    "SerdeInfo": {
                        "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                        "Parameters": {"serialization.format": "1"},
                    },
                },
                "PartitionKeys": [
                    {
                        "Name": "date",
                        "Type": "string",
                        'Comment': 'Partição de filtro por data'
                    }
                ]
            },
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "AlreadyExistsException":
            return None
        else:
            raise

def main():
    glue_client = boto3.client(
        "glue",
        aws_access_key_id=API_KEY,
        aws_secret_access_key=API_SECRET,
        region_name=API_REGION,
    )

    if glue_check_database(ATHENA_DATABASE, glue_client) is None:
        glue_create_database(ATHENA_DATABASE, glue_client)
        logger.info(f"Database '{ATHENA_DATABASE}' criada com sucesso")
    else:
        logger.info(f"Database '{ATHENA_DATABASE}' já existe")

    if glue_check_table(TABLE_JOBS_LOGS, ATHENA_DATABASE, glue_client) is None:
        glue_create_raw_layer(
            TABLE_JOBS_LOGS, ATHENA_DATABASE, RAW_LAYER_LOCATION, glue_client
        )
        logger.info(f"Tabela '{TABLE_JOBS_LOGS}' criada com sucesso")
    else:
        logger.info(f"Tabela '{TABLE_JOBS_LOGS}' já existe")


if __name__ == "__main__":
    main()
