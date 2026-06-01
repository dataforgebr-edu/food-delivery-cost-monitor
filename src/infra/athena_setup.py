import boto3
import os
from botocore.exceptions import ClientError
from .logi import get_logger

from dotenv import load_dotenv

load_dotenv()

logger = get_logger(__name__)

api_key = os.getenv("AWS_ACCESS_KEY_ID")
api_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
api_region = os.getenv("AWS_REGION")
athena_db = os.getenv("ATHENA_DATABASE")
bucket_name = os.getenv("S3_BUCKET")
athena_output_location = f"s3://{bucket_name}/athena-results/"
table_jobs_logs = "job_logs"
raw_layer_location = f"s3://{bucket_name}/raw/{table_jobs_logs}/"   

def glue_check_database(database_name: str, client: boto3.client) -> str:
    try:
        response = client.get_database(Name=database_name)
        return response["Database"]["Name"]
    except ClientError as e:
        if e.response["Error"]["Code"] == "EntityNotFoundException":
            return None
        else:
            raise

def glue_check_table(table_name: str, database_name: str, client: boto3.client) -> str:
    try:
        response = client.get_table(DatabaseName=database_name, Name=table_name)
        return response["Table"]["Name"]
    except ClientError as e:
        if e.response["Error"]["Code"] == "EntityNotFoundException":
            return None
        else:
            raise

def glue_create_database(database_name: str, client: boto3.client) -> None:
    try:
        client.create_database(
            DatabaseInput={
                "Name": database_name,
                "Description": "Database para monitoramento de custos de recursos de dados."
            }
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "AlreadyExistsException":
            return None
        else:
            raise

def glue_create_raw_layer(table_name: str, database_name: str, storage_location: str, client: boto3.client) -> None:
    try:
        client.create_table(
            DatabaseName=database_name,
            TableInput={
                "Name": table_name,
                "TableType": "EXTERNAL_TABLE",
                "Parameters": {
                    "classification": "parquet",
                    "EXTERNAL": "TRUE"
                },
                'StorageDescriptor': {
                    'Columns': [
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
                    'Location': storage_location,
                    'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                    'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                    'SerdeInfo': {
                        'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
                        'Parameters': {
                            'serialization.format': '1'
                        }
                    }
                }
            }
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "AlreadyExistsException":
            return None
        else:
            raise

def main():
    glue_client = boto3.client(
        "glue",
        aws_access_key_id=api_key,
        aws_secret_access_key=api_secret,
        region_name=api_region
    ) 

    if glue_check_database(athena_db, glue_client) is None:
        glue_create_database(athena_db, glue_client)
        logger.info(f"Database '{athena_db}' criada com sucesso")
    else:
        logger.info(f"Database '{athena_db}' já existe")

    if glue_check_table(table_jobs_logs, athena_db, glue_client) is None:
        glue_create_raw_layer(table_jobs_logs, athena_db, raw_layer_location, glue_client)
        logger.info(f"Tabela '{table_jobs_logs}' criada com sucesso")
    else:
        logger.info(f"Tabela '{table_jobs_logs}' já existe")
    
if __name__ == "__main__":
    main()
