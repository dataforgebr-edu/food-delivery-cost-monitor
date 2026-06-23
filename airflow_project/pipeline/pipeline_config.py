import os
from pathlib import Path

import boto3
from dotenv import load_dotenv
from mypy_boto3_glue import GlueClient
from mypy_boto3_s3 import S3Client

load_dotenv()


def check_variables(name: str, default: str | None = None) -> str:
    valor = os.getenv(name, default)
    if valor is None:
        raise ValueError(f"Variável {name} não encontrada, valor default {default}")
    return valor


def create_s3_client() -> S3Client:
    return boto3.client(
        "s3",
        region_name=API_REGION,
        aws_access_key_id=API_KEY,
        aws_secret_access_key=API_SECRET,
    )


def create_glue_client() -> GlueClient:
    return boto3.client(
        "glue",
        aws_access_key_id=API_KEY,
        aws_secret_access_key=API_SECRET,
        region_name=API_REGION,
    )


# Variáveis de ambiente
API_KEY = check_variables("AWS_ACCESS_KEY_ID")
API_SECRET = check_variables("AWS_SECRET_ACCESS_KEY")
API_REGION = check_variables("AWS_REGION", "us-east-1")
ATHENA_DATABASE = check_variables("ATHENA_DATABASE", "cost_monitor")
BUCKET_NAME = check_variables("S3_BUCKET")

# Constantes
ATHENA_OUTPUT_LOCATION = f"s3://{BUCKET_NAME}/athena-results/"
TABLE_JOBS_LOGS = "job_logs"
RAW_LAYER_PREFIX = f"raw/{TABLE_JOBS_LOGS}/"
RAW_LAYER_LOCATION = f"s3://{BUCKET_NAME}/{RAW_LAYER_PREFIX}"
DEFAULT_LOCAL_DIR = Path(__file__).parent / "data" / "raw"
