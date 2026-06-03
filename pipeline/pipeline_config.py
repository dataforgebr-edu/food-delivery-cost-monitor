import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

def check_variables(name: str, default: str | None = None) -> str:
    valor = os.getenv(name, default)
    if valor is None:
        raise ValueError(f"Variável {name} não encontrada, valor default {default}")
    return valor

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
DEFAULT_LOCAL_DIR = Path(__file__).parent  / "DATA" / "raw"
