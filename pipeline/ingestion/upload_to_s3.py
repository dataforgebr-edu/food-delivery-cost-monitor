from typing import List, Any
import boto3
import boto3
from pathlib import Path
from botocore.exceptions import ClientError 
from infra.logi import get_logger
from pipeline_config import (
    API_KEY,
    API_SECRET,
    API_REGION,
    BUCKET_NAME,
    RAW_LAYER_PREFIX,
    DEFAULT_LOCAL_DIR,
    ATHENA_DATABASE,
    TABLE_JOBS_LOGS,
    RAW_LAYER_LOCATION

)

logger = get_logger(__name__)

def list_local_dates(local_dir: Path) -> List[str]:
    try:
        local_dates = set()
        for f in Path(local_dir).glob("date=*"):
            datas = str(f).split("=")[-1]
            local_dates.add(datas)
        return list(local_dates)
    except FileNotFoundError:
        logger.error(f"Diretório {local_dir} não encontrado")
        raise

def s3_list_uploaded_dates(client: Any, bucket: str, prefix: str) -> List[str]:
    try:
        response = client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix, 
        )
        
        uploaded_files = set()
        if "Contents" in response:
            for conts in response["Contents"]:
                if "date=" in conts["Key"]:
                    date_file = conts["Key"].split("date=")[1].split("/")[0]
                    uploaded_files.add(date_file)
        return list(uploaded_files)
    except ClientError as e:
        if e.response['Error']['Code'] == "NoSuchBucket":
            logger.error(f"Bucket {bucket} não encontrado")
            raise 
        else:
            raise
        
def s3_check_dates_to_upload(client: Any, bucket: str, prefix: str, local_dir: Path) -> List[str]:
    try:
        local_dates = list_local_dates(local_dir)
        uploaded_dates = s3_list_uploaded_dates(client, bucket, prefix)
        return [date for date in local_dates if date not in uploaded_dates]
    except Exception as e:
        logger.error(f"Erro ao verificar datas para upload: {e}")
        raise

def s3_upload_file(client: Any, bucket: str, local_dir: str, s3_prefix: str, file_date: str) -> bool:
    local_path = f"{local_dir}/date={file_date}/data.parquet"
    s3_path = f"{s3_prefix}date={file_date}/data.parquet"
    try:
        client.upload_file(
            Filename=local_path,
            Bucket=bucket,
            Key=s3_path
        )
        logger.info(f"Arquivo {local_path} enviado para s3://{bucket}/{s3_path}")
        return True
    except ClientError as e:
        logger.error(f"Erro ao enviar arquivo {local_path} para s3: {e}")
        raise

def glue_register_partition(database_name: str, table_name: str, file_date: str, s3_location: str) -> bool:
    try:
        final_location = f"{s3_location}date={file_date}/"
        glue_client = boto3.client(
            "glue",
            region_name=API_REGION,
            aws_access_key_id=API_KEY,
            aws_secret_access_key=API_SECRET
        )

        glue_client.batch_create_partition(
            DatabaseName=database_name,
            TableName=table_name,
            PartitionInputList=[
                {
                    "Values": [file_date],
                    "StorageDescriptor": {
                        "Location": final_location,
                        "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                        "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                        "SerdeInfo": {
                            "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                        },
                    },
                }
            ],
        )
        logger.info(f"Partição {file_date} registrada no Glue")
        return True
    except ClientError as e:
        raise

logger.info("Início")
s3_client = boto3.client(
    "s3",
    region_name=API_REGION,
    aws_access_key_id=API_KEY,
    aws_secret_access_key=API_SECRET
)
# resultado = glue_register_partition(ATHENA_DATABASE, TABLE_JOBS_LOGS, "2026-05-04", RAW_LAYER_LOCATION)
# print(resultado)
# result = s3_upload_file(s3_client, BUCKET_NAME, str(DEFAULT_LOCAL_DIR), RAW_LAYER_PREFIX, "2026-05-05")
# print(result)
# lista = list_local_dates(DEFAULT_LOCAL_DIR)
# print(lista)
# upload = list_uploaded_dates(s3_client, BUCKET_NAME, RAW_LAYER_PREFIX)
# print(upload)
# dts = check_dates_to_upload(s3_client, BUCKET_NAME, RAW_LAYER_PREFIX, DEFAULT_LOCAL_DIR)
# print(dts)
