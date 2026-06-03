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
    DEFAULT_LOCAL_DIR

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

def list_uploaded_dates(client: Any, bucket: str, prefix: str) -> List[str]:
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
        
def check_dates_to_upload(client: Any, bucket: str, prefix: str, local_dir: Path) -> List[str]:
    try:
        local_dates = list_local_dates(local_dir)
        uploaded_dates = list_uploaded_dates(client, bucket, prefix)
        return [date for date in local_dates if date not in uploaded_dates]
    except Exception as e:
        logger.error(f"Erro ao verificar datas para upload: {e}")
        raise

logger.info("Início")
s3_client = boto3.client(
    "s3",
    region_name=API_REGION,
    aws_access_key_id=API_KEY,
    aws_secret_access_key=API_SECRET
)
lista = list_local_dates(DEFAULT_LOCAL_DIR)
print(lista)
upload = list_uploaded_dates(s3_client, BUCKET_NAME, RAW_LAYER_PREFIX)
print(upload)
dts = check_dates_to_upload(s3_client, BUCKET_NAME, RAW_LAYER_PREFIX, DEFAULT_LOCAL_DIR)
print(dts)
