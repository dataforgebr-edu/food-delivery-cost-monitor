from pathlib import Path
from typing import List

from botocore.exceptions import ClientError
from infra.athena_setup import glue_register_partition
from infra.logi import get_logger
from mypy_boto3_s3 import S3Client
from pipeline_config import (
    BUCKET_NAME,
    DEFAULT_LOCAL_DIR,
    RAW_LAYER_PREFIX,
    create_s3_client,
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


def s3_list_uploaded_dates(client: S3Client, bucket: str, prefix: str) -> List[str]:
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
        if e.response["Error"]["Code"] == "NoSuchBucket":
            logger.error(f"Bucket {bucket} não encontrado")
            raise
        else:
            raise


def s3_check_dates_to_upload(
    client: S3Client, bucket: str, prefix: str, local_dir: Path
) -> List[str]:
    try:
        local_dates = list_local_dates(local_dir)
        uploaded_dates = s3_list_uploaded_dates(client, bucket, prefix)
        return [date for date in local_dates if date not in uploaded_dates]
    except Exception as e:
        logger.error(f"Erro ao verificar datas para upload: {e}")
        raise


def s3_upload_file(
    client: S3Client, bucket: str, local_dir: str, s3_prefix: str, file_date: str
):
    local_path = f"{local_dir}/date={file_date}/data.parquet"
    s3_path = f"{s3_prefix}date={file_date}/data.parquet"
    try:
        client.upload_file(Filename=local_path, Bucket=bucket, Key=s3_path)
        # logger.info(f"Arquivo {local_path} enviado para s3://{bucket}/{s3_path}")
    except ClientError as e:
        logger.error(f"Erro ao enviar arquivo {local_path} para s3: {e}")
        raise


def main():
    logger.info("Iniciando carga incremental")

    s3_client = create_s3_client()

    dates_to_upload = s3_check_dates_to_upload(
        s3_client, BUCKET_NAME, RAW_LAYER_PREFIX, DEFAULT_LOCAL_DIR
    )
    if len(dates_to_upload) == 0:
        logger.info("Nenhum arquivo novo para ser carregado.")
    else:
        logger.info(f"Lista de arquivos a serem carregados: {dates_to_upload}")

        for date in dates_to_upload:
            s3_upload_file(
                s3_client, BUCKET_NAME, str(DEFAULT_LOCAL_DIR), RAW_LAYER_PREFIX, date
            )
            glue_register_partition(file_date=date)
        logger.info(
            "Arquivos importados para o s3 e registrados no glueDataCatalog com sucesso!"
        )


if __name__ == "__main__":
    main()
