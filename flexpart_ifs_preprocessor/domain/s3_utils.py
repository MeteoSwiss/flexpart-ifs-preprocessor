import logging
import os
import typing
from pathlib import Path
import uuid

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.client import BaseClient
from botocore.exceptions import ClientError

from flexpart_ifs_preprocessor import CONFIG
from flexpart_ifs_preprocessor.domain.data_model import InputDataAggregatorEvent, IFSForecastFile

logger = logging.getLogger(__name__)

def download_file(file: IFSForecastFile, target_dir: Path) -> None:
    sts_client = boto3.client('sts')
    assumed_role = sts_client.assume_role(
        RoleArn=os.environ['SOURCE_ROLE_ARN'],
        RoleSessionName=f'product_publisher_{str(uuid.uuid4())}' # TODO check this RoleSessionName
    )
    credentials = assumed_role['Credentials']
    target_s3_client = boto3.client(
        's3',
        aws_access_key_id=credentials['AccessKeyId'],
        aws_secret_access_key=credentials['SecretAccessKey'],
        aws_session_token=credentials['SessionToken']
    )

    # create target path if not exists including its parents
    target_dir.mkdir(parents=True, exist_ok=True)
    target_path = target_dir / file.filename

    if target_path.exists():
        logger.debug(f"File already exists, skipping download: {target_path}")
        return

    # download the file from S3 bucket
    target_s3_client.download_file(
        os.environ['SOURCE_S3_BUCKET_ARN'],
        file.object_key,
        target_path
    )

    logger.info('Object "%s" downloaded at %s', file.object_key, target_path)

def upload_to_s3(file_path: Path, object_key: str) -> None:
    s3_client = boto3.client('s3')
    s3_client.upload_file(str(file_path), os.environ['TARGET_S3_BUCKET_NAME'], object_key)
    logger.info(f"Uploaded {file_path} to s3://{os.environ['TARGET_S3_BUCKET_NAME']}/{object_key}")
