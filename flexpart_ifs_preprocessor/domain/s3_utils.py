import logging
import os
import tempfile
import typing

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.client import BaseClient
from botocore.exceptions import ClientError

from flexpart_ifs_preprocessor import CONFIG

logger = logging.getLogger(__name__)

FileObject = dict[str, typing.Any]


class S3client:
    def __init__(self) -> None:
        self.s3_client_input = self._create_s3_client(
            endpoint_url=CONFIG.main.s3_buckets.input.endpoint_url,
            access_key=os.getenv("S3_ACCESS_KEY", ""),
            secret_key=os.getenv("S3_SECRET_KEY", ""),
        )

        self.s3_client_output = self._create_s3_client(
            endpoint_url=CONFIG.main.s3_buckets.output.endpoint_url,
            access_key=os.getenv("S3_ACCESS_KEY", ""),
            secret_key=os.getenv("S3_SECRET_KEY", ""),
        )

    def check_bucket(self, s3_client: BaseClient, bucket_name: str) -> None:
        try:
            s3_objects = s3_client.list_objects_v2(Bucket=bucket_name)
            if "Contents" not in s3_objects:
                logger.exception(f"No objects found in bucket {bucket_name}")
                raise ValueError(f"No objects found in bucket {bucket_name}")
            logger.debug(f"The bucket {bucket_name} is not empty.")
        except Exception as e:
            logger.exception(f"Error checking S3 bucket content: {e}")
            raise e

    def _create_s3_client(
        self, endpoint_url: str, access_key: str, secret_key: str
    ) -> BaseClient:
        """Create and return an S3 client."""
        return boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            use_ssl=True,
        )

    def download_file(self, file_info: FileObject) -> str:
        """Download a file from an S3 bucket to a temporary file."""
        temp_file = tempfile.NamedTemporaryFile(suffix=file_info["key"], delete=False)
        try:
            self.s3_client_input.download_file(
                CONFIG.main.s3_buckets.input.name,
                file_info["key"],
                temp_file.name,
                Config=TransferConfig(multipart_threshold=5 * 1024**3),
            )
            logger.info(
                f"Downloaded file from S3 to temporary file: {file_info['key']}"
            )
            file_info["temp_file"] = temp_file.name
            return temp_file.name
        except ClientError as e:
            logger.exception(
                f"Error downloading file {file_info['key']} to temporary file: {e}"
            )
            raise e

    def upload_file(self, local_path: str, key: str) -> None:
        """Upload a local file to an S3 bucket."""
        try:
            self.s3_client_output.upload_file(
                local_path, CONFIG.main.s3_buckets.output.name, key
            )
            logger.info(f"Uploaded file to S3: {key}")
        except ClientError as e:
            logger.exception(f"Error uploading file {local_path}: {e}")
            raise
