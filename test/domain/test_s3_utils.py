"""Tests for flexpart_ifs_preprocessor.domain.s3_utils."""

import json
import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import boto3
import pytest
from moto import mock_aws

from flexpart_ifs_preprocessor.domain.data_model import IFSForecastFile
from flexpart_ifs_preprocessor.domain.s3_utils import download_file, upload_to_s3

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SOURCE_BUCKET = "source-bucket"
TARGET_BUCKET = "target-bucket"
FILENAME = "s4y_f2_ifs-ens-cf_od_scda_fc_20260331T060000Z_20260403T080000Z_74h"
OBJECT_KEY = f"prefix/{FILENAME}"
DUMMY_CONTENT = b"dummy grib content"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def forecast_file() -> IFSForecastFile:
    return IFSForecastFile(object_key=OBJECT_KEY, filename=FILENAME)


@pytest.fixture()
def populated_source_bucket():
    """Create a mocked S3 source bucket pre-populated with a test object."""
    with mock_aws():
        s3 = boto3.client("s3", region_name="eu-central-1")
        s3.create_bucket(
            Bucket=SOURCE_BUCKET,
            CreateBucketConfiguration={"LocationConstraint": "eu-central-1"},
        )
        s3.put_object(Bucket=SOURCE_BUCKET, Key=OBJECT_KEY, Body=DUMMY_CONTENT)
        yield s3


# ---------------------------------------------------------------------------
# upload_to_s3
# ---------------------------------------------------------------------------


@pytest.fixture()
def s3_client_with_target_bucket():
    """Open a single mock_aws context, create the target bucket, yield (mock_context, s3_client)."""
    with mock_aws():
        s3 = boto3.client("s3", region_name="eu-central-1")
        s3.create_bucket(
            Bucket=TARGET_BUCKET,
            CreateBucketConfiguration={"LocationConstraint": "eu-central-1"},
        )
        yield s3


class TestUploadToS3:
    def test_file_is_uploaded(self, s3_client_with_target_bucket):
        s3 = s3_client_with_target_bucket
        with tempfile.NamedTemporaryFile(delete=False, suffix=".grib") as tmp:
            tmp.write(DUMMY_CONTENT)
            tmp_path = Path(tmp.name)
        try:
            upload_to_s3(tmp_path, "output/file.grib", TARGET_BUCKET)
            response = s3.get_object(Bucket=TARGET_BUCKET, Key="output/file.grib")
            assert response["Body"].read() == DUMMY_CONTENT
        finally:
            tmp_path.unlink(missing_ok=True)

    def test_metadata_attached(self, s3_client_with_target_bucket):
        s3 = s3_client_with_target_bucket
        metadata = {"model": "IFS", "date": "20260331", "step": 74}
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp.write(DUMMY_CONTENT)
            tmp_path = Path(tmp.name)
        try:
            upload_to_s3(tmp_path, "output/file.grib", TARGET_BUCKET, metadata)
            head = s3.head_object(Bucket=TARGET_BUCKET, Key="output/file.grib")
            stored_meta = head["Metadata"]
            # The metadata should carry a 'data' key with JSON privateMetadata
            assert "data" in stored_meta
            private = json.loads(stored_meta["data"])
            assert private["privateMetadata"]["model"] == "IFS"
        finally:
            tmp_path.unlink(missing_ok=True)

    def test_upload_without_metadata_succeeds(self, s3_client_with_target_bucket):
        s3 = s3_client_with_target_bucket
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp.write(DUMMY_CONTENT)
            tmp_path = Path(tmp.name)
        try:
            upload_to_s3(tmp_path, "output/no-meta.grib", TARGET_BUCKET)
            response = s3.get_object(Bucket=TARGET_BUCKET, Key="output/no-meta.grib")
            assert response["Body"].read() == DUMMY_CONTENT
        finally:
            tmp_path.unlink(missing_ok=True)

    @pytest.mark.parametrize("object_key", [
        "simple.grib",
        "nested/path/file.grib",
        "very/deep/nested/path/to/file.grib",
    ])
    def test_various_object_keys(self, object_key, s3_client_with_target_bucket):
        s3 = s3_client_with_target_bucket
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp.write(DUMMY_CONTENT)
            tmp_path = Path(tmp.name)
        try:
            upload_to_s3(tmp_path, object_key, TARGET_BUCKET)
            response = s3.get_object(Bucket=TARGET_BUCKET, Key=object_key)
            assert response["Body"].read() == DUMMY_CONTENT
        finally:
            tmp_path.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# download_file
# ---------------------------------------------------------------------------


class TestDownloadFile:
    def _make_mock_s3_client(self, tmp_path: Path, content: bytes = DUMMY_CONTENT):
        """Return a mock boto3 S3 client that writes content to the target path."""
        mock_client = MagicMock()

        def fake_download(bucket, key, dest):
            Path(dest).write_bytes(content)

        mock_client.download_file.side_effect = fake_download
        return mock_client

    def _make_mock_sts(self, mock_s3_client):
        """Return a mock STS client + patched boto3.client that injects our s3 mock."""
        mock_sts = MagicMock()
        mock_sts.assume_role.return_value = {
            "Credentials": {
                "AccessKeyId": "AK",
                "SecretAccessKey": "SK",
                "SessionToken": "ST",
            }
        }

        def fake_boto3_client(service, **kwargs):
            if service == "sts":
                return mock_sts
            if service == "s3":
                return mock_s3_client
            raise ValueError(f"Unexpected service: {service}")

        return fake_boto3_client

    def test_file_downloaded_to_target_dir(self, forecast_file, tmp_path):
        mock_s3 = self._make_mock_s3_client(tmp_path)
        with patch("flexpart_ifs_preprocessor.domain.s3_utils.boto3.client",
                   side_effect=self._make_mock_sts(mock_s3)):
            download_file(forecast_file, tmp_path)
        assert (tmp_path / FILENAME).read_bytes() == DUMMY_CONTENT

    def test_target_dir_created_if_missing(self, forecast_file, tmp_path):
        new_dir = tmp_path / "nested" / "dir"
        mock_s3 = self._make_mock_s3_client(new_dir)
        with patch("flexpart_ifs_preprocessor.domain.s3_utils.boto3.client",
                   side_effect=self._make_mock_sts(mock_s3)):
            download_file(forecast_file, new_dir)
        assert new_dir.exists()

    def test_skips_download_if_file_exists(self, forecast_file, tmp_path):
        # Pre-create the target file
        existing = tmp_path / FILENAME
        existing.write_bytes(b"already here")
        mock_s3 = self._make_mock_s3_client(tmp_path)
        with patch("flexpart_ifs_preprocessor.domain.s3_utils.boto3.client",
                   side_effect=self._make_mock_sts(mock_s3)):
            download_file(forecast_file, tmp_path)
        # download_file should not have been called on the S3 client
        mock_s3.download_file.assert_not_called()
        # Original content untouched
        assert existing.read_bytes() == b"already here"

    def test_sts_assume_role_called_with_correct_arn(self, forecast_file, tmp_path):
        mock_sts = MagicMock()
        mock_sts.assume_role.return_value = {
            "Credentials": {
                "AccessKeyId": "AK",
                "SecretAccessKey": "SK",
                "SessionToken": "ST",
            }
        }
        mock_s3 = self._make_mock_s3_client(tmp_path)

        def fake_boto3_client(service, **kwargs):
            if service == "sts":
                return mock_sts
            return mock_s3

        with patch("flexpart_ifs_preprocessor.domain.s3_utils.boto3.client",
                   side_effect=fake_boto3_client):
            download_file(forecast_file, tmp_path)

        call_kwargs = mock_sts.assume_role.call_args
        assert call_kwargs.kwargs["RoleArn"] == os.environ["SOURCE_ROLE_ARN"]
