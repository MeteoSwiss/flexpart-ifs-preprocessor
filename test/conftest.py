import base64
import json
from datetime import datetime, timezone
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

# ---------------------------------------------------------------------------
# Environment variables required by the application
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def set_test_env_vars(monkeypatch):
    monkeypatch.setenv("DYNAMODB_TABLE", "test-table")
    monkeypatch.setenv("SOURCE_ROLE_ARN", "arn:aws:iam::123456789012:role/test-role")
    monkeypatch.setenv("SOURCE_S3_BUCKET_ARN", "source-bucket")
    monkeypatch.setenv("TARGET_S3_BUCKET_NAME", "target-bucket")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "eu-central-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

RESOURCES_DIR = Path(__file__).parent / "resources"
GRIB_FILE = RESOURCES_DIR / "s4y_f2_ifs-ens-cf_od_scda_fc_20260331T060000Z_20260403T080000Z_74h"


@pytest.fixture(scope="session")
def grib_path() -> Path:
    """Return the path to the test GRIB file."""
    assert GRIB_FILE.exists(), f"Test GRIB file not found: {GRIB_FILE}"
    return GRIB_FILE


# ---------------------------------------------------------------------------
# IFSForecastFile helpers
# ---------------------------------------------------------------------------

F2_FILENAME = "s4y_f2_ifs-ens-cf_od_scda_fc_20260331T060000Z_20260403T080000Z_74h"
F1_FILENAME = "s4y_f1_ifs-ens-cf_od_scda_fc_20260331T060000Z_20260403T080000Z_74h"
OBJECT_KEY = "some/prefix/" + F2_FILENAME
FORECAST_REF_TIME = datetime(2026, 3, 31, 6, 0, 0, tzinfo=timezone.utc)


@pytest.fixture()
def f2_forecast_file():
    """A standard F2 (Europe) IFSForecastFile."""
    from flexpart_ifs_preprocessor.domain.data_model import IFSForecastFile
    return IFSForecastFile(object_key=OBJECT_KEY, filename=F2_FILENAME)


@pytest.fixture()
def f1_forecast_file():
    """A standard F1 (Global) IFSForecastFile."""
    from flexpart_ifs_preprocessor.domain.data_model import IFSForecastFile
    return IFSForecastFile(object_key=OBJECT_KEY, filename=F1_FILENAME)


# ---------------------------------------------------------------------------
# DynamoDB table
# ---------------------------------------------------------------------------

DYNAMODB_TABLE_NAME = "test-table"


@pytest.fixture()
def dynamodb_table():
    """Create a mocked DynamoDB table and return the boto3 Table resource."""
    with mock_aws():
        client = boto3.resource("dynamodb", region_name="eu-central-1")
        table = client.create_table(
            TableName=DYNAMODB_TABLE_NAME,
            KeySchema=[
                {"AttributeName": "ReferenceTimePartitionKey", "KeyType": "HASH"},
                {"AttributeName": "ObjectKey", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "ReferenceTimePartitionKey", "AttributeType": "N"},
                {"AttributeName": "ObjectKey", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        table.meta.client.get_waiter("table_exists").wait(TableName=DYNAMODB_TABLE_NAME)
        yield table


# ---------------------------------------------------------------------------
# Kafka / Lambda event helpers
# ---------------------------------------------------------------------------

def _make_kafka_record(object_key: str, filename: str) -> dict:
    payload = json.dumps({"objectStoreUuid": object_key, "fileName": filename})
    encoded = base64.b64encode(payload.encode()).decode()
    return {"value": encoded}


def _make_lambda_event(records: list[dict]) -> dict:
    return {"records": {"partition-0": records}}


@pytest.fixture()
def f2_kafka_record():
    return _make_kafka_record(OBJECT_KEY, F2_FILENAME)


@pytest.fixture()
def f2_lambda_event(f2_kafka_record):
    return _make_lambda_event([f2_kafka_record])


@pytest.fixture()
def make_kafka_record():
    """Factory fixture: make_kafka_record(object_key, filename) -> dict."""
    return _make_kafka_record


@pytest.fixture()
def make_lambda_event():
    """Factory fixture: make_lambda_event(records) -> dict."""
    return _make_lambda_event
