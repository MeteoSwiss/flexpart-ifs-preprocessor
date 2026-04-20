import base64
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import boto3
import pytest
from moto import mock_aws


@pytest.fixture(autouse=True)
def set_test_env_vars(monkeypatch):
    monkeypatch.setenv("MAIN__DYNAMODB_TABLE_NAME", "test-table")
    monkeypatch.setenv("MAIN__SOURCE_ROLE_ARN", "arn:aws:iam::123456789012:role/test-role")
    monkeypatch.setenv("MAIN__SOURCE_S3_BUCKET_ARN", "source-bucket")
    monkeypatch.setenv("MAIN__TARGET_S3_BUCKET_NAME_GLOBAL", "target-bucket-global")
    monkeypatch.setenv("MAIN__TARGET_S3_BUCKET_NAME_EUROPE", "target-bucket-europe")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "eu-central-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")


RESOURCES_DIR = Path(__file__).parent / "resources"


# ---------------------------------------------------------------------------
# IFSForecastFile helpers
# ---------------------------------------------------------------------------

F2_FILENAME = "s4y_f2_ifs-ens-cf_od_scda_fc_20260331T060000Z_20260403T080000Z_74h"
OBJECT_KEY = "some/prefix/" + F2_FILENAME


# ---------------------------------------------------------------------------
# DynamoDB table
# ---------------------------------------------------------------------------

DYNAMODB_TABLE_NAME = "test-table"


# ---------------------------------------------------------------------------
# Kafka / Lambda event helpers
# ---------------------------------------------------------------------------

def _make_kafka_record(object_key: str, filename: str) -> dict:
    payload = json.dumps({"objectStoreUuid": object_key, "fileName": filename})
    encoded = base64.b64encode(payload.encode()).decode()
    return {"value": encoded}


def _make_lambda_event(records: list[dict]) -> dict:
    return {"records": {"partition-0": records}}


# ---------------------------------------------------------------------------
# 20260408 end-to-end integration scenario
# ---------------------------------------------------------------------------

OPER_REF_TIME    = datetime(2026, 4, 8, 0, 0, 0, tzinfo=timezone.utc)
OPER_REF_TIME_TS = int(OPER_REF_TIME.timestamp())
OPER_PREFIX      = "raw/s4y_f2/data"
OPER_SOURCE_BUCKET = "source-bucket"
OPER_TARGET_BUCKET_GLOBAL = "target-bucket-global"
OPER_TARGET_BUCKET_EU = "target-bucket-europe"

OPER_DA_0H  = "s4y_f2_ifs-da_od_oper_an_20260408T000000Z_20260408T000000Z_0h"
OPER_ENS_0H = "s4y_f2_ifs-ens-cf_od_oper_fc_20260408T000000Z_20260408T000000Z_0h"
OPER_ENS_2H = "s4y_f2_ifs-ens-cf_od_oper_fc_20260408T000000Z_20260408T020000Z_2h"
OPER_ENS_3H = "s4y_f2_ifs-ens-cf_od_oper_fc_20260408T000000Z_20260408T030000Z_3h"
OPER_ENS_4H = "s4y_f2_ifs-ens-cf_od_oper_fc_20260408T000000Z_20260408T040000Z_4h"

# The GRIB file used for unit tests that load real data.
GRIB_FILE = RESOURCES_DIR / OPER_ENS_4H


def _kafka_event(filename: str, prefix: str = OPER_PREFIX) -> dict:
    """Build a single-record Lambda Kafka event for *filename*."""
    object_key = f"{prefix}/{filename}"
    return _make_lambda_event([_make_kafka_record(object_key, filename)])


@dataclass
class IntegrationTest:
    """Holds live moto AWS handles for the 4h processing integration scenario."""
    table: object  # boto3 Table resource for the test DynamoDB table
    s3: object     # boto3 S3 client


@pytest.fixture()
def mocked_aws():
    """Start the moto mock_aws context for the duration of a test.

    All 4h scenario fixtures depend on this so they share one moto backend.
    """
    with mock_aws():
        yield


def _make_ddb_table(ddb):
    """Create the test DynamoDB table and return it."""
    return ddb.create_table(
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


@pytest.fixture(scope="function")
def oper_dynamodb_table_4h(mocked_aws, request):
    """Create and pre-populate the DynamoDB table for the 4h scenario.

    Inserts PENDING entries for DA-0h, ENS-0h and ENS-3h (the prerequisites
    that must already exist before the 4h file arrives).
    """

    entries = [
        (OPER_DA_0H, 0),
        (OPER_ENS_0H, 0),
        (OPER_ENS_3H, 3),
    ]

    ddb = boto3.resource("dynamodb", region_name="eu-central-1")
    table = _make_ddb_table(ddb)
    table.meta.client.get_waiter("table_exists").wait(TableName=DYNAMODB_TABLE_NAME)
    for filename, step in entries:
        table.put_item(Item={
            "ReferenceTimePartitionKey": OPER_REF_TIME_TS,
            "ObjectKey": f"{OPER_PREFIX}/{filename}",
            "ReferenceTime": str(OPER_REF_TIME),
            "LeadTime": step,
            "FileName": filename,
            "Domain": "EUROPE",
            "CreatedAt": 0,
            "Status_1h": "PENDING",
            "Status_3h": "PENDING"
        })
    yield table

@pytest.fixture(scope="function")
def oper_dynamodb_table_3h(mocked_aws, request):
    """Create and pre-populate the DynamoDB table for the 4h scenario.

    Inserts PENDING entries for DA-0h, ENS-0h, ENS-2h, (the prerequisites
    that must already exist before the 3h file arrives).
    """

    entries = [
        (OPER_DA_0H, 0),
        (OPER_ENS_0H, 0),
        (OPER_ENS_2H, 2),
    ]

    ddb = boto3.resource("dynamodb", region_name="eu-central-1")
    table = _make_ddb_table(ddb)
    table.meta.client.get_waiter("table_exists").wait(TableName=DYNAMODB_TABLE_NAME)
    for filename, step in entries:
        table.put_item(Item={
            "ReferenceTimePartitionKey": OPER_REF_TIME_TS,
            "ObjectKey": f"{OPER_PREFIX}/{filename}",
            "ReferenceTime": str(OPER_REF_TIME),
            "LeadTime": step,
            "FileName": filename,
            "Domain": "EUROPE",
            "CreatedAt": 0,
            "Status_1h": "PENDING",
            "Status_3h": "PENDING"
        })
    yield table


@pytest.fixture(scope="function")
def oper_s3_buckets(mocked_aws):
    """Create source/target S3 buckets and upload all four test GRIB files.

    Source bucket receives DA-0h, ENS-0h, ENS-3h and ENS-4h.
    Target bucket is left empty for the code under test to populate.
    """
    _resources = Path(__file__).parent / "resources"
    s3 = boto3.client("s3", region_name="eu-central-1")
    for bucket in (OPER_SOURCE_BUCKET, OPER_TARGET_BUCKET_EU, OPER_TARGET_BUCKET_GLOBAL):
        s3.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-central-1"},
        )
    for filename in (OPER_DA_0H, OPER_ENS_0H, OPER_ENS_2H, OPER_ENS_3H, OPER_ENS_4H):
        s3.upload_file(
            str(_resources / filename),
            OPER_SOURCE_BUCKET,
            f"{OPER_PREFIX}/{filename}",
        )
    yield s3


@pytest.fixture()
def aws_environment_3h(oper_dynamodb_table_3h, oper_s3_buckets):
    """Compose the 3h scenario fixtures into a single environment handle."""
    yield IntegrationTest(table=oper_dynamodb_table_3h, s3=oper_s3_buckets)

@pytest.fixture()
def aws_environment_4h(oper_dynamodb_table_4h, oper_s3_buckets):
    """Compose the 4h scenario fixtures into a single environment handle."""
    yield IntegrationTest(table=oper_dynamodb_table_4h, s3=oper_s3_buckets)
