"""Unit tests for flexpart_ifs_preprocessor.domain.db_utils.

These unit tests cover the complex branching in get_steps_to_process (missing
predecessor, already-processed skip, multiple pending steps, no step-zero)
and the exact DynamoDB item shape written by write_product_index.
"""

from datetime import datetime, timezone, UTC
from unittest.mock import patch

import boto3
import pytest
from moto import mock_aws

from flexpart_ifs_preprocessor.domain.data_model import Feed, IFSForecastFile
from flexpart_ifs_preprocessor.domain.db_utils import (
    dynamodb_item_to_ifs_forecast_file,
    get_steps_to_process,
    update_product_index_processed,
    write_product_index,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

FORECAST_REF_TIME = datetime(2026, 3, 31, 6, 0, 0, tzinfo=timezone.utc)
REF_TIME_KEY = int(FORECAST_REF_TIME.timestamp())

_FILENAME_TEMPLATE = "s4y_f2_ifs-ens-cf_od_scda_fc_20260331T060000Z_20260403T080000Z_{step}h"
def _make_file(step: int, processed: bool = False) -> IFSForecastFile:
    filename = _FILENAME_TEMPLATE.format(step=step)
    return IFSForecastFile(
        object_key=f"prefix/{filename}",
        filename=filename,
        forecast_ref_time=FORECAST_REF_TIME,
        step=step,
        processed=processed,
    )


def _put_items(table, items: list[tuple[int, str]]) -> None:
    """Insert a list of (step, status) items into *table*.

    Multiple entries with the same step get distinct ObjectKeys by varying the
    path prefix (``prefix/``, ``prefix2/``, …) so the DynamoDB composite primary
    key (ReferenceTimePartitionKey + ObjectKey) is never duplicated, while the
    FileName stays parseable by IFSForecastFile.
    """
    step_counts: dict[int, int] = {}
    for step, status in items:
        idx = step_counts.get(step, 0)
        step_counts[step] = idx + 1
        prefix = "prefix" if idx == 0 else f"prefix{idx + 1}"
        filename = _FILENAME_TEMPLATE.format(step=step)
        table.put_item(Item={
            "ReferenceTimePartitionKey": REF_TIME_KEY,
            "ObjectKey": f"{prefix}/{filename}",
            "ReferenceTime": str(FORECAST_REF_TIME),
            "LeadTime": step,
            "FileName": filename,
            "CreatedAt": 0,
            "Status": status,
        })


def _put_item(table, step: int, status: str = "PENDING") -> None:
    """Convenience wrapper for inserting a single item (no duplicate-step handling)."""
    _put_items(table, [(step, status)])


# ---------------------------------------------------------------------------
# dynamodb_item_to_ifs_forecast_file
# ---------------------------------------------------------------------------


class TestDynamodbItemToIFSForecastFile:
    @pytest.mark.parametrize("status, expected_processed", [
        ("PENDING", False),
        ("PROCESSED", True),
    ])
    def test_status_maps_to_processed(self, status, expected_processed):
        filename = _FILENAME_TEMPLATE.format(step=6)
        item = {
            "ReferenceTimePartitionKey": REF_TIME_KEY,
            "ObjectKey": f"prefix/{filename}",
            "LeadTime": 6,
            "FileName": filename,
            "Status": status,
        }
        f = dynamodb_item_to_ifs_forecast_file(item)
        assert f.processed == expected_processed

    def test_forecast_ref_time_roundtrip(self):
        filename = _FILENAME_TEMPLATE.format(step=6)
        item = {
            "ReferenceTimePartitionKey": REF_TIME_KEY,
            "ObjectKey": f"prefix/{filename}",
            "LeadTime": 6,
            "FileName": filename,
            "Status": "PENDING",
        }
        f = dynamodb_item_to_ifs_forecast_file(item)
        assert f.forecast_ref_time == FORECAST_REF_TIME

    def test_step_stored(self):
        filename = _FILENAME_TEMPLATE.format(step=12)
        item = {
            "ReferenceTimePartitionKey": REF_TIME_KEY,
            "ObjectKey": f"prefix/{filename}",
            "LeadTime": 12,
            "FileName": filename,
            "Status": "PENDING",
        }
        f = dynamodb_item_to_ifs_forecast_file(item)
        assert f.step == 12

    def test_object_key_stored(self):
        filename = _FILENAME_TEMPLATE.format(step=6)
        key = f"prefix/{filename}"
        item = {
            "ReferenceTimePartitionKey": REF_TIME_KEY,
            "ObjectKey": key,
            "LeadTime": 6,
            "FileName": filename,
            "Status": "PENDING",
        }
        f = dynamodb_item_to_ifs_forecast_file(item)
        assert f.object_key == key


# ---------------------------------------------------------------------------
# write_product_index
# ---------------------------------------------------------------------------


class TestWriteProductIndex:
    def test_writes_item_to_table(self):
        with mock_aws():
            ddb = boto3.resource("dynamodb", region_name="eu-central-1")
            table = _make_ddb_table(ddb)
            with patch("flexpart_ifs_preprocessor.domain.db_utils.db_client", return_value=ddb):
                f = _make_file(step=6)
                write_product_index(f)
                item = table.get_item(
                    Key={
                        "ReferenceTimePartitionKey": REF_TIME_KEY,
                        "ObjectKey": f.object_key,
                    }
                )["Item"]
                assert item["LeadTime"] == 6
                assert item["Status"] == "PENDING"
                assert item["FileName"] == f.filename
                assert "CreatedAt" in item


# ---------------------------------------------------------------------------
# get_steps_to_process
# ---------------------------------------------------------------------------


def _make_ddb_table(ddb):
    """Create the test DynamoDB table and return it."""
    return ddb.create_table(
        TableName="test-table",
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


class TestGetStepsToProcess:
    """Tests for the DynamoDB query + business logic in get_steps_to_process."""

    def _run(self, table, ddb, items: list[tuple[int, str]]) -> tuple:
        """Populate table with (step, status) items and call get_steps_to_process."""
        _put_items(table, items)
        with patch("flexpart_ifs_preprocessor.domain.db_utils.db_client", return_value=ddb):
            return get_steps_to_process(FORECAST_REF_TIME)

    def test_returns_empty_when_no_step_zero(self):
        with mock_aws():
            ddb = boto3.resource("dynamodb", region_name="eu-central-1")
            table = _make_ddb_table(ddb)
            # Only one step=0 file present (need 2)
            items, zeros = self._run(table, ddb, [(0, "PENDING"), (3, "PENDING")])
            assert items == []
            assert zeros == []

    def test_returns_empty_when_no_items(self):
        with mock_aws():
            ddb = boto3.resource("dynamodb", region_name="eu-central-1")
            table = _make_ddb_table(ddb)
            items, zeros = self._run(table, ddb, [])
            assert items == []
            assert zeros == []

    def test_skips_pending_item_when_previous_step_missing(self):
        with mock_aws():
            ddb = boto3.resource("dynamodb", region_name="eu-central-1")
            table = _make_ddb_table(ddb)
            # Two step=0 files, but step=6 has no step=3 predecessor
            items_to_process, zeros = self._run(
                table, ddb, [(0, "PROCESSED"), (0, "PROCESSED"), (6, "PENDING")]
            )
            assert items_to_process == []

    def test_returns_processable_step_with_predecessor(self):
        with mock_aws():
            ddb = boto3.resource("dynamodb", region_name="eu-central-1")
            table = _make_ddb_table(ddb)
            # Two step=0 files + step=3 (PROCESSED predecessor) + step=6 (PENDING)
            items_to_process, zeros = self._run(
                table, ddb,
                [(0, "PROCESSED"), (0, "PROCESSED"), (3, "PROCESSED"), (6, "PENDING")],
            )
            assert len(items_to_process) == 1
            current, prev = items_to_process[0]
            assert current.step == 6
            assert prev.step == 3

    def test_step_zero_files_returned(self):
        with mock_aws():
            ddb = boto3.resource("dynamodb", region_name="eu-central-1")
            table = _make_ddb_table(ddb)
            _, zeros = self._run(
                table, ddb,
                [(0, "PROCESSED"), (0, "PROCESSED"), (3, "PROCESSED"), (6, "PENDING")],
            )
            assert len(zeros) == 2
            assert all(z.step == 0 for z in zeros)

    def test_already_processed_items_not_re_queued(self):
        with mock_aws():
            ddb = boto3.resource("dynamodb", region_name="eu-central-1")
            table = _make_ddb_table(ddb)
            items_to_process, _ = self._run(
                table, ddb,
                [(0, "PROCESSED"), (0, "PROCESSED"), (3, "PROCESSED"), (6, "PROCESSED")],
            )
            assert items_to_process == []

    @pytest.mark.parametrize("pending_steps", [
        [6],
        [6, 9],
        [6, 9, 12],
    ])
    def test_multiple_pending_steps_all_queued(self, pending_steps):
        with mock_aws():
            ddb = boto3.resource("dynamodb", region_name="eu-central-1")
            table = _make_ddb_table(ddb)
            # Build a complete chain: 0, 3, 6, 9, ... all PROCESSED except the pending ones
            max_step = max(pending_steps)
            all_steps = list(range(0, max_step + 3, 3))
            statuses = [
                (s, "PENDING" if s in pending_steps else "PROCESSED")
                for s in all_steps
            ]
            # Add second step=0 (required by business logic)
            statuses.append((0, "PROCESSED"))
            items_to_process, _ = self._run(table, ddb, statuses)
            assert len(items_to_process) == len(pending_steps)


# ---------------------------------------------------------------------------
# update_product_index_processed
# ---------------------------------------------------------------------------


class TestUpdateProductIndexProcessed:
    def test_processed_at_is_set(self):
        with mock_aws():
            ddb = boto3.resource("dynamodb", region_name="eu-central-1")
            table = _make_ddb_table(ddb)
            _put_item(table, step=6, status="PENDING")
            filename = _FILENAME_TEMPLATE.format(step=6)
            object_key = f"prefix/{filename}"
            with patch("flexpart_ifs_preprocessor.domain.db_utils.db_client", return_value=ddb):
                update_product_index_processed(object_key, FORECAST_REF_TIME)
            item = table.get_item(
                Key={
                    "ReferenceTimePartitionKey": REF_TIME_KEY,
                    "ObjectKey": object_key,
                }
            )["Item"]
            now_ts = int(datetime.now(UTC).timestamp())
            # ProcessedAt should be within 5 seconds of now
            assert abs(int(item["ProcessedAt"]) - now_ts) < 5
