"""Unit tests for the Lambda handler and Kafka event parsing.

Why these exist alongside the integration tests
-----------------------------------------------
The integration test fires one well-formed F2 event and checks the end result
in S3/DynamoDB.  These unit tests cover: all stream/feed filtering combinations
(S4Y/S5Y/S6Y × F1/F2/invalid), multiple Kafka partitions, empty events, the
no-processable-steps guard, and multi-file orchestration — branches that
are impractical to trigger end-to-end without setting up many different DB
states.
"""

from unittest.mock import MagicMock, patch

import pytest

from flexpart_ifs_preprocessor.domain.data_model import Feed, IFSForecastFile, Stream
from flexpart_ifs_preprocessor.flexpart_ifs_preprocessor import (
    _kafka_event_to_input_data_aggregator_event,
    lambda_handler,
)
from aws_lambda_powertools.utilities.kafka import ConsumerRecords

from conftest import (
    F2_FILENAME,
    OBJECT_KEY,
    _make_kafka_record,
    _make_lambda_event,
)

F1_FILENAME = "s4y_f1_ifs-ens-cf_od_scda_fc_20260331T060000Z_20260403T080000Z_74h"

def _make_event(*records: dict) -> dict:
    """Thin wrapper so tests can pass records as positional args."""
    return _make_lambda_event(list(records))


# ===========================================================================
# _kafka_event_to_input_data_aggregator_event
# ===========================================================================


class TestKafkaEventToInputDataAggregatorEvent:
    def test_decodes_base64_correctly(self):
        record = _make_kafka_record(OBJECT_KEY, F2_FILENAME)
        ev = _kafka_event_to_input_data_aggregator_event(record)
        assert ev.object_key == OBJECT_KEY
        assert ev.filename == F2_FILENAME

    def test_extracts_stream(self):
        record = _make_kafka_record(OBJECT_KEY, F2_FILENAME)
        ev = _kafka_event_to_input_data_aggregator_event(record)
        assert ev.stream == Stream.S4Y

    def test_extracts_feed(self):
        record = _make_kafka_record(OBJECT_KEY, F2_FILENAME)
        ev = _kafka_event_to_input_data_aggregator_event(record)
        assert ev.feed == Feed.F2

    @pytest.mark.parametrize("filename, expected_stream", [
        ("s4y_f2_ifs-ens-cf_od_scda_fc_20260331T060000Z_20260403T080000Z_74h", Stream.S4Y),
        ("s5y_f1_ifs-ens-cf_od_scda_fc_20260331T060000Z_20260403T080000Z_74h", Stream.S5Y),
        ("s6y_f2_ifs-ens-cf_od_scda_fc_20260331T060000Z_20260403T080000Z_74h", Stream.S6Y),
    ])
    def test_stream_variants(self, filename, expected_stream):
        record = _make_kafka_record(OBJECT_KEY, filename)
        ev = _kafka_event_to_input_data_aggregator_event(record)
        assert ev.stream == expected_stream


# ===========================================================================
# Stream/feed filtering (tested via lambda_handler → write_product_index)
# ===========================================================================


class TestParseEventRecords:
    """Tests that only relevant stream/feed combinations reach write_product_index."""

    @pytest.fixture(autouse=True)
    def _suppress_side_effects(self):
        with patch("flexpart_ifs_preprocessor.flexpart_ifs_preprocessor.get_steps_to_process", return_value=([], [])), \
             patch("flexpart_ifs_preprocessor.flexpart_ifs_preprocessor.run_preprocessing"):
            yield

    @pytest.fixture
    def write_mock(self):
        with patch("flexpart_ifs_preprocessor.flexpart_ifs_preprocessor.write_product_index") as m:
            yield m

    @pytest.mark.parametrize("filename, expected_count", [
        # S4Y + F2 → accepted
        ("s4y_f2_ifs-ens-cf_od_scda_fc_20260331T060000Z_20260403T080000Z_74h", 1),
        # S4Y + F1 → accepted
        ("s4y_f1_ifs-ens-cf_od_scda_fc_20260331T060000Z_20260403T080000Z_74h", 1),
        # S5Y + F2 → accepted
        ("s5y_f2_ifs-ens-cf_od_scda_fc_20260331T060000Z_20260403T080000Z_74h", 1),
        # S6Y + F1 → accepted
        ("s6y_f1_ifs-ens-cf_od_scda_fc_20260331T060000Z_20260403T080000Z_74h", 1),
        # Unknown stream → filtered out
        ("xxx_f2_ifs-ens-cf_od_scda_fc_20260331T060000Z_20260403T080000Z_74h", 0),
        # Unknown feed → filtered out
        ("s4y_f3_ifs-ens-cf_od_scda_fc_20260331T060000Z_20260403T080000Z_74h", 0),
    ])
    def test_filtering(self, write_mock, filename, expected_count):
        lambda_handler(_make_event(_make_kafka_record(OBJECT_KEY, filename)), MagicMock())
        assert write_mock.call_count == expected_count

    def test_multiple_records_parsed(self, write_mock):
        event = _make_event(_make_kafka_record(OBJECT_KEY, F2_FILENAME), _make_kafka_record(OBJECT_KEY, F1_FILENAME))
        lambda_handler(event, MagicMock())
        assert write_mock.call_count == 2

    def test_domain_set_on_file_passed_to_write(self, write_mock):
        lambda_handler(_make_event(_make_kafka_record(OBJECT_KEY, F2_FILENAME)), MagicMock())
        assert write_mock.call_args[0][0].domain == Feed.F2

    def test_multiple_partitions_all_parsed(self, write_mock):
        """Records from different Kafka partitions must all reach write_product_index."""
        event = {
            "records": {
                "partition-0": [_make_kafka_record(OBJECT_KEY, F2_FILENAME)],
                "partition-1": [_make_kafka_record(OBJECT_KEY, F1_FILENAME)],
            }
        }
        lambda_handler(event, MagicMock())
        assert write_mock.call_count == 2

    def test_empty_event_does_not_call_write(self, write_mock):
        lambda_handler({"records": {}}, MagicMock())
        write_mock.assert_not_called()

    def test_mixed_valid_and_invalid_records(self, write_mock):
        event = _make_event(
            _make_kafka_record(OBJECT_KEY, F2_FILENAME),
            _make_kafka_record(OBJECT_KEY, "xxx_f3_unknown_20260331T060000Z_20260403T080000Z_74h"),
        )
        lambda_handler(event, MagicMock())
        assert write_mock.call_count == 1
        assert write_mock.call_args[0][0].domain == Feed.F2


# ===========================================================================
# lambda_handler
# ===========================================================================


class TestLambdaHandler:
    """Tests for the top-level lambda_handler orchestration."""

    def _make_mocks(self):
        return {
            "write_product_index": MagicMock(),
            "get_steps_to_process": MagicMock(return_value=([], [])),
            "run_preprocessing": MagicMock(),
            "update_product_index_processed": MagicMock(),
        }

    def test_no_preprocessing_when_no_processable_steps(self):
        mocks = self._make_mocks()
        event = _make_event(_make_kafka_record(OBJECT_KEY, F2_FILENAME))
        with patch("flexpart_ifs_preprocessor.flexpart_ifs_preprocessor.write_product_index", mocks["write_product_index"]), \
             patch("flexpart_ifs_preprocessor.flexpart_ifs_preprocessor.get_steps_to_process", mocks["get_steps_to_process"]), \
             patch("flexpart_ifs_preprocessor.flexpart_ifs_preprocessor.run_preprocessing", mocks["run_preprocessing"]):
            lambda_handler(event, MagicMock())
        mocks["run_preprocessing"].assert_not_called()

    def test_run_preprocessing_called_for_each_processable_step(self):
        mocks = self._make_mocks()
        real_file = IFSForecastFile(object_key=OBJECT_KEY, filename=F2_FILENAME)
        real_prev = IFSForecastFile(object_key=OBJECT_KEY, filename=F2_FILENAME)
        real_zeros = [
            IFSForecastFile(object_key=OBJECT_KEY, filename=F2_FILENAME),
            IFSForecastFile(object_key=OBJECT_KEY, filename=F2_FILENAME),
        ]
        mocks["get_steps_to_process"].return_value = ([(real_file, real_prev)], real_zeros)

        event = _make_event(_make_kafka_record(OBJECT_KEY, F2_FILENAME))
        with patch("flexpart_ifs_preprocessor.flexpart_ifs_preprocessor.write_product_index", mocks["write_product_index"]), \
             patch("flexpart_ifs_preprocessor.flexpart_ifs_preprocessor.get_steps_to_process", mocks["get_steps_to_process"]), \
             patch("flexpart_ifs_preprocessor.flexpart_ifs_preprocessor.run_preprocessing", mocks["run_preprocessing"]), \
             patch("flexpart_ifs_preprocessor.flexpart_ifs_preprocessor.update_product_index_processed", mocks["update_product_index_processed"]):
            lambda_handler(event, MagicMock())

        mocks["run_preprocessing"].assert_called_once_with(real_file, real_prev, real_zeros, 1)

    def test_multiple_files_in_event_each_processed_independently(self):
        mocks = self._make_mocks()
        r1 = _make_kafka_record(OBJECT_KEY, F2_FILENAME)
        r2 = _make_kafka_record(OBJECT_KEY, F1_FILENAME)
        event = _make_event(r1, r2)

        with patch("flexpart_ifs_preprocessor.flexpart_ifs_preprocessor.write_product_index", mocks["write_product_index"]), \
             patch("flexpart_ifs_preprocessor.flexpart_ifs_preprocessor.get_steps_to_process", mocks["get_steps_to_process"]), \
             patch("flexpart_ifs_preprocessor.flexpart_ifs_preprocessor.run_preprocessing", mocks["run_preprocessing"]), \
             patch("flexpart_ifs_preprocessor.flexpart_ifs_preprocessor.update_product_index_processed", mocks["update_product_index_processed"]):
            lambda_handler(event, MagicMock())

        assert mocks["write_product_index"].call_count == 2
        assert mocks["get_steps_to_process"].call_count == 2

    def test_filtered_records_do_not_trigger_writes(self):
        """Records with unknown stream/feed should be silently discarded."""
        mocks = self._make_mocks()
        # Unknown stream: xxx
        bad_record = _make_kafka_record(OBJECT_KEY, "xxx_f3_unknown_20260331T060000Z_20260403T080000Z_74h")
        event = _make_event(bad_record)

        with patch("flexpart_ifs_preprocessor.flexpart_ifs_preprocessor.write_product_index", mocks["write_product_index"]), \
             patch("flexpart_ifs_preprocessor.flexpart_ifs_preprocessor.get_steps_to_process", mocks["get_steps_to_process"]), \
             patch("flexpart_ifs_preprocessor.flexpart_ifs_preprocessor.run_preprocessing", mocks["run_preprocessing"]):
            lambda_handler(event, MagicMock())

        mocks["write_product_index"].assert_not_called()

    def test_returns_empty_batch_item_failures_on_success(self):
        mocks = self._make_mocks()
        event = _make_event(_make_kafka_record(OBJECT_KEY, F2_FILENAME))
        with patch("flexpart_ifs_preprocessor.flexpart_ifs_preprocessor.write_product_index", mocks["write_product_index"]), \
             patch("flexpart_ifs_preprocessor.flexpart_ifs_preprocessor.get_steps_to_process", mocks["get_steps_to_process"]), \
             patch("flexpart_ifs_preprocessor.flexpart_ifs_preprocessor.run_preprocessing", mocks["run_preprocessing"]):
            result = lambda_handler(event, MagicMock())
        assert result == {"batchItemFailures": []}

    def test_returns_batch_item_failure_when_record_raises(self):
        mocks = self._make_mocks()
        mocks["write_product_index"].side_effect = RuntimeError("boom")
        record = {**_make_kafka_record(OBJECT_KEY, F2_FILENAME), "eventID": "partition-0:42"}
        event = _make_event(record)
        with patch("flexpart_ifs_preprocessor.flexpart_ifs_preprocessor.write_product_index", mocks["write_product_index"]), \
             patch("flexpart_ifs_preprocessor.flexpart_ifs_preprocessor.get_steps_to_process", mocks["get_steps_to_process"]), \
             patch("flexpart_ifs_preprocessor.flexpart_ifs_preprocessor.run_preprocessing", mocks["run_preprocessing"]):
            result = lambda_handler(event, MagicMock())
        assert result == {"batchItemFailures": [{"itemIdentifier": "partition-0:42"}]}
