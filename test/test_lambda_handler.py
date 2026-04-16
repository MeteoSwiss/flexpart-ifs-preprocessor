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
    _parse_event_records,
    lambda_handler,
)

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
# _parse_event_records
# ===========================================================================


class TestParseEventRecords:
    """Tests that only relevant stream/feed combinations are returned."""

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
    def test_filtering(self, filename, expected_count):
        event = _make_event(_make_kafka_record(OBJECT_KEY, filename))
        result = _parse_event_records(event)
        assert len(result) == expected_count

    def test_multiple_records_parsed(self):
        r1 = _make_kafka_record(OBJECT_KEY, F2_FILENAME)
        r2 = _make_kafka_record(OBJECT_KEY, F1_FILENAME)
        event = _make_event(r1, r2)
        result = _parse_event_records(event)
        assert len(result) == 2

    def test_domain_set_on_returned_files(self):
        event = _make_event(_make_kafka_record(OBJECT_KEY, F2_FILENAME))
        result = _parse_event_records(event)
        assert result[0].domain == Feed.F2

    def test_multiple_partitions_all_parsed(self):
        """Records from different Kafka partitions must all be returned."""
        event = {
            "records": {
                "partition-0": [_make_kafka_record(OBJECT_KEY, F2_FILENAME)],
                "partition-1": [_make_kafka_record(OBJECT_KEY, F1_FILENAME)],
            }
        }
        result = _parse_event_records(event)
        assert len(result) == 2

    def test_empty_event_returns_empty_list(self):
        event = {"records": {}}
        result = _parse_event_records(event)
        assert result == []

    def test_mixed_valid_and_invalid_records(self):
        valid = _make_kafka_record(OBJECT_KEY, F2_FILENAME)
        invalid = _make_kafka_record(OBJECT_KEY, "xxx_f3_unknown_20260331T060000Z_20260403T080000Z_74h")
        event = _make_event(valid, invalid)
        result = _parse_event_records(event)
        assert len(result) == 1
        assert result[0].domain == Feed.F2


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
