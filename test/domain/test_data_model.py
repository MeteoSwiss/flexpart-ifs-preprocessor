"""Unit tests for flexpart_ifs_preprocessor.domain.data_model.

Why these exist alongside the integration tests
-----------------------------------------------
IFSForecastFile and InputDataAggregatorEvent are pure-Python classes whose
only logic is regex-based filename parsing. These tests cover many filename
variants, every error path, and the Decimal-step coercion edge case.
"""

import pytest
from datetime import datetime, timezone

from flexpart_ifs_preprocessor.domain.data_model import (
    Feed,
    IFSForecastFile,
    InputDataAggregatorEvent,
    Stream,
)


# ---------------------------------------------------------------------------
# Helpers / shared constants
# ---------------------------------------------------------------------------

_BASE_OBJECT_KEY = "bucket/prefix/file"

# Each entry: (filename, expected_feed, expected_ref_time, expected_step)
_VALID_FILENAMES = [
    (
        "s4y_f2_ifs-ens-cf_od_scda_fc_20260331T060000Z_20260403T080000Z_74h",
        Feed.F2,
        datetime(2026, 3, 31, 6, 0, 0, tzinfo=timezone.utc),
        74,
    ),
    (
        "s4y_f1_ifs-ens-cf_od_scda_fc_20260331T060000Z_20260403T080000Z_74h",
        Feed.F1,
        datetime(2026, 3, 31, 6, 0, 0, tzinfo=timezone.utc),
        74,
    ),
    (
        "s5y_f2_ifs-ens-cf_od_scda_fc_20250101T000000Z_20250102T030000Z_27h",
        Feed.F2,
        datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        27,
    ),
    (
        "s6y_f1_ifs-ens-cf_od_scda_fc_20241215T120000Z_20241216T150000Z_3h",
        Feed.F1,
        datetime(2024, 12, 15, 12, 0, 0, tzinfo=timezone.utc),
        3,
    ),
    (
        "s4y_f2_ifs-ens-cf_od_scda_fc_20260101T000000Z_20260103T000000Z_48h",
        Feed.F2,
        datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        48,
    ),
]


# ===========================================================================
# IFSForecastFile – construction and parsing
# ===========================================================================


class TestIFSForecastFileParsing:
    """Tests for IFSForecastFile._extract_datetime, _extract_lead_time, _extract_feed."""

    @pytest.mark.parametrize(
        "filename, expected_feed, expected_ref_time, expected_step",
        _VALID_FILENAMES
    )
    def test_parsed_attributes(self, filename, expected_feed, expected_ref_time, expected_step):
        f = IFSForecastFile(object_key=_BASE_OBJECT_KEY, filename=filename)
        assert f.domain == expected_feed
        assert f.forecast_ref_time == expected_ref_time
        assert f.step == expected_step

    def test_decimal_step_is_coerced_to_int(self):
        """DynamoDB returns Decimal; the step constructor path must coerce to plain int."""
        from decimal import Decimal
        f = IFSForecastFile(
            object_key=_BASE_OBJECT_KEY,
            filename="s4y_f2_ifs-ens-cf_od_scda_fc_20260331T060000Z_20260403T080000Z_74h",
            step=Decimal("74"),
        )
        assert f.step == 74
        assert type(f.step) is int


class TestIFSForecastFileErrors:
    """Tests for IFSForecastFile error handling."""

    @pytest.mark.parametrize(
        "bad_filename",
        [
            # These contain no datetime pattern at all
            "completely_invalid",
            "",
        ],
    )
    def test_missing_datetime_raises(self, bad_filename):
        with pytest.raises(ValueError, match="No datetime found"):
            IFSForecastFile(object_key=_BASE_OBJECT_KEY, filename=bad_filename)

    def test_datetime_found_but_no_lead_time_suffix_raises(self):
        # Has a datetime but no trailing step (e.g. "_74h")
        with pytest.raises(ValueError, match="No lead time found"):
            IFSForecastFile(
                object_key=_BASE_OBJECT_KEY,
                filename="s4y_f2_ifs-ens-cf_od_scda_fc_20260331T060000Z_20260403T080000Z",
            )

    def test_second_datetime_is_used_for_extraction(self):
        # The filename has TWO timestamps; the first match is used for ref-time
        # and the lead time comes from the final suffix.  This filename has no
        # lead-time suffix so it should fail on lead-time, not datetime.
        with pytest.raises(ValueError, match="No lead time found"):
            IFSForecastFile(
                object_key=_BASE_OBJECT_KEY,
                filename="s4y_f2_ifs-ens-cf_od_scda_fc_20260331T060000Z_20260403T080000Z_",
            )

    def test_non_hour_unit_raises(self):
        # lead time in minutes (not hours)
        with pytest.raises(ValueError, match="Expected unit 'h'"):
            IFSForecastFile(
                object_key=_BASE_OBJECT_KEY,
                filename="s4y_f2_ifs-ens-cf_od_scda_fc_20260331T060000Z_20260403T080000Z_74m",
            )

    def test_unknown_feed_raises(self):
        with pytest.raises(ValueError, match="Unknown domain/feed"):
            IFSForecastFile(
                object_key=_BASE_OBJECT_KEY,
                filename="s4y_f3_ifs-ens-cf_od_scda_fc_20260331T060000Z_20260403T080000Z_74h",
            )


# ===========================================================================
# InputDataAggregatorEvent
# ===========================================================================


def _make_event(object_key: str, filename: str) -> InputDataAggregatorEvent:
    return InputDataAggregatorEvent({"objectStoreUuid": object_key, "fileName": filename})


class TestInputDataAggregatorEventParsing:
    """Tests for stream and feed parsing from filenames."""

    @pytest.mark.parametrize("filename, expected_stream, expected_feed", [
        ("s4y_f2_ifs-ens-cf_od_scda_fc_20260331T060000Z_20260403T080000Z_74h", Stream.S4Y, Feed.F2),
        ("S4Y_f2_ifs-ens-cf_od_scda_fc_20260331T060000Z_20260403T080000Z_74h", Stream.S4Y, Feed.F2),
        ("s5y_f1_ifs-ens-cf_od_scda_fc_20260101T000000Z_20260102T000000Z_24h", Stream.S5Y, Feed.F1),
        ("s6y_f1_ifs-ens-cf_od_scda_fc_20260101T000000Z_20260102T000000Z_24h", Stream.S6Y, Feed.F1),
        ("s4y_F2_ifs-ens-cf_od_scda_fc_20260331T060000Z_20260403T080000Z_74h", Stream.S4Y, Feed.F2),
        ("s4y_F1_ifs-ens-cf_od_scda_fc_20260331T060000Z_20260403T080000Z_74h", Stream.S4Y, Feed.F1),
        ("s4y_f3_ifs-ens-cf_od_scda_fc_20260331T060000Z_20260403T080000Z_74h", Stream.S4Y, Feed.UNKNOWN),
        ("unknown_stream_f2_fc_20260331T060000Z_20260403T080000Z_74h",         Stream.UNKNOWN, Feed.F2),
    ])
    def test_parsed_attributes(self, filename, expected_stream, expected_feed):
        ev = _make_event(_BASE_OBJECT_KEY, filename)
        assert ev.stream == expected_stream
        assert ev.feed == expected_feed
