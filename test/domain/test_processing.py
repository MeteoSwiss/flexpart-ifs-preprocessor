"""Unit tests for flexpart_ifs_preprocessor.domain.processing and the flexprep pipeline.

Why these exist alongside the integration tests
-----------------------------------------------
load_grib / preprocess: tested here against the real GRIB file with precise
assertions (exact field names, grid dimensions, unit contracts, coordinate
presence) that the integration test never checks.

_generate_and_upload_grib_file: tested with mocked I/O to verify Feed→prefix
mapping, upload-count correctness, output file deletion (including on error),
metadata key set, and Decimal-step JSON serialisation — none of which the
integration test asserts.

run_preprocessing: error-path and temp-file-cleanup behaviour that the
integration test does not trigger.
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from flexpart_ifs_preprocessor.domain.data_model import Feed, IFSForecastFile

from conftest import GRIB_FILE

class TestLoadGrib:
    """Tests for flexprep.sources.local.load_grib against the real GRIB file."""

    @pytest.fixture(scope="class")
    def raw(self):
        from flexprep.sources.local import load_grib
        return load_grib(GRIB_FILE)

    def test_expected_surface_fields_present(self, raw):
        expected = {"sp", "sd", "tcc", "2t", "2d", "10u", "10v"}
        assert expected.issubset(raw.keys())

    def test_expected_3d_fields_present(self, raw):
        expected = {"t", "u", "v", "q", "etadot"}
        assert expected.issubset(raw.keys())

    def test_ak_bk_coordinates_present(self, raw):
        assert "ak" in raw
        assert "bk" in raw

    @pytest.mark.parametrize("field", ["t", "u", "v", "q", "etadot"])
    def test_3d_field_has_level_dim(self, raw, field):
        assert "level" in raw[field].dims

    @pytest.mark.parametrize("field", ["sp", "sd", "tcc", "2t", "2d", "10u", "10v"])
    def test_surface_field_has_lat_lon_dims(self, raw, field):
        assert "latitude" in raw[field].dims
        assert "longitude" in raw[field].dims

    @pytest.mark.parametrize("field", ["t", "u", "v", "q"])
    def test_3d_field_spatial_shape(self, raw, field):
        # Europe domain: 301 latitudes, 571 longitudes
        assert raw[field].sizes["latitude"] == 301
        assert raw[field].sizes["longitude"] == 571


# ===========================================================================
# preprocess
# ===========================================================================


class TestPreprocess:
    """Tests for flexprep.preprocessing.preprocess."""

    @pytest.fixture(scope="class")
    def processed(self):
        from flexprep.sources.local import load_grib
        from flexprep.preprocessing import preprocess
        raw = load_grib(GRIB_FILE)
        return preprocess(raw)

    def test_expected_output_fields_present(self, processed):
        expected = {"t", "u", "v", "q", "omega", "sp", "sd", "tcc", "2t", "2d", "10u", "10v"}
        assert expected.issubset(processed.keys())

    def test_omega_replaces_etadot(self, processed):
        """After preprocessing etadot should be converted to omega."""
        assert "omega" in processed
        assert "etadot" not in processed

    @pytest.mark.parametrize("field", ["t", "u", "v", "q", "omega"])
    def test_3d_field_spatial_shape(self, processed, field):
        assert processed[field].sizes["latitude"] == 301
        assert processed[field].sizes["longitude"] == 571

    def test_sp_units_are_pascals(self, processed):
        attrs = processed["sp"].attrs
        assert attrs.get("units") == "Pa"


# ===========================================================================
# _generate_and_upload_grib_file  (unit test with mocks)
# ===========================================================================


class TestGenerateAndUploadGribFile:
    """Unit tests for the internal helper that writes and uploads GRIB output."""

    _FILENAME = "s4y_f2_ifs-ens-cf_od_scda_fc_20260331T060000Z_20260403T080000Z_74h"

    @pytest.fixture(scope="class")
    def processed_fields(self):
        from flexprep.sources.local import load_grib
        from flexprep.preprocessing import preprocess
        return preprocess(load_grib(GRIB_FILE))

    @pytest.fixture()
    def input_file(self):
        return IFSForecastFile(
            object_key=f"prefix/{self._FILENAME}",
            filename=self._FILENAME,
        )

    @pytest.fixture()
    def fake_path(self, tmp_path):
        p = tmp_path / "dispf_out"
        p.write_bytes(b"grib")
        return p

    @pytest.mark.parametrize("domain, expected_prefix", [
        (Feed.F1, "dispc"),
        (Feed.F2, "dispf"),
    ])
    def test_correct_prefix_used(self, processed_fields, domain, expected_prefix, fake_path, tmp_path):
        from flexpart_ifs_preprocessor.domain.processing import _generate_and_upload_grib_file
        input_file = IFSForecastFile(
            object_key=f"prefix/{self._FILENAME}",
            filename=self._FILENAME,
            domain=domain,
        )
        with patch("flexpart_ifs_preprocessor.domain.processing.write_grib", return_value=[fake_path]) as mock_write, \
             patch("flexpart_ifs_preprocessor.domain.processing.upload_to_s3"):
            _generate_and_upload_grib_file(tmp_path, processed_fields, input_file)
            assert mock_write.call_args.kwargs["prefix"] == expected_prefix

    def test_upload_called_for_each_written_path(self, processed_fields, input_file, tmp_path):
        from flexpart_ifs_preprocessor.domain.processing import _generate_and_upload_grib_file
        fake_paths = [tmp_path / f"dispf_out_{i}" for i in range(3)]
        for p in fake_paths:
            p.write_bytes(b"grib")
        with patch("flexpart_ifs_preprocessor.domain.processing.write_grib", return_value=fake_paths), \
             patch("flexpart_ifs_preprocessor.domain.processing.upload_to_s3") as mock_upload:
            _generate_and_upload_grib_file(tmp_path, processed_fields, input_file)
        assert mock_upload.call_count == 3

    @pytest.mark.parametrize("upload_side_effect, expect_raises", [
        (None,                       None),
        (RuntimeError("S3 error"),   RuntimeError),
    ])
    def test_output_files_deleted(self, processed_fields, input_file, fake_path, tmp_path,
                                   upload_side_effect, expect_raises):
        from flexpart_ifs_preprocessor.domain.processing import _generate_and_upload_grib_file
        with patch("flexpart_ifs_preprocessor.domain.processing.write_grib", return_value=[fake_path]), \
             patch("flexpart_ifs_preprocessor.domain.processing.upload_to_s3", side_effect=upload_side_effect):
            if expect_raises:
                with pytest.raises(expect_raises):
                    _generate_and_upload_grib_file(tmp_path, processed_fields, input_file)
            else:
                _generate_and_upload_grib_file(tmp_path, processed_fields, input_file)
        assert not fake_path.exists()

    def test_upload_metadata_contains_required_keys(self, processed_fields, input_file, fake_path, tmp_path):
        from flexpart_ifs_preprocessor.domain.processing import _generate_and_upload_grib_file
        with patch("flexpart_ifs_preprocessor.domain.processing.write_grib", return_value=[fake_path]), \
             patch("flexpart_ifs_preprocessor.domain.processing.upload_to_s3") as mock_upload:
            _generate_and_upload_grib_file(tmp_path, processed_fields, input_file)
        _, _, _, metadata = mock_upload.call_args.args
        assert set(metadata.keys()) == {"model", "date", "time", "step", "domain"}
        assert metadata["model"] == "IFS"
        assert metadata["step"] == "74"
        assert metadata["domain"] == "EUROPE"

    def test_metadata_is_json_serialisable_with_decimal_step(self, processed_fields, fake_path, tmp_path):
        """Regression: DynamoDB Decimal step must not cause TypeError in json.dumps."""
        import json
        from decimal import Decimal
        from flexpart_ifs_preprocessor.domain.processing import _generate_and_upload_grib_file
        input_file = IFSForecastFile(
            object_key=f"prefix/{self._FILENAME}",
            filename=self._FILENAME,
            step=Decimal("74"),
        )
        with patch("flexpart_ifs_preprocessor.domain.processing.write_grib", return_value=[fake_path]), \
             patch("flexpart_ifs_preprocessor.domain.processing.upload_to_s3") as mock_upload:
            _generate_and_upload_grib_file(tmp_path, processed_fields, input_file)
        _, _, _, metadata = mock_upload.call_args.args
        assert '"74"' in json.dumps(metadata)


# ===========================================================================
# run_preprocessing  (end-to-end with mocked S3 / download)
# ===========================================================================


class TestRunPreprocessing:
    """Integration-style tests for run_preprocessing with mocked I/O."""

    _FILENAME = "s4y_f2_ifs-ens-cf_od_scda_fc_20260331T060000Z_20260403T080000Z_74h"

    @pytest.fixture()
    def three_files(self):
        make = lambda: IFSForecastFile(object_key=f"prefix/{self._FILENAME}", filename=self._FILENAME)
        return make(), make(), [make(), make()]

    @pytest.fixture()
    def fake_download(self, tmp_path):
        """Side-effect for download_file that writes a dummy file and records the path."""
        created: list[Path] = []

        def _download(file, directory):
            directory.mkdir(parents=True, exist_ok=True)
            p = directory / file.filename
            p.write_bytes(b"dummy")
            created.append(p)

        return _download, created

    def test_run_preprocessing_raises_if_load_returns_empty(self, three_files, fake_download):
        from flexpart_ifs_preprocessor.domain.processing import run_preprocessing
        download_fn, _ = fake_download
        input_file, previous_file, step_zero_files = three_files
        with patch("flexpart_ifs_preprocessor.domain.processing.download_file", side_effect=download_fn), \
             patch("flexpart_ifs_preprocessor.domain.processing.load_grib", return_value={}):
            with pytest.raises(ValueError, match="No fields loaded"):
                run_preprocessing(input_file, previous_file, step_zero_files)

    @pytest.mark.parametrize("load_grib_kwargs", [
        # success path: load_grib returns data, pipeline completes normally
        {"return_value": {"sp": MagicMock()}},
        # failure path: load_grib raises, pipeline should still clean up
        {"side_effect": RuntimeError("boom")},
    ])
    def test_temp_files_cleaned_up(self, three_files, fake_download, load_grib_kwargs):
        from flexpart_ifs_preprocessor.domain.processing import run_preprocessing
        download_fn, created_files = fake_download
        input_file, previous_file, step_zero_files = three_files
        with patch("flexpart_ifs_preprocessor.domain.processing.download_file", side_effect=download_fn), \
             patch("flexpart_ifs_preprocessor.domain.processing.load_grib", **load_grib_kwargs), \
             patch("flexpart_ifs_preprocessor.domain.processing.preprocess", return_value={"sp": MagicMock()}), \
             patch("flexpart_ifs_preprocessor.domain.processing._generate_and_upload_grib_file"):
            try:
                run_preprocessing(input_file, previous_file, step_zero_files)
            except RuntimeError:
                pass
        for p in created_files:
            assert not p.exists(), f"Temp file was not cleaned up: {p}"
