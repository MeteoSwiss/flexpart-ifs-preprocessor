"""Tests for flexpart_ifs_preprocessor.domain.processing and the flexprep pipeline."""

import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import xarray as xr

from flexpart_ifs_preprocessor.domain.data_model import Feed, IFSForecastFile

# ---------------------------------------------------------------------------
# The real GRIB file provided as a test resource
# ---------------------------------------------------------------------------

RESOURCES_DIR = Path(__file__).parent.parent / "resources"
GRIB_FILE = RESOURCES_DIR / "s4y_f2_ifs-ens-cf_od_scda_fc_20260331T060000Z_20260403T080000Z_74h"


# ===========================================================================
# load_grib
# ===========================================================================


class TestLoadGrib:
    """Tests for flexprep.sources.local.load_grib against the real GRIB file."""

    @pytest.fixture(scope="class")
    def raw(self):
        from flexprep.sources.local import load_grib
        return load_grib(GRIB_FILE)

    def test_returns_dict(self, raw):
        assert isinstance(raw, dict)

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

    def test_all_values_are_xarray_dataarrays(self, raw):
        for key, val in raw.items():
            assert isinstance(val, xr.DataArray), f"Field '{key}' is not a DataArray"


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

    def test_returns_dict(self, processed):
        assert isinstance(processed, dict)

    def test_expected_output_fields_present(self, processed):
        expected = {"t", "u", "v", "q", "omega", "sp", "sd", "tcc", "2t", "2d", "10u", "10v"}
        assert expected.issubset(processed.keys())

    def test_omega_replaces_etadot(self, processed):
        """After preprocessing etadot should be converted to omega."""
        assert "omega" in processed
        assert "etadot" not in processed

    @pytest.mark.parametrize("field", ["t", "u", "v", "q", "omega"])
    def test_3d_field_has_level_dim(self, processed, field):
        assert "level" in processed[field].dims

    @pytest.mark.parametrize("field", ["sp", "sd", "tcc", "2t", "2d", "10u", "10v"])
    def test_surface_field_has_no_level_dim(self, processed, field):
        assert "level" not in processed[field].dims

    @pytest.mark.parametrize("field", ["t", "u", "v", "q", "omega"])
    def test_3d_field_spatial_shape(self, processed, field):
        assert processed[field].sizes["latitude"] == 301
        assert processed[field].sizes["longitude"] == 571

    def test_all_values_are_xarray_dataarrays(self, processed):
        for key, val in processed.items():
            assert isinstance(val, xr.DataArray), f"Field '{key}' is not a DataArray"

    def test_sp_units_are_pascals(self, processed):
        attrs = processed["sp"].attrs
        assert attrs.get("units") == "Pa"


# ===========================================================================
# _generate_and_upload_grib_file  (unit test with mocks)
# ===========================================================================


class TestGenerateAndUploadGribFile:
    """Unit tests for the internal helper that writes and uploads GRIB output."""

    @pytest.fixture()
    def processed_fields(self):
        from flexprep.sources.local import load_grib
        from flexprep.preprocessing import preprocess
        raw = load_grib(GRIB_FILE)
        return preprocess(raw)

    @pytest.mark.parametrize("domain, expected_prefix", [
        (Feed.F1, "dispc"),
        (Feed.F2, "dispf"),
    ])
    def test_correct_prefix_used(self, processed_fields, domain, expected_prefix, tmp_path):
        from flexpart_ifs_preprocessor.domain.processing import _generate_and_upload_grib_file

        filename = "s4y_f2_ifs-ens-cf_od_scda_fc_20260331T060000Z_20260403T080000Z_74h"
        input_file = IFSForecastFile(
            object_key=f"prefix/{filename}",
            filename=filename,
            domain=domain,
        )

        fake_path = tmp_path / "dispf20260403080074"
        fake_path.write_bytes(b"grib")

        with patch("flexpart_ifs_preprocessor.domain.processing.write_grib", return_value=[fake_path]) as mock_write, \
             patch("flexpart_ifs_preprocessor.domain.processing.upload_to_s3"):
            _generate_and_upload_grib_file(tmp_path, processed_fields, input_file)
            call_kwargs = mock_write.call_args.kwargs
            assert call_kwargs["prefix"] == expected_prefix

    def test_upload_called_for_each_written_path(self, processed_fields, tmp_path):
        from flexpart_ifs_preprocessor.domain.processing import _generate_and_upload_grib_file

        filename = "s4y_f2_ifs-ens-cf_od_scda_fc_20260331T060000Z_20260403T080000Z_74h"
        input_file = IFSForecastFile(
            object_key=f"prefix/{filename}",
            filename=filename,
        )

        fake_paths = [tmp_path / f"dispf_out_{i}" for i in range(3)]
        for p in fake_paths:
            p.write_bytes(b"grib")

        with patch("flexpart_ifs_preprocessor.domain.processing.write_grib", return_value=fake_paths), \
             patch("flexpart_ifs_preprocessor.domain.processing.upload_to_s3") as mock_upload:
            _generate_and_upload_grib_file(tmp_path, processed_fields, input_file)
            assert mock_upload.call_count == 3

    def test_output_files_deleted_after_upload(self, processed_fields, tmp_path):
        from flexpart_ifs_preprocessor.domain.processing import _generate_and_upload_grib_file

        filename = "s4y_f2_ifs-ens-cf_od_scda_fc_20260331T060000Z_20260403T080000Z_74h"
        input_file = IFSForecastFile(
            object_key=f"prefix/{filename}",
            filename=filename,
        )

        fake_path = tmp_path / "dispf_out"
        fake_path.write_bytes(b"grib")

        with patch("flexpart_ifs_preprocessor.domain.processing.write_grib", return_value=[fake_path]), \
             patch("flexpart_ifs_preprocessor.domain.processing.upload_to_s3"):
            _generate_and_upload_grib_file(tmp_path, processed_fields, input_file)

        assert not fake_path.exists()

    def test_output_files_deleted_even_if_upload_raises(self, processed_fields, tmp_path):
        from flexpart_ifs_preprocessor.domain.processing import _generate_and_upload_grib_file

        filename = "s4y_f2_ifs-ens-cf_od_scda_fc_20260331T060000Z_20260403T080000Z_74h"
        input_file = IFSForecastFile(
            object_key=f"prefix/{filename}",
            filename=filename,
        )

        fake_path = tmp_path / "dispf_out"
        fake_path.write_bytes(b"grib")

        with patch("flexpart_ifs_preprocessor.domain.processing.write_grib", return_value=[fake_path]), \
             patch("flexpart_ifs_preprocessor.domain.processing.upload_to_s3", side_effect=RuntimeError("S3 error")):
            with pytest.raises(RuntimeError, match="S3 error"):
                _generate_and_upload_grib_file(tmp_path, processed_fields, input_file)

        assert not fake_path.exists()

    def test_upload_metadata_contains_required_keys(self, processed_fields, tmp_path):
        from flexpart_ifs_preprocessor.domain.processing import _generate_and_upload_grib_file

        filename = "s4y_f2_ifs-ens-cf_od_scda_fc_20260331T060000Z_20260403T080000Z_74h"
        input_file = IFSForecastFile(
            object_key=f"prefix/{filename}",
            filename=filename,
        )

        fake_path = tmp_path / "dispf_out"
        fake_path.write_bytes(b"grib")

        with patch("flexpart_ifs_preprocessor.domain.processing.write_grib", return_value=[fake_path]), \
             patch("flexpart_ifs_preprocessor.domain.processing.upload_to_s3") as mock_upload:
            _generate_and_upload_grib_file(tmp_path, processed_fields, input_file)

        _, _, _, metadata = mock_upload.call_args.args
        assert set(metadata.keys()) == {"model", "date", "time", "step", "domain"}
        assert metadata["model"] == "IFS"
        assert metadata["step"] == "74"
        assert metadata["domain"] == "EUROPE"

    def test_metadata_is_json_serialisable_with_decimal_step(self, processed_fields, tmp_path):
        """Regression: DynamoDB Decimal step must not cause TypeError in json.dumps."""
        import json
        from decimal import Decimal
        from flexpart_ifs_preprocessor.domain.processing import _generate_and_upload_grib_file

        filename = "s4y_f2_ifs-ens-cf_od_scda_fc_20260331T060000Z_20260403T080000Z_74h"
        input_file = IFSForecastFile(
            object_key=f"prefix/{filename}",
            filename=filename,
            step=Decimal("74"),  # as returned by DynamoDB
        )

        fake_path = tmp_path / "dispf_out"
        fake_path.write_bytes(b"grib")

        with patch("flexpart_ifs_preprocessor.domain.processing.write_grib", return_value=[fake_path]), \
             patch("flexpart_ifs_preprocessor.domain.processing.upload_to_s3") as mock_upload:
            _generate_and_upload_grib_file(tmp_path, processed_fields, input_file)

        _, _, _, metadata = mock_upload.call_args.args
        # Must not raise TypeError
        serialised = json.dumps(metadata)
        assert '"74"' in serialised


# ===========================================================================
# run_preprocessing  (end-to-end with mocked S3 / download)
# ===========================================================================


class TestRunPreprocessing:
    """Integration-style tests for run_preprocessing with mocked I/O."""

    @pytest.fixture()
    def three_files(self):
        filename = "s4y_f2_ifs-ens-cf_od_scda_fc_20260331T060000Z_20260403T080000Z_74h"
        make = lambda: IFSForecastFile(object_key=f"prefix/{filename}", filename=filename)
        return make(), make(), [make(), make()]

    def test_run_preprocessing_calls_upload(self, three_files, tmp_path):
        from flexpart_ifs_preprocessor.domain.processing import run_preprocessing

        input_file, previous_file, step_zero_files = three_files

        def fake_download(file, directory):
            directory.mkdir(parents=True, exist_ok=True)
            (directory / file.filename).write_bytes(b"dummy")

        fake_out = tmp_path / "dispf_result"
        fake_out.write_bytes(b"grib")

        with patch("flexpart_ifs_preprocessor.domain.processing.download_file", side_effect=fake_download), \
             patch("flexpart_ifs_preprocessor.domain.processing.load_grib") as mock_load, \
             patch("flexpart_ifs_preprocessor.domain.processing.preprocess") as mock_preprocess, \
             patch("flexpart_ifs_preprocessor.domain.processing._generate_and_upload_grib_file") as mock_gen:
            mock_load.return_value = {"sp": MagicMock()}
            mock_preprocess.return_value = {"sp": MagicMock()}

            run_preprocessing(input_file, previous_file, step_zero_files)

            mock_gen.assert_called_once()

    def test_run_preprocessing_raises_if_load_returns_empty(self, three_files):
        from flexpart_ifs_preprocessor.domain.processing import run_preprocessing

        input_file, previous_file, step_zero_files = three_files

        def fake_download(file, directory):
            directory.mkdir(parents=True, exist_ok=True)
            (directory / file.filename).write_bytes(b"dummy")

        with patch("flexpart_ifs_preprocessor.domain.processing.download_file", side_effect=fake_download), \
             patch("flexpart_ifs_preprocessor.domain.processing.load_grib", return_value={}):
            with pytest.raises(ValueError, match="No fields loaded"):
                run_preprocessing(input_file, previous_file, step_zero_files)

    def test_temp_files_cleaned_up_on_success(self, three_files, tmp_path):
        from flexpart_ifs_preprocessor.domain.processing import run_preprocessing

        input_file, previous_file, step_zero_files = three_files
        created_files: list[Path] = []

        def fake_download(file, directory):
            directory.mkdir(parents=True, exist_ok=True)
            p = directory / file.filename
            p.write_bytes(b"dummy")
            created_files.append(p)

        with patch("flexpart_ifs_preprocessor.domain.processing.download_file", side_effect=fake_download), \
             patch("flexpart_ifs_preprocessor.domain.processing.load_grib", return_value={"sp": MagicMock()}), \
             patch("flexpart_ifs_preprocessor.domain.processing.preprocess", return_value={"sp": MagicMock()}), \
             patch("flexpart_ifs_preprocessor.domain.processing._generate_and_upload_grib_file"):
            run_preprocessing(input_file, previous_file, step_zero_files)

        for p in created_files:
            assert not p.exists(), f"Temp file was not cleaned up: {p}"

    def test_temp_files_cleaned_up_on_failure(self, three_files, tmp_path):
        from flexpart_ifs_preprocessor.domain.processing import run_preprocessing

        input_file, previous_file, step_zero_files = three_files
        created_files: list[Path] = []

        def fake_download(file, directory):
            directory.mkdir(parents=True, exist_ok=True)
            p = directory / file.filename
            p.write_bytes(b"dummy")
            created_files.append(p)

        with patch("flexpart_ifs_preprocessor.domain.processing.download_file", side_effect=fake_download), \
             patch("flexpart_ifs_preprocessor.domain.processing.load_grib", side_effect=RuntimeError("boom")):
            with pytest.raises(RuntimeError, match="boom"):
                run_preprocessing(input_file, previous_file, step_zero_files)

        for p in created_files:
            assert not p.exists(), f"Temp file was not cleaned up after failure: {p}"
