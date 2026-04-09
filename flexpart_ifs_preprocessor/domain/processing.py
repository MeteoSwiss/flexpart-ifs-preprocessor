import contextlib
import logging
from pathlib import Path
import os
import tempfile
from typing import Any, Generator

from flexprep.io_grib import write_grib
from flexprep.preprocessing import preprocess
from flexprep.sources.local import load_grib

from flexpart_ifs_preprocessor.domain.s3_utils import download_file, upload_to_s3
from flexpart_ifs_preprocessor.domain.data_model import IFSForecastFile, Feed
from xarray import DataArray

logger = logging.getLogger(__name__)


def run_preprocessing(input_file: IFSForecastFile,
                      previous_file: IFSForecastFile,
                      step_zero_files: list[IFSForecastFile]) -> None:
    # Download the files, skipping any that already exist in the temp directory
    logger.info("Downloading main file for processing: %s", input_file.object_key)
    with _download_temp_files([input_file, previous_file] + step_zero_files) as directory:
        # Load raw fields
        logger.info("Loading GRIB source: %s", directory / input_file.filename)

        raw = load_grib([
            directory / input_file.filename,
            directory / step_zero_files[0].filename,
            directory / step_zero_files[1].filename])

        if not raw:
            logger.error("No fields loaded - aborting.")
            raise ValueError("No fields loaded from GRIB files.")

        logger.info("Loaded fields: %s", ", ".join(sorted(raw.keys())))

        # Preprocess (rates to per-hour/per-second, omega, etc.)
        processed = preprocess(raw)
        logger.info("Prepared fields: %s", ", ".join(sorted(processed.keys())))

        _generate_and_upload_grib_file(directory, processed, input_file)


def _generate_and_upload_grib_file(output_dir: Path, processed: dict[str, DataArray], input_file: IFSForecastFile):
    """Write FLEXPART-ready GRIB2 (one file per forecast step)"""

    logger.info("Writing GRIB2 file to %s ...", output_dir)

    if input_file.domain == Feed.F1:
        prefix = "dispc"
        bucket = os.environ['TARGET_S3_BUCKET_NAME_GLOBAL']
    elif input_file.domain == Feed.F2:
        prefix = "dispf"
        bucket = os.environ['TARGET_S3_BUCKET_NAME_EUROPE']
    else:
        logger.error("Unknown feed/domain: %s", input_file.domain)
        raise ValueError(f"Unknown feed/domain: {input_file.domain}")

    paths = write_grib(
        processed,
        output_dir=output_dir,
        prefix=prefix,
        suffix="")
    try:
        metadata = {
            "model": input_file.model,
            "date": input_file.forecast_ref_time.strftime("%Y%m%d"),
            "time": input_file.forecast_ref_time.strftime("%H%M"),
            "step": str(input_file.step),
            "domain": str(input_file.domain.value),
            }
        for path in paths:
            logger.info("Finished writing processed output at: %s", path.name)
            upload_to_s3(path, path.name, bucket, metadata)
            logger.info("Uploaded file to S3: %s", path.name)
    finally:
        # Delete all local files if uploaded or not
        for path in paths:
            logger.info("Deleting temp uploaded file: %s", path.name)
            path.unlink()


@contextlib.contextmanager
def _download_temp_files(file_paths: list[IFSForecastFile]) -> Generator[Path, Any, None]:
    with tempfile.TemporaryDirectory() as tmp_dir:
        target_dir = Path(tmp_dir)
        for file in file_paths:
            download_file(file, target_dir)
        yield target_dir
