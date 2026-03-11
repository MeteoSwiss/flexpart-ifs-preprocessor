import logging
from pathlib import Path
import os

from flexprep.io_grib import write_grib
from flexprep.preprocessing import preprocess
from flexprep.sources.local import load_grib

from flexpart_ifs_preprocessor.domain.s3_utils import download_file, upload_to_s3
from flexpart_ifs_preprocessor.domain.data_model import IFSForecastFile, Feed

logger = logging.getLogger(__name__)


def run_preprocessing(input_file: IFSForecastFile,
                      previous_file: IFSForecastFile,
                      step_zero_files: list[IFSForecastFile]) -> None:

    directory = Path.home() / "data"

    # Download the files, skipping any that already exist in the temp directory
    logger.info("Downloading main file for processing: %s", input_file.object_key)
    for file in [input_file, previous_file] + step_zero_files:
        download_file(file, directory)

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

    # Write FLEXPART-ready GRIB2 (one file per forecast step)
    logger.info("Writing GRIB2 files to %s ...", directory)
    paths = write_grib(processed, output_dir=directory, suffix="-out.grib")
    for path in paths:
        logger.info("Finished writing processed output at: %s", path.name)
        bucket = os.environ['TARGET_S3_BUCKET_NAME']

        # TODO: add metadata to the object key if needed
        # TODO: rename the file if needed, e.g. to match the expected output filename for the step
        upload_to_s3(path, path.name, bucket)
        logger.info("Uploaded file to S3: %s", path.name)
