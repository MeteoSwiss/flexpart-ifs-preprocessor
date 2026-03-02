import logging
from datetime import timedelta
import os
import tempfile
import typing
from pathlib import Path

from flexprep.io_grib import write_grib
from flexprep.preprocessing import preprocess
from flexprep.sources.local import load_grib

from flexpart_ifs_preprocessor.domain.s3_utils import download_file, upload_to_s3
from flexpart_ifs_preprocessor.domain.data_model import IFSForecastFile

logger = logging.getLogger(__name__)


def run_preprocessing(input_file: IFSForecastFile, previous_file: IFSForecastFile, step_zero_files: list[IFSForecastFile]) -> None:

    dir = Path.home() / "data"

    # Download the files, skipping any that already exist in the temp directory
    logger.info(f"Downloading main file for processing: {input_file.object_key}")
    for file in [input_file, previous_file] + step_zero_files:
        download_file(file, dir)

    # Load raw fields
    logger.info("Loading GRIB source: %s", dir / input_file.filename)

    raw = load_grib([
        dir / input_file.filename,
        dir / step_zero_files[0].filename,
        dir / step_zero_files[1].filename])

    if not raw:
        logger.error("No fields loaded - aborting.")
        raise ValueError("No fields loaded from GRIB files.")

    logger.info("Loaded fields: %s", ", ".join(sorted(raw.keys())))

    # Preprocess (rates to per-hour/per-second, omega, etc.)
    processed = preprocess(raw)
    logger.info("Prepared fields: %s", ", ".join(sorted(processed.keys())))

    # Write FLEXPART-ready GRIB2 (one file per forecast step)
    logger.info("Writing GRIB2 files to %s ...", dir)
    paths = write_grib(processed, output_dir=dir, suffix="-out.grib")
    for path in paths:
        logger.info("Finished writing processed output at: %s", path.name)

        # TODO: add metadata to the object key if needed
        # TODO: rename the file if needed, e.g. to match the expected output filename for the step
        upload_to_s3(path, path.name)
        logger.info(f"Uploaded file to S3: {path.name}")









class Processing:
    FileObject = dict[str, typing.Any]

    def __init__(self) -> None:
        self.s3_client = S3client()

    def process(self, file_objs: list[FileObject]) -> None:
        if file_objs:
            logger.info(f"Processing timestep: {file_objs[-1]['step']}")

        result = self._sort_and_download_files(file_objs)
        if result is None:
            logger.exception("Failed to sort and download files.")
            raise

        temp_files, to_process = result
        input_dir = Path(temp_files[0]).parent

        with tempfile.TemporaryDirectory() as temp_output_dir:
            output_dir = Path(temp_output_dir)

            run_preprocessing(input_dir=str(input_dir), output_dir=str(output_dir))

            # Compute expected output filename for the processed step
            step_to_process = file_objs[-1]['step']
            forecast_ref_time = file_objs[-1]['forecast_ref_time']
            lead_time = forecast_ref_time + timedelta(hours=step_to_process)
            lead_time_str = lead_time.strftime("%Y%m%d%H")
            expected_filename = f"dispf{lead_time_str}"

            # Upload only the matching output file
            for file_path in output_dir.iterdir():
                if file_path.is_file() and file_path.name == expected_filename:
                    key = file_path.name
                    self.s3_client.upload_file(str(file_path), key)

        # Clean up temp input files
        for temp_file in temp_files:
            try:
                os.unlink(temp_file)
            except FileNotFoundError:
                logger.warning(f"Tried to delete missing file: {temp_file}")


        # Mark DB as processed
        DB().update_item_as_processed(to_process["row_id"])

    def _sort_and_download_files(
        self, file_objs: list[FileObject]
    ) -> tuple[list[str], FileObject] | None:
        """Sort file objects, validate, and select files for processing."""
        try:
            sorted_files = sorted(file_objs, key=lambda x: int(x["step"]), reverse=True)
            if len(sorted_files) < 3:
                raise ValueError("Not enough files for pre-processing")

            to_process = sorted_files[0]
            prev_file = sorted_files[1]

            init_files = (
                sorted_files[2:4] if int(prev_file["step"]) == 0 else sorted_files[2:4]
            )
            files_to_download = [to_process, prev_file] + init_files

            tempfiles = self._download_files(files_to_download)
            return tempfiles, to_process

        except Exception as e:
            logger.exception(f"Sorting and validation failed: {e}")
            return None
