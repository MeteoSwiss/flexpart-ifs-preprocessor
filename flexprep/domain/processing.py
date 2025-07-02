import logging
import os
import tempfile
import typing
from pathlib import Path

from preflexpart import run_preprocessing

from flexprep.domain.db_utils import DB
from flexprep.domain.s3_utils import S3client

logger = logging.getLogger(__name__)


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

            for file_path in output_dir.iterdir():
                if file_path.is_file():
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

    def _download_files(self, files_to_download: list[FileObject]) -> list[str]:
        """Download files from S3 based on the file objects."""
        try:
            return [
                self.s3_client.download_file(file_obj) for file_obj in files_to_download
            ]
        except Exception as e:
            logger.exception(f"File download failed: {e}")
            raise RuntimeError("An error occurred while downloading files.") from e
