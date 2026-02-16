import logging
from io import StringIO

import pytest

from flexpart_ifs_preprocessor.domain.processing import Processing


@pytest.fixture
def log_capture():
    # Set up a StringIO object to capture logging output
    log_capture = StringIO()
    log_handler = logging.StreamHandler(log_capture)
    log_handler.setLevel(logging.ERROR)
    logger = logging.getLogger()
    logger.addHandler(log_handler)
    yield log_capture
    logger.removeHandler(log_handler)


def test_sorted_files_length_less_than_3(log_capture):

    processing_obj = Processing()
    # Create file objects with length less than 3
    file_objs = [
        processing_obj.FileObject(step="1", filename="file1"),
        processing_obj.FileObject(step="2", filename="file2"),
    ]

    processing_obj._sort_and_download_files(file_objs)

    # Check the captured log output
    log_capture.flush()
    log_contents = log_capture.getvalue().strip()

    # Verify the logged message
    assert (
        "Sorting and validation failed: Not enough files for pre-processing"
        in log_contents
    )
