"""End-to-end integration test: lambda_handler processes the 4h GRIB file.

AWS resources (DynamoDB, S3) are fully mocked with moto via the
``aws_environment`` fixture defined in conftest.py.  The real compressed
GRIB files from test/resources are uploaded to the mocked source bucket so
that download_file → load_grib → preprocess → write_grib → upload_to_s3
all run against actual data.

Pre-conditions (set up by the fixture):
  - DynamoDB contains PENDING entries for DA-0h, ENS-0h and ENS-3h.
  - Source S3 bucket contains all four GRIB files.
  - Target S3 bucket is empty and ready to receive output.

The incoming Kafka event carries the step-4 file.  After lambda_handler
returns the tests assert:
  1. At least one dispf* output file was written to the target S3 bucket.
  2. The step-4 DynamoDB entry was set to PROCESSED.
  3. The step-0 and step-3 entries are still PENDING (not touched).
"""

from conftest import (
    OPER_ENS_4H,
    OPER_DA_0H,
    OPER_ENS_0H,
    OPER_ENS_3H,
    OPER_PREFIX,
    OPER_REF_TIME_TS,
    OPER_TARGET_BUCKET_EU,
    OPER_TARGET_BUCKET_GLOBAL,
    _kafka_event,
)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestProcess4hEndToEnd:
    """lambda_handler processes the 4h GRIB file end-to-end against mocked AWS."""

    def test_output_uploaded_to_target_s3(self, aws_environment_4h):
        """After processing the 4h file, at least one dispf* file appears in target S3."""
        from flexpart_ifs_preprocessor.flexpart_ifs_preprocessor import lambda_handler

        lambda_handler(_kafka_event(OPER_ENS_4H), None)

        resp = aws_environment_4h.s3.list_objects_v2(Bucket=OPER_TARGET_BUCKET_EU)
        assert "Contents" in resp, "No files were uploaded to the target S3 bucket"
        keys = [obj["Key"] for obj in resp["Contents"]]
        assert any(k.startswith("dispf") for k in keys), (
            f"Expected a dispf* output file in target bucket, got: {keys}"
        )

        resp = aws_environment_4h.s3.list_objects_v2(Bucket=OPER_TARGET_BUCKET_GLOBAL)
        assert "Contents" not in resp, "Files were uploaded to the target S3 bucket when none were expected"

    def test_4h_item_marked_processed_in_dynamodb(self, aws_environment_4h):
        """DynamoDB Status for the 4h file is set to PROCESSED after lambda_handler."""
        from flexpart_ifs_preprocessor.flexpart_ifs_preprocessor import lambda_handler

        lambda_handler(_kafka_event(OPER_ENS_4H), None)

        item = aws_environment_4h.table.get_item(Key={
            "ReferenceTimePartitionKey": OPER_REF_TIME_TS,
            "ObjectKey": f"{OPER_PREFIX}/{OPER_ENS_4H}",
        }).get("Item")
        assert item is not None, "4h DynamoDB item not found"
        assert item["Status_1h"] == "PROCESSED", (
            f"Expected Status=PROCESSED for the 4h item, got: {item['Status']}"
        )
        assert item["Status_3h"] == "PENDING", (
            f"Expected Status=PENDING for the 4h item, got: {item['Status']}"
        )

    def test_prerequisite_items_remain_pending(self, aws_environment_4h):
        """The step-0 and step-3 DB entries are not touched – they stay PENDING."""
        from flexpart_ifs_preprocessor.flexpart_ifs_preprocessor import lambda_handler

        lambda_handler(_kafka_event(OPER_ENS_4H), None)

        for filename, step in [(OPER_DA_0H, 0), (OPER_ENS_0H, 0), (OPER_ENS_3H, 3)]:
            item = aws_environment_4h.table.get_item(Key={
                "ReferenceTimePartitionKey": OPER_REF_TIME_TS,
                "ObjectKey": f"{OPER_PREFIX}/{filename}",
            }).get("Item")
            assert item is not None, f"Item for {filename} not found in DynamoDB"
            assert item["Status_1h"] == "PENDING", (
                f"{filename} should still be PENDING but got: {item['Status_1h']}"
            )
            assert item["Status_3h"] == "PENDING", (
                f"{filename} should still be PENDING but got: {item['Status_3h']}"
            )


class TestProcess3hEndToEnd:
    """lambda_handler processes the 3h GRIB file end-to-end against mocked AWS."""

    def test_output_uploaded_to_target_s3(self, aws_environment_3h):
        """After processing the 4h file, at least one dispf* file appears in target S3."""
        from flexpart_ifs_preprocessor.flexpart_ifs_preprocessor import lambda_handler

        lambda_handler(_kafka_event(OPER_ENS_3H), None)

        resp = aws_environment_3h.s3.list_objects_v2(Bucket=OPER_TARGET_BUCKET_EU)

        assert "Contents" in resp, "No files were uploaded to the target S3 bucket (europe)"
        keys = [obj["Key"] for obj in resp["Contents"]]
        assert any(k.startswith("dispf") for k in keys), (
            f"Expected a dispf* output file in target bucket, got: {keys}"
        )
        resp = aws_environment_3h.s3.list_objects_v2(Bucket=OPER_TARGET_BUCKET_GLOBAL)
        assert "Contents" in resp, "No files were uploaded to the target S3 bucket (global)"
        keys = [obj["Key"] for obj in resp["Contents"]]
        assert any(k.startswith("dispf") for k in keys), (
            f"Expected a dispf* output file in target bucket, got: {keys}"
        )

    def test_3h_item_marked_processed_in_dynamodb(self, aws_environment_3h):
        """DynamoDB Status for the 4h file is set to PROCESSED after lambda_handler."""
        from flexpart_ifs_preprocessor.flexpart_ifs_preprocessor import lambda_handler

        lambda_handler(_kafka_event(OPER_ENS_3H), None)

        item = aws_environment_3h.table.get_item(Key={
            "ReferenceTimePartitionKey": OPER_REF_TIME_TS,
            "ObjectKey": f"{OPER_PREFIX}/{OPER_ENS_3H}",
        }).get("Item")
        assert item is not None, "3h DynamoDB item not found"
        assert item["Status_1h"] == "PROCESSED", (
            f"Expected Status=PROCESSED for the 3h item, got: {item['Status_1h']}"
        )
        assert item["Status_3h"] == "PROCESSED", (
            f"Expected Status=PROCESSED for the 3h item, got: {item['Status_3h']}"
        )
