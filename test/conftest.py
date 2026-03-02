import pytest

@pytest.fixture(autouse=True)
def set_test_env_vars(monkeypatch):
    monkeypatch.setenv("DYNAMODB_TABLE", "test_value")
    monkeypatch.setenv("SOURCE_ROLE_ARN", "another_value")
    monkeypatch.setenv("SOURCE_S3_BUCKET_ARN", "test_value")
    monkeypatch.setenv("TARGET_S3_BUCKET_NAME", "another_value")
