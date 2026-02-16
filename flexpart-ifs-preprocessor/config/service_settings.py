from mch_python_commons.audit.logger import LoggingSettings
from mch_python_commons.config.base_settings import BaseServiceSettings
from pydantic import BaseModel


class S3Bucket(BaseModel):
    endpoint_url: str
    name: str


class S3Buckets(BaseModel):
    input: S3Bucket
    output: S3Bucket


class TimeSettings(BaseModel):
    tincr: int
    tstart: int


class AppSettings(BaseModel):
    app_name: str
    db_path: str
    s3_buckets: S3Buckets
    time_settings: TimeSettings


class ServiceSettings(BaseServiceSettings):
    logging: LoggingSettings
    main: AppSettings

    class Config:
        env_prefix = "SVC__"  # Shortenend prefix
