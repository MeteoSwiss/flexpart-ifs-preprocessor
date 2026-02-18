from pydantic import BaseModel, ConfigDict

from mchpy.audit.logger import LoggingSettings
from mchpy.config.base_settings import BaseServiceSettings


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
    model_config = ConfigDict(validate_assignment=True)

    app_name: str
    db_path: str
    s3_buckets: S3Buckets
    time_settings: TimeSettings

class JobSettings(BaseServiceSettings):
    logging: LoggingSettings
    main: AppSettings
