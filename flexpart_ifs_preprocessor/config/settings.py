from pydantic import BaseModel, ConfigDict

from mchpy.audit.logger import LoggingSettings
from mchpy.config.base_settings import BaseServiceSettings


class Bucket(BaseModel):
    endpoint_url: str
    name: str


class Buckets(BaseModel):
    input: Bucket
    output: Bucket


class TimeSettings(BaseModel):
    tincr: int
    tstart: int

class AppSettings(BaseModel):
    model_config = ConfigDict(validate_assignment=True)
    s3: Buckets
    time_settings: TimeSettings

class JobSettings(BaseServiceSettings):
    logging: LoggingSettings
    app_name: str
    main: AppSettings
