from pydantic import BaseModel, ConfigDict

from mchpy.audit.logger import LoggingSettings
from mchpy.config.base_settings import BaseServiceSettings


class Bucket(BaseModel):
    endpoint_url: str
    name: str


class Buckets(BaseModel):
    output: Bucket


class TimeSettings(BaseModel):
    tincr: int
    tstart: int

class JobSettings(BaseServiceSettings):
    logging: LoggingSettings
    app_name: str
