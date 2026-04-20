from pydantic import BaseModel, ConfigDict

from mchpy.audit.logger import LoggingSettings
from mchpy.config.base_settings import BaseServiceSettings


class AppSettings(BaseModel):
    model_config = ConfigDict(validate_assignment=True)
    source_role_arn: str
    source_s3_bucket_arn: str
    target_s3_bucket_name_europe: str
    target_s3_bucket_name_global: str
    dynamodb_table_name: str


class JobSettings(BaseServiceSettings):
    logging: LoggingSettings
    app_name: str
    main: AppSettings
