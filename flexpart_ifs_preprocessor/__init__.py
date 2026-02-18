""" Initializations """
import os

from mchpy.audit import logger, http_audit
from flexpart_ifs_preprocessor.config.settings import JobSettings

CONFIG = JobSettings(
    settings_file_names='settings.yaml',
    settings_dirname=os.path.join(os.path.dirname(__file__), 'config')
)

# Configure logger
logger.apply_logging_settings(CONFIG.logging)
