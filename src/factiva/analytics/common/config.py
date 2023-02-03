
import os
from . import const

def load_environment_value(config_key, default=None) -> str:
    """Obtain a environmental variable."""
    tmp_val = os.getenv(config_key, default)
    if tmp_val is None:
        raise Exception(
            "Environment Variable {} not found!".format(config_key))
    return tmp_val

# Logging Level
FACTIVA_LOGLEVEL = load_environment_value('FACTIVA_LOGLEVEL', 'INFO').upper()


# Default file locations
DOWNLOAD_DEFAULT_FOLDER = load_environment_value(
    "DOWNLOAD_FILES_DIR", os.path.join(os.getcwd(), 'downloads'))
LISTENER_FILES_DEFAULT_FOLDER = load_environment_value(
    'STREAM_FILES_DIR', os.path.join(os.getcwd(), 'listener'))
LOGS_DEFAULT_FOLDER = load_environment_value(
    'LOG_FILES_DIR', os.path.join(os.path.expanduser('~'), const.LOGS_DEFAULT_PATH))

