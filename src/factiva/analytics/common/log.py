import functools
import time
import logging
import os
import sys
from inspect import getframeinfo, stack
from pathlib import Path
from .config import LOGS_DEFAULT_FOLDER, FACTIVA_LOGLEVEL
import datetime

class CustomFormatter(logging.Formatter):
    """ Custom Formatter does these 2 things:
    1. Overrides 'funcName' with the value of 'func_name_override', if it exists.
    2. Overrides 'filename' with the value of 'file_name_override', if it exists.
    """

    def format(self, record):
        if hasattr(record, 'func_name_override'):
            record.funcName = record.func_name_override
        if hasattr(record, 'file_name_override'):
            record.filename = record.file_name_override
        return super(CustomFormatter, self).format(record)


def get_factiva_logger():
    if not os.path.exists(LOGS_DEFAULT_FOLDER):
        # os.mkdir(LOGS_DEFAULT_FOLDER)
        Path(LOGS_DEFAULT_FOLDER).mkdir(parents=True, exist_ok=True)
    logger = logging.Logger(__name__)
    logger.setLevel(FACTIVA_LOGLEVEL)
    file_name = f'factiva-analytics-{datetime.datetime.now().strftime("%Y-%m-%d")}'
    handler = logging.FileHandler(
        f"{LOGS_DEFAULT_FOLDER}/{file_name}.log", 'a+')
    handler.setFormatter(
        CustomFormatter(
            "%(asctime)s [%(levelname)s] [%(filename)s] %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S"
        ))
    handler.formatter.converter = time.gmtime
    logger.addHandler(handler)
    return logger


def factiva_logger(_func=None):
    def log_decorator_info(func):
        @functools.wraps(func)
        def factiva_log(self=None, *args, **kwargs):
            logger_obj = get_factiva_logger()
            # args_passed_in_function = [repr(a) for a in args]
            # kwargs_passed_in_function = [
            #     f"{k}={v!r}" for k, v in kwargs.items()
            # ]
            # formatted_arguments = ", ".join(args_passed_in_function +
            #                                 kwargs_passed_in_function)
            py_file_caller = getframeinfo(stack()[1][0])
            extra_args = {
                'func_name_override': func.__name__,
                'file_name_override': os.path.basename(py_file_caller.filename)
            }
            logger_obj.debug(
                f"Begin function {func.__name__}",
                extra=extra_args)
            try:
                if(self is None):
                    value = func(*args, **kwargs)
                else:
                    value = func(self, *args, **kwargs)
                logger_obj.debug(f"End function {func.__name__}")
            except:
                logger_obj.error(f"Exception in {func.__name__}: {str(sys.exc_info()[1])}")
                raise
            return value

        return factiva_log

    if _func is None:
        return log_decorator_info
    else:
        return log_decorator_info(_func)
