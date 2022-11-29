import functools
import logging
import os
import sys
from inspect import getframeinfo, stack
from .const import LOGS_DEFAULT_FOLDER
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
        os.mkdir(LOGS_DEFAULT_FOLDER)
    logger = logging.Logger(__name__)
    logger.setLevel(logging.DEBUG)
    file_name = f'factiva-{datetime.datetime.now().strftime("%d-%m-%Y-%H")}'
    handler = logging.FileHandler(
        "{0}/{1}.log".format(LOGS_DEFAULT_FOLDER, file_name), 'a+')
    handler.setFormatter(
        CustomFormatter(
            '%(asctime)s - %(levelname)-10s - %(filename)s - %(funcName)s - %(message)s'
        ))
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
                "Begin function",
                extra=extra_args)
            try:
                if(self is None):
                    value = func(*args, **kwargs)
                else:
                    value = func(self, *args, **kwargs)
                logger_obj.debug("End function")
            except:
                logger_obj.error("Exception: {}".format(str(sys.exc_info()[1])))
                raise
            return value

        return factiva_log

    if _func is None:
        return log_decorator_info
    else:
        return log_decorator_info(_func)
