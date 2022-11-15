import datetime
import os

from dateutil import parser
from factiva.core import const


def load_environment_value(config_key, default=None) -> str:
    """Obtain a environmental variable."""
    tmp_val = os.getenv(config_key, default)
    if tmp_val is None:
        raise Exception(
            "Environment Variable {} not found!".format(config_key))
    return tmp_val


def mask_string(raw_str, right_padding=4) -> str:
    """Mask a string with a default value of 4."""
    return raw_str[-right_padding:].rjust(len(raw_str), '*')


def validate_type(var_to_validate, expected_type, error_message) -> bool:
    """Validate a given type."""
    if not isinstance(var_to_validate, expected_type):
        raise ValueError(error_message)


def validate_field_options(field, available_options):
    """Validate that field is among the available options.
    Parameters
    ----------
    field:
        field to be validated, could be any type.
    available_options:
        options amongst field should be located.
    """
    if field not in available_options:
        raise ValueError(
            f'Value {field} is not within the allowed options: {available_options}'
        )


def flatten_dict(multi_level_dict) -> dict:
    """Flatten a dictionary."""
    flattened_dict = {}
    for entry in multi_level_dict:
        if isinstance(multi_level_dict[entry], dict):
            new_elements = flatten_dict(multi_level_dict[entry])
            flattened_dict.update(new_elements)
        else:
            flattened_dict[entry] = multi_level_dict[entry]

    return flattened_dict


def isots_to_tsms(isodatestr: str) -> int:
    return round(
        datetime.datetime.fromisoformat(
            str(isodatestr).replace('&', '')
        ).timestamp())


def now_to_tsms() -> int:
    return round(datetime.datetime.utcnow().timestamp())


def format_timestamps(message: dict) -> dict:
    """Format datetimes from a dict
    Parameters
    ----------
    message:
        dict with datetime values to be formated
    Returns
    -------
    dict
        Dict with datetimes formated
    """
    for fieldname in const.TIMESTAMP_FIELDS:
        if fieldname in message.keys():
            message[fieldname] = isots_to_tsms(message[fieldname])
    message["delivery_datetime"] = now_to_tsms()
    return message


def format_timestamps_mongodb(message: dict) -> dict:
    """Format datetimes into mongodb datetime from a dict
    Parameters
    ----------
    message:
        dict with datetime values to be formated
    Returns
    -------
    dict
        Dict with datetimes formated
    """
    for fieldname in const.TIMESTAMP_FIELDS:
        if fieldname in message.keys():
            message[fieldname] = parser.parse(message[fieldname])
    message["delivery_datetime"] = parser.parse(
        datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
    return message


def multivalue_to_list(fieldvalue: str, sep=',') -> list:
    if (fieldvalue is None) or (fieldvalue == ''):
        retval = []
    else:
        allvals = fieldvalue.split(sep)
        allvals = [x for x in allvals if x != '']
        retval = list(set(allvals))  # Removes duplicates
    return retval


def format_multivalues(message: dict) -> dict:
    """Format multivalues from a dict
    Parameters
    ----------
    message:
        dict with multivalues values to be formated
    Returns
    -------
    dict
        Dict with multivalues formated
    """
    for fieldname in const.MULTIVALUE_FIELDS_SPACE:
        if fieldname in message.keys():
            message[fieldname] = multivalue_to_list(message[fieldname],
                                                    sep=' ')
    for fieldname in const.MULTIVALUE_FIELDS_COMMA:
        if fieldname in message.keys():
            message[fieldname] = multivalue_to_list(message[fieldname])
    return message


def create_path_if_not_exist(path):
    """Check if a path exist, if not then is created
    Parameters
    ----------
    path:
        path to create
    Returns
    -------
    dict
        Dict with multivalues formated
    """
    if not os.path.exists(path):
        os.makedirs(path)
