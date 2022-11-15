"""
    Module to handle the API and other requests
"""
import json
import os
from datetime import datetime

import requests

from .. import const, tools
from ..log import factiva_logger, get_factiva_logger

log = get_factiva_logger()

def send_get_request(endpoint_url=const.API_HOST,
                     headers=None,
                     qs_params=None,
                     stream=False):
    """Send get request."""
    if (qs_params is not None) and (not isinstance(qs_params, dict)):
        raise ValueError('Unexpected qs_params value')
    return requests.get(endpoint_url,
                        headers=headers,
                        params=qs_params,
                        stream=stream)


def send_post_request(endpoint_url=const.API_HOST, headers=None, payload=None):
    """Send post request."""
    if payload is not None:
        if isinstance(payload, dict):
            payload_str = json.dumps(payload)
        elif isinstance(payload, str):
            payload_str = payload
        else:
            raise ValueError('Unexpected payload value')
        return requests.post(endpoint_url, headers=headers, data=payload_str)

    return requests.post(endpoint_url, headers=headers)

@factiva_logger()
def api_send_request(method='GET',
                     endpoint_url=const.API_HOST,
                     headers=None,
                     payload=None,
                     qs_params=None,
                     stream=False):
    """Send a generic request to a certain API end point."""
    if headers is None:
        raise ValueError('Heders for Factiva requests cannot be empty')

    if not isinstance(headers, dict):
        raise ValueError('Unexpected headers value')

    try:
        if method == 'GET':
            response = send_get_request(endpoint_url=endpoint_url,
                                        headers=headers,
                                        qs_params=qs_params,
                                        stream=stream)

        elif method == 'POST':
            response = send_post_request(endpoint_url=endpoint_url,
                                         headers=headers,
                                         payload=payload)

        elif method == 'DELETE':
            response = requests.delete(endpoint_url, headers=headers)

        else:
            raise ValueError('Unexpected method value')

    except Exception:
        raise RuntimeError('API Request failed. Unspecified Error.')

    return response


@factiva_logger()
def download_file(file_url,
                  headers,
                  file_name,
                  file_extension,
                  to_save_path,
                  add_timestamp=False) -> str:
    """Download a file on a specific path.
    
    Parameters
    ----------
    file_url : str
        URL of the file to be downloaded.
    headers : dict
        Auth headers
    file_name : str
        Name to be used as local filename
    file_extension : str
        Extension of the file
    to_save_path : str
        Path to be used to store the file
    add_timestamp : bool, optional
        Flag to determine if include timestamp info at the filename
    
    Returns
    -------
    str
        Dowloaded file path
    """

    tools.validate_field_options(file_extension,
                                 const.API_EXTRACTION_FILE_FORMATS)

    tools.create_path_if_not_exist(to_save_path)

    if add_timestamp:
        file_name = f'{file_name}-{datetime.now()}'

    response = send_get_request(endpoint_url=file_url,
                                headers=headers,
                                stream=True)

    local_file_name = os.path.join(to_save_path,
                                   f'{file_name}.{file_extension}')
    with open(local_file_name, 'wb') as f:
        f.write(response.content)

    return local_file_name
