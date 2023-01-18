"""
    Module to handle the API and other requests
"""
import json
import os
from datetime import datetime
import requests
from . import tools
from . import const
from ...analytics import __version__
from .log import factiva_logger, get_factiva_logger

__log = get_factiva_logger()

@factiva_logger
def _send_get_request(endpoint_url:str=const.API_HOST,
                     headers:dict=None,
                     qs_params=None,
                     stream:bool=False):
    """Send get request."""
    if (qs_params is not None) and (not isinstance(qs_params, dict)):
        raise ValueError('_send_get_request: Unexpected qs_params value')
    get_response = requests.get(endpoint_url,
                        headers=headers,
                        params=qs_params,
                        stream=stream)
    if get_response.status_code >= 400:
        __log.error(f'GET Request Error [{get_response.status_code}]: {get_response.text}')
    return get_response

@factiva_logger
def _send_post_request(endpoint_url:str=const.API_HOST,
                       headers:dict=None,
                       payload=None):
    """Send post request."""
    if payload is not None:
        if isinstance(payload, dict):
            payload_str = json.dumps(payload)
        elif isinstance(payload, str):
            payload_str = payload
        else:
            raise ValueError('Unexpected payload value')

        __log.debug(f'POST request with payload - Start')
        post_response = requests.post(endpoint_url, headers=headers, data=payload_str)
        if post_response.status_code >= 400:
            __log.error(f'POST Request Error [{post_response.status_code}]: {post_response.text}')
        __log.debug(f'POST request with Payload - End')
        return post_response

    __log.debug(f'POST request NO payload - Start')
    post_response = requests.post(endpoint_url, headers=headers)
    if post_response.status_code >= 400:
        __log.error(f'POST Request Error [{post_response.status_code}]: {post_response.text}')
    __log.debug(f'POST request NO Payload - End')
    return post_response

@factiva_logger
def api_send_request(method:str='GET',
                     endpoint_url:str=const.API_HOST,
                     headers:dict=None,
                     payload=None,
                     qs_params=None,
                     stream:bool=False):
    """Send a generic request to a certain API end point."""
    if headers is None:
        raise ValueError('Factiva requests headers cannot be empty')

    if not isinstance(headers, dict):
        raise ValueError('Unexpected headers value')

    vsum = 'f4c71v4f4c71v4f4c71v4f4c71v4f4c7'
    if 'user-key' in headers:
        vsum = tools.md5hash(headers['user-key'])

    headers.update({
        'User-Agent': f'RDL-Python-{__version__}-{vsum}'
    })

    __log.debug(f"{method} Request with User-Agent {headers['User-Agent']}")

    try:
        if method == 'GET':
            response = _send_get_request(endpoint_url=endpoint_url,
                                        headers=headers,
                                        qs_params=qs_params,
                                        stream=stream)

        elif method == 'POST':
            response = _send_post_request(endpoint_url=endpoint_url,
                                         headers=headers,
                                         payload=payload)

        elif method == 'DELETE':
            response = requests.delete(endpoint_url, headers=headers)

        else:
            raise ValueError('api_send_request: Unexpected method value')

        __log.debug(f"{method} request status: {response.status_code}, in: {response.elapsed.microseconds/1000} ms")

    except Exception:
        raise RuntimeError('api_send_request: API Request failed. Unspecified Error.')

    return response


def download_file(file_url:str,
                  headers:dict,
                  file_name:str,
                  file_extension:str,
                  to_save_path:str,
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

    vsum = 'f4c71v4f4c71v4f4c71v4f4c71v4f4c7'
    if 'user-key' in headers:
        vsum = tools.md5hash(headers['user-key'])

    headers.update({
        'User-Agent': f'RDL-Python-{__version__}-{vsum}'
    })

    response = _send_get_request(endpoint_url=file_url,
                                headers=headers,
                                stream=True)

    local_file_name = os.path.join(to_save_path,
                                   f'{file_name}.{file_extension}')
    with open(local_file_name, 'wb') as f:
        f.write(response.content)

    return local_file_name
