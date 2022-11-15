"""Factiva Core User Stream Class."""

import json

from factiva.core import UserKey, const
from google.cloud.pubsub_v1 import SubscriberClient
from google.oauth2 import service_account

from .log import factiva_logger, get_factiva_logger
from .req import api_send_request


class StreamUser(UserKey):
    """
    Implements the class that represents an Stream user.

    This entity is an extension from the APIKeyUser class.
    This class is capable of consulting the list streams from a given user
    It can authenticate for obtaining the required credentials for Pubsub
    The authentication can be with OAuth or User Key

    Parameters
    ----------
    key : str
        String containing the 32-character long APi Key. If not provided, the
        constructor will try to obtain its value from the FACTIVA_APIKEY
        environment variable.
    request_info : boolean, optional (Default: False)
        Indicates if user data has to be pulled from the server.
        This operation fills account detail properties along with maximum,
        used and remaining values. It may take several seconds to complete.

    See Also
    --------
    UserOAuth : API user that follows the OAuth guidelines.

    Examples
    --------
    Creates a new stream user.
        >>> stream_user = StreamUser(
                key='abcd1234abcd1234abcd1234abcd1234',
                request_info=False
            )
            print(stream_user.get_streams())

    """

    __API_ACCOUNT_STREAM_CREDENTIALS_BASEPATH = const.API_ACCOUNT_STREAM_CREDENTIALS_BASEPATH
    __DEFAULT_HOST_DNA = f'{const.API_HOST}{const.DNA_BASEPATH}'
    __DEFAULT_HOST_ALPHA = f'{const.API_HOST}{const.ALPHA_BASEPATH}'

    # __API_ENDPOINT_STREAM_URL = f'{const.API_HOST}{const.API_STREAMS_BASEPATH}/'

    def __init__(
        self,
        key=None,
        request_info=False,
    ):
        """Construct the object instance."""
        super().__init__(key, request_info)
        self.log = get_factiva_logger()

    # # TODO: Please remove as this is a functionality that belongs to the News package
    # def get_streams(self) -> pd.DataFrame:
    #     """Obtain streams from a given user.

    #     Function which returns the streams a given user with
    #     its respective key using the default stream url

    #     Returns
    #     -------
    #     Json object -> list of objects containing
    #     information about every stream (id, link, state, etc)

    #     Raises
    #     ------
    #     AttributeError:
    #         When is not possible to parse the data as json or dataframe
    #     ValueError:
    #         When API key is not valid
    #     RuntimeError:
    #         When API request returns unexpected error

    #     """
    #     request_headers = {'user-key': self.key}
    #     response = api_send_request(
    #         method="GET",
    #         endpoint_url=self.__API_ENDPOINT_STREAM_URL,
    #         headers=request_headers
    #     )
    #     if response.status_code == 200:
    #         try:
    #             response_data = response.json()

    #             return [StreamResponse(data=stream, links=stream.get('links', None)) for stream in response_data['data']]
    #         except Exception:
    #             raise AttributeError('Unexpected Get Streams API Response.')
    #     elif response.status_code == 403:
    #         raise ValueError('Factiva API-Key does not exist or inactive.')
    #     else:
    #         raise RuntimeError('Unexpected Get Streams API Error')

    @factiva_logger()
    def fetch_credentials(self) -> dict:
        """Fetch the current headers and uri (v1 or v2).

        Then it is executed a call to an api with the given header and uri
        The result is a json with the streaming credentials for Pubsub

        Returns
        -------
        Json object -> credentials in json format obtained
        from Account Streams Credentials endpoint

        Raises
        ------
        ValueError: When the credentials are not valid
        RuntimeError: When API request returns unexpected error

        """
        headers = self.get_authentication_headers()
        uri = self.get_uri_context()
        endpoint_url = '{}{}'.format(
            uri, self.__API_ACCOUNT_STREAM_CREDENTIALS_BASEPATH)

        response = api_send_request(method="GET",
                                    endpoint_url=endpoint_url,
                                    headers=headers)

        if response.status_code == 401:
            message = '''
                Extraction API authentication failed for given
                credentials header:{}
                '''.format(headers)
            raise RuntimeError(message)
        try:
            response_data = response.json()
            streaming_credentials_string = response_data['data']['attributes'][
                'streaming_credentials']
        except KeyError:
            raise ValueError('''
                Unable to find streaming credentials for given account
                ''')

        return json.loads(streaming_credentials_string)

    def get_uri_context(self) -> str:
        """Obtain the current headers, return the uri based on the headers used.

        Returns
        -------
        uri in string format which can be used for
        OAuth or User-Key Authentication

        Raises
        ------
        ValueError: When the headers doesn't have
            OAuth credentials or user key

        """
        headers = self.get_authentication_headers()
        if "Authorization" in headers:
            return self.__DEFAULT_HOST_DNA

        if 'user-key' in headers:
            return self.__DEFAULT_HOST_ALPHA

        msg = '''
        Could not determine user credentials:
        Must specify account credentials as key
        through env vars
        (see README.rst)
        '''
        raise ValueError(msg)

    @factiva_logger()
    def get_client_subscription(self) -> SubscriberClient:
        """Obtain the subscriber client for pubsub.

        The credentials are obtained from fetch_credentials() function
        These credentials are used to authenticate with Google services
        If all is correct, it will be created a SubscriberClient for Pubsub

        Returns
        -------
        SubscriberClient object from google cloud library used for Pubsub

        Raises
        ------
        RuntimeError: When the Pubsub client cannot be created

        """
        streaming_credentials = self.fetch_credentials()
        try:
            credentials = service_account.Credentials.from_service_account_info(
                streaming_credentials)

            return SubscriberClient(credentials=credentials)
        except Exception:
            raise RuntimeError('''
                Something unexpected happened while creating Pubsub client
                ''')

    def get_authentication_headers(self) -> dict:
        """Obtain the current auhtentication headers.

        The headers are the user-key

        Returns
        -------
        Object with User Key

        Raises
        ------
        ValueError: When the Pubsub client cannot be created

        """
        if self.key:
            return {'user-key': self.key}

        msg = '''
            Could not find credentials:
            Must specify account credenstials as key
            (see README.rst)
        '''
        raise ValueError(msg)
