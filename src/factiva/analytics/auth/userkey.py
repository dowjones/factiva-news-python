"""Factiva Core UserKey Class."""
import json
import pandas as pd
from .. import const
from ..log import factiva_logger, get_factiva_logger
from ..req import api_send_request
from ..tools import flatten_dict, load_environment_value, mask_string


class UserKey:
    """Class that represents an API user. This entity is identifiable by a User Key.

    Parameters
    ----------
    key : str
        String containing the 32-character long APi Key. If not provided, the
        constructor will try to obtain its value from the FACTIVA_USERKEY
        environment variable.
    stats : bool, optional (Default: False)
        Indicates if user data has to be pulled from the server. This operation
        fills account detail properties along with maximum, used and remaining
        values. It may take several seconds to complete.

    Examples
    --------
    Creating a new UserKey instance providing the key string explicitly and requesting to retrieve the latest account details:
        >>> u = UserKey('abcd1234abcd1234abcd1234abcd1234', stats=True)
        >>> print(u)
            <class 'factiva.core.userkey.UserKey'>
            |-key = ****************************1234
            |-cloud_token = **Not Fetched**
            |-account_name = AccName1234
            |-account_type = account_with_contract_limits
            |-active_products = DNA
            |-max_allowed_concurrent_extractions = 5
            |-max_allowed_extracted_documents = 200,000
            |-max_allowed_extractions = 3
            |-currently_running_extractions = 0
            |-total_downloaded_bytes = 7,253,890
            |-total_extracted_documents = 2,515
            |-total_extractions = 1
            |-total_stream_instances = 4
            |-total_stream_subscriptions = 1
            |-enabled_company_identifiers = [{'id': 4, 'name': 'isin'}, {'id': 3, 'name': 'cusip'}, {'id': 1, 'name': 'sedol'}, {'id': 5, 'name': 'ticker_exchange'}]
            |-remaining_documents = 197,485
            |-remaining_extractions = 2

    Creating a new instance taking the key value from the environment varaible FACTIVA_USERKEY, and not requesting account statistics (default).
        >>> u = UserKey()
        >>> print(u)
            <class 'factiva.core.userkey.UserKey'>
            |-key = ****************************1234
            |-cloud_token = **Not Fetched**
            |-account_name =
            |-account_type =
            |-active_products =
            |-max_allowed_concurrent_extractions = 0
            |-max_allowed_extracted_documents = 0
            |-max_allowed_extractions = 0
            |-currently_running_extractions = 0
            |-total_downloaded_bytes = 0
            |-total_extracted_documents = 0
            |-total_extractions = 0
            |-total_stream_instances = 0
            |-total_stream_subscriptions = 0
            |-enabled_company_identifiers = []
            |-remaining_documents = 0
            |-remaining_extractions = 0

    """
    # pylint: disable=too-many-instance-attributes
    # Twelve is reasonable in this case.

    __API_ENDPOINT_BASEURL = f'{const.API_HOST}{const.API_ACCOUNT_BASEPATH}/'
    __API_CLOUD_TOKEN_URL = f'{const.API_HOST}{const.ALPHA_BASEPATH}{const.API_ACCOUNT_STREAM_CREDENTIALS_BASEPATH}'

    key = ''
    cloud_token = {}
    account_name = ''
    account_type = ''
    active_products = ''
    max_allowed_concurrent_extractions = 0
    max_allowed_extracted_documents = 0
    max_allowed_extractions = 0
    currently_running_extractions = 0
    total_downloaded_bytes = 0
    total_extracted_documents = 0
    total_extractions = 0
    total_stream_instances = 0
    total_stream_subscriptions = 0
    enabled_company_identifiers = []

    def __init__(self, key=None, stats=False):
        """Construct the instance of the class."""
        self.log = get_factiva_logger()
        if key is None:
            try:
                key = load_environment_value('FACTIVA_USERKEY')
            except Exception as error:
                raise ValueError(
                    'Factiva user key not provided and environment variable FACTIVA_USERKEY not set.'
                ) from error

        if len(key) != 32:
            raise ValueError('Factiva User-Key has the wrong length')

        self.key = key
        self.cloud_token = {}

        if stats is True:
            self.get_stats()
        else:
            self.account_name = ''
            self.account_type = ''
            self.active_products = ''
            self.max_allowed_concurrent_extractions = 0
            self.max_allowed_extracted_documents = 0
            self.max_allowed_extractions = 0
            self.currently_running_extractions = 0
            self.total_downloaded_bytes = 0
            self.total_extracted_documents = 0
            self.total_extractions = 0
            self.total_stream_instances = 0
            self.total_stream_subscriptions = 0
            self.enabled_company_identifiers = []


    @property
    def remaining_extractions(self):
        """Account remaining extractions."""
        return self.max_allowed_extractions - self.total_extractions


    @property
    def remaining_documents(self):
        """Account remaining documents."""
        return self.max_allowed_extracted_documents - self.total_extracted_documents

    @property
    def extractions_done(self):
        """Number of executed extractions"""
        return self.get_extractions()

    @property
    def streams_running(self):
        """Number of currently running Streaming Instances"""
        return self.get_streams()

    @factiva_logger()
    def get_stats(self) -> bool:
        """Request the account details to the Factiva Account API Endpoint.

        This operation can take several seconds to complete.

        Returns
        -------
        True if the operation was completed successfully. All returned values
        are assigned to the object's properties directly.

        Examples
        --------
        >>> u = UserKey('abcd1234abcd1234abcd1234abcd1234')
        >>> print(u)
            <class 'factiva.core.userkey.UserKey'>
            |-key = ****************************1234
            |-cloud_token = **Not Fetched**
            |-account_name =
            |-account_type =
            |-active_products =
            |-max_allowed_concurrent_extractions = 0
            |-max_allowed_extracted_documents = 0
            |-max_allowed_extractions = 0
            |-currently_running_extractions = 0
            |-total_downloaded_bytes = 0
            |-total_extracted_documents = 0
            |-total_extractions = 0
            |-total_stream_instances = 0
            |-total_stream_subscriptions = 0
            |-enabled_company_identifiers = []
            |-remaining_documents = 0
            |-remaining_extractions = 0
        >>> u.get_stats()
        >>> print(u)
            <class 'factiva.core.userkey.UserKey'>
            |-key = ****************************1234
            |-cloud_token = **Not Fetched**
            |-account_name = AccName1234
            |-account_type = account_with_contract_limits
            |-active_products = DNA
            |-max_allowed_concurrent_extractions = 5
            |-max_allowed_extracted_documents = 200,000
            |-max_allowed_extractions = 3
            |-currently_running_extractions = 0
            |-total_downloaded_bytes = 7,253,890
            |-total_extracted_documents = 2,515
            |-total_extractions = 1
            |-total_stream_instances = 4
            |-total_stream_subscriptions = 1
            |-enabled_company_identifiers = [{'id': 4, 'name': 'isin'}, {'id': 3, 'name': 'cusip'}, {'id': 1, 'name': 'sedol'}, {'id': 5, 'name': 'ticker_exchange'}]
            |-remaining_documents = 197,485
            |-remaining_extractions = 2

        """
        account_endpoint = f'{self.__API_ENDPOINT_BASEURL}{self.key}'
        req_head = {'user-key': self.key}
        resp = api_send_request(method='GET', endpoint_url=account_endpoint, headers=req_head)
        if resp.status_code == 200:
            try:
                resp_obj = json.loads(resp.text)
                self.account_name = resp_obj['data']['attributes']['name']
                self.account_type = resp_obj['data']['type']
                self.active_products = resp_obj['data']['attributes']['products']
                self.max_allowed_concurrent_extractions = resp_obj['data']['attributes']['max_allowed_concurrent_extracts']
                self.max_allowed_extracted_documents = resp_obj['data']['attributes']['max_allowed_document_extracts']
                self.max_allowed_extractions = resp_obj['data']['attributes']['max_allowed_extracts']
                self.currently_running_extractions = resp_obj['data']['attributes']['cnt_curr_ext']
                self.total_downloaded_bytes = resp_obj['data']['attributes']['current_downloaded_amount']
                self.total_extracted_documents = resp_obj['data']['attributes']['tot_document_extracts']
                self.total_extractions = resp_obj['data']['attributes']['tot_extracts']
                self.total_stream_instances = resp_obj['data']['attributes']['tot_topics']
                self.total_stream_subscriptions = resp_obj['data']['attributes']['tot_subscriptions']
                self.enabled_company_identifiers = resp_obj['data']['attributes']['enabled_company_identifiers']
            except Exception as error:
                raise AttributeError('Unexpected Account Information API Response.') from error
        elif resp.status_code == 403:
            raise ValueError('Factiva User-Key does not exist or inactive.')
        else:
            raise RuntimeError('Unexpected Account Information API Error')
        return True

    @factiva_logger()
    def get_cloud_token(self) -> bool:
        """
        Request a cloud token to the API and saves its value
        in the cloud_token property

        Returns
        -------
        True if the operation was completed successfully. Returned value
        is assigned to the cloud_token property.

        Raises
        ------
        ValueError: When the credentials are not valid
        RuntimeError: When API request returns unexpected error

        """
        req_head = {'user-key': self.key}
        response = api_send_request(
            method="GET",
            endpoint_url=f'{self.__API_CLOUD_TOKEN_URL}',
            headers=req_head
        )

        if response.status_code == 401:
            message = '''
                Extraction API authentication failed for given
                credentials header:{}
                '''.format(req_head)
            raise RuntimeError(message)
        try:
            streaming_credentials_string = response.json()['data']['attributes']['streaming_credentials']
        except TypeError as type_error:
            raise ValueError('Unable to get a cloud token for the given key. This account might have limited access.') from type_error

        self.cloud_token = json.loads(streaming_credentials_string)
        return True

    @factiva_logger()
    def get_extractions(self, updates=False) -> pd.DataFrame:
        """Request a list of the extractions of the account.

        Parameters
        ----------
        updates : bool
            Flag that indicates whether the retrieved list should include (True)
            or not (False) Snapshot Update calls.

        Returns
        -------
        Dataframe containing the list of historical extractions for the account.

        Raises
        ------
        - ValueError when the API Key provided is not valid
        - RuntimeError when the API returns an unexpected error

        """
        endpoint = f'{const.API_HOST}{const.API_EXTRACTIONS_BASEPATH}'

        headers_dict = {'user-key': self.key}

        response = api_send_request(method='GET', endpoint_url=endpoint, headers=headers_dict)

        if response.status_code != 200:
            if response.status_code == 403:
                raise ValueError('Factiva API-Key does not exist or inactive.')

            raise RuntimeError(f'Unexpected API Error with message: {response.text}')

        response_data = response.json()

        extraction_df = pd.DataFrame([flatten_dict(extraction) for extraction in response_data['data']])
        extraction_df.rename(columns={'id': 'object_id'}, inplace=True)
        ids_df = extraction_df['object_id'].str.split('-', expand=True)

        if ids_df.shape[1] >= 5:
            extraction_df['snapshot_sid'] = ids_df[4]
        else:
            extraction_df['snapshot_sid'] = None

        if ids_df.shape[1] >= 7:
            extraction_df['update_id'] = ids_df[6]
        else:
            extraction_df['update_id'] = None

        extraction_df.drop(['self', 'type'], axis=1, inplace=True)

        if not updates:
            extraction_df = extraction_df.loc[extraction_df.update_id.isnull()]

        return extraction_df


    def show_extractions(self, updates=False):
        """Shows a list of the extractions of the account.

        Parameters
        ----------
        updates : bool
            Flag that indicates whether the displayed list should include (True)
            or not (False) Snapshot Update calls.

        Returns
        -------
        Dataframe containing the list of historical extractions for the account.

        Raises
        ------
        - ValueError when the API Key provided is not valid
        - RuntimeError when the API returns an unexpected error

        """
        extractions = self.get_extractions(updates=updates)
        print(extractions.loc[:, extractions.columns != 'object_id'])


    @factiva_logger()
    def get_streams(self, running=True) -> pd.DataFrame:
        """Obtain streams from a given user.

        Function which returns the streams a given user with
        its respective key using the default stream url

        Parameters
        ----------
        running : bool
            Flag that indicates whether the retrieved list should be restricted
            to only running streams (True) or also include cancelled and failed
            ones (False).

        Returns
        -------
        DataFrame -> DataFrame with the list of historical extractions

        Raises
        ------
        AttributeError:
            When is not possible to parse the data as json or dataframe
        ValueError:
            When API key is not valid
        RuntimeError:
            When API request returns unexpected error

        """
        request_headers = {'user-key': self.key}
        response = api_send_request(
            method="GET",
            endpoint_url=f'{const.API_HOST}{const.API_STREAMS_BASEPATH}',
            headers=request_headers
        )
        if response.status_code == 200:
            try:
                def extract_subscriptions(subscription):
                    # Fixed issue#12 (https://github.com/dowjones/factiva-core-python/issues/12)
                    id_list = []
                    for i in subscription:
                        s_idp = i['id'].split('-')
                        s_id = f"{s_idp[-3]}-{s_idp[-2]}-{s_idp[-1]}"
                        id_list.append(s_id)
                    return id_list

                response_data = response.json()
                stream_df = pd.DataFrame([flatten_dict(extraction) for extraction in response_data['data']])
                stream_df.rename(columns={'id': 'object_id'}, inplace=True)
                ids_df = stream_df['object_id'].str.split('-', expand=True)
                stream_df['stream_id'] = ids_df[4]
                stream_df['stream_type'] = ids_df[2]
                stream_df['subscriptions'] = stream_df['data'].apply(extract_subscriptions)
                stream_df['n_subscriptions'] = stream_df['subscriptions'].str.len()
                stream_df.drop(['self', 'type', 'data'], axis=1, inplace=True)

                if running:
                    stream_df = stream_df.loc[stream_df.job_status == const.API_JOB_RUNNING_STATE]

                return stream_df
            except Exception as error:
                raise AttributeError('Unexpected Get Streams API Response.') from error
        elif response.status_code == 403:
            raise ValueError('Factiva API-Key does not exist or inactive.')
        else:
            raise RuntimeError('Unexpected Get Streams API Error')


    def show_streams(self, running=True):
        """Shows the list of streams for a given user.

        This function runs the existing function get_streams and
        prints a user-friendly table with stream details.

        Parameters
        ----------
        running : bool
            Flag that indicates whether the displayed list should be restricted
            to only running streams (True) or also include cancelled and failed
            ones (False).

        Returns
        -------
        None

        Raises
        ------
        AttributeError:
            When is not possible to parse the data as json or dataframe
        ValueError:
            When API key is not valid
        RuntimeError:
            When API request returns unexpected error

        """
        account_streams = self.get_streams(running=running)
        print(account_streams.loc[:, account_streams.columns != 'object_id'])


    def is_active(self) -> bool:
        request_headers = {'user-key': self.key}
        response = api_send_request(
            method="GET",
            endpoint_url=f'{const.API_HOST}{const.API_SNAPSHOTS_TAXONOMY_BASEPATH}',
            headers=request_headers
        )
        if response.status_code == 200:
            return True
        else:
            return False


    def __print_property__(self, property_value) -> str:
        if isinstance(property_value, int):
            pval = f'{property_value:,d}'
        else:
            pval = property_value
        return pval


    def __repr__(self):
        """Return a string representation of the object."""
        return self.__str__()


    def __str__(self, detailed=True, prefix='  |-', root_prefix=''):
        # TODO: Improve the output for enabled_company_identifiers
        pprop = self.__dict__.copy()
        del pprop['key']
        del pprop['cloud_token']
        masked_key = mask_string(self.key)
        if self.cloud_token == {}:
            masked_token = '**Not Fetched**'
        else:
            masked_token = mask_string(self.cloud_token['private_key'][58:92], 12)

        ret_val = f'{root_prefix}{str(self.__class__)}\n'
        ret_val += f'{prefix}key = {masked_key}\n'
        ret_val += f'{prefix}cloud_token = {masked_token}\n'
        if detailed:
            ret_val += '\n'.join((f'{prefix}{item} = {self.__print_property__(pprop[item])}' for item in pprop))
            ret_val += f'\n{prefix}remaining_documents = {self.__print_property__(self.remaining_documents)}\n'
            ret_val += f'{prefix}remaining_extractions = {self.__print_property__(self.remaining_extractions)}\n'
        else:
            ret_val += f'{prefix}...'
        return ret_val

    @staticmethod
    def create_user_key(key=None, stats=False):
        """Determine the way to initialize an api key user according to the type of parameter provided.

        Parameters
        ----------
        api_user : None, str, UserKey
                Source to create a UserKey instance
        stats : boolean, optional (Default: False)
                Indicates if user data has to be pulled from the server

        Returns
        -------
        UserKey instance accordingly:
            - When None is passed, UserKey instance using credentials from ENV variables
            - When str is passed, UserKey instance using the provided parameter as credentials
            - When UserKey is passed, it returns the same instance

        Raises
        ------
            RuntimeError: When an UserKey instance cannot be created using the provided parameters

        """
        if isinstance(key, UserKey):
            return key

        if isinstance(key, str):
            try:
                return UserKey(key, stats=stats)
            except Exception as error:
                raise RuntimeError("User cannot be obtained from the provided key.") from error

        if key is None:
            try:
                return UserKey(stats=stats)
            except Exception as error:
                raise RuntimeError("User cannot be obtained from ENV variables") from error

        raise RuntimeError("Unexpected api_user value")
