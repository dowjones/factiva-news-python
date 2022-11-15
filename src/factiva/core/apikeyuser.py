"""Factiva Core API Key User Class."""
import requests
import pandas as pd

from .req import api_send_request
from .tools import load_environment_value, mask_string, flatten_dict
from factiva.core import const


class APIKeyUser:  # TODO: Create a DJUserBase class that defines root properties for all user types, and inherit here.
    """Class that represents an API user. This entity is identifiable by an API-Key.

    Parameters
    ----------
    key : str
        String containing the 32-character long APi Key. If not provided, the
        constructor will try to obtain its value from the FACTIVA_APIKEY
        environment variable.
    request_info : boolean, optional (Default: False)
        Indicates if user data has to be pulled from the server. This operation
        fills account detail properties along with maximum, used and remaining
        values. It may take several seconds to complete.

    See Also
    --------
    UserOAuth: API user that follows the OAuth guidelines.

    Examples
    --------
    Creating a new API user providing the API-Key explicitly and requesting to retrieve the latest account details:
        >>> aku = APIKeyUser('abcd1234abcd1234abcd1234abcd1234', request_info=True)
        >>> print(aku)
        (class 'factiva.core.apikeyuser.APIKeyUser')
            key = ****************************1234
            account_name = Demo Account
            account_type = account_with_limits
            active_products = Snapshots
            max_allowed_concurrent_extractions = 2
            max_allowed_extracted_documents = 100000
            max_allowed_extractions = 10
            total_downloaded_bytes = 12345678
            total_extracted_documents = 5500
            total_extractions = 2
            total_stream_subscriptions = 2
            total_stream_topics = 1
            remaining_documents = 94500
            remaining_extractions = 8
    Creating a new instance taking the api-key value from the environment varaible FACTIVA_APIKEY, and not requesting any account details.
        >>> aku = APIKeyUser()
        >>> print(aku)
        (class 'factiva.core.apikeyuser.APIKeyUser')
            key = ****************************1234
            account_name =
            account_type =
            active_products =
            max_allowed_concurrent_extractions = 0
            max_allowed_extracted_documents = 0
            max_allowed_extractions = 0
            total_downloaded_bytes = 0
            total_extracted_documents = 0
            total_extractions = 0
            total_stream_subscriptions = 0
            total_stream_topics = 0
            remaining_documents = 0
            remaining_extractions = 0

    """
    # pylint: disable=too-many-instance-attributes
    # Twelve is reasonable in this case.

    __API_ENDPOINT_BASEURL = f'{const.API_HOST}{const.API_ACCOUNT_BASEPATH}/'

    key = ''
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
    total_stream_subscriptions = 0
    total_stream_topics = 0
    enabled_company_identifiers = []

    def __init__(
        self,
        key=None,
        request_info=False
    ):
        """Construct the instance of the class."""
        if key is None:
            try:
                key = load_environment_value('FACTIVA_APIKEY')
            except Exception:
                raise ValueError('Factiva API-Key not provided and environment variable FACTIVA_APIKEY not set.')

        if len(key) != 32:
            raise ValueError('Factiva API-Key has the wrong length')

        self.key = key

        if request_info is True:
            self.get_info()
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
            self.total_stream_subscriptions = 0
            self.total_stream_topics = 0
            self.enabled_company_identifiers = []

    @property
    def remaining_extractions(self):
        """Account remaining extractions."""
        return self.max_allowed_extractions - self.total_extractions

    @property
    def remaining_documents(self):
        """Account remaining documents."""
        return self.max_allowed_extracted_documents - self.total_extracted_documents

    # [MB] TODO: Please move this method to factiva-news/snapshot/snapshot.py
    # The number of extractions is OK under this class for account management
    # information (get_info), but the extraction list belongs to the Snapshot class.
    def get_extractions(self) -> pd.DataFrame:
        """Request a list of the extractions of the account.

        Returns
        -------
        Dataframe containing the information about the account extractions

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
        extraction_df['snapshot_sid'] = ids_df[4]
        extraction_df['update_id'] = ids_df[6]
        extraction_df.drop(['id', 'self', 'type'], axis=1, inplace=True)

        return extraction_df

    def get_info(self):
        """Request the account details to the Factiva Account API Endpoint.

        This operation can take several seconds to complete.

        Returns
        -------
        True if the operation was completed successfully. All returned values
        are set to the object's properties directly.

        Examples
        --------
        >>> aku = APIKeyUser('abcd1234abcd1234abcd1234abcd1234')
        >>> aku
        (class 'factiva.core.apikeyuser.APIKeyUser')
            key = ****************************1234
            account_name =
            account_type =
            active_products =
            max_allowed_concurrent_extractions = 0
            max_allowed_extracted_documents = 0
            max_allowed_extractions = 0
            total_downloaded_bytes = 0
            total_extracted_documents = 0
            total_extractions = 0
            total_stream_subscriptions = 0
            total_stream_topics = 0
            remaining_documents = 0
            remaining_extractions = 0
        >>> aku.get_info()
        >>> aku
        (class 'factiva.core.apikeyuser.APIKeyUser')
            key = ****************************1234
            account_name = Demo Account
            account_type = account_with_limits
            active_products = Snapshots
            max_allowed_concurrent_extractions = 2
            max_allowed_extracted_documents = 100000
            max_allowed_extractions = 10
            total_downloaded_bytes = 12345678
            total_extracted_documents = 5500
            total_extractions = 2
            total_stream_subscriptions = 2
            total_stream_topics = 1
            remaining_documents = 94500
            remaining_extractions = 8

        """
        account_endpoint = f'{self.__API_ENDPOINT_BASEURL}{self.key}'
        req_head = {'user-key': self.key}
        resp = requests.get(account_endpoint, headers=req_head)  # TODO: Consider processing all GET/POST requests in a separate class/module
        if resp.status_code == 200:
            try:
                resp_obj = eval(resp.text)
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
                self.total_stream_subscriptions = resp_obj['data']['attributes']['tot_subscriptions']
                self.total_stream_topics = resp_obj['data']['attributes']['tot_topics']
                self.enabled_company_identifiers = resp_obj['data']['attributes']['enabled_company_identifiers']
            except Exception:
                raise AttributeError('Unexpected Account Information API Response.')
        elif resp.status_code == 403:
            raise ValueError('Factiva API-Key does not exist or inactive.')
        else:
            raise RuntimeError('Unexpected Account Information API Error')
        return True

    def __print_property__(self, property_value) -> str:
        if type(property_value) == int:
            pval = f'{property_value:,d}'
        else:
            pval = property_value
        return pval

    def __repr__(self):
        """Return a string representation of the object."""
        return self.__str__()

    def __str__(self, detailed=True, prefix='  |-', root_prefix=''):
        pprop = self.__dict__.copy()
        del pprop['key']
        masked_key = mask_string(self.__dict__['key'])

        ret_val = f'{root_prefix}{str(self.__class__)}\n'
        ret_val += f'{prefix}key = {masked_key}\n'
        if detailed:
            ret_val += '\n'.join((f'{prefix}{item} = {self.__print_property__(pprop[item])}' for item in pprop))
            ret_val += f'\n{prefix}remaining_documents = {self.__print_property__(self.remaining_documents)}\n'
            ret_val += f'{prefix}remaining_extractions = {self.__print_property__(self.remaining_extractions)}\n'
        else:
            ret_val += f'{prefix}...'
        return ret_val

    @staticmethod
    def create_api_user(api_user, request_userinfo):
        """Determine the way to initialize an api key user according to the type of parameter provided.

        Parameters
        ----------
        api_user: None, str or APIKeyUser.
                Source to create an instance of APIKeyUser
        request_userinfo
                Indicates if user data has to be pulled from the server

        Returns
        -------
        APIKeyUser instance accordingly:
            - When None is passed, APIKeyUser instance using credentials from ENV variables
            - When str is passed, APIKeyUser instance using the provided parameter as credentials
            - When ApiKeyUser is passed, this same instance is returned

        Raises
        ------
            RuntimeError: When an APIKeyUser instance cannot be created using the provided argument

        """
        if isinstance(api_user, APIKeyUser):
            return api_user

        if isinstance(api_user, str):
            try:
                return APIKeyUser(api_user, request_info=request_userinfo)
            except Exception:
                raise RuntimeError("User cannot be obtained from the provided key.")

        if api_user is None:
            try:
                return APIKeyUser(request_info=request_userinfo)
            except Exception:
                raise RuntimeError("User cannot be obtained from ENV variables")

        raise RuntimeError("Unexpected api_user value")
