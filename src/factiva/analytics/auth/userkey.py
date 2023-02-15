"""
This module contains classes and tools to instantiate and support the lifecycle
of UserKey objects. UserKey is the most used authentication method within
Factiva Analytics APIs.
"""
import json
import pandas as pd
from ..common import log, req, tools, const, config


class UserKey:
    """
    Class that represents an API user and can be instantiated based on the
    user-key value provided by the Dow Jones Developer Support team.

    """

    __API_ENDPOINT_BASEURL = f'{const.API_HOST}{const.API_ACCOUNT_BASEPATH}/'
    __API_CLOUD_TOKEN_URL = f'{const.API_HOST}{const.ALPHA_BASEPATH}{const.API_ACCOUNT_STREAM_CREDENTIALS_BASEPATH}'
    __log = None
    
    key: str = None
    cloud_token: dict = None
    account_name: str = None
    account_type: str = None
    active_product: str = None
    max_allowed_concurrent_extractions: int = None
    max_allowed_extracted_documents: int = None
    max_allowed_extractions: int = None
    currently_running_extractions: int = None
    total_downloaded_bytes: int = None
    total_extracted_documents: int = None
    total_extractions: int = None
    total_stream_instances: int = None
    total_stream_subscriptions: int = None
    enabled_company_identifiers: list = None
    streams: pd.DataFrame = None
    snapshots: pd.DataFrame = None


    def __init__(self, key=None, stats=False):
        """
        Construct the instance of the class
        
        Parameters
        ----------
        key : str
            String containing the 32-character long APi Key. If not provided, the
            constructor will try to obtain its value from the ``FACTIVA_USERKEY``
            environment variable.
        stats : bool
            Indicates if user data has to be pulled from the server at creation
            time (``True``) or just create an instance with no stats data
            (``False`` - default). This operation fills account detail properties
            along with maximum, used and remaining values. It may take several 
            seconds to complete.
        
        Examples
        --------
        Creating a new UserKey instance providing the ``key`` string explicitly and
        retrieving the latest account details:

        .. code-block:: python

            from factiva.analytics import UserKey
            u = UserKey('abcd1234abcd1234abcd1234abcd1234', True)
            print(u)

        .. code-block::

            <class 'factiva.analytics.UserKey'>
            ├─key: ****************************1234
            ├─cloud_token: <NotLoaded>
            ├─account_name: AccName1234
            ├─account_type: account_with_contract_limits
            ├─active_product: DNA
            ├─max_allowed_concurrent_extractions: 5
            ├─max_allowed_extracted_documents: 200,000
            ├─max_allowed_extractions: 3
            ├─currently_running_extractions: 0
            ├─total_downloaded_bytes: 7,253,890
            ├─total_extracted_documents: 2,515
            ├─total_extractions: 1
            ├─total_stream_instances: 4
            ├─total_stream_subscriptions: 1
            ├─enabled_company_identifiers:
            |  ├─[1]: sedol
            |  ├─[3]: cusip
            |  ├─[4]: isin
            |  ├─[5]: ticker_exchange
            ├─remaining_documents: 197,485
            └─remaining_extractions: 2

        Creating a new instance taking the key value from the ``FACTIVA_USERKEY``
        environment varaible, and not requesting account statistics.

        .. code-block:: python

            from factiva.analytics import UserKey
            u = UserKey()
            print(u)

        .. code-block::

            <class 'factiva.analytics.UserKey'>
            ├─key: ****************************1234
            ├─cloud_token: <NotLoaded>
            ├─account_name: <NotLoaded>
            ├─account_type: <NotLoaded>
            ├─active_product: <NotLoaded>
            ├─max_allowed_concurrent_extractions: 0
            ├─max_allowed_extracted_documents: 0
            ├─max_allowed_extractions: 0
            ├─currently_running_extractions: 0
            ├─total_downloaded_bytes: 0
            ├─total_extracted_documents: 0
            ├─total_extractions: 0
            ├─total_stream_instances: 0
            ├─total_stream_subscriptions: 0
            ├─enabled_company_identifiers:
            │  └─<NotLoaded>
            ├─remaining_documents: 0
            └─remaining_extractions: 0

        """
        self.__log = log.get_factiva_logger()
        if key is None:
            try:
                key = config.load_environment_value('FACTIVA_USERKEY')
            except Exception as error:
                raise ValueError(
                    'key parameter not provided and environment variable FACTIVA_USERKEY not set.'
                ) from error

        if len(key) != 32:
            raise ValueError('Factiva User-Key has the wrong length')

        self.key = key
        self.cloud_token = {}

        if stats is True:
            self.get_stats()
        else:
            self.account_name = None
            self.account_type = None
            self.active_product = None
            self.max_allowed_concurrent_extractions = None
            self.max_allowed_extracted_documents = None
            self.max_allowed_extractions = None
            self.currently_running_extractions = None
            self.total_downloaded_bytes = None
            self.total_extracted_documents = None
            self.total_extractions = None
            self.total_stream_instances = None
            self.total_stream_subscriptions = None
            self.enabled_company_identifiers = []
            self.streams = []
            self.snapshots = []


    @property
    def remaining_extractions(self):
        """
        Dynamic property that calculates the account's remaining extractions
        """
        if self.max_allowed_extractions:
            return self.max_allowed_extractions - self.total_extractions
        return None


    @property
    def remaining_documents(self):
        """
        Dynamic property that calculates the account's remaining documents
        """
        if self.max_allowed_extracted_documents:
            return self.max_allowed_extracted_documents - self.total_extracted_documents
        return None

    # @property
    # def extractions_done(self):
    #     """Number of executed extractions"""
    #     return self.get_extractions()

    # @property
    # def streams_running(self):
    #     """Number of currently running Streaming Instances"""
    #     return self.get_streams()


    @log.factiva_logger()
    def get_stats(self) -> bool:
        """
        Request the account details to the Factiva Account API Endpoint.
        This operation can take several seconds to complete.

        Returns
        -------
        bool:
            ``True`` if the operation was completed successfully. ``False``
            otherwise. All returned values are assigned to the object's 
            properties directly.

        Examples
        --------
        Creates a local ``UserKey`` instance and then retrieves the stats.

        .. code-block:: python

            from factiva.analytics import UserKey
            u = UserKey('abcd1234abcd1234abcd1234abcd1234')
            print(u)

        output

        .. code-block::

            <class 'factiva.analytics.UserKey'>
            ├─key: ****************************1234
            ├─cloud_token: <NotLoaded>
            ├─account_name: <NotLoaded>
            ├─account_type: <NotLoaded>
            ├─active_product: <NotLoaded>
            ├─max_allowed_concurrent_extractions: 0
            ├─max_allowed_extracted_documents: 0
            ├─max_allowed_extractions: 0
            ├─currently_running_extractions: 0
            ├─total_downloaded_bytes: 0
            ├─total_extracted_documents: 0
            ├─total_extractions: 0
            ├─total_stream_instances: 0
            ├─total_stream_subscriptions: 0
            ├─enabled_company_identifiers:
            │  └─<NotLoaded>
            ├─remaining_documents: 0
            └─remaining_extractions: 0
        
        .. code-block:: python

            u.get_stats()
            print(u)

        output

        .. code-block::

            <class 'factiva.analytics.UserKey'>
            ├─key: ****************************1234
            ├─cloud_token: <NotLoaded>
            ├─account_name: AccName1234
            ├─account_type: account_with_contract_limits
            ├─active_product: DNA
            ├─max_allowed_concurrent_extractions: 5
            ├─max_allowed_extracted_documents: 200,000
            ├─max_allowed_extractions: 3
            ├─currently_running_extractions: 0
            ├─total_downloaded_bytes: 7,253,890
            ├─total_extracted_documents: 2,515
            ├─total_extractions: 1
            ├─total_stream_instances: 4
            ├─total_stream_subscriptions: 1
            ├─enabled_company_identifiers:
            |  ├─[1]: sedol
            |  ├─[3]: cusip
            |  ├─[4]: isin
            |  ├─[5]: ticker_exchange
            ├─remaining_documents: 197,485
            └─remaining_extractions: 2

        """
        self.__log.info('get_stats started')
        account_endpoint = f'{self.__API_ENDPOINT_BASEURL}{self.key}'
        req_head = {'user-key': self.key}
        resp = req.api_send_request(method='GET', endpoint_url=account_endpoint, headers=req_head)
        if resp.status_code == 200:
            try:
                resp_obj = json.loads(resp.text)
                self.account_name = resp_obj['data']['attributes']['name']
                self.account_type = resp_obj['data']['type']
                self.active_product = resp_obj['data']['attributes']['products']
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
        self.__log.info('get_stats ended')
        return True


    @log.factiva_logger()
    def get_cloud_token(self) -> bool:
        """
        Request a cloud token and stores its content in the ``cloud_token``
        property

        Returns
        -------
        bool:
            ``True`` if the operation was completed successfully. ``False``
            otherwise.

        """
        self.__log.info('get_cloud_token started')
        req_head = {'user-key': self.key}
        response = req.api_send_request(
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
        self.__log.info('get_cloud_token ended')
        return True


    @log.factiva_logger()
    def get_extractions(self, updates=False) -> pd.DataFrame:
        """
        Request a list of historical extractions for the account.

        Parameters
        ----------
        updates : bool
            Indicates whether the retrieved list should include update
            operations (``True``) or not (``False`` - default).

        Returns
        -------
        padas.Dataframe:
            containing the list of historical extractions for the account.

        """
        self.__log.info('get_extractions started')
        endpoint = f'{const.API_HOST}{const.API_EXTRACTIONS_BASEPATH}'

        headers_dict = {'user-key': self.key}

        response = req.api_send_request(method='GET', endpoint_url=endpoint, headers=headers_dict)

        if response.status_code != 200:
            if response.status_code == 403:
                raise ValueError('Factiva API-Key does not exist or inactive.')

            raise RuntimeError(f'Unexpected API Error with message: {response.text}')

        response_data = response.json()

        extraction_df = pd.DataFrame([tools.flatten_dict(extraction) for extraction in response_data['data']])
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

        self.__log.info('get_extractions ended')
        return extraction_df


    def show_extractions(self, updates=False):
        """
        Shows the list of historical extractions for the account. Intended
        to be used in notebooks or manual Python command executions.

        Parameters
        ----------
        updates : bool
            Indicates whether the retrieved list should include update
            operations (``True``) or not (``False`` - default).

        Returns
        -------
        nothing:
            Displays a table with the extraction list.

        Examples
        --------
        Show the historical extractions for the current user:

        .. code-block:: python

            from factiva.analytics import UserKey
            u = UserKey()
            u.show_extractions()

        .. code-block::

                  current_state format extraction_type snapshot_sid update_id
            0    JOB_STATE_DONE   avro       documents   0pjfkz33ra      None
            1    JOB_STATE_DONE   json       documents   0rsfemt846      None
            2    JOB_STATE_DONE   json       documents   1snv7pjx1a      None
            3    JOB_STATE_DONE   json       documents   2toxzrekx1      None
            4    JOB_STATE_DONE    csv       documents   2udvglt9xy      None
            ..              ...    ...             ...          ...       ...
            12   JOB_STATE_DONE   avro       documents   re9xq88syg      None
            13   JOB_STATE_DONE   json       documents   wfbf3eacz8      None
            14   JOB_STATE_DONE   json       documents   ymhsvx20tl      None
            15   JOB_STATE_DONE   json       documents   yonrtw2hbe      None
            16   JOB_STATE_DONE   avro       documents   zpxgqyrqgr      None

        """
        extractions = self.get_extractions(updates=updates)
        print(extractions.loc[:, extractions.columns != 'object_id'])


    @log.factiva_logger()
    def get_streams(self, running=True) -> pd.DataFrame:
        """
        Retrieves the list of streams for the user.

        Parameters
        ----------
        running : bool
            Indicates whether the retrieved list should be restricted
            to only running streams (``True`` - default) or also include
            historical ones (``False``).

        Returns
        -------
        pandas.DataFrame:
            DataFrame with the list of historical extractions

        """
        self.__log.info('get_streams started')
        request_headers = {'user-key': self.key}
        response = req.api_send_request(
            method="GET",
            endpoint_url=f'{const.API_HOST}{const.API_STREAMS_BASEPATH}',
            headers=request_headers
        )
        if response.status_code == 200:
            try:
                def extract_subscriptions(subscription):
                    id_list = []
                    for i in subscription:
                        s_idp = i['id'].split('-')
                        s_id = f"{s_idp[-3]}-{s_idp[-2]}-{s_idp[-1]}"
                        id_list.append(s_id)
                    return id_list

                response_data = response.json()
                stream_df = pd.DataFrame([tools.flatten_dict(extraction) for extraction in response_data['data']])
                stream_df.rename(columns={'id': 'object_id'}, inplace=True)
                ids_df = stream_df['object_id'].str.split('-', expand=True)
                stream_df['stream_id'] = ids_df[4]
                stream_df['stream_type'] = ids_df[2]
                stream_df['subscriptions'] = stream_df['data'].apply(extract_subscriptions)
                stream_df['n_subscriptions'] = stream_df['subscriptions'].str.len()
                stream_df.drop(['self', 'type', 'data'], axis=1, inplace=True)

                if running:
                    stream_df = stream_df.loc[stream_df.job_status == const.API_JOB_RUNNING_STATE]

                self.__log.info('get_streams ended')
                return stream_df
            except Exception as error:
                raise AttributeError('Unexpected Get Streams API Response.') from error
        elif response.status_code == 403:
            raise ValueError('Factiva API-Key does not exist or inactive.')
        else:
            raise RuntimeError('Unexpected Get Streams API Error')


    def show_streams(self, running=True):
        """
        Shows the list of streams for a given user.

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
        nothing:
            Displays a table with the extraction list.

        Examples
        --------
        Show running streams:

        .. code-block:: python

            from factiva.analytics import UserKey
            u = UserKey()
            u.show_streams()

        .. code-block::

              job_status        stream_id  stream_type                subscriptions n_subscriptions
            1 JOB_STATE_RUNNING kmzx8wrbzs      stream [kmzx8wrbzs-filtered-1nJvA5]               1

        """
        account_streams = self.get_streams(running=running)
        print(account_streams.loc[:, account_streams.columns != 'object_id'])


    def is_active(self) -> bool:
        request_headers = {'user-key': self.key}
        response = req.api_send_request(
            method="GET",
            endpoint_url=f'{const.API_HOST}{const.API_SNAPSHOTS_TAXONOMY_BASEPATH}',
            headers=request_headers
        )
        if response.status_code == 200:
            return True
        else:
            return False


    @staticmethod
    def _create_user_key(key=None, stats=False):
        """
        Private method.
        Determine the way to initialize an api key user according to the type
        of parameter provided.

        Parameters
        ----------
        api_user : None, str, UserKey
            Source to create a UserKey instance
        stats : boolean, optional (Default: False)
            Indicates if user data has to be pulled from the server

        Returns
        -------
        UserKey:
            - When None is passed, UserKey instance using credentials from ENV variables
            - When str is passed, UserKey instance using the provided parameter as credentials
            - When UserKey is passed, it returns the same instance

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


    def __repr__(self):
        """Return a string representation of the object."""
        return self.__str__()


    def __str__(self, detailed=True, prefix='  ├─', root_prefix=''):
        pprop = self.__dict__.copy()
        del pprop['_UserKey__log']
        del pprop['key']
        del pprop['cloud_token']
        del pprop['account_name']
        del pprop['account_type']
        del pprop['active_product']
        del pprop['enabled_company_identifiers']
        masked_key = tools.mask_string(self.key)
        
        if self.cloud_token == {}:
            masked_token = '<NotLoaded>'
        else:
            masked_token = tools.mask_string(self.cloud_token['private_key'][58:92], 12)

        ret_val = f"{root_prefix}<'factiva.analytics.{str(self.__class__).split('.')[-1]}"
        ret_val += f'\n{prefix}key: {masked_key}'

        if detailed:
            ret_val += f'\n{prefix}cloud_token: {masked_token}'
            ret_val += f"\n{prefix}account_name: {tools.print_property(self.account_name, '<NotLoaded>')}"
            ret_val += f"\n{prefix}account_type: {tools.print_property(self.account_type, '<NotLoaded')}"
            ret_val += f"\n{prefix}active_product: {tools.print_property(self.active_product, '<NotLoaded>')}\n"
            ret_val += "\n".join((f"{prefix}{item}: {tools.print_property(pprop[item], '<NotLoaded>')}" for item in pprop))
            ret_val += f"\n{prefix}enabled_company_identifiers:"
            if len(self.enabled_company_identifiers) >= 1:
                ci_list = [f"\n{prefix.replace('├', '│')[0:-1]}  ├─[{ci['id']}]: {ci['name']}" for ci in self.enabled_company_identifiers]
                ci_list.sort()
                ci_list[-1] = ci_list[-1].replace('├', '└')
                for ci in ci_list:
                    ret_val += ci
            else:
                ret_val += f"\n{prefix.replace('├', '│')[0:-1]}  └─<NotLoaded>"
            ret_val += f"\n{prefix}remaining_documents: {tools.print_property(self.remaining_documents, '<NotLoaded>')}"
            ret_val += f"\n{prefix[0:-2]}└─remaining_extractions: {tools.print_property(self.remaining_extractions, '<NotLoaded>')}"
        else:
            ret_val += f'{prefix[0:-2]}└─...'
        return ret_val

