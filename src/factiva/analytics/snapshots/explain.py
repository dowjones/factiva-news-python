"""
  Module with Classes to interact with the Snapshot Explain endpoint
"""
from .base import SnapshotBase, SnapshotBaseQuery, SnapshotBaseJobResponse
from ..common import log, const, req, tools
import time
import pandas as pd


class SnapshotExplainSamplesResponse():
    """
    Snapshot Explain Samples Response class. Essentially contains the list of
    metadata samples randomly selected from a previously sent criteria linked
    to the Job ID.

    Attributes
    ----------
    num_samples : int
        Number with the returned number of samples
    data : pandas.DataFrame
        Pandas DataFrame with the samples dataset

    """
    num_samples : int = None
    data : pd.DataFrame = None


    def __init__(self, samples_list:list) -> None:
        if not isinstance(samples_list, list):
            raise ValueError('Unexpected samples_list parameter.')

        self.data = pd.DataFrame(samples_list)
        self.num_samples = len(samples_list)


    def __repr__(self):
        return super().__repr__()
        

    def __str__(self, detailed=True, prefix='  ├─', root_prefix='') -> None:
        ret_val = f"{root_prefix}<factiva.analytics.{str(self.__class__).split('.')[-1]}"
        ret_val += f"\n{prefix}num_samples: {tools.print_property(self.num_samples)}"
        ret_val += f"\n{prefix[0:-2]}└─data: {tools.print_property(self.data)}"
        return ret_val

    # Returns the following columns.
    # TODO: Create methods to split multi-value fields.
    # ['an', 'company_codes', 'company_codes_about', 'company_codes_occur',
    #    'industry_codes', 'ingestion_datetime', 'modification_datetime',
    #    'publication_datetime', 'publisher_name', 'region_codes',
    #    'region_of_origin', 'source_code', 'source_name', 'subject_codes',
    #    'title', 'word_count']



class SnapshotExplainJobResponse(SnapshotBaseJobResponse):
    """
    Snapshot Explain Job Response class. Essentially contains the volume
    of estimate documents.

    Attributes
    ----------
    job_id : str
        Explain Job ID with a format like ``abcd1234-ab12-ab12-ab12-abcdef123456``.
    job_link : str
        Unique URL referring to the job instance
    job_state : str
        Latest known job status. Value is self-explanatory.
    volume_estimate : int
        Number representing the total volume of matching documents
    errors : list[dict]
        Job execution errors returned by the API

    """
    volume_estimate : int = None
    errors : list[dict] = None


    def __repr__(self):
        return super().__repr__()


    def __str__(self, detailed=True, prefix='  ├─', root_prefix=''):
        ret_val = super().__str__(detailed, prefix, root_prefix)
        if isinstance(self.volume_estimate, int):
            ret_val += f"{prefix}volume_estimate: {self.volume_estimate:,d}"
        else:
            ret_val += f"{prefix}volume_estimate: <NotCalculated>"
        if self.errors:
            ret_val += f"\n{prefix.replace('├', '└')}errors: [{len(self.errors)}]"
            err_list = [f"\n{prefix[0:-1]}  |-{err['title']}: {err['detail']}" for err in self.errors]
            for err in err_list:
                ret_val += err
        else:
            ret_val += f"\n{prefix.replace('├', '└')}errors: <NoErrors>"
        return ret_val



class SnapshotExplainQuery(SnapshotBaseQuery):
    """
    Query class used specifically for Snapshot Explain operations.

    Attributes
    ----------
    where : str
        User representation for service authentication
    includes : dict
        Dictionary with a fixed list of codes to include
    includes_list : dict
        Dictionary with references to Lists for inclusion
    excludes : dict
        Dictionary with a fixed list of codes to exclude
    excludes_list : dict
        Dictionary with references to Lists for inclusion
        
    """

    def __init__(self,
                where=None,
                includes: dict = None,
                include_lists: dict = None,
                excludes: dict = None,
                exclude_lists: dict = None):
        """
        Creates a new SnapshotExplainQuery instance.

        Parameters
        ----------
        where : str
            String containing the query WHERE statement.
        includes : dict, optional
            Collection of bulk values to be added to the selection criteria.
            Python dictionary with the format ``{column_name1: ['value1', 'value2, ...],
            column_name2: ['value1', 'value2', ...]}``.
        excludes : dict, optional
            Collection of bulk values to be removed from the selection criteria.
            Python dictionary with the format ``{column_name1: ['value1', 'value2, ...],
            column_name2: ['value1', 'value2', ...]}``.
        include_list : dict, optional
            Collection of column-List values to be added to the selection crieria
            Python dictionary with the format ``{column_name1: ['listID1', 'listID2, ...],
            column_name2: ['listID1', 'listID2', ...]}``.
        exclude_list : dict, optional
            Collection of bulk values to be removed from the selection criteria.
            Python dictionary with the format ``{column_name1: ['ListID1', 'listID2, ...],
            column_name2: ['listID1', 'listID2', ...]}``.
        """
        super().__init__(where, includes, include_lists, excludes, exclude_lists)


    def get_payload(self) -> dict:
        """
        Create the basic request payload to be used within Snapshots Explain API
        request.

        Returns
        -------
        dict
            Dictionary containing non-null query attributes.

        """
        return super().get_payload()


    def __repr__(self):
        return super().__repr__()


    def __str__(self, detailed=True, prefix='  ├─', root_prefix=''):
        return super().__str__(detailed, prefix, root_prefix)



class SnapshotExplain(SnapshotBase): # TODO: Refactor when repeating code across Explain, TimeSeries and Extraction
    """
    Main class to interact with the Explain service from Factiva Analytics.

    Attributes
    ----------
    user_key : UserKey
        User representation for service authentication
    query : SnapshotExtractionQuery
        Query object tailored for Extraction operations
    job_response : SnapshotExtractionJobReponse
        Object containing job status and execution details
    samples : SnapshotExplainSamplesResponse

    """

    __SAMPLES_BASEURL = f'{const.API_HOST}{const.API_EXTRACTIONS_BASEPATH}{const.API_EXTRACTIONS_SAMPLES_SUFFIX}'
    __MAX_SAMPLES = 100
    samples : SnapshotExplainSamplesResponse = None
    job_response : SnapshotExplainJobResponse = None
    query : SnapshotExplainQuery = None

    def __init__(
        self,
        user_key=None,
        query=None,
        job_id=None
    ):
        """
        SnapshotExplain constructor.

        Parameters
        ----------
        user_key : str or UserKey
            String containing the 32-character long APi Key. If not provided, the
            constructor will try to obtain its value from the ``FACTIVA_USERKEY``
            environment variable.
        query : str or SnapshotExplainQuery, optional
            If no value is provided the init operation attempt to get a value from the ENV
            variable ``FACTIVA_WHERE``. If a str is
            provided, a simple query with a `where` clause is created. If other
            query fields are required, either provide the SnapshotExplainQuery object at
            creation, or set the appropriate object values after creation. Not 
            compatible with the parameter ``job_id``.
        job_id : str, optional
            Explain Job ID with a format like ``abcd1234-ab12-ab12-ab12-abcdef123456``.
            Not compatible if the parameter ``query``.
        """
        super().__init__(user_key=user_key, query=query, job_id=job_id)
        self.__log = log.get_factiva_logger()
        self.__JOB_BASE_URL = f'{const.API_HOST}{const.API_SNAPSHOTS_BASEPATH}'

        if job_id:
            self.__log.info(f'Creating SnapshotExplain instance with JobID {job_id}')
            self.job_response = SnapshotExplainJobResponse(job_id)
            self.get_job_response()
        elif query:
            if isinstance(query, SnapshotExplainQuery):
                self.query = query
            elif isinstance(query, str):
                self.query = SnapshotExplainQuery(query)
            else:
                raise ValueError('Unexpected query type')
        else:
            self.query = SnapshotExplainQuery()
        self.__log.info('SnapshotExplain created OK')

    @log.factiva_logger
    def submit_job(self):
        """
        Performs a POST request to the API using the assigned query to start
        an Explain job.

        If the job is initiated succesfully, results are assigned to the ``job_response``
        object. Otherwise any HTTP error will raise an exception.

        Returns
        -------
        bool
            True if the submission was successful. An Exception otherwise.

        """
        self.__log.info('submit_job Start')
        if not self.query:
            raise ValueError('A query is needed to submit an Explain Job')

        headers_dict = {
                'user-key': self.user_key.key,
                'Content-Type': 'application/json'
            }
        
        submit_url = f'{self.__JOB_BASE_URL}{const.API_EXPLAIN_SUFFIX}'
        submit_payload = self.query.get_payload()

        response = req.api_send_request(method='POST', endpoint_url=submit_url, headers=headers_dict, payload=submit_payload)

        if response.status_code == 201:
            response_data = response.json()
            self.job_response = SnapshotExplainJobResponse(response_data["data"]["id"])
            self.job_response.job_state = response_data['data']['attributes']['current_state']
            self.job_response.job_link = response_data['links']['self']
        elif response.status_code == 400:
            raise ValueError(f'Invalid Query [{response.text}]')
        else:
            raise RuntimeError(f'API request returned an unexpected HTTP status, with content [{response.text}]')
        self.__log.info('submit_job End')
        return True


    @log.factiva_logger
    def get_job_response(self) -> bool:
        """
        Performs a request to the API using the job ID to get its status.

        If the job has been completed, results are assigned to the ``job_response`` object.

        Returns
        -------
        bool
            True if the get request was successful. An Exception otherwise.

        """
        self.__log.info('get_job_response Start')

        if (not self.job_response):
            raise RuntimeError('Job has not yet been submitted or Job ID was not set')

        headers_dict = {
            'user-key': self.user_key.key,
            'Content-Type': 'application/json'
        }

        self.__log.info(f'Requesting Explain Job info for ID {self.job_response.job_id}')
        getinfo_url = f'{self.__JOB_BASE_URL}/{self.job_response.job_id}{const.API_EXPLAIN_SUFFIX}'
        response = req.api_send_request(method='GET', endpoint_url=getinfo_url, headers=headers_dict)

        if response.status_code == 200:
            self.__log.info(f'Job ID {self.job_response.job_id} info retrieved successfully')
            response_data = response.json()
            self.job_response.job_state = response_data['data']['attributes']['current_state']
            self.job_response.job_link = response_data['links']['self']
            if self.job_response.job_state == const.API_JOB_DONE_STATE:
                self.job_response.volume_estimate = response_data['data']['attributes']['counts']
            if 'errors' in response_data.keys():
                self.job_response.errors = response_data['errors']
            # elif self.job_response.job_state == const.API_JOB_FAILED_STATE:
                # errors = response_data['errors']
                # raise RuntimeError(f"Job Failed with reason: {[e['title'] + e['detail'] for e in errors]}")
        elif response.status_code == 404:
            raise RuntimeError('Job ID does not exist.')
        elif response.status_code == 400:
            detail = response_data['errors'][0]['detail']
            raise ValueError(f'Bad Request: {detail}')
        else:
            raise RuntimeError(f'API request returned an unexpected HTTP status, with content [{response.text}]')
        self.__log.info('get_job_response End')
        return True


    def get_samples(self, num_samples:int=__MAX_SAMPLES):
        """
        Performs a request to the API using the job ID to get its status.

        If the job has been completed, results are assigned to the ``job_response`` object.

        Returns
        -------
        bool
            True if the get request was successful. An Exception otherwise.

        """
        self.__log.info('get_samples Start')
        # super().get_job_response_base()
        if (not self.job_response):
            raise RuntimeError('Job has not yet been submitted or Job ID was not set')
        
        if (num_samples < 1) or (num_samples > self.__MAX_SAMPLES):
            raise ValueError(f'The n_samples value must be an integer between 1 and {self.__MAX_SAMPLES}')

        headers_dict = {
            'user-key': self.user_key.key,
            'Content-Type': 'application/json'
        }

        qs_parameters = {
            'num_samples': num_samples
        }

        self.__log.info(f'Requesting Samples for JobID {self.job_response.job_id}')
        samples_url = f'{self.__SAMPLES_BASEURL}/{self.job_response.job_id}'
        response = req.api_send_request(method='GET',
                                        endpoint_url=samples_url,
                                        headers=headers_dict,
                                        qs_params=qs_parameters)

        if response.status_code == 200:
            self.__log.info(f'Samples for Job ID {self.job_response.job_id} retrieved successfully')
            response_data = response.json()
            self.samples = SnapshotExplainSamplesResponse(response_data['data']['attributes']['sample'])
        elif response.status_code == 404:
            raise RuntimeError('Job ID does not exist.')
        elif response.status_code == 400:
            detail = response_data['errors'][0]['detail']
            raise ValueError(f'Bad Request: {detail}')
        else:
            raise RuntimeError(f'API request returned an unexpected HTTP status, with content [{response.text}]')
        self.__log.info('get_samples End')
        return True


    def process_job(self):  # TODO: Implement Retries if a 500 or timeout is returned during the active wait
        """
        Submits a new job to be processed, wait until the job is completed
        and then retrieves the job results.

        Returns
        -------
        bool
            True if the explain processing was successful. An Exception
            otherwise.

        """
        self.__log.info('process_job Start')
        self.submit_job()
        self.get_job_response()

        while not (self.job_response.job_state in
                    [const.API_JOB_DONE_STATE,
                     const.API_JOB_FAILED_STATE]
                  ):
            if self.job_response.job_state not in const.API_JOB_EXPECTED_STATES:
                raise RuntimeError('Unexpected job state')
            # if self.job_response.job_state == const.API_JOB_FAILED_STATE:
            #     raise Exception('Job failed')
            time.sleep(const.API_JOB_ACTIVE_WAIT_SPACING)
            self.get_job_response()
        
        self.__log.info('process_job End')
        return True


    def __repr__(self):
        return super().__repr__()


    def __str__(self, detailed=True, prefix='  ├─', root_prefix=''):
        ret_val = super().__str__(detailed, prefix, root_prefix)
        if self.samples:
            ret_val += f"\n{prefix[0:-2]}└─samples: {self.samples.__str__(detailed=False, prefix='     ├─')}"
        else:
            ret_val += f"\n{prefix[0:-2]}└─samples: <NotRetrieved>"
        return ret_val

