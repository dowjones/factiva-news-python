"""
  Classes to interact with the Snapshot Analytics (TimeSeries) endpoint
"""
import time
import pandas as pd
from .base import SnapshotBase, SnapshotBaseQuery, SnapshotBaseJobResponse
from ..common import log, const, tools, req


class SnapshotTimeSeriesJobReponse(SnapshotBaseJobResponse):
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
    data : pandas.DataFrame
        Obtained Time-Series data from job execution
    errors : list[dict]
        Job execution errors returned by the API

    """

    data : pd.DataFrame = None
    errors : list[dict] = None
    # Consider adding calculated values for start/end date and the number
    # of records


    def __init__(self, job_id: str = None) -> None:
        super().__init__(job_id)


    def __repr__(self):
        return super().__repr__()


    def __str__(self, detailed=True, prefix='  ├─', root_prefix=''):
        ret_val = super().__str__(detailed, prefix, root_prefix)
        ret_val += f"{prefix}data: {tools.print_property(self.data)}"
        if self.errors:
            ret_val += f"\n{prefix.replace('├', '└')}errors: [{len(self.errors)}]"
            err_list = [f"\n{prefix[0:-1]}  |-{err['title']}: {err['detail']}" for err in self.errors]
            for err in err_list:
                ret_val += err
        else:
            ret_val += f"\n{prefix.replace('├', '└')}errors: <NoErrors>"
        return ret_val



class SnapshotTimeSeriesQuery(SnapshotBaseQuery):
    """
    Snapshot Query for TimeSeries operations class. Used only in the context of
    SnapshotTimeSeries, but can be transformed to other SnapshotQuery types when 
    those are created using an instance of this class as parameter.

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
    frequency : str
        Time unit used to aggregate values in the time-series calculation
    date_field : str
        Schema date-time field used to calculate the time-series dataset
    group_dimensions : list[str]
        List of fields to break-down aggregates per time period unit
    top : str
        Max entries per group_dimension per time period unit
    """

    frequency : str = None
    date_field : str = None
    group_dimensions : list[str] = None
    top : int = None

    def __init__(self,
                where=None,
                includes: dict = None,
                include_lists: dict = None,
                excludes: dict = None,
                exclude_lists: dict = None,
                frequency: str = const.API_MONTH_PERIOD,
                date_field:str = const.API_PUBLICATION_DATETIME_FIELD,
                group_dimensions: list = [],
                top: int = 10):
        """
        Class constructor
        
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
        include_lists : dict, optional
            Collection of column-List values to be added to the selection crieria
            Python dictionary with the format ``{column_name1: ['listID1', 'listID2, ...],
            column_name2: ['listID1', 'listID2', ...]}``.
        exclude_lists : dict, optional
            Collection of bulk values to be removed from the selection criteria.
            Python dictionary with the format ``{column_name1: ['ListID1', 'listID2, ...],
            column_name2: ['listID1', 'listID2', ...]}``.
        frequency : str, optional
            Date part to be used to group subtotals in the time-series dataset. Allowed values
            are ``DAY``, ``MONTH`` (default) and ``YEAR``.
        date_field : str, optional
            Timestamp column that will be used to calculate the time-series dataset. It can be
            any of the three values: ``publication_datetime`` (default), ``modification_datetime``,
            and ``ingestion_datetime``.
        group_dimensions : list[str], optional
            List of fields that will be used to break-down subtotals for each period. This list can
            have a maximum of 4 elements. Allowed values are ``['source_code', 'subject_codes', 
            'region_codes', 'industry_codes', 'company_codes', 'person_codes', 'company_codes_about', 
            'company_codes_relevance', 'company_codes_cusip', 'company_codes_isin', 
            'company_codes_sedol', 'company_codes_ticker', 'company_codes_about_cusip', 
            'company_codes_about_isin', 'company_codes_about_sedol', 'company_codes_about_ticker', 
            'company_codes_relevance_cusip', 'company_codes_relevance_isin', 
            'company_codes_relevance_sedol', 'company_codes_relevance_ticker']``
        top : int, optional
            Limits the dataset to return only the top X values for each dimension passed in the
            ``group_dimensions`` parameter. Default 10.
        """
        super().__init__(where, includes, include_lists, excludes, exclude_lists)

        tools.validate_type(frequency, str, "Unexpected value for frequency")
        frequency = frequency.upper().strip()
        tools.validate_field_options(frequency, const.API_DATETIME_PERIODS)
        self.frequency = frequency

        tools.validate_type(date_field, str, "Unexpected value for date_field")
        date_field = date_field.lower().strip()
        tools.validate_field_options(date_field, const.API_DATETIME_FIELDS)
        self.date_field = date_field

        if isinstance(group_dimensions, list):
            self.group_dimensions = group_dimensions
        # TODO: Validate values in the list group_dimensions are valid form the
        #       list of all possible columns that can be used for this purpose

        tools.validate_type(top, int, "Unexpected value for top")
        if top >= 0:
            self.top = top
        else:
            raise ValueError('Top value is not valid')


    def get_payload(self) -> dict:
        """
        Create the basic request payload to be used within Snapshots Explain API
        request.

        Returns
        -------
        dict
            Dictionary containing non-null query attributes.

        """
        payload = super().get_payload()

        self.frequency = self.frequency.upper().strip()
        tools.validate_field_options(self.frequency, const.API_DATETIME_PERIODS)
        payload["query"].update({"frequency": self.frequency})

        self.date_field = self.date_field.lower().strip()
        tools.validate_field_options(self.date_field, const.API_DATETIME_FIELDS)
        payload["query"].update({"date_field": self.date_field})

        if(self.group_dimensions):
            if(len(self.group_dimensions)<=4):
                for option in self.group_dimensions:
                    tools.validate_field_options(option, const.API_GROUP_DIMENSIONS_FIELDS)
            else:
                raise ValueError("The maximiun group_dimensions size is 4")
        else:
            self.group_dimensions = []
        
        payload["query"].update(
            {"group_dimensions": self.group_dimensions})

        payload["query"].update({"top": self.top})

        return payload


    def __repr__(self):
        return super().__repr__()


    def __str__(self, detailed=True, prefix='  ├─', root_prefix=''):
        ret_val = super().__str__(detailed, prefix, root_prefix)
        ret_val = ret_val.replace('└─...', '├─...')
        ret_val += f"\n{prefix}frequency: {tools.print_property(self.frequency)}"
        ret_val += f"\n{prefix}date_field: {tools.print_property(self.date_field)}"
        ret_val += f"\n{prefix}group_dimensions: {tools.print_property(self.group_dimensions)}"
        ret_val += f"\n{prefix[0:-2]}└─top: {tools.print_property(self.top)}"
        return ret_val



class SnapshotTimeSeries(SnapshotBase):
    """
    Main class to interact with the Time Series service from Factiva Analytics.

    Attributes
    ----------
    user_key : UserKey
        User representation for service authentication
    query : SnapshotExtractionQuery
        Query object tailored for Extraction operations
    job_response : SnapshotExtractionJobReponse
        Object containing job status and execution details

    """

    query : SnapshotTimeSeriesQuery = None
    job_response : SnapshotTimeSeriesJobReponse = None

    def __init__(
        self,
        user_key=None,
        query=None,
        job_id=None
    ):
        super().__init__(user_key=user_key, query=query, job_id=job_id)
        self.__log = log.get_factiva_logger()
        self.__JOB_BASE_URL = f'{const.API_HOST}{const.API_ANALYTICS_BASEPATH}'

        if job_id:
            self.__log.info(f'Creating SnapshotTimeSeries instance with JobID {job_id}')
            self.job_response = SnapshotTimeSeriesJobReponse(job_id)
            self.get_job_response()
        elif query:
            if isinstance(query, SnapshotTimeSeriesQuery):
                self.query = query
            elif isinstance(query, str):
                self.query = SnapshotTimeSeriesQuery(query)
            else:
                raise ValueError('Unexpected query type')
        else:
            self.query = SnapshotTimeSeriesQuery()
        self.__log.info('SnapshotExtraction created OK')



    @log.factiva_logger
    def submit_job(self):
        """
        Performs a POST request to the API using the assigned query to start
        a TimeSeries job.

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
                'Content-Type': 'application/json',
                'X-API-VERSION': '2.0'
            }
        
        submit_url = f'{self.__JOB_BASE_URL}'
        submit_payload = self.query.get_payload()

        response = req.api_send_request(method='POST', endpoint_url=submit_url, headers=headers_dict, payload=submit_payload)

        if response.status_code == 201:
            response_data = response.json()
            self.job_response = SnapshotTimeSeriesJobReponse(response_data["data"]["id"])
            self.job_response.job_state = response_data['data']['attributes']['current_state']
            self.job_response.job_link = response_data['links']['self']
            if 'errors' in response_data.keys():
                self.job_response.errors = response_data['errors']
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

        self.__log.info(f'Requesting Analytics Job info for ID {self.job_response.job_id}')
        getinfo_url = f'{self.__JOB_BASE_URL}/{self.job_response.job_id}'
        response = req.api_send_request(method='GET', endpoint_url=getinfo_url, headers=headers_dict)

        if response.status_code == 200:
            self.__log.info(f'Job ID {self.job_response.job_id} info retrieved successfully')
            response_data = response.json()
            self.job_response.job_state = response_data['data']['attributes']['current_state']
            self.job_response.job_link = response_data['links']['self']
            if self.job_response.job_state == const.API_JOB_DONE_STATE:
                self.job_response.data = pd.DataFrame(response_data['data']['attributes']['results'])
            if 'errors' in response_data.keys():
                self.job_response.errors = response_data['errors']
        elif response.status_code == 404:
            raise RuntimeError('Job ID does not exist.')
        elif response.status_code == 400:
            detail = response_data['errors'][0]['detail']
            raise ValueError(f'Bad Request: {detail}')
        else:
            raise RuntimeError(f'API request returned an unexpected HTTP status, with content [{response.text}]')
        self.__log.info('get_job_response End')
        return True


    def process_job(self):  # TODO: Implement Retries if a 500 or timeout is returned during the active wait
        """
        Submit a new job to be processed, wait until the job is completed
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
        ret_val = ret_val.replace('├─job_response', '└─job_response')
        return ret_val
