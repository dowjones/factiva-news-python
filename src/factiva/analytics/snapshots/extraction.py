"""
  Module containing all clases that interact with the Snapshot Extraction service
"""

from .base import SnapshotBase, SnapshotBaseQuery, SnapshotBaseJobResponse
from ..common import log, const, req, tools
from ..auth import UserKey


class SnapshotExtraction(SnapshotBase):
    """
    Main class to interact with the Extractions service from Factiva Analytics.

    Attributes
    ----------
    user_key : UserKey
        User representation for service authentication
    query : SnapshotExtractionQuery
        Query object tailored for Extraction operations
    job_response : SnapshotExtractionJobReponse
        Object containing job status and execution details

    Methods
    -------
    submit_job()
        Sends a job request based on the value of ``user_key`` and
        ``query``. Creates an initial instance of ``job_response``.
    get_job_response()
        Gets the latest job status from the server and updates the
        ``job_response`` attribute properties.
    download_files(path='/my/local/path')
        Download all extraction files to the specified location. If not provided
        files are downloaded to the default location (Extraction short ID).
    process_job()
        Runs the full sequence of ``submit_job()``, ``get_job_response()`` and
        ``download_files()``. It handles all intermediate stages and only ends
        when the service reports that the job is done. It's an automation of
        full job execution and the preferred method to be used in most cases.

    """

    @log.factiva_logger
    def __init__(self, user_key=None, query=None, job_id=None):
        """
        Creates a SnapshotExtraction instance.

        Parameters
        ----------
        user_key : str or UserKey
            String containing the 32-character long APi Key. If not provided, the
            constructor will try to obtain its value from the FACTIVA_USERKEY
            environment variable.
        query : str or SnapshotExtractionQuery, optional
            Query used to run any of the Snapshot-related operations. If a str is
            provided, a simple query with a `where` clause is created. This
            parameter is not compatible with job_id.
        job_id: str, optional
            String containing the 10-character long Snapshot ID. This parameter is
            not compatible with query.
        """

        super().__init__(user_key, query, job_id)
        self.__log = log.get_factiva_logger()
        self.__JOB_BASE_URL = f'{const.API_HOST}{const.API_SNAPSHOTS_BASEPATH}'

        if job_id:
            self.__log.info(f'Creating SnapshotTimeSeries instance with JobID {job_id}')
            self.job_response = SnapshotExtractionJobReponse(job_id, self.user_key)
            self.get_job_response()
        elif query:
            if isinstance(query, SnapshotExtractionQuery):
                self.query = query
            else:
                self.query = SnapshotExtractionQuery(query)

# TODO: Next --> Test this!
    @log.factiva_logger
    def submit_job(self):
        """
        Performs a POST request to the API using the assigned values in ``user_key``
        and ``query``.

        If the job is initiated succesfully, the initial status is stored in the ``job_response``
        object. Otherwise any HTTP error will raise an exception.

        Returns
        -------
        Boolean : True if the submission was successful. An Exception otherwise.

        Raises
        ------
        ValueError
            When the ``query`` is empty or invalid.
        """

        self.__log.info('submit_job Start')
        if not self.query:
            raise ValueError('A query is needed to submit an Explain Job')

        headers_dict = {
                'user-key': self.user_key.key,
                'Content-Type': 'application/json'
            }
        
        submit_url = f'{self.__JOB_BASE_URL}'
        submit_payload = self.query.get_payload()

        response = req.api_send_request(method='POST', endpoint_url=submit_url, headers=headers_dict, payload=submit_payload)

        if response.status_code == 201:
            response_data = response.json()
            self.job_response = SnapshotExtractionJobReponse(response_data["data"]["id"])
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
        Performs a request to the API to obtain an updated status of a job
        execution.

        If the job has been completed, result details are assigned to the
        ``job_response`` object.

        Returns
        -------
        Boolean : True if the get request was successful. An Exception otherwise.

        Raises
        ------
        ValueError
            If the Job ID doesn't exist for the user key, or the get request
            is invalid.
        """

        self.__log.info('get_job_response Start')

        if (not self.job_response):
            raise RuntimeError('Job has not yet been submitted or Job ID was not set')

        headers_dict = {
            'user-key': self.user_key.key,
            'Content-Type': 'application/json'
        }

        self.__log.info(f'Requesting Explain Job info for ID {self.job_response.job_id}')
        getinfo_url = f'{self.__JOB_BASE_URL}/{self.job_response.job_id}'
        response = req.api_send_request(method='GET', endpoint_url=getinfo_url, headers=headers_dict)

        if response.status_code == 200:
            self.__log.info(f'Job ID {self.job_response.job_id} info retrieved successfully')
            response_data = response.json()
            self.job_response.job_state = response_data['data']['attributes']['current_state']
            self.job_response.job_link = response_data['links']['self']
            if self.job_response.job_state == const.API_JOB_DONE_STATE:
                self.job_response.files = response_data['data']['attributes']['files']
            if 'errors' in response_data.keys():
                self.job_response.errors = response_data['errors']
        elif response.status_code == 404:
            raise ValueError('Job ID does not exist for the provided user key.')
        elif response.status_code == 400:
            detail = response_data['errors'][0]['detail']
            raise ValueError(f'Bad Request: {detail}')
        else:
            raise RuntimeError(f'API request returned an unexpected HTTP status, with content [{response.text}]')
        self.__log.info('get_job_response End')
        return True


    def __str__(self, detailed=True, prefix='  ├─', root_prefix=''):
        ret_val = super().__str__(detailed, prefix, root_prefix)
        ret_val = ret_val.replace('├─job_response', '└─job_response')
        return ret_val


class SnapshotExtractionQuery(SnapshotBaseQuery):
    """
    Query class used specifically for Snapshot Extraction operations.

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

    Methods
    -------
    get_payload()
        Returns a dict with the required format for a Snapshot
        Extraction API request.

    """

    file_format = None
    limit = None

    def __init__(self,
                where=None,
                includes: dict = None,
                include_lists: dict = None,
                excludes: dict = None,
                exclude_lists: dict = None,
                file_format: str = const.API_AVRO_FORMAT,
                limit: int = 0):
        """
        Creates a new SnapshotExtractionQuery instance.

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
        file_format : str, optional
            Preferred format for the generated files in the extraction operation.
            Possible values are 'AVRO' (default), 'JSON' and 'CSV'.
        limit : int, optional
            Max article limit for the extraction operation. Recommended for test only.
            Full extractions and updates will be affected by this parameter.
        """

        super().__init__(where, includes, include_lists, excludes, exclude_lists)
        tools.validate_type(limit, int, "Unexpected value for limit")
        if limit >= 0:
            self.limit = limit
        else:
            raise ValueError("Limit value is not valid or not positive")

        tools.validate_type(file_format, str, "Unexpected value for file_format")
        file_format = file_format.lower().strip()
        tools.validate_field_options(file_format, const.API_EXTRACTION_FILE_FORMATS)
        self.file_format = file_format


    def get_payload(self) -> dict:
        query_dict = super().get_payload()

        if self.limit > 0:
            query_dict["query"].update({"limit": self.limit})

        query_dict["query"].update({"format": self.file_format})

        return query_dict


    def __repr__(self):
        return super().__repr__()


    def __str__(self, detailed=True, prefix='  ├─', root_prefix=''):
        ret_val = super().__str__(detailed, prefix, root_prefix)
        ret_val = ret_val.replace('└─...', '├─...')
        ret_val += f"\n{prefix}file_format: {tools.print_property(self.file_format)}"
        ret_val += f"\n{prefix[0:-2]}└─limit: {tools.print_property(self.limit)}"
        return ret_val


class SnapshotExtractionJobReponse(SnapshotBaseJobResponse):
    """
    Snapshot Explain Job Response class. Essentially contains the volume
    of estimate documents.
    """

    errors = None
    files = None
    short_id = None


    def __init__(self, job_id: str = None, user_key: UserKey = None) -> None:
        if len(job_id) == 64:
            self.job_id = job_id
            self.short_id = job_id.split('-')[-1]
        elif (len(job_id) == 10) and (user_key):
            self.job_id = f'dj-synhub-extraction-{user_key.key}-{job_id}'
            self.short_id = job_id
        else:
            raise ValueError('Unexpected value for job_id. If a short_id is provided, a user_key instance is needed.')



    def __repr__(self):
        return super().__repr__()


    def __str__(self, detailed=True, prefix='  ├─', root_prefix=''):
        ret_val = super().__str__(detailed, prefix, root_prefix)
        ret_val += f"{prefix}files: {tools.print_property(self.files)}"
        if self.errors:
            ret_val += f"\n{prefix.replace('├', '└')}errors: [{len(self.errors)}]"
            err_list = [f"\n{prefix[0:-1]}  |-{err['title']}: {err['detail']}" for err in self.errors]
            for err in err_list:
                ret_val += err
        else:
            ret_val += f"\n{prefix.replace('├', '└')}errors: <NoErrors>"
        return ret_val


