"""
  Module containing all clases that interact with the Snapshot Extraction service
"""

import time, os
from .base import SnapshotBase, SnapshotBaseQuery, SnapshotBaseJobResponse
from ..common import log, const, req, tools
from ..auth import UserKey
from pathlib import Path


class SnapshotExtractionJobReponse(SnapshotBaseJobResponse):
    """
    Snapshot Explain Job Response class. Essentially contains the volume
    of estimate documents.

    Attributes
    ----------
    job_id : str
        Job ID returned by Factiva Analyitcs at submission time
    short_id : str
        Unique portion from the attribute ``job_id``
    job_link : str
        Job unique URI
    job_state : str
        Job status value
    errors : list
        If not empty, a list of errors during the job execution
    files : list
        If the job is successful, this shows the list of files that can
        be downloaded with the selected content.

    """

    errors : list[dict] = None
    files : list[str] = None
    short_id : str = None


    def __init__(self, job_id: str = None, user_key: UserKey = None) -> None:
        if len(job_id) == 64:
            self.job_id = job_id
            self.short_id = job_id.split('-')[-1]
        elif (len(job_id) == 10) and (user_key):
            self.job_id = f'dj-synhub-extraction-{user_key.key.lower()}-{job_id}'
            self.short_id = job_id
        else:
            raise ValueError('Unexpected value for job_id. If a short_id is provided, a user_key instance is needed.')



    def __repr__(self):
        return super().__repr__()


    def __str__(self, detailed=True, prefix='  ├─', root_prefix=''):
        ret_val = super().__str__(detailed, prefix, root_prefix)
        ret_val += f"{prefix.replace('│  ├', '   ├')}short_id: {tools.print_property(self.short_id)}"
        ret_val += f"\n{prefix}files: {tools.print_property(self.files)}"
        if self.errors:
            ret_val += f"\n{prefix.replace('│  ├', '   └')}errors: [{len(self.errors)}]"
            err_list = [f"\n{prefix[0:-1]}  |-{err['title']}: {err['detail']}" for err in self.errors]
            for err in err_list:
                ret_val += err
        else:
            ret_val += f"\n{prefix.replace('│  ├', '   └')}errors: <NoErrors>"
        ret_val = ret_val.replace('  │  ├─job_', '     ├─job_')
        ret_val = ret_val.replace('  │  ├─files', '     ├─files')
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
    file_format : str
        Chosen file fomat for extraction files
    limit : int
        Max number of articles to extract

    """

    file_format: str
    limit: int

    def __init__(self,
                where:str = None,
                includes: dict = None,
                include_lists: dict = None,
                excludes: dict = None,
                exclude_lists: dict = None,
                file_format: str = const.API_AVRO_FORMAT,
                limit: int = 0) -> None:
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
        """
        Create the basic request payload to be used within a Snapshots Extraction API
        request.

        Returns
        -------
        dict
            Dictionary containing non-null query attributes.

        """
        query_dict = super().get_payload()

        if self.limit > 0:
            query_dict["query"].update({"limit": self.limit})

        query_dict["query"].update({"format": self.file_format})

        return query_dict


    def __repr__(self):
        return super().__repr__()


    def __str__(self, detailed=True, prefix='  ├─', root_prefix=''):
        ret_val = super().__str__(detailed, prefix, root_prefix)
        ret_val = ret_val.replace('└─', '├─')
        ret_val += f"\n{prefix}file_format: {tools.print_property(self.file_format)}"
        ret_val += f"\n{prefix[0:-2]}└─limit: {tools.print_property(self.limit)}"
        return ret_val



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

    """

    query: SnapshotExtractionQuery = None
    job_response: SnapshotExtractionJobReponse = None

    @log.factiva_logger
    def __init__(self, job_id=None, query=None, user_key=None) -> None:
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

        self.__log.info('creating SnapshotExtraction...')

        if job_id:
            self.__log.info(f'Creating SnapshotExtraction instance with JobID {job_id}')
            self.job_response = SnapshotExtractionJobReponse(job_id, self.user_key)
            self.get_job_response()
        elif query:
            if isinstance(query, SnapshotExtractionQuery):
                self.query = query
            elif isinstance(query, str):
                self.query = SnapshotExtractionQuery(query)
            else:
                raise ValueError('Unexpected query type')
        else:
            self.query = SnapshotExtractionQuery()
        self.__log.info('SnapshotExtraction created OK')


    @log.factiva_logger
    def submit_job(self):
        """
        Performs a POST request to the API using the assigned values in ``user_key``
        and ``query``.

        If the job is initiated succesfully, the initial status is stored in the ``job_response``
        object. Otherwise any HTTP error will raise an exception.

        Returns
        -------
        bool
            True if the submission was successful. An Exception otherwise.

        Raises
        ------
        ValueError
            When the ``query`` is empty or invalid.
        """

        self.__log.info('submit_job submitting...')
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
        
        self.__log.info('submit_job OK')
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
        bool
            True if the get request was successful. An Exception otherwise.

        Raises
        ------
        ValueError
            If the Job ID doesn't exist for the user key, or the get request
            is invalid.
        """

        self.__log.info(f'get_job_response for ID {self.job_response.short_id}')

        if (not self.job_response):
            raise RuntimeError('Job has not yet been submitted or Job ID was not set')

        headers_dict = {
            'user-key': self.user_key.key,
            'Content-Type': 'application/json'
        }

        getinfo_url = f'{self.__JOB_BASE_URL}/{self.job_response.job_id}'
        response = req.api_send_request(method='GET', endpoint_url=getinfo_url, headers=headers_dict)

        if response.status_code == 200:
            self.__log.info(f'Job ID {self.job_response.job_id} info retrieved successfully')
            response_data = response.json()
            self.job_response.job_state = response_data['data']['attributes']['current_state']
            self.job_response.job_link = response_data['links']['self']
            if self.job_response.job_state == const.API_JOB_DONE_STATE:
                files_obj_list = response_data['data']['attributes']['files']
                self.job_response.files = [obj['uri'] for obj in files_obj_list]
            if 'errors' in response_data.keys():
                self.job_response.errors = response_data['errors']
        elif response.status_code == 404:
            raise ValueError('Job ID does not exist for the provided user key.')
        elif response.status_code == 400:
            detail = response_data['errors'][0]['detail']
            raise ValueError(f'Bad Request: {detail}')
        else:
            raise RuntimeError(f'API request returned an unexpected HTTP status, with content [{response.text}]')
        self.__log.info('get_job_response OK')
        return True

    
    def __download_extraction_file(self, file_uri: str, download_path: str) -> bool:
        """Download a file from a job, using the file URL and stores them in download_path.

        Parameters
        ----------
        endpoint_url: str
            String containing the URL to download the file from
        download_path: str
            String containing the path where to store the downloaded file

        Returns
        -------
        bool
            True if the download was successful. An Exception otherwise.

        Raises
        ------
        - RuntimeException when the response from the API is not successful

        """
        headers_dict = {
                'user-key': self.user_key.key
            }
        response = req.api_send_request(method='GET', endpoint_url=file_uri, headers=headers_dict)

        if response.status_code == 200:
            with open(download_path, 'wb') as download_file_path:
                download_file_path.write(response.content)
        else:
            raise RuntimeError(f'API request returned an unexpected HTTP status, with content [{response.text}]')
        return True


    @log.factiva_logger
    def download_files(self, path=None):
        """
        Download all files from a job and stores them in the given path.

        If the ``path`` parameter is empty, files are stored in a folder
        with the name of the job short id.

        Parameters
        ----------
        path: str, Optional
            String containing the path where to store the downloaded files.
            If not provided, the files are stored in a folder named after
            the job short_id. If such folder does not exists, it is created 
            in the current working directory.

        Returns
        -------
        bool
            True if files were correctly downloaded, False if no files
            are available for download or the download failed.

        """

        if self.job_response:
            if path is None:
                path = os.path.join(os.getcwd(), self.job_response.short_id)
            Path(path).mkdir(parents=True, exist_ok=True)

            if len(self.job_response.files) > 0:
                for file_uri in self.job_response.files:
                    file_name = file_uri.split('/')[-1]
                    local_path = f'{path}/{file_name}'
                    self.__download_extraction_file(file_uri, local_path)
            else:
                return False
            return True
        else:
            print("Job has not yet been submitted")
        return False


    @log.factiva_logger
    def process_job(self, path=None):  # TODO: Implement Retries if a 500 or timeout is returned during the active wait
        """
        Submit a new job to be processed, wait until the job is completed
        and then retrieves the job results.

        Returns
        -------
        bool
            True if the extraction processing was successful. An Exception
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
            time.sleep(const.API_JOB_ACTIVE_WAIT_SPACING)
            self.get_job_response()
        
        self.download_files(path=path)
        self.__log.info('process_job End')
        return True


    def __repr__(self):
        return self.__str__()


    def __str__(self, detailed=True, prefix='  ├─', root_prefix=''):
        ret_val = super().__str__(detailed, prefix, root_prefix)
        ret_val = ret_val.replace('├─job_response', '└─job_response')
        return ret_val
