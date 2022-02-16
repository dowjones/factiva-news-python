"""Implement actions with Bulk news such as Snapshot and Stream."""
import os
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
from factiva.core import UserKey, const, req
from factiva.core.tools import mask_string


def parse_field(field, field_name):
    """Parse field according to field type.

    Parameters
    ----------
    field: str, dict
        field to be parsed. When a dictionary is given, it will return it
        as is. When a string is provided it will return the eval version
        of it, in order to return a dict
    field_name: str
        name of the field to be parsed. It is displayed in the error message
        when the field type is not valid.

    """
    if isinstance(field, dict):
        return field

    if isinstance(field, str):
        return eval(field)

    raise ValueError(f'Unexpected value for {field_name}')


class BulkNewsBase():
    """Represent actions with Bulk news such as Snapshot and Stream.

    Class that serves as a base for the diferent classes representing actions with bulk news such as Snapshot and Stream
    This class is not to be instantiated directly.

    Parameters
    ----------
    key : str or UserKey
        String containing the 32-character long APi Key. If not provided, the constructor will try to obtain its
        value from the FACTIVA_USERKEY environment variable.
    stats : boolean, optional (Default: False)
        Indicates if user data has to be pulled from the server. This operation fills account detail properties
        along with maximum, used and remaining values. It may take several seconds to complete.

    """

    user_key = None

    _request_datetime = ''
    _job_status = ''

    def __init__(
        self,
        user_key=None,
        user_stats=False
    ):
        """Initialize class constructor."""
        self.user_key = UserKey.create_user_key(user_key, user_stats)

    def load_data(self):
        """Load generic data."""
        pass

    def process_operation(self):
        """Process generic operation."""
        pass

    def __str__(self):
        """Create string representation for BulkNewsBase Class."""
        pprop = self.__dict__.copy()
        del pprop['user_key']
        masked_key = mask_string(self.user_key.user_key)
        user_class = str(self.user_key.__class__)

        ret_val = str(self.__class__) + '\n'
        ret_val += f'  user_key = {masked_key} ({user_class})\n'
        ret_val += '  '.join(('{} = {}\n'.format(item, pprop[item]) for item in pprop))
        return ret_val

    def __repr__(self):
        """Create string representation for BulkNewsBase Class."""
        return self.__str__()


class BulkNewsQuery():  # pylint: disable=too-few-public-methods
    """Define base class for queries to make API requests.

    Class that serves as a base for the queries used to make requests to the APIs.

    Parameters
    ----------
    where: str
        String containing the query where clause
    includes: str or dict, Optional (Default: None)
        Contains the query includes clause, which defines the codes to include for specific fields.
        - When a dict is given it is assigned as it is.
        - When a string is given, it is expected to have a JSON format and is parsed to be stored as a dict
    excludes: str or dict, Optional (Default: None)
        Contains the query excludes clause, which defines the codes to exclude for especific fields.
        - When a dict is given it is assigned as it is.
        - When a string is given, it is expected to have a JSON format and is parsed to be stored as a dict.
    select_fields: str or list, Optional (Default: None)
        Contains the query select clause, which defines the fields to be returned by the query.
        - When a list is given it is assigned as it is
        - When a string is given, it is expected to have a JSON array format and is parsed to be stored as a list

    """

    where = ""
    includes = ""
    excludes = ""
    select_fields = ""

    def __init__(
        self,
        where,
        includes=None,
        excludes=None,
        select_fields=None
    ):
        """Initialize class."""
        if isinstance(where, str):
            self.where = where  # TODO: Validate syntax if possible. At least it's a string.
        else:
            raise ValueError("Unexpected where clause.")

        if includes:
            self.includes = parse_field(includes, 'includes')

        if excludes:
            self.excludes = parse_field(excludes, 'excludes')

        if select_fields:
            if isinstance(select_fields, list):
                self.select_fields = select_fields  # TODO: Validate syntax if possible
            elif isinstance(select_fields, str):
                self.select_fields = eval(select_fields)
            else:
                raise ValueError("Unexpected value for select_fields")

    def get_base_query(self):
        """Create the basic query to be used within the Factiva Snapshots API and the Factiva Streams API.

        Returns
        -------
        Dictionary containing the fields that where assigned to the query.

        """
        query_dict = {
            "query": {
                "where": self.where
            }
        }

        if self.select_fields:
            query_dict["query"].update({"select": self.select_fields})

        if self.includes:
            query_dict["query"].update({"includes": self.includes})

        if self.excludes:
            query_dict["query"].update({'excludes': self.excludes})

        return query_dict


class BulkNewsJob():
    """Represent the operations of the base class.

    Base class to represent the operations that can be done using Factiva Snapshots API or Factiva Streams API.

    The operations are done in two steps:
    1. Submit job:
        This will make a request to the corresponding API to start a new job and, if successful, the API will return a job_id that can be used to monitor the status of the job
    2. Get job results:
        This will make a request to the API to get the status of the job. If the job is finished, the results are saved.

    This class is not to be instantiated directly, rather to extend in order to implement the missing methods.

    Parameters
    ----------
    user_key:
        user_key : str or UserKey
        String containing the 32-character long APi Key. If not provided, the
        constructor will try to obtain its value from the FACTIVA_USERKEY
        environment variable.
    user_key_stats : boolean, optional (Default: False)
        Indicates if user data has to be pulled from the server. This operation
        fills account detail properties along with maximum, used and remaining
        values. It may take several seconds to complete.

    """

    job_id = ''
    job_state = ''
    submitted_datetime = 0
    link = ''
    files = []

    def __init__(self, user_key=None, user_key_stats=False):
        """Initialize Bulk news job class."""
        self.job_id = ''
        self.job_state = ''
        self.submitted_datetime = datetime.now()
        self.link = ''
        self.user_key = UserKey.create_user_key(user_key, user_key_stats)

    def get_endpoint_url(self) -> str:
        """Create the URL for the API endpoint to send the request to sumbit a job, according to the kind of job that is being created.

        This method needs to be defined by each class that extends this one.

        Returns
        -------
        String containing the endpoint URL to sumbit the job to.

        Raises
        ------
        - NotImplementedError when the method has not been defined by the extending class

        """
        raise NotImplementedError('Method has not been defined')

    def get_job_id(self, source: dict) -> str:
        """Obtain the job_id from the source parameter. The job_id is defined diferently according to the type of job.

        Parameters
        ----------
        source: dict
            Dictionary containing the data from where to obtain the id from. Usually is a dictionary representation of the response given by the API

        Returns
        -------
        String containing the job id

        Raises
        ------
        - NotImplementedError when the method has not been defined by the extending class

        """
        raise NotImplementedError('Method has not been defined')

    def set_job_data(self, source: dict):
        """Obtain the data for the job, based on the response from the API.

        Parameters
        ----------
        source: dict
            Dictionary containing the data from where to obtain the job results from. Usually is a dictionary representation of the response given by the API

        Returns
        -------
        No return value

        Raises
        ------
        - NotImplementedError when the method has not been defined by the extending class

        """
        raise NotImplementedError('Method has not been defined')

    def submit_job(self, payload=None, use_latest_api_version=False) -> bool:
        """Submit a new job to be processed to the Factiva Snapshots API or Streams API.

        Submits a new job to be processed to the Factiva Snapshots API or Streams API.
        On a successful response from the API, saves the link of the job as well as the job_id on the caller instance.

        Parameters
        ----------
        payload: dict or str, Optional
            Contains the payload required to create the new job. Usually some, the query to be sent to the API.
            - When a dict is given, it is converted into a string
            - When a string is given, it is used as it is.

        Returns
        -------
        Boolean: True if the data was retrieved successfully. An Exception
            otherwise.

        Raises
        ------
        - RuntimeError when the API response does not have a 201 status code.

        """
        self.submitted_datetime = datetime.now()

        headers_dict = {
                'user-key': self.user_key.key,
                'Content-Type': 'application/json'
            }
        if use_latest_api_version:
            version_header = {'X-API-VERSION': const.API_LATEST_VERSION}
            headers_dict.update(version_header)

        response = req.api_send_request(method='POST', endpoint_url=self.get_endpoint_url(), headers=headers_dict, payload=payload)

        if response.status_code == 201:
            response_data = response.json()
            self.job_id = self.get_job_id(response_data)
            self.job_state = response_data['data']['attributes']['current_state']
            self.link = response_data['links']['self']
        elif response.status_code == 400:
            raise ValueError(f'Invalid Query [{response.text}]')
        else:
            raise RuntimeError(f'API request returned an unexpected HTTP status, with content [{response.text}]')
        return True

    def get_job_results(self) -> bool:
        """Make a request to the API using the link of the job to get its status.

        Makes a request to the API using the link of the job to get its status. If the job has been completed,
        obtains the results of the job.

        Returns
        -------
        Boolean : True if the submission was successful. An Exception otherwise.

        Raises
        ------
        - RuntimeError when the job does not have a link to request the status to or when the response from the API is 404, meaning that the job was not found.
        Also, when the API returns a status code different than 200.

        """
        if self.link == '':
            raise RuntimeError('Job has not yet been submitted or Job ID was not set')

        headers_dict = {
            'user-key': self.user_key.key,
            'Content-Type': 'application/json'
        }

        response = req.api_send_request(method='GET', endpoint_url=self.link, headers=headers_dict)

        if response.status_code == 200:
            response_data = response.json()
            self.job_state = response_data['data']['attributes']['current_state']
            if self.job_state == const.API_JOB_DONE_STATE:
                self.set_job_data(response_data)
        elif response.status_code == 404:
            raise RuntimeError('Job ID does not exist.')
        else:
            raise RuntimeError(f'API request returned an unexpected HTTP status, with content [{response.text}]')
        return True

    def process_job(self, payload=None, use_latest_api_version=False) -> bool:
        """Submit a new job to be processed, wait until the job is completed and then retrieves the job results.

        Parameters
        ----------
        payload: dict or str, Optional
            Contains the payload required to create the new job.
            - When a dict is given, it is converted into a string
            - When a string is given, it is used as it is.

        Returns
        -------
        Boolean : True if the explain processing was successful. An Exception
            otherwise.

        Raises
        ------
        - RuntimeError when the job returns and unexpected state
        - Exception when the job has failed to complete

        """
        self.submit_job(payload=payload, use_latest_api_version=use_latest_api_version)
        self.get_job_results()

        while self.job_state != const.API_JOB_DONE_STATE:
            if self.job_state not in const.API_JOB_EXPECTED_STATES:
                raise RuntimeError('Unexpected job state')
            if self.job_state == const.API_JOB_FAILED_STATE:
                raise Exception('Job failed')

            time.sleep(const.API_JOB_ACTIVE_WAIT_SPACING)
            self.get_job_results()

        return True

    def download_file(self, endpoint_url: str, download_path: str):
        """Download a file from a job, using the file URL and stores them in download_path.

        Parameters
        ----------
        endpoint_url: str
            String containing the URL to download the file from
        download_path: str
            String containing the path where to store the downloaded file

        Returns
        -------
        Boolean : True if the download was successful. An Exception otherwise.

        Raises
        ------
        - RuntimeException when the response from the API is not successful

        """
        headers_dict = {
                'user-key': self.user_key.key
            }
        response = req.api_send_request(method='GET', endpoint_url=endpoint_url, headers=headers_dict)

        if response.status_code == 200:
            with open(download_path, 'wb') as download_file_path:
                download_file_path.write(response.content)
        else:
            raise RuntimeError(f'API request returned an unexpected HTTP status, with content [{response.text}]')
        return True

    def download_job_files(self, download_path=None):
        """Download all the files from a job ans stores them in the given download_path.

        If no download path is given, the files are stored in a folder with the name of the job_id.

        Parameters
        ----------
        download_path: str, Optional
            String containing the path where to store the downloaded files.
            If not provided, the files are stored in a folder named after the job_id. If such folder does not exists, it is created in the current working directory.

        Returns
        -------
        Boolean : True if the files were correctly downloaded, False otherwise.

        Raises
        ------
        - RuntimeError when there are no files available for download

        """
        if download_path is None:
            download_path = os.path.join(os.getcwd(), self.job_id)
        Path(download_path).mkdir(parents=True, exist_ok=True)

        if len(self.files) > 0:
            for file_uri in self.files:
                file_name = file_uri.split('/')[-1]
                local_path = f'{download_path}/{file_name}'
                self.download_file(file_uri, local_path)
        else:
            raise RuntimeError('No files available for download')
        return True

    def get_job_samples(self, num_samples):
        """Obtain the Explain job samples from the Factiva Snapshots API.
        Returns a dataframe of up to 100 sample documents which  includes title and metadata fields.

        Parameters
        ----------
        num_samples: int, Optional
            Number of sample documents to get explained by a job

        Returns
        -------
        Boolean : True if the files were correctly downloaded, False otherwise.

        Raises
        ------
        - RuntimeError when there are no files available for download

        """
        headers_dict = {
            'user-key': self.user_key.key
        }
        s_param = { 'num_samples': num_samples }
        samples_url=f'{self.get_endpoint_url()}/{self.job_id}'
        response = req.api_send_request(method='GET', endpoint_url=samples_url, headers=headers_dict, qs_params=s_param)
        if response.status_code == 200:
            resp_json = response.json()['data']['attributes']['sample']
            samples = pd.DataFrame(resp_json)
            print(f'DataFrame size: {samples.shape}')
            print(f'Columns: {samples.columns}')
            return samples
        else:
            print(f'Unexpected Response: {response.text}')

    def __repr__(self):
        """Create string representation for BulkNews Class."""
        return self.__str__()

    def __str__(self, detailed=True, prefix='  |-', root_prefix=''):
        """Create string representation for BulkNews Class."""
        pprop = self.__dict__.copy()
        ret_val = str(self.__class__)
        ret_val += '\n'
        if self.job_id == '':
            ret_val += f'{prefix}<Empty>'
        else:
            ret_val += '\n'.join(('{}{} = {}'.format(prefix, item, pprop[item]) for item in pprop))
        return ret_val
