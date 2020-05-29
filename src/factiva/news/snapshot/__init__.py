__all__ = ['query']

import os
import time
import pandas as pd
from pathlib import Path
from datetime import datetime
from factiva.core import const
from factiva.news.bulknews import BulkNewsBase, BulkNewsJob
from .query import SnapshotQuery


class Snapshot(BulkNewsBase):
    """
    Class that represents a Factiva Snapshot.

    Parameters
    ----------
    api_user : str or APIKeyUser
        String containing the 32-character long APi Key. If not provided, the
        constructor will try to obtain its value from the FACTIVA_APIKEY
        environment variable.
    request_info : boolean, optional (Default: False)
        Indicates if user data has to be pulled from the server. This operation
        fills account detail properties along with maximum, used and remaining
        values. It may take several seconds to complete.
    query : str or SnapshotQuery, required
        Query used to run any of the Snapshot-related operations. If a str is
        provided, a simple query with a `where` clause is created. If other
        query fields are required, either provide the SnapshotQuery object at
        creation, or set the appropriate object values after creation.

    See Also
    --------
    Stream: Class that represents the continuous Factiva News document stream.

    Examples
    --------
    Creating a new Snapshot with an key string and a Where statement. Then,
    running a full Explain process.
        >>> from factiva.news.snapshot import Snapshot
        >>> my_key = "abcd1234abcd1234abcd1234abcd1234"
        >>> my_query = "publication_datetime >= '2020-01-01 00:00:00' AND LOWER(language_code) = 'en'"
        >>> my_snapshot = Snapshot(api_key=my_key, query=my_query)
        >>> my_snapshot.process_explain()
        106535

    Creating a new Snapshot from an APIKeyUser and a SnapshotQuery instances.
    Then, running a full Analytics process.
        >>> my_user = APIKeyUser()
        >>> my_query = SnapshotQuery("publication_datetime >= '2020-01-01 00:00:00' AND LOWER(language_code) = 'en'")
        >>> my_query.frequency = 'YEAR'
        >>> my_query.group_by_source_code = True
        >>> my_query.top = 20
        >>> my_snapshot = Snapshot(api_user=my_user, query=my_query)
        >>> analytics_df = my_snapshot.process_analytics()
        >>> analytics_df.head()
              count  publication_datetime  source_code
            0	20921	1995	NGCIOS
            1	20371	1995	LATAM
            2	18303	1995	REUTES
            3	10593	1995	EXPNSI
            4	4212	1995	MUNDO

    """
    query = None
    folder_path = ''
    file_format = ''
    file_list = []
    news_data = None
    last_explain_job = None
    last_analytics_job = None
    last_extraction_job = None

    def __init__(
        self,
        api_user=None,
        request_userinfo=False,
        query=None
    ):
        super().__init__(api_user=api_user, request_userinfo=request_userinfo)

        self.last_explain_job = ExplainJob()
        self.last_analytics_job = AnalyticsJob()
        self.last_extraction_job = ExtractionJob()

        if type(query) == SnapshotQuery:
            self.query = query
        elif type(query) == str:
            self.query = SnapshotQuery(query)
        else:
            raise ValueError("Unexpected value for where_clause")

    def submit_explain_job(self):
        """
        Submits an Explain job to the Factiva Snapshots API, using the
        assigned user in the `api_user`, and SnapshotQuery in the `query`
        properties.

        Returns
        -------
        Boolean : True if the submission was successful. An Exception otherwise.
        """
        explain_url = f'{const.API_HOST}{const.API_SNAPSHOTS_BASEPATH}{const.API_EXPLAIN_SUFFIX}'
        self.last_explain_job.submitted_datetime = datetime.now()
        response = self.submit_job(endpoint_url=explain_url, payload=self.query.get_explain_query())
        if response.status_code == 201:
            self.last_explain_job.job_id = response.json()['data']['id']
            self.last_explain_job.job_state = response.json()['data']['attributes']['current_state']
            self.last_explain_job.link = response.json()['links']['self']
        else:  # TODO: Add InvalidQuery, invalid syntax error which returns a 400
            raise RuntimeError('API Request returned an unexpected HTTP status')
        return True

    def get_explain_job_results(self):
        """
        Obtains the Explain job results from the Factiva Snapshots API. Results
        are stored in the `last_explain_job` class property.

        Returns
        -------
        Boolean : True if the data was retrieved successfully. An Exception
            otherwise.
        """
        if self.last_explain_job.link == '':
            raise RuntimeError('Explain job has not yet been submitted or job ID was not set')
        response = self.get_job_results(self.last_explain_job.link)
        if response.status_code == 200:
            self.last_explain_job.job_state = response.json()['data']['attributes']['current_state']
            if self.last_explain_job.job_state == 'JOB_STATE_DONE':
                self.last_explain_job.document_volume = response.json()['data']['attributes']['counts']
        else:
            raise RuntimeError('API request returned an unexpected HTTP status')
        return True

    def process_explain(self):
        """
        Submits an Explain job to the Factiva Snapshots API, using the same
        parameters used by `submit_explain_job`. Then, monitors the job until
        its status change to `JOB_STATE_DONE`. Finally, retrieves and stores
        the results in the property `last_explain_job`.

        Returns
        -------
        Boolean : True if the explain processing was successful. An Exception
            otherwise.
        """
        self.submit_explain_job()
        self.get_explain_job_results()
        while(True):
            if self.last_explain_job.job_state not in const.API_JOB_EXPECTED_STATES:
                raise RuntimeError('Unexpected explain job state')
            if self.last_explain_job.job_state == 'JOB_STATE_DONE':
                break
            elif self.last_explain_job.job_state == 'JOB_STATE_FAILED':
                raise Exception('Explain job failed')
            else:
                time.sleep(const.API_JOB_ACTIVE_WAIT_SPACING)
                self.get_explain_job_results()
        return True

    def submit_analytics_job(self):
        """
        Submits an Analytics job to the Factiva Snapshots API, using the
        assigned user in the `api_user`, and SnapshotQuery in the `query`
        properties.

        Returns
        -------
        Boolean : True if the submission was successful. An Exception otherwise.
        """
        analytics_url = f'{const.API_HOST}{const.API_ANALYTICS_BASEPATH}'
        self.last_analytics_job.submitted_datetime = datetime.now()
        response = self.submit_job(endpoint_url=analytics_url, payload=self.query.get_analytics_query())
        if response.status_code == 201:
            self.last_analytics_job.job_id = response.json()['data']['id']
            self.last_analytics_job.job_state = response.json()['data']['attributes']['current_state']
            self.last_analytics_job.link = response.json()['links']['self']
        else:  # TODO: Add InvalidQuery, invalid syntax error which returns a 400
            raise RuntimeError('API Request returned an unexpected HTTP status')
        return True

    def get_analytics_job_results(self):
        """
        Obtains the Analytics job results from the Factiva Snapshots API.
        Results are stored in the `last_analytics_job` class property.

        Returns
        -------
        Boolean : True if the data was retrieved successfully. An Exception
            otherwise.
        """
        if self.last_analytics_job.link == '':
            raise RuntimeError('Analytics job has not yet been submitted or job ID was not set')
        response = self.get_job_results(self.last_analytics_job.link)
        if response.status_code == 200:
            self.last_analytics_job.job_state = response.json()['data']['attributes']['current_state']
            if self.last_analytics_job.job_state == 'JOB_STATE_DONE':
                self.last_analytics_job.data = pd.DataFrame(response.json()['data']['attributes']['results'])
        else:
            raise RuntimeError('API request returned an unexpected HTTP status')
        return True

    def process_analytics(self):
        """
        Submits an Analytics job to the Factiva Snapshots API, using the same
        parameters used by `submit_analyttics_job`. Then, monitors the job until
        its status change to `JOB_STATE_DONE`. Finally, retrieves and stores
        the results in the property `last_analytics_job`.

        Returns
        -------
        Boolean : True if the analytics processing was successful. An Exception
            otherwise.
        """
        self.submit_analytics_job()
        self.get_analytics_job_results()
        while(True):
            if self.last_analytics_job.job_state not in const.API_JOB_EXPECTED_STATES:
                raise RuntimeError('Unexpected analytics job state')
            if self.last_analytics_job.job_state == 'JOB_STATE_DONE':
                break
            elif self.last_analytics_job.job_state == 'JOB_STATE_FAILED':
                raise Exception('Analytics job failed')
            else:
                time.sleep(const.API_JOB_ACTIVE_WAIT_SPACING)
                self.get_analytics_job_results()
        return True

    def submit_extraction_job(self):
        """
        Submits an Extraction job to the Factiva Snapshots API, using the
        assigned user in the `api_user`, and SnapshotQuery in the `query`
        properties.

        Returns
        -------
        Boolean : True if the submission was successful. An Exception otherwise.
        """
        extraction_url = f'{const.API_HOST}{const.API_SNAPSHOTS_BASEPATH}'
        self.last_extraction_job.submitted_datetime = datetime.now()
        response = self.submit_job(endpoint_url=extraction_url, payload=self.query.get_extraction_query())
        if response.status_code == 201:
            self.last_extraction_job.job_id = response.json()['data']['id'].split('-')[-1]
            self.last_extraction_job.job_state = response.json()['data']['attributes']['current_state']
            self.last_extraction_job.link = response.json()['links']['self']
        else:  # TODO: Add InvalidQuery, invalid syntax error which returns a 400
            raise RuntimeError('API Request returned an unexpected HTTP status')
        return True

    def get_extraction_job_results(self):
        """
        Obtains the Extraction job results from the Factiva Snapshots API.
        Results are stored in the `last_extraction_job` class property.

        Returns
        -------
        Boolean : True if the data was retrieved successfully. An Exception
            otherwise.
        """
        if self.last_extraction_job.link == '':
            raise RuntimeError('Extraction job has not yet been submitted or job ID was not set')
        response = self.get_job_results(self.last_extraction_job.link)
        if response.status_code == 200:
            self.last_extraction_job.job_state = response.json()['data']['attributes']['current_state']
            if self.last_extraction_job.job_state == 'JOB_STATE_DONE':
                self.last_extraction_job.file_format = response.json()['data']['attributes']['format']
                file_list = response.json()['data']['attributes']['files']
                self.last_extraction_job.files = []
                for file_item in file_list:
                    self.last_extraction_job.files.append(file_item['uri'])
        else:
            raise RuntimeError('API request returned an unexpected HTTP status')
        return True

    def download_extraction_files(self):
        base_path = os.path.join(os.getcwd(), self.last_extraction_job.job_id)
        Path(base_path).mkdir(parents=True, exist_ok=True)
        if len(self.last_extraction_job.files) > 0:
            for file_uri in self.last_extraction_job.files:
                file_name = file_uri.split('/')[-1]
                local_path = f'{base_path}/{file_name}'
                self.download_file(file_uri, local_path)
        return True

    def process_extraction(self):
        """
        Submits an Extraction job to the Factiva Snapshots API, using the same
        parameters used by `submit_extraction_job`. Then, monitors the job until
        its status change to `JOB_STATE_DONE`. The final status is retrieved
        and stored in the property `last_extraction_job`, which among other
        properties, contains the list of files to download. The process then
        downloads all files to a subfolder named equal to the `short_id`
        property. Finally, the process ends after all files are downloaded.

        Because the whole processing takes places in a single call, it's
        expected that the execution of this operation takes several
        minutes, or even hours.

        Returns
        -------
        Boolean : True if the extraction processing was successful. An Exception
            otherwise.
        """
        self.submit_extraction_job()
        self.get_extraction_job_results()
        while(True):
            if self.last_extraction_job.job_state not in const.API_JOB_EXPECTED_STATES:
                raise RuntimeError('Unexpected analytics job state')
            if self.last_extraction_job.job_state == 'JOB_STATE_DONE':
                break
            elif self.last_extraction_job.job_state == 'JOB_STATE_FAILED':
                raise Exception('Analytics job failed')
            else:
                time.sleep(const.API_JOB_ACTIVE_WAIT_SPACING)
                self.get_extraction_job_results()
        self.download_extraction_files()
        return True


class ExplainJob(BulkNewsJob):
    document_volume = 0

    def __init__(self):
        super().__init__()
        self.document_volume = 0


class AnalyticsJob(BulkNewsJob):
    data = []

    def __init__(self):
        super().__init__()
        self.data = []


class ExtractionJob(BulkNewsJob):
    files = []
    file_format = ''

    def __init__(self):
        super().__init__()
        self.files = []
        self.file_format = ''
