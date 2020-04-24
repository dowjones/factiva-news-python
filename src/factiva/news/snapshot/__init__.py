import time
from datetime import datetime
from factiva.core import const
from factiva.news.bulknews import BulkNewsBase, BulkNewsJob
from factiva.news.snapshot.query import SnapshotQuery


class Snapshot(BulkNewsBase):

    query = None
    folder_path = ''
    file_format = ''
    file_list = []
    news_data = None
    last_explain_job = None
    last_analytics_job = None

    def __init__(
        self,
        api_user=None,
        request_userinfo=False,
        query=None
    ):
        super().__init__(api_user=api_user, request_userinfo=request_userinfo)

        self.last_explain_job = ExplainJob()
        self.last_analytics_job = AnalyticsJob()

        if type(query) == SnapshotQuery:
            self.query = query
        elif type(query) == str:
            self.query = SnapshotQuery(query)
        else:
            raise ValueError("Unexpected value for where_clause")

    def submit_explain_job(self):
        explain_url = f'{const.DJ_API_HOST}{const.DJ_API_SNAPSHOTS_BASEPATH}{const.DJ_API_EXPLAIN_SUFFIX}'
        self.last_explain_job.submitted_datetime = datetime.now()
        response = self.submit_job(endpoint_url=explain_url, payload=self.query.get_explain_query())
        if response.status_code == 201:
            self.last_explain_job.job_id = response.json()['data']['id'],
            self.last_explain_job.job_state = response.json()['data']['attributes']['current_state']
            self.last_explain_job.link = response.json()['links']['self']
        else:
            raise RuntimeError('API Request returned an unexpected HTTP status')
        return True

    def get_explain_job_results(self):
        if self.last_explain_job.link == '':
            raise RuntimeError('Explain job has not yet been submitted')
        response = self.get_job_results(self.last_explain_job.link)
        if response.status_code == 200:
            self.last_explain_job.job_state = response.json()['data']['attributes']['current_state']
            if self.last_explain_job.job_state == 'JOB_STATE_DONE':
                self.last_explain_job.document_volume = response.json()['data']['attributes']['counts']
        else:
            raise RuntimeError('API request returned an unexpected HTTP status')
        return True

    def process_explain(self):
        self.submit_explain_job()
        self.get_explain_job_results()
        while(True):
            if self.last_explain_job.job_state not in const.EXPECTED_JOB_STATES:
                raise RuntimeError('Unexpected explain job state')
            if self.last_explain_job.job_state == 'JOB_STATE_DONE':
                break
            elif self.last_explain_job.job_state == 'JOB_STATE_FAILED':
                raise Exception('Explain job failed')
            else:
                time.sleep(const.ACTIVE_WAIT_SPACING)
                self.get_explain_job_results()
        return self.last_explain_job.document_volume

    def submit_analytics_job(self):
        analytics_url = f'{const.DJ_API_HOST}{const.DJ_API_ANALYTICS_BASEPATH}'
        self.last_analytics_job.submitted_datetime = datetime.now()
        response = self.submit_job(endpoint_url=analytics_url, payload=self.query.get_analytics_query())
        if response.status_code == 201:
            self.last_analytics_job.job_id = response.json()['data']['id'],
            self.last_analytics_job.job_state = response.json()['data']['attributes']['current_state']
            self.last_analytics_job.link = response.json()['links']['self']
        else:
            raise RuntimeError('API Request returned an unexpected HTTP status')
        return True

    def get_analytics_job_results(self):
        if self.last_analytics_job.link == '':
            raise RuntimeError('Analytics job has not yet been submitted')
        response = self.get_job_results(self.last_analytics_job.link)
        if response.status_code == 200:
            self.last_analytics_job.job_state = response.json()['data']['attributes']['current_state']
            if self.last_analytics_job.job_state == 'JOB_STATE_DONE':
                self.last_analytics_job.data = response.json()['data']['attributes']['results']
        else:
            raise RuntimeError('API request returned an unexpected HTTP status')
        # TODO: Transform results to Pandas DataFrame
        return True

    def process_analytics(self):
        self.submit_analytics_job()
        self.get_analytics_job_results()
        while(True):
            if self.last_analytics_job.job_state not in const.EXPECTED_JOB_STATES:
                raise RuntimeError('Unexpected analytics job state')
            if self.last_analytics_job.job_state == 'JOB_STATE_DONE':
                break
            elif self.last_analytics_job.job_state == 'JOB_STATE_FAILED':
                raise Exception('Analytics job failed')
            else:
                time.sleep(const.ACTIVE_WAIT_SPACING)
                self.get_analytics_job_results()
        return self.last_analytics_job.data


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
