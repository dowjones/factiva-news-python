import os
# import pandas as pd
# import fastavro
from factiva.news.bulknewsbase import BulkNewsBase


class Snapshot(BulkNewsBase):

    folder_path = ''
    file_format = ''
    file_list = []

    def __init__(
        self,
        api_user=None,
        request_userinfo=False,
        query=None,
        job_id=None
    ):
        super().__init__(api_user, request_userinfo, query, job_id)
        self.folder_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        self.file_format = 'AVRO'
        self.file_list = []
