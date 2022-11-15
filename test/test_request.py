import os
import shutil
import unittest

import pytest
from factiva.core.const import (API_HOST, API_SNAPSHOTS_COMPANIES_PIT,
                                API_SNAPSHOTS_TAXONOMY_BASEPATH,
                                DOWNLOAD_DEFAULT_FOLDER,
                                TICKER_COMPANY_IDENTIFIER)
from factiva.core.req import download_file
from factiva.core.tools import load_environment_value

FACTIVA_USERKEY = load_environment_value("FACTIVA_USERKEY")
headers_dict = {'user-key': FACTIVA_USERKEY}


def dowload_pit_url(identifier, file_format):
    return f'{API_HOST}{API_SNAPSHOTS_TAXONOMY_BASEPATH}{API_SNAPSHOTS_COMPANIES_PIT}/{identifier}/{file_format}'


class TestRequests(unittest.TestCase):

    def test_dowload_csv_file(self):
        file_format = 'csv'
        file_name = download_file(dowload_pit_url(TICKER_COMPANY_IDENTIFIER,
                                                  file_format),
                                  headers_dict,
                                  'demo',
                                  file_format,
                                  DOWNLOAD_DEFAULT_FOLDER,
                                  add_timestamp=True)
        assert isinstance(file_name, str)

    def test_download_error_bad_file_type(self):
        file_format = 'jpg'
        with pytest.raises(ValueError):
            file_name = download_file(
                dowload_pit_url(TICKER_COMPANY_IDENTIFIER, file_format),
                headers_dict, 'demo', file_format, DOWNLOAD_DEFAULT_FOLDER)
            assert isinstance(file_name, str)

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(DOWNLOAD_DEFAULT_FOLDER):
            shutil.rmtree(DOWNLOAD_DEFAULT_FOLDER)
