import shutil
import unittest
import pytest

from factiva.core.const import (DOWNLOAD_DEFAULT_FOLDER,
                                TICKER_COMPANY_IDENTIFIER)
from factiva.core.tools import load_environment_value
from factiva.news.taxonomy import Company

FACTIVA_USERKEY = load_environment_value("FACTIVA_USERKEY")


class TestRequests(unittest.TestCase):

    def test_download_companies_identifier(self):
        c = Company(FACTIVA_USERKEY)
        local_file_name = c.point_in_time_download_all(
            TICKER_COMPANY_IDENTIFIER, 'local_test', 'csv', add_timestamp=True)
        assert isinstance(local_file_name, str)

    def test_download_error_bad_file_type(self):
        c = Company(FACTIVA_USERKEY)
        with pytest.raises(ValueError):
            local_file_name = c.point_in_time_download_all(
                TICKER_COMPANY_IDENTIFIER,
                'local_test',
                'jpg',
                add_timestamp=True)
            assert isinstance(local_file_name, str)

    def test_download_error_bad_identifier(self):
        c = Company(FACTIVA_USERKEY)
        with pytest.raises(ValueError):
            local_file_name = c.point_in_time_download_all('dump_identifier',
                                                           'local_test',
                                                           'jpg',
                                                           add_timestamp=True)
            assert isinstance(local_file_name, str)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(DOWNLOAD_DEFAULT_FOLDER)
