import os
import shutil
import unittest

import pytest
from factiva.analytics.common import (DOWNLOAD_DEFAULT_FOLDER,
                                ISIN_COMPANY_IDENTIFIER,
                                TICKER_COMPANY_IDENTIFIER)
from factiva.analytics.common.tools import load_environment_value
from factiva.analytics.taxonomy import Company

FACTIVA_USERKEY = load_environment_value("FACTIVA_USERKEY")


class TestCompany(unittest.TestCase):

    def test_validate_point_time_request(self):
        c = Company(FACTIVA_USERKEY)
        c.validate_point_time_request(TICKER_COMPANY_IDENTIFIER)
        assert True

    def test_validate_point_time_request_not_allowed(self):
        c = Company(FACTIVA_USERKEY)
        with pytest.raises(ValueError):
            c.validate_point_time_request('badcode')  # ISIN_COMPANY_IDENTIFIER
            assert True

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

    def test_point_in_time_query(self):
        c = Company(FACTIVA_USERKEY)
        response = c.point_in_time_query(TICKER_COMPANY_IDENTIFIER, 'BC1P')
        assert isinstance(response, dict)

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(DOWNLOAD_DEFAULT_FOLDER):
            shutil.rmtree(DOWNLOAD_DEFAULT_FOLDER)
