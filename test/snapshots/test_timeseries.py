import pytest
import pandas as pd
from factiva.analytics import SnapshotTimeSeries, UserKey, SnapshotTimeSeriesQuery
from factiva.analytics.common import config, const

GITHUB_CI = config.load_environment_value('CI', False)
ENVIRONMENT_USER_KEY = config.load_environment_value('FACTIVA_USERKEY')
VALID_USER_KEY = config.load_environment_value('FACTIVA_USERKEY')
ENVIRONMENT_WHERE_STATEMENT = config.load_environment_value('FACTIVA_WHERE')
VALID_WHERE_STATEMENT = "publication_datetime >= '2023-01-01 00:00:00' AND publication_datetime < '2023-02-01 00:00:00' AND LOWER(language_code) = 'en'"
INVALID_WHERE_STATEMENT = "publecation_datetime >= '2023-01-01 00:00:00'"  # date field name is misspelled on purpose


def test_create_from_envuser():
    sts = SnapshotTimeSeries()
    assert isinstance(sts, SnapshotTimeSeries)
    assert sts.user_key.key == ENVIRONMENT_USER_KEY
    assert sts.query.get_payload() == {
        'query': {
            'where': ENVIRONMENT_WHERE_STATEMENT,
            'frequency': const.API_MONTH_PERIOD,
            'date_field': const.API_PUBLICATION_DATETIME_FIELD,
            'group_dimensions': [],
            'top': 10
        }
    }

def test_create_from_user_param():
    sts = SnapshotTimeSeries(user_key=VALID_USER_KEY)
    assert isinstance(sts, SnapshotTimeSeries)
    assert sts.user_key.key == VALID_USER_KEY
    assert sts.query.get_payload() == {
        'query': {
            'where': ENVIRONMENT_WHERE_STATEMENT,
            'frequency': const.API_MONTH_PERIOD,
            'date_field': const.API_PUBLICATION_DATETIME_FIELD,
            'group_dimensions': [],
            'top': 10
        }
    }

def test_create_from_userkey():
    u = UserKey()
    assert isinstance(u, UserKey)
    sts = SnapshotTimeSeries(user_key=u)
    assert isinstance(sts, SnapshotTimeSeries)
    assert sts.user_key.key == ENVIRONMENT_USER_KEY
    assert sts.query.get_payload() == {
        'query': {
            'where': ENVIRONMENT_WHERE_STATEMENT,
            'frequency': const.API_MONTH_PERIOD,
            'date_field': const.API_PUBLICATION_DATETIME_FIELD,
            'group_dimensions': [],
            'top': 10
        }
    }

def test_create_envuser_where():
    sts = SnapshotTimeSeries(query=VALID_WHERE_STATEMENT)
    assert isinstance(sts, SnapshotTimeSeries)
    assert sts.user_key.key == ENVIRONMENT_USER_KEY
    assert sts.query.get_payload() == {
        'query': {
            'where': VALID_WHERE_STATEMENT,
            'frequency': const.API_MONTH_PERIOD,
            'date_field': const.API_PUBLICATION_DATETIME_FIELD,
            'group_dimensions': [],
            'top': 10
        }
    }

def test_create_envuser_envwhere():
    seq = SnapshotTimeSeriesQuery()
    assert isinstance(seq, SnapshotTimeSeriesQuery)
    sts = SnapshotTimeSeries(query=seq)
    assert isinstance(sts, SnapshotTimeSeries)
    assert sts.user_key.key == ENVIRONMENT_USER_KEY
    assert sts.query.get_payload() == {
        'query': {
            'where': ENVIRONMENT_WHERE_STATEMENT,
            'frequency': const.API_MONTH_PERIOD,
            'date_field': const.API_PUBLICATION_DATETIME_FIELD,
            'group_dimensions': [],
            'top': 10
        }
    }

def test_failed_where_and_jobid():
    with pytest.raises(ValueError, match=r'The query and job_id parameters*'):
        sts = SnapshotTimeSeries(query=VALID_WHERE_STATEMENT, job_id='abcd1234-ab12-ab12-ab12-abcdef123456')
        assert isinstance(sts, SnapshotTimeSeries)


# Test operations sending requests to the API
# These are only executed when running locally. For optimisation purposes
# no API tests are executed in the CI/CD (GitHub Actions) environment.

def test_job_envuser_envwhere():
    if GITHUB_CI:
        pytest.skip("Not to be tested in GitHub Actions")
    sts = SnapshotTimeSeries()
    assert isinstance(sts, SnapshotTimeSeries)
    assert sts.process_job()
    assert sts.job_response.job_state == const.API_JOB_DONE_STATE
    assert isinstance(sts.job_response.job_id, str)
    assert len(sts.job_response.job_id) == 36
    assert sts.job_response.job_link.startswith(const.API_HOST)
    assert (sts.job_response.errors == None)
    assert isinstance(sts.job_response.data, pd.DataFrame)
    assert len(sts.job_response.data.columns) >= 2

