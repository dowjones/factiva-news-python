import pytest
from factiva.analytics import SnapshotTimeSeries, UserKey, SnapshotTimeSeriesQuery
from factiva.analytics.common import config, const

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



# def test_analytics_job_V1():
#     s = Snapshot(query=VALID_QUERY)
#     assert s.user_key.key == ENVIRONMENT_USER_KEY
#     s.query.group_by_source_code = False
#     assert s.query.get_base_query() == {'query': {'where': VALID_QUERY}}
#     s.process_analytics()
#     data = s.last_analytics_job.data
#     assert len(data) > 0
#     assert data[data['publication_datetime'] == '2018-01'] is not None
#     assert len(data.loc[data['region_codes']=="ALL_REGION_CODES"])>0

# def test_analytics_job_group_dimensions():
#     s = Snapshot(query=VALID_QUERY)
#     assert s.user_key.key == ENVIRONMENT_USER_KEY
#     s.query.group_dimensions = ['region_codes', 'industry_codes']
#     assert s.query.get_base_query() == {'query': {'where': VALID_QUERY}}
#     s.process_analytics()
#     data = s.last_analytics_job.data
#     assert len(data.loc[data['region_codes']=="ALL_REGION_CODES"])==0

# def test_analytics_job_no_group_dimensions():
#     s = Snapshot(query=VALID_QUERY)
#     assert s.user_key.key == ENVIRONMENT_USER_KEY
#     assert s.query.get_base_query() == {'query': {'where': VALID_QUERY}}
#     s.process_analytics()
#     data = s.last_analytics_job.data
#     assert len(data.loc[data['region_codes']=="ALL_REGION_CODES"])>0
#     assert len(data.loc[data['industry_codes']=="ALL_INDUSTRY_CODES"])>0

# def test_analytics_error():
#     s = Snapshot(query=INVALID_WHERE_STATEMENT)
#     with pytest.raises(ValueError, match=r'Bad Request*'):
#         s.process_analytics()
