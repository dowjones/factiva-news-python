import pytest

from factiva.analytics.snapshot import Snapshot
from factiva.analytics.common.tools import load_environment_value

ENVIRONMENT_USER_KEY = load_environment_value('FACTIVA_USERKEY')
VALID_QUERY = "publication_datetime >= '2018-01-01 00:00:00' AND publication_datetime <= '2018-03-01 00:00:00' AND LOWER(language_code) = 'en'"
INVALID_WHERE_STATEMENT = "publecation_datetime >= '2018-01-01 00:00:00'"  # date field name is misspelled on purpose


def test_analytics_job_V1():
    s = Snapshot(query=VALID_QUERY)
    assert s.user_key.key == ENVIRONMENT_USER_KEY
    s.query.group_by_source_code = False
    assert s.query.get_base_query() == {'query': {'where': VALID_QUERY}}
    s.process_analytics()
    data = s.last_analytics_job.data
    assert len(data) > 0
    assert data[data['publication_datetime'] == '2018-01'] is not None
    assert len(data.loc[data['region_codes']=="ALL_REGION_CODES"])>0

def test_analytics_job_group_dimensions():
    s = Snapshot(query=VALID_QUERY)
    assert s.user_key.key == ENVIRONMENT_USER_KEY
    s.query.group_dimensions = ['region_codes', 'industry_codes']
    assert s.query.get_base_query() == {'query': {'where': VALID_QUERY}}
    s.process_analytics()
    data = s.last_analytics_job.data
    assert len(data.loc[data['region_codes']=="ALL_REGION_CODES"])==0

def test_analytics_job_no_group_dimensions():
    s = Snapshot(query=VALID_QUERY)
    assert s.user_key.key == ENVIRONMENT_USER_KEY
    assert s.query.get_base_query() == {'query': {'where': VALID_QUERY}}
    s.process_analytics()
    data = s.last_analytics_job.data
    assert len(data.loc[data['region_codes']=="ALL_REGION_CODES"])>0
    assert len(data.loc[data['industry_codes']=="ALL_INDUSTRY_CODES"])>0

def test_analytics_error():
    s = Snapshot(query=INVALID_WHERE_STATEMENT)
    with pytest.raises(ValueError, match=r'Bad Request*'):
        s.process_analytics()
