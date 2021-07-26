import pytest

from factiva.news.snapshot import Snapshot
from factiva.helper import load_environment_value

ENVIRONMENT_USER_KEY = load_environment_value('FACTIVA_USERKEY')
VALID_QUERY = "publication_datetime >= '2018-01-01 00:00:00' AND publication_datetime <= '2018-03-01 00:00:00' AND LOWER(language_code) = 'en'"
INVALID_WHERE_STATEMENT = "publecation_datetime >= '2018-01-01 00:00:00'"  # date field name is misspelled on purpose


def test_analytics_job(): 
    s = Snapshot(query=VALID_QUERY)
    assert s.user_key.key == ENVIRONMENT_USER_KEY
    assert s.query.get_base_query() == {'query': {'where': VALID_QUERY}}
    s.process_analytics()
    data = s.last_analytics_job.data
    assert len(data) > 0
    assert data[data['publication_datetime'] == '2018-01'] is not None


def test_analytics_error():
    s = Snapshot(query=INVALID_WHERE_STATEMENT)
    with pytest.raises(ValueError, match=r'Unrecognized name*'):
        s.process_analytics()
