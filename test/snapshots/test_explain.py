import pytest
from factiva.analytics.common import config
from factiva.analytics import SnapshotExplain, UserKey, SnapshotExplainQuery

ENVIRONMENT_USER_KEY = config.load_environment_value('FACTIVA_USERKEY')
VALID_USER_KEY = config.load_environment_value('FACTIVA_USERKEY')
ENVIRONMENT_WHERE_STATEMENT = config.load_environment_value('FACTIVA_WHERE')
VALID_WHERE_STATEMENT = "publication_datetime >= '2023-01-01 00:00:00' AND publication_datetime < '2023-02-01 00:00:00' AND LOWER(language_code) = 'en'"
INVALID_WHERE_STATEMENT = "publecation_datetime >= '2023-01-01 00:00:00'"  # date field name is misspelled on purpose

# Test operations before any API request

def test_create_from_envuser():
    se = SnapshotExplain()
    assert isinstance(se, SnapshotExplain)
    assert se.user_key.key == ENVIRONMENT_USER_KEY
    assert se.query.get_payload() == {
        'query': {
            'where': ENVIRONMENT_WHERE_STATEMENT
        }
    }

def test_create_from_user_param():
    se = SnapshotExplain(user_key=VALID_USER_KEY)
    assert isinstance(se, SnapshotExplain)
    assert se.user_key.key == VALID_USER_KEY
    assert se.query.get_payload() == {
        'query': {
            'where': ENVIRONMENT_WHERE_STATEMENT
        }
    }

def test_create_from_userkey():
    u = UserKey()
    assert isinstance(u, UserKey)
    se = SnapshotExplain(user_key=u)
    assert isinstance(se, SnapshotExplain)
    assert se.user_key.key == ENVIRONMENT_USER_KEY
    assert se.query.get_payload() == {
        'query': {
            'where': ENVIRONMENT_WHERE_STATEMENT
        }
    }

def test_create_envuser_where():
    se = SnapshotExplain(query=VALID_WHERE_STATEMENT)
    assert isinstance(se, SnapshotExplain)
    assert se.user_key.key == ENVIRONMENT_USER_KEY
    assert se.query.get_payload() == {
        'query': {
            'where': VALID_WHERE_STATEMENT
        }
    }

def test_create_envuser_envwhere():
    seq = SnapshotExplainQuery()
    assert isinstance(seq, SnapshotExplainQuery)
    se = SnapshotExplain(query=seq)
    assert isinstance(se, SnapshotExplain)
    assert se.user_key.key == ENVIRONMENT_USER_KEY
    assert se.query.get_payload() == {
        'query': {
            'where': ENVIRONMENT_WHERE_STATEMENT
        }
    }

def test_failed_where_and_jobid():
    with pytest.raises(ValueError, match=r'The query and job_id parameters*'):
        se = SnapshotExplain(query=VALID_WHERE_STATEMENT, job_id='abcd1234-ab12-ab12-ab12-abcdef123456')
        assert isinstance(se, SnapshotExplain)


