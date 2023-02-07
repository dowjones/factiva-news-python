import pytest
from factiva.analytics.common import config, const
from factiva.analytics import SnapshotExplain, UserKey, SnapshotExplainQuery
import pandas as pd

GITHUB_CI = config.load_environment_value('CI', False)
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

# Test operations sending requests to the API
# These are only executed when running locally. For optimisation purposes
# no API tests are executed in the CI/CD (GitHub Actions) environment.

def test_job_envwhere_samples():
    if GITHUB_CI:
        pytest.skip("Not to be tested in GitHub Actions")
    se = SnapshotExplain()
    assert isinstance(se, SnapshotExplain)
    assert se.process_job()
    assert se.job_response.job_state == const.API_JOB_DONE_STATE
    assert isinstance(se.job_response.job_id, str)
    assert len(se.job_response.job_id) == 36
    assert se.job_response.job_link.startswith(const.API_HOST)
    assert isinstance(se.job_response.volume_estimate, int)
    assert se.job_response.volume_estimate > 0
    assert se.job_response.errors == None
    assert se.get_samples()
    assert se.samples.num_samples > 0
    assert isinstance(se.samples.data, pd.DataFrame)
    assert len(se.samples.data.columns) >= 3
