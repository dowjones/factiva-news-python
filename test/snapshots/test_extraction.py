import pytest
from factiva.analytics import SnapshotExtraction, UserKey, SnapshotExtractionQuery
from factiva.analytics.common import config, const

GITHUB_CI = config.load_environment_value('CI', False)
ENVIRONMENT_USER_KEY = config.load_environment_value('FACTIVA_USERKEY')
VALID_USER_KEY = config.load_environment_value('FACTIVA_USERKEY')
ENVIRONMENT_WHERE_STATEMENT = config.load_environment_value('FACTIVA_WHERE')
VALID_WHERE_STATEMENT = "publication_datetime >= '2023-01-01 00:00:00' AND publication_datetime < '2023-02-01 00:00:00' AND LOWER(language_code) = 'en'"
INVALID_WHERE_STATEMENT = "publecation_datetime >= '2023-01-01 00:00:00'"  # date field name is misspelled on purpose

# Test operations before any API request

def test_create_from_envuser():
    se = SnapshotExtraction()
    assert isinstance(se, SnapshotExtraction)
    assert se.user_key.key == ENVIRONMENT_USER_KEY
    assert se.query.get_payload() == {
        'query': {
            'where': ENVIRONMENT_WHERE_STATEMENT,
            'format': 'avro'
        }
    }

def test_create_from_user_param():
    se = SnapshotExtraction(user_key=VALID_USER_KEY)
    assert isinstance(se, SnapshotExtraction)
    assert se.user_key.key == VALID_USER_KEY
    assert se.query.get_payload() == {
        'query': {
            'where': ENVIRONMENT_WHERE_STATEMENT,
            'format': 'avro'
        }
    }

def test_create_from_userkey():
    u = UserKey()
    assert isinstance(u, UserKey)
    se = SnapshotExtraction(user_key=u)
    assert isinstance(se, SnapshotExtraction)
    assert se.user_key.key == ENVIRONMENT_USER_KEY
    assert se.query.get_payload() == {
        'query': {
            'where': ENVIRONMENT_WHERE_STATEMENT,
            'format': 'avro'
        }
    }

def test_create_envuser_where():
    se = SnapshotExtraction(query=VALID_WHERE_STATEMENT)
    assert isinstance(se, SnapshotExtraction)
    assert se.user_key.key == ENVIRONMENT_USER_KEY
    assert se.query.get_payload() == {
        'query': {
            'where': VALID_WHERE_STATEMENT,
            'format': 'avro'
        }
    }

def test_create_envuser_envwhere():
    seq = SnapshotExtractionQuery()
    assert isinstance(seq, SnapshotExtractionQuery)
    se = SnapshotExtraction(query=seq)
    assert isinstance(se, SnapshotExtraction)
    assert se.user_key.key == ENVIRONMENT_USER_KEY
    assert se.query.get_payload() == {
        'query': {
            'where': ENVIRONMENT_WHERE_STATEMENT,
            'format': 'avro'
        }
    }

def test_failed_where_and_jobid():
    with pytest.raises(ValueError, match=r'The query and job_id parameters*'):
        se = SnapshotExtraction(query=VALID_WHERE_STATEMENT, job_id='abcd1234-ab12-ab12-ab12-abcdef123456')
        assert isinstance(se, SnapshotExtraction)


# Test operations sending requests to the API
# These are only executed when running locally. For optimisation purposes
# no API tests are executed in the CI/CD (GitHub Actions) environment.

def test_job_envuser_envwhere():
    if GITHUB_CI:
        pytest.skip("Not to be tested in GitHub Actions")
    sts = SnapshotExtraction()
    assert isinstance(sts, SnapshotExtraction)
    assert sts.process_job()
    assert sts.job_response.job_state == const.API_JOB_DONE_STATE
    assert isinstance(sts.job_response.job_id, str)
    assert len(sts.job_response.job_id) == 64
    assert sts.job_response.job_link.startswith(const.API_HOST)
    assert len(sts.job_response.files) >= 1
    assert (sts.job_response.errors == None)
    assert len(sts.job_response.short_id) == 10
