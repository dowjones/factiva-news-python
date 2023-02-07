import pytest
from factiva.analytics import SnapshotExtraction, UserKey, SnapshotExtractionQuery
from factiva.analytics.common import config

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



# def test_ideal_extraction_job():
#     s = Snapshot(query=VALID_QUERY)
#     assert s.user_key.key == ENVIRONMENT_USER_KEY
#     assert s.query.get_base_query() == {'query': {'where': VALID_QUERY}}
#     s.process_extraction()
#     # Assert if files are downloaded to the local folder
#     # potentially use os.listdir(".").__len__()


# def test_invalid_query():
#     s = Snapshot(query=INVALID_WHERE_STATEMENT)
#     assert s.user_key.key == ENVIRONMENT_USER_KEY
#     assert s.query.get_base_query() == {'query': {'where': INVALID_WHERE_STATEMENT}}
#     with pytest.raises(ValueError, match=r'Invalid Query*'):
#         s.process_extraction()


# def test_download_files_custom_path():
#     s = Snapshot(snapshot_id=VALID_SNAPSHOT_ID)
#     assert s.user_key.key == ENVIRONMENT_USER_KEY
#     assert s.last_extraction_job.files.count >= 1
#     s.download_extraction_files(download_path='custom_folder')
#     # Assert files were donwloaded to custom_folder
#     # Delete custom_folder
