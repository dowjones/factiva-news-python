import pytest
from factiva.analytics import Snapshot
from factiva.analytics.tools import load_environment_value

ENVIRONMENT_USER_KEY = load_environment_value('FACTIVA_USERKEY')
VALID_SNAPSHOT_ID = load_environment_value('FACTIVA_SNAPSHOTID')
VALID_QUERY = "publication_datetime >= '2018-01-01 00:00:00' AND publication_datetime <= '2018-01-02 00:00:00' AND LOWER(language_code) = 'es'"
INVALID_WHERE_STATEMENT = "publecation_datetime >= '2018-01-01 00:00:00'"


# def test_ideal_extraction_job():
#     s = Snapshot(query=VALID_QUERY)
#     assert s.user_key.key == ENVIRONMENT_USER_KEY
#     assert s.query.get_base_query() == {'query': {'where': VALID_QUERY}}
#     s.process_extraction()
#     # Assert if files are downloaded to the local folder
#     # potentially use os.listdir(".").__len__()


def test_invalid_query():
    s = Snapshot(query=INVALID_WHERE_STATEMENT)
    assert s.user_key.key == ENVIRONMENT_USER_KEY
    assert s.query.get_base_query() == {'query': {'where': INVALID_WHERE_STATEMENT}}
    with pytest.raises(ValueError, match=r'Invalid Query*'):
        s.process_extraction()


# def test_download_files_custom_path():
#     s = Snapshot(snapshot_id=VALID_SNAPSHOT_ID)
#     assert s.user_key.key == ENVIRONMENT_USER_KEY
#     assert s.last_extraction_job.files.count >= 1
#     s.download_extraction_files(download_path='custom_folder')
#     # Assert files were donwloaded to custom_folder
#     # Delete custom_folder
