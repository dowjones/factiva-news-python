import pytest

from factiva.analytics import UserKey, common
from factiva.analytics.common.tools import load_environment_value
from factiva.analytics.snapshot import Snapshot, SnapshotQuery

# TEST DATA. Fill out these values with valid data
# Environment values are loaded for automated testing.
ENVIRONMENT_USER_KEY = load_environment_value('FACTIVA_USERKEY')
VALID_USER_KEY = load_environment_value('FACTIVA_USERKEY')
VALID_SNAPSHOT_ID = load_environment_value('FACTIVA_SNAPSHOTID')
VALID_WHERE_STATEMENT = "publication_datetime = '2021-01-01'"

# Dummy key to test data flow without making requests to API
aku = UserKey(VALID_USER_KEY)


# USER-CENTRIC tests
def test_create_snapshot_evironment_variable():
    s = Snapshot(query=VALID_WHERE_STATEMENT)
    assert s.user_key.key == ENVIRONMENT_USER_KEY
    assert s.query.get_base_query() == {'query': {'where': VALID_WHERE_STATEMENT}}


def test_create_snapshot_str_key():
    s = Snapshot(query=VALID_WHERE_STATEMENT, user_key=VALID_USER_KEY)
    assert s.user_key.key == VALID_USER_KEY
    assert s.query.get_base_query() == {'query': {'where': VALID_WHERE_STATEMENT}}


def test_create_snapshot_user_stats():
    s = Snapshot(query=VALID_WHERE_STATEMENT, user_stats=True)
    assert len(s.user_key.account_name) > 0
    assert s.user_key.max_allowed_extracted_documents != 0
    assert s.query.get_base_query() == {'query': {'where': VALID_WHERE_STATEMENT}}


def test_create_snapshot_parameter_user_stats():
    s = Snapshot(query=VALID_WHERE_STATEMENT, user_key=VALID_USER_KEY, user_stats=True)
    assert s.user_key.key == VALID_USER_KEY
    assert len(s.user_key.account_name) > 0
    assert s.user_key.max_allowed_extracted_documents != 0
    assert s.query.get_base_query() == {'query': {'where': VALID_WHERE_STATEMENT}}


def test_create_snapshot_existing_user():
    s = Snapshot(query=VALID_WHERE_STATEMENT, user_key=aku)
    assert s.user_key.key == VALID_USER_KEY
    assert s.query.get_base_query() == {'query': {'where': VALID_WHERE_STATEMENT}}


# Query-Object test
def test_create_snapshot_query_parameter():
    q = SnapshotQuery(VALID_WHERE_STATEMENT)
    s = Snapshot(query=q)
    assert s.user_key.key == ENVIRONMENT_USER_KEY
    assert s.query.get_base_query() == {'query': {'where': VALID_WHERE_STATEMENT}}


# Existing Snapshot
def test_existing_snapshot():
    s = Snapshot(snapshot_id=VALID_SNAPSHOT_ID)
    assert s.last_extraction_job.job_id.endswith(VALID_SNAPSHOT_ID)
    assert s.last_extraction_job.job_state == common.API_JOB_DONE_STATE
    assert len(s.last_extraction_job.files) != 0


# Exception Tests
def test_query_snapshot_id_parameters():
    with pytest.raises(Exception, match=r'The query and snapshot_id parameters cannot be set simultaneously'):
        Snapshot(snapshot_id=VALID_SNAPSHOT_ID, query=VALID_WHERE_STATEMENT)


def test_query_value_error():
    with pytest.raises(ValueError, match=r'Unexpected value for the query-where clause'):
        Snapshot(query=[VALID_WHERE_STATEMENT])
