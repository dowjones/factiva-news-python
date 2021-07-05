import pytest

from factiva.core import APIKeyUser, const
from factiva.helper import load_environment_value
from factiva.news.snapshot import Snapshot, SnapshotQuery

# [MB] This file needs to test mainly constructor outputs. Specific
# operations are tested in separate files.

# TEST DATA. Fill out these values with valid data
# Environment values are loaded for automated testing.
ENVIRONMENT_USER_KEY = load_environment_value('FACTIVA_APIKEY')
VALID_USER_KEY = load_environment_value('FACTIVA_APIKEY')
VALID_SNAPSHOT_ID = load_environment_value('FACTIVA_SNAPSHOTID')
VALID_WHERE_STATEMENT = "publication_datetime >= '2021-01-01'"

# Dummy key to test data flow without making requests to API
aku = APIKeyUser(VALID_USER_KEY)

# USER-CENTRIC tests
def test_create_snapshot_evironment_variable():
    s = Snapshot(query=VALID_WHERE_STATEMENT)
    assert s.api_user.api_key == ENVIRONMENT_USER_KEY
    assert s.query.get_base_query() == {'query': {'where': VALID_WHERE_STATEMENT}}

def test_create_snapshot_str_key():
    s = Snapshot(query=VALID_WHERE_STATEMENT, api_user=VALID_USER_KEY)
    assert s.api_user.api_key == VALID_USER_KEY
    assert s.query.get_base_query() == {'query': {'where': VALID_WHERE_STATEMENT}}

def test_create_snapshot_request_info():
    s = Snapshot(query=VALID_WHERE_STATEMENT, request_userinfo=True)
    assert len(s.api_user.account_name) > 0
    assert s.api_user.max_allowed_extracted_documents != 0
    assert s.query.get_base_query() == {'query': {'where': VALID_WHERE_STATEMENT}}

def test_create_snapshot_parameter_request_info():
    s = Snapshot(query=VALID_WHERE_STATEMENT, api_user=VALID_USER_KEY, request_userinfo=True)
    assert s.api_user.api_key == VALID_USER_KEY
    assert len(s.api_user.account_name) > 0
    assert s.api_user.max_allowed_extracted_documents != 0
    assert s.query.get_base_query() == {'query': {'where': VALID_WHERE_STATEMENT}}

def test_create_snapshot_existing_user():
    s = Snapshot(query=VALID_WHERE_STATEMENT, api_user=aku)  
    assert s.api_user.api_key == VALID_USER_KEY
    assert s.query.get_base_query() == {'query': {'where': VALID_WHERE_STATEMENT}}

# Query-Object test
def test_create_snapshot_query_parameter():
    q = SnapshotQuery(VALID_WHERE_STATEMENT)
    s = Snapshot(query=q)
    assert s.api_user.api_key == ENVIRONMENT_USER_KEY
    assert s.query.get_base_query() == {'query': {'where': VALID_WHERE_STATEMENT}}

# Existing Snapshot
def test_existing_snapshot():
    s = Snapshot(snapshot_id=VALID_SNAPSHOT_ID)
    assert s.last_extraction_job.job_id.endswith(VALID_SNAPSHOT_ID)
    assert s.last_extraction_job.job_state == const.API_JOB_DONE_STATE
    assert len(s.last_extraction_job.files) != 0 

# Exception Tests
def test_query_snapshot_id_parameters():
    with pytest.raises(Exception, match=r'The query and snapshot_id parameters cannot be set simultaneously'):
        s = Snapshot(snapshot_id=VALID_SNAPSHOT_ID, query=VALID_WHERE_STATEMENT)

def test_query_value_error():
    with pytest.raises(ValueError, match=r'Unexpected value for the query-where clause'):
        s = Snapshot(query=[VALID_WHERE_STATEMENT])
