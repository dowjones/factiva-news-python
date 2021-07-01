import pytest

from factiva.core import APIKeyUser, const
from factiva.helper import load_environment_value
from factiva.news.snapshot import Snapshot, ExplainJob

ENVIRONMENT_USER_KEY = load_environment_value('FACTIVA_APIKEY')
VALID_USER_KEY = load_environment_value('FACTIVA_APIKEY')
VALID_SNAPSHOT_ID = load_environment_value('FACTIVA_SNAPSHOTID')
VALID_WHERE_STATEMENT = "publication_datetime >= '2018-01-01 00:00:00' AND publication_datetime <= '2018-01-10 00:00:00' AND LOWER(language_code) = 'en'"
INVALID_WHERE_STATEMENT = "publecation_datetime >= '2018-01-01 00:00:00'" # date field name is misspelled on purpose

# Test successful process explain
def test_process_explain():
    s = Snapshot(query=VALID_WHERE_STATEMENT)
    assert s.api_user.api_key == ENVIRONMENT_USER_KEY
    assert s.query.get_base_query() == {'query': {'where': VALID_WHERE_STATEMENT}}
    s.process_explain()
    assert s.last_explain_job.document_volume > 0
    assert s.last_explain_job.job_state == const.API_JOB_DONE_STATE
    assert len(s.last_explain_job.job_id) == 36

# Test step by step explain

# Test failed explain (wrong SQL syntax)
# With the query "publecation_datetime >= '2018-01-01 00:00:00'"
# Expecting the response:
# {
#     "errors": [
#         {
#             "title": "invalidQuery",
#             "detail": "Unrecognized name: publecation_datetime; Did you mean publication_datetime? at [9:52]"
#         }
#     ]
# }
# The send_request method is triggering a "RuntimeError" when not receiving a 201, but it needs
# to process the message and display it to the end customer (within the exception object), so the
# end-user knows what's going on.
def test_failed_explain():
    s = Snapshot(query=INVALID_WHERE_STATEMENT)
    with pytest.raises(ValueError, match=r'Unrecognized name*'):
        s.process_explain()
