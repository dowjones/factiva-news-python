
import pytest
import time

from factiva.core import UserKey, StreamResponse
from factiva.helper import load_environment_value
from factiva.news.stream import Stream

# [MB] This file needs to test mainly constructor outputs. Specific
# operations are tested in separate files.

# TEST DATA. Fill out these values with valid data
# Environment values are loaded for automated testing.
ENVIRONMENT_USER_KEY = load_environment_value('FACTIVA_USERKEY')
VALID_USER_KEY = load_environment_value('FACTIVA_USERKEY')
VALID_STREAM_ID = load_environment_value('FACTIVA_STREAM_ID')
VALID_WHERE_STATEMENT = "publication_datetime >= '2021-04-01 00:00:00' AND LOWER(language_code)='en' AND UPPER(source_code) = 'DJDN'"

# Dummy key to test data flow without making requests to API
user_key = UserKey(VALID_USER_KEY)


# USER-CENTRIC tests
def test_create_stream_evironment_variable():
    stream = Stream()
    assert stream.user_key.key == ENVIRONMENT_USER_KEY


def test_create_stream_str_key():
    stream = Stream(user_key=VALID_USER_KEY)
    assert stream.user_key.key == VALID_USER_KEY


def test_create_stream_request_info():
    stream = Stream(user_key=VALID_USER_KEY, user_stats=True)
    assert len(stream.user_key.account_name) > 0
    assert stream.user_key.max_allowed_extracted_documents != 0


def test_create_stream_existing_user():
    stream = Stream(user_key=user_key)
    assert stream.user_key.key == VALID_USER_KEY


def test_create_stream_invalid_stream():
    with pytest.raises(ValueError, match=r'Not allowed stream id with query or snapshot'):
        Stream(
            user_key=VALID_USER_KEY,
            query=VALID_WHERE_STATEMENT,
            stream_id=VALID_STREAM_ID
        )


def test_create_stream_with_query():
    s = Stream(query=VALID_WHERE_STATEMENT)
    assert s.query.get_base_query() == {'query': {'where': VALID_WHERE_STATEMENT}}


def test_stream_create_from_query():
    s = Stream(query=VALID_WHERE_STATEMENT)
    response = s.create()
    assert isinstance(response, StreamResponse)
    assert len(response.id) > 0


def test_stream_get_info_from_id():
    s = Stream(stream_id=VALID_STREAM_ID)
    response = s.get_info()
    assert isinstance(response, StreamResponse)
    assert len(response.id) > 0


def test_stream_delete_from_id():
    s = Stream(query=VALID_WHERE_STATEMENT)
    s.create()
    time.sleep(60)
    response = s.delete()
    assert isinstance(response, StreamResponse)
    assert len(response.id) > 0


# def __main__():
#     test_create_stream_invalid_stream()


# __main__()
