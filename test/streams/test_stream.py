import time

import pytest
from factiva.analytics import StreamingInstance, UserKey
from factiva.analytics.common import config

# [MB] This file needs to test mainly constructor outputs. Specific
# operations are tested in separate files.

# TEST DATA. Fill out these values with valid data
# Environment values are loaded for automated testing.
ENVIRONMENT_USER_KEY = config.load_environment_value('FACTIVA_USERKEY')
VALID_USER_KEY = config.load_environment_value('FACTIVA_USERKEY')
VALID_STREAM_ID = config.load_environment_value('FACTIVA_STREAMID')
VALID_WHERE_STATEMENT = "publication_datetime >= '2021-04-01 00:00:00' AND LOWER(language_code)='en' AND UPPER(source_code) = 'DJDN'"

# Dummy key to test data flow without making requests to API
user_key = UserKey(VALID_USER_KEY)


# USER-CENTRIC tests
# def test_create_stream_evironment_variable():
#     stream = Stream()
#     assert stream.stream_user.key == ENVIRONMENT_USER_KEY
#     # WLTODO: Delete this stream!, it may need to wait ~30 seconds to ensure it's in RUNNING state
#     #         Also, in this first test, ensure the DELETE Stream operation works fine


# def test_create_stream_str_key():
#     stream = Stream(user_key=VALID_USER_KEY)
#     assert stream.stream_user.key == VALID_USER_KEY
#     # WLTODO: Delete this stream!, it may need to wait ~30 seconds to ensure it's in RUNNING state

# def test_create_stream_request_info():
#     stream = Stream(user_key=VALID_USER_KEY, user_stats=True)
#     assert len(stream.stream_user.account_name) > 0
#     assert stream.stream_user.max_allowed_extracted_documents != 0
#     # WLTODO: Delete this stream!, it may need to wait ~30 seconds to ensure it's in RUNNING state


# def test_create_stream_existing_user():
#     stream = Stream(user_key=user_key)
#     assert stream.stream_user.key == VALID_USER_KEY
#     # WLTODO: Delete this stream!, it may need to wait ~30 seconds to ensure it's in RUNNING state


# def test_create_stream_instance_invalid_params():
#     with pytest.raises(ValueError,
#                        match=r'Not allowed stream id with query or snapshot'):
#         Stream(user_key=VALID_USER_KEY,
#                query=VALID_WHERE_STATEMENT,
#                stream_id=VALID_STREAM_ID)


# def test_create_stream_with_query():
#     s = Stream(query=VALID_WHERE_STATEMENT)
#     assert s.query.get_base_query() == {'query': {'where': VALID_WHERE_STATEMENT}}
#     # WLTODO: Delete this stream!, it may need to wait ~30 seconds to ensure it's in RUNNING state


# def test_stream_get_info_from_id():
#     s = Stream(stream_id=VALID_STREAM_ID)  # ID must be the long ID!
#     response = s.get_info()
#     assert isinstance(response, StreamResponse)
#     assert len(response.id) > 0


# def test_stream_get_all_streams():
#     s = Stream()
#     response = s.get_all_streams()
#     assert isinstance(response, list)
#     assert isinstance(response[0], StreamResponse)


# def test_stream_create_from_query():
#     stream = Stream(query=VALID_WHERE_STATEMENT)
#     stream_response = stream.create()
#     subscription_id = stream.get_suscription_by_index(0)

#     assert isinstance(stream_response, StreamResponse)
#     assert len(stream_response.id) > 0

#     time.sleep(60)
#     delete_suscription = stream.delete_subscription(subscription_id)
#     assert delete_suscription == True

#     time.sleep(15)
#     delete_stream = stream.delete()
#     assert isinstance(delete_stream, StreamResponse)


# def test_stream_create_from_existing_snapshot_id():
#     s = Stream(query=VALID_WHERE_STATEMENT)
#     s.snapshot_id = VALID_STREAM_ID
#     response = s.create()
#     assert isinstance(response, StreamResponse)
#     assert len(response.id) > 0
