import time

import pytest
from factiva.core.tools import load_environment_value
from factiva.news import Listener, Stream, StreamResponse

VALID_STREAM_ID = load_environment_value('FACTIVA_STREAMID')
VALID_SUSCRIPTION_ID = load_environment_value('FACTIVA_SUBSCRIPTION_ID')
VALID_WHERE_STATEMENT = "publication_datetime >= '2022-01-01 00:00:00' AND LOWER(language_code)='en' AND UPPER(source_code) = 'DJDN'"


def handle_message(message, subscription_id):
    print(subscription_id)
    print(message)


def test_get_listener_by_index():
    stream = Stream(stream_id=VALID_STREAM_ID)
    subscription = stream.get_suscription_by_index(0)
    listener = subscription.listener
    assert isinstance(listener, Listener)


def test_get_listener_by_index_fails():
    stream = Stream(stream_id=VALID_STREAM_ID)
    with pytest.raises(ValueError):
        subscription = stream.get_suscription_by_index(1000)
        listener = subscription.listener
        assert isinstance(listener, Listener)


def test_get_listener_by_suscription_id():
    stream = Stream(stream_id=VALID_STREAM_ID)
    subscription = stream.get_suscription_by_id(VALID_SUSCRIPTION_ID)
    listener = subscription.listener
    assert isinstance(listener, Listener)


def test_get_listener_by_suscription_id_fails():
    stream = Stream(stream_id=VALID_STREAM_ID)
    with pytest.raises(ValueError):
        subscription = stream.get_suscription_by_id("Dummy-id")
        listener = subscription.listener
        assert isinstance(listener, Listener)


""" def test_listener():
    stream = Stream(query=VALID_WHERE_STATEMENT)
    stream_response = stream.create()
    assert isinstance(stream_response, StreamResponse)
    assert len(stream_response.id) > 0

    subscription = stream.get_suscription_by_index(0)
    listener = subscription.listener
    listener.listen(handle_message, 10)

    time.sleep(60)
    delete_suscription = stream.delete_subscription(subscription)
    assert delete_suscription == True

    time.sleep(15)
    delete_stream = stream.delete()
    assert isinstance(delete_stream, StreamResponse) """
