import os
import shutil
import unittest

import pytest
from factiva.core import StreamResponse, const
from factiva.core.tools import load_environment_value
from factiva.news import Listener, ListenerTools, Stream

VALID_STREAM_ID = load_environment_value('FACTIVA_STREAMID')
VALID_SUSCRIPTION_ID = load_environment_value('FACTIVA_SUBSCRIPTIONID')
VALID_WHERE_STATEMENT = "publication_datetime >= '2022-01-01 00:00:00' AND LOWER(language_code)='en' AND UPPER(source_code) = 'DJDN'"


class TestStreamListener(unittest.TestCase):

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(const.FILES_DEFAULT_FOLDER):
            shutil.rmtree(const.FILES_DEFAULT_FOLDER)

    def test_get_listener_by_index(self):
        stream = Stream(stream_id=VALID_STREAM_ID)
        subscription = stream.get_suscription_by_index(0)
        listener = subscription.listener
        assert isinstance(listener, Listener)

    def test_get_listener_by_index_fails(self):
        stream = Stream(stream_id=VALID_STREAM_ID)
        with pytest.raises(ValueError):
            subscription = stream.get_suscription_by_index(1000)
            listener = subscription.listener
            assert isinstance(listener, Listener)

    def test_get_listener_by_suscription_id(self):
        stream = Stream(stream_id=VALID_STREAM_ID)
        subscription = stream.get_suscription_by_id(VALID_SUSCRIPTION_ID)
        listener = subscription.listener
        assert isinstance(listener, Listener)

    def test_get_listener_by_suscription_id_fails(self):
        stream = Stream(stream_id=VALID_STREAM_ID)
        with pytest.raises(ValueError):
            subscription = stream.get_suscription_by_id("Dummy-id")
            listener = subscription.listener
            assert isinstance(listener, Listener)

    """ def test_listener(self):
        stream = Stream(stream_id=VALID_STREAM_ID)

        subscription = stream.get_suscription_by_index(0)
        listener = subscription.listener
        listenerTools = ListenerTools()
        listener.listen(listenerTools.save_json_file, 2)

        time.sleep(60)
        delete_suscription = stream.delete_subscription(subscription)
        assert delete_suscription == True

        time.sleep(15)
        delete_stream = stream.delete()
        assert isinstance(delete_stream, StreamResponse) """
