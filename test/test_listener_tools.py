import datetime
import os
import shutil
import unittest

from factiva.core import const
from factiva.news import ListenerTools

BASIC_ADD_MESSAGE = {
    "an": "DJDN000020220222ei2m003yf",
    "action": "add",
    "source_code": "DJDN",
    "source_name": "Dow Jones Institutional News",
}

BASIC_ERROR_MESSAGE = {
    "an": "DJDN000020220222ei2m003yf",
    "action": "dummy_action",
    "source_code": "loremIpsum",
    "source_name": "loremIpsum",
}

BASIC_SUBSCRIPTION_ID = "aaaa-bbbb-cccc-dddd-eeeee"


class TestListenerTools(unittest.TestCase):

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(const.LISTENER_FILES_DEFAULT_FOLDER):
            shutil.rmtree(const.LISTENER_FILES_DEFAULT_FOLDER)

    def test_write_json_add_action(self):
        listenerTools = ListenerTools()
        result = listenerTools.save_json_file(BASIC_ADD_MESSAGE,
                                              BASIC_SUBSCRIPTION_ID)
        stream_short_id = BASIC_SUBSCRIPTION_ID.split('-')[-3]
        action = BASIC_ADD_MESSAGE['action']
        current_hour = datetime.datetime.utcnow().strftime('%Y%m%d%H')
        file_result = f'{stream_short_id}_{action}_{current_hour}.jsonl'
        assert result == True
        assert os.path.exists(
            os.path.join(const.LISTENER_FILES_DEFAULT_FOLDER, file_result)) == True

    def test_write_json_undefined_action(self):
        listenerTools = ListenerTools()
        result = listenerTools.save_json_file(BASIC_ERROR_MESSAGE,
                                              BASIC_SUBSCRIPTION_ID)
        file_result = os.path.join(const.LISTENER_FILES_DEFAULT_FOLDER, 'errors.log')

        assert result == True
        assert os.path.exists(file_result) == True
