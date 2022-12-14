"""
    Unit tests for the UseKey class
"""
import unittest
from factiva.analytics import UserKey
from factiva.analytics.common.tools import load_environment_value

FACTIVA_USERKEY = load_environment_value("FACTIVA_USERKEY")
DUMMY_KEY = 'abcd1234abcd1234abcd1234abcd1234'


class TestUserKey(unittest.TestCase):
    """
        Class for the UserKey unit tests
    """

    def test_create_user_with_env_key(self):
        """
        Tests a UserKey instance creation from a valid key
        """
        usr = UserKey(stats=True)
        self.assertEqual(usr.key, FACTIVA_USERKEY)


    def test_create_user_with_key_dummy(self):
        """
        Tests a UserKey instance creation from a dummy key.
        No API request is sent, so, no error is expected.
        """
        usr = UserKey(key=DUMMY_KEY)
        self.assertNotEqual(usr.key, FACTIVA_USERKEY)


    def test_create_user_with_invalid_key(self):
        """
        Tests a UserKey instance creation from a key with the wrong length
        """
        with self.assertRaises(ValueError):
            UserKey(key='aabbcc')

    def test_fetch_cloud_token(self):
        """
        Tests a UserKey instance creation from a valid key (env), and
        checks the returned cloud token is valid.
        """
        usr = UserKey()
        usr.get_cloud_token()
        self.assertNotEqual(usr.cloud_token, {})
