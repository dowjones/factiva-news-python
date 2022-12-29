"""
    Tests for the UserKey module
"""
import pytest
from factiva.analytics import OAuthUser
from factiva.analytics.common import config

FACTIVA_CLIENTID = config.load_environment_value("FACTIVA_CLIENTID")
FACTIVA_USERNAME = config.load_environment_value("FACTIVA_USERNAME")
FACTIVA_PASSWORD = config.load_environment_value("FACTIVA_PASSWORD")


def _test_oauthuser_types(usr):
    """"
    Checks the correct types were returned.
    """
    assert isinstance(usr.current_jwt_token, str)


def _test_oauthuser_values(usr):
    """
    Checks if values within the expected lengths and ranges
    were returned
    """
    assert len(usr.current_jwt_token.split('.')) == 3


def test_oauthuser_create_from_env():
    """"
    Creates the object using the ENV variables
    """
    usr = OAuthUser()
    _test_oauthuser_types(usr)
    _test_oauthuser_values(usr)


def test_oauthuser_create_from_params():
    """
    Creates an empty object from the passed params
    """
    usr = OAuthUser(client_id=FACTIVA_CLIENTID,
                    username=FACTIVA_USERNAME,
                    password=FACTIVA_PASSWORD)
    _test_oauthuser_types(usr)
    _test_oauthuser_values(usr)


def test_wrong_credentials():
    """
    Creates an object from the provided string
    The key is invalid and this should validate how the error is processed
    """
    with pytest.raises(PermissionError, match=r'Invalid user credentials'):
        o = OAuthUser(client_id='client_id_value',
                  username='username_value',
                  password='password_value')
        o.get_id_token()


def test_invald_param_types():
    """
    Attempts to create an object with wrong parameter types
    """
    with pytest.raises(ValueError, match=r'The client_id param must be a string'):
        OAuthUser({}, 'abc', 'abc')
    with pytest.raises(ValueError, match=r'The username param must be a string'):
        OAuthUser('abc', 44.5, 'abc')
    with pytest.raises(ValueError, match=r'The password param must be a string'):
        OAuthUser('abc', 'abc', 123)
