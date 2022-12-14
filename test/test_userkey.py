"""
    Tests for the UserKey class
"""
import pytest
from factiva.analytics import UserKey
from factiva.analytics.common.tools import load_environment_value

FACTIVA_USERKEY = load_environment_value("FACTIVA_USERKEY")
DUMMY_KEY = 'abcd1234abcd1234abcd1234abcd1234'

# API Response sample with the most complete set of attributes
# {
#     "data": {
#         "id": "abcd1234abcd1234abcd1234abcd1234",
#         "attributes": {
#             "cnt_curr_ext": 1,
#             "current_downloaded_amount": 427567508,
#             "max_allowed_concurrent_extracts": 10,
#             "max_allowed_document_extracts": 2500000,
#             "max_allowed_extracts": 5,
#             "name": "Company Corp",
#             "products": "DNA",
#             "tot_document_extracts": 1595383,
#             "tot_extracts": 4,
#             "tot_subscriptions": 0,
#             "tot_topics": 0,
#             "licensed_company_ids": [
#                 4,
#                 3,
#                 1,
#                 5
#             ],
#             "enabled_company_identifiers": [
#                 {
#                     "id": 4,
#                     "name": "isin"
#                 },
#                 {
#                     "id": 3,
#                     "name": "cusip"
#                 },
#                 {
#                     "id": 1,
#                     "name": "sedol"
#                 },
#                 {
#                     "id": 5,
#                     "name": "ticker_exchange"
#                 }
#             ]
#         },
#         "type": "account_with_contract_limits"
#     }
# }


def check_userkey_types(usr):
    """"
    Checks the correct types were returned.
    """
    usr = UserKey(stats=True)
    assert isinstance(usr.key, str)
    assert isinstance(usr.cloud_token, dict)
    assert isinstance(usr.account_name, str)
    assert isinstance(usr.active_products, str)
    assert isinstance(usr.max_allowed_concurrent_extractions, int)
    assert isinstance(usr.max_allowed_extracted_documents, int)
    assert isinstance(usr.max_allowed_extractions, int)
    assert isinstance(usr.remaining_documents, int)
    assert isinstance(usr.remaining_extractions, int)
    assert isinstance(usr.total_downloaded_bytes, int)
    assert isinstance(usr.total_extracted_documents, int)
    assert isinstance(usr.total_extractions, int)
    assert isinstance(usr.total_stream_instances, int)
    assert isinstance(usr.total_stream_subscriptions, int)
    assert isinstance(usr.enabled_company_identifiers, list)


def test_userkey_with_stats():
    """"
    Creates the object using the ENV variable and request the usage details to the API service
    """
    aku = UserKey(stats=True)
    check_userkey_types(aku)
    assert aku.key == FACTIVA_USERKEY
    assert len(aku.account_name) > 0
    assert len(aku.active_products) > 0
    assert aku.max_allowed_concurrent_extractions >= 0
    assert aku.max_allowed_extracted_documents >= 0
    assert aku.max_allowed_extractions >= 0
    assert aku.total_downloaded_bytes >= 0
    assert aku.total_extracted_documents >= 0
    assert aku.total_extractions >= 0
    assert aku.total_stream_instances >= 0
    assert aku.total_stream_subscriptions >= 0
    assert len(aku.enabled_company_identifiers) > 0


def test_userkey_without_stats():
    """
    Creates an empty object from the ENV variable with a value only for the key property
    """
    aku = UserKey()
    check_userkey_types(aku)
    assert aku.key == FACTIVA_USERKEY
    assert len(aku.account_name) == 0
    assert len(aku.active_products) == 0
    assert aku.max_allowed_concurrent_extractions == 0
    assert aku.max_allowed_extracted_documents == 0
    assert aku.max_allowed_extractions == 0
    assert aku.total_downloaded_bytes == 0
    assert aku.total_extracted_documents == 0
    assert aku.total_extractions == 0
    assert aku.total_stream_instances == 0
    assert aku.total_stream_subscriptions == 0
    assert len(aku.enabled_company_identifiers) == 0


def test_user_with_parameter_and_stats():
    """
    API Key is passed as a string and stats=True
    """
    aku = UserKey(key=FACTIVA_USERKEY, stats=True)
    check_userkey_types(aku)
    assert aku.key == FACTIVA_USERKEY
    assert len(aku.account_name) > 0
    assert len(aku.active_products) > 0
    assert aku.max_allowed_concurrent_extractions >= 0
    assert aku.max_allowed_extracted_documents >= 0
    assert aku.max_allowed_extractions >= 0
    assert aku.total_downloaded_bytes >= 0
    assert aku.total_extracted_documents >= 0
    assert aku.total_extractions >= 0
    assert aku.total_stream_instances >= 0
    assert aku.total_stream_subscriptions >= 0
    assert len(aku.enabled_company_identifiers) > 0


def test_user_with_parameter_without_stats():
    """
    Creates an empty object from the provided string with a value only for the key property
    """
    usr = UserKey(DUMMY_KEY)
    check_userkey_types(usr)
    assert usr.key == DUMMY_KEY
    assert usr.account_name == ''
    assert usr.active_products == ''
    assert usr.max_allowed_concurrent_extractions == 0
    assert usr.max_allowed_extracted_documents == 0
    assert usr.max_allowed_extractions == 0
    assert usr.total_downloaded_bytes == 0
    assert usr.total_extracted_documents == 0
    assert usr.total_extractions == 0
    assert usr.total_stream_instances == 0
    assert usr.total_stream_subscriptions == 0
    assert len(usr.enabled_company_identifiers) == 0


def test_invalid_key():
    """
    Creates an object from the provided string and request the usage details to the API service
    The key is invalid and this should validate how the error is processed
    """
    with pytest.raises(ValueError, match=r'Factiva User-Key does not exist or inactive.'):
        usr = UserKey(DUMMY_KEY, stats=True)
        assert usr.account_name != ''


def test_invald_lenght_key():
    """
    Attempts to create an object with malformed keys. This requires assert the raised exception.
    """
    with pytest.raises(ValueError, match=r'Factiva User-Key has the wrong length'):
        usr = UserKey('abc')
        assert usr.account_name != ''
