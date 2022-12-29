"""
    Tests for the ArticleRetrieval module
"""
import pytest
from factiva.analytics import OAuthUser, ArticleRetrieval, UIArticle
from factiva.analytics.common import config

FACTIVA_CLIENTID = config.load_environment_value("FACTIVA_CLIENTID")
FACTIVA_USERNAME = config.load_environment_value("FACTIVA_USERNAME")
FACTIVA_PASSWORD = config.load_environment_value("FACTIVA_PASSWORD")
ARTICLE_ID = 'WSJO000020221229eict000jh'

def _assert_uiarticle_values(uiarticle: UIArticle):
    assert isinstance(uiarticle, UIArticle)
    assert isinstance(uiarticle.an, str)
    assert isinstance(uiarticle.headline, str)
    assert isinstance(uiarticle.source_code, str)
    assert isinstance(uiarticle.source_name, str)
    assert isinstance(uiarticle.metadata, dict)
    assert isinstance(uiarticle.content, dict)
    assert isinstance(uiarticle.included, list)
    assert isinstance(uiarticle.relationships, dict)
    assert uiarticle.an == ARTICLE_ID
    assert 'headline' in uiarticle.content.keys()
    assert uiarticle.source_code == 'WSJO'
    assert uiarticle.source_name == 'The Wall Street Journal Online'


def test_article_retrieval_env_user():
    """"
    Creates the object using the ENV variable and request the usage details to the API service
    """
    ar = ArticleRetrieval()
    article = ar.retrieve_single_article(ARTICLE_ID)
    _assert_uiarticle_values(article)


def test_article_retrieval_params_user():
    o = OAuthUser(client_id=FACTIVA_CLIENTID,
                  username=FACTIVA_USERNAME,
                  password=FACTIVA_PASSWORD)
    ar = ArticleRetrieval(oauth_user=o)
    article = ar.retrieve_single_article(ARTICLE_ID)
    _assert_uiarticle_values(article)


