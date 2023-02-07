import os
import pandas as pd
from factiva.analytics import FactivaTaxonomy, FactivaTaxonomyCategories, UserKey
from factiva.analytics.common import config

FACTIVA_USERKEY = config.load_environment_value("FACTIVA_USERKEY")
SAVE_PATH = os.getcwd()

def test_create_taxonomy_instance_env_user():
    t = FactivaTaxonomy()
    assert t.user_key.key == FACTIVA_USERKEY


def test_create_taxonomy_instance_str_user():
    t = FactivaTaxonomy(user_key=FACTIVA_USERKEY)
    assert t.user_key.key == FACTIVA_USERKEY


def test_create_taxonomy_instance_userkey_user():
    u = UserKey()
    t = FactivaTaxonomy(user_key=u)
    assert t.user_key.key == FACTIVA_USERKEY


def test_download_category_file():
    t = FactivaTaxonomy()
    assert t.download_raw_category(FactivaTaxonomyCategories.INDUSTRIES, path=SAVE_PATH)
    assert t.download_raw_category(FactivaTaxonomyCategories.INDUSTRIES, path=SAVE_PATH, file_format='avro')
    try:
        os.remove(f'{SAVE_PATH}/{FactivaTaxonomyCategories.INDUSTRIES.value}.csv')
        os.remove(f'{SAVE_PATH}/{FactivaTaxonomyCategories.INDUSTRIES.value}.avro')
    except:
        pass


def test_get_category_codes():
    t = FactivaTaxonomy()
    industries = t.get_category_codes(FactivaTaxonomyCategories.INDUSTRIES)
    assert isinstance(industries, pd.DataFrame)
    assert isinstance(t.all_industries, pd.DataFrame)
    assert 'I0' in t.all_industries.index
    assert 'I16' in t.all_industries.index


def test_lookup_code_good():
    t = FactivaTaxonomy()
    assert t.all_subjects == None
    mcat = t.lookup_code('MCAT', FactivaTaxonomyCategories.SUBJECTS)
    assert isinstance(t.all_subjects, pd.DataFrame)
    assert 'code' in mcat.keys()
    assert 'descriptor' in mcat.keys()
    assert mcat['code'] == 'MCAT'
