import pytest
from factiva.analytics import SnapshotQuery

VALID_WHERE_STATEMENT = "publication_datetime >= '2021-01-01'"


def test_base_query():
    query = SnapshotQuery(VALID_WHERE_STATEMENT)
    assert query.get_base_query() == {'query': {'where': VALID_WHERE_STATEMENT}}


def test_explain_query():
    query = SnapshotQuery(VALID_WHERE_STATEMENT)
    assert query.get_explain_query() == {'query': {'where': VALID_WHERE_STATEMENT}}


def test_analytics_V1_query():
    query = SnapshotQuery(VALID_WHERE_STATEMENT)
    query.group_by_source_code = False
    assert query.get_analytics_query() == {'query': {
        'where': "publication_datetime >= '2021-01-01'",
        'frequency': 'MONTH',
        'date_field': 'publication_datetime',
        'group_by_source_code': False,
        'top': 10
        }
    }

def test_analytics_query_group_conflict():
    query = SnapshotQuery(VALID_WHERE_STATEMENT)
    query.group_dimensions = ["source_code"]
    query.group_by_source_code = True
    with pytest.raises(ValueError, match="The value group_by_source_code and group_dimensions are not compatible each other"):
        query.get_analytics_query()

def test_analytics_query():
    query = SnapshotQuery(VALID_WHERE_STATEMENT)
    assert query.get_analytics_query() == {'query': {
        'where': "publication_datetime >= '2021-01-01'",
        'frequency': 'MONTH',
        'date_field': 'publication_datetime',
        'group_dimensions': [],
        'top': 10
        }
    }

def test_analytics_query_group_dimensions():
    query = SnapshotQuery(VALID_WHERE_STATEMENT)
    query.group_dimensions = ["source_code", "subject_codes","industry_codes", "company_codes"]
    assert query.get_analytics_query() == {'query': {
        'where': "publication_datetime >= '2021-01-01'",
        'frequency': 'MONTH',
        'date_field': 'publication_datetime',
        'group_dimensions':  ["source_code", "subject_codes","industry_codes", "company_codes"],
        'top': 10
        }
    }

def test_analytics_query_excedes_group_dimensions():
    query = SnapshotQuery(VALID_WHERE_STATEMENT)
    query.group_dimensions = ["source_code", "subject_codes", "region_codes", "industry_codes", "company_codes"]
    with pytest.raises(ValueError, match="The maximiun group_dimensions options is 4"):
        query.get_analytics_query()
    

def test_extraction_query():
    query = SnapshotQuery(VALID_WHERE_STATEMENT)
    assert query.get_extraction_query() == {'query': {
        'where': "publication_datetime >= '2021-01-01'",
        'format': 'avro'
        }
    }


def test_invalid_field():
    with pytest.raises(ValueError, match=r'Value txt is not within the allowed options'):
        SnapshotQuery(VALID_WHERE_STATEMENT, file_format='TXT')
