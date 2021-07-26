import pytest
from factiva.news.snapshot import SnapshotQuery

VALID_WHERE_STATEMENT = "publication_datetime >= '2021-01-01'"


def test_base_query():
    query = SnapshotQuery(VALID_WHERE_STATEMENT)
    assert query.get_base_query() == {'query': {'where': VALID_WHERE_STATEMENT}}


def test_explain_query():
    query = SnapshotQuery(VALID_WHERE_STATEMENT)
    assert query.get_explain_query() == {'query': {'where': VALID_WHERE_STATEMENT}}


def test_analytics_query():
    query = SnapshotQuery(VALID_WHERE_STATEMENT)
    assert query.get_analytics_query() == {'query': {
        'where': "publication_datetime >= '2021-01-01'",
        'frequency': 'MONTH',
        'date_field': 'publication_datetime',
        'group_by_source_code': False,
        'top': 10
        }
    }


def test_extraction_query():
    query = SnapshotQuery(VALID_WHERE_STATEMENT)
    print(query.get_extraction_query())
    assert query.get_extraction_query() == {'query': {
        'where': "publication_datetime >= '2021-01-01'",
        'format': 'avro'
        }
    }


def test_invalid_field():
    with pytest.raises(ValueError, match=r'Value of file_format is not within the allowed options'):
        SnapshotQuery(VALID_WHERE_STATEMENT, file_format='TXT')
