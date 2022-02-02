from factiva.news import Taxonomy


def test_create_taxonomy_instance():
    taxonomy = Taxonomy()
    assert len(taxonomy.categories) > 0
    assert 'industries' in taxonomy.categories


def test_get_identifiers_for_category():
    taxonomy = Taxonomy()
    industry_codes = taxonomy.get_category_codes('industries')
    assert len(industry_codes) > 0
    assert industry_codes.loc['i25121'] is not None


def test_get_identifiers_for_category_big_files():
    taxonomy = Taxonomy()
    industry_codes = taxonomy.get_category_codes('companies')
    print(industry_codes)
    assert len(industry_codes) > 0
    assert industry_codes.loc['SSYRVO'] is not None

def test_request_data_for_company():
    taxonomy = Taxonomy()
    company_data = taxonomy.get_company('isin', company_codes='PLUNMST00014')
    assert len(company_data) > 0
    assert company_data[company_data['id'] == 'PLUNMST00014'] is not None


def test_request_data_for_multiple_companies():
    taxonomy = Taxonomy()
    companies = ['US0378331005', 'US0231351067', 'US5949181045']
    companies_data = taxonomy.get_company('isin', company_codes=companies)
    assert len(companies_data) == 3
    for company in companies:
        assert companies_data[companies_data['id'] == company] is not None
