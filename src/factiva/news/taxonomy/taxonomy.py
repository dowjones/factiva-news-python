"""Taxonomy class implementation."""
from io import StringIO

import pandas as pd
from factiva.core import UserKey, const, req
from factiva.core.tools import validate_type


class Taxonomy():
    """Class that represents the taxonomy available within the Snapshots API.

    Parameters
    ----------
    user_key : str or UserKey
        String containing the 32-character long APi Key. If not provided, the
        constructor will try to obtain its value from the FACTIVA_USERKEY
        environment variable.
    user_stats : boolean, optional (Default: False)
        Indicates if user statistics have to be pulled from the API. This operation
        fills account detail properties along with maximum, used and remaining
        values. It may take several seconds to complete.

    Examples
    --------
    Creating a taxonomy instance providing the user key
        >>> t = Taxonomy(u='abcd1234abcd1234abcd1234abcd1234')

    Creating a taxonomy instance with an existing UserKey instance
        >>> u = UserKey('abcd1234abcd1234abcd1234abcd1234')
        >>> t = Taxonomy(user_key=u)

    """

    categories = []

    def __init__(self, user_key=None, user_stats=False):
        """Class initializer."""
        self.user_key = UserKey.create_user_key(user_key, user_stats)
        self.categories = self.get_categories()

    def get_categories(self):
        """Request for a list of available taxonomy categories.

        Returns
        -------
        List of available taxonomy categories.

        Raises
        ------
        RuntimeError: When API request returns unexpected error

        Examples
        --------
        This method is called with in the __init__ method, so the categories can be accessed as is.
            >>> taxonomy = Taxonomy()
            >>> print(taxonomy.categories)
            ['news_subjects', 'regions', 'companies', 'industries', 'executives']

        Calling the method on its own
            >>> taxonomy = Taxonomy()
            >>> print(taxonomy.get_categories())
            ['news_subjects', 'regions', 'companies', 'industries', 'executives']

        """
        headers_dict = {
            'user-key': self.user_key.key
        }

        endpoint = f'{const.API_HOST}{const.API_SNAPSHOTS_TAXONOMY_BASEPATH}'

        response = req.api_send_request(method='GET', endpoint_url=endpoint, headers=headers_dict)

        if response.status_code == 200:
            return [entry['attributes']['name'] for entry in response.json()['data']]

        raise RuntimeError('API Request returned an unexpected HTTP status')

    def get_category_codes(self, category):
        """Request for available codes in the taxonomy for the specified category.

        Parameters
        ----------
        category : str
            String with the name of the taxonomy category to request the codes from

        Returns
        -------
        Dataframe containing the codes for the specified category

        Raises
        ------
        ValueError: When category is not of a valid type
        RuntimeError: When API request returns unexpected error

        Examples
        --------
        Getting the codes for the 'industries' category
            >>> taxonomy = Taxonomy()
            >>> industry_codes = taxonomy.get_category_codes('industries')
            >>> print(industry_codes)
                    code                description
            0     i25121             Petrochemicals
            1     i14001         Petroleum Refining
            2       i257            Pharmaceuticals
            3     iphrws  Pharmaceuticals Wholesale
            4       i643     Pharmacies/Drug Stores

        """
        validate_type(category, str, 'Unexpected value: category value must be string')

        response_format = 'csv'

        headers_dict = {
            'user-key': self.user_key.key
        }

        endpoint = f'{const.API_HOST}{const.API_SNAPSHOTS_TAXONOMY_BASEPATH}/{category}/{response_format}'

        response = req.api_send_request(method='GET', endpoint_url=endpoint, headers=headers_dict)

        if response.status_code == 200:
            return pd.read_csv(StringIO(response.content.decode()))

        raise RuntimeError('API Request returned an unexpected HTTP Status')

    def get_single_company(self, code_type, company_code):
        """Request information about a single company.

        Parameters
        ----------
        code_type : str
            String describing the code type used to request the information about the company. E.g. isin, ticker.
        company_code : str
            String containing the company code

        Returns
        -------
        DataFrame containing the company information

        Raises
        ------
        RuntimeError: When API request returns unexpected error

        Examples
        --------
        Get the company data using the code type 'isin' and the company code 'ABCNMST00394'
            >>> taxonomy = Taxonomy()
            >>> company_data = taxonomy.get_single_company('isin', 'ABCNMST00394')
            >>> print(company_data)
                         id  fcode           common_name
            0  ABCNMST00394  ABCYT  Systemy Company S.A.

        """
        validate_type(code_type, str, 'Unexpected value: code_type must be str')
        validate_type(company_code, str, 'Unexpected value: company must be str')

        headers_dict = {
            'user-key': self.user_key.key
        }

        endpoint = f'{const.API_HOST}{const.API_SNAPSHOTS_COMPANIES_BASEPATH}/{code_type}/{company_code}'

        response = req.api_send_request(method='GET', endpoint_url=endpoint, headers=headers_dict)

        if response.status_code == 200:
            response_data = response.json()
            return pd.DataFrame.from_records([response_data['data']['attributes']])

        raise RuntimeError('API Request returned an unexpected HTTP status')

    def get_multiple_companies(self, code_type, company_codes):
        """Request information about a list of companies.

        Parameters
        ----------
        code_type : str
            String describing the code type used to request the information about the company. E.g. isin, ticker.
        companies_codes : list
            List containing the company codes to request information about

        Returns
        -------
        DataFrame containing the company information

        Raises
        ------
        RuntimeError: When API request returns unexpected error

        Examples
        --------
        Get multiple companies data using the code type 'isin' and a company codes list
            >>> taxonomy = Taxonomy()
            >>> companies_data = taxonomy.get_multiple_companies('isin', ['ABC3E53433100', 'XYZ233341067', 'MN943181045'])
            >>> print(companies_data)
                         id   fcode      common_name
            0  ABC3E5343310  MCABST  M**************
            1  XYZ233341067   AXYZC    A************
            2  MN9431810453     MNM     M***********

        """
        validate_type(code_type, str, 'Unexpected value: code_type must be str')
        validate_type(company_codes, list, 'Unexpected value: companies must be list')
        for single_company_code in company_codes:
            validate_type(single_company_code, str, 'Unexpected value: each company in companies must be str')

        headers_dict = {
            'user-key': self.user_key.key
        }

        payload_dict = {
            "data": {
                "attributes": {
                    "ids": company_codes
                }
            }
        }

        endpoint = f'{const.API_HOST}{const.API_SNAPSHOTS_COMPANIES_BASEPATH}/{code_type}'

        response = req.api_send_request(method='POST', endpoint_url=endpoint, headers=headers_dict, payload=payload_dict)

        if response.status_code == 200 or response.status_code == 207:
            response_data = response.json()
            return pd.DataFrame.from_records(response_data['data']['attributes']['successes'])
        raise RuntimeError(f'API Request returned an unexpected HTTP status with message: {response.text}')

    def get_company(self, code_type, company_code=None, companies_codes=None):
        """Request information about either a single company or a list of companies.

        Parameters
        ----------
        code_type : str
            String describing the code type used to request the information about the company. E.g. isin, ticker.
        company_code: str, Optional
            Single company code to request data about. Not compatible with companies_codes.
        companies_codes: List[str], Optional
            List of string that contains the company codes to request data about. Not compatible with company_code.

        Returns
        -------
        Dataframe with the information about the requested company(ies)

        Raises
        ------
        ValueError: When any given argument is not of the expected type
        RuntimeError:
            - When both company and companies arguments are set
            - When API request returns unexpected error

        Examples
        --------
        Get data for a single company using the code type 'isin'
            >>> taxonomy = Taxonomy()
            >>> single_company_data = taxonomy.get_company('isin', company_code='ABCNMST00394')
            >>> print(single_company_data)
                         id  fcode           common_name
            0  ABCNMST00394  ABCYT  Systemy Company S.A.

        Get data for multiple companies sugin the code type 'isin'
            >>> taxonomy = Taxonomy()
            >>> multiple_companies_data = taxonomy.get_company('isin', company_codes=['ABC3E53433100', 'XYZ233341067', 'MN943181045'])
            >>> print(multiple_companies_data)
                         id   fcode      common_name
            0  ABC3E5343310  MCABST  M**************
            1  XYZ233341067   AXYZC    A************
            2  MN9431810453     MNM     M***********

        """
        if company_code is not None and companies_codes is not None:
            raise RuntimeError('company and companies parameters cannot be set simultaneously')

        if company_code is not None:
            return self.get_single_company(code_type, company_code)

        if companies_codes is not None:
            return self.get_multiple_companies(code_type, companies_codes)

        return None
