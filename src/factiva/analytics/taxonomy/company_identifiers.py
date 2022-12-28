from ..common import tools
from ..common import req
from .. import (UserKey, factiva_logger, get_factiva_logger)
from ..common import (API_COMPANIES_IDENTIFIER_TYPE, API_HOST,
                                API_SNAPSHOTS_COMPANIES_BASEPATH,
                                API_SNAPSHOTS_COMPANIES_PIT,
                                API_SNAPSHOTS_TAXONOMY_BASEPATH,
                                DOWNLOAD_DEFAULT_FOLDER,
                                TICKER_COMPANY_IDENTIFIER)


class Company():
    """Class that represents the company available within the Snapshots API.
    
    Parameters
    ----------
    user_key : str or UserKey
        String containing the 32-character long APi Key. If not provided, the
        constructor will try to obtain its value from the FACTIVA_USERKEY
        environment variable.
    
    Examples
    --------
    Creating a company instance providing the user key
        >>> c = Company(u='abcd1234abcd1234abcd1234abcd1234')

    Creating a company instance with an existing UserKey instance
        >>> u = UserKey('abcd1234abcd1234abcd1234abcd1234', True)
        >>> c = Company(user_key=u)
    """

    __API_ENDPOINT_TAXONOMY = f'{API_HOST}{API_SNAPSHOTS_TAXONOMY_BASEPATH}'
    __API_ENDPOINT_COMPANY = f'{API_HOST}{API_SNAPSHOTS_COMPANIES_BASEPATH}'
    __TICKER_COMPANY_IDENTIFIER_NAME = 'ticker_exchange'

    user_key=None
    
    def __init__(self, user_key=None):
        """Class initializar"""
        self.user_key = UserKey(user_key, True)
        self.log= get_factiva_logger()


    @factiva_logger
    def get_identifiers(self) -> list:
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

        endpoint = f'{common.API_HOST}{common.API_SNAPSHOTS_COMPANY_IDENTIFIERS_BASEPATH}'

        response = req.api_send_request(method='GET', endpoint_url=endpoint, headers=headers_dict)

        if response.status_code == 200:
            return response.json()['data']['attributes']

        raise RuntimeError('API Request returned an unexpected HTTP status')


    @factiva_logger
    def validate_point_time_request(self, identifier):
        """Validate if the user is allowes to perform company operation and if the identifier given is valid
        
        Parameters
        ----------
        identifier : str
            A company identifier type
        
        Raises
        ------
        ValueError: When the user is not allowed to permorm this operation
        ValueError: When the identifier requested is not valid
        """

        if (not len(self.user_key.enabled_company_identifiers)):
            raise ValueError('User is not allowed to perform this operation')

        tools.validate_field_options(identifier, API_COMPANIES_IDENTIFIER_TYPE)

        if (identifier == TICKER_COMPANY_IDENTIFIER):
            identifier = self.__TICKER_COMPANY_IDENTIFIER_NAME

        identifier_description = list(
            filter(lambda company: company['name'] == identifier,
                   self.user_key.enabled_company_identifiers))
        if (not len(identifier_description)):
            raise ValueError('User is not allowed to perform this operation')

    @factiva_logger
    def point_in_time_download_all(self,
                                   identifier,
                                   file_name,
                                   file_format,
                                   to_save_path=None,
                                   add_timestamp=False) -> str:
        """Returns a file with the historical and current identifiers for each category and news coded companies.

        Parameters
        ----------
        identifier : str
            A company identifier type
        file_name : str
            Name to be used as local filename
        file_format : str
            Format of the file
        to_save_path : str, optional
            Path to be used to store the file
        add_timestamp : bool, optional
            Flag to determine if include timestamp info at the filename

        Returns
        -------
        str:
            Dowloaded file path

        Raises
        ------
        ValueError: When the user is not allowed to permorm this operation
        ValueError: When the identifier requested is not valid
        ValueError: When the format file requested is not valid
        """

        self.validate_point_time_request(identifier)
        if (to_save_path is None):
            to_save_path = DOWNLOAD_DEFAULT_FOLDER

        headers_dict = {'user-key': self.user_key.key}
        endpoint = f'{self.__API_ENDPOINT_TAXONOMY}{API_SNAPSHOTS_COMPANIES_PIT}/{identifier}/{file_format}'

        local_file_name = req.download_file(endpoint, headers_dict, file_name,
                                            file_format, to_save_path,
                                            add_timestamp)
        return local_file_name

    @factiva_logger
    def point_in_time_query(self, identifier, value) -> dict:
        """Returns the resolved Factiva code and date ranges when the instrument from the identifier, was valid.
        
        Parameters
        ----------
        identifier : str
            A company identifier type
        value : str
            Identifier value
        
        Returns
        -------
        dict:
            Factiva code and date ranges from a company
        
        Raises
        ------
        ValueError: When the user is not allowed to permorm this operation
        ValueError: When the identifier requested is not valid
        """

        self.validate_point_time_request(identifier)
        headers_dict = {'user-key': self.user_key.key}
        endpoint = f'{self.__API_ENDPOINT_COMPANY}{API_SNAPSHOTS_COMPANIES_PIT}/{identifier}/{value}'

        response = req.api_send_request(endpoint_url=endpoint,
                                        headers=headers_dict)
        if response.status_code == 200:
            response = response.json()
            return response
        else:
            raise RuntimeError(
                '''Unexpected HTTP Response from API while checking for limits'''
            )

    @log.factiva_logger
    def get_single_company(self, code_type, company_code) -> pd.DataFrame:
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
        tools.validate_type(code_type, str, 'Unexpected value: code_type must be str')
        tools.validate_type(company_code, str, 'Unexpected value: company must be str')

        headers_dict = {
            'user-key': self.user_key.key
        }

        endpoint = f'{common.API_HOST}{common.API_SNAPSHOTS_COMPANIES_BASEPATH}/{code_type}/{company_code}'

        response = req.api_send_request(method='GET', endpoint_url=endpoint, headers=headers_dict)

        if response.status_code == 200:
            response_data = response.json()
            return pd.DataFrame.from_records([response_data['data']['attributes']])

        raise RuntimeError('API Request returned an unexpected HTTP status')

    @log.factiva_logger
    def get_multiple_companies(self, code_type, company_codes) -> pd.DataFrame:
        """
        Request information about a list of companies.

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
        tools.validate_type(code_type, str, 'Unexpected value: code_type must be str')
        tools.validate_type(company_codes, list, 'Unexpected value: companies must be list')
        for single_company_code in company_codes:
            tools.validate_type(single_company_code, str, 'Unexpected value: each company in companies must be str')

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

        endpoint = f'{common.API_HOST}{common.API_SNAPSHOTS_COMPANIES_BASEPATH}/{code_type}'

        response = req.api_send_request(method='POST', endpoint_url=endpoint, headers=headers_dict, payload=payload_dict)

        if response.status_code == 200 or response.status_code == 207:
            response_data = response.json()
            return pd.DataFrame.from_records(response_data['data']['attributes']['successes'])
        raise RuntimeError(f'API Request returned an unexpected HTTP status with message: {response.text}')

    @log.factiva_logger
    def get_company(self, code_type, company_codes) -> pd.DataFrame:
        """Request information about either a single company or a list of companies.

        Parameters
        ----------
        code_type : str
            String describing the code type used to request the information about the company. E.g. isin, ticker.
        company_code: str or list
            Single company code (str) or list of company codes to translate.

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
        if type(company_codes) is str:
            return self.get_single_company(code_type, company_codes)
        elif type(company_codes) is list:
            return self.get_multiple_companies(code_type, company_codes)
        else:
            raise ValueError('company_codes must be a string or a list')