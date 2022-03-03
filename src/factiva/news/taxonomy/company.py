from factiva.core import (UserKey, factiva_logger, get_factiva_logger, req,
                          tools)
from factiva.core.const import (API_COMPANIES_IDENTIFIER_TYPE, API_HOST,
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
        self.user_key = UserKey.create_user_key(user_key, True)
        self.log= get_factiva_logger()

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
