"""FactivaTaxonomy Module."""
import os
import pandas as pd
from io import StringIO
from enum import Enum
from ..common import req
from ..common import log
from ..common import tools
from ..common import const
from ..auth import UserKey


class FactivaTaxonomyCategories(Enum):
    """
    Class that provides a unique way to reference the different taxonomy
    cateories present in Factiva data.

        >>> FactivaTaxonomyCategories.SUBJECTS
        >>> FactivaTaxonomyCategories.REGIONS
        >>> FactivaTaxonomyCategories.COMPANIES
        >>> FactivaTaxonomyCategories.INDUSTRIES
        >>> FactivaTaxonomyCategories.EXECUTIVES
    """
    SUBJECTS = 'news_subjects'
    REGIONS = 'regions'
    COMPANIES = 'companies'
    INDUSTRIES = 'industries'
    EXECUTIVES = 'executives'


class FactivaTaxonomy():
    """
    Class that represents the Factiva Taxonomy endpoints in Factiva
    Analytics.

    Parameters
    ----------
    user_key : str or UserKey
        String containing the 32-character long APi Key or UserKey instance
        that represents an existing user. If not provided, the
        constructor will try to obtain its value from the FACTIVA_USERKEY
        environment variable.

    Examples
    --------
    Creating a taxonomy instance with no user key. Fails if the environment
    variable FACTIVA_USERKEY is not set.

        >>> t = FactivaTaxonomy()

    Creating a taxonomy instance providing the user key

        >>> t = FactivaTaxonomy(user_key='abcd1234abcd1234abcd1234abcd1234')

    Creating a taxonomy instance with an existing UserKey instance

        >>> u = UserKey('abcd1234abcd1234abcd1234abcd1234')
        >>> t = FactivaTaxonomy(user_key=u)

    """

    __TAXONOMY_BASEURL = f'{const.API_HOST}{const.API_SNAPSHOTS_TAXONOMY_BASEPATH}'

    all_subjects = None
    all_regions = None
    all_industries = None
    all_companies = None

    def __init__(self, user_key=None):
        """Class initializer."""
        self.user_key = UserKey.create_user_key(user_key)
        self.log= log.get_factiva_logger()
        self.all_subjects = None
        self.all_regions = None
        self.all_industries = None
        self.all_companies = None


    # TODO: Implement a save_path parameter to alternatively store the taxonomy
    # file locally, and don't load its content to a DataFrame
    # https://github.com/dowjones/factiva-news-python/issues/4#issue-956942535
    # Check also differences by loading the data in AVRO. In case the issue is
    # too common with Executives, force the download option.
    @log.factiva_logger
    def get_category_codes(self, category:FactivaTaxonomyCategories) -> pd.DataFrame:
        """
        Request for available codes in the taxonomy for the specified category.
        WARNING: The taxonomy category FactivaTaxonomyCategories.EXECUTIVES is 
        not currently supported for this operation. Use the download_category_codes() 
        method instead.

        Parameters
        ----------
        category : FactivaTaxonomyCategories
            Enumerator entry that specifies the taxonomy category for which the
            codes will be retrieved. 

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
            >>> t = FactivaTaxonomy()
            >>> industry_codes = t.get_category_codes(FactivaTaxonomyCategories.INDUSTRIES)
            >>> print(industry_codes)
                    code                description
            0     i25121             Petrochemicals
            1     i14001         Petroleum Refining
            2       i257            Pharmaceuticals
            3     iphrws  Pharmaceuticals Wholesale
            4       i643     Pharmacies/Drug Stores
        """

        if category == FactivaTaxonomyCategories.EXECUTIVES:
            raise ValueError('The category EXECUTIVES is not currently supported for this operation')
        response_format = 'csv'
        headers_dict = {
            'user-key': self.user_key.key
        }
        endpoint = f'{self.__TAXONOMY_BASEURL}/{category.value}/{response_format}'

        response = req.api_send_request(method='GET', endpoint_url=endpoint, headers=headers_dict, stream=True)
        if response.status_code == 200:
            r_df = pd.read_csv(StringIO(response.content.decode()))
            if 'executiveFactivaCode' in r_df.columns:
                r_df.rename(columns = {'executiveFactivaCode':'code'}, inplace = True)
            elif 'Code' in r_df.columns:
                r_df.rename(columns = {'Code':'code'}, inplace = True)
            r_df.set_index('code', inplace=True)

            if category == FactivaTaxonomyCategories.SUBJECTS:
                self.all_subjects = r_df
            elif category == FactivaTaxonomyCategories.REGIONS:
                self.all_regions = r_df
            elif category == FactivaTaxonomyCategories.INDUSTRIES:
                self.all_industries = r_df
            elif category == FactivaTaxonomyCategories.COMPANIES:
                self.all_companies = r_df

            return r_df.copy()
        else:
            raise RuntimeError('API Request returned an unexpected HTTP Status')


    @log.factiva_logger
    def download_category_codes(self, category:FactivaTaxonomyCategories, path=None, file_format='csv') -> bool:
        if file_format not in ['csv', 'avro']:
            raise ValueError('The file_format parameter must be either csv or avro.')
        if not path:
            path = os.getcwd()
        endpoint = f'{self.__TAXONOMY_BASEURL}/{category.value}/{file_format}'
        download_headers = {
            'user-key': self.user_key.key
        }
        req.download_file(file_url=endpoint,
                        headers=download_headers,
                        file_name=category.value,
                        file_extension=file_format,
                        to_save_path=path)
        return True

    def resolve_code(self, code:str=None, category:FactivaTaxonomyCategories = None) -> dict or None:
        """
        Finds the code equivalence in the provided category and returns
        all available columns for that entry.

        Parameters
        ----------
        code : str
            Factiva code to be resolved
        category : FactivaTaxonomyCategories
            Enumerator entry that specifies the taxonomy category for which the
            codes will be retrieved. 

        Returns
        -------
        Dict containing the code details

        Raises
        ------
        RuntimeError: When API request returns unexpected error

        Examples
        --------
        Getting the codes for the 'industries' category
            >>> f = FactivaTaxonomy()
            >>> f.resolve_code(code='CWKDIV', category=FactivaTaxonomyCategories.SUBJECTS)
            {'description': 'Workplace Diversity'}
        """
        if category == FactivaTaxonomyCategories.SUBJECTS:
            if not isinstance(self.all_subjects, pd.DataFrame):
                self.get_category_codes(category=category)
            return eval(self.all_subjects.loc[code.upper()].to_json())
        elif category == FactivaTaxonomyCategories.REGIONS:
            if not isinstance(self.all_regions, pd.DataFrame):
                self.get_category_codes(category=category)
            return eval(self.all_regions.loc[code.upper()].to_json())
        elif category == FactivaTaxonomyCategories.INDUSTRIES:
            if not isinstance(self.all_industries, pd.DataFrame):
                self.get_category_codes(category=category)
            return eval(self.all_industries.loc[code.upper()].to_json())
        elif category == FactivaTaxonomyCategories.COMPANIES:
            if not isinstance(self.all_companies, pd.DataFrame):
                self.get_category_codes(category=category)
            return eval(self.all_companies.loc[code.upper()].to_json())
        return None


    def __print_property__(self, property_value) -> str:
        if isinstance(property_value, pd.DataFrame):
            pval = f"<pandas.DataFrame> - [{property_value.shape[0]}] rows"
        elif not property_value:
            pval = '<NotSet>'
        elif isinstance(property_value, int):
            pval = f'{property_value:,d}'
        elif isinstance(property_value, float):
            pval = f'{property_value:,f}'
        else:
            pval = property_value
        return pval


    def __repr__(self):
        """Return a string representation of the object."""
        return self.__str__()


    def __str__(self, detailed=True, prefix='  |-', root_prefix=''):
        # TODO: Improve the output for enabled_company_identifiers
        pprop = self.__dict__.copy()
        del pprop['user_key']
        del pprop['log']
        
        ret_val = f'{root_prefix}{str(self.__class__)}\n'
        ret_val += f"{prefix}user_key = {self.user_key.__str__(detailed=False, prefix='  |  |-')}\n"

        if detailed:
            ret_val += '\n'.join((f'{prefix}{item} = {self.__print_property__(pprop[item])}' for item in pprop))
        else:
            ret_val += f'{prefix}...'
        return ret_val

