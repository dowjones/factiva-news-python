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
    cateories present in Factiva data. Given the fact that the API has two
    versions of Subjects, Regions and Industries; only the full hierarchy
    version is implemented. The simple version is a sub-set of the
    hierarchical dataset.

        >>> FactivaTaxonomyCategories.SUBJECTS
        >>> FactivaTaxonomyCategories.REGIONS
        >>> FactivaTaxonomyCategories.COMPANIES
        >>> FactivaTaxonomyCategories.INDUSTRIES
        >>> FactivaTaxonomyCategories.EXECUTIVES
    """
    SUBJECTS = 'hierarchySubject'
    REGIONS = 'hierarchyRegion'
    INDUSTRIES = 'hierarchyIndustry'
    COMPANIES = 'companies'
    EXECUTIVES = 'executives'


class FactivaTaxonomy():
    """
    Class that represents the Factiva Taxonomy endpoints in Factiva
    Analytics.
    
    `Subject`, `industry` and `region` taxonomies have two separate
    categories in the API. However, current implementation uses only a 
    simplified version where the dataset returns all codes with
    the minimum set of columns to build a hierarchy.

    Parameters
    ----------
    user_key: str or UserKey
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
        self.user_key = UserKey(user_key)
        self.log= log.get_factiva_logger()
        self.all_subjects = None
        self.all_regions = None
        self.all_industries = None
        self.all_companies = None


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
        Pandas Dataframe containing the codes for the specified category

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

            if 'Code' in r_df.columns:
                r_df.rename(columns = {'Code':'code'}, inplace = True)
            r_df['code'] = r_df['code'].str.upper()
            r_df.set_index('code', inplace=True, drop=False)

            if category == FactivaTaxonomyCategories.SUBJECTS:
                for column in const.TAXONOMY_H_FIELDS_REMOVE:
                    if column in r_df.columns:
                        r_df.drop(column, axis=1, inplace=True)
                r_df.rename(columns = const.TAXONOMY_H_FIELDS_RENAME_DICT, inplace = True)
                self.all_subjects = r_df
            elif category == FactivaTaxonomyCategories.REGIONS:
                for column in const.TAXONOMY_H_FIELDS_REMOVE:
                    if column in r_df.columns:
                        r_df.drop(column, axis=1, inplace=True)
                r_df.rename(columns = const.TAXONOMY_H_FIELDS_RENAME_DICT, inplace = True)
                self.all_regions = r_df
            elif category == FactivaTaxonomyCategories.INDUSTRIES:
                for column in const.TAXONOMY_H_FIELDS_REMOVE:
                    if column in r_df.columns:
                        r_df.drop(column, axis=1, inplace=True)
                r_df.rename(columns = const.TAXONOMY_H_FIELDS_RENAME_DICT, inplace = True)
                self.all_industries = r_df
            elif category == FactivaTaxonomyCategories.COMPANIES:
                r_df.rename(columns = {'description':'descriptor'}, inplace = True)
                self.all_companies = r_df

            return r_df.copy()
        else:
            raise RuntimeError('API Request returned an unexpected HTTP Status')


    @log.factiva_logger
    def download_raw_category(self, category:FactivaTaxonomyCategories, path=None, file_format='csv') -> bool:
        """
        Downloads a CSV or AVRO file with the specified taxonomy category.
        The file columns preserve their original name, thus it may not match the
        same column naming used in other methods in this FactivaTaxonomy class.

        Parameters
        ----------
        category : FactivaTaxonomyCategories
            Enumerator entry that specifies the taxonomy category for which the
            codes will be retrieved.
        path : str
            Folder path where the output file will be stored.
        file_format : str (optional)
            String specifying the download format ('csv' or 'avro')

        Returns
        -------
        True if the file is correctly downloaded. False otherwise.

        Raises
        ------
        ValueError: When the parameter file_fomat is invalid or not a string
        RuntimeError: When API request returns unexpected error

        Examples
        --------
        Getting the raw file for the 'industries' category
            >>> f = FactivaTaxonomy()
            >>> f.download_raw_category(category=FactivaTaxonomyCategories.INDUSTRIES)
            {'descriptor': 'Workplace Diversity'}
        """
        if not isinstance(file_format, str):
            raise ValueError('The file_format parameter must be a string')
        file_format = file_format.lower()
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


    def lookup_code(self, code:str, category:FactivaTaxonomyCategories) -> dict:
        """
        Finds the descriptor and other details based on the provide code and
        category. Returns all available columns for that entry.

        Parameters
        ----------
        code : str
            Factiva code for lookup
        category : FactivaTaxonomyCategories
            Enumerator entry that specifies the taxonomy category for which the
            codes will be retrieved. 

        Returns
        -------
        Dict containing the code details

        Raises
        ------
        ValueError: When the parameter code is not a string
        RuntimeError: When API request returns unexpected error

        Examples
        --------
        Lookup a code in the 'subjects' category
            >>> f = FactivaTaxonomy()
            >>> f.lookup_code(code='CWKDIV', category=FactivaTaxonomyCategories.SUBJECTS)
            {'code': 'CWKDIV', 'descriptor': 'Workplace Diversity', 'description': 'Diversity
             and inclusion in the workplace to ensure employees encompass varying traits such 
             as race, gender, ethnicity, age, religion, sexual orientation, socioeconomic 
             background or disability.', 'direct_parent': 'C42'}
        """
        if not isinstance(code, str):
            raise ValueError('Parameter code is not a string')
        code = code.upper()

        if category == FactivaTaxonomyCategories.SUBJECTS:
            if not isinstance(self.all_subjects, pd.DataFrame):
                self.get_category_codes(category=category)
            if code in self.all_subjects.index:
                return self.all_subjects.loc[code].to_dict()

        elif category == FactivaTaxonomyCategories.REGIONS:
            if not isinstance(self.all_regions, pd.DataFrame):
                self.get_category_codes(category=category)
            if code in self.all_regions.index:
                return self.all_regions.loc[code].to_dict()

        elif category == FactivaTaxonomyCategories.INDUSTRIES:
            if not isinstance(self.all_industries, pd.DataFrame):
                self.get_category_codes(category=category)
            if code in self.all_industries.index:
                return self.all_industries.loc[code].to_dict()

        elif category == FactivaTaxonomyCategories.COMPANIES:
            # Requires handling duplicates as some companies have
            # multiple entries because of ticker values
            if not isinstance(self.all_companies, pd.DataFrame):
                self.get_category_codes(category=category)
            if code in self.all_companies.index:
                f_df = self.all_companies[self.all_companies.code == code]
                if f_df.shape[0] == 1:
                    return self.all_companies.loc[code].to_dict()
                else:
                    f_df = f_df[f_df.exchange == f_df.primary_exchange]
                    if f_df.shape[0] == 1:
                        return f_df.loc[code].to_dict()
                    else:
                        return f_df.iloc[0].to_dict()

        # When code is not found
        return {'error': f'Code {code} not found in {category.value}',
                'code': 'UNKNOWN',
                'descriptor': f'ERR: Code {code} not found in {category.value}'}


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

