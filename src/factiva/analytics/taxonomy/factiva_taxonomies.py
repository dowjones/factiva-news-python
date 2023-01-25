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

    Examples
    --------
    Use it directly when needed. Usually as param in ``FactivaTaxonomy`` methods.

    .. code-block:: python

        from factiva.analytics import FactivaTaxonomyCategories
        FactivaTaxonomyCategories.SUBJECTS
        FactivaTaxonomyCategories.REGIONS
        FactivaTaxonomyCategories.COMPANIES
        FactivaTaxonomyCategories.INDUSTRIES
        FactivaTaxonomyCategories.EXECUTIVES

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
    
    ``Subject``, ``industry`` and ``region`` taxonomies have two separate
    categories in the API. However, current implementation uses only a 
    simplified version where the dataset returns all codes with
    the minimum set of columns to build a hierarchy.

    Parameters
    ----------
    user_key: str or UserKey
        String containing the 32-character long APi Key or UserKey instance
        that represents an existing user. If not provided, the
        constructor will try to obtain its value from the ``FACTIVA_USERKEY``
        environment variable.

    Examples
    --------
    Creating a taxonomy instance with no user key. Fails if the environment
    variable ``FACTIVA_USERKEY`` is not set.

    .. code-block:: python

        from factiva.analytics import FactivaTaxonomy
        t = FactivaTaxonomy()

    Creating a taxonomy instance providing the user key as string

    .. code-block:: python

        from factiva.analytics import FactivaTaxonomy
        t = FactivaTaxonomy(user_key='abcd1234abcd1234abcd1234abcd1234')

    Creating a taxonomy instance with an existing ``UserKey`` instance

    .. code-block:: python

        from factiva.analytics import UseKey, FactivaTaxonomy
        u = UserKey('abcd1234abcd1234abcd1234abcd1234')
        t = FactivaTaxonomy(user_key=u)

    With the ``FactivaTaxonomy`` instance ``t``, it's now possible to call any
    method. Please see below.

    """

    __TAXONOMY_BASEURL = f'{const.API_HOST}{const.API_SNAPSHOTS_TAXONOMY_BASEPATH}'

    all_subjects = None
    all_regions = None
    all_industries = None
    all_companies = None

    def __init__(self, user_key=None):
        """Class initializer."""
        if isinstance(user_key, UserKey):
            self.user_key = user_key
        else:
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

        .. important::

            The taxonomy category ``FactivaTaxonomyCategories.EXECUTIVES`` is 
            not currently supported by this operation. Use the ``download_category_codes()`` 
            method instead.

        Parameters
        ----------
        category : FactivaTaxonomyCategories
            Enumerator entry that specifies the taxonomy category for which the
            codes will be retrieved. 

        Returns
        -------
        pandas.DataFrame:
            Dataframe containing the codes for the specified category

        Examples
        --------
        Getting the codes for the 'industries' category

        .. code-block:: python

            from factiva.analytics import FactivaTaxonomy, FactivaTaxonomyCategories
            t = FactivaTaxonomy()
            industry_codes = t.get_category_codes(FactivaTaxonomyCategories.INDUSTRIES)
            industry_codes

        .. code-block::

                          code                descriptor                                        description direct_parent
            code
            I0              I0               Agriculture  All farming, forestry, commercial fishing, hun...           NaN
            I01001      I01001                   Farming  Agricultural crop production, seed supply and ...            i0
            I03001      I03001               Aquaculture  The farming of aquatic animals and plants such...        i01001
            I0100144  I0100144             Cocoa Growing                               Growing cocoa beans.        i01001
            I0100137  I0100137            Coffee Growing                              Growing coffee beans.        i01001
            ...            ...                       ...                                                ...           ...
            I162          I162             Gas Utilities  Operating gas distribution and transmission sy...           i16
            IMULTI      IMULTI            Multiutilities  Utility companies with significant presence in...         iutil
            I17            I17           Water Utilities  Operating water treatment plants and/or operat...         iutil
            IDESAL      IDESAL              Desalination  Desalination is the process of removing salt a...           i17
            IDISHEA    IDISHEA  District Heating/Cooling  Heating systems that involve the distribution ...           i17
        
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
        file_format : str {csv, avro}
            String specifying the download format

        Returns
        -------
        bool:
            ``True`` if the file is correctly downloaded. ``False`` otherwise.

        Raises
        ------
        ValueError:
            When the parameter ``file_fomat`` is invalid or not a string

        Examples
        --------
        Getting the raw file for the 'industries' category

        .. code-block:: python

            from factiva.analytics import FactivaTaxonomy, FactivaTaxonomyCategories
            f = FactivaTaxonomy()
            f.download_raw_category(category=FactivaTaxonomyCategories.INDUSTRIES, path='/home/user/')

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
        dict:
            Dict containing the code details


        .. important::

            The return dict structure can vary depending on the passed category
            and the enabled settings for the used account (e.g. company identifiers
            like ISIN, CUSIP, etc.).

        Raises
        ------
        ValueError: When the parameter code is not a string

        Examples
        --------
        Lookup a code in the 'subjects' category

        .. code-block:: python

            from factiva.analytics import FactivaTaxonomy, FactivaTaxonomyCategories
            f = FactivaTaxonomy()
            f.lookup_code(code='CWKDIV', category=FactivaTaxonomyCategories.SUBJECTS)

        .. code-block:: python

            {'code': 'CWKDIV', 'descriptor': 'Workplace Diversity', 'description': 'Diversity and inclusion in the workplace to ensure employees encompass varying traits such as race, gender, ethnicity, age, religion, sexual orientation, socioeconomic background or disability.', 'direct_parent': 'C42'}
        
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


    def __repr__(self):
        """Return a string representation of the object."""
        return self.__str__()


    def __str__(self, detailed=True, prefix='  ├─', root_prefix=''):
        # TODO: Improve the output for enabled_company_identifiers
        pprop = self.__dict__.copy()
        del pprop['user_key']
        del pprop['log']
        del pprop['all_companies']
        
        ret_val = f"{root_prefix}<factiva.analytics.{str(self.__class__).split('.')[-1]}\n"
        ret_val += f"{prefix}user_key: {self.user_key.__str__(detailed=False, prefix='  │  ├─')}\n"

        if detailed:
            ret_val += '\n'.join((f'{prefix}{item}: {tools.print_property(pprop[item])}' for item in pprop))
            ret_val += f"\n{prefix[0:-2]}└─all_companies: {tools.print_property(self.all_companies)}"
        else:
            ret_val += f'\n{prefix[0:-2]}└─...'
        return ret_val

