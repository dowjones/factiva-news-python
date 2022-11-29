"""Define basic dictionaries of Hierarchies adn Taxonomies."""
import os
import pandas as pd
import numpy as np

rootdir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
datadir = os.path.join(rootdir, 'dicts')
ind_hrchy_path = os.path.join(datadir, 'industries-hrchy.csv')
reg_hrchy_path = os.path.join(datadir, 'regions-hrchy.csv')
countries_path = os.path.join(datadir, 'factiva-countries.csv')


def industries_hierarchy() -> pd.DataFrame:
    """Read the Dow Jones Industry hierarchy CSV file.

    Reads the Dow Jones Industry hierarchy CSV file and returns
    its content as a Pandas DataFrame. The root node has
    the fcode `indroot` and an empty parent.

    Returns
    -------
    DataFrame : A Pandas DataFrame with the columns:
        * ind_fcode : string
            Industry Factiva Code
        * name : string
            Name of the Industry
        * parent : string
            Factiva Code of the parent Industry

    """
    ret_ind = pd.read_csv(ind_hrchy_path)
    ret_ind = ret_ind.replace(np.nan, '', regex=True)
    return ret_ind


def regions_hierarchy() -> pd.DataFrame:
    """Read the Dow Jones Regions hierarchy CSV file.

    Reads the Dow Jones Regions hierarchy CSV file and returns
    its content as a Pandas DataFrame. The root node has
    the fcode `WORLD` and an empty parent.

    Names containng an asterisk denote nodes not officially in
    the hierarchy, but that help balancing and reading the
    structure. Level balancing is quite useful specially for
    visualising data.

    Returns
    -------
    DataFrame : A Pandas DataFrame with the columns:
        * name : string
            Name of the region node
        * reg_fcode : string
            Factiva Code of the region
        * parent : string
            Factiva Code of the parent region
        * level : int
            Level number of the node

    """
    ret_reg = pd.read_csv(reg_hrchy_path)
    ret_reg = ret_reg.replace(np.nan, '', regex=True)
    return ret_reg


def countries_list() -> pd.DataFrame:
    """Read a list of official countries.

    Reads a list of official countries with several additional fields that
    are helpful in data merges. All contries have the Factiva Code along with
    other identifiers.

    Returns
    -------
    DataFrame : A Pandas DataFrame

    """
    ret_reg = pd.read_csv(countries_path)
    ret_reg['factiva_code'] = ret_reg['factiva_code'].str.lower()
    ret_reg.set_index('factiva_code', inplace=True)
    ret_reg = ret_reg.replace(np.nan, '', regex=True)
    return ret_reg
