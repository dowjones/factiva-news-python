__all__ = ['snapshot', 'stream', 'bulknews']

import os
import pandas as pd
import fastavro


_ARTICLES_STAT_FIELDS = ['an', 'company_codes', 'company_codes_about',
                         'company_codes_occur', 'industry_codes', 'ingestion_datetime',
                         'language_code', 'modification_date',
                         'modification_datetime',
                         'publication_date', 'publication_datetime',
                         'publisher_name', 'region_codes', 'region_of_origin',
                         'source_code', 'source_name',
                         'subject_codes', 'title', 'word_count'
                         ]

_ARTICLE_DELETE_FIELDS = ['art', 'credit', 'document_type']


def read_snapshot_file(self, filepath, only_stats=True, merge_body=True) -> pd.DataFrame:
    """
    Reads a single Dow Jones snapshot datafile

    Parameters
    ----------
    filepath : str
        Relative or absolute file path
    only_stats : bool, optional
        Specifies if only file metadata is loaded (True), or if the full article content is loaded (False). On average,
        only_stats loads about 1/10 and is recommended for quick metadata-based analysis. (Default is True)
    merge_body : bool, optional
        Specifies if the body field should be merged with the snippet and this last column being dropped.
        (default is True)

    Returns
    -------
    pandas.DataFrame
        A single Pandas Dataframe with the file's content
    """
    with open(filepath, "rb") as fp:
        reader = fastavro.reader(fp)
        records = [r for r in reader]
        r_df = pd.DataFrame.from_records(records)

    if only_stats is True:
        r_df = r_df[self._ARTICLES_STAT_FIELDS]

    if (only_stats is False) & (merge_body is True):
        r_df['body'] = r_df['snippet'] + '\n\n' + r_df['body']
        r_df.drop('snippet', axis=1, inplace=True)

    r_df['body'] = r_df[['body']].apply(lambda x: '{}'.format(x[0]), axis=1)

    for d_field in self._ARTICLE_DELETE_FIELDS:
        if d_field in r_df.columns:
            r_df.drop(d_field, axis=1, inplace=True)
    r_df['publication_date'] = r_df['publication_date'].astype('datetime64[ms]')
    r_df['publication_datetime'] = r_df['publication_datetime'].astype('datetime64[ms]')
    r_df['modification_date'] = r_df['modification_date'].astype('datetime64[ms]')
    r_df['modification_datetime'] = r_df['modification_datetime'].astype('datetime64[ms]')
    r_df['ingestion_datetime'] = r_df['ingestion_datetime'].astype('datetime64[ms]')
    return r_df


def read_snapshot_folder(self, folderpath, file_format='AVRO', only_stats=True, merge_body=True) -> pd.DataFrame:
    """
    Scans a folder and reads the content of all files matching the format (file_format)

    Parameters
    ----------
    folderpath : str
        Relative or absolute folder path
    file_format : str, optional
        Supported file format. Current options are AVRO, JSON or CSV. (default is AVRO)
    only_stats : bool, optional (Default: True)
        Specifies if only file metadata is loaded (True), or if the full article
        content is loaded (False). On average, only_stats loads about 1/10 and is
        recommended for quick metadata-based analysis.
    merge_body : bool, optional (Default: True)
        Specifies if the body field should be merged with the snippet and this last
        column being dropped.

    Returns
    -------
    pandas.DataFrame
        A single Pandas Dataframe with the content from all read files within the folder.
    """
    format_suffix = file_format.lower()
    r_df = pd.DataFrame()
    for filename in os.listdir(folderpath):
        if filename.lower().endswith("." + format_suffix):
            t_df = self.read_file(folderpath + "/" + filename, only_stats, merge_body)
            r_df = pd.concat([r_df, t_df])
    return r_df
