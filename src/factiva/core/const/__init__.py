from __future__ import absolute_import, division, print_function

import os

from ..tools import load_environment_value
from .errors import *

"""Define library's constant literals."""
API_HOST = 'https://api.dowjones.com'
API_ACCOUNT_OAUTH2_HOST = 'https://accounts.dowjones.com/oauth2/v1/token'
API_LATEST_VERSION = "2.0"

# UserKey
API_ACCOUNT_BASEPATH = '/alpha/accounts'
API_ACCOUNT_STREAM_CREDENTIALS_BASEPATH = '/accounts/streaming-credentials'

# Dynamic Prefixes
ALPHA_BASEPATH = '/alpha'
DNA_BASEPATH = '/dna'  # Deprecated

# Snapshots
API_SNAPSHOTS_BASEPATH = '/alpha/extractions/documents'
API_EXPLAIN_SUFFIX = '/_explain'
API_ANALYTICS_BASEPATH = '/alpha/analytics'
API_EXTRACTIONS_BASEPATH = '/alpha/extractions'
API_EXTRACTIONS_SAMPLES_SUFFIX = '/samples'
API_DEFAULT_EXTRACTION_TYPE = "documents"
API_SAMPLES_EXTRACTION_TYPE = "samples"

API_SNAPSHOTS_TAXONOMY_BASEPATH = '/alpha/taxonomies'
API_SNAPSHOTS_COMPANIES_BASEPATH = '/alpha/companies'
API_SNAPSHOTS_COMPANY_IDENTIFIERS_BASEPATH = '/alpha/companies/identifiers'
API_SNAPSHOTS_COMPANIES_PIT = '/pit'
CUSIP_COMPANY_IDENTIFIER = 'cusip'
ISIN_COMPANY_IDENTIFIER = 'isin'
SEDOL_COMPANY_IDENTIFIER = 'sedol'
TICKER_COMPANY_IDENTIFIER = 'ticker'
API_COMPANIES_IDENTIFIER_TYPE = [
    CUSIP_COMPANY_IDENTIFIER, ISIN_COMPANY_IDENTIFIER,
    SEDOL_COMPANY_IDENTIFIER, TICKER_COMPANY_IDENTIFIER
]

# ANALYTICS
API_AVRO_FORMAT = 'avro'
API_CSV_FORMAT = 'csv'
API_JSON_FORMAT = 'json'
API_EXTRACTION_FILE_FORMATS = [
    API_AVRO_FORMAT, API_JSON_FORMAT, API_CSV_FORMAT
]

API_DAY_PERIOD = 'DAY'
API_MONTH_PERIOD = 'MONTH'
API_YEAR_PERIOD = 'YEAR'
API_DATETIME_PERIODS = [API_DAY_PERIOD, API_MONTH_PERIOD, API_YEAR_PERIOD]

API_PUBLICATION_DATETIME_FIELD = 'publication_datetime'
API_MODIFICATION_DATETIME_FIELD = 'modification_datetime'
API_INGESTION_DATETIME_FIELD = 'ingestion_datetime'
API_DATETIME_FIELDS = [
    API_PUBLICATION_DATETIME_FIELD, API_MODIFICATION_DATETIME_FIELD,
    API_INGESTION_DATETIME_FIELD
]
API_GROUP_DIMENSIONS_FIELDS = [
    'source_code', 'subject_codes', 'region_codes', 'industry_codes',
    'company_codes', 'person_codes', 'company_codes_about',
    'company_codes_relevance', 'company_codes_cusip', 'company_codes_isin',
    'company_codes_sedol', 'company_codes_ticker', 'company_codes_about_cusip',
    'company_codes_about_isin', 'company_codes_about_sedol',
    'company_codes_about_ticker', 'company_codes_relevance_cusip',
    'company_codes_relevance_isin', 'company_codes_relevance_sedol',
    'company_codes_relevance_ticker'
]

# Streams
API_STREAMS_BASEPATH = '/alpha/streams'
DOC_COUNT_EXCEEDED = "DOC_COUNT_EXCEEDED"
CHECK_EXCEEDED_WAIT_SPACING = 300
PUBSUB_MESSAGES_WAIT_SPACING = 10

# API STATES
API_JOB_CREATED_STATE = 'JOB_CREATED'
API_JOB_QUEUED_STATE = 'JOB_QUEUED'
API_JOB_PENDING_STATE = 'JOB_STATE_PENDING'
API_JOB_VALIDATING_STATE = 'JOB_VALIDATING'
API_JOB_STATE_VALIDATING = 'JOB_STATE_VALIDATING'
API_JOB_RUNNING_STATE = 'JOB_STATE_RUNNING'
API_JOB_DONE_STATE = 'JOB_STATE_DONE'
API_JOB_FAILED_STATE = 'JOB_STATE_FAILED'
API_JOB_CANCELLED_STATE = 'JOB_STATE_CANCELLED'

API_JOB_EXPECTED_STATES = [
    API_JOB_CREATED_STATE, API_JOB_QUEUED_STATE, API_JOB_PENDING_STATE,
    API_JOB_VALIDATING_STATE, API_JOB_STATE_VALIDATING, API_JOB_RUNNING_STATE,
    API_JOB_DONE_STATE, API_JOB_FAILED_STATE, API_JOB_CANCELLED_STATE
]

API_JOB_ACTIVE_WAIT_SPACING = 10

# SNAPSHOT FILES
SNAPSHOT_FILE_STATS_FIELDS = [
    'an', 'company_codes', 'company_codes_about', 'company_codes_occur',
    'industry_codes', 'ingestion_datetime', 'language_code',
    'modification_datetime', 'publication_datetime', 'publisher_name',
    'region_codes', 'region_of_origin', 'source_code', 'source_name',
    'subject_codes', 'title', 'word_count'
]

SNAPSHOT_FILE_DELETE_FIELDS = [
    'art', 'credit', 'document_type', 'publication_date', 'modfication_date'
]  # publication_date and modification_date are deprecated

# Files options
DOWNLOAD_DEFAULT_FOLDER = load_environment_value(
    "DOWNLOAD_FILES_DIR", os.path.join(os.getcwd(), 'downloads'))
LISTENER_FILES_DEFAULT_FOLDER = load_environment_value(
    'STREAM_FILES_DIR', os.path.join(os.getcwd(), 'listener'))
LOGS_DEFAULT_FOLDER = load_environment_value(
    'LOG_FILES_DIR', os.path.join(os.getcwd(), 'logs'))

#TIMESTAMP
TIMESTAMP_FIELDS = [
    "ingestion_datetime", "modification_date", "modification_datetime",
    "publication_date", "publication_datetime"
]

#Multivalue
MULTIVALUE_FIELDS_COMMA = [
    'company_codes', 'subject_codes', 'region_codes', 'industry_codes',
    'person_codes', 'currency_codes', 'market_index_codes',
    'company_codes_about', 'company_codes_association',
    'company_codes_lineage', 'company_codes_occur', 'company_codes_relevance',
    'company_codes_about_cusip', 'company_codes_association_cusip',
    'company_codes_lineage_cusip', 'company_codes_occur_cusip',
    'company_codes_relevance_cusip', 'company_codes_cusip',
    'company_codes_about_isin', 'company_codes_association_isin',
    'company_codes_lineage_isin', 'company_codes_occur_isin',
    'company_codes_relevance_isin', 'company_codes_isin',
    'company_codes_about_ticker_exchange',
    'company_codes_association_ticker_exchange',
    'company_codes_lineage_ticker_exchange',
    'company_codes_occur_ticker_exchange',
    'company_codes_relevance_ticker_exchange', 'company_codes_ticker_exchange',
    'company_codes_about_sedol', 'company_codes_association_sedol',
    'company_codes_lineage_sedol', 'company_codes_occur_sedol',
    'company_codes_relevance_sedol', 'company_codes_sedol'
]

MULTIVALUE_FIELDS_SPACE = ["region_of_origin"]

ADD_ACTION = 'add'
REP_ACTION = 'rep'
DEL_ACTION = 'del'
ERR_ACTION = 'error'
ALLOWED_ACTIONS = [ADD_ACTION, REP_ACTION, DEL_ACTION]
ACTION_CONSOLE_INDICATOR = {}
ACTION_CONSOLE_INDICATOR[ADD_ACTION] = '.'
ACTION_CONSOLE_INDICATOR[REP_ACTION] = ':'
ACTION_CONSOLE_INDICATOR[DEL_ACTION] = '&'
ACTION_CONSOLE_INDICATOR[ERR_ACTION] = '!'
