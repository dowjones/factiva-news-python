"""Implement different helper functions and classes for jobs."""
import pandas as pd


from factiva.core import const
from factiva.news.bulknews import BulkNewsJob


class ExplainJob(BulkNewsJob):
    """Represent the operation of creating an explain from Factiva Snapshots API."""

    document_volume = 0
    extraction_type = const.API_DEFAULT_EXTRACTION_TYPE

    def __init__(self, user_key):
        """Initialize class."""
        super().__init__(user_key=user_key)
        self.document_volume = 0

    # It's important for these methods to be class methods since they
    # are overrriden methods from the base class.
    # pylint: disable=no-self-use
    def get_endpoint_url(self):
        """Get endpoint URL."""
        endpoint = ''
        if (self.extraction_type == const.API_SAMPLES_EXTRACTION_TYPE):
            endpoint = f'{const.API_HOST}{const.API_EXTRACTIONS_BASEPATH}{const.API_EXTRACTIONS_SAMPLES_SUFFIX}'
        else:
            endpoint = f'{const.API_HOST}{const.API_SNAPSHOTS_BASEPATH}{const.API_EXPLAIN_SUFFIX}'

        # Set default for safety
        self.extraction_type = const.API_DEFAULT_EXTRACTION_TYPE
        return endpoint

    # pylint: disable=no-self-use
    def get_job_id(self, source):
        """Get job ID."""
        return source['data']['id']

    def set_job_data(self, source):
        """Set job data."""
        self.document_volume = source['data']['attributes']['counts']


class AnalyticsJob(BulkNewsJob):
    """Represent the operation of creating Analtyics from Factiva Snapshots API."""
    data = []

    def __init__(self, user_key):
        """Initialize class."""
        super().__init__(user_key=user_key)
        self.data = []

    # pylint: disable=no-self-use
    def get_endpoint_url(self):
        """Get endpoint URL."""
        return f'{const.API_HOST}{const.API_ANALYTICS_BASEPATH}'

    # pylint: disable=no-self-use
    def get_job_id(self, source):
        """Get job ID."""
        return source['data']['id']

    def set_job_data(self, source):
        """Sets job data."""
        self.data = pd.DataFrame(source['data']['attributes']['results'])

        for field in const.API_GROUP_DIMENSIONS_FIELDS:
            if field not in self.data.columns:
                self.data[field] = f'ALL_{field.upper().strip()}'


class ExtractionJob(BulkNewsJob):
    """Class that represents the operation of creating a Snapshot from Factiva Snapshots API."""
    files = []
    file_format = ''

    def __init__(self, snapshot_id=None, user_key=None):
        """Initialize class."""
        super().__init__(user_key=user_key)
        self.files = []
        self.file_format = ''

        if snapshot_id and user_key:
            self.job_id = snapshot_id
            self.link = f'{const.API_HOST}{const.API_SNAPSHOTS_BASEPATH}/dj-synhub-extraction-{self.user_key.key}-{snapshot_id}'

    # pylint: disable=no-self-use
    def get_endpoint_url(self):
        """Obtain endpoint URL."""
        return f'{const.API_HOST}{const.API_SNAPSHOTS_BASEPATH}'

    # pylint: disable=no-self-use
    def get_job_id(self, source):
        """Obtain Job ID."""
        return source['data']['id'].split('-')[-1]

    def set_job_data(self, source):
        """Set job data."""
        self.file_format = source['data']['attributes']['format']
        file_list = source['data']['attributes']['files']
        self.files = [file_item['uri'] for file_item in file_list]

    def process_job(self, payload=None, path=None):
        """Override method from parent class to call the method for downloading the files once the snapshot has been completed.

        Overrides method from parent class to call the method for downloading the files once the snapshot has been completed.

        Parameters
        ----------
        payload: str, Optional
            String containg the snapshot instance.

        path: str, Optional
            String containg the path where to store the snapshots files that are downloaded from the snapshot.
            If no path is given, the files will be stored in a folder named after the snapshot_id in the current working directory.

        """
        super().process_job(payload)
        self.download_job_files(path)


class UpdateJob(ExtractionJob):
    """Represent the Snapshot Updates.

    Class that represents the Snapshot Updates. There can be three types of updates: additions, replacements and deletes.

    Parameters
    ----------
    update_type: str, Optional
        String describing the type of update that this job represents. Requires snapshot_id to be provided as well. Not compatible with update_id
    snapshot_id: str, Optional
        String containing the id of the snapshot that is being updated. Requires update_type to be provided as well. Not compatible with update_id
    update_id: str, Optional
        String containing the id of an update job that has been created previously. Both update_type and snapshot_id can be obtained from this value.
        Not compatible with update_type nor snapshot_id

    Raises
    ------
    - Exception when fields that are not compatible are provided or when not enough parameters are provided to create the job.

    """
    update_type = None
    snapshot_id = None

    def __init__(self, update_type=None, snapshot_id=None, update_id=None, user_key=None):
        """Construct class instance."""
        super().__init__(user_key=user_key)

        if update_id and (update_type or snapshot_id):
            raise Exception('update_id parameter is not compatible with update_type and snapshot id')

        if update_id:
            self.job_id = update_id
            self.update_type = update_id.split('-')[1]
            self.snapshot_id = update_id.split('-')[0]
            self.link = f'{const.API_HOST}{const.API_SNAPSHOTS_BASEPATH}/dj-synhub-extraction-{self.user_key.key}-{update_id}'
            self.get_job_results()

        elif update_type and snapshot_id:
            self.update_type = update_type
            self.snapshot_id = snapshot_id
        else:
            raise Exception('Not enough parameters to create an update job')

    def get_endpoint_url(self):
        """Get endpoint URL."""
        return f'{const.API_HOST}{const.API_EXTRACTIONS_BASEPATH}/dj-synhub-extraction-{self.user_key.key}-{self.snapshot_id}/{self.update_type}'

    def get_job_id(self, source):
        """Get job ID from source."""
        # UPDATE_ID FORMAT: {API_URL}/dj-synhub-extraction-{USER-KEY}-{SNAPSHOT_ID}-{UPDATE_TYPE}-{DATETIME}
        return '-'.join(source['data']['id'].split('-')[-3:])
