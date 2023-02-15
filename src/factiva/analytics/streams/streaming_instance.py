"""
  Module containing all clases that interact with the Factiva Analytics - Streams service
"""
import time
from ..auth import UserKey
from ..snapshots.base import SnapshotBaseQuery
from ..common import log, const, req, config, tools



class StreamingSubscription():
    id: str = None
    short_id: str = None
    user_key: UserKey = None

    def __init__(self, id: str, user_key: UserKey or str = None) -> None:
        if len(id) == 76:
            self.id = id
        elif len(id) > 76:
            self.id = id.split('/')[-1]
        else:
            raise ValueError("Invalid subscription id.")

        if isinstance(user_key, UserKey):
            self.user_key = user_key
        else:
            self.user_key = UserKey(user_key)

        self.short_id = self.id.split('-')[-1]


    def __repr__(self):
        return self.__str__()


    def __str__(self, table=False, prefix='  ├─', root_prefix=''):
        if not table:
            ret_val = f"{root_prefix}<factiva.analytics.{str(self.__class__).split('.')[-1]}"
            ret_val += f'{prefix}short_id: {self.short_id}'
        else:
            ret_val = f"{prefix}{self.short_id:>8}"
        return ret_val



class StreamingQuery(SnapshotBaseQuery):
    where: str = None
    includes: list[str] = None
    excludes: list[str] = None

    # TODO: Consider implementing a SQL validation functionality to ensure 
    # fields are valid. There's sample projects doing something similar.
    # https://github.com/David-Wobrock/sqlvalidator


    def __init__(
        self,
        where=None,
        includes:dict=None,
        excludes:dict=None
    ):
        if isinstance(where, str):
            self.where = where
        elif config.load_environment_value('FACTIVA_WHERE', '') != '':
            self.where = config.load_environment_value('FACTIVA_WHERE')
        else:
            raise ValueError("Where value not provided and env variable FACTIVA_WHERE not set.")

        # TODO: Create a validation method that checks the dict structure as key: [list]
        # for all properties below. Additionally, keys must be in a predefined column
        # dictionary.
        if includes:
            self.includes = tools.parse_field(includes, 'includes')

        if excludes:
            self.excludes = tools.parse_field(excludes, 'excludes')


    def get_payload(self) -> dict:
        query_dict = {
            "data": {
                "attributes": {
                    "where": self.where
                },
                "type": "stream"
            }
        }
        if self.includes:
            query_dict["query"].update({"includes": self.includes})

        if self.excludes:
            query_dict["query"].update({'excludes': self.excludes})

        return query_dict


    def __repr__(self):
        return self.__str__()


    def __str__(self, detailed=True, prefix='  ├─', root_prefix=''):
        ret_val = f"{root_prefix}<factiva.analytics.{str(self.__class__).split('.')[-1]}\n"
        ret_val += f'{prefix}where: '
        ret_val += (self.where[:77] + '...') if len(self.where) > 80 else self.where
        # if detailed:
        ret_val += f"\n{prefix}includes: "
        ret_val += f"\n{prefix.replace('├', '│')[0:-1]}  └─{len(self.includes.keys())} conditions" if self.includes else "<NotSet>"
        ret_val += f"\n{prefix.replace('├', '└')}excludes: "
        ret_val += f"\n{prefix.replace('├', '│')[0:-1]}  └─{len(self.excludes.keys())} conditions" if self.excludes else "<NotSet>"
        # else:
        #     ret_val += f"\n{prefix.replace('├', '└')}..."
        return ret_val



class StreamingInstance():

    __JOB_BASE_URL = None
    __log = None

    id: str = None
    short_id: str = None
    user_key: UserKey = None
    query: StreamingQuery = None
    status: str = None
    subscriptions: list[StreamingSubscription] = None

    def __init__(self, id=None, query=None, user_key=None) -> None:
        self.__log = log.get_factiva_logger()
        self.__JOB_BASE_URL = f'{const.API_HOST}{const.API_STREAMS_BASEPATH}'
        self.status = 'NOT_CREATED'

        self.__log.info('creating StreamingInstance...')

        if isinstance(user_key, UserKey):
            self.user_key = user_key
        else:
            self.user_key = UserKey(user_key)

        if not self.user_key.cloud_token:
            self.user_key.get_cloud_token()

        if query and id:
            raise ValueError("The query and id parameters cannot be assigned simultaneously")

        if id:
            self.__log.info(f'Creating a StreamingInstance with ID {id}')
            # Considers two types of IDs:
            # - dj-synhub-stream-lufcwmlbrmmpg1p1kmq9c1ex8blcnqdu-obhztjwvqa
            # - obhztjwvqa
            if len(id) == 60:
                self.id = id
                self.short_id = id.split('-')[-1]
            elif len(id) == 10:
                self.short_id = id
                self.id = f'dj-synhub-stream-{self.user_key.key.lower()}-{id}'
            self.get_status()
        elif query:
            if isinstance(query, StreamingQuery):
                self.query = query
            elif isinstance(query, str):
                self.query = StreamingQuery(query)
            else:
                raise ValueError('Unexpected query type')
        else:
            self.query = StreamingQuery()
        self.__log.info('StreamingInstance created OK')


    @log.factiva_logger
    def create(self):
        """
        Performs a POST request to the API using the assigned values in ``user_key``
        and ``query``.

        If the Streaming Instance is created successfully, the ID and status will be
        updated

        Returns
        -------
        bool
            True if the submission was successful. An Exception otherwise.

        Raises
        ------
        ValueError
            When the ``query`` is empty or invalid.
        RuntimeError
            When the job status is not one of the expected values or if an unexpected
            response is received from the API.
        """
        self.__log.info('submit_job submitting...')
        if not self.query:
            raise ValueError('A query is needed to submit an Explain Job')

        headers_dict = {
                'user-key': self.user_key.key,
                'Content-Type': 'application/json'
            }
        
        create_url = self.__JOB_BASE_URL
        submit_payload = self.query.get_payload()

        response = req.api_send_request(method='POST', endpoint_url=create_url, headers=headers_dict, payload=submit_payload)

        if response.status_code == 201:
            resp_data = response.json()
            self.id = str(resp_data['data']['id'])
            self.short_id = self.id.split('-')[-1]
            self.status = str(resp_data['data']['attributes']['job_status'])
            resp_subscriptions = resp_data['data']['relationships']['subscriptions']['data']
            self.subscriptions = []
            for sub in resp_subscriptions:
                self.subscriptions.append(StreamingSubscription(sub['id'], self.user_key))
            while not (self.status in [const.API_JOB_CANCELLED_STATE,
                                       const.API_JOB_FAILED_STATE,
                                       const.API_JOB_RUNNING_STATE]):
                if self.status not in const.API_JOB_EXPECTED_STATES:
                    raise RuntimeError(f'Unexpected job status: {self.status}')
                time.sleep(const.API_JOB_ACTIVE_WAIT_SPACING)
                self.get_status()
            if self.status in [const.API_JOB_CANCELLED_STATE, const.API_JOB_FAILED_STATE]:
                raise RuntimeError(f'StreamingInstance creation failed with status: {self.status}')
        elif response.status_code == 400:
            raise ValueError(f'Invalid Query [{response.text}]')
        else:
            raise RuntimeError(f'API request returned an unexpected HTTP status, with content [{response.text}]')
        
        self.__log.info('submit_job OK')
        return True


    @log.factiva_logger
    def get_status(self):
        """
        Performs a GET request to the API using the assigned values in ``user_key``
        and ``id``.

        If the Streaming Instance is retrieved successfully, the status will be
        updated

        Returns
        -------
        bool
            True if the submission was successful. An Exception otherwise.

        Raises
        ------
        ValueError
            When the ``id`` is empty or invalid.
        """
        self.__log.info('get_status submitting...')
        if not self.id:
            raise ValueError('An ID is needed to get the status of a Streaming Instance')

        headers_dict = {
                'user-key': self.user_key.key,
                'Content-Type': 'application/json'
            }
        
        status_url = f'{self.__JOB_BASE_URL}/{self.id}'
        response = req.api_send_request(method='GET', endpoint_url=status_url, headers=headers_dict)

        if response.status_code == 200:
            resp_data = response.json()
            self.status = str(resp_data['data']['attributes']['job_status'])
            self.subscriptions = []
            resp_subscriptions = resp_data['data']['relationships']['subscriptions']['data']
            for sub in resp_subscriptions:
                self.subscriptions.append(StreamingSubscription(sub['id'], self.user_key))
        else:
            raise RuntimeError(f'API request returned an unexpected HTTP status, with content [{response.text}]')
        
        self.__log.info('get_status OK')
        return True


    def __repr__(self):
        return self.__str__()


    def __str__(self, detailed=True, prefix='  ├─', root_prefix=''):
        ret_val = f"{root_prefix}<factiva.analytics.{str(self.__class__).split('.')[-1]}"
        if self.id:
            ret_val += f"\n{prefix}id: <Hidden>"
            ret_val += f"\n{prefix}short_id: {tools.print_property(self.short_id)}"
        else:
            ret_val += f"\n{prefix}id: <NotCreated>"
            ret_val += f"\n{prefix}short_id: <NotCreated>"
        
        ret_val += f"\n{prefix}user_key: {self.user_key.__str__(detailed=False, prefix='  │  ├─')}"
        
        if self.query:
            ret_val += f"\n{prefix}query: {self.query.__str__(detailed=False, prefix='  │  ├─')}"
        else:
            ret_val += f"\n{prefix}query: <NotRetrieved>"

        if self.subscriptions:
            ret_val += f"\n{prefix}subscriptions:"
            n_sub = 1
            for sub in self.subscriptions:
                s_prefix = '  │  ├─' if n_sub < len(self.subscriptions) else '  │  └─'
                ret_val += f"\n{sub.__str__(table=True, prefix=s_prefix)}"
                n_sub += 1
        else:
            ret_val += f"\n{prefix}subscriptions: <NotCreated>"

        ret_val += f"\n{prefix.replace('├', '└')}status: {tools.print_property(self.status)}"

        return ret_val
