"""Implement Stream Class definition."""
from typing import List

from factiva.core import UserKey, StreamResponse, const
from factiva.news.bulknews import BulkNewsQuery
from factiva.core import req

from .subscription import Subscription


class Stream:
    """Represent a Stream workflow for Factiva API.

    Parameters
    ----------
    stream_id: str
        represents a given stream by its id
        if exists there is no need to have
        a query or a given snapshot id
    snapshot_id: str
        represents a snapshot by its id
        if exists it will be used
        to create a new stream
    query: str
        represents a query
        if exists it will be used
        to create a new stream
    user_key: Stream User
        constructor will asign a stream user which has the access
        to Pubsub client, authentication headers and urls

    Auth method 1 (All required)
    user_key: str
        if the user_key is not passed
        it can be created based on the api key param
    user_stats: bool
        if the user_key is not passed
        it can be created based on the request info param

    Auth method 2 (All required)
    user_id: str
        if the user_key is not passed
        it can be created based on the user id param
    client_id: str
        if the user_key is not passed
        it can be created based on the client id param
    password: str
        if the user_key is not passed
        it can be created based on the password param

    Examples
    --------
    Creating a new Stream directly:
        >>> stream_query_test = Stream(
            user_key='abcd1234abcd1234abcd1234abcd1234',
            user_stats=True,
            snapshot_id='<snapshot-id>',
            )
        >>> print(stream_query_test.create())
        >>>                       attributes                          id      relationships                                     type
        job_status     JOB_STATE_PENDING  dj-synhub-extraction-...                NaN                                          stream
        subscriptions                NaN  dj-synhub-extraction-...          {'data': [{'id': 'dj-synhub-extraction-...         stream

    """
    stream_id = None
    user_key = None
    snapshot_id = None
    listener = None
    subscriptions = dict()

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        stream_id=None,
        snapshot_id=None,
        query='',
        user_key=None,
        user_stats=False
    ):
        """Construct Stream class."""
        if (stream_id and (snapshot_id or query)):
            raise ValueError(
                'Not allowed stream id with query or snapshot'
            )
        self.stream_id = stream_id
        self.snapshot_id = snapshot_id
        self.query = BulkNewsQuery(query)
        self.user_key = UserKey.create_user_key(user_key, user_stats)
        if not self.user_key:
            raise RuntimeError('Undefined Stream User')

        if stream_id:
            self.set_all_subscriptions()

    @property
    def stream_url(self) -> str:
        """List Stream's URL address."""
        return f'{const.API_HOST}{const.API_STREAMS_BASEPATH}'

    @property
    def all_subscriptions(self) -> List[str]:
        """List all subscriptions to a stream."""
        return [sub.__repr__() for sub in self.subscriptions.values()]

    def get_info(self) -> StreamResponse:
        """Query a stream by its id.

        Returns
        -------
        StreamResponse which contains all information
            of the current sream

        Raises
        ------
        ValueError: when stream id is undefined
        RuntimeError: when the stream does not exists
        RuntimeError: when exists an unexpected HTTP error

        """
        if not self.stream_id:
            raise const.UNDEFINED_STREAM_ID_ERROR
        uri = '{}/{}'.format(self.stream_url, self.stream_id)
        headers = {
                'user-key': self.user_key.key
            }
        response = req.api_send_request(
            method='GET',
            endpoint_url=uri,
            headers=headers
        )
        if response.status_code == 200:
            response = response.json()
            return StreamResponse(data=response['data'], links=response.get('links', None))

        raise RuntimeError(response.text)

    def delete(self) -> StreamResponse:
        """Delete a stream.

        Returns
        -------
        StreamResponse which contains all information
            of the current which is expected
            to be CANCELLED

        Raises
        ------
        ValueError: when stream id is undefined
        RuntimeError: when the stream does not exists
        RuntimeError: when exists an unexpected HTTP error

        """
        if not self.stream_id:
            raise const.UNDEFINED_STREAM_ID_ERROR

        uri = f'{self.stream_url}/{self.stream_id}'
        headers = {
                'user-key': self.user_key.key,
                'content-type': 'application/json'
            }
        response = req.api_send_request(
            method='DELETE',
            endpoint_url=uri,
            headers=headers,
        )
        if response.status_code == 200:
            response = response.json()
            return StreamResponse(data=response['data'], links=response.get('links', None))

        if response == 404:
            raise RuntimeError('The Stream does not exist')

        raise const.UNEXPECTED_HTTP_ERROR

    def create(self) -> StreamResponse:
        """Create a stream subscription.

        There are two available options:
        Create a stream using a query
        Create a stream using a snapshot id

        Returns
        -------
        StreamResponse which contains all information
            of the current sream

        Raises
        ------
        ValueError: snapshot_id and query are undefined

        """
        if not self.snapshot_id and not self.query:
            raise ValueError('Snapshot id and query not found')
        if self.snapshot_id:
            return self._create_by_snapshot_id()

        return self._create_by_query()

    def create_subscription(self) -> str:
        """Create another subscription for an existing stream.

        Returns
        -------
        String which represents
            the new subscription id

        Raises
        ------
        RuntimeError: when unable to create a subscription

        """
        try:
            new_subscription = Subscription(stream_id=self.stream_id)
            headers = {
                'user-key': self.user_key.key
            }
            new_subscription.create(
                headers=headers
            )
            new_subscription.create_listener(self.user_key)
            self.subscriptions[new_subscription.id] = new_subscription
            return new_subscription.id
        except Exception as error:
            raise RuntimeError(
                f'''
                Unexpected error happened while
                creating the subscription: {error}
                '''
            )

    def delete_subscription(self, sus_id) -> bool:
        """Delete subscription for an existing stream.

        Parameters
        ----------
        sus_id:  str
            is the representation of a given
            subscription planned to be deleted

        Returns
        -------
        boolean which represents if the delete
            was successfully done

        Raises
        ------
        ValueError: when there is invalid subscription id
        RuntimeError: when unable to delete a subscription

        """
        if sus_id not in self.subscriptions:
            raise const.INVALID_SUBSCRIPTION_ID_ERROR
        try:
            if self.subscriptions[sus_id].delete(
                headers={'user-key': self.user_key.key}
            ):
                del self.subscriptions[sus_id]
                return True
        except Exception:
            raise RuntimeError('Unable to delete subscription')
        return False

    def create_default_subscription(self, response):
        """Create the default subscriptions at initialization.

        Adds the subscriptions to subscriptions dict

        Parameters
        ----------
        response :  dict
            is used for setting every subscription
            which exists inside the stream

        """
        for subscription in response['data']['relationships']['subscriptions']['data']:
            subscription_obj = Subscription(
                id=subscription['id'],
                stream_id=self.stream_id,
                subscription_type=subscription['type'],
                )
            subscription_obj.create_listener(self.user_key)
            self.subscriptions[subscription_obj.id] = subscription_obj

    def set_all_subscriptions(self):
        """Allow a user to set all subscriptions from a stream to local storage.

        Returns
        -------
        Dataframe which contains the state about the current stream

        Raises
        ------
        ValueError: when stream id is undefined

        """
        if not self.stream_id:
            raise const.UNDEFINED_STREAM_ID_ERROR
        uri = '{}/{}'.format(self.stream_url, self.stream_id)
        headers = {
                'user-key': self.user_key.key
            }
        response = req.api_send_request(
            method='GET',
            endpoint_url=uri,
            headers=headers
        )
        if response.status_code == 200:
            response = response.json()
            self.create_default_subscription(response)
        else:
            raise const.UNEXPECTED_HTTP_ERROR

    def consume_messages(
        self,
        callback=None,
        subscription_id=None,
        maximum_messages=None,
        batch_size=None,
        ack_enabled=False,
    ):
        """Consume messages (News) from a pubsub subscription in sync.

        Parameters
        ----------
        callback :  function
            is used for processing a message
        subscription_id :  str
            is used for connecting to pubsub
        maximum_messages : int
            is used for consuming a specific
            number of messages
        batch_size : int
            the limit of the batch expected
        ack_enabled :  boolean
            is used for acknowledging a message

        Raises
        ------
        ValueError: when subscription id is invalid

        """
        if subscription_id not in self.subscriptions:
            raise const.INVALID_SUBSCRIPTION_ID_ERROR
        self.subscriptions[subscription_id].consume_messages(
            callback=callback,
            maximum_messages=maximum_messages,
            batch_size=batch_size,
            ack_enabled=ack_enabled,
            )

    def consume_async_messages(
        self,
        callback=None,
        subscription_id=None,
        ack_enabled=False
    ):
        """Consume messages (News) from a pubsub subscription in async.

        Parameters
        ----------
        callback :  function
            is used for processing a message
        subscription_id :  str
            is used for connecting to pubsub
        ack_enabled :  boolean
            is used for acknowledging a message

        Raises
        ------
        ValueError: when subscription id is invalid

        """
        if subscription_id not in self.subscriptions:
            raise const.INVALID_SUBSCRIPTION_ID_ERROR
        self.subscriptions[subscription_id].consume_async_messages(
            callback=callback,
            ack_enabled=ack_enabled,
            )

    def _create_by_snapshot_id(self) -> StreamResponse:
        """Create by snapshot id that allows a user to create a stream subscription using a snapshot id.

        Returns
        -------
        StreamResponse
            which contains all information of the current stream

        Raises
        ------
        ValueError: When query is undefined
        RuntimeError: When API request returns unexpected error

        """
        print(self.snapshot_id)
        if not self.snapshot_id:
            raise ValueError('create fails: snaphot_id undefined')

        headers = {
                'user-key': self.user_key.key,
                'content-type': 'application/json'
            }
        uri = f'{const.API_HOST}{const.API_SNAPSHOTS_BASEPATH}/{self.snapshot_id}/streams'
        response = req.api_send_request(
            method='POST',
            endpoint_url=uri,
            headers=headers,
        )
        if response.status_code == 201:
            response = response.json()
            self.stream_id = response['data']['id']
            self.create_default_subscription(response)

            return StreamResponse(data=response['data'], links=response.get('links', None))

        raise const.UNEXPECTED_HTTP_ERROR

    def _create_by_query(self) -> StreamResponse:
        """Create by query that allows a user to create a stream subscription using a query.

        Returns
        -------
        StreamResponse
            which contains all information of the current stream

        Raises
        ------
        ValueError: When query is undefined
        RuntimeError: When API request returns unexpected error

        """
        if not self.query:
            raise ValueError('Streams query undefined in Create by query')

        base_query = self.query.get_base_query()
        streams_query = {
            "data": {
                "attributes": base_query['query'],
                "type": "stream"
                }
            }

        headers = {
                'user-key': self.user_key.key,
                'content-type': 'application/json'
            }
        response = req.api_send_request(
                method='POST',
                endpoint_url=self.stream_url,
                headers=headers,
                payload=streams_query,
            )

        if response.status_code == 201:
            response = response.json()
            self.stream_id = response['data']['id']
            self.create_default_subscription(response)

            return StreamResponse(data=response['data'], links=response.get('links', None))

        raise const.UNEXPECTED_HTTP_ERROR
