"""Implement Subscription class."""
from factiva.core import (StreamUser, UserKey, const, factiva_logger,
                          get_factiva_logger, req)
from factiva.core.tools import load_environment_value

from .listener import Listener


class Subscription:
    """Represent a Subscription inside a stream.

    Class that represents a Subscription inside a stream.
    There a two possible operations for a Subscription:
    - Create new one based on an existing Stream
    - Delete an existing subscription from a Stream

    Parameters
    ----------
    url: str
        url used to create/delete a subscription
    stream_id: str
        represents a given stream by its id

    Raises
    ------
    ValueError: when a stream_id is undefined

    Examples
    --------
    Creating a new Subscription directly:
        >>> subscription = Subscription(<stream_id>)
        >>> created_subs = subscription.create(
                headers={'authorization': 'user-key'}
            )
        >>> print(created_subs)
        >>> { "id": "dj-synhub-extraction-*HH**", "type": "subscription" }

    """

    SUBSCRIPTION_IDX = 0
    id = None
    stream_id = None
    subscription_type = None
    listener = None

    def __init__(self, stream_id=None, id=None, subscription_type=None):
        """Instantiate listener class constructor."""
        if not stream_id:
            try:
                stream_id = load_environment_value('FACTIVA_STREAM_SUBSCRIPTION_ID')
            except Exception:
                raise const.UNDEFINED_STREAM_ID_ERROR

        self.url = f'{const.API_HOST}{const.API_STREAMS_BASEPATH}'
        self.stream_id = stream_id
        # pylint: disable=invalid-name
        self.id = id
        self.subscription_type = subscription_type
        self.log= get_factiva_logger()

    def __repr__(self):
        """Create string representation for Subscription Class."""
        return f'Subscription(id={self.id}, type={self.subscription_type})'

    @factiva_logger
    def create_listener(self, user):
        """Create a listener in a separate step.

        Create listener allows to create a listener
        in a separate step for avoiding undefined
        subscription id

        Parameters
        ----------
        user: StreamUser
            user which possess access to any
            credentials/client needed
            for listener

        Raises
        ------
        RuntimeError: when user is not a StreamUser

        """
        if not isinstance(user, StreamUser) and not isinstance(user, UserKey):
            raise RuntimeError('user is not a StreamUser instance')

        self.listener = Listener(
            subscription_id=self.id,
            stream_user=user
            )

    @factiva_logger
    def create(self, headers=None):
        """Create a subscription for a given stream instance.

        Create subscription allows a user to create
        another subscription to a given stream

        Parameters
        ----------
        headers: dict
            which contains the token/acces key for authorization

        Returns
        -------
        Data which contains:
        subscription's id and type created

        Raises
        ------
        ValueError: when a stream_id is undefined
        RuntimeError: when Unexpected API response happens

        """
        if not self.stream_id:
            raise ValueError(
                '''
                stream_id is not defined,
                it must be defined for creating a subscription
                '''
            )

        uri = '{}/{}/subscriptions'.format(self.url, self.stream_id)
        response = req.api_send_request(
            method='POST',
            endpoint_url=uri,
            headers=headers
            )
        if response.status_code == 201:
            response = response.json()
            data = response['data']
            self.id = data[self.SUBSCRIPTION_IDX]['id']
            self.subscription_type = data[self.SUBSCRIPTION_IDX]['type']
            return data

        raise RuntimeError('Unexpected API response')

    @factiva_logger
    def delete(self, headers=None) -> bool:
        """Delete subscription for a given stream.

        Delete subscription allows a user to delete
        a subscription to a given stream

        Parameters
        ----------
        headers: dict
            which contains the token/acces key for authorization

        Returns
        -------
        bool value which shows if the subscription is complete deleted

        Raises
        ------
        RuntimeError: when Unexpected API response happens

        """
        uri = '{}/{}/subscriptions/{}'.format(
            self.url,
            self.stream_id,
            self.id
            )
        response = req.api_send_request(
            method='DELETE',
            endpoint_url=uri,
            headers=headers
            )
        if response.status_code == 200:
            return True

        raise RuntimeError('Unexpected API response')

    def consume_messages(
        self,
        callback=None,
        maximum_messages=None,
        batch_size=None,
        ack_enabled=False,
    ):
        """Consume messages from a pubsub subscription in sync.

        Consume messages is a listener function
        which consumes the current messages (News)
        from a pubsub subscription in sync

        Parameters
        ----------
        callback :  function
            is used for processing a message
        maximum_messages: int
            is used for consuming a specific
            number of messages
        batch_size: int
            the limit of the batch expected
        ack_enabled :  boolean
            is used for acknowledging a message

        Raises
        ------
        RuntimeError: when listener is not yet init

        """
        if not self.listener:
            raise RuntimeError('uninitialized listener')
        if batch_size:
            self.listener.listen(
                callback=callback,
                maximum_messages=maximum_messages,
                batch_size=batch_size,
                ack_enabled=ack_enabled,
                )
        else:
            self.listener.listen(
                callback=callback,
                maximum_messages=maximum_messages,
                ack_enabled=ack_enabled,
                )

    def consume_async_messages(self, callback=None, ack_enabled=False):
        """Consume async messages is a listener function.

        Consume async messages is a listener function
        which consumes the current messages (News)
        from a pub-sub subscription in async

        Parameters
        ----------
        callback :  function
            is used for processing a message
        ack_enabled :  boolean
            is used for acknowledging a message

        Raises
        ------
        RuntimeError: when listener is not yet init

        """
        if not self.listener:
            raise RuntimeError('uninitialized listener')
        self.listener.listen_async(
            callback=callback,
            ack_enabled=ack_enabled,
        )
