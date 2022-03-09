"""Implement Listener class."""
import json
import time
from threading import Thread

from factiva.core import const, factiva_logger, get_factiva_logger, req
from factiva.core.tools import load_environment_value
from google.api_core.exceptions import GoogleAPICallError, NotFound


def default_callback(message, subscription_id):
    """Call to default callback function."""
    print('Subscription ID: {}: Message: {}'.format(subscription_id, message))


class Listener:
    r"""Class that represents a Listener for Google Pubsub.

    Parameters
    ----------
    stream_user : Stream User
        constructor will asign a stream user which has the access
        to the proper url and headers which are going to be used for:

        - Checking the exceeded documents
        - Consuming messages (articles) in sync
        - Consuming messages (articles) in async

    subscription_id :  str
        is used by Pubsub
        to consume messages in async/sync

    Examples
    --------
    Creating a new Listener directly:
        >>> listener = Listener(
                stream_user=StreamUser(
                    user_key='****************************1234',
                    request_info=False,
                    user_id='******-svcaccount@dowjones.com',
                    client_id='****************************5678',
                    password='*******'
                )
            )
        >>> def callback(message, subscription_id):
        >>>     print('Subscription ID: {}: Message: {}'.format(
                    subscription_id, message
                ))
        >>> print(listener.listen(
                callback,
                subscription_id='<subscription-id>',
                maximum_messages=10
                ))
        Received news message with ID: DJDN******************
        Subscription ID: dj-synhub-stream-********************-
        km******-filtered-******:
        Message: {'an': 'DJDN0*********************',
        'document_type': 'article', 'action': 'rep',
        'source_code': 'DJDN', 'source_name': 'Dow Jones ---- -----',
        'publication_date': '2021-05-20T08:00:10.255Z',
        'publication_datetime': '2021-05-20T08:00:10.255Z',
        'modification_date': '2021-05-20T08:04:56.175Z',
        'modification_datetime': '2021-05-20T08:02:54.000Z',
        'ingestion_datetime': '2021-05-20T08:00:13.000Z',
        'title': "----- ------ ------ ------ 2020",
        'snippet': '', 'body': "\nOn Thursday -- --- ----,
        --- Plc. announced its ----- ---- --- \n
        ......
        }

    """

    _check_exceeds_thread = None
    FIRST_OBJECT = 0

    def __init__(self, subscription_id=None, stream_user=None):
        """Instantiate listener class constructor."""
        if not subscription_id:
            try:
                subscription_id = load_environment_value('FACTIVA_STREAM_SUBSCRIPTION_ID')
            except Exception:
                raise const.UNDEFINED_SUBSCRIPTION_ERROR

        if not stream_user:
            raise ValueError('Undefined stream_user')

        self.user_key = stream_user
        self.subscription_id = subscription_id
        self.is_consuming = True
        self.limit_msg = None
        self.log= get_factiva_logger()

    @property
    def stream_id_uri(self):
        """Property for retrieving the stream id uri."""
        host = self.user_key.get_uri_context()
        stream_id = '-'.join(self.subscription_id.split("-")[:-2])
        return f'{host}/streams/{stream_id}'

    @factiva_logger
    def _check_account_status(self):
        """Check the account status for max allowed extracts done.

        Raises
        ------
        RuntimeError: When HTTP API Response is unexpected

        """
        # TODO: Implement using UserKey.get_stats()
        host = self.user_key.get_uri_context()
        headers = self.user_key.get_authentication_headers()
        limits_uri = f'{host}/accounts/{self.user_key.user_key}'
        limit_response = req.api_send_request(
            method='GET',
            endpoint_url=limits_uri,
            headers=headers
            )
        if limit_response.status_code == 200:
            limit_response = limit_response.json()
            self.limit_msg = limit_response['data']['attributes']['max_allowed_extracts']
        else:
            raise RuntimeError(
                '''
                Unexpected HTTP Response from API
                while checking for limits
                '''
                )
    
    @factiva_logger
    def _check_stream_status(self):
        """Check the stream status.

        if it has reached an
        exceeded status at some point

        Raises
        ------
        RuntimeError: When HTTP API Response is unexpected

        """
        headers = self.user_key.get_authentication_headers()
        response = req.api_send_request(
            method='GET',
            endpoint_url=self.stream_id_uri,
            headers=headers
            )
        if response.status_code == 200:
            response = response.json()
            job_status = response['data']['attributes']['job_status']
            if job_status == const.DOC_COUNT_EXCEEDED:
                self._check_account_status()
        else:
            raise RuntimeError('HTTP API Response unexpected')

    @factiva_logger
    def _check_exceeded(self):
        """Check exceeded time function.

        checks if the documents have been exceeded
        (max allowed extractions exceeded)
        """
        while self.is_consuming:
            print('Checking if extractions limit is reached')
            self._check_stream_status()
            time.sleep(const.CHECK_EXCEEDED_WAIT_SPACING)
        if not self.limit_msg:
            print('Job finished')
        else:
            print(
                '''
                OOPS! Looks like you\'ve exceeded the maximum number of
                documents received for your account ({}). As such, no
                new documents will be added to your stream\'s queue.
                However, you won\'t lose access to any documents that
                have already been added to the queue. These will continue
                to be streamed to you. Contact your account administrator
                with any questions or to upgrade your account limits.
                '''.format(self.limit_msg)
            )
    
    @factiva_logger
    def check_exceeded_thread(self):
        """Check exceeded thread function.

        creates threads for checking
        if the doc count has been exceeded

        """
        self._check_exceeds_thread = Thread(target=self._check_exceeded)
        self._check_exceeds_thread.start()

    # pylint: disable=too-many-arguments
    @factiva_logger
    def _pull_pubsub_messages(
        self,
        pubsub_client,
        pubsub_request,
        subscription_path,
        callback,
        ack_enabled
    ):
        """Pull messages from pubsub.

        if any message exist for
        the given subscription id

        Parameters
        ----------
        pubsub_client:  Google Pubsub client
            is used for consuming
            pubsub messages
        pubsub_request: object
            which represents a request for
            google pubsub
        subscription_path: str
            represents the path inside pubsub
            for the current subscription
        callback: function
            function executed
            by message received
        ack_enabled: bool
            a listener can consume a message
            again if ack_enabled is false,
            otherwise it won't

        """
        pubsub_messages = pubsub_client.pull(request=pubsub_request)
        if pubsub_messages and pubsub_messages.received_messages:
            for message in pubsub_messages.received_messages:
                pubsub_message = json.loads(message.message.data)
                self.log.info("Received news message with ID: {}".format(
                    pubsub_message['data'][self.FIRST_OBJECT]['id'])
                )
                news_message = pubsub_message['data'][self.FIRST_OBJECT]['attributes']
                callback_result = callback(
                    news_message,
                    self.subscription_id
                    )
                if ack_enabled:
                    pubsub_client.acknowledge(
                        subscription=subscription_path,
                        ack_ids=[message.ack_id]
                        )
                self.messages_count += 1
                if not callback_result:
                    return
    
    @factiva_logger
    def listen(
        self,
        callback=default_callback,
        maximum_messages=None,
        batch_size=10,
        ack_enabled=False
    ):
        """Listen function.

        listens the current messages (News)
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
        ack_enabled: boolean
            flag for acknowledge
            a message

        Raises
        ------
        ValueError:
            When maximum_messages is undefined
        GoogleAPICallError:
            When there is no valid instance to pull from
            When something unexpected happened with Pubsub client

        """
        if not maximum_messages:
            raise ValueError('undefined maximum messages to proceed')

        pubsub_client = self.user_key.get_client_subscription()
        self.check_exceeded_thread()

        streaming_credentials = self.user_key.fetch_credentials()
        subscription_path = pubsub_client.subscription_path(
            streaming_credentials['project_id'],
            self.subscription_id
            )
        print(
            '''
            Listeners for subscriptions have been set up
            and await message arrival.
            '''
            )

        self.messages_count = 0
        pubsub_request = {
            "subscription": subscription_path,
            "max_messages": batch_size,
            "return_immediately": False
            }
        while (maximum_messages is None) or (self.messages_count < maximum_messages):
            try:
                if maximum_messages is not None:
                    maximum_messages = min(
                        batch_size,
                        maximum_messages - self.messages_count
                        )
                    self._pull_pubsub_messages(
                        pubsub_client,
                        pubsub_request,
                        subscription_path,
                        callback,
                        ack_enabled
                    )
            except GoogleAPICallError as google_error:
                if isinstance(google_error, NotFound):
                    raise google_error
                print(
                    '''
                    Encountered a problem while trying to pull a message
                    from a stream. Error is as follows: {}
                    '''.format(str(google_error))
                    )
                print(
                    '''
                    Due to the previous error, system will pause 10 seconds.
                    System will then attempt to pull the message from
                    the stream again.
                    '''
                    )
                time.sleep(const.PUBSUB_MESSAGES_WAIT_SPACING)
                pubsub_client = self.user_key.get_client_subscription()

        self.is_consuming = False

    @factiva_logger
    def listen_async(self, callback=default_callback, ack_enabled=False):
        """Listen async function.

        listens the current messages (News)
        from a pubsub subscription in async

        Parameters
        ----------
        callback:  function
            is used for processing a message
        ack_enabled: boolean
            flag for acknowledge
            a message

        """
        def ack_message_and_callback(message):
            pubsub_message = json.loads(message.data)
            self.log.info("Received news message with ID: {}".format(
                pubsub_message['data'][self.FIRST_OBJECT]['id']
                )
            )
            news_message = pubsub_message['data'][self.FIRST_OBJECT]['attributes']
            callback(news_message, self.subscription_id)
            if ack_enabled:
                message.ack()

        pubsub_client = self.user_key.get_client_subscription()
        self.check_exceeded_thread()

        streaming_credentials = self.user_key.fetch_credentials()
        subscription_path = pubsub_client.subscription_path(
            streaming_credentials['project_id'],
            self.subscription_id
            )
        pubsub_client.subscribe(
            subscription_path,
            callback=ack_message_and_callback
            )
        print(
            '''
            Listeners for subscriptions have been set up
            and await message arrival.
            '''
            )
