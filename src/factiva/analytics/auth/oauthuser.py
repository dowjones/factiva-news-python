"""
This module contains classes and tools to instantiate and support the lifecycle
of a OAuth user instance in the context of Factiva Analytics API.
"""

from ..common import config
from ..common import tools
from ..common import const
from ..common import req
import base64
import datetime

class OAuthUser:
    """
    Class that represents a Dow Jones OAuth user.

    Parameters
    ----------
    client_id : str
        Assigned Client ID and communicated via the Welcome Letter. Retrieves
        the value from the ENV variable ``FACTIVA_CLIENTID`` it not provided.
    username : str
        Assigned Username and communicated via the Welcome Letter. Retrieves
        the value from the ENV variable ``FACTIVA_USERNAME`` it not provided.
    password : str
        Assigned password and communicated via the Welcome Letter. Retrieves
        the value from the ENV variable ``FACTIVA_PASSWORD`` it not provided.

    Examples
    --------
    Create an ``OAuthUser`` instance from ENV variables and assign the JWT
    token to a request headers dictionary.
    
    .. code-block:: python

        from factiva.analytics import OAuthUser
        o = OAuthUser()
        headers = {
            'Authorization': f'Bearer {o.current_jwt_token}'
        }

    Shows the relevant properties of a ``OAuthUser`` instance.

    .. code-block:: python

        from factiva.analytics import OAuthUser
        o = OAuthUser()
        o

    output

    .. code-block::

        <'factiva.analytics.OAuthUser'>
        |-client_id = ****************************4Cs6
        |-username = 9ZZZ000000-svcaccount@dowjones.com
        |-password = ************gRk3
        |-token_status = not_authenticated

    """

    __API_OAUTH_BASEURL = const.API_ACCOUNT_OAUTH2_URL

    _client_id = None
    _username = None
    _password = None
    _access_token = None
    _id_token = None
    _id_expiration = None
    _refresh_token = None
    _jwt_token = None
    _jwt_expiration = None


    def __init__(
        self,
        client_id:str=None,
        username:str=None,
        password:str=None
    ):
        """Constructs the instance of the class."""
        
        if client_id is None:
            try:
                self._client_id = config.load_environment_value('FACTIVA_CLIENTID')
            except Exception as error:
                raise ValueError('client_id parameter not provided and environment variable FACTIVA_CLIENTID not set.') from error
        else:
            if not isinstance(client_id, str):
                raise ValueError('The client_id param must be a string')
            else:
                self._client_id = client_id

        if username is None:
            try:
                self._username = config.load_environment_value('FACTIVA_USERNAME')
            except Exception as error:
                raise ValueError('username parameter not provided and environment variable FACTIVA_USERNAME not set.') from error
        else:
            if not isinstance(username, str):
                raise ValueError('The username param must be a string')
            else:
                self._username = username

        if password is None:
            try:
                self._password = config.load_environment_value('FACTIVA_PASSWORD')
            except Exception as error:
                raise ValueError('password parameter not provided and environment variable FACTIVA_PASSWORD not set.') from error
        else:
            if not isinstance(password, str):
                raise ValueError('The password param must be a string')
            else:
                self._password = password
        
        self._access_token = None
        self._id_token = None
        self._id_expiration = None
        self._jwt_token = None
        self._jwt_expiration = None


    @property
    def current_jwt_token(self):
        """
        Returns a valid token to be used in the ``Authorization`` HTTP header.
        Recalculates the JWT token automatically if needed.
        """
        if not self._jwt_token:
            self.get_jwt_token() 
        if self._jwt_expiration <= datetime.datetime.now(datetime.timezone.utc):
            self.get_jwt_token()
        return self._jwt_token


    @property
    def token_status(self) -> str:
        """
        Provides the current token status:

        - ``not_authenticated`` (``get_id_token()`` has not been executed)
        - ``id_token_expired`` (previously obtained ID token has expired)
        - ``not_authorized`` (``get_jwt_token()`` has not been executed)
        - ``jwt_token_expired`` (previously obtained JWT token has expired)
        - ``OK`` (token is ready for authenticated requests)
        """
        if not self._id_token:
            return 'not_authenticated'
        elif self._id_expiration <= datetime.datetime.now(datetime.timezone.utc):
            return 'id_token_expired'
        elif not self._jwt_token:
            return 'not_authorized'
        elif self._jwt_expiration <= datetime.datetime.now(datetime.timezone.utc):
            return 'jwt_token_expired'
        return 'OK'


    def get_id_token(self) -> bool:
        """
        Requests an ID token to the DJ auth service (authentication operation)
        and store the necessary information for furher requests in the relevant
        instance properties.

        Returns
        -------
        bool:
            ``True`` if the operation was completed successfully. ``False``
            otherwise.

        """
        id_token_payload = {
            "client_id": self._client_id,
            "username": self._username,
            "grant_type": "password",
            "connection": "service-account",
            "scope": "openid service_account_id",
            "password": self._password
        }
        authn_response = req.api_send_request(
            method="POST",
            endpoint_url=self.__API_OAUTH_BASEURL,
            payload=id_token_payload,
            headers={}
        )
        if authn_response.status_code == 200:
            response_body = authn_response.json()
            self._id_token = response_body['id_token']
            self._access_token = response_body['access_token']
            bearer_payload = eval(base64.b64decode(self._access_token.split('.')[1] + '==').decode('utf-8'))
            self._id_expiration = datetime.datetime.utcfromtimestamp(int(bearer_payload['exp'])).replace(tzinfo=datetime.timezone.utc)
            return True
        elif authn_response.status_code == 403:
            raise PermissionError('Invalid user credentials')
        else:
            raise RuntimeError('Unspecified error')


    def get_jwt_token(self) -> bool:
        """
        Requests a JWT Authorization token to the Factiva Auth service. The
        returned token is stored internally and available via the
        ``current_jwt_token`` property. Usual expiration is 1 hour
        (3600 seconds).

        Returns
        -------
        bool:
            ``True`` if the operation was completed successfully. ``False``
            otherwise.

        """
        if (not self._access_token) or (not self._id_token):
            self.get_id_token()

        # TODO: Validate id_token is valid and not yet expired
        #       Handle exceptions or unexpected responses like 403

        authz_token_payload = {
            "client_id": self._client_id,
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "connection": "service-account",
            "scope": "openid pib",
            "access_token": self._access_token,
            "assertion": self._id_token
        }
        authz_response = req.api_send_request(
            method="POST",
            endpoint_url=self.__API_OAUTH_BASEURL,
            payload=authz_token_payload,
            headers={}
        )
        response_body = authz_response.json()
        self._jwt_token = response_body["access_token"]
        bearer_payload = eval(base64.b64decode(self._jwt_token.split('.')[1] + '==').decode('utf-8'))
        self._jwt_expiration = datetime.datetime.utcfromtimestamp(int(bearer_payload['exp'])).replace(tzinfo=datetime.timezone.utc)
        return True


    def __repr__(self):
        """Return a string representation of the object."""
        return self.__str__()


    def __str__(self, detailed=True, prefix='  ├─', root_prefix=''):
        masked_clientid = tools.mask_string(self._client_id)
        ret_val = f"{root_prefix}<'factiva.analytics.{str(self.__class__).split('.')[-1]}\n"
        ret_val += f'{prefix}client_id: {masked_clientid}\n'
        ret_val += f'{prefix}username: {self._username}\n'
        if detailed:
            masked_password = tools.mask_string(self._password)
            ret_val += f'{prefix}password: {masked_password}\n'
            ret_val += f'{prefix[0:-2]}└─token_status: {self.token_status}\n'
        else:
            ret_val += f'{prefix}token_status: {self.token_status}\n'
            ret_val += f"{prefix[0:-2]}└─..."
        return ret_val
