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
    """Class that represents a Dow Jones OAuth user.

    Parameters
    ----------
    client_id : str
        Assigned Client ID and communicated via the Welcome Letter.
    username : str
        Assigned Username and communicated via the Welcome Letter.
    password : str
        Assigned password and communicated via the Welcome Letter.

    """

    __API_OAUTH_BASEURL = const.API_ACCOUNT_OAUTH2_URL

    client_id = None
    """
    Property assigned at creation time based on the passed parameter or the content
    of the FACTIVA_CLIENTID environment variable. The value for this attribute is
    provided by Dow Jones based on an existing agreement.
    """

    username = None
    """
    Property assigned at creation time based on the passed parameter or the content
    of the FACTIVA_USERNAME environment variable. The value for this attribute is
    provided by Dow Jones based on an existing agreement.
    """

    password = None
    """
    Property assigned at creation time based on the passed parameter or the content
    of the FACTIVA_PASSWORD environment variable. The value for this attribute is
    provided by Dow Jones based on an existing agreement.
    """

    _access_token = None
    _id_token = None
    _refresh_token = None
    _jwt_token = None
    _jwt_expiration = None


    def __init__(
        self,
        client_id=None,
        username=None,
        password=None
    ):
        """Constructs the instance of the class."""
        if client_id is None:
            try:
                self.client_id = config.load_environment_value('FACTIVA_CLIENTID')
            except Exception as error:
                raise ValueError('client_id parameter not provided and environment variable FACTIVA_CLIENTID not set.') from error

        if username is None:
            try:
                self.username = config.load_environment_value('FACTIVA_USERNAME')
            except Exception as error:
                raise ValueError('username parameter not provided and environment variable FACTIVA_USERNAME not set.') from error

        if password is None:
            try:
                self.password = config.load_environment_value('FACTIVA_PASSWORD')
            except Exception as error:
                raise ValueError('password parameter not provided and environment variable FACTIVA_PASSWORD not set.') from error
        
        self._access_token = None
        self._id_token = None
        self._jwt_token = None
        self._jwt_expiration = None


    @property
    def current_jwt_token(self):
        """
        Returns a valid token to be used in the authorization HTTP header.
        Recalculates the JWT token automatically if needed.
        """
        if not self._jwt_token:
            self.get_jwt_token() 
        if self._jwt_expiration <= datetime.datetime.now(datetime.timezone.utc):
            self.get_jwt_token()
        return self._jwt_token


    def get_id_token(self) -> bool:
        """
        Requests an ID token to the DJ auth service and store the necessary
        information for furher requests in the instance properties.
        """
        # TODO: Validate params format
        #       Handle exceptions or unexpected responses like 403

        id_token_payload = {
            "client_id": self.client_id,
            "username": self.username,
            "grant_type": "password",
            "connection": "service-account",
            "scope": "openid service_account_id",
            "password": self.password
        }
        authn_response = req.api_send_request(
            method="POST",
            endpoint_url=self.__API_OAUTH_BASEURL,
            payload=id_token_payload,
            headers={}
        )
        response_body = authn_response.json()
        self._id_token = response_body['id_token']
        self._access_token = response_body['access_token']
        return True


    def get_jwt_token(self):
        """
        Requests a JWT Authorization token to the Factiva Auth service. The
        returned token is stored internally and available via the `current_jwt_token`
        property. Usual expiration is 1 hour.
        """
        if (not self._access_token) or (not self._id_token):
            self.get_id_token()

        # TODO: Validate id_token is valid and not yet expired
        #       Handle exceptions or unexpected responses like 403

        authz_token_payload = {
            "client_id": self.client_id,
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


    def __print_property__(self, property_value) -> str:
        if not property_value:
            pval = '<NotSet>'
        elif isinstance(property_value, int):
            pval = f'{property_value:,d}'
        elif isinstance(property_value, float):
            pval = f'{property_value:,f}'
        else:
            pval = property_value
        return pval


    def __repr__(self):
        """Return a string representation of the object."""
        return self.__str__()


    def __str__(self, detailed=False, prefix='  |-', root_prefix=''):
        pprop = self.__dict__.copy()
        del pprop['client_id']
        del pprop['password']
        del pprop['username']

        ret_val = f'{root_prefix}{str(self.__class__)}\n'
        ret_val += f'{prefix}username = {self.username}\n'
        if detailed:
            masked_client_id = tools.mask_string(self.client_id)
            masked_password = tools.mask_string(self.password)
            ret_val += f'{prefix}client_id = {masked_client_id}\n'
            ret_val += f'{prefix}password = {masked_password}\n'
            ret_val += '\n'.join((f'{prefix}{item} = {self.__print_property__(pprop[item])}' for item in pprop))
        else:
            ret_val += f'{prefix}...'
        return ret_val
