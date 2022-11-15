"""Factiva Core OAuthUser Class."""
from ..tools import load_environment_value


class OAuthUser:
    """Class that represents an API user. This entity is identifiable by a User Key.

    Parameters
    ----------
    key : str
        String containing the 32-character long APi Key. If not provided, the
        constructor will try to obtain its value from the FACTIVA_USERKEY
        environment variable.
    stats : bool, optional (Default: False)
        Indicates if user data has to be pulled from the server. This operation
        fills account detail properties along with maximum, used and remaining
        values. It may take several seconds to complete.

    """

    client_id = ''
    username = ''
    password = ''


    def __init__(
        self,
        client_id=None,
        username=None,
        password=None
    ):
        """Constructs the instance of the class."""
        if client_id is None:
            try:
                self.client_id = load_environment_value('FACTIVA_CLIENTID')
            except Exception as error:
                raise ValueError('Factiva client ID not provided and environment variable FACTIVA_CLIENTID not set.') from error

        if username is None:
            try:
                self.username = load_environment_value('FACTIVA_USERNAME')
            except Exception as error:
                raise ValueError('Factiva username not provided and environment variable FACTIVA_USERNAME not set.') from error

        if password is None:
            try:
                self.password = load_environment_value('FACTIVA_PASSWORD')
            except Exception as error:
                raise ValueError('Factiva password not provided and environment variable FACTIVA_PASSWORD not set.') from error


    def get_auth_token(self):
        """To be implemented"""


    def get_jwt_token(self):
        """To be implemented"""


    def __print_property__(self, property_value) -> str:
        if isinstance(property_value, int):
            pval = f'{property_value:,d}'
        else:
            pval = property_value
        return pval


    def __repr__(self):
        """Return a string representation of the object."""
        return self.__str__()


    def __str__(self, detailed=True, prefix='  |-', root_prefix=''):
        pprop = self.__dict__.copy()

        ret_val = f'{root_prefix}{str(self.__class__)}\n'
        if detailed:
            ret_val += '\n'.join((f'{prefix}{item} = {self.__print_property__(pprop[item])}' for item in pprop))
        else:
            ret_val += f'{prefix}...'
        return ret_val
