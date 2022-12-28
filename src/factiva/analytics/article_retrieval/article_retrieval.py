"""
  Classes to interact with the Article Retrieval endpoints.
"""
from ..common import const
from ..common import req
from ..auth import OAuthUser


class ArticleRetrieval():
    """
    Allows to fetch articles against the Article Retrieval Service using
    the provided OAuthUser credentials.
    """
    __API_RETRIEVAL_ENDPOINT_BASEURL = f'{const.API_HOST}{const.API_RETRIEVAL_ENDPOINT_BASEURL}/'

    oauth_user = None
    """
    User instance wich provides the credentials to connect to the Article Retrieval API endpoints.
    """

    # TODO: When UIArticle is implemente, this will become a list of UIArticle
    last_retrieval_response = None
    """
    List that stores last retrieved articles.
    """


    def __init__(self, oauth_user=None) -> None:
        """Class constructor"""
        if not oauth_user:
          self.oauth_user = OAuthUser()
        else:
          self.oauth_user = oauth_user
        if (not isinstance(self.oauth_user.current_jwt_token, str)) or (len(self.oauth_user.current_jwt_token.split('.')) != 3):
          raise ValueError('Unexpected token for the OAuthUser instance')
        self.last_retrieval_response = []


    def retrieve_single_article(self, an):
        """
        Method that retrieves a single article to be displayed in a user interface.
        The returned variable is a JSON object. Additionally, the retrieved data is
        stored in the class atttribute `last_retrieval_response`.

        Parameters
        ----------
        an : str
            String containing the 32-character long article ID (AN).
            e.g. TRIB000020191217efch0001w

        Examples
        --------
        Creating a new ``ArticleRetrieval`` instance which reads credentials values from
        environment variables and retrieves a single article:

        .. code-block:: python

            from factiva.analytics import ArticleRetrieval
            ar = ArticleRetrieval()
            ar.retrieve_single_article(an='TRIB000020191217efch0001w')

        """
        if (not isinstance(an, str) or (not len(an) == 25)):
          raise ValueError('AN parameter not valid. Length should be 25 characters.')
        drn_ref = f'drn:archive.newsarticle.{an}'
        req_headers = {
            "Authorization": f'Bearer {self.oauth_user.current_jwt_token}'
        }
        article_response = req.api_send_request(
            method="GET",
            endpoint_url=f'{self.__API_RETRIEVAL_ENDPOINT_BASEURL}{drn_ref}',
            headers=req_headers
        )
        article_dict = article_response.json()
        self.last_retrieval_response = [article_dict]
        return article_dict


    # TODO: Implement a metod to retrieve multiple articles based on a list of ANs param


    def __repr__(self):
        """Create string representation for Snapshot Class."""
        return self.__str__()


    def __str__(self, detailed=True, prefix='  |-', root_prefix=''):
        """Create string representation for Snapshot Class."""
        child_prefix = '  |' + prefix
        ret_val = str(self.__class__) + '\n'

        ret_val += f'{prefix}oauth_user: '
        ret_val += self.oauth_user.__str__(detailed=False, prefix=child_prefix)
        ret_val += '\n'

        ret_val += f'{prefix}last_retrieval_response: <list>\n'
        if len(self.last_retrieval_response) > 0:
            ret_val += f'{child_prefix}list_items = {len(self.last_retrieval_response)}\n'
            ret_val += f'{child_prefix}list_ids = {[article["data"]["id"] for article in self.last_retrieval_response]}'
        else:
            ret_val += f'{child_prefix}<empty>'

        return ret_val


class UIArticle():
    """
    Class that represents a single article for visualization purposes. Methods
    and attributes are tailored for front-end environments .
    """
    # TODO: Implement this class that must represent an article and allows to render
    #       its content using multiple output formats (JSON, HTML and TEXT)
    pass

