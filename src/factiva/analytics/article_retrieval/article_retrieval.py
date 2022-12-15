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

    last_retrieval_response = None
    """
    Attribute to store the last retrieved article(s).
    """


    def __init__(self, oauth_user=None) -> None:
        """Class constructor"""
        if not oauth_user:
          self.oauth_user = OAuthUser()
        else:
          self.oauth_user = oauth_user
        if (not isinstance(self.oauth_user.current_jwt_token, str)) or (len(self.oauth_user.current_jwt_token) < 100):
          raise ValueError('Unexpected token for the OAuthUser instance')
        self.last_retrieval_response = []


    def retrieve_single_article(self, an=None):
        """
        Method that retrieves a single article to be displayed in a user interface.
        The returned variable is a JSON object. Additionally, the retrieved data is
        stored in the class atttribute `last_retrieval_response`.

        Parameters
        ----------
        an : str
            String containing the 32-character long article ID (AN)

        Examples
        --------
        Creating a new UserKey instance providing the key string explicitly and requesting to retrieve the latest account details:
            >>> ar = ArticleRetrieval()  # Take credentials from env variables
            >>> ar.retrieve_single_article(an='TRIB000020191217efch0001w')

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

class UIArticle():
    """
    Class that represents a single article for visualization purposes. Methods
    and attributes are tailored for front-end environments .
    """
    pass

