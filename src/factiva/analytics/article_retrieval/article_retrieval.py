"""
  Classes to interact with the Article Retrieval endpoints.
"""
from ..common import const
from ..common import req
from ..common import articleparser as ap
from ..auth import OAuthUser


class ArticleRetrieval():
    """
    Allows to fetch articles against the Article Retrieval Service using
    the provided OAuthUser credentials.

    Parameters
    ----------
    oauth_user : OAuthUser
        An instance of an OAuthUser with working credentials. If not provided
        the user instance is created automatically from ENV variables.

    Examples
    --------
    Create an ArticleRetrieval instance.

    .. code-block:: python
    
        from factiva.analytics import ArticleRetrieval
        ar = ArticleRetrieval()
        ar

    .. code-block::

        <class 'factiva.analytics.article_retrieval.article_retrieval.ArticleRetrieval'>
          |-oauth_user: <class 'factiva.analytics.auth.oauthuser.OAuthUser'>
          |  |-client_id = fbwqyORz0te484RQTt0E7qj6Tooj4Cs6
          |  |-token_status = OK
          |  |-...

    """
    __API_RETRIEVAL_ENDPOINT_BASEURL = f'{const.API_HOST}{const.API_RETRIEVAL_ENDPOINT_BASEURL}/'

    oauth_user = None
    """
    User instance wich provides the credentials to connect to the Article Retrieval API endpoints.
    """

    # TODO: Clarify how royalties work at user level. If one-click per UIUser is enough, it will
    #       be possible to implement a 'cache' to avoid excess of retrievals when refreshing a
    #       page.
    # TODO: When UIArticle is implemente, this will become a list of UIArticle
    # retrieved_articles = {}
    # """
    # List that stores last retrieved articles.
    # """


    def __init__(self, oauth_user:OAuthUser=None) -> None:
        """Class constructor"""
        if not oauth_user:
          self.oauth_user = OAuthUser()
        else:
          self.oauth_user = oauth_user
        if (not isinstance(self.oauth_user.current_jwt_token, str)) or (len(self.oauth_user.current_jwt_token.split('.')) != 3):
          raise ValueError('Unexpected token for the OAuthUser instance')
        # self.retrieved_articles = {}


    def retrieve_single_article(self, an:str) -> dict:
        """
        Method that retrieves a single article to be displayed in a user interface.
        The requested item is initially retrieved from the . Additionally, the retrieved data is
        stored in the class atttribute ``last_retrieval_response``.

        Parameters
        ----------
        an : str
            String containing the 32-character long article ID (AN).
            e.g. WSJO000020221229eict000jh

        Returns
        -------
        dict:
            Python dict containing full articles' data. This will be replaced when
            the ``UIArticle`` class is implemented.

        Examples
        --------
        Creating a new ``ArticleRetrieval`` instance which reads credentials values from
        environment variables and retrieves a single article:

        .. code-block:: python

            from factiva.analytics import ArticleRetrieval
            ar = ArticleRetrieval()
            article = ar.retrieve_single_article(an='WSJO000020221229eict000jh')
            article

        output

        .. code-block::

            <class 'factiva.analytics.article_retrieval.article_retrieval.UIArticle'>
              |-an: WSJO000020221229eict000jh
              |-headline: Europe Taps Tech's Power-Hungry Data Centers to Heat Homes.
              |-source_code: WSJO
              |-source_name: The Wall Street Journal Online
              |-publication_date: 2022-12-29
              |-metadata: <dict> - [4] keys
              |-content: <dict> - [19] keys
              |-included: <list> - [0] items
              |-relationships: <dict> - [0] keys

        Raises
        ------
        PermissionError
            If the user doesn't have access to the requested content

        """
        if (not isinstance(an, str) or (not len(an) == 25)):
          raise ValueError('AN parameter not valid. Length should be 25 characters.')
        
        # if an in self.retrieved_articles.keys():
        #     return self.retrieved_articles[an]
        drn_ref = f'drn:archive.newsarticle.{an}'
        req_headers = {
            "Authorization": f'Bearer {self.oauth_user.current_jwt_token}'
        }
        article_response = req.api_send_request(
            method="GET",
            endpoint_url=f'{self.__API_RETRIEVAL_ENDPOINT_BASEURL}{drn_ref}',
            headers=req_headers
        )
        if article_response.status_code == 200:
            article_obj = UIArticle(article_response.json())
        elif article_response.status_code == 500:
            err_details = article_response.json()
            if 'errors' in err_details.keys():
                err_msg = err_details['errors'][0]['title']
                raise PermissionError(err_msg)
        # self.retrieved_articles = [article_obj]
        return article_obj


    # TODO: Implement a metod to retrieve multiple articles based on a param containing a list of ANs


    def __repr__(self):
        """Create string representation for this Class."""
        return self.__str__()


    def __str__(self, detailed=True, prefix='  ├─', root_prefix=''):
        """Create string representation for this Class."""
        child_prefix = '  │' + prefix
        ret_val = f"{root_prefix}<factiva.analytics.{str(self.__class__).split('.')[-1]}\n"

        ret_val += f'{prefix}oauth_user: '
        ret_val += self.oauth_user.__str__(detailed=False, prefix=child_prefix)
        ret_val += f'\n{prefix[0:-2]}└─'

        return ret_val


class UIArticle():
    """
    Class that represents a single article for visualization purposes. Methods
    and attributes are tailored for front-end environments.

    Parameters
    ----------
    article_dict : dict
        A python dict with the structure returned by the Dow Jones Article
        Retrieval service.
    
    Examples
    --------
    See ArticleRetrieval class examples.

    """
    # TODO: Improve implementation and add methods to render
    #       its content using multiple output formats (JSON, HTML and TEXT)
    # an, headline, snippet, source_name, source_code, retrieved_datetime
    # Methods - as_html, as_json, as_text

    an = None
    """Article unique identifier, also known as Accession Number"""
    headline = None
    """Article's headline, also known as title"""
    source_code = None
    """Article content creator's code. e.g. WSJO"""
    source_name = None
    """Article content creator's name. e.g. The Wall Street Journal Online"""
    publication_date = None
    """Article's publication date in ISO format as provided by the source.
       e.g. '2022-12-03'"""
    metadata = {}
    """Article's metadata dict. Contains Dow Jones Intelligent Identifiers
       among other codes."""
    content = {}
    """Article's content dict. Full text with annotations and other UI elements."""
    included = []
    """References to objects linked to a specific article"""
    relationships = {}
    """References to related objects"""


    def __init__(self, article_dict:dict) -> None:
        if not isinstance(article_dict, dict):
            raise ValueError('Param article_dict is not a python dict')
        if ((not 'data' in article_dict.keys()) or
           (not 'attributes' in article_dict['data']) or
           (not 'id' in article_dict['data']) or
           (not 'meta' in article_dict['data'])):
            raise ValueError('Unexpected dict structure')

        self.an = article_dict['data']['id']
        self.headline = ap.extract_headline(article_dict['data']['attributes']['headline'])
        self.source_code = article_dict['data']['attributes']['sources'][0]['code']
        self.source_name = article_dict['data']['attributes']['sources'][0]['name']
        self.publication_date = article_dict['data']['attributes']['publication_date']
        self.metadata = article_dict['data']['meta']
        self.content = article_dict['data']['attributes']
        if 'included' in article_dict.keys():
            self.included = article_dict['included']
        else:
            self.included = []
        if 'relationships' in article_dict['data'].keys():
            self.relationships = article_dict['data']['relationships']
        else:
            self.relationships = {}


    @property
    def txt(self) -> str:
        disp_txt = f"\n{self.headline}"
        disp_txt += f"\n\n{self.source_name}, {self.publication_date}, {self.metadata['metrics']['word_count']} words\n\n"
        disp_txt += ap.extract_body(self.content['body'][0], 'txt')
        disp_txt += f"{ap.extract_txt(self.content['copyright'])}"
        disp_txt += f"\nDocument identifier: {self.an}\n"
        return disp_txt


    @property
    def html(self) -> str:
        if 'logo' in self.content['sources'][0]:
            disp_txt = f"\n<img src='{self.content['sources'][0]['logo']['size'][0]['uri']}'>"
        disp_txt += f"\n<h1>{self.headline}<h1>"
        disp_txt += f"\n<p style='dateline'>{self.source_name}, {self.publication_date}, {self.metadata['metrics']['word_count']} words</p>"
        disp_txt += ap.extract_body(self.content['body'][0], 'html')
        disp_txt += f"\n<p style='copyright'>{ap.extract_txt(self.content['copyright'])}</p>"
        disp_txt += f"\n<p style='docid'>Document identifier: {self.an}</p>"
        return disp_txt


    def __repr__(self):
        """Create string representation for this Class."""
        return self.__str__()


    def __str__(self, detailed=True, prefix='  ├─', root_prefix=''):
        """Create string representation for this Class."""
        ret_val = f"{root_prefix}<factiva.analytics.{str(self.__class__).split('.')[-1]}\n"
        ret_val += f'{prefix}an: {self.an}\n'
        ret_val += f'{prefix}headline: {self.headline}\n'
        ret_val += f'{prefix}source_code: {self.source_code}\n'
        ret_val += f'{prefix}source_name: {self.source_name}\n'
        ret_val += f'{prefix}publication_date: {self.publication_date}\n'
        ret_val += f'{prefix}metadata: <dict> - [{len(self.metadata.keys())}] keys\n'
        ret_val += f'{prefix}content: <dict> - [{len(self.content.keys())}] keys\n'
        ret_val += f'{prefix}included: <list> - [{len(self.included)}] items\n'
        ret_val += f'{prefix[0:-2]}└─relationships: <dict> - [{len(self.relationships.keys())}] keys\n'
        return ret_val

