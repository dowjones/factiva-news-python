from ..auth import UserKey
from ..common import tools, config


class SnapshotBaseJobResponse():
    job_id: str = None
    job_link: str = None
    job_state: str = None


    def __init__(self, job_id:str=None) -> None:
        self.job_id = job_id


    def __repr__(self):
        return self.__str__()


    def __str__(self, detailed=True, prefix='  |-', root_prefix=''):
        ret_val = f"{root_prefix}<factiva.analytics.{str(self.__class__).split('.')[-1]}\n"
        ret_val += f"{prefix}job_id: {tools.print_property(self.job_id)}\n"
        ret_val += f"{prefix}job_link: {tools.print_property(self.job_link)}\n"
        ret_val += f"{prefix}job_state: {tools.print_property(self.job_state)}\n"
        return ret_val



class SnapshotBaseQuery():
    where: str = None
    includes: list[str] = None
    include_lists: list[dict] = None
    excludes: list[str] = None
    exclude_lists: list[dict] = None

    # TODO: Consider implementing a SQL validation functionality to ensure 
    # fields are valid. There's sample projects doing something similar.
    # https://github.com/David-Wobrock/sqlvalidator


    def __init__(
        self,
        where=None,
        includes:dict=None,
        include_lists:dict=None,
        excludes:dict=None,
        exclude_lists:dict=None
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

        if include_lists:  # TODO: Validate data structure
            self.include_lists = tools.parse_field(include_lists, 'includes')

        if excludes:
            self.excludes = tools.parse_field(excludes, 'excludes')

        if exclude_lists:  # TODO: Validate data structure
            self.exclude_lists = tools.parse_field(exclude_lists, 'excludes')


    def get_payload(self) -> dict:
        query_dict = {
            "query": {
                "where": self.where
            }
        }

        if self.includes:
            query_dict["query"].update({"includes": self.includes})

        if self.excludes:
            query_dict["query"].update({'excludes': self.excludes})

        if self.include_lists:
            query_dict["query"].update({"includesList": self.includes})

        if self.exclude_lists:
            query_dict["query"].update({'excludesList': self.excludes})

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
        ret_val += f"\n{prefix}excludes: "
        ret_val += f"\n{prefix.replace('├', '│')[0:-1]}  └─{len(self.excludes.keys())} conditions" if self.excludes else "<NotSet>"
        ret_val += f"\n{prefix}include_lists: "
        ret_val += f"\n{prefix.replace('├', '│')[0:-1]}  └─{len(self.include_lists.keys())} conditions" if self.include_lists else "<NotSet>"
        ret_val += f"\n{prefix.replace('├', '└')}exclude_lists: "
        ret_val += f"\n{prefix.replace('├', '│')[0:-1]}  └─{len(self.exclude_lists.keys())} conditions" if self.exclude_lists else "<NotSet>"
        # else:
        #     ret_val += f"\n{prefix.replace('├', '└')}..."
        return ret_val



class SnapshotBase():

    __JOB_BASE_URL = None
    __SUBMIT_URL = None
    __GETINFO_URL = None
    __GETLIST_URL = None
    __log = None

    user_key: UserKey = None
    job_response: SnapshotBaseJobResponse = None
    query: SnapshotBaseQuery = None

    def __init__(
        self,
        user_key=None,
        query=None,
        job_id=None
    ) -> None:
        if isinstance(user_key, UserKey):
            self.user_key = user_key
        else:
            self.user_key = UserKey(user_key)

        if query and job_id:
            raise ValueError("The query and job_id parameters cannot be assigned simultaneously")

        # if (not query) and (not job_id):
        #     raise ValueError("Paramters query or job id are required")


    def submit_job(self, payload=None):  # TODO: NEXT!
        pass


    def get_job_response_base(self) -> bool:
        pass


    def __repr__(self):
        return self.__str__()


    def __str__(self, detailed=True, prefix='  ├─', root_prefix=''):
        ret_val = f"{root_prefix}<factiva.analytics.{str(self.__class__).split('.')[-1]}\n"
        ret_val += f"{prefix}user_key: {self.user_key.__str__(detailed=False, prefix='  │  ├─')}\n"
        if self.query:
            ret_val += f"{prefix}query: {self.query.__str__(detailed=False, prefix='  │  ├─')}\n"
        else:
            ret_val += f"{prefix}query: <NotRetrieved>\n"
        if self.job_response:
            ret_val += f"{prefix}job_response: {self.job_response.__str__(detailed=False, prefix='  │  ├─')}"
        else:
            ret_val += f"{prefix}job_response: <NotSubmitted>"
        return ret_val
