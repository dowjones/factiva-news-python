from ..auth import UserKey
from ..common import log, const, req

class SnapshotBase():

    __JOB_BASE_URL = None
    __SUBMIT_URL = None
    __GETINFO_URL = None
    __GETLIST_URL = None
    __log = None

    user_key: UserKey = None
    job_response = None
    query = None

    def __init__(
        self,
        user_key=None,
        query=None,
        job_id=None
    ):
        """Instantiate class."""
        if isinstance(user_key, UserKey):
            self.user_key = user_key
        else:
            self.user_key = UserKey(user_key)

        if query and job_id:
            raise ValueError("The query and job_id parameters cannot be assigned simultaneously")


    def submit_job(self, payload=None):  # TODO: NEXT!
        pass


    def get_job_response_base(self) -> bool:
        pass


    def __repr__(self):
        return self.__str__()


    def __str__(self, detailed=True, prefix='  |-', root_prefix=''):
        ret_val = f"{root_prefix}<factiva.analytics.{str(self.__class__).split('.')[-1]}\n"
        ret_val += f"{prefix}user_key: {self.user_key.__str__(detailed=False, prefix='  |  |-')}\n"
        if self.query:
            ret_val += f"{prefix}query: {self.query.__str__(detailed=True, prefix='  |  |-')}\n"
        else:
            ret_val += f"{prefix}query: <NotRetrieved>\n"
        if self.job_response:
            ret_val += f"{prefix}job_response: {self.job_response.__str__(detailed=True, prefix='  |  |-')}"
        else:
            ret_val += f"{prefix}job_response: <NotSubmitted>"
        return ret_val


class SnapshotBaseQuery():
    pass


class SnapshotBaseJobResponse():
    job_id = None
    job_link = None
    job_state = None


    def __init__(self, job_id:str=None) -> None:
        self.job_id = job_id


    def __repr__(self):
        return self.__str__()


    def __str__(self, detailed=True, prefix='  |-', root_prefix=''):
        ret_val = f"{root_prefix}<factiva.analytics.{str(self.__class__).split('.')[-1]}\n"
        ret_val += f"{prefix}job_id: {self.job_id}\n"
        ret_val += f"{prefix}job_link: {self.job_link}\n"
        ret_val += f"{prefix}job_state: {self.job_state}\n"
        return ret_val