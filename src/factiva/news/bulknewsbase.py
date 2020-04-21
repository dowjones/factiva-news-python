from factiva.core import APIKeyUser
from factiva.helper import mask_string


class BulkNewsBase(object):

    api_user = None
    query = ''
    job_id = ''
    job_data = None
    data = None

    _request_datetime = ''
    _job_status = ''

    def __init__(
        self,
        api_user=None,
        request_userinfo=False,
        query=None,
        job_id=None
    ):
        if api_user is None:
            try:
                self.api_user = APIKeyUser(request_info=request_userinfo)
            except Exception:
                raise RuntimeError("User cannot be obtained from ENV variables")
        elif type(api_user) == str:
            try:
                self.api_user = APIKeyUser(api_user, request_info=request_userinfo)
            except Exception:
                raise RuntimeError("User cannot be obtained from the provided key.")
        elif type(api_user) == APIKeyUser:
            self.api_user = api_user
        else:
            raise RuntimeError("Unexpected api_user value")

        if query is not None:  # Validate correct syntax
            self.query = query
        else:
            self.query = ''

        self.job_id = ''
        self.job_data = None
        self.data = None

    def create_job(self):
        pass

    def get_job_status(self):
        pass

    def download_files(self):
        pass

    def load_data(self):
        pass

    def process_operation(self):
        pass

    def __str__(self):
        pprop = self.__dict__.copy()
        del pprop['api_user']
        masked_key = mask_string(self.api_user.api_key)
        user_class = str(self.api_user.__class__)

        ret_val = str(self.__class__) + '\n'
        ret_val += f'  api_User = {masked_key} ({user_class})\n'
        ret_val += '\n'.join(('  {} = {}'.format(item, pprop[item]) for item in pprop))
        return ret_val

    def __repr__(self):
        return self.__str__()
