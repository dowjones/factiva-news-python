from datetime import datetime
from factiva.core import APIKeyUser
from factiva.helper import mask_string
from factiva import helper


class BulkNewsBase(object):

    api_user = None

    _request_datetime = ''
    _job_status = ''

    def __init__(
        self,
        api_user=None,
        request_userinfo=False
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

    def submit_job(self, endpoint_url, payload):
        headers_dict = {
                'user-key': self.api_user.api_key,
                'Content-Type': 'application/json'
            }
        response = helper.api_send_request(method='POST', endpoint_url=endpoint_url, headers=headers_dict, payload=payload)
        return response

    def get_job_results(self, endpoint_url):
        headers_dict = {
                'user-key': self.api_user.api_key,
                'Content-Type': 'application/json'
            }
        response = helper.api_send_request(method='GET', endpoint_url=endpoint_url, headers=headers_dict)
        return response

    def download_file(self, endpoint_url, local_path):
        headers_dict = {
                'user-key': self.api_user.api_key
            }
        response = helper.api_send_request(method='GET', endpoint_url=endpoint_url, headers=headers_dict)
        open(local_path, 'wb').write(response.content)
        return response

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
        ret_val += '  '.join(('{} = {}\n'.format(item, pprop[item]) for item in pprop))
        return ret_val

    def __repr__(self):
        return self.__str__()


class BulkNewsQuery(object):

    where = ''
    includes = ''
    excludes = ''
    select_fields = ''

    def __init__(
        self,
        where,
        includes=None,
        excludes=None,
        select_fields=None
    ):
        if type(where) == str:
            self.where = where  # TODO: Validate syntax if possible. At least it's a string.
        else:
            raise ValueError("Unexpected where clause.")

        if includes:
            if type(includes) == dict:
                self.includes = includes  # TODO: Validate syntax if possible
            elif type(includes) == str:
                self.includes = eval(includes)
            else:
                raise ValueError("Unexpected value for includes")

        if excludes:
            if type(excludes) == dict:
                self.excludes = excludes  # TODO: Validate syntax if possible
            elif type(excludes) == str:
                self.excludes = eval(excludes)
            else:
                raise ValueError("Unexpected value for excludes")

        if select_fields:
            if type(select_fields) == list:
                self.select_fields = select_fields  # TODO: Validate syntax if possible
            elif type(select_fields) == str:
                self.select_fields = eval(select_fields)
            else:
                raise ValueError("Unexpected value for select_fields")

    def get_base_query(self):
        query_dict = {
            'query': {
                'where': self.where
            }
        }

        if self.select_fields:
            query_dict['query'].update({'select': self.select_fields})

        if self.includes:
            query_dict['query'].update({'includes': self.includes})

        if self.excludes:
            query_dict['query'].update({'excludes': self.excludes})

        return query_dict


class BulkNewsJob(object):
    job_id = ''
    job_state = '',
    submitted_datetime = 0
    link = ''

    def __init__(self):
        self.job_id = ''
        self.job_state = ''
        self.submitted_datetime = datetime.now()
        self.link = ''
