import copy
import datetime
import json
import os

import pymongo
from factiva.core import const, tools
from google.cloud import bigquery
from pymongo import MongoClient

from .bq_schemas import *


class ListenerTools:

    def __init__(self):
        """Initialize class constructor."""
        self.table_id = None
        self.counter = 0
        self.bqclient = None
        self.log_line = ''
        self.mongodb_collection = None

    def write_jsonl_line(self, file_prefix, action, file_suffix, message):
        """Write a new Jsonl line.
        
        Parameters
        ----------
        file_prefix : str
            File prefix
        action : str
            Action from the stream response
        file_suffix : str
            File suffix
        message : str
            Message to be write on the file
        """
        output_filename = f'{file_prefix}_{action}_{file_suffix}.jsonl'
        output_filepath = os.path.join(const.FILES_DEFAULT_FOLDER,
                                       output_filename)
        with open(output_filepath, mode='a', encoding='utf-8') as fp:
            fp.write(
                f"{json.dumps(message, ensure_ascii=False, sort_keys=True)}\n")

    def save_json_file(self, message, subscription_id) -> bool:
        """Listener to save response into a jsonl
        
        Parameters
        ----------
        message : str
            Message to be stored
        subscription_id : str
            Suscription id from response
        
        Returns
        -------
        bool:
            Status from the process
        """

        print("\n[ACTIVITY] Receiving messages (SYNC)...\n[0]", end='')

        tools.create_path_if_not_exist(const.FILES_DEFAULT_FOLDER)

        errorFile = os.path.join(const.FILES_DEFAULT_FOLDER, 'errors.log')
        erroMessage = f"{datetime.datetime.utcnow()}\tERR\t$$ERROR$$\t$$MESSAGE$$\n"

        stream_short_id = subscription_id.split('-')[-3]
        current_hour = datetime.datetime.utcnow().strftime('%Y%m%d%H')

        if 'action' in message.keys():

            message = tools.format_timestamps(message)
            message = tools.format_multivalues(message)
            current_action = message['action']

            if current_action in const.ALLOWED_ACTIONS:
                print(const.ACTION_CONSOLE_INDICATOR[current_action], end='')
                self.write_jsonl_line(stream_short_id, current_action,
                                      current_hour, message)
            else:
                print(const.ACTION_CONSOLE_INDICATOR[const.ERR_ACTION], end='')
                with open(os.path.join(errorFile), mode='a',
                          encoding='utf-8') as efp:
                    efp.write(
                        erroMessage.replace('$$ERROR$$',
                                            'InvalidAction').replace(
                                                '$$MESSAGE$$',
                                                json.dumps(message)))

            self.counter += 1
            if self.counter % 100 == 0:
                print(f'\n[{self.counter}]', end='')

        else:
            print(const.ACTION_CONSOLE_INDICATOR[const.ERR_ACTION], end='')
            with open(errorFile, mode='a', encoding='utf-8') as efp:
                efp.write(
                    erroMessage.replace('$$ERROR$$', 'InvalidMessage').replace(
                        '$$MESSAGE$$', json.dumps(message)))
            return False

        return True

    def verify_bigquery_table(self):
        """Check if table id is set"""

        self.table_id = os.getenv('STREAMLOG_BQ_TABLENAME', None)
        if self.table_id is None:
            raise RuntimeError('Env variable STREAMLOG_BQ_TABLENAME not set')

    def save_on_bigquery_table(self, message, subscription_id) -> bool:
        """Listener to save response into a big query table
        
        Parameters
        ----------
        message : str
            Message to be stored
        subscription_id : str
            Suscription id from response
        
        Returns
        -------
        bool:
            Status from the process
        """
        self.verify_bigquery_table()
        self.bqclient = bigquery.Client()

        print("\n[ACTIVITY] Receiving messages (SYNC)...\n[0]", end='')
        tools.create_path_if_not_exist(const.FILES_DEFAULT_FOLDER)

        ret_val = False
        msg_an = ''
        _message = copy.deepcopy(message)

        try:
            if 'action' in _message.keys():
                msg_an = _message['an']
                _message = tools.format_timestamps(_message)
                _message = tools.format_multivalues(_message)
                _message = format_message_to_response_schema(_message)
                errors = self.bqclient.insert_rows_json(
                    self.table_id, [_message])
                if errors == []:
                    ret_val = True
                    current_action = _message['action']
                    if current_action in const.ALLOWED_ACTIONS:
                        self.log_line += const.ACTION_CONSOLE_INDICATOR[
                            current_action]
                    else:
                        self.log_line += const.ACTION_CONSOLE_INDICATOR[
                            const.ERR_ACTION]
                else:
                    self.log_line += '#'

                self.counter += 1
                if self.counter % 100 == 0:
                    self.log_line = ''

            else:
                print(const.ACTION_CONSOLE_INDICATOR[const.ERR_ACTION], end='')

        except Exception as e:
            log_path = const.FILES_DEFAULT_FOLDER
            if msg_an != '':
                file_name = os.path.join(log_path, f"{msg_an}.json")
            else:
                file_name = os.path.join(log_path,
                                         f"{tools.now_to_tssec()}.json")

            with open(file_name, mode='a', encoding='utf-8') as fp:
                fp.write(
                    f"{json.dumps(message, ensure_ascii=False, sort_keys=True)}\n"
                )
            ret_val = True
            self.log_line += '#'
            self.counter += 1

        print(self.log_line)
        return ret_val

    def get_mongodb_database(self):
        """Check and initaize the mongodb connection"""
        connection_string = os.getenv('MONGODB_CONNECTION_STRING', None)
        database_name = os.getenv('MONGODB_DATABASE_NAME', None)
        collection_name = os.getenv('MONGODB_COLLECTION_NAME', None)
        if connection_string is None or database_name is None or collection_name is None:
            raise RuntimeError('MongoDB environment vars are not set')

        client = MongoClient(connection_string)
        database = client[database_name]
        self.mongodb_collection = database[collection_name]

    def save_on_mongodb(self, message, subscription_id) -> bool:
        """Listener to save response into a mongodb table
        
        Parameters
        ----------
        message : str
            Message to be stored
        subscription_id : str
            Suscription id from response
        
        Returns
        -------
        bool:
            Status from the process
        """

        self.get_mongodb_database()

        print("\n[ACTIVITY] Receiving messages (SYNC)...\n[0]", end='')
        tools.create_path_if_not_exist(const.FILES_DEFAULT_FOLDER)

        errorFile = os.path.join(const.FILES_DEFAULT_FOLDER, 'errors.log')
        erroMessage = f"{datetime.datetime.utcnow()}\tERR\t$$ERROR$$\t$$MESSAGE$$\n"

        if 'action' in message.keys():

            message = tools.format_timestamps_mongodb(message)
            message = tools.format_multivalues(message)
            current_action = message['action']

            if current_action in const.ALLOWED_ACTIONS:
                print(const.ACTION_CONSOLE_INDICATOR[current_action], end='')
                self.mongodb_collection.insert_one(message)

            else:
                print(const.ACTION_CONSOLE_INDICATOR[const.ERR_ACTION], end='')
                with open(os.path.join(errorFile), mode='a',
                          encoding='utf-8') as efp:
                    efp.write(
                        erroMessage.replace('$$ERROR$$',
                                            'InvalidAction').replace(
                                                '$$MESSAGE$$',
                                                json.dumps(message)))

            self.counter += 1
            if self.counter % 100 == 0:
                print(f'\n[{self.counter}]', end='')

        else:
            print(const.ACTION_CONSOLE_INDICATOR[const.ERR_ACTION], end='')
            with open(errorFile, mode='a', encoding='utf-8') as efp:
                efp.write(
                    erroMessage.replace('$$ERROR$$', 'InvalidMessage').replace(
                        '$$MESSAGE$$', json.dumps(message)))
            return False

        return True