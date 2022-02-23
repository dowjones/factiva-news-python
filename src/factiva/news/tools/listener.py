import datetime
import json
import os

from factiva.core import const, tools


class ListenerTools():

    def write_jsonl_line(self, file_prefix, action, file_suffix, message):
        output_filename = f'{file_prefix}_{action}_{file_suffix}.jsonl'
        output_filepath = os.path.join(const.FILES_DEFAULT_FOLDER,
                                       output_filename)
        with open(output_filepath, mode='a', encoding='utf-8') as fp:
            fp.write(
                f"{json.dumps(message, ensure_ascii=False, sort_keys=True)}\n")

    def save_json_file(self, message, subscription_id) -> bool:
        print("\n[ACTIVITY] Receiving messages (SYNC)...\n[0]", end='')

        tools.create_path_if_not_exist(const.FILES_DEFAULT_FOLDER)

        errorFile = os.path.join(const.FILES_DEFAULT_FOLDER, 'errors.log')
        erroMessage = f"{datetime.datetime.utcnow()}\tERR\t$$ERROR$$\t$$MESSAGE$$\n"

        counter = 0
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

            counter += 1
            if counter % 100 == 0:
                print(f'\n[{counter}]', end='')

        else:
            print(const.ACTION_CONSOLE_INDICATOR[const.ERR_ACTION], end='')
            with open(errorFile, mode='a', encoding='utf-8') as efp:
                efp.write(
                    erroMessage.replace('$$ERROR$$', 'InvalidMessage').replace(
                        '$$MESSAGE$$', json.dumps(message)))
            return False

        return True
