"""
Copyright 2019 Peter F Nabicht, Big Shoulders Software
Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 documentation files (the "Software"), to deal in the Software without restriction, including without
 limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 the Software, and to permit persons to whom the Software is furnished to do so, subject to the following
 conditions:
The above copyright notice and this permission notice shall be included in all copies or substantial portions
 of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED
 TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF
 CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 DEALINGS IN THE SOFTWARE.
"""

import argparse
import requests
import json
import shlex
import subprocess
import time
from urlparse import urljoin
import uuid


def add_task(server, command, name=None, description=None, dependent_on=None, max_attempts=None, duration=None):
    payload = {"command": command}
    if name is not None:
        payload["name"] = str(name)
    if description is not None:
        payload["description"] = str(description)
    if dependent_on is not None:
        dependent_on_list = []
        for d in dependent_on:
            dependent_on_list.append(str(d))
        payload["dependent_on"] = dependent_on_list
    if max_attempts is not None:
        payload['max_attempts'] = max_attempts
    if duration is not None:
        payload['duration'] = duration
    r = requests.post(urljoin(server,  "task"), data=payload)
    response_dict = json.loads(r.text)
    return str(response_dict['task_id'])


def delete_task(server, task_id):
    r = requests.delete(urljoin(server, "task"), data={'task_id': task_id})
    return json.loads(r.text)


def report_failed_attempt(server, runner_id, task_id, attempt_id, message=None):
    payload = {'runner_id': runner_id,
               'task_id': task_id,
               'attempt_id': attempt_id,
               'status': 'failed'}
    if message is not None:
        payload['message'] = message
    r = requests.put(urljoin(server, "attempt"), data=payload)
    return json.loads(r.text)


def report_completed_attempt(server, runner_id, task_id, attempt_id, message=None):
    payload = {'runner_id': runner_id,
               'task_id': task_id,
               'attempt_id': attempt_id,
               'status': 'completed'}
    if message is not None:
        payload['message'] = message
    r = requests.put(urljoin(server, "attempt"), data=payload)
    return json.loads(r.text)


def get_next_attempt(server, runner_id):
    payload = {'runner_id': runner_id}
    r = requests.get(urljoin(server, 'attempt'), params=payload)
    return json.loads(r.text)


def get_tasks(server, task_type):
    r = requests.get(urljoin(server, 'listtasks/%s' % task_type))
    d = json.loads(r.text)
    tasks = {}
    if d is not None:
        for task_dict in d.get("data"):
            tasks[task_dict.get('task_id')] = task_dict
    return tasks


def get_todo_tasks(server):
    return get_tasks(server, "todo")


def get_inprocess_tasks(server):
    return get_tasks(server, "inprocess")


def get_failed_tasks(server):
    return get_tasks(server, "failed")


def get_completed_tasks(server):
    return get_tasks(server, "completed")


def main(server, wait_seconds, runner_id, risky=False):
    print runner_id
    while True:
        attempt_info = get_next_attempt(server, runner_id)
        if attempt_info["status"] == "attempt":
            cmd = attempt_info['command']
            try:
                if risky:
                    subprocess.check_call(cmd, shell=True)
                else:
                    subprocess.check_call(shlex.split(cmd))
                report_completed_attempt(server, runner_id, attempt_info['task_id'], attempt_info['attempt_id'])
            except Exception as e:
                report_failed_attempt(server, runner_id, attempt_info['task_id'], attempt_info['attempt_id'], message=str(e))
        else:
            time.sleep(wait_seconds)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    temp_runner_id = str(uuid.uuid1())
    parser.add_argument('-s', action="store", dest="server_url", required=True,
                        help="the url of the SimpleTaskServer")
    parser.add_argument("-risky", action="store_true", dest="risky", required=False,
                        help="if present will run the client with shell=True, which is pretty damned risky. Not recommended.")
    parser.add_argument("-wait_time", action="store", dest="wait_time", type=float, nargs='?',
                        const=5.0, default=5.0, required=False,
                        help="the number of seconds to wait after no attempts to run before querying server for a new attempt. Defaults to 5.")
    parser.add_argument("-runner_id", action="store", dest="runner_id", nargs='?', const=temp_runner_id,
                        default=temp_runner_id, required=False,
                        help="The client's identifier. It should be unique across runners. If not defined, a unique id is randomly selected")
    args = parser.parse_args()
    main(args.server_url, args.wait_time, args.runner_id, risky=args.risky)
