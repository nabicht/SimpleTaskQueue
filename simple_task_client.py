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

import requests
import json
from requests.compat import urljoin


def add_task(server, command, name=None, description=None, dependent_on=None, max_attempts=None, duration=None, log=None):
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
    url = urljoin(server, "task")
    if log is not None:
        log.debug("add_task post: %s  %s" % (str(url), str(payload)))
    r = requests.post(url, data=payload)
    if log is not None:
        log.debug("add_task received: %s" % r.text)
    response_dict = json.loads(r.text)
    return int(response_dict['task_id'])


def delete_task(server, task_id, log=None):
    url = urljoin(server, "task")
    payload = {'task_id': task_id}
    if log is not None:
        log.debug("delete_task delete: %s  %s" % (str(url), str(payload)))
    r = requests.delete(url, data=payload)
    if log is not None:
        log.debug("delete_task received: %s" % r.text)
    return json.loads(r.text)


def report_failed_attempt(server, runner_id, task_id, attempt_id, message=None, log=None):
    payload = {'runner_id': runner_id,
               'task_id': task_id,
               'attempt_id': attempt_id,
               'status': 'failed'}
    if message is not None:
        payload['message'] = message
    url = urljoin(server, "attempt")
    if log is not None:
        log.debug("report_failed_attempt put: %s  %s" % (str(url), str(payload)))
    r = requests.put(url, data=payload)
    if log is not None:
        log.debug("report_failed_attempt received: %s" % r.text)
    return json.loads(r.text)


def report_completed_attempt(server, runner_id, task_id, attempt_id, message=None, log=None):
    payload = {'runner_id': runner_id,
               'task_id': task_id,
               'attempt_id': attempt_id,
               'status': 'completed'}
    if message is not None:
        payload['message'] = message
    url = urljoin(server, "attempt")
    if log is not None:
        log.debug("report_completed_attempt put: %s  %s" % (str(url), str(payload)))
    r = requests.put(url, data=payload)
    if log is not None:
        log.debug("report_completed_attempt received: %s" % r.text)
    return json.loads(r.text)


def get_next_attempt(server, runner_id, log=None):
    payload = {'runner_id': runner_id}
    url = urljoin(server, 'attempt')
    if log is not None:
        log.debug("get_next_attempt get: %s  %s" % (str(url), str(payload)))
    r = requests.get(url, params=payload)
    if log is not None:
        log.debug("get_next_attempt received: %s" % r.text)
    return json.loads(r.text)


def get_tasks(server, task_type, log=None):
    url = urljoin(server, 'listtasks/%s' % task_type)
    if log is not None:
        log.debug("get_tasks get: %s" % str(url))
    r = requests.get(url)
    if log is not None:
        log.debug("get_tasks received: %s" % r.text)
    d = json.loads(r.text)
    tasks = {}
    if d is not None:
        for task_dict in d.get("data"):
            tasks[task_dict.get('task_id')] = task_dict
    return tasks


def get_todo_tasks(server, log=None):
    return get_tasks(server, "todo", log)


def get_inprocess_tasks(server, log=None):
    return get_tasks(server, "inprocess", log)


def get_failed_tasks(server, log=None):
    return get_tasks(server, "failed", log)


def get_completed_tasks(server, log=None):
    return get_tasks(server, "completed", log)
