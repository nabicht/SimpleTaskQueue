
import requests
import json

# TODO handle URL formatting better in all these calls, can't assume / passed in for server


def add_task(server, command, name=None, description=None, dependent_on=None, max_attempts=None, duration=None):
    payload = {"command": command}
    if name is not None:
        payload["name"] = str(name)
    if description is not None:
        payload["description"]=str(description)
    if dependent_on is not None:
        l = []
        for d in dependent_on:
            l.append(str(d))
        payload["dependent_on"] = l
    if max_attempts is not None:
        payload['max_attempts'] = max_attempts
    if duration is not None:
        payload['duration'] = duration
    r = requests.post(server + "addtask", data=payload)
    response_dict = json.loads(r.text)
    return str(response_dict['task_id'])


def fail_task(server, runner_id, task_id, attempt_id, message=None):
    payload = {'runner_id': runner_id,
               'task_id': task_id,
               'attempt_id': attempt_id,
               'status': 'failed'}
    if message is not None:
        payload['message'] = message
    requests.put(server + "attempt", data=payload)


def complete_attempt(server, runner_id, task_id, attempt_id, message=None):
    payload = {'runner_id': runner_id,
               'task_id': task_id,
               'attempt_id': attempt_id,
               'status': 'completed'}
    if message is not None:
        payload['message'] = message
    requests.put(server + "attempt", data=payload)


def get_next_attempt(server, runner_id):
    payload = {'runner_id': runner_id}
    r = requests.get(server + 'attempt', params=payload)
    return json.loads(r.text)
