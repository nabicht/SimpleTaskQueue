
import requests
import json
import shlex
import subprocess
import sys
import time
from urlparse import urljoin
import uuid


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
    r = requests.post(urljoin(server,  "addtask"), data=payload)
    response_dict = json.loads(r.text)
    return str(response_dict['task_id'])


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


def main(server, risky=False):
    wait_seconds = 5.0
    runner_id = uuid.uuid1()
    while True:
        attempt_info = get_next_attempt(server, runner_id)
        if attempt_info["status"] == "task":
            cmd = attempt_info['command']
            try:
                if risky:
                    subprocess.check_call(cmd, shell=True)
                else:
                    subprocess.check_call(shlex.split(cmd))
                report_completed_attempt(server, runner_id, attempt_info['task_id'], attempt_info['attempt_id'])
            except Exception as e:
                report_failed_attempt(server, runner_id, attempt_info['task_id'], attempt_info['attempt_id'], message=e.message)
        else:
            time.sleep(wait_seconds)


if __name__ == "__main__":
    server_address = sys.argv[1]
    run_risky = False
    if len(sys.argv) > 2:
        run_risky = bool(sys.argv[2])
    main(server_address, run_risky)
