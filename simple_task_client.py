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
import util
import logging
from collections import OrderedDict


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
        logger.debug("add_task post: %s  %s" % (str(url), str(payload)))
    r = requests.post(url, data=payload)
    if log is not None:
        logger.debug("add_task received: %s" % r.text)
    response_dict = json.loads(r.text)
    return str(response_dict['task_id'])


def delete_task(server, task_id, log=None):
    url = urljoin(server, "task")
    payload = {'task_id': task_id}
    if log is not None:
        logger.debug("delete_task delete: %s  %s" % (str(url), str(payload)))
    r = requests.delete(url, data=payload)
    if log is not None:
        logger.debug("delete_task received: %s" % r.text)
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
        logger.debug("report_failed_attempt put: %s  %s" % (str(url), str(payload)))
    r = requests.put(url, data=payload)
    if log is not None:
        logger.debug("report_failed_attempt received: %s" % r.text)
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
        logger.debug("report_completed_attempt put: %s  %s" % (str(url), str(payload)))
    r = requests.put(url, data=payload)
    if log is not None:
        logger.debug("report_completed_attempt received: %s" % r.text)
    return json.loads(r.text)


def get_next_attempt(server, runner_id, log=None):
    payload = {'runner_id': runner_id}
    url = urljoin(server, 'attempt')
    if log is not None:
        logger.debug("get_next_attempt get: %s  %s" % (str(url), str(payload)))
    r = requests.get(url, params=payload)
    if log is not None:
        logger.debug("get_next_attempt received: %s" % r.text)
    return json.loads(r.text)


def get_tasks(server, task_type, log=None):
    url = urljoin(server, 'listtasks/%s' % task_type)
    if log is not None:
        logger.debug("get_tasks get: %s" % str(url))
    r = requests.get(url)
    if log is not None:
        logger.debug("get_tasks received: %s" % r.text)
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


def main(server, wait_seconds, runner_id, log, risky=False):
    if log is not None:
        log.info("Started Runner: %s" % runner_id)
        log.info("Server: %s" % str(server))
        log.info("Wait Time: %0.6f" % wait_seconds)
        log.info("Risky: %s" % str(risky))
    while True:
        attempt_info = get_next_attempt(server, runner_id)
        if attempt_info["status"] == "attempt":
            cmd = attempt_info['command']
            if log is not None:
                log.info("Running: %s" % str(cmd))
            try:
                if risky:
                    subprocess.check_call(cmd, shell=True)
                else:
                    subprocess.check_call(shlex.split(cmd))
                report_completed_attempt(server, runner_id, attempt_info['task_id'], attempt_info['attempt_id'])
            except Exception as e:
                log.exception("Issue running and/or reporting.")
                report_failed_attempt(server, runner_id, attempt_info['task_id'], attempt_info['attempt_id'], message=str(e))
        else:
            if log is not None:
                log.info("No attempt to run. Waiting %0.6f" % wait_seconds)
            time.sleep(wait_seconds)


if __name__ == "__main__":
    # using OrderedDict so that use in strings below is in order I define
    log_level_str_to_level = OrderedDict([("DEBUG", logging.DEBUG),("INFO", logging.INFO),
                                          ("WARNING", logging.WARNING),("ERROR", logging.ERROR)])
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
    parser.add_argument("-verbose", action="store_true", dest="verbose", required=False,
                        help="if present will run client in verbose mode, with logging to console")
    parser.add_argument("-log", action="store_true", dest="log", required=False,
                        help="if present will log to file. Log file name can be defined with -logfile. Without that, runner-YYYYMMDD-HHmmss.log will be used.")
    parser.add_argument("-logfile", action="store", dest="logfile", nargs=1, required=False,
                        help="Optional argument that lets you define your log file name. Can also define location as this can take the entire path. Append is used in case file already exists. If this argument is used but -log is not, it will be ignored.")
    parser.add_argument("-loglevel", action="store", dest="loglevel", nargs=1, required=False,
                        help="Optionally define the log level if you are logging. Must use one of: %s. If -log is not used then this will be ignored. If not present log level defaults to INFO." % (", ".join(log_level_str_to_level.keys())))
    args = parser.parse_args()
    log_file_name = util.time_stamped_file_name(args.runner_id)
    log_file_level = None
    if args.log:
        log_file_level = logging.INFO
        if args.logfile is not None:
            log_file_name = args.logfile[0]
        if args.loglevel is not None:
            if args.loglevel[0] not in log_level_str_to_level:
                raise Exception("-loglevel set to %s. Must be one of: %s" % (args.loglevel[0], ", ".join(log_level_str_to_level.keys())))
            else:
                log_file_level = log_level_str_to_level.get(args.loglevel[0])
    log_console_level = None
    if args.verbose:
        log_console_level = logging.DEBUG
    logger = util.basic_logger(log_file_name=log_file_name, file_level=log_file_level, console_level=log_console_level)

    main(args.server_url, args.wait_time, args.runner_id, logger, risky=args.risky)
