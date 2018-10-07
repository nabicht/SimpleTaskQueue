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

import uuid
import collections

# Task states are either to be done or complete
# TaskAttempt states are in is_started, confirmed, in-process, completed, failed


class Task(object):

    def __init__(self, task_id, command, create_time, queue, name="", desc="", duration=None, max_attempts=1, dependent_on=None):
        self.__task_id = task_id
        self.cmd = command
        self.name = name
        self.queue = queue
        self.desc = desc
        self.duration = duration
        self.max_attempts = max_attempts
        self.created_time = create_time
        self.dependent_on = dependent_on if dependent_on is not None else []

    def task_id(self):
        return self.__task_id

    def __hash__(self):
        return self.task_id

    def to_json(self):
        return {'task_id': self.__task_id,
                'name': self.name,
                'command': self.cmd,
                'description': self.desc,
                'duration': self.duration,
                'max_attempts': self.max_attempts,
                'dependent_on': self.dependent_on}


class TaskAttempt:

    DEFAULT_STATUS = 0
    COMPLETED_STATUS = 50
    FAILED_STATUS = 100

    def __init__(self, attempt_id, start_time, runner, done_time, status, fail_reason):
        self._attempt_id = attempt_id
        self.runner = runner
        self.start_time = start_time
        self._fail_reason = fail_reason
        self.completed_time = done_time  # TODO change this to done_time and keep track fo time for failed as well
        self._status = status

    def id(self):
        return self._attempt_id

    def mark_failed(self, reason):
        self._fail_reason = reason
        self._status = TaskAttempt.FAILED_STATUS

    def mark_completed(self, time_stamp):
        self._status = TaskAttempt.COMPLETED_STATUS
        self.completed_time = time_stamp

    def is_failed(self):
        return self._status == TaskAttempt.FAILED_STATUS

    def is_completed(self):
        return self._status == TaskAttempt.COMPLETED_STATUS

    def is_in_process(self):
        return self._status < TaskAttempt.COMPLETED_STATUS

    def __hash__(self):
        return self._attempt_id
