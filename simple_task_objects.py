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

    def __init__(self, task_id, command, create_time, name="", desc="", duration=None, max_attempts=1, dependent_on=None):
        self.__task_id = task_id
        self.cmd = command
        self.name = name
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
    COMPLETED = 30
    FAILED = 40

    def __init__(self, runner, time_stamp):
        self._attempt_id = uuid.uuid1().hex  # to avoid the whole json serialization of a UUID, i'm just going straight to hex
        self.runner = runner
        self.start_time = time_stamp
        self._fail_reason = None
        self.completed_time = None
        self._status = 0

    def id(self):
        return self._attempt_id

    def mark_failed(self, reason):
        self._fail_reason = reason
        self._status = TaskAttempt.FAILED

    def mark_completed(self, time_stamp):
        self._status = TaskAttempt.COMPLETED
        self.completed_time = time_stamp

    def is_failed(self):
        return self._status == TaskAttempt.FAILED

    def is_completed(self):
        return self._status == TaskAttempt.COMPLETED

    def is_in_process(self):
        return self._status < TaskAttempt.COMPLETED

    def __hash__(self):
        return self._attempt_id
