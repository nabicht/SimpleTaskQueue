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
        self._attempts = collections.OrderedDict()
        self._most_recent_attempt = None

    def attempt_task(self, runner, time_stamp):
        """
        Creates a new attempt and returns the attempt that was created.

        Will return None if cannot be attempted because above max attempts.
        """
        if len(self._attempts) >= self.max_attempts:
            return None
        attempt = TaskAttempt(runner, time_stamp)
        self._attempts[attempt.id()] = self._most_recent_attempt = attempt
        return self._most_recent_attempt

    def get_attempt(self, attempt_id):
        return self._attempts.get(attempt_id)

    def most_recent_attempt(self):
        return self._most_recent_attempt

    def task_id(self):
        return self.__task_id

    def is_completed(self):
        try:
            values = self._attempts.itervalues()
        except AttributeError:
            values = self._attempts.values()
        completed = False
        for attempt in values:
            if attempt.is_completed():
                completed = True
                break
        return completed

    def is_in_process(self):
        """
        Returns that the task is in process if there have been one or more attempts, not attempt is complete and not all
         possible attempts have failed.

        :return: bool
        """
        return not self.is_completed() and not self.is_failed() and len(self._attempts) > 0

    def is_started(self):
        """
        Returns if the Task has been is_started. So if there is any attempt than it is is_started, regardless of if that
         attempt has been completed or failed.

        :return: bool
        """
        return len(self._attempts) > 0

    def is_failed(self):
        """
        to be failed, all attempts need to be failed and number of attempts >= max attempts

        :return: bool
        """
        if len(self._attempts) >= self.max_attempts:
            try:
                values = self._attempts.itervalues()
            except AttributeError:
                values = self._attempts.values()
            failed = True
            for attempt in values:
                if not attempt.is_failed():
                    failed = False
                    break
        else:
            failed = False
        return failed

    def open_time(self):
        """
        The amount of time between when a task is created and when it is completed. If the task fails or if the task
         has not had one Attempt that was completed yet, then it will be None.

        :return: total seconds as a float.
        """
        min_close_time = self.completed_time()
        if min_close_time is not None:
            return (min_close_time - self.created_time).total_seconds()
        else:
            return None

    def started_time(self):
        """
        The time the first attempt was started. None if no attempts.

        :return: datetime.datetime
        """
        start_time = None
        if len(self._attempts) > 0:
            # this is a hacky way to get fast access to the value of the first item in the dictionary
            start_time = self._attempts.get(next(iter(self._attempts))).start_time
        return start_time

    def completed_time(self):
        """
        Gets the time of the first completed attempt. If no completed attempt then it returns None.

        :return: datetime.datetime
        """
        try:
            values = self._attempts.itervalues()
        except AttributeError:
            values = self._attempts.values()
        min_close_time = None
        for attempt in values:
            if attempt.is_completed():
                if min_close_time is None:
                    min_close_time = attempt.completed_time
                else:
                    min_close_time = min(min_close_time, attempt.completed_time)
        return min_close_time

    def num_attempts(self):
        return len(self._attempts)

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
