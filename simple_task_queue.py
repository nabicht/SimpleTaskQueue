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

import datetime
import uuid
import collections


# Task states are either to be done or complete
# TaskAttempt states are in started, confirmed, in-process, completed, failed

class Task(object):

    def __init__(self, task_id, command, name="", desc="", duration=None, max_attempts=5):
        self.__task_id = task_id
        self.cmd = command
        self.name = name
        self.desc = desc
        self.duration = duration
        self.max_attempts = max_attempts
        self.created_time = datetime.datetime.now()
        self._attempts = collections.OrderedDict()
        self._most_recent_attempt = None

    def attempt_task(self, runner, time_stamp):
        """
        Creates a new attempt and returns the attempt that was created.

        Will return None if cannot be attempted because above max attempts.
        """
        if len(self._attempts) > self.max_attempts:
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

    def __hash__(self):
        return self.task_id


class TaskAttempt:
    STARTED = 0
    COMPLETED = 30
    FAILED = 40

    def __init__(self, runner, time_stamp):
        self._attempt_id = uuid.uuid1()
        self.runner = runner
        self.start_time = time_stamp
        self._status = TaskAttempt.STARTED
        self._fail_reason = None
        self._completed_time = None

    def id(self):
        return self._attempt_id

    def mark_failed(self, reason):
        self._fail_reason = reason
        self._status = TaskAttempt.FAILED

    def mark_completed(self, time_stamp):
        self._status = TaskAttempt.COMPLETED
        self._completed_time = time_stamp

    def has_failed(self):
        return self._status == TaskAttempt.FAILED

    def __hash__(self):
        return self._attempt_id


class TaskQueue(object):

    def __init__(self):
        pass

    def next_task(self):
        raise NotImplementedError

    def task(self, task_id):
        raise NotImplementedError

    def add_task(self, task):
        raise NotImplementedError

    def remove_task(self, task_id):
        raise NotImplementedError

    def __len__(self):
        raise NotImplementedError


class SimpleTaskQueue(TaskQueue):

    def __init__(self):
        TaskQueue.__init__(self)
        self._queue = collections.OrderedDict()

    def next_task(self):
        # this is nasty hack on ordereddict. I can't index it so I just iterate it and return first one
        for value in self._queue.itervalues():
            return value
        return None

    def task(self, task_id):
        return self._queue.get(task_id)

    def add_task(self, task):
        self._queue[task.task_id()] = task

    def remove_task(self, task_id):
        if task_id in self._queue:
            del self._queue[task_id]

    def __len__(self):
        return len(self._queue)


class OpenTasks(object):
    """
    When there is an attempt being run, it gets added to the open attempts queue.
    This is a separate queue for two reasons:
        1) it gives us a way to see if there are any jobs that didn't complete / failed that need to be run
        2) there is more "find the task and update the task" work with open tasks, so it separates out open tasks
            (which should be shorter in high task throughput world) from open tasks.
    Since there is more checking/updating, i'm making this a dict, but we care about order added, so going
     with ordereddict.

    tasks that have an expected duration associated are being kept separate from those without a duration.
    """

    # TODO this needs to be cleaned up to handle in-process tasks

    def __init__(self):
        # two main containers:
        #  1) tasks with a duration
        #  2) tasks without a duration
        self._durations = collections.OrderedDict()
        self._no_durations = collections.OrderedDict()

    def task_to_retry(self, current_time):
        """
        Tasks get retried if:
            1) previous attempt failed (and they are allowed to have more attempts)
            2) current attempt has been running for longer than expected duration

        If mulitple tasks match the above critera then the oldest one gets returned.

        If multiple tasks have the same age and need to be retried there are no more tie
         breakers. One just gets returned.

        If there are tasks that have run out of attempts, they are "failed" and get returned as second value
         in tuple

        :return: (Task, failed_tasks)
        """
        # since both queues are ordered dicts the first one in the dict should be the oldest
        #  so we need to get the first one to be redone from each dict and then take the oldest of those two
        failed_tasks = []
        no_duration = None
        for task in self._no_durations.itervalues():
            if task.most_recent_attempt().has_failed():
                if len(task.attempts) >= task.max_attempts:
                    failed_tasks.append(task)
                else:
                    no_duration = task
                    break

        with_duration = None
        for task in self._durations.itervalues():
            if task.most_recent_attempt().has_failed() or current_time - task.most_recent_attempt().start_time > task.duration:
                if len(task.attempts) >= task.max_attempts:
                    failed_tasks.append(task)
                else:
                    with_duration = task
                    break

        # arbitrarily returning no duration failure ahead of with duration here
        #  but I guess there is a logic to it -- if with duration is because of over duration maybe first job
        #   comes back successfully in the extra time gained by returning the other one first.
        if no_duration.created_time <= with_duration.created_time:
            return no_duration
        else:
            return with_duration

    def add_task(self, task):
        """
        If the task has an expected duration then add it to durations, otherwise add it to no durations
        """
        assert isinstance(task, Task)
        assert task.most_recent_attempt() is not None, "Cannot add task to OpenTasks because no current attempt"
        if task.duration is None:
            self._no_durations[task.task_id] = task
        else:
            self._durations[task.task_id] = task

    def remove_task(self, task):
        """
        remove the task from OpenTasks. If task doesn't exist in OpenTask then no-op.
        """
        assert isinstance(task, Task)
        if task.duration is None:
            if task.task_id in self._no_durations:
                del self._no_durations[task.task_id]
        else:
            if task.task_id in self._durations:
                del self._no_durations[task.task_id]

    def get_task(self, task_id):
        # the task doesn't exist here then return none
        task = self._durations.get(task_id)
        if task is None:
            task = self._no_durations.get(task_id)
        return task


class TaskManager(object):

    def __init__(self, logger):
        self._todo_queue = SimpleTaskQueue()
        self._in_process = OpenTasks()
        self._done = collections.OrderedDict()
        self._logger = logger

    def _move_task_to_done(self, task):
        self._in_process.remove_task(task)
        self._done[task.task_id] = task

    def start_next_attempt(self, runner, current_time):
        # if there is one in process that needs to be re-attempted then do that
        next_task, failed_tasks = self._in_process.task_to_retry(current_time)
        # for each failed task: 1) remove from in process, 2) add to done
        for task in failed_tasks:
            self._move_task_to_done(task)

        if next_task is not None:
            next_task.attempt_task(runner)
        else:  # if still no next task, get one from the queued up new tasks
            next_task = self._todo_queue.next_task()
            # and move this task from todo to in process & create attempt
            if next_task is not None:
                self._todo_queue.remove_task(next_task)
                next_task.attempt_task(runner)
                self._in_process.add_task(next_task)
        return next_task

    def _find_task(self, task_id, todo=False, in_process=False, done=False):
        task = None
        if task is None and todo:
            task = self._todo_queue.task(task_id)
        if task is None and in_process:
            task = self._in_process.get_task(task_id)
        if task is None and done:
            task = self._done.get(task_id)
        return task

    def fail_attempt(self, task_id, attempt_id):
        # need to fail the attempt
        # first find the task, should be in in process or done
        task = self._find_task(task_id, in_process=True, done=True)
        if task is not None:
            # fail the attempt
            task.get_attempt(attempt_id).mark_failed()
            # if attempts is > max attempts and the attempt that failed is the most recent one then move it to done
            if len(task.attempts) >= task.max_attempts and task.most_recent_attempt.id == attempt_id:
                self._move_task_to_done(task)

    def complete_attempt(self, task_id, attempt_id):
        task = self._find_task(task_id, in_process=True, done=True)
        if task is not None:
            task.get_attempt(attempt_id).mark_completed()
