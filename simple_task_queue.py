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


# Custom Exceptions
class UnknownDependencyException(Exception):
    pass


# Task states are either to be done or complete
# TaskAttempt states are in started, confirmed, in-process, completed, failed

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

    def is_completed(self):
        completed = False
        for attempt in self._attempts.itervalues():
            if attempt.completed():
                completed = True
                break
        return completed

    def in_process(self):
        in_proc = False
        if not self.is_completed() and not self.failed():
            for attempt in self._attempts.itervalues():
                if attempt.in_process():
                    in_proc = True
                    break
        return in_proc

    def started(self):
        return len(self._attempts) > 0

    def failed(self):
        # to be failed, all attempts need to be failed and number of attempts >= max attempts
        if len(self._attempts) >= self.max_attempts:
            failed = True
            for attempt in self._attempts.itervalues():
                if not attempt.failed():
                    failed = False
                    break
        else:
            failed = False
        return failed

    def open_time(self):
        # TODO unit test
        min_close_time = self.finished_time()
        if min_close_time is not None:
            return (min_close_time - self.created_time).total_seconds()
        else:
            return None

    def started_time(self):
        # TODO unit test
        start_time = None
        if len(self._attempts) > 0:
            start_time = self._attempts[0].start_time
        return start_time

    def finished_time(self):
        # TODO unit test
        min_close_time = None
        for attempt in self._attempts.itervalues():
            if attempt.completed():
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
    STARTED = 0
    COMPLETED = 30
    FAILED = 40

    def __init__(self, runner, time_stamp):
        self._attempt_id = uuid.uuid1().hex  # to avoid the whole json serialization of a UUID, i'm just going straight to hex
        self.runner = runner
        self.start_time = time_stamp
        self._status = TaskAttempt.STARTED
        self._fail_reason = None
        self.completed_time = None

    def id(self):
        return self._attempt_id

    def mark_failed(self, reason):
        self._fail_reason = reason
        self._status = TaskAttempt.FAILED

    def mark_completed(self, time_stamp):
        self._status = TaskAttempt.COMPLETED
        self.completed_time = time_stamp

    def failed(self):
        return self._status == TaskAttempt.FAILED

    def completed(self):
        return self._status == TaskAttempt.COMPLETED

    def started(self):
        return self._status == TaskAttempt.STARTED

    def in_process(self):
        return TaskAttempt.STARTED <= self._status < TaskAttempt.COMPLETED

    def __hash__(self):
        return self._attempt_id


class TaskQueue(object):

    def __init__(self, logger):
        self._logger = logger

    def next_task(self, skip_task_ids=None):
        raise NotImplementedError

    def task(self, task_id):
        raise NotImplementedError

    def add_task(self, task):
        raise NotImplementedError

    def remove_task(self, task_id):
        raise NotImplementedError

    def task_ids(self):
        raise NotImplementedError

    def all_tasks(self):
        raise NotImplementedError

    def __len__(self):
        raise NotImplementedError


class SimpleTaskQueue(TaskQueue):

    def __init__(self, logger):
        TaskQueue.__init__(self, logger)
        self._queue = collections.OrderedDict()

    def next_task(self, skip_task_ids=None):
        task_to_send_back = None
        # this is nasty hack on ordereddict. I can't index it so I just iterate it and return first one
        for task in self._queue.itervalues():
            if skip_task_ids is not None and task.task_id() in skip_task_ids:
                continue
            else:
                task_to_send_back = task
                break
        return task_to_send_back

    def task(self, task_id):
        return self._queue.get(task_id)

    def add_task(self, task):
        self._queue[task.task_id()] = task

    def remove_task(self, task_id):
        if task_id in self._queue:
            del self._queue[task_id]

    def task_ids(self):
        return self._queue.keys()

    def all_tasks(self):
        return self._queue.values()

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

    def __init__(self, logger):
        self._logger = logger
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
            if task.most_recent_attempt().failed():
                if len(task.attempts) >= task.max_attempts:
                    self._logger.debug("OpenTasks.task_to_retry: Task %s has failed attempt %d of %d. Treating it as failed." %
                                       (str(task.task_id()), task.num_attempts(), task.max_attempts))
                    failed_tasks.append(task)
                else:
                    self._logger.debug("OpenTasks.task_to_retry: Task %s has failed attempt %d of %d. Should be retried." %
                                       (str(task.task_id()), task.num_attempts(), task.max_attempts))
                    no_duration = task
                    break

        with_duration = None
        for task in self._durations.itervalues():
            failed = task.most_recent_attempt().failed()
            timed_out = (current_time - task.most_recent_attempt().start_time).total_seconds() > task.duration
            if failed or timed_out:
                if task.num_attempts() >= task.max_attempts:
                    self._logger.debug("OpenTasks.task_to_retry: Task %s has %s attempt %d of %d. Treating it as failed." %
                                       (str(task.task_id()), "failed" if failed else "timed out", task.num_attempts(), task.max_attempts))
                    failed_tasks.append(task)
                else:
                    self._logger.debug("OpenTasks.task_to_retry: Task %s has %s attempt %d of %d. Should be retried." %
                        (str(task.task_id()), "failed" if failed else "timed out", task.num_attempts(), task.max_attempts))
                    with_duration = task
                    break

        retry_task = None
        if no_duration is not None and no_duration.created_time <= with_duration.created_time:
            retry_task = no_duration
        else:
            retry_task = with_duration
        if retry_task is None:
            self._logger.debug("OpenTasks.task_to_retry: No task to be retried. Returning it and %d failed tasks" %
                               len(failed_tasks))
        else:
            self._logger.debug("OpenTasks.task_to_retry: Task %s is oldest task to be retried. Returning it and %d failed tasks" %
                               (str(retry_task.task_id()), len(failed_tasks)))
        return retry_task, failed_tasks

    def add_task(self, task):
        """
        If the task has an expected duration then add it to durations, otherwise add it to no durations
        """
        assert isinstance(task, Task)
        assert task.most_recent_attempt() is not None, "Cannot add task to OpenTasks because no current attempt"
        if task.duration is None:
            self._no_durations[task.task_id()] = task
            self._logger.debug("OpenTasks.add_task: Task %s added to no durations." % str(task.task_id()))
        else:
            self._durations[task.task_id()] = task
            self._logger.debug("OpenTasks.add_task: Task %s added to durations." % str(task.task_id()))

    def remove_task(self, task_id):
        """
        remove the task from OpenTasks. If task doesn't exist in OpenTask then no-op.
        """
        if task_id in self._no_durations:
            del self._no_durations[task_id]
            self._logger.debug("OpenTasks.remove_task: Task %s removed from no durations." % str(task_id))
        elif task_id in self._durations:
            del self._durations[task_id]
            self._logger.debug("OpenTasks.remove_task: Task %s removed from durations." % str(task_id))

    def get_task(self, task_id):
        # the task doesn't exist here then return none
        task = self._durations.get(task_id)
        if task is None:
            task = self._no_durations.get(task_id)
        return task

    def all_tasks(self):
        tasks = []
        tasks.extend(self._durations.values())
        tasks.extend(self._no_durations.values())
        # todo unit test sorting
        return sorted(tasks, key=lambda task: task.created_time)

    def __len__(self):
        return len(self._durations) + len(self._no_durations)


class TaskManager(object):

    def __init__(self, logger):
        self._todo_queue = SimpleTaskQueue(logger)
        self._in_process = OpenTasks(logger)
        self._done = collections.OrderedDict()
        self._logger = logger

    def _move_task_to_done(self, task):
        # TODO unit test this
        task_id = task.task_id()
        self._in_process.remove_task(task_id)
        self._logger.debug("Task %s removed from in process tasks." % str(task_id))
        self._done[task.task_id()] = task
        self._logger.debug("Task %s added to done tasks." % str(task_id))

    def start_next_attempt(self, runner, current_time):
        self._logger.debug("Starting next attempt for runner %s at %s" % (str(runner), str(current_time)))
        attempt = None
        # if there is one in process that needs to be re-attempted then do that
        next_task, failed_tasks = self._in_process.task_to_retry(current_time)
        # for each failed task: 1) remove from in process, 2) add to done
        for task in failed_tasks:
            self._logger.info("Task %s has failed. Moving it to Done." % str(task.task_id()))
            self._move_task_to_done(task)

        if next_task is not None:
            attempt = next_task.attempt_task(runner, current_time)
        else:  # if still no next task, get one from the queued up new tasks
            skip_task_ids = set()
            todo_ids = self._todo_queue.task_ids()
            # as long as not every task_id in todo_queue is in skip_tasks, we keep trying
            while not skip_task_ids.issuperset(todo_ids):
                possible_next_task = self._todo_queue.next_task(skip_task_ids=skip_task_ids)
                # if any dependency is not done, then we need to continue
                can_run = True
                for dependency in possible_next_task.dependent_on:
                    if not dependency.is_completed():
                        can_run = False
                        break
                if can_run:  # if it can run then all dependencies are completed (or there are no dependencies) so break the while loop, we've found our next task
                    next_task = possible_next_task
                    break

            # and move this task from something to do to in process & create attempt
            if next_task is not None:
                self._todo_queue.remove_task(next_task.task_id())
                attempt = next_task.attempt_task(runner, current_time)
                self._in_process.add_task(next_task)
        return next_task, attempt

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

    def complete_attempt(self, task_id, attempt_id, time_stamp):
        task = self._find_task(task_id, in_process=True, done=True)
        if task is not None:
            task.get_attempt(attempt_id).mark_completed(time_stamp)
            if self._find_task(task_id, in_process=True):
                self._in_process.remove_task(task_id)
                self._done[task.task_id()] = task
                return True
        return False

    def add_task(self, task):
        # all tasks dependent_on must exist
        for task_id in task.dependent_on:
            if self._find_task(task_id, todo=True, in_process=True, done=True) is None:
                raise UnknownDependencyException()
        self._todo_queue.add_task(task)

    def done_tasks(self):
        return self._done.values()

    def todo_tasks(self):
        return self._todo_queue.all_tasks()

    def in_process_tasks(self):
        return self._in_process.all_tasks()

    def dependencies(self, task_id):
        dependencies = []
        tasks = []
        tasks.extend(self.todo_tasks())
        tasks.extend(self.in_process_tasks())
        tasks.extend(self.in_process_tasks())
        for task in tasks:
            if task_id in task.dependent_on():
                dependencies.append(task_id)
        return dependencies
