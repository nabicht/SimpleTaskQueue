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

import collections
from persistence import SQLitePersistence


# Custom Exceptions
class UnknownDependencyException(Exception):
    pass


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

    def __init__(self, persistence, logger):
        self._logger = logger
        self._persistence = persistence

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
        no_duration_tasks = self._persistence.get_tasks_with_duration_filter(SQLitePersistence.IN_PROCESS_QUEUE, False)
        for task in no_duration_tasks:
            most_recent_attempt = self._persistence.get_most_recent_attempt(task.task_id())
            if most_recent_attempt is not None and most_recent_attempt.is_failed():
                attempt_count = self._persistence.get_attempt_count()
                if attempt_count >= task.max_attempts:
                    self._logger.debug("OpenTasks.task_to_retry: Task %s has failed attempt %d of %d. Treating it as failed." %
                                       (str(task.task_id()), attempt_count, task.max_attempts))
                    failed_tasks.append(task)
                else:
                    self._logger.debug("OpenTasks.task_to_retry: Task %s has failed attempt %d of %d. Should be retried." %
                                       (str(task.task_id()), attempt_count, task.max_attempts))
                    no_duration = task
                    break

        with_duration = None
        duration_tasks = self._persistence.get_tasks_with_duration_filter(SQLitePersistence.IN_PROCESS_QUEUE, True)
        for task in duration_tasks:
            most_recent_attempt = self._persistence.get_most_recent_attempt(task.task_id())
            failed = False
            timed_out = False
            if most_recent_attempt is not None:
                failed = most_recent_attempt.is_failed()
                timed_out = (current_time - most_recent_attempt.start_time).total_seconds() > task.duration
            if failed or timed_out:
                attempt_count = self._persistence.get_attempt_count(task.task_id())
                if attempt_count >= task.max_attempts:
                    self._logger.debug("OpenTasks.task_to_retry: Task %s has %s attempt %d of %d. Treating it as failed." %
                                       (str(task.task_id()), "failed" if failed else "timed out", attempt_count,
                                        task.max_attempts))
                    failed_tasks.append(task)
                else:
                    self._logger.debug("OpenTasks.task_to_retry: Task %s has %s attempt %d of %d. Should be retried." %
                                       (str(task.task_id()), "failed" if failed else "timed out", attempt_count,
                                        task.max_attempts))
                    with_duration = task
                    break

        retry_task = None
        if no_duration is not None and with_duration is None:
            retry_task = no_duration
        elif no_duration is None and with_duration is not None:
            retry_task = with_duration
        elif no_duration is not None and no_duration.created_time <= with_duration.created_time:
            retry_task = no_duration
        elif with_duration is not None:
            retry_task = with_duration
        if retry_task is None:
            self._logger.debug("OpenTasks.task_to_retry: No task to be retried. Returning it and %d failed tasks" %
                               len(failed_tasks))
        else:
            self._logger.debug("OpenTasks.task_to_retry: Task %s is oldest task to be retried. Returning it and %d failed tasks" %
                               (str(retry_task.task_id()), len(failed_tasks)))
        return retry_task, failed_tasks

    def get_task(self, task_id):
        # the task doesn't exist here then return none
        return self._persistence.get_task(task_id, SQLitePersistence.IN_PROCESS_QUEUE)

    def all_tasks(self):
        return self._persistence.get_tasks(SQLitePersistence.IN_PROCESS_QUEUE, sort_by_created_time=True)

    def __len__(self):
        return self._persistence.get_task_count(SQLitePersistence.IN_PROCESS_QUEUE)


"""
TODO I think we can move all of the above into TaskManager and not keep separate data structures, but 
 first let's get it working with persistence and then I'll figure it out
"""


class TaskManager(object):

    def __init__(self, db_file, logger):
        self._persistence = SQLitePersistence(db_file, logger)
        self._in_process = OpenTasks(self._persistence, logger)
        self._done = collections.OrderedDict()
        self._logger = logger

    def _move_task_to_done(self, task):
        task_id = task.task_id()
        self._logger.debug("TaskManager._move_task_to_done: Task %s moving to Done." % str(task_id))
        try:
            self._persistence.update_task_to_done(task.task_id())
        except:
            self._logger.warn("TaskManager._move_task_to_done: FAILED to move task %s moved to Done tasks." % str(task_id))
        else:
            self._logger.debug("TaskManager._move_task_to_done: Task %s moved to Done tasks." % str(task_id))

    def _attempt_task(self, task, runner, time_stamp):
        attempt_id = None
        try:
            attempt_id = self._persistence.new_attempt(task.task_id(), runner, time_stamp)
        except:
            self._logger.error("TaskManager._attempt_task: There was a problem creating a new attempt for task %s" % str(task.task_id()))
        return attempt_id

    def start_next_attempt(self, runner, current_time):
        self._logger.debug("TaskManager.start_next_attempt: Starting next attempt for runner %s at %s" % (str(runner), str(current_time)))
        attempt_id = None
        attempt = None
        # if there is one in process that needs to be re-attempted then do that
        next_task, failed_tasks = self._in_process.task_to_retry(current_time)
        # for each failed task: 1) remove from in process, 2) add to done
        for task in failed_tasks:
            self._logger.info("TaskManager.start_next_attempt: Task %s has failed. Moving it to Done." % str(task.task_id()))
            self._move_task_to_done(task)

        if next_task is not None:
            attempt_id = self._attempt_task(next_task, runner, current_time)
            if attempt_id is not None:
                num_attempts = self._persistence.get_attempt_count(next_task.task_id())
                self._logger.info("TaskManager.start_next_attempt: Created Attempt %s for Task %s. Attempt %d of %d." %
                                  (attempt_id, str(next_task.task_id()), num_attempts, next_task.max_attempts))
        else:  # if still no next task, get one from the queued up new tasks
            todo_ids = self._persistence.get_task_ids(SQLitePersistence.TODO_QUEUE)
            for todo_id in todo_ids:
                # if all dependencies are complete then run it, else, continue
                dependent_on_task_ids = self._persistence.get_dependent_on(todo_id)
                can_run = True
                for dependent_on_task_id in dependent_on_task_ids:
                    if not self._persistence.is_task_completed(dependent_on_task_id):
                        self._logger.debug(
                            "TaskManager.start_next_attempt: Task %s is dependent on Task %s, which is not completed. Skipping it for now." %
                            (str(todo_id), str(dependent_on_task_id)))
                        can_run = False
                        break
                if can_run: # if it can run then all dependencies are completed (or there are no dependencies) we've found our next task
                    next_task = self._persistence.get_task(todo_id)
                    break

            # if there is a next_task we need to create an attempt and move to task to inprocess
            if next_task is not None:
                attempt_id = self._attempt_task(next_task, runner, current_time)
                num_attempts = self._persistence.get_attempt_count(next_task.task_id())
                self._logger.info("TaskManager.start_next_attempt: Created Attempt %s for Task %s. Attempt %d of %d." %
                                  (str(attempt_id), str(next_task.task_id()), num_attempts, next_task.max_attempts))
                try:
                    self._persistence.update_task_to_inprocess(next_task.task_id())
                    self._logger.debug("TaskManager.start_next_attempt: Task %s is being moved from todo to in process." %
                                       str(next_task.task_id()))
                except:
                    self._logger.error("TaskManager.start_next_attempt: Task %s could not be moved from todo to in process" %
                                       str(next_task.task_id()))

        if attempt_id is not None:
            attempt = self._persistence.get_attempt(attempt_id)
            if attempt is not None:
                num_attempts = self._persistence.get_attempt_count(next_task.task_id())
                self._logger.info("TaskManager.start_next_attempt: Next attempt is Attempt %s for Task %s. Attempt %d of %d." %
                                  (str(attempt.id()), str(next_task.task_id()), num_attempts, next_task.max_attempts))
            else:
                self._logger.error("Could not get attempt for attempt_id %s" % str(attempt_id))
        else:
            self._logger.info("TaskManager.start_next_attempt: No next task to attempt. Returning None for next task and None for attempt.")
        # refresh task before returning to make sure that all state changes from above are captures
        if next_task is not None:
            return_task = self.get_task(next_task.task_id())
        else:
            return_task = None
        return return_task, attempt

    def _find_task(self, task_id, todo=False, in_process=False, done=False):
        self._logger.debug("TaskManager._find_task: Looking for Task %s in todo = %s, is_in_process = %s, done = %s" %
                           (str(task_id), str(todo), str(in_process), str(done)))
        task = None
        if task is None and todo:
            task = self._persistence.get_task(task_id, queue=SQLitePersistence.TODO_QUEUE)
            if task is not None:
                self._logger.debug("TaskManager._find_task: Task %s found in todo." % str(task_id))
        if task is None and in_process:
            task = self._persistence.get_task(task_id, queue=SQLitePersistence.IN_PROCESS_QUEUE)
            if task is not None:
                self._logger.debug("TaskManager._find_task: Task %s found in is_in_process." % str(task_id))
        if task is None and done:
            task = self._persistence.get_task(task_id, queue=SQLitePersistence.DONE_QUEUE)
            if task is not None:
                self._logger.debug("TaskManager._find_task: Task %s found in done." % str(task_id))
        return task

    def fail_attempt(self, task_id, attempt_id, fail_reason, time_stamp):
        # first get the attempt
        attempt = self._persistence.get_attempt(attempt_id)
        # if the attempt is None we have a problem
        if attempt is None:
            self._logger.error("TaskManager.fail_attempt: Tried to fail attempt id %s but no attempt for that id." % str(attempt_id))

        # if the attempt's task_id does not match passed in task_id we have a problem
        elif attempt.task_id != task_id:
            self._logger.error("TaskManager.fail_attempt: Tried to fail attempt id %s for task %s but the attempt is actually for task %s" %
                               (str(attempt_id), str(task_id), str(attempt.task_id)))

        # otherwise, process the fail
        else:
            # fail the attempt
            self._persistence.update_attempt_to_fail(attempt_id, fail_reason, time_stamp)
            # fail the task
            task = self._persistence.get_task(task_id)
            if task is None:
                self._logger.info("Failing attempt %s for task %s but task no longer exists" % (str(attempt_id), str(task_id)))
            else:
                # if number of attempts >= task's max attempts then move task to done
                num_attempts = self._persistence.get_attempt_count(task_id)
                self._logger.info("TaskManager.fail_attempt: failed Attempt %s for Task %s." % (str(attempt_id), str(task_id)))
                if num_attempts >= task.max_attempts:
                    self._persistence.update_task_to_done(task_id)
                    self._logger.info("TaskManager.fail_attempt: Task %s Attempt %s is last attempt failed. Moved to done" %
                                      (str(task_id), str(attempt_id)))
                return True
        return False

    def complete_attempt(self, task_id, attempt_id, time_stamp):
        # first get the attempt
        attempt = self._persistence.get_attempt(attempt_id)
        # if the attempt is None we have a problem
        if attempt is None:
            self._logger.error("TaskManager.complete_attempt: Tried to complete attempt id %s but no attempt for that id." % str(attempt_id))

        # if the attempt's task_id does not match passed in task_id we have a problem
        elif attempt.task_id != task_id:
            self._logger.error("TaskManager.complete_attempt: Tried to complete attempt id %s for task %s but the attempt is actually for task %s" %
                               (str(attempt_id), str(task_id), str(attempt.task_id)))

        # otherwise, process the complete
        else:
            # complete the attempt
            self._persistence.update_attempt_to_complete(attempt_id, time_stamp)
            self._logger.info("TaskManager.complete_attempt: completed Attempt %s for Task %s." % (str(attempt_id), str(task_id)))
            # move task to done
            self._persistence.update_task_to_done(task_id)
            return True
        return False

    def add_task(self, command, create_time, name="", desc="", duration=None, max_attempts=1, dependent_on=None):
        # all tasks dependent_on must exist
        if dependent_on is not None:
            for dependent_on_task_id in dependent_on:
                if self._find_task(dependent_on_task_id, todo=True, in_process=True, done=True) is None:
                    raise UnknownDependencyException()
        task_id = self._persistence.add_task(SQLitePersistence.TODO_QUEUE, command, create_time, name, desc, duration, max_attempts, dependent_on)
        self._logger.info("TaskManager.add_task: Added Task %s to todo." % str(task_id))
        return self._persistence.get_task(task_id)

    def delete_task(self, task_id):
        # IF DELETING TASKS IT MAKES SENSE TO DELETE ATTEMPTS TOO
        # if there is a task that is a non-done task is dependent on then cannot delete it.
        deleted = False
        can_delete = True
        dependent_task_ids = self._persistence.get_dependents(task_id)
        for dependent_task_id in dependent_task_ids:
            if not self._persistence.is_task_done(task_id):
                self._logger.info("Cannot delete task %s because Task %s is dependent on it and is not done yet." % (str(task_id), str(dependent_task_id)))
                can_delete = False
                break
        if can_delete:
            try:
                deleted = self._persistence.delete_task(task_id)
                print("DELETED", deleted)
                self._logger.info("Task %s and all of its attempts have been deleted")
            except:
                self._logger.error("Could not delete Task %s.")
        return deleted

    def get_task(self, task_id):
        return self._persistence.get_task(task_id)

    def get_done_time(self, task_id):
        return self._persistence.get_task_done_time(task_id)

    def get_task_attempts(self, task_id):
        return self._persistence.get_attempts(task_id)

    def get_most_recent_attempt(self, task_id):
        return self._persistence.get_most_recent_attempt(task_id)

    def done_tasks(self):
        return self._persistence.get_tasks(SQLitePersistence.DONE_QUEUE)

    def todo_tasks(self):
        return self._persistence.get_tasks(SQLitePersistence.TODO_QUEUE)

    def in_process_tasks(self):
        return self._persistence.get_tasks(SQLitePersistence.IN_PROCESS_QUEUE)

    def dependencies(self, task_id):
        return self._persistence.get_dependents(task_id)

    def close(self):
        """
        Closes the TaskManager instance. Doing any necessary cleanup.
        :return:
        """
        self._logger.info("Closing SimpleTaskManager.")
        self._persistence.close()
        self._logger.info("SimpleTaskManager closed.")