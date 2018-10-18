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

from simple_task_objects import Task
from simple_task_manager import TaskManager
from datetime import datetime
import pytest
import logging
import tempfile

LOGGER = logging.getLogger(__name__)


@pytest.fixture
def basic_task_manager():
    temp = tempfile.NamedTemporaryFile()
    task_manager = TaskManager(temp.name, LOGGER)
    time_stamp = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=5, microsecond=100222)
    task_manager.add_task("run command example", time_stamp, name="example run",
                          desc="this is a bologna command that does nothing")
    task_manager.add_task("python -m some_script", time_stamp,
                          name="example python run that will only try to run once and should last 3 minutes")
    task_manager.add_task("cd my_directory; python -m some_script", time_stamp, name="multiple commands",
                          desc="an example of multiple commands in  one task")
    yield task_manager
    task_manager.close()
    temp.close()


@pytest.fixture
def empty_task_manager():
    temp = tempfile.NamedTemporaryFile()
    task_manager = TaskManager(temp.name, LOGGER)
    yield task_manager
    task_manager.close()
    temp.close()


def test_new_task_manager(empty_task_manager):
    # should be empty
    assert len(empty_task_manager.todo_tasks()) == 0
    assert len(empty_task_manager.in_process_tasks()) == 0
    assert len(empty_task_manager.done_tasks()) == 0
    # next task to do should be None
    task, attempt = empty_task_manager.start_next_attempt("runner", datetime.now())
    assert task is None
    assert attempt is None


def test_add_task(empty_task_manager):
    t1 = empty_task_manager.add_task("run command example", datetime.now(), name="example run",
                                     desc="this is a bologna command that does nothing")
    assert len(empty_task_manager.todo_tasks()) == 1
    assert len(empty_task_manager.in_process_tasks()) == 0
    assert len(empty_task_manager.done_tasks()) == 0
    task = empty_task_manager.get_task(t1.task_id())
    assert task is not None
    assert task.task_id() == t1.task_id()
    assert task.cmd == t1.cmd
    assert task.name == t1.name
    assert task.desc == t1.desc
    assert task.duration == t1.duration
    assert task.max_attempts == t1.max_attempts
    assert task.created_time == t1.created_time

    t2 = empty_task_manager.add_task("another run command example", datetime.now(), name="example run",
                                     desc="this is a bologna command that does nothing")
    assert len(empty_task_manager.todo_tasks()) == 2
    assert len(empty_task_manager.in_process_tasks()) == 0
    assert len(empty_task_manager.done_tasks()) == 0
    task = empty_task_manager.get_task(t2.task_id())
    assert task.task_id() == t2.task_id()
    assert task.cmd == t2.cmd
    assert task.name == t2.name
    assert task.desc == t2.desc
    assert task.duration == t2.duration
    assert task.max_attempts == t2.max_attempts
    assert task.created_time == t2.created_time


def test_next_task_no_expired_or_failed(basic_task_manager):
    # make sure baseline is right
    assert len(basic_task_manager.todo_tasks()) == 3
    assert len(basic_task_manager.in_process_tasks()) == 0
    assert len(basic_task_manager.done_tasks()) == 0

    time = datetime.now()
    runner = "runner1"
    task, attempt = basic_task_manager.start_next_attempt(runner, time)
    assert task.task_id() == 1
    assert len(basic_task_manager.get_task_attempts(task.task_id())) == 1
    assert basic_task_manager.get_most_recent_attempt(task.task_id()).id() == attempt.id()  # TODO attempt equality
    assert attempt.start_time == time
    assert attempt.runner == runner
    assert not attempt.is_failed()
    assert not attempt.is_completed()
    assert attempt.is_in_process()
    assert len(basic_task_manager.todo_tasks()) == 2
    assert len(basic_task_manager.in_process_tasks()) == 1
    assert len(basic_task_manager.done_tasks()) == 0

    time = datetime.now()
    runner = "runner2"
    task, attempt = basic_task_manager.start_next_attempt(runner, time)
    assert len(basic_task_manager.get_task_attempts(task.task_id())) == 1
    assert task.task_id() == 2
    assert basic_task_manager.get_most_recent_attempt(task.task_id()).id() == attempt.id()  # TODO attempt equality
    assert attempt.start_time == time
    assert attempt.runner == runner
    assert not attempt.is_failed()
    assert not attempt.is_completed()
    assert attempt.is_in_process()
    assert len(basic_task_manager.todo_tasks()) == 1
    assert len(basic_task_manager.in_process_tasks()) == 2
    assert len(basic_task_manager.done_tasks()) == 0

    time = datetime.now()
    runner = "runner1"
    task, attempt = basic_task_manager.start_next_attempt(runner, time)
    assert len(basic_task_manager.get_task_attempts(task.task_id())) == 1
    assert task.task_id() == 3
    assert basic_task_manager.get_most_recent_attempt(task.task_id()).id() == attempt.id()  # TODO attempt equality
    assert attempt.start_time == time
    assert attempt.runner == runner
    assert not attempt.is_failed()
    assert not attempt.is_completed()
    assert attempt.is_in_process()
    assert len(basic_task_manager.todo_tasks()) == 0
    assert len(basic_task_manager.in_process_tasks()) == 3
    assert len(basic_task_manager.done_tasks()) == 0


def test_mark_completed_when_in_process(basic_task_manager):
    # make sure baseline is right
    assert len(basic_task_manager.todo_tasks()) == 3
    assert len(basic_task_manager.in_process_tasks()) == 0
    assert len(basic_task_manager.done_tasks()) == 0

    start_time = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=30, microsecond=100222)
    complete_time = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=40, microsecond=100222)

    # start one up and make sure as expected
    task, attempt = basic_task_manager.start_next_attempt("runner", start_time)
    assert task.task_id() == 1
    assert basic_task_manager.get_done_time(task.task_id()) is None
    assert task.is_in_process() is True
    assert task.is_todo() is False
    assert task.has_completed() is False
    assert task.has_failed() is False
    # make sure baseline is right
    assert len(basic_task_manager.todo_tasks()) == 2
    assert len(basic_task_manager.in_process_tasks()) == 1
    assert len(basic_task_manager.done_tasks()) == 0
    # should not find task in to do tasks or done
    assert basic_task_manager._find_task(task.task_id(), todo=True) is None
    assert basic_task_manager._find_task(task.task_id(), done=True) is None
    # should find task in in process
    assert basic_task_manager._find_task(task.task_id(), in_process=True) is not None

    # mark completed
    basic_task_manager.complete_attempt(task.task_id(), attempt.id(), complete_time)
    assert len(basic_task_manager.todo_tasks()) == 2
    assert len(basic_task_manager.in_process_tasks()) == 0
    assert len(basic_task_manager.done_tasks()) == 1
    # should not find task in to do tasks or in process
    assert basic_task_manager._find_task(task.task_id(), todo=True) is None
    assert basic_task_manager._find_task(task.task_id(), in_process=True) is None
    # should find task in done
    assert basic_task_manager._find_task(task.task_id(), done=True) is not None
    # get most recent state of task
    task = basic_task_manager.get_task(task.task_id())
    assert task.has_failed() is False
    assert task.has_completed() is True
    assert task.is_in_process() is False
    assert task.is_todo() is False
    assert basic_task_manager.get_done_time(task.task_id()) == complete_time


def test_mark_completed_when_multiple_attempts_in_process(basic_task_manager):
    # make sure baseline is what we expect
    assert len(basic_task_manager.todo_tasks()) == 3
    assert len(basic_task_manager.in_process_tasks()) == 0
    assert len(basic_task_manager.done_tasks()) == 0

    start_time = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=30, microsecond=100222)
    complete_time = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=40, microsecond=100222)

    # start one up and make sure as expected
    task1, attempt1 = basic_task_manager.start_next_attempt("runner", start_time)
    assert task1.task_id() == 1
    task2, attempt2 = basic_task_manager.start_next_attempt("runner", start_time)
    assert task2.task_id() == 2

    assert len(basic_task_manager.todo_tasks()) == 1
    assert len(basic_task_manager.in_process_tasks()) == 2
    assert len(basic_task_manager.done_tasks()) == 0

    # mark completed
    basic_task_manager.complete_attempt(task1.task_id(), attempt1.id(), complete_time)

    assert len(basic_task_manager.todo_tasks()) == 1
    assert len(basic_task_manager.in_process_tasks()) == 1
    assert len(basic_task_manager.done_tasks()) == 1
    # get most recent state refresh of task
    task1 = basic_task_manager.get_task(task1.task_id())
    assert task1.has_completed() is True
    assert task1.is_in_process() is False
    assert task1.has_failed() is False

    # get most recent task 2 to make sure nothing chanted
    task2 = basic_task_manager.get_task(task2.task_id())
    assert task2.has_completed() is False
    assert task2.has_failed() is False
    assert task2.is_in_process() is True
    assert task2.is_todo() is False


def test_mark_completed_when_multiple_attempts_in_process_other_order(basic_task_manager):
    # make sure baseline is what we expect
    assert len(basic_task_manager.todo_tasks()) == 3
    assert len(basic_task_manager.in_process_tasks()) == 0
    assert len(basic_task_manager.done_tasks()) == 0

    start_time = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=30, microsecond=100222)
    complete_time = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=40, microsecond=100222)

    # start a couple up and make sure as expected
    task1, attempt1 = basic_task_manager.start_next_attempt("runner", start_time)
    assert task1.task_id() == 1
    task2, attempt2 = basic_task_manager.start_next_attempt("runner", start_time)
    assert task2.task_id() == 2

    # make sure baseline is what we expect
    assert len(basic_task_manager.todo_tasks()) == 1
    assert len(basic_task_manager.in_process_tasks()) == 2
    assert len(basic_task_manager.done_tasks()) == 0

    # mark completed
    basic_task_manager.complete_attempt(task2.task_id(), attempt2.id(), complete_time)

    # make sure baseline is what we expect
    assert len(basic_task_manager.todo_tasks()) == 1
    assert len(basic_task_manager.in_process_tasks()) == 1
    assert len(basic_task_manager.done_tasks()) == 1
    # refresh task2 to get most recent state
    task2 = basic_task_manager.get_task(task2.task_id())
    assert task2.has_completed() is True
    assert task2.is_in_process() is False
    assert task2.has_failed() is False
    assert task2.is_todo() is False

    # refresh task 1 to get most recent state
    task1 = basic_task_manager.get_task(task1.task_id())
    assert task1.has_completed() is False
    assert task1.has_failed() is False
    assert task1.is_in_process() is True
    assert task1.is_todo() is False

    # get task3 to make sure nothing changed
    task3 = basic_task_manager.get_task(3)
    assert task3.has_completed() is False
    assert task3.has_failed() is False
    assert task3.is_in_process() is False
    assert task3.is_todo() is True


def test_mark_completed_when_done(empty_task_manager):
    time_stamp1 = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=5, microsecond=100222)
    time_stamp2 = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=10, microsecond=0)
    time_stamp3 = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=15, microsecond=222222)
    empty_task_manager.add_task("run command example", time_stamp1, name="example run",
                                desc="this is a bologna command that does nothing",
                                duration=100, max_attempts=5)
    empty_task_manager.add_task("python -m some_script", time_stamp2,
                                name="example python run that will only try to run once and should last 3 minutes",
                                duration=180, max_attempts=1)
    empty_task_manager.add_task("cd my_directory; python -m some_script", time_stamp3, name="multiple commands",
                                desc="an example of multiple commands in  one task", duration=200)

    # make sure baseline is what we expect
    assert len(empty_task_manager.todo_tasks()) == 3
    assert len(empty_task_manager.in_process_tasks()) == 0
    assert len(empty_task_manager.done_tasks()) == 0

    start_time = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=30, microsecond=500000)
    complete_time = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=40, microsecond=600222)

    # start one up and make sure as expected
    task, attempt = empty_task_manager.start_next_attempt("runner", start_time)
    assert task.task_id() == 1
    assert len(empty_task_manager.todo_tasks()) == 2
    assert len(empty_task_manager.in_process_tasks()) == 1
    assert len(empty_task_manager.done_tasks()) == 0

    # start a second attempt
    second_start_time = datetime(year=2018, month=8, day=13, hour=5, minute=13, second=30, microsecond=100222)
    task2, attempt2 = empty_task_manager.start_next_attempt("runner2", second_start_time)
    assert task2.task_id() == 1
    assert attempt2.id() != attempt.id()
    assert len(empty_task_manager.todo_tasks()) == 2
    assert len(empty_task_manager.in_process_tasks()) == 1
    assert len(empty_task_manager.done_tasks()) == 0

    # mark completed
    empty_task_manager.complete_attempt(task.task_id(), attempt.id(), complete_time)

    assert len(empty_task_manager.todo_tasks()) == 2
    assert len(empty_task_manager.in_process_tasks()) == 0
    assert len(empty_task_manager.done_tasks()) == 1
    # get most recent version of task to get all state
    task = empty_task_manager.get_task(task.task_id())
    assert task.has_completed() is True
    assert task.is_in_process() is False
    assert task.has_failed() is False
    assert task.is_todo() is False
    assert empty_task_manager.get_done_time(task.task_id()) == complete_time

    # mark completed again
    second_complete_time = datetime(year=2018, month=8, day=13, hour=5, minute=10+3, second=40, microsecond=100222)
    empty_task_manager.complete_attempt(task2.task_id(), attempt2.id(), second_complete_time)
    # should have no impact; all should be the same
    assert len(empty_task_manager.todo_tasks()) == 2
    assert len(empty_task_manager.in_process_tasks()) == 0
    assert len(empty_task_manager.done_tasks()) == 1
    assert task.has_completed() is True
    assert task.is_in_process() is False
    assert task.has_failed() is False
    assert task.is_todo() is False
    assert empty_task_manager.get_done_time(task.task_id()) == complete_time


def test_mark_completed_when_attempt_unknown(empty_task_manager):
    time_stamp1 = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=5, microsecond=100222)
    t1 = empty_task_manager.add_task("run command example", time_stamp1, name="example run",
                                     desc="this is a bologna command that does nothing",
                                     duration=100, max_attempts=2)

    completed_time_stamp = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=44, microsecond=100222)
    assert empty_task_manager.complete_attempt(t1.task_id(), "some_random_unknown_attempt_id", completed_time_stamp) is False


def test_find_task_in_todo(basic_task_manager):
    t = basic_task_manager._find_task(1, todo=True, in_process=False, done=False)
    assert t is not None
    assert t.task_id() == 1


def test_delete_from_todo(basic_task_manager):
    # make sure baseline is what we expect
    assert len(basic_task_manager.todo_tasks()) == 3
    assert len(basic_task_manager.in_process_tasks()) == 0
    assert len(basic_task_manager.done_tasks()) == 0
    assert basic_task_manager._find_task(1, todo=True) is not None

    basic_task_manager.delete_task(1)

    # now the task should be gone
    print (basic_task_manager.todo_tasks())
    assert len(basic_task_manager.todo_tasks()) == 2
    assert len(basic_task_manager.in_process_tasks()) == 0
    assert len(basic_task_manager.done_tasks()) == 0
    # shouldn't be in to do
    assert basic_task_manager._find_task(1, todo=True) is None
    # shouldn't be anywhere
    assert basic_task_manager._find_task(1, todo=True, in_process=True, done=True) is None


def test_delete_from_inprocess(basic_task_manager):
    # make sure baseline is what we expect
    assert len(basic_task_manager.todo_tasks()) == 3
    assert len(basic_task_manager.in_process_tasks()) == 0
    assert len(basic_task_manager.done_tasks()) == 0
    assert basic_task_manager._find_task(1, todo=True) is not None

    # move task to is_in_process
    basic_task_manager.start_next_attempt("runner", datetime.now())
    assert len(basic_task_manager.todo_tasks()) == 2
    assert len(basic_task_manager.in_process_tasks()) == 1
    assert len(basic_task_manager.done_tasks()) == 0
    assert basic_task_manager._find_task(1, todo=True) is None
    assert basic_task_manager._find_task(1, in_process=True) is not None

    basic_task_manager.delete_task(1)

    # now the task should be gone
    assert len(basic_task_manager.todo_tasks()) == 2
    assert len(basic_task_manager.in_process_tasks()) == 0
    assert len(basic_task_manager.done_tasks()) == 0
    assert basic_task_manager._find_task(1, in_process=True) is None
    assert basic_task_manager._find_task(1, todo=True, in_process=True, done=True) is None


def test_delete_from_done_when_completed(basic_task_manager):
    # make sure baseline is what we expect
    assert len(basic_task_manager.todo_tasks()) == 3
    assert len(basic_task_manager.in_process_tasks()) == 0
    assert len(basic_task_manager.done_tasks()) == 0
    assert basic_task_manager._find_task(1, todo=True) is not None

    # move task to is_in_process
    task, attempt = basic_task_manager.start_next_attempt("runner", datetime.now())
    assert task.task_id() == 1
    assert len(basic_task_manager.todo_tasks()) == 2
    assert len(basic_task_manager.in_process_tasks()) == 1
    assert len(basic_task_manager.done_tasks()) == 0
    assert basic_task_manager._find_task(1, todo=True) is None
    assert basic_task_manager._find_task(1, in_process=True) is not None

    # now complete
    basic_task_manager.complete_attempt(task.task_id(), attempt.id(), datetime.now())
    assert len(basic_task_manager.todo_tasks()) == 2
    assert len(basic_task_manager.in_process_tasks()) == 0
    assert len(basic_task_manager.done_tasks()) == 1
    assert basic_task_manager._find_task(1, todo=True) is None
    assert basic_task_manager._find_task(1, in_process=True) is None
    assert basic_task_manager._find_task(1, done=True) is not None

    basic_task_manager.delete_task(1)

    # now the task should be gone
    assert len(basic_task_manager.todo_tasks()) == 2
    assert len(basic_task_manager.in_process_tasks()) == 0
    assert len(basic_task_manager.done_tasks()) == 0
    assert basic_task_manager._find_task(1, done=True) is None


def test_delete_from_done_when_failed(basic_task_manager):
    # make sure baseline is what we expect
    assert len(basic_task_manager.todo_tasks()) == 3
    assert len(basic_task_manager.in_process_tasks()) == 0
    assert len(basic_task_manager.done_tasks()) == 0
    assert basic_task_manager._find_task(1, todo=True) is not None

    # move task to is_in_process
    task, attempt = basic_task_manager.start_next_attempt("runner", datetime.now())
    assert task.task_id() == 1
    assert len(basic_task_manager.todo_tasks()) == 2
    assert len(basic_task_manager.in_process_tasks()) == 1
    assert len(basic_task_manager.done_tasks()) == 0
    assert basic_task_manager._find_task(1, todo=True) is None
    assert basic_task_manager._find_task(1, in_process=True) is not None

    # now fail
    basic_task_manager.fail_attempt(task.task_id(), attempt.id(), "cause it just failed", datetime.now())
    assert len(basic_task_manager.todo_tasks()) == 2
    assert len(basic_task_manager.in_process_tasks()) == 0
    assert len(basic_task_manager.done_tasks()) == 1
    assert basic_task_manager._find_task(1, todo=True) is None
    assert basic_task_manager._find_task(1, in_process=True) is None
    assert basic_task_manager._find_task(1, done=True) is not None

    basic_task_manager.delete_task(1)

    # now the task should be gone
    assert len(basic_task_manager.todo_tasks()) == 2
    assert len(basic_task_manager.in_process_tasks()) == 0
    assert len(basic_task_manager.done_tasks()) == 0
    assert basic_task_manager._find_task(1, done=True) is None
    assert basic_task_manager._find_task(1, todo=True, in_process=True, done=True) is None


def test_move_task_to_done_when_in_process(basic_task_manager):
    task, attempt = basic_task_manager.start_next_attempt("runner", datetime.now())
    assert task.task_id() == 1
    assert len(basic_task_manager.todo_tasks()) == 2
    assert len(basic_task_manager.in_process_tasks()) == 1
    assert len(basic_task_manager.done_tasks()) == 0
    assert basic_task_manager._find_task(1, todo=True) is None
    assert basic_task_manager._find_task(1, in_process=True) is not None

    basic_task_manager._move_task_to_done(task)
    assert len(basic_task_manager.todo_tasks()) == 2
    assert len(basic_task_manager.in_process_tasks()) == 0
    assert len(basic_task_manager.done_tasks()) == 1
    assert basic_task_manager._find_task(1, todo=True) is None
    assert basic_task_manager._find_task(1, in_process=True) is None
    assert basic_task_manager._find_task(1, done=True) is not None


def test_move_task_to_done_when_not_in_process(empty_task_manager):
    time_stamp = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=5, microsecond=100222)
    t1 = empty_task_manager.add_task("run command example", time_stamp, name="example run",
                                     desc="this is a bologna command that does nothing")
    empty_task_manager.add_task("python -m some_script", time_stamp,
                                name="example python run that will only try to run once and should last 3 minutes")
    empty_task_manager.add_task("cd my_directory; python -m some_script", time_stamp, name="multiple commands",
                                desc="an example of multiple commands in  one task")

    assert len(empty_task_manager.todo_tasks()) == 3
    assert len(empty_task_manager.in_process_tasks()) == 0
    assert len(empty_task_manager.done_tasks()) == 0
    assert empty_task_manager._find_task(t1.task_id(), todo=True) is not None
    assert empty_task_manager._find_task(t1.task_id(), in_process=True) is None
    assert empty_task_manager._find_task(t1.task_id(), done=True) is None

    empty_task_manager._move_task_to_done(t1)
    assert len(empty_task_manager.todo_tasks()) == 2
    assert len(empty_task_manager.in_process_tasks()) == 0
    assert len(empty_task_manager.done_tasks()) == 1
    assert empty_task_manager._find_task(t1.task_id(), todo=True) is None
    assert empty_task_manager._find_task(t1.task_id(), in_process=True) is None
    assert empty_task_manager._find_task(t1.task_id(), done=True) is not None


def test_find_task_non_existant(basic_task_manager):
    assert basic_task_manager._find_task("NON existant task id", todo=True, in_process=True, done=True) is None


def test_find_task_todo_looking_in_in_process(basic_task_manager):
    assert basic_task_manager._find_task(1, todo=False, in_process=True, done=False) is None


def test_find_task_todo_looking_in_done(basic_task_manager):
    assert basic_task_manager._find_task(1, todo=False, in_process=False, done=True) is None


def test_find_task_todo_looking_in_todo(basic_task_manager):
    assert basic_task_manager._find_task(1, todo=True, in_process=False, done=False).task_id() == 1


def test_find_task_todo_looking_in_all(basic_task_manager):
    assert basic_task_manager._find_task(1, todo=True, in_process=True, done=True).task_id() == 1


def test_find_task_inprocess_looking_in_in_process(basic_task_manager):
    time_stamp = datetime(year=2018, month=8, day=13, hour=7, minute=10, second=5, microsecond=100222)
    task, attempt = basic_task_manager.start_next_attempt("runner", time_stamp)
    assert basic_task_manager._find_task(1, todo=False, in_process=True, done=False).task_id() == task.task_id()


def test_find_task_inprocess_looking_in_done(basic_task_manager):
    time_stamp = datetime(year=2018, month=8, day=13, hour=7, minute=10, second=5, microsecond=100222)
    basic_task_manager.start_next_attempt("runner", time_stamp)
    assert basic_task_manager._find_task(1, todo=False, in_process=False, done=True) is None


def test_find_task_inprocess_looking_in_todo(basic_task_manager):
    time_stamp = datetime(year=2018, month=8, day=13, hour=7, minute=10, second=5, microsecond=100222)
    basic_task_manager.start_next_attempt("runner", time_stamp)
    assert basic_task_manager._find_task(1, todo=True, in_process=False, done=False) is None


def test_find_task_inprocess_looking_in_all(basic_task_manager):
    time_stamp = datetime(year=2018, month=8, day=13, hour=7, minute=10, second=5, microsecond=100222)
    task, attempt = basic_task_manager.start_next_attempt("runner", time_stamp)
    assert basic_task_manager._find_task(1, todo=True, in_process=True, done=True).task_id() == task.task_id()


def test_find_task_done_looking_in_in_process(basic_task_manager):
    time_stamp = datetime(year=2018, month=8, day=13, hour=7, minute=10, second=5, microsecond=100222)
    task, attempt = basic_task_manager.start_next_attempt("runner", time_stamp)
    task = basic_task_manager._find_task(task.task_id(), in_process=True)
    basic_task_manager._move_task_to_done(task)
    assert basic_task_manager._find_task(1, todo=False, in_process=True, done=False) is None


def test_find_task_done_looking_in_done(basic_task_manager):
    time_stamp = datetime(year=2018, month=8, day=13, hour=7, minute=10, second=5, microsecond=100222)
    task, attempt = basic_task_manager.start_next_attempt("runner", time_stamp)
    task = basic_task_manager._find_task(task.task_id(), in_process=True)
    basic_task_manager._move_task_to_done(task)
    assert basic_task_manager._find_task(1, todo=False, in_process=False, done=True).task_id() == task.task_id()


def test_find_task_done_looking_in_todo(basic_task_manager):
    time_stamp = datetime(year=2018, month=8, day=13, hour=7, minute=10, second=5, microsecond=100222)
    task, attempt = basic_task_manager.start_next_attempt("runner", time_stamp)
    task = basic_task_manager._find_task(task.task_id(), in_process=True)
    basic_task_manager._move_task_to_done(task)
    assert basic_task_manager._find_task(1, todo=True, in_process=False, done=False) is None


def test_find_task_done_looking_in_all(basic_task_manager):
    time_stamp = datetime(year=2018, month=8, day=13, hour=7, minute=10, second=5, microsecond=100222)
    task, attempt = basic_task_manager.start_next_attempt("runner", time_stamp)
    task = basic_task_manager._find_task(task.task_id(), in_process=True)
    basic_task_manager._move_task_to_done(task)
    assert basic_task_manager._find_task(1, todo=True, in_process=True, done=True).task_id() == task.task_id()


# TODO test completing a deleted attempt
