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

from simple_task_server import Task
from simple_task_server import OpenTasks
from datetime import datetime
import logging
import pytest

LOGGER = logging.getLogger(__name__)


def test_task_to_retry_none_to_retry():
    time_stamp = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=5, microsecond=100222)

    t1 = Task(1, "run command example", time_stamp, name="example run",
              desc="this is a bologna command that does nothing", duration=100)
    start_time_stamp = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=6, microsecond=100222)
    t1.attempt_task("runner", start_time_stamp)

    t2 = Task(2, "run command example 2", time_stamp, name="example run 2",
              desc="this is a bologna command that does nothing", duration=120)
    start_time_stamp = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=6, microsecond=100222)
    t2.attempt_task("runner", start_time_stamp)

    ot = OpenTasks(LOGGER)
    ot.add_task(t1)
    ot.add_task(t2)
    current_time = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=8, microsecond=100222)
    task, failed_tasks = ot.task_to_retry(current_time)
    # should be no failed tasks
    assert task is None
    assert len(failed_tasks) == 0


def test_task_to_retry_first_started_retry():
    time_stamp = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=5, microsecond=100222)

    t1 = Task(1, "run command example", time_stamp, name="example run",
              desc="this is a bologna command that does nothing", duration=100, max_attempts=3)
    start_time_stamp = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=6, microsecond=100222)
    t1.attempt_task("runner", start_time_stamp)

    t2 = Task(2, "run command example 2", time_stamp, name="example run 2",
              desc="this is a bologna command that does nothing", duration=820, max_attempts=3)
    start_time_stamp = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=6, microsecond=100222)
    t2.attempt_task("runner", start_time_stamp)

    ot = OpenTasks(LOGGER)
    ot.add_task(t1)
    ot.add_task(t2)
    current_time = datetime(year=2018, month=8, day=13, hour=5, minute=12, second=8, microsecond=100222)
    task, failed_tasks = ot.task_to_retry(current_time)
    # should be no failed tasks
    assert task.task_id() == t1.task_id()
    assert task == t1
    assert len(failed_tasks) == 0


def test_task_to_retry_second_started_retry():
    time_stamp = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=5, microsecond=100222)

    t1 = Task(1, "run command example", time_stamp, name="example run",
              desc="this is a bologna command that does nothing", duration=500, max_attempts=3)
    start_time_stamp = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=6, microsecond=100222)
    t1.attempt_task("runner", start_time_stamp)

    t2 = Task(2, "run command example 2", time_stamp, name="example run 2",
              desc="this is a bologna command that does nothing", duration=90, max_attempts=3)
    start_time_stamp = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=8, microsecond=100222)
    t2.attempt_task("runner", start_time_stamp)

    ot = OpenTasks(LOGGER)
    ot.add_task(t1)
    ot.add_task(t2)
    current_time = datetime(year=2018, month=8, day=13, hour=5, minute=12, second=8, microsecond=100222)
    task, failed_tasks = ot.task_to_retry(current_time)
    # should be no failed tasks
    assert task.task_id() == t2.task_id()
    assert task == t2
    assert len(failed_tasks) == 0


def test_add_task_with_an_attempt():
    time_stamp = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=5, microsecond=100222)

    t1 = Task(1, "run command example", time_stamp, name="example run",
              desc="this is a bologna command that does nothing", duration=500, max_attempts=3)
    start_time_stamp = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=6, microsecond=100222)
    t1.attempt_task("runner", start_time_stamp)
    ot = OpenTasks(LOGGER)
    assert len(ot) == 0
    ot.add_task(t1)
    # should be there now
    assert len(ot) == 1
    assert ot.get_task(1) == t1


def test_add_task_without_an_attempt():
    time_stamp = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=5, microsecond=100222)

    t1 = Task(1, "run command example", time_stamp, name="example run",
              desc="this is a bologna command that does nothing", duration=500, max_attempts=3)
    ot = OpenTasks(LOGGER)
    assert len(ot) == 0
    with pytest.raises(AssertionError):
        ot.add_task(t1)
    # should be there now
    assert len(ot) == 0
    assert ot.get_task(1) is None


def test_add_already_added_task():
    time_stamp = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=5, microsecond=100222)

    t1 = Task(1, "run command example", time_stamp, name="example run",
              desc="this is a bologna command that does nothing", duration=500, max_attempts=3)
    start_time_stamp = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=6, microsecond=100222)
    t1.attempt_task("runner", start_time_stamp)
    ot = OpenTasks(LOGGER)
    assert len(ot) == 0
    ot.add_task(t1)
    # should be there now
    assert len(ot) == 1
    assert ot.get_task(1) == t1
    ot.add_task(t1)
    # should be there now
    assert len(ot) == 1
    assert ot.get_task(1) == t1


def test_remove_task_with_duration():
    time_stamp = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=5, microsecond=100222)

    t1 = Task(1, "run command example", time_stamp, name="example run",
              desc="this is a bologna command that does nothing", duration=500, max_attempts=3)
    start_time_stamp = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=6, microsecond=100222)
    t1.attempt_task("runner", start_time_stamp)

    t2 = Task(2, "run command example 2", time_stamp, name="example run 2",
              desc="this is a bologna command that does nothing",  max_attempts=3)
    start_time_stamp = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=8, microsecond=100222)
    t2.attempt_task("runner", start_time_stamp)
    ot = OpenTasks(LOGGER)
    ot.add_task(t1)
    ot.add_task(t2)
    assert len(ot) == 2
    assert ot.get_task(1) == t1
    assert ot.get_task(2) == t2

    # remove task with duration
    ot.remove_task(1)
    assert len(ot) == 1
    assert ot.get_task(1) is None
    assert ot.get_task(2) == t2


def test_remove_task_without_duration():
    time_stamp = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=5, microsecond=100222)

    t1 = Task(1, "run command example", time_stamp, name="example run",
              desc="this is a bologna command that does nothing", duration=500, max_attempts=3)
    start_time_stamp = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=6, microsecond=100222)
    t1.attempt_task("runner", start_time_stamp)

    t2 = Task(2, "run command example 2", time_stamp, name="example run 2",
              desc="this is a bologna command that does nothing",  max_attempts=3)
    start_time_stamp = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=8, microsecond=100222)
    t2.attempt_task("runner", start_time_stamp)
    ot = OpenTasks(LOGGER)
    ot.add_task(t1)
    ot.add_task(t2)
    assert len(ot) == 2
    assert ot.get_task(1) == t1
    assert ot.get_task(2) == t2

    # remove task without duration
    ot.remove_task(2)
    assert len(ot) == 1
    assert ot.get_task(1) == t1
    assert ot.get_task(2) is None


def test_remove_task_that_doesnt_exist():
    time_stamp = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=5, microsecond=100222)

    t1 = Task(1, "run command example", time_stamp, name="example run",
              desc="this is a bologna command that does nothing", duration=500, max_attempts=3)
    start_time_stamp = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=6, microsecond=100222)
    t1.attempt_task("runner", start_time_stamp)

    t2 = Task(2, "run command example 2", time_stamp, name="example run 2",
              desc="this is a bologna command that does nothing", duration=90, max_attempts=3)
    start_time_stamp = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=8, microsecond=100222)
    t2.attempt_task("runner", start_time_stamp)
    ot = OpenTasks(LOGGER)
    ot.add_task(t1)
    ot.add_task(t2)
    assert len(ot) == 2
    assert ot.get_task(1) == t1
    assert ot.get_task(2) == t2

    # remove non-existent task
    ot.remove_task("id that doesn't exist")

    assert len(ot) == 2
    assert ot.get_task(1) == t1
    assert ot.get_task(2) == t2


# def test_get_task():
    # pass
    # well tested in other functions


def test_get_task_that_doesnt_exist():
    time_stamp = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=5, microsecond=100222)

    t1 = Task(1, "run command example", time_stamp, name="example run",
              desc="this is a bologna command that does nothing", duration=500, max_attempts=3)
    start_time_stamp = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=6, microsecond=100222)
    t1.attempt_task("runner", start_time_stamp)

    t2 = Task(2, "run command example 2", time_stamp, name="example run 2",
              desc="this is a bologna command that does nothing", duration=90, max_attempts=3)
    start_time_stamp = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=8, microsecond=100222)
    t2.attempt_task("runner", start_time_stamp)
    ot = OpenTasks(LOGGER)
    ot.add_task(t1)
    ot.add_task(t2)

    assert ot.get_task("non existant id") is None


def test_all_tasks():
    time_stamp = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=5, microsecond=100222)

    t1 = Task(1, "run command example", time_stamp, name="example run",
              desc="this is a bologna command that does nothing", duration=500, max_attempts=3)
    start_time_stamp = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=6, microsecond=100222)
    t1.attempt_task("runner", start_time_stamp)

    t2 = Task(2, "run command example 2", time_stamp, name="example run 2",
              desc="this is a bologna command that does nothing", duration=90, max_attempts=3)
    start_time_stamp = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=8, microsecond=100222)
    t2.attempt_task("runner", start_time_stamp)
    ot = OpenTasks(LOGGER)
    ot.add_task(t1)
    ot.add_task(t2)

    # should be sorted by creation time
    tasks = ot.all_tasks()
    assert len(tasks) == 2
    assert tasks[0] == t1
    assert tasks[1] == t2


def test_all_tasks_added_in_reverse_order():
    # same as test_all_tasks but the creation times are reversed
    time_stamp = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=5, microsecond=100222)

    t1 = Task(1, "run command example", time_stamp, name="example run",
              desc="this is a bologna command that does nothing", duration=500, max_attempts=3)
    start_time_stamp = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=8, microsecond=100222)
    t1.attempt_task("runner", start_time_stamp)

    t2 = Task(2, "run command example 2", time_stamp, name="example run 2",
              desc="this is a bologna command that does nothing", duration=90, max_attempts=3)
    start_time_stamp = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=6, microsecond=100222)
    t2.attempt_task("runner", start_time_stamp)
    ot = OpenTasks(LOGGER)
    ot.add_task(t1)
    ot.add_task(t2)

    # should be sorted by creation time
    tasks = ot.all_tasks()
    assert len(tasks) == 2
    assert tasks[0] == t1
    assert tasks[1] == t2


# def test_len():
    # pass
    # tested well enough in other tests

