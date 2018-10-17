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

from simple_task_manager import TaskManager
from datetime import datetime
import logging
import pytest
import tempfile

LOGGER = logging.getLogger(__name__)


@pytest.fixture
def task_manager():
    temp = tempfile.NamedTemporaryFile()
    task_manager = TaskManager(temp.name, LOGGER)
    yield task_manager
    task_manager.close()
    temp.close()


def test_task_to_retry_none_to_retry(task_manager):
    time_stamp = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=5, microsecond=100222)

    t1 = task_manager.add_task("run command example", time_stamp, name="example run",
                               desc="this is a bologna command that does nothing", duration=100)

    start_time_stamp = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=6, microsecond=100222)
    next_task, attempt1 = task_manager.start_next_attempt("runner", start_time_stamp)
    assert next_task.task_id() == t1.task_id()

    t2 = task_manager.add_task("run command example 2", time_stamp, name="example run 2",
                               desc="this is a bologna command that does nothing", duration=120)
    start_time_stamp = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=6, microsecond=100222)
    next_task, attempt2 = task_manager.start_next_attempt("runner", start_time_stamp)
    assert next_task.task_id() == t2.task_id()

    current_time = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=8, microsecond=100222)
    task, failed_tasks = task_manager._in_process.task_to_retry(current_time)
    # should be no failed tasks
    assert task is None
    assert len(failed_tasks) == 0


def test_task_to_retry_first_started_retry(task_manager):
    time_stamp = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=5, microsecond=100222)

    t1 = task_manager.add_task("run command example", time_stamp, name="example run",
                               desc="this is a bologna command that does nothing", duration=100, max_attempts=3)
    start_time_stamp = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=6, microsecond=100222)
    t1_a1 = task_manager.start_next_attempt("runner", start_time_stamp)

    t2 = task_manager.add_task("run command example 2", time_stamp, name="example run 2",
                               desc="this is a bologna command that does nothing", duration=820, max_attempts=3)
    start_time_stamp = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=6, microsecond=100222)
    t2_a1 = task_manager.start_next_attempt("runner", start_time_stamp)

    current_time = datetime(year=2018, month=8, day=13, hour=5, minute=12, second=8, microsecond=100222)
    task, failed_tasks = task_manager._in_process.task_to_retry(current_time)
    # should be no failed tasks
    assert task.task_id() == t1.task_id()
    assert task.cmd == t1.cmd
    assert task.name == t1.name
    assert task.desc == t1.desc
    assert task.duration == t1.duration
    assert task.max_attempts == t1.max_attempts
    assert task.created_time == t1.created_time
    assert len(failed_tasks) == 0


def test_task_to_retry_second_started_retry(task_manager):
    time_stamp = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=5, microsecond=100222)

    t1 = task_manager.add_task("run command example", time_stamp, name="example run",
                               desc="this is a bologna command that does nothing", duration=500, max_attempts=3)
    start_time_stamp = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=6, microsecond=100222)
    t1_a1 = task_manager.start_next_attempt("runner", start_time_stamp)

    t2 = task_manager.add_task("run command example 2", time_stamp, name="example run 2",
                               desc="this is a bologna command that does nothing", duration=90, max_attempts=3)
    start_time_stamp = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=8, microsecond=100222)
    t2_a1 = task_manager.start_next_attempt("runner", start_time_stamp)

    current_time = datetime(year=2018, month=8, day=13, hour=5, minute=12, second=8, microsecond=100222)
    task, failed_tasks = task_manager._in_process.task_to_retry(current_time)
    # should be no failed tasks
    assert task.task_id() == t2.task_id()
    assert task.cmd == t2.cmd
    assert task.name == t2.name
    assert task.desc == t2.desc
    assert task.duration == t2.duration
    assert task.max_attempts == t2.max_attempts
    assert task.created_time == t2.created_time
    assert len(failed_tasks) == 0
