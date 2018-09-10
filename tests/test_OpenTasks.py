from simple_task_server import Task
from simple_task_server import OpenTasks
from datetime import datetime
import logging

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

