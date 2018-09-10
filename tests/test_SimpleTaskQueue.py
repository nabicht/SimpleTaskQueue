from simple_task_server import SimpleTaskQueue
from simple_task_server import Task
from datetime import datetime
import pytest
import logging

LOGGER = logging.getLogger(__name__)


@pytest.fixture
def basic_simple_task_queue():
    time_stamp = datetime.now()
    tq = SimpleTaskQueue(LOGGER)
    t1 = Task(1, "run command example", time_stamp, name="example run", desc="this is a bologna command that does nothing")
    tq.add_task(t1)
    t2 = Task(2, "python -m some_script", time_stamp, name="example python run that will only try to run once and should last 3 minutes",
              duration=3*60, max_attempts=1)
    tq.add_task(t2)
    t3 = Task(3, "cd my_directory; python -m some_script", time_stamp, name="multiple commands", desc="an example of multiple commands in  one task")
    tq.add_task(t3)
    return tq


def test_empty_task_queue():
    tq = SimpleTaskQueue(LOGGER)
    # empty test queue
    assert len(tq) == 0
    assert tq.next_task() is None


def test_get_non_existent_task_from_empty_queue():
    # things should just work. no failures, no impact.
    tq = SimpleTaskQueue(LOGGER)
    assert tq.task("some_identifier") is None
    assert tq.task(24601) is None
    # test no impact
    assert len(tq) == 0
    assert tq.next_task() is None


def test_remove_non_existent_task_from_empty_queue():
    tq = SimpleTaskQueue(LOGGER)
    tq.remove_task("")
    tq.remove_task("random identifier")
    tq.remove_task(45663.00)
    tq.remove_task(24601)
    # test no impact
    assert len(tq) == 0
    assert tq.next_task() is None


def test_populated_simple_task_queue(basic_simple_task_queue):
    tq = basic_simple_task_queue
    assert len(tq) == 3


def test_add_task():
    tq = SimpleTaskQueue(LOGGER)
    t = Task(15, 'run command', datetime.now())
    tq.add_task(t)
    assert len(tq) == 1
    assert tq.task(15) == t


def test_get_task(basic_simple_task_queue):
    tq = basic_simple_task_queue
    assert isinstance(tq.task(1), Task)
    t = tq.task(1)
    assert t.name == "example run"
    assert t.task_id() == 1
    assert t.cmd == "run command example"


def test_get_non_existent_task(basic_simple_task_queue):
    tq = basic_simple_task_queue
    assert tq.task(15) is None
    assert tq.task("random identifier") is None
    assert tq.task(24601) is None
    assert tq.task(54663.00) is None


