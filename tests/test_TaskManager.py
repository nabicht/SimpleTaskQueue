
from simple_task_queue import SimpleTaskQueue
from simple_task_queue import Task
from simple_task_queue import TaskManager
from datetime import datetime
import pytest

@pytest.fixture
def basic_task_manager():
    time_stamp = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=5, microsecond=100222)
    tq = SimpleTaskQueue()
    t1 = Task(1, "run command example", time_stamp, name="example run", desc="this is a bologna command that does nothing")
    tq.add_task(t1)
    t2 = Task(2, "python -m some_script", time_stamp, name="example python run that will only try to run once and should last 3 minutes")
    tq.add_task(t2)
    t3 = Task(3, "cd my_directory; python -m some_script", time_stamp, name="multiple commands", desc="an example of multiple commands in  one task")
    tq.add_task(t3)
    tm = TaskManager(None)
    tm._todo_queue = tq
    return tm


def test_new_task_manager():
    # should be empty
    tm = TaskManager(None)
    assert len(tm._todo_queue) == 0
    assert len(tm._in_process) == 0
    assert len(tm._done) == 0
    # next task to do should be None
    task, attempt = tm.start_next_attempt("runner", datetime.now())
    assert task is None
    assert attempt is None


def test_add_task():
    tm = TaskManager(None)
    t1 = Task(1, "run command example", datetime.now(), name="example run", desc="this is a bologna command that does nothing")
    tm.add_task(t1)
    assert len(tm._todo_queue) == 1
    assert len(tm._in_process) == 0
    assert len(tm._done) == 0

    t2 = Task(2, "another run command example", datetime.now(), name="example run", desc="this is a bologna command that does nothing")
    tm.add_task(t2)
    assert len(tm._todo_queue) == 2
    assert len(tm._in_process) == 0
    assert len(tm._done) == 0


def test_add_same_task_id_more_than_once():
    tm = TaskManager(None)
    t1 = Task(1, "run command example", datetime.now(), name="example run", desc="this is a bologna command that does nothing")
    tm.add_task(t1)
    assert len(tm._todo_queue) == 1
    assert len(tm._in_process) == 0
    assert len(tm._done) == 0

    t2 = Task(1, "another run command example", datetime.now(), name="example run", desc="this is a bologna command that does nothing")
    tm.add_task(t2)
    assert len(tm._todo_queue) == 1
    assert len(tm._in_process) == 0
    assert len(tm._done) == 0


def test_add_same_task_more_than_once():
    tm = TaskManager(None)
    t1 = Task(1, "run command example", datetime.now(), name="example run", desc="this is a bologna command that does nothing")
    tm.add_task(t1)
    assert len(tm._todo_queue) == 1
    assert len(tm._in_process) == 0
    assert len(tm._done) == 0

    tm.add_task(t1)
    assert len(tm._todo_queue) == 1
    assert len(tm._in_process) == 0
    assert len(tm._done) == 0


def test_next_task_no_expired_or_failed(basic_task_manager):
    # make sure baseline is right
    assert len(basic_task_manager._todo_queue) == 3
    assert len(basic_task_manager._in_process) == 0
    assert len(basic_task_manager._done) == 0

    time = datetime.now()
    runner = "runner1"
    task, attempt = basic_task_manager.start_next_attempt(runner, time)
    assert task.task_id() == 1
    assert len(task._attempts) == 1
    assert task.most_recent_attempt().id() == attempt.id()  # TODO attempt equality
    assert attempt.start_time == time
    assert attempt.runner == runner
    assert attempt.started()
    assert not attempt.failed()
    assert not attempt.completed()
    assert attempt.in_process()
    assert len(basic_task_manager._todo_queue) == 2
    assert len(basic_task_manager._in_process) == 1
    assert len(basic_task_manager._done) == 0

    time = datetime.now()
    runner = "runner2"
    task, attempt = basic_task_manager.start_next_attempt(runner, time)
    assert len(task._attempts) == 1
    assert task.task_id() == 2
    assert task.most_recent_attempt().id() == attempt.id()  # TODO attempt equality
    assert attempt.start_time == time
    assert attempt.runner == runner
    assert attempt.started()
    assert not attempt.failed()
    assert not attempt.completed()
    assert attempt.in_process()
    assert len(basic_task_manager._todo_queue) == 1
    assert len(basic_task_manager._in_process) == 2
    assert len(basic_task_manager._done) == 0

    time = datetime.now()
    runner = "runner1"
    task, attempt = basic_task_manager.start_next_attempt(runner, time)
    assert len(task._attempts) == 1
    assert task.task_id() == 3
    assert task.most_recent_attempt().id() == attempt.id()  # TODO attempt equality
    assert attempt.start_time == time
    assert attempt.runner == runner
    assert attempt.started()
    assert not attempt.failed()
    assert not attempt.completed()
    assert attempt.in_process()
    assert len(basic_task_manager._todo_queue) == 0
    assert len(basic_task_manager._in_process) == 3
    assert len(basic_task_manager._done) == 0


def test_mark_completed_when_in_process(basic_task_manager):
    # make sure baseline is what we expect
    assert len(basic_task_manager._todo_queue) == 3
    assert len(basic_task_manager._in_process) == 0
    assert len(basic_task_manager._done) == 0

    start_time = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=30, microsecond=100222)
    complete_time = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=40, microsecond=100222)

    # start one up and make sure as expected
    task, attempt = basic_task_manager.start_next_attempt("runner", start_time)
    assert task.task_id() == 1
    assert len(basic_task_manager._todo_queue) == 2
    assert len(basic_task_manager._in_process) == 1
    assert len(basic_task_manager._done) == 0

    # mark completed
    basic_task_manager.complete_attempt(task.task_id(), attempt.id(), complete_time)

    assert len(basic_task_manager._todo_queue) == 2
    assert len(basic_task_manager._in_process) == 0
    assert len(basic_task_manager._done) == 1
    assert task.is_completed()
    assert not task.in_process()
    assert task.started()
    assert not task.failed()
    assert task.open_time() == 35.0


def test_mark_completed_when_multiple_attemtps_in_process(basic_task_manager):
    # make sure baseline is what we expect
    assert len(basic_task_manager._todo_queue) == 3
    assert len(basic_task_manager._in_process) == 0
    assert len(basic_task_manager._done) == 0

    start_time = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=30, microsecond=100222)
    complete_time = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=40, microsecond=100222)

    # start one up and make sure as expected
    task1, attempt1 = basic_task_manager.start_next_attempt("runner", start_time)
    assert task1.task_id() == 1
    task2, attempt2 = basic_task_manager.start_next_attempt("runner", start_time)
    assert task2.task_id() == 2

    assert len(basic_task_manager._todo_queue) == 1
    assert len(basic_task_manager._in_process) == 2
    assert len(basic_task_manager._done) == 0

    # mark completed
    basic_task_manager.complete_attempt(task1.task_id(), attempt1.id(), complete_time)

    assert len(basic_task_manager._todo_queue) == 1
    assert len(basic_task_manager._in_process) == 1
    assert len(basic_task_manager._done) == 1
    assert task1.is_completed()
    assert not task1.in_process()
    assert task1.started()
    assert not task1.failed()
    assert task1.open_time() == 35.0

    assert not task2.is_completed()
    assert task2.started()
    assert not task2.failed()
    assert task2.open_time() is None

def test_mark_completed_when_multiple_attempts_in_process_other_order(basic_task_manager):
    # make sure baseline is what we expect
    assert len(basic_task_manager._todo_queue) == 3
    assert len(basic_task_manager._in_process) == 0
    assert len(basic_task_manager._done) == 0

    start_time = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=30, microsecond=100222)
    complete_time = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=40, microsecond=100222)

    # start one up and make sure as expected
    task1, attempt1 = basic_task_manager.start_next_attempt("runner", start_time)
    assert task1.task_id() == 1
    task2, attempt2 = basic_task_manager.start_next_attempt("runner", start_time)
    assert task2.task_id() == 2

    assert len(basic_task_manager._todo_queue) == 1
    assert len(basic_task_manager._in_process) == 2
    assert len(basic_task_manager._done) == 0

    # mark completed
    basic_task_manager.complete_attempt(task2.task_id(), attempt2.id(), complete_time)

    assert len(basic_task_manager._todo_queue) == 1
    assert len(basic_task_manager._in_process) == 1
    assert len(basic_task_manager._done) == 1
    assert task2.is_completed()
    assert not task2.in_process()
    assert task2.started()
    assert not task2.failed()
    assert task2.open_time() == 35.0

    assert not task1.is_completed()
    assert task1.started()
    assert not task1.failed()
    assert task1.open_time() is None


def test_mark_completed_when_done():
    time_stamp1 = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=5, microsecond=100222)
    time_stamp2 = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=10, microsecond=0)
    time_stamp3 = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=15, microsecond=222222)
    tq = SimpleTaskQueue()
    t1 = Task(1, "run command example", time_stamp1, name="example run",
              desc="this is a bologna command that does nothing",
              duration=100, max_attempts=5)
    tq.add_task(t1)
    t2 = Task(2, "python -m some_script", time_stamp2,
              name="example python run that will only try to run once and should last 3 minutes",
              duration=180, max_attempts=1)
    tq.add_task(t2)
    t3 = Task(3, "cd my_directory; python -m some_script", time_stamp3, name="multiple commands",
              desc="an example of multiple commands in  one task", duration=200)
    tq.add_task(t3)
    basic_task_manager = TaskManager(None)
    basic_task_manager._todo_queue = tq

    # make sure baseline is what we expect
    assert len(basic_task_manager._todo_queue) == 3
    assert len(basic_task_manager._in_process) == 0
    assert len(basic_task_manager._done) == 0

    start_time = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=30, microsecond=500000)
    complete_time = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=40, microsecond=600222)

    # start one up and make sure as expected
    task, attempt = basic_task_manager.start_next_attempt("runner", start_time)
    assert task.task_id() == 1
    assert len(basic_task_manager._todo_queue) == 2
    assert len(basic_task_manager._in_process) == 1
    assert len(basic_task_manager._done) == 0

    # start a second attempt
    second_start_time = datetime(year=2018, month=8, day=13, hour=5, minute=13, second=30, microsecond=100222)
    task2, attempt2 = basic_task_manager.start_next_attempt("runner2", second_start_time)
    assert task2.task_id() == 1
    assert attempt2.id() != attempt.id()
    assert len(basic_task_manager._todo_queue) == 2
    assert len(basic_task_manager._in_process) == 1
    assert len(basic_task_manager._done) == 0

    # mark completed
    basic_task_manager.complete_attempt(task.task_id(), attempt.id(), complete_time)

    assert len(basic_task_manager._todo_queue) == 2
    assert len(basic_task_manager._in_process) == 0
    assert len(basic_task_manager._done) == 1
    assert task.is_completed()
    assert not task.in_process()
    assert task.started()
    assert not task.failed()
    assert task.open_time() == 35.5

    # mark completed again
    second_complete_time = datetime(year=2018, month=8, day=13, hour=5, minute=10+3, second=40, microsecond=100222)
    basic_task_manager.complete_attempt(task2.task_id(), attempt2.id(), second_complete_time)
    # should have no impact; all should be the same
    assert len(basic_task_manager._todo_queue) == 2
    assert len(basic_task_manager._in_process) == 0
    assert len(basic_task_manager._done) == 1
    assert task.is_completed()
    assert not task.in_process()
    assert task.started()
    assert not task.failed()
    assert task.open_time() == 35.5


def test_mark_completed_when_attempt_unknown():
    time_stamp1 = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=5, microsecond=100222)
    tq = SimpleTaskQueue()
    t1 = Task(1, "run command example", time_stamp1, name="example run",
              desc="this is a bologna command that does nothing",
              duration=100, max_attempts=2)
    tq.add_task(t1)
    tm = TaskManager(None)
    tm._todo_queue = tq

    completed_time_stamp = datetime(year=2018, month=8, day=13, hour=5, minute=10, second=44, microsecond=100222)
    assert tm.complete_attempt(t1.task_id(), "some_random_unknown_attempt_id", completed_time_stamp) is False


def test_find_task_in_todo(basic_task_manager):
    t = basic_task_manager._find_task(1, todo=True, in_process=True, done=True)
    assert t is not None
    assert t.task_id() == 1



# def test_find_in_inprocess(basic_task_manager):