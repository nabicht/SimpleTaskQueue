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
import logging
import datetime

LOGGER = logging.getLogger(__name__)


def test_attempt_task():
    task = Task("1234", "some cmd", datetime.datetime(2018, 1, 15, 12, 35, 0), max_attempts=2)
    # no attempts
    assert task.num_attempts() == 0
    assert task.most_recent_attempt() is None

    attempt_time = datetime.datetime(2018, 1, 15, 12, 35, 30)
    attempt = task.attempt_task("runner 1", attempt_time)

    assert task.num_attempts() == 1
    assert task.most_recent_attempt() == attempt

    attempt_time_2 = datetime.datetime(2018, 1, 15, 12, 35, 45)
    attempt_2 = task.attempt_task("runner 2", attempt_time_2)
    assert task.num_attempts() == 2
    assert task.most_recent_attempt() == attempt_2

    # now get another attempt (which will be greater than max_attempts so should fail.
    attempt_time_3 = datetime.datetime(2018, 1, 15, 12, 36, 10)
    attempt_3 = task.attempt_task("runner 3", attempt_time_3)
    assert attempt_3 is None
    assert task.num_attempts() == 2
    assert task.most_recent_attempt() == attempt_2


def test_get_attempt():
    task = Task("1234", "some cmd", datetime.datetime(2018, 1, 15, 12, 35, 0), max_attempts=2)
    attempt_time = datetime.datetime(2018, 1, 15, 12, 35, 30)
    attempt_1 = task.attempt_task("runner 1", attempt_time)
    attempt_time_2 = datetime.datetime(2018, 1, 15, 12, 35, 45)
    attempt_2 = task.attempt_task("runner 2", attempt_time_2)

    # no id should be None
    assert task.get_attempt("fake_attempt_id") is None
    assert attempt_1 == task.get_attempt(attempt_1._attempt_id)
    assert attempt_2 == task.get_attempt(attempt_2._attempt_id)


# def test_most_recent_attempt():
    # properly tested in test_attempt_task()
    # pass


# def test_num_attempts():
    # properly tested in test_attempt_task()
    # pass


def test_is_completed_no_attempts():
    task = Task("1234", "some cmd", datetime.datetime(2018, 1, 15, 12, 35, 0), max_attempts=2)
    assert task.is_completed() is False


def test_is_completed_open_attempt():
    task = Task("1234", "some cmd", datetime.datetime(2018, 1, 15, 12, 35, 0), max_attempts=2)
    attempt_time = datetime.datetime(2018, 1, 15, 12, 35, 30)
    task.attempt_task("runner 1", attempt_time)
    assert task.is_completed() is False


def test_is_completed_completed_attempt():
    task = Task("1234", "some cmd", datetime.datetime(2018, 1, 15, 12, 35, 0), max_attempts=2)
    attempt_time = datetime.datetime(2018, 1, 15, 12, 35, 30)
    attempt = task.attempt_task("runner 1", attempt_time)
    attempt.mark_completed(datetime.datetime(2018, 1, 15, 12, 35, 45))
    assert task.is_completed() is True


def test_is_completed_failed_attempt():
    task = Task("1234", "some cmd", datetime.datetime(2018, 1, 15, 12, 35, 0), max_attempts=2)
    attempt_time = datetime.datetime(2018, 1, 15, 12, 35, 30)
    attempt = task.attempt_task("runner 1", attempt_time)
    attempt.mark_failed(datetime.datetime(2018, 1, 15, 12, 35, 45))
    assert task.is_completed() is False


def test_is_completed_all_attempts_failed():
    task = Task("1234", "some cmd", datetime.datetime(2018, 1, 15, 12, 35, 0), max_attempts=2)
    attempt_time = datetime.datetime(2018, 1, 15, 12, 35, 30)
    attempt = task.attempt_task("runner 1", attempt_time)
    attempt.mark_failed(datetime.datetime(2018, 1, 15, 12, 35, 45))
    attempt_time_2 = datetime.datetime(2018, 1, 15, 12, 35, 45)
    attempt_2 = task.attempt_task("runner 2", attempt_time_2)
    attempt_2.mark_failed(datetime.datetime(2018, 1, 15, 12, 35, 48))
    assert task.is_completed() is False


def test_is_completed_all_attempts_completed():
    task = Task("1234", "some cmd", datetime.datetime(2018, 1, 15, 12, 35, 0), max_attempts=2)
    attempt_time = datetime.datetime(2018, 1, 15, 12, 35, 30)
    attempt = task.attempt_task("runner 1", attempt_time)
    attempt.mark_completed(datetime.datetime(2018, 1, 15, 12, 35, 45))
    attempt_time_2 = datetime.datetime(2018, 1, 15, 12, 35, 45)
    attempt_2 = task.attempt_task("runner 2", attempt_time_2)
    attempt_2.mark_completed(datetime.datetime(2018, 1, 15, 12, 35, 48))
    assert task.is_completed() is True


def test_is_completed_mixed_attempts_failed_completed():
    task = Task("1234", "some cmd", datetime.datetime(2018, 1, 15, 12, 35, 0), max_attempts=2)
    attempt_time = datetime.datetime(2018, 1, 15, 12, 35, 30)
    attempt = task.attempt_task("runner 1", attempt_time)
    attempt.mark_completed(datetime.datetime(2018, 1, 15, 12, 35, 45))
    attempt_time_2 = datetime.datetime(2018, 1, 15, 12, 35, 45)
    attempt_2 = task.attempt_task("runner 2", attempt_time_2)
    attempt_2.mark_failed(datetime.datetime(2018, 1, 15, 12, 35, 48))
    assert task.is_completed() is True


def test_is_completed_mixed_attempts_failed_completed_2():
    task = Task("1234", "some cmd", datetime.datetime(2018, 1, 15, 12, 35, 0), max_attempts=2)
    attempt_time = datetime.datetime(2018, 1, 15, 12, 35, 30)
    attempt = task.attempt_task("runner 1", attempt_time)
    attempt.mark_failed(datetime.datetime(2018, 1, 15, 12, 35, 45))
    attempt_time_2 = datetime.datetime(2018, 1, 15, 12, 35, 45)
    attempt_2 = task.attempt_task("runner 2", attempt_time_2)
    attempt_2.mark_completed(datetime.datetime(2018, 1, 15, 12, 35, 48))
    assert task.is_completed() is True


def test_is_in_process_no_attempts():
    task = Task("1234", "some cmd", datetime.datetime(2018, 1, 15, 12, 35, 0), max_attempts=2)
    assert task.is_in_process() is False


def test_is_in_process_open_attempt():
    task = Task("1234", "some cmd", datetime.datetime(2018, 1, 15, 12, 35, 0), max_attempts=2)
    attempt_time = datetime.datetime(2018, 1, 15, 12, 35, 30)
    task.attempt_task("runner 1", attempt_time)
    assert task.is_in_process() is True


def test_is_in_process_one_fail():
    task = Task("1234", "some cmd", datetime.datetime(2018, 1, 15, 12, 35, 0), max_attempts=2)
    attempt_time = datetime.datetime(2018, 1, 15, 12, 35, 30)
    attempt = task.attempt_task("runner 1", attempt_time)
    attempt.mark_failed(datetime.datetime(2018, 1, 15, 12, 35, 45))
    assert task.is_in_process() is True


def test_is_in_process_all_fail():
    task = Task("1234", "some cmd", datetime.datetime(2018, 1, 15, 12, 35, 0), max_attempts=2)
    attempt_time = datetime.datetime(2018, 1, 15, 12, 35, 30)
    attempt = task.attempt_task("runner 1", attempt_time)
    attempt.mark_failed(datetime.datetime(2018, 1, 15, 12, 35, 45))
    attempt_time_2 = datetime.datetime(2018, 1, 15, 12, 35, 45)
    attempt_2 = task.attempt_task("runner 2", attempt_time_2)
    attempt_2.mark_failed(datetime.datetime(2018, 1, 15, 12, 35, 48))
    assert task.is_in_process() is False


def test_started_new_task():
    task = Task("1234", "some cmd", datetime.datetime(2018, 1, 15, 12, 35, 0), max_attempts=2)
    assert task.is_started() is False


def test_started_open_attempt():
    task = Task("1234", "some cmd", datetime.datetime(2018, 1, 15, 12, 35, 0), max_attempts=2)
    attempt_time = datetime.datetime(2018, 1, 15, 12, 35, 30)
    task.attempt_task("runner 1", attempt_time)
    assert task.is_started() is True


def test_started_failed_attempt():
    task = Task("1234", "some cmd", datetime.datetime(2018, 1, 15, 12, 35, 0), max_attempts=2)
    attempt_time = datetime.datetime(2018, 1, 15, 12, 35, 30)
    attempt = task.attempt_task("runner 1", attempt_time)
    attempt.mark_failed(datetime.datetime(2018, 1, 15, 12, 35, 45))
    assert task.is_started() is True


def test_started_both_failed_attempts():
    task = Task("1234", "some cmd", datetime.datetime(2018, 1, 15, 12, 35, 0), max_attempts=2)
    attempt_time = datetime.datetime(2018, 1, 15, 12, 35, 30)
    attempt = task.attempt_task("runner 1", attempt_time)
    attempt.mark_failed(datetime.datetime(2018, 1, 15, 12, 35, 45))
    attempt_time_2 = datetime.datetime(2018, 1, 15, 12, 35, 45)
    attempt_2 = task.attempt_task("runner 2", attempt_time_2)
    attempt_2.mark_failed(datetime.datetime(2018, 1, 15, 12, 35, 48))
    assert task.is_started() is True


def test_started_one_completed_attempt():
    task = Task("1234", "some cmd", datetime.datetime(2018, 1, 15, 12, 35, 0), max_attempts=2)
    attempt_time = datetime.datetime(2018, 1, 15, 12, 35, 30)
    attempt = task.attempt_task("runner 1", attempt_time)
    attempt.mark_completed(datetime.datetime(2018, 1, 15, 12, 35, 45))
    assert task.is_started() is True


def test_is_failed_new_task():
    task = Task("1234", "some cmd", datetime.datetime(2018, 1, 15, 12, 35, 0), max_attempts=2)
    assert task.is_failed() is False


def test_is_failed_one_fail_attempt():
    task = Task("1234", "some cmd", datetime.datetime(2018, 1, 15, 12, 35, 0), max_attempts=2)
    attempt_time = datetime.datetime(2018, 1, 15, 12, 35, 30)
    attempt = task.attempt_task("runner 1", attempt_time)
    attempt.mark_failed(datetime.datetime(2018, 1, 15, 12, 35, 45))
    assert task.is_failed() is False


def test_is_failed_all_attempts_failed():
    task = Task("1234", "some cmd", datetime.datetime(2018, 1, 15, 12, 35, 0), max_attempts=2)
    attempt_time = datetime.datetime(2018, 1, 15, 12, 35, 30)
    attempt = task.attempt_task("runner 1", attempt_time)
    attempt.mark_failed(datetime.datetime(2018, 1, 15, 12, 35, 45))
    attempt_time_2 = datetime.datetime(2018, 1, 15, 12, 35, 45)
    attempt_2 = task.attempt_task("runner 2", attempt_time_2)
    attempt_2.mark_failed(datetime.datetime(2018, 1, 15, 12, 35, 48))
    assert task.is_failed() is True


def test_is_failed_open_attempt():
    task = Task("1234", "some cmd", datetime.datetime(2018, 1, 15, 12, 35, 0), max_attempts=2)
    attempt_time = datetime.datetime(2018, 1, 15, 12, 35, 30)
    task.attempt_task("runner 1", attempt_time)
    assert task.is_failed() is False


def test_is_failed_mixed_attempts_failed_completed():
    task = Task("1234", "some cmd", datetime.datetime(2018, 1, 15, 12, 35, 0), max_attempts=2)
    attempt_time = datetime.datetime(2018, 1, 15, 12, 35, 30)
    attempt = task.attempt_task("runner 1", attempt_time)
    attempt.mark_completed(datetime.datetime(2018, 1, 15, 12, 35, 45))
    attempt_time_2 = datetime.datetime(2018, 1, 15, 12, 35, 45)
    attempt_2 = task.attempt_task("runner 2", attempt_time_2)
    attempt_2.mark_failed(datetime.datetime(2018, 1, 15, 12, 35, 48))
    assert task.is_failed() is False


def test_is_failed_mixed_attempts_failed_completed_2():
    task = Task("1234", "some cmd", datetime.datetime(2018, 1, 15, 12, 35, 0), max_attempts=2)
    attempt_time = datetime.datetime(2018, 1, 15, 12, 35, 30)
    attempt = task.attempt_task("runner 1", attempt_time)
    attempt.mark_failed(datetime.datetime(2018, 1, 15, 12, 35, 45))
    attempt_time_2 = datetime.datetime(2018, 1, 15, 12, 35, 45)
    attempt_2 = task.attempt_task("runner 2", attempt_time_2)
    attempt_2.mark_completed(datetime.datetime(2018, 1, 15, 12, 35, 48))
    assert task.is_failed() is False


def test_open_time_new_task():
    task = Task("1234", "some cmd", datetime.datetime(2018, 1, 15, 12, 35, 0), max_attempts=2)
    assert task.open_time() is None


def test_open_time_open_attempt():
    task = Task("1234", "some cmd", datetime.datetime(2018, 1, 15, 12, 35, 0), max_attempts=2)
    attempt_time = datetime.datetime(2018, 1, 15, 12, 35, 30)
    task.attempt_task("runner 1", attempt_time)
    assert task.open_time() is None


def test_open_time_all_attempts_failed():
    task = Task("1234", "some cmd", datetime.datetime(2018, 1, 15, 12, 35, 0), max_attempts=2)
    attempt_time = datetime.datetime(2018, 1, 15, 12, 35, 30)
    attempt = task.attempt_task("runner 1", attempt_time)
    attempt.mark_failed(datetime.datetime(2018, 1, 15, 12, 35, 45))
    attempt_time_2 = datetime.datetime(2018, 1, 15, 12, 35, 45)
    attempt_2 = task.attempt_task("runner 2", attempt_time_2)
    attempt_2.mark_failed(datetime.datetime(2018, 1, 15, 12, 35, 48))
    assert task.open_time() is None


def test_open_time_one_attempt_failed():
    task = Task("1234", "some cmd", datetime.datetime(2018, 1, 15, 12, 35, 0), max_attempts=2)
    attempt_time = datetime.datetime(2018, 1, 15, 12, 35, 30)
    attempt = task.attempt_task("runner 1", attempt_time)
    attempt.mark_failed(datetime.datetime(2018, 1, 15, 12, 35, 45))
    assert task.open_time() is None


def test_open_time_one_completed_attempt():
    task_created_time = datetime.datetime(2018, 1, 15, 12, 35, 0)
    task = Task("1234", "some cmd", task_created_time, max_attempts=2)
    attempt_time = datetime.datetime(2018, 1, 15, 12, 35, 30)
    attempt = task.attempt_task("runner 1", attempt_time)
    attempt_completed_time = datetime.datetime(2018, 1, 15, 12, 35, 45)
    attempt.mark_completed(attempt_completed_time)
    assert task.open_time() == (attempt_completed_time - task_created_time).total_seconds()


def test_open_time_two_completed_attempts():
    task_created_time = datetime.datetime(2018, 1, 15, 12, 35, 0)
    task = Task("1234", "some cmd", task_created_time, max_attempts=2)
    attempt_time = datetime.datetime(2018, 1, 15, 12, 35, 30)
    attempt = task.attempt_task("runner 1", attempt_time)
    attempt_completed_time = datetime.datetime(2018, 1, 15, 12, 35, 45)
    attempt.mark_completed(attempt_completed_time)
    attempt_time_2 = datetime.datetime(2018, 1, 15, 12, 35, 45)
    attempt_2 = task.attempt_task("runner 2", attempt_time_2)
    attempt_2.mark_completed(datetime.datetime(2018, 1, 15, 12, 35, 48))
    assert task.is_completed() is True


def test_started_time_new_task():
    task_created_time = datetime.datetime(2018, 1, 15, 12, 35, 0)
    task = Task("1234", "some cmd", task_created_time, max_attempts=2)
    assert task.started_time() is None


def test_started_time_open_attempt():
    task_created_time = datetime.datetime(2018, 1, 15, 12, 35, 0)
    task = Task("1234", "some cmd", task_created_time, max_attempts=2)
    attempt_time = datetime.datetime(2018, 1, 15, 12, 35, 30)
    task.attempt_task("runner 1", attempt_time)
    assert task.started_time() == attempt_time


def test_started_time_with_completed_attempts():
    task_created_time = datetime.datetime(2018, 1, 15, 12, 35, 0)
    task = Task("1234", "some cmd", task_created_time, max_attempts=2)
    attempt_time = datetime.datetime(2018, 1, 15, 12, 35, 30)
    attempt = task.attempt_task("runner 1", attempt_time)
    attempt_completed_time = datetime.datetime(2018, 1, 15, 12, 35, 45)
    attempt.mark_completed(attempt_completed_time)
    attempt_time_2 = datetime.datetime(2018, 1, 15, 12, 35, 45)
    attempt_2 = task.attempt_task("runner 2", attempt_time_2)
    attempt_2.mark_completed(datetime.datetime(2018, 1, 15, 12, 35, 48))
    assert task.started_time() == attempt_time


def test_completed_time_new_task():
    task_created_time = datetime.datetime(2018, 1, 15, 12, 35, 0)
    task = Task("1234", "some cmd", task_created_time, max_attempts=2)
    assert task.completed_time() is None


def test_completed_time_open_attempt():
    task_created_time = datetime.datetime(2018, 1, 15, 12, 35, 0)
    task = Task("1234", "some cmd", task_created_time, max_attempts=2)
    attempt_time = datetime.datetime(2018, 1, 15, 12, 35, 30)
    task.attempt_task("runner 1", attempt_time)
    assert task.completed_time() is None


def test_completed_time_one_failed_attempt():
    task = Task("1234", "some cmd", datetime.datetime(2018, 1, 15, 12, 35, 0), max_attempts=2)
    attempt_time = datetime.datetime(2018, 1, 15, 12, 35, 30)
    attempt = task.attempt_task("runner 1", attempt_time)
    attempt.mark_failed(datetime.datetime(2018, 1, 15, 12, 35, 45))
    assert task.completed_time() is None


def test_completed_time_all_failed_attempts():
    task = Task("1234", "some cmd", datetime.datetime(2018, 1, 15, 12, 35, 0), max_attempts=2)
    attempt_time = datetime.datetime(2018, 1, 15, 12, 35, 30)
    attempt = task.attempt_task("runner 1", attempt_time)
    attempt.mark_failed(datetime.datetime(2018, 1, 15, 12, 35, 45))
    attempt_time_2 = datetime.datetime(2018, 1, 15, 12, 35, 45)
    attempt_2 = task.attempt_task("runner 2", attempt_time_2)
    attempt_2.mark_failed(datetime.datetime(2018, 1, 15, 12, 35, 48))
    assert task.completed_time() is None


def test_completed_time_one_completed_attempt():
    task_created_time = datetime.datetime(2018, 1, 15, 12, 35, 0)
    task = Task("1234", "some cmd", task_created_time, max_attempts=2)
    attempt_time = datetime.datetime(2018, 1, 15, 12, 35, 30)
    attempt = task.attempt_task("runner 1", attempt_time)
    attempt_completed_time = datetime.datetime(2018, 1, 15, 12, 35, 45)
    attempt.mark_completed(attempt_completed_time)
    assert task.completed_time() == attempt_completed_time


def test_completed_time_multiple_completed_attempts():
    task_created_time = datetime.datetime(2018, 1, 15, 12, 35, 0)
    task = Task("1234", "some cmd", task_created_time, max_attempts=2)
    attempt_time = datetime.datetime(2018, 1, 15, 12, 35, 30)
    attempt = task.attempt_task("runner 1", attempt_time)
    attempt_completed_time = datetime.datetime(2018, 1, 15, 12, 35, 45)
    attempt.mark_completed(attempt_completed_time)
    attempt_time_2 = datetime.datetime(2018, 1, 15, 12, 35, 45)
    attempt_2 = task.attempt_task("runner 2", attempt_time_2)
    attempt_2.mark_completed(datetime.datetime(2018, 1, 15, 12, 35, 48))
    assert task.completed_time() == attempt_completed_time
