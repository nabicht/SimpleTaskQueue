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

from simple_task_objects import TaskAttempt
import datetime
import logging

LOGGER = logging.getLogger(__name__)


def test_state_tracking_with_failed():
    attempt = TaskAttempt("runner 1",  datetime.datetime(2018, 1, 15, 12, 35, 0))
    # assert it is open
    assert attempt.is_failed() is False
    assert attempt.is_completed() is False
    assert attempt.is_in_process() is True  # the creation of an attempt means it is in process
    attempt.mark_failed("some reason")
    # assert that states have changed
    assert attempt.is_failed() is True
    assert attempt.is_completed() is False
    assert attempt.is_in_process() is False


def test_state_tracking_with_completed():
    attempt = TaskAttempt("runner 1", datetime.datetime(2018, 1, 15, 12, 35, 0))
    # assert it is open
    assert attempt.is_failed() is False
    assert attempt.is_completed() is False
    assert attempt.is_in_process() is True  # the creation of an attempt means it is in process
    attempt.mark_completed(datetime.datetime(2018, 1, 15, 12, 35, 22))
    # assert that states have changed
    assert attempt.is_failed() is False
    assert attempt.is_completed() is True
    assert attempt.is_in_process() is False
