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

from simple_task_client import add_task
from simple_task_client import get_todo_tasks
from simple_task_client import get_inprocess_tasks
from simple_task_client import get_completed_tasks
from simple_task_client import get_failed_tasks
from simple_task_client import delete_task
from simple_task_client import report_completed_attempt
from simple_task_client import report_failed_attempt
from simple_task_client import get_next_attempt

SERVER = 'http://localhost:5000/'

task_1_id = add_task(SERVER, "cp some_files to_here/.", name="copy files", description="copying some stuff")
task_2_id = add_task(SERVER, "cp some_files to_here/.", name="copy files again", description="more copying some stuff")
task_3_id = add_task(SERVER, "python run_some_stuff.py", name="running python", description="running some python stuff because python runs on the command line", max_attempts=2)
task_4_id = add_task(SERVER, "python run_different_stuff.py", name="running python")
task_5_id = add_task(SERVER, "grep blah big_ol.txt >> /mnt/remote_storage/smaller.txt", name="result filtering", duration=100.0)

todo_tasks = get_todo_tasks(SERVER)
assert len(todo_tasks) == 5
inprocess_tasks = get_inprocess_tasks(SERVER)
assert len(inprocess_tasks) == 0
completed_tasks = get_completed_tasks(SERVER)
assert len(completed_tasks) == 0
failed_tasks = get_failed_tasks(SERVER)
assert len(failed_tasks) == 0

assert task_1_id in todo_tasks
assert task_2_id in todo_tasks
assert task_3_id in todo_tasks
assert task_4_id in todo_tasks
assert task_5_id in todo_tasks

# delete a task
delete_response = delete_task(SERVER, task_1_id)

# delete worked properly
assert "status" in delete_response
assert delete_response.get("status") == "task deleted"
assert "task_id" in delete_response
assert delete_response.get("task_id") == task_1_id
# todo_tasks should change, the rest shouldn't
todo_tasks = get_todo_tasks(SERVER)
assert len(todo_tasks) == 4
inprocess_tasks = get_inprocess_tasks(SERVER)
assert len(inprocess_tasks) == 0
completed_tasks = get_completed_tasks(SERVER)
assert len(completed_tasks) == 0
failed_tasks = get_failed_tasks(SERVER)
assert len(failed_tasks) == 0

# move a task to in process
task_attempt = get_next_attempt(SERVER, 'runner_1')
task_attempt_id = task_attempt['task_id']
assert task_attempt_id == task_2_id
todo_tasks = get_todo_tasks(SERVER)
assert len(todo_tasks) == 3
inprocess_tasks = get_inprocess_tasks(SERVER)
assert len(inprocess_tasks) == 1
assert task_attempt_id in inprocess_tasks
completed_tasks = get_completed_tasks(SERVER)
assert len(completed_tasks) == 0
failed_tasks = get_failed_tasks(SERVER)
assert len(failed_tasks) == 0

# delete a non-existant task
delete_response = delete_task(SERVER, "this id is not a real id")
assert "message" in delete_response

# nothing changes in task lists since delete not successful
assert len(todo_tasks) == 3
assert task_3_id in todo_tasks
assert task_4_id in todo_tasks
assert task_5_id in todo_tasks
inprocess_tasks = get_inprocess_tasks(SERVER)
assert len(inprocess_tasks) == 1
assert task_attempt_id in inprocess_tasks
completed_tasks = get_completed_tasks(SERVER)
assert len(completed_tasks) == 0
failed_tasks = get_failed_tasks(SERVER)
assert len(failed_tasks) == 0

# delete task in inprocess
delete_response = delete_task(SERVER, task_attempt_id)
assert delete_response.get("status") == "task deleted"
assert "task_id" in delete_response
assert delete_response.get("task_id") == task_attempt_id == task_2_id

todo_tasks = get_todo_tasks(SERVER)
assert len(todo_tasks) == 3
assert task_3_id in todo_tasks
assert task_4_id in todo_tasks
assert task_5_id in todo_tasks
inprocess_tasks = get_inprocess_tasks(SERVER)
assert len(inprocess_tasks) == 0
completed_tasks = get_completed_tasks(SERVER)
assert len(completed_tasks) == 0
failed_tasks = get_failed_tasks(SERVER)
assert len(failed_tasks) == 0

# complete a task
task_attempt = get_next_attempt(SERVER, 'runner_2')
task_attempt_id = task_attempt['task_id']
assert task_attempt_id == task_3_id
report_completed_attempt(SERVER, "runner_2", task_attempt_id, task_attempt.get("attempt_id"))

# fail a task
task_attempt = get_next_attempt(SERVER, 'runner_3')
task_attempt_id = task_attempt['task_id']
assert task_attempt_id == task_4_id
report_failed_attempt(SERVER, "runner_3", task_attempt_id, task_attempt.get("attempt_id"))

todo_tasks = get_todo_tasks(SERVER)
assert len(todo_tasks) == 1
assert task_5_id in todo_tasks
inprocess_tasks = get_inprocess_tasks(SERVER)
assert len(inprocess_tasks) == 0
completed_tasks = get_completed_tasks(SERVER)
assert len(completed_tasks) == 1
assert task_3_id in completed_tasks
failed_tasks = get_failed_tasks(SERVER)
assert len(failed_tasks) == 1
assert task_4_id in failed_tasks

# delete from failed
delete_response = delete_task(SERVER, task_4_id)
assert delete_response.get("status") == "task deleted"
assert "task_id" in delete_response
assert delete_response.get("task_id") == task_4_id

todo_tasks = get_todo_tasks(SERVER)
assert len(todo_tasks) == 1
assert task_5_id in todo_tasks
inprocess_tasks = get_inprocess_tasks(SERVER)
assert len(inprocess_tasks) == 0
completed_tasks = get_completed_tasks(SERVER)
assert len(completed_tasks) == 1
assert task_3_id in completed_tasks
failed_tasks = get_failed_tasks(SERVER)
assert len(failed_tasks) == 0

# delete from completed
delete_response = delete_task(SERVER, task_3_id)
assert delete_response.get("status") == "task deleted"
assert "task_id" in delete_response
assert delete_response.get("task_id") == task_3_id

todo_tasks = get_todo_tasks(SERVER)
assert len(todo_tasks) == 1
assert task_5_id in todo_tasks
inprocess_tasks = get_inprocess_tasks(SERVER)
assert len(inprocess_tasks) == 0
completed_tasks = get_completed_tasks(SERVER)
assert len(completed_tasks) == 0
failed_tasks = get_failed_tasks(SERVER)
assert len(failed_tasks) == 0

# delete task 4 a second time and it should be a failed delete returned
delete_response = delete_task(SERVER, task_4_id)
assert "message" in delete_response
todo_tasks = get_todo_tasks(SERVER)
assert len(todo_tasks) == 1
assert task_5_id in todo_tasks
inprocess_tasks = get_inprocess_tasks(SERVER)
assert len(inprocess_tasks) == 0
completed_tasks = get_completed_tasks(SERVER)
assert len(completed_tasks) == 0
failed_tasks = get_failed_tasks(SERVER)
assert len(failed_tasks) == 0
