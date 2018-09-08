
from simple_task_client import add_task
from simple_task_client import get_next_attempt
from simple_task_client import report_completed_attempt

SERVER = 'http://localhost:5000/'

task_1_id = add_task(SERVER, "cp some_files to_here/.", name="copy files", description="copying some stuff")
task_2_id = add_task(SERVER, "cp some_files to_here/.", name="copy files again", description="more copying some stuff")
task_3_id = add_task(SERVER, "python run_some_stuff.py", name="running python", description="running some python stuff because python runs on the command line", max_attempts=2)
task_4_id = add_task(SERVER, "python run_different_stuff.py", name="running python", max_attempts=3, dependent_on=[task_1_id, task_3_id])
task_5_id = add_task(SERVER, "grep blah big_ol.txt >> /mnt/remote_storage/smaller.txt", name="result filtering", duration=100.0)

# first attempt we get is task_1
attempt_1_dict = get_next_attempt(SERVER, "runner_a")
assert attempt_1_dict["status"] == "task"
assert attempt_1_dict["command"] == "cp some_files to_here/."
assert attempt_1_dict["task_id"] == task_1_id
assert "attempt_id" in attempt_1_dict

# report back that attempt 1 is complete
report_completed_attempt(SERVER, "runner_a", attempt_1_dict["task_id"], attempt_1_dict['attempt_id'])

# second attempt we get is task_2
attempt_2_dict = get_next_attempt(SERVER, "runner_b")
assert attempt_2_dict["status"] == "task"
assert attempt_2_dict["command"] == "cp some_files to_here/."
assert attempt_2_dict["task_id"] == task_2_id
assert "attempt_id" in attempt_2_dict

# report back that attempt 2 is complete
report_completed_attempt(SERVER, "runner_b", attempt_2_dict["task_id"], attempt_2_dict['attempt_id'])

# third attempt we get is task_3
attempt_3_dict = get_next_attempt(SERVER, "runner_c")
assert attempt_3_dict["status"] == "task"
assert attempt_3_dict["command"] == "python run_some_stuff.py"
assert attempt_3_dict["task_id"] == task_3_id
assert "attempt_id" in attempt_3_dict

# fourth attempt we get is task_5
# should skip task_4 because it is dependent on 1 and 3 and those are not both complete
attempt_4_dict = get_next_attempt(SERVER, "runner_d")
assert attempt_4_dict["status"] == "task"
assert attempt_4_dict["command"] == "grep blah big_ol.txt >> /mnt/remote_storage/smaller.txt"
assert attempt_4_dict["task_id"] == task_5_id
assert "attempt_id" in attempt_4_dict

# report fourth attempt is done
report_completed_attempt(SERVER, "runner_d", attempt_4_dict["task_id"], attempt_4_dict['attempt_id'])

# fifth attempt should have no attempts because 4 is still waiting on 1 and 3, and there are no other tasks
attempt_5_dict = get_next_attempt(SERVER, "runner_e")
assert attempt_5_dict["status"] == "no task"

# now complete attempt 3
# report back that attempt 3 is complete
report_completed_attempt(SERVER, "runner_a", attempt_3_dict["task_id"], attempt_3_dict['attempt_id'])

# now try getting another task and we should get task 4 since the dependencies are complete
attempt_6_dict = get_next_attempt(SERVER, "runner_e")
assert attempt_6_dict["status"] == "task"
assert attempt_6_dict["command"] == "python run_different_stuff.py"
assert attempt_6_dict["task_id"] == task_4_id
assert "attempt_id" in attempt_6_dict

# report attempt 6 is done
report_completed_attempt(SERVER, "runner_e", attempt_6_dict["task_id"], attempt_6_dict['attempt_id'])
