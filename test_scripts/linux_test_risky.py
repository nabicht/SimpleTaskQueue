
from simple_task_client import add_task
from simple_task_client import main

SERVER = 'http://localhost:5000/'

task_1_id = add_task(SERVER, "ifconfig", name="run something", description="basic command, can run in non-risky")
task_2_id = add_task(SERVER, "xzfv -34 -2 -s", name="fake command", description="a fake command that should fail")
task_3_id = add_task(SERVER, "exit 1", name="non zero exit", description="first part should succeed but the whole thing fails")
task_4_id = add_task(SERVER, "python -m this", name="python ", description="a simple python test")
task_5_id = add_task(SERVER, "ls -l", name="list", description="using a simple shell command")
task_6_id = add_task(SERVER, "ls -l; exit 1", name="list", description="first is successful but overall should fail")


# now run the client
main(SERVER, risky=True)
