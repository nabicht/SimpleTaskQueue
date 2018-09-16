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
from simple_task_client import main

SERVER = 'http://localhost:5000/'

task_1_id = add_task(SERVER, "ifconfig", name="run something", description="basic command, can run in non-risky")
task_2_id = add_task(SERVER, "xzfv -34 -2 -s", name="fake command", description="a fake command that should fail")
task_3_id = add_task(SERVER, "exit 1", name="non zero exit", description="first part should succeed but the whole thing fails")
task_4_id = add_task(SERVER, "python -m this", name="python ", description="a simple python test")
task_5_id = add_task(SERVER, "ls -l", name="list", description="using a simple shell command")
task_6_id = add_task(SERVER, "ls -l; exit 1", name="list", description="first is successful but overall should fail")


# now run the client
main(SERVER, 20, "runner_1", risky=True)
