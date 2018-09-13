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

SERVER = 'http://localhost:5000/'

task_1_id = add_task(SERVER, "cp some_files to_here/.", name="copy files", description="copying some stuff")
task_2_id = add_task(SERVER, "cp some_files to_here/.", name="copy files again", description="more copying some stuff")
task_3_id = add_task(SERVER, "python run_some_stuff.py", name="running python", description="running some python stuff because python runs on the command line", max_attempts=2)
task_4_id = add_task(SERVER, "python run_different_stuff.py", name="running python", max_attempts=3, dependent_on=[task_1_id, task_3_id])
task_5_id = add_task(SERVER, "grep blah big_ol.txt >> /mnt/remote_storage/smaller.txt", name="result filtering", duration=100.0)
