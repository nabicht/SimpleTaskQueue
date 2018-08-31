import requests
import json

SERVER = 'http://localhost:5000/'


def add_task(command, name=None, description=None, dependent_on=None, max_attempts=None, duration=None):
    payload = {"command": command}
    if name is not None:
        payload["name"] = str(name)
    if description is not None:
        payload["description"]=str(description)
    if dependent_on is not None:
        l = []
        for d in dependent_on:
            l.append(str(d))
        payload["dependent_on"] = l
    if max_attempts is not None:
        payload['max_attempts'] = max_attempts
    if duration is not None:
        payload['duration'] = duration
    r = requests.post(SERVER + "addtask", data=payload)
    response_dict = json.loads(r.text)
    return str(response_dict['task_id'])


task_1_id = add_task("cp some_files to_here/.", name="copy files", description="copying some stuff")
task_2_id = add_task("cp some_files to_here/.", name="copy files again", description="more copying some stuff")
task_3_id = add_task("python run_some_stuff.py", name="running python", description="running some python stuff because python runs on the command line", max_attempts=2)
task_4_id = add_task("python run_different_stuff.py", name="running python", max_attempts=3, dependent_on=[task_1_id, task_3_id])
task_5_id = add_task("grep blah big_ol.txt >> /mnt/remote_storage/smaller.txt", name="result filtering", duration=100.0)

