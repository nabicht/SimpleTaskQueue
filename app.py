from flask import Flask
from simple_task_queue import TaskManager

app = Flask(__name__)
task_manager = TaskManager(None)

@app.route('/')
def task_queue_overview():
    return 'Hello World!'


if __name__ == '__main__':
    app.run()
