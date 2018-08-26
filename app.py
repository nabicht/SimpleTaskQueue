from flask import Flask
from simple_task_queue import TaskManager
from simple_task_queue import Task
from flask_restful import Resource, Api
from flask_restful import reqparse
from datetime import datetime
import uuid


class TaskIDCreator:

    def __init__(self):
        pass

    def id(self):
        return uuid.uuid1().hex

errors = {
    'UnknownDependencyException': {
        'message': "One or more specified dependent_on Task IDs are unknown by the server. Task not added!",
        'status': 400,
    },
}


app = Flask(__name__)
api = Api(app, catch_all_404s=True, errors=errors)

task_id_creator = TaskIDCreator()

task_manager = TaskManager(None)

task_post_parser = reqparse.RequestParser()
task_post_parser.add_argument('command', dest='command', required=True,
                              help="what gets executed in the command line")
task_post_parser.add_argument('name', dest='name', required=False,
                              help='a brief, human readable name for the task (optional)')
task_post_parser.add_argument('description', dest='description', required=False,
                              help='a more in-depth description of the task (optional)')
task_post_parser.add_argument('duration', dest='duration', required=False, type=float,
                              help='how long, in seconds, the task should run before a new attempt is made. Microseconds are defined to the right of the decimal (optional, with default of no applied duration).')
task_post_parser.add_argument('max_attempts', dest='max_attempts', location='form', required=False, type=int,
                              help="The max amount of times you want to try to attempt to run the task (optional, with default of 1)")
task_post_parser.add_argument('dependent_on', dest='dependent_on', location='form', required=False, action='append',
                              help="the ID of a task that this task is dependent upon (optional, can be multiple).")


# a wrapper that creates a Resource to interact with TaskManager and does some JSON/restful specific stuff
class TaskManagement(Resource):

    def post(self):
        print "we be posting!"
        args = task_post_parser.parse_args()
        task = Task(task_id_creator.id(),
                    args.command,
                    datetime.now,
                    name=args.name if args.name is not None else "",
                    desc=args.description if args.description is not None else "",
                    duration=args.duration,
                    max_attempts=args.max_attempts if args.max_attempts is not None else 1,
                    dependent_on=args.dependent_on)
        task_manager.add_task(task)
        return task.to_json(), 201


api.add_resource(TaskManagement, '/addtask')


@app.route('/')
def task_queue_overview():
    return 'Hello World!'


if __name__ == '__main__':
    app.run()

