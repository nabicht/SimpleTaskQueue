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

from flask import Flask
from flask import render_template
from simple_task_manager import TaskManager
from flask_restful import Resource, Api
from flask_restful import reqparse
from datetime import datetime
from flask_bootstrap import Bootstrap
import uuid
import util
import logging
import argparse


TIME_FORMAT = "%Y-%m-%d %H:%M:%S.%f"


class TaskIDCreator:

    def __init__(self):
        pass

    def id(self):
        return uuid.uuid1().hex


# a wrapper that creates a Resource to interact with TaskManager and does some JSON/restful specific stuff
class TaskManagement(Resource):

    def __init__(self, **kwargs):
        self._logger = kwargs.get("logger")
        self._task_manager = kwargs.get("task_manager")
        self._task_post_parser = reqparse.RequestParser()
        self._task_post_parser.add_argument('command', dest='command', required=True,
                                            help="what gets executed in the command line")
        self._task_post_parser.add_argument('name', dest='name', required=False,
                                            help='a brief, human readable name for the task (optional)')
        self._task_post_parser.add_argument('description', dest='description', required=False,
                                            help='a more in-depth description of the task (optional)')
        self._task_post_parser.add_argument('duration', dest='duration', required=False, type=float,
                                            help='how long, in seconds, the task should run before a new attempt is made. Microseconds are defined to the right of the decimal (optional, with default of no applied duration).')
        self._task_post_parser.add_argument('max_attempts', dest='max_attempts', required=False, type=int,
                                            help="The max amount of times you want to try to attempt to run the task (optional, with default of 1)")
        self._task_post_parser.add_argument('dependent_on', dest='dependent_on', required=False, action='append',
                                            help="the ID of a task that this task is dependent upon (optional, can be multiple).")

        self._task_delete_parser = reqparse.RequestParser()
        self._task_delete_parser.add_argument("task_id", dest="task_id", required=True, type=int, help="ID of Task to be deleted.")

    def post(self):
        args = self._task_post_parser.parse_args()
        self._logger.info("TaskManagement.post: %s" % str(args))
        task = self._task_manager.add_task(args.command,
                                           datetime.now(),
                                           name=args.name if args.name is not None else "",
                                           desc=args.description if args.description is not None else "",
                                           duration=args.duration,
                                           max_attempts=args.max_attempts if args.max_attempts is not None else 1,
                                           dependent_on=args.dependent_on)
        return task.to_json(), 201

    def delete(self):
        args = self._task_delete_parser.parse_args()
        self._logger.info("TaskManagement.delete: %s" % str(args))
        deleted = self._task_manager.delete_task(args.task_id)
        if deleted:
            return {"status": "task deleted", "task_id": args.task_id}, 200
        else:
            return {"message": "task for %s not found, cannot delete" % args.task_id}, 400


class AttemptManagement(Resource):

    NO_TASK = {'status': "no attempt"}

    def __init__(self, **kwargs):
        self._logger = kwargs.get("logger")
        self._task_manager = kwargs.get("task_manager")
        self._get_next_attempt = reqparse.RequestParser()
        self._get_next_attempt.add_argument('runner_id', dest='runner_id', required=True,
                                            help='The unique identifier of the runner.')
        self._attempt_update = reqparse.RequestParser()
        self._attempt_update.add_argument('runner_id', dest='runner_id', required=True,
                                          help='The unique identifier of the runner.')
        self._attempt_update.add_argument('task_id', dest='task_id', required=True, type=int,
                                          help='The unique identifier of the task being attempted.')
        self._attempt_update.add_argument('attempt_id', dest='attempt_id', required=True,
                                          help='The unique identifier of the attempt.')
        self._attempt_update.add_argument('status', dest='status', required=True,
                                          help='Status of attempt: "failed" or "completed".')
        self._attempt_update.add_argument('message', dest='message', required=False,
                                          help='Status of attempt: "failed" or "completed".')

    @staticmethod
    def _task_attempt_json(task, attempt):
        return {'status': "attempt",
                'task_id': task.task_id(),
                'command': task.cmd,
                'attempt_id': attempt.id()}

    def get(self):
        args = self._get_next_attempt.parse_args()
        self._logger.info("AttemptManagement.get: %s" % str(args))
        current_time = datetime.now()
        task, attempt = self._task_manager.start_next_attempt(args.runner_id, current_time)
        if task is not None:
            json_dict = self._task_attempt_json(task, attempt)
            self._logger.info("AttemptManagement.get next attempt: %s" % str(json_dict))
            return json_dict, 200
        else:
            json_dict = self.NO_TASK
            self._logger.info("AttemptManagement.get no next attempt: %s" % str(json_dict))
            return json_dict, 200

    def put(self):
        args = self._attempt_update.parse_args()
        self._logger.info("AttemptManagement.put: %s" % str(args))
        status = args.status.lower()
        if status == "failed":
            self._task_manager.fail_attempt(args.task_id, args.attempt_id, "client reported", datetime.now())
        elif status == "completed":
            self._task_manager.complete_attempt(args.task_id, args.attempt_id, datetime.now())
        else:
            self._task_manager.fail_attempt(args.task_id, args.attempt_id, "unknown status reported")
            # TODO log this
            return {"message": "%s is an unknown status. Should be 'completed' or 'failed'. Falling back to failed." % args.status}, 400


class MonitorTasks(Resource):

    def __init__(self, **kwargs):
        self._logger = kwargs.get("logger")
        self._task_manager = kwargs.get("task_manager")

    @staticmethod
    def _dependent_on_str(dependent_ons):
        return ", ".join([str(dependent_on) for dependent_on in dependent_ons])

    @staticmethod
    def _dependencies_str(dependencies):
        return ", ".join([str(dependency) for dependency in dependencies])

    def get(self, list_type):
        self._logger.info("MonitorTasks.get: %s" % list_type)
        list_of_tasks = []
        if list_type.lower() == "todo":
            to_do = self._task_manager.todo_tasks()
            for task in to_do:
                d = {"task_id": task.task_id(),
                     "status": "To Do",
                     "created": task.created_time.strftime(TIME_FORMAT),
                     "name": task.name,
                     "description": task.desc,
                     "command": task.cmd,
                     "dependent_on": self._dependent_on_str(task.dependent_on),
                     "duration": task.duration,
                     "max_attempts": task.max_attempts,
                     }
                list_of_tasks.append(d)
        elif list_type.lower() == "inprocess":
            in_process = self._task_manager.in_process_tasks()
            for task in in_process:
                current_runner = ""
                attempt_count = self._task_manager.get_task_attempt_count(task.task_id())
                most_recent_attempt = self._task_manager.get_most_recent_attempt(task.task_id())
                if most_recent_attempt.is_in_process():
                    current_runner = most_recent_attempt.runner
                d = {"task_id": task.task_id(),
                     "status": "In Process",
                     "created": task.created_time.strftime(TIME_FORMAT),
                     "started": self._task_manager.get_start_time(task.task_id()).strftime(TIME_FORMAT),
                     "name": task.name,
                     "description": task.desc,
                     "command": task.cmd,
                     "dependent_on": self._dependent_on_str(task.dependent_on),
                     "duration": task.duration,
                     "attempted": attempt_count,
                     "attempts_left": task.max_attempts - attempt_count,
                     "attempt_open": most_recent_attempt.is_in_process() is True,
                     "current_runner": current_runner
                     }
                list_of_tasks.append(d)
        elif list_type.lower() == "failed":
            done = self._task_manager.done_tasks()
            for task in done:
                if task.has_failed():
                    d = {"task_id": task.task_id(),
                         "status": "Failed",
                         "created": task.created_time.strftime(TIME_FORMAT),
                         "name": task.name,
                         "description": task.desc,
                         "command": task.cmd,
                         "dependencies": self._dependent_on_str(self._task_manager.dependencies(task.task_id())),
                         "attempts": self._task_manager.get_task_attempt_count(task.task_id())
                         }
                    list_of_tasks.append(d)
        elif list_type.lower() == "completed":
            done = self._task_manager.done_tasks()
            for task in done:
                if task.has_completed():
                    d = {"task_id": task.task_id(),
                         "status": "Completed",
                         "created": task.created_time.strftime(TIME_FORMAT),
                         "finished": self._task_manager.get_done_time(task.task_id()).strftime(TIME_FORMAT),
                         "name": task.name,
                         "description": task.desc,
                         "command": task.cmd,
                         "dependencies": self._dependent_on_str(self._task_manager.dependencies(task.task_id())),
                         "attempts": self._task_manager.get_task_attempt_count(task.task_id())
                         }
                    list_of_tasks.append(d)
        else:
            return {"message": "%s is an unknown list type. No tasks to return." % list_type}, 400
        return {"data": list_of_tasks}, 200


def create_app(db_file):
    errors = {
        'UnknownDependencyException': {
            'message': "One or more specified dependent_on Task IDs are unknown by the server. Task not added!",
            'status': 400,
        },
    }

    app = Flask(__name__)
    Bootstrap(app)
    api = Api(app, catch_all_404s=True, errors=errors)

    log_file_name = util.time_stamped_file_name("stq")
    logger = util.basic_logger(log_file_name=log_file_name, file_level=logging.DEBUG, console_level=logging.DEBUG)

    task_manager = TaskManager(db_file, logger)

    api.add_resource(TaskManagement, '/task', resource_class_kwargs={'logger': logger, 'task_manager': task_manager})
    api.add_resource(AttemptManagement, '/attempt', resource_class_kwargs={'logger': logger, 'task_manager': task_manager})
    api.add_resource(MonitorTasks, '/listtasks/<list_type>', resource_class_kwargs={'logger': logger, "task_manager": task_manager})

    @app.route('/')
    def task_queue_overview():
        return render_template('overview.html')

    return app


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-host", action="store", dest="host", nargs=1,
                        default=['127.0.0.1'], required=False,
                        help="The host. Defaults to 127.0.0.1")
    parser.add_argument("-port", action="store", dest="port", type=int, nargs=1,
                        default=[5000], required=False,
                        help="The port. Defaults to 5000")
    parser.add_argument("-dbfile", action="store", dest="dbfile", nargs=1, default=["stq_persistence.db"],
                        required=False, help="The file that all persistence is in. Defaults to 'stq_persistence.db'")
    cmd_args = parser.parse_args()
    app = create_app(cmd_args.dbfile[0])
    app.run(host=cmd_args.host[0], port=cmd_args.port[0], threaded=False)


