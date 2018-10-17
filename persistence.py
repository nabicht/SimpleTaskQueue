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

from simple_task_objects import Task
from simple_task_objects import TaskAttempt
import sqlite3


class SQLitePersistence:

    TODO_QUEUE = 1
    IN_PROCESS_QUEUE = 2
    DONE_QUEUE = 3
    QUEUE_STR = {TODO_QUEUE: "ToDo", IN_PROCESS_QUEUE: "InProcess", DONE_QUEUE: "Done"}

    TASK_TABLE = """
                 CREATE TABLE tasks (
                    task_id integer PRIMARY KEY,
                    cmd text NOT NULL,
                    description text,
                    name text,
                    max_attempts integer NOT NULL,
                    duration real,
                    created_time timestamp NOT NULL,
                    queue integer);
                 """

    ATTEMPT_TABLE = """
                    CREATE TABLE attempts (
                       attempt_id integer PRIMARY KEY,
                       task_id integer,
                       runner text NOT NULL,
                       start_time timestamp NOT NULL,
                       fail_reason text,
                       done_time timestamp,
                       status int);
                    """

    DEPENDENT_ON_TABLE = """
                         CREATE TABLE dependencies (
                            dependency_id integer PRIMARY KEY,
                            task_id int,
                            dependent_on_task_id int);
                         """

    TABLE_EXISTS = "SELECT name FROM sqlite_master WHERE type='table' AND name=?"

    NEXT_TASK = "SELECT min(task_id) FROM tasks WHERE queue=? AND task_id NOT IN (%s);"

    GET_TASK_IDS_FROM_QUEUE = "SELECT task_id FROM tasks WHERE queue=? ORDER BY task_id;"
    GET_TASK = "SELECT * FROM tasks WHERE task_id=?;"
    GET_TASK_FROM_QUEUE = "SELECT * FROM tasks WHERE task_id=? and queue=?;"
    GET_TASKS_FROM_QUEUE = "SELECT * FROM tasks WHERE queue=? ORDER BY task_id;"
    GET_TASKS_FROM_QUEUE_CREATED_TIME_SORTED = "SELECT * FROM tasks WHERE queue=? ORDER BY created_time;"
    GET_TASKS_NO_DURATION_FROM_QUEUE = "SELECT * FROM tasks WHERE queue=? and duration is NULL ORDER BY task_id;"
    GET_TASKS_DURATION_FROM_QUEUE = "SELECT * FROM tasks WHERE queue=? and duration is NOT NULL ORDER BY task_id;"
    GET_TASK_COUNT_FROM_QUEUE = "SELECT COUNT(task_id) as the_count FROM tasks where queue=?;"

    IS_TASK_DONE = "SELECT rowid FROM tasks WHERE task_id=? and queue=%s" % DONE_QUEUE

    # This will probably break if there are no current attempts, so best to do an attempt count first
    GET_TASK_START_TIME = "SELECT start_time FROM attempts WHERE attempt_id = (SELECT MIN(attempt_id) FROM attempts WHERE task_id=?);"
    
    GET_TASK_DONE_TIME = "SELECT MIN(done_time) AS done_time FROM attempts WHERE task_id=?;"

    GET_DEPENDENT_ON = "SELECT dependent_on_task_id FROM dependencies WHERE task_id=? ORDER BY dependent_on_task_id;"
    GET_DEPENDENTS = "SELECT task_id FROM dependencies WHERE dependent_on_task_id=? ORDER BY task_id;"

    GET_ATTEMPT = "SELECT * FROM attempts WHERE attempt_id=?;"
    GET_ATTEMPTS_FOR_TASK = "SELECT * FROM attempts WHERE task_id=? ORDER BY attempt_id;"
    GET_MOST_RECENT_ATTEMPT_ID = "SELECT MAX(attempt_id) AS most_recent_attempt FROM ATTEMPTS WHERE task_id=?;"
    GET_ATTEMPTS_COUNT = "SELECT COUNT(*) AS num_attempts FROM attempts WHERE task_id=?"
    GET_ATTEMPTS_COUNT_FOR_STATUS = "SELECT COUNT(*) AS num_attempts FROM attempts WHERE task_id=? and status=?;"

    INSERT_TASK = """
                  INSERT INTO tasks(cmd, description, name, max_attempts, duration, created_time, queue) 
                  VALUES(?, ?, ?, ?, ?, ?, ?);
                  """
    INSERT_DEPENDENT_ON = "INSERT INTO dependencies(task_id, dependent_on_task_id) VALUES(?,?);"
    
    INSERT_ATTEMPT = "INSERT INTO attempts(task_id, runner, start_time, status) VALUES(?,?,?,?);"

    UPDATE_TASK_TO_DONE = "UPDATE tasks SET queue = %d WHERE task_id=?;" % DONE_QUEUE
    UPDATE_TASK_TO_INPROCESS = "UPDATE tasks SET queue = %d WHERE task_id=?;" % IN_PROCESS_QUEUE
    UPDATE_ATTEMPT_TO_FAILED = "UPDATE attempts SET done_time=?, fail_reason=?, status=%s WHERE attempt_id=?" % str(TaskAttempt.FAILED_STATUS)
    UPDATE_ATTEMPT_TO_COMPLETE = "UPDATE attempts SET done_time=?, status=%s WHERE attempt_id=?" % str(TaskAttempt.COMPLETED_STATUS)

    def __init__(self, db_file, logger):
        self._db_file = db_file
        self._logger = logger

        self._writer = sqlite3.connect(db_file, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES,
                                       isolation_level=None)
        self._writer.execute('pragma journal_mode=wal;')

        self._setup_tables(self._writer)

        self._reader = sqlite3.connect(db_file, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES,
                                       isolation_level=None)
        self._reader.execute('pragma journal_mode=wal;')
        self._reader.row_factory = sqlite3.Row

    def close(self):
        """
        Close the database connection.
        :return:
        """
        # TODO should probably have some error catching and appropriate logging
        self._writer.close()
        self._logger.info("The writer connection to the SQLite3 database is closed.")
        self._reader.close()
        self._logger.info("The reader connection to the SQLite3 database is closed.")

    @staticmethod
    def _table_exists(db_conn, table_name):
        exists = False
        if db_conn.execute(SQLitePersistence.TABLE_EXISTS, (table_name, )).fetchone():
            exists = True
        return exists

    def _setup_tables(self, conn):
        self._logger.debug("Setting up table `tasks`")

        if self._table_exists(conn, "tasks"):
            self._logger.debug("Table `tasks` already exists. Not setting up.")
        else:
            self._logger.info("Creating Table `tasks`")
            conn.execute(self.TASK_TABLE)

        if self._table_exists(conn, "dependencies"):
            self._logger.debug("Table `dependencies` already exists. Not setting up.")
        else:
            self._logger.info("Creating Table `dependencies`")
            conn.execute(self.DEPENDENT_ON_TABLE)

        if self._table_exists(conn, "attempts"):
            self._logger.debug("Table `attempts` already exists. Not setting up.")
        else:
            self._logger.info("Creating Table `attempts`")
            conn.execute(self.ATTEMPT_TABLE)

    def get_dependent_on(self, task_id):
        """
        Returns a list of task ids where each task id is for a task that the past in task_id's task is dependent on.
        :param task_id: int
        :return: list of ints where each int is a task id
        """
        cursor = self._reader.cursor()
        cursor.execute(self.GET_DEPENDENT_ON, (task_id,))
        rows = cursor.fetchall()
        cursor.close()
        dependent_on = []
        for row in rows:
            dependent_on.append(row['dependent_on_task_id'])
        return dependent_on

    def get_task_ids(self, queue):
        """
        Returns a list of task ids in increasing order, which means in order that they were added to the queue.
        :param queue: which queue to query
        :return: list of ints (where an int is a task id)
        """
        cursor = self._reader.cursor()
        cursor.execute(self.GET_TASK_IDS_FROM_QUEUE, (queue,))
        rows = cursor.fetchall()
        cursor.close()
        task_ids = []
        for row in rows:
            task_ids.append(row['task_id'])
        return task_ids

    def _row_to_task(self, row):
        task_id = row['task_id']
        dependent_on = self.get_dependent_on(task_id)
        queue = row['queue']
        status = None
        if queue == SQLitePersistence.TODO_QUEUE:
            status = Task.STATE_TODO
        elif queue == SQLitePersistence.IN_PROCESS_QUEUE:
            status = Task.STATE_INPROCESS
        elif queue == SQLitePersistence.DONE_QUEUE:
            # if done and completed then completed if done and not completed, then failed
            if self.is_task_completed(task_id):
                status = Task.STATE_COMPLETED
            else:
                status = Task.STATE_FAILED

        if status is None:
            raise Exception("Could not determine status of Task %s" % str(task_id))

        name = "" if row["name"] is None else row["name"]
        desc = "" if row["description"] is None else row["description"]
        task = Task(task_id, row['cmd'], row['created_time'], status, name=name, desc=desc, duration=row["duration"],
                    max_attempts=row['max_attempts'], dependent_on=dependent_on)
        return task

    def get_tasks_with_duration_filter(self, queue, with_duration):
        cursor = self._reader.cursor()
        if with_duration:
            cursor.execute(self.GET_TASKS_DURATION_FROM_QUEUE, (queue,))
        else:
            cursor.execute(self.GET_TASKS_NO_DURATION_FROM_QUEUE, (queue,))
        rows = cursor.fetchall()
        cursor.close()
        tasks = []
        for row in rows:
            task = self._row_to_task(row)
            tasks.append(task)
        return tasks

    def get_tasks(self, queue, sort_by_created_time=False):
        cursor = self._reader.cursor()
        if sort_by_created_time:
            cursor.execute(self.GET_TASKS_FROM_QUEUE_CREATED_TIME_SORTED, (queue,))
        else:
            cursor.execute(self.GET_TASKS_FROM_QUEUE, (queue,))
        rows = cursor.fetchall()
        cursor.close()
        tasks = []
        for row in rows:
            task = self._row_to_task(row)
            tasks.append(task)
        return tasks

    def get_task(self, task_id, queue=None):
        cursor = self._reader.cursor()
        if queue is None:
            cursor.execute(self.GET_TASK, (task_id,))
        else:
            cursor.execute(self.GET_TASK_FROM_QUEUE, (task_id, queue))
        row = cursor.fetchone()
        cursor.close()
        task = None
        if row is not None:
            task = self._row_to_task(row)
        return task

    def next_task(self, queue, skip_task_ids=None):
        sql_args = [queue]
        if skip_task_ids is None:
            skip_task_ids = []
        sql_args.extend(skip_task_ids)
        cursor = self._reader.cursor()
        cursor.execute(self.NEXT_TASK % ','.join('?'*len(skip_task_ids)), sql_args)
        row = cursor.fetchone()
        cursor.close()
        task = None
        if row is not None:
            task_id = row['task_id']
            self.get_task(task_id, queue)
            task = self.get_task(task_id)
        return task

    def add_task(self, queue, command, created_time, name, desc, duration, max_attempts, dependent_on):
        self._writer.execute('BEGIN EXCLUSIVE')
        cursor = self._writer.cursor()
        try:
            cursor.execute(self.INSERT_TASK, (command, desc, name, max_attempts, duration, created_time, queue))
            task_id = cursor.lastrowid
            if dependent_on is not None:
                for dependent_on_task_id in dependent_on:
                    cursor.execute(self.INSERT_DEPENDENT_ON, (task_id, dependent_on_task_id))
            cursor.close()
        except:
            self._writer.rollback()  # Roll back all changes if an exception occurs.
            raise
        else:
            self._writer.commit()
        cursor.close()
        return task_id

    def get_task_count(self, queue):
        cursor = self._reader.cursor()
        cursor.execute(self.GET_TASK_COUNT_FROM_QUEUE, (queue,))
        row = cursor.fetchone()
        cursor.close()
        return row['the_count']

    def get_task_start_time(self, task_id):
        cursor = self._reader.cursor()
        cursor.execute(self.GET_ATTEMPTS_COUNT, (task_id,))
        count_row = cursor.fetchone()
        row = None
        if count_row is not None:
            cursor.execute(self.GET_TASK_START_TIME, (task_id,))
            row = cursor.fetchone()
        cursor.close()
        start_time = None
        if row is not None:
            start_time = row["start_time"]
        return start_time

    def get_task_close_time(self, task_id):
        cursor = self._reader.cursor()
        cursor.execute(self.GET_TASK_START_TIME, (task_id,))
        row = cursor.fetchone()
        cursor.close()
        start_time = None
        if row is not None:
            start_time = row["start_time"]
        return start_time

    @staticmethod
    def _row_to_attempt(row):
        attempt = TaskAttempt(row["attempt_id"], row["task_id"], row["start_time"], row["runner"], row["done_time"],
                              row["status"], row["fail_reason"])
        return attempt

    def get_attempt(self, attempt_id):
        attempt = None
        cursor = self._reader.cursor()
        cursor.execute(self.GET_ATTEMPT, (attempt_id,))
        attempt_row = cursor.fetchone()
        cursor.close()
        if attempt_row is not None:
            attempt = self._row_to_attempt(attempt_row)
        return attempt

    def get_attempts(self, task_id):
        attempts = []
        cursor = self._reader.cursor()
        cursor.execute(self.GET_ATTEMPTS_FOR_TASK, (task_id,))
        attempt_rows = cursor.fetchall()
        cursor.close()
        for attempt_row in attempt_rows:
            attempts.append(self._row_to_attempt(attempt_row))
        return attempts

    def get_most_recent_attempt(self, task_id):
        attempt = None
        cursor = self._reader.cursor()
        cursor.execute(self.GET_MOST_RECENT_ATTEMPT_ID, (task_id,))
        attempt_id_row = cursor.fetchone()
        if attempt_id_row is not None:
            attempt_id = attempt_id_row["most_recent_attempt"]
            cursor.execute(self.GET_ATTEMPT, (attempt_id, ))
            attempt_row = cursor.fetchone()
            if attempt_row is not None:
                attempt = self._row_to_attempt(attempt_row)
        cursor.close()
        return attempt

    def get_attempt_count(self, task_id):
        cursor = self._reader.cursor()
        cursor.execute(self.GET_ATTEMPTS_COUNT, (task_id,))
        row = cursor.fetchone()
        cursor.close()
        attempt_count = row["num_attempts"]
        return attempt_count

    def update_task_to_done(self, task_id):
        self._writer.execute('BEGIN EXCLUSIVE')
        cursor = self._writer.cursor()
        try:
            cursor.execute(self.UPDATE_TASK_TO_DONE, (task_id, ))
        except Exception as e:
            cursor.close()
            self._writer.rollback()
            raise e
        else:
            cursor.close()
            self._writer.commit()

    def update_task_to_inprocess(self, task_id):
        self._writer.execute('BEGIN EXCLUSIVE')
        cursor = self._writer.cursor()
        try:
            cursor.execute(self.UPDATE_TASK_TO_INPROCESS, (task_id, ))
        except Exception as e:
            cursor.close()
            self._writer.rollback()
            raise e
        else:
            cursor.close()
            self._writer.commit()

    def new_attempt(self, task_id, runner, start_time):
        self._writer.execute('BEGIN EXCLUSIVE')
        cursor = self._writer.cursor()
        attempt_id = None
        try:
            cursor.execute(self.INSERT_ATTEMPT, (task_id, runner, start_time, TaskAttempt.DEFAULT_STATUS))
            attempt_id = cursor.lastrowid
        except Exception as e:
            cursor.close()
            self._writer.rollback()
            raise e
        else:
            cursor.close()
            self._writer.commit()
        return attempt_id

    def is_task_completed(self, task_id):
        # any completed attempt for a task is the whole task is completed
        cursor = self._reader.cursor()
        cursor.execute(self.GET_ATTEMPTS_COUNT_FOR_STATUS, (task_id, TaskAttempt.COMPLETED_STATUS))
        attempt_count_row = cursor.fetchone()
        attempt_count = attempt_count_row["num_attempts"]
        cursor.close()
        return attempt_count > 0

    def is_task_failed(self, task_id):
        # if the number of failed attempts for a task is >= max_attempts for the task and there is no completed attempt then it is failed
        if self.is_task_completed(task_id):
            return False
        cursor = self._reader.cursor()
        cursor.execute(self.GET_ATTEMPTS_COUNT_FOR_STATUS, (task_id, TaskAttempt.FAILED_STATUS))
        attempt_count_row = cursor.fetchone()
        attempt_count = attempt_count_row["num_attempts"]
        cursor.execute(self.GET_TASK, (task_id,))
        task_row = cursor.fetchone()
        max_attempts = task_row["max_attempts"]
        cursor.close()
        return attempt_count >= max_attempts

    def update_attempt_to_fail(self, attempt_id, fail_reason, time_stamp):
        # only update if not already reported on
        attempt = self.get_attempt(attempt_id)
        if attempt is not None:
            if not attempt.is_in_process():
                self._logger.error("Trying to update attempt %s to failed but it is not in process. No update will be done." % str(attempt_id))
            else:
                self._writer.execute('BEGIN EXCLUSIVE')
                cursor = self._writer.cursor()
                try:
                    cursor.execute(self.UPDATE_ATTEMPT_TO_FAILED, (time_stamp, fail_reason, attempt_id))
                except Exception as e:
                    cursor.close()
                    self._writer.rollback()
                    self._logger.exception("Error updating attempt %s to failed!" % str(attempt_id))
                else:
                    cursor.close()
                    self._writer.commit()

    def update_attempt_to_complete(self, attempt_id, time_stamp):
        # only update if not already complete
        attempt = self.get_attempt(attempt_id)
        if attempt is not None:
            if attempt.is_completed():
                self._logger.info("Trying to update attempt %s to completed but it is already complete. No update will be done." % str(attempt_id))
            else:
                self._writer.execute('BEGIN EXCLUSIVE')
                cursor = self._writer.cursor()
                try:
                    cursor.execute(self.UPDATE_ATTEMPT_TO_COMPLETE, (time_stamp, attempt_id))
                except Exception as e:
                    cursor.close()
                    self._writer.rollback()
                    self._logger.exception("Error updating attempt %s to complete!" % str(attempt_id))
                else:
                    cursor.close()
                    self._writer.commit()

    def get_dependents(self, task_id):
        cursor = self._reader.cursor()
        cursor.execute(self.GET_DEPENDENTS, (task_id, ))
        rows = cursor.fetchall()
        cursor.close()
        dependent_task_ids = []
        for row in rows:
            dependent_task_ids.append(row["task_id"])
        return dependent_task_ids

    def is_task_done(self, task_id):
        cursor = self._reader.cursor()
        cursor.execute(self.GET_DEPENDENTS, (task_id,))
        row = cursor.fetchall()
        cursor.close()
        if row is not None:
            return True
        else:
            return False
