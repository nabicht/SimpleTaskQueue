_Note to all people wanting to use this project -- it's close. v0.1.0 is scheduled for 2018-09-15 and I'm fairly certain I'm going to hit that self-imposed deadline. You will know it is ready for prime time when there is more documentation than this simple README.md_

# SimpleTaskQueue
SimpleTaskQueue (STQ) simple to deploy and use task queue used for coordinating distributed, parallel work.

It might be smart and complex on the backend but it is simple for you. The goal of STQ: allow you to go from need to processing quickly.

Distributing tasks and automating processing shouldn't be an excercise in extreme infrastrucutre management. STQ doesn't require database configuration, messaging queues, multiple process coordination, and on and on. Download. Launch a server. Add some tasks. Launch a runner. Then launch some more runners.

## RESTful communication
Process a series of tasks with multiple runners without having to worry about installing messaging queues, databases, message protocols, etc. 

Starting a new task runner is simply running the client and passing in the server url.

## DAG
Tasks are dependent on other tasks. The dependency flow can be many-to-many. Naturally protects against circular dependencies.

## Batteries Included
Task management server, smart task runner, RESTful server, and basic task monitoring dashboard all included. No additional applications/servers/languages/etc. required. Just a handful of basic python libraries, all readily avaible via pip.

## The Language of SimpleTaskQueue

### Runner
A Runner is an STQ client that peforms the actual work of the Task. It runs one Task's Attempt at a time (see 'Attempts vs Tasks' below to understand the difference between a Task and an Attempt).

STQ comes with a python-based Runner. But since Runners communicate with STQ via a RESTful API, it is easy to write a Runner in whatever language you want.

### Task States
At any given time, Tasks are in one of four states:

1. **To Do**: The Task is waiting to be attempted.
2. **In Process**: A Task is underway. This does not mean that a runner is actively working on the task; rather, it is indicative of a state where at least the first Attempt has been started and the Task is not Completed or Failed.
3. **Completed**: A Task has been completed successfully, but we just say "completed" since success is implied by the fact that it isn't Failed. To be Completed only one of the Task's Attempts needs to have Completed. Once a Task is Completed no other Attempts will be distributed to Runners.
4. **Failed**: A Task has failed. All Attempts have been run and all of them failed. Once a Task is Failed, no other Attempts will be distributed to Runners.

### Attempts vs Tasks
You want your tasks to complete successfully so STQ does too. This is why each task can be attempted more than once (optionally). 

Every Task has one ore more Attempts. The amount of Attempts defaults to one for each Task and can be set with `max_attempts` upon Task creation. 

A Task is completed as soon as any on Attempt is completed. A Task is not failed until all of its Attempts are failed.

A Task can have an expected duration. This can be set with `duration` upon Task creation. If a Runner is executing an Attempt more than the expected duration, STQ aggressively assumes that the running of the Attempt has failed. If there are more Attempts left of the Task, then the next Attempt will be queued up and distributed to a Runner. This way a Runner error, hung Runner, infrastructure issue, etc. can possibly be overcome and mission critical tasks get another shot at completion.

Note that this does mean mulitple attempts for Task could end up being completed. This is okay and should be acceptable. Better to be completed more than once than not completed at all.


