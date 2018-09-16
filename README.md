[Latest Release] (https://github.com/nabicht/SimpleTaskQueue/releases/tag/0.1.0)

# About SimpleTaskQueue
SimpleTaskQueue (STQ) is a simple to deploy and use task queue that coordinates distributed, parallel work.

It might be smart and complex on the backend but it is simple for you. The goal of STQ: allow you to go from need to processing quickly.

Distributing tasks and automating processing shouldn't be an exercise in extreme infrastructure management. STQ doesn't require database configuration, messaging queues, multiple process coordination, and on and on. Download. Launch a server. Add some tasks. Launch a runner. Then launch some more runners.

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

## What SimpleTaskQueue is Not
SimpleTaskQueue is a simple to use task queue. It does its job. it doesn't do other jobs. This means there is a lot that it is not.

### NOT a Streaming Data / Data Distribution Platform
STQ does not send a lot of data around with each Task and Attempt. In fact, it just sends the commands to be run and some organizational data. The Tasks that run need to worry about sourcing input data and pushing output to the proper places. There are great tools/platforms/paradigms for moving data around. STQ is not one of these things.

### NOT Environment Coordination / Infrastructure Setup / Configuration Synchronization
You could use STQ to drive running the tasks to do Environment Coordination / Infrastructure Setup / Configuration Synchronization, but you would need to write all the playbooks/tasks/etc. necessary for this to work. STQ does not come with this out of the box. And it probably never will. If you need this and you don't want to write all the tasks and manage all the necessary code then use a different tool. Lots of people use lots of different tools for this. There are Dev Ops holy wars over the right ones to use. I'll leave it to you to find your own.

### NOT a Timed Job Schedule (Yet?)
STQ does not do timed tasks. There is no concept (yet) of starting a given job at a given time. This isn't cron or task scheduler (or whatever you like to use). If you want to have timed jobs then use something like CRON to kick off a script that uses STQ's RESTful interface to load a Task (or series of Tasks) into STQ. Just note that this does not guarantee that they get started at the time your job kicks off. This simply guarantees that your tasks are added to the job queue at that time.



