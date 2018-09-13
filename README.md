_Note to all people wanting to use this project -- it's close. v0.1.0 is scheduled for 2018-09-15 and I'm fairly certain I'm going to hit that self-imposed deadline. You will know it is ready for prime time when there is more documentation than this simple README.md_

# SimpleTaskQueue
SimpleTaskQueue (STQ) simple to deploy and use task queue used for coordinating distributed, parallel work.

It might be smart and complex on the backend but it is simple for you. The goal of STQ: allow you to o from need to processing quickly.

Distributing tasks and automating processing shouldn't be an excercise in extreme infrastrucutre management. STQ doesn't require database configuration, messaging queues, multiple process coordination, and on and on. Download. Launch a server. Add some tasks. Launch a runner. Then launch some more runners.

## RESTful communication
Process a series of tasks with multiple runners without having to worry about installing messaging queues, databases, message protocols, etc. 

Starting a new task runner is simply running the client and passing in the server url.

## DAG
Tasks are dependent on other tasks. The dependency flow can be many-to-many. Naturally protects against circular dependencies.

## Batteries Included
Task management server, smart task runner, RESTful server, and basic task monitoring dashboard all included.

