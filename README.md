# Tullius: Fault-tolerant Python Task Processing

Tullius can be used to run tasks that are critical to your application. It is designed to ensure that no task silently fails; every failure will be handled. It can run on one or more servers, with each server processing tasks and handling failures.

Tullius relies on MongoDB for fault-tolerant data storage. If you are processing tasks on multiple servers, you should also run a MongoDB replica set to ensure maximum fault tolerance.

## Example

Here is an example task that will count up to a number with one-second intervals:

    class ExampleTask(tullius.Task):

        timeout = 5

        def run(self):
            for i in range(self.count_to):
                logging.info('Counting: {}'.format(i))
                time.sleep(1)

        def failed(self):
            return self.copy(count_to=max(self.count_to - 1, 0))

To count to 3, you can execute:

    task = ExampleTask(count_to=3)
    tullius.queue(task)

Notice that the timeout is 5 (seconds). If we try to count to 10, our task will begin counting and get to 5, at which point 5 seconds will have elapsed. Then, the task processor will terminate the task, and it will be considered failed. A task processor will notice the failure, and call `task.failed()` to find out what to do. In this case, it will queue a copy of the task, with count_to reduced by one. This new task will try to count to 9, which will also fail, and queue a task counting to 8. These tasks will continue to fail until one tries to count to 4, which will complete in 5 seconds, and will be considered done.

## Setup

To install Tullius, first set up MongoDB. If you are processing tasks on more than one server, it should be a replica set.

Ensure you have Python 2 and pymongo installed. Install Tullius by running `./install` in the Tullius directory.

Configure Tullius by creating a `tullius_deps.py` module in your Python path. Here is an example:

    import pymongo

    # Connect to local MongoDB, with journaling enabled, using database name "tullius"
    db = pymongo.MongoClient(j=True).tullius

    # Run 10 processes handling tasks of priorities 0 through 10,
    # and 5 processes handling tasks of only priority 0
    task_processes = [(10, 0, 10), (5, 0, 0)]

`db` is the pymongo connection to your MongoDB database. Make sure to set the appropriate write concern options -- if you are using a replica set, you should probably set `w='majority'`.

`task_processes` is the number of processes that should be allocated to processing tasks of different priorities. The above configuration allocates 10 processes to process tasks of priority 0 to 10, and 5 processes to process tasks of priority 0. This reserves 5 processes for processing real-time (priority 0) tasks.

Now that Tullius is installed and configured, you can run the daemon, `tulliusd`. Try running it on the command line to make sure everything is working. If you see no output and it keeps running, it is set up properly and watching for tasks to process.

You can create an Upstart job to automatically run `tulliusd`. Just create a file `/etc/init/tullius.conf` with the following contents:

    start on runlevel [2]
    stop on runlevel [016]
    exec tulliusd

Now you can use `service tullius start` and `service tullius stop` to start and stop tullius.
