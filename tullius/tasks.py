import bson
from tullius_deps import db, task_processes
import pickle
import pymongo
from datetime import datetime, timedelta
from . import utils
import time
import numbers
import multiprocessing

def ensure_indexes():
    db.tasks.ensure_index('priority')
    db.tasks.ensure_index('status')
    db.tasks.ensure_index('timeout_time')
    db.tasks.ensure_index('start_after')

utils.mongo_retry(ensure_indexes)

class Task(object):

    priority = 5
    timeout = 60

    def __init__(self, input=None):
        self.input = input

    def run(self):
        return None

    def failed(self):
        return None

    def for_db(self):
        id = bson.ObjectId()
        return {'_id': id, 'start_after': datetime.now(), 'status': 'new', 'priority': self.priority, 'pickle': pickle.dumps(self)}

    @staticmethod
    def from_db(db_obj):
        return pickle.loads(db_obj['pickle'])

class ScheduledTask(object):

    def __init__(self, task, time):
        if isinstance(time, numbers.Number):
            time = timedelta(seconds=time)
        if isinstance(time, timedelta):
            time = datetime.now() + time
        self.task = task
        self.time = time

    def for_db(self):
        db_obj = task.for_db()
        db_obj.start_after = self.time
        return db_obj

def insert_db_tasks(db_tasks):
    for db_task in db_tasks:
        utils.mongo_retry(lambda: db.tasks.insert(db_task), True)

def queue(tasks):
    tasks = ensure_list(tasks)
    db_tasks = [task.for_db() for task in tasks]
    insert_db_tasks(db_tasks)

def done_task(task, id, status, next_tasks):
    if len(next_tasks) == 0:
        update = {'$set': {'status': status}}
    else:
        next_tasks_for_db = [task.for_db() for task in next_tasks]
        update = {'$set': {'status': 'next_tasks_pending', 'next_status': status, 'next_tasks': next_tasks_for_db}}
    utils.mongo_retry(lambda: db.tasks.update({'_id': id, 'status': 'running'}, update))

def process_tasks(min_priority, max_priority):
    while True:
        db_task = utils.mongo_retry(lambda: db.tasks.find_one({'status': 'new', 'start_after': {'$lte': datetime.now()}, 'priority': {'$gte': min_priority, '$lte': max_priority}}, sort=[('priority', pymongo.ASCENDING), ('start_after', pymongo.ASCENDING)]))
        if db_task is None:
            time.sleep(1)
            continue

        task = task_from_db(db_task)
        id = db_task['_id']
        res = utils.mongo_retry(lambda: db.tasks.update({'_id': id, 'status': 'new'}, {'$set': {'status': 'running', 'timeout_time': datetime.now() + timedelta(seconds=task.timeout)}}))
        if res['n'] == 0:
            continue

        try:
            next_tasks = utils.timeout(task.run, task.timeout)
        except Exception:
            next_tasks = task.failed()
            status = 'failed'
        else:
            status = 'done'

        next_tasks = ensure_list(next_tasks)

        done_task(task, id, status, next_tasks)

def process_updates():
    while True:
        if not (handle_dead_tasks() or handle_pending_tasks()):
            time.sleep(1)

def handle_dead_tasks():
    result = False
    while True:
        db_task = utils.mongo_retry(lambda: db.tasks.find_one({'status': 'running', 'timeout_time': {'$lte': datetime.now() - timedelta(seconds=60)}}))
        if db_task is None:
            return result
        result = True
        task = task_from_db(db_task)
        id = db_task['_id']
        next_tasks = task.failed()
        done_task(task, id, 'failed', next_tasks)

def handle_pending_tasks():
    result = False
    while True:
        db_task = utils.mongo_retry(lambda: db.tasks.find_one({'status': 'next_tasks_pending'}))
        if db_task is None:
            return result
        result = True
        insert_db_tasks(db_task['next_tasks'])
        utils.mongo_retry(lambda: db.tasks.update({'_id': db_task['_id']}, {'$set': {'status': db_task['next_status']}}))

def daemon():
    for num, min_priority, max_priority in task_processes:
        for i in range(num):
            def target():
                process_tasks(min_priority, max_priority)
            proc = multiprocessing.Process(target=target)
            proc.start()
    process_updates()
