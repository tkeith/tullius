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

    required_attrs = ['priority', 'timeout', 'start_after', 'run', 'failed']

    priority = 5

    def __init__(self, start_after=None, delay=None, **kwargs):
        self.input = input
        if start_after is not None:
            assert isinstance(start_after, datetime)
            self.start_after = start_after
        elif delay is not None:
            self.start_after = datetime.now() + utils.ensure_timedelta(delay)
        else:
            self.start_after = datetime.now()

        self.attrs_to_copy = list()

        for k, v in kwargs.items():
            setattr(self, k, v)
            self.attrs_to_copy.append(k)

        for attr in self.required_attrs:
            assert hasattr(self, attr)

    def copy(self, **kwargs):
        for attr in self.attrs_to_copy:
            if attr not in kwargs:
                kwargs[attr] = getattr(self, attr)
        return type(self)(**kwargs)

    def for_db(self):
        id = bson.ObjectId()
        return {'_id': id, 'start_after': self.start_after, 'status': 'queued', 'priority': self.priority, 'pickle': pickle.dumps(self)}

    @staticmethod
    def from_db(db_obj):
        return pickle.loads(db_obj['pickle'])

def insert_db_tasks(db_tasks):
    for db_task in db_tasks:
        utils.mongo_retry(lambda: db.tasks.insert(db_task))

def queue(tasks):
    tasks = utils.ensure_list(tasks)
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
        db_task = utils.mongo_retry(lambda: db.tasks.find_one({'status': 'queued', 'start_after': {'$lte': datetime.now()}, 'priority': {'$gte': min_priority, '$lte': max_priority}}, sort=[('priority', pymongo.ASCENDING), ('start_after', pymongo.ASCENDING)]))
        if db_task is None:
            time.sleep(1)
            continue

        def process_db_task():
            task = Task.from_db(db_task)
            id = db_task['_id']
            res = utils.mongo_retry(lambda: db.tasks.update({'_id': id, 'status': 'queued'}, {'$set': {'status': 'running', 'timeout_time': datetime.now() + timedelta(seconds=task.timeout)}}))
            if res['n'] == 0:
                return

            try:
                next_tasks = utils.call_in_process(task.run, timeout=task.timeout)
            except Exception:
                try:
                    next_tasks = task.failed()
                except Exception:
                    # Failure logic raised an exception, just ignore
                    next_tasks = []
                status = 'failed'
            else:
                status = 'done'

            next_tasks = utils.ensure_list(next_tasks)

            done_task(task, id, status, next_tasks)

        utils.call_in_process(process_db_task)

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

        def process_db_task():
            task = Task.from_db(db_task)
            id = db_task['_id']
            next_tasks = utils.ensure_list(task.failed())
            done_task(task, id, 'failed', next_tasks)

        utils.call_in_process(process_db_task)

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
