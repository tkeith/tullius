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
            self.start_after = datetime.now() + delay
        else:
            self.start_after = datetime.now()

        self.attrs_to_copy = list()

        for k, v in kwargs.items():
            setattr(self, k, v)
            self.attrs_to_copy.append(k)

        for attr in self.required_attrs:
            if not hasattr(self, attr):
                raise Exception('Missing attribute: {}'.format(attr))

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

def process_task(task_processes_avail):
    for sem, min_priority, max_priority in task_processes_avail:
        if sem.acquire(False):
            db_task = utils.mongo_retry(lambda: db.tasks.find_one({'status': 'queued', 'start_after': {'$lte': datetime.now()}, 'priority': {'$gte': min_priority, '$lte': max_priority}}, sort=[('priority', pymongo.ASCENDING), ('start_after', pymongo.ASCENDING)]))
            if db_task is None:
                sem.release()
            else:
                break
    else:
        return False

    def process_db_task():
        task = Task.from_db(db_task)
        id = db_task['_id']
        res = utils.mongo_retry(lambda: db.tasks.update({'_id': id, 'status': 'queued'}, {'$set': {'status': 'running', 'timeout_time': datetime.now() + task.timeout}}))
        if res['n'] == 0:
            return

        try:
            next_tasks = utils.call_in_process(task.run, timeout=task.timeout.total_seconds())
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

        sem.release()

    multiprocessing.Process(target=process_db_task).start()

    return True

def handle_dead_task():
    db_task = utils.mongo_retry(lambda: db.tasks.find_one({'status': 'running', 'timeout_time': {'$lte': datetime.now() - timedelta(seconds=60)}}))
    if db_task is None:
        return False

    # Unpickle Task in new process so that it reloads without restarting Tullius

    def process_db_task():
        task = Task.from_db(db_task)
        id = db_task['_id']
        next_tasks = utils.ensure_list(task.failed())
        done_task(task, id, 'failed', next_tasks)

    utils.call_in_process(process_db_task)

    return True

def handle_pending_task():
    db_task = utils.mongo_retry(lambda: db.tasks.find_one({'status': 'next_tasks_pending'}))
    if db_task is None:
        return False

    insert_db_tasks(db_task['next_tasks'])
    utils.mongo_retry(lambda: db.tasks.update({'_id': db_task['_id']}, {'$set': {'status': db_task['next_status']}}))

    return True

def daemon():
    task_processes_avail = [(multiprocessing.Semaphore(num), min_priority, max_priority) for (num, min_priority, max_priority) in task_processes]
    while True:
        if not (handle_dead_task() or handle_pending_task() or process_task(task_processes_avail)):
            time.sleep(0.25)
