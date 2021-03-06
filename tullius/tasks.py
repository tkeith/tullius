import bson
from tullius_deps import task_processes
import pickle
import pymongo
from datetime import datetime, timedelta
from . import utils
from utils import get_db
import time
import numbers
import multiprocessing
import threading
import logging
import traceback

log = logging.getLogger(__name__)

def ensure_indexes():
    utils.mongo_retry(lambda: get_db().tasks.ensure_index([('status', pymongo.ASCENDING), ('start_after', pymongo.ASCENDING)]))
    utils.mongo_retry(lambda: get_db().tasks.ensure_index([('class', pymongo.ASCENDING), ('status', pymongo.ASCENDING)]))

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

        assert isinstance(self.timeout, timedelta)
        assert isinstance(self.priority, numbers.Real)
        assert isinstance(self.start_after, datetime)

    def copy(self, **kwargs):
        for attr in self.attrs_to_copy:
            if attr not in kwargs:
                kwargs[attr] = getattr(self, attr)
        return type(self)(**kwargs)

    def for_db(self):
        id = bson.ObjectId()
        return {'_id': id, 'start_after': self.start_after, 'status': 'queued', 'priority': self.priority, 'timeout': self.timeout.total_seconds(), 'class': utils.qualified_name(type(self)), 'pickle': pickle.dumps(self)}

    @staticmethod
    def from_db(db_obj):
        return pickle.loads(db_obj['pickle'])

def task_failed(task):
    try:
        next_tasks = utils.ensure_list(task.failed())
    except Exception:
        # Failure logic raised an exception, just ignore
        next_tasks = []
    return next_tasks

def insert_db_tasks(db_tasks):
    for db_task in db_tasks:
        utils.mongo_retry(lambda: get_db().tasks.insert(db_task))

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
    utils.mongo_retry(lambda: get_db().tasks.update({'_id': id, 'status': 'running'}, update))

def process_task(task_processes_avail):
    for sem, min_priority, max_priority in task_processes_avail:
        log.debug('trying to acquire sem for min {} max {}'.format(min_priority, max_priority))
        if sem.acquire(block=False):
            log.debug('acquired semaphore')
            db_task = utils.mongo_retry(lambda: get_db().tasks.find_one({'status': 'queued', 'start_after': {'$lte': datetime.now()}, 'priority': {'$gte': min_priority, '$lte': max_priority}}, sort=[('priority', pymongo.ASCENDING), ('start_after', pymongo.ASCENDING)]))
            if db_task is None:
                log.debug('no task to process, releasing sem')
                sem.release()
                log.debug('sem released')
            else:
                log.debug('found task to process')
                break
        else:
            log.debug('did not acquire semaphore')
    else:
        return False

    id = db_task['_id']

    log.debug('trying to lock task')
    res = utils.mongo_retry(lambda: get_db().tasks.update({'_id': id, 'status': 'queued'}, {'$set': {'status': 'running', 'timeout_time': datetime.now() + timedelta(seconds=db_task['timeout'])}}))
    if res['n'] == 0:
        log.debug('another instance got the task first, releasing sem')
        # Another instance got to this task before us
        sem.release()
        log.debug('sem released')
        return True

    def process_db_task():
        task = Task.from_db(db_task)

        log.info('processing task: {}'.format(task))

        try:
            next_tasks = utils.ensure_list(utils.call_in_process(task.run, timeout=task.timeout.total_seconds()))
        except Exception:
            log.info('task {} failed: {}'.format(task, traceback.format_exc()))
            log.debug('running failure method')
            next_tasks = task_failed(task)
            log.debug('done failure method')
            status = 'failed'
        else:
            log.info('task succeeded: {}'.format(task))
            status = 'done'

        done_task(task, id, status, next_tasks)
        log.debug('releasing sem')
        sem.release()

    proc = multiprocessing.Process(target=process_db_task)
    proc.start()

    def join_process():
        proc.join()

    threading.Thread(target=join_process).start()

    return True

def handle_dead_task():
    db_task = utils.mongo_retry(lambda: get_db().tasks.find_one({'status': 'running', 'timeout_time': {'$lte': datetime.now() - timedelta(seconds=60)}}))
    if db_task is None:
        return False

    # Unpickle Task in new process so that it reloads without restarting Tullius

    def process_db_task():
        task = Task.from_db(db_task)
        log.info('handling dead task: {}'.format(task))
        id = db_task['_id']
        next_tasks = task_failed(task)
        done_task(task, id, 'failed', next_tasks)
        log.info('done handling dead task: {}'.format(task))

    utils.call_in_process(process_db_task)

    return True

def handle_pending_task():
    db_task = utils.mongo_retry(lambda: get_db().tasks.find_one({'status': 'next_tasks_pending'}))
    if db_task is None:
        return False

    log.debug('handling pending task: {}'.format(db_task['class']))
    insert_db_tasks(db_task['next_tasks'])
    utils.mongo_retry(lambda: get_db().tasks.update({'_id': db_task['_id']}, {'$set': {'status': db_task['next_status']}, '$unset': {'next_tasks': '', 'next_status': ''}}))
    log.debug('done handling pending task: {}'.format(db_task['class']))

    return True

def daemon():
    log.info('daemon starting')
    log.info('ensuring indexes')
    ensure_indexes()
    log.info('done ensuring indexes. creating semaphores')
    task_processes_avail = [(multiprocessing.Semaphore(num), min_priority, max_priority) for (num, min_priority, max_priority) in task_processes]
    log.info('semaphores created. beginning main loop')
    while True:
        if not (handle_dead_task() or handle_pending_task() or process_task(task_processes_avail)):
            time.sleep(0.1)
