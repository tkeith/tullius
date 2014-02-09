import bson
from tullius_deps import db
import pickle
import pymongo
from datetime import datetime, timedelta
from . import utils
import time

def ensure_indexes():
    db.tasks.ensure_index('priority')
    db.tasks.ensure_index('status')
    db.tasks.ensure_index('dead_time')
    db.tasks.ensure_index('queue_time')

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

def task_for_db(task):
    id = bson.ObjectId()
    return {'_id': id, queue_time: datetime.now(), 'status': 'new', 'priority': task.priority, 'obj': pickle.dumps(task)}

def task_from_db(db_task):
    return pickle.loads(db_task['obj'])

def queue(task):
    utils.mongo_retry(lambda: db.tasks.insert(task_for_db(task)), True)

def done_task(task, id, status, next_task):
    if next_task is None:
        update = {'$set': {'status': status}}
    else:
        update = {'$set': {'status': 'next_task_pending', 'next_status': status, 'next_task': task_for_db(next_task)}}
    utils.mongo_retry(lambda: db.tasks.update({'_id': id, 'status': 'processing'}, update))

def process_tasks(min_priority, max_priority):
    while True:
        db_task = utils.mongo_retry(lambda: db.tasks.find_one({'status': 'new', 'priority': {'$gte': min_priority, '$lte': max_priority}}, sort=[('priority', pymongo.ASCENDING), ('queue_time': pymongo.ASCENDING)]))
        if db_task is None:
            time.sleep(1)
            continue

        task = task_from_db(db_task)
        id = db_task['_id']
        res = utils.mongo_retry(lambda: db.tasks.update({'_id': id, 'status': 'new'}, {'$set': {'status': 'processing', 'dead_time': datetime.now() + timedelta(seconds=task.timeout + 60)}}))
        if res['n'] == 0:
            continue

        try:
            next_task = utils.timeout(task.run, task.timeout)
        except Exception:
            next_task = task.failed()
            status = 'failed'
        else:
            status = 'done'

        done_task(task, id, status, next_task)

def process_updates():
    while True:
        if not (handle_dead_tasks() or handle_pending_tasks()):
            time.sleep(1)

def handle_dead_tasks():
    result = False
    while True:
        db_task = utils.mongo_retry(lambda: db.tasks.find_one({'status': 'processing', 'dead_time': {'$lte': datetime.now()}}))
        if db_task is None:
            return result
        result = True
        task = task_from_db(db_task)
        id = db_task['_id']
        next_task = task.failed()
        done_task(task, id, 'failed', next_task)

def handle_pending_tasks():
    result = False
    while True:
        db_task = utils.mongo_retry(lambda: db.tasks.find_one({'status': 'next_task_pending'}))
        if db_task is None:
            return result
        result = True
        utils.mongo_retry(lambda: db.tasks.insert(db_task['next_task']), True)
        utils.mongo_retry(lambda: db.tasks.update({'_id': db_task['_id']}, {'$set': {'status': db_task['next_status']}}))
