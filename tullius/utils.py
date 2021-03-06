import multiprocessing
import multiprocessing.queues
import time
import pymongo
import tullius_deps as deps

class Timeout(Exception): pass

def call_in_process(func, timeout=None):
    res_queue = multiprocessing.Queue()

    def target():
        try:
            res_queue.put((True, func()))
        except Exception as e:
            res_queue.put((False, e))

    proc = multiprocessing.Process(target=target)
    proc.start()
    proc.join(timeout)
    proc.terminate()
    proc.join()

    try:
        res = res_queue.get_nowait()
    except multiprocessing.queues.Empty:
        raise Timeout

    if(res[0]):
        return res[1]
    else:
        raise res[1]

def retry(function, exception, interval=0, backoff=0, tries=None, timeout=None):
    if tries == 0 or timeout == 0:
        raise Timeout
    attempts = 0
    end = time.time() + timeout if timeout is not None else None
    while True:
        try:
            return function()
        except exception:
            attempts += 1
            if (tries is not None and attempts >= tries) or (end is not None and time.time() >= end - interval):
                raise Timeout
            time.sleep(interval)
            interval += backoff

def mongo_retry(function):
    try:
        return retry(function, pymongo.errors.AutoReconnect, backoff=1, timeout=60)
    except pymongo.errors.DuplicateKeyError:
        return None

def ensure_list(x):
    try:
        return list(x)
    except TypeError:
        if x is None:
            return []
        return [x]

_db = None

def get_db():
    global _db
    if _db is None:
        _db = deps.make_db()
    return _db

def qualified_name(cls):
    return cls.__module__ + '.' + cls.__name__
