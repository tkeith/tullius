from . import utils
from .tasks import Task, handle_pending_task

def add_class_to_task_documents():
    # Make sure we don't have any next_tasks fields that will be inserted
    while handle_pending_task():
        pass

    db = utils.get_db()
    while True:
        db_task = db.tasks.find_one({'class': {'$exists': False}})
        if db_task is None:
            print 'Done!'
            return
        task = Task.from_db(db_task)
        class_name = utils.qualified_name(type(task))
        db.tasks.update({'_id': db_task['_id'], 'class': {'$exists': False}}, {'$set': {'class': class_name}})
        print 'Updated {}'.format(task)

def remove_next_tasks_from_completed_tasks():
    while handle_pending_task():
        pass

    db = utils.get_db()
    docs_updated = db.tasks.update({'$or': [{'next_tasks': {'$exists': True}}, {'next_status': {'$exists': True}}]}, {'$unset': {'next_tasks': '', 'next_status': ''}}, multi=True)['n']
    print 'Documents updated: {}'.format(docs_updated)
