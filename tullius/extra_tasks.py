from . import tasks
from datetime import timedelta

class RetryTask(tasks.Task):
    '''
    Retry a task up to `tries` times until it succeeds. If it fails `tries`
    times, run the task returned by `retry_failed`. Wait `interval` time
    between tries, with `backoff` additional delay each time.
    '''

    required_attrs = tasks.Task.required_attrs + ['tries', 'interval', 'backoff', 'retry_failed']

    backoff = timedelta(0)

    def __init__(self, next_delay=None, **kwargs):
        super(RetryTask, self).__init__(**kwargs)
        if next_delay is None:
            next_delay = self.interval
        self.next_delay = next_delay

    def failed(self):
        if self.tries <= 1:
            return self.retry_failed()

        return self.copy(tries=self.tries - 1, delay=self.next_delay, next_delay=self.next_delay + self.backoff)
