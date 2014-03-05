# This file is part of Pebble.

# Pebble is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License
# as published by the Free Software Foundation,
# either version 3 of the License, or (at your option) any later version.

# Pebble is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.

# You should have received a copy of the GNU Lesser General Public License
# along with Pebble.  If not, see <http://www.gnu.org/licenses/>.


from time import time, sleep
from inspect import isclass
from traceback import format_exc, print_exc
from itertools import count
from threading import Thread
from collections import Callable
from functools import update_wrapper
try:  # Python 2
    from Queue import Queue
except:  # Python 3
    from queue import Queue

from .pebble import Task, TimeoutError


STOPPED = 0
RUNNING = 1
CLOSING = 2
CREATED = 3


def thread(*args, **kwargs):
    """Turns a *function* into a Thread and runs its logic within.

    A decorated *function* will return a *Task* object once is called.

    If *callback* is a callable, it will be called once the task has ended
    with the task identifier and the *function* return values.

    """
    def wrapper(function):
        return ThreadWrapper(function, callback)

    if len(args) == 1 and not len(kwargs) and isinstance(args[0], Callable):
        return ThreadWrapper(args[0], None)
    elif not len(args) and len(kwargs):
        callback = kwargs.get('callback')

        return wrapper
    else:
        raise ValueError("Decorator accepts only keyword arguments.")


def thread_pool(*args, **kwargs):
    """Turns a *function* into a Thread and runs its logic within.

    A decorated *function* will return a *Task* object once is called.

    If *callback* is a callable, it will be called once the task has ended
    with the task identifier and the *function* return values.

    """
    def wrapper(function):
        return PoolWrapper(function, workers, task_limit, queue, queue_args,
                           callback, initializer, initargs)

    if len(args) == 1 and not len(kwargs) and isinstance(args[0], Callable):
        return PoolWrapper(args[0], 1, 0, None, None, None, None, None)
    elif not len(args) and len(kwargs):
        queue = kwargs.get('queue')
        queue_args = kwargs.get('queueargs')
        workers = kwargs.get('workers', 1)
        callback = kwargs.get('callback')
        initargs = kwargs.get('initargs')
        initializer = kwargs.get('initializer')
        task_limit = kwargs.get('worker_task_limit', 0)

        return wrapper
    else:
        raise ValueError("Decorator accepts only keyword arguments.")


class Worker(Thread):
    def __init__(self, queue, limit, initializer, initargs):
        Thread.__init__(self)
        self.queue = queue
        self.limit = limit
        self.initializer = initializer
        self.initargs = initargs
        self.daemon = True

    def run(self):
        error = None
        results = None
        counter = count()

        if self.initializer is not None:
            try:
                self.initializer(*self.initargs)
            except Exception as err:
                error = err
                error.traceback = format_exc()

        while self.limit == 0 or next(counter) < self.limit:
            task = self.queue.get()
            if task is None:  # worker terminated
                self.queue.task_done()
                return
            function = task._function
            args = task._args
            kwargs = task._kwargs
            try:
                if not task._cancelled:
                    task._timestamp = time()
                    results = function(*args, **kwargs)
            except Exception as err:
                error = err
                error.traceback = format_exc()
            finally:
                task._set(error is not None and error or results)
                if task._callback is not None:
                    try:
                        task._callback(task)
                    except:
                        print_exc()
                self.queue.task_done()
                error = None
                results = None


class ThreadPool(object):
    def __init__(self, workers=1, task_limit=0, queue=None, queueargs=None,
                 initializer=None, initargs=None):
        if queue is not None:
            if isclass(queue):
                self._queue = queue(*queueargs)
            else:
                raise ValueError("Queue must be Class")
        else:
            self._queue = Queue()
        self._pool_maintainer = Thread(target=self._maintain_pool)
        self._pool_maintainer.daemon = True
        self._counter = count()
        self._workers = workers
        self._limit = task_limit
        self._pool = []
        self._state = CREATED
        self.initializer = initializer
        self.initargs = initargs

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
        self.join()

    def _maintain_pool(self):
        while self._state != STOPPED:
            expired = [w for w in self._pool if not w.is_alive()]
            self._pool = [w for w in self._pool if w not in expired]
            for _ in range(self._workers - len(self._pool)):
                w = Worker(self._queue, self._limit,
                           self.initializer, self.initargs)
                w.start()
                self._pool.append(w)
            sleep(0.6)

    @property
    def active(self):
        return self._state == RUNNING and True or False

    def stop(self):
        """Stops the pool without performing any pending task."""
        self._state = STOPPED
        for w in self._pool:
            w.limit = - 1
        for w in self._pool:
            self._queue.put(None)

    def close(self):
        """Close the pool allowing all queued tasks to be performed."""
        self._state = CLOSING
        self._queue.join()
        self._state = STOPPED
        for w in self._pool:
            w.limit = - 1
        for w in self._pool:
            self._queue.put(None)

    def join(self, timeout=0):
        """Joins the pool waiting until all workers exited.

        If *timeout* is greater than 0,
        it block until all workers exited or raise TimeoutError.

        """
        counter = 0

        if self._state == RUNNING:
            raise RuntimeError('The Pool is still running')
        # if timeout is set join workers until its value
        while counter < timeout and self._pool:
            counter += (len(self._pool) + 1) / 10.0
            if self._pool_maintainer.is_alive():
                self._pool_maintainer.join(0.1)
            expired = [w for w in self._pool if w.join(0.1) is None
                       and not w.is_alive()]
            self._pool = [w for w in self._pool if w not in expired]
        # verify timeout expired
        if timeout > 0 and self._pool:
            raise TimeoutError('Workers are still running')
        # timeout not set
        self.pool = [w for w in self._pool if w.join() is None
                     and w.is_alive()]

    def schedule(self, function, args=(), kwargs={}, callback=None):
        """Schedules *function* into the Pool, passing *args* and *kwargs*
        respectively as arguments and keyword arguments.

        If *callback* is a callable it will be executed once the function
        execution has completed with the returned *Task* as a parameter.

        A *Task* object is returned.

        """
        if self._state == CREATED:
            self._pool_maintainer.start()
            self._state = RUNNING
        elif self._state != RUNNING:
            raise RuntimeError('The Pool is not running')
        if not isinstance(function, Callable):
            raise ValueError('function must be callable')
        task = Task(next(self._counter), function, args, kwargs, callback, 0)
        self._queue.put(task)

        return task


class ThreadWrapper(object):
    """Used by *thread* decorator."""
    def __init__(self, function, callback):
        self._function = function
        self._counter = count()
        self.callback = callback
        update_wrapper(self, function)

    def __call__(self, *args, **kwargs):
        t = Task(next(self._counter),
                 self._function, args, kwargs, self.callback, 0)
        q = DummyQueue(t)
        w = Worker(q, 1, None, None)
        w.start()
        return t


class PoolWrapper(object):
    """Used by *thread_pool* decorator."""
    def __init__(self, function, workers, task_limit, queue, queueargs,
                 callback, initializer, initargs):
        self._function = function
        self._pool = ThreadPool(workers, task_limit, queue, queueargs,
                                initializer, initargs)
        self.callback = callback
        update_wrapper(self, function)

    def __call__(self, *args, **kwargs):
        return self._pool.schedule(self._function, args=args, kwargs=kwargs,
                                   callback=self.callback)


class DummyQueue(list):
    def __init__(self, elements):
        super(DummyQueue, self).__init__(((elements), ))

    def get(self):
        return self.pop()

    def task_done(self):
        pass
