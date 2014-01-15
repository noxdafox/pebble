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


from uuid import uuid4
from inspect import isclass
from traceback import format_exc
from itertools import count
from threading import Thread, Event
from collections import Callable
from functools import update_wrapper
try:  # Python 2
    from Queue import Queue, Empty, Full
except:  # Python 3
    from queue import Queue, Empty, Full

from .pebble import TimeoutError, TaskCancelled


def worker(queue, limit, initializer, initargs):
    error = None
    results = None
    counter = count()

    if isinstance(initializer, Callable):
        try:
            initializer(*initargs)
        except Exception as err:
            error = err
            error.traceback = format_exc()

    while limit == 0 or next(counter) < limit:
        try:
            function, task, args, kwargs = queue.get()
            results = function(*args, **kwargs)
        except Exception as err:
            error = err
            error.traceback = format_exc()
        finally:
            task._set(error is not None and error or results)
            error = None
            results = None


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


class Task(object):
    """Handler to the ongoing task."""
    def __init__(self, task_nr, callback):
        self.id = uuid4()
        self.number = task_nr
        self._ready = False
        self._cancelled = False
        self._results = None
        self._event = Event()
        self._callback = callback

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return "%s (Task-%d, %s)" % (self.__class__, self.number, self.id)

    @property
    def ready(self):
        return self._ready

    @property
    def cancelled(self):
        return self._cancelled

    def get(self, timeout=None):
        """Retrieves the produced results.

        If the executed code raised an error it will be re-raised.

        """
        self._event.wait(timeout)
        if self._ready:
            if (isinstance(self._results, BaseException)):
                raise self._results
            else:
                return self._results
        else:
            raise TimeoutError("Task is still running")

    def cancel(self):
        """Cancels the Task dropping the results."""
        self._cancelled = True
        self._set(TaskCancelled("Task has been cancelled"))

    def _set(self, results):
        self._results = results
        self._ready = True
        self._event.set()
        if self._callback is not None and not self._cancelled:
            self._callback(self)


class ThreadPool(object):
    def __init__(self, workers=1, task_limit=0, queue=None, queueargs=None,
                 initializer=None, initargs=None):
        self._counter = count()
        self._workers = workers
        self._limit = task_limit
        self._pool = []
        self.initializer = initializer
        self.initargs = initargs
        if queue is not None:
            if isclass(queue):
                self._queue = queue(*queueargs)
            else:
                raise ValueError("Queue must be Class")
        else:
            self._queue = Queue()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    @property
    def queue(self):
        return self._queue

    @queue.setter
    def queue(self, queue):
        while 1:
            try:
                queue.put(self._queue.get(False), False)
            except Empty:
                self._queue = queue
                return
            except Full:
                raise ValueError("Provided queue too small")

    def _restart_workers(self):
        """Spawn one worker if pool is not full."""
        self._pool = [w for w in self._pool if w.is_alive()]
        missing = self._workers - len(self._pool)
        if missing:
            t = Thread(target=worker,
                       args=(self._queue, self._limit,
                             self.initializer, self.initargs))
            t.daemon = True
            t.start()
            self._pool.append(t)

    def schedule(self, function, args=(), kwargs={}, callback=None):
        task = Task(next(self._counter), callback)
        # loop maintaining the workers alive in case of full queue
        while 1:
            self._restart_workers()
            try:
                self._queue.put((function, task, args, kwargs), timeout=0.1)
                break
            except Full:
                continue

        return task


class ThreadWrapper(object):
    def __init__(self, function, callback):
        self._function = function
        self._counter = count()
        self.callback = callback
        update_wrapper(self, function)

    def __call__(self, *args, **kwargs):
        task = Task(next(self._counter), self.callback)
        q = DummyQueue((self._function, task, args, kwargs))
        t = Thread(target=worker, args=(q, 1, None, None))
        t.daemon = True
        t.start()
        return task


class PoolWrapper(object):
    def __init__(self, function, workers, task_limit, queue, queueargs,
                 callback, initializer, initargs):
        self._function = function
        self._pool = ThreadPool(workers, task_limit, queue, queueargs,
                                initializer, initargs)
        self.callback = callback
        update_wrapper(self, function)

    @property
    def queue(self):
        return self._pool.queue

    @queue.setter
    def queue(self, queue):
        self._pool.queue = queue

    def __call__(self, *args, **kwargs):
        return self._pool.schedule(self._function, args=args, kwargs=kwargs,
                                   callback=self.callback)


class DummyQueue(list):
    def __init__(self, elements):
        super(DummyQueue, self).__init__(((elements), ))

    def get(self):
        return self.pop()
