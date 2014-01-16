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
    from Queue import Queue, Full
except:  # Python 3
    from queue import Queue, Full

from .pebble import TimeoutError, TaskCancelled


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
            function, task, args, kwargs = self.queue.get()
            if function is None:
                self.queue.task_done()
                return
            try:
                results = function(*args, **kwargs)
            except Exception as err:
                error = err
                error.traceback = format_exc()
            finally:
                self.queue.task_done()
                task._set(error is not None and error or results)
                error = None
                results = None


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
        self._active = True
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
        self.close()
        self.join()

    @property
    def active(self):
        return self._active

    def _restart_workers(self):
        """Spawn one worker if pool is not full."""
        self._pool = [w for w in self._pool if w.is_alive()]
        missing = self._workers - len(self._pool)
        if missing:
            w = Worker(self._queue, self._limit, self.initializer,
                       self.initargs)
            w.start()
            self._pool.append(w)

    def stop(self):
        """Stops the pool without performing any pending task."""
        self._active = False
        for w in self._pool:
            w.limit = - 1
        for w in self._pool:
            self._queue.put((None, None, None, None))

    def close(self):
        """Close the pool allowing all queued tasks to be performed."""
        self._active = False
        self._queue.join()
        for w in self._pool:
            w.limit = - 1
        for w in self._pool:
            self._queue.put((None, None, None, None))

    def join(self, timeout=0):
        """Joins the pool waiting until all workers exited.

        If *timeout* is greater than 0,
        it block until all workers exited or raise TimeoutError.

        """
        while timeout > 0:
            for w in self._pool[:]:
                w.join(0.1)
                if not w.is_alive():
                    self._pool.remove(w)
                timeout -= 0.1
            if len(self._pool):
                raise TimeoutError('Some of the workers is still running')
            else:
                break
        # if timeout is not set
        for w in self._pool[:]:
            w.join()
            self._pool.remove(w)

    def schedule(self, function, args=(), kwargs={}, callback=None):
        """Schedules *function* into the Pool, passing *args* and *kwargs*
        respectively as arguments and keyword arguments.

        If *callback* is a callable it will be executed once the function
        execution has completed with the returned *Task* as a parameter.

        A *Task* object is returned.

        """
        if not self._active:
            raise RuntimeError('The Pool is not running')
        if not isinstance(function, Callable):
            raise ValueError('function must be callable')
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
    """Used by *thread* decorator."""
    def __init__(self, function, callback):
        self._function = function
        self._counter = count()
        self.callback = callback
        update_wrapper(self, function)

    def __call__(self, *args, **kwargs):
        t = Task(next(self._counter), self.callback)
        q = DummyQueue((self._function, t, args, kwargs))
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
