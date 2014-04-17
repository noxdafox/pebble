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


import sys

from time import time
from itertools import count
from collections import Callable
from functools import update_wrapper, wraps
from traceback import print_exc
from multiprocessing.connection import Listener
try:  # Python 2
    from cPickle import PicklingError
except:  # Python 3
    from pickle import PicklingError

from .thread import ThreadWorker, ThreadPool
from .process import ProcessWorker, ProcessPool, FAMILY
from .pebble import Task, TimeoutError


def synchronized(lock):
    """Synchronization decorator, locks the execution on given *lock*.

    Works with both threading and multiprocessing Lock.

    """
    def wrap(function):
        @wraps(function)
        def wrapper(*args, **kwargs):
            with lock:
                return function(*args, **kwargs)

        return wrapper

    return wrap


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
        return ThreadPoolWrapper(function, workers, task_limit,
                                 queue, queue_args,
                                 callback, initializer, initargs)

    if len(args) == 1 and not len(kwargs) and isinstance(args[0], Callable):
        return ThreadPoolWrapper(args[0], 1, 0, None, None, None, None, None)
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


class ThreadWrapper(object):
    """Used by *thread* decorator."""
    def __init__(self, function, callback):
        self._function = function
        self._counter = count()
        self.callback = callback
        update_wrapper(self, function)

    def __call__(self, *args, **kwargs):
        t = Task(next(self._counter),
                 self._function, args, kwargs, self.callback, 0, None)
        q = DummyQueue(t)
        w = ThreadWorker(q, 1, None, None, None)
        w.start()
        return t


class ThreadPoolWrapper(object):
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


def process(*args, **kwargs):
    """Turns a *function* into a Process and runs its logic within.

    A decorated *function* will return a *Task* object once is called.

    If *callback* is a callable, it will be called once the task has ended
    with the task identifier and the *function* return values.

    """
    def wrapper(function):
        return ProcessWrapper(function, timeout, callback)

    if len(args) == 1 and not len(kwargs) and isinstance(args[0], Callable):
        return ProcessWrapper(args[0], 0, None)
    elif not len(args) and len(kwargs):
        timeout = kwargs.get('timeout', 0)
        callback = kwargs.get('callback')

        return wrapper
    else:
        raise ValueError("Decorator accepts only keyword arguments.")


def process_pool(*args, **kwargs):
    """Turns a *function* into a ProcessPool and runs its logic within.

    A decorated *function* will return a *Task* object once is called.

    If *callback* is a callable, it will be called once the task has ended
    with the task identifier and the *function* return values.

    """
    def wrapper(function):
        return ProcessPoolWrapper(function, workers, task_limit,
                                  queue, queueargs,
                                  initializer, initargs, callback, timeout)

    if len(args) == 1 and not len(kwargs) and isinstance(args[0], Callable):
        return ProcessPoolWrapper(args[0])
    elif not len(args) and len(kwargs):
        queue = kwargs.get('queue')
        queueargs = kwargs.get('queueargs')
        workers = kwargs.get('workers', 1)
        callback = kwargs.get('callback')
        timeout = kwargs.get('timeout', 0)
        initargs = kwargs.get('initargs')
        initializer = kwargs.get('initializer')
        task_limit = kwargs.get('worker_task_limit', 0)

        return wrapper
    else:
        raise ValueError("Decorator accepts only keyword arguments.")


def trampoline(function, *args, **kwargs):
    """Trampoline function for decorators."""
    func = load_function(function)
    return func(*args, **kwargs)


def dump_function(function):
    """Dumps the decorated function for pickling."""
    try:
        name = function.__name__
        module = function.__module__
        __import__(module)
        mod = sys.modules[module]
        getattr(mod, name)
        return {'name': name, 'module': module}
    except (ImportError, KeyError, AttributeError):
        raise PicklingError(
            "Can't pickle %r: it's not found as %s.%s" %
            (function, module, name))


def load_function(state):
    """Loads the function and extracts it from its decorator."""
    name = state.get('name')
    module = state.get('module')
    __import__(module)
    mod = sys.modules[module]
    decorated = getattr(mod, name)
    return decorated._function


class ProcessWrapper(object):
    def __init__(self, function, timeout, callback):
        self._function = function
        self._counter = count()
        self._connection = None
        self.timeout = timeout
        self.callback = callback
        update_wrapper(self, function)

    @staticmethod
    def task_valid(worker):
        """Check if the current task is not cancelled or timeout."""
        task = None
        timestamp = time()
        current = worker.current
        timeout = lambda c, t: c.timeout and t - c._timestamp > c.timeout

        if timeout(current, timestamp):
            worker.stop()
            task = worker.get_current()
            task._set(TimeoutError('Task timeout'))
        elif current.cancelled:
            worker.stop()
            task = worker.get_current()

        return task

    @thread
    def _handle_job(self, worker):
        task = None

        # wait for task to complete, timeout or to be cancelled
        while task is None:
            if worker.channel.poll(0.2):
                task, results = worker.task_complete()
                task._set(results)
            else:
                task = self.task_valid(worker)
        # run tasks callback
        if task._callback is not None:
            try:
                task._callback(task)
            except:
                print_exc()

        # join the process
        worker.join()

    def __call__(self, *args, **kwargs):
        if self._connection is None:
            self._connection = Listener(family=FAMILY)
        # serialize decorated function
        function = dump_function(self._function)
        # attach decorated function to the arguments
        args = list(args)
        args.insert(0, function)
        t = Task(next(self._counter), trampoline, args, kwargs,
                 self.callback, self.timeout, None)
        w = ProcessWorker(self._connection.address, limit=1)
        w.start()
        w.finalize(self._connection.accept())
        w.schedule_task(t)
        self._handle_job(self, w)

        return t


class ProcessPoolWrapper(object):
    def __init__(self, function, workers=1, task_limit=0,
                 queue=None, queueargs=None,
                 initializer=None, initargs=None, callback=None, timeout=0):
        self._function = function
        self._pool = ProcessPool(workers, task_limit, queue, queueargs,
                                 initializer, initargs)
        self.timeout = timeout
        self.callback = callback
        update_wrapper(self, function)

    def __call__(self, *args, **kwargs):
        # serialize decorated function
        function = dump_function(self._function)
        # attach decorated function to the arguments
        args = list(args)
        args.insert(0, function)
        return self._pool.schedule(trampoline, args=args, kwargs=kwargs,
                                   callback=self.callback,
                                   timeout=self.timeout)
