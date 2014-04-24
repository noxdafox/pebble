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


from time import time
from itertools import count
from threading import Thread
from collections import Callable
from functools import update_wrapper
from traceback import format_exc, print_exc

from ..pebble import Task
from ..pools.thread import ThreadPool


# --------------------------------------------------------------------------- #
#                          Decorator Functions                                #
# --------------------------------------------------------------------------- #
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


# --------------------------------------------------------------------------- #
#                                 Internals                                   #
# --------------------------------------------------------------------------- #
# ----------------------- @thread decorator specific ------------------------ #
def decorator_worker(task):
    """Thread decorator worker.

    Realizes the logic executed by the *thread* decorator.

    """
    error = None
    results = None
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


# ----------------------- Decorated function Wrappers ----------------------- #
class ThreadWrapper(object):
    """Used by *thread* decorator."""
    def __init__(self, function, callback):
        self._function = function
        self._counter = count()
        self.callback = callback
        update_wrapper(self, function)

    def __call__(self, *args, **kwargs):
        task = Task(next(self._counter), self._function, args, kwargs,
                    callback=self.callback)

        worker = Thread(target=decorator_worker, args=(task, ))
        worker.daemon = True
        worker.start()

        return task


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
