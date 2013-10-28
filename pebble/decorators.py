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


from itertools import count
from threading import Thread
from collections import Callable
from functools import update_wrapper
from multiprocessing import Process, Pipe

from .pebble import ThreadTask, ProcessTask, thread_worker, process_worker


class AsynchronousWrapper(object):
    def __init__(self, function, callback, error_callback):
        self._function = function
        self._counter = count()
        self.callback = callback
        self.error_callback = error_callback
        update_wrapper(self, function)

    def __call__(self, *args, **kwargs):
        t = ThreadTask(next(self._counter),
                       self.callback, self.error_callback)
        args = list(args)
        args.insert(0, self._function)
        args.insert(1, t)
        t._worker = Thread(target=thread_worker, args=(args), kwargs=(kwargs))
        t._worker.daemon = True
        t._worker.start()
        return t


def asynchronous(*args, **kwargs):
    """Turns a *function* into a Thread and runs its logic within.

    A decorated *function* will return a *Task* object once is called.

    If *callback* is a callable, it will be called once the task has ended
    with the task identifier and the *function* return values.
    If *error_callback* is a callable, it will be called if the task has raised
    an exception, passing the task identifier and the raised exception.

    """
    def wrapper(function):
        return AsynchronousWrapper(function, callback, error_callback)

    if len(args) == 1 and not len(kwargs) and isinstance(args[0], Callable):
        return AsynchronousWrapper(args[0], None, None)
    elif not len(args) and len(kwargs):
        callback = kwargs.get('callback', None)
        error_callback = kwargs.get('error_callback', None)

        return wrapper
    else:
        raise ValueError("Decorator accepts only keyword arguments.")


class ConcurrentWrapper(object):
    def __init__(self, function, timeout, callback, error_callback):
        self._function = function
        self._counter = count()
        self.timeout = timeout
        self.callback = callback
        self.error_callback = error_callback
        update_wrapper(self, function)

    def __call__(self, *args, **kwargs):
        reader, writer = Pipe(duplex=False)
        args = list(args)
        args.insert(0, self._function)
        args.insert(1, writer)
        p = Process(target=process_worker, args=(args), kwargs=(kwargs))
        p.daemon = True
        p.start()
        return ProcessTask(next(self._counter), p, reader,
                           self.callback, self.error_callback, self.timeout)


def concurrent(*args, **kwargs):
    """Turns a *function* into a Process and runs its logic within.

    A decorated *function* will return a *Task* object once is called.

    If *callback* is a callable, it will be called once the task has ended
    with the task identifier and the *function* return values.
    If *error_callback* is a callable, it will be called if the task has raised
    an exception, passing the task identifier and the raised exception.

    """
    def wrapper(function):
        return ConcurrentWrapper(function, timeout, callback, error_callback)

    if len(args) == 1 and not len(kwargs) and isinstance(args[0], Callable):
        return ConcurrentWrapper(args[0], None, None, None)
    elif not len(args) and len(kwargs):
        timeout = kwargs.get('timeout', None)
        callback = kwargs.get('callback', None)
        error_callback = kwargs.get('error_callback', None)

        return wrapper
    else:
        raise ValueError("Decorator accepts only keyword arguments.")
