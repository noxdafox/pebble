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
from traceback import format_exc
from itertools import count
from threading import Thread, current_thread
from collections import Callable
from functools import update_wrapper

from .pebble import TimeoutError, TaskCancelled


def thread_worker(function, task, *args, **kwargs):
    try:
        task._set(function(*args, **kwargs))
    except Exception as error:
        error.traceback = format_exc()
        task._set(error)


def thread(*args, **kwargs):
    """Turns a *function* into a Thread and runs its logic within.

    A decorated *function* will return a *Task* object once is called.

    If *callback* is a callable, it will be called once the task has ended
    with the task identifier and the *function* return values.

    """
    def wrapper(function):
        return Wrapper(function, callback)

    if len(args) == 1 and not len(kwargs) and isinstance(args[0], Callable):
        return Wrapper(args[0], None)
    elif not len(args) and len(kwargs):
        callback = kwargs.get('callback', None)

        return wrapper
    else:
        raise ValueError("Decorator accepts only keyword arguments.")


class Task(object):
    def __init__(self, task_nr, callback):
        self.id = uuid4()
        self.number = task_nr
        self._ready = False
        self._cancelled = False
        self._results = None
        self._worker = None  # set by Asynchronous._wrapper
        self._callback = callback

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
        if self._worker is not current_thread():  # called by main thread
            self._worker.join(timeout)
            if self._worker.is_alive():
                raise TimeoutError("Task is still running")
        if (isinstance(self._results, BaseException)):
            raise self._results
        else:
            return self._results

    def cancel(self):
        """Cancels the Task dropping the results."""
        self._cancelled = True

    def _set(self, results):
        if not self._cancelled:
            self._results = results
            if self._callback is not None:
                self._callback(self)
        else:
            self._results = TaskCancelled("Task has been cancelled")
        self._ready = True


class Wrapper(object):
    def __init__(self, function, callback):
        self._function = function
        self._counter = count()
        self.callback = callback
        update_wrapper(self, function)

    def __call__(self, *args, **kwargs):
        t = Task(next(self._counter), self.callback)
        args = list(args)
        args.insert(0, self._function)
        args.insert(1, t)
        t._worker = Thread(target=thread_worker, args=(args), kwargs=(kwargs))
        t._worker.daemon = True
        t._worker.start()
        return t
