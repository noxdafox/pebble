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
from types import MethodType
from collections import Callable
from functools import update_wrapper
from traceback import print_exc, format_exc

from .concurrent import concurrent
from ..pebble import Task


def task(*args, **kwargs):
    """Turns a *function* into a Process and runs its logic within.

    A decorated *function* will return a *Task* object once is called.

    If *callback* is a callable, it will be called once the task has ended
    with the task identifier and the *function* return values.

    """
    def wrapper(function):
        return TaskDecoratorWrapper(function, callback)

    if len(args) == 1 and not len(kwargs) and isinstance(args[0], Callable):
        return TaskDecoratorWrapper(args[0], None)
    elif not len(args) and len(kwargs):
        callback = kwargs.get('callback')

        return wrapper
    else:
        raise ValueError("Decorator accepts only keyword arguments.")


@concurrent(daemon=True)
def task_worker(task):
    """Runs the actual function in separate thread."""
    error = None
    results = None
    function = task._function
    args = task._args
    kwargs = task._kwargs

    try:
        results = function(*args, **kwargs)
    except Exception as err:
        error = err
        error.traceback = format_exc()
    finally:
        task._set(error is not None and error or results)
        if task._callback is not None:
            try:
                task._callback(task)
            except Exception:
                print_exc()


class TaskDecoratorWrapper(object):
    """Used by *task* decorator."""
    def __init__(self, function, callback):
        self._counter = count()
        self._function = function
        self._ismethod = False
        self.callback = callback
        update_wrapper(self, function)

    def __get__(self, instance, owner=None):
        """Turns the decorator into a descriptor
        in order to use it with methods."""
        if instance is None:
            return self
        return MethodType(self, instance)

    def __call__(self, *args, **kwargs):
        task = Task(next(self._counter), callback=self.callback,
                    function=self._function, args=args, kwargs=kwargs)

        task_worker(task)

        return task
