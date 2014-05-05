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

from itertools import count
from types import MethodType
from collections import Callable
from functools import update_wrapper
from multiprocessing import Queue
from traceback import print_exc, format_exc
try:  # Python 2
    from Queue import Empty
    from cPickle import PicklingError
except:  # Python 3
    from queue import Empty
    from pickle import PicklingError

from .worker import worker as process_worker
#from ..thread import worker as thread_worker
from ..pebble import Task, TimeoutError, TaskCancelled
from .generic import trampoline, dump_function, dump_method


def task(*args, **kwargs):
    """Turns a *function* into a Process and runs its logic within.

    A decorated *function* will return a *Task* object once is called.

    If *callback* is a callable, it will be called once the task has ended
    with the task identifier and the *function* return values.

    """
    def wrapper(function):
        return TaskDecoratorWrapper(function, timeout, callback)

    if len(args) == 1 and not len(kwargs) and isinstance(args[0], Callable):
        return TaskDecoratorWrapper(args[0], 0, None)
    elif not len(args) and len(kwargs):
        timeout = kwargs.get('timeout', 0)
        callback = kwargs.get('callback')

        return wrapper
    else:
        raise ValueError("Decorator accepts only keyword arguments.")


@process_worker(daemon=True)
def task_worker(queue, function, args, kwargs):
    """Runs the actual function in separate process."""
    error = None
    results = None

    try:
        results = function(*args, **kwargs)
    except (IOError, OSError):
        sys.exit(1)
    except Exception as err:
        error = err
        error.traceback = format_exc()
    finally:
        try:
            queue.put(error is not None and error or results)
        except (IOError, OSError, EOFError):
            sys.exit(1)
        except PicklingError as err:
            error = err
            error.traceback = format_exc()
            queue.put(error)


#@thread_worker(daemon=True)
def task_lifecycle(task, ismethod):
    """Starts a new worker, waits for the *Task* to be performed,
    collects results, runs the callback and cleans up the process.

    """
    args = task._args
    queue = task._queue
    function = task._function
    timeout = task.timeout > 0 and task.timeout or None

    if sys.platform == 'win32':
        if ismethod:
            args = [dump_method(function, args[0])] + list(args)
        else:
            args = [dump_function(function)] + list(args)

        function = trampoline

    process = task_worker(queue, function, task._args, task._kwargs)

    try:
        results = queue.get(timeout)
        task._set(results)
    except Empty:
        process.terminate()
        task._set(TimeoutError('Task Timeout'))

    if task._callback is not None:
        try:
            task._callback(task)
        except Exception:
            print_exc()

    process.join()


class ProcessTask(Task):
    """Expands the *Task* object to support *process* decorator."""
    def __init__(self, task_nr, function=None, args=None, kwargs=None,
                 callback=None, timeout=0, identifier=None, queue=None):
        super(ProcessTask, self).__init__(task_nr, callback=callback,
                                          function=function, args=args,
                                          kwargs=kwargs, timeout=timeout,
                                          identifier=identifier)
        self._queue = queue

    def _cancel(self):
        """Overrides the *Task* cancel method in order to signal it
        to the *process* decorator handler."""
        self._cancelled = True
        self._queue.put(TaskCancelled('Task Cancelled'))


class TaskDecoratorWrapper(object):
    """Used by *task* decorator."""
    def __init__(self, function, timeout, callback):
        self._counter = count()
        self._function = function
        self._ismethod = False
        self.timeout = timeout
        self.callback = callback
        update_wrapper(self, function)

    def __get__(self, instance, owner=None):
        """Turns the decorator into a descriptor
        in order to use it with methods."""
        self._ismethod = True
        if instance is None:
            return self
        return MethodType(self, instance)

    def __call__(self, *args, **kwargs):
        queue = Queue()

        task = ProcessTask(next(self._counter),
                           function=self._function, args=args, kwargs=kwargs,
                           callback=self.callback, timeout=self.timeout,
                           queue=queue)

        task_lifecycle(task, self._ismethod)

        return task
