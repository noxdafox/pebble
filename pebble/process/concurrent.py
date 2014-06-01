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

import os
import sys

from functools import wraps
from itertools import count
from traceback import print_exc, format_exc
try:  # Python 2
    from Queue import Empty
    from cPickle import PicklingError
except:  # Python 3
    from queue import Empty
    from pickle import PicklingError

from .generic import SimpleQueue, dump_function
from ..thread import concurrent as thread_worker
from .concurrent import concurrent as process_worker
from ..pebble import Task, TimeoutError, TaskCancelled


_task_counter = count()


def spawn(function, timeout, callback, args, kwargs):
    """Launches the function within a process."""
    queue = SimpleQueue()
    task = ProcessTask(next(_task_counter),
                       function=function, args=args, kwargs=kwargs,
                       callback=callback, timeout=timeout, queue=queue)
    task_manager(task)

    return task


def wrapped(function, timeout, callback, args, kwargs):
    """Starts decorated function within a process."""
    if os.name == 'nt':
        function, args = dump_function(function, args)

    return spawn(function, timeout, callback, args, kwargs)


def task(*args, **kwargs):
    """Runs the given function in a concurrent process,
    taking care of the results and error management.

    The task function works as well as a decorator.

    *target* is the desired function to be run
    with the given *args* and *kwargs* parameters; if *timeout* is set,
    the process will be stopped once expired returning TimeoutError as results.
    If a *callback* is passed, it will be run after the job has finished with
    the returned *Task* as parameter.

    The task function returns a *Task* object.

    .. note:
       The decorator accepts the keywords *timeout* and *callback* only.
       If *target* keyword is not specified, the function will act as
       a decorator.

    """
    timeout = 0
    callback = None

    if len(args) > 0 and len(kwargs) == 0:  # @task
        function = args[0]

        @wraps(function)
        def wrapper(*args, **kwargs):
            return wrapped(function, timeout, callback, args, kwargs)

        return wrapper
    elif len(kwargs) > 0 and len(args) == 0:  # task() or @task()
        timeout = kwargs.pop('timeout', 0)
        callback = kwargs.pop('callback', None)
        target = kwargs.pop('target', None)
        args = kwargs.pop('args', [])
        kwargs = kwargs.pop('kwargs', {})

        if target is not None:
            return spawn(target, timeout, callback, args, kwargs)

        def wrap(function):

            @wraps(function)
            def wrapper(*args, **kwargs):
                return wrapped(function, timeout, callback, args, kwargs)

            return wrapper

        return wrap
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


@thread_worker(daemon=True)
def task_manager(task):
    """Task's lifecycle manager.

    Starts a new worker, waits for the *Task* to be performed,
    collects results, runs the callback and cleans up the process.

    """
    queue = task._queue
    function = task._function
    timeout = task.timeout > 0 and task.timeout or None

    process = task_worker(queue, function, task._args, task._kwargs)

    try:
        results = queue.get(timeout)
        task._set(results)
    except Empty:
        task._set(TimeoutError('Task Timeout', timeout))

    process.terminate()
    process.join()

    if task._callback is not None:
        try:
            task._callback(task)
        except Exception:
            print_exc()


class ProcessTask(Task):
    """Extends the *Task* object to support *process* decorator."""
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
