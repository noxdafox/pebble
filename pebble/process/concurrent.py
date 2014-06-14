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

from functools import wraps
from itertools import count
from multiprocessing import Pipe
from traceback import print_exc, format_exc
try:  # Python 2
    from cPickle import PicklingError
except:  # Python 3
    from pickle import PicklingError

from .spawn import spawn as spawn_process
from ..thread import spawn as spawn_thread
from ..pebble import Task, TimeoutError, ProcessExpired
from .generic import dump_function, stop_worker, register_function


_task_counter = count()


def launch(function, timeout, callback, identifier, args, kwargs):
    """Launches the function within a process."""
    reader, writer = Pipe(duplex=False)
    worker = task_worker(writer, function, args, kwargs)
    writer.close()

    task = ProcessTask(next(_task_counter), callback=callback, timeout=timeout,
                       function=function, args=args, kwargs=kwargs,
                       identifier=identifier, worker=worker)
    task_manager(task, reader)

    return task


def wrapped(function, timeout, callback, identifier, args, kwargs):
    """Starts decorated function within a process."""
    if os.name == 'nt':
        function, args = dump_function(function, args)

    return launch(function, timeout, callback, identifier, args, kwargs)


def concurrent(*args, **kwargs):
    """Runs the given function in a concurrent process,
    taking care of the results and error management.

    The *concurrent* function works as well as a decorator.

    *target* is the desired function to be run
    with the given *args* and *kwargs* parameters; if *timeout* is set,
    the process will be stopped once expired returning TimeoutError as results.
    If a *callback* is passed, it will be run after the job has finished with
    the returned *Task* as parameter.

    The *concurrent* function returns a *Task* object.

    .. note:
       The decorator accepts the keywords *timeout* and *callback* only.
       If *target* keyword is not specified, the function will act as
       a decorator.

    """
    timeout = 0
    callback = None
    identifier = None

    if len(args) > 0 and len(kwargs) == 0:  # @task
        function = args[0]
        if os.name == 'nt':
            register_function(function)

        @wraps(function)
        def wrapper(*args, **kwargs):
            return wrapped(function, timeout, callback, identifier,
                           args, kwargs)

        return wrapper
    elif len(kwargs) > 0 and len(args) == 0:  # task() or @task()
        timeout = kwargs.pop('timeout', 0)
        callback = kwargs.pop('callback', None)
        identifier = kwargs.pop('identifier', None)
        target = kwargs.pop('target', None)
        args = kwargs.pop('args', [])
        kwargs = kwargs.pop('kwargs', {})

        if target is not None:
            return launch(target, timeout, callback, identifier,
                          args, kwargs)

        def wrap(function):
            if os.name == 'nt':
                register_function(function)

            @wraps(function)
            def wrapper(*args, **kwargs):
                return wrapped(function, timeout, callback, identifier,
                               args, kwargs)

            return wrapper

        return wrap
    else:
        raise ValueError("Decorator accepts only keyword arguments.")


@spawn_process(daemon=True)
def task_worker(writer, function, args, kwargs):
    """Runs the actual function in separate process."""
    error = None
    results = None

    try:
        results = function(*args, **kwargs)
    except (IOError, OSError):
        return 0
    except Exception as err:
        error = err
        error.traceback = format_exc()
    finally:
        try:
            writer.send(error is not None and error or results)
        except (IOError, OSError, EOFError):
            return 0
        except PicklingError as err:
            error = err
            error.traceback = format_exc()
            writer.send.put(error)

    return 0


@spawn_thread(daemon=True)
def task_manager(task, reader):
    """Task's lifecycle manager.

    Starts a new worker, waits for the *Task* to be performed,
    collects results, runs the callback and cleans up the process.

    """
    worker = task._worker
    timeout = task.timeout > 0 and task.timeout or None

    try:
        if reader.poll(timeout):
            results = reader.recv()
        else:
            results = TimeoutError('Task Timeout', timeout)
    except (IOError, OSError, EOFError):
        results = ProcessExpired('Abnormal termination')

    try:
        worker.terminate()
    except:
        pass
    finally:
        worker.join()
    if isinstance(results, ProcessExpired):
        results.exitcode = worker.exitcode

    task._set(results)
    if task._callback is not None:
        try:
            task._callback(task)
        except Exception:
            print_exc()


class ProcessTask(Task):
    """Extends the *Task* object to support *process* decorator."""
    def __init__(self, task_nr, function=None, args=None, kwargs=None,
                 callback=None, timeout=0, identifier=None, worker=None):
        super(ProcessTask, self).__init__(task_nr, callback=callback,
                                          function=function, args=args,
                                          kwargs=kwargs, timeout=timeout,
                                          identifier=identifier)
        self._worker = worker

    def _cancel(self):
        """Overrides the *Task* cancel method in order to signal it
        to the *process* decorator handler."""
        super(ProcessTask, self)._cancel()
        stop_worker(self._worker)
