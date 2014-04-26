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
from collections import Callable
from functools import update_wrapper
from traceback import print_exc, format_exc
from multiprocessing import Process, Pipe, Event
try:  # Python 2
    from cPickle import PicklingError
except:  # Python 3
    from pickle import PicklingError

from ..pebble import Task, TimeoutError, TaskCancelled
from ..pools.process import ProcessPool
from ..decorators.thread import thread


# --------------------------------------------------------------------------- #
#                          Decorator Functions                                #
# --------------------------------------------------------------------------- #
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


# --------------------------------------------------------------------------- #
#                                 Internals                                   #
# --------------------------------------------------------------------------- #
# -------------------- Deal with decoration and pickling -------------------- #
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


# ----------------------- @process decorator specific ----------------------- #
def decorator_worker(channel, event, function, args, kwargs):
    """Process decorator worker.

    Realizes the logic executed by the *process* decorator.
     * channel: writer side of the Pipe
     * event: Event signalling the worker ended
     * function: the decorated function
     * args, kwargs: arguments of the decorated function

    """
    error = None
    results = None

    try:
        if sys.platform != 'win32':
            results = function(*args, **kwargs)
        else:
            results = trampoline(function, *args, **kwargs)
    except (IOError, OSError):
        sys.exit(1)
    except Exception as err:
        error = err
        error.traceback = format_exc()
    finally:
        try:
            channel.send(error is not None and error or results)
        except (IOError, OSError, EOFError):
            sys.exit(1)
        except PicklingError as err:
            error = err
            error.traceback = format_exc()
            channel.send(error)

        event.set()


class ProcessDecoratorTask(Task):
    """Expands the *Task* object to support *process* decorator."""
    def __init__(self, task_nr, callback=None, timeout=0, identifier=None,
                 event=None):
        super(ProcessDecoratorTask, self).__init__(task_nr, callback=callback,
                                                   timeout=timeout,
                                                   identifier=identifier)
        self._event = event

    def _cancel(self):
        """Overrides the *Task* cancel method in order to signal it
        to the *process* decorator handler."""
        self._cancelled = True
        self._event.set()


# ----------------------- Decorated function Wrappers ----------------------- #
class ProcessWrapper(object):
    """Used by *process* decorator."""
    def __init__(self, function, timeout, callback):
        self._counter = count()
        self._function = function
        self.timeout = timeout
        self.callback = callback
        update_wrapper(self, function)

    @staticmethod
    def _spawn_worker(function, writer, event, *args, **kwargs):
        """Spawns a new worker and returns it."""
        args = [function, writer, event] + list(args)

        worker = Process(target=decorator_worker, args=args, kwargs=kwargs)
        worker.daemon = True
        worker.start()

        return worker

    @staticmethod
    def _wait_for_task(task, channel, event, timeout):
        """Waits for task to complete, timeout or be cancelled.

        Blocks until one of the events happen.
        Returns the task.

        """
        results = None

        if event.wait(timeout=timeout):
            if not task.cancelled:
                results = channel.recv()
            else:
                results = TaskCancelled('Task cancelled')
        else:
            results = TimeoutError('Task timeout')

        return results

    @thread
    def _handle_worker(self, task, worker, channel, event):
        """Handles the lifecycle of a single worker.

        Waits for task to be completed, cleans up the process
        and runs the callback.

        """
        timeout = self.timeout > 0 and self.timeout or None

        results = self._wait_for_task(task, channel, event, timeout)
        task._set(results)

        if worker.is_alive():
            worker.terminate()
        worker.join()

        if task._callback is not None:
            try:
                task._callback(task)
            except:
                print_exc()

    def __call__(self, *args, **kwargs):
        event = Event()
        reader, writer = Pipe(duplex=False)

        if sys.platform != 'win32':
            function = self._function
        else:
            function = trampoline
            args = [dump_function(self._function)] + list(args)

        worker = self._spawn_worker(writer, event, function, args, kwargs)

        task = ProcessDecoratorTask(next(self._counter),
                                    callback=self.callback,
                                    timeout=self.timeout, event=event)

        self._handle_worker(self, task, worker, reader, event)

        return task


class ProcessPoolWrapper(object):
    """Used by *process_pool* decorator."""
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
        args = [dump_function(self._function)] + list(args)

        return self._pool.schedule(trampoline, args=args, kwargs=kwargs,
                                   callback=self.callback,
                                   timeout=self.timeout)
