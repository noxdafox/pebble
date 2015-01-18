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

# Common utility functions

from time import sleep
from inspect import isclass
from itertools import count
from traceback import format_exc, print_exc

try:  # Python 2
    from Queue import Queue
except:  # Python 3
    from queue import Queue

from .exceptions import PoolError, TimeoutError


# Pool states
STOPPED = 0
RUNNING = 1
CLOSED = 2
CREATED = 3
EXPIRED = 4
ERROR = 5


def function_handler(launcher, decorator, *args, **kwargs):
    """Distinguishes between function and decorator usage of spawn and
    concurrent functions.

    """
    if isfunction(args, kwargs):
        return launcher(kwargs.pop('target', None), **kwargs)
    elif issimpledecorator(args, kwargs):
        return decorator(args[0], launcher)
    elif isparametrizeddecorator(args, kwargs):
        def wrap(function):
            return decorator(function, launcher, **kwargs)

        return wrap
    else:
        raise ValueError("Only keyword arguments are accepted.")


def isfunction(args, kwargs):
    """spawn or concurrent used as regular function."""
    if not args and kwargs and 'target' in kwargs:
        return True
    else:
        return False


def issimpledecorator(args, kwargs):
    """spawn or concurrent used as decorator with no parameters."""
    if args and not kwargs:
        return True
    else:
        return False


def isparametrizeddecorator(args, kwargs):
    """spawn or concurrent used as decorator with parameters."""
    if not args and kwargs:
        return True
    else:
        return False


def execute(function, args, kwargs):
    """Runs the given function returning its results or exception."""
    try:
        return function(*args, **kwargs)
    except Exception as error:
        error.traceback = format_exc()
        return error


class PoolContext(object):
    """Pool's Context.

    Wraps the Pool's state.

    """
    def __init__(self, queue, queueargs, initializer, initargs,
                 workers, limit):
        self.state = CREATED
        self.pool = {}  # {tid/pid: Thread/Process}
        self.tasks = {}  # {Task.number: Task}
        self.managers = None  # threads managing the Pool
        self.initializer = initializer
        self.initargs = initargs
        self.worker_number = workers
        self.worker_limit = limit
        if queue is not None:
            if isclass(queue):
                self.queue = queue(*queueargs)
            else:
                raise ValueError("Queue must be Class")
        else:
            self.queue = Queue()

    def join(self, timeout):
        """Joins pool's workers."""
        while len(self.pool) > 0 and (timeout is None or timeout > 0):
            for identifier, worker in list(self.pool.items()):
                worker.join(timeout is not None and 0.1 or None)
                if not worker.is_alive():
                    self.pool.pop(identifier)

            if timeout is not None:
                timeout = timeout - (len(self.pool) / 10.0)

        if len(self.pool) > 0:
            raise TimeoutError('Workers are still running')

    def task_done(self, task, results):
        task._set(results)

        if task._callback is not None:
            try:
                task._callback(task)
            except Exception:
                print_exc()
                self.state = ERROR

        self.queue.task_done()


class BasePool(object):
    def __init__(self):
        self._counter = count()
        self._context = None
        self._managers = ()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
        self.join()

    @property
    def active(self):
        return self._context.state == RUNNING and True or False

    def _state(self):
        """Updates Pool's state."""
        if self._context.state == CREATED:
            self._start()
        else:
            for manager in self._managers:
                if not manager.is_alive():
                    self._context.state = ERROR

    def _schedule(self, task):
        """Schedules *Task* into the Pool."""
        self._state()

        if self._context.state == ERROR:
            raise PoolError('Unexpected error within the Pool')
        elif self._context.state != RUNNING:
            raise RuntimeError('The Pool is not running')

        self._context.queue.put(task)

        return task

    def close(self):
        """Closes the pool.

        No new tasks will be accepted, enqueued ones will be performed.

        """
        self._context.state = CLOSED

    def join(self, timeout=None):
        """Joins the pool waiting until all workers exited.

        If *timeout* is set, it block until all workers are done
        or raise TimeoutError.

        """
        if self._context.state == RUNNING:
            raise RuntimeError('The Pool is still running')
        elif self._context.state == CLOSED:
            queue = self._context.queue

            if timeout is not None:
                while queue.unfinished_tasks > 0 and timeout > 0:
                    sleep(0.1)
                    timeout -= 0.1
            else:
                queue.join()
            self.stop()

        self._context.join(timeout)
