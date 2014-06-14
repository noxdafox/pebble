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

"""Container for generic objects."""


import signal
import threading

from functools import wraps
from types import MethodType


# Pool states
STOPPED = 0
RUNNING = 1
CLOSED = 2
CREATED = 3
EXPIRED = 4
ERROR = 5


# --------------------------------------------------------------------------- #
#                                 Exceptions                                  #
# --------------------------------------------------------------------------- #
class PebbleError(Exception):
    """Pebble base exception."""
    pass


class TaskCancelled(PebbleError):
    """Raised if get is called on a cancelled task."""
    pass


class TimeoutError(PebbleError):
    """Raised when a timeout expires."""
    def __init__(self, msg, value=0):
        super(TimeoutError, self).__init__(msg)
        self.timeout = value


class ProcessExpired(PebbleError):
    """Raised when process dies unexpectedly."""
    def __init__(self, msg, code=0):
        super(ProcessExpired, self).__init__(msg)
        self.exitcode = code


# --------------------------------------------------------------------------- #
#                                 Decorators                                  #
# --------------------------------------------------------------------------- #
def synchronized(lock):
    """Locks the execution of decorated function on given *lock*.

    Works with both threading and multiprocessing Lock.

    """
    def wrap(function):
        @wraps(function)
        def wrapper(*args, **kwargs):
            with lock:
                return function(*args, **kwargs)

        return wrapper

    return wrap


def sighandler(signals):
    """Sets the decorated function as signal handler of given *signals*.

    *signals* can be either a single signal or a list/tuple
    of multiple ones.

    """
    def wrap(function):
        if isinstance(signals, (list, tuple)):
            for signum in signals:
                signal.signal(signum, function)
        else:
            signal.signal(signals, function)

        @wraps(function)
        def wrapper(*args, **kwargs):
            return function(*args, **kwargs)

        return wrapper

    return wrap


# --------------------------------------------------------------------------- #
#                               Common Functions                              #
# --------------------------------------------------------------------------- #
def new(self, *args):
    self._old(*args)
    with self._external_lock:
        self._external_lock.notify_all()


def waitfortasks(tasks, timeout=None):
    """Waits for one or more *Task* to be ready or until *timeout* expires.

    *tasks* is a list containing one or more *pebble.Task* objects.
    If *timeout* is not None the function will block
    for the specified amount of seconds.

    The function returns a list containing the ready *Tasks*.

    """
    block = threading.Condition(threading.Lock())
    ready = lambda: [t for t in tasks if t.ready]

    for task in tasks:
        task._external_lock = block
        with task._task_ready:
            task._old = task._set
            task._set = MethodType(new, task)

    with block:
        if len(ready()) == 0:
            block.wait(timeout)

    for task in tasks:
        with task._task_ready:
            task._set = task._old
            delattr(task, '_old')
            delattr(task, '_external_lock')

    return ready()


def waitforthreads(threads, timeout=None):
    """Waits for one or more *Thread* to exit or until *timeout* expires.

    .. note::

       Expired *Threads* are not joined by *waitforthreads*.

    *threads* is a list containing one or more *threading.Thread* objects.
    If *timeout* is not None the function will block
    for the specified amount of seconds.

    The function returns a list containing the ready *Threads*.

    """
    block = threading.Condition(threading.Lock())
    ready = lambda: [t for t in threads if not t.is_alive()]

    def new(*args):
        old(*args)
        with block:
            block.notify_all()

    if hasattr(threading, 'get_ident'):
        old = threading.get_ident
        threading.get_ident = new
    else:
        old = threading._get_ident
        threading._get_ident = new

    with block:
        while len(ready()) == 0:
            if not block.wait(timeout):
                break

    if hasattr(threading, 'get_ident'):
        threading.get_ident = old
    else:
        threading._get_ident = old

    return ready()


def waitforqueues(queues, timeout=None):
    """Waits for one or more *Queue* to be ready or until *timeout* expires.

    *queues* is a list containing one or more *Queue.Queue* objects.
    If *timeout* is not None the function will block
    for the specified amount of seconds.

    The function returns a list containing the ready *Queues*.

    """
    block = threading.Condition(threading.Lock())
    ready = lambda: [q for q in queues if not q.empty()]

    for queue in queues:
        queue._external_lock = block
        with queue.mutex:
            queue._old = queue.put
            queue.put = MethodType(new, queue)

    with block:
        if len(ready()) == 0:
            block.wait(timeout)

    for queue in queues:
        with queue.mutex:
            queue.put = queue._old
        delattr(queue, '_old')
        delattr(queue, '_external_lock')

    return ready()


# --------------------------------------------------------------------------- #
#                               Common Objects                                #
# --------------------------------------------------------------------------- #
class Task(object):
    """Handler to the ongoing task."""
    def __init__(self, task_nr, function=None, args=None, kwargs=None,
                 callback=None, timeout=0, identifier=None):
        self.id = identifier
        self.timeout = timeout
        self._function = function
        self._args = args
        self._kwargs = kwargs
        self._number = task_nr
        self._ready = False
        self._cancelled = False
        self._results = None
        self._task_ready = threading.Condition(threading.Lock())
        self._timestamp = 0
        self._callback = callback

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return "%s (Task-%d, %s)" % (self.__class__, self.number, self.id)

    @property
    def number(self):
        return self._number

    @property
    def ready(self):
        return self._ready

    @property
    def cancelled(self):
        return self._cancelled

    @property
    def started(self):
        return self._timestamp > 0 and True or False

    @property
    def success(self):
        return (self._ready and not
                isinstance(self._results, BaseException) or False)

    def wait(self, timeout=None):
        """Waits until results are ready.

        If *timeout* is set the call will block until the timeout expires.

        Returns *True* if results are ready, *False* if timeout expired.

        """
        with self._task_ready:
            if not self._ready:
                self._task_ready.wait(timeout)
            return self._ready

    def get(self, timeout=None, cancel=False):
        """Retrieves the produced results, blocks until results are ready.

        If the executed code raised an error it will be re-raised.

        If *timeout* is set the call will block until the timeout expires
        raising *TimeoutError" if results are not yet available.
        If *cancel* is True while *timeout* is set *Task* will be cancelled
        once the timeout expires.

        """
        with self._task_ready:
            if not self._ready:  # block if not ready
                self._task_ready.wait(timeout)
            if self._ready:  # return results
                if (isinstance(self._results, BaseException)):
                    raise self._results
                else:
                    return self._results
            else:  # get timeout
                if cancel:
                    self._cancel()
                raise TimeoutError("Task is still running")

    def cancel(self):
        """Cancels the Task."""
        with self._task_ready:
            self._cancel()

    def _cancel(self):
        """Cancels the Task."""
        self._results = TaskCancelled("Task cancelled")
        self._ready = self._cancelled = True
        self._task_ready.notify_all()

    def _set(self, results):
        """Sets the results within the task."""
        with self._task_ready:
            if not self._ready:
                self._ready = True
                self._results = results
                self._task_ready.notify_all()
