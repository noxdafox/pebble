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


from uuid import uuid4
from inspect import isclass
from itertools import count
from threading import Thread, Condition, Lock, Event
try:  # Python 2
    from Queue import Queue
except:  # Python 3
    from queue import Queue


# Pool states
STOPPED = 0
RUNNING = 1
CLOSING = 2
CREATED = 3


class PebbleError(Exception):
    """Pebble base exception."""
    pass


class TimeoutError(PebbleError):
    """Raised when Task.get() timeout expires."""
    def __init__(self, msg):
        self.msg = msg

    def __repr__(self):
        return "%s %s" % (self.__class__, self.msg)

    def __str__(self):
        return str(self.msg)


class TaskCancelled(PebbleError):
    """Raised if get is called on a cancelled task."""
    def __init__(self, msg):
        self.msg = msg

    def __repr__(self):
        return "%s %s" % (self.__class__, self.msg)

    def __str__(self):
        return str(self.msg)


class Task(object):
    """Handler to the ongoing task."""
    def __init__(self, task_nr, function, args, kwargs,
                 callback, timeout, identifier):
        self.id = identifier is not None and identifier or uuid4()
        self.timeout = timeout
        self._function = function
        self._args = args
        self._kwargs = kwargs
        self._number = task_nr
        self._ready = False
        self._cancelled = False
        self._results = None
        self._task_ready = Condition(Lock())
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


class PoolContext(object):
    """Container for the Pool state.

    Wraps all the variables needed to represent a Pool.

    """
    def __init__(self, workers, limit, queue, queueargs,
                 initializer, initargs):
        self.state = CREATED
        self.workers = workers
        self.pool = []
        self.limit = limit
        self.task_counter = count()
        self.workers_event = Event()
        self.initializer = initializer
        self.initargs = initargs
        if queue is not None:
            if isclass(queue):
                self.queue = queue(*queueargs)
            else:
                raise ValueError("Queue must be Class")
        else:
            self.queue = Queue()

    @property
    def counter(self):
        """Tasks counter."""
        return next(self.task_counter)


class PoolManager(Thread):
    """Pool management routine.

    Respawns missing workers.
    Collects expired ones and cleans them up.

    """
    def __init__(self, context):
        Thread.__init__(self)
        self.daemon = True
        self.context = context

    def wait_for_worker(self, timeout):
        """Waits for expired workers and restarts them."""
        self.context.workers_event.wait(timeout)
        self.context.workers_event.clear()

        return [w for w in self.context.pool if w.expired]

    def cleanup_workers(self, expired):
        raise NotImplementedError('Not implemented')

    def spawn_workers(self):
        """Spawns missing Workers."""
        raise NotImplementedError('Not implemented')

    def run(self):
        while self.context.state != STOPPED:
            self.spawn_workers()
            expired_workers = self.wait_for_worker(0.8)
            self.cleanup_workers(expired_workers)
