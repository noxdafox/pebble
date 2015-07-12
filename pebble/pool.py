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

import time

from inspect import isclass
from itertools import count
from traceback import print_exc
from collections import namedtuple

try:
    from queue import Queue
except ImportError:
    from Queue import Queue

from .task import Task
from .exceptions import PoolError, TimeoutError


SLEEP_UNIT = 0.1


# Pool states
STOPPED = 0
RUNNING = 1
CLOSED = 2
CREATED = 3
EXPIRED = 4
ERROR = 5


WorkerParameters = namedtuple('WorkerParameters', ('task_limit',
                                                   'initializer',
                                                   'initargs'))


class BasePool(object):
    def __init__(self, workers, task_limit, queue_factory,
                 initializer, initargs):
        self._context = PoolContext(workers, task_limit, queue_factory,
                                    initializer, initargs)
        self._loops = ()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
        self.join()

    @property
    def active(self):
        return self._context.state == RUNNING

    def close(self):
        """Closes the Pool preventing new tasks from being accepted.

        Pending tasks will be completed.
        """
        self._context.state = CLOSED

    def stop(self):
        """Stops the pool without performing any pending task."""
        self._context.state = STOPPED

    def join(self, timeout=None):
        """Joins the pool waiting until all workers exited.

        If *timeout* is set, it block until all workers are done
        or raises TimeoutError.
        """
        if self._context.state == RUNNING:
            raise RuntimeError('The Pool is still running')
        if self._context.state == CLOSED:
            wait_queue_depletion(self._context.task_queue, timeout)
            self.stop()
            self.join()
        else:
            for loop in self._loops:
                loop.join()
            self._stop_pool()

    def schedule(self, function, args=(), kwargs={}, identifier=None,
                 callback=None, timeout=0):
        """Schedules *function* to be run the Pool.

        *args* and *kwargs* will be forwareded to the scheduled function
        respectively as arguments and keyword arguments.

        If *callback* is a callable it will be executed once the function
        execution has completed with the returned *Task* as a parameter.

        *timeout* is an integer, if expires the task will be terminated
        and *Task.get()* will raise *TimeoutError*.

        The *identifier* value will be forwarded to the *Task.id* attribute.

        A *Task* object is returned.
        """
        metadata = (function, args, kwargs)
        return self._schedule_task(callback, timeout, identifier, metadata)

    def _schedule_task(self, callback, timeout, identifier, metadata):
        self._check_pool_state()

        task = Task(next(self._context.task_counter), callback=callback,
                    timeout=timeout, identifier=identifier, metadata=metadata)
        self._context.task_queue.put(task)

        return task

    def _check_pool_state(self):
        self._update_pool_state()

        if self._context.state == ERROR:
            raise PoolError('Unexpected error within the Pool')
        elif self._context.state != RUNNING:
            raise RuntimeError('The Pool is not running')

    def _update_pool_state(self):
        if self._context.state == CREATED:
            self._start_pool()
        else:
            for loop in self._loops:
                if not loop.is_alive():
                    self._context.state = ERROR

    def _start_pool(self):
        raise NotImplementedError("Not implemented")

    def _stop_pool(self):
        raise NotImplementedError("Not implemented")


class PoolContext(object):
    def __init__(self, workers, task_limit, queue_factory,
                 initializer, initargs):
        self.state = CREATED
        self.workers = workers
        self.task_counter = count()
        self.task_queue = create_queue(queue_factory)
        self.worker_parameters = WorkerParameters(task_limit,
                                                  initializer, initargs)

    @property
    def alive(self):
        return self.state not in (ERROR, STOPPED)


def wait_queue_depletion(queue, timeout):
    if timeout is not None:
        wait_queue_timeout(queue, timeout)
    else:
        queue.join()


def wait_queue_timeout(queue, timeout):
    for _ in range(int(timeout / SLEEP_UNIT)):
        if queue.unfinished_tasks:
            time.sleep(SLEEP_UNIT)
        else:
            break
    else:
        raise TimeoutError("Tasks are still being executed")


def create_queue(queue_factory):
    if queue_factory is not None:
        return queue_factory()
    else:
        return Queue()


def run_initializer(initializer, initargs):
    try:
        initializer(*initargs)
        return True
    except Exception:
        print_exc()
        return False


def task_limit_reached(counter, task_limit):
    return task_limit > 0 and next(counter) >= task_limit
