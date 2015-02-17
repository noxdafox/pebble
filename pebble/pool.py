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

try:  # Python 2
    from Queue import Queue
except ImportError:  # Python 3
    from queue import Queue

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


class BasePool(object):
    def __init__(self, queue, queueargs):
        self._managers = ()
        self._context = PoolContext(queue, queueargs)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
        self.join()

    @property
    def active(self):
        return self._context.state == RUNNING

    def close(self):
        self._context.state = CLOSED

    def stop(self):
        for _ in self._context.workers:
            self._context.schedule(None)
        self._context.state = STOPPED
        for _ in self._context.workers:
            self._context.schedule(None)

    def join(self, timeout=None):
        if self._context.state == RUNNING:
            raise RuntimeError('The Pool is still running')
        if self._context.state == CLOSED:
            wait_queue_depletion(self._context.task_queue, timeout)
            self.stop()
            self.join()
        else:
            for manager in self._managers:
                manager.join()

    def schedule(self, function, args=(), kwargs={}, identifier=None,
                 callback=None, timeout=0):
        metadata = (function, args, kwargs)
        return self._schedule_task(callback, timeout, identifier, metadata)

    def _schedule_task(self, callback, timeout, identifier, metadata):
        self._check_pool_state()

        task = Task(next(self._context.counter), callback=callback,
                    timeout=timeout, identifier=identifier, metadata=metadata)
        self._context.schedule(task)

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
            for manager in self._managers:
                if not manager.is_alive():
                    self._context._state = ERROR


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


class PoolContext(object):
    def __init__(self, queue, queueargs):
        self.workers = None
        self.state = CREATED
        self.counter = count()
        self.task_queue = create_queue(queue, queueargs)

    @property
    def alive(self):
        return self.state not in (ERROR, STOPPED)

    def schedule(self, task):
        self.task_queue.put(task)

    def acknowledge(self):
        self.task_queue.task_done()


def create_queue(queue, queueargs):
    if queue is not None:
        if isclass(queue):
            return queue(*queueargs)
        else:
            raise ValueError("Queue must be Class")
    else:
        return Queue()


class WorkersManager(object):
    def __init__(self, pool):
        self.pool = pool

    @staticmethod
    def manage_expired_workers(expired_workers):
        for worker in expired_workers:
            worker.reset()

    def stop_workers(self):
        for worker in self.pool.workers:
            worker.stop()


WorkerParameters = namedtuple('WorkerParameters',
                              ('task_limit',
                               'initializer',
                               'initargs',
                               'deinitializer',
                               'deinitargs'))


def run_initializer(initializer, initargs):
    try:
        initializer(*initargs)
        return True
    except Exception:
        print_exc()
        return False
