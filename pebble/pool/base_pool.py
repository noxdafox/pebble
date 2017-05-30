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

from itertools import count
from traceback import print_exc
from collections import namedtuple
from concurrent.futures import TimeoutError
try:
    from queue import Queue
except ImportError:
    from Queue import Queue

from pebble.common import execute, ProcessFuture, SLEEP_UNIT


class BasePool(object):
    def __init__(self, max_workers, max_tasks, initializer, initargs):
        self._context = PoolContext(
            max_workers, max_tasks, initializer, initargs)
        self._loops = ()
        self._task_counter = count()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
        self.join()

    @property
    def active(self):
        self._update_pool_state()

        return self._context.state in (CLOSED, RUNNING)

    def close(self):
        """Closes the Pool preventing new tasks from being accepted.

        Pending tasks will be completed.
        """
        self._context.state = CLOSED

    def stop(self):
        """Stops the pool without performing any pending task."""
        self._context.state = STOPPED
        self._context.task_queue.put(None)

    def join(self, timeout=None):
        """Joins the pool waiting until all workers exited.

        If *timeout* is set, it block until all workers are done
        or raises TimeoutError.
        """
        if self._context.state == RUNNING:
            raise RuntimeError('The Pool is still running')
        if self._context.state == CLOSED:
            self._wait_queue_depletion(timeout)
            self.stop()
            self.join()
        else:
            self._stop_pool()

    def _wait_queue_depletion(self, timeout):
        tick = time.time()

        while self.active:
            if timeout is not None and time.time() - tick > timeout:
                raise TimeoutError("Tasks are still being executed")
            elif self._context.task_queue.unfinished_tasks:
                time.sleep(SLEEP_UNIT)
            else:
                return

    def _check_pool_state(self):
        self._update_pool_state()

        if self._context.state == ERROR:
            raise RuntimeError('Unexpected error within the Pool')
        elif self._context.state != RUNNING:
            raise RuntimeError('The Pool is not active')

    def _update_pool_state(self):
        if self._context.state == CREATED:
            self._start_pool()

        for loop in self._loops:
            if not loop.is_alive():
                self._context.state = ERROR

    def _start_pool(self):
        raise NotImplementedError("Not implemented")

    def _stop_pool(self):
        raise NotImplementedError("Not implemented")


class PoolContext:
    def __init__(self, max_workers, max_tasks, initializer, initargs):
        self.state = CREATED
        self.task_queue = Queue()
        self.workers = max_workers
        self.task_counter = count()
        self.worker_parameters = Worker(max_tasks, initializer, initargs)

    @property
    def alive(self):
        return self.state not in (ERROR, STOPPED)


class Task:
    def __init__(self, identifier, future, timeout, payload):
        self.id = identifier
        self.future = future
        self.timeout = timeout
        self.payload = payload
        self.timestamp = 0
        self.worker_id = 0

    @property
    def started(self):
        return bool(self.timestamp > 0)

    def set_running_or_notify_cancel(self):
        if hasattr(self.future, 'map_future'):
            if not self.future.map_future.done():
                try:
                    self.future.map_future.set_running_or_notify_cancel()
                except RuntimeError:
                    pass

        try:
            self.future.set_running_or_notify_cancel()
        except RuntimeError:
            pass


def run_initializer(initializer, initargs):
    try:
        initializer(*initargs)
        return True
    except Exception:
        print_exc()
        return False


def map_function(function, chunk):
    """Processes a chunk of the iterable passed to map dealing with errors."""
    return [execute(function, *args) for args in chunk]


class MapResults:
    def __init__(self, futures):
        self._current = None
        self._futures = futures

    def __iter__(self):
        return self

    def next(self):
        result = self._next_result()

        if isinstance(result, Exception):
            raise result

        return result

    def _next_result(self):
        while True:
            if self._current is None:
                try:
                    future = self._futures.pop(0)
                    self._current = future.result()
                except IndexError:
                    raise StopIteration

            try:
                return self._current.pop(0)
            except IndexError:
                self._current = None
                continue

    __next__ = next


# Pool states
CREATED = 0
RUNNING = 1
CLOSED = 2
STOPPED = 3
ERROR = 4


Worker = namedtuple('Worker', ('max_tasks', 'initializer', 'initargs'))
TaskPayload = namedtuple('TaskPayload', ('function', 'args', 'kwargs'))
