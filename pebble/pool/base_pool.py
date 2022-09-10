# This file is part of Pebble.
# Copyright (c) 2013-2022, Matteo Cafasso

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
import logging

from queue import Queue
from threading import RLock
from collections import namedtuple
from typing import Callable, Optional
from itertools import chain, count, islice
from concurrent.futures import Future, TimeoutError

from pebble.common import PebbleFuture, ProcessFuture, SLEEP_UNIT


class BasePool:
    def __init__(self, max_workers: int,
                 max_tasks: int,
                 initializer: Optional[Callable],
                 initargs: list):
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
    def active(self) -> bool:
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

    def join(self, timeout: float = None):
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
            self._context.task_queue.put(None)
            self._stop_pool()

    def _wait_queue_depletion(self, timeout: Optional[float]):
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
    def __init__(self, max_workers: int,
                 max_tasks: int,
                 initializer: Callable,
                 initargs: list):
        self._state = CREATED
        self.state_mutex = RLock()

        self.task_queue = Queue()
        self.workers = max_workers
        self.task_counter = count()
        self.worker_parameters = Worker(max_tasks, initializer, initargs)

    @property
    def state(self) -> int:
        return self._state

    @state.setter
    def state(self, state: int):
        with self.state_mutex:
            if self.alive:
                self._state = state

    @property
    def alive(self) -> bool:
        return self.state not in (ERROR, STOPPED)


class Task:
    def __init__(self, identifier: int,
                 future: Future,
                 timeout: Optional[float],
                 payload: 'TaskPayload'):
        self.id = identifier
        self.future = future
        self.timeout = timeout
        self.payload = payload
        self.timestamp = 0.0
        self.worker_id = 0

    @property
    def started(self) -> bool:
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


class MapFuture(PebbleFuture):
    def __init__(self, futures: list):
        super().__init__()
        self._futures = futures

    @property
    def futures(self) -> list:
        return self._futures

    def cancel(self) -> bool:
        """Cancel the future.

        Returns True if any of the elements of the iterables is cancelled.
        False otherwise.
        """
        super().cancel()

        return any(tuple(f.cancel() for f in self._futures))


class ProcessMapFuture(ProcessFuture):
    def __init__(self, futures: list):
        super().__init__()
        self._futures = futures

    @property
    def futures(self) -> list:
        return self._futures

    def cancel(self) -> bool:
        """Cancel the future.

        Returns True if any of the elements of the iterables is cancelled.
        False otherwise.
        """
        super().cancel()

        return any(tuple(f.cancel() for f in self._futures))


class MapResults:
    def __init__(self, futures: list, timeout: float = None):
        self._results = chain.from_iterable(
            chunk_result(f, timeout) for f in futures)

    def __iter__(self):
        return self

    def next(self):
        result = next(self._results)

        if isinstance(result, Exception):
            raise result

        return result

    __next__ = next


def map_results(map_future: MapFuture, timeout: Optional[float]) -> MapFuture:
    futures = map_future.futures
    if not futures:
        map_future.set_result(MapResults(futures))
        return map_future

    def done_map(_):
        if not map_future.done():
            map_future.set_result(MapResults(futures, timeout=timeout))

    for future in futures:
        future.add_done_callback(done_map)
        setattr(future, 'map_future', map_future)

    return map_future


def iter_chunks(chunksize: int, *iterables):
    """Iterates over zipped iterables in chunks."""
    iterables = iter(zip(*iterables))

    while 1:
        chunk = tuple(islice(iterables, chunksize))

        if not chunk:
            return

        yield chunk


def chunk_result(future: ProcessFuture, timeout: Optional[float]):
    """Returns the results of a processed chunk."""
    try:
        return future.result(timeout=timeout)
    except Exception as error:
        return (error, )


def run_initializer(initializer: Callable, initargs: list):
    """Runs the Pool initializer dealing with errors."""
    try:
        initializer(*initargs)
        return True
    except Exception as error:
        logging.exception(error)
        return False


# Pool states
CREATED = 0
RUNNING = 1
CLOSED = 2
STOPPED = 3
ERROR = 4


Worker = namedtuple('Worker', ('max_tasks', 'initializer', 'initargs'))
TaskPayload = namedtuple('TaskPayload', ('function', 'args', 'kwargs'))
