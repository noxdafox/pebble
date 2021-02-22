# This file is part of Pebble.
# Copyright (c) 2013-2021, Matteo Cafasso

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
from multiprocessing import cpu_count
from concurrent.futures import Future

from pebble.common import execute, launch_thread
from pebble.pool.base_pool import MapFuture, MapResults
from pebble.pool.base_pool import BasePool, Task, TaskPayload
from pebble.pool.base_pool import iter_chunks, run_initializer
from pebble.pool.base_pool import CREATED, ERROR, RUNNING, SLEEP_UNIT


class ThreadPool(BasePool):
    """Allows to schedule jobs within a Pool of Threads.

    max_workers is an integer representing the amount of desired process workers
    managed by the pool.
    If max_tasks is a number greater than zero,
    each worker will be restarted after performing an equal amount of tasks.

    initializer must be callable, if passed, it will be called
    every time a worker is started, receiving initargs as arguments.

    """
    def __init__(self, max_workers=cpu_count(), max_tasks=0,
                 initializer=None, initargs=()):
        super(ThreadPool, self).__init__(
            max_workers, max_tasks, initializer, initargs)
        self._pool_manager = PoolManager(self._context)
        self._pool_manager_loop = None

    def _start_pool(self):
        with self._context.state_mutex:
            if self._context.state == CREATED:
                self._pool_manager.start()
                self._pool_manager_loop = launch_thread(
                    None, pool_manager_loop, True, self._pool_manager)

                self._context.state = RUNNING

    def _stop_pool(self):
        if self._pool_manager_loop is not None:
            self._pool_manager_loop.join()
        self._pool_manager.stop()

    def schedule(self, function, args=(), kwargs={}):
        """Schedules *function* to be run the Pool.

        *args* and *kwargs* will be forwareded to the scheduled function
        respectively as arguments and keyword arguments.

        A *concurrent.futures.Future* object is returned.
        """
        self._check_pool_state()

        future = Future()
        payload = TaskPayload(function, args, kwargs)
        task = Task(next(self._task_counter), future, None, payload)

        self._context.task_queue.put(task)

        return future

    def map(self, function, *iterables, **kwargs):
        """Returns an iterator equivalent to map(function, iterables).

        *chunksize* controls the size of the chunks the iterable will
        be broken into before being passed to the function. If None
        the size will be controlled by the Pool.

        """
        self._check_pool_state()

        timeout = kwargs.get('timeout')
        chunksize = kwargs.get('chunksize', 1)

        if chunksize < 1:
            raise ValueError("chunksize must be >= 1")

        futures = [self.schedule(process_chunk, args=(function, chunk))
                   for chunk in iter_chunks(chunksize, *iterables)]

        map_future = MapFuture(futures)
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


def pool_manager_loop(pool_manager):
    context = pool_manager.context

    while context.alive:
        pool_manager.update_status()
        time.sleep(SLEEP_UNIT)


class PoolManager:
    def __init__(self, context):
        self.workers = []
        self.context = context

    def start(self):
        self.create_workers()

    def stop(self):
        for worker in self.workers:
            self.context.task_queue.put(None)
        for worker in tuple(self.workers):
            self.join_worker(worker)

    def update_status(self):
        expired = self.inspect_workers()

        for worker in expired:
            self.join_worker(worker)

        self.create_workers()

    def inspect_workers(self):
        return tuple(w for w in self.workers if not w.is_alive())

    def create_workers(self):
        for _ in range(self.context.workers - len(self.workers)):
            worker = launch_thread(None, worker_thread, True, self.context)

            self.workers.append(worker)

    def join_worker(self, worker):
        worker.join()
        self.workers.remove(worker)


def worker_thread(context):
    """The worker thread routines."""
    queue = context.task_queue
    parameters = context.worker_parameters

    if parameters.initializer is not None:
        if not run_initializer(parameters.initializer, parameters.initargs):
            context.state = ERROR
            return

    for task in get_next_task(context, parameters.max_tasks):
        execute_next_task(task)
        queue.task_done()


def get_next_task(context, max_tasks):
    counter = count()
    queue = context.task_queue

    while context.alive and (max_tasks == 0 or next(counter) < max_tasks):
        task = queue.get()

        if task is not None:
            if task.future.cancelled():
                task.set_running_or_notify_cancel()
                queue.task_done()
            else:
                yield task


def execute_next_task(task):
    payload = task.payload
    task.timestamp = time.time()
    task.set_running_or_notify_cancel()

    result = execute(payload.function, *payload.args, **payload.kwargs)

    if isinstance(result, BaseException):
        task.future.set_exception(result)
    else:
        task.future.set_result(result)


def process_chunk(function, chunk):
    """Processes a chunk of the iterable passed to map dealing with errors."""
    return [execute(function, *args) for args in chunk]
