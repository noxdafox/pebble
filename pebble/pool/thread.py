# This file is part of Pebble.
# Copyright (c) 2013-2024, Matteo Cafasso

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
import multiprocessing

from itertools import count
from typing import Callable
from concurrent.futures import Future

from pebble.common import ResultStatus, execute, launch_thread, CONSTS
from pebble.pool.base_pool import iter_chunks, run_initializer
from pebble.pool.base_pool import PoolStatus, MapFuture, map_results
from pebble.pool.base_pool import PoolContext, BasePool, Task, TaskPayload


class ThreadPool(BasePool):
    """Allows to schedule jobs within a Pool of Threads.

    max_workers is an integer representing the amount of desired process workers
    managed by the pool.
    If max_tasks is a number greater than zero,
    each worker will be restarted after performing an equal amount of tasks.

    initializer must be callable, if passed, it will be called
    every time a worker is started, receiving initargs as arguments.

    """
    def __init__(self, max_workers: int = multiprocessing.cpu_count(),
                 max_tasks: int = 0,
                 initializer: Callable = None,
                 initargs: list = ()):
        super().__init__(max_workers, max_tasks, initializer, initargs)
        self._pool_manager = PoolManager(self._context)
        self._pool_manager_loop = None

    def _start_pool(self):
        with self._context.status_mutex:
            if self._context.status == PoolStatus.CREATED:
                self._pool_manager.start()
                self._pool_manager_loop = launch_thread(
                    None, pool_manager_loop, True, self._pool_manager)

                self._context.status = PoolStatus.RUNNING

    def _stop_pool(self):
        if self._pool_manager_loop is not None:
            self._pool_manager_loop.join()
        self._pool_manager.stop()

    def schedule(self, function, args=(), kwargs={}) -> Future:
        """Schedules *function* to be run the Pool.

        *args* and *kwargs* will be forwareded to the scheduled function
        respectively as arguments and keyword arguments.

        A *concurrent.futures.Future* object is returned.

        """
        self._check_pool_status()

        future = Future()
        payload = TaskPayload(function, args, kwargs)
        task = Task(next(self._task_counter), future, None, payload)

        self._context.task_queue.put(task)

        return future

    def submit(self, function: Callable, *args, **kwargs) -> Future:
        """This function is provided for compatibility with
        `asyncio.loop.run_in_executor`.

        For scheduling jobs within the pool use `schedule` instead.

        """
        return self.schedule(function, args=args, kwargs=kwargs)

    def map(self, function: Callable, *iterables, **kwargs) -> MapFuture:
        """Returns an iterator equivalent to map(function, iterables).

        *chunksize* controls the size of the chunks the iterable will
        be broken into before being passed to the function. If None
        the size will be controlled by the Pool.

        """
        self._check_pool_status()

        timeout = kwargs.get('timeout')
        chunksize = kwargs.get('chunksize', 1)

        if chunksize < 1:
            raise ValueError("chunksize must be >= 1")

        futures = [self.schedule(process_chunk, args=(function, chunk))
                   for chunk in iter_chunks(zip(*iterables), chunksize)]

        return map_results(MapFuture(futures), timeout)


def pool_manager_loop(pool_manager: 'PoolManager'):
    context = pool_manager.context

    while context.alive:
        pool_manager.update_status()
        time.sleep(CONSTS.sleep_unit)


class PoolManager:
    def __init__(self, context: PoolContext):
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

    def inspect_workers(self) -> tuple:
        return tuple(w for w in self.workers if not w.is_alive())

    def create_workers(self):
        for _ in range(self.context.workers - len(self.workers)):
            worker = launch_thread(None, worker_thread, True, self.context)

            self.workers.append(worker)

    def join_worker(self, worker):
        worker.join()
        self.workers.remove(worker)


def worker_thread(context: PoolContext):
    """The worker thread routines."""
    queue = context.task_queue
    parameters = context.worker_parameters

    if parameters.initializer is not None:
        if not run_initializer(parameters.initializer, parameters.initargs):
            context.status = PoolStatus.ERROR
            return

    for task in get_next_task(context, parameters.max_tasks):
        execute_next_task(task)
        queue.task_done()


def get_next_task(context: PoolContext, max_tasks: int):
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


def execute_next_task(task: Task):
    payload = task.payload
    task.timestamp = time.time()
    task.set_running_or_notify_cancel()

    result = execute(payload.function, *payload.args, **payload.kwargs)

    if result.status == ResultStatus.SUCCESS:
        task.future.set_result(result.value)
    else:
        task.future.set_exception(result.value)


def process_chunk(function: Callable, chunk: list) -> list:
    """Processes a chunk of the iterable passed to map dealing with errors."""
    return [execute(function, *args) for args in chunk]
