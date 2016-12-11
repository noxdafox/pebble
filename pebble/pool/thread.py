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
from traceback import format_exc
from pebble.common import launch_thread
from pebble.pool.base_pool import BasePool, run_initializer
from pebble.pool.base_pool import ERROR, RUNNING, SLEEP_UNIT


class ThreadPool(BasePool):
    """Allows to schedule jobs within a Pool of Threads.

    max_workers is an integer representing the amount of desired process workers
    managed by the pool.
    If max_tasks is a number greater than zero,
    each worker will be restarted after performing an equal amount of tasks.

    initializer must be callable, if passed, it will be called
    every time a worker is started, receiving initargs as arguments.

    """
    def __init__(self, max_workers=1, max_tasks=0,
                 initializer=None, initargs=()):
        super(ThreadPool, self).__init__(
            max_workers, max_tasks, initializer, initargs)
        self._pool_manager = PoolManager(self._context)

    def _start_pool(self):
        self._pool_manager.start()
        self._loops = (launch_thread(pool_manager_loop, self._pool_manager),)
        self._context.state = RUNNING

    def _stop_pool(self):
        for loop in self._loops:
            loop.join()
        self._pool_manager.stop()


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
            worker = launch_thread(worker_thread, self.context)

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
                task.future.set_running_or_notify_cancel()
                queue.task_done()
            else:
                yield task


def execute_next_task(task):
    payload = task.payload
    task.timestamp = time.time()
    task.future.set_running_or_notify_cancel()

    try:
        results = payload.function(*payload.args, **payload.kwargs)
    except BaseException as error:
        error.traceback = format_exc()
        task.future.set_exception(error)
    else:
        task.future.set_result(results)
