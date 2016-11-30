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
from pebble.utils import execute
from pebble.thread.decorators import spawn
from pebble.pool import ERROR, RUNNING, SLEEP_UNIT
from pebble.pool import BasePool, run_initializer, task_limit_reached


class Pool(BasePool):
    """Allows to schedule jobs within a Pool of Threads.

    workers is an integer representing the amount of desired thread workers
    managed by the pool.
    If worker_task_limit is a number greater than zero,
    each worker will be restarted after performing an equal amount of tasks.

    The queue_factory callable allows to replace the internal task buffer
    of the Pool with a custom one. The callable must return a thread safe
    object exposing the same interface of the standard Python Queue.

    initializer must be callable, if passed, it will be called
    every time a worker is started, receiving initargs as arguments.
    """
    def __init__(self, workers=1, task_limit=0, queue_factory=None,
                 initializer=None, initargs=()):
        super(Pool, self).__init__(workers, task_limit, queue_factory,
                                   initializer, initargs)
        self._pool_manager = PoolManager(self._context)

    def _start_pool(self):
        self._pool_manager.start()
        self._loops = (pool_manager_loop(self._pool_manager),)
        self._context.state = RUNNING

    def _stop_pool(self):
        for loop in self._loops:
            loop.join()
        self._pool_manager.stop()


@spawn(daemon=True, name='pool_manager')
def pool_manager_loop(pool_manager):
    context = pool_manager.context

    while context.alive:
        pool_manager.update_status()
        time.sleep(SLEEP_UNIT)


class PoolManager(object):
    def __init__(self, context):
        self.context = context
        self.workers = []

    def start(self):
        self.create_workers()

    def stop(self):
        for _ in self.workers:
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
            self.workers.append(worker_thread(self.context))

    def join_worker(self, worker):
        worker.join()
        self.workers.remove(worker)


@spawn(name='worker_thread', daemon=True)
def worker_thread(context):
    """The worker thread routines."""
    parameters = context.worker_parameters
    task_limit = parameters.task_limit

    if parameters.initializer is not None:
        if not run_initializer(parameters.initializer, parameters.initargs):
            context.state = ERROR
            return

    for task in get_next_task(context, task_limit):
        execute_next_task(task)
        context.task_queue.task_done()


def get_next_task(context, task_limit):
    counter = count()
    queue = context.task_queue

    while context.alive and not task_limit_reached(counter, task_limit):
        task = queue.get()

        if task is not None and not task.cancelled:
            yield task
        else:
            queue.task_done()


def execute_next_task(task):
    parameters = task._metadata
    task._timestamp = time.time()
    results = execute(parameters.function, parameters.args, parameters.kwargs)
    task.set_results(results)
