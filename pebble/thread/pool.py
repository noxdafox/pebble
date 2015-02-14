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
from threading import Event
from traceback import format_exc
from collections import namedtuple

from pebble.task import Task
from pebble.utils import execute
from pebble.thread.decorators import spawn
from pebble.pool import reset_workers, stop_workers
from pebble.pool import CREATED, STOPPED, RUNNING, ERROR, SLEEP_UNIT
from pebble.pool import BasePool, WorkerParameters, run_initializer


class Pool(BasePool):
    def __init__(self, workers=1, task_limit=0, queue=None, queueargs=None,
                 initializer=None, initargs=()):
        super(Pool, self).__init__(queue, queueargs)
        self._context.workers = create_workers(workers, task_limit,
                                               initializer, initargs,
                                               self._context)

    def _start_pool(self):
        reset_workers(self._context.workers)
        self._managers = (pool_manager_loop(self._context), )
        self._context.state = RUNNING


def create_workers(workers, task_limit, initializer, initargs, pool):
    parameters = WorkerParameters(task_limit, initializer, initargs, None, None)
    return [Worker(parameters, pool) for _ in range(workers)]


@spawn(daemon=True, name='pool_manager')
def pool_manager_loop(pool):
    while pool.state not in (ERROR, STOPPED):
        reset_workers(pool.workers)
        time.sleep(SLEEP_UNIT)

    for worker in pool.workers:
        pool.schedule(None)
    stop_workers(pool.workers)


class Worker(object):
    def __init__(self, parameters, pool):
        self.pool = pool
        self.thread_worker = None
        self.parameters = parameters

    @property
    def alive(self):
        if self.thread_worker is not None:
            return self.thread_worker.is_alive()
        else:
            return False

    def stop(self):
        self.thread_worker.join()

    def reset(self):
        self.thread_worker = worker_thread(self.parameters, self.pool)


@spawn(name='worker_thread', daemon=True)
def worker_thread(parameters, pool):
    """Runs the actual function in separate thread."""
    if parameters.initializer is not None:
        if not run_initializer(parameters.initializer, parameters.initargs):
            return

    for task in get_next_task(pool, parameters.task_limit):
        execute_next_task(task)
        pool.acknowledge()

    if parameters.deinitializer is not None:
        if not run_initializer(parameters.deinitializer, parameters.deinitargs):
            return

    return


def get_next_task(pool, task_limit):
    counter = count()
    queue = pool.task_queue

    while pool.alive and not task_limit or next(counter) < task_limit:
        task = queue.get()

        if task is not None and not task.cancelled:
            task._timestamp = time.time()
            yield task
        else:
            pool.acknowledge()
            return


def execute_next_task(task):
    function, args, kwargs = task._metadata
    results = execute(function, args, kwargs)
    task.set_results(results)
