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
from pebble.pool import RUNNING, SLEEP_UNIT
from pebble.pool import BasePool, run_initializer


class Pool(BasePool):
    def __init__(self, workers=1, task_limit=0, queue=None, queueargs=None,
                 initializer=None, initargs=()):
        super(Pool, self).__init__(workers, task_limit, queue, queueargs,
                                   initializer, initargs)
        self._pool_manager = PoolManager(self._context)

    def _start_pool(self):
        self._pool_manager.start()
        self._loops = (pool_manager_loop(self._pool_manager),)
        self._context.state = RUNNING

    def _stop_pool(self):
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
        for worker in self.workers:
            worker.join()

    def update_status(self):
        expired = self.inspect_workers()

        for worker in expired:
            self.workers.pop(worker)

        self.create_workers()

    def inspect_workers(self):
        return [(w for w in self.workers if not w.is_alive())]

    def create_workers(self):
        for _ in range(self.context.workers - len(self.workers)):
            self.workers.append(worker_thread(self.context))


@spawn(name='worker_thread', daemon=True)
def worker_thread(context):
    """Runs the actual function in separate thread."""
    parameters = context.parameters
    task_limit = parameters.task_limit

    if parameters.initializer is not None:
        if not run_initializer(parameters.initializer, parameters.initargs):
            return

    for task in get_next_task(context, task_limit):
        execute_next_task(task)
        context.acknowledge()

    if parameters.deinitializer is not None:
        if not run_initializer(parameters.deinitializer, parameters.deinitargs):
            return


def get_next_task(context, task_limit):
    counter = count()
    queue = context.task_queue

    while context.alive and task_limit != 0 or next(counter) < task_limit:
        task = queue.get()

        if task is not None and not task.cancelled:
            task._timestamp = time.time()
            yield task
        else:
            context.acknowledge()
            return


def execute_next_task(task):
    function, args, kwargs = task._metadata
    results = execute(function, args, kwargs)
    task.set_results(results)
