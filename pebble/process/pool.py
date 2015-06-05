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


import os
import time

from itertools import count
from collections import namedtuple
from signal import SIG_IGN, SIGINT, SIGTERM, SIGKILL, signal

from pebble import thread
from pebble.task import Task
from pebble.utils import execute
from pebble.pool import run_initializer
from pebble.pool import BasePool, WorkerParameters
from pebble.pool import RUNNING, SLEEP_UNIT
from pebble.process.channel import channels
from pebble.process.decorators import spawn
from pebble.process.utils import stop, send_results
from pebble.exceptions import TimeoutError, TaskCancelled, ProcessExpired


NoMessage = namedtuple('NoMessage', ())
NewTask = namedtuple('NewTask', ('id', 'payload'))
Results = namedtuple('Results', ('worker', 'results'))
Acknowledgement = namedtuple('Acknowledgement', ('worker', 'task'))


class Pool(BasePool):
    def __init__(self, workers=1, task_limit=0, queue=None, queueargs=None,
                 initializer=None, initargs=()):
        super(Pool, self).__init__(queue, queueargs)
        self._context.task_manager = TaskManager(self._context)
        self._context.workers_manager = ProcessWorkersManager(workers)
        self._context.workers_manager.worker_parameters = WorkerParameters(
            task_limit, initializer, initargs, None, None)

    def _start_pool(self):
        self._context.workers_manager.update_workers_status()
        self._managers = (pool_manager_loop(self._context),
                          task_scheduler_loop(self._context))
        self._context.state = RUNNING

    def stop(self):
        super(Pool, self).stop()
        self._context.schedule(None)


@thread.spawn(daemon=True, name='task_scheduler')
def task_scheduler_loop(context):
    task_manager = context.task_manager
    workers_manager = context.workers_manager

    for task in pool_get_next_task(context):
        task_manager.schedule(task)
        workers_manager.schedule(task)


def pool_get_next_task(context):
    queue = context.task_queue

    while context.alive:
        task = queue.get()

        if isinstance(task, Task) and not task.cancelled:
            yield task
        else:
            queue.task_done()
            return


@thread.spawn(daemon=True, name='context_manager')
def pool_manager_loop(context):
    task_manager = context.task_manager
    workers_manager = context.workers_manager

    for message in get_next_message(context):
        task_manager.update_task_status(message)
        workers_manager.update_workers_status()


def get_next_message(context):
    channel = context.workers_manager.pool_channel

    while context.alive:
        if channel.poll(SLEEP_UNIT):
            yield channel.recv()
        else:
            yield NoMessage()


class TaskManager(object):
    def __init__(self, context):
        self.context = context
        self.queued = {}
        self.in_progress = {}
        self.last_inspection = 0

    def schedule(self, task):
        self.queued[id(task)] = task

    def update_task_status(self, message):
        if isinstance(message, Acknowledgement):
            self.task_start(message)
        elif isinstance(message, Results):
            self.task_done(message)

        self.inspect_tasks()

    def task_start(self, acknowledgement):
        task = self.queued.pop(acknowledgement.task)
        task._timestamp = time.time()
        self.in_progress[acknowledgement.worker] = task

    def task_done(self, results):
        try:
            task = self.in_progress.pop(results.worker)
            task.set_results(results.results)
            self.context.acknowledge_task()
        except KeyError:  # timeout/cancelled task deliver
            pass

    def inspect_tasks(self):
        timestamp = time.time()

        if timestamp - self.last_inspection >= SLEEP_UNIT:
            self.inspection()
            self.last_inspection = timestamp

    def inspection(self):
        for worker_id, task in tuple(self.in_progress.items()):
            if self.has_timeout(task):
                self.task_problem(worker_id, TimeoutError('Task timeout'))
            elif task.cancelled:
                self.task_problem(worker_id, TaskCancelled('Task cancelled'))

    def task_problem(self, worker_id, error):
        self.context.workers_manager.stop_worker(worker_id)
        self.task_done(Results(worker_id, error))

    @staticmethod
    def has_timeout(task):
        if task.timeout:
            return time.time() - task._timestamp > task.timeout
        else:
            return False


class ProcessWorkersManager(object):
    def __init__(self, workers):
        self.workers = {}
        self.workers_number = workers
        self.worker_parameters = None
        self.pool_channel, self.workers_channel = channels()

    def schedule(self, task):
        self.pool_channel.send(NewTask(id(task), task._metadata))

    def update_workers_status(self):
        self.inspect_workers()
        self.create_workers()

    def inspect_workers(self):
        for worker_id, worker in tuple(self.workers.items()):
            if not worker.is_alive():
                self.expired_worker(worker_id)

    def create_workers(self):
        for _ in range(self.workers_number - len(self.workers)):
            self.new_worker()

    def expired_worker(self, worker_id):
        worker = self.workers.pop(worker_id)
        self.inspect_expiration(worker)

    def inspect_expiration(self, worker):
        if abs(worker.exitcode) not in (os.EX_OK, SIGTERM, SIGKILL):
            results = ProcessExpired('Abnormal termination')
            results.exitcode = worker.exitcode
            self.workers_channel.send(Results(worker.pid, results))

    def new_worker(self):
        worker = worker_process(self.worker_parameters, self.workers_channel)
        self.workers[worker.pid] = worker

    def stop_workers(self):
        for worker_id in self.workers.keys():
            self.stop_worker(worker_id)

    def stop_worker(self, worker_id):
        if worker_id in self.workers:
            with self.workers_channel.lock:
                stop(self.workers[worker_id])


@spawn(name='worker_process', daemon=True)
def worker_process(params, channel):
    """Runs the actual function in separate process."""
    signal(SIGINT, SIG_IGN)

    if params.initializer is not None:
        if not run_initializer(params.initializer, params.initargs):
            os._exit(os.EX_OK)

    try:
        for task in worker_get_next_task(channel, params.task_limit):
            results = execute_next_task(task)
            send_results(channel, Results(os.getpid(), results))
    except (EOFError, EnvironmentError) as error:
        return error.errno

    if params.deinitializer is not None:
        if not run_initializer(params.deinitializer, params.deinitargs):
            os._exit(os.EX_OK)

    return os._exit(os.EX_OK)


def worker_get_next_task(channel, task_limit):
    counter = count()

    while not task_limit or next(counter) < task_limit:
        task = fetch_task(channel)
        yield task


def fetch_task(channel):
    channel.poll()
    task = channel.recv()
    channel.send(Acknowledgement(os.getpid(), task.id))

    return task.payload


def execute_next_task(task):
    function, args, kwargs = task
    return execute(function, args, kwargs)
