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

from threading import Lock
from itertools import count
from collections import deque
from multiprocessing import Pipe
from signal import SIG_IGN, SIGINT, signal

from pebble import thread
from pebble.task import Task
from pebble.utils import coroutine, execute
from pebble.pool import reset_workers, stop_workers
from pebble.pool import BasePool, WorkerParameters, run_initializer
from pebble.pool import CREATED, RUNNING, ERROR, CLOSED, STOPPED, SLEEP_UNIT
from pebble.process.decorators import spawn
from pebble.process.utils import stop, send_results
from pebble.exceptions import TimeoutError, TaskCancelled, ProcessExpired


class Pool(BasePool):
    def __init__(self, workers=1, task_limit=0, queue=None, queueargs=None,
                 initializer=None, initargs=()):
        super(Pool, self).__init__(queue, queueargs)
        self._context.workers = create_workers(workers, task_limit,
                                               initializer, initargs)

    def _start_pool(self):
        reset_workers(self._context.workers)
        self._managers = (pool_manager_loop(self._context),
                          task_scheduler_loop(self._context))
        self._context.state = RUNNING


def create_workers(workers, task_limit, initializer, initargs):
    parameters = WorkerParameters(task_limit, initializer, initargs, None, None)
    return [Worker(parameters) for _ in range(workers)]


@thread.spawn(daemon=True, name='task_scheduler')
def task_scheduler_loop(pool):
    try:
        scheduler = task_scheduler(pool)

        for task in task_fetcher(pool):
            scheduler.send(task)
    except StopIteration:
        return


@coroutine
def task_scheduler(pool):
    task = None
    workers = pool.workers

    while pool.state not in (ERROR, STOPPED):
        for worker in workers:
            task = task or (yield)

            try:
                worker.schedule_task(task)
            except (EnvironmentError, RuntimeError):
                time.sleep(SLEEP_UNIT)  # throttle if workers are not ready
            else:
                task = None


def task_fetcher(pool):
    queue = pool.task_queue

    while pool.state not in (ERROR, STOPPED):
        task = queue.get()

        if isinstance(task, Task) and not task.cancelled:
            yield task
        else:
            queue.task_done()
            return


@thread.spawn(daemon=True, name='pool_manager')
def pool_manager_loop(pool):
    while pool.state not in (ERROR, STOPPED):
        reset_workers(pool.workers)
        manage_tasks(pool)
        time.sleep(SLEEP_UNIT)

    pool.schedule(None)
    stop_workers(pool.workers)


def manage_tasks(pool):
    for worker in pool.workers:
        if worker.handle_result():
            pool.acknowledge()
        if worker.handle_timeout():
            pool.acknowledge()
        if worker.handle_cancel():
            pool.acknowledge()


class Worker(object):
    def __init__(self, parameters):
        self.mutex = Lock()
        self.process_worker = None
        self.parameters = parameters
        self.task_manager = WorkerTaskManager(parameters.task_limit)

    @property
    def alive(self):
        if self.process_worker is not None:
            return self.process_worker.alive
        else:
            return False

    def schedule_task(self, task):
        with self.mutex:
            self.task_manager.schedule_task(task)
            self.send_task(task)

    def send_task(self, task):
        try:
            self.process_worker.send(task)
        except EnvironmentError:
            self.task_manager.retract_task()
            raise

    def handle_result(self):
        try:
            if self.process_worker.results_ready:
                result = self.get_result()
                self.task_manager.set_result(result)
                return True
            else:
                return False
        except EOFError:
            pass

    def get_result(self):
        try:
            return self.process_worker.receive()
        except (EOFError, EnvironmentError):
            return self.handle_result_error()

    def handle_result_error(self):
        if self.process_worker.exitcode != os.EX_OK:
            result = ProcessExpired('Abnormal termination')
            result.exitcode = self.process_worker.exitcode
            return result
        else:
            raise

    def handle_timeout(self):
        if self.task_manager.task_timeout:
            self.process_worker.stop()
            self.task_manager.set_result(TimeoutError('Task timeout'))
            return True
        else:
            return False

    def handle_cancel(self):
        if self.task_manager.task_cancelled:
            self.process_worker.stop()
            self.task_manager.set_result(TaskCancelled('Task cancelled'))
            return True
        else:
            return False

    def stop(self):
        if self.process_worker.alive:
            self.process_worker.stop()

    def reset(self):
        with self.mutex:
            self.process_worker = WorkerProcess(self.parameters)
            self.task_manager.reset()
            self.reschedule_tasks()

    def reschedule_tasks(self):
        for task in self.task_manager.tasks:
            try:
                self.process_worker.send(task)
            except EnvironmentError:
                return


class WorkerTaskManager(object):
    def __init__(self, task_limit):
        self.task_counter = None
        self.task_buffer = deque()
        self.task_limit = task_limit

    @property
    def tasks(self):
        for task in self.task_buffer:
            yield task

    @property
    def task_timeout(self):
        if self.task_buffer:
            return self.has_timeout(self.task_buffer[0])
        else:
            return False

    @staticmethod
    def has_timeout(task):
        if task.timeout:
            return time.time() - task._timestamp > task.timeout
        else:
            return False

    @property
    def task_cancelled(self):
        if self.task_buffer:
            return self.task_buffer[0].cancelled
        else:
            return False

    def reset(self):
        if self.task_limit:
            self.task_counter = count()

    def schedule_task(self, task):
        self.task_schedulable()

        if not self.task_buffer:
            task._timestamp = time.time()
        self.task_buffer.append(task)

    def task_schedulable(self):
        if self.task_limit and next(self.task_counter) >= self.task_limit:
            raise RuntimeError("No more task accepted into worker")

    def retract_task(self):
        task = self.task_buffer.pop()
        task._timestamp = 0

    def set_result(self, result):
        task = self.task_buffer.popleft()
        task.set_results(result)

        if self.task_buffer:
            self.task_buffer[0]._timestamp = time.time()


class WorkerProcess(object):
    def __init__(self, parameters):
        pool_side, worker_side = self.create_channels()
        self.alive = True
        self.channel = pool_side
        self.process = worker_process(parameters, worker_side)
        worker_side.close()

    @staticmethod
    def create_channels():
        reader1, writer1 = Pipe()
        reader2, writer2 = Pipe()

        return Channel(reader1, writer2), Channel(reader2, writer1)

    @property
    def exitcode(self):
        return self.process.exitcode

    @property
    def results_ready(self):
        return self.channel.reader.poll()

    def send(self, task):
        self.channel.writer.send(task._metadata)

    def receive(self):
        try:
            return self.channel.reader.recv()
        except (EOFError, EnvironmentError):
            self.alive = False
            raise

    def stop(self):
        stop(self.process)
        self.alive = False


class Channel(object):
    def __init__(self, reader, writer):
        self.reader = reader
        self.writer = writer

    def close(self):
        self.reader.close()
        self.writer.close()


@spawn(name='worker_process', daemon=True)
def worker_process(parameters, channel):
    """Runs the actual function in separate process."""
    signal(SIGINT, SIG_IGN)

    if parameters.initializer is not None:
        if not run_initializer(parameters.initializer, parameters.initargs):
            os._exit(os.EX_OK)

    try:
        for task in get_next_task(channel, parameters.task_limit):
            results = execute_next_task(task)
            send_results(channel.writer, results)
    except (EOFError, EnvironmentError) as error:
        return error.errno

    if parameters.deinitializer is not None:
        if not run_initializer(parameters.deinitializer, parameters.deinitargs):
            os._exit(os.EX_OK)

    return os._exit(os.EX_OK)


def get_next_task(channel, task_limit):
    counter = count()

    while not task_limit or next(counter) < task_limit:
        task = channel.reader.recv()
        yield task


def execute_next_task(task):
    function, args, kwargs = task
    return execute(function, args, kwargs)
