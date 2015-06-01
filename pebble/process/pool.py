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

from select import select
from threading import Lock
from itertools import count
from collections import deque
from multiprocessing import Pipe
from signal import SIG_IGN, SIGINT, signal

from pebble import thread
from pebble.task import Task
from pebble.utils import coroutine, execute
from pebble.pool import run_initializer
from pebble.pool import BasePool, WorkerParameters, WorkersManager
from pebble.pool import RUNNING, ERROR, STOPPED, SLEEP_UNIT
from pebble.process.decorators import spawn
from pebble.process.utils import stop, send_results
from pebble.exceptions import TimeoutError, TaskCancelled, ProcessExpired


class Pool(BasePool):
    def __init__(self, workers=1, task_limit=0, queue=None, queueargs=None,
                 initializer=None, initargs=()):
        super(Pool, self).__init__(queue, queueargs)
        self._context.workers_manager = ProcessWorkersManager(self._context)
        self._context.workers_manager.create_workers(workers, task_limit,
                                                     initializer, initargs)

    def _start_pool(self):
        self._context.workers_manager.manage_expired_workers(
            self._context.workers_manager.workers)
        self._managers = (pool_manager_loop(self._context),
                          task_scheduler_loop(self._context))
        self._context.state = RUNNING

    def stop(self):
        super(Pool, self).stop()
        self._context.schedule(None)


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
    workers = pool.workers_manager.workers

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
    workers_manager = pool.workers_manager

    while pool.state not in (ERROR, STOPPED):
        workers = workers_manager.inspect_workers()

        if any(workers):
            workers_manager.manage_workers(workers)


class ProcessWorkersManager(WorkersManager):
    def __init__(self, pool):
        super(ProcessWorkersManager, self).__init__(pool)
        self.last_inspection = 0

    def create_workers(self, workers, task_limit, initializer, initargs):
        params = WorkerParameters(task_limit, initializer, initargs, None, None)
        self.workers = tuple(Worker(params) for _ in range(workers))

    def inspect_workers(self):
        workers = self.workers

        ready = get_ready_workers(workers)
        timeout, cancelled, expired = self.get_problematic_workers(workers)

        return ready, timeout, cancelled, expired

    def get_problematic_workers(self, workers):
        timestamp = time.time()

        if timestamp - self.last_inspection >= SLEEP_UNIT:
            self.last_inspection = timestamp
            return ((w for w in workers if w.task_timeout()),
                    (w for w in workers if w.task_cancelled()),
                    (w for w in workers if not w.alive))
        else:
            return (), (), ()

    def manage_workers(self, workers):
        ready, timeout, cancelled, expired = workers

        self.manage_ready_workers(ready)
        self.manage_timeout_workers(timeout)
        self.manage_cancelled_workers(cancelled)
        self.manage_expired_workers(expired)

    def manage_ready_workers(self, ready_workers):
        for worker in ready_workers:
            try:
                worker.handle_result()
                self.pool.acknowledge()
            except RuntimeError:
                continue

    def manage_timeout_workers(self, timeout_workers):
        for worker in timeout_workers:
            worker.handle_timeout()
            self.pool.acknowledge()

    def manage_cancelled_workers(self, cancelled_workers):
        for worker in cancelled_workers:
            worker.handle_cancel()
            self.pool.acknowledge()

    def stop_workers(self):
        for worker in self.workers:
            worker.stop()


def get_ready_workers_unix(workers):
    valid_selectors = (w.selector for w in workers if not w.selector.closed)
    ready_selectors = select(valid_selectors, (), (), SLEEP_UNIT)
    return (w for w in workers if w.selector in ready_selectors[0])


def get_ready_workers_windows(workers):
    ready_workers = (w for w in workers if w.task_ready())
    if not any(ready_workers):
        time.sleep(SLEEP_UNIT)
    return ready_workers


if os.name == 'nt':
    get_ready_workers = get_ready_workers_windows
else:
    get_ready_workers = get_ready_workers_unix


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

    @property
    def selector(self):
        return self.process_worker.receiver

    def task_ready(self):
        if self.process_worker.alive:
            return self.process_worker.receiver_ready
        else:
            return False

    def task_timeout(self):
        return self.task_manager.task_timeout

    def task_cancelled(self):
        return self.task_manager.task_cancelled

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
            result = self.get_result()
            self.task_manager.set_result(result)
        except EOFError:
            raise RuntimeError('Process expired')

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
        self.stop()
        self.task_manager.set_result(TimeoutError('Task timeout'))

    def handle_cancel(self):
        self.stop()
        self.task_manager.set_result(TaskCancelled('Task cancelled'))

    def stop(self):
        if self.alive:
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
        self.channel = pool_side
        self.process = worker_process(parameters, worker_side)
        worker_side.close()

    @staticmethod
    def create_channels():
        reader1, writer1 = Pipe()
        reader2, writer2 = Pipe()

        return Channel(reader1, writer2), Channel(reader2, writer1)

    @property
    def alive(self):
        return not self.channel.closed

    @property
    def exitcode(self):
        if self.channel.closed:
            return os.EX_OK
        else:
            return self.process.exitcode

    @property
    def receiver(self):
        return self.channel.reader

    @property
    def receiver_ready(self):
        return self.channel.reader.poll()

    def send(self, task):
        self.channel.writer.send(task._metadata)

    def receive(self):
        try:
            return self.channel.reader.recv()
        except (EOFError, EnvironmentError):
            self.stop()
            raise

    def stop(self):
        stop(self.process)
        self.channel.close()


class Channel(object):
    def __init__(self, reader, writer):
        self.reader = reader
        self.writer = writer

    @property
    def closed(self):
        return all((self.reader.closed, self.writer.closed))

    def close(self):
        self.reader.close()
        self.writer.close()


@spawn(name='worker_process', daemon=True)
def worker_process(params, channel):
    """Runs the actual function in separate process."""
    signal(SIGINT, SIG_IGN)

    if params.initializer is not None:
        if not run_initializer(params.initializer, params.initargs):
            os._exit(os.EX_OK)

    try:
        for task in get_next_task(channel, params.task_limit):
            results = execute_next_task(task)
            send_results(channel.writer, results)
    except (EOFError, EnvironmentError) as error:
        return error.errno

    if params.deinitializer is not None:
        if not run_initializer(params.deinitializer, params.deinitargs):
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
