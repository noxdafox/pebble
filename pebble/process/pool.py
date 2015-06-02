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

from multiprocessing import Pipe
from signal import SIG_IGN, SIGINT, signal

from pebble import thread
from pebble.task import Task
from pebble.utils import execute
from pebble.pool import run_initializer
from pebble.pool import BasePool, WorkerParameters, WorkersManager
from pebble.pool import RUNNING, ERROR, STOPPED, SLEEP_UNIT
from pebble.process.decorators import spawn
from pebble.process.utils import stop, send_results
from pebble.exceptions import TimeoutError, TaskCancelled, ProcessExpired


NoMessage = namedtuple('NoMessage', ())
Results = namedtuple('Results', ('worker', 'results'))
Acknowledgement = namedtuple('Acknowledgement', ('worker', 'task'))


class Pool(BasePool):
    def __init__(self, workers=1, task_limit=0, queue=None, queueargs=None,
                 initializer=None, initargs=()):
        super(Pool, self).__init__(queue, queueargs)
        self._context.task_manager = TaskManager(self._context)
        self._context.workers_manager = ProcessWorkersManager(self._context)
        self._context.workers_manager.create_workers(workers, task_limit,
                                                     initializer, initargs)

    def _start_pool(self):
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
    channel = workers_manager.pool_channel

    while context.alive:
        try:
            channel.poll(SLEEP_UNIT)
        except TimeoutError:
            yield NoMessage()
        else:
            yield channel.recv()


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
        task = self.tasks.pop(message.task)
        task._timestamp = time.time()
        self.in_progress[message.worker] = task

    def task_done(self, results):
        task = self.in_progress.pop(results.worker)
        task.set_results(results)
        self.context.acknowledge_task()

    def inspect_tasks(self):
        if timestamp - self.last_inspection >= SLEEP_UNIT:
            self.inspection()
            self.last_inspection = timestamp

    def inspection(self):
        for worker_id, task in self.in_progress.items():
            if self.has_timeout(task):
                self.task_problem(worker_id, TimeoutError('Task timeout'))
            elif task.cancelled:
                self.task_problem(worker_id, TaskCancelled('Task cancelled'))

    def task_problem(self, worker_id, error):
        task = self.in_progress.pop(worker_id)
        context.workers_manager.stop_worker(worker_id)
        task.set_results(error)
        self.context.acknowledge_task()

    @staticmethod
    def has_timeout(task):
        if task.timeout:
            return time.time() - task._timestamp > task.timeout
        else:
            return False


class ProcessWorkersManager(object):
    def __init__(self):
        self.workers = {}
        self.pool_channel, self.workers_channel = channels()

    def schedule(self, task):
        self.workers_channel.send(task._metadata)

    def create_workers(self, workers, task_limit, initializer, initargs):
        self.worker_parameters = WorkerParameters(task_limit, initializer,
                                                  initargs, None, None)
        for _ in range(workers):
            self.new_worker()

    def new_worker(self):
        worker = worker_process(self.worker_parameters, self.workers_channel)
        self.workers[worker.pid] = worker

    def inspect_workers(self):
        for worker_id, worker in self.workers.items():
            if not worker.is_alive():
                self.expired_worker(worker_id)

    def expired_worker(self, worker_id):
        worker = self.workers.pop(worker_id)
        self.inspect_expiration(worker)
        self.new_worker()

    def inspect_expiration(self, worker):
        if worker.exitcode != os.EX_OK:
            results = ProcessExpired('Abnormal termination')
            results.exitcode = self.process_worker.exitcode
            self.workers_channel.send(Results(worker.pid, results))

    def stop_workers(self):
        for worker_id in self.workers.keys():
            self.stop_worker(worker_id)

    def stop_worker(self, worker_id):
        with self.workers_channel.lock:
            worker = self.workers.pop(worker_id)
            stop(worker)


def channels():
    read0, write0 = Pipe()
    read1, write1 = Pipe()

    return Channel(read1, write0), WorkerChannel(read0, write1)


class Channel(object):
    def __init__(self, reader, writer):
        self.reader = reader
        self.writer = writer

    def poll(self, timeout=None):
        return self.reader.poll(timeout)

    def recv(self):
        return self.reader.recv()

    def send(self, obj):
        return self.writer.send(obj)


class WorkerChannel(Channel):
    def __init__(self, reader, writer):
        super(WorkerChannel, self).__init__(reader, writer)
        self.reader_mutex = Lock()
        self.writer_mutex = os.name != 'nt' and Lock() or None
        self.recv = self._make_recv_method()
        self.send = self._make_send_method()

    def __getstate__(self):
        return (self.reader, self.writer,
                self.reader_mutex, self.writer_mutex)

    def __setstate__(self, state):
        (self.reader, self.writer,
         self.reader_mutex, self.writer_mutex) = state

        self.recv = self._make_recv_method()
        self.send = self._make_send_method()

    def _make_recv_method(self):
        def recv():
            with self.rlock:
                return self.reader.recv()

        return recv

    def _make_send_method(self):
        def send(obj):
            if self.wlock is not None:
                with self.wlock:
                    return self.writer.send(obj)
            else:
                return self.writer.send(obj)

        return send

    @contextmanager
    def lock(self):
        with self.reader_mutex:
            if self.writer_mutex is not None:
                with self.writer_mutex:
                    yield self
            else:
                yield self


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
    channel.send(Acknowledgement(os.getpid(), id(task)))

    return task


def execute_next_task(task):
    function, args, kwargs = task
    return execute(function, args, kwargs)
