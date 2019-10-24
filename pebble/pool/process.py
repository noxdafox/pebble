# This file is part of Pebble.
# Copyright (c) 2013-2019, Matteo Cafasso

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
from multiprocessing import cpu_count
from signal import SIG_IGN, SIGINT, signal
from concurrent.futures import CancelledError, TimeoutError
try:
    from concurrent.futures.process import BrokenProcessPool
except ImportError:
    class BrokenProcessPool(OSError):
        pass

from pebble.pool.channel import ChannelError, channels
from pebble.pool.base_pool import BasePool, Task, TaskPayload
from pebble.pool.base_pool import ProcessMapFuture, MapResults
from pebble.pool.base_pool import iter_chunks, run_initializer
from pebble.pool.base_pool import CREATED, ERROR, RUNNING, SLEEP_UNIT
from pebble.common import launch_process, stop_process
from pebble.common import ProcessExpired, ProcessFuture
from pebble.common import process_execute, launch_thread, send_result


class ProcessPool(BasePool):
    """Allows to schedule jobs within a Pool of Processes.

    max_workers is an integer representing the amount of desired process workers
    managed by the pool.
    If max_tasks is a number greater than zero,
    each worker will be restarted after performing an equal amount of tasks.

    initializer must be callable, if passed, it will be called
    every time a worker is started, receiving initargs as arguments.

    """

    def __init__(self, max_workers=cpu_count(), max_tasks=0,
                 initializer=None, initargs=()):
        super(ProcessPool, self).__init__(
            max_workers, max_tasks, initializer, initargs)
        self._pool_manager = PoolManager(self._context)

    def _start_pool(self):
        with self._context.state_mutex:
            if self._context.state == CREATED:
                self._pool_manager.start()
                self._loops = (launch_thread(None, task_scheduler_loop,
                                             self._pool_manager),
                               launch_thread(None, pool_manager_loop,
                                             self._pool_manager),
                               launch_thread(None, message_manager_loop,
                                             self._pool_manager))
                self._context.state = RUNNING

    def schedule(self, function, args=(), kwargs={}, timeout=None):
        """Schedules *function* to be run the Pool.

        *args* and *kwargs* will be forwareded to the scheduled function
        respectively as arguments and keyword arguments.

        *timeout* is an integer, if expires the task will be terminated
        and *Future.result()* will raise *TimeoutError*.

        A *pebble.ProcessFuture* object is returned.

        """
        self._check_pool_state()

        future = ProcessFuture()
        payload = TaskPayload(function, args, kwargs)
        task = Task(next(self._task_counter), future, timeout, payload)

        self._context.task_queue.put(task)

        return future

    def map(self, function, *iterables, **kwargs):
        """Computes the *function* using arguments from
        each of the iterables. Stops when the shortest iterable is exhausted.

        *timeout* is an integer, if expires the task will be terminated
        and the call to next will raise *TimeoutError*.
        The *timeout* is applied to each chunk of the iterable.

        *chunksize* controls the size of the chunks the iterable will
        be broken into before being passed to the function.

        A *pebble.ProcessFuture* object is returned.

        """
        self._check_pool_state()

        timeout = kwargs.get('timeout')
        chunksize = kwargs.get('chunksize', 1)

        if chunksize < 1:
            raise ValueError("chunksize must be >= 1")

        futures = [self.schedule(
            process_chunk, args=(function, chunk), timeout=timeout)
            for chunk in iter_chunks(chunksize, *iterables)]

        map_future = ProcessMapFuture(futures)
        if not futures:
            map_future.set_result(MapResults(futures))
            return map_future

        def done_map(_):
            if not map_future.done():
                map_future.set_result(MapResults(futures))

        for future in futures:
            future.add_done_callback(done_map)
            setattr(future, 'map_future', map_future)

        return map_future

    def stop_worker(self, worker_id, force=False):
        """Stops worker (if it's hung for example)."""
        self._pool_manager.worker_manager.stop_worker(worker_id, force)


def task_scheduler_loop(pool_manager):
    context = pool_manager.context
    task_queue = context.task_queue

    try:
        while context.alive:
            task = task_queue.get()

            if task is not None:
                if task.future.cancelled():
                    task.set_running_or_notify_cancel()
                    task_queue.task_done()
                else:
                    pool_manager.schedule(task)
            else:
                task_queue.task_done()
    except BrokenProcessPool:
        context.state = ERROR


def pool_manager_loop(pool_manager):
    context = pool_manager.context

    try:
        while context.alive:
            pool_manager.update_status()
            time.sleep(SLEEP_UNIT)
    except BrokenProcessPool:
        context.state = ERROR


def message_manager_loop(pool_manager):
    context = pool_manager.context

    try:
        while context.alive:
            pool_manager.process_next_message(SLEEP_UNIT)
    except BrokenProcessPool:
        context.state = ERROR


class PoolManager:
    """Combines Task and Worker Managers providing a higher level one."""

    def __init__(self, context):
        self.context = context
        self.task_manager = TaskManager(context.task_queue.task_done)
        self.worker_manager = WorkerManager(context.workers,
                                            context.worker_parameters)

    def start(self):
        self.worker_manager.create_workers()

    def stop(self):
        self.worker_manager.stop_workers()
        self.worker_manager.close_channels()

    def schedule(self, task):
        """Schedules a new Task in the PoolManager."""
        self.task_manager.register(task)
        self.worker_manager.dispatch(task)

    def process_next_message(self, timeout):
        """Processes the next message coming from the workers."""
        message = self.worker_manager.receive(timeout)

        if isinstance(message, Acknowledgement):
            self.task_manager.task_start(message.task, message.worker)
        elif isinstance(message, Result):
            self.task_manager.task_done(message.task, message.result)

    def update_status(self):
        self.update_tasks()
        self.update_workers()

    def update_tasks(self):
        """Handles timing out Tasks."""
        for task in self.task_manager.timeout_tasks():
            self.task_manager.task_done(
                task.id, TimeoutError("Task timeout", task.timeout))
            self.worker_manager.stop_worker(task.worker_id)

        for task in self.task_manager.cancelled_tasks():
            self.task_manager.task_done(
                task.id, CancelledError())
            self.worker_manager.stop_worker(task.worker_id)

    def update_workers(self):
        """Handles unexpected processes termination."""
        for expiration in self.worker_manager.inspect_workers():
            self.handle_worker_expiration(expiration)

        self.worker_manager.create_workers()

    def handle_worker_expiration(self, expiration):
        worker_id, exitcode = expiration

        try:
            task = self.find_expired_task(worker_id)
        except LookupError:
            return
        else:
            error = ProcessExpired('Abnormal termination', code=exitcode)
            self.task_manager.task_done(task.id, error)

    def find_expired_task(self, worker_id):
        tasks = tuple(self.task_manager.tasks.values())
        running_tasks = tuple(t for t in tasks if t.worker_id != 0)

        if running_tasks:
            return task_worker_lookup(running_tasks, worker_id)
        else:
            raise BrokenProcessPool("All workers expired")


class TaskManager:
    """Manages the tasks flow within the Pool.

    Tasks are registered, acknowledged and completed.
    Timing out and cancelled tasks are handled as well.
    """

    def __init__(self, task_done_callback):
        self.tasks = {}
        self.task_done_callback = task_done_callback

    def register(self, task):
        self.tasks[task.id] = task

    def task_start(self, task_id, worker_id):
        task = self.tasks[task_id]
        task.worker_id = worker_id
        task.timestamp = time.time()
        task.set_running_or_notify_cancel()

    def task_done(self, task_id, result):
        """Set the tasks result and run the callback."""
        try:
            task = self.tasks.pop(task_id)
        except KeyError:
            return  # result of previously timeout Task
        else:
            if task.future.cancelled():
                task.set_running_or_notify_cancel()
            elif isinstance(result, BaseException):
                task.future.set_exception(result)
            else:
                task.future.set_result(result)

            self.task_done_callback()

    def timeout_tasks(self):
        return tuple(t for t in tuple(self.tasks.values()) if self.timeout(t))

    def cancelled_tasks(self):
        return tuple(t for t in tuple(self.tasks.values())
                     if t.timestamp != 0 and t.future.cancelled())

    @staticmethod
    def timeout(task):
        if task.timeout and task.started:
            return time.time() - task.timestamp > task.timeout
        else:
            return False


class WorkerManager:
    """Manages the workers related mechanics within the Pool.

    Maintains the workers active and encapsulates their communication logic.
    """

    def __init__(self, workers, worker_parameters):
        self.workers = {}
        self.workers_number = workers
        self.worker_parameters = worker_parameters
        self.pool_channel, self.workers_channel = channels()

    def dispatch(self, task):
        try:
            self.pool_channel.send(WorkerTask(task.id, task.payload))
        except (OSError, EnvironmentError, TypeError) as error:
            raise BrokenProcessPool(error)

    def receive(self, timeout):
        try:
            if self.pool_channel.poll(timeout):
                return self.pool_channel.recv()
            else:
                return NoMessage()
        except (OSError, EnvironmentError) as error:
            raise BrokenProcessPool(error)

    def inspect_workers(self):
        """Updates the workers status.

        Returns the workers which have unexpectedly ended.

        """
        workers = tuple(self.workers.values())
        expired = tuple(w for w in workers if not w.is_alive())

        for worker in expired:
            self.workers.pop(worker.pid)

        return ((w.pid, w.exitcode) for w in expired if w.exitcode != 0)

    def create_workers(self):
        for _ in range(self.workers_number - len(self.workers)):
            self.new_worker()

    def close_channels(self):
        self.pool_channel.close()
        self.workers_channel.close()

    def stop_workers(self):
        for worker_id in tuple(self.workers.keys()):
            self.stop_worker(worker_id, force=True)

    def new_worker(self):
        try:
            worker = launch_process(
                None, worker_process, self.worker_parameters, self.workers_channel)
            self.workers[worker.pid] = worker
        except (OSError, EnvironmentError) as error:
            raise BrokenProcessPool(error)

    def stop_worker(self, worker_id, force=False):
        try:
            if force:
                stop_process(self.workers.pop(worker_id))
            else:
                with self.workers_channel.lock:
                    stop_process(self.workers.pop(worker_id))
        except ChannelError as error:
            raise BrokenProcessPool(error)
        except KeyError:
            return  # worker already expired


def worker_process(params, channel):
    """The worker process routines."""
    signal(SIGINT, SIG_IGN)

    if params.initializer is not None:
        if not run_initializer(params.initializer, params.initargs):
            os._exit(1)

    try:
        for task in worker_get_next_task(channel, params.max_tasks):
            payload = task.payload
            result = process_execute(
                payload.function, *payload.args, **payload.kwargs)
            send_result(channel, Result(task.id, result))
    except (EnvironmentError, OSError, RuntimeError) as error:
        os._exit(error.errno if error.errno else 1)
    except EOFError:
        os._exit(0)


def worker_get_next_task(channel, max_tasks):
    counter = count()

    while max_tasks == 0 or next(counter) < max_tasks:
        yield fetch_task(channel)


def fetch_task(channel):
    while channel.poll():
        try:
            return task_transaction(channel)
        except RuntimeError:
            continue  # another worker got the task


def task_transaction(channel):
    """Ensures a task is fetched and acknowledged atomically."""
    with channel.lock:
        if channel.poll(0):
            task = channel.recv()
            channel.send(Acknowledgement(os.getpid(), task.id))
        else:
            raise RuntimeError("Race condition between workers")

    return task


def task_worker_lookup(running_tasks, worker_id):
    for task in running_tasks:
        if task.worker_id == worker_id:
            return task

    raise LookupError("Not found")


def process_chunk(function, chunk):
    """Processes a chunk of the iterable passed to map dealing with errors."""
    return [process_execute(function, *args) for args in chunk]


NoMessage = namedtuple('NoMessage', ())
Result = namedtuple('Result', ('task', 'result'))
WorkerTask = namedtuple('WorkerTask', ('id', 'payload'))
Acknowledgement = namedtuple('Acknowledgement', ('worker', 'task'))
