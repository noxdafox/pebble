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

import os
import time
import atexit
import signal
import pickle
import multiprocessing

from itertools import count
from dataclasses import dataclass
from typing import Any, Callable, Optional
from concurrent.futures.process import BrokenProcessPool
from concurrent.futures import CancelledError, TimeoutError

from pebble.pool.base_pool import Worker, iter_chunks, run_initializer
from pebble.pool.base_pool import PoolContext, BasePool, Task, TaskPayload
from pebble.pool.base_pool import PoolStatus, ProcessMapFuture, map_results
from pebble.pool.channel import ChannelError, WorkerChannel, channels
from pebble.common import Result, ResultStatus, CONSTS
from pebble.common import launch_process, stop_process
from pebble.common import ProcessExpired, ProcessFuture
from pebble.common import process_execute, launch_thread


class ProcessPool(BasePool):
    """Allows to schedule jobs within a Pool of Processes.

    max_workers is an integer representing the amount of desired process workers
    managed by the pool.
    If max_tasks is a number greater than zero,
    each worker will be restarted after performing an equal amount of tasks.

    initializer must be callable, if passed, it will be called
    every time a worker is started, receiving initargs as arguments.

    The context parameter can be used to specify the multiprocessing.context object
    used for starting the worker processes.

    """

    def __init__(self, max_workers: int = multiprocessing.cpu_count(),
                 max_tasks: int = 0,
                 initializer: Callable = None,
                 initargs: list = (),
                 context: multiprocessing.context.BaseContext = multiprocessing):
        super().__init__(max_workers, max_tasks, initializer, initargs)
        self._pool_manager = PoolManager(self._context, context)
        self._task_scheduler_loop = None
        self._pool_manager_loop = None
        self._message_manager_loop = None

    def _start_pool(self):
        with self._context.status_mutex:
            if self._context.status == PoolStatus.CREATED:
                self._pool_manager.start()

                self._task_scheduler_loop = launch_thread(
                    None, task_scheduler_loop, True, self._pool_manager)
                self._pool_manager_loop = launch_thread(
                    None, pool_manager_loop, True, self._pool_manager)
                self._message_manager_loop = launch_thread(
                    None, message_manager_loop, True, self._pool_manager)

                self._context.status = PoolStatus.RUNNING

    def _stop_pool(self):
        if self._pool_manager_loop is not None:
            self._pool_manager_loop.join()
        self._pool_manager.stop()
        if self._task_scheduler_loop is not None:
            self._task_scheduler_loop.join()
        if self._message_manager_loop is not None:
            self._message_manager_loop.join()

    def schedule(self, function: Callable,
                 args: list = (),
                 kwargs: dict = {},
                 timeout: float = None) -> ProcessFuture:
        """Schedules *function* to be run the Pool.

        *args* and *kwargs* will be forwareded to the scheduled function
        respectively as arguments and keyword arguments.

        *timeout* is an integer, if expires the task will be terminated
        and *Future.result()* will raise *TimeoutError*.

        A *pebble.ProcessFuture* object is returned.

        """
        self._check_pool_status()

        future = ProcessFuture()
        payload = TaskPayload(function, args, kwargs)
        task = Task(next(self._task_counter), future, timeout, payload)

        self._context.task_queue.put(task)

        return future

    def submit(self, function: Callable,
               timeout: Optional[float],
               /, *args, **kwargs) -> ProcessFuture:
        """This function is provided for compatibility with
        `asyncio.loop.run_in_executor`.

        For scheduling jobs within the pool use `schedule` instead.

        """
        return self.schedule(
            function, args=args, kwargs=kwargs, timeout=timeout)

    def map(self, function: Callable,
            *iterables, **kwargs) -> ProcessMapFuture:
        """Computes the *function* using arguments from
        each of the iterables. Stops when the shortest iterable is exhausted.

        *timeout* is an integer, if expires the task will be terminated
        and the call to next will raise *TimeoutError*.
        The *timeout* is applied to each chunk of the iterable.

        *chunksize* controls the size of the chunks the iterable will
        be broken into before being passed to the function.

        A *pebble.ProcessFuture* object is returned.

        """
        self._check_pool_status()

        timeout = kwargs.get('timeout')
        chunksize = kwargs.get('chunksize', 1)

        if chunksize < 1:
            raise ValueError("chunksize must be >= 1")

        futures = [self.schedule(
            process_chunk, args=(function, chunk), timeout=timeout)
            for chunk in iter_chunks(zip(*iterables), chunksize)]

        return map_results(ProcessMapFuture(futures), timeout)


def task_scheduler_loop(pool_manager: 'PoolManager'):
    context = pool_manager.context
    task_queue = context.task_queue

    try:
        while context.alive and not GLOBAL_SHUTDOWN:
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
        context.status = PoolStatus.ERROR


def pool_manager_loop(pool_manager: 'PoolManager'):
    context = pool_manager.context

    try:
        while context.alive and not GLOBAL_SHUTDOWN:
            pool_manager.update_status()
            time.sleep(CONSTS.sleep_unit)
    except BrokenProcessPool:
        context.status = PoolStatus.ERROR


def message_manager_loop(pool_manager: 'PoolManager'):
    context = pool_manager.context

    try:
        while context.alive and not GLOBAL_SHUTDOWN:
            pool_manager.process_next_message(CONSTS.sleep_unit)
    except BrokenProcessPool:
        context.status = PoolStatus.ERROR


class PoolManager:
    """Combines Task and Worker Managers providing a higher level one."""
    def __init__(self, context: PoolContext,
                 mp_context: multiprocessing.context.BaseContext):
        self.context = context
        self.task_manager = TaskManager(context.task_queue.task_done)
        self.worker_manager = WorkerManager(context.workers,
                                            context.worker_parameters,
                                            mp_context)

    def start(self):
        self.worker_manager.create_workers()

    def stop(self):
        self.worker_manager.close_channels()
        self.worker_manager.stop_workers()

    def schedule(self, task: Task):
        """Schedules a new Task in the PoolManager."""
        self.task_manager.register(task)
        try:
            self.worker_manager.dispatch(task)
        except (pickle.PicklingError, TypeError) as error:
            self.task_manager.task_problem(task.id, error)

    def process_next_message(self, timeout: float):
        """Processes the next message coming from the workers."""
        message = self.worker_manager.receive(timeout)

        if isinstance(message, Acknowledgement):
            self.task_manager.task_start(message.task, message.worker)
        elif isinstance(message, TaskResult):
            self.task_manager.task_done(message.task, message.result)
        elif isinstance(message, TaskProblem):
            self.task_manager.task_problem(message.task, message.error)

    def update_status(self):
        self.update_tasks()
        self.update_workers()

    def update_tasks(self):
        """Handles timing out Tasks."""
        for task in self.task_manager.timeout_tasks():
            self.worker_manager.stop_worker(task.worker_id)
            self.task_manager.task_done(
                task.id,
                Result(ResultStatus.FAILURE,
                       TimeoutError("Task timeout", task.timeout)))

        for task in self.task_manager.cancelled_tasks():
            self.worker_manager.stop_worker(task.worker_id)
            self.task_manager.task_done(
                task.id, Result(ResultStatus.FAILURE, CancelledError()))

    def update_workers(self):
        """Handles unexpected processes termination."""
        for expiration in self.worker_manager.inspect_workers():
            self.handle_worker_expiration(expiration)

        self.worker_manager.create_workers()

    def handle_worker_expiration(self, expiration: tuple):
        worker_id, exitcode = expiration

        try:
            task = self.find_expired_task(worker_id)
        except LookupError:
            return
        else:
            error = ProcessExpired('Abnormal termination', code=exitcode, pid=worker_id)
            self.task_manager.task_done(
                task.id, Result(ResultStatus.ERROR, error))

    def find_expired_task(self, worker_id: int) -> Task:
        tasks = dictionary_values(self.task_manager.tasks)
        running_tasks = tuple(t for t in tasks if t.worker_id != 0)
        if running_tasks:
            return task_worker_lookup(running_tasks, worker_id)

        raise BrokenProcessPool("All workers expired")


class TaskManager:
    """Manages the tasks flow within the Pool.

    Tasks are registered, acknowledged and completed.
    Timing out and cancelled tasks are handled as well.
    """

    def __init__(self, task_done_callback: Callable):
        self.tasks = {}
        self.task_done_callback = task_done_callback

    def register(self, task: Task):
        self.tasks[task.id] = task

    def task_start(self, task_id: int, worker_id: Optional[int]):
        task = self.tasks[task_id]
        task.worker_id = worker_id
        task.timestamp = time.time()
        task.set_running_or_notify_cancel()

    def task_done(self, task_id: int, result: Result):
        """Set the tasks result and run the callback."""
        try:
            task = self.tasks.pop(task_id)
        except KeyError:
            return  # result of previously timeout Task
        else:
            if task.future.cancelled():
                task.set_running_or_notify_cancel()
            elif result.status == ResultStatus.SUCCESS:
                task.future.set_result(result.value)
            else:
                task.future.set_exception(result.value)

            self.task_done_callback()

    def task_problem(self, task_id: int, error: Exception):
        """Set the task with the error it caused within the Pool."""
        self.task_start(task_id, None)
        self.task_done(task_id, Result(ResultStatus.ERROR, error))

    def timeout_tasks(self) -> tuple:
        return tuple(t for t in dictionary_values(self.tasks)
                     if self.timeout(t))

    def cancelled_tasks(self) -> tuple:
        return tuple(t for t in dictionary_values(self.tasks)
                     if t.timestamp != 0 and t.future.cancelled())

    @staticmethod
    def timeout(task: Task) -> bool:
        if task.timeout and task.started:
            return time.time() - task.timestamp > task.timeout
        else:
            return False


class WorkerManager:
    """Manages the workers related mechanics within the Pool.

    Maintains the workers active and encapsulates their communication logic.
    """

    def __init__(self, workers:int,
                 worker_parameters: Worker,
                 mp_context: multiprocessing.context.BaseContext):
        self.workers = {}
        self.workers_number = workers
        self.worker_parameters = worker_parameters
        self.pool_channel, self.workers_channel = channels(mp_context)
        self.mp_context = mp_context

    def dispatch(self, task: Task):
        try:
            self.pool_channel.send(WorkerTask(task.id, task.payload))
        except (pickle.PicklingError, TypeError) as error:
            raise error
        except OSError as error:
            raise BrokenProcessPool from error

    def receive(self, timeout: float):
        try:
            if self.pool_channel.poll(timeout):
                return self.pool_channel.recv()
            else:
                return NoMessage()
        except (OSError, TypeError) as error:
            raise BrokenProcessPool from error
        except EOFError:  # Pool shutdown
            return NoMessage()

    def inspect_workers(self) -> tuple:
        """Updates the workers status.

        Returns the workers which have unexpectedly ended.

        """
        expired = tuple(w for w in dictionary_values(self.workers)
                        if not w.is_alive())

        for worker in expired:
            self.workers.pop(worker.pid)

        return tuple((w.pid, w.exitcode) for w in expired if w.exitcode != 0)

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
                WORKERS_NAME, worker_process, False, self.mp_context,
                self.worker_parameters, self.workers_channel)
            self.workers[worker.pid] = worker
        except OSError as error:
            raise BrokenProcessPool from error

    def stop_worker(self, worker_id: int, force=False):
        try:
            if force:
                stop_process(self.workers.pop(worker_id))
            else:
                with self.workers_channel.lock:
                    stop_process(self.workers.pop(worker_id))
        except ChannelError as error:
            raise BrokenProcessPool from error
        except KeyError:
            return  # worker already expired


def worker_process(params: Worker, channel: WorkerChannel):
    """The worker process routines."""
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, signal.SIG_DFL)

    channel.initialize()

    if params.initializer is not None:
        if not run_initializer(params.initializer, params.initargs):
            os._exit(1)

    try:
        for task in worker_get_next_task(channel, params.max_tasks):
            payload = task.payload
            result = process_execute(
                payload.function, *payload.args, **payload.kwargs)
            send_result(channel, TaskResult(task.id, result))
    except (OSError, RuntimeError) as error:
        errno = getattr(error, 'errno', 1)
        os._exit(errno if isinstance(errno, int) else 1)
    except EOFError as error:
        os._exit(0)


def worker_get_next_task(channel: WorkerChannel, max_tasks: int):
    counter = count()

    while max_tasks == 0 or next(counter) < max_tasks:
        yield fetch_task(channel)


def send_result(channel: WorkerChannel, result: Any):
    """Send result handling pickling and communication errors."""
    try:
        channel.send(result)
    except (pickle.PicklingError, TypeError) as error:
        channel.send(TaskProblem(result.task, error))


def fetch_task(channel: WorkerChannel) -> Task:
    while channel.poll():
        try:
            return task_transaction(channel)
        except RuntimeError:
            continue  # another worker got the task


def task_transaction(channel: WorkerChannel) -> Task:
    """Ensures a task is fetched and acknowledged atomically."""
    with channel.lock:
        if channel.poll(0):
            task = channel.recv()
            channel.send(Acknowledgement(os.getpid(), task.id))
        else:
            raise RuntimeError("Race condition between workers")

    return task


def task_worker_lookup(running_tasks: tuple, worker_id: int) -> Task:
    for task in running_tasks:
        if task.worker_id == worker_id:
            return task

    raise LookupError("Not found")


def process_chunk(function: Callable, chunk: list) -> list:
    """Processes a chunk of the iterable passed to map dealing with errors."""
    return [process_execute(function, *args) for args in chunk]


def interpreter_shutdown():
    global GLOBAL_SHUTDOWN
    GLOBAL_SHUTDOWN = True

    workers = [p for p in multiprocessing.active_children()
               if p.name == WORKERS_NAME]

    for worker in workers:
        stop_process(worker)


def dictionary_values(dictionary: dict) -> tuple:
    """Returns a snapshot of the dictionary values handling race conditions."""
    while True:
        try:
            return tuple(dictionary.values())
        except RuntimeError:  # race condition
            pass


atexit.register(interpreter_shutdown)


GLOBAL_SHUTDOWN = False
WORKERS_NAME = 'pebble_pool_worker'


@dataclass
class NoMessage:
    pass


@dataclass
class TaskResult:
    """The result of a Task."""
    task: id
    result: Any


@dataclass
class TaskProblem:
    """Issue occurred within a Task."""
    task: id
    error: BaseException


@dataclass
class WorkerTask:
    """A Task assigned to a worker."""
    id: id
    payload: TaskPayload


@dataclass
class Acknowledgement:
    """Ack from a worker of a received Task."""
    worker: id
    task: id
