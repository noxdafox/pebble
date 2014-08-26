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
import sys
import asyncio

from itertools import count
from time import sleep, time
from signal import SIG_IGN, SIGINT, signal
from traceback import format_exc
try:  # Python 2
    from cPickle import PicklingError
except:  # Python 3
    from pickle import PicklingError

from .generic import channels, lock, stop_worker
from .spawn import spawn as spawn_process
from ..thread import spawn as spawn_thread
from ..pebble import BasePool, PoolContext
from ..pebble import STOPPED, RUNNING, ERROR
from ..pebble import Task, TimeoutError, TaskCancelled


@spawn_process(name='pool_worker', daemon=True)
def pool_worker(tasks, results, initializer, initargs, limit):
    """Runs the actual function in separate process."""
    error = None
    value = None
    counter = count()
    pid = os.getpid()
    signal(SIGINT, SIG_IGN)

    if initializer is not None:
        try:
            initializer(*initargs)
        except Exception as err:
            error = err
            error.traceback = format_exc()

    while limit == 0 or next(counter) < limit:
        number, function, args, kwargs = get_task(channel, pid)

        try:
            value = function(*args, **kwargs)
        except Exception as err:
            if error is None:
                error = err
                error.traceback = format_exc()

        try:
            channel.send((RES, number, error is not None and error or value))
        except PicklingError as error:
            channel.send((RES, number, error))

        error = None
        value = None

    sys.exit(0)


@asyncio.coroutine
def worker_manager(context):
    """Collects expired workers and spawns new ones."""
    pool = context.pool
    queue = context.queue
    limit = context.worker_limit
    initializer = context.initializer
    initargs = context.initargs
    workers = context.worker_number

    while context.state not in (ERROR, STOPPED):
        expired = [w for w in pool if not w.is_alive()]

        for worker in expired:
            worker.join()
            pool.remove(worker)
            while not worker.tasks.empty():
                yield from queue.put(worker.cancel())

        for _ in range(workers - len(pool)):
            worker = Worker(initializer, initargs, limit)
            pool.append(worker)

        yield from asyncio.sleep(0.2)


@asyncio.coroutine
def task_scheduler(context):
    """Schedules enqueued tasks to the workers."""
    queue = context.queue

    while context.state not in (ERROR, STOPPED):
        workers = [w for w in pool if not w.closed]

        for worker in workers:
            task = yield from queue.get()

            try:
                worker.schedule(task)
            except (IOError, OSError):
                yield from queue.put(task)


@asyncio.coroutine
def result_manager(context):
    """Manages results coming from the Workers."""
    pending = []
    done = context.task_done

    while context.state not in (ERROR, STOPPED):
        workers = [w for w in pool if not w.expired]

        for worker in workers:
            future = asyncio.Future()
            asyncio.async(worker.result(future))
            future.add_done_callback(done)
            pending.append(future)

        _, pending = yield from asyncio.wait(
            pending, timeout=0.2, return_when=asyncio.FIRST_COMPLETED)


@asyncio.coroutine
def task_manager(context):
    """Manages running tasks.

    If a task's timeout expired it stops its worker and set it.
    If a task is cancelled it stops its worker and set it.
    If an expired worker has accidentally fetched a task it re-enqueues
    it in the Pool's queue.

    """
    pool = context.pool
    queue = context.queue
    tasks = context.tasks

    while context.state not in (ERROR, STOPPED):
        now = time()
        workers = [w for w in pool if not w.expired]

        for worker in workers:
            results = None
            task = worker.current

            if (task.timeout > 0 and now - task._timestamp > task.timeout):
                results = TimeoutError('Task timeout')
            elif task.cancelled:
                results = TaskCancelled('Task cancelled')
            elif not worker.alive and not worker.closed:
                results = ProcessExpired('Process expired')

            if results is not None:
                worker.stop()
                worker.cancel()
                done(task, results)

        yield from asyncio.sleep(0.2)


@spawn_thread(name='pool_manager', daemon=True)
def pool_manager(context):
    """Schedules enqueued tasks to the workers."""
    loop = asyncio.get_event_loop()

    loop.call_soon(workers_manager(context))
    loop.call_soon(task_scheduler(context))
    loop.call_soon(result_manager(context))
    loop.call_soon(task_manager(context))

    loop.run_forever()


class Worker(object):
    def __init__(self, initializer, initargs, limit):
        self.initializer = initializer
        self.initargs = initargs
        self.limit = limit
        self.counter = count()
        self.tasks = deque()

    @property
    def alive(self):
        return self.process.is_alive()

    @property
    def current(self):
        return self.tasks[0]

    @property
    def closed(self):
        return self.task_writer.closed

    @property
    def expired(self):
        return self.result_reader.closed

    def cancel(self):
        return self.tasks.popleft()

    def start(self):
        self.task_reader, self.task_writer = pipe()
        self.result_reader, self.result_writer = pipe()
        self.process = pool_worker(self.task_reader, self.result_writer,
                                   self.initializer, self.initargs, self.limit)
        self.task_reader.close()
        self.result_writer.close()

    def stop(self):
        stop_worker(self.process)
        self.task_writer.close()

    def schedule(self, task):
        if not self.tasks:
            task._timestamp = time()
        self.tasks.append(task)

        try:
            self.task_writer.send(task)
        except (IOError, OSError):
            task._timestamp = 0
            self.cancel()
            raise

        if next(self.counter) == limit:
            self.task_writer.close()

    @asyncio.coroutine
    def result(self, future):
        try:
            data = yield from self.result_reader.recv()
            task = self.tasks.popleft()
            future.set_result((task, data))
        except EOFError, IOError as error:
            self.result_reader.close()
            raise


class PoolTask(Task):
    """Extends the *Task* object to support *process* decorator."""
    def _cancel(self):
        """Overrides the *Task* cancel method."""
        self._cancelled = True


class Context(PoolContext):
    """Pool's Context."""
    def __init__(self, queue, queueargs, initializer, initargs,
                 workers, limit):
        super(Context, self).__init__(queue, queueargs,
                                      initializer, initargs,
                                      workers, limit)

    def stop(self):
        """Stop the workers."""
        for worker in self.pool:
            stop_worker(worker)


class Pool(BasePool):
    """A ProcessPool allows to schedule jobs into a Pool of Processes
    which will perform them concurrently.

    workers is an integer representing the amount of desired process workers
    managed by the pool.
    If worker_task_limit is a number greater than zero,
    each worker will be restarted after performing an equal amount of tasks.
    initializer must be callable, if passed, it will be called
    every time a worker is started, receiving initargs as arguments.
    queue represents a Class which, if passed, will be constructed
    with queueargs as parameters and used internally as a task queue.
    The queue object resulting from its construction must expose
    same functionalities of Python standard Queue object,
    especially for what concerns the put(), get() and join() methods.

    """
    def __init__(self, workers=1, task_limit=0, queue=None, queueargs=None,
                 initializer=None, initargs=()):
        super(Pool, self).__init__()
        self._context = Context(queue, queueargs, initializer, initargs,
                                workers, task_limit)

    def _start(self):
        """Start the Pool managers."""
        self._manager = pool_manager(self._context)
        self._context.state = RUNNING

    def stop(self):
        """Stops the pool without performing any pending task."""
        self._context.state = STOPPED

        self._context.queue.put(None)  # task_scheduler stop sentinel
        self._context.worker_channel.send(None)  # task_manager stop sentinel

        for manager in self._managers:
            manager.join()

        self._context.stop()

    def kill(self):
        """Kills the pool forcing all workers to terminate immediately."""
        self._context.state = STOPPED

        self._context.kill()

    def schedule(self, function, args=(), kwargs={}, identifier=None,
                 callback=None, timeout=0):
        """Schedules *function* into the Pool, passing *args* and *kwargs*
        respectively as arguments and keyword arguments.

        If *callback* is a callable it will be executed once the function
        execution has completed with the returned *Task* as a parameter.

        *timeout* is an integer, if expires the task will be terminated
        and *Task.get()* will raise *TimeoutError*.

        The *identifier* value will be forwarded to the *Task.id* attribute.

        A *Task* object is returned.

        """
        task = PoolTask(next(self._counter), function, args, kwargs,
                        callback, timeout, identifier)

        self._schedule(task)

        return task
