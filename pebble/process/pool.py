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
from collections import deque
from signal import SIG_IGN, SIGINT, signal
from traceback import format_exc
try:  # Python 2
    from Queue import Empty
    from cPickle import PicklingError
except:  # Python 3
    from queue import Empty
    from pickle import PicklingError

from .connection import pipe
from .generic import channels, lock, stop_worker
from .spawn import spawn as spawn_process
from ..thread import spawn as spawn_thread
from ..pebble import BasePool, PoolContext
from ..pebble import STOPPED, RUNNING, ERROR
from ..pebble import Task, TimeoutError, TaskCancelled


@asyncio.coroutine
def worker_connect(tasks, results):
    yield from tasks.connect()
    yield from results.connect()


@asyncio.coroutine
def worker_loop(tasks, results, limit):
    counter = count()

    while limit == 0 or next(counter) < limit:
        error = None
        value = None
        function, args, kwargs = yield from tasks.recv()

        try:
            value = function(*args, **kwargs)
        except Exception as err:
            if error is None:
                error = err
                error.traceback = format_exc()

        try:
            results.send(error is not None and error or value)
        except PicklingError as error:
            results.send(error)


@spawn_process(name='pool_worker', daemon=True)
def pool_worker(tasks, results, initializer, initargs, limit):
    """Runs the actual function in separate process."""
    signal(SIGINT, SIG_IGN)

    old_loop = asyncio.get_event_loop()
    old_loop.stop()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    loop.run_until_complete(worker_connect(tasks, results))

    if initializer is not None:
        try:
            initializer(*initargs)
        except Exception as error:
            error.traceback = format_exc()
            results.send(error is not None and error or value)

            os._exit(0)

    loop.run_until_complete(worker_loop(tasks, results, limit))

    # if deinitializer is not None:
    #     try:
    #         deinitializer(*deinitargs)
    #     except Exception as error:
    #         print_exc()

    os._exit(0)


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
        expired = [w for w in pool if w.expired]

        for worker in expired:
            worker.join()
            pool.remove(worker)
            while worker.tasks:
                queue.put(worker.cancel())

        for _ in range(workers - len(pool)):
            worker = Worker(initializer, initargs, limit)
            yield from worker.start()
            pool.append(worker)

        yield from asyncio.sleep(0.3)


@asyncio.coroutine
def task_scheduler(context):
    """Schedules enqueued tasks to the workers."""
    pool = context.pool
    queue = context.queue

    while context.state not in (ERROR, STOPPED):
        workers = [w for w in pool if not w.closed]

        if workers:
            try:
                for worker in workers:
                    task = queue.get_nowait()
                    try:
                        worker.schedule(task)
                    except (IOError, OSError):
                        queue.put(task)
            except Empty:
                yield from asyncio.sleep(0.3)
        else:
            yield from asyncio.sleep(0.1)


@asyncio.coroutine
def result_manager(context):
    """Manages results coming from the Workers."""
    pending = []
    pool = context.pool
    task_done = context.task_done

    while context.state not in (ERROR, STOPPED):
        workers = [w for w in pool if not w.expired and
                   w not in [p[0] for p in pending]]

        for worker in workers:
            future = asyncio.Future()
            asyncio.async(worker.result(future))
            future.add_done_callback(task_done)
            pending.append((worker, future))

        if pending:
            futures = [p[1] for p in pending]
            done, _ = yield from asyncio.wait(
                futures, timeout=0.3, return_when=asyncio.FIRST_COMPLETED)
            pending = [p for p in pending if p[1] not in done]
        else:
            yield from asyncio.sleep(0.3)


@asyncio.coroutine
def timeout_manager(context):
    pool = context.pool

    while context.state not in (ERROR, STOPPED):
        now = time()
        workers = [w for w in pool
                   if not w.expired and w.current is not None]

        for worker in workers:
            task = worker.current
            if (task.timeout > 0 and now - task._timestamp > task.timeout):
                worker.stop()
                worker.cancel()
                done(task, TimeoutError('Task timeout'))

        yield from asyncio.sleep(0.1)


@asyncio.coroutine
def cancellation_manager(context):
    pool = context.pool

    while context.state not in (ERROR, STOPPED):
        workers = [w for w in pool
                   if not w.expired and w.current is not None]

        for worker in workers:
            task = worker.current
            if task.cancelled:
                worker.stop()
                worker.cancel()
                done(task, TaskCancelled('Task cancelled'))

        yield from asyncio.sleep(0.3)


@asyncio.coroutine
def expiration_manager(context):
    pool = context.pool

    while context.state not in (ERROR, STOPPED):
        workers = [w for w in pool
                   if not w.expired and w.current is not None]

        for worker in workers:
            task = worker.current
            if not worker.alive and not worker.closed:
                worker.cancel()
                done(task, ProcessExpired('Process expired'))

        yield from asyncio.sleep(0.3)


@spawn_thread(name='pool_manager', daemon=True)
def pool_manager(context):
    """Schedules enqueued tasks to the workers."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    context.loop = loop

    asyncio.async(worker_manager(context), loop=loop)
    asyncio.async(task_scheduler(context), loop=loop)
    asyncio.async(result_manager(context), loop=loop)
    asyncio.async(timeout_manager(context), loop=loop)
    asyncio.async(cancellation_manager(context), loop=loop)
    asyncio.async(expiration_manager(context), loop=loop)

    loop.run_forever()


class Worker(object):
    def __init__(self, initializer, initargs, limit):
        self.initializer = initializer
        self.initargs = initargs
        self.limit = limit
        self.counter = count()
        self.tasks = deque()
        self.process = None
        self.task_reader, self.task_writer = pipe()
        self.result_reader, self.result_writer = pipe()

    @property
    def alive(self):
        return self.process.is_alive()

    @property
    def current(self):
        return self.tasks and self.tasks[0] or None

    @property
    def closed(self):
        return not self.task_writer.writable

    @property
    def expired(self):
        return self.result_reader.closed

    @asyncio.coroutine
    def result(self, future):
        try:
            data = yield from self.result_reader.recv()
            task = self.tasks.popleft()
            future.set_result((task, data))
        except (EOFError, IOError) as error:
            self.result_reader.close()
            raise

    @asyncio.coroutine
    def start(self):
        self.process = pool_worker(self.task_reader, self.result_writer,
                                   self.initializer, self.initargs, self.limit)
        self.task_reader.close()
        self.result_writer.close()
        yield from self.task_writer.connect()
        yield from self.result_reader.connect()

    def is_alive(self):
        return self.process.is_alive()

    def cancel(self):
        return self.tasks.popleft()

    def stop(self):
        stop_worker(self.process)
        self.task_writer.close()

    def join(self, timeout):
        self.process.join(timeout)

    def schedule(self, task):
        if not self.tasks:
            task._timestamp = time()
        self.tasks.append(task)

        try:
            self.task_writer.send((task._function, task._args, task._kwargs))
        except (IOError, OSError):
            task._timestamp = 0
            self.cancel()
            raise

        if self.limit and next(self.counter) == self.limit:
            self.task_writer.close()


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
        self.loop = None

    def task_done(self, future):
        task, results = future.result()
        task._set(results)

        if task._callback is not None:
            try:
                task._callback(task)
            except Exception:
                print_exc()
                self.state = ERROR

        self.queue.task_done()

    def stop(self):
        """Stop the workers."""
        for worker in self.pool:
            stop_worker(worker.process)


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

        self._context.loop.stop()
        self._manager.join()

        self._context.stop()

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
