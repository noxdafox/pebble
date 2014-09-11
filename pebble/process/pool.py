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
from threading import Thread
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
from ..pebble import BasePool
from ..pebble import STOPPED, RUNNING, ERROR
from ..pebble import Task, TimeoutError, TaskCancelled


def cancel_future(future, *callbacks):
    for callback in callbacks:
        future.remove_done_callback(callback)
    future.cancel()


def pool_manager(loop, pool, queue, initializer, initargs, workers, limit):
    """Schedules enqueued tasks to the workers."""
    asyncio.set_event_loop(loop)
    asyncio.async(worker_manager(pool, queue, initializer, initargs,
                                 workers, limit), loop=loop)
    loop.run_forever()


@asyncio.coroutine
def connect(*channels):
    for channel in channels:
        yield from channel.connect()


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

    loop.run_until_complete(connect(tasks, results))

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
def worker_manager(pool, queue, initializer, initargs, workers, limit):
    """Collects expired workers and spawns new ones."""
    pool = pool
    worker_expired = asyncio.Event()
    worker_expired.set()

    while 1:
        yield from worker_expired.wait()
        worker_expired.clear()
        expired = [w for w in pool if w.expired]

        for worker in expired:
            worker.join()
            pool.remove(worker)
            while worker.tasks:
                yield from queue.put(worker.tasks.popleft())

        for _ in range(workers - len(pool)):
            worker = Worker(queue, limit, worker_expired)
            yield from worker.start(initializer, initargs)
            asyncio.async(worker.loop())
            pool.append(worker)


def join_workers(workers, timeout=None):
    """Joins pool's workers."""
    while len(workers) > 0 and (timeout is None or timeout > 0):
        for worker in workers[:]:
            worker.join(timeout is not None and 0.1 or None)
            if not worker.is_alive():
                workers.remove(worker)

        if timeout is not None:
            timeout = timeout - (len(workers) / 10.0)

    if len(workers) > 0:
        raise TimeoutError('Workers are still running')


class Worker(object):
    def __init__(self, queue, task_limit, expiration_event):
        self.queue = queue
        self.task_limit = task_limit
        self.task_sent = count()
        self.tasks = deque()
        self.process = None
        self.result_future = None
        self.timeout_future = None
        self.expiration = expiration_event
        self.task_reader, self.task_writer = pipe()
        self.result_reader, self.result_writer = pipe()

    @property
    def expired(self):
        return self.result_reader.closed

    @asyncio.coroutine
    def start(self, initializer, initargs):
        self.process = pool_worker(self.task_reader, self.result_writer,
                                   initializer, initargs, self.task_limit)
        self.task_reader.close()
        self.result_writer.close()
        yield from connect(self.task_writer, self.result_reader)

    @asyncio.coroutine
    def result(self):
        try:
            data = yield from self.result_reader.recv()
            self.result_future.set_result(data)
        except (EOFError, IOError) as error:
            self.result_future.set_result(ProcessExpired('Process expired'))
            self.stop()
        # finally:
        #     cancel_future(self.timeout_future, self.task_done)

    @asyncio.coroutine
    def timeout(self, timeout):
        yield from asyncio.sleep(timeout)

        self.stop()
        self.timeout_future.set_result(TimeoutError('Task timeout'))
        cancel_future(self.result_future, self.task_done)

    @asyncio.coroutine
    def loop(self):
        self.set_result_future()

        while self.task_limit == 0 or next(self.task_sent) < self.task_limit:
            task = yield from self.queue.get()

            # if not self.tasks and task.timeout is not None:
            #     self.set_timeout_future(task.timeout)
            self.tasks.append(task)

            data = (task._function, task._args, task._kwargs)
            self.task_writer.send(data)

    def set_result_future(self):
        self.result_future = asyncio.Future()
        self.result_future.add_done_callback(self.task_done)
        asyncio.async(self.result())

    def set_timeout_future(self, timeout):
        self.timeout_future = asyncio.Future()
        self.timeout_future.add_done_callback(self.task_done)
        asyncio.async(self.timeout(timeout))

    def task_done(self, future):
        task = self.tasks.popleft()
        results = future.result()
        task._set(results)

        if task._callback is not None:
            try:
                task._callback(task)
            except Exception:
                print_exc()
                self.state = ERROR

        self.queue.task_done()

        self.set_result_future()
        # if self.tasks and self.tasks[0].timeout is not None:
        #     self.set_timeout_future(self.tasks[0].timeout)

    def stop(self):
        stop_worker(self.process)
        self.expiration.set()


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
        super(Pool, self).__init__(queue, queueargs, initializer, initargs,
                                   workers, task_limit)
        self._queue = asyncio.JoinableQueue()
        self._loop = asyncio.new_event_loop()
        self._manager = Thread(target=pool_manager,
                               args=(self._loop, self._pool, self._queue,
                                     initializer, initargs,
                                     workers, task_limit))
        self._manager.daemon = True

    @asyncio.coroutine
    def _enqueue(self, task):
        yield from self._queue.put(task)

    @asyncio.coroutine
    def _join(self):
        yield from self._queue.join()

    def stop(self):
        """Stops the pool without performing any pending task."""
        self._loop.stop()
        self._manager.join()

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
        if not self._manager.is_alive():
            try:
                self._manager.start()
            except RuntimeError:
                raise RuntimeError('The Pool is not running')

        task = Task(next(self._counter), function, args, kwargs,
                    callback, timeout, identifier)

        self._loop.call_soon_threadsafe(asyncio.async, self._enqueue(task))

        return task

    def join(self, timeout=None):
        """Joins the pool waiting until all workers exited.

        If *timeout* is set, it block until all workers are done
        or raise TimeoutError.

        """
        if self._closed:
            sleep(3)
            #asyncio.wait_for(self._join(), timeout)
            self.stop()
        elif self._manager.is_alive():
            raise RuntimeError('The Pool is still running')

        join_workers(self._pool)
