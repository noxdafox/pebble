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
from traceback import format_exc, print_exc
from threading import Thread, Event
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
from ..pebble import BasePool, join_workers
from ..pebble import Task, TimeoutError, TaskCancelled, ProcessExpired


@asyncio.coroutine
def connect(*channels):
    for channel in channels:
        yield from channel.connect()


# --------------------------------------------------------------------------- #
#                               Worker Process                                #
# --------------------------------------------------------------------------- #
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

    try:
        loop.run_until_complete(worker_loop(tasks, results, limit))
    except:
        print_exc()
        os._exit(1)

    # if deinitializer is not None:
    #     try:
    #         deinitializer(*deinitargs)
    #     except Exception as error:
    #         print_exc()

    os._exit(0)


# --------------------------------------------------------------------------- #
#                            Pool's Maintenance Loop                          #
# --------------------------------------------------------------------------- #
def pool_manager(pool):
    """Separate thread managing the Pool's flow."""
    asyncio.set_event_loop(pool._loop)
    asyncio.async(worker_manager(pool), loop=pool._loop)
    pool._loop.run_forever()


@asyncio.coroutine
def worker_manager(pool):
    """Collects expired workers and spawns new ones."""
    queue = pool._queue
    workers = pool._pool
    limit = pool._worker_limit
    initializer = pool._initializer
    initargs = pool._initargs
    worker_number = pool._worker_number
    worker_expired = asyncio.Event(loop=pool._loop)

    while 1:
        expired = [w for w in workers if not w.alive]

        join_workers(expired[:])
        for worker in expired:
            workers.remove(worker)
            while not worker.tasks.empty():
                task = worker.tasks.get_nowait()
                yield from queue.put(task)

        for _ in range(worker_number - len(workers)):
            worker = Worker(queue, limit, worker_expired)
            yield from worker.start(initializer, initargs)
            workers.append(worker)

        yield from worker_expired.wait()
        worker_expired.clear()


# --------------------------------------------------------------------------- #
#                                  Worker                                     #
# --------------------------------------------------------------------------- #
class Worker(object):
    def __init__(self, queue, maxtasks, expiration_event):
        self.queue = queue
        self.limit = maxtasks
        self.expiration = expiration_event

        self.sent = count()
        self.received = count()
        self.tasks = asyncio.Queue()
        self.process = None
        self.task_reader = None
        self.task_writer = None
        self.result_reader = None
        self.result_writer = None

    @property
    def expired(self):
        return self.result_reader.closed

    @asyncio.coroutine
    def start(self, initializer, initargs):
        """Starts the worker."""
        self.task_reader, self.task_writer = pipe()
        self.result_reader, self.result_writer = pipe()

        self.process = pool_worker(self.task_reader, self.result_writer,
                                   initializer, initargs, self.limit)
        self.task_reader.close()
        self.result_writer.close()

        yield from connect(self.task_writer, self.result_reader)

        asyncio.async(self.taskloop())
        asyncio.async(self.resultloop())

    @asyncio.coroutine
    def taskloop(self):
        while self.limit == 0 or next(self.sent) < self.limit:
            task = yield from self.queue.get()
            yield from self.tasks.put(task)

            data = (task._function, task._args, task._kwargs)
            self.task_writer.send(data)

        self.task_writer.close()

    @asyncio.coroutine
    def resultloop(self):
        process = self.process
        reader = self.result_reader

        while self.limit == 0 or next(self.received) < self.limit:
            error = None
            task = yield from self.tasks.get()
            timeout = task.timeout

            try:
                results = yield from asyncio.wait_for(reader.recv(), timeout)
            except TimeoutError as error:
                results = TimeoutError('Task timeout', timeout)
            except EnvironmentError as error:
                results = ProcessExpired('Process expired', process.exitcode)

            self.task_done(task, results)

            if error is not None:
                self.stop()
                return

        reader.close()

    def task_done(self, task, results):
        """Task done callback.

        Sets the results within the Task and runs the Callback.

        """
        task._set(results)

        if task._callback is not None:
            try:
                task._callback(task)
            except Exception:
                print_exc()

        self.queue.task_done()

    def stop(self):
        self.task_writer.close()
        self.result_reader.close()
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
        self._event = Event()
        self._loop = asyncio.new_event_loop()
        self._queue = asyncio.JoinableQueue(loop=self._loop)
        self._manager = Thread(target=pool_manager, args=(self, ))
        self._manager.daemon = True

    @asyncio.coroutine
    def _enqueue(self, task):
        yield from self._queue.put(task)

    @asyncio.coroutine
    def _join(self, timeout):
        yield from asyncio.wait_for(self._queue.join(), timeout)
        self._event.set()

    def stop(self):
        """Stops the pool without performing any pending task."""
        for worker in self._pool:
            self._loop.call_soon_threadsafe(worker.stop)
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._manager.join()

    def join(self, timeout=None):
        """Joins the pool waiting until all workers exited.

        If *timeout* is set, it block until all workers are done
        or raise TimeoutError.

        """
        if self._closed:
            self._loop.call_soon_threadsafe(asyncio.async, self._join(timeout))
            self._event.wait()
            self.stop()
        elif self._manager.is_alive():
            raise RuntimeError('The Pool is still running')

        join_workers(self._pool, timeout=timeout)

    def schedule(self, function, args=(), kwargs={}, identifier=None,
                 callback=None, timeout=None):
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
