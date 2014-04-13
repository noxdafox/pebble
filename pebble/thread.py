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


from time import time
from traceback import format_exc, print_exc
from itertools import count
from threading import Thread
from collections import Callable

from .pebble import Task, TimeoutError, PoolContext, PoolManager
from .pebble import STOPPED, RUNNING, CLOSING, CREATED


class ThreadWorker(Thread):
    def __init__(self, queue, limit, event, initializer, initargs):
        Thread.__init__(self)
        self.queue = queue
        self.limit = limit
        self.worker_event = event
        self.initializer = initializer
        self.initargs = initargs
        self.daemon = True

    @property
    def expired(self):
        return not self.is_alive()

    def run(self):
        error = None
        results = None
        counter = count()

        if self.initializer is not None:
            try:
                self.initializer(*self.initargs)
            except Exception as err:
                error = err
                error.traceback = format_exc()

        while self.limit == 0 or next(counter) < self.limit:
            task = self.queue.get()
            if task is None:  # worker terminated
                self.queue.task_done()
                return
            function = task._function
            args = task._args
            kwargs = task._kwargs
            try:
                if not task._cancelled:
                    task._timestamp = time()
                    results = function(*args, **kwargs)
            except Exception as err:
                error = err
                error.traceback = format_exc()
            finally:
                task._set(error is not None and error or results)
                if task._callback is not None:
                    try:
                        task._callback(task)
                    except:
                        print_exc()
                self.queue.task_done()
                error = None
                results = None

        ##TODO: deinitializer
        if self.worker_event is not None:
            self.worker_event.set()


class ThreadPoolManager(PoolManager):
    """ThreadPool management routine."""
    def __init__(self, context):
        PoolManager.__init__(self, context)

    def cleanup_workers(self, expired):
        pool = self.context.pool

        for worker in expired:
            worker.join()
            pool.remove(worker)

    def spawn_workers(self):
        """Spawns missing Workers."""
        pool = self.context.pool

        for _ in range(self.context.workers - len(pool)):
            worker = ThreadWorker(self.context.queue,
                                  self.context.limit,
                                  self.context.workers_event,
                                  self.context.initializer,
                                  self.context.initargs)
            worker.start()

            pool.append(worker)


class ThreadPool(object):
    """A ThreadPool allows to schedule jobs into a Pool of Threades
    which will perform them concurrently.

    workers is an integer representing the amount of desired thread workers
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
                 initializer=None, initargs=None):
        self._context = PoolContext(workers, task_limit, queue, queueargs,
                                    initializer, initargs)
        self._pool_manager = ThreadPoolManager(self._context)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
        self.join()

    def _start(self):
        """Starts the pool."""
        self._pool_manager.start()
        self._context.state = RUNNING

    def _join_workers(self, timeout=None):
        """Join terminated workers."""
        counter = 0
        workers = self._context.pool

        while len(workers) > 0 and (timeout is None or counter < timeout):
            for worker in workers[:]:
                worker.join(timeout is not None and 0.1 or None)
                if not worker.is_alive():
                    workers.remove(worker)
            counter += timeout is not None and (len(workers)) / 10.0 or 0

        return workers

    def _join_managers(self):
        if self._pool_manager.is_alive():
            self._pool_manager.join()

    @property
    def initializer(self):
        return self._context.initializer

    @initializer.setter
    def initializer(self, value):
        self._context.initializer = value

    @property
    def initargs(self):
        return self._context.initargs

    @initargs.setter
    def initargs(self, value):
        self._context.initargs = value

    @property
    def active(self):
        return self._context.state == RUNNING and True or False

    def close(self):
        """Closes the pool allowing all queued tasks to be performed."""
        self._context.state = CLOSING
        self._context.queue.join()
        self.stop()

    def stop(self):
        """Stops the pool without performing any pending task."""
        self._context.state = STOPPED
        self._join_managers()

        for w in self._context.pool:
            w.limit = - 1
        for w in self._context.pool:
            self._context.queue.put(None)

    def join(self, timeout=0):
        """Joins the pool waiting until all workers exited.

        If *timeout* is greater than 0,
        it block until all workers exited or raise TimeoutError.

        """
        if self._context.state == RUNNING:
            raise RuntimeError('The Pool is still running')

        if timeout > 0:
            # wait for Pool processes
            self._context.pool = self._join_workers(timeout)
            # verify timeout expired
            if len(self._context.pool) > 0:
                raise TimeoutError('Workers are still running')
        else:
            self._context.pool = self._join_workers()

    def schedule(self, function, args=(), kwargs={},
                 identifier=None, callback=None):
        """Schedules *function* into the Pool, passing *args* and *kwargs*
        respectively as arguments and keyword arguments.

        If *callback* is a callable it will be executed once the function
        execution has completed with the returned *Task* as a parameter.

        A *Task* object is returned.

        """
        if self._context.state == CREATED:
            self._start()
        elif self._context.state != RUNNING:
            raise RuntimeError('The Pool is not running')

        if not isinstance(function, Callable):
            raise ValueError('function must be callable')

        task = Task(self._context.counter, function, args, kwargs,
                    callback, 0, identifier)
        self._context.queue.put(task)

        return task
