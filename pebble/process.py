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

from time import sleep
from uuid import uuid4
from select import select
from signal import SIGKILL
from inspect import isclass
from traceback import format_exc
from itertools import count
from threading import Thread, Event
from collections import Callable
from functools import update_wrapper
from multiprocessing import Process, Pipe
try:  # Python 2
    from Queue import Queue
except:  # Python 3
    from queue import Queue
try:  # Python 2
    from cPickle import PicklingError
except:  # Python 3
    from pickle import PicklingError

from .pebble import TimeoutError, TaskCancelled


STOPPED = 0
RUNNING = 1
CLOSING = 2


def process(*args, **kwargs):
    """Turns a *function* into a Process and runs its logic within.

    A decorated *function* will return a *Task* object once is called.

    If *callback* is a callable, it will be called once the task has ended
    with the task identifier and the *function* return values.

    """
    def wrapper(function):
        return Wrapper(function, timeout, callback)

    if len(args) == 1 and not len(kwargs) and isinstance(args[0], Callable):
        return Wrapper(args[0], None, None)
    elif not len(args) and len(kwargs):
        timeout = kwargs.get('timeout')
        callback = kwargs.get('callback')

        return wrapper
    else:
        raise ValueError("Decorator accepts only keyword arguments.")


# def process_pool(*args, **kwargs):
#     """Turns a *function* into a Thread and runs its logic within.

#     A decorated *function* will return a *Task* object once is called.

#     If *callback* is a callable, it will be called once the task has ended
#     with the task identifier and the *function* return values.

#     """
#     def wrapper(function):
#         return PoolWrapper(function, workers, task_limit, queue, queue_args,
#                            callback, initializer, initargs)

#     if len(args) == 1 and not len(kwargs) and isinstance(args[0], Callable):
#         return PoolWrapper(args[0], 1, 0, None, None, None, None, None)
#     elif not len(args) and len(kwargs):
#         queue = kwargs.get('queue')
#         queue_args = kwargs.get('queueargs')
#         workers = kwargs.get('workers', 1)
#         callback = kwargs.get('callback')
#         initargs = kwargs.get('initargs')
#         initializer = kwargs.get('initializer')
#         task_limit = kwargs.get('worker_task_limit', 0)

#         return wrapper
#     else:
#         raise ValueError("Decorator accepts only keyword arguments.")


class Worker(Process):
    def __init__(self, limit, function, initializer, initargs):
        Process.__init__(self)
        self._function = function
        self.taskin, self.taskout = Pipe(duplex=False)
        self.resin, self.resout = Pipe(duplex=False)
        self.limit = limit
        self.initializer = initializer
        self.initargs = initargs
        self.daemon = True

    def run(self):
        error = None
        results = None
        counter = count()
        self.taskout.close()
        self.resin.close()

        if self.initializer is not None:
            try:
                self.initializer(*self.initargs)
            except Exception as err:
                error = err
                error.traceback = format_exc()

        while self.limit == 0 or next(counter) < self.limit:
            function, args, kwargs = self.taskin.recv()
            if function is None:
                function = self._function
            try:
                results = function(*args, **kwargs)
            except (IOError, OSError):  # pipe was closed
                return
            except Exception as err:
                error = err
                error.traceback = format_exc()
            finally:
                try:
                    self.resout.send(error is not None and error or results)
                except PicklingError as err:
                    self.resout.send(err)
                error = None
                results = None


class Task(object):
    """Handler to the ongoing task."""
    def __init__(self, task_nr, worker, callback, timeout):
        self.id = uuid4()
        self.number = task_nr
        self._ready = False
        self._cancelled = False
        self._results = None
        self._event = Event()
        self._worker = worker
        self._timeout = timeout
        self._callback = callback
        self._worker_listener = Thread(target=self._set)
        self._worker_listener.daemon = True
        self._worker_listener.start()

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return "%s (Task-%d, %s)" % (self.__class__, self.number, self.id)

    @property
    def ready(self):
        return self._ready

    @property
    def cancelled(self):
        return self._cancelled

    def get(self, timeout=None):
        """Retrieves the produced results.

        If the executed code raised an error it will be re-raised.

        """
        self._event.wait(timeout)
        if self._ready:
            if (isinstance(self._results, BaseException)):
                raise self._results
            else:
                return self._results
        else:
            raise TimeoutError("Task is still running")

    def cancel(self):
        """Cancels the Task terminating the running process
        and dropping the results."""
        if not self._ready:
            self._cancelled = True
            self._worker.terminate()
        else:
            raise RuntimeError('A completed task cannot be cancelled')

    def _set(self):
        try:
            if self._worker.resin.poll(self._timeout):
                self._results = self._worker.resin.recv()
            elif self._worker.is_alive():
                self._worker.terminate()
                self._results = TimeoutError("Task timeout expired")
        except (IOError, OSError, EOFError) as error:  # pipe was closed
            if not self._cancelled:
                self._results = error
            else:
                self._results = TaskCancelled("Task cancelled")
        finally:
            if not self._worker.is_alive():  # join the process if exited
                self._worker.join()
            self._ready = True
            self._event.set()
            if self._callback is not None and not self._cancelled:
                self._callback(self)


class Wrapper(object):
    def __init__(self, function, timeout, callback):
        self._function = function
        self._counter = count()
        self.timeout = timeout
        self.callback = callback
        update_wrapper(self, function)

    def __call__(self, *args, **kwargs):
        w = Worker(1, self._function, None, None)
        w.start()
        w.taskout.send((None, args, kwargs))
        w.taskin.close()
        w.resout.close()
        return Task(next(self._counter), w, self.callback, self.timeout)


class ProcessPool(object):
    def __init__(self, workers=1, task_limit=0, queue=None, queueargs=None,
                 initializer=None, initargs=None):
        if queue is not None:
            if isclass(queue):
                self._queue = queue(*queueargs)
            else:
                raise ValueError("Queue must be Class")
        else:
            self._queue = Queue()
        self._pool_maintainer = Thread(target=self._maintain_pool)
        self._pool_maintainer.daemon = True
        self._results_handler = Thread(target=self._manage_results)
        self._results_handler.daemon = True
        self._counter = count()
        self._workers = workers
        self._limit = task_limit
        self._pool = []
        self._state = RUNNING
        self.initializer = initializer
        self.initargs = initargs

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
        self.join()

    def _terminate_workers(self, workers):
        for w in workers:
            w.terminate()
            w.join(2)
            if w.is_alive():
                os.kill(w.pid, SIGKILL)

    def _maintain_pool(self):
        while self._state != STOPPED:
            workers = []
            expired = [w for w in self._pool if not w.is_alive()]
            self._pool = [w for w in self._pool if w not in expired]
            for w in expired:
                w.join()
            for _ in range(self._workers - len(self._pool)):
                w = Worker(self._queue, self._limit,
                           self.initializer, self.initargs)
                w.start()
                workers.append(w)
            self._pool.extend(workers)
            self._dispatch_tasks.send(workers)
            sleep(0.6)

    def _manage_results(self):
        while self._state != STOPPED:
            ready, _, _ = select([w.resout for w in self._pool], [], [])
            workers = [w for w in self._pool if w.resout in ready]
            self._dispatch_tasks.send(workers)
            for w in workers:
                try:
                    results = w.resout.recv()
                    task = self._tasks[w]
                    task.set(results)
                except (IOError, OSError, EOFError):  # closed pipe
                    self._queue.put(self._tasks[w])
                finally:
                    del self._tasks[w]

    def _dispatch_tasks(self):
        workers = (yield)
        for w in workers:
            t = self._queue.get()
            try:
                w.taskin.send(tuple(t[1:]))
                self._tasks[w] = t[0]
            except (IOError, OSError, EOFError):  # closed pipe
                self._queue.put(t)

    @property
    def active(self):
        return self._state == RUNNING and True or False

    def stop(self):
        """Stops the pool without performing any pending task."""
        self._state = STOPPED
        self._terminate_workers(self._pool)

    def close(self):
        """Close the pool allowing all queued tasks to be performed."""
        self._state = CLOSING
        self._queue.join()
        self._state = STOPPED
        self._terminate_workers(self._pool)

    def join(self, timeout=0):
        """Joins the pool waiting until all workers exited.

        If *timeout* is greater than 0,
        it block until all workers exited or raise TimeoutError.

        """
        counter = 0

        if self._state == RUNNING:
            raise RuntimeError('The Pool is still running')
        # if timeout is set join workers until its value
        while counter < timeout and self._pool:
            counter += len(self._pool) / 10.0
            expired = [w for w in self._pool if w.join(0.1) is None
                       and not w.is_alive()]
            self._pool = [w for w in self._pool if w not in expired]
        # verify timeout expired
        if timeout > 0 and self._pool:
            raise TimeoutError('Workers are still running')
        # timeout not set
        self.pool = [w for w in self._pool if w.join() is None
                     and w.is_alive()]

    def schedule(self, function, args=(), kwargs={}, callback=None, timeout=0):
        """Schedules *function* into the Pool, passing *args* and *kwargs*
        respectively as arguments and keyword arguments.

        If *callback* is a callable it will be executed once the function
        execution has completed with the returned *Task* as a parameter.

        A *Task* object is returned.

        """
        if self._state != RUNNING:
            raise RuntimeError('The Pool is not running')
        if not isinstance(function, Callable):
            raise ValueError('function must be callable')
        if not self._pool_maintainer.is_alive():
            self._pool_maintainer.start()
        if not self._results_handler.is_alive():
            self._results_handler.start()
        task = Task(next(self._counter), callback)
        self._queue.put((task, function, args, kwargs))

        return task
