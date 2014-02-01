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

from sys import exit
from uuid import uuid4
from select import select
from inspect import isclass
from traceback import format_exc, print_exc
from itertools import count
from threading import Condition, Event, Lock
from collections import Callable, deque
from functools import update_wrapper
from multiprocessing import Process, Pipe
from signal import SIG_IGN, SIGKILL, SIGINT, signal
try:  # Python 2
    from Queue import Queue, Empty
except:  # Python 3
    from queue import Queue, Empty
try:  # Python 2
    from cPickle import PicklingError
except:  # Python 3
    from pickle import PicklingError

from .thread import thread
from .pebble import TimeoutError, TaskCancelled


STOPPED = 0
RUNNING = 1
CLOSING = 2
CREATED = 3


def _managers_callback(task):
    """Callback for Pool thread managers."""
    try:
        task.get()
    except Exception as error:
        print error
        print error.traceback


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


class Task(object):
    """Handler to the ongoing task."""
    def __init__(self, task_nr, function, args, kwargs, callback, timeout):
        self.id = uuid4()
        self._function = function
        self._args = args
        self._kwargs = kwargs
        self._number = task_nr
        self._ready = False
        self._cancelled = False
        self._results = None
        self._event = Event()
        self._timeout = timeout
        self._timestamp = 0
        self._callback = callback

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return "%s (Task-%d, %s)" % (self.__class__, self.number, self.id)

    def __hash__(self):
        return self._number

    @property
    def number(self):
        return self._number

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
            self._worker._terminate()
        else:
            raise RuntimeError('A completed task cannot be cancelled')

    def _set(self, results):
        self._results = results
        self._ready = True
        self._event.set()
        if self._callback is not None and not self._cancelled:
            try:
                self._callback(self)
            except:
                print_exc()


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


class Worker(Process):
    def __init__(self, limit, function, initializer, initargs):
        Process.__init__(self)
        self.counter = count()
        self.function = function
        self.queue = deque()  # queued tasks
        self.taskin, self.taskout = Pipe(duplex=False)
        self.resin, self.resout = Pipe(duplex=False)
        self.limit = limit
        self.initializer = initializer
        self.initargs = initargs
        self.daemon = True

    def __hash__(self):
        return self.pid

    def stop(self):
        self.terminate()
        self.join(2)
        if self.is_alive():
            os.kill(self.pid, SIGKILL)

    def send_task(self, task):
        try:
            self.queue.append(task)
            self.taskout.send((task._function, task._args, task._kwargs))
        except (IOError, OSError):  # closed pipe
            self.taskout.close()
        finally:
            if self.limit != 0 and next(self.counter) == self.limit:
                self.taskout.close()

    def receive_results(self):
        try:
            results = self.resin.recv()
            task = self.queue.popleft()
            task._set(results)
            return True
        except EOFError:  # closed pipe
            self.resin.close()
            return False

    def send_results(self, value):
        try:
            self.resout.send(value)
        except (IOError, OSError, EOFError):  # pipe was closed
            exit(1)
        except PicklingError as err:
            self.resout.send(err)

    def receive_task(self):
        try:
            function, args, kwargs = self.taskin.recv()
            if function is None:
                function = self._function
            return function, args, kwargs
        except (IOError, OSError, EOFError):  # pipe was closed
            exit(1)

    def run(self):
        signal(SIGINT, SIG_IGN)
        error = None
        results = None
        self.taskout.close()
        self.resin.close()

        if self.initializer is not None:
            try:
                self.initializer(*self.initargs)
            except Exception as err:
                error = err
                error.traceback = format_exc()

        while self.limit == 0 or next(self.counter) < self.limit:
            try:
                function, args, kwargs = self.receive_task()
                results = function(*args, **kwargs)
            except Exception as err:
                error = err
                error.traceback = format_exc()
            finally:
                self.send_results(error is not None and error or results)
                error = results = None

        self.taskin.close()
        self.resout.close()
        exit(0)


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
        self._error = None
        self._counter = count()
        self._workers = workers
        self._limit = task_limit
        self._rejected = deque()  # task enqueued on dead worker
        self._pool = []  # active workers container [worker, worker]
        self._maintenance = []  # pool maintenance tasks
        self._pool_condition = Condition(Lock())
        self._state = CREATED  # pool state flag
        self.initializer = initializer
        self.initargs = initargs

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
        self.join()

    @thread(callback=_managers_callback)
    def _maintain_pool(self):
        while self._state != STOPPED:
            # wait for any children to expire
            with self._pool_condition:
                while (self._workers - len(self._pool) == 0
                       and self._state != STOPPED):
                    self._pool_condition.wait(timeout=0.1)
                    # collect expired workers and update pool
                    expired = [w for w in self._pool if not w.is_alive()
                               and w.resin.closed]
                    self._pool = [w for w in self._pool if w not in expired]
                    # clean up the worker, re-enqueue any expired task
                    for worker in expired:
                        worker.join()
                        worker.taskout.close()
                        self._rejected.extend(worker.queue)
                # re-spawn missing processes
                for _ in range(self._workers - len(self._pool)):
                    worker = Worker(self._limit, None,
                                    self.initializer, self.initargs)
                    worker.start()
                    worker.taskin.close()
                    worker.resout.close()
                    self._pool.append(worker)  # add worker to pool

    @thread(callback=_managers_callback)
    def _manage_results(self):
        while self._state != STOPPED:
            # collect possible results
            descriptors = [w.resin for w in self._pool[:]
                           if not w.resin.closed]
            select(descriptors, [], [], 0.8)
            workers = [w for w in self._pool[:] if w.resin.poll()]
            # process all results
            for worker in workers:
                if worker.receive_results():
                    self._queue.task_done()

    @thread(callback=_managers_callback)
    def _schedule_tasks(self):
        while self._state != STOPPED:
            # wait for free workers
            descriptors = [w.taskout for w in self._pool[:]
                           if not w.taskout.closed]
            _, ready, _ = select([], descriptors, [], 0.8)
            workers = [w for w in self._pool[:] if w.taskout in ready]
            for worker in workers:
                try:
                    try:
                        task = self._rejected.popleft()
                    except IndexError:
                        task = self._queue.get(timeout=0.3)
                    worker.send_task(task)
                except Empty:  # no tasks available
                    continue

    @property
    def active(self):
        return self._state == RUNNING and True or False

    def stop(self):
        """Stops the pool without performing any pending task."""
        with self._pool_condition:
            self._workers = 0
            self._state = STOPPED
            for w in self._pool:
                w.stop()

    def close(self):
        """Close the pool allowing all queued tasks to be performed."""
        self._state = CLOSING
        self._queue.join()
        self.stop()

    def join(self, timeout=0):
        """Joins the pool waiting until all workers exited.

        If *timeout* is greater than 0,
        it block until all workers exited or raise TimeoutError.

        """
        counter = 0

        if self._state == RUNNING:
            raise RuntimeError('The Pool is still running')
        with self._pool_condition:
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
        # wait until pool maintainer tasks are ended
        for maintainer in self._maintenance:
            maintainer.get()

    def schedule(self, function, args=(), kwargs={}, callback=None, timeout=0):
        """Schedules *function* into the Pool, passing *args* and *kwargs*
        respectively as arguments and keyword arguments.

        If *callback* is a callable it will be executed once the function
        execution has completed with the returned *Task* as a parameter.

        A *Task* object is returned.

        """
        # start the pool at first call
        if self._state == CREATED:
            self._maintenance.extend((self._maintain_pool(self),
                                      self._schedule_tasks(self),
                                      self._manage_results(self)))
            self._state = RUNNING
        elif self._state != RUNNING:
            raise RuntimeError('The Pool is not running')
        if not isinstance(function, Callable):
            raise ValueError('function must be callable')
        task = Task(next(self._counter), function, args, kwargs,
                    callback, timeout)
        self._queue.put(task)

        return task
