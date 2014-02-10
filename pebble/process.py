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
from time import time
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
        print(error)
        print(error.traceback)


def process(*args, **kwargs):
    """Turns a *function* into a Process and runs its logic within.

    A decorated *function* will return a *Task* object once is called.

    If *callback* is a callable, it will be called once the task has ended
    with the task identifier and the *function* return values.

    """
    def wrapper(function):
        return Wrapper(function, timeout, callback)

    if len(args) == 1 and not len(kwargs) and isinstance(args[0], Callable):
        return Wrapper(args[0], 0, None)
    elif not len(args) and len(kwargs):
        timeout = kwargs.get('timeout', 0)
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

    @thread(callback=_managers_callback)
    def _handle_job(self, worker):
        counter = 0
        # wait for task to complete, timeout or to be cancelled
        while (self.timeout == 0 or counter < self.timeout):
            if worker.results.poll(0.1) or worker.current.cancelled:
                break
            else:
                counter += 0.1
        # get results if ready, otherwise set TimeoutError or TaskCancelled
        try:
            task, results = worker.get_results(0)
            task._set(results)
        except TimeoutError:
            worker.stop()
            worker.current._set(worker.current.cancelled
                                and TaskCancelled('Task has been cancelled')
                                or TimeoutError('Task timeout'))
        # join the process
        worker.join()

    def __call__(self, *args, **kwargs):
        t = Task(next(self._counter), None, args, kwargs,
                 self.callback, self.timeout)
        w = Worker(1, self._function, None, None)
        w.start()
        w.new_task(t)
        w.tasks.reader.close()
        w.results.writer.close()
        self._handle_job(self, w)

        return t


class Channel(object):
    def __init__(self):
        self.reader, self.writer = Pipe(duplex=False)

    def send(self, obj):
        self.writer.send(obj)

    def receive(self):
        return self.reader.recv()

    def poll(self, timeout):
        return self.reader.poll(timeout)

    def sending(self):
        return not self.writer.closed

    def receiving(self):
        return not self.reader.closed


class Worker(Process):
    def __init__(self, limit, function, initializer, initargs):
        Process.__init__(self)
        self.counter = count()
        self.in_progress = None
        self._function = function
        self.queue = deque()  # queued tasks
        self.tasks = Channel()
        self.results = Channel()
        self.limit = limit
        self.initializer = initializer
        self.initargs = initargs
        self.daemon = True

    @property
    def current(self):
        return self.in_progress

    @current.setter
    def current(self, task):
        if task is not None:
            task._timestamp = time()
        self.in_progress = task

    def stop(self):
        self.terminate()
        self.join(2)
        if self.is_alive():
            os.kill(self.pid, SIGKILL)

    def new_task(self, task):
        """Sends a *Task* to the worker."""
        try:
            if self.current is None:
                self.current = task
            else:
                self.queue.append(task)
            self.tasks.send((task._function, task._args, task._kwargs))
        except (IOError, OSError):  # closed pipe
            self.tasks.writer.close()
        finally:
            if self.limit != 0 and next(self.counter) == self.limit:
                self.tasks.writer.close()

    def get_results(self, timeout):
        """Gets results from Worker, blocks until any result is ready
        or *timeout* expires.

        Raises TimeoutError if *timeout* expired.

        """
        if not self.results.poll(timeout):
            raise TimeoutError('Still running')

        try:
            results = self.results.receive()
            task = self.current
            try:
                self.current = self.queue.popleft()
            except IndexError:
                self.current = None
            return task, results
        except EOFError:  # closed pipe
            self.results.reader.close()

    def run(self):
        """Worker process logic."""
        error = None
        results = None
        # ignore SIGINT signal and close unused desciptors
        signal(SIGINT, SIG_IGN)
        self.tasks.writer.close()
        self.results.reader.close()
        # run initializer function
        if self.initializer is not None:
            try:
                self.initializer(*self.initargs)
            except Exception as err:
                error = err
                error.traceback = format_exc()

        while self.limit == 0 or next(self.counter) < self.limit:
            # get next task and execute it
            try:
                function, args, kwargs = self.tasks.receive()
                function = function is not None and function or self._function
                results = function(*args, **kwargs)
            except (IOError, OSError, EOFError):  # pipe was closed
                exit(1)
            except Exception as err:  # error occurred in function
                if error is None:  # do not overwrite initializer errors
                    error = err
                    error.traceback = format_exc()
            # send produced results or raised exception
            finally:
                try:
                    self.results.send(error is not None and error or results)
                except (IOError, OSError, EOFError):  # pipe was closed
                    exit(1)
                except PicklingError as err:
                    self.results.send(err)

                error = results = None
        # close all descriptors and exit
        self.tasks.reader.close()
        self.results.writer.close()
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
                               and w.results.reader.closed]
                    self._pool = [w for w in self._pool if w not in expired]
                    # clean up the worker, re-enqueue any expired task
                    for worker in expired:
                        worker.join()
                        worker.tasks.writer.close()
                        self._rejected.extend(worker.queue)
                # re-spawn missing processes
                for _ in range(self._workers - len(self._pool)):
                    worker = Worker(self._limit, None,
                                    self.initializer, self.initargs)
                    worker.start()
                    worker.tasks.reader.close()
                    worker.results.writer.close()
                    self._pool.append(worker)  # add worker to pool

    @thread(callback=_managers_callback)
    def _manage_results(self):
        while self._state != STOPPED:
            # collect possible results
            descriptors = [w.results.reader for w in self._pool[:]
                           if not w.results.reader.closed]
            select(descriptors, [], [], 0.8)
            workers = [w for w in self._pool[:] if w.results.poll()]
            # process all results
            for worker in workers:
                if worker.receive_results():
                    self._queue.task_done()

    @thread(callback=_managers_callback)
    def _schedule_tasks(self):
        while self._state != STOPPED:
            # wait for free workers
            descriptors = [w.tasks.writer for w in self._pool[:]
                           if not w.tasks.writer.closed]
            _, ready, _ = select([], descriptors, [], 0.8)
            workers = [w for w in self._pool[:] if w.tasks.writer in ready]
            # schedule new tasks
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
            if counter == timeout and self._pool:
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
