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
from itertools import count
from inspect import isclass
from time import time, sleep
from traceback import format_exc, print_exc
from threading import Thread, Event
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
        task = None
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
            task = worker.current
            task._set(worker.current.cancelled
                      and TaskCancelled('Task has been cancelled')
                      or TimeoutError('Task timeout'))
        # run tasks callback
        if task._callback is not None:
            try:
                task._callback(task)
            except:
                print_exc()

        # join the process
        worker.join()

    def __call__(self, *args, **kwargs):
        t = Task(next(self._counter), None, args, kwargs,
                 self.callback, self.timeout)
        w = Worker(1, self._function, None, None)
        w.start()
        w.schedule_task(t)
        w.tasks.reader.close()
        w.results.writer.close()
        self._handle_job(self, w)

        return t


class Channel(object):
    """Wraps the Workers Pipes."""
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
        try:
            return self.queue[0]
        except IndexError:
            return None

    def stop(self):
        self.tasks.writer.close()
        self.terminate()
        self.join(2)
        if self.is_alive():
            os.kill(self.pid, SIGKILL)

    def schedule_task(self, task):
        """Sends a *Task* to the worker."""
        try:
            self.queue.append(task)
            if task is self.current:
                self.current._timestamp = time()
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
        task = results = None

        try:
            task = self.queue.popleft()
            results = self.results.receive()
            if self.current is not None:
                self.current._timestamp = time()
        except EOFError:  # closed pipe
            self.results.reader.close()
        finally:
            return task, results

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


class TaskScheduler(Thread):
    """Sends tasks to the workers."""
    def __init__(self, pool, queue, rejected):
        Thread.__init__(self)
        self.daemon = True
        self.state = RUNNING
        self.pool = pool
        self.queue = queue
        self.rejected = rejected

    @staticmethod
    def free_workers(workers, timeout):
        """Wait for available workers."""
        descriptors = [w.tasks.writer for w in workers
                       if not w.tasks.writer.closed]
        _, ready, _ = select([], descriptors, [], timeout)
        return [w for w in workers if w.tasks.writer in ready]

    def schedule_tasks(self, workers, timeout):
        """For each worker, delivers a task if available."""
        for worker in workers:
            try:
                try:
                    task = self.rejected.popleft()
                except IndexError:
                    task = self.queue.get(timeout=timeout)
                worker.schedule_task(task)
            except Empty:  # no tasks available
                continue

    def run(self):
        while self.state != STOPPED:
            workers = self.pool[:]
            available = self.free_workers(workers, 0.6)
            self.schedule_tasks(available, 0.2)


class PoolMaintainer(Thread):
    """Maintains the workers within the Pool."""
    def __init__(self, pool, rejected, workers, limit, initializer, initargs):
        Thread.__init__(self)
        self.daemon = True
        self.state = RUNNING
        self.pool = pool
        self.rejected = rejected
        self.workers = workers
        self.limit = limit
        self.initializer = initializer
        self.initargs = initargs

    @staticmethod
    def expired_workers(workers, timeout):
        """Wait for expired workers.

        Expired workers are those which are not alive, which
        results channel has been closed and which current task
        has not started.
        """
        ##TODO: wait for SIGCHLD
        sleep(timeout)
        return [w for w in workers if not w.is_alive()
                and w.results.reader.closed]

    @staticmethod
    def clean_workers(workers):
        """Join expired workers and collect pending tasks."""
        rejected = []

        for worker in workers:
            worker.join()
            worker.tasks.writer.close()
            rejected.extend(worker.queue)

        return rejected

    def respawn_workers(self):
        """Respawn missing workers."""
        pool = []

        for _ in range(self.workers - len(self.pool)):
            worker = Worker(self.limit, None, self.initializer, self.initargs)
            worker.start()
            worker.tasks.reader.close()
            worker.results.writer.close()
            pool.append(worker)  # add worker to pool

        return pool

    def run(self):
        while self.state != STOPPED:
            workers = self.pool[:]
            expired = self.expired_workers(workers, 0.1)
            for worker in expired:
                self.pool.remove(worker)
            rejected_tasks = self.clean_workers(expired)
            self.rejected.extend(rejected_tasks)
            respawned = self.respawn_workers()
            self.pool.extend(respawned)


class ResultsFetcher(Thread):
    """Fetches results from the pool and forwards them to their tasks."""
    def __init__(self, pool, queue):
        Thread.__init__(self)
        self.daemon = True
        self.state = RUNNING
        self.pool = pool
        self.queue = queue

    @staticmethod
    def results(workers, timeout):
        """Wait for expired workers."""
        descriptors = [w.results.reader for w in workers
                       if not w.results.reader.closed]
        select(descriptors, [], [], timeout)
        return [w for w in workers if w.results.poll(0)]

    @staticmethod
    def check_current(workers):
        """Check if ongoing jobs have been cancelled or timeout expired."""
        now = time()
        for worker in workers:
            if worker.is_alive() and worker.current is not None:
                task = worker.current
                if task._timeout > 0 and now - task._timestamp > task._timeout:
                    task._set(TimeoutError("Task timeout"))
                    worker.stop()
                elif task.cancelled:
                    task._set(TaskCancelled("Task cancelled"))
                    worker.stop()

    def set_tasks(self, workers):
        """Sets tasks, runs callbacks and notifies to the Queue."""
        for worker in workers:
            task, results = worker.get_results(0)
            if task is not None:  # EOF on expired worker
                if not task.ready:  # task might timeout or be cancelled
                    task._set(results)
                if task._callback is not None:
                    try:
                        task._callback(task)
                    except:
                        print_exc()
                self.queue.task_done()

    def run(self):
        while self.state != STOPPED:
            workers = self.pool[:]
            self.check_current(workers)
            ready = self.results(workers, 0.6)
            self.set_tasks(ready)


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
        self._pool = []  # active workers container
        self._state = CREATED  # pool state flag
        self._task_scheduler = TaskScheduler(self._pool, self._queue,
                                             self._rejected)
        self._pool_maintainer = PoolMaintainer(self._pool, self._rejected,
                                               workers, task_limit,
                                               initializer, initargs)
        self._results_fetcher = ResultsFetcher(self._pool, self._queue)
        self.initializer = initializer
        self.initargs = initargs

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
        self.join()

    @property
    def active(self):
        return self._state == RUNNING and True or False

    def stop(self):
        """Stops the pool without performing any pending task."""
        self._state = STOPPED
        self._task_scheduler.state = STOPPED
        self._pool_maintainer.state = STOPPED
        self._results_fetcher.state = STOPPED
        self._task_scheduler.join()
        self._pool_maintainer.join()
        self._results_fetcher.join()
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
        # if timeout is set join workers until its value
        while counter < timeout and self._pool:
            counter += len(self._pool) / 10.0
            expired = [w for w in self._pool if w.join(0.1) is None
                       and not w.is_alive()]
            self._pool = [w for w in self._pool if w not in expired]
        # verify timeout expired
        if timeout > 0 and counter == timeout and self._pool:
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
        # start the pool at first call
        if self._state == CREATED:
            self._task_scheduler.start()
            self._pool_maintainer.start()
            self._results_fetcher.start()
            self._state = RUNNING
        elif self._state != RUNNING:
            raise RuntimeError('The Pool is not running')
        if not isinstance(function, Callable):
            raise ValueError('function must be callable')
        task = Task(next(self._counter), function, args, kwargs,
                    callback, timeout)
        self._queue.put(task)

        return task
