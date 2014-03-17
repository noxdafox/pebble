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

from select import select
from itertools import count
from inspect import isclass
from time import time, sleep
from traceback import format_exc, print_exc
from threading import Thread
from collections import Callable, deque
from functools import update_wrapper
from multiprocessing import Process
from multiprocessing.connection import Listener, Client
try:  # Python 2
    from Queue import Queue, Empty
    from cPickle import PicklingError
except:  # Python 3
    from queue import Queue, Empty
    from pickle import PicklingError

from .thread import thread
from .pebble import TimeoutError, Task

### platform dependent code ###
if os.name in ('posix', 'os2'):
    FAMILY = 'AF_UNIX'
    from signal import SIG_IGN, SIGKILL, SIGINT, signal
else:
    FAMILY = 'AF_INET'
    from signal import SIG_IGN, SIGINT, signal


STOPPED = 0
RUNNING = 1
CLOSING = 2
CREATED = 3


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


def trampoline(function, *args, **kwargs):
    """Trampoline function for decorators."""
    func = load_function(function)
    return func(*args, **kwargs)


def dump_function(function):
    """Dumps the decorated function for pickling."""
    try:
        name = function.__name__
        module = function.__module__
        __import__(module)
        mod = sys.modules[module]
        getattr(mod, name)
        return {'name': name, 'module': module}
    except (ImportError, KeyError, AttributeError):
        raise PicklingError(
            "Can't pickle %r: it's not found as %s.%s" %
            (function, module, name))


def load_function(state):
    """Loads the function and extracts it from its decorator."""
    name = state.get('name')
    module = state.get('module')
    __import__(module)
    mod = sys.modules[module]
    decorated = getattr(mod, name)
    return decorated._function


class Wrapper(object):
    def __init__(self, function, timeout, callback):
        self._function = function
        self._counter = count()
        self._connection = None
        self.timeout = timeout
        self.callback = callback
        update_wrapper(self, function)

    def __del__(self):
        if self._connection is not None:
            self._connection.close()

    @thread
    def _handle_job(self, worker):
        task = None

        # wait for task to complete, timeout or to be cancelled
        while task is None:
            if worker.channel.poll(0.2):
                task = worker.task_complete()
            else:
                task = worker.task_valid(time())
        # run tasks callback
        if task._callback is not None:
            try:
                task._callback(task)
            except:
                print_exc()

        # join the process
        worker.join()

    def __call__(self, *args, **kwargs):
        if self._connection is None:
            self._connection = Listener(family=FAMILY)
        # serialize decorated function
        function = dump_function(self._function)
        # attach decorated function to the arguments
        args = list(args)
        args.insert(0, function)
        t = Task(next(self._counter), trampoline, args, kwargs,
                 self.callback, self.timeout)
        w = Worker(self._connection.address)
        w.start()
        w.finalize(self._connection)
        w.schedule_task(t)
        self._handle_job(self, w)

        return t


class Worker(Process):
    def __init__(self, address, limit=1, initializer=None, initargs=None):
        Process.__init__(self)
        self.address = address
        self.limit = limit
        self.initializer = initializer
        self.initargs = initargs
        self.daemon = True
        self.counter = count()  # task counter
        self.queue = deque()  # queued tasks
        self.channel = None  # task/result channel
        self.closed = False  # no more task allowed

    @property
    def expired(self):
        return self.channel.closed

    def finalize(self, connection):
        """Finalizes the worker, to be called after it has been started."""
        self.channel = connection.accept()

    def stop(self):
        """Stops the worker terminating the process."""
        self.closed = True
        self.channel.close()
        self.terminate()
        self.join(1)
        if self.is_alive():
            self.kill()

    def kill(self):
        """Kills the process."""
        try:
            os.kill(self.pid, SIGKILL)
        except:  # process already dead or Windows platform
            pass

    def task_valid(self, timestamp):
        """Check if current task is not in timeout or cancelled;
        if so, stop the worker and return the task."""
        task = None

        try:
            curr = self.queue[0]
            if curr.timeout > 0 and timestamp - curr._timestamp > curr.timeout:
                task = self.queue.popleft()
                task._set(TimeoutError("Task timeout"))
                self.stop()
            elif curr.cancelled:
                task = self.queue.popleft()
                self.stop()
        except IndexError:  # no tasks in worker queue
            pass

        return task

    def schedule_task(self, task):
        """Sends a *Task* to the worker."""
        if len(self.queue) == 0:  # scheduled task is current one
            task._timestamp = time()
        self.queue.append(task)
        try:
            self.channel.send((task._function, task._args, task._kwargs))
            if self.limit > 0 and next(self.counter) >= self.limit - 1:
                self.closed = True
        except Exception:  # worker closed
            self.queue.popleft()
            task._timestamp = 0
            raise RuntimeError('Worker closed')

    def task_complete(self):
        """Waits for the next task to be completed and sets the task.
        Blocks until any result is ready or *timeout* expires.

        Raises TimeoutError if *timeout* expired.

        """
        task = None

        try:
            task = self.queue.popleft()
        except IndexError:  # process expired
            pass
        try:
            results = self.channel.recv()
            if task:
                task._set(results)
            if len(self.queue) > 0:
                self.queue[0]._timestamp = time()
        except (EOFError, IOError):  # process expired
            self.channel.close()

        return task

    def run(self):
        """Worker process logic."""
        error = None
        results = None
        signal(SIGINT, SIG_IGN)
        self.channel = Client(self.address)

        if self.initializer is not None:  # run initializer function
            try:
                self.initializer(*self.initargs)
            except Exception as err:
                error = err
                error.traceback = format_exc()

        while self.limit == 0 or next(self.counter) < self.limit:
            try:  # get next task and execute it
                function, args, kwargs = self.channel.recv()
                results = function(*args, **kwargs)
            except EOFError:  # other side closed
                pass
            except (IOError, OSError):  # pipe was closed
                sys.exit(1)
            except Exception as err:  # error occurred in function
                if error is None:  # do not overwrite initializer errors
                    error = err
                    error.traceback = format_exc()
            finally:
                try:  # send produced results or raised exception
                    self.channel.send(error is not None and error or results)
                except (IOError, OSError, EOFError):  # pipe was closed
                    sys.exit(1)
                except PicklingError as err:
                    self.channel.send(err)

                error = results = None

        # close all descriptors and exit
        self.channel.close()
        sys.exit(0)


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
        descriptors = [w.channel for w in workers]
        try:
            _, ready, _ = select([], descriptors, [], timeout)
            return [w for w in workers if w.channel in ready]
        except:  # worker expired or stopped
            return []

    def schedule_tasks(self, workers, timeout):
        """For each worker, delivers a task if available."""
        for worker in workers:
            try:
                try:  # first get any rejected task
                    task = self.rejected.popleft()
                except IndexError:  # then get enqueued ones
                    task = self.queue.get(timeout=timeout)
                try:
                    worker.schedule_task(task)
                except RuntimeError:  # worker closed
                    self.rejected.append(task)
            except Empty:  # no tasks available
                continue

    def run(self):
        while self.state != STOPPED:
            workers = [w for w in self.pool[:] if not w.closed]
            available = self.free_workers(workers, 0.8)
            self.schedule_tasks(available, 0)


class PoolMaintainer(Thread):
    """Maintains the workers within the Pool."""
    def __init__(self, pool, rejected, workers, connection,
                 limit, initializer, initargs):
        Thread.__init__(self)
        self.daemon = True
        self.state = RUNNING
        self.pool = pool
        self.rejected = rejected
        self.workers = workers
        self.limit = limit
        self.initializer = initializer
        self.initargs = initargs
        self.connection = connection

    @staticmethod
    def expired_workers(workers, timeout):
        """Wait for expired workers.

        Expired workers are those which are not alive, which
        results channel has been closed and which current task
        has not started.
        """
        ##TODO: wait for SIGCHLD
        sleep(timeout)
        return [w for w in workers if not w.is_alive() and w.expired]

    @staticmethod
    def clean_workers(workers):
        """Join expired workers and collect pending tasks."""
        rejected = []

        for worker in workers:
            worker.join()
            rejected.extend(worker.queue)

        return rejected

    def respawn_workers(self):
        """Respawn missing workers."""
        pool = []

        for _ in range(self.workers - len(self.pool)):
            worker = Worker(self.connection.address, self.limit,
                            self.initializer, self.initargs)
            worker.start()
            worker.finalize(self.connection)
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
    def __init__(self, pool, queue, rejected):
        Thread.__init__(self)
        self.daemon = True
        self.state = RUNNING
        self.pool = pool
        self.queue = queue
        self.rejected = rejected

    @staticmethod
    def results(workers, timeout):
        """Wait for expired workers."""
        descriptors = [w.channel for w in workers if not w.expired]
        try:
            ready, _, _ = select(descriptors, [], [], timeout)
            return [w for w in workers if w.channel in ready]
        except:  # worker expired or stopped
            return []

    def finalize_tasks(self, tasks):
        for task in tasks:
            if task._callback is not None:
                try:
                    task._callback(task)
                except:
                    print_exc()
            self.queue.task_done()

    def run(self):
        while self.state != STOPPED:
            timestamp = time()
            workers = [w for w in self.pool[:] if not w.expired]
            tasks = [w.task_valid(timestamp) for w in workers]
            ready = self.results(workers, 0.6)
            tasks.extend([w.task_complete() for w in ready])
            self.finalize_tasks([t for t in tasks if t is not None])


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
        self._connection = Listener(family=FAMILY)
        self._rejected = deque()  # task enqueued on dead worker
        self._pool = []  # active workers container
        self._state = CREATED  # pool state flag
        self._task_scheduler = TaskScheduler(self._pool, self._queue,
                                             self._rejected)
        self._pool_maintainer = PoolMaintainer(self._pool, self._rejected,
                                               workers, self._connection,
                                               task_limit,
                                               initializer, initargs)
        self._results_fetcher = ResultsFetcher(self._pool, self._queue,
                                               self._rejected)
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

    def close(self):
        """Closes the pool waiting for all tasks to be completed."""
        self._state = CLOSING
        self._queue.join()
        self.stop()

    def stop(self):
        """Stops the pool without performing any pending task."""
        self._state = STOPPED
        self._task_scheduler.state = STOPPED
        self._pool_maintainer.state = STOPPED
        self._results_fetcher.state = STOPPED
        if self._pool_maintainer.is_alive():
            self._pool_maintainer.join()
        for w in self._pool:
            w.channel.close()
            w.terminate()
        self._connection.close()

    def kill(self):
        """Kills the pool forcing all workers to terminate immediately."""
        self._state = STOPPED
        self._task_scheduler.state = STOPPED
        self._pool_maintainer.state = STOPPED
        self._results_fetcher.state = STOPPED
        if self._pool_maintainer.is_alive():
            self._pool_maintainer.join()
        for w in self._pool:
            w.channel.close()
            w.kill()

    def join(self, timeout=0):
        """Joins the pool waiting until all workers exited.

        If *timeout* is greater than 0,
        it block until all workers exited or raise TimeoutError.

        """
        counter = 0

        if self._state == RUNNING:
            raise RuntimeError('The Pool is still running')
        # if timeout is set join workers until its value
        while counter < timeout and len(self._pool) > 0:
            counter += (len(self._pool) + 2) / 10.0
            if self._task_scheduler.is_alive():
                self._task_scheduler.join(0.1)
            if self._results_fetcher.is_alive():
                self._results_fetcher.join(0.1)
            self._pool = [w for w in self._pool
                          if w.join(0.1) is None and w.is_alive()]
        # verify timeout expired
        if timeout > 0 and counter >= timeout and self._pool:
            raise TimeoutError('Workers are still running')
        # timeout not set
        self._pool = [w for w in self._pool if w.join() is None
                      and w.is_alive()]

    def schedule(self, function, args=(), kwargs={}, callback=None, timeout=0):
        """Schedules *function* into the Pool, passing *args* and *kwargs*
        respectively as arguments and keyword arguments.

        If *callback* is a callable it will be executed once the function
        execution has completed with the returned *Task* as a parameter.

        *timeout* is an integer, if expires the task will be terminated
        and *Task.get()* will raise *TimeoutError*.

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
        if not isinstance(timeout, int):
            raise ValueError('timeout must be integer')
        task = Task(next(self._counter), function, args, kwargs,
                    callback, timeout)
        self._queue.put(task)

        return task
