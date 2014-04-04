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
import atexit

from time import time
from select import select
from itertools import count
from traceback import format_exc, print_exc
from threading import Thread
from collections import Callable, deque
from multiprocessing import Process
from multiprocessing.connection import Listener, Client
try:  # Python 2
    from Queue import Empty
    from cPickle import PicklingError
except:  # Python 3
    from queue import Empty
    from pickle import PicklingError

from .pebble import TimeoutError, Task, PoolContext

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


class ProcessWorker(Process):
    def __init__(self, address, limit=1, initializer=None, initargs=None):
        Process.__init__(self)
        self.address = address
        self.limit = limit
        self.initializer = initializer
        self.initargs = initargs
        self.daemon = True
        self.expire = None
        self.task_counter = count()  # task counter
        self.queue = deque()  # queued tasks
        self.channel = None  # task/result channel
        self.closed = False  # no more task allowed

    @property
    def expired(self):
        return self.channel.closed

    @property
    def counter(self):
        return next(self.task_counter)

    @property
    def current(self):
        try:
            return self.queue[0]
        except IndexError:
            return None

    def get_current(self):
        """Gets the task which is currently under processing."""
        try:
            return self.queue.popleft()
        except IndexError:
            return None

    def finalize(self, expire, channel):
        """Finalizes the worker, to be called after it has been started."""
        self.expire = expire
        self.channel = channel

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

    def schedule_task(self, task):
        """Sends a *Task* to the worker."""
        if len(self.queue) == 0:  # scheduled task is current one
            task._timestamp = time()
        self.queue.append(task)
        try:
            self.channel.send((task._function, task._args, task._kwargs))
            if self.limit > 0 and self.counter >= self.limit - 1:
                self.closed = True
        except Exception:  # worker closed
            self.queue.popleft()
            self.expire.set()
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
            self.expire.set()

        return task

    def run(self):
        """Worker process logic."""
        error = None
        results = None
        signal(SIGINT, SIG_IGN)

        def exit_function(channel):
            ## TODO: deinitializer
            if channel is not None:
                channel.close()
        atexit.register(exit_function, self.channel)

        try:
            self.channel = Client(self.address)
        except:  # main process shutdown
            sys.exit(1)

        if self.initializer is not None:  # run initializer function
            try:
                self.initializer(*self.initargs)
            except Exception as err:
                error = err
                error.traceback = format_exc()

        while self.limit == 0 or self.counter < self.limit:
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

        sys.exit(0)


class TaskScheduler(Thread):
    """Sends tasks to the workers."""
    def __init__(self, context, rejected):
        Thread.__init__(self)
        self.daemon = True
        self.context = context
        self.rejected = rejected

    @staticmethod
    def free_workers(workers, timeout):
        """Wait for available workers."""
        descriptors = [w.channel for w in workers if not w.closed]
        try:
            timeout = len(descriptors) > 0 and timeout or 0.01
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
                    task = self.context.queue.get(timeout=timeout)
                try:
                    worker.schedule_task(task)
                except RuntimeError:  # worker closed
                    self.rejected.append(task)
            except Empty:  # no tasks available
                continue

    def run(self):
        while self.context.state != STOPPED:
            available = self.free_workers(self.context.pool[:], 0.8)
            self.schedule_tasks(available, 0.6)


class ResultsFetcher(Thread):
    """Fetches results from the pool and forwards them to their tasks."""
    def __init__(self, context, rejected):
        Thread.__init__(self)
        self.daemon = True
        self.context = context
        self.rejected = rejected

    @staticmethod
    def results(workers, timeout):
        """Wait for expired workers."""
        descriptors = [w.channel for w in workers if not w.expired]
        try:
            timeout = len(descriptors) > 0 and timeout or 0.01
            ready, _, _ = select(descriptors, [], [], timeout)
            return [w for w in workers if w.channel in ready]
        except:  # worker expired or stopped
            return []

    @staticmethod
    def current_task_valid(workers):
        """Check if current tasks have been cancelled or timing out."""
        tasks = []
        timestamp = time()

        for worker in workers:
            current = worker.current
            if current is not None:
                if (current.timeout > 0 and
                    timestamp - current._timestamp > current.timeout):
                    worker.stop()
                    current._set(TimeoutError("Task timeout"))
                    tasks.append(worker.get_current())
                elif current.cancelled:
                    worker.stop()
                    tasks.append(worker.get_current())

        return tasks

    def finalize_tasks(self, tasks):
        for task in tasks:
            if task._callback is not None:
                try:
                    task._callback(task)
                except:
                    print_exc()
            self.context.queue.task_done()

    def run(self):
        while self.context.state != STOPPED:
            workers = [w for w in self.context.pool[:] if not w.expired]
            tasks = self.current_task_valid(workers)
            ready = self.results(workers, 0.6)
            tasks.extend([w.task_complete() for w in ready])
            self.finalize_tasks([t for t in tasks if t is not None])


class PoolMaintainer(Thread):
    """Maintains the workers within the Pool."""
    def __init__(self, context, rejected, connection,
                 initializer, initargs):
        Thread.__init__(self)
        self.daemon = True
        self.context = context
        self.rejected = rejected
        self.initargs = initargs
        self.initializer = initializer
        self.connection = connection

    @staticmethod
    def clean_workers(workers):
        """Join expired workers and collect pending tasks."""
        rejected = []

        for worker in workers:
            worker.join()
            rejected.extend(worker.queue)

        return rejected

    def expired_workers(self, workers, timeout):
        """Wait for expired workers.

        Expired workers are those which are not alive, which
        results channel has been closed and which current task
        has not started.
        """
        self.context.expired_workers.wait(timeout)
        self.context.expired_workers.clear()
        return [w for w in workers if not w.is_alive() and w.expired]

    def respawn_workers(self):
        """Respawn missing workers."""
        pool = []

        for _ in range(self.context.workers - len(self.context.pool)):
            worker = ProcessWorker(self.connection.address,
                                   self.context.limit,
                                   self.initializer, self.initargs)
            worker.start()
            worker_channel = self.connection.accept()
            worker.finalize(self.context.expired_workers, worker_channel)
            pool.append(worker)  # add worker to pool

        return pool

    def run(self):
        while self.context.state != STOPPED:
            respawned = self.respawn_workers()
            self.context.pool.extend(respawned)
            expired = self.expired_workers(self.context.pool[:], 0.8)
            for worker in expired:
                self.context.pool.remove(worker)
            rejected_tasks = self.clean_workers(expired)
            self.rejected.extend(rejected_tasks)


class ProcessPool(object):
    def __init__(self, workers=1, task_limit=0, queue=None, queueargs=None,
                 initializer=None, initargs=None):
        self._context = PoolContext(CREATED, workers, task_limit,
                                    queue, queueargs)
        self._connection = Listener(family=FAMILY)
        self._rejected = deque()  # task enqueued on dead worker
        self._task_scheduler = TaskScheduler(self._context, self._rejected)
        self._pool_maintainer = PoolMaintainer(self._context, self._rejected,
                                               self._connection, initializer,
                                               initargs)
        self._results_fetcher = ResultsFetcher(self._context, self._rejected)
        self.initializer = initializer
        self.initargs = initargs

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
        self.join()

    def _start(self):
        """Starts the pool."""
        self._task_scheduler.start()
        self._pool_maintainer.start()
        self._results_fetcher.start()
        self._context.state = RUNNING

    @staticmethod
    def _join_workers(workers, timeout=None):
        counter = 0

        while len(workers) > 0 and (timeout is None or counter < timeout):
            for worker in workers[:]:
                worker.join(timeout is not None and 0.1 or None)
                if not worker.is_alive():
                    workers.remove(worker)
            counter += timeout is not None and (len(workers)) / 10.0 or 0

        return workers

    @property
    def active(self):
        return self._context.state == RUNNING and True or False

    def close(self):
        """Closes the pool waiting for all tasks to be completed."""
        self._context.state = CLOSING
        self._context.queue.join()
        self.stop()

    def stop(self):
        """Stops the pool without performing any pending task."""
        self._context.state = STOPPED
        if self._pool_maintainer.is_alive():
            self._pool_maintainer.join()
        for w in self._context.pool:
            w.channel.close()
            w.terminate()

    def kill(self):
        """Kills the pool forcing all workers to terminate immediately."""
        self._context.state = STOPPED
        if self._pool_maintainer.is_alive():
            self._pool_maintainer.join()
        for w in self._context.pool:
            w.channel.close()
            w.kill()

    def join(self, timeout=0):
        """Joins the pool waiting until all workers exited.

        If *timeout* is greater than 0,
        it block until all workers exited or raise TimeoutError.

        """
        if self._context.state == RUNNING:
            raise RuntimeError('The Pool is still running')

        self._connection.close()

        if timeout > 0:
            counter = 0

            # wait for Pool threads
            while (self._task_scheduler.is_alive() or
                   self._results_fetcher.is_alive()) and counter < timeout:
                self._task_scheduler.join(0.1)
                self._results_fetcher.join(0.1)
                counter += 0.2
            # wait for Pool processes
            timeout = counter < timeout and timeout - counter or 0
            self._context.pool = self._join_workers(self._context.pool[:],
                                                    timeout)
            # verify timeout expired
            if len(self._context.pool) > 0:
                raise TimeoutError('Workers are still running')
        else:
            if (self._task_scheduler.is_alive() or
                self._results_fetcher.is_alive()):
                self._task_scheduler.join()
                self._results_fetcher.join()
            self._context.pool = self._join_workers(self._context.pool[:])

    def schedule(self, function, args=(), kwargs={}, identifier=None,
                 callback=None, timeout=0):
        """Schedules *function* into the Pool, passing *args* and *kwargs*
        respectively as arguments and keyword arguments.

        If *callback* is a callable it will be executed once the function
        execution has completed with the returned *Task* as a parameter.

        *timeout* is an integer, if expires the task will be terminated
        and *Task.get()* will raise *TimeoutError*.

        The *identifier* will be forwarded to the *Task*, if None a random
        UUID will be provided.

        A *Task* object is returned.

        """
        # start the pool at first call
        if self._context.state == CREATED:
            self._start()
        elif self._context.state != RUNNING:
            raise RuntimeError('The Pool is not running')
        if not isinstance(function, Callable):
            raise ValueError('function must be callable')
        if not isinstance(timeout, int):
            raise ValueError('timeout must be integer')
        task = Task(next(self._context.counter), function, args, kwargs,
                    callback, timeout, identifier)
        self._context.queue.put(task)

        return task
