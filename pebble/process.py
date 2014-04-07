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
    from Queue import Queue, Empty
    from cPickle import PicklingError
except:  # Python 3
    from queue import Queue, Empty
    from pickle import PicklingError

from .pebble import TimeoutError, TaskCancelled, Task, PoolContext, coroutine

### platform dependent code ###
if os.name in ('posix', 'os2'):
    FAMILY = 'AF_UNIX'
    from signal import SIG_IGN, SIGKILL, SIGINT, signal
else:
    FAMILY = 'AF_INET'
    from signal import SIG_IGN, SIGINT, signal


SEND = 0
RECV = 1
TIMEOUT = 2
CANCELLED = 3
EXPIRED = 4


STOPPED = 0
RUNNING = 1
CLOSING = 2
CREATED = 3


@coroutine
def schedule_task(queue, rejected, timeout):
    while 1:
        worker = (yield)

        try:
            try:  # first get any rejected task
                task = rejected.popleft()
            except IndexError:  # then get enqueued ones
                task = queue.get(timeout=timeout)
            try:
                worker.schedule_task(task)
            except RuntimeError:  # worker expired
                rejected.appendleft(task)
        except Empty:  # no tasks available
            continue


@coroutine
def task_done(queue, callback_handler):
    while 1:
        worker = (yield)

        try:
            task, results = worker.task_complete()
            task._set(results)
            callback_handler.send(task)
            queue.task_done()
        except RuntimeError:  # worker expired
            pass


@coroutine
def task_timeout(callback_handler):
    while 1:
        worker = (yield)

        worker.stop()
        task = worker.get_current()
        task._set(TimeoutError('Task timeout'))
        callback_handler.send(task)


@coroutine
def task_cancelled(callback_handler):
    while 1:
        worker = (yield)

        worker.stop()
        task = worker.get_current()
        task._set(TaskCancelled('Task cancelled'))
        callback_handler.send(task)


@coroutine
def worker_expired(pool, rejected):
    while 1:
        worker = (yield)

        worker.join()
        rejected.extend(worker.queue)
        pool.remove(worker)


@coroutine
def task_callback():
    while 1:
        task = (yield)

        if task._callback is not None:
            try:
                task._callback(task)
            except Exception:
                print_exc()
                # self.context.state = ERROR


class ProcessWorker(Process):
    def __init__(self, address, limit=1, initializer=None, initargs=None):
        Process.__init__(self)
        self.address = address
        self.limit = limit
        self.initializer = initializer
        self.initargs = initargs
        self.daemon = True
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

    def finalize(self, channel):
        """Finalizes the worker, to be called after it has been started."""
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
        except (IOError, OSError):
            self.queue.pop()
            task._timestamp = 0
            raise RuntimeError('Worker stopped')

    def task_complete(self):
        """Waits for the next task to be completed and sets the task.
        Blocks until any result is ready or *timeout* expires.

        Raises TimeoutError if *timeout* expired.

        """
        task = None
        results = None

        try:
            task = self.queue.popleft()
        except IndexError:  # process expired
            pass
        try:
            results = self.channel.recv()
            if len(self.queue) > 0:
                self.queue[0]._timestamp = time()
        except (EOFError, IOError):  # process expired
            self.channel.close()
            raise RuntimeError('Worker expired')

        return task, results

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


class PoolManager(Thread):
    """Sends tasks to the workers."""
    def __init__(self, context, rejected, connection):
        Thread.__init__(self)
        self.daemon = True
        self.context = context
        self.rejected = rejected
        self.connection = connection
        self.event_handlers = None

    def pool_events(self, timeout):
        events = []  # (event, worker)

        events.extend([(EXPIRED, w) for w in self.context.pool if w.expired])
        events.extend(self.problematic_tasks())
        events.extend(self.results_ready(timeout))

        return events

    def problematic_tasks(self):
        pool = self.context.pool
        timestamp = time()
        timeout = lambda c, t: c.timeout and t - c._timestamp > c.timeout

        timeout_events = [(TIMEOUT, w) for w in pool if w.current is not None
                          and timeout(w.current, timestamp)]
        cancelled_events = [(CANCELLED, w) for w in pool
                            if w.current is not None and w.current._cancelled]

        return timeout_events + cancelled_events

    def results_ready(self, timeout):
        pool = self.context.pool
        channels = [w.channel for w in pool if not w.expired]

        timeout = len(channels) > 0 and timeout or 0.01
        try:
            ready, _, _ = select(channels, [], [], timeout)
        except:  # select on expired worker
            return []

        return [(RECV, w) for w in pool if w.channel in ready]

    def respawn_workers(self):
        """Respawn missing workers."""
        pool = []

        for _ in range(self.context.workers - len(self.context.pool)):
            worker = ProcessWorker(self.connection.address,
                                   self.context.limit,
                                   None, None)
            worker.start()
            worker_channel = self.connection.accept()
            worker.finalize(worker_channel)
            pool.append(worker)  # add worker to pool

        return pool

    def run(self):
        # initialize coroutines
        callback_manager = task_callback()
        results_fetcher = task_done(self.context.queue, callback_manager)
        timeout_manager = task_timeout(callback_manager)
        cancellation_manager = task_cancelled(callback_manager)
        workers_manager = worker_expired(self.context.pool, self.rejected)
        self.events_handlers = (callback_manager,
                                results_fetcher,
                                timeout_manager,
                                cancellation_manager,
                                workers_manager)

        while self.context.state != STOPPED:
            self.context.pool.extend(self.respawn_workers())
            events = self.pool_events(0.8)
            for event, worker in events:
                self.events_handlers[event].send(worker)

        for handler in self.events_handlers:
            handler.close()
        callback_manager.close()


class TaskScheduler(Thread):
    """Sends tasks to the workers."""
    def __init__(self, context, rejected):
        Thread.__init__(self)
        self.daemon = True
        self.context = context
        self.rejected = rejected
        self.event_handlers = None

    @staticmethod
    def schedule_ready(workers, timeout):
        channels = [w.channel for w in workers if not w.closed]

        timeout = len(channels) > 0 and timeout or 0.01
        try:
            _, ready, _ = select([], channels, [], timeout)
        except:  # select on expired worker
            return []

        return [(SEND, w) for w in workers if w.channel in ready]

    def pool_events(self, timeout):
        events = []  # (event, worker)

        events.extend(self.schedule_ready(self.context.pool, timeout))

        return events

    def run(self):
        # initialize coroutines
        task_scheduler = schedule_task(self.context.queue, self.rejected, 0.8)
        self.events_handlers = (task_scheduler, )

        while self.context.state != STOPPED:
            events = self.pool_events(0.2)
            for event, worker in events:
                self.events_handlers[event].send(worker)

        for handler in self.events_handlers:
            handler.close()


class ProcessPool(object):
    def __init__(self, workers=1, task_limit=0, queue=None, queueargs=None,
                 initializer=None, initargs=None):
        self._context = PoolContext(CREATED, workers, task_limit,
                                    queue, queueargs)
        self._connection = Listener(family=FAMILY)
        self._rejected = deque()  # task enqueued on dead worker
        self._event_queue = Queue()  # events PoolManager and TaskManager
        self._pool_manager = PoolManager(self._context, self._rejected,
                                         self._connection)
        self._task_scheduler = TaskScheduler(self._context, self._rejected)
        # self.initializer = initializer
        # self.initargs = initargs

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
        self.join()

    def _start(self):
        """Starts the pool."""
        self._pool_manager.start()
        self._task_scheduler.start()
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
        if self._pool_manager.is_alive():
            self._pool_manager.join()
        for w in self._context.pool:
            w.channel.close()
            w.terminate()

    def kill(self):
        """Kills the pool forcing all workers to terminate immediately."""
        self._context.state = STOPPED
        if self._pool_manager.is_alive():
            self._pool_manager.join()
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
            # wait for Pool processes
            self._context.pool = self._join_workers(self._context.pool[:],
                                                    timeout)
            # verify timeout expired
            if len(self._context.pool) > 0:
                raise TimeoutError('Workers are still running')
        else:
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
        task = Task(self._context.counter, function, args, kwargs,
                    callback, timeout, identifier)
        self._context.queue.put(task)

        return task
