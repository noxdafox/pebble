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

from .pebble import TimeoutError, Task, PoolContext, coroutine

### platform dependent code ###
if os.name in ('posix', 'os2'):
    FAMILY = 'AF_UNIX'
    from signal import SIG_IGN, SIGKILL, SIGINT, signal
else:
    FAMILY = 'AF_INET'
    from signal import SIG_IGN, SIGINT, signal


# Pool states
STOPPED = 0
RUNNING = 1
CLOSING = 2
CREATED = 3


def spawn_worker(context, connection):
    """Spawns a new Worker process.

    *context* is the PoolContext object.
    *connection* is the Listener object to set up Pool - Worker channels.
    Returns the new started *Worker*.

    """
    worker = ProcessWorker(connection.address, context.limit,
                           context.initializer, context.initargs)
    worker.start()
    worker_channel = connection.accept()
    worker.finalize(worker_channel)

    return worker


@coroutine
def schedule_task(queue, pending, timeout):
    """Coroutine for Pool task scheduling.

    For each worker schedules a task from the queue if available.
    Blocks until a new *Task* is available or *timeout* expires.

    *queue* Pool queue of *Tasks*.
    *pending* queue of pending tasks as pending by other Workers.
    *timeout* Pool queue get timeout.

    """
    while 1:
        worker = (yield)

        try:
            try:  # first get any pending task
                task = pending.popleft()
            except IndexError:  # then get enqueued ones
                task = queue.get(timeout=timeout)
            try:
                worker.schedule_task(task)
            except RuntimeError:  # worker expired
                pending.appendleft(task)
        except Empty:  # no tasks available
            continue


@coroutine
def task_done(queue, callback_handler, worker_expired):
    """Coroutine for Pool results fetching.

    For each worker, fetches the results, sets the *Task*,
    delivers it to the callback handler and aknowledges it
    with the Pool's *Task* queue.

    If the Worker has expired it will forward it to the Workers manager
    coroutine.

    *queue* Pool queue of *Tasks*.
    *callback_handler* coroutine for the handling of *Tasks* callback.
    *worker_expired* coroutine for the handling of expired Workers.

    """
    while 1:
        worker = (yield)

        try:
            task, results = worker.task_complete()
            task._set(results)
            callback_handler.send(task)
            queue.task_done()
        except RuntimeError:  # worker expired
            worker_expired.send(worker)


@coroutine
def task_timeout(queue, callback_handler, worker_expired):
    """Coroutine for *Task* timeout handling.

    Stops each worker, respawn new ones, collects back the *Task*
    which timeout has expired, sets it, forwards it to the callbacks
    handler coroutine and aknowledges it to the Pool's *Task* queue.

    *queue* Pool queue of *Tasks*.
    *callback_handler* coroutine for the handling of *Tasks* callback.
    *worker_expired* coroutine for the handling of expired Workers.

    """
    while 1:
        worker = (yield)
        task = worker.get_current()

        worker.stop()
        worker_expired.send(worker)
        task._set(TimeoutError('Task timeout'))
        callback_handler.send(task)
        queue.task_done()


@coroutine
def task_cancelled(queue, callback_handler, worker_expired):
    """Coroutine for cancelled *Tasks*.

    Stops each worker, respawn new ones, collects back the cancelled *Task*,
    sets it, forwards it to the callbacks handler coroutine
    and aknowledges it to the Pool's *Task* queue.

    *queue* Pool queue of *Tasks*.
    *callback_handler* coroutine for the handling of *Tasks* callback.
    *worker_expired* coroutine for the handling of expired Workers.

    """
    while 1:
        worker = (yield)
        task = worker.get_current()

        worker.stop()
        worker_expired.send(worker)
        callback_handler.send(task)
        queue.task_done()


@coroutine
def worker_expired(context, connection, pending):
    """Coroutine for expired Workers.

    For each Worker, joins its exit, collects back any pending *Task*,
    removes it from the Pool and spawns a new one.

    *context* is the PoolContext object.
    *connection* is the Listener object to set up Pool - Worker channels.
    *pending* queue of pending tasks as pending by other Workers.

    """
    pool = context.pool

    while 1:
        worker = (yield)

        worker.join()
        pending.extend(worker.queue)
        pool.remove(worker)
        pool.append(spawn_worker(context, connection))


@coroutine
def task_callback():
    """Coroutine for *Tasks* callback.

    For each *Task*, if its callback is set, it runs it.

    """
    while 1:
        task = (yield)

        if task._callback is not None:
            try:
                task._callback(task)
            except Exception:
                print_exc()
                ## TODO: context state == ERROR
                # self.context.state = ERROR


class ProcessWorker(Process):
    """Worker class for processes."""
    def __init__(self, address, limit=0, initializer=None, initargs=None):
        Process.__init__(self)
        self.address = address
        self.limit = limit
        self.initializer = initializer
        self.initargs = initargs
        self.daemon = True
        self.counter = 0  # task counter
        self.queue = deque()  # queued tasks
        self.channel = None  # task/result channel

    @property
    def closed(self):
        """A Worker is closed if no more *Tasks*
        can be delivered to its process.

        """
        if self.limit > 0:
            return not self.is_alive() and self.counter >= self.limit
        else:
            return not self.is_alive()

    @property
    def expired(self):
        """A Worker is expired if its process is not alive
        and there are no more results on its channel.

        """
        return not self.is_alive() and self.channel.closed

    @property
    def current(self):
        """Ongoing *Task*."""
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
        self.channel.close()
        self.terminate()
        self.join(3)
        if self.is_alive():
            self.kill()

    def kill(self):
        """Kills the process."""
        try:
            os.kill(self.pid, SIGKILL)
        except:  # process already dead or Windows platform
            self.terminate()  # one more try

    def schedule_task(self, task):
        """Sends a *Task* to the worker."""
        if len(self.queue) == 0:  # scheduled task is current one
            task._timestamp = time()
        self.queue.append(task)

        try:
            self.channel.send((task._function, task._args, task._kwargs))
        except (IOError, OSError):
            self.queue.pop()
            task._timestamp = 0
            self.channel.close()
            raise RuntimeError('Worker stopped')

        self.counter += 1

    def task_complete(self):
        """Retrieves results from channel.

        If the Worker has expired raises RuntimeError.

        """
        results = None

        try:
            results = self.channel.recv()
        except (EOFError, IOError):  # process expired
            self.channel.close()
            raise RuntimeError('Worker expired')

        task = self.get_current()
        if self.current is not None:
            self.current._timestamp = time()

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

            self.counter += 1
            error = results = None

        sys.exit(0)


class PoolManager(Thread):
    """Main Pool management routine.

    Collects expired workers and respawn them.
    Checks for timeout/cancelled tasks.
    Fetches the results and forwards them to the *Tasks*.
    Runs callbacks.

    """
    def __init__(self, context, pending, connection):
        Thread.__init__(self)
        self.daemon = True
        self.context = context
        self.pending = pending
        self.connection = connection

    @staticmethod
    def restart_workers(workers, handler):
        """Restart the expired workers."""
        for worker in [w for w in workers if w.expired]:
            handler.send(worker)

    @staticmethod
    def problematic_tasks(workers, timeout_handler, cancel_handler):
        """Check for timeout or cancelled tasks
        and generates related events.

        """
        timestamp = time()
        timeout = lambda c, t: c.timeout and t - c._timestamp > c.timeout

        timeout_workers = [w for w in workers if w.current is not None
                           and timeout(w.current, timestamp)]
        for worker in timeout_workers:
            timeout_handler.send(worker)

        cancelled_workers = [w for w in workers if w.current is not None and
                             w.current._cancelled]
        for worker in cancelled_workers:
            cancel_handler.send(worker)

    @staticmethod
    def fetch_results(workers, timeout, results_handler):
        """Waits for results to be ready and generates related events.

        *timeout* is the amount of time to wait for any result to be ready.

        """
        channels = [w.channel for w in workers if not w.expired]

        timeout = len(channels) > 0 and timeout or 0.01
        try:
            ready, _, _ = select(channels, [], [], timeout)
        except:  # select on expired worker
            return []

        for worker in [w for w in workers if w.channel in ready]:
            results_handler.send(worker)

    def run(self):
        pool = self.context.pool
        queue = self.context.queue

        # initialize coroutines
        callback_manager = task_callback()
        workers_manager = worker_expired(self.context, self.connection,
                                         self.pending)
        results_manager = task_done(queue, callback_manager, workers_manager)
        timeout_manager = task_timeout(queue, callback_manager,
                                       workers_manager)
        cancel_manager = task_cancelled(queue, callback_manager,
                                        workers_manager)

        # main loop
        while self.context.state != STOPPED:
            self.restart_workers(pool, workers_manager)
            self.problematic_tasks(pool, timeout_manager, cancel_manager)
            self.fetch_results(pool, 0.8, results_manager)

        # deinitialize coroutines
        callback_manager.close()
        workers_manager.close()
        results_manager.close()
        timeout_manager.close()
        cancel_manager.close()


class TaskScheduler(Thread):
    """Schedule the *Tasks* whithin the workers.

    Ensures Workers to have always a ready task, if available.

    """
    def __init__(self, context, pending):
        Thread.__init__(self)
        self.daemon = True
        self.context = context
        self.pending = pending

    @staticmethod
    def schedule_ready(workers, timeout):
        """Waits for free channels to send new tasks."""
        channels = [w.channel for w in workers if not w.closed]

        timeout = len(channels) > 0 and timeout or 0.01
        try:
            _, ready, _ = select([], channels, [], timeout)
        except:  # select on expired worker
            return []

        return [w for w in workers if w.channel in ready]

    def run(self):
        pool = self.context.pool
        # initialize coroutines
        task_scheduler = schedule_task(self.context.queue, self.pending, 0.8)

        # scheduling loop
        while self.context.state != STOPPED:
            for worker in self.schedule_ready(pool[:], 0.2):
                task_scheduler.send(worker)

        # deinitialize coroutines
        task_scheduler.close()


class ProcessPool(object):
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
                 initializer=None, initargs=None):
        self._context = PoolContext(CREATED, workers, task_limit,
                                    queue, queueargs, initializer, initargs)
        self._connection = Listener(family=FAMILY)
        self._pending = deque()  # task enqueued on dead worker
        self._pool_manager = PoolManager(self._context, self._pending,
                                         self._connection)
        self._task_scheduler = TaskScheduler(self._context, self._pending)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
        self.join()

    def _start(self):
        """Starts the pool."""
        # spawn workers
        for _ in range(self._context.workers):
            self._context.pool.append(spawn_worker(self._context,
                                                   self._connection))
        # start maintenance routines
        self._pool_manager.start()
        self._task_scheduler.start()
        self._context.state = RUNNING

    @staticmethod
    def _join_workers(workers, timeout=None):
        """Join terminated workers."""
        counter = 0

        while len(workers) > 0 and (timeout is None or counter < timeout):
            for worker in workers[:]:
                worker.join(timeout is not None and 0.1 or None)
                if not worker.is_alive():
                    workers.remove(worker)
            counter += timeout is not None and (len(workers)) / 10.0 or 0

        return workers

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
        """Closes the pool waiting for all tasks to be completed."""
        self._context.state = CLOSING
        self._context.queue.join()
        self.stop()

    def stop(self):
        """Stops the pool without performing any pending task."""
        self._context.state = STOPPED
        if (self._pool_manager.is_alive() or self._task_scheduler.is_alive()):
            self._pool_manager.join()
            self._task_scheduler.join()

        for w in self._context.pool:
            w.channel.close()
            w.terminate()

    def kill(self):
        """Kills the pool forcing all workers to terminate immediately."""
        self._context.state = STOPPED
        if (self._pool_manager.is_alive() or self._task_scheduler.is_alive()):
            self._pool_manager.join()
            self._task_scheduler.join()

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
