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


import gc
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

from .pebble import TimeoutError, Task, PoolContext, PoolManager
from .pebble import STOPPED, RUNNING, CLOSING, CREATED

### platform dependent code ###
if os.name in ('posix', 'os2'):
    FAMILY = 'AF_UNIX'
    from signal import SIG_IGN, SIGKILL, SIGINT, signal
else:
    FAMILY = 'AF_INET'
    from signal import SIG_IGN, SIGINT, signal


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
            return not self.is_alive() or self.counter >= self.limit
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
        self.join()

    def kill(self):
        """Kills the process."""
        try:
            os.kill(self.pid, SIGKILL)
        except:  # process already dead or Windows platform
            pass

    def schedule_task(self, task):
        """Sends a *Task* to the worker."""
        if self.current is None:
            task._timestamp = time()
        self.queue.append(task)

        try:
            self.channel.send((task._function, task._args, task._kwargs))
        except (IOError, OSError):  # process killed (task timeout/cancelled)
            self.queue.pop()
            task._timestamp = 0
            raise RuntimeError('Worker stopped')

        self.counter += 1

    def task_complete(self):
        """Retrieves results from channel.

        If the Worker has expired raises RuntimeError.

        """
        results = None

        try:
            results = self.channel.recv()
        except EOFError:  # process expired
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

        # connect to parent process (Pool)
        try:
            self.channel = Client(self.address)
        except:  # main process shutdown
            sys.exit(1)

        # run initializer function
        if self.initializer is not None:
            try:
                self.initializer(*self.initargs)
            except Exception as err:
                error = err
                error.traceback = format_exc()

        # install deinitializers
        def exit_function(channel):
            ## TODO: deinitializer
            channel.close()
        atexit.register(exit_function, self.channel)

        # main process loop
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

        self.channel.close()
        sys.exit(0)


class ProcessPoolManager(PoolManager):
    """ProcessPool management routine."""
    def __init__(self, context, pending, connection):
        PoolManager.__init__(self, context)
        self.pending = pending
        self.connection = connection

    def cleanup_workers(self, expired):
        pool = self.context.pool

        for worker in expired:
            worker.join()
            self.pending.extend(worker.queue)
            pool.remove(worker)

        # force garbage collection as Python3 is a bit lazy
        gc.collect()

    def spawn_workers(self):
        """Spawns missing Workers."""
        pool = self.context.pool

        for _ in range(self.context.workers - len(pool)):
            worker = ProcessWorker(self.connection.address,
                                   self.context.limit,
                                   self.context.initializer,
                                   self.context.initargs)
            worker.start()
            worker.finalize(self.connection.accept())

            pool.append(worker)


class TaskScheduler(Thread):
    """Schedule the *Tasks* whithin the workers.

    Ensures Workers to have always a ready task, if available.

    """
    def __init__(self, context, pending):
        Thread.__init__(self)
        self.daemon = True
        self.context = context
        self.pending = pending

    def wait_for_channel(self, workers, timeout):
        """Waits for free channels to send new tasks."""
        channels = [w.channel for w in workers]

        timeout = len(channels) > 0 and timeout or 0.01
        try:
            _, ready, _ = select([], channels, [], timeout)
        except:  # select on expired worker
            return []

        return [w for w in workers if w.channel in ready]

    def schedule_tasks(self, workers, timeout):
        queue = self.context.queue

        for worker in workers:
            try:
                try:  # first get any pending task
                    task = self.pending.popleft()
                except IndexError:  # then get enqueued ones
                    task = queue.get(timeout=timeout)
                try:
                    worker.schedule_task(task)
                except RuntimeError:  # worker expired
                    self.context.workers_event.set()
                    self.pending.appendleft(task)
            except Empty:  # no tasks available
                continue

    def run(self):
        pool = self.context.pool

        while self.context.state != STOPPED:
            workers = [w for w in pool[:] if not w.closed]
            ready_workers = self.wait_for_channel(workers, 0.6)
            self.schedule_tasks(ready_workers, 0.2)


class ResultsManager(Thread):
    """Collects results from Workers.

    Handles timeout and cancelled tasks.
    Collects results.
    Runs callbacks.

    """
    def __init__(self, context):
        Thread.__init__(self)
        self.daemon = True
        self.context = context

    def done_tasks(self, workers):
        queue = self.context.queue
        workers_event = self.context.workers_event

        for worker in workers:
            try:
                task, results = worker.task_complete()

                task._set(results)
                if task._callback is not None:
                    self.task_callback(task)

                queue.task_done()
            except RuntimeError:  # worker expired
                workers_event.set()

    def timeout_tasks(self, workers):
        queue = self.context.queue
        workers_event = self.context.workers_event

        for worker in workers:
            task = worker.get_current()

            worker.stop()
            workers_event.set()

            task._set(TimeoutError('Task timeout'))
            if task._callback is not None:
                self.task_callback(task)

            queue.task_done()

    def cancelled_tasks(self, workers):
        queue = self.context.queue
        workers_event = self.context.workers_event

        for worker in workers:
            task = worker.get_current()

            worker.stop()
            workers_event.set()

            if task._callback is not None:
                self.task_callback(task)

            queue.task_done()

    def task_callback(self, task):
        try:
            task._callback(task)
        except Exception:
            print_exc()
            ## TODO: context state == ERROR
            # self.context.state = ERROR

    def problematic_tasks(self, workers):
        """Check for timeout or cancelled tasks
        and generates related events.

        """
        timeout = lambda c, t: t - c._timestamp > c.timeout

        timestamp = time()
        timeout_workers = [w for w in workers if w.current is not None and
                           w.current.timeout > 0 and w.current.started and
                           timeout(w.current, timestamp)]
        self.timeout_tasks(timeout_workers)

        cancelled_workers = [w for w in workers if w.current is not None and
                             w.current.started and w.current._cancelled]
        self.cancelled_tasks(cancelled_workers)

    def wait_for_result(self, workers, timeout):
        """Waits for results to be ready and generates related events.

        *timeout* is the amount of time to wait for any result to be ready.

        """
        channels = [w.channel for w in workers]

        timeout = len(channels) > 0 and timeout or 0.01
        try:
            ready, _, _ = select(channels, [], [], timeout)
        except:  # select on expired worker
            return []

        return [w for w in workers if w.channel in ready]

    def run(self):
        workers = lambda: [w for w in self.context.pool[:] if not w.expired]

        while self.context.state != STOPPED:
            self.problematic_tasks(workers())
            ready_workers = self.wait_for_result(workers(), 0.8)
            self.done_tasks(ready_workers)


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
        self._context = PoolContext(workers, task_limit, queue, queueargs,
                                    initializer, initargs)
        self._connection = Listener(family=FAMILY)
        self._pending = deque()  # task enqueued on dead worker
        self._pool_manager = ProcessPoolManager(self._context, self._pending,
                                                self._connection)
        self._task_scheduler = TaskScheduler(self._context, self._pending)
        self._results_manager = ResultsManager(self._context)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
        self.join()

    def _start(self):
        """Starts the pool."""
        self._pool_manager.start()
        self._task_scheduler.start()
        self._results_manager.start()
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
        if (self._pool_manager.is_alive() or self._task_scheduler.is_alive() or
            self._results_manager.is_alive()):
            self._pool_manager.join()
            self._task_scheduler.join()
            self._results_manager.join()

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
        self._join_managers()

        for w in self._context.pool:
            w.channel.close()
            w.terminate()

    def kill(self):
        """Kills the pool forcing all workers to terminate immediately."""
        self._context.state = STOPPED
        self._join_managers()

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
            self._context.pool = self._join_workers(timeout)
            # verify timeout expired
            if len(self._context.pool) > 0:
                raise TimeoutError('Workers are still running')
        else:
            self._context.pool = self._join_workers()

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
