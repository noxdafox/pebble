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

from inspect import isclass
from itertools import count
from time import sleep, time
from collections import Callable
from traceback import format_exc, print_exc
try:  # Python 2
    from Queue import Queue
    from cPickle import PicklingError
except:  # Python 3
    from queue import Queue
    from pickle import PicklingError

from .generic import SimpleQueue, suspend
from .worker import worker as process_worker
from ..thread import worker as thread_worker
from ..pebble import Task, TimeoutError, TaskCancelled
from ..pebble import STOPPED, RUNNING, CLOSED, CREATED, ERROR


### platform dependent code ###
if os.name in ('posix', 'os2'):
    from signal import SIG_IGN, SIGKILL, SIGINT, signal
else:
    from signal import SIG_IGN, SIGINT, signal


ACK = 0  # task aknowledged by worker
RES = 1  # task results from worker


def stop_worker(worker):
    """Does its best to stop the worker."""
    try:
        worker.terminate()
        worker.join(3)
        if worker.is_alive() and os.name != 'nt':
            os.kill(worker.pid, SIGKILL)
    except:
        print_exc()


def get_task(task_queue, result_queue):
    """Gets new task for Pool Worker.

    Waits for next task to be ready, unpacks its contents
    and sends back an acknowledgment to the Pool.

    """
    try:
        number, function, args, kwargs = task_queue.get()
        result_queue.put((ACK, number, os.getpid()))

        return number, function, args, kwargs
    except (IOError, OSError, EOFError):
        sys.exit(1)


def put_results(queue, number, results):
    """Sends results back to Pool."""
    try:
        queue.put((RES, number, results))
    except PicklingError as error:
        queue.put((RES, number, error))
    except (IOError, OSError, EOFError):
        sys.exit(1)


@process_worker(daemon=True)
def pool_worker(queues, initializer, initargs, limit):
    """Runs the actual function in separate process."""
    error = None
    results = None
    counter = count()
    signal(SIGINT, SIG_IGN)
    task_queue, result_queue = queues
    task_queue._writer.close()
    result_queue._reader.close()

    if initializer is not None:
        try:
            initializer(*initargs)
        except Exception as err:
            error = err
            error.traceback = format_exc()

    while limit == 0 or next(counter) < limit:
        number, function, args, kwargs = get_task(task_queue, result_queue)

        try:
            results = function(*args, **kwargs)
        except Exception as err:
            if error is None:
                error = err
                error.traceback = format_exc()

        put_results(result_queue, number,
                    error is not None and error or results)

        error = None
        results = None

    sys.exit(0)


class TaskQueue(SimpleQueue):
    def __init__(self):
        super(TaskQueue, self).__init__()
        self._wlock = None

    def _make_put_method(self):
        def put(obj):
            return self._writer.send(obj)

        return put


class ResultQueue(SimpleQueue):
    def __init__(self):
        super(ResultQueue, self).__init__()
        self._rlock = None

    def _make_get_method(self):
        def get():
            return self._reader.recv()

        return get


class PoolManager(object):
    def __init__(self, queue, queueargs, initializer, initargs,
                 workers, limit):
        self.state = CREATED
        self.pool = {}  # {pid: process}
        self.tasks = {}  # {task_number: task_obj}
        self.managers = None
        self.task_queue = TaskQueue()
        self.result_queue = ResultQueue()
        self.initializer = initializer
        self.initargs = initargs
        self.worker_number = workers
        self.worker_limit = limit
        if queue is not None:
            if isclass(queue):
                self.queue = queue(*queueargs)
            else:
                raise ValueError("Queue must be Class")
        else:
            self.queue = Queue()

    @thread_worker(daemon=True)
    def task_scheduler(self):
        """Schedules tasks in queue to the workers."""
        queue = self.queue
        tasks = self.tasks
        put = lambda o: self.task_queue.put(o)

        while self.state != STOPPED:
            task = queue.get()

            if task is not None:
                number = task.number
                tasks[number] = task

                put((number, task._function, task._args, task._kwargs))
            else:  # stop sentinel
                return

    @thread_worker(daemon=True)
    def task_manager(self):
        """Checks for tasks cancelled or timing out."""
        pool = self.pool
        tasks = self.tasks
        timeout = lambda t, timestamp: timestamp - t._timestamp > t.timeout

        while self.state != STOPPED:
            tick = time()

            for task in tasks.values():
                results = None

                if (task.timeout > 0 and task.started and timeout(task, tick)):
                    results = TimeoutError('Task timeout')
                elif task.started and task.cancelled:
                    results = TaskCancelled('Task cancelled')

                if results is not None:
                    with suspend(self.task_queue):
                        with suspend(self.result_queue):
                            stop_worker(pool[task._pid])

                    tasks.pop(task.number)
                    self.task_done(task, results)

            sleep(0.2)

    @thread_worker(daemon=True)
    def message_manager(self):
        """Gather messages from workers,
        sets tasks status and runs callbacks.

        """
        tasks = self.tasks
        get = lambda: self.result_queue.get()

        while self.state != STOPPED:
            try:
                message, number, data = get()
            except TypeError:  # stop sentinel
                return

            if message is ACK:
                task = tasks[number]
                task._timestamp = time()
                task._pid = data
            elif message is RES:
                task = tasks.pop(number)
                self.task_done(task, data)

    @thread_worker(daemon=True)
    def workers_manager(self):
        """Collects expired workers and spawn new ones."""
        pool = self.pool
        workers = self.worker_number
        queues = (self.task_queue, self.result_queue)

        while self.state != STOPPED:
            expired = [w for w in pool.values() if not w.is_alive()]

            for worker in expired:
                worker.join()
                pool.pop(worker.pid)

            for _ in range(workers - len(pool)):
                worker = pool_worker(queues, self.initializer, self.initargs,
                                     self.worker_limit)
                pool[worker.pid] = worker

            sleep(0.2)

    def start(self):
        """Start the Pool managers."""
        ts = self.task_scheduler()
        tm = self.task_manager()
        mm = self.message_manager()
        wm = self.workers_manager()
        self.managers = (ts, tm, mm, wm)

        self.state = RUNNING

    def stop(self):
        """Stop the pool."""
        self.state = STOPPED
        self.queue.put(None)
        self.result_queue.put(None)

        for manager in self.managers:
            manager.join()

        with suspend(self.queues[0]):
            with suspend(self.queues[1]):
                for worker in self.pool:
                    worker.terminate()

    def kill(self):
        """Forces all workers to stop."""
        self.stop()

        with suspend(self.workers_queue):
            for worker in self.pool:
                stop_worker(worker)

    def join(self, timeout):
        if self.state == RUNNING:
            raise RuntimeError('The Pool is still running')

        if timeout > 0:
            self.pool = self.join_workers(timeout)
            if len(self.pool) > 0:
                raise TimeoutError('Workers are still running')
        else:
            self.pool = self.join_workers()

    def join_workers(self, timeout=None):
        """Join terminated workers."""
        counter = 0
        workers = self.pool

        while len(workers) > 0 and (timeout is None or counter < timeout):
            for worker in workers[:]:
                worker.join(timeout is not None and 0.1 or None)
                if not worker.is_alive():
                    workers.remove(worker)
            counter += timeout is not None and (len(workers)) / 10.0 or 0

        return workers

    def task_done(self, task, results):
        task._set(results)

        if task._callback is not None:
            try:
                task._callback(task)
            except Exception:
                print_exc()
                self.state = ERROR

        self.queue.task_done()


class Pool(object):
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
        self._counter = count()
        self._pool_manager = PoolManager(queue, queueargs,
                                         initializer, initargs,
                                         workers, task_limit)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
        self.join()

    @property
    def initializer(self):
        return self._pool_manager.initializer

    @initializer.setter
    def initializer(self, value):
        self._pool_manager.initializer = value

    @property
    def initargs(self):
        return self._pool_manager.initargs

    @initargs.setter
    def initargs(self, value):
        self._pool_manager.initargs = value

    @property
    def active(self):
        return self._pool_manager.state == RUNNING and True or False

    def close(self):
        """Closes the pool waiting for all tasks to be completed."""
        self._pool_manager.state = CLOSED
        self._pool_manager.queue.join()
        self.stop()

    def stop(self):
        """Stops the pool without performing any pending task."""
        self._pool_manager.stop()

    def kill(self):
        """Kills the pool forcing all workers to terminate immediately."""
        self._pool_manager.kill()

    def join(self, timeout=0):
        """Joins the pool waiting until all workers exited.

        If *timeout* is greater than 0,
        it block until all workers exited or raise TimeoutError.

        """
        self._pool_manager.join(timeout)

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
        if self._pool_manager.state == CREATED:
            self._pool_manager.start()
        elif self._pool_manager.state != RUNNING:
            raise RuntimeError('The Pool is not running')

        if not isinstance(function, Callable):
            raise ValueError('function must be callable')
        if not isinstance(timeout, int):
            raise ValueError('timeout must be integer')

        task = Task(next(self._counter), function, args, kwargs,
                    callback, timeout, identifier)
        self._pool_manager.queue.put(task)

        return task
