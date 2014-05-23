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
    from Queue import Queue, Empty
    from cPickle import PicklingError
except:  # Python 3
    from queue import Queue, Empty
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


def join_workers(pool, timeout=None):
    """Join terminated workers."""
    counter = 0

    while len(pool) > 0 and (timeout is None or counter < timeout):
        for identifier, worker in pool.items():
            worker.join(timeout is not None and 0.1 or None)
            if not worker.is_alive():
                pool.pop(identifier)
        counter += timeout is not None and len(pool) / 10.0 or 0

    return pool


def get_task(task_queue, result_queue, pid):
    """Gets new task for Pool Worker.

    Waits for next task to be ready, unpacks its contents
    and sends back an acknowledgment to the Pool.

    """
    function = None

    while function is None:
        try:
            task_queue.wait()
            number, function, args, kwargs = task_queue.get(timeout=0)
            result_queue.put((ACK, number, pid))
        except Empty:
            continue
        except (IOError, OSError, EOFError):
            sys.exit(1)

    return number, function, args, kwargs


def put_results(queue, number, results):
    """Sends results back to Pool."""
    try:
        queue.put((RES, number, results))
    except PicklingError as error:
        queue.put((RES, number, error))
    except (IOError, OSError, EOFError):
        sys.exit(1)


@process_worker(name='pool_worker', daemon=True)
def pool_worker(queues, initializer, initargs, limit):
    """Runs the actual function in separate process."""
    error = None
    value = None
    counter = count()
    pid = os.getpid()
    signal(SIGINT, SIG_IGN)
    tasks, results = queues
    tasks._writer.close()
    results._reader.close()

    if initializer is not None:
        try:
            initializer(*initargs)
        except Exception as err:
            error = err
            error.traceback = format_exc()

    while limit == 0 or next(counter) < limit:
        number, function, args, kwargs = get_task(tasks, results, pid)

        try:
            value = function(*args, **kwargs)
        except Exception as err:
            if error is None:
                error = err
                error.traceback = format_exc()

        put_results(results, number, error is not None and error or value)

        error = None
        value = None

    sys.exit(0)


@thread_worker(name='task_scheduler', daemon=True)
def task_scheduler(pool_context):
    """Schedules tasks in queue to the workers."""
    queue = pool_context.queue
    tasks = pool_context.tasks
    put = lambda o: pool_context.task_queue.put(o)

    while pool_context.state not in (ERROR, STOPPED):
        task = queue.get()

        if task is not None:
            number = task.number
            tasks[number] = task

            put((number, task._function, task._args, task._kwargs))
        else:  # stop sentinel
            return


@thread_worker(name='task_manager', daemon=True)
def task_manager(pool_context):
    """Checks for tasks cancelled or timing out."""
    pool = pool_context.pool
    tasks = pool_context.tasks
    timeout = lambda task, tick: tick - task._timestamp > task.timeout

    while pool_context.state not in (ERROR, STOPPED):
        timestamp = time()
        running = [t for t in tasks.values() if t.started and not t.ready]

        for task in running:
            error = None

            if (task.timeout > 0 and timeout(task, timestamp)):
                error = TimeoutError('Task timeout')
            elif task.cancelled:
                error = TaskCancelled('Task cancelled')

            if error is not None:
                try:
                    del tasks[task.number]
                    pool_context.task_done(task, error)

                    with suspend(pool_context.task_queue):
                        with suspend(pool_context.result_queue):
                            stop_worker(pool[task._pid])
                except KeyError:  # task completed or worker already expired
                    continue

        sleep(0.2)


@thread_worker(name='message_manager', daemon=True)
def message_manager(pool_context):
    """Gather messages from workers,
    sets tasks status and runs callbacks.

    """
    tasks = pool_context.tasks
    get = lambda: pool_context.result_queue.get()

    while pool_context.state not in (ERROR, STOPPED):
        try:
            message, number, data = get()

            if message is ACK:
                task = tasks[number]
                task._timestamp = time()
                task._pid = data
            elif message is RES:
                task = tasks.pop(number)
                pool_context.task_done(task, data)
        except KeyError:  # task was cancelled
            continue
        except TypeError:  # stop sentinel
            return


@thread_worker(name='worker_manager', daemon=True)
def worker_manager(pool_context):
    """Collects expired workers and spawns new ones."""
    pool = pool_context.pool
    limit = pool_context.worker_limit
    initializer = pool_context.initializer
    initargs = pool_context.initargs
    workers = pool_context.worker_number
    queues = (pool_context.task_queue, pool_context.result_queue)

    while pool_context.state not in (ERROR, STOPPED):
        expired = [w for w in pool.values() if not w.is_alive()]

        for worker in expired:
            worker.join()
            del pool[worker.pid]

        for _ in range(workers - len(pool)):
            worker = pool_worker(queues, initializer, initargs, limit)
            pool[worker.pid] = worker

        sleep(0.2)


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


class PoolContext(object):
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

    def stop(self):
        """Stop the pool."""
        with suspend(self.task_queue):
            with suspend(self.result_queue):
                for worker in self.pool.itervalues():
                    worker.terminate()

    def kill(self):
        """Forces all workers to stop."""
        self.stop()

        with suspend(self.task_queue):
            with suspend(self.result_queue):
                for worker in self.pool.itervalues():
                    stop_worker(worker)

    def join(self, timeout):
        if self.state == RUNNING:
            raise RuntimeError('The Pool is still running')

        if timeout > 0:
            self.pool = join_workers(self.pool, timeout)
            if len(self.pool) > 0:
                raise TimeoutError('Workers are still running')
        else:
            self.pool = join_workers(self.pool)

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
        self._context = PoolContext(queue, queueargs,
                                    initializer, initargs,
                                    workers, task_limit)
        self._managers = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
        self.join()

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

    def _start(self):
        """Start the Pool managers."""
        self._managers = (task_scheduler(self._context),
                          task_manager(self._context),
                          message_manager(self._context),
                          worker_manager(self._context))
        self._context.state = RUNNING

    def close(self):
        """Closes the pool waiting for all tasks to be completed."""
        self._context.state = CLOSED
        self._context.queue.join()
        self.stop()

    def stop(self):
        """Stops the pool without performing any pending task."""
        self._context.state = STOPPED

        # send stop sentinels
        self._context.queue.put(None)
        self._context.result_queue.put(None)

        for manager in self._managers:
            manager.join()

        self._context.stop()

    def kill(self):
        """Kills the pool forcing all workers to terminate immediately."""
        self._context.kill()

    def join(self, timeout=0):
        """Joins the pool waiting until all workers exited.

        If *timeout* is greater than 0,
        it block until all workers exited or raise TimeoutError.

        """
        self._context.join(timeout)

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
        if self._context.state == CREATED:
            self._start()
        elif self._context.state != RUNNING:
            raise RuntimeError('The Pool is not running')

        if not isinstance(function, Callable):
            raise ValueError('function must be callable')
        if not isinstance(timeout, int):
            raise ValueError('timeout must be integer')

        task = Task(next(self._counter), function, args, kwargs,
                    callback, timeout, identifier)
        self._context.queue.put(task)

        return task
