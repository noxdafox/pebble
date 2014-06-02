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
from signal import SIG_IGN, SIGINT, signal
from traceback import format_exc, print_exc
try:  # Python 2
    from Queue import Queue, Empty
    from cPickle import PicklingError
except:  # Python 3
    from queue import Queue, Empty
    from pickle import PicklingError

from .generic import channels, lock, stop_worker
from .spawn import spawn as spawn_process
from ..thread import spawn as spawn_thread
from ..pebble import Task, TimeoutError, TaskCancelled
from ..pebble import STOPPED, RUNNING, CLOSED, CREATED, ERROR


ACK = 0  # task aknowledged by worker
RES = 1  # task results from worker


def get_task(channel, pid):
    """Gets new task for Pool Worker.

    Waits for next task to be ready, atomically unpacks its contents
    and sends back acknowledgment to the Pool.

    """
    function = None

    while function is None:
        try:
            channel.poll()
            with lock(channel):
                number, function, args, kwargs = channel.recv(0)
                channel.send((ACK, number, pid))
        except TimeoutError:  # race condition between workers
            continue
        except (OSError, IOError):
            sys.exit(0)

    return number, function, args, kwargs


def task_state(task, pool, timestamp):
    """Inspects the task state.

    Checks for tasks timing out, cancelled or fetched
    by an accidentally stopped worker.

    """
    error = False
    results = None

    if (task.timeout > 0 and timestamp - task._timestamp > task.timeout):
        error = True
        results = TimeoutError('Task timeout')
    elif task.cancelled:
        error = True
        results = TaskCancelled('Task cancelled')
    else:
        try:
            if not pool[task._pid].is_alive():
                error = True
        except KeyError:
            error = True

    return error, results


@spawn_process(name='pool_worker', daemon=True)
def pool_worker(channel, initializer, initargs, limit):
    """Runs the actual function in separate process."""
    error = None
    value = None
    counter = count()
    pid = os.getpid()
    signal(SIGINT, SIG_IGN)

    if initializer is not None:
        try:
            initializer(*initargs)
        except Exception as err:
            error = err
            error.traceback = format_exc()

    while limit == 0 or next(counter) < limit:
        number, function, args, kwargs = get_task(channel, pid)

        try:
            value = function(*args, **kwargs)
        except Exception as err:
            if error is None:
                error = err
                error.traceback = format_exc()

        try:
            channel.send((RES, number, error is not None and error or value))
        except PicklingError as error:
            channel.send((RES, number, error))

        error = None
        value = None

    sys.exit(0)


@spawn_thread(name='task_scheduler', daemon=True)
def task_scheduler(context):
    """Schedules enqueued tasks to the workers."""
    queue = context.queue
    tasks = context.tasks
    put = lambda o: context.pool_channel.send(o)

    while context.state not in (ERROR, STOPPED):
        try:
            task = queue.get(0.6)
        except Empty:
            continue

        if task is not None:
            number = task.number
            tasks[number] = task

            put((number, task._function, task._args, task._kwargs))
        else:  # stop sentinel
            return


@spawn_thread(name='task_manager', daemon=True)
def task_manager(context):
    """Manages running tasks.

    If a task's timeout expired it stops its worker and set it.
    If a task is cancelled it stops its worker and set it.
    If an expired worker has accidentally fetched a task it re-enqueues
    it in the Pool's queue.

    """
    pool = context.pool
    queue = context.queue
    tasks = context.tasks

    while context.state not in (ERROR, STOPPED):
        timestamp = time()
        running = [t for t in list(tasks.values())
                   if t.started and not t.ready]

        for task in running:
            error, results = task_state(task, pool, timestamp)

            if error:
                try:
                    del tasks[task.number]

                    if results is not None:  # timeout or cancelled
                        context.task_done(task, results)

                        with lock(context.worker_channel):
                            stop_worker(pool[task._pid])
                    else:  # race condition between pool and worker
                        task._timestamp = task._pid = 0
                        queue.put(task)
                        queue.task_done()
                except KeyError:  # task completed or worker already expired
                    continue

        sleep(0.2)


@spawn_thread(name='message_manager', daemon=True)
def message_manager(context):
    """Gather messages from workers,
    sets tasks status and runs callbacks.

    """
    tasks = context.tasks
    get = lambda t: context.pool_channel.recv(timeout=t)

    while context.state not in (ERROR, STOPPED):
        try:
            message, number, data = get(0.6)

            if message is ACK:
                task = tasks[number]
                task._timestamp = time()
                task._pid = data
            elif message is RES:
                task = tasks.pop(number)
                context.task_done(task, data)
        except (TimeoutError, KeyError):
            continue  # nothing on channel or task cancelled
        except TypeError:  # stop sentinel
            return


@spawn_thread(name='worker_manager', daemon=True)
def worker_manager(context):
    """Collects expired workers and spawns new ones."""
    pool = context.pool
    limit = context.worker_limit
    initializer = context.initializer
    initargs = context.initargs
    workers = context.worker_number
    channel = context.worker_channel

    while context.state not in (ERROR, STOPPED):
        expired = [w for w in pool.values() if not w.is_alive()]

        for worker in expired:
            worker.join()
            del pool[worker.pid]

        for _ in range(workers - len(pool)):
            worker = pool_worker(channel, initializer, initargs, limit)
            pool[worker.pid] = worker

        sleep(0.2)


class PoolTask(Task):
    """Extends the *Task* object to support *process* decorator."""
    def _cancel(self):
        """Overrides the *Task* cancel method."""
        self._cancelled = True


class PoolContext(object):
    """Pool's Context.

    Wraps the Pool's state.

    """
    def __init__(self, queue, queueargs, initializer, initargs,
                 workers, limit):
        self.state = CREATED
        self.pool = {}  # {pid: process}
        self.tasks = {}  # {task_number: task_obj}
        self.managers = None
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
        self.pool_channel, self.worker_channel = channels()

    def stop(self):
        """Stop the workers."""
        with lock(self.worker_channel):
            for worker in self.pool.values():
                worker.terminate()

    def kill(self):
        """Forces all workers to stop."""
        self.stop()

        with lock(self.worker_channel):
            for worker in self.pool.values():
                stop_worker(worker)

    def join(self, timeout):
        """Joins pool's workers."""
        while len(self.pool) > 0 and (timeout is None or timeout > 0):
            for identifier, worker in list(self.pool.items()):
                worker.join(timeout is not None and 0.1 or None)
                if not worker.is_alive():
                    self.pool.pop(identifier)

            if timeout is not None:
                timeout = timeout - (len(self.pool) / 10.0)

        if len(self.pool) > 0:
            raise TimeoutError('Workers are still running')

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
                 initializer=None, initargs=()):
        self._counter = count()
        self._context = PoolContext(queue, queueargs,
                                    initializer, initargs,
                                    workers, task_limit)
        self._managers = ()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
        self.join()

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
        """Closes the pool.

        No new tasks will be accepted, enqueued ones will be performed.

        """
        self._context.state = CLOSED

    def stop(self):
        """Stops the pool without performing any pending task."""
        self._context.state = STOPPED

        self._context.queue.put(None)  # task_scheduler stop sentinel
        self._context.worker_channel.send(None)  # task_manager stop sentinel

        for manager in self._managers:
            manager.join()

        self._context.stop()

    def kill(self):
        """Kills the pool forcing all workers to terminate immediately."""
        self._context.state = STOPPED

        self._context.kill()

    def join(self, timeout=None):
        """Joins the pool waiting until all workers exited.

        If *timeout* is set, it block until all workers are done
        or raise TimeoutError.

        """
        if self._context.state == RUNNING:
            raise RuntimeError('The Pool is still running')
        elif self._context.state == CLOSED:
            queue = self._context.queue

            if timeout is not None:
                while queue.unfinished_tasks > 0 and timeout > 0:
                    sleep(0.1)
                    timeout -= 0.1
            else:
                queue.join()
            self.stop()

        self._context.join(timeout)

    def schedule(self, function, args=(), kwargs={}, identifier=None,
                 callback=None, timeout=0):
        """Schedules *function* into the Pool, passing *args* and *kwargs*
        respectively as arguments and keyword arguments.

        If *callback* is a callable it will be executed once the function
        execution has completed with the returned *Task* as a parameter.

        *timeout* is an integer, if expires the task will be terminated
        and *Task.get()* will raise *TimeoutError*.

        The *identifier* value will be forwarded to the *Task.id* attribute.

        A *Task* object is returned.

        """
        if self._context.state == CREATED:
            self._start()
        elif self._context.state != RUNNING:
            raise RuntimeError('The Pool is not running')

        task = PoolTask(next(self._counter), function, args, kwargs,
                        callback, timeout, identifier)
        self._context.queue.put(task)

        return task
