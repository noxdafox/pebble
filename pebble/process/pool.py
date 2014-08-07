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
from time import time
from select import select
from itertools import count
from collections import deque
from multiprocessing import Pipe
from signal import SIG_IGN, SIGINT, signal
from traceback import format_exc, print_exc
try:  # Python 2
    from cPickle import PicklingError
except:  # Python 3
    from pickle import PicklingError

from .generic import stop_worker
from .spawn import spawn as spawn_process
from ..thread import spawn as spawn_thread
from ..pebble import BasePool, PoolContext
from ..pebble import STOPPED, RUNNING, ERROR
from ..pebble import Task, TimeoutError, TaskCancelled, ProcessExpired


ACK = 0  # task aknowledged by worker
RES = 1  # task results from worker


def problematic(worker):
    """Checks the current task state.

    Returns True if the current task is timing out
    or it has been cancelled.

    """
    try:
        task = worker.current
    except IndexError:
        return False

    if (task.timeout and time() - task._timestamp > task.timeout):
        return True
    elif task.cancelled:
        return True
    elif worker.exitcode and not worker.exitcode == -15:
        return True
    else:
        return False


def nt_reader(reader):
    """Checks if the reader is ready in Windows."""
    try:
        if reader.poll(0):
            return True
    except (IOError, OSError):  # Pipe closed means EOF
        return True

    return False


def ready_readers(workers, interval):
    """Collects workers ready to send/receive."""
    if os.name == 'nt':
        return [w.reader for w in workers if nt_reader(w.reader)]
    else:
        ready, _, _ = select([w.reader for w in workers], [], [], interval)
        return ([w for w in workers if w.reader in ready])


def ready_writers(workers, interval):
    if os.name == 'nt':
        return workers
    else:
        _, ready, _ = select([], [w.writer for w in workers], [], interval)
        return ([w for w in workers if w.writer in ready])


@spawn_thread(name='pool_manager', daemon=True)
def pool_manager(context):
    """Pool manager Thread, event loop."""
    pool = context.pool

    while context.state not in (ERROR, STOPPED):
        workers = [w for w in pool if not w.expired]
        ready = ready_readers(workers, workers and 0.2 or 0.01)

        manage_results(context, ready)
        manage_problems(context, [w for w in pool if problematic(w)])
        manage_workers(context, [w for w in pool if w.expired])


@spawn_thread(name='task_scheduler', daemon=True)
def task_manager(context):
    """Schedules Tasks to Workers."""
    pool = context.pool

    while context.state not in (ERROR, STOPPED):
        workers = [w for w in pool if not w.closed]
        ready = ready_writers(workers, not workers and 0.01 or None)

        try:
            manage_tasks(context, ready)
        except ValueError:  # termination sentinel
            return


def manage_tasks(context, workers):
    """Schedules tasks to the workers."""
    queue = context.queue

    for worker in workers:
        task = queue.get()

        if task is None:
            raise ValueError("No task in queue")

        try:
            worker.schedule(task)
        except (IOError, OSError):
            worker.writer.close()
            try:
                queue.put(worker.cancel())
                queue.task_done()
            except IndexError:
                continue


def manage_results(context, workers):
    """Fetches results from Workers."""
    tasks = []  # [(Task, results), (Task, results)]

    for worker in workers:
        try:
            tasks.append(worker.receive())
        except (EOFError, IOError, OSError):
            worker.stop()

    for task, results in tasks:
        context.task_done(task, results)


def manage_problems(context, workers):
    """Manages problematic Tasks."""
    problematics = []  # [(Worker, Task), (Worker, Task)]

    for worker in workers:
        worker.stop()
        problematics.append((worker, worker.cancel()))

    for worker, task in problematics:
        if (task.timeout and time() - task._timestamp > task.timeout):
            context.task_done(task, TimeoutError('Task timeout'))
        elif task.cancelled:
            context.task_done(task, TaskCancelled('Task cancelled'))
        elif worker.exitcode:
            context.task_done(task, ProcessExpired('Abnormal termination'))


def manage_workers(context, workers):
    """Manages expired Workers."""
    pool = context.pool
    queue = context.queue
    limit = context.worker_limit
    initializer = context.initializer
    initargs = context.initargs
    deinitializer = context.deinitializer
    deinitargs = context.deinitargs
    worker_number = context.worker_number

    for worker in workers:
        worker.join()
        pool.remove(worker)
        for task in worker.clear():
            queue.put(task)
            queue.task_done()

    for _ in range(worker_number - len(pool)):
        worker = Worker(limit, initializer, initargs,
                        deinitializer, deinitargs)
        worker.start()
        pool.append(worker)


@spawn_process(name='pool_worker', daemon=True)
def pool_worker(tasks, results, limit,
                initializer, initargs, deinitializer, deinitargs):
    """Runs the actual function in separate process."""
    error = None
    value = None
    counter = count()
    signal(SIGINT, SIG_IGN)

    if initializer is not None:
        try:
            initializer(*initargs)
        except Exception as err:
            error = err
            error.traceback = format_exc()

    while not limit or next(counter) < limit:
        try:
            function, args, kwargs = tasks.recv()
        except (EOFError, IOError, OSError):
            return

        try:
            value = function(*args, **kwargs)
        except Exception as err:
            if error is None:
                error = err
                error.traceback = format_exc()

        try:
            results.send(error is not None and error or value)
        except (EOFError, IOError, OSError):
            return
        except PicklingError as error:
            results.send(error)

        error = None
        value = None

    if deinitializer is not None:
        try:
            deinitializer(*deinitargs)
        except Exception:
            print_exc()


class Worker(object):
    """Wraps the Worker process within a Class."""
    def __init__(self, limit, initializer, initargs,
                 deinitializer, deinitargs):
        self.limit = limit
        self.counter = count()
        self.initializer = initializer
        self.initargs = initargs
        self.deinitializer = deinitializer
        self.deinitargs = deinitargs
        self.tasks = deque()
        self.process = None
        self.task_reader, self.task_writer = Pipe(duplex=False)
        self.result_reader, self.result_writer = Pipe(duplex=False)

    @property
    def current(self):
        return self.tasks[0]

    @property
    def expired(self):
        return self.task_reader.closed and self.result_reader.closed

    @property
    def closed(self):
        return self.task_writer.closed or self.result_reader.closed

    @property
    def reader(self):
        return self.result_reader

    @property
    def writer(self):
        return self.task_writer

    @property
    def exitcode(self):
        return self.process.exitcode

    def clear(self):
        """Returns enqueued tasks."""
        tasks = []

        while 1:
            try:
                tasks.append(self.tasks.pop())
            except IndexError:
                return tasks

    def is_alive(self):
        """Checks if process is alive."""
        return self.process.is_alive()

    def start(self):
        """Starts the Worker's process."""
        self.process = pool_worker(self.task_reader, self.result_writer,
                                   self.limit, self.initializer, self.initargs,
                                   self.deinitializer, self.deinitargs)
        self.task_reader.close()
        self.result_writer.close()

    def stop(self):
        """Does its best to stop the Worker."""
        stop_worker(self.process)
        self.task_writer.close()
        self.result_reader.close()

    def join(self, timeout=None):
        """Joins the Worker's process."""
        self.process.join(timeout=timeout)

    def cancel(self):
        """Cancels the current task.

        Returns the cancelled *Task* object.

        """
        return self.tasks.popleft()

    def schedule(self, task):
        """Schedules a *Task* to the *Worker*.

        Raises OSError IOError in case of communication problems.

        """
        if not self.tasks:
            task._timestamp = time()
        self.tasks.append(task)

        self.task_writer.send((task._function, task._args, task._kwargs))

        if self.limit and next(self.counter) == self.limit:
            self.task_writer.close()

    def receive(self):
        """Receives the results.

        Returns the ready *Task* object and the related results.
        Raises EOFError OSError IOError in case of communication problems.

        """
        results = self.result_reader.recv()

        task = self.tasks.popleft()
        if self.tasks:
            self.tasks[0]._timestamp = time()

        return task, results


class PoolTask(Task):
    """Extends the *Task* object to support *process* *Pool*."""
    def _cancel(self):
        """Overrides the *Task* cancel method."""
        self._cancelled = True


class Context(PoolContext):
    """Pool's Context."""
    def __init__(self, queue, queueargs, initializer, initargs,
                 deinitializer, deinitargs, workers, limit):
        super(Context, self).__init__(
            queue, queueargs, initializer, initargs,
            deinitializer, deinitargs, workers, limit)

    def stop(self):
        """Stop the workers."""
        for worker in self.pool:
            stop_worker(worker.process)


class Pool(BasePool):
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
                 initializer=None, initargs=(),
                 deinitializer=None, deinitargs=()):
        super(Pool, self).__init__()
        self._context = Context(queue, queueargs, initializer, initargs,
                                deinitializer, deinitargs,
                                workers, task_limit)

    def _start(self):
        """Starts the Pool manager."""
        self._managers = (pool_manager(self._context), task_manager(self._context))
        self._context.state = RUNNING

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
        task = PoolTask(next(self._counter), function, args, kwargs,
                        callback, timeout, identifier)

        self._schedule(task)

        return task
