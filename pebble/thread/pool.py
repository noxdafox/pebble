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


from inspect import isclass
from itertools import count
from threading import Event
from time import sleep, time
from traceback import format_exc, print_exc
try:  # Python 2
    from Queue import Queue
except:  # Python 3
    from queue import Queue

from .spawn import spawn
from ..pebble import Task, TimeoutError
from ..pebble import STOPPED, RUNNING, CLOSED, CREATED, ERROR


@spawn(name='pool_worker', daemon=True)
def pool_worker(context):
    """Runs the actual function in separate process."""
    error = None
    value = None
    counter = count()
    queue = context.queue
    limit = context.worker_limit
    task_done = context.task_done
    running = lambda c: c.state not in (ERROR, STOPPED)

    if context.initializer is not None:
        try:
            context.initializer(*context.initargs)
        except Exception as err:
            error = err
            error.traceback = format_exc()

    while running(context) and (limit == 0 or next(counter) < limit):
        task = queue.get()

        if task is None:  # stop sentinel
            queue.task_done()
            return

        try:
            if not task._cancelled:
                task._timestamp = time()
                value = task._function(*task._args, **task._kwargs)
        except Exception as err:
            if error is None:  # do not overwrite initializer errors
                error = err
                error.traceback = format_exc()

        task_done(task, error is not None and error or value)
        error = None
        value = None

    context.worker_event.set()


@spawn(name='worker_manager', daemon=True)
def worker_manager(context):
    """Collects expired workers and spawns new ones."""
    pool = context.pool
    event = context.worker_event
    workers = context.worker_number
    event.set()

    while context.state not in (ERROR, STOPPED):
        event.wait(0.6)
        event.clear()

        expired = [w for w in pool if not w.is_alive()]

        for worker in expired:
            worker.join()
            pool.remove(worker)

        for _ in range(workers - len(pool)):
            worker = pool_worker(context)
            pool.append(worker)


class PoolContext(object):
    """Pool's Context.

    Wraps the Pool's state.

    """
    def __init__(self, queue, queueargs, initializer, initargs,
                 workers, limit):
        self.state = CREATED
        self.pool = []
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
        self.worker_event = Event()

    def stop(self):
        for _ in range(self.worker_number):
            self.queue.put(None)

    def join(self, timeout):
        """Joins pool's workers."""
        while len(self.pool) > 0 and (timeout is None or timeout > 0):
            for worker in self.pool[:]:
                worker.join(timeout is not None and 0.1 or None)
                if not worker.is_alive():
                    self.pool.remove(worker)

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
        self._worker_manager = None

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
        self._worker_manager = worker_manager(self._context)
        self._context.state = RUNNING

    def close(self):
        """Closes the pool.

        No new tasks will be accepted, enqueued ones will be performed.

        """
        self._context.state = CLOSED

    def stop(self):
        """Stops the pool without performing any pending task."""
        self._context.state = STOPPED
        self._context.worker_event.set()
        if self._worker_manager is not None:
            self._worker_manager.join()
        self._context.stop()

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
                 callback=None):
        """Schedules *function* into the Pool, passing *args* and *kwargs*
        respectively as arguments and keyword arguments.

        If *callback* is a callable it will be executed once the function
        execution has completed with the returned *Task* as a parameter.

        The *identifier* value will be forwarded to the *Task.id* attribute.

        A *Task* object is returned.

        """
        if self._context.state == CREATED:
            self._start()
        elif self._context.state != RUNNING:
            raise RuntimeError('The Pool is not running')

        task = Task(next(self._counter), function, args, kwargs,
                    callback, 0, identifier)
        self._context.queue.put(task)

        return task
