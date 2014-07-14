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


from itertools import count
from threading import Event
from time import time
from traceback import format_exc

from .spawn import spawn
from ..pebble import STOPPED, RUNNING, ERROR
from ..pebble import BasePool, PoolContext, Task


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

        expired = [w for w in pool.values() if not w.is_alive()]

        for worker in expired:
            worker.join()
            del pool[worker.ident]

        for _ in range(workers - len(pool)):
            worker = pool_worker(context)
            pool[worker.ident] = worker


class Context(PoolContext):
    """Pool's Context."""
    def __init__(self, queue, queueargs, initializer, initargs,
                 workers, limit):
        super(Context, self).__init__(queue, queueargs,
                                      initializer, initargs,
                                      workers, limit)
        self.worker_event = Event()

    def stop(self):
        for _ in range(self.worker_number):
            self.queue.put(None)


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
                 initializer=None, initargs=()):
        super(Pool, self).__init__()
        self._context = Context(queue, queueargs, initializer, initargs,
                                workers, task_limit)

    def _start(self):
        """Start the Pool managers."""
        self._managers = [worker_manager(self._context)]
        self._context.state = RUNNING

    def stop(self):
        """Stops the pool without performing any pending task."""
        self._context.state = STOPPED

        self._context.worker_event.set()

        for manager in self._managers:
            manager.join()

        self._context.stop()

    def schedule(self, function, args=(), kwargs={}, identifier=None,
                 callback=None):
        """Schedules *function* into the Pool, passing *args* and *kwargs*
        respectively as arguments and keyword arguments.

        If *callback* is a callable it will be executed once the function
        execution has completed with the returned *Task* as a parameter.

        The *identifier* value will be forwarded to the *Task.id* attribute.

        A *Task* object is returned.

        """
        task = Task(next(self._counter), function, args, kwargs,
                    callback, 0, identifier)

        self._schedule(task)

        return task
