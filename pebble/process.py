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


from uuid import uuid4
from inspect import isclass
from traceback import format_exc
from itertools import count
from threading import Thread, Event
from collections import Callable
from functools import update_wrapper
from multiprocessing import Process
from multiprocessing.queues import SimpleQueue
try:  # Python 2
    from Queue import Queue, Empty, Full
except:  # Python 3
    from queue import Queue, Empty, Full
try:  # Python 2
    from cPickle import PicklingError
except:  # Python 3
    from pickle import PicklingError

from .pebble import PebbleError, TimeoutError, TaskCancelled


def worker(function, tqueue, rqueue, limit, initializer, initargs):
    error = None
    counter = count()

    if isinstance(initializer, Callable):
        try:
            initializer(*initargs)
        except Exception as err:
            error = err
            error.traceback = format_exc()

    while limit == 0 or next(counter) < limit:
        try:
            args, kwargs = tqueue.get()
            results = function(*args, **kwargs)
        except (IOError, OSError):  # pipe was closed
            return
        except Exception as err:
            error = err
            error.traceback = format_exc()
        finally:
            try:
                rqueue.put(error is not None and error or results)
            except PicklingError as err:
                rqueue.put(err)
            error = None
            results = None


def process(*args, **kwargs):
    """Turns a *function* into a Process and runs its logic within.

    A decorated *function* will return a *Task* object once is called.

    If *callback* is a callable, it will be called once the task has ended
    with the task identifier and the *function* return values.

    """
    def wrapper(function):
        return Wrapper(function, timeout, callback)

    if len(args) == 1 and not len(kwargs) and isinstance(args[0], Callable):
        return Wrapper(args[0], None, None)
    elif not len(args) and len(kwargs):
        timeout = kwargs.get('timeout')
        callback = kwargs.get('callback')

        return wrapper
    else:
        raise ValueError("Decorator accepts only keyword arguments.")


def process_pool(*args, **kwargs):
    """Turns a *function* into a Thread and runs its logic within.

    A decorated *function* will return a *Task* object once is called.

    If *callback* is a callable, it will be called once the task has ended
    with the task identifier and the *function* return values.

    """
    def wrapper(function):
        return PoolWrapper(function, workers, task_limit, queue, queue_args,
                           callback, initializer, initargs)

    if len(args) == 1 and not len(kwargs) and isinstance(args[0], Callable):
        return PoolWrapper(args[0], 1, 0, None, None, None, None, None)
    elif not len(args) and len(kwargs):
        queue = kwargs.get('queue')
        queue_args = kwargs.get('queueargs')
        workers = kwargs.get('workers', 1)
        callback = kwargs.get('callback')
        initargs = kwargs.get('initargs')
        initializer = kwargs.get('initializer')
        task_limit = kwargs.get('worker_task_limit', 0)

        return wrapper
    else:
        raise ValueError("Decorator accepts only keyword arguments.")


class Task(object):
    """Handler to the ongoing task."""
    def __init__(self, task_nr, worker, channel, callback, timeout):
        self.id = uuid4()
        self.number = task_nr
        self._ready = False
        self._cancelled = False
        self._results = None
        self._event = Event()
        self._channel = channel
        self._worker = worker
        self._timeout = timeout
        self._callback = callback
        self._worker_listener = Thread(target=self._set)
        self._worker_listener.daemon = True
        self._worker_listener.start()

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return "%s (Task-%d, %s)" % (self.__class__, self.number, self.id)

    @property
    def ready(self):
        return self._ready

    @property
    def cancelled(self):
        return self._cancelled

    def get(self, timeout=None):
        """Retrieves the produced results.

        If the executed code raised an error it will be re-raised.

        """
        self._event.wait(timeout)
        if self._ready:
            if (isinstance(self._results, BaseException)):
                raise self._results
            else:
                return self._results
        else:
            raise TimeoutError("Task is still running")

    def cancel(self):
        """Cancels the Task terminating the running process
        and dropping the results."""
        self._cancelled = True
        self._worker.terminate()

    def _set(self):
        try:
            if self._channel._reader.poll(self._timeout):
                self._results = self._channel.get()
            elif self._worker.is_alive():
                self._worker.terminate()
                self._results = TimeoutError("Task timeout expired")
        except (IOError, OSError, EOFError) as error:  # pipe was closed
            if not self._cancelled:
                self._results = error
            else:
                self._results = TaskCancelled("Task cancelled")
        finally:
            if not self._worker.is_alive():  # join the process if exited
                self._worker.join()
            self._ready = True
            self._event.set()
            if self._callback is not None and not self._cancelled:
                self._callback(self)


class Wrapper(object):
    def __init__(self, function, timeout, callback):
        self._function = function
        self._counter = count()
        self.timeout = timeout
        self.callback = callback
        update_wrapper(self, function)

    def __call__(self, *args, **kwargs):
        tqueue = SimpleQueue()
        rqueue = SimpleQueue()
        p = Process(target=worker, args=(self._function, tqueue, rqueue,
                                         1, None, None))
        p.daemon = True
        p.start()
        tqueue.put((args, kwargs))
        rqueue._writer.close()
        return Task(next(self._counter), p, rqueue,
                    self.callback, self.timeout)


class PoolWrapper(object):
    def __init__(self, function, workers, task_limit, queue, queueargs,
                 callback, initializer, initargs):
        self._alive = True
        self._function = function
        self._counter = count()
        self._workers = workers
        self._limit = task_limit
        self._pool = []
        self._task_channel = None
        self._results_channel = None
        self.initializer = initializer
        self.initargs = initargs
        self.callback = callback
        if queue is not None:
            if isclass(queue):
                self._queue = queue(*queueargs)
            else:
                raise ValueError("Queue must be Class")
        else:
            self._queue = Queue(workers)
        update_wrapper(self, function)

    @property
    def queue(self):
        return self._queue

    @queue.setter
    def queue(self, queue):
        while 1:
            try:
                queue.put(self._queue.get(False), False)
            except Empty:
                self._queue = queue
                return
            except Full:
                raise ValueError("Provided queue too small")

    def _schedule_tasks(self):
        """Fetch tasks from the queue and submits to the pipe."""
        while self._alive:
            t = self._queue.get()
            try:
                self._task_channel.put(t)
            except (IOError, OSError):
                self._alive = False
                raise PebbleError("Connection with workers was lost")

    def _restart_workers(self):
        """Spawn one worker if pool is not full."""
        # join terminated processes
        exited = [w for w in self._pool if not w.is_alive()]
        for w in exited:
            w.join()
        self._pool = list(set(self._pool) - set(exited))
        if self._workers - len(self._pool):
            p = Process(target=worker,
                        args=(self._function,
                              self._task_channel,
                              self._results_channel,
                              self._limit,
                              self.initializer,
                              self.initargs))
            p.daemon = True
            p.start()
            self._pool.append(p)

    def __call__(self, *args, **kwargs):
        if self._task_channel is None or self._results_channel is None:
            self._task_channel = SimpleQueue()
            self._results_channel = SimpleQueue()
        task = Task(next(self._counter), None, self._results_channel,
                    self.callback, self.timeout)
        # loop maintaining the workers alive in case of full queue
        while self._alive:
            self._restart_workers()
            try:
                self._queue.put((task, args, kwargs), timeout=0.1)
                break
            except Full:
                continue

        return task
