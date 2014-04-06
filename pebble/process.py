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

from time import time, sleep
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
TERMINATE = 4


STOPPED = 0
RUNNING = 1
CLOSING = 2
CREATED = 3


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
        except Exception:  # worker closed
            self.queue.pop()
            task._timestamp = 0
            raise RuntimeError('Worker closed')

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
    """Maintains the workers within the Pool."""
    def __init__(self, context, rejected, connection,
                 event_queue, initializer, initargs):
        Thread.__init__(self)
        self.daemon = True
        self.context = context
        self.rejected = rejected
        self.initargs = initargs
        self.event_queue = event_queue
        self.initializer = initializer
        self.connection = connection

    def clean_workers(self):
        """Join expired workers and collect pending tasks."""
        for worker in [w for w in self.context.pool[:] if w.expired]:
            worker.join()
            self.rejected.extend(worker.queue)
            self.context.pool.remove(worker)

    def respawn_workers(self):
        """Respawn missing workers."""
        for _ in range(self.context.workers - len(self.context.pool)):
            worker = ProcessWorker(self.connection.address,
                                   self.context.limit,
                                   self.initializer, self.initargs)
            worker.start()
            worker.finalize(self.connection.accept())
            self.context.pool.append(worker)

    def run(self):
        while self.context.state != STOPPED:
            self.respawn_workers()
            self.context.expired_workers.wait(0.8)
            self.context.expired_workers.clear()
            self.clean_workers()


class TaskManager(Thread):
    """Sends tasks to the workers."""
    def __init__(self, context, rejected, event_queue):
        Thread.__init__(self)
        self.daemon = True
        self.context = context
        self.rejected = rejected
        self.event_queue = event_queue
        self.event_handlers = None

    def pool_event(self, timeout):
        event = None,
        worker = None

        event, worker = self.task_events()
        if event is None:
            event, worker = self.channel_events(timeout)

        return event, worker

    def task_events(self):
        """Check if current tasks have been cancelled or timing out."""
        timestamp = time()

        for worker in self.context.pool:
            current = worker.current
            if current is not None and current.started:
                if (current.timeout > 0 and
                    timestamp - current._timestamp > current.timeout):
                    return TIMEOUT, worker
                if current.cancelled:
                    return CANCELLED, worker

        return None, None

    def channel_events(self, timeout):
        """Wait for available workers."""
        pool = self.context.pool
        recv_channels = [w.channel for w in pool if not w.expired]
        send_channels = []

        # poll on send channels only if there are queued tasks
        if len(self.rejected) > 0 or not self.context.queue.empty():
            send_channels = [w.channel for w in pool if not w.closed]

        # poll for ready channels
        timeout = len(recv_channels + send_channels) > 0 and timeout or 0.01
        try:
            recv, send, _ = select(recv_channels, send_channels, [], timeout)
        except:  # select on expired worker
            return

        # return event
        for ready in recv:
            for worker in pool:
                if worker.channel == ready:
                    return RECV, worker
        for ready in send:
            for worker in pool:
                if worker.channel == ready:
                    return SEND, worker

        return None, None

    @coroutine
    def schedule_task(self):
        while 1:
            worker = (yield)

            try:
                try:  # first get any rejected task
                    task = self.rejected.popleft()
                except IndexError:  # then get enqueued ones
                    task = self.context.queue.get_nowait()
                try:
                    worker.schedule_task(task)
                except RuntimeError:  # worker closed
                    self.rejected.append(task)
            except Empty:  # no tasks available
                continue

    @coroutine
    def task_done(self):
        while 1:
            worker = (yield)

            try:
                task, results = worker.task_complete()
                task._set(results)
                self.task_callback.send(task)
                self.context.queue.task_done()
            except RuntimeError:  # worker expired
                self.context.expired_workers.set()

    @coroutine
    def task_timeout(self):
        while 1:
            worker = (yield)

            worker.stop()
            task = worker.get_current()
            task._set(TimeoutError('Task timeout'))
            self.task_callback.send(task)

    @coroutine
    def task_cancelled(self):
        while 1:
            worker = (yield)

            worker.stop()
            task = worker.get_current()
            task._set(TaskCancelled('Task cancelled'))
            self.task_callback.send(task)

    @coroutine
    def task_callback(self):
        while 1:
            task = (yield)

            if task._callback is not None:
                try:
                    task._callback(task)
                except Exception:
                    print_exc()
                    # self.context.state = ERROR

    def run(self):
        self.event_handlers = (self.schedule_task(), self.task_done(),
                               self.task_timeout(), self.task_cancelled())
        self.task_callback = self.task_callback()

        while self.context.state != STOPPED:
            event, worker = self.pool_event(0.8)
            if event is not None:
                self.event_handlers[event].send(worker)

        for handler in self.event_handlers:
            handler.close()
        self.task_callback.close()


class ProcessPool(object):
    def __init__(self, workers=1, task_limit=0, queue=None, queueargs=None,
                 initializer=None, initargs=None):
        self._context = PoolContext(CREATED, workers, task_limit,
                                    queue, queueargs)
        self._connection = Listener(family=FAMILY)
        self._rejected = deque()  # task enqueued on dead worker
        self._event_queue = Queue()  # events PoolManager and TaskManager
        self._pool_manager = PoolManager(self._context, self._rejected,
                                         self._connection, self._event_queue,
                                         initializer, initargs)
        self._task_manager = TaskManager(self._context, self._rejected,
                                         self._event_queue)
        self.initializer = initializer
        self.initargs = initargs

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
        self.join()

    def _start(self):
        """Starts the pool."""
        self._pool_manager.start()
        self._task_manager.start()
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
        if (self._pool_manager.is_alive() or self._task_manager.is_alive()):
            self._pool_manager.join()
            self._task_manager.join()
        for w in self._context.pool:
            w.channel.close()
            w.terminate()

    def kill(self):
        """Kills the pool forcing all workers to terminate immediately."""
        self._context.state = STOPPED
        if (self._pool_manager.is_alive() or self._task_manager.is_alive()):
            self._pool_manager.join()
            self._task_manager.join()
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
