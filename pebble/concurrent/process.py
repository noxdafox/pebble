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
import signal

from threading import Thread
from traceback import format_exc
from pickle import PicklingError
from functools import update_wrapper
from concurrent.futures import Future
from multiprocessing import Pipe, Process


class ProcessExpired(Exception):
    """Raised when process dies unexpectedly."""
    def __init__(self, msg, code=0):
        super(ProcessExpired, self).__init__(msg)
        self.exitcode = code


class process:
    """Runs the decorated function in a concurrent process,
    taking care of the result and error management.

    Decorated functions will return a concurrent.futures.Future object
    once called.

    Decorated functions will have a *timeout* attribute. If set,
    the process will be stopped once expired returning TimeoutError as result.

    """
    def __init__(self, function):
        self.timeout = None
        self._function = function

        update_wrapper(self, function)

    def __call__(self, *args, **kwargs):
        future = Future()
        reader, writer = Pipe(duplex=False)

        worker = Process(target=function_handler,
                         args=(self._function, args, kwargs, writer))
        worker.daemon = True
        worker.start()

        future.set_running_or_notify_cancel()

        handler = Thread(target=worker_handler,
                         args=(future, worker, reader, self.timeout))
        handler.daemon = True
        handler.start()

        return future


def worker_handler(future, worker, pipe, timeout):
    """Task's lifecycle manager.

    Waits for the *Task* to be performed,
    collects result, runs the callback and cleans up the process.
    """
    result = get_result(pipe, timeout)

    if isinstance(result, BaseException):
        if isinstance(result, ProcessExpired):
            result.exitcode = worker.exitcode

        future.set_exception(result)
    else:
        future.set_result(result)

    if worker.is_alive():
        stop(worker)


def function_handler(function, args, kwargs, pipe):
    """Runs the actual function in separate process and returns its result."""
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    try:
        result = function(*args, **kwargs)
    except BaseException as error:
        error.traceback = format_exc()
        return error

    try:
        pipe.send(result)
    except PicklingError as error:
        error.traceback = format_exc()
        pipe.send(error)


def get_result(pipe, timeout):
    """Waits for result and handles communication errors."""
    try:
        if pipe.poll(timeout):
            return pipe.recv()
        else:
            return TimeoutError('Task Timeout', timeout)
    except (EOFError, OSError):
        return ProcessExpired('Abnormal termination')
    except Exception as error:
        return error


def stop(worker):
    """Does its best to stop the worker."""
    worker.terminate()
    worker.join(3)

    if worker.is_alive() and os.name != 'nt':
        try:
            os.kill(worker.pid, signal.SIGINT)
            worker.join()
        except OSError:
            return

    if worker.is_alive():
        raise RuntimeError("Unable to terminate PID %d" % os.getpid())
