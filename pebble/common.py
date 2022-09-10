# This file is part of Pebble.
# Copyright (c) 2013-2022, Matteo Cafasso

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

from __future__ import absolute_import

import os
import pickle
import signal

from threading import Thread
from traceback import format_exc

from concurrent.futures import Future


class ProcessExpired(OSError):
    """Raised when process dies unexpectedly."""

    def __init__(self, msg, code=0):
        super(ProcessExpired, self).__init__(msg)
        self.exitcode = code


class PebbleFuture(Future):
    # Same as base class, removed logline
    def set_running_or_notify_cancel(self):
        """Mark the future as running or process any cancel notifications.

        Should only be used by Executor implementations and unit tests.

        If the future has been cancelled (cancel() was called and returned
        True) then any threads waiting on the future completing (though calls
        to as_completed() or wait()) are notified and False is returned.

        If the future was not cancelled then it is put in the running state
        (future calls to running() will return True) and True is returned.

        This method should be called by Executor implementations before
        executing the work associated with this future. If this method returns
        False then the work should not be executed.

        Returns:
            False if the Future was cancelled, True otherwise.

        Raises:
            RuntimeError: if set_result() or set_exception() was called.
        """
        with self._condition:
            if self._state == CANCELLED:
                self._state = CANCELLED_AND_NOTIFIED
                for waiter in self._waiters:
                    waiter.add_cancelled(self)

                return False
            elif self._state == PENDING:
                self._state = RUNNING

                return True
            else:
                raise RuntimeError('Future in unexpected state')


class ProcessFuture(PebbleFuture):
    def cancel(self):
        """Cancel the future.

        Returns True if the future was cancelled, False otherwise. A future
        cannot be cancelled if it has already completed.
        """
        with self._condition:
            if self._state == FINISHED:
                return False

            if self._state in (CANCELLED, CANCELLED_AND_NOTIFIED):
                return True

            self._state = CANCELLED
            self._condition.notify_all()

        self._invoke_callbacks()

        return True


class RemoteTraceback(Exception):
    """Traceback wrapper for exceptions in remote process.

    Exception.__cause__ requires a BaseException subclass.

    """

    def __init__(self, traceback):
        self.traceback = traceback

    def __str__(self):
        return self.traceback


class RemoteException(object):
    """Pickling wrapper for exceptions in remote process."""

    def __init__(self, exception, traceback):
        self.exception = exception
        self.traceback = traceback

    def __reduce__(self):
        return rebuild_exception, (self.exception, self.traceback)


def rebuild_exception(exception, traceback):
    exception.__cause__ = RemoteTraceback(traceback)

    return exception


def launch_thread(name, function, daemon, *args, **kwargs):
    thread = Thread(target=function, name=name, args=args, kwargs=kwargs)
    thread.daemon = daemon
    thread.start()

    return thread


def launch_process(name, function, daemon, mp_context, *args, **kwargs):
    process = mp_context.Process(
        target=function, name=name, args=args, kwargs=kwargs)
    process.daemon = daemon
    process.start()

    return process


def stop_process(process):
    """Does its best to stop the process."""
    process.terminate()
    process.join(3)

    if process.is_alive() and os.name != 'nt':
        try:
            os.kill(process.pid, signal.SIGKILL)
            process.join()
        except OSError:
            return

    if process.is_alive():
        raise RuntimeError("Unable to terminate PID %d" % os.getpid())


def execute(function, *args, **kwargs):
    """Runs the given function returning its results or exception."""
    try:
        return function(*args, **kwargs)
    except Exception as error:
        error.traceback = format_exc()
        return error


def process_execute(function, *args, **kwargs):
    """Runs the given function returning its results or exception."""
    try:
        return function(*args, **kwargs)
    except Exception as error:
        error.traceback = format_exc()
        return RemoteException(error, error.traceback)


def send_result(pipe, data):
    """Send result handling pickling and communication errors."""
    try:
        pipe.send(data)
    except (pickle.PicklingError, TypeError) as error:
        error.traceback = format_exc()
        pipe.send(RemoteException(error, error.traceback))


SLEEP_UNIT = 0.1
# Borrowed from concurrent.futures
PENDING = 'PENDING'
RUNNING = 'RUNNING'
FINISHED = 'FINISHED'
CANCELLED = 'CANCELLED'
CANCELLED_AND_NOTIFIED = 'CANCELLED_AND_NOTIFIED'
