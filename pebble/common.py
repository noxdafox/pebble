from __future__ import absolute_import

import os
import signal

from threading import Thread
from traceback import format_exc
from multiprocessing import Process

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


def launch_thread(function, *args, **kwargs):
    thread = Thread(target=function, args=args, kwargs=kwargs)
    thread.daemon = True
    thread.start()

    return thread


def launch_process(function, *args, **kwargs):
    process = Process(target=function, args=args, kwargs=kwargs)
    process.daemon = True
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


def send_result(pipe, data):
    """Send result handling communication errors."""
    try:
        pipe.send(data)
    except TypeError as error:
        error.traceback = format_exc()
        pipe.send(error)


SLEEP_UNIT = 0.1
# Borrowed from concurrent.futures
PENDING = 'PENDING'
RUNNING = 'RUNNING'
FINISHED = 'FINISHED'
CANCELLED = 'CANCELLED'
CANCELLED_AND_NOTIFIED = 'CANCELLED_AND_NOTIFIED'
