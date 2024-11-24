# This file is part of Pebble.
# Copyright (c) 2013-2024, Matteo Cafasso

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

import asyncio

from enum import Enum, IntEnum
from dataclasses import dataclass
from concurrent.futures import Future
from typing import Any, TypeVar, Callable


P = TypeVar("P")
T = TypeVar("T")


try:
    FutureType = Future[T]
except TypeError:
    FutureType = Future


class ProcessExpired(OSError):
    """Raised when process dies unexpectedly."""
    def __init__(self, msg, code=0, pid=None):
        super(ProcessExpired, self).__init__(msg)
        self.exitcode = code
        self.pid = pid


class PebbleFuture(FutureType):
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
            if self._state == FutureStatus.CANCELLED:
                self._state = FutureStatus.CANCELLED_AND_NOTIFIED
                for waiter in self._waiters:
                    waiter.add_cancelled(self)

                return False
            elif self._state == FutureStatus.PENDING:
                self._state = FutureStatus.RUNNING

                return True
            else:
                raise RuntimeError('Future in unexpected state')


try:
    PebbleFutureType = PebbleFuture[T]
except TypeError:
    PebbleFutureType = PebbleFuture


class ProcessFuture(PebbleFutureType):
    def cancel(self):
        """Cancel the future.

        Returns True if the future was cancelled, False otherwise. A future
        cannot be cancelled if it has already completed.
        """
        with self._condition:
            if self._state == FutureStatus.FINISHED:
                return False

            if self._state in (FutureStatus.CANCELLED,
                               FutureStatus.CANCELLED_AND_NOTIFIED):
                return True

            self._state = FutureStatus.CANCELLED
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


class RemoteException:
    """Pickling wrapper for exceptions in remote process."""

    def __init__(self, exception, traceback):
        self.exception = exception
        self.traceback = traceback

    def __reduce__(self):
        return self.rebuild_exception, (self.exception, self.traceback)

    @staticmethod
    def rebuild_exception(exception, traceback):
        try:
            exception.traceback = traceback
            exception.__cause__ = RemoteTraceback(traceback)
        except AttributeError:  # Frozen exception
            pass

        return exception


class ResultStatus(IntEnum):
    """Status of results of a function execution."""
    SUCCESS = 0
    FAILURE = 1
    ERROR = 2


@dataclass
class Result:
    """Result of a function execution."""
    status: ResultStatus
    value: Any


class FutureStatus(str, Enum):
    """Borrowed from concurrent.futures."""
    PENDING = 'PENDING'
    RUNNING = 'RUNNING'
    FINISHED = 'FINISHED'
    CANCELLED = 'CANCELLED'
    CANCELLED_AND_NOTIFIED = 'CANCELLED_AND_NOTIFIED'


@dataclass
class Consts:
    """Internal constants.

    WARNING: changing these values will affect the behaviour
    of Pools and decorators.

    """
    sleep_unit: float = 0.1
    """Any cycle which needs to periodically assess the state."""
    term_timeout: float = 3
    """On UNIX once a SIGTERM signal is issued to a process,
    the amount of seconds to wait before issuing a SIGKILL signal."""


try:
    CallableType = Callable[[P], T]
    AsyncIODecoratorReturnType = Callable[[P], asyncio.Future[T]]
    AsyncIODecoratorParamsReturnType = Callable[[Callable[[P], T]],
                                                Callable[[P], asyncio.Future[T]]]
    ThreadDecoratorReturnType = Callable[[P], Future[T]]
    ThreadDecoratorParamsReturnType = Callable[[Callable[[P], T]],
                                               Callable[[P], Future[T]]]
    ProcessDecoratorReturnType = Callable[[P], ProcessFuture[T]]
    ProcessDecoratorParamsReturnType = Callable[[Callable[[P], T]],
                                                Callable[[P], ProcessFuture[T]]]
except TypeError:
    ReturnType = Callable
    AsyncIODecoratorReturnType = Callable
    AsyncIODecoratorParamsReturnType = Callable
    ThreadDecoratorReturnType = Callable
    ThreadDecoratorParamsReturnType = Callable
    ProcessDecoratorReturnType = Callable
    ProcessDecoratorParamsReturnType = Callable


CONSTS = Consts()
