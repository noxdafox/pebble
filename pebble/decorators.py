# This file is part of Pebble.
# Copyright (c) 2013-2026, Matteo Cafasso

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

from __future__ import annotations

import signal
import threading

from functools import wraps
from typing import Callable, Iterable, overload

from pebble.common.types import P, T


_synchronized_lock: threading.Lock = threading.Lock()


@overload
def synchronized(function: Callable[P, T]) -> Callable[P, T]: ...
@overload
def synchronized(lock: threading.Lock | None) -> Callable[[Callable[P, T]], Callable[P, T]]: ...
def synchronized(*args, **kwargs):
    """A synchronized function prevents two or more callers to interleave
    its execution preventing race conditions.

    The synchronized decorator accepts as optional parameter a Lock, RLock or
    Semaphore object which will be employed to ensure the function's atomicity.

    If no synchronization object is given, a single threading.Lock will be used.
    This implies that between different decorated function only one at a time
    will be executed.

    """
    if callable(args[0]):
        return decorate_synchronized(args[0], _synchronized_lock)
    else:
        def wrap(function: Callable[P, T]) -> Callable[P, T]:
            return decorate_synchronized(function, args[0])

        return wrap


def decorate_synchronized(
    function: Callable[P, T], lock: threading.Lock
) -> Callable[P, T]:
    @wraps(function)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        with lock:
            return function(*args, **kwargs)

    return wrapper


def sighandler(signals: int | Iterable[int]) -> Callable:
    """Sets the decorated function as signal handler of given *signals*.

    *signals* can be either a single signal or a list/tuple
    of multiple ones.

    """

    def wrap(function: Callable[P, T]):
        set_signal_handlers(signals, function)

        @wraps(function)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            return function(*args, **kwargs)

        return wrapper

    return wrap


def set_signal_handlers(signals: int | Iterable[int], function: Callable):
    if isinstance(signals, int):
        signal.signal(signals, function)
    else:
        for signum in signals:
            signal.signal(signum, function)
            signal.signal(signum, function)
