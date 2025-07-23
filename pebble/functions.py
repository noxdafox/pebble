# This file is part of Pebble.
# Copyright (c) 2013-2025, Matteo Cafasso

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

import threading

from time import time
from types import MethodType
from typing import Callable, Optional


def waitforqueues(queues: list, timeout: float = None) -> filter:
    """Waits for one or more *Queue* to be ready or until *timeout* expires.

    *queues* is a list containing one or more *Queue.Queue* objects.
    If *timeout* is not None the function will block
    for the specified amount of seconds.

    The function returns a list containing the ready *Queues*.

    """
    lock = threading.Condition(threading.Lock())

    prepare_queues(queues, lock)
    try:
        wait_queues(queues, lock, timeout)
    finally:
        reset_queues(queues)

    return filter(lambda q: not q.empty(), queues)


def prepare_queues(queues: list, lock: threading.Condition):
    """Replaces queue._put() method in order to notify the waiting Condition."""
    for queue in queues:
        queue._pebble_lock = lock
        with queue.mutex:
            queue._pebble_old_method = queue._put
            queue._put = MethodType(new_method, queue)


def wait_queues(queues: list,
                lock: threading.Condition,
                timeout: Optional[float]):
    with lock:
        if not any(map(lambda q: not q.empty(), queues)):
            lock.wait(timeout)


def reset_queues(queues: list):
    """Resets original queue._put() method."""
    for queue in queues:
        with queue.mutex:
            queue._put = queue._pebble_old_method
        delattr(queue, '_pebble_old_method')
        delattr(queue, '_pebble_lock')


def waitforthreads(threads: list, timeout: float = None) -> filter:
    """Waits for one or more *Thread* to exit or until *timeout* expires.

    .. note::

       Expired *Threads* are not joined by *waitforthreads*.

    *threads* is a list containing one or more *threading.Thread* objects.
    If *timeout* is not None the function will block
    for the specified amount of seconds.

    The function returns a list containing the ready *Threads*.

    """
    old_get_ident = None
    event = threading.Event()

    def new_get_ident(*args) -> int:
        retval = old_get_ident(*args)
        event.set()

        return retval

    old_get_ident = prepare_threads(new_get_ident)
    try:
        wait_threads(threads, event, timeout)
    finally:
        reset_threads(old_get_ident)

    return filter(lambda t: not t.is_alive(), threads)


def prepare_threads(new_get_ident: Callable) -> Callable:
    """Replaces threading.get_ident() function in order to notify
    the waiting Condition."""
    with threading._active_limbo_lock:
        old_get_ident = threading.get_ident
        threading.get_ident = new_get_ident

        return old_get_ident


def wait_threads(threads: list,
                 event: threading.Event,
                 timeout: Optional[float]):
    timestamp = time()

    while not any(map(lambda t: not t.is_alive(), threads)):
        if timeout is None:
            event.wait()
        elif timeout - (time() - timestamp) > 0:
            event.wait(timeout - (time() - timestamp))
        else:
            return


def reset_threads(old_function: Callable):
    """Resets original threading.get_ident() function."""
    with threading._active_limbo_lock:
        threading.get_ident = old_function


def new_method(self, *args):
    self._pebble_old_method(*args)
    with self._pebble_lock:
        self._pebble_lock.notify_all()
