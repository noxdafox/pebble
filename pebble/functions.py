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

import threading

from time import time
from types import MethodType


_waitforthreads_lock = threading.Lock()


def waitforqueues(queues, timeout=None):
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


def prepare_queues(queues, lock):
    """Replaces queue._put() method in order to notify the waiting Condition."""
    for queue in queues:
        queue._pebble_lock = lock
        with queue.mutex:
            queue._pebble_old_method = queue._put
            queue._put = MethodType(new_method, queue)


def wait_queues(queues, lock, timeout):
    with lock:
        if not any(map(lambda q: not q.empty(), queues)):
            lock.wait(timeout)


def reset_queues(queues):
    """Resets original queue._put() method."""
    for queue in queues:
        with queue.mutex:
            queue._put = queue._pebble_old_method
        delattr(queue, '_pebble_old_method')
        delattr(queue, '_pebble_lock')


def waitforthreads(threads, timeout=None):
    """Waits for one or more *Thread* to exit or until *timeout* expires.

    .. note::

       Expired *Threads* are not joined by *waitforthreads*.

    *threads* is a list containing one or more *threading.Thread* objects.
    If *timeout* is not None the function will block
    for the specified amount of seconds.

    The function returns a list containing the ready *Threads*.

    """
    old_function = None
    lock = threading.Condition(threading.Lock())

    def new_function(*args):
        old_function(*args)
        with lock:
            lock.notify_all()

    old_function = prepare_threads(new_function)
    try:
        wait_threads(threads, lock, timeout)
    finally:
        reset_threads(old_function)

    return filter(lambda t: not t.is_alive(), threads)


def prepare_threads(new_function):
    """Replaces threading._get_ident() function in order to notify
    the waiting Condition."""
    with _waitforthreads_lock:
        if hasattr(threading, 'get_ident'):
            old_function = threading.get_ident
            threading.get_ident = new_function
        else:
            old_function = threading._get_ident
            threading._get_ident = new_function

        return old_function


def wait_threads(threads, lock, timeout):
    timestamp = time()
    time_left = lambda: timeout - (time() - timestamp)

    with lock:
        while not any(map(lambda t: not t.is_alive(), threads)):
            if timeout is None:
                lock.wait()
            elif time_left() > 0:
                lock.wait(time_left())
            else:
                return


def reset_threads(old_function):
    """Resets original threading._get_ident() function."""
    with _waitforthreads_lock:
        if hasattr(threading, 'get_ident'):
            threading.get_ident = old_function
        else:
            threading._get_ident = old_function


def new_method(self, *args):
    self._pebble_old_method(*args)
    with self._pebble_lock:
        self._pebble_lock.notify_all()
