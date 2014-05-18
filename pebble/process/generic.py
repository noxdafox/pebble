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
from contextlib import contextmanager

from multiprocessing import Pipe, Lock
try:  # Python 2
    from Queue import Empty
except:  # Python 3
    from queue import Empty


_registered_functions = {}


def trampoline(identifier, *args, **kwargs):
    """Trampoline function for decorators."""
    function = _registered_functions[identifier]

    return function(*args, **kwargs)


def dump_function(function, args):
    global _registered_functions

    identifier = id(function)
    if identifier not in _registered_functions:
        _registered_functions[identifier] = function
    args = [identifier] + list(args)

    return trampoline, args


@contextmanager
def suspend(queue):
    if queue._rlock is not None:
        queue._rlock.acquire()
    if queue._wlock is not None:
        queue._wlock.acquire()
    try:
        yield queue
    finally:
        if queue._rlock is not None:
            queue._rlock.acquire()
        if queue._wlock is not None:
            queue._wlock.acquire()


class SimpleQueue(object):
    def __init__(self):
        self._reader, self._writer = Pipe(duplex=False)
        self._rlock = Lock()
        self._wlock = os.name != 'nt' and Lock() or None
        self.get = self._make_get_method()
        self.put = self._make_put_method()

    def empty(self):
        return not self._reader.poll()

    def __getstate__(self):
        return (self._reader, self._writer,
                self._rlock, self._wlock, self._empty)

    def __setstate__(self, state):
        (self._reader, self._writer,
         self._rlock, self._wlock, self._empty) = state

        self.get = self._make_get_method()
        self.put = self._make_put_method()

    def _make_get_method(self):
        def get(timeout=None):
            if self._reader.poll(timeout):
                with self._rlock:
                    return self._reader.recv()
            else:
                raise Empty

        return get

    def _make_put_method(self):
        def put(obj, timeout=None):
            if self._wlock is not None:
                with self._wlock:
                    return self._writer.send(obj)
            else:
                return self._writer.send(obj)

        return put
