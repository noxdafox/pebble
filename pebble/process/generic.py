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

from multiprocessing import Pipe, Lock, Event
try:  # Python 2
    from Queue import Queue, Empty, Full
except:  # Python 3
    from queue import Queue, Empty, Full


_registered_functions = {}


# -------------------- Deal with decoration and pickling -------------------- #
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


class Channel(object):
    def __init__(self, buffered=False):
        self._reader, self._writer = Pipe(duplex=False)
        self._rlock = Lock()
        self._wlock = os.name != 'nt' and Lock() or None
        if not buffered:
            self._empty = Event()
            self._empty.set()
        else:
            self._empty = None
        self.get = self._make_get_method()
        self.put = self._make_put_method()

    def empty(self):
        return not self._reader.poll()

    def pause(self):
        self._rlock.acquire()
        if self._wlock is not None:
            self._wlock.acquire()

    def resume(self):
        self._rlock.release()
        if self._wlock is not None:
            self._wlock.release()

    def __getstate__(self):
        return (self._reader, self._writer,
                self._rlock, self._wlock, self._empty)

    def __setstate__(self, state):
        (self._reader, self._writer,
         self._rlock, self._wlock, self._empty) = state

        self.get = self._make_get_method()
        self.put = self._make_put_method()

    def _make_get_method(self):
        reader = self._reader

        def get(timeout=None):
            with self._rlock:
                if reader.poll(timeout):
                    if self._empty is not None:
                        self._empty.set()

                    return reader.recv()
                else:
                    raise Empty

        return get

    def _make_put_method(self):
        writer = self._writer

        def put(obj, timeout=None):
            if self._wlock is None:
                if self._empty is not None:
                    if not self._empty.wait(timeout):
                        raise Full
                    self._empty.clear()

                return writer.send(obj)

            with self._wlock:
                if self._empty is not None:
                    if not self._empty.wait(timeout):
                        raise Full
                    self._empty.clear()

                return writer.send(obj)

        return put
