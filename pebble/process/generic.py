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

from multiprocessing import Pipe, Lock, RLock
try:  # Python 2
    from Queue import Empty
except:  # Python 3
    from queue import Empty

from ..pebble import TimeoutError


_registered_functions = {}


def trampoline(identifier, *args, **kwargs):
    """Trampoline function for decorators."""
    function = _registered_functions[identifier]

    return function(*args, **kwargs)


def dump_function(function, args):
    """Dumps a decorated function."""
    global _registered_functions

    identifier = id(function)
    if identifier not in _registered_functions:
        _registered_functions[identifier] = function
    args = [identifier] + list(args)

    return trampoline, args


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
            with self._rlock:
                if self._reader.poll(timeout):
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


# --------------------------------------------------------------------------- #
#                              Pool's Related                                 #
# --------------------------------------------------------------------------- #
def channels():
    """Process Pool channel factory."""
    read0, write0 = Pipe()
    read1, write1 = Pipe()

    return PoolChannel(read1, write0), WorkerChannel(read0, write1)


@contextmanager
def lock(channel):
    channel.rlock.acquire()
    if channel.wlock is not None:
        channel.wlock.acquire()
    try:
        yield channel
    finally:
        channel.rlock.release()
        if channel.wlock is not None:
            channel.wlock.release()


class PoolChannel(object):
    """Pool's side of the channel."""
    def __init__(self, reader, writer):
        self.reader = reader
        self.writer = writer

    def poll(self, timeout=None):
        return self.reader.poll(timeout)

    def recv(self, timeout=None):
        if self.reader.poll(timeout):
            return self.reader.recv()
        else:
            raise TimeoutError("Channel timeout")

    def send(self, obj):
        return self.writer.send(obj)


class WorkerChannel(PoolChannel):
    """Worker's side of the channel."""
    def __init__(self, reader, writer):
        super(WorkerChannel, self).__init__(reader, writer)
        self.rlock = RLock()
        self.wlock = os.name != 'nt' and RLock() or None
        self.recv = self._make_recv_method()
        self.send = self._make_send_method()

    def __getstate__(self):
        return (self._reader, self._writer,
                self._rlock, self._wlock, self._empty)

    def __setstate__(self, state):
        (self._reader, self._writer,
         self._rlock, self._wlock, self._empty) = state

        self.recv = self._make_recv_method()
        self.send = self._make_send_method()

    def _make_recv_method(self):
        def recv(timeout=None):
            with self.rlock:
                if self.reader.poll(timeout):
                    return self.reader.recv()
                else:
                    raise TimeoutError("Channel timeout")

        return recv

    def _make_send_method(self):
        def send(obj):
            if self.wlock is not None:
                with self.wlock:
                    return self.writer.send(obj)
            else:
                return self.writer.send(obj)

        return send
