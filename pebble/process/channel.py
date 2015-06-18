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
import sys
import time

from select import select
from contextlib import contextmanager
from multiprocessing import RLock, Pipe

from pebble.exceptions import TimeoutError


def channels(timeout):
    read0, write0 = Pipe(duplex=False)
    read1, write1 = Pipe(duplex=False)

    if os.name == 'nt':
        return WindowsChannel(read1, write0), UnixWorkerChannel(read0, write1,
                                                                timeout)
    else:
        return UnixChannel(read1, write0), UnixWorkerChannel(read0, write1,
                                                             timeout)


class Channel(object):
    def __init__(self, reader, writer):
        self.reader = reader
        self.writer = writer

    def recv(self):
        return self.reader.recv()

    def send(self, obj):
        return self.writer.send(obj)


class UnixChannel(Channel):
    def poll(self, timeout=None):
        if select([self.reader], [], [], timeout)[0]:
            return True
        else:
            return False


class WindowsChannel(Channel):
    def poll(self, timeout=None):
        return self.reader.poll(timeout=timeout)


class UnixWorkerChannel(UnixChannel):
    def __init__(self, reader, writer, timeout):
        super(UnixWorkerChannel, self).__init__(reader, writer)
        self.mutex = ChannelMutex(timeout)
        self.recv = self._make_recv_method()
        self.send = self._make_send_method()

    def __getstate__(self):
        return self.reader, self.writer, self.mutex.__getstate__()

    def __setstate__(self, state):
        self.reader, self.writer, mutex_state = state
        self.mutex.__setstate__(mutex_state)

        self.recv = self._make_recv_method()
        self.send = self._make_send_method()

    def _make_recv_method(self):
        def recv():
            with self.mutex.reader:
                return self.reader.recv()

        return recv

    def _make_send_method(self):
        def send(obj):
            with self.mutex.writer:
                return self.writer.send(obj)

        return send

    @property
    @contextmanager
    def lock(self):
        with self.mutex:
            yield self


class ChannelMutex(object):
    def __init__(self, acquire_timeout):
        self.reader_mutex = RLock()
        self.writer_mutex = RLock()
        self.acquire_timeout = acquire_timeout

    def __getstate__(self):
        return self.reader_mutex, self.writer_mutex

    def __setstate__(self, state):
        self.reader_mutex, self.writer_mutex = state

    def __enter__(self):
        if self.acquire(self.acquire_timeout):
            return self
        else:
            raise TimeoutError("Unable to acquire lock")

    def __exit__(self, *_):
        self.reader_mutex.release()
        self.writer_mutex.release()

    def acquire(self, timeout):
        timestamp = time.time()

        if acquire_mutex(self.reader_mutex, timeout):
            elapsed = time.time() - timestamp
            if acquire_mutex(self.writer_mutex, timeout - elapsed):
                return True

            self.reader_mutex.release()

        return False

    @property
    @contextmanager
    def reader(self):
        with self.reader_mutex:
            yield self

    @property
    @contextmanager
    def writer(self):
        with self.writer_mutex:
            yield self


def python_2_lock(mutex, timeout):
    """As Python 2 Lock has no timeout we implement our own."""
    timestamp = time.time()

    while 1:
        if mutex.acquire(False):
            return True
        else:
            if time.time() - timestamp > timeout:
                return False
            time.sleep(0.01)


if sys.version_info[0] < 3:
    acquire_mutex = python_2_lock
else:
    acquire_mutex = lambda m, t: m.acquire(timeout=t)
