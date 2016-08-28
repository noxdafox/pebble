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

from select import select
from contextlib import contextmanager
from multiprocessing import RLock, Pipe

from pebble.exceptions import ChannelError


def channels():
    read0, write0 = Pipe(duplex=False)
    read1, write1 = Pipe(duplex=False)

    return Channel(read1, write0), WorkerChannel(read0, write1)


class Channel(object):
    def __init__(self, reader, writer):
        self.reader = reader
        self.writer = writer
        self.poll = self._make_poll_method()

    def _make_poll_method(self):
        def unix_poll(timeout=None):
            return bool(select([self.reader], [], [], timeout)[0])

        def windows_poll(timeout=None):
            return self.reader.poll(timeout)

        return os.name != 'nt' and unix_poll or windows_poll

    def recv(self):
        return self.reader.recv()

    def send(self, obj):
        return self.writer.send(obj)

    def close(self):
        self.reader.close()
        self.writer.close()


class WorkerChannel(Channel):
    def __init__(self, reader, writer):
        super(WorkerChannel, self).__init__(reader, writer)
        self.mutex = ChannelMutex()
        self.recv = self._make_recv_method()
        self.send = self._make_send_method()

    def __getstate__(self):
        return self.reader, self.writer, self.mutex

    def __setstate__(self, state):
        self.reader, self.writer, self.mutex = state

        self.poll = self._make_poll_method()
        self.recv = self._make_recv_method()
        self.send = self._make_send_method()

    def _make_recv_method(self):
        def recv():
            with self.mutex.reader:
                return self.reader.recv()

        return recv

    def _make_send_method(self):
        def unix_send(obj):
            with self.mutex.writer:
                return self.writer.send(obj)

        def windows_send(obj):
            return self.writer.send(obj)

        return os.name != 'nt' and unix_send or windows_send

    @property
    @contextmanager
    def lock(self):
        with self.mutex:
            yield self


class ChannelMutex(object):
    def __init__(self):
        self.reader_mutex = RLock()
        self.writer_mutex = os.name != 'nt' and RLock() or None
        self.acquire = self._make_acquire_method()
        self.release = self._make_release_method()

    def __getstate__(self):
        return self.reader_mutex, self.writer_mutex

    def __setstate__(self, state):
        self.reader_mutex, self.writer_mutex = state
        self.acquire = self._make_acquire_method()
        self.release = self._make_release_method()

    def __enter__(self):
        if self.acquire():
            return self
        else:
            raise ChannelError("Channel mutex time out")

    def __exit__(self, *_):
        self.release()

    def _make_acquire_method(self):
        def unix_acquire():
            return (self.reader_mutex.acquire(timeout=LOCK_TIMEOUT) and
                    self.writer_mutex.acquire(timeout=LOCK_TIMEOUT))

        def windows_acquire():
            return self.reader_mutex.acquire(timeout=LOCK_TIMEOUT)

        return os.name != 'nt' and unix_acquire or windows_acquire

    def _make_release_method(self):
        def unix_release():
            self.reader_mutex.release()
            self.writer_mutex.release()

        def windows_release():
            self.reader_mutex.release()

        return os.name != 'nt' and unix_release or windows_release

    @property
    @contextmanager
    def reader(self):
        if self.reader_mutex.acquire(timeout=LOCK_TIMEOUT):
            try:
                yield self
            finally:
                self.reader_mutex.release()
        else:
            raise ChannelError("Channel mutex time out")

    @property
    @contextmanager
    def writer(self):
        if self.writer_mutex.acquire(timeout=LOCK_TIMEOUT):
            try:
                yield self
            finally:
                self.writer_mutex.release()
        else:
            raise ChannelError("Channel mutex time out")


LOCK_TIMEOUT = 60
