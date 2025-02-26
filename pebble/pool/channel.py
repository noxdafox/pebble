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


import os
import select
import multiprocessing

from typing import Any, Callable
from contextlib import contextmanager

from pebble.common import CONSTS


class ChannelError(OSError):
    """Error occurring within the process channel."""


def channels(mp_context: multiprocessing.context.BaseContext) -> tuple:
    read0, write0 = mp_context.Pipe(duplex=False)
    read1, write1 = mp_context.Pipe(duplex=False)

    return (Channel(read1, write0),
            WorkerChannel(read0, write1, (read1, write0), mp_context))


class Channel:
    def __init__(self, reader: multiprocessing.connection.Connection,
                 writer: multiprocessing.connection.Connection):
        self.reader = reader
        self.writer = writer
        self.poll = self._make_poll_method()

    def _make_poll_method(self):
        def unix_poll(timeout: float = None) -> bool:
            readonly_mask = (select.POLLIN  |
                             select.POLLPRI |
                             select.POLLHUP |
                             select.POLLERR)

            poll = select.poll()
            poll.register(self.reader, readonly_mask)

            # Convert from Seconds to Milliseconds
            if timeout is not None:
                timeout *= MILLISECONDS

            return bool(poll.poll(timeout))

        def windows_poll(timeout: float = None) -> bool:
            return self.reader.poll(timeout)

        return unix_poll if os.name != 'nt' else windows_poll

    def recv(self) -> Any:
        return self.reader.recv()

    def send(self, obj: Any):
        return self.writer.send(obj)

    def close(self):
        self.reader.close()
        self.writer.close()


class WorkerChannel(Channel):
    def __init__(self, reader: multiprocessing.connection.Connection,
                 writer: multiprocessing.connection.Connection,
                 unused: tuple,
                 mp_context: multiprocessing.context.BaseContext):
        super().__init__(reader, writer)
        self.mutex = ChannelMutex(mp_context)
        self.recv = self._make_recv_method()
        self.send = self._make_send_method()
        self.unused = unused

    def __getstate__(self) -> tuple:
        return self.reader, self.writer, self.mutex, self.unused

    def __setstate__(self, state: tuple):
        self.reader, self.writer, self.mutex, self.unused = state

        self.poll = self._make_poll_method()
        self.recv = self._make_recv_method()
        self.send = self._make_send_method()

    def _make_recv_method(self) -> Callable:
        def recv():
            with self.mutex.reader:
                return self.reader.recv()

        return recv

    def _make_send_method(self) -> Callable:
        def unix_send(obj: Any):
            with self.mutex.writer:
                return self.writer.send(obj)

        def windows_send(obj: Any):
            return self.writer.send(obj)

        return unix_send if os.name != 'nt' else windows_send

    @contextmanager
    def lock(self, block: bool = True, timeout: int = None) -> bool:
        """Lock the channel, yields True if channel is locked."""
        acquired = self.mutex.acquire(block=block, timeout=timeout)

        try:
            yield acquired
        finally:
            if acquired:
                self.mutex.release()

    def initialize(self):
        """Close unused connections."""
        for connection in self.unused:
            connection.close()


class ChannelMutex:
    def __init__(self, mp_context: multiprocessing.context.BaseContext):
        self.reader_mutex = mp_context.RLock()
        self.writer_mutex = mp_context.RLock() if os.name != 'nt' else None
        self.acquire = self._make_acquire_method()
        self.release = self._make_release_method()

    def __getstate__(self):
        return self.reader_mutex, self.writer_mutex

    def __setstate__(self, state: tuple):
        self.reader_mutex, self.writer_mutex = state
        self.acquire = self._make_acquire_method()
        self.release = self._make_release_method()

    def __enter__(self):
        if self.acquire():
            return self

        raise ChannelError("Channel mutex time out")

    def __exit__(self, *_):
        self.release()

    def _make_acquire_method(self) -> Callable:
        def unix_acquire(
                block: bool = True, timeout: int = CONSTS.channel_lock_timeout
        ) -> bool:
            """Acquire both locks. Returns True if both locks where acquired.
            Otherwise, handle the locks state.

            """
            if self.reader_mutex.acquire(block=block, timeout=timeout):
                if self.writer_mutex.acquire(block=block, timeout=timeout):
                    return True

                self.reader_mutex.release()

            return False

        def windows_acquire(
                block: bool = True, timeout: int = CONSTS.channel_lock_timeout
        ) -> bool:
            """Acquire the reader lock (on NT OS, writes are atomic)."""
            return self.reader_mutex.acquire(block=block, timeout=timeout)

        return windows_acquire if os.name == 'nt' else unix_acquire

    def _make_release_method(self) -> Callable:
        def unix_release():
            """Release both the locks."""
            self.reader_mutex.release()
            self.writer_mutex.release()

        def windows_release():
            """Release the reader lock (on NT OS, writes are atomic)."""
            self.reader_mutex.release()

        return windows_release if os.name == 'nt' else unix_release

    @property
    @contextmanager
    def reader(self):
        """Reader lock context manager."""
        if self.reader_mutex.acquire(timeout=CONSTS.channel_lock_timeout):
            try:
                yield self
            finally:
                self.reader_mutex.release()
        else:
            raise ChannelError("Channel mutex time out")

    @property
    @contextmanager
    def writer(self):
        """Writer lock context manager."""
        if self.writer_mutex.acquire(timeout=CONSTS.channel_lock_timeout):
            try:
                yield self
            finally:
                self.writer_mutex.release()
        else:
            raise ChannelError("Channel mutex time out")


MILLISECONDS = 1000
