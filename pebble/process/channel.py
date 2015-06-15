import os
import time

from select import select
from contextlib import contextmanager
from multiprocessing import RLock, Pipe

from pebble.pool import SLEEP_UNIT
from pebble.exceptions import TimeoutError


def channels():
    read0, write0 = Pipe(duplex=False)
    read1, write1 = Pipe(duplex=False)

    if os.name == 'nt':
        return WindowsChannel(read1, write0), UnixWorkerChannel(read0, write1)
    else:
        return UnixChannel(read1, write0), UnixWorkerChannel(read0, write1)


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
    def __init__(self, reader, writer):
        super(UnixWorkerChannel, self).__init__(reader, writer)
        self.mutex = ChannelMutex()
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
    def __init__(self):
        self.reader_mutex = RLock()
        self.writer_mutex = RLock()

    def __getstate__(self):
        return self.reader_mutex, self.writer_mutex

    def __setstate__(self, state):
        self.reader_mutex, self.writer_mutex = state

    def __enter__(self):
        if self.acquire():
            return self
        else:
            self.reader_mutex.release()
            raise TimeoutError("Unable to acquire lock")

    def __exit__(self, *_):
        self.reader_mutex.release()
        self.writer_mutex.release()

    def acquire(self):
        self.reader_mutex.acquire()
        try:
            return self.writer_mutex.acquire(timeout=SLEEP_UNIT)
        except TypeError:
            return python_2_lock(self.writer_mutex, SLEEP_UNIT)

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
        if mutex.acquire(blocking=False):
            return True
        else:
            if time.time() - timestamp > timeout:
                time.sleep(0.01)
                return False
