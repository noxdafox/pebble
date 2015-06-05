import os

from select import select
from contextlib import contextmanager
from multiprocessing import Lock, Pipe


def channels():
    read0, write0 = Pipe(duplex=False)
    read1, write1 = Pipe(duplex=False)

    if os.name == 'nt':
        return WindowsChannel(read1, write0), WorkerChannel(read0, write1)
    else:
        return UnixChannel(read1, write0), WorkerChannel(read0, write1)


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


class WorkerChannel(Channel):
    def __init__(self, reader, writer):
        super(WorkerChannel, self).__init__(reader, writer)
        self.reader_mutex = Lock()
        self.writer_mutex = os.name != 'nt' and Lock() or None
        self.recv = self._make_recv_method()
        self.send = self._make_send_method()

    def __getstate__(self):
        return (self.reader, self.writer,
                self.reader_mutex, self.writer_mutex)

    def __setstate__(self, state):
        (self.reader, self.writer,
         self.reader_mutex, self.writer_mutex) = state

        self.recv = self._make_recv_method()
        self.send = self._make_send_method()

    def _make_recv_method(self):
        def recv():
            with self.reader_mutex:
                return self.reader.recv()

        return recv

    def _make_send_method(self):
        def send(obj):
            if self.writer_mutex is not None:
                with self.writer_mutex:
                    return self.writer.send(obj)
            else:
                return self.writer.send(obj)

        return send

    @property
    @contextmanager
    def lock(self):
        with self.reader_mutex:
            if self.writer_mutex is not None:
                with self.writer_mutex:
                    yield self
            else:
                yield self
