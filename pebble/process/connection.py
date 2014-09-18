import os
import struct
import asyncio

from pickle import loads, dumps


def pipe():
    reader, writer = os.pipe()

    reader = Connection(os.fdopen(reader, 'rb', 0))
    writer = Connection(os.fdopen(writer, 'wb', 0))

    return reader, writer


@asyncio.coroutine
def recv(reader, size):
    """Waits until size bytes are received."""
    data = bytearray()

    while len(data) < size:
        data += yield from reader.read(size - len(data))

    return data


class Connection(object):
    def __init__(self, handle):
        self._reader = None
        self._writer = None
        self._handle = handle

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        self.close()

    @asyncio.coroutine
    def _recv(self):
        """Receives data from the other side of the connection
        and deserializes it.

        """
        if self._reader.at_eof():
            raise EOFError("End Of File")

        data = yield from recv(self._reader, 4)
        size, = struct.unpack('!i', data)
        data = yield from recv(self._reader, size)

        return loads(data)

    def _closed(self):
        if self.closed:
            raise OSError("handle is closed")

    def _readable(self):
        if not self.readable:
            raise OSError("connection is not readable")

    def _writable(self):
        if not self.writable:
            raise OSError("connection is not writable")

    @property
    def closed(self):
        """True if the Connection is closed"""
        return self._handle is None

    @property
    def readable(self):
        """True if the connection is readable"""
        if self._handle is not None:
            return self._reader is not None
        else:
            return False

    @property
    def writable(self):
        """True if the connection is writable"""
        if self._handle is not None:
            return self._writer is not None
        else:
            return False

    @asyncio.coroutine
    def connect(self, loop=None):
        loop = loop is not None and loop or asyncio.get_event_loop()

        if self._handle.mode == 'rb':
            self._reader = asyncio.StreamReader(loop=loop)
            protocol = asyncio.StreamReaderProtocol(self._reader, loop=loop)
            yield from loop.connect_read_pipe(lambda: protocol, self._handle)
        else:
            trns, prot = yield from loop.connect_write_pipe(asyncio.Protocol,
                                                            self._handle)
            self._writer = asyncio.StreamWriter(trns, prot, None, loop)

    @asyncio.coroutine
    def poll(self, timeout=None):
        self._closed()
        self._readable()

        try:
            yield from asyncio.wait_for(self._receive(), timeout)
        except asyncio.TimeoutError:
            return False
        else:
            return True

    @asyncio.coroutine
    def recv(self):
        self._closed()
        self._readable()

        return (yield from self._recv())

    def fileno(self):
        """File descriptor or handle of the connection"""
        self._closed()

        return self._handle

    def close(self):
        """Close the connection"""
        if self._handle is not None:
            try:
                self._handle.close()
            finally:
                self._handle = None

    def send(self, obj):
        self._closed()
        self._writable()

        data = dumps(obj)
        size = struct.pack('!i', len(data))

        self._writer.write(size)
        self._writer.write(data)
