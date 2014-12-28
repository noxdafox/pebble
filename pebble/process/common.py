
import os
import sys
from functools import wraps
from contextlib import contextmanager

from multiprocessing import Pipe, RLock
if os.name in ('posix', 'os2'):
    from signal import SIGKILL

from ..pebble import TimeoutError


_registered_functions = {}


def stop(worker):
    """Does its best to stop the worker."""
    worker.terminate()
    worker.join(3)

    if worker.is_alive() and os.name != 'nt':
        try:
            os.kill(worker.pid, SIGKILL)
            worker.join()
        except OSError:
            return

    if worker.is_alive():
        raise RuntimeError("Unable to terminate PID %d" % os.getpid())


def decorate(function, launcher, **properties):
    """Decorates the given function
    taking care of Windows process decoration issues.

    *function* represent the target function to be decorated,
    *launcher* takes care of executing the function with the
    given decoration *properties*.

    """
    if os.name == 'nt':
        register_function(function)

    @wraps(function)
    def wrapper(*args, **kwargs):
        if os.name == 'nt':
            target, args = dump_function(function, args)
        else:
            target = function

        return launcher(target, args=args, kwargs=kwargs, **properties)

    return wrapper


def register_function(function):
    global _registered_functions

    _registered_functions[function.__name__] = function


def dump_function(function, args):
    """Dumps a decorated function."""
    args = [function.__name__, function.__module__] + list(args)

    return trampoline, args


def trampoline(name, module, *args, **kwargs):
    """Trampoline function for decorators.

    Lookups the function between the registered ones;
    if not found, forces its registering and then executes it.

    """
    try:
        function = _registered_functions[name]
    except KeyError:  # force function registering
        __import__(module)
        mod = sys.modules[module]
        getattr(mod, name)
    finally:
        function = _registered_functions[name]

    return function(*args, **kwargs)


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
        return (self.reader, self.writer,
                self.rlock, self.wlock)

    def __setstate__(self, state):
        (self.reader, self.writer,
         self.rlock, self.wlock) = state

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
