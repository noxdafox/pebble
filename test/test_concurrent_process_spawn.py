import os
import sys
import time
import signal
import unittest
import threading
import multiprocessing
from concurrent.futures import CancelledError, TimeoutError

from pebble import concurrent, ProcessExpired


# set start method
supported = False

if sys.version_info.major > 2 and sys.version_info.minor > 3:
    methods = multiprocessing.get_all_start_methods()
    if 'spawn' in methods:
        try:
            multiprocessing.set_start_method('spawn')

            if multiprocessing.get_start_method() == 'spawn':
                supported = True
        except RuntimeError:  # child process
            pass


@concurrent.process
def decorated(argument, keyword_argument=0):
    """A docstring."""
    return argument + keyword_argument


@concurrent.process
def error_decorated():
    raise RuntimeError("BOOM!")


@concurrent.process
def pickling_error_decorated():
    event = threading.Event()
    return event


@concurrent.process
def critical_decorated():
    os._exit(123)


@concurrent.process
def decorated_cancel():
    time.sleep(10)


@concurrent.process(timeout=0.1)
def long_decorated():
    time.sleep(10)


@concurrent.process(timeout=0.1)
def sigterm_decorated():
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    time.sleep(10)


class ProcessConcurrentObj:
    a = 0

    def __init__(self):
        self.b = 1

    @classmethod
    @concurrent.process
    def clsmethod(cls):
        return cls.a

    @concurrent.process
    def instmethod(self):
        return self.b


@unittest.skipIf(not supported, "Start method is not supported")
class TestProcessConcurrent(unittest.TestCase):
    def setUp(self):
        self.results = 0
        self.exception = None
        self.event = threading.Event()
        self.event.clear()
        self.concurrentobj = ProcessConcurrentObj()

    def callback(self, future):
        try:
            self.results = future.result()
        except (ProcessExpired, RuntimeError, TimeoutError) as error:
            self.exception = error
        finally:
            self.event.set()

    def test_docstring(self):
        """Process Spawn docstring is preserved."""
        self.assertEqual(decorated.__doc__, "A docstring.")

    def test_wrong_timeout(self):
        """Process Spawn TypeError is raised if timeout is not number."""
        with self.assertRaises(TypeError):
            @concurrent.process(timeout='Foo')
            def function():
                return

    def test_class_method(self):
        """Process Spawn decorated classmethods."""
        future = ProcessConcurrentObj.clsmethod()
        self.assertEqual(future.result(), 0)

    def test_instance_method(self):
        """Process Spawn decorated instance methods."""
        future = self.concurrentobj.instmethod()
        self.assertEqual(future.result(), 1)

    def test_decorated_results(self):
        """Process Spawn results are produced."""
        future = decorated(1, 1)
        self.assertEqual(future.result(), 2)

    def test_decorated_results_callback(self):
        """Process Spawn results are forwarded to the callback."""
        future = decorated(1, 1)
        future.add_done_callback(self.callback)
        self.event.wait(timeout=1)
        self.assertEqual(self.results, 2)

    def test_error_decorated(self):
        """Process Spawn errors are raised by future.result."""
        future = error_decorated()
        with self.assertRaises(RuntimeError):
            future.result()

    def test_error_decorated_callback(self):
        """Process Spawn errors are forwarded to callback."""
        future = error_decorated()
        future.add_done_callback(self.callback)
        self.event.wait(timeout=1)
        self.assertTrue(isinstance(self.exception, RuntimeError),
                        msg=str(self.exception))

    def test_pickling_error_decorated(self):
        """Process Spawn pickling errors are raised by future.result."""
        future = pickling_error_decorated()
        with self.assertRaises(TypeError):
            future.result()

    def test_timeout_decorated(self):
        """Process Spawn raises TimeoutError if so."""
        future = long_decorated()
        with self.assertRaises(TimeoutError):
            future.result()

    def test_timeout_decorated_callback(self):
        """Process Spawn TimeoutError is forwarded to callback."""
        future = long_decorated()
        future.add_done_callback(self.callback)
        self.event.wait(timeout=1)
        self.assertTrue(isinstance(self.exception, TimeoutError),
                        msg=str(self.exception))

    def test_decorated_dead_process(self):
        """Process Spawn ProcessExpired is raised if process dies."""
        future = critical_decorated()
        with self.assertRaises(ProcessExpired):
            future.result()

    def test_timeout_decorated_callback(self):
        """Process Spawn ProcessExpired is forwarded to callback."""
        future = critical_decorated()
        future.add_done_callback(self.callback)
        self.event.wait(timeout=1)
        self.assertTrue(isinstance(self.exception, ProcessExpired),
                        msg=str(self.exception))

    def test_cancel_decorated(self):
        """Process Spawn raises CancelledError if future was cancelled."""
        future = decorated_cancel()
        future.cancel()
        self.assertRaises(CancelledError, future.result)

    @unittest.skipIf(os.name == 'nt', "Test won't run on Windows.")
    def test_decorated_ignoring_sigterm(self):
        """Process Spawn Concurrent ignored SIGTERM signal are handled on Unix."""
        future = sigterm_decorated()
        with self.assertRaises(TimeoutError):
            future.result()
