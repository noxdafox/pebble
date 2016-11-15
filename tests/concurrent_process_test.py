import os
import time
import signal
import unittest
import threading
from concurrent.futures import TimeoutError

from pebble import concurrent, ProcessExpired


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
def long_decorated():
    time.sleep(10)


@concurrent.process
def sigterm_decorated():
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    time.sleep(10)


class TestProcessConcurrentObj(object):
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

    @staticmethod
    @concurrent.process
    def stcmethod():
        return 2


class TestProcessConcurrent(unittest.TestCase):
    def setUp(self):
        self.results = 0
        self.exception = None
        self.event = threading.Event()
        self.event.clear()
        self.concurrentobj = TestProcessConcurrentObj()
        decorated.add_done_callback(self.callback)
        error_decorated.add_done_callback(self.callback)
        long_decorated.add_done_callback(self.callback)
        long_decorated.add_timeout(0.1)
        sigterm_decorated.add_timeout(0.1)
        critical_decorated.add_done_callback(self.callback)

    def callback(self, future):
        try:
            self.results = future.result()
        except (ProcessExpired, RuntimeError, TimeoutError) as error:
            self.exception = error
        finally:
            self.event.set()

    def test_docstring(self):
        """Process docstring is preserved."""
        self.assertEqual(decorated.__doc__, "A docstring.")

    def test_wrong_callback(self):
        """Process TypeError is raised if callback is not callable."""
        with self.assertRaises(TypeError):
            decorated.add_done_callback(1)

    def test_wrong_timeout(self):
        """Process TypeError is raised if timeout is not number."""
        with self.assertRaises(TypeError):
            decorated.add_timeout('timeout')

    def test_class_method(self):
        """Process decorated classmethods."""
        future = TestProcessConcurrentObj.clsmethod()
        self.assertEqual(future.result(), 0)

    def test_instance_method(self):
        """Process decorated instance methods."""
        future = self.concurrentobj.instmethod()
        self.assertEqual(future.result(), 1)

    @unittest.skipIf(os.name == 'nt', "Test won't run on Windows.")
    def test_static_method(self):
        """Process decorated static methods (Unix only)."""
        future = self.concurrentobj.stcmethod()
        self.assertEqual(future.result(), 2)

    def test_decorated_results(self):
        """Process results are produced."""
        future = decorated(1, 1)
        self.assertEqual(future.result(), 2)

    def test_decorated_results_callback(self):
        """Process results are forwarded to the callback."""
        decorated(1, 1)
        self.event.wait(timeout=1)
        self.assertEqual(self.results, 2)

    def test_error_decorated(self):
        """Process errors are raised by future.result."""
        future = error_decorated()
        with self.assertRaises(RuntimeError):
            future.result()

    def test_error_decorated_callback(self):
        """Process errors are forwarded to callback."""
        error_decorated()
        self.event.wait(timeout=1)
        self.assertTrue(isinstance(self.exception, RuntimeError),
                        msg=str(self.exception))

    def test_pickling_error_decorated(self):
        """Process pickling errors are raised by future.result."""
        future = pickling_error_decorated()
        with self.assertRaises(TypeError):
            future.result()

    def test_timeout_decorated(self):
        """Process raises TimeoutError if so."""
        future = long_decorated()
        with self.assertRaises(TimeoutError):
            future.result()

    def test_timeout_decorated_callback(self):
        """Process TimeoutError is forwarded to callback."""
        long_decorated()
        self.event.wait(timeout=1)
        self.assertTrue(isinstance(self.exception, TimeoutError),
                        msg=str(self.exception))

    def test_decorated_dead_process(self):
        """Process ProcessExpired is raised if process dies."""
        future = critical_decorated()
        with self.assertRaises(ProcessExpired):
            future.result()

    def test_timeout_decorated_callback(self):
        """Process ProcessExpired is forwarded to callback."""
        critical_decorated()
        self.event.wait(timeout=1)
        self.assertTrue(isinstance(self.exception, ProcessExpired),
                        msg=str(self.exception))

    @unittest.skipIf(os.name == 'nt', "Test won't run on Windows.")
    def test_decorated_pool_ignoring_sigterm(self):
        """Process Concurrent ignored SIGTERM signal are handled on Unix."""
        future = sigterm_decorated()
        with self.assertRaises(TimeoutError):
            future.result()
