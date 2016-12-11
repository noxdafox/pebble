import unittest
import threading

from pebble import concurrent


@concurrent.thread
def decorated(argument, keyword_argument=0):
    """A docstring."""
    return argument + keyword_argument


@concurrent.thread
def error_decorated():
    raise RuntimeError("BOOM!")


class TestThreadConcurrentObj:
    a = 0

    def __init__(self):
        self.b = 1

    @classmethod
    @concurrent.thread
    def clsmethod(cls):
        return cls.a

    @concurrent.thread
    def instmethod(self):
        return self.b

    @staticmethod
    @concurrent.thread
    def stcmethod():
        return 2


class TestThreadConcurrent(unittest.TestCase):
    def setUp(self):
        self.results = 0
        self.exception = None
        self.event = threading.Event()
        self.event.clear()
        self.concurrentobj = TestThreadConcurrentObj()

    def callback(self, future):
        try:
            self.results = future.result()
        except (RuntimeError) as error:
            self.exception = error
        finally:
            self.event.set()

    def test_docstring(self):
        """Thread  docstring is preserved."""
        self.assertEqual(decorated.__doc__, "A docstring.")

    def test_class_method(self):
        """Thread  decorated classmethods."""
        future = TestThreadConcurrentObj.clsmethod()
        self.assertEqual(future.result(), 0)

    def test_instance_method(self):
        """Thread  decorated instance methods."""
        future = self.concurrentobj.instmethod()
        self.assertEqual(future.result(), 1)

    def test_static_method(self):
        """Thread  decorated static methods ( startmethod only)."""
        future = self.concurrentobj.stcmethod()
        self.assertEqual(future.result(), 2)

    def test_decorated_results(self):
        """Thread  results are produced."""
        future = decorated(1, 1)
        self.assertEqual(future.result(), 2)

    def test_decorated_results_callback(self):
        """Thread  results are forwarded to the callback."""
        future = decorated(1, 1)
        future.add_done_callback(self.callback)
        self.event.wait(timeout=1)
        self.assertEqual(self.results, 2)

    def test_error_decorated(self):
        """Thread  errors are raised by future.result."""
        future = error_decorated()
        with self.assertRaises(RuntimeError):
            future.result()

    def test_error_decorated_callback(self):
        """Thread  errors are forwarded to callback."""
        future = error_decorated()
        future.add_done_callback(self.callback)
        self.event.wait(timeout=1)
        self.assertTrue(isinstance(self.exception, RuntimeError),
                        msg=str(self.exception))
