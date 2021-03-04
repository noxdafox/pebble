import unittest
import threading

from pebble import concurrent


def not_decorated(argument, keyword_argument=0):
    return argument + keyword_argument


@concurrent.thread
def decorated(argument, keyword_argument=0):
    """A docstring."""
    return argument + keyword_argument


@concurrent.thread
def error_decorated():
    raise RuntimeError("BOOM!")


@concurrent.thread()
def name_keyword_argument(name='function_kwarg'):
    return name


@concurrent.thread(name='concurrent_thread_name')
def name_keyword_decorated():
    return threading.current_thread().name


@concurrent.thread(name='decorator_kwarg')
def name_keyword_decorated_and_argument(name='bar'):
    return (threading.current_thread().name, name)


@concurrent.thread(daemon=False)
def daemon_keyword_decorated():
    return threading.current_thread().daemon


class ThreadConcurrentObj:
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
        self.concurrentobj = ThreadConcurrentObj()

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
        future = ThreadConcurrentObj.clsmethod()
        self.assertEqual(future.result(), 0)

    def test_instance_method(self):
        """Thread  decorated instance methods."""
        future = self.concurrentobj.instmethod()
        self.assertEqual(future.result(), 1)

    def test_static_method(self):
        """Thread  decorated static methods ( startmethod only)."""
        future = self.concurrentobj.stcmethod()
        self.assertEqual(future.result(), 2)

    def test_not_decorated_results(self):
        """Process Fork results are produced."""
        non_decorated = concurrent.thread(not_decorated)
        future = non_decorated(1, 1)
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

    def test_name_keyword_argument(self):
        """name keyword can be passed to a decorated function process without name """
        f = name_keyword_argument()
        fn_out = f.result()
        self.assertEqual(fn_out, "function_kwarg")

    def test_name_keyword_decorated(self):
        """
        Check that a simple use case of the name keyword passed to the decorator works
        """
        f = name_keyword_decorated()
        dec_out = f.result()
        self.assertEqual(dec_out, "concurrent_thread_name")

    def test_name_keyword_decorated_result(self):
        """name kwarg is handled  without modifying the function kwargs"""
        f = name_keyword_decorated_and_argument(name="function_kwarg")
        dec_out, fn_out = f.result()
        self.assertEqual(dec_out, "decorator_kwarg")
        self.assertEqual(fn_out, "function_kwarg")

    def test_daemon_keyword_decorated(self):
        """Daemon keyword can be passed to a decorated function and spawns correctly."""
        f = daemon_keyword_decorated()
        dec_out = f.result()
        self.assertEqual(dec_out, False)
