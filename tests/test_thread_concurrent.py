import time
import unittest
import threading

from pebble import thread, TaskCancelled


event = threading.Event()
initarg = 0
results = 0
exception = None


def callback(task):
    global results
    global exception

    try:
        results = task.get()
    except Exception as error:
        exception = error
    finally:
        event.set()


def undecorated_simple():
    return 0


def undecorated(argument, keyword_argument=0):
    """A docstring."""
    return argument + keyword_argument


@thread.concurrent
def decorated(argument, keyword_argument=0):
    """A docstring."""
    return argument + keyword_argument


@thread.concurrent
def error_decorated():
    raise Exception("BOOM!")


@thread.concurrent
def long_decorated():
    time.sleep(1)


@thread.concurrent(callback=callback)
def decorated_callback(argument, keyword_argument=0):
    """A docstring."""
    return argument + keyword_argument


@thread.concurrent(callback=callback)
def error_decorated_callback():
    raise Exception("BOOM!")


@thread.concurrent(callback=callback)
def long_decorated_callback():
    time.sleep(1)


class TestThreadConcurrentObj(object):
    a = 0

    def __init__(self):
        self.b = 1

    @classmethod
    @thread.concurrent
    def clsmethod(cls):
        return cls.a

    @thread.concurrent
    def instmethod(self):
        return self.b

    @staticmethod
    @thread.concurrent
    def stcmethod():
        return 2


class TestThreadConcurrent(unittest.TestCase):
    def setUp(self):
        global results
        global exception
        results = 0
        exception = None
        event.clear()
        self.concurrentobj = TestThreadConcurrentObj()

    def callback(self, task):
        self.results = task.get()
        event.set()

    def test_docstring(self):
        """Thread Concurrent docstring is preserved."""
        self.assertEqual(decorated.__doc__, "A docstring.")

    def test_wrong_parameters(self):
        """Thread Concurrent raises ValueError if wrong params."""
        self.assertRaises(ValueError, thread.concurrent, undecorated,
                          args=[1])

    def test_thread_wrong_decoration(self):
        """Thread Concurrent raises ValueError if given wrong params."""
        try:
            @thread.concurrent(5, name='foo')
            def wrong():
                return
        except Exception as error:
            self.assertTrue(isinstance(error, ValueError))

    def test_class_method(self):
        """Thread Concurrent decorated classmethods."""
        task = TestThreadConcurrentObj.clsmethod()
        self.assertEqual(task.get(), 0)

    def test_instance_method(self):
        """Thread Concurrent decorated instance methods."""
        task = self.concurrentobj.instmethod()
        self.assertEqual(task.get(), 1)

    def test_static_method(self):
        """Thread Concurrent decorated static methods."""
        task = self.concurrentobj.stcmethod()
        self.assertEqual(task.get(), 2)

    def test_undecorated_results(self):
        """Process Concurrent undecorated results are produced."""
        task = thread.concurrent(target=undecorated_simple)
        self.assertEqual(task.get(), 0)

    def test_undecorated_results_arguments(self):
        """Process Concurrent undecorated with args results are produced."""
        task = thread.concurrent(target=undecorated, args=[1],
                                 kwargs={'keyword_argument': 1})
        self.assertEqual(task.get(), 2)

    def test_decorated_results(self):
        """Thread Concurrent results are produced."""
        task = decorated(1, 1)
        self.assertEqual(task.get(), 2)

    def test_decorated_results_callback(self):
        """Thread Concurrent results are forwarded to the callback."""
        decorated_callback(1, 1)
        event.wait()
        self.assertEqual(results, 2)

    def test_error_decorated(self):
        """Thread Concurrent errors are raised by task get."""
        task = error_decorated()
        self.assertRaises(Exception, task.get)

    def test_error_decorated_callback(self):
        """Thread Concurrent errors are forwarded to callback."""
        error_decorated_callback()
        event.wait()
        self.assertTrue(isinstance(exception, Exception))

    def test_cancel_decorated(self):
        """Thread Concurrent task raises TaskCancelled if so."""
        task = long_decorated()
        task.cancel()
        self.assertRaises(TaskCancelled, task.get)

    def test_cancel_decorated_callback(self):
        """Thread Concurrent TaskCancelled is forwarded to callback."""
        task = long_decorated_callback()
        task.cancel()
        event.wait()
        self.assertTrue(isinstance(exception, TaskCancelled))

    def test_undecorated_callback(self):
        """Thread Concurrent undecorated results are forwarded to callback."""
        task = thread.concurrent(target=undecorated, args=[1],
                                 kwargs={'keyword_argument': 1},
                                 callback=self.callback)
        event.wait()
        self.assertEqual(task.get(), 2)

    def test_undecorated_id(self):
        """Thread Concurrent ID is forwarded to it."""
        task = thread.concurrent(target=undecorated, args=[1],
                                 kwargs={'keyword_argument': 1},
                                 identifier='foo')
        self.assertEqual(task.id, 'foo')
