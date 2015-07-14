import os
import time
import signal
import unittest
import threading

from pebble import process, TaskCancelled, TimeoutError, ProcessExpired


event = threading.Event()
initarg = 0
results = 0
exception = None


def callback(concurrent):
    global results
    global exception

    try:
        results = concurrent.get()
    except Exception as error:
        exception = error
    finally:
        event.set()


def undecorated_simple():
    return 0


def undecorated(argument, keyword_argument=0):
    """A docstring."""
    return argument + keyword_argument


@process.concurrent
def decorated(argument, keyword_argument=0):
    """A docstring."""
    return argument + keyword_argument


@process.concurrent
def error_decorated():
    raise Exception("BOOM!")


@process.concurrent
def critical_decorated():
    os._exit(123)


@process.concurrent
def long_decorated():
    time.sleep(1)


@process.concurrent(timeout=0.2)
def timeout_decorated():
    time.sleep(1)


@process.concurrent(callback=callback)
def decorated_callback(argument, keyword_argument=0):
    """A docstring."""
    return argument + keyword_argument


@process.concurrent(callback=callback)
def error_decorated_callback():
    raise Exception("BOOM!")


@process.concurrent(callback=callback)
def long_decorated_callback():
    time.sleep(1)


@process.concurrent(callback=callback, timeout=0.2)
def timeout_decorated_callback():
    time.sleep(1)


@process.concurrent(timeout=0.2)
def sigterm_decorated():
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    time.sleep(100)


class TestProcessConcurrentObj(object):
    a = 0

    def __init__(self):
        self.b = 1

    @classmethod
    @process.concurrent
    def clsmethod(cls):
        return cls.a

    @process.concurrent
    def instmethod(self):
        return self.b

    @staticmethod
    @process.concurrent
    def stcmethod():
        return 2


class TestProcessConcurrent(unittest.TestCase):
    def setUp(self):
        global results
        global exception
        results = 0
        exception = None
        event.clear()
        self.results = 0
        self.concurrentobj = TestProcessConcurrentObj()

    def callback(self, task):
        self.results = task.get()
        event.set()

    def test_docstring(self):
        """Process Concurrent docstring is preserved."""
        self.assertEqual(decorated.__doc__, "A docstring.")

    def test_wrong_parameters(self):
        """Process Concurrent raises ValueError if wrong params."""
        self.assertRaises(ValueError, process.concurrent, undecorated,
                          args=[1])

    def test_process_wrong_decoration(self):
        """Process Concurrent raises ValueError if given wrong params."""
        try:
            @process.concurrent(5, name='foo')
            def wrong():
                return
        except Exception as error:
            self.assertTrue(isinstance(error, ValueError))

    def test_class_method(self):
        """Process Concurrent decorated classmethods."""
        task = TestProcessConcurrentObj.clsmethod()
        self.assertEqual(task.get(), 0)

    def test_instance_method(self):
        """Process Concurrent decorated instance methods."""
        task = self.concurrentobj.instmethod()
        self.assertEqual(task.get(), 1)

    @unittest.skipIf(os.name == 'nt', "Test won't run on Windows.")
    def test_static_method(self):
        """Process Concurrent decorated static methods (Unix only)."""
        task = self.concurrentobj.stcmethod()
        self.assertEqual(task.get(), 2)

    def test_undecorated_results(self):
        """Process Concurrent undecorated results are produced."""
        task = process.concurrent(target=undecorated_simple)
        self.assertEqual(task.get(), 0)

    def test_undecorated_results_arguments(self):
        """Process Concurrent undecorated with args results are produced."""
        task = process.concurrent(target=undecorated, args=[1],
                                  kwargs={'keyword_argument': 1})
        self.assertEqual(task.get(), 2)

    def test_undecorated_started(self):
        """Process Concurrent undecorated task is set to started."""
        task = process.concurrent(target=undecorated, args=[1],
                                  kwargs={'keyword_argument': 1})
        self.assertTrue(task.started)

    def test_decorated_results(self):
        """Process Concurrent results are produced."""
        task = decorated(1, 1)
        self.assertEqual(task.get(), 2)

    def test_decorated_results_callback(self):
        """Process Concurrent results are forwarded to the callback."""
        decorated_callback(1, 1)
        event.wait()
        self.assertEqual(results, 2)

    def test_error_decorated(self):
        """Process Concurrent errors are raised by concurrent get."""
        task = error_decorated()
        self.assertRaises(Exception, task.get)

    def test_error_decorated_callback(self):
        """Process Concurrent errors are forwarded to callback."""
        error_decorated_callback()
        event.wait()
        self.assertTrue(isinstance(exception, Exception))

    def test_cancel_decorated(self):
        """Process Concurrent concurrent raises ConcurrentCancelled if so."""
        task = long_decorated()
        task.cancel()
        self.assertRaises(TaskCancelled, task.get)

    def test_cancel_decorated_callback(self):
        """Process Concurrent TaskCancelled is forwarded to callback."""
        task = long_decorated_callback()
        task.cancel()
        event.wait()
        self.assertTrue(isinstance(exception, TaskCancelled))

    def test_undecorated_callback(self):
        """Process Concurrent undecorated results are forwarded to callback."""
        task = process.concurrent(target=undecorated, args=[1],
                                  kwargs={'keyword_argument': 1},
                                  callback=self.callback)
        event.wait()
        self.assertEqual(task.get(), 2)

    def test_timeout_decorated(self):
        """Process Concurrent concurrent raises TimeoutError if so."""
        task = timeout_decorated()
        self.assertRaises(TimeoutError, task.get)

    def test_timeout_decorated_callback(self):
        """Process Concurrent TimeoutError is forwarded to callback."""
        timeout_decorated_callback()
        event.wait()
        self.assertTrue(isinstance(exception, TimeoutError))

    def test_undecorated_id(self):
        """Process concurrent ID is forwarded to it."""
        task = process.concurrent(target=undecorated, args=[1],
                                  kwargs={'keyword_argument': 1},
                                  identifier='foo')
        self.assertEqual(task.id, 'foo')

    def test_decorated_dead_process(self):
        """Process Concurrent ProcessExpired is raised if process dies."""
        task = critical_decorated()
        self.assertRaises(ProcessExpired, task.get)

    @unittest.skipIf(os.name == 'nt', "Test won't run on Windows.")
    def test_decorated_pool_ignoring_sigterm(self):
        """Process Concurrent ignored SIGTERM signal are handled on Unix."""
        task = sigterm_decorated()
        self.assertRaises(TimeoutError, task.get)
