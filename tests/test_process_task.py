import time
import unittest
import threading

from pebble import process, TaskCancelled, TimeoutError


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


def undecorated(argument, keyword_argument=0):
    """A docstring."""
    return argument + keyword_argument


@process.task
def decorated(argument, keyword_argument=0):
    """A docstring."""
    return argument + keyword_argument


@process.task
def error_decorated():
    raise Exception("BOOM!")


@process.task
def long_decorated():
    time.sleep(1)


@process.task(timeout=0.2)
def timeout_decorated():
    time.sleep(1)


@process.task(callback=callback)
def decorated_callback(argument, keyword_argument=0):
    """A docstring."""
    return argument + keyword_argument


@process.task(callback=callback)
def error_decorated_callback():
    raise Exception("BOOM!")


@process.task(callback=callback)
def long_decorated_callback():
    time.sleep(1)


@process.task(callback=callback, timeout=0.2)
def timeout_decorated_callback():
    time.sleep(1)


class TestProcessTaskObj(object):
    a = 0

    def __init__(self):
        self.b = 1

    @classmethod
    @process.task
    def clsmethod(cls):
        return cls.a

    @process.task
    def instmethod(self):
        return self.b

    @staticmethod
    @process.task
    def stcmethod():
        return 2


class TestProcessTask(unittest.TestCase):
    def setUp(self):
        global results
        global exception
        results = 0
        exception = None
        event.clear()
        self.results = 0
        self.taskobj = TestProcessTaskObj()

    def callback(self, task):
        self.results = task.get()
        event.set()

    def test_docstring(self):
        """Process Task docstring is preserved."""
        self.assertEqual(decorated.__doc__, "A docstring.")

    def test_wrong_parameters(self):
        """Process Task raises ValueError if wrong params."""
        self.assertRaises(ValueError, process.task, undecorated,
                          args=[1])

    def test_process_wrong_decoration(self):
        """Process Task raises ValueError if given wrong params."""
        try:
            @process.task(5, name='foo')
            def wrong():
                return
        except Exception as error:
            self.assertTrue(isinstance(error, ValueError))

    def test_class_method(self):
        """Process Task decorated classmethods."""
        task = TestProcessTaskObj.clsmethod()
        self.assertEqual(task.get(), 0)

    def test_instance_method(self):
        """Process Task decorated instance methods."""
        task = self.taskobj.instmethod()
        self.assertEqual(task.get(), 1)

    def test_static_method(self):
        """Process Task decorated static methods."""
        task = self.taskobj.stcmethod()
        self.assertEqual(task.get(), 2)

    def test_undecorated_results(self):
        """Process Task undecorated results are produced."""
        task = process.task(target=undecorated, args=[1],
                            kwargs={'keyword_argument': 1})
        self.assertEqual(task.get(), 2)

    def test_decorated_results(self):
        """Process Task results are produced."""
        task = decorated(1, 1)
        self.assertEqual(task.get(), 2)

    def test_decorated_results_callback(self):
        """Process Task results are forwarded to the callback."""
        decorated_callback(1, 1)
        event.wait()
        self.assertEqual(results, 2)

    def test_error_decorated(self):
        """Process Task errors are raised by task get."""
        task = error_decorated()
        self.assertRaises(Exception, task.get)

    def test_error_decorated_callback(self):
        """Process Task errors are forwarded to callback."""
        error_decorated_callback()
        event.wait()
        self.assertTrue(isinstance(exception, Exception))

    def test_cancel_decorated(self):
        """Process Task task raises TaskCancelled if so."""
        task = long_decorated()
        task.cancel()
        self.assertRaises(TaskCancelled, task.get)

    def test_cancel_decorated_callback(self):
        """Process Task TaskCancelled is forwarded to callback."""
        task = long_decorated_callback()
        task.cancel()
        event.wait()
        self.assertTrue(isinstance(exception, TaskCancelled))

    def test_undecorated_callback(self):
        """Process Task undecorated results are forwarded to the callback."""
        task = process.task(target=undecorated, args=[1],
                            kwargs={'keyword_argument': 1},
                            callback=self.callback)
        event.wait()
        self.assertEqual(task.get(), 2)

    def test_timeout_decorated(self):
        """Process Task task raises TimeoutError if so."""
        task = timeout_decorated()
        self.assertRaises(TimeoutError, task.get)

    def test_timeout_decorated_callback(self):
        """Process Task TimeoutError is forwarded to callback."""
        timeout_decorated_callback()
        event.wait()
        self.assertTrue(isinstance(exception, TimeoutError))
