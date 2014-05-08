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

    event.set()


@process.task(callback=callback)
def function(argument, keyword_argument=0):
    """A docstring."""
    return argument + keyword_argument


@process.task(callback=callback)
def error_function():
    raise Exception("BOOM!")


@process.task(callback=callback)
def long_function():
    time.sleep(1)


@process.task(callback=callback, timeout=0.2)
def timeout_function():
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
        self.taskobj = TestProcessTaskObj()

    def callback(self, task):
        self.results = task.get()
        event.set()

    def test_docstring(self):
        """Process Task docstring is preserved."""
        self.assertEqual(function.__doc__, "A docstring.")

    def test_process_wrong_decoration(self):
        """Process Task raises ValueError if given wrong params."""
        try:
            @process.task(5)
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

    def test_function_results(self):
        """Process Task results are produced."""
        task = function(1, 1)
        self.assertEqual(task.get(), 2)

    def test_function_results_callback(self):
        """Process Task results are forwarded to the callback."""
        function(1, 1)
        event.wait()
        self.assertEqual(results, 2)

    def test_error_function(self):
        """Process Task errors are raised by task get."""
        task = error_function()
        self.assertRaises(Exception, task.get)

    def test_error_function_callback(self):
        """Process Task errors are forwarded to callback."""
        error_function()
        event.wait()
        self.assertTrue(isinstance(exception, Exception))

    def test_cancel_function(self):
        """Process Task task raises TaskCancelled if so."""
        task = long_function()
        task.cancel()
        self.assertRaises(TaskCancelled, task.get)

    def test_cancel_function_callback(self):
        """Process Task TaskCancelled is forwarded to callback."""
        task = long_function()
        task.cancel()
        event.wait()
        self.assertTrue(isinstance(exception, TaskCancelled))

    def test_dynamic_callback(self):
        """Process Task callback can be set dynamically."""
        function.callback = self.callback
        function(1, 1)
        event.wait()
        function.callback = callback
        self.assertEqual(self.results, 2)

    def test_timeout_function(self):
        """Process Task task raises TimeoutError if so."""
        task = timeout_function()
        self.assertRaises(TimeoutError, task.get)

    def test_timeout_function_callback(self):
        """Process Task TimeoutError is forwarded to callback."""
        timeout_function()
        event.wait()
        self.assertTrue(isinstance(exception, TimeoutError))
