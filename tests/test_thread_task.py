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

    event.set()


@thread.task(callback=callback)
def function(argument, keyword_argument=0):
    """A docstring."""
    return argument + keyword_argument


@thread.task(callback=callback)
def error_function():
    raise Exception("BOOM!")


@thread.task(callback=callback)
def long_function():
    time.sleep(1)


class TestThreadTaskObj(object):
    a = 0

    def __init__(self):
        self.b = 1

    @classmethod
    @thread.task
    def clsmethod(cls):
        return cls.a

    @thread.task
    def instmethod(self):
        return self.b

    @staticmethod
    @thread.task
    def stcmethod():
        return 2


class TestThreadTask(unittest.TestCase):
    def setUp(self):
        global results
        global exception
        results = 0
        exception = None
        event.clear()
        self.taskobj = TestThreadTaskObj()

    def callback(self, task):
        self.results = task.get()
        event.set()

    def test_docstring(self):
        """Thread Task docstring is preserved."""
        self.assertEqual(function.__doc__, "A docstring.")

    def test_thread_wrong_decoration(self):
        """Thread Task raises ValueError if given wrong params."""
        try:
            @thread.task(5)
            def wrong():
                return
        except Exception as error:
            self.assertTrue(isinstance(error, ValueError))

    def test_class_method(self):
        """Thread Task decorated classmethods."""
        task = TestThreadTaskObj.clsmethod()
        self.assertEqual(task.get(), 0)

    def test_instance_method(self):
        """Thread Task decorated instance methods."""
        task = self.taskobj.instmethod()
        self.assertEqual(task.get(), 1)

    def test_static_method(self):
        """Thread Task decorated static methods."""
        task = self.taskobj.stcmethod()
        self.assertEqual(task.get(), 2)

    def test_function_results(self):
        """Thread Task results are produced."""
        task = function(1, 1)
        self.assertEqual(task.get(), 2)

    def test_function_results_callback(self):
        """Thread Task results are forwarded to the callback."""
        function(1, 1)
        event.wait()
        self.assertEqual(results, 2)

    def test_error_function(self):
        """Thread Task errors are raised by task get."""
        task = error_function()
        self.assertRaises(Exception, task.get)

    def test_error_function_callback(self):
        """Thread Task errors are forwarded to callback."""
        error_function()
        event.wait()
        self.assertTrue(isinstance(exception, Exception))

    def test_cancel_function(self):
        """Thread Task task raises TaskCancelled if so."""
        task = long_function()
        task.cancel()
        self.assertRaises(TaskCancelled, task.get)

    def test_cancel_function_callback(self):
        """Thread Task TaskCancelled is forwarded to callback."""
        task = long_function()
        task.cancel()
        event.wait()
        self.assertTrue(isinstance(exception, TaskCancelled))

    def test_dynamic_callback(self):
        """Thread Task callback can be set dynamically."""
        function.callback = self.callback
        function(1, 1)
        event.wait()
        function.callback = callback
        self.assertEqual(self.results, 2)
