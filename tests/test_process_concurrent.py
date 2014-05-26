import os
import unittest
from multiprocessing import Queue

from pebble import process


@process.concurrent(name='foo')
def function(queue, argument, keyword_argument=0):
    """A docstring."""
    queue.put(argument + keyword_argument)


class TestProcessConcurrentObj(object):
    a = 0

    def __init__(self):
        self.b = 1

    @classmethod
    @process.concurrent
    def clsmethod(cls, queue):
        queue.put(cls.a)

    @process.concurrent
    def instmethod(self, queue):
        queue.put(self.b)

    @staticmethod
    @process.concurrent
    def stcmethod(queue):
        queue.put(2)


class TestProcessConcurrent(unittest.TestCase):
    def setUp(self):
        os.name = 'posix'
        self.concurrentobj = TestProcessConcurrentObj()

    def test_docstring(self):
        """Process Concurrent docstring is preserved."""
        self.assertEqual(function.__doc__, "A docstring.")

    def test_process_wrong_decoration(self):
        """Process Concurrent raises ValueError if given wrong params."""
        try:
            @process.concurrent(5)
            def wrong():
                return
        except Exception as error:
            self.assertTrue(isinstance(error, ValueError))

    def test_defaults(self):
        """Process Concurrent default values are preserved."""
        queue = Queue()
        thrd = function(queue, 1, 1)
        thrd.join()
        self.assertFalse(thrd.daemon)

    def test_arguments(self):
        """Process Concurrent decorator arguments are forwarded."""
        queue = Queue()
        thrd = function(queue, 1, 1)
        thrd.join()
        self.assertEqual(thrd.name, 'foo')

    def test_function_results(self):
        """Process Concurrent results are produced."""
        queue = Queue()
        thrd = function(queue, 1, 1)
        results = queue.get()
        thrd.join()
        self.assertEqual(results, 2)

    def test_function_results_windows(self):
        """Process Concurrent results are produced in Windows."""
        os.name = 'nt'
        queue = Queue()
        thrd = function(queue, 1, 1)
        results = queue.get()
        thrd.join()
        self.assertEqual(results, 2)

    def test_class_method(self):
        """Process Concurrent decorated classmethods."""
        queue = Queue()
        thrd = TestProcessConcurrentObj.clsmethod(queue)
        results = queue.get()
        thrd.join()
        self.assertEqual(results, 0)

    def test_instance_method(self):
        """Process Concurrent decorated instance methods."""
        queue = Queue()
        thrd = self.concurrentobj.instmethod(queue)
        results = queue.get()
        thrd.join()
        self.assertEqual(results, 1)

    def test_static_method(self):
        """Process Concurrent decorated static methods."""
        queue = Queue()
        thrd = self.concurrentobj.stcmethod(queue)
        results = queue.get()
        thrd.join()
        self.assertEqual(results, 2)
