import os
import unittest
from multiprocessing import Queue

from pebble import process


@process.worker(name='foo')
def function(queue, argument, keyword_argument=0):
    """A docstring."""
    queue.put(argument + keyword_argument)


class TestProcessWorkerObj(object):
    a = 0

    def __init__(self):
        self.b = 1

    @classmethod
    @process.worker
    def clsmethod(cls, queue):
        queue.put(cls.a)

    @process.worker
    def instmethod(self, queue):
        queue.put(self.a)

    @staticmethod
    @process.worker
    def stcmethod(queue):
        queue.put(2)


class TestProcessWorker(unittest.TestCase):
    def setUp(self):
        os.name = 'posix'
        self.workerobj = TestProcessWorkerObj()

    def test_docstring(self):
        """Process Worker docstring is preserved."""
        self.assertEqual(function.__doc__, "A docstring.")

    def test_process_wrong_decoration(self):
        """Process Worker raises ValueError if given wrong params."""
        try:
            @process.worker(5)
            def wrong():
                return
        except Exception as error:
            self.assertTrue(isinstance(error, ValueError))

    def test_defaults(self):
        """Process Worker default values are preserved."""
        queue = Queue()
        thrd = function(queue, 1, 1)
        thrd.join()
        self.assertFalse(thrd.daemon)

    def test_arguments(self):
        """Process Worker decorator arguments are forwarded."""
        queue = Queue()
        thrd = function(queue, 1, 1)
        thrd.join()
        self.assertEqual(thrd.name, 'foo')

    def test_function_results(self):
        """Process Worker results are produced."""
        queue = Queue()
        thrd = function(queue, 1, 1)
        results = queue.get()
        thrd.join()
        self.assertEqual(results, 2)

    def test_function_results_windows(self):
        """Process Worker results are produced in Windows."""
        os.name = 'nt'
        queue = Queue()
        thrd = function(queue, 1, 1)
        results = queue.get()
        thrd.join()
        self.assertEqual(results, 2)

    def test_class_method(self):
        """Process Worker decorated classmethods."""
        queue = Queue()
        thrd = TestProcessWorkerObj.clsmethod(queue)
        results = queue.get()
        thrd.join()
        self.assertEqual(results, 0)

    def test_instance_method(self):
        """Process Worker decorated instance methods."""
        queue = Queue()
        thrd = self.workerobj.instmethod(queue)
        results = queue.get()
        thrd.join()
        self.assertEqual(results, 0)

    def test_static_method(self):
        """Process Worker decorated static methods."""
        queue = Queue()
        thrd = self.workerobj.stcmethod(queue)
        results = queue.get()
        thrd.join()
        self.assertEqual(results, 2)
