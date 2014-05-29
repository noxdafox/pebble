import os
import unittest
from multiprocessing import Queue

from pebble import process


def undecorated(queue, argument, keyword_argument=0):
    queue.put(argument + keyword_argument)


@process.concurrent(name='foo')
def decorated(queue, argument, keyword_argument=0):
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
        self.assertEqual(decorated.__doc__, "A docstring.")

    def test_wrong_parameters(self):
        """Process Concurrent raises ValueError if wrong params."""
        self.assertRaises(ValueError, process.concurrent, undecorated,
                          args=[1])

    def test_wrong_decoration(self):
        """Process Concurrent decorator raises ValueError if wrong params."""
        try:
            @process.concurrent(5, name='foo')
            def wrong():
                return
        except Exception as error:
            self.assertTrue(isinstance(error, ValueError))

    def test_undecorated_results(self):
        """Process Concurrent undecorated results are produced."""
        queue = Queue()
        proc = process.concurrent(target=decorated, args=[queue, 1],
                                  kwargs={'keyword_argument': 1})
        results = queue.get()
        proc.join()
        self.assertEqual(results, 2)

    def test_defaults(self):
        """Process Concurrent default values are preserved."""
        queue = Queue()
        proc = decorated(queue, 1, 1)
        proc.join()
        self.assertFalse(proc.daemon)

    def test_arguments(self):
        """Process Concurrent decorator arguments are forwarded."""
        queue = Queue()
        proc = decorated(queue, 1, 1)
        proc.join()
        self.assertEqual(proc.name, 'foo')

    def test_decorated_results(self):
        """Process Concurrent results are produced."""
        queue = Queue()
        proc = decorated(queue, 1, 1)
        results = queue.get()
        proc.join()
        self.assertEqual(results, 2)

    def test_decorated_results_windows(self):
        """Process Concurrent results are produced in Windows."""
        os.name = 'nt'
        queue = Queue()
        proc = decorated(queue, 1, 1)
        results = queue.get()
        proc.join()
        self.assertEqual(results, 2)

    def test_class_method(self):
        """Process Concurrent decorated classmethods."""
        queue = Queue()
        proc = TestProcessConcurrentObj.clsmethod(queue)
        results = queue.get()
        proc.join()
        self.assertEqual(results, 0)

    def test_instance_method(self):
        """Process Concurrent decorated instance methods."""
        queue = Queue()
        proc = self.concurrentobj.instmethod(queue)
        results = queue.get()
        proc.join()
        self.assertEqual(results, 1)

    def test_static_method(self):
        """Process Concurrent decorated static methods."""
        queue = Queue()
        proc = self.concurrentobj.stcmethod(queue)
        results = queue.get()
        proc.join()
        self.assertEqual(results, 2)
