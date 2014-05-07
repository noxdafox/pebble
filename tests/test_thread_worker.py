import unittest
try:  # Python 2
    from Queue import Queue
except:  # Python 3
    from queue import Queue

from pebble import thread


@thread.worker(name='foo')
def function(queue, argument, keyword_argument=0):
    """A docstring."""
    queue.put(argument + keyword_argument)


class TestThreadWorkerObj(object):
    a = 0

    def __init__(self):
        self.b = 1

    @classmethod
    @thread.worker
    def clsmethod(cls, queue):
        queue.put(cls.a)

    @thread.worker
    def instmethod(self, queue):
        queue.put(self.a)

    @staticmethod
    @thread.worker
    def stcmethod(queue):
        queue.put(2)


class TestThreadWorker(unittest.TestCase):
    def setUp(self):
        self.workerobj = TestThreadWorkerObj()

    def test_docstring(self):
        """Thread Worker docstring is preserved."""
        self.assertEqual(function.__doc__, "A docstring.")

    def test_thread_wrong_decoration(self):
        """Thread Worker raises ValueError if given wrong params."""
        try:
            @thread.worker(5)
            def wrong():
                return
        except Exception as error:
            self.assertTrue(isinstance(error, ValueError))

    def test_defaults(self):
        """Thread Worker default values are preserved."""
        queue = Queue()
        thrd = function(queue, 1, 1)
        thrd.join()
        self.assertFalse(thrd.daemon)

    def test_arguments(self):
        """Thread Worker decorator arguments are forwarded."""
        queue = Queue()
        thrd = function(queue, 1, 1)
        thrd.join()
        self.assertEqual(thrd.name, 'foo')

    def test_function_results(self):
        """Thread Worker results are produced."""
        queue = Queue()
        thrd = function(queue, 1, 1)
        results = queue.get()
        thrd.join()
        self.assertEqual(results, 2)

    def test_class_method(self):
        """Thread Worker decorated classmethods."""
        queue = Queue()
        thrd = TestThreadWorkerObj.clsmethod(queue)
        results = queue.get()
        thrd.join()
        self.assertEqual(results, 0)

    def test_instance_method(self):
        """Thread Worker decorated instance methods."""
        queue = Queue()
        thrd = self.workerobj.instmethod(queue)
        results = queue.get()
        thrd.join()
        self.assertEqual(results, 0)

    def test_static_method(self):
        """Thread Worker decorated static methods."""
        queue = Queue()
        thrd = self.workerobj.stcmethod(queue)
        results = queue.get()
        thrd.join()
        self.assertEqual(results, 2)
