import os
import unittest
try:  # Python 2
    from Queue import Queue
except:  # Python 3
    from queue import Queue

from pebble import thread


def undecorated(queue, argument, keyword_argument=0):
    queue.put(argument + keyword_argument)


@thread.spawn(name='foo')
def decorated(queue, argument, keyword_argument=0):
    """A docstring."""
    queue.put(argument + keyword_argument)


class TestThreadSpawnObj(object):
    a = 0

    def __init__(self):
        self.b = 1

    @classmethod
    @thread.spawn
    def clsmethod(cls, queue):
        queue.put(cls.a)

    @thread.spawn
    def instmethod(self, queue):
        queue.put(self.b)

    @staticmethod
    @thread.spawn
    def stcmethod(queue):
        queue.put(2)


class TestThreadSpawn(unittest.TestCase):
    def setUp(self):
        self.spawnobj = TestThreadSpawnObj()

    def test_docstring(self):
        """Thread Spawn docstring is preserved."""
        self.assertEqual(decorated.__doc__, "A docstring.")

    def test_wrong_parameters(self):
        """Thread Spawn raises ValueError if wrong params."""
        self.assertRaises(ValueError, thread.spawn, undecorated,
                          args=[1])

    def test_thread_wrong_decoration(self):
        """Thread Spawn raises ValueError if given wrong params."""
        try:
            @thread.spawn(5, name='foo')
            def wrong():
                return
        except Exception as error:
            self.assertTrue(isinstance(error, ValueError))

    def test_defaults(self):
        """Thread Spawn default values are preserved."""
        queue = Queue()
        thrd = decorated(queue, 1, 1)
        thrd.join()
        self.assertFalse(thrd.daemon)

    def test_arguments(self):
        """Thread Spawn decorator arguments are forwarded."""
        queue = Queue()
        thrd = decorated(queue, 1, 1)
        thrd.join()
        self.assertEqual(thrd.name, 'foo')

    def test_undecorated_results(self):
        """Thread Spawn undecorated results are produced."""
        queue = Queue()
        thrd = thread.spawn(target=decorated, args=[queue, 1],
                            kwargs={'keyword_argument': 1})
        results = queue.get()
        thrd.join()
        self.assertEqual(results, 2)

    def test_decorated_results(self):
        """Thread Spawn results are produced."""
        queue = Queue()
        thrd = decorated(queue, 1, 1)
        results = queue.get()
        thrd.join()
        self.assertEqual(results, 2)

    def test_class_method(self):
        """Thread Spawn decorated classmethods."""
        queue = Queue()
        thrd = TestThreadSpawnObj.clsmethod(queue)
        results = queue.get()
        thrd.join()
        self.assertEqual(results, 0)

    def test_instance_method(self):
        """Thread Spawn decorated instance methods."""
        queue = Queue()
        thrd = self.spawnobj.instmethod(queue)
        results = queue.get()
        thrd.join()
        self.assertEqual(results, 1)

    def test_static_method(self):
        """Thread Spawn decorated static methods."""
        if os.name != 'nt':
            queue = Queue()
            thrd = self.spawnobj.stcmethod(queue)
            results = queue.get()
            thrd.join()
            self.assertEqual(results, 2)
