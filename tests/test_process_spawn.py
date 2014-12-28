import os
import unittest
from multiprocessing import Queue

from pebble import process


def undecorated(queue, argument, keyword_argument=0):
    queue.put(argument + keyword_argument)


@process.spawn
def decorated(queue, argument, keyword_argument=0):
    """A docstring."""
    queue.put(argument + keyword_argument)


@process.spawn(name='foo')
def decorated_kword(queue, argument, keyword_argument=0):
    """A docstring."""
    queue.put(argument + keyword_argument)


class TestProcessSpawnObj(object):
    a = 0

    def __init__(self):
        self.b = 1

    @classmethod
    @process.spawn
    def clsmethod(cls, queue):
        queue.put(cls.a)

    @process.spawn
    def instmethod(self, queue):
        queue.put(self.b)

    @staticmethod
    @process.spawn
    def stcmethod(queue):
        queue.put(2)


class TestProcessSpawn(unittest.TestCase):
    def setUp(self):
        self.spawnobj = TestProcessSpawnObj()

    def test_docstring(self):
        """Process Spawn docstring is preserved."""
        self.assertEqual(decorated.__doc__, "A docstring.")

    def test_wrong_parameters(self):
        """Process Spawn raises ValueError if wrong params."""
        self.assertRaises(ValueError, process.spawn, undecorated,
                          args=[1])

    def test_wrong_decoration(self):
        """Process Spawn decorator raises ValueError if wrong params."""
        try:
            @process.spawn(5, name='foo')
            def wrong():
                return
        except Exception as error:
            self.assertTrue(isinstance(error, ValueError))

    def test_undecorated_results(self):
        """Process Spawn undecorated results are produced."""
        queue = Queue()
        proc = process.spawn(target=decorated_kword, args=[queue, 1])
        results = queue.get()
        proc.join()
        self.assertEqual(results, 1)

    def test_undecorated_keyworkd_results(self):
        """Process Spawn undecorated with keyword, results are produced."""
        queue = Queue()
        proc = process.spawn(target=decorated_kword, args=[queue, 1],
                             kwargs={'keyword_argument': 1})
        results = queue.get()
        proc.join()
        self.assertEqual(results, 2)

    def test_defaults(self):
        """Process Spawn default values are preserved."""
        queue = Queue()
        proc = decorated(queue, 1, 1)
        proc.join()
        self.assertFalse(proc.daemon)

    def test_arguments(self):
        """Process Spawn decorator arguments are forwarded."""
        queue = Queue()
        proc = decorated_kword(queue, 1, 1)
        proc.join()
        self.assertEqual(proc.name, 'foo')

    def test_decorated_results(self):
        """Process Spawn results are produced."""
        queue = Queue()
        proc = decorated(queue, 1, 1)
        results = queue.get()
        proc.join()
        self.assertEqual(results, 2)

    def test_class_method(self):
        """Process Spawn decorated_kword classmethods."""
        queue = Queue()
        proc = TestProcessSpawnObj.clsmethod(queue)
        results = queue.get()
        proc.join()
        self.assertEqual(results, 0)

    def test_instance_method(self):
        """Process Spawn decorated_kword instance methods."""
        queue = Queue()
        proc = self.spawnobj.instmethod(queue)
        results = queue.get()
        proc.join()
        self.assertEqual(results, 1)

    def test_static_method(self):
        """Process Spawn decorated_kword static methods."""
        if os.name != 'nt':
            queue = Queue()
            proc = self.spawnobj.stcmethod(queue)
            results = queue.get()
            proc.join()
            self.assertEqual(results, 2)
