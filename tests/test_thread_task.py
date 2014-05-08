import unittest

from pebble import thread


@thread.task
def function(argument, keyword_argument=0):
    """A docstring."""
    return argument + keyword_argument


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
        self.taskobj = TestThreadTaskObj()

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

    def test_function_results(self):
        """Thread Task results are produced."""
        task = function(1, 1)
        self.assertEqual(task.get(), 2)

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
