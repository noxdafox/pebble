import unittest

from pebble import process


@process.task
def function(argument, keyword_argument=0):
    """A docstring."""
    return argument + keyword_argument


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
        self.taskobj = TestProcessTaskObj()

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

    def test_function_results(self):
        """Process Task results are produced."""
        task = function(1, 1)
        self.assertEqual(task.get(), 2)

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
