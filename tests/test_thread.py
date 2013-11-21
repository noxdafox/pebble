import time
import unittest
from multiprocessing import TimeoutError

from pebble import thread, TimeoutError


_results = 0
_exception = None


def callback(task):
    global _results
    _results = task.get()


def error_callback(task):
    global _exception
    _exception = task.get()


@thread
def ajob(argument, keyword_argument=0):
    return argument + keyword_argument


@thread
def ajob_error(argument, keyword_argument=0):
    raise Exception("BOOM!")


@thread(callback=callback)
def ajob_callback(argument, keyword_argument=0):
    return argument + keyword_argument


@thread
def ajob_long():
    time.sleep(1)
    return 1


@thread
def ajob_count():
    return None


class TestThreadDecorators(unittest.TestCase):
    def setUp(self):
        self.exception = None
        self.callback_results = 0

    def callback(self, task):
        self.callback_results = task.get()

    def error_callback(self, task):
        try:
            task.get()
        except Exception as error:
            self.exception = error

    def test_thread_wrong_decoration(self):
        """Decorator raises ValueError if given wrong params."""
        try:
            @thread(callback)
            def wrong(argument, keyword_argument=0):
                return argument + keyword_argument
        except Exception as error:
            self.assertTrue(isinstance(error, ValueError))

    def test_thread_task(self):
        """Test that an thread task is correctly executed."""
        task = ajob(1, 1)
        self.assertEqual(task.get(), 2)

    def test_thread_task_callback_static(self):
        """Test static callback is executed with thread task."""
        task = ajob_callback(1, 1)
        task.get()
        self.assertEqual(2, _results)

    def test_thread_task_callback_dynamic(self):
        """Test dynamic callback is executed with thread task."""
        ajob.callback = self.callback
        task = ajob(1, 1)
        task.get()
        self.assertEqual(2, self.callback_results)

    def test_thread_task_error(self):
        """Test that an exception in an asynch task is raised by get()."""
        task = ajob_error(1, 1)
        try:
            task.get()
        except:
            pass
        self.assertRaises(Exception, task.get)

    def test_thread_task_error_callback(self):
        """Test that an exception in a task is managed in error_callback."""
        ajob_error.callback = self.error_callback
        task = ajob_error(1, 1)
        try:
            task.get()
        except:
            pass
        self.assertEqual('BOOM!', str(self.exception))

    def test_thread_task_long(self):
        """Test timeout get parameter works."""
        task = ajob_long()
        self.assertEqual(task.get(2), 1)

    def test_thread_timeout_error(self):
        """TimeoutError is raised if task has not yet finished."""
        task = ajob_long()
        self.assertRaises(TimeoutError, task.get, 0)

    def test_thread_task_number(self):
        """Task number are correctly assigned."""
        for i in range(0, 5):
            task = ajob_count(1, 1)
        self.assertEqual(task.number, 4)
