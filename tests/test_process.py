import time
import threading
import unittest
from multiprocessing import TimeoutError

from pebble import SerializingError, process


_results = 0
_exception = None


class UnserializeableError(Exception):
    def __init__(self):
        self.lock = threading.Lock()  # unpickleable

    def __str__(self):
        return "BOOM!"


def callback(task):
    global _results
    _results = task.get()


def error_callback(task):
    global _exception
    _exception = task.get()


@process
def cjob(argument, keyword_argument=0):
    return argument + keyword_argument


@process
def cjob_error(argument, keyword_argument=0):
    raise Exception("BOOM!")


@process(callback=callback)
def cjob_callback(argument, keyword_argument=0):
    return argument + keyword_argument


@process
def cjob_long():
    time.sleep(1)
    return 1


@process
def cjob_count():
    return None


@process(timeout=1)
def cjob_timeout():
    time.sleep(2)
    return 1


@process
def cjob_unserializeable():
    return threading.Lock()


@process
def cjob_unserializeable_error():
    raise UnserializeableError()


class TestProcessDecorator(unittest.TestCase):
    def setUp(self):
        global _results
        global _exception
        _results = 0
        _exception = None
        self.task_id = ''
        self.exception = None
        self.callback_results = 0

    def callback(self, task):
        self.callback_results = task.get()

    def error_callback(self, task):
        try:
            task.get()
        except Exception as error:
            self.exception = error

    def test_process_wrong_decoration(self):
        """Decorator raises ValueError if given wrong params."""
        try:
            @process(callback, error_callback)
            def wrong(argument, keyword_argument=0):
                return argument + keyword_argument
        except Exception as error:
            self.assertTrue(isinstance(error, ValueError))

    def test_process_task(self):
        """Test that a process task is correctly executed."""
        task = cjob(1, 1)
        self.assertEqual(task.get(), 2)

    def test_process_task_callback_static(self):
        """Test static callback is executed with process task."""
        task = cjob_callback(1, 1)
        task.get()
        self.assertEqual(2, _results)

    def test_process_task_callback_dynamic(self):
        """Test dynamic callback is executed with process task."""
        cjob.callback = self.callback
        task = cjob(1, 1)
        task.get()
        self.assertEqual(2, self.callback_results)

    def test_process_task_error(self):
        """Test that an exception in a process task is raised by get()."""
        task = cjob_error(1, 1)
        self.assertRaises(Exception, task.get)

    def test_process_task_error_callback(self):
        """Test that an exception in a task is managed in error_callback."""
        cjob_error.callback = self.error_callback
        task = cjob_error(1, 1)
        try:
            task.get()
        except:
            pass
        self.assertEqual('BOOM!', str(self.exception))

    def test_process_task_long(self):
        """Test timeout get parameter works."""
        task = cjob_long()
        self.assertEqual(task.get(2), 1)

    def test_process_timeout_error(self):
        """TimeoutError is raised if task has not yet finished."""
        task = cjob_long()
        self.assertRaises(TimeoutError, task.get, 0)

    def test_process_task_number(self):
        """Task number are correctly assigned."""
        for i in range(0, 5):
            task = cjob_count(1, 1)
        self.assertEqual(task.number, 4)

    def test_process_timeout(self):
        """Timeout decorator kills the task."""
        task = cjob_timeout()
        self.assertFalse(task.get(), None)

    def test_process_timeout_not_expired(self):
        """Timeout decorator doesn't kill the task."""
        cjob_timeout.timeout = 5
        task = cjob_timeout()
        self.assertEqual(task.get(), 1)

    def test_process_unserializable(self):
        """PicklingError is returned if results are not serializeable."""
        task = cjob_unserializeable()
        try:  # Python 2
            from cPickle import PicklingError
            self.assertRaises(PicklingError, task.get)
        except ImportError:  # Python 3
            from pickle import PicklingError
            self.assertRaises(PicklingError, task.get)

    def test_process_unserializable_error(self):
        """SerializingError is returned if exception is not serializeable."""
        task = cjob_unserializeable_error()
        self.assertRaises(SerializingError, task.get)


if __name__ == "__main__":
    unittest.main()
