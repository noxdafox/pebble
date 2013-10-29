import time
import threading
import unittest

from pebble.pebble import TimeoutError, SerializingError
from pebble.decorators import asynchronous, concurrent


_task_id = ""
_results = 0
_exception = None


class UnserializeableError(Exception):
    def __init__(self):
        self.lock = threading.Lock()  # unpickleable

    def __str__(self):
        return "BOOM!"


def callback(task_id, results):
    global _task_id
    global _results
    _task_id = task_id
    _results = results


def error_callback(task_id, results):
    global _task_id
    global _exception
    _task_id = task_id
    _exception = results


@asynchronous
def ajob(argument, keyword_argument=0):
    return argument + keyword_argument


@asynchronous
def ajob_error(argument, keyword_argument=0):
    raise Exception("BOOM!")


@asynchronous(callback=callback)
def ajob_callback(argument, keyword_argument=0):
    return argument + keyword_argument


@asynchronous
def ajob_long():
    time.sleep(1)
    return 1


@asynchronous
def ajob_count():
    return None


@concurrent
def cjob(argument, keyword_argument=0):
    return argument + keyword_argument


@concurrent
def cjob_error(argument, keyword_argument=0):
    raise Exception("BOOM!")


@concurrent(callback=callback)
def cjob_callback(argument, keyword_argument=0):
    return argument + keyword_argument


@concurrent
def cjob_long():
    time.sleep(1)
    return 1


@concurrent
def cjob_count():
    return None


@concurrent(timeout=1)
def cjob_timeout():
    time.sleep(2)
    return 1


@concurrent
def cjob_unserializeable():
    return threading.Lock()


@concurrent
def cjob_unserializeable_error():
    raise UnserializeableError()


class TestPebbleDecorators(unittest.TestCase):
    def setUp(self):
        global _task_id
        global _results
        global _exception
        _task_id = ""
        _results = 0
        _exception = None
        self.task_id = ''
        self.exception = None
        self.callback_results = 0

    def callback(self, task_id, results):
        self.task_id = task_id
        self.callback_results = results

    def error_callback(self, task_id, exception):
        self.task_id = task_id
        self.exception = exception

    def test_asynchronous_wrong_decoration(self):
        """Decorator raises ValueError if given wrong params."""
        try:
            @asynchronous(callback, error_callback)
            def wrong(argument, keyword_argument=0):
                return argument + keyword_argument
        except Exception as error:
            self.assertTrue(isinstance(error, ValueError))

    def test_asynchronous_task(self):
        """Test that an asynchronous task is correctly executed."""
        task = ajob(1, 1)
        self.assertEqual(task.get(), 2)

    def test_asynchronous_task_callback_static(self):
        """Test static callback is executed with asynchronous task."""
        task = ajob_callback(1, 1)
        task.get()
        self.assertEqual((task.id, 2), (_task_id, _results))

    def test_asynchronous_task_callback_dynamic(self):
        """Test dynamic callback is executed with asynchronous task."""
        ajob.callback = self.callback
        task = ajob(1, 1)
        task.get()
        self.assertEqual((task.id, 2), (self.task_id, self.callback_results))

    def test_asynchronous_task_error(self):
        """Test that an exception in an asynch task is raised by get()."""
        task = ajob_error(1, 1)
        try:
            task.get()
        except:
            pass
        self.assertRaises(Exception, task.get)

    def test_asynchronous_task_error_callback(self):
        """Test that an exception in a task is managed in error_callback."""
        ajob_error.error_callback = self.error_callback
        task = ajob_error(1, 1)
        try:
            task.get()
        except:
            pass
        self.assertEqual((task.id, 'BOOM!'),
                         (self.task_id, str(self.exception)))

    def test_asynchronous_task_long(self):
        """Test timeout get parameter works."""
        task = ajob_long()
        self.assertEqual(task.get(2), 1)

    def test_asynchronous_timeout_error(self):
        """TimeoutError is raised if task has not yet finished."""
        task = ajob_long()
        self.assertRaises(TimeoutError, task.get, 0)

    def test_asynchronous_task_number(self):
        """Task number are correctly assigned."""
        for i in range(0, 5):
            task = ajob_count(1, 1)
        self.assertEqual(task.number, 4)

    def test_concurrent_wrong_decoration(self):
        """Decorator raises ValueError if given wrong params."""
        try:
            @concurrent(callback, error_callback)
            def wrong(argument, keyword_argument=0):
                return argument + keyword_argument
        except Exception as error:
            self.assertTrue(isinstance(error, ValueError))

    def test_concurrent_task(self):
        """Test that a concurrent task is correctly executed."""
        task = cjob(1, 1)
        self.assertEqual(task.get(), 2)

    def test_concurrent_task_callback_static(self):
        """Test static callback is executed with concurrent task."""
        task = cjob_callback(1, 1)
        task.get()
        self.assertEqual((task.id, 2), (_task_id, _results))

    def test_concurrent_task_callback_dynamic(self):
        """Test dynamic callback is executed with concurrent task."""
        cjob.callback = self.callback
        task = cjob(1, 1)
        task.get()
        self.assertEqual((task.id, 2), (self.task_id, self.callback_results))

    def test_concurrent_task_error(self):
        """Test that an exception in a concurrent task is raised by get()."""
        task = cjob_error(1, 1)
        self.assertRaises(Exception, task.get)

    def test_concurrent_task_error_callback(self):
        """Test that an exception in a task is managed in error_callback."""
        cjob_error.error_callback = self.error_callback
        task = cjob_error(1, 1)
        try:
            task.get()
        except:
            pass
        self.assertEqual((task.id, 'BOOM!'),
                         (self.task_id, str(self.exception)))

    def test_concurrent_task_long(self):
        """Test timeout get parameter works."""
        task = cjob_long()
        self.assertEqual(task.get(2), 1)

    def test_concurrent_timeout_error(self):
        """TimeoutError is raised if task has not yet finished."""
        task = cjob_long()
        self.assertRaises(TimeoutError, task.get, 0)

    def test_concurrent_task_number(self):
        """Task number are correctly assigned."""
        for i in range(0, 5):
            task = cjob_count(1, 1)
        self.assertEqual(task.number, 4)

    def test_concurrent_timeout(self):
        """Timeout decorator kills the task."""
        task = cjob_timeout()
        self.assertFalse(task.get(), None)

    def test_concurrent_timeout_not_expired(self):
        """Timeout decorator doesn't kill the task."""
        cjob_timeout.timeout = 5
        task = cjob_timeout()
        self.assertEqual(task.get(), 1)

    def test_concurrent_unserializable(self):
        """PicklingError is returned if results are not serializeable."""
        task = cjob_unserializeable()
        try:  # Python 2
            from cPickle import PicklingError
            self.assertRaises(PicklingError, task.get)
        except ImportError:  # Python 3
            from pickle import PicklingError
            self.assertRaises(PicklingError, task.get)

    def test_concurrent_unserializable_error(self):
        """SerializingError is returned if exception is not serializeable."""
        task = cjob_unserializeable_error()
        self.assertRaises(SerializingError, task.get)


if __name__ == "__main__":
    unittest.main()
