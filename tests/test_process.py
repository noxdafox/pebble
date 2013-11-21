import time
import threading
import unittest

from pebble import SerializingError, TimeoutError, TaskCancelled, process


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
def job(argument, keyword_argument=0):
    return argument + keyword_argument


@process
def job_error(argument, keyword_argument=0):
    raise Exception("BOOM!")


@process(callback=callback)
def job_callback(argument, keyword_argument=0):
    return argument + keyword_argument


@process
def job_long():
    time.sleep(1)
    return 1


@process
def job_count():
    return None


@process(timeout=1)
def job_timeout():
    time.sleep(2)
    return 1


@process(timeout=2)
def job_long_timeout():
    return 1


@process
def job_unserializeable():
    return threading.Lock()


@process
def job_unserializeable_error():
    raise UnserializeableError()


class TestProcessDecorator(unittest.TestCase):
    def setUp(self):
        global _results
        global _exception
        _results = 0
        _exception = None
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
            @process(callback, 5)
            def wrong(argument, keyword_argument=0):
                return argument + keyword_argument
        except Exception as error:
            self.assertTrue(isinstance(error, ValueError))

    def test_process_callback_static(self):
        """Test static callback is executed once task is done."""
        job_callback(1, 1).get()
        self.assertEqual(2, _results)

    def test_process_callback_dynamic(self):
        """Test dynamic callback is executed once task is done."""
        job.callback = self.callback
        job(1, 1).get()
        self.assertEqual(2, self.callback_results)

    def test_process_error(self):
        """Test that an exception in a process task is raised by get."""
        self.assertRaises(Exception, job_error(1, 1).get)

    def test_process_error_callback(self):
        """Test that an exception in a task is managed in callback."""
        job_error.callback = self.error_callback
        task = job_error(1, 1)
        try:
            task.get()
        except:
            pass
        self.assertEqual('BOOM!', str(self.exception))

    def test_process_timeout(self):
        """TimeoutError is raised if task times out."""
        task = job_timeout()
        self.assertRaises(TimeoutError, task.get)

    def test_process_no_timeout(self):
        """Timeout decorator doesn't kill the task."""
        self.assertEqual(job_long_timeout().get(), 1)

    def test_process_unserializable(self):
        """PicklingError is returned if results are not serializeable."""
        task = job_unserializeable()
        try:  # Python 2
            from cPickle import PicklingError
            self.assertRaises(PicklingError, task.get)
        except ImportError:  # Python 3
            from pickle import PicklingError
            self.assertRaises(PicklingError, task.get)

    def test_process_unserializable_error(self):
        """SerializingError is returned if exception is not serializeable."""
        task = job_unserializeable_error()
        self.assertRaises(SerializingError, task.get)


class TestProcessTask(unittest.TestCase):
    def setUp(self):
        pass

    def test_task_get(self):
        """Values are correctly returned from get."""
        task = job(1, 1)
        self.assertEqual(task.get(), 2)

    def test_task_get_error(self):
        """An exception in a task is raised by get."""
        task = job_error(1, 1)
        self.assertRaises(Exception, task.get)

    def test_task_timeout(self):
        """TimeoutError is raised if task has not yet finished."""
        task = job_long()
        self.assertRaises(TimeoutError, task.get, 0)

    def test_task_no_timeout(self):
        """Test timeout get parameter works."""
        task = job_long()
        self.assertEqual(task.get(2), 1)

    def test_task_number(self):
        """Task number are correctly assigned."""
        for i in range(0, 5):
            task = job_count(1, 1)
        self.assertEqual(task.number, 4)

    def test_task_ready(self):
        """Ready parameter is true if task is done."""
        task = job(1, 1)
        task.get()
        self.assertTrue(task.ready)

    def test_task_not_ready(self):
        """Ready parameter is false if task is not done."""
        task = job_long()
        self.assertFalse(task.ready)

    def test_task_cancel(self):
        """Task is cancelled."""
        task = job_long()
        task.cancel()
        self.assertRaises(TaskCancelled, task.get)

    def test_task_cancelled(self):
        """Cancelled is true if task is cancelled."""
        task = job_long()
        task.cancel()
        self.assertTrue(task.cancelled)


if __name__ == "__main__":
    unittest.main()
