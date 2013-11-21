import time
import unittest

from pebble import thread, TimeoutError, TaskCancelled


_results = 0
_exception = None


def callback(task):
    global _results
    _results = task.get()


def error_callback(task):
    global _exception
    _exception = task.get()


@thread
def job(argument, keyword_argument=0):
    return argument + keyword_argument


@thread
def job_error(argument, keyword_argument=0):
    raise Exception("BOOM!")


@thread(callback=callback)
def job_callback(argument, keyword_argument=0):
    return argument + keyword_argument


@thread
def job_long():
    time.sleep(1)
    return 1


@thread
def job_count():
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
            @thread(callback, 5)
            def wrong(argument, keyword_argument=0):
                return argument + keyword_argument
        except Exception as error:
            self.assertTrue(isinstance(error, ValueError))

    def test_thread_callback_static(self):
        """Test static callback is executed with thread task."""
        task = job_callback(1, 1)
        task.get()
        self.assertEqual(2, _results)

    def test_thread_callback_dynamic(self):
        """Test dynamic callback is executed with thread task."""
        job.callback = self.callback
        task = job(1, 1)
        task.get()
        self.assertEqual(2, self.callback_results)

    def test_thread_error_callback(self):
        """Test that an exception in a task is managed in error_callback."""
        job_error.callback = self.error_callback
        task = job_error(1, 1)
        try:
            task.get()
        except:
            pass
        self.assertEqual('BOOM!', str(self.exception))


class TestThreadTask(unittest.TestCase):
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
