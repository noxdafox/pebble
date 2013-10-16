import unittest

from pebble.decorators import Asynchronous, Concurrent


_task_id = ""
_results = 0
_exception = None


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


@Asynchronous
def ajob(argument, keyword_argument=0):
    return argument + keyword_argument


@Asynchronous
def ajob_error(argument, keyword_argument=0):
    raise Exception("BOOM!")


@Asynchronous(callback=callback)
def ajob_callback(argument, keyword_argument=0):
    return argument + keyword_argument


@Concurrent
def cjob(argument, keyword_argument=0):
    return argument + keyword_argument


@Concurrent
def cjob_error(argument, keyword_argument=0):
    raise Exception("BOOM!")


@Concurrent(callback=callback)
def cjob_callback(argument, keyword_argument=0):
    return argument + keyword_argument


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
            @Asynchronous(callback, error_callback)
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

    def test_concurrent_wrong_decoration(self):
        """Decorator raises ValueError if given wrong params."""
        try:
            @Concurrent(callback, error_callback)
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
        try:
            task.get()
        except:
            pass
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
