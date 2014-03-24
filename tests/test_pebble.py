import unittest

from pebble import Task, TimeoutError, TaskCancelled


class TestTask(unittest.TestCase):
    def setUp(self):
        self.task = Task(0, None, None, None, None, None, None)

    def test_number(self):
        """Task number is reported correctly."""
        t = Task(42, None, None, None, None, None, None)
        self.assertEqual(t.number, 42)

    def test_task_id(self):
        """Task ID is forwarded to it."""
        t = Task(0, None, None, None, None, None, 'foo')
        self.assertEqual(t.id, 'foo')

    def test_task_uuid(self):
        """Task a UUID is assigned if None."""
        t = Task(0, None, None, None, None, None, None)
        self.assertEqual(t.id.version, 4)

    def test_ready(self):
        """Task is ready if results are seself.task."""
        self.task._set(None)
        self.assertTrue(self.task.ready)

    def test_not_read(self):
        """Task is not ready if results are not seself.task."""
        self.assertFalse(self.task.ready)

    def test_cancelled(self):
        """Task is cancelled if cancel() is called."""
        self.task.cancel()
        self.assertTrue(self.task.cancelled)

    def test_not_cancelled(self):
        """Task is not cancelled if cancel() is not called."""
        self.assertFalse(self.task.cancelled)

    def test_started(self):
        """Task is started if timestamp is seself.task."""
        self.task._timestamp = 42
        self.assertTrue(self.task.started)

    def test_not_started(self):
        """Task is not started if timestamp is not seself.task."""
        self.assertFalse(self.task.started)

    def test_success(self):
        """Task is successful if results are seself.task."""
        self.task._set(42)
        self.assertTrue(self.task.success)

    def test_not_success(self):
        """Task is not successful if results are not seself.task."""
        self.assertFalse(self.task.success)

    def test_not_success_exception(self):
        """Task is not successful if results are an Exception."""
        self.task._set(Exception("BOOM"))
        self.assertFalse(self.task.success)

    def test_wait(self):
        """Task wait returns True if results are ready."""
        self.task._set(42)
        self.assertTrue(self.task.wait())

    def test_wait_no_timeout(self):
        """Task wait returns True if timeout does not expire."""
        self.task._set(42)
        self.assertTrue(self.task.wait(timeout=0))

    def test_wait_timeout(self):
        """Task wait returns False if timeout expired."""
        self.assertFalse(self.task.wait(timeout=0))

    def test_get(self):
        """Task values are returned by get if results are set."""
        self.task._set(42)
        self.assertEqual(self.task.get(), 42)

    def test_get_exception(self):
        """Task get raises the exception set as results."""
        self.task._set(Exception("BOOM"))
        self.assertRaises(Exception, self.task.get)

    def test_get_timeout(self):
        """Task get raises TimeoutError if timeout expires."""
        self.assertRaises(TimeoutError, self.task.get, 0)

    def test_get_no_timeout(self):
        """Task values are returned by get if results are set
        before timeout expires."""
        self.task._set(42)
        self.assertEqual(self.task.get(0), 42)

    def test_get_timeout_cancelled(self):
        """Task is cancelled if Timeout expires and cancel is set."""
        try:
            self.task.get(timeout=0, cancel=True)
        except TimeoutError:
            pass
        self.assertTrue(self.task.cancelled)

    def test_cancel(self):
        """Task get raises TaskCancelled if task is cancelled."""
        self.task.cancel()
        self.assertRaises(TaskCancelled, self.task.get)

    def test_set_unique(self):
        """Task _set works only once."""
        self.task._set(42)
        self.task._set(None)
        self.assertEqual(self.task.get(), 42)

    def test_set_not_overriding(self):
        """Task _set does not override a cancelled task."""
        self.task.cancel()
        self.task._set(42)
        self.assertRaises(TaskCancelled, self.task.get)

    def test_cancel_overriding(self):
        """Task cancel overrides a set task."""
        self.task._set(42)
        self.task.cancel()
        self.assertRaises(TaskCancelled, self.task.get)
