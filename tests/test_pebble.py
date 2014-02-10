import unittest
import threading

from pebble import synchronized


lock = threading.Lock()


@synchronized(lock)
def function():
    """A docstring."""
    return lock.acquire(False)


class TestSynchronizedDecorator(unittest.TestCase):
    def test_wrapper_decorator_docstring(self):
        """Docstring of the original function is preserved."""
        self.assertEqual(function.__doc__, "A docstring.")

    def test_syncronized_locked(self):
        """Lock is acquired during execution of decorated function."""
        self.assertFalse(function())

    def test_syncronized_released(self):
        """Lock is acquired during execution of decorated function."""
        function()
        self.assertTrue(lock.acquire(False))
        lock.release()
