import os
import time
import signal
import unittest
import threading
try:  # Python 2
    from Queue import Queue
except:  # Python 3
    from queue import Queue

from pebble import decorators

from pebble.common import launch_thread
from pebble import synchronized, sighandler
from pebble import waitforthreads, waitforqueues


results = 0
semaphore = threading.Semaphore()


@synchronized
def synchronized_function():
    """A docstring."""
    return decorators._synchronized_lock.acquire(False)


@synchronized(semaphore)
def custom_synchronized_function():
    """A docstring."""
    return semaphore.acquire(False)


try:
    from signal import SIGALRM, SIGFPE, SIGIO

    @sighandler(SIGALRM)
    def signal_handler(signum, frame):
        """A docstring."""
        global results
        results = 1

    @sighandler((SIGFPE, SIGIO))
    def signals_handler(signum, frame):
        pass
except ImportError:
    pass


def thread_function(value):
    time.sleep(value)
    return value


def queue_function(queues, index, value):
    time.sleep(value)
    queues[index].put(value)
    return value


def spurious_wakeup_function(value, lock):
    value = value / 2
    time.sleep(value)
    lock.acquire()
    time.sleep(value)
    return value


class TestSynchronizedDecorator(unittest.TestCase):
    def test_wrapper_decorator_docstring(self):
        """Synchronized docstring of the original function is preserved."""
        self.assertEqual(synchronized_function.__doc__, "A docstring.")

    def test_syncronized_locked(self):
        """Synchronized Lock is acquired
        during execution of decorated function."""
        self.assertFalse(synchronized_function())

    def test_syncronized_released(self):
        """Synchronized Lock is released
        during execution of decorated function."""
        synchronized_function()
        self.assertTrue(decorators._synchronized_lock.acquire(False))
        decorators._synchronized_lock.release()

    def test_custom_syncronized_locked(self):
        """Synchronized semaphore is acquired
        during execution of decorated function."""
        self.assertFalse(custom_synchronized_function())

    def test_custom_syncronized_released(self):
        """Synchronized semaphore is acquired
        during execution of decorated function."""
        custom_synchronized_function()
        self.assertTrue(semaphore.acquire(False))
        semaphore.release()


class TestSigHandler(unittest.TestCase):
    def test_wrapper_decorator_docstring(self):
        """Sighandler docstring of the original function is preserved."""
        if os.name != 'nt':
            self.assertEqual(signal_handler.__doc__, "A docstring.")

    def test_sighandler(self):
        """Sighandler installs SIGALRM."""
        if os.name != 'nt':
            self.assertEqual(signal.getsignal(signal.SIGALRM).__name__,
                             signal_handler.__name__)

    def test_sighandler_multiple(self):
        """Sighandler installs SIGFPE and SIGIO."""
        if os.name != 'nt':
            self.assertEqual(signal.getsignal(signal.SIGFPE).__name__,
                             signals_handler.__name__)
            self.assertEqual(signal.getsignal(signal.SIGIO).__name__,
                             signals_handler.__name__)

    def test_sigalarm_sighandler(self):
        """Sighandler for SIGALARM works."""
        if os.name != 'nt':
            os.kill(os.getpid(), signal.SIGALRM)
            time.sleep(0.1)
            self.assertEqual(results, 1)


class TestWaitForThreads(unittest.TestCase):
    def test_waitforthreads_single(self):
        """Waitforthreads waits for a single thread."""
        thread = launch_thread(thread_function, 0.01)
        self.assertEqual(list(waitforthreads([thread]))[0], thread)

    def test_waitforthreads_multiple(self):
        """Waitforthreads waits for multiple threads."""
        threads = []
        for _ in range(5):
            threads.append(launch_thread(thread_function, 0.01))
        time.sleep(0.1)
        self.assertEqual(list(waitforthreads(threads)), threads)

    def test_waitforthreads_timeout(self):
        """Waitforthreads returns empty list if timeout."""
        thread = launch_thread(thread_function, 0.1)
        self.assertEqual(list(waitforthreads([thread], timeout=0.01)), [])

    def test_waitforthreads_restore(self):
        """Waitforthreads get_ident is restored to original one."""
        if hasattr(threading, 'get_ident'):
            expected = threading.get_ident
        else:
            expected = threading._get_ident
        thread = launch_thread(thread_function, 0)
        time.sleep(0.01)
        waitforthreads([thread])
        if hasattr(threading, 'get_ident'):
            self.assertEqual(threading.get_ident, expected)
        else:
            self.assertEqual(threading._get_ident, expected)

    def test_waitforthreads_spurious(self):
        """Waitforthreads tolerates spurious wakeups."""
        lock = threading.RLock()
        thread = launch_thread(spurious_wakeup_function, 0.1, lock)
        self.assertEqual(list(waitforthreads([thread])), [thread])


class TestWaitForQueues(unittest.TestCase):
    def setUp(self):
        self.queues = [Queue(), Queue(), Queue()]

    def test_waitforqueues_single(self):
        """Waitforqueues waits for a single queue."""
        launch_thread(queue_function, self.queues, 0, 0.01)
        self.assertEqual(list(waitforqueues(self.queues))[0], self.queues[0])

    def test_waitforqueues_multiple(self):
        """Waitforqueues waits for multiple queues."""
        for index in range(3):
            launch_thread(queue_function, self.queues, index, 0.01)
        time.sleep(0.1)
        self.assertEqual(list(waitforqueues(self.queues)), self.queues)

    def test_waitforqueues_timeout(self):
        """Waitforqueues returns empty list if timeout."""
        launch_thread(queue_function, self.queues, 0, 0.1)
        self.assertEqual(list(waitforqueues(self.queues, timeout=0.01)), [])

    def test_waitforqueues_restore(self):
        """Waitforqueues Queue object is restored to original one."""
        expected = sorted(dir(self.queues[0]))
        launch_thread(queue_function, self.queues, 0, 0)
        waitforqueues(self.queues)
        self.assertEqual(sorted(dir(self.queues[0])), expected)
