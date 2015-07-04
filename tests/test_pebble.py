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

from pebble import synchronized, sighandler, thread
from pebble import waitfortasks, waitforthreads, waitforqueues
from pebble import Task, TimeoutError, TaskCancelled


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


@thread.spawn
def thread_function(value):
    time.sleep(value)
    return value


@thread.spawn
def queue_function(queues, index, value):
    time.sleep(value)
    queues[index].put(value)
    return value


@thread.concurrent
def concurrent_function(value):
    time.sleep(value)
    return value


@thread.spawn
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


class TestWaitForTasks(unittest.TestCase):
    def test_waitfortasks_single(self):
        """Waitfortasks waits for a single task."""
        task = concurrent_function(0.01)
        self.assertEqual(list(waitfortasks([task]))[0], task)

    def test_waitfortasks_multiple(self):
        """Waitfortasks waits for multiple tasks."""
        tasks = []
        for _ in range(5):
            tasks.append(concurrent_function(0.01))
        time.sleep(0.1)
        self.assertEqual(list(waitfortasks(tasks)), tasks)

    def test_waitfortasks_timeout(self):
        """Waitfortasks returns empty list if timeout."""
        task = concurrent_function(0.1)
        self.assertEqual(list(waitfortasks([task], timeout=0.01)), [])

    def test_waitfortasks_restore(self):
        """Waitfortasks Task object is restored to original one."""
        task = concurrent_function(0.01)
        expected = sorted(dir(task))
        waitfortasks([task])
        self.assertEqual(sorted(dir(task)), expected)


class TestWaitForThreads(unittest.TestCase):
    def test_waitforthreads_single(self):
        """Waitforthreads waits for a single thread."""
        thread = thread_function(0.01)
        self.assertEqual(list(waitforthreads([thread]))[0], thread)

    def test_waitforthreads_multiple(self):
        """Waitforthreads waits for multiple threads."""
        threads = []
        for _ in range(5):
            threads.append(thread_function(0.01))
        time.sleep(0.1)
        self.assertEqual(list(waitforthreads(threads)), threads)

    def test_waitforthreads_timeout(self):
        """Waitforthreads returns empty list if timeout."""
        thread = thread_function(0.1)
        self.assertEqual(list(waitforthreads([thread], timeout=0.01)), [])

    def test_waitforthreads_restore(self):
        """Waitforthreads get_ident is restored to original one."""
        if hasattr(threading, 'get_ident'):
            expected = threading.get_ident
        else:
            expected = threading._get_ident
        thread = thread_function(0)
        time.sleep(0.01)
        waitforthreads([thread])
        if hasattr(threading, 'get_ident'):
            self.assertEqual(threading.get_ident, expected)
        else:
            self.assertEqual(threading._get_ident, expected)

    def test_waitforthreads_spurious(self):
        """Waitforthreads tolerates spurious wakeups."""
        lock = threading.RLock()
        thread = spurious_wakeup_function(0.1, lock)
        self.assertEqual(list(waitforthreads([thread])), [thread])


class TestWaitForQueues(unittest.TestCase):
    def setUp(self):
        self.queues = [Queue(), Queue(), Queue()]

    def test_waitforqueues_single(self):
        """Waitforqueues waits for a single queue."""
        queue_function(self.queues, 0, 0.01)
        self.assertEqual(list(waitforqueues(self.queues))[0], self.queues[0])

    def test_waitforqueues_multiple(self):
        """Waitforqueues waits for multiple queues."""
        for index in range(3):
            queue_function(self.queues, index, 0.01)
        time.sleep(0.1)
        self.assertEqual(list(waitforqueues(self.queues)), self.queues)

    def test_waitforqueues_timeout(self):
        """Waitforqueues returns empty list if timeout."""
        queue_function(self.queues, 0, 0.1)
        self.assertEqual(list(waitforqueues(self.queues, timeout=0.01)), [])

    def test_waitforqueues_restore(self):
        """Waitforqueues Queue object is restored to original one."""
        expected = sorted(dir(self.queues[0]))
        queue_function(self.queues, 0, 0)
        waitforqueues(self.queues)
        self.assertEqual(sorted(dir(self.queues[0])), expected)


class TestTask(unittest.TestCase):
    def setUp(self):
        self.task = Task(0)

    def test_number(self):
        """Task number is reported correctly."""
        t = Task(42)
        self.assertEqual(t.number, 42)

    def test_task_id(self):
        """Task ID is forwarded to it."""
        t = Task(0, identifier='foo')
        self.assertEqual(t.id, 'foo')

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
        """Task is started if timestamp is self.task."""
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
        """Task cancel does not override a set task."""
        self.task._set(42)
        self.task.cancel()
        self.assertEqual(self.task.get(), 42)
