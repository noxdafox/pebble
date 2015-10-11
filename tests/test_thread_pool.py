import time
import unittest
import threading
try:
    from queue import Queue
except ImportError:
    from Queue import Queue

from pebble import thread
from pebble import PoolError, TaskCancelled, TimeoutError


event = threading.Event()
initarg = 0
results = 0
exception = None


def callback(task):
    global results
    global exception

    try:
        results = task.get()
    except Exception as error:
        exception = error

    event.set()


def queue_factory():
    return Queue(maxsize=5)


def error_callback(task):
    raise Exception("BOOM!")


def initializer(value):
    global initarg
    initarg = value


def broken_initializer():
    raise Exception("BOOM!")


def function(argument, keyword_argument=0):
    """A docstring."""
    return argument + keyword_argument


def initializer_function():
    return initarg


def error_function():
    raise Exception("BOOM!")


def long_function():
    time.sleep(1)


def tid_function():
    time.sleep(0.1)
    return threading.current_thread()


class TestThreadPool(unittest.TestCase):
    def setUp(self):
        global initarg
        initarg = 0
        self.event = threading.Event()
        self.event.clear()
        self.results = None
        self.exception = None

    def callback(self, task):
        try:
            self.results = task.get()
        except Exception as error:
            self.exception = error
        finally:
            self.event.set()

    def test_thread_pool_queue_factory(self):
        """Thread Pool queue factory is called."""
        with thread.Pool(queue_factory=queue_factory) as pool:
            self.assertEqual(pool._context.task_queue.maxsize, 5)

    def test_thread_pool_single_task(self):
        """Thread Pool single task."""
        with thread.Pool() as pool:
            task = pool.schedule(function, args=[1],
                                 kwargs={'keyword_argument': 1})
        self.assertEqual(task.get(), 2)

    def test_thread_pool_multiple_tasks(self):
        """Thread Pool multiple tasks."""
        tasks = []
        with thread.Pool() as pool:
            for index in range(5):
                tasks.append(pool.schedule(function, args=[1]))
        self.assertEqual(sum([t.get() for t in tasks]), 5)

    def test_thread_pool_callback(self):
        """Thread Pool results are forwarded to the callback."""
        with thread.Pool() as pool:
            pool.schedule(function, args=[1], callback=self.callback,
                          kwargs={'keyword_argument': 1})
        self.event.wait()
        self.assertEqual(self.results, 2)

    def test_thread_pool_error(self):
        """Thread Pool errors are raised by task get."""
        with thread.Pool() as pool:
            task = pool.schedule(error_function)
        self.assertRaises(Exception, task.get)

    def test_thread_pool_error_callback(self):
        """Thread Pool errors are forwarded to callback."""
        with thread.Pool() as pool:
            pool.schedule(error_function, callback=self.callback)
        self.event.wait()
        self.assertTrue(isinstance(self.exception, Exception))

    def test_thread_pool_cancel(self):
        """Thread Pool task raises TaskCancelled if so."""
        with thread.Pool() as pool:
            task = pool.schedule(long_function)
            task.cancel()
        self.assertRaises(TaskCancelled, task.get)

    def test_thread_pool_cancel_callback(self):
        """Thread Pool TaskCancelled is forwarded to callback."""
        with thread.Pool() as pool:
            task = pool.schedule(long_function, callback=self.callback)
            task.cancel()
        self.event.wait()
        self.assertTrue(isinstance(self.exception, TaskCancelled))

    def test_thread_pool_different_thread(self):
        """Thread Pool multiple tasks are handled by different threades."""
        tasks = []
        with thread.Pool(workers=2) as pool:
            for i in range(0, 5):
                tasks.append(pool.schedule(tid_function))
        self.assertEqual(len(set([t.get() for t in tasks])), 2)

    def test_thread_pool_task_limit(self):
        """Thread Pool task limit is honored."""
        tasks = []
        with thread.Pool(task_limit=2) as pool:
            for i in range(0, 4):
                tasks.append(pool.schedule(tid_function))
        self.assertEqual(len(set([t.get() for t in tasks])), 2)

    def test_thread_pool_schedule_id(self):
        """Thread Pool task ID is forwarded to it."""
        with thread.Pool() as pool:
            task = pool.schedule(function, args=[1], identifier='foo')
        self.assertEqual(task.id, 'foo')

    def test_thread_pool_initializer(self):
        """Thread Pool initializer is correctly run."""
        with thread.Pool(initializer=initializer, initargs=[1]) as pool:
            task = pool.schedule(initializer_function)
        self.assertEqual(task.get(), 1)

    def test_thread_pool_broken_initializer(self):
        """Thread Pool broken initializer is notified."""
        with self.assertRaises(PoolError):
            with thread.Pool(initializer=broken_initializer) as pool:
                pool.schedule(function)

    def test_thread_pool_running(self):
        """Thread Pool is active if a task is scheduled."""
        with thread.Pool() as pool:
            pool.schedule(function, args=[1])
            self.assertTrue(pool.active)

    def test_thread_pool_stopped(self):
        """Thread Pool is not active once stopped."""
        with thread.Pool() as pool:
            pool.schedule(function, args=[1])
        self.assertFalse(pool.active)

    def test_thread_pool_close_tasks(self):
        """Thread Pool all tasks are performed on close."""
        tasks = []
        pool = thread.Pool()
        for index in range(10):
            tasks.append(pool.schedule(function, args=[index]))
        pool.close()
        pool.join()
        map(self.assertTrue, [t.ready for t in tasks])

    def test_thread_pool_close_stopped(self):
        """Thread Pool is stopped after close."""
        pool = thread.Pool()
        pool.schedule(function, args=[1])
        pool.close()
        pool.join()
        self.assertFalse(pool.active)

    def test_thread_pool_stop_tasks(self):
        """Thread Pool not all tasks are performed on stop."""
        tasks = []
        pool = thread.Pool()
        for index in range(10):
            tasks.append(pool.schedule(long_function, args=[index]))
        pool.stop()
        pool.join()
        self.assertTrue(len([t for t in tasks if not t.ready]) > 0)

    def test_thread_pool_stop_stopped(self):
        """Thread Pool is stopped after stop."""
        pool = thread.Pool()
        pool.schedule(function, args=[1])
        pool.stop()
        pool.join()
        self.assertFalse(pool.active)

    def test_thread_pool_join_workers(self):
        """Thread Pool no worker is running after join."""
        pool = thread.Pool(workers=4)
        pool.schedule(function, args=[1])
        pool.stop()
        pool.join()
        self.assertEqual(len(pool._pool_manager.workers), 0)

    def test_thread_pool_join_running(self):
        """Thread Pool RuntimeError is raised if active pool joined."""
        with thread.Pool() as pool:
            pool.schedule(function, args=[1])
            self.assertRaises(RuntimeError, pool.join)

    def test_thread_pool_join_tasks_timeout(self):
        """Thread Pool TimeoutError is raised if join on long tasks."""
        pool = thread.Pool()
        for index in range(2):
            pool.schedule(long_function)
        pool.close()
        self.assertRaises(TimeoutError, pool.join, 0.4)
        pool.stop()
        pool.join()

    def test_thread_pool_callback_error(self):
        """Thread Pool stop if error in callback."""
        with thread.Pool() as pool:
            pool.schedule(function, args=[1], callback=error_callback,
                          kwargs={'keyword_argument': 1})
        time.sleep(0.1)
        self.assertRaises(PoolError, pool.schedule, function, args=[1])

    def test_thread_pool_exception_isolated(self):
        """Thread Pool an Exception does not affect other tasks."""
        with thread.Pool() as pool:
            task = pool.schedule(error_function)
            try:
                task.get()
            except:
                pass
            task = pool.schedule(function, args=[1],
                                 kwargs={'keyword_argument': 1})
        self.assertEqual(task.get(), 2)
