import os
import time
import unittest
import threading

from pebble import process, TaskCancelled, TimeoutError


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


def error_callback(task):
    raise Exception("BOOM!")


def initializer(value):
    global initarg
    initarg = value


def initializer_error():
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


def pid_function():
    time.sleep(0.1)
    return os.getpid()


# class TestProcessTaskObj(object):
#     a = 0

#     def __init__(self):
#         self.b = 1

#     @classmethod
#     @process.task
#     def clsmethod(cls):
#         return cls.a

#     @process.task
#     def instmethod(self):
#         return self.b

#     @staticmethod
#     @process.task
#     def stcmethod():
#         return 2


class TestProcessPool(unittest.TestCase):
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

    def test_process_pool_single_task(self):
        """Process Pool single task."""
        with process.Pool() as pool:
            task = pool.schedule(function, args=[1],
                                 kwargs={'keyword_argument': 1})
        self.assertEqual(task.get(), 2)

    def test_process_pool_multiple_tasks(self):
        """Process Pool multiple tasks."""
        tasks = []
        with process.Pool() as pool:
            for index in range(5):
                tasks.append(pool.schedule(function, args=[1]))
        self.assertEqual(sum([t.get() for t in tasks]), 5)

    def test_process_pool_callback(self):
        """Process Pool results are forwarded to the callback."""
        with process.Pool() as pool:
            pool.schedule(function, args=[1], callback=self.callback,
                          kwargs={'keyword_argument': 1})
        self.event.wait()
        self.assertEqual(self.results, 2)

    def test_process_pool_error(self):
        """Process Pool errors are raised by task get."""
        with process.Pool() as pool:
            task = pool.schedule(error_function)
        self.assertRaises(Exception, task.get)

    def test_process_pool_error_callback(self):
        """Process Pool errors are forwarded to callback."""
        with process.Pool() as pool:
            pool.schedule(error_function, callback=self.callback)
        self.event.wait()
        self.assertTrue(isinstance(self.exception, Exception))

    def test_process_pool_timeout(self):
        """Process Pool task raises TimeoutError if so."""
        with process.Pool() as pool:
            task = pool.schedule(long_function, timeout=0.1)
        self.assertRaises(TimeoutError, task.get)

    def test_process_pool_timeout_callback(self):
        """Process Pool TimeoutError is forwarded to callback."""
        with process.Pool() as pool:
            pool.schedule(long_function, callback=self.callback, timeout=0.1)
        self.event.wait()
        self.assertTrue(isinstance(self.exception, TimeoutError))

    def test_process_pool_cancel(self):
        """Process Pool task raises TaskCancelled if so."""
        with process.Pool() as pool:
            task = pool.schedule(long_function)
            task.cancel()
        self.assertRaises(TaskCancelled, task.get)

    def test_process_pool_cancel_callback(self):
        """Process Pool TaskCancelled is forwarded to callback."""
        with process.Pool() as pool:
            task = pool.schedule(long_function, callback=self.callback)
            task.cancel()
        self.event.wait()
        self.assertTrue(isinstance(self.exception, TaskCancelled))

    def test_process_pool_different_process(self):
        """Process Pool multiple tasks are handled by different processes."""
        tasks = []
        with process.Pool(workers=2) as pool:
            for i in range(0, 5):
                tasks.append(pool.schedule(pid_function))
        self.assertEqual(len(set([t.get() for t in tasks])), 2)

    def test_process_pool_task_limit(self):
        """Process Pool task limit is honored."""
        tasks = []
        with process.Pool(task_limit=2) as pool:
            for i in range(0, 4):
                tasks.append(pool.schedule(pid_function))
        self.assertEqual(len(set([t.get() for t in tasks])), 2)

    def test_process_pool_stop_timeout(self):
        """Process Pool workers are stopped if task timeout."""
        with process.Pool() as pool:
            task1 = pool.schedule(pid_function)
            pool.schedule(long_function, timeout=0.1)
            task2 = pool.schedule(pid_function)
        self.assertNotEqual(task1.get(), task2.get())

    def test_process_pool_stop_cancel(self):
        """Process Pool workers are stopped if task cancelled."""
        with process.Pool() as pool:
            task1 = pool.schedule(pid_function)
            task = pool.schedule(long_function)
            task.cancel()
            task2 = pool.schedule(pid_function)
        self.assertNotEqual(task1.get(), task2.get())

    def test_process_pool_schedule_id(self):
        """Process Pool task ID is forwarded to it."""
        with process.Pool() as pool:
            task = pool.schedule(function, args=[1], identifier='foo')
        self.assertEqual(task.id, 'foo')

    def test_process_pool_initializer(self):
        """Process Pool initializer is correctly run."""
        with process.Pool(initializer=initializer, initargs=[1]) as pool:
            task = pool.schedule(initializer_function)
        self.assertEqual(task.get(), 1)

    def test_process_pool_initializer_error(self):
        """Process Pool an exception in a initializer is raised by get."""
        with process.Pool(initializer=initializer_error) as pool:
            task = pool.schedule(initializer_function)
        self.assertRaises(Exception, task.get)

    def test_process_pool_created(self):
        """Process Pool is not active if nothing is scheduled."""
        with process.Pool() as pool:
            self.assertFalse(pool.active)

    def test_process_pool_running(self):
        """Process Pool is active if a task is scheduled."""
        with process.Pool() as pool:
            pool.schedule(function, args=[1])
            self.assertTrue(pool.active)

    def test_process_pool_stopped(self):
        """Process Pool is not active once stopped."""
        with process.Pool() as pool:
            pool.schedule(function, args=[1])
        self.assertFalse(pool.active)

    def test_process_pool_close_tasks(self):
        """Process Pool all tasks are performed on close."""
        tasks = []
        pool = process.Pool()
        for index in range(10):
            tasks.append(pool.schedule(function, args=[index]))
        pool.close()
        pool.join()
        map(self.assertTrue, [t.ready for t in tasks])

    def test_process_pool_close_stopped(self):
        """Process Pool is stopped after close."""
        pool = process.Pool()
        pool.schedule(function, args=[1])
        pool.close()
        pool.join()
        self.assertFalse(pool.active)

    def test_process_pool_stop_tasks(self):
        """Process Pool not all tasks are performed on stop."""
        tasks = []
        pool = process.Pool()
        for index in range(10):
            tasks.append(pool.schedule(function, args=[index]))
        pool.stop()
        pool.join()
        self.assertTrue(len([t for t in tasks if not t.ready]) > 0)

    def test_process_pool_stop_stopped(self):
        """Process Pool is stopped after stop."""
        pool = process.Pool()
        pool.schedule(function, args=[1])
        pool.stop()
        pool.join()
        self.assertFalse(pool.active)

    def test_process_pool_kill_tasks(self):
        """Process Pool not all tasks are performed on kill."""
        tasks = []
        pool = process.Pool()
        for index in range(10):
            tasks.append(pool.schedule(function, args=[index]))
        pool.kill()
        pool.join()
        self.assertTrue(len([t for t in tasks if not t.ready]) > 0)

    def test_process_pool_kill_stopped(self):
        """Process Pool is killed after kill."""
        pool = process.Pool()
        pool.schedule(function, args=[1])
        pool.kill()
        pool.join()
        self.assertFalse(pool.active)

    def test_process_pool_join_workers(self):
        """Process Pool no worker is running after join."""
        pool = process.Pool(workers=4)
        pool.schedule(function, args=[1])
        pool.stop()
        pool.join()
        self.assertEqual(len(pool._context.pool), 0)

    def test_process_pool_join_running(self):
        """Process Pool RuntimeError is raised if active pool joined."""
        with process.Pool() as pool:
            pool.schedule(function, args=[1])
            self.assertRaises(RuntimeError, pool.join)

    def test_process_pool_join_tasks_timeout(self):
        """Process Pool TimeoutError is raised if join on long tasks."""
        pool = process.Pool()
        for index in range(2):
            pool.schedule(long_function)
        pool.close()
        self.assertRaises(TimeoutError, pool.join, 0.4)
        pool.stop()
        pool.join()

    def test_process_pool_callback_error(self):
        """Process Pool stop if error in callback."""
        with process.Pool() as pool:
            pool.schedule(function, args=[1], callback=error_callback,
                          kwargs={'keyword_argument': 1})
        time.sleep(0.1)
        self.assertRaises(RuntimeError, pool.schedule, function, args=[1])

    def test_process_pool_exception_isolated(self):
        """Process Pool an Exception does not affect other tasks."""
        with process.Pool() as pool:
            task = pool.schedule(error_function)
            try:
                task.get()
            except:
                pass
            task = pool.schedule(function, args=[1],
                                 kwargs={'keyword_argument': 1})
        self.assertEqual(task.get(), 2)
