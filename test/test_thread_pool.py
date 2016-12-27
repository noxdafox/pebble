import time
import unittest
import threading

from pebble import ThreadPool

from concurrent.futures import CancelledError, TimeoutError


initarg = 0


def error_callback(future):
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

    def callback(self, future):
        try:
            self.results = future.result()
        except Exception as error:
            self.exception = error
        finally:
            self.event.set()

    def test_thread_pool_single_future(self):
        """Thread Pool single future."""
        with ThreadPool() as pool:
            future = pool.schedule(function, args=[1],
                                   kwargs={'keyword_argument': 1})
        self.assertEqual(future.result(), 2)

    def test_thread_pool_multiple_futures(self):
        """Thread Pool multiple futures."""
        futures = []
        with ThreadPool() as pool:
            for _ in range(5):
                futures.append(pool.schedule(function, args=[1]))
        self.assertEqual(sum([t.result() for t in futures]), 5)

    def test_thread_pool_callback(self):
        """Thread Pool results are forwarded to the callback."""
        with ThreadPool() as pool:
            future = pool.schedule(
                function, args=[1], kwargs={'keyword_argument': 1})
            future.add_done_callback(self.callback)

        self.event.wait()
        self.assertEqual(self.results, 2)

    def test_thread_pool_error(self):
        """Thread Pool errors are raised by future get."""
        with ThreadPool() as pool:
            future = pool.schedule(error_function)
        with self.assertRaises(Exception):
            future.result()

    def test_thread_pool_error_callback(self):
        """Thread Pool errors are forwarded to callback."""
        with ThreadPool() as pool:
            future = pool.schedule(error_function)
            future.add_done_callback(self.callback)
        self.event.wait()
        self.assertTrue(isinstance(self.exception, Exception))

    def test_thread_pool_cancel_callback(self):
        """Thread Pool FutureCancelled is forwarded to callback."""
        with ThreadPool() as pool:
            future = pool.schedule(long_function)
            future.add_done_callback(self.callback)
            future.cancel()
        self.event.wait()
        self.assertTrue(isinstance(self.exception, CancelledError))

    def test_thread_pool_different_thread(self):
        """Thread Pool multiple futures are handled by different threades."""
        futures = []
        with ThreadPool(max_workers=2) as pool:
            for _ in range(0, 5):
                futures.append(pool.schedule(tid_function))
        self.assertEqual(len(set([t.result() for t in futures])), 2)

    def test_thread_pool_tasks_limit(self):
        """Thread Pool future limit is honored."""
        futures = []
        with ThreadPool(max_tasks=2) as pool:
            for _ in range(0, 4):
                futures.append(pool.schedule(tid_function))
        self.assertEqual(len(set([t.result() for t in futures])), 2)

    def test_thread_pool_initializer(self):
        """Thread Pool initializer is correctly run."""
        with ThreadPool(initializer=initializer, initargs=[1]) as pool:
            future = pool.schedule(initializer_function)
        self.assertEqual(future.result(), 1)

    def test_thread_pool_broken_initializer(self):
        """Thread Pool broken initializer is notified."""
        with self.assertRaises(RuntimeError):
            with ThreadPool(initializer=broken_initializer) as pool:
                pool.active
                time.sleep(0.3)
                pool.schedule(function)

    def test_thread_pool_running(self):
        """Thread Pool is active if a future is scheduled."""
        with ThreadPool() as pool:
            pool.schedule(function, args=[1])
            self.assertTrue(pool.active)

    def test_thread_pool_stopped(self):
        """Thread Pool is not active once stopped."""
        with ThreadPool() as pool:
            pool.schedule(function, args=[1])
        self.assertFalse(pool.active)

    def test_thread_pool_close_futures(self):
        """Thread Pool all futures are performed on close."""
        futures = []
        pool = ThreadPool()
        for index in range(10):
            futures.append(pool.schedule(function, args=[index]))
        pool.close()
        pool.join()
        map(self.assertTrue, [t.done() for t in futures])

    def test_thread_pool_close_stopped(self):
        """Thread Pool is stopped after close."""
        pool = ThreadPool()
        pool.schedule(function, args=[1])
        pool.close()
        pool.join()
        self.assertFalse(pool.active)

    def test_thread_pool_stop_futures(self):
        """Thread Pool not all futures are performed on stop."""
        futures = []
        pool = ThreadPool()
        for index in range(10):
            futures.append(pool.schedule(long_function, args=[index]))
        pool.stop()
        pool.join()
        self.assertTrue(len([t for t in futures if not t.done()]) > 0)

    def test_thread_pool_stop_stopped(self):
        """Thread Pool is stopped after stop."""
        pool = ThreadPool()
        pool.schedule(function, args=[1])
        pool.stop()
        pool.join()
        self.assertFalse(pool.active)

    def test_thread_pool_join_workers(self):
        """Thread Pool no worker is running after join."""
        pool = ThreadPool(max_workers=4)
        pool.schedule(function, args=[1])
        pool.stop()
        pool.join()
        self.assertEqual(len(pool._pool_manager.workers), 0)

    def test_thread_pool_join_running(self):
        """Thread Pool RuntimeError is raised if active pool joined."""
        with ThreadPool() as pool:
            pool.schedule(function, args=[1])
            self.assertRaises(RuntimeError, pool.join)

    def test_thread_pool_join_futures_timeout(self):
        """Thread Pool TimeoutError is raised if join on long futures."""
        pool = ThreadPool()
        for _ in range(2):
            pool.schedule(long_function)
        pool.close()
        self.assertRaises(TimeoutError, pool.join, 0.4)
        pool.stop()
        pool.join()

    def test_thread_pool_exception_isolated(self):
        """Thread Pool an Exception does not affect other futures."""
        with ThreadPool() as pool:
            future = pool.schedule(error_function)
            try:
                future.result()
            except:
                pass
            future = pool.schedule(function, args=[1],
                                   kwargs={'keyword_argument': 1})
        self.assertEqual(future.result(), 2)


    def test_thread_pool_map(self):
        """Thread Pool map simple."""
        elements = [1, 2, 3]

        with ThreadPool() as pool:
            generator = pool.map(function, elements)
            self.assertEqual(list(generator), elements)

    def test_thread_pool_map_empty(self):
        """Thread Pool map no elements."""
        elements = []

        with ThreadPool() as pool:
            generator = pool.map(function, elements)
            self.assertEqual(list(generator), elements)

    def test_thread_pool_map_single(self):
        """Thread Pool map one element."""
        elements = [0]

        with ThreadPool() as pool:
            generator = pool.map(function, elements)
            self.assertEqual(list(generator), elements)

    def test_thread_pool_map_multi(self):
        """Thread Pool map multiple iterables."""
        expected = (2, 4)

        with ThreadPool() as pool:
            generator = pool.map(function, (1, 2, 3), (1, 2))
            self.assertEqual(tuple(generator), expected)

    def test_thread_pool_map_one_chunk(self):
        """Thread Pool map chunksize 1."""
        elements = [1, 2, 3]

        with ThreadPool() as pool:
            generator = pool.map(function, elements, chunksize=1)
            self.assertEqual(list(generator), elements)

    def test_thread_pool_map_zero_chunk(self):
        """Thread Pool map chunksize 0."""
        with ThreadPool() as pool:
            with self.assertRaises(ValueError):
                pool.map(function, [], chunksize=0)

    def test_thread_pool_map_error(self):
        """Thread Pool errors do not stop the iteration."""
        raised = None
        elements = [1, 'a', 3]

        with ThreadPool() as pool:
            generator = pool.map(function, elements)
            while True:
                try:
                    next(generator)
                except TypeError as error:
                    raised = error
                except StopIteration:
                    break

        self.assertTrue(isinstance(raised, TypeError))
