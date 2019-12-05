import os
import sys
import time
import signal
import platform
import unittest
import threading
import multiprocessing

from concurrent.futures import CancelledError, TimeoutError

import pebble
from pebble import ProcessPool, ProcessExpired


# set start method
supported = False

if sys.version_info.major > 2 and sys.version_info.minor > 3:
    methods = multiprocessing.get_all_start_methods()
    if 'fork' in methods:
        try:
            multiprocessing.set_start_method('fork')

            if multiprocessing.get_start_method() == 'fork':
                supported = True
        except RuntimeError:  # child process
            pass
else:
    supported = True


initarg = 0


def initializer(value):
    global initarg
    initarg = value


def long_initializer():
    time.sleep(60)


def broken_initializer():
    raise Exception("BOOM!")


def function(argument, keyword_argument=0):
    """A docstring."""
    return argument + keyword_argument


def initializer_function():
    return initarg


def error_function():
    raise Exception("BOOM!")


def long_function(value=1):
    time.sleep(value)
    return value


def pid_function():
    time.sleep(0.1)
    return os.getpid()


def sigterm_function():
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    time.sleep(10)


def suicide_function():
    os._exit(1)


@unittest.skipIf(not supported, "Start method is not supported")
class TestProcessPool(unittest.TestCase):
    def setUp(self):
        global initarg
        initarg = 0
        self.event = threading.Event()
        self.event.clear()
        self.result = None
        self.exception = None

    def callback(self, future):
        try:
            self.result = future.result()
        except Exception as error:
            self.exception = error
        finally:
            self.event.set()

    def test_process_pool_single_future(self):
        """Process Pool Fork single future."""
        with ProcessPool(max_workers=1) as pool:
            future = pool.schedule(function, args=[1],
                                   kwargs={'keyword_argument': 1})
        self.assertEqual(future.result(), 2)

    def test_process_pool_multiple_futures(self):
        """Process Pool Fork multiple futures."""
        futures = []
        with ProcessPool(max_workers=1) as pool:
            for _ in range(5):
                futures.append(pool.schedule(function, args=[1]))
        self.assertEqual(sum([f.result() for f in futures]), 5)

    def test_process_pool_callback(self):
        """Process Pool Fork result is forwarded to the callback."""
        with ProcessPool(max_workers=1) as pool:
            future = pool.schedule(
                function, args=[1], kwargs={'keyword_argument': 1})
        future.add_done_callback(self.callback)
        self.event.wait()
        self.assertEqual(self.result, 2)

    def test_process_pool_error(self):
        """Process Pool Fork errors are raised by future get."""
        with ProcessPool(max_workers=1) as pool:
            future = pool.schedule(error_function)
        self.assertRaises(Exception, future.result)

    def test_process_pool_error_callback(self):
        """Process Pool Fork errors are forwarded to callback."""
        with ProcessPool(max_workers=1) as pool:
            future = pool.schedule(error_function)
        future.add_done_callback(self.callback)
        self.event.wait()
        self.assertTrue(isinstance(self.exception, Exception))

    def test_process_pool_timeout(self):
        """Process Pool Fork future raises TimeoutError if so."""
        with ProcessPool(max_workers=1) as pool:
            future = pool.schedule(long_function, timeout=0.1)
        self.assertRaises(TimeoutError, future.result)

    def test_process_pool_timeout_callback(self):
        """Process Pool Fork TimeoutError is forwarded to callback."""
        with ProcessPool(max_workers=1) as pool:
            future = pool.schedule(long_function, timeout=0.1)
        future.add_done_callback(self.callback)
        self.event.wait()
        self.assertTrue(isinstance(self.exception, TimeoutError))

    def test_process_pool_cancel(self):
        """Process Pool Fork future raises CancelledError if so."""
        with ProcessPool(max_workers=1) as pool:
            future = pool.schedule(long_function)
            time.sleep(0.1)  # let the process pick up the task
            self.assertTrue(future.cancel())
        self.assertRaises(CancelledError, future.result)

    def test_process_pool_cancel_callback(self):
        """Process Pool Fork CancelledError is forwarded to callback."""
        with ProcessPool(max_workers=1) as pool:
            future = pool.schedule(long_function)
            future.add_done_callback(self.callback)
            time.sleep(0.1)  # let the process pick up the task
            self.assertTrue(future.cancel())
        self.event.wait()
        self.assertTrue(isinstance(self.exception, CancelledError))

    def test_process_pool_different_process(self):
        """Process Pool Fork multiple futures are handled by different processes."""
        futures = []
        with ProcessPool(max_workers=2) as pool:
            for _ in range(0, 5):
                futures.append(pool.schedule(pid_function))
        self.assertEqual(len(set([f.result() for f in futures])), 2)

    def test_process_pool_future_limit(self):
        """Process Pool Fork tasks limit is honored."""
        futures = []
        with ProcessPool(max_workers=1, max_tasks=2) as pool:
            for _ in range(0, 4):
                futures.append(pool.schedule(pid_function))
        self.assertEqual(len(set([f.result() for f in futures])), 2)

    def test_process_pool_stop_timeout(self):
        """Process Pool Fork workers are stopped if future timeout."""
        with ProcessPool(max_workers=1) as pool:
            future1 = pool.schedule(pid_function)
            pool.schedule(long_function, timeout=0.1)
            future2 = pool.schedule(pid_function)
        self.assertNotEqual(future1.result(), future2.result())

    def test_process_pool_stop_cancel(self):
        """Process Pool Fork workers are stopped if future is cancelled."""
        with ProcessPool(max_workers=1) as pool:
            future1 = pool.schedule(pid_function)
            cancel_future = pool.schedule(long_function)
            time.sleep(0.1)  # let the process pick up the task
            cancel_future.cancel()
            future2 = pool.schedule(pid_function)
        self.assertNotEqual(future1.result(), future2.result())

    def test_process_pool_initializer(self):
        """Process Pool Fork initializer is correctly run."""
        with ProcessPool(initializer=initializer, initargs=[1]) as pool:
            future = pool.schedule(initializer_function)
        self.assertEqual(future.result(), 1)

    def test_process_pool_broken_initializer(self):
        """Process Pool Fork broken initializer is notified."""
        py_version = platform.python_version()
        with self.assertRaises(RuntimeError):
            with ProcessPool(initializer=broken_initializer) as pool:
                pool.active
                if py_version.startswith('3.4') or py_version.startswith('2.7'):
                    # seems v2.7 & v3.4 affected by https://bugs.python.org/issue32057, because `time.sleep` doesn't wait
                    os.system('sleep 0.4')
                else:
                    time.sleep(0.4)
                pool.schedule(function)

    def test_process_pool_running(self):
        """Process Pool Fork is active if a future is scheduled."""
        with ProcessPool(max_workers=1) as pool:
            pool.schedule(function, args=[1])
            self.assertTrue(pool.active)

    def test_process_pool_stopped(self):
        """Process Pool Fork is not active once stopped."""
        with ProcessPool(max_workers=1) as pool:
            pool.schedule(function, args=[1])
        self.assertFalse(pool.active)

    def test_process_pool_close_futures(self):
        """Process Pool Fork all futures are performed on close."""
        futures = []
        pool = ProcessPool(max_workers=1)
        for index in range(10):
            futures.append(pool.schedule(function, args=[index]))
        pool.close()
        pool.join()
        map(self.assertTrue, [f.done() for f in futures])

    def test_process_pool_close_stopped(self):
        """Process Pool Fork is stopped after close."""
        pool = ProcessPool(max_workers=1)
        pool.schedule(function, args=[1])
        pool.close()
        pool.join()
        self.assertFalse(pool.active)

    def test_process_pool_stop_futures(self):
        """Process Pool Fork not all futures are performed on stop."""
        futures = []
        pool = ProcessPool(max_workers=1)
        for index in range(10):
            futures.append(pool.schedule(function, args=[index]))
        pool.stop()
        pool.join()
        self.assertTrue(len([f for f in futures if not f.done()]) > 0)

    def test_process_pool_stop_stopped(self):
        """Process Pool Fork is stopped after stop."""
        pool = ProcessPool(max_workers=1)
        pool.schedule(function, args=[1])
        pool.stop()
        pool.join()
        self.assertFalse(pool.active)

    def test_process_pool_stop_stopped_callback(self):
        """Process Pool Fork is stopped in callback."""
        with ProcessPool(max_workers=1) as pool:
            def stop_pool_callback(_):
                pool.stop()

            future = pool.schedule(function, args=[1])
            future.add_done_callback(stop_pool_callback)
            with self.assertRaises(RuntimeError):
                for index in range(10):
                    time.sleep(0.1)
                    pool.schedule(long_function, args=[index])

        self.assertFalse(pool.active)

    def test_process_pool_large_data(self):
        """Process Pool Fork large data is sent on the channel."""
        data = "a" * 1098 * 1024 * 50  # 50 Mb

        with ProcessPool(max_workers=1) as pool:
            future = pool.schedule(
                function, args=[data], kwargs={'keyword_argument': ''})

        self.assertEqual(data, future.result())

    def test_process_pool_stop_large_data(self):
        """Process Pool Fork is stopped if large data is sent on the channel."""
        data = "a" * 1098 * 1024 * 50  # 50 Mb
        pool = ProcessPool(max_workers=1)
        pool.schedule(function, args=[data])
        pool.stop()
        pool.join()

        self.assertFalse(pool.active)

    def test_process_pool_join_workers(self):
        """Process Pool Fork no worker is running after join."""
        pool = ProcessPool(max_workers=4)
        pool.schedule(function, args=[1])
        pool.stop()
        pool.join()
        self.assertEqual(len(pool._pool_manager.worker_manager.workers), 0)

    def test_process_pool_join_running(self):
        """Process Pool Fork RuntimeError is raised if active pool joined."""
        with ProcessPool(max_workers=1) as pool:
            pool.schedule(function, args=[1])
            self.assertRaises(RuntimeError, pool.join)

    def test_process_pool_join_futures_timeout(self):
        """Process Pool Fork TimeoutError is raised if join on long futures."""
        pool = ProcessPool(max_workers=1)
        for _ in range(2):
            pool.schedule(long_function)
        pool.close()
        self.assertRaises(TimeoutError, pool.join, 0.4)
        pool.stop()
        pool.join()

    def test_process_pool_callback_error(self):
        """Process Pool Fork does not stop if error in callback."""
        with ProcessPool(max_workers=1) as pool:
            future = pool.schedule(function, args=[1],
                                   kwargs={'keyword_argument': 1})
            future.add_done_callback(self.callback)
            # sleep enough to ensure callback is run
            time.sleep(0.1)
            pool.schedule(function, args=[1],
                          kwargs={'keyword_argument': 1})

    def test_process_pool_exception_isolated(self):
        """Process Pool Fork an Exception does not affect other futures."""
        with ProcessPool(max_workers=1) as pool:
            future = pool.schedule(error_function)
            try:
                future.result()
            except Exception:
                pass
            future = pool.schedule(function, args=[1],
                                   kwargs={'keyword_argument': 1})
        self.assertEqual(future.result(), 2)

    @unittest.skipIf(os.name == 'nt', "Test won't run on Windows'.")
    def test_process_pool_ignoring_sigterm(self):
        """Process Pool Fork ignored SIGTERM signal are handled on Unix."""
        with ProcessPool(max_workers=1) as pool:
            future = pool.schedule(sigterm_function, timeout=0.2)
            with self.assertRaises(TimeoutError):
                future.result()

    def test_process_pool_expired_worker(self):
        """Process Pool Fork unexpect death of worker raises ProcessExpired."""
        with ProcessPool(max_workers=1) as pool:
            future = pool.schedule(suicide_function)
            self.assertRaises(ProcessExpired, future.result)

    def test_process_pool_map(self):
        """Process Pool Fork map simple."""
        elements = [1, 2, 3]

        with ProcessPool(max_workers=1) as pool:
            future = pool.map(function, elements)
            generator = future.result()
            self.assertEqual(list(generator), elements)

    def test_process_pool_map_empty(self):
        """Process Pool Fork map no elements."""
        elements = []

        with ProcessPool(max_workers=1) as pool:
            future = pool.map(function, elements)
            generator = future.result()
            self.assertEqual(list(generator), elements)

    def test_process_pool_map_single(self):
        """Process Pool Fork map one element."""
        elements = [0]

        with ProcessPool(max_workers=1) as pool:
            future = pool.map(function, elements)
            generator = future.result()
            self.assertEqual(list(generator), elements)

    def test_process_pool_map_multi(self):
        """Process Pool Fork map multiple iterables."""
        expected = (2, 4)

        with ProcessPool(max_workers=1) as pool:
            future = pool.map(function, (1, 2, 3), (1, 2))
            generator = future.result()
            self.assertEqual(tuple(generator), expected)

    def test_process_pool_map_one_chunk(self):
        """Process Pool Fork map chunksize 1."""
        elements = [1, 2, 3]

        with ProcessPool(max_workers=1) as pool:
            future = pool.map(function, elements, chunksize=1)
            generator = future.result()
            self.assertEqual(list(generator), elements)

    def test_process_pool_map_zero_chunk(self):
        """Process Pool Fork map chunksize 0."""
        with ProcessPool(max_workers=1) as pool:
            with self.assertRaises(ValueError):
                pool.map(function, [], chunksize=0)

    def test_process_pool_map_timeout(self):
        """Process Pool Fork map with timeout."""
        raised = []
        elements = [1, 2, 3]

        with ProcessPool(max_workers=1) as pool:
            future = pool.map(long_function, elements, timeout=0.1)
            generator = future.result()
            while True:
                try:
                    next(generator)
                except TimeoutError as error:
                    raised.append(error)
                except StopIteration:
                    break

        self.assertTrue(all((isinstance(e, TimeoutError) for e in raised)))

    def test_process_pool_map_timeout_chunks(self):
        """Process Pool Fork map timeout is assigned per chunk."""
        elements = [0.1]*10

        with ProcessPool(max_workers=1) as pool:
            # it takes 0.5s to process a chunk
            future = pool.map(
                long_function, elements, chunksize=5, timeout=0.8)
            generator = future.result()
            self.assertEqual(list(generator), elements)

    def test_process_pool_map_error(self):
        """Process Pool Fork errors do not stop the iteration."""
        raised = None
        elements = [1, 'a', 3]

        with ProcessPool(max_workers=1) as pool:
            future = pool.map(function, elements)
            generator = future.result()
            while True:
                try:
                    result = next(generator)
                except TypeError as error:
                    raised = error
                except StopIteration:
                    break

        self.assertEqual(result, 3)
        self.assertTrue(isinstance(raised, TypeError))

    def test_process_pool_map_cancel(self):
        """Process Pool Fork cancel iteration."""
        with ProcessPool(max_workers=1) as pool:
            future = pool.map(long_function, range(5))
            generator = future.result()

            self.assertEqual(next(generator), 0)

            future.cancel()

            for _ in range(4):
                with self.assertRaises(CancelledError):
                    next(generator)


# DEADLOCK TESTS


def broken_worker_process_tasks(_, channel):
    """Process failing in receiving new tasks."""
    with channel.mutex.reader:
        os._exit(1)


def broken_worker_process_result(_, channel):
    """Process failing in delivering result."""
    try:
        for _ in pebble.pool.process.worker_get_next_task(channel, 2):
            with channel.mutex.writer:
                os._exit(1)
    except OSError:
        os._exit(1)


@unittest.skipIf(not supported, "Start method is not supported")
class TestProcessPoolDeadlockOnNewFutures(unittest.TestCase):
    def setUp(self):
        self.worker_process = pebble.pool.process.worker_process
        pebble.pool.process.worker_process = broken_worker_process_tasks
        pebble.pool.channel.LOCK_TIMEOUT = 0.1

    def tearDown(self):
        pebble.pool.process.worker_process = self.worker_process
        pebble.pool.channel.LOCK_TIMEOUT = 60

    def test_pool_deadlock_stop(self):
        """Process Pool Fork reading deadlocks are stopping the Pool."""
        with self.assertRaises(RuntimeError):
            pool = pebble.ProcessPool(max_workers=1)
            for _ in range(10):
                pool.schedule(function)
                time.sleep(0.1)


@unittest.skipIf(not supported, "Start method is not supported")
class TestProcessPoolDeadlockOnResult(unittest.TestCase):
    def setUp(self):
        self.worker_process = pebble.pool.process.worker_process
        pebble.pool.process.worker_process = broken_worker_process_result
        pebble.pool.channel.LOCK_TIMEOUT = 0.1

    def tearDown(self):
        pebble.pool.process.worker_process = self.worker_process
        pebble.pool.channel.LOCK_TIMEOUT = 60

    def test_pool_deadlock(self):
        """Process Pool Fork no deadlock if writing worker dies locking channel."""
        with pebble.ProcessPool(max_workers=1) as pool:
            with self.assertRaises(pebble.ProcessExpired):
                pool.schedule(function).result()

    def test_pool_deadlock_stop(self):
        """Process Pool Fork writing deadlocks are stopping the Pool."""
        with self.assertRaises(RuntimeError):
            pool = pebble.ProcessPool(max_workers=1)
            for _ in range(10):
                pool.schedule(function)
                time.sleep(0.1)
