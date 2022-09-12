import os
import sys
import time
import pickle
import signal
import asyncio
import unittest
import threading
import multiprocessing

from concurrent.futures import CancelledError, TimeoutError

import pebble
from pebble import ProcessPool, ProcessExpired
from pebble.pool.base_pool import ERROR


# set start method
supported = False
mp_context = None


methods = multiprocessing.get_all_start_methods()
if 'spawn' in methods:
    try:
        mp_context = multiprocessing.get_context('spawn')

        if mp_context.get_start_method() == 'spawn':
            supported = True
    except RuntimeError:  # child process
        pass


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


def pickle_error_function():
    return threading.Lock()


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


def process_function():
    p = multiprocessing.Process(target=function, args=[1])
    p.start()
    p.join()

    return 1


def pool_function():
    pool = multiprocessing.Pool(1)
    result = pool.apply(function, args=[1])
    pool.close()
    pool.join()

    return result


def pebble_function():
    with ProcessPool(max_workers=1) as pool:
        f = pool.submit(function, None, 1)

    return f.result()


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
        """Process Pool Spawn single future."""
        with ProcessPool(max_workers=1, context=mp_context) as pool:
            future = pool.submit(function, None, 1, keyword_argument=1)
        self.assertEqual(future.result(), 2)

    def test_process_pool_multiple_futures(self):
        """Process Pool Spawn multiple futures."""
        futures = []
        with ProcessPool(max_workers=1, context=mp_context) as pool:
            for _ in range(5):
                futures.append(pool.submit(function, None, 1))
        self.assertEqual(sum([f.result() for f in futures]), 5)

    def test_process_pool_callback(self):
        """Process Pool Spawn result is forwarded to the callback."""
        with ProcessPool(max_workers=1, context=mp_context) as pool:
            future = pool.submit(function, None, 1, keyword_argument=1)
        future.add_done_callback(self.callback)
        self.event.wait()
        self.assertEqual(self.result, 2)

    def test_process_pool_error(self):
        """Process Pool Spawn errors are raised by future get."""
        with ProcessPool(max_workers=1, context=mp_context) as pool:
            future = pool.submit(error_function, None)
        self.assertRaises(Exception, future.result)

    def test_process_pool_error_callback(self):
        """Process Pool Spawn errors are forwarded to callback."""
        with ProcessPool(max_workers=1, context=mp_context) as pool:
            future = pool.submit(error_function, None)
        future.add_done_callback(self.callback)
        self.event.wait()
        self.assertTrue(isinstance(self.exception, Exception))

    def test_process_pool_pickling_error_task(self):
        """Process Pool Spawn task pickling errors
        are raised by future.result."""
        with ProcessPool(max_workers=1, context=mp_context) as pool:
            future = pool.submit(function, None, threading.Lock())
            self.assertRaises((pickle.PicklingError, TypeError), future.result)

    def test_process_pool_pickling_error_result(self):
        """Process Pool Spawn result pickling errors
        are raised by future.result."""
        with ProcessPool(max_workers=1, context=mp_context) as pool:
            future = pool.submit(pickle_error_function, None)
            self.assertRaises((pickle.PicklingError, TypeError), future.result)

    def test_process_pool_timeout(self):
        """Process Pool Spawn future raises TimeoutError if so."""
        with ProcessPool(max_workers=1, context=mp_context) as pool:
            future = pool.submit(long_function, 0.1)
        self.assertRaises(TimeoutError, future.result)

    def test_process_pool_timeout_callback(self):
        """Process Pool Spawn TimeoutError is forwarded to callback."""
        with ProcessPool(max_workers=1, context=mp_context) as pool:
            future = pool.submit(long_function, 0.1)
        future.add_done_callback(self.callback)
        self.event.wait()
        self.assertTrue(isinstance(self.exception, TimeoutError))

    def test_process_pool_cancel(self):
        """Process Pool Spawn future raises CancelledError if so."""
        with ProcessPool(max_workers=1, context=mp_context) as pool:
            future = pool.submit(long_function, None)
            time.sleep(0.1)  # let the process pick up the task
            self.assertTrue(future.cancel())
        self.assertRaises(CancelledError, future.result)

    def test_process_pool_cancel_callback(self):
        """Process Pool Spawn CancelledError is forwarded to callback."""
        with ProcessPool(max_workers=1, context=mp_context) as pool:
            future = pool.submit(long_function, None)
            future.add_done_callback(self.callback)
            time.sleep(0.1)  # let the process pick up the task
            self.assertTrue(future.cancel())
        self.event.wait()
        self.assertTrue(isinstance(self.exception, CancelledError))

    @unittest.skipIf(sys.platform == 'darwin', "Not supported on MAC OS")
    def test_process_pool_different_process(self):
        """Process Pool Spawn futures are handled by different processes."""
        futures = []
        with ProcessPool(max_workers=2, context=mp_context) as pool:
            for _ in range(0, 5):
                futures.append(pool.submit(pid_function, None))
        self.assertEqual(len(set([f.result() for f in futures])), 2)

    def test_process_pool_future_limit(self):
        """Process Pool Spawn tasks limit is honored."""
        futures = []
        with ProcessPool(max_workers=1, max_tasks=2, context=mp_context) as pool:
            for _ in range(0, 4):
                futures.append(pool.submit(pid_function, None))
        self.assertEqual(len(set([f.result() for f in futures])), 2)

    def test_process_pool_stop_timeout(self):
        """Process Pool Spawn workers are stopped if future timeout."""
        with ProcessPool(max_workers=1, context=mp_context) as pool:
            future1 = pool.submit(pid_function, None)
            pool.submit(long_function, 0.1)
            future2 = pool.submit(pid_function, None)
        self.assertNotEqual(future1.result(), future2.result())

    def test_process_pool_stop_cancel(self):
        """Process Pool Spawn workers are stopped if future is cancelled."""
        with ProcessPool(max_workers=1, context=mp_context) as pool:
            future1 = pool.submit(pid_function, None)
            cancel_future = pool.submit(long_function, None)
            time.sleep(0.1)  # let the process pick up the task
            cancel_future.cancel()
            future2 = pool.submit(pid_function, None)
        self.assertNotEqual(future1.result(), future2.result())

    def test_process_pool_initializer(self):
        """Process Pool Spawn initializer is correctly run."""
        with ProcessPool(initializer=initializer, initargs=[1], context=mp_context) as pool:
            future = pool.submit(initializer_function, None)
        self.assertEqual(future.result(), 1)

    def test_process_pool_broken_initializer(self):
        """Process Pool Spawn broken initializer is notified."""
        with self.assertRaises(RuntimeError):
            with ProcessPool(initializer=broken_initializer, context=mp_context) as pool:
                pool.active
                time.sleep(2)
                pool.submit(function, None)

    def test_process_pool_running(self):
        """Process Pool Spawn is active if a future is scheduled."""
        with ProcessPool(max_workers=1, context=mp_context) as pool:
            pool.submit(function, None, 1)
            self.assertTrue(pool.active)

    def test_process_pool_stopped(self):
        """Process Pool Spawn is not active once stopped."""
        with ProcessPool(max_workers=1, context=mp_context) as pool:
            pool.submit(function, None, 1)
        self.assertFalse(pool.active)

    def test_process_pool_close_futures(self):
        """Process Pool Spawn all futures are performed on close."""
        futures = []
        pool = ProcessPool(max_workers=1, context=mp_context)
        for index in range(10):
            futures.append(pool.submit(function, None, index))
        pool.close()
        pool.join()
        map(self.assertTrue, [f.done() for f in futures])

    def test_process_pool_close_stopped(self):
        """Process Pool Spawn is stopped after close."""
        pool = ProcessPool(max_workers=1, context=mp_context)
        pool.submit(function, None, 1)
        pool.close()
        pool.join()
        self.assertFalse(pool.active)

    def test_process_pool_stop_futures(self):
        """Process Pool Spawn not all futures are performed on stop."""
        futures = []
        pool = ProcessPool(max_workers=1, context=mp_context)
        for index in range(10):
            futures.append(pool.submit(function, None, index))
        pool.stop()
        pool.join()
        self.assertTrue(len([f for f in futures if not f.done()]) > 0)

    def test_process_pool_stop_stopped(self):
        """Process Pool Spawn is stopped after stop."""
        pool = ProcessPool(max_workers=1, context=mp_context)
        pool.submit(function, None, 1)
        pool.stop()
        pool.join()
        self.assertFalse(pool.active)

    def test_process_pool_stop_stopped_callback(self):
        """Process Pool Spawn is stopped in callback."""
        with ProcessPool(max_workers=1, context=mp_context) as pool:
            def stop_pool_callback(_):
                pool.stop()

            future = pool.submit(function, None, 1)
            future.add_done_callback(stop_pool_callback)
            with self.assertRaises(RuntimeError):
                for index in range(10):
                    time.sleep(0.1)
                    pool.submit(long_function, None, index)

        self.assertFalse(pool.active)

    def test_process_pool_large_data(self):
        """Process Pool Spawn large data is sent on the channel."""
        data = "a" * 1098 * 1024 * 100  # 100 Mb

        with ProcessPool(max_workers=1, context=mp_context) as pool:
            future = pool.submit(function, None, data, keyword_argument='')

        self.assertEqual(data, future.result())

    def test_process_pool_stop_large_data(self):
        """Process Pool Spawn stopped if large data is sent on the channel."""
        data = "a" * 1098 * 1024 * 100  # 100 Mb
        pool = ProcessPool(max_workers=1, context=mp_context)
        pool.submit(function, None, data)
        time.sleep(1)
        pool.stop()
        pool.join()

        self.assertFalse(pool.active)

    def test_process_pool_join_workers(self):
        """Process Pool Spawn no worker is running after join."""
        pool = ProcessPool(max_workers=4, context=mp_context)
        pool.submit(function, None, 1)
        pool.stop()
        pool.join()
        self.assertEqual(len(pool._pool_manager.worker_manager.workers), 0)

    def test_process_pool_join_running(self):
        """Process Pool Spawn RuntimeError is raised if active pool joined."""
        with ProcessPool(max_workers=1, context=mp_context) as pool:
            pool.submit(function, None, 1)
            self.assertRaises(RuntimeError, pool.join)

    def test_process_pool_join_futures_timeout(self):
        """Process Pool Spawn TimeoutError is raised if join on long tasks."""
        pool = ProcessPool(max_workers=1, context=mp_context)
        for _ in range(2):
            pool.submit(long_function, None)
        pool.close()
        self.assertRaises(TimeoutError, pool.join, 0.4)
        pool.stop()
        pool.join()

    def test_process_pool_callback_error(self):
        """Process Pool Spawn does not stop if error in callback."""
        with ProcessPool(max_workers=1, context=mp_context) as pool:
            future = pool.submit(function, None, 1, keyword_argument=1)
            future.add_done_callback(self.callback)
            # sleep enough to ensure callback is run
            time.sleep(0.1)
            pool.submit(function, None, 1, keyword_argument=1)

    def test_process_pool_exception_isolated(self):
        """Process Pool Spawn an Exception does not affect other futures."""
        with ProcessPool(max_workers=1, context=mp_context) as pool:
            future = pool.submit(error_function, None)
            try:
                future.result()
            except Exception:
                pass
            future = pool.submit(function, None, 1, keyword_argument=1)
        self.assertEqual(future.result(), 2)

    @unittest.skipIf(os.name == 'nt', "Test won't run on Windows'.")
    def test_process_pool_ignoring_sigterm(self):
        """Process Pool Spawn ignored SIGTERM signal are handled on Unix."""
        with ProcessPool(max_workers=1, context=mp_context) as pool:
            future = pool.submit(sigterm_function, 0.2)
            with self.assertRaises(TimeoutError):
                future.result()

    def test_process_pool_expired_worker(self):
        """Process Pool Spawn unexpect death of worker raises ProcessExpired."""
        with ProcessPool(max_workers=1, context=mp_context) as pool:
            future = pool.submit(suicide_function, None)
            self.assertRaises(ProcessExpired, future.result)

    def test_process_pool_map(self):
        """Process Pool Spawn map simple."""
        elements = [1, 2, 3]

        with ProcessPool(max_workers=1, context=mp_context) as pool:
            future = pool.map(function, elements)
            generator = future.result()
            self.assertEqual(list(generator), elements)

    def test_process_pool_map_empty(self):
        """Process Pool Spawn map no elements."""
        elements = []

        with ProcessPool(max_workers=1, context=mp_context) as pool:
            future = pool.map(function, elements)
            generator = future.result()
            self.assertEqual(list(generator), elements)

    def test_process_pool_map_single(self):
        """Process Pool Spawn map one element."""
        elements = [0]

        with ProcessPool(max_workers=1, context=mp_context) as pool:
            future = pool.map(function, elements)
            generator = future.result()
            self.assertEqual(list(generator), elements)

    def test_process_pool_map_multi(self):
        """Process Pool Spawn map multiple iterables."""
        expected = (2, 4)

        with ProcessPool(max_workers=1, context=mp_context) as pool:
            future = pool.map(function, (1, 2, 3), (1, 2))
            generator = future.result()
            self.assertEqual(tuple(generator), expected)

    def test_process_pool_map_one_chunk(self):
        """Process Pool Spawn map chunksize 1."""
        elements = [1, 2, 3]

        with ProcessPool(max_workers=1, context=mp_context) as pool:
            future = pool.map(function, elements, chunksize=1)
            generator = future.result()
            self.assertEqual(list(generator), elements)

    def test_process_pool_map_zero_chunk(self):
        """Process Pool Spawn map chunksize 0."""
        with ProcessPool(max_workers=1, context=mp_context) as pool:
            with self.assertRaises(ValueError):
                pool.map(function, [], chunksize=0)

    def test_process_pool_map_timeout(self):
        """Process Pool Spawn map with timeout."""
        raised = []
        elements = [1, 2, 3]

        with ProcessPool(max_workers=1, context=mp_context) as pool:
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
        """Process Pool Spawn map timeout is assigned per chunk."""
        elements = [0.1]*20

        with ProcessPool(max_workers=1, context=mp_context) as pool:
            # it takes 1s to process a chunk
            future = pool.map(
                long_function, elements, chunksize=5, timeout=1.8)
            generator = future.result()
            self.assertEqual(list(generator), elements)

    def test_process_pool_map_error(self):
        """Process Pool Spawn errors do not stop the iteration."""
        raised = None
        elements = [1, 'a', 3]

        with ProcessPool(max_workers=1, context=mp_context) as pool:
            future = pool.map(function, elements)
            generator = future.result()
            while True:
                try:
                    next(generator)
                except TypeError as error:
                    raised = error
                except StopIteration:
                    break

        self.assertTrue(isinstance(raised, TypeError))

    def test_process_pool_map_cancel(self):
        """Process Pool Spawn cancel iteration."""
        with ProcessPool(max_workers=1, context=mp_context) as pool:
            future = pool.map(long_function, range(5))
            generator = future.result()

            self.assertEqual(next(generator), 0)

            future.cancel()

            for _ in range(4):
                with self.assertRaises(CancelledError):
                    next(generator)

    def test_process_pool_map_broken_pool(self):
        """Process Pool Spawn Broken Pool."""
        elements = [1, 2, 3]

        with ProcessPool(max_workers=1, context=mp_context) as pool:
            future = pool.map(long_function, elements, timeout=1)
            generator = future.result()
            pool._context.state = ERROR
            while True:
                try:
                    next(generator)
                except TimeoutError as error:
                    self.assertFalse(pool.active)
                    future.cancel()
                    break
                except StopIteration:
                    break

    def test_process_pool_child_process(self):
        """Process Pool Spawn worker starts process."""
        with ProcessPool(max_workers=1, context=mp_context) as pool:
            future = pool.submit(process_function, None)
        self.assertEqual(future.result(), 1)

    def test_process_pool_child_pool(self):
        """Process Pool Spawn worker starts multiprocessing.Pool."""
        with ProcessPool(max_workers=1, context=mp_context) as pool:
            future = pool.submit(pool_function, None)
        self.assertEqual(future.result(), 1)

    def test_process_pool_child_pebble(self):
        """Process Pool Spawn worker starts pebble.ProcessPool."""
        with ProcessPool(max_workers=1, context=mp_context) as pool:
            future = pool.submit(pebble_function, None)
        self.assertEqual(future.result(), 1)


@unittest.skipIf(not supported, "Start method is not supported")
class TestAsyncIOProcessPool(unittest.TestCase):
    def setUp(self):
        self.event = None
        self.result = None
        self.exception = None

    def callback(self, future):
        try:
            self.result = future.result()
        # asyncio.exception.CancelledError does not inherit from Exception
        except BaseException as error:
            self.exception = error
        finally:
            self.event.set()

    def test_process_pool_single_future(self):
        """Process Pool Fork single future."""
        async def test(pool):
            loop = asyncio.get_running_loop()

            return await loop.run_in_executor(pool, function, None, 1)

        with ProcessPool(max_workers=1, context=mp_context) as pool:
            self.assertEqual(asyncio.run(test(pool)), 1)

    def test_process_pool_multiple_futures(self):
        """Process Pool Fork multiple futures."""
        async def test(pool):
            futures = []
            loop = asyncio.get_running_loop()

            for _ in range(5):
                futures.append(loop.run_in_executor(pool, function, None, 1))

            return await asyncio.wait(futures)

        with ProcessPool(max_workers=2, context=mp_context) as pool:
            self.assertEqual(sum(r.result()
                                 for r in asyncio.run(test(pool))[0]), 5)

    def test_process_pool_callback(self):
        """Process Pool Fork result is forwarded to the callback."""
        async def test(pool):
            loop = asyncio.get_running_loop()

            self.event = asyncio.Event()
            self.event.clear()

            future = loop.run_in_executor(pool, function, None, 1)
            future.add_done_callback(self.callback)

            await self.event.wait()

        with ProcessPool(max_workers=1, context=mp_context) as pool:
            asyncio.run(test(pool))
            self.assertEqual(self.result, 1)

    def test_process_pool_error(self):
        """Process Pool Fork errors are raised by future get."""
        async def test(pool):
            loop = asyncio.get_running_loop()

            return await loop.run_in_executor(pool, error_function, None)

        with ProcessPool(max_workers=1, context=mp_context) as pool:
            with self.assertRaises(Exception):
                asyncio.run(test(pool))

    def test_process_pool_error_callback(self):
        """Process Pool Fork errors are forwarded to callback."""
        async def test(pool):
            loop = asyncio.get_running_loop()

            self.event = asyncio.Event()
            self.event.clear()

            future = loop.run_in_executor(pool, error_function, None)
            future.add_done_callback(self.callback)

            await self.event.wait()

        with ProcessPool(max_workers=1, context=mp_context) as pool:
            asyncio.run(test(pool))
            self.assertTrue(isinstance(self.exception, Exception))

    def test_process_pool_timeout(self):
        """Process Pool Fork future raises TimeoutError if so."""
        async def test(pool):
            loop = asyncio.get_running_loop()

            return await loop.run_in_executor(pool, long_function, 0.1)

        with ProcessPool(max_workers=1, context=mp_context) as pool:
            with self.assertRaises(asyncio.TimeoutError):
                asyncio.run(test(pool))

    def test_process_pool_timeout_callback(self):
        """Process Pool Fork TimeoutError is forwarded to callback."""
        async def test(pool):
            loop = asyncio.get_running_loop()

            self.event = asyncio.Event()
            self.event.clear()

            future = loop.run_in_executor(pool, long_function, 0.1)
            future.add_done_callback(self.callback)

            await asyncio.sleep(0.1) # let the process pick up the task

            await self.event.wait()

        with ProcessPool(max_workers=1, context=mp_context) as pool:
            asyncio.run(test(pool))
            self.assertTrue(isinstance(self.exception, asyncio.TimeoutError))

    def test_process_pool_cancel(self):
        """Process Pool Fork future raises CancelledError if so."""
        async def test(pool):
            loop = asyncio.get_running_loop()
            future = loop.run_in_executor(pool, long_function, None)

            await asyncio.sleep(0.1) # let the process pick up the task

            self.assertTrue(future.cancel())

            return await future

        with ProcessPool(max_workers=1, context=mp_context) as pool:
            with self.assertRaises(asyncio.CancelledError):
                asyncio.run(test(pool))

    def test_process_pool_cancel_callback(self):
        """Process Pool Fork CancelledError is forwarded to callback."""
        async def test(pool):
            loop = asyncio.get_running_loop()

            self.event = asyncio.Event()
            self.event.clear()

            future = loop.run_in_executor(pool, long_function, None)
            future.add_done_callback(self.callback)

            await asyncio.sleep(0.1) # let the process pick up the task

            self.assertTrue(future.cancel())

            await self.event.wait()

        with ProcessPool(max_workers=1, context=mp_context) as pool:
            asyncio.run(test(pool))
            self.assertTrue(isinstance(self.exception, asyncio.CancelledError))

    def test_process_pool_stop_timeout(self):
        """Process Pool Fork workers are stopped if future timeout."""
        async def test(pool):
            loop = asyncio.get_running_loop()
            future1 = loop.run_in_executor(pool, pid_function, None)
            with self.assertRaises(asyncio.TimeoutError):
                await loop.run_in_executor(pool, long_function, 0.1)
            future2 = loop.run_in_executor(pool, pid_function, None)

            self.assertNotEqual(await future1, await future2)

        with ProcessPool(max_workers=1, context=mp_context) as pool:
            asyncio.run(test(pool))

    def test_process_pool_stop_cancel(self):
        """Process Pool Fork workers are stopped if future is cancelled."""
        async def test(pool):
            loop = asyncio.get_running_loop()
            future1 = loop.run_in_executor(pool, pid_function, None)
            cancel_future = loop.run_in_executor(pool, long_function, None)
            await asyncio.sleep(0.1)  # let the process pick up the task
            self.assertTrue(cancel_future.cancel())
            future2 = loop.run_in_executor(pool, pid_function, None)

            self.assertNotEqual(await future1, await future2)

        with ProcessPool(max_workers=1, context=mp_context) as pool:
            asyncio.run(test(pool))
