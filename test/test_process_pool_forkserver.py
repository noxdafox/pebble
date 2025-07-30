import os
import sys
import time
import pickle
import signal
import asyncio
import unittest
import threading
import concurrent
import dataclasses
import multiprocessing

from concurrent.futures import CancelledError, TimeoutError

import pebble
from pebble import ProcessPool, ProcessExpired
from pebble.pool.base_pool import PoolStatus


# set start method
supported = False
mp_context = None


methods = multiprocessing.get_all_start_methods()
if 'forkserver' in methods:
    try:
        mp_context = multiprocessing.get_context('forkserver')

        if mp_context.get_start_method() == 'forkserver':
            supported = True
        else:
            raise BaseException(mp_context.get_start_method())
    except RuntimeError:  # child process
        pass


initarg = 0


def initializer(value):
    global initarg
    initarg = value


def long_initializer():
    time.sleep(60)


def broken_initializer():
    raise BaseException("BOOM!")


def function(argument, keyword_argument=0):
    """A docstring."""
    return argument + keyword_argument


def initializer_function():
    return initarg


def error_function():
    raise BaseException("BOOM!")


def return_error_function():
    return BaseException("BOOM!")


def pickle_error_function():
    return threading.Lock()


@dataclasses.dataclass(frozen=True)
class FrozenError(Exception):
    pass


def frozen_error_function():
    raise FrozenError()


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
        f = pool.schedule(function, args=[1])

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
        except BaseException as error:
            self.exception = error
        finally:
            self.event.set()

    def test_process_pool_single_future(self):
        """Process Pool Forkserver single future."""
        with ProcessPool(max_workers=1, context=mp_context) as pool:
            future = pool.schedule(function, args=[1],
                                   kwargs={'keyword_argument': 1})
        self.assertEqual(future.result(), 2)

    def test_process_pool_multiple_futures(self):
        """Process Pool Forkserver multiple futures."""
        futures = []
        with ProcessPool(max_workers=1, context=mp_context) as pool:
            for _ in range(5):
                futures.append(pool.schedule(function, args=[1]))
        self.assertEqual(sum([f.result() for f in futures]), 5)

    def test_process_pool_callback(self):
        """Process Pool Forkserver result is forwarded to the callback."""
        with ProcessPool(max_workers=1, context=mp_context) as pool:
            future = pool.schedule(
                function, args=[1], kwargs={'keyword_argument': 1})
        future.add_done_callback(self.callback)
        self.event.wait()
        self.assertEqual(self.result, 2)

    def test_process_pool_error(self):
        """Process Pool Forkserver errors are raised by future get."""
        with ProcessPool(max_workers=1, context=mp_context) as pool:
            future = pool.schedule(error_function)
        self.assertRaises(BaseException, future.result)

    def test_process_pool_error_returned(self):
        """Process Pool Forkserver returned errors are returned by future get."""
        with ProcessPool(max_workers=1, context=mp_context) as pool:
            future = pool.schedule(return_error_function)
        self.assertIsInstance(future.result(), BaseException)

    def test_process_pool_error_callback(self):
        """Process Pool Forkserver errors are forwarded to callback."""
        with ProcessPool(max_workers=1, context=mp_context) as pool:
            future = pool.schedule(error_function)
        future.add_done_callback(self.callback)
        self.event.wait()
        self.assertTrue(isinstance(self.exception, BaseException))

    def test_process_pool_pickling_error_task(self):
        """Process Pool Forkserver task pickling errors
        are raised by future.result."""
        with ProcessPool(max_workers=1, context=mp_context) as pool:
            future = pool.schedule(function, args=[threading.Lock()])
            self.assertRaises((pickle.PicklingError, TypeError), future.result)

    def test_process_pool_pickling_error_result(self):
        """Process Pool Forkserver result pickling errors
        are raised by future.result."""
        with ProcessPool(max_workers=1, context=mp_context) as pool:
            future = pool.schedule(pickle_error_function)
            self.assertRaises((pickle.PicklingError, TypeError), future.result)

    def test_process_pool_frozen_error(self):
        """Process Pool Forkserver frozen errors are raised by future get."""
        with ProcessPool(max_workers=1, context=mp_context) as pool:
            future = pool.schedule(frozen_error_function)
        self.assertRaises(FrozenError, future.result)

    def test_process_pool_timeout(self):
        """Process Pool Forkserver future raises TimeoutError if so."""
        with ProcessPool(max_workers=1, context=mp_context) as pool:
            future = pool.schedule(long_function, timeout=0.1)
        self.assertRaises(TimeoutError, future.result)

    def test_process_pool_timeout_callback(self):
        """Process Pool Forkserver TimeoutError is forwarded to callback."""
        with ProcessPool(max_workers=1, context=mp_context) as pool:
            future = pool.schedule(long_function, timeout=0.1)
        future.add_done_callback(self.callback)
        self.event.wait()
        self.assertTrue(isinstance(self.exception, TimeoutError))

    def test_process_pool_cancel(self):
        """Process Pool Forkserver future raises CancelledError if so."""
        with ProcessPool(max_workers=1, context=mp_context) as pool:
            future = pool.schedule(long_function)
            time.sleep(0.1)  # let the process pick up the task
            self.assertTrue(future.cancel())
        self.assertRaises(CancelledError, future.result)

    def test_process_pool_cancel_callback(self):
        """Process Pool Forkserver CancelledError is forwarded to callback."""
        with ProcessPool(max_workers=1, context=mp_context) as pool:
            future = pool.schedule(long_function)
            future.add_done_callback(self.callback)
            time.sleep(0.1)  # let the process pick up the task
            self.assertTrue(future.cancel())
        self.event.wait()
        self.assertTrue(isinstance(self.exception, CancelledError))

    @unittest.skipIf(sys.platform == 'darwin', "Not supported on MAC OS")
    def test_process_pool_different_process(self):
        """Process Pool Forkserver multiple futures are handled by different processes."""
        futures = []
        with ProcessPool(max_workers=2, context=mp_context) as pool:
            for _ in range(0, 5):
                futures.append(pool.schedule(pid_function))
        self.assertEqual(len(set([f.result() for f in futures])), 2)

    def test_process_pool_future_limit(self):
        """Process Pool Forkserver tasks limit is honored."""
        futures = []
        with ProcessPool(max_workers=1, max_tasks=2, context=mp_context) as pool:
            for _ in range(0, 4):
                futures.append(pool.schedule(pid_function))
        self.assertEqual(len(set([f.result() for f in futures])), 2)

    def test_process_pool_stop_timeout(self):
        """Process Pool Forkserver workers are stopped if future timeout."""
        with ProcessPool(max_workers=1, context=mp_context) as pool:
            future1 = pool.schedule(pid_function)
            pool.schedule(long_function, timeout=0.1)
            future2 = pool.schedule(pid_function)
        self.assertNotEqual(future1.result(), future2.result())

    def test_process_pool_stop_cancel(self):
        """Process Pool Forkserver workers are stopped if future is cancelled."""
        with ProcessPool(max_workers=1, context=mp_context) as pool:
            future1 = pool.schedule(pid_function)
            cancel_future = pool.schedule(long_function)
            time.sleep(0.1)  # let the process pick up the task
            cancel_future.cancel()
            future2 = pool.schedule(pid_function)
        self.assertNotEqual(future1.result(), future2.result())

    def test_process_pool_initializer(self):
        """Process Pool Forkserver initializer is correctly run."""
        with ProcessPool(initializer=initializer, initargs=[1], context=mp_context) as pool:
            future = pool.schedule(initializer_function)
        self.assertEqual(future.result(), 1)

    def test_process_pool_broken_initializer(self):
        """Process Pool Forkserver broken initializer is notified."""
        with self.assertRaises(RuntimeError):
            with ProcessPool(initializer=broken_initializer, context=mp_context) as pool:
                pool.active
                time.sleep(1)
                pool.schedule(function)

    def test_process_pool_running(self):
        """Process Pool Forkserver is active if a future is scheduled."""
        with ProcessPool(max_workers=1, context=mp_context) as pool:
            pool.schedule(function, args=[1])
            self.assertTrue(pool.active)

    def test_process_pool_stopped(self):
        """Process Pool Forkserver is not active once stopped."""
        with ProcessPool(max_workers=1, context=mp_context) as pool:
            pool.schedule(function, args=[1])
        self.assertFalse(pool.active)

    def test_process_pool_close_futures(self):
        """Process Pool Forkserver all futures are performed on close."""
        futures = []
        pool = ProcessPool(max_workers=1, context=mp_context)
        for index in range(10):
            futures.append(pool.schedule(function, args=[index]))
        pool.close()
        pool.join()
        map(self.assertTrue, [f.done() for f in futures])

    def test_process_pool_close_stopped(self):
        """Process Pool Forkserver is stopped after close."""
        pool = ProcessPool(max_workers=1, context=mp_context)
        pool.schedule(function, args=[1])
        pool.close()
        pool.join()
        self.assertFalse(pool.active)

    def test_process_pool_stop_futures(self):
        """Process Pool Forkserver not all futures are performed on stop."""
        futures = []
        pool = ProcessPool(max_workers=1, context=mp_context)
        for index in range(10):
            futures.append(pool.schedule(function, args=[index]))
        pool.stop()
        pool.join()
        self.assertTrue(len([f for f in futures if not f.done()]) > 0)

    def test_process_pool_stop_stopped(self):
        """Process Pool Forkserver is stopped after stop."""
        pool = ProcessPool(max_workers=1, context=mp_context)
        pool.schedule(function, args=[1])
        pool.stop()
        pool.join()
        self.assertFalse(pool.active)

    def test_process_pool_stop_stopped_callback(self):
        """Process Pool Forkserver is stopped in callback."""
        with ProcessPool(max_workers=1, context=mp_context) as pool:
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
        """Process Pool Forkserver large data is sent on the channel."""
        data = "a" * 1098 * 1024 * 100  # 100 Mb

        with ProcessPool(max_workers=1, context=mp_context) as pool:
            future = pool.schedule(
                function, args=[data], kwargs={'keyword_argument': ''})

        self.assertEqual(data, future.result())

    def test_process_pool_stop_large_data(self):
        """Process Pool Forkserver is stopped if large data is sent on the channel."""
        data = "a" * 1098 * 1024 * 100  # 100 Mb
        pool = ProcessPool(max_workers=1, context=mp_context)
        pool.schedule(function, args=[data])
        time.sleep(1)
        pool.stop()
        pool.join()

        self.assertFalse(pool.active)

    def test_process_pool_join_workers(self):
        """Process Pool Forkserver no worker is running after join."""
        pool = ProcessPool(max_workers=4, context=mp_context)
        pool.schedule(function, args=[1])
        pool.stop()
        pool.join()
        self.assertEqual(len(pool._pool_manager.worker_manager.workers), 0)

    def test_process_pool_join_running(self):
        """Process Pool Forkserver RuntimeError is raised if active pool joined."""
        with ProcessPool(max_workers=1, context=mp_context) as pool:
            pool.schedule(function, args=[1])
            self.assertRaises(RuntimeError, pool.join)

    def test_process_pool_join_futures_timeout(self):
        """Process Pool Forkserver TimeoutError is raised if join on long tasks."""
        pool = ProcessPool(max_workers=1, context=mp_context)
        for _ in range(2):
            pool.schedule(long_function)
        pool.close()
        self.assertRaises(TimeoutError, pool.join, 0.4)
        pool.stop()
        pool.join()

    def test_process_pool_callback_error(self):
        """Process Pool Forkserver does not stop if error in callback."""
        with ProcessPool(max_workers=1, context=mp_context) as pool:
            future = pool.schedule(function, args=[1],
                                   kwargs={'keyword_argument': 1})
            future.add_done_callback(self.callback)
            # sleep enough to ensure callback is run
            time.sleep(0.1)
            pool.schedule(function, args=[1],
                          kwargs={'keyword_argument': 1})

    def test_process_pool_exception_isolated(self):
        """Process Pool Forkserver an BaseException does not affect other futures."""
        with ProcessPool(max_workers=1, context=mp_context) as pool:
            future = pool.schedule(error_function)
            try:
                future.result()
            except BaseException:
                pass
            future = pool.schedule(function, args=[1],
                                   kwargs={'keyword_argument': 1})
        self.assertEqual(future.result(), 2)

    @unittest.skipIf(os.name == 'nt', "Test won't run on Windows'.")
    def test_process_pool_ignoring_sigterm(self):
        """Process Pool Forkserver ignored SIGTERM signal are handled on Unix."""
        with ProcessPool(max_workers=1, context=mp_context) as pool:
            future = pool.schedule(sigterm_function, timeout=0.2)
            with self.assertRaises(TimeoutError):
                future.result()

    def test_process_pool_expired_worker(self):
        """Process Pool Forkserver unexpect death of worker raises ProcessExpired."""
        with ProcessPool(max_workers=1, context=mp_context) as pool:
            future = pool.schedule(suicide_function)
            worker_pid = list(pool._pool_manager.worker_manager.workers)[0]
            with self.assertRaises(ProcessExpired) as exc_ctx:
                future.result()
            self.assertEqual(exc_ctx.exception.exitcode, 1)
            self.assertEqual(exc_ctx.exception.pid, worker_pid)

    def test_process_pool_map(self):
        """Process Pool Forkserver map simple."""
        elements = [1, 2, 3]

        with ProcessPool(max_workers=1, context=mp_context) as pool:
            future = pool.map(function, elements)
            generator = future.result()
            self.assertEqual(list(generator), elements)

    def test_process_pool_map_empty(self):
        """Process Pool Forkserver map no elements."""
        elements = []

        with ProcessPool(max_workers=1, context=mp_context) as pool:
            future = pool.map(function, elements)
            generator = future.result()
            self.assertEqual(list(generator), elements)

    def test_process_pool_map_single(self):
        """Process Pool Forkserver map one element."""
        elements = [0]

        with ProcessPool(max_workers=1, context=mp_context) as pool:
            future = pool.map(function, elements)
            generator = future.result()
            self.assertEqual(list(generator), elements)

    def test_process_pool_map_multi(self):
        """Process Pool Forkserver map multiple iterables."""
        expected = (2, 4)

        with ProcessPool(max_workers=1, context=mp_context) as pool:
            future = pool.map(function, (1, 2, 3), (1, 2))
            generator = future.result()
            self.assertEqual(tuple(generator), expected)

    def test_process_pool_map_one_chunk(self):
        """Process Pool Forkserver map chunksize 1."""
        elements = [1, 2, 3]

        with ProcessPool(max_workers=1, context=mp_context) as pool:
            future = pool.map(function, elements, chunksize=1)
            generator = future.result()
            self.assertEqual(list(generator), elements)

    def test_process_pool_map_zero_chunk(self):
        """Process Pool Forkserver map chunksize 0."""
        with ProcessPool(max_workers=1, context=mp_context) as pool:
            with self.assertRaises(ValueError):
                pool.map(function, [], chunksize=0)

    def test_process_pool_map_timeout(self):
        """Process Pool Forkserver map with timeout."""
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
        """Process Pool Forkserver map timeout is assigned per chunk."""
        elements = [0.1]*20

        with ProcessPool(max_workers=1, context=mp_context) as pool:
            # it takes 1s to process a chunk
            future = pool.map(
                long_function, elements, chunksize=5, timeout=1.8)
            generator = future.result()
            self.assertEqual(list(generator), elements)

    def test_process_pool_map_error(self):
        """Process Pool Forkserver errors do not stop the iteration."""
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
        """Process Pool Forkserver cancel iteration."""
        with ProcessPool(max_workers=1, context=mp_context) as pool:
            future = pool.map(long_function, range(5))
            generator = future.result()

            self.assertEqual(next(generator), 0)

            future.cancel()

            for _ in range(4):
                with self.assertRaises(CancelledError):
                    next(generator)

    def test_process_pool_map_broken_pool(self):
        """Process Pool Forkserver Broken Pool."""
        elements = [1, 2, 3]

        with ProcessPool(max_workers=1, context=mp_context) as pool:
            future = pool.map(long_function, elements, timeout=1)
            generator = future.result()
            pool._context.status = PoolStatus.ERROR
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
        """Process Pool Forkserver worker starts process."""
        with ProcessPool(max_workers=1, context=mp_context) as pool:
            future = pool.schedule(process_function)
        self.assertEqual(future.result(), 1)

    def test_process_pool_child_pool(self):
        """Process Pool Forkserver worker starts multiprocessing.Pool."""
        with ProcessPool(max_workers=1, context=mp_context) as pool:
            future = pool.schedule(pool_function)
        self.assertEqual(future.result(), 1)

    def test_process_pool_child_pebble(self):
        """Process Pool Forkserver worker starts pebble.ProcessPool."""
        with ProcessPool(max_workers=1, context=mp_context) as pool:
            future = pool.schedule(pebble_function)
        self.assertEqual(future.result(), 1)

    def test_process_pool_pickle_function(self):
        """Process Pool Forkserver picklable functions."""
        queue = mp_context.Manager().Queue()
        with ProcessPool(max_workers=1, context=mp_context) as pool:
            pool.schedule(queue.put, args=[1])
        self.assertEqual(queue.get(timeout=1), 1)


@unittest.skipIf(not supported, "Start method is not supported")
class TestAsyncIOProcessPool(unittest.TestCase):
    def setUp(self):
        self.event = None
        self.result = None
        self.exception = None

    def callback(self, future):
        try:
            self.result = future.result()
        # asyncio.exception.CancelledError does not inherit from BaseException
        except BaseException as error:
            self.exception = error
        finally:
            self.event.set()

    def test_process_pool_single_future(self):
        """Process Pool Forkserver single future."""
        async def test(pool):
            loop = asyncio.get_running_loop()

            return await loop.run_in_executor(pool, function, None, 1)

        with ProcessPool(max_workers=1, context=mp_context) as pool:
            self.assertEqual(asyncio.run(test(pool)), 1)

    def test_process_pool_multiple_futures(self):
        """Process Pool Forkserver multiple futures."""
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
        """Process Pool Forkserver result is forwarded to the callback."""
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
        """Process Pool Forkserver errors are raised by future get."""
        async def test(pool):
            loop = asyncio.get_running_loop()

            return await loop.run_in_executor(pool, error_function, None)

        with ProcessPool(max_workers=1, context=mp_context) as pool:
            with self.assertRaises(BaseException):
                asyncio.run(test(pool))

    def test_process_pool_error_returned(self):
        """Process Pool Forkserver returned errors are returned by future get."""
        async def test(pool):
            loop = asyncio.get_running_loop()

            return await loop.run_in_executor(pool, return_error_function, None)

        with ProcessPool(max_workers=1, context=mp_context) as pool:
            self.assertIsInstance(asyncio.run(test(pool)), BaseException)

    def test_process_pool_error_callback(self):
        """Process Pool Forkserver errors are forwarded to callback."""
        async def test(pool):
            loop = asyncio.get_running_loop()

            self.event = asyncio.Event()
            self.event.clear()

            future = loop.run_in_executor(pool, error_function, None)
            future.add_done_callback(self.callback)

            await self.event.wait()

        with ProcessPool(max_workers=1, context=mp_context) as pool:
            asyncio.run(test(pool))
            self.assertTrue(isinstance(self.exception, BaseException))

    def test_process_pool_timeout(self):
        """Process Pool Forkserver future raises TimeoutError if so."""
        async def test(pool):
            loop = asyncio.get_running_loop()

            return await loop.run_in_executor(pool, long_function, 0.1)

        with ProcessPool(max_workers=1, context=mp_context) as pool:
            with self.assertRaises(asyncio.TimeoutError):
                asyncio.run(test(pool))

    def test_process_pool_timeout_callback(self):
        """Process Pool Forkserver TimeoutError is forwarded to callback."""
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
        """Process Pool Forkserver future raises CancelledError if so."""
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
        """Process Pool Forkserver CancelledError is forwarded to callback."""
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
        """Process Pool Forkserver workers are stopped if future timeout."""
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
        """Process Pool Forkserver workers are stopped if future is cancelled."""
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
        pebble.CONSTS.channel_lock_timeout = 0.1

    def tearDown(self):
        pebble.pool.process.worker_process = self.worker_process
        pebble.CONSTS.channel_lock_timeout = 60

    def test_pool_deadlock_stop(self):
        """Process Pool Forkserver reading deadlocks are stopping the Pool."""
        with self.assertRaises(RuntimeError):
            pool = pebble.ProcessPool(max_workers=1, context=mp_context)
            for _ in range(10):
                pool.schedule(function)
                time.sleep(0.2)


@unittest.skipIf(not supported, "Start method is not supported")
class TestProcessPoolDeadlockOnResult(unittest.TestCase):
    def setUp(self):
        self.worker_process = pebble.pool.process.worker_process
        pebble.pool.process.worker_process = broken_worker_process_result
        pebble.CONSTS.channel_lock_timeout = 0.1

    def tearDown(self):
        pebble.pool.process.worker_process = self.worker_process
        pebble.CONSTS.channel_lock_timeout = 60

    def test_pool_deadlock(self):
        """Process Pool Forkserver no deadlock if writing worker dies locking channel."""
        with pebble.ProcessPool(max_workers=1, context=mp_context) as pool:
            with self.assertRaises(pebble.ProcessExpired):
                pool.schedule(function).result()


@unittest.skipIf(not supported, "Start method is not supported")
class TestProcessPoolDeadlockOnCancelLargeData(unittest.TestCase):
    def test_pool_deadlock_stop_cancel(self):
        """Process Pool Forkserver is stopped when futures are cancelled on large data."""
        data = b'A' * 1024 * 1024 * 100

        with pebble.ProcessPool() as pool:
            futures = [pool.schedule(function, args=[data]) for _ in range(10)]
            concurrent.futures.wait(
                futures,
                return_when=concurrent.futures.FIRST_COMPLETED
            )
            for f in futures:
                f.cancel()
            pool.stop()
