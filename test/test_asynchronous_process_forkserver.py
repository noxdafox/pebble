import os
import time
import pickle
import signal
import asyncio
import unittest
import threading
import multiprocessing
from concurrent.futures import CancelledError, TimeoutError

from pebble import asynchronous, ProcessExpired


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
            raise Exception(mp_context.get_start_method())
    except RuntimeError:  # child process
        pass


def not_decorated(argument, keyword_argument=0):
    return argument + keyword_argument


@asynchronous.process(context=mp_context)
def decorated(argument, keyword_argument=0):
    """A docstring."""
    return argument + keyword_argument


@asynchronous.process(context=mp_context)
def error_decorated():
    raise RuntimeError("BOOM!")


@asynchronous.process(context=mp_context)
def pickling_error_decorated():
    event = threading.Event()
    return event


@asynchronous.process(context=mp_context)
def critical_decorated():
    os._exit(123)


@asynchronous.process(context=mp_context)
def decorated_cancel():
    time.sleep(10)


@asynchronous.process(timeout=0.1, context=mp_context)
def long_decorated():
    time.sleep(10)


@asynchronous.process(timeout=0.1, context=mp_context)
def sigterm_decorated():
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    time.sleep(10)


@asynchronous.process(context=mp_context)
def name_keyword_argument(name='function_kwarg'):
    return name


@asynchronous.process(name='asynchronous_process_name', context=mp_context)
def name_keyword_decorated():
    return multiprocessing.current_process().name


@asynchronous.process(name='decorator_kwarg', context=mp_context)
def name_keyword_decorated_and_argument(name='bar'):
    return (multiprocessing.current_process().name, name)


@asynchronous.process(daemon=False, context=mp_context)
def daemon_keyword_decorated():
    return multiprocessing.current_process().daemon


class ProcessAsynchronousObj:
    a = 0

    def __init__(self):
        self.b = 1

    @classmethod
    @asynchronous.process(context=mp_context)
    def clsmethod(cls):
        return cls.a

    @asynchronous.process(context=mp_context)
    def instmethod(self):
        return self.b

    @staticmethod
    @asynchronous.process(context=mp_context)
    def stcmethod():
        return 2


class ProcessAsynchronousSub1(ProcessAsynchronousObj):
    @classmethod
    @asynchronous.process(context=mp_context)
    def clsmethod(cls):
        return cls.a + 1

    @asynchronous.process(context=mp_context)
    def instmethod(self):
        return self.b + 1

    @staticmethod
    @asynchronous.process(context=mp_context)
    def stcmethod():
        return 2 + 1


class ProcessAsynchronousSub2(ProcessAsynchronousObj):
    @classmethod
    @asynchronous.process(context=mp_context)
    def clsmethod(cls):
        return cls.a + 2

    @asynchronous.process(context=mp_context)
    def instmethod(self):
        return self.b + 2

    @staticmethod
    @asynchronous.process(context=mp_context)
    def stcmethod():
        return 2 + 2


class CallableClass:
    def __call__(self, argument, keyword_argument=0):
        return argument + keyword_argument


@unittest.skipIf(not supported, "Start method is not supported")
class TestProcessAsynchronous(unittest.TestCase):
    def setUp(self):
        self.results = 0
        self.exception = None
        self.event = None
        self.asynchronousobj = ProcessAsynchronousObj()
        self.asynchronousobj1 = ProcessAsynchronousSub1()
        self.asynchronousobj2 = ProcessAsynchronousSub2()

    def callback(self, future):
        try:
            self.results = future.result()
        except (ProcessExpired, RuntimeError, TimeoutError) as error:
            self.exception = error
        finally:
            self.event.set()

    def test_docstring(self):
        """Process Forkserver docstring is preserved."""
        self.assertEqual(decorated.__doc__, "A docstring.")

    def test_wrong_timeout(self):
        """Process Forkserver TypeError is raised if timeout is not number."""
        with self.assertRaises(TypeError):
            @asynchronous.process(timeout='Foo', context=mp_context)
            def function():
                return

    def test_class_method(self):
        """Process Forkserver decorated classmethods."""
        async def test0():
            return await ProcessAsynchronousObj.clsmethod()

        self.assertEqual(asyncio.run(test0()), 0)

        async def test1():
            return await ProcessAsynchronousSub1.clsmethod()

        self.assertEqual(asyncio.run(test1()), 1)

        async def test2():
            return await ProcessAsynchronousSub2.clsmethod()

        self.assertEqual(asyncio.run(test2()), 2)

    def test_instance_method(self):
        """Process Forkserver decorated instance methods."""
        async def test0():
            return await self.asynchronousobj.instmethod()

        self.assertEqual(asyncio.run(test0()), 1)

        async def test1():
            return await self.asynchronousobj1.instmethod()

        self.assertEqual(asyncio.run(test1()), 2)

        async def test2():
            return await self.asynchronousobj2.instmethod()

        self.assertEqual(asyncio.run(test2()), 3)

    def test_not_decorated_results(self):
        """Process Forkserver results are produced."""
        non_decorated = asynchronous.process(not_decorated, context=mp_context)
        async def test():
            return await non_decorated(1, 1)

        self.assertEqual(asyncio.run(test()), 2)

    def test_decorated_results(self):
        """Process Forkserver results are produced."""
        async def test():
            return await decorated(1, 1)

        self.assertEqual(asyncio.run(test()), 2)

    def test_decorated_results_callback(self):
        """Process Forkserver results are forwarded to the callback."""
        async def test():
            self.event = asyncio.Event()
            self.event.clear()

            future = decorated(1, 1)
            future.add_done_callback(self.callback)

            await self.event.wait()

        asyncio.run(test())

        self.assertEqual(self.results, 2)

    def test_error_decorated(self):
        """Process Forkserver errors are raised by future.result."""
        async def test():
            return await error_decorated()

        with self.assertRaises(RuntimeError):
            asyncio.run(test())

    def test_error_decorated_callback(self):
        """Process Forkserver errors are forwarded to callback."""
        async def test():
            self.event = asyncio.Event()
            self.event.clear()

            future = error_decorated()
            future.add_done_callback(self.callback)

            await self.event.wait()

        asyncio.run(test())

        self.assertTrue(isinstance(self.exception, RuntimeError),
                        msg=str(self.exception))

    def test_pickling_error_decorated(self):
        """Process Forkserver pickling errors are raised by future.result."""
        async def test():
            return await pickling_error_decorated()

        with self.assertRaises((pickle.PicklingError, TypeError)):
            asyncio.run(test())

    def test_timeout_decorated(self):
        """Process Forkserver raises TimeoutError if so."""
        async def test():
            return await long_decorated()

        with self.assertRaises(TimeoutError):
            asyncio.run(test())

    def test_timeout_decorated_callback(self):
        """Process Forkserver TimeoutError is forwarded to callback."""
        async def test():
            self.event = asyncio.Event()
            self.event.clear()

            future = long_decorated()
            future.add_done_callback(self.callback)

            await self.event.wait()

        asyncio.run(test())

        self.assertTrue(isinstance(self.exception, TimeoutError),
                        msg=str(self.exception))

    def test_decorated_dead_process(self):
        """Process Forkserver ProcessExpired is raised if process dies."""
        async def test():
            return await critical_decorated()

        with self.assertRaises(ProcessExpired):
            asyncio.run(test())

    def test_timeout_decorated_callback(self):
        """Process Forkserver ProcessExpired is forwarded to callback."""
        async def test():
            self.event = asyncio.Event()
            self.event.clear()

            future = critical_decorated()
            future.add_done_callback(self.callback)

            await self.event.wait()

        asyncio.run(test())

        self.assertTrue(isinstance(self.exception, ProcessExpired),
                        msg=str(self.exception))

    def test_cancel_decorated(self):
        """Process Forkserver raises CancelledError if future was cancelled."""
        async def test():
            future = decorated_cancel()
            future.cancel()

            return await future

        with self.assertRaises(asyncio.CancelledError):
            asyncio.run(test())

    @unittest.skipIf(os.name == 'nt', "Test won't run on Windows.")
    def test_decorated_ignoring_sigterm(self):
        """Process Forkserver Asynchronous ignored SIGTERM signal are handled on Unix."""
        async def test():
            return await sigterm_decorated()

        with self.assertRaises(TimeoutError):
            asyncio.run(test())

    def test_name_keyword_argument(self):
        """name keyword can be passed to a decorated function process without name"""
        async def test():
            return await name_keyword_argument()

        self.assertEqual(asyncio.run(test()), "function_kwarg")

    def test_name_keyword_decorated(self):
        """
        Check that a simple use case of the name keyword passed to the decorator works
        """
        async def test():
            return await name_keyword_decorated()

        self.assertEqual(asyncio.run(test()), "asynchronous_process_name")

    def test_daemon_keyword_decorated(self):
        """Daemon keyword can be passed to a decorated function and spawns correctly."""
        async def test():
            return await daemon_keyword_decorated()

        self.assertEqual(asyncio.run(test()), False)

    def test_callable_objects(self):
        """Callable objects are correctly handled."""
        callable_object = asynchronous.process(context=mp_context)(CallableClass())

        async def test():
            return await callable_object(1)

        self.assertEqual(asyncio.run(test()), 1)
