import asyncio
import unittest
import threading

from pebble import asynchronous


def not_decorated(argument, keyword_argument=0):
    return argument + keyword_argument


@asynchronous.thread
def decorated(argument, keyword_argument=0):
    """A docstring."""
    return argument + keyword_argument


@asynchronous.thread
def error_decorated():
    raise RuntimeError("BOOM!")


@asynchronous.thread()
def name_keyword_argument(name='function_kwarg'):
    return name


@asynchronous.thread(name='asynchronous_thread_name')
def name_keyword_decorated():
    return threading.current_thread().name


@asynchronous.thread(name='decorator_kwarg')
def name_keyword_decorated_and_argument(name='bar'):
    return (threading.current_thread().name, name)


@asynchronous.thread(daemon=False)
def daemon_keyword_decorated():
    return threading.current_thread().daemon


class ThreadAsynchronousObj:
    a = 0

    def __init__(self):
        self.b = 1

    @classmethod
    @asynchronous.thread
    def clsmethod(cls):
        return cls.a

    @asynchronous.thread
    def instmethod(self):
        return self.b

    @staticmethod
    @asynchronous.thread
    def stcmethod():
        return 2


class TestThreadAsynchronous(unittest.TestCase):
    def setUp(self):
        self.results = 0
        self.exception = None
        self.event = None
        self.asynchronousobj = ThreadAsynchronousObj()

    def callback(self, future):
        try:
            self.results = future.result()
        except (RuntimeError) as error:
            self.exception = error
        finally:
            self.event.set()

    def test_docstring(self):
        """Thread  docstring is preserved."""
        self.assertEqual(decorated.__doc__, "A docstring.")

    def test_class_method(self):
        """Thread  decorated classmethods."""
        async def test():
            return await ThreadAsynchronousObj.clsmethod()

        self.assertEqual(asyncio.run(test()), 0)

    def test_instance_method(self):
        """Thread  decorated instance methods."""
        async def test():
            return await self.asynchronousobj.instmethod()

        self.assertEqual(asyncio.run(test()), 1)

    def test_static_method(self):
        """Thread  decorated static methods ( startmethod only)."""
        async def test():
            return await self.asynchronousobj.stcmethod()

        self.assertEqual(asyncio.run(test()), 2)

    def test_not_decorated_results(self):
        """Process Fork results are produced."""
        non_decorated = asynchronous.thread(not_decorated)

        async def test():
            return await non_decorated(1, 1)

        self.assertEqual(asyncio.run(test()), 2)

    def test_decorated_results(self):
        """Thread  results are produced."""
        async def test():
            return await decorated(1, 1)

        self.assertEqual(asyncio.run(test()), 2)

    def test_decorated_results_callback(self):
        """Thread  results are forwarded to the callback."""
        async def test():
            self.event = asyncio.Event()
            self.event.clear()

            future = decorated(1, 1)
            future.add_done_callback(self.callback)

            await self.event.wait()

        asyncio.run(test())

        self.assertEqual(self.results, 2)

    def test_error_decorated(self):
        """Thread  errors are raised by future.result."""
        async def test():
            return await error_decorated()

        with self.assertRaises(RuntimeError):
            asyncio.run(test())

    def test_error_decorated_callback(self):
        """Thread  errors are forwarded to callback."""
        async def test():
            self.event = asyncio.Event()
            self.event.clear()

            future = error_decorated()
            future.add_done_callback(self.callback)

            await self.event.wait()

        asyncio.run(test())

        self.assertTrue(isinstance(self.exception, RuntimeError),
                        msg=str(self.exception))

    def test_name_keyword_argument(self):
        """name keyword can be passed to a decorated function process without name """
        async def test():
            return await name_keyword_argument()

        self.assertEqual(asyncio.run(test()), "function_kwarg")

    def test_name_keyword_decorated(self):
        """
        Check that a simple use case of the name keyword passed to the decorator works
        """
        async def test():
            return await name_keyword_decorated()

        self.assertEqual(asyncio.run(test()), "asynchronous_thread_name")

    def test_name_keyword_decorated_result(self):
        """name kwarg is handled  without modifying the function kwargs"""
        async def test():
            return await name_keyword_decorated_and_argument(
                name="function_kwarg")
        dec_out, fn_out = asyncio.run(test())
        self.assertEqual(dec_out, "decorator_kwarg")
        self.assertEqual(fn_out, "function_kwarg")

    def test_daemon_keyword_decorated(self):
        """Daemon keyword can be passed to a decorated function and spawns correctly."""
        async def test():
            return await daemon_keyword_decorated()

        self.assertEqual(asyncio.run(test()), False)
