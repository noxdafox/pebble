import os
import time
import unittest
import threading
try:  # Python 2
    from Queue import Queue
except:  # Python 3
    from queue import Queue

from pebble import TimeoutError, TaskCancelled
from pebble import synchronized, thread, thread_pool, process, process_pool


lock = threading.Lock()
event = threading.Event()
initarg = 0
results = 0
exception = None


def callback(task):
    global results
    results = task.get()
    event.set()


def error_callback(task):
    global exception
    exception = task.get()
    event.set()


def initializer(value):
    global initarg
    initarg = value


def initializer_error(value):
    raise Exception("BOOM!")


@synchronized(lock)
def function():
    """A docstring."""
    return lock.acquire(False)


@thread
def tjob(argument, keyword_argument=0):
    return argument + keyword_argument


@thread
def tjob_error(argument, keyword_argument=0):
    raise Exception("BOOM!")


@thread(callback=callback)
def tjob_callback(argument, keyword_argument=0):
    return argument + keyword_argument


@thread
def tjob_long():
    time.sleep(1)
    return 1


@thread
def tjob_count():
    return None


@thread_pool
def tjob_pool_single(argument, keyword_argument=0):
    return argument + keyword_argument


@thread_pool(workers=2, callback=callback)
def tjob_pool(argument, keyword_argument=0):
    time.sleep(0.01)
    return argument + keyword_argument, threading.current_thread()


@thread_pool(workers=2, callback=callback)
def tjob_pool_long(argument, keyword_argument=0):
    time.sleep(1)
    return argument + keyword_argument, threading.current_thread()


@thread_pool(workers=2, queue=Queue, queueargs=(5, ))
def tjob_pool_queue(argument, keyword_argument=0):
    return argument + keyword_argument, threading.current_thread()


@thread_pool(workers=2)
def tjob_pool_dyn_queue(argument, keyword_argument=0):
    time.sleep(1)
    return argument + keyword_argument, threading.current_thread()


@thread_pool(workers=2, initializer=initializer, initargs=(1, ))
def tjob_pool_init(argument, keyword_argument=0):
    return argument + keyword_argument + initarg


@thread_pool(workers=2, initializer=initializer_error, initargs=(1, ))
def tjob_pool_init_error(argument, keyword_argument=0):
    return argument + keyword_argument + initarg


@process
def pjob(argument, keyword_argument=0):
    return argument + keyword_argument


@process
def pjob_error(argument, keyword_argument=0):
    raise Exception("BOOM!")


@process(callback=callback)
def pjob_callback(argument, keyword_argument=0):
    return argument + keyword_argument


@process
def pjob_long():
    time.sleep(1)
    return 1


@process
def pjob_count():
    return None


@process(timeout=1)
def pjob_timeout():
    time.sleep(2)
    return 1


@process(timeout=2)
def pjob_long_timeout():
    return 1


@process
def pjob_unserializeable():
    return threading.Lock()


@process
def pjob_unserializeable_error():
    raise UnserializeableError()


@process_pool
def pjob_pool_single(argument, keyword_argument=0):
    return argument + keyword_argument


@process_pool(workers=2, callback=callback)
def pjob_pool(argument, keyword_argument=0):
    return argument + keyword_argument, os.getpid()


@process_pool(workers=2, callback=callback)
def pjob_pool_long(argument, keyword_argument=0):
    time.sleep(1)
    return argument + keyword_argument, os.getpid()


@process_pool(workers=2, queue=Queue, queueargs=(5, ))
def pjob_pool_queue(argument, keyword_argument=0):
    return argument + keyword_argument, os.getpid()


@process_pool(workers=2)
def pjob_pool_dyn_queue(argument, keyword_argument=0):
    time.sleep(1)
    return argument + keyword_argument, os.getpid()


@process_pool(workers=2, initializer=initializer, initargs=(1, ))
def pjob_pool_init(argument, keyword_argument=0):
    return argument + keyword_argument + initarg


@process_pool(workers=2, initializer=initializer_error, initargs=(1, ))
def pjob_pool_init_error(argument, keyword_argument=0):
    return argument + keyword_argument + initarg


class ThreadClass(object):
    class_value = 1

    def __init__(self):
        self.value = 1

    @thread
    def tjob(self, argument, keyword_argument=1):
        return self.value + argument + keyword_argument

    @thread_pool
    def tjob_pool(self, argument, keyword_argument=1):
        return self.value + argument + keyword_argument


class ProcessClass(object):
    class_value = 1

    def __init__(self):
        self.value = 1

    @process
    def pjob(self, argument, keyword_argument=1):
        return self.value + argument + keyword_argument

    @process_pool
    def pjob_pool(self, argument, keyword_argument=1):
        return self.value + argument + keyword_argument


class UnserializeableError(Exception):
    def __init__(self):
        self.lock = threading.Lock()  # unpickleable

    def __str__(self):
        return "BOOM!"


class TestSynchronizedDecorator(unittest.TestCase):
    def test_wrapper_decorator_docstring(self):
        """Synchronized docstring of the original function is preserved."""
        self.assertEqual(function.__doc__, "A docstring.")

    def test_syncronized_locked(self):
        """Synchronized Lock is acquired
        during execution of decorated function."""
        self.assertFalse(function())

    def test_syncronized_released(self):
        """Synchronized Lock is acquired
        during execution of decorated function."""
        function()
        self.assertTrue(lock.acquire(False))
        lock.release()


class TestThreadDecorators(unittest.TestCase):
    def setUp(self):
        global results
        results = 0
        event.clear()
        self.exception = None
        self.callbackresults = 0
        self.threadclass = ThreadClass()

    def callback(self, task):
        self.callbackresults = task.get()
        event.set()

    def error_callback(self, task):
        try:
            task.get()
        except Exception as error:
            self.exception = error
        event.set()

    def test_thread_wrong_decoration(self):
        """ThreadDecorator raises ValueError if given wrong params."""
        try:
            @thread(callback, 5)
            def wrong(argument, keyword_argument=0):
                return argument + keyword_argument
        except Exception as error:
            self.assertTrue(isinstance(error, ValueError))

    def test_class_decorated_method(self):
        """ThreadDecorator decorated method."""
        task = self.threadclass.tjob(1)
        self.assertEqual(task.get(), 3)

    def test_thread_callback_static(self):
        """ThreadDecorator static callback is executed with thread task."""
        tjob_callback(1, 1)
        event.wait()
        self.assertEqual(2, results)

    def test_thread_callback_dynamic(self):
        """ThreadDecorator dynamic callback is executed with thread task."""
        tjob.callback = self.callback
        tjob(1, 1)
        event.wait()
        self.assertEqual(2, self.callbackresults)

    def test_thread_error_callback(self):
        """ThreadDecorator an exception in a task
        is managed in error_callback."""
        tjob_error.callback = self.error_callback
        tjob_error(1, 1)
        event.wait()
        self.assertEqual('BOOM!', str(self.exception))

    def test_thread_cancelled(self):
        """ThreadDecorator TaskCancelled is raised if task is cancelled."""
        task = tjob_long()
        task.cancel()
        self.assertRaises(TaskCancelled, task.get)

    def test_thread_cancelled_callback(self):
        """ThreadDecorator TaskCancelled is raised within the callback
        if task is cancelled."""
        tjob_long.callback = self.error_callback
        task = tjob_long()
        task.cancel()
        event.wait()
        self.assertTrue(isinstance(self.exception, TaskCancelled))


class TestThreadPoolDecorator(unittest.TestCase):
    def setUp(self):
        global results
        results = 0
        event.clear()
        self.exception = None
        self.callback_results = 0
        self.threadclass = ThreadClass()

    def callback(self, task):
        self.callback_results = task.get()
        event.set()

    def error_callback(self, task):
        try:
            task.get()
        except Exception as error:
            self.exception = error
        event.set()

    def test_class_pool_decorated_method(self):
        """ThreadPoolDecorator decorated method."""
        task = self.threadclass.tjob_pool(1)
        self.assertEqual(task.get(), 3)

    def test_thread_pool_single_task(self):
        """ThreadPoolDecorator Single task with no parameters."""
        task = tjob_pool_single(1, 1)
        self.assertEqual(task.get(), 2)

    def test_thread_pool(self):
        """ThreadPoolDecorator multiple tasks are correctly handled."""
        tasks = []
        for i in range(0, 5):
            tasks.append(tjob_pool(1, 1))
        self.assertEqual(sum([t.get()[0] for t in tasks]), 10)

    def test_thread_pool_different_threads(self):
        """ThreadPoolDecorator multiple tasks
        are handled by different threads."""
        tasks = []
        for i in range(0, 20):
            tasks.append(tjob_pool(1, 1))
        self.assertEqual(len(set([t.get()[1] for t in tasks])), 2)

    def test_thread_pool_wrong_decoration(self):
        """ThreadPoolDecorator raises ValueError if given wrong params."""
        try:
            @thread_pool(callback, 5)
            def wrong(argument, keyword_argument=0):
                return argument + keyword_argument
        except Exception as error:
            self.assertTrue(isinstance(error, ValueError))

    def test_thread_callback_static(self):
        """ThreadPoolDecorator static callback is executed with thread pool."""
        tjob_pool(1, 1)
        event.wait()
        self.assertEqual(2, results[0])

    def test_thread_pool_callback_dynamic(self):
        """ThreadPoolDecorator dynamic callback is executed
        with thread pool."""
        tjob_pool.callback = self.callback
        tjob_pool(1, 1)
        event.wait()
        self.assertEqual(2, self.callback_results[0])

    def test_thread_pool_default_queue(self):
        """ThreadPoolDecorator default queue has same pool size."""
        self.assertEqual(tjob_pool._pool._context.queue.maxsize, 0)

    def test_thread_pool_queue(self):
        """ThreadPoolDecorator queue is correctly initialized."""
        self.assertEqual(tjob_pool_queue._pool._context.queue.maxsize, 5)

    def test_thread_pool_queue_error(self):
        """ThreadPoolDecorator raises ValueError
        if given wrong queue params."""
        try:
            @thread_pool(queue=5)
            def wrong(argument, keyword_argument=0):
                return argument + keyword_argument
        except Exception as error:
            self.assertTrue(isinstance(error, ValueError))

    def test_thread_pool_initializer(self):
        """ThreadPoolDecorator initializer is correctly run."""
        task = tjob_pool_init(1, 1)
        self.assertEqual(task.get(), 3)

    def test_thread_pool_initializer_error(self):
        """ThreadPoolDecorator an exception in a initializer
        is raised by get."""
        task = tjob_pool_init_error(1, 1)
        self.assertRaises(Exception, task.get)

    def test_thread_pool_cancel(self):
        """ThreadPoolDecorator callback gets notification if Task is cancelled."""
        tjob_pool_long.callback = self.error_callback
        task = tjob_pool_long(1, 1)
        task.cancel()
        event.wait()
        self.assertTrue(isinstance(self.exception, TaskCancelled))

    def test_thread_pool_callback_error(self):
        """ThreadPoolDecorator error within callback is safely handled."""
        tjob_pool_long.callback = self.error_callback
        task = tjob_pool_long(1, 1)
        task.cancel()
        event.wait()
        self.assertTrue(tjob_pool_long._pool.active)


class TestProcessDecorator(unittest.TestCase):
    def setUp(self):
        global results
        global exception
        results = 0
        exception = None
        event.clear()
        self.exception = None
        self.callback_results = 0
        self.processclass = ProcessClass()

    def callback(self, task):
        self.callback_results = task.get()
        event.set()

    def error_callback(self, task):
        try:
            task.get()
        except Exception as error:
            self.exception = error
        event.set()

    def test_process_wrong_decoration(self):
        """ProcessDecorator raises ValueError if given wrong params."""
        try:
            @process(callback, 5)
            def wrong(argument, keyword_argument=0):
                return argument + keyword_argument
        except Exception as error:
            self.assertTrue(isinstance(error, ValueError))

    def test_class_decorated_method(self):
        """ProcessDecorator decorated method."""
        task = self.processclass.pjob(1)
        self.assertEqual(task.get(), 3)

    def test_process_callback_static(self):
        """ProcessDecorator static callback is executed once task is done."""
        pjob_callback(1, 1)
        event.wait()
        self.assertEqual(2, results)

    def test_process_callback_dynamic(self):
        """ProcessDecorator dynamic callback is executed once task is done."""
        pjob.callback = self.callback
        pjob(1, 1)
        event.wait()
        self.assertEqual(2, self.callback_results)

    def test_process_error(self):
        """ProcessDecorator exceptions in process tasks are raised by get."""
        self.assertRaises(Exception, pjob_error(1, 1).get)

    def test_process_error_callback(self):
        """ProcessDecorator exception in tasks are managed in callback."""
        pjob_error.callback = self.error_callback
        pjob_error(1, 1)
        event.wait()
        self.assertEqual('BOOM!', str(self.exception))

    def test_process_timeout(self):
        """ProcessDecorator TimeoutError is raised if task times out."""
        task = pjob_timeout()
        self.assertRaises(TimeoutError, task.get)

    def test_process_timeout_callback(self):
        """ProcessDecorator TimeoutError is raised within the callback
        if task times out."""
        pjob_timeout.callback = self.error_callback
        pjob_timeout()
        event.wait()
        self.assertTrue(isinstance(self.exception, TimeoutError))

    def test_process_no_timeout(self):
        """ProcessDecorator timeout doesn't kill the task."""
        self.assertEqual(pjob_long_timeout().get(), 1)

    def test_process_cancelled(self):
        """ProcessDecorator TaskCancelled is raised if task is cancelled."""
        task = pjob_long()
        task.cancel()
        self.assertRaises(TaskCancelled, task.get)

    def test_process_cancelled_callback(self):
        """ProcessDecorator TaskCancelled is raised within the callback
        if task is cancelled."""
        pjob_long.callback = self.error_callback
        task = pjob_long()
        task.cancel()
        event.wait()
        self.assertTrue(isinstance(self.exception, TaskCancelled))

    def test_process_unserializable(self):
        """ProcessDecorator PicklingError is returned
        if results are not serializeable."""
        task = pjob_unserializeable()
        try:  # Python 2
            from cPickle import PicklingError
            self.assertRaises(PicklingError, task.get)
        except ImportError:  # Python 3
            from pickle import PicklingError
            self.assertRaises(PicklingError, task.get)

    def test_process_unserializable_error(self):
        """ProcessDecorator SerializingError is returned
        if exception is not serializeable."""
        task = pjob_unserializeable_error()
        try:  # Python 2
            from cPickle import PicklingError
            self.assertRaises(PicklingError, task.get)
        except ImportError:  # Python 3
            from pickle import PicklingError
            self.assertRaises(PicklingError, task.get)


class TestProcessPoolDecorator(unittest.TestCase):
    def setUp(self):
        global results
        results = 0
        event.clear()
        self.exception = None
        self.callback_results = 0
        self.processclass = ProcessClass()

    def callback(self, task):
        self.callback_results = task.get()
        event.set()

    def error_callback(self, task):
        try:
            task.get()
        except Exception as error:
            self.exception = error
        event.set()

    def test_class_decorated_method(self):
        """ProcessPoolDecorator decorated method."""
        task = self.processclass.pjob_pool(1)
        self.assertEqual(task.get(), 3)

    def test_process_pool_single_task(self):
        """ProcessPoolDecorator Single task with no parameters."""
        task = pjob_pool_single(1, 1)
        self.assertEqual(task.get(), 2)

    def test_process_pool(self):
        """ProcessPoolDecorator multiple tasks are correctly handled."""
        tasks = []
        for i in range(0, 5):
            tasks.append(pjob_pool(1, 1))
        self.assertEqual(sum([t.get()[0] for t in tasks]), 10)

    def test_process_pool_different_processs(self):
        """ProcessPoolDecorator multiple tasks are handled
        by different processs."""
        tasks = []
        for i in range(0, 20):
            tasks.append(pjob_pool(1, 1))
        self.assertEqual(len(set([t.get()[1] for t in tasks])), 2)

    def test_process_pool_wrong_decoration(self):
        """ProcessPoolDecorator decorator raises ValueError
        if given wrong params."""
        try:
            @process_pool(callback, 5)
            def wrong(argument, keyword_argument=0):
                return argument + keyword_argument
        except Exception as error:
            self.assertTrue(isinstance(error, ValueError))

    def test_process_callback_static(self):
        """ProcessPoolDecorator static callback is executed
        with process pool."""
        pjob_pool(1, 1)
        event.wait()
        self.assertEqual(2, results[0])

    def test_process_pool_callback_dynamic(self):
        """ProcessPoolDecorator dynamic callback is executed
        with process pool."""
        pjob_pool.callback = self.callback
        pjob_pool(1, 1)
        event.wait()
        self.assertEqual(2, self.callback_results[0])

    def test_process_pool_default_queue(self):
        """ProcessPoolDecorator default queue has same pool size."""
        self.assertEqual(pjob_pool._pool._context.queue.maxsize, 0)

    def test_process_pool_queue(self):
        """ProcessPoolDecorator Queue is correctly initialized."""
        self.assertEqual(pjob_pool_queue._pool._context.queue.maxsize, 5)

    def test_process_pool_queue_error(self):
        """ProcessPoolDecorator raises ValueError
        if given wrong queue params."""
        try:
            @process_pool(queue=5)
            def wrong(argument, keyword_argument=0):
                return argument + keyword_argument
        except Exception as error:
            self.assertTrue(isinstance(error, ValueError))

    def test_process_pool_initializer(self):
        """ProcessPoolDecorator initializer is correctly run."""
        task = pjob_pool_init(1, 1)
        self.assertEqual(task.get(), 3)

    def test_process_pool_initializer_error(self):
        """ProcessPoolDecorator an exception in a initializer
        is raised by get."""
        task = pjob_pool_init_error(1, 1)
        self.assertRaises(Exception, task.get)

    def test_process_pool_cancel(self):
        """ProcessPool callback gets notification if Task is cancelled."""
        pjob_pool_long.callback = self.error_callback
        task = pjob_pool_long(1, 1)
        task.cancel()
        event.wait()
        self.assertTrue(isinstance(self.exception, TaskCancelled))

    def test_process_pool_callback_error(self):
        """ProcessPool error within callback is safely handled."""
        pjob_pool_long.callback = self.error_callback
        task = pjob_pool_long(1, 1)
        task.cancel()
        event.wait()
        self.assertTrue(pjob_pool_long._pool.active)
