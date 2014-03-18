import time
import threading
import unittest
from os import getpid
try:  # Python 2
    from Queue import Queue
except:  # Python 3
    from queue import Queue

from pebble import TimeoutError, TaskCancelled
from pebble import ProcessPool, process, process_pool


_initiarg = 0
_results = 0
_exception = None


class UnserializeableError(Exception):
    def __init__(self):
        self.lock = threading.Lock()  # unpickleable

    def __str__(self):
        return "BOOM!"


def callback(task):
    global _results
    _results = task.get()


def error_callback(task):
    global _exception
    _exception = task.get()


def initializer(value):
    global _initiarg
    _initiarg = value


def initializer_error(value):
    raise Exception("BOOM!")


def jp(argument, keyword_argument=0):
    return argument + keyword_argument + _initiarg, getpid()


def jp_long(argument, keyword_argument=0):
    time.sleep(0.1)
    return argument + keyword_argument + _initiarg, getpid()


def jp_very_long(argument, keyword_argument=0):
    time.sleep(3)
    return argument + keyword_argument + _initiarg, getpid()


def jp_error(argument, keyword_argument=0):
    raise Exception("BOOM!")


@process
def job(argument, keyword_argument=0):
    return argument + keyword_argument


@process
def job_error(argument, keyword_argument=0):
    raise Exception("BOOM!")


@process(callback=callback)
def job_callback(argument, keyword_argument=0):
    return argument + keyword_argument


@process
def job_long():
    time.sleep(1)
    return 1


@process
def job_count():
    return None


@process(timeout=1)
def job_timeout():
    time.sleep(2)
    return 1


@process(timeout=2)
def job_long_timeout():
    return 1


@process
def job_unserializeable():
    return threading.Lock()


@process
def job_unserializeable_error():
    raise UnserializeableError()


@process_pool
def job_pool_single(argument, keyword_argument=0):
    return argument + keyword_argument


@process_pool(workers=2, callback=callback)
def job_pool(argument, keyword_argument=0):
    return argument + keyword_argument, getpid()


@process_pool(workers=2, queue=Queue, queueargs=(5, ))
def job_pool_queue(argument, keyword_argument=0):
    return argument + keyword_argument, getpid()


@process_pool(workers=2)
def job_pool_dyn_queue(argument, keyword_argument=0):
    time.sleep(1)
    return argument + keyword_argument, getpid()


@process_pool(workers=2, initializer=initializer, initargs=(1, ))
def job_pool_init(argument, keyword_argument=0):
    return argument + keyword_argument + _initiarg


@process_pool(workers=2, initializer=initializer_error, initargs=(1, ))
def job_pool_init_error(argument, keyword_argument=0):
    return argument + keyword_argument + _initiarg


class TestProcessDecorator(unittest.TestCase):
    def setUp(self):
        global _results
        global _exception
        _results = 0
        _exception = None
        self.exception = None
        self.callback_results = 0

    def callback(self, task):
        self.callback_results = task.get()

    def error_callback(self, task):
        try:
            task.get()
        except Exception as error:
            self.exception = error

    def test_process_wrong_decoration(self):
        """Decorator raises ValueError if given wrong params."""
        try:
            @process(callback, 5)
            def wrong(argument, keyword_argument=0):
                return argument + keyword_argument
        except Exception as error:
            self.assertTrue(isinstance(error, ValueError))

    def test_process_callback_static(self):
        """Test static callback is executed once task is done."""
        job_callback(1, 1).get()
        time.sleep(0.1)
        self.assertEqual(2, _results)

    def test_process_callback_dynamic(self):
        """Test dynamic callback is executed once task is done."""
        job.callback = self.callback
        job(1, 1).get()
        time.sleep(0.1)
        self.assertEqual(2, self.callback_results)

    def test_process_error(self):
        """Test that an exception in a process task is raised by get."""
        self.assertRaises(Exception, job_error(1, 1).get)

    def test_process_error_callback(self):
        """Test that an exception in a task is managed in callback."""
        job_error.callback = self.error_callback
        task = job_error(1, 1)
        try:
            task.get()
        except:
            pass
        time.sleep(0.1)
        self.assertEqual('BOOM!', str(self.exception))

    def test_process_timeout(self):
        """TimeoutError is raised if task times out."""
        task = job_timeout()
        self.assertRaises(TimeoutError, task.get)

    def test_process_timeout_callback(self):
        """TimeoutError is raised within the callback if task times out."""
        job_timeout.callback = self.error_callback
        task = job_timeout()
        try:
            task.get()
        except:
            pass
        time.sleep(0.1)
        self.assertTrue(isinstance(self.exception, TimeoutError))

    def test_process_no_timeout(self):
        """Timeout decorator doesn't kill the task."""
        self.assertEqual(job_long_timeout().get(), 1)

    def test_process_unserializable(self):
        """PicklingError is returned if results are not serializeable."""
        task = job_unserializeable()
        try:  # Python 2
            from cPickle import PicklingError
            self.assertRaises(PicklingError, task.get)
        except ImportError:  # Python 3
            from pickle import PicklingError
            self.assertRaises(PicklingError, task.get)

    def test_process_unserializable_error(self):
        """SerializingError is returned if exception is not serializeable."""
        task = job_unserializeable_error()
        try:  # Python 2
            from cPickle import PicklingError
            self.assertRaises(PicklingError, task.get)
        except ImportError:  # Python 3
            from pickle import PicklingError
            self.assertRaises(PicklingError, task.get)


class TestProcessPool(unittest.TestCase):
    def setUp(self):
        global _results
        _results = 0
        global _initiarg
        _initiarg = 0
        self.exception = None
        self.callback_results = 0

    def callback(self, task):
        self.callback_results = task.get()

    def error_callback(self, task):
        try:
            task.get()
        except Exception as error:
            self.exception = error

    def test_process_pool_single_task(self):
        """Single task with no parameters."""
        with ProcessPool() as tp:
            task = tp.schedule(jp, args=(1, ),
                               kwargs={'keyword_argument': 1})
        self.assertEqual(task.get()[0], 2)

    def test_process_pool(self):
        """Multiple tasks are correctly handled."""
        tasks = []
        with ProcessPool() as tp:
            for i in range(0, 5):
                tasks.append(tp.schedule(jp, args=(1, ),
                                         kwargs={'keyword_argument': 1}))
        self.assertEqual(sum([t.get()[0] for t in tasks]), 10)

    def test_process_pool_different_processs(self):
        """Multiple tasks are handled by different processs."""
        tasks = []
        with ProcessPool(workers=2) as tp:
            for i in range(0, 5):
                tasks.append(tp.schedule(jp_long, args=(1, ),
                                         kwargs={'keyword_argument': 1}))
        self.assertEqual(len(set([t.get()[1] for t in tasks])), 2)

    def test_process_pool_restart(self):
        """Expired processs are restarted."""
        tasks = []
        with ProcessPool(task_limit=2) as tp:
            for i in range(0, 5):
                tasks.append(tp.schedule(jp, args=(1, ),
                                         kwargs={'keyword_argument': 1}))
        self.assertEqual(len(set([t.get()[1] for t in tasks])), 3)

    def test_process_pool_callback(self):
        """Test callback is executed with process pool."""
        with ProcessPool() as tp:
            tp.schedule(jp, args=(1, ),
                        kwargs={'keyword_argument': 1},
                        callback=self.callback)
        time.sleep(0.1)
        self.assertEqual(2, self.callback_results[0])

    def test_process_pool_timeout_error(self):
        """TimeoutError is raised if timeout occurrs."""
        with ProcessPool() as tp:
            t = tp.schedule(jp_very_long, args=(1, ),
                            kwargs={'keyword_argument': 1},
                            timeout=1)
        self.assertRaises(TimeoutError, t.get)

    def test_process_pool_timeout_process(self):
        """Process is terminated if timeout occurrs."""
        with ProcessPool() as tp:
            t = tp.schedule(jp_very_long, args=(1, ),
                            kwargs={'keyword_argument': 1},
                            timeout=1)
            time.sleep(0.3)
            p = tp._pool[0]
            try:
                t.get()
            except TimeoutError:
                pass
            time.sleep(0.3)
            self.assertFalse(p.is_alive())

    def test_process_pool_timeout_int(self):
        """ValueError is raised if timeout is not integer."""
        with ProcessPool() as tp:
            self.assertRaises(ValueError, tp.schedule,
                              jp, args=(1, ), timeout=0.1)

    def test_process_pool_default_queue(self):
        """Default queue has same pool size."""
        with ProcessPool() as tp:
            self.assertEqual(tp._queue.maxsize, 0)

    def test_process_pool_queue(self):
        """Static queue is correctly initialized."""
        with ProcessPool(queue=Queue, queueargs=(5, )) as tp:
            self.assertEqual(tp._queue.maxsize, 5)

    def test_process_pool_initializer(self):
        """Initializer is correctly run."""
        with ProcessPool(initializer=initializer, initargs=(1, )) as tp:
            task = tp.schedule(jp, args=(1, ),
                               kwargs={'keyword_argument': 1})
            self.assertEqual(task.get()[0], 3)

    def test_process_pool_initializer_error(self):
        """An exception in a initializer is raised by get."""
        with ProcessPool(initializer=initializer, initargs=(1, )) as tp:
            task = tp.schedule(jp, args=(1, ),
                               kwargs={'keyword_argument': 1})
            self.assertEqual(task.get()[0], 3)

    def test_process_pool_stop(self):
        """Pool is stopped without consuming more tasks."""
        tp = ProcessPool()
        for i in range(0, 10):
            tp.schedule(jp, args=(1, ))
        tp.stop()
        tp.join()
        self.assertFalse(tp._queue.empty())

    def test_process_pool_close(self):
        """Pool is closed consuming all tasks."""
        tp = ProcessPool()
        for i in range(0, 10):
            tp.schedule(jp, args=(1, ))
        tp.close()
        tp.join()
        self.assertTrue(tp._queue.qsize() <= 1)

    def test_process_pool_join_running(self):
        """RuntimeError is raised if join() is called on a running pool"""
        tp = ProcessPool()
        tp.schedule(jp, args=(1, ))
        self.assertRaises(RuntimeError, tp.join)
        tp.stop()
        tp.join()

    def test_process_pool_join_timeout(self):
        """Timeout is raised if workers are still active once joined."""
        tp = ProcessPool()
        tp.schedule(jp_very_long, args=(1, ))
        time.sleep(0.3)
        tp._state = 2
        self.assertRaises(TimeoutError, tp.join, timeout=0.1)
        tp.kill()

    def test_process_pool_no_new_tasks(self):
        """No more tasks are allowed if Pool is closed."""
        tp = ProcessPool()
        tp.stop()
        tp.join()
        self.assertRaises(RuntimeError, tp.schedule, jp)

    def test_process_pool_wrong_function(self):
        """Function must be callable."""
        tp = ProcessPool()
        self.assertRaises(ValueError, tp.schedule, None)
        tp.stop()
        tp.join()

    def test_process_pool_active(self):
        """Active is False if pool has been created."""
        with ProcessPool(initializer=initializer, initargs=(1, )) as tp:
            tp.schedule(jp, args=(1, ), kwargs={'keyword_argument': 1})
            self.assertTrue(tp.active)

    def test_process_pool_not_active_started(self):
        """Active is False if pool has been created."""
        with ProcessPool(initializer=initializer, initargs=(1, )) as tp:
            self.assertFalse(tp.active)

    def test_process_pool_not_active(self):
        """Active is False if pool is not running."""
        tp = ProcessPool()
        tp.stop()
        tp.join()
        self.assertFalse(tp.active)

    def test_process_pool_cancel(self):
        """ProcessPool callback gets notification if Task is cancelled."""
        with ProcessPool() as tp:
            task = tp.schedule(jp_very_long, args=(1, ),
                               callback=self.error_callback)
            task.cancel()
        self.assertTrue(isinstance(self.exception, TaskCancelled))

    def test_process_pool_callback_error(self):
        """ProcessPool error within callback is safely handled."""
        with ProcessPool() as tp:
            tp.schedule(jp_error, args=(1, ), callback=self.callback)
            self.assertTrue(tp.active)


class TestProcessPoolDecorator(unittest.TestCase):
    def setUp(self):
        global _results
        _results = 0
        self.exception = None
        self.callback_results = 0

    def callback(self, task):
        self.callback_results = task.get()

    def error_callback(self, task):
        try:
            task.get()
        except Exception as error:
            self.exception = error

    def test_process_pool_single_task(self):
        """Single task with no parameters."""
        task = job_pool_single(1, 1)
        self.assertEqual(task.get(), 2)

    def test_process_pool(self):
        """Multiple tasks are correctly handled."""
        tasks = []
        for i in range(0, 5):
            tasks.append(job_pool(1, 1))
        self.assertEqual(sum([t.get()[0] for t in tasks]), 10)

    def test_process_pool_different_processs(self):
        """Multiple tasks are handled by different processs."""
        tasks = []
        for i in range(0, 20):
            tasks.append(job_pool(1, 1))
        self.assertEqual(len(set([t.get()[1] for t in tasks])), 2)

    def test_process_pool_wrong_decoration(self):
        """Decorator raises ValueError if given wrong params."""
        try:
            @process_pool(callback, 5)
            def wrong(argument, keyword_argument=0):
                return argument + keyword_argument
        except Exception as error:
            self.assertTrue(isinstance(error, ValueError))

    def test_process_callback_static(self):
        """Test static callback is executed with process pool."""
        t = job_pool(1, 1)
        t.wait()
        self.assertEqual(2, _results[0])

    def test_process_pool_callback_dynamic(self):
        """Test dynamic callback is executed with process pool."""
        job_pool.callback = self.callback
        job_pool(1, 1)
        time.sleep(0.1)
        self.assertEqual(2, self.callback_results[0])

    def test_process_pool_default_queue(self):
        """Default queue has same pool size."""
        self.assertEqual(job_pool._pool._queue.maxsize, 0)

    def test_process_pool_queue(self):
        """Queue is correctly initialized."""
        self.assertEqual(job_pool_queue._pool._queue.maxsize, 5)

    def test_process_pool_queue_error(self):
        """Decorator raises ValueError if given wrong queue params."""
        try:
            @process_pool(queue=5)
            def wrong(argument, keyword_argument=0):
                return argument + keyword_argument
        except Exception as error:
            self.assertTrue(isinstance(error, ValueError))

    def test_process_pool_initializer(self):
        """Initializer is correctly run."""
        task = job_pool_init(1, 1)
        self.assertEqual(task.get(), 3)

    def test_process_pool_initializer_error(self):
        """An exception in a initializer is raised by get."""
        task = job_pool_init_error(1, 1)
        self.assertRaises(Exception, task.get)
