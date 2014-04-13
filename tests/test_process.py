import time
import threading
import unittest
from os import getpid
try:  # Python 2
    from Queue import Queue
except:  # Python 3
    from queue import Queue

from pebble import TimeoutError, TaskCancelled
from pebble import ProcessPool


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
        """ProcessPool single task with no parameters."""
        with ProcessPool() as tp:
            task = tp.schedule(jp, args=(1, ),
                               kwargs={'keyword_argument': 1})
        self.assertEqual(task.get()[0], 2)

    def test_process_pool_schedule_id(self):
        """ProcessPool task ID is forwarded to it."""
        with ProcessPool() as pp:
            task = pp.schedule(jp, args=(1, ), identifier='foo')
        self.assertEqual(task.id, 'foo')

    def test_process_pool_schedule_uuid(self):
        """ProcessPool task UUID is assigned if None."""
        with ProcessPool() as pp:
            task = pp.schedule(jp, args=(1, ))
        self.assertEqual(task.id.version, 4)

    def test_process_pool(self):
        """ProcessPool multiple tasks are correctly handled."""
        tasks = []
        with ProcessPool() as tp:
            for i in range(0, 5):
                tasks.append(tp.schedule(jp, args=(1, ),
                                         kwargs={'keyword_argument': 1}))
        self.assertEqual(sum([t.get()[0] for t in tasks]), 10)

    def test_process_pool_different_processs(self):
        """ProcessPool multiple tasks are handled by different processes."""
        tasks = []
        with ProcessPool(workers=2) as pp:
            for i in range(0, 100):
                tasks.append(pp.schedule(jp, args=(1, ),
                                         kwargs={'keyword_argument': 1}))
        self.assertEqual(len(set([t.get()[1] for t in tasks])), 2)

    def test_process_pool_restart(self):
        """ProcessPool expired processes are restarted."""
        tasks = []
        with ProcessPool(task_limit=2) as tp:
            for i in range(0, 5):
                tasks.append(tp.schedule(jp, args=(1, ),
                                         kwargs={'keyword_argument': 1}))
        self.assertEqual(len(set([t.get()[1] for t in tasks])), 3)

    def test_process_pool_callback(self):
        """ProcessPool test callback is executed with process pool."""
        with ProcessPool() as tp:
            tp.schedule(jp, args=(1, ),
                        kwargs={'keyword_argument': 1},
                        callback=self.callback)
        time.sleep(0.1)
        self.assertEqual(2, self.callback_results[0])

    def test_process_pool_timeout_error(self):
        """ProcessPool TimeoutError is raised if timeout occurrs."""
        with ProcessPool() as tp:
            t = tp.schedule(jp_very_long, args=(1, ),
                            kwargs={'keyword_argument': 1},
                            timeout=1)
        self.assertRaises(TimeoutError, t.get)

    def test_process_pool_timeout_process(self):
        """ProcessPool process is terminated if timeout occurrs."""
        with ProcessPool() as tp:
            t = tp.schedule(jp_very_long, args=(1, ),
                            kwargs={'keyword_argument': 1},
                            timeout=1)
            time.sleep(0.3)
            p = tp._context.pool[0]
            try:
                t.get()
            except TimeoutError:
                pass
            time.sleep(0.3)
            self.assertFalse(p.is_alive())

    def test_process_pool_timeout_int(self):
        """ProcessPool ValueError is raised if timeout is not integer."""
        with ProcessPool() as tp:
            self.assertRaises(ValueError, tp.schedule,
                              jp, args=(1, ), timeout=0.1)

    def test_process_pool_default_queue(self):
        """ProcessPool default queue has same pool size."""
        with ProcessPool() as tp:
            self.assertEqual(tp._context.queue.maxsize, 0)

    def test_process_pool_queue(self):
        """ProcessPool static queue is correctly initialized."""
        with ProcessPool(queue=Queue, queueargs=(5, )) as tp:
            self.assertEqual(tp._context.queue.maxsize, 5)

    def test_process_pool_initializer(self):
        """ProcessPool initializer is correctly run."""
        with ProcessPool(initializer=initializer, initargs=(1, )) as tp:
            task = tp.schedule(jp, args=(1, ),
                               kwargs={'keyword_argument': 1})
            self.assertEqual(task.get()[0], 3)

    def test_process_pool_initializer_error(self):
        """ProcessPool an exception in a initializer is raised by get."""
        with ProcessPool(initializer=initializer, initargs=(1, )) as tp:
            task = tp.schedule(jp, args=(1, ),
                               kwargs={'keyword_argument': 1})
            self.assertEqual(task.get()[0], 3)

    def test_process_pool_stop(self):
        """ProcessPool is stopped without consuming more tasks."""
        tp = ProcessPool()
        for i in range(0, 10):
            tp.schedule(jp_long, args=(1, ))
        tp.stop()
        tp.join()
        self.assertFalse(tp._context.queue.empty())

    def test_process_pool_close(self):
        """ProcessPool is closed consuming all tasks."""
        tp = ProcessPool()
        for i in range(0, 10):
            tp.schedule(jp, args=(1, ))
        tp.close()
        tp.join()
        self.assertTrue(tp._context.queue.qsize() <= 1)

    def test_process_pool_join_running(self):
        """ProcessPool RuntimeError is raised
        if join() is called on a running pool"""
        tp = ProcessPool()
        tp.schedule(jp, args=(1, ))
        self.assertRaises(RuntimeError, tp.join)
        tp.stop()
        tp.join()

    def test_process_pool_join_timeout(self):
        """ProcessPool TimeoutError is raised
        if workers are still active once joined."""
        tp = ProcessPool()
        tp.schedule(jp_very_long, args=(1, ))
        time.sleep(0.3)
        tp._context.state = 2
        self.assertRaises(TimeoutError, tp.join, timeout=0.1)
        tp.kill()

    def test_process_pool_no_new_tasks(self):
        """ProcessPool no more tasks are allowed if Pool is closed."""
        tp = ProcessPool()
        tp.stop()
        tp.join()
        self.assertRaises(RuntimeError, tp.schedule, jp)

    def test_process_pool_wrong_function(self):
        """ProcessPool function must be callable."""
        tp = ProcessPool()
        self.assertRaises(ValueError, tp.schedule, None)
        tp.stop()
        tp.join()

    def test_process_pool_active(self):
        """ProcessPool active is False if pool has been created."""
        with ProcessPool(initializer=initializer, initargs=(1, )) as tp:
            tp.schedule(jp, args=(1, ), kwargs={'keyword_argument': 1})
            self.assertTrue(tp.active)

    def test_process_pool_not_active_started(self):
        """ProcessPool active is False if pool has been created."""
        with ProcessPool(initializer=initializer, initargs=(1, )) as tp:
            self.assertFalse(tp.active)

    def test_process_pool_not_active(self):
        """ProcessPool active is False if pool is not running."""
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
