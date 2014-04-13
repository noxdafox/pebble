import time
import unittest
from threading import current_thread
try:  # Python 2
    from Queue import Queue
except:  # Python 3
    from queue import Queue

from pebble import ThreadPool, TimeoutError, TaskCancelled


_initiarg = 0
_results = 0
_exception = None


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
    return argument + keyword_argument + _initiarg, current_thread()


def jp_long(argument, keyword_argument=0):
    time.sleep(0.1)
    return argument + keyword_argument + _initiarg, current_thread()


def jp_very_long(argument, keyword_argument=0):
    time.sleep(1)
    return argument + keyword_argument + _initiarg, current_thread()


def jp_error(argument, keyword_argument=0):
    raise Exception("BOOM!")


class TestThreadPool(unittest.TestCase):
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

    def test_thread_pool_single_task(self):
        """ThreadPool single task with no parameters."""
        with ThreadPool() as tp:
            task = tp.schedule(jp, args=(1, ),
                               kwargs={'keyword_argument': 1})
        self.assertEqual(task.get()[0], 2)

    def test_thread_pool_schedule_id(self):
        """ThreadPool task ID is forwarded to it."""
        with ThreadPool() as tp:
            task = tp.schedule(jp, args=(1, ), identifier='foo')
        self.assertEqual(task.id, 'foo')

    def test_thread_pool_schedule_uuid(self):
        """ThreadPool task UUID is assigned if None."""
        with ThreadPool() as tp:
            task = tp.schedule(jp, args=(1, ))
        self.assertEqual(task.id.version, 4)

    def test_thread_pool(self):
        """ThreadPool multiple tasks are correctly handled."""
        tasks = []
        with ThreadPool() as tp:
            for i in range(0, 5):
                tasks.append(tp.schedule(jp, args=(1, ),
                                         kwargs={'keyword_argument': 1}))
        self.assertEqual(sum([t.get()[0] for t in tasks]), 10)

    def test_thread_pool_different_threads(self):
        """ThreadPool multiple tasks are handled by different threads."""
        tasks = []
        with ThreadPool(workers=2) as tp:
            for i in range(0, 5):
                tasks.append(tp.schedule(jp_long, args=(1, ),
                                         kwargs={'keyword_argument': 1}))
        self.assertEqual(len(set([t.get()[1] for t in tasks])), 2)

    def test_thread_pool_restart(self):
        """ThreadPool expired threads are restarted."""
        tasks = []
        with ThreadPool(task_limit=2) as tp:
            for i in range(0, 5):
                tasks.append(tp.schedule(jp, args=(1, ),
                                         kwargs={'keyword_argument': 1}))
        self.assertEqual(len(set([t.get()[1] for t in tasks])), 3)

    def test_thread_pool_callback(self):
        """ThreadPool callback is executed with thread pool."""
        with ThreadPool() as tp:
            tp.schedule(jp, args=(1, ),
                        kwargs={'keyword_argument': 1},
                        callback=self.callback)
        time.sleep(0.1)
        self.assertEqual(2, self.callback_results[0])

    def test_thread_pool_default_queue(self):
        """ThreadPool default queue has same pool size."""
        with ThreadPool() as tp:
            self.assertEqual(tp._context.queue.maxsize, 0)

    def test_thread_pool_queue(self):
        """ThreadPool static queue is correctly initialized."""
        with ThreadPool(queue=Queue, queueargs=(5, )) as tp:
            self.assertEqual(tp._context.queue.maxsize, 5)

    def test_thread_pool_initializer(self):
        """ThreadPool initializer is correctly run."""
        with ThreadPool(initializer=initializer, initargs=(1, )) as tp:
            task = tp.schedule(jp, args=(1, ),
                               kwargs={'keyword_argument': 1})
            self.assertEqual(task.get()[0], 3)

    def test_thread_pool_initializer_error(self):
        """ThreadPool an exception in a initializer is raised by get."""
        with ThreadPool(initializer=initializer, initargs=(1, )) as tp:
            task = tp.schedule(jp, args=(1, ),
                               kwargs={'keyword_argument': 1})
            self.assertEqual(task.get()[0], 3)

    def test_thread_pool_stop(self):
        """ThreadPool is stopped without consuming more tasks."""
        tp = ThreadPool()
        for i in range(0, 10):
            tp.schedule(jp_long, args=(1, ))
        tp.stop()
        tp.join()
        self.assertFalse(tp._context.queue.empty())

    def test_thread_pool_close(self):
        """ThreadPool is closed consuming all tasks."""
        tp = ThreadPool()
        for i in range(0, 10):
            tp.schedule(jp, args=(1, ))
        tp.close()
        tp.join()
        self.assertTrue(tp._context.queue.qsize() <= 1)

    def test_thread_pool_join_running(self):
        """ThreadPool RuntimeError is raised
        if join() is called on a running pool."""
        tp = ThreadPool()
        tp.schedule(jp, args=(1, ))
        self.assertRaises(RuntimeError, tp.join, jp)
        tp.stop()
        tp.join()

    def test_thread_pool_join_timeout(self):
        """ThreadPool TimeoutError is raised
        if workers are still active once joined."""
        tp = ThreadPool()
        tp.schedule(jp_very_long, args=(1, ))
        time.sleep(0.1)
        tp.stop()
        self.assertRaises(TimeoutError, tp.join, 0.1)
        tp.join()

    def test_thread_pool_no_new_tasks(self):
        """ThreadPool no more tasks are allowed if Pool is closed."""
        tp = ThreadPool()
        tp.stop()
        tp.join()
        self.assertRaises(RuntimeError, tp.schedule, jp)

    def test_thread_pool_wrong_function(self):
        """ThreadPool function must be callable."""
        tp = ThreadPool()
        self.assertRaises(ValueError, tp.schedule, None)
        tp.stop()
        tp.join()

    def test_thread_pool_active(self):
        """ThreadPool active is True if pool is running."""
        with ThreadPool(initializer=initializer, initargs=(1, )) as tp:
            tp.schedule(jp, args=(1, ))
            self.assertTrue(tp.active)

    def test_thread_pool_not_active(self):
        """ThreadPool active is False if pool is not running."""
        tp = ThreadPool()
        tp.stop()
        tp.join()
        self.assertFalse(tp.active)

    def test_thread_pool_cancel(self):
        """ThreadPool callback gets notification if Task is cancelled."""
        with ThreadPool() as tp:
            task = tp.schedule(jp_very_long, args=(1, ),
                               callback=self.error_callback)
            task.cancel()
        self.assertTrue(isinstance(self.exception, TaskCancelled))

    def test_thread_pool_callback_error(self):
        """ThreadPool error within callback is safely handled."""
        with ThreadPool() as tp:
            tp.schedule(jp_error, args=(1, ), callback=self.callback)
            self.assertTrue(tp.active)
