import time
import unittest
from threading import current_thread
try:  # Python 2
    from Queue import Queue
except:  # Python 3
    from queue import Queue

from pebble import thread, thread_pool, ThreadPool, TimeoutError, TaskCancelled


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


@thread
def job(argument, keyword_argument=0):
    return argument + keyword_argument


@thread
def job_error(argument, keyword_argument=0):
    raise Exception("BOOM!")


@thread(callback=callback)
def job_callback(argument, keyword_argument=0):
    return argument + keyword_argument


@thread
def job_long():
    time.sleep(1)
    return 1


@thread
def job_count():
    return None


@thread_pool
def job_pool_single(argument, keyword_argument=0):
    return argument + keyword_argument


@thread_pool(workers=2, callback=callback)
def job_pool(argument, keyword_argument=0):
    time.sleep(0.01)
    return argument + keyword_argument, current_thread()


@thread_pool(workers=2, queue=Queue, queueargs=(5, ))
def job_pool_queue(argument, keyword_argument=0):
    return argument + keyword_argument, current_thread()


@thread_pool(workers=2)
def job_pool_dyn_queue(argument, keyword_argument=0):
    time.sleep(1)
    return argument + keyword_argument, current_thread()


@thread_pool(workers=2, initializer=initializer, initargs=(1, ))
def job_pool_init(argument, keyword_argument=0):
    return argument + keyword_argument + _initiarg


@thread_pool(workers=2, initializer=initializer_error, initargs=(1, ))
def job_pool_init_error(argument, keyword_argument=0):
    return argument + keyword_argument + _initiarg


class TestThreadDecorators(unittest.TestCase):
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

    def test_thread_wrong_decoration(self):
        """Decorator raises ValueError if given wrong params."""
        try:
            @thread(callback, 5)
            def wrong(argument, keyword_argument=0):
                return argument + keyword_argument
        except Exception as error:
            self.assertTrue(isinstance(error, ValueError))

    def test_thread_callback_static(self):
        """Test static callback is executed with thread task."""
        job_callback(1, 1)
        time.sleep(0.1)
        self.assertEqual(2, _results)

    def test_thread_callback_dynamic(self):
        """Test dynamic callback is executed with thread task."""
        job.callback = self.callback
        job(1, 1)
        time.sleep(0.1)
        self.assertEqual(2, self.callback_results)

    def test_thread_error_callback(self):
        """Test that an exception in a task is managed in error_callback."""
        job_error.callback = self.error_callback
        job_error(1, 1)
        time.sleep(0.1)
        self.assertEqual('BOOM!', str(self.exception))


class TestThreadTask(unittest.TestCase):
    def setUp(self):
        pass

    def test_task_get(self):
        """Values are correctly returned from get."""
        task = job(1, 1)
        self.assertEqual(task.get(), 2)

    def test_task_get_error(self):
        """An exception in a task is raised by get."""
        task = job_error(1, 1)
        self.assertRaises(Exception, task.get)

    def test_task_timeout(self):
        """TimeoutError is raised if task has not yet finished."""
        task = job_long()
        self.assertRaises(TimeoutError, task.get, 0)

    def test_task_no_timeout(self):
        """Test timeout get parameter works."""
        task = job_long()
        self.assertEqual(task.get(2), 1)

    def test_task_number(self):
        """Task number are correctly assigned."""
        for i in range(0, 5):
            task = job_count(1, 1)
        self.assertEqual(task.number, 4)

    def test_task_ready(self):
        """Ready parameter is true if task is done."""
        task = job(1, 1)
        task.get()
        self.assertTrue(task.ready)

    def test_task_not_ready(self):
        """Ready parameter is false if task is not done."""
        task = job_long()
        self.assertFalse(task.ready)

    def test_task_cancel(self):
        """Task is cancelled."""
        task = job_long()
        task.cancel()
        self.assertRaises(TaskCancelled, task.get)

    def test_task_cancel_error(self):
        """Cannot cancel a completed task."""
        task = job(1, 1)
        task.get()
        self.assertRaises(RuntimeError, task.cancel)

    def test_task_cancelled(self):
        """Cancelled is true if task is cancelled."""
        task = job_long()
        task.cancel()
        self.assertTrue(task.cancelled)


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
        """Single task with no parameters."""
        with ThreadPool() as tp:
            task = tp.schedule(jp, args=(1, ),
                               kwargs={'keyword_argument': 1})
        self.assertEqual(task.get()[0], 2)

    def test_thread_pool(self):
        """Multiple tasks are correctly handled."""
        tasks = []
        with ThreadPool() as tp:
            for i in range(0, 5):
                tasks.append(tp.schedule(jp, args=(1, ),
                                         kwargs={'keyword_argument': 1}))
        self.assertEqual(sum([t.get()[0] for t in tasks]), 10)

    def test_thread_pool_different_threads(self):
        """Multiple tasks are handled by different threads."""
        tasks = []
        with ThreadPool(workers=2) as tp:
            for i in range(0, 5):
                tasks.append(tp.schedule(jp_long, args=(1, ),
                                         kwargs={'keyword_argument': 1}))
        self.assertEqual(len(set([t.get()[1] for t in tasks])), 2)

    def test_thread_pool_restart(self):
        """Expired threads are restarted."""
        tasks = []
        with ThreadPool(task_limit=2) as tp:
            for i in range(0, 5):
                tasks.append(tp.schedule(jp, args=(1, ),
                                         kwargs={'keyword_argument': 1}))
        self.assertEqual(len(set([t.get()[1] for t in tasks])), 3)

    def test_thread_pool_callback(self):
        """Test callback is executed with thread pool."""
        with ThreadPool() as tp:
            tp.schedule(jp, args=(1, ),
                        kwargs={'keyword_argument': 1},
                        callback=self.callback)
        time.sleep(0.1)
        self.assertEqual(2, self.callback_results[0])

    def test_thread_pool_default_queue(self):
        """Default queue has same pool size."""
        with ThreadPool() as tp:
            self.assertEqual(tp._queue.maxsize, 0)

    def test_thread_pool_queue(self):
        """Static queue is correctly initialized."""
        with ThreadPool(queue=Queue, queueargs=(5, )) as tp:
            self.assertEqual(tp._queue.maxsize, 5)

    def test_thread_pool_initializer(self):
        """Initializer is correctly run."""
        with ThreadPool(initializer=initializer, initargs=(1, )) as tp:
            task = tp.schedule(jp, args=(1, ),
                               kwargs={'keyword_argument': 1})
            self.assertEqual(task.get()[0], 3)

    def test_thread_pool_initializer_error(self):
        """An exception in a initializer is raised by get."""
        with ThreadPool(initializer=initializer, initargs=(1, )) as tp:
            task = tp.schedule(jp, args=(1, ),
                               kwargs={'keyword_argument': 1})
            self.assertEqual(task.get()[0], 3)

    def test_thread_pool_stop(self):
        """Pool is stopped without consuming more tasks."""
        tp = ThreadPool()
        for i in range(0, 10):
            tp.schedule(jp, args=(1, ))
        tp.stop()
        tp.join()
        self.assertFalse(tp._queue.empty())

    def test_thread_pool_close(self):
        """Pool is closed consuming all tasks."""
        tp = ThreadPool()
        for i in range(0, 10):
            tp.schedule(jp, args=(1, ))
        tp.close()
        tp.join()
        self.assertTrue(tp._queue.qsize() <= 1)

    def test_thread_pool_join_running(self):
        """RuntimeError is raised if join() is called on a running pool"""
        tp = ThreadPool()
        self.assertRaises(RuntimeError, tp.join, jp)
        tp.stop()
        tp.join()

    def test_thread_pool_join_timeout(self):
        """Timeout is raised if workers are still active once joined."""
        tp = ThreadPool()
        tp.schedule(jp_very_long, args=(1, ))
        time.sleep(0.1)
        tp.stop()
        self.assertRaises(TimeoutError, tp.join, 0.5)
        tp.join()

    def test_thread_pool_no_new_tasks(self):
        """No more tasks are allowed if Pool is closed."""
        tp = ThreadPool()
        tp.stop()
        tp.join()
        self.assertRaises(RuntimeError, tp.schedule, jp)

    def test_thread_pool_wrong_function(self):
        """Function must be callable."""
        tp = ThreadPool()
        self.assertRaises(ValueError, tp.schedule, None)
        tp.stop()
        tp.join()

    def test_thread_pool_active(self):
        """Active is True if pool is running."""
        with ThreadPool(initializer=initializer, initargs=(1, )) as tp:
            self.assertTrue(tp.active)

    def test_thread_pool_not_active(self):
        """Active is False if pool is not running."""
        tp = ThreadPool()
        tp.stop()
        tp.join()
        self.assertFalse(tp.active)


class TestThreadPoolDecorator(unittest.TestCase):
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

    def test_thread_pool_single_task(self):
        """Single task with no parameters."""
        task = job_pool_single(1, 1)
        self.assertEqual(task.get(), 2)

    def test_thread_pool(self):
        """Multiple tasks are correctly handled."""
        tasks = []
        for i in range(0, 5):
            tasks.append(job_pool(1, 1))
        self.assertEqual(sum([t.get()[0] for t in tasks]), 10)

    def test_thread_pool_different_threads(self):
        """Multiple tasks are handled by different threads."""
        tasks = []
        for i in range(0, 20):
            tasks.append(job_pool(1, 1))
        self.assertEqual(len(set([t.get()[1] for t in tasks])), 2)

    def test_thread_pool_wrong_decoration(self):
        """Decorator raises ValueError if given wrong params."""
        try:
            @thread_pool(callback, 5)
            def wrong(argument, keyword_argument=0):
                return argument + keyword_argument
        except Exception as error:
            self.assertTrue(isinstance(error, ValueError))

    def test_thread_callback_static(self):
        """Test static callback is executed with thread pool."""
        job_pool(1, 1)
        time.sleep(0.1)
        self.assertEqual(2, _results[0])

    def test_thread_pool_callback_dynamic(self):
        """Test dynamic callback is executed with thread pool."""
        job_pool.callback = self.callback
        job_pool(1, 1)
        time.sleep(0.1)
        self.assertEqual(2, self.callback_results[0])

    def test_thread_pool_default_queue(self):
        """Default queue has same pool size."""
        self.assertEqual(job_pool._pool._queue.maxsize, 0)

    def test_thread_pool_queue(self):
        """Queue is correctly initialized."""
        self.assertEqual(job_pool_queue._pool._queue.maxsize, 5)

    def test_thread_pool_queue_error(self):
        """Decorator raises ValueError if given wrong queue params."""
        try:
            @thread_pool(queue=5)
            def wrong(argument, keyword_argument=0):
                return argument + keyword_argument
        except Exception as error:
            self.assertTrue(isinstance(error, ValueError))

    def test_thread_pool_initializer(self):
        """Initializer is correctly run."""
        task = job_pool_init(1, 1)
        self.assertEqual(task.get(), 3)

    def test_thread_pool_initializer_error(self):
        """An exception in a initializer is raised by get."""
        task = job_pool_init_error(1, 1)
        self.assertRaises(Exception, task.get)
