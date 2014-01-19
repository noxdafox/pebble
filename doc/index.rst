.. Pebble documentation master file, created by
   sphinx-quickstart on Thu Oct 17 23:52:22 2013.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Pebble's documentation!
==================================

Modern languages should natively support concurrency, threading and synchronization primitives. Their usage should be the most intuitive possible, yet allowing all the possible flexibility.

Pebble aims to help managing threads and processes in an easier way; it wraps Python's standard libray threading and multiprocessing objects.


:mod:`decorators`
-----------------

    :synopsis: Function decorators

    .. decorator:: thread(callback=None)

       When called, the *function* will be run in a new thread, a *Task* object will be returned to the caller.

       *callback* must be callable, if passed, it will be called once the task has ended with the *Task* object as parameter.

    .. decorator:: thread_pool(callback=None, workers=1, worker_task_limit=0, initializer=None, initargs=(), queue=None, queueargs=())

       When called, the *function* will be run in a worker thread, a *Task* object will be returned to the caller; if all workers are busy the subsequent calls will be queued into an internal Queue.

       *callback* must be callable, if passed, it will be called once the task has ended with the *Task* object as parameter.
       *workers* is an integer representing the amount of desired thread workers managed by the pool. If *worker_task_limit* is a number greater than zero each worker will be restarted after performing an equal amount of tasks.
       *initializer* must be callable, if passed, it will be called every time a worker is started, receiving *initargs* as arguments.
       *queue* represents a Class which, if passed, will be constructed with *queueargs* as parameters and used internally as a task queue. The *queue* object resulting from its construction must expose same functionalities of Python standard *Queue* object, especially for what concerns the *put()*, *get()* and *join()* methods.

       .. note::

          The *thread_pool* decorator is working as a simple decorator (turns a function into a ThreadPool): no thread is started at decoration time but when the decorated function is actually called.

    .. decorator:: process(callback=None, timeout=None)

       When called, the *function* will be run in a new process, a *Task* object will be returned to the caller.

       Values returned by the decorated *function* will be sent back to the caller through a *Pipe*, therefore they must be serializable into a *Pickle* object.

       *callback* must be callable, if passed, it will be called once the task has ended with the *Task* object as parameter.
       A *timeout* value greater than 0 will terminate the running process if it has not yet finished once the *timeout* expires; any *Task.get()* call will raise a TimeoutError, callbacks will still be executed.


    :synopsis: Pools

    .. class:: pebble.thread.ThreadPool(workers=1, task_limit=0, queue=None, queueargs=None, initializer=None, initargs=None)

       A ThreadPool allows to schedule jobs into a Pool of Threads which will perform them asynchronously.
       Thread pools work as well as *context managers*.

       *workers* is an integer representing the amount of desired thread workers managed by the pool. If *worker_task_limit* is a number greater than zero each worker will be restarted after performing an equal amount of tasks.
       *initializer* must be callable, if passed, it will be called every time a worker is started, receiving *initargs* as arguments.
       *queue* represents a Class which, if passed, will be constructed with *queueargs* as parameters and used internally as a task queue. The *queue* object resulting from its construction must expose same functionalities of Python standard *Queue* object, especially for what concerns the *put()*, *get()* and *join()* methods.

       .. data:: initializer

          If re-assigned, the new initializer will be run only for re-spawned workers; already running workers will still be affected by the old initializer.

       .. data:: initargs

          If re-assigned, the new initargs will be taken into use only for re-spawned workers; old workers will still be affected by the old ones.

       .. function:: schedule(function, args=(), kwargs={}, callback=None)

          Schedule a job within the Pool.

          *function* is the function which is about to be scheduled.
          *args* and *kwargs* will be passed to the function respectively as its arguments and keyword arguments.
          *callback* must be callable, if passed, it will be called once the task has ended with the *Task* object as parameter.

       .. function:: close()

          No more job will be allowed into the Pool, queued jobs will be consumed.
          To ensure all the jobs are performed call *ThreadPool.join()* just after closing the Pool.

       .. function:: stop()

          The ongoing jobs will be performed, all the enqueued ones dropped; this is a fast way to terminate the Pool.
          To ensure the Pool to be released call *ThreadPool.join()* after stopping the Pool.

       .. function:: join(timeout=0)

          Waits for all workers to exit, must not be called before calling either *stop()* or *close()*.
          If *timeout* is greater than 0 and some worker is still running after it expired a TimeoutError will be raised.


:mod:`pebble`
-------------

    :synopsis: Task and Exceptions

    .. exception:: TimeoutError

       Raised when *Task.get()* *timeout* expires.

    .. exception:: SerializingError

       Raised whenever a concurrent task raised an unpickleable exception; this is tipically happening with exception raised by C libraries ported through *ctypes*.

    .. exception:: TaskCancelled

       Raised by *Task.get()* functions if *Task.cancel()* has been called.

    .. class:: Task

       Functions decorated by *thread*, *process* and *thread_pool* decorators, as well as the ThreadPool.schedule method, once called, will return a *Task* object.
       *Task* objects are handlers to the ongoing jobs within spawned threads and processes.

       .. data:: id

	  A string containing the unique identifier (UUID) of the task.

       .. data:: number

          An integer representing the task number, formerly the amount of times the decorated function has been previously called.

       .. data:: ready

          A boolean, False if *Task* is still ongoing, True if results are ready.

       .. function:: get([timeout])

	  Returns the values given back by the decorated *function*.
          If an exception has been raised within it, it will be re-raised by the *get()* method with the traceback appended as attribute.
	  The *get()* method blocks until the thread or process has not finished.

	  If *timeout* is a number greater than 0 it will block for the specified amount of seconds, raising a TimeoutError if the results are not ready yet; a value equal or smaller than 0 will force the method to return immediately.

       .. function:: cancel()

          Cancel the ongoing *Task*, results will be dropped, *callbacks* won't be executed and any *Task.get()* blocking will raise *TaskCancelled* exception.
          If the task is running into a process it will be terminated, as thread cannot be stopped, its results will simply be ignored but the function itself will keep running.

          *cancel()* should not be called on *Tasks* which logic is using shared resources as *Pipes*, *Locks* or *Files*.


.. toctree::
   :maxdepth: 2
