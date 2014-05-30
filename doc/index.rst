.. Pebble documentation master file, created by
   sphinx-quickstart on Thu Oct 17 23:52:22 2013.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Pebble's documentation!
==================================

Modern languages should natively support concurrency, threading and synchronization primitives. Their usage should be the most intuitive possible, yet allowing all the possible flexibility.

Pebble aims to help managing threads and processes in an easier way; it wraps Python's standard libray threading and multiprocessing objects.


:mod:`process`
-----------------

.. function:: concurrent(target=None, args=(), kwargs={}, name=None, daemon=False)

   Spawns a concurrent process and runs a function within it.

   The concurrent function works as well as a decorator.

   *target* is the desired function to be run with the given *args* and *kwargs* parameters; if *daemon* is True, the process will be stopped if the parent exits (default False).
   *name* is a string, if assigned will be forwarded to the process object.

   The concurrent function returns the Process object which is running the *target* or decorated one.

   .. note::

       The decorator accepts the keywords *daemon* and *name* only.
       If *target* keyword is not specified, the function will act as a decorator.

.. function:: task(target=None, args=(), kwargs={}, callback=None, identifier=None, timeout=None)

   Runs the given function in a concurrent process, taking care of the results and error management.

   The task function works as well as a decorator.

   *target* is the desired function to be run with the given *args* and *kwargs* parameters; if *timeout* is set, the process will be stopped once expired returning TimeoutError as results.
   If a *callback* is passed, it will be executed after the job has finished with the returned *Task* as parameter.
   If *identifier* is not None, it will be assigned as the *Task.id* value.

   The task function returns a *Task* object.

   .. note::

       The decorator accepts the keywords *timeout* and *callback* only.
       If *target* keyword is not specified, the function will act as a decorator.

.. class:: Pool(workers=1, task_limit=0, queue=None, queueargs=None, initializer=None, initargs=None)

   A Pool allows to schedule jobs into a Pool of Processes which will perform them concurrently.
   Process pools work as well as a *context manager*.

   *workers* is an integer representing the amount of desired process workers managed by the pool. If *worker_task_limit* is a number greater than zero each worker will be restarted after performing an equal amount of tasks.
   *initializer* must be callable, if passed, it will be called every time a worker is started, receiving *initargs* as arguments.
   *queue* represents a Class which, if passed, will be constructed with *queueargs* as parameters and used internally as a task queue. The *queue* object resulting from its construction must expose same functionalities of Python standard *Queue* object, especially for what concerns the *put()*, *get()* and *join()* methods.

   .. data:: active

      True if the Pool is running, false otherwise.

   .. function:: schedule(function, args=(), kwargs={}, callback=None, timeout=0, identifier=None)

      Schedule a job within the Pool.

      *function* is the function which is about to be scheduled.
      *args* and *kwargs* will be passed to the function respectively as its arguments and keyword arguments.
      *callback* must be callable, if passed, it will be called once the task has ended with the *Task* object as parameter.
      *timeout* is an integer, if greater than zero, once expired will force the timed out task to be interrupted and the worker will be restarted; *Task.get()* will raise *TimeoutError*, callbacks will be executed.
      If *identifier* is not None, it will be assigned as the *Task.id* value.

   .. function:: close()

      No more job will be allowed into the Pool, queued jobs will be consumed.
      To ensure all the jobs are performed call *ThreadPool.join()* just after closing the Pool.

   .. function:: stop()

      The ongoing jobs will be performed, all the enqueued ones dropped; this is a fast way to terminate the Pool.
      To ensure the Pool to be released call *ThreadPool.join()* after stopping the Pool.

   .. function:: kill()

      All workers will be killed forcing the pool to terminate.
      To ensure the Pool to be released call *ThreadPool.join()* after killing the Pool.

   .. function:: join(timeout=None)

      Waits for all workers to exit, must not be called before calling either *close()*, *stop()* or *kill()*.
      If *timeout* is set and some worker is still running after it expired a TimeoutError will be raised, a timeout of 0 will return immediately.


:mod:`thread`
-----------------

.. function:: concurrent(target=None, args=(), kwargs={}, name=None, daemon=False)

   Spawns a concurrent thread and runs a function within it.

   The concurrent function works as well as a decorator.

   *target* is the desired function to be run with the given *args* and *kwargs* parameters; if *daemon* is True, the thread will be stopped if the parent exits (default False).
   *name* is a string, if assigned will be forwarded to the thread object.

   The concurrent function returns the Thread object which is running the *target* or decorated one.

   .. note::

       The decorator accepts the keywords *daemon* and *name* only.
       If *target* keyword is not specified, the function will act as a decorator.

.. function:: task(target=None, args=(), kwargs={}, callback=None, identifier=None)

   Runs the given function in a concurrent thread, taking care of the results and error management.

   The task function works as well as a decorator.

   *target* is the desired function to be run with the given *args* and *kwargs* parameters; if *timeout* is set, the thread will be stopped once expired returning TimeoutError as results.
   If a *callback* is passed, it will be executed after the job has finished with the returned *Task* as parameter.
   If *identifier* is not None, it will be assigned as the *Task.id* value.

   The task function returns a *Task* object.

   .. note::

       The decorator accepts the *callback* keyword only.
       If *target* keyword is not specified, the function will act as a decorator.

.. class:: Pool(workers=1, task_limit=0, queue=None, queueargs=None, initializer=None, initargs=None)

   A Pool allows to schedule jobs into a Pool of Threades which will perform them concurrently.
   Thread pools work as well as a *context manager*.

   *workers* is an integer representing the amount of desired thread workers managed by the pool. If *worker_task_limit* is a number greater than zero each worker will be restarted after performing an equal amount of tasks.
   *initializer* must be callable, if passed, it will be called every time a worker is started, receiving *initargs* as arguments.
   *queue* represents a Class which, if passed, will be constructed with *queueargs* as parameters and used internally as a task queue. The *queue* object resulting from its construction must expose same functionalities of Python standard *Queue* object, especially for what concerns the *put()*, *get()* and *join()* methods.

   .. data:: active

      True if the Pool is running, false otherwise.

   .. function:: schedule(function, args=(), kwargs={}, callback=None, identifier=None)

      Schedule a job within the Pool.

      *function* is the function which is about to be scheduled.
      *args* and *kwargs* will be passed to the function respectively as its arguments and keyword arguments.
      *callback* must be callable, if passed, it will be called once the task has ended with the *Task* object as parameter.
      If *identifier* is not None, it will be assigned as the *Task.id* value.

   .. function:: close()

      No more job will be allowed into the Pool, queued jobs will be consumed.
      To ensure all the jobs are performed call *ThreadPool.join()* just after closing the Pool.

   .. function:: stop()

      The ongoing jobs will be performed, all the enqueued ones dropped; this is a fast way to terminate the Pool.
      To ensure the Pool to be released call *ThreadPool.join()* after stopping the Pool.

   .. function:: kill()

      All workers will be killed forcing the pool to terminate.
      To ensure the Pool to be released call *ThreadPool.join()* after killing the Pool.

   .. function:: join(timeout=None)

      Waits for all workers to exit, must not be called before calling either *close()*, *stop()* or *kill()*.
      If *timeout* is set and some worker is still running after it expired a TimeoutError will be raised, a timeout of 0 will return immediately.


:mod:`pebble`
-------------

.. decorator:: synchronized(lock)

   A synchronized *function* will be run exclusively accordingly to its *lock* type. If a Thread or Process is executing a synchronized function, all the other Threads or Processes calling the same *function* will be blocked until the first caller has finished its execution.

   The *synchronized* decorator accepts all the synchronizing objects exposed by the Python standard *threading* and *multiprocessing* libraries.

.. decorator:: sighandler(signals)

   Convenience decorator for setting the decorated *function* as signal handler for the specified *signals*.

   *signals* can either be a single signal or a list/tuple of signals.

   For example the syntax ::

     import signal
     from pebble import sighandler

     @sighandler((signal.SIGINT, signal.SIGTERM))
     def signal_handler(signum, frame):
         print("Termination request received!")

   Is equivalent to ::

     import signal

     def signal_handler(signum, frame):
         print("Termination request received!")

     signal.signal(signal.SIGINT, signal_handler)
     signal.signal(signal.TERM, signal_handler)


.. exception:: TimeoutError

   Raised when *Task.get()* *timeout* expires.

.. exception:: TaskCancelled

   Raised by *Task.get()* functions if *Task.cancel()* has been called.

.. class:: Task

   Functions decorated by *thread*, *process* and *thread_pool* decorators, as well as the ThreadPool.schedule method, once called, will return a *Task* object.
   *Task* objects are handlers to the ongoing jobs within spawned threads and processes.

   .. data:: id

      Contains None as long as an identifier string is not specified at creation time.

   .. data:: number

      An integer representing the task number.

   .. data:: ready

      A boolean, False if *Task* is still ongoing, True if results are ready.

   .. data:: successful

      A boolean, False if *Task* is still ongoing or an exception occurred whithin, True if results were successfully delivered.

   .. data:: started

      A boolean, True if *Task* has been started, False if it is still in the queue.

   .. function:: wait(timeout=None)

      Wait until the results are ready or a *timeout* occurrs.
      Returns True if *Task* completed within the timeout period, False otherwise.

   .. function:: get(timeout=None, cancel=False)

      Returns the values given back by the decorated *function*.
      If an exception has been raised within it, it will be re-raised by the *get()* method with the traceback appended as attribute.
      The *get()* method blocks until the thread or process has not finished.

      If *timeout* is a number greater than 0 it will block for the specified amount of seconds, raising a *TimeoutError* if the results are not ready yet; a value equal or smaller than 0 will force the method to return immediately.
      If *cancel* is True the *task* will be cancelled in case of timeout; the function will raise *TimeoutError*, subsequent calls to *Task.get()* will raise *TaskCancelled*, the parameter has no effect if *timeout* has not been set.

   .. function:: cancel()

      Cancels the *Task*, results will be dropped, *callbacks* will be executed and any *Task.get()* blocking will raise *TaskCancelled* exception.
      If the task is running into a process it will be terminated, as thread cannot be stopped, its results will simply be ignored but the function itself will keep running.

      *cancel()* should not be called on *Tasks* which logic is using shared resources as *Pipes*, *Locks* or *Files*.


Examples
--------

Use of concurrent
=================

The concurrent function is a simple convenience method for spawning Processes and Threads, the following code snippets are all equivalent.

::

     from threading import Thread

     def function(arg, keyarg=0):
         print arg + keyarg

     proc = Thread(target=function, args=[1], kwargs={'keyarg': 1})
     proc.daemon = True
     proc.start

::

     from pebble import thread

     def function(arg, keyarg=0):
         print arg + keyarg

     proc = thread.concurrent(target=function, args=[1], kwargs={'keyarg': 1}, daemon=True)

::

     from pebble import thread

     @thread.concurrent(daemon=True)
     def function(arg, keyarg=0):
         print arg + keyarg

     proc = function(1, keyarg=1)

The latter example allows build up concurrent logic in a more neat way.

Producer and consumer example.

::

     from pebble import thread
     from queue import Queue

     @thread.concurrent
     def producer(queue):
         for product in range(10):
             print("I'm the producer, I produced: %d" % product)
             queue.put(product)
         print("I'm the producer and I'm leaving.")
         queue.put(None)

     @thread.concurrent
     def consumer(queue):
         while True:
             product = queue.get()
             if product is not None:
                 print("I'm the consumer, I consume: %d" % product)
             else:
                 print("I'm the consumer and I'm leaving.")
                 return


     queue = Queue()
     cons = consumer(queue)
     prod = producer(queue)

     cons.join()
     prod.join()


Task functions
==============

The task functions represent what use to be the thread and process decorators in Pebble2.

::

     from pebble import process

     @process.task
     def function(arg, keyarg=0):
         return arg + keyarg

     task = function(1, keyarg=1)
     print(task.get())

Quite often developers need to integrate in their projects third party code which appears to be unstable, to leak memory or to hang. The task function allows to easily take advantage of the isolation offered by processes without the need of handling any multiprocessing primitive.

::

    from pebble import process

    from third_party_lib import unstable_function

    task = process.task(target=unstable_function, args=[1, 2], timeout=5)

    try:
        results = task.get()
    except TimeoutError:
        print("unstable_function took more than 5 seconds to complete")
    except Exception as error:
        print("unstable_function raised %s" % error)
        print(error.traceback)  # Python's traceback of remote process

.. toctree::
   :maxdepth: 2
