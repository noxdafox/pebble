.. Pebble documentation master file, created by
   sphinx-quickstart on Thu Oct 17 23:52:22 2013.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Pebble's documentation!
==================================

Modern languages should natively support concurrency, threading and synchronization primitives. Their usage should be the most intuitive possible, yet allowing all the required flexibility.

Pebble aims to help managing threads and processes in an easier way. It wraps Python's standard libray threading and multiprocessing objects.


:mod:`concurrent`
-----------------

.. decorator:: process(timeout=None)

   Runs the decorated function in a concurrent process, taking care of the results and error management.

   The decorated function returns a *pebble.ProcessFuture* object.

   If *timeout* is set, the process will be stopped once expired and the *Future* object will raise a *concurrent.futures.TimeoutError* exception.

.. decorator:: thread()

   Runs the decorated function in a concurrent thread, taking care of the results and error management.

   The decorated function returns a *concurrent.futures.Future* object.


:mod:`pebble`
-------------

.. class:: ProcessPool(max_workers=1, max_tasks=0, initializer=None, initargs=None)

   A Pool allows to schedule jobs into a Pool of Processes which will perform them concurrently.
   Process pools work as well as a *context manager*.

   *max_workers* is an integer representing the amount of desired process workers managed by the pool. If *max_tasks* is a number greater than zero each worker will be restarted after performing an equal amount of tasks.
   *initializer* must be callable, if passed, it will be called every time a worker is started, receiving *initargs* as arguments.

   .. data:: active

      True if the Pool is running, false otherwise.

   .. function:: schedule(function, args=(), kwargs={}, timeout=None)

      Schedule a job within the Pool.

      Returns a *pebble.ProcessFuture* object representing the execution of the callable.

      *function* is the function which is about to be scheduled.
      *args* and *kwargs* will be passed to the function respectively as its arguments and keyword arguments.
      *timeout* is an integer or a float. If given, once expired it will force the timed out task to be interrupted and the worker will be restarted. *Future.result()* will raise *TimeoutError*, callbacks will be executed.

   .. function:: map(function, *iterables, timeout=None, chunksize=None)

      Returns an iterator equivalent to map(function, iterables).

      *timeout* is an integer or a float. If given, once expired it will force the timed out task to be interrupted and the worker will be restarted. The subsequent call to __next__ on the iterator will raise *TimeoutError*. The timeout is assigned per element of the iterator. Therefore, a *TimeoutError* will not prevent the iterator to continue. See example below.

      *chunksize* controls the size of the chunks the iterable will be broken into before being passed to the function. If None the size will be controlled by the Pool.

   .. function:: close()

      No more job will be allowed into the Pool, queued jobs will be consumed.
      To ensure all the jobs are performed call *ProcessPool.join()* just after closing the Pool.

   .. function:: stop()

      The Pool will be stopped abruptly. All enqueued and running jobs will be lost.
      To ensure the Pool to be released call *ProcessPool.join()* after stopping the Pool.

   .. function:: join(timeout=None)

      Waits for all workers to exit, must not be called before calling either *close()*, *stop()* or *kill()*.
      If *timeout* is set and some worker is still running after it expired a TimeoutError will be raised, a timeout of 0 will return immediately.

.. class:: ThreadPool(max_workers=1, max_tasks=0, initializer=None, initargs=None)

   A ThreadPool allows to schedule jobs into a Pool of Threads which will perform them concurrently.
   Thread pools work as well as a *context manager*.

   *max_workers* is an integer representing the amount of desired thread workers managed by the pool. If *max_tasks* is a number greater than zero each worker will be restarted after performing an equal amount of tasks.
   *initializer* must be callable, if passed, it will be called every time a worker is started, receiving *initargs* as arguments.

   .. data:: active

      True if the Pool is running, false otherwise.

   .. function:: schedule(function, args=(), kwargs={})

      Schedule a job within the Pool.

      Returns a *Future* object representing the execution of the callable.

      *function* is the function which is about to be scheduled.
      *args* and *kwargs* will be passed to the function respectively as its arguments and keyword arguments.

   .. function:: map(function, *iterables, chunksize=None)
      Returns an iterator equivalent to map(function, iterables).

      *chunksize* controls the size of the chunks the iterable will be broken into before being passed to the function. If None the size will be controlled by the Pool.

   .. function:: close()

      No more job will be allowed into the Pool, queued jobs will be consumed.
      To ensure all the jobs are performed call *ThreadPool.join()* just after closing the Pool.

   .. function:: stop()

      The ongoing jobs will be performed, all the enqueued ones dropped; this is a fast way to terminate the Pool.
      To ensure the Pool to be released call *ThreadPool.join()* after stopping the Pool.

   .. function:: join(timeout=None)

      Waits for all workers to exit, must not be called before calling either *close()*, *stop()* or *kill()*.
      If *timeout* is set and some worker is still running after it expired a TimeoutError will be raised, a timeout of 0 will return immediately.

.. class:: pebble.ProcessFuture()

   This class inherits from *concurrent.futures.Future*. The only difference with the parent class is the possibility to cancel running call.

   .. function:: cancel()

      Cancel a running or enqueued call. If the call has already completed then the method will return False, otherwise the call will be cancelled and the method will return True. If the call is running, the process executing it will be stopped allowing to reclaim its resources.

.. decorator:: synchronized([lock])

    A synchronized function prevents two or more callers to interleave its execution preventing race conditions.

    The *synchronized* decorator accepts as optional parameter a *Lock*, *RLock* or *Semaphore* from *threading* and *multiprocessing* modules.

    If no synchronization object is given, a single *threading.Lock* will be employed. This implies that between different decorated functions only one at a time will be executed.

.. decorator:: sighandler(signals)

   Convenience decorator for setting the decorated *function* as signal handler for the specified *signals*.

   *signals* can either be a single signal or a list/tuple of signals.

.. function:: waitforthreads(threads, timeout=None)

   Waits for one or more *Thread* to exit or until *timeout* expires.

   *threads* is a list containing one or more *threading.Thread* objects.
   If *timeout* is not None the function will block for the specified amount of seconds returning an empty list if no *Thread* is ready.

   The function returns a list containing the ready *Threads*.

   .. note::

      Expired *Threads* are not joined by *waitforthreads*.

.. function:: waitforqueues(queues, timeout=None)

   Waits for one or more *Queue* to be ready or until *timeout* expires.

   *queues* is a list containing one or more *Queue.Queue* objects.
   If *timeout* is not None the function will block for the specified amount of seconds returning an empty list if no *Queue* is ready.

   The function returns a list containing the ready *Queues*.

.. exception:: ProcessExpired

   Raised by *Future.result()* functions if the related process died unexpectedly during the execution.

   .. data:: exitcode

      Integer representing the process' exit code.


General notes
-------------

Processes
+++++++++

The Python's multiprocessing guidelines apply as well for all functionalities within the *process* namespace.

Examples
--------

Concurrent decorators
+++++++++++++++++++++

Run a function in a separate process and wait for its results.

::

    from pebble import concurrent

    @concurrent.process
    def function(arg, kwarg=0):
        return arg + kwarg

    future = function(1, kwarg=1)
    print(future.result())

Quite often developers need to integrate in their projects third party code which appears to be unstable, to leak memory or to hang. The concurrent function allows to easily take advantage of the isolation offered by processes without the need of handling any multiprocessing primitive.

::

    from pebble import concurrent
    from concurrent.futures import Timeouterror
    from third_party_lib import unstable_function

    @concurrent.process(timeout=10)
    def function(arg, kwarg=0):
        unstable_function(arg, kwarg=kwarg)

    future = function(1, kwarg=1)

    try:
        results = future.result()
    except TimeoutError as error:
        print("unstable_function took longer than %d seconds" % error.args[1])
    except ProcessExpired as error:
        print("%s. Exit code: %d" % (error, error.exitcode))
    except Exception as error:
        print("unstable_function raised %s" % error)
        print(error.traceback)  # Python's traceback of remote process

Pools
+++++

The ProcessPool has been designed to support task timeouts and critical errors. If a task reaches its timeout, the worker will be interrupted immediately. Abrupt interruptions of the workers are dealt trasparently.

::

    from pebble import ProcessPool
    from concurrent.futures import Timeouterror

    def function(foo, bar=0):
    	return foo + bar

    elements = list(range(1000))

    with ProcessPool() as pool:
        iterator = pool.map(function, elements, timeout=10)
        while True:
            try:
                results = next(iterator)
                print(results)
            except TimeoutError as error:
                print("Function took longer than %d seconds" % error.args[1])
            except ProcessExpired as error:
                print("%s. Exit code: %d" % (error, error.exitcode))
            except Exception as error:
                print("function raised %s" % error)
                print(error.traceback)  # Python's traceback of remote process
            except StopIteration:
                break

Sighandler decorator
++++++++++++++++++++

The syntax ::

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

Running the tests
-----------------

On Python 3, the tests will cover all the multiprocessing starting methods supported by the platform.

Due to multiprocessing limitations, it is not possible to change the starting method once set. Therefore test frameworks such as nose and pytest which run all the tests in a single process will fail.

To see the tests work, it's enough to test one test file at a time.

.. toctree::
   :maxdepth: 2
