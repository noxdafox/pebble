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

    .. decorator:: process(callback=None, timeout=None)

       When called, the *function* will be run in a new process, a *Task* object will be returned to the caller.

       Values returned by the decorated *function* will be sent back to the caller through a *Pipe*, therefore they must be serializable into a *Pickle* object.

       *callback* must be callable, if passed, it will be called once the task has ended with the *Task* object as parameter.
       A *timeout* value greater than 0 will terminate the running process if it has not yet finished once the *timeout* expires; any *Task.get()* call will raise a TimeoutError, callbacks will still be executed.


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

       Functions decorated by *thread* and *process* decorators, once called, will return a *Task* object.
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
