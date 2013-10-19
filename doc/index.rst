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

    .. decorator:: Asynchronous(callback=None, error_callback=None)

       When called, the *function* will be run in a separate thread, a *Task* object will be returned to the caller.

       *callback* and *error_callback* must be callables.
       If *callback* is not None, it will be called once the task has ended with the task identifier and the *function* return values.
       If *error_callback* is defined, it will be called if the task has raised an exception, passing the task identifier and the raised exception.

    .. decorator:: Concurrent(callback=None, error_callback=None)

       When called, the *function* will be run in a separate process, a *Task* object will be returned to the caller.

       *callback* and *error_callback* must be callables.
       If *callback* is not None, it will be called once the task has ended with the task identifier and the *function* return values.
       If *error_callback* is defined, it will be called if the task has raised an exception, passing the task identifier and the raised exception.

:mod:`pebble`
-------------

    :synopsis: Task and Exceptions

    .. exception:: TimeoutError

       Raised when *Task.get()* *timeout* expires.

    .. class:: Task

       Functions decorated by *Asynchronous* and *Concurrent* decorators, once called, will return a *Task* object.
       *Task* objects are handlers to the ongoing jobs within spawned threads and processes.

       .. data:: id

	  A string containing the unique identifier (UUID) of the task.

       .. data:: number

          An integer representing the task number, formerly the amount of times the decorated function has been previously called.

       .. function:: get([timeout])

	  Returns the values given back by the decorated *function*; if an exception has been raised within it, it will be re-raised by the *get()* method.
	  The *get()* method blocks until the thread or process has not finished.

	  If *timeout* is a number greater than 0 it will block for the specified amount of seconds, raising a TimeoutError if the results are not ready yet; a value equal or smaller than 0 will force the methor to return immediately.


.. toctree::
   :maxdepth: 2