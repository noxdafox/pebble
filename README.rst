Pebble
======

Description
-----------

Pebble provides a neat API to manage threads and processes within an application.


Examples
--------

Run a job in a separate thread and wait for its results::

    from pebble import concurrent

    @concurrent.thread
    def function(foo, bar=0):
        return foo + bar

    future = function(1, bar=2)

    result = future.result()  # blocks until results are ready

Run a function with a timeout of ten seconds and deal with errors::

    from pebble import concurrent

    @concurrent.process(timeout=10)
    def function(foo, bar=0):
        return foo + bar

    future = function(1, bar=2)

    try:
        result = future.result()  # blocks until results are ready
    except Exception as error:
        print("Function raised %s" % error)
        print(error.traceback)  # traceback of the function
    except TimeoutError as error:
        print("Function took longer than %d seconds" % error.args[1])

Pools allow to execute several tasks without the need of spawning a new worker for each one of them::

    from threading import current_thread
    from pebble import thread

    def task_done(task):
        results, thread_id = task.get()
    	print "Task %s returned %d from thread %s" % (task.id,
                                                      results,
                                                      thread_id)

    def do_job(foo, bar=0):
    	return foo + bar, current_thread().ident

    with thread.Pool(workers=5) as pool:
        for i in range(0, 10):
            pool.schedule(do_job, args=(i, ), callback=task_done)


Check the documentation for more examples.
