Pebble
======

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
    from concurrent.futures import TimeoutError

    @concurrent.process(timeout=10)
    def function(foo, bar=0):
        return foo + bar

    future = function(1, bar=2)

    try:
        result = future.result()  # blocks until results are ready
    except TimeoutError as error:
        print("Function took longer than %d seconds" % error.args[1])
    except Exception as error:
        print("Function raised %s" % error)
        print(error.traceback)  # traceback of the function

Pools support workers restart, timeout for long running tasks and more::

    from pebble import ProcessPool
    from concurrent.futures import TimeoutError

    def function(foo, bar=0):
    	return foo + bar

    def task_done(future):
        try:
            result = future.result()  # blocks until results are ready
        except TimeoutError as error:
            print("Function took longer than %d seconds" % error.args[1])
        except Exception as error:
            print("Function raised %s" % error)
            print(error.traceback)  # traceback of the function

    with ProcessPool(max_workers=5, max_tasks=10) as pool:
        for i in range(0, 10):
            future = pool.schedule(function, args=[i], timeout=3)
            future.add_done_callback(task_done)

More examples in the documentation_.

.. _documentation: http://pythonhosted.org/Pebble/
