Pebble
======

Pebble provides a neat API to manage threads and processes within an application.

:Source: https://github.com/noxdafox/pebble
:Documentation: https://pebble.readthedocs.io
:Download: https://pypi.org/project/Pebble/

|build badge| |docs badge| |downloads badge|

.. |build badge| image:: https://github.com/noxdafox/pebble/actions/workflows/action.yml/badge.svg
   :target: https://github.com/noxdafox/pebble/actions/workflows/action.yml
   :alt: Build Status
.. |docs badge| image:: https://readthedocs.org/projects/pebble/badge/?version=latest
   :target: https://pebble.readthedocs.io
   :alt: Documentation Status
.. |downloads badge| image:: https://img.shields.io/pypi/dm/pebble
   :target: https://pypistats.org/packages/pebble
   :alt: PyPI - Downloads

Examples
--------

Run a job in a separate thread and wait for its results.

.. code:: python

    from pebble import concurrent

    @concurrent.thread
    def function(foo, bar=0):
        return foo + bar

    future = function(1, bar=2)

    result = future.result()  # blocks until results are ready

Same code with AsyncIO support.

.. code:: python

    import asyncio

    from pebble import asynchronous

    @asynchronous.thread
    def function(foo, bar=0):
        return foo + bar

    async def asynchronous_function():
        result = await function(1, bar=2)  # blocks until results are ready
        print(result)

    asyncio.run(asynchronous_function())

Run a function with a timeout of ten seconds and deal with errors.

.. code:: python

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

Pools support workers restart, timeout for long running tasks and more.

.. code:: python

    from pebble import ProcessPool
    from concurrent.futures import TimeoutError
    
    TIMEOUT_SECONDS = 3

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
        for index in range(0, 10):
            future = pool.submit(function, TIMEOUT_SECONDS, index, bar=1)
            future.add_done_callback(task_done)
