Pebble
======


Description
-----------

Pebble provides a neat API to manage threads and processes within an application.


Examples
--------

Spawn a function within a thread::

    from pebble import thread

    def function(foo, bar=0):
    	print foo + bar

    thrd = thread.spawn(target=function, args=[1], kwargs={'bar':2})
    thrd.join()


Most of the functions work as well as decorators::

    from pebble import process

    @process.spawn(daemon=True)
    def function(foo, bar=0):
    	print(foo + bar)

    proc = function(1, bar=2)
    proc.join()


Run a job in a separate process and wait for its results::

    from pebble import process

    @process.concurrent
    def function(foo, bar=0):
        return foo + bar

    task = function(1, bar=2)
    results = task.get()  # blocks until results are ready


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

Pebble 4:

 * use Futures instead of Tasks
 * move callback and timeout assignments to Futures
 * merge with concurrent.futures and asyncio modules API
