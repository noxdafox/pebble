Pebble
======


Description
-----------

Pebble provides a neat API to manage threads and processes within an application.


Examples
--------

Launch a task in a thread and wait for its results::

    from pebble import thread


    @thread
    def do_job(foo, bar=0):
    	return foo + bar


    if __name__ == "__main__":
        task = do_job(1, bar=2)
	print task.get()  # it will block until do_job has completed

Launch five tasks in separate processes and handle their results in a callback::

    from pebble import process


    def task_done(task):
    	print "Task %s has returned %d" % (task.id, task.get())


    @process(callback=task_done)
    def do_job(foo, bar=0):
    	return foo + bar


    if __name__ == "__main__":
        for i in range(0, 5):
            do_job(i)

	raw_input("Press return to exit.")

Callbacks can be dynamically (re)assigned, useful to set instance methods as callback::

    import time
    from pebble import process


    class Foo(object):
	def __init__(self):
	    self.counter = 0
	    self.errors = 0
	    self.do_job.callback = self.task_done

	def task_done(self, task):
            try:
                self.counter += task.get()
            except:  # exception are re-raised by the get() method
                self.errors += 1

	@process
	def do_job():
	    return 1

	@process
	def do_wrong_job():
	    raise Exception("Ops!")


    if __name__ == "__main__":
	foo = Foo()
	tasks = []

	for i in range(0, 5):
	    task = foo.do_job()
	    tasks.append(task)
	    task = foo.do_wrong_job()
	    tasks.append(task)

        time.sleep(1)

	print foo.counter
	print foo.errors

Thread pools allow to execute several tasks asynchronously without the need of spawning a new thread for each task::

    from threading import current_thread
    from pebble import thread_pool


    def task_done(task):
        results, thread = task.get()
    	print "Task %s has returned %d from thread %s" % (task.id, results, thread.ident)


    @thread_pool(callback=task_done, workers=5)
    def do_job(foo, bar=0):
    	return foo + bar, current_thread()


    if __name__ == "__main__":
        for i in range(0, 10):
            do_job(i)

	raw_input("Press return to exit.")


TODO
----

A roadmap::

 * pools of workers::

   - @process_pool
